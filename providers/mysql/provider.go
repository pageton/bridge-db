// Package mysql implements the MySQL migration provider for bridge-db.
// It supports row-based migrations with table enumeration, cursor-based scanning,
// and schema migration.
package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	provider.Register("mysql", func() provider.Provider {
		return &MySQLProvider{}
	})
}

// MySQLProvider implements provider.Provider for MySQL databases.
// A single instance connects to one MySQL instance (either source or destination).
type MySQLProvider struct {
	mu   sync.Mutex
	db   *sql.DB
	cfg  *config.MySQLConfig
	role string // "source" or "destination"
}

// Name returns the provider identifier.
func (p *MySQLProvider) Name() string {
	return "mysql"
}

// Connect establishes a connection to a MySQL instance.
//
// The pipeline calls Connect in two ways:
//   - Source:      Connect(ctx, srcConfig, nil)
//   - Destination: Connect(ctx, nil, dstConfig)
//
// srcConfig/dstConfig can be either:
//   - *config.MySQLConfig  (direct connection)
//   - map[string]string    (tunnel-resolved address)
func (p *MySQLProvider) Connect(_ context.Context, srcConfig, dstConfig any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	raw := srcConfig
	if raw == nil {
		raw = dstConfig
		p.role = "destination"
	} else {
		p.role = "source"
	}

	cfg, err := config.ResolveConfig(raw, "mysql", mysqlConfigFromMap)
	if err != nil {
		return fmt.Errorf("mysql %s: %w", p.role, err)
	}
	p.cfg = cfg

	// Build DSN
	dsn := buildMySQLDSN(cfg)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mysql %s connect: %w", p.role, err)
	}
	p.db = db

	log := logger.L().With("provider", "mysql", "role", p.role)
	if cfg.IsUnixSocket() {
		log.Info("configured mysql client", "socket", cfg.Host, "database", cfg.Database)
	} else {
		log.Info("configured mysql client", "host", cfg.Host, "port", cfg.Port, "database", cfg.Database)
	}

	return nil
}

// Close releases the MySQL connection.
func (p *MySQLProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db != nil {
		err := p.db.Close()
		p.db = nil
		return err
	}
	return nil
}

// Ping verifies the MySQL connection is alive.
func (p *MySQLProvider) Ping(ctx context.Context) error {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return fmt.Errorf("mysql %s: not connected", p.role)
	}

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("mysql %s ping: %w", p.role, err)
	}
	return nil
}

// Scanner returns a Scanner that enumerates rows from the source MySQL.
func (p *MySQLProvider) Scanner(ctx context.Context, opts provider.ScanOptions) provider.Scanner {
	return newMySQLScanner(p.db, opts)
}

// Writer returns a Writer that persists rows to the destination MySQL.
func (p *MySQLProvider) Writer(ctx context.Context, opts provider.WriteOptions) provider.Writer {
	return newMySQLWriter(p.db, opts)
}

// Verifier returns a Verifier for post-migration checks.
func (p *MySQLProvider) Verifier(ctx context.Context) provider.Verifier {
	return newMySQLVerifier(p.db)
}

// SchemaMigrator returns a SchemaMigrator for MySQL schema migration.
func (p *MySQLProvider) SchemaMigrator(ctx context.Context) provider.SchemaMigrator {
	return newMySQLSchemaMigrator(p.db)
}

// EnumerateTables returns table names and their row counts.
func (p *MySQLProvider) EnumerateTables(ctx context.Context) (map[string]int64, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mysql %s: not connected", p.role)
	}

	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]int64)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			continue
		}

		var count int64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(table))
		if err := db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
			continue
		}
		result[table] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("enumerate tables: %w", err)
	}
	return result, nil
}

// ReadRecords reads specific rows by their keys.
// Keys are in the format "table:primaryKey".
func (p *MySQLProvider) ReadRecords(ctx context.Context, keys []string) (map[string]map[string]any, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mysql %s: not connected", p.role)
	}

	result := make(map[string]map[string]any, len(keys))
	for _, key := range keys {
		table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}

		whereClause, whereArgs := buildPKWhere(pk, 1)
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s",
			quoteIdentifier(table), whereClause)

		rows, err := db.QueryContext(ctx, query, whereArgs...)
		if err != nil {
			continue
		}

		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			continue
		}

		colPtrs := make([]any, len(cols))
		colVals := make([]any, len(cols))
		for i := range colPtrs {
			colPtrs[i] = &colVals[i]
		}

		if rows.Next() {
			if err := rows.Scan(colPtrs...); err != nil {
				_ = rows.Close()
				continue
			}
			row := make(map[string]any, len(cols))
			for i, col := range cols {
				row[col] = colVals[i]
			}
			result[key] = row
		}
		_ = rows.Close()
	}
	return result, nil
}

// ComputeChecksums returns MD5 checksums for the given keys.
// Keys are in the format "table:primaryKey".
func (p *MySQLProvider) ComputeChecksums(ctx context.Context, keys []string) (map[string]string, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mysql %s: not connected", p.role)
	}

	result := make(map[string]string, len(keys))
	for _, key := range keys {
		table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}

		whereClause, whereArgs := buildPKWhere(pk, 1)
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s",
			quoteIdentifier(table), whereClause)

		rows, err := db.QueryContext(ctx, query, whereArgs...)
		if err != nil {
			continue
		}

		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			continue
		}

		colPtrs := make([]any, len(cols))
		colVals := make([]any, len(cols))
		for i := range colPtrs {
			colPtrs[i] = &colVals[i]
		}

		if rows.Next() {
			if err := rows.Scan(colPtrs...); err != nil {
				_ = rows.Close()
				continue
			}

			// Sort column names for deterministic hashing.
			sortedCols := make([]string, len(cols))
			copy(sortedCols, cols)
			sort.Strings(sortedCols)

			var buf strings.Builder
			for i, col := range sortedCols {
				if i > 0 {
					buf.WriteByte('|')
				}
				// Find the index in the original cols slice for this sorted name.
				for j, c := range cols {
					if c == col {
						fmt.Fprintf(&buf, "%v", colVals[j])
						break
					}
				}
			}
			hash := sha256.Sum256([]byte(buf.String()))
			result[key] = hex.EncodeToString(hash[:])
		}
		_ = rows.Close()
	}
	return result, nil
}

// DryRun returns a wrapped Provider that logs writes without executing them.
func (p *MySQLProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
}

// Capabilities declares what the mysql provider supports.
func (p *MySQLProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Schema: true, Transactions: true, Verification: provider.VerifyCross, Incremental: true}
}

// DB returns the underlying MySQL database connection (for internal use by sub-components).
func (p *MySQLProvider) DB() *sql.DB {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db
}

// ---------------------------------------------------------------------------
// Config resolution
// ---------------------------------------------------------------------------

// mysqlConfigFromMap builds a MySQLConfig from a tunnel-resolved address map.
func mysqlConfigFromMap(m map[string]string) (*config.MySQLConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}

	host, portStr, err := provider.ParseHostPort(addr, 3306)
	if err != nil {
		return nil, fmt.Errorf("invalid tunnel address %q: %w", addr, err)
	}

	cfg := config.DefaultMySQLConfig()
	cfg.Host = host
	cfg.Port = portStr

	// Copy database name if provided
	if db := m["database"]; db != "" {
		cfg.Database = db
	}

	// Copy username if provided
	if user := m["username"]; user != "" {
		cfg.Username = user
	}

	return &cfg, nil
}

// buildMySQLDSN constructs a MySQL DSN from config.
func buildMySQLDSN(cfg *config.MySQLConfig) string {
	return cfg.DSNWithPassword() + "?parseTime=true&multiStatements=true"
}
