package mariadb

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
	provider.Register("mariadb", func() provider.Provider {
		return &MariaDBProvider{}
	})
}

type MariaDBProvider struct {
	mu   sync.Mutex
	db   *sql.DB
	cfg  *config.MariaDBConfig
	role string
}

func (p *MariaDBProvider) Name() string {
	return "mariadb"
}

func (p *MariaDBProvider) Connect(_ context.Context, srcConfig, dstConfig any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	raw := srcConfig
	if raw == nil {
		raw = dstConfig
		p.role = "destination"
	} else {
		p.role = "source"
	}

	cfg, err := config.ResolveConfig(raw, "mariadb", mariaDBConfigFromMap)
	if err != nil {
		return fmt.Errorf("mariadb %s: %w", p.role, err)
	}
	p.cfg = cfg

	dsn := buildMariaDBDSN(cfg)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mariadb %s connect: %w", p.role, err)
	}
	p.db = db

	log := logger.L().With("provider", "mariadb", "role", p.role)
	if cfg.IsUnixSocket() {
		log.Debug("configured mariadb client", "socket", cfg.Host, "database", cfg.Database)
	} else {
		log.Debug("configured mariadb client", "host", cfg.Host, "port", cfg.Port, "database", cfg.Database)
	}
	return nil
}

func (p *MariaDBProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.db != nil {
		err := p.db.Close()
		p.db = nil
		return err
	}
	return nil
}

func (p *MariaDBProvider) Ping(ctx context.Context) error {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()
	if db == nil {
		return fmt.Errorf("mariadb %s: not connected", p.role)
	}
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("mariadb %s ping: %w", p.role, err)
	}
	return nil
}

func (p *MariaDBProvider) Scanner(ctx context.Context, opts provider.ScanOptions) provider.Scanner {
	return newMariaDBScanner(p.db, opts)
}

func (p *MariaDBProvider) Writer(ctx context.Context, opts provider.WriteOptions) provider.Writer {
	return newMariaDBWriter(p.db, opts)
}

func (p *MariaDBProvider) Verifier(ctx context.Context) provider.Verifier {
	return newMariaDBVerifier(p.db)
}

func (p *MariaDBProvider) SchemaMigrator(ctx context.Context) provider.SchemaMigrator {
	return newMariaDBSchemaMigrator(p.db)
}

// EnumerateTables returns table names and their row counts.
func (p *MariaDBProvider) EnumerateTables(ctx context.Context) (map[string]int64, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mariadb %s: not connected", p.role)
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
func (p *MariaDBProvider) ReadRecords(ctx context.Context, keys []string) (map[string]map[string]any, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mariadb %s: not connected", p.role)
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
func (p *MariaDBProvider) ComputeChecksums(ctx context.Context, keys []string) (map[string]string, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mariadb %s: not connected", p.role)
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

func (p *MariaDBProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
}

// Capabilities declares what the mariadb provider supports.
func (p *MariaDBProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Schema: true, Transactions: true, Verification: provider.VerifyCross, Incremental: true}
}

func (p *MariaDBProvider) DB() *sql.DB {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db
}

func mariaDBConfigFromMap(m map[string]string) (*config.MariaDBConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}
	host, port, err := provider.ParseHostPort(addr, 3306)
	if err != nil {
		return nil, fmt.Errorf("invalid tunnel address %q: %w", addr, err)
	}
	cfg := config.DefaultMariaDBConfig()
	cfg.Host = host
	cfg.Port = port
	if db := m["database"]; db != "" {
		cfg.Database = db
	}
	if user := m["username"]; user != "" {
		cfg.Username = user
	}
	return &cfg, nil
}

func buildMariaDBDSN(cfg *config.MariaDBConfig) string {
	return cfg.DSNWithPassword() + "?parseTime=true&multiStatements=true"
}
