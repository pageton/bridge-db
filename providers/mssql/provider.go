//go:build mssql

package mssql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	provider.Register("mssql", func() provider.Provider {
		return &MSSQLProvider{}
	})
}

type MSSQLProvider struct {
	mu   sync.Mutex
	db   *sql.DB
	cfg  *config.MSSQLConfig
	role string
}

func (p *MSSQLProvider) Name() string {
	return "mssql"
}

func (p *MSSQLProvider) Connect(_ context.Context, srcConfig, dstConfig any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	raw := srcConfig
	if raw == nil {
		raw = dstConfig
		p.role = "destination"
	} else {
		p.role = "source"
	}

	cfg, err := config.ResolveConfig(raw, "mssql", mssqlConfigFromMap)
	if err != nil {
		return fmt.Errorf("mssql %s: %w", p.role, err)
	}
	p.cfg = cfg

	dsn := buildMSSQLDSN(cfg)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("mssql %s connect: %w", p.role, err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	p.db = db

	log := logger.L().With("provider", "mssql", "role", p.role)
	log.Debug("configured mssql client", "host", cfg.Host, "port", cfg.Port, "database", cfg.Database)

	return nil
}

func (p *MSSQLProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db != nil {
		err := p.db.Close()
		p.db = nil
		return err
	}
	return nil
}

func (p *MSSQLProvider) Ping(ctx context.Context) error {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return fmt.Errorf("mssql %s: not connected", p.role)
	}

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("mssql %s ping: %w", p.role, err)
	}
	return nil
}

func (p *MSSQLProvider) Scanner(ctx context.Context, opts provider.ScanOptions) provider.Scanner {
	return newMSSQLScanner(p.db, opts)
}

func (p *MSSQLProvider) Writer(ctx context.Context, opts provider.WriteOptions) provider.Writer {
	return newMSSQLWriter(p.db, opts)
}

func (p *MSSQLProvider) Verifier(ctx context.Context) provider.Verifier {
	return newMSSQLVerifier(p.db)
}

func (p *MSSQLProvider) SchemaMigrator(ctx context.Context) provider.SchemaMigrator {
	return newMSSQLSchemaMigrator(p.db)
}

func (p *MSSQLProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
}

// Capabilities declares what the mssql provider supports.
func (p *MSSQLProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Schema: true, Transactions: true, Verification: provider.VerifyCross, Incremental: true}
}

// EnumerateTables returns table names and their row counts.
func (p *MSSQLProvider) EnumerateTables(ctx context.Context) (map[string]int64, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mssql %s: not connected", p.role)
	}

	query := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			continue
		}

		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(table))
		var count int64
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
func (p *MSSQLProvider) ReadRecords(ctx context.Context, keys []string) (map[string]map[string]any, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mssql %s: not connected", p.role)
	}

	result := make(map[string]map[string]any, len(keys))
	for _, key := range keys {
		table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}

		whereClauses := make([]string, 0, len(pk))
		whereArgs := make([]any, 0, len(pk))
		for col, val := range pk {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = @p%d", quoteIdentifier(col), len(whereArgs)+1))
			whereArgs = append(whereArgs, val)
		}

		query := fmt.Sprintf("SELECT * FROM %s WHERE %s",
			quoteIdentifier(table), strings.Join(whereClauses, " AND "))

		rows, err := db.QueryContext(ctx, query, whereArgs...)
		if err != nil {
			continue
		}

		cols, _ := rows.Columns()
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
// MSSQL's HASHBYTES has input length limits, so hashing is done in Go.
func (p *MSSQLProvider) ComputeChecksums(ctx context.Context, keys []string) (map[string]string, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mssql %s: not connected", p.role)
	}

	result := make(map[string]string, len(keys))
	for _, key := range keys {
		table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}

		whereClauses := make([]string, 0, len(pk))
		whereArgs := make([]any, 0, len(pk))
		for col, val := range pk {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = @p%d", quoteIdentifier(col), len(whereArgs)+1))
			whereArgs = append(whereArgs, val)
		}

		query := fmt.Sprintf("SELECT * FROM %s WHERE %s",
			quoteIdentifier(table), strings.Join(whereClauses, " AND "))

		rows, err := db.QueryContext(ctx, query, whereArgs...)
		if err != nil {
			continue
		}

		cols, _ := rows.Columns()
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
			sortedCols := make([]string, len(cols))
			copy(sortedCols, cols)
			sort.Strings(sortedCols)

			var buf strings.Builder
			for _, col := range sortedCols {
				for i, c := range cols {
					if c == col {
						fmt.Fprintf(&buf, "%v", colVals[i])
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

func (p *MSSQLProvider) DB() *sql.DB {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db
}

func mssqlConfigFromMap(m map[string]string) (*config.MSSQLConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}

	host, port, err := provider.ParseHostPort(addr, 1433)
	if err != nil {
		return nil, fmt.Errorf("invalid tunnel address %q: %w", addr, err)
	}

	cfg := config.DefaultMSSQLConfig()
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

func buildMSSQLDSN(cfg *config.MSSQLConfig) string {
	return cfg.DSNWithPassword()
}
