//go:build sqlite

package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	provider.Register("sqlite", func() provider.Provider {
		return &SQLiteProvider{}
	})
}

type SQLiteProvider struct {
	mu   sync.Mutex
	db   *sql.DB
	cfg  *config.SQLiteConfig
	role string
}

func (p *SQLiteProvider) Name() string {
	return "sqlite"
}

func (p *SQLiteProvider) Connect(_ context.Context, srcConfig, dstConfig any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	raw := srcConfig
	if raw == nil {
		raw = dstConfig
		p.role = "destination"
	} else {
		p.role = "source"
	}

	cfg, err := config.ResolveConfig(raw, "sqlite", sqliteConfigFromMap)
	if err != nil {
		return fmt.Errorf("sqlite %s: %w", p.role, err)
	}
	p.cfg = cfg

	dsn := buildSQLiteDSN(cfg)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("sqlite %s connect: %w", p.role, err)
	}

	db.SetMaxOpenConns(1)

	p.db = db

	log := logger.L().With("provider", "sqlite", "role", p.role)
	log.Debug("configured sqlite client", "path", cfg.Path)

	return nil
}

func (p *SQLiteProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db != nil {
		err := p.db.Close()
		p.db = nil
		return err
	}
	return nil
}

func (p *SQLiteProvider) Ping(ctx context.Context) error {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return fmt.Errorf("sqlite %s: not connected", p.role)
	}

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("sqlite %s ping: %w", p.role, err)
	}
	return nil
}

func (p *SQLiteProvider) Scanner(ctx context.Context, opts provider.ScanOptions) provider.Scanner {
	return newSQLiteScanner(p.db, opts)
}

func (p *SQLiteProvider) Writer(ctx context.Context, opts provider.WriteOptions) provider.Writer {
	return newSQLiteWriter(p.db, opts)
}

func (p *SQLiteProvider) Verifier(ctx context.Context) provider.Verifier {
	return newSQLiteVerifier(p.db)
}

func (p *SQLiteProvider) SchemaMigrator(ctx context.Context) provider.SchemaMigrator {
	return newSQLiteSchemaMigrator(p.db)
}

func (p *SQLiteProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
}

// Capabilities declares what the sqlite provider supports.
func (p *SQLiteProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Schema: true, Transactions: false, Verification: provider.VerifyCross, Incremental: true}
}

// EnumerateTables returns table names and their row counts.
func (p *SQLiteProvider) EnumerateTables(ctx context.Context) (map[string]int64, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("sqlite %s: not connected", p.role)
	}

	// Collect table names first — must close rows before running COUNT
	// queries because SQLite uses MaxOpenConns(1).
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			continue
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("enumerate tables: %w", err)
	}
	rows.Close()

	result := make(map[string]int64, len(tables))
	for _, table := range tables {
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(table))
		var count int64
		if err := db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
			continue
		}
		result[table] = count
	}
	return result, nil
}

// ReadRecords reads specific rows by their keys.
// Keys are in the format "table:primaryKey".
func (p *SQLiteProvider) ReadRecords(ctx context.Context, keys []string) (map[string]map[string]any, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("sqlite %s: not connected", p.role)
	}

	result := make(map[string]map[string]any, len(keys))
	for _, key := range keys {
		table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}

		var clauses []string
		var args []any
		for col, val := range pk {
			clauses = append(clauses, fmt.Sprintf("%s = ?", quoteIdentifier(col)))
			args = append(args, val)
		}

		query := fmt.Sprintf("SELECT * FROM %s WHERE %s",
			quoteIdentifier(table), strings.Join(clauses, " AND "))

		rows, err := db.QueryContext(ctx, query, args...)
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
// SQLite has no built-in MD5, so hashing is done in Go.
func (p *SQLiteProvider) ComputeChecksums(ctx context.Context, keys []string) (map[string]string, error) {
	p.mu.Lock()
	db := p.db
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("sqlite %s: not connected", p.role)
	}

	result := make(map[string]string, len(keys))
	for _, key := range keys {
		table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}

		var clauses []string
		var args []any
		for col, val := range pk {
			clauses = append(clauses, fmt.Sprintf("%s = ?", quoteIdentifier(col)))
			args = append(args, val)
		}

		query := fmt.Sprintf("SELECT * FROM %s WHERE %s",
			quoteIdentifier(table), strings.Join(clauses, " AND "))

		rows, err := db.QueryContext(ctx, query, args...)
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

func (p *SQLiteProvider) DB() *sql.DB {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db
}

func sqliteConfigFromMap(m map[string]string) (*config.SQLiteConfig, error) {
	path := m["path"]
	if path == "" {
		path = m["database"]
	}
	if path == "" {
		return nil, fmt.Errorf("missing path in sqlite config map")
	}

	cfg := config.DefaultSQLiteConfig()
	cfg.Path = path
	return &cfg, nil
}

func buildSQLiteDSN(cfg *config.SQLiteConfig) string {
	return cfg.Path
}
