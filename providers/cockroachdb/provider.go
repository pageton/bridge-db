package cockroachdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	provider.Register("cockroachdb", func() provider.Provider {
		return &CockroachDBProvider{}
	})
}

type CockroachDBProvider struct {
	mu   sync.Mutex
	pool *pgxpool.Pool
	cfg  *config.CockroachDBConfig
	role string
}

func (p *CockroachDBProvider) Name() string {
	return "cockroachdb"
}

func (p *CockroachDBProvider) Connect(ctx context.Context, srcConfig, dstConfig any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	raw := srcConfig
	if raw == nil {
		raw = dstConfig
		p.role = "destination"
	} else {
		p.role = "source"
	}

	cfg, err := config.ResolveConfig(raw, "cockroachdb", cockroachDBConfigFromMap)
	if err != nil {
		return fmt.Errorf("cockroachdb %s: %w", p.role, err)
	}
	p.cfg = cfg

	connStr := buildCockroachDBConnStr(cfg)

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("cockroachdb %s parse config: %w", p.role, err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("cockroachdb %s connect: %w", p.role, err)
	}
	p.pool = pool

	log := logger.L().With("provider", "cockroachdb", "role", p.role)
	log.Info("configured cockroachdb client", "host", cfg.Host, "port", cfg.Port, "database", cfg.Database)

	return nil
}

func (p *CockroachDBProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pool != nil {
		p.pool.Close()
		p.pool = nil
	}
	return nil
}

func (p *CockroachDBProvider) Ping(ctx context.Context) error {
	p.mu.Lock()
	pool := p.pool
	p.mu.Unlock()

	if pool == nil {
		return fmt.Errorf("cockroachdb %s: not connected", p.role)
	}

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("cockroachdb %s ping: %w", p.role, err)
	}
	return nil
}

func (p *CockroachDBProvider) Scanner(ctx context.Context, opts provider.ScanOptions) provider.Scanner {
	return newCockroachDBScanner(p.pool, opts)
}

func (p *CockroachDBProvider) Writer(ctx context.Context, opts provider.WriteOptions) provider.Writer {
	return newCockroachDBWriter(p.pool, opts)
}

func (p *CockroachDBProvider) Verifier(ctx context.Context) provider.Verifier {
	return newCockroachDBVerifier(p.pool)
}

func (p *CockroachDBProvider) SchemaMigrator(ctx context.Context) provider.SchemaMigrator {
	return newCockroachDBSchemaMigrator(p.pool)
}

func (p *CockroachDBProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
}

// Capabilities declares what the cockroachdb provider supports.
func (p *CockroachDBProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Schema: true, Transactions: true, Verification: provider.VerifyCross, Incremental: true}
}

// EnumerateTables returns table names and their row counts.
func (p *CockroachDBProvider) EnumerateTables(ctx context.Context) (map[string]int64, error) {
	p.mu.Lock()
	pool := p.pool
	p.mu.Unlock()

	if pool == nil {
		return nil, fmt.Errorf("cockroachdb %s: not connected", p.role)
	}

	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'crdb_internal')
		AND table_type = 'BASE TABLE'
	`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			continue
		}
		fqn := schema + "." + table

		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s",
			quoteIdentifier(schema), quoteIdentifier(table))
		var count int64
		if err := pool.QueryRow(ctx, countQuery).Scan(&count); err != nil {
			continue
		}
		result[fqn] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("enumerate tables: %w", err)
	}
	return result, nil
}

// ReadRecords reads specific rows by their keys.
// Keys are in the format "schema.table:primaryKey".
func (p *CockroachDBProvider) ReadRecords(ctx context.Context, keys []string) (map[string]map[string]any, error) {
	p.mu.Lock()
	pool := p.pool
	p.mu.Unlock()

	if pool == nil {
		return nil, fmt.Errorf("cockroachdb %s: not connected", p.role)
	}

	result := make(map[string]map[string]any, len(keys))
	for _, key := range keys {
		schema, table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}

		whereClause, whereArgs := buildPKWhere(pk, 1)
		query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s",
			quoteIdentifier(schema), quoteIdentifier(table), whereClause)

		rows, err := pool.Query(ctx, query, whereArgs...)
		if err != nil {
			continue
		}

		fields := rows.FieldDescriptions()
		if rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				continue
			}
			row := make(map[string]any, len(fields))
			for i, fd := range fields {
				row[fd.Name] = values[i]
			}
			result[key] = row
		}
		rows.Close()
	}
	return result, nil
}

// ComputeChecksums returns MD5 checksums for the given keys.
func (p *CockroachDBProvider) ComputeChecksums(ctx context.Context, keys []string) (map[string]string, error) {
	p.mu.Lock()
	pool := p.pool
	p.mu.Unlock()

	if pool == nil {
		return nil, fmt.Errorf("cockroachdb %s: not connected", p.role)
	}

	result := make(map[string]string, len(keys))
	for _, key := range keys {
		schema, table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}

		whereClause, whereArgs := buildPKWhere(pk, 1)
		query := fmt.Sprintf(
			"SELECT md5(row_to_json(t)::text) FROM (SELECT * FROM %s.%s WHERE %s) t",
			quoteIdentifier(schema), quoteIdentifier(table), whereClause,
		)

		var hash string
		if err := pool.QueryRow(ctx, query, whereArgs...).Scan(&hash); err != nil {
			continue
		}
		result[key] = hash
	}
	return result, nil
}

func (p *CockroachDBProvider) Pool() *pgxpool.Pool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pool
}

func cockroachDBConfigFromMap(m map[string]string) (*config.CockroachDBConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}

	host, port, err := provider.ParseHostPort(addr, 26257)
	if err != nil {
		return nil, fmt.Errorf("invalid tunnel address %q: %w", addr, err)
	}

	cfg := config.DefaultCockroachDBConfig()
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

func buildCockroachDBConnStr(cfg *config.CockroachDBConfig) string {
	connStr := fmt.Sprintf("host=%s port=%d", cfg.Host, cfg.Port)

	if cfg.Database != "" {
		connStr += fmt.Sprintf(" dbname=%s", cfg.Database)
	}
	if cfg.Username != "" {
		connStr += fmt.Sprintf(" user=%s", cfg.Username)
	}
	if cfg.Password != "" {
		connStr += fmt.Sprintf(" password=%s", cfg.Password)
	}
	if cfg.SSLMode != "" {
		connStr += fmt.Sprintf(" sslmode=%s", cfg.SSLMode)
	}

	return connStr
}
