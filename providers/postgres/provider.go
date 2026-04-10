// Package postgres implements the PostgreSQL migration provider for bridge-db.
// It supports row-based migrations with table enumeration, cursor-based scanning,
// and schema migration including type mapping.
package postgres

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
	provider.Register("postgres", func() provider.Provider {
		return &PostgresProvider{}
	})
}

// PostgresProvider implements provider.Provider for PostgreSQL databases.
// A single instance connects to one PostgreSQL instance (either source or destination).
type PostgresProvider struct {
	mu   sync.Mutex
	pool *pgxpool.Pool
	cfg  *config.PostgresConfig
	role string // "source" or "destination"
}

// Name returns the provider identifier.
func (p *PostgresProvider) Name() string {
	return "postgres"
}

// Connect establishes a connection to a PostgreSQL instance.
//
// The pipeline calls Connect in two ways:
//   - Source:      Connect(ctx, srcConfig, nil)
//   - Destination: Connect(ctx, nil, dstConfig)
//
// srcConfig/dstConfig can be either:
//   - *config.PostgresConfig  (direct connection)
//   - map[string]string       (tunnel-resolved address)
func (p *PostgresProvider) Connect(ctx context.Context, srcConfig, dstConfig any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	raw := srcConfig
	if raw == nil {
		raw = dstConfig
		p.role = "destination"
	} else {
		p.role = "source"
	}

	cfg, err := config.ResolveConfig(raw, "postgres", postgresConfigFromMap)
	if err != nil {
		return fmt.Errorf("postgres %s: %w", p.role, err)
	}
	p.cfg = cfg

	// Build connection string
	connStr := buildPostgresConnStr(cfg)

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("postgres %s parse config: %w", p.role, err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("postgres %s connect: %w", p.role, err)
	}
	p.pool = pool

	log := logger.L().With("provider", "postgres", "role", p.role)
	log.Info("configured postgres client", "host", cfg.Host, "port", cfg.Port, "database", cfg.Database)

	return nil
}

// Close releases the PostgreSQL connection pool.
func (p *PostgresProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pool != nil {
		p.pool.Close()
		p.pool = nil
	}
	return nil
}

// Ping verifies the PostgreSQL connection is alive.
func (p *PostgresProvider) Ping(ctx context.Context) error {
	p.mu.Lock()
	pool := p.pool
	p.mu.Unlock()

	if pool == nil {
		return fmt.Errorf("postgres %s: not connected", p.role)
	}

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres %s ping: %w", p.role, err)
	}
	return nil
}

// Scanner returns a Scanner that enumerates rows from the source PostgreSQL.
func (p *PostgresProvider) Scanner(ctx context.Context, opts provider.ScanOptions) provider.Scanner {
	return newPostgresScanner(p.pool, opts)
}

// Writer returns a Writer that persists rows to the destination PostgreSQL.
func (p *PostgresProvider) Writer(ctx context.Context, opts provider.WriteOptions) provider.Writer {
	return newPostgresWriter(p.pool, opts)
}

// Verifier returns a Verifier for post-migration checks.
func (p *PostgresProvider) Verifier(ctx context.Context) provider.Verifier {
	return newPostgresVerifier(p.pool)
}

// SchemaMigrator returns a SchemaMigrator for PostgreSQL schema migration.
func (p *PostgresProvider) SchemaMigrator(ctx context.Context) provider.SchemaMigrator {
	return newPostgresSchemaMigrator(p.pool)
}

// DryRun returns a wrapped Provider that logs writes without executing them.
func (p *PostgresProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
}

// Capabilities declares what the postgres provider supports.
func (p *PostgresProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Schema: true, Transactions: true, Verification: provider.VerifyCross, Incremental: true}
}

// EnumerateTables returns table names and their row counts.
func (p *PostgresProvider) EnumerateTables(ctx context.Context) (map[string]int64, error) {
	p.mu.Lock()
	pool := p.pool
	p.mu.Unlock()

	if pool == nil {
		return nil, fmt.Errorf("postgres %s: not connected", p.role)
	}

	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
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

		var count int64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s",
			quoteIdentifier(schema), quoteIdentifier(table))
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
func (p *PostgresProvider) ReadRecords(ctx context.Context, keys []string) (map[string]map[string]any, error) {
	p.mu.Lock()
	pool := p.pool
	p.mu.Unlock()

	if pool == nil {
		return nil, fmt.Errorf("postgres %s: not connected", p.role)
	}

	result := make(map[string]map[string]any, len(keys))
	for _, key := range keys {
		schema, table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}
		pk = resolvePrimaryKeyMap(ctx, pool, schema, table, pk)

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
func (p *PostgresProvider) ComputeChecksums(ctx context.Context, keys []string) (map[string]string, error) {
	p.mu.Lock()
	pool := p.pool
	p.mu.Unlock()

	if pool == nil {
		return nil, fmt.Errorf("postgres %s: not connected", p.role)
	}

	result := make(map[string]string, len(keys))
	for _, key := range keys {
		schema, table, pk, err := parseRowKey(key)
		if err != nil {
			continue
		}
		pk = resolvePrimaryKeyMap(ctx, pool, schema, table, pk)

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

// Pool returns the underlying PostgreSQL connection pool (for internal use by sub-components).
func (p *PostgresProvider) Pool() *pgxpool.Pool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pool
}

// ---------------------------------------------------------------------------
// Config resolution
// ---------------------------------------------------------------------------

// postgresConfigFromMap builds a PostgresConfig from a tunnel-resolved address map.
func postgresConfigFromMap(m map[string]string) (*config.PostgresConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}

	host, portStr, err := provider.ParseHostPort(addr, 5432)
	if err != nil {
		return nil, fmt.Errorf("invalid tunnel address %q: %w", addr, err)
	}

	cfg := config.DefaultPostgresConfig()
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

// buildPostgresConnStr constructs a PostgreSQL connection string from config.
func buildPostgresConnStr(cfg *config.PostgresConfig) string {
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

func resolvePrimaryKeyMap(ctx context.Context, pool *pgxpool.Pool, schema, table string, pk map[string]any) map[string]any {
	if pool == nil {
		return pk
	}
	if len(pk) != 1 {
		return pk
	}
	val, ok := pk["id"]
	if !ok {
		return pk
	}

	pkColumns, err := queryPrimaryKeyColumns(ctx, pool, schema, table)
	if err != nil || len(pkColumns) != 1 || pkColumns[0] == "id" {
		return pk
	}

	return map[string]any{pkColumns[0]: val}
}

func queryPrimaryKeyColumns(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]string, error) {
	const pkQuery = `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2
		ORDER BY kcu.ordinal_position
	`

	rows, err := pool.Query(ctx, pkQuery, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			continue
		}
		pkColumns = append(pkColumns, col)
	}
	return pkColumns, rows.Err()
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------
