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

	cfg, err := resolvePostgresConfig(raw)
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

// Pool returns the underlying PostgreSQL connection pool (for internal use by sub-components).
func (p *PostgresProvider) Pool() *pgxpool.Pool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pool
}

// ---------------------------------------------------------------------------
// Config resolution
// ---------------------------------------------------------------------------

// resolvePostgresConfig handles both direct config and tunnel-resolved address.
func resolvePostgresConfig(raw any) (*config.PostgresConfig, error) {
	switch v := raw.(type) {
	case *config.PostgresConfig:
		if v == nil {
			return nil, fmt.Errorf("nil postgres config")
		}
		return v, nil

	case config.PostgresConfig:
		return &v, nil

	case map[string]string:
		return postgresConfigFromMap(v)

	default:
		return nil, fmt.Errorf("unsupported postgres config type: %T", raw)
	}
}

// postgresConfigFromMap builds a PostgresConfig from a tunnel-resolved address map.
func postgresConfigFromMap(m map[string]string) (*config.PostgresConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}

	host, portStr, err := parseHostPort(addr)
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

// parseHostPort parses a host:port string.
func parseHostPort(addr string) (string, int, error) {
	if len(addr) == 0 {
		return "", 0, fmt.Errorf("empty address")
	}

	var host string
	var port int

	if addr[0] == '[' {
		// IPv6 format: [::1]:5432
		end := -1
		for i := 1; i < len(addr); i++ {
			if addr[i] == ']' {
				end = i
				break
			}
		}
		if end == -1 {
			return "", 0, fmt.Errorf("invalid IPv6 address")
		}
		host = addr[1:end]
		if end+1 < len(addr) && addr[end+1] == ':' {
			_, _ = fmt.Sscanf(addr[end+2:], "%d", &port)
		}
	} else {
		// IPv4 or hostname
		for i := len(addr) - 1; i >= 0; i-- {
			if addr[i] == ':' {
				host = addr[:i]
				_, _ = fmt.Sscanf(addr[i+1:], "%d", &port)
				break
			}
		}
		if host == "" {
			host = addr
		}
	}

	if port == 0 {
		port = 5432
	}

	return host, port, nil
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

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------
