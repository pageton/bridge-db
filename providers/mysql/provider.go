// Package mysql implements the MySQL migration provider for bridge-db.
// It supports row-based migrations with table enumeration, cursor-based scanning,
// and schema migration.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
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

	cfg, err := resolveMySQLConfig(raw)
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

// DryRun returns a wrapped Provider that logs writes without executing them.
func (p *MySQLProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
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

// resolveMySQLConfig handles both direct config and tunnel-resolved address.
func resolveMySQLConfig(raw any) (*config.MySQLConfig, error) {
	switch v := raw.(type) {
	case *config.MySQLConfig:
		if v == nil {
			return nil, fmt.Errorf("nil mysql config")
		}
		return v, nil

	case config.MySQLConfig:
		return &v, nil

	case map[string]string:
		return mysqlConfigFromMap(v)

	default:
		return nil, fmt.Errorf("unsupported mysql config type: %T", raw)
	}
}

// mysqlConfigFromMap builds a MySQLConfig from a tunnel-resolved address map.
func mysqlConfigFromMap(m map[string]string) (*config.MySQLConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}

	host, portStr, err := parseHostPort(addr)
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

// parseHostPort parses a host:port string.
func parseHostPort(addr string) (string, int, error) {
	if len(addr) == 0 {
		return "", 0, fmt.Errorf("empty address")
	}

	var host string
	var port int

	if addr[0] == '[' {
		// IPv6 format: [::1]:3306
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
		port = 3306
	}

	return host, port, nil
}

// buildMySQLDSN constructs a MySQL DSN from config.
func buildMySQLDSN(cfg *config.MySQLConfig) string {
	dsn := ""

	if cfg.Username != "" {
		dsn += cfg.Username
		if cfg.Password != "" {
			dsn += ":" + cfg.Password
		}
		dsn += "@"
	}

	if cfg.IsUnixSocket() {
		dsn += fmt.Sprintf("unix(%s)", cfg.Host)
	} else {
		dsn += fmt.Sprintf("tcp(%s:%d)", cfg.Host, cfg.Port)
	}

	if cfg.Database != "" {
		dsn += "/" + cfg.Database
	}

	dsn += "?parseTime=true&multiStatements=true"

	return dsn
}
