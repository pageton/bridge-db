package mariadb

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

	cfg, err := resolveMariaDBConfig(raw)
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
		log.Info("configured mariadb client", "socket", cfg.Host, "database", cfg.Database)
	} else {
		log.Info("configured mariadb client", "host", cfg.Host, "port", cfg.Port, "database", cfg.Database)
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

func (p *MariaDBProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
}

func (p *MariaDBProvider) DB() *sql.DB {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db
}

func resolveMariaDBConfig(raw any) (*config.MariaDBConfig, error) {
	switch v := raw.(type) {
	case *config.MariaDBConfig:
		if v == nil {
			return nil, fmt.Errorf("nil mariadb config")
		}
		return v, nil
	case config.MariaDBConfig:
		return &v, nil
	case map[string]string:
		return mariaDBConfigFromMap(v)
	default:
		return nil, fmt.Errorf("unsupported mariadb config type: %T", raw)
	}
}

func mariaDBConfigFromMap(m map[string]string) (*config.MariaDBConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}
	host, port, err := parseHostPort(addr)
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

func parseHostPort(addr string) (string, int, error) {
	if len(addr) == 0 {
		return "", 0, fmt.Errorf("empty address")
	}
	var host string
	var port int
	if addr[0] == '[' {
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

func buildMariaDBDSN(cfg *config.MariaDBConfig) string {
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
