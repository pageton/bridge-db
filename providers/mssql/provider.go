//go:build mssql

package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

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

	cfg, err := resolveMSSQLConfig(raw)
	if err != nil {
		return fmt.Errorf("mssql %s: %w", p.role, err)
	}
	p.cfg = cfg

	dsn := buildMSSQLDSN(cfg)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("mssql %s connect: %w", p.role, err)
	}
	p.db = db

	log := logger.L().With("provider", "mssql", "role", p.role)
	log.Info("configured mssql client", "host", cfg.Host, "port", cfg.Port, "database", cfg.Database)

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

func (p *MSSQLProvider) DB() *sql.DB {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db
}

func resolveMSSQLConfig(raw any) (*config.MSSQLConfig, error) {
	switch v := raw.(type) {
	case *config.MSSQLConfig:
		if v == nil {
			return nil, fmt.Errorf("nil mssql config")
		}
		return v, nil

	case config.MSSQLConfig:
		return &v, nil

	case map[string]string:
		return mssqlConfigFromMap(v)

	default:
		return nil, fmt.Errorf("unsupported mssql config type: %T", raw)
	}
}

func mssqlConfigFromMap(m map[string]string) (*config.MSSQLConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}

	host, port, err := parseHostPort(addr)
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
		port = 1433
	}

	return host, port, nil
}

func buildMSSQLDSN(cfg *config.MSSQLConfig) string {
	return cfg.DSN()
}
