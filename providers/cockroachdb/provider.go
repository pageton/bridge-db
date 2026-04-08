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

	cfg, err := resolveCockroachDBConfig(raw)
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

func (p *CockroachDBProvider) Pool() *pgxpool.Pool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pool
}

func resolveCockroachDBConfig(raw any) (*config.CockroachDBConfig, error) {
	switch v := raw.(type) {
	case *config.CockroachDBConfig:
		if v == nil {
			return nil, fmt.Errorf("nil cockroachdb config")
		}
		return v, nil
	case config.CockroachDBConfig:
		return &v, nil
	case map[string]string:
		return cockroachDBConfigFromMap(v)
	default:
		return nil, fmt.Errorf("unsupported cockroachdb config type: %T", raw)
	}
}

func cockroachDBConfigFromMap(m map[string]string) (*config.CockroachDBConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}

	host, port, err := parseHostPortProvider(addr)
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

func parseHostPortProvider(addr string) (string, int, error) {
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
		port = 26257
	}

	return host, port, nil
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
