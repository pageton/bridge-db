//go:build sqlite

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
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

	cfg, err := resolveSQLiteConfig(raw)
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
	log.Info("configured sqlite client", "path", cfg.Path)

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

func (p *SQLiteProvider) DB() *sql.DB {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db
}

func resolveSQLiteConfig(raw any) (*config.SQLiteConfig, error) {
	switch v := raw.(type) {
	case *config.SQLiteConfig:
		if v == nil {
			return nil, fmt.Errorf("nil sqlite config")
		}
		return v, nil
	case config.SQLiteConfig:
		return &v, nil
	case map[string]string:
		return sqliteConfigFromMap(v)
	default:
		return nil, fmt.Errorf("unsupported sqlite config type: %T", raw)
	}
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
