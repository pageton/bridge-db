//go:build redis

// Package redis implements the Redis migration provider for bridge-db.
// It supports all 6 Redis data types (string, hash, list, set, zset, stream)
// with TTL preservation, pipelined writes, and type-aware verification.
package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	provider.Register("redis", func() provider.Provider {
		return &RedisProvider{}
	})
}

// RedisProvider implements provider.Provider for Redis databases.
// A single instance connects to one Redis instance (either source or destination).
type RedisProvider struct {
	mu     sync.Mutex
	client *redis.Client
	cfg    *config.RedisConfig
	role   string // "source" or "destination"
}

// Name returns the provider identifier.
func (p *RedisProvider) Name() string {
	return "redis"
}

// Connect establishes a connection to a Redis instance.
//
// The pipeline calls Connect in two ways:
//   - Source:      Connect(ctx, srcConfig, nil)
//   - Destination: Connect(ctx, nil, dstConfig)
//
// srcConfig/dstConfig can be either:
//   - *config.RedisConfig  (direct connection)
//   - map[string]string    (tunnel-resolved address)
func (p *RedisProvider) Connect(_ context.Context, srcConfig, dstConfig any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	raw := srcConfig
	if raw == nil {
		raw = dstConfig
		p.role = "destination"
	} else {
		p.role = "source"
	}

	cfg, err := resolveRedisConfig(raw)
	if err != nil {
		return fmt.Errorf("redis %s: %w", p.role, err)
	}
	p.cfg = cfg

	opts := &redis.Options{
		Addr:     cfg.Address(),
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	p.client = redis.NewClient(opts)

	log := logger.L().With("provider", "redis", "role", p.role)
	log.Info("configured redis client", "addr", cfg.Address(), "db", cfg.DB)

	return nil
}

// Close releases the Redis connection.
func (p *RedisProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		err := p.client.Close()
		p.client = nil
		return err
	}
	return nil
}

// Ping verifies the Redis connection is alive.
func (p *RedisProvider) Ping(ctx context.Context) error {
	p.mu.Lock()
	client := p.client
	p.mu.Unlock()

	if client == nil {
		return fmt.Errorf("redis %s: not connected", p.role)
	}

	pong, err := client.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("redis %s ping: %w", p.role, err)
	}
	if pong != "PONG" {
		return fmt.Errorf("redis %s: unexpected PING response: %s", p.role, pong)
	}
	return nil
}

// Scanner returns a Scanner that enumerates keys from the source Redis.
func (p *RedisProvider) Scanner(ctx context.Context, opts provider.ScanOptions) provider.Scanner {
	return newRedisScanner(p.client, opts)
}

// Writer returns a Writer that persists keys to the destination Redis.
func (p *RedisProvider) Writer(ctx context.Context, opts provider.WriteOptions) provider.Writer {
	return newRedisWriter(p.client, opts)
}

// Verifier returns a Verifier for post-migration checks.
func (p *RedisProvider) Verifier(ctx context.Context) provider.Verifier {
	return newRedisVerifier(p.client)
}

// SchemaMigrator returns nil — Redis has no schema to migrate.
func (p *RedisProvider) SchemaMigrator(_ context.Context) provider.SchemaMigrator {
	return nil
}

// DryRun returns a wrapped Provider that logs writes without executing them.
func (p *RedisProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
}

// Client returns the underlying Redis client (for internal use by sub-components).
func (p *RedisProvider) Client() *redis.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}

// ---------------------------------------------------------------------------
// Config resolution
// ---------------------------------------------------------------------------

// resolveRedisConfig handles both direct config and tunnel-resolved address.
func resolveRedisConfig(raw any) (*config.RedisConfig, error) {
	switch v := raw.(type) {
	case *config.RedisConfig:
		if v == nil {
			return nil, fmt.Errorf("nil redis config")
		}
		return v, nil

	case config.RedisConfig:
		return &v, nil

	case map[string]string:
		return redisConfigFromMap(v)

	default:
		return nil, fmt.Errorf("unsupported redis config type: %T", raw)
	}
}

// redisConfigFromMap builds a RedisConfig from a tunnel-resolved address map.
func redisConfigFromMap(m map[string]string) (*config.RedisConfig, error) {
	addr := m["tunnel_addr"]
	if addr == "" {
		return nil, fmt.Errorf("missing tunnel_addr in config map")
	}

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid tunnel address %q: %w", addr, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid tunnel port %q: %w", portStr, err)
	}

	cfg := config.DefaultRedisConfig()
	cfg.Host = host
	cfg.Port = port

	return &cfg, nil
}
