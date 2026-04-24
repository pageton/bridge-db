//go:build redis

// Package redis implements the Redis migration provider for bridge-db.
// It supports all 6 Redis data types (string, hash, list, set, zset, stream)
// with TTL preservation, pipelined writes, and type-aware verification.
package redis

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"sort"
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

	cfg, err := config.ResolveConfig(raw, "redis", redisConfigFromMap)
	if err != nil {
		return fmt.Errorf("redis %s: %w", p.role, err)
	}
	p.cfg = cfg

	opts := &redis.Options{
		Addr:     cfg.Address(),
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.GetDB(),
	}
	if cfg.GetTLS() {
		opts.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	p.client = redis.NewClient(opts)

	log := logger.L().With("provider", "redis", "role", p.role)
	if !cfg.GetTLS() && cfg.Host != "127.0.0.1" && cfg.Host != "localhost" {
		log.Warn("redis connection is unencrypted — consider enabling TLS for non-local hosts",
			"addr", cfg.Address())
	}
	log.Debug("configured redis client", "addr", cfg.Address(), "db", cfg.GetDB())

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

// Capabilities declares what the redis provider supports.
func (p *RedisProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Schema: false, Transactions: false, Verification: provider.VerifyCross, Incremental: true}
}

// Client returns the underlying Redis client (for internal use by sub-components).
func (p *RedisProvider) Client() *redis.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}

// ---------------------------------------------------------------------------
// Cross-verification interfaces
// ---------------------------------------------------------------------------

// EnumerateTables returns the total key count in the Redis database.
// Redis does not have tables, so it returns a single entry with key "".
func (p *RedisProvider) EnumerateTables(ctx context.Context) (map[string]int64, error) {
	p.mu.Lock()
	client := p.client
	p.mu.Unlock()

	if client == nil {
		return nil, fmt.Errorf("redis %s: not connected", p.role)
	}

	count, err := client.DBSize(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("dbsize: %w", err)
	}

	return map[string]int64{"": count}, nil
}

// ReadRecords reads the value, type, and TTL for each requested key.
// Returns a map from key to fields {"type", "value", "ttl"}.
func (p *RedisProvider) ReadRecords(ctx context.Context, keys []string) (map[string]map[string]any, error) {
	p.mu.Lock()
	client := p.client
	p.mu.Unlock()

	if client == nil {
		return nil, fmt.Errorf("redis %s: not connected", p.role)
	}

	result := make(map[string]map[string]any, len(keys))
	for _, key := range keys {
		// Get key type
		keyType, err := client.Type(ctx, key).Result()
		if err != nil {
			continue
		}

		// Get TTL
		ttl, err := client.TTL(ctx, key).Result()
		ttlSeconds := int64(0)
		if err == nil && ttl > 0 {
			ttlSeconds = int64(ttl.Seconds())
		}

		// Read value based on type
		var value any
		switch keyType {
		case "string":
			val, err := client.Get(ctx, key).Result()
			if err != nil {
				continue
			}
			value = val

		case "hash":
			val, err := client.HGetAll(ctx, key).Result()
			if err != nil {
				continue
			}
			value = val

		case "list":
			val, err := client.LRange(ctx, key, 0, -1).Result()
			if err != nil {
				continue
			}
			value = val

		case "set":
			val, err := client.SMembers(ctx, key).Result()
			if err != nil {
				continue
			}
			value = val

		case "zset":
			val, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
			if err != nil {
				continue
			}
			members := make([]map[string]any, len(val))
			for i, z := range val {
				members[i] = map[string]any{
					"member": z.Member,
					"score":  z.Score,
				}
			}
			value = members

		default:
			continue
		}

		result[key] = map[string]any{
			"type":  keyType,
			"value": value,
			"ttl":   ttlSeconds,
		}
	}

	return result, nil
}

// ComputeChecksums computes MD5 checksums for each requested key.
// It serializes the key's type, value, and TTL to deterministic JSON with
// sorted keys, then hashes the result.
func (p *RedisProvider) ComputeChecksums(ctx context.Context, keys []string) (map[string]string, error) {
	p.mu.Lock()
	client := p.client
	p.mu.Unlock()

	if client == nil {
		return nil, fmt.Errorf("redis %s: not connected", p.role)
	}

	result := make(map[string]string, len(keys))
	for _, key := range keys {
		// Read the record
		record, err := p.ReadRecords(ctx, []string{key})
		if err != nil || len(record) == 0 {
			continue
		}

		data, ok := record[key]
		if !ok {
			continue
		}

		jsonBytes, err := marshalChecksumRecord(data)
		if err != nil {
			continue
		}

		// Compute MD5 hash
		hash := sha256.Sum256(jsonBytes)
		result[key] = hex.EncodeToString(hash[:])
	}

	return result, nil
}

func marshalChecksumRecord(data map[string]any) ([]byte, error) {
	normalized := make(map[string]any, len(data))
	for k, v := range data {
		normalized[k] = normalizeChecksumValue(data["type"], v)
	}
	return json.Marshal(normalized)
}

func normalizeChecksumValue(keyType any, value any) any {
	switch v := value.(type) {
	case map[string]string:
		norm := make(map[string]any, len(v))
		for k, item := range v {
			norm[k] = item
		}
		return norm
	case map[string]any:
		norm := make(map[string]any, len(v))
		for k, item := range v {
			norm[k] = item
		}
		return norm
	case []string:
		if keyType == "set" {
			norm := append([]string(nil), v...)
			sort.Strings(norm)
			return norm
		}
		return v
	case []any:
		if keyType == "set" {
			norm := append([]any(nil), v...)
			sort.Slice(norm, func(i, j int) bool {
				return fmt.Sprint(norm[i]) < fmt.Sprint(norm[j])
			})
			return norm
		}
		return v
	default:
		return value
	}
}

// ---------------------------------------------------------------------------
// Config resolution
// ---------------------------------------------------------------------------

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
	cfg.Port = config.IntPtr(port)

	return &cfg, nil
}
