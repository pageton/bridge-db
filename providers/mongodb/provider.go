//go:build mongodb

// Package mongodb implements the MongoDB migration provider for bridge-db.
// It supports document-based migrations with collection enumeration,
// cursor-based scanning, and index preservation.
package mongodb

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	provider.Register("mongodb", func() provider.Provider {
		return &MongoDBProvider{}
	})
}

// MongoDBProvider implements provider.Provider for MongoDB databases.
// A single instance connects to one MongoDB instance (either source or destination).
type MongoDBProvider struct {
	mu       sync.Mutex
	client   *mongo.Client
	database *mongo.Database
	cfg      *config.MongoDBConfig
	role     string // "source" or "destination"
}

// Name returns the provider identifier.
func (p *MongoDBProvider) Name() string {
	return "mongodb"
}

// Connect establishes a connection to a MongoDB instance.
//
// The pipeline calls Connect in two ways:
//   - Source:      Connect(ctx, srcConfig, nil)
//   - Destination: Connect(ctx, nil, dstConfig)
//
// srcConfig/dstConfig can be either:
//   - *config.MongoDBConfig  (direct connection)
//   - map[string]string      (tunnel-resolved address)
func (p *MongoDBProvider) Connect(_ context.Context, srcConfig, dstConfig any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	raw := srcConfig
	if raw == nil {
		raw = dstConfig
		p.role = "destination"
	} else {
		p.role = "source"
	}

	cfg, err := resolveMongoDBConfig(raw)
	if err != nil {
		return fmt.Errorf("mongodb %s: %w", p.role, err)
	}
	p.cfg = cfg

	// Build connection URI
	uri := buildMongoDBURI(cfg)

	clientOpts := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return fmt.Errorf("mongodb %s connect: %w", p.role, err)
	}
	p.client = client
	p.database = client.Database(cfg.Database)

	log := logger.L().With("provider", "mongodb", "role", p.role)
	log.Info("configured mongodb client", "host", cfg.Host, "port", cfg.Port, "database", cfg.Database)

	return nil
}

// Close releases the MongoDB connection.
func (p *MongoDBProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		err := p.client.Disconnect(context.Background())
		p.client = nil
		p.database = nil
		return err
	}
	return nil
}

// Ping verifies the MongoDB connection is alive.
func (p *MongoDBProvider) Ping(ctx context.Context) error {
	p.mu.Lock()
	client := p.client
	p.mu.Unlock()

	if client == nil {
		return fmt.Errorf("mongodb %s: not connected", p.role)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("mongodb %s ping: %w", p.role, err)
	}
	return nil
}

// Scanner returns a Scanner that enumerates documents from the source MongoDB.
func (p *MongoDBProvider) Scanner(ctx context.Context, opts provider.ScanOptions) provider.Scanner {
	return newMongoDBScanner(p.database, opts)
}

// Writer returns a Writer that persists documents to the destination MongoDB.
func (p *MongoDBProvider) Writer(ctx context.Context, opts provider.WriteOptions) provider.Writer {
	return newMongoDBWriter(p.database, opts)
}

// Verifier returns a Verifier for post-migration checks.
func (p *MongoDBProvider) Verifier(ctx context.Context) provider.Verifier {
	return newMongoDBVerifier(p.database)
}

// SchemaMigrator returns a SchemaMigrator for MongoDB index migration.
func (p *MongoDBProvider) SchemaMigrator(ctx context.Context) provider.SchemaMigrator {
	return newMongoDBSchemaMigrator(p.database)
}

// DryRun returns a wrapped Provider that logs writes without executing them.
func (p *MongoDBProvider) DryRun() provider.Provider {
	return &dryRunProvider{inner: p}
}

// Database returns the underlying MongoDB database (for internal use by sub-components).
func (p *MongoDBProvider) Database() *mongo.Database {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.database
}

// ---------------------------------------------------------------------------
// Config resolution
// ---------------------------------------------------------------------------

// resolveMongoDBConfig handles both direct config and tunnel-resolved address.
func resolveMongoDBConfig(raw any) (*config.MongoDBConfig, error) {
	switch v := raw.(type) {
	case *config.MongoDBConfig:
		if v == nil {
			return nil, fmt.Errorf("nil mongodb config")
		}
		return v, nil

	case config.MongoDBConfig:
		return &v, nil

	case map[string]string:
		return mongoDBConfigFromMap(v)

	default:
		return nil, fmt.Errorf("unsupported mongodb config type: %T", raw)
	}
}

// mongoDBConfigFromMap builds a MongoDBConfig from a tunnel-resolved address map.
func mongoDBConfigFromMap(m map[string]string) (*config.MongoDBConfig, error) {
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

	cfg := config.DefaultMongoDBConfig()
	cfg.Host = host
	cfg.Port = port

	// Copy database name if provided
	if db := m["database"]; db != "" {
		cfg.Database = db
	}

	return &cfg, nil
}

// buildMongoDBURI constructs a MongoDB connection URI from config.
func buildMongoDBURI(cfg *config.MongoDBConfig) string {
	uri := "mongodb://"

	if cfg.Username != "" {
		uri += cfg.Username
		if cfg.Password != "" {
			uri += ":" + cfg.Password
		}
		uri += "@"
	}

	uri += fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	if cfg.Database != "" {
		uri += "/" + cfg.Database
	}

	if cfg.AuthSource != "" && cfg.AuthSource != "admin" {
		uri += "?authSource=" + cfg.AuthSource
	}

	return uri
}
