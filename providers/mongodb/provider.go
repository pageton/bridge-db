//go:build mongodb

// Package mongodb implements the MongoDB migration provider for bridge-db.
// It supports document-based migrations with collection enumeration,
// cursor-based scanning, and index preservation.
package mongodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bytedance/sonic"
	"go.mongodb.org/mongo-driver/v2/bson"
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

	cfg, err := config.ResolveConfig(raw, "mongodb", mongoDBConfigFromMap)
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
	log.Debug("configured mongodb client", "host", cfg.Host, "port", cfg.GetPort(), "database", cfg.Database)

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

// Capabilities declares what the mongodb provider supports.
func (p *MongoDBProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Schema: true, Transactions: true, Verification: provider.VerifyCross, Incremental: true}
}

// Database returns the underlying MongoDB database (for internal use by sub-components).
func (p *MongoDBProvider) Database() *mongo.Database {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.database
}

// ---------------------------------------------------------------------------
// Cross-verification interfaces
// ---------------------------------------------------------------------------

// EnumerateTables returns collection names and their document counts.
// System collections (names starting with '.') are excluded.
func (p *MongoDBProvider) EnumerateTables(ctx context.Context) (map[string]int64, error) {
	p.mu.Lock()
	db := p.database
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mongodb %s: not connected", p.role)
	}

	cursor, err := db.ListCollections(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	result := make(map[string]int64)
	for cursor.Next(ctx) {
		var collInfo bson.M
		if err := cursor.Decode(&collInfo); err != nil {
			continue
		}

		name, ok := collInfo["name"].(string)
		if !ok || len(name) == 0 || name[0] == '.' {
			continue
		}

		count, err := db.Collection(name).CountDocuments(ctx, bson.M{})
		if err != nil {
			continue
		}
		result[name] = count
	}

	return result, nil
}

// ReadRecords reads specific documents by their keys.
// Keys are in the format "collection:documentID".
func (p *MongoDBProvider) ReadRecords(ctx context.Context, keys []string) (map[string]map[string]any, error) {
	p.mu.Lock()
	db := p.database
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mongodb %s: not connected", p.role)
	}

	result := make(map[string]map[string]any, len(keys))
	for _, key := range keys {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		collection, idStr := parts[0], parts[1]

		// Resolve the document ID (try ObjectID for 24-char hex strings)
		var docID any = idStr
		if len(idStr) == 24 {
			if objID, err := bson.ObjectIDFromHex(idStr); err == nil {
				docID = objID
			}
		}

		var doc bson.M
		if err := db.Collection(collection).FindOne(ctx, bson.M{"_id": docID}).Decode(&doc); err != nil {
			continue
		}

		// Convert bson.M to map[string]any
		record := make(map[string]any, len(doc))
		for k, v := range doc {
			record[k] = v
		}
		result[key] = record
	}

	return result, nil
}

// ComputeChecksums computes MD5 checksums for each requested document.
// It serializes the document to canonical JSON with sorted keys, then hashes
// the result.
func (p *MongoDBProvider) ComputeChecksums(ctx context.Context, keys []string) (map[string]string, error) {
	p.mu.Lock()
	db := p.database
	p.mu.Unlock()

	if db == nil {
		return nil, fmt.Errorf("mongodb %s: not connected", p.role)
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

		// Serialize to deterministic JSON
		jsonBytes, err := sonic.Marshal(canonicalizeMongoValue(data))
		if err != nil {
			continue
		}

		// Compute MD5 hash
		hash := sha256.Sum256(jsonBytes)
		result[key] = hex.EncodeToString(hash[:])
	}

	return result, nil
}

type canonicalKV struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func canonicalizeMongoValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]canonicalKV, 0, len(keys))
		for _, k := range keys {
			out = append(out, canonicalKV{Key: k, Value: canonicalizeMongoValue(x[k])})
		}
		return out
	case bson.M:
		m := make(map[string]any, len(x))
		for k, v := range x {
			m[k] = v
		}
		return canonicalizeMongoValue(m)
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = canonicalizeMongoValue(x[i])
		}
		return out
	case bson.A:
		out := make([]any, len(x))
		for i := range x {
			out[i] = canonicalizeMongoValue(x[i])
		}
		return out
	case bson.ObjectID:
		return x.Hex()
	default:
		return v
	}
}

// ---------------------------------------------------------------------------
// Config resolution
// ---------------------------------------------------------------------------

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
	cfg.Port = config.IntPtr(port)

	// Copy database name if provided
	if db := m["database"]; db != "" {
		cfg.Database = db
	}

	return &cfg, nil
}

// buildMongoDBURI constructs a MongoDB connection URI from config.
// Uses url.UserPassword for proper URL-encoding of credentials.
func buildMongoDBURI(cfg *config.MongoDBConfig) string {
	uri := "mongodb://"

	if cfg.Username != "" {
		uri += url.UserPassword(cfg.Username, cfg.Password).String()
		uri += "@"
	}

	port := 27017
	if cfg.Port != nil {
		port = cfg.GetPort()
	}
	uri += fmt.Sprintf("%s:%d", cfg.Host, port)

	if cfg.Database != "" {
		uri += "/" + cfg.Database
	}

	if cfg.AuthSource != "" && cfg.AuthSource != "admin" {
		uri += "?authSource=" + cfg.AuthSource
	}

	if cfg.GetTLS() {
		if strings.Contains(uri, "?") {
			uri += "&tls=true"
		} else {
			uri += "?tls=true"
		}
	}

	return uri
}
