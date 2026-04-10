//go:build mongodb

package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// mongoDBSchemaMigrator handles index migration for MongoDB databases.
// It can inspect source indexes and create them on the destination.
type mongoDBSchemaMigrator struct {
	database *mongo.Database
	log      interface {
		Debug(msg string, args ...any)
	}
}

func newMongoDBSchemaMigrator(database *mongo.Database) *mongoDBSchemaMigrator {
	return &mongoDBSchemaMigrator{
		database: database,
		log:      logger.L().With("component", "mongodb-schema"),
	}
}

// Inspect reads the schema (indexes) from the source database.
func (m *mongoDBSchemaMigrator) Inspect(ctx context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}

	// List collections
	cursor, err := m.database.ListCollections(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			continue
		}

		name, ok := result["name"].(string)
		if !ok || len(name) == 0 || name[0] == '.' {
			continue
		}

		// Get indexes for this collection
		indexes, err := m.getCollectionIndexes(ctx, name)
		if err != nil {
			m.log.Debug("failed to get indexes", "collection", name, "error", err)
			continue
		}

		tableSchema := provider.TableSchema{
			Name:    name,
			Indexes: indexes,
		}
		schema.Tables = append(schema.Tables, tableSchema)
	}

	return schema, nil
}

// Create applies the given schema to the destination database.
// For MongoDB, this creates indexes on each collection.
func (m *mongoDBSchemaMigrator) Create(ctx context.Context, schema *provider.Schema, mapper provider.TypeMapper) error {
	for _, table := range schema.Tables {
		if err := m.createCollectionIndexes(ctx, table.Name, table.Indexes); err != nil {
			return fmt.Errorf("create indexes for %s: %w", table.Name, err)
		}
	}
	return nil
}

// Drop removes all indexes (except _id) from all collections.
func (m *mongoDBSchemaMigrator) Drop(ctx context.Context, schema *provider.Schema) error {
	for _, table := range schema.Tables {
		if err := m.dropCollectionIndexes(ctx, table.Name); err != nil {
			return fmt.Errorf("drop indexes for %s: %w", table.Name, err)
		}
	}
	return nil
}

// getCollectionIndexes retrieves all indexes for a collection.
func (m *mongoDBSchemaMigrator) getCollectionIndexes(ctx context.Context, collection string) ([]provider.IndexSchema, error) {
	coll := m.database.Collection(collection)

	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()

	var indexes []provider.IndexSchema

	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			continue
		}

		name, _ := index["name"].(string)
		if name == "_id_" {
			// Skip the default _id index
			continue
		}

		// Extract key fields
		keys, ok := index["key"].(bson.M)
		if !ok {
			continue
		}

		var columns []string
		for key := range keys {
			columns = append(columns, key)
		}

		unique, _ := index["unique"].(bool)

		indexSchema := provider.IndexSchema{
			Name:    name,
			Columns: columns,
			Unique:  unique,
		}
		indexes = append(indexes, indexSchema)
	}

	return indexes, nil
}

// createCollectionIndexes creates indexes on a collection.
func (m *mongoDBSchemaMigrator) createCollectionIndexes(ctx context.Context, collection string, indexes []provider.IndexSchema) error {
	if len(indexes) == 0 {
		return nil
	}

	coll := m.database.Collection(collection)

	var indexModels []mongo.IndexModel

	for _, idx := range indexes {
		// Build index keys
		keys := bson.D{}
		for _, col := range idx.Columns {
			keys = append(keys, bson.E{Key: col, Value: 1})
		}

		indexModel := mongo.IndexModel{
			Keys: keys,
		}

		// Set index options
		indexOpts := options.Index()
		if idx.Name != "" {
			indexOpts.SetName(idx.Name)
		}
		if idx.Unique {
			indexOpts.SetUnique(true)
		}

		indexModel.Options = indexOpts
		indexModels = append(indexModels, indexModel)
	}

	// Create indexes
	_, err := coll.Indexes().CreateMany(ctx, indexModels)
	if err != nil {
		return err
	}

	m.log.Debug("created indexes", "collection", collection, "count", len(indexModels))
	return nil
}

// dropCollectionIndexes drops all non-_id indexes from a collection.
func (m *mongoDBSchemaMigrator) dropCollectionIndexes(ctx context.Context, collection string) error {
	coll := m.database.Collection(collection)

	// List existing indexes
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close(ctx) }()

	var indexNames []string
	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			continue
		}

		name, _ := index["name"].(string)
		if name != "_id_" {
			indexNames = append(indexNames, name)
		}
	}

	// Drop each index
	for _, name := range indexNames {
		if err := coll.Indexes().DropOne(ctx, name); err != nil {
			m.log.Debug("failed to drop index", "collection", collection, "index", name, "error", err)
		}
	}

	return nil
}
