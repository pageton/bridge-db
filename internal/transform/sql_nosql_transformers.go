package transform

import (
	"context"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	RegisterTransformer("postgres", "mongodb", func() Transformer { return &SQLToMongoDBWithFK{cfg: TransformerConfig{}} })
	RegisterTransformer("postgres", "redis", func() Transformer { return &sqlToRedisTransformer{} })
	RegisterTransformer("mysql", "mongodb", func() Transformer { return &SQLToMongoDBWithFK{cfg: TransformerConfig{}} })
	RegisterTransformer("mysql", "redis", func() Transformer { return &sqlToRedisTransformer{} })
	RegisterTransformer("mongodb", "mysql", func() Transformer { return &mongoToMySQLTransformer{} })
	RegisterTransformer("redis", "mysql", func() Transformer { return &redisToMySQLTransformer{} })
	RegisterTransformer("redis", "postgres", func() Transformer { return &redisToPostgresTransformer{} })
}

// SQLToMongoDBWithFK extends SQLToMongoDB with $lookup metadata for foreign keys.
type SQLToMongoDBWithFK struct {
	cfg    TransformerConfig
	schema *provider.Schema
}

func (t *SQLToMongoDBWithFK) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	// Build FK lookup map from schema
	fkLookups := t.buildFKLookups()

	result := make([]provider.MigrationUnit, 0, len(units))
	for _, unit := range units {
		var envelope map[string]any
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			continue
		}

		table, _ := envelope["table"].(string)
		lookups := fkLookups[table]

		doc, _ := envelope["data"].(map[string]any) // SQL envelope uses "data", not "document"
		if doc == nil {
			continue
		}

		// Apply null handler
		if t.cfg.NullHandler != nil {
			var err error
			doc, err = t.cfg.NullHandler.Apply(doc)
			if err != nil {
				continue
			}
		}

		// Apply field mappings
		if t.cfg.FieldMapping != nil {
			var err error
			doc, err = t.cfg.FieldMapping.Apply(table, doc)
			if err != nil {
				continue
			}
		}

		// Add $lookup metadata if this table has foreign keys
		if len(lookups) > 0 {
			lookupMeta := make([]map[string]any, 0, len(lookups))
			for _, fk := range lookups {
				// Check if the local field exists in the document
				localField := fk.Columns[0]
				if _, exists := doc[localField]; exists {
					lookupMeta = append(lookupMeta, map[string]any{
						"from":    fk.ReferencedTable,
						"local":   localField,
						"foreign": fk.ReferencedColumns[0],
						"as":      fk.ReferencedTable,
					})
				}
			}

			if len(lookupMeta) > 0 {
				envelope["lookups"] = lookupMeta
			}
		}

		// Preserve backward compatibility: _id should be in document
		doc["_id"] = unit.Key

		mongoEnvelope := map[string]any{
			"collection":  table,
			"document_id": unit.Key,
			"document":    doc,
		}

		if lookups, ok := envelope["lookups"].([]map[string]any); ok {
			mongoEnvelope["lookups"] = lookups
		}

		encoded, err := sonic.Marshal(mongoEnvelope)
		if err != nil {
			continue
		}

		result = append(result, provider.MigrationUnit{
			Key:      unit.Key,
			Table:    table,
			DataType: provider.DataTypeDocument,
			Data:     encoded,
			Size:     int64(len(encoded)),
		})
	}
	return result, nil
}

func (t *SQLToMongoDBWithFK) buildFKLookups() map[string][]provider.ForeignKey {
	if t.schema == nil {
		return nil
	}

	result := make(map[string][]provider.ForeignKey)
	for _, table := range t.schema.Tables {
		if len(table.ForeignKeys) > 0 {
			result[table.Name] = table.ForeignKeys
		}
	}
	return result
}

func (t *SQLToMongoDBWithFK) NeedsSchema() bool { return true }

func (t *SQLToMongoDBWithFK) SetSchema(schema *provider.Schema) {
	t.schema = schema
}

func (t *SQLToMongoDBWithFK) Configure(cfg TransformerConfig) {
	t.cfg = cfg
}

type sqlToRedisTransformer struct{ cfg TransformerConfig }

func (t *sqlToRedisTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return SQLToRedis(units, &t.cfg)
}
func (t *sqlToRedisTransformer) NeedsSchema() bool               { return false }
func (t *sqlToRedisTransformer) SetSchema(_ *provider.Schema)    {}
func (t *sqlToRedisTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

type mongoToMySQLTransformer struct{ cfg TransformerConfig }

func (t *mongoToMySQLTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return MongoDBToSQL(units, SQLEnvelopeConfig{}, &t.cfg)
}
func (t *mongoToMySQLTransformer) NeedsSchema() bool               { return false }
func (t *mongoToMySQLTransformer) SetSchema(_ *provider.Schema)    {}
func (t *mongoToMySQLTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

type redisToMySQLTransformer struct{ cfg TransformerConfig }

func (t *redisToMySQLTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return RedisToSQL(units, SQLEnvelopeConfig{}, &t.cfg)
}
func (t *redisToMySQLTransformer) NeedsSchema() bool               { return false }
func (t *redisToMySQLTransformer) SetSchema(_ *provider.Schema)    {}
func (t *redisToMySQLTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

type redisToPostgresTransformer struct{ cfg TransformerConfig }

func (t *redisToPostgresTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return RedisToSQL(units, SQLEnvelopeConfig{SchemaName: "public"}, &t.cfg)
}
func (t *redisToPostgresTransformer) NeedsSchema() bool               { return false }
func (t *redisToPostgresTransformer) SetSchema(_ *provider.Schema)    {}
func (t *redisToPostgresTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }
