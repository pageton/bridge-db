package transform

import (
	"context"

	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	RegisterTransformer("postgres", "mongodb", func() Transformer { return &sqlToMongoTransformer{} })
	RegisterTransformer("postgres", "redis", func() Transformer { return &sqlToRedisTransformer{} })
	RegisterTransformer("mysql", "mongodb", func() Transformer { return &sqlToMongoTransformer{} })
	RegisterTransformer("mysql", "redis", func() Transformer { return &sqlToRedisTransformer{} })
	RegisterTransformer("mongodb", "mysql", func() Transformer { return &mongoToMySQLTransformer{} })
	RegisterTransformer("redis", "mysql", func() Transformer { return &redisToMySQLTransformer{} })
	RegisterTransformer("redis", "postgres", func() Transformer { return &redisToPostgresTransformer{} })
}

type sqlToMongoTransformer struct{ cfg TransformerConfig }

func (t *sqlToMongoTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return SQLToMongoDB(units, &t.cfg)
}
func (t *sqlToMongoTransformer) NeedsSchema() bool               { return false }
func (t *sqlToMongoTransformer) SetSchema(_ *provider.Schema)    {}
func (t *sqlToMongoTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

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
