package transform

import (
	"context"
	"strings"

	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	RegisterTransformer("sqlite", "postgres", func() Transformer {
		return &SQLiteToPostgresTransformer{}
	})
	RegisterTransformer("sqlite", "mysql", func() Transformer {
		return &SQLiteToMySQLTransformer{}
	})
	RegisterTransformer("postgres", "sqlite", func() Transformer {
		return &PostgresToSQLiteTransformer{}
	})
	RegisterTransformer("mysql", "sqlite", func() Transformer {
		return &MySQLToSQLiteTransformer{}
	})
	RegisterTransformer("sqlite", "redis", func() Transformer {
		return &sqliteToRedisTransformer{}
	})
	RegisterTransformer("sqlite", "mongodb", func() Transformer {
		return &sqliteToMongoDBTransformer{}
	})
	RegisterTransformer("redis", "sqlite", func() Transformer {
		return &redisToSQLiteTransformer{}
	})
	RegisterTransformer("mongodb", "sqlite", func() Transformer {
		return &mongoDBToSQLiteTransformer{}
	})
}

// ---------------------------------------------------------------------------
// SQLite <-> SQL transformers
// ---------------------------------------------------------------------------

type SQLiteToPostgresTransformer struct {
	cfg TransformerConfig
}

func (t *SQLiteToPostgresTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	units, err := NewStagePipeline(
		NullHandlingStage(&t.cfg),
		FieldMappingStage(&t.cfg),
	).Transform(ctx, units)
	if err != nil {
		return nil, err
	}
	return AdjustSchemaField(units, true, "public")
}

func (t *SQLiteToPostgresTransformer) NeedsSchema() bool            { return false }
func (t *SQLiteToPostgresTransformer) SetSchema(_ *provider.Schema) {}
func (t *SQLiteToPostgresTransformer) TypeMapper() provider.TypeMapper {
	return SQLiteToPostgresTypeMapper{}
}
func (t *SQLiteToPostgresTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

type SQLiteToMySQLTransformer struct {
	cfg TransformerConfig
}

func (t *SQLiteToMySQLTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return NewStagePipeline(
		NullHandlingStage(&t.cfg),
		FieldMappingStage(&t.cfg),
	).Transform(ctx, units)
}

func (t *SQLiteToMySQLTransformer) NeedsSchema() bool            { return false }
func (t *SQLiteToMySQLTransformer) SetSchema(_ *provider.Schema) {}
func (t *SQLiteToMySQLTransformer) TypeMapper() provider.TypeMapper {
	return SQLiteToMySQLTypeMapper{}
}
func (t *SQLiteToMySQLTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

type PostgresToSQLiteTransformer struct {
	cfg TransformerConfig
}

func (t *PostgresToSQLiteTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	units, err := NewStagePipeline(
		NullHandlingStage(&t.cfg),
		FieldMappingStage(&t.cfg),
	).Transform(ctx, units)
	if err != nil {
		return nil, err
	}
	return AdjustSchemaField(units, false, "")
}

func (t *PostgresToSQLiteTransformer) NeedsSchema() bool            { return false }
func (t *PostgresToSQLiteTransformer) SetSchema(_ *provider.Schema) {}
func (t *PostgresToSQLiteTransformer) TypeMapper() provider.TypeMapper {
	return PostgresToSQLiteTypeMapper{}
}
func (t *PostgresToSQLiteTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

type MySQLToSQLiteTransformer struct {
	cfg TransformerConfig
}

func (t *MySQLToSQLiteTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return NewStagePipeline(
		NullHandlingStage(&t.cfg),
		FieldMappingStage(&t.cfg),
	).Transform(ctx, units)
}

func (t *MySQLToSQLiteTransformer) NeedsSchema() bool            { return false }
func (t *MySQLToSQLiteTransformer) SetSchema(_ *provider.Schema) {}
func (t *MySQLToSQLiteTransformer) TypeMapper() provider.TypeMapper {
	return MySQLToSQLiteTypeMapper{}
}
func (t *MySQLToSQLiteTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

// ---------------------------------------------------------------------------
// SQLite <-> NoSQL transformers (delegate to shared converters)
// ---------------------------------------------------------------------------

type sqliteToRedisTransformer struct {
	cfg TransformerConfig
}

func (t *sqliteToRedisTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return SQLToRedis(units, &t.cfg)
}

func (t *sqliteToRedisTransformer) NeedsSchema() bool               { return false }
func (t *sqliteToRedisTransformer) SetSchema(_ *provider.Schema)    {}
func (t *sqliteToRedisTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

type sqliteToMongoDBTransformer struct {
	cfg TransformerConfig
}

func (t *sqliteToMongoDBTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return SQLToMongoDB(units, &t.cfg)
}

func (t *sqliteToMongoDBTransformer) NeedsSchema() bool               { return false }
func (t *sqliteToMongoDBTransformer) SetSchema(_ *provider.Schema)    {}
func (t *sqliteToMongoDBTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

type redisToSQLiteTransformer struct {
	cfg TransformerConfig
}

func (t *redisToSQLiteTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return RedisToSQL(units, SQLEnvelopeConfig{}, &t.cfg)
}

func (t *redisToSQLiteTransformer) NeedsSchema() bool               { return false }
func (t *redisToSQLiteTransformer) SetSchema(_ *provider.Schema)    {}
func (t *redisToSQLiteTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

type mongoDBToSQLiteTransformer struct {
	cfg TransformerConfig
}

func (t *mongoDBToSQLiteTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return MongoDBToSQL(units, SQLEnvelopeConfig{}, &t.cfg)
}

func (t *mongoDBToSQLiteTransformer) NeedsSchema() bool               { return false }
func (t *mongoDBToSQLiteTransformer) SetSchema(_ *provider.Schema)    {}
func (t *mongoDBToSQLiteTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

// ---------------------------------------------------------------------------
// Type mappers (unchanged)
// ---------------------------------------------------------------------------

type SQLiteToPostgresTypeMapper struct{}

func (m SQLiteToPostgresTypeMapper) MapType(colType string) (string, bool) {
	upper := strings.ToUpper(colType)
	switch {
	case strings.HasPrefix(upper, "INTEGER") || strings.HasPrefix(upper, "INT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "REAL") || strings.HasPrefix(upper, "DOUBLE") || strings.HasPrefix(upper, "FLOAT"):
		return "DOUBLE PRECISION", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "TEXT") || strings.HasPrefix(upper, "VARCHAR") || strings.HasPrefix(upper, "CHAR") || strings.HasPrefix(upper, "CLOB"):
		return "TEXT", true
	case strings.HasPrefix(upper, "BLOB"):
		return "BYTEA", true
	case upper == "DATETIME" || strings.HasPrefix(upper, "TIMESTAMP"):
		return "TIMESTAMP", true
	case upper == "DATE":
		return "DATE", true
	case upper == "BOOLEAN":
		return "BOOLEAN", true
	}
	return "", false
}

type SQLiteToMySQLTypeMapper struct{}

func (m SQLiteToMySQLTypeMapper) MapType(colType string) (string, bool) {
	upper := strings.ToUpper(colType)
	switch {
	case strings.HasPrefix(upper, "INTEGER") || strings.HasPrefix(upper, "INT"):
		return "INT", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "REAL") || strings.HasPrefix(upper, "FLOAT"):
		return "FLOAT", true
	case strings.HasPrefix(upper, "DOUBLE"):
		return "DOUBLE", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "DECIMAL", true
	case strings.HasPrefix(upper, "TEXT") || strings.HasPrefix(upper, "CLOB"):
		return "LONGTEXT", true
	case strings.HasPrefix(upper, "VARCHAR"):
		return "VARCHAR(255)", true
	case strings.HasPrefix(upper, "CHAR"):
		return "CHAR(255)", true
	case strings.HasPrefix(upper, "BLOB"):
		return "LONGBLOB", true
	case upper == "DATETIME" || strings.HasPrefix(upper, "TIMESTAMP"):
		return "DATETIME", true
	case upper == "DATE":
		return "DATE", true
	case upper == "BOOLEAN":
		return "TINYINT(1)", true
	}
	return "", false
}

type PostgresToSQLiteTypeMapper struct{}

func (m PostgresToSQLiteTypeMapper) MapType(colType string) (string, bool) {
	upper := strings.ToUpper(colType)
	switch {
	case strings.HasPrefix(upper, "SERIAL"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "BIGSERIAL"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "INTEGER") || strings.HasPrefix(upper, "INT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "REAL"):
		return "REAL", true
	case strings.HasPrefix(upper, "DOUBLE PRECISION"):
		return "REAL", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "CHARACTER VARYING") || strings.HasPrefix(upper, "VARCHAR"):
		return "TEXT", true
	case strings.HasPrefix(upper, "CHARACTER") || strings.HasPrefix(upper, "CHAR"):
		return "TEXT", true
	case strings.HasPrefix(upper, "TEXT"):
		return "TEXT", true
	case strings.HasPrefix(upper, "BYTEA"):
		return "BLOB", true
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return "TEXT", true
	case upper == "DATE":
		return "TEXT", true
	case upper == "TIME":
		return "TEXT", true
	case strings.HasPrefix(upper, "BOOL"):
		return "INTEGER", true
	case upper == "JSON" || upper == "JSONB":
		return "TEXT", true
	case upper == "UUID":
		return "TEXT", true
	}
	return "TEXT", true
}

type MySQLToSQLiteTypeMapper struct{}

func (m MySQLToSQLiteTypeMapper) MapType(colType string) (string, bool) {
	upper := strings.ToUpper(colType)
	switch {
	case strings.HasPrefix(upper, "TINYINT") || strings.HasPrefix(upper, "SMALLINT") ||
		strings.HasPrefix(upper, "MEDIUMINT") || strings.HasPrefix(upper, "INT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "FLOAT"):
		return "REAL", true
	case strings.HasPrefix(upper, "DOUBLE"):
		return "REAL", true
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "VARCHAR") || strings.HasPrefix(upper, "CHAR") ||
		strings.HasPrefix(upper, "TINYTEXT") || strings.HasPrefix(upper, "MEDIUMTEXT") ||
		strings.HasPrefix(upper, "LONGTEXT") || strings.HasPrefix(upper, "TEXT") ||
		strings.HasPrefix(upper, "ENUM") || strings.HasPrefix(upper, "SET"):
		return "TEXT", true
	case strings.HasPrefix(upper, "TINYBLOB") || strings.HasPrefix(upper, "MEDIUMBLOB") ||
		strings.HasPrefix(upper, "LONGBLOB") || strings.HasPrefix(upper, "BLOB") ||
		strings.HasPrefix(upper, "BINARY") || strings.HasPrefix(upper, "VARBINARY"):
		return "BLOB", true
	case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP"):
		return "TEXT", true
	case upper == "DATE":
		return "TEXT", true
	case upper == "TIME":
		return "TEXT", true
	case upper == "YEAR":
		return "INTEGER", true
	case strings.HasPrefix(upper, "JSON"):
		return "TEXT", true
	case strings.HasPrefix(upper, "BOOL"):
		return "INTEGER", true
	}
	return "TEXT", true
}
