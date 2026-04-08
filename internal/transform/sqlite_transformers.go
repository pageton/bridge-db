package transform

import (
	"context"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
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
		return &SQLiteToRedisTransformer{}
	})
	RegisterTransformer("sqlite", "mongodb", func() Transformer {
		return &SQLiteToMongoDBTransformer{}
	})
	RegisterTransformer("redis", "sqlite", func() Transformer {
		return &RedisToSQLiteTransformer{}
	})
	RegisterTransformer("mongodb", "sqlite", func() Transformer {
		return &MongoDBToSQLiteTransformer{}
	})
}

// --- SQLite <-> SQL transformers ---
// SQLite uses the same row envelope as MySQL/PostgreSQL.
// These transformers primarily handle type mapping and timestamp format conversion.

type SQLiteToPostgresTransformer struct{}

func (t *SQLiteToPostgresTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, len(units))
	for i, unit := range units {
		var envelope map[string]any
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			result[i] = unit
			continue
		}
		envelope["schema"] = "public"
		encoded, err := sonic.Marshal(envelope)
		if err != nil {
			result[i] = unit
			continue
		}
		unit.Data = encoded
		result[i] = unit
	}
	return result, nil
}

func (t *SQLiteToPostgresTransformer) NeedsSchema() bool            { return false }
func (t *SQLiteToPostgresTransformer) SetSchema(_ *provider.Schema) {}
func (t *SQLiteToPostgresTransformer) TypeMapper() provider.TypeMapper {
	return SQLiteToPostgresTypeMapper{}
}

type SQLiteToMySQLTransformer struct{}

func (t *SQLiteToMySQLTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return units, nil
}

func (t *SQLiteToMySQLTransformer) NeedsSchema() bool            { return false }
func (t *SQLiteToMySQLTransformer) SetSchema(_ *provider.Schema) {}
func (t *SQLiteToMySQLTransformer) TypeMapper() provider.TypeMapper {
	return SQLiteToMySQLTypeMapper{}
}

type PostgresToSQLiteTransformer struct{}

func (t *PostgresToSQLiteTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, len(units))
	for i, unit := range units {
		var envelope map[string]any
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			result[i] = unit
			continue
		}
		delete(envelope, "schema")
		encoded, err := sonic.Marshal(envelope)
		if err != nil {
			result[i] = unit
			continue
		}
		unit.Data = encoded
		result[i] = unit
	}
	return result, nil
}

func (t *PostgresToSQLiteTransformer) NeedsSchema() bool            { return false }
func (t *PostgresToSQLiteTransformer) SetSchema(_ *provider.Schema) {}
func (t *PostgresToSQLiteTransformer) TypeMapper() provider.TypeMapper {
	return PostgresToSQLiteTypeMapper{}
}

type MySQLToSQLiteTransformer struct{}

func (t *MySQLToSQLiteTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return units, nil
}

func (t *MySQLToSQLiteTransformer) NeedsSchema() bool            { return false }
func (t *MySQLToSQLiteTransformer) SetSchema(_ *provider.Schema) {}
func (t *MySQLToSQLiteTransformer) TypeMapper() provider.TypeMapper {
	return MySQLToSQLiteTypeMapper{}
}

// --- Type mappers ---

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

// --- SQLite <-> NoSQL transformers ---

type SQLiteToRedisTransformer struct{}

func (t *SQLiteToRedisTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, 0, len(units))
	for _, unit := range units {
		var envelope map[string]any
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			continue
		}

		data, _ := envelope["data"].(map[string]any)
		if data == nil {
			continue
		}

		fields := make(map[string]any)
		for k, v := range data {
			switch val := v.(type) {
			case map[string]any, []any:
				b, _ := sonic.Marshal(val)
				fields[k] = string(b)
			default:
				fields[k] = v
			}
		}

		redisEnvelope := map[string]any{
			"type":        "hash",
			"value":       fields,
			"ttl_seconds": 0,
		}

		encoded, err := sonic.Marshal(redisEnvelope)
		if err != nil {
			continue
		}

		result = append(result, provider.MigrationUnit{
			Key:      unit.Key,
			Table:    unit.Table,
			DataType: provider.DataTypeHash,
			Data:     encoded,
			Size:     int64(len(encoded)),
		})
	}
	return result, nil
}

func (t *SQLiteToRedisTransformer) NeedsSchema() bool            { return false }
func (t *SQLiteToRedisTransformer) SetSchema(_ *provider.Schema) {}

type SQLiteToMongoDBTransformer struct{}

func (t *SQLiteToMongoDBTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, 0, len(units))
	for _, unit := range units {
		var envelope map[string]any
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			continue
		}

		data, _ := envelope["data"].(map[string]any)
		table, _ := envelope["table"].(string)
		if data == nil || table == "" {
			continue
		}

		pk, _ := envelope["primary_key"].(map[string]any)
		docID := "unknown"
		if len(pk) > 0 {
			for _, v := range pk {
				docID = strings.ReplaceAll(strings.ReplaceAll(
					fmt.Sprintf("%v", v), " ", "_"), ":", "_")
				break
			}
		}

		doc := make(map[string]any, len(data))
		for k, v := range data {
			doc[k] = v
		}
		doc["_id"] = unit.Key

		mongoEnvelope := map[string]any{
			"collection":  table,
			"document_id": docID,
			"document":    doc,
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

func (t *SQLiteToMongoDBTransformer) NeedsSchema() bool            { return false }
func (t *SQLiteToMongoDBTransformer) SetSchema(_ *provider.Schema) {}

type RedisToSQLiteTransformer struct{}

func (t *RedisToSQLiteTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, 0, len(units))
	for _, unit := range units {
		var rd struct {
			Type       string `json:"type"`
			Value      any    `json:"value"`
			TTLSeconds int64  `json:"ttl_seconds"`
		}
		if err := sonic.Unmarshal(unit.Data, &rd); err != nil {
			continue
		}

		fields, ok := rd.Value.(map[string]any)
		if !ok {
			b, _ := sonic.Marshal(map[string]any{"value": rd.Value})
			fields = map[string]any{"value": string(b)}
		}

		data := make(map[string]any, len(fields))
		columnTypes := make(map[string]string, len(fields))
		for k, v := range fields {
			data[k] = v
			columnTypes[k] = "TEXT"
		}
		data["_key"] = unit.Key
		columnTypes["_key"] = "TEXT"

		table := "redis_data"
		if unit.Table != "" {
			table = unit.Table
		}

		pk := map[string]any{"_key": unit.Key}

		sqLiteEnvelope := map[string]any{
			"table":        table,
			"primary_key":  pk,
			"data":         data,
			"column_types": columnTypes,
		}

		encoded, err := sonic.Marshal(sqLiteEnvelope)
		if err != nil {
			continue
		}

		result = append(result, provider.MigrationUnit{
			Key:      unit.Key,
			Table:    table,
			DataType: provider.DataTypeRow,
			Data:     encoded,
			Size:     int64(len(encoded)),
		})
	}
	return result, nil
}

func (t *RedisToSQLiteTransformer) NeedsSchema() bool            { return false }
func (t *RedisToSQLiteTransformer) SetSchema(_ *provider.Schema) {}

type MongoDBToSQLiteTransformer struct{}

func (t *MongoDBToSQLiteTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, 0, len(units))
	for _, unit := range units {
		var envelope map[string]any
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			continue
		}

		doc, ok := envelope["document"].(map[string]any)
		collection, _ := envelope["collection"].(string)
		if !ok || collection == "" {
			continue
		}

		data := make(map[string]any, len(doc))
		columnTypes := make(map[string]string, len(doc))
		for k, v := range doc {
			switch val := v.(type) {
			case map[string]any, []any:
				b, _ := sonic.Marshal(val)
				data[k] = string(b)
			default:
				data[k] = v
			}
			columnTypes[k] = "TEXT"
		}

		pk := map[string]any{"_id": unit.Key}

		sqLiteEnvelope := map[string]any{
			"table":        collection,
			"primary_key":  pk,
			"data":         data,
			"column_types": columnTypes,
		}

		encoded, err := sonic.Marshal(sqLiteEnvelope)
		if err != nil {
			continue
		}

		result = append(result, provider.MigrationUnit{
			Key:      unit.Key,
			Table:    collection,
			DataType: provider.DataTypeRow,
			Data:     encoded,
			Size:     int64(len(encoded)),
		})
	}
	return result, nil
}

func (t *MongoDBToSQLiteTransformer) NeedsSchema() bool            { return false }
func (t *MongoDBToSQLiteTransformer) SetSchema(_ *provider.Schema) {}
