package transform

import (
	"context"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	pairs := []struct{ src, dst string }{
		{"cockroachdb", "postgres"},
		{"cockroachdb", "mysql"},
		{"cockroachdb", "sqlite"},
		{"cockroachdb", "redis"},
		{"cockroachdb", "mongodb"},
		{"postgres", "cockroachdb"},
		{"mysql", "cockroachdb"},
		{"sqlite", "cockroachdb"},
		{"redis", "cockroachdb"},
		{"mongodb", "cockroachdb"},
	}
	for _, p := range pairs {
		src, dst := p.src, p.dst
		RegisterTransformer(src, dst, func() Transformer {
			return &cockroachDBPassthroughTransformer{src: src, dst: dst}
		})
	}
}

type cockroachDBPassthroughTransformer struct {
	src string
	dst string
}

func (t *cockroachDBPassthroughTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	switch t.dst {
	case "redis":
		return cockroachDBToRedis(units)
	case "mongodb":
		return cockroachDBToMongoDB(units)
	case "sqlite":
		return cockroachDBToSQLite(units)
	default:
		return units, nil
	}
}

func (t *cockroachDBPassthroughTransformer) NeedsSchema() bool            { return false }
func (t *cockroachDBPassthroughTransformer) SetSchema(_ *provider.Schema) {}
func (t *cockroachDBPassthroughTransformer) TypeMapper() provider.TypeMapper {
	return cockroachDBTypeMapper{src: t.src, dst: t.dst}
}

func cockroachDBToRedis(units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
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
			Key: unit.Key, Table: unit.Table,
			DataType: provider.DataTypeHash,
			Data:     encoded, Size: int64(len(encoded)),
		})
	}
	return result, nil
}

func cockroachDBToMongoDB(units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
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
				s := fmt.Sprintf("%v", v)
				s = strings.ReplaceAll(s, " ", "_")
				s = strings.ReplaceAll(s, ":", "_")
				s = strings.ReplaceAll(s, ".", "_")
				s = strings.ReplaceAll(s, "/", "_")
				docID = s
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
			Key: unit.Key, Table: table,
			DataType: provider.DataTypeDocument,
			Data:     encoded, Size: int64(len(encoded)),
		})
	}
	return result, nil
}

func cockroachDBToSQLite(units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
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

type cockroachDBTypeMapper struct {
	src string
	dst string
}

func (m cockroachDBTypeMapper) MapType(colType string) (string, bool) {
	upper := strings.ToUpper(colType)
	switch m.dst {
	case "mysql", "mariadb":
		return cockroachDBToMySQLType(upper)
	case "sqlite":
		return cockroachDBToSQLiteType(upper)
	case "redis", "mongodb":
		return "", false
	default:
		return "", false
	}
}

func cockroachDBToMySQLType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "SERIAL") || strings.HasPrefix(upper, "BIGSERIAL"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "SMALLSERIAL"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "INTEGER") || strings.HasPrefix(upper, "INT"):
		return "INT", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "REAL") || strings.HasPrefix(upper, "DOUBLE PRECISION"):
		return "DOUBLE", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "DECIMAL", true
	case strings.HasPrefix(upper, "CHARACTER VARYING") || strings.HasPrefix(upper, "VARCHAR"):
		return "VARCHAR(255)", true
	case strings.HasPrefix(upper, "CHARACTER") || strings.HasPrefix(upper, "CHAR"):
		return "CHAR(255)", true
	case strings.HasPrefix(upper, "TEXT"):
		return "LONGTEXT", true
	case strings.HasPrefix(upper, "BYTEA"):
		return "LONGBLOB", true
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return "DATETIME", true
	case upper == "DATE":
		return "DATE", true
	case upper == "TIME":
		return "TIME", true
	case strings.HasPrefix(upper, "BOOL"):
		return "TINYINT(1)", true
	case upper == "JSON" || upper == "JSONB":
		return "JSON", true
	case upper == "UUID":
		return "CHAR(36)", true
	}
	return "", false
}

func cockroachDBToSQLiteType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "SERIAL") || strings.HasPrefix(upper, "BIGSERIAL"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "SMALLSERIAL"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "INTEGER") || strings.HasPrefix(upper, "INT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "REAL") || strings.HasPrefix(upper, "DOUBLE PRECISION"):
		return "REAL", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "CHARACTER VARYING") || strings.HasPrefix(upper, "VARCHAR") ||
		strings.HasPrefix(upper, "CHARACTER") || strings.HasPrefix(upper, "CHAR") ||
		strings.HasPrefix(upper, "TEXT"):
		return "TEXT", true
	case strings.HasPrefix(upper, "BYTEA"):
		return "BLOB", true
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return "TEXT", true
	case upper == "DATE" || upper == "TIME":
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
