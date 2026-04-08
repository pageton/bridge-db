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
		{"mariadb", "postgres"},
		{"mariadb", "mysql"},
		{"mariadb", "sqlite"},
		{"mariadb", "redis"},
		{"mariadb", "mongodb"},
		{"mariadb", "cockroachdb"},
		{"postgres", "mariadb"},
		{"mysql", "mariadb"},
		{"sqlite", "mariadb"},
		{"redis", "mariadb"},
		{"mongodb", "mariadb"},
		{"cockroachdb", "mariadb"},
	}
	for _, p := range pairs {
		src, dst := p.src, p.dst
		RegisterTransformer(src, dst, func() Transformer {
			return &mariadbPassthroughTransformer{src: src, dst: dst}
		})
	}
}

type mariadbPassthroughTransformer struct {
	src string
	dst string
}

func (t *mariadbPassthroughTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	switch t.dst {
	case "redis":
		return mariadbToNoSQL(units, "redis")
	case "mongodb":
		return mariadbToNoSQL(units, "mongodb")
	default:
		return units, nil
	}
}

func (t *mariadbPassthroughTransformer) NeedsSchema() bool            { return false }
func (t *mariadbPassthroughTransformer) SetSchema(_ *provider.Schema) {}
func (t *mariadbPassthroughTransformer) TypeMapper() provider.TypeMapper {
	return mariadbTypeMapper{src: t.src, dst: t.dst}
}

func mariadbToNoSQL(units []provider.MigrationUnit, targetType string) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, 0, len(units))
	for _, unit := range units {
		var envelope map[string]any
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			continue
		}

		data, _ := envelope["data"].(map[string]any)
		table, _ := envelope["table"].(string)
		if data == nil {
			continue
		}

		if targetType == "redis" {
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
		} else {
			pk, _ := envelope["primary_key"].(map[string]any)
			docID := "unknown"
			if len(pk) > 0 {
				for _, v := range pk {
					docID = strings.ReplaceAll(strings.ReplaceAll(
						strings.ReplaceAll(fmt.Sprintf("%v", v), " ", "_"), ":", "_"), ".", "_")
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
	}
	return result, nil
}

type mariadbTypeMapper struct {
	src string
	dst string
}

func (m mariadbTypeMapper) MapType(colType string) (string, bool) {
	upper := strings.ToUpper(colType)
	switch m.dst {
	case "postgres", "cockroachdb":
		return mariadbToPostgresType(upper)
	case "sqlite":
		return mariadbToSQLiteType(upper)
	case "redis", "mongodb":
		return "", false
	default:
		return "", false
	}
}

func mariadbToPostgresType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "TINYINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "MEDIUMINT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "INT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "FLOAT"):
		return "REAL", true
	case strings.HasPrefix(upper, "DOUBLE"):
		return "DOUBLE PRECISION", true
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "VARCHAR") || strings.HasPrefix(upper, "CHAR"):
		return "TEXT", true
	case strings.HasPrefix(upper, "TINYTEXT") || strings.HasPrefix(upper, "MEDIUMTEXT") ||
		strings.HasPrefix(upper, "LONGTEXT") || strings.HasPrefix(upper, "TEXT"):
		return "TEXT", true
	case strings.HasPrefix(upper, "TINYBLOB") || strings.HasPrefix(upper, "MEDIUMBLOB") ||
		strings.HasPrefix(upper, "LONGBLOB") || strings.HasPrefix(upper, "BLOB"):
		return "BYTEA", true
	case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP"):
		return "TIMESTAMP", true
	case upper == "DATE":
		return "DATE", true
	case upper == "TIME":
		return "TIME", true
	case strings.HasPrefix(upper, "BOOL"):
		return "BOOLEAN", true
	case strings.HasPrefix(upper, "JSON"):
		return "JSONB", true
	}
	return "", false
}

func mariadbToSQLiteType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "TINYINT") || strings.HasPrefix(upper, "SMALLINT") ||
		strings.HasPrefix(upper, "MEDIUMINT") || strings.HasPrefix(upper, "INT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "FLOAT") || strings.HasPrefix(upper, "DOUBLE"):
		return "REAL", true
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "VARCHAR") || strings.HasPrefix(upper, "CHAR") ||
		strings.HasPrefix(upper, "TINYTEXT") || strings.HasPrefix(upper, "MEDIUMTEXT") ||
		strings.HasPrefix(upper, "LONGTEXT") || strings.HasPrefix(upper, "TEXT") ||
		strings.HasPrefix(upper, "ENUM") || strings.HasPrefix(upper, "SET"):
		return "TEXT", true
	case strings.HasPrefix(upper, "TINYBLOB") || strings.HasPrefix(upper, "MEDIUMBLOB") ||
		strings.HasPrefix(upper, "LONGBLOB") || strings.HasPrefix(upper, "BLOB"):
		return "BLOB", true
	case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP"):
		return "TEXT", true
	case upper == "DATE" || upper == "TIME":
		return "TEXT", true
	case strings.HasPrefix(upper, "BOOL"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "JSON"):
		return "TEXT", true
	}
	return "TEXT", true
}
