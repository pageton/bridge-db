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
		{"mssql", "postgres"},
		{"mssql", "mysql"},
		{"mssql", "mariadb"},
		{"mssql", "cockroachdb"},
		{"mssql", "sqlite"},
		{"mssql", "redis"},
		{"mssql", "mongodb"},
		{"postgres", "mssql"},
		{"mysql", "mssql"},
		{"mariadb", "mssql"},
		{"cockroachdb", "mssql"},
		{"sqlite", "mssql"},
		{"redis", "mssql"},
		{"mongodb", "mssql"},
	}
	for _, p := range pairs {
		src, dst := p.src, p.dst
		RegisterTransformer(src, dst, func() Transformer {
			return &mssqlPassthroughTransformer{src: src, dst: dst}
		})
	}
}

type mssqlPassthroughTransformer struct {
	src string
	dst string
}

func (t *mssqlPassthroughTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	switch t.dst {
	case "redis":
		return mssqlToRedis(units)
	case "mongodb":
		return mssqlToMongoDB(units)
	case "sqlite":
		return mssqlToSQLite(units)
	default:
		return units, nil
	}
}

func (t *mssqlPassthroughTransformer) NeedsSchema() bool            { return false }
func (t *mssqlPassthroughTransformer) SetSchema(_ *provider.Schema) {}
func (t *mssqlPassthroughTransformer) TypeMapper() provider.TypeMapper {
	return mssqlTypeMapper{src: t.src, dst: t.dst}
}

func mssqlToRedis(units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
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

func mssqlToMongoDB(units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
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

func mssqlToSQLite(units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
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

type mssqlTypeMapper struct {
	src string
	dst string
}

func (m mssqlTypeMapper) MapType(colType string) (string, bool) {
	upper := strings.ToUpper(colType)
	switch m.src {
	case "mssql":
		return mssqlSrcToDst(upper, m.dst)
	default:
		return dstToMSSQL(upper, m.src)
	}
}

func mssqlSrcToDst(upper string, dst string) (string, bool) {
	switch dst {
	case "postgres", "cockroachdb":
		return mssqlToPostgresType(upper)
	case "mysql", "mariadb":
		return mssqlToMySQLType(upper)
	case "sqlite":
		return mssqlToSQLiteType(upper)
	case "redis", "mongodb":
		return "", false
	default:
		return "", false
	}
}

func dstToMSSQL(upper string, src string) (string, bool) {
	switch src {
	case "postgres", "cockroachdb":
		return postgresToMSSQLType(upper)
	case "mysql", "mariadb":
		return mysqlToMSSQLType(upper)
	case "sqlite":
		return sqliteToMSSQLType(upper)
	case "redis", "mongodb":
		return "", false
	default:
		return "", false
	}
}

func mssqlToPostgresType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "BIGINT") || strings.HasPrefix(upper, "INT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "TINYINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "REAL") || strings.HasPrefix(upper, "FLOAT"):
		return "REAL", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "VARCHAR") || strings.HasPrefix(upper, "NVARCHAR"):
		return "TEXT", true
	case strings.HasPrefix(upper, "CHAR") || strings.HasPrefix(upper, "NCHAR"):
		return "TEXT", true
	case strings.HasPrefix(upper, "TEXT") || strings.HasPrefix(upper, "NTEXT"):
		return "TEXT", true
	case strings.HasPrefix(upper, "VARBINARY") || strings.HasPrefix(upper, "IMAGE"):
		return "BYTEA", true
	case strings.HasPrefix(upper, "DATETIMEOFFSET"):
		return "TIMESTAMP WITH TIME ZONE", true
	case strings.HasPrefix(upper, "DATETIME2") || strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "SMALLDATETIME"):
		return "TIMESTAMP", true
	case upper == "DATE":
		return "DATE", true
	case upper == "TIME":
		return "TIME", true
	case upper == "BIT":
		return "BOOLEAN", true
	case upper == "UNIQUEIDENTIFIER":
		return "UUID", true
	case strings.HasPrefix(upper, "XML"):
		return "XML", true
	}
	return "", false
}

func mssqlToMySQLType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "INT"):
		return "INT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "TINYINT"):
		return "TINYINT", true
	case strings.HasPrefix(upper, "REAL") || strings.HasPrefix(upper, "FLOAT"):
		return "DOUBLE", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "DECIMAL", true
	case strings.HasPrefix(upper, "VARCHAR") || strings.HasPrefix(upper, "NVARCHAR"):
		return "VARCHAR(255)", true
	case strings.HasPrefix(upper, "CHAR") || strings.HasPrefix(upper, "NCHAR"):
		return "CHAR(255)", true
	case strings.HasPrefix(upper, "TEXT") || strings.HasPrefix(upper, "NTEXT"):
		return "LONGTEXT", true
	case strings.HasPrefix(upper, "VARBINARY") || strings.HasPrefix(upper, "IMAGE"):
		return "LONGBLOB", true
	case strings.HasPrefix(upper, "DATETIME2") || strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "SMALLDATETIME") || strings.HasPrefix(upper, "DATETIMEOFFSET"):
		return "DATETIME", true
	case upper == "DATE":
		return "DATE", true
	case upper == "TIME":
		return "TIME", true
	case upper == "BIT":
		return "TINYINT(1)", true
	case upper == "UNIQUEIDENTIFIER":
		return "CHAR(36)", true
	}
	return "", false
}

func mssqlToSQLiteType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "BIGINT") || strings.HasPrefix(upper, "INT") || strings.HasPrefix(upper, "SMALLINT") || strings.HasPrefix(upper, "TINYINT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "REAL") || strings.HasPrefix(upper, "FLOAT"):
		return "REAL", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "VARCHAR") || strings.HasPrefix(upper, "NVARCHAR") ||
		strings.HasPrefix(upper, "CHAR") || strings.HasPrefix(upper, "NCHAR") ||
		strings.HasPrefix(upper, "TEXT") || strings.HasPrefix(upper, "NTEXT") ||
		strings.HasPrefix(upper, "XML"):
		return "TEXT", true
	case strings.HasPrefix(upper, "VARBINARY") || strings.HasPrefix(upper, "IMAGE"):
		return "BLOB", true
	case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "SMALLDATETIME"):
		return "TEXT", true
	case upper == "DATE" || upper == "TIME":
		return "TEXT", true
	case upper == "BIT":
		return "INTEGER", true
	case upper == "UNIQUEIDENTIFIER":
		return "TEXT", true
	}
	return "TEXT", true
}

func postgresToMSSQLType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "SERIAL") || strings.HasPrefix(upper, "BIGSERIAL"):
		return "BIGINT IDENTITY(1,1)", true
	case strings.HasPrefix(upper, "SMALLSERIAL"):
		return "SMALLINT IDENTITY(1,1)", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "INTEGER") || strings.HasPrefix(upper, "INT"):
		return "INT", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "REAL") || strings.HasPrefix(upper, "DOUBLE PRECISION"):
		return "FLOAT", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "DECIMAL", true
	case strings.HasPrefix(upper, "CHARACTER VARYING") || strings.HasPrefix(upper, "VARCHAR"):
		return "NVARCHAR(255)", true
	case strings.HasPrefix(upper, "CHARACTER") || strings.HasPrefix(upper, "CHAR"):
		return "NCHAR(255)", true
	case strings.HasPrefix(upper, "TEXT"):
		return "NVARCHAR(MAX)", true
	case strings.HasPrefix(upper, "BYTEA"):
		return "VARBINARY(MAX)", true
	case strings.HasPrefix(upper, "TIMESTAMP WITH TIME ZONE"):
		return "DATETIMEOFFSET", true
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return "DATETIME2", true
	case upper == "DATE":
		return "DATE", true
	case upper == "TIME":
		return "TIME", true
	case strings.HasPrefix(upper, "BOOL"):
		return "BIT", true
	case upper == "UUID":
		return "UNIQUEIDENTIFIER", true
	case upper == "JSON" || upper == "JSONB":
		return "NVARCHAR(MAX)", true
	case upper == "XML":
		return "XML", true
	}
	return "", false
}

func mysqlToMSSQLType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "TINYINT"):
		return "TINYINT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "MEDIUMINT"):
		return "INT", true
	case strings.HasPrefix(upper, "INT"):
		return "INT", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "FLOAT"):
		return "FLOAT", true
	case strings.HasPrefix(upper, "DOUBLE"):
		return "FLOAT", true
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		return "DECIMAL", true
	case strings.HasPrefix(upper, "VARCHAR"):
		return "NVARCHAR(255)", true
	case strings.HasPrefix(upper, "CHAR"):
		return "NCHAR(255)", true
	case strings.HasPrefix(upper, "TINYTEXT") || strings.HasPrefix(upper, "MEDIUMTEXT") ||
		strings.HasPrefix(upper, "LONGTEXT") || strings.HasPrefix(upper, "TEXT"):
		return "NVARCHAR(MAX)", true
	case strings.HasPrefix(upper, "TINYBLOB") || strings.HasPrefix(upper, "MEDIUMBLOB") ||
		strings.HasPrefix(upper, "LONGBLOB") || strings.HasPrefix(upper, "BLOB"):
		return "VARBINARY(MAX)", true
	case strings.HasPrefix(upper, "DATETIME"):
		return "DATETIME2", true
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return "DATETIME2", true
	case upper == "DATE":
		return "DATE", true
	case upper == "TIME":
		return "TIME", true
	case strings.HasPrefix(upper, "BOOL"):
		return "BIT", true
	case strings.HasPrefix(upper, "JSON"):
		return "NVARCHAR(MAX)", true
	case strings.HasPrefix(upper, "ENUM") || strings.HasPrefix(upper, "SET"):
		return "NVARCHAR(255)", true
	}
	return "", false
}

func sqliteToMSSQLType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "INTEGER") || strings.HasPrefix(upper, "INT"):
		return "INT", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "REAL") || strings.HasPrefix(upper, "FLOAT") || strings.HasPrefix(upper, "DOUBLE"):
		return "FLOAT", true
	case strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL"):
		return "DECIMAL", true
	case strings.HasPrefix(upper, "TEXT") || strings.HasPrefix(upper, "CHAR") || strings.HasPrefix(upper, "VARCHAR"):
		return "NVARCHAR(MAX)", true
	case strings.HasPrefix(upper, "BLOB"):
		return "VARBINARY(MAX)", true
	}
	return "NVARCHAR(MAX)", true
}
