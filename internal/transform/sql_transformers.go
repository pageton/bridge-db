package transform

import (
	"context"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	RegisterTransformer("mysql", "postgres", func() Transformer {
		return &MySQLToPostgresTransformer{}
	})
	RegisterTransformer("postgres", "mysql", func() Transformer {
		return &PostgresToMySQLTransformer{}
	})
}

// MySQLToPostgresTransformer converts MySQL rows to PostgreSQL format.
type MySQLToPostgresTransformer struct {
	schema *provider.Schema
}

func (t *MySQLToPostgresTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	dtCols := t.datetimeColumns()
	result := make([]provider.MigrationUnit, len(units))

	for i, unit := range units {
		var envelope map[string]any
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			result[i] = unit
			continue
		}

		if _, ok := envelope["schema"]; !ok {
			envelope["schema"] = "public"
		}

		if len(dtCols) > 0 {
			data, ok := envelope["data"].(map[string]any)
			if ok {
				for _, col := range dtCols {
					val, ok := data[col]
					if !ok {
						continue
					}
					s, ok := val.(string)
					if !ok {
						continue
					}
					t, err := time.Parse("2006-01-02 15:04:05", s)
					if err != nil {
						continue
					}
					data[col] = t.Format(time.RFC3339Nano)
				}
			}
		}

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

func (t *MySQLToPostgresTransformer) datetimeColumns() []string {
	if t.schema == nil {
		return nil
	}
	var cols []string
	for _, tbl := range t.schema.Tables {
		for _, col := range tbl.Columns {
			upper := strings.ToUpper(col.Type)
			if strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP") {
				cols = append(cols, col.Name)
			}
		}
	}
	return cols
}

func (t *MySQLToPostgresTransformer) NeedsSchema() bool {
	return true
}

func (t *MySQLToPostgresTransformer) SetSchema(schema *provider.Schema) {
	t.schema = schema
}

func (t *MySQLToPostgresTransformer) TypeMapper() provider.TypeMapper {
	return MySQLToPostgresTypeMapper{}
}

// PostgresToMySQLTransformer converts PostgreSQL rows to MySQL format.
type PostgresToMySQLTransformer struct {
	schema *provider.Schema
}

func (t *PostgresToMySQLTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	tsCols := t.timestampColumns()
	if len(tsCols) == 0 {
		return units, nil
	}

	result := make([]provider.MigrationUnit, len(units))
	for i, unit := range units {
		var envelope map[string]any
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			result[i] = unit
			continue
		}

		data, ok := envelope["data"].(map[string]any)
		if !ok {
			result[i] = unit
			continue
		}

		changed := false
		for _, col := range tsCols {
			val, ok := data[col]
			if !ok {
				continue
			}
			s, ok := val.(string)
			if !ok {
				continue
			}
			parsed, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				continue
			}
			data[col] = parsed.Format("2006-01-02 15:04:05")
			changed = true
		}

		if changed {
			envelope["data"] = data
			encoded, err := sonic.Marshal(envelope)
			if err != nil {
				result[i] = unit
				continue
			}
			unit.Data = encoded
		}

		result[i] = unit
	}

	return result, nil
}

func (t *PostgresToMySQLTransformer) timestampColumns() []string {
	if t.schema == nil {
		return nil
	}
	var cols []string
	for _, tbl := range t.schema.Tables {
		for _, col := range tbl.Columns {
			upper := strings.ToUpper(col.Type)
			if strings.HasPrefix(upper, "TIMESTAMP") || strings.HasPrefix(upper, "TIMESTAMPTZ") {
				cols = append(cols, col.Name)
			}
		}
	}
	return cols
}

func (t *PostgresToMySQLTransformer) NeedsSchema() bool {
	return true
}

func (t *PostgresToMySQLTransformer) SetSchema(schema *provider.Schema) {
	t.schema = schema
}

func (t *PostgresToMySQLTransformer) TypeMapper() provider.TypeMapper {
	return PostgresToMySQLTypeMapper{}
}

// MySQLToPostgresTypeMapper maps MySQL types to PostgreSQL types.
type MySQLToPostgresTypeMapper struct{}

func (m MySQLToPostgresTypeMapper) MapType(mysqlType string) (string, bool) {
	upper := strings.ToUpper(mysqlType)

	switch {
	case strings.HasPrefix(upper, "TINYINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT", true
	case strings.HasPrefix(upper, "MEDIUMINT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "INT"):
		return "INTEGER", true
	case strings.HasPrefix(upper, "FLOAT"):
		return "REAL", true
	case strings.HasPrefix(upper, "DOUBLE"):
		return "DOUBLE PRECISION", true
	case strings.HasPrefix(upper, "DECIMAL"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "NUMERIC"):
		return "NUMERIC", true
	case strings.HasPrefix(upper, "VARCHAR"):
		return "VARCHAR", true
	case strings.HasPrefix(upper, "CHAR"):
		return "CHAR", true
	case strings.HasPrefix(upper, "TINYTEXT"):
		return "TEXT", true
	case strings.HasPrefix(upper, "MEDIUMTEXT"):
		return "TEXT", true
	case strings.HasPrefix(upper, "LONGTEXT"):
		return "TEXT", true
	case strings.HasPrefix(upper, "TEXT"):
		return "TEXT", true
	case strings.HasPrefix(upper, "TINYBLOB") ||
		strings.HasPrefix(upper, "MEDIUMBLOB") ||
		strings.HasPrefix(upper, "LONGBLOB"):
		return "BYTEA", true
	case strings.HasPrefix(upper, "BINARY"):
		return "BYTEA", true
	case strings.HasPrefix(upper, "VARBINARY"):
		return "BYTEA", true
	case strings.HasPrefix(upper, "BLOB"):
		return "BYTEA", true
	case strings.HasPrefix(upper, "DATETIME"):
		return "TIMESTAMP", true
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return "TIMESTAMP", true
	case strings.HasPrefix(upper, "ENUM"):
		return "VARCHAR(255)", true
	case strings.HasPrefix(upper, "SET"):
		return "VARCHAR(255)", true
	case strings.HasPrefix(upper, "JSON"):
		return "JSONB", true
	case strings.HasPrefix(upper, "YEAR"):
		return "SMALLINT", true
	case upper == "DATE":
		return "DATE", true
	case upper == "TIME":
		return "TIME", true
	case strings.HasPrefix(upper, "BOOL"):
		return "BOOLEAN", true
	}

	return "", false
}

// PostgresToMySQLTypeMapper maps PostgreSQL types to MySQL types.
type PostgresToMySQLTypeMapper struct{}

func (m PostgresToMySQLTypeMapper) MapType(postgresType string) (string, bool) {
	normalized := strings.ToUpper(postgresType)

	switch {
	case strings.HasPrefix(normalized, "TIMESTAMP WITH TIME ZONE"):
		return "DATETIME", true
	case strings.HasPrefix(normalized, "TIMESTAMP WITHOUT TIME ZONE"):
		return "DATETIME", true
	case strings.HasPrefix(normalized, "TIMESTAMP"):
		return "DATETIME", true
	case strings.HasPrefix(normalized, "CHARACTER VARYING"):
		parts := strings.Fields(postgresType)
		if len(parts) >= 3 {
			return "VARCHAR" + parts[2], true
		}
		return "VARCHAR(255)", true
	case strings.HasPrefix(normalized, "CHARACTER"):
		return "CHAR(255)", true
	case strings.HasPrefix(normalized, "DOUBLE PRECISION"):
		return "DOUBLE", true
	case strings.HasPrefix(normalized, "NUMERIC"):
		return "DECIMAL", true
	}

	typeMap := map[string]string{
		"SMALLINT":    "SMALLINT",
		"INTEGER":     "INT",
		"BIGINT":      "BIGINT",
		"REAL":        "FLOAT",
		"NUMERIC":     "DECIMAL",
		"DECIMAL":     "DECIMAL",
		"CHAR":        "CHAR",
		"VARCHAR":     "VARCHAR",
		"TEXT":        "TEXT",
		"BYTEA":       "BLOB",
		"DATE":        "DATE",
		"TIME":        "TIME",
		"BOOLEAN":     "TINYINT(1)",
		"BOOL":        "TINYINT(1)",
		"JSON":        "JSON",
		"JSONB":       "JSON",
		"UUID":        "CHAR(36)",
		"SERIAL":      "INT AUTO_INCREMENT",
		"BIGSERIAL":   "BIGINT AUTO_INCREMENT",
		"TIMESTAMPTZ": "DATETIME",
	}

	if mapped, ok := typeMap[normalized]; ok {
		return mapped, true
	}

	return "", false
}
