package transform

import (
	"context"
	"strings"

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
// Applies: null handling → field mapping → timestamp conversion → schema field addition.
type MySQLToPostgresTransformer struct {
	schema   *provider.Schema
	cfg      TransformerConfig
	pipeline *StagePipeline
}

func (t *MySQLToPostgresTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	if t.pipeline == nil {
		t.buildPipeline()
	}
	return t.pipeline.Transform(ctx, units)
}

func (t *MySQLToPostgresTransformer) buildPipeline() {
	var stages []TransformStage

	// Null handling + field mapping.
	stages = append(stages, NullHandlingStage(&t.cfg))
	stages = append(stages, FieldMappingStage(&t.cfg))

	// Timestamp conversion: MySQL datetime → PostgreSQL RFC3339.
	if t.schema != nil && hasDatetimeColumns(t.schema) {
		stages = append(stages, TimestampConversionStage(t.schema, DialectMySQL, DialectPostgres))
	}

	// Add schema="public" field for PostgreSQL.
	stages = append(stages, SchemaFieldStage(true, "public"))

	t.pipeline = NewStagePipeline(stages...)
	t.pipeline.desc = []string{"null_handling", "field_mapping", "timestamp_conversion", "schema_field"}
}

func (t *MySQLToPostgresTransformer) NeedsSchema() bool {
	return true
}

func (t *MySQLToPostgresTransformer) SetSchema(schema *provider.Schema) {
	t.schema = schema
	t.pipeline = nil // rebuild with new schema
}

func (t *MySQLToPostgresTransformer) TypeMapper() provider.TypeMapper {
	return MySQLToPostgresTypeMapper{}
}

func (t *MySQLToPostgresTransformer) Configure(cfg TransformerConfig) {
	t.cfg = cfg
	t.pipeline = nil // rebuild with new config
}

// PostgresToMySQLTransformer converts PostgreSQL rows to MySQL format.
// Applies: null handling → field mapping → timestamp conversion → schema field removal.
type PostgresToMySQLTransformer struct {
	schema   *provider.Schema
	cfg      TransformerConfig
	pipeline *StagePipeline
}

func (t *PostgresToMySQLTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	if t.pipeline == nil {
		t.buildPipeline()
	}
	return t.pipeline.Transform(ctx, units)
}

func (t *PostgresToMySQLTransformer) buildPipeline() {
	var stages []TransformStage

	// Null handling + field mapping.
	stages = append(stages, NullHandlingStage(&t.cfg))
	stages = append(stages, FieldMappingStage(&t.cfg))

	// Timestamp conversion: PostgreSQL RFC3339 → MySQL datetime.
	if t.schema != nil && hasTimestampColumns(t.schema) {
		stages = append(stages, TimestampConversionStage(t.schema, DialectPostgres, DialectMySQL))
	}

	// Remove schema field for MySQL.
	stages = append(stages, SchemaFieldStage(false, ""))

	t.pipeline = NewStagePipeline(stages...)
	t.pipeline.desc = []string{"null_handling", "field_mapping", "timestamp_conversion", "schema_field"}
}

func (t *PostgresToMySQLTransformer) NeedsSchema() bool {
	return true
}

func (t *PostgresToMySQLTransformer) SetSchema(schema *provider.Schema) {
	t.schema = schema
	t.pipeline = nil // rebuild with new schema
}

func (t *PostgresToMySQLTransformer) TypeMapper() provider.TypeMapper {
	return PostgresToMySQLTypeMapper{}
}

func (t *PostgresToMySQLTransformer) Configure(cfg TransformerConfig) {
	t.cfg = cfg
	t.pipeline = nil // rebuild with new config
}

// ---------------------------------------------------------------------------
// Helper predicates
// ---------------------------------------------------------------------------

// hasDatetimeColumns returns true if any table has DATETIME or TIMESTAMP columns.
func hasDatetimeColumns(schema *provider.Schema) bool {
	for _, tbl := range schema.Tables {
		for _, col := range tbl.Columns {
			if IsTimestampColumn(col.Type) {
				return true
			}
		}
	}
	return false
}

// hasTimestampColumns returns true if any table has TIMESTAMP or TIMESTAMPTZ columns.
func hasTimestampColumns(schema *provider.Schema) bool {
	for _, tbl := range schema.Tables {
		for _, col := range tbl.Columns {
			upper := strings.ToUpper(col.Type)
			if strings.HasPrefix(upper, "TIMESTAMP") {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Type mappers
// ---------------------------------------------------------------------------

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
