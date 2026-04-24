package transform

import (
	"context"
	"strings"

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
	src    string
	dst    string
	cfg    TransformerConfig
	schema *provider.Schema
}

func (t *mariadbPassthroughTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	// NoSQL→SQL: convert Redis/MongoDB envelopes to SQL row envelopes first.
	if t.src == "redis" && IsSQLProvider(t.dst) {
		return RedisToSQL(units, sqlEnvelopeConfigForProvider(t.dst), &t.cfg)
	}
	if t.src == "mongodb" && IsSQLProvider(t.dst) {
		return MongoDBToSQL(units, sqlEnvelopeConfigForProvider(t.dst), &t.cfg)
	}

	// Apply null handling and field mappings for all paths.
	pipe := NewStagePipeline(
		NullHandlingStage(&t.cfg),
		FieldMappingStage(&t.cfg),
	)

	var err error
	units, err = pipe.Transform(ctx, units)
	if err != nil {
		return nil, err
	}

	// For SQL→SQL paths, apply timestamp conversion and schema field adjustments.
	if IsSQLProvider(t.dst) {
		stages := BuildSQLToSQLStages(t.src, t.dst, t.schema)
		for _, stage := range stages {
			units, err = stage(units)
			if err != nil {
				return nil, err
			}
		}
		return units, nil
	}

	switch t.dst {
	case "redis":
		return SQLToRedis(units, &t.cfg)
	case "mongodb":
		return SQLToMongoDB(units, &t.cfg)
	default:
		return units, nil
	}
}

func (t *mariadbPassthroughTransformer) NeedsSchema() bool {
	return IsSQLProvider(t.src) && IsSQLProvider(t.dst)
}
func (t *mariadbPassthroughTransformer) SetSchema(s *provider.Schema) { t.schema = s }
func (t *mariadbPassthroughTransformer) TypeMapper() provider.TypeMapper {
	return mariadbTypeMapper{src: t.src, dst: t.dst}
}
func (t *mariadbPassthroughTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

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
	case "mariadb", "mysql":
		// MariaDB and MySQL share the same type system.
		// Map incoming types to MySQL/MariaDB-compatible types.
		switch m.src {
		case "postgres", "cockroachdb":
			return postgresToMariaDBType(upper)
		case "sqlite":
			return SQLiteToMySQLTypeMapper{}.MapType(colType)
		default:
			// Same-family types (e.g. mariadb→mysql) are directly compatible.
			return colType, true
		}
	case "redis", "mongodb":
		// NoSQL destinations are schema-free; source types are preserved as-is.
		return colType, true
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

// postgresToMariaDBType maps PostgreSQL types to MariaDB-compatible types.
// MariaDB uses the same DDL type syntax as MySQL for these purposes.
func postgresToMariaDBType(upper string) (string, bool) {
	switch {
	case strings.HasPrefix(upper, "TIMESTAMP WITH TIME ZONE"):
		return "DATETIME", true
	case strings.HasPrefix(upper, "TIMESTAMP WITHOUT TIME ZONE"):
		return "DATETIME", true
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return "DATETIME", true
	case strings.HasPrefix(upper, "CHARACTER VARYING"):
		return "VARCHAR(255)", true
	case strings.HasPrefix(upper, "CHARACTER"):
		return "CHAR(255)", true
	case strings.HasPrefix(upper, "DOUBLE PRECISION"):
		return "DOUBLE", true
	case strings.HasPrefix(upper, "NUMERIC"):
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

	if mapped, ok := typeMap[upper]; ok {
		return mapped, true
	}

	return "", false
}
