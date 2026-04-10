package transform

import (
	"context"
	"strings"

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
	schema *provider.Schema
	cfg TransformerConfig
}

func (t *cockroachDBPassthroughTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	if t.src == "redis" && IsSQLProvider(t.dst) {
		return RedisToSQL(units, sqlEnvelopeConfigForProvider(t.dst), &t.cfg)
	}
	if t.src == "mongodb" && IsSQLProvider(t.dst) {
		return MongoDBToSQL(units, sqlEnvelopeConfigForProvider(t.dst), &t.cfg)
	}

	pipe := NewStagePipeline(
		NullHandlingStage(&t.cfg),
		FieldMappingStage(&t.cfg),
	)

	var err error
	units, err = pipe.Transform(ctx, units)
	if err != nil {
		return nil, err
	}

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
	case "sqlite":
		return AdjustSchemaField(units, false, "")
	default:
		return units, nil
	}
}

func (t *cockroachDBPassthroughTransformer) NeedsSchema() bool {
	return IsSQLProvider(t.src) && IsSQLProvider(t.dst)
}
func (t *cockroachDBPassthroughTransformer) SetSchema(schema *provider.Schema) { t.schema = schema }
func (t *cockroachDBPassthroughTransformer) TypeMapper() provider.TypeMapper {
	return cockroachDBTypeMapper{src: t.src, dst: t.dst}
}
func (t *cockroachDBPassthroughTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }

func sqlEnvelopeConfigForProvider(providerName string) SQLEnvelopeConfig {
	if NeedsSchemaField(providerName) {
		return SQLEnvelopeConfig{SchemaName: "public"}
	}
	return SQLEnvelopeConfig{}
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
