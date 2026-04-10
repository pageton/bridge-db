package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/pkg/provider"
)

// TransformStage is a single step in the transformation pipeline.
// Stages are applied in order: null handling → field mapping → type-specific.
type TransformStage func(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error)

// StagePipeline chains multiple transform stages and applies them sequentially.
// If any stage returns an error, the pipeline stops and returns the error.
type StagePipeline struct {
	stages []TransformStage
	desc   []string // human-readable description of each stage
}

// NewStagePipeline creates a pipeline from the given stages.
func NewStagePipeline(stages ...TransformStage) *StagePipeline {
	desc := make([]string, len(stages))
	for i := range stages {
		desc[i] = fmt.Sprintf("stage_%d", i)
	}
	return &StagePipeline{stages: stages, desc: desc}
}

// Transform applies all stages in order.
func (sp *StagePipeline) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	var err error
	for i, stage := range sp.stages {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		units, err = stage(ctx, units)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", sp.desc[i], err)
		}
	}
	return units, nil
}

// StageCount returns the number of stages in the pipeline.
func (sp *StagePipeline) StageCount() int {
	return len(sp.stages)
}

// ---------------------------------------------------------------------------
// Built-in stage constructors
// ---------------------------------------------------------------------------

// NullHandlingStage creates a stage that applies null policy to SQL row data.
// No-op for non-SQL data (Redis, MongoDB envelopes).
func NullHandlingStage(cfg *TransformerConfig) TransformStage {
	return func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		if cfg == nil || cfg.NullHandler == nil {
			return units, nil
		}
		result := make([]provider.MigrationUnit, len(units))
		for i, unit := range units {
			data, table, err := extractSQLData(unit)
			if err != nil || data == nil {
				result[i] = unit
				continue
			}
			data, err = cfg.NullHandler.Apply(data)
			if err != nil {
				return nil, fmt.Errorf("null handling for table %q: %w", table, err)
			}
			unit, _ = replaceSQLData(unit, data)
			result[i] = unit
		}
		return result, nil
	}
}

// FieldMappingStage creates a stage that applies user-configured field mappings
// to SQL row data.
func FieldMappingStage(cfg *TransformerConfig) TransformStage {
	return func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		if cfg == nil || cfg.FieldMapping == nil {
			return units, nil
		}
		result := make([]provider.MigrationUnit, len(units))
		for i, unit := range units {
			data, table, err := extractSQLData(unit)
			if err != nil || data == nil {
				result[i] = unit
				continue
			}
			data, err = cfg.FieldMapping.Apply(table, data)
			if err != nil {
				return nil, fmt.Errorf("field mapping for table %q: %w", table, err)
			}
			unit, _ = replaceSQLData(unit, data)
			result[i] = unit
		}
		return result, nil
	}
}

// TimestampConversionStage creates a stage that converts timestamp column values
// between SQL dialects.
func TimestampConversionStage(schema *provider.Schema, srcDialect, dstDialect Dialect) TransformStage {
	return func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		return ConvertTimestampColumns(units, schema, srcDialect, dstDialect)
	}
}

// SchemaFieldStage creates a stage that adds or removes the "schema" field
// from SQL row envelopes.
func SchemaFieldStage(add bool, schemaName string) TransformStage {
	return func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		return AdjustSchemaField(units, add, schemaName)
	}
}

// SQLToRedisStage creates a stage that converts SQL row envelopes into Redis
// hash envelopes.
func SQLToRedisStage(cfg *TransformerConfig) TransformStage {
	return func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		return SQLToRedis(units, cfg)
	}
}

// SQLToMongoDBStage creates a stage that converts SQL row envelopes into MongoDB
// document envelopes.
func SQLToMongoDBStage(cfg *TransformerConfig) TransformStage {
	return func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		return SQLToMongoDB(units, cfg)
	}
}

// RedisToSQLStage creates a stage that converts Redis data into SQL row envelopes.
func RedisToSQLStage(envCfg SQLEnvelopeConfig, cfg *TransformerConfig) TransformStage {
	return func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		return RedisToSQL(units, envCfg, cfg)
	}
}

// MongoDBToSQLStage creates a stage that converts MongoDB documents into SQL
// row envelopes.
func MongoDBToSQLStage(envCfg SQLEnvelopeConfig, cfg *TransformerConfig) TransformStage {
	return func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		return MongoDBToSQL(units, envCfg, cfg)
	}
}

// ---------------------------------------------------------------------------
// Envelope helpers
// ---------------------------------------------------------------------------

// extractSQLData extracts the "data" map and "table" from a SQL row envelope.
// Returns nil data if the unit is not a SQL row envelope.
func extractSQLData(unit provider.MigrationUnit) (map[string]any, string, error) {
	if unit.DataType != provider.DataTypeRow && unit.DataType != "" {
		return nil, "", nil
	}
	var envelope map[string]any
	if err := unmarshalEnvelope(unit.Data, &envelope); err != nil {
		return nil, "", nil // not a map envelope, skip
	}
	raw, ok := envelope["data"]
	if !ok {
		return nil, "", nil
	}
	data, ok := raw.(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("sql envelope \"data\" field is %T, expected map[string]any", raw)
	}
	table, _ := envelope["table"].(string)
	return data, table, nil
}

// replaceSQLData replaces the "data" field in a SQL row envelope.
func replaceSQLData(unit provider.MigrationUnit, data map[string]any) (provider.MigrationUnit, error) {
	var envelope map[string]any
	if err := unmarshalEnvelope(unit.Data, &envelope); err != nil {
		return unit, nil
	}
	envelope["data"] = data
	encoded, err := marshalEnvelope(envelope)
	if err != nil {
		return unit, err
	}
	unit.Data = encoded
	return unit, nil
}

func unmarshalEnvelope(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}

func marshalEnvelope(v any) ([]byte, error) {
	return sonic.Marshal(v)
}
