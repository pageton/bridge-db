package transform

import (
	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/pkg/provider"
)

// SQLEnvelopeConfig controls the output SQL row envelope format.
type SQLEnvelopeConfig struct {
	// SchemaName is the target schema name. If non-empty, the "schema" field
	// is included in the envelope. If empty, it is omitted.
	SchemaName string

	// DefaultTableName is used when the source unit has no table name
	// (e.g. Redis keys without a table prefix).
	DefaultTableName string
}

// RedisToSQL converts Redis hash/string/list data into SQL row envelopes.
// Generalized from RedisToSQLite to support any SQL destination.
func RedisToSQL(units []provider.MigrationUnit, envCfg SQLEnvelopeConfig, cfg *TransformerConfig) ([]provider.MigrationUnit, error) {
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

		// Apply null handler
		if cfg != nil && cfg.NullHandler != nil {
			var err error
			fields, err = cfg.NullHandler.Apply(fields)
			if err != nil {
				continue
			}
		}

		data := make(map[string]any, len(fields))
		columnTypes := make(map[string]string, len(fields))
		for k, v := range fields {
			data[k] = v
			columnTypes[k] = "TEXT"
		}
		data["_key"] = unit.Key
		columnTypes["_key"] = "TEXT"

		table := envCfg.DefaultTableName
		if table == "" {
			table = "redis_data"
		}
		if unit.Table != "" {
			table = unit.Table
		}

		pk := map[string]any{"_key": unit.Key}

		sqlEnvelope := map[string]any{
			"table":        table,
			"primary_key":  pk,
			"data":         data,
			"column_types": columnTypes,
		}
		if envCfg.SchemaName != "" {
			sqlEnvelope["schema"] = envCfg.SchemaName
		}

		encoded, err := sonic.Marshal(sqlEnvelope)
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

// MongoDBToSQL converts MongoDB document envelopes into SQL row envelopes.
// Generalized from MongoDBToSQLite to support any SQL destination.
func MongoDBToSQL(units []provider.MigrationUnit, envCfg SQLEnvelopeConfig, cfg *TransformerConfig) ([]provider.MigrationUnit, error) {
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

		// Apply null handler
		if cfg != nil && cfg.NullHandler != nil {
			var err error
			doc, err = cfg.NullHandler.Apply(doc)
			if err != nil {
				continue
			}
		}

		// Apply field mappings
		if cfg != nil && cfg.FieldMapping != nil {
			var err error
			doc, err = cfg.FieldMapping.Apply(collection, doc)
			if err != nil {
				continue
			}
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

		sqlEnvelope := map[string]any{
			"table":        collection,
			"primary_key":  pk,
			"data":         data,
			"column_types": columnTypes,
		}
		if envCfg.SchemaName != "" {
			sqlEnvelope["schema"] = envCfg.SchemaName
		}

		encoded, err := sonic.Marshal(sqlEnvelope)
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
