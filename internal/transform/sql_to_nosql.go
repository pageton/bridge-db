package transform

import (
	"fmt"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/pkg/provider"
)

// SQLToRedis converts SQL row envelopes into Redis hash envelopes.
// Replaces the duplicated SQL-to-Redis logic across sqlite, mssql, cockroachdb, mariadb transformers.
func SQLToRedis(units []provider.MigrationUnit, cfg *TransformerConfig) ([]provider.MigrationUnit, error) {
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

		// Apply null handler
		if cfg != nil && cfg.NullHandler != nil {
			var err error
			data, err = cfg.NullHandler.Apply(data)
			if err != nil {
				continue
			}
		}

		// Apply field mappings
		table, _ := envelope["table"].(string)
		if cfg != nil && cfg.FieldMapping != nil {
			var err error
			data, err = cfg.FieldMapping.Apply(table, data)
			if err != nil {
				continue
			}
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

// SQLToMongoDB converts SQL row envelopes into MongoDB document envelopes.
// Replaces the duplicated SQL-to-MongoDB logic across sqlite, mssql, cockroachdb, mariadb transformers.
func SQLToMongoDB(units []provider.MigrationUnit, cfg *TransformerConfig) ([]provider.MigrationUnit, error) {
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

		// Apply null handler
		if cfg != nil && cfg.NullHandler != nil {
			var err error
			data, err = cfg.NullHandler.Apply(data)
			if err != nil {
				continue
			}
		}

		// Apply field mappings
		if cfg != nil && cfg.FieldMapping != nil {
			var err error
			data, err = cfg.FieldMapping.Apply(table, data)
			if err != nil {
				continue
			}
		}

		pk, _ := envelope["primary_key"].(map[string]any)
		docID := sanitizeDocID(pk, unit.Key)

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

// sanitizeDocID produces a valid MongoDB document ID from a primary key value.
func sanitizeDocID(pk map[string]any, fallback string) string {
	if len(pk) == 0 {
		return fallback
	}
	for _, v := range pk {
		s := fmt.Sprintf("%v", v)
		s = strings.ReplaceAll(s, " ", "_")
		s = strings.ReplaceAll(s, ":", "_")
		s = strings.ReplaceAll(s, ".", "_")
		s = strings.ReplaceAll(s, "/", "_")
		return s
	}
	return fallback
}
