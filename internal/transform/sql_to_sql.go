package transform

import (
	"encoding/json"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/pkg/provider"
)

// AdjustSchemaField adds or removes the "schema" field in SQL row envelopes.
// Use add=true for migrations to Postgres/CockroachDB, add=false for others.
func AdjustSchemaField(units []provider.MigrationUnit, add bool, schemaName string) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, len(units))
	for i, unit := range units {
		var envelope map[string]json.RawMessage
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			result[i] = unit
			continue
		}

		if add {
			if _, ok := envelope["schema"]; !ok {
				encodedSchema, err := sonic.Marshal(schemaName)
				if err != nil {
					result[i] = unit
					continue
				}
				envelope["schema"] = encodedSchema
			}
		} else {
			delete(envelope, "schema")
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

// ConvertTimestampColumns finds datetime/timestamp columns using the schema
// and reformats their values from srcDialect to dstDialect format.
// No-op if schema is nil.
func ConvertTimestampColumns(units []provider.MigrationUnit, schema *provider.Schema, srcDialect, dstDialect Dialect) ([]provider.MigrationUnit, error) {
	if schema == nil {
		return units, nil
	}

	tsCols := timestampColumns(schema)
	if len(tsCols) == 0 {
		return units, nil
	}

	result := make([]provider.MigrationUnit, len(units))
	for i, unit := range units {
		var envelope map[string]json.RawMessage
		if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
			result[i] = unit
			continue
		}

		dataRaw, ok := envelope["data"]
		if !ok {
			result[i] = unit
			continue
		}

		var data map[string]json.RawMessage
		if err := sonic.Unmarshal(dataRaw, &data); err != nil {
			result[i] = unit
			continue
		}

		changed := false
		for _, col := range tsCols {
			valRaw, ok := data[col]
			if !ok {
				continue
			}
			var s string
			if err := sonic.Unmarshal(valRaw, &s); err != nil {
				continue
			}
			converted := ConvertTimestamp(s, srcDialect, dstDialect)
			if converted != s {
				encoded, err := sonic.Marshal(converted)
				if err != nil {
					continue
				}
				data[col] = encoded
				changed = true
			}
		}

		if changed {
			encodedData, err := sonic.Marshal(data)
			if err != nil {
				result[i] = unit
				continue
			}
			envelope["data"] = encodedData
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

// timestampColumns extracts column names that look like datetime/timestamp types from the schema.
func timestampColumns(schema *provider.Schema) []string {
	var cols []string
	for _, tbl := range schema.Tables {
		for _, col := range tbl.Columns {
			if IsTimestampColumn(col.Type) {
				cols = append(cols, col.Name)
			}
		}
	}
	return cols
}

// NeedsTimestampConversion returns true if two SQL dialects have different timestamp formats.
func NeedsTimestampConversion(src, dst string) bool {
	if src == dst {
		return false
	}
	srcCat := CategorizeProvider(src)
	dstCat := CategorizeProvider(dst)
	if srcCat != "sql" || dstCat != "sql" {
		return false
	}

	// MySQL and MariaDB share the same datetime format
	if (src == "mysql" || src == "mariadb") && (dst == "mysql" || dst == "mariadb") {
		return false
	}
	// Postgres and CockroachDB share the same timestamp format
	if (src == "postgres" || src == "cockroachdb") && (dst == "postgres" || dst == "cockroachdb") {
		return false
	}
	return true
}

// BuildSQLToSQLStages returns the transformation steps needed between two SQL providers.
// Returns nil if no transformation is needed.
func BuildSQLToSQLStages(src, dst string, schema *provider.Schema) []func([]provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	var stages []func([]provider.MigrationUnit) ([]provider.MigrationUnit, error)

	if NeedsTimestampConversion(src, dst) {
		sd := Dialect(src)
		dd := Dialect(dst)
		stages = append(stages, func(units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
			return ConvertTimestampColumns(units, schema, sd, dd)
		})
	}

	needsSchema := NeedsSchemaField(dst)
	hasSchema := NeedsSchemaField(src)

	if needsSchema && !hasSchema {
		stages = append(stages, func(units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
			return AdjustSchemaField(units, true, "public")
		})
	} else if !needsSchema && hasSchema {
		stages = append(stages, func(units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
			return AdjustSchemaField(units, false, "")
		})
	}

	return stages
}

// ApplyNullAndMappings applies null handling and field mapping to a data map extracted
// from a SQL row envelope. Returns the modified data map.
func ApplyNullAndMappings(cfg *TransformerConfig, data map[string]any, table string) (map[string]any, error) {
	if cfg == nil {
		return data, nil
	}
	if cfg.NullHandler != nil {
		var err error
		data, err = cfg.NullHandler.Apply(data)
		if err != nil {
			return nil, err
		}
	}
	if cfg.FieldMapping != nil {
		var err error
		data, err = cfg.FieldMapping.Apply(table, data)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

// IsSQLProvider returns true if the provider is a SQL database.
func IsSQLProvider(p string) bool {
	return p == "postgres" || p == "mysql" || p == "sqlite" || p == "mariadb" || p == "cockroachdb" || p == "mssql"
}
