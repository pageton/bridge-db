package postgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bytedance/sonic"

	"github.com/jackc/pgx/v5/pgtype"
)

// postgresRow is the JSON envelope for a PostgreSQL MigrationUnit's Data field.
// It carries the row's data alongside metadata needed to reconstruct it
// on the destination.
type postgresRow struct {
	// Table is the source table name.
	Table string `json:"table"`

	// Schema is the source schema name (usually "public").
	Schema string `json:"schema"`

	// PrimaryKey is the primary key column(s) and value(s).
	PrimaryKey map[string]any `json:"primary_key"`

	// Data holds the column name to value mapping.
	Data map[string]any `json:"data"`

	// ColumnTypes maps column names to their PostgreSQL type names.
	ColumnTypes map[string]string `json:"column_types,omitempty"`
}

// encodePostgresRow serialises a postgresRow into JSON bytes.
func encodePostgresRow(row *postgresRow) ([]byte, error) {
	return sonic.Marshal(row)
}

// decodePostgresRow deserialises JSON bytes into a postgresRow.
func decodePostgresRow(data []byte) (*postgresRow, error) {
	var row postgresRow
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&row); err != nil {
		return nil, fmt.Errorf("decode postgres row: %w", err)
	}
	return &row, nil
}

// convertValue converts a PostgreSQL value to a Go type suitable for JSON serialization.
func convertValue(value any, typeName string) any {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case pgtype.Numeric:
		// Convert numeric to float64 or string
		if v.Valid {
			if v.NaN {
				return "NaN"
			}
			if v.InfinityModifier == pgtype.Infinity {
				return "Infinity"
			}
			if v.InfinityModifier == pgtype.NegativeInfinity {
				return "-Infinity"
			}
			// Try to convert to float64
			f, err := v.Float64Value()
			if err == nil && f.Valid {
				return f.Float64
			}
			// Fall back to JSON representation
			b, _ := v.MarshalJSON()
			return string(b)
		}
		return nil

	case pgtype.Timestamptz:
		if v.Valid {
			return v.Time.Format(time.RFC3339Nano)
		}
		return nil

	case pgtype.Timestamp:
		if v.Valid {
			return v.Time.Format(time.RFC3339Nano)
		}
		return nil

	case pgtype.Date:
		if v.Valid {
			return v.Time.Format("2006-01-02")
		}
		return nil

	case pgtype.Time:
		if v.Valid {
			return v.Microseconds // Return as microseconds since midnight
		}
		return nil

	case pgtype.Interval:
		if v.Valid {
			return map[string]any{
				"microseconds": v.Microseconds,
				"days":         v.Days,
				"months":       v.Months,
			}
		}
		return nil

	case []byte:
		// Return as base64 or hex string
		return fmt.Sprintf("\\x%x", v)

	case pgtype.UUID:
		if v.Valid {
			return v.String()
		}
		return nil

	default:
		// For basic types (string, int, float, bool), return as-is
		// For JSON/JSONB, pgx automatically decodes to map[string]any or []any
		return v
	}
}

// formatPrimaryKey formats a primary key map as a string for use as a key component.
func formatPrimaryKey(pk map[string]any) string {
	if len(pk) == 1 {
		for _, k := range sortedPKKeys(pk) {
			return fmt.Sprintf("%v", pk[k])
		}
	}
	parts := make([]string, 0, len(pk))
	for _, k := range sortedPKKeys(pk) {
		parts = append(parts, fmt.Sprintf("%q:%s", k, formatPKValue(pk[k])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// buildRowKey builds a unique key for a row in the format "schema.table:pk".
func buildRowKey(schema, table string, pk map[string]any) string {
	key := table
	if schema != "" && schema != "public" {
		key = schema + "." + table
	}
	return key + ":" + formatPrimaryKey(pk)
}

func sortedPKKeys(pk map[string]any) []string {
	keys := make([]string, 0, len(pk))
	for k := range pk {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func formatPKValue(v any) string {
	b, err := sonic.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%q", fmt.Sprintf("%v", v))
	}
	return string(b)
}
