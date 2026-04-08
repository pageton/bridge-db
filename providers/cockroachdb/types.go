package cockroachdb

import (
	"fmt"
	"github.com/bytedance/sonic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type cockroachDBRow struct {
	Table       string            `json:"table"`
	Schema      string            `json:"schema"`
	PrimaryKey  map[string]any    `json:"primary_key"`
	Data        map[string]any    `json:"data"`
	ColumnTypes map[string]string `json:"column_types,omitempty"`
}

func encodeCockroachDBRow(row *cockroachDBRow) ([]byte, error) {
	return sonic.Marshal(row)
}

func decodeCockroachDBRow(data []byte) (*cockroachDBRow, error) {
	var row cockroachDBRow
	if err := sonic.Unmarshal(data, &row); err != nil {
		return nil, fmt.Errorf("decode cockroachdb row: %w", err)
	}
	return &row, nil
}

func convertValue(value any, typeName string) any {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case pgtype.Numeric:
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
			f, err := v.Float64Value()
			if err == nil && f.Valid {
				return f.Float64
			}
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
			return v.Microseconds
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
		return fmt.Sprintf("\\x%x", v)
	case pgtype.UUID:
		if v.Valid {
			return v.String()
		}
		return nil
	default:
		return v
	}
}

func formatPrimaryKey(pk map[string]any) string {
	if len(pk) == 1 {
		for _, v := range pk {
			return fmt.Sprintf("%v", v)
		}
	}
	b, _ := sonic.Marshal(pk)
	return string(b)
}

func buildRowKey(schema, table string, pk map[string]any) string {
	key := table
	if schema != "" && schema != "public" {
		key = schema + "." + table
	}
	return key + ":" + formatPrimaryKey(pk)
}
