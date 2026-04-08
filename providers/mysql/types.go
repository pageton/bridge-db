package mysql

import (
	"fmt"
	"github.com/bytedance/sonic"
)

// mysqlRow is the JSON envelope for a MySQL MigrationUnit's Data field.
// It carries the row's data alongside metadata needed to reconstruct it
// on the destination.
type mysqlRow struct {
	// Table is the source table name.
	Table string `json:"table"`

	// PrimaryKey is the primary key column(s) and value(s).
	PrimaryKey map[string]any `json:"primary_key"`

	// Data holds the column name to value mapping.
	Data map[string]any `json:"data"`

	// ColumnTypes maps column names to their MySQL type names.
	ColumnTypes map[string]string `json:"column_types,omitempty"`
}

// encodeMySQLRow serialises a mysqlRow into JSON bytes.
func encodeMySQLRow(row *mysqlRow) ([]byte, error) {
	return sonic.Marshal(row)
}

// decodeMySQLRow deserialises JSON bytes into a mysqlRow.
func decodeMySQLRow(data []byte) (*mysqlRow, error) {
	var row mysqlRow
	if err := sonic.Unmarshal(data, &row); err != nil {
		return nil, fmt.Errorf("decode mysql row: %w", err)
	}
	return &row, nil
}

// formatPrimaryKey formats a primary key map as a string for use as a key component.
func formatPrimaryKey(pk map[string]any) string {
	if len(pk) == 1 {
		// Single column PK
		for _, v := range pk {
			return fmt.Sprintf("%v", v)
		}
	}

	// Composite PK
	b, _ := sonic.Marshal(pk)
	return string(b)
}

// buildRowKey builds a unique key for a row in the format "table:pk".
func buildRowKey(table string, pk map[string]any) string {
	return table + ":" + formatPrimaryKey(pk)
}
