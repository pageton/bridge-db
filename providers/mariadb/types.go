package mariadb

import (
	"fmt"

	"github.com/bytedance/sonic"
)

type mariaDBRow struct {
	Table       string            `json:"table"`
	PrimaryKey  map[string]any    `json:"primary_key"`
	Data        map[string]any    `json:"data"`
	ColumnTypes map[string]string `json:"column_types,omitempty"`
}

func encodeMariaDBRow(row *mariaDBRow) ([]byte, error) {
	return sonic.Marshal(row)
}

func decodeMariaDBRow(data []byte) (*mariaDBRow, error) {
	var row mariaDBRow
	if err := sonic.Unmarshal(data, &row); err != nil {
		return nil, fmt.Errorf("decode mariadb row: %w", err)
	}
	return &row, nil
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

func buildRowKey(table string, pk map[string]any) string {
	return table + ":" + formatPrimaryKey(pk)
}
