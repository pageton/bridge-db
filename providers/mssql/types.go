//go:build mssql

package mssql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bytedance/sonic"
)

type mssqlRow struct {
	Table       string            `json:"table"`
	PrimaryKey  map[string]any    `json:"primary_key"`
	Data        map[string]any    `json:"data"`
	ColumnTypes map[string]string `json:"column_types,omitempty"`
}

func encodeMSSQLRow(row *mssqlRow) ([]byte, error) {
	return sonic.Marshal(row)
}

func decodeMSSQLRow(data []byte) (*mssqlRow, error) {
	var row mssqlRow
	if err := sonic.Unmarshal(data, &row); err != nil {
		return nil, fmt.Errorf("decode mssql row: %w", err)
	}
	return &row, nil
}

func formatPrimaryKey(pk map[string]any) string {
	if pk == nil {
		return "null"
	}
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

func buildRowKey(table string, pk map[string]any) string {
	return table + ":" + formatPrimaryKey(pk)
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
