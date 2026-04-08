package transform

import (
	"context"
	"github.com/bytedance/sonic"
	"testing"

	"github.com/pageton/bridge-db/pkg/provider"
)

func mysqlRowEnvelope(table string, pk, data map[string]any, colTypes map[string]any) []byte {
	env := map[string]any{
		"table":        table,
		"primary_key":  pk,
		"data":         data,
		"column_types": colTypes,
	}
	b, _ := sonic.Marshal(env)
	return b
}

func pgRowEnvelope(table, schema string, pk, data map[string]any, colTypes map[string]any) []byte {
	env := map[string]any{
		"table":        table,
		"schema":       schema,
		"primary_key":  pk,
		"data":         data,
		"column_types": colTypes,
	}
	b, _ := sonic.Marshal(env)
	return b
}

func makeSchema(tables []tableDef) *provider.Schema {
	s := &provider.Schema{}
	for _, tbl := range tables {
		ts := provider.TableSchema{Name: tbl.name}
		for _, c := range tbl.columns {
			ts.Columns = append(ts.Columns, provider.ColumnSchema{Name: c.name, Type: c.typ})
		}
		s.Tables = append(s.Tables, ts)
	}
	return s
}

type tableDef struct {
	name    string
	columns []colDef
}

type colDef struct {
	name string
	typ  string
}

func TestMySQLToPostgres_AddsSchemaField(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	data := mysqlRowEnvelope("orders", map[string]any{"id": 1.0}, map[string]any{"status": "open"}, map[string]any{"status": "varchar(50)"})
	units := []provider.MigrationUnit{{Key: "orders:1", Data: data}}

	result, err := tr.Transform(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var env map[string]any
	if err := sonic.Unmarshal(result[0].Data, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env["schema"] != "public" {
		t.Errorf("expected schema='public', got %v", env["schema"])
	}
}

func TestMySQLToPostgres_PreservesExistingSchema(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	env := map[string]any{
		"table":       "orders",
		"schema":      "custom",
		"primary_key": map[string]any{"id": 1.0},
		"data":        map[string]any{"status": "open"},
	}
	b, _ := sonic.Marshal(env)
	units := []provider.MigrationUnit{{Key: "orders:1", Data: b}}

	result, _ := tr.Transform(context.Background(), units)
	var got map[string]any
	if err := sonic.Unmarshal(result[0].Data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["schema"] != "custom" {
		t.Errorf("expected schema='custom', got %v", got["schema"])
	}
}

func TestMySQLToPostgres_ConvertsDatetimeColumns(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "orders", columns: []colDef{
			{name: "id", typ: "INT"},
			{name: "created_at", typ: "DATETIME"},
			{name: "updated_at", typ: "TIMESTAMP"},
		}},
	}))

	data := mysqlRowEnvelope("orders",
		map[string]any{"id": 1.0},
		map[string]any{
			"id":         1.0,
			"created_at": "2024-01-15 10:30:00",
			"updated_at": "2024-06-20 14:00:00",
		},
		map[string]any{
			"id":         "INT",
			"created_at": "DATETIME",
			"updated_at": "TIMESTAMP",
		},
	)

	result, err := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "orders:1", Data: data}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var env map[string]any
	if err := sonic.Unmarshal(result[0].Data, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	rowData := env["data"].(map[string]any)

	if ca, ok := rowData["created_at"].(string); ok {
		if ca != "2024-01-15T10:30:00Z" {
			t.Errorf("created_at: got %q, want RFC3339 format", ca)
		}
	} else {
		t.Error("created_at should be string")
	}

	if ua, ok := rowData["updated_at"].(string); ok {
		if ua != "2024-06-20T14:00:00Z" {
			t.Errorf("updated_at: got %q, want RFC3339 format", ua)
		}
	} else {
		t.Error("updated_at should be string")
	}
}

func TestMySQLToPostgres_SkipsInvalidDatetime(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "dt", typ: "DATETIME"}}},
	}))

	data := mysqlRowEnvelope("t",
		map[string]any{"id": 1.0},
		map[string]any{"dt": "not-a-datetime"},
		map[string]any{"dt": "DATETIME"},
	)

	result, _ := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "t:1", Data: data}})
	var env map[string]any
	if err := sonic.Unmarshal(result[0].Data, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	rowData := env["data"].(map[string]any)
	if rowData["dt"] != "not-a-datetime" {
		t.Error("invalid datetime should be left unchanged")
	}
}

func TestMySQLToPostgres_NonStringDatetime(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "dt", typ: "DATETIME"}}},
	}))

	data := mysqlRowEnvelope("t",
		map[string]any{"id": 1.0},
		map[string]any{"dt": 12345},
		map[string]any{"dt": "DATETIME"},
	)

	result, _ := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "t:1", Data: data}})
	var env map[string]any
	if err := sonic.Unmarshal(result[0].Data, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	rowData := env["data"].(map[string]any)
	if rowData["dt"] != 12345.0 {
		t.Errorf("non-string datetime should be unchanged, got %v", rowData["dt"])
	}
}

func TestMySQLToPostgres_InvalidJSONPassthrough(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	badData := []byte("not json at all")
	units := []provider.MigrationUnit{{Key: "k", Data: badData}}

	result, err := tr.Transform(context.Background(), units)
	if err != nil {
		t.Fatalf("should not error on bad JSON: %v", err)
	}
	if string(result[0].Data) != string(badData) {
		t.Error("bad JSON data should be passed through unchanged")
	}
}

func TestMySQLToPostgres_NeedsSchema(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	if !tr.NeedsSchema() {
		t.Error("MySQLToPostgresTransformer should need schema")
	}
}

func TestMySQLToPostgres_datetimeColumns_NilSchema(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	cols := tr.datetimeColumns()
	if cols != nil {
		t.Error("nil schema should return nil columns")
	}
}

func TestMySQLToPostgres_datetimeColumns_NoDatetimeColumns(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "id", typ: "INT"}, {name: "name", typ: "VARCHAR(50)"}}},
	}))
	cols := tr.datetimeColumns()
	if len(cols) != 0 {
		t.Errorf("expected 0 datetime columns, got %d", len(cols))
	}
}

func TestPostgresToMySQL_ConvertsTimestampColumns(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "orders", columns: []colDef{
			{name: "id", typ: "INTEGER"},
			{name: "created_at", typ: "TIMESTAMP"},
			{name: "updated_at", typ: "TIMESTAMPTZ"},
		}},
	}))

	data := pgRowEnvelope("orders", "public",
		map[string]any{"id": 1.0},
		map[string]any{
			"id":         1.0,
			"created_at": "2024-01-15T10:30:00Z",
			"updated_at": "2024-06-20T14:00:00Z",
		},
		map[string]any{
			"id":         "INTEGER",
			"created_at": "TIMESTAMP",
			"updated_at": "TIMESTAMPTZ",
		},
	)

	result, err := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "orders:1", Data: data}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var env map[string]any
	if err := sonic.Unmarshal(result[0].Data, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	rowData := env["data"].(map[string]any)

	if ca := rowData["created_at"].(string); ca != "2024-01-15 10:30:00" {
		t.Errorf("created_at: got %q, want MySQL format", ca)
	}
	if ua := rowData["updated_at"].(string); ua != "2024-06-20 14:00:00" {
		t.Errorf("updated_at: got %q, want MySQL format", ua)
	}
}

func TestPostgresToMySQL_NoTimestampColumns_Passthrough(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "id", typ: "INTEGER"}}},
	}))

	data := pgRowEnvelope("t", "public",
		map[string]any{"id": 1.0},
		map[string]any{"id": 1.0},
		map[string]any{"id": "INTEGER"},
	)

	result, err := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "t:1", Data: data}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result[0].Data) != string(data) {
		t.Error("data without timestamp columns should pass through unchanged")
	}
}

func TestPostgresToMySQL_InvalidJSONPassthrough(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "ts", typ: "TIMESTAMP"}}},
	}))
	badData := []byte("not json")
	result, _ := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "k", Data: badData}})
	if string(result[0].Data) != string(badData) {
		t.Error("bad JSON should pass through unchanged")
	}
}

func TestPostgresToMySQL_InvalidTimestampSkipped(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "ts", typ: "TIMESTAMP"}}},
	}))

	data := pgRowEnvelope("t", "public",
		map[string]any{"id": 1.0},
		map[string]any{"ts": "not-a-timestamp"},
		map[string]any{"ts": "TIMESTAMP"},
	)

	result, _ := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "t:1", Data: data}})
	var env map[string]any
	if err := sonic.Unmarshal(result[0].Data, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	rowData := env["data"].(map[string]any)
	if rowData["ts"] != "not-a-timestamp" {
		t.Error("invalid timestamp should be left unchanged")
	}
}

func TestPostgresToMySQL_NonStringTimestamp(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "ts", typ: "TIMESTAMP"}}},
	}))

	data := pgRowEnvelope("t", "public",
		map[string]any{"id": 1.0},
		map[string]any{"ts": 999},
		map[string]any{"ts": "TIMESTAMP"},
	)

	result, _ := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "t:1", Data: data}})
	var env map[string]any
	if err := sonic.Unmarshal(result[0].Data, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	rowData := env["data"].(map[string]any)
	if rowData["ts"] != 999.0 {
		t.Errorf("non-string timestamp should be unchanged, got %v", rowData["ts"])
	}
}

func TestPostgresToMySQL_MissingDataField(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "ts", typ: "TIMESTAMP"}}},
	}))

	env := map[string]any{
		"table":       "t",
		"schema":      "public",
		"primary_key": map[string]any{"id": 1.0},
	}
	b, _ := sonic.Marshal(env)

	result, _ := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "t:1", Data: b}})
	if string(result[0].Data) != string(b) {
		t.Error("missing data field should pass through unchanged")
	}
}

func TestPostgresToMySQL_NeedsSchema(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	if !tr.NeedsSchema() {
		t.Error("PostgresToMySQLTransformer should need schema")
	}
}

func TestPostgresToMySQL_timestampColumns_NilSchema(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	cols := tr.timestampColumns()
	if cols != nil {
		t.Error("nil schema should return nil columns")
	}
}

func TestPostgresToMySQL_MultipleUnits(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "ts", typ: "TIMESTAMP"}}},
	}))

	units := make([]provider.MigrationUnit, 3)
	for i := range units {
		units[i] = provider.MigrationUnit{
			Key: "t:" + string(rune('1'+i)),
			Data: pgRowEnvelope("t", "public",
				map[string]any{"id": float64(i + 1)},
				map[string]any{"ts": "2024-01-15T10:30:00Z"},
				map[string]any{"ts": "TIMESTAMP"},
			),
		}
	}

	result, err := tr.Transform(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}
	for i, u := range result {
		var env map[string]any
		if err := sonic.Unmarshal(u.Data, &env); err != nil {
			t.Fatalf("unmarshal unit %d: %v", i, err)
		}
		rowData := env["data"].(map[string]any)
		if ts := rowData["ts"].(string); ts != "2024-01-15 10:30:00" {
			t.Errorf("unit %d: ts=%q, want MySQL format", i, ts)
		}
	}
}

func TestMySQLToPostgres_NilSchemaNoCrash(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	data := mysqlRowEnvelope("t",
		map[string]any{"id": 1.0},
		map[string]any{"status": "ok"},
		map[string]any{"status": "VARCHAR(50)"},
	)
	result, err := tr.Transform(context.Background(), []provider.MigrationUnit{{Key: "t:1", Data: data}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var env map[string]any
	if err := sonic.Unmarshal(result[0].Data, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env["schema"] != "public" {
		t.Errorf("schema should still be added even with nil schema, got %v", env["schema"])
	}
}

func TestPostgresToMySQL_EmptyUnits(t *testing.T) {
	tr := &PostgresToMySQLTransformer{}
	tr.SetSchema(makeSchema([]tableDef{
		{name: "t", columns: []colDef{{name: "ts", typ: "TIMESTAMP"}}},
	}))

	result, err := tr.Transform(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty for nil input, got %d", len(result))
	}
}

func TestMySQLToPostgres_EmptyUnits(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	result, err := tr.Transform(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty slice for nil input, got %d", len(result))
	}
}
