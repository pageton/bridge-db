//go:build mssql

package mssql

import (
	"strings"
	"testing"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/internal/config"
)

func TestEncodeMSSQLRow_DecodeMSSQLRow_RoundTrip(t *testing.T) {
	row := &mssqlRow{
		Table: "users",
		PrimaryKey: map[string]any{
			"id": 42,
		},
		Data: map[string]any{
			"id":    42,
			"name":  "alice",
			"email": "alice@example.com",
		},
		ColumnTypes: map[string]string{
			"id":    "int",
			"name":  "nvarchar(255)",
			"email": "nvarchar(255)",
		},
	}

	data, err := encodeMSSQLRow(row)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("encode produced empty bytes")
	}

	decoded, err := decodeMSSQLRow(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Table != row.Table {
		t.Errorf("table = %q, want %q", decoded.Table, row.Table)
	}
	pkVal := decoded.PrimaryKey["id"]
	switch v := pkVal.(type) {
	case float64:
		if v != 42 {
			t.Errorf("pk[id] = %v, want 42", v)
		}
	case int:
		if v != 42 {
			t.Errorf("pk[id] = %v, want 42", v)
		}
	default:
		t.Errorf("pk[id] type = %T, want int or float64", pkVal)
	}
	if decoded.Data["name"] != row.Data["name"] {
		t.Errorf("data[name] = %v, want %v", decoded.Data["name"], row.Data["name"])
	}
	if decoded.ColumnTypes["id"] != "int" {
		t.Errorf("column_types[id] = %q, want %q", decoded.ColumnTypes["id"], "int")
	}
}

func TestDecodeMSSQLRow_InvalidJSON(t *testing.T) {
	_, err := decodeMSSQLRow([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "decode mssql row") {
		t.Errorf("error = %q, want wrapping 'decode mssql row'", err.Error())
	}
}

func TestEncodeMSSQLRow_OmitsEmptyColumnTypes(t *testing.T) {
	row := &mssqlRow{
		Table:      "orders",
		PrimaryKey: map[string]any{"id": 1},
		Data:       map[string]any{"id": 1, "total": 99.99},
	}

	data, err := encodeMSSQLRow(row)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if strings.Contains(string(data), "column_types") {
		t.Errorf("empty column_types should be omitted via omitempty, got %s", string(data))
	}

	decoded, err := decodeMSSQLRow(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.ColumnTypes) != 0 {
		t.Errorf("column_types should be empty after decode of omitted field, got %v", decoded.ColumnTypes)
	}
}

func TestFormatPrimaryKey_SingleColumn(t *testing.T) {
	pk := map[string]any{"id": 42}
	got := formatPrimaryKey(pk)
	if got != "42" {
		t.Errorf("formatPrimaryKey = %q, want %q", got, "42")
	}
}

func TestFormatPrimaryKey_Composite(t *testing.T) {
	pk := map[string]any{"org_id": 1, "user_id": 5}
	got := formatPrimaryKey(pk)
	if !strings.HasPrefix(got, "{") || !strings.HasSuffix(got, "}") {
		t.Errorf("composite PK should be JSON object, got %q", got)
	}
	var decoded map[string]any
	if err := sonic.Unmarshal([]byte(got), &decoded); err != nil {
		t.Fatalf("failed to parse composite PK JSON: %v", err)
	}
	if decoded["org_id"] == nil || decoded["user_id"] == nil {
		t.Errorf("composite PK JSON missing expected keys, got %v", decoded)
	}
}

func TestFormatPrimaryKey_Nil(t *testing.T) {
	got := formatPrimaryKey(nil)
	if got != "null" {
		t.Errorf("formatPrimaryKey(nil) = %q, want %q", got, "null")
	}
}

func TestFormatPrimaryKey_Empty(t *testing.T) {
	got := formatPrimaryKey(map[string]any{})
	if got != "{}" {
		t.Errorf("formatPrimaryKey(empty) = %q, want %q", got, "{}")
	}
}

func TestBuildRowKey_Simple(t *testing.T) {
	pk := map[string]any{"id": 42}
	got := buildRowKey("users", pk)
	want := "users:42"
	if got != want {
		t.Errorf("buildRowKey = %q, want %q", got, want)
	}
}

func TestBuildRowKey_Composite(t *testing.T) {
	pk := map[string]any{"org_id": 1, "user_id": 5}
	got := buildRowKey("memberships", pk)
	if !strings.HasPrefix(got, "memberships:{") {
		t.Errorf("buildRowKey composite = %q, want prefix 'memberships:{'", got)
	}
	if !strings.HasSuffix(got, "}") {
		t.Errorf("buildRowKey composite = %q, want suffix '}'", got)
	}
}

func TestConvertValue_Nil(t *testing.T) {
	got := convertValue(nil)
	if got != nil {
		t.Errorf("convertValue(nil) = %v, want nil", got)
	}
}

func TestConvertValue_ByteSliceToString(t *testing.T) {
	got := convertValue([]byte("hello"))
	if got != "hello" {
		t.Errorf("convertValue([]byte(\"hello\")) = %v (type %T), want \"hello\" string", got, got)
	}
}

func TestConvertValue_IntPassthrough(t *testing.T) {
	got := convertValue(42)
	if got != 42 {
		t.Errorf("convertValue(42) = %v, want 42", got)
	}
}

func TestConvertValue_StringPassthrough(t *testing.T) {
	got := convertValue("world")
	if got != "world" {
		t.Errorf("convertValue(\"world\") = %v, want \"world\"", got)
	}
}

func TestConvertValue_FloatPassthrough(t *testing.T) {
	got := convertValue(3.14)
	if got != 3.14 {
		t.Errorf("convertValue(3.14) = %v, want 3.14", got)
	}
}

func TestQuoteIdentifier_Simple(t *testing.T) {
	got := quoteIdentifier("users")
	if got != "[users]" {
		t.Errorf("quoteIdentifier = %q, want %q", got, "[users]")
	}
}

func TestQuoteIdentifier_WithBracket(t *testing.T) {
	got := quoteIdentifier("my[table]")
	if got != "[my[table]]]" {
		t.Errorf("quoteIdentifier = %q, want %q", got, "[my[table]]]")
	}
}

func TestQuoteIdentifier_Empty(t *testing.T) {
	got := quoteIdentifier("")
	if got != "[]" {
		t.Errorf("quoteIdentifier = %q, want %q", got, "[]")
	}
}

func TestQuoteIdentifier_WithSpaces(t *testing.T) {
	got := quoteIdentifier("my table")
	if got != "[my table]" {
		t.Errorf("quoteIdentifier = %q, want %q", got, "[my table]")
	}
}

func TestQuoteString_Simple(t *testing.T) {
	got := quoteString("hello")
	if got != "'hello'" {
		t.Errorf("quoteString = %q, want %q", got, "'hello'")
	}
}

func TestQuoteString_EmbeddedSingleQuote(t *testing.T) {
	got := quoteString("it's a test")
	if got != "'it''s a test'" {
		t.Errorf("quoteString = %q, want %q", got, "'it''s a test'")
	}
}

func TestQuoteString_Empty(t *testing.T) {
	got := quoteString("")
	if got != "''" {
		t.Errorf("quoteString = %q, want %q", got, "''")
	}
}

func TestQuoteString_OnlySingleQuote(t *testing.T) {
	got := quoteString("'")
	if got != "''''" {
		t.Errorf("quoteString = %q, want %q", got, "''''")
	}
}

func TestExtractPKColumns_Single(t *testing.T) {
	row := mssqlRow{PrimaryKey: map[string]any{"id": 1}}
	cols := extractPKColumns(row)
	if len(cols) != 1 {
		t.Fatalf("len = %d, want 1", len(cols))
	}
	if cols[0] != "id" {
		t.Errorf("cols[0] = %q, want %q", cols[0], "id")
	}
}

func TestExtractPKColumns_Composite(t *testing.T) {
	row := mssqlRow{PrimaryKey: map[string]any{"org_id": 1, "user_id": 5}}
	cols := extractPKColumns(row)
	if len(cols) != 2 {
		t.Fatalf("len = %d, want 2", len(cols))
	}
	found := map[string]bool{}
	for _, c := range cols {
		found[c] = true
	}
	if !found["org_id"] || !found["user_id"] {
		t.Errorf("cols = %v, want both org_id and user_id", cols)
	}
}

func TestExtractPKColumns_Nil(t *testing.T) {
	row := mssqlRow{}
	cols := extractPKColumns(row)
	if cols != nil {
		t.Errorf("extractPKColumns(empty) = %v, want nil", cols)
	}
}

func TestParseRowKey_SingleColumnPK(t *testing.T) {
	table, pk, err := parseRowKey("users:42")
	if err != nil {
		t.Fatalf("parseRowKey: %v", err)
	}
	if table != "users" {
		t.Errorf("table = %q, want %q", table, "users")
	}
	if pk["id"] != "42" {
		t.Errorf("pk[id] = %v, want %q", pk["id"], "42")
	}
}

func TestParseRowKey_CompositeJSONPK(t *testing.T) {
	table, pk, err := parseRowKey(`memberships:{"org_id":1,"user_id":5}`)
	if err != nil {
		t.Fatalf("parseRowKey: %v", err)
	}
	if table != "memberships" {
		t.Errorf("table = %q, want %q", table, "memberships")
	}
	if pk["org_id"] == nil {
		t.Error("missing org_id in composite PK")
	}
	if pk["user_id"] == nil {
		t.Error("missing user_id in composite PK")
	}
}

func TestParseRowKey_InvalidFormat(t *testing.T) {
	_, _, err := parseRowKey("nocolonhere")
	if err == nil {
		t.Fatal("expected error for key without colon")
	}
	if !strings.Contains(err.Error(), "invalid key format") {
		t.Errorf("error = %q, want 'invalid key format'", err.Error())
	}
}

func TestParseRowKey_InvalidJSON(t *testing.T) {
	_, _, err := parseRowKey(`table:{not valid json}`)
	if err == nil {
		t.Fatal("expected error for invalid JSON in PK")
	}
	if !strings.Contains(err.Error(), "invalid primary key JSON") {
		t.Errorf("error = %q, want 'invalid primary key JSON'", err.Error())
	}
}

func TestParseRowKey_EmptyTable(t *testing.T) {
	table, pk, err := parseRowKey(":42")
	if err != nil {
		t.Fatalf("parseRowKey: %v", err)
	}
	if table != "" {
		t.Errorf("table = %q, want empty", table)
	}
	if pk["id"] != "42" {
		t.Errorf("pk[id] = %v, want %q", pk["id"], "42")
	}
}

func TestBuildMSSQLDSN_Basic(t *testing.T) {
	cfg := &config.MSSQLConfig{
		Host:     "localhost",
		Port:     1433,
		Username: "sa",
		Password: "Secret123!",
		Database: "mydb",
		Encrypt:  true,
	}
	dsn := buildMSSQLDSN(cfg)
	if !strings.HasPrefix(dsn, "sqlserver://") {
		t.Errorf("DSN should start with sqlserver://, got %q", dsn)
	}
	if !strings.Contains(dsn, "Secret123") {
		t.Errorf("DSN should contain password, got %q", dsn)
	}
	if !strings.Contains(dsn, "database=mydb") {
		t.Errorf("DSN should contain database param, got %q", dsn)
	}
	if !strings.Contains(dsn, "encrypt=true") {
		t.Errorf("DSN should contain encrypt=true, got %q", dsn)
	}
}

func TestBuildMSSQLDSN_WithTrustCert(t *testing.T) {
	cfg := &config.MSSQLConfig{
		Host:      "10.0.0.1",
		Port:      1433,
		Username:  "admin",
		Password:  "pw",
		Database:  "testdb",
		Encrypt:   true,
		TrustCert: true,
	}
	dsn := buildMSSQLDSN(cfg)
	if !strings.Contains(dsn, "TrustServerCertificate=true") {
		t.Errorf("DSN should contain TrustServerCertificate=true, got %q", dsn)
	}
}

func TestBuildMSSQLDSN_NoEncrypt(t *testing.T) {
	cfg := &config.MSSQLConfig{
		Host:     "localhost",
		Port:     1433,
		Username: "sa",
		Password: "pw",
		Database: "db",
		Encrypt:  false,
	}
	dsn := buildMSSQLDSN(cfg)
	if strings.Contains(dsn, "encrypt") {
		t.Errorf("DSN should not contain encrypt param, got %q", dsn)
	}
}
