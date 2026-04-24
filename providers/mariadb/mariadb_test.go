package mariadb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pageton/bridge-db/internal/config"
)

// ---------------------------------------------------------------------------
// encodeMariaDBRow / decodeMariaDBRow
// ---------------------------------------------------------------------------

func TestEncodeDecodeMariaDBRow_RoundTrip(t *testing.T) {
	original := &mariaDBRow{
		Table: "users",
		PrimaryKey: map[string]any{
			"id": int64(42),
		},
		Data: map[string]any{
			"name":  "alice",
			"email": "alice@example.com",
		},
		ColumnTypes: map[string]string{
			"id":    "bigint",
			"name":  "varchar(255)",
			"email": "varchar(255)",
		},
	}

	encoded, err := encodeMariaDBRow(original)
	if err != nil {
		t.Fatalf("encodeMariaDBRow failed: %v", err)
	}

	decoded, err := decodeMariaDBRow(encoded)
	if err != nil {
		t.Fatalf("decodeMariaDBRow failed: %v", err)
	}

	if decoded.Table != original.Table {
		t.Errorf("Table = %q, want %q", decoded.Table, original.Table)
	}
	if fmt.Sprintf("%v", decoded.PrimaryKey["id"]) != "42" {
		t.Errorf("PrimaryKey[id] = %v, want 42", decoded.PrimaryKey["id"])
	}
	if decoded.Data["name"] != original.Data["name"] {
		t.Errorf("Data[name] = %v, want %v", decoded.Data["name"], original.Data["name"])
	}
	if decoded.Data["email"] != original.Data["email"] {
		t.Errorf("Data[email] = %v, want %v", decoded.Data["email"], original.Data["email"])
	}
	if decoded.ColumnTypes["id"] != original.ColumnTypes["id"] {
		t.Errorf("ColumnTypes[id] = %q, want %q", decoded.ColumnTypes["id"], original.ColumnTypes["id"])
	}
}

func TestDecodeMariaDBRow_InvalidJSON(t *testing.T) {
	_, err := decodeMariaDBRow([]byte("not-json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestDecodeMariaDBRow_EmptyBytes(t *testing.T) {
	_, err := decodeMariaDBRow([]byte(""))
	if err == nil {
		t.Fatal("expected error for empty bytes, got nil")
	}
}

func TestEncodeMariaDBRow_OmitsEmptyColumnTypes(t *testing.T) {
	row := &mariaDBRow{
		Table:      "t",
		PrimaryKey: map[string]any{"id": 1},
		Data:       map[string]any{"x": "y"},
	}
	data, err := encodeMariaDBRow(row)
	if err != nil {
		t.Fatalf("encodeMariaDBRow failed: %v", err)
	}
	if strings.Contains(string(data), "column_types") {
		t.Errorf("expected column_types to be omitted when empty, got %s", string(data))
	}
}

// ---------------------------------------------------------------------------
// formatPrimaryKey
// ---------------------------------------------------------------------------

func TestFormatPrimaryKey_SingleColumn(t *testing.T) {
	pk := map[string]any{"id": int64(99)}
	got := formatPrimaryKey(pk)
	want := "99"
	if got != want {
		t.Errorf("formatPrimaryKey(single) = %q, want %q", got, want)
	}
}

func TestFormatPrimaryKey_Composite(t *testing.T) {
	pk := map[string]any{
		"org_id":  int64(1),
		"user_id": int64(2),
	}
	got := formatPrimaryKey(pk)
	if !strings.HasPrefix(got, "{") || !strings.HasSuffix(got, "}") {
		t.Errorf("formatPrimaryKey(composite) = %q, expected JSON object", got)
	}
}

func TestFormatPrimaryKey_Empty(t *testing.T) {
	got := formatPrimaryKey(map[string]any{})
	want := "{}"
	if got != want {
		t.Errorf("formatPrimaryKey(empty) = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// buildRowKey
// ---------------------------------------------------------------------------

func TestBuildRowKey_SimplePK(t *testing.T) {
	got := buildRowKey("users", map[string]any{"id": int64(5)})
	want := "users:5"
	if got != want {
		t.Errorf("buildRowKey = %q, want %q", got, want)
	}
}

func TestBuildRowKey_CompositePK(t *testing.T) {
	got := buildRowKey("orders", map[string]any{
		"customer_id": int64(10),
		"order_id":    int64(20),
	})
	if !strings.HasPrefix(got, "orders:{") {
		t.Errorf("buildRowKey(composite) = %q, want prefix 'orders:{'", got)
	}
}

// ---------------------------------------------------------------------------
// convertValue
// ---------------------------------------------------------------------------

func TestConvertValue_Nil(t *testing.T) {
	got := convertValue(nil)
	if got != nil {
		t.Errorf("convertValue(nil) = %v, want nil", got)
	}
}

func TestConvertValue_BytesToString(t *testing.T) {
	got := convertValue([]byte("hello"))
	want := "hello"
	if got != want {
		t.Errorf("convertValue([]byte) = %T(%v), want string(%q)", got, got, want)
	}
}

func TestConvertValue_IntPassthrough(t *testing.T) {
	got := convertValue(int64(42))
	want := int64(42)
	if got != want {
		t.Errorf("convertValue(int64) = %v, want %v", got, want)
	}
}

func TestConvertValue_StringPassthrough(t *testing.T) {
	got := convertValue("world")
	if got != "world" {
		t.Errorf("convertValue(string) = %v, want %q", got, "world")
	}
}

// ---------------------------------------------------------------------------
// quoteIdentifier
// ---------------------------------------------------------------------------

func TestQuoteIdentifier_Simple(t *testing.T) {
	got := quoteIdentifier("users")
	want := "`users`"
	if got != want {
		t.Errorf("quoteIdentifier = %q, want %q", got, want)
	}
}

func TestQuoteIdentifier_WithBacktick(t *testing.T) {
	got := quoteIdentifier("my`table")
	want := "`my``table`"
	if got != want {
		t.Errorf("quoteIdentifier = %q, want %q", got, want)
	}
}

func TestQuoteIdentifier_Empty(t *testing.T) {
	got := quoteIdentifier("")
	want := "``"
	if got != want {
		t.Errorf("quoteIdentifier = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// buildPlaceholders
// ---------------------------------------------------------------------------

func TestBuildPlaceholders_Zero(t *testing.T) {
	got := buildPlaceholders(0)
	if got != "" {
		t.Errorf("buildPlaceholders(0) = %q, want empty", got)
	}
}

func TestBuildPlaceholders_One(t *testing.T) {
	got := buildPlaceholders(1)
	if got != "?" {
		t.Errorf("buildPlaceholders(1) = %q, want %q", got, "?")
	}
}

func TestBuildPlaceholders_Three(t *testing.T) {
	got := buildPlaceholders(3)
	want := "?, ?, ?"
	if got != want {
		t.Errorf("buildPlaceholders(3) = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// buildPKWhere
// ---------------------------------------------------------------------------

func TestBuildPKWhere_SinglePK(t *testing.T) {
	pk := map[string]any{"id": int64(7)}
	clause, args := buildPKWhere(pk, 0)
	if clause != "`id` = ?" {
		t.Errorf("clause = %q, want %q", clause, "`id` = ?")
	}
	if len(args) != 1 || args[0] != int64(7) {
		t.Errorf("args = %v, want [7]", args)
	}
}

func TestBuildPKWhere_CompositePK(t *testing.T) {
	pk := map[string]any{
		"org_id":  int64(1),
		"user_id": int64(2),
	}
	clause, args := buildPKWhere(pk, 0)
	if !strings.Contains(clause, " AND ") {
		t.Errorf("clause = %q, want AND-joined clauses", clause)
	}
	if len(args) != 2 {
		t.Errorf("len(args) = %d, want 2", len(args))
	}
}

// ---------------------------------------------------------------------------
// parseRowKey
// ---------------------------------------------------------------------------

func TestParseRowKey_SingleColumnPK(t *testing.T) {
	table, pk, err := parseRowKey("users:42")
	if err != nil {
		t.Fatalf("parseRowKey failed: %v", err)
	}
	if table != "users" {
		t.Errorf("table = %q, want %q", table, "users")
	}
	if pk["id"] != "42" {
		t.Errorf("pk[id] = %v, want %q", pk["id"], "42")
	}
}

func TestParseRowKey_CompositeJSONPK(t *testing.T) {
	table, pk, err := parseRowKey(`orders:{"order_id":10,"customer_id":5}`)
	if err != nil {
		t.Fatalf("parseRowKey failed: %v", err)
	}
	if table != "orders" {
		t.Errorf("table = %q, want %q", table, "orders")
	}
	if len(pk) != 2 {
		t.Errorf("len(pk) = %d, want 2", len(pk))
	}
}

func TestParseRowKey_InvalidFormat(t *testing.T) {
	_, _, err := parseRowKey("no-colon-here")
	if err == nil {
		t.Fatal("expected error for missing colon, got nil")
	}
}

func TestParseRowKey_InvalidJSON(t *testing.T) {
	_, _, err := parseRowKey("t:{bad-json")
	if err == nil {
		t.Fatal("expected error for invalid JSON PK, got nil")
	}
}

// ---------------------------------------------------------------------------
// buildMariaDBDSN
// ---------------------------------------------------------------------------

func TestBuildMariaDBDSN_Format(t *testing.T) {
	cfg := &config.MariaDBConfig{
		Host:     "localhost",
		Port:     config.IntPtr(3306),
		Username: "root",
		Password: "secret",
		Database: "mydb",
	}
	dsn := buildMariaDBDSN(cfg)

	if !strings.Contains(dsn, "tcp(localhost:3306)") {
		t.Errorf("DSN missing host:port, got %q", dsn)
	}
	if !strings.Contains(dsn, "parseTime=true") {
		t.Errorf("DSN missing parseTime, got %q", dsn)
	}
	if !strings.Contains(dsn, "multiStatements=true") {
		t.Errorf("DSN missing multiStatements, got %q", dsn)
	}
	if !strings.HasPrefix(dsn, "root:secret@") {
		t.Errorf("DSN missing credentials, got %q", dsn)
	}
}
