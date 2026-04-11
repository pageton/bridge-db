//go:build sqlite

package sqlite

import (
	"testing"

	"github.com/pageton/bridge-db/internal/config"
)

// ---------------------------------------------------------------------------
// types.go: encodeSQLiteRow / decodeSQLiteRow
// ---------------------------------------------------------------------------

func TestEncodeDecodeSQLiteRow(t *testing.T) {
	original := &sqliteRow{
		Table: "users",
		PrimaryKey: map[string]any{
			"id": 42,
		},
		Data: map[string]any{
			"name": "alice",
			"age":  30,
		},
		ColumnTypes: map[string]string{
			"id":   "INTEGER",
			"name": "TEXT",
			"age":  "INTEGER",
		},
	}

	data, err := encodeSQLiteRow(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("encode returned empty bytes")
	}

	decoded, err := decodeSQLiteRow(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Table != original.Table {
		t.Errorf("table = %q, want %q", decoded.Table, original.Table)
	}
	if decoded.Data["name"] != "alice" {
		t.Errorf("name = %v, want alice", decoded.Data["name"])
	}
	if decoded.ColumnTypes["id"] != "INTEGER" {
		t.Errorf("column_type id = %q, want INTEGER", decoded.ColumnTypes["id"])
	}
}

func TestDecodeSQLiteRow_InvalidJSON(t *testing.T) {
	_, err := decodeSQLiteRow([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// ---------------------------------------------------------------------------
// types.go: formatPrimaryKey
// ---------------------------------------------------------------------------

func TestFormatPrimaryKey_SingleColumn(t *testing.T) {
	got := formatPrimaryKey(map[string]any{"id": 42})
	if got != "42" {
		t.Errorf("single-column pk = %q, want %q", got, "42")
	}
}

func TestFormatPrimaryKey_Composite(t *testing.T) {
	got := formatPrimaryKey(map[string]any{"user_id": 1, "org_id": 10})
	// Composite keys are JSON-encoded
	if got == "" {
		t.Error("composite pk should not be empty")
	}
}

func TestFormatPrimaryKey_Empty(t *testing.T) {
	got := formatPrimaryKey(map[string]any{})
	// Empty map still marshals to JSON "{}"
	if got != "{}" {
		t.Errorf("empty pk = %q, want %q", got, "{}")
	}
}

// ---------------------------------------------------------------------------
// types.go: buildRowKey
// ---------------------------------------------------------------------------

func TestBuildRowKey(t *testing.T) {
	got := buildRowKey("users", map[string]any{"id": 42})
	want := "users:42"
	if got != want {
		t.Errorf("buildRowKey = %q, want %q", got, want)
	}
}

func TestBuildRowKey_CompositePK(t *testing.T) {
	got := buildRowKey("orders", map[string]any{"user_id": 1, "order_id": 99})
	if len(got) < 7 {
		t.Errorf("buildRowKey = %q, too short", got)
	}
}

// ---------------------------------------------------------------------------
// scanner.go: convertValue
// ---------------------------------------------------------------------------

func TestConvertValue_Nil(t *testing.T) {
	got := convertValue(nil)
	if got != nil {
		t.Errorf("convertValue(nil) = %v, want nil", got)
	}
}

func TestConvertValue_ByteArray(t *testing.T) {
	got := convertValue([]byte("hello"))
	if got != "hello" {
		t.Errorf("convertValue([]byte) = %T(%v), want string(hello)", got, got)
	}
}

func TestConvertValue_IntPassthrough(t *testing.T) {
	got := convertValue(42)
	if got != 42 {
		t.Errorf("convertValue(42) = %v, want 42", got)
	}
}

func TestConvertValue_StringPassthrough(t *testing.T) {
	got := convertValue("hello")
	if got != "hello" {
		t.Errorf("convertValue(string) = %v, want hello", got)
	}
}

// ---------------------------------------------------------------------------
// scanner.go: quoteIdentifier
// ---------------------------------------------------------------------------

func TestQuoteIdentifier_Simple(t *testing.T) {
	got := quoteIdentifier("users")
	if got != `"users"` {
		t.Errorf("quoteIdentifier = %q, want %q", got, `"users"`)
	}
}

func TestQuoteIdentifier_WithDoubleQuote(t *testing.T) {
	got := quoteIdentifier(`col"name`)
	if got != `"col""name"` {
		t.Errorf("quoteIdentifier = %q, want %q", got, `"col""name"`)
	}
}

func TestQuoteIdentifier_Empty(t *testing.T) {
	got := quoteIdentifier("")
	if got != `""` {
		t.Errorf("quoteIdentifier(empty) = %q, want %q", got, `""`)
	}
}

// ---------------------------------------------------------------------------
// writer.go: buildPlaceholders
// ---------------------------------------------------------------------------

func TestBuildPlaceholders(t *testing.T) {
	got := buildPlaceholders(3)
	if got != "?, ?, ?" {
		t.Errorf("buildPlaceholders(3) = %q, want %q", got, "?, ?, ?")
	}
}

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

// ---------------------------------------------------------------------------
// verifier.go: parseRowKey
// ---------------------------------------------------------------------------

func TestParseRowKey_SingleColumnPK(t *testing.T) {
	table, pk, err := parseRowKey("users:42")
	if err != nil {
		t.Fatalf("parseRowKey: %v", err)
	}
	if table != "users" {
		t.Errorf("table = %q, want users", table)
	}
	if pk["id"] != "42" {
		t.Errorf("pk = %v, want map[id:42]", pk)
	}
}

func TestParseRowKey_CompositePK(t *testing.T) {
	table, pk, err := parseRowKey(`orders:{"user_id":1,"order_id":99}`)
	if err != nil {
		t.Fatalf("parseRowKey: %v", err)
	}
	if table != "orders" {
		t.Errorf("table = %q, want orders", table)
	}
	if len(pk) != 2 {
		t.Errorf("pk length = %d, want 2", len(pk))
	}
}

func TestParseRowKey_InvalidFormat(t *testing.T) {
	_, _, err := parseRowKey("nodelimiter")
	if err == nil {
		t.Fatal("expected error for missing colon")
	}
}

func TestParseRowKey_InvalidJSON(t *testing.T) {
	_, _, err := parseRowKey("users:{bad}")
	if err == nil {
		t.Fatal("expected error for invalid JSON pk")
	}
}

// ---------------------------------------------------------------------------
// schema.go: sqliteSafeDefault
// ---------------------------------------------------------------------------

func TestSQLiteSafeDefault_Empty(t *testing.T) {
	got, ok := sqliteSafeDefault("")
	if ok || got != "" {
		t.Errorf("empty = (%q, %v), want (%q, false)", got, ok, "")
	}
}

func TestSQLiteSafeDefault_NextVal(t *testing.T) {
	// PostgreSQL sequence functions should be rejected
	_, ok := sqliteSafeDefault("nextval('users_id_seq'::regclass)")
	if ok {
		t.Error("nextval should be rejected")
	}
}

func TestSQLiteSafeDefault_CurrVal(t *testing.T) {
	_, ok := sqliteSafeDefault("currval('seq')")
	if ok {
		t.Error("currval should be rejected")
	}
}

func TestSQLiteSafeDefault_SetVal(t *testing.T) {
	_, ok := sqliteSafeDefault("setval('seq', 1)")
	if ok {
		t.Error("setval should be rejected")
	}
}

func TestSQLiteSafeDefault_RegClass(t *testing.T) {
	_, ok := sqliteSafeDefault("'seq'::regclass")
	if ok {
		t.Error("::regclass should be rejected")
	}
}

func TestSQLiteSafeDefault_Now(t *testing.T) {
	got, ok := sqliteSafeDefault("NOW()")
	if !ok || got != "CURRENT_TIMESTAMP" {
		t.Errorf("NOW() = (%q, %v), want (CURRENT_TIMESTAMP, true)", got, ok)
	}
}

func TestSQLiteSafeDefault_CurrentTimestamp(t *testing.T) {
	got, ok := sqliteSafeDefault("CURRENT_TIMESTAMP")
	if !ok || got != "CURRENT_TIMESTAMP" {
		t.Errorf("CURRENT_TIMESTAMP = (%q, %v), want (CURRENT_TIMESTAMP, true)", got, ok)
	}
}

func TestSQLiteSafeDefault_LiteralString(t *testing.T) {
	got, ok := sqliteSafeDefault("'hello'")
	if !ok || got != "'hello'" {
		t.Errorf("literal = (%q, %v), want ('hello', true)", got, ok)
	}
}

func TestSQLiteSafeDefault_SimpleValue(t *testing.T) {
	got, ok := sqliteSafeDefault("42")
	if !ok || got != "42" {
		t.Errorf("number = (%q, %v), want (42, true)", got, ok)
	}
}

func TestSQLiteSafeDefault_SQLiteFunction(t *testing.T) {
	got, ok := sqliteSafeDefault("datetime('now')")
	if !ok || got != "(datetime('now'))" {
		t.Errorf("sqlite function = (%q, %v), want ((datetime('now')), true)", got, ok)
	}
}

func TestSQLiteSafeDefault_UnknownFunction(t *testing.T) {
	// Function with parens but not in the known list should be rejected
	// unless it contains parens without being in langFns
	_, ok := sqliteSafeDefault("uuid_generate_v4()")
	if ok {
		t.Error("unknown function should be rejected")
	}
}

// ---------------------------------------------------------------------------
// provider.go: buildSQLiteDSN
// ---------------------------------------------------------------------------

func TestBuildSQLiteDSN(t *testing.T) {
	cfg := &config.SQLiteConfig{Path: "/tmp/test.db"}
	got := buildSQLiteDSN(cfg)
	if got != "/tmp/test.db" {
		t.Errorf("DSN = %q, want /tmp/test.db", got)
	}
}
