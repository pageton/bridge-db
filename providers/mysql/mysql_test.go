package mysql

import (
	"strings"
	"testing"

	"github.com/pageton/bridge-db/internal/config"
)

// ---------------------------------------------------------------------------
// encodeMySQLRow / decodeMySQLRow
// ---------------------------------------------------------------------------

func TestEncodeMySQLRow_BasicRow(t *testing.T) {
	row := &mysqlRow{
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
			"name":  "varchar(255)",
			"email": "varchar(255)",
		},
	}

	data, err := encodeMySQLRow(row)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("encode produced empty bytes")
	}

	decoded, err := decodeMySQLRow(data)
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
}

func TestEncodeMySQLRow_RoundTripPreservesAllFields(t *testing.T) {
	original := &mysqlRow{
		Table: "orders",
		PrimaryKey: map[string]any{
			"order_id":  1001,
			"customer": "bob",
		},
		Data: map[string]any{
			"order_id":  1001,
			"customer": "bob",
			"total":     99.50,
			"active":    true,
		},
		ColumnTypes: map[string]string{
			"order_id": "int",
			"customer": "varchar(100)",
			"total":    "decimal(10,2)",
			"active":   "tinyint(1)",
		},
	}

	data, err := encodeMySQLRow(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeMySQLRow(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Table != "orders" {
		t.Errorf("table = %q, want %q", decoded.Table, "orders")
	}
	if len(decoded.PrimaryKey) != 2 {
		t.Fatalf("pk len = %d, want 2", len(decoded.PrimaryKey))
	}
	if len(decoded.Data) != 4 {
		t.Fatalf("data len = %d, want 4", len(decoded.Data))
	}
	if len(decoded.ColumnTypes) != 4 {
		t.Fatalf("column_types len = %d, want 4", len(decoded.ColumnTypes))
	}
}

func TestDecodeMySQLRow_InvalidJSON(t *testing.T) {
	_, err := decodeMySQLRow([]byte("not json at all"))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "decode mysql row") {
		t.Errorf("error = %q, want it to contain 'decode mysql row'", err.Error())
	}
}

func TestDecodeMySQLRow_EmptyBytes(t *testing.T) {
	_, err := decodeMySQLRow([]byte{})
	if err == nil {
		t.Fatal("expected error for empty bytes, got nil")
	}
}

// ---------------------------------------------------------------------------
// formatPrimaryKey
// ---------------------------------------------------------------------------

func TestFormatPrimaryKey_SingleColumn(t *testing.T) {
	got := formatPrimaryKey(map[string]any{"id": 42})
	want := "42"
	if got != want {
		t.Errorf("single column pk = %q, want %q", got, want)
	}
}

func TestFormatPrimaryKey_SingleColumnString(t *testing.T) {
	got := formatPrimaryKey(map[string]any{"id": "abc-123"})
	want := "abc-123"
	if got != want {
		t.Errorf("single string pk = %q, want %q", got, want)
	}
}

func TestFormatPrimaryKey_Composite(t *testing.T) {
	pk := map[string]any{
		"order_id":  1001,
		"customer": "bob",
	}
	got := formatPrimaryKey(pk)
	if !strings.Contains(got, "order_id") {
		t.Errorf("composite pk = %q, want to contain 'order_id'", got)
	}
	if !strings.Contains(got, "customer") {
		t.Errorf("composite pk = %q, want to contain 'customer'", got)
	}
	if !strings.Contains(got, "bob") {
		t.Errorf("composite pk = %q, want to contain 'bob'", got)
	}
}

func TestFormatPrimaryKey_Empty(t *testing.T) {
	got := formatPrimaryKey(map[string]any{})
	if got != "{}" {
		t.Errorf("empty pk = %q, want '{}'", got)
	}
}

func TestFormatPrimaryKey_Nil(t *testing.T) {
	got := formatPrimaryKey(nil)
	if got != "null" {
		t.Errorf("nil pk = %q, want 'null'", got)
	}
}

// ---------------------------------------------------------------------------
// buildRowKey
// ---------------------------------------------------------------------------

func TestBuildRowKey_SimplePK(t *testing.T) {
	got := buildRowKey("users", map[string]any{"id": 42})
	want := "users:42"
	if got != want {
		t.Errorf("row key = %q, want %q", got, want)
	}
}

func TestBuildRowKey_CompositePK(t *testing.T) {
	pk := map[string]any{"org_id": 1, "user_id": 5}
	got := buildRowKey("members", pk)
	if !strings.HasPrefix(got, "members:") {
		t.Errorf("row key = %q, want prefix 'members:'", got)
	}
	if !strings.Contains(got, "org_id") {
		t.Errorf("row key = %q, want to contain 'org_id'", got)
	}
}

// ---------------------------------------------------------------------------
// convertValue
// ---------------------------------------------------------------------------

func TestConvertValue_Nil(t *testing.T) {
	got := convertValue(nil)
	if got != nil {
		t.Errorf("nil input = %v, want nil", got)
	}
}

func TestConvertValue_ByteSliceToString(t *testing.T) {
	got := convertValue([]byte("hello"))
	s, ok := got.(string)
	if !ok {
		t.Fatalf("[]byte input type = %T, want string", got)
	}
	if s != "hello" {
		t.Errorf("[]byte input = %q, want %q", s, "hello")
	}
}

func TestConvertValue_EmptyByteSlice(t *testing.T) {
	got := convertValue([]byte{})
	s, ok := got.(string)
	if !ok {
		t.Fatalf("empty []byte type = %T, want string", got)
	}
	if s != "" {
		t.Errorf("empty []byte = %q, want empty string", s)
	}
}

func TestConvertValue_IntPassthrough(t *testing.T) {
	got := convertValue(42)
	if _, ok := got.(int); !ok {
		t.Fatalf("int input type = %T, want int", got)
	}
	if got != 42 {
		t.Errorf("int input = %v, want 42", got)
	}
}

func TestConvertValue_StringPassthrough(t *testing.T) {
	got := convertValue("hello")
	if _, ok := got.(string); !ok {
		t.Fatalf("string input type = %T, want string", got)
	}
	if got != "hello" {
		t.Errorf("string input = %v, want 'hello'", got)
	}
}

// ---------------------------------------------------------------------------
// quoteIdentifier
// ---------------------------------------------------------------------------

func TestQuoteIdentifier_Simple(t *testing.T) {
	got := quoteIdentifier("users")
	want := "`users`"
	if got != want {
		t.Errorf("quoteIdentifier('users') = %q, want %q", got, want)
	}
}

func TestQuoteIdentifier_WithBacktick(t *testing.T) {
	got := quoteIdentifier("my`table")
	want := "`my``table`"
	if got != want {
		t.Errorf("quoteIdentifier('my`table') = %q, want %q", got, want)
	}
}

func TestQuoteIdentifier_Empty(t *testing.T) {
	got := quoteIdentifier("")
	want := "``"
	if got != want {
		t.Errorf("quoteIdentifier('') = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// buildPlaceholders
// ---------------------------------------------------------------------------

func TestBuildPlaceholders_Zero(t *testing.T) {
	got := buildPlaceholders(0)
	want := ""
	if got != want {
		t.Errorf("buildPlaceholders(0) = %q, want %q", got, want)
	}
}

func TestBuildPlaceholders_One(t *testing.T) {
	got := buildPlaceholders(1)
	want := "?"
	if got != want {
		t.Errorf("buildPlaceholders(1) = %q, want %q", got, want)
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
	pk := map[string]any{"id": 42}
	where, args := buildPKWhere(pk, 1)
	wantWhere := "`id` = ?"
	if where != wantWhere {
		t.Errorf("where = %q, want %q", where, wantWhere)
	}
	if len(args) != 1 {
		t.Fatalf("args len = %d, want 1", len(args))
	}
	if args[0] != 42 {
		t.Errorf("args[0] = %v, want 42", args[0])
	}
}

func TestBuildPKWhere_CompositePK(t *testing.T) {
	pk := map[string]any{"org_id": 1, "user_id": 5}
	where, args := buildPKWhere(pk, 1)

	parts := strings.Split(where, " AND ")
	if len(parts) != 2 {
		t.Fatalf("where clause parts = %d, want 2: %q", len(parts), where)
	}

	if len(args) != 2 {
		t.Fatalf("args len = %d, want 2", len(args))
	}

	for _, part := range parts {
		if !strings.Contains(part, " = ?") {
			t.Errorf("part %q missing ' = ?'", part)
		}
	}
}

func TestBuildPKWhere_WithStartIdx(t *testing.T) {
	pk := map[string]any{"id": 99}
	where, args := buildPKWhere(pk, 3)
	if where != "`id` = ?" {
		t.Errorf("where = %q, want '`id` = ?'", where)
	}
	if len(args) != 1 || args[0] != 99 {
		t.Errorf("args = %v, want [99]", args)
	}
}

// ---------------------------------------------------------------------------
// parseRowKey
// ---------------------------------------------------------------------------

func TestParseRowKey_SingleColumnPK(t *testing.T) {
	table, pk, err := parseRowKey("users:42")
	if err != nil {
		t.Fatalf("parseRowKey: %v", err)
	}
	if table != "users" {
		t.Errorf("table = %q, want 'users'", table)
	}
	if pk["id"] != "42" {
		t.Errorf("pk = %v, want map with id='42'", pk)
	}
}

func TestParseRowKey_CompositeJSONPK(t *testing.T) {
	table, pk, err := parseRowKey(`orders:{"order_id":1001,"customer":"bob"}`)
	if err != nil {
		t.Fatalf("parseRowKey: %v", err)
	}
	if table != "orders" {
		t.Errorf("table = %q, want 'orders'", table)
	}
	if len(pk) != 2 {
		t.Fatalf("pk len = %d, want 2", len(pk))
	}
}

func TestParseRowKey_InvalidFormat(t *testing.T) {
	_, _, err := parseRowKey("nocolonhere")
	if err == nil {
		t.Fatal("expected error for key without colon, got nil")
	}
	if !strings.Contains(err.Error(), "invalid key format") {
		t.Errorf("error = %q, want 'invalid key format'", err.Error())
	}
}

func TestParseRowKey_InvalidJSON(t *testing.T) {
	_, _, err := parseRowKey("table:{not valid json}")
	if err == nil {
		t.Fatal("expected error for invalid JSON pk, got nil")
	}
	if !strings.Contains(err.Error(), "invalid primary key JSON") {
		t.Errorf("error = %q, want 'invalid primary key JSON'", err.Error())
	}
}

func TestParseRowKey_EmptyKey(t *testing.T) {
	_, _, err := parseRowKey("")
	if err == nil {
		t.Fatal("expected error for empty key, got nil")
	}
}

func TestParseRowKey_EmptyTable(t *testing.T) {
	table, pk, err := parseRowKey(":42")
	if err != nil {
		t.Fatalf("parseRowKey: %v", err)
	}
	if table != "" {
		t.Errorf("table = %q, want empty string", table)
	}
	if pk["id"] != "42" {
		t.Errorf("pk = %v, want map with id='42'", pk)
	}
}

// ---------------------------------------------------------------------------
// buildMySQLDSN
// ---------------------------------------------------------------------------

func TestBuildMySQLDSN_ContainsRequiredParams(t *testing.T) {
	cfg := &config.MySQLConfig{
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "secret",
		Database: "mydb",
	}

	dsn := buildMySQLDSN(cfg)

	if !strings.Contains(dsn, "tcp(localhost:3306)") {
		t.Errorf("DSN = %q, want to contain 'tcp(localhost:3306)'", dsn)
	}
	if !strings.Contains(dsn, "parseTime=true") {
		t.Errorf("DSN = %q, want to contain 'parseTime=true'", dsn)
	}
	if !strings.Contains(dsn, "multiStatements=true") {
		t.Errorf("DSN = %q, want to contain 'multiStatements=true'", dsn)
	}
	if !strings.Contains(dsn, "root:secret@") {
		t.Errorf("DSN = %q, want to contain 'root:secret@'", dsn)
	}
	if !strings.Contains(dsn, "/mydb") {
		t.Errorf("DSN = %q, want to contain '/mydb'", dsn)
	}
}

func TestBuildMySQLDSN_QueryParamsAfterDSNBase(t *testing.T) {
	cfg := &config.MySQLConfig{
		Host:     "10.0.0.1",
		Port:     3307,
		Username: "admin",
		Password: "pass",
		Database: "production",
	}

	dsn := buildMySQLDSN(cfg)

	if !strings.HasPrefix(dsn, "admin:pass@tcp(10.0.0.1:3307)/production") {
		t.Errorf("DSN prefix = %q, unexpected", dsn)
	}
	if dsn[len(dsn)-len("?parseTime=true&multiStatements=true"):] != "?parseTime=true&multiStatements=true" {
		t.Errorf("DSN suffix = %q, want '?parseTime=true&multiStatements=true'", dsn)
	}
}
