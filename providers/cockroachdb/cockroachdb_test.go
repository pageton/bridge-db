package cockroachdb

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/pageton/bridge-db/internal/config"
)

func TestEncodeCockroachDBRow_RoundTrip(t *testing.T) {
	original := &cockroachDBRow{
		Table:  "users",
		Schema: "public",
		PrimaryKey: map[string]any{
			"id": 42,
		},
		Data: map[string]any{
			"id":   42,
			"name": "alice",
		},
		ColumnTypes: map[string]string{
			"id":   "INT",
			"name": "TEXT",
		},
	}

	encoded, err := encodeCockroachDBRow(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeCockroachDBRow(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Table != original.Table {
		t.Errorf("table = %q, want %q", decoded.Table, original.Table)
	}
	if decoded.Schema != original.Schema {
		t.Errorf("schema = %q, want %q", decoded.Schema, original.Schema)
	}
	if len(decoded.ColumnTypes) != len(original.ColumnTypes) {
		t.Errorf("column_types count = %d, want %d", len(decoded.ColumnTypes), len(original.ColumnTypes))
	}
}

func TestEncodeCockroachDBRow_EmptyRow(t *testing.T) {
	row := &cockroachDBRow{}
	encoded, err := encodeCockroachDBRow(row)
	if err != nil {
		t.Fatalf("encode empty row: %v", err)
	}
	decoded, err := decodeCockroachDBRow(encoded)
	if err != nil {
		t.Fatalf("decode empty row: %v", err)
	}
	if decoded.Table != "" {
		t.Errorf("table = %q, want empty", decoded.Table)
	}
}

func TestDecodeCockroachDBRow_InvalidJSON(t *testing.T) {
	_, err := decodeCockroachDBRow([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "decode cockroachdb row") {
		t.Errorf("error = %v, want 'decode cockroachdb row' wrapper", err)
	}
}

func TestDecodeCockroachDBRow_InvalidBytes(t *testing.T) {
	_, err := decodeCockroachDBRow(nil)
	if err == nil {
		t.Fatal("expected error for nil input")
	}
}

func TestFormatPrimaryKey_SingleColumn(t *testing.T) {
	got := formatPrimaryKey(map[string]any{"id": 42})
	if got != "42" {
		t.Errorf("single PK = %q, want %q", got, "42")
	}
}

func TestFormatPrimaryKey_Composite(t *testing.T) {
	got := formatPrimaryKey(map[string]any{"org_id": 1, "user_id": 5})
	if got != `{"org_id":1,"user_id":5}` && got != `{"user_id":5,"org_id":1}` {
		t.Errorf("composite PK = %q, want JSON object", got)
	}
}

func TestFormatPrimaryKey_Empty(t *testing.T) {
	got := formatPrimaryKey(map[string]any{})
	if got != "{}" {
		t.Errorf("empty PK = %q, want %q", got, "{}")
	}
}

func TestBuildRowKey_PublicSchema(t *testing.T) {
	got := buildRowKey("public", "users", map[string]any{"id": 1})
	want := "users:1"
	if got != want {
		t.Errorf("public schema key = %q, want %q", got, want)
	}
}

func TestBuildRowKey_NonPublicSchema(t *testing.T) {
	got := buildRowKey("myapp", "orders", map[string]any{"id": 99})
	want := "myapp.orders:99"
	if got != want {
		t.Errorf("non-public schema key = %q, want %q", got, want)
	}
}

func TestBuildRowKey_EmptySchema(t *testing.T) {
	got := buildRowKey("", "items", map[string]any{"id": 7})
	want := "items:7"
	if got != want {
		t.Errorf("empty schema key = %q, want %q", got, want)
	}
}

func TestConvertValue_Nil(t *testing.T) {
	got := convertValue(nil, "INT")
	if got != nil {
		t.Errorf("nil input = %v, want nil", got)
	}
}

func TestConvertValue_ByteSlice(t *testing.T) {
	got := convertValue([]byte{0xde, 0xad, 0xbe, 0xef}, "BYTEA")
	want := `\xdeadbeef`
	if got != want {
		t.Errorf("[]byte = %q, want %q", got, want)
	}
}

func TestConvertValue_StringPassthrough(t *testing.T) {
	got := convertValue("hello", "TEXT")
	if got != "hello" {
		t.Errorf("string = %q, want %q", got, "hello")
	}
}

func TestConvertValue_IntPassthrough(t *testing.T) {
	got := convertValue(42, "INT")
	if got != 42 {
		t.Errorf("int = %v, want 42", got)
	}
}

func TestConvertValue_Numeric(t *testing.T) {
	n := pgtype.Numeric{}
	if err := n.Scan("3.14"); err != nil {
		t.Fatalf("scan numeric: %v", err)
	}
	got := convertValue(n, "NUMERIC")
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("numeric type = %T, want float64", got)
	}
	if f != 3.14 {
		t.Errorf("numeric value = %v, want 3.14", f)
	}
}

func TestConvertValue_NumericInvalid(t *testing.T) {
	n := pgtype.Numeric{Valid: false}
	got := convertValue(n, "NUMERIC")
	if got != nil {
		t.Errorf("invalid numeric = %v, want nil", got)
	}
}

func TestConvertValue_NumericNaN(t *testing.T) {
	n := pgtype.Numeric{Valid: true, NaN: true}
	got := convertValue(n, "NUMERIC")
	if got != "NaN" {
		t.Errorf("NaN numeric = %v, want 'NaN'", got)
	}
}

func TestConvertValue_Timestamptz(t *testing.T) {
	now := time.Date(2026, 4, 11, 12, 0, 0, 123456789, time.UTC)
	n := pgtype.Timestamptz{Time: now, Valid: true}
	got := convertValue(n, "TIMESTAMPTZ")
	s, ok := got.(string)
	if !ok {
		t.Fatalf("timestamptz type = %T, want string", got)
	}
	if !strings.HasPrefix(s, "2026-04-11T12:00:00") {
		t.Errorf("timestamptz value = %q, want RFC3339Nano starting with 2026-04-11T12:00:00", s)
	}
}

func TestConvertValue_Timestamp(t *testing.T) {
	now := time.Date(2026, 1, 15, 8, 30, 0, 0, time.UTC)
	ts := pgtype.Timestamp{Time: now, Valid: true}
	got := convertValue(ts, "TIMESTAMP")
	s, ok := got.(string)
	if !ok {
		t.Fatalf("timestamp type = %T, want string", got)
	}
	if !strings.HasPrefix(s, "2026-01-15T08:30:00") {
		t.Errorf("timestamp value = %q, want starting with 2026-01-15T08:30:00", s)
	}
}

func TestConvertValue_Date(t *testing.T) {
	d := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	pg := pgtype.Date{Time: d, Valid: true}
	got := convertValue(pg, "DATE")
	if got != "2026-06-01" {
		t.Errorf("date = %v, want %q", got, "2026-06-01")
	}
}

func TestConvertValue_Time(t *testing.T) {
	pg := pgtype.Time{Microseconds: 36610000000, Valid: true}
	got := convertValue(pg, "TIME")
	m, ok := got.(int64)
	if !ok {
		t.Fatalf("time type = %T, want int64", got)
	}
	if m != 36610000000 {
		t.Errorf("time microseconds = %d, want 36610000000", m)
	}
}

func TestConvertValue_UUID(t *testing.T) {
	u := pgtype.UUID{Bytes: [16]byte{0x55, 0x55, 0x55, 0x55, 0x66, 0x66, 0x66, 0x66, 0x77, 0x77, 0x77, 0x77, 0x88, 0x88, 0x88, 0x88}, Valid: true}
	got := convertValue(u, "UUID")
	expected := u.String()
	if got != expected {
		t.Errorf("uuid = %v, want %v", got, expected)
	}
}

func TestConvertValue_UUIDInvalid(t *testing.T) {
	u := pgtype.UUID{Valid: false}
	got := convertValue(u, "UUID")
	if got != nil {
		t.Errorf("invalid uuid = %v, want nil", got)
	}
}

func TestQuoteIdentifier_Simple(t *testing.T) {
	got := quoteIdentifier("users")
	if got != `"users"` {
		t.Errorf("simple = %q, want %q", got, `"users"`)
	}
}

func TestQuoteIdentifier_WithDoubleQuote(t *testing.T) {
	got := quoteIdentifier(`col"umn`)
	if got != `"col""umn"` {
		t.Errorf("with quote = %q, want %q", got, `"col""umn"`)
	}
}

func TestQuoteIdentifier_Empty(t *testing.T) {
	got := quoteIdentifier("")
	if got != `""` {
		t.Errorf("empty = %q, want %q", got, `""`)
	}
}

func TestParseTableKey_WithSchema(t *testing.T) {
	schema, table := parseTableKey("myapp.users")
	if schema != "myapp" {
		t.Errorf("schema = %q, want %q", schema, "myapp")
	}
	if table != "users" {
		t.Errorf("table = %q, want %q", table, "users")
	}
}

func TestParseTableKey_NoSchema(t *testing.T) {
	schema, table := parseTableKey("orders")
	if schema != "public" {
		t.Errorf("schema = %q, want %q", schema, "public")
	}
	if table != "orders" {
		t.Errorf("table = %q, want %q", table, "orders")
	}
}

func TestParseTableKey_SchemaWithDots(t *testing.T) {
	schema, table := parseTableKey("schema.with.dots.table")
	if schema != "schema" {
		t.Errorf("schema = %q, want %q", schema, "schema")
	}
	if table != "with.dots.table" {
		t.Errorf("table = %q, want %q", table, "with.dots.table")
	}
}

func TestBuildPlaceholders_Zero(t *testing.T) {
	got := buildPlaceholders(0)
	if got != "" {
		t.Errorf("zero placeholders = %q, want empty", got)
	}
}

func TestBuildPlaceholders_One(t *testing.T) {
	got := buildPlaceholders(1)
	if got != "$1" {
		t.Errorf("one placeholder = %q, want %q", got, "$1")
	}
}

func TestBuildPlaceholders_Three(t *testing.T) {
	got := buildPlaceholders(3)
	if got != "$1, $2, $3" {
		t.Errorf("three placeholders = %q, want %q", got, "$1, $2, $3")
	}
}

func TestBuildPKWhere_SinglePK(t *testing.T) {
	pk := map[string]any{"id": 42}
	clause, args := buildPKWhere(pk, 1)
	if clause != `"id" = $1` {
		t.Errorf("clause = %q, want %q", clause, `"id" = $1`)
	}
	if len(args) != 1 || args[0] != 42 {
		t.Errorf("args = %v, want [42]", args)
	}
}

func TestBuildPKWhere_CompositePKWithOffset(t *testing.T) {
	pk := map[string]any{"org_id": 1, "user_id": 5}
	clause, args := buildPKWhere(pk, 3)
	if !strings.Contains(clause, "$3") || !strings.Contains(clause, "$4") {
		t.Errorf("clause = %q, want $3 and $4 placeholders", clause)
	}
	if len(args) != 2 {
		t.Errorf("args count = %d, want 2", len(args))
	}
}

func TestBuildPKWhere_EmptyPK(t *testing.T) {
	clause, args := buildPKWhere(map[string]any{}, 1)
	if clause != "" {
		t.Errorf("empty clause = %q, want empty", clause)
	}
	if len(args) != 0 {
		t.Errorf("empty args = %v, want empty", args)
	}
}

func TestParseRowKey_PublicTable(t *testing.T) {
	schema, table, pk, err := parseRowKey("public.users:42")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if schema != "public" {
		t.Errorf("schema = %q, want %q", schema, "public")
	}
	if table != "users" {
		t.Errorf("table = %q, want %q", table, "users")
	}
	if pk["id"] != int64(42) {
		t.Errorf("pk = %v, want map[id:42]", pk)
	}
}

func TestParseRowKey_SchemaTable(t *testing.T) {
	schema, table, pk, err := parseRowKey("myapp.orders:99")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if schema != "myapp" {
		t.Errorf("schema = %q, want %q", schema, "myapp")
	}
	if table != "orders" {
		t.Errorf("table = %q, want %q", table, "orders")
	}
	if pk["id"] != int64(99) {
		t.Errorf("pk = %v, want map[id:99]", pk)
	}
}

func TestParseRowKey_CompositePK(t *testing.T) {
	_, _, pk, err := parseRowKey(`myapp.items:{"org_id":1,"user_id":5}`)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(pk) != 2 {
		t.Errorf("pk len = %d, want 2", len(pk))
	}
}

func TestParseRowKey_InvalidFormat(t *testing.T) {
	_, _, _, err := parseRowKey("no_colon_here")
	if err == nil {
		t.Fatal("expected error for missing colon")
	}
	if !strings.Contains(err.Error(), "invalid key format") {
		t.Errorf("error = %v, want 'invalid key format'", err)
	}
}

func TestParseRowKey_InvalidJSON(t *testing.T) {
	_, _, _, err := parseRowKey("myapp.users:{bad_json}")
	if err == nil {
		t.Fatal("expected error for invalid JSON in PK")
	}
	if !strings.Contains(err.Error(), "invalid primary key JSON") {
		t.Errorf("error = %v, want 'invalid primary key JSON'", err)
	}
}

func TestParsePrimaryKeyValue_Integer(t *testing.T) {
	got := parsePrimaryKeyValue("42")
	if got != int64(42) {
		t.Errorf("integer = %v (%T), want int64(42)", got, got)
	}
}

func TestParsePrimaryKeyValue_NegativeInteger(t *testing.T) {
	got := parsePrimaryKeyValue("-5")
	if got != int64(-5) {
		t.Errorf("negative int = %v (%T), want int64(-5)", got, got)
	}
}

func TestParsePrimaryKeyValue_Float(t *testing.T) {
	got := parsePrimaryKeyValue("3.14")
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("float type = %T, want float64", got)
	}
	if f != 3.14 {
		t.Errorf("float = %v, want 3.14", f)
	}
}

func TestParsePrimaryKeyValue_BoolTrue(t *testing.T) {
	got := parsePrimaryKeyValue("true")
	if got != true {
		t.Errorf("true = %v (%T), want bool true", got, got)
	}
}

func TestParsePrimaryKeyValue_BoolFalse(t *testing.T) {
	got := parsePrimaryKeyValue("false")
	if got != false {
		t.Errorf("false = %v (%T), want bool false", got, got)
	}
}

func TestParsePrimaryKeyValue_String(t *testing.T) {
	got := parsePrimaryKeyValue("abc-123")
	if got != "abc-123" {
		t.Errorf("string = %v, want %q", got, "abc-123")
	}
}

func TestParsePrimaryKeyValue_UUIDString(t *testing.T) {
	got := parsePrimaryKeyValue("550e8400-e29b-41d4-a716-446655440000")
	if got != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("uuid string = %v, want uuid string", got)
	}
}

func TestCrdbSafeDefault_EmptyRejected(t *testing.T) {
	_, ok := crdbSafeDefault("")
	if ok {
		t.Error("empty default should be rejected")
	}
}

func TestCrdbSafeDefault_NextvalAccepted(t *testing.T) {
	got, ok := crdbSafeDefault("nextval('users_id_seq')")
	if !ok {
		t.Fatal("NEXTVAL should be accepted")
	}
	if got != "nextval('users_id_seq')" {
		t.Errorf("NEXTVAL = %q, want original", got)
	}
}

func TestCrdbSafeDefault_CurrvalAccepted(t *testing.T) {
	got, ok := crdbSafeDefault("currval('seq')")
	if !ok {
		t.Fatal("CURRVAL should be accepted")
	}
	if got != "currval('seq')" {
		t.Errorf("CURRVAL = %q, want original", got)
	}
}

func TestCrdbSafeDefault_SetvalAccepted(t *testing.T) {
	got, ok := crdbSafeDefault("setval('seq', 1)")
	if !ok {
		t.Fatal("SETVAL should be accepted")
	}
	if got != "setval('seq', 1)" {
		t.Errorf("SETVAL = %q, want original", got)
	}
}

func TestCrdbSafeDefault_NowNormalized(t *testing.T) {
	got, ok := crdbSafeDefault("NOW()")
	if !ok {
		t.Fatal("NOW() should be accepted")
	}
	if got != "CURRENT_TIMESTAMP" {
		t.Errorf("NOW() = %q, want %q", got, "CURRENT_TIMESTAMP")
	}
}

func TestCrdbSafeDefault_CurrentTimestampNormalized(t *testing.T) {
	got, ok := crdbSafeDefault("CURRENT_TIMESTAMP()")
	if !ok {
		t.Fatal("CURRENT_TIMESTAMP() should be accepted")
	}
	if got != "CURRENT_TIMESTAMP" {
		t.Errorf("CURRENT_TIMESTAMP() = %q, want %q", got, "CURRENT_TIMESTAMP")
	}
}

func TestCrdbSafeDefault_LiteralAccepted(t *testing.T) {
	got, ok := crdbSafeDefault("'hello'")
	if !ok {
		t.Fatal("literal string should be accepted")
	}
	if got != "'hello'" {
		t.Errorf("literal = %q, want %q", got, "'hello'")
	}
}

func TestCrdbSafeDefault_NumericLiteralAccepted(t *testing.T) {
	got, ok := crdbSafeDefault("42")
	if !ok {
		t.Fatal("numeric literal should be accepted")
	}
	if got != "42" {
		t.Errorf("numeric = %q, want %q", got, "42")
	}
}

func TestCrdbSafeDefault_DatetimeRejected(t *testing.T) {
	_, ok := crdbSafeDefault("DATETIME('now')")
	if ok {
		t.Error("DATETIME should be rejected")
	}
}

func TestCrdbSafeDefault_SQLiteFunctionRejected(t *testing.T) {
	_, ok := crdbSafeDefault("SQLITE_VERSION()")
	if ok {
		t.Error("SQLITE_ functions should be rejected")
	}
}

func TestCrdbSafeDefault_AutoincrementRejected(t *testing.T) {
	_, ok := crdbSafeDefault("AUTOINCREMENT")
	if ok {
		t.Error("AUTOINCREMENT should be rejected")
	}
}

func TestCrdbSafeDefault_UnknownFunctionRejected(t *testing.T) {
	_, ok := crdbSafeDefault("UNKNOWN_FUNC()")
	if ok {
		t.Error("unknown function with () should be rejected")
	}
}

func TestBuildCockroachDBConnStr_Basic(t *testing.T) {
	cfg := &config.CockroachDBConfig{
		Host:     "localhost",
		Port:     26257,
		Database: "mydb",
		SSLMode:  "disable",
	}
	got := buildCockroachDBConnStr(cfg)
	if !strings.Contains(got, "host=localhost") {
		t.Errorf("missing host in %q", got)
	}
	if !strings.Contains(got, "port=26257") {
		t.Errorf("missing port in %q", got)
	}
	if !strings.Contains(got, "dbname=mydb") {
		t.Errorf("missing dbname in %q", got)
	}
	if !strings.Contains(got, "sslmode=disable") {
		t.Errorf("missing sslmode in %q", got)
	}
}

func TestBuildCockroachDBConnStr_WithAuth(t *testing.T) {
	cfg := &config.CockroachDBConfig{
		Host:     "10.0.0.1",
		Port:     26257,
		Database: "prod",
		Username: "admin",
		Password: "secret",
		SSLMode:  "require",
	}
	got := buildCockroachDBConnStr(cfg)
	if !strings.Contains(got, "user=admin") {
		t.Errorf("missing user in %q", got)
	}
	if !strings.Contains(got, "password=secret") {
		t.Errorf("missing password in %q", got)
	}
}

func TestBuildCockroachDBConnStr_Minimal(t *testing.T) {
	cfg := &config.CockroachDBConfig{
		Host: "localhost",
		Port: 26257,
	}
	got := buildCockroachDBConnStr(cfg)
	if !strings.HasPrefix(got, "host=localhost port=26257") {
		t.Errorf("minimal connstr = %q, want host/port prefix", got)
	}
	if strings.Contains(got, "dbname=") {
		t.Errorf("minimal should not contain dbname: %q", got)
	}
}
