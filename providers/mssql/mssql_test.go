//go:build mssql

package mssql

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/pkg/provider"
)

type stubExecResult int64

func (r stubExecResult) LastInsertId() (int64, error) { return 0, nil }
func (r stubExecResult) RowsAffected() (int64, error) { return int64(r), nil }

type captureExecutor struct {
	query string
	args  []any
	res   sql.Result
	err   error
}

func (e *captureExecutor) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query = query
	e.args = append([]any(nil), args...)
	if e.res == nil {
		e.res = stubExecResult(1)
	}
	return e.res, e.err
}

type scriptedExecCall struct {
	res sql.Result
	err error
}

type scriptedExecutor struct {
	queries []string
	args    [][]any
	calls   []scriptedExecCall
	idx     int
}

type nopWriterLogger struct{}

func (nopWriterLogger) Info(string, ...any)  {}
func (nopWriterLogger) Debug(string, ...any) {}

func (e *scriptedExecutor) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.queries = append(e.queries, query)
	e.args = append(e.args, append([]any(nil), args...))
	if e.idx >= len(e.calls) {
		return stubExecResult(1), nil
	}
	call := e.calls[e.idx]
	e.idx++
	if call.res == nil {
		call.res = stubExecResult(1)
	}
	return call.res, call.err
}

func TestMSSQLSafeDefault_Now(t *testing.T) {
	got, ok := mssqlSafeDefault("now()")
	if !ok {
		t.Fatal("expected now() to be accepted")
	}
	if got != "CURRENT_TIMESTAMP" {
		t.Fatalf("mssqlSafeDefault(now()) = %q, want CURRENT_TIMESTAMP", got)
	}
}

func TestMSSQLSafeDefault_CurrentTimestamp(t *testing.T) {
	got, ok := mssqlSafeDefault("CURRENT_TIMESTAMP")
	if !ok {
		t.Fatal("expected CURRENT_TIMESTAMP to be accepted")
	}
	if got != "CURRENT_TIMESTAMP" {
		t.Fatalf("mssqlSafeDefault(CURRENT_TIMESTAMP) = %q, want CURRENT_TIMESTAMP", got)
	}
}

func TestMSSQLSafeDefault_RejectsUnknownFunction(t *testing.T) {
	if got, ok := mssqlSafeDefault("unknown_func()"); ok {
		t.Fatalf("expected unknown_func() to be rejected, got %q", got)
	}
}

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
	want := []string{"org_id", "user_id"}
	for i, col := range want {
		if cols[i] != col {
			t.Fatalf("cols[%d] = %q, want %q (full=%v)", i, cols[i], col, cols)
		}
	}
}

func TestExtractPKColumns_Nil(t *testing.T) {
	row := mssqlRow{}
	cols := extractPKColumns(row)
	if cols != nil {
		t.Errorf("extractPKColumns(empty) = %v, want nil", cols)
	}
}

func TestExecUpdateFallback_UsesDirectUpdateQuery(t *testing.T) {
	w := &mssqlWriter{opts: provider.WriteOptions{}}
	row := mssqlRow{
		Data: map[string]any{
			"id":     3.0,
			"status": "delivered",
			"total":  12.5,
		},
		ColumnTypes: map[string]string{
			"id":     "integer",
			"status": "nvarchar",
			"total":  "numeric",
		},
	}
	exec := &captureExecutor{}

	err := w.execUpdateFallback(context.Background(), exec, "orders", row, []string{"id", "status", "total"}, []string{"id"})
	if err != nil {
		t.Fatalf("execUpdateFallback: %v", err)
	}

	wantQuery := "UPDATE [orders] SET [status] = @p1, [total] = @p2 WHERE [id] = @p3"
	if exec.query != wantQuery {
		t.Fatalf("query = %q, want %q", exec.query, wantQuery)
	}
	if len(exec.args) != 3 {
		t.Fatalf("len(args) = %d, want 3", len(exec.args))
	}
	if exec.args[0] != "delivered" {
		t.Fatalf("args[0] = %#v, want %q", exec.args[0], "delivered")
	}
	if exec.args[1] != 12.5 {
		t.Fatalf("args[1] = %#v, want %v", exec.args[1], 12.5)
	}
	if got, ok := exec.args[2].(int64); !ok || got != 3 {
		t.Fatalf("args[2] = %#v (type %T), want int64(3)", exec.args[2], exec.args[2])
	}
}

func TestExecUpdateFallback_ReturnsErrNoRowsWhenNothingMatched(t *testing.T) {
	w := &mssqlWriter{}
	row := mssqlRow{
		Data:        map[string]any{"id": 3.0, "status": "delivered"},
		ColumnTypes: map[string]string{"id": "integer", "status": "nvarchar"},
	}
	exec := &captureExecutor{res: stubExecResult(0)}

	err := w.execUpdateFallback(context.Background(), exec, "orders", row, []string{"id", "status"}, []string{"id"})
	if err != sql.ErrNoRows {
		t.Fatalf("execUpdateFallback err = %v, want %v", err, sql.ErrNoRows)
	}
}

func TestPrepareValue_CoercesPostgresIntegerAliases(t *testing.T) {
	tests := []struct {
		name    string
		colType string
	}{
		{name: "int2", colType: "int2"},
		{name: "int4", colType: "int4"},
		{name: "int8", colType: "int8"},
		{name: "int4 with modifier", colType: "int4(10)"},
	}

	w := &mssqlWriter{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := mssqlRow{
				Data:        map[string]any{"id": 7.0},
				ColumnTypes: map[string]string{"id": tt.colType},
			}

			got := w.prepareValue("id", row)
			if v, ok := got.(int64); !ok || v != 7 {
				t.Fatalf("prepareValue() = %#v (type %T), want int64(7)", got, got)
			}
		})
	}
}

func TestExecUpdateFallback_CoercesPostgresInt4PrimaryKey(t *testing.T) {
	w := &mssqlWriter{opts: provider.WriteOptions{}}
	row := mssqlRow{
		Data: map[string]any{
			"id":     3.0,
			"status": "delivered",
		},
		ColumnTypes: map[string]string{
			"id":     "int4",
			"status": "nvarchar",
		},
	}
	exec := &captureExecutor{}

	err := w.execUpdateFallback(context.Background(), exec, "orders", row, []string{"id", "status"}, []string{"id"})
	if err != nil {
		t.Fatalf("execUpdateFallback: %v", err)
	}

	if len(exec.args) != 2 {
		t.Fatalf("len(args) = %d, want 2", len(exec.args))
	}
	if got, ok := exec.args[1].(int64); !ok || got != 3 {
		t.Fatalf("args[1] = %#v (type %T), want int64(3)", exec.args[1], exec.args[1])
	}
}

func TestExecUpdateThenInsertUpsert_UpdatesExistingRowWithoutInsert(t *testing.T) {
	w := &mssqlWriter{opts: provider.WriteOptions{}, log: nopWriterLogger{}}
	row := mssqlRow{
		PrimaryKey: map[string]any{"id": 3.0},
		Data: map[string]any{
			"id":     3.0,
			"status": "delivered",
		},
		ColumnTypes: map[string]string{
			"id":     "int4",
			"status": "nvarchar",
		},
	}
	exec := &scriptedExecutor{calls: []scriptedExecCall{{res: stubExecResult(1)}}}

	err := w.execUpdateThenInsertUpsert(context.Background(), exec, "orders", row, []string{"id", "status"}, []string{"id"})
	if err != nil {
		t.Fatalf("execUpdateThenInsertUpsert: %v", err)
	}
	if len(exec.queries) != 1 {
		t.Fatalf("exec count = %d, want 1", len(exec.queries))
	}
	if !strings.HasPrefix(exec.queries[0], "UPDATE [orders] SET") {
		t.Fatalf("first query = %q, want UPDATE", exec.queries[0])
	}
}

func TestExecUpdateThenInsertUpsert_RetriesUpdateAfterDuplicateInsert(t *testing.T) {
	w := &mssqlWriter{opts: provider.WriteOptions{}, log: nopWriterLogger{}}
	row := mssqlRow{
		Table:      "orders",
		PrimaryKey: map[string]any{"id": 3.0},
		Data: map[string]any{
			"id":     3.0,
			"status": "delivered",
		},
		ColumnTypes: map[string]string{
			"id":     "int4",
			"status": "nvarchar",
		},
	}
	exec := &scriptedExecutor{calls: []scriptedExecCall{
		{res: stubExecResult(0)},
		{err: &mssqlDuplicateKeyError{}},
		{res: stubExecResult(1)},
	}}

	err := w.execUpdateThenInsertUpsert(context.Background(), exec, "orders", row, []string{"id", "status"}, []string{"id"})
	if err != nil {
		t.Fatalf("execUpdateThenInsertUpsert: %v", err)
	}
	if len(exec.queries) != 3 {
		t.Fatalf("exec count = %d, want 3", len(exec.queries))
	}
	if !strings.HasPrefix(exec.queries[0], "UPDATE [orders] SET") {
		t.Fatalf("query[0] = %q, want UPDATE", exec.queries[0])
	}
	if !strings.HasPrefix(exec.queries[1], "INSERT INTO [orders]") {
		t.Fatalf("query[1] = %q, want INSERT", exec.queries[1])
	}
	if !strings.HasPrefix(exec.queries[2], "UPDATE [orders] SET") {
		t.Fatalf("query[2] = %q, want UPDATE", exec.queries[2])
	}
}

type mssqlDuplicateKeyError struct{}

func (*mssqlDuplicateKeyError) Error() string {
	return "Violation of PRIMARY KEY constraint 'PK_orders'. Cannot insert duplicate key in object 'dbo.orders'"
}

func TestChoosePKColumns_PrefersDestinationPrimaryKey(t *testing.T) {
	row := mssqlRow{PrimaryKey: map[string]any{"id": 3, "status": "delivered"}}
	got := choosePKColumns([]string{"id"}, row, []string{"id", "status"})
	if len(got) != 1 || got[0] != "id" {
		t.Fatalf("choosePKColumns() = %v, want [id]", got)
	}
}

func TestChoosePKColumns_FallsBackToRowPrimaryKey(t *testing.T) {
	row := mssqlRow{PrimaryKey: map[string]any{"org_id": 1, "user_id": 5}}
	got := choosePKColumns(nil, row, []string{"org_id", "user_id", "status"})
	if len(got) != 2 || got[0] != "org_id" || got[1] != "user_id" {
		t.Fatalf("choosePKColumns() = %v, want [org_id user_id]", got)
	}
}

func TestEffectivePrimaryKey_PrefersDestinationPrimaryKey(t *testing.T) {
	row := mssqlRow{
		PrimaryKey: map[string]any{"id": 118, "email": "alice@example.com", "status": "active"},
		Data:       map[string]any{"id": 118, "email": "alice@example.com", "status": "active"},
	}
	got := effectivePrimaryKey([]string{"id"}, row, []string{"id", "email", "status"})
	if len(got) != 1 || got["id"] != 118 {
		t.Fatalf("effectivePrimaryKey() = %v, want map[id:118]", got)
	}
}

func TestNormalizeMSSQLRows_RewritesMalformedPrimaryKeyBeforeDedup(t *testing.T) {
	rows := []mssqlRow{
		{Table: "users", PrimaryKey: map[string]any{"id": 118, "email": "alice@example.com", "status": "active"}, Data: map[string]any{"id": 118, "email": "alice@example.com", "status": "active"}},
		{Table: "users", PrimaryKey: map[string]any{"id": 118, "email": "alice@example.com", "status": "active"}, Data: map[string]any{"id": 118, "email": "alice@example.com", "status": "active"}},
	}

	normalized := normalizeMSSQLRows(rows, []string{"id"}, []string{"id", "email", "status"})
	if got := buildRowKey("users", normalized[0].PrimaryKey); got != "users:118" {
		t.Fatalf("normalized key = %q, want %q", got, "users:118")
	}
	deduped := dedupMSSQLRows(normalized)
	if len(deduped) != 1 {
		t.Fatalf("dedupMSSQLRows() len = %d, want 1", len(deduped))
	}
}

func TestIsRetryableMSSQLError_Deadlock(t *testing.T) {
	msg := strings.ToLower("mssql: Transaction (Process ID 57) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction.")
	if !isRetryableMSSQLError(msg) {
		t.Fatal("expected deadlock error to be retryable")
	}
}

func TestIsRetryableMSSQLError_DuplicateKeyNotRetryable(t *testing.T) {
	msg := strings.ToLower("Violation of PRIMARY KEY constraint 'PK_orders'. Cannot insert duplicate key in object 'dbo.orders'")
	if isRetryableMSSQLError(msg) {
		t.Fatal("expected duplicate key error to be non-retryable")
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
	cfg := config.DefaultMSSQLConfig()
	cfg.Host = "localhost"
	cfg.Port = config.IntPtr(1433)
	cfg.Username = "sa"
	cfg.Password = "Secret123!"
	cfg.Database = "mydb"
	cfg.Encrypt = config.BoolPtr(true)
	dsn := buildMSSQLDSN(&cfg)
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
	cfg := config.DefaultMSSQLConfig()
	cfg.Host = "10.0.0.1"
	cfg.Port = config.IntPtr(1433)
	cfg.Username = "admin"
	cfg.Password = "pw"
	cfg.Database = "testdb"
	cfg.Encrypt = config.BoolPtr(true)
	cfg.TrustCert = config.BoolPtr(true)
	dsn := buildMSSQLDSN(&cfg)
	if !strings.Contains(dsn, "TrustServerCertificate=true") {
		t.Errorf("DSN should contain TrustServerCertificate=true, got %q", dsn)
	}
}

func TestBuildMSSQLDSN_NoEncrypt(t *testing.T) {
	cfg := config.DefaultMSSQLConfig()
	cfg.Host = "localhost"
	cfg.Port = config.IntPtr(1433)
	cfg.Username = "sa"
	cfg.Password = "pw"
	cfg.Database = "db"
	cfg.Encrypt = config.BoolPtr(false)
	dsn := buildMSSQLDSN(&cfg)
	if strings.Contains(dsn, "encrypt") {
		t.Errorf("DSN should not contain encrypt param, got %q", dsn)
	}
}

func TestEstimateChunkSize_RespectsParameterLimit(t *testing.T) {
	w := &mssqlWriter{}
	rows := make([]mssqlRow, 500)
	columns := []string{"c1", "c2", "c3", "c4", "c5"}

	got := w.estimateChunkSize(rows, columns)
	if got != 419 {
		t.Fatalf("estimateChunkSize() = %d, want 419", got)
	}
}
