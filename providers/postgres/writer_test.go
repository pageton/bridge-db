package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestBuildCreateTableStatements_FromSQLRowEnvelope(t *testing.T) {
	row := postgresRow{
		Schema: "public",
		Table:  "users",
		PrimaryKey: map[string]any{
			"_id": "507f1f77bcf86cd799439011",
		},
		Data: map[string]any{
			"_id":   "507f1f77bcf86cd799439011",
			"name":  "alice",
			"roles": "[\"admin\"]",
		},
		ColumnTypes: map[string]string{
			"_id":   "TEXT",
			"name":  "TEXT",
			"roles": "TEXT",
		},
	}

	stmts := buildCreateTableStatements("public", "users", row, []string{"_id", "name", "roles"})
	if len(stmts) != 2 {
		t.Fatalf("statement count = %d, want 2", len(stmts))
	}
	if stmts[0] != `CREATE SCHEMA IF NOT EXISTS "public"` {
		t.Fatalf("schema statement = %q", stmts[0])
	}
	if !strings.Contains(stmts[1], `CREATE TABLE IF NOT EXISTS "public"."users"`) {
		t.Fatalf("missing create table statement: %q", stmts[1])
	}
	if !strings.Contains(stmts[1], `"_id" TEXT`) {
		t.Fatalf("missing _id column: %q", stmts[1])
	}
	if !strings.Contains(stmts[1], `"name" TEXT`) {
		t.Fatalf("missing name column: %q", stmts[1])
	}
	if !strings.Contains(stmts[1], `PRIMARY KEY ("_id")`) {
		t.Fatalf("missing primary key clause: %q", stmts[1])
	}
}

func TestCoercePostgresValue_TextColumnsStringifyScalars(t *testing.T) {
	if got := coercePostgresValue(999.99, "TEXT"); got != "999.99" {
		t.Fatalf("float coercion = %#v, want %q", got, "999.99")
	}
	if got := coercePostgresValue(30, "TEXT"); got != "30" {
		t.Fatalf("int coercion = %#v, want %q", got, "30")
	}
	if got := coercePostgresValue(true, "TEXT"); got != "true" {
		t.Fatalf("bool coercion = %#v, want %q", got, "true")
	}
}

func TestCoercePostgresValue_TimestampParsesRFC3339(t *testing.T) {
	got := coercePostgresValue("2026-04-09T21:00:00Z", "TIMESTAMP")
	if _, ok := got.(time.Time); !ok {
		t.Fatalf("timestamp coercion type = %T, want time.Time", got)
	}
}

func TestCoercePostgresValue_StringNumericsForMariaDBTypes(t *testing.T) {
	if got := coercePostgresValue("118", "int"); got != int64(118) {
		t.Fatalf("int coercion = %#v, want int64(118)", got)
	}
	if got := coercePostgresValue("670.2137952762896", "float"); got != 670.2137952762896 {
		t.Fatalf("float coercion = %#v, want parsed float64", got)
	}
	if got := coercePostgresValue("1", "bool"); got != true {
		t.Fatalf("bool coercion = %#v, want true", got)
	}
}

func TestResolvePrimaryKeyMap_RewritesSingleColumnID(t *testing.T) {
	pk := map[string]any{"id": "507f1f77bcf86cd799439011"}
	got := resolvePrimaryKeyMap(context.Background(), nil, "public", "users", pk)
	if got["id"] != "507f1f77bcf86cd799439011" {
		t.Fatalf("nil pool should keep pk unchanged: %#v", got)
	}
}

func TestBuildCreateTableStatements_DefaultsEmptySchemaToPublic(t *testing.T) {
	row := postgresRow{
		Table:      "orders",
		PrimaryKey: map[string]any{"id": 1},
		Data:       map[string]any{"id": 1, "status": "open"},
		ColumnTypes: map[string]string{
			"id":     "BIGINT",
			"status": "TEXT",
		},
	}

	stmts := buildCreateTableStatements("", "orders", row, []string{"id", "status"})
	if stmts[0] != `CREATE SCHEMA IF NOT EXISTS "public"` {
		t.Fatalf("schema statement = %q, want public schema", stmts[0])
	}
	if !strings.Contains(stmts[1], `CREATE TABLE IF NOT EXISTS "public"."orders"`) {
		t.Fatalf("missing public orders create statement: %q", stmts[1])
	}
}

func TestBuildCreateTableStatements_NormalizesMariaDBTypes(t *testing.T) {
	row := postgresRow{
		Table:      "orders",
		Schema:     "public",
		PrimaryKey: map[string]any{"id": 1},
		Data: map[string]any{
			"id":           1,
			"status":       "open",
			"total_amount": 12.5,
		},
		ColumnTypes: map[string]string{
			"id":           "int",
			"status":       "longtext",
			"total_amount": "float",
		},
	}

	stmts := buildCreateTableStatements("public", "orders", row, []string{"id", "status", "total_amount"})
	ddl := stmts[1]
	if !strings.Contains(ddl, `"id" INTEGER`) {
		t.Fatalf("expected INTEGER mapping in %q", ddl)
	}
	if !strings.Contains(ddl, `"status" TEXT`) {
		t.Fatalf("expected TEXT mapping in %q", ddl)
	}
	if !strings.Contains(ddl, `"total_amount" REAL`) {
		t.Fatalf("expected REAL mapping in %q", ddl)
	}
}

func TestParseTableKey_DefaultsEmptySchemaToPublic(t *testing.T) {
	schema, table := parseTableKey(".orders")
	if schema != "public" || table != "orders" {
		t.Fatalf("parseTableKey('.orders') = (%q, %q), want ('public', 'orders')", schema, table)
	}
}
