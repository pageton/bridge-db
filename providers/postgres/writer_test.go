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

func TestResolvePrimaryKeyMap_RewritesSingleColumnID(t *testing.T) {
	pk := map[string]any{"id": "507f1f77bcf86cd799439011"}
	got := resolvePrimaryKeyMap(context.Background(), nil, "public", "users", pk)
	if got["id"] != "507f1f77bcf86cd799439011" {
		t.Fatalf("nil pool should keep pk unchanged: %#v", got)
	}
}
