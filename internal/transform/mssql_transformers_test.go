package transform

import "testing"

func TestPostgresToMSSQLType_PreservesDecimalPrecision(t *testing.T) {
	got, ok := postgresToMSSQLType("NUMERIC(10,2)")
	if !ok {
		t.Fatal("expected NUMERIC(10,2) to map")
	}
	if got != "NUMERIC(10,2)" {
		t.Fatalf("postgresToMSSQLType(NUMERIC(10,2)) = %q, want %q", got, "NUMERIC(10,2)")
	}
}
