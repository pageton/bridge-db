package transform

import (
	"testing"

	"github.com/pageton/bridge-db/internal/config"
)

// ---------------------------------------------------------------------------
// NullHandler tests
// ---------------------------------------------------------------------------

func TestNullHandler_PassThrough(t *testing.T) {
	h := &NullHandler{Policy: NullPassThrough}
	data := map[string]any{"a": 1, "b": nil}
	result, err := h.Apply(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["b"] != nil {
		t.Error("passthrough should keep nil values")
	}
}

func TestNullHandler_Drop(t *testing.T) {
	h := &NullHandler{Policy: NullDrop}
	data := map[string]any{"a": 1, "b": nil, "c": "hello"}
	result, err := h.Apply(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["b"]; ok {
		t.Error("nil value should be dropped")
	}
	if result["a"] != 1 {
		t.Error("non-nil values should remain")
	}
}

func TestNullHandler_Replace(t *testing.T) {
	h := &NullHandler{Policy: NullReplace}
	data := map[string]any{"a": 1, "b": nil}
	result, err := h.Apply(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["b"] != "" {
		t.Errorf("nil should be replaced with empty string, got %v", result["b"])
	}
}

func TestNullHandler_Error(t *testing.T) {
	h := &NullHandler{Policy: NullError}
	data := map[string]any{"a": 1, "b": nil}
	_, err := h.Apply(data)
	if err == nil {
		t.Fatal("expected error for nil value")
	}
}

func TestNullHandler_ErrorNoNil(t *testing.T) {
	h := &NullHandler{Policy: NullError}
	data := map[string]any{"a": 1, "b": 2}
	_, err := h.Apply(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNullHandler_NilReceiver(t *testing.T) {
	var h *NullHandler
	data := map[string]any{"a": 1}
	result, err := h.Apply(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["a"] != 1 {
		t.Error("nil handler should pass through")
	}
}

func TestNullPolicyFromString(t *testing.T) {
	tests := []struct {
		input string
		want  NullPolicy
	}{
		{"drop", NullDrop},
		{"replace", NullReplace},
		{"error", NullError},
		{"unknown", NullPassThrough},
		{"", NullPassThrough},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := NullPolicyFromString(tt.input)
			if got != tt.want {
				t.Errorf("NullPolicyFromString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// FieldMappingApplier tests
// ---------------------------------------------------------------------------

func TestFieldMapping_Rename(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "old_name", Destination: "new_name"}},
	})
	result, err := a.Apply("users", map[string]any{"old_name": "alice", "age": 30})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["old_name"]; ok {
		t.Error("old_name should be removed")
	}
	if result["new_name"] != "alice" {
		t.Error("new_name should have the value")
	}
	if result["age"] != 30 {
		t.Error("unmapped fields should pass through")
	}
}

func TestFieldMapping_Drop(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "secret", Action: "drop"}},
	})
	result, err := a.Apply("users", map[string]any{"secret": "pw", "name": "alice"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["secret"]; ok {
		t.Error("secret should be dropped")
	}
	if result["name"] != "alice" {
		t.Error("name should remain")
	}
}

func TestFieldMapping_ConvertString(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "age", Action: "convert", Convert: "string"}},
	})
	result, err := a.Apply("users", map[string]any{"age": 30})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["age"] != "30" {
		t.Errorf("expected age='30', got %v", result["age"])
	}
}

func TestFieldMapping_ConvertInt(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "age", Action: "convert", Convert: "int"}},
	})
	result, err := a.Apply("users", map[string]any{"age": "42"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["age"] != int64(42) {
		t.Errorf("expected age=int64(42), got %v (%T)", result["age"], result["age"])
	}
}

func TestFieldMapping_ConvertFloat(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "score", Action: "convert", Convert: "float"}},
	})
	result, err := a.Apply("users", map[string]any{"score": "3.14"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["score"] != 3.14 {
		t.Errorf("expected score=3.14, got %v", result["score"])
	}
}

func TestFieldMapping_ConvertBool(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "active", Action: "convert", Convert: "bool"}},
	})
	result, err := a.Apply("users", map[string]any{"active": "true"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["active"] != true {
		t.Errorf("expected active=true, got %v", result["active"])
	}
}

func TestFieldMapping_ConvertTimestamp(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"events": {{Source: "ts", Action: "convert", Convert: "timestamp:mysql:postgres"}},
	})
	result, err := a.Apply("events", map[string]any{"ts": "2024-01-15 10:30:00"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["ts"] == nil {
		t.Error("timestamp should be converted")
	}
}

func TestFieldMapping_Wildcard(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"*": {{Source: "internal_id", Action: "drop"}},
	})
	result, err := a.Apply("orders", map[string]any{"internal_id": 123, "name": "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["internal_id"]; ok {
		t.Error("wildcard should apply to all tables")
	}
}

func TestFieldMapping_NoMatchingTable(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "name", Action: "drop"}},
	})
	result, err := a.Apply("orders", map[string]any{"name": "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["name"] != "test" {
		t.Error("unmapped table should pass through unchanged")
	}
}

func TestFieldMapping_NilApplier(t *testing.T) {
	var a *FieldMappingApplier
	data := map[string]any{"a": 1}
	result, err := a.Apply("table", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["a"] != 1 {
		t.Error("nil applier should pass through")
	}
}

func TestFieldMapping_SourceMissing(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "nonexistent", Destination: "renamed"}},
	})
	result, err := a.Apply("users", map[string]any{"other": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["renamed"]; ok {
		t.Error("should not create destination for missing source")
	}
}

func TestFieldMapping_ConvertWithRename(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "val", Destination: "score", Action: "convert", Convert: "int"}},
	})
	result, err := a.Apply("users", map[string]any{"val": "100"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// convert writes to destination but does not remove source
	if result["val"] != "100" {
		t.Error("source field should remain after convert")
	}
	if result["score"] != int64(100) {
		t.Errorf("expected score=int64(100), got %v", result["score"])
	}
}

// ---------------------------------------------------------------------------
// Implicit rename tests (no Action, Source != Destination)
// ---------------------------------------------------------------------------

func TestFieldMapping_ImplicitRename(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "foo", Destination: "bar"}},
	})
	result, err := a.Apply("users", map[string]any{"foo": "baz"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["foo"]; ok {
		t.Error("source should be removed on implicit rename")
	}
	if result["bar"] != "baz" {
		t.Error("destination should have the value")
	}
}

func TestFieldMapping_SameSourceDest_NoAction(t *testing.T) {
	a := NewFieldMappingApplier(map[string][]config.FieldMapping{
		"users": {{Source: "foo", Destination: "foo"}},
	})
	result, err := a.Apply("users", map[string]any{"foo": "bar"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["foo"] != "bar" {
		t.Error("same source/dest with no action should be a no-op")
	}
}

// ---------------------------------------------------------------------------
// coerceType tests
// ---------------------------------------------------------------------------

func TestCoerceType(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		target string
		want   any
	}{
		{"int to string", 42, "string", "42"},
		{"float to string", 3.14, "string", "3.14"},
		{"string to int", "42", "int", int64(42)},
		{"float to int", 42.5, "int", int64(42)},
		{"int to float", 42, "float", float64(42)},
		{"string to float", "3.14", "float", 3.14},
		{"string to bool true", "true", "bool", true},
		{"string to bool false", "false", "bool", false},
		{"int to bool", 1, "bool", true},
		{"zero to bool", 0, "bool", false},
		{"nil passthrough", nil, "string", nil},
		{"unknown target", 42, "custom", 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := coerceType(tt.input, tt.target)
			if got != tt.want {
				t.Errorf("coerceType(%v, %q) = %v (%T), want %v (%T)",
					tt.input, tt.target, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestCoerceInt_FromFloatString(t *testing.T) {
	result := coerceInt("42.9")
	if result != int64(42) {
		t.Errorf("expected int64(42), got %v", result)
	}
}

func TestCoerceBool_FromFloat(t *testing.T) {
	result := coerceBool(1.0)
	if result != true {
		t.Errorf("expected true, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// isNil tests
// ---------------------------------------------------------------------------

func TestIsNil(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  bool
	}{
		{"nil", nil, true},
		{"empty string", "", true},
		{"empty slice", []any{}, true},
		{"empty map", map[string]any{}, true},
		{"non-empty string", "hello", false},
		{"non-empty int", 42, false},
		{"false bool", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNil(tt.input)
			if got != tt.want {
				t.Errorf("isNil(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
