//go:build redis

package redis

import (
	"encoding/json"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// encodeRedisData / decodeRedisData
// ---------------------------------------------------------------------------

func TestEncodeDecodeRedisData_RoundTripString(t *testing.T) {
	original := &redisKeyData{
		Type:       "string",
		Value:      "hello world",
		TTLSeconds: 3600,
	}

	encoded, err := encodeRedisData(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeRedisData(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.Value != original.Value {
		t.Errorf("value = %v, want %v", decoded.Value, original.Value)
	}
	if decoded.TTLSeconds != original.TTLSeconds {
		t.Errorf("ttl = %d, want %d", decoded.TTLSeconds, original.TTLSeconds)
	}
}

func TestEncodeDecodeRedisData_RoundTripHash(t *testing.T) {
	original := &redisKeyData{
		Type: "hash",
		Value: map[string]string{
			"field1": "val1",
			"field2": "val2",
		},
		TTLSeconds: 0,
	}

	encoded, err := encodeRedisData(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeRedisData(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != "hash" {
		t.Errorf("type = %q, want %q", decoded.Type, "hash")
	}

	got, ok := decoded.Value.(map[string]interface{})
	if !ok {
		t.Fatalf("value type = %T, want map[string]interface{}", decoded.Value)
	}
	if got["field1"] != "val1" {
		t.Errorf("field1 = %v, want %q", got["field1"], "val1")
	}
	if got["field2"] != "val2" {
		t.Errorf("field2 = %v, want %q", got["field2"], "val2")
	}
	if decoded.TTLSeconds != 0 {
		t.Errorf("ttl = %d, want 0", decoded.TTLSeconds)
	}
}

func TestEncodeDecodeRedisData_RoundTripList(t *testing.T) {
	original := &redisKeyData{
		Type:       "list",
		Value:      []string{"a", "b", "c"},
		TTLSeconds: 120,
	}

	encoded, err := encodeRedisData(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeRedisData(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != "list" {
		t.Errorf("type = %q, want %q", decoded.Type, "list")
	}

	got, ok := decoded.Value.([]interface{})
	if !ok {
		t.Fatalf("value type = %T, want []interface{}", decoded.Value)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	want := []string{"a", "b", "c"}
	for i, v := range got {
		if v != want[i] {
			t.Errorf("[%d] = %v, want %q", i, v, want[i])
		}
	}
	if decoded.TTLSeconds != 120 {
		t.Errorf("ttl = %d, want 120", decoded.TTLSeconds)
	}
}

func TestEncodeDecodeRedisData_RoundTripSet(t *testing.T) {
	original := &redisKeyData{
		Type:       "set",
		Value:      []string{"charlie", "alpha", "bravo"},
		TTLSeconds: 0,
	}

	encoded, err := encodeRedisData(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeRedisData(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != "set" {
		t.Errorf("type = %q, want %q", decoded.Type, "set")
	}

	got, ok := decoded.Value.([]interface{})
	if !ok {
		t.Fatalf("value type = %T, want []interface{}", decoded.Value)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
}

func TestEncodeDecodeRedisData_RoundTripZset(t *testing.T) {
	original := &redisKeyData{
		Type: "zset",
		Value: []sortedSetMember{
			{Member: "one", Score: 1.0},
			{Member: "two", Score: 2.5},
		},
		TTLSeconds: 300,
	}

	encoded, err := encodeRedisData(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeRedisData(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != "zset" {
		t.Errorf("type = %q, want %q", decoded.Type, "zset")
	}

	got, ok := decoded.Value.([]interface{})
	if !ok {
		t.Fatalf("value type = %T, want []interface{}", decoded.Value)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if decoded.TTLSeconds != 300 {
		t.Errorf("ttl = %d, want 300", decoded.TTLSeconds)
	}
}

func TestEncodeDecodeRedisData_RoundTripStream(t *testing.T) {
	original := &redisKeyData{
		Type: "stream",
		Value: []streamEntry{
			{ID: "1234567890-0", Fields: map[string]string{"key1": "val1"}},
			{ID: "1234567890-1", Fields: map[string]string{"key2": "val2"}},
		},
		TTLSeconds: 0,
	}

	encoded, err := encodeRedisData(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeRedisData(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != "stream" {
		t.Errorf("type = %q, want %q", decoded.Type, "stream")
	}

	got, ok := decoded.Value.([]interface{})
	if !ok {
		t.Fatalf("value type = %T, want []interface{}", decoded.Value)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
}

func TestDecodeRedisData_InvalidJSON(t *testing.T) {
	_, err := decodeRedisData([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "decode redis data") {
		t.Errorf("error = %q, want wrapped error containing 'decode redis data'", err.Error())
	}
}

func TestDecodeRedisData_EmptyBytes(t *testing.T) {
	_, err := decodeRedisData([]byte{})
	if err == nil {
		t.Fatal("expected error for empty input, got nil")
	}
}

func TestEncodeRedisData_NilPointer(t *testing.T) {
	got, err := encodeRedisData(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "null" {
		t.Errorf("got %s, want null", got)
	}
}

// ---------------------------------------------------------------------------
// marshalChecksumRecord
// ---------------------------------------------------------------------------

func TestMarshalChecksumRecord_StringKey(t *testing.T) {
	record := map[string]any{
		"type":  "string",
		"value": "hello",
		"ttl":   int64(3600),
	}

	got, err := marshalChecksumRecord(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}

	if parsed["type"] != "string" {
		t.Errorf("type = %v, want %q", parsed["type"], "string")
	}
	if parsed["value"] != "hello" {
		t.Errorf("value = %v, want %q", parsed["value"], "hello")
	}
}

func TestMarshalChecksumRecord_HashKey(t *testing.T) {
	record := map[string]any{
		"type":  "hash",
		"value": map[string]string{"a": "1", "b": "2"},
		"ttl":   int64(0),
	}

	got, err := marshalChecksumRecord(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}

	val, ok := parsed["value"].(map[string]any)
	if !ok {
		t.Fatalf("value type = %T, want map[string]any", parsed["value"])
	}
	if val["a"] != "1" {
		t.Errorf("a = %v, want %q", val["a"], "1")
	}
	if val["b"] != "2" {
		t.Errorf("b = %v, want %q", val["b"], "2")
	}
}

func TestMarshalChecksumRecord_SetKey_SortedDeterministically(t *testing.T) {
	for _, order := range [][]string{
		{"charlie", "alpha", "bravo"},
		{"bravo", "charlie", "alpha"},
		{"alpha", "bravo", "charlie"},
	} {
		record := map[string]any{
			"type":  "set",
			"value": order,
			"ttl":   int64(60),
		}

		got, err := marshalChecksumRecord(record)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		var parsed map[string]any
		if err := json.Unmarshal(got, &parsed); err != nil {
			t.Fatalf("result is not valid JSON: %v", err)
		}

		val, ok := parsed["value"].([]any)
		if !ok {
			t.Fatalf("value type = %T, want []any", parsed["value"])
		}
		if len(val) != 3 {
			t.Fatalf("len = %d, want 3", len(val))
		}
		if val[0] != "alpha" || val[1] != "bravo" || val[2] != "charlie" {
			t.Errorf("value = %v, want [alpha bravo charlie]", val)
		}
	}
}

func TestMarshalChecksumRecord_DeterministicOutput(t *testing.T) {
	record := map[string]any{
		"type":  "list",
		"value": []string{"x", "y", "z"},
		"ttl":   int64(42),
	}

	out1, err := marshalChecksumRecord(record)
	if err != nil {
		t.Fatalf("marshal 1: %v", err)
	}
	out2, err := marshalChecksumRecord(record)
	if err != nil {
		t.Fatalf("marshal 2: %v", err)
	}

	if string(out1) != string(out2) {
		t.Errorf("outputs differ:\n  %s\n  %s", out1, out2)
	}
}

// ---------------------------------------------------------------------------
// normalizeChecksumValue
// ---------------------------------------------------------------------------

func TestNormalizeChecksumValue_MapStringString(t *testing.T) {
	input := map[string]string{"k1": "v1", "k2": "v2"}
	got := normalizeChecksumValue("hash", input)

	typed, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("type = %T, want map[string]any", got)
	}
	if typed["k1"] != "v1" {
		t.Errorf("k1 = %v, want %q", typed["k1"], "v1")
	}
	if typed["k2"] != "v2" {
		t.Errorf("k2 = %v, want %q", typed["k2"], "v2")
	}
}

func TestNormalizeChecksumValue_SetMembersSorted(t *testing.T) {
	input := []string{"delta", "alpha", "charlie", "bravo"}
	got := normalizeChecksumValue("set", input)

	typed, ok := got.([]string)
	if !ok {
		t.Fatalf("type = %T, want []string", got)
	}
	want := []string{"alpha", "bravo", "charlie", "delta"}
	for i, v := range typed {
		if v != want[i] {
			t.Errorf("[%d] = %q, want %q", i, v, want[i])
		}
	}
}

func TestNormalizeChecksumValue_ListNotSorted(t *testing.T) {
	input := []string{"z", "a", "m"}
	got := normalizeChecksumValue("list", input)

	typed, ok := got.([]string)
	if !ok {
		t.Fatalf("type = %T, want []string", got)
	}
	if typed[0] != "z" || typed[1] != "a" || typed[2] != "m" {
		t.Errorf("list should not be sorted, got %v", typed)
	}
}

func TestNormalizeChecksumValue_SimpleStringPassthrough(t *testing.T) {
	got := normalizeChecksumValue("string", "hello world")
	if got != "hello world" {
		t.Errorf("got %v, want %q", got, "hello world")
	}
}

func TestNormalizeChecksumValue_IntegerPassthrough(t *testing.T) {
	got := normalizeChecksumValue("string", int64(42))
	if got != int64(42) {
		t.Errorf("got %v, want 42", got)
	}
}

func TestNormalizeChecksumValue_MapStringAny(t *testing.T) {
	input := map[string]any{"score": 1.5, "member": "test"}
	got := normalizeChecksumValue("zset", input)

	typed, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("type = %T, want map[string]any", got)
	}
	if typed["score"] != 1.5 {
		t.Errorf("score = %v, want 1.5", typed["score"])
	}
	if typed["member"] != "test" {
		t.Errorf("member = %v, want %q", typed["member"], "test")
	}
}

func TestNormalizeChecksumValue_NilValuePassthrough(t *testing.T) {
	got := normalizeChecksumValue("string", nil)
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}
