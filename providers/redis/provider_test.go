//go:build redis

package redis

import (
	"testing"
)

func TestMarshalChecksumRecord_NormalizesHashOrder(t *testing.T) {
	a, err := marshalChecksumRecord(map[string]any{
		"type":  "hash",
		"value": map[string]string{"name": "Grace Lee", "email": "grace@example.com", "age": "33"},
		"ttl":   int64(0),
	})
	if err != nil {
		t.Fatalf("marshal a: %v", err)
	}

	b, err := marshalChecksumRecord(map[string]any{
		"ttl":   int64(0),
		"value": map[string]string{"age": "33", "email": "grace@example.com", "name": "Grace Lee"},
		"type":  "hash",
	})
	if err != nil {
		t.Fatalf("marshal b: %v", err)
	}

	if string(a) != string(b) {
		t.Fatalf("expected equal checksum payloads, got %s != %s", a, b)
	}
}

func TestMarshalChecksumRecord_NormalizesSetOrder(t *testing.T) {
	a, err := marshalChecksumRecord(map[string]any{
		"type":  "set",
		"value": []string{"b", "a", "c"},
		"ttl":   int64(0),
	})
	if err != nil {
		t.Fatalf("marshal a: %v", err)
	}

	b, err := marshalChecksumRecord(map[string]any{
		"type":  "set",
		"value": []string{"c", "b", "a"},
		"ttl":   int64(0),
	})
	if err != nil {
		t.Fatalf("marshal b: %v", err)
	}

	if string(a) != string(b) {
		t.Fatalf("expected equal checksum payloads, got %s != %s", a, b)
	}
}
