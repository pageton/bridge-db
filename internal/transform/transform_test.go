package transform

import (
	"context"
	"testing"

	"github.com/pageton/bridge-db/pkg/provider"
)

func TestNoopTransformer_Transform(t *testing.T) {
	units := []provider.MigrationUnit{
		{Key: "k1", Data: []byte(`{"foo":"bar"}`)},
		{Key: "k2", Data: []byte(`{"baz":1}`)},
	}
	result, err := NoopTransformer{}.Transform(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != len(units) {
		t.Fatalf("expected %d units, got %d", len(units), len(result))
	}
	for i, u := range result {
		if string(u.Data) != string(units[i].Data) {
			t.Errorf("unit %d: data changed from %q to %q", i, units[i].Data, u.Data)
		}
	}
}

func TestNoopTransformer_NeedsSchema(t *testing.T) {
	n := NoopTransformer{}
	if n.NeedsSchema() {
		t.Error("NoopTransformer should not need schema")
	}
}

func TestNoopTransformer_SetSchema(t *testing.T) {
	n := NoopTransformer{}
	n.SetSchema(&provider.Schema{})
}
