package transform

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// StagePipeline tests
// ---------------------------------------------------------------------------

func TestStagePipeline_Empty(t *testing.T) {
	sp := NewStagePipeline()
	result, err := sp.Transform(context.Background(), []provider.MigrationUnit{
		{Key: "k1", Data: []byte(`{"x":1}`)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 unit, got %d", len(result))
	}
}

func TestStagePipeline_SingleStage(t *testing.T) {
	called := false
	sp := NewStagePipeline(func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		called = true
		return units, nil
	})
	_, err := sp.Transform(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("stage was not called")
	}
}

func TestStagePipeline_StopsOnError(t *testing.T) {
	stage1Called := false
	stage2Called := false
	sp := NewStagePipeline(
		func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
			stage1Called = true
			return nil, fmt.Errorf("stage1 failed")
		},
		func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
			stage2Called = true
			return units, nil
		},
	)
	_, err := sp.Transform(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !stage1Called {
		t.Error("stage1 should be called")
	}
	if stage2Called {
		t.Error("stage2 should not be called after stage1 error")
	}
	if !strings.Contains(err.Error(), "stage1 failed") {
		t.Errorf("error should contain stage description, got: %v", err)
	}
}

func TestStagePipeline_RespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sp := NewStagePipeline(func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		return units, nil
	})
	_, err := sp.Transform(ctx, nil)
	if err == nil {
		t.Fatal("expected context cancelled error")
	}
}

func TestStagePipeline_Chaining(t *testing.T) {
	// Double each unit's key via two stages.
	stage1 := func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		for i := range units {
			units[i].Key = units[i].Key + "_s1"
		}
		return units, nil
	}
	stage2 := func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
		for i := range units {
			units[i].Key = units[i].Key + "_s2"
		}
		return units, nil
	}

	sp := NewStagePipeline(stage1, stage2)
	result, err := sp.Transform(context.Background(), []provider.MigrationUnit{
		{Key: "k"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Key != "k_s1_s2" {
		t.Errorf("expected key 'k_s1_s2', got %q", result[0].Key)
	}
}

func TestStagePipeline_StageCount(t *testing.T) {
	sp := NewStagePipeline(
		func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
			return units, nil
		},
		func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
			return units, nil
		},
		func(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
			return units, nil
		},
	)
	if sp.StageCount() != 3 {
		t.Errorf("expected 3 stages, got %d", sp.StageCount())
	}
}

// ---------------------------------------------------------------------------
// NullHandlingStage tests
// ---------------------------------------------------------------------------

func TestNullHandlingStage_DropsNulls(t *testing.T) {
	cfg := &TransformerConfig{
		NullHandler: &NullHandler{Policy: NullDrop},
	}
	stage := NullHandlingStage(cfg)

	units := []provider.MigrationUnit{sqlRowUnit("t", map[string]any{"a": 1, "b": nil})}
	result, err := stage(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var env map[string]any
	_ = sonic.Unmarshal(result[0].Data, &env)
	data := env["data"].(map[string]any)
	if _, ok := data["b"]; ok {
		t.Error("null key 'b' should be dropped")
	}
	if data["a"] != 1.0 {
		t.Error("non-null key 'a' should remain")
	}
}

func TestNullHandlingStage_NilConfig(t *testing.T) {
	stage := NullHandlingStage(nil)
	units := []provider.MigrationUnit{{Key: "k", Data: []byte(`{}`)}}
	result, err := stage(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Error("nil config should pass through")
	}
}

func TestNullHandlingStage_NonSQLData_Passthrough(t *testing.T) {
	cfg := &TransformerConfig{
		NullHandler: &NullHandler{Policy: NullDrop},
	}
	stage := NullHandlingStage(cfg)

	// Non-row DataType should pass through without modification.
	units := []provider.MigrationUnit{{Key: "k", DataType: provider.DataTypeHash, Data: []byte(`{"type":"hash"}`)}}
	result, err := stage(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result[0].Data) != `{"type":"hash"}` {
		t.Error("non-SQL data should pass through unchanged")
	}
}

// ---------------------------------------------------------------------------
// FieldMappingStage tests
// ---------------------------------------------------------------------------

func TestFieldMappingStage_Renames(t *testing.T) {
	cfg := &TransformerConfig{
		FieldMapping: NewFieldMappingApplier(map[string][]config.FieldMapping{
			"users": {{Source: "old", Destination: "new"}},
		}),
	}
	stage := FieldMappingStage(cfg)

	units := []provider.MigrationUnit{sqlRowUnit("users", map[string]any{"old": "val"})}
	result, err := stage(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var env map[string]any
	_ = sonic.Unmarshal(result[0].Data, &env)
	data := env["data"].(map[string]any)
	if _, ok := data["old"]; ok {
		t.Error("old field should be removed")
	}
	if data["new"] != "val" {
		t.Error("new field should have value")
	}
}

func TestFieldMappingStage_NilConfig(t *testing.T) {
	stage := FieldMappingStage(nil)
	units := []provider.MigrationUnit{{Key: "k", Data: []byte(`{}`)}}
	result, err := stage(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Error("nil config should pass through")
	}
}

// ---------------------------------------------------------------------------
// Full pipeline integration test
// ---------------------------------------------------------------------------

func TestStagePipeline_NullThenMapping(t *testing.T) {
	cfg := &TransformerConfig{
		NullHandler: &NullHandler{Policy: NullDrop},
		FieldMapping: NewFieldMappingApplier(map[string][]config.FieldMapping{
			"t": {{Source: "secret", Action: "drop"}},
		}),
	}

	sp := NewStagePipeline(
		NullHandlingStage(cfg),
		FieldMappingStage(cfg),
	)

	units := []provider.MigrationUnit{
		sqlRowUnit("t", map[string]any{"a": 1, "b": nil, "secret": "x", "c": "keep"}),
	}
	result, err := sp.Transform(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var env map[string]any
	_ = sonic.Unmarshal(result[0].Data, &env)
	data := env["data"].(map[string]any)

	if _, ok := data["b"]; ok {
		t.Error("null 'b' should be dropped by null stage")
	}
	if _, ok := data["secret"]; ok {
		t.Error("secret should be dropped by mapping stage")
	}
	if data["a"] != 1.0 || data["c"] != "keep" {
		t.Error("non-null, non-mapped fields should remain")
	}
}

// ---------------------------------------------------------------------------
// IsNoopTransformer tests
// ---------------------------------------------------------------------------

func TestIsNoopTransformer_True(t *testing.T) {
	if !IsNoopTransformer(NoopTransformer{}) {
		t.Error("NoopTransformer should be detected as noop")
	}
}

func TestIsNoopTransformer_False(t *testing.T) {
	tr := &MySQLToPostgresTransformer{}
	if IsNoopTransformer(tr) {
		t.Error("MySQLToPostgresTransformer should not be detected as noop")
	}
}

// ---------------------------------------------------------------------------
// Cross-DB transformer null+mapping coverage tests
// ---------------------------------------------------------------------------

func TestPassthroughTransformer_AppliesNullHandling(t *testing.T) {
	// Verify that a passthrough transformer (e.g. mariadb→postgres) applies
	// null handling and field mappings via the staged pipeline.
	tr := &mariadbPassthroughTransformer{
		src: "mariadb",
		dst: "postgres",
		cfg: TransformerConfig{
			NullHandler: &NullHandler{Policy: NullDrop},
		},
	}

	units := []provider.MigrationUnit{
		sqlRowUnit("t", map[string]any{"a": 1, "b": nil}),
	}
	result, err := tr.Transform(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var env map[string]any
	_ = sonic.Unmarshal(result[0].Data, &env)
	data := env["data"].(map[string]any)
	if _, ok := data["b"]; ok {
		t.Error("null 'b' should be dropped by mariadb→postgres transformer")
	}
}

func TestSQLiteTransformer_AppliesFieldMapping(t *testing.T) {
	tr := &SQLiteToMySQLTransformer{
		cfg: TransformerConfig{
			FieldMapping: NewFieldMappingApplier(map[string][]config.FieldMapping{
				"t": {{Source: "internal", Action: "drop"}},
			}),
		},
	}

	units := []provider.MigrationUnit{
		sqlRowUnit("t", map[string]any{"id": 1, "internal": "secret"}),
	}
	result, err := tr.Transform(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var env map[string]any
	_ = sonic.Unmarshal(result[0].Data, &env)
	data := env["data"].(map[string]any)
	if _, ok := data["internal"]; ok {
		t.Error("internal field should be dropped by field mapping stage")
	}
	if data["id"] != 1.0 {
		t.Error("non-mapped field should remain")
	}
}

func TestMSSQLTransformer_AppliesNullAndMapping(t *testing.T) {
	tr := &mssqlPassthroughTransformer{
		src: "mssql",
		dst: "postgres",
		cfg: TransformerConfig{
			NullHandler: &NullHandler{Policy: NullDrop},
			FieldMapping: NewFieldMappingApplier(map[string][]config.FieldMapping{
				"t": {{Source: "secret", Action: "drop"}},
			}),
		},
	}

	units := []provider.MigrationUnit{
		sqlRowUnit("t", map[string]any{"id": 1, "secret": "x", "b": nil}),
	}
	result, err := tr.Transform(context.Background(), units)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var env map[string]any
	_ = sonic.Unmarshal(result[0].Data, &env)
	data := env["data"].(map[string]any)

	if _, ok := data["b"]; ok {
		t.Error("null 'b' should be dropped")
	}
	if _, ok := data["secret"]; ok {
		t.Error("secret should be dropped")
	}
	if data["id"] != 1.0 {
		t.Error("id should remain")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func sqlRowUnit(table string, data map[string]any) provider.MigrationUnit {
	env := map[string]any{
		"table":       table,
		"primary_key": map[string]any{"id": 1},
		"data":        data,
	}
	b, _ := sonic.Marshal(env)
	return provider.MigrationUnit{
		Key:      table + ":1",
		Table:    table,
		DataType: provider.DataTypeRow,
		Data:     b,
	}
}
