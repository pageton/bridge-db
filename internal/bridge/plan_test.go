package bridge

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// Plan builder tests
// ---------------------------------------------------------------------------

func TestPlan_SameEngine_NoTypeMappings(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "postgres"},
			Destination: config.ConnectionConfig{Provider: "postgres"},
		},
		opts: PipelineOptions{
			BatchSize:     100,
			FKHandling:    "skip",
			MigrateSchema: true,
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{Schema: true, Transactions: true},
		dstCaps:     provider.Capabilities{Schema: true, Transactions: true},
		src:         &planEnumProvider{tables: map[string]int64{"users": 500, "orders": 1200}},
	}

	plan := p.buildPlan(context.Background())

	if plan.CrossDB {
		t.Error("same-engine should not be cross-DB")
	}
	if plan.TransformerType != "noop" {
		t.Errorf("expected noop transformer, got %s", plan.TransformerType)
	}
	if plan.EstimatedRows != 1700 {
		t.Errorf("expected 1700 rows, got %d", plan.EstimatedRows)
	}
	if plan.EstimatedBatches != 17 {
		t.Errorf("expected 17 batches, got %d", plan.EstimatedBatches)
	}
	if len(plan.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(plan.Tables))
	}
	if len(plan.TypeMappings) != 0 {
		t.Errorf("same-engine should have no type mappings, got %d", len(plan.TypeMappings))
	}
	if !plan.SchemaMigration {
		t.Error("schema migration should be planned")
	}
}

func TestPlan_NoSchema_NoSQLSource(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "redis"},
			Destination: config.ConnectionConfig{Provider: "redis"},
		},
		opts: PipelineOptions{
			BatchSize:  100,
			FKHandling: "skip",
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{Schema: false, Transactions: false},
		dstCaps:     provider.Capabilities{Schema: false, Transactions: false},
		src:         &planEnumProvider{tables: map[string]int64{"": 10000}},
	}

	plan := p.buildPlan(context.Background())

	if plan.SchemaMigration {
		t.Error("NoSQL source should not plan schema migration")
	}
	if plan.EstimatedRows != 10000 {
		t.Errorf("expected 10000 rows, got %d", plan.EstimatedRows)
	}
	if len(plan.TypeMappings) != 0 {
		t.Error("NoSQL should have no type mappings")
	}
}

func TestPlan_CrossDB_WithUnsupportedFields(t *testing.T) {
	transform.RegisterTransformer("pgx", "myx", func() transform.Transformer {
		return &stubTypeMapperTransformer{}
	})

	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "pgx"},
			Destination: config.ConnectionConfig{Provider: "myx"},
		},
		opts: PipelineOptions{
			BatchSize:     50,
			FKHandling:    "skip",
			MigrateSchema: true,
		},
		transformer: &stubTypeMapperTransformer{},
		srcCaps:     provider.Capabilities{Schema: true, Transactions: true},
		dstCaps:     provider.Capabilities{Schema: true, Transactions: true},
		src:         &planSchemaProvider{tables: []planTableDef{{"users", []planColDef{{"id", "BIGINT"}, {"name", "VARCHAR(255)"}, {"ts", "TIMESTAMPTZ"}, {"bio", "UNKNOWN_TYPE"}}}}},
	}

	plan := p.buildPlan(context.Background())

	if !plan.CrossDB {
		t.Error("different providers should be cross-DB")
	}
	if len(plan.TypeMappings) == 0 {
		t.Fatal("expected type mappings for cross-DB")
	}

	// Check that UNKNOWN_TYPE is flagged as unsupported.
	foundUnsupported := false
	for _, u := range plan.UnsupportedFields {
		if u.Field == "bio" && strings.Contains(u.Reason, "no type mapping") {
			foundUnsupported = true
		}
	}
	if !foundUnsupported {
		t.Errorf("expected bio/UNKNOWN_TYPE to be flagged unsupported, got: %v", plan.UnsupportedFields)
	}

	// TIMESTAMPTZ → TIMESTAMP should be flagged lossy.
	foundLossy := false
	for _, u := range plan.UnsupportedFields {
		if u.Field == "ts" && strings.Contains(u.Reason, "lossy") {
			foundLossy = true
		}
	}
	if !foundLossy {
		t.Errorf("expected ts/TIMESTAMPTZ→TIMESTAMP to be flagged lossy, got: %v", plan.UnsupportedFields)
	}
}

func TestPlan_FieldMappings(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "postgres"},
			Destination: config.ConnectionConfig{Provider: "postgres"},
			Transform: config.TransformConfig{
				Mappings: map[string][]config.FieldMapping{
					"users": {
						{Source: "internal_id", Action: "drop"},
						{Source: "name", Destination: "full_name", Action: "rename"},
						{Source: "age", Action: "convert", Convert: "int"},
					},
				},
			},
		},
		opts: PipelineOptions{
			BatchSize:  100,
			FKHandling: "skip",
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{Schema: true},
		dstCaps:     provider.Capabilities{Schema: true},
		src:         &planEnumProvider{tables: map[string]int64{"users": 100}},
	}

	plan := p.buildPlan(context.Background())

	if len(plan.FieldMappings) != 1 {
		t.Fatalf("expected 1 field mapping entry, got %d", len(plan.FieldMappings))
	}
	fm := plan.FieldMappings[0]
	if fm.Table != "users" {
		t.Errorf("expected users table, got %s", fm.Table)
	}
	if fm.NumRules != 3 {
		t.Errorf("expected 3 rules, got %d", fm.NumRules)
	}

	// Dropped field should appear in unsupported.
	found := false
	for _, u := range plan.UnsupportedFields {
		if u.Field == "internal_id" && u.Table == "users" && strings.Contains(u.Reason, "dropped") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected internal_id to be flagged as dropped, got: %v", plan.UnsupportedFields)
	}
}

func TestPlan_VerificationWarnings(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "a"},
			Destination: config.ConnectionConfig{Provider: "b"},
		},
		opts: PipelineOptions{
			BatchSize:  100,
			Verify:     true,
			FKHandling: "skip",
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{Verification: provider.VerifyNone},
		dstCaps:     provider.Capabilities{Verification: provider.VerifyNone},
		src:         &planEnumProvider{tables: map[string]int64{"t": 10}},
	}

	plan := p.buildPlan(context.Background())

	found := false
	for _, w := range plan.Warnings {
		if strings.Contains(w, "verification requested") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected verification warning, got: %v", plan.Warnings)
	}
}

func TestPlan_EmptySource_NoTables(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "postgres"},
			Destination: config.ConnectionConfig{Provider: "postgres"},
		},
		opts: PipelineOptions{
			BatchSize:  100,
			FKHandling: "skip",
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{Schema: true},
		dstCaps:     provider.Capabilities{Schema: true},
		src:         &planEnumProvider{tables: map[string]int64{}},
	}

	plan := p.buildPlan(context.Background())

	if plan.EstimatedRows != 0 {
		t.Errorf("expected 0 rows for empty source, got %d", plan.EstimatedRows)
	}
	if plan.EstimatedBatches != 0 {
		t.Errorf("expected 0 batches for empty source, got %d", plan.EstimatedBatches)
	}
}

func TestPlan_SchemaFallback_WhenNoEnumerator(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "test"},
		},
		opts: PipelineOptions{
			BatchSize:  100,
			FKHandling: "skip",
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{Schema: true},
		dstCaps:     provider.Capabilities{Schema: true},
		src:         &planSchemaNoEnumProvider{tables: []string{"orders", "products"}},
	}

	plan := p.buildPlan(context.Background())

	if len(plan.Tables) != 2 {
		t.Fatalf("expected 2 tables from schema fallback, got %d", len(plan.Tables))
	}
	for _, tp := range plan.Tables {
		if tp.EstimatedRows != -1 {
			t.Errorf("expected -1 for unknown row count, got %d", tp.EstimatedRows)
		}
	}
	if plan.EstimatedRows != 0 {
		t.Errorf("expected 0 total rows when counts unavailable, got %d", plan.EstimatedRows)
	}
}

func TestPlan_EnumError_ProducesWarning(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "test"},
		},
		opts: PipelineOptions{
			BatchSize:  100,
			FKHandling: "skip",
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{},
		dstCaps:     provider.Capabilities{},
		src:         &planEnumErrorProvider{err: fmt.Errorf("connection lost")},
	}

	plan := p.buildPlan(context.Background())

	found := false
	for _, w := range plan.Warnings {
		if strings.Contains(w, "could not enumerate") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected enumeration error warning, got: %v", plan.Warnings)
	}
}

func TestPlan_FKHandlingWarning(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "redis"},
		},
		opts: PipelineOptions{
			BatchSize:     100,
			FKHandling:    "defer_constraints",
			MigrateSchema: false,
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{},
		dstCaps:     provider.Capabilities{Transactions: false},
		src:         &planEnumProvider{tables: map[string]int64{"t": 5}},
	}

	plan := p.buildPlan(context.Background())

	found := false
	for _, w := range plan.Warnings {
		if strings.Contains(w, "defer_constraints") && strings.Contains(w, "transactions") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected FK handling warning, got: %v", plan.Warnings)
	}
}

// ---------------------------------------------------------------------------
// Stub providers for plan tests
// ---------------------------------------------------------------------------

// planEnumProvider supports TableEnumerator.
type planEnumProvider struct {
	capStubProvider
	tables map[string]int64
}

func (p *planEnumProvider) EnumerateTables(_ context.Context) (map[string]int64, error) {
	return p.tables, nil
}

// planSchemaProvider supports SchemaMigrator but not TableEnumerator.
type planColDef struct {
	name string
	typ  string
}
type planTableDef struct {
	name    string
	columns []planColDef
}

type planSchemaProvider struct {
	capStubProvider
	tables []planTableDef
}

func (p *planSchemaProvider) SchemaMigrator(_ context.Context) provider.SchemaMigrator {
	return &planSchemaMigrator{tables: p.tables}
}

type planSchemaMigrator struct {
	tables []planTableDef
}

func (m *planSchemaMigrator) Inspect(_ context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}
	for _, td := range m.tables {
		ts := provider.TableSchema{Name: td.name}
		for _, col := range td.columns {
			ts.Columns = append(ts.Columns, provider.ColumnSchema{Name: col.name, Type: col.typ})
		}
		schema.Tables = append(schema.Tables, ts)
	}
	return schema, nil
}

func (m *planSchemaMigrator) Create(_ context.Context, _ *provider.Schema, _ provider.TypeMapper) error {
	return nil
}

// planSchemaNoEnumProvider supports SchemaMigrator but not TableEnumerator.
// Used to test the schema-inspection fallback path.
type planSchemaNoEnumProvider struct {
	capStubProvider
	tables []string
}

func (p *planSchemaNoEnumProvider) SchemaMigrator(_ context.Context) provider.SchemaMigrator {
	return &planSchemaOnlyMigrator{tables: p.tables}
}

type planSchemaOnlyMigrator struct {
	tables []string
}

func (m *planSchemaOnlyMigrator) Inspect(_ context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}
	for _, name := range m.tables {
		schema.Tables = append(schema.Tables, provider.TableSchema{Name: name})
	}
	return schema, nil
}

func (m *planSchemaOnlyMigrator) Create(_ context.Context, _ *provider.Schema, _ provider.TypeMapper) error {
	return nil
}

// planEnumErrorProvider fails on EnumerateTables.
type planEnumErrorProvider struct {
	capStubProvider
	err error
}

func (p *planEnumErrorProvider) EnumerateTables(_ context.Context) (map[string]int64, error) {
	return nil, p.err
}

// stubTypeMapperTransformer is a cross-DB transformer stub with type mapping.
type stubTypeMapperTransformer struct{}

func (t *stubTypeMapperTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return units, nil
}
func (t *stubTypeMapperTransformer) NeedsSchema() bool            { return false }
func (t *stubTypeMapperTransformer) SetSchema(_ *provider.Schema) {}
func (t *stubTypeMapperTransformer) TypeMapper() provider.TypeMapper {
	return stubTypeMapper{}
}

type stubTypeMapper struct{}

func (stubTypeMapper) MapType(colType string) (string, bool) {
	upper := strings.ToUpper(colType)
	switch {
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT", true
	case strings.HasPrefix(upper, "VARCHAR"):
		return "TEXT", true
	case strings.HasPrefix(upper, "TIMESTAMPTZ"):
		return "TIMESTAMP", true
	default:
		return "", false
	}
}
