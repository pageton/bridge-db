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
// Preflight validation tests
// ---------------------------------------------------------------------------

func TestPreflight_TransformerMissing(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "alpha"},
			Destination: config.ConnectionConfig{Provider: "beta"},
		},
		opts: PipelineOptions{
			FKHandling:    "skip",
			MigrateSchema: false,
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{},
		dstCaps:     provider.Capabilities{},
		src:         &capStubProvider{},
	}

	var errs []string
	p.checkTransformerAvailable(&errs)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	if !strings.Contains(errs[0], "alpha") || !strings.Contains(errs[0], "beta") {
		t.Errorf("error should mention both providers, got: %s", errs[0])
	}
}

func TestPreflight_TransformerPresent(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "alpha"},
			Destination: config.ConnectionConfig{Provider: "beta"},
		},
		opts: PipelineOptions{
			FKHandling:    "skip",
			MigrateSchema: false,
		},
		transformer: transform.NoopTransformer{}, // HasTransformer checks registry, not type
		srcCaps:     provider.Capabilities{},
		dstCaps:     provider.Capabilities{},
		src:         &capStubProvider{},
	}

	// Register a transformer for this pair so HasTransformer returns true.
	transform.RegisterTransformer("alpha", "beta", func() transform.Transformer {
		return transform.NoopTransformer{}
	})

	var errs []string
	p.checkTransformerAvailable(&errs)

	if len(errs) != 0 {
		t.Errorf("expected 0 errors when transformer registered, got: %v", errs)
	}
}

func TestPreflight_SameEngine_NoTransformerNeeded(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "postgres"},
			Destination: config.ConnectionConfig{Provider: "postgres"},
		},
		opts: PipelineOptions{
			FKHandling:    "skip",
			MigrateSchema: false,
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{},
		dstCaps:     provider.Capabilities{},
		src:         &capStubProvider{},
	}

	var errs []string
	p.checkTransformerAvailable(&errs)

	if len(errs) != 0 {
		t.Errorf("same-engine migration should not require a transformer, got: %v", errs)
	}
}

func TestPreflight_MigrateSchemaUnsupported(t *testing.T) {
	tests := []struct {
		name    string
		srcCaps provider.Capabilities
		dstCaps provider.Capabilities
		wantErr bool
	}{
		{
			name:    "both no schema",
			srcCaps: provider.Capabilities{Schema: false},
			dstCaps: provider.Capabilities{Schema: false},
			wantErr: true,
		},
		{
			name:    "source no schema",
			srcCaps: provider.Capabilities{Schema: false},
			dstCaps: provider.Capabilities{Schema: true},
			wantErr: true,
		},
		{
			name:    "dest no schema",
			srcCaps: provider.Capabilities{Schema: true},
			dstCaps: provider.Capabilities{Schema: false},
			wantErr: true,
		},
		{
			name:    "both have schema",
			srcCaps: provider.Capabilities{Schema: true},
			dstCaps: provider.Capabilities{Schema: true},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pipeline{
				config: &config.MigrationConfig{
					Source:      config.ConnectionConfig{Provider: "test"},
					Destination: config.ConnectionConfig{Provider: "test2"},
				},
				opts: PipelineOptions{
					MigrateSchema: true,
				},
				srcCaps: tt.srcCaps,
				dstCaps: tt.dstCaps,
				src:     &capStubProvider{},
			}

			var errs []string
			p.checkMigrateSchemaCapability(&errs)

			if (len(errs) > 0) != tt.wantErr {
				t.Errorf("wantErr=%v, got %d errors: %v", tt.wantErr, len(errs), errs)
			}
		})
	}
}

func TestPreflight_MigrateSchemaDisabled_NoCheck(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "test2"},
		},
		opts: PipelineOptions{
			MigrateSchema: false,
		},
		srcCaps: provider.Capabilities{Schema: false},
		dstCaps: provider.Capabilities{Schema: false},
		src:     &capStubProvider{},
	}

	var errs []string
	p.checkMigrateSchemaCapability(&errs)

	if len(errs) != 0 {
		t.Errorf("migrate-schema=false should skip check, got: %v", errs)
	}
}

func TestPreflight_FKHandlingNoTx(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "redis"},
		},
		opts: PipelineOptions{
			FKHandling: "defer_constraints",
		},
		dstCaps: provider.Capabilities{Transactions: false},
		src:     &capStubProvider{},
	}

	var errs []string
	p.checkFKHandlingCapability(&errs)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	if !strings.Contains(errs[0], "redis") {
		t.Errorf("error should mention redis, got: %s", errs[0])
	}
	if !strings.Contains(errs[0], "--fk-handling=skip") {
		t.Errorf("error should suggest alternatives, got: %s", errs[0])
	}
}

func TestPreflight_FKHandling_Skip_NoError(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "redis"},
		},
		opts: PipelineOptions{
			FKHandling: "skip",
		},
		dstCaps: provider.Capabilities{Transactions: false},
		src:     &capStubProvider{},
	}

	var errs []string
	p.checkFKHandlingCapability(&errs)

	if len(errs) != 0 {
		t.Errorf("fk-handling=skip should not error, got: %v", errs)
	}
}

func TestPreflight_FKHandling_HasTx_NoError(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "test2"},
		},
		opts: PipelineOptions{
			FKHandling: "defer_constraints",
		},
		dstCaps: provider.Capabilities{Transactions: true},
		src:     &capStubProvider{},
	}

	var errs []string
	p.checkFKHandlingCapability(&errs)

	if len(errs) != 0 {
		t.Errorf("tx-supported + defer_constraints should not error, got: %v", errs)
	}
}

func TestPreflight_SourceSchemaEmpty(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "test2"},
		},
		srcCaps: provider.Capabilities{Schema: true},
		src:     &schemaStubProvider{tables: 0},
	}

	var errs []string
	p.checkSourceSchema(context.Background(), &errs)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error for empty source, got %d", len(errs))
	}
	if !strings.Contains(errs[0], "no tables") {
		t.Errorf("error should mention no tables, got: %s", errs[0])
	}
}

func TestPreflight_SourceSchemaHasTables(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "test2"},
		},
		srcCaps: provider.Capabilities{Schema: true},
		src:     &schemaStubProvider{tables: 5},
	}

	var errs []string
	p.checkSourceSchema(context.Background(), &errs)

	if len(errs) != 0 {
		t.Errorf("source with tables should not error, got: %v", errs)
	}
}

func TestPreflight_SourceSchemaNoCaps(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "redis"},
			Destination: config.ConnectionConfig{Provider: "test"},
		},
		srcCaps: provider.Capabilities{Schema: false},
		src:     &capStubProvider{},
	}

	var errs []string
	p.checkSourceSchema(context.Background(), &errs)

	if len(errs) != 0 {
		t.Errorf("no-schema provider should skip source schema check, got: %v", errs)
	}
}

func TestPreflight_SourceSchemaInspectError(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "test"},
			Destination: config.ConnectionConfig{Provider: "test2"},
		},
		srcCaps: provider.Capabilities{Schema: true},
		src:     &schemaErrorProvider{err: fmt.Errorf("permission denied")},
	}

	var errs []string
	p.checkSourceSchema(context.Background(), &errs)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error for inspect failure, got %d", len(errs))
	}
	if !strings.Contains(errs[0], "permission denied") {
		t.Errorf("error should include original error, got: %s", errs[0])
	}
}

func TestPreflight_AllPass(t *testing.T) {
	p := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: "postgres"},
			Destination: config.ConnectionConfig{Provider: "postgres"},
		},
		opts: PipelineOptions{
			FKHandling:    "skip",
			MigrateSchema: false,
			Verify:        false,
		},
		transformer: transform.NoopTransformer{},
		srcCaps:     provider.Capabilities{Schema: true, Transactions: true},
		dstCaps:     provider.Capabilities{Schema: true, Transactions: true},
		src:         &schemaStubProvider{tables: 3},
	}

	err := p.runPreflight(context.Background())
	if err != nil {
		t.Errorf("expected preflight to pass, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Stub providers for preflight tests
// ---------------------------------------------------------------------------

// capStubProvider is a minimal provider for capability-level tests.
type capStubProvider struct{}

func (p *capStubProvider) Name() string                                             { return "capStub" }
func (p *capStubProvider) Connect(_ context.Context, _, _ any) error                { return nil }
func (p *capStubProvider) Close() error                                             { return nil }
func (p *capStubProvider) Ping(_ context.Context) error                             { return nil }
func (p *capStubProvider) DryRun() provider.Provider                                { return p }
func (p *capStubProvider) SchemaMigrator(_ context.Context) provider.SchemaMigrator { return nil }
func (p *capStubProvider) Verifier(_ context.Context) provider.Verifier             { return &stubVerifier{} }
func (p *capStubProvider) Writer(_ context.Context, _ provider.WriteOptions) provider.Writer {
	return nil
}

func (p *capStubProvider) Scanner(_ context.Context, _ provider.ScanOptions) provider.Scanner {
	return nil
}

// schemaStubProvider returns a SchemaMigrator that inspects a schema with n tables.
type schemaStubProvider struct {
	capStubProvider
	tables int
}

func (p *schemaStubProvider) SchemaMigrator(_ context.Context) provider.SchemaMigrator {
	return &stubMigrator{tables: p.tables}
}

type stubMigrator struct {
	tables int
}

func (m *stubMigrator) Inspect(_ context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}
	for i := 0; i < m.tables; i++ {
		schema.Tables = append(schema.Tables, provider.TableSchema{
			Name: fmt.Sprintf("table_%d", i),
		})
	}
	return schema, nil
}

func (m *stubMigrator) Create(_ context.Context, _ *provider.Schema, _ provider.TypeMapper) error {
	return nil
}

// schemaErrorProvider returns a SchemaMigrator that fails on Inspect.
type schemaErrorProvider struct {
	capStubProvider
	err error
}

func (p *schemaErrorProvider) SchemaMigrator(_ context.Context) provider.SchemaMigrator {
	return &errorMigrator{err: p.err}
}

type errorMigrator struct {
	err error
}

func (m *errorMigrator) Inspect(_ context.Context) (*provider.Schema, error) {
	return nil, m.err
}

func (m *errorMigrator) Create(_ context.Context, _ *provider.Schema, _ provider.TypeMapper) error {
	return nil
}
