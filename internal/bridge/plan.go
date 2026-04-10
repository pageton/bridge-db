package bridge

import (
	"context"
	"fmt"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// MigrationPlan — structured output of the planning stage
// ---------------------------------------------------------------------------

// MigrationPlan describes what will happen during a migration before any data
// is moved. It is built during step 5 (stepPlan) and stored on RunResult for
// dry-run output, debugging, and logging.
type MigrationPlan struct {
	// SourceProvider is the source database provider name.
	SourceProvider string `json:"source_provider"`

	// DestProvider is the destination database provider name.
	DestProvider string `json:"dest_provider"`

	// CrossDB is true when source and destination providers differ.
	CrossDB bool `json:"cross_db"`

	// TransformerType describes the transformer being used (e.g. "noop",
	// "mysql_to_postgres", "redis_to_mongo").
	TransformerType string `json:"transformer_type"`

	// Tables lists the entities (tables, collections) that will be migrated,
	// with estimated row counts from source enumeration. Nil when the source
	// does not support table enumeration or when schema inspection failed.
	Tables []TablePlan `json:"tables,omitempty"`

	// TypeMappings lists resolved source→destination type conversions for
	// each table's columns. Empty when no type mapper is available.
	TypeMappings []TableTypeMapping `json:"type_mappings,omitempty"`

	// UnsupportedFields lists columns or features that cannot be fully
	// represented on the destination (e.g. lossy type conversions, dropped
	// columns from field mappings, unsupported constraints).
	UnsupportedFields []UnsupportedField `json:"unsupported_fields,omitempty"`

	// EstimatedRows is the sum of row counts across all planned tables.
	// 0 when table enumeration is unavailable.
	EstimatedRows int64 `json:"estimated_rows"`

	// EstimatedBatches is the approximate number of write batches based on
	// EstimatedRows and BatchSize. 0 when row estimate is unavailable.
	EstimatedBatches int `json:"estimated_batches"`

	// SchemaMigration indicates whether DDL migration will run.
	SchemaMigration bool `json:"schema_migration"`

	// Verification describes the planned verification level.
	Verification string `json:"verification"`

	// FieldMappings summarizes user-configured field mappings per table.
	FieldMappings []TableFieldMapping `json:"field_mappings,omitempty"`

	// Warnings contains non-fatal issues discovered during planning.
	Warnings []string `json:"warnings,omitempty"`
}

// TablePlan describes a single table/collection in the migration.
type TablePlan struct {
	// Name is the table or collection name.
	Name string `json:"name"`

	// EstimatedRows is the approximate row count from source enumeration.
	// -1 when unavailable.
	EstimatedRows int64 `json:"estimated_rows"`
}

// ColumnTypeMapping describes a single column type conversion.
type ColumnTypeMapping struct {
	Column       string `json:"column"`
	SourceType   string `json:"source_type"`
	DestType     string `json:"dest_type"`
	Lossy        bool   `json:"lossy"`
	NeedsConvert bool   `json:"needs_convert"`
}

// TableTypeMapping groups column type conversions for a table.
type TableTypeMapping struct {
	Table   string              `json:"table"`
	Columns []ColumnTypeMapping `json:"columns"`
}

// UnsupportedField describes a field or feature that cannot be fully migrated.
type UnsupportedField struct {
	Table  string `json:"table"`
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

// TableFieldMapping summarizes user-configured field mappings for a table.
type TableFieldMapping struct {
	Table    string   `json:"table"`
	Actions  []string `json:"actions"`
	NumRules int      `json:"num_rules"`
}

// ---------------------------------------------------------------------------
// Plan builder
// ---------------------------------------------------------------------------

// buildPlan constructs a MigrationPlan from the current pipeline state.
// Must be called after providers are connected and capabilities resolved.
func (p *Pipeline) buildPlan(ctx context.Context) *MigrationPlan {
	plan := &MigrationPlan{
		SourceProvider:  p.config.Source.Provider,
		DestProvider:    p.config.Destination.Provider,
		CrossDB:         p.config.IsCrossDB(),
		SchemaMigration: p.shouldMigrateSchema(),
		TransformerType: transformerName(p.transformer),
		Verification:    string(provider.EffectiveVerifyLevel(p.srcCaps, p.dstCaps)),
	}

	p.planTables(ctx, plan)
	p.planTypeMappings(ctx, plan)
	p.planFieldMappings(plan)
	p.planWarnings(plan)

	if plan.EstimatedRows > 0 && p.opts.BatchSize > 0 {
		plan.EstimatedBatches = int((plan.EstimatedRows + int64(p.opts.BatchSize) - 1) / int64(p.opts.BatchSize))
	}

	return plan
}

// planTables enumerates source tables and populates the Tables and row estimates.
func (p *Pipeline) planTables(ctx context.Context, plan *MigrationPlan) {
	// Try TableEnumerator first (fastest, works for all providers).
	if te, ok := p.src.(provider.TableEnumerator); ok {
		counts, err := te.EnumerateTables(ctx)
		if err != nil {
			plan.Warnings = append(plan.Warnings,
				fmt.Sprintf("could not enumerate source tables: %v", err))
			return
		}
		for name, count := range counts {
			plan.Tables = append(plan.Tables, TablePlan{Name: name, EstimatedRows: count})
			plan.EstimatedRows += count
		}
		return
	}

	// Fallback: inspect schema if provider supports it.
	if p.srcCaps.Schema {
		migrator := p.src.SchemaMigrator(ctx)
		if migrator != nil {
			schema, err := migrator.Inspect(ctx)
			if err != nil {
				plan.Warnings = append(plan.Warnings,
					fmt.Sprintf("could not inspect source schema for planning: %v", err))
				return
			}
			for _, t := range schema.Tables {
				plan.Tables = append(plan.Tables, TablePlan{Name: t.Name, EstimatedRows: -1})
			}
		}
	}
}

// planTypeMappings resolves column type conversions using the transformer's
// TypeMapper (if available) and the inspected source schema.
func (p *Pipeline) planTypeMappings(ctx context.Context, plan *MigrationPlan) {
	if !p.config.IsCrossDB() {
		return
	}

	// Resolve type mapper from transformer.
	var mapper provider.TypeMapper
	if tm, ok := p.transformer.(transform.TypeMapperProvider); ok {
		mapper = tm.TypeMapper()
	}
	if mapper == nil {
		return
	}

	// Get source schema for column types.
	if !p.srcCaps.Schema {
		return
	}
	migrator := p.src.SchemaMigrator(ctx)
	if migrator == nil {
		return
	}
	schema, err := migrator.Inspect(ctx)
	if err != nil {
		plan.Warnings = append(plan.Warnings,
			fmt.Sprintf("could not inspect schema for type mapping: %v", err))
		return
	}

	for _, tbl := range schema.Tables {
		ttm := TableTypeMapping{Table: tbl.Name}
		for _, col := range tbl.Columns {
			mapped, ok := mapper.MapType(col.Type)
			if !ok {
				plan.UnsupportedFields = append(plan.UnsupportedFields, UnsupportedField{
					Table:  tbl.Name,
					Field:  col.Name,
					Reason: fmt.Sprintf("no type mapping for source type %q", col.Type),
				})
				continue
			}
			ctm := ColumnTypeMapping{
				Column:       col.Name,
				SourceType:   col.Type,
				DestType:     mapped,
				NeedsConvert: col.Type != mapped,
				Lossy:        isLossyConversion(col.Type, mapped),
			}
			ttm.Columns = append(ttm.Columns, ctm)

			if ctm.Lossy {
				plan.UnsupportedFields = append(plan.UnsupportedFields, UnsupportedField{
					Table:  tbl.Name,
					Field:  col.Name,
					Reason: fmt.Sprintf("potentially lossy conversion: %s → %s", col.Type, mapped),
				})
			}
		}
		if len(ttm.Columns) > 0 {
			plan.TypeMappings = append(plan.TypeMappings, ttm)
		}
	}
}

// planFieldMappings summarizes user-configured field mappings from the config.
func (p *Pipeline) planFieldMappings(plan *MigrationPlan) {
	if len(p.config.Transform.Mappings) == 0 {
		return
	}
	for table, mappings := range p.config.Transform.Mappings {
		tfm := TableFieldMapping{Table: table, NumRules: len(mappings)}
		seen := make(map[string]bool)
		for _, m := range mappings {
			action := m.Action
			if action == "" {
				action = "rename"
			}
			if !seen[action] {
				seen[action] = true
				tfm.Actions = append(tfm.Actions, action)
			}
			// Dropped fields are unsupported in the output.
			if m.Action == "drop" {
				plan.UnsupportedFields = append(plan.UnsupportedFields, UnsupportedField{
					Table:  table,
					Field:  m.Source,
					Reason: "dropped by field mapping rule",
				})
			}
		}
		plan.FieldMappings = append(plan.FieldMappings, tfm)
	}
}

// planWarnings adds planning warnings for config combinations that may cause
// issues but are not hard errors.
func (p *Pipeline) planWarnings(plan *MigrationPlan) {
	if p.opts.Verify && plan.Verification == string(provider.VerifyNone) {
		plan.Warnings = append(plan.Warnings,
			"verification requested but neither provider supports it — data will not be verified")
	}
	if plan.EstimatedRows == 0 && len(plan.Tables) == 0 && !p.srcCaps.Schema {
		plan.Warnings = append(plan.Warnings,
			"source provider does not support table enumeration — row count estimate unavailable")
	}
	if p.opts.FKHandling == "defer_constraints" && !p.dstCaps.Transactions {
		plan.Warnings = append(plan.Warnings,
			fmt.Sprintf("FK handling set to defer_constraints but %s does not support transactions — this will fail at runtime",
				p.config.Destination.Provider))
	}
}

// logPlan logs the migration plan summary.
func logPlan(plan *MigrationPlan) {
	log := logger.L().With("component", "pipeline", "phase", "plan")

	log.Debug("migration plan built",
		"source", plan.SourceProvider,
		"destination", plan.DestProvider,
		"cross_db", plan.CrossDB,
		"tables", len(plan.Tables),
		"estimated_rows", plan.EstimatedRows,
		"estimated_batches", plan.EstimatedBatches,
		"schema_migration", plan.SchemaMigration,
		"verification", plan.Verification,
		"type_mappings", len(plan.TypeMappings),
		"unsupported_fields", len(plan.UnsupportedFields),
	)

	for _, t := range plan.Tables {
		log.Debug("  table",
			"name", t.Name,
			"estimated_rows", t.EstimatedRows,
		)
	}
	for _, u := range plan.UnsupportedFields {
		log.Warn("  unsupported field",
			"table", u.Table,
			"field", u.Field,
			"reason", u.Reason,
		)
	}
	for _, w := range plan.Warnings {
		log.Warn("  plan warning", "message", w)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// transformerName returns a human-readable name for the transformer.
func transformerName(t transform.Transformer) string {
	switch t.(type) {
	case transform.NoopTransformer:
		return "noop"
	default:
		return fmt.Sprintf("%T", t)
	}
}

// isLossyConversion performs a heuristic check for potentially lossy type
// conversions between SQL dialects.
func isLossyConversion(srcType, dstType string) bool {
	src := normalizeType(srcType)
	dst := normalizeType(dstType)
	if src == dst {
		return false
	}
	lossyPairs := []struct{ src, dst string }{
		{"TIMESTAMPTZ", "TIMESTAMP"},
		{"TIMESTAMP", "DATE"},
		{"TEXT", "VARCHAR"},
		{"BIGINT", "INT"},
		{"BIGINT", "SMALLINT"},
		{"INT", "SMALLINT"},
		{"DOUBLE", "FLOAT"},
		{"DOUBLE", "REAL"},
		{"DECIMAL", "FLOAT"},
		{"NUMERIC", "FLOAT"},
		{"JSONB", "JSON"},
		{"BYTEA", "BLOB"},
	}
	for _, p := range lossyPairs {
		if src == p.src && dst == p.dst {
			return true
		}
	}
	return false
}

// normalizeType strips parameters and normalizes a SQL type name for comparison.
func normalizeType(t string) string {
	s := t
	for i := 0; i < len(s); i++ {
		if s[i] == '(' {
			s = s[:i]
			break
		}
	}
	// Upper-case for comparison.
	if len(s) > 0 {
		upper := make([]byte, len(s))
		for i := range s {
			c := s[i]
			if c >= 'a' && c <= 'z' {
				c -= 'a' - 'A'
			}
			upper[i] = c
		}
		return string(upper)
	}
	return s
}
