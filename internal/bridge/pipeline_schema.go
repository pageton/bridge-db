package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/pkg/provider"
)

// Step 4: Inspect schema/metadata — loads checkpoint, inspects source schema,
// and migrates it to the destination if supported.
func (p *Pipeline) stepInspect(ctx context.Context, result *RunResult, ms *migrationState) error {
	log := p.stepLog(4, "inspect")

	if !p.opts.CheckpointEnabled {
		log.Debug("checkpointing disabled")
	} else {
		checkpoint, err := p.checkpoint.Load(ctx)
		if err != nil {
			log.Warn("failed to load checkpoint, starting fresh", "error", err)
			checkpoint = nil
		}
		ms.checkpoint = checkpoint
	}

	desc := phaseDesc(provider.PhaseSchemaMigration)
	if p.shouldMigrateSchema() {
		phaseStart := time.Now()
		p.reporter.OnPhaseStart(desc)

		if err := p.migrateSchema(ctx); err != nil {
			p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
			return p.abort(NewSchemaError(fmt.Sprintf("Schema migration from %s to %s failed — destination may have incompatible types or constraints", p.config.Source.Provider, p.config.Destination.Provider), err).WithStep(4))
		}

		dur := time.Since(phaseStart)
		p.reporter.OnPhaseDone(desc, dur, nil)
		result.Phases = append(result.Phases, PhaseResult{Phase: provider.PhaseSchemaMigration, Duration: dur})
	} else {
		p.reporter.OnPhaseStart(desc)
		p.reporter.OnPhaseDone(desc, 0, provider.ErrPhaseSkipped)
	}

	return nil
}

// shouldMigrateSchema returns true if schema migration should run,
// based on the declared capabilities of both providers.
func (p *Pipeline) shouldMigrateSchema() bool {
	if !p.opts.MigrateSchema {
		return false
	}
	return provider.SupportsSchemaMigration(p.srcCaps, p.dstCaps)
}

// migrateSchema inspects the source schema and creates it on the destination.
func (p *Pipeline) migrateSchema(ctx context.Context) error {
	log := logger.L().With("component", "pipeline", "phase", "schema")

	srcMigrator := p.src.SchemaMigrator(ctx)
	if srcMigrator == nil {
		if p.srcCaps.Schema {
			log.Warn("source claims schema support but returned nil migrator — schema migration silently skipped")
		} else {
			log.Debug("source has no schema migrator, skipping")
		}
		return nil
	}

	schema, err := srcMigrator.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect source schema: %w", err)
	}

	log.Debug("inspected source schema", "tables", len(schema.Tables))

	if p.transformer.NeedsSchema() {
		p.transformer.SetSchema(schema)
	}

	hasColumns := false
	for _, t := range schema.Tables {
		if len(t.Columns) > 0 {
			hasColumns = true
			break
		}
	}
	if !hasColumns {
		log.Debug("source schema has no column definitions — skipping schema migration (NoSQL source)")
		return nil
	}

	dstMigrator := p.dst.SchemaMigrator(ctx)
	if dstMigrator == nil {
		if p.dstCaps.Schema {
			log.Warn("destination claims schema support but returned nil migrator — schema migration silently skipped")
		} else {
			log.Debug("destination has no schema migrator, skipping")
		}
		return nil
	}

	var mapper provider.TypeMapper
	if p.config.IsCrossDB() {
		if tm, ok := p.transformer.(transform.TypeMapperProvider); ok {
			mapper = tm.TypeMapper()
		}
	}

	if err := dstMigrator.Create(ctx, schema, mapper); err != nil {
		return fmt.Errorf("create destination schema: %w", err)
	}

	log.Debug("schema created on destination")
	return nil
}

// buildTransformerConfig constructs the TransformerConfig from MigrationConfig.
func (p *Pipeline) buildTransformerConfig() transform.TransformerConfig {
	tc := transform.TransformerConfig{
		SrcDialect: transform.Dialect(p.config.Source.Provider),
		DstDialect: transform.Dialect(p.config.Destination.Provider),
	}

	tc.NullHandler = &transform.NullHandler{
		Policy: transform.NullPolicyFromString(p.config.Transform.NullPolicy),
	}

	if len(p.config.Transform.Mappings) > 0 {
		tc.FieldMapping = transform.NewFieldMappingApplier(p.config.Transform.Mappings)
	}

	return tc
}
