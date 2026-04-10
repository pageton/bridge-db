package bridge

import (
	"context"
	"fmt"
	"strings"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/pkg/provider"
)

// runPreflight validates migration feasibility before any data movement.
// It checks transformer availability, capability compatibility, source schema,
// and config combinations. Returns an error with actionable messages if any
// critical issue is found. Non-critical issues are logged as warnings.
func (p *Pipeline) runPreflight(ctx context.Context) error {
	log := logger.L().With("component", "pipeline", "phase", "preflight")

	var errs []string

	// 1. Transformer availability for cross-engine pairs.
	p.checkTransformerAvailable(&errs)

	// 2. --migrate-schema vs provider capabilities.
	p.checkMigrateSchemaCapability(&errs)

	// 3. --fk-handling vs destination transaction support.
	p.checkFKHandlingCapability(&errs)

	// 4. --verify vs capabilities (warning only, not a hard error).
	if p.opts.Verify {
		level := provider.EffectiveVerifyLevel(p.srcCaps, p.dstCaps)
		if level == provider.VerifyNone {
			log.Warn("verification enabled but neither provider supports it — verification will report unsupported",
				"source", p.config.Source.Provider,
				"destination", p.config.Destination.Provider,
			)
		}
	}

	// 5. Source schema validation (SQL providers with schema support).
	p.checkSourceSchema(ctx, &errs)

	if len(errs) > 0 {
		return fmt.Errorf("pre-migration validation failed:\n  \u2022 %s", strings.Join(errs, "\n  \u2022 "))
	}

	log.Info("preflight checks passed")
	return nil
}

// checkTransformerAvailable validates that a data transformer is registered
// for cross-engine migration pairs. Same-engine migrations use NoopTransformer
// (passthrough) which is correct.
func (p *Pipeline) checkTransformerAvailable(errs *[]string) {
	if p.config.IsCrossDB() {
		if !transform.HasTransformer(p.config.Source.Provider, p.config.Destination.Provider) {
			*errs = append(*errs, fmt.Sprintf(
				"No data transformer registered for %s \u2192 %s. "+
					"Data will pass through without conversion, which will likely produce incorrect results. "+
					"Check that both providers are compiled in (use 'bridge providers' to list available providers).",
				p.config.Source.Provider, p.config.Destination.Provider,
			))
		}
	}
}

// checkMigrateSchemaCapability validates that --migrate-schema is only used
// with providers that support schema inspection and creation.
func (p *Pipeline) checkMigrateSchemaCapability(errs *[]string) {
	if !p.opts.MigrateSchema {
		return
	}
	if !p.srcCaps.Schema && !p.dstCaps.Schema {
		*errs = append(*errs, fmt.Sprintf(
			"--migrate-schema is enabled but neither source (%s) nor destination (%s) supports schema migration. "+
				"NoSQL providers do not have DDL schemas. Disable with --migrate-schema=false.",
			p.config.Source.Provider, p.config.Destination.Provider,
		))
	} else if !p.srcCaps.Schema {
		*errs = append(*errs, fmt.Sprintf(
			"--migrate-schema is enabled but source provider (%s) does not support schema inspection. "+
				"Disable with --migrate-schema=false.",
			p.config.Source.Provider,
		))
	} else if !p.dstCaps.Schema {
		*errs = append(*errs, fmt.Sprintf(
			"--migrate-schema is enabled but destination provider (%s) does not support schema creation. "+
				"Disable with --migrate-schema=false.",
			p.config.Destination.Provider,
		))
	}
}

// checkFKHandlingCapability validates that FK handling strategies requiring
// transaction support are only used with providers that support transactions.
func (p *Pipeline) checkFKHandlingCapability(errs *[]string) {
	if p.opts.FKHandling == "defer_constraints" && !p.dstCaps.Transactions {
		*errs = append(*errs, fmt.Sprintf(
			"--fk-handling=defer_constraints requires transaction support, but %s does not support transactions. "+
				"Use --fk-handling=skip or --fk-handling=ordered instead.",
			p.config.Destination.Provider,
		))
	}
}

// checkSourceSchema validates that the source database contains tables to migrate.
// Only runs for providers with schema support (SQL databases).
func (p *Pipeline) checkSourceSchema(ctx context.Context, errs *[]string) {
	if !p.srcCaps.Schema {
		return
	}

	migrator := p.src.SchemaMigrator(ctx)
	if migrator == nil {
		return
	}

	schema, err := migrator.Inspect(ctx)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf(
			"Cannot inspect source schema: %v. "+
				"Check that the database is accessible and the user has read permissions.",
			err,
		))
		return
	}

	if len(schema.Tables) == 0 {
		*errs = append(*errs,
			"Source database has no tables — nothing to migrate. "+
				"Check that the database name is correct and contains data.",
		)
	}
}
