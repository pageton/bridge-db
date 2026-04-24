package app

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/progress"
	"github.com/pageton/bridge-db/internal/tunnel"
	verifypkg "github.com/pageton/bridge-db/internal/verify"
	"github.com/pageton/bridge-db/pkg/provider"
)

// ProviderCapabilities is the stable application-level provider descriptor used
// by MCP and future non-CLI interfaces.
type ProviderCapabilities struct {
	Name                    string   `json:"name"`
	Kind                    string   `json:"kind"`
	CompiledIn              bool     `json:"compiled_in"`
	SupportsSchemaMigration bool     `json:"supports_schema_migration"`
	SupportsVerification    bool     `json:"supports_verification"`
	VerificationLevel       string   `json:"verification_level"`
	SupportsTransactions    bool     `json:"supports_transactions"`
	SupportsCheckpointing   bool     `json:"supports_checkpointing"`
	SupportsDryRun          bool     `json:"supports_dry_run"`
	SupportedConflictModes  []string `json:"supported_conflict_modes"`
	SupportedDataTypes      []string `json:"supported_data_types,omitempty"`
	ConstraintNotes         []string `json:"constraint_notes,omitempty"`
	Limitations             []string `json:"limitations,omitempty"`
}

// ProviderRegistryService exposes compiled-in providers with structured
// metadata derived from the provider registry and capability table.
type ProviderRegistryService struct{}

// ListProviders returns all compiled-in providers in stable order.
func (ProviderRegistryService) ListProviders(_ context.Context) ([]ProviderCapabilities, error) {
	names := provider.AvailableProviders()
	sort.Strings(names)
	out := make([]ProviderCapabilities, 0, len(names))
	for _, name := range names {
		caps, err := (ProviderRegistryService{}).GetProviderCapabilities(context.Background(), name)
		if err != nil {
			return nil, err
		}
		out = append(out, *caps)
	}
	return out, nil
}

// GetProviderCapabilities returns structured metadata for a single provider.
func (ProviderRegistryService) GetProviderCapabilities(_ context.Context, name string) (*ProviderCapabilities, error) {
	known := provider.KnownCapabilities(name)
	if _, err := provider.New(name); err != nil {
		return nil, err
	}

	out := &ProviderCapabilities{
		Name:                    name,
		Kind:                    providerKind(name),
		CompiledIn:              true,
		SupportsSchemaMigration: known.Schema,
		SupportsVerification:    known.Verification != provider.VerifyNone,
		VerificationLevel:       string(known.Verification),
		SupportsTransactions:    known.Transactions,
		SupportsCheckpointing:   known.Incremental,
		SupportsDryRun:          true,
		SupportedConflictModes:  []string{string(provider.ConflictOverwrite), string(provider.ConflictSkip), string(provider.ConflictError)},
		SupportedDataTypes:      providerDataTypes(name),
	}

	if !known.Schema {
		out.Limitations = append(out.Limitations, "schema migration is not supported")
	}
	if !known.Transactions {
		out.ConstraintNotes = append(out.ConstraintNotes, "transaction-based FK deferral is unavailable")
	}
	return out, nil
}

// PlanningService exposes first-class migration planning.
type PlanningService struct{}

// PlanMigration returns a structured migration plan without executing transfer.
func (PlanningService) PlanMigration(ctx context.Context, cfg *config.MigrationConfig, opts bridge.PipelineOptions) (*bridge.MigrationPlan, error) {
	opts.DryRun = true
	opts.Verify = false
	pipeline, err := bridge.NewPipeline(cfg, opts, progress.NopReporter{}, nil)
	if err != nil {
		return nil, err
	}
	result, err := pipeline.Plan(ctx)
	if err != nil {
		return nil, err
	}
	if result.Plan == nil {
		return nil, fmt.Errorf("migration plan unavailable")
	}
	return result.Plan, nil
}

// MigrationPlanExplanation is a human-oriented rendering of a plan, designed
// for assistants that need to explain bridge-db behavior to users.
type MigrationPlanExplanation struct {
	WhatWillHappen  []string `json:"what_will_happen"`
	WhatMightFail   []string `json:"what_might_fail,omitempty"`
	RequiresReview  []string `json:"requires_review,omitempty"`
	Transformations []string `json:"transformations,omitempty"`
	Limitations     []string `json:"limitations,omitempty"`
	Summary         string   `json:"summary"`
}

// ExplainPlan translates a structured plan into an assistant-friendly summary.
func (PlanningService) ExplainPlan(_ context.Context, plan *bridge.MigrationPlan) (*MigrationPlanExplanation, error) {
	if plan == nil {
		return nil, fmt.Errorf("plan is required")
	}

	explanation := &MigrationPlanExplanation{}
	explanation.WhatWillHappen = append(explanation.WhatWillHappen,
		fmt.Sprintf("bridge-db will migrate from %s to %s.", plan.SourceProvider, plan.DestProvider),
	)
	if len(plan.Tables) > 0 {
		explanation.WhatWillHappen = append(explanation.WhatWillHappen,
			fmt.Sprintf("%d table(s) or collection(s) are included in the plan.", len(plan.Tables)),
		)
	}
	if plan.SchemaMigration {
		explanation.WhatWillHappen = append(explanation.WhatWillHappen,
			"Schema migration will run before data transfer.",
		)
	}
	if plan.EstimatedBatches > 0 {
		explanation.WhatWillHappen = append(explanation.WhatWillHappen,
			fmt.Sprintf("The transfer is estimated to use about %d batch(es).", plan.EstimatedBatches),
		)
	}
	if plan.Verification != "" && plan.Verification != string(provider.VerifyNone) {
		explanation.WhatWillHappen = append(explanation.WhatWillHappen,
			fmt.Sprintf("Verification is planned at the %s level.", plan.Verification),
		)
	}

	for _, mapping := range plan.TypeMappings {
		for _, col := range mapping.Columns {
			if col.NeedsConvert {
				explanation.Transformations = append(explanation.Transformations,
					fmt.Sprintf("%s.%s: %s → %s", mapping.Table, col.Column, col.SourceType, col.DestType),
				)
			}
		}
	}
	for _, unsupported := range plan.UnsupportedFields {
		explanation.WhatMightFail = append(explanation.WhatMightFail,
			fmt.Sprintf("%s.%s: %s", unsupported.Table, unsupported.Field, unsupported.Reason),
		)
	}
	for _, warning := range plan.Warnings {
		explanation.RequiresReview = append(explanation.RequiresReview, warning)
		explanation.Limitations = append(explanation.Limitations, warning)
	}

	summaryParts := []string{
		fmt.Sprintf("This plan migrates %s to %s", plan.SourceProvider, plan.DestProvider),
	}
	if plan.CrossDB {
		summaryParts = append(summaryParts, "across different database engines")
	}
	if len(plan.UnsupportedFields) > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("with %d field(s) requiring review", len(plan.UnsupportedFields)))
	}
	explanation.Summary = strings.Join(summaryParts, " ") + "."
	return explanation, nil
}

// VerificationService exposes verification independently of migration.
type VerificationService struct{}

// VerifyMigration connects providers and runs cross-verification.
func (VerificationService) VerifyMigration(ctx context.Context, cfg *config.MigrationConfig, opts verifypkg.Options) (*verifypkg.VerificationReport, error) {
	tunnelPool := tunnel.NewPool()
	if cfg.Source.SSH != nil && cfg.Source.SSH.Enabled {
		if err := tunnelPool.OpenAll(ctx, map[string]tunnel.Config{"source": *cfg.Source.SSH}); err != nil {
			return nil, fmt.Errorf("source tunnel: %w", err)
		}
	}
	if cfg.Destination.SSH != nil && cfg.Destination.SSH.Enabled {
		if err := tunnelPool.OpenAll(ctx, map[string]tunnel.Config{"destination": *cfg.Destination.SSH}); err != nil {
			_ = tunnelPool.CloseAll()
			return nil, fmt.Errorf("dest tunnel: %w", err)
		}
	}
	defer func() { _ = tunnelPool.CloseAll() }()

	srcProvider, err := provider.New(cfg.Source.Provider)
	if err != nil {
		return nil, fmt.Errorf("source provider: %w", err)
	}
	defer func() { _ = srcProvider.Close() }()

	dstProvider, err := provider.New(cfg.Destination.Provider)
	if err != nil {
		return nil, fmt.Errorf("dest provider: %w", err)
	}
	defer func() { _ = dstProvider.Close() }()

	srcCfg, err := config.ProviderConfigWithTunnel(&cfg.Source, tunnelPool.ResolvedAddr("source"))
	if err != nil {
		return nil, fmt.Errorf("source tunnel config: %w", err)
	}
	if err := srcProvider.Connect(ctx, srcCfg, nil); err != nil {
		return nil, fmt.Errorf("source connect: %w", err)
	}
	if err := srcProvider.Ping(ctx); err != nil {
		return nil, fmt.Errorf("source ping: %w", err)
	}

	dstCfg, err := config.ProviderConfigWithTunnel(&cfg.Destination, tunnelPool.ResolvedAddr("destination"))
	if err != nil {
		return nil, fmt.Errorf("dest tunnel config: %w", err)
	}
	if err := dstProvider.Connect(ctx, nil, dstCfg); err != nil {
		return nil, fmt.Errorf("dest connect: %w", err)
	}
	if err := dstProvider.Ping(ctx); err != nil {
		return nil, fmt.Errorf("dest ping: %w", err)
	}

	cv, err := verifypkg.NewCrossVerifier(srcProvider, dstProvider, opts)
	if err != nil {
		return nil, err
	}
	return cv.Verify(ctx)
}

func providerKind(name string) string {
	switch name {
	case "mongodb", "redis":
		return "nosql"
	default:
		return "sql"
	}
}

func providerDataTypes(name string) []string {
	switch name {
	case "redis":
		return []string{
			string(provider.DataTypeString),
			string(provider.DataTypeHash),
			string(provider.DataTypeList),
			string(provider.DataTypeSet),
			string(provider.DataTypeZSet),
			string(provider.DataTypeStream),
		}
	case "mongodb":
		return []string{string(provider.DataTypeDocument)}
	default:
		return []string{string(provider.DataTypeRow)}
	}
}
