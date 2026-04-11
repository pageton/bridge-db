// Package mcp implements a Model Context Protocol server that exposes bridge-db
// functionality as tools for AI assistants.
package mcp

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/pageton/bridge-db/internal/app"
	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	verifypkg "github.com/pageton/bridge-db/internal/verify"
	"github.com/pageton/bridge-db/pkg/provider"
)

var defaultRunStatusService = newDefaultRunStatusService()
var defaultMigrationService = app.NewMigrationService(defaultRunStatusService)

func newDefaultRunStatusService() *app.RunStatusService {
	runs, err := app.NewPersistentRunStatusService(".bridge-db/runs")
	if err != nil {
		return app.NewRunStatusService()
	}
	return runs
}

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

// ConnectionInput describes a database connection for MCP tool parameters.
type ConnectionInput struct {
	Provider string `json:"provider" jsonschema:"Database provider name (postgres, mysql, mariadb, cockroachdb, mongodb, mssql, sqlite, redis)"`
	URL      string `json:"url" jsonschema:"Connection URL (e.g. postgres://user:pass@host:5432/db). Provider is auto-detected from URL scheme if not set."`
}

// ---------------------------------------------------------------------------
// list_providers
// ---------------------------------------------------------------------------

// ListProvidersInput has no parameters.
type ListProvidersInput struct{}

// ProviderInfo describes a compiled-in database provider and its capabilities.
type ProviderInfo struct {
	Name         string `json:"name"`
	Schema       bool   `json:"schema"`
	Transactions bool   `json:"transactions"`
	Verification string `json:"verification"`
}

// ListProvidersOutput lists all compiled-in providers.
type ListProvidersOutput struct {
	Providers []app.ProviderCapabilities `json:"providers"`
	Error     *app.StructuredError       `json:"error,omitempty"`
}

func handleListProviders(_ context.Context, _ *mcp.CallToolRequest, _ ListProvidersInput) (*mcp.CallToolResult, ListProvidersOutput, error) {
	providers, err := (app.ProviderRegistryService{}).ListProviders(context.Background())
	if err != nil {
		return nil, ListProvidersOutput{Error: app.ErrorFrom(err, string(bridge.ErrInternal), "provider_discovery", "", "")}, nil
	}
	out := ListProvidersOutput{Providers: providers}
	return nil, out, nil
}

// ---------------------------------------------------------------------------
// get_provider_capabilities
// ---------------------------------------------------------------------------

// GetProviderCapabilitiesInput selects a single provider by name.
type GetProviderCapabilitiesInput struct {
	Provider string `json:"provider" jsonschema:"Database provider name to inspect"`
}

// GetProviderCapabilitiesOutput contains structured provider metadata.
type GetProviderCapabilitiesOutput struct {
	Provider app.ProviderCapabilities `json:"provider"`
	Error    *app.StructuredError     `json:"error,omitempty"`
}

func handleGetProviderCapabilities(_ context.Context, _ *mcp.CallToolRequest, input GetProviderCapabilitiesInput) (*mcp.CallToolResult, GetProviderCapabilitiesOutput, error) {
	meta, err := (app.ProviderRegistryService{}).GetProviderCapabilities(context.Background(), input.Provider)
	if err != nil {
		return nil, GetProviderCapabilitiesOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "provider_discovery", input.Provider, "")}, nil
	}
	return nil, GetProviderCapabilitiesOutput{Provider: *meta}, nil
}

// ---------------------------------------------------------------------------
// inspect_schema
// ---------------------------------------------------------------------------

// InspectSchemaInput specifies which database to inspect.
type InspectSchemaInput struct {
	Connection ConnectionInput `json:"connection"`
}

// InspectSchemaOutput contains the database schema.
type InspectSchemaOutput struct {
	Provider string                 `json:"provider"`
	Tables   []provider.TableSchema `json:"tables"`
	Error    *app.StructuredError   `json:"error,omitempty"`
}

func handleInspectSchema(ctx context.Context, _ *mcp.CallToolRequest, input InspectSchemaInput) (*mcp.CallToolResult, InspectSchemaOutput, error) {
	connCfg, err := resolveConnection(input.Connection)
	if err != nil {
		return nil, InspectSchemaOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "config", input.Connection.Provider, "source")}, nil
	}

	p, err := provider.New(connCfg.Provider)
	if err != nil {
		return nil, InspectSchemaOutput{Provider: connCfg.Provider, Error: app.ErrorFrom(err, string(bridge.ErrConfig), "provider_discovery", connCfg.Provider, "source")}, nil
	}
	defer func() { _ = p.Close() }()

	cfgAny := config.ProviderConfig(connCfg)
	if err := p.Connect(ctx, cfgAny, nil); err != nil {
		return nil, InspectSchemaOutput{Provider: connCfg.Provider, Error: app.ErrorFrom(err, string(bridge.ErrConnection), "connect", connCfg.Provider, "source")}, nil
	}
	if err := p.Ping(ctx); err != nil {
		return nil, InspectSchemaOutput{Provider: connCfg.Provider, Error: app.ErrorFrom(err, string(bridge.ErrConnection), "connect", connCfg.Provider, "source")}, nil
	}

	migrator := p.SchemaMigrator(ctx)
	if migrator == nil {
		return nil, InspectSchemaOutput{
			Provider: connCfg.Provider,
			Error:    app.ErrorFrom(fmt.Errorf("provider %q does not support schema inspection", connCfg.Provider), string(bridge.ErrSchema), "inspect_schema", connCfg.Provider, "source"),
		}, nil
	}

	schema, err := migrator.Inspect(ctx)
	if err != nil {
		return nil, InspectSchemaOutput{Provider: connCfg.Provider, Error: app.ErrorFrom(err, string(bridge.ErrSchema), "inspect_schema", connCfg.Provider, "source")}, nil
	}

	return nil, InspectSchemaOutput{
		Provider: connCfg.Provider,
		Tables:   schema.Tables,
	}, nil
}

// ---------------------------------------------------------------------------
// migrate
// ---------------------------------------------------------------------------

// MigrateInput specifies a migration between two databases.
// Supports either structured fields or a config file/YAML.
type MigrateInput struct {
	// Config file or inline YAML takes precedence when set.
	ConfigPath string `json:"config_path,omitempty" jsonschema:"Path to a YAML config file (e.g. configs/example.yaml). When set, source/destination/pipeline fields are used as overrides."`
	ConfigYAML string `json:"config_yaml,omitempty" jsonschema:"Inline YAML config string. When set, source/destination/pipeline fields are used as overrides."`

	Source         ConnectionInput `json:"source"`
	Destination    ConnectionInput `json:"destination"`
	CheckpointPath string          `json:"checkpoint_path,omitempty" jsonschema:"Checkpoint file path (default .bridge-db/checkpoint.json)"`
	BatchSize      int             `json:"batch_size,omitempty" jsonschema:"Number of records per batch (default 1000)"`
	Verify         *bool           `json:"verify,omitempty" jsonschema:"Verify data after migration (default true)"`
	MigrateSchema  *bool           `json:"migrate_schema,omitempty" jsonschema:"Migrate DDL schema (default true)"`
	OnConflict     string          `json:"on_conflict,omitempty" jsonschema:"Conflict strategy: overwrite, skip, error (default overwrite)"`
	FKHandling     string          `json:"fk_handling,omitempty" jsonschema:"Foreign key handling: defer_constraints, ordered, skip (default defer_constraints)"`
	MaxRetries     int             `json:"max_retries,omitempty" jsonschema:"Max retry attempts per batch (default 3)"`
	Parallel       int             `json:"parallel,omitempty" jsonschema:"Scan/write buffer depth (default 4)"`
	WriteWorkers   int             `json:"write_workers,omitempty" jsonschema:"Concurrent writer goroutines (default 1)"`
	Checkpoint     *bool           `json:"checkpoint,omitempty" jsonschema:"Enable checkpointing for resumability (default true)"`
	Resume         *bool           `json:"resume,omitempty" jsonschema:"Resume from last checkpoint (default false)"`
	FailFast       *bool           `json:"fail_fast,omitempty" jsonschema:"Abort on first transform error (default false)"`
}

// PlanMigrationOutput contains the first-class structured migration plan.
type PlanMigrationOutput struct {
	Plan  *bridge.MigrationPlan `json:"plan"`
	Error *app.StructuredError  `json:"error,omitempty"`
}

func handlePlanMigration(ctx context.Context, _ *mcp.CallToolRequest, input MigrateInput) (*mcp.CallToolResult, PlanMigrationOutput, error) {
	cfg, opts, err := buildPlanRequest(input)
	if err != nil {
		return nil, PlanMigrationOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "planning", "", "")}, nil
	}
	plan, err := (app.PlanningService{}).PlanMigration(ctx, cfg, opts)
	if err != nil {
		return nil, PlanMigrationOutput{Error: app.ErrorFrom(err, "planning", "planning", cfg.Source.Provider, "source")}, nil
	}
	return nil, PlanMigrationOutput{Plan: plan}, nil
}

// ExplainMigrationPlanInput asks the server to explain a structured plan.
type ExplainMigrationPlanInput struct {
	Plan bridge.MigrationPlan `json:"plan"`
}

// ExplainMigrationPlanOutput contains a user-friendly explanation of a plan.
type ExplainMigrationPlanOutput struct {
	Explanation *app.MigrationPlanExplanation `json:"explanation"`
	Error       *app.StructuredError          `json:"error,omitempty"`
}

func handleExplainMigrationPlan(ctx context.Context, _ *mcp.CallToolRequest, input ExplainMigrationPlanInput) (*mcp.CallToolResult, ExplainMigrationPlanOutput, error) {
	explanation, err := (app.PlanningService{}).ExplainPlan(ctx, &input.Plan)
	if err != nil {
		return nil, ExplainMigrationPlanOutput{Error: app.ErrorFrom(err, "planning", "planning", input.Plan.SourceProvider, "source")}, nil
	}
	return nil, ExplainMigrationPlanOutput{Explanation: explanation}, nil
}

// MigrateOutput contains migration results.
type MigrateOutput struct {
	Success     bool                    `json:"success"`
	Source      string                  `json:"source_provider"`
	Destination string                  `json:"dest_provider"`
	Duration    string                  `json:"duration"`
	Written     int64                   `json:"written"`
	Failed      int64                   `json:"failed"`
	Skipped     int64                   `json:"skipped"`
	Bytes       int64                   `json:"bytes"`
	Tables      []provider.TableMetrics `json:"tables,omitempty"`
	Verified    bool                    `json:"verified"`
	Errors      []string                `json:"errors,omitempty"`
	Error       *app.StructuredError    `json:"error,omitempty"`
}

// RunMigrationInput starts a tracked migration run.
type RunMigrationInput struct {
	MigrateInput
	WaitForCompletion *bool `json:"wait_for_completion,omitempty" jsonschema:"Wait until the migration completes before returning"`
}

// RunMigrationOutput returns the tracked migration run.
type RunMigrationOutput struct {
	Run   *app.MigrationRun    `json:"run"`
	Error *app.StructuredError `json:"error,omitempty"`
}

// GetMigrationStatusInput looks up a tracked migration run.
type GetMigrationStatusInput struct {
	RunID string `json:"run_id" jsonschema:"Tracked migration run identifier"`
}

// GetMigrationStatusOutput returns the latest status snapshot for a run.
type GetMigrationStatusOutput struct {
	Run   *app.MigrationRun    `json:"run"`
	Error *app.StructuredError `json:"error,omitempty"`
}

// ResumeMigrationInput resumes a migration from checkpoint state.
type ResumeMigrationInput struct {
	MigrateInput
	WaitForCompletion *bool `json:"wait_for_completion,omitempty" jsonschema:"Wait until the resumed migration completes before returning"`
}

// ResumeMigrationOutput returns the resume validation result and tracked run.
type ResumeMigrationOutput struct {
	Valid      bool                 `json:"valid"`
	Reason     string               `json:"reason,omitempty"`
	Checkpoint *app.CheckpointState `json:"checkpoint,omitempty"`
	Run        *app.MigrationRun    `json:"run,omitempty"`
	Error      *app.StructuredError `json:"error,omitempty"`
}

func handleRunMigration(ctx context.Context, _ *mcp.CallToolRequest, input RunMigrationInput) (*mcp.CallToolResult, RunMigrationOutput, error) {
	cfg, opts, err := buildPlanRequest(input.MigrateInput)
	if err != nil {
		return nil, RunMigrationOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "config", "", "")}, nil
	}
	run, err := defaultMigrationService.StartRun(ctx, cfg, opts)
	if err != nil {
		return nil, RunMigrationOutput{Error: app.ErrorFrom(err, string(bridge.ErrInternal), "run_migration", cfg.Source.Provider, "source")}, nil
	}
	if input.WaitForCompletion != nil && *input.WaitForCompletion {
		run, err = defaultRunStatusService.Wait(run.RunID, ctx)
		if err != nil {
			return nil, RunMigrationOutput{Run: run, Error: app.ErrorFrom(err, string(bridge.ErrInternal), "run_migration", cfg.Source.Provider, "source")}, nil
		}
	}
	return nil, RunMigrationOutput{Run: run}, nil
}

func handleGetMigrationStatus(ctx context.Context, _ *mcp.CallToolRequest, input GetMigrationStatusInput) (*mcp.CallToolResult, GetMigrationStatusOutput, error) {
	run, err := defaultMigrationService.GetRun(ctx, input.RunID)
	if err != nil {
		return nil, GetMigrationStatusOutput{Error: app.ErrorFrom(err, string(bridge.ErrInternal), "status", "", "")}, nil
	}
	return nil, GetMigrationStatusOutput{Run: run}, nil
}

// ---------------------------------------------------------------------------
// list_migration_runs
// ---------------------------------------------------------------------------

// ListMigrationRunsInput has no required parameters.
type ListMigrationRunsInput struct{}

// ListMigrationRunsOutput returns summary records for all known runs.
type ListMigrationRunsOutput struct {
	Runs []app.MigrationRunSummary `json:"runs"`
}

func handleListMigrationRuns(_ context.Context, _ *mcp.CallToolRequest, _ ListMigrationRunsInput) (*mcp.CallToolResult, ListMigrationRunsOutput, error) {
	runs, err := defaultRunStatusService.List()
	if err != nil {
		return nil, ListMigrationRunsOutput{}, err
	}
	return nil, ListMigrationRunsOutput{Runs: runs}, nil
}

// ---------------------------------------------------------------------------
// inspect_checkpoint
// ---------------------------------------------------------------------------

// InspectCheckpointInput selects a checkpoint file to inspect.
type InspectCheckpointInput struct {
	Path string `json:"path" jsonschema:"Checkpoint file path to inspect"`
}

// InspectCheckpointOutput returns checkpoint state and a human-readable summary.
type InspectCheckpointOutput struct {
	Checkpoint *app.CheckpointState `json:"checkpoint"`
	Summary    string               `json:"summary"`
	Error      *app.StructuredError `json:"error,omitempty"`
}

func handleInspectCheckpoint(ctx context.Context, _ *mcp.CallToolRequest, input InspectCheckpointInput) (*mcp.CallToolResult, InspectCheckpointOutput, error) {
	cp, err := (app.CheckpointService{}).LoadCheckpoint(ctx, input.Path)
	if err != nil {
		return nil, InspectCheckpointOutput{Error: app.ErrorFrom(err, "checkpoint", "inspect_checkpoint", "", "")}, nil
	}
	summary := fmt.Sprintf("Checkpoint for %s → %s, batch %d, %d records written, %d table(s) completed.",
		cp.SourceProvider, cp.DestProvider, cp.LastBatchID, cp.TotalWritten, len(cp.TablesCompleted))
	return nil, InspectCheckpointOutput{Checkpoint: cp, Summary: summary}, nil
}

func handleResumeMigration(ctx context.Context, _ *mcp.CallToolRequest, input ResumeMigrationInput) (*mcp.CallToolResult, ResumeMigrationOutput, error) {
	cfg, opts, err := buildPlanRequest(input.MigrateInput)
	if err != nil {
		return nil, ResumeMigrationOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "resume", "", "")}, nil
	}
	if cfg.Checkpoint.Path == "" {
		return nil, ResumeMigrationOutput{Error: app.ErrorFrom(fmt.Errorf("checkpoint_path is required for resume"), "resume", "resume", "", "")}, nil
	}
	validation, err := (app.CheckpointService{}).ValidateResume(ctx, cfg, cfg.Checkpoint.Path)
	if err != nil {
		return nil, ResumeMigrationOutput{Error: app.ErrorFrom(err, "checkpoint", "resume", cfg.Source.Provider, "source")}, nil
	}
	if !validation.Valid {
		return nil, ResumeMigrationOutput{Valid: false, Reason: validation.Reason, Checkpoint: validation.Checkpoint}, nil
	}
	opts.Resume = true
	opts.CheckpointEnabled = true
	run, err := defaultMigrationService.StartRun(ctx, cfg, opts)
	if err != nil {
		return nil, ResumeMigrationOutput{Valid: true, Checkpoint: validation.Checkpoint, Error: app.ErrorFrom(err, string(bridge.ErrInternal), "resume", cfg.Source.Provider, "source")}, nil
	}
	if input.WaitForCompletion != nil && *input.WaitForCompletion {
		run, err = defaultRunStatusService.Wait(run.RunID, ctx)
		if err != nil {
			return nil, ResumeMigrationOutput{Valid: true, Checkpoint: validation.Checkpoint, Run: run, Error: app.ErrorFrom(err, string(bridge.ErrInternal), "resume", cfg.Source.Provider, "source")}, nil
		}
	}
	return nil, ResumeMigrationOutput{Valid: true, Checkpoint: validation.Checkpoint, Run: run}, nil
}

func handleMigrate(ctx context.Context, _ *mcp.CallToolRequest, input MigrateInput) (*mcp.CallToolResult, MigrateOutput, error) {
	cfg, opts, err := buildPlanRequest(input)
	if err != nil {
		return nil, MigrateOutput{Success: false, Error: app.ErrorFrom(err, string(bridge.ErrConfig), "config", "", "")}, nil
	}

	result, _, err := defaultMigrationService.RunSync(ctx, cfg, opts, nil)
	if err != nil {
		return nil, MigrateOutput{
			Success:     false,
			Source:      cfg.Source.Provider,
			Destination: cfg.Destination.Provider,
			Error:       app.ErrorFrom(err, string(bridge.ErrInternal), "run_migration", cfg.Source.Provider, "source"),
		}, nil
	}

	out := MigrateOutput{
		Success:     result.Summary.TotalFailed == 0,
		Source:      result.SrcProvider,
		Destination: result.DstProvider,
		Duration:    result.Summary.Duration.Round(time.Millisecond).String(),
		Written:     result.Summary.TotalWritten,
		Failed:      result.Summary.TotalFailed,
		Skipped:     result.Summary.TotalSkipped,
		Bytes:       result.Summary.BytesTransferred,
		Tables:      result.Summary.TableMetrics,
		Verified:    result.Summary.VerificationOK,
	}
	for _, e := range result.Summary.Errors {
		out.Errors = append(out.Errors, e.Error())
	}

	return nil, out, nil
}

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

// VerifyInput specifies a verification between two databases.
type VerifyInput struct {
	Source      ConnectionInput `json:"source"`
	Destination ConnectionInput `json:"destination"`
	SampleMode  string          `json:"sample_mode,omitempty" jsonschema:"Sampling mode: none, count, pct (default pct)"`
	SampleN     int             `json:"sample_n,omitempty" jsonschema:"Records to sample per table when mode=count (default 100)"`
	SamplePct   float64         `json:"sample_pct,omitempty" jsonschema:"Percentage to sample per table when mode=pct (default 5.0)"`
	NoChecksum  *bool           `json:"no_checksum,omitempty" jsonschema:"Disable checksum comparison"`
	CountsOnly  *bool           `json:"counts_only,omitempty" jsonschema:"Only compare row counts, skip sampling"`
}

// VerifyOutput contains verification results.
type VerifyOutput struct {
	Status          string                     `json:"status"`
	SourceProvider  string                     `json:"source_provider"`
	DestProvider    string                     `json:"dest_provider"`
	TotalTables     int                        `json:"total_tables"`
	PassCount       int                        `json:"pass_count"`
	FailCount       int                        `json:"fail_count"`
	WarnCount       int                        `json:"warn_count"`
	TotalSampled    int                        `json:"total_sampled"`
	TotalMismatches int                        `json:"total_mismatches"`
	Duration        string                     `json:"duration"`
	Tables          []verifypkg.TableResult    `json:"tables,omitempty"`
	Mismatches      []verifypkg.MismatchDetail `json:"mismatches,omitempty"`
	Error           *app.StructuredError       `json:"error,omitempty"`
}

func handleVerify(ctx context.Context, _ *mcp.CallToolRequest, input VerifyInput) (*mcp.CallToolResult, VerifyOutput, error) {
	cfg, err := buildMigrationConfig(input.Source, input.Destination)
	if err != nil {
		return nil, VerifyOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "verify", "", "")}, nil
	}

	opts := verifypkg.DefaultOptions()
	if input.CountsOnly != nil && *input.CountsOnly {
		opts = verifypkg.CountsOnly()
	} else {
		if input.SampleMode != "" {
			opts.SampleMode = input.SampleMode
		}
		if input.SampleN > 0 {
			opts.SampleN = input.SampleN
		}
		if input.SamplePct > 0 {
			opts.SamplePct = input.SamplePct
		}
		if input.NoChecksum != nil {
			opts.ChecksumComparison = !*input.NoChecksum
		}
	}

	report, err := (app.VerificationService{}).VerifyMigration(ctx, cfg, opts)
	if err != nil {
		return nil, VerifyOutput{
			SourceProvider: cfg.Source.Provider,
			DestProvider:   cfg.Destination.Provider,
			Error:          app.ErrorFrom(err, string(bridge.ErrVerify), "verify", cfg.Source.Provider, "source"),
		}, nil
	}

	return nil, VerifyOutput{
		Status:          string(report.Status),
		SourceProvider:  report.SourceProvider,
		DestProvider:    report.DestProvider,
		TotalTables:     report.TotalTables,
		PassCount:       report.PassCount,
		FailCount:       report.FailCount,
		WarnCount:       report.WarnCount,
		TotalSampled:    report.TotalSampled,
		TotalMismatches: report.TotalMismatches,
		Duration:        report.Duration.Round(time.Millisecond).String(),
		Tables:          report.Tables,
		Mismatches:      report.Mismatches,
	}, nil
}

// ---------------------------------------------------------------------------
// dry_run
// ---------------------------------------------------------------------------

// DryRunInput reuses MigrateInput fields.
type DryRunInput = MigrateInput

// DryRunOutput contains the migration plan without writing data.
type DryRunOutput struct {
	Source           string                    `json:"source_provider"`
	Destination      string                    `json:"dest_provider"`
	CrossDB          bool                      `json:"cross_db"`
	SchemaMigration  bool                      `json:"schema_migration"`
	TransformerType  string                    `json:"transformer_type"`
	EstimatedRows    int64                     `json:"estimated_rows"`
	EstimatedBatches int                       `json:"estimated_batches"`
	Verification     string                    `json:"verification"`
	Tables           []bridge.TablePlan        `json:"tables,omitempty"`
	TypeMappings     []bridge.TableTypeMapping `json:"type_mappings,omitempty"`
	Unsupported      []bridge.UnsupportedField `json:"unsupported_fields,omitempty"`
	Warnings         []string                  `json:"warnings,omitempty"`
	Error            *app.StructuredError      `json:"error,omitempty"`
}

func handleDryRun(ctx context.Context, _ *mcp.CallToolRequest, input DryRunInput) (*mcp.CallToolResult, DryRunOutput, error) {
	cfg, opts, err := buildPlanRequest(input)
	if err != nil {
		return nil, DryRunOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "planning", "", "")}, nil
	}

	plan, err := (app.PlanningService{}).PlanMigration(ctx, cfg, opts)
	if err != nil {
		return nil, DryRunOutput{
			Source:      cfg.Source.Provider,
			Destination: cfg.Destination.Provider,
			Error:       app.ErrorFrom(err, "planning", "planning", cfg.Source.Provider, "source"),
		}, nil
	}

	return nil, DryRunOutput{
		Source:           plan.SourceProvider,
		Destination:      plan.DestProvider,
		CrossDB:          plan.CrossDB,
		SchemaMigration:  plan.SchemaMigration,
		TransformerType:  plan.TransformerType,
		EstimatedRows:    plan.EstimatedRows,
		EstimatedBatches: plan.EstimatedBatches,
		Verification:     plan.Verification,
		Tables:           plan.Tables,
		TypeMappings:     plan.TypeMappings,
		Unsupported:      plan.UnsupportedFields,
		Warnings:         plan.Warnings,
	}, nil
}

// ---------------------------------------------------------------------------
// list_config_templates
// ---------------------------------------------------------------------------

// ListConfigTemplatesInput has no required parameters.
type ListConfigTemplatesInput struct{}

// ListConfigTemplatesOutput returns available config template summaries.
type ListConfigTemplatesOutput struct {
	Templates []app.ConfigTemplateSummary `json:"templates"`
	Error     *app.StructuredError        `json:"error,omitempty"`
}

func handleListConfigTemplates(_ context.Context, _ *mcp.CallToolRequest, _ ListConfigTemplatesInput) (*mcp.CallToolResult, ListConfigTemplatesOutput, error) {
	templates, err := (app.ConfigService{}).ListTemplates(context.Background())
	if err != nil {
		return nil, ListConfigTemplatesOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "config_templates", "", "")}, nil
	}
	return nil, ListConfigTemplatesOutput{Templates: templates}, nil
}

// ---------------------------------------------------------------------------
// get_config_template
// ---------------------------------------------------------------------------

// GetConfigTemplateInput selects a template by filename.
type GetConfigTemplateInput struct {
	Name string `json:"name" jsonschema:"Config template filename (e.g. example.yaml, mssql-to-postgres.yaml)"`
}

// GetConfigTemplateOutput returns the full template content.
type GetConfigTemplateOutput struct {
	Template *app.ConfigTemplateDetail `json:"template"`
	Error    *app.StructuredError      `json:"error,omitempty"`
}

func handleGetConfigTemplate(_ context.Context, _ *mcp.CallToolRequest, input GetConfigTemplateInput) (*mcp.CallToolResult, GetConfigTemplateOutput, error) {
	tmpl, err := (app.ConfigService{}).GetTemplate(context.Background(), input.Name)
	if err != nil {
		return nil, GetConfigTemplateOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "config_templates", "", "")}, nil
	}
	return nil, GetConfigTemplateOutput{Template: tmpl}, nil
}

// ---------------------------------------------------------------------------
// validate_config
// ---------------------------------------------------------------------------

// ValidateConfigInput accepts an inline YAML config string.
type ValidateConfigInput struct {
	ConfigYAML string `json:"config_yaml" jsonschema:"YAML migration config string to validate"`
}

// ValidateConfigOutput returns validation results.
type ValidateConfigOutput struct {
	Result *app.ConfigValidationResult `json:"result"`
	Error  *app.StructuredError        `json:"error,omitempty"`
}

func handleValidateConfig(_ context.Context, _ *mcp.CallToolRequest, input ValidateConfigInput) (*mcp.CallToolResult, ValidateConfigOutput, error) {
	result, err := (app.ConfigService{}).ValidateConfig(context.Background(), input.ConfigYAML)
	if err != nil {
		return nil, ValidateConfigOutput{Error: app.ErrorFrom(err, string(bridge.ErrConfig), "validate_config", "", "")}, nil
	}
	return nil, ValidateConfigOutput{Result: result}, nil
}

// ---------------------------------------------------------------------------
// buildPlanRequest (shared config builder)
// ---------------------------------------------------------------------------

func buildPlanRequest(input MigrateInput) (*config.MigrationConfig, bridge.PipelineOptions, error) {
	cfg, err := buildBaseConfig(input)
	if err != nil {
		return nil, bridge.PipelineOptions{}, err
	}
	if input.CheckpointPath != "" {
		cfg.Checkpoint.Path = input.CheckpointPath
	}

	opts := bridge.DefaultPipelineOptions()
	if input.BatchSize > 0 {
		opts.BatchSize = input.BatchSize
	}
	if input.MigrateSchema != nil {
		opts.MigrateSchema = *input.MigrateSchema
	}
	if input.Verify != nil {
		opts.Verify = *input.Verify
	}
	if input.OnConflict != "" {
		opts.ConflictStrategy = provider.ConflictStrategy(input.OnConflict)
	}
	if input.FKHandling != "" {
		opts.FKHandling = input.FKHandling
	}
	if input.MaxRetries > 0 {
		opts.MaxRetries = input.MaxRetries
	}
	if input.Parallel > 0 {
		opts.Parallel = input.Parallel
	}
	if input.WriteWorkers > 0 {
		opts.WriteWorkers = input.WriteWorkers
	}
	if input.Checkpoint != nil {
		opts.CheckpointEnabled = *input.Checkpoint
	}
	if input.Resume != nil {
		opts.Resume = *input.Resume
	}
	if input.FailFast != nil {
		opts.FailFast = *input.FailFast
	}
	return cfg, opts, nil
}

// buildBaseConfig resolves the config from one of three sources (in priority
// order): config_path (file), config_yaml (inline), or structured source/
// destination fields. When a config file or YAML is used, individual fields
// (source.provider, source.url, etc.) act as overrides.
func buildBaseConfig(input MigrateInput) (*config.MigrationConfig, error) {
	switch {
	case input.ConfigPath != "":
		cfg, err := config.LoadFromFile(input.ConfigPath)
		if err != nil {
			return nil, err
		}
		overlayConnectionFields(cfg, input)
		if err := config.Resolve(cfg); err != nil {
			return nil, err
		}
		return cfg, nil

	case input.ConfigYAML != "":
		cfg, err := app.LoadConfigFromYAML(input.ConfigYAML)
		if err != nil {
			return nil, err
		}
		overlayConnectionFields(cfg, input)
		if err := config.Resolve(cfg); err != nil {
			return nil, err
		}
		return cfg, nil

	default:
		return buildMigrationConfig(input.Source, input.Destination)
	}
}

// overlayConnectionFields applies individual MCP input fields on top of a
// config loaded from file or YAML. Only non-empty fields are applied.
func overlayConnectionFields(cfg *config.MigrationConfig, input MigrateInput) {
	if input.Source.Provider != "" {
		cfg.Source.Provider = input.Source.Provider
	}
	if input.Source.URL != "" {
		cfg.Source.URL = input.Source.URL
	}
	if input.Destination.Provider != "" {
		cfg.Destination.Provider = input.Destination.Provider
	}
	if input.Destination.URL != "" {
		cfg.Destination.URL = input.Destination.URL
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// resolveConnection builds and resolves a ConnectionConfig from MCP input.
func resolveConnection(input ConnectionInput) (*config.ConnectionConfig, error) {
	connCfg := &config.ConnectionConfig{
		Provider: input.Provider,
		URL:      input.URL,
	}
	if connCfg.Provider == "" && connCfg.URL != "" {
		connCfg.Provider = providerFromURL(connCfg.URL)
	}
	if connCfg.Provider == "" {
		return nil, fmt.Errorf("provider is required (set provider or url)")
	}
	if err := connCfg.Resolve(); err != nil {
		return nil, err
	}
	return connCfg, nil
}

// buildMigrationConfig creates a full MigrationConfig from MCP tool input.
func buildMigrationConfig(source, dest ConnectionInput) (*config.MigrationConfig, error) {
	cfg := config.DefaultConfig()
	cfg.Source.Provider = source.Provider
	cfg.Source.URL = source.URL
	cfg.Destination.Provider = dest.Provider
	cfg.Destination.URL = dest.URL

	if cfg.Source.Provider == "" && cfg.Source.URL != "" {
		cfg.Source.Provider = providerFromURL(cfg.Source.URL)
	}
	if cfg.Destination.Provider == "" && cfg.Destination.URL != "" {
		cfg.Destination.Provider = providerFromURL(cfg.Destination.URL)
	}

	if err := config.Validate(cfg); err != nil {
		return nil, err
	}
	if err := config.Resolve(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// providerFromURL auto-detects the database provider from a connection URL scheme.
func providerFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	switch u.Scheme {
	case "postgres", "postgresql":
		return "postgres"
	case "mysql":
		return "mysql"
	case "mariadb":
		return "mariadb"
	case "cockroachdb", "crdb":
		return "cockroachdb"
	case "mongodb", "mongodb+srv":
		return "mongodb"
	case "mssql", "sqlserver":
		return "mssql"
	case "sqlite":
		return "sqlite"
	case "redis", "rediss":
		return "redis"
	default:
		return ""
	}
}
