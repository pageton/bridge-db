package cli

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/pageton/bridge-db/internal/app"
	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/progress"
	verifypkg "github.com/pageton/bridge-db/internal/verify"
	"github.com/pageton/bridge-db/pkg/provider"
	"github.com/spf13/cobra"
)

func runMigrate(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	warnPasswordFlags(cmd)

	cfg, opts, err := normalizeMigrateInputs()
	if err != nil {
		return err
	}

	return executeMigrate(ctx, cfg, opts)
}

func normalizeMigrateInputs() (*config.MigrationConfig, bridge.PipelineOptions, error) {
	cfg, err := buildConfigFromFlags()
	if err != nil {
		return nil, bridge.PipelineOptions{}, fmt.Errorf("invalid configuration: %w", err)
	}

	initLogger(cfg.Logging.Level, cfg.Logging.JSON)

	if err := preflightValidate(cfg); err != nil {
		return nil, bridge.PipelineOptions{}, err
	}
	if err := config.Validate(cfg); err != nil {
		return nil, bridge.PipelineOptions{}, fmt.Errorf("validation failed: %w", err)
	}
	if err := config.Resolve(cfg); err != nil {
		return nil, bridge.PipelineOptions{}, fmt.Errorf("config resolution failed: %w", err)
	}

	return cfg, buildMigrateOptions(), nil
}

func buildMigrateOptions() bridge.PipelineOptions {
	opts := bridge.DefaultPipelineOptions()
	opts.BatchSize = batchSize
	opts.DryRun = dryRun
	opts.Verify = verify
	opts.MigrateSchema = migrateSchema
	opts.MaxRetries = maxRetries
	opts.MaxPerUnitRetry = maxPerUnitRetry
	opts.FailFast = failFast
	opts.ConflictStrategy = provider.ConflictStrategy(conflictStrategy)
	opts.FKHandling = fkHandling
	opts.CheckpointEnabled = checkpoint
	opts.Resume = resume
	opts.Parallel = parallel
	opts.WriteWorkers = writeWorkers
	opts.MaxBatchBytes = maxBatchBytes
	opts.MaxWrittenKeys = maxWrittenKeys
	opts.CheckpointInterval = checkpointInterval
	opts.VerifyOptions = verifypkg.Options{
		CountComparison:    true,
		SampleMode:         verifySampleMode,
		SamplePct:          verifySamplePct,
		SampleN:            verifySampleN,
		ChecksumComparison: !verifyNoChecksum,
		MaxSampleKeys:      verifyMaxKeys,
		Seed:               42,
	}
	return opts
}

func executeMigrate(ctx context.Context, cfg *config.MigrationConfig, opts bridge.PipelineOptions) error {
	if opts.DryRun {
		plan, err := (app.PlanningService{}).PlanMigration(ctx, cfg, opts)
		if err != nil {
			return fmt.Errorf("dry-run planning failed: %w", err)
		}
		printDryRunPreview(cfg, plan)
		return nil
	}

	printStartupContext(cfg, opts)

	svc := app.NewMigrationService(app.NewRunStatusService())
	reporter := progress.NewConsoleReporter()
	result, _, err := svc.RunSync(ctx, cfg, opts, reporter)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	printSummary(result)
	return nil
}
