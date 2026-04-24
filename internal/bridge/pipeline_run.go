package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/internal/retry"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/pkg/provider"
)

// Run executes the full migration pipeline in 8 visible phases.
// Internally, the "Transferring data" phase decomposes into three
// concurrent substeps (extract, transform, write) via a producer-consumer pipeline.
func (p *Pipeline) Run(ctx context.Context) (*RunResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancelFn = cancel
	defer cancel()

	result := &RunResult{
		Config:      p.config,
		SrcProvider: p.config.Source.Provider,
		DstProvider: p.config.Destination.Provider,
		Failures:    NewFailureSummary(),
	}

	ms := &migrationState{
		summary: &provider.MigrationSummary{
			StartTime: time.Now(),
		},
	}
	p.startTime = ms.summary.StartTime

	if err := p.stepLoadConfig(result); err != nil {
		return nil, err
	}
	logger.L().Debug("step completed", "step", 1, "phase", "init", "duration", result.Phases[len(result.Phases)-1].Duration)

	if err := p.stepValidate(ctx, result); err != nil {
		return nil, err
	}
	logger.L().Debug("step completed", "step", 2, "phase", "tunnel", "duration", result.Phases[len(result.Phases)-1].Duration)
	defer func() { _ = p.tunnels.CloseAll() }()

	if err := p.stepInitProviders(ctx, result); err != nil {
		return nil, err
	}
	logger.L().Debug("step completed", "step", 3, "phase", "connect", "duration", result.Phases[len(result.Phases)-1].Duration)

	defer closeProviderWithTimeout(p.src)
	defer closeProviderWithTimeout(p.dst)

	if err := p.stepInspect(ctx, result, ms); err != nil {
		return nil, err
	}
	if err := p.stepPlan(ctx, result, ms); err != nil {
		return nil, err
	}
	if err := p.stepTransfer(ctx, result, ms); err != nil {
		return nil, err
	}

	logger.L().Debug("step completed", "step", 8, "phase", "transfer", "duration", result.Phases[len(result.Phases)-1].Duration)

	p.stepVerify(ctx, result, ms)
	p.stepFinalize(ctx, result, ms)

	return result, nil
}

// Plan executes the pipeline only through the planning phase and returns the
// structured MigrationPlan without transferring or verifying data.
func (p *Pipeline) Plan(ctx context.Context) (*RunResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancelFn = cancel
	defer cancel()

	result := &RunResult{
		Config:      p.config,
		SrcProvider: p.config.Source.Provider,
		DstProvider: p.config.Destination.Provider,
		Failures:    NewFailureSummary(),
	}

	ms := &migrationState{
		summary: &provider.MigrationSummary{
			StartTime: time.Now(),
		},
	}
	p.startTime = ms.summary.StartTime

	if err := p.stepLoadConfig(result); err != nil {
		return nil, err
	}
	if err := p.stepValidate(ctx, result); err != nil {
		return nil, err
	}
	defer func() { _ = p.tunnels.CloseAll() }()

	if err := p.stepInitProviders(ctx, result); err != nil {
		return nil, err
	}
	defer closeProviderWithTimeout(p.src)
	defer closeProviderWithTimeout(p.dst)

	if err := p.stepInspect(ctx, result, ms); err != nil {
		return nil, err
	}
	if err := p.stepPlan(ctx, result, ms); err != nil {
		return nil, err
	}

	return result, nil
}

// Step 1: Load config — validates pipeline options and logs migration parameters.
func (p *Pipeline) stepLoadConfig(result *RunResult) error {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhaseInit)
	p.reporter.OnPhaseStart(desc)

	if err := p.opts.Validate(); err != nil {
		p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
		return NewConfigError(1, "pipeline options", err)
	}

	logger.L().With("component", "pipeline").Debug("migration starting",
		"source", p.config.Source.Provider,
		"destination", p.config.Destination.Provider,
		"cross_db", p.config.IsCrossDB(),
		"dry_run", p.opts.DryRun,
	)

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)
	result.Phases = append(result.Phases, PhaseResult{Phase: provider.PhaseInit, Duration: dur})
	return nil
}

// Step 2: Validate source and destination — establishes SSH tunnels.
func (p *Pipeline) stepValidate(ctx context.Context, result *RunResult) error {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhaseTunnel)
	p.reporter.OnPhaseStart(desc)

	tunnelConfigs := p.buildTunnelConfigs()
	tunnelRetry := retry.ConnectionRetryConfig()
	if err := retry.Do(ctx, tunnelRetry, func() error {
		return p.tunnels.OpenAll(ctx, tunnelConfigs)
	}); err != nil {
		p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
		return p.abort(NewConnectionError("tunnel", "Failed to establish SSH tunnel to source or destination database", err).WithStep(2))
	}

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)
	result.Phases = append(result.Phases, PhaseResult{Phase: provider.PhaseTunnel, Duration: dur})
	return nil
}

// Step 3: Initialize providers — creates, connects, and pings source and
// destination. Resolves capabilities and sets up the transformer.
func (p *Pipeline) stepInitProviders(ctx context.Context, result *RunResult) error {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhaseConnect)
	p.reporter.OnPhaseStart(desc)
	log := p.stepLog(3, "connect")

	srcProvider, err := provider.New(p.config.Source.Provider)
	if err != nil {
		return p.abort(NewConnectionError("connect", fmt.Sprintf("Failed to create source provider %q — check that the provider name is correct", p.config.Source.Provider), err).WithStep(3))
	}
	p.src = srcProvider

	dstProvider, err := provider.New(p.config.Destination.Provider)
	if err != nil {
		return p.abort(NewConnectionError("connect", fmt.Sprintf("Failed to create destination provider %q — check that the provider name is correct", p.config.Destination.Provider), err).WithStep(3))
	}
	p.dst = dstProvider

	srcConfig, err := p.resolveProviderConfig("source", &p.config.Source)
	if err != nil {
		return p.abort(NewConnectionError("connect", "Failed to resolve source provider configuration", err).WithStep(3))
	}
	dstConfig, err := p.resolveProviderConfig("destination", &p.config.Destination)
	if err != nil {
		return p.abort(NewConnectionError("connect", "Failed to resolve destination provider configuration", err).WithStep(3))
	}

	connectRetry := retry.ConnectionRetryConfig()
	if err := retry.Do(ctx, connectRetry, func() error {
		return p.src.Connect(ctx, srcConfig, nil)
	}); err != nil {
		return p.abort(NewConnectionError("connect", fmt.Sprintf("Could not connect to source %q — check host, port, credentials, and network", p.config.Source.Provider), err).WithStep(3))
	}
	if err := retry.Do(ctx, connectRetry, func() error {
		return p.dst.Connect(ctx, nil, dstConfig)
	}); err != nil {
		return p.abort(NewConnectionError("connect", fmt.Sprintf("Could not connect to destination %q — check host, port, credentials, and network", p.config.Destination.Provider), err).WithStep(3))
	}
	if err := retry.Do(ctx, connectRetry, func() error {
		return p.src.Ping(ctx)
	}); err != nil {
		return p.abort(NewConnectionError("connect", fmt.Sprintf("Source %q is unreachable after connecting — it may have gone down", p.config.Source.Provider), err).WithStep(3))
	}
	if err := retry.Do(ctx, connectRetry, func() error {
		return p.dst.Ping(ctx)
	}); err != nil {
		return p.abort(NewConnectionError("connect", fmt.Sprintf("Destination %q is unreachable after connecting — it may have gone down", p.config.Destination.Provider), err).WithStep(3))
	}

	log.Debug("connected to both databases")

	p.srcCaps = provider.ProviderCapabilities(p.src)
	p.dstCaps = provider.ProviderCapabilities(p.dst)
	log.Debug("capabilities resolved", "source", p.srcCaps, "destination", p.dstCaps)

	if p.opts.DryRun {
		p.dst = p.dst.DryRun()
		log.Debug("dry-run mode enabled")
	}

	p.transformer = transform.GetTransformer(
		p.config.Source.Provider,
		p.config.Destination.Provider,
		p.buildTransformerConfig(),
	)
	if p.transformer.NeedsSchema() {
		log.Debug("transformer requires schema information")
	}

	if err := p.runPreflight(ctx); err != nil {
		p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
		return p.abort(err)
	}

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)
	result.Phases = append(result.Phases, PhaseResult{Phase: provider.PhaseConnect, Duration: dur})
	return nil
}

// Step 5: Plan migration — handles resume logic, builds a structured migration
// plan, restores dedup state, and prepares the batch start ID.
func (p *Pipeline) stepPlan(ctx context.Context, result *RunResult, ms *migrationState) error {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhasePlanning)
	p.reporter.OnPhaseStart(desc)
	log := p.stepLog(5, "plan")

	if ms.checkpoint != nil && p.opts.Resume {
		currentHash := computeConfigHash(p.config)
		if ms.checkpoint.ConfigHash != "" && ms.checkpoint.ConfigHash != currentHash {
			return NewConfigError(5, fmt.Sprintf("config has changed since the checkpoint was created. "+
				"Remove the checkpoint file or use the same configuration to resume. "+
				"Checkpoint hash: %s, current hash: %s", ms.checkpoint.ConfigHash[:8], currentHash[:8]), nil)
		}
		if ms.checkpoint.SourceProvider != p.config.Source.Provider {
			return NewConfigError(5, fmt.Sprintf("checkpoint source provider %q does not match current %q",
				ms.checkpoint.SourceProvider, p.config.Source.Provider), nil)
		}
		if ms.checkpoint.DestProvider != p.config.Destination.Provider {
			return NewConfigError(5, fmt.Sprintf("checkpoint destination provider %q does not match current %q",
				ms.checkpoint.DestProvider, p.config.Destination.Provider), nil)
		}
		if len(ms.checkpoint.WrittenKeys) > 0 {
			for _, k := range ms.checkpoint.WrittenKeys {
				p.writtenKeys.Store(k, true)
			}
			ringSize := keyRingSizeFor(len(ms.checkpoint.WrittenKeys), p.opts.MaxWrittenKeys)
			ring := make([]string, ringSize)
			copy(ring, ms.checkpoint.WrittenKeys)
			p.keyRing = ring
			p.keyRingLen = len(ms.checkpoint.WrittenKeys)
			p.keyRingHead = len(ms.checkpoint.WrittenKeys) % ringSize
			p.totalWritten = ms.checkpoint.TotalWritten
		}
		result.Resumed = true

		if ms.checkpoint.TotalWritten > int64(p.opts.MaxWrittenKeys) {
			if p.opts.ConflictStrategy != provider.ConflictOverwrite {
				return NewConfigError(5, fmt.Sprintf("cannot resume: %d keys were written but only %d tracked for dedup "+
					"with conflict strategy %q. Either increase --max-written-keys, switch to --on-conflict overwrite, "+
					"or remove the checkpoint to start fresh",
					ms.checkpoint.TotalWritten, p.opts.MaxWrittenKeys, p.opts.ConflictStrategy), nil)
			}
			log.Warn("resume dedup is incomplete: more keys were written than the tracking cap allows. "+
				"Evicted keys will be re-written (harmless with overwrite strategy)",
				"total_written", ms.checkpoint.TotalWritten,
				"max_tracked_keys", p.opts.MaxWrittenKeys,
			)
		}

		log.Debug("resuming from checkpoint",
			"last_batch", ms.checkpoint.LastBatchID,
			"total_written", ms.checkpoint.TotalWritten,
			"tables_completed", len(ms.checkpoint.TablesCompleted),
			"last_table_scanning", ms.checkpoint.LastTableScanning,
			"restored_keys", len(ms.checkpoint.WrittenKeys),
			"timestamp", ms.checkpoint.Timestamp,
		)
	} else if ms.checkpoint != nil {
		log.Debug("checkpoint found but --resume not set, starting fresh")
		_ = p.checkpoint.Clear(ctx)
		ms.checkpoint = nil
	}

	ms.startBatchID = lastBatchID(ms.checkpoint)
	result.Plan = p.buildPlan(ctx)
	logPlan(result.Plan)

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)
	result.Phases = append(result.Phases, PhaseResult{Phase: provider.PhasePlanning, Duration: dur})
	return nil
}

func closeProviderWithTimeout(p provider.Provider) {
	done := make(chan struct{})
	go func() {
		_ = p.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
}
