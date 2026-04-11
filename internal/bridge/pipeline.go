package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/internal/progress"
	"github.com/pageton/bridge-db/internal/retry"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/internal/tunnel"
	"github.com/pageton/bridge-db/internal/verify"
	"github.com/pageton/bridge-db/pkg/provider"
)

// scanResult is a batch of migration units produced by the scanner goroutine.
type scanResult struct {
	batchID int
	units   []provider.MigrationUnit
	err     error
}

// migrationState carries per-run state that accumulates across pipeline steps.
type migrationState struct {
	summary      *provider.MigrationSummary
	checkpoint   *Checkpoint
	startBatchID int
	allErrors    []error
}

// Pipeline orchestrates a full migration from source to destination.
// It manages tunnel lifecycle, provider connections, scanning, transformation,
// writing, verification, checkpointing, and progress reporting.
type Pipeline struct {
	config     *config.MigrationConfig
	opts       PipelineOptions
	reporter   provider.ProgressReporter
	metrics    *progress.MetricsCollector
	checkpoint CheckpointStore
	tunnels    *tunnel.Pool

	src         provider.Provider
	dst         provider.Provider
	transformer transform.Transformer
	srcCaps     provider.Capabilities
	dstCaps     provider.Capabilities

	keyRing       []string
	keyRingHead   int
	keyRingLen    int
	writtenKeys   sync.Map // map[string]bool — lock-free dedup checks
	totalWritten  int64
	startTime     time.Time
	scannedTables []string
	tableSet      map[string]bool
	keysEvicted   bool // true when key dedup set exceeded MaxWrittenKeys

	// keyMu protects keyRing for concurrent ring buffer access.
	keyMu sync.Mutex
	// cpMu serialises checkpoint writes across concurrent workers.
	cpMu           sync.Mutex
	batchesSinceCP int

	pauseCond *sync.Cond
	paused    atomic.Bool
	cancelFn  context.CancelFunc
}

// NewPipeline creates a new pipeline from the given configuration.
// The caller must invoke Run() to start the migration.
func NewPipeline(cfg *config.MigrationConfig, opts PipelineOptions, reporter provider.ProgressReporter, cpStore CheckpointStore) (*Pipeline, error) {
	if reporter == nil {
		reporter = progress.NopReporter{}
	}
	if cpStore == nil {
		var err error
		cpStore, err = NewFileCheckpointStore(cfg.Checkpoint.Path)
		if err != nil {
			return nil, fmt.Errorf("create checkpoint store: %w", err)
		}
	}
	p := &Pipeline{
		config:     cfg,
		opts:       opts,
		reporter:   reporter,
		metrics:    progress.NewMetricsCollector(),
		checkpoint: cpStore,
		tunnels:    tunnel.NewPool(),
		keyRing:    make([]string, 1024),
		tableSet:   make(map[string]bool),
	}
	p.pauseCond = sync.NewCond(&sync.Mutex{})
	return p, nil
}

// stepLog returns a logger pre-loaded with step and phase context for structured
// logging. Every pipeline step should use this instead of creating ad-hoc loggers.
func (p *Pipeline) stepLog(step int, phase string) *slog.Logger {
	return logger.L().With("component", "pipeline", "step", step, "phase", phase)
}

// visiblePhases defines the ordered set of phases shown to the user.
// The step number and total are derived from this slice's position and length.
var visiblePhases = []provider.PhaseDesc{
	{Phase: provider.PhaseInit, Description: "Validating config"},
	{Phase: provider.PhaseTunnel, Description: "Validating connections"},
	{Phase: provider.PhaseConnect, Description: "Connecting to databases"},
	{Phase: provider.PhaseSchemaMigration, Description: "Inspecting schema"},
	{Phase: provider.PhasePlanning, Description: "Building migration plan"},
	{Phase: provider.PhaseScanning, Description: "Transferring data"},
	{Phase: provider.PhaseVerifying, Description: "Verifying data"},
	{Phase: provider.PhaseFinalizing, Description: "Cleaning up"},
}

// phaseDesc returns a numbered PhaseDesc for a given MigrationPhase.
func phaseDesc(phase provider.MigrationPhase) provider.PhaseDesc {
	for i, vp := range visiblePhases {
		if vp.Phase == phase {
			return provider.PhaseDesc{
				Phase:       phase,
				Step:        i + 1,
				TotalSteps:  len(visiblePhases),
				Description: vp.Description,
			}
		}
	}
	return provider.PhaseDesc{Phase: phase, Step: 0, TotalSteps: len(visiblePhases), Description: string(phase)}
}

// Run executes the full migration pipeline in 8 visible phases.
// Internally, the "Transferring data" phase decomposes into three
// concurrent substeps (extract, transform, write) via a producer-consumer pipeline.
//
// User-visible phases:
//
//  1. Validating config
//  2. Validating connections (SSH tunnels)
//  3. Connecting to databases
//  4. Inspecting schema
//  5. Building migration plan
//  6. Transferring data  (extract → transform → write, concurrent)
//  7. Verifying data
//  8. Cleaning up
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

	// Step 1: Load config — validate pipeline options.
	if err := p.stepLoadConfig(result); err != nil {
		return nil, err
	}

	logger.L().Debug("step completed", "step", 1, "phase", "init", "duration", result.Phases[len(result.Phases)-1].Duration)

	// Step 2: Validate source and destination — establish SSH tunnels.
	if err := p.stepValidate(ctx, result); err != nil {
		return nil, err
	}
	logger.L().Debug("step completed", "step", 2, "phase", "tunnel", "duration", result.Phases[len(result.Phases)-1].Duration)
	defer func() { _ = p.tunnels.CloseAll() }()

	// Step 3: Initialize providers — connect to source and destination.
	if err := p.stepInitProviders(ctx, result); err != nil {
		return nil, err
	}
	logger.L().Debug("step completed", "step", 3, "phase", "connect", "duration", result.Phases[len(result.Phases)-1].Duration)

	defer func() {
		done := make(chan struct{})
		go func() { _ = p.src.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}()
	defer func() {
		done := make(chan struct{})
		go func() { _ = p.dst.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}()

	// Step 4: Inspect schema/metadata — load checkpoint, inspect and migrate schema.
	if err := p.stepInspect(ctx, result, ms); err != nil {
		return nil, err
	}

	// Step 5: Plan migration — handle resume, prepare scan/write options.
	if err := p.stepPlan(ctx, result, ms); err != nil {
		return nil, err
	}

	// Steps 6–8: Extract, transform, write — concurrent data transfer pipeline.
	if err := p.stepTransfer(ctx, result, ms); err != nil {
		return nil, err
	}

	logger.L().Debug("step completed", "step", 8, "phase", "transfer", "duration", result.Phases[len(result.Phases)-1].Duration)

	// Step 9: Verify results — compare source and destination data.
	p.stepVerify(ctx, result, ms)

	// Step 10: Finalize — build summary, clear checkpoint, report completion.
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
	defer func() {
		done := make(chan struct{})
		go func() { _ = p.src.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}()
	defer func() {
		done := make(chan struct{})
		go func() { _ = p.dst.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}()

	if err := p.stepInspect(ctx, result, ms); err != nil {
		return nil, err
	}
	if err := p.stepPlan(ctx, result, ms); err != nil {
		return nil, err
	}

	return result, nil
}

// ---------------------------------------------------------------------------
// Pipeline step methods
// ---------------------------------------------------------------------------

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

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseInit,
		Duration: dur,
	})
	return nil
}

// Step 2: Validate source and destination — establishes SSH tunnels.
func (p *Pipeline) stepValidate(ctx context.Context, result *RunResult) error {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhaseTunnel)
	p.reporter.OnPhaseStart(desc)

	tunnelConfigs := p.buildTunnelConfigs()

	// Retry tunnel open to tolerate transient network issues.
	tunnelRetry := retry.ConnectionRetryConfig()
	if err := retry.Do(ctx, tunnelRetry, func() error {
		return p.tunnels.OpenAll(ctx, tunnelConfigs)
	}); err != nil {
		p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
		return p.abort(NewConnectionError("tunnel", "Failed to establish SSH tunnel to source or destination database", err).WithStep(2))
	}

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseTunnel,
		Duration: dur,
	})
	return nil
}

// Step 3: Initialize providers — creates, connects, and pings source and
// destination. Resolves capabilities and sets up the transformer.
func (p *Pipeline) stepInitProviders(ctx context.Context, result *RunResult) error {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhaseConnect)
	p.reporter.OnPhaseStart(desc)
	log := p.stepLog(3, "connect")

	// Create provider instances.
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

	// Resolve tunnel addresses.
	srcConfig, err := p.resolveProviderConfig("source", &p.config.Source)
	if err != nil {
		return p.abort(NewConnectionError("connect", "Failed to resolve source provider configuration", err).WithStep(3))
	}
	dstConfig, err := p.resolveProviderConfig("destination", &p.config.Destination)
	if err != nil {
		return p.abort(NewConnectionError("connect", "Failed to resolve destination provider configuration", err).WithStep(3))
	}

	connectRetry := retry.ConnectionRetryConfig()

	// Connect to source.
	if err := retry.Do(ctx, connectRetry, func() error {
		return p.src.Connect(ctx, srcConfig, nil)
	}); err != nil {
		return p.abort(NewConnectionError("connect", fmt.Sprintf("Could not connect to source %q — check host, port, credentials, and network", p.config.Source.Provider), err).WithStep(3))
	}

	// Connect to destination.
	if err := retry.Do(ctx, connectRetry, func() error {
		return p.dst.Connect(ctx, nil, dstConfig)
	}); err != nil {
		return p.abort(NewConnectionError("connect", fmt.Sprintf("Could not connect to destination %q — check host, port, credentials, and network", p.config.Destination.Provider), err).WithStep(3))
	}

	// Ping both databases.
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

	// Resolve capabilities.
	p.srcCaps = provider.ProviderCapabilities(p.src)
	p.dstCaps = provider.ProviderCapabilities(p.dst)
	log.Debug("capabilities resolved",
		"source", p.srcCaps,
		"destination", p.dstCaps,
	)

	// Wrap destination for dry-run mode.
	if p.opts.DryRun {
		p.dst = p.dst.DryRun()
		log.Debug("dry-run mode enabled")
	}

	// Resolve transformer.
	transform.SetGlobalConfig(p.buildTransformerConfig())
	p.transformer = transform.GetTransformer(
		p.config.Source.Provider,
		p.config.Destination.Provider,
	)
	if p.transformer.NeedsSchema() {
		log.Debug("transformer requires schema information")
	}

	// Run pre-migration validation before any data movement.
	if err := p.runPreflight(ctx); err != nil {
		p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
		return p.abort(err)
	}

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseConnect,
		Duration: dur,
	})
	return nil
}

// Step 4: Inspect schema/metadata — loads checkpoint, inspects source schema,
// and migrates it to the destination if supported.
func (p *Pipeline) stepInspect(ctx context.Context, result *RunResult, ms *migrationState) error {
	log := p.stepLog(4, "inspect")

	// Load checkpoint.
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

	// Schema migration — always report the phase to the user.
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

		result.Phases = append(result.Phases, PhaseResult{
			Phase:    provider.PhaseSchemaMigration,
			Duration: dur,
		})
	} else {
		p.reporter.OnPhaseStart(desc)
		p.reporter.OnPhaseDone(desc, 0, provider.ErrPhaseSkipped)
	}

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
		// Validate config hasn't changed between runs.
		currentHash := computeConfigHash(p.config)
		if ms.checkpoint.ConfigHash != "" && ms.checkpoint.ConfigHash != currentHash {
			return NewConfigError(5, fmt.Sprintf("config has changed since the checkpoint was created. "+
				"Remove the checkpoint file or use the same configuration to resume. "+
				"Checkpoint hash: %s, current hash: %s", ms.checkpoint.ConfigHash[:8], currentHash[:8]), nil)
		}
		// Validate providers match.
		if ms.checkpoint.SourceProvider != p.config.Source.Provider {
			return NewConfigError(5, fmt.Sprintf("checkpoint source provider %q does not match current %q",
				ms.checkpoint.SourceProvider, p.config.Source.Provider), nil)
		}
		if ms.checkpoint.DestProvider != p.config.Destination.Provider {
			return NewConfigError(5, fmt.Sprintf("checkpoint destination provider %q does not match current %q",
				ms.checkpoint.DestProvider, p.config.Destination.Provider), nil)
		}
		// Restore written keys for dedup.  The ring buffer must be
		// large enough to hold all restored keys to prevent immediate
		// eviction by recordKeys.
		if len(ms.checkpoint.WrittenKeys) > 0 {
			for _, k := range ms.checkpoint.WrittenKeys {
				p.writtenKeys.Store(k, true)
			}
			ringSize := len(ms.checkpoint.WrittenKeys)
			if ringSize < p.opts.MaxWrittenKeys {
				ringSize = p.opts.MaxWrittenKeys
			}
			ring := make([]string, ringSize)
			copy(ring, ms.checkpoint.WrittenKeys)
			p.keyRing = ring
			p.keyRingLen = len(ms.checkpoint.WrittenKeys)
			p.keyRingHead = len(ms.checkpoint.WrittenKeys) % ringSize
			p.totalWritten = ms.checkpoint.TotalWritten
		}
		result.Resumed = true

		// Warn if restored keys hit the cap - older keys were evicted,
		// so resume dedup is incomplete. For non-overwrite strategies,
		// this would cause duplicate-key errors on re-write.
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

	// Build the structured migration plan.
	plan := p.buildPlan(ctx)
	result.Plan = plan
	logPlan(plan)

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhasePlanning,
		Duration: dur,
	})
	return nil
}

// Step 6–8: Transfer data — runs the concurrent data transfer pipeline.
//
// The scanner goroutine reads from the source (step 6: extract), transforms
// inline (step 7: transform), and produces into a buffered channel. N writer
// goroutines consume batches and write to the destination (step 8: write).
//
// TODO: The single scanner goroutine is the throughput bottleneck when source
// reads are slow relative to writes. Write workers spend time idle waiting for
// the channel. A future improvement would support parallel scanners (one per
// table or range-partitioned within a table).
func (p *Pipeline) stepTransfer(ctx context.Context, result *RunResult, ms *migrationState) error {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhaseScanning)
	p.reporter.OnPhaseStart(desc)
	log := p.stepLog(8, "transfer")

	scanOpts := provider.ScanOptions{
		BatchSize:       p.opts.BatchSize,
		ResumeToken:     resumeToken(ms.checkpoint),
		TablesCompleted: tablesCompleted(ms.checkpoint),
	}
	scanner := p.src.Scanner(ctx, scanOpts)

	writeOpts := provider.WriteOptions{
		BatchSize:  p.opts.BatchSize,
		OnConflict: p.opts.ConflictStrategy,
	}

	batchID := ms.startBatchID
	var allErrors []error
	const maxErrorsToTrack = 1000
	var errorsMu sync.Mutex
	var lastWrittenBatchID atomic.Int64

	scanBuffer := p.opts.Parallel
	scanCh := make(chan scanResult, scanBuffer)

	// Step 6–7: Scanner goroutine — extracts batches from source and
	// transforms them inline before producing into the channel.
	var scanWg sync.WaitGroup
	scanWg.Add(1)
	go func() {
		defer scanWg.Done()
		defer close(scanCh)
		defer func() { _ = scanner.Close() }()

		for {
			if err := ctx.Err(); err != nil {
				scanCh <- scanResult{err: err}
				return
			}

			// Step 6: Extract — read next batch from source.
			var units []provider.MigrationUnit
			units, err := scanner.Next(ctx)
			if err == io.EOF {
				return
			}
			if err != nil {
				scanRetryErr := retry.Do(ctx, retry.Config{
					MaxAttempts:     p.opts.MaxRetries + 1,
					InitialInterval: p.opts.RetryBackoff,
					MaxInterval:     10 * time.Second,
					Multiplier:      2.0,
				}, func() error {
					units, err = scanner.Next(ctx)
					if err == io.EOF {
						return nil
					}
					return err
				})
				if scanRetryErr != nil {
					select {
					case scanCh <- scanResult{err: scanRetryErr}:
					case <-ctx.Done():
					}
					continue
				}
				if err == io.EOF {
					return
				}
			}

			if len(units) == 0 {
				continue
			}

			// Step 7: Transform — apply data transformation.
			if !transform.IsNoopTransformer(p.transformer) {
				transformErr := retry.Do(ctx, retry.Config{
					MaxAttempts:     p.opts.MaxRetries + 1,
					InitialInterval: p.opts.RetryBackoff,
					MaxInterval:     10 * time.Second,
					Multiplier:      2.0,
					Operation:       "transform batch",
				}, func() error {
					var terr error
					units, terr = p.transformer.Transform(ctx, units)
					return terr
				})
				if transformErr != nil {
					p.reporter.OnError(transformErr, nil)
					p.metrics.RecordError()
					errorsMu.Lock()
					appendError(&allErrors, maxErrorsToTrack, NewTransformError(batchID+1, transformErr))
					errorsMu.Unlock()

					if p.opts.FailFast {
						log.Error("transform error after retries, aborting (fail-fast enabled)",
							"error", transformErr,
							"batch", batchID+1,
						)
						scanCh <- scanResult{err: transformErr}
						return
					}
					log.Warn("transform error after retries, skipping batch",
						"error", transformErr,
						"batch", batchID+1,
					)
					continue
				}
			}

			// Split into sub-batches when MaxBatchBytes is set and exceeded.
			for _, sub := range splitBatch(units, p.opts.MaxBatchBytes) {
				batchID++
				p.metrics.RecordScan(int64(len(sub)), totalSize(sub))
				p.reporter.OnBatchStart(batchID, len(sub))
				select {
				case scanCh <- scanResult{batchID: batchID, units: sub}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Create one batchWriter per worker so that concurrent writes are safe.
	numWorkers := p.opts.WriteWorkers
	batchWriters := make([]*batchWriter, numWorkers)
	for i := range batchWriters {
		batchWriters[i] = &batchWriter{
			w: p.dst.Writer(ctx, writeOpts),
			cfg: writeConfig{
				MaxRetries:       p.opts.MaxRetries,
				RetryBackoff:     p.opts.RetryBackoff,
				ConflictStrategy: p.opts.ConflictStrategy,
				MaxPerUnitRetry:  min(p.opts.MaxRetries+1, 50),
			},
		}
	}

	// Step 8: Write — writer goroutines consume batches and persist to destination.
	var writeWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		writeWg.Add(1)
		go func(bw *batchWriter) {
			defer writeWg.Done()
			for sr := range scanCh {
				// Respect pause.
				p.waitIfPaused()

				if err := ctx.Err(); err != nil {
					return
				}

				// Scan errors from the producer.
				if sr.err != nil {
					p.reporter.OnError(sr.err, nil)
					p.metrics.RecordError()
					errorsMu.Lock()
					appendError(&allErrors, maxErrorsToTrack, NewScanError("scan failed", sr.err))
					errorsMu.Unlock()
					log.Warn("scan error, skipping batch",
						"error", sr.err,
						"batch", sr.batchID,
					)
					continue
				}

				bw.batchID = sr.batchID
				out := bw.writeBatch(ctx, p, sr.units)
				processWriteOutcome(out, sr.batchID, sr.units, p, p.reporter, p.metrics, &allErrors, &errorsMu, maxErrorsToTrack)

				if out.writeErr == nil && out.result != nil {
					lastWrittenBatchID.Store(int64(sr.batchID))

					// Checkpoint (throttled by interval).
					p.maybeCheckpoint(ctx, sr.batchID, scanner)
				}

				p.reporter.OnProgress(p.metrics.Snapshot(provider.PhaseWriting))
			}
		}(batchWriters[i])
	}

	// Wait for scanner to finish producing (scanner goroutine closes scanCh via defer).
	scanWg.Wait()

	// Wait for all writer workers to drain.
	writeWg.Wait()

	// Flush all writer instances.
	for _, bw := range batchWriters {
		if err := bw.flush(ctx); err != nil {
			p.saveCheckpoint(context.Background(), int(lastWrittenBatchID.Load()), scanner)
			p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
			return p.abort(NewWriteErrorExhausted(0, err).WithStep(8))
		}
	}

	// Final checkpoint on cancellation for resumability.
	if err := ctx.Err(); err != nil {
		p.saveCheckpoint(context.Background(), int(lastWrittenBatchID.Load()), scanner)
		p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
		return p.abort(NewCancelledError("Migration was cancelled", err).WithStep(8))
	}

	ms.allErrors = allErrors

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseWriting,
		Duration: dur,
	})
	return nil
}

// Step 9: Verify results — compares source and destination data using
// capability-aware verification.
func (p *Pipeline) stepVerify(ctx context.Context, result *RunResult, ms *migrationState) {
	desc := phaseDesc(provider.PhaseVerifying)

	if !p.opts.Verify {
		p.reporter.OnPhaseStart(desc)
		p.reporter.OnPhaseDone(desc, 0, provider.ErrPhaseSkipped)
		return
	}

	phaseStart := time.Now()
	p.reporter.OnPhaseStart(desc)
	log := p.stepLog(9, "verify")

	effectiveLevel := provider.EffectiveVerifyLevel(p.srcCaps, p.dstCaps)
	log.Debug("effective verification level", "level", effectiveLevel)

	// Use user-provided verification options, falling back to defaults.
	verifyOpts := p.opts.VerifyOptions
	if verifyOpts.SampleMode == "" {
		verifyOpts = verify.DefaultOptions()
	}

	switch effectiveLevel {
	case provider.VerifyCross:
		cv := verify.NewCrossVerifier(p.src, p.dst, verifyOpts)
		report, err := cv.Verify(ctx)
		if err != nil {
			log.Warn("verification error", "error", err)
		} else {
			result.VerificationReport = report
			ms.summary.VerificationOK = report.Passed()
			ms.summary.VerificationErrs = verify.ToVerificationErrors(report)
			log.Debug("verification complete",
				"status", report.Status,
				"tables", report.TotalTables,
				"passed", report.PassCount,
				"failed", report.FailCount,
				"warned", report.WarnCount,
				"sampled", report.TotalSampled,
				"duration", report.Duration,
			)
		}

	case provider.VerifyBasic:
		verifier := p.dst.Verifier(ctx)
		verifierErrors, err := verifier.Verify(ctx, p.writtenKeysFlat())
		if err != nil {
			log.Warn("verification error", "error", err)
			ms.summary.VerificationOK = false
			ms.summary.VerificationErrs = []provider.VerificationError{{
				Message: fmt.Sprintf("verification failed: %v", err),
			}}
		} else if len(verifierErrors) > 0 {
			ms.summary.VerificationOK = false
			ms.summary.VerificationErrs = verifierErrors
			log.Warn("verification found mismatches", "count", len(verifierErrors))
		} else {
			ms.summary.VerificationOK = true
			log.Debug("verification passed")
		}

	default:
		log.Warn("verification skipped — neither source nor destination supports verification",
			"source_level", p.srcCaps.Verification,
			"destination_level", p.dstCaps.Verification,
		)
		ms.summary.VerificationOK = false
		ms.summary.VerificationErrs = []provider.VerificationError{{
			Message: "verification not supported by source or destination provider",
		}}
	}

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseVerifying,
		Duration: dur,
	})
}

// Step 10: Finalize — builds the migration summary, records failures,
// clears the checkpoint, and reports completion.
func (p *Pipeline) stepFinalize(ctx context.Context, result *RunResult, ms *migrationState) {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhaseFinalizing)
	p.reporter.OnPhaseStart(desc)
	log := p.stepLog(10, "finalize")
	_ = p.checkpoint.Clear(ctx)

	ms.summary.EndTime = time.Now()
	ms.summary.Duration = ms.summary.EndTime.Sub(ms.summary.StartTime)
	p.metrics.ToSummary(ms.summary)
	ms.summary.Errors = ms.allErrors
	for _, e := range ms.allErrors {
		result.Failures.Record(e)
	}

	result.Summary = ms.summary
	result.CheckpointPath = p.config.Checkpoint.Path

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)

	p.reporter.OnMigrationComplete(ms.summary)

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseFinalizing,
		Duration: dur,
	})

	log.Debug("migration complete",
		"duration", ms.summary.Duration.Round(time.Millisecond),
		"written", ms.summary.TotalWritten,
		"failed", ms.summary.TotalFailed,
		"scanned", ms.summary.TotalScanned,
		"skipped", ms.summary.TotalSkipped,
		"bytes", ms.summary.BytesTransferred,
		"tables", len(ms.summary.TableMetrics),
		"errors", len(ms.allErrors),
	)

	// Per-table summary at debug level for debugging.
	if len(ms.summary.TableMetrics) > 0 {
		for _, tm := range ms.summary.TableMetrics {
			log.Debug("table result",
				"table", tm.Table,
				"scanned", tm.Scanned,
				"written", tm.Written,
				"failed", tm.Failed,
				"bytes", tm.Bytes,
				"duration", tm.Duration.Round(time.Millisecond),
			)
		}
	}

	// Error category summary when there are failures.
	if result.Failures != nil && result.Failures.Total > 0 {
		log.Warn("migration completed with errors",
			"total_errors", result.Failures.Total,
			"categories", result.Failures.Counts,
		)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle controls
// ---------------------------------------------------------------------------

// Pause suspends the pipeline at the next batch boundary.
func (p *Pipeline) Pause() {
	p.paused.Store(true)
	p.reporter.OnPhaseChange(provider.PhasePaused)
}

// Resume resumes a paused pipeline.
func (p *Pipeline) Resume() {
	p.paused.Store(false)
	p.pauseCond.Broadcast()
}

// Cancel cancels the pipeline context.
func (p *Pipeline) Cancel() {
	if p.cancelFn != nil {
		p.cancelFn()
	}
}

// waitIfPaused blocks until the pipeline is resumed. Safe for concurrent workers.
func (p *Pipeline) waitIfPaused() {
	if !p.paused.Load() {
		return
	}
	p.pauseCond.L.Lock()
	for p.paused.Load() {
		p.pauseCond.Wait()
	}
	p.pauseCond.L.Unlock()
}

// ---------------------------------------------------------------------------
// Key tracking
// ---------------------------------------------------------------------------

// hashKey returns a truncated SHA-256 hash of a key for dedup tracking.
// This avoids storing raw key names (which may contain sensitive data like
// Redis key patterns) in memory and on-disk checkpoints.
func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:8]) // 16 hex chars = 64 bits, sufficient for dedup
}

// recordKeys adds unit keys to the tracking set, evicting the oldest entries
// when the capacity (opts.MaxWrittenKeys) is exceeded. Uses a ring buffer
// for O(1) eviction and sync.Map for lock-free dedup checks on the read path.
func (p *Pipeline) recordKeys(units []provider.MigrationUnit) {
	cap := p.opts.MaxWrittenKeys

	// Grow ring buffer if needed (e.g. after restore from checkpoint).
	p.keyMu.Lock()
	if cap > len(p.keyRing) {
		newRing := make([]string, cap)
		copy(newRing, p.keyRing)
		p.keyRing = newRing
	}
	p.keyMu.Unlock()

	for _, u := range units {
		hk := hashKey(u.Key)

		// Lock-free dedup check — sync.Map.LoadOrStore is atomic.
		if _, loaded := p.writtenKeys.LoadOrStore(hk, true); loaded {
			continue
		}

		// Ring buffer eviction/insertion and table tracking need a mutex
		// for ordered access and safe writes to tableSet/scannedTables.
		p.keyMu.Lock()
		if u.Table != "" && !p.tableSet[u.Table] {
			p.tableSet[u.Table] = true
			p.scannedTables = append(p.scannedTables, u.Table)
		}
		if p.keyRingLen >= cap {
			evictIdx := p.keyRingHead
			p.writtenKeys.Delete(p.keyRing[evictIdx])
			p.keyRingLen--
			p.keysEvicted = true
		}
		p.keyRing[p.keyRingHead] = hk
		p.keyRingHead = (p.keyRingHead + 1) % cap
		p.keyRingLen++
		p.keyMu.Unlock()
	}

	atomic.AddInt64(&p.totalWritten, int64(len(units)))
}

// writtenKeysFlat materialises the ring buffer as a flat slice for consumers
// that need an ordered list (verification, checkpoint, resume token).
func (p *Pipeline) writtenKeysFlat() []string {
	if p.keyRingLen == 0 {
		return nil
	}
	start := (p.keyRingHead - p.keyRingLen + len(p.keyRing)) % len(p.keyRing)
	out := make([]string, p.keyRingLen)
	for i := 0; i < p.keyRingLen; i++ {
		out[i] = p.keyRing[(start+i)%len(p.keyRing)]
	}
	return out
}

// ---------------------------------------------------------------------------
// Checkpoint management
// ---------------------------------------------------------------------------

// maybeCheckpoint saves a checkpoint when the throttle interval has been met.
func (p *Pipeline) maybeCheckpoint(ctx context.Context, batchID int, scanner provider.Scanner) {
	p.cpMu.Lock()
	p.batchesSinceCP++
	shouldSave := p.opts.CheckpointInterval == 0 || p.batchesSinceCP >= p.opts.CheckpointInterval
	if shouldSave {
		p.batchesSinceCP = 0
	}
	p.cpMu.Unlock()

	if shouldSave {
		p.saveCheckpoint(ctx, batchID, scanner)
	}
}

// saveCheckpoint persists the current migration progress.
func (p *Pipeline) saveCheckpoint(ctx context.Context, batchID int, scanner provider.Scanner) {
	p.cpMu.Lock()
	defer p.cpMu.Unlock()

	stats := scanner.Stats()

	p.keyMu.Lock()
	keysFlat := p.writtenKeysFlat()
	p.keyMu.Unlock()
	totalWritten := atomic.LoadInt64(&p.totalWritten)

	var token []byte
	if stats.TotalScanned > 0 {
		token = encodeResumeToken(p.config.Source.Provider, stats, keysFlat)
	}

	// Only include truly completed tables: tables whose scanner cursor was
	// fully exhausted.  The scanner increments TablesDone when it closes a
	// table's cursor, so scannedTables[:TablesDone] are the ones that are
	// safe to skip on resume.  Tables beyond that index are in-progress and
	// must NOT be skipped — the scanner will re-read them and dedup will
	// prevent duplicate writes.
	completedCount := stats.TablesDone
	if completedCount > len(p.scannedTables) {
		completedCount = len(p.scannedTables)
	}
	completedTables := make([]string, completedCount)
	copy(completedTables, p.scannedTables[:completedCount])

	// Track the in-progress table (if any) for diagnostics and future
	// row-level resume support.
	var lastTableScanning string
	if completedCount < len(p.scannedTables) {
		lastTableScanning = p.scannedTables[completedCount]
	}

	cp := &Checkpoint{
		SourceProvider:    p.config.Source.Provider,
		DestProvider:      p.config.Destination.Provider,
		ConfigHash:        computeConfigHash(p.config),
		StartTime:         p.startTime,
		LastBatchID:       batchID,
		TotalWritten:      totalWritten,
		TablesCompleted:   completedTables,
		LastTableScanning: lastTableScanning,
		WrittenKeys:       keysFlat,
		ResumeToken:       token,
		Timestamp:         time.Now(),
		Version:           checkpointVersion,
	}

	if err := p.checkpoint.Save(ctx, cp); err != nil {
		logger.L().Warn("failed to save checkpoint", "error", err)
	} else {
		logger.L().Debug("checkpoint saved",
			"batch", batchID,
			"written", totalWritten,
			"tables_completed", len(completedTables),
			"keys_tracked", len(keysFlat),
			"last_table", lastTableScanning,
		)
	}
}

// ---------------------------------------------------------------------------
// Batch splitting
// ---------------------------------------------------------------------------

// splitBatch splits a slice of migration units into sub-batches when the
// cumulative byte size exceeds maxBytes. If maxBytes is 0 the batch is
// returned as-is.
func splitBatch(units []provider.MigrationUnit, maxBytes int64) [][]provider.MigrationUnit {
	if maxBytes <= 0 || len(units) == 0 {
		return [][]provider.MigrationUnit{units}
	}
	var batches [][]provider.MigrationUnit
	var current []provider.MigrationUnit
	var currentBytes int64
	for _, u := range units {
		if len(current) > 0 && currentBytes+u.Size > maxBytes {
			batches = append(batches, current)
			current = nil
			currentBytes = 0
		}
		current = append(current, u)
		currentBytes += u.Size
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}

// ---------------------------------------------------------------------------
// Abort
// ---------------------------------------------------------------------------

// abort cleans up and returns an error result.
func (p *Pipeline) abort(err error) error {
	p.reporter.OnPhaseChange(provider.PhaseError)
	_ = p.tunnels.CloseAll()
	return err
}

// ---------------------------------------------------------------------------
// Tunnel config helpers
// ---------------------------------------------------------------------------

// buildTunnelConfigs creates tunnel configurations from the migration config.
func (p *Pipeline) buildTunnelConfigs() map[string]tunnel.Config {
	configs := make(map[string]tunnel.Config)

	if p.config.Source.SSH != nil && p.config.Source.SSH.Enabled {
		cfg := *p.config.Source.SSH
		cfg.RemoteHost = hostFromConnection(p.config.Source)
		cfg.RemotePort = portFromConnection(p.config.Source)
		configs["source"] = cfg
	}

	if p.config.Destination.SSH != nil && p.config.Destination.SSH.Enabled {
		cfg := *p.config.Destination.SSH
		cfg.RemoteHost = hostFromConnection(p.config.Destination)
		cfg.RemotePort = portFromConnection(p.config.Destination)
		configs["destination"] = cfg
	}

	return configs
}

// resolveProviderConfig returns the provider-specific config struct,
// with tunnel-resolved address if applicable.
func (p *Pipeline) resolveProviderConfig(side string, connCfg *config.ConnectionConfig) (any, error) {
	addr := p.tunnels.ResolvedAddr(side)
	return config.ProviderConfigWithTunnel(connCfg, addr)
}

// ---------------------------------------------------------------------------
// Schema migration
// ---------------------------------------------------------------------------

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

	// NoSQL sources (e.g. MongoDB) produce collections without column
	// definitions.  SQL destinations require at least one column to generate
	// valid CREATE TABLE DDL.  Skip schema migration when the inspected
	// schema has zero columns.
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

// ---------------------------------------------------------------------------
// Config extraction helpers
// ---------------------------------------------------------------------------

func hostFromConnection(cfg config.ConnectionConfig) string {
	if r := cfg.Resolved(); r != nil {
		return r.GetHost()
	}
	return ""
}

func portFromConnection(cfg config.ConnectionConfig) int {
	if r := cfg.Resolved(); r != nil {
		return r.GetPort()
	}
	return 0
}

func resumeToken(cp *Checkpoint) []byte {
	if cp == nil {
		return nil
	}
	return cp.ResumeToken
}

func lastBatchID(cp *Checkpoint) int {
	if cp == nil {
		return 0
	}
	return cp.LastBatchID
}

func tablesCompleted(cp *Checkpoint) []string {
	if cp == nil {
		return nil
	}
	return cp.TablesCompleted
}

// encodeResumeToken creates a provider-specific resume token from scan stats.
func encodeResumeToken(providerName string, stats provider.ScanStats, writtenKeys []string) []byte {
	token := map[string]any{
		"total_scanned": stats.TotalScanned,
		"total_bytes":   stats.TotalBytes,
		"tables_done":   stats.TablesDone,
		"tables_total":  stats.TablesTotal,
	}
	if providerName == "redis" && len(writtenKeys) > 0 {
		token["processed_keys"] = writtenKeys
	}
	data, err := sonic.Marshal(token)
	if err != nil {
		return nil
	}
	return data
}

// totalSize computes the total byte size of a slice of MigrationUnits.
func totalSize(units []provider.MigrationUnit) int64 {
	var total int64
	for _, u := range units {
		total += u.Size
	}
	return total
}

// appendError adds an error to the slice, respecting the cap.
func appendError(errors *[]error, max int, err error) {
	if len(*errors) < max {
		*errors = append(*errors, err)
	}
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

// computeConfigHash returns a deterministic hash of key config fields.
// If the config or pipeline options change between runs, the checkpoint
// is invalidated to prevent resuming with incompatible settings.
func computeConfigHash(cfg *config.MigrationConfig) string {
	h := sha256.New()

	h.Write([]byte(cfg.Source.Provider))
	h.Write([]byte{0})
	h.Write([]byte(cfg.Destination.Provider))
	h.Write([]byte{0})

	h.Write([]byte(hostFromConnection(cfg.Source)))
	h.Write([]byte{0})
	fmt.Fprintf(h, "%d", portFromConnection(cfg.Source)) //nolint:errcheck
	h.Write([]byte{0})
	h.Write([]byte(dbFromConnection(cfg.Source)))
	h.Write([]byte{0})
	h.Write([]byte(hostFromConnection(cfg.Destination)))
	h.Write([]byte{0})
	fmt.Fprintf(h, "%d", portFromConnection(cfg.Destination)) //nolint:errcheck
	h.Write([]byte{0})
	h.Write([]byte(dbFromConnection(cfg.Destination)))
	h.Write([]byte{0})

	// Pipeline options that affect correctness of resume.
	fmt.Fprintf(h, "%d", cfg.Pipeline.BatchSize) //nolint:errcheck
	h.Write([]byte{0})
	h.Write([]byte(string(cfg.Pipeline.ConflictStrategy)))
	h.Write([]byte{0})
	h.Write([]byte(cfg.Pipeline.FKHandling))
	h.Write([]byte{0})

	for table, mappings := range cfg.Transform.Mappings {
		h.Write([]byte(table))
		h.Write([]byte{0})
		for _, m := range mappings {
			h.Write([]byte(m.Source))
			h.Write([]byte{0})
			h.Write([]byte(m.Destination))
			h.Write([]byte{0})
		}
	}
	h.Write([]byte{0})

	return hex.EncodeToString(h.Sum(nil))
}

// ComputeConfigHash returns a deterministic hash of key config fields used to
// validate checkpoint resume compatibility.
func ComputeConfigHash(cfg *config.MigrationConfig) string {
	return computeConfigHash(cfg)
}

// dbFromConnection returns the database name from connection config.
func dbFromConnection(cfg config.ConnectionConfig) string {
	if r := cfg.Resolved(); r != nil {
		return r.GetDatabase()
	}
	return ""
}
