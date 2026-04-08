package bridge

import (
	"context"
	"fmt"
	"github.com/bytedance/sonic"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/internal/progress"
	"github.com/pageton/bridge-db/internal/retry"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/internal/tunnel"
	"github.com/pageton/bridge-db/pkg/provider"
)

// scanResult is a batch of migration units produced by the scanner goroutine.
type scanResult struct {
	batchID int
	units   []provider.MigrationUnit
	err     error
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
	writtenKeys []string

	pauseCh  chan struct{}
	resumeCh chan struct{}
	paused   atomic.Bool
	cancelFn context.CancelFunc
}

// NewPipeline creates a new pipeline from the given configuration.
// The caller must invoke Run() to start the migration.
func NewPipeline(cfg *config.MigrationConfig, opts PipelineOptions, reporter provider.ProgressReporter, cpStore CheckpointStore) *Pipeline {
	if reporter == nil {
		reporter = progress.NopReporter{}
	}
	if cpStore == nil {
		var err error
		cpStore, err = NewFileCheckpointStore(cfg.Checkpoint.Path)
		if err != nil {
			return nil
		}
	}
	return &Pipeline{
		config:     cfg,
		opts:       opts,
		reporter:   reporter,
		metrics:    progress.NewMetricsCollector(),
		checkpoint: cpStore,
		tunnels:    tunnel.NewPool(),
		pauseCh:    make(chan struct{}),
		resumeCh:   make(chan struct{}),
	}
}

// Run executes the full migration pipeline and returns a summary.
// It respects context cancellation for graceful shutdown.
func (p *Pipeline) Run(ctx context.Context) (*RunResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancelFn = cancel
	defer cancel()

	log := logger.L().With("component", "pipeline")
	result := &RunResult{
		Config:      p.config,
		SrcProvider: p.config.Source.Provider,
		DstProvider: p.config.Destination.Provider,
	}

	summary := &provider.MigrationSummary{
		StartTime: time.Now(),
	}

	// ---------------------------------------------------------------
	// Phase: INIT
	// ---------------------------------------------------------------
	phaseStart := time.Now()
	p.reporter.OnPhaseChange(provider.PhaseInit)

	if err := p.opts.Validate(); err != nil {
		return nil, fmt.Errorf("pipeline options: %w", err)
	}

	log.Info("migration starting",
		"source", p.config.Source.Provider,
		"destination", p.config.Destination.Provider,
		"cross_db", p.config.IsCrossDB(),
		"dry_run", p.opts.DryRun,
	)

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseInit,
		Duration: time.Since(phaseStart),
	})

	// ---------------------------------------------------------------
	// Phase: TUNNEL
	// ---------------------------------------------------------------
	phaseStart = time.Now()
	p.reporter.OnPhaseChange(provider.PhaseTunnel)

	tunnelConfigs := p.buildTunnelConfigs()
	if err := p.tunnels.OpenAll(ctx, tunnelConfigs); err != nil {
		return nil, p.abort(result, fmt.Errorf("tunnel: %w", err))
	}
	defer func() { _ = p.tunnels.CloseAll() }()

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseTunnel,
		Duration: time.Since(phaseStart),
	})

	// ---------------------------------------------------------------
	// Phase: CONNECT
	// ---------------------------------------------------------------
	phaseStart = time.Now()
	p.reporter.OnPhaseChange(provider.PhaseConnect)

	srcProvider, err := provider.New(p.config.Source.Provider)
	if err != nil {
		return nil, p.abort(result, fmt.Errorf("source provider: %w", err))
	}
	p.src = srcProvider

	dstProvider, err := provider.New(p.config.Destination.Provider)
	if err != nil {
		return nil, p.abort(result, fmt.Errorf("dest provider: %w", err))
	}
	p.dst = dstProvider

	// Resolve tunnel addresses for connections
	srcConfig := p.resolveProviderConfig("source", &p.config.Source)
	dstConfig := p.resolveProviderConfig("destination", &p.config.Destination)

	if err := p.src.Connect(ctx, srcConfig, nil); err != nil {
		return nil, p.abort(result, fmt.Errorf("source connect: %w", err))
	}
	defer func() { _ = p.src.Close() }()

	if err := p.dst.Connect(ctx, nil, dstConfig); err != nil {
		return nil, p.abort(result, fmt.Errorf("dest connect: %w", err))
	}
	defer func() { _ = p.dst.Close() }()

	if err := p.src.Ping(ctx); err != nil {
		return nil, p.abort(result, fmt.Errorf("source ping: %w", err))
	}
	if err := p.dst.Ping(ctx); err != nil {
		return nil, p.abort(result, fmt.Errorf("dest ping: %w", err))
	}

	log.Info("connected to both databases")

	// Wrap providers for dry-run mode
	if p.opts.DryRun {
		p.dst = p.dst.DryRun()
		log.Info("dry-run mode enabled")
	}

	// ---------------------------------------------------------------
	// RESOLVE TRANSFORMER (before schema migration needs the type mapper)
	// ---------------------------------------------------------------
	p.transformer = transform.GetTransformer(
		p.config.Source.Provider,
		p.config.Destination.Provider,
	)
	if p.transformer.NeedsSchema() {
		log.Info("transformer requires schema information")
	}

	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseConnect,
		Duration: time.Since(phaseStart),
	})

	// ---------------------------------------------------------------
	// RESUME CHECK
	// ---------------------------------------------------------------
	var checkpoint *Checkpoint
	if !p.opts.CheckpointEnabled {
		log.Info("checkpointing disabled")
	} else {
		checkpoint, err = p.checkpoint.Load(ctx)
		if err != nil {
			log.Warn("failed to load checkpoint, starting fresh", "error", err)
			checkpoint = nil
		}
		if checkpoint != nil && p.opts.Resume {
			result.Resumed = true
			log.Info("resuming from checkpoint",
				"last_batch", checkpoint.LastBatchID,
				"timestamp", checkpoint.Timestamp,
			)
		} else if checkpoint != nil {
			log.Info("checkpoint found but --resume not set, starting fresh")
			_ = p.checkpoint.Clear(ctx)
			checkpoint = nil
		}
	}

	// ---------------------------------------------------------------
	// Phase: SCHEMA MIGRATION
	// ---------------------------------------------------------------
	if p.shouldMigrateSchema() {
		phaseStart = time.Now()
		p.reporter.OnPhaseChange(provider.PhaseSchemaMigration)

		if err := p.migrateSchema(ctx); err != nil {
			return nil, p.abort(result, fmt.Errorf("schema migration: %w", err))
		}

		result.Phases = append(result.Phases, PhaseResult{
			Phase:    provider.PhaseSchemaMigration,
			Duration: time.Since(phaseStart),
		})
	}

	// ---------------------------------------------------------------
	// Phase: SCAN + TRANSFORM + WRITE
	// ---------------------------------------------------------------
	phaseStart = time.Now()
	p.reporter.OnPhaseChange(provider.PhaseScanning)

	scanOpts := provider.ScanOptions{
		BatchSize:       p.opts.BatchSize,
		ResumeToken:     resumeToken(checkpoint),
		TablesCompleted: tablesCompleted(checkpoint),
	}
	scanner := p.src.Scanner(ctx, scanOpts)

	writeOpts := provider.WriteOptions{
		BatchSize:  p.opts.BatchSize,
		OnConflict: p.opts.ConflictStrategy,
	}
	writer := p.dst.Writer(ctx, writeOpts)

	batchID := lastBatchID(checkpoint)
	var allErrors []error

	// maxErrorsToTrack caps the number of errors stored in memory.
	// After this limit, errors are counted but not retained.
	const maxErrorsToTrack = 1000

	var errorsMu sync.Mutex

	// scanBuffer controls how many batches can be queued between scanner and writer.
	// The value is taken from opts.Parallel.
	scanBuffer := p.opts.Parallel

	scanCh := make(chan scanResult, scanBuffer)

	// Start scanner goroutine.
	var scanWg sync.WaitGroup
	scanWg.Add(1)
	go func() {
		defer scanWg.Done()
		defer close(scanCh)

		for {
			if err := ctx.Err(); err != nil {
				scanCh <- scanResult{err: err}
				return
			}

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
					scanCh <- scanResult{err: scanRetryErr}
					continue
				}
				if err == io.EOF {
					return
				}
			}

			if len(units) == 0 {
				continue
			}

			batchID++
			p.metrics.RecordScan(int64(len(units)), totalSize(units))
			p.reporter.OnBatchStart(batchID, len(units))

			if _, ok := p.transformer.(transform.NoopTransformer); !ok {
				p.reporter.OnPhaseChange(provider.PhaseTransforming)
				transformErr := retry.Do(ctx, retry.Config{
					MaxAttempts:     p.opts.MaxRetries + 1,
					InitialInterval: p.opts.RetryBackoff,
					MaxInterval:     10 * time.Second,
					Multiplier:      2.0,
				}, func() error {
					var terr error
					units, terr = p.transformer.Transform(ctx, units)
					return terr
				})
				if transformErr != nil {
					p.reporter.OnError(transformErr, nil)
					p.metrics.RecordError()
					errorsMu.Lock()
					appendError(&allErrors, maxErrorsToTrack, transformErr)
					errorsMu.Unlock()
					log.Warn("transform error after retries, skipping batch", "batch", batchID, "error", transformErr)
					continue
				}
				p.reporter.OnPhaseChange(provider.PhaseWriting)
			}

			scanCh <- scanResult{batchID: batchID, units: units}
		}
	}()

	// Consume scan results and write them.
	for sr := range scanCh {
		// Check for pause
		p.waitForResume()

		// Check tunnel health
		if !p.tunnels.AllHealthy() {
			// Drain remaining buffered batches before aborting
			// so we can checkpoint everything that was already scanned.
			p.drainAndCheckpoint(ctx, scanCh, writer, &allErrors, maxErrorsToTrack, &errorsMu, scanner, sr.batchID)
			return nil, p.abort(result, fmt.Errorf("SSH tunnel disconnected"))
		}

		// Check context cancellation
		if err := ctx.Err(); err != nil {
			p.saveCheckpoint(ctx, sr.batchID, scanner)
			return nil, p.abort(result, fmt.Errorf("cancelled: %w", err))
		}

		// Handle scan errors from the producer.
		if sr.err != nil {
			p.reporter.OnError(sr.err, nil)
			p.metrics.RecordError()
			errorsMu.Lock()
			appendError(&allErrors, maxErrorsToTrack, sr.err)
			errorsMu.Unlock()
			log.Warn("scan error, skipping batch", "error", sr.err)
			continue
		}

		// Write with retry
		var batchResult *provider.BatchResult
		retryCfg := retry.Config{
			MaxAttempts:     p.opts.MaxRetries + 1,
			InitialInterval: p.opts.RetryBackoff,
			MaxInterval:     30 * time.Second,
			Multiplier:      2.0,
		}
		writeErr := retry.Do(ctx, retryCfg, func() error {
			var werr error
			batchResult, werr = writer.Write(ctx, sr.units)
			return werr
		})

		if writeErr != nil {
			p.reporter.OnError(writeErr, nil)
			p.metrics.RecordError()
			errorsMu.Lock()
			appendError(&allErrors, maxErrorsToTrack, fmt.Errorf("batch %d: %w", sr.batchID, writeErr))
			errorsMu.Unlock()
			continue
		}

		p.metrics.RecordBatch(batchResult)
		p.reporter.OnBatchComplete(sr.batchID, batchResult)

		for _, u := range sr.units {
			p.writtenKeys = append(p.writtenKeys, u.Key)
		}

		// Save checkpoint after each successful batch
		p.saveCheckpoint(ctx, sr.batchID, scanner)

		// Report progress
		p.reporter.OnProgress(p.metrics.Snapshot(provider.PhaseWriting))
	}

	// Wait for scanner to finish.
	scanWg.Wait()

	// Flush remaining writes
	if err := writer.Flush(ctx); err != nil {
		p.saveCheckpoint(ctx, batchID, scanner)
		return nil, p.abort(result, fmt.Errorf("flush: %w", err))
	}

	scanWriteDuration := time.Since(phaseStart)
	result.Phases = append(result.Phases, PhaseResult{
		Phase:    provider.PhaseWriting,
		Duration: scanWriteDuration,
	})

	// ---------------------------------------------------------------
	// Phase: VERIFY
	// ---------------------------------------------------------------
	if p.opts.Verify {
		phaseStart = time.Now()
		p.reporter.OnPhaseChange(provider.PhaseVerifying)

		verifier := p.dst.Verifier(ctx)
		// Verify a sample of keys
		stats := p.metrics.Snapshot(provider.PhaseVerifying)
		sampleKeys := make([]string, 0, min(100, int(stats.TotalWritten)))
		// In a real implementation, we'd collect sample keys during writing.
		// For now, pass an empty slice — the verifier will do count-based checks.
		verifierErrors, err := verifier.Verify(ctx, sampleKeys)
		if err != nil {
			log.Warn("verification error", "error", err)
		} else if len(verifierErrors) > 0 {
			summary.VerificationOK = false
			summary.VerificationErrs = verifierErrors
			log.Warn("verification found mismatches", "count", len(verifierErrors))
		} else {
			summary.VerificationOK = true
			log.Info("verification passed")
		}

		result.Phases = append(result.Phases, PhaseResult{
			Phase:    provider.PhaseVerifying,
			Duration: time.Since(phaseStart),
		})
	}

	// ---------------------------------------------------------------
	// Phase: COMPLETE
	// ---------------------------------------------------------------
	p.reporter.OnPhaseChange(provider.PhaseComplete)
	_ = p.checkpoint.Clear(ctx)

	summary.EndTime = time.Now()
	summary.Duration = summary.EndTime.Sub(summary.StartTime)
	p.metrics.ToSummary(summary)
	summary.Errors = allErrors

	result.Summary = summary
	result.CheckpointPath = p.config.Checkpoint.Path

	p.reporter.OnMigrationComplete(summary)

	log.Info("migration complete",
		"duration", summary.Duration,
		"written", summary.TotalWritten,
		"failed", summary.TotalFailed,
	)

	return result, nil
}

// Pause suspends the pipeline at the next batch boundary.
func (p *Pipeline) Pause() {
	p.paused.Store(true)
	p.pauseCh <- struct{}{}
	p.reporter.OnPhaseChange(provider.PhasePaused)
}

func (p *Pipeline) Resume() {
	p.paused.Store(false)
	p.resumeCh <- struct{}{}
}

func (p *Pipeline) Cancel() {
	if p.cancelFn != nil {
		p.cancelFn()
	}
}

func (p *Pipeline) waitForResume() {
	if p.paused.Load() {
		<-p.resumeCh
	}
}

// drainAndCheckpoint writes remaining buffered batches from the scan channel
// and checkpoints the last successful batch. Used before aborting on tunnel failure.
func (p *Pipeline) drainAndCheckpoint(
	ctx context.Context,
	scanCh <-chan scanResult,
	writer provider.Writer,
	allErrors *[]error,
	maxErrors int,
	errorsMu *sync.Mutex,
	scanner provider.Scanner,
	currentBatchID int,
) {
	lastWrittenBatch := currentBatchID

drainLoop:
	for {
		select {
		case sr, ok := <-scanCh:
			if !ok {
				break drainLoop
			}
			if sr.err != nil {
				continue
			}
			if len(sr.units) == 0 {
				continue
			}
			if _, err := writer.Write(ctx, sr.units); err != nil {
				errorsMu.Lock()
				appendError(allErrors, maxErrors, fmt.Errorf("batch %d (drain): %w", sr.batchID, err))
				errorsMu.Unlock()
				continue
			}
			lastWrittenBatch = sr.batchID
		default:
			break drainLoop
		}
	}

	p.saveCheckpoint(ctx, lastWrittenBatch, scanner)
}

// abort cleans up and returns an error result.
func (p *Pipeline) abort(result *RunResult, err error) error {
	p.reporter.OnPhaseChange(provider.PhaseError)
	_ = p.tunnels.CloseAll()
	return err
}

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
func (p *Pipeline) resolveProviderConfig(side string, connCfg *config.ConnectionConfig) any {
	// If there's a tunnel, override the host/port with the local address
	addr := p.tunnels.ResolvedAddr(side)
	if addr == "" {
		// No tunnel — return the resolved config as-is
		return providerConfig(connCfg)
	}

	// Tunnel active — create a copy with overridden host/port
	// The specific provider config will be resolved by the provider's Connect method
	return providerConfigWithAddr(connCfg, addr)
}

// shouldMigrateSchema returns true if schema migration should run.
func (p *Pipeline) shouldMigrateSchema() bool {
	if !p.opts.MigrateSchema {
		return false
	}
	// Only SQL databases support schema migration
	src := p.config.Source.Provider
	dst := p.config.Destination.Provider
	isSQLSrc := src == "postgres" || src == "mysql" || src == "sqlite" || src == "mariadb" || src == "cockroachdb" || src == "mssql"
	isSQLDst := dst == "postgres" || dst == "mysql" || dst == "sqlite" || dst == "mariadb" || dst == "cockroachdb" || dst == "mssql"
	return isSQLSrc && isSQLDst
}

// migrateSchema inspects the source schema and creates it on the destination.
func (p *Pipeline) migrateSchema(ctx context.Context) error {
	log := logger.L().With("component", "pipeline", "phase", "schema")

	srcMigrator := p.src.SchemaMigrator(ctx)
	if srcMigrator == nil {
		log.Info("source has no schema migrator, skipping")
		return nil
	}

	schema, err := srcMigrator.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect source schema: %w", err)
	}

	log.Info("inspected source schema", "tables", len(schema.Tables))

	if p.transformer.NeedsSchema() {
		p.transformer.SetSchema(schema)
	}

	dstMigrator := p.dst.SchemaMigrator(ctx)
	if dstMigrator == nil {
		log.Info("destination has no schema migrator, skipping")
		return nil
	}

	// If cross-database, use transformer's type mapper
	var mapper provider.TypeMapper
	if p.config.IsCrossDB() {
		if tm, ok := p.transformer.(interface{ TypeMapper() provider.TypeMapper }); ok {
			mapper = tm.TypeMapper()
		}
	}

	if err := dstMigrator.Create(ctx, schema, mapper); err != nil {
		return fmt.Errorf("create destination schema: %w", err)
	}

	log.Info("schema created on destination")
	return nil
}

// saveCheckpoint persists the current migration progress.
func (p *Pipeline) saveCheckpoint(ctx context.Context, batchID int, scanner provider.Scanner) {
	stats := scanner.Stats()

	// Build resume token from scanner stats (provider-specific encoding).
	// Each scanner's marshalScanStats encodes its cursor position.
	var token []byte
	if stats.TotalScanned > 0 {
		token = encodeResumeToken(p.config.Source.Provider, stats, p.writtenKeys)
	}

	cp := &Checkpoint{
		SourceProvider:  p.config.Source.Provider,
		DestProvider:    p.config.Destination.Provider,
		StartTime:       time.Now(), // TODO: use actual start time
		LastBatchID:     batchID,
		TablesCompleted: completedTables(stats),
		ResumeToken:     token,
		Timestamp:       time.Now(),
	}

	if err := p.checkpoint.Save(ctx, cp); err != nil {
		logger.L().Warn("failed to save checkpoint", "error", err)
	}
}

// ---------------------------------------------------------------------------
// Config extraction helpers
// ---------------------------------------------------------------------------

func hostFromConnection(cfg config.ConnectionConfig) string {
	switch cfg.Provider {
	case "redis":
		if cfg.Redis != nil {
			return cfg.Redis.Host
		}
	case "mongodb":
		if cfg.MongoDB != nil {
			return cfg.MongoDB.Host
		}
	case "postgres":
		if cfg.Postgres != nil {
			return cfg.Postgres.Host
		}
	case "mysql":
		if cfg.MySQL != nil {
			return cfg.MySQL.Host
		}
	case "mariadb":
		if cfg.MariaDB != nil {
			return cfg.MariaDB.Host
		}
	case "cockroachdb":
		if cfg.CockroachDB != nil {
			return cfg.CockroachDB.Host
		}
	case "mssql":
		if cfg.MSSQL != nil {
			return cfg.MSSQL.Host
		}
	case "sqlite":
	}
	return ""
}

func portFromConnection(cfg config.ConnectionConfig) int {
	switch cfg.Provider {
	case "redis":
		if cfg.Redis != nil {
			return cfg.Redis.Port
		}
	case "mongodb":
		if cfg.MongoDB != nil {
			return cfg.MongoDB.Port
		}
	case "postgres":
		if cfg.Postgres != nil {
			return cfg.Postgres.Port
		}
	case "mysql":
		if cfg.MySQL != nil {
			return cfg.MySQL.Port
		}
	case "mariadb":
		if cfg.MariaDB != nil {
			return cfg.MariaDB.Port
		}
	case "cockroachdb":
		if cfg.CockroachDB != nil {
			return cfg.CockroachDB.Port
		}
	case "mssql":
		if cfg.MSSQL != nil {
			return cfg.MSSQL.Port
		}
	case "sqlite":
	}
	return 0
}

func providerConfig(cfg *config.ConnectionConfig) any {
	switch cfg.Provider {
	case "redis":
		return cfg.Redis
	case "mongodb":
		return cfg.MongoDB
	case "postgres":
		return cfg.Postgres
	case "mysql":
		return cfg.MySQL
	case "sqlite":
		return cfg.SQLite
	case "mariadb":
		return cfg.MariaDB
	case "cockroachdb":
		return cfg.CockroachDB
	case "mssql":
		return cfg.MSSQL
	default:
		return nil
	}
}

func providerConfigWithAddr(cfg *config.ConnectionConfig, addr string) any {
	// Return a map with the tunnel-resolved address
	// Providers will handle the specific extraction
	return map[string]string{
		"tunnel_addr": addr,
		"provider":    cfg.Provider,
	}
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

func completedTables(stats provider.ScanStats) []string {
	if stats.TablesDone == 0 {
		return nil
	}
	result := make([]string, stats.TablesDone)
	for i := range result {
		result[i] = fmt.Sprintf("%d", i)
	}
	return result
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
