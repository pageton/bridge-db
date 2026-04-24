package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/pkg/provider"
)

// BenchmarkConfig defines a single benchmark configuration.
type BenchmarkConfig struct {
	Scenario  Scenario
	Size      DatasetSizeConfig
	BatchSize int
	Workers   int
	Verify    bool
}

// BenchmarkResult captures the outcome of a single benchmark run.
type BenchmarkResult struct {
	Label         string        `json:"label"`
	Scenario      string        `json:"scenario"`
	Source        string        `json:"source"`
	Destination   string        `json:"destination"`
	SizeName      string        `json:"size_name"`
	Records       int           `json:"records"`
	DataSizeBytes int64         `json:"data_size_bytes"`
	BatchSize     int           `json:"batch_size"`
	Workers       int           `json:"workers"`
	Verify        bool          `json:"verify"`
	TotalDuration time.Duration `json:"total_duration"`
	RowsPerSec    float64       `json:"rows_per_sec"`
	MBPerSec      float64       `json:"mb_per_sec"`
	PeakRAMMB     float64       `json:"peak_ram_mb"`
	SchemaTime    time.Duration `json:"schema_time,omitempty"`
	TransferTime  time.Duration `json:"transfer_time,omitempty"`
	VerifyTime    time.Duration `json:"verify_time,omitempty"`
	Errors        int           `json:"errors"`
	// Resume-specific fields
	IsResumeTest   bool          `json:"is_resume_test,omitempty"`
	InterruptAfter time.Duration `json:"interrupt_after,omitempty"`
	RecoveryTime   time.Duration `json:"recovery_time,omitempty"`
	RecordsBefore  int64         `json:"records_before_interrupt,omitempty"`
	RecordsAfter   int64         `json:"records_after_resume,omitempty"`
	DataCorrect    bool          `json:"data_correct,omitempty"`
}

// BenchmarkRunner executes benchmarks.
type BenchmarkRunner struct {
	Seed          int64
	KeepDBs       bool
	HW            HardwareInfo
	SeedBatchSize int
	SeedWorkers   int
}

type PreparedSource struct {
	TmpDir         string
	SourceURL      string
	DataSizeBytes  int64
	SourceProvider ProviderEndpoint
	Size           DatasetSizeConfig
}

func (r *BenchmarkRunner) prepareSource(sc Scenario, size DatasetSizeConfig) (*PreparedSource, error) {
	tmpDir, err := os.MkdirTemp("", "bench-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	srcURL, dataSize, err := SeedProvider(context.Background(), sc.Source, tmpDir, size, r.Seed, r.SeedBatchSize, r.SeedWorkers)
	if err != nil {
		if !r.KeepDBs {
			_ = os.RemoveAll(tmpDir)
		}
		return nil, fmt.Errorf("seed source: %w", err)
	}

	return &PreparedSource{
		TmpDir:         tmpDir,
		SourceURL:      srcURL,
		DataSizeBytes:  dataSize,
		SourceProvider: sc.Source,
		Size:           size,
	}, nil
}

func (r *BenchmarkRunner) cleanupPreparedSource(ps *PreparedSource) {
	if ps == nil || r.KeepDBs {
		return
	}
	_ = os.RemoveAll(ps.TmpDir)
}

// runOne executes a single benchmark configuration.
func (r *BenchmarkRunner) runOne(cfg BenchmarkConfig) (BenchmarkResult, error) {
	ps, err := r.prepareSource(cfg.Scenario, cfg.Size)
	if err != nil {
		return BenchmarkResult{}, err
	}
	defer r.cleanupPreparedSource(ps)
	return r.runPrepared(cfg, ps)
}

func (r *BenchmarkRunner) runPrepared(cfg BenchmarkConfig, ps *PreparedSource) (BenchmarkResult, error) {
	srcEp := cfg.Scenario.Source
	dstEp := cfg.Scenario.Destination
	scenarioLabel := cfg.Scenario.Label()
	tmpDir := ps.TmpDir

	if err := cleanupEndpoint(context.Background(), dstEp); err != nil {
		return BenchmarkResult{}, fmt.Errorf("cleanup destination: %w", err)
	}

	dstURL := DestURL(dstEp, tmpDir)
	if dstEp.IsSQLite() {
		_ = os.Remove(dstURL)
	}
	cpPath := filepath.Join(tmpDir, fmt.Sprintf("checkpoint-b%d-w%d.json", cfg.BatchSize, cfg.Workers))

	migrationCfg := &config.MigrationConfig{
		Source: config.ConnectionConfig{
			Provider: srcEp.Provider,
		},
		Destination: config.ConnectionConfig{
			Provider: dstEp.Provider,
		},
		Pipeline: config.PipelineConfig{
			BatchSize:        cfg.BatchSize,
			Verify:           cfg.Verify,
			ConflictStrategy: provider.ConflictOverwrite,
			MigrateSchema:    supportsSchemaMigration(srcEp, dstEp),
			FKHandling:       "skip",
			MaxRetries:       3,
			RetryBackoff:     500 * time.Millisecond,
		},
		Checkpoint: config.CheckpointConfig{
			Enabled: true,
			Path:    cpPath,
		},
		Transform: config.DefaultTransformConfig(),
		Logging:   config.LoggingConfig{Level: "warn"},
	}

	// SQLite: set path directly, skip URL resolution (file paths aren't URLs).
	// Non-SQLite: set URL so Resolve() can parse into provider-specific config.
	if srcEp.IsSQLite() {
		migrationCfg.Source.SQLite = &config.SQLiteConfig{Path: ps.SourceURL}
	} else {
		migrationCfg.Source.URL = ps.SourceURL
	}
	if dstEp.IsSQLite() {
		migrationCfg.Destination.SQLite = &config.SQLiteConfig{Path: dstURL}
	} else {
		migrationCfg.Destination.URL = dstURL
	}

	// Resolve connection URLs into provider-specific configs.
	if err := config.Resolve(migrationCfg); err != nil {
		return BenchmarkResult{}, fmt.Errorf("resolve config: %w", err)
	}

	opts := bridge.DefaultPipelineOptions()
	opts.BatchSize = cfg.BatchSize
	opts.Verify = cfg.Verify
	opts.WriteWorkers = cfg.Workers
	opts.Parallel = cfg.Workers
	opts.CheckpointEnabled = true
	opts.CheckpointInterval = 10
	opts.MaxWrittenKeys = benchmarkMaxWrittenKeys(cfg.Size.Records)
	opts.MigrateSchema = supportsSchemaMigration(srcEp, dstEp)
	opts.FKHandling = "skip"

	// Track peak memory
	var peakRAM uint64
	ramStop := startRAMTracker(&peakRAM)
	defer ramStop()

	// Run the pipeline
	pipeline, err := bridge.NewPipeline(migrationCfg, opts, &benchReporter{}, nil)
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("create pipeline: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), benchmarkRunTimeout(cfg))
	defer cancel()
	result, err := pipeline.Run(ctx)
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("pipeline run: %w", err)
	}

	var schemaTime, transferTime, verifyTime time.Duration
	for _, phase := range result.Phases {
		switch phase.Phase {
		case provider.PhaseSchemaMigration:
			schemaTime = phase.Duration
		case provider.PhaseScanning:
			transferTime = phase.Duration
		case provider.PhaseVerifying:
			verifyTime = phase.Duration
		}
	}

	totalDur := result.Summary.Duration
	recordsWritten := result.Summary.TotalWritten
	bytesWritten := result.Summary.BytesTransferred

	rowsPerSec := 0.0
	if totalDur.Seconds() > 0 && recordsWritten > 0 {
		rowsPerSec = float64(recordsWritten) / totalDur.Seconds()
	}

	mbPerSec := 0.0
	if totalDur.Seconds() > 0 && bytesWritten > 0 {
		mbPerSec = float64(bytesWritten) / (1024 * 1024) / totalDur.Seconds()
	}

	errorCount := result.Summary.TotalFailed

	fmt.Fprintf(os.Stderr, "    completed in %v (%.0f rows/s, %.1f MB/s, %d errors)\n",
		totalDur, rowsPerSec, mbPerSec, errorCount)

	return BenchmarkResult{
		Label:         fmt.Sprintf("%s-%s-b%d-w%d-v%t", scenarioLabel, cfg.Size.Name, cfg.BatchSize, cfg.Workers, cfg.Verify),
		Scenario:      scenarioLabel,
		Source:        srcEp.Provider,
		Destination:   dstEp.Provider,
		SizeName:      cfg.Size.Name,
		Records:       cfg.Size.Records,
		DataSizeBytes: ps.DataSizeBytes,
		BatchSize:     cfg.BatchSize,
		Workers:       cfg.Workers,
		Verify:        cfg.Verify,
		TotalDuration: totalDur,
		RowsPerSec:    rowsPerSec,
		MBPerSec:      mbPerSec,
		PeakRAMMB:     float64(peakRAM) / (1024 * 1024),
		SchemaTime:    schemaTime,
		TransferTime:  transferTime,
		VerifyTime:    verifyTime,
		Errors:        int(errorCount),
	}, nil
}

func benchmarkRunTimeout(cfg BenchmarkConfig) time.Duration {
	base := 10 * time.Minute
	switch cfg.Size.Name {
	case "medium":
		base = 20 * time.Minute
	case "large":
		base = 45 * time.Minute
	}
	if cfg.Verify {
		base += 5 * time.Minute
	}
	return base
}

// runResumeTest benchmarks the resume/interruption scenario.
func (r *BenchmarkRunner) runResumeTest(cfg BenchmarkConfig) (BenchmarkResult, error) {
	tmpDir, err := os.MkdirTemp("", "bench-resume-*")
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("create temp dir: %w", err)
	}
	if !r.KeepDBs {
		defer os.RemoveAll(tmpDir)
	}

	srcEp := cfg.Scenario.Source
	dstEp := cfg.Scenario.Destination

	if err := cleanupEndpoint(context.Background(), dstEp); err != nil {
		return BenchmarkResult{}, fmt.Errorf("cleanup destination: %w", err)
	}

	// Seed source
	fmt.Fprintf(os.Stderr, "    generating %d records for resume test...\n", cfg.Size.Records)
	srcURL, dataSize, err := SeedProvider(context.Background(), srcEp, tmpDir, cfg.Size, r.Seed, r.SeedBatchSize, r.SeedWorkers)
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("seed source: %w", err)
	}

	dstURL := DestURL(dstEp, tmpDir)
	cpPath := filepath.Join(tmpDir, "checkpoint.json")

	// ---- Phase 1: Run migration and cancel after 2s ----
	migrationCfg := buildMigrationConfig(cfg, srcEp, dstEp, srcURL, dstURL, cpPath)

	opts := bridge.DefaultPipelineOptions()
	opts.BatchSize = cfg.BatchSize
	opts.Verify = false
	opts.WriteWorkers = cfg.Workers
	opts.Parallel = cfg.Workers
	opts.CheckpointEnabled = true
	opts.CheckpointInterval = 1
	opts.MaxWrittenKeys = benchmarkMaxWrittenKeys(cfg.Size.Records)
	opts.MigrateSchema = supportsSchemaMigration(srcEp, dstEp)
	opts.FKHandling = "skip"

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	pipeline1, err := bridge.NewPipeline(migrationCfg, opts, &benchReporter{}, nil)
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("create pipeline (run 1): %w", err)
	}

	go func() {
		time.Sleep(2 * time.Second)
		cancel1()
	}()

	_, _ = pipeline1.Run(ctx1)

	// Read checkpoint to see how far we got
	recordsBefore := int64(0)
	if cpData, err := os.ReadFile(cpPath); err == nil && len(cpData) > 0 {
		var cp struct {
			TotalWritten int64 `json:"total_written"`
		}
		if err := parseCheckpointJSON(cpData, &cp); err == nil {
			recordsBefore = cp.TotalWritten
		}
	}
	fmt.Fprintf(os.Stderr, "    interrupted after %d records written\n", recordsBefore)

	// ---- Phase 2: Resume ----
	var peakRAM uint64
	ramStop := startRAMTracker(&peakRAM)
	defer ramStop()

	opts.Resume = true
	opts.Verify = cfg.Verify

	pipeline2, err := bridge.NewPipeline(migrationCfg, opts, &benchReporter{}, nil)
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("create pipeline (resume): %w", err)
	}

	resumeStart := time.Now()
	result2, err := pipeline2.Run(context.Background())
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("resume run: %w", err)
	}
	recoveryTime := time.Since(resumeStart)

	totalWritten := result2.Summary.TotalWritten
	dataCorrect := result2.Summary.TotalFailed == 0

	fmt.Fprintf(os.Stderr, "    resume completed in %v (%d records, %d errors, correct=%v)\n",
		recoveryTime, totalWritten, result2.Summary.TotalFailed, dataCorrect)

	return BenchmarkResult{
		Label:         fmt.Sprintf("resume-%s", cfg.Size.Name),
		Scenario:      cfg.Scenario.Label(),
		Source:        srcEp.Provider,
		Destination:   dstEp.Provider,
		SizeName:      cfg.Size.Name,
		Records:       cfg.Size.Records,
		DataSizeBytes: dataSize,
		BatchSize:     cfg.BatchSize,
		Workers:       cfg.Workers,
		Verify:        cfg.Verify,
		TotalDuration: recoveryTime,
		RowsPerSec:    float64(totalWritten) / recoveryTime.Seconds(),
		PeakRAMMB:     float64(peakRAM) / (1024 * 1024),
		IsResumeTest:  true,
		RecoveryTime:  recoveryTime,
		RecordsBefore: recordsBefore,
		RecordsAfter:  totalWritten,
		DataCorrect:   dataCorrect,
	}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func buildMigrationConfig(cfg BenchmarkConfig, srcEp, dstEp ProviderEndpoint, srcURL, dstURL, cpPath string) *config.MigrationConfig {
	mc := &config.MigrationConfig{
		Source: config.ConnectionConfig{
			Provider: srcEp.Provider,
		},
		Destination: config.ConnectionConfig{
			Provider: dstEp.Provider,
		},
		Pipeline: config.PipelineConfig{
			BatchSize:        cfg.BatchSize,
			Verify:           cfg.Verify,
			ConflictStrategy: provider.ConflictOverwrite,
			MigrateSchema:    supportsSchemaMigration(srcEp, dstEp),
			FKHandling:       "skip",
			MaxRetries:       3,
			RetryBackoff:     500 * time.Millisecond,
		},
		Checkpoint: config.CheckpointConfig{
			Enabled: true,
			Path:    cpPath,
		},
		Transform: config.DefaultTransformConfig(),
		Logging:   config.LoggingConfig{Level: "warn"},
	}

	if srcEp.IsSQLite() {
		mc.Source.SQLite = &config.SQLiteConfig{Path: srcURL}
	} else {
		mc.Source.URL = srcURL
	}
	if dstEp.IsSQLite() {
		mc.Destination.SQLite = &config.SQLiteConfig{Path: dstURL}
	} else {
		mc.Destination.URL = dstURL
	}

	return mc
}

func benchmarkMaxWrittenKeys(records int) int {
	if records < defaultBenchTrackedKeys {
		return defaultBenchTrackedKeys
	}
	return records * 2
}

// supportsSchemaMigration returns true if the source→destination pair can
// use --migrate-schema. Both providers must support schema operations:
// source must support schema inspection, destination must support DDL.
func supportsSchemaMigration(src, dst ProviderEndpoint) bool {
	switch src.Provider {
	case "redis":
		return false
	}
	switch dst.Provider {
	case "redis":
		return false
	}
	return true
}

const defaultBenchTrackedKeys = 1024

// startRAMTracker starts a goroutine that polls process RSS and falls back to
// live Go-managed memory when RSS is unavailable.
func startRAMTracker(peak *uint64) func() {
	stop := make(chan struct{})
	*peak = 0
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		updatePeak := func() {
			usage := currentMemoryUsage()
			if usage > *peak {
				*peak = usage
			}
		}
		for {
			select {
			case <-ticker.C:
				updatePeak()
			case <-stop:
				updatePeak()
				return
			}
		}
	}()
	return func() { close(stop) }
}

func currentMemoryUsage() uint64 {
	if rss, err := linuxRSSBytes(); err == nil && rss > 0 {
		return rss
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse + m.StackInuse
}

func linuxRSSBytes() (uint64, error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return kb * 1024, nil
	}
	return 0, fmt.Errorf("VmRSS not found")
}

// parseCheckpointJSON extracts basic fields from a checkpoint file.
func parseCheckpointJSON(data []byte, v any) error {
	return unmarshalCheckpoint(data, v)
}

// benchReporter is a minimal progress reporter that discards all output.
type benchReporter struct{}

var benchErrorPrints atomic.Int32

func (benchReporter) OnBatchStart(int, int)                          {}
func (benchReporter) OnBatchComplete(int, *provider.BatchResult)     {}
func (benchReporter) OnMigrationComplete(*provider.MigrationSummary) {}
func (benchReporter) OnError(err error, _ *provider.MigrationUnit) {
	if err == nil {
		return
	}
	if benchErrorPrints.Add(1) <= 50 {
		fmt.Fprintf(os.Stderr, "    provider error: %v\n", err)
	}
}
func (benchReporter) OnPhaseChange(provider.MigrationPhase)                {}
func (benchReporter) OnPhaseStart(provider.PhaseDesc)                      {}
func (benchReporter) OnPhaseDone(provider.PhaseDesc, time.Duration, error) {}
func (benchReporter) OnProgress(provider.ProgressStats)                    {}

// Ensure imports are used.
var _ = transform.NoopTransformer{}
