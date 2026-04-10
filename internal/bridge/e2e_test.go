package bridge

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// E2E: full scan -> transform -> write pipeline with mock providers
// ---------------------------------------------------------------------------

// stubProvider implements provider.Provider for testing.
type stubProvider struct {
	name    string
	units   []provider.MigrationUnit
	written [][]provider.MigrationUnit
	mu      sync.Mutex
}

func (p *stubProvider) Name() string                                             { return p.name }
func (p *stubProvider) Connect(_ context.Context, _, _ any) error                { return nil }
func (p *stubProvider) Close() error                                             { return nil }
func (p *stubProvider) Ping(_ context.Context) error                             { return nil }
func (p *stubProvider) DryRun() provider.Provider                                { return p }
func (p *stubProvider) SchemaMigrator(_ context.Context) provider.SchemaMigrator { return nil }

func (p *stubProvider) Verifier(_ context.Context) provider.Verifier {
	return &stubVerifier{}
}

func (p *stubProvider) Writer(_ context.Context, _ provider.WriteOptions) provider.Writer {
	return &stubWriter{p: p}
}

func (p *stubProvider) Scanner(_ context.Context, _ provider.ScanOptions) provider.Scanner {
	return &stubScanner{units: p.units}
}

func (p *stubProvider) EnumerateTables(_ context.Context) (map[string]int64, error) {
	tables := make(map[string]int64)
	for _, u := range p.units {
		tables[u.Table]++
	}
	return tables, nil
}

type stubScanner struct {
	units []provider.MigrationUnit
	pos   int
	mu    sync.Mutex
}

func (s *stubScanner) Next(_ context.Context) ([]provider.MigrationUnit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pos >= len(s.units) {
		return nil, io.EOF
	}
	end := s.pos + 10 // batch of 10
	if end > len(s.units) {
		end = len(s.units)
	}
	batch := s.units[s.pos:end]
	s.pos = end
	return batch, nil
}

func (s *stubScanner) Stats() provider.ScanStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return provider.ScanStats{TotalScanned: int64(s.pos), TablesTotal: 1}
}

func (s *stubScanner) Close() error { return nil }

type stubWriter struct {
	p *stubProvider
}

func (w *stubWriter) Write(_ context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	w.p.mu.Lock()
	w.p.written = append(w.p.written, append([]provider.MigrationUnit{}, units...))
	w.p.mu.Unlock()
	return &provider.BatchResult{
		TotalUnits:   len(units),
		WrittenUnits: len(units),
		BytesWritten: totalSize(units),
	}, nil
}

func (w *stubWriter) Flush(_ context.Context) error { return nil }

type stubVerifier struct{}

func (v *stubVerifier) Verify(_ context.Context, _ []string) ([]provider.VerificationError, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestE2E_SimpleMigration(t *testing.T) {
	// Register stub providers
	srcProvider := &stubProvider{
		name:  "stub_src",
		units: makeTestUnits(50, "users"),
	}
	dstProvider := &stubProvider{
		name: "stub_dst",
	}

	provider.Register("stub_src", func() provider.Provider { return srcProvider })
	provider.Register("stub_dst", func() provider.Provider { return dstProvider })
	transform.RegisterTransformer("stub_src", "stub_dst", func() transform.Transformer { return transform.NoopTransformer{} })

	cfg := &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "stub_src"},
		Destination: config.ConnectionConfig{Provider: "stub_dst"},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: t.TempDir() + "/cp.json"},
	}

	opts := PipelineOptions{
		BatchSize:          10,
		Verify:             false, // skip verification for stub providers
		ConflictStrategy:   provider.ConflictOverwrite,
		FKHandling:         "skip",
		MigrateSchema:      false,
		MaxRetries:         1,
		RetryBackoff:       0,
		Parallel:           4,
		WriteWorkers:       1,
		MaxBatchBytes:      0,
		MaxWrittenKeys:     10000,
		CheckpointInterval: 0,
		CheckpointEnabled:  true,
		VerifyMode:         "cross",
	}

	reporter := &noopReporter{}
	pipeline, err := NewPipeline(cfg, opts, reporter, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	result, err := pipeline.Run(context.Background())
	if err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}

	if result.Summary.TotalWritten != 50 {
		t.Errorf("expected 50 written, got %d", result.Summary.TotalWritten)
	}
	if result.Summary.TotalFailed > 0 {
		t.Errorf("expected 0 failed, got %d", result.Summary.TotalFailed)
	}

	dstProvider.mu.Lock()
	totalWritten := 0
	for _, batch := range dstProvider.written {
		totalWritten += len(batch)
	}
	dstProvider.mu.Unlock()

	if totalWritten != 50 {
		t.Errorf("destination received %d units, expected 50", totalWritten)
	}
}

func TestE2E_ConcurrentWriters(t *testing.T) {
	srcProvider := &stubProvider{
		name:  "stub_src2",
		units: makeTestUnits(100, "orders"),
	}
	dstProvider := &stubProvider{
		name: "stub_dst2",
	}

	provider.Register("stub_src2", func() provider.Provider { return srcProvider })
	provider.Register("stub_dst2", func() provider.Provider { return dstProvider })
	transform.RegisterTransformer("stub_src2", "stub_dst2", func() transform.Transformer { return transform.NoopTransformer{} })

	cfg := &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "stub_src2"},
		Destination: config.ConnectionConfig{Provider: "stub_dst2"},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: t.TempDir() + "/cp.json"},
	}

	opts := PipelineOptions{
		BatchSize:          10,
		Verify:             false,
		ConflictStrategy:   provider.ConflictOverwrite,
		FKHandling:         "skip",
		MigrateSchema:      false,
		MaxRetries:         1,
		RetryBackoff:       0,
		Parallel:           4,
		WriteWorkers:       4,
		MaxBatchBytes:      0,
		MaxWrittenKeys:     10000,
		CheckpointInterval: 0,
		CheckpointEnabled:  true,
		VerifyMode:         "cross",
	}

	pipeline, err := NewPipeline(cfg, opts, &noopReporter{}, nil)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	result, err := pipeline.Run(context.Background())
	if err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}

	if result.Summary.TotalWritten != 100 {
		t.Errorf("expected 100 written with 4 workers, got %d", result.Summary.TotalWritten)
	}
}

func TestE2E_BatchByteSplitting(t *testing.T) {
	units := make([]provider.MigrationUnit, 10)
	for i := range units {
		units[i] = provider.MigrationUnit{
			Key:   string(rune('a' + i)),
			Table: "bigtable",
			Size:  100, // 100 bytes each
			Data:  make([]byte, 100),
		}
	}

	srcProvider := &stubProvider{name: "stub_src3", units: units}
	dstProvider := &stubProvider{name: "stub_dst3"}

	provider.Register("stub_src3", func() provider.Provider { return srcProvider })
	provider.Register("stub_dst3", func() provider.Provider { return dstProvider })
	transform.RegisterTransformer("stub_src3", "stub_dst3", func() transform.Transformer { return transform.NoopTransformer{} })

	cfg := &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "stub_src3"},
		Destination: config.ConnectionConfig{Provider: "stub_dst3"},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: t.TempDir() + "/cp.json"},
	}

	opts := PipelineOptions{
		BatchSize:          100, // high count limit
		Verify:             false,
		ConflictStrategy:   provider.ConflictOverwrite,
		FKHandling:         "skip",
		MigrateSchema:      false,
		MaxRetries:         1,
		RetryBackoff:       0,
		Parallel:           4,
		WriteWorkers:       1,
		MaxBatchBytes:      250, // should split into batches of ~2-3 units
		MaxWrittenKeys:     10000,
		CheckpointInterval: 0,
		CheckpointEnabled:  true,
		VerifyMode:         "cross",
	}

	pipeline, err := NewPipeline(cfg, opts, &noopReporter{}, nil)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	result, err := pipeline.Run(context.Background())
	if err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}

	if result.Summary.TotalWritten != 10 {
		t.Errorf("expected 10 written, got %d", result.Summary.TotalWritten)
	}

	// With 250 byte budget and 100 bytes per unit, each batch should have 2-3 units
	dstProvider.mu.Lock()
	numBatches := len(dstProvider.written)
	dstProvider.mu.Unlock()

	if numBatches <= 1 {
		t.Errorf("expected batch splitting, got only %d batch(es)", numBatches)
	}
}

func TestE2E_CheckpointResume(t *testing.T) {
	cpPath := t.TempDir() + "/cp.json"

	// First run: migrate 30 units, save checkpoint
	srcProvider := &stubProvider{
		name:  "stub_src4",
		units: makeTestUnits(30, "items"),
	}
	dstProvider := &stubProvider{name: "stub_dst4"}

	provider.Register("stub_src4", func() provider.Provider { return srcProvider })
	provider.Register("stub_dst4", func() provider.Provider { return dstProvider })
	transform.RegisterTransformer("stub_src4", "stub_dst4", func() transform.Transformer { return transform.NoopTransformer{} })

	cfg := &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "stub_src4"},
		Destination: config.ConnectionConfig{Provider: "stub_dst4"},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: cpPath},
	}

	opts := PipelineOptions{
		BatchSize:          10,
		Verify:             false,
		ConflictStrategy:   provider.ConflictOverwrite,
		FKHandling:         "skip",
		MigrateSchema:      false,
		MaxRetries:         1,
		RetryBackoff:       0,
		Parallel:           2,
		WriteWorkers:       1,
		MaxBatchBytes:      0,
		MaxWrittenKeys:     10000,
		CheckpointInterval: 1,
		CheckpointEnabled:  true,
		VerifyMode:         "cross",
	}

	// Run 1: full migration
	pipeline, err := NewPipeline(cfg, opts, &noopReporter{}, nil)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	result, err := pipeline.Run(context.Background())
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if result.Summary.TotalWritten != 30 {
		t.Errorf("run 1: expected 30 written, got %d", result.Summary.TotalWritten)
	}

	// Run 2: with --resume, the checkpoint should be loaded and cleared
	opts.Resume = true
	pipeline2, err := NewPipeline(cfg, opts, &noopReporter{}, nil)
	if err != nil {
		t.Fatalf("create pipeline2: %v", err)
	}
	result2, err := pipeline2.Run(context.Background())
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	// Since checkpoint was from same config and the scanner is fresh,
	// it will re-migrate all 30 units (resume uses scanner from fresh provider)
	if result2.Summary.TotalWritten != 30 {
		t.Errorf("run 2: expected 30 written on re-scan, got %d", result2.Summary.TotalWritten)
	}
}

func TestE2E_Cancelled(t *testing.T) {
	srcProvider := &stubProvider{
		name:  "stub_src5",
		units: makeTestUnits(1000, "big"),
	}
	dstProvider := &stubProvider{name: "stub_dst5"}

	provider.Register("stub_src5", func() provider.Provider { return srcProvider })
	provider.Register("stub_dst5", func() provider.Provider { return dstProvider })
	transform.RegisterTransformer("stub_src5", "stub_dst5", func() transform.Transformer { return transform.NoopTransformer{} })

	cfg := &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "stub_src5"},
		Destination: config.ConnectionConfig{Provider: "stub_dst5"},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: t.TempDir() + "/cp.json"},
	}

	opts := PipelineOptions{
		BatchSize:          10,
		Verify:             false,
		ConflictStrategy:   provider.ConflictOverwrite,
		FKHandling:         "skip",
		MigrateSchema:      false,
		MaxRetries:         1,
		RetryBackoff:       0,
		Parallel:           2,
		WriteWorkers:       1,
		MaxBatchBytes:      0,
		MaxWrittenKeys:     10000,
		CheckpointInterval: 0,
		CheckpointEnabled:  true,
		VerifyMode:         "cross",
	}

	ctx, cancel := context.WithCancel(context.Background())

	pipeline, err := NewPipeline(cfg, opts, &noopReporter{}, nil)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}

	// Cancel from a goroutine after the first write completes.
	go func() {
		for {
			dstProvider.mu.Lock()
			n := len(dstProvider.written)
			dstProvider.mu.Unlock()
			if n >= 1 {
				cancel()
				return
			}
		}
	}()

	_, err = pipeline.Run(ctx)
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

func TestE2E_PauseResume(t *testing.T) {
	srcProvider := &stubProvider{
		name:  "stub_src6",
		units: makeTestUnits(50, "data"),
	}
	dstProvider := &stubProvider{name: "stub_dst6"}

	provider.Register("stub_src6", func() provider.Provider { return srcProvider })
	provider.Register("stub_dst6", func() provider.Provider { return dstProvider })
	transform.RegisterTransformer("stub_src6", "stub_dst6", func() transform.Transformer { return transform.NoopTransformer{} })

	cfg := &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "stub_src6"},
		Destination: config.ConnectionConfig{Provider: "stub_dst6"},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: t.TempDir() + "/cp.json"},
	}

	opts := PipelineOptions{
		BatchSize:          10,
		Verify:             false,
		ConflictStrategy:   provider.ConflictOverwrite,
		FKHandling:         "skip",
		MigrateSchema:      false,
		MaxRetries:         1,
		RetryBackoff:       0,
		Parallel:           2,
		WriteWorkers:       1,
		MaxBatchBytes:      0,
		MaxWrittenKeys:     10000,
		CheckpointInterval: 0,
		CheckpointEnabled:  true,
		VerifyMode:         "cross",
	}

	pipeline, err := NewPipeline(cfg, opts, &noopReporter{}, nil)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}

	// Pause and resume in a goroutine
	go func() {
		pipeline.Pause()
		pipeline.Resume()
	}()

	result, err := pipeline.Run(context.Background())
	if err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}
	if result.Summary.TotalWritten != 50 {
		t.Errorf("expected 50 written after pause/resume, got %d", result.Summary.TotalWritten)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func makeTestUnits(n int, table string) []provider.MigrationUnit {
	units := make([]provider.MigrationUnit, n)
	for i := range units {
		units[i] = provider.MigrationUnit{
			Key:      table + ":" + string(rune('0'+i%10)) + string(rune('0'+i/10)),
			Table:    table,
			DataType: provider.DataTypeRow,
			Size:     50,
			Data:     []byte(`{"id":` + string(rune('0'+i%10)) + `}`),
		}
	}
	return units
}

type noopReporter struct{}

func (n noopReporter) OnBatchStart(int, int)                                {}
func (n noopReporter) OnBatchComplete(int, *provider.BatchResult)           {}
func (n noopReporter) OnMigrationComplete(*provider.MigrationSummary)       {}
func (n noopReporter) OnError(error, *provider.MigrationUnit)               {}
func (n noopReporter) OnPhaseChange(provider.MigrationPhase)                {}
func (n noopReporter) OnPhaseStart(provider.PhaseDesc)                      {}
func (n noopReporter) OnPhaseDone(provider.PhaseDesc, time.Duration, error) {}
func (n noopReporter) OnProgress(provider.ProgressStats)                    {}
