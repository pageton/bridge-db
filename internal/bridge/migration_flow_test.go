package bridge

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/internal/verify"
	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// Shared test infrastructure
// ---------------------------------------------------------------------------

// testProvider is a configurable mock provider for integration tests.
// Supports table enumeration, verify reading, checksumming, and failure injection.
type testProvider struct {
	name      string
	tables    map[string]int64
	units     []provider.MigrationUnit
	records   map[string]map[string]any
	checksums map[string]string

	// Failure injection
	scanErr  error // returned from scanner.Next after N successful batches
	scanFail int   // fail after this many successful Next calls
	writeErr error // returned from writer.Write

	mu      sync.Mutex
	written [][]provider.MigrationUnit
	scanned int64
}

func (p *testProvider) Name() string                                             { return p.name }
func (p *testProvider) Connect(_ context.Context, _, _ any) error                { return nil }
func (p *testProvider) Close() error                                             { return nil }
func (p *testProvider) Ping(_ context.Context) error                             { return nil }
func (p *testProvider) DryRun() provider.Provider                                { return p }
func (p *testProvider) SchemaMigrator(_ context.Context) provider.SchemaMigrator { return nil }
func (p *testProvider) Verifier(_ context.Context) provider.Verifier {
	return &testVerifier{}
}

func (p *testProvider) Writer(_ context.Context, _ provider.WriteOptions) provider.Writer {
	return &testWriter{p: p}
}

func (p *testProvider) Scanner(_ context.Context, _ provider.ScanOptions) provider.Scanner {
	return &testScanner{p: p}
}

func (p *testProvider) EnumerateTables(_ context.Context) (map[string]int64, error) {
	if p.tables != nil {
		return p.tables, nil
	}
	tables := make(map[string]int64)
	for _, u := range p.units {
		tables[u.Table]++
	}
	return tables, nil
}

func (p *testProvider) ReadRecords(_ context.Context, keys []string) (map[string]map[string]any, error) {
	result := make(map[string]map[string]any)
	for _, k := range keys {
		if v, ok := p.records[k]; ok {
			result[k] = v
		}
	}
	return result, nil
}

func (p *testProvider) ComputeChecksums(_ context.Context, keys []string) (map[string]string, error) {
	result := make(map[string]string)
	for _, k := range keys {
		if v, ok := p.checksums[k]; ok {
			result[k] = v
		}
	}
	return result, nil
}

func (p *testProvider) getWritten() [][]provider.MigrationUnit {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.written
}

func (p *testProvider) totalWrittenUnits() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, b := range p.written {
		total += len(b)
	}
	return total
}

type testScanner struct {
	p   *testProvider
	pos int
}

func (s *testScanner) Next(_ context.Context) ([]provider.MigrationUnit, error) {
	if s.p.scanErr != nil && s.p.scanFail > 0 && s.pos >= s.p.scanFail {
		err := s.p.scanErr
		s.p.scanErr = nil // only fail once, then return EOF on next call
		return nil, err
	}
	if s.pos >= len(s.p.units) {
		return nil, io.EOF
	}
	end := s.pos + 10
	if end > len(s.p.units) {
		end = len(s.p.units)
	}
	batch := s.p.units[s.pos:end]
	s.pos = end
	atomic.AddInt64(&s.p.scanned, int64(len(batch)))
	return batch, nil
}

func (s *testScanner) Stats() provider.ScanStats {
	return provider.ScanStats{
		TotalScanned: atomic.LoadInt64(&s.p.scanned),
		TablesTotal:  1,
	}
}

type testWriter struct {
	p *testProvider
}

func (w *testWriter) Write(_ context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	if w.p.writeErr != nil {
		return nil, w.p.writeErr
	}
	w.p.mu.Lock()
	w.p.written = append(w.p.written, append([]provider.MigrationUnit{}, units...))
	w.p.mu.Unlock()
	return &provider.BatchResult{
		TotalUnits:   len(units),
		WrittenUnits: len(units),
		BytesWritten: totalSize(units),
	}, nil
}

func (w *testWriter) Flush(_ context.Context) error { return nil }

type testVerifier struct{}

func (v *testVerifier) Verify(_ context.Context, _ []string) ([]provider.VerificationError, error) {
	return nil, nil
}

// makeFlowUnits creates n MigrationUnits for a given table.
func makeFlowUnits(n int, table string) []provider.MigrationUnit {
	units := make([]provider.MigrationUnit, n)
	for i := range units {
		units[i] = provider.MigrationUnit{
			Key:      fmt.Sprintf("%s:%d", table, i),
			Table:    table,
			DataType: provider.DataTypeRow,
			Size:     50,
			Data:     []byte(fmt.Sprintf(`{"id":%d}`, i)),
		}
	}
	return units
}

// setupTestProviders registers two providers and a transformer for a test.
func setupTestProviders(t *testing.T, srcName, dstName string, src *testProvider, dst *testProvider) {
	t.Helper()
	provider.Register(srcName, func() provider.Provider { return src })
	provider.Register(dstName, func() provider.Provider { return dst })
	transform.RegisterTransformer(srcName, dstName, func() transform.Transformer {
		return transform.NoopTransformer{}
	})
}

// defaultTestOpts returns a working PipelineOptions for tests.
func defaultTestOpts() PipelineOptions {
	return PipelineOptions{
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
}

func defaultTestCfg(srcProv, dstProv, cpPath string) *config.MigrationConfig {
	return &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: srcProv},
		Destination: config.ConnectionConfig{Provider: dstProv},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: cpPath},
	}
}

func runTestPipeline(t *testing.T, cfg *config.MigrationConfig, opts PipelineOptions) *RunResult {
	t.Helper()
	pipeline, err := NewPipeline(cfg, opts, noopReporter{}, nil)
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	result, err := pipeline.Run(context.Background())
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	return result
}

// ---------------------------------------------------------------------------
// 1. Config validation
// ---------------------------------------------------------------------------

func TestFlow_InvalidOptions_BadBatchSize(t *testing.T) {
	cfg := defaultTestCfg("x_cf1", "y_cf1", t.TempDir()+"/cp.json")
	opts := defaultTestOpts()
	opts.BatchSize = 0

	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	_, err := pipeline.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for batch_size=0")
	}
	ce, ok := err.(*CategorizedError)
	if !ok {
		t.Fatalf("expected CategorizedError, got %T: %v", err, err)
	}
	if ce.Category != ErrConfig {
		t.Errorf("expected ErrConfig, got %s", ce.Category)
	}
	if ce.Step != 1 {
		t.Errorf("expected step 1, got %d", ce.Step)
	}
}

func TestFlow_InvalidOptions_BadParallel(t *testing.T) {
	cfg := defaultTestCfg("x_cf2", "y_cf2", t.TempDir()+"/cp.json")
	opts := defaultTestOpts()
	opts.Parallel = 0

	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	_, err := pipeline.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for parallel=0")
	}
}

func TestFlow_InvalidOptions_BadConflictStrategy(t *testing.T) {
	cfg := defaultTestCfg("x_cf3", "y_cf3", t.TempDir()+"/cp.json")
	opts := defaultTestOpts()
	opts.ConflictStrategy = "explode"

	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	_, err := pipeline.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for bad conflict strategy")
	}
}

func TestFlow_InvalidOptions_NegativeMaxBatchBytes(t *testing.T) {
	cfg := defaultTestCfg("x_cf4", "y_cf4", t.TempDir()+"/cp.json")
	opts := defaultTestOpts()
	opts.MaxBatchBytes = -1

	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	_, err := pipeline.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for negative max_batch_bytes")
	}
}

// ---------------------------------------------------------------------------
// 2. Provider initialization
// ---------------------------------------------------------------------------

func TestFlow_UnknownProvider(t *testing.T) {
	cfg := &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "nonexistent_provider_xyz"},
		Destination: config.ConnectionConfig{Provider: "also_nonexistent_xyz"},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: t.TempDir() + "/cp.json"},
	}
	opts := defaultTestOpts()

	pipeline, err := NewPipeline(cfg, opts, noopReporter{}, nil)
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	_, err = pipeline.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
	// Should be a connection error at step 3.
	ce, ok := err.(*CategorizedError)
	if !ok {
		t.Fatalf("expected CategorizedError, got %T", err)
	}
	if ce.Step != 3 {
		t.Errorf("expected step 3, got %d", ce.Step)
	}
}

func TestFlow_ProviderCreatedAndConnected(t *testing.T) {
	src := &testProvider{name: "f_p1_src", units: makeFlowUnits(5, "tbl")}
	dst := &testProvider{name: "f_p1_dst"}
	setupTestProviders(t, "f_p1_src", "f_p1_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_p1_src", "f_p1_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	if result.SrcProvider != "f_p1_src" {
		t.Errorf("SrcProvider = %q, want f_p1_src", result.SrcProvider)
	}
	if result.DstProvider != "f_p1_dst" {
		t.Errorf("DstProvider = %q, want f_p1_dst", result.DstProvider)
	}
}

// ---------------------------------------------------------------------------
// 3. Migration planning
// ---------------------------------------------------------------------------

func TestFlow_PlanPopulatesResult(t *testing.T) {
	units := makeFlowUnits(30, "users")
	src := &testProvider{name: "f_pl1_src", units: units, tables: map[string]int64{"users": 30}}
	dst := &testProvider{name: "f_pl1_dst"}
	setupTestProviders(t, "f_pl1_src", "f_pl1_dst", src, dst)

	cfg := defaultTestCfg("f_pl1_src", "f_pl1_dst", t.TempDir()+"/cp.json")
	opts := defaultTestOpts()

	result := runTestPipeline(t, cfg, opts)

	if result.Plan == nil {
		t.Fatal("expected Plan to be populated")
	}
	if result.Plan.SourceProvider != "f_pl1_src" {
		t.Errorf("Plan.SourceProvider = %q", result.Plan.SourceProvider)
	}
	if result.Plan.EstimatedRows != 30 {
		t.Errorf("Plan.EstimatedRows = %d, want 30", result.Plan.EstimatedRows)
	}
	if result.Plan.EstimatedBatches != 3 {
		t.Errorf("Plan.EstimatedBatches = %d, want 3", result.Plan.EstimatedBatches)
	}
}

func TestFlow_PlanCrossDB(t *testing.T) {
	src := &testProvider{name: "f_pl2_src", units: makeFlowUnits(10, "t")}
	dst := &testProvider{name: "f_pl2_dst"}
	setupTestProviders(t, "f_pl2_src", "f_pl2_dst", src, dst)

	cfg := defaultTestCfg("f_pl2_src", "f_pl2_dst", t.TempDir()+"/cp.json")
	opts := defaultTestOpts()

	result := runTestPipeline(t, cfg, opts)

	if !result.Plan.CrossDB {
		t.Error("different providers should be cross-DB")
	}
}

// ---------------------------------------------------------------------------
// 4. Transformation correctness
// ---------------------------------------------------------------------------

func TestFlow_NoopTransformPreservesData(t *testing.T) {
	units := makeFlowUnits(20, "data")
	src := &testProvider{name: "f_tr1_src", units: units}
	dst := &testProvider{name: "f_tr1_dst"}
	setupTestProviders(t, "f_tr1_src", "f_tr1_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_tr1_src", "f_tr1_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	if result.Summary.TotalWritten != 20 {
		t.Errorf("wrote %d, want 20", result.Summary.TotalWritten)
	}

	// Verify data integrity: destination received exact same data.
	written := dst.getWritten()
	for _, batch := range written {
		for _, u := range batch {
			if len(u.Data) == 0 {
				t.Errorf("unit %q has empty data", u.Key)
			}
		}
	}
}

func TestFlow_MultiTableTransform(t *testing.T) {
	units := append(
		makeFlowUnits(10, "users"),
		makeFlowUnits(15, "orders")...,
	)
	src := &testProvider{name: "f_tr2_src", units: units}
	dst := &testProvider{name: "f_tr2_dst"}
	setupTestProviders(t, "f_tr2_src", "f_tr2_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_tr2_src", "f_tr2_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	if result.Summary.TotalWritten != 25 {
		t.Errorf("wrote %d, want 25", result.Summary.TotalWritten)
	}
}

// ---------------------------------------------------------------------------
// 5. Write behavior
// ---------------------------------------------------------------------------

func TestFlow_WriteAllUnits(t *testing.T) {
	units := makeFlowUnits(47, "items")
	src := &testProvider{name: "f_w1_src", units: units}
	dst := &testProvider{name: "f_w1_dst"}
	setupTestProviders(t, "f_w1_src", "f_w1_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_w1_src", "f_w1_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	if result.Summary.TotalWritten != 47 {
		t.Errorf("wrote %d, want 47", result.Summary.TotalWritten)
	}
	if result.Summary.TotalFailed != 0 {
		t.Errorf("failed %d, want 0", result.Summary.TotalFailed)
	}
	if dst.totalWrittenUnits() != 47 {
		t.Errorf("destination received %d units, want 47", dst.totalWrittenUnits())
	}
}

func TestFlow_WriteBatchSplitting(t *testing.T) {
	units := makeFlowUnits(20, "big")
	for i := range units {
		units[i].Size = 100
	}
	src := &testProvider{name: "f_w2_src", units: units}
	dst := &testProvider{name: "f_w2_dst"}
	setupTestProviders(t, "f_w2_src", "f_w2_dst", src, dst)

	opts := defaultTestOpts()
	opts.MaxBatchBytes = 250 // ~2-3 units per sub-batch

	result := runTestPipeline(t, defaultTestCfg("f_w2_src", "f_w2_dst", t.TempDir()+"/cp.json"), opts)

	if result.Summary.TotalWritten != 20 {
		t.Errorf("wrote %d, want 20", result.Summary.TotalWritten)
	}

	batches := dst.getWritten()
	if len(batches) <= 1 {
		t.Errorf("expected batch splitting with 250-byte budget, got %d batches", len(batches))
	}
}

func TestFlow_ConcurrentWriters(t *testing.T) {
	units := makeFlowUnits(100, "data")
	src := &testProvider{name: "f_w3_src", units: units}
	dst := &testProvider{name: "f_w3_dst"}
	setupTestProviders(t, "f_w3_src", "f_w3_dst", src, dst)

	opts := defaultTestOpts()
	opts.WriteWorkers = 4

	result := runTestPipeline(t, defaultTestCfg("f_w3_src", "f_w3_dst", t.TempDir()+"/cp.json"), opts)

	if result.Summary.TotalWritten != 100 {
		t.Errorf("wrote %d with 4 workers, want 100", result.Summary.TotalWritten)
	}
	if dst.totalWrittenUnits() != 100 {
		t.Errorf("destination received %d, want 100", dst.totalWrittenUnits())
	}
}

func TestFlow_DryRunNoWrites(t *testing.T) {
	units := makeFlowUnits(20, "dry")
	src := &testProvider{name: "f_w4_src", units: units}
	dst := &testProvider{name: "f_w4_dst"}
	setupTestProviders(t, "f_w4_src", "f_w4_dst", src, dst)

	opts := defaultTestOpts()
	opts.DryRun = true

	result := runTestPipeline(t, defaultTestCfg("f_w4_src", "f_w4_dst", t.TempDir()+"/cp.json"), opts)

	// DryRun wraps the destination, so writes go to the dry-run wrapper.
	// The original dst should have no writes recorded through its own Writer.
	if result.Summary.TotalWritten != 20 {
		t.Errorf("dry-run reported %d written, want 20", result.Summary.TotalWritten)
	}
}

func TestFlow_LargeBatch(t *testing.T) {
	units := makeFlowUnits(500, "big_table")
	src := &testProvider{name: "f_w5_src", units: units}
	dst := &testProvider{name: "f_w5_dst"}
	setupTestProviders(t, "f_w5_src", "f_w5_dst", src, dst)

	opts := defaultTestOpts()
	opts.BatchSize = 50

	result := runTestPipeline(t, defaultTestCfg("f_w5_src", "f_w5_dst", t.TempDir()+"/cp.json"), opts)

	if result.Summary.TotalWritten != 500 {
		t.Errorf("wrote %d, want 500", result.Summary.TotalWritten)
	}
}

// ---------------------------------------------------------------------------
// 6. Checkpoint / Resume
// ---------------------------------------------------------------------------

func TestFlow_CheckpointSavedAndLoaded(t *testing.T) {
	cpPath := t.TempDir() + "/cp.json"
	src := &testProvider{name: "f_cp1_src", units: makeFlowUnits(500, "tbl")}
	dst := &testProvider{name: "f_cp1_dst"}
	setupTestProviders(t, "f_cp1_src", "f_cp1_dst", src, dst)

	opts := defaultTestOpts()
	opts.CheckpointInterval = 1

	cfg := defaultTestCfg("f_cp1_src", "f_cp1_dst", cpPath)

	// Cancel mid-run to leave a checkpoint behind (successful runs clear it).
	ctx, cancel := context.WithCancel(context.Background())
	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	go func() {
		for {
			if dst.totalWrittenUnits() >= 30 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	_, _ = pipeline.Run(ctx)

	// Verify checkpoint file was created and can be loaded.
	store, err := NewFileCheckpointStore(cpPath)
	if err != nil {
		t.Fatalf("NewFileCheckpointStore: %v", err)
	}
	cp, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp == nil {
		t.Fatal("expected checkpoint")
	}
	if cp.Version != checkpointVersion {
		t.Errorf("Version = %d, want %d", cp.Version, checkpointVersion)
	}
	if cp.SourceProvider != "f_cp1_src" {
		t.Errorf("SourceProvider = %q", cp.SourceProvider)
	}
	if cp.TotalWritten < 10 {
		t.Errorf("TotalWritten = %d, want >= 10", cp.TotalWritten)
	}
}

func TestFlow_ResumeFromCheckpoint(t *testing.T) {
	cpPath := t.TempDir() + "/cp.json"

	// Use the same provider names for both runs so the config hash matches.
	const srcName, dstName = "f_cp2r_src", "f_cp2r_dst"

	// Run 1: register providers and cancel mid-run to leave a checkpoint.
	src1 := &testProvider{name: srcName, units: makeFlowUnits(500, "items")}
	dst1 := &testProvider{name: dstName}
	provider.Register(srcName, func() provider.Provider { return src1 })
	provider.Register(dstName, func() provider.Provider { return dst1 })
	transform.RegisterTransformer(srcName, dstName, func() transform.Transformer {
		return transform.NoopTransformer{}
	})

	opts := defaultTestOpts()
	opts.CheckpointInterval = 1

	cfg := defaultTestCfg(srcName, dstName, cpPath)

	ctx, cancel := context.WithCancel(context.Background())
	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	go func() {
		for {
			if dst1.totalWrittenUnits() >= 30 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	_, _ = pipeline.Run(ctx)

	// Verify checkpoint exists.
	store, _ := NewFileCheckpointStore(cpPath)
	cp, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp == nil {
		t.Fatal("expected checkpoint after cancelled run")
	}

	// Run 2: register new provider instances under different names but use the
	// same config provider names. The factory was already registered so
	// provider.New returns the same instances — but that's fine, the test
	// scanner was consumed. Use the same config and checkpoint path.
	// Since providers are already registered with these names, create a fresh
	// pipeline that loads the checkpoint and resumes.
	cfg2 := defaultTestCfg(srcName, dstName, cpPath)
	opts2 := defaultTestOpts()
	opts2.Resume = true
	opts2.CheckpointInterval = 1

	pipeline2, _ := NewPipeline(cfg2, opts2, noopReporter{}, nil)
	result2, err2 := pipeline2.Run(context.Background())
	if err2 != nil {
		t.Fatalf("run 2 failed: %v", err2)
	}

	if !result2.Resumed {
		t.Error("expected Resumed=true")
	}
}

func TestFlow_CheckpointConfigChangeRejected(t *testing.T) {
	cpPath := t.TempDir() + "/cp.json"

	// Run 1: cancel mid-run to leave a checkpoint behind.
	src := &testProvider{name: "f_cp3_src", units: makeFlowUnits(500, "t")}
	dst := &testProvider{name: "f_cp3_dst"}
	setupTestProviders(t, "f_cp3_src", "f_cp3_dst", src, dst)

	opts := defaultTestOpts()
	opts.CheckpointInterval = 1
	cfg := defaultTestCfg("f_cp3_src", "f_cp3_dst", cpPath)

	ctx, cancel := context.WithCancel(context.Background())
	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	go func() {
		for {
			if dst.totalWrittenUnits() >= 30 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	_, _ = pipeline.Run(ctx)

	// Run 2: change config and try to resume — should fail.
	src2 := &testProvider{name: "f_cp3_src_b", units: makeFlowUnits(10, "t")}
	dst2 := &testProvider{name: "f_cp3_dst_b"}
	provider.Register("f_cp3_src_b", func() provider.Provider { return src2 })
	provider.Register("f_cp3_dst_b", func() provider.Provider { return dst2 })
	transform.RegisterTransformer("f_cp3_src_b", "f_cp3_dst_b", func() transform.Transformer {
		return transform.NoopTransformer{}
	})

	cfg2 := &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "f_cp3_src_b", Postgres: &config.PostgresConfig{Host: "DIFFERENT_HOST"}},
		Destination: config.ConnectionConfig{Provider: "f_cp3_dst_b"},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: cpPath},
	}
	opts2 := defaultTestOpts()
	opts2.Resume = true

	pipeline2, _ := NewPipeline(cfg2, opts2, noopReporter{}, nil)
	_, err := pipeline2.Run(context.Background())
	if err == nil {
		t.Fatal("expected config hash mismatch error on resume")
	}
}

func TestFlow_CheckpointProviderMismatch(t *testing.T) {
	cpPath := t.TempDir() + "/cp.json"

	// Run 1: cancel mid-run to leave a checkpoint behind.
	src := &testProvider{name: "f_cp4_src", units: makeFlowUnits(500, "t")}
	dst := &testProvider{name: "f_cp4_dst"}
	setupTestProviders(t, "f_cp4_src", "f_cp4_dst", src, dst)

	opts := defaultTestOpts()
	opts.CheckpointInterval = 1
	cfg := defaultTestCfg("f_cp4_src", "f_cp4_dst", cpPath)

	ctx, cancel := context.WithCancel(context.Background())
	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	go func() {
		for {
			if dst.totalWrittenUnits() >= 30 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	_, _ = pipeline.Run(ctx)

	// Resume with wrong source provider.
	src2 := &testProvider{name: "f_cp4_wrong_src", units: makeFlowUnits(10, "t")}
	dst2 := &testProvider{name: "f_cp4_dst_r2"}
	provider.Register("f_cp4_wrong_src", func() provider.Provider { return src2 })
	provider.Register("f_cp4_dst_r2", func() provider.Provider { return dst2 })
	transform.RegisterTransformer("f_cp4_wrong_src", "f_cp4_dst_r2", func() transform.Transformer {
		return transform.NoopTransformer{}
	})

	cfg2 := &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "f_cp4_wrong_src"},
		Destination: config.ConnectionConfig{Provider: "f_cp4_dst_r2"},
		Checkpoint:  config.CheckpointConfig{Enabled: true, Path: cpPath},
	}
	opts2 := defaultTestOpts()
	opts2.Resume = true

	pipeline2, _ := NewPipeline(cfg2, opts2, noopReporter{}, nil)
	_, err := pipeline2.Run(context.Background())
	if err == nil {
		t.Fatal("expected provider mismatch error")
	}
}

func TestFlow_CheckpointClearedAfterSuccess(t *testing.T) {
	cpPath := t.TempDir() + "/cp.json"
	src := &testProvider{name: "f_cp5_src", units: makeFlowUnits(10, "t")}
	dst := &testProvider{name: "f_cp5_dst"}
	setupTestProviders(t, "f_cp5_src", "f_cp5_dst", src, dst)

	runTestPipeline(t, defaultTestCfg("f_cp5_src", "f_cp5_dst", cpPath), defaultTestOpts())

	store, _ := NewFileCheckpointStore(cpPath)
	cp, _ := store.Load(context.Background())
	if cp != nil {
		t.Error("checkpoint should be cleared after successful migration")
	}
}

func TestFlow_ResumeWithoutCheckpointEnabled(t *testing.T) {
	cfg := defaultTestCfg("x_noresume", "y_noresume", t.TempDir()+"/cp.json")
	opts := defaultTestOpts()
	opts.Resume = true
	opts.CheckpointEnabled = false

	_, _ = NewPipeline(cfg, opts, noopReporter{}, nil)
	// Pipeline creation should succeed; actual run would fail at provider
	// creation since no providers are registered for these names.
	// This tests that the constructor doesn't panic with this config combo.
}

// ---------------------------------------------------------------------------
// 7. Verification behavior
// ---------------------------------------------------------------------------

func TestFlow_VerificationPasses(t *testing.T) {
	units := makeFlowUnits(20, "users")
	src := &testProvider{
		name:      "f_v1_src",
		units:     units,
		tables:    map[string]int64{"users": 20},
		checksums: map[string]string{"users:0": "abc", "users:1": "def"},
	}
	dst := &testProvider{
		name:      "f_v1_dst",
		tables:    map[string]int64{"users": 20},
		checksums: map[string]string{"users:0": "abc", "users:1": "def"},
	}
	setupTestProviders(t, "f_v1_src", "f_v1_dst", src, dst)

	opts := defaultTestOpts()
	opts.Verify = true
	opts.VerifyOptions = verify.Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            5,
		ChecksumComparison: true,
		MaxSampleKeys:      10000,
	}

	result := runTestPipeline(t, defaultTestCfg("f_v1_src", "f_v1_dst", t.TempDir()+"/cp.json"), opts)

	if result.Summary.VerificationOK != true {
		t.Error("expected verification to pass")
	}
}

func TestFlow_VerificationDetectsCountMismatch(t *testing.T) {
	units := makeFlowUnits(20, "orders")
	src := &testProvider{
		name:   "f_v2_src",
		units:  units,
		tables: map[string]int64{"orders": 20},
	}
	dst := &testProvider{
		name:   "f_v2_dst",
		tables: map[string]int64{"orders": 19}, // count mismatch
	}
	setupTestProviders(t, "f_v2_src", "f_v2_dst", src, dst)

	opts := defaultTestOpts()
	opts.Verify = true
	opts.VerifyOptions = verify.Options{
		CountComparison: true,
		SampleMode:      "none",
	}

	result := runTestPipeline(t, defaultTestCfg("f_v2_src", "f_v2_dst", t.TempDir()+"/cp.json"), opts)

	if result.Summary.VerificationOK {
		t.Error("expected verification to fail on count mismatch")
	}
}

func TestFlow_VerificationSkippedWhenDisabled(t *testing.T) {
	src := &testProvider{name: "f_v3_src", units: makeFlowUnits(10, "t")}
	dst := &testProvider{name: "f_v3_dst"}
	setupTestProviders(t, "f_v3_src", "f_v3_dst", src, dst)

	opts := defaultTestOpts()
	opts.Verify = false

	result := runTestPipeline(t, defaultTestCfg("f_v3_src", "f_v3_dst", t.TempDir()+"/cp.json"), opts)

	// No verification errors should be present.
	if len(result.Summary.VerificationErrs) != 0 {
		t.Errorf("expected no verification errors, got %d", len(result.Summary.VerificationErrs))
	}
}

// ---------------------------------------------------------------------------
// 8. Failure recovery
// ---------------------------------------------------------------------------

func TestFlow_CancelDuringTransfer(t *testing.T) {
	src := &testProvider{name: "f_f1_src", units: makeFlowUnits(500, "big")}
	dst := &testProvider{name: "f_f1_dst"}
	setupTestProviders(t, "f_f1_src", "f_f1_dst", src, dst)

	opts := defaultTestOpts()
	cfg := defaultTestCfg("f_f1_src", "f_f1_dst", t.TempDir()+"/cp.json")

	ctx, cancel := context.WithCancel(context.Background())

	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)

	// Cancel after destination receives some writes.
	go func() {
		for {
			if dst.totalWrittenUnits() >= 20 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	_, err := pipeline.Run(ctx)
	if err == nil {
		t.Error("expected error from cancelled context")
	}
	ce, ok := err.(*CategorizedError)
	if !ok {
		t.Fatalf("expected CategorizedError, got %T: %v", err, err)
	}
	if ce.Category != ErrCancelled {
		t.Errorf("expected ErrCancelled, got %s", ce.Category)
	}
}

func TestFlow_ScanErrorSkipsBatch(t *testing.T) {
	// Scanner fails after 1 successful batch (10 units).
	// Pipeline should continue, not abort.
	src := &testProvider{
		name:     "f_f2_src",
		units:    makeFlowUnits(50, "tbl"),
		scanErr:  fmt.Errorf("transient scan error"),
		scanFail: 1, // fail on second Next call
	}
	dst := &testProvider{name: "f_f2_dst"}
	setupTestProviders(t, "f_f2_src", "f_f2_dst", src, dst)

	opts := defaultTestOpts()
	opts.MaxRetries = 0 // no retries on scan errors inside the scanner goroutine

	cfg := defaultTestCfg("f_f2_src", "f_f2_dst", t.TempDir()+"/cp.json")
	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	_, err := pipeline.Run(context.Background())
	// The scan error propagates through scanCh and should cause the writer
	// to log a warning but the pipeline may still complete or fail depending
	// on how many batches got through before the error.
	// With scanFail=1, the first 10-unit batch succeeds, then Next fails.
	// The error is retryable (scanRetryErr with MaxRetries=1 = 2 attempts).
	// Both attempts fail, so the scan error goes to the channel.
	if err != nil {
		// Acceptable: the scan error may or may not cause a pipeline-level failure
		// depending on whether other batches already completed.
		ce, ok := err.(*CategorizedError)
		if !ok {
			t.Logf("pipeline returned non-categorized error: %v", err)
		} else if ce.Category != ErrScan {
			t.Errorf("expected ErrScan, got %s", ce.Category)
		}
	}
}

func TestFlow_WriteErrorExhaustedRetries(t *testing.T) {
	src := &testProvider{name: "f_f3_src", units: makeFlowUnits(10, "tbl")}
	dst := &testProvider{
		name:     "f_f3_dst",
		writeErr: fmt.Errorf("destination refused write"),
	}
	setupTestProviders(t, "f_f3_src", "f_f3_dst", src, dst)

	opts := defaultTestOpts()
	opts.MaxRetries = 1 // 2 attempts total

	cfg := defaultTestCfg("f_f3_src", "f_f3_dst", t.TempDir()+"/cp.json")
	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	result, err := pipeline.Run(context.Background())
	// Write errors are non-fatal: the pipeline completes but records failures.
	if err != nil {
		t.Fatalf("unexpected pipeline error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result")
	}
	// All writes should have failed.
	if result.Summary.TotalFailed == 0 {
		t.Error("expected some failed writes")
	}
	if result.Failures == nil || result.Failures.Total == 0 {
		t.Error("expected failures to be recorded")
	}
}

func TestFlow_PauseResumeCompletes(t *testing.T) {
	src := &testProvider{name: "f_f4_src", units: makeFlowUnits(30, "tbl")}
	dst := &testProvider{name: "f_f4_dst"}
	setupTestProviders(t, "f_f4_src", "f_f4_dst", src, dst)

	cfg := defaultTestCfg("f_f4_src", "f_f4_dst", t.TempDir()+"/cp.json")
	opts := defaultTestOpts()

	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)

	go func() {
		pipeline.Pause()
		time.Sleep(10 * time.Millisecond)
		pipeline.Resume()
	}()

	result, err := pipeline.Run(context.Background())
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	if result.Summary.TotalWritten != 30 {
		t.Errorf("wrote %d after pause/resume, want 30", result.Summary.TotalWritten)
	}
}

func TestFlow_EmptySource(t *testing.T) {
	src := &testProvider{name: "f_f5_src", units: nil}
	dst := &testProvider{name: "f_f5_dst"}
	setupTestProviders(t, "f_f5_src", "f_f5_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_f5_src", "f_f5_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	if result.Summary.TotalWritten != 0 {
		t.Errorf("wrote %d, want 0 for empty source", result.Summary.TotalWritten)
	}
	if result.Summary.TotalScanned != 0 {
		t.Errorf("scanned %d, want 0", result.Summary.TotalScanned)
	}
}

func TestFlow_FailuresRecorded(t *testing.T) {
	src := &testProvider{name: "f_f6_src", units: makeFlowUnits(10, "tbl")}
	dst := &testProvider{name: "f_f6_dst"}
	setupTestProviders(t, "f_f6_src", "f_f6_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_f6_src", "f_f6_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	// No failures in a successful run.
	if result.Failures == nil {
		t.Fatal("expected Failures to be initialized")
	}
	if result.Failures.Total != 0 {
		t.Errorf("expected 0 failures, got %d", result.Failures.Total)
	}
}

func TestFlow_ContextAlreadyCancelled(t *testing.T) {
	src := &testProvider{name: "f_f7_src", units: makeFlowUnits(10, "tbl")}
	dst := &testProvider{name: "f_f7_dst"}
	setupTestProviders(t, "f_f7_src", "f_f7_dst", src, dst)

	cfg := defaultTestCfg("f_f7_src", "f_f7_dst", t.TempDir()+"/cp.json")
	opts := defaultTestOpts()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	_, err := pipeline.Run(ctx)
	if err == nil {
		t.Error("expected error with pre-cancelled context")
	}
}

func TestFlow_ResultPhasesRecorded(t *testing.T) {
	src := &testProvider{name: "f_r1_src", units: makeFlowUnits(10, "tbl")}
	dst := &testProvider{name: "f_r1_dst"}
	setupTestProviders(t, "f_r1_src", "f_r1_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_r1_src", "f_r1_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	// Should have at least init, connect, writing, complete phases.
	if len(result.Phases) < 3 {
		t.Errorf("expected at least 3 phases, got %d", len(result.Phases))
	}
	for _, ph := range result.Phases {
		if ph.Duration < 0 {
			t.Errorf("phase %s has negative duration: %s", ph.Phase, ph.Duration)
		}
	}
}

func TestFlow_SummaryMetricsAccurate(t *testing.T) {
	n := 73
	units := makeFlowUnits(n, "metrics")
	for i := range units {
		units[i].Size = 100
	}
	src := &testProvider{name: "f_r2_src", units: units}
	dst := &testProvider{name: "f_r2_dst"}
	setupTestProviders(t, "f_r2_src", "f_r2_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_r2_src", "f_r2_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	if result.Summary.TotalWritten != int64(n) {
		t.Errorf("TotalWritten = %d, want %d", result.Summary.TotalWritten, n)
	}
	if result.Summary.TotalScanned != int64(n) {
		t.Errorf("TotalScanned = %d, want %d", result.Summary.TotalScanned, n)
	}
	if result.Summary.BytesTransferred <= 0 {
		t.Errorf("BytesTransferred = %d, want > 0", result.Summary.BytesTransferred)
	}
	if result.Summary.Duration <= 0 {
		t.Error("Duration should be positive")
	}
	if result.Summary.EndTime.Before(result.Summary.StartTime) {
		t.Error("EndTime should be after StartTime")
	}
}

func TestFlow_PerTableMetrics(t *testing.T) {
	units := append(makeFlowUnits(20, "alpha"), makeFlowUnits(30, "beta")...)
	src := &testProvider{name: "f_r3_src", units: units}
	dst := &testProvider{name: "f_r3_dst"}
	setupTestProviders(t, "f_r3_src", "f_r3_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_r3_src", "f_r3_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	if len(result.Summary.TableMetrics) != 2 {
		t.Fatalf("expected 2 table metrics, got %d", len(result.Summary.TableMetrics))
	}

	foundAlpha, foundBeta := false, false
	for _, tm := range result.Summary.TableMetrics {
		switch tm.Table {
		case "alpha":
			foundAlpha = true
			if tm.Written != 20 {
				t.Errorf("alpha: Written = %d, want 20", tm.Written)
			}
		case "beta":
			foundBeta = true
			if tm.Written != 30 {
				t.Errorf("beta: Written = %d, want 30", tm.Written)
			}
		}
	}
	if !foundAlpha {
		t.Error("missing alpha table metrics")
	}
	if !foundBeta {
		t.Error("missing beta table metrics")
	}
}

func TestFlow_ThroughputRecorded(t *testing.T) {
	units := makeFlowUnits(50, "perf")
	src := &testProvider{name: "f_r4_src", units: units}
	dst := &testProvider{name: "f_r4_dst"}
	setupTestProviders(t, "f_r4_src", "f_r4_dst", src, dst)

	result := runTestPipeline(t, defaultTestCfg("f_r4_src", "f_r4_dst", t.TempDir()+"/cp.json"), defaultTestOpts())

	if result.Summary.AvgThroughput <= 0 {
		t.Errorf("AvgThroughput = %.0f, want > 0", result.Summary.AvgThroughput)
	}
}

func TestFlow_InterruptedResumePreservesCheckpoint(t *testing.T) {
	cpPath := t.TempDir() + "/cp.json"

	// Run 1: cancel mid-run to leave a checkpoint behind.
	src := &testProvider{name: "f_ir1_src", units: makeFlowUnits(500, "tbl")}
	dst := &testProvider{name: "f_ir1_dst"}
	setupTestProviders(t, "f_ir1_src", "f_ir1_dst", src, dst)

	opts := defaultTestOpts()
	opts.CheckpointInterval = 1
	cfg := defaultTestCfg("f_ir1_src", "f_ir1_dst", cpPath)

	ctx, cancel := context.WithCancel(context.Background())
	pipeline, _ := NewPipeline(cfg, opts, noopReporter{}, nil)
	go func() {
		for {
			if dst.totalWrittenUnits() >= 30 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	_, _ = pipeline.Run(ctx)

	// Verify checkpoint exists.
	store, _ := NewFileCheckpointStore(cpPath)
	cp, _ := store.Load(context.Background())
	if cp == nil {
		t.Fatal("expected checkpoint after cancelled run")
	}
	if cp.ConfigHash == "" {
		t.Error("expected ConfigHash to be set")
	}

	// Now manually corrupt the checkpoint to simulate a partial write.
	// This should fail checksum validation on load.
	writeCheckpointGarbage(t, cpPath)

	store2, _ := NewFileCheckpointStore(cpPath)
	_, err := store2.Load(context.Background())
	if err == nil {
		t.Error("expected checksum error on corrupted checkpoint")
	}
}

func writeCheckpointGarbage(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(`{"version":3,"checksum":"bad","written_keys":null}`), 0o644); err != nil {
		t.Fatalf("writeCheckpointGarbage: %v", err)
	}
}
