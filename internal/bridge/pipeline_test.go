package bridge

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// Unit tests: splitBatch
// ---------------------------------------------------------------------------

func TestSplitBatch_NoSplitWhenMaxBytesZero(t *testing.T) {
	units := makeUnits(5, 100)
	batches := splitBatch(units, 0)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0]) != 5 {
		t.Fatalf("expected 5 units in batch, got %d", len(batches[0]))
	}
}

func TestSplitBatch_NoSplitWhenUnderBudget(t *testing.T) {
	units := makeUnits(3, 100) // total 300 bytes
	batches := splitBatch(units, 500)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
}

func TestSplitBatch_SplitsWhenOverBudget(t *testing.T) {
	// 5 units of 100 bytes each, budget 250 bytes => should split
	units := makeUnits(5, 100)
	batches := splitBatch(units, 250)
	if len(batches) < 2 {
		t.Fatalf("expected at least 2 batches, got %d", len(batches))
	}
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 5 {
		t.Fatalf("expected 5 total units across batches, got %d", total)
	}
}

func TestSplitBatch_SingleOversizedUnit(t *testing.T) {
	units := []provider.MigrationUnit{{Key: "big", Size: 1000}}
	batches := splitBatch(units, 100)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch for single oversized unit, got %d", len(batches))
	}
	if len(batches[0]) != 1 {
		t.Fatalf("expected 1 unit in batch, got %d", len(batches[0]))
	}
}

func TestSplitBatch_EmptyInput(t *testing.T) {
	batches := splitBatch(nil, 1000)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch for nil input, got %d", len(batches))
	}
	if len(batches[0]) != 0 {
		t.Fatalf("expected empty batch, got %d units", len(batches[0]))
	}
}

// ---------------------------------------------------------------------------
// Unit tests: recordKeys (eviction)
// ---------------------------------------------------------------------------

func TestRecordKeys_BasicTracking(t *testing.T) {
	p := newTestPipeline(1000)
	units := makeUnits(3, 10)
	p.recordKeys(units)

	if p.totalWritten != 3 {
		t.Errorf("expected totalWritten=3, got %d", p.totalWritten)
	}
	if p.keyRingLen != 3 {
		t.Errorf("expected 3 written keys, got %d", p.keyRingLen)
	}
}

func TestRecordKeys_Eviction(t *testing.T) {
	p := newTestPipeline(5) // cap of 5 keys

	// Write 8 unique keys total
	for i := 0; i < 8; i++ {
		p.recordKeys([]provider.MigrationUnit{{Key: string(rune('a' + i)), Size: 1}})
	}

	if p.keyRingLen > 5 {
		t.Errorf("expected at most 5 keys after eviction, got %d", p.keyRingLen)
	}
	if p.totalWritten != 8 {
		t.Errorf("expected totalWritten=8, got %d", p.totalWritten)
	}

	// Oldest keys should be evicted from the set
	if _, ok := p.writtenKeys.Load(hashKey("a")); ok {
		t.Error("key 'a' should have been evicted")
	}
	if _, ok := p.writtenKeys.Load(hashKey("b")); ok {
		t.Error("key 'b' should have been evicted")
	}
	if _, ok := p.writtenKeys.Load(hashKey("c")); ok {
		t.Error("key 'c' should have been evicted")
	}
	// Newest keys should remain
	if _, ok := p.writtenKeys.Load(hashKey("f")); !ok {
		t.Error("key 'f' should still be tracked")
	}
}

func TestRecordKeys_Dedup(t *testing.T) {
	p := newTestPipeline(1000)
	units := []provider.MigrationUnit{
		{Key: "x", Size: 1},
		{Key: "x", Size: 1},
		{Key: "y", Size: 1},
	}
	p.recordKeys(units)

	if p.keyRingLen != 2 {
		t.Errorf("expected 2 unique keys, got %d", p.keyRingLen)
	}
	if p.totalWritten != 3 {
		t.Errorf("expected totalWritten=3, got %d", p.totalWritten)
	}
}

type pipelineConfigCaptureTransformer struct {
	transform.NoopTransformer
	received transform.TransformerConfig
}

func (c *pipelineConfigCaptureTransformer) Configure(cfg transform.TransformerConfig) {
	c.received = cfg
}

func TestPipelinesBuildIndependentTransformerConfigs(t *testing.T) {
	srcProvider := t.Name() + "_src"
	dstProvider := t.Name() + "_dst"

	transform.RegisterTransformer(srcProvider, dstProvider, func() transform.Transformer {
		return &pipelineConfigCaptureTransformer{}
	})

	p1 := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: srcProvider},
			Destination: config.ConnectionConfig{Provider: dstProvider},
			Transform: config.TransformConfig{
				NullPolicy: "skip",
				Mappings: map[string][]config.FieldMapping{
					"users": {{Source: "first_name", Destination: "given_name"}},
				},
			},
		},
	}
	p2 := &Pipeline{
		config: &config.MigrationConfig{
			Source:      config.ConnectionConfig{Provider: srcProvider + "_other"},
			Destination: config.ConnectionConfig{Provider: dstProvider + "_other"},
			Transform: config.TransformConfig{
				NullPolicy: "error",
			},
		},
	}

	t1 := transform.GetTransformer(srcProvider, dstProvider, p1.buildTransformerConfig()).(*pipelineConfigCaptureTransformer)
	t2 := transform.GetTransformer(srcProvider, dstProvider, p2.buildTransformerConfig()).(*pipelineConfigCaptureTransformer)

	if t1.received.SrcDialect != transform.Dialect(srcProvider) {
		t.Errorf("t1 SrcDialect = %q, want %q", t1.received.SrcDialect, srcProvider)
	}
	if t2.received.SrcDialect != transform.Dialect(srcProvider+"_other") {
		t.Errorf("t2 SrcDialect = %q, want %q", t2.received.SrcDialect, srcProvider+"_other")
	}
	if t1.received.NullHandler == nil || t2.received.NullHandler == nil {
		t.Fatal("expected both transformers to receive a NullHandler")
	}
	if t1.received.NullHandler.Policy == t2.received.NullHandler.Policy {
		t.Errorf("expected different null policies, got %q for both", t1.received.NullHandler.Policy)
	}
	if t1.received.FieldMapping == nil {
		t.Fatal("expected t1 FieldMapping to be configured")
	}
	if t2.received.FieldMapping != nil {
		t.Fatal("expected t2 FieldMapping to remain nil")
	}
}

// ---------------------------------------------------------------------------
// Unit tests: maybeCheckpoint (interval throttling)
// ---------------------------------------------------------------------------

func TestMaybeCheckpoint_EveryBatch(t *testing.T) {
	store := &memCheckpointStore{}
	p := newTestPipeline(1000)
	p.checkpoint = store
	p.opts.CheckpointInterval = 0 // every batch
	p.config = &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "postgres", Postgres: &config.PostgresConfig{}},
		Destination: config.ConnectionConfig{Provider: "mysql", MySQL: &config.MySQLConfig{}},
	}

	scanner := &noopScanner{}
	p.maybeCheckpoint(context.Background(), 1, scanner)
	p.maybeCheckpoint(context.Background(), 2, scanner)

	if store.saveCount != 2 {
		t.Errorf("expected 2 saves with interval=0, got %d", store.saveCount)
	}
}

func TestMaybeCheckpoint_Throttled(t *testing.T) {
	store := &memCheckpointStore{}
	p := newTestPipeline(1000)
	p.checkpoint = store
	p.opts.CheckpointInterval = 3
	p.config = &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "postgres", Postgres: &config.PostgresConfig{}},
		Destination: config.ConnectionConfig{Provider: "mysql", MySQL: &config.MySQLConfig{}},
	}

	scanner := &noopScanner{}
	p.maybeCheckpoint(context.Background(), 1, scanner)
	p.maybeCheckpoint(context.Background(), 2, scanner)
	p.maybeCheckpoint(context.Background(), 3, scanner) // triggers save
	p.maybeCheckpoint(context.Background(), 4, scanner)

	// Only batch 3 should have saved
	if store.saveCount != 1 {
		t.Errorf("expected 1 save with interval=3 after 4 calls, got %d", store.saveCount)
	}
}

// ---------------------------------------------------------------------------
// Unit tests: concurrent recordKeys
// ---------------------------------------------------------------------------

func TestRecordKeys_Concurrent(t *testing.T) {
	p := newTestPipeline(10000)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			units := make([]provider.MigrationUnit, 100)
			for j := range units {
				units[j] = provider.MigrationUnit{Key: string(rune('a'+offset)) + string(rune('0'+j%10)), Size: 1}
			}
			p.recordKeys(units)
		}(i)
	}

	wg.Wait()

	if p.totalWritten != 1000 {
		t.Errorf("expected totalWritten=1000, got %d", p.totalWritten)
	}
}

// ---------------------------------------------------------------------------
// Unit tests: checkpoint save/load round-trip
// ---------------------------------------------------------------------------

func TestCheckpointSaveLoad(t *testing.T) {
	store := &memCheckpointStore{}
	p := newTestPipeline(1000)
	p.checkpoint = store
	p.config = &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "postgres", Postgres: &config.PostgresConfig{}},
		Destination: config.ConnectionConfig{Provider: "mysql", MySQL: &config.MySQLConfig{}},
	}
	p.startTime = p.startTime.UTC()

	p.recordKeys(makeUnits(3, 10))
	p.saveCheckpoint(context.Background(), 5, &noopScanner{})

	cp, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cp == nil {
		t.Fatal("expected checkpoint, got nil")
	}
	if cp.LastBatchID != 5 {
		t.Errorf("expected LastBatchID=5, got %d", cp.LastBatchID)
	}
	if cp.TotalWritten != 3 {
		t.Errorf("expected TotalWritten=3, got %d", cp.TotalWritten)
	}
}

func TestCheckpointConfigHashMismatch(t *testing.T) {
	store := &memCheckpointStore{}
	p := newTestPipeline(1000)
	p.checkpoint = store
	p.config = &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "postgres", Postgres: &config.PostgresConfig{Host: "host1"}},
		Destination: config.ConnectionConfig{Provider: "mysql", MySQL: &config.MySQLConfig{}},
	}

	p.saveCheckpoint(context.Background(), 1, &noopScanner{})

	// Simulate config change by loading the checkpoint and verifying hash
	cp, _ := store.Load(context.Background())
	originalHash := cp.ConfigHash

	// Change config and recompute hash
	p.config.Source.Postgres.Host = "host2"
	newHash := computeConfigHash(p.config)

	if originalHash == newHash {
		t.Error("config hash should differ after host change")
	}
}

// ---------------------------------------------------------------------------
// Unit tests: PipelineOptions validation
// ---------------------------------------------------------------------------

func TestPipelineOptions_Validate(t *testing.T) {
	tests := []struct {
		name    string
		opts    PipelineOptions
		wantErr bool
	}{
		{
			name:    "defaults",
			opts:    DefaultPipelineOptions(),
			wantErr: false,
		},
		{
			name:    "zero batch_size",
			opts:    PipelineOptions{BatchSize: 0},
			wantErr: true,
		},
		{
			name:    "negative max_retries",
			opts:    PipelineOptions{BatchSize: 1, MaxRetries: -1},
			wantErr: true,
		},
		{
			name:    "zero parallel",
			opts:    PipelineOptions{BatchSize: 1, Parallel: 0},
			wantErr: true,
		},
		{
			name:    "zero write_workers",
			opts:    PipelineOptions{BatchSize: 1, Parallel: 1, WriteWorkers: 0},
			wantErr: true,
		},
		{
			name:    "negative max_batch_bytes",
			opts:    PipelineOptions{BatchSize: 1, Parallel: 1, WriteWorkers: 1, MaxBatchBytes: -1},
			wantErr: true,
		},
		{
			name:    "invalid conflict_strategy",
			opts:    PipelineOptions{BatchSize: 1, Parallel: 1, WriteWorkers: 1, ConflictStrategy: "bad"},
			wantErr: true,
		},
		{
			name:    "invalid fk_handling",
			opts:    PipelineOptions{BatchSize: 1, Parallel: 1, WriteWorkers: 1, FKHandling: "bad"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Unit tests: error categorization
// ---------------------------------------------------------------------------

func TestCategorize(t *testing.T) {
	connErr := NewConnectionError("connect", "failed", nil)
	if cat := Categorize(connErr); cat != ErrConnection {
		t.Errorf("expected ErrConnection, got %s", cat)
	}
	if !IsRetryable(connErr) {
		t.Error("connection errors should be retryable")
	}

	transformErr := NewTransformError(1, nil)
	if IsRetryable(transformErr) {
		t.Error("transform errors should not be retryable")
	}
}

func TestFailureSummary(t *testing.T) {
	fs := NewFailureSummary()
	fs.Record(NewConnectionError("connect", "failed", nil))
	fs.Record(NewConnectionError("tunnel", "failed", nil))
	fs.Record(NewTransformError(1, nil))

	if fs.Total != 3 {
		t.Errorf("expected Total=3, got %d", fs.Total)
	}
	if fs.Counts[ErrConnection] != 2 {
		t.Errorf("expected 2 connection errors, got %d", fs.Counts[ErrConnection])
	}
	if fs.Counts[ErrTransform] != 1 {
		t.Errorf("expected 1 transform error, got %d", fs.Counts[ErrTransform])
	}
	if fs.Examples[ErrConnection] == nil {
		t.Error("expected connection error example")
	}
}

func TestRetryPolicyFor(t *testing.T) {
	p := RetryPolicyFor(ErrWrite)
	if p.MaxAttempts != 5 {
		t.Errorf("expected 5 write retries, got %d", p.MaxAttempts)
	}
	p = RetryPolicyFor(ErrTransform)
	if p.MaxAttempts != 1 {
		t.Errorf("expected 1 transform attempt, got %d", p.MaxAttempts)
	}
}

// ---------------------------------------------------------------------------
// Unit tests: structured error boundaries per step
// ---------------------------------------------------------------------------

func TestConfigError_HasStep(t *testing.T) {
	err := NewConfigError(1, "bad batch size", nil)
	if err.Step != 1 {
		t.Errorf("expected Step=1, got %d", err.Step)
	}
	if err.Category != ErrConfig {
		t.Errorf("expected ErrConfig, got %s", err.Category)
	}
	if IsRetryable(err) {
		t.Error("config errors should not be retryable")
	}
}

func TestCancelledError(t *testing.T) {
	err := NewCancelledError("Migration was cancelled", context.Canceled)
	if err.Category != ErrCancelled {
		t.Errorf("expected ErrCancelled, got %s", err.Category)
	}
	if IsRetryable(err) {
		t.Error("cancelled errors should not be retryable")
	}
	if StepOf(err) != 0 {
		t.Errorf("expected Step=0 before WithStep, got %d", StepOf(err))
	}

	withStep := err.WithStep(8)
	if withStep.Step != 8 {
		t.Errorf("expected Step=8 after WithStep, got %d", withStep.Step)
	}
}

func TestWithStep(t *testing.T) {
	err := NewConnectionError("connect", "failed", nil)
	if err.Step != 0 {
		t.Errorf("expected Step=0 before WithStep, got %d", err.Step)
	}
	result := err.WithStep(3)
	if result.Step != 3 {
		t.Errorf("expected Step=3, got %d", result.Step)
	}
	// WithStep should return the same pointer.
	if result != err {
		t.Error("WithStep should return the same error pointer")
	}
}

func TestStepOf_NonCategorized(t *testing.T) {
	if step := StepOf(fmt.Errorf("plain error")); step != 0 {
		t.Errorf("expected 0 for non-categorized error, got %d", step)
	}
}

func TestErrorFormat_WithStep(t *testing.T) {
	err := NewConnectionError("connect", "source down", fmt.Errorf("dial tcp: refused")).WithStep(3)
	msg := err.Error()
	if !contains(msg, "[connection]") {
		t.Errorf("expected [connection] in error: %s", msg)
	}
	if !contains(msg, "step 3") {
		t.Errorf("expected 'step 3' in error: %s", msg)
	}
	if !contains(msg, "(connect)") {
		t.Errorf("expected '(connect)' phase in error: %s", msg)
	}
}

func TestErrorFormat_WithoutStep(t *testing.T) {
	err := NewConnectionError("tunnel", "SSH failed", nil)
	msg := err.Error()
	if contains(msg, "step") {
		t.Errorf("step should not appear when Step=0: %s", msg)
	}
}

func TestConfigError_Step5(t *testing.T) {
	err := NewConfigError(5, "checkpoint provider mismatch", nil)
	if err.Step != 5 {
		t.Errorf("expected Step=5, got %d", err.Step)
	}
	if err.Category != ErrConfig {
		t.Errorf("expected ErrConfig, got %s", err.Category)
	}
	msg := err.Error()
	if !contains(msg, "step 5") {
		t.Errorf("expected 'step 5' in error: %s", msg)
	}
	if !contains(msg, "[config]") {
		t.Errorf("expected '[config]' in error: %s", msg)
	}
}

func TestCancelledError_WithStep(t *testing.T) {
	err := NewCancelledError("Migration was cancelled", context.Canceled).WithStep(8)
	msg := err.Error()
	if !contains(msg, "[cancelled]") {
		t.Errorf("expected '[cancelled]' in error: %s", msg)
	}
	if !contains(msg, "step 8") {
		t.Errorf("expected 'step 8' in error: %s", msg)
	}
}

func TestWriteErrorExhausted_NotRetryable(t *testing.T) {
	err := NewWriteErrorExhausted(42, fmt.Errorf("disk full"))
	if IsRetryable(err) {
		t.Error("exhausted write errors should not be retryable")
	}
	if err.Category != ErrWrite {
		t.Errorf("expected ErrWrite, got %s", err.Category)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || findSubstring(s, sub))
}

func findSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func makeUnits(n int, size int64) []provider.MigrationUnit {
	units := make([]provider.MigrationUnit, n)
	for i := range units {
		units[i] = provider.MigrationUnit{
			Key:  string(rune('a' + i)),
			Size: size,
		}
	}
	return units
}

func newTestPipeline(maxKeys int) *Pipeline {
	return &Pipeline{
		opts: PipelineOptions{
			MaxWrittenKeys:     maxKeys,
			CheckpointInterval: 0,
		},
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}
}

// memCheckpointStore is an in-memory CheckpointStore for testing.
type memCheckpointStore struct {
	mu         sync.Mutex
	checkpoint *Checkpoint
	saveCount  int
}

func (s *memCheckpointStore) Save(_ context.Context, cp *Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoint = cp
	s.saveCount++
	return nil
}

func (s *memCheckpointStore) Load(_ context.Context) (*Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoint, nil
}

func (s *memCheckpointStore) Clear(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoint = nil
	return nil
}

// noopScanner is a minimal Scanner for tests.
type noopScanner struct{}

func (s *noopScanner) Next(_ context.Context) ([]provider.MigrationUnit, error) {
	return nil, nil
}

func (s *noopScanner) Stats() provider.ScanStats { return provider.ScanStats{} }

// ---------------------------------------------------------------------------
// Resume behaviour tests
// ---------------------------------------------------------------------------

// statsScanner is a Scanner whose Stats() returns a configurable value.
type statsScanner struct {
	noopScanner
	stats provider.ScanStats
}

func (s *noopScanner) Close() error { return nil }

func (s *statsScanner) Stats() provider.ScanStats { return s.stats }

func (s *statsScanner) Close() error { return nil }

// TestCheckpointSave_OnlyCompletedTables verifies that saveCheckpoint only
// includes truly completed tables (where scanner.TablesDone was incremented)
// and NOT partially-scanned tables that merely had some rows written.
func TestCheckpointSave_OnlyCompletedTables(t *testing.T) {
	store := &memCheckpointStore{}
	p := newTestPipeline(1000)
	p.checkpoint = store
	p.config = &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "postgres", Postgres: &config.PostgresConfig{}},
		Destination: config.ConnectionConfig{Provider: "mysql", MySQL: &config.MySQLConfig{}},
	}
	p.startTime = time.Now().UTC()

	// Simulate: scanned 3 tables, wrote keys for all 3, but scanner only
	// finished 2 (TablesDone=2). Table C is in-progress.
	p.scannedTables = []string{"A", "B", "C"}
	for _, tbl := range p.scannedTables {
		p.tableSet[tbl] = true
	}
	// Write some keys for each table.
	p.recordKeys([]provider.MigrationUnit{
		{Key: "a1", Table: "A", Size: 1},
		{Key: "a2", Table: "A", Size: 1},
	})
	p.recordKeys([]provider.MigrationUnit{
		{Key: "b1", Table: "B", Size: 1},
	})
	p.recordKeys([]provider.MigrationUnit{
		{Key: "c1", Table: "C", Size: 1},
	})

	scanner := &statsScanner{
		stats: provider.ScanStats{
			TotalScanned: 4,
			TablesDone:   2, // A and B completed; C in-progress
			TablesTotal:  3,
		},
	}

	p.saveCheckpoint(context.Background(), 10, scanner)

	cp, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cp == nil {
		t.Fatal("expected checkpoint, got nil")
	}

	// Only A and B should be in TablesCompleted.
	if len(cp.TablesCompleted) != 2 {
		t.Fatalf("expected 2 completed tables, got %d: %v", len(cp.TablesCompleted), cp.TablesCompleted)
	}
	if cp.TablesCompleted[0] != "A" || cp.TablesCompleted[1] != "B" {
		t.Errorf("expected completed [A B], got %v", cp.TablesCompleted)
	}

	// C should be the in-progress table.
	if cp.LastTableScanning != "C" {
		t.Errorf("expected LastTableScanning='C', got %q", cp.LastTableScanning)
	}

	// All 4 keys should be preserved (no truncation).
	if len(cp.WrittenKeys) != 4 {
		t.Errorf("expected 4 written keys, got %d", len(cp.WrittenKeys))
	}
}

// TestCheckpointSave_AllTablesComplete verifies behaviour when all scanned
// tables are complete (TablesDone == len(scannedTables)).
func TestCheckpointSave_AllTablesComplete(t *testing.T) {
	store := &memCheckpointStore{}
	p := newTestPipeline(1000)
	p.checkpoint = store
	p.config = &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "postgres", Postgres: &config.PostgresConfig{}},
		Destination: config.ConnectionConfig{Provider: "mysql", MySQL: &config.MySQLConfig{}},
	}
	p.startTime = time.Now().UTC()

	p.scannedTables = []string{"A", "B"}
	p.tableSet["A"] = true
	p.tableSet["B"] = true
	p.recordKeys([]provider.MigrationUnit{{Key: "a1", Table: "A", Size: 1}})

	scanner := &statsScanner{
		stats: provider.ScanStats{TablesDone: 2, TablesTotal: 2},
	}

	p.saveCheckpoint(context.Background(), 5, scanner)

	cp, _ := store.Load(context.Background())
	if len(cp.TablesCompleted) != 2 {
		t.Errorf("expected 2 completed tables, got %d", len(cp.TablesCompleted))
	}
	if cp.LastTableScanning != "" {
		t.Errorf("expected empty LastTableScanning, got %q", cp.LastTableScanning)
	}
}

// TestCheckpointSave_TablesDoneExceedsScannedTables verifies that we don't
// panic when the scanner reports more TablesDone than the pipeline has
// recorded (can happen due to concurrent scanner/writer timing).
func TestCheckpointSave_TablesDoneExceedsScannedTables(t *testing.T) {
	store := &memCheckpointStore{}
	p := newTestPipeline(1000)
	p.checkpoint = store
	p.config = &config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "postgres", Postgres: &config.PostgresConfig{}},
		Destination: config.ConnectionConfig{Provider: "mysql", MySQL: &config.MySQLConfig{}},
	}
	p.startTime = time.Now().UTC()

	p.scannedTables = []string{"A"} // writer only saw 1 table
	p.tableSet["A"] = true

	scanner := &statsScanner{
		stats: provider.ScanStats{TablesDone: 3, TablesTotal: 3}, // scanner ahead
	}

	p.saveCheckpoint(context.Background(), 5, scanner)

	cp, _ := store.Load(context.Background())
	// Should cap at len(scannedTables) = 1, not panic with index 3.
	if len(cp.TablesCompleted) != 1 {
		t.Errorf("expected 1 completed table (capped), got %d", len(cp.TablesCompleted))
	}
}

// TestCheckpointVersion_RejectsOldVersions verifies that v1 and v2 checkpoints
// are rejected to prevent silent data loss from the old TablesCompleted semantics.
func TestCheckpointVersion_RejectsOldVersions(t *testing.T) {
	cp := &Checkpoint{
		SourceProvider: "postgres",
		DestProvider:   "mysql",
		StartTime:      time.Now(),
		Version:        1,
	}
	if err := cp.Validate(); err == nil {
		t.Error("expected v1 checkpoint to be rejected")
	}

	cp.Version = 2
	if err := cp.Validate(); err == nil {
		t.Error("expected v2 checkpoint to be rejected")
	}
}

// TestCheckpointVersion_RejectsFutureVersion verifies that newer checkpoint
// versions are rejected with an upgrade hint.
func TestCheckpointVersion_RejectsFutureVersion(t *testing.T) {
	cp := &Checkpoint{
		SourceProvider: "postgres",
		DestProvider:   "mysql",
		StartTime:      time.Now(),
		Version:        999,
	}
	if err := cp.Validate(); err == nil {
		t.Error("expected future version to be rejected")
	}
}

// TestCheckpointVersion_AcceptsCurrentVersion verifies that the current version
// passes validation.
func TestCheckpointVersion_AcceptsCurrentVersion(t *testing.T) {
	cp := &Checkpoint{
		SourceProvider: "postgres",
		DestProvider:   "mysql",
		StartTime:      time.Now(),
		Version:        checkpointVersion,
	}
	if err := cp.Validate(); err != nil {
		t.Errorf("expected current version to pass, got: %v", err)
	}
}

// TestConfigHash_IncludesPipelineOpts verifies that changing pipeline options
// (batch_size, conflict_strategy, fk_handling) changes the config hash.
func TestConfigHash_IncludesPipelineOpts(t *testing.T) {
	base := config.MigrationConfig{
		Source:      config.ConnectionConfig{Provider: "postgres", Postgres: &config.PostgresConfig{Host: "h", Database: "d"}},
		Destination: config.ConnectionConfig{Provider: "mysql", MySQL: &config.MySQLConfig{Host: "h", Database: "d"}},
		Pipeline: config.PipelineConfig{
			BatchSize:        1000,
			ConflictStrategy: provider.ConflictOverwrite,
			FKHandling:       "defer_constraints",
		},
	}

	hash1 := computeConfigHash(&base)

	cfg2 := base
	cfg2.Pipeline.BatchSize = 500
	hash2 := computeConfigHash(&cfg2)
	if hash1 == hash2 {
		t.Error("config hash should differ when batch_size changes")
	}

	cfg3 := base
	cfg3.Pipeline.ConflictStrategy = provider.ConflictSkip
	hash3 := computeConfigHash(&cfg3)
	if hash1 == hash3 {
		t.Error("config hash should differ when conflict_strategy changes")
	}

	cfg4 := base
	cfg4.Pipeline.FKHandling = "skip"
	hash4 := computeConfigHash(&cfg4)
	if hash1 == hash4 {
		t.Error("config hash should differ when fk_handling changes")
	}
}

// TestKeyRestore_RingBufferSize verifies that restoring from a checkpoint
// creates a ring buffer large enough to hold all restored keys.
func TestKeyRestore_RingBufferSize(t *testing.T) {
	p := newTestPipeline(100) // MaxWrittenKeys = 100

	// Simulate a checkpoint with 80 hashed keys (fewer than MaxWrittenKeys).
	keys := make([]string, 80)
	for i := range keys {
		keys[i] = hashKey(fmt.Sprintf("key_%d", i))
	}
	cp := &Checkpoint{
		SourceProvider: "postgres",
		DestProvider:   "mysql",
		StartTime:      time.Now(),
		ConfigHash:     "test",
		Version:        checkpointVersion,
		WrittenKeys:    keys,
		TotalWritten:   80,
	}
	store := &memCheckpointStore{checkpoint: cp}
	p.checkpoint = store

	// Manually restore keys (mirroring stepPlan logic).
	if len(cp.WrittenKeys) > 0 {
		for _, k := range cp.WrittenKeys {
			p.writtenKeys.Store(k, true)
		}
		ringSize := keyRingSizeFor(len(cp.WrittenKeys), p.opts.MaxWrittenKeys)
		ring := make([]string, ringSize)
		copy(ring, cp.WrittenKeys)
		p.keyRing = ring
		p.keyRingLen = len(cp.WrittenKeys)
		p.keyRingHead = len(cp.WrittenKeys) % ringSize
		atomic.StoreInt64(&p.totalWritten, cp.TotalWritten)
	}

	if len(p.keyRing) != p.opts.MaxWrittenKeys {
		t.Errorf("expected ring size %d, got %d", p.opts.MaxWrittenKeys, len(p.keyRing))
	}
	// All 80 keys should be in the set — count via range over ring.
	restoredCount := 0
	p.writtenKeys.Range(func(_, _ any) bool {
		restoredCount++
		return true
	})
	if restoredCount != 80 {
		t.Errorf("expected 80 keys in set, got %d", restoredCount)
	}
	// Head should be 80 (mod 100).
	if p.keyRingHead != 80 {
		t.Errorf("expected head=80, got %d", p.keyRingHead)
	}
}

// TestKeyRestore_LargeCheckpoint verifies that restoring more keys than
// MaxWrittenKeys still works (ring grows to fit).
func TestKeyRestore_LargeCheckpoint(t *testing.T) {
	p := newTestPipeline(100) // MaxWrittenKeys = 100

	keys := make([]string, 250)
	for i := range keys {
		keys[i] = fmt.Sprintf("key_%d", i)
	}
	cp := &Checkpoint{
		SourceProvider: "postgres",
		DestProvider:   "mysql",
		StartTime:      time.Now(),
		ConfigHash:     "test",
		Version:        checkpointVersion,
		WrittenKeys:    keys,
		TotalWritten:   250,
	}

	// Restore.
	ringSize := keyRingSizeFor(len(cp.WrittenKeys), p.opts.MaxWrittenKeys)
	ring := make([]string, ringSize)
	copy(ring, cp.WrittenKeys)
	p.keyRing = ring
	p.keyRingLen = len(cp.WrittenKeys)
	p.keyRingHead = len(cp.WrittenKeys) % ringSize

	if len(p.keyRing) != 250 {
		t.Errorf("expected ring size 250, got %d", len(p.keyRing))
	}
	if p.keyRingHead != 0 {
		t.Errorf("expected head=0, got %d", p.keyRingHead)
	}
}
