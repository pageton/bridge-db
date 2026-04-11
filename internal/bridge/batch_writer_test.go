package bridge

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// Batch writer tests
// ---------------------------------------------------------------------------

func TestBatchWriter_FullSuccess(t *testing.T) {
	w := &trackingWriter{}
	bw := &batchWriter{
		w: w,
		cfg: writeConfig{
			MaxRetries:       2,
			RetryBackoff:     0,
			ConflictStrategy: provider.ConflictOverwrite,
			MaxPerUnitRetry:  3,
		},
	}
	p := &Pipeline{
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}
	p.keyRingHead = 0
	p.keyRingLen = 0

	units := makeTestUnits(5, "users")
	out := bw.writeBatch(context.Background(), p, units)

	if out.writeErr != nil {
		t.Fatalf("unexpected write error: %v", out.writeErr)
	}
	if out.result == nil {
		t.Fatal("expected result")
	}
	if out.result.WrittenUnits != 5 {
		t.Errorf("expected 5 written, got %d", out.result.WrittenUnits)
	}
	if out.result.FailedUnits != 0 {
		t.Errorf("expected 0 failed, got %d", out.result.FailedUnits)
	}
	if out.skipped != 0 {
		t.Errorf("expected 0 skipped, got %d", out.skipped)
	}
}

func TestBatchWriter_Dedup(t *testing.T) {
	w := &trackingWriter{}
	bw := &batchWriter{
		w: w,
		cfg: writeConfig{
			MaxRetries:       2,
			RetryBackoff:     0,
			ConflictStrategy: provider.ConflictOverwrite,
		},
	}
	p := &Pipeline{
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}
	p.writtenKeys.Store(hashKey("users:00"), true)
	p.writtenKeys.Store(hashKey("users:10"), true)

	units := makeTestUnits(5, "users")
	out := bw.writeBatch(context.Background(), p, units)

	if out.skipped != 2 {
		t.Errorf("expected 2 dedup'd units, got %d", out.skipped)
	}
	if out.result.WrittenUnits != 3 {
		t.Errorf("expected 3 written (after dedup), got %d", out.result.WrittenUnits)
	}
	if w.callCount() != 1 {
		t.Errorf("expected 1 write call, got %d", w.callCount())
	}
	if len(w.lastUnits()) != 3 {
		t.Errorf("expected 3 units sent to writer, got %d", len(w.lastUnits()))
	}
}

func TestBatchWriter_DedupAll(t *testing.T) {
	w := &trackingWriter{}
	bw := &batchWriter{
		w: w,
		cfg: writeConfig{
			MaxRetries:       2,
			RetryBackoff:     0,
			ConflictStrategy: provider.ConflictOverwrite,
		},
	}

	p := &Pipeline{
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("users:%c%c", rune('0'+i%10), rune('0'+i/10))
		p.writtenKeys.Store(hashKey(key), true)
	}

	units := makeTestUnits(5, "users")
	out := bw.writeBatch(context.Background(), p, units)

	if out.skipped != 5 {
		t.Errorf("expected 5 skipped, got %d", out.skipped)
	}
	if w.callCount() != 0 {
		t.Error("writer should not be called when all units are dedup'd")
	}
}

func TestBatchWriter_WholeBatchFailWithRetry(t *testing.T) {
	w := &failWriter{err: fmt.Errorf("connection reset")}
	bw := &batchWriter{
		w: w,
		cfg: writeConfig{
			MaxRetries:       2,
			RetryBackoff:     0,
			ConflictStrategy: provider.ConflictOverwrite,
		},
	}
	p := &Pipeline{
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}

	units := makeTestUnits(3, "t")
	out := bw.writeBatch(context.Background(), p, units)

	if out.writeErr == nil {
		t.Fatal("expected write error")
	}
	if !strings.Contains(out.writeErr.Error(), "connection reset") {
		t.Errorf("error should mention cause, got: %v", out.writeErr)
	}
	// Should have tried 3 times (initial + 2 retries).
	if w.attempts() != 3 {
		t.Errorf("expected 3 attempts, got %d", w.attempts())
	}
}

func TestBatchWriter_PartialFailure_IndividualRetry(t *testing.T) {
	// Writer succeeds on batch but reports 2 failed keys.
	// On individual retry, those keys succeed.
	failKey := "users:10" // second unit from makeTestUnits(5, "users")
	w := &partialFailWriter{
		failKeys:   map[string]bool{failKey: true},
		succeedKey: map[string]bool{failKey: true}, // succeeds on individual retry
	}

	bw := &batchWriter{
		w: w,
		cfg: writeConfig{
			MaxRetries:       2,
			RetryBackoff:     0,
			ConflictStrategy: provider.ConflictOverwrite,
			MaxPerUnitRetry:  10,
		},
	}
	p := &Pipeline{
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}

	units := makeTestUnits(3, "users")
	out := bw.writeBatch(context.Background(), p, units)

	if out.writeErr != nil {
		t.Fatalf("unexpected write error: %v", out.writeErr)
	}
	// The failed key should have been recovered via individual retry.
	if out.result.WrittenUnits != 3 {
		t.Errorf("expected 3 written after recovery, got %d", out.result.WrittenUnits)
	}
	if len(out.unitErrors) != 0 {
		t.Errorf("expected 0 unit errors after recovery, got %d", len(out.unitErrors))
	}
}

func TestBatchWriter_PartialFailure_IndividualRetryFails(t *testing.T) {
	failKey := "users:10"
	w := &partialFailWriter{
		failKeys:   map[string]bool{failKey: true},
		succeedKey: nil, // individual retry also fails
	}

	bw := &batchWriter{
		w: w,
		cfg: writeConfig{
			MaxRetries:       2,
			RetryBackoff:     0,
			ConflictStrategy: provider.ConflictOverwrite,
			MaxPerUnitRetry:  10,
		},
	}
	p := &Pipeline{
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}

	units := makeTestUnits(3, "users")
	out := bw.writeBatch(context.Background(), p, units)

	if out.writeErr != nil {
		t.Fatalf("unexpected write error: %v", out.writeErr)
	}
	// 2 succeeded in batch, 1 failed.
	if out.result.WrittenUnits != 2 {
		t.Errorf("expected 2 written, got %d", out.result.WrittenUnits)
	}
	if len(out.unitErrors) != 1 {
		t.Errorf("expected 1 unit error, got %d", len(out.unitErrors))
	}
	if out.unitErrors[0].key != failKey {
		t.Errorf("expected error for key %q, got %q", failKey, out.unitErrors[0].key)
	}
}

func TestBatchWriter_EmptyBatch(t *testing.T) {
	w := &trackingWriter{}
	bw := &batchWriter{
		w:   w,
		cfg: writeConfig{MaxRetries: 1},
	}
	p := &Pipeline{
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}

	out := bw.writeBatch(context.Background(), p, nil)
	if out.writeErr != nil {
		t.Fatalf("unexpected error: %v", out.writeErr)
	}
	if out.result.TotalUnits != 0 {
		t.Errorf("expected 0 total, got %d", out.result.TotalUnits)
	}
}

func TestProcessWriteOutcome_RecordsMetrics(t *testing.T) {
	w := &trackingWriter{}
	bw := &batchWriter{w: w, cfg: writeConfig{MaxPerUnitRetry: 0}}
	p := &Pipeline{
		opts:          PipelineOptions{MaxWrittenKeys: 1024},
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}
	reporter := &noopReporter{}
	mc := &testMetricsRecorder{}

	units := makeTestUnits(5, "t")
	out := bw.writeBatch(context.Background(), p, units)

	var errs []error
	var mu sync.Mutex
	processWriteOutcome(out, 1, units, p, reporter, mc, &errs, &mu, 100)

	if mc.batches != 1 {
		t.Errorf("expected 1 batch recorded, got %d", mc.batches)
	}
}

func TestProcessWriteOutcome_WholeBatchFail(t *testing.T) {
	bw := &batchWriter{
		w:   &failWriter{err: fmt.Errorf("dead")},
		cfg: writeConfig{MaxRetries: 0},
	}
	p := &Pipeline{
		keyRing:  make([]string, 1024),
		tableSet: make(map[string]bool),
	}
	reporter := &noopReporter{}
	mc := &testMetricsRecorder{}

	units := makeTestUnits(3, "t")
	out := bw.writeBatch(context.Background(), p, units)

	var errs []error
	var mu sync.Mutex
	processWriteOutcome(out, 1, units, p, reporter, mc, &errs, &mu, 100)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	if mc.errors == 0 {
		t.Error("expected error to be recorded in metrics")
	}
}

func TestFilterWrittenUnits(t *testing.T) {
	units := makeTestUnits(5, "t")
	// Mark key at index 2 as failed.
	result := &provider.BatchResult{
		WrittenUnits: 4,
		FailedUnits:  1,
		FailedKeys:   []string{units[2].Key},
	}

	written := filterWrittenUnits(units, result)
	if len(written) != 4 {
		t.Errorf("expected 4 written units, got %d", len(written))
	}
	for _, u := range written {
		if u.Key == units[2].Key {
			t.Error("failed unit should not be in written set")
		}
	}
}

func TestFilterWrittenUnits_NoFailures(t *testing.T) {
	units := makeTestUnits(3, "t")
	result := &provider.BatchResult{WrittenUnits: 3}

	written := filterWrittenUnits(units, result)
	if len(written) != 3 {
		t.Errorf("expected 3 written when no failures, got %d", len(written))
	}
}

func TestUnitsByKeys(t *testing.T) {
	units := makeTestUnits(5, "t")
	keys := []string{units[0].Key, units[3].Key}

	matched := unitsByKeys(units, keys)
	if len(matched) != 2 {
		t.Fatalf("expected 2 matched, got %d", len(matched))
	}
	if matched[0].Key != units[0].Key {
		t.Errorf("expected key %q, got %q", units[0].Key, matched[0].Key)
	}
}

// ---------------------------------------------------------------------------
// Stub writers
// ---------------------------------------------------------------------------

// trackingWriter counts calls and records the last batch.
type trackingWriter struct {
	called atomic.Int64
	last   []provider.MigrationUnit
}

func (w *trackingWriter) Write(_ context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	w.called.Add(1)
	w.last = append([]provider.MigrationUnit{}, units...)
	return &provider.BatchResult{
		TotalUnits:   len(units),
		WrittenUnits: len(units),
		BytesWritten: totalSize(units),
	}, nil
}

func (w *trackingWriter) Flush(_ context.Context) error       { return nil }
func (w *trackingWriter) callCount() int                      { return int(w.called.Load()) }
func (w *trackingWriter) lastUnits() []provider.MigrationUnit { return w.last }

// failWriter always fails.
type failWriter struct {
	err          error
	attemptCount atomic.Int64
}

func (w *failWriter) Write(_ context.Context, _ []provider.MigrationUnit) (*provider.BatchResult, error) {
	w.attemptCount.Add(1)
	return nil, w.err
}

func (w *failWriter) Flush(_ context.Context) error { return nil }
func (w *failWriter) attempts() int64               { return w.attemptCount.Load() }

// partialFailWriter succeeds on batch write but reports FailedKeys.
// On individual retry (1 unit), it checks succeedKey to decide.
type partialFailWriter struct {
	failKeys   map[string]bool
	succeedKey map[string]bool // nil = never succeed on retry
}

func (w *partialFailWriter) Write(_ context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	var failedKeys []string
	var written int
	var failed int

	// On individual retry (single unit), check succeedKey.
	if len(units) == 1 && w.succeedKey != nil && w.succeedKey[units[0].Key] {
		return &provider.BatchResult{
			TotalUnits:   1,
			WrittenUnits: 1,
			BytesWritten: 50,
		}, nil
	}

	for _, u := range units {
		if w.failKeys[u.Key] {
			failed++
			failedKeys = append(failedKeys, u.Key)
		} else {
			written++
		}
	}

	return &provider.BatchResult{
		TotalUnits:   len(units),
		WrittenUnits: written,
		FailedUnits:  failed,
		FailedKeys:   failedKeys,
	}, nil
}

func (w *partialFailWriter) Flush(_ context.Context) error { return nil }

// testMetricsRecorder is a minimal metrics recorder for tests.
type testMetricsRecorder struct {
	batches int
	errors  int
}

func (m *testMetricsRecorder) RecordBatch(_ *provider.BatchResult) { m.batches++ }
func (m *testMetricsRecorder) RecordBatchTables(_ []provider.MigrationUnit, _ *provider.BatchResult) {
}
func (m *testMetricsRecorder) RecordError() { m.errors++ }
