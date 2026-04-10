package progress

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pageton/bridge-db/internal/util"
	"github.com/pageton/bridge-db/pkg/provider"
)

func TestMetricsCollector_RecordScan(t *testing.T) {
	m := NewMetricsCollector()
	m.RecordScan(100, 5000)
	snap := m.Snapshot(provider.PhaseScanning)
	if snap.TotalScanned != 100 {
		t.Errorf("scanned = %d, want 100", snap.TotalScanned)
	}
	if snap.BytesTransferred != 5000 {
		t.Errorf("bytes = %d, want 5000", snap.BytesTransferred)
	}
}

func TestMetricsCollector_RecordBatch(t *testing.T) {
	m := NewMetricsCollector()
	m.RecordBatch(&provider.BatchResult{
		BatchID:      1,
		WrittenUnits: 50,
		FailedUnits:  2,
		SkippedUnits: 3,
		BytesWritten: 1024,
		Duration:     100 * time.Millisecond,
	})
	snap := m.Snapshot(provider.PhaseWriting)
	if snap.TotalWritten != 50 {
		t.Errorf("written = %d, want 50", snap.TotalWritten)
	}
	if snap.TotalFailed != 2 {
		t.Errorf("failed = %d, want 2", snap.TotalFailed)
	}
	if snap.TotalSkipped != 3 {
		t.Errorf("skipped = %d, want 3", snap.TotalSkipped)
	}
	if snap.CurrentBatchID != 1 {
		t.Errorf("batch_id = %d, want 1", snap.CurrentBatchID)
	}
}

func TestMetricsCollector_RecordError(t *testing.T) {
	m := NewMetricsCollector()
	m.RecordError()
	m.RecordError()
	snap := m.Snapshot(provider.PhaseWriting)
	if snap.TotalFailed != 2 {
		t.Errorf("failed = %d, want 2", snap.TotalFailed)
	}
}

func TestMetricsCollector_SetTables(t *testing.T) {
	m := NewMetricsCollector()
	m.SetTables(2, 5)
	snap := m.Snapshot(provider.PhaseWriting)
	if snap.TablesCompleted != 2 {
		t.Errorf("tables_completed = %d, want 2", snap.TablesCompleted)
	}
	if snap.TablesTotal != 5 {
		t.Errorf("tables_total = %d, want 5", snap.TablesTotal)
	}
}

func TestMetricsCollector_IncrementTablesCompleted(t *testing.T) {
	m := NewMetricsCollector()
	m.SetTables(0, 3)
	m.IncrementTablesCompleted()
	m.IncrementTablesCompleted()
	snap := m.Snapshot(provider.PhaseWriting)
	if snap.TablesCompleted != 2 {
		t.Errorf("tables_completed = %d, want 2", snap.TablesCompleted)
	}
}

func TestMetricsCollector_ToSummary(t *testing.T) {
	m := NewMetricsCollector()
	m.RecordScan(200, 10000)
	m.RecordBatch(&provider.BatchResult{WrittenUnits: 100, BytesWritten: 5000})
	summary := &provider.MigrationSummary{}
	m.ToSummary(summary)
	if summary.TotalScanned != 200 {
		t.Errorf("summary scanned = %d", summary.TotalScanned)
	}
	if summary.TotalWritten != 100 {
		t.Errorf("summary written = %d", summary.TotalWritten)
	}
	if summary.BytesTransferred != 10000+5000 {
		t.Errorf("summary bytes = %d", summary.BytesTransferred)
	}
}

func TestMetricsCollector_RingBuffer(t *testing.T) {
	m := NewMetricsCollector()
	for i := 0; i < 1050; i++ {
		m.RecordBatch(&provider.BatchResult{BatchID: i, Duration: time.Millisecond})
	}
	if m.batchTimeFill != 1000 {
		t.Errorf("fill = %d, want 1000 (maxBatchHistory)", m.batchTimeFill)
	}
}

func TestMetricsCollector_ThroughputCalculation(t *testing.T) {
	m := NewMetricsCollector()
	m.RecordScan(1000, 0)
	m.RecordBatch(&provider.BatchResult{WrittenUnits: 500})
	snap := m.Snapshot(provider.PhaseWriting)
	if snap.Throughput <= 0 {
		t.Error("throughput should be positive")
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KiB"},
		{1048576, "1.0 MiB"},
		{1073741824, "1.0 GiB"},
		{1536, "1.5 KiB"},
	}
	for _, tt := range tests {
		got := util.HumanBytes(tt.input)
		if got != tt.want {
			t.Errorf("HumanBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, "<1µs"},
		{500 * time.Nanosecond, "<1µs"},
		{50 * time.Microsecond, "50µs"},
		{800 * time.Microsecond, "800µs"},
		{5 * time.Millisecond, "5.0ms"},
		{50 * time.Millisecond, "50.0ms"},
		{150 * time.Millisecond, "150ms"},
		{30 * time.Second, "30s"},
		{1500 * time.Millisecond, "1.5s"},
		{90 * time.Second, "1m30s"},
		{3700 * time.Second, "1h1m"},
	}
	for _, tt := range tests {
		got := util.FormatDuration(tt.input)
		if got != tt.want {
			t.Errorf("FormatDuration(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestNopReporter(t *testing.T) {
	r := NopReporter{}
	r.OnBatchStart(1, 100)
	r.OnBatchComplete(1, &provider.BatchResult{})
	r.OnMigrationComplete(&provider.MigrationSummary{})
	r.OnError(errors.New("test"), &provider.MigrationUnit{})
	r.OnPhaseChange(provider.PhaseInit)
	r.OnProgress(provider.ProgressStats{})
}

func TestConsoleReporter_OnPhaseStart(t *testing.T) {
	var buf bytes.Buffer
	r := NewConsoleReporterWith(&buf)
	r.OnPhaseStart(provider.PhaseDesc{
		Phase:       provider.PhaseScanning,
		Step:        6,
		TotalSteps:  8,
		Description: "Transferring data",
	})
	output := buf.String()
	if !strings.Contains(output, "[6/8]") {
		t.Errorf("output should contain [6/8], got: %q", output)
	}
	if !strings.Contains(output, "Transferring data") {
		t.Errorf("output should contain description, got: %q", output)
	}
}

func TestConsoleReporter_OnPhaseDone_Skipped(t *testing.T) {
	var buf bytes.Buffer
	r := NewConsoleReporterWith(&buf)
	r.OnPhaseDone(provider.PhaseDesc{Phase: provider.PhaseSchemaMigration, Step: 4, TotalSteps: 8}, 0, provider.ErrPhaseSkipped)
	output := buf.String()
	if !strings.Contains(output, "skipped") {
		t.Errorf("output should contain 'skipped', got: %q", output)
	}
}

func TestConsoleReporter_OnPhaseDone_Failed(t *testing.T) {
	var buf bytes.Buffer
	r := NewConsoleReporterWith(&buf)
	r.OnPhaseDone(provider.PhaseDesc{Phase: provider.PhaseConnect, Step: 3, TotalSteps: 8}, 2*time.Second, errors.New("connection refused"))
	output := buf.String()
	if !strings.Contains(output, "FAILED") {
		t.Errorf("output should contain 'FAILED', got: %q", output)
	}
	if !strings.Contains(output, "connection refused") {
		t.Errorf("output should contain error message, got: %q", output)
	}
}

func TestConsoleReporter_OnMigrationComplete(t *testing.T) {
	var buf bytes.Buffer
	r := NewConsoleReporterWith(&buf)
	r.OnMigrationComplete(&provider.MigrationSummary{
		Duration:       5 * time.Second,
		TotalScanned:   100,
		TotalWritten:   98,
		TotalFailed:    1,
		TotalSkipped:   1,
		VerificationOK: true,
	})
	output := buf.String()
	if !strings.Contains(output, "completed successfully") {
		t.Errorf("output should contain written count, got: %q", output)
	}
	if !strings.Contains(output, "Migration completed") {
		t.Errorf("output should contain 'Migration complete', got: %q", output)
	}
}

func TestConsoleReporter_OnError(t *testing.T) {
	var buf bytes.Buffer
	r := NewConsoleReporterWith(&buf)
	r.OnError(errors.New("connection refused"), &provider.MigrationUnit{})
	if !strings.Contains(buf.String(), "connection refused") {
		t.Errorf("output = %q", buf.String())
	}
}
