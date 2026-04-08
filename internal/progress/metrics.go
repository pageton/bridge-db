package progress

import (
	"sync"
	"time"

	"github.com/pageton/bridge-db/pkg/provider"
)

const maxBatchHistory = 1000

// MetricsCollector tracks real-time migration metrics. It is thread-safe.
type MetricsCollector struct {
	mu sync.RWMutex

	startTime        time.Time
	totalScanned     int64
	totalWritten     int64
	totalFailed      int64
	totalSkipped     int64
	bytesTransferred int64
	tablesCompleted  int
	tablesTotal      int
	currentBatchID   int

	// Ring buffer for batch durations — bounded to avoid OOM at scale.
	batchTimes    []time.Duration
	batchTimePos  int
	batchTimeFill int
}

// NewMetricsCollector creates a collector ready to track a migration.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		startTime:  time.Now(),
		batchTimes: make([]time.Duration, maxBatchHistory),
	}
}

// RecordScan increments the scanned unit count and bytes.
func (m *MetricsCollector) RecordScan(count int64, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalScanned += count
	m.bytesTransferred += bytes
}

// RecordBatch updates metrics from a completed batch result.
func (m *MetricsCollector) RecordBatch(result *provider.BatchResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalWritten += int64(result.WrittenUnits)
	m.totalFailed += int64(result.FailedUnits)
	m.totalSkipped += int64(result.SkippedUnits)
	m.bytesTransferred += result.BytesWritten
	m.currentBatchID = result.BatchID

	// Store in ring buffer
	m.batchTimes[m.batchTimePos] = result.Duration
	m.batchTimePos = (m.batchTimePos + 1) % maxBatchHistory
	if m.batchTimeFill < maxBatchHistory {
		m.batchTimeFill++
	}
}

// RecordError increments the failed count for non-batch errors.
func (m *MetricsCollector) RecordError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalFailed++
}

// SetTables updates table completion tracking.
func (m *MetricsCollector) SetTables(completed, total int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tablesCompleted = completed
	m.tablesTotal = total
}

// IncrementTablesCompleted increments the completed table count.
func (m *MetricsCollector) IncrementTablesCompleted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tablesCompleted++
}

// SetBatchID sets the current batch ID.
func (m *MetricsCollector) SetBatchID(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentBatchID = id
}

// Snapshot returns a point-in-time copy of the metrics as ProgressStats.
func (m *MetricsCollector) Snapshot(phase provider.MigrationPhase) provider.ProgressStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	elapsed := time.Since(m.startTime)
	var throughput float64
	if elapsed.Seconds() > 0 {
		throughput = float64(m.totalWritten) / elapsed.Seconds()
	}

	var estimatedRemain time.Duration
	if throughput > 0 && m.totalScanned > m.totalWritten {
		remaining := m.totalScanned - m.totalWritten
		estimatedRemain = time.Duration(float64(remaining)/throughput) * time.Second
	}

	return provider.ProgressStats{
		Phase:            phase,
		TotalScanned:     m.totalScanned,
		TotalWritten:     m.totalWritten,
		TotalFailed:      m.totalFailed,
		TotalSkipped:     m.totalSkipped,
		Throughput:       throughput,
		Elapsed:          elapsed,
		EstimatedRemain:  estimatedRemain,
		BytesTransferred: m.bytesTransferred,
		CurrentBatchID:   m.currentBatchID,
		TablesCompleted:  m.tablesCompleted,
		TablesTotal:      m.tablesTotal,
		ErrorCount:       int(m.totalFailed),
	}
}

// ToSummary populates a MigrationSummary from the collected metrics.
func (m *MetricsCollector) ToSummary(summary *provider.MigrationSummary) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	summary.TotalScanned = m.totalScanned
	summary.TotalWritten = m.totalWritten
	summary.TotalFailed = m.totalFailed
	summary.TotalSkipped = m.totalSkipped
	summary.BytesTransferred = m.bytesTransferred
}
