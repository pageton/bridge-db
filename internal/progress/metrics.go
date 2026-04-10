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

	// Per-table tracking
	tableMetrics map[string]*provider.TableMetrics

	// Throughput tracking
	peakThroughput float64

	// Ring buffer for batch durations — bounded to avoid OOM at scale.
	batchTimes    []time.Duration
	batchTimePos  int
	batchTimeFill int
}

// NewMetricsCollector creates a collector ready to track a migration.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		startTime:    time.Now(),
		batchTimes:   make([]time.Duration, maxBatchHistory),
		tableMetrics: make(map[string]*provider.TableMetrics),
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

	// Track throughput
	elapsed := time.Since(m.startTime)
	if elapsed.Seconds() > 0 {
		tp := float64(m.totalWritten) / elapsed.Seconds()
		if tp > m.peakThroughput {
			m.peakThroughput = tp
		}
	}

	// Store in ring buffer
	m.batchTimes[m.batchTimePos] = result.Duration
	m.batchTimePos = (m.batchTimePos + 1) % maxBatchHistory
	if m.batchTimeFill < maxBatchHistory {
		m.batchTimeFill++
	}
}

// RecordBatchTables updates per-table metrics from a batch's migration units.
// When FailedKeys are available in the result, failures are attributed to the
// specific table of each failed unit. Otherwise, written/failed/skipped are
// distributed proportionally by scan count as a fallback.
func (m *MetricsCollector) RecordBatchTables(units []provider.MigrationUnit, result *provider.BatchResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Group units by table for per-table tracking.
	tableScanned := make(map[string]int64)
	tableBytes := make(map[string]int64)
	tableUnits := make(map[string][]provider.MigrationUnit)
	for _, u := range units {
		tableScanned[u.Table]++
		tableBytes[u.Table] += u.Size
		tableUnits[u.Table] = append(tableUnits[u.Table], u)
	}

	// Build failed-key set for per-table attribution.
	failedSet := make(map[string]bool, len(result.FailedKeys))
	for _, k := range result.FailedKeys {
		failedSet[k] = true
	}

	totalScannedInBatch := int64(len(units))
	for table, scanned := range tableScanned {
		tm, ok := m.tableMetrics[table]
		if !ok {
			tm = &provider.TableMetrics{Table: table}
			m.tableMetrics[table] = tm
		}
		tm.Scanned += scanned
		tm.Bytes += tableBytes[table]
		tm.BatchCount++

		// Use per-unit attribution when FailedKeys are available.
		if len(failedSet) > 0 {
			var written, failed int64
			for _, u := range tableUnits[table] {
				if failedSet[u.Key] {
					failed++
				} else {
					written++
				}
			}
			tm.Written += written
			tm.Failed += failed
		} else if totalScannedInBatch > 0 {
			// Fallback: proportional distribution when no per-key outcome.
			ratio := float64(scanned) / float64(totalScannedInBatch)
			tm.Written += int64(float64(result.WrittenUnits) * ratio)
			tm.Failed += int64(float64(result.FailedUnits) * ratio)
			tm.Skipped += int64(float64(result.SkippedUnits) * ratio)
		}
		tm.Duration += result.Duration
	}
}

// RecordScanTable tracks per-table scan counts.
func (m *MetricsCollector) RecordScanTable(table string, count int64, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tm, ok := m.tableMetrics[table]
	if !ok {
		tm = &provider.TableMetrics{Table: table}
		m.tableMetrics[table] = tm
	}
	tm.Scanned += count
	tm.Bytes += bytes
}

// CurrentTable returns the table being actively processed (highest scan count).
func (m *MetricsCollector) CurrentTable() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentTableName()
}

// currentTableName returns the most-scanned table. Caller must hold the read lock.
func (m *MetricsCollector) currentTableName() string {
	var best string
	var bestCount int64
	for _, tm := range m.tableMetrics {
		if tm.Scanned > bestCount {
			bestCount = tm.Scanned
			best = tm.Table
		}
	}
	return best
}

// TableMetricsList returns a sorted slice of per-table metrics.
func (m *MetricsCollector) TableMetricsList() []provider.TableMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]provider.TableMetrics, 0, len(m.tableMetrics))
	for _, tm := range m.tableMetrics {
		result = append(result, *tm)
	}
	return result
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
		CurrentTable:     m.currentTableName(),
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

	// Per-table metrics
	tables := make([]provider.TableMetrics, 0, len(m.tableMetrics))
	for _, tm := range m.tableMetrics {
		tables = append(tables, *tm)
	}
	summary.TableMetrics = tables

	// Throughput
	elapsed := time.Since(m.startTime)
	if elapsed.Seconds() > 0 {
		summary.AvgThroughput = float64(m.totalWritten) / elapsed.Seconds()
	}
	summary.PeakThroughput = m.peakThroughput
}
