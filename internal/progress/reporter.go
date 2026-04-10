// Package progress provides progress reporting implementations for the
// migration pipeline.
package progress

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pageton/bridge-db/internal/util"
	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// NopReporter
// ---------------------------------------------------------------------------

// NopReporter silently discards all progress events. Use it for library-mode
// callers that don't need progress feedback.
type NopReporter struct{}

func (NopReporter) OnBatchStart(_ int, _ int)                        {}
func (NopReporter) OnBatchComplete(_ int, _ *provider.BatchResult)   {}
func (NopReporter) OnMigrationComplete(_ *provider.MigrationSummary) {}
func (NopReporter) OnError(_ error, _ *provider.MigrationUnit)       {}
func (NopReporter) OnPhaseChange(_ provider.MigrationPhase)          {}
func (NopReporter) OnProgress(_ provider.ProgressStats)              {}

// ---------------------------------------------------------------------------
// ConsoleReporter
// ---------------------------------------------------------------------------

// ConsoleReporter prints progress to an io.Writer using carriage-return
// overwriting for a compact single-line progress display.
type ConsoleReporter struct {
	w    io.Writer
	mu   sync.Mutex
	last string
}

// NewConsoleReporter creates a reporter that writes to os.Stdout.
func NewConsoleReporter() *ConsoleReporter {
	return &ConsoleReporter{w: os.Stdout}
}

// NewConsoleReporterWith creates a reporter that writes to the given writer.
func NewConsoleReporterWith(w io.Writer) *ConsoleReporter {
	return &ConsoleReporter{w: w}
}

func (r *ConsoleReporter) OnBatchStart(batchID int, unitCount int) {
	r.printf("  [BATCH %d] Processing %d units...", batchID, unitCount)
}

func (r *ConsoleReporter) OnBatchComplete(batchID int, result *provider.BatchResult) {
	r.printf("  [BATCH %d] Written %d | Skipped %d | Failed %d | %v",
		batchID, result.WrittenUnits, result.SkippedUnits, result.FailedUnits, result.Duration.Round(0))
}

func (r *ConsoleReporter) OnMigrationComplete(summary *provider.MigrationSummary) {
	r.println("")
	r.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	r.println("  Migration Complete")
	r.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	r.println(fmt.Sprintf("  Duration:     %s", summary.Duration.Round(time.Millisecond)))
	r.println(fmt.Sprintf("  Records:      %d scanned / %d written / %d failed / %d skipped",
		summary.TotalScanned, summary.TotalWritten, summary.TotalFailed, summary.TotalSkipped))
	r.println(fmt.Sprintf("  Transferred:  %s", util.HumanBytes(summary.BytesTransferred)))
	if summary.AvgThroughput > 0 {
		r.println(fmt.Sprintf("  Throughput:   %.0f avg / %.0f peak units/s", summary.AvgThroughput, summary.PeakThroughput))
	}
	if summary.VerificationOK {
		r.println("  Verification: PASSED")
	} else if len(summary.VerificationErrs) > 0 {
		r.println(fmt.Sprintf("  Verification: %d MISMATCHES", len(summary.VerificationErrs)))
	}
	if len(summary.Errors) > 0 {
		r.println(fmt.Sprintf("  Errors:       %d", len(summary.Errors)))
	}

	// Per-table breakdown
	if len(summary.TableMetrics) > 0 {
		r.println("")
		r.println("  Per-table breakdown:")
		r.println(fmt.Sprintf("  %-30s %8s %8s %8s %10s", "Table", "Scanned", "Written", "Failed", "Size"))
		r.println("  " + strings.Repeat("-", 70))
		for _, tm := range summary.TableMetrics {
			r.println(fmt.Sprintf("  %-30s %8d %8d %8d %10s",
				util.Truncate(tm.Table, 30), tm.Scanned, tm.Written, tm.Failed, util.HumanBytes(tm.Bytes)))
		}
	}

	r.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

func (r *ConsoleReporter) OnError(err error, _ *provider.MigrationUnit) {
	r.println(fmt.Sprintf("  [ERROR] %s", err.Error()))
}

func (r *ConsoleReporter) OnPhaseChange(phase provider.MigrationPhase) {
	label := phaseLabel(phase)
	r.println(fmt.Sprintf("[%s]", label))
}

func (r *ConsoleReporter) OnProgress(stats provider.ProgressStats) {
	label := phaseLabel(stats.Phase)
	eta := formatDuration(stats.EstimatedRemain)
	elapsed := formatDuration(stats.Elapsed)

	var tableInfo string
	if stats.CurrentTable != "" {
		tableInfo = fmt.Sprintf(" | table: %s", stats.CurrentTable)
	}
	var tableProgress string
	if stats.TablesTotal > 0 {
		tableProgress = fmt.Sprintf(" | %d/%d tables", stats.TablesCompleted, stats.TablesTotal)
	}

	r.printf("[%s] %d written | %.0f units/s | %s%s%s | ETA: %s",
		label, stats.TotalWritten, stats.Throughput, elapsed,
		tableInfo, tableProgress, eta,
	)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (r *ConsoleReporter) printf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	line := fmt.Sprintf(format, args...)
	if strings.HasPrefix(line, "\r") {
		_, _ = fmt.Fprint(r.w, line)
	} else {
		if r.last != "" {
			clearLen := len(r.last)
			if clearLen > len(line) {
				_, _ = fmt.Fprintf(r.w, "\r%s\r%s", strings.Repeat(" ", clearLen), line)
			} else {
				_, _ = fmt.Fprintf(r.w, "\r%s", line)
			}
		} else {
			_, _ = fmt.Fprint(r.w, line)
		}
	}
	r.last = line
}

func (r *ConsoleReporter) println(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = fmt.Fprintln(r.w, line)
	r.last = ""
}

func phaseLabel(phase provider.MigrationPhase) string {
	switch phase {
	case provider.PhaseInit:
		return "INIT"
	case provider.PhaseTunnel:
		return "TUNNEL"
	case provider.PhaseConnect:
		return "CONNECTING"
	case provider.PhaseSchemaMigration:
		return "SCHEMA"
	case provider.PhaseScanning:
		return "SCANNING"
	case provider.PhaseTransforming:
		return "TRANSFORMING"
	case provider.PhaseWriting:
		return "WRITING"
	case provider.PhaseVerifying:
		return "VERIFYING"
	case provider.PhaseComplete:
		return "COMPLETE"
	case provider.PhaseError:
		return "ERROR"
	case provider.PhasePaused:
		return "PAUSED"
	default:
		return strings.ToUpper(string(phase))
	}
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}
