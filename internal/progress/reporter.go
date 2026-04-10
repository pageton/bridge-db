// Package progress provides progress reporting implementations for the
// migration pipeline.
package progress

import (
	"errors"
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

func (NopReporter) OnBatchStart(_ int, _ int)                                  {}
func (NopReporter) OnBatchComplete(_ int, _ *provider.BatchResult)             {}
func (NopReporter) OnMigrationComplete(_ *provider.MigrationSummary)           {}
func (NopReporter) OnError(_ error, _ *provider.MigrationUnit)                 {}
func (NopReporter) OnPhaseChange(_ provider.MigrationPhase)                    {}
func (NopReporter) OnPhaseStart(_ provider.PhaseDesc)                          {}
func (NopReporter) OnPhaseDone(_ provider.PhaseDesc, _ time.Duration, _ error) {}
func (NopReporter) OnProgress(_ provider.ProgressStats)                        {}

// ---------------------------------------------------------------------------
// ConsoleReporter
// ---------------------------------------------------------------------------

// ConsoleReporter prints structured, numbered phase output to an io.Writer.
// Each phase shows a clear header line, live progress during data transfer,
// and an indented completion line (done / skipped / FAILED).
type ConsoleReporter struct {
	w       io.Writer
	mu      sync.Mutex
	last    string // last carriage-return line (for overwriting)
	current provider.PhaseDesc
}

// NewConsoleReporter creates a reporter that writes to os.Stdout.
func NewConsoleReporter() *ConsoleReporter {
	return &ConsoleReporter{w: os.Stdout}
}

// NewConsoleReporterWith creates a reporter that writes to the given writer.
func NewConsoleReporterWith(w io.Writer) *ConsoleReporter {
	return &ConsoleReporter{w: w}
}

// OnBatchStart is a no-op in the console reporter; progress is shown via
// OnProgress instead of per-batch messages.
func (r *ConsoleReporter) OnBatchStart(_ int, _ int) {}

// OnBatchComplete is a no-op in the console reporter.
func (r *ConsoleReporter) OnBatchComplete(_ int, _ *provider.BatchResult) {}

// OnPhaseStart prints the phase header: [N/M] Description
func (r *ConsoleReporter) OnPhaseStart(desc provider.PhaseDesc) {
	r.mu.Lock()
	r.current = desc
	r.mu.Unlock()
	r.println(fmt.Sprintf("[%d/%d] %s", desc.Step, desc.TotalSteps, desc.Description))
}

// OnPhaseDone prints the phase completion line, indented under the header.
//   - success: "      done (duration)"
//   - skipped: "      skipped"
//   - error: "      FAILED: message"
func (r *ConsoleReporter) OnPhaseDone(desc provider.PhaseDesc, dur time.Duration, err error) {
	if err != nil {
		if errors.Is(err, provider.ErrPhaseSkipped) {
			r.println("      skipped")
			return
		}
		r.println(fmt.Sprintf("      FAILED: %s", err))
		return
	}
	r.println(fmt.Sprintf("      done (%s)", util.FormatDuration(dur)))
}

// OnPhaseChange is a legacy entry point kept for backward compat. It is a
// no-op; the pipeline uses OnPhaseStart/OnPhaseDone instead.
func (r *ConsoleReporter) OnPhaseChange(_ provider.MigrationPhase) {}

// OnMigrationComplete prints a brief completion line after all phases.
// The detailed summary is printed separately by the CLI layer.
func (r *ConsoleReporter) OnMigrationComplete(summary *provider.MigrationSummary) {
	r.println("")
	r.println("\u2714 Migration completed successfully")
	r.println(fmt.Sprintf("  %d records written in %s",
		summary.TotalWritten, util.FormatDuration(summary.Duration)))
}

// OnError prints an error indented under the current phase.
func (r *ConsoleReporter) OnError(err error, _ *provider.MigrationUnit) {
	r.println(fmt.Sprintf("      [ERROR] %s", err.Error()))
}

// OnProgress shows a live progress line during data transfer, overwritten
// in-place using carriage returns.
func (r *ConsoleReporter) OnProgress(stats provider.ProgressStats) {
	elapsed := util.FormatDuration(stats.Elapsed)
	eta := util.FormatDuration(stats.EstimatedRemain)

	var parts []string
	parts = append(parts, fmt.Sprintf("%d written", stats.TotalWritten))
	parts = append(parts, fmt.Sprintf("%.0f records/s", stats.Throughput))
	parts = append(parts, elapsed)

	if stats.CurrentTable != "" {
		parts = append(parts, fmt.Sprintf("table: %s", stats.CurrentTable))
	}
	if stats.TablesTotal > 0 {
		parts = append(parts, fmt.Sprintf("%d/%d tables", stats.TablesCompleted, stats.TablesTotal))
	}
	parts = append(parts, fmt.Sprintf("ETA: %s", eta))

	r.printf("      %s", strings.Join(parts, " | "))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (r *ConsoleReporter) printf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	line := fmt.Sprintf(format, args...)
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
	r.last = line
}

func (r *ConsoleReporter) println(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.last != "" {
		// Clear the current \r progress line before printing a new line.
		clearLen := len(r.last)
		if clearLen > len(line) {
			_, _ = fmt.Fprintf(r.w, "\r%s\r%s\n", strings.Repeat(" ", clearLen), line)
		} else {
			_, _ = fmt.Fprintf(r.w, "\r%s\n", line)
		}
	} else {
		_, _ = fmt.Fprintln(r.w, line)
	}
	r.last = ""
}
