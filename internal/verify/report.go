package verify

import (
	"fmt"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/internal/util"
)

// FormatTable renders the report as a human-readable table with per-table
// breakdown, coverage metrics, and a clear trustworthiness verdict.
func FormatTable(report *VerificationReport) string {
	var b strings.Builder

	fmt.Fprintf(&b, "\n=== Verification Report ===\n")
	fmt.Fprintf(&b, "Source:      %s\n", report.SourceProvider)
	fmt.Fprintf(&b, "Destination: %s\n", report.DestProvider)
	fmt.Fprintf(&b, "Duration:    %s\n", report.Duration.Round(1e6))

	// Status badge with clear color-coding language.
	statusLabel := string(report.Status)
	switch report.Status {
	case StatusPass:
		statusLabel = "PASS"
	case StatusFail:
		statusLabel = "FAIL"
	case StatusWarn:
		statusLabel = "WARN"
	case StatusSkipped:
		statusLabel = "SKIPPED"
	}
	fmt.Fprintf(&b, "Status:      %s\n", statusLabel)

	// Verdict — the most important line.
	fmt.Fprintf(&b, "\n%s\n", report.Verdict())

	// Aggregate stats.
	if report.TotalTables > 0 {
		fmt.Fprintf(&b, "\nTABLES: %d total, %d passed, %d failed, %d warned, %d skipped\n",
			report.TotalTables, report.PassCount, report.FailCount, report.WarnCount, report.SkipCount)
	}

	// Row coverage.
	if report.TotalSrcRows > 0 {
		fmt.Fprintf(&b, "ROWS:   %d source, %d destination", report.TotalSrcRows, report.TotalDstRows)
		if report.TotalSrcRows == report.TotalDstRows {
			fmt.Fprint(&b, " (match)")
		} else {
			fmt.Fprintf(&b, " (delta: %+d)", report.TotalDstRows-report.TotalSrcRows)
		}
		fmt.Fprintln(&b)
	}

	// Sample coverage.
	if report.TotalSampled > 0 && report.TotalSrcRows > 0 {
		pct := float64(report.TotalSampled) / float64(report.TotalSrcRows) * 100
		fmt.Fprintf(&b, "SAMPLE: %d records checked (%.1f%% of source rows)\n",
			report.TotalSampled, pct)
	}

	// Per-table breakdown.
	if len(report.Tables) > 0 {
		fmt.Fprintf(&b, "\n%-30s %10s %10s %7s %7s %7s %s\n",
			"TABLE", "SRC", "DST", "DELTA", "SAMPLED", "ISSUES", "STATUS")
		fmt.Fprintf(&b, "%s\n", strings.Repeat("-", 90))
		for _, tr := range report.Tables {
			delta := fmt.Sprintf("%+d", tr.CountDelta)
			sampled := "-"
			if tr.SampleChecked > 0 {
				sampled = fmt.Sprintf("%d", tr.SampleChecked)
			}
			issues := fmt.Sprintf("%d", len(tr.Mismatches))
			fmt.Fprintf(&b, "%-30s %10d %10d %7s %7s %7s %s\n",
				truncate(tr.Table, 30), tr.SrcCount, tr.DstCount, delta, sampled, issues, tr.Status)
		}
	}

	// Mismatch detail (show up to 20, then summary).
	if len(report.Mismatches) > 0 {
		fmt.Fprintf(&b, "\nMISMATCHES (%d total)\n", len(report.Mismatches))
		limit := len(report.Mismatches)
		if limit > 20 {
			limit = 20
		}
		for _, m := range report.Mismatches[:limit] {
			fmt.Fprintf(&b, "  %s\n", m.String())
			for _, fd := range m.FieldDiffs {
				fmt.Fprintf(&b, "    %-20s src=%v  dst=%v\n", fd.Field, fd.SourceValue, fd.DestValue)
			}
		}
		if len(report.Mismatches) > 20 {
			fmt.Fprintf(&b, "  ... and %d more\n", len(report.Mismatches)-20)
		}
	}

	if len(report.Skipped) > 0 {
		fmt.Fprintf(&b, "\nSKIPPED\n")
		for _, s := range report.Skipped {
			fmt.Fprintf(&b, "  - %s\n", s)
		}
	}

	return b.String()
}

// FormatCompact renders a one-line verification status for successful reports.
func FormatCompact(report *VerificationReport) string {
	return fmt.Sprintf("Verification: %s (%d tables, %d sampled, %s)",
		report.Status, report.TotalTables, report.TotalSampled,
		util.FormatDuration(report.Duration))
}

// FormatJSON renders the report as JSON.
func FormatJSON(report *VerificationReport) (string, error) {
	data, err := sonic.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal verification report: %w", err)
	}
	return string(data), nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max > 3 {
		return s[:max-3] + "..."
	}
	return s[:max]
}
