package verify

import (
	"fmt"
	"sort"
	"time"
)

// ---------------------------------------------------------------------------
// Status and verdict
// ---------------------------------------------------------------------------

// VerificationStatus represents the overall verification outcome.
type VerificationStatus string

const (
	// StatusPass means all checks passed — the migration is trustworthy.
	StatusPass VerificationStatus = "PASS"

	// StatusWarn means checks ran but some non-critical issues were found.
	// The migration is likely usable but needs manual spot-checking.
	StatusWarn VerificationStatus = "WARN"

	// StatusFail means significant mismatches were found — the migration
	// needs manual inspection before relying on the data.
	StatusFail VerificationStatus = "FAIL"

	// StatusSkipped means verification could not run (e.g. providers don't
	// support the required interfaces).
	StatusSkipped VerificationStatus = "SKIPPED"

	// StatusError means verification encountered a runtime error.
	StatusError VerificationStatus = "ERROR"
)

// ---------------------------------------------------------------------------
// Mismatch detail
// ---------------------------------------------------------------------------

// MismatchCategory classifies the kind of verification mismatch.
type MismatchCategory string

const (
	MismatchCountMismatch MismatchCategory = "count_mismatch"
	MismatchMissingInDst  MismatchCategory = "missing_in_destination"
	MismatchExtraInDst    MismatchCategory = "extra_in_destination"
	MismatchValueDiff     MismatchCategory = "value_diff"
	MismatchChecksumDiff  MismatchCategory = "checksum_diff"
)

// FieldDiff describes a single field-level difference between source and destination.
type FieldDiff struct {
	Field       string `json:"field"`
	SourceValue any    `json:"source_value"`
	DestValue   any    `json:"dest_value"`
}

// MismatchDetail describes a single mismatch found during verification.
type MismatchDetail struct {
	Category    MismatchCategory `json:"category"`
	Table       string           `json:"table"`
	Schema      string           `json:"schema,omitempty"`
	Key         string           `json:"key,omitempty"`
	SrcCount    int64            `json:"src_count,omitempty"`
	DstCount    int64            `json:"dst_count,omitempty"`
	FieldDiffs  []FieldDiff      `json:"field_diffs,omitempty"`
	SrcChecksum string           `json:"src_checksum,omitempty"`
	DstChecksum string           `json:"dst_checksum,omitempty"`
	Message     string           `json:"message"`
}

// String returns a human-readable summary of the mismatch.
func (m MismatchDetail) String() string {
	switch m.Category {
	case MismatchCountMismatch:
		delta := m.DstCount - m.SrcCount
		return fmt.Sprintf("[%s] %s: src=%d, dst=%d (delta: %+d)",
			m.Category, m.Table, m.SrcCount, m.DstCount, delta)
	case MismatchMissingInDst:
		return fmt.Sprintf("[%s] %s key %q not found in destination",
			m.Category, m.Table, m.Key)
	case MismatchExtraInDst:
		return fmt.Sprintf("[%s] %s key %q exists in destination but not in source",
			m.Category, m.Table, m.Key)
	case MismatchValueDiff:
		return fmt.Sprintf("[%s] %s key %q: %d field(s) differ",
			m.Category, m.Table, m.Key, len(m.FieldDiffs))
	case MismatchChecksumDiff:
		return fmt.Sprintf("[%s] %s key %q: src=%s dst=%s",
			m.Category, m.Table, m.Key, m.SrcChecksum, m.DstChecksum)
	default:
		return m.Message
	}
}

// ---------------------------------------------------------------------------
// Per-table result
// ---------------------------------------------------------------------------

// TableResult records the verification outcome for a single table.
type TableResult struct {
	// Table is the table or collection name.
	Table string `json:"table"`

	// SrcCount is the row count on the source (0 if unavailable).
	SrcCount int64 `json:"src_count"`

	// DstCount is the row count on the destination (0 if unavailable).
	DstCount int64 `json:"dst_count"`

	// CountDelta is DstCount - SrcCount.
	CountDelta int64 `json:"count_delta"`

	// CountMatch is true when SrcCount == DstCount.
	CountMatch bool `json:"count_match"`

	// SampleTarget is how many records we intended to sample.
	SampleTarget int `json:"sample_target"`

	// SampleChecked is how many records were actually compared.
	SampleChecked int `json:"sample_checked"`

	// SampleMismatch is how many of the checked records had differences.
	SampleMismatch int `json:"sample_mismatch"`

	// Mismatches are the detailed mismatches found for this table.
	Mismatches []MismatchDetail `json:"mismatches,omitempty"`

	// Status is the per-table verification outcome.
	Status VerificationStatus `json:"status"`
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

// VerificationReport is the complete output of a verification run.
type VerificationReport struct {
	SourceProvider string        `json:"source_provider"`
	DestProvider   string        `json:"dest_provider"`
	Timestamp      time.Time     `json:"timestamp"`
	Duration       time.Duration `json:"duration"`

	// Status is the top-level verification verdict.
	Status VerificationStatus `json:"status"`

	// Per-table results in table-name order.
	Tables []TableResult `json:"tables"`

	// Aggregate counts.
	TotalTables int `json:"total_tables"`
	PassCount   int `json:"pass_count"`
	FailCount   int `json:"fail_count"`
	WarnCount   int `json:"warn_count"`
	SkipCount   int `json:"skip_count"`

	// Coverage metrics.
	TotalSrcRows    int64 `json:"total_src_rows"`
	TotalDstRows    int64 `json:"total_dst_rows"`
	TotalSampled    int   `json:"total_sampled"`
	TotalMismatches int   `json:"total_mismatches"`

	// Flat mismatch list for backward compatibility.
	Mismatches []MismatchDetail `json:"mismatches"`

	// Skipped lists check types that were not performed.
	Skipped []string `json:"skipped,omitempty"`

	// VerifiedAny is true when at least one check ran.
	VerifiedAny bool `json:"verified_any"`
}

// Passed returns true when verification ran and found no mismatches.
func (r *VerificationReport) Passed() bool {
	return r.VerifiedAny && len(r.Mismatches) == 0
}

// Verdict returns a human-readable verdict with a recommendation.
func (r *VerificationReport) Verdict() string {
	switch r.Status {
	case StatusPass:
		return fmt.Sprintf("PASS — migration is trustworthy. %d table(s) verified in %s.",
			r.TotalTables, r.Duration.Round(time.Millisecond))
	case StatusWarn:
		return fmt.Sprintf("WARN — minor issues detected. %d/%d table(s) have mismatches (%d total). Spot-check the flagged tables before relying on the data.",
			r.WarnCount, r.TotalTables, r.TotalMismatches)
	case StatusFail:
		return fmt.Sprintf("FAIL — significant mismatches detected. %d/%d table(s) failed (%d total issues). Manual inspection required.",
			r.FailCount+r.WarnCount, r.TotalTables, r.TotalMismatches)
	case StatusSkipped:
		return "SKIPPED — verification could not run. Manually verify destination data."
	case StatusError:
		return "ERROR — verification encountered a runtime error. Manually verify destination data."
	default:
		return "UNKNOWN — no verification result available."
	}
}

// Summary returns a one-line status string.
func (r *VerificationReport) Summary() string {
	if !r.VerifiedAny {
		return fmt.Sprintf("SKIPPED (no verification checks could run, %s)", r.Duration.Round(time.Millisecond))
	}
	if r.Passed() {
		return fmt.Sprintf("PASS (%d tables verified, %s)", r.TotalTables, r.Duration.Round(time.Millisecond))
	}
	return fmt.Sprintf("FAIL (%d/%d tables with mismatches, %d total issues, %s)",
		r.FailCount+r.WarnCount, r.TotalTables, len(r.Mismatches), r.Duration.Round(time.Millisecond))
}

// computeStatus derives the overall status from per-table results.
func (r *VerificationReport) computeStatus() {
	if !r.VerifiedAny {
		r.Status = StatusSkipped
		return
	}
	if r.FailCount > 0 {
		r.Status = StatusFail
		return
	}
	if r.WarnCount > 0 {
		r.Status = StatusWarn
		return
	}
	r.Status = StatusPass
}

// sortTables sorts Tables by name for deterministic output.
func (r *VerificationReport) sortTables() {
	sort.Slice(r.Tables, func(i, j int) bool {
		return r.Tables[i].Table < r.Tables[j].Table
	})
}
