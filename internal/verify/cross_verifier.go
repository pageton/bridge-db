package verify

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// CrossVerifier performs bidirectional verification between source and destination.
type CrossVerifier struct {
	src     provider.Provider
	dst     provider.Provider
	srcName string
	dstName string
	opts    Options
}

// NewCrossVerifier creates a new cross-verifier.
func NewCrossVerifier(src, dst provider.Provider, opts Options) *CrossVerifier {
	if err := opts.Validate(); err != nil {
		opts = DefaultOptions()
	}
	return &CrossVerifier{
		src:     src,
		dst:     dst,
		srcName: src.Name(),
		dstName: dst.Name(),
		opts:    opts,
	}
}

// Verify runs all enabled verification phases and returns a report.
//
// The verification proceeds in three phases:
//
//  1. Count comparison — enumerate tables on both sides and compare row counts.
//  2. Sampling — read a subset of records from the source and compare against
//     the destination.
//  3. Checksum or field comparison — for sampled records, verify data integrity
//     using checksums (when available) or field-by-field comparison.
//
// Each phase contributes to a per-table TableResult and the aggregate report.
func (cv *CrossVerifier) Verify(ctx context.Context) (*VerificationReport, error) {
	log := logger.L().With("component", "cross-verifier")
	start := time.Now()

	report := &VerificationReport{
		SourceProvider: cv.srcName,
		DestProvider:   cv.dstName,
		Timestamp:      start,
	}

	// Determine table counts from both sides.
	srcCounts, err := cv.enumerateCounts(ctx, cv.src, "source")
	if err != nil {
		return nil, err
	}
	dstCounts, err := cv.enumerateCounts(ctx, cv.dst, "destination")
	if err != nil {
		return nil, err
	}

	if srcCounts == nil && dstCounts == nil {
		log.Warn("neither provider supports table enumeration")
		report.Skipped = append(report.Skipped, "count_comparison (not supported)")
		report.Skipped = append(report.Skipped, "sampling (requires table enumeration)")
		report.Status = StatusSkipped
		report.Duration = time.Since(start)
		return report, nil
	}

	log.Debug("enumerated tables", "source_tables", len(srcCounts), "dest_tables", len(dstCounts))
	report.VerifiedAny = true

	// Normalize counts for cross-provider table name matching while preserving
	// source-side raw table identifiers for schema-aware sampling.
	srcNorm, srcRaw := normalizeTableCounts(srcCounts)
	dstNorm, _ := normalizeTableCounts(dstCounts)
	if counts, ok := remapDestinationCounts(cv.srcName, cv.dstName, srcNorm, dstNorm); ok {
		dstNorm = counts
	}
	if counts, ok := remapSourceCounts(cv.srcName, cv.dstName, srcNorm, dstNorm); ok {
		srcNorm = counts
	}

	// Phase 1: Build per-table results from count comparison.
	cv.buildTableResults(srcNorm, dstNorm, report)

	// Phase 2+3: Sampling and data comparison.
	if cv.opts.SampleMode != "none" {
		cv.runSampling(ctx, srcNorm, dstNorm, srcRaw, report)
	}

	// Finalize.
	report.Duration = time.Since(start)
	report.sortTables()
	report.computeStatus()
	cv.aggregateStats(report)

	cv.logResult(report)
	return report, nil
}

func remapDestinationCounts(srcName, dstName string, srcCounts, dstCounts map[string]int64) (map[string]int64, bool) {
	if len(dstCounts) != 1 {
		return nil, false
	}
	count, ok := dstCounts[""]
	if !ok {
		return nil, false
	}
	if dstName != "redis" || srcName != "sqlite" && srcName != "mysql" && srcName != "postgres" && srcName != "mariadb" && srcName != "cockroachdb" && srcName != "mssql" {
		return nil, false
	}
	if len(srcCounts) == 0 {
		return nil, false
	}
	remapped := make(map[string]int64, len(srcCounts))
	for table, srcCount := range srcCounts {
		remapped[table] = srcCount
	}
	var total int64
	for _, c := range remapped {
		total += c
	}
	if total != count {
		return nil, false
	}
	return remapped, true
}

func remapSourceCounts(srcName, dstName string, srcCounts, dstCounts map[string]int64) (map[string]int64, bool) {
	if len(srcCounts) != 1 {
		return nil, false
	}
	count, ok := srcCounts[""]
	if !ok {
		return nil, false
	}
	if srcName != "redis" || dstName != "mongodb" {
		return nil, false
	}
	if len(dstCounts) == 0 {
		return nil, false
	}
	var total int64
	for _, c := range dstCounts {
		total += c
	}
	if total != count {
		return nil, false
	}
	remapped := make(map[string]int64, len(dstCounts))
	for table, dstCount := range dstCounts {
		remapped[table] = dstCount
	}
	return remapped, true
}

// enumerateCounts calls EnumerateTables on a provider, returning nil if the
// interface is not supported.
func (cv *CrossVerifier) enumerateCounts(ctx context.Context, p provider.Provider, side string) (map[string]int64, error) {
	te, ok := p.(provider.TableEnumerator)
	if !ok {
		return nil, nil
	}
	counts, err := te.EnumerateTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("enumerate %s tables: %w", side, err)
	}
	return counts, nil
}

// buildTableResults creates a TableResult for every table found on either side,
// populating count information and marking count mismatches.
// Table names should be pre-normalized (stripped "public." prefix) by the caller
// so that cross-provider comparisons match correctly.
func (cv *CrossVerifier) buildTableResults(srcCounts, dstCounts map[string]int64, report *VerificationReport) {
	seen := make(map[string]bool)

	addResult := func(table string, srcCount, dstCount int64) {
		if seen[table] {
			return
		}
		seen[table] = true

		delta := dstCount - srcCount
		match := srcCount == dstCount

		tr := TableResult{
			Table:      table,
			SrcCount:   srcCount,
			DstCount:   dstCount,
			CountDelta: delta,
			CountMatch: match,
			Status:     StatusPass,
		}

		if !match && cv.opts.CountComparison {
			tr.Mismatches = append(tr.Mismatches, MismatchDetail{
				Category: MismatchCountMismatch,
				Table:    table,
				SrcCount: srcCount,
				DstCount: dstCount,
				Message:  fmt.Sprintf("table %q count mismatch: source=%d, destination=%d", table, srcCount, dstCount),
			})
			tr.Status = StatusFail
		}

		report.Tables = append(report.Tables, tr)
		report.TotalSrcRows += srcCount
		report.TotalDstRows += dstCount
	}

	for t, sc := range srcCounts {
		dc := dstCounts[t]
		addResult(t, sc, dc)
	}
	for t, dc := range dstCounts {
		if !seen[t] {
			addResult(t, 0, dc)
		}
	}
}

// normalizeTableCounts strips the "public." schema prefix from table name keys.
// PostgreSQL and CockroachDB prefix tables with "public." in EnumerateTables;
// other providers do not. Normalizing ensures cross-provider count matching works.

func normalizeTableCounts(counts map[string]int64) (map[string]int64, map[string][]string) {
	out := make(map[string]int64, len(counts))
	raw := make(map[string][]string, len(counts))
	for table, count := range counts {
		key := strings.TrimPrefix(table, "public.")
		out[key] = count
		raw[key] = append(raw[key], table)
	}
	for key := range raw {
		sort.Strings(raw[key])
	}
	return out, raw
}

// findTableResult returns a pointer to the TableResult for the given table.
func findTableResult(report *VerificationReport, table string) *TableResult {
	for i := range report.Tables {
		if report.Tables[i].Table == table {
			return &report.Tables[i]
		}
	}
	return nil
}

// runSampling iterates over source tables, samples keys, and compares them.
func (cv *CrossVerifier) runSampling(ctx context.Context, srcCounts, dstCounts map[string]int64, srcRaw map[string][]string, report *VerificationReport) {
	srcReader, srcHasReader := cv.src.(provider.VerifyReader)
	dstReader, dstHasReader := cv.dst.(provider.VerifyReader)

	srcChecksum, srcHasChecksum := cv.src.(provider.Checksummer)
	dstChecksum, dstHasChecksum := cv.dst.(provider.Checksummer)

	hasReader := srcHasReader && dstHasReader
	useChecksum := cv.opts.ChecksumComparison && srcHasChecksum && dstHasChecksum && cv.src.Name() == cv.dst.Name()

	if !hasReader && !useChecksum {
		report.Skipped = append(report.Skipped, "sampling (VerifyReader/Checksummer not supported)")
		return
	}

	remainingBudget := cv.opts.MaxSampleKeys
	if remainingBudget <= 0 {
		remainingBudget = 10000
	}

	for table, srcCount := range srcCounts {
		if ctx.Err() != nil {
			return
		}

		tr := findTableResult(report, table)
		if tr == nil {
			continue
		}

		// Only sample tables with matching counts (skip count-mismatched tables
		// — sampling won't add useful info and may mislead).
		if !tr.CountMatch {
			continue
		}

		n := cv.sampleSize(srcCount)
		if n == 0 {
			continue
		}
		if n > remainingBudget {
			n = remainingBudget
		}
		if n == 0 {
			continue
		}
		remainingBudget -= n

		tr.SampleTarget = n

		// Sample keys from the source.
		rawTables := srcRaw[table]
		if len(rawTables) == 0 {
			if cv.srcName == "redis" && cv.dstName == "mongodb" {
				continue
			}
			rawTables = []string{table}
		}
		keys, err := cv.sampleKeys(ctx, rawTables[0], n, flattenRawTables(srcRaw))
		if err != nil {
			report.Skipped = append(report.Skipped, fmt.Sprintf("%s sampling: %v", table, err))
			tr.Status = StatusWarn
			continue
		}
		if len(keys) == 0 {
			continue
		}

		tr.SampleChecked = len(keys)
		report.TotalSampled += len(keys)

		// Compare sampled records.
		if useChecksum {
			compareChecksums(ctx, srcChecksum, dstChecksum, table, keys, tr)
		} else if hasReader {
			compareRecords(ctx, srcReader, dstReader, table, keys, tr)
		}

		// Upgrade table status if sampling found issues.
		if tr.SampleMismatch > 0 && tr.Status == StatusPass {
			tr.Status = StatusWarn
		}
	}
}

// sampleSize computes how many records to sample for a given table count.
func (cv *CrossVerifier) sampleSize(count int64) int {
	switch cv.opts.SampleMode {
	case "count":
		n := cv.opts.SampleN
		if int64(n) > count {
			return int(count)
		}
		return n
	case "pct":
		n := int(float64(count) * cv.opts.SamplePct / 100.0)
		if n < 1 && count > 0 {
			n = 1
		}
		return n
	default:
		return 0
	}
}

// sampleKeys reads keys from a specific source table by using TablesCompleted
// filtering to skip all other tables.  This ensures we sample from the correct
// table regardless of how the scanner enumerates tables internally.
func (cv *CrossVerifier) sampleKeys(ctx context.Context, table string, n int, allTables []string) ([]string, error) {
	// Build a skip list of every table except the target.
	var skip []string
	for _, t := range allTables {
		if t != table {
			skip = append(skip, t)
		}
	}

	scanner := cv.src.Scanner(ctx, provider.ScanOptions{
		BatchSize:       n,
		TablesCompleted: skip,
	})
	defer func() { _ = scanner.Close() }()

	var keys []string
	for len(keys) < n {
		units, err := scanner.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("scan table %q: %w", table, err)
		}
		if len(units) == 0 {
			break
		}
		for _, u := range units {
			keys = append(keys, u.Key)
			if len(keys) >= n {
				break
			}
		}
	}
	return keys, nil
}

func flattenRawTables(srcRaw map[string][]string) []string {
	tables := make([]string, 0, len(srcRaw))
	for _, rawTables := range srcRaw {
		tables = append(tables, rawTables...)
	}
	sort.Strings(tables)
	return tables
}

// aggregateStats computes pass/fail/warn/skip counts and the flat mismatch list.
func (cv *CrossVerifier) aggregateStats(report *VerificationReport) {
	report.TotalTables = len(report.Tables)
	report.PassCount = 0
	report.FailCount = 0
	report.WarnCount = 0
	report.SkipCount = 0
	report.Mismatches = nil
	report.TotalMismatches = 0

	for i := range report.Tables {
		tr := &report.Tables[i]
		switch tr.Status {
		case StatusPass:
			report.PassCount++
		case StatusFail:
			report.FailCount++
		case StatusWarn:
			report.WarnCount++
		case StatusSkipped:
			report.SkipCount++
		}
		report.Mismatches = append(report.Mismatches, tr.Mismatches...)
		report.TotalMismatches += len(tr.Mismatches)
	}
}

// logResult logs the final verification outcome.
func (cv *CrossVerifier) logResult(report *VerificationReport) {
	log := logger.L().With("component", "cross-verifier")
	if !report.VerifiedAny {
		log.Warn("verification skipped", "skipped", report.Skipped)
		return
	}
	if report.Passed() {
		log.Debug("verification passed",
			"tables", report.TotalTables,
			"sampled", report.TotalSampled,
			"duration", report.Duration)
		return
	}
	log.Warn("verification found mismatches",
		"status", report.Status,
		"tables", report.TotalTables,
		"passed", report.PassCount,
		"failed", report.FailCount,
		"warned", report.WarnCount,
		"mismatches", report.TotalMismatches,
		"sampled", report.TotalSampled,
		"duration", report.Duration,
	)
}
