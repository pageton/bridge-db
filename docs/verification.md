# Verification

Post-migration verification checks data integrity between the source and destination databases. It runs automatically as pipeline Step 9 when `--verify` is enabled (on by default). This document explains the verification levels, how the CrossVerifier works, and how to interpret results.

## Overview

Verification is a **statistical check**, not an exhaustive row-by-row comparison. It compares:

1. **Row counts** per table/collection (exact)
2. **Sampled records** (default 5% per table, capped at 10,000 total keys)
3. **Data values** of sampled records via checksum or field-by-field comparison

## Verification Levels

The verification level is determined by `provider.EffectiveVerifyLevel(srcCaps, dstCaps)` based on provider capabilities:

| Level | Condition | Verifier Used | What It Checks |
|---|---|---|---|
| `VerifyCross` | Both providers support verification | `CrossVerifier` | Row counts + sampled data comparison |
| `VerifyBasic` | Only destination supports verification | Legacy `dst.Verify(ctx, keys)` | Destination-only count check |
| `VerifyNone` | Neither provider supports verification | None | Skipped with warning |

## CrossVerifier Flow

When both providers support verification, the `CrossVerifier` runs three phases:

### Phase 1: Count Comparison

Enumerates tables on both sides via the `TableEnumerator` interface and compares row counts per table.

```go
// internal/verify/cross_verifier.go
type TableEnumerator interface {
    EnumerateTables(ctx context.Context) (map[string]int64, error)
}
```

- Source and destination tables are matched by name.
- Table names are normalized for comparison (e.g., PostgreSQL/CockroachDB `"public.users"` → `"users"`).
- Count delta per table is reported.

### Phase 2: Sampling

For tables with matching counts, records are sampled from the source:

```go
// internal/verify/options.go
type Options struct {
    SampleMode  string // "none", "count", "pct"
    SampleN     int    // fixed count (when SampleMode == "count")
    SamplePct   int    // percentage (when SampleMode == "pct")
}
```

**Defaults:**
- Mode: `pct`
- Percentage: 5% per table
- Cap: 10,000 total keys across all tables

**How sampling works:**
1. The scanner is asked to scan only the target table (using `TablesCompleted` to filter).
2. Keys are collected until the sample target is reached.
3. Sampled keys are then compared on both sides.

### Phase 3: Data Comparison

For each sampled key, the CrossVerifier compares source and destination records:

**Checksum comparison** (`compareChecksums`):
- Used when both providers implement the `Checksummer` interface **and** are the same provider type.
- MD5 hashes are compared per record.

```go
type Checksummer interface {
    ComputeChecksums(ctx context.Context, keys []string) (map[string]string, error)
}
```

**Field-by-field comparison** (`compareRecords`):
- Used when providers implement `VerifyReader` but are different provider types (cross-engine).
- Records are read from both sides and compared field by field with type coercion.

```go
type VerifyReader interface {
    ReadRecords(ctx context.Context, keys []string) (map[string]map[string]any, error)
}
```

### Type Coercion Rules

Cross-engine field comparison applies type coercion to handle serialization differences:

| Scenario | Handling |
|---|---|
| Numeric mismatch (e.g., `1` vs `1.0`) | Both parsed as `float64` and compared |
| Time format difference | Parsed with multiple format attempts |
| Decimal precision | Compared as strings after normalization |
| BSON ObjectID | Compared as string representation |
| PostgreSQL `Numeric` | Parsed as `decimal.Decimal` for exact comparison |
| JSON encoding differences | Both parsed as `map[string]any` and compared structurally |

## VerificationReport Structure

```go
type VerificationReport struct {
    SourceProvider string
    DestProvider   string
    Timestamp      time.Time
    Duration       time.Duration
    Status         VerificationStatus  // PASS, WARN, FAIL, SKIPPED, ERROR

    Tables         []TableResult       // Per-table breakdown
    TotalTables    int
    PassCount      int
    FailCount      int
    WarnCount      int
    SkipCount      int

    TotalSrcRows   int64
    TotalDstRows   int64
    TotalSampled   int64
    TotalMismatches int64

    Mismatches     []MismatchDetail    // All mismatches (flat)
    Skipped        []string            // Check types not performed
    VerifiedAny    bool
}
```

### TableResult

| Field | Description |
|---|---|
| `Table` | Table/collection name |
| `SrcCount` | Source row count |
| `DstCount` | Destination row count |
| `CountDelta` | Difference in row counts |
| `CountMatch` | Whether counts are equal |
| `SampleTarget` | Number of records targeted for sampling |
| `SampleChecked` | Number of records actually compared |
| `SampleMismatch` | Number of records with mismatches |
| `Mismatches` | Per-table mismatch details |
| `Status` | PASS, FAIL, or SKIP |

## Interpreting Results

### Status Verdicts

| Status | Meaning | Action |
|---|---|---|
| **PASS** | All table counts match, no sampled mismatches | Migration is trustworthy |
| **WARN** | Minor issues detected (some tables skipped, non-critical mismatches) | Spot-check flagged tables manually |
| **FAIL** | Significant mismatches (count differences or data differences) | Manual inspection required — do not rely on the migration |
| **SKIPPED** | Verification could not run (neither provider supports it) | Manually verify destination data |
| **ERROR** | Verification encountered a runtime error | Check logs with `--log-level debug` |

### Common False Positives

Cross-engine verification may flag semantically equivalent data as mismatches:

- **`1` vs `1.0`**: Numeric serialization differs between databases. The field comparator handles this in most cases.
- **Date format differences**: `2024-01-15T10:30:00Z` vs `2024-01-15 10:30:00`. Timezone handling may differ.
- **Whitespace or encoding**: Trailing spaces, line endings, or character encoding differences.
- **NULL vs empty string**: Some databases distinguish NULL from empty string; others don't.
- **JSON key ordering**: `{"a":1,"b":2}` vs `{"b":2,"a":1}` — structurally identical but serialized differently.

If you see mismatches that appear to be false positives, increase the sample size with `--verify-sample-mode count --verify-sample-n 10000` and inspect the specific mismatched records.

## Cross-Engine Verification Differences

### SQL to SQL

- Full type-aware field comparison.
- Checksum comparison available for same-provider pairs.
- Table name normalization handles schema prefixes (`public.`).

### SQL to NoSQL

- Row counts are compared (SQL table rows vs MongoDB documents / Redis keys).
- Field comparison: SQL column values compared against MongoDB document fields or Redis hash fields.
- Type coercion handles numeric/string format differences.
- Nested JSON in SQL becomes embedded documents in MongoDB — comparison handles this.

### NoSQL to SQL

- Document/key counts compared.
- Field comparison: MongoDB document fields / Redis hash fields compared against SQL TEXT columns.
- All SQL columns are TEXT, so comparison is string-based.
- Nested documents in MongoDB are JSON-serialized in SQL — comparison handles this.

### NoSQL to NoSQL

- Key counts compared.
- Field comparison with type coercion for Redis↔MongoDB value differences.

## Standalone Verification

Run verification independently of a migration:

```sh
# CLI
bridge verify \
  --source-url "mysql://root@localhost/db1" \
  --dest-url "postgresql://localhost/db2"
```

This re-runs the verification step using the checkpoint's written keys to sample and compare records.

### MCP

```json
{
  "tool": "verify",
  "arguments": {
    "source_url": "mysql://root@localhost/db1",
    "dest_url": "postgresql://localhost/db2"
  }
}
```

## Example Output

```
[7/8] Verifying data
      done (2.5s)

Verification: PASSED (3 tables, 150 sampled, 2.5s)
```

Full report (via `--log-level debug` or MCP response):

```
Verification Report
  Status: PASS
  Duration: 2.5s
  Source: mysql | Destination: postgresql

  Tables:
  ┌────────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
  │ Table      │ Src Rows │ Dst Rows │ Δ Count  │ Sampled  │ Mismatch │
  ├────────────┼──────────┼──────────┼──────────┼──────────┼──────────┤
  │ users      │   100000 │   100000 │        0 │       50 │        0 │
  │ orders     │    30000 │    30000 │        0 │       75 │        0 │
  │ products   │     5000 │     5000 │        0 │       25 │        0 │
  └────────────┴──────────┴──────────┴──────────┴──────────┴──────────┘

  Summary: 3 tables, 150 sampled, 0 mismatches
  Verdict: migration is trustworthy
```

## Troubleshooting Verification Failures

1. **Count mismatch**: Some rows may have failed to write. Check the migration summary's `TotalFailed` count. Re-run the migration with `--resume` to retry failed rows.

2. **Field mismatch on specific records**: Check the `Mismatches` array in the report for specific field/value differences. Common causes: type coercion edge cases, character encoding, NULL handling.

3. **Verification skipped**: One or both providers do not implement the required verification interfaces. Run standalone `bridge verify` to check.

4. **SQLite cross-verification deadlock**: SQLite uses `MaxOpenConns(1)`. The cross-verifier may need a second connection. Use `--verify=false` or accept the hang.

5. **Increase sample coverage**: `--verify-sample-mode count --verify-sample-n 50000` for more thorough checking at the cost of longer verification time.

## Files Involved

| File | Role |
|---|---|
| `pkg/provider/provider.go` | `Verifier`, `VerifyReader`, `TableEnumerator`, `Checksummer` interfaces |
| `internal/verify/cross_verifier.go` | `CrossVerifier` — count, sample, compare phases |
| `internal/verify/comparator.go` | Field-by-field comparison with type coercion |
| `internal/verify/checksum.go` | Checksum comparison |
| `internal/verify/count.go` | Count comparison |
| `internal/verify/report.go` | Output formatters (Table, Compact, JSON) |
| `internal/verify/options.go` | Verification options and defaults |
| `internal/verify/types.go` | `VerificationReport`, `TableResult`, `MismatchDetail` |
| `internal/bridge/pipeline.go` | `stepVerify()` — pipeline integration |
