# Monitoring and Progress

Bridge-db provides real-time progress reporting during migrations and a detailed summary after completion. This document describes the console output format, available metrics, and how to access progress information programmatically.

## Console Output

The default `ConsoleReporter` prints numbered phases to stdout as the migration progresses:

```
Migration: mysql -> mongodb
  Workers: 1 | Batch: 1000 | Verify: on | Checkpoint: on
[1/8] Validating config
      done (<1µs)
[2/8] Validating connections
      done (5.0ms)
[3/8] Connecting to databases
      done (150ms)
[4/8] Inspecting schema
      skipped
[5/8] Building migration plan
      done (1.5s)
[6/8] Transferring data
      50000 written | 8200 records/s | 6s | table: users | 1/3 tables | ETA: 12s
      120000 written | 7600 records/s | 15s | table: orders | 2/3 tables | ETA: 4s
      150000 written | 7400 records/s | 20s | table: products | 3/3 tables | ETA: 0s
      done (20s)
[7/8] Verifying data
      done (2.5s)
[8/8] Cleaning up
      done (<1µs)
```

### Phase transitions

- `OnPhaseStart(desc)` prints `[N/M] Description`
- `OnPhaseDone(desc, duration, error)` prints `done (duration)`, `skipped`, or `FAILED: message`

### Live progress line

During the transfer phase (step 6), `OnProgress(stats)` overwrites the current line with:

```
      {TotalWritten} written | {Throughput} records/s | {Elapsed} | table: {CurrentTable} | {TablesCompleted}/{TablesTotal} tables | ETA: {EstimatedRemain}
```

This line updates after each batch write completes. It is thread-safe via a mutex.

## ProgressStats

The `ProgressStats` struct is available in real-time during a running migration:

```go
// pkg/provider/provider.go
type ProgressStats struct {
    Phase           MigrationPhase
    TotalScanned    int64
    TotalWritten    int64
    TotalFailed     int64
    TotalSkipped    int64
    Throughput      float64     // records per second
    Elapsed         time.Duration
    EstimatedRemain time.Duration
    BytesTransferred int64
    CurrentBatchID  int
    TablesCompleted int
    TablesTotal     int
    ErrorCount      int
    CurrentTable    string
}
```

### Field reference

| Field | Type | Description |
|---|---|---|
| `Phase` | `MigrationPhase` | Current pipeline phase |
| `TotalScanned` | `int64` | Rows read from source so far |
| `TotalWritten` | `int64` | Rows successfully written to destination |
| `TotalFailed` | `int64` | Rows that failed to write |
| `TotalSkipped` | `int64` | Rows skipped (dedup, conflict-skip) |
| `Throughput` | `float64` | Current write throughput in records/second |
| `Elapsed` | `time.Duration` | Time since migration started |
| `EstimatedRemain` | `time.Duration` | Estimated time remaining (based on current throughput) |
| `BytesTransferred` | `int64` | Total bytes written to destination |
| `CurrentBatchID` | `int` | ID of the most recent batch |
| `TablesCompleted` | `int` | Number of tables fully scanned |
| `TablesTotal` | `int` | Total number of tables to migrate |
| `ErrorCount` | `int` | Total errors encountered |
| `CurrentTable` | `string` | Name of the table currently being scanned |

## Throughput Calculation

Throughput is calculated by the `MetricsCollector` after each batch write:

```go
// Current throughput
throughput = totalWritten / elapsed.Seconds()

// Peak throughput (max of all observed values)
peakThroughput = max(peakThroughput, currentThroughput)

// ETA estimation
estimatedRemain = (totalScanned - totalWritten) / throughput
```

Batch durations are stored in a ring buffer of 1000 entries (bounded memory usage).

## ProgressReporter Interface

All progress events flow through the `ProgressReporter` interface:

```go
// pkg/provider/provider.go
type ProgressReporter interface {
    OnBatchStart(batchID int, unitCount int)
    OnBatchComplete(batchID int, result *BatchResult)
    OnMigrationComplete(summary *MigrationSummary)
    OnError(err error, unit *MigrationUnit)
    OnPhaseChange(phase MigrationPhase)
    OnPhaseStart(desc PhaseDesc)
    OnPhaseDone(desc PhaseDesc, dur time.Duration, err error)
    OnProgress(stats ProgressStats)
}
```

### Implementations

| Reporter | Description | Used by |
|---|---|---|
| `NopReporter` | Discards all events | Library-mode callers, MCP tools, tests |
| `ConsoleReporter` | Writes formatted output to stdout | CLI `bridge migrate` |
| `statusReporter` | Updates `MigrationRun` structs | App/MCP layer for status queries |

### `ConsoleReporter` behavior

- `OnPhaseStart` prints `[N/M] Description`
- `OnPhaseDone` prints indented `done (duration)`, `skipped`, or `FAILED: message`
- `OnProgress` overwrites the current line with the live progress line (carriage return)
- `OnBatchComplete` triggers `OnProgress` with updated stats
- Thread-safe via mutex

## Post-Migration Summary

After migration completes (or is interrupted), `OnMigrationComplete` receives a `MigrationSummary`:

### MigrationSummary fields

| Field | Type | Description |
|---|---|---|
| `SourceProvider` | `string` | Source database provider |
| `DestProvider` | `string` | Destination database provider |
| `Duration` | `time.Duration` | Total migration duration |
| `TotalScanned` | `int64` | Total rows read from source |
| `TotalWritten` | `int64` | Total rows written to destination |
| `TotalFailed` | `int64` | Total rows that failed |
| `TotalSkipped` | `int64` | Total rows skipped |
| `BytesTransferred` | `int64` | Total bytes transferred |
| `AvgThroughput` | `float64` | Average throughput (records/second) |
| `PeakThroughput` | `float64` | Peak throughput observed |
| `Tables` | `[]TableMetrics` | Per-table breakdown |
| `Verification` | `*VerificationReport` | Verification results (if enabled) |
| `Resumed` | `bool` | Whether this run was resumed from a checkpoint |

### TableMetrics

Each table gets a `TableMetrics` entry:

| Field | Type | Description |
|---|---|---|
| `Table` | `string` | Table name |
| `Scanned` | `int64` | Rows scanned |
| `Written` | `int64` | Rows written |
| `Failed` | `int64` | Rows failed |
| `Skipped` | `int64` | Rows skipped |
| `Bytes` | `int64` | Bytes transferred |
| `Duration` | `time.Duration` | Time spent on this table |
| `BatchCount` | `int` | Number of batches written |

### Example console summary

```
--- Summary ---
Source:      mysql
Destination: mongodb
Duration:    22.4s

Records:
  Written:     150000
  Transferred: 256.3 MiB
  Throughput:  6696 avg / 8200 peak records/s

Tables:
  Table                               Records   Failed       Size
  ------------------------------------------------------------
  users                               100000        0   128.5 MiB
  orders                               30000        0   102.4 MiB
  products                             20000        0    25.4 MiB

Verification: PASSED (3 tables, 150 sampled, 2.5s)
```

### FailureSummary

When errors occur, the summary includes categorized error counts with example messages:

```go
type FailureSummary struct {
    Categories map[string]ErrorCategory // category -> count + examples
}
```

Error categories: `config`, `connection`, `schema`, `scan`, `transform`, `write`, `verify`, `cancelled`, `internal`.

## Structured Logging

All pipeline steps use `slog` structured logging:

```sh
# Default human-readable output
bridge migrate --source-url ... --dest-url ...

# Machine-readable JSON
bridge migrate --source-url ... --dest-url --log-json

# Debug level (shows batch-level detail)
bridge migrate --source-url ... --dest-url --log-level debug
```

Structured log fields include `component`, `step`, `phase`, `table`, `batch`, `duration`, `error`.

## MCP/API Integration

The `statusReporter` bridges `ProgressStats` to `MigrationProgress` for the app/MCP layer:

```go
type MigrationProgress struct {
    Phase             string
    CurrentTable      string
    RecordsScanned    int64
    RecordsWritten    int64
    RecordsFailed     int64
    RecordsSkipped    int64
    BytesTransferred  int64
    Throughput        float64
    EstimatedRemaining time.Duration
    CurrentBatchID    int
    TablesCompleted   int
    TablesTotal       int
    UpdatedAt         time.Time
}
```

Query migration status via the MCP `get_migration_status` tool.

## Files Involved

| File | Role |
|---|---|
| `pkg/provider/provider.go` | `ProgressReporter`, `ProgressStats`, `MigrationSummary` interfaces |
| `internal/progress/reporter.go` | `ConsoleReporter`, `NopReporter` implementations |
| `internal/progress/metrics.go` | `MetricsCollector` — thread-safe accumulator, throughput calculation |
| `internal/bridge/pipeline.go` | Reporter integration, phase lifecycle |
| `internal/bridge/batch_writer.go` | `processWriteOutcome()` — triggers metrics and progress after each batch |
| `internal/app/runs.go` | `statusReporter` — bridges to MCP layer |
