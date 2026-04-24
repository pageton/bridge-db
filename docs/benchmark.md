# Benchmark Suite

The benchmark suite measures end-to-end migration performance using the real `bridge.Pipeline` engine. It generates realistic datasets, runs full migrations with configurable parameters, and produces structured markdown and JSON reports.

Source: `cmd/bench/`.

## Hardware Requirements

Hardware specs are auto-detected and printed in every report. The benchmark is self-contained and requires no external databases — it uses SQLite via `modernc.org/sqlite` (pure Go, no CGO).

Fill in your machine's specs when sharing results:

| Property | Your Machine |
|----------|-------------|
| CPU | _e.g. AMD Ryzen 9 7950X_ |
| Cores | _e.g. 16_ |
| RAM | _e.g. 32GB_ |
| Disk | _e.g. NVMe SSD_ |
| OS/Arch | _e.g. linux/amd64_ |
| Go | _e.g. go1.23.4_ |

## Running Benchmarks

All commands require the `sqlite` build tag. Use `make` or `just`:

```bash
# Quick smoke test (small dataset, defaults)
make benchmark

# Parameter sweep: small + medium, multiple batch sizes and worker counts
make benchmark-all

# Large dataset with full parameter sweep and JSON output
make benchmark-large

# Resume / interruption test
make benchmark-resume
```

Or run directly:

```bash
go run -tags sqlite ./cmd/bench -sizes small -output docs/benchmark-results.md
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-sizes` | `small` | Comma-separated dataset sizes: `small`, `medium`, `large` |
| `-batch-sizes` | `1000` | Comma-separated batch sizes |
| `-workers` | `auto` | Comma-separated worker counts (`auto` = NumCPU) |
| `-verify` | `true` | Run with verification enabled |
| `-resume` | `false` | Run resume/interruption benchmark |
| `-output` | _(none)_ | Write markdown report to file |
| `-json` | _(none)_ | Write JSON results to file |
| `-keep` | `false` | Keep source/destination databases after benchmarks |
| `-seed` | `42` | Random seed for deterministic data generation |
| `-warmup` | `0` | Warmup runs (excluded from results) |
| `-runs` | `1` | Runs per configuration (median reported) |

### Make / just targets

| Target | What it does |
|--------|-------------|
| `make benchmark` | Small dataset, default batch size, auto workers |
| `make benchmark-all` | Small + medium, batch sizes 500/1000/5000, workers 1/2/4, verify on |
| `make benchmark-large` | Large dataset, batch sizes 500/1000/5000, workers 1/2/4/8, verify on, JSON output |
| `make benchmark-resume` | Small + medium, resume/interruption test |

## Dataset Sizes

| Size | Records | Approx. Size | Tables | Columns | Wide Rows |
|------|--------:|-------------:|-------:|--------:|:---------:|
| small | 10,000 | ~2 MB | 3 | 10 | no |
| medium | 100,000 | ~20 MB | 5 | 10-12 | no |
| large | 1,000,000 | ~200-500 MB | 5 | 10-12 | yes |

### Table schemas

The generator creates realistic tables with mixed data types and skewed distributions:

**users** — `id`, `username`, `email`, `full_name`, `age`, `balance`, `is_active`, `created_at`, `bio` (nullable), `avatar_url` (nullable)

**orders** — `id`, `user_id`, `status` (enum), `total_amount`, `item_count`, `shipping_address`, `notes` (nullable), `ordered_at`, `shipped_at` (nullable), `metadata` (JSON)

**products** — `id`, `sku`, `name`, `description`, `price`, `stock`, `category` (enum), `weight_kg`, `is_available`, `created_at`

**audit_log** (medium/large) — `id`, `user_id`, `action` (enum), `resource_type` (enum), `resource_id`, `ip_address`, `user_agent`, `request_body` (JSON), `response_code`, `duration_ms`, `error_message` (nullable), `created_at`

**events** (medium/large) — `id`, `event_type` (enum), `source` (enum), `payload` (JSON), `correlation_id`, `session_id`, `user_id` (nullable), `timestamp`, `processed`, `retry_count`

Data distribution is skewed (not uniform): the first table receives more records, values use realistic distributions (e.g. 80/20 for booleans, 15% NULL rates, zipf-like integer ranges).

## Metrics Explained

| Metric | Description |
|--------|-------------|
| **Rows/s** | Total rows migrated per second (including all phases) |
| **MB/s** | Destination data written per second |
| **Peak RAM** | Peak Go runtime memory (`runtime.MemStats.Sys`) during migration |
| **Schema time** | Time spent in schema migration phase |
| **Transfer time** | Time spent scanning and writing data |
| **Verify time** | Time spent in the verification phase |
| **Errors** | Total failed rows/batches |

## Parameter Exploration

### Batch size

Larger batches reduce per-row overhead (fewer round-trips, better amortization of transaction costs) but increase memory usage. The optimal batch size depends on row width and available RAM. Use `benchmark-all` or `benchmark-large` to sweep across `500`, `1000`, and `5000`.

### Worker count

The `workers` flag controls `WriteWorkers` in the pipeline. Each worker processes a batch concurrently. Scaling beyond CPU cores yields diminishing returns due to scheduler contention. The parameter analysis section of the report shows speedup ratios relative to single-worker runs and flags when worker counts exceed CPU capacity.

### CPU relationship

The report auto-detects CPU cores and annotates whether tested worker counts fit within capacity. As a rule of thumb:
- `workers <= cores`: linear or near-linear scaling expected
- `workers > cores`: diminishing returns, potential contention

## Resume Benchmark

The resume test (`make benchmark-resume` or `-resume` flag) validates checkpoint/resume behavior:

1. A full dataset is generated
2. The pipeline runs with checkpointing enabled (every batch)
3. After 2 seconds, the context is cancelled — simulating an interruption
4. The checkpoint file is read to determine how many records were written
5. The pipeline is restarted with `--resume` and verification enabled
6. Results show records before/after interrupt, recovery time, and data correctness

This tests the full checkpoint → resume → verify path through the real pipeline engine.

## Output Formats

### Markdown

The markdown report (`-output`) includes:
- Environment table (CPU, RAM, disk, OS, Go version)
- Migration performance table (rows/s, MB/s, peak RAM by configuration)
- Resume performance table (if applicable)
- Parameter analysis (batch size comparison, worker scaling, verify overhead)
- Phase breakdown (schema / transfer / verify times)
- Interpretation guide tailored to the detected hardware

### JSON

The JSON output (`-json`) contains the full structured data:

```json
{
  "hardware": { "cpu_model": "...", "cpu_cores": 16, ... },
  "go_version": "go1.23.4",
  "results": [
    {
      "label": "sqlite-to-sqlite-small-b1000-w1-vtrue",
      "size_name": "small",
      "records": 10000,
      "total_duration": 1234567890,
      "rows_per_sec": 8092.3,
      "mb_per_sec": 1.6,
      "peak_ram_mb": 24.5,
      ...
    }
  ]
}
```

## Interpreting Results

- **CPU impact**: Workers beyond detected core count will compete for CPU time. Diminishing returns are expected when workers >> cores.
- **RAM impact**: Peak RAM should stay well below total system memory. If Peak RAM approaches system total, expect swapping and degraded performance.
- **Batch size**: Larger batches reduce per-row overhead but increase memory usage. The optimal size depends on row width and available memory.
- **Verification**: Adds a full pass over destination data; overhead is proportional to dataset size and verification depth.
- **Disk I/O**: SQLite uses file-based I/O. SSD results will differ significantly from HDD.

## Reproducibility

All data generation is seed-based and deterministic. The default seed is `42` — use `-seed` to change it. To reproduce a result exactly, run the same command with the same seed on the same hardware.

Document the full command when sharing results:

```bash
go run -tags sqlite ./cmd/bench \
  -sizes small,medium \
  -batch-sizes 500,1000,5000 \
  -workers 1,2,4 \
  -verify -runs 1 \
  -seed 42 \
  -output docs/benchmark-results.md \
  -json docs/benchmark-results.json
```
