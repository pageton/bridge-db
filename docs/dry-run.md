# Dry Run

Dry-run mode lets you preview a migration before committing to it. Bridge-db provides three dry-run modes with increasing depth, from quick config validation to a full pipeline execution that scans and transforms real data without persisting it.

## Three Dry-Run Modes

| Mode | What it does | Connects to DB? | Scans data? | Use case |
|---|---|---|---|---|
| CLI `--dry-run` | Validates config, prints provider info and capabilities | No | No | Quick sanity check |
| Plan mode | Connects, inspects schema, builds MigrationPlan | Yes | No | Review type mappings and warnings |
| Full dry-run | Runs full pipeline with DryRunWriter | Yes | Yes | Validate with real data and metrics |

## Mode 1: CLI `--dry-run`

The fastest mode. Validates configuration and prints a summary without connecting to any database.

```sh
bridge migrate --dry-run \
  --source-url "mysql://root@localhost/db1" \
  --dest-url "mongodb://localhost/db2"
```

**What it outputs:**
- Source and destination provider names
- Whether the migration is cross-engine
- Pipeline settings (batch size, workers, conflict strategy, FK handling)
- Provider capabilities (schema migration support, verification support)

This exits immediately after config validation. No connections are opened and no data is read.

## Mode 2: Plan Mode

Connects to both databases, inspects the source schema, and builds a `MigrationPlan`. Does not transfer data.

This mode is used by the MCP `plan_migration` and `dry_run` tools. It is equivalent to running `Pipeline.Plan()` programmatically.

The `MigrationPlan` includes:

| Field | Description |
|---|---|
| `SourceProvider` | Source database provider name |
| `DestProvider` | Destination database provider name |
| `CrossDB` | Whether this is a cross-engine migration |
| `TransformerType` | Name of the resolved transformer |
| `Tables` | List of tables with `EstimatedRows` per table |
| `TypeMappings` | Column type conversions (source type → dest type, lossy flags) |
| `UnsupportedFields` | Warnings for unmapped types and lossy conversions |
| `EstimatedRows` | Total estimated row count across all tables |
| `EstimatedBatches` | Total estimated batches based on batch size |
| `SchemaMigration` | Whether schema migration will run |
| `Verification` | Verification level (cross, basic, none) |
| `FieldMappings` | User-configured field mapping rules |
| `Warnings` | General warnings from the planning phase |

## Mode 3: Full Dry-Run Pipeline

Wraps the destination provider in a `DryRunWriter` and runs the full pipeline. Data is scanned from the source and transformed, but the `DryRunWriter.Write()` method logs each batch at debug level and returns a fake `BatchResult` — nothing is persisted to the destination.

This mode produces real metrics (throughput, row counts, batch durations) because the scan and transform stages run against actual data.

```sh
bridge migrate \
  --source-url "mysql://root@localhost/db1" \
  --dest-url "mongodb://localhost/db2" \
  --log-level debug
```

At debug log level, each batch produces output like:

```
dry-run: would write batch 1 (500 units, 128.5 KiB) to collection "users"
dry-run: would write batch 2 (500 units, 127.2 KiB) to collection "orders"
```

## Validation Checklist

Before committing to a migration, use dry-run output to verify:

### 1. Correct tables selected

- The `Tables` list should include all tables you intend to migrate.
- Check `EstimatedRows` per table — do the counts match your expectations?
- If specific tables are missing, check your `--tables` filter or source database permissions.

### 2. Type mappings are acceptable

- Review `TypeMappings` for each table.
- Check for **lossy conversions** (marked with `lossy: true`):
  - `TIMESTAMPTZ → TIMESTAMP`: timezone info will be lost
  - `JSONB → JSON`: query optimizations will be lost
  - `UUID → CHAR(36)`: native UUID operations will be lost
  - `ENUM → VARCHAR(255)`: value constraints will be lost
  - `BOOLEAN → TINYINT(1)`: semantic boolean type will be lost
- Decide whether the information loss is acceptable for your use case.

### 3. No unexpected unsupported fields

- `UnsupportedFields` lists columns that could not be mapped.
- Each entry includes the table, column, source type, and reason.
- Common reasons: `"no type mapping for source type"` (exotic types), `"lossy conversion"` (acceptable but flagged).
- For unmapped types, either pre-create the destination table manually or use field mappings to convert the values.

### 4. Warnings reviewed

- Check `Warnings` for general concerns.
- Common warnings: missing capabilities, schema migration limitations, verification limitations.

### 5. Pipeline settings confirmed

- `BatchSize`: appropriate for your data size? Large batches are faster but use more memory.
- `WriteWorkers`: increase for write-heavy destinations.
- `OnConflict`: `overwrite` (default), `skip`, or `error` — choose based on your idempotency needs.
- `FKHandling`: `defer_constraints` (default), `ordered`, or `skip`.

## Example Workflow

### Step 1: Quick check

```sh
bridge migrate --dry-run \
  --source-url "postgresql://user@localhost/myapp" \
  --dest-url "mongodb://localhost/myapp"
```

Verify the providers are correct and the pipeline settings are what you expect.

### Step 2: Plan with schema inspection

Use the MCP `dry_run` tool or run programmatically to get the full `MigrationPlan`:

```json
{
  "source_provider": "postgresql",
  "dest_provider": "mongodb",
  "cross_db": true,
  "tables": [
    {"name": "users", "estimated_rows": 100000},
    {"name": "orders", "estimated_rows": 300000},
    {"name": "products", "estimated_rows": 5000}
  ],
  "type_mappings": {
    "users": [
      {"column": "id", "source": "integer", "dest": "integer", "needs_conversion": false, "lossy": false},
      {"column": "created_at", "source": "timestamptz", "dest": "timestamptz", "needs_conversion": false, "lossy": false}
    ],
    "orders": [
      {"column": "total", "source": "numeric(10,2)", "dest": "numeric(10,2)", "needs_conversion": false, "lossy": false}
    ]
  },
  "unsupported_fields": [],
  "estimated_rows": 405000,
  "estimated_batches": 405,
  "warnings": [],
  "verification": "none",
  "schema_migration": false
}
```

### Step 3: Address issues

If you see `schema_migration: false` for a NoSQL destination, that is expected — NoSQL databases are schemaless.

If you see lossy type mappings, decide whether to:
- Accept the loss and proceed
- Pre-create the destination schema manually
- Use field mappings to convert values

### Step 4: Run for real

```sh
bridge migrate \
  --source-url "postgresql://user@localhost/myapp" \
  --dest-url "mongodb://localhost/myapp"
```

## Interpreting Dry-Run Results

| Result | Meaning | Action |
|---|---|---|
| No unsupported fields, no lossy conversions | Clean migration | Proceed |
| Lossy conversions present | Data will lose some type fidelity | Review if acceptable |
| Unsupported fields with `"no type mapping"` | Some columns cannot be mapped | Pre-create tables or use field mappings |
| Verification level is `none` | Post-migration verification will be skipped | Run standalone `bridge verify` manually |
| Schema migration is `false` | No DDL will be created | Expected for NoSQL; unexpected for SQL destinations |
| Warnings present | Non-critical concerns | Review each warning |

## Files Involved

| File | Role |
|---|---|
| `internal/cli/migrate.go` | CLI `--dry-run` flag handling, `printDryRunPreview()` |
| `internal/bridge/pipeline.go` | `Pipeline.Plan()` (plan mode), `Pipeline.Run()` with `DryRun` option |
| `internal/bridge/plan.go` | `MigrationPlan` construction, `planTypeMappings()`, lossy detection |
| `providers/mongodb/dryrun.go` | `DryRunWriter` for MongoDB |
| `internal/transform/dryrun.go` | `dryRunProvider` wrapper |
