# Troubleshooting

This document covers common errors, recovery strategies, and cross-engine pitfalls when using bridge-db.

## Error Categories

Bridge-db categorizes errors into 8 types. Each category has its own retry policy and recovery approach:

| Category | Description | Retry Policy |
|---|---|---|
| `config` | Invalid configuration, missing required fields | No retry — fail immediately |
| `connection` | Database connection refused, timeout, auth failure | Retry with exponential backoff |
| `schema` | Schema inspection failure, DDL error | No retry — fail with actionable error |
| `scan` | Source read failure, cursor error | Retry with exponential backoff |
| `transform` | Field mapping failure, null policy violation | Retry (configurable, default: no retry) |
| `write` | Batch write failure, constraint violation | Retry with exponential backoff |
| `verify` | Count mismatch, field comparison error | No retry — report failure |
| `cancelled` | SIGINT/SIGTERM received | No retry — save checkpoint and exit |

## Common Issues

### "Unknown provider"

**Cause**: The provider requires a build tag that was not included during compilation.

**Solution**:
```sh
# Build with all providers
make build-all

# Or specify tags
go build -tags "mongodb,mssql,sqlite,redis" ./cmd/bridge

# Check which providers are compiled
bridge providers
```

### "Connection refused"

**Cause**: Database not reachable, wrong host/port, firewall, or SSH tunnel failure.

**Solution**:
1. Verify the database is running: `pg_isready`, `mysqladmin ping`, `redis-cli ping`
2. Check host and port in the connection URL
3. For SSH tunnels: verify SSH key, bastion host, and that `BRIDGE_ALLOW_INSECURE_SSH=1` is set if using password auth
4. Check logs with `--log-level debug` for the exact connection error

### "Type mapping not found"

**Cause**: A source column type has no mapping to the destination dialect.

**Solution**:
1. Check [docs/type-mapping.md](type-mapping.md) for available mappings
2. Pre-create the destination table manually with correct types, then run with `--migrate-schema=false`
3. Use field mappings to convert column values: `transform.mappings` in config

### "Config hash mismatch" (on resume)

**Cause**: A pipeline-affecting setting changed between the original migration and the resume attempt.

**Solution**: This is intentional — it prevents resuming with incompatible settings. Either:
1. Revert the changed setting and retry `--resume`
2. Delete `.bridge-db/checkpoint.json` and start fresh

Settings that invalidate the checkpoint: source/destination host, port, database, batch size, conflict strategy, FK handling, transform mappings. Password changes do **not** invalidate the checkpoint.

### "Verification count mismatch"

**Cause**: Some rows failed to write or were skipped during migration.

**Solution**:
1. Check the migration summary for `TotalFailed` count
2. Re-run with `--resume` to retry (dedup prevents duplicate writes)
3. If using `--on-conflict skip`, some rows may have been pre-existing — this is expected
4. Cross-engine verification may have false positives due to type coercion (see [docs/verification.md](verification.md))

### "Migration interrupted" (SIGINT/SIGTERM)

**Cause**: The migration was cancelled manually (Ctrl+C) or by the system.

**Solution**:
1. A checkpoint was saved automatically on cancellation
2. Resume with `--resume`
3. The in-progress table will be re-scanned from the beginning, but dedup prevents duplicate writes

### "Out of memory" on large tables

**Cause**: Default batch size is too large, or the row data is very wide (many columns, large blobs).

**Solution**:
1. Reduce batch size: `--batch-size 500`
2. Reduce parallel channel depth: `--parallel 2`
3. Reduce max batch bytes: `--max-batch-bytes 1048576` (1 MiB)
4. The scanner streams rows one at a time — OOM is typically in the batch buffer, not the scanner

### "Slow over SSH tunnel"

**Cause**: SSH tunnel adds latency and bandwidth constraints.

**Solution**:
1. This is expected — SSH tunnels add overhead
2. Reduce batch size to improve responsiveness
3. Increase write workers: `--write-workers 4`
4. Use a VPN or direct connection if possible

### "Foreign key constraint violation"

**Cause**: Parent table rows not yet written when child table rows arrive.

**Solution**:
1. Use `--fk-handling ordered` (default: `defer_constraints`)
2. Use `--fk-handling defer_constraints` to wrap writes in a transaction with deferred FK checks
3. Use `--fk-handling skip` to disable FK constraints entirely

### "write: UNSUPPORTED_TYPE" or DDL error

**Cause**: The destination database does not support a column type from the source.

**Solution**:
1. Check [docs/type-mapping.md](type-mapping.md) — some types are unmapped
2. Pre-create the destination table with compatible types
3. Run with `--migrate-schema=false` to skip automatic DDL

## Cross-Engine Pitfalls

### ENUM and SET types

MySQL/MariaDB `ENUM` and `SET` types convert to `VARCHAR(255)`. The value constraint is lost — any string value will be accepted on the destination. If you need the constraint, create the `ENUM` type manually on the destination after migration.

### JSONB to JSON

PostgreSQL `JSONB` converts to MySQL `JSON`. Binary JSON optimizations (GIN indexes, containment operators) are lost. The data itself is valid JSON.

### NoSQL to SQL: all TEXT columns

When migrating from MongoDB or Redis to SQL, all columns are typed as `TEXT`. Numeric, date, and boolean data loses its SQL type. If you need typed columns, pre-create the destination table with correct types and run with `--migrate-schema=false`.

### Foreign keys lost in NoSQL

SQL foreign key relationships are not carried over to MongoDB or Redis. FK column values are preserved as flat fields, but referential integrity and ON DELETE/UPDATE actions are lost. See [docs/sql-to-nosql.md](sql-to-nosql.md#foreign-key-handling) for workarounds.

### Redis TTL only preserved Redis-to-Redis

TTL metadata is only carried over in Redis-to-Redis migrations. SQL and MongoDB destinations do not have a TTL concept.

### SQLite cross-verification deadlock

SQLite uses `MaxOpenConns(1)` for safety. The cross-verifier may need a second connection to read source data while the first is in use, causing a deadlock. Use `--verify=false` for SQLite sources.

### MySQL 8 to MariaDB syntax incompatibility

MariaDB does not support MySQL 8's `INSERT ... AS new ON DUPLICATE KEY UPDATE` syntax. Use `--on-conflict skip` or pre-create tables to avoid upsert.

### Redis FK handling

Redis has no transaction support. The default `--fk-handling=defer_constraints` fails preflight for any migration involving Redis. Use `--fk-handling=skip`.

## Recovery Strategies

### For connection errors

1. Verify database is running and accessible
2. Check credentials in the connection URL
3. For SSH tunnels: verify SSH key and bastion connectivity
4. Retry — connection errors are retried automatically with exponential backoff (max 30s interval, multiplier 2.0)

### For schema errors

1. Run `--dry-run` to see what schema will be generated
2. Pre-create tables manually if the automatic DDL fails
3. Use `--migrate-schema=false` to skip DDL entirely
4. Check [docs/type-mapping.md](type-mapping.md) for unsupported types

### For type incompatibility

1. Use field mappings in config to convert values:
   ```yaml
   transform:
     mappings:
       - table: products
         source: price
         action: convert
         convert: "float"
   ```
2. Pre-cast values in SQL: `SELECT id, CAST(price AS FLOAT) AS price FROM products`
3. Implement a custom TypeMapper (see [docs/type-mapping.md](type-mapping.md#custom-type-mapper))

### For write failures

1. Reduce batch size: `--batch-size 500`
2. Check constraint violations — pre-create indexes or modify data
3. Increase retry count: `--max-retries 5`
4. Use `--on-conflict overwrite` (default) for idempotent writes
5. Individual failed units are retried automatically if `--max-per-unit-retry > 0`

### For verification failures

1. Re-run migration with `--resume` to retry any failed writes
2. Check mismatch details in the verification report
3. Increase sample size: `--verify-sample-mode count --verify-sample-n 50000`
4. Cross-engine mismatches may be false positives — see [docs/verification.md](verification.md)

## Structured Logging

Use `--log-level` and `--log-json` for debugging:

```sh
# Debug level shows batch-level detail
bridge migrate --log-level debug --source-url ... --dest-url ...

# Machine-readable JSON for log aggregation
bridge migrate --log-json --source-url ... --dest-url ...

# Both
bridge migrate --log-level debug --log-json --source-url ... --dest-url ...
```

Log levels: `debug` (most verbose), `info` (default), `warn`, `error`.

## Getting Help

- Run `bridge migrate --help` for all available flags
- Run `bridge version` to check your build version
- Check [docs/LIMITATIONS.md](LIMITATIONS.md) for known limitations
- Open a GitHub issue with `--log-level debug` output
