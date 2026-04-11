# Limitations

This document describes what bridge-db does **not** do, where conversions lose fidelity, and how edge cases behave. It is intended to help you decide whether this tool fits your migration and plan manual steps for the gaps.

---

## Schema Migration

Schema migration (`--migrate-schema`) handles tables, columns, and indexes. It does **not** cover everything.

### What is migrated

- Tables (`CREATE TABLE`)
- Columns with type, nullable, default, and auto-increment metadata
- Primary keys and secondary indexes (unique and non-unique)
- MongoDB indexes (on `--migrate-schema`, only indexes are created, not table schemas)

### What is not migrated

| Category                       | Examples                                      | Reason                                                                                     |
| ------------------------------ | --------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Foreign keys                   | `REFERENCES`, `ON DELETE CASCADE`             | FK constraints are relationship-level constructs that often don't translate across engines |
| CHECK constraints              | Column-level and table-level checks           | Syntax and semantics differ across databases                                               |
| Triggers                       | `CREATE TRIGGER`                              | Business logic is database-specific                                                        |
| Stored procedures & functions  | `CREATE FUNCTION`, `CREATE PROCEDURE`         | Language and runtime differ (PL/pgSQL vs T-SQL vs etc.)                                    |
| Views                          | `CREATE VIEW`                                 | Dependencies may reference objects that don't exist on the destination                     |
| Sequences                      | `CREATE SEQUENCE`                             | Identity columns are handled, but standalone sequences are not                             |
| Partitioning                   | Range/hash/list partitioning                  | DDL syntax varies and is often tied to specific database versions                          |
| Tablespaces                    | Storage allocation                            | Infrastructure-level concern                                                               |
| Computed/generated columns     | `GENERATED ALWAYS AS`                         | Expression syntax differs                                                                  |
| Partial and expression indexes | `WHERE` clause on indexes, functional indexes | Not supported by all engines                                                               |
| Collations                     | Column-level collation overrides              | Defaults are used on the destination                                                       |
| Comments                       | Table/column comments                         | Metadata, not structural                                                                   |
| MongoDB validators             | `collMod` with `validator` or `$jsonSchema`   | No SQL equivalent                                                                          |

### Default value handling

Default values are filtered for safety. Only well-known function calls are carried over:

- Allowed: `NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, `GEN_RANDOM_UUID()`, `UUID_GENERATE_V4()`, `NEXTVAL(...)`
- Dropped: anything with `()`, database-specific syntax (e.g. MySQL `ON UPDATE CURRENT_TIMESTAMP`), SQLite-specific functions

If your schema relies on database-specific default expressions, set them manually on the destination after migration.

---

## Type Mapping

Cross-SQL migrations automatically map column types. The mapping covers common types but is not exhaustive.

### Known downgrades

| Source type          | Destination type                             | When                       | Impact                                                         |
| -------------------- | -------------------------------------------- | -------------------------- | -------------------------------------------------------------- |
| `ENUM`               | `VARCHAR(255)`                               | MySQL/MariaDB to other SQL | Constraint is lost; any string value is accepted               |
| `SET`                | `VARCHAR(255)`                               | MySQL/MariaDB to other SQL | Multi-value constraint is lost                                 |
| `JSONB`              | `JSON`                                       | PostgreSQL to MySQL        | Binary JSON optimizations are lost; data is valid              |
| `UUID`               | `CHAR(36)`                                   | PostgreSQL to MySQL        | Stored as string; no native UUID operations on destination     |
| `BOOLEAN`            | `TINYINT(1)`                                 | PostgreSQL to MySQL        | Functional equivalent but different type family                |
| `SERIAL`/`BIGSERIAL` | `INT AUTO_INCREMENT`/`BIGINT AUTO_INCREMENT` | PostgreSQL to MySQL        | Identity behavior is similar; sequence objects are not created |
| `BYTEA`              | `BLOB`                                       | PostgreSQL to MySQL        | Binary data preserved; access patterns differ                  |

### Fallback behavior

Types that are not in the mapping table fall through **unmapped**. When this happens:

- In schema migration: the original type string is used verbatim, which may cause a DDL error if the destination does not understand it.
- In data transformation: unmapped types are passed through as-is. If the destination rejects the value, the write fails and the batch is retried (up to `--max-retries`).

If you hit unmapped types, check the error output and either:

1. Pre-create the destination table with the correct types, or
2. Use field mappings (`transform.mappings` in config) to convert the column values.

### Types not mapped at all

These types have no mapping and will likely cause DDL errors or require manual handling:

- PostgreSQL: `GEOMETRY`, `GEOGRAPHY`, `HSTORE`, `LTREE`, `CIDR`, `INET`, `MACADDR`, `TSVECTOR`, `TSQUERY`, `MONEY`, `INTERVAL`, custom composite types, domain types
- MySQL: `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `SPATIAL` indexes
- MSSQL: `XML`, `HIERARCHYID`, `SQL_VARIANT`, `GEOMETRY`, `GEOGRAPHY`, `UNIQUEIDENTIFIER` (mapped to `CHAR(36)`)
- SQLite: Type affinity system means most types are accepted, but precision metadata is lost

---

## SQL to NoSQL Conversion

When migrating from SQL databases to Redis or MongoDB, the relational structure is flattened. This is not a schema-preserving operation.

### SQL to Redis

- Each row becomes a Redis **hash** key. Key format: `table:primaryKeyValue`.
- Column values become hash fields. Nested values (arrays, objects) are JSON-encoded as strings.
- All column values are stored as-is. There is no type coercion beyond what the serializer does.
- Foreign key relationships are lost. Redis has no concept of relations.
- `AUTO_INCREMENT` identity values are carried as data but are not auto-generating on the Redis side.
- Indexes are not recreated (Redis has no secondary indexes).
- TTL is not set on destination keys (TTL is a Redis-to-Redis feature only).

### SQL to MongoDB

- Each row becomes a document in a collection named after the table.
- The primary key value is used as `_id`. Multi-column primary keys are concatenated.
- Column values are stored as document fields. Nested SQL structures (arrays, JSON columns) become embedded fields.
- Foreign key relationships are not represented. No `$lookup` or references are created.
- `AUTO_INCREMENT`/`SERIAL` values are carried as data; MongoDB's own `ObjectID` generation is not activated.

---

## NoSQL to SQL Conversion

### Redis to SQL

- Redis keys are grouped by type. Only **hash** keys produce structured data (multiple columns). String, list, set, and zset values are stored in a single `value` column.
- Keys without a common prefix produce a single flat table (default: `redis_data`) with columns `key` and `value`.
- All columns are typed as `TEXT`. No type inference is performed on Redis values.
- TTL metadata is not carried over to SQL.
- Redis **streams** are not migrated.

### MongoDB to SQL

- Each document becomes a row in a table named after the collection.
- Nested documents and arrays are JSON-encoded into a single `TEXT` column, not normalized into separate tables.
- All columns are typed as `TEXT` regardless of the actual BSON value types. Numeric, date, and boolean data loses its SQL type.
- `ObjectID` is stored as a string `_id` column.
- Documents with varying schemas across the same collection will produce rows with different column sets. Columns present in some documents but not others will have `NULL` values.

---

## Verification

Verification (`--verify`) checks data integrity after migration. It is not a full data comparison.

### How it works

1. **Count comparison**: Row/key counts are compared per table/collection.
2. **Sampling**: A configurable number of records are sampled from the source and compared against the destination.
3. **Checksum comparison**: If both providers support it, MD5 checksums are compared. Otherwise, raw record values are compared field by field.

### Limitations

- **Sampling is statistical, not exhaustive**. The default sample size may miss rare edge cases. Increase `--verify-sample-n` or use `--verify-sample-mode count` with a high count for more coverage.
- **Checksums use MD5** for speed. This is not a security concern (it is used for comparison, not authentication), but MD5 collisions are theoretically possible.
- **Cross-engine verification may have false positives**. Data that is semantically equivalent but serialized differently (e.g., `1` vs `1.0`, date format differences) may be flagged as a mismatch even though the data is correct.
- **Redis streams are not verified**. The verification engine skips stream keys.
- **Verification requires a live connection to both databases**. There is no offline verification mode.
- **The legacy `--verify-mode basic`** performs a destination-only count check using the `Verifier` interface. It does not compare against the source.

---

## General Limitations

### Multi-source consolidation

Bridge-db supports single source-to-destination per run. There is no built-in multi-source orchestration. For consolidating data from multiple databases into one destination, run sequential migrations and verify after each. See [docs/multi-source.md](multi-source.md) for the full strategy guide.

### Migration model

- **Point-in-time snapshot**, not live sync. Changes made to the source after the scan starts are not captured. There is no CDC (change data capture) support.
- **Single scanner goroutine**. Data is read from the source sequentially. Write concurrency is configurable (`--write-workers`), but scan parallelism is not.
- **No schema diffing**. Schema migration creates tables with `IF NOT EXISTS`. Running migration twice does not update the schema to match changes. Drop and re-migrate if the schema has changed.

### Resume and checkpointing

- **Fully-completed tables are skipped on resume.** Tables whose scanner cursor was fully exhausted are recorded by name in the checkpoint. On resume, the scanner filters these tables out and only scans remaining tables.
- **In-progress tables are re-scanned from the beginning.** When a migration is interrupted mid-table, the in-progress table is NOT marked as completed. On resume, the scanner re-reads all rows from that table. The dedup key set prevents duplicate writes for rows that were already persisted.
- **Dedup prevents silent duplication.** All keys written since the last fully-completed table are stored in the checkpoint. On resume, these keys are loaded into the dedup set and any re-scanned rows that match are skipped. This guarantees at-most-once write semantics for the default `--on-conflict overwrite` strategy.
- **Key tracking is memory-bounded** (`--max-written-keys`, default 100,000). The ring buffer evicts the oldest keys when full. For interrupted migrations where the in-progress table has more rows than `MaxWrittenKeys`, some already-written keys may be evicted from the dedup set. With `--on-conflict overwrite` (default), this causes harmless re-writes. With `--on-conflict error`, evicted keys may cause write failures.
- **Config changes invalidate checkpoints.** If source/destination host, port, database, batch size, conflict strategy, FK handling, or transform mappings change between runs, the checkpoint is rejected with an actionable error. Password changes do **not** invalidate the checkpoint.
- **Provider changes invalidate checkpoints.** The checkpoint records source and destination provider names. Resuming with different providers is rejected.
- **Old checkpoint versions are rejected.** Checkpoints written by older bridge-db versions (v1/v2 schema) are rejected to prevent data loss from incompatible semantics. The error message instructs the user to delete the checkpoint file and start fresh.
- **Checkpoints include a checksum.** A SHA-256 hash over all checkpoint fields is verified on load. Corrupted or partially-written checkpoint files are detected and rejected with an actionable error.
- **Checkpoint writes are atomic.** Checkpoint data is written to a temporary file and renamed. A crash during write preserves the previous checkpoint.

### Redis-specific

- **All 6 data types are migrated**: string, hash, list, set, zset, stream.
- **Streams** are migrated as data but are **not verified** by the verification engine.
- **TTL is preserved only in Redis-to-Redis migrations**. TTL is meaningless on SQL and MongoDB destinations.
- **Redis cluster is not supported**. Only standalone Redis instances are supported.
- **Large datasets** (millions of keys) may be slow to scan due to `SCAN` iteration. The `--batch-size` flag controls how many keys are scanned per iteration.

### MongoDB-specific

- **Schema migration creates indexes only**, not collection schemas or validation rules.
- **BSON-specific types** (`ObjectID`, `Date`, `Int32`, `Int64`, `Decimal128`, `Binary`, `Regex`) may not round-trip perfectly through SQL. After a MongoDB to SQL to MongoDB cycle, these types will be stored as strings or JSON-encoded values.
- **System collections** (names starting with `.`) are skipped during scanning and verification.
- **Large documents** (approaching the 16 MB BSON limit) are migrated as-is but may be slow over SSH tunnels.

### SQLite-specific

- **No concurrent write transactions**. SQLite's `Transactions` capability is `false`. The pipeline does not wrap writes in transactions for SQLite destinations.
- **Type affinity** means SQLite accepts most type names without error. This is convenient but means type mapping errors are silently tolerated rather than caught during schema creation.
- **In-memory databases** are not supported as source or destination.
- **Cross-verification deadlocks with SQLite source**. SQLite uses `MaxOpenConns(1)` for safety. When running `--verify` with a SQLite source, the cross-verifier may deadlock because it needs a second connection to read source data while the first is still in use. Use `--verify=false` for SQLite-to-SQL migrations, or accept the verification step may hang.

### Concurrency and ordering

- **FK handling with `ordered`** writes tables in dependency order (parent tables first). This is best-effort and depends on correct FK metadata from the source schema.
- **FK handling with `defer_constraints`** wraps writes in a transaction with deferred constraint checking. Not supported on SQLite or Redis.
- **With `--write-workers > 1`**, batches from the same table may be written out of order. This is fine for most data but matters if you rely on insert order for trigger-like behavior.

### Specific provider-pair issues

| Source  | Destination | Issue                                                                                        | Workaround                                                               |
| ------- | ----------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| MySQL 8 | MariaDB     | `INSERT ... AS new ON DUPLICATE KEY UPDATE` syntax not supported by MariaDB                  | Use `--on-conflict skip` or pre-create tables so upsert is not needed    |
| SQLite  | any         | Cross-verification may deadlock due to `MaxOpenConns(1)`                                     | Use `--verify=false` for SQLite sources                                  |
| Redis   | any         | Default `--fk-handling=defer_constraints` fails preflight (Redis has no transaction support) | Use `--fk-handling=skip` for any migration involving Redis               |
| MongoDB | any SQL     | All columns are typed as `TEXT`; numeric, date, boolean data loses SQL type information      | Pre-create destination tables with correct types, or accept TEXT columns |

### Binary data

- `BLOB`, `BYTEA`, and binary fields are migrated in full.
- Binary data is **not modified or re-encoded**. It passes through as raw bytes.
- Large binary fields may be slow over SSH tunnels due to bandwidth. Use `--max-batch-bytes` to split large batches.

---

## Quick Reference: What Requires Manual Steps

| Scenario                                | What to do manually                                                      |
| --------------------------------------- | ------------------------------------------------------------------------ |
| Foreign keys                            | Create FK constraints on destination after migration                     |
| Triggers, procedures, functions         | Rewrite in the destination's language                                    |
| Views                                   | Recreate on the destination                                              |
| `ENUM`/`SET` types                      | Create `ENUM` types on the destination if supported, or accept `VARCHAR` |
| Custom/default expressions              | Set defaults on the destination after schema migration                   |
| MongoDB document schema validation      | Configure `collMod` validators on the destination                        |
| Redis cluster topology                  | Use a standalone node as source/destination, or move keys manually       |
| Partial/expression indexes              | Create manually on the destination                                       |
| Database users and permissions          | Recreate on the destination                                              |
| Collation-specific sorting              | Set collation on the destination columns                                 |
| Spatial types (`GEOMETRY`, `GEOGRAPHY`) | Export/import separately or use a specialized tool                       |

---

## Further Reading

| Topic | Document |
|---|---|
| Complete type mapping tables | [docs/type-mapping.md](type-mapping.md) |
| Dry-run modes and validation | [docs/dry-run.md](dry-run.md) |
| Real-time monitoring and metrics | [docs/monitoring.md](monitoring.md) |
| Verification levels and interpretation | [docs/verification.md](verification.md) |
| Common errors and recovery | [docs/troubleshooting.md](troubleshooting.md) |
| Multi-source consolidation | [docs/multi-source.md](multi-source.md) |
