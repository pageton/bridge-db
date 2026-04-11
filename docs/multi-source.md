# Multi-Source Consolidation

Bridge-db migrates data from a single source to a single destination per run. For use cases that require consolidating data from multiple heterogeneous databases into one unified destination, you run multiple sequential migrations and verify after each one.

This document describes the strategy, consistency considerations, and an example workflow.

## Limitations

Bridge-db has **no built-in multi-source support**. The pipeline holds exactly one `src provider.Provider` and one `dst provider.Provider`. There is no configuration for multiple sources, no automatic orchestration, and no cross-source transactional guarantees.

Each migration run is an independent, point-in-time snapshot.

## Strategy: Sequential Migration

### Overview

1. Plan the migration order (independent tables first, dependent tables last)
2. Migrate each source to the unified destination
3. Verify after each migration
4. Handle table name conflicts

### Step-by-Step Workflow

#### Step 1: Plan the order

Identify dependencies between sources:

- If Source A has tables that Source B references, migrate Source A first.
- If no cross-source dependencies exist, order doesn't matter.
- For NoSQL destinations, foreign key ordering is irrelevant (no FK constraints).

#### Step 2: Dry-run each migration

```sh
# Source 1: PostgreSQL
bridge migrate --dry-run \
  --source-url "postgresql://user@localhost/app_users" \
  --dest-url "mongodb://localhost/unified_db"

# Source 2: MySQL
bridge migrate --dry-run \
  --source-url "mysql://root@localhost/app_orders" \
  --dest-url "mongodb://localhost/unified_db"

# Source 3: MongoDB
bridge migrate --dry-run \
  --source-url "mongodb://localhost/app_analytics" \
  --dest-url "mongodb://localhost/unified_db"
```

Review each plan for type mappings, unsupported fields, and warnings.

#### Step 3: Migrate each source

```sh
# Source 1: PostgreSQL → MongoDB
bridge migrate \
  --source-url "postgresql://user@localhost/app_users" \
  --dest-url "mongodb://localhost/unified_db" \
  --migrate-schema=false

# Verify
bridge verify \
  --source-url "postgresql://user@localhost/app_users" \
  --dest-url "mongodb://localhost/unified_db"

# Source 2: MySQL → MongoDB
bridge migrate \
  --source-url "mysql://root@localhost/app_orders" \
  --dest-url "mongodb://localhost/unified_db" \
  --migrate-schema=false

# Verify
bridge verify \
  --source-url "mysql://root@localhost/app_orders" \
  --dest-url "mongodb://localhost/unified_db"

# Source 3: MongoDB → MongoDB
bridge migrate \
  --source-url "mongodb://localhost/app_analytics" \
  --dest-url "mongodb://localhost/unified_db"

# Verify
bridge verify \
  --source-url "mongodb://localhost/app_analytics" \
  --dest-url "mongodb://localhost/unified_db"
```

Note: `--migrate-schema=false` is used for NoSQL destinations (MongoDB is schemaless). For SQL destinations, omit this flag to get automatic DDL.

#### Step 4: Handle table name conflicts

If two sources have tables with the same name, they will collide on the destination. Options:

**Option A: Pre-migration renaming via field mappings**

```sh
bridge migrate \
  --source-url "mysql://root@localhost/app_orders" \
  --dest-url "mongodb://localhost/unified_db" \
  --migrate-schema=false
```

MongoDB uses the source table name as the collection name. To avoid collisions, you can rename tables on the source before migration:

```sql
-- On MySQL source
RENAME TABLE orders TO mysql_orders;
```

**Option B: Use separate databases and merge later**

```sh
# Migrate to separate databases
bridge migrate --source-url "mysql://..." --dest-url "mongodb://localhost/mysql_data"
bridge migrate --source-url "postgresql://..." --dest-url "mongodb://localhost/pg_data"

# Merge using MongoDB's aggregation framework
# mongosh unified_db --eval '
#   db.pg_users.aggregate([{$merge: {into: "users", on: "_id"}}])
# '
```

## Consistency Considerations

### No cross-source transactional guarantees

Each migration run is independent. There is no way to atomically commit or roll back multiple migrations. If migration 2 fails after migration 1 succeeds, you have a partially consolidated destination.

### Point-in-time snapshots

Each migration captures a point-in-time snapshot of its source. If the sources are being actively written to during migration, the consolidated data will not be from the same moment in time.

### Strategies for consistency

1. **Quiesce sources**: Stop writes to all sources before starting the consolidation. This ensures all snapshots are from approximately the same time.

2. **Migrate in dependency order**: If Source B references data from Source A, migrate A first so that when B is migrated, A's data is already in the destination.

3. **Verify after each run**: Catch failures early before moving to the next source. Use `bridge verify` after each migration.

4. **Use checkpoints**: If a migration is interrupted, `--resume` picks up where it left off without re-scanning completed tables.

5. **Document the order**: Record which sources were migrated, in what order, and at what time. This is essential for debugging if data inconsistencies are found later.

## Minimizing Downtime

1. **Dry-run first**: Validate all migrations before committing to any of them.
2. **Use checkpoint/resume**: Large tables can be interrupted and resumed without losing progress.
3. **Increase parallelism**: Use `--write-workers 4 --parallel 8` for faster transfers.
4. **Batch size tuning**: Larger batches (`--batch-size 5000`) are faster but use more memory. Find the sweet spot for your data.
5. **Post-migration verification**: Instead of verifying during migration (which adds time), you can skip verification and run it separately afterward with `bridge verify`.

## Example: Consolidate PostgreSQL + MySQL + MongoDB into MongoDB

### Scenario

You have three databases:
- PostgreSQL: `users` table (100K rows)
- MySQL: `orders` table (300K rows), `products` table (5K rows)
- MongoDB: `events` collection (50K documents)

Target: single MongoDB instance, database `unified`.

### Workflow

```sh
# 1. PostgreSQL → MongoDB
bridge migrate \
  --source-url "postgresql://user:pass@pg-host/app" \
  --dest-url "mongodb://admin:pass@mongo-host/unified" \
  --migrate-schema=false \
  --write-workers 4 --batch-size 2000

bridge verify \
  --source-url "postgresql://user:pass@pg-host/app" \
  --dest-url "mongodb://admin:pass@mongo-host/unified"

# 2. MySQL → MongoDB
bridge migrate \
  --source-url "mysql://root:mysql-host/app" \
  --dest-url "mongodb://admin:pass@mongo-host/unified" \
  --migrate-schema=false \
  --write-workers 4 --batch-size 2000

bridge verify \
  --source-url "mysql://root:mysql-host/app" \
  --dest-url "mongodb://admin:pass@mongo-host/unified"

# 3. MongoDB → MongoDB (same provider, uses NoopTransformer)
bridge migrate \
  --source-url "mongodb://admin:pass@mongo-source/analytics" \
  --dest-url "mongodb://admin:pass@mongo-host/unified" \
  --write-workers 4 --batch-size 2000

bridge verify \
  --source-url "mongodb://admin:pass@mongo-source/analytics" \
  --dest-url "mongodb://admin:pass@mongo-host/unified"
```

### Result

The `unified` MongoDB database now contains:
- `users` collection (from PostgreSQL)
- `orders` collection (from MySQL)
- `products` collection (from MySQL)
- `events` collection (from MongoDB)

Each collection was individually verified.

## Config File for Sequential Migrations

Create separate config files for each migration:

```yaml
# configs/pg-to-mongo.yaml
source:
  url: "postgresql://user:pass@pg-host/app"
destination:
  url: "mongodb://admin:pass@mongo-host/unified"
pipeline:
  migrate_schema: false
  write_workers: 4
  batch_size: 2000
  verify: true
```

```yaml
# configs/mysql-to-mongo.yaml
source:
  url: "mysql://root@mysql-host/app"
destination:
  url: "mongodb://admin:pass@mongo-host/unified"
pipeline:
  migrate_schema: false
  write_workers: 4
  batch_size: 2000
  verify: true
```

Run sequentially:

```sh
bridge migrate -c configs/pg-to-mongo.yaml
bridge migrate -c configs/mysql-to-mongo.yaml
```

## Files Involved

| File | Role |
|---|---|
| `internal/bridge/pipeline.go` | Single source→destination pipeline |
| `internal/config/config.go` | `MigrationConfig` (Source + Destination, singular) |
| `internal/bridge/checkpoint.go` | Per-migration checkpoint (one at a time) |
| `internal/verify/cross_verifier.go` | Per-migration verification |
