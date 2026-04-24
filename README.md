<h1 align="center">bridge-db</h1>
<p align="center"><strong>Move data between SQL and NoSQL databases with a single CLI command.</strong></p>
<p align="center">
<a href="#demo">Demo</a> &middot;
<a href="#install">Install</a> &middot;
<a href="#usage">Usage</a> &middot;
<a href="#supported-databases">Databases</a> &middot;
<a href="#how-it-works">How it works</a>
</p>

---

## Quick Example

```sh
bridge migrate \
  --source-url "postgresql://user:pass@localhost:5432/myapp" \
  --dest-url   "mongodb://admin:pass@dest:27017/myapp"
```

One command. Postgres to MongoDB. Schema translation, type mapping, and verification included.

```sh
bridge migrate \
  --source-url "sqlite:///data/local.db" \
  --dest-url   "mysql://root@127.0.0.1:3306/myapp"
```

SQLite to MySQL. Just as easy.

## Demo

Migrate 150,000 rows from PostgreSQL to MongoDB — schema translation, data transfer, and verification in one command:

```sh
$ bridge migrate \
    --source-url "postgresql://user:pass@localhost:5432/myapp?sslmode=disable" \
    --dest-url   "mongodb://admin:pass@localhost:27017/myapp"
```

```
Migration: postgresql -> mongodb
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

✔ Migration completed successfully
  150000 records written in 22.4s

--- Summary ---
Source:      postgresql
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

150,000 records. Three tables. Postgres to MongoDB. 22 seconds. Verified.

### What just happened

Behind the scenes, bridge-db:

1. Connected to both databases and resolved capabilities
2. Flattened the relational schema into MongoDB documents
3. Streamed data through a concurrent pipeline (scan -> transform -> write)
4. Saved checkpoints after every batch so an interruption would be recoverable
5. Ran cross-database verification — sampled records and compared checksums

### Try it yourself

```sh
# 1. Install
curl -fsSL https://raw.githubusercontent.com/pageton/bridge-db/main/install.sh | sh

# 2. Run a dry run (no data written)
bridge migrate --dry-run \
  --source-url "postgresql://user:pass@localhost:5432/myapp" \
  --dest-url   "mysql://root@localhost:3306/myapp"

# 3. Migrate for real
bridge migrate \
  --source-url "postgresql://user:pass@localhost:5432/myapp" \
  --dest-url   "mysql://root@localhost:3306/myapp"
```

## Why bridge-db?

| Tool                   | What it does       | What it doesn't                                    |
| ---------------------- | ------------------ | -------------------------------------------------- |
| **Flyway / Liquibase** | Schema versioning  | No data migration, SQL only                        |
| **ETL platforms**      | Full pipelines     | Heavy setup, GUI-first, overkill for one-off moves |
| **Custom scripts**     | Whatever you write | Fragile, no resume, no verification                |

**bridge-db** is a CLI-first tool that moves data across database engines — not just schema, actual rows. It handles the type mapping, schema translation, checkpointing, and verification so you don't have to write throwaway migration scripts.

What you get:

- **Cross-database** — any of 8 databases to any other, 64 pairings
- **Resumable** — interrupted migrations pick up where they left off
- **Verified** — row counts and sampled checksums after every run
- **Zero config** — connection URLs are enough to start

## Features

- Cross-database migration (SQL <-> NoSQL, any-to-any)
- Automatic schema and type translation between engines
- 8 database providers (see below)
- Streaming pipeline with concurrent writers
- Checkpoint and resume — survive interruptions, config-guarded
- Post-migration verification (count + checksum sampling)
- SSH tunneling for databases behind firewalls
- Dry-run mode to preview before writing
- Field mapping and transformation layer
- Conflict strategies (overwrite / skip / error)
- Foreign key handling (defer constraints / ordered / skip)
- Exponential backoff retry with per-category policies

## Supported Databases

| Provider    | Schema Migration | Build Tag |
| ----------- | ---------------- | --------- |
| PostgreSQL  | Yes              | always    |
| MySQL       | Yes              | always    |
| MariaDB     | Yes              | always    |
| CockroachDB | Yes              | always    |
| MSSQL       | Yes              | `mssql`   |
| SQLite      | Yes              | `sqlite`  |
| Redis       | N/A              | `redis`   |
| MongoDB     | Indexes          | `mongodb` |

All 64 pairwise combinations are supported.

**Cross-engine behavior:**

- **SQL <-> SQL** — automatic type mapping (e.g. MySQL `TINYINT` to Postgres `SMALLINT`)
- **SQL -> NoSQL** — rows flattened into key-value pairs or documents
- **NoSQL -> SQL** — data normalized into row envelopes
- **NoSQL <-> NoSQL** — Redis hashes to MongoDB documents and vice versa

## Install

**Binary (recommended):**

```sh
curl -fsSL https://raw.githubusercontent.com/pageton/bridge-db/main/install.sh | sh
```

**Go install (base providers only):**

```sh
go install github.com/pageton/bridge-db/cmd/bridge@latest
```

**From source with all providers:**

```sh
git clone https://github.com/pageton/bridge-db.git
cd bridge-db
make build-all    # builds with mongodb,mssql,sqlite,redis
```

**Nix:**

```sh
nix run github:pageton/bridge-db -- migrate --source-url ... --dest-url ...
nix profile install github:pageton/bridge-db
```

**Verify installation:**

```sh
bridge version
bridge providers    # lists compiled-in providers
```

## Usage

### Basic migration

```sh
bridge migrate \
  --source-url "postgresql://user@localhost:5432/myapp?sslmode=disable" \
  --dest-url   "mysql://root@127.0.0.1:3306/myapp"
```

Provider is auto-detected from the URL scheme. Override with `--source-provider` / `--dest-provider` if needed.

### Dry run

```sh
bridge migrate --dry-run \
  --source-url "postgresql://localhost/myapp" \
  --dest-url   "mysql://localhost/myapp"
```

Previews the migration plan without connecting or writing anything.

### Resume an interrupted migration

```sh
bridge migrate --resume \
  --source-url "postgresql://localhost/myapp" \
  --dest-url   "mysql://localhost/myapp"
```

Resumes from the last checkpoint. Config is validated to prevent resuming against a different setup.

### High-throughput tuning

```sh
bridge migrate \
  --write-workers 4 --parallel 8 --batch-size 5000 \
  --source-url "postgresql://localhost/bigdb" \
  --dest-url   "mysql://localhost/bigdb"
```

### SSH tunneling

```sh
bridge migrate \
  --source-url "postgresql://user@10.0.1.50:5432/myapp" \
  --source-ssh-host bastion.example.com \
  --source-ssh-user deploy \
  --source-ssh-key ~/.ssh/id_rsa \
  --dest-url "postgresql://user@localhost:5432/myapp"
```

### Standalone verification

```sh
bridge verify \
  --source-url "postgresql://localhost/myapp" \
  --dest-url   "mysql://localhost/myapp"
```

Compares row counts and sampled checksums without running a migration.

### Self-update

```sh
bridge update              # update to the latest version
bridge update --check      # check only, don't install
bridge update --version v1.2.0  # update to a specific version
```

Downloads the latest release from GitHub, verifies the SHA-256 checksum, and replaces the running binary in-place. No new dependencies required.

### Config file

```sh
bridge migrate -c configs/mssql-to-postgres.yaml
```

Example configs are in `configs/` for common scenarios (MSSQL to Postgres, CockroachDB to MariaDB, SQLite to MySQL, multi-hop pipelines).

### Key flags

| Flag               | Default           | Description                            |
| ------------------ | ----------------- | -------------------------------------- |
| `--dry-run`        | false             | Preview without writing                |
| `--resume`         | false             | Resume from checkpoint                 |
| `--verify`         | true              | Verify after migration                 |
| `--migrate-schema` | true              | Migrate DDL schema (SQL)               |
| `--batch-size`     | 1000              | Records per batch                      |
| `--write-workers`  | 1                 | Concurrent writers                     |
| `--on-conflict`    | overwrite         | `overwrite`, `skip`, `error`           |
| `--fk-handling`    | defer_constraints | `defer_constraints`, `ordered`, `skip` |
| `-c, --config`     |                   | YAML config file path                  |

Run `bridge migrate --help` for the full list.

## How It Works

```
┌──────────┐    ┌───────────┐    ┌──────────────────┐    ┌──────────┐
│ Scanner  │───>│ Transform │───>│ buffered channel │───>│  Writer  │
│ (1 proc) │    │ (inline)  │    │  (backpressure)  │    │ (N proc) │
└──────────┘    └───────────┘    └──────────────────┘    └──────────┘
```

The pipeline runs in phases:

```
init -> tunnel -> connect -> schema -> scan+transform+write -> verify -> done
```

1. **Connect** — establish connections to both databases, resolve capabilities
2. **Schema** — translate and create DDL on the destination (SQL-to-SQL)
3. **Scan + Transform + Write** — streaming concurrent pipeline with backpressure
4. **Verify** — cross-database count and checksum comparison
5. **Checkpoint** — progress saved after each batch, guarded by config hash

Each database is backed by a **provider** that implements scan, write, schema, and verify operations. **Transformers** are selected automatically based on the source/destination pair and handle type mapping, timestamp conversion, null handling, and field mapping.

## Example Use Cases

**Migrate a production Postgres database to MongoDB:**

```sh
bridge migrate \
  --source-url "postgresql://app:pass@prod-db:5432/myapp" \
  --dest-url   "mongodb://admin:pass@mongo-cluster:27017/myapp"
```

**Migrate a MySQL database to MongoDB:**

```sh
bridge migrate \
  --source-url "mysql://root:password@127.0.0.1:3306/ecommerce" \
  --dest-url   "mongodb://admin:password@127.0.0.1:27017/ecommerce" \
  --migrate-schema=false \
  --write-workers 4 --batch-size 2000
```

**Copy a local SQLite database to Postgres:**

```sh
bridge migrate \
  --source-url "sqlite:///home/user/data.db" \
  --dest-url   "postgresql://user@localhost:5432/myapp"
```

**Replicate across environments with SSH tunneling:**

```sh
bridge migrate \
  --source-url "postgresql://reader@10.0.1.50:5432/analytics" \
  --source-ssh-host bastion.example.com \
  --source-ssh-user deploy \
  --source-ssh-key ~/.ssh/id_rsa \
  --dest-url "postgresql://user@localhost:5432/analytics_copy"
```

## Limitations

**SQL to NoSQL** — Relational structure is flattened. JOINs, foreign keys, and auto-increment identities are not preserved on the NoSQL side. Indexes are not recreated.

**NoSQL to SQL** — Nested documents and arrays are JSON-encoded into single columns rather than normalized. Redis keys without a common prefix produce a flat table.

**Cross-SQL** — Exotic or database-specific types may fall back to `TEXT`. Stored procedures, triggers, views, and functions are not migrated. `ENUM`/`SET` types convert to `TEXT`.

**General** — Large objects (BLOB, BYTEA) work but may be slow over SSH tunnels. Redis TTL is preserved only in Redis-to-Redis migrations.

See [docs/LIMITATIONS.md](docs/LIMITATIONS.md) for the full list.

## Documentation

| Document | Description |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Provider interfaces, registry, capabilities |
| [docs/configuration.md](docs/configuration.md) | Config file schema, env vars, CLI flags, examples |
| [docs/pipeline.md](docs/pipeline.md) | 10-step pipeline with code references |
| [docs/concurrency.md](docs/concurrency.md) | Goroutine layout, backpressure, tuning |
| [docs/data-model.md](docs/data-model.md) | MigrationUnit, DataType, envelopes |
| [docs/checkpoint-resume.md](docs/checkpoint-resume.md) | Checkpoint lifecycle, resume flow |
| [docs/transform.md](docs/transform.md) | Transformer registry, field mapping, null handling |
| [docs/type-mapping.md](docs/type-mapping.md) | All 19 type mapping tables, lossy conversions, custom mapper |
| [docs/sql-to-nosql.md](docs/sql-to-nosql.md) | SQL to NoSQL flow with code walkthrough |
| [docs/nosql-to-sql.md](docs/nosql-to-sql.md) | NoSQL to SQL flow with code walkthrough |
| [docs/dry-run.md](docs/dry-run.md) | Dry-run modes, output format, validation checklist |
| [docs/monitoring.md](docs/monitoring.md) | Real-time progress, throughput metrics |
| [docs/verification.md](docs/verification.md) | Verification levels, interpreting results |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Common errors, recovery strategies |
| [docs/multi-source.md](docs/multi-source.md) | Multi-source consolidation strategy |
| [docs/LIMITATIONS.md](docs/LIMITATIONS.md) | Comprehensive known limitations |
| [docs/mcp.md](docs/mcp.md) | MCP server setup and tools |

## FAQ

**Does it migrate schema?**
Yes, for SQL-to-SQL migrations. DDL is translated and applied on the destination automatically (`--migrate-schema`, on by default). For NoSQL destinations, schema migration doesn't apply.

**What happens if the migration is interrupted?**
A checkpoint is saved after each batch and on cancellation (SIGINT/SIGTERM). Re-run with `--resume` to pick up where you left off. The config is validated against the checkpoint to prevent accidental mismatches.

**Can I use it in production?**
The tool handles retries, backpressure, SSH tunneling, and verification — but it's young software (v0.x). Run `--dry-run` first, verify your data after migration, and test on a copy before migrating anything critical.

**How do I migrate to/from MSSQL, SQLite, Redis, or MongoDB?**
These providers require build tags. Build with `make build-all` or specify tags: `go build -tags "mongodb,mssql,sqlite,redis" ./cmd/bridge`.

## Contributing

Contributions are welcome. The rough process:

1. Fork the repo
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Make your changes, add tests
4. Run `make check` (fmt + vet + lint + test)
5. Open a pull request

### Development

```sh
make dev          # run without building
make build        # build to build/bridge
make build-all    # build with all providers
make test         # run tests
make test-race    # run tests with race detector
make check        # fmt-check + vet + lint + test
```

### Local test environment

The `example/` directory contains a Nix flake that starts all 8 databases with seed data:

```sh
cd example && nix develop
# then from repo root:
bridge migrate \
  --source-url "postgresql://sadiq@127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-url   "cockroachdb://root@localhost:26257/testdb?sslmode=disable"
```

### Adding a provider

1. Implement the `provider.Provider` interface from `pkg/provider/`
2. Add a transformer pair in `internal/transform/`
3. Register in `cmd/bridge/` with a build tag
4. Add tests

## About

bridge is a database migration tool that supports moving data between 8 different database systems: Redis, MongoDB, PostgreSQL, MySQL, MariaDB, CockroachDB, MSSQL, and SQLite.

### Key Features

- **Cross-database migration**: Migrate data between any pair of supported databases with automatic type mapping
- **Same-database migration**: Copy data between instances of the same database type
- **Schema migration**: Automatically migrate table schemas for SQL databases
- **SSH tunneling**: Connect to databases behind firewalls via SSH bastion hosts
- **Checkpoint & resume**: Pause and resume migrations with checkpoint support
- **Verification**: Verify data integrity after migration
- **Dry-run mode**: Test migrations without writing to destination

### Why bridge?

- Single unified tool for all database migration needs
- No intermediate export/import steps required
- Handles type conversion for cross-database migrations automatically
- Written in Go for fast performance and zero runtime dependencies

## License

[MIT](LICENSE)
