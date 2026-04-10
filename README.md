# bridge-db

Move data between any two databases in a single command.

Bridge-db migrates data across SQL and NoSQL databases — Postgres to MongoDB, SQLite to MySQL, MSSQL to CockroachDB, and every other pairing. It handles schema translation, type mapping, SSH tunneling, and verification so you don't have to.

```sh
# Postgres -> MongoDB
bridge migrate \
  --source-provider postgres \
  --source-url "postgresql://user@localhost:5432/myapp" \
  --dest-provider mongodb \
  --dest-url "mongodb://admin:pass@dest:27017/myapp"

# SQLite -> Postgres
bridge migrate \
  --source-provider sqlite \
  --source-url "sqlite:///data/local.db" \
  --dest-provider postgres \
  --dest-url "postgresql://user@localhost:5432/myapp"

# MongoDB -> MySQL
bridge migrate \
  --source-provider mongodb \
  --source-url "mongodb://admin:pass@source:27017/myapp" \
  --dest-provider mysql \
  --dest-url "mysql://root@127.0.0.1:3306/myapp"
```

## Why bridge-db

- **One command** — no scripts, no ETL pipeline, no custom mapping code
- **Cross-database** — SQL to NoSQL and back, with automatic schema and type conversion
- **Resumable** — built-in checkpointing so interrupted migrations pick up where they left off
- **Verifiable** — `--verify` confirms row counts and sampled checksums after every migration
- **Production-aware** — SSH tunneling, concurrent writers, adaptive batching, conflict strategies, retry with backoff

## Supported Databases

| Provider    | Same-engine | Cross-engine | Schema Migration | Build Tag |
| ----------- | ----------- | ------------ | ---------------- | --------- |
| PostgreSQL  | Yes         | Yes          | Yes              | (always)  |
| MySQL       | Yes         | Yes          | Yes              | (always)  |
| MariaDB     | Yes         | Yes          | Yes              | (always)  |
| CockroachDB | Yes         | Yes          | Yes              | (always)  |
| MSSQL       | Yes         | Yes          | Yes              | `mssql`   |
| SQLite      | Yes         | Yes          | Yes              | `sqlite`  |
| Redis       | Yes         | Yes          | N/A              | `redis`   |
| MongoDB     | Yes         | Yes          | N/A              | `mongodb` |

All 64 pairwise combinations are supported. SQL-to-SQL migrations include automatic schema and type mapping.

### Cross-engine capabilities

| Source \ Dest   | PostgreSQL | MySQL     | MariaDB   | CockroachDB | MSSQL     | SQLite    | Redis   | MongoDB |
| --------------- | ---------- | --------- | --------- | ----------- | --------- | --------- | ------- | ------- |
| **PostgreSQL**  | clone      | types     | types     | types       | types     | types     | flatten | flatten |
| **MySQL**       | types      | clone     | types     | types       | types     | types     | flatten | flatten |
| **MariaDB**     | types      | types     | clone     | types       | types     | types     | flatten | flatten |
| **CockroachDB** | types      | types     | types     | clone       | types     | types     | flatten | flatten |
| **MSSQL**       | types      | types     | types     | types       | clone     | types     | flatten | flatten |
| **SQLite**      | types      | types     | types     | types       | types     | clone     | flatten | flatten |
| **Redis**       | normalize  | normalize | normalize | normalize   | normalize | normalize | clone   | convert |
| **MongoDB**     | normalize  | normalize | normalize | normalize   | normalize | normalize | convert | clone   |

- **types** = automatic type mapping (e.g. MySQL `TINYINT` to Postgres `SMALLINT`)
- **flatten** = SQL rows flattened into key-value pairs or documents
- **normalize** = NoSQL data normalized into SQL row envelopes
- **convert** = Redis hashes to MongoDB documents and vice versa
- **clone** = same-engine passthrough (no transformation)

### Limitations

**SQL to NoSQL (Redis, MongoDB)**

- Relational structure is flattened: rows become key-value pairs or documents. JOIN relationships are not preserved.
- Foreign key constraints are not meaningful on the NoSQL side and are skipped.
- `AUTO_INCREMENT` / `SERIAL` identity columns are not carried over as auto-generated IDs in the destination.
- Indexes are not recreated on the NoSQL destination (Redis has no secondary indexes; MongoDB indexes must be created manually).

**NoSQL to SQL (Redis, MongoDB to SQL)**

- Redis keys without a common prefix produce a single flat table with `key` and `value` columns.
- MongoDB nested documents and arrays are JSON-encoded into a single column rather than normalized into separate tables.
- No schema inference: the destination table must either be created by `--migrate-schema` (best-effort) or pre-created manually.

**Cross-SQL (e.g. MySQL to Postgres)**

- Type mappings cover common cases but exotic or database-specific types may fall back to `TEXT`. Review the generated schema before committing.
- Stored procedures, triggers, views, and functions are not migrated.
- `ENUM` and `SET` types are converted to `TEXT` across engines that don't support them natively.
- Default values with database-specific syntax (e.g. MySQL `CURRENT_TIMESTAMP ON UPDATE`) may need manual adjustment.

**General**

- Large objects (BLOB, BYTEA) are migrated but may be slow over SSH tunnels due to bandwidth.
- TTL on Redis keys is preserved in Redis-to-Redis migrations only; TTL is not applied to other destinations.

See [docs/LIMITATIONS.md](docs/LIMITATIONS.md) for the full list of limitations, edge cases, and manual steps needed for complex migrations.

## Install

```sh
go install github.com/pageton/bridge-db/cmd/bridge@latest
```

Or install the latest release binary to `~/.local/bin`:

```sh
curl -fsSL https://raw.githubusercontent.com/pageton/bridge-db/main/install.sh | sh
```

```sh
wget -qO- https://raw.githubusercontent.com/pageton/bridge-db/main/install.sh | sh
```

Optional overrides:

```sh
curl -fsSL https://raw.githubusercontent.com/pageton/bridge-db/main/install.sh | \
  BRIDGE_VERSION=v0.1.0 BRIDGE_INSTALL_DIR="$HOME/bin" sh
```

Tagged releases also publish prebuilt binaries in GitHub Releases.

### Building from source

Base providers (PostgreSQL, MySQL, MariaDB, CockroachDB) are always compiled:

```sh
go build -trimpath -ldflags="-s -w" -o bridge ./cmd/bridge
```

Include additional providers with build tags:

```sh
# All providers
go build -tags "mongodb,mssql,sqlite,redis" -o bridge ./cmd/bridge

# Specific providers only
go build -tags "redis" -o bridge ./cmd/bridge
```

Verify which providers are compiled in:

```sh
bridge providers
```

## Usage

```sh
bridge migrate \
  --source-url <url> \
  --dest-url <url>
```

Provider names are auto-detected from URL schemes when `--source-url`/`--dest-url` are used. Use `--source-provider`/`--dest-provider` to override.

### Same-engine migration

```sh
# PostgreSQL to PostgreSQL
bridge migrate \
  --source-url "postgresql://user@localhost:5432/myapp?sslmode=disable" \
  --dest-url "postgresql://user@localhost:5433/myapp?sslmode=disable"

# MySQL to MySQL
bridge migrate \
  --source-url "mysql://root@127.0.0.1:3306/myapp" \
  --dest-url "mysql://root@127.0.0.1:3307/myapp"

# Redis to Redis
bridge migrate \
  --source-url "redis://:password@source:6379" \
  --dest-url "redis://:password@dest:6379"
```

### Cross-engine migration

```sh
# PostgreSQL to MySQL
bridge migrate \
  --source-url "postgresql://user@localhost:5432/myapp?sslmode=disable" \
  --dest-url "mysql://root@127.0.0.1:3306/myapp"

# MSSQL to PostgreSQL
bridge migrate \
  --source-url "mssql://sa:Password123@localhost:1433/myapp" \
  --dest-url "postgresql://user@localhost:5432/myapp?sslmode=disable"

# SQLite to MySQL
bridge migrate \
  --source-url "sqlite:///path/to/myapp.db" \
  --dest-url "mysql://root@127.0.0.1:3306/myapp"
```

### SSH tunneling

For databases behind firewalls, use SSH tunneling to reach them:

```sh
# Remote PostgreSQL behind firewall -> local
bridge migrate \
  --source-url "postgresql://user@10.0.1.50:5432/myapp" \
  --source-ssh-host bastion.example.com \
  --source-ssh-user deploy \
  --source-ssh-key ~/.ssh/id_rsa \
  --dest-url "postgresql://user@localhost:5432/myapp?sslmode=disable"
```

### Dry run

Preview what would be migrated without writing anything:

```sh
bridge migrate --dry-run \
  --source-url "postgresql://localhost/myapp" \
  --dest-url "mysql://localhost/myapp"
```

### Resume

Interrupted migrations can be resumed from the last checkpoint:

```sh
bridge migrate --resume \
  --source-url "postgresql://localhost/myapp" \
  --dest-url "mysql://localhost/myapp"
```

### High-throughput tuning

For large datasets, increase concurrency:

```sh
bridge migrate --write-workers 4 --parallel 8 --batch-size 5000 \
  --source-url "postgresql://localhost/bigdb" \
  --dest-url "mysql://localhost/bigdb"
```

### Verify-only

Run standalone verification against an existing migration:

```sh
bridge verify \
  --source-url "postgresql://localhost/myapp?sslmode=disable" \
  --dest-url "mysql://localhost/myapp"
```

Or use `--verify` with `bridge migrate` to verify automatically after migration (default: on).

## Configuration

### Config file

```sh
bridge migrate -c configs/example.yaml
```

### Example configs

Ready-made config files for common migration scenarios are in `configs/`:

| File                                  | Scenario                                              |
| ------------------------------------- | ----------------------------------------------------- |
| `configs/example.yaml`                | General reference (all provider overrides documented) |
| `configs/mssql-to-postgres.yaml`      | Microsoft SQL Server to PostgreSQL                    |
| `configs/cockroachdb-to-mariadb.yaml` | CockroachDB to MariaDB                                |
| `configs/sqlite-to-mysql.yaml`        | SQLite to MySQL                                       |
| `configs/multi-hop.yaml`              | Multi-step pipeline guide                             |

```sh
# Run a specific migration
bridge migrate -c configs/mssql-to-postgres.yaml

# Chain multiple migrations
bridge migrate -c configs/sqlite-to-mysql.yaml && \
bridge migrate -c configs/cockroachdb-to-mariadb.yaml

# With nix
nix run github:pageton/bridge-db -- migrate -c configs/mssql-to-postgres.yaml
```

### Full YAML reference

```yaml
source:
  provider: postgres # required: redis | mongodb | postgres | mysql | mariadb | cockroachdb | mssql | sqlite
  url: "postgres://user:pass@host:5432/db?sslmode=disable"

  # Provider-specific overrides (override URL-parsed values)
  postgres:
    host: "localhost"
    port: 5432
    username: "postgres"
    password: "secret"
    database: "myapp"
    ssl_mode: "disable"

  redis:
    host: "localhost"
    port: 6379
    username: ""
    password: "secret"
    db: 0
    tls: false

  mongodb:
    host: "localhost"
    port: 27017
    username: "admin"
    password: "secret"
    database: "myapp"
    auth_source: "admin"

  mysql:
    host: "localhost"
    port: 3306
    username: "root"
    password: "secret"
    database: "myapp"

  mariadb:
    host: "localhost"
    port: 3306
    username: "root"
    password: "secret"
    database: "myapp"

  cockroachdb:
    host: "localhost"
    port: 26257
    username: "root"
    password: ""
    database: "myapp"
    ssl_mode: "disable"

  mssql:
    host: "localhost"
    port: 1433
    username: "sa"
    password: "secret"
    database: "myapp"
    instance: "" # named instance (optional)
    encrypt: false # encrypt connection
    trust_cert: false # trust server certificate without verification

  sqlite:
    path: "/path/to/myapp.db"

  # SSH tunnel (optional)
  ssh:
    enabled: true
    host: bastion.example.com
    port: 22
    username: deploy
    password: ""
    key_file: "~/.ssh/id_rsa"
    remote_host: "10.0.1.50"
    remote_port: 5432
    known_hosts_file: "" # path to known_hosts (default: ~/.ssh/known_hosts)
    insecure: false # skip host key verification (dev only)

destination:
  provider: mysql
  url: "mysql://root:pass@tcp(localhost:3306)/myapp"
  # Same structure as source (provider-specific overrides, ssh)

pipeline:
  batch_size: 1000 # records per batch
  dry_run: false # simulate without writing
  verify: true # verify counts after migration
  conflict_strategy: overwrite # overwrite | skip | error
  migrate_schema: true # migrate DDL schema (SQL only)
  fk_handling: defer_constraints # defer_constraints | ordered | skip
  max_retries: 3 # max retry attempts per failed batch
  retry_backoff: 500ms # initial retry backoff (doubles each attempt)

transform:
  null_policy: passthrough # passthrough | drop | replace | error
  mappings: # per-table field mapping rules ("*" for all tables)
    # users:
    #   - source: "user_name"
    #     destination: "username"
    #     action: "rename"
    #   - source: "legacy_field"
    #     action: "drop"
    #   - source: "price"
    #     action: "convert"
    #     convert: "float"

checkpoint:
  enabled: true
  path: ".bridge-db/checkpoint.json"

logging:
  level: info # debug | info | warn | error
  json: false
```

## CLI Flags Reference

### Global flags

| Flag           | Default | Description                                 |
| -------------- | ------- | ------------------------------------------- |
| `-c, --config` |         | config file path                            |
| `--log-level`  | `info`  | log level: `debug`, `info`, `warn`, `error` |
| `--log-json`   | `false` | output logs in JSON format                  |

### `bridge migrate` flags

#### Source connection

| Flag                | Default | Description                                   |
| ------------------- | ------- | --------------------------------------------- |
| `--source-url`      |         | source connection URL (auto-detects provider) |
| `--source-provider` |         | source provider (overrides URL detection)     |
| `--source-host`     |         | source database host (overrides URL)          |
| `--source-port`     |         | source database port (overrides URL)          |
| `--source-user`     |         | source database username (overrides URL)      |
| `--source-pass`     |         | source database password (overrides URL)      |
| `--source-db`       |         | source database name (overrides URL)          |

#### Destination connection

| Flag              | Default | Description                                        |
| ----------------- | ------- | -------------------------------------------------- |
| `--dest-url`      |         | destination connection URL (auto-detects provider) |
| `--dest-provider` |         | destination provider (overrides URL detection)     |
| `--dest-host`     |         | destination database host (overrides URL)          |
| `--dest-port`     |         | destination database port (overrides URL)          |
| `--dest-user`     |         | destination database username (overrides URL)      |
| `--dest-pass`     |         | destination database password (overrides URL)      |
| `--dest-db`       |         | destination database name (overrides URL)          |

#### SSH tunnel

| Flag                          | Default | Description                               |
| ----------------------------- | ------- | ----------------------------------------- |
| `--source-ssh-host`           |         | SSH tunnel host for source                |
| `--source-ssh-port`           | `22`    | SSH tunnel port for source                |
| `--source-ssh-user`           |         | SSH tunnel username for source            |
| `--source-ssh-password`       |         | SSH tunnel password for source            |
| `--source-ssh-key`            |         | SSH private key file path for source      |
| `--source-ssh-password-stdin` | `false` | read SSH password from stdin              |
| `--source-ssh-insecure`       | `false` | skip SSH host key verification (dev only) |
| `--source-ssh-known-hosts`    |         | path to known_hosts file for source       |
| `--dest-ssh-host`             |         | SSH tunnel host for destination           |
| `--dest-ssh-port`             | `22`    | SSH tunnel port for destination           |
| `--dest-ssh-user`             |         | SSH tunnel username for destination       |
| `--dest-ssh-password`         |         | SSH tunnel password for destination       |
| `--dest-ssh-key`              |         | SSH private key file path for destination |
| `--dest-ssh-password-stdin`   | `false` | read SSH password from stdin              |
| `--dest-ssh-insecure`         | `false` | skip SSH host key verification (dev only) |
| `--dest-ssh-known-hosts`      |         | path to known_hosts file for destination  |

#### Pipeline

| Flag                 | Default             | Description                                                          |
| -------------------- | ------------------- | -------------------------------------------------------------------- |
| `-b, --batch-size`   | `1000`              | number of records per batch                                          |
| `--parallel`         | `4`                 | scan/write buffer depth between scanner and writers                  |
| `--write-workers`    | `1`                 | concurrent writer goroutines (1 = sequential)                        |
| `--max-batch-bytes`  | `32 MiB`            | soft byte budget per batch; splits oversized batches (0 = unlimited) |
| `--max-retries`      | `3`                 | max retry attempts per failed batch                                  |
| `--on-conflict`      | `overwrite`         | conflict strategy: `overwrite`, `skip`, `error`                      |
| `--migrate-schema`   | `true`              | migrate DDL schema (SQL databases only)                              |
| `--fk-handling`      | `defer_constraints` | foreign key handling: `defer_constraints`, `ordered`, `skip`         |
| `--verify`           | `true`              | verify data after migration                                          |
| `--dry-run`          | `false`             | preview migration plan without connecting or writing                 |
| `--fail-fast`        | `false`             | abort on first transform error instead of skipping batch             |
| `--max-written-keys` | `100000`            | max written keys retained for checkpoint dedup                       |

#### Verification

| Flag                    | Default | Description                                                  |
| ----------------------- | ------- | ------------------------------------------------------------ |
| `--verify-sample-mode`  | `pct`   | sampling mode: `none`, `pct`, `count`                        |
| `--verify-sample-pct`   | `5.0`   | percentage of records to sample per table (when mode=pct)    |
| `--verify-sample-n`     | `100`   | number of records to sample per table (when mode=count)      |
| `--verify-no-checksums` | `false` | disable checksum comparison during verification              |
| `--verify-max-keys`     | `10000` | max total keys sampled across all tables during verification |

#### Checkpoint

| Flag                    | Default                      | Description                                       |
| ----------------------- | ---------------------------- | ------------------------------------------------- |
| `--checkpoint`          | `true`                       | enable checkpoint for resumability                |
| `--checkpoint-path`     | `.bridge-db/checkpoint.json` | checkpoint file path                              |
| `--checkpoint-interval` | `0`                          | save checkpoint every N batches (0 = every batch) |
| `--resume`              | `false`                      | resume from last checkpoint                       |

### Connection URL formats

| Provider    | URL format                                                                                        |
| ----------- | ------------------------------------------------------------------------------------------------- |
| Redis       | `redis://[:password@]host:port/db` or `rediss://` for TLS                                         |
| MongoDB     | `mongodb://[user:pass@]host:port/database[?authSource=admin]`                                     |
| PostgreSQL  | `postgres://[user:pass@]host:port/database[?sslmode=disable]`                                     |
| MySQL       | `mysql://user:pass@tcp(host:port)/database` or `mysql://user:pass@unix(/path/to/socket)/database` |
| MariaDB     | `mariadb://[user:pass@]host:port/database`                                                        |
| CockroachDB | `cockroachdb://[user:pass@]host:port/database[?sslmode=disable]`                                  |
| MSSQL       | `mssql://user:pass@host:port/database` or `sqlserver://user:pass@host:port?database=name`         |
| SQLite      | `sqlite:///path/to/database.db`                                                                   |

## Architecture

### Data flow

```
                           scanCh (buffered channel)
                          ┌──────────────────────────┐
                          │     capacity = parallel  │
                          └──────────────────────────┘
                                     │
┌──────────┐    ┌───────────┐   ┌────┴───────┐    ┌──────────┐
│ Scanner  │───>│ Transform │──>│   channel  │───>│  Writer  │
│(producer)│    │  (inline) │   │  (buffer)  │    │(workers) │
└──────────┘    └───────────┘   └────────────┘    └──────────┘
  1 goroutine                      │                 N goroutines
                                   │                 (write-workers)
                         backpressure: scanner
                         blocks when channel is full
```

- The **scanner** goroutine reads batches from the source, transforms inline, and produces into a buffered channel.
- **N writer goroutines** consume from the channel concurrently, each with its own Writer instance.
- **Backpressure** is implicit: when all writers are busy and the channel is full, the scanner blocks.
- Checkpoints are saved periodically (configurable interval) and on cancellation for resumability.

### Pipeline phases

```
init -> tunnel -> connect -> schema_migration -> scan+transform+write -> verify -> complete
```

| Phase                      | Description                                             |
| -------------------------- | ------------------------------------------------------- |
| `init`                     | Validate options, resolve config                        |
| `tunnel`                   | Open SSH tunnels (if configured)                        |
| `connect`                  | Create providers, establish connections, ping           |
| `schema_migration`         | Inspect source schema, create on destination (SQL only) |
| `scan + transform + write` | Concurrent producer-consumer pipeline                   |
| `verify`                   | Cross-database count and checksum comparison            |
| `complete`                 | Clear checkpoint, emit summary                          |

### Directory layout

```
cmd/bridge/main.go            Entry point
internal/
  cli/                        Cobra commands (migrate, verify)
  config/                     Config loading, validation, URL parsing
  bridge/                     Pipeline engine, checkpointing, error categorisation
  tunnel/                     SSH tunnel pool management
  transform/                  Data transformers and type mappers per provider pair
  progress/                   Metrics collection and console reporting
  logger/                     Structured logging
  retry/                      Retry with exponential backoff
  util/                       Shared formatting utilities (HumanBytes, Truncate)
  verify/                     Cross-database verification engine
pkg/provider/                 Core interfaces (Provider, Scanner, Writer, Verifier)
providers/
  redis/                      Redis adapter
  mongodb/                    MongoDB adapter
  postgres/                   PostgreSQL adapter
  mysql/                      MySQL adapter
  mariadb/                    MariaDB adapter
  cockroachdb/                CockroachDB adapter
  mssql/                      Microsoft SQL Server adapter
  sqlite/                     SQLite adapter
```

### Error handling

Errors are categorised into seven types, each with its own retry policy:

| Category   | Max attempts | Initial backoff | Max backoff | Retries?      |
| ---------- | ------------ | --------------- | ----------- | ------------- |
| Connection | 3            | 1s              | 10s         | Yes           |
| Schema     | 2            | 1s              | 5s          | Yes           |
| Scan       | 3            | 500ms           | 10s         | Yes           |
| Transform  | 1            | —               | —           | No (bad data) |
| Write      | 5            | 500ms           | 30s         | Yes           |
| Verify     | 1            | —               | —           | No            |
| Internal   | 1            | —               | —           | No (bug)      |

Each category includes a user-facing hint (e.g. "Check that the host, port, credentials, and network are correct").

### Checkpoint and resume

- Checkpoints are saved after every successful batch write (or at a configurable interval via `--checkpoint-interval`).
- On resume (`--resume`), the checkpoint is validated against the current config hash. If the config has changed (different providers, hosts, databases, or transform mappings), the checkpoint is rejected.
- Written keys are tracked in memory (capped at `--max-written-keys`, default 100k) for duplicate-write prevention on resume.
- On cancellation (SIGINT/SIGTERM), a final checkpoint is saved so the migration can be resumed.

## Nix

```sh
# Run directly (no install needed)
nix run github:pageton/bridge-db -- migrate \
  --source-url "postgresql://user@localhost:5432/myapp?sslmode=disable" \
  --dest-url "mysql://root@127.0.0.1:3306/myapp"

# Install into profile
nix profile install github:pageton/bridge-db

# Enter dev shell (go, gopls, gotools)
nix develop
```

## Development

```sh
make dev        # go run without building
make build      # build to build/bridge
make build-all  # build with all providers
make test       # run tests (-v -count=1)
make test-race  # run tests with race detection
make vet        # go vet
make lint       # golangci-lint
make fmt        # gofmt (write)
make fmt-check  # gofmt (check only)
make check      # fmt-check + vet + lint + test
make all        # clean + build + check
```

### Local test environment

The `example/` directory contains a Nix flake that starts all 8 database providers with seed data (25 users, 25 products, ~100 orders per source):

```sh
cd example
nix develop

# Then from the repo root, run migrations:
bridge migrate \
  --source-url "postgresql://sadiq@127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-url "cockroachdb://root@localhost:26257/testdb?sslmode=disable"
```

An automated test script runs 9 migration scenarios against the test environment:

```sh
cd example
nix develop --command bash test-migrations.sh
```

#### Tested migration pairs

| Source      | Destination     | Records | Status | Notes                                                           |
| ----------- | --------------- | ------- | ------ | --------------------------------------------------------------- |
| PostgreSQL  | MySQL           | 3737    | PASS   | Cross-engine with schema migration and verification             |
| MySQL       | MariaDB         | 3074    | PASS   | Use `--on-conflict skip` (overwrite uses MySQL 8 syntax)        |
| CockroachDB | PostgreSQL      | 1925    | PASS   | Schema migration correctly maps CRDB SERIAL to PG BIGSERIAL     |
| SQLite      | MySQL           | 4085    | PASS   | Disable `--verify` for SQLite source (single-conn limit)        |
| PostgreSQL  | PostgreSQL      | 3737    | PASS   | Same-engine replication with full verification                  |
| Redis       | Redis           | 75      | PASS   | All data types, use `--fk-handling=skip --migrate-schema=false` |
| MongoDB     | PostgreSQL      | 83      | PASS   | NoSQL to SQL with verification (3/3 tables passed)              |
| PostgreSQL  | MySQL (dry-run) | —       | PASS   | Preview only, no data written                                   |

MSSQL requires Docker (no native Nix package):

```sh
docker run -d --name mssql -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=BridgeDb123!' \
  -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest
```

## License

[MIT](LICENSE)
