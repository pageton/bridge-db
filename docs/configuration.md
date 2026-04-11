# Configuration

Bridge-db accepts configuration from three sources: YAML config files, environment variables, and CLI flags. This document covers the complete config schema, all options, and how they interact.

## Quick Start

The simplest way to configure bridge-db is with connection URLs:

```sh
bridge migrate \
  --source-url "postgresql://user:pass@localhost:5432/myapp" \
  --dest-url "mongodb://admin:pass@localhost:27017/myapp"
```

For more complex setups, use a config file:

```sh
bridge migrate -c configs/my-migration.yaml
```

## Config Priority

Configuration sources are applied in order. Later sources override earlier ones:

| Priority | Source | Notes |
|----------|--------|-------|
| 1 (lowest) | **Config file** (`-c`) | Full YAML config |
| 2 | **Environment variables** | `BRIDGE_*` prefix |
| 3 | **URL flags** | `--source-url`, `--dest-url` |
| 4 | **Provider flags** | `--source-provider`, `--dest-provider` |
| 5 | **Individual field flags** | `--source-host`, `--source-port`, etc. |
| 6 | **SSH flags** | `--source-ssh-host`, etc. |
| 7 (highest) | **Pipeline/Checkpoint/Logging flags** | **Replaces** (not merges) the entire section |

**Important**: Pipeline, Checkpoint, and Logging CLI flags **replace** the entire config section rather than merging. If you set `--batch-size 5000` on the CLI, all other pipeline fields from the config file (like `verify`, `conflict_strategy`) are lost unless you also pass them as flags. Use either the config file **or** CLI flags for these sections, not both.

## YAML Config Schema

```yaml
source:            # ConnectionConfig (required)
destination:       # ConnectionConfig (required)
pipeline:          # PipelineConfig
transform:         # TransformConfig (optional)
checkpoint:        # CheckpointConfig
logging:           # LoggingConfig
```

### Source / Destination (`ConnectionConfig`)

Both `source` and `destination` use the same structure. At minimum, provide `provider` and either `url` or provider-specific fields.

```yaml
source:
  provider: postgres              # Required: postgres, mysql, mariadb, cockroachdb, mssql, sqlite, redis, mongodb
  url: "postgres://user:pass@localhost:5432/myapp"  # Connection URL (optional if using provider fields)

  # SSH tunnel (optional)
  ssh:
    enabled: true
    host: bastion.example.com
    port: 22
    username: deploy
    password: ""               # Use password or key_file
    key_file: "~/.ssh/id_rsa"
    known_hosts_file: ""       # Defaults to ~/.ssh/known_hosts
    insecure: false            # Requires BRIDGE_ALLOW_INSECURE_SSH=1

  # Provider-specific overrides (optional — override URL-parsed values)
  # Only the block matching `provider` is used. Non-zero fields win.
```

#### Provider-specific fields

Each provider has its own config block. Use these when you don't want to put credentials in a URL, or need provider-specific options:

**PostgreSQL:**

```yaml
  postgres:
    host: "127.0.0.1"
    port: 5432
    username: "postgres"
    password: "secret"
    database: "myapp"
    ssl_mode: "disable"       # disable, require, verify-ca, verify-full
```

**MySQL:**

```yaml
  mysql:
    host: "127.0.0.1"
    port: 3306
    username: "root"
    password: "secret"
    database: "myapp"
```

**MariaDB:**

```yaml
  mariadb:
    host: "127.0.0.1"
    port: 3306
    username: "root"
    password: "secret"
    database: "myapp"
```

**CockroachDB:**

```yaml
  cockroachdb:
    host: "127.0.0.1"
    port: 26257
    username: "root"
    password: ""
    database: "myapp"
    ssl_mode: "disable"
```

**MSSQL:**

```yaml
  mssql:
    host: "127.0.0.1"
    port: 1433
    username: "sa"
    password: "secret"
    database: "myapp"
    instance: ""              # Named instance (optional)
    encrypt: true             # TLS encryption
    trust_cert: false         # TrustServerCertificate
```

**SQLite:**

```yaml
  sqlite:
    path: "/path/to/myapp.db"
```

**Redis:**

```yaml
  redis:
    host: "127.0.0.1"
    port: 6379
    username: ""
    password: "secret"
    db: 0
    tls: false
```

**MongoDB:**

```yaml
  mongodb:
    host: "127.0.0.1"
    port: 27017
    username: "admin"
    password: "secret"
    database: "myapp"
    auth_source: "admin"
    tls: false
```

### Pipeline (`PipelineConfig`)

```yaml
pipeline:
  batch_size: 1000                 # Records per batch (default: 1000, must be > 0)
  dry_run: false                   # Simulate without writing (default: false)
  verify: true                     # Post-migration verification (default: true)
  conflict_strategy: overwrite     # On write conflict: overwrite, skip, error
  migrate_schema: true             # DDL migration for SQL destinations (default: true)
  fk_handling: defer_constraints   # FK strategy: defer_constraints, ordered, skip
  max_retries: 3                   # Write retries per batch (default: 3)
  retry_backoff: 500ms             # Initial retry delay (default: 500ms)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `batch_size` | int | `1000` | Records per batch. Larger = faster but more memory. |
| `dry_run` | bool | `false` | Preview migration without writing data. |
| `verify` | bool | `true` | Run post-migration verification. |
| `conflict_strategy` | string | `overwrite` | `overwrite`: upsert existing. `skip`: skip existing. `error`: fail on conflict. |
| `migrate_schema` | bool | `true` | Create DDL on destination (SQL→SQL only). |
| `fk_handling` | string | `defer_constraints` | `defer_constraints`: defer FK checks in transaction. `ordered`: write parent tables first. `skip`: ignore FKs. |
| `max_retries` | int | `3` | Exponential backoff retries per batch write. |
| `retry_backoff` | duration | `500ms` | Initial retry delay. Doubles each retry, max 30s. |

### Transform (`TransformConfig`)

```yaml
transform:
  null_policy: passthrough        # How to handle NULL values
  mappings:                       # Field-level transformations
    "users":
      - source: "created_at"
        destination: "created"
        action: rename
      - source: "internal_id"
        action: drop
    "orders":
      - source: "total"
        action: convert
        convert: "float"
      - source: "order_date"
        action: convert
        convert: "timestamp:mysql:postgres"
    "*":                           # Wildcard: applies to all tables
      - source: "legacy_field"
        action: drop
```

#### Null policy

| Policy | Behavior |
|---|---|
| `passthrough` | Keep NULL values as-is (default) |
| `drop` | Remove keys where value is NULL |
| `replace` | Replace NULL with empty string `""` |
| `error` | Fail if any NULL value is found |

#### Field mapping actions

| Action | Description |
|---|---|
| `rename` | Move value to a new key (set `destination`) |
| `drop` | Remove the field entirely |
| `convert` | Coerce type: `"string"`, `"int"`, `"float"`, `"bool"`, `"timestamp:src:dst"` |

### Checkpoint (`CheckpointConfig`)

```yaml
checkpoint:
  enabled: true                       # Enable checkpoint saving (default: true)
  path: ".bridge-db/checkpoint.json"  # Checkpoint file path (default: .bridge-db/checkpoint.json)
```

### Logging (`LoggingConfig`)

```yaml
logging:
  level: info       # Log level: debug, info, warn, error (default: info)
  json: false       # Structured JSON output (default: false)
```

## Connection URLs

Provider is auto-detected from the URL scheme:

| Scheme | Provider |
|---|---|
| `postgres://`, `postgresql://` | PostgreSQL |
| `mysql://` | MySQL |
| `mariadb://` | MariaDB |
| `cockroachdb://`, `crdb://` | CockroachDB |
| `mssql://`, `sqlserver://` | MSSQL |
| `sqlite://` | SQLite |
| `redis://`, `rediss://` | Redis (`rediss://` enables TLS) |
| `mongodb://`, `mongodb+srv://` | MongoDB |

Format: `provider://[username[:password]@][host][:port]/[database][?params]`

Examples:

```sh
# PostgreSQL
--source-url "postgresql://user:pass@localhost:5432/myapp?sslmode=disable"

# MySQL
--source-url "mysql://root:password@tcp(localhost:3306)/myapp"

# MongoDB
--dest-url "mongodb://admin:pass@localhost:27017/myapp?authSource=admin"

# SQLite (file path after ://)
--source-url "sqlite:///home/user/data.db"

# Redis with TLS
--source-url "rediss://:password@localhost:6379/0"

# MSSQL
--source-url "mssql://sa:password@localhost:1433/myapp?encrypt=true"
```

## Environment Variables

All config fields can be set via `BRIDGE_*` environment variables:

### Connection

| Variable | Maps To |
|---|---|
| `BRIDGE_SOURCE_URL` | `source.url` |
| `BRIDGE_SOURCE_PROVIDER` | `source.provider` |
| `BRIDGE_DEST_URL` | `destination.url` |
| `BRIDGE_DEST_PROVIDER` | `destination.provider` |

### Pipeline

| Variable | Maps To | Type |
|---|---|---|
| `BRIDGE_BATCH_SIZE` | `pipeline.batch_size` | int |
| `BRIDGE_DRY_RUN` | `pipeline.dry_run` | bool (`true`, `1`, `yes`) |
| `BRIDGE_VERIFY` | `pipeline.verify` | bool |
| `BRIDGE_CONFLICT_STRATEGY` | `pipeline.conflict_strategy` | `overwrite`, `skip`, `error` |
| `BRIDGE_MIGRATE_SCHEMA` | `pipeline.migrate_schema` | bool |
| `BRIDGE_FK_HANDLING` | `pipeline.fk_handling` | defer_constraints, ordered, skip |

### Logging

| Variable | Maps To |
|---|---|
| `BRIDGE_LOG_LEVEL` | `logging.level` |
| `BRIDGE_LOG_JSON` | `logging.json` |

### SSH Tunnel

| Variable | Maps To |
|---|---|
| `BRIDGE_SOURCE_SSH_HOST` | `source.ssh.host` |
| `BRIDGE_SOURCE_SSH_PORT` | `source.ssh.port` |
| `BRIDGE_SOURCE_SSH_USER` | `source.ssh.username` |
| `BRIDGE_SOURCE_SSH_PASSWORD` | `source.ssh.password` |
| `BRIDGE_SOURCE_SSH_KEY` | `source.ssh.key_file` |
| `BRIDGE_SOURCE_SSH_INSECURE` | `source.ssh.insecure` |
| `BRIDGE_DEST_SSH_HOST` | `destination.ssh.host` |
| `BRIDGE_DEST_SSH_PORT` | `destination.ssh.port` |
| `BRIDGE_DEST_SSH_USER` | `destination.ssh.username` |
| `BRIDGE_DEST_SSH_PASSWORD` | `destination.ssh.password` |
| `BRIDGE_DEST_SSH_KEY` | `destination.ssh.key_file` |
| `BRIDGE_DEST_SSH_INSECURE` | `destination.ssh.insecure` |

## Concurrency Flags (CLI only)

These are not in the YAML config — set them via CLI flags:

| Flag | Default | Description |
|---|---|---|
| `--parallel` | `4` | Scan/write buffer depth (backpressure control) |
| `--write-workers` | `1` | Concurrent writer goroutines |
| `--max-batch-bytes` | `32 MiB` | Soft byte budget per batch |
| `--max-written-keys` | `100000` | Max keys retained for checkpoint dedup |
| `--checkpoint-interval` | `0` | Save checkpoint every N batches (0 = every batch) |
| `--fail-fast` | `false` | Abort on first transform error |

## Verification Flags (CLI only)

| Flag | Default | Description |
|---|---|---|
| `--verify-sample-mode` | `pct` | `none`, `count`, `pct` |
| `--verify-sample-pct` | `5.0` | Percentage of rows to sample per table |
| `--verify-sample-n` | `100` | Fixed count per table (when mode is `count`) |
| `--verify-no-checksums` | `false` | Disable row-level hash comparison |
| `--verify-max-keys` | `10000` | Max total keys sampled across all tables |

## Example Configs

### PostgreSQL to MongoDB

```yaml
source:
  provider: postgres
  postgres:
    host: localhost
    port: 5432
    username: user
    password: secret
    database: myapp
    ssl_mode: disable

destination:
  provider: mongodb
  mongodb:
    host: localhost
    port: 27017
    username: admin
    password: secret
    database: myapp

pipeline:
  batch_size: 2000
  migrate_schema: false       # MongoDB is schemaless
  write_workers: 4             # CLI flag, not in YAML
  verify: true
```

```sh
bridge migrate -c configs/pg-to-mongo.yaml --write-workers 4
```

### MySQL to PostgreSQL with SSH tunnel

```yaml
source:
  provider: mysql
  url: "mysql://root:pass@10.0.1.50:3306/myapp"
  ssh:
    enabled: true
    host: bastion.example.com
    port: 22
    username: deploy
    key_file: ~/.ssh/id_rsa

destination:
  provider: postgres
  postgres:
    host: localhost
    port: 5432
    username: postgres
    password: secret
    database: myapp

pipeline:
  batch_size: 1000
  fk_handling: ordered
  verify: true

checkpoint:
  path: ".bridge-db/checkpoint-mysql-pg.json"
```

### SQLite to MySQL with field mappings

```yaml
source:
  provider: sqlite
  sqlite:
    path: "/home/user/data.db"

destination:
  provider: mysql
  mysql:
    host: localhost
    port: 3306
    username: root
    password: secret
    database: myapp

pipeline:
  batch_size: 500
  fk_handling: ordered

transform:
  null_policy: drop
  mappings:
    "users":
      - source: "created_at"
        action: convert
        convert: "timestamp:sqlite:mysql"
    "*":
      - source: "legacy_id"
        action: drop
```

### Redis to Redis with provider fields

```yaml
source:
  provider: redis
  redis:
    host: redis-prod.example.com
    port: 6379
    password: secret
    db: 0

destination:
  provider: redis
  redis:
    host: localhost
    port: 6379
    password: secret
    db: 0

pipeline:
  batch_size: 5000
```

### MSSQL to PostgreSQL

```yaml
source:
  provider: mssql
  mssql:
    host: mssql-prod.example.com
    port: 1433
    username: sa
    password: "P@ssw0rd!"
    database: production
    encrypt: true

destination:
  provider: postgres
  postgres:
    host: localhost
    port: 5432
    username: postgres
    password: secret
    database: production

pipeline:
  batch_size: 2000
  fk_handling: ordered

checkpoint:
  path: ".bridge-db/checkpoint-mssql-pg.json"
```

## Files Involved

| File | Role |
|---|---|
| `internal/config/config.go` | `MigrationConfig`, `PipelineConfig`, `TransformConfig` structs |
| `internal/config/loader.go` | YAML file loading, env variable loading |
| `internal/config/source.go` | Source-specific config structs |
| `internal/config/destination.go` | Destination-specific config structs |
| `internal/cli/migrate.go` | CLI flag definitions, config priority logic |
| `configs/example.yaml` | Annotated template config |
| `configs/mssql-to-postgres.yaml` | MSSQL → PostgreSQL example |
| `configs/cockroachdb-to-mariadb.yaml` | CockroachDB → MariaDB example |
| `configs/sqlite-to-mysql.yaml` | SQLite → MySQL example |
| `configs/multi-hop.yaml` | Multi-hop migration documentation |
