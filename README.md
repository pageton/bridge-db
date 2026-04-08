# bridge

Database migration CLI for Redis, MongoDB, PostgreSQL, MySQL, MariaDB, CockroachDB, MSSQL, and SQLite.

## Supported Databases

| Provider | Same-db | Cross-db | Schema Migration |
|---|---|---|---|
| Redis | Yes | Yes | N/A |
| MongoDB | Yes | Yes | N/A |
| PostgreSQL | Yes | Yes | Yes |
| MySQL | Yes | Yes | Yes |
| MariaDB | Yes | Yes | Yes |
| CockroachDB | Yes | Yes | Yes |
| MSSQL | Yes | Yes | Yes |
| SQLite | Yes | Yes | Yes |

All pairwise combinations between SQL databases are supported with automatic type mapping.

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

Or from the repo root:

```sh
go build -trimpath -ldflags="-s -w" -o bridge ./cmd/bridge
```

## Usage

```sh
bridge migrate \
  --source-provider <provider> \
  --source-url <url> \
  --dest-provider <provider> \
  --dest-url <url>
```

### Same-database migration

```sh
# PostgreSQL to PostgreSQL
bridge migrate \
  --source-provider postgres \
  --source-url "postgresql://user@localhost:5432/myapp?sslmode=disable" \
  --dest-provider postgres \
  --dest-url "postgresql://user@localhost:5433/myapp?sslmode=disable"

# MySQL to MySQL
bridge migrate \
  --source-provider mysql \
  --source-url "mysql://root@127.0.0.1:3306/myapp" \
  --dest-provider mysql \
  --dest-url "mysql://root@127.0.0.1:3307/myapp"

# MariaDB to MariaDB
bridge migrate \
  --source-provider mariadb \
  --source-url "mariadb://root@127.0.0.1:3306/myapp" \
  --dest-provider mariadb \
  --dest-url "mariadb://root@127.0.0.1:3307/myapp"

# CockroachDB to CockroachDB
bridge migrate \
  --source-provider cockroachdb \
  --source-url "cockroachdb://root@localhost:26257/myapp?sslmode=disable" \
  --dest-provider cockroachdb \
  --dest-url "cockroachdb://root@localhost:26258/myapp?sslmode=disable"

# MSSQL to MSSQL
bridge migrate \
  --source-provider mssql \
  --source-url "mssql://sa:Password123@localhost:1433/myapp" \
  --dest-provider mssql \
  --dest-url "mssql://sa:Password123@localhost:1434/myapp"

# SQLite to SQLite
bridge migrate \
  --source-provider sqlite \
  --source-url "sqlite:///path/to/source.db" \
  --dest-provider sqlite \
  --dest-url "sqlite:///path/to/dest.db"

# MongoDB to MongoDB
bridge migrate \
  --source-provider mongodb \
  --source-url "mongodb://admin:pass@source:27017/myapp?authSource=admin" \
  --dest-provider mongodb \
  --dest-url "mongodb://admin:pass@dest:27017/myapp?authSource=admin"

# Redis to Redis
bridge migrate \
  --source-provider redis \
  --source-url "redis://:password@source:6379" \
  --dest-provider redis \
  --dest-url "redis://:password@dest:6379"
```

### Cross-database migration

```sh
# PostgreSQL to MySQL
bridge migrate \
  --source-provider postgres \
  --source-url "postgresql://user@localhost:5432/myapp?sslmode=disable" \
  --dest-provider mysql \
  --dest-url "mysql://root@127.0.0.1:3306/myapp"

# MySQL to PostgreSQL
bridge migrate \
  --source-provider mysql \
  --source-url "mysql://root@127.0.0.1:3306/myapp" \
  --dest-provider postgres \
  --dest-url "postgresql://user@localhost:5432/myapp?sslmode=disable"

# MSSQL to PostgreSQL
bridge migrate \
  --source-provider mssql \
  --source-url "mssql://sa:Password123@localhost:1433/myapp" \
  --dest-provider postgres \
  --dest-url "postgresql://user@localhost:5432/myapp?sslmode=disable"

# CockroachDB to MariaDB
bridge migrate \
  --source-provider cockroachdb \
  --source-url "cockroachdb://root@localhost:26257/myapp?sslmode=disable" \
  --dest-provider mariadb \
  --dest-url "mariadb://root@127.0.0.1:3306/myapp"

# SQLite to MySQL
bridge migrate \
  --source-provider sqlite \
  --source-url "sqlite:///path/to/myapp.db" \
  --dest-provider mysql \
  --dest-url "mysql://root@127.0.0.1:3306/myapp"
```

### SSH tunneling

For databases behind firewalls, use SSH tunneling to reach them:

```sh
# Remote PostgreSQL behind firewall → local
bridge migrate \
  --source-provider postgres \
  --source-url "postgresql://user@10.0.1.50:5432/myapp" \
  --source-ssh-host bastion.example.com \
  --source-ssh-user deploy \
  --source-ssh-key ~/.ssh/id_rsa \
  --dest-provider postgres \
  --dest-url "postgresql://user@localhost:5432/myapp?sslmode=disable"

# Local → remote Redis via SSH (password auth)
bridge migrate \
  --source-provider redis \
  --source-url "redis://localhost:6379" \
  --dest-provider redis \
  --dest-host 10.0.1.50 \
  --dest-port 6379 \
  --dest-pass "secret" \
  --dest-ssh-host bastion.example.com \
  --dest-ssh-port 22 \
  --dest-ssh-user root \
  --dest-ssh-password "ssh-password"
```

### Verify-only

Verify data integrity after migration using the `--verify` flag with the `migrate` command:

```sh
bridge migrate \
  --source-provider redis \
  --source-url "redis://localhost:6379" \
  --dest-provider redis \
  --dest-url "redis://localhost:6380" \
  --verify
```

> **Note:** The standalone `bridge verify` command is a placeholder. Use `--verify` with `bridge migrate` for data verification.

## Configuration

### Config file

```sh
bridge migrate -c configs/example.yaml
```

### Example configs

Ready-made config files for common migration scenarios are in `configs/`:

| File | Scenario |
|------|----------|
| `configs/example.yaml` | General reference (all provider overrides documented) |
| `configs/mssql-to-postgres.yaml` | Microsoft SQL Server to PostgreSQL |
| `configs/cockroachdb-to-mariadb.yaml` | CockroachDB to MariaDB |
| `configs/sqlite-to-mysql.yaml` | SQLite to MySQL |
| `configs/multi-hop.yaml` | Multi-step pipeline guide |

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
  provider: postgres              # required: redis | mongodb | postgres | mysql | mariadb | cockroachdb | mssql | sqlite
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

  sqlite:
    database: "/path/to/myapp.db"

  # SSH tunnel (optional)
  ssh:
    enabled: true
    host: bastion.example.com      # SSH bastion host
    port: 22                       # SSH port
    username: deploy               # SSH username
    password: ""                   # SSH password (use password or key_file)
    key_file: "~/.ssh/id_rsa"      # SSH private key path
    remote_host: "10.0.1.50"       # database host behind bastion (inferred from URL)
    remote_port: 5432              # database port behind bastion (inferred from URL)

destination:
  provider: mysql
  url: "mysql://root:pass@tcp(localhost:3306)/myapp"
  # Same structure as source (provider-specific overrides, ssh)

pipeline:
  batch_size: 1000                 # records per batch
  dry_run: false                   # simulate without writing
  verify: true                     # verify counts after migration
  conflict_strategy: overwrite     # overwrite | skip | error
  migrate_schema: true             # migrate DDL schema (SQL only)
  fk_handling: defer_constraints  # defer_constraints | ordered | skip
  max_retries: 3                   # max retry attempts per failed batch
  retry_backoff: 500ms             # initial retry backoff (doubles each attempt)

checkpoint:
  enabled: true                    # enable checkpoint for resumability
  path: ".bridge-db/checkpoint.json"  # checkpoint file path

logging:
  level: info                      # debug | info | warn | error
  json: false                      # output logs in JSON format
```

## CLI Flags Reference

### Global flags

| Flag | Default | Description |
|------|---------|-------------|
| `-c, --config` | | config file path |
| `--log-level` | `info` | log level: `debug`, `info`, `warn`, `error` |
| `--log-json` | `false` | output logs in JSON format |

### `bridge migrate` flags

#### Source connection

| Flag | Default | Description |
|------|---------|-------------|
| `--source-provider` | | source provider: `redis`, `mongodb`, `postgres`, `mysql`, `mariadb`, `cockroachdb`, `mssql`, `sqlite` |
| `--source-url` | | source connection URL |
| `--source-host` | | source database host (overrides URL) |
| `--source-port` | | source database port (overrides URL) |
| `--source-user` | | source database username (overrides URL) |
| `--source-pass` | | source database password (overrides URL) |
| `--source-db` | | source database name (overrides URL) |

#### Destination connection

| Flag | Default | Description |
|------|---------|-------------|
| `--dest-provider` | | destination provider: `redis`, `mongodb`, `postgres`, `mysql`, `mariadb`, `cockroachdb`, `mssql`, `sqlite` |
| `--dest-url` | | destination connection URL |
| `--dest-host` | | destination database host (overrides URL) |
| `--dest-port` | | destination database port (overrides URL) |
| `--dest-user` | | destination database username (overrides URL) |
| `--dest-pass` | | destination database password (overrides URL) |
| `--dest-db` | | destination database name (overrides URL) |

#### Source SSH tunnel

| Flag | Default | Description |
|------|---------|-------------|
| `--source-ssh-host` | | SSH tunnel host for source |
| `--source-ssh-port` | `22` | SSH tunnel port for source |
| `--source-ssh-user` | | SSH tunnel username for source |
| `--source-ssh-password` | | SSH tunnel password for source |
| `--source-ssh-key` | | SSH private key file path for source |
| `--source-ssh-password-stdin` | `false` | read SSH password from stdin |

#### Destination SSH tunnel

| Flag | Default | Description |
|------|---------|-------------|
| `--dest-ssh-host` | | SSH tunnel host for destination |
| `--dest-ssh-port` | `22` | SSH tunnel port for destination |
| `--dest-ssh-user` | | SSH tunnel username for destination |
| `--dest-ssh-password` | | SSH tunnel password for destination |
| `--dest-ssh-key` | | SSH private key file path for destination |
| `--dest-ssh-password-stdin` | `false` | read SSH password from stdin |

#### Pipeline options

| Flag | Default | Description |
|------|---------|-------------|
| `-b, --batch-size` | `1000` | number of records per batch |
| `--max-retries` | `3` | max retry attempts per failed batch |
| `--on-conflict` | `overwrite` | conflict strategy: `overwrite`, `skip`, `error` |
| `--migrate-schema` | `true` | migrate DDL schema (SQL databases only) |
| `--fk-handling` | `defer_constraints` | foreign key handling: `defer_constraints`, `ordered`, `skip` |
| `--verify` | `true` | verify data after migration |
| `--dry-run` | `false` | simulate migration without writing |
| `--parallel` | `4` | number of parallel scan/write buffers |

#### Checkpoint options

| Flag | Default | Description |
|------|---------|-------------|
| `--checkpoint` | `true` | enable checkpoint for resumability |
| `--checkpoint-path` | `.bridge-db/checkpoint.json` | checkpoint file path |
| `--resume` | `false` | resume from last checkpoint |

### Connection URL formats

| Provider | URL format |
|----------|------------|
| Redis | `redis://[:password@]host:port/db` or `rediss://` for TLS |
| MongoDB | `mongodb://[user:pass@]host:port/database[?authSource=admin]` |
| PostgreSQL | `postgres://[user:pass@]host:port/database[?sslmode=disable]` |
| MySQL | `mysql://user:pass@tcp(host:port)/database` or `mysql://user:pass@unix(/path/to/socket)/database` |
| MariaDB | `mariadb://[user:pass@]host:port/database` |
| CockroachDB | `cockroachdb://[user:pass@]host:port/database[?sslmode=disable]` |
| MSSQL | `mssql://user:pass@host:port/database` or `sqlserver://user:pass@host:port?database=name` |
| SQLite | `sqlite:///path/to/database.db` |

## Nix

```sh
# Run directly (no install needed)
nix run github:pageton/bridge-db -- migrate \
  --source-provider postgres \
  --source-url "postgresql://user@localhost:5432/myapp?sslmode=disable" \
  --dest-provider mysql \
  --dest-url "mysql://root@127.0.0.1:3306/myapp"

# Cross-database migration with MSSQL source
nix run github:pageton/bridge-db -- migrate \
  --source-provider mssql \
  --source-url "mssql://sa:Password123@localhost:1433/myapp" \
  --dest-provider cockroachdb \
  --dest-url "cockroachdb://root@localhost:26257/myapp?sslmode=disable"

# Migrate SQLite to MariaDB
nix run github:pageton/bridge-db -- migrate \
  --source-provider sqlite \
  --source-url "sqlite:///data/myapp.db" \
  --dest-provider mariadb \
  --dest-url "mariadb://root@127.0.0.1:3306/myapp"

# Install into profile
nix profile install github:pageton/bridge-db

# Enter dev shell (go, golangci-lint, test databases)
nix develop
```

### Nix with config files

```sh
# Run with a config file
nix run github:pageton/bridge-db -- migrate -c configs/mssql-to-postgres.yaml

# Run multiple migrations sequentially
nix run github:pageton/bridge-db -- migrate -c configs/sqlite-to-mysql.yaml && \
nix run github:pageton/bridge-db -- migrate -c configs/cockroachdb-to-mariadb.yaml
```

## Architecture

```
cmd/bridge/main.go           Entry point
internal/
  cli/                       Cobra commands (migrate, verify)
  config/                    Config loading, validation, URL parsing
  bridge/                    Pipeline engine (scan → transform → write → verify)
  tunnel/                    SSH tunnel management
  transform/                 Data transformers and type mappers
  schema/                    Schema migration logic
  progress/                  Metrics collection and console reporting
  logger/                    Structured logging
  retry/                     Retry with exponential backoff
pkg/provider/                Core interfaces (Provider, Scanner, Writer, Verifier)
providers/
  redis/                     Redis implementation
  mongodb/                   MongoDB implementation
  postgres/                  PostgreSQL implementation
  mysql/                     MySQL implementation
  mariadb/                   MariaDB implementation
  cockroachdb/               CockroachDB implementation
  mssql/                     Microsoft SQL Server implementation
  sqlite/                    SQLite implementation
```

Pipeline phases:

`init` → `tunnel` → `connecting` → `schema_migration` → `scanning` → `transforming` → `writing` → `verifying` → `complete`

## Development

```sh
make dev        # go run without building
make build      # build to build/bridge
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

The `example/` directory contains a Nix flake that starts all 8 database providers with seed data:

```sh
cd example
nix develop

# Then from the repo root, run migrations:
bridge migrate --source-provider postgres \
  --source-url "postgresql://127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-provider cockroachdb \
  --dest-url "cockroachdb://root@localhost:26257/testdb?sslmode=disable"
```

MSSQL requires Docker (no native Nix package):

```sh
docker run -d --name mssql -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=BridgeDb123!' \
  -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest
```

## License

MIT
