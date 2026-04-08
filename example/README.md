# Bridge-DB Test Environment

Nix flake that starts all 8 supported database providers with seed data for local testing and development.

## Quick start

```sh
cd example
nix develop
```

This auto-starts all databases, seeds them with test data, and prints connection URLs.

## Databases

| Database | Primary | Secondary |
|----------|---------|-----------|
| Redis | `redis://localhost:6379` | `redis://localhost:6380` |
| MongoDB | `mongodb://localhost:27017/testdb` | `mongodb://localhost:27018/testdb` |
| SQLite | `sqlite://$SEED_DIR/test.db` | — |
| PostgreSQL | `postgresql://127.0.0.1:5432/testdb?sslmode=disable` | `postgresql://127.0.0.1:5433/testdb?sslmode=disable` |
| MySQL | `mysql://root@127.0.0.1:3306/testdb` | `mysql://root@127.0.0.1:3307/testdb` |
| MariaDB | `mariadb://root@127.0.0.1:3308/testdb` | `mariadb://root@127.0.0.1:3309/testdb` |
| CockroachDB | `cockroachdb://root@localhost:26257/testdb?sslmode=disable` | `cockroachdb://root@localhost:26258/testdb?sslmode=disable` |
| MSSQL | `mssql://sa:BridgeDb123!@localhost:1433/testdb` | `mssql://sa:BridgeDb123!@localhost:1434/testdb` |

### MSSQL (Docker only)

MSSQL has no native Nix package. Start it separately:

```sh
docker run -d --name mssql \
  -e 'ACCEPT_EULA=Y' \
  -e 'MSSQL_SA_PASSWORD=BridgeDb123!' \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

docker run -d --name mssql2 \
  -e 'ACCEPT_EULA=Y' \
  -e 'MSSQL_SA_PASSWORD=BridgeDb123!' \
  -p 1434:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
```

Seed MSSQL manually after the container is up:

```sh
docker exec mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'BridgeDb123!' -C -Q "CREATE DATABASE testdb;"
docker exec mssql2 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'BridgeDb123!' -C -Q "CREATE DATABASE testdb;"
```

## Seed data

Each SQL database gets three tables with 25+ rows:

- **users** — 25 rows (name, email, age)
- **products** — 25 rows (name, category, price, stock)
- **orders** — 32 rows (user_id, product_id, quantity, total_price)

Redis gets matching hash keys, string keys, and list keys. MongoDB gets matching collections.

## Example migrations

From the repo root after starting the test environment:

```sh
go build -o bridge ./cmd/bridge
```

### Same-provider migration

```sh
# PostgreSQL primary → secondary
./bridge migrate \
  --source-provider postgres \
  --source-url "postgresql://127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-provider postgres \
  --dest-url "postgresql://127.0.0.1:5433/testdb?sslmode=disable"

# MySQL primary → secondary
./bridge migrate \
  --source-provider mysql \
  --source-url "mysql://root@127.0.0.1:3306/testdb" \
  --dest-provider mysql \
  --dest-url "mysql://root@127.0.0.1:3307/testdb"

# MariaDB primary → secondary
./bridge migrate \
  --source-provider mariadb \
  --source-url "mariadb://root@127.0.0.1:3308/testdb" \
  --dest-provider mariadb \
  --dest-url "mariadb://root@127.0.0.1:3309/testdb"

# CockroachDB primary → secondary
./bridge migrate \
  --source-provider cockroachdb \
  --source-url "cockroachdb://root@localhost:26257/testdb?sslmode=disable" \
  --dest-provider cockroachdb \
  --dest-url "cockroachdb://root@localhost:26258/testdb?sslmode=disable"

# MSSQL primary → secondary (requires Docker)
./bridge migrate \
  --source-provider mssql \
  --source-url "mssql://sa:BridgeDb123!@localhost:1433/testdb" \
  --dest-provider mssql \
  --dest-url "mssql://sa:BridgeDb123!@localhost:1434/testdb"

# Redis primary → secondary
./bridge migrate \
  --source-provider redis \
  --source-url "redis://localhost:6379" \
  --dest-provider redis \
  --dest-url "redis://localhost:6380"

# MongoDB primary → secondary
./bridge migrate \
  --source-provider mongodb \
  --source-url "mongodb://localhost:27017/testdb" \
  --dest-provider mongodb \
  --dest-url "mongodb://localhost:27018/testdb"
```

### Cross-provider migration

```sh
./bridge migrate \
  --source-provider postgres \
  --source-url "postgresql://127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-provider cockroachdb \
  --dest-url "cockroachdb://root@localhost:26257/testdb?sslmode=disable"

# MySQL → MariaDB
./bridge migrate \
  --source-provider mysql \
  --source-url "mysql://root@127.0.0.1:3306/testdb" \
  --dest-provider mariadb \
  --dest-url "mariadb://root@127.0.0.1:3308/testdb"

# SQLite → PostgreSQL
./bridge migrate \
  --source-provider sqlite \
  --source-url "sqlite:///$PWD/.seed-data/test.db" \
  --dest-provider postgres \
  --dest-url "postgresql://127.0.0.1:5433/testdb?sslmode=disable"

# MSSQL → MySQL (requires Docker MSSQL)
./bridge migrate \
  --source-provider mssql \
  --source-url "mssql://sa:BridgeDb123!@localhost:1433/testdb" \
  --dest-provider mysql \
  --dest-url "mysql://root@127.0.0.1:3307/testdb"
```

Or use config files:

```sh
./bridge migrate -c configs/mssql-to-postgres.yaml
./bridge migrate -c configs/cockroachdb-to-mariadb.yaml
./bridge migrate -c configs/sqlite-to-mysql.yaml
```

## Cleanup

```sh
# Stop Redis
redis-cli shutdown
redis-cli -p 6380 shutdown

# Stop MongoDB
mongo --quiet --eval "db.adminCommand({shutdown:1})"
mongo --quiet --port 27018 --eval "db.adminCommand({shutdown:1})"

# Stop PostgreSQL
pg_ctl -D "$SEED_DIR/postgres" stop
pg_ctl -D "$SEED_DIR/postgres2" stop

# Stop MySQL
mysqladmin -u root --socket=/tmp/bridge-test-mysql.sock shutdown
mysqladmin -u root --socket=/tmp/bridge-test-mysql2.sock shutdown

# Stop MariaDB
mysqladmin -u root --socket=/tmp/bridge-test-maria.sock shutdown
mysqladmin -u root --socket=/tmp/bridge-test-maria2.sock shutdown

# Stop CockroachDB
cockroach quit --host=localhost --port=26257 --insecure
cockroach quit --host=localhost --port=26258 --insecure

# Stop MSSQL (Docker)
docker stop mssql mssql2
docker rm mssql mssql2

# Remove all seed data
rm -rf .seed-data
```
