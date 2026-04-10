#!/usr/bin/env bash
# Test migrations using the example/flake.nix environment.
# Run from within: cd example && nix develop --command bash ../example/test-migrations.sh
set -euo pipefail

BRIDGE="${BRIDGE:-/home/sadiq/Projects/bridge-db/build/bridge}"
PASS=0
FAIL=0
RESULTS=()

reset_postgres_db() {
  local port="$1"
  psql -h /tmp -p "$port" -d testdb -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;" >/dev/null
}

reset_mysql_db() {
  local socket="$1"
  "$MYSQL_BIN/mysql" -u root --socket="$socket" -e "DROP DATABASE IF EXISTS testdb; CREATE DATABASE testdb;" >/dev/null
}

reset_mariadb_db() {
  local socket="$1"
  "$MARIA_BIN/mariadb" -u root --socket="$socket" -e "DROP DATABASE IF EXISTS testdb; CREATE DATABASE testdb;" >/dev/null
}

reset_cockroach_db() {
  local port="$1"
  cockroach sql --host=localhost --port="$port" --insecure -e "DROP DATABASE IF EXISTS testdb CASCADE; CREATE DATABASE testdb;" >/dev/null
}

reset_redis_db() {
  local port="$1"
  redis-cli -p "$port" FLUSHDB >/dev/null
}

run_test() {
  local name="$1"
  shift
  echo ""
  echo "--------------------------------------------"
  echo "TEST: $name"
  echo "CMD: $*"
  echo "--------------------------------------------"
  if "$BRIDGE" migrate "$@" --log-level warn 2>&1; then
    echo "PASS: $name"
    RESULTS+=("PASS: $name")
    ((PASS++)) || true
  else
    echo "FAIL: $name"
    RESULTS+=("FAIL: $name")
    ((FAIL++)) || true
  fi
}

# ── Test 1: PostgreSQL → MySQL (cross-engine SQL) ──
reset_mysql_db "$MYSQL2_SOCK_DIR/$MYSQL2_SOCK"
run_test "PostgreSQL → MySQL" \
  --source-url "postgres://sadiq@127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-url "mysql://root@127.0.0.1:3307/testdb" \
  --verify --migrate-schema --batch-size 500

# ── Test 2: MySQL → MariaDB (same-engine clone) ──
reset_mariadb_db "$MARIA2_SOCK_DIR/$MARIA2_SOCK"
run_test "MySQL → MariaDB" \
  --source-url "mysql://root@127.0.0.1:3306/testdb" \
  --dest-url "mysql://root@127.0.0.1:3309/testdb" \
  --verify --migrate-schema --batch-size 500

# ── Test 3: MariaDB → CockroachDB (cross-engine SQL) ──
reset_cockroach_db 26258
run_test "MariaDB → CockroachDB" \
  --source-url "mysql://root@127.0.0.1:3308/testdb" \
  --dest-url "cockroachdb://root@localhost:26258/testdb?sslmode=disable" \
  --verify --migrate-schema --batch-size 500

# ── Test 4: CockroachDB → PostgreSQL (cross-engine SQL) ──
reset_postgres_db 5433
run_test "CockroachDB → PostgreSQL" \
  --source-url "cockroachdb://root@localhost:26257/testdb?sslmode=disable" \
  --dest-url "postgres://sadiq@127.0.0.1:5433/testdb?sslmode=disable" \
  --verify --migrate-schema --batch-size 500

# ── Test 5: SQLite → MySQL (file-based to network) ──
reset_mysql_db "$MYSQL2_SOCK_DIR/$MYSQL2_SOCK"
run_test "SQLite → MySQL" \
  --source-url "sqlite://$SQLITE_DB" \
  --dest-url "mysql://root@127.0.0.1:3307/testdb" \
  --verify --migrate-schema --batch-size 500

# ── Test 6: PostgreSQL → PostgreSQL (same-engine replication) ──
reset_postgres_db 5433
run_test "PostgreSQL → PostgreSQL (replication)" \
  --source-url "postgres://sadiq@127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-url "postgres://sadiq@127.0.0.1:5433/testdb?sslmode=disable" \
  --verify --migrate-schema --batch-size 500

# ── Test 7: Redis → Redis (same-engine clone) ──
reset_redis_db 6380
run_test "Redis → Redis" \
  --source-url "redis://localhost:6379/0" \
  --dest-url "redis://localhost:6380/0" \
  --verify --batch-size 500 --migrate-schema=false --fk-handling=skip

# ── Test 8: MongoDB → PostgreSQL (NoSQL → SQL cross-engine) ──
reset_postgres_db 5433
run_test "MongoDB → PostgreSQL" \
  --source-url "mongodb://localhost:27017/testdb" \
  --dest-url "postgres://sadiq@127.0.0.1:5433/testdb?sslmode=disable" \
  --verify --migrate-schema --batch-size 500

# ── Test 9: Dry-run mode (no data written) ──
run_test "PostgreSQL → MySQL (dry-run)" \
  --source-url "postgres://sadiq@127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-url "mysql://root@127.0.0.1:3307/testdb" \
  --verify --migrate-schema --dry-run --batch-size 500

# ── Summary ──
echo ""
echo "--------------------------------------------"
echo "TEST SUMMARY"
echo "--------------------------------------------"
for r in "${RESULTS[@]}"; do
  echo "  $r"
done
echo ""
echo "Total: $((PASS + FAIL))  Passed: $PASS  Failed: $FAIL"
echo "--------------------------------------------"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
