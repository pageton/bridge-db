#!/usr/bin/env bash
# Test migrations using the example/flake.nix environment.
# Run from within: cd example && nix develop --command bash ../example/test-migrations.sh
set -euo pipefail

BRIDGE="${BRIDGE:-/home/sadiq/Projects/bridge-db/build/bridge}"
PASS=0
FAIL=0
RESULTS=()

run_test() {
  local name="$1"
  shift
  echo ""
  echo "--------------------------------------------"
  echo "TEST: $name"
  echo "CMD: $*"
  echo "--------------------------------------------"
  if "$BRIDGE" migrate "$@" --log-level info 2>&1; then
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
run_test "PostgreSQL → MySQL" \
  --source-url "postgres://sadiq@127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-url "mysql://root@127.0.0.1:3307/testdb" \
  --verify --migrate-schema --batch-size 500

# ── Test 2: MySQL → MariaDB (same-engine clone) ──
run_test "MySQL → MariaDB" \
  --source-url "mysql://root@127.0.0.1:3306/testdb" \
  --dest-url "mysql://root@127.0.0.1:3309/testdb" \
  --verify --migrate-schema --batch-size 500

# ── Test 3: MariaDB → CockroachDB (cross-engine SQL) ──
run_test "MariaDB → CockroachDB" \
  --source-url "mysql://root@127.0.0.1:3308/testdb" \
  --dest-url "cockroachdb://root@localhost:26258/testdb?sslmode=disable" \
  --verify --migrate-schema --batch-size 500

# ── Test 4: CockroachDB → PostgreSQL (cross-engine SQL) ──
run_test "CockroachDB → PostgreSQL" \
  --source-url "cockroachdb://root@localhost:26257/testdb?sslmode=disable" \
  --dest-url "postgres://sadiq@127.0.0.1:5433/testdb?sslmode=disable" \
  --verify --migrate-schema --batch-size 500

# ── Test 5: SQLite → MySQL (file-based to network) ──
run_test "SQLite → MySQL" \
  --source-url "sqlite://$SQLITE_DB" \
  --dest-url "mysql://root@127.0.0.1:3307/testdb" \
  --verify --migrate-schema --batch-size 500

# ── Test 6: PostgreSQL → PostgreSQL (same-engine replication) ──
run_test "PostgreSQL → PostgreSQL (replication)" \
  --source-url "postgres://sadiq@127.0.0.1:5432/testdb?sslmode=disable" \
  --dest-url "postgres://sadiq@127.0.0.1:5433/testdb?sslmode=disable" \
  --verify --migrate-schema --batch-size 500

# ── Test 7: Redis → Redis (same-engine clone) ──
run_test "Redis → Redis" \
  --source-url "redis://localhost:6379/0" \
  --dest-url "redis://localhost:6380/0" \
  --verify --batch-size 500

# ── Test 8: MongoDB → PostgreSQL (NoSQL → SQL cross-engine) ──
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
