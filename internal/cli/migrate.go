package cli

import (
	"github.com/spf13/cobra"
)

// migrateCmd is the migrate command.
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate data from source to destination database",
	Long: `Migrate data between database instances -- same engine or cross-engine.

Bridge-DB scans the source, transforms data if needed, writes to the
destination, and optionally verifies the result.  Checkpoints are saved
automatically so interrupted migrations can be resumed with --resume.

Providers:
  postgres  mysql  mariadb  cockroachdb  (always available)
  mongodb   mssql  sqlite   redis        (build tag required)

Configuration sources (later overrides earlier):
  1. Config file  (-c / --config)
  2. URL flags    (--source-url, --dest-url)
  3. Individual flags (--source-host, --source-port, ...)

Examples:
  # Postgres to MySQL (URL-based, simplest)
  bridge migrate \
    --source-url "postgres://user:pass@localhost:5432/myapp" \
    --dest-url "mysql://user:pass@tcp(localhost:3306)/myapp"

  # Redis to Redis (same engine clone)
  bridge migrate \
    --source-url "redis://:pass@src-host:6379/0" \
    --dest-url "redis://:pass@dst-host:6379/0"

  # MongoDB to PostgreSQL (cross-engine, NoSQL to SQL)
  bridge migrate \
    --source-url "mongodb://user:pass@localhost:27017/myapp" \
    --dest-url "postgres://user:pass@localhost:5432/myapp"

  # SSH tunnel to remote database behind firewall
  bridge migrate \
    --source-url "postgres://user:pass@10.0.1.50:5432/myapp" \
    --source-ssh-host bastion.example.com \
    --source-ssh-key ~/.ssh/id_rsa \
    --dest-url "mysql://root:pass@tcp(localhost:3306)/myapp"

  # Dry run (preview what would be migrated without writing)
  bridge migrate --dry-run \
    --source-url "postgres://localhost/myapp" \
    --dest-url "mysql://localhost/myapp"

  # Resume an interrupted migration
  bridge migrate --resume \
    --source-url "postgres://localhost/myapp" \
    --dest-url "mysql://localhost/myapp"

  # High-throughput with concurrent writers
  bridge migrate --write-workers 4 --parallel 8 --batch-size 5000 \
    --source-url "postgres://localhost/bigdb" \
    --dest-url "mysql://localhost/bigdb"

Performance tuning flags:
  --parallel N            scan/write buffer depth (default 4)
  --write-workers N       concurrent writer goroutines (default 1)
  --batch-size N          rows per batch (default 1000)
  --max-batch-bytes BYTES soft byte budget per batch (default 32 MiB)
  --checkpoint-interval N save checkpoint every N batches (default 1)`,
	RunE: runMigrate,
}
