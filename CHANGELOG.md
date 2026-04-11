# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0-rc.1] - 2026-04-11

### Added

- **MCP integration** (experimental) — exposes bridge-db as structured tools (`plan_migration`, `run_migration`, `verify_migration`, `get_migration_status`) for AI agent workflows via stdio and HTTP transports
- **App services layer** (`internal/app/`) — `PlanningService`, `MigrationService`, `VerificationService`, `RunStatusService` for programmatic and MCP-based migration control
- **Provider unit tests** — 200+ tests across all 8 providers covering scanner, writer, verifier, schema migrator, and capabilities
- **`ResolvedConfig` interface** on all 8 config types with `GetHost()`, `GetPort()`, `GetDatabase()` methods
- **`docs/configuration.md`** — complete config reference with YAML schema, all 8 provider blocks, environment variables, URL schemes, and example configs
- **`docs/type-mapping.md`** — consolidated reference with all 19 SQL-to-SQL type mapping tables, lossy conversions, fallback behavior, and custom TypeMapper example
- **`docs/dry-run.md`** — three dry-run modes (CLI, plan, full pipeline), output format, and validation checklist
- **`docs/monitoring.md`** — real-time progress reporting, `ProgressStats` fields, `MigrationSummary` structure, console output format
- **`docs/verification.md`** — verification levels, CrossVerifier flow, type coercion rules, result interpretation, troubleshooting
- **`docs/troubleshooting.md`** — 8 error categories, common issues with solutions, cross-engine pitfalls, recovery strategies
- **`docs/multi-source.md`** — sequential migration strategy for multi-source consolidation with consistency considerations
- **`llms.txt`** — structured LLM-friendly project index for AI tool indexing (Context7, Claude, etc.)
- **Connection pool configuration** for MySQL, MariaDB, and MSSQL providers
- **`estimateChunkSize`** uses max row size instead of average for safer memory budgeting

### Changed

- **Concurrency model** — replaced mutex-protected `map` with `sync.Map` for lock-free key deduplication on the hot path; replaced `int64` counters with `atomic.Int64` for lock-free metrics
- **Config internals** — replaced 8 `mergeX` functions (~160 lines) with single `mergeStruct[T any]` generic (~12 lines); replaced 8 `resolveX` functions (~170 lines) with single `resolve[T any]` generic (~64 lines)
- **`ProviderConfigWithTunnel`** — replaced 70-line explicit struct with ~40-line reflection-based implementation
- **Version defaults** — unset version now reports `"dev"` instead of empty string; `version`, `commit`, and `buildTime` are set via ldflags

### Improved

- **MigrationPlan** — enhanced with type mapping details, unsupported field detection with reasons, lossy conversion warnings, estimated rows per table, and field mapping visibility
- **`planTypeMappings`** — inspects source schema, iterates all columns, builds per-table `ColumnTypeMapping` with source/dest/lossy flags
- **Checkpoint store** — `FileCheckpointStore.Save`, `Load`, and `Clear` now check `ctx.Err()` before I/O operations for faster cancellation
- **SQL-to-NoSQL documentation** — added foreign key handling section with workarounds (denormalization views, MongoDB `$lookup`, application-level references), `_id` derivation rules, and MySQL-to-MongoDB example
- **Transform documentation** — added `planTypeMappings` explanation and custom `TypeMapperProvider` implementation example
- **LIMITATIONS.md** — added multi-source consolidation limitation and cross-references to all new documentation
- **README.md** — added MySQL-to-MongoDB example, 16-row documentation index table
- **Example configs** — added 8 provider-specific config files for common migration scenarios

### Fixed

- **SSH tunnel reconnect** — `reconnect()` now rebinds the same local port via `listenReuse()` instead of opening a random port, which broke existing DB driver connections
- **MSSQL `SET IDENTITY_INSERT`** — now runs within a transaction to pin the same connection (Go's connection pool hands out different connections otherwise)
- **MSSQL schema migrator** — `getTableColumns` query now returns correct lengths for `NVARCHAR`/`VARCHAR` (was creating `NVARCHAR(1)` instead of `NVARCHAR(100)`) and correct precision/scale for `DECIMAL` types
- **Dead imports** — removed unused `pgx/pgtype` and `encoding/json` imports from MySQL and MariaDB writers (these were dead code since `sonic.Unmarshal` produces `float64`, not `json.Number`)
- **MCP server cleanup** — fixed 3 `errcheck` warnings in MCP server cleanup paths

## [0.1.2] - 2026-04-10

### Fixed

- Silence errcheck on scanner.Close in cross_verifier

## [0.1.1] - 2026-03-28

### Fixed

- Format files with gofmt

## [0.1.0] - 2026-03-20

### Added

- Initial release with 8 database providers (PostgreSQL, MySQL, MariaDB, CockroachDB, MSSQL, SQLite, MongoDB, Redis)
- Streaming pipeline with concurrent writers
- Checkpoint and resume
- Post-migration verification
- SSH tunneling
- Dry-run mode
- Field mapping and transformation layer
