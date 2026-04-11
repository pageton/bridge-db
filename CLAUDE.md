# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```sh
make build            # binary → build/bridge
make build-all        # build with all optional providers (-tags "mongodb,mssql,sqlite,redis")
make dev ARGS="migrate --dry-run ..."  # run without building
make test             # run tests (respects PROVIDERS env var for tags)
make test-all         # test with all provider tags
make test-race        # tests with -race
make test-short       # tests with -short
make test-ci          # verbose + count=1 + race on concurrent packages
make lint             # golangci-lint run --timeout=3m ./...
make check            # build + fmt-check + vet + lint + test
make fmt              # gofmt -w -s .
```

Run a single test: `go test ./internal/bridge/ -run TestStepLoadConfig -v`

Build with specific providers: `PROVIDERS=mongodb,redis make build`

## Architecture

**Module**: `github.com/pageton/bridge-db` · Go 1.25

The tool migrates data between 8 database engines through a streaming pipeline. The architecture has three main layers:

### Provider Interface (`pkg/provider/`)

The central abstraction. Every database adapter implements `Provider`, which is a factory for `Scanner`, `Writer`, `Verifier`, and `SchemaMigrator`. Data flows as `MigrationUnit` envelopes (key, table, data bytes, metadata).

Providers self-register via `init()` + `provider.Register()`. The factory (`pkg/provider/factory.go`) creates instances by name.

### Provider Implementations (`providers/`)

Eight providers, each in its own directory with the same shape: `provider.go`, `scanner.go`, `writer.go`, `verifier.go`, `schema.go`.

- **Base** (always compiled): postgres, mysql, mariadb, cockroachdb
- **Optional** (build-tag gated): mongodb, mssql, redis, sqlite

Build tags are wired through side-effect imports in `cmd/bridge/` (e.g., `provider_mongodb.go` has `//go:build mongodb` and blank-imports `providers/mongodb`).

### Pipeline Engine (`internal/bridge/`)

`Pipeline.Run()` orchestrates 8 visible phases: config validation → SSH tunnels → connect → schema → plan → transfer (concurrent scan/transform/write) → verify → finalize.

Key files: `pipeline.go` (core engine), `plan.go` (table ordering), `checkpoint.go` (resume support), `batch_writer.go`, `preflight.go`.

### Transform Layer (`internal/transform/`)

Registry-based: `RegisterTransformer(srcProvider, dstProvider, ctor)`. Pairs like `sql_to_sql`, `sql_to_nosql`, `nosql_to_sql` handle type mapping and format conversion. Falls back to `NoopTransformer` when no pair is registered.

### Other Internal Packages

- `cli/` — Cobra commands (migrate, verify, version, providers)
- `config/` — YAML/flag/env config with per-provider connection structs
- `progress/` — Metrics collection and console reporter
- `verify/` — Post-migration count + checksum verification
- `tunnel/` — SSH tunnel support for remote databases
- `retry/` — Exponential backoff with per-category policies

## Key Conventions

- JSON serialization uses `bytedance/sonic` (not `encoding/json`)
- YAML via `gopkg.in/yaml.v3`
- `pkg/provider/` is the only public API surface; everything else is `internal/`
- Tests are colocated with source (`_test.go`)
- Version info injected via `-ldflags` at build time
- Both `Makefile` and `justfile` exist with the same targets
