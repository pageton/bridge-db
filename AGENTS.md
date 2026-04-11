# AGENTS.md

Read `CLAUDE.md` first for the repo's fuller workflow rules; keep this file as the quick-start delta.

## Commands that matter

- `make build` builds `build/bridge` with base providers only.
- `make build-all` builds with all optional providers: `mongodb,mssql,sqlite,redis`.
- `make dev ARGS="migrate --dry-run ..."` runs the CLI without producing a binary.
- `make test` / `make lint` / `make check` respect `PROVIDERS`; default runs do **not** include optional providers.
- CI parity for provider-gated code is `go test -v -count=1 -race -tags "mongodb,mssql,sqlite,redis" ./...` plus `go build -tags "mongodb,mssql,sqlite,redis" -trimpath -ldflags "-s -w" ./cmd/bridge`.
- Focused test example from repo guidance: `go test ./internal/bridge/ -run TestStepLoadConfig -v`.

## Build-tag gotcha

- Base providers are always compiled: `postgres`, `mysql`, `mariadb`, `cockroachdb`.
- `mongodb`, `mssql`, `sqlite`, and `redis` only exist when their build tags are enabled.
- The binary exposes compiled-in providers via `bridge providers`; use this when debugging “unknown provider” failures.

## Repo structure worth knowing

- `cmd/bridge/` is the only binary entrypoint. Provider inclusion is wired here via blank imports in `provider_*.go`; optional providers are gated by `//go:build` files in this directory.
- `pkg/provider/` is the stable public surface. Keep new provider-facing abstractions here; most other code is intentionally `internal/`.
- `providers/<name>/` follows a fixed shape (`provider.go`, `scanner.go`, `writer.go`, `verifier.go`, `schema.go`, optional `dryrun.go`/`types.go`). Reuse that layout for provider changes.
- `internal/bridge/pipeline.go` is the orchestration center. User-visible flow is 8 phases: validate config → validate/tunnel → connect → inspect schema → plan → transfer → verify → finalize.
- `internal/transform/` is registry-based by source/destination pair. Add pair-specific transformers with `RegisterTransformer`; missing pairs fall back to `NoopTransformer`.

## Config and runtime quirks

- `bridge migrate` applies config sources in this order: config file → URL flags → individual flags.
- Environment loading uses `BRIDGE_*` variables (`BRIDGE_SOURCE_URL`, `BRIDGE_DEST_URL`, `BRIDGE_BATCH_SIZE`, etc.).
- Default checkpoint path is `.bridge-db/checkpoint.json`; `--resume` is guarded by config-hash checks, so changing pipeline options can intentionally reject resume.
- Insecure SSH mode is blocked unless `BRIDGE_ALLOW_INSECURE_SSH=1` is set.

## Conventions to match

- Prefer `github.com/bytedance/sonic` for JSON work; that is the dominant serialization choice in core pipeline/config code.
- YAML is `gopkg.in/yaml.v3`.
- Keep changes minimal and local; this repo already has parallel `Makefile` and `justfile` targets, so update both if you change developer workflows.

## Test environment

- `example/` contains a Nix-based integration environment. `cd example && nix develop` auto-starts seeded databases for local migration testing.
- MSSQL is the exception: `example/README.md` documents separate Docker startup and manual DB creation.
