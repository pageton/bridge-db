# MCP System Design

This document proposes a full-system redesign of the bridge-db MCP integration so MCP becomes a first-class interface over the migration engine rather than a thin transport wrapper.

It is intentionally aligned with the current codebase:

- core pipeline orchestration already lives in `internal/bridge/`
- planning already has a structured `bridge.MigrationPlan`
- verification already has a structured `verify.VerificationReport`
- checkpoint and resume already exist in `internal/bridge/checkpoint.go`
- provider discovery and capabilities already exist in `pkg/provider/`

The goal is to reorganize these building blocks behind stable services that can be consumed by both the CLI and MCP.

## Goals

- MCP must call the internal engine directly, not shell out to the CLI.
- CLI and MCP must be separate interfaces over the same application services.
- Planning must be a first-class API, not only a dry-run side effect.
- Migration runs must be addressable, queryable, resumable, and explainable.
- MCP inputs and outputs must be strongly typed, explicit, and machine-friendly.
- Errors must be structured and preserved across service and transport boundaries.
- MCP concerns must stop at the transport boundary and not leak into providers.

## Non-goals

- Replacing the existing provider abstraction.
- Rewriting the transfer pipeline from scratch.
- Introducing MCP-specific behavior inside `providers/<name>/`.
- Removing the CLI.

## Target architecture

```mermaid
flowchart LR
    MCP[MCP transport/server] --> APP[Application services]
    CLI[CLI transport] --> APP

    APP --> PLAN[PlanningService]
    APP --> MIG[MigrationService]
    APP --> VER[VerificationService]
    APP --> REG[ProviderRegistryService]
    APP --> CP[CheckpointService]
    APP --> RUN[RunStatusService]

    PLAN --> CORE[bridge core]
    MIG --> CORE
    VER --> VCORE[verify core]
    CP --> CORE
    REG --> PROV[pkg/provider registry]

    CORE --> PROV
    VCORE --> PROV
```

### Layer responsibilities

#### 1. MCP transport/server

Package responsibility:

- tool registration
- request validation
- schema publication
- structured response serialization
- mapping service errors to MCP tool errors

Must not:

- build command strings
- duplicate migration orchestration
- know provider implementation details
- own checkpoint logic

#### 2. Application services

Packages suggested under `internal/app/`.

Responsibilities:

- accept stable request models
- orchestrate planning, execution, verification, status tracking, and provider discovery
- reuse the existing bridge and verify engines
- adapt internal models into transport-safe response models

This layer is the shared boundary for both CLI and MCP.

#### 3. Planning layer

Responsibilities:

- connect to source/destination providers as needed
- inspect schema and enumerate tables
- resolve transformer and type mappings
- return a first-class `MigrationPlan`
- generate warnings before execution

#### 4. Execution pipeline

Responsibilities:

- run the transfer pipeline
- publish run lifecycle events
- checkpoint state
- expose progress snapshots
- emit final run result and failure summary

#### 5. Verification layer

Responsibilities:

- run verification independently of migration execution
- return `VerificationReport`
- support verification-only workflows and post-run verification

#### 6. Provider layer

Responsibilities:

- provider registry
- provider capabilities
- scanner/writer/verifier/schema migrator implementation

Must remain transport-agnostic.

## Separation between CLI and MCP

The CLI and MCP should become sibling interfaces over the same services.

```mermaid
flowchart TB
    CLI[internal/cli] --> SERVICES[internal/app/*]
    MCP[internal/mcp] --> SERVICES
    SERVICES --> BRIDGE[internal/bridge]
    SERVICES --> VERIFY[internal/verify]
    SERVICES --> PROVIDER[pkg/provider]
```

### CLI behavior

- human-oriented flags and console output
- convenience defaults and terminal reporter
- can still offer synchronous execution UX

### MCP behavior

- structured requests only
- no shell-like free-form command strings
- JSON-first outputs for models and tooling
- run status is pollable by `run_id`

## Proposed package layout

```text
internal/
  app/
    models/
      migration.go
      provider.go
      verification.go
      errors.go
    planning/
      service.go
      mapper.go
    migration/
      service.go
      runner.go
      status_store.go
    verification/
      service.go
    providers/
      registry_service.go
    checkpoints/
      service.go
  mcp/
    server.go
    tool_schemas.go
    tool_handlers.go
    error_adapter.go
  bridge/
    pipeline.go
    plan.go
    checkpoint.go
    ...
  verify/
    cross_verifier.go
    ...
pkg/
  provider/
    provider.go
    capabilities.go
    factory.go
```

### Notes on placement

- `internal/bridge` remains the execution engine.
- `internal/verify` remains the verification engine.
- `internal/app/models` defines stable internal application models and MCP-facing DTOs where the current engine types are not enough.
- `internal/mcp` becomes transport-only.

## Service boundaries

## `PlanningService`

Purpose: make planning a first-class operation.

```go
type PlanningService interface {
    PlanMigration(ctx context.Context, req PlanMigrationRequest) (*MigrationPlan, error)
    ExplainPlan(ctx context.Context, req ExplainMigrationPlanRequest) (*MigrationPlanExplanation, error)
}
```

Responsibilities:

- validate request
- resolve provider configs
- inspect source schema when available
- inspect destination capabilities
- resolve transformer and type mappings
- return normalized plan model
- emit structured warnings and unsupported-field findings

Implementation notes:

- Extract the current planning logic in `internal/bridge/plan.go` into a reusable planning component.
- Keep `bridge.MigrationPlan` as the core plan object.
- Add an adapter layer rather than duplicating plan logic in MCP handlers.

## `MigrationService`

Purpose: create, execute, and manage migration runs.

```go
type MigrationService interface {
    StartRun(ctx context.Context, req RunMigrationRequest) (*MigrationRun, error)
    GetRun(ctx context.Context, runID string) (*MigrationRunStatus, error)
    ResumeRun(ctx context.Context, req ResumeMigrationRequest) (*MigrationRun, error)
}
```

Responsibilities:

- create `run_id`
- build run-scoped pipeline dependencies
- start migration execution
- store live progress and phase updates
- persist final result
- attach `MigrationPlan`, `VerificationReport`, checkpoint metadata, and failures

Implementation notes:

- Preserve `bridge.Pipeline` as the worker engine.
- Add a run manager around it.
- Use a custom `ProgressReporter` implementation that writes into `RunStatusService`.
- Support async-first behavior even if CLI chooses to block on completion.

## `VerificationService`

Purpose: run verification independently of migration execution.

```go
type VerificationService interface {
    VerifyMigration(ctx context.Context, req VerifyMigrationRequest) (*VerificationReport, error)
}
```

Responsibilities:

- resolve source and destination providers
- apply verification options
- return normalized verification report
- optionally verify against a stored run record

Implementation notes:

- Reuse `internal/verify.CrossVerifier`.
- Preserve existing `verify.VerificationReport` as the core report.

## `ProviderRegistryService`

Purpose: expose provider metadata in a stable application-level contract.

```go
type ProviderRegistryService interface {
    ListProviders(ctx context.Context) ([]ProviderCapabilities, error)
    GetProviderCapabilities(ctx context.Context, providerName string) (*ProviderCapabilities, error)
}
```

Responsibilities:

- enumerate compiled-in providers
- expose static capabilities and metadata
- provide a transport-safe metadata object for MCP and CLI

Implementation notes:

- Wrap `provider.AvailableProviders()` and `provider.KnownCapabilities()`.
- Extend metadata beyond the current static table.

## `CheckpointService`

Purpose: provide explicit checkpoint inspection and resume validation.

```go
type CheckpointService interface {
    LoadCheckpoint(ctx context.Context, ref CheckpointReference) (*CheckpointState, error)
    ValidateResume(ctx context.Context, req ResumeValidationRequest) (*ResumeValidationResult, error)
}
```

Responsibilities:

- load checkpoint state
- validate provider match and config compatibility
- explain invalid resume attempts in structured form

Implementation notes:

- Reuse `bridge.CheckpointStore` and `bridge.Checkpoint`.
- Promote config-hash compatibility checks into a reusable service method instead of keeping them buried in pipeline step logic.

## `RunStatusService`

Purpose: retain live and completed run state.

```go
type RunStatusService interface {
    CreateRun(ctx context.Context, run *MigrationRun) error
    UpdatePhase(ctx context.Context, runID string, phase MigrationPhase) error
    UpdateProgress(ctx context.Context, runID string, progress MigrationProgress) error
    CompleteRun(ctx context.Context, runID string, result MigrationRunResult) error
    FailRun(ctx context.Context, runID string, err StructuredError) error
    GetRun(ctx context.Context, runID string) (*MigrationRunStatus, error)
}
```

Responsibilities:

- store current phase
- store latest progress snapshot
- retain final result after completion
- support querying running and completed migrations

Implementation notes:

- Start with in-memory store for single-process MCP usage.
- Keep the interface persistence-neutral so future file/db-backed stores are possible.

## Stable data models

## `MigrationPlan`

Reuse current `internal/bridge.MigrationPlan` as the core object, with the following MCP-facing shape.

```json
{
  "plan_id": "plan_01J...",
  "source_provider": "postgres",
  "destination_provider": "mysql",
  "migration_mode": "full",
  "cross_database": true,
  "schema_migration_needed": true,
  "verification_mode": "cross",
  "transformer_type": "postgres_to_mysql",
  "tables": [
    { "name": "users", "estimated_rows": 150000 },
    { "name": "orders", "estimated_rows": 420000 }
  ],
  "type_mappings": [
    {
      "table": "users",
      "columns": [
        {
          "column": "created_at",
          "source_type": "TIMESTAMPTZ",
          "dest_type": "TIMESTAMP",
          "lossy": true,
          "needs_convert": true
        }
      ]
    }
  ],
  "unsupported_fields": [
    {
      "table": "users",
      "field": "profile_json",
      "reason": "potentially lossy conversion: JSONB → JSON"
    }
  ],
  "warnings": [
    "destination does not support deferred constraints"
  ],
  "estimated_rows": 570000,
  "estimated_batches": 570,
  "field_mappings": [],
  "summary": {
    "human": "2 tables will be migrated from PostgreSQL to MySQL with schema creation and cross-verification.",
    "risk_level": "medium"
  }
}
```

### Additional normalized fields

Add, on top of the existing engine plan:

- `plan_id`
- `migration_mode` (`full`, `resume`, `verification_only`)
- `cross_database` alias for `CrossDB`
- `schema_migration_needed`
- summary block for assistant-friendly explanations

## `MigrationRun`

Represents the lifecycle record for a migration execution.

```go
type MigrationRun struct {
    RunID              string                 `json:"run_id"`
    Status             MigrationRunStatusType `json:"status"`
    Phase              string                 `json:"phase"`
    StartedAt          time.Time              `json:"started_at"`
    FinishedAt         *time.Time             `json:"finished_at,omitempty"`
    SourceProvider     string                 `json:"source_provider"`
    DestinationProvider string                `json:"destination_provider"`
    Plan               *MigrationPlan         `json:"plan,omitempty"`
    Progress           MigrationProgress      `json:"progress"`
    Result             *MigrationRunResult    `json:"result,omitempty"`
    Checkpoint         *CheckpointState       `json:"checkpoint,omitempty"`
    Warnings           []string               `json:"warnings,omitempty"`
    Errors             []StructuredError      `json:"errors,omitempty"`
}
```

### `MigrationRunStatusType`

```text
PENDING | RUNNING | PAUSED | SUCCEEDED | FAILED | CANCELLED | RESUMABLE | ERROR
```

## `MigrationProgress`

Derived from `provider.ProgressStats` plus run metadata.

```go
type MigrationProgress struct {
    Phase             string         `json:"phase"`
    CurrentTable      string         `json:"current_table,omitempty"`
    RecordsScanned    int64          `json:"records_scanned"`
    RecordsWritten    int64          `json:"records_written"`
    RecordsFailed     int64          `json:"records_failed"`
    RecordsSkipped    int64          `json:"records_skipped"`
    BytesTransferred  int64          `json:"bytes_transferred"`
    Throughput        float64        `json:"throughput_records_per_sec"`
    EstimatedRemaining string        `json:"estimated_remaining,omitempty"`
    CurrentBatchID    int            `json:"current_batch_id,omitempty"`
    TablesCompleted   int            `json:"tables_completed,omitempty"`
    TablesTotal       int            `json:"tables_total,omitempty"`
    UpdatedAt         time.Time      `json:"updated_at"`
}
```

## `VerificationReport`

Reuse `internal/verify.VerificationReport` as the core object, adding an MCP-facing summary block:

```json
{
  "status": "WARN",
  "summary": "Counts matched for most tables, but sampled record mismatches were found in orders.",
  "source_provider": "postgres",
  "destination_provider": "mysql",
  "total_tables": 12,
  "pass_count": 10,
  "warn_count": 1,
  "fail_count": 1,
  "total_sampled": 500,
  "total_mismatches": 8,
  "tables": [],
  "mismatches": [],
  "sampled_records": [],
  "counts": {
    "source_rows": 1200000,
    "destination_rows": 1199994
  }
}
```

### MCP additions

- `summary`
- `sampled_records` extracted from mismatches where useful
- explicit `counts` object

## `ProviderCapabilities`

This should be richer than the current `pkg/provider.Capabilities`.

```go
type ProviderCapabilities struct {
    Name                    string   `json:"name"`
    Kind                    string   `json:"kind"` // sql | nosql
    CompiledIn              bool     `json:"compiled_in"`
    SupportsSchemaMigration bool     `json:"supports_schema_migration"`
    SupportsVerification    bool     `json:"supports_verification"`
    VerificationLevel       string   `json:"verification_level"`
    SupportsTransactions    bool     `json:"supports_transactions"`
    SupportsCheckpointing   bool     `json:"supports_checkpointing"`
    SupportsDryRun          bool     `json:"supports_dry_run"`
    SupportedConflictModes  []string `json:"supported_conflict_modes"`
    SupportedDataTypes      []string `json:"supported_data_types,omitempty"`
    ConstraintNotes         []string `json:"constraint_notes,omitempty"`
    Limitations             []string `json:"limitations,omitempty"`
}
```

## `StructuredError`

This becomes the stable error contract across services and MCP.

```go
type StructuredError struct {
    Code           string            `json:"code"`
    Category       string            `json:"category"`
    Phase          string            `json:"phase"`
    Provider       string            `json:"provider,omitempty"`
    ProviderRole   string            `json:"provider_role,omitempty"` // source | destination
    Retryable      bool              `json:"retryable"`
    HumanMessage   string            `json:"human_message"`
    TechnicalDetail string           `json:"technical_detail,omitempty"`
    Metadata       map[string]string `json:"metadata,omitempty"`
}
```

### Error category vocabulary

Base categories should be compatible with the current `bridge.ErrorCategory` values:

- `config`
- `connection`
- `schema`
- `planning`
- `scan`
- `transform`
- `write`
- `verify`
- `checkpoint`
- `resume`
- `cancelled`
- `internal`

## MCP tool contracts

The MCP layer should expose the following tools.

## 1. `plan_migration`

### Purpose

Build a migration plan without executing the migration.

### Request schema

```json
{
  "source": {
    "provider": "postgres",
    "url": "postgresql://user:pass@db:5432/app"
  },
  "destination": {
    "provider": "mysql",
    "url": "mysql://user:pass@tcp(db:3306)/app"
  },
  "tables": ["users", "orders"],
  "batch_size": 1000,
  "migrate_schema": true,
  "verify": true,
  "parallelism": 4,
  "field_mappings": [],
  "checkpoint": {
    "enabled": true,
    "path": ".bridge-db/checkpoint.json"
  }
}
```

### Response schema

```json
{
  "ok": true,
  "plan": { "...": "MigrationPlan" }
}
```

## 2. `run_migration`

### Purpose

Start a migration run from structured input.

### Request schema

```json
{
  "source": {
    "provider": "postgres",
    "url": "postgresql://user:pass@db:5432/app"
  },
  "destination": {
    "provider": "mysql",
    "url": "mysql://user:pass@tcp(db:3306)/app"
  },
  "tables": ["users", "orders"],
  "batch_size": 1000,
  "parallelism": 4,
  "write_workers": 2,
  "verify": true,
  "migrate_schema": true,
  "checkpoint": {
    "enabled": true,
    "path": ".bridge-db/checkpoint.json"
  },
  "resume": false,
  "wait_for_completion": false,
  "failure_mode": {
    "fail_fast": false,
    "max_retries": 3
  }
}
```

### Response schema

```json
{
  "ok": true,
  "run": {
    "run_id": "run_01J...",
    "status": "RUNNING",
    "phase": "planning",
    "started_at": "2026-04-11T16:12:00Z",
    "progress": {
      "phase": "planning",
      "records_scanned": 0,
      "records_written": 0,
      "throughput_records_per_sec": 0,
      "updated_at": "2026-04-11T16:12:00Z"
    },
    "plan": { "...": "MigrationPlan" }
  }
}
```

### Final result shape when `wait_for_completion=true`

```json
{
  "ok": true,
  "run": {
    "run_id": "run_01J...",
    "status": "SUCCEEDED",
    "phase": "complete",
    "started_at": "2026-04-11T16:12:00Z",
    "finished_at": "2026-04-11T16:18:49Z",
    "progress": {
      "phase": "complete",
      "records_scanned": 570000,
      "records_written": 570000,
      "throughput_records_per_sec": 1463.2,
      "updated_at": "2026-04-11T16:18:49Z"
    },
    "result": {
      "verification_status": "PASS",
      "tables": [],
      "warnings": [],
      "errors": []
    }
  }
}
```

### Failure categories to expose

- config
- connection
- schema
- planning
- scan
- transform
- write
- verify
- checkpoint
- resume
- cancelled
- internal

## 3. `verify_migration`

### Purpose

Run verification independently.

### Request schema

```json
{
  "source": {
    "provider": "postgres",
    "url": "postgresql://user:pass@db:5432/app"
  },
  "destination": {
    "provider": "mysql",
    "url": "mysql://user:pass@tcp(db:3306)/app"
  },
  "tables": ["users", "orders"],
  "sample_mode": "pct",
  "sample_pct": 5,
  "counts_only": false,
  "checksum": true
}
```

### Response schema

```json
{
  "ok": true,
  "report": {
    "status": "PASS",
    "summary": "All sampled tables matched.",
    "counts": {
      "source_rows": 570000,
      "destination_rows": 570000
    },
    "sampled_records": [],
    "mismatches": [],
    "tables": []
  }
}
```

## 4. `list_providers`

### Purpose

List all compiled-in providers.

### Request schema

```json
{}
```

### Response schema

```json
{
  "ok": true,
  "providers": [
    {
      "name": "postgres",
      "kind": "sql",
      "supports_schema_migration": true,
      "supports_verification": true,
      "supports_transactions": true,
      "supports_checkpointing": true
    }
  ]
}
```

## 5. `get_provider_capabilities`

### Purpose

Return structured metadata for one provider.

### Request schema

```json
{ "provider": "postgres" }
```

### Response schema

```json
{
  "ok": true,
  "provider": {
    "name": "postgres",
    "kind": "sql",
    "compiled_in": true,
    "supports_schema_migration": true,
    "supports_verification": true,
    "verification_level": "cross",
    "supports_transactions": true,
    "supports_checkpointing": true,
    "supported_conflict_modes": ["overwrite", "skip", "error"],
    "supported_data_types": ["row"],
    "limitations": []
  }
}
```

## 6. `get_migration_status`

### Purpose

Query a running or completed migration.

### Request schema

```json
{ "run_id": "run_01J..." }
```

### Response schema

```json
{
  "ok": true,
  "run": {
    "run_id": "run_01J...",
    "status": "RUNNING",
    "phase": "scanning",
    "started_at": "2026-04-11T16:12:00Z",
    "progress": {
      "phase": "scanning",
      "current_table": "orders",
      "records_scanned": 320000,
      "records_written": 300000,
      "throughput_records_per_sec": 1800.5,
      "updated_at": "2026-04-11T16:15:28Z"
    },
    "warnings": [],
    "errors": []
  }
}
```

## 7. `resume_migration`

### Purpose

Resume a migration from stored checkpoint state.

### Request schema

```json
{
  "checkpoint": {
    "path": ".bridge-db/checkpoint.json"
  },
  "source": {
    "provider": "postgres",
    "url": "postgresql://user:pass@db:5432/app"
  },
  "destination": {
    "provider": "mysql",
    "url": "mysql://user:pass@tcp(db:3306)/app"
  },
  "wait_for_completion": false
}
```

### Response schema

```json
{
  "ok": true,
  "resume": {
    "valid": true,
    "resumed": true,
    "run": {
      "run_id": "run_01J...",
      "status": "RUNNING"
    },
    "checkpoint": {
      "path": ".bridge-db/checkpoint.json",
      "last_batch_id": 120,
      "tables_completed": ["users"]
    }
  }
}
```

### Invalid resume response

```json
{
  "ok": false,
  "error": {
    "category": "resume",
    "phase": "planning",
    "retryable": false,
    "human_message": "Checkpoint configuration is incompatible with the current request.",
    "technical_detail": "config hash mismatch: checkpoint=ab12cd34 current=ef56gh78"
  }
}
```

## 8. `explain_migration_plan`

### Purpose

Translate a structured plan into an assistant-friendly explanation.

### Request schema

```json
{
  "plan": { "...": "MigrationPlan" },
  "detail_level": "standard"
}
```

### Response schema

```json
{
  "ok": true,
  "explanation": {
    "what_will_happen": [
      "bridge-db will create destination tables before copying data.",
      "Data will be copied in batches of 1000 records.",
      "Cross-verification will run after the transfer completes."
    ],
    "what_might_fail": [
      "TIMESTAMPTZ values may lose timezone information in MySQL."
    ],
    "requires_review": [
      "Review unsupported field mappings for profile_json."
    ],
    "transformations": [
      "created_at: TIMESTAMPTZ → TIMESTAMP"
    ],
    "limitations": [
      "Destination does not support deferred constraints."
    ],
    "summary": "This migration is feasible, but one lossy type conversion should be reviewed."
  }
}
```

## Input validation rules

All tool boundaries must enforce:

- explicit provider names or resolvable URLs
- numeric bounds (`batch_size > 0`, `parallelism >= 1`)
- enumerated values (`sample_mode`, conflict strategies, verification mode)
- checkpoint path presence when resume is requested
- source/destination presence for plan, run, verify, and resume
- no raw shell-style strings such as `"--batch-size 1000 --resume"`

## Structured error model

Errors should be returned as structured tool responses and, where appropriate, as MCP protocol errors.

### Mapping rules

| Service error source | MCP error category | Notes |
| --- | --- | --- |
| `bridge.NewConfigError` | `config` | request or pipeline option invalid |
| `bridge.NewConnectionError` | `connection` | source or destination connect/ping/tunnel failure |
| `bridge.NewSchemaError` | `schema` | inspection or DDL migration failure |
| planning validation failure | `planning` | unsupported configuration before execution |
| `bridge.NewScanError` | `scan` | source read failure |
| `bridge.NewTransformError` | `transform` | transform/type conversion failure |
| `bridge.NewWriteError` | `write` | destination write failure |
| `bridge.NewVerifyError` | `verify` | post-run verification failure |
| checkpoint load/save failure | `checkpoint` | file or integrity issue |
| resume validation failure | `resume` | config hash mismatch, missing checkpoint |
| unknown or uncategorized error | `internal` | fallback |

### Example error payload

```json
{
  "ok": false,
  "error": {
    "code": "BRIDGE_RESUME_HASH_MISMATCH",
    "category": "resume",
    "phase": "planning",
    "provider": "postgres",
    "provider_role": "source",
    "retryable": false,
    "human_message": "The saved checkpoint cannot be resumed with the current migration settings.",
    "technical_detail": "config hash mismatch: checkpoint=ab12cd34 current=ef56gh78",
    "metadata": {
      "checkpoint_path": ".bridge-db/checkpoint.json"
    }
  }
}
```

## Migration run lifecycle

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> RUNNING
    RUNNING --> PAUSED
    PAUSED --> RUNNING
    RUNNING --> SUCCEEDED
    RUNNING --> FAILED
    RUNNING --> CANCELLED
    FAILED --> RESUMABLE
    RESUMABLE --> RUNNING
    SUCCEEDED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

### Lifecycle phases

The run should surface the same user-visible phases already present in the pipeline:

1. `init`
2. `tunnel`
3. `connecting`
4. `schema_migration`
5. `planning`
6. `scanning`
7. `verifying`
8. `finalizing`
9. `complete`
10. `error`

### Status query semantics

`get_migration_status` should return:

- current `status`
- current `phase`
- latest `MigrationProgress`
- last warnings
- structured errors
- final result when available

Completed runs should remain queryable for a retention period configurable by the host process.

## Explainability design

`explain_migration_plan` should be deterministic and structure-preserving.

It should derive its explanation from:

- `MigrationPlan.Tables`
- `MigrationPlan.TypeMappings`
- `MigrationPlan.UnsupportedFields`
- `MigrationPlan.Warnings`
- provider capabilities
- whether schema migration and verification are enabled

It must not invent transformations that are absent from the plan.

## How MCP differs from CLI

| Area | CLI | MCP |
| --- | --- | --- |
| Input model | flags, env, config files, URLs | typed JSON request objects |
| Output model | human text and terminal progress | structured JSON documents |
| Run tracking | local process output | pollable `run_id` state |
| Planning | hidden in migrate/dry-run UX | first-class `plan_migration` tool |
| Explainability | user interprets logs | `explain_migration_plan` tool |
| Resume UX | `--resume` flag | `resume_migration` with validation result |
| Error shape | mixed text and exit codes | structured error objects |

The CLI remains the best interface for operators at a terminal. MCP becomes the best interface for assistants, automation, and tool-driven orchestration.

## Safe usage guidance for AI assistants

Assistants using the MCP interface should follow this sequence:

1. Call `list_providers` or `get_provider_capabilities` when provider support is uncertain.
2. Call `plan_migration` before `run_migration`.
3. Call `explain_migration_plan` when the user needs a human explanation.
4. Warn on:
   - lossy type mappings
   - unsupported fields
   - missing schema support
   - disabled verification
   - resume validation failures
5. Prefer `get_migration_status` over assuming completion.
6. Use `verify_migration` independently after interrupted or externally-triggered runs.

Assistants should never invent provider capabilities or claim a migration succeeded without a successful final run status or verification report.

## Recommended refactor plan

The safest migration path is incremental.

### Phase 1: extract shared application services

- Add `internal/app/providers.RegistryService`
- Add `internal/app/verification.Service`
- Add `internal/app/planning.Service`
- Move MCP and CLI config-to-service request mapping into shared helpers

Outcome:

- removes duplicated orchestration in `internal/cli` and `internal/mcp`
- planning becomes callable without `dry_run`

### Phase 2: add stable run model and status store

- Add `MigrationRun`, `MigrationProgress`, `MigrationRunResult`
- Add in-memory `RunStatusService`
- Implement `run_migration`, `get_migration_status`
- Wire a status reporter into pipeline execution

Outcome:

- MCP can start runs and poll them
- CLI can optionally reuse the same run model for local status output

### Phase 3: promote checkpoint and resume to explicit services

- Extract config-hash validation from pipeline step logic into `CheckpointService`
- Implement `resume_migration`
- Add checkpoint metadata to run state

Outcome:

- resume becomes explicit and explainable
- invalid resume attempts produce structured errors

### Phase 4: harden provider discovery and explanation support

- Expand provider metadata model
- Implement `get_provider_capabilities`
- Implement `explain_migration_plan`

Outcome:

- AI assistants can reason about migrations without parsing logs

### Phase 5: simplify MCP transport layer

- Replace direct bridge/verify calls in `internal/mcp/tools.go` with service invocations
- keep transport-only request validation and response serialization
- remove thin-wrapper duplication

Outcome:

- MCP is a clean first-class interface over the application services

## Migration path from the current MCP wrapper

## Current state

Today `internal/mcp`:

- directly builds config objects
- directly instantiates providers
- directly runs `bridge.NewPipeline(...).Run(ctx)`
- directly runs `verify.NewCrossVerifier(...).Verify(ctx)`
- returns flattened string-based errors in several tool outputs

## Transitional target

The first transitional target should look like this:

```text
internal/mcp -> internal/app/* -> internal/bridge + internal/verify + pkg/provider
internal/cli -> internal/app/* -> internal/bridge + internal/verify + pkg/provider
```

## Compatibility strategy

### Existing tools

- `migrate` can be kept temporarily as an alias for `run_migration`
- `verify` can be kept temporarily as an alias for `verify_migration`
- `dry_run` can be kept temporarily as an alias for `plan_migration`
- `inspect_schema` may remain as an additional tool, but it is not part of the minimum full-system MCP contract

### Deprecation plan

1. introduce new tools with stable schemas
2. keep old tool names for one release window
3. mark old tools deprecated in tool descriptions and docs
4. remove thin-wrapper-only handlers after downstream clients migrate

## Example end-to-end assistant workflow

### 1. Plan

```json
{
  "tool": "plan_migration",
  "arguments": {
    "source": { "provider": "postgres", "url": "postgresql://src/app" },
    "destination": { "provider": "mysql", "url": "mysql://dst/app" },
    "batch_size": 1000,
    "verify": true,
    "migrate_schema": true
  }
}
```

### 2. Explain

```json
{
  "tool": "explain_migration_plan",
  "arguments": {
    "plan": { "plan_id": "plan_01J..." },
    "detail_level": "standard"
  }
}
```

### 3. Run

```json
{
  "tool": "run_migration",
  "arguments": {
    "source": { "provider": "postgres", "url": "postgresql://src/app" },
    "destination": { "provider": "mysql", "url": "mysql://dst/app" },
    "batch_size": 1000,
    "parallelism": 4,
    "verify": true,
    "wait_for_completion": false
  }
}
```

### 4. Poll

```json
{
  "tool": "get_migration_status",
  "arguments": {
    "run_id": "run_01J..."
  }
}
```

### 5. Verify independently if needed

```json
{
  "tool": "verify_migration",
  "arguments": {
    "source": { "provider": "postgres", "url": "postgresql://src/app" },
    "destination": { "provider": "mysql", "url": "mysql://dst/app" },
    "sample_mode": "pct",
    "sample_pct": 5
  }
}
```

## Summary of recommended design decisions

- Keep `bridge.Pipeline` as the execution engine.
- Keep `verify.CrossVerifier` as the verification engine.
- Keep `bridge.MigrationPlan` as the core planning object.
- Introduce `internal/app/*` services as the shared boundary for CLI and MCP.
- Add `MigrationRun`, `MigrationProgress`, `ProviderCapabilities`, and `StructuredError` as stable service/MCP models.
- Make `run_migration` async-first with `run_id` and `get_migration_status`.
- Promote resume validation to an explicit service and tool.
- Make explanation a first-class structured capability rather than expecting assistants to parse raw plans.

This design turns MCP into a complete system interface over the bridge-db core while preserving the current engine investments and repository conventions.
