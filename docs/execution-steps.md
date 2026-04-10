# Execution Steps

The pipeline runs a fixed 10-step sequence. Each step has a single responsibility and clear entry/exit points. Steps are sequential except for 6–8, which overlap in a streaming pipeline.

## Step-by-step flow

```mermaid
flowchart TB
    S1["1. Load config<br/>Validate options, log parameters"]
    S2["2. SSH tunnels<br/>Open tunnels with retry + rollback"]
    S3["3. Init providers<br/>Connect, ping, resolve caps"]
    S4["4. Inspect schema<br/>Load checkpoint, migrate schema"]
    S5["5. Plan migration<br/>Resume logic, build plan"]
    S6["6. Extract<br/>Read batches from source"]
    S7["7. Transform<br/>Convert data between formats"]
    S8["8. Write<br/>Persist batches to destination"]
    S9["9. Verify<br/>Compare source and destination"]
    S10["10. Finalize<br/>Summary, clear checkpoint"]

    S1 --> S2 --> S3 --> S4 --> S5 --> S6
    S6 -.-> S7 -.-> S8
    S8 --> S9 --> S10

    style S6 fill:#4a9eff,color:#fff
    style S7 fill:#4a9eff,color:#fff
    style S8 fill:#4a9eff,color:#fff
    style S9 fill:#2ecc71,color:#fff
    style S10 fill:#2ecc71,color:#fff
```

## Step details

### Step 1: Load config

```mermaid
flowchart LR
    A[PipelineOptions] --> V[Validate]
    V -->|valid| LOG[Log parameters]
    V -->|invalid| ERR["[config] step 1<br/>validation error"]
    LOG --> DONE[Proceed]
```

- Validates batch size, retry count, worker count, conflict strategy
- Logs source/dest providers, cross-DB flag, dry-run mode
- No I/O, no connections

### Step 2: SSH tunnels

```mermaid
flowchart LR
    A[buildTunnelConfigs] --> RETRY["retry.Do (3 attempts,<br/>1s–10s backoff)"]
    RETRY -->|success| DONE[Tunnels open]
    RETRY -->|failure| ROLLBACK["Rollback opened tunnels"]
    ROLLBACK --> ERR["[connection] step 2<br/>tunnel error"]
```

- Opens SSH tunnels for source and/or destination if configured
- On failure, already-opened tunnels are closed (rollback)
- 3 retry attempts with exponential backoff

### Step 3: Init providers

```mermaid
flowchart TB
    A["provider.New(source)"] --> B["provider.New(dest)"]
    B --> C["Connect source<br/>(3 retries)"]
    C --> D["Connect dest<br/>(3 retries)"]
    D --> E["Ping source<br/>(3 retries)"]
    E --> F["Ping dest<br/>(3 retries)"]
    F --> G["Resolve capabilities"]
    G --> H["Resolve transformer"]
    H --> I["runPreflight()"]
    I -->|failure| ERR["[connection/schema] step 3<br/>with provider name"]

    style ERR fill:#e74c3c,color:#fff
```

- Creates provider instances from the registry
- Connects with 3 retries per side (1s–10s backoff)
- Pings to verify liveness
- Resolves capabilities (schema support, verification level, transactions)
- Resolves transformer for the provider pair
- Runs preflight checks (transformer availability, schema caps, FK handling)

### Step 4: Inspect schema

```mermaid
flowchart TB
    CP{Checkpoint<br/>enabled?}
    CP -->|yes| LOAD[Load checkpoint]
    CP -->|no| SKIP[Skip]
    LOAD --> SCHEMA{Schema<br/>migration?}
    SKIP --> SCHEMA
    SCHEMA -->|yes| INSPECT["src.Inspect()"]
    INSPECT --> SET["transformer.SetSchema()"]
    SET --> CREATE["dst.Create()"]
    SCHEMA -->|no| DONE[Proceed]
    CREATE --> DONE
    INSPECT -->|failure| ERR["[schema] step 4"]
    CREATE -->|failure| ERR

    style ERR fill:#e74c3c,color:#fff
```

### Step 5: Plan migration

```mermaid
flowchart TB
    RESUME{Resume from<br/>checkpoint?}
    RESUME -->|yes| HASH["Validate config hash"]
    HASH -->|mismatch| ERR1["[config] step 5<br/>hash changed"]
    HASH -->|match| PROV["Validate providers match"]
    PROV -->|mismatch| ERR2["[config] step 5<br/>provider mismatch"]
    PROV -->|match| KEYS["Restore written keys"]
    KEYS --> DEDUP{"Dedup cap<br/>exceeded?"}
    DEDUP -->|yes, non-overwrite| ERR3["[config] step 5<br/>cannot resume"]
    DEDUP -->|yes, overwrite| WARN["Warn: incomplete dedup"]
    DEDUP -->|no| PLAN
    WARN --> PLAN["buildPlan()"]
    RESUME -->|no| PLAN
    PLAN --> ENUM["Enumerate tables<br/>estimate rows"]

    style ERR1 fill:#e74c3c,color:#fff
    style ERR2 fill:#e74c3c,color:#fff
    style ERR3 fill:#e74c3c,color:#fff
```

### Steps 6–8: Transfer

See [Concurrency Model](concurrency.md) for the detailed goroutine layout. Key behaviors:

- **Scan retry**: `maxRetries+1` attempts, 500ms–10s backoff
- **Transform retry**: same as scan. On failure: skip batch (default) or abort (`--fail-fast`)
- **Write retry**: `maxRetries+1` attempts, 500ms–30s backoff
- **Dedup**: per-batch key filtering against `writtenKeySet`
- **Checkpoint**: throttled by `--checkpoint-interval`

### Step 9: Verify

```mermaid
flowchart TB
    OPTS["User verify options<br/>(or defaults)"]
    LEVEL["EffectiveVerifyLevel<br/>(capability-aware)"]

    OPTS --> LEVEL
    LEVEL -->|cross| CROSS["CrossVerifier<br/>count + sample + checksum"]
    LEVEL -->|basic| BASIC["Destination count check"]
    LEVEL -->|none| SKIP["Warning logged"]

    CROSS --> RESULT["VerificationOK"]
    BASIC --> RESULT
    SKIP --> FALSE["VerificationOK = false"]

    style CROSS fill:#2ecc71,color:#fff
    style BASIC fill:#2ecc71,color:#fff
    style SKIP fill:#95a5a6,color:#fff
```

- Verification failures are **non-fatal** — logged and stored in summary
- `VerifiedAny` tracks whether any real checks ran (prevents false "passed")

### Step 10: Finalize

```mermaid
flowchart LR
    A[Clear checkpoint] --> B[Build summary]
    B --> C[Aggregate failures]
    C --> D[Report completion]
    D --> E[Log final stats]
```

## Error categorization by step

| Step | Possible error categories          |
| ---- | ---------------------------------- |
| 1    | `config`                           |
| 2    | `connection`                       |
| 3    | `connection`, `schema` (preflight) |
| 4    | `schema`                           |
| 5    | `config`                           |
| 6    | `scan` (per-batch, non-fatal)      |
| 7    | `transform` (per-batch, non-fatal) |
| 8    | `write` (per-batch), `cancelled`   |
| 9    | `verify` (non-fatal)               |
| 10   | none                               |
