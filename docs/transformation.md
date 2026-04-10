# Transformation Layer

The transformation layer converts data between provider formats when the source and destination use different database engines. Same-engine migrations (e.g. Postgres to Postgres) skip this step entirely with a `NoopTransformer`.

## How transformations are resolved

```mermaid
flowchart TB
    SRC["Source provider name"]
    DST["Destination provider name"]

    SAME{"src == dst?"}

    NOOP["NoopTransformer<br/>(pass-through, zero cost)"]

    REG["Transformer Registry<br/>map[pairKey]func() Transformer"]

    CROSS["Cross-DB Transformer<br/>(e.g. SQLToSQL, SQLToMongo)"]

    SRC --> SAME
    DST --> SAME
    SAME -->|yes| NOOP
    SAME -->|no| REG
    REG --> CROSS

    style NOOP fill:#2ecc71,color:#fff
    style CROSS fill:#e67e22,color:#fff
```

## Transformer interface

```mermaid
classDiagram
    class Transformer {
        <<interface>>
        +Transform(ctx, units) []MigrationUnit, error
        +NeedsSchema() bool
        +SetSchema(schema)
    }

    class NoopTransformer {
        Transform() pass-through
        NeedsSchema() false
    }

    class SQLToSQLTransformer {
        -TypeMapper mapper
        -NullHandler nullHandler
        -FieldMappingApplier mappings
        Transform() type conversion
        NeedsSchema() true
    }

    class SQLToMongoTransformer {
        Transform() row to document
        NeedsSchema() true
    }

    class RedisToMongoTransformer {
        Transform() hash to document
        NeedsSchema() false
    }

    class MongoToRedisTransformer {
        Transform() document to hash
        NeedsSchema() false
    }

    Transformer <|-- NoopTransformer
    Transformer <|-- SQLToSQLTransformer
    Transformer <|-- SQLToMongoTransformer
    Transformer <|-- RedisToMongoTransformer
    Transformer <|-- MongoToRedisTransformer
```

## Transformation cases

### SQL to SQL (e.g. Postgres to MySQL)

```mermaid
flowchart LR
    subgraph Source["Source Schema"]
        T1["users table<br/>id INTEGER<br/>name VARCHAR<br/>created TIMESTAMPTZ<br/>meta JSONB"]
    end

    subgraph Transform["SQLToSQLTransformer"]
        TM["Type mapping<br/>TIMESTAMPTZ → DATETIME<br/>JSONB → JSON<br/>BOOLEAN → TINYINT(1)<br/><br/>Null handling<br/>SET NULL → NULL<br/><br/>Field mapping<br/>(user-defined renames)"]
    end

    subgraph Dest["Destination Schema"]
        T2["users table<br/>id INT<br/>name VARCHAR<br/>created DATETIME<br/>meta JSON<br/><br/>Lost: timezone info<br/>from TIMESTAMPTZ"]
    end

    T1 --> TM --> T2

    style TM fill:#e67e22,color:#fff
```

### SQL to NoSQL (e.g. Postgres to MongoDB)

```mermaid
flowchart LR
    subgraph Source["Source Row"]
        ROW["id: 42<br/>name: 'Alice'<br/>email: 'alice@ex.com'<br/>created: 2024-01-01"]
    end

    subgraph Transform["SQLToMongoTransformer"]
        TM["Row → Document<br/>id → _id<br/>columns → fields<br/>NULL → omit field"]
    end

    subgraph Dest["Destination Document"]
        DOC["_id: 42<br/>name: 'Alice'<br/>email: 'alice@ex.com'<br/>created: ISODate(...)"]
    end

    ROW --> TM --> DOC

    style TM fill:#e67e22,color:#fff
```

### NoSQL to NoSQL (e.g. Redis to MongoDB)

```mermaid
flowchart LR
    subgraph Source["Redis HASH"]
        H1["Key: session:abc<br/>field: user_id → 42<br/>field: expires → 1700..."]
    end

    subgraph Transform["RedisToMongoTransformer"]
        TM["Hash → Document<br/>key → _id<br/>fields → document fields<br/>string values → native types"]
    end

    subgraph Dest["MongoDB Document"]
        D1["_id: session:abc<br/>user_id: 42<br/>expires: 1700..."]
    end

    H1 --> TM --> D1

    style TM fill:#e67e22,color:#fff
```

## Configuration

Transform behavior is controlled by pipeline config:

| Setting      | Purpose                                                    |
| ------------ | ---------------------------------------------------------- |
| `NullPolicy` | How to handle NULL values (`propagate`, `omit`, `default`) |
| `Mappings`   | Per-table field rename rules (e.g. `user_name → username`) |
| `TypeMapper` | Auto-selected based on source/dest provider pair           |

## Lossy conversions

The migration plan (`MigrationPlan.UnsupportedFields`) reports any type mappings that lose information:

| Source Type   | Destination Type | Information Lost        |
| ------------- | ---------------- | ----------------------- |
| `TIMESTAMPTZ` | `TIMESTAMP`      | Timezone offset         |
| `JSONB`       | `JSON`           | Nested query capability |
| `MONEY`       | `DECIMAL`        | Currency symbol         |
| `UUID`        | `VARCHAR`        | Type-level validation   |
