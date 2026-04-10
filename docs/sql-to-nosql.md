# SQL to NoSQL Migration Flow

This document explains how data moves from a SQL database (PostgreSQL, MySQL, MariaDB, CockroachDB, MSSQL, SQLite) to a NoSQL destination (MongoDB or Redis), step by step, through the actual code.

## Overview

```mermaid
flowchart LR
    subgraph Source["SQL Source"]
        DB[(MySQL / Postgres /<br/>MariaDB / etc.)]
    end

    subgraph Scanner["Scanner"]
        SCAN["providers/mysql/scanner.go<br/>Next() → []MigrationUnit"]
    end

    subgraph Transform["Transform"]
        TX["internal/transform/sql_to_nosql.go<br/>SQLToRedis() or SQLToMongoDB()"]
    end

    subgraph Writer["Writer"]
        WR["providers/mongodb/writer.go<br/>or providers/redis/writer.go"]
    end

    subgraph Dest["NoSQL Destination"]
        DDB[(MongoDB / Redis)]
    end

    DB --> SCAN --> TX --> WR --> DDB
```

## Step 1: SQL Scanning — Rows become MigrationUnits

**File: `providers/<sql>/scanner.go`** (e.g. `providers/mysql/scanner.go:73`)

Each SQL provider implements its own scanner. The MySQL scanner is representative:

```go
// providers/mysql/scanner.go:39-69
type mysqlScanner struct {
    db              *sql.DB
    opts            provider.ScanOptions
    tables          []string
    currentTable    int
    rows            *sql.Rows
    columns         []columnInfo
    pkColumns       []string
    done            bool
    tablesCompleted map[string]bool // tables to skip on resume
}
```

### Scan loop

`Next()` (`providers/mysql/scanner.go:73`) drives the scan loop:

1. **List tables** (`listTables`, line 167): runs `SHOW TABLES`, filters out tables in `TablesCompleted` (for resume).
2. **Get table info** (`getTableInfo`, line 209): runs `DESCRIBE <table>` to get column names, types, nullable info, and primary key columns.
3. **Open cursor** (`buildScanQuery`, line 249): builds `SELECT <cols> FROM <table> ORDER BY <pk>` and executes it via `db.QueryContext`.
4. **Read rows** (`readRow`, line 273): scans each row into a `map[string]any` and wraps it in a SQL row envelope:

```go
// providers/mysql/scanner.go:288-336 (simplified)
func (s *mysqlScanner) readRow(ctx context.Context) (*provider.MigrationUnit, error) {
    data := make(map[string]any)
    pk := make(map[string]any)
    columnTypes := make(map[string]string)

    for i, col := range s.columns {
        val := convertValue(values[i])
        data[col.Name] = val
        columnTypes[col.Name] = col.Type
        // extract primary key columns
        for _, pkCol := range s.pkColumns {
            if col.Name == pkCol {
                pk[col.Name] = val
            }
        }
    }

    row := &mysqlRow{
        Table: table, PrimaryKey: pk,
        Data: data, ColumnTypes: columnTypes,
    }
    rowData, _ := encodeMySQLRow(row) // JSON envelope

    return &provider.MigrationUnit{
        Key:      buildRowKey(table, pk), // "table:pk1:pk2"
        Table:    table,
        DataType: provider.DataTypeRow,
        Data:     rowData,
        Meta: provider.UnitMeta{
            PrimaryKey: pk, ColumnTypes: columnTypes,
        },
        Size: int64(len(rowData)),
    }, nil
}
```

### SQL row envelope format

The `Data []byte` field holds a JSON envelope:

```json
{
  "table": "users",
  "schema": "public",
  "primary_key": { "id": 42 },
  "data": { "id": 42, "name": "Alice", "email": "alice@example.com" },
  "column_types": {
    "id": "integer",
    "name": "varchar(255)",
    "email": "varchar(255)"
  }
}
```

The `schema` field is present for PostgreSQL and CockroachDB (set in provider-specific scanner), absent for MySQL/SQLite/MariaDB/MSSQL.

## Step 2: Transformation — SQL rows become NoSQL documents/keys

**File: `internal/transform/sql_to_nosql.go`**

The pipeline resolves the transformer in `stepInitProviders` (`internal/bridge/pipeline.go:325-329`):

```go
p.transformer = transform.GetTransformer(
    p.config.Source.Provider,
    p.config.Destination.Provider,
)
```

`GetTransformer` (`internal/transform/registry.go:35-48`) looks up the source→destination pair in the registry. For SQL→MongoDB and SQL→Redis, no dedicated per-provider transformer is registered — instead, the pipeline uses `NoopTransformer` and the actual conversion happens via the shared functions `SQLToMongoDB()` and `SQLToRedis()`.

### SQL to MongoDB

**File: `internal/transform/sql_to_nosql.go:81-142`**

`SQLToMongoDB()` converts each SQL row envelope into a MongoDB document envelope:

```go
func SQLToMongoDB(units []provider.MigrationUnit, cfg *TransformerConfig) ([]provider.MigrationUnit, error) {
    for _, unit := range units {
        // 1. Unmarshal SQL row envelope
        var envelope map[string]any
        sonic.Unmarshal(unit.Data, &envelope)
        data := envelope["data"].(map[string]any)
        table := envelope["table"].(string)

        // 2. Apply null handler (drop/replace/error/passthrough)
        if cfg.NullHandler != nil {
            data, _ = cfg.NullHandler.Apply(data)
        }

        // 3. Apply field mappings (rename/drop/convert)
        if cfg.FieldMapping != nil {
            data, _ = cfg.FieldMapping.Apply(table, data)
        }

        // 4. Extract document ID from primary key
        pk := envelope["primary_key"].(map[string]any)
        docID := sanitizeDocID(pk, unit.Key)

        // 5. Build document with _id
        doc := make(map[string]any, len(data))
        for k, v := range data { doc[k] = v }
        doc["_id"] = unit.Key

        // 6. Create MongoDB envelope
        mongoEnvelope := map[string]any{
            "collection":  table,
            "document_id": docID,
            "document":    doc,
        }
        encoded, _ := sonic.Marshal(mongoEnvelope)
        result = append(result, provider.MigrationUnit{
            Key: unit.Key, Table: table,
            DataType: provider.DataTypeDocument,
            Data: encoded, Size: int64(len(encoded)),
        })
    }
    return result, nil
}
```

The output envelope:

```json
{
  "collection": "users",
  "document_id": "42",
  "document": {
    "_id": "users:42",
    "id": 42,
    "name": "Alice",
    "email": "alice@example.com"
  }
}
```

### SQL to Redis

**File: `internal/transform/sql_to_nosql.go:14-77`**

`SQLToRedis()` converts each SQL row into a Redis hash:

```go
func SQLToRedis(units []provider.MigrationUnit, cfg *TransformerConfig) ([]provider.MigrationUnit, error) {
    for _, unit := range units {
        var envelope map[string]any
        sonic.Unmarshal(unit.Data, &envelope)
        data := envelope["data"].(map[string]any)

        // Apply null handler and field mappings...

        // Flatten complex values to JSON strings
        fields := make(map[string]any)
        for k, v := range data {
            switch val := v.(type) {
            case map[string]any, []any:
                b, _ := sonic.Marshal(val)
                fields[k] = string(b) // nested data becomes JSON strings
            default:
                fields[k] = v
            }
        }

        redisEnvelope := map[string]any{
            "type":        "hash",
            "value":       fields,
            "ttl_seconds": 0,
        }
        encoded, _ := sonic.Marshal(redisEnvelope)
        result = append(result, provider.MigrationUnit{
            Key: unit.Key, DataType: provider.DataTypeHash,
            Data: encoded, Size: int64(len(encoded)),
        })
    }
}
```

The output envelope:

```json
{
  "type": "hash",
  "value": { "id": "42", "name": "Alice", "email": "alice@example.com" },
  "ttl_seconds": 0
}
```

Key behaviors:

- All SQL column values become hash fields with string keys.
- Complex values (JSON objects, arrays) are serialized to JSON strings.
- TTL is set to 0 (no expiry) since SQL has no TTL concept.

## Step 3: Writing to NoSQL destination

### MongoDB Writer

**File: `providers/mongodb/writer.go:41-93`**

The MongoDB writer groups documents by collection and uses bulk operations:

```go
func (w *mongoDBWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
    // 1. Decode each unit's envelope and group by collection
    collectionDocs := make(map[string][]mongoDocument)
    for _, unit := range units {
        doc, _ := decodeMongoDocument(unit.Data)
        collectionDocs[doc.Collection] = append(collectionDocs[doc.Collection], *doc)
    }

    // 2. Write each collection using upsert (overwrite) or skip
    for collection, docs := range collectionDocs {
        w.writeCollection(ctx, collection, docs, &failedKeys, &errors)
    }
}
```

For **overwrite** mode (`providers/mongodb/writer.go:109-153`), it uses bulk `UpdateOne` with `Upsert(true)`:

```go
func (w *mongoDBWriter) writeWithUpsert(ctx context.Context, coll *mongo.Collection, docs []mongoDocument, ...) error {
    var writeModels []mongo.WriteModel
    for _, doc := range docs {
        docID, _ := extractDocumentID(doc.Data)
        filter := bson.M{"_id": docID}
        update := bson.M{"$set": doc.Data}
        model := mongo.NewUpdateOneModel().
            SetFilter(filter).SetUpdate(update).SetUpsert(true)
        writeModels = append(writeModels, model)
    }
    result, _ := coll.BulkWrite(ctx, writeModels, options.BulkWrite().SetOrdered(false))
    w.written += result.UpsertedCount + result.ModifiedCount
}
```

For **skip** mode, it checks existence before inserting.

### Redis Writer

**File: `providers/redis/writer.go:39-113`**

The Redis writer uses pipelined commands for performance:

```go
func (w *redisWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
    pipe := w.client.Pipeline()
    for _, unit := range units {
        rd, _ := decodeRedisData(unit.Data)
        w.applyKey(ctx, pipe, unit.Key, rd, &cmds)
    }
    pipe.Exec(ctx)
}
```

`applyKey` (`providers/redis/writer.go:123-223`) dispatches by type:

- `hash` → `pipe.HSet(ctx, key, fields...)`
- `string` → `pipe.Set(ctx, key, val, 0)`
- `list` → `pipe.RPush(ctx, key, val...)`
- `set` → `pipe.SAdd(ctx, key, val...)`
- `zset` → `pipe.ZAdd(ctx, key, members...)`

TTL is applied with drift compensation (subtracts 1 second from the original TTL).

## Transformation pipeline sequence

The transform step runs inline in the scanner goroutine (`internal/bridge/pipeline.go:550-583`):

```go
// Step 7: Transform — apply data transformation.
if !transform.IsNoopTransformer(p.transformer) {
    p.reporter.OnPhaseChange(provider.PhaseTransforming)
    transformErr := retry.Do(ctx, retry.Config{...}, func() error {
        var terr error
        units, terr = p.transformer.Transform(ctx, units)
        return terr
    })
    // On failure: skip batch (default) or abort (fail-fast mode)
}
```

Then the batch is split by byte budget and sent to writer goroutines:

```go
// Split into sub-batches when MaxBatchBytes is set and exceeded.
for _, sub := range splitBatch(units, p.opts.MaxBatchBytes) {
    batchID++
    scanCh <- scanResult{batchID: batchID, units: sub}
}
```

## Key mapping rules

| SQL Concept        | MongoDB Destination        | Redis Destination               |
| ------------------ | -------------------------- | ------------------------------- |
| Table              | Collection                 | Key prefix (via `Key` field)    |
| Row                | Document                   | Redis hash (type `"hash"`)      |
| Primary key        | `_id` field in document    | Part of the `Key` string        |
| Column             | Document field             | Hash field                      |
| NULL value         | Controlled by `NullPolicy` | Controlled by `NullPolicy`      |
| Nested JSON/arrays | Native MongoDB objects     | JSON-serialized strings in hash |
| Column types       | Ignored (BSON auto-typing) | Ignored (all become strings)    |
| Schema name        | Dropped                    | Dropped                         |
| Foreign key hints  | Dropped                    | Dropped                         |

## Files involved

| File                                  | Role                                                     |
| ------------------------------------- | -------------------------------------------------------- |
| `providers/mysql/scanner.go`          | MySQL row scanning, envelope encoding                    |
| `providers/postgres/scanner.go`       | PostgreSQL row scanning, includes schema name            |
| `providers/sqlite/scanner.go`         | SQLite row scanning                                      |
| `providers/mariadb/scanner.go`        | MariaDB row scanning                                     |
| `providers/cockroachdb/scanner.go`    | CockroachDB row scanning, includes schema name           |
| `providers/mssql/scanner.go`          | MSSQL row scanning                                       |
| `internal/transform/sql_to_nosql.go`  | `SQLToRedis()` and `SQLToMongoDB()` conversion functions |
| `internal/transform/registry.go`      | Transformer lookup by source→destination pair            |
| `internal/transform/null_handler.go`  | Null policy application (drop/replace/error/passthrough) |
| `internal/transform/field_mapping.go` | Field rename/drop/convert rules                          |
| `providers/mongodb/writer.go`         | MongoDB bulk upsert                                      |
| `providers/redis/writer.go`           | Redis pipeline write                                     |
| `internal/bridge/pipeline.go:474-687` | Scanner goroutine that ties it all together              |
| `providers/mysql/types.go`            | `mysqlRow` struct, `encodeMySQLRow`/`decodeMySQLRow`     |
