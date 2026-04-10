# Data Flow Diagram

Data flows through the pipeline as `MigrationUnit` structs — a provider-agnostic envelope that carries a key, table name, data type, and serialized payload.

## MigrationUnit structure

```mermaid
classDiagram
    class MigrationUnit {
        +string Key
        +string Table
        +DataType DataType
        +[]byte Data
        +int64 Size
    }

    class DataType {
        <<enumeration>>
        Row
        String
        Hash
        List
        Set
        ZSet
        Stream
        Document
    }

    MigrationUnit --> DataType
```

## End-to-end data flow

```mermaid
flowchart LR
    subgraph Source["Source Database"]
        SDB[(Postgres / MySQL /<br/>MongoDB / Redis)]
    end

    subgraph Extract
        SC[Scanner<br/>scanner.Next]
    end

    subgraph Transform["Transform (if cross-DB)"]
        TM[Transformer<br/>type mapping +<br/>field conversion]
    end

    subgraph Write
        WR[Writer<br/>writer.Write]
    end

    subgraph Dest["Destination Database"]
        DDB[(MySQL / MongoDB /<br/>Redis / etc.)]
    end

    SDB -->|SQL rows /<br/>documents / keys| SC
    SC -->|[]MigrationUnit| TM
    TM -->|[]MigrationUnit<br/>(converted format)| WR
    WR -->|INSERT / PUT /<br/>bulk write| DDB
```

## Data encoding examples

Each provider serializes its native format into the `Data []byte` field as a JSON envelope:

**SQL row → MigrationUnit**

```json
{
  "table": "users",
  "schema": "public",
  "primary_key": "id:42",
  "data": { "id": 42, "name": "Alice", "email": "alice@example.com" },
  "column_types": { "id": "integer", "name": "varchar", "email": "varchar" }
}
```

**MongoDB document → MigrationUnit**

```json
{
  "collection": "orders",
  "document_id": "507f1f77bcf86cd799439011",
  "document": { "_id": "507f1f77...", "total": 99.95, "items": 3 }
}
```

**Redis key → MigrationUnit**

```json
{
  "type": "hash",
  "key": "session:abc123",
  "value": { "user_id": "42", "expires": "1700000000" }
}
```

## Cross-DB transformation examples

When source and destination use different engines, the transformer converts between formats:

```mermaid
flowchart TB
    subgraph SQL_to_SQL["SQL → SQL (e.g. Postgres → MySQL)"]
        direction LR
        A1["INTEGER"] --> B1["INT"]
        A2["TIMESTAMPTZ"] --> B2["TIMESTAMP"]
        A3["JSONB"] --> B3["JSON"]
        A4["BOOLEAN"] --> B4["TINYINT(1)"]
    end

    subgraph SQL_to_NoSQL["SQL → NoSQL (e.g. Postgres → MongoDB)"]
        direction LR
        C1["Row {id, name}"] --> D1["Document {_id, name}"]
        C2["VARCHAR PK"] --> D2["ObjectId / string"]
        C3["NULL values"] --> D3["Omitted field"]
    end

    subgraph NoSQL_to_NoSQL["NoSQL → NoSQL (e.g. Redis → MongoDB)"]
        direction LR
        E1["Redis HASH"] --> F1["MongoDB Document"]
        E2["Redis key"] --> F2["_id field"]
        E3["Hash fields"] --> F3["Document fields"]
    end
```
