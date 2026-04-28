# Foreign Key Support for SQL → MongoDB Migrations

## Problem

Currently, SQL → MongoDB migrations lose foreign key relationships. They're flattened, producing denormalized documents with no references to related data.

## Solution

Add support for MongoDB `$lookup` stages — documents retain references, and querying layer can perform joins at read time.

## Design

### 1. Schema Extension

Add `ForeignKeys` to `TableSchema`:

```go
// pkg/provider/provider.go
type ForeignKey struct {
    Name           string   // Constraint name (optional)
    Columns        []string // Source columns (FK side)
    ReferencedTable string  // Target table
    ReferencedColumns []string // Target columns (PK side)
}

type TableSchema struct {
    Name       string
    Columns    []ColumnSchema
    Indexes    []IndexSchema
    ForeignKeys []ForeignKey // NEW
}
```

### 2. Transformer Option

Add config option to enable `$lookup` generation:

```go
// internal/config/config.go
type FKHandling string

const (
    FKDefer     FKHandling = "defer_constraints" // default
    FKOrdered   FKHandling = "ordered"
    FKSkip      FKHandling = "skip"
    FKLookup   FKHandling = "lookup"          // NEW: emit $lookup stages
)
```

Or a separate pipeline option:

```go
type PipelineConfig struct {
    // ... existing fields
    EnableFKLookup bool `yaml:"enable_fk_lookup"`
}
```

### 3. Document Output with `$lookup` Metadata

When `FKLookup` is enabled, document includes `$lookup` specification:

```go
map[string]any{
    "collection": "orders",
    "document": map[string]any{
        "order_id": 1,
        "user_id":  42,
        "_id":     "orders:1",
    },
    "lookups": []map[string]any{  // NEW
        {
            "from":     "users",
            "local":    "user_id",
            "foreign": "_id",
            "as":       "user",
        },
    },
}
```

MongoDB writer then applies these as post-write `$lookup` aggregation pipeline.

### 4. FK Detection in SQL Providers

MySQL example (`providers/mysql/schema.go`):

```go
func (m *mysqlSchemaMigrator) Inspect(ctx context.Context) (*provider.Schema, error) {
    // ... existing table/column inspection
    
    // FK detection query (MySQL 8.0+)
    fkQuery := `
        SELECT
            kcu.COLUMN_NAME,
            kcu.CONSTRAINT_NAME,
            kcu.TABLE_NAME,
            kcu.REFERENCED_TABLE_NAME,
            kcu.REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE kcu
        WHERE kcu.TABLE_SCHEMA = DATABASE()
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
    `
    // ... iterate and populate ForeignKeys
}
```

PostgreSQL uses `information_schema.table_constraints` + `information_schema.key_column_usage`.

### 5. Transformer Implementation

New file: `internal/transform/fk_lookup.go`

```go
func init() {
    RegisterTransformer("postgres", "mongodb", func() Transformer {
        return &SQLToMongoTransformerWithFK{}
    })
}
```

## Implementation Order

1. Add `ForeignKey` type to `pkg/provider/provider.go`
2. Extend `TableSchema` with `ForeignKeys []ForeignKey`
3. Update SQL providers to detect FKs in `Inspect()`
4. Add `EnableFKLookup` to pipeline config
5. Create `SQLToMongoDBWithFK` transformer
6. Update MongoDB writer to apply `$lookup`

## Alternative: Pre-migration View (Already Supported)

Current workaround — create denormalized view before migration:

```sql
CREATE VIEW order_details AS
SELECT o.*, u.name AS user_name, u.email AS user_email
FROM orders o
JOIN users u ON o.user_id = u.id;
```

This approach is simpler but requires manual SQL work. The `$lookup` feature automates it.