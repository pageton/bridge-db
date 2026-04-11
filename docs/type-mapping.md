# Type Mapping Reference

When migrating between different database engines, bridge-db automatically translates column types from the source dialect to the destination dialect. This document contains the complete mapping tables for all 19 supported SQL-to-SQL pairs, explains SQL-to-NoSQL and NoSQL-to-SQL behavior, and describes how to extend the system with custom type mappers.

## Overview

Type mapping is driven by the `TypeMapper` interface:

```go
// pkg/provider/provider.go
type TypeMapper interface {
    MapType(colType string) (string, bool)
}
```

- `MapType` takes a source column type string (e.g. `"VARCHAR(255)"`, `"TINYINT"`) and returns the destination type and whether a mapping was found.
- Returns `("", false)` for unmappable types — these are flagged as `UnsupportedField` warnings in the migration plan.

The mapping is used during **schema migration** (DDL generation) and **migration planning** (advisory warnings about lossy conversions). It does not modify row data during transfer — data conversion happens in the transformation layer.

## How planTypeMappings works

During the plan phase (`internal/bridge/plan.go`), the function `planTypeMappings` iterates all source tables and columns:

1. Checks if the transformer implements `TypeMapperProvider` (optional interface).
2. Inspects source schema via `SchemaMigrator.Inspect(ctx)`.
3. Calls `mapper.MapType(col.Type)` for each column.
4. Builds a `ColumnTypeMapping` with source type, destination type, whether conversion is needed, and whether it is lossy.
5. Populates `MigrationPlan.TypeMappings` and adds `UnsupportedField` warnings for unmapped or lossy types.

This is purely advisory — it does not modify data. It helps you review what type conversions will happen before committing to the migration.

## SQL to SQL Mapping Tables

### MySQL → PostgreSQL

**File:** `internal/transform/sql_transformers.go:159-224`

| Source (MySQL) | Destination (PostgreSQL) | Notes |
|---|---|---|
| `TINYINT*` | `SMALLINT` | |
| `SMALLINT*` | `SMALLINT` | |
| `MEDIUMINT*` | `INTEGER` | |
| `BIGINT*` | `BIGINT` | |
| `INT*` | `INTEGER` | |
| `FLOAT*` | `REAL` | |
| `DOUBLE*` | `DOUBLE PRECISION` | |
| `DECIMAL*` | `NUMERIC` | |
| `NUMERIC*` | `NUMERIC` | |
| `VARCHAR*` | `VARCHAR` | Length preserved |
| `CHAR*` | `CHAR` | Length preserved |
| `TINYTEXT*` | `TEXT` | |
| `MEDIUMTEXT*` | `TEXT` | |
| `LONGTEXT*` | `TEXT` | |
| `TEXT*` | `TEXT` | |
| `TINYBLOB*` | `BYTEA` | |
| `MEDIUMBLOB*` | `BYTEA` | |
| `LONGBLOB*` | `BYTEA` | |
| `BLOB*` | `BYTEA` | |
| `BINARY*` | `BYTEA` | |
| `VARBINARY*` | `BYTEA` | |
| `DATETIME*` | `TIMESTAMP` | |
| `TIMESTAMP*` | `TIMESTAMP` | |
| `ENUM*` | `VARCHAR(255)` | **Lossy**: constraint lost |
| `SET*` | `VARCHAR(255)` | **Lossy**: multi-value constraint lost |
| `JSON*` | `JSONB` | |
| `YEAR*` | `SMALLINT` | |
| `DATE` | `DATE` | Exact match |
| `TIME` | `TIME` | Exact match |
| `BOOL*` | `BOOLEAN` | |

### PostgreSQL → MySQL

**File:** `internal/transform/sql_transformers.go:229-293`

| Source (PostgreSQL) | Destination (MySQL) | Notes |
|---|---|---|
| `TIMESTAMP WITH TIME ZONE*` | `DATETIME` | **Lossy**: timezone info lost |
| `TIMESTAMP WITHOUT TIME ZONE*` | `DATETIME` | |
| `TIMESTAMP*` | `DATETIME` | |
| `VARCHAR(*` | `VARCHAR` | Length preserved |
| `CHARACTER VARYING*` | `VARCHAR(N)` | Length extracted |
| `CHAR(*` | `CHAR` | Length preserved |
| `CHARACTER*` | `CHAR(255)` | |
| `DOUBLE PRECISION*` | `DOUBLE` | |
| `NUMERIC(N)` | `DECIMAL(N)` | Precision preserved |
| `DECIMAL(N)` | `DECIMAL(N)` | Precision preserved |
| `SMALLINT` | `SMALLINT` | |
| `INTEGER` | `INT` | |
| `BIGINT` | `BIGINT` | |
| `REAL` | `FLOAT` | |
| `NUMERIC` | `DECIMAL` | |
| `DECIMAL` | `DECIMAL` | |
| `CHAR` | `CHAR` | |
| `VARCHAR` | `VARCHAR` | |
| `TEXT` | `TEXT` | |
| `BYTEA` | `BLOB` | |
| `DATE` | `DATE` | |
| `TIME` | `TIME` | |
| `BOOLEAN` | `TINYINT(1)` | Different type family |
| `BOOL` | `TINYINT(1)` | Different type family |
| `JSON` | `JSON` | |
| `JSONB` | `JSON` | **Lossy**: binary JSON optimizations lost |
| `UUID` | `CHAR(36)` | **Lossy**: native UUID operations lost |
| `SERIAL` | `INT AUTO_INCREMENT` | Sequence not created |
| `BIGSERIAL` | `BIGINT AUTO_INCREMENT` | Sequence not created |
| `TIMESTAMPTZ` | `DATETIME` | **Lossy**: timezone info lost |

### SQLite → PostgreSQL

**File:** `internal/transform/sqlite_transformers.go:179-204`

| Source (SQLite) | Destination (PostgreSQL) |
|---|---|
| `INTEGER*`, `INT*` | `INTEGER` |
| `BIGINT*` | `BIGINT` |
| `SMALLINT*` | `SMALLINT` |
| `REAL*`, `DOUBLE*`, `FLOAT*` | `DOUBLE PRECISION` |
| `NUMERIC*`, `DECIMAL*` | `NUMERIC` |
| `TEXT*`, `VARCHAR*`, `CHAR*`, `CLOB*` | `TEXT` |
| `BLOB*` | `BYTEA` |
| `DATETIME`, `TIMESTAMP*` | `TIMESTAMP` |
| `DATE` | `DATE` |
| `BOOLEAN` | `BOOLEAN` |

### SQLite → MySQL

**File:** `internal/transform/sqlite_transformers.go:208-239`

| Source (SQLite) | Destination (MySQL) |
|---|---|
| `INTEGER*`, `INT*` | `INT` |
| `BIGINT*` | `BIGINT` |
| `SMALLINT*` | `SMALLINT` |
| `REAL*`, `FLOAT*` | `FLOAT` |
| `DOUBLE*` | `DOUBLE` |
| `NUMERIC*`, `DECIMAL*` | `DECIMAL` |
| `TEXT*`, `CLOB*` | `LONGTEXT` |
| `VARCHAR*` | `VARCHAR(255)` |
| `CHAR*` | `CHAR(255)` |
| `BLOB*` | `LONGBLOB` |
| `DATETIME`, `TIMESTAMP*` | `DATETIME` |
| `DATE` | `DATE` |
| `BOOLEAN` | `TINYINT(1)` |

### PostgreSQL → SQLite

**File:** `internal/transform/sqlite_transformers.go:243-284`

Default fallback: `TEXT` (any unmatched type maps to TEXT)

| Source (PostgreSQL) | Destination (SQLite) |
|---|---|
| `SERIAL*` | `INTEGER` |
| `BIGSERIAL*` | `INTEGER` |
| `SMALLINT*` | `SMALLINT` |
| `INTEGER*`, `INT*` | `INTEGER` |
| `BIGINT*` | `BIGINT` |
| `REAL*` | `REAL` |
| `DOUBLE PRECISION*` | `REAL` |
| `NUMERIC*`, `DECIMAL*` | `NUMERIC` |
| `CHARACTER VARYING*`, `VARCHAR*` | `TEXT` |
| `CHARACTER*`, `CHAR*` | `TEXT` |
| `TEXT*` | `TEXT` |
| `BYTEA*` | `BLOB` |
| `TIMESTAMP*` | `TEXT` |
| `DATE` | `TEXT` |
| `TIME` | `TEXT` |
| `BOOL*` | `INTEGER` |
| `JSON`, `JSONB` | `TEXT` |
| `UUID` | `TEXT` |

### MySQL → SQLite

**File:** `internal/transform/sqlite_transformers.go:288-325`

Default fallback: `TEXT` (any unmatched type maps to TEXT)

| Source (MySQL) | Destination (SQLite) |
|---|---|
| `TINYINT*`, `SMALLINT*`, `MEDIUMINT*`, `INT*` | `INTEGER` |
| `BIGINT*` | `BIGINT` |
| `FLOAT*` | `REAL` |
| `DOUBLE*` | `REAL` |
| `DECIMAL*`, `NUMERIC*` | `NUMERIC` |
| `VARCHAR*`, `CHAR*`, `TINYTEXT*`, `MEDIUMTEXT*`, `LONGTEXT*`, `TEXT*`, `ENUM*`, `SET*` | `TEXT` |
| `TINYBLOB*`, `MEDIUMBLOB*`, `LONGBLOB*`, `BLOB*`, `BINARY*`, `VARBINARY*` | `BLOB` |
| `DATETIME*`, `TIMESTAMP*` | `TEXT` |
| `DATE` | `TEXT` |
| `TIME` | `TEXT` |
| `YEAR` | `INTEGER` |
| `JSON*` | `TEXT` |
| `BOOL*` | `INTEGER` |

### MSSQL → PostgreSQL

**File:** `internal/transform/mssql_transformers.go:117-153`

| Source (MSSQL) | Destination (PostgreSQL) |
|---|---|
| `BIGINT*`, `INT*` | `BIGINT` |
| `SMALLINT*` | `SMALLINT` |
| `TINYINT*` | `SMALLINT` |
| `REAL*`, `FLOAT*` | `REAL` |
| `NUMERIC*`, `DECIMAL*` | `NUMERIC` |
| `VARCHAR*`, `NVARCHAR*` | `TEXT` |
| `CHAR*`, `NCHAR*` | `TEXT` |
| `TEXT*`, `NTEXT*` | `TEXT` |
| `VARBINARY*`, `IMAGE*` | `BYTEA` |
| `DATETIMEOFFSET*` | `TIMESTAMP WITH TIME ZONE` |
| `DATETIME2*`, `DATETIME*`, `SMALLDATETIME*` | `TIMESTAMP` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `BIT` | `BOOLEAN` |
| `UNIQUEIDENTIFIER` | `UUID` |
| `XML*` | `XML` |

### MSSQL → MySQL

**File:** `internal/transform/mssql_transformers.go:155-189`

| Source (MSSQL) | Destination (MySQL) |
|---|---|
| `BIGINT*` | `BIGINT` |
| `INT*` | `INT` |
| `SMALLINT*` | `SMALLINT` |
| `TINYINT*` | `TINYINT` |
| `REAL*`, `FLOAT*` | `DOUBLE` |
| `NUMERIC*`, `DECIMAL*` | `DECIMAL` |
| `VARCHAR*`, `NVARCHAR*` | `VARCHAR(255)` |
| `CHAR*`, `NCHAR*` | `CHAR(255)` |
| `TEXT*`, `NTEXT*` | `LONGTEXT` |
| `VARBINARY*`, `IMAGE*` | `LONGBLOB` |
| `DATETIME2*`, `DATETIME*`, `SMALLDATETIME*`, `DATETIMEOFFSET*` | `DATETIME` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `BIT` | `TINYINT(1)` |
| `UNIQUEIDENTIFIER` | `CHAR(36)` |

### MSSQL → SQLite

**File:** `internal/transform/mssql_transformers.go:191-216`

Default fallback: `TEXT` (any unmatched type maps to TEXT)

| Source (MSSQL) | Destination (SQLite) |
|---|---|
| `BIGINT*`, `INT*`, `SMALLINT*`, `TINYINT*` | `INTEGER` |
| `REAL*`, `FLOAT*` | `REAL` |
| `NUMERIC*`, `DECIMAL*` | `NUMERIC` |
| `VARCHAR*`, `NVARCHAR*`, `CHAR*`, `NCHAR*`, `TEXT*`, `NTEXT*`, `XML*` | `TEXT` |
| `VARBINARY*`, `IMAGE*` | `BLOB` |
| `DATETIME*`, `SMALLDATETIME*` | `TEXT` |
| `DATE`, `TIME` | `TEXT` |
| `BIT` | `INTEGER` |
| `UNIQUEIDENTIFIER` | `TEXT` |

### PostgreSQL → MSSQL

**File:** `internal/transform/mssql_transformers.go:218-260`

| Source (PostgreSQL) | Destination (MSSQL) |
|---|---|
| `SERIAL*`, `BIGSERIAL*` | `BIGINT IDENTITY(1,1)` |
| `SMALLSERIAL*` | `SMALLINT IDENTITY(1,1)` |
| `SMALLINT*` | `SMALLINT` |
| `INTEGER*`, `INT*` | `INT` |
| `BIGINT*` | `BIGINT` |
| `REAL*`, `DOUBLE PRECISION*` | `FLOAT` |
| `NUMERIC*`, `DECIMAL*` | `DECIMAL` |
| `CHARACTER VARYING*`, `VARCHAR*` | `NVARCHAR(255)` |
| `CHARACTER*`, `CHAR*` | `NCHAR(255)` |
| `TEXT*` | `NVARCHAR(MAX)` |
| `BYTEA*` | `VARBINARY(MAX)` |
| `TIMESTAMP WITH TIME ZONE*` | `DATETIMEOFFSET` |
| `TIMESTAMP*` | `DATETIME2` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `BOOL*` | `BIT` |
| `UUID` | `UNIQUEIDENTIFIER` |
| `JSON`, `JSONB` | `NVARCHAR(MAX)` |
| `XML` | `XML` |

### MySQL → MSSQL

**File:** `internal/transform/mssql_transformers.go:262-306`

Also used for MariaDB → MSSQL.

| Source (MySQL/MariaDB) | Destination (MSSQL) |
|---|---|
| `TINYINT*` | `TINYINT` |
| `SMALLINT*` | `SMALLINT` |
| `MEDIUMINT*` | `INT` |
| `INT*` | `INT` |
| `BIGINT*` | `BIGINT` |
| `FLOAT*` | `FLOAT` |
| `DOUBLE*` | `FLOAT` |
| `DECIMAL*`, `NUMERIC*` | `DECIMAL` |
| `VARCHAR*` | `NVARCHAR(255)` |
| `CHAR*` | `NCHAR(255)` |
| `TINYTEXT*`, `MEDIUMTEXT*`, `LONGTEXT*`, `TEXT*` | `NVARCHAR(MAX)` |
| `TINYBLOB*`, `MEDIUMBLOB*`, `LONGBLOB*`, `BLOB*` | `VARBINARY(MAX)` |
| `DATETIME*` | `DATETIME2` |
| `TIMESTAMP*` | `DATETIME2` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `BOOL*` | `BIT` |
| `JSON*` | `NVARCHAR(MAX)` |
| `ENUM*`, `SET*` | `NVARCHAR(255)` |

### SQLite → MSSQL

**File:** `internal/transform/mssql_transformers.go:308-326`

Default fallback: `NVARCHAR(MAX)` (any unmatched type maps to NVARCHAR(MAX))

| Source (SQLite) | Destination (MSSQL) |
|---|---|
| `INTEGER*`, `INT*` | `INT` |
| `BIGINT*` | `BIGINT` |
| `SMALLINT*` | `SMALLINT` |
| `REAL*`, `FLOAT*`, `DOUBLE*` | `FLOAT` |
| `NUMERIC*`, `DECIMAL*` | `DECIMAL` |
| `TEXT*`, `CHAR*`, `VARCHAR*` | `NVARCHAR(MAX)` |
| `BLOB*` | `VARBINARY(MAX)` |

### CockroachDB → PostgreSQL

**File:** `internal/transform/cockroachdb_transformers.go:128-169`

| Source (CockroachDB) | Destination (PostgreSQL) |
|---|---|
| `TIMESTAMP WITH TIME ZONE*` | `TIMESTAMPTZ` |
| `TIMESTAMP WITHOUT TIME ZONE*` | `TIMESTAMP` |
| `TIMESTAMP*` | `TIMESTAMP` |
| `CHARACTER VARYING*` | `VARCHAR` |
| `CHARACTER*` | `CHAR` |
| `DECIMAL*` | `DECIMAL` |
| `NUMERIC*` | `NUMERIC` |
| `SMALLINT` | `SMALLINT` |
| `INTEGER`, `INT` | `INTEGER` |
| `BIGINT` | `BIGINT` |
| `REAL` | `REAL` |
| `DOUBLE` | `DOUBLE PRECISION` |
| `BOOLEAN`, `BOOL` | `BOOLEAN` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `TEXT` | `TEXT` |
| `BYTEA` | `BYTEA` |
| `JSON`, `JSONB` | `JSONB` |
| `UUID` | `UUID` |

### PostgreSQL → CockroachDB

**File:** `internal/transform/cockroachdb_transformers.go:171-192`

| Source (PostgreSQL) | Destination (CockroachDB) |
|---|---|
| `BIGSERIAL*` | `BIGINT` |
| `SMALLSERIAL*` | `SMALLINT` |
| `SERIAL*` | `INT` |
| `CHARACTER VARYING*` | `STRING` |
| `CHARACTER*` | `STRING` |
| `TIMESTAMP WITH TIME ZONE*` | `TIMESTAMPTZ` |
| `TIMESTAMP WITHOUT TIME ZONE*` | `TIMESTAMP` |
| `JSONB*` | `JSONB` |

### CockroachDB → MySQL

**File:** `internal/transform/cockroachdb_transformers.go:194-232`

Also used for CockroachDB → MariaDB.

| Source (CockroachDB) | Destination (MySQL/MariaDB) |
|---|---|
| `SERIAL*`, `BIGSERIAL*` | `BIGINT` |
| `SMALLSERIAL*` | `SMALLINT` |
| `SMALLINT*` | `SMALLINT` |
| `INTEGER*`, `INT*` | `INT` |
| `BIGINT*` | `BIGINT` |
| `REAL*`, `DOUBLE PRECISION*` | `DOUBLE` |
| `NUMERIC*`, `DECIMAL*` | `DECIMAL` |
| `CHARACTER VARYING*`, `VARCHAR*` | `VARCHAR(255)` |
| `CHARACTER*`, `CHAR*` | `CHAR(255)` |
| `TEXT*` | `LONGTEXT` |
| `BYTEA*` | `LONGBLOB` |
| `TIMESTAMP*` | `DATETIME` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `BOOL*` | `TINYINT(1)` |
| `JSON`, `JSONB` | `JSON` |
| `UUID` | `CHAR(36)` |

### CockroachDB → SQLite

**File:** `internal/transform/cockroachdb_transformers.go:234-268`

Default fallback: `TEXT` (any unmatched type maps to TEXT)

| Source (CockroachDB) | Destination (SQLite) |
|---|---|
| `SERIAL*`, `BIGSERIAL*` | `INTEGER` |
| `SMALLSERIAL*` | `SMALLINT` |
| `SMALLINT*` | `SMALLINT` |
| `INTEGER*`, `INT*` | `INTEGER` |
| `BIGINT*` | `BIGINT` |
| `REAL*`, `DOUBLE PRECISION*` | `REAL` |
| `NUMERIC*`, `DECIMAL*` | `NUMERIC` |
| `CHARACTER VARYING*`, `VARCHAR*`, `CHARACTER*`, `CHAR*`, `TEXT*` | `TEXT` |
| `BYTEA*` | `BLOB` |
| `TIMESTAMP*` | `TEXT` |
| `DATE`, `TIME` | `TEXT` |
| `BOOL*` | `INTEGER` |
| `JSON`, `JSONB` | `TEXT` |
| `UUID` | `TEXT` |

### MariaDB → PostgreSQL

**File:** `internal/transform/mariadb_transformers.go:110-148`

Also used for MariaDB → CockroachDB.

| Source (MariaDB) | Destination (PostgreSQL/CockroachDB) |
|---|---|
| `TINYINT*` | `SMALLINT` |
| `SMALLINT*` | `SMALLINT` |
| `MEDIUMINT*` | `INTEGER` |
| `INT*` | `INTEGER` |
| `BIGINT*` | `BIGINT` |
| `FLOAT*` | `REAL` |
| `DOUBLE*` | `DOUBLE PRECISION` |
| `DECIMAL*`, `NUMERIC*` | `NUMERIC` |
| `VARCHAR*`, `CHAR*` | `TEXT` |
| `TINYTEXT*`, `MEDIUMTEXT*`, `LONGTEXT*`, `TEXT*` | `TEXT` |
| `TINYBLOB*`, `MEDIUMBLOB*`, `LONGBLOB*`, `BLOB*` | `BYTEA` |
| `DATETIME*`, `TIMESTAMP*` | `TIMESTAMP` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `BOOL*` | `BOOLEAN` |
| `JSON*` | `JSONB` |

### MariaDB → SQLite

**File:** `internal/transform/mariadb_transformers.go:150-179`

Default fallback: `TEXT` (any unmatched type maps to TEXT)

| Source (MariaDB) | Destination (SQLite) |
|---|---|
| `TINYINT*`, `SMALLINT*`, `MEDIUMINT*`, `INT*` | `INTEGER` |
| `BIGINT*` | `BIGINT` |
| `FLOAT*`, `DOUBLE*` | `REAL` |
| `DECIMAL*`, `NUMERIC*` | `NUMERIC` |
| `VARCHAR*`, `CHAR*`, `TINYTEXT*`, `MEDIUMTEXT*`, `LONGTEXT*`, `TEXT*`, `ENUM*`, `SET*` | `TEXT` |
| `TINYBLOB*`, `MEDIUMBLOB*`, `LONGBLOB*`, `BLOB*` | `BLOB` |
| `DATETIME*`, `TIMESTAMP*` | `TEXT` |
| `DATE`, `TIME` | `TEXT` |
| `BOOL*` | `INTEGER` |
| `JSON*` | `TEXT` |

### PostgreSQL → MariaDB

**File:** `internal/transform/mariadb_transformers.go:183-229`

| Source (PostgreSQL) | Destination (MariaDB) |
|---|---|
| `TIMESTAMP WITH TIME ZONE*` | `DATETIME` |
| `TIMESTAMP WITHOUT TIME ZONE*` | `DATETIME` |
| `TIMESTAMP*` | `DATETIME` |
| `CHARACTER VARYING*` | `VARCHAR(255)` |
| `CHARACTER*` | `CHAR(255)` |
| `DOUBLE PRECISION*` | `DOUBLE` |
| `NUMERIC*` | `DECIMAL` |
| `SMALLINT` | `SMALLINT` |
| `INTEGER` | `INT` |
| `BIGINT` | `BIGINT` |
| `REAL` | `FLOAT` |
| `NUMERIC` | `DECIMAL` |
| `DECIMAL` | `DECIMAL` |
| `CHAR` | `CHAR` |
| `VARCHAR` | `VARCHAR` |
| `TEXT` | `TEXT` |
| `BYTEA` | `BLOB` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `BOOLEAN`, `BOOL` | `TINYINT(1)` |
| `JSON`, `JSONB` | `JSON` |
| `UUID` | `CHAR(36)` |
| `SERIAL` | `INT AUTO_INCREMENT` |
| `BIGSERIAL` | `BIGINT AUTO_INCREMENT` |
| `TIMESTAMPTZ` | `DATETIME` |

## SQL to NoSQL Type Behavior

SQL-to-NoSQL migrations do not use type mapping tables because NoSQL databases are schemaless. Instead:

### SQL → MongoDB

- All SQL column values become top-level fields in a MongoDB document.
- BSON auto-typing preserves the JavaScript-native representation: integers stay integers, strings stay strings, floats stay floats.
- Nested JSON columns (`JSON`, `JSONB`) become embedded documents or arrays natively.
- No DDL schema is created on the MongoDB side (collections are created implicitly on first write).
- Column type metadata from the source is available in `MigrationUnit.Meta.ColumnTypes` but is not used for DDL.

### SQL → Redis

- All SQL column values become hash fields (string keys with string values).
- Complex values (`map[string]any`, `[]any`) are JSON-serialized to strings.
- Numeric values are stored as their string representation.
- TTL is set to 0 (no expiry) since SQL has no TTL concept.

## NoSQL to SQL Type Behavior

### MongoDB → SQL

- All document fields are typed as `TEXT` regardless of BSON type.
- Nested documents and arrays are JSON-serialized into single TEXT columns.
- `ObjectID` values are stored as strings in the `_id` column.
- This means numeric, date, boolean, and decimal data loses its SQL type information.

### Redis → SQL

- All hash fields are typed as `TEXT`.
- A `_key` column is added containing the original Redis key.
- Redis strings, lists, sets, and sorted sets produce a single `value` TEXT column.
- No type inference is performed on Redis values.

## Fallback Behavior

When a source type is not found in the mapping table:

| Destination | Fallback | Behavior |
|---|---|---|
| SQLite | `TEXT` | Always succeeds. Data is stored as text. |
| MSSQL | `NVARCHAR(MAX)` | Always succeeds. Data is stored as Unicode text. |
| PostgreSQL | `("", false)` | Warning raised. Original type used verbatim — may cause DDL error. |
| MySQL | `("", false)` | Warning raised. Original type used verbatim — may cause DDL error. |
| CockroachDB | `("", false)` | Warning raised. Original type used verbatim — may cause DDL error. |
| MariaDB | `("", false)` | Warning raised. Original type used verbatim — may cause DDL error. |
| MongoDB | N/A | No schema migration. All values pass through as-is. |
| Redis | N/A | No schema migration. All values pass through as-is. |

If you hit unmapped types:

1. Pre-create the destination table with the correct types manually, then run with `--migrate-schema=false`.
2. Use field mappings (`transform.mappings` in config) to convert column values to compatible types.
3. Implement a custom `TypeMapper` (see below).

## Lossy Conversions

Some type conversions lose information. These are detected during planning and flagged as `UnsupportedField` warnings:

| Source Type | Destination Type | What Is Lost |
|---|---|---|
| `TIMESTAMPTZ` | `TIMESTAMP` | Timezone information |
| `TIMESTAMP` | `DATE` | Time-of-day component |
| `JSONB` | `JSON` | Binary JSON indexing and query optimizations |
| `UUID` | `CHAR(36)` | Native UUID type safety and operations |
| `BOOLEAN` | `TINYINT(1)` | Semantic boolean type |
| `SERIAL` | `INT AUTO_INCREMENT` | Sequence object is not created |
| `ENUM` | `VARCHAR(255)` | Value constraint is lost |
| `SET` | `VARCHAR(255)` | Multi-value constraint is lost |
| `DOUBLE PRECISION` | `FLOAT` | Floating-point precision |
| `DECIMAL` | `FLOAT` | Exact-to-approximate precision |
| `TEXT` | `VARCHAR` | Potential truncation if length is insufficient |
| `BYTEA` | `BLOB` | Encoding/access pattern differences |
| Any timestamp type | `TEXT` (SQLite) | Date/time type semantics |
| Any numeric type | `TEXT` (NoSQL→SQL) | Numeric operations and constraints |

## Custom Type Mapper

You can implement a custom type mapper by creating a transformer that implements the `TypeMapperProvider` interface:

```go
// internal/transform/transform.go
type TypeMapperProvider interface {
    Transformer
    TypeMapper() provider.TypeMapper
}
```

### Example: Override mapping for a custom type

```go
package transform

import "github.com/pageton/bridge-db/pkg/provider"

type myCustomTransformer struct {
    NoopTransformer
}

// TypeMapper provides custom type mapping
func (t *myCustomTransformer) TypeMapper() provider.TypeMapper {
    return &myCustomTypeMapper{}
}

type myCustomTypeMapper struct{}

func (m *myCustomTypeMapper) MapType(colType string) (string, bool) {
    switch {
    case strings.HasPrefix(colType, "MY_CUSTOM_TYPE"):
        return "TEXT", true
    case strings.HasPrefix(colType, "MONEY"):
        return "NUMERIC(19,4)", true  // precise decimal for currency
    default:
        return "", false  // fall through to default mapping
    }
}

func (t *myCustomTransformer) Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
    // Your transformation logic here
    return units, nil
}

func (t *myCustomTransformer) NeedsSchema() bool { return false }
func (t *myCustomTransformer) SetSchema(schema *provider.Schema) {}

func init() {
    RegisterTransformer("mysql", "postgres", func() Transformer {
        return &myCustomTransformer{}
    })
}
```

### Field-level value conversion

For field-level type coercion without creating a custom transformer, use field mappings in the config:

```yaml
transform:
  mappings:
    - table: products
      source: price
      destination: price
      action: convert
      convert: "float"
    - table: users
      source: is_active
      destination: active
      action: convert
      convert: "bool"
    - table: orders
      source: created_at
      destination: created_at
      action: convert
      convert: "timestamp:mysql:postgres"
```

Supported convert types: `string`, `int`, `float`, `bool`, `timestamp:src_dialect:dst_dialect`.

## Files Involved

| File | Role |
|---|---|
| `pkg/provider/provider.go` | `TypeMapper` interface definition |
| `internal/transform/transform.go` | `Transformer`, `TypeMapperProvider`, `ConfigurableTransformer` interfaces |
| `internal/transform/registry.go` | Transformer registration and lookup |
| `internal/transform/sql_transformers.go` | MySQL↔PostgreSQL type mappers |
| `internal/transform/sqlite_transformers.go` | SQLite↔PostgreSQL, SQLite↔MySQL type mappers |
| `internal/transform/mssql_transformers.go` | MSSQL↔PostgreSQL, MSSQL↔MySQL, MSSQL↔SQLite type mappers |
| `internal/transform/cockroachdb_transformers.go` | CockroachDB↔PostgreSQL, CockroachDB↔MySQL, CockroachDB↔SQLite type mappers |
| `internal/transform/mariadb_transformers.go` | MariaDB↔PostgreSQL, MariaDB↔SQLite type mappers |
| `internal/transform/sql_to_nosql.go` | SQL→MongoDB, SQL→Redis conversion functions |
| `internal/transform/nosql_to_sql.go` | MongoDB→SQL, Redis→SQL conversion functions |
| `internal/bridge/plan.go` | `planTypeMappings` function, lossy conversion detection |
