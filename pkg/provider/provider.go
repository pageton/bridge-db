// Package provider defines the core interfaces for database migration providers.
// Every supported database (Redis, MongoDB, PostgreSQL, MySQL) implements these
// interfaces so the pipeline engine can orchestrate migrations generically.
package provider

import (
	"context"
	"time"
)

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

// DataType identifies the kind of data a MigrationUnit carries.
// Each provider uses a subset appropriate to its model.
type DataType string

const (
	// Redis data types.
	DataTypeString DataType = "string"
	DataTypeHash   DataType = "hash"
	DataTypeList   DataType = "list"
	DataTypeSet    DataType = "set"
	DataTypeZSet   DataType = "zset"
	DataTypeStream DataType = "stream"

	// MongoDB data type.
	DataTypeDocument DataType = "document"

	// SQL data type.
	DataTypeRow DataType = "row"
)

// ---------------------------------------------------------------------------
// Migration unit
// ---------------------------------------------------------------------------

// MigrationUnit is the fundamental unit of data moved through the pipeline.
// For Redis it represents a single key (with its type, value and TTL).
// For MongoDB it represents a single document.
// For SQL databases it represents a single row.
type MigrationUnit struct {
	// Key is the unique identifier: Redis key, document _id, or composite
	// "table:PK" for SQL rows.
	Key string

	// Table holds the collection or table name (empty for Redis).
	Table string

	// DataType classifies the unit so the writer knows how to serialize it.
	DataType DataType

	// Data holds the serialised value. Encoding depends on DataType:
	//   Redis    – JSON envelope with type-specific fields
	//   MongoDB  – raw BSON bytes
	//   SQL      – JSON map of column→value
	Data []byte

	// Metadata carries optional provider-specific information (e.g. Redis TTL,
	// MongoDB _id type, SQL column types).
	Metadata map[string]any

	// Size is the approximate byte size of Data (for metrics and batching).
	Size int64
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

// BatchResult summarises the outcome of writing a batch of units.
type BatchResult struct {
	BatchID      int
	TotalUnits   int
	WrittenUnits int
	SkippedUnits int
	FailedUnits  int
	FailedKeys   []string
	BytesWritten int64
	Duration     time.Duration
	Errors       []error
}

// VerificationError describes a mismatch found during post-migration verification.
type VerificationError struct {
	Key     string
	Table   string
	Type    DataType
	Message string
}

// ScanStats reports progress of the scanning phase.
type ScanStats struct {
	TotalScanned int64
	TotalBytes   int64
	TablesDone   int
	TablesTotal  int
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// ScanOptions configures how a Scanner enumerates data from the source.
type ScanOptions struct {
	// Pattern limits the scan (e.g. Redis MATCH pattern, SQL WHERE clause).
	Pattern string

	// BatchSize controls how many units are returned per Next() call.
	BatchSize int

	// ResumeToken is an opaque cursor for resuming a previous scan.
	ResumeToken []byte

	// TablesCompleted lists tables/collections already fully migrated.
	// Scanners should skip these when resuming.
	TablesCompleted []string
}

// WriteOptions configures how a Writer persists data to the destination.
type WriteOptions struct {
	// OnConflict controls behaviour when a key/row already exists.
	OnConflict ConflictStrategy

	// BatchSize controls how many units are flushed per Write() call.
	BatchSize int
}

// ConflictStrategy enumerates ways to handle existing destination data.
type ConflictStrategy string

const (
	ConflictOverwrite ConflictStrategy = "overwrite"
	ConflictSkip      ConflictStrategy = "skip"
	ConflictError     ConflictStrategy = "error"
)

// ---------------------------------------------------------------------------
// Migration lifecycle
// ---------------------------------------------------------------------------

// MigrationPhase identifies the current stage of the pipeline.
type MigrationPhase string

const (
	PhaseInit            MigrationPhase = "init"
	PhaseTunnel          MigrationPhase = "tunnel"
	PhaseConnect         MigrationPhase = "connecting"
	PhaseSchemaMigration MigrationPhase = "schema_migration"
	PhaseScanning        MigrationPhase = "scanning"
	PhaseTransforming    MigrationPhase = "transforming"
	PhaseWriting         MigrationPhase = "writing"
	PhaseVerifying       MigrationPhase = "verifying"
	PhaseComplete        MigrationPhase = "complete"
	PhaseError           MigrationPhase = "error"
	PhasePaused          MigrationPhase = "paused"
)

// ProgressStats carries real-time progress information for the frontend.
type ProgressStats struct {
	Phase            MigrationPhase
	TotalScanned     int64
	TotalWritten     int64
	TotalFailed      int64
	TotalSkipped     int64
	Throughput       float64 // units per second
	Elapsed          time.Duration
	EstimatedRemain  time.Duration
	BytesTransferred int64
	CurrentBatchID   int
	TablesCompleted  int
	TablesTotal      int
	ErrorCount       int
}

// MigrationSummary is the final report after a migration completes.
type MigrationSummary struct {
	StartTime        time.Time
	EndTime          time.Time
	Duration         time.Duration
	TotalScanned     int64
	TotalWritten     int64
	TotalFailed      int64
	TotalSkipped     int64
	BytesTransferred int64
	TablesCompleted  []string
	VerificationOK   bool
	VerificationErrs []VerificationError
	Errors           []error
}

// ---------------------------------------------------------------------------
// Core interfaces
// ---------------------------------------------------------------------------

// Provider is the top-level interface every database adapter implements.
// It acts as a factory for Scanner, Writer, and Verifier instances.
type Provider interface {
	// Name returns the provider identifier (e.g. "redis", "mongodb").
	Name() string

	// Connect establishes connections to the source and destination databases.
	Connect(ctx context.Context, srcConfig, dstConfig any) error

	// Close releases all resources held by the provider.
	Close() error

	// Ping verifies that both connections are alive.
	Ping(ctx context.Context) error

	// Scanner returns a Scanner that enumerates MigrationUnits from the source.
	Scanner(ctx context.Context, opts ScanOptions) Scanner

	// Writer returns a Writer that persists MigrationUnits to the destination.
	Writer(ctx context.Context, opts WriteOptions) Writer

	// Verifier returns a Verifier for post-migration checks.
	Verifier(ctx context.Context) Verifier

	// SchemaMigrator returns a SchemaMigrator (nil for providers that don't
	// support schema migration, e.g. Redis).
	SchemaMigrator(ctx context.Context) SchemaMigrator

	// DryRun returns a wrapped Provider that logs writes without executing them.
	DryRun() Provider
}

// Scanner enumerates data from the source in batches.
type Scanner interface {
	// Next returns the next batch of MigrationUnits. It returns io.EOF when
	// there is no more data.
	Next(ctx context.Context) ([]MigrationUnit, error)

	// Stats returns current scan statistics.
	Stats() ScanStats
}

// Writer persists batches of MigrationUnits to the destination.
type Writer interface {
	// Write persists a batch of units and returns a summary.
	Write(ctx context.Context, units []MigrationUnit) (*BatchResult, error)

	// Flush forces any buffered writes to be committed.
	Flush(ctx context.Context) error
}

// Verifier checks that destination data matches source data.
type Verifier interface {
	// Verify checks the given keys/rows and returns any mismatches.
	Verify(ctx context.Context, keys []string) ([]VerificationError, error)
}

// SchemaMigrator handles DDL migration for SQL databases.
type SchemaMigrator interface {
	// Inspect reads the schema from the source database.
	Inspect(ctx context.Context) (*Schema, error)

	// Create applies the given schema to the destination database.
	Create(ctx context.Context, schema *Schema, mapper TypeMapper) error
}

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

// Schema represents a database schema (tables, columns, indexes).
type Schema struct {
	Tables []TableSchema
}

// TableSchema describes a single table.
type TableSchema struct {
	Name    string
	Columns []ColumnSchema
	Indexes []IndexSchema
}

// ColumnSchema describes a single column.
type ColumnSchema struct {
	Name     string
	Type     string
	Nullable bool
	Default  string
	AutoInc  bool
}

// IndexSchema describes a single index.
type IndexSchema struct {
	Name    string
	Columns []string
	Unique  bool
	Primary bool
}

// TypeMapper converts a column type from one database dialect to another.
type TypeMapper interface {
	MapType(colType string) (string, bool)
}

// ---------------------------------------------------------------------------
// Progress reporting
// ---------------------------------------------------------------------------

// ProgressReporter decouples the pipeline from the frontend layer.
// Both the TUI and CLI implement this interface.
type ProgressReporter interface {
	OnBatchStart(batchID int, unitCount int)
	OnBatchComplete(batchID int, result *BatchResult)
	OnMigrationComplete(summary *MigrationSummary)
	OnError(err error, unit *MigrationUnit)
	OnPhaseChange(phase MigrationPhase)
	OnProgress(stats ProgressStats)
}
