// Package bridge implements the core pipeline engine that orchestrates
// database migrations from source to destination.
package bridge

import (
	"fmt"
	"time"

	"github.com/pageton/bridge-db/internal/verify"
	"github.com/pageton/bridge-db/pkg/provider"
)

// PipelineOptions controls migration execution behaviour.
type PipelineOptions struct {
	// BatchSize is the number of migration units per batch.
	BatchSize int

	// DryRun logs writes without executing them.
	DryRun bool

	// Verify enables post-migration data verification.
	Verify bool

	// ConflictStrategy controls behaviour when destination data already exists.
	ConflictStrategy provider.ConflictStrategy

	// MigrateSchema enables DDL migration for SQL databases.
	MigrateSchema bool

	// FKHandling controls foreign key constraint handling during migration.
	// Valid values: "defer_constraints", "ordered", "skip".
	FKHandling string

	// MaxRetries is the maximum number of write retries per batch.
	MaxRetries int

	// RetryBackoff is the initial delay between retries.
	RetryBackoff time.Duration

	// MaxPerUnitRetry limits how many units from a partially-failed batch
	// are retried individually. 0 means no per-unit retry.
	// Default: min(BatchSize, 100).
	MaxPerUnitRetry int

	// Parallel controls the scan/write buffer depth between the scanner
	// and writer goroutines. Higher values allow more pipelining but use
	// more memory. Default: 4.
	Parallel int

	// WriteWorkers is the number of concurrent writer goroutines that
	// consume from the scan channel. Default: 1 (sequential, backward-
	// compatible). Set higher to parallelise writes when the destination
	// can handle concurrent inserts.
	WriteWorkers int

	// MaxBatchBytes is a soft byte budget per batch. When the cumulative
	// size of units in a batch exceeds this limit, the batch is shipped
	// even if it has fewer than BatchSize units. 0 means unlimited
	// (only BatchSize controls the cutoff). Default: 32 MB.
	MaxBatchBytes int64

	// MaxWrittenKeys caps the number of written keys retained in memory
	// for checkpoint deduplication on resume. Default: 100_000.
	MaxWrittenKeys int

	// CheckpointInterval controls how often checkpoints are persisted.
	// 0 means after every batch (original behaviour, safest but slowest
	// for high-throughput migrations). Default: 0.
	CheckpointInterval int

	// CheckpointEnabled controls whether checkpoints are saved during migration.
	CheckpointEnabled bool

	// Resume indicates whether to resume from a previous checkpoint.
	Resume bool

	// VerifyMode controls verification depth.
	// "basic" -- destination-only count check (legacy)
	// "cross" -- full source vs destination comparison (default)
	VerifyMode string

	// FailFast controls whether the pipeline aborts on the first transform
	// error instead of skipping the batch and continuing. Default: false
	// (skip batches, continue migration).
	FailFast bool

	// VerifyOptions controls verification depth. When zero-valued,
	// DefaultOptions() is used.
	VerifyOptions verify.Options
}

// DefaultPipelineOptions returns a PipelineOptions with sensible defaults.
func DefaultPipelineOptions() PipelineOptions {
	return PipelineOptions{
		BatchSize:          1000,
		Verify:             true,
		ConflictStrategy:   provider.ConflictOverwrite,
		MigrateSchema:      true,
		FKHandling:         "defer_constraints",
		MaxRetries:         3,
		RetryBackoff:       500 * time.Millisecond,
		MaxPerUnitRetry:    0, // resolved to min(BatchSize, 100) by Validate
		Parallel:           4,
		WriteWorkers:       1,
		MaxBatchBytes:      32 * 1024 * 1024, // 32 MB
		MaxWrittenKeys:     100_000,
		CheckpointInterval: 0, // every batch (safest)
		VerifyMode:         "cross",
	}
}

// Validate checks the pipeline options for errors.
func (o PipelineOptions) Validate() error {
	if o.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be positive, got %d", o.BatchSize)
	}
	if o.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be non-negative, got %d", o.MaxRetries)
	}
	if o.MaxPerUnitRetry < 0 {
		return fmt.Errorf("max_per_unit_retry must be non-negative, got %d", o.MaxPerUnitRetry)
	}
	if o.Parallel < 1 {
		return fmt.Errorf("parallel must be at least 1, got %d", o.Parallel)
	}
	if o.WriteWorkers < 1 {
		return fmt.Errorf("write_workers must be at least 1, got %d", o.WriteWorkers)
	}
	if o.MaxBatchBytes < 0 {
		return fmt.Errorf("max_batch_bytes must be non-negative, got %d", o.MaxBatchBytes)
	}
	if o.MaxWrittenKeys < 1 {
		return fmt.Errorf("max_written_keys must be at least 1, got %d", o.MaxWrittenKeys)
	}
	if o.CheckpointInterval < 0 {
		return fmt.Errorf("checkpoint_interval must be non-negative, got %d", o.CheckpointInterval)
	}
	switch o.ConflictStrategy {
	case provider.ConflictOverwrite, provider.ConflictSkip, provider.ConflictError:
		// valid
	default:
		return fmt.Errorf("invalid conflict_strategy %q", o.ConflictStrategy)
	}
	switch o.FKHandling {
	case "defer_constraints", "ordered", "skip":
		// valid
	default:
		return fmt.Errorf("invalid fk_handling %q (must be defer_constraints, ordered, or skip)", o.FKHandling)
	}
	switch o.VerifyMode {
	case "basic", "cross":
		// valid
	default:
		return fmt.Errorf("invalid verify_mode %q (must be basic or cross)", o.VerifyMode)
	}
	return nil
}
