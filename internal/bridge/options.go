// Package bridge implements the core pipeline engine that orchestrates
// database migrations from source to destination.
package bridge

import (
	"fmt"
	"time"

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

	// Parallel controls how many batches can be in-flight between
	// the scanner and writer goroutines. Higher values allow more
	// pipelining but use more memory. Default: 4.
	Parallel int

	// CheckpointEnabled controls whether checkpoints are saved during migration.
	CheckpointEnabled bool

	// Resume indicates whether to resume from a previous checkpoint.
	Resume bool
}

// DefaultPipelineOptions returns a PipelineOptions with sensible defaults.
func DefaultPipelineOptions() PipelineOptions {
	return PipelineOptions{
		BatchSize:        1000,
		Verify:           true,
		ConflictStrategy: provider.ConflictOverwrite,
		MigrateSchema:    true,
		FKHandling:       "defer_constraints",
		MaxRetries:       3,
		RetryBackoff:     500 * time.Millisecond,
		Parallel:         4,
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
	if o.Parallel < 1 {
		return fmt.Errorf("parallel must be at least 1, got %d", o.Parallel)
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
	return nil
}
