package bridge

import (
	"time"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/pkg/provider"
)

// RunResult wraps a MigrationSummary with additional context about the run.
type RunResult struct {
	// Summary is the final migration summary.
	Summary *provider.MigrationSummary

	// Config is the configuration used for this run.
	Config *config.MigrationConfig

	// SrcProvider is the source provider name.
	SrcProvider string

	// DstProvider is the destination provider name.
	DstProvider string

	// Phases holds timing and status for each pipeline phase.
	Phases []PhaseResult

	// CheckpointPath is the path where checkpoints were stored.
	CheckpointPath string

	// Resumed is true if this run resumed from a previous checkpoint.
	Resumed bool
}

// PhaseResult records timing and outcome for a single pipeline phase.
type PhaseResult struct {
	// Phase identifies the pipeline stage.
	Phase provider.MigrationPhase

	// Duration is how long this phase took.
	Duration time.Duration

	// Error is non-nil if the phase failed.
	Error error
}

// Elapsed returns the total duration across all phases.
func (r *RunResult) Elapsed() time.Duration {
	var total time.Duration
	for _, p := range r.Phases {
		total += p.Duration
	}
	return total
}
