package verify

import (
	"fmt"
)

// Options controls how cross-verification is performed.
type Options struct {
	// CountComparison enables source vs destination count checks per table.
	CountComparison bool

	// SampleMode controls sampling behavior:
	//   "none"  -- skip sampling
	//   "count" -- sample exactly SampleN records per table
	//   "pct"   -- sample SamplePct percent of records per table
	SampleMode string

	// SampleN is the fixed number of records to sample per table (when SampleMode="count").
	SampleN int

	// SamplePct is the percentage of records to sample per table (when SampleMode="pct").
	SamplePct float64

	// ChecksumComparison enables row-level hash comparison.
	ChecksumComparison bool

	// Seed is the deterministic sampling seed for reproducibility.
	Seed int64

	// MaxSampleKeys caps the total number of keys sampled across all tables.
	// 0 means no cap.
	MaxSampleKeys int
}

// DefaultOptions returns sensible verification defaults.
func DefaultOptions() Options {
	return Options{
		CountComparison:    true,
		SampleMode:         "pct",
		SamplePct:          5.0,
		SampleN:            100,
		ChecksumComparison: true,
		Seed:               42,
		MaxSampleKeys:      10000,
	}
}

// Validate checks the options for errors.
func (o Options) Validate() error {
	switch o.SampleMode {
	case "none", "count", "pct":
	default:
		return fmt.Errorf("invalid sample_mode %q (must be none, count, or pct)", o.SampleMode)
	}
	if o.SampleMode == "count" && o.SampleN <= 0 {
		return fmt.Errorf("sample_n must be positive when sample_mode is count, got %d", o.SampleN)
	}
	if o.SampleMode == "pct" && (o.SamplePct <= 0 || o.SamplePct > 100) {
		return fmt.Errorf("sample_pct must be in (0, 100], got %f", o.SamplePct)
	}
	return nil
}

// CountsOnly returns options that only perform count comparison.
func CountsOnly() Options {
	return Options{
		CountComparison:    true,
		SampleMode:         "none",
		ChecksumComparison: false,
		Seed:               42,
	}
}
