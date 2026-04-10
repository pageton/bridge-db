package bridge

import (
	"errors"
	"fmt"
	"time"

	"github.com/pageton/bridge-db/internal/retry"
)

// ErrorCategory classifies migration errors for user-facing messaging and retry decisions.
type ErrorCategory string

const (
	// ErrConfig covers validation errors in pipeline options, checkpoint config, etc.
	ErrConfig ErrorCategory = "config"

	// ErrConnection covers network/auth failures connecting to source or destination.
	ErrConnection ErrorCategory = "connection"

	// ErrSchema covers DDL and schema migration failures.
	ErrSchema ErrorCategory = "schema"

	// ErrScan covers failures reading data from the source.
	ErrScan ErrorCategory = "scan"

	// ErrTransform covers data transformation/conversion failures.
	ErrTransform ErrorCategory = "transform"

	// ErrWrite covers failures writing data to the destination.
	ErrWrite ErrorCategory = "write"

	// ErrVerify covers post-migration verification failures.
	ErrVerify ErrorCategory = "verify"

	// ErrCancelled covers context cancellation (user interrupt, timeout).
	ErrCancelled ErrorCategory = "cancelled"

	// ErrInternal covers unexpected pipeline bugs.
	ErrInternal ErrorCategory = "internal"
)

// RetryPolicy describes the retry strategy for an error category.
type RetryPolicy struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

// RetryConfig converts a RetryPolicy into a retry.Config.
func (r RetryPolicy) RetryConfig() retry.Config {
	return retry.Config{
		MaxAttempts:     r.MaxAttempts,
		InitialInterval: r.InitialInterval,
		MaxInterval:     r.MaxInterval,
		Multiplier:      r.Multiplier,
	}
}

// retryPolicies defines per-category retry strategies.
var retryPolicies = map[ErrorCategory]RetryPolicy{
	ErrConfig:     {MaxAttempts: 1, InitialInterval: 0, MaxInterval: 0, Multiplier: 1.0}, // no retry — bad config
	ErrConnection: {MaxAttempts: 3, InitialInterval: 1 * time.Second, MaxInterval: 10 * time.Second, Multiplier: 2.0},
	ErrScan:       {MaxAttempts: 3, InitialInterval: 500 * time.Millisecond, MaxInterval: 10 * time.Second, Multiplier: 2.0},
	ErrTransform:  {MaxAttempts: 1, InitialInterval: 0, MaxInterval: 0, Multiplier: 1.0}, // no retry — bad data
	ErrWrite:      {MaxAttempts: 5, InitialInterval: 500 * time.Millisecond, MaxInterval: 30 * time.Second, Multiplier: 2.0},
	ErrSchema:     {MaxAttempts: 2, InitialInterval: 1 * time.Second, MaxInterval: 5 * time.Second, Multiplier: 2.0},
	ErrVerify:     {MaxAttempts: 1, InitialInterval: 0, MaxInterval: 0, Multiplier: 1.0}, // no retry
	ErrCancelled:  {MaxAttempts: 0, InitialInterval: 0, MaxInterval: 0, Multiplier: 1.0}, // not retryable
	ErrInternal:   {MaxAttempts: 1, InitialInterval: 0, MaxInterval: 0, Multiplier: 1.0}, // no retry
}

// RetryPolicyFor returns the configured retry policy for a category.
func RetryPolicyFor(cat ErrorCategory) RetryPolicy {
	if p, ok := retryPolicies[cat]; ok {
		return p
	}
	return RetryPolicy{MaxAttempts: 1}
}

// CategorizedError wraps an error with a category, pipeline step, and user-facing message.
type CategorizedError struct {
	Category  ErrorCategory
	Step      int    // pipeline step 1-10, 0 when not tied to a specific step
	Phase     string // sub-phase within the step (e.g. "connect", "tunnel")
	Message   string // user-facing
	Cause     error  // underlying error
	Retryable bool
}

func (e *CategorizedError) Error() string {
	prefix := fmt.Sprintf("[%s]", e.Category)
	if e.Step > 0 {
		prefix += fmt.Sprintf(" step %d", e.Step)
	}
	if e.Phase != "" {
		prefix += fmt.Sprintf(" (%s)", e.Phase)
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s %s: %v", prefix, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s %s", prefix, e.Message)
}

func (e *CategorizedError) Unwrap() error { return e.Cause }

// WithStep returns the same error with the pipeline step set. Fluent API for
// call-site clarity: NewConnectionError(...).WithStep(3).
func (e *CategorizedError) WithStep(step int) *CategorizedError {
	e.Step = step
	return e
}

// Categorize attempts to extract the ErrorCategory from an error.
// Returns ErrInternal if the error is not a CategorizedError.
func Categorize(err error) ErrorCategory {
	var ce *CategorizedError
	if errors.As(err, &ce) {
		return ce.Category
	}
	return ErrInternal
}

// IsRetryable returns whether the error should be retried.
func IsRetryable(err error) bool {
	var ce *CategorizedError
	if errors.As(err, &ce) {
		return ce.Retryable
	}
	return false
}

// StepOf returns the pipeline step number from a CategorizedError, or 0.
func StepOf(err error) int {
	var ce *CategorizedError
	if errors.As(err, &ce) {
		return ce.Step
	}
	return 0
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

// NewConfigError wraps a configuration or validation failure.
func NewConfigError(step int, message string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:  ErrConfig,
		Step:      step,
		Phase:     "config",
		Message:   message,
		Cause:     cause,
		Retryable: false,
	}
}

// NewConnectionError wraps a connection failure.
func NewConnectionError(phase, message string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:  ErrConnection,
		Phase:     phase,
		Message:   message,
		Cause:     cause,
		Retryable: true,
	}
}

// NewSchemaError wraps a schema migration failure.
func NewSchemaError(message string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:  ErrSchema,
		Phase:     "schema_migration",
		Message:   message,
		Cause:     cause,
		Retryable: true,
	}
}

// NewScanError wraps a scan failure.
func NewScanError(message string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:  ErrScan,
		Phase:     "scan",
		Message:   message,
		Cause:     cause,
		Retryable: true,
	}
}

// NewTransformError wraps a transform failure.
func NewTransformError(batchID int, cause error) *CategorizedError {
	return &CategorizedError{
		Category:  ErrTransform,
		Phase:     "transform",
		Message:   fmt.Sprintf("batch %d: transform failed", batchID),
		Cause:     cause,
		Retryable: false,
	}
}

// NewWriteError wraps a write failure.
func NewWriteError(batchID int, cause error) *CategorizedError {
	return &CategorizedError{
		Category:  ErrWrite,
		Phase:     "write",
		Message:   fmt.Sprintf("batch %d: write failed", batchID),
		Cause:     cause,
		Retryable: true,
	}
}

// NewWriteErrorExhausted wraps a write failure after all retries have been
// exhausted. The Retryable flag is set to false.
func NewWriteErrorExhausted(batchID int, cause error) *CategorizedError {
	return &CategorizedError{
		Category:  ErrWrite,
		Phase:     "write",
		Message:   fmt.Sprintf("batch %d: write failed after retries", batchID),
		Cause:     cause,
		Retryable: false,
	}
}

// NewVerifyError wraps a verification failure.
func NewVerifyError(message string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:  ErrVerify,
		Phase:     "verify",
		Message:   message,
		Cause:     cause,
		Retryable: false,
	}
}

// NewCancelledError wraps a context cancellation.
func NewCancelledError(message string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:  ErrCancelled,
		Phase:     "cancelled",
		Message:   message,
		Cause:     cause,
		Retryable: false,
	}
}

// ---------------------------------------------------------------------------
// FailureSummary
// ---------------------------------------------------------------------------

// FailureSummary aggregates categorized errors for reporting.
type FailureSummary struct {
	Counts   map[ErrorCategory]int
	Examples map[ErrorCategory]*CategorizedError
	Total    int
}

// NewFailureSummary creates an empty summary.
func NewFailureSummary() *FailureSummary {
	return &FailureSummary{
		Counts:   make(map[ErrorCategory]int),
		Examples: make(map[ErrorCategory]*CategorizedError),
	}
}

// Record adds an error to the summary.
func (fs *FailureSummary) Record(err error) {
	fs.Total++
	cat := Categorize(err)
	fs.Counts[cat]++
	if _, ok := fs.Examples[cat]; !ok {
		var ce *CategorizedError
		if errors.As(err, &ce) {
			fs.Examples[cat] = ce
		}
	}
}

// UserMessage returns a clear, actionable message for a category.
func (c ErrorCategory) UserMessage() string {
	switch c {
	case ErrConfig:
		return "Configuration is invalid. Check command flags, config file, and checkpoint compatibility."
	case ErrConnection:
		return "Could not connect to the database. Check that the host, port, credentials, and network are correct."
	case ErrSchema:
		return "Schema migration failed. The destination database may have incompatible types or constraints."
	case ErrScan:
		return "Failed to read data from the source database. The source may be unreachable or the query may be invalid."
	case ErrTransform:
		return "Data transformation failed. A value could not be converted between source and destination formats."
	case ErrWrite:
		return "Failed to write data to the destination. The destination may be rejecting rows due to constraints or type mismatches."
	case ErrVerify:
		return "Post-migration verification found differences between source and destination."
	case ErrCancelled:
		return "Migration was cancelled. Use --resume to continue from the last checkpoint."
	case ErrInternal:
		return "An unexpected error occurred in the migration pipeline."
	default:
		return "An unknown error occurred."
	}
}
