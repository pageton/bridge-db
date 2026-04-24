package bridge

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/internal/retry"
	"github.com/pageton/bridge-db/pkg/provider"
)

// batchWriter wraps a provider.Writer and adds dedup filtering, partial failure
// handling, per-unit retry, and correct metrics accounting. Each writer goroutine
// gets its own batchWriter instance.
type batchWriter struct {
	w       provider.Writer
	batchID int
	cfg     writeConfig
}

// writeConfig controls batchWriter behaviour.
type writeConfig struct {
	// MaxRetries is the number of retries beyond the initial attempt.
	MaxRetries int

	// RetryBackoff is the initial delay between retries.
	RetryBackoff time.Duration

	// ConflictStrategy controls how duplicate keys are handled.
	ConflictStrategy provider.ConflictStrategy

	// MaxPerUnitRetry limits how many units from a partially-failed batch
	// are retried individually. 0 means no per-unit retry.
	MaxPerUnitRetry int
}

// writeOutcome captures what happened for a single batch write.
type writeOutcome struct {
	result     *provider.BatchResult
	writeErr   error       // non-nil if Write() itself failed
	unitErrors []unitError // per-unit errors from partial failure fallback
	skipped    int         // units skipped by dedup
}

// unitError records a per-unit failure after individual retry.
type unitError struct {
	key string
	err error
}

// writeBatch executes one batch write with the following stages:
//
//  1. Dedup — filter out keys already written in this run.
//  2. Batch write — call w.Write() with retry.
//  3. Partial failure recovery — if Write() succeeded but reported
//     FailedKeys, retry those units individually.
//  4. Metrics — record written/failed/skipped counts from actual results.
func (bw *batchWriter) writeBatch(
	ctx context.Context,
	pipeline *Pipeline,
	units []provider.MigrationUnit,
) writeOutcome {
	out := writeOutcome{}

	// Stage 1: Dedup — filter already-written keys.
	deduped, skipped := pipeline.filterWritten(units)
	out.skipped = skipped
	if len(deduped) == 0 {
		out.result = &provider.BatchResult{
			TotalUnits:   len(units),
			SkippedUnits: skipped,
		}
		return out
	}

	// Stage 2: Batch write with retry.
	var result *provider.BatchResult
	retryCfg := retry.Config{
		MaxAttempts:     bw.cfg.MaxRetries + 1,
		InitialInterval: bw.cfg.RetryBackoff,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
	}
	writeErr := retry.Do(ctx, retryCfg, func() error {
		var werr error
		result, werr = bw.w.Write(ctx, deduped)
		return werr
	})

	if writeErr != nil {
		out.writeErr = writeErr
		return out
	}

	// Stage 3: Partial failure recovery — retry failed units individually.
	if result != nil && len(result.FailedKeys) > 0 && bw.cfg.MaxPerUnitRetry > 0 {
		failedUnits := unitsByKeys(deduped, result.FailedKeys)
		if len(failedUnits) > 0 {
			bw.retryIndividual(ctx, failedUnits, result, &out)
		}
	}

	// Stage 4: Adjust result to account for dedup'd units.
	if result != nil {
		result.TotalUnits = len(units)
		result.SkippedUnits += skipped
	}
	out.result = result
	return out
}

// retryIndividual retries failed units one at a time, updating the batch result.
func (bw *batchWriter) retryIndividual(
	ctx context.Context,
	units []provider.MigrationUnit,
	batchResult *provider.BatchResult,
	out *writeOutcome,
) {
	log := logger.L().With("component", "batch-writer", "batch", bw.batchID)
	recovered := 0
	attempted := 0
	maxUnits := bw.cfg.MaxPerUnitRetry
	if maxUnits <= 0 {
		return
	}

	for _, unit := range units {
		if ctx.Err() != nil {
			break
		}
		if attempted >= maxUnits {
			key := unit.Key
			if key == "" {
				key = fmt.Sprintf("batch_%d_unit", bw.batchID)
			}
			out.unitErrors = append(out.unitErrors, unitError{
				key: key,
				err: fmt.Errorf("individual retry skipped after reaching limit (%d units)", maxUnits),
			})
			continue
		}
		attempted++

		singleResult, err := bw.writeSingleWithRetry(ctx, unit, log)
		if err != nil || !isSuccessfulSingleWrite(singleResult) {
			key := unit.Key
			if key == "" {
				key = fmt.Sprintf("batch_%d_unit", bw.batchID)
			}
			unitErr := err
			if unitErr == nil {
				unitErr = fmt.Errorf("write did not report success")
			}
			out.unitErrors = append(out.unitErrors, unitError{
				key: key,
				err: unitErr,
			})
			continue
		}

		recovered++
		if singleResult.WrittenUnits > 0 {
			batchResult.WrittenUnits += singleResult.WrittenUnits
		} else {
			batchResult.SkippedUnits += max(singleResult.SkippedUnits, 1)
		}
		batchResult.FailedUnits--
		batchResult.BytesWritten += singleResult.BytesWritten

		// Remove from failed keys list.
		for i, fk := range batchResult.FailedKeys {
			if fk == unit.Key {
				batchResult.FailedKeys = append(batchResult.FailedKeys[:i], batchResult.FailedKeys[i+1:]...)
				break
			}
		}
	}

	if recovered > 0 {
		log.Debug("recovered failed units via individual retry",
			"recovered", recovered,
			"attempted", attempted,
		)
	}
	if len(units) > attempted {
		log.Warn("skipped individual retries beyond configured limit",
			"failed_units", len(units),
			"retried_units", attempted,
			"retry_unit_limit", maxUnits,
		)
	}
}

func (bw *batchWriter) writeSingleWithRetry(
	ctx context.Context,
	unit provider.MigrationUnit,
	log interface {
		Debug(string, ...any)
		Warn(string, ...any)
	},
) (*provider.BatchResult, error) {
	var result *provider.BatchResult
	key := unit.Key
	if key == "" {
		key = fmt.Sprintf("batch_%d_unit", bw.batchID)
	}

	retryCfg := retry.Config{
		MaxAttempts:     bw.cfg.MaxRetries + 1,
		InitialInterval: bw.cfg.RetryBackoff,
		MaxInterval:     5 * time.Second,
		Multiplier:      2.0,
		Operation:       fmt.Sprintf("write unit %s", key),
	}

	attempt := 0
	err := retry.Do(ctx, retryCfg, func() error {
		attempt++
		started := time.Now()
		remaining := deadlineRemaining(ctx)
		var werr error
		result, werr = bw.w.Write(ctx, []provider.MigrationUnit{unit})
		duration := time.Since(started)

		fields := []any{
			"key", key,
			"attempt", attempt,
			"duration", duration,
			"deadline_remaining", remaining,
		}
		if result != nil {
			fields = append(fields,
				"written_units", result.WrittenUnits,
				"failed_units", result.FailedUnits,
			)
		}

		if werr != nil {
			log.Warn("individual unit write attempt failed", append(fields, "error", werr)...)
			return werr
		}
		if !isSuccessfulSingleWrite(result) {
			diag := fmt.Errorf("write did not report success (written=%d, failed=%d)",
				result.WrittenUnits, result.FailedUnits)
			if len(result.Errors) > 0 {
				diag = fmt.Errorf("write did not report success: %w", result.Errors[0])
			}
			log.Warn("individual unit write attempt did not report success", append(fields, "error", diag)...)
			return diag
		}
		if result.WrittenUnits == 0 {
			log.Debug("individual unit write attempt completed as no-op", fields...)
			return nil
		}

		log.Debug("individual unit write attempt succeeded", fields...)
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, nil
}

func isSuccessfulSingleWrite(result *provider.BatchResult) bool {
	if result == nil {
		return false
	}
	if result.FailedUnits > 0 || len(result.FailedKeys) > 0 || len(result.Errors) > 0 {
		return false
	}
	return result.WrittenUnits > 0 || result.SkippedUnits > 0 || result.TotalUnits == 0
}

func deadlineRemaining(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	remaining := time.Until(deadline)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// flush wraps the underlying writer's Flush.
func (bw *batchWriter) flush(ctx context.Context) error {
	return bw.w.Flush(ctx)
}

// ---------------------------------------------------------------------------
// Per-unit retry resolution
// ---------------------------------------------------------------------------

// resolveMaxPerUnitRetry returns the effective per-unit retry limit.
// If explicit > 0 it is used directly; otherwise a sensible default of
// min(batchSize, 100) is computed so that most of a batch can be retried
// individually without unbounded work.
func resolveMaxPerUnitRetry(explicit, batchSize int) int {
	if explicit > 0 {
		return explicit
	}
	limit := batchSize
	if limit > 100 {
		limit = 100
	}
	return limit
}

// ---------------------------------------------------------------------------
// Dedup helper
// ---------------------------------------------------------------------------

// filterWritten removes keys that have already been written in this migration
// run. Returns the filtered slice and the number of skipped units.
// Lock-free: sync.Map handles concurrent access without a mutex.
func (p *Pipeline) filterWritten(units []provider.MigrationUnit) ([]provider.MigrationUnit, int) {
	filtered := make([]provider.MigrationUnit, 0, len(units))
	skipped := 0
	for _, u := range units {
		if _, ok := p.writtenKeys.Load(hashKey(u.Key)); ok {
			skipped++
			continue
		}
		filtered = append(filtered, u)
	}
	return filtered, skipped
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// unitsByKeys returns the subset of units whose keys match the given set.
func unitsByKeys(units []provider.MigrationUnit, keys []string) []provider.MigrationUnit {
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	var result []provider.MigrationUnit
	for _, u := range units {
		if keySet[u.Key] {
			result = append(result, u)
		}
	}
	return result
}

// processWriteOutcome handles the result of a batch write: records metrics,
// tracks keys, reports errors, and appends to the error list.
func processWriteOutcome(
	out writeOutcome,
	batchID int,
	units []provider.MigrationUnit,
	p *Pipeline,
	reporter provider.ProgressReporter,
	metrics metricsRecorder,
	errors *[]error,
	errorsMu *sync.Mutex,
	maxErrors int,
) {
	if out.writeErr != nil {
		// Whole batch failed after retries.
		reporter.OnError(out.writeErr, nil)
		metrics.RecordError()
		errorsMu.Lock()
		appendError(errors, maxErrors, NewWriteErrorExhausted(batchID, out.writeErr))
		errorsMu.Unlock()
		return
	}

	result := out.result
	if result == nil {
		return
	}

	// Record partial failures from individual retry fallback.
	for _, ue := range out.unitErrors {
		err := fmt.Errorf("unit %q: %w", ue.key, ue.err)
		reporter.OnError(err, nil)
		metrics.RecordError()
		errorsMu.Lock()
		appendError(errors, maxErrors, NewWriteError(batchID, err))
		errorsMu.Unlock()
	}

	// Record provider-reported errors.
	for _, e := range result.Errors {
		reporter.OnError(e, nil)
		metrics.RecordError()
		errorsMu.Lock()
		appendError(errors, maxErrors, NewWriteError(batchID, e))
		errorsMu.Unlock()
	}

	// Update metrics from actual result.
	metrics.RecordBatch(result)
	metrics.RecordBatchTables(units, result)
	reporter.OnBatchComplete(batchID, result)

	// Track only the keys that were actually written.
	writtenUnits := filterWrittenUnits(units, result)
	p.recordKeys(writtenUnits)
}

// filterWrittenUnits returns only the units that were successfully written
// according to the batch result. This prevents counting failed or skipped
// units as written in the key tracker.
func filterWrittenUnits(units []provider.MigrationUnit, result *provider.BatchResult) []provider.MigrationUnit {
	if result == nil || result.FailedUnits == 0 {
		return units
	}
	// Build a set of failed keys for fast lookup.
	failedSet := make(map[string]bool, len(result.FailedKeys))
	for _, k := range result.FailedKeys {
		failedSet[k] = true
	}
	written := make([]provider.MigrationUnit, 0, len(units))
	for _, u := range units {
		if !failedSet[u.Key] {
			written = append(written, u)
		}
	}
	return written
}

// metricsRecorder is the interface subset needed by processWriteOutcome.
// Avoids importing the full progress package.
type metricsRecorder interface {
	RecordBatch(result *provider.BatchResult)
	RecordBatchTables(units []provider.MigrationUnit, result *provider.BatchResult)
	RecordError()
}
