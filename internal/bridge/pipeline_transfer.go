package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pageton/bridge-db/internal/retry"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/pkg/provider"
)

// Step 6–8: Transfer data — runs the concurrent data transfer pipeline.
func (p *Pipeline) stepTransfer(ctx context.Context, result *RunResult, ms *migrationState) error {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhaseScanning)
	p.reporter.OnPhaseStart(desc)
	log := p.stepLog(8, "transfer")

	scanOpts := provider.ScanOptions{
		BatchSize:       p.opts.BatchSize,
		ResumeToken:     resumeToken(ms.checkpoint),
		TablesCompleted: tablesCompleted(ms.checkpoint),
	}
	scanner := p.src.Scanner(ctx, scanOpts)

	writeOpts := provider.WriteOptions{
		BatchSize:  p.opts.BatchSize,
		OnConflict: p.opts.ConflictStrategy,
	}

	batchID := ms.startBatchID
	var allErrors []error
	const maxErrorsToTrack = 1000
	var errorsMu sync.Mutex
	var lastWrittenBatchID atomic.Int64

	// Use max(Parallel, WriteWorkers) to preserve scan-ahead pipelining
	// while still bounding in-flight memory for multi-worker setups.
	scanBuf := max(p.opts.Parallel, p.opts.WriteWorkers)
	scanCh := make(chan scanResult, scanBuf)

	var scanWg sync.WaitGroup
	scanWg.Add(1)
	go func() {
		defer scanWg.Done()
		defer close(scanCh)
		defer func() { _ = scanner.Close() }()

		for {
			if err := ctx.Err(); err != nil {
				select {
				case scanCh <- scanResult{err: err}:
				case <-ctx.Done():
				}
				return
			}

			var units []provider.MigrationUnit
			units, err := scanner.Next(ctx)
			if err == io.EOF {
				return
			}
			if err != nil {
				scanRetryErr := retry.Do(ctx, retry.Config{
					MaxAttempts:     p.opts.MaxRetries + 1,
					InitialInterval: p.opts.RetryBackoff,
					MaxInterval:     10 * time.Second,
					Multiplier:      2.0,
				}, func() error {
					units, err = scanner.Next(ctx)
					if err == io.EOF {
						return nil
					}
					return err
				})
				if scanRetryErr != nil {
					select {
					case scanCh <- scanResult{err: scanRetryErr}:
					case <-ctx.Done():
					}
					continue
				}
				if err == io.EOF {
					return
				}
			}

			if len(units) == 0 {
				continue
			}

			if !transform.IsNoopTransformer(p.transformer) {
				transformErr := retry.Do(ctx, retry.Config{
					MaxAttempts:     p.opts.MaxRetries + 1,
					InitialInterval: p.opts.RetryBackoff,
					MaxInterval:     10 * time.Second,
					Multiplier:      2.0,
					Operation:       "transform batch",
				}, func() error {
					var terr error
					units, terr = p.transformer.Transform(ctx, units)
					return terr
				})
				if transformErr != nil {
					p.reporter.OnError(transformErr, nil)
					p.metrics.RecordError()
					errorsMu.Lock()
					appendError(&allErrors, maxErrorsToTrack, NewTransformError(batchID+1, transformErr))
					errorsMu.Unlock()

					if p.opts.FailFast {
						log.Error("transform error after retries, aborting (fail-fast enabled)",
							"error", transformErr,
							"batch", batchID+1,
						)
						select {
						case scanCh <- scanResult{err: transformErr}:
						case <-ctx.Done():
						}
						return
					}
					log.Warn("transform error after retries, skipping batch",
						"error", transformErr,
						"batch", batchID+1,
					)
					continue
				}
			}

			for _, sub := range splitBatch(units, p.opts.MaxBatchBytes) {
				batchID++
				p.metrics.RecordScan(int64(len(sub)), totalSize(sub))
				p.reporter.OnBatchStart(batchID, len(sub))
				select {
				case scanCh <- scanResult{batchID: batchID, units: sub}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	batchWriters := make([]*batchWriter, p.opts.WriteWorkers)
	for i := range batchWriters {
		batchWriters[i] = &batchWriter{
			w: p.dst.Writer(ctx, writeOpts),
			cfg: writeConfig{
				MaxRetries:       p.opts.MaxRetries,
				RetryBackoff:     p.opts.RetryBackoff,
				ConflictStrategy: p.opts.ConflictStrategy,
				MaxPerUnitRetry:  resolveMaxPerUnitRetry(p.opts.MaxPerUnitRetry, p.opts.BatchSize),
			},
		}
	}

	var writeWg sync.WaitGroup
	for i := 0; i < p.opts.WriteWorkers; i++ {
		writeWg.Add(1)
		go func(bw *batchWriter) {
			defer writeWg.Done()
			for sr := range scanCh {
				p.waitIfPaused()

				if err := ctx.Err(); err != nil {
					return
				}

				if sr.err != nil {
					p.reporter.OnError(sr.err, nil)
					p.metrics.RecordError()
					errorsMu.Lock()
					appendError(&allErrors, maxErrorsToTrack, NewScanError("scan failed", sr.err))
					errorsMu.Unlock()
					log.Warn("scan error, skipping batch", "error", sr.err, "batch", sr.batchID)
					continue
				}

				bw.batchID = sr.batchID
				out := bw.writeBatch(ctx, p, sr.units)
				processWriteOutcome(out, sr.batchID, sr.units, p, p.reporter, p.metrics, &allErrors, &errorsMu, maxErrorsToTrack)

				if out.writeErr == nil && out.result != nil {
					lastWrittenBatchID.Store(int64(sr.batchID))
					p.maybeCheckpoint(ctx, sr.batchID, scanner)
				}

				p.reporter.OnProgress(p.metrics.Snapshot(provider.PhaseWriting))
			}
		}(batchWriters[i])
	}

	scanWg.Wait()
	writeWg.Wait()

	for _, bw := range batchWriters {
		if err := bw.flush(ctx); err != nil {
			p.saveCheckpoint(context.Background(), int(lastWrittenBatchID.Load()), scanner)
			p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
			return p.abort(NewWriteErrorExhausted(0, err).WithStep(8))
		}
	}

	if err := ctx.Err(); err != nil {
		p.saveCheckpoint(context.Background(), int(lastWrittenBatchID.Load()), scanner)
		p.reporter.OnPhaseDone(desc, time.Since(phaseStart), err)
		return p.abort(NewCancelledError("Migration was cancelled", err).WithStep(8))
	}

	ms.allErrors = allErrors

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)
	result.Phases = append(result.Phases, PhaseResult{Phase: provider.PhaseWriting, Duration: dur})
	return nil
}

// waitIfPaused blocks until the pipeline is resumed. Safe for concurrent workers.
func (p *Pipeline) waitIfPaused() {
	if !p.paused.Load() {
		return
	}
	p.pauseCond.L.Lock()
	for p.paused.Load() {
		p.pauseCond.Wait()
	}
	p.pauseCond.L.Unlock()
}

// hashKey returns a truncated SHA-256 hash of a key for dedup tracking.
func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:8])
}

// recordKeys adds unit keys to the tracking set, evicting the oldest entries
// when the capacity (opts.MaxWrittenKeys) is exceeded.
func (p *Pipeline) recordKeys(units []provider.MigrationUnit) {
	cap := p.opts.MaxWrittenKeys

	for _, u := range units {
		hk := hashKey(u.Key)
		if _, loaded := p.writtenKeys.LoadOrStore(hk, true); loaded {
			continue
		}

		p.keyMu.Lock()
		if u.Table != "" && !p.tableSet[u.Table] {
			p.tableSet[u.Table] = true
			p.scannedTables = append(p.scannedTables, u.Table)
		}
		if p.keyRingLen >= len(p.keyRing) && len(p.keyRing) < cap {
			p.growKeyRing(cap)
		}
		if p.keyRingLen >= cap {
			evictIdx := p.keyRingHead
			p.writtenKeys.Delete(p.keyRing[evictIdx])
			p.keyRingLen--
			p.keysEvicted = true
		}
		p.keyRing[p.keyRingHead] = hk
		p.keyRingHead = (p.keyRingHead + 1) % cap
		p.keyRingLen++
		p.keyMu.Unlock()
	}

	atomic.AddInt64(&p.totalWritten, int64(len(units)))
}

func keyRingSizeFor(required, max int) int {
	if required <= 0 {
		required = 1
	}
	if max > 0 && required > max {
		return required
	}
	size := defaultKeyRingSize
	if max > 0 && max < size {
		size = max
	}
	for size < required && (max <= 0 || size < max) {
		next := size * 2
		if max > 0 && next > max {
			next = max
		}
		if next <= size {
			break
		}
		size = next
	}
	if size < required {
		return required
	}
	return size
}

func (p *Pipeline) growKeyRing(max int) {
	newSize := keyRingSizeFor(p.keyRingLen+1, max)
	if newSize <= len(p.keyRing) {
		return
	}
	newRing := make([]string, newSize)
	if p.keyRingLen > 0 {
		start := (p.keyRingHead - p.keyRingLen + len(p.keyRing)) % len(p.keyRing)
		for i := 0; i < p.keyRingLen; i++ {
			newRing[i] = p.keyRing[(start+i)%len(p.keyRing)]
		}
	}
	p.keyRing = newRing
	p.keyRingHead = p.keyRingLen % len(p.keyRing)
}

// writtenKeysFlat materialises the ring buffer as a flat slice for consumers
// that need an ordered list (verification, checkpoint, resume token).
func (p *Pipeline) writtenKeysFlat() []string {
	if p.keyRingLen == 0 {
		return nil
	}
	start := (p.keyRingHead - p.keyRingLen + len(p.keyRing)) % len(p.keyRing)
	out := make([]string, p.keyRingLen)
	for i := 0; i < p.keyRingLen; i++ {
		out[i] = p.keyRing[(start+i)%len(p.keyRing)]
	}
	return out
}

// splitBatch splits a slice of migration units into sub-batches when the
// cumulative byte size exceeds maxBytes. If maxBytes is 0 the batch is
// returned as-is.
func splitBatch(units []provider.MigrationUnit, maxBytes int64) [][]provider.MigrationUnit {
	if maxBytes <= 0 || len(units) == 0 {
		return [][]provider.MigrationUnit{units}
	}
	var batches [][]provider.MigrationUnit
	var current []provider.MigrationUnit
	var currentBytes int64
	for _, u := range units {
		if len(current) > 0 && currentBytes+u.Size > maxBytes {
			batches = append(batches, current)
			current = nil
			currentBytes = 0
		}
		current = append(current, u)
		currentBytes += u.Size
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}

// totalSize computes the total byte size of a slice of MigrationUnits.
func totalSize(units []provider.MigrationUnit) int64 {
	var total int64
	for _, u := range units {
		total += u.Size
	}
	return total
}

// appendError adds an error to the slice, respecting the cap.
func appendError(errors *[]error, max int, err error) {
	if len(*errors) < max {
		*errors = append(*errors, err)
	}
}
