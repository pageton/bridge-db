package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/pageton/bridge-db/internal/verify"
	"github.com/pageton/bridge-db/pkg/provider"
)

// Step 9: Verify results — compares source and destination data using
// capability-aware verification.
func (p *Pipeline) stepVerify(ctx context.Context, result *RunResult, ms *migrationState) {
	desc := phaseDesc(provider.PhaseVerifying)

	if !p.opts.Verify {
		p.reporter.OnPhaseStart(desc)
		p.reporter.OnPhaseDone(desc, 0, provider.ErrPhaseSkipped)
		return
	}

	phaseStart := time.Now()
	p.reporter.OnPhaseStart(desc)
	log := p.stepLog(9, "verify")

	effectiveLevel := provider.EffectiveVerifyLevel(p.srcCaps, p.dstCaps)
	log.Debug("effective verification level", "level", effectiveLevel)

	verifyOpts := p.opts.VerifyOptions
	if verifyOpts.SampleMode == "" {
		verifyOpts = verify.DefaultOptions()
	}

	switch effectiveLevel {
	case provider.VerifyCross:
		cv, cerr := verify.NewCrossVerifier(p.src, p.dst, verifyOpts)
		if cerr != nil {
			log.Warn("verification config invalid", "error", cerr)
			break
		}
		report, err := cv.Verify(ctx)
		if err != nil {
			log.Warn("verification error", "error", err)
		} else {
			result.VerificationReport = report
			ms.summary.VerificationOK = report.Passed()
			ms.summary.VerificationErrs = verify.ToVerificationErrors(report)
			log.Debug("verification complete",
				"status", report.Status,
				"tables", report.TotalTables,
				"passed", report.PassCount,
				"failed", report.FailCount,
				"warned", report.WarnCount,
				"sampled", report.TotalSampled,
				"duration", report.Duration,
			)
		}

	case provider.VerifyBasic:
		verifier := p.dst.Verifier(ctx)
		verifierErrors, err := verifier.Verify(ctx, p.writtenKeysFlat())
		if err != nil {
			log.Warn("verification error", "error", err)
			ms.summary.VerificationOK = false
			ms.summary.VerificationErrs = []provider.VerificationError{{
				Message: fmt.Sprintf("verification failed: %v", err),
			}}
		} else if len(verifierErrors) > 0 {
			ms.summary.VerificationOK = false
			ms.summary.VerificationErrs = verifierErrors
			log.Warn("verification found mismatches", "count", len(verifierErrors))
		} else {
			ms.summary.VerificationOK = true
			log.Debug("verification passed")
		}

	default:
		log.Warn("verification skipped — neither source nor destination supports verification",
			"source_level", p.srcCaps.Verification,
			"destination_level", p.dstCaps.Verification,
		)
		ms.summary.VerificationOK = false
		ms.summary.VerificationErrs = []provider.VerificationError{{
			Message: "verification not supported by source or destination provider",
		}}
	}

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)
	result.Phases = append(result.Phases, PhaseResult{Phase: provider.PhaseVerifying, Duration: dur})
}

// Step 10: Finalize — builds the migration summary, records failures,
// clears the checkpoint, and reports completion.
func (p *Pipeline) stepFinalize(ctx context.Context, result *RunResult, ms *migrationState) {
	phaseStart := time.Now()
	desc := phaseDesc(provider.PhaseFinalizing)
	p.reporter.OnPhaseStart(desc)
	log := p.stepLog(10, "finalize")
	_ = p.checkpoint.Clear(ctx)

	ms.summary.EndTime = time.Now()
	ms.summary.Duration = ms.summary.EndTime.Sub(ms.summary.StartTime)
	p.metrics.ToSummary(ms.summary)
	ms.summary.Errors = ms.allErrors
	for _, e := range ms.allErrors {
		result.Failures.Record(e)
	}

	result.Summary = ms.summary
	result.CheckpointPath = p.config.Checkpoint.Path

	dur := time.Since(phaseStart)
	p.reporter.OnPhaseDone(desc, dur, nil)
	p.reporter.OnMigrationComplete(ms.summary)
	result.Phases = append(result.Phases, PhaseResult{Phase: provider.PhaseFinalizing, Duration: dur})

	log.Debug("migration complete",
		"duration", ms.summary.Duration.Round(time.Millisecond),
		"written", ms.summary.TotalWritten,
		"failed", ms.summary.TotalFailed,
		"scanned", ms.summary.TotalScanned,
		"skipped", ms.summary.TotalSkipped,
		"bytes", ms.summary.BytesTransferred,
		"tables", len(ms.summary.TableMetrics),
		"errors", len(ms.allErrors),
	)

	if len(ms.summary.TableMetrics) > 0 {
		for _, tm := range ms.summary.TableMetrics {
			log.Debug("table result",
				"table", tm.Table,
				"scanned", tm.Scanned,
				"written", tm.Written,
				"failed", tm.Failed,
				"bytes", tm.Bytes,
				"duration", tm.Duration.Round(time.Millisecond),
			)
		}
	}

	if result.Failures != nil && result.Failures.Total > 0 {
		log.Warn("migration completed with errors",
			"total_errors", result.Failures.Total,
			"categories", result.Failures.Counts,
		)
	}
}
