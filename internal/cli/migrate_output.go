package cli

import (
	"fmt"
	"strings"

	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/util"
	verifypkg "github.com/pageton/bridge-db/internal/verify"
	"github.com/pageton/bridge-db/pkg/provider"
)

// printDryRunPreview shows a migration plan without executing it.
func printDryRunPreview(cfg *config.MigrationConfig, plan *bridge.MigrationPlan) {
	fmt.Println()
	fmt.Println("=== Dry Run Preview ===")
	fmt.Printf("Source:      %s", cfg.Source.Provider)
	if cfg.Source.URL != "" {
		fmt.Printf(" (%s)", maskPassword(cfg.Source.URL))
	}
	fmt.Println()
	fmt.Printf("Destination: %s", cfg.Destination.Provider)
	if cfg.Destination.URL != "" {
		fmt.Printf(" (%s)", maskPassword(cfg.Destination.URL))
	}
	fmt.Println()

	if plan != nil && plan.CrossDB {
		fmt.Println("Mode:        Cross-engine migration")
		fmt.Println("             Data will be transformed between database formats")
	} else {
		fmt.Println("Mode:        Same-engine migration")
	}

	fmt.Println()
	fmt.Println("Pipeline settings:")
	fmt.Printf("  Batch size:       %d\n", batchSize)
	fmt.Printf("  Parallel buffers: %d\n", parallel)
	fmt.Printf("  Write workers:    %d\n", writeWorkers)
	if maxBatchBytes > 0 {
		fmt.Printf("  Max batch bytes:  %s\n", util.HumanBytes(maxBatchBytes))
	}
	fmt.Printf("  Conflict action:  %s\n", conflictStrategy)
	fmt.Printf("  Schema migration: %t\n", migrateSchema)
	fmt.Printf("  Verify after:     %t\n", verify)

	if cfg.Source.Provider != "" && cfg.Destination.Provider != "" {
		srcCaps := provider.KnownCapabilities(cfg.Source.Provider)
		dstCaps := provider.KnownCapabilities(cfg.Destination.Provider)
		fmt.Println()
		fmt.Println("Capabilities:")
		fmt.Printf("  Source  %s: %s\n", cfg.Source.Provider, srcCaps)
		fmt.Printf("  Dest    %s: %s\n", cfg.Destination.Provider, dstCaps)

		effectiveVerify := provider.EffectiveVerifyLevel(srcCaps, dstCaps)
		schemaOK := provider.SupportsSchemaMigration(srcCaps, dstCaps)
		fmt.Printf("  Effective verify: %s\n", effectiveVerify)
		if migrateSchema && !schemaOK {
			fmt.Printf("  Schema migration: skipped (one or both providers lack schema support)\n")
		}
	}

	if plan != nil {
		fmt.Println()
		fmt.Println("Plan:")
		fmt.Printf("  Transformer:     %s\n", plan.TransformerType)
		fmt.Printf("  Estimated rows:  %d\n", plan.EstimatedRows)
		fmt.Printf("  Estimated batch: %d\n", plan.EstimatedBatches)
		fmt.Printf("  Verification:    %s\n", plan.Verification)
		if len(plan.Tables) > 0 {
			fmt.Println()
			fmt.Println("Entities:")
			for _, table := range plan.Tables {
				if table.EstimatedRows >= 0 {
					fmt.Printf("  - %s (%d rows)\n", table.Name, table.EstimatedRows)
				} else {
					fmt.Printf("  - %s\n", table.Name)
				}
			}
		}
		if len(plan.UnsupportedFields) > 0 {
			fmt.Println()
			fmt.Println("Unsupported or lossy fields:")
			for _, field := range plan.UnsupportedFields {
				fmt.Printf("  - %s.%s: %s\n", field.Table, field.Field, field.Reason)
			}
		}
		if len(plan.Warnings) > 0 {
			fmt.Println()
			fmt.Println("Warnings:")
			for _, warning := range plan.Warnings {
				fmt.Printf("  - %s\n", warning)
			}
		}
	}

	fmt.Println()
	fmt.Println("No data will be written. Remove --dry-run to execute the migration.")
}

// printStartupContext prints migration context before the phases begin.
func printStartupContext(cfg *config.MigrationConfig, opts bridge.PipelineOptions) {
	fmt.Printf("Migration: %s -> %s\n", cfg.Source.Provider, cfg.Destination.Provider)
	verifyStr := "off"
	if opts.Verify {
		verifyStr = "on"
	}
	cpStr := "off"
	if opts.CheckpointEnabled {
		cpStr = "on"
	}
	fmt.Printf("  Workers: %d | Batch: %d | Verify: %s | Checkpoint: %s\n",
		opts.WriteWorkers, opts.BatchSize, verifyStr, cpStr)
	if opts.Resume {
		fmt.Println("  Resuming from checkpoint")
	}
}

// printSummary prints the migration summary to stdout.
func printSummary(result *bridge.RunResult) {
	fmt.Println()
	fmt.Println("--- Summary ---")
	fmt.Printf("Source:      %s\n", result.SrcProvider)
	fmt.Printf("Destination: %s\n", result.DstProvider)
	fmt.Printf("Duration:    %s\n", util.FormatDuration(result.Summary.Duration))

	if result.Summary.TotalWritten > 0 || result.Summary.TotalFailed > 0 {
		fmt.Printf("\nRecords:\n")
		fmt.Printf("  Written:     %d\n", result.Summary.TotalWritten)
		if result.Summary.TotalFailed > 0 {
			fmt.Printf("  Failed:      %d\n", result.Summary.TotalFailed)
		}
		if result.Summary.TotalSkipped > 0 {
			fmt.Printf("  Skipped:     %d\n", result.Summary.TotalSkipped)
		}
		if result.Summary.BytesTransferred > 0 {
			fmt.Printf("  Transferred: %s\n", util.HumanBytes(result.Summary.BytesTransferred))
		}
		if result.Summary.AvgThroughput > 0 {
			fmt.Printf("  Throughput:  %.0f avg / %.0f peak records/s\n",
				result.Summary.AvgThroughput, result.Summary.PeakThroughput)
		}
	}

	if len(result.Summary.TableMetrics) > 0 {
		fmt.Printf("\nTables:\n")
		fmt.Printf("  %-30s %8s %8s %10s\n", "Table", "Records", "Failed", "Size")
		fmt.Println("  " + strings.Repeat("-", 60))
		for _, tm := range result.Summary.TableMetrics {
			fmt.Printf("  %-30s %8d %8d %10s\n",
				util.Truncate(tm.Table, 30), tm.Written, tm.Failed, util.HumanBytes(tm.Bytes))
		}
	}

	if vr := result.VerificationReport; vr != nil {
		if vr.Passed() && vr.WarnCount == 0 {
			fmt.Printf("\n%s\n", verifypkg.FormatCompact(vr))
		} else {
			fmt.Print(verifypkg.FormatTable(vr))
		}
	} else if result.Summary.VerificationOK {
		fmt.Println("\nVerification: PASSED")
	} else if len(result.Summary.VerificationErrs) > 0 {
		fmt.Printf("\nVerification: FAILED (%d mismatch(es))\n", len(result.Summary.VerificationErrs))
		for _, ve := range result.Summary.VerificationErrs {
			fmt.Printf("  - %s", ve.Message)
			if ve.Table != "" {
				fmt.Printf(" (table: %s)", ve.Table)
			}
			if ve.Key != "" {
				fmt.Printf(" (key: %s)", ve.Key)
			}
			fmt.Println()
		}
	}

	if result.Failures != nil && result.Failures.Total > 0 {
		fmt.Printf("\nErrors by category (%d total):\n", result.Failures.Total)
		categories := []bridge.ErrorCategory{
			bridge.ErrConfig, bridge.ErrConnection, bridge.ErrSchema, bridge.ErrScan,
			bridge.ErrTransform, bridge.ErrWrite, bridge.ErrVerify, bridge.ErrCancelled, bridge.ErrInternal,
		}
		for _, cat := range categories {
			count := result.Failures.Counts[cat]
			if count == 0 {
				continue
			}
			fmt.Printf("  %-12s %d\n", cat, count)
			if ex, ok := result.Failures.Examples[cat]; ok {
				fmt.Printf("               Example: %s\n", ex.Message)
				if ex.Cause != nil {
					fmt.Printf("               Cause:   %v\n", ex.Cause)
				}
				fmt.Printf("               Hint:    %s\n", cat.UserMessage())
			}
		}
	}
}
