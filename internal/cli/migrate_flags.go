package cli

import "github.com/spf13/cobra"

func init() {
	registerMigrateFlags(migrateCmd)
	rootCmd.AddCommand(migrateCmd)
}

func registerMigrateFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceURL, "source-url", "", "source database connection URL")
	cmd.Flags().StringVar(&sourceProvider, "source-provider", "", "source provider (redis, mongodb, postgres, mysql, mariadb, cockroachdb, mssql, sqlite)")
	cmd.Flags().StringVar(&sourceHost, "source-host", "", "source database host")
	cmd.Flags().IntVar(&sourcePort, "source-port", 0, "source database port")
	cmd.Flags().StringVar(&sourceUser, "source-user", "", "source database username")
	cmd.Flags().StringVar(&sourcePass, "source-pass", "", "source database password")
	cmd.Flags().StringVar(&sourceDB, "source-db", "", "source database name")

	cmd.Flags().StringVar(&sourceSSHHost, "source-ssh-host", "", "source SSH tunnel host")
	cmd.Flags().IntVar(&sourceSSHPort, "source-ssh-port", 22, "source SSH tunnel port")
	cmd.Flags().StringVar(&sourceSSHUser, "source-ssh-user", "", "source SSH tunnel username")
	cmd.Flags().StringVar(&sourceSSHKey, "source-ssh-key", "", "source SSH private key file path")
	cmd.Flags().StringVar(&sourceSSHPass, "source-ssh-password", "", "source SSH password")
	cmd.Flags().BoolVar(&sourceSSHPassStdin, "source-ssh-password-stdin", false, "read source SSH password from stdin")
	cmd.Flags().BoolVar(&sourceSSHInsecure, "source-ssh-insecure", false, "skip SSH host key verification (insecure, use only in development)")
	cmd.Flags().StringVar(&sourceSSHKnownHosts, "source-ssh-known-hosts", "", "path to known_hosts file for SSH host key verification")

	cmd.Flags().StringVar(&destURL, "dest-url", "", "destination database connection URL")
	cmd.Flags().StringVar(&destProvider, "dest-provider", "", "destination provider (redis, mongodb, postgres, mysql, mariadb, cockroachdb, mssql, sqlite)")
	cmd.Flags().StringVar(&destHost, "dest-host", "", "destination database host")
	cmd.Flags().IntVar(&destPort, "dest-port", 0, "destination database port")
	cmd.Flags().StringVar(&destUser, "dest-user", "", "destination database username")
	cmd.Flags().StringVar(&destPass, "dest-pass", "", "destination database password")
	cmd.Flags().StringVar(&destDB, "dest-db", "", "destination database name")

	cmd.Flags().StringVar(&destSSHHost, "dest-ssh-host", "", "destination SSH tunnel host")
	cmd.Flags().IntVar(&destSSHPort, "dest-ssh-port", 22, "destination SSH tunnel port")
	cmd.Flags().StringVar(&destSSHUser, "dest-ssh-user", "", "destination SSH tunnel username")
	cmd.Flags().StringVar(&destSSHKey, "dest-ssh-key", "", "destination SSH private key file path")
	cmd.Flags().StringVar(&destSSHPass, "dest-ssh-password", "", "destination SSH password")
	cmd.Flags().BoolVar(&destSSHPassStdin, "dest-ssh-password-stdin", false, "read destination SSH password from stdin")
	cmd.Flags().BoolVar(&destSSHInsecure, "dest-ssh-insecure", false, "skip SSH host key verification (insecure, use only in development)")
	cmd.Flags().StringVar(&destSSHKnownHosts, "dest-ssh-known-hosts", "", "path to known_hosts file for SSH host key verification")

	cmd.Flags().IntVarP(&batchSize, "batch-size", "b", 1000, "number of records per batch")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "simulate migration without writing")
	cmd.Flags().BoolVar(&verify, "verify", true, "verify data after migration")
	cmd.Flags().BoolVar(&migrateSchema, "migrate-schema", true, "migrate schema (SQL databases only)")
	cmd.Flags().StringVar(&conflictStrategy, "on-conflict", "overwrite", "conflict strategy: overwrite, skip, error")
	cmd.Flags().StringVar(&fkHandling, "fk-handling", "defer_constraints", "foreign key handling: defer_constraints, ordered, skip")
	cmd.Flags().IntVar(&maxRetries, "max-retries", 3, "maximum retry attempts per batch")
	cmd.Flags().IntVar(&maxPerUnitRetry, "max-per-unit-retry", 0, "max failed units to retry individually per batch (0 = auto)")
	cmd.Flags().BoolVar(&failFast, "fail-fast", false, "abort on first transform error instead of skipping batch")

	cmd.Flags().BoolVar(&checkpoint, "checkpoint", true, "enable checkpoint for resumability")
	cmd.Flags().StringVar(&checkpointPath, "checkpoint-path", ".bridge-db/checkpoint.json", "checkpoint file path")
	cmd.Flags().BoolVar(&resume, "resume", false, "resume from last checkpoint")
	cmd.Flags().IntVar(&parallel, "parallel", 4, "number of parallel scan/write buffers")

	cmd.Flags().IntVar(&writeWorkers, "write-workers", 1, "number of concurrent writer goroutines (1 = sequential)")
	cmd.Flags().Int64Var(&maxBatchBytes, "max-batch-bytes", 32*1024*1024, "soft byte budget per batch (0 = unlimited)")
	cmd.Flags().IntVar(&maxWrittenKeys, "max-written-keys", 100_000, "max written keys retained for checkpoint dedup")
	cmd.Flags().IntVar(&checkpointInterval, "checkpoint-interval", 0, "checkpoint every N batches (0 = every batch)")

	cmd.Flags().StringVar(&verifySampleMode, "verify-sample-mode", "pct", "verification sample mode: none, pct, count")
	cmd.Flags().Float64Var(&verifySamplePct, "verify-sample-pct", 5.0, "percentage of records to sample per table (when --verify-sample-mode=pct)")
	cmd.Flags().IntVar(&verifySampleN, "verify-sample-n", 100, "number of records to sample per table (when --verify-sample-mode=count)")
	cmd.Flags().BoolVar(&verifyNoChecksum, "verify-no-checksums", false, "disable checksum comparison during verification")
	cmd.Flags().IntVar(&verifyMaxKeys, "verify-max-keys", 10000, "max total keys sampled across all tables during verification")

	cmd.Flags().SortFlags = false
	_ = cmd.Flags().Set("source-url", "")
	_ = cmd.Flags().Set("dest-url", "")

	cmd.MarkFlagsMutuallyExclusive("source-url", "source-provider")
	cmd.MarkFlagsMutuallyExclusive("dest-url", "dest-provider")
	cmd.MarkFlagsRequiredTogether("source-ssh-host", "source-ssh-user")
	cmd.MarkFlagsRequiredTogether("dest-ssh-host", "dest-ssh-user")
}
