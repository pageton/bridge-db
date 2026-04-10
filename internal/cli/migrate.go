package cli

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/progress"
	"github.com/pageton/bridge-db/internal/tunnel"
	"github.com/pageton/bridge-db/internal/util"
	verifypkg "github.com/pageton/bridge-db/internal/verify"
	"github.com/pageton/bridge-db/pkg/provider"
)

var (
	// Source connection flags
	sourceProvider string
	sourceHost     string
	sourcePort     int
	sourceUser     string
	sourcePass     string
	sourceDB       string

	// Source SSH flags
	sourceSSHHost       string
	sourceSSHPort       int
	sourceSSHUser       string
	sourceSSHKey        string
	sourceSSHPass       string
	sourceSSHPassStdin  bool
	sourceSSHInsecure   bool
	sourceSSHKnownHosts string

	// Destination connection flags
	destProvider string
	destHost     string
	destPort     int
	destUser     string
	destPass     string
	destDB       string

	// Destination SSH flags
	destSSHHost       string
	destSSHPort       int
	destSSHUser       string
	destSSHKey        string
	destSSHPass       string
	destSSHPassStdin  bool
	destSSHInsecure   bool
	destSSHKnownHosts string

	// Pipeline flags
	batchSize        int
	dryRun           bool
	verify           bool
	migrateSchema    bool
	conflictStrategy string
	fkHandling       string
	maxRetries       int
	failFast         bool

	// Checkpoint flags
	checkpoint     bool
	checkpointPath string

	resume bool

	parallel           int
	writeWorkers       int
	maxBatchBytes      int64
	maxWrittenKeys     int
	checkpointInterval int
)

// migrateCmd is the migrate command.
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate data from source to destination database",
	Long: `Migrate data between database instances -- same engine or cross-engine.

Bridge-DB scans the source, transforms data if needed, writes to the
destination, and optionally verifies the result.  Checkpoints are saved
automatically so interrupted migrations can be resumed with --resume.

Providers:
  postgres  mysql  mariadb  cockroachdb  (always available)
  mongodb   mssql  sqlite   redis        (build tag required)

Configuration sources (later overrides earlier):
  1. Config file  (-c / --config)
  2. URL flags    (--source-url, --dest-url)
  3. Individual flags (--source-host, --source-port, ...)

Examples:
  # Postgres to MySQL (URL-based, simplest)
  bridge migrate \
    --source-url "postgres://user:pass@localhost:5432/myapp" \
    --dest-url "mysql://user:pass@tcp(localhost:3306)/myapp"

  # Redis to Redis (same engine clone)
  bridge migrate \
    --source-url "redis://:pass@src-host:6379/0" \
    --dest-url "redis://:pass@dst-host:6379/0"

  # MongoDB to PostgreSQL (cross-engine, NoSQL to SQL)
  bridge migrate \
    --source-url "mongodb://user:pass@localhost:27017/myapp" \
    --dest-url "postgres://user:pass@localhost:5432/myapp"

  # SSH tunnel to remote database behind firewall
  bridge migrate \
    --source-url "postgres://user:pass@10.0.1.50:5432/myapp" \
    --source-ssh-host bastion.example.com \
    --source-ssh-key ~/.ssh/id_rsa \
    --dest-url "mysql://root:pass@tcp(localhost:3306)/myapp"

  # Dry run (preview what would be migrated without writing)
  bridge migrate --dry-run \
    --source-url "postgres://localhost/myapp" \
    --dest-url "mysql://localhost/myapp"

  # Resume an interrupted migration
  bridge migrate --resume \
    --source-url "postgres://localhost/myapp" \
    --dest-url "mysql://localhost/myapp"

  # High-throughput with concurrent writers
  bridge migrate --write-workers 4 --parallel 8 --batch-size 5000 \
    --source-url "postgres://localhost/bigdb" \
    --dest-url "mysql://localhost/bigdb"

Performance tuning flags:
  --parallel N            scan/write buffer depth (default 4)
  --write-workers N       concurrent writer goroutines (default 1)
  --batch-size N          rows per batch (default 1000)
  --max-batch-bytes BYTES soft byte budget per batch (default 32 MiB)
  --checkpoint-interval N save checkpoint every N batches (default 1)`,
	RunE: runMigrate,
}

func init() {
	// Source connection flags
	migrateCmd.Flags().StringVar(&sourceURL, "source-url", "", "source database connection URL")
	migrateCmd.Flags().StringVar(&sourceProvider, "source-provider", "", "source provider (redis, mongodb, postgres, mysql, mariadb, cockroachdb, mssql, sqlite)")
	migrateCmd.Flags().StringVar(&sourceHost, "source-host", "", "source database host")
	migrateCmd.Flags().IntVar(&sourcePort, "source-port", 0, "source database port")
	migrateCmd.Flags().StringVar(&sourceUser, "source-user", "", "source database username")
	migrateCmd.Flags().StringVar(&sourcePass, "source-pass", "", "source database password")
	migrateCmd.Flags().StringVar(&sourceDB, "source-db", "", "source database name")

	// Source SSH flags
	migrateCmd.Flags().StringVar(&sourceSSHHost, "source-ssh-host", "", "source SSH tunnel host")
	migrateCmd.Flags().IntVar(&sourceSSHPort, "source-ssh-port", 22, "source SSH tunnel port")
	migrateCmd.Flags().StringVar(&sourceSSHUser, "source-ssh-user", "", "source SSH tunnel username")
	migrateCmd.Flags().StringVar(&sourceSSHKey, "source-ssh-key", "", "source SSH private key file path")
	migrateCmd.Flags().StringVar(&sourceSSHPass, "source-ssh-password", "", "source SSH password")
	migrateCmd.Flags().BoolVar(&sourceSSHPassStdin, "source-ssh-password-stdin", false, "read source SSH password from stdin")
	migrateCmd.Flags().BoolVar(&sourceSSHInsecure, "source-ssh-insecure", false, "skip SSH host key verification (insecure, use only in development)")
	migrateCmd.Flags().StringVar(&sourceSSHKnownHosts, "source-ssh-known-hosts", "", "path to known_hosts file for SSH host key verification")

	// Destination connection flags
	migrateCmd.Flags().StringVar(&destURL, "dest-url", "", "destination database connection URL")
	migrateCmd.Flags().StringVar(&destProvider, "dest-provider", "", "destination provider (redis, mongodb, postgres, mysql, mariadb, cockroachdb, mssql, sqlite)")
	migrateCmd.Flags().StringVar(&destHost, "dest-host", "", "destination database host")
	migrateCmd.Flags().IntVar(&destPort, "dest-port", 0, "destination database port")
	migrateCmd.Flags().StringVar(&destUser, "dest-user", "", "destination database username")
	migrateCmd.Flags().StringVar(&destPass, "dest-pass", "", "destination database password")
	migrateCmd.Flags().StringVar(&destDB, "dest-db", "", "destination database name")

	// Destination SSH flags
	migrateCmd.Flags().StringVar(&destSSHHost, "dest-ssh-host", "", "destination SSH tunnel host")
	migrateCmd.Flags().IntVar(&destSSHPort, "dest-ssh-port", 22, "destination SSH tunnel port")
	migrateCmd.Flags().StringVar(&destSSHUser, "dest-ssh-user", "", "destination SSH tunnel username")
	migrateCmd.Flags().StringVar(&destSSHKey, "dest-ssh-key", "", "destination SSH private key file path")
	migrateCmd.Flags().StringVar(&destSSHPass, "dest-ssh-password", "", "destination SSH password")
	migrateCmd.Flags().BoolVar(&destSSHPassStdin, "dest-ssh-password-stdin", false, "read destination SSH password from stdin")
	migrateCmd.Flags().BoolVar(&destSSHInsecure, "dest-ssh-insecure", false, "skip SSH host key verification (insecure, use only in development)")
	migrateCmd.Flags().StringVar(&destSSHKnownHosts, "dest-ssh-known-hosts", "", "path to known_hosts file for SSH host key verification")

	// Pipeline flags
	migrateCmd.Flags().IntVarP(&batchSize, "batch-size", "b", 1000, "number of records per batch")
	migrateCmd.Flags().BoolVar(&dryRun, "dry-run", false, "simulate migration without writing")
	migrateCmd.Flags().BoolVar(&verify, "verify", true, "verify data after migration")
	migrateCmd.Flags().BoolVar(&migrateSchema, "migrate-schema", true, "migrate schema (SQL databases only)")
	migrateCmd.Flags().StringVar(&conflictStrategy, "on-conflict", "overwrite", "conflict strategy: overwrite, skip, error")
	migrateCmd.Flags().StringVar(&fkHandling, "fk-handling", "defer_constraints", "foreign key handling: defer_constraints, ordered, skip")
	migrateCmd.Flags().IntVar(&maxRetries, "max-retries", 3, "maximum retry attempts per batch")
	migrateCmd.Flags().BoolVar(&failFast, "fail-fast", false, "abort on first transform error instead of skipping batch")

	// Checkpoint flags
	migrateCmd.Flags().BoolVar(&checkpoint, "checkpoint", true, "enable checkpoint for resumability")
	migrateCmd.Flags().StringVar(&checkpointPath, "checkpoint-path", ".bridge-db/checkpoint.json", "checkpoint file path")

	// Resume flag
	migrateCmd.Flags().BoolVar(&resume, "resume", false, "resume from last checkpoint")

	// Parallel flag
	migrateCmd.Flags().IntVar(&parallel, "parallel", 4, "number of parallel scan/write buffers")

	// Concurrency tuning flags
	migrateCmd.Flags().IntVar(&writeWorkers, "write-workers", 1, "number of concurrent writer goroutines (1 = sequential)")
	migrateCmd.Flags().Int64Var(&maxBatchBytes, "max-batch-bytes", 32*1024*1024, "soft byte budget per batch (0 = unlimited)")
	migrateCmd.Flags().IntVar(&maxWrittenKeys, "max-written-keys", 100_000, "max written keys retained for checkpoint dedup")
	migrateCmd.Flags().IntVar(&checkpointInterval, "checkpoint-interval", 0, "checkpoint every N batches (0 = every batch)")

	// Verification tuning flags
	migrateCmd.Flags().StringVar(&verifySampleMode, "verify-sample-mode", "pct", "verification sample mode: none, pct, count")
	migrateCmd.Flags().Float64Var(&verifySamplePct, "verify-sample-pct", 5.0, "percentage of records to sample per table (when --verify-sample-mode=pct)")
	migrateCmd.Flags().IntVar(&verifySampleN, "verify-sample-n", 100, "number of records to sample per table (when --verify-sample-mode=count)")
	migrateCmd.Flags().BoolVar(&verifyNoChecksum, "verify-no-checksums", false, "disable checksum comparison during verification")
	migrateCmd.Flags().IntVar(&verifyMaxKeys, "verify-max-keys", 10000, "max total keys sampled across all tables during verification")

	// Flag groups for better help readability
	migrateCmd.Flags().SortFlags = false
	_ = migrateCmd.Flags().Set("source-url", "")
	_ = migrateCmd.Flags().Set("dest-url", "")

	migrateCmd.MarkFlagsMutuallyExclusive("source-url", "source-provider")
	migrateCmd.MarkFlagsMutuallyExclusive("dest-url", "dest-provider")
	migrateCmd.MarkFlagsRequiredTogether("source-ssh-host", "source-ssh-user")
	migrateCmd.MarkFlagsRequiredTogether("dest-ssh-host", "dest-ssh-user")

	rootCmd.AddCommand(migrateCmd)
}

// runMigrate executes the migration.
func runMigrate(cmd *cobra.Command, args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Warn about password exposure via CLI flags.
	warnPasswordFlags(cmd)

	cfg, err := buildConfigFromFlags()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Pre-flight validation with actionable hints.
	if err := preflightValidate(cfg); err != nil {
		return err
	}

	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := config.Resolve(cfg); err != nil {
		return fmt.Errorf("config resolution failed: %w", err)
	}

	// Print dry-run preview and exit without connecting.
	if dryRun {
		printDryRunPreview(cfg)
		return nil
	}

	reporter := progress.NewConsoleReporter()

	opts := bridge.DefaultPipelineOptions()
	opts.BatchSize = batchSize
	opts.DryRun = dryRun
	opts.Verify = verify
	opts.MigrateSchema = migrateSchema
	opts.MaxRetries = maxRetries
	opts.FailFast = failFast
	opts.ConflictStrategy = provider.ConflictStrategy(conflictStrategy)
	opts.FKHandling = fkHandling
	opts.CheckpointEnabled = checkpoint
	opts.Resume = resume
	opts.Parallel = parallel
	opts.WriteWorkers = writeWorkers
	opts.MaxBatchBytes = maxBatchBytes
	opts.MaxWrittenKeys = maxWrittenKeys
	opts.CheckpointInterval = checkpointInterval

	// Wire verification options from CLI flags.
	opts.VerifyOptions = verifypkg.Options{
		CountComparison:    true,
		SampleMode:         verifySampleMode,
		SamplePct:          verifySamplePct,
		SampleN:            verifySampleN,
		ChecksumComparison: !verifyNoChecksum,
		MaxSampleKeys:      verifyMaxKeys,
		Seed:               42,
	}

	pipeline, err := bridge.NewPipeline(cfg, opts, reporter, nil)
	if err != nil {
		return fmt.Errorf("failed to create migration pipeline: %w", err)
	}

	result, err := pipeline.Run(ctx)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	printSummary(result)
	return nil
}

// buildConfigFromFlags constructs a MigrationConfig from CLI flags.
func buildConfigFromFlags() (*config.MigrationConfig, error) {
	// If config file specified, use it as base
	cfg, err := loadConfig()
	if err != nil {
		cfg = config.DefaultConfig()
	}

	// Override with URL flags
	if sourceURL != "" {
		cfg.Source.URL = sourceURL
	}
	if destURL != "" {
		cfg.Destination.URL = destURL
	}

	// Override with individual flags
	if sourceProvider != "" {
		cfg.Source.Provider = sourceProvider
	}
	if destProvider != "" {
		cfg.Destination.Provider = destProvider
	}

	// Auto-detect provider from URL scheme when not explicitly set
	if cfg.Source.Provider == "" && cfg.Source.URL != "" {
		cfg.Source.Provider = providerFromURLScheme(cfg.Source.URL)
	}
	if cfg.Destination.Provider == "" && cfg.Destination.URL != "" {
		cfg.Destination.Provider = providerFromURLScheme(cfg.Destination.URL)
	}

	// Source individual fields override
	if sourceHost != "" || sourcePort != 0 || sourceUser != "" || sourceDB != "" {
		config.ApplyOverrides(&cfg.Source, config.OverrideFields{
			Host: sourceHost, Port: sourcePort, User: sourceUser,
			Password: sourcePass, Database: sourceDB,
		})
	}

	// Destination individual fields override
	if destHost != "" || destPort != 0 || destUser != "" || destDB != "" {
		config.ApplyOverrides(&cfg.Destination, config.OverrideFields{
			Host: destHost, Port: destPort, User: destUser,
			Password: destPass, Database: destDB,
		})
	}

	// Source SSH tunnel config
	if sourceSSHHost != "" {
		cfg.Source.SSH = &tunnel.Config{
			Enabled:        true,
			Host:           sourceSSHHost,
			Port:           sourceSSHPort,
			Username:       sourceSSHUser,
			KeyFile:        sourceSSHKey,
			Password:       sourceSSHPass,
			KnownHostsFile: sourceSSHKnownHosts,
			Insecure:       sourceSSHInsecure,
		}
		if sourceSSHPassStdin {
			pass, err := readPasswordFromStdin()
			if err != nil {
				return nil, fmt.Errorf("failed to read SSH password: %w", err)
			}
			cfg.Source.SSH.Password = pass
		}
	}

	// Destination SSH tunnel config
	if destSSHHost != "" {
		cfg.Destination.SSH = &tunnel.Config{
			Enabled:        true,
			Host:           destSSHHost,
			Port:           destSSHPort,
			Username:       destSSHUser,
			KeyFile:        destSSHKey,
			Password:       destSSHPass,
			KnownHostsFile: destSSHKnownHosts,
			Insecure:       destSSHInsecure,
		}
		if destSSHPassStdin {
			pass, err := readPasswordFromStdin()
			if err != nil {
				return nil, fmt.Errorf("failed to read SSH password: %w", err)
			}
			cfg.Destination.SSH.Password = pass
		}
	}

	// Pipeline config
	cfg.Pipeline = config.PipelineConfig{
		BatchSize:        batchSize,
		DryRun:           dryRun,
		Verify:           verify,
		MigrateSchema:    migrateSchema,
		ConflictStrategy: provider.ConflictStrategy(conflictStrategy),
		FKHandling:       fkHandling,
	}

	// Checkpoint config
	cfg.Checkpoint = config.CheckpointConfig{
		Enabled: checkpoint,
		Path:    checkpointPath,
	}

	// Logging config
	cfg.Logging = config.LoggingConfig{
		Level: logLevel,
		JSON:  logJSON,
	}

	return cfg, nil
}

// readPasswordFromStdin reads a password from stdin.
func readPasswordFromStdin() (string, error) {
	var buf strings.Builder
	buf.Grow(64)
	for {
		var b [1]byte
		n, err := os.Stdin.Read(b[:])
		if n > 0 {
			if b[0] == '\n' || b[0] == '\r' {
				break
			}
			buf.WriteByte(b[0])
		}
		if err != nil {
			break
		}
	}
	return buf.String(), nil
}

// warnPasswordFlags prints a deprecation warning when password values are
// passed via CLI flags, which expose them in the process listing.
func warnPasswordFlags(cmd *cobra.Command) {
	passwordFlags := map[string]string{
		"source-pass":         "BRIDGE_SOURCE_URL",
		"dest-pass":           "BRIDGE_DEST_URL",
		"source-ssh-password": "BRIDGE_SOURCE_SSH_KEY + SSH agent",
		"dest-ssh-password":   "BRIDGE_DEST_SSH_KEY + SSH agent",
	}
	for flag, alt := range passwordFlags {
		if cmd.Flags().Lookup(flag).Changed {
			_, _ = fmt.Fprintf(os.Stderr, "Warning: --%s exposes credentials in the process listing. Prefer %s or --*-stdin instead.\n", flag, alt)
		}
	}
}

// knownProviders lists providers that can be checked without runtime registration.
var knownProviders = map[string]bool{
	"postgres": true, "mysql": true, "mariadb": true, "cockroachdb": true,
	"mongodb": true, "mssql": true, "sqlite": true, "redis": true,
}

// heavyProviders require build tags to be available.
var heavyProviders = map[string]bool{
	"mongodb": true, "mssql": true, "sqlite": true, "redis": true,
}

// preflightValidate checks configuration before connecting and provides
// actionable error messages with hints.
func preflightValidate(cfg *config.MigrationConfig) error {
	var hints []string

	// Source provider checks
	if cfg.Source.Provider == "" {
		if sourceURL == "" {
			hints = append(hints, "Specify a source with --source-url or --source-provider")
		} else {
			hints = append(hints, "Could not detect provider from --source-url. Add --source-provider")
		}
	} else if !knownProviders[cfg.Source.Provider] {
		hints = append(hints, fmt.Sprintf("Unknown source provider %q", cfg.Source.Provider))
		hints = append(hints, "Valid providers: postgres, mysql, mariadb, cockroachdb, mongodb, mssql, sqlite, redis")
	} else if heavyProviders[cfg.Source.Provider] {
		available := provider.AvailableProviders()
		found := false
		for _, p := range available {
			if p == cfg.Source.Provider {
				found = true
				break
			}
		}
		if !found {
			hints = append(hints, fmt.Sprintf("Provider %q is not compiled into this binary", cfg.Source.Provider))
			hints = append(hints, "Rebuild with: go build -tags \"mongodb,mssql,sqlite,redis\" ./cmd/bridge")
		}
	}

	// Destination provider checks
	if cfg.Destination.Provider == "" {
		if destURL == "" {
			hints = append(hints, "Specify a destination with --dest-url or --dest-provider")
		} else {
			hints = append(hints, "Could not detect provider from --dest-url. Add --dest-provider")
		}
	} else if !knownProviders[cfg.Destination.Provider] {
		hints = append(hints, fmt.Sprintf("Unknown destination provider %q", cfg.Destination.Provider))
		hints = append(hints, "Valid providers: postgres, mysql, mariadb, cockroachdb, mongodb, mssql, sqlite, redis")
	} else if heavyProviders[cfg.Destination.Provider] {
		available := provider.AvailableProviders()
		found := false
		for _, p := range available {
			if p == cfg.Destination.Provider {
				found = true
				break
			}
		}
		if !found {
			hints = append(hints, fmt.Sprintf("Provider %q is not compiled into this binary", cfg.Destination.Provider))
			hints = append(hints, "Rebuild with: go build -tags \"mongodb,mssql,sqlite,redis\" ./cmd/bridge")
		}
	}

	// Same source and dest check
	if cfg.Source.Provider != "" && cfg.Destination.Provider != "" &&
		cfg.Source.Provider == cfg.Destination.Provider &&
		cfg.Source.URL == cfg.Destination.URL {
		hints = append(hints, "Source and destination URLs are identical -- use different databases")
	}

	// Resume without checkpoint
	if resume && !checkpoint {
		hints = append(hints, "--resume requires --checkpoint to be enabled")
	}

	// Pipeline option sanity
	if batchSize <= 0 {
		hints = append(hints, fmt.Sprintf("--batch-size must be positive, got %d", batchSize))
	}
	if writeWorkers < 1 {
		hints = append(hints, fmt.Sprintf("--write-workers must be at least 1, got %d", writeWorkers))
	}
	if parallel < 1 {
		hints = append(hints, fmt.Sprintf("--parallel must be at least 1, got %d", parallel))
	}

	// Capability compatibility warnings
	if cfg.Source.Provider != "" && cfg.Destination.Provider != "" {
		srcCaps := provider.KnownCapabilities(cfg.Source.Provider)
		dstCaps := provider.KnownCapabilities(cfg.Destination.Provider)

		if migrateSchema && !provider.SupportsSchemaMigration(srcCaps, dstCaps) {
			if !srcCaps.Schema {
				hints = append(hints, fmt.Sprintf("Source %q does not support schema migration -- --migrate-schema will be skipped", cfg.Source.Provider))
			} else if !dstCaps.Schema {
				hints = append(hints, fmt.Sprintf("Destination %q does not support schema migration -- --migrate-schema will be skipped", cfg.Destination.Provider))
			}
		}
	}

	if len(hints) > 0 {
		return fmt.Errorf("configuration errors:\n  %s", strings.Join(hints, "\n  "))
	}
	return nil
}

// printDryRunPreview shows a migration plan without executing it.
func printDryRunPreview(cfg *config.MigrationConfig) {
	fmt.Println()
	fmt.Println("=== Dry Run Preview ===")
	fmt.Printf("Source:      %s", cfg.Source.Provider)
	if cfg.Source.URL != "" {
		// Mask password in URL
		u := maskPassword(cfg.Source.URL)
		fmt.Printf(" (%s)", u)
	}
	fmt.Println()
	fmt.Printf("Destination: %s", cfg.Destination.Provider)
	if cfg.Destination.URL != "" {
		u := maskPassword(cfg.Destination.URL)
		fmt.Printf(" (%s)", u)
	}
	fmt.Println()

	if cfg.IsCrossDB() {
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

	// Show capability summary
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

	fmt.Println()
	fmt.Println("No data will be written. Remove --dry-run to execute the migration.")
	fmt.Println(strings.Repeat("=", 24))
}

// maskPassword replaces the password component in a database URL with ***.
// Uses net/url for robust parsing across all supported URL formats.
func maskPassword(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.User == nil {
		return rawURL
	}
	username := u.User.Username()
	u.User = url.UserPassword(username, "***")
	return u.String()
}

// printSummary prints the migration summary to stdout.
func printSummary(result *bridge.RunResult) {
	fmt.Println()
	fmt.Println("=== Migration Summary ===")
	fmt.Printf("Source:      %s\n", result.SrcProvider)
	fmt.Printf("Destination: %s\n", result.DstProvider)
	fmt.Printf("Duration:    %s\n", result.Summary.Duration)

	// Phase timing
	if len(result.Phases) > 0 {
		fmt.Println("\nPhases:")
		for _, p := range result.Phases {
			status := "OK"
			if p.Error != nil {
				status = fmt.Sprintf("FAILED: %v", p.Error)
			}
			fmt.Printf("  %-20s %s  %s\n", p.Phase, p.Duration.Round(1e6), status)
		}
	}

	if result.Summary.TotalScanned > 0 {
		fmt.Printf("\nRecords:\n")
		fmt.Printf("  Scanned:     %d\n", result.Summary.TotalScanned)
		fmt.Printf("  Written:     %d\n", result.Summary.TotalWritten)
		fmt.Printf("  Failed:      %d\n", result.Summary.TotalFailed)
		fmt.Printf("  Skipped:     %d\n", result.Summary.TotalSkipped)
	}

	if result.Summary.BytesTransferred > 0 {
		fmt.Printf("  Transferred: %d bytes\n", result.Summary.BytesTransferred)
	}

	// Throughput
	if result.Summary.AvgThroughput > 0 {
		fmt.Printf("  Throughput:  %.0f avg / %.0f peak units/s\n",
			result.Summary.AvgThroughput, result.Summary.PeakThroughput)
	}

	// Per-table breakdown
	if len(result.Summary.TableMetrics) > 0 {
		fmt.Println("\nPer-table breakdown:")
		fmt.Printf("  %-30s %8s %8s %8s %10s\n", "Table", "Scanned", "Written", "Failed", "Size")
		fmt.Println("  " + strings.Repeat("-", 70))
		for _, tm := range result.Summary.TableMetrics {
			fmt.Printf("  %-30s %8d %8d %8d %10s\n",
				util.Truncate(tm.Table, 30), tm.Scanned, tm.Written, tm.Failed, util.HumanBytes(tm.Bytes))
		}
	}

	// Verification status — use the detailed report when available.
	if vr := result.VerificationReport; vr != nil {
		fmt.Print(verifypkg.FormatTable(vr))
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

	// Categorized error breakdown
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

	fmt.Println(strings.Repeat("=", 24))
}

// providerFromURLScheme extracts the provider name from a database URL scheme.
// Returns empty string if the scheme is unrecognized.
func providerFromURLScheme(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	switch u.Scheme {
	case "postgres", "postgresql":
		return "postgres"
	case "mysql":
		return "mysql"
	case "mariadb":
		return "mariadb"
	case "cockroachdb", "crdb":
		return "cockroachdb"
	case "mongodb", "mongodb+srv":
		return "mongodb"
	case "mssql", "sqlserver":
		return "mssql"
	case "sqlite":
		return "sqlite"
	case "redis", "rediss":
		return "redis"
	default:
		return ""
	}
}
