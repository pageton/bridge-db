package cli

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/pageton/bridge-db/internal/app"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	verifypkg "github.com/pageton/bridge-db/internal/verify"
)

var (
	// Verify-specific flags
	verifySampleMode string
	verifySampleN    int
	verifySamplePct  float64
	verifySeed       int64
	verifyNoChecksum bool
	verifyCountsOnly bool
	verifyOutput     string
	verifyMaxKeys    int
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify data integrity between source and destination",
	Long: `Verify that data was correctly migrated by comparing row counts,
document counts, or key counts between source and destination databases.
Optionally sample and compare individual records with checksums.

Examples:
  # Verify postgres → mysql migration
  bridge verify \
    --source-provider postgres --source-host localhost --source-port 5432 \
    --source-user postgres --source-pass pass --source-db myapp \
    --dest-provider mysql --dest-host localhost --dest-port 3306 \
    --dest-user root --dest-pass pass --dest-db myapp

  # Verify with JSON output
  bridge verify --verify-output json ...`,
	RunE: runVerify,
}

func init() {
	// Source connection flags
	verifyCmd.Flags().StringVar(&sourceURL, "source-url", "", "source database connection URL")
	verifyCmd.Flags().StringVar(&sourceProvider, "source-provider", "", "source provider (redis, mongodb, postgres, mysql, mariadb, cockroachdb, mssql, sqlite)")
	verifyCmd.Flags().StringVar(&sourceHost, "source-host", "", "source database host")
	verifyCmd.Flags().IntVar(&sourcePort, "source-port", 0, "source database port")
	verifyCmd.Flags().StringVar(&sourceUser, "source-user", "", "source database username")
	verifyCmd.Flags().StringVar(&sourcePass, "source-pass", "", "source database password")
	verifyCmd.Flags().StringVar(&sourceDB, "source-db", "", "source database name")

	// Source SSH flags
	verifyCmd.Flags().StringVar(&sourceSSHHost, "source-ssh-host", "", "source SSH tunnel host")
	verifyCmd.Flags().IntVar(&sourceSSHPort, "source-ssh-port", 22, "source SSH tunnel port")
	verifyCmd.Flags().StringVar(&sourceSSHUser, "source-ssh-user", "", "source SSH tunnel username")
	verifyCmd.Flags().StringVar(&sourceSSHKey, "source-ssh-key", "", "source SSH private key file path")
	verifyCmd.Flags().StringVar(&sourceSSHPass, "source-ssh-password", "", "source SSH password")
	verifyCmd.Flags().BoolVar(&sourceSSHInsecure, "source-ssh-insecure", false, "skip SSH host key verification")

	// Destination connection flags
	verifyCmd.Flags().StringVar(&destURL, "dest-url", "", "destination database connection URL")
	verifyCmd.Flags().StringVar(&destProvider, "dest-provider", "", "destination provider (redis, mongodb, postgres, mysql, mariadb, cockroachdb, mssql, sqlite)")
	verifyCmd.Flags().StringVar(&destHost, "dest-host", "", "destination database host")
	verifyCmd.Flags().IntVar(&destPort, "dest-port", 0, "destination database port")
	verifyCmd.Flags().StringVar(&destUser, "dest-user", "", "destination database username")
	verifyCmd.Flags().StringVar(&destPass, "dest-pass", "", "destination database password")
	verifyCmd.Flags().StringVar(&destDB, "dest-db", "", "destination database name")

	// Destination SSH flags
	verifyCmd.Flags().StringVar(&destSSHHost, "dest-ssh-host", "", "destination SSH tunnel host")
	verifyCmd.Flags().IntVar(&destSSHPort, "dest-ssh-port", 22, "destination SSH tunnel port")
	verifyCmd.Flags().StringVar(&destSSHUser, "dest-ssh-user", "", "destination SSH tunnel username")
	verifyCmd.Flags().StringVar(&destSSHKey, "dest-ssh-key", "", "destination SSH private key file path")
	verifyCmd.Flags().StringVar(&destSSHPass, "dest-ssh-password", "", "destination SSH password")
	verifyCmd.Flags().BoolVar(&destSSHInsecure, "dest-ssh-insecure", false, "skip SSH host key verification")

	// Verify-specific flags
	verifyCmd.Flags().StringVar(&verifySampleMode, "verify-sample-mode", "pct", "sampling mode: none, count, pct")
	verifyCmd.Flags().IntVar(&verifySampleN, "verify-sample-n", 100, "records to sample per table (when mode=count)")
	verifyCmd.Flags().Float64Var(&verifySamplePct, "verify-sample-pct", 5.0, "percentage to sample per table (when mode=pct)")
	verifyCmd.Flags().Int64Var(&verifySeed, "verify-seed", 42, "deterministic sampling seed")
	verifyCmd.Flags().BoolVar(&verifyNoChecksum, "verify-no-checksums", false, "disable checksum comparison")
	verifyCmd.Flags().BoolVar(&verifyCountsOnly, "verify-counts-only", false, "only compare row counts, skip sampling")
	verifyCmd.Flags().StringVar(&verifyOutput, "verify-output", "table", "output format: table, json")

	rootCmd.AddCommand(verifyCmd)
}

func runVerify(cmd *cobra.Command, args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Build config from flags (reuses migrate.go's buildConfigFromFlags)
	cfg, err := buildConfigFromFlags()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Initialize logger before any provider work.
	initLogger(cfg.Logging.Level, cfg.Logging.JSON)

	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	if err := config.Resolve(cfg); err != nil {
		return fmt.Errorf("config resolution failed: %w", err)
	}

	logger.L().With("component", "verify").Debug("verification configuration resolved")

	// Build verification options
	opts := verifypkg.DefaultOptions()
	if verifyCountsOnly {
		opts = verifypkg.CountsOnly()
	} else {
		opts.SampleMode = verifySampleMode
		opts.SampleN = verifySampleN
		opts.SamplePct = verifySamplePct
		opts.Seed = verifySeed
		opts.ChecksumComparison = !verifyNoChecksum
	}

	report, err := (app.VerificationService{}).VerifyMigration(ctx, cfg, opts)
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	// Output report
	switch verifyOutput {
	case "json":
		jsonStr, err := verifypkg.FormatJSON(report)
		if err != nil {
			return fmt.Errorf("format report: %w", err)
		}
		fmt.Println(jsonStr)
	default:
		fmt.Print(verifypkg.FormatTable(report))
	}

	if !report.Passed() {
		return fmt.Errorf("verification failed: %d table(s) did not pass", report.FailCount)
	}
	return nil
}

// resolveCliProviderConfig returns the provider-specific config,
// using the tunnel address if available.
func resolveCliProviderConfig(connCfg *config.ConnectionConfig, tunnelAddr string) (any, error) {
	return config.ProviderConfigWithTunnel(connCfg, tunnelAddr)
}
