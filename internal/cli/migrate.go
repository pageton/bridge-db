package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/progress"
	"github.com/pageton/bridge-db/internal/tunnel"
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

	// Checkpoint flags
	checkpoint     bool
	checkpointPath string

	resume bool

	parallel int
)

// migrateCmd is the migrate command.
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate data from source to destination database",
	Long: `Migrate data between database instances. Supports same-database
(e.g., redis→redis) and cross-database (e.g., mysql→postgres) migrations.

Examples:
  # Local to local (postgres → mysql)
  bridge migrate \
    --source-url "postgres://postgres:pass@localhost:5432/myapp" \
    --dest-url "mysql://root:pass@tcp(localhost:3306)/myapp"

  # Remote behind firewall → local (SSH tunnel)
  bridge migrate \
    --source-url "postgres://postgres:pass@10.0.1.50:5432/myapp" \
    --source-ssh-host bastion.example.com \
    --source-ssh-key ~/.ssh/id_rsa \
    --dest-url "mysql://root:pass@localhost:3306/myapp"`,
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

	// Checkpoint flags
	migrateCmd.Flags().BoolVar(&checkpoint, "checkpoint", true, "enable checkpoint for resumability")
	migrateCmd.Flags().StringVar(&checkpointPath, "checkpoint-path", ".bridge-db/checkpoint.json", "checkpoint file path")

	// Resume flag
	migrateCmd.Flags().BoolVar(&resume, "resume", false, "resume from last checkpoint")

	// Parallel flag
	migrateCmd.Flags().IntVar(&parallel, "parallel", 4, "number of parallel scan/write buffers")

	rootCmd.AddCommand(migrateCmd)
}

// runMigrate executes the migration.
func runMigrate(cmd *cobra.Command, args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := buildConfigFromFlags()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := config.Resolve(cfg); err != nil {
		return fmt.Errorf("config resolution failed: %w", err)
	}

	reporter := progress.NewConsoleReporter()

	opts := bridge.DefaultPipelineOptions()
	opts.BatchSize = batchSize
	opts.DryRun = dryRun
	opts.Verify = verify
	opts.MigrateSchema = migrateSchema
	opts.MaxRetries = maxRetries
	opts.ConflictStrategy = provider.ConflictStrategy(conflictStrategy)
	opts.FKHandling = fkHandling
	opts.CheckpointEnabled = checkpoint
	opts.Resume = resume
	opts.Parallel = parallel

	pipeline := bridge.NewPipeline(cfg, opts, reporter, nil)
	if pipeline == nil {
		return fmt.Errorf("failed to create migration pipeline: invalid checkpoint path")
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

	// Source individual fields override
	if sourceHost != "" || sourcePort != 0 || sourceUser != "" || sourceDB != "" {
		// Determine provider type and set appropriate config
		switch cfg.Source.Provider {
		case "postgres":
			if cfg.Source.Postgres == nil {
				cfg.Source.Postgres = &config.PostgresConfig{}
			}
			if sourceHost != "" {
				cfg.Source.Postgres.Host = sourceHost
			}
			if sourcePort != 0 {
				cfg.Source.Postgres.Port = sourcePort
			}
			if sourceUser != "" {
				cfg.Source.Postgres.Username = sourceUser
			}
			if sourcePass != "" {
				cfg.Source.Postgres.Password = sourcePass
			}
			if sourceDB != "" {
				cfg.Source.Postgres.Database = sourceDB
			}
		case "mysql":
			if cfg.Source.MySQL == nil {
				cfg.Source.MySQL = &config.MySQLConfig{}
			}
			if sourceHost != "" {
				cfg.Source.MySQL.Host = sourceHost
			}
			if sourcePort != 0 {
				cfg.Source.MySQL.Port = sourcePort
			}
			if sourceUser != "" {
				cfg.Source.MySQL.Username = sourceUser
			}
			if sourcePass != "" {
				cfg.Source.MySQL.Password = sourcePass
			}
			if sourceDB != "" {
				cfg.Source.MySQL.Database = sourceDB
			}
		case "redis":
			if cfg.Source.Redis == nil {
				cfg.Source.Redis = &config.RedisConfig{}
			}
			if sourceHost != "" {
				cfg.Source.Redis.Host = sourceHost
			}
			if sourcePort != 0 {
				cfg.Source.Redis.Port = sourcePort
			}
			if sourcePass != "" {
				cfg.Source.Redis.Password = sourcePass
			}
		case "mongodb":
			if cfg.Source.MongoDB == nil {
				cfg.Source.MongoDB = &config.MongoDBConfig{}
			}
			if sourceHost != "" {
				cfg.Source.MongoDB.Host = sourceHost
			}
			if sourcePort != 0 {
				cfg.Source.MongoDB.Port = sourcePort
			}
			if sourceUser != "" {
				cfg.Source.MongoDB.Username = sourceUser
			}
			if sourcePass != "" {
				cfg.Source.MongoDB.Password = sourcePass
			}
			if sourceDB != "" {
				cfg.Source.MongoDB.Database = sourceDB
			}
		case "sqlite":
			if cfg.Source.SQLite == nil {
				cfg.Source.SQLite = &config.SQLiteConfig{}
			}
			if sourceDB != "" {
				cfg.Source.SQLite.Path = sourceDB
			}
			if sourceHost != "" {
				cfg.Source.SQLite.Path = sourceHost
			}
		case "mariadb":
			if cfg.Source.MariaDB == nil {
				cfg.Source.MariaDB = &config.MariaDBConfig{}
			}
			if sourceHost != "" {
				cfg.Source.MariaDB.Host = sourceHost
			}
			if sourcePort != 0 {
				cfg.Source.MariaDB.Port = sourcePort
			}
			if sourceUser != "" {
				cfg.Source.MariaDB.Username = sourceUser
			}
			if sourcePass != "" {
				cfg.Source.MariaDB.Password = sourcePass
			}
			if sourceDB != "" {
				cfg.Source.MariaDB.Database = sourceDB
			}
		case "cockroachdb":
			if cfg.Source.CockroachDB == nil {
				cfg.Source.CockroachDB = &config.CockroachDBConfig{}
			}
			if sourceHost != "" {
				cfg.Source.CockroachDB.Host = sourceHost
			}
			if sourcePort != 0 {
				cfg.Source.CockroachDB.Port = sourcePort
			}
			if sourceUser != "" {
				cfg.Source.CockroachDB.Username = sourceUser
			}
			if sourcePass != "" {
				cfg.Source.CockroachDB.Password = sourcePass
			}
			if sourceDB != "" {
				cfg.Source.CockroachDB.Database = sourceDB
			}
		case "mssql":
			if cfg.Source.MSSQL == nil {
				cfg.Source.MSSQL = &config.MSSQLConfig{}
			}
			if sourceHost != "" {
				cfg.Source.MSSQL.Host = sourceHost
			}
			if sourcePort != 0 {
				cfg.Source.MSSQL.Port = sourcePort
			}
			if sourceUser != "" {
				cfg.Source.MSSQL.Username = sourceUser
			}
			if sourcePass != "" {
				cfg.Source.MSSQL.Password = sourcePass
			}
			if sourceDB != "" {
				cfg.Source.MSSQL.Database = sourceDB
			}
		}
	}

	// Destination individual fields override
	if destHost != "" || destPort != 0 || destUser != "" || destDB != "" {
		switch cfg.Destination.Provider {
		case "postgres":
			if cfg.Destination.Postgres == nil {
				cfg.Destination.Postgres = &config.PostgresConfig{}
			}
			if destHost != "" {
				cfg.Destination.Postgres.Host = destHost
			}
			if destPort != 0 {
				cfg.Destination.Postgres.Port = destPort
			}
			if destUser != "" {
				cfg.Destination.Postgres.Username = destUser
			}
			if destPass != "" {
				cfg.Destination.Postgres.Password = destPass
			}
			if destDB != "" {
				cfg.Destination.Postgres.Database = destDB
			}
		case "mysql":
			if cfg.Destination.MySQL == nil {
				cfg.Destination.MySQL = &config.MySQLConfig{}
			}
			if destHost != "" {
				cfg.Destination.MySQL.Host = destHost
			}
			if destPort != 0 {
				cfg.Destination.MySQL.Port = destPort
			}
			if destUser != "" {
				cfg.Destination.MySQL.Username = destUser
			}
			if destPass != "" {
				cfg.Destination.MySQL.Password = destPass
			}
			if destDB != "" {
				cfg.Destination.MySQL.Database = destDB
			}
		case "redis":
			if cfg.Destination.Redis == nil {
				cfg.Destination.Redis = &config.RedisConfig{}
			}
			if destHost != "" {
				cfg.Destination.Redis.Host = destHost
			}
			if destPort != 0 {
				cfg.Destination.Redis.Port = destPort
			}
			if destPass != "" {
				cfg.Destination.Redis.Password = destPass
			}
		case "mongodb":
			if cfg.Destination.MongoDB == nil {
				cfg.Destination.MongoDB = &config.MongoDBConfig{}
			}
			if destHost != "" {
				cfg.Destination.MongoDB.Host = destHost
			}
			if destPort != 0 {
				cfg.Destination.MongoDB.Port = destPort
			}
			if destUser != "" {
				cfg.Destination.MongoDB.Username = destUser
			}
			if destPass != "" {
				cfg.Destination.MongoDB.Password = destPass
			}
			if destDB != "" {
				cfg.Destination.MongoDB.Database = destDB
			}
		case "sqlite":
			if cfg.Destination.SQLite == nil {
				cfg.Destination.SQLite = &config.SQLiteConfig{}
			}
			if destDB != "" {
				cfg.Destination.SQLite.Path = destDB
			}
			if destHost != "" {
				cfg.Destination.SQLite.Path = destHost
			}
		case "mariadb":
			if cfg.Destination.MariaDB == nil {
				cfg.Destination.MariaDB = &config.MariaDBConfig{}
			}
			if destHost != "" {
				cfg.Destination.MariaDB.Host = destHost
			}
			if destPort != 0 {
				cfg.Destination.MariaDB.Port = destPort
			}
			if destUser != "" {
				cfg.Destination.MariaDB.Username = destUser
			}
			if destPass != "" {
				cfg.Destination.MariaDB.Password = destPass
			}
			if destDB != "" {
				cfg.Destination.MariaDB.Database = destDB
			}
		case "cockroachdb":
			if cfg.Destination.CockroachDB == nil {
				cfg.Destination.CockroachDB = &config.CockroachDBConfig{}
			}
			if destHost != "" {
				cfg.Destination.CockroachDB.Host = destHost
			}
			if destPort != 0 {
				cfg.Destination.CockroachDB.Port = destPort
			}
			if destUser != "" {
				cfg.Destination.CockroachDB.Username = destUser
			}
			if destPass != "" {
				cfg.Destination.CockroachDB.Password = destPass
			}
			if destDB != "" {
				cfg.Destination.CockroachDB.Database = destDB
			}
		case "mssql":
			if cfg.Destination.MSSQL == nil {
				cfg.Destination.MSSQL = &config.MSSQLConfig{}
			}
			if destHost != "" {
				cfg.Destination.MSSQL.Host = destHost
			}
			if destPort != 0 {
				cfg.Destination.MSSQL.Port = destPort
			}
			if destUser != "" {
				cfg.Destination.MSSQL.Username = destUser
			}
			if destPass != "" {
				cfg.Destination.MSSQL.Password = destPass
			}
			if destDB != "" {
				cfg.Destination.MSSQL.Database = destDB
			}
		}
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

// printSummary prints the migration summary to stdout.
func printSummary(result *bridge.RunResult) {
	fmt.Println()
	fmt.Println("=== Migration Summary ===")
	fmt.Printf("Source:      %s\n", result.SrcProvider)
	fmt.Printf("Destination: %s\n", result.DstProvider)
	fmt.Printf("Duration:    %s\n", result.Summary.Duration)

	if result.Summary.TotalScanned > 0 {
		fmt.Printf("Scanned:     %d\n", result.Summary.TotalScanned)
		fmt.Printf("Written:     %d\n", result.Summary.TotalWritten)
		fmt.Printf("Failed:      %d\n", result.Summary.TotalFailed)
		fmt.Printf("Skipped:     %d\n", result.Summary.TotalSkipped)
	}

	if result.Summary.BytesTransferred > 0 {
		fmt.Printf("Transferred: %d bytes\n", result.Summary.BytesTransferred)
	}

	if len(result.Summary.Errors) > 0 {
		fmt.Printf("\nErrors: %d\n", len(result.Summary.Errors))
	}

	if result.Summary.VerificationOK {
		fmt.Println("\nVerification: PASSED")
	}

	fmt.Println("========================")
}
