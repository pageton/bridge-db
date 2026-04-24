package cli

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
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
	maxPerUnitRetry  int
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

// knownProviders lists providers that can be checked without runtime registration.
var knownProviders = map[string]bool{
	"postgres": true, "mysql": true, "mariadb": true, "cockroachdb": true,
	"mongodb": true, "mssql": true, "sqlite": true, "redis": true,
}

// heavyProviders require build tags to be available.
var heavyProviders = map[string]bool{
	"mongodb": true, "mssql": true, "sqlite": true, "redis": true,
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

// initLogger configures the structured logger from the CLI flags.
func initLogger(level string, json bool) {
	var l logger.Level
	switch level {
	case "debug":
		l = logger.LevelDebug
	case "warn":
		l = logger.LevelWarn
	case "error":
		l = logger.LevelError
	default:
		l = logger.LevelInfo
	}
	logger.Init(l, json)
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
				fmt.Fprintf(os.Stderr, "Warning: source %q does not support schema migration -- --migrate-schema will be skipped\n", cfg.Source.Provider)
			} else if !dstCaps.Schema {
				fmt.Fprintf(os.Stderr, "Warning: destination %q does not support schema migration -- --migrate-schema will be skipped\n", cfg.Destination.Provider)
			}
		}
	}

	if len(hints) > 0 {
		return fmt.Errorf("configuration errors:\n  %s", strings.Join(hints, "\n  "))
	}
	return nil
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
