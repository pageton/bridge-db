package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/pkg/provider"
)

var (
	cfgFile   string
	sourceURL string
	destURL   string
	logLevel  string
	logJSON   bool
	Version   string
)

// rootCmd is the base command for the bridge CLI.
var rootCmd = &cobra.Command{
	Use:   "bridge",
	Short: "Multi-database migration tool",
	Long: `Bridge-DB copies data between database instances — same-engine
(replication/cloning) or cross-engine (e.g. MySQL to Postgres).

Supported providers:
  SQL:       postgres, mysql, mariadb, cockroachdb, mssql, sqlite
  NoSQL:     mongodb, redis                (requires build tag)

Quick start:
  bridge migrate --source-url postgres:// ... --dest-url mysql:// ...

Use "bridge providers" to list providers compiled into this binary.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file path")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().BoolVar(&logJSON, "log-json", false, "output logs in JSON format")

	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("bridge %s\n", Version)
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "providers",
		Short: "List available database providers",
		Long: `List all database providers compiled into this binary.

Providers are enabled at build time. Base providers (postgres, mysql,
mariadb, cockroachdb) are always available. Additional providers are
enabled via build tags:

  bridge build -tags "mongodb,mssql,sqlite,redis"   # all providers
  bridge build -tags "redis"                         # redis only`,
		Run: func(cmd *cobra.Command, args []string) {
			providers := provider.AvailableProviders()
			if len(providers) == 0 {
				fmt.Println("No providers registered.")
				return
			}
			fmt.Printf("Available providers (%d):\n", len(providers))
			for _, p := range providers {
				fmt.Printf("  %s\n", p)
			}
		},
	})
}

// SetVersion sets the version string (called from main via ldflags).
func SetVersion(v string) {
	Version = v
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		// Provide actionable hints for common configuration errors.
		msg := err.Error()
		switch {
		case strings.Contains(msg, "source provider is required"):
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "  Hint: specify a source with --source-url or --source-provider")
			fmt.Fprintln(os.Stderr, "  Example: --source-url postgres://user:pass@host:5432/db")
		case strings.Contains(msg, "destination provider is required"):
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "  Hint: specify a destination with --dest-url or --dest-provider")
			fmt.Fprintln(os.Stderr, "  Example: --dest-url mysql://user:pass@tcp(host:3306)/db")
		case strings.Contains(msg, "unknown provider"):
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "  Hint: use \"bridge providers\" to see which providers are compiled in.")
			fmt.Fprintln(os.Stderr, "  Heavy providers (mongodb, mssql, sqlite, redis) require build tags.")
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// loadConfig loads configuration from file, flags, or environment.
func loadConfig() (*config.MigrationConfig, error) {
	if cfgFile != "" {
		return config.LoadFromFile(cfgFile)
	}

	// Try environment variables
	cfg, err := config.LoadFromEnv("BRIDGE")
	if err == nil && cfg != nil {
		return cfg, nil
	}

	// Return default config
	return config.DefaultConfig(), nil
}
