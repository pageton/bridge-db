package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pageton/bridge-db/internal/config"
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
	Short: "Bridge-DB: Multi-database migration framework",
	Long: `Bridge-DB is a client-only database migration tool that copies data
between database instances. It supports Redis, MongoDB, PostgreSQL, MySQL,
MariaDB, CockroachDB, MSSQL, and SQLite with optional SSH tunneling for
databases behind firewalls.`,
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
}

// SetVersion sets the version string (called from main via ldflags).
func SetVersion(v string) {
	Version = v
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
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
