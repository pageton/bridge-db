package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pageton/bridge-db/internal/tunnel"
	"github.com/pageton/bridge-db/pkg/provider"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// File loading
// ---------------------------------------------------------------------------

const maxConfigSize = 10 << 20 // 10 MB

// LoadFromFile reads a YAML configuration file and returns a fully
// defaulted, validated, and resolved MigrationConfig.
func LoadFromFile(path string) (*MigrationConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	if len(data) > maxConfigSize {
		return nil, fmt.Errorf("config file too large: %d bytes (max %d)", len(data), maxConfigSize)
	}

	cfg := DefaultMigrationConfig()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if err := finish(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ---------------------------------------------------------------------------
// Environment variable loading
// ---------------------------------------------------------------------------

// LoadFromEnv builds a MigrationConfig from environment variables with the
// given prefix. For example, with prefix "BRIDGE":
//
//	BRIDGE_SOURCE_PROVIDER=redis
//	BRIDGE_SOURCE_URL=redis://localhost:6379/0
//	BRIDGE_DEST_PROVIDER=redis
//	BRIDGE_DEST_URL=redis://remote:6379/0
//	BRIDGE_BATCH_SIZE=500
//	BRIDGE_DRY_RUN=true
func LoadFromEnv(prefix string) (*MigrationConfig, error) {
	cfg := DefaultMigrationConfig()

	getEnv := envHelper(prefix)

	// Source connection
	cfg.Source.Provider = getEnv("SOURCE", "PROVIDER")
	cfg.Source.URL = getEnv("SOURCE", "URL")
	cfg.Source.SSH = sshFromEnv(getEnv, "SOURCE")

	// Destination connection
	cfg.Destination.Provider = getEnv("DEST", "PROVIDER")
	cfg.Destination.URL = getEnv("DEST", "URL")
	cfg.Destination.SSH = sshFromEnv(getEnv, "DEST")

	// Pipeline options
	if v := getEnv("BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Pipeline.BatchSize = n
		}
	}
	if v := getEnv("DRY_RUN"); v != "" {
		cfg.Pipeline.DryRun = isTruthy(v)
	}
	if v := getEnv("VERIFY"); v != "" {
		cfg.Pipeline.Verify = isTruthy(v)
	}
	if v := getEnv("CONFLICT_STRATEGY"); v != "" {
		cfg.Pipeline.ConflictStrategy = provider.ConflictStrategy(strings.ToLower(v))
	}
	if v := getEnv("MIGRATE_SCHEMA"); v != "" {
		cfg.Pipeline.MigrateSchema = isTruthy(v)
	}
	if v := getEnv("FK_HANDLING"); v != "" {
		cfg.Pipeline.FKHandling = strings.ToLower(v)
	}

	// Logging
	if v := getEnv("LOG_LEVEL"); v != "" {
		cfg.Logging.Level = strings.ToLower(v)
	}
	if v := getEnv("LOG_JSON"); v != "" {
		cfg.Logging.JSON = isTruthy(v)
	}

	if err := finish(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ---------------------------------------------------------------------------
// Merge: file + env
// ---------------------------------------------------------------------------

// LoadFromFileAndEnv loads the YAML file first, then overlays any
// non-empty environment variables on top.
func LoadFromFileAndEnv(path, envPrefix string) (*MigrationConfig, error) {
	cfg, err := LoadFromFile(path)
	if err != nil {
		return nil, err
	}

	getEnv := envHelper(envPrefix)

	// Overlay env vars (env overrides file)
	if v := getEnv("SOURCE", "URL"); v != "" {
		cfg.Source.URL = v
	}
	if v := getEnv("DEST", "URL"); v != "" {
		cfg.Destination.URL = v
	}
	if v := getEnv("BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Pipeline.BatchSize = n
		}
	}
	if v := getEnv("DRY_RUN"); v != "" {
		cfg.Pipeline.DryRun = isTruthy(v)
	}

	// Re-resolve since URLs may have changed
	if err := finish(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// finish validates and resolves a config. Shared by all load paths.
func finish(cfg *MigrationConfig) error {
	if err := Validate(cfg); err != nil {
		return err
	}
	return Resolve(cfg)
}

// envHelper returns a function that joins parts with "_" and looks up the
// resulting environment variable.
func envHelper(prefix string) func(parts ...string) string {
	return func(parts ...string) string {
		all := append([]string{prefix}, parts...)
		key := strings.ToUpper(strings.Join(all, "_"))
		val, _ := os.LookupEnv(key)
		return val
	}
}

// sshFromEnv builds a *tunnel.Config from environment variables if the
// SSH_HOST variable is set for the given side. Returns nil otherwise.
func sshFromEnv(getEnv func(...string) string, side string) *tunnel.Config {
	host := getEnv(side, "SSH_HOST")
	if host == "" {
		return nil
	}
	cfg := tunnel.DefaultConfig()
	cfg.Enabled = true
	cfg.Host = host
	cfg.Username = getEnv(side, "SSH_USER")
	cfg.Password = getEnv(side, "SSH_PASSWORD")
	cfg.KeyFile = getEnv(side, "SSH_KEY")
	if v := getEnv(side, "SSH_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Port = n
		}
	}
	cfg.KnownHostsFile = getEnv(side, "SSH_KNOWN_HOSTS")
	if v := getEnv(side, "SSH_INSECURE"); v != "" {
		cfg.Insecure = isTruthy(v)
	}
	return &cfg
}

// isTruthy returns true for "true", "1", "yes" (case-insensitive).
func isTruthy(s string) bool {
	return strings.EqualFold(s, "true") || s == "1" || strings.EqualFold(s, "yes")
}
