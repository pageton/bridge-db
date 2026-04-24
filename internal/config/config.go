// Package config defines the top-level migration configuration and provides
// loading, validation, and resolution of connection parameters.
package config

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pageton/bridge-db/internal/tunnel"
	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// Top-level configuration
// ---------------------------------------------------------------------------

// MigrationConfig is the root configuration for a migration run.
type MigrationConfig struct {
	Source      ConnectionConfig `yaml:"source" json:"source"`
	Destination ConnectionConfig `yaml:"destination" json:"destination"`
	Pipeline    PipelineConfig   `yaml:"pipeline" json:"pipeline"`
	Transform   TransformConfig  `yaml:"transform" json:"transform"`
	Checkpoint  CheckpointConfig `yaml:"checkpoint" json:"checkpoint"`
	Logging     LoggingConfig    `yaml:"logging" json:"logging"`
}

// DefaultMigrationConfig returns a MigrationConfig with sensible defaults.
func DefaultMigrationConfig() MigrationConfig {
	return MigrationConfig{
		Pipeline:   DefaultPipelineConfig(),
		Checkpoint: DefaultCheckpointConfig(),
		Logging:    DefaultLoggingConfig(),
	}
}

// DefaultConfig returns a pointer to a MigrationConfig with sensible defaults.
func DefaultConfig() *MigrationConfig {
	cfg := DefaultMigrationConfig()
	return &cfg
}

// ---------------------------------------------------------------------------
// Connection configuration
// ---------------------------------------------------------------------------

// ConnectionConfig describes a single database connection (source or destination).
// The user may supply either a URL (parsed into provider-specific fields) or
// individual provider fields (which override URL-parsed values).
type ConnectionConfig struct {
	// Provider identifies the database type: "redis", "mongodb", "postgres", "mysql", "mariadb", "cockroachdb", "mssql", "sqlite".
	Provider string `yaml:"provider" json:"provider"`

	// URL is an optional connection URL. If set, it is parsed into the
	// provider-specific config struct. Individual fields below override
	// any values parsed from the URL.
	URL string `yaml:"url" json:"url"`

	// SSH is an optional SSH tunnel configuration for reaching databases
	// behind firewalls.
	SSH *tunnel.Config `yaml:"ssh" json:"ssh"`

	// Provider-specific overrides. Only the struct matching Provider is used.
	// If non-nil, its fields override what was parsed from URL.
	Redis       *RedisConfig       `yaml:"redis" json:"redis"`
	MongoDB     *MongoDBConfig     `yaml:"mongodb" json:"mongodb"`
	Postgres    *PostgresConfig    `yaml:"postgres" json:"postgres"`
	MySQL       *MySQLConfig       `yaml:"mysql" json:"mysql"`
	SQLite      *SQLiteConfig      `yaml:"sqlite" json:"sqlite"`
	MariaDB     *MariaDBConfig     `yaml:"mariadb" json:"mariadb"`
	CockroachDB *CockroachDBConfig `yaml:"cockroachdb" json:"cockroachdb"`
	MSSQL       *MSSQLConfig       `yaml:"mssql" json:"mssql"`
}

// Resolve parses the URL (if set) into the provider-specific config, then
// applies any individual field overrides. After resolution the appropriate
// provider config struct is guaranteed to be non-nil.
func (c *ConnectionConfig) Resolve() error {
	switch c.Provider {
	case "redis":
		return c.resolveRedis()
	case "mongodb":
		return c.resolveMongoDB()
	case "postgres":
		return c.resolvePostgres()
	case "mysql":
		return c.resolveMySQL()
	case "sqlite":
		return c.resolveSQLite()
	case "mariadb":
		return c.resolveMariaDB()
	case "cockroachdb":
		return c.resolveCockroachDB()
	case "mssql":
		return c.resolveMSSQL()
	default:
		return fmt.Errorf("unknown provider %q (available: redis, mongodb, postgres, mysql, mariadb, cockroachdb, mssql, sqlite)", c.Provider)
	}
}

// ResolvedConfig is implemented by each provider-specific config type.
// It provides uniform access to connection parameters for tunnel setup,
// config hashing, and logging.
type ResolvedConfig interface {
	GetHost() string
	GetPort() int
	GetDatabase() string
}

// Resolved returns the active provider config as a ResolvedConfig interface.
// Returns nil if no provider config is set.
func (c *ConnectionConfig) Resolved() ResolvedConfig {
	switch c.Provider {
	case "redis":
		return c.Redis
	case "mongodb":
		return c.MongoDB
	case "postgres":
		return c.Postgres
	case "mysql":
		return c.MySQL
	case "mariadb":
		return c.MariaDB
	case "cockroachdb":
		return c.CockroachDB
	case "mssql":
		return c.MSSQL
	case "sqlite":
		return c.SQLite
	}
	return nil
}

func (c *ConnectionConfig) resolveRedis() error {
	cfg, err := resolve(c.URL, "redis", ParseRedisURL, DefaultRedisConfig, c.Redis)
	if err != nil {
		return err
	}
	c.Redis = cfg
	return nil
}

func (c *ConnectionConfig) resolveMongoDB() error {
	cfg, err := resolve(c.URL, "mongodb", ParseMongoDBURL, DefaultMongoDBConfig, c.MongoDB)
	if err != nil {
		return err
	}
	c.MongoDB = cfg
	return nil
}

func (c *ConnectionConfig) resolvePostgres() error {
	cfg, err := resolve(c.URL, "postgres", ParsePostgresURL, DefaultPostgresConfig, c.Postgres)
	if err != nil {
		return err
	}
	c.Postgres = cfg
	return nil
}

func (c *ConnectionConfig) resolveMySQL() error {
	cfg, err := resolve(c.URL, "mysql", ParseMySQLURL, DefaultMySQLConfig, c.MySQL)
	if err != nil {
		return err
	}
	c.MySQL = cfg
	return nil
}

func (c *ConnectionConfig) resolveSQLite() error {
	cfg, err := resolve(c.URL, "sqlite", ParseSQLiteURL, DefaultSQLiteConfig, c.SQLite)
	if err != nil {
		return err
	}
	c.SQLite = cfg
	return nil
}

func (c *ConnectionConfig) resolveMariaDB() error {
	cfg, err := resolve(c.URL, "mariadb", ParseMariaDBURL, DefaultMariaDBConfig, c.MariaDB)
	if err != nil {
		return err
	}
	c.MariaDB = cfg
	return nil
}

func (c *ConnectionConfig) resolveCockroachDB() error {
	cfg, err := resolve(c.URL, "cockroachdb", ParseCockroachDBURL, DefaultCockroachDBConfig, c.CockroachDB)
	if err != nil {
		return err
	}
	c.CockroachDB = cfg
	return nil
}

func (c *ConnectionConfig) resolveMSSQL() error {
	cfg, err := resolve(c.URL, "mssql", ParseMSSQLURL, DefaultMSSQLConfig, c.MSSQL)
	if err != nil {
		return err
	}
	c.MSSQL = cfg
	return nil
}

// ---------------------------------------------------------------------------
// Pointer helpers — used for optional config fields
// ---------------------------------------------------------------------------

// BoolPtr returns a pointer to the given bool value.
func BoolPtr(b bool) *bool { return &b }

// IntPtr returns a pointer to the given int value.
func IntPtr(i int) *int { return &i }

// StringPtr returns a pointer to the given string value.
func StringPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// Merge helper: override fields from src into dst.
// ---------------------------------------------------------------------------

// mergeStruct merges src into dst and returns the result.
//
// Presence semantics:
//   - Pointer fields are presence-aware. A nil pointer means "not provided";
//     a non-nil pointer means "explicitly provided", even if it points to a
//     zero value like false, 0, or "".
//   - Value fields preserve the historical behavior for compatibility: the Go
//     zero value means "not provided" and is skipped.
//
// This design lets provider configs opt into explicit zero-value overrides by
// using pointer fields for optional values without forcing a broader schema
// change across the entire config surface.
func mergeStruct[T any](dst, src T) T {
	dstVal := reflect.ValueOf(&dst).Elem()
	srcVal := reflect.ValueOf(&src).Elem()
	for i := range srcVal.NumField() {
		srcField := srcVal.Field(i)
		dstField := dstVal.Field(i)

		switch srcField.Kind() {
		case reflect.Ptr:
			if !srcField.IsNil() {
				cloned := reflect.New(srcField.Elem().Type())
				cloned.Elem().Set(srcField.Elem())
				dstField.Set(cloned)
			}
		case reflect.Map, reflect.Slice:
			if !srcField.IsNil() {
				dstField.Set(srcField)
			}
		default:
			if !srcField.IsZero() {
				dstField.Set(srcField)
			}
		}
	}
	return dst
}

// resolve parses a URL (if set) into a provider-specific config, merges any
// user overrides, validates, and stores the result. This generic helper
// eliminates the repetitive resolve-pattern duplicated across 8 providers.
//
// Parameters:
//   - url: raw connection URL (may be empty)
//   - name: provider name, used only in error messages
//   - parse: URL parser function (e.g. ParseRedisURL)
//   - defaults: default config function (e.g. DefaultRedisConfig)
//   - override: user-supplied override (may be nil)
//
// Returns the resolved config pointer or an error.
func resolve[T any](url, name string, parse func(string) (T, error), defaults func() T, override *T) (*T, error) {
	var cfg T
	if url != "" {
		parsed, err := parse(url)
		if err != nil {
			return nil, fmt.Errorf("%s URL: %w", name, err)
		}
		cfg = parsed
	} else {
		cfg = defaults()
	}
	if override != nil {
		cfg = mergeStruct(cfg, *override)
	}
	// Validate via interface — all config types implement Validator.
	if v, ok := any(cfg).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return nil, err
		}
	}
	return &cfg, nil
}

// ---------------------------------------------------------------------------
// Pipeline configuration
// ---------------------------------------------------------------------------

// PipelineConfig controls migration execution behaviour.
type PipelineConfig struct {
	BatchSize        int                       `yaml:"batch_size" json:"batch_size"`
	DryRun           bool                      `yaml:"dry_run" json:"dry_run"`
	Verify           bool                      `yaml:"verify" json:"verify"`
	ConflictStrategy provider.ConflictStrategy `yaml:"conflict_strategy" json:"conflict_strategy"`
	MigrateSchema    bool                      `yaml:"migrate_schema" json:"migrate_schema"`
	FKHandling       string                    `yaml:"fk_handling" json:"fk_handling"`
	MaxRetries       int                       `yaml:"max_retries" json:"max_retries"`
	MaxPerUnitRetry  int                       `yaml:"max_per_unit_retry" json:"max_per_unit_retry"`
	RetryBackoff     time.Duration             `yaml:"retry_backoff" json:"retrybackoff"`
}

// DefaultPipelineConfig returns a PipelineConfig with sensible defaults.
func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		BatchSize:        1000,
		Verify:           true,
		ConflictStrategy: provider.ConflictOverwrite,
		MigrateSchema:    true,
		FKHandling:       "defer_constraints",
		MaxRetries:       3,
		MaxPerUnitRetry:  0, // 0 = auto-resolved to min(BatchSize, 100)
		RetryBackoff:     500 * time.Millisecond,
	}
}

// Validate checks the pipeline config for errors.
func (c PipelineConfig) Validate() error {
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be positive")
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be non-negative")
	}
	if c.MaxPerUnitRetry < 0 {
		return fmt.Errorf("max_per_unit_retry must be non-negative")
	}
	switch c.ConflictStrategy {
	case provider.ConflictOverwrite, provider.ConflictSkip, provider.ConflictError:
		// valid
	default:
		return fmt.Errorf("invalid conflict_strategy %q", c.ConflictStrategy)
	}
	switch c.FKHandling {
	case "defer_constraints", "ordered", "skip":
		// valid
	default:
		return fmt.Errorf("invalid fk_handling %q (must be defer_constraints, ordered, or skip)", c.FKHandling)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Checkpoint configuration
// ---------------------------------------------------------------------------

// CheckpointConfig controls migration resumability.
type CheckpointConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Path    string `yaml:"path" json:"path"`
}

// DefaultCheckpointConfig returns a CheckpointConfig with sensible defaults.
func DefaultCheckpointConfig() CheckpointConfig {
	return CheckpointConfig{
		Enabled: true,
		Path:    ".bridge-db/checkpoint.json",
	}
}

// ---------------------------------------------------------------------------
// Logging configuration
// ---------------------------------------------------------------------------

// LoggingConfig controls log output.
type LoggingConfig struct {
	Level string `yaml:"level" json:"level"`
	JSON  bool   `yaml:"json" json:"json"`
}

// DefaultLoggingConfig returns a LoggingConfig with sensible defaults.
func DefaultLoggingConfig() LoggingConfig {
	return LoggingConfig{
		Level: "info",
		JSON:  false,
	}
}

// ---------------------------------------------------------------------------
// Transform configuration
// ---------------------------------------------------------------------------

// TransformConfig controls how data is transformed during migration.
// All fields are optional. When unset, sensible defaults apply.
type TransformConfig struct {
	// NullPolicy controls null value handling during transformation.
	// Valid values: "passthrough" (default), "drop", "replace", "error".
	NullPolicy string `yaml:"null_policy" json:"null_policy"`

	// Mappings is a list of field mapping rules, keyed by table name.
	// The key "*" applies to all tables (wildcard).
	Mappings map[string][]FieldMapping `yaml:"mappings" json:"mappings"`
}

// DefaultTransformConfig returns a TransformConfig with sensible defaults.
func DefaultTransformConfig() TransformConfig {
	return TransformConfig{
		NullPolicy: "passthrough",
		Mappings:   map[string][]FieldMapping{},
	}
}

// FieldMapping describes a single field transformation rule.
type FieldMapping struct {
	// Source is the source field/column name. Required.
	Source string `yaml:"source" json:"source"`

	// Destination is the target field name. If empty, defaults to Source.
	Destination string `yaml:"destination" json:"destination"`

	// Action controls what happens to the field.
	// "rename"  -- rename Source to Destination (default if Destination is set)
	// "drop"    -- exclude this field from output
	// "convert" -- apply type coercion using Convert field
	Action string `yaml:"action" json:"action"`

	// Convert specifies a type conversion for "convert" action.
	// Examples: "string", "int", "float", "bool", "timestamp:mysql:postgres"
	Convert string `yaml:"convert" json:"convert"`
}

// Validate checks the transform config for errors.
func (c TransformConfig) Validate() error {
	switch c.NullPolicy {
	case "", "passthrough", "drop", "replace", "error":
		// valid
	default:
		return fmt.Errorf("invalid null_policy %q (must be passthrough, drop, replace, or error)", c.NullPolicy)
	}
	for table, mappings := range c.Mappings {
		for i, m := range mappings {
			if m.Source == "" {
				return fmt.Errorf("transform: mappings.%s[%d]: source is required", table, i)
			}
			switch m.Action {
			case "", "rename", "drop", "convert":
				// valid
			default:
				return fmt.Errorf("transform: mappings.%s[%d]: invalid action %q", table, i, m.Action)
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

// Validate checks the full MigrationConfig for required fields and logical errors.
func Validate(cfg *MigrationConfig) error {
	if cfg.Source.Provider == "" {
		return fmt.Errorf("source provider is required")
	}
	if cfg.Destination.Provider == "" {
		return fmt.Errorf("destination provider is required")
	}
	if err := cfg.Pipeline.Validate(); err != nil {
		return fmt.Errorf("pipeline: %w", err)
	}
	return nil
}

// Resolve parses all connection URLs and applies overrides for both
// source and destination.
func Resolve(cfg *MigrationConfig) error {
	if err := cfg.Source.Resolve(); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if err := cfg.Destination.Resolve(); err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	return nil
}

// IsCrossDB returns true if source and destination use different providers.
func (c *MigrationConfig) IsCrossDB() bool {
	return c.Source.Provider != c.Destination.Provider
}
