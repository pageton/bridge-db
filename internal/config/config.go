// Package config defines the top-level migration configuration and provides
// loading, validation, and resolution of connection parameters.
package config

import (
	"fmt"
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

func (c *ConnectionConfig) resolveRedis() error {
	var cfg RedisConfig
	if c.URL != "" {
		parsed, err := ParseRedisURL(c.URL)
		if err != nil {
			return fmt.Errorf("redis URL: %w", err)
		}
		cfg = parsed
	} else {
		cfg = DefaultRedisConfig()
	}
	if c.Redis != nil {
		cfg = mergeRedis(cfg, *c.Redis)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.Redis = &cfg
	return nil
}

func (c *ConnectionConfig) resolveMongoDB() error {
	var cfg MongoDBConfig
	if c.URL != "" {
		parsed, err := ParseMongoDBURL(c.URL)
		if err != nil {
			return fmt.Errorf("mongodb URL: %w", err)
		}
		cfg = parsed
	} else {
		cfg = DefaultMongoDBConfig()
	}
	if c.MongoDB != nil {
		cfg = mergeMongoDB(cfg, *c.MongoDB)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.MongoDB = &cfg
	return nil
}

func (c *ConnectionConfig) resolvePostgres() error {
	var cfg PostgresConfig
	if c.URL != "" {
		parsed, err := ParsePostgresURL(c.URL)
		if err != nil {
			return fmt.Errorf("postgres URL: %w", err)
		}
		cfg = parsed
	} else {
		cfg = DefaultPostgresConfig()
	}
	if c.Postgres != nil {
		cfg = mergePostgres(cfg, *c.Postgres)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.Postgres = &cfg
	return nil
}

func (c *ConnectionConfig) resolveMySQL() error {
	var cfg MySQLConfig
	if c.URL != "" {
		parsed, err := ParseMySQLURL(c.URL)
		if err != nil {
			return fmt.Errorf("mysql URL: %w", err)
		}
		cfg = parsed
	} else {
		cfg = DefaultMySQLConfig()
	}
	if c.MySQL != nil {
		cfg = mergeMySQL(cfg, *c.MySQL)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.MySQL = &cfg
	return nil
}

func (c *ConnectionConfig) resolveSQLite() error {
	var cfg SQLiteConfig
	if c.URL != "" {
		parsed, err := ParseSQLiteURL(c.URL)
		if err != nil {
			return fmt.Errorf("sqlite URL: %w", err)
		}
		cfg = parsed
	} else {
		cfg = DefaultSQLiteConfig()
	}
	if c.SQLite != nil {
		cfg = mergeSQLite(cfg, *c.SQLite)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.SQLite = &cfg
	return nil
}

func (c *ConnectionConfig) resolveMariaDB() error {
	var cfg MariaDBConfig
	if c.URL != "" {
		parsed, err := ParseMariaDBURL(c.URL)
		if err != nil {
			return fmt.Errorf("mariadb URL: %w", err)
		}
		cfg = parsed
	} else {
		cfg = DefaultMariaDBConfig()
	}
	if c.MariaDB != nil {
		cfg = mergeMariaDB(cfg, *c.MariaDB)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.MariaDB = &cfg
	return nil
}

func (c *ConnectionConfig) resolveCockroachDB() error {
	var cfg CockroachDBConfig
	if c.URL != "" {
		parsed, err := ParseCockroachDBURL(c.URL)
		if err != nil {
			return fmt.Errorf("cockroachdb URL: %w", err)
		}
		cfg = parsed
	} else {
		cfg = DefaultCockroachDBConfig()
	}
	if c.CockroachDB != nil {
		cfg = mergeCockroachDB(cfg, *c.CockroachDB)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.CockroachDB = &cfg
	return nil
}

func (c *ConnectionConfig) resolveMSSQL() error {
	var cfg MSSQLConfig
	if c.URL != "" {
		parsed, err := ParseMSSQLURL(c.URL)
		if err != nil {
			return fmt.Errorf("mssql URL: %w", err)
		}
		cfg = parsed
	} else {
		cfg = DefaultMSSQLConfig()
	}
	if c.MSSQL != nil {
		cfg = mergeMSSQL(cfg, *c.MSSQL)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.MSSQL = &cfg
	return nil
}

// ---------------------------------------------------------------------------
// Merge helpers (override non-zero fields from override into base)
// ---------------------------------------------------------------------------

func mergeRedis(base, override RedisConfig) RedisConfig {
	if override.Host != "" {
		base.Host = override.Host
	}
	if override.Port != 0 {
		base.Port = override.Port
	}
	if override.Username != "" {
		base.Username = override.Username
	}
	if override.Password != "" {
		base.Password = override.Password
	}
	if override.DB != 0 {
		base.DB = override.DB
	}
	if override.TLS {
		base.TLS = true
	}
	return base
}

func mergeMongoDB(base, override MongoDBConfig) MongoDBConfig {
	if override.Host != "" {
		base.Host = override.Host
	}
	if override.Port != 0 {
		base.Port = override.Port
	}
	if override.Username != "" {
		base.Username = override.Username
	}
	if override.Password != "" {
		base.Password = override.Password
	}
	if override.Database != "" {
		base.Database = override.Database
	}
	if override.AuthSource != "" {
		base.AuthSource = override.AuthSource
	}
	if override.TLS {
		base.TLS = true
	}
	return base
}

func mergePostgres(base, override PostgresConfig) PostgresConfig {
	if override.Host != "" {
		base.Host = override.Host
	}
	if override.Port != 0 {
		base.Port = override.Port
	}
	if override.Username != "" {
		base.Username = override.Username
	}
	if override.Password != "" {
		base.Password = override.Password
	}
	if override.Database != "" {
		base.Database = override.Database
	}
	if override.SSLMode != "" {
		base.SSLMode = override.SSLMode
	}
	return base
}

func mergeMySQL(base, override MySQLConfig) MySQLConfig {
	if override.Host != "" {
		base.Host = override.Host
	}
	if override.Port != 0 {
		base.Port = override.Port
	}
	if override.Username != "" {
		base.Username = override.Username
	}
	if override.Password != "" {
		base.Password = override.Password
	}
	if override.Database != "" {
		base.Database = override.Database
	}
	return base
}

func mergeSQLite(base, override SQLiteConfig) SQLiteConfig {
	if override.Path != "" {
		base.Path = override.Path
	}
	return base
}

func mergeMariaDB(base, override MariaDBConfig) MariaDBConfig {
	if override.Host != "" {
		base.Host = override.Host
	}
	if override.Port != 0 {
		base.Port = override.Port
	}
	if override.Username != "" {
		base.Username = override.Username
	}
	if override.Password != "" {
		base.Password = override.Password
	}
	if override.Database != "" {
		base.Database = override.Database
	}
	return base
}

func mergeCockroachDB(base, override CockroachDBConfig) CockroachDBConfig {
	if override.Host != "" {
		base.Host = override.Host
	}
	if override.Port != 0 {
		base.Port = override.Port
	}
	if override.Username != "" {
		base.Username = override.Username
	}
	if override.Password != "" {
		base.Password = override.Password
	}
	if override.Database != "" {
		base.Database = override.Database
	}
	if override.SSLMode != "" {
		base.SSLMode = override.SSLMode
	}
	return base
}

func mergeMSSQL(base, override MSSQLConfig) MSSQLConfig {
	if override.Host != "" {
		base.Host = override.Host
	}
	if override.Port != 0 {
		base.Port = override.Port
	}
	if override.Username != "" {
		base.Username = override.Username
	}
	if override.Password != "" {
		base.Password = override.Password
	}
	if override.Database != "" {
		base.Database = override.Database
	}
	if override.Instance != "" {
		base.Instance = override.Instance
	}
	if override.Encrypt {
		base.Encrypt = true
	}
	if override.TrustCert {
		base.TrustCert = true
	}
	return base
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
	RetryBackoff     time.Duration             `yaml:"retry_backoff" json:"retry_backoff"`
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
