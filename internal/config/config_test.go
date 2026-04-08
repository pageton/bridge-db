package config

import (
	"testing"
	"time"

	"github.com/pageton/bridge-db/pkg/provider"
)

func TestDefaultMigrationConfig(t *testing.T) {
	cfg := DefaultMigrationConfig()
	if cfg.Pipeline.BatchSize != 1000 {
		t.Errorf("default batch_size = %d, want 1000", cfg.Pipeline.BatchSize)
	}
	if cfg.Pipeline.Verify != true {
		t.Error("default verify should be true")
	}
	if cfg.Checkpoint.Enabled != true {
		t.Error("default checkpoint enabled should be true")
	}
	if cfg.Logging.Level != "info" {
		t.Errorf("default log level = %q", cfg.Logging.Level)
	}
}

func TestDefaultConfig_ReturnsPointer(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}
	if cfg.Pipeline.BatchSize != 1000 {
		t.Errorf("default batch_size = %d", cfg.Pipeline.BatchSize)
	}
}

func TestPipelineConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     PipelineConfig
		wantErr bool
	}{
		{
			name: "valid defaults",
			cfg:  DefaultPipelineConfig(),
		},
		{
			name:    "zero batch_size",
			cfg:     PipelineConfig{BatchSize: 0, MaxRetries: 1, ConflictStrategy: provider.ConflictOverwrite, FKHandling: "defer_constraints"},
			wantErr: true,
		},
		{
			name:    "negative batch_size",
			cfg:     PipelineConfig{BatchSize: -5, MaxRetries: 1, ConflictStrategy: provider.ConflictOverwrite, FKHandling: "defer_constraints"},
			wantErr: true,
		},
		{
			name:    "negative max_retries",
			cfg:     PipelineConfig{BatchSize: 1, MaxRetries: -1, ConflictStrategy: provider.ConflictOverwrite, FKHandling: "defer_constraints"},
			wantErr: true,
		},
		{
			name:    "invalid conflict_strategy",
			cfg:     PipelineConfig{BatchSize: 1, MaxRetries: 0, ConflictStrategy: "bogus", FKHandling: "defer_constraints"},
			wantErr: true,
		},
		{
			name:    "invalid fk_handling",
			cfg:     PipelineConfig{BatchSize: 1, MaxRetries: 0, ConflictStrategy: provider.ConflictOverwrite, FKHandling: "bogus"},
			wantErr: true,
		},
		{
			name: "conflict_skip",
			cfg:  PipelineConfig{BatchSize: 1, MaxRetries: 0, ConflictStrategy: provider.ConflictSkip, FKHandling: "ordered"},
		},
		{
			name: "conflict_error",
			cfg:  PipelineConfig{BatchSize: 1, MaxRetries: 0, ConflictStrategy: provider.ConflictError, FKHandling: "skip"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_MissingSourceProvider(t *testing.T) {
	cfg := DefaultMigrationConfig()
	cfg.Source.Provider = ""
	cfg.Destination.Provider = "redis"
	if err := Validate(&cfg); err == nil {
		t.Fatal("expected error for missing source provider")
	}
}

func TestValidate_MissingDestProvider(t *testing.T) {
	cfg := DefaultMigrationConfig()
	cfg.Source.Provider = "redis"
	cfg.Destination.Provider = ""
	if err := Validate(&cfg); err == nil {
		t.Fatal("expected error for missing destination provider")
	}
}

func TestValidate_InvalidPipeline(t *testing.T) {
	cfg := DefaultMigrationConfig()
	cfg.Source.Provider = "redis"
	cfg.Destination.Provider = "redis"
	cfg.Pipeline.BatchSize = 0
	if err := Validate(&cfg); err == nil {
		t.Fatal("expected error for invalid pipeline")
	}
}

func TestResolve_UnknownProvider(t *testing.T) {
	cfg := DefaultMigrationConfig()
	cfg.Source.Provider = "unknown"
	cfg.Source.URL = "foo://bar"
	if err := cfg.Source.Resolve(); err == nil {
		t.Fatal("expected error for unknown provider")
	}
}

func TestIsCrossDB(t *testing.T) {
	cfg := DefaultMigrationConfig()
	cfg.Source.Provider = "redis"
	cfg.Destination.Provider = "mysql"
	if !cfg.IsCrossDB() {
		t.Error("expected IsCrossDB = true for different providers")
	}
}

func TestIsCrossDB_SameProvider(t *testing.T) {
	cfg := DefaultMigrationConfig()
	cfg.Source.Provider = "redis"
	cfg.Destination.Provider = "redis"
	if cfg.IsCrossDB() {
		t.Error("expected IsCrossDB = false for same providers")
	}
}

func TestDefaultPipelineConfig_Values(t *testing.T) {
	cfg := DefaultPipelineConfig()
	if cfg.RetryBackoff != 500*time.Millisecond {
		t.Errorf("retry_backoff = %v, want 500ms", cfg.RetryBackoff)
	}
	if cfg.FKHandling != "defer_constraints" {
		t.Errorf("fk_handling = %q", cfg.FKHandling)
	}
	if cfg.MigrateSchema != true {
		t.Error("migrate_schema should be true")
	}
}

func TestDefaultCheckpointConfig(t *testing.T) {
	cfg := DefaultCheckpointConfig()
	if !cfg.Enabled {
		t.Error("checkpoint should be enabled by default")
	}
	if cfg.Path != ".bridge-db/checkpoint.json" {
		t.Errorf("checkpoint path = %q", cfg.Path)
	}
}

func TestDefaultLoggingConfig(t *testing.T) {
	cfg := DefaultLoggingConfig()
	if cfg.Level != "info" {
		t.Errorf("log level = %q", cfg.Level)
	}
	if cfg.JSON {
		t.Error("json logging should be false by default")
	}
}
