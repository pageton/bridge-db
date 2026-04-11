package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pageton/bridge-db/internal/config"
	"gopkg.in/yaml.v3"
)

// ConfigTemplateSummary is a lightweight descriptor for a config template file.
type ConfigTemplateSummary struct {
	Name            string `json:"name"`
	SourceProvider  string `json:"source_provider"`
	DestProvider    string `json:"dest_provider"`
	Description     string `json:"description"`
	Path            string `json:"path"`
	CrossDB         bool   `json:"cross_db"`
	SchemaMigration bool   `json:"schema_migration"`
}

// ConfigTemplateDetail is the full config template content plus metadata.
type ConfigTemplateDetail struct {
	Name            string               `json:"name"`
	SourceProvider  string               `json:"source_provider"`
	DestProvider    string               `json:"dest_provider"`
	Description     string               `json:"description"`
	Path            string               `json:"path"`
	CrossDB         bool                 `json:"cross_db"`
	SchemaMigration bool                 `json:"schema_migration"`
	Raw             string               `json:"raw"`
	Config          *config.MigrationConfig `json:"config"`
}

// ConfigValidationResult holds the outcome of validating a config.
type ConfigValidationResult struct {
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// ConfigService exposes config file operations: template listing, reading,
// and validation.
type ConfigService struct {
	// ConfigsDir is the filesystem path to the config templates directory.
	// Defaults to "configs/" relative to the working directory.
	ConfigsDir string
}

// ListTemplates discovers YAML config templates in the configured directory.
func (s ConfigService) ListTemplates(_ context.Context) ([]ConfigTemplateSummary, error) {
	dir := s.dir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read configs dir: %w", err)
	}

	var out []ConfigTemplateSummary
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		if entry.Name() == "multi-hop.yaml" {
			continue // documentation-only, not a runnable config
		}
		path := filepath.Join(dir, entry.Name())
		summary, err := parseSummary(path)
		if err != nil {
			continue // skip unparseable files
		}
		out = append(out, *summary)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// GetTemplate loads a full config template by filename.
func (s ConfigService) GetTemplate(_ context.Context, name string) (*ConfigTemplateDetail, error) {
	dir := s.dir()
	path := filepath.Join(dir, name)

	// Prevent path traversal.
	if strings.Contains(name, "..") || strings.Contains(name, "/") {
		return nil, fmt.Errorf("invalid template name %q", name)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read template %q: %w", name, err)
	}

	cfg := config.DefaultMigrationConfig()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse template %q: %w", name, err)
	}
	normalizeNilMaps(&cfg)

	return &ConfigTemplateDetail{
		Name:            name,
		SourceProvider:  cfg.Source.Provider,
		DestProvider:    cfg.Destination.Provider,
		Description:     extractDescription(data),
		Path:            path,
		CrossDB:         cfg.Source.Provider != "" && cfg.Destination.Provider != "" && cfg.Source.Provider != cfg.Destination.Provider,
		SchemaMigration: cfg.Pipeline.MigrateSchema,
		Raw:             string(data),
		Config:          &cfg,
	}, nil
}

// ValidateConfig validates an inline YAML config string without executing anything.
func (s ConfigService) ValidateConfig(_ context.Context, yamlContent string) (*ConfigValidationResult, error) {
	result := &ConfigValidationResult{Valid: true}

	cfg := config.DefaultMigrationConfig()
	if err := yaml.Unmarshal([]byte(yamlContent), &cfg); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("YAML parse error: %v", err))
		return result, nil
	}

	// Structural validation.
	if err := config.Validate(&cfg); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, err.Error())
	}

	// Resolution validation (URL parsing, provider checks).
	if err := config.Resolve(&cfg); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, err.Error())
	}

	// Warnings.
	if cfg.Source.Provider != "" && cfg.Destination.Provider != "" &&
		cfg.Source.Provider == cfg.Destination.Provider {
		result.Warnings = append(result.Warnings, "source and destination providers are identical — ensure they point to different databases")
	}

	return result, nil
}

// LoadConfigFromYAML parses a YAML string into a fully resolved MigrationConfig.
// This is used when MCP tools receive config as inline YAML.
func LoadConfigFromYAML(yamlContent string) (*config.MigrationConfig, error) {
	cfg := config.DefaultMigrationConfig()
	if err := yaml.Unmarshal([]byte(yamlContent), &cfg); err != nil {
		return nil, fmt.Errorf("parse YAML config: %w", err)
	}
	if err := config.Validate(&cfg); err != nil {
		return nil, err
	}
	if err := config.Resolve(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (s ConfigService) dir() string {
	if s.ConfigsDir != "" {
		return s.ConfigsDir
	}
	// Walk up from working directory to find configs/.
	dir := "."
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "configs")); err == nil {
			return filepath.Join(dir, "configs")
		}
		dir = filepath.Join(dir, "..")
	}
	return "configs"
}

// parseSummary reads a config file and extracts its summary metadata.
func parseSummary(path string) (*ConfigTemplateSummary, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := config.DefaultMigrationConfig()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	name := filepath.Base(path)
	return &ConfigTemplateSummary{
		Name:            name,
		SourceProvider:  cfg.Source.Provider,
		DestProvider:    cfg.Destination.Provider,
		Description:     extractDescription(data),
		Path:            path,
		CrossDB:         cfg.Source.Provider != "" && cfg.Destination.Provider != "" && cfg.Source.Provider != cfg.Destination.Provider,
		SchemaMigration: cfg.Pipeline.MigrateSchema,
	}, nil
}

// extractDescription returns the first non-empty comment line from a YAML file.
func extractDescription(data []byte) string {
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || !strings.HasPrefix(trimmed, "#") {
			continue
		}
		desc := strings.TrimSpace(strings.TrimPrefix(trimmed, "#"))
		// Skip separator lines.
		if strings.HasPrefix(desc, "---") || strings.HasPrefix(desc, "===") {
			continue
		}
		if desc != "" {
			return desc
		}
	}
	return ""
}

// normalizeNilMaps ensures map fields are never nil so they serialize as
// empty objects rather than JSON null (which the MCP SDK rejects).
func normalizeNilMaps(cfg *config.MigrationConfig) {
	if cfg.Transform.Mappings == nil {
		cfg.Transform.Mappings = map[string][]config.FieldMapping{}
	}
}
