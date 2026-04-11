package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestConfigService_ListTemplates(t *testing.T) {
	svc := ConfigService{ConfigsDir: "../../configs"}
	templates, err := svc.ListTemplates(context.Background())
	if err != nil {
		t.Fatalf("ListTemplates: %v", err)
	}
	if len(templates) == 0 {
		t.Fatal("expected at least one config template")
	}
	for _, tmpl := range templates {
		if tmpl.Name == "" {
			t.Fatal("template name should not be empty")
		}
		if tmpl.SourceProvider == "" {
			t.Fatalf("template %q source_provider should not be empty", tmpl.Name)
		}
		if tmpl.DestProvider == "" {
			t.Fatalf("template %q dest_provider should not be empty", tmpl.Name)
		}
		if tmpl.Description == "" {
			t.Fatalf("template %q description should not be empty", tmpl.Name)
		}
	}
}

func TestConfigService_ListTemplates_SkipsMultiHop(t *testing.T) {
	svc := ConfigService{ConfigsDir: "../../configs"}
	templates, err := svc.ListTemplates(context.Background())
	if err != nil {
		t.Fatalf("ListTemplates: %v", err)
	}
	for _, tmpl := range templates {
		if tmpl.Name == "multi-hop.yaml" {
			t.Fatal("multi-hop.yaml should be skipped (documentation-only)")
		}
	}
}

func TestConfigService_GetTemplate(t *testing.T) {
	svc := ConfigService{ConfigsDir: "../../configs"}
	tmpl, err := svc.GetTemplate(context.Background(), "example.yaml")
	if err != nil {
		t.Fatalf("GetTemplate: %v", err)
	}
	if tmpl.Name != "example.yaml" {
		t.Fatalf("expected name example.yaml, got %q", tmpl.Name)
	}
	if tmpl.SourceProvider != "postgres" {
		t.Fatalf("expected source_provider postgres, got %q", tmpl.SourceProvider)
	}
	if tmpl.DestProvider != "mysql" {
		t.Fatalf("expected dest_provider mysql, got %q", tmpl.DestProvider)
	}
	if tmpl.Raw == "" {
		t.Fatal("expected raw YAML content")
	}
	if tmpl.Config == nil {
		t.Fatal("expected parsed config")
	}
}

func TestConfigService_GetTemplate_PathTraversal(t *testing.T) {
	svc := ConfigService{ConfigsDir: "../../configs"}
	_, err := svc.GetTemplate(context.Background(), "../etc/passwd")
	if err == nil {
		t.Fatal("expected error for path traversal")
	}
}

func TestConfigService_GetTemplate_NotFound(t *testing.T) {
	svc := ConfigService{ConfigsDir: "../../configs"}
	_, err := svc.GetTemplate(context.Background(), "nonexistent.yaml")
	if err == nil {
		t.Fatal("expected error for missing template")
	}
}

func TestConfigService_ValidateConfig_Valid(t *testing.T) {
	svc := ConfigService{}
	result, err := svc.ValidateConfig(context.Background(), `
source:
  provider: postgres
  url: "postgres://user:pass@localhost:5432/mydb"
destination:
  provider: cockroachdb
  url: "cockroachdb://user:pass@localhost:26257/mydb"
pipeline:
  batch_size: 1000
  verify: true
`)
	if err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid config, got errors: %v", result.Errors)
	}
}

func TestConfigService_ValidateConfig_InvalidYAML(t *testing.T) {
	svc := ConfigService{}
	result, err := svc.ValidateConfig(context.Background(), `not: valid: yaml: {{{`)
	if err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid config")
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected at least one error")
	}
}

func TestConfigService_ValidateConfig_MissingProvider(t *testing.T) {
	svc := ConfigService{}
	result, err := svc.ValidateConfig(context.Background(), `
source:
  url: "postgres://user:pass@localhost:5432/mydb"
destination:
  url: "mysql://user:pass@tcp(localhost:3306)/mydb"
`)
	if err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid config for missing provider")
	}
}

func TestConfigService_ValidateConfig_SameProviderWarning(t *testing.T) {
	svc := ConfigService{}
	result, err := svc.ValidateConfig(context.Background(), `
source:
  provider: postgres
  url: "postgres://user:pass@localhost:5432/mydb"
destination:
  provider: postgres
  url: "postgres://user:pass@remote:5432/mydb"
pipeline:
  batch_size: 1000
`)
	if err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid config, got errors: %v", result.Errors)
	}
	if len(result.Warnings) == 0 {
		t.Fatal("expected at least one warning for same source/dest provider")
	}
}

func TestLoadConfigFromYAML(t *testing.T) {
	cfg, err := LoadConfigFromYAML(`
source:
  provider: postgres
  url: "postgres://user:pass@localhost:5432/mydb"
destination:
  provider: cockroachdb
  url: "cockroachdb://user:pass@localhost:26257/mydb"
pipeline:
  batch_size: 500
`)
	if err != nil {
		t.Fatalf("LoadConfigFromYAML: %v", err)
	}
	if cfg.Source.Provider != "postgres" {
		t.Fatalf("expected source provider postgres, got %q", cfg.Source.Provider)
	}
	if cfg.Destination.Provider != "cockroachdb" {
		t.Fatalf("expected dest provider cockroachdb, got %q", cfg.Destination.Provider)
	}
	if cfg.Pipeline.BatchSize != 500 {
		t.Fatalf("expected batch_size 500, got %d", cfg.Pipeline.BatchSize)
	}
}

func TestConfigService_DefaultDir(t *testing.T) {
	svc := ConfigService{}
	dir := svc.dir()
	// The default dir walks up to find configs/, so it may be "configs",
	// "../../configs", etc. depending on working directory.
	if dir == "" {
		t.Fatal("expected non-empty configs dir")
	}
	// Verify the resolved directory actually exists.
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("configs dir %q does not exist: %v", dir, err)
	}
}

func TestConfigService_CustomDir(t *testing.T) {
	tmp := t.TempDir()
	svc := ConfigService{ConfigsDir: tmp}
	templates, err := svc.ListTemplates(context.Background())
	if err != nil {
		t.Fatalf("ListTemplates on empty dir: %v", err)
	}
	if len(templates) != 0 {
		t.Fatalf("expected 0 templates in empty dir, got %d", len(templates))
	}
}

func TestExtractDescription(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "# Bridge-DB Example\n", "Bridge-DB Example"},
		{"separator skip", "# ====\n# Real description\n", "Real description"},
		{"empty", "", ""},
		{"no comments", "source:\n  provider: postgres\n", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractDescription([]byte(tt.input))
			if got != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestParseSummary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")
	content := []byte(`# Test Config Description
source:
  provider: postgres
  url: "postgres://localhost/mydb"
destination:
  provider: mysql
  url: "mysql://localhost/mydb"
pipeline:
  batch_size: 1000
`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	summary, err := parseSummary(path)
	if err != nil {
		t.Fatalf("parseSummary: %v", err)
	}
	if summary.Name != "test.yaml" {
		t.Fatalf("expected name test.yaml, got %q", summary.Name)
	}
	if summary.SourceProvider != "postgres" {
		t.Fatalf("expected source_provider postgres, got %q", summary.SourceProvider)
	}
	if summary.DestProvider != "mysql" {
		t.Fatalf("expected dest_provider mysql, got %q", summary.DestProvider)
	}
	if !summary.CrossDB {
		t.Fatal("expected cross_db=true for postgres→mysql")
	}
	if summary.Description != "Test Config Description" {
		t.Fatalf("expected description, got %q", summary.Description)
	}
}
