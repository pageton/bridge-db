package mcp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/pageton/bridge-db/internal/app"
	_ "github.com/pageton/bridge-db/providers/cockroachdb"
	_ "github.com/pageton/bridge-db/providers/mariadb"
	_ "github.com/pageton/bridge-db/providers/mysql"
	_ "github.com/pageton/bridge-db/providers/postgres"

	"github.com/pageton/bridge-db/pkg/provider"
)

func setupTestClient(t *testing.T) (*mcp.ClientSession, context.Context) {
	t.Helper()

	server := NewServer("test")

	client := mcp.NewClient(&mcp.Implementation{
		Name:    "test-client",
		Version: "0.0.1",
	}, nil)

	t1, t2 := mcp.NewInMemoryTransports()

	ss, err := server.Connect(context.Background(), t1, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Wait() })

	cs, err := client.Connect(context.Background(), t2, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	return cs, context.Background()
}

func TestListTools(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	wantTools := map[string]bool{
		"list_providers":            false,
		"get_provider_capabilities": false,
		"list_config_templates":     false,
		"get_config_template":       false,
		"validate_config":           false,
		"inspect_schema":            false,
		"plan_migration":            false,
		"explain_migration_plan":    false,
		"run_migration":             false,
		"get_migration_status":      false,
		"resume_migration":          false,
		"list_migration_runs":       false,
		"inspect_checkpoint":        false,
		"migrate":                   false,
		"verify":                    false,
		"dry_run":                   false,
	}

	for _, tool := range result.Tools {
		if _, ok := wantTools[tool.Name]; ok {
			wantTools[tool.Name] = true
		}
		if tool.Description == "" {
			t.Errorf("tool %q has no description", tool.Name)
		}
	}

	for name, found := range wantTools {
		if !found {
			t.Errorf("tool %q not found in tools/list response", name)
		}
	}
}

func TestListProviders(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "list_providers",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool list_providers: %v", err)
	}
	if result.IsError {
		t.Fatalf("list_providers returned error")
	}

	// Parse structured output
	if len(result.Content) == 0 {
		t.Fatal("no content in result")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output ListProvidersOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}

	if len(output.Providers) == 0 {
		t.Error("expected at least one provider")
	}

	// Base providers (always compiled) should be present
	providerMap := make(map[string]app.ProviderCapabilities)
	for _, p := range output.Providers {
		providerMap[p.Name] = p
	}

	for _, name := range []string{"postgres", "mysql", "mariadb", "cockroachdb"} {
		info, ok := providerMap[name]
		if !ok {
			t.Errorf("expected provider %q in output", name)
			continue
		}
		if !info.SupportsSchemaMigration {
			t.Errorf("provider %q should have schema support", name)
		}
	}

	// Verify consistency with the provider package
	available := provider.AvailableProviders()
	if len(output.Providers) != len(available) {
		t.Errorf("provider count mismatch: MCP returned %d, package reports %d",
			len(output.Providers), len(available))
	}
}

func TestGetProviderCapabilities(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_provider_capabilities",
		Arguments: map[string]any{
			"provider": "postgres",
		},
	})
	if err != nil {
		t.Fatalf("CallTool get_provider_capabilities: %v", err)
	}
	if result.IsError {
		t.Fatal("get_provider_capabilities returned error")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output GetProviderCapabilitiesOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if output.Provider.Name != "postgres" {
		t.Fatalf("unexpected provider %q", output.Provider.Name)
	}
	if !output.Provider.SupportsSchemaMigration {
		t.Fatal("expected schema migration support")
	}
}

func TestGetProviderCapabilitiesUnknownProviderReturnsStructuredError(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_provider_capabilities",
		Arguments: map[string]any{
			"provider": "definitely-not-a-provider",
		},
	})
	if err != nil {
		t.Fatalf("CallTool get_provider_capabilities: %v", err)
	}
	if result.IsError {
		t.Fatal("expected structured tool response, got MCP error")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output GetProviderCapabilitiesOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if output.Error == nil {
		t.Fatal("expected structured error")
	}
	if output.Error.Category == "" {
		t.Fatal("expected error category")
	}
}

func TestGetMigrationStatusUnknownRunReturnsStructuredError(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_migration_status",
		Arguments: map[string]any{
			"run_id": "run_missing",
		},
	})
	if err != nil {
		t.Fatalf("CallTool get_migration_status: %v", err)
	}
	if result.IsError {
		t.Fatal("expected structured tool response, got MCP error")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output GetMigrationStatusOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if output.Error == nil {
		t.Fatal("expected structured error")
	}
}

func TestRunMigrationAndGetStatus(t *testing.T) {
	cs, ctx := setupTestClient(t)

	runResult, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "run_migration",
		Arguments: map[string]any{
			"source":      map[string]any{"provider": "postgres", "url": "postgres://user:pass@127.0.0.1:1/srcdb?sslmode=disable"},
			"destination": map[string]any{"provider": "mysql", "url": "mysql://user:pass@127.0.0.1:1/dstdb"},
		},
	})
	if err != nil {
		t.Fatalf("CallTool run_migration: %v", err)
	}
	if runResult.IsError {
		t.Fatal("run_migration returned error")
	}

	textContent, ok := runResult.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", runResult.Content[0])
	}

	var output RunMigrationOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal run output: %v", err)
	}
	if output.Run == nil || output.Run.RunID == "" {
		t.Fatal("expected run id")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		statusResult, err := cs.CallTool(ctx, &mcp.CallToolParams{
			Name: "get_migration_status",
			Arguments: map[string]any{
				"run_id": output.Run.RunID,
			},
		})
		if err != nil {
			t.Fatalf("CallTool get_migration_status: %v", err)
		}
		if statusResult.IsError {
			t.Fatal("get_migration_status returned error")
		}

		statusContent, ok := statusResult.Content[0].(*mcp.TextContent)
		if !ok {
			t.Fatalf("unexpected content type: %T", statusResult.Content[0])
		}

		var statusOutput GetMigrationStatusOutput
		if err := json.Unmarshal([]byte(statusContent.Text), &statusOutput); err != nil {
			t.Fatalf("unmarshal status output: %v", err)
		}
		if statusOutput.Run == nil || statusOutput.Run.RunID != output.Run.RunID {
			t.Fatal("expected matching run status")
		}
		if statusOutput.Run.Status == app.RunStatusFailed || statusOutput.Run.Status == app.RunStatusSucceeded || statusOutput.Run.Status == app.RunStatusError {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach terminal state, last status=%s", output.Run.RunID, statusOutput.Run.Status)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestResumeMigrationMissingCheckpoint(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "resume_migration",
		Arguments: map[string]any{
			"source":          map[string]any{"provider": "postgres", "url": "postgres://user:pass@127.0.0.1:1/srcdb?sslmode=disable"},
			"destination":     map[string]any{"provider": "mysql", "url": "mysql://user:pass@127.0.0.1:1/dstdb"},
			"checkpoint_path": filepath.Join(t.TempDir(), "missing-checkpoint.json"),
		},
	})
	if err != nil {
		t.Fatalf("CallTool resume_migration: %v", err)
	}
	if result.IsError {
		t.Fatal("resume_migration returned MCP error")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output ResumeMigrationOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if output.Valid {
		t.Fatal("expected invalid resume")
	}
	if output.Reason == "" {
		t.Fatal("expected invalid reason")
	}
}

func TestListMigrationRuns(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "list_migration_runs",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool list_migration_runs: %v", err)
	}
	if result.IsError {
		t.Fatal("list_migration_runs returned error")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output ListMigrationRunsOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	// Runs may be empty or populated depending on prior tests; just verify shape.
	for _, run := range output.Runs {
		if run.RunID == "" {
			t.Fatal("run summary should have run_id")
		}
	}
}

func TestInspectCheckpointMissingFile(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "inspect_checkpoint",
		Arguments: map[string]any{
			"path": filepath.Join(t.TempDir(), "no-such-checkpoint.json"),
		},
	})
	if err != nil {
		t.Fatalf("CallTool inspect_checkpoint: %v", err)
	}
	if result.IsError {
		t.Fatal("inspect_checkpoint returned MCP error")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output InspectCheckpointOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if output.Error == nil {
		t.Fatal("expected structured error for missing checkpoint")
	}
}

func TestListConfigTemplates(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "list_config_templates",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool list_config_templates: %v", err)
	}
	if result.IsError {
		t.Fatal("list_config_templates returned error")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output ListConfigTemplatesOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if len(output.Templates) == 0 {
		t.Fatal("expected at least one config template")
	}
	for _, tmpl := range output.Templates {
		if tmpl.Name == "" {
			t.Fatal("template name should not be empty")
		}
		if tmpl.SourceProvider == "" {
			t.Fatalf("template %q missing source_provider", tmpl.Name)
		}
	}
}

func TestGetConfigTemplate(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_config_template",
		Arguments: map[string]any{
			"name": "example.yaml",
		},
	})
	if err != nil {
		t.Fatalf("CallTool get_config_template: %v", err)
	}
	if result.IsError {
		t.Fatal("get_config_template returned error")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output GetConfigTemplateOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if output.Template == nil {
		t.Fatal("expected template")
	}
	if output.Template.SourceProvider != "postgres" {
		t.Fatalf("expected source_provider postgres, got %q", output.Template.SourceProvider)
	}
	if output.Template.Raw == "" {
		t.Fatal("expected raw YAML content")
	}
}

func TestGetConfigTemplateNotFound(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_config_template",
		Arguments: map[string]any{
			"name": "nonexistent.yaml",
		},
	})
	if err != nil {
		t.Fatalf("CallTool get_config_template: %v", err)
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output GetConfigTemplateOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if output.Error == nil {
		t.Fatal("expected structured error for missing template")
	}
}

func TestValidateConfig(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "validate_config",
		Arguments: map[string]any{
			"config_yaml": `
source:
  provider: postgres
  url: "postgres://user:pass@localhost:5432/mydb"
destination:
  provider: cockroachdb
  url: "cockroachdb://user:pass@localhost:26257/mydb"
pipeline:
  batch_size: 1000
`,
		},
	})
	if err != nil {
		t.Fatalf("CallTool validate_config: %v", err)
	}
	if result.IsError {
		t.Fatal("validate_config returned error")
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output ValidateConfigOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if output.Result == nil {
		t.Fatal("expected validation result")
	}
	if !output.Result.Valid {
		t.Fatalf("expected valid config, got errors: %v", output.Result.Errors)
	}
}

func TestValidateConfigInvalid(t *testing.T) {
	cs, ctx := setupTestClient(t)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "validate_config",
		Arguments: map[string]any{
			"config_yaml": `
source:
  provider: notreal
destination:
  provider: mysql
`,
		},
	})
	if err != nil {
		t.Fatalf("CallTool validate_config: %v", err)
	}

	textContent, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type: %T", result.Content[0])
	}

	var output ValidateConfigOutput
	if err := json.Unmarshal([]byte(textContent.Text), &output); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if output.Result == nil {
		t.Fatal("expected validation result")
	}
	if output.Result.Valid {
		t.Fatal("expected invalid config")
	}
}
