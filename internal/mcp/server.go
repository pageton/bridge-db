package mcp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	mcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewServer creates an MCP server with all bridge-db tools registered.
func NewServer(version string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "bridge-db",
		Version: version,
	}, &mcp.ServerOptions{
		Instructions: `bridge-db MCP server — migrate, inspect, and verify data across SQL and NoSQL databases.

Tools:
  list_providers — list compiled-in database providers and capabilities
  get_provider_capabilities — return structured metadata for a single provider
  list_config_templates — list available migration config templates
  get_config_template — read a full config template by filename
  validate_config — validate a YAML config without executing anything
  inspect_schema — connect to a database and return its schema
  plan_migration — build a structured migration plan without executing transfer
  explain_migration_plan — convert a migration plan into a human explanation
  run_migration — start a tracked migration run and return a run_id
  get_migration_status — query the latest state of a tracked migration run
  resume_migration — validate checkpoint compatibility and resume a tracked migration run
  list_migration_runs — list all tracked migration runs (in-memory and persisted)
  inspect_checkpoint — inspect a checkpoint file and return its state
  migrate       — synchronous one-shot migration with inline results
  verify        — verify data integrity between source and destination
  dry_run       — preview a migration plan without writing data`,
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_providers",
		Description: "List all database providers compiled into this binary with their capabilities (schema support, transactions, verification level).",
	}, handleListProviders)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_provider_capabilities",
		Description: "Return structured capability metadata for a single compiled-in provider.",
	}, handleGetProviderCapabilities)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_config_templates",
		Description: "List available migration config templates from the configs/ directory. Each template is a pre-built YAML config for a specific source-to-destination provider pair.",
	}, handleListConfigTemplates)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_config_template",
		Description: "Read a full config template by filename. Returns the raw YAML, parsed config, and metadata for a migration config template from the configs/ directory.",
	}, handleGetConfigTemplate)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "validate_config",
		Description: "Validate a YAML migration config without executing anything. Checks provider names, connection URLs, pipeline options, and returns structured errors and warnings.",
	}, handleValidateConfig)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "inspect_schema",
		Description: "Connect to a database and return its schema (tables, columns, indexes). Requires a valid connection URL.",
	}, handleInspectSchema)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "plan_migration",
		Description: "Build a structured migration plan without transferring data. Returns tables, row estimates, type mappings, unsupported fields, and warnings.",
	}, handlePlanMigration)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "explain_migration_plan",
		Description: "Convert a structured migration plan into a human-friendly explanation for assistants and users.",
	}, handleExplainMigrationPlan)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "run_migration",
		Description: "Start a tracked migration run from structured input and return a run_id for later status polling.",
	}, handleRunMigration)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_migration_status",
		Description: "Return the latest status, phase, progress, and result for a tracked migration run.",
	}, handleGetMigrationStatus)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "resume_migration",
		Description: "Validate a checkpoint and resume a tracked migration run from saved checkpoint state.",
	}, handleResumeMigration)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_migration_runs",
		Description: "List all tracked migration runs with summary status, ordered by start time descending.",
	}, handleListMigrationRuns)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "inspect_checkpoint",
		Description: "Inspect a checkpoint file and return structured state including tables completed, batch position, and resumability.",
	}, handleInspectCheckpoint)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "migrate",
		Description: "Run a synchronous migration and return inline results. Simpler than run_migration when you don't need run tracking or async polling.",
	}, handleMigrate)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "verify",
		Description: "Verify data integrity between source and destination by comparing row counts, sampling records, and checking checksums.",
	}, handleVerify)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "dry_run",
		Description: "Preview a migration plan without writing data. Returns a flat summary of tables, estimates, type mappings, and warnings.",
	}, handleDryRun)

	return server
}

// RunStdio starts the MCP server using stdin/stdout (standard transport for
// local clients like Claude Desktop and Cursor).
func RunStdio(ctx context.Context, server *mcp.Server) error {
	return server.Run(ctx, &mcp.StdioTransport{})
}

// RunHTTP starts the MCP server using Streamable HTTP transport for remote or
// network deployments.
func RunHTTP(ctx context.Context, server *mcp.Server, addr string) error {
	handler := mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server {
		return server
	}, nil)

	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.Handle("/mcp/", handler)

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	slog.Info("bridge MCP server listening", "addr", addr, "transport", "http")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}
