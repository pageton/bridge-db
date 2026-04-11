package cli

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	mcpsdk "github.com/pageton/bridge-db/internal/mcp"
)

var (
	mcpTransport string
	mcpAddr      string
)

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Start an MCP server exposing bridge-db tools",
	Long: `Start a Model Context Protocol (MCP) server that exposes bridge-db
functionality as tools for AI assistants.

Tools provided:
  list_providers — list available database providers
  inspect_schema — inspect a database schema
  migrate       — migrate data between databases
  verify        — verify data integrity
  dry_run       — simulate a migration

Examples:
  # Stdio transport (for Claude Desktop, Cursor, etc.)
  bridge mcp

  # HTTP transport (for remote access)
  bridge mcp --transport http --addr :8080`,
	RunE: runMCP,
}

func init() {
	mcpCmd.Flags().StringVar(&mcpTransport, "transport", "stdio", "Transport protocol: stdio, http")
	mcpCmd.Flags().StringVar(&mcpAddr, "addr", ":8080", "HTTP listen address (when --transport=http)")
	rootCmd.AddCommand(mcpCmd)
}

func runMCP(cmd *cobra.Command, args []string) error {
	initLogger(logLevel, logJSON)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := mcpsdk.NewServer(Version)

	switch mcpTransport {
	case "stdio":
		return mcpsdk.RunStdio(ctx, server)
	case "http":
		return mcpsdk.RunHTTP(ctx, server, mcpAddr)
	default:
		return fmt.Errorf("unknown transport %q (must be stdio or http)", mcpTransport)
	}
}
