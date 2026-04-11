# MCP Server

bridge-db exposes a [Model Context Protocol](https://modelcontextprotocol.io) server that lets AI assistants inspect schemas, migrate data, and verify results directly.

For the full-system redesign proposal that turns MCP into a first-class interface over the core engine, see [MCP System Design](./mcp-system-design.md).

## Error model

MCP tools return structured JSON errors inside tool responses instead of relying on transport-level MCP errors for normal operational failures.

Error objects use this shape:

```json
{
  "error": {
    "code": "BRIDGE_CONNECTION_ERROR",
    "category": "connection",
    "phase": "connect",
    "provider": "postgres",
    "provider_role": "source",
    "retryable": true,
    "human_message": "failed to connect to source database",
    "technical_detail": "dial tcp 127.0.0.1:5432: connect: connection refused"
  }
}
```

This makes tool responses stable for assistants and automation:

- validation and runtime failures remain machine-readable
- callers do not need to parse plain-text error strings
- invalid resume attempts can return structured results without being treated as protocol failures

## Starting the server

```sh
# stdio transport (default, for local clients)
bridge mcp

# Streamable HTTP transport (for remote/network access)
bridge mcp --transport http --addr :8080
```

### Flags

| Flag          | Default | Description                                   |
| ------------- | ------- | --------------------------------------------- |
| `--transport` | `stdio` | Transport protocol: `stdio` or `http`         |
| `--addr`      | `:8080` | HTTP listen address (when `--transport=http`) |
| `--log-level` | `info`  | Log level (inherited from root command)       |
| `--log-json`  | `false` | JSON log output (inherited from root command) |

## Available tools

| Tool             | Description                                                                                                                       |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `list_providers` | List compiled-in database providers and their capabilities. No connection needed.                                                 |
| `get_provider_capabilities` | Return structured capability metadata for a single compiled-in provider.                                              |
| `list_config_templates` | List available migration config templates from the `configs/` directory. Each template is a pre-built YAML for a specific source→destination pair. |
| `get_config_template` | Read a full config template by filename. Returns raw YAML, parsed config, and metadata. |
| `validate_config` | Validate a YAML migration config without executing anything. Returns structured errors and warnings. |
| `inspect_schema` | Connect to a database and return its schema (tables, columns, indexes).                                                           |
| `plan_migration` | Build a structured migration plan without transferring data. Returns tables, estimates, type mappings, unsupported fields, and warnings. |
| `explain_migration_plan` | Convert a structured migration plan into a human-friendly explanation for assistants and users.                     |
| `run_migration` | Start a tracked migration run and return a `run_id` that can be polled later. |
| `get_migration_status` | Return the latest status, phase, progress, and result for a tracked migration run. |
| `resume_migration` | Validate checkpoint compatibility and resume a tracked migration run from saved checkpoint state. |
| `list_migration_runs` | List all tracked migration runs (in-memory and persisted) with summary status. |
| `inspect_checkpoint` | Inspect a checkpoint file and return structured state including tables completed, batch position, and resumability. |
| `migrate`        | Run a synchronous migration and return inline results. Simpler than `run_migration` when you don't need run tracking or async polling. |
| `verify`         | Verify data integrity between source and destination by comparing row counts, sampling records, and checking checksums.           |
| `dry_run`        | Preview a migration plan without writing data. Returns a flat summary of tables, estimates, type mappings, and warnings.          |

### Tool parameters

All tools that connect to a database accept a `provider` + `url` pair. The provider is auto-detected from the URL scheme if omitted.

#### Config templates

**`list_config_templates` input** (no parameters):

```json
{}
```

Returns an array of template summaries:

```json
{
  "templates": [
    {
      "name": "postgres-to-mysql.yaml",
      "source_provider": "postgres",
      "dest_provider": "mysql",
      "description": "Migrate a PostgreSQL database to MySQL.",
      "path": "configs/postgres-to-mysql.yaml",
      "cross_db": true,
      "schema_migration": true
    }
  ]
}
```

**`get_config_template` input:**

```json
{
  "name": "postgres-to-mysql.yaml"
}
```

Returns the full template including raw YAML, parsed config, and metadata.

**`validate_config` input:**

```json
{
  "config_yaml": "source:\n  provider: postgres\n  url: \"postgres://...\"\ndestination:\n  provider: mysql\n  url: \"mysql://...\"\npipeline:\n  batch_size: 1000\n"
}
```

Returns a validation result:

```json
{
  "result": {
    "valid": true,
    "errors": [],
    "warnings": []
  }
}
```

#### Migration tools

`plan_migration`, `run_migration`, `resume_migration`, `migrate`, and `dry_run` accept three input modes:

1. **Structured fields** (original): provide `source`, `destination`, and pipeline options directly.
2. **Config file**: set `config_path` to load from a YAML file (e.g. `configs/example.yaml`). Individual fields override file values.
3. **Inline YAML**: set `config_yaml` to provide the full config as a string. Individual fields override YAML values.

**Structured fields input:**

```json
{
  "source": {
    "provider": "postgres",
    "url": "postgresql://user:pass@localhost:5432/myapp"
  },
  "destination": {
    "provider": "mysql",
    "url": "mysql://root@localhost:3306/myapp"
  },
  "batch_size": 1000,
  "verify": true,
  "migrate_schema": true,
  "on_conflict": "overwrite",
  "fk_handling": "defer_constraints"
}
```

**Config file input:**

```json
{
  "config_path": "configs/postgres-to-mysql.yaml",
  "source": {
    "url": "postgresql://prod-user:pass@prod-host:5432/myapp"
  }
}
```

**Inline YAML input:**

```json
{
  "config_yaml": "source:\n  provider: postgres\n  url: \"postgres://...\"\ndestination:\n  provider: mysql\n  url: \"mysql://...\"\npipeline:\n  batch_size: 500\n"
}
```

#### Verification

**`verify` input:**

```json
{
  "source": { "provider": "postgres", "url": "postgresql://..." },
  "destination": { "provider": "mysql", "url": "mysql://..." },
  "sample_mode": "pct",
  "sample_pct": 5.0,
  "no_checksum": false,
  "counts_only": false
}
```

#### Schema inspection

**`inspect_schema` input:**

```json
{
  "connection": { "provider": "postgres", "url": "postgresql://..." }
}
```

## Connecting from AI clients

### Claude Code

**CLI (recommended):**

```sh
claude mcp add --transport stdio bridge-db -- bridge mcp
```

**Project file** (`.mcp.json` in project root, shared via VCS):

```json
{
  "mcpServers": {
    "bridge-db": {
      "command": "bridge",
      "args": ["mcp"]
    }
  }
}
```

**Remote HTTP:**

```sh
# Start the server on a remote machine
bridge mcp --transport http --addr :8080

# Connect from Claude Code
claude mcp add --transport http bridge-db https://your-server:8080/mcp
```

For more details, see the [Claude Code MCP docs](https://code.claude.com/docs/en/mcp).

### Cursor

Create or edit `.cursor/mcp.json` in your project root:

```json
{
  "mcpServers": {
    "bridge-db": {
      "command": "bridge",
      "args": ["mcp"]
    }
  }
}
```

For a global config, use `~/.cursor/mcp.json`. After editing, reload the window with `Cmd+Shift+P` → "Reload Window".

For remote access, use a URL-based config:

```json
{
  "mcpServers": {
    "bridge-db": {
      "url": "http://your-server:8080/mcp"
    }
  }
}
```

See the [Cursor MCP docs](https://docs.cursor.com/context/model-context-protocol) for more.

### OpenCode

Add an MCP server entry to your `opencode.json` (project root or `~/.config/opencode/`):

```json
{
  "mcp": [
    {
      "type": "local",
      "command": ["bridge", "mcp"],
      "enabled": true
    }
  ]
}
```

For a remote server:

```json
{
  "mcp": [
    {
      "type": "remote",
      "url": "http://your-server:8080/mcp",
      "enabled": true
    }
  ]
}
```

Toggle MCP servers at runtime with the `/mcps` command in the TUI.

### OpenAI Codex CLI

Add a server entry to `.codex/config.toml` (project) or `~/.codex/config.toml` (global):

```toml
[mcp_servers.bridge-db]
command = "bridge"
args = ["mcp"]
enabled = true
```

For a remote server:

```toml
[mcp_servers.bridge-db]
url = "http://your-server:8080/mcp"
enabled = true
```

Manage servers at runtime with `codex mcp` or the `/mcp` command in the TUI.

### Any MCP-compatible client

bridge-db uses the standard MCP protocol over stdio and Streamable HTTP. If your client supports MCP, point it at:

- **stdio**: Run `bridge mcp` as the command
- **HTTP**: Connect to `http://<host>:8080/mcp`

## Example usage

Once connected, you can ask your AI assistant to:

```
What database providers are available?
```

```
Show me available migration config templates.
```

```
Read the postgres-to-mysql config template so I can customize it for my migration.
```

```
Validate this config before I run it:
source:
  provider: postgres
  url: "postgres://user:pass@localhost:5432/myapp"
destination:
  provider: mysql
  url: "mysql://root@localhost:3306/myapp"
pipeline:
  batch_size: 500
```

```
Inspect the schema of my PostgreSQL database at postgresql://user:pass@localhost:5432/myapp
```

```
Migrate all data from my MySQL database to PostgreSQL. Source: mysql://root@localhost:3306/legacy.
Destination: postgresql://admin@localhost:5432/newapp.
```

```
Run a migration using the config file configs/postgres-to-mysql.yaml, but override the source URL.
```

```
Verify that the migration from SQLite to PostgreSQL was successful.
```

```
Show me what would happen if I migrated from MongoDB to PostgreSQL. Just a dry run.
```
