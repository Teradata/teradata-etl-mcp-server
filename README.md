# Teradata ETL MCP Server

A unified Model Context Protocol (MCP) server for comprehensive ELT/ETL operations, integrating Teradata, Airbyte, Apache Airflow, and dbt for end-to-end data pipeline management.


## Table of Contents

- [Quick Start](#quick-start)
- [Features](#features)
- [Architecture](#architecture)
- [SSH Setup](#ssh-setup-bidirectional)
- [Configuration](#configuration)
- [Connection Profiles](#connection-profiles)
- [Usage](#usage)
- [Tool Catalog](#tool-catalog)
- [Development](#development)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Documentation](#documentation)
- [License](#license)

---

## Quick Start

> **Audience**: End users who want to run the MCP server and use it with an LLM client (Copilot, Claude Desktop, Claude Code, etc.).

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.10 -- 3.13 | Required |
| Teradata database | Any supported version | Required for Teradata operations |
| Teradata Tools & Utilities (TTU) | 17.20+ | Required on MCP client for BTEQ/TdLoad/TPT execution |
| OpenSSH client | Any | Required on MCP client for DAG deployment to Airflow |
| OpenSSH server | Any | Required on MCP client if Airflow executes BTEQ/TdLoad remotely via SSH |
| Apache Airflow | 2.x | Optional -- needed for DAG orchestration |
| Airbyte | OSS | Optional -- needed for data replication |
| dbt + dbt-teradata | >=1.7,<2.0 + 0.19.0+ | Optional -- needed for transformations |

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/Teradata/teradata-etl-mcp-server.git
cd teradata-etl-mcp-server

# 2. Create and activate a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate

# 3. Install the package with all extras (includes all optional dependencies)
pip install -e ".[dev,all]"

```

### Post-install Setup

> Create a **separate workspace folder** for configuration files — do not place `.env` or `connections.yaml` inside the source repo (it is protected by pre-commit hooks that block `.env` commits).

```bash
# 4. Create a dedicated workspace folder outside the source repo
mkdir ../teradata-etl-mcp-workspace
cd ../teradata-etl-mcp-workspace

# 5. Copy templates from the source repo
cp ../teradata-etl-mcp-server/.env.example .env
cp ../teradata-etl-mcp-server/connections.yaml.example connections.yaml

# 6. Edit .env with your Teradata, Airflow, Airbyte, and dbt settings
# 7. Edit connections.yaml with your connection profiles (see Connection Profiles section)
```

### Verify Installation

```bash
# Start the server (stdio transport, default)
python -m teradata_etl_mcp_server
```

---

## Features

The server exposes **22 MCP tools** across 7 categories. Each tool is a router that accepts an
`action` or `method` parameter — one tool name covers multiple operations.

| Category | Tools | Description |
|----------|-------|-------------|
| Pipeline Management | 5 | Deploy DAGs, control schedules, manage Airflow connections, validate DAGs |
| Orchestration & Execution | 3 | Trigger DAG runs, monitor status, retry tasks, get logs |
| Data Movement | 5 | Airbyte pipelines, syncs, stream selection, TdLoad/CSV DAG generation |
| dbt Management | 5 | Run/test/build models, generate docs, generate models from metadata |
| Metadata Discovery | 2 | Discover tables, describe schemas, profile data, compare structures |
| Connection Profiles | 1 | List and reload credential profiles (LLM never sees secrets) |
| TTU Management | 1 | Execute DDL via teradatasql, load data via tdload, run BTEQ scripts, check TTU installation |

### Security: Credential Isolation

The LLM **never** sees passwords, tokens, or API keys. All credentials are resolved server-side from `connections.yaml` profiles. The LLM only references profile names:

```
User: "Build a daily ELT pipeline from Postgres to Teradata for customers table"

LLM calls: create_intelligent_airbyte_pipeline(
    source_profile="my_postgres",        # just a name
    destination_profile="prod_teradata", # just a name
    ...
)

Server: resolves credentials from connections.yaml, creates pipeline
Response: sanitized -- LLM sees success status but NO passwords
```

---

## Architecture

```
+---------------------------------------------------------------+
|                       MCP Server Layer                         |
|  +----------------------------------------------------------+ |
|  |  22 Tools (7 Categories) via FastMCP                      | |
|  +----------------------------------------------------------+ |
+---------------------------------------------------------------+
                              |
+---------------------------------------------------------------+
|                   Pipeline Orchestrator                        |
|  +-------------------+ +-------------+ +--------------------+ |
|  | Credential        | | Intelligence| | Code Generators    | |
|  | Resolver          | | Engine      | | (DAG, dbt, TPT,    | |
|  | (connections.yaml)| |             | |  BTEQ, TdLoad)     | |
|  +-------------------+ +-------------+ +--------------------+ |
|  +-----------------+ +---------------+ +--------------------+ |
|  | Response        | | Validators    | | Metadata Store     | |
|  | Sanitizer       | | & Utils       | | (SQLite/JSON)      | |
|  +-----------------+ +---------------+ +--------------------+ |
+---------------------------------------------------------------+
                              |
+---------------------------------------------------------------+
|                        Client Layer                            |
|  +----------+ +----------+ +----------+ +----------+          |
|  | Teradata | | Airflow  | | Airbyte  | |   dbt    |          |
|  | Client   | | Client   | | Client   | | Client   |          |
|  +----------+ +----------+ +----------+ +----------+          |
+---------------------------------------------------------------+
                              |
+---------------------------------------------------------------+
|                      External Systems                          |
|  +----------+ +----------+ +----------+ +----------+          |
|  | Teradata | | Airflow  | | Airbyte  | |   dbt    |          |
|  | Database | | Server   | | Server   | | Project  |          |
|  +----------+ +----------+ +----------+ +----------+          |
+---------------------------------------------------------------+
```

### Key Components

| Component | Description |
|-----------|-------------|
| **FastMCP Server** | MCP protocol layer exposing all tools to LLM clients |
| **Pipeline Orchestrator** | Central coordinator; lazy-loads clients via `@property` |
| **Credential Resolver** | Loads `connections.yaml`, resolves `${ENV_VAR}` interpolation, serves profiles by name |
| **Response Sanitizer** | Deep-clones and masks sensitive keys (password, token, secret, etc.) in all tool responses |
| **Intelligence Engine** | AI-driven transport method selection (Airbyte vs TPT) |
| **Code Generators** | Jinja2-based generators for Airflow DAGs, dbt models, TPT scripts, BTEQ queries |
| **Clients** | Abstraction layer for Teradata, Airflow, Airbyte, and dbt APIs |
| **Metadata Store** | Optional persistence for execution history (SQLite or JSON) |
| **Plugin Manager** | Extensibility framework for custom operators and validators |

---

## SSH Setup (Bidirectional)

For DAG deployment to Airflow and runtime BTEQ/TdLoad/TPT execution, bidirectional SSH is required between the MCP client and Airflow server.

**Complete SSH setup guide:** See [SSH-SETUP.md](SSH-SETUP.md) for detailed platform-specific instructions (Windows, Linux, macOS).

**Key sections covered:**
- Installing SSH client and server
- Generating Ed25519 key pairs (both directions)
- Authorizing keys on both machines
- File permission configuration
- SSH config file setup
- Verification checklist

**Quick reference - environment variables:**
```bash
# MCP Client → Airflow (DAG deployment)
AIRFLOW_REMOTE_HOST=<airflow-host>
AIRFLOW_REMOTE_USER=airflow
AIRFLOW_REMOTE_SSH_KEY=~/.ssh/id_ed25519_airflow

# Airflow → MCP Client (runtime execution)
MCP_CLIENT_SSH_HOST=<your-machine-ip>
MCP_CLIENT_SSH_USER=<your-username>
MCP_CLIENT_SSH_KEY_PATH=~/.ssh/id_ed25519_mcp
```

**Host-key verification:** DAG deployment tools support strict SSH host-key verification via the `strict_host_key_checking` parameter. See [SSH-SETUP.md § SSH Host-Key Verification](SSH-SETUP.md) for security best practices.

---

## Configuration

> **SSH host-key verification** for DAG deployment is controlled per-call via
> the `strict_host_key_checking` tool parameter (default `False`, with a
> WARNING logged on every connection). See
> [SSH-SETUP.md § SSH Host-Key Verification](SSH-SETUP.md) for the threat
> model and upgrade steps.

### Environment Variables (`.env`)

Copy the template and fill in your values:

```bash
cp .env.example .env
```

Key sections in `.env.example`:

| Section | Variable | Description | Required? |
|---------|----------|-------------|-----------|
| **Environment** | `ENVIRONMENT` | Runtime environment: `development`, `staging`, `production` | No (default: `development`) |
| **Teradata** | `TERADATA_HOST` | Teradata database host or IP address | Yes |
| | `TERADATA_USERNAME` | Teradata login username | Yes |
| | `TERADATA_PASSWORD` | Teradata login password | Yes |
| | `TERADATA_DATABASE` | Default database/schema | No |
| | `TERADATA_PORT` | Database port | No (default: `1025`) |
| | `TERADATA_LOGMECH` | Auth mechanism: `TD2`, `LDAP`, `JWT`, `BEARER`, `SECRET` | No (default: `TD2`) |
| **Teradata-to-Teradata** | `TERADATA_SOURCE_HOST` | Source Teradata host (for cross-system transfers) | No |
| | `TERADATA_SOURCE_USERNAME` | Source Teradata username | No |
| | `TERADATA_SOURCE_PASSWORD` | Source Teradata password | No |
| | `TERADATA_SOURCE_DATABASE` | Source Teradata database | No |
| | `TERADATA_TARGET_HOST` | Target Teradata host | No |
| | `TERADATA_TARGET_USERNAME` | Target Teradata username | No |
| | `TERADATA_TARGET_PASSWORD` | Target Teradata password | No |
| | `TERADATA_TARGET_DATABASE` | Target Teradata database | No |
| **Airflow API** | `AIRFLOW_BASE_URL` | Airflow REST API URL (e.g., `http://localhost:8080`) | For orchestration |
| | `AIRFLOW_USERNAME` | Airflow API username | For orchestration |
| | `AIRFLOW_PASSWORD` | Airflow API password | For orchestration |
| | `AIRFLOW_TOKEN_ENDPOINT` | JWT token endpoint | No (default: `/auth/token`) |
| | `AIRFLOW_ACCESS_TOKEN` | Pre-configured Bearer token | No |
| **Airflow DAG Deployment** | `AIRFLOW_REMOTE_HOST` | Airflow server hostname for SSH DAG deployment | For DAG deployment |
| | `AIRFLOW_REMOTE_USER` | SSH username on the Airflow server | For DAG deployment |
| | `AIRFLOW_REMOTE_SSH_KEY` | Path to SSH private key (on this machine) | For DAG deployment |
| | `AIRFLOW_REMOTE_PASSWORD` | SSH password (if not using key auth) | No |
| | `AIRFLOW_REMOTE_PORT` | SSH port on Airflow server | No (default: `22`) |
| | `AIRFLOW_REMOTE_SSH_KEY_PASSPHRASE` | Passphrase for the SSH key | No |
| | `AIRFLOW_DAG_FOLDER` | Remote DAG folder path on the Airflow server | No (default: `/opt/airflow/dags`) |
| **MCP Client SSH** | `MCP_CLIENT_SSH_HOST` | This machine's hostname/IP (Airflow SSHes back here at runtime) | For runtime SSH |
| | `MCP_CLIENT_SSH_USER` | SSH username on this machine | For runtime SSH |
| | `MCP_CLIENT_SSH_PORT` | SSH port on this machine | No (default: `22`) |
| | `MCP_CLIENT_SSH_KEY_PATH` | Path to SSH private key **on the Airflow worker** | For runtime SSH |
| **Airbyte** | `AIRBYTE_ENABLED` | Enable Airbyte integration | No (default: `false`) |
| | `AIRBYTE_BASE_URL` | Airbyte API base URL | When Airbyte enabled |
| | `AIRBYTE_CLIENT_ID` | OAuth2 client ID (from Airbyte Settings > Applications) | No |
| | `AIRBYTE_CLIENT_SECRET` | OAuth2 client secret | No |
| | `AIRBYTE_TOKEN_URL` | OAuth2 token endpoint | No |
| | `AIRBYTE_WORKSPACE_ID` | Default workspace ID (auto-detected if omitted) | No |
| | `AIRBYTE_DEFAULT_NAMESPACE` | Default namespace for connections | No (default: `default`) |
| **dbt** | `DBT_PROJECT_DIR` | Path to dbt project directory | For dbt |
| | `DBT_PROFILES_DIR` | Path to dbt profiles directory | No (default: `~/.dbt`) |
| | `DBT_TARGET` | dbt target environment | No (default: `dev`) |
| | `DBT_THREADS` | Number of threads for dbt execution | No (default: `4`) |
| **Pipeline** | `PIPELINE_DAGS_OUTPUT_DIR` | Directory for generated DAG files | No (default: `./airflow_dags`) |
| | `PIPELINE_DEFAULT_SCHEDULE_INTERVAL` | Default schedule for generated DAGs | No (default: `@daily`) |
| | `PIPELINE_GENERATE_DBT_BY_DEFAULT` | Auto-generate dbt models with pipelines | No (default: `true`) |
| **MCP Server** | `MCP_LOG_LEVEL` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | No (default: `INFO`) |
| | `MCP_LOG_FILE` | Log file path | No (default: `./logs/teradata-etl-mcp-server.log`) |
| | `MCP_FAIL_FAST_ON_STARTUP` | Crash on connectivity failure at startup | No (default: `false`) |
| | `MCP_REDIS_URL` | Redis URL for distributed circuit breaker | No |
| **TTU** | `TTU_ENABLED` | Enable local TPT/BTEQ/TdLoad execution | No (default: `false`) |
| | `TTU_TTU_VERSION` | TTU version (e.g., `17.20`); auto-detected if not set | No |
| | `TTU_TPT_BINARY_PATH` | Path to `tbuild` binary (auto-detected from version) | No |
| | `TTU_BTEQ_BINARY_PATH` | Path to `bteq` binary (auto-detected from version) | No |
| | `TTU_TDLOAD_BINARY_PATH` | Path to `tdload` binary (auto-detected from version) | No |
| | `TTU_SCRIPTS_DIR` | Directory for generated TTU scripts | No (default: `./ttu_scripts`) |
| | `TTU_COMMAND_TIMEOUT` | Subprocess timeout in seconds | No (default: `600`) |
| **Security** | `SECURITY_CONNECTIONS_FILE` | Path to `connections.yaml` for credential profiles | No (auto-discovered) |

---

## Connection Profiles

Connection profiles decouple credentials from LLM interactions. The LLM references profiles by **name**; the server resolves actual credentials at runtime.

### Setup

```bash
cp connections.yaml.example connections.yaml
# Edit connections.yaml with your actual credentials
```

### File Locations (searched in order)

1. Path set via `CONNECTIONS_FILE` environment variable
2. `connections.yaml` in the current working directory
3. `settings.security.connections_file` (if configured in server settings)

### Format

```yaml
version: "1"

profiles:
  postgres_prod:
    host: "pg-host.example.com"
    port: 5432
    database: "testdb"
    username: "testuser"
    password: "${POSTGRES_PASSWORD}"   # env var interpolation
    schemas:
      - "public"
    description: "Production Postgres database"

  teradata_prod:
    host: "td-host.example.com"
    port: 1025
    username: "dbc"
    password: "${TERADATA_PASSWORD}"
    default_schema: "analytics_raw"
    description: "Production Teradata destination"

  airflow_ssh:
    host: ${MCP_CLIENT_SSH_HOST}
    port: ${MCP_CLIENT_SSH_PORT}
    username: ${MCP_CLIENT_SSH_USER}
    key_file: ${MCP_CLIENT_SSH_KEY_PATH}
    description: "MCP Client machine — Airflow SSHes here to run BTEQ/TdLoad"

aliases:
  source: "postgres_prod"
  teradata: "teradata_prod"
  ssh: "airflow_ssh"
```

### Key Behaviors

- **`${ENV_VAR}`** values are interpolated at load time
- **`description`** is exposed to the LLM; all other fields are hidden
- **Aliases** let you write `source` instead of `postgres_prod`
- **`connection_profiles(action="list")`** returns names and descriptions only -- no secrets
- **`connection_profiles(action="reload")`** picks up file changes without a server restart

### How Tools Use Profiles

| Tool Parameter | Example Value | Description |
|----------------|--------------|-------------|
| `source_profile` | `"my_postgres"` | Airbyte source credentials |
| `destination_profile` | `"prod_teradata"` | Airbyte destination credentials |
| `source_teradata_profile` | `"td_source"` | TdLoad source Teradata connection |
| `target_teradata_profile` | `"prod_teradata"` | TdLoad target Teradata connection |
| `teradata_profile` | `"prod_teradata"` | Airflow Teradata connection |
| `ssh_profile` | `"airflow_ssh"` | Airflow SSH connection |
| `connection_profile` | `"my_postgres"` | Environment/secrets connection |

---

## Usage

### Starting the Server

```bash
# Start the server (stdio transport — works with any MCP client)
python -m teradata_etl_mcp_server

# Or using the console script
teradata-etl-mcp-server
```

### Using with Claude Desktop

Add to your Claude Desktop configuration (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "teradata-etl-mcp": {
      "command": "teradata-etl-mcp-server",
      "args": ["--env-file", "/absolute/path/to/teradata-etl-mcp-workspace/.env"]
    }
  }
}
```

> Using `--env-file` keeps all credentials in `.env` only — nothing sensitive in `claude_desktop_config.json`.

### Using with VS Code

Add to your VS Code MCP configuration (`.vscode/mcp.json` in your workspace):

```json
{
  "servers": {
    "teradata-etl-mcp": {
      "command": "teradata-etl-mcp-server",
      "args": ["--env-file", "/absolute/path/to/teradata-etl-mcp-workspace/.env"]
    }
  }
}
```

> Use an absolute path to `.env`. On Windows use forward slashes or escaped backslashes: `C:/Users/you/teradata-etl-mcp-workspace/.env`.

### Example: Create an Airbyte Pipeline

```
User: "Build a daily ELT pipeline from Postgres to Teradata
       for customers and orders tables, incremental sync, daily at 02:00 UTC"

The LLM will:
1. Call list_connection_profiles() to discover available profiles
2. Call airbyte_pipeline(
       action="create",
       source_name="postgres_source",
       source_type="Postgres",
       source_profile="source",
       destination_name="teradata_dest",
       destination_type="Teradata",
       destination_profile="target",
       streams=[{"name": "customers"}, {"name": "orders"}],
       schedule_type="cron",
       schedule_cron="0 2 * * *"
   )
```

### Example: Generate a TdLoad DAG

```
User: "Create a table transfer DAG from staging to production
       for the sales_data table, daily at 3 AM"

The LLM will call:
  airflow_teradata_load(
      method="table_transfer",
      dag_id="transfer_sales_data",
      source_teradata_profile="td_source",
      target_teradata_profile="teradata_prod",
      source_database="staging_db",
      source_table="sales_data",
      target_database="prod_db",
      target_table="sales_data",
      schedule="0 3 * * *"
  )
```

---

## Tool Catalog

All tools follow a **router pattern**: a single tool name accepts an `action` or `method` parameter
that selects the operation. This keeps the MCP tool list concise while preserving full capability.

### Pipeline Management (5 tools)

| Tool | Key actions / methods | Description |
|------|-----------------------|-------------|
| `pipeline_status` | `dag`, `task`, `log` | Query DAG run status, task state, and task logs |
| `pipeline_control` | `list`, `pause`, `resume`, `delete`, `update_schedule` | List, pause, resume, delete DAGs, or change their schedule |
| `pipeline_deploy` | `deploy_dags`, `deploy_complete`, `create_sync_dag` | Deploy DAG files or full pipeline artifacts to Airflow via SSH/SFTP |
| `pipeline_validate` | `dag`, `directory`, `files` | Validate DAG syntax and configuration before deployment |
| `airflow_connections` | `list`, `create_teradata`, `create_airbyte`, `create_ssh` | Create and list Airflow connections (Teradata, Airbyte, SSH) |

### Orchestration & Execution (3 tools)

| Tool | Key actions / methods | Description |
|------|-----------------------|-------------|
| `dag_trigger` | `run`, `idempotent`, `backfill` | Trigger DAG runs immediately, with deduplication, or as a backfill |
| `dag_monitor` | `status`, `history`, `logs`, `metrics` | Query DAG run status, history, task logs, and performance metrics |
| `airflow_admin` | `health`, `reset_circuit_breaker` | Airflow health check and circuit breaker management |

### Data Movement (5 tools)

| Tool | Key actions / methods | Description |
|------|-----------------------|-------------|
| `airbyte_pipeline` | `create`, `update`, `preview`, `check_health` | End-to-end Airbyte pipeline with smart stream selection and scheduling |
| `airbyte_sync` | `trigger`, `status`, `cancel` | Trigger and monitor Airbyte sync jobs |
| `airbyte_inventory` | `list_connectors`, `list_workspaces`, `get_schema` | Browse connector definitions, workspaces, and source schemas |
| `airbyte_manage` | `create_source`, `create_destination`, `create_connection`, `select_streams`, `build_catalog`, `delete_*` | Create, configure, and delete Airbyte sources, destinations, and connections |
| `airflow_teradata_load` | `csv_dag`, `table_transfer`, `csv_complete` | Generate Airflow DAGs for CSV loads or table transfers via TdLoad/TPT |

### dbt Management (5 tools)

| Tool | Key actions / methods | Description |
|------|-----------------------|-------------|
| `dbt_execute` | `run`, `test`, `build`, `compile`, `snapshot`, `seed`, `clean`, `debug`, `deps`, `parse` | Execute any dbt command with model selection and variable support |
| `dbt_docs` | `generate`, `generate_schema` | Generate dbt documentation (returns a shell command for local serving) and schema YAML |
| `dbt_info` | `list_models`, `list_sources`, `list_tests`, `project_info` | Inspect project structure, models, sources, and tests |
| `dbt_generate_model` | *(positional: table name)* | Generate dbt model SQL from Teradata table metadata |
| `dbt_project` | `init`, `clean`, `debug`, `deps` | Project-level lifecycle operations |

### Metadata Discovery (2 tools)

| Tool | Key actions / methods | Description |
|------|-----------------------|-------------|
| `teradata_discover` | `find`, `describe`, `profile`, `preview`, `compare`, `list` | Find, describe, profile, preview, and compare Teradata tables |
| `teradata_analyze` | `column`, `size`, `lineage`, `search` | Column statistics, size estimates, lineage, and metadata search |

### Connection Profiles (1 tool)

| Tool | Key actions | Description |
|------|-------------|-------------|
| `connection_profiles` | `list`, `reload` | List available profiles (no secrets) or reload from `connections.yaml` after edits |

### TTU Management (1 tool)

| Tool | Key actions | Description |
|------|-------------|-------------|
| `ttu_execute` | `execute_ddl`, `load_data`, `execute_bteq`, `check_installation` | Execute DDL via teradatasql, load data via tdload, run BTEQ scripts (with teradatasql fallback), check TTU installation |

---

## Development

> **Audience**: Contributors who want to modify, test, or extend the codebase.

### Development Setup

```bash
# Clone and install with dev dependencies
git clone <repository-url>
cd teradata-etl-mcp-server
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux/macOS
pip install -e ".[dev,all]"

# Install pre-commit hooks (runs linters/checks on every git commit)
pre-commit install

# Copy configuration templates
cp .env.example .env
cp connections.yaml.example connections.yaml
```

### Running Linters

```bash
# Lint with auto-fix
ruff check src tests --fix

# Format code
ruff format src tests

# Type checking
mypy src

# Security scan
bandit -c pyproject.toml -r src
```

### Code Patterns

- **Tool registration**: All tools are async functions registered via `register_*_tools(orchestrator)` returning `Dict[str, Callable]`.
- **Orchestrator**: `PipelineOrchestrator` lazy-loads clients via `@property` decorators.
- **Credential resolution**: Tools accept `*_profile` string parameters. The server calls `orchestrator.credential_resolver.resolve_profile(name)` to get the actual credentials.
- **Response sanitization**: All tool responses pass through `sanitize_response()` which deep-clones and masks sensitive keys (`password`, `secret`, `token`, `api_key`, `credential`, `connection_configuration`).

### Testing

#### Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run a specific test file
pytest tests/unit/test_airbyte_client.py

# Run a specific test class or method
pytest tests/unit/test_airbyte_client.py::TestCreateAirbyteSource
pytest tests/unit/test_airbyte_client.py::TestCreateAirbyteSource::test_success

# Run tests matching a keyword
pytest -k "intelligent_pipeline"

# Run only unit tests / skip slow tests
pytest -m unit
pytest -m "not slow"
```

#### Coverage

Coverage is configured in `pyproject.toml` and runs automatically with pytest. Reports are generated as:
- **Terminal**: term-missing (inline with pytest output)
- **HTML**: `htmlcov/index.html`
- **XML**: `coverage.xml`

#### Test Files

| Test File | Covers |
|-----------|--------|
| `test_airbyte_client.py` | Airbyte client + all data movement tools (288 tests) |
| `test_credential_resolver.py` | Profile resolution, env var interpolation, aliases (13 tests) |
| `test_response_sanitizer.py` | Sensitive key masking in tool responses (18 tests) |
| `test_connection_profile_tools.py` | list/reload connection profile tools (5 tests) |
| `test_airflow_client.py` | Airflow REST API client |
| `test_teradata_client.py` | Teradata database client |
| `test_dbt_client.py` | dbt CLI wrapper |
| `test_config.py` | Settings and configuration loading |
| `test_orchestrator.py` | Pipeline orchestrator |
| `test_pipeline_management_tools.py` | Pipeline management MCP tools |
| `test_metadata_discovery_tools.py` | Metadata discovery MCP tools |
| `test_airflow_dag_generator.py` | Airflow DAG code generation |
| `test_airflow_tdload_dag_generator.py` | TdLoad DAG code generation |
| `test_csv_analyzer.py` | CSV file analysis |
| `test_dbt_generator.py` | dbt model code generation |
| `test_bteq_generator.py` | BTEQ script generation |
| `test_tpt_generator.py` | TPT script generation |
| `test_intelligence_engine.py` | Transport method recommendation |
| `test_metrics_collector.py` | Metrics collection |
| `test_metadata_store.py` | Metadata persistence |
| `test_plugin_manager.py` | Plugin system |
| `test_validators.py` | Input validation utilities |

#### Writing Tests

- Mock the `PipelineOrchestrator` and its clients using `unittest.mock.Mock()` / `AsyncMock()`.
- Always include a `credential_resolver` mock on the orchestrator:
  ```python
  orch = Mock()
  resolver = Mock()
  resolver.resolve_profile.return_value = {"host": "localhost", "username": "user", "password": "pw"}
  orch.credential_resolver = resolver
  ```
- Test internal closures via the tools dict returned by `register_*_tools(orchestrator)`.
- HTTP response mocks must include `resp.headers = {"Content-Type": "application/json"}`.

### Pre-commit Hooks

The project uses 13 pre-commit hooks that run on every `git commit`:

| Hook | Description |
|------|-------------|
| `ruff-lint` | Python linting with auto-fix (pycodestyle, pyflakes, bugbear, security, etc.) |
| `ruff-format` | Code formatting check |
| `bandit` | Security vulnerability scan (source files only) |
| `check-ast` | Python syntax validation |
| `no-debug-statements` | Detect `print()`, `breakpoint()`, `pdb` in source |
| `no-private-keys` | Detect private keys in any file |
| `no-env-files` | Prevent `.env` files from being committed |
| `no-hardcoded-secrets` | Detect hardcoded passwords/tokens in source |
| `trailing-whitespace` | Remove trailing whitespace |
| `check-yaml` | YAML syntax validation |
| `check-toml` | TOML syntax validation |
| `check-merge-conflict` | Detect merge conflict markers |
| `no-large-files` | Reject files > 500 KB |

```bash
# Setup (one-time)
pre-commit install

# Manual run on staged files
pre-commit run

# Run on all files
pre-commit run --all-files

# Run a specific hook
pre-commit run ruff-lint
pre-commit run bandit
```

**Handling failures:**
- **ruff-lint**: Auto-fixes are applied. Review changes, re-stage, and commit again.
- **ruff-format**: Run `ruff format src tests` to fix, then re-stage.
- **bandit**: Add `# nosec BXXX` inline comments for false positives. Add rules to `skips` in `pyproject.toml` for project-wide suppression.

---

## Project Structure

```
teradata-etl-mcp-server/
|-- src/
|   |-- teradata_etl_mcp_server/
|       |-- __init__.py
|       |-- __main__.py              # Console script entrypoint
|       |-- main.py                  # CLI (argparse, signal handling, async)
|       |-- server.py                # FastMCP server, tool registration
|       |-- orchestrator.py          # PipelineOrchestrator (lazy-loads clients)
|       |-- config.py                # Pydantic settings (env vars, .env, YAML)
|       |-- credential_resolver.py   # Connection profile resolution
|       |-- response_sanitizer.py    # Mask sensitive keys in responses
|       |-- intelligence.py          # Transport method recommendation engine
|       |
|       |-- clients/
|       |   |-- airbyte_client.py    # Airbyte Public API v1 client
|       |   |-- airflow_client.py    # Airflow REST API client
|       |   |-- teradata_client.py   # Teradata SQL client
|       |   |-- dbt_client.py        # dbt CLI wrapper
|       |
|       |-- tools/
|       |   |-- pipeline_management.py      # 20 pipeline CRUD + Airflow connection tools
|       |   |-- orchestration_execution.py  # 6 DAG run + monitoring tools
|       |   |-- data_movement.py            # 21 Airbyte + TdLoad + CSV tools
|       |   |-- dbt_management.py           # 27 dbt operation tools
|       |   |-- governance_observability.py  # 5 lineage + audit + quality tools
|       |   |-- metadata_discovery.py       # 10 table discovery + profiling tools
|       |   |-- connection_profiles.py      # 2 profile listing/reload tools
|       |   |-- environment_secrets.py      # 6 connection + env var tools
|       |   |-- extensibility.py            # Plugin management tools
|       |   |-- deployment_validator.py     # Deployment validation utilities
|       |
|       |-- generators/
|       |   |-- airflow_dag_generator.py         # Airflow DAG Jinja2 templates
|       |   |-- airflow_tdload_dag_generator.py  # TdLoad DAG generation
|       |   |-- bteq_generator.py                # BTEQ script generation
|       |   |-- dbt_generator.py                 # dbt model generation
|       |   |-- tpt_generator.py                 # TPT script generation
|       |
|       |-- monitoring/
|       |   |-- metrics_collector.py  # Prometheus-format metrics
|       |
|       |-- plugins/
|       |   |-- plugin_manager.py     # Plugin discovery and lifecycle
|       |
|       |-- storage/
|       |   |-- metadata_store.py     # SQLite/JSON metadata persistence
|       |
|       |-- utils/
|           |-- csv_analyzer.py       # CSV file analysis
|           |-- file_operations.py    # File I/O utilities
|           |-- validators.py         # Input validation
|
|-- tests/
|   |-- unit/                  # 27 test files, 324+ tests
|
|-- scripts/                   # Utility scripts for manual testing
|-- airflow_dags/              # Generated DAG output directory
|-- .env.example               # Environment variable template
|-- connections.yaml.example   # Connection profile template
|-- .pre-commit-config.yaml    # Pre-commit hook configuration
|-- pyproject.toml             # Build config, tool settings, dependencies
|-- DESIGN.md                  # High-level architecture design document
```

---

## Troubleshooting

### Common Issues

**Pre-commit hooks fail on first commit after setup:**
```bash
pre-commit install
pre-commit run --all-files   # Fix all existing issues first
```

**Bandit false positives on SQL f-strings:**
B608 (hardcoded SQL expressions) is globally skipped in `pyproject.toml` because the project constructs BTEQ/validation SQL from internal configuration values, not user input. For other bandit findings, use `# nosec BXXX` inline comments.

**Tests fail with `TypeError: unexpected keyword argument`:**
Ensure test mocks include a `credential_resolver` on the orchestrator mock:
```python
resolver = Mock()
resolver.resolve_profile.return_value = {"host": "localhost"}
orch.credential_resolver = resolver
```

**`connections.yaml` not found:**
The resolver searches these locations in order:
1. `CONNECTIONS_FILE` environment variable
2. `connections.yaml` in current working directory
3. `settings.security.connections_file` (if configured in server settings)

**Airbyte API returns unexpected format:**
The Airbyte Public API v1 wraps list responses in `{"data": [...]}`. The client handles this internally via `resp.get("data", [])`.

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:
- Development setup
- Code style and standards
- Testing requirements
- Publishing updates

---

## Documentation

| Document | Purpose |
|----------|---------|
| [README.md](README.md) | Getting started and general usage (this file) |
| [SSH-SETUP.md](SSH-SETUP.md) | Bidirectional SSH configuration for Airflow integration |
| [DESIGN.md](DESIGN.md) | High-level architecture and system design |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development guidelines and workflow |

---

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

---
