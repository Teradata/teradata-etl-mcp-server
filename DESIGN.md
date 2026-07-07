# Teradata ETL MCP Server - High-Level Design

## Architecture Overview

The Teradata ETL MCP Server is a Model Context Protocol server that orchestrates end-to-end data pipelines by integrating Teradata, dbt, Airbyte, and Apache Airflow. It exposes 22 router tools to MCP clients (e.g. VS Code Copilot) via stdio transport.

## System Architecture

```
+-----------------------------------------------------------------------------+
|                          Teradata ETL MCP Server                                     |
|                     (FastMCP Framework)                                      |
+-----------------------------------------------------------------------------+
|                                                                              |
|  +------------------------------------------------------------------------+ |
|  |                    MCP Tools Layer (22 Router Tools)                   | |
|  +------------------------------------------------------------------------+ |
|  |  Pipeline Management  |  Orchestration Execution  |  Data Movement     | |
|  |  dbt Management       |  Metadata Discovery       |  Connection Profiles| |
|  |  TTU Management       |                           |                    | |
|  +------------------------------------------------------------------------+ |
|                                    |                                         |
|  +------------------------------------------------------------------------+ |
|  |              Pipeline Orchestration Engine (orchestrator.py)           | |
|  +------------------------------------------------------------------------+ |
|  |  * Source-type based routing (Airbyte vs TPT)                         | |
|  |  * Pipeline Generation Coordination                                    | |
|  |  * Schema Metadata Extraction                                          | |
|  |  * Airbyte Registry Cache (SQLite)                                     | |
|  +------------------------------------------------------------------------+ |
|                                    |                                         |
|  +------------------------------------------------------------------------+ |
|  |                    Service Clients Layer                               | |
|  +----------------+----------------+----------------+---------------------+ |
|  |  Teradata      |  Airbyte       |  Airflow       |  dbt                | |
|  |  Client        |  Client        |  Client        |  Client             | |
|  |  (sync)        |  (async)       |  (async)       |  (subprocess)       | |
|  |                |                |                |                     | |
|  |  * Metadata    |  * Connector   |  * DAG Mgmt    |  * run/test/build   | |
|  |  * Lineage     |    Registry    |  * Execution   |  * Model Gen        | |
|  |  * Profiling   |  * Sync Ops    |  * Monitor     |  * Docs Gen         | |
|  |  * Query       |  * Workspace   |  * Logs        |  * Profile Mgmt     | |
|  +----------------+----------------+----------------+---------------------+ |
|                                    |                                         |
|  +------------------------------------------------------------------------+ |
|  |                    Code Generators Layer                               | |
|  +----------------+----------------+----------------+---------------------+ |
|  |  dbt Model     |  Airflow DAG   |  TPT Script    |  BTEQ Script        | |
|  |  Generator     |  Generator     |  Generator     |  Generator          | |
|  |                |                |                |                     | |
|  |  * Staging     |  * Tasks       |  * Export/     |  * Metadata         | |
|  |  * Intermediate|  * Deps        |    Load Jobs   |  * Validation       | |
|  |  * Marts       |  * Schedule    |  * Error Tbl   |  * DDL Exec         | |
|  |  * Incremental |  * Retry       |  * Logging     |                     | |
|  |  * Snapshots   |  * SFTP Deploy |                |                     | |
|  +----------------+----------------+----------------+---------------------+ |
|                                                                              |
|  +------------------------------------------------------------------------+ |
|  |                    Support Layer                                       | |
|  +----------------+----------------+----------------+---------------------+ |
|  |  Circuit       |  SQLite        |  Response      |  Credential         | |
|  |  Breaker       |  Metadata      |  Sanitizer     |  Resolver           | |
|  |  (Redis/mem)   |  Store         |  (strip secrets|  (connections.yaml) | |
|  |                |                |   from output) |                     | |
|  +----------------+----------------+----------------+---------------------+ |
|                                                                              |
+-----------------------------------------------------------------------------+
                                    |
+-----------------------------------------------------------------------------+
|                        External Systems & Services                          |
+-------------+-------------+-------------+-------------+--------------------+
|  Teradata   |  Airbyte    |  Airflow    |  dbt Core   |  Remote Airflow    |
|  Database   |  (OSS/Cloud)|  Instance   |  (local)    |  (SFTP deploy)     |
+-------------+-------------+-------------+-------------+--------------------+
```

## Tool Categories & Router Tools

The server uses a **router-tool pattern**: each MCP tool accepts a `mode`, `action`, `query`, `command`, or similar dispatch parameter. This keeps the MCP tool surface compact while exposing rich functionality.

### 1. Pipeline Management (5 tools) — `airflow_pipeline_management.py`

#### `pipeline_status`
Query Airflow pipeline status, list pipelines, or check DAG existence.
- `get_status` — Get current status and run history of a pipeline
- `list_pipelines` — List all Airflow pipelines (DAGs) with tag filtering
- `check_dag_exists` — Check if a DAG exists in Airflow

#### `pipeline_control`
Control Airflow pipeline lifecycle.
- `update_schedule` — Update pipeline execution schedule (cron or preset); supports auto-deploy
- `pause` — Pause a pipeline to prevent execution
- `resume` — Resume a paused pipeline
- `delete` — Delete pipeline and optionally remove DAG file and dbt models

#### `pipeline_deploy`
Deploy pipeline artifacts to Airflow.
- `deploy_complete` — Deploy a complete pipeline (DAG + TPT scripts + BTEQ scripts + dbt project) via SFTP
- `deploy_dags` — Deploy DAG files to a remote Airflow server via SFTP with rollback support
- `create_sync_dag` — Generate and save an Airbyte sync DAG file

#### `pipeline_validate`
Pre-flight validation for a pipeline configuration (single action, no dispatch parameter).
- Checks: Teradata connectivity, Airflow reachability, Airbyte availability, required fields, source-specific constraints
- Supports source types: `airbyte`, `csv`, `csv_file`, `file`, `tpt_file`

#### `airflow_connections`
Manage Airflow connections. Credentials resolved server-side — the LLM never handles passwords.
- `list` — List existing Airflow connections with optional prefix/type filtering
- `create_teradata` — Create an Airflow Teradata connection from a profile
- `create_airbyte` — Create an Airflow Airbyte connection
- `create_ssh` — Create an Airflow SSH connection from a profile

---

### 2. Orchestration & Execution (3 tools) — `orchestration_execution.py`

#### `dag_trigger`
Trigger Airflow DAG runs in various modes.
- `run` — Trigger a single DAG run immediately
- `idempotent` — Trigger with deduplication guarantee (by idempotency key)
- `multiple` — Trigger several DAGs concurrently
- `retry_failed` — Clear and retry failed tasks in a DAG run

#### `dag_monitor`
Query DAG run status, history, task logs, and monitoring data.
- `run_status` — Get status of a specific or latest DAG run
- `list_runs` — List recent DAG runs with state/date filtering
- `list_dags` — List available DAGs from Airflow
- `task_logs` — Get logs for a specific task instance (truncated at 100 KB)
- `monitor_execution` — Comprehensive monitoring with performance metrics and task breakdown

#### `airflow_admin`
Airflow administrative operations.
- `health` — Get Airflow health status and circuit breaker state
- `reset_circuit_breaker` — Reset circuit breaker to closed state

---

### 3. Data Movement & Integration (5 tools) — `data_movement.py`

#### `airbyte_pipeline`
Create, update, preview, or health-check Airbyte pipelines.
- `create` — Create an Airbyte pipeline (source + destination + connection) from profiles
- `update` — Update an existing Airbyte connection (schedule, namespace, status)
- `preview` — Preview pipeline configuration without creating resources
- `check_health` — Full health check on an Airbyte pipeline

#### `airbyte_sync`
Trigger, monitor, or wait for Airbyte sync jobs.
- `trigger` — Trigger a new sync for a connection
- `get_status` — Get status of a sync job (optionally with logs)
- `wait` — Poll until a sync job completes (configurable timeout/interval)

#### `airbyte_inventory`
List Airbyte connectors, connections, sources, destinations, or streams.
- `connectors` — List available connector definitions (filterable by type/name)
- `connections` — List all Airbyte connections
- `connection_details` — Get full details for a specific connection
- `sources` — List all configured sources
- `destinations` — List all configured destinations
- `streams` — Discover streams for a source
- `select_streams` — Intent-based stream selection from natural language prompt

#### `airbyte_manage`
Create, delete, or test Airbyte sources, destinations, and connections.
Credentials resolved from `connections.yaml` — the LLM never handles API keys.
- `create_source` — Create an Airbyte source
- `create_destination` — Create an Airbyte destination
- `delete_source` — Delete an Airbyte source
- `delete_destination` — Delete an Airbyte destination
- `delete_connection` — Delete an Airbyte connection
- `test_api` — Test Airbyte API connectivity
- `check_source` — Check source connection health
- `check_destination` — Check destination configuration

#### `airflow_teradata_load`
Generate and execute Teradata data load pipelines.
- `csv_dag` — Generate an Airflow DAG for CSV-to-Teradata loading via TPT
- `csv_complete` — Generate DAG + TPT script + optional deploy for CSV loading
- `table_transfer` — Generate Teradata-to-Teradata transfer DAG using TPT

---

### 4. dbt Management (5 tools) — `dbt_management.py`

#### `dbt_execute`
Execute dbt commands (never suggest raw CLI commands — always use this tool).
- `run` — Run dbt models
- `test` — Run dbt tests
- `build` — Build models, run tests, apply snapshots and seeds
- `compile` — Compile dbt project to SQL
- `parse` — Parse project and write manifest.json (use for validate/check intents)
- `snapshot` — Run dbt snapshots
- `seed` — Load seed data
- `clean` — Remove compiled artifacts
- `debug` — Debug dbt connection
- `deps` — Install dbt package dependencies

#### `dbt_docs`
Generate dbt documentation.
- `generate` — Generate dbt docs (catalog + manifest); returns `serve_command` for local preview

#### `dbt_info`
Retrieve dbt project information, metadata, and configuration.
- `version` — Get installed dbt version
- `project_info` — Get project information and statistics
- `model_sql` — Get compiled SQL for a specific model
- `manifest` — Read dbt manifest.json
- `catalog` — Read dbt catalog.json
- `run_results` — Read dbt run_results.json
- `project_config` — Read dbt_project.yml configuration
- `profiles_config` — Read profiles.yml (credentials masked)
- `check_installation` — Check dbt and dbt-teradata adapter installation
- `list_models` — List dbt models (filterable by type, includes sources/tests optionally)
- `validate_project` — Validate dbt project configuration

#### `dbt_generate_model`
Generate dbt transformation models from Teradata source metadata.
- `staging` — Generate staging models from Teradata source tables (with tests)
- `intermediate` — Generate intermediate transformation models with join logic
- `mart` — Generate mart models with dimensions and measures
- `incremental` — Generate incremental models with configurable strategy
- `snapshot` — Generate SCD Type 2 snapshot models

#### `dbt_project`
Manage dbt project structure and profile configuration.
- `create_structure` — Create standard dbt project folder layout (staging/intermediate/marts/snapshots)
- `generate_profiles` — Generate profiles.yml from `connections.yaml` credentials

---

### 5. Metadata Discovery (2 tools) — `metadata_discovery.py`

#### `teradata_discover`
Discover and search Teradata database objects.
- `test_connection` — Test Teradata connectivity and return server info
- `discover` — Discover tables in a database with pattern matching and size estimates
- `list` — Enumerate tables in a database with optional type filtering
- `search` — Full-text search across database objects
- `describe` — Get complete table schema with columns, types, and constraints

#### `teradata_analyze`
Analyze and profile Teradata table data.
- `profile` — Generate statistical profiles (min/max/avg/stddev/distinct)
- `compare` — Compare schemas between two tables to detect structural drift
- `analyze_column` — Deep-dive column analysis with distribution patterns
- `estimate_size` — Calculate table size and row count for transport decisions
- `analyze_dependencies` — Discover upstream/downstream table dependencies (lineage)
- `preview_data` — Sample table data with configurable row limit

---

### 6. Connection Profiles (1 tool) — `connection_profiles.py`

#### `connection_profiles`
Manage connection credential profiles from `connections.yaml`.
- `list` — List all available connection profiles
- `reload` — Reload profiles from disk (pick up changes without restart)

---

### 7. TTU Management (1 tool) — `ttu_tools.py`

#### `ttu_execute`
Execute local Teradata Tools & Utilities (TPT, BTEQ, tdload) operations.
- `execute_ddl` — Execute DDL statements (CREATE, DROP, ALTER) via teradatasql direct connection
- `load_data` — Load data into Teradata via TPT/tdload (CSV, file, table transfer)
- `execute_bteq` — Execute arbitrary BTEQ scripts against Teradata
- `check_installation` — Verify TTU binaries (tbuild, bteq, tdload) are installed and accessible

---

## Tool Annotations (MCP Hints)

Each tool is assigned `ToolAnnotations` to help MCP clients classify tools:

| Classification | Tools |
|---|---|
| Read-only (`readOnlyHint=True`) | `pipeline_status`, `pipeline_validate`, `airbyte_inventory`, `dbt_info`, `teradata_discover`, `teradata_analyze`, `dag_monitor`, `connection_profiles` |
| Destructive (`destructiveHint=True`) | `pipeline_control` (has delete), `airbyte_manage` (has delete actions), `ttu_execute` (has destructive DDL) |
| Additive/mutating (default) | All other tools (create, update, trigger, deploy, generate) |

---

## Pipeline Routing

### Source-Type Based Routing

The transfer method is determined by the `source_type` supplied by the caller.

| Source Type | Transfer Method | Tool Used |
|---|---|---|
| `airbyte` (or unspecified) | Airbyte | `airbyte_pipeline`, `airbyte_sync` |
| `csv` / `csv_file` | TPT bulk load | `airflow_teradata_load` (method: `csv_dag`, `csv_complete`) |
| Teradata-to-Teradata | TPT export/load | `airflow_teradata_load` (method: `table_transfer`) |
| Airbyte sync DAG | Airbyte | `pipeline_deploy` (action: `create_sync_dag`) |

### Pipeline Generation Flow

```
1. User invokes: pipeline_deploy (action: deploy_complete) or pipeline_validate
   |
2. Extract Metadata (for Teradata sources)
   * teradata_discover (action: describe)
   |
3. Transfer Method is determined by source_type supplied by the caller
   * csv / csv_file  -> TPT
   * airbyte         -> Airbyte
   * table_transfer  -> TPT (Teradata-to-Teradata)
   |
4. Generate dbt Models (if requested)
   * dbt_generate_model (model_type: staging) -> source.yml, stg_table.sql
   * Auto-generates not_null, unique, and relationship tests
   |
5. Generate Airflow DAG
   * AirflowDAGGenerator -> pipeline_<name>.py
     - extract_load_task (TPT BashOperator or AirbyteSyncOperator)
     - dbt_run_task (optional)
     - dbt_test_task (optional)
   |
6. Generate Supporting Scripts (if TPT)
   * teradata_load (method: csv_complete or table_transfer)
   * TPT export/load job with error tables and checkpoint/restart
   |
7. Deploy (if requested)
   * pipeline_deploy (action: deploy_dags) -> SFTP to remote Airflow
   * Rollback on failure, optional wait for DAG reload
   |
8. Validate
   * pipeline_validate -> connectivity + config checks
```

---

## Data Flow Examples

### Example 1: Airbyte Pipeline (connector-based source)

```
Source: External database via Airbyte connector

Step 1: Create Airbyte source and destination
  -> airbyte_manage(action="create_source", source_type="Postgres", ...)
  -> airbyte_manage(action="create_destination", destination_type="Teradata", ...)

Step 2: Create Airbyte connection pipeline
  -> airbyte_pipeline(action="create", connection_name="customers_sync", ...)

Step 3: dbt Model Generation
  -> dbt_generate_model(model_type="staging")
     -> _sources_prod_db.yml
     -> stg_customers.sql
     -> _models_stg_customers.yml (with tests)

Step 4: Create Airbyte Sync DAG
  -> pipeline_deploy(action="create_sync_dag", dag_id="customers_sync", connection_id="...")
     -> customers_sync_dag.py (AirbyteSyncOperator + dbt tasks)

Step 5: Deploy
  -> pipeline_deploy(action="deploy_dags") -> SFTP to Airflow

Step 6: Execute
  -> dag_trigger(mode="run", pipeline_name="customers_sync")
```

### Example 2: TPT Pipeline (Teradata-to-Teradata or CSV source)

```
Source: ANALYTICS_DB.TRANSACTIONS (Teradata-to-Teradata)

Step 1: Metadata Inspection (optional)
  -> teradata_discover(action="describe") -> columns, types, PKs

Step 2: Transfer Method
  -> Caller specifies source_type = "table_transfer" -> TPT is used

Step 3: dbt Model Generation
  -> dbt_generate_model(model_type="staging")
     -> _sources_analytics_db.yml
     -> stg_transactions.sql
     -> _models_stg_transactions.yml

Step 4: TPT Pipeline Generation
  -> teradata_load(method="table_transfer")
     -> tpt_transactions_export_load.tpt
        +--> EXPORT operator (source Teradata)
        +--> LOAD operator (target Teradata)
        +--> Error tables configuration
        +--> Checkpoint/restart enabled

Step 5: Airflow DAG Generation + Deploy
  -> pipeline_transactions_to_dwh.py
     +--> tpt_extract_load_task (BashOperator)
     +--> validate_load_task (check error tables)
     +--> dbt_run_task
     +--> dbt_test_task
  -> pipeline_deploy(action="deploy_dags") -> SFTP to Airflow

Step 6: Execute
  -> dag_trigger(mode="run", pipeline_name="pipeline_transactions_to_dwh")
```

---

## Configuration System

Configuration is managed by 13 Pydantic `BaseSettings` classes, each with its own environment variable prefix. All are aggregated by the root `Settings` class (`config.py`).

| Class | Env Prefix | Key Fields |
|---|---|---|
| `TeradataSettings` | `TERADATA_` | host, username, password, port, logmech, pool_size, query_timeout |
| `AirflowSettings` | `AIRFLOW_` | base_url, username, password, token_endpoint, access_token, remote_host, remote_user, remote_ssh_key, remote_port |
| `AirbyteSettings` | `AIRBYTE_` | enabled, base_url, client_id, client_secret (OAuth2), workspace_id |
| `DBTSettings` | `DBT_` | project_dir, profiles_dir, target, threads, command_timeout |
| `TTUSettings` | `TTU_` | enabled, ttu_version, tpt_binary_path, bteq_binary_path, tdload_binary_path, scripts_dir, command_timeout, tpt_error_limit |
| `PipelineSettings` | `PIPELINE_` | dags_output_dir, default_schedule_interval, generate_dbt_by_default, dq thresholds (null %, duplicate %) |
| `ObservabilitySettings` | `OBSERVABILITY_` | enable_audit_log, audit_log_file, enable_lineage_tracking, enable_metrics |
| `SecuritySettings` | `SECURITY_` | connections_file |
| `MCPServerSettings` | `MCP_` | log_level, log_file, redis_url, validate_on_startup, fail_fast_on_startup |
| `OrchestratorSettings` | `ORCHESTRATOR_` | backend (currently `"airflow"`) |

### Cross-Setting Validation (`Settings.validate_settings`)

The root `Settings` class validates cross-cutting dependencies at startup:
- Auto-constructs `teradata_source` and `teradata_target` from `TERADATA_SOURCE_*` / `TERADATA_TARGET_*` env vars
- Creates required output directories (DAGs, dbt project, TTU scripts, logs)
- Validates Airbyte `base_url` is set when enabled
- Validates Airflow `simple` auth has either `access_token` or `token_endpoint`

### Environment Configuration Examples

```
Development:
  TERADATA_HOST=dev-td.company.com
  AIRFLOW_BASE_URL=http://localhost:8080
  DBT_TARGET=dev

Staging:
  TERADATA_HOST=staging-td.company.com
  AIRFLOW_BASE_URL=https://staging-airflow.company.com
  DBT_TARGET=staging

Production:
  TERADATA_HOST=prod-td.company.com
  AIRFLOW_BASE_URL=https://airflow.company.com
  DBT_TARGET=prod
```

---

## Security & Secrets

### Credential Resolution

Credentials are resolved server-side via `credential_resolver.py`. The LLM never receives or handles passwords, tokens, or API keys.

Resolution order:
1. Named profile from `connections.yaml` (path configured via `SECURITY_CONNECTIONS_FILE`)
2. Fallback to `.env` / environment variable settings

### Secrets Backends

Configured via `SECURITY_SECRETS_BACKEND`:
- `env` (default) — Environment variables / `.env` file
- `aws` — AWS Secrets Manager
- `azure` — Azure Key Vault
- `vault` — HashiCorp Vault
- `airflow` — Airflow Connections (encrypted)

### Response Sanitizer

`response_sanitizer.py` strips sensitive data from all tool outputs before returning to the MCP client. Passwords, tokens, and webhook URLs are masked.

### Tool Annotations

Read-only tools are annotated `readOnlyHint=True` so MCP clients can enable them by default without user approval prompts.

---

## Reliability — Circuit Breaker

`utils/circuit_breaker.py` wraps Airflow API calls with a circuit breaker to prevent cascading failures.

- **Backend**: Redis (via `MCP_REDIS_URL`) or in-memory fallback
- **States**: CLOSED (normal) → OPEN (blocking, after threshold failures) → HALF-OPEN (recovery probe)
- **Admin**: `airflow_admin(action="health")` reports state; `airflow_admin(action="reset_circuit_breaker")` resets to CLOSED

---

## Startup Validation

On startup, `Settings.validate_connectivity()` probes each enabled service:

| Service | Required? | On Failure |
|---|---|---|
| Airflow | Yes (if configured) | Sets `valid=False`; `MCP_FAIL_FAST_ON_STARTUP=true` aborts |
| Airbyte | Yes (if enabled) | Sets `valid=False`; fail-fast applies |
| Teradata | No | Warning only; server continues |
| Redis | No | Warning only; falls back to in-memory circuit breaker |

Control with:
- `MCP_VALIDATE_ON_STARTUP=false` — skip validation entirely
- `MCP_FAIL_FAST_ON_STARTUP=true` — abort if required services unreachable

---

## SQLite Metadata Store

`storage/metadata_store.py` provides a local SQLite cache for:
- Table metadata (columns, types, sizes) to reduce Teradata round-trips
- Airbyte OSS connector registry (preloaded at startup via `orchestrator.preload_airbyte_registry()`)

---

## Observability & Monitoring

### Lineage Tracking

```
teradata_analyze(analysis_type="analyze_dependencies") -> upstream/downstream tables
                 +
dbt manifest (models, sources, tests)
                 +
Airflow DAG task graph
                 =
End-to-end lineage: Source TD Table -> Transfer -> Staging -> dbt Models -> Target
```

Lineage graph outputs are written to `OBSERVABILITY_LINEAGE_OUTPUT_DIR`.

### Audit Logging

When `OBSERVABILITY_ENABLE_AUDIT_LOG=true`, all pipeline creation, modification, and deletion events are written to `OBSERVABILITY_AUDIT_LOG_FILE` with user context and timestamp.

### Metrics

Execution metrics (rows transferred, duration, test pass/fail counts) are collected when `OBSERVABILITY_ENABLE_METRICS=true` and stored in `OBSERVABILITY_METRICS_OUTPUT_DIR`.

---

## Technology Stack

| Component | Library / Version |
|---|---|
| MCP Framework | FastMCP >= 2.11.3, < 3 |
| Language | Python >= 3.10, < 3.14 |
| Teradata Driver | teradatasql >= 17.20 |
| Teradata SQLAlchemy | teradatasqlalchemy >= 20.0 |
| dbt Core | dbt-core >= 1.7, < 2.0 |
| dbt Adapter | dbt-teradata >= 0.19 |
| HTTP Client | httpx >= 0.25 |
| SSH / SFTP | paramiko >= 3.4 |
| ORM | SQLAlchemy >= 2.0 |
| Templating | Jinja2 >= 3.1 |
| Configuration | pydantic-settings >= 2.0 |
| Data Analysis | pandas >= 2.0 |
| Async File I/O | aiofiles >= 23.0 |
| YAML | PyYAML >= 6.0 |
| Environment | python-dotenv >= 1.0 |
| Optional — Lineage viz | graphviz >= 0.20 |
| Optional — ML features | numpy, scikit-learn >= 1.3 |
| Optional — Redis | redis (for distributed circuit breaker) |

---

## Deployment Models

### 1. Standalone stdio (any MCP client)

```bash
teradata-etl-mcp-server
# or
teradata-etl-mcp-server --env-file /path/to/.env
```

VS Code `settings.json`:
```json
{
  "github.copilot.chat.mcp.servers": {
    "elt-pipeline": {
      "command": "teradata-etl-mcp-server",
      "args": ["--env-file", "/absolute/path/to/.env"]
    }
  }
}
```

### 2. Container Deployment

```yaml
# docker-compose.yml (example)
services:
  teradata-etl-mcp-server:
    image: teradata-etl-mcp-server:latest
    environment:
      - TERADATA_HOST=prod-td.company.com
      - TERADATA_USERNAME=svc_elt
      - TERADATA_PASSWORD=${TERADATA_PASSWORD}
      - AIRFLOW_BASE_URL=https://airflow.company.com
      - AIRFLOW_USERNAME=admin
      - AIRFLOW_PASSWORD=${AIRFLOW_PASSWORD}
      - MCP_REDIS_URL=redis://redis:6379/0
```

---

## Runtime Deployment Architecture

The server runs on the local development machine alongside the MCP client. Generated artifacts are transferred to a remote Airflow server via SFTP.

```
+------------------------------------------------------------------------------+
|                        LOCAL DEVELOPMENT MACHINE                             |
|                                                                              |
|  MCP Client (VS Code / Claude Desktop)                                       |
|    |  STDIO (JSON-RPC)                                                        |
|  Teradata ETL MCP Server (Python Process)                                             |
|    |  generates                                                               |
|  Generated Artifacts: ./airflow_dags/*.py, ./dbt_project/, ./test_data/      |
+------------------------------------------------------------------------------+
                              |
                              |  SSH/SFTP (port 22)
                              |  HTTP REST API (port 8080)
                              v
+------------------------------------------------------------------------------+
|                         REMOTE AIRFLOW SERVER                                |
|                                                                              |
|  Webserver (port 8080)  |  Scheduler  |  Worker(s)                          |
|                                                                              |
|  /opt/airflow/dags/   <- DAG files deployed here via SFTP                   |
|                                                                              |
|  Required Python packages (pip install on Airflow server):                  |
|    apache-airflow-providers-teradata                                         |
|    apache-airflow-providers-ssh                                              |
|    apache-airflow-providers-airbyte  (optional, for Airbyte DAGs)           |
+------------------------------------------------------------------------------+
                              |
                              |  SSH (port 22) — dbt execution
                              |  Teradata protocol (port 1025) — data loading
                              v
+------------------------------------------------------------------------------+
|                          EXTERNAL SYSTEMS                                    |
|                                                                              |
|  dbt Worker Node                     Teradata Database                       |
|  * dbt installed                     * Port 1025                             |
|  * dbt project files                 * Target + error tables                 |
|  * SSH access from Airflow                                                   |
+------------------------------------------------------------------------------+
```

### DAG Deployment Flow

```
1. Generate DAG locally
   -> teradata_load(method="csv_dag") or pipeline_deploy(action="deploy_complete")
   -> Output: ./airflow_dags/pipeline_<name>.py

2. Transfer via SFTP (paramiko)
   -> pipeline_deploy(action="deploy_dags")
   -> SSH connect to AIRFLOW_REMOTE_HOST:22
   -> sftp.put(local_dag, /opt/airflow/dags/<dag>.py)
   -> chmod 644

3. Airflow scheduler scans and registers
   -> Scheduler scans /opt/airflow/dags/ every ~30s
   -> Parses Python file, extracts DAG objects
   -> Registers in metadata DB
   -> MCP server polls GET /api/v1/dags/{dag_id} (up to 360s) to confirm
```

---

## How Airflow Executes dbt

When a pipeline includes dbt transformations, the generated Airflow DAG runs dbt via one of two operators:

**Option A: SSHOperator (remote dbt worker)**
```python
from airflow.providers.ssh.operators.ssh import SSHOperator

dbt_run = SSHOperator(
    task_id='dbt_run_staging',
    ssh_conn_id='ssh_dbt_worker',
    command='cd /opt/dbt_projects/my_project && dbt run --models staging.*',
    cmd_timeout=300,
)
```
Use this when dbt is installed on a dedicated worker node reachable by SSH from Airflow.

**Option B: BashOperator (local to Airflow worker)**
```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id='dbt_run_staging',
    bash_command='cd /opt/dbt_projects/my_project && dbt run --models staging.*',
)
```
Use this when dbt is installed directly on the Airflow worker node.

In both cases the dbt project (models, `dbt_project.yml`, `profiles.yml`) must already exist on the node where dbt executes. The MCP server generates model files locally via `dbt_generate_model`; those files must be copied to the worker node separately before the DAG runs.

---

## CSV to Teradata Pipeline Flow

### Phase 1: CSV Analysis

`teradata_load(method="csv_dag")` invokes the CSV Analyzer before DAG generation:

```
CSV File
  -> csv_analyzer.py
     * Detect delimiter and encoding
     * Infer column types (-> Teradata types: INTEGER, VARCHAR, DECIMAL)
     * Count rows, calculate file size
     * Estimate load time

CSVAnalysis object:
  file_path, row_count, column_count, file_size_mb,
  columns: [{name, type, max_length}], delimiter, has_header
```

### Phase 2: Airflow Connection Setup

```
airflow_connections(action="create_teradata", ...)
  -> Connection ID: teradata_default
  -> Credentials from TERADATA_HOST / TERADATA_USERNAME resolved server-side

airflow_connections(action="create_ssh", ...)
  -> Connection ID: ssh_localhost
  -> Host: AIRFLOW_REMOTE_HOST, key: AIRFLOW_REMOTE_SSH_KEY
```

### Phase 3: DAG Deployment

```
pipeline_deploy(action="deploy_dags")
  -> SFTP transfer: ./airflow_dags/<dag>.py -> /opt/airflow/dags/<dag>.py
  -> Rollback on failure
  -> Optional: wait for Airflow DAG discovery (max 360s)
```

### Phase 4: Generated DAG Structure

```
Airflow DAG: load_<db>_<table>
  |
  start (EmptyOperator)
  |
  TdLoadOperator
    source_file_name: "/path/to/input.csv"
    target_table: "DB.TABLE"
    source_format: "Delimited"
    source_text_delimiter: ","
    error_limit: 100
    session_count: 4
    teradata_conn_id: "teradata_default"
    ssh_conn_id: "ssh_localhost"
  |
  BteqOperator (Validation 1 — row count check)
    sql: SELECT COUNT(*) FROM DB.TABLE
  |
  BteqOperator (Validation 2 — NULL checks)
  |
  end (EmptyOperator)
```

### Phase 5: TdLoadOperator Execution

```
Airflow Worker
  -> TdLoadOperator
     -> SSH to remote host (ssh_conn_id)
        -> Execute tbuild / tdload
           -> Read CSV, parse delimiter, validate types
           -> Load into Teradata via parallel sessions
              -> Target table: DB.TABLE
              -> Error rows:   DB.TABLE_ET  (rejected rows)
              -> UV violations: DB.TABLE_UV (uniqueness violations)
```

### Session Count Guidelines

| File Size | Recommended session_count |
|---|---|
| Small (< 10 MB) | 2–4 |
| Medium (10–100 MB) | 4–8 |
| Large (> 100 MB) | 8–16 |

---

## CLI Commands

```bash
# Run server (default: stdio)
teradata-etl-mcp-server

# Show current configuration (masks secrets)
teradata-etl-mcp-server config
teradata-etl-mcp-server config --json

# Validate configuration and test all connections
teradata-etl-mcp-server validate

# Show version
teradata-etl-mcp-server version
```

---

## Not Yet Implemented

The following capabilities are planned but not currently implemented:

| Category | Planned Tools |
|---|---|
| Governance & Observability | `get_data_quality_report`, `audit_pipeline_changes`, `generate_lineage_graph`, `alert_on_failure`, `cost_estimation` |
| Advanced Intelligence | `recommend_pipeline_optimization`, `predict_pipeline_runtime`, `recommend_transport`, `detect_schema_changes` |
| Extensibility | `register_custom_operator`, `list_plugins`, `install_plugin` |
| Secrets | `rotate_credentials`, `get_environment_config` |
| MCP Resources | Active pipelines list, connection status, recent runs |
| MCP Prompts | Guided workflows (create pipeline, troubleshoot failures, optimize) |

---

## Design Principles

- **Credentials never flow through the LLM** — all secrets resolved server-side via `credential_resolver.py` and masked in `response_sanitizer.py`
- **Router-tool pattern** — compact MCP surface (22 tools) with rich dispatch, enabling IDE auto-approval of read-only tools
- **Async throughout** — Airflow and Airbyte clients are fully async; Teradata (sync driver) uses `asyncio.to_thread`
- **Dependency injection** — `PipelineOrchestrator` accepts a `client_factory` parameter for testability
- **Configuration-driven behavior** — schedule defaults and DQ thresholds are all env-configurable
- **Graceful degradation** — circuit breaker, startup fail-fast toggle, and per-service validation allow partial-availability operation
- **Composition over inheritance** — tools delegate to orchestrator; orchestrator delegates to clients and generators

## License

Proprietary — license to be finalized with team before release.

**Referenced Projects** (for integration patterns only, not code reuse):
- dbt-mcp (Apache 2.0) - dbt patterns
- mcp-server-apache-airflow (MIT) - Airflow API patterns
- PyAirbyte (ELv2) - API usage only, not embedded
- teradata-mcp-server (MIT) - Teradata patterns
