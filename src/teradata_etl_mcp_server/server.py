"""FastMCP server setup and tool registration.

This module initializes the FastMCP application, registers all tools,
and manages the lifecycle of the Teradata ETL MCP Server.
"""

import asyncio
import atexit
import functools
import logging
import sys
import time
from contextlib import asynccontextmanager

from fastmcp import FastMCP
from fastmcp.tools.tool import Tool
from mcp.types import ToolAnnotations

from . import __version__
from .config import Settings, load_settings
from .orchestrator import PipelineOrchestrator

# Import tool modules
from .tools import (
    airflow_pipeline_management,
    connection_profiles,
    data_movement,
    dbt_management,
    metadata_discovery,
    orchestration_execution,
    ttu_tools,
)
from .utils.audit_logger import AuditLogger


class TeradataETLMCPServer:
    """Teradata ETL MCP Server application manager."""

    def __init__(self, settings: Settings | None = None):
        """
        Initialize the Teradata ETL MCP Server.

        Args:
            settings: Optional settings instance (loads from env if not provided)
        """
        self.settings = settings or load_settings()
        self.logger = self._setup_logging()
        self.app: FastMCP | None = None
        self.orchestrator: PipelineOrchestrator | None = None
        self._request_semaphore = asyncio.Semaphore(self.settings.mcp.max_concurrent_requests)
        self._audit_logger = AuditLogger(
            log_file=self.settings.observability.audit_log_file,
            enabled=self.settings.observability.enable_audit_log,
        )

    def _setup_logging(self) -> logging.Logger:
        """Configure logging based on settings."""
        # Create logger
        logger = logging.getLogger("teradata_etl_mcp_server")
        logger.setLevel(self.settings.mcp.log_level)

        # Avoid duplicate handlers
        if logger.handlers:
            return logger

        # Console handler
        # In STDIO transport, logging to stdout interferes with MCP protocol.
        # Route logs to stderr to avoid parse warnings in clients.
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(self.settings.mcp.log_level)
        console_formatter = logging.Formatter(self.settings.mcp.log_format)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File handler (if configured)
        if self.settings.mcp.log_file:
            file_handler = logging.FileHandler(self.settings.mcp.log_file)
            file_handler.setLevel(self.settings.mcp.log_level)
            file_formatter = logging.Formatter(self.settings.mcp.log_format)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        return logger

    def _initialize_orchestrator(self) -> PipelineOrchestrator:
        """Initialize the pipeline orchestrator with all clients."""
        self.logger.info("Initializing pipeline orchestrator...")

        try:
            orchestrator = PipelineOrchestrator(self.settings)
            # Preload Airbyte OSS registry into local cache DB once at startup
            try:
                orchestrator.preload_airbyte_registry()
            except Exception as preload_err:
                self.logger.warning("Airbyte registry preload skipped/failed: %s", preload_err)

            self.logger.info("Pipeline orchestrator initialized successfully")
            return orchestrator
        except Exception as e:
            self.logger.error("Failed to initialize orchestrator: %s", e, exc_info=True)
            raise

    def _validate_startup(self) -> bool:
        """
        Validate connectivity to services on startup.

        Returns:
            True if validation passed (or was skipped), False if critical failures
        """
        if not self.settings.mcp.validate_on_startup:
            self.logger.info("Startup validation disabled (MCP_VALIDATE_ON_STARTUP=false)")
            return True

        self.logger.info("Validating service connectivity...")

        try:
            results = self.settings.validate_connectivity(timeout=10)

            # Log service status
            for service, status in results["services"].items():
                if status["status"] == "ok":
                    latency = status.get("latency_ms", "N/A")
                    self.logger.info(
                        "  [OK] %s: %s (latency: %sms)", service, status["message"], latency
                    )
                elif status["status"] == "error":
                    self.logger.error("  [FAIL] %s: %s", service, status["message"])
                elif status["status"] in ("disabled", "not_configured", "skipped"):
                    self.logger.info("  [SKIP] %s: %s", service, status["message"])
                else:
                    self.logger.warning("  [WARN] %s: %s", service, status["message"])

            # Log warnings
            for warning in results["warnings"]:
                self.logger.warning("Startup validation warning: %s", warning)

            # Check if we should fail fast
            if not results["valid"]:
                for error in results["errors"]:
                    self.logger.error("Startup validation error: %s", error)

                if self.settings.mcp.fail_fast_on_startup:
                    self.logger.error(
                        "Startup validation failed and MCP_FAIL_FAST_ON_STARTUP=true. Aborting."
                    )
                    return False
                else:
                    self.logger.warning(
                        "Startup validation failed but MCP_FAIL_FAST_ON_STARTUP=false. Continuing with warnings."
                    )

            self.logger.info("Startup validation complete (valid=%s)", results["valid"])
            return True

        except Exception as e:
            self.logger.error("Startup validation error: %s", e, exc_info=True)
            if self.settings.mcp.fail_fast_on_startup:
                return False
            return True

    @staticmethod
    def _get_tool_annotations(name: str) -> ToolAnnotations:
        """Return MCP ToolAnnotations for a tool based on its name.

        Properly classifying tools lets MCP clients (e.g. VS Code Copilot)
        enable non-destructive tools by default instead of hiding them.

        Router tools are classified by their most permissive action:
        - Read-only routers: all actions are queries/inspections.
        - Destructive routers: at least one action deletes resources.
        - Default: additive/mutating but non-destructive.
        """
        # Read-only router tools (all actions are queries/inspections)
        read_only_tools = {
            "pipeline_status",
            "pipeline_validate",
            "airbyte_inventory",
            "dbt_info",
            "teradata_discover",
            "teradata_analyze",
            "dag_monitor",
        }
        if name in read_only_tools:
            return ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=True,
            )

        # Destructive router tools (contain delete/remove actions)
        destructive_tools = {
            "pipeline_control",  # has delete action
            "airbyte_manage",  # has delete_source/destination/connection
            "ttu_execute",  # has destructive DDL (DROP/DELETE/TRUNCATE)
        }
        if name in destructive_tools:
            return ToolAnnotations(
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=False,
                openWorldHint=True,
            )

        # Everything else (create, update, trigger, generate, deploy, etc.)
        # is non-read-only but also non-destructive (additive operations).
        return ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=False,
            idempotentHint=False,
            openWorldHint=True,
        )

    def _wrap_tool(self, name: str, func):
        """Wrap tool with concurrency limit and audit logging."""

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                await asyncio.wait_for(
                    self._request_semaphore.acquire(), timeout=self.settings.mcp.request_timeout
                )
            except asyncio.TimeoutError:
                return {"success": False, "error": "Server busy — max concurrent requests reached."}
            start = time.monotonic()
            result = None
            success = False
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception:
                success = False
                self.logger.exception("Unexpected error in tool '%s'", name)
                result = {"success": False, "error": "Unexpected error while executing tool."}
                return result
            finally:
                self._request_semaphore.release()
                duration_ms = (time.monotonic() - start) * 1000
                if self._audit_logger:
                    action = (
                        kwargs.get("action")
                        or kwargs.get("analysis_type")
                        or kwargs.get("model_type")
                        or kwargs.get("mode")
                        or kwargs.get("query")
                        or kwargs.get("list_type")
                        or kwargs.get("command")
                        or kwargs.get("method")
                    )
                    if result is not None:
                        if isinstance(result, dict):
                            if "success" in result:
                                success = bool(result["success"])
                            elif "error" in result:
                                success = False
                            else:
                                success = True
                        else:
                            success = True
                    self._audit_logger.log_tool_call(name, action, kwargs, success, duration_ms)

        return wrapper

    def _register_tools(self, app: FastMCP):
        """
        Register all MCP tools with the FastMCP application.

        Args:
            app: FastMCP application instance
        """
        self.logger.info("Registering MCP tools...")

        enabled = self.settings.mcp.enabled_tools

        tool_count = 0

        # Helper to register a set of tools with filtering and wrapping
        def _add_tools(tools_dict: dict, category: str, default_desc: str):  # noqa: ARG001
            nonlocal tool_count
            for name, func in tools_dict.items():
                if enabled is not None and name not in enabled:
                    self.logger.info("Skipping tool '%s' (not in enabled_tools)", name)
                    continue
                tool = Tool.from_function(
                    fn=self._wrap_tool(name, func),
                    name=name,
                    description=(func.__doc__ or default_desc),
                    annotations=self._get_tool_annotations(name),
                )
                app.add_tool(tool)
                tool_count += 1

        # Register Airflow Pipeline Management tools
        try:
            pm = airflow_pipeline_management.register_pipeline_tools(self.orchestrator)
            _add_tools(pm, "pipeline management", "Pipeline management tool")
        except Exception as e:
            self.logger.error("Failed to register pipeline management tools: %s", e, exc_info=True)

        # Register Orchestration & Execution tools
        try:
            oe = orchestration_execution.register_orchestration_tools(self.orchestrator)
            _add_tools(oe, "orchestration", "Orchestration tool")
        except Exception as e:
            self.logger.error("Failed to register orchestration tools: %s", e, exc_info=True)

        # Register Data Movement tools
        try:
            dm = data_movement.register_data_movement_tools(self.orchestrator)
            _add_tools(dm, "data movement", "Data movement tool")
        except Exception as e:
            self.logger.error("Failed to register data movement tools: %s", e, exc_info=True)

        # Register dbt Management tools
        try:
            dbt = dbt_management.register_dbt_tools(self.orchestrator)
            _add_tools(dbt, "dbt", "dbt management tool")
        except Exception as e:
            self.logger.error("Failed to register dbt tools: %s", e, exc_info=True)

        # Register Metadata Discovery tools
        try:
            md = metadata_discovery.register_metadata_tools(self.orchestrator)
            _add_tools(md, "metadata discovery", "Metadata discovery tool")
        except Exception as e:
            self.logger.error("Failed to register metadata discovery tools: %s", e, exc_info=True)

        # Register Connection Profile tools
        try:
            cp = connection_profiles.register_connection_profile_tools(self.orchestrator)
            _add_tools(cp, "connection profile", "Connection profile tool")
        except Exception as e:
            self.logger.error("Failed to register connection profile tools: %s", e, exc_info=True)

        # Register TTU (Teradata Tools & Utilities) tools
        try:
            ttu = ttu_tools.register_ttu_tools(self.orchestrator)
            _add_tools(ttu, "TTU", "TTU execution tool")
        except Exception as e:
            self.logger.error("Failed to register TTU tools: %s", e, exc_info=True)

        self.logger.info("Registered %d MCP tools", tool_count)

    def _register_prompts(self, app: FastMCP):  # noqa: ARG002
        """
        Register prompt templates for common operations.

        Args:
            app: FastMCP application instance
        """
        self.logger.info("Registering prompt templates...")

        # TODO: Add prompt templates for guided workflows
        # Example prompts:
        # - Create pipeline workflow
        # - Troubleshoot failed pipeline
        # - Optimize existing pipeline
        # - Generate data quality report

        prompt_count = 0
        self.logger.info("Registered %d prompt templates", prompt_count)

    def _register_resources(self, app: FastMCP):  # noqa: ARG002
        """
        Register MCP resources for configuration and state.

        Args:
            app: FastMCP application instance
        """
        self.logger.info("Registering MCP resources...")

        # TODO: Add resources for:
        # - Server configuration (read-only)
        # - Active pipelines list
        # - Connection status
        # - Recent pipeline runs

        resource_count = 0
        self.logger.info("Registered %d MCP resources", resource_count)

    def create_app(self) -> FastMCP:
        """
        Create and configure the FastMCP application.

        Returns:
            Configured FastMCP application

        Raises:
            RuntimeError: If startup validation fails and fail_fast_on_startup is True
        """
        self.logger.info("Creating Teradata ETL MCP Server application...")
        self.logger.info("Environment: %s", self.settings.environment)
        self.logger.info("Transport: stdio")

        # Validate connectivity before full initialization
        if not self._validate_startup():
            raise RuntimeError(
                "Startup validation failed. Check service connectivity and configuration."
            )

        # Create FastMCP app
        app = FastMCP(
            name="teradata-etl-mcp-server",
            version=__version__,
            instructions=(
                "MANDATORY: dbt file and command operations\n"
                "  Never read dbt project files directly (dbt_project.yml, manifest.json,\n"
                "  profiles.yml, catalog.json, run_results.json) even if the file path is\n"
                "  known from prior context. Never run dbt shell commands directly (dbt --version,\n"
                "  dbt ls, dbt list). Always use the dbt_info tool instead:\n"
                "    dbt_project.yml       → dbt_info(info_type=\"project_config\")\n"
                "    profiles.yml          → dbt_info(info_type=\"profiles_config\")\n"
                "    manifest.json         → dbt_info(info_type=\"manifest\")\n"
                "    catalog.json          → dbt_info(info_type=\"catalog\")\n"
                "    run_results.json      → dbt_info(info_type=\"run_results\")\n"
                "    dbt --version         → dbt_info(info_type=\"version\")\n"
                "    dbt ls / dbt list     → dbt_info(info_type=\"list_models\")\n"
                "    project overview      → dbt_info(info_type=\"project_info\")\n"
                "  This rule applies even when the file path is visible in context.\n"
                "\n"
                "Tool parameter boundaries — do NOT mix parameters across tools:\n"
                "  - dbt_generate_model: uses 'model_type' (not 'action'). 'action' and "
                "'teradata_profile' are accepted defensively but IGNORED — model scaffolding "
                "doesn't connect to Teradata. Use dbt_execute or dbt_project if you need either.\n"
                "  - dbt_project: uses 'action' (e.g. 'create_structure', 'generate_teradata_macros').\n"
                "  - teradata_discover / teradata_analyze: use 'teradata_profile' for connection selection.\n"
                "  - dbt_generate_model staging: use 'source_tables' (list) not 'source_table' (singular). "
                "e.g. source_tables=['customers'] not source_table='customers'.\n"
                "  - pipeline_deploy: use action='create_dbt_dag' to generate Airflow DAGs for dbt "
                "transformations. Required: dag_id, project_name. "
                "Always set use_ssh_for_dbt=True for remote Airflow servers. Do NOT write DAG files manually.\n"
                "Always check each tool's exact parameter names before calling.\n"
                "\n"
                "WORKSPACE LAYOUT — do not ask the user for project paths:\n"
                "  All dbt / Airflow / pipeline artifacts default to a single workspace root "
                "(env: WORKSPACE_DIR; default: ~/teradata-etl-mcp-workspace). Inside it:\n"
                "    dbt_project/dbt_<name>/   per-Teradata-profile dbt sub-projects\n"
                "    airflow_dags/             generated Airflow DAG files\n"
                "    ttu_scripts/              generated TTU/BTEQ/TPT scripts\n"
                "    logs/                     server logs\n"
                "  Do NOT ask the user for ``project_dir`` / ``dbt_project_dir`` / "
                "``output_filename`` paths unless they want to override the default. Pass "
                "``project_name`` (a slug) for dbt sub-project selection — the server "
                "resolves it under ``<workspace>/dbt_project/dbt_<slug>/``.\n"
                "\n"
                "CONFIGURATION FILE OWNERSHIP — credentials are USER-managed:\n"
                "  AGENT must NEVER write OR read these files (no edit, no Read tool, no "
                "``cat``/``head``/``tail``, no shell redirection like "
                "``echo TERADATA_DATABASE=... >> .env``):\n"
                "    <workspace>/.env                          — wizard-managed user creds\n"
                "    connections.yaml                          — named connection profiles\n"
                "    profiles.yml                              — dbt-managed (server scaffolds it)\n"
                "    <workspace>/dbt_project/dbt_<slug>/.env   — server-written, per-sub-project "
                "TERADATA_* dotenv (loaded by ``dotenv run --`` at dbt task time)\n"
                "  The MCP SERVER writes the per-sub-project ``.env`` at scaffold time and on "
                "explicit ``dbt_project(action='refresh_env', ...)`` calls — that is fine and "
                "expected. The agent does NOT write or read it; the agent uses the tool's "
                "``keys_written`` / ``keys_skipped_empty`` response fields (which list KEY NAMES "
                "only, never values) to confirm the file's content.\n"
                "  When a tool reports missing credentials or configuration "
                "(``action_required: set_teradata_database`` / ``connections_yaml_not_found`` "
                "/ Rule-5 missing-profile / SSH-key-missing / ``no_identity`` / similar), DO NOT "
                "create or edit those files yourself and DO NOT fabricate placeholder "
                "credentials. ASK THE USER to either:\n"
                "    1. Open the Setup Wizard (VS Code: Teradata ETL MCP Server → Setup Wizard) and fill "
                "the missing field, then click Save and Reload.\n"
                "    2. OR add/edit a named profile in their ``connections.yaml`` (point them at "
                "``connections.yaml.example`` if needed) and call "
                "``connection_profiles(action='reload')`` after they save.\n"
                "    3. After credential rotation, run "
                "``dbt_project(action='refresh_env', project_name=..., teradata_profile=...)`` "
                "to overwrite the per-sub-project ``.env`` (server-side write).\n"
                "  Credentials are out of the agent's scope. Surface the error verbatim to the "
                "user and stop until they confirm the change.\n"
                "\n"
                "CONNECTION SELECTION POLICY — wizard default vs. named profile:\n"
                "  Definitions:\n"
                "    • Wizard connection: the Teradata identity configured in the setup wizard "
                "(env vars). Default for local/interactive Teradata work unless overridden.\n"
                "    • Profile connection: a named entry in connections.yaml, referenced by the "
                "'teradata_profile' / 'target_profile' / 'source_profile' / 'destination_profile' / "
                "'ssh_profile' parameter on the relevant tool.\n"
                "\n"
                "  Section A — Default selection:\n"
                "  Rule 1 (default — wizard for local/interactive work): for any Teradata-touching "
                "local or interactive operation (file load, query, DDL, BTEQ, dbt, schema discovery, "
                "table profiling), if the user did NOT name a profile, call the tool with no profile "
                "param → wizard default.\n"
                "  Rule 2 (explicit profile wins fully): when the user names a profile ('use profile "
                "X' / 'using profile X'), pass X through the profile param. The profile wins fully — "
                "mechanism and all fields come from connections.yaml, no mixing with wizard or with "
                "wizard-style override params (e.g. target_host / target_username / target_password). "
                "If the user supplies both, the profile still wins.\n"
                "  Rule 3 (ambiguity): if the user says 'use this profile' without naming which AND "
                "multiple profiles exist, ask ONE short question: 'Which profile (e.g., dev/prod/…)?' "
                "Do NOT invent a name. Do NOT ask if the user never mentioned a profile (Rule 1 "
                "default applies).\n"
                "\n"
                "  Section B — Hard requirements (tools that REJECT the wizard default):\n"
                "  Rule 4 (table_to_table copy via ttu_execute): do NOT assume source OR target "
                "connection. Explicitly ask the user for (a) source table + source system/database + "
                "source connection (wizard or a profile) and (b) target table + target system/database "
                "+ target connection. The tool rejects calls missing 'teradata_profile' or "
                "'target_profile'. Pass the literal 'wizard' (or 'default') through either parameter "
                "to record explicit user confirmation of the wizard-default connection — distinct "
                "from silence. 'wizard' / 'default' are reserved sentinel names; any connections.yaml "
                "profile literally named 'wizard' or 'default' is unreachable by name.\n"
                "  Rule 5 (persistent Teradata assets that flow credentials through Airflow / "
                "Airbyte MUST use a named profile): when a deployed asset stores or transmits "
                "Teradata credentials via an Airflow Teradata Connection or an Airbyte connector "
                "configuration, that asset MUST reference a named connections.yaml profile. The "
                "wizard-default identity AND the 'wizard'/'default' sentinel are REJECTED for "
                "these tools — they're for local/interactive use only and baking them into a "
                "deployed asset would expose dev credentials. Tools enforcing this: "
                "airbyte_manage (create_source, create_destination); airflow_teradata_load "
                "(csv_dag, csv_complete, table_transfer — these provision Airflow Teradata "
                "Connections for TdLoadOperator).\n"
                "    Exception — pipeline_deploy create_dbt_dag / create_sync_dag (with "
                "project_name): Rule 5 does NOT apply because these DAGs do NOT use Airflow "
                "Teradata Connections OR Airflow Variables for credentials. TERADATA_* "
                "credentials flow via the per-sub-project ``.env`` written by "
                "dbt_project(action='create_structure'); the generated DAG runs "
                "``dotenv run -- dbt ...`` and reads the .env at task time. **``teradata_profile`` "
                "is NOT required** for these two actions; ``project_name`` is the only locator "
                "(the parameter is accepted on the router for shape consistency with sibling "
                "actions but ignored by the dbt-DAG paths). The Rule 5 spirit (no wizard creds "
                "in deployed assets) is preserved upstream: ``dbt_project(action='create_structure')`` "
                "is what writes the ``.env``, and that scaffold step does require an explicit "
                "Teradata identity — so the wizard-default can never silently end up in a "
                "deployed dbt sub-project unless the user explicitly scaffolded with it.\n"
                "  Worker prerequisite: ``pip install \"python-dotenv[cli]\"``. The dbt sub-project "
                "(including .env) must be on the Airflow worker filesystem at the same path the "
                "MCP server wrote it to (mount, CI/CD, or manual sync — out of scope for the MCP "
                "server). After credential rotation in connections.yaml or the wizard, call "
                "dbt_project(action='refresh_env', project_name=<name>, ...) to overwrite the "
                "local .env, then re-sync the sub-project to the worker. The shape of the call "
                "depends on the binding form returned in the DAG-creation response's "
                "``teradata_identity`` field: if it's a named profile (e.g. ``'prod'``), pass "
                "that name as ``teradata_profile``; if it's the wizard sentinel form "
                "``'wizard:<host_slug>'``, OMIT ``teradata_profile`` so refresh_env folds to "
                "the wizard-default identity. Passing ``'wizard:<host_slug>'`` verbatim as "
                "``teradata_profile`` would trigger a connections.yaml lookup and fail — only "
                "the literal sentinels ``'wizard'`` / ``'default'`` / empty fold to wizard-"
                "default; the colon-suffix form is treated as an explicit named profile.\n"
                "\n"
                "  Section C — Failure handling:\n"
                "  Rule 6 (no auto-pivot on wizard failure): when a Teradata operation fails using "
                "the wizard-default connection (the call had no profile named, or the "
                "'wizard'/'default' sentinel was passed), do NOT scan connections.yaml and retry "
                "with a different profile. Surface the error to the user verbatim and stop. The "
                "user may then explicitly name a profile in their next prompt; only then retry. "
                "Failed responses from ttu_execute include a 'connection_source' field "
                "('wizard' or 'profile:<name>') so this rule is machine-checkable — when "
                "connection_source == 'wizard' the response also carries a 'wizard_failure_hint' "
                "repeating this rule. Other tools may not yet emit these fields; default to the "
                "prose form of the rule for them.\n"
                "\n"
                "NEXT_STEPS RESPONSES — chained guidance from successful tool calls:\n"
                "  Most successful tool responses include a ``next_steps`` field: a list of "
                "Markdown-formatted prose strings describing what the agent should consider "
                "doing next. Each entry is one self-contained recommendation with four parts "
                "inline: ``**N. <imperative>**: <command>. **Why**: <reason>. **Effect**: "
                "<change>. **If missing**: <fallback>.``\n"
                "  How to use them:\n"
                "    • Treat ``next_steps`` as **suggestions, not commands**. Surface them to "
                "the user (or evaluate against the user's stated goal) before acting. The "
                "user may want exactly the next step, a different one, or nothing more.\n"
                "    • The ``If missing`` clause says when to skip the step — read it before "
                "deciding to chain. Many steps are explicitly optional (docs generation, "
                "scheduling) and skipping is the right call for ad-hoc / dev work.\n"
                "    • Steps reference real tool calls with concrete arguments where "
                "possible. Re-derive arguments from the user's intent rather than copy-"
                "pasting verbatim.\n"
                "    • Failure paths (``failed`` Airbyte / Airflow runs) also include "
                "``next_steps`` pointing at log-pulling and retry — use them when triaging.\n"
                "  Do NOT auto-execute every step in a chain. Stop after each meaningful "
                "milestone unless the user has explicitly asked for end-to-end automation."
            ),
        )

        # Install the alias-rewrite + literal-enum-error-enrichment
        # middleware. Rewrites natural-language parameter aliases
        # (e.g. ``query`` → ``sql``) before validation and turns
        # Pydantic ``literal_error`` into a message that lists the
        # allowed values so the LLM can self-correct.
        from .server_middleware import ParamAliasingAndEnumErrorEnrichmentMiddleware
        app.add_middleware(ParamAliasingAndEnumErrorEnrichmentMiddleware())

        # Initialize orchestrator
        self.orchestrator = self._initialize_orchestrator()

        # Make orchestrator available to tools
        app.state = {"orchestrator": self.orchestrator, "settings": self.settings}

        # Register tools, prompts, and resources
        self._register_tools(app)
        self._register_prompts(app)
        self._register_resources(app)

        # Log configuration summary
        self._log_configuration()

        self.app = app

        # Ensure cleanup runs on process exit regardless of shutdown path.
        # Guard against multiple registrations if create_app is called more than once.
        if not getattr(self, "_cleanup_registered", False):

            def _sync_cleanup():
                asyncio.run(self.cleanup())

            atexit.register(_sync_cleanup)
            self._cleanup_registered = True

        self.logger.info("Teradata ETL MCP Server application created successfully")

        return app

    def _log_configuration(self):
        """Log current configuration summary."""
        self.logger.info("Configuration summary:")
        self.logger.info("  Teradata: %s", self.settings.teradata.host)
        self.logger.info("  Airflow: %s", self.settings.airflow.base_url)
        self.logger.info(
            "  Airbyte: %s", "Enabled" if self.settings.airbyte.enabled else "Disabled"
        )
        self.logger.info(
            "  dbt: %s (target: %s)", self.settings.dbt.project_dir, self.settings.dbt.target
        )

    async def cleanup(self):
        """Clean up resources before shutdown."""
        self.logger.info("Cleaning up resources...")

        if self.orchestrator:
            try:
                await self.orchestrator.cleanup()
                self.logger.info("Orchestrator cleaned up successfully")
            except Exception as e:
                self.logger.error("Error closing orchestrator: %s", e, exc_info=True)

        if self._audit_logger:
            self._audit_logger.close()

        self.logger.info("Cleanup complete")


def create_app(settings: Settings | None = None) -> FastMCP:
    """
    Factory function to create a configured FastMCP application.

    This is the main entry point for creating the Teradata ETL MCP Server.

    Args:
        settings: Optional settings instance

    Returns:
        Configured FastMCP application

    Example:
        >>> app = create_app()
        >>> # Run with stdio transport
        >>> import asyncio
        >>> asyncio.run(app.run_stdio_async())
    """
    server = TeradataETLMCPServer(settings)
    return server.create_app()


# Global server instance for lifecycle management
_server_instance: TeradataETLMCPServer | None = None


def get_server_instance() -> TeradataETLMCPServer | None:
    """
    Get the global server instance.

    Returns:
        Server instance if initialized, None otherwise
    """
    return _server_instance


def set_server_instance(server: TeradataETLMCPServer):
    """
    Set the global server instance.

    Args:
        server: Server instance to set
    """
    global _server_instance
    _server_instance = server


@asynccontextmanager
async def lifespan(app: FastMCP):  # noqa: ARG001
    """
    Async context manager for application lifespan.

    Handles startup and shutdown operations for the server.

    Args:
        app: FastMCP application
    """
    server = get_server_instance()

    # Startup
    if server:
        server.logger.info("Starting Teradata ETL MCP Server...")
        server.logger.info("Server ready on stdio transport")

    yield

    # Shutdown
    if server:
        server.logger.info("Shutting down Teradata ETL MCP Server...")
        await server.cleanup()
        server.logger.info("Server shutdown complete")


def create_app_with_lifespan(settings: Settings | None = None) -> FastMCP:
    """
    Create FastMCP application with lifespan management.

    Args:
        settings: Optional settings instance

    Returns:
        Configured FastMCP application with lifespan
    """
    server = TeradataETLMCPServer(settings)
    set_server_instance(server)
    app = server.create_app()

    # Note: FastMCP may not support lifespan directly like FastAPI
    # This is a placeholder for potential future support

    return app


# Convenience functions for common operations
def get_orchestrator() -> PipelineOrchestrator | None:
    """
    Get the pipeline orchestrator from the global server instance.

    Returns:
        PipelineOrchestrator if server is initialized, None otherwise
    """
    server = get_server_instance()
    return server.orchestrator if server else None


def get_settings() -> Settings | None:
    """
    Get settings from the global server instance.

    Returns:
        Settings if server is initialized, None otherwise
    """
    server = get_server_instance()
    return server.settings if server else None


def get_logger() -> logging.Logger:
    """
    Get logger from the global server instance or create a default one.

    Returns:
        Logger instance
    """
    server = get_server_instance()
    if server:
        return server.logger
    return logging.getLogger("teradata_etl_mcp_server")


# Version info (__version__ is imported from the package at the top of this module)
__author__ = "Teradata ETL MCP Server Team"
__description__ = (
    "Unified data pipeline orchestration server integrating "
    "Teradata, dbt, Airbyte, and Apache Airflow"
)
