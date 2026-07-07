"""Unit tests for Teradata ETL MCP Server."""

import asyncio
import logging
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp.types import ToolAnnotations

from teradata_etl_mcp_server import __version__ as package_version
from teradata_etl_mcp_server.server import (
    TeradataETLMCPServer,
    __version__,
    create_app,
    create_app_with_lifespan,
    get_logger,
    get_orchestrator,
    get_server_instance,
    get_settings,
    lifespan,
    set_server_instance,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _registered_tools(app):
    """Return the list of registered tool objects via the public FastMCP 3.x API.

    FastMCP 3.x removed the private ``app._tool_manager._tools`` mapping;
    ``list_tools()`` is the supported accessor and is async (returns a list).
    """
    return asyncio.run(app.list_tools())


def _make_mock_settings():
    """Build a lightweight mock Settings that satisfies TeradataETLMCPServer."""
    settings = MagicMock()
    settings.environment = "development"

    # MCPServerSettings
    settings.mcp.log_level = "WARNING"
    settings.mcp.log_format = "%(message)s"
    settings.mcp.log_file = None
    settings.mcp.validate_on_startup = False
    settings.mcp.fail_fast_on_startup = False
    settings.mcp.max_concurrent_requests = 10
    settings.mcp.request_timeout = 300
    settings.mcp.enabled_tools = None

    # ObservabilitySettings
    settings.observability.enable_audit_log = False
    settings.observability.audit_log_file = None

    # Sub-settings (used by _log_configuration)
    settings.teradata.host = "localhost"
    settings.airflow.base_url = "http://localhost:8080"
    settings.airbyte.enabled = False
    settings.dbt.project_dir = "/tmp/dbt"
    settings.dbt.target = "dev"
    # validate_connectivity (used by _validate_startup when enabled)
    settings.validate_connectivity = MagicMock(return_value={
        "valid": True,
        "services": {},
        "warnings": [],
        "errors": [],
    })

    return settings


def _make_mock_orchestrator():
    """Build a mock PipelineOrchestrator with an async cleanup."""
    orch = MagicMock()
    orch.cleanup = AsyncMock()
    orch.preload_airbyte_registry = MagicMock(return_value=True)
    return orch


# ---------------------------------------------------------------------------
# Tests: TeradataETLMCPServer Initialization
# ---------------------------------------------------------------------------


class TestTeradataETLMCPServerInit:
    """Tests for TeradataETLMCPServer.__init__ and attribute setup."""

    @patch.dict(os.environ, {}, clear=True)
    def test_init_with_provided_settings(self):
        """Server should store the provided settings object."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)

        assert server.settings is settings
        assert server.app is None
        assert server.orchestrator is None

    @patch.dict(os.environ, {}, clear=True)
    def test_init_creates_logger(self):
        """Server should set up a logger on init."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)

        assert isinstance(server.logger, logging.Logger)
        assert server.logger.name == "teradata_etl_mcp_server"

    @patch.dict(os.environ, {}, clear=True)
    def test_init_loads_settings_when_none(self):
        """When no settings are passed, load_settings() should be called."""
        with patch("teradata_etl_mcp_server.server.load_settings") as mock_load:
            mock_load.return_value = _make_mock_settings()
            server = TeradataETLMCPServer(settings=None)

            mock_load.assert_called_once()
            assert server.settings is mock_load.return_value


# ---------------------------------------------------------------------------
# Tests: Logging Setup
# ---------------------------------------------------------------------------


class TestSetupLogging:
    """Tests for TeradataETLMCPServer._setup_logging."""

    @patch.dict(os.environ, {}, clear=True)
    def test_logger_level_matches_settings(self):
        """Logger level should match the configured log_level."""
        settings = _make_mock_settings()
        settings.mcp.log_level = "DEBUG"
        server = TeradataETLMCPServer(settings=settings)

        assert server.logger.level == logging.DEBUG

    @patch.dict(os.environ, {}, clear=True)
    def test_no_duplicate_handlers(self):
        """Calling _setup_logging twice should not duplicate handlers."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)

        handler_count_before = len(server.logger.handlers)
        server._setup_logging()
        handler_count_after = len(server.logger.handlers)

        assert handler_count_after == handler_count_before


# ---------------------------------------------------------------------------
# Tests: Tool Annotations
# ---------------------------------------------------------------------------


class TestGetToolAnnotations:
    """Tests for TeradataETLMCPServer._get_tool_annotations (static method)."""

    def test_read_only_tool(self):
        """Read-only tools should have readOnlyHint=True, destructiveHint=False."""
        for name in [
            "pipeline_status", "pipeline_validate",
            "airbyte_inventory",
            "dbt_info",
            "teradata_discover", "teradata_analyze",
            "dag_monitor",
        ]:
            ann = TeradataETLMCPServer._get_tool_annotations(name)
            assert isinstance(ann, ToolAnnotations), f"{name}: wrong type"
            assert ann.readOnlyHint is True, f"{name}: should be read-only"
            assert ann.destructiveHint is False, f"{name}: should not be destructive"
            assert ann.idempotentHint is True, f"{name}: should be idempotent"

    def test_destructive_tool(self):
        """Destructive tools should have destructiveHint=True."""
        for name in ["pipeline_control", "airbyte_manage"]:
            ann = TeradataETLMCPServer._get_tool_annotations(name)
            assert ann.readOnlyHint is False, f"{name}: should not be read-only"
            assert ann.destructiveHint is True, f"{name}: should be destructive"
            assert ann.idempotentHint is False, f"{name}: should not be idempotent"

    def test_default_tool(self):
        """Unrecognised / additive tools get the default classification."""
        ann = TeradataETLMCPServer._get_tool_annotations("dbt_execute")
        assert ann.readOnlyHint is False
        assert ann.destructiveHint is False
        assert ann.idempotentHint is False
        assert ann.openWorldHint is True

    def test_all_annotations_have_open_world_hint(self):
        """Every classification sets openWorldHint=True."""
        for name in [
            "pipeline_status", "pipeline_control", "dbt_execute", "some_unknown_tool",
        ]:
            ann = TeradataETLMCPServer._get_tool_annotations(name)
            assert ann.openWorldHint is True


# ---------------------------------------------------------------------------
# Tests: create_app
# ---------------------------------------------------------------------------


class TestCreateApp:
    """Tests for TeradataETLMCPServer.create_app."""

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_create_app_returns_fastmcp(self, mock_orch_cls):
        """create_app should return a FastMCP instance."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        from fastmcp import FastMCP
        assert isinstance(app, FastMCP)

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_instructions_include_config_ownership_block(self, mock_orch_cls):
        """The server's ``instructions=`` block must contain the
        CONFIGURATION FILE OWNERSHIP section so agents are told
        explicitly not to write or read credential-bearing files."""
        mock_orch_cls.return_value = _make_mock_orchestrator()
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()
        instructions = app.instructions or ""
        assert "CONFIGURATION FILE OWNERSHIP" in instructions
        # AGENT-vs-SERVER distinction is explicit.
        assert "AGENT must NEVER write OR read" in instructions
        assert "MCP SERVER writes the per-sub-project ``.env``" in instructions
        # Read prohibition for `.env` is spelled out.
        assert "no Read tool" in instructions
        assert "``cat``/``head``/``tail``" in instructions
        # Per-sub-project .env path is documented.
        assert "<workspace>/dbt_project/dbt_<slug>/.env" in instructions
        # Refresh action is mentioned as the rotation path.
        assert "refresh_env" in instructions
        # Asking-the-user fallback paths still present.
        assert "ASK THE USER" in instructions
        assert "Setup Wizard" in instructions
        assert "connections.yaml" in instructions
        # Old action name no longer appears anywhere in the instructions.
        assert "provision_teradata_variables" not in instructions

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_instructions_describe_next_steps_template(self, mock_orch_cls):
        """The server's ``instructions=`` block must document the
        ``next_steps`` Markdown-prose template so agents know how to
        consume the chained guidance returned by tool success
        responses."""
        mock_orch_cls.return_value = _make_mock_orchestrator()
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()
        instructions = app.instructions or ""
        # Section header present
        assert "NEXT_STEPS RESPONSES" in instructions
        # All four parts of the template are named
        assert "**Why**" in instructions
        assert "**Effect**" in instructions
        assert "**If missing**" in instructions
        # Treat as suggestion, not a command
        assert "suggestions, not commands" in instructions
        # Don't auto-execute every step
        assert "Do NOT auto-execute" in instructions

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_create_app_stores_app_and_orchestrator(self, mock_orch_cls):
        """After create_app, server.app and server.orchestrator should be set."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        assert server.app is app
        assert server.orchestrator is mock_orch_cls.return_value

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_create_app_sets_state(self, mock_orch_cls):
        """The FastMCP app.state should contain orchestrator and settings."""
        mock_orch = _make_mock_orchestrator()
        mock_orch_cls.return_value = mock_orch

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        assert app.state["orchestrator"] is mock_orch
        assert app.state["settings"] is settings

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_create_app_registers_atexit_once(self, mock_orch_cls):
        """atexit cleanup should only be registered once, even if create_app is called twice."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)

        with patch("teradata_etl_mcp_server.server.atexit") as mock_atexit:
            server.create_app()
            assert mock_atexit.register.call_count == 1

            server.create_app()
            # Should NOT register a second time
            assert mock_atexit.register.call_count == 1

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_create_app_raises_on_failed_validation(self, mock_orch_cls):
        """If startup validation fails with fail_fast, create_app should raise RuntimeError."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        settings.mcp.validate_on_startup = True
        settings.mcp.fail_fast_on_startup = True
        settings.validate_connectivity.return_value = {
            "valid": False,
            "services": {},
            "warnings": [],
            "errors": ["Teradata unreachable"],
        }

        server = TeradataETLMCPServer(settings=settings)

        with pytest.raises(RuntimeError, match="Startup validation failed"):
            server.create_app()


# ---------------------------------------------------------------------------
# Tests: Tool Registration
# ---------------------------------------------------------------------------


class TestToolRegistration:
    """Tests for tool registration via _register_tools."""

    # The full set of 21 expected tool names across 6 modules
    EXPECTED_TOOLS = {
        # airflow_pipeline_management
        "pipeline_status",
        "pipeline_control",
        "pipeline_deploy",
        "pipeline_validate",
        "airflow_connections",
        # orchestration_execution
        "dag_trigger",
        "dag_monitor",
        "airflow_admin",
        # data_movement
        "airbyte_pipeline",
        "airbyte_sync",
        "airbyte_inventory",
        "airbyte_manage",
        "airflow_teradata_load",
        # dbt_management
        "dbt_execute",
        "dbt_docs",
        "dbt_info",
        "dbt_generate_model",
        "dbt_project",
        # metadata_discovery
        "teradata_discover",
        "teradata_analyze",
        # connection_profiles
        "connection_profiles",
        # ttu_tools
        "ttu_execute",
    }

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_all_22_tools_registered(self, mock_orch_cls):
        """create_app should register all 22 router tools."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        registered_names = {t.name for t in _registered_tools(app)}
        assert self.EXPECTED_TOOLS.issubset(registered_names), (
            f"Missing tools: {self.EXPECTED_TOOLS - registered_names}"
        )

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_tool_count_is_22(self, mock_orch_cls):
        """Exactly 22 tools should be registered."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        registered_count = len(_registered_tools(app))
        assert registered_count == 22, f"Expected 22 tools, got {registered_count}"

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_pipeline_management_tools_registered(self, mock_orch_cls):
        """Pipeline management module should register its 5 tools."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        registered_names = {t.name for t in _registered_tools(app)}
        expected = {
            "pipeline_status", "pipeline_control", "pipeline_deploy",
            "pipeline_validate", "airflow_connections",
        }
        assert expected.issubset(registered_names)

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_data_movement_tools_registered(self, mock_orch_cls):
        """Data movement module should register its 5 tools."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        registered_names = {t.name for t in _registered_tools(app)}
        expected = {
            "airbyte_pipeline", "airbyte_sync", "airbyte_inventory",
            "airbyte_manage", "airflow_teradata_load",
        }
        assert expected.issubset(registered_names)

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_dbt_tools_registered(self, mock_orch_cls):
        """dbt management module should register its 5 tools."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        registered_names = {t.name for t in _registered_tools(app)}
        expected = {"dbt_execute", "dbt_docs", "dbt_info", "dbt_generate_model", "dbt_project"}
        assert expected.issubset(registered_names)

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_metadata_tools_registered(self, mock_orch_cls):
        """Metadata discovery module should register its 2 tools."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        registered_names = {t.name for t in _registered_tools(app)}
        expected = {"teradata_discover", "teradata_analyze"}
        assert expected.issubset(registered_names)

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_orchestration_tools_registered(self, mock_orch_cls):
        """Orchestration execution module should register its 3 tools."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        registered_names = {t.name for t in _registered_tools(app)}
        expected = {"dag_trigger", "dag_monitor", "airflow_admin"}
        assert expected.issubset(registered_names)

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_connection_profiles_tool_registered(self, mock_orch_cls):
        """Connection profiles module should register its 1 tool."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        app = server.create_app()

        registered_names = {t.name for t in _registered_tools(app)}
        assert "connection_profiles" in registered_names

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_tool_registration_error_is_non_fatal(self, mock_orch_cls):
        """If one tool module fails to register, the others should still succeed."""
        mock_orch_cls.return_value = _make_mock_orchestrator()

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)

        # Force dbt_management.register_dbt_tools to raise
        with patch(
            "teradata_etl_mcp_server.server.dbt_management.register_dbt_tools",
            side_effect=Exception("dbt import error"),
        ):
            app = server.create_app()

        registered_names = {t.name for t in _registered_tools(app)}
        # dbt tools should be absent, but others should still be present
        assert "dbt_execute" not in registered_names
        assert "pipeline_status" in registered_names


# ---------------------------------------------------------------------------
# Tests: Startup Validation
# ---------------------------------------------------------------------------


class TestValidateStartup:
    """Tests for TeradataETLMCPServer._validate_startup."""

    @patch.dict(os.environ, {}, clear=True)
    def test_validation_skipped_when_disabled(self):
        """When validate_on_startup is False, _validate_startup should return True."""
        settings = _make_mock_settings()
        settings.mcp.validate_on_startup = False

        server = TeradataETLMCPServer(settings=settings)
        assert server._validate_startup() is True

    @patch.dict(os.environ, {}, clear=True)
    def test_validation_passes_when_services_ok(self):
        """When all services are healthy, validation should return True."""
        settings = _make_mock_settings()
        settings.mcp.validate_on_startup = True
        settings.validate_connectivity.return_value = {
            "valid": True,
            "services": {
                "teradata": {"status": "ok", "message": "Connected", "latency_ms": 5},
            },
            "warnings": [],
            "errors": [],
        }

        server = TeradataETLMCPServer(settings=settings)
        assert server._validate_startup() is True

    @patch.dict(os.environ, {}, clear=True)
    def test_validation_fails_with_fail_fast(self):
        """When validation fails and fail_fast is True, should return False."""
        settings = _make_mock_settings()
        settings.mcp.validate_on_startup = True
        settings.mcp.fail_fast_on_startup = True
        settings.validate_connectivity.return_value = {
            "valid": False,
            "services": {
                "teradata": {"status": "error", "message": "Connection refused"},
            },
            "warnings": [],
            "errors": ["Teradata: Connection refused"],
        }

        server = TeradataETLMCPServer(settings=settings)
        assert server._validate_startup() is False

    @patch.dict(os.environ, {}, clear=True)
    def test_validation_warns_without_fail_fast(self):
        """When validation fails but fail_fast is False, should return True."""
        settings = _make_mock_settings()
        settings.mcp.validate_on_startup = True
        settings.mcp.fail_fast_on_startup = False
        settings.validate_connectivity.return_value = {
            "valid": False,
            "services": {
                "teradata": {"status": "error", "message": "Connection refused"},
            },
            "warnings": [],
            "errors": ["Teradata: Connection refused"],
        }

        server = TeradataETLMCPServer(settings=settings)
        assert server._validate_startup() is True

    @patch.dict(os.environ, {}, clear=True)
    def test_validation_exception_with_fail_fast_returns_false(self):
        """If validate_connectivity raises and fail_fast is True, return False."""
        settings = _make_mock_settings()
        settings.mcp.validate_on_startup = True
        settings.mcp.fail_fast_on_startup = True
        settings.validate_connectivity.side_effect = Exception("Network error")

        server = TeradataETLMCPServer(settings=settings)
        assert server._validate_startup() is False

    @patch.dict(os.environ, {}, clear=True)
    def test_validation_exception_without_fail_fast_returns_true(self):
        """If validate_connectivity raises and fail_fast is False, return True."""
        settings = _make_mock_settings()
        settings.mcp.validate_on_startup = True
        settings.mcp.fail_fast_on_startup = False
        settings.validate_connectivity.side_effect = Exception("Network error")

        server = TeradataETLMCPServer(settings=settings)
        assert server._validate_startup() is True


# ---------------------------------------------------------------------------
# Tests: Cleanup
# ---------------------------------------------------------------------------


class TestCleanup:
    """Tests for TeradataETLMCPServer.cleanup."""

    @pytest.mark.asyncio
    @patch.dict(os.environ, {}, clear=True)
    async def test_cleanup_calls_orchestrator_cleanup(self):
        """cleanup() should call orchestrator.cleanup() when orchestrator is set."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        server.orchestrator = _make_mock_orchestrator()

        await server.cleanup()

        server.orchestrator.cleanup.assert_awaited_once()

    @pytest.mark.asyncio
    @patch.dict(os.environ, {}, clear=True)
    async def test_cleanup_handles_no_orchestrator(self):
        """cleanup() should not fail when orchestrator is None."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        server.orchestrator = None

        # Should not raise
        await server.cleanup()

    @pytest.mark.asyncio
    @patch.dict(os.environ, {}, clear=True)
    async def test_cleanup_handles_orchestrator_error(self):
        """cleanup() should log but not raise when orchestrator.cleanup() fails."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)

        mock_orch = _make_mock_orchestrator()
        mock_orch.cleanup = AsyncMock(side_effect=Exception("Cleanup failed"))
        server.orchestrator = mock_orch

        # Should not raise
        await server.cleanup()


# ---------------------------------------------------------------------------
# Tests: Module-Level Factory Function
# ---------------------------------------------------------------------------


class TestCreateAppFactory:
    """Tests for the module-level create_app() factory."""

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_create_app_function(self, mock_orch_cls):
        """The module-level create_app() should return a FastMCP instance."""
        mock_orch_cls.return_value = _make_mock_orchestrator()
        settings = _make_mock_settings()

        from fastmcp import FastMCP
        app = create_app(settings=settings)
        assert isinstance(app, FastMCP)

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_create_app_with_lifespan_sets_global_instance(self, mock_orch_cls):
        """create_app_with_lifespan should set the global server instance."""
        mock_orch_cls.return_value = _make_mock_orchestrator()
        settings = _make_mock_settings()

        app = create_app_with_lifespan(settings=settings)

        from fastmcp import FastMCP
        assert isinstance(app, FastMCP)
        assert get_server_instance() is not None


# ---------------------------------------------------------------------------
# Tests: Global Instance Management
# ---------------------------------------------------------------------------


class TestGlobalInstanceManagement:
    """Tests for get/set_server_instance and convenience getters."""

    @patch.dict(os.environ, {}, clear=True)
    def test_set_and_get_server_instance(self):
        """set_server_instance/get_server_instance round-trip."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)

        set_server_instance(server)
        assert get_server_instance() is server

    @patch.dict(os.environ, {}, clear=True)
    def test_get_orchestrator_returns_none_without_server(self):
        """get_orchestrator() returns None when no global server is set."""
        # Reset global
        import teradata_etl_mcp_server.server as srv_mod
        srv_mod._server_instance = None

        assert get_orchestrator() is None

    @patch.dict(os.environ, {}, clear=True)
    def test_get_orchestrator_returns_orchestrator(self):
        """get_orchestrator() returns the server's orchestrator."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        mock_orch = _make_mock_orchestrator()
        server.orchestrator = mock_orch
        set_server_instance(server)

        assert get_orchestrator() is mock_orch

    @patch.dict(os.environ, {}, clear=True)
    def test_get_settings_returns_none_without_server(self):
        """get_settings() returns None when no global server is set."""
        import teradata_etl_mcp_server.server as srv_mod
        srv_mod._server_instance = None

        assert get_settings() is None

    @patch.dict(os.environ, {}, clear=True)
    def test_get_settings_returns_settings(self):
        """get_settings() returns the server's settings."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        set_server_instance(server)

        assert get_settings() is settings

    @patch.dict(os.environ, {}, clear=True)
    def test_get_logger_returns_server_logger(self):
        """get_logger() returns the server's logger when instance exists."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        set_server_instance(server)

        assert get_logger() is server.logger

    @patch.dict(os.environ, {}, clear=True)
    def test_get_logger_returns_fallback_without_server(self):
        """get_logger() returns a fallback logger when no server instance exists."""
        import teradata_etl_mcp_server.server as srv_mod
        srv_mod._server_instance = None

        logger = get_logger()
        assert isinstance(logger, logging.Logger)
        assert logger.name == "teradata_etl_mcp_server"


# ---------------------------------------------------------------------------
# Tests: Lifespan Context Manager
# ---------------------------------------------------------------------------


class TestLifespan:
    """Tests for the async lifespan context manager."""

    @pytest.mark.asyncio
    @patch.dict(os.environ, {}, clear=True)
    async def test_lifespan_calls_cleanup_on_exit(self):
        """On exiting the lifespan context, server.cleanup() should be called."""
        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        server.orchestrator = _make_mock_orchestrator()
        set_server_instance(server)

        mock_app = MagicMock()

        with patch.object(server, "cleanup", new_callable=AsyncMock) as mock_cleanup:
            async with lifespan(mock_app):
                pass  # simulate server running
            mock_cleanup.assert_awaited_once()

    @pytest.mark.asyncio
    @patch.dict(os.environ, {}, clear=True)
    async def test_lifespan_handles_no_server_instance(self):
        """Lifespan should not fail when no global server instance is set."""
        import teradata_etl_mcp_server.server as srv_mod
        srv_mod._server_instance = None

        mock_app = MagicMock()
        async with lifespan(mock_app):
            pass  # should not raise


# ---------------------------------------------------------------------------
# Tests: Orchestrator Initialization
# ---------------------------------------------------------------------------


class TestInitializeOrchestrator:
    """Tests for TeradataETLMCPServer._initialize_orchestrator."""

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_initializes_orchestrator(self, mock_orch_cls):
        """_initialize_orchestrator should create a PipelineOrchestrator."""
        mock_orch = _make_mock_orchestrator()
        mock_orch_cls.return_value = mock_orch

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        result = server._initialize_orchestrator()

        mock_orch_cls.assert_called_once_with(settings)
        assert result is mock_orch

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_preloads_airbyte_registry(self, mock_orch_cls):
        """_initialize_orchestrator should attempt to preload the Airbyte registry."""
        mock_orch = _make_mock_orchestrator()
        mock_orch_cls.return_value = mock_orch

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)
        server._initialize_orchestrator()

        mock_orch.preload_airbyte_registry.assert_called_once()

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_preload_failure_does_not_raise(self, mock_orch_cls):
        """If preload_airbyte_registry fails, it should log a warning but not raise."""
        mock_orch = _make_mock_orchestrator()
        mock_orch.preload_airbyte_registry.side_effect = Exception("Network error")
        mock_orch_cls.return_value = mock_orch

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)

        # Should not raise
        result = server._initialize_orchestrator()
        assert result is mock_orch

    @patch.dict(os.environ, {}, clear=True)
    @patch("teradata_etl_mcp_server.server.PipelineOrchestrator")
    def test_orchestrator_creation_failure_raises(self, mock_orch_cls):
        """If PipelineOrchestrator() itself fails, the exception should propagate."""
        mock_orch_cls.side_effect = Exception("Fatal init error")

        settings = _make_mock_settings()
        server = TeradataETLMCPServer(settings=settings)

        with pytest.raises(Exception, match="Fatal init error"):
            server._initialize_orchestrator()


# ---------------------------------------------------------------------------
# Tests: Version Metadata
# ---------------------------------------------------------------------------


class TestVersionMetadata:
    """Tests for module-level metadata constants."""

    def test_version_is_string(self):
        assert isinstance(__version__, str)
        assert __version__ == package_version
