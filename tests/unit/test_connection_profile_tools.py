"""Tests for the connection profile MCP tools."""

from unittest.mock import MagicMock

import pytest

from teradata_etl_mcp_server.credential_resolver import ProfileSummary
from teradata_etl_mcp_server.tools.connection_profiles import register_connection_profile_tools


@pytest.fixture
def mock_orchestrator():
    """Create a mock orchestrator with a credential resolver."""
    orch = MagicMock()
    resolver = MagicMock()
    resolver.is_configured = True
    resolver.guard_configured.return_value = None
    resolver.list_profiles.return_value = [
        ProfileSummary(name="my_postgres", description="Test Postgres"),
        ProfileSummary(name="prod_teradata", description="Production Teradata"),
    ]
    resolver.reload.return_value = None
    orch.credential_resolver = resolver
    return orch


@pytest.fixture
def unconfigured_orchestrator():
    """Create a mock orchestrator with an unconfigured credential resolver."""
    orch = MagicMock()
    resolver = MagicMock()
    resolver.is_configured = False
    resolver.guard_configured.return_value = {
        "success": False,
        "error": (
            "Action required: connections.yaml is not configured. "
            "Do not create files or use placeholder credentials. "
            "Ask the user to create this file by copying "
            "connections.yaml.example and editing it with their real credentials, "
            "then call connection_profiles(action='reload')."
        ),
        "searched_locations": ["~/fake/path/connections.yaml"],
        "setup_instructions": [
            "1. Ask the user to copy connections.yaml.example to connections.yaml",
            "2. Ask the user to edit it with their real credentials",
            "3. Call connection_profiles(action='reload') to pick up changes",
        ],
    }
    resolver.list_profiles.return_value = []
    resolver.reload.return_value = None
    orch.credential_resolver = resolver
    return orch


@pytest.fixture
def malformed_yaml_orchestrator():
    """Create a mock orchestrator where connections.yaml exists but is malformed."""
    orch = MagicMock()
    resolver = MagicMock()
    resolver.is_configured = False
    resolver.guard_configured.return_value = {
        "success": False,
        "error": (
            "Action required: connections.yaml at "
            "~/fake/path/connections.yaml could not be parsed: "
            "YAML syntax error at line 5, column 3. "
            "Do not create files or use placeholder credentials. "
            "Ask the user to fix the YAML formatting, "
            "then call connection_profiles(action='reload')."
        ),
        "searched_locations": ["~/fake/path/connections.yaml"],
        "setup_instructions": [
            "1. Show the user the exact parse error above",
            "2. Ask the user to fix the YAML formatting in connections.yaml",
            "3. Call connection_profiles(action='reload') to pick up changes",
        ],
    }
    resolver.list_profiles.return_value = []
    resolver.reload.return_value = None
    orch.credential_resolver = resolver
    return orch


@pytest.fixture
def tools(mock_orchestrator):
    """Register and return the connection profile tools."""
    return register_connection_profile_tools(mock_orchestrator)


class TestListConnectionProfiles:
    @pytest.mark.asyncio
    async def test_returns_profiles(self, tools):
        result = await tools["connection_profiles"](action="list")
        assert result["success"] is True
        assert result["total"] == 2
        assert len(result["profiles"]) == 2
        assert result["profiles"][0]["name"] == "my_postgres"
        assert result["profiles"][0]["description"] == "Test Postgres"
        assert result["profiles"][1]["name"] == "prod_teradata"

    @pytest.mark.asyncio
    async def test_no_secrets_in_profiles(self, tools):
        result = await tools["connection_profiles"](action="list")
        for profile in result["profiles"]:
            assert "password" not in profile
            assert "host" not in profile
            assert "username" not in profile

    @pytest.mark.asyncio
    async def test_error_handling(self, mock_orchestrator):
        mock_orchestrator.credential_resolver.list_profiles.side_effect = Exception("fail")
        tools = register_connection_profile_tools(mock_orchestrator)
        result = await tools["connection_profiles"](action="list")
        assert result["success"] is False
        assert "Exception" in result["error"]  # safe_error_message includes type


class TestReloadConnectionProfiles:
    @pytest.mark.asyncio
    async def test_reload_calls_resolver(self, tools, mock_orchestrator):
        result = await tools["connection_profiles"](action="reload")
        assert result["success"] is True
        assert result["profiles_loaded"] == 2
        mock_orchestrator.credential_resolver.reload.assert_called_once()

    @pytest.mark.asyncio
    async def test_reload_error_handling(self, mock_orchestrator):
        mock_orchestrator.credential_resolver.reload.side_effect = Exception("reload failed")
        tools = register_connection_profile_tools(mock_orchestrator)
        result = await tools["connection_profiles"](action="reload")
        assert result["success"] is False
        assert "Exception" in result["error"]  # safe_error_message includes type


class TestUnconfiguredListProfiles:
    @pytest.mark.asyncio
    async def test_returns_setup_instructions_when_unconfigured(self, unconfigured_orchestrator):
        tools = register_connection_profile_tools(unconfigured_orchestrator)
        result = await tools["connection_profiles"](action="list")
        assert result["success"] is False
        assert "Action required" in result["error"]
        assert "Do not create" in result["error"]
        assert result["profiles"] == []
        assert "setup_instructions" in result
        assert len(result["setup_instructions"]) > 0
        assert "searched_locations" in result

    @pytest.mark.asyncio
    async def test_error_warns_against_placeholder_credentials(self, unconfigured_orchestrator):
        tools = register_connection_profile_tools(unconfigured_orchestrator)
        result = await tools["connection_profiles"](action="list")
        assert "placeholder credentials" in result["error"]

    @pytest.mark.asyncio
    async def test_instructions_say_ask_the_user(self, unconfigured_orchestrator):
        tools = register_connection_profile_tools(unconfigured_orchestrator)
        result = await tools["connection_profiles"](action="list")
        assert any("Ask the user" in s for s in result["setup_instructions"])


class TestUnconfiguredReloadProfiles:
    @pytest.mark.asyncio
    async def test_returns_setup_instructions_after_failed_reload(self, unconfigured_orchestrator):
        tools = register_connection_profile_tools(unconfigured_orchestrator)
        result = await tools["connection_profiles"](action="reload")
        assert result["success"] is False
        assert "Action required" in result["error"]
        assert "not configured" in result["error"]
        assert result["profiles_loaded"] == 0
        assert "setup_instructions" in result
        assert "searched_locations" in result


class TestMalformedYamlListProfiles:
    @pytest.mark.asyncio
    async def test_parse_error_context_flows_through(self, malformed_yaml_orchestrator):
        tools = register_connection_profile_tools(malformed_yaml_orchestrator)
        result = await tools["connection_profiles"](action="list")
        assert result["success"] is False
        assert "could not be parsed" in result["error"]
        assert "line 5" in result["error"]
        assert "column 3" in result["error"]
        assert result["profiles"] == []

    @pytest.mark.asyncio
    async def test_parse_error_instructions_say_fix_yaml(self, malformed_yaml_orchestrator):
        tools = register_connection_profile_tools(malformed_yaml_orchestrator)
        result = await tools["connection_profiles"](action="list")
        assert any("fix the YAML" in s for s in result["setup_instructions"])


class TestMalformedYamlReloadProfiles:
    @pytest.mark.asyncio
    async def test_parse_error_context_after_reload(self, malformed_yaml_orchestrator):
        tools = register_connection_profile_tools(malformed_yaml_orchestrator)
        result = await tools["connection_profiles"](action="reload")
        assert result["success"] is False
        assert "could not be parsed" in result["error"]
        assert "line 5" in result["error"]
        assert result["profiles_loaded"] == 0
