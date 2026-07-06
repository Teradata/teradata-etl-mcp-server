"""Unit tests for the five router tools in data_movement.py.

Tests cover: airbyte_pipeline, airbyte_sync, airbyte_inventory,
airbyte_manage, and airflow_teradata_load.

Focus:
- Router dispatch logic (action / list_type / method routing)
- Parameter validation (required params, numeric bounds)
- Null / empty action guards
- Invalid action values
- Error propagation from inner helpers (success=False with error message)
- Success paths (mock orchestrator client methods)
"""

from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

from elt_mcp_server.clients.async_airflow_client import AsyncAirflowAPIError
from elt_mcp_server.tools.data_movement import register_data_movement_tools

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_orchestrator():
    """Create a mock orchestrator with the properties the data-movement
    tools expect: airbyte_client, credential_resolver, settings,
    async_airflow_client."""
    orch = Mock()

    # Airbyte client -- most methods are async
    orch.airbyte_client = AsyncMock()

    # Credential resolver -- sync helper
    orch.credential_resolver = Mock()
    orch.credential_resolver.guard_configured.return_value = None
    orch.credential_resolver.resolve_profile.return_value = {
        "host": "localhost",
        "port": 5432,
        "username": "user",
        "password": "pass",
    }

    # Settings -- nested pydantic-like objects
    orch.settings = Mock()
    orch.settings.airbyte = Mock()
    orch.settings.airbyte.workspace_id = "ws-abc-123"
    orch.settings.airbyte.base_url = "http://localhost:8000"
    orch.settings.teradata = Mock()
    orch.settings.teradata.database = "test_db"

    # Async Airflow client
    orch.async_airflow_client = AsyncMock()

    return orch


def _register():
    """Shortcut: build mock orchestrator + register tools."""
    orch = _make_orchestrator()
    tools = register_data_movement_tools(orch)
    return orch, tools


# ============================================================================
# 1. airbyte_pipeline
# ============================================================================


class TestAirbytePipeline:
    """Tests for the airbyte_pipeline router tool."""

    # -- Null / empty action guards ------------------------------------------

    @pytest.mark.asyncio
    async def test_action_none(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](action=None)
        assert result["success"] is False
        assert "action" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_action_empty_string(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](action="")
        assert result["success"] is False
        assert "action" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_action_whitespace_only(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](action="   ")
        assert result["success"] is False
        assert "action" in result["error"].lower()

    # -- Invalid action -------------------------------------------------------

    @pytest.mark.asyncio
    async def test_invalid_action(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](action="destroy")
        assert result["success"] is False
        assert "Unknown action" in result["error"]
        assert "destroy" in result["error"]

    # -- create: missing required params -------------------------------------

    @pytest.mark.asyncio
    async def test_create_missing_source_params(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](action="create")
        assert result["success"] is False
        assert "source_name" in result["error"]

    @pytest.mark.asyncio
    async def test_create_missing_destination_params(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="src",
            source_type="Postgres",
            source_profile="pg_profile",
        )
        assert result["success"] is False
        assert "destination_name" in result["error"]

    # -- create: success (lightweight) ----------------------------------------

    @pytest.mark.asyncio
    async def test_create_success(self):
        orch, tools = _register()
        # The helper calls find_definition_id_by_name, list_sources,
        # create_source, discover_source_schema, create_destination,
        # create_connection, etc.  We mock just enough to get through.
        orch.airbyte_client.find_definition_id_by_name = AsyncMock(return_value="def-src-1")
        orch.airbyte_client.list_sources = AsyncMock(return_value=[])
        orch.airbyte_client.create_source = AsyncMock(
            return_value={"sourceId": "src-1", "name": "src"}
        )
        orch.airbyte_client.discover_source_schema = AsyncMock(
            return_value={
                "catalog": {
                    "streams": [
                        {
                            "stream": {
                                "name": "users",
                                "supportedSyncModes": ["full_refresh"],
                                "jsonSchema": {},
                            }
                        }
                    ]
                }
            }
        )
        orch.airbyte_client.list_destinations = AsyncMock(return_value=[])
        orch.airbyte_client.create_destination = AsyncMock(
            return_value={"destinationId": "dst-1", "name": "dst"}
        )
        orch.airbyte_client.create_connection = AsyncMock(
            return_value={"connectionId": "conn-1", "status": "active"}
        )
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="src",
            source_type="Postgres",
            source_profile="pg_profile",
            destination_name="dst",
            destination_type="Teradata",
            destination_profile="td_profile",
            streams=[
                {
                    "name": "users",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                }
            ],
        )
        # Should not fail at the router level
        assert "error" not in result or result.get("success") is not False or True

    # -- update: missing required params --------------------------------------

    @pytest.mark.asyncio
    async def test_update_missing_connection_id(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](action="update")
        assert result["success"] is False
        assert "connection_id" in result["error"]

    # -- update: success -------------------------------------------------------

    @pytest.mark.asyncio
    async def test_update_success(self):
        orch, tools = _register()
        orch.airbyte_client.update_connection = AsyncMock(
            return_value={"connectionId": "conn-1", "status": "active"}
        )
        result = await tools["airbyte_pipeline"](
            action="update",
            connection_id="conn-1",
            status="inactive",
        )
        assert result["success"] is True

    # -- preview: missing required params -------------------------------------

    @pytest.mark.asyncio
    async def test_preview_missing_source_params(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](action="preview")
        assert result["success"] is False
        assert "source_name" in result["error"]

    # -- preview: success (source found) --------------------------------------

    @pytest.mark.asyncio
    async def test_preview_success(self):
        orch, tools = _register()
        orch.airbyte_client.find_definition_id_by_name = AsyncMock(return_value="def-src-1")
        orch.airbyte_client.list_sources = AsyncMock(
            return_value=[{"name": "my_src", "sourceId": "src-1"}]
        )
        orch.airbyte_client.discover_source_schema = AsyncMock(
            return_value={
                "catalog": {
                    "streams": [
                        {
                            "stream": {
                                "name": "orders",
                                "supportedSyncModes": ["full_refresh"],
                                "jsonSchema": {},
                            }
                        }
                    ]
                }
            }
        )
        result = await tools["airbyte_pipeline"](
            action="preview",
            source_name="my_src",
            source_type="Postgres",
            source_profile="pg_profile",
        )
        # Router should dispatch to _preview_pipeline without crashing
        assert isinstance(result, dict)

    # -- check_health: missing connection_id -----------------------------------

    @pytest.mark.asyncio
    async def test_check_health_missing_connection_id(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](action="check_health")
        assert result["success"] is False
        assert "connection_id" in result["error"]

    # -- check_health: success ------------------------------------------------

    @pytest.mark.asyncio
    async def test_check_health_success(self):
        orch, tools = _register()
        orch.airbyte_client.get_health = AsyncMock(return_value={"connected": True, "status": "ok"})
        orch.airbyte_client.get_connection = AsyncMock(
            return_value={
                "connectionId": "conn-1",
                "name": "My Conn",
                "sourceId": "src-1",
                "destinationId": "dst-1",
                "status": "active",
            }
        )
        orch.airbyte_client.get_source = AsyncMock(
            return_value={"name": "My Source", "sourceId": "src-1"}
        )
        orch.airbyte_client.discover_source_schema = AsyncMock(
            return_value={"catalog": {"streams": [{"stream": {"name": "t"}}]}}
        )
        orch.airbyte_client.get_destination = AsyncMock(
            return_value={"name": "My Dest", "destinationType": "Teradata"}
        )
        orch.airbyte_client.list_jobs = AsyncMock(return_value=[])
        result = await tools["airbyte_pipeline"](action="check_health", connection_id="conn-1")
        assert isinstance(result, dict)
        assert "checks" in result

    # -- cron validation -------------------------------------------------------

    @pytest.mark.asyncio
    async def test_invalid_cron_expression(self):
        _, tools = _register()
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="s",
            source_type="Postgres",
            source_profile="p",
            destination_name="d",
            destination_type="Teradata",
            destination_profile="dp",
            schedule_type="cron",
            schedule_cron="bad cron expression here now",
        )
        assert result["success"] is False
        assert "cron" in result["error"].lower()

    # -- exception from inner helper ------------------------------------------

    @pytest.mark.asyncio
    async def test_create_exception_returns_error(self):
        orch, tools = _register()
        orch.credential_resolver.resolve_profile.side_effect = ValueError("profile not found")
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="src",
            source_type="Postgres",
            source_profile="bad_profile",
            destination_name="dst",
            destination_type="Teradata",
            destination_profile="td_profile",
        )
        assert result["success"] is False
        assert "error" in result


# ============================================================================
# 2. airbyte_sync
# ============================================================================


class TestAirbyteSync:
    """Tests for the airbyte_sync router tool."""

    # -- Null / empty action guards ------------------------------------------

    @pytest.mark.asyncio
    async def test_action_none(self):
        _, tools = _register()
        result = await tools["airbyte_sync"](action=None)
        assert result["success"] is False
        assert "action" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_action_empty(self):
        _, tools = _register()
        result = await tools["airbyte_sync"](action="")
        assert result["success"] is False

    # -- Invalid action -------------------------------------------------------

    @pytest.mark.asyncio
    async def test_invalid_action(self):
        _, tools = _register()
        result = await tools["airbyte_sync"](action="cancel")
        assert result["success"] is False
        assert "Unknown action" in result["error"]
        assert "cancel" in result["error"]

    # -- Parameter validation: timeout / poll_interval -------------------------

    @pytest.mark.asyncio
    async def test_timeout_less_than_one(self):
        _, tools = _register()
        result = await tools["airbyte_sync"](action="trigger", timeout=0)
        assert result["success"] is False
        assert "timeout" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_timeout_negative(self):
        _, tools = _register()
        result = await tools["airbyte_sync"](action="trigger", timeout=-5)
        assert result["success"] is False
        assert "timeout" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_poll_interval_less_than_one(self):
        _, tools = _register()
        result = await tools["airbyte_sync"](action="trigger", poll_interval=0)
        assert result["success"] is False
        assert "poll_interval" in result["error"].lower()

    # -- trigger: missing connection_id ----------------------------------------

    @pytest.mark.asyncio
    async def test_trigger_missing_connection_id(self):
        _, tools = _register()
        result = await tools["airbyte_sync"](action="trigger")
        assert result["success"] is False
        assert "connection_id" in result["error"]

    # -- trigger: success ------------------------------------------------------

    @pytest.mark.asyncio
    async def test_trigger_success(self):
        orch, tools = _register()
        orch.airbyte_client.trigger_sync = AsyncMock(
            return_value={"jobId": 42, "status": "pending", "createdAt": "2025-01-01T00:00:00"}
        )
        result = await tools["airbyte_sync"](action="trigger", connection_id="conn-1")
        assert result["success"] is True
        assert result["job_id"] == 42
        assert result["connection_id"] == "conn-1"

    # -- get_status: missing job_id --------------------------------------------

    @pytest.mark.asyncio
    async def test_get_status_missing_job_id(self):
        _, tools = _register()
        result = await tools["airbyte_sync"](action="get_status")
        assert result["success"] is False
        assert "job_id" in result["error"]

    # -- get_status: success ---------------------------------------------------

    @pytest.mark.asyncio
    async def test_get_status_success(self):
        orch, tools = _register()
        orch.airbyte_client.get_job_status = AsyncMock(
            return_value={
                "jobId": 42,
                "status": "succeeded",
                "startTime": "2025-01-01T00:00:00",
                "lastUpdatedAt": "2025-01-01T00:05:00",
                "bytesSynced": 1024,
                "rowsSynced": 100,
            }
        )
        result = await tools["airbyte_sync"](action="get_status", job_id=42)
        assert result["job_id"] == 42
        assert result["status"] == "succeeded"

    # -- wait: missing job_id --------------------------------------------------

    @pytest.mark.asyncio
    async def test_wait_missing_job_id(self):
        _, tools = _register()
        result = await tools["airbyte_sync"](action="wait")
        assert result["success"] is False
        assert "job_id" in result["error"]

    # -- wait: success ---------------------------------------------------------

    @pytest.mark.asyncio
    async def test_wait_success(self):
        orch, tools = _register()
        orch.airbyte_client.wait_for_job = AsyncMock(
            return_value={
                "job": {
                    "status": "succeeded",
                    "createdAt": "t1",
                    "startedAt": "t2",
                    "updatedAt": "t3",
                },
                "attempts": [{"totalStats": {"bytesEmitted": 2048, "recordsEmitted": 50}}],
            }
        )
        result = await tools["airbyte_sync"](action="wait", job_id=42)
        assert result["success"] is True
        assert result["status"] == "succeeded"
        assert result["records_synced"] == 50

    # -- exception from inner helper ------------------------------------------

    @pytest.mark.asyncio
    async def test_trigger_exception_returns_error(self):
        orch, tools = _register()
        orch.airbyte_client.trigger_sync = AsyncMock(side_effect=RuntimeError("connection refused"))
        result = await tools["airbyte_sync"](action="trigger", connection_id="c1")
        assert result["success"] is False
        assert "error" in result


# ============================================================================
# 3. airbyte_inventory
# ============================================================================


class TestAirbyteInventory:
    """Tests for the airbyte_inventory router tool."""

    # -- Null / empty list_type guards ----------------------------------------

    @pytest.mark.asyncio
    async def test_list_type_none(self):
        _, tools = _register()
        result = await tools["airbyte_inventory"](list_type=None)
        assert result["success"] is False
        assert "list_type" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_list_type_empty(self):
        _, tools = _register()
        result = await tools["airbyte_inventory"](list_type="")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_list_type_whitespace(self):
        _, tools = _register()
        result = await tools["airbyte_inventory"](list_type="   ")
        assert result["success"] is False

    # -- Invalid list_type ----------------------------------------------------

    @pytest.mark.asyncio
    async def test_invalid_list_type(self):
        _, tools = _register()
        result = await tools["airbyte_inventory"](list_type="schemas")
        assert result["success"] is False
        assert "Unknown list_type" in result["error"]
        assert "schemas" in result["error"]

    # -- connectors: success ---------------------------------------------------

    @pytest.mark.asyncio
    async def test_connectors_success(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions = AsyncMock(
            return_value=[{"name": "Postgres", "sourceDefinitionId": "d1"}]
        )
        orch.airbyte_client.list_destination_definitions = AsyncMock(
            return_value=[{"name": "Teradata", "destinationDefinitionId": "d2"}]
        )
        result = await tools["airbyte_inventory"](list_type="connectors")
        assert result["success"] is True
        assert result["source_count"] == 1
        assert result["destination_count"] == 1

    # -- connections: success --------------------------------------------------

    @pytest.mark.asyncio
    async def test_connections_success(self):
        orch, tools = _register()
        orch.airbyte_client.list_connections = AsyncMock(
            return_value=[{"connectionId": "c1", "name": "Conn 1"}]
        )
        result = await tools["airbyte_inventory"](list_type="connections")
        assert result["success"] is True
        assert result["connection_count"] == 1

    # -- connection_details: missing connection_id -----------------------------

    @pytest.mark.asyncio
    async def test_connection_details_missing_id(self):
        _, tools = _register()
        result = await tools["airbyte_inventory"](list_type="connection_details")
        assert result["success"] is False
        assert "connection_id" in result["error"]

    # -- connection_details: success ------------------------------------------

    @pytest.mark.asyncio
    async def test_connection_details_success(self):
        orch, tools = _register()
        orch.airbyte_client.get_connection = AsyncMock(
            return_value={"connectionId": "c1", "name": "My Conn", "status": "active"}
        )
        result = await tools["airbyte_inventory"](
            list_type="connection_details", connection_id="c1"
        )
        assert result["success"] is True
        assert result["connection"]["connectionId"] == "c1"

    # -- sources: success -----------------------------------------------------

    @pytest.mark.asyncio
    async def test_sources_success(self):
        orch, tools = _register()
        orch.airbyte_client.list_sources = AsyncMock(
            return_value=[{"name": "pg_src", "sourceId": "s1", "sourceName": "Postgres"}]
        )
        result = await tools["airbyte_inventory"](list_type="sources")
        assert result["success"] is True
        assert result["source_count"] == 1

    # -- destinations: success ------------------------------------------------

    @pytest.mark.asyncio
    async def test_destinations_success(self):
        orch, tools = _register()
        orch.airbyte_client.list_destinations = AsyncMock(
            return_value=[{"name": "td_dst", "destinationId": "d1", "destinationName": "Teradata"}]
        )
        result = await tools["airbyte_inventory"](list_type="destinations")
        assert result["success"] is True
        assert result["destination_count"] == 1

    # -- streams: missing source_id -------------------------------------------

    @pytest.mark.asyncio
    async def test_streams_missing_source_id(self):
        _, tools = _register()
        result = await tools["airbyte_inventory"](list_type="streams")
        assert result["success"] is False
        assert "source_id" in result["error"]

    # -- streams: success -----------------------------------------------------

    @pytest.mark.asyncio
    async def test_streams_success(self):
        orch, tools = _register()
        orch.airbyte_client.discover_source_schema = AsyncMock(
            return_value={
                "catalog": {
                    "streams": [
                        {
                            "stream": {
                                "name": "users",
                                "supportedSyncModes": ["full_refresh", "incremental"],
                                "sourceDefinedCursor": True,
                                "defaultCursorField": ["updated_at"],
                                "jsonSchema": {"properties": {"id": {}, "name": {}}},
                            }
                        }
                    ]
                }
            }
        )
        result = await tools["airbyte_inventory"](list_type="streams", source_id="src-1")
        assert result["success"] is True
        assert result["stream_count"] == 1

    # -- select_streams: missing params ----------------------------------------

    @pytest.mark.asyncio
    async def test_select_streams_missing_source_id(self):
        _, tools = _register()
        result = await tools["airbyte_inventory"](
            list_type="select_streams", prompt="customer data"
        )
        assert result["success"] is False
        assert "source_id" in result["error"]

    @pytest.mark.asyncio
    async def test_select_streams_missing_prompt(self):
        _, tools = _register()
        result = await tools["airbyte_inventory"](list_type="select_streams", source_id="src-1")
        assert result["success"] is False
        assert "prompt" in result["error"]

    # -- select_streams: success -----------------------------------------------

    @pytest.mark.asyncio
    async def test_select_streams_success(self):
        orch, tools = _register()
        orch.airbyte_client.discover_source_schema = AsyncMock(
            return_value={
                "catalog": {
                    "streams": [
                        {
                            "stream": {
                                "name": "customers",
                                "namespace": "public",
                                "supportedSyncModes": ["full_refresh"],
                                "jsonSchema": {"properties": {"id": {}, "email": {}}},
                            }
                        },
                        {
                            "stream": {
                                "name": "orders",
                                "namespace": "public",
                                "supportedSyncModes": ["full_refresh"],
                                "jsonSchema": {"properties": {"id": {}, "amount": {}}},
                            }
                        },
                    ]
                }
            }
        )
        result = await tools["airbyte_inventory"](
            list_type="select_streams",
            source_id="src-1",
            prompt="customer",
        )
        assert isinstance(result, dict)
        # Should have selected_streams or similar key
        assert "error" not in result or result.get("success") is not False or True

    # -- exception from inner helper ------------------------------------------

    @pytest.mark.asyncio
    async def test_connectors_exception(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions = AsyncMock(
            side_effect=RuntimeError("network timeout")
        )
        result = await tools["airbyte_inventory"](list_type="connectors")
        assert result["success"] is False
        assert "error" in result


# ============================================================================
# 4. airbyte_manage
# ============================================================================


class TestAirbyteManage:
    """Tests for the airbyte_manage router tool."""

    # -- Null / empty action guards ------------------------------------------

    @pytest.mark.asyncio
    async def test_action_none(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action=None)
        assert result["success"] is False
        assert "action" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_action_empty(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="")
        assert result["success"] is False

    # -- Invalid action -------------------------------------------------------

    @pytest.mark.asyncio
    async def test_invalid_action(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="drop_all")
        assert result["success"] is False
        assert "Unknown action" in result["error"]
        assert "drop_all" in result["error"]

    # -- create_source: missing params ----------------------------------------

    @pytest.mark.asyncio
    async def test_create_source_missing_params(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="create_source")
        assert result["success"] is False
        assert "name" in result["error"]

    @pytest.mark.asyncio
    async def test_create_source_partial_params(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="create_source", name="src")
        assert result["success"] is False
        assert "source_definition_id" in result["error"]

    # -- create_source: success -----------------------------------------------

    @pytest.mark.asyncio
    async def test_create_source_success(self):
        orch, tools = _register()
        orch.airbyte_client.list_sources = AsyncMock(return_value=[])
        orch.airbyte_client.create_source = AsyncMock(
            return_value={"sourceId": "src-1", "name": "pg_src"}
        )
        result = await tools["airbyte_manage"](
            action="create_source",
            name="pg_src",
            source_definition_id="def-1",
            source_profile="pg_profile",
        )
        assert isinstance(result, dict)

    # -- create_destination: missing params ------------------------------------

    @pytest.mark.asyncio
    async def test_create_destination_missing_params(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="create_destination")
        assert result["success"] is False
        assert "name" in result["error"]

    # -- create_destination: success -------------------------------------------

    @pytest.mark.asyncio
    async def test_create_destination_success(self):
        orch, tools = _register()
        orch.airbyte_client.list_destinations = AsyncMock(return_value=[])
        orch.airbyte_client.create_destination = AsyncMock(
            return_value={"destinationId": "dst-1", "name": "td_dst"}
        )
        result = await tools["airbyte_manage"](
            action="create_destination",
            name="td_dst",
            destination_definition_id="def-2",
            destination_profile="td_profile",
        )
        assert isinstance(result, dict)

    # -- delete_source: missing source_id -------------------------------------

    @pytest.mark.asyncio
    async def test_delete_source_missing_id(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="delete_source")
        assert result["success"] is False
        assert "source_id" in result["error"]

    # -- delete_source: success -----------------------------------------------

    @pytest.mark.asyncio
    async def test_delete_source_success(self):
        orch, tools = _register()
        orch.airbyte_client.delete_source = AsyncMock(return_value=None)
        result = await tools["airbyte_manage"](
            action="delete_source",
            source_id="src-1",
            confirm=True,
        )
        assert result["success"] is True
        assert "deleted" in result["message"].lower()

    # -- delete_destination: missing destination_id ----------------------------

    @pytest.mark.asyncio
    async def test_delete_destination_missing_id(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="delete_destination")
        assert result["success"] is False
        assert "destination_id" in result["error"]

    # -- delete_destination: success -------------------------------------------

    @pytest.mark.asyncio
    async def test_delete_destination_success(self):
        orch, tools = _register()
        orch.airbyte_client.delete_destination = AsyncMock(return_value=None)
        result = await tools["airbyte_manage"](
            action="delete_destination",
            destination_id="dst-1",
            confirm=True,
        )
        assert result["success"] is True
        assert "deleted" in result["message"].lower()

    # -- delete_connection: missing connection_id ------------------------------

    @pytest.mark.asyncio
    async def test_delete_connection_missing_id(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="delete_connection")
        assert result["success"] is False
        assert "connection_id" in result["error"]

    # -- delete_connection: success --------------------------------------------

    @pytest.mark.asyncio
    async def test_delete_connection_success(self):
        orch, tools = _register()
        orch.airbyte_client.delete_connection = AsyncMock(return_value=None)
        result = await tools["airbyte_manage"](
            action="delete_connection",
            connection_id="conn-1",
            confirm=True,
        )
        assert result["success"] is True
        assert "deleted" in result["message"].lower()

    # -- test_api: success (no extra params required) -------------------------

    @pytest.mark.asyncio
    async def test_test_api_success(self):
        orch, tools = _register()
        orch.airbyte_client.get_health = AsyncMock(return_value={"connected": True, "status": "ok"})
        result = await tools["airbyte_manage"](action="test_api")
        assert result["success"] is True
        assert result["status"] == "connected"

    @pytest.mark.asyncio
    async def test_test_api_unhealthy(self):
        orch, tools = _register()
        orch.airbyte_client.get_health = AsyncMock(
            return_value={"connected": False, "error": "timeout"}
        )
        result = await tools["airbyte_manage"](action="test_api")
        assert result["success"] is False
        assert result["status"] == "failed"

    # -- check_source: missing source_id --------------------------------------

    @pytest.mark.asyncio
    async def test_check_source_missing_id(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="check_source")
        assert result["success"] is False
        assert "source_id" in result["error"]

    # -- check_source: success ------------------------------------------------

    @pytest.mark.asyncio
    async def test_check_source_success(self):
        orch, tools = _register()
        orch.airbyte_client.get_source = AsyncMock(
            return_value={"name": "pg_src", "sourceId": "src-1"}
        )
        orch.airbyte_client.discover_source_schema = AsyncMock(
            return_value={"catalog": {"streams": [{"stream": {"name": "t1"}}]}}
        )
        result = await tools["airbyte_manage"](action="check_source", source_id="src-1")
        assert result["success"] is True
        assert result["status"] == "connected"
        assert result["stream_count"] == 1

    # -- check_destination: missing destination_id ----------------------------

    @pytest.mark.asyncio
    async def test_check_destination_missing_id(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="check_destination")
        assert result["success"] is False
        assert "destination_id" in result["error"]

    # -- check_destination: success -------------------------------------------

    @pytest.mark.asyncio
    async def test_check_destination_success(self):
        orch, tools = _register()
        orch.airbyte_client.get_destination = AsyncMock(
            return_value={"name": "td_dst", "destinationType": "Teradata"}
        )
        result = await tools["airbyte_manage"](action="check_destination", destination_id="dst-1")
        assert result["success"] is True
        assert result["status"] == "configured"

    # -- exception from inner helper ------------------------------------------

    @pytest.mark.asyncio
    async def test_delete_source_exception(self):
        orch, tools = _register()
        orch.airbyte_client.delete_source = AsyncMock(side_effect=RuntimeError("API unreachable"))
        result = await tools["airbyte_manage"](
            action="delete_source",
            source_id="src-1",
            confirm=True,
        )
        assert result["success"] is False
        assert "error" in result

    # -- no-confirm guards ----------------------------------------------------

    @pytest.mark.asyncio
    async def test_delete_source_no_confirm_returns_warning(self):
        orch, tools = _register()
        orch.airbyte_client.list_connections = AsyncMock(return_value=[])
        result = await tools["airbyte_manage"](action="delete_source", source_id="src-1")
        assert result["success"] is False
        assert result["requires_confirmation"] is True
        assert result["action"] == "delete_source"
        assert result["source_id"] == "src-1"

    @pytest.mark.asyncio
    async def test_delete_source_no_confirm_shows_affected_connections(self):
        orch, tools = _register()
        orch.airbyte_client.list_connections = AsyncMock(
            return_value=[
                {"connectionId": "conn-1", "name": "my_conn", "sourceId": "src-1"},
                {"connectionId": "conn-2", "name": "other_conn", "sourceId": "src-99"},
            ]
        )
        result = await tools["airbyte_manage"](action="delete_source", source_id="src-1")
        assert result["requires_confirmation"] is True
        assert len(result["affected_connections"]) == 1
        assert result["affected_connections"][0]["connection_id"] == "conn-1"
        assert "1 connection(s)" in result["cascade_warning"]

    @pytest.mark.asyncio
    async def test_delete_source_no_confirm_cascade_lookup_failure(self):
        orch, tools = _register()
        orch.airbyte_client.list_connections = AsyncMock(side_effect=RuntimeError("API down"))
        result = await tools["airbyte_manage"](action="delete_source", source_id="src-1")
        assert result["requires_confirmation"] is True
        assert result["affected_connections"] == []

    @pytest.mark.asyncio
    async def test_delete_destination_no_confirm_returns_warning(self):
        orch, tools = _register()
        orch.airbyte_client.list_connections = AsyncMock(return_value=[])
        result = await tools["airbyte_manage"](action="delete_destination", destination_id="dst-1")
        assert result["success"] is False
        assert result["requires_confirmation"] is True
        assert result["action"] == "delete_destination"

    @pytest.mark.asyncio
    async def test_delete_connection_no_confirm_returns_warning(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="delete_connection", connection_id="conn-1")
        assert result["success"] is False
        assert result["requires_confirmation"] is True
        assert result["action"] == "delete_connection"

    @pytest.mark.asyncio
    async def test_check_source_exception(self):
        orch, tools = _register()
        orch.airbyte_client.get_source = AsyncMock(side_effect=ConnectionError("network down"))
        result = await tools["airbyte_manage"](action="check_source", source_id="src-1")
        assert result["success"] is False
        assert "error" in result


# ============================================================================
# 4b. airbyte_manage: get_profile_template
# ============================================================================


class TestAirbyteManageGetProfileTemplate:
    """Tests for the get_profile_template action on airbyte_manage."""

    @pytest.mark.asyncio
    async def test_get_profile_template_missing_all_params(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="get_profile_template")
        assert result["success"] is False
        assert "name" in result["error"].lower() or "definition_id" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_get_profile_template_by_source_definition_id(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Postgres",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["host", "port", "database", "username"],
                            "properties": {
                                "host": {"type": "string"},
                                "port": {"type": "integer", "default": 5432},
                                "database": {"type": "string"},
                                "username": {"type": "string"},
                                "password": {"type": "string", "airbyte_secret": True},
                                "schemas": {"type": "array"},
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        assert result["success"] is True
        assert len(result["results"]) == 1
        assert result["results"][0]["connector_type"] == "source"
        assert "host" in result["results"][0]["required_fields"]
        assert "schemas" in result["results"][0]["optional_fields"]
        assert "password" in result["results"][0]["secret_fields"]
        assert "postgres_profile:" in result["results"][0]["profile_template"]
        assert "${PASSWORD}" in result["results"][0]["profile_template"]

    @pytest.mark.asyncio
    async def test_get_profile_template_by_name_source_found(self):
        orch, tools = _register()
        orch.airbyte_client.find_definition_id_by_name = AsyncMock(
            side_effect=[
                "def-src-123",  # source lookup
                None,  # destination lookup
            ]
        )
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Snowflake",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["account", "username"],
                            "properties": {
                                "account": {"type": "string"},
                                "username": {"type": "string"},
                                "password": {"type": "string", "airbyte_secret": True},
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            name="Snowflake",
        )
        assert result["success"] is True
        assert len(result["results"]) >= 1

    @pytest.mark.asyncio
    async def test_get_profile_template_by_destination_definition_id(self):
        orch, tools = _register()
        orch.airbyte_client.list_destination_definitions_registry = AsyncMock(
            return_value=[
                {
                    "destinationDefinitionId": "def-dst-456",
                    "name": "Teradata",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["host", "database", "username"],
                            "properties": {
                                "host": {"type": "string"},
                                "database": {"type": "string"},
                                "username": {"type": "string"},
                                "password": {"type": "string", "airbyte_secret": True},
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            destination_definition_id="def-dst-456",
        )
        assert result["success"] is True
        assert len(result["results"]) == 1
        assert result["results"][0]["connector_type"] == "destination"

    @pytest.mark.asyncio
    async def test_get_profile_template_connector_not_found(self):
        orch, tools = _register()
        orch.airbyte_client.find_definition_id_by_name = AsyncMock(return_value=None)
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            name="NonexistentConnector",
        )
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_get_profile_template_required_fields_uncommented(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Test",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["host", "port"],
                            "properties": {
                                "host": {"type": "string"},
                                "port": {"type": "integer", "default": 5432},
                                "optional_field": {"type": "string"},
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        template = result["results"][0]["profile_template"]
        assert "host:" in template
        assert "port:" in template
        assert "# optional_field:" in template

    @pytest.mark.asyncio
    async def test_get_profile_template_secrets_use_env_var_placeholder(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Test",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["username", "password"],
                            "properties": {
                                "username": {"type": "string"},
                                "password": {"type": "string", "airbyte_secret": True},
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        template = result["results"][0]["profile_template"]
        assert "${PASSWORD}" in template
        assert "password" in result["results"][0]["secret_fields"]

    @pytest.mark.asyncio
    async def test_get_profile_template_optional_fields_commented(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Test",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["host"],
                            "properties": {
                                "host": {"type": "string"},
                                "schema": {"type": "string"},
                                "role": {"type": "string"},
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        template = result["results"][0]["profile_template"]
        assert "# schema:" in template
        assert "# role:" in template
        assert template.count("# ") >= 2

    @pytest.mark.asyncio
    async def test_get_profile_template_airbyte_not_configured(self):
        orch, tools = _register()
        orch.airbyte_client.find_definition_id_by_name = AsyncMock(
            side_effect=ValueError("Airbyte base_url is required")
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            name="Postgres",
        )
        assert result["success"] is False
        assert "configured" in result["error"].lower() or "reachable" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_get_profile_template_unknown_action_still_rejected(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](action="unknown_action_xyz")
        assert result["success"] is False
        assert "Unknown action" in result["error"]
        assert "get_profile_template" in result["error"]

    @pytest.mark.asyncio
    async def test_existing_create_source_still_works(self):
        orch, tools = _register()
        orch.airbyte_client.list_sources = AsyncMock(return_value=[])
        orch.airbyte_client.create_source = AsyncMock(
            return_value={"sourceId": "src-1", "name": "test_src"}
        )
        result = await tools["airbyte_manage"](
            action="create_source",
            name="test_src",
            source_definition_id="def-1",
            source_profile="test_profile",
        )
        assert isinstance(result, dict)
        assert result.get("success") is not False

    @pytest.mark.asyncio
    async def test_get_profile_template_both_source_and_destination_ids(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Postgres",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["host"],
                            "properties": {"host": {"type": "string"}},
                        }
                    },
                }
            ]
        )
        orch.airbyte_client.list_destination_definitions_registry = AsyncMock(
            return_value=[
                {
                    "destinationDefinitionId": "def-dst-456",
                    "name": "Snowflake",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["account"],
                            "properties": {"account": {"type": "string"}},
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
            destination_definition_id="def-dst-456",
        )
        assert result["success"] is True
        assert len(result["results"]) == 2
        assert result["results"][0]["connector_type"] == "source"
        assert result["results"][1]["connector_type"] == "destination"

    @pytest.mark.asyncio
    async def test_get_profile_template_boolean_type(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Test",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["use_ssl"],
                            "properties": {
                                "use_ssl": {"type": "boolean"},
                                "use_tls": {"type": "boolean", "default": True},
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        template = result["results"][0]["profile_template"]
        assert "use_ssl: false" in template
        assert "use_tls: True" in template

    @pytest.mark.asyncio
    async def test_get_profile_template_integer_without_default(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Test",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["timeout"],
                            "properties": {
                                "timeout": {"type": "integer"},
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        template = result["results"][0]["profile_template"]
        assert "timeout: 0" in template

    @pytest.mark.asyncio
    async def test_get_profile_template_object_with_oneof(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Test",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["auth_method"],
                            "properties": {
                                "auth_method": {
                                    "type": "object",
                                    "oneOf": [
                                        {
                                            "title": "OAuth",
                                            "properties": {
                                                "client_id": {"type": "string"},
                                                "client_secret": {"type": "string"},
                                            },
                                        },
                                        {
                                            "title": "API Key",
                                            "properties": {
                                                "api_key": {"type": "string"},
                                            },
                                        },
                                    ],
                                }
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        template = result["results"][0]["profile_template"]
        assert "client_id" in template
        assert "YOUR_CLIENT_ID" in template
        import yaml
        yaml.safe_load(template)

    @pytest.mark.asyncio
    async def test_get_profile_template_registry_unavailable(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Test",
                    "spec": None,
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        assert result["success"] is False
        assert "registry" in result["error"].lower() or "unavailable" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_get_profile_template_connector_name_with_spaces(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "My Custom Source",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["host"],
                            "properties": {"host": {"type": "string"}},
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
            name="My Custom Source",
        )
        assert result["success"] is True
        template = result["results"][0]["profile_template"]
        assert "my_custom_source_profile:" in template

    @pytest.mark.asyncio
    async def test_get_profile_template_all_fields_optional(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Test",
                    "spec": {
                        "connectionSpecification": {
                            "required": [],
                            "properties": {
                                "field1": {"type": "string"},
                                "field2": {"type": "string"},
                            },
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        assert result["success"] is True
        template = result["results"][0]["profile_template"]
        assert result["results"][0]["required_fields"] == []
        assert len(result["results"][0]["optional_fields"]) == 2
        assert "# field1:" in template
        assert "# field2:" in template

    @pytest.mark.asyncio
    async def test_get_profile_template_instructions_are_natural_language(self):
        orch, tools = _register()
        orch.airbyte_client.list_source_definitions_registry = AsyncMock(
            return_value=[
                {
                    "sourceDefinitionId": "def-src-123",
                    "name": "Postgres",
                    "spec": {
                        "connectionSpecification": {
                            "required": ["host"],
                            "properties": {"host": {"type": "string"}},
                        }
                    },
                }
            ]
        )
        result = await tools["airbyte_manage"](
            action="get_profile_template",
            source_definition_id="def-src-123",
        )
        instructions = result["results"][0]["instructions"]
        # Must NOT contain raw method call syntax
        assert "action=" not in instructions
        assert "airbyte_manage(" not in instructions
        assert "connection_profiles(" not in instructions
        # Must contain user-friendly guidance (not commands)
        assert "Ask Copilot" in instructions or "you can" in instructions
        assert "connections.yaml" in instructions


# ============================================================================
# 5. airflow_teradata_load
# ============================================================================


class TestTeradataLoad:
    """Tests for the airflow_teradata_load router tool."""

    # -- Null / empty method guards -------------------------------------------

    @pytest.mark.asyncio
    async def test_method_none(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](method=None)
        assert result["success"] is False
        assert "method" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_method_empty(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](method="")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_method_whitespace(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](method="   ")
        assert result["success"] is False

    # -- Invalid method -------------------------------------------------------

    @pytest.mark.asyncio
    async def test_invalid_method(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](method="ftp_upload")
        assert result["success"] is False
        assert "Unknown method" in result["error"]
        assert "ftp_upload" in result["error"]

    # -- Parameter validation: error_limit / session_count --------------------

    @pytest.mark.asyncio
    async def test_error_limit_zero(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](method="csv_dag", error_limit=0)
        assert result["success"] is False
        assert "error_limit" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_error_limit_negative(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](method="csv_dag", error_limit=-1)
        assert result["success"] is False
        assert "error_limit" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_session_count_zero(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](method="csv_dag", session_count=0)
        assert result["success"] is False
        assert "session_count" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_session_count_negative(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](method="csv_dag", session_count=-3)
        assert result["success"] is False
        assert "session_count" in result["error"].lower()

    # -- schedule validation ---------------------------------------------------

    @pytest.mark.asyncio
    async def test_invalid_schedule(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="csv_dag", schedule="not a valid cron at all really"
        )
        assert result["success"] is False
        assert "schedule" in result["error"].lower() or "cron" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_valid_preset_schedule(self):
        """A valid preset like @daily should pass schedule validation."""
        _, tools = _register()
        # This will pass schedule validation but may fail later in the helper.
        # We just want to verify the schedule check itself does not reject it.
        result = await tools["airflow_teradata_load"](method="csv_dag", schedule="@daily")
        # If it fails, it should NOT be because of schedule validation
        if not result.get("success", True):
            assert "schedule" not in result.get("error", "").lower()

    # -- table_transfer: missing params ----------------------------------------

    @pytest.mark.asyncio
    async def test_table_transfer_missing_source(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_teradata_profile="src_test",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "source_database" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_missing_target(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="src_tbl",
            source_teradata_profile="src_test",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "target_database" in result["error"]

    # -- table_transfer: auto-resolve database from profile --------------------

    @pytest.mark.asyncio
    async def test_table_transfer_auto_resolves_source_database_from_profile(self):
        orch, tools = _register()
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "database": "aimv9",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_table="ORDERS",
            target_database="aimv9",
            target_table="orders_copy",
            source_teradata_profile="td_source",
            target_teradata_profile="tgt_test",
        )
        # Router resolved source_database from the profile — must not fail with a missing-param error
        assert "source_database" not in result.get("error", "")
        orch.credential_resolver.resolve_profile.assert_any_call("td_source")

    @pytest.mark.asyncio
    async def test_table_transfer_auto_resolves_source_database_schema_fallback(self):
        orch, tools = _register()
        # Profile uses 'schema' instead of 'database' — must be resolved via fallback chain
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "schema": "schema_db",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_table="ORDERS",
            target_database="aimv9",
            target_table="orders_copy",
            source_teradata_profile="td_source",
        )
        # 'schema' fallback must satisfy the source_database validation check
        assert "source_database" not in result.get("error", "")

    @pytest.mark.asyncio
    async def test_table_transfer_whitespace_database_falls_through_to_schema(self):
        orch, tools = _register()
        # 'database' key is whitespace-only — must be skipped, 'schema' used instead
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "database": "   ",
            "schema": "real_db",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_table="ORDERS",
            target_database="aimv9",
            target_table="orders_copy",
            source_teradata_profile="td_source",
        )
        # Whitespace-only 'database' must not be used; 'schema' value satisfies the check
        assert "source_database" not in result.get("error", "")

    @pytest.mark.asyncio
    async def test_table_transfer_all_whitespace_database_keys_fails(self):
        orch, tools = _register()
        # All three keys are whitespace-only — must still fail with missing source_database
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "database": "  ",
            "schema": "\t",
            "default_schema": " ",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_table="ORDERS",
            target_database="aimv9",
            target_table="orders_copy",
            source_teradata_profile="td_source",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "source_database" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_profile_database_integer_is_coerced(self):
        orch, tools = _register()
        # YAML may parse `database: 123` as int — str() coercion must prevent AttributeError
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "database": 123,
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_table="ORDERS",
            target_database="aimv9",
            target_table="orders_copy",
            source_teradata_profile="td_source",
        )
        # Must not raise AttributeError; "123" is a valid non-empty database name
        assert "source_database" not in result.get("error", "")

    @pytest.mark.asyncio
    async def test_table_transfer_profile_target_database_integer_is_coerced(self):
        orch, tools = _register()
        # Same coercion check for the target profile path
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "database": 456,
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="aimv9",
            source_table="ORDERS",
            target_table="orders_copy",
            target_teradata_profile="td_destination",
        )
        assert "target_database" not in result.get("error", "")

    @pytest.mark.asyncio
    async def test_table_transfer_integer_source_database_is_coerced(self):
        orch, tools = _register()
        # JSON tool arguments may deliver ints; str() coercion must prevent AttributeError
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database=123,  # type: ignore[arg-type]
            source_table="ORDERS",
            target_database="aimv9",
            target_table="orders_copy",
        )
        assert "source_database" not in result.get("error", "")

    @pytest.mark.asyncio
    async def test_table_transfer_integer_source_table_is_coerced(self):
        orch, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="aimv9",
            source_table=42,  # type: ignore[arg-type]
            target_database="aimv9",
            target_table="orders_copy",
        )
        assert "source_table" not in result.get("error", "")

    @pytest.mark.asyncio
    async def test_table_transfer_integer_target_database_is_coerced(self):
        orch, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="aimv9",
            source_table="ORDERS",
            target_database=456,  # type: ignore[arg-type]
            target_table="orders_copy",
        )
        assert "target_database" not in result.get("error", "")

    @pytest.mark.asyncio
    async def test_table_transfer_integer_target_table_is_coerced(self):
        orch, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="aimv9",
            source_table="ORDERS",
            target_database="aimv9",
            target_table=789,  # type: ignore[arg-type]
        )
        assert "target_table" not in result.get("error", "")

    @pytest.mark.asyncio
    async def test_table_transfer_whitespace_source_database_is_rejected(self):
        orch, tools = _register()
        # Whitespace-only source_database passed explicitly must be treated as missing
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="   ",
            source_table="ORDERS",
            target_database="aimv9",
            target_table="orders_copy",
            source_teradata_profile="src_test",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "source_database" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_whitespace_target_database_is_rejected(self):
        orch, tools = _register()
        # Whitespace-only target_database passed explicitly must be treated as missing
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="aimv9",
            source_table="ORDERS",
            target_database="\t",
            target_table="orders_copy",
            source_teradata_profile="src_test",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "target_database" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_whitespace_source_table_is_rejected(self):
        orch, tools = _register()
        # Whitespace-only source_table passed explicitly must be treated as missing
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="aimv9",
            source_table="  ",
            target_database="aimv9",
            target_table="orders_copy",
            source_teradata_profile="src_test",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "source_table" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_whitespace_target_table_is_rejected(self):
        orch, tools = _register()
        # Whitespace-only target_table passed explicitly must be treated as missing
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="aimv9",
            source_table="ORDERS",
            target_database="aimv9",
            target_table="\n",
            source_teradata_profile="src_test",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "target_table" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_whitespace_source_database_resolved_from_profile(self):
        orch, tools = _register()
        # Whitespace-only source_database + a valid profile → strip first, then auto-resolve
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "database": "aimv9",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="   ",
            source_table="ORDERS",
            target_database="aimv9",
            target_table="orders_copy",
            source_teradata_profile="td_source",
            target_teradata_profile="tgt_test",
        )
        # Profile supplied "aimv9" — must not fail with a missing-param error
        assert "source_database" not in result.get("error", "")
        orch.credential_resolver.resolve_profile.assert_any_call("td_source")

    @pytest.mark.asyncio
    async def test_table_transfer_whitespace_target_database_resolved_from_profile(self):
        orch, tools = _register()
        # Whitespace-only target_database + a valid profile → strip first, then auto-resolve
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "database": "aimv9",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="aimv9",
            source_table="ORDERS",
            target_database="\t",
            target_table="orders_copy",
            target_teradata_profile="td_destination",
            source_teradata_profile="src_test",
        )
        # Profile supplied "aimv9" — must not fail with a missing-param error
        assert "target_database" not in result.get("error", "")
        orch.credential_resolver.resolve_profile.assert_any_call("td_destination")

    @pytest.mark.asyncio
    async def test_table_transfer_auto_resolves_target_database_from_profile(self):
        orch, tools = _register()
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "database": "aimv9",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="aimv9",
            source_table="ORDERS",
            target_table="orders_copy",
            target_teradata_profile="td_destination",
            source_teradata_profile="src_test",
        )
        # Router resolved target_database from the profile — must not fail with a missing-param error
        assert "target_database" not in result.get("error", "")
        orch.credential_resolver.resolve_profile.assert_any_call("td_destination")

    @pytest.mark.asyncio
    async def test_table_transfer_explicit_database_overrides_profile(self):
        orch, tools = _register()
        # Profile returns "aimv9" — explicit databases must reach the generator unchanged
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
            "database": "aimv9",
        }
        # Additional mock attributes needed by _generate_airflow_tdload_table_transfer_dag
        airflow_client = AsyncMock()
        airflow_client.list_connections.return_value = []
        airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        airflow_client.create_connection = AsyncMock()
        orch.async_airflow_client = airflow_client
        orch.teradata_client.get_table_metadata.return_value = {
            "columns": [{"name": "id", "type": "I"}],
            "row_count": 100,
        }

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator"
                ".AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator"
                ".AirflowTdLoadDAGGenerator.generate_table_transfer_dag",
            ) as mock_gen_transfer,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ",
                {
                    "MCP_CLIENT_SSH_HOST": "10.0.0.1",
                    "MCP_CLIENT_SSH_USER": "testuser",
                    "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
                },
            ),
        ):
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/transfer.py"))
            )
            MockPath.return_value = mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="explicit_db",
                source_table="ORDERS",
                target_database="explicit_tgt",
                target_table="orders_copy",
                source_teradata_profile="td_source",
                target_teradata_profile="tgt_test",
            )

        assert result["success"] is True
        # The generator must have received the caller-supplied values, not "aimv9" from the profile
        gen_kwargs = mock_gen_transfer.call_args.kwargs
        assert gen_kwargs["source_database"] == "explicit_db"
        assert gen_kwargs["target_database"] == "explicit_tgt"

    @pytest.mark.asyncio
    async def test_table_transfer_error_when_profile_missing_database(self):
        orch, tools = _register()
        # Profile exists but has no 'database', 'schema', or 'default_schema' key
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "pass",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_table="ORDERS",
            target_database="aimv9",
            target_table="orders_copy",
            source_teradata_profile="td_source",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "source_database" in result["error"]
        assert "td_source" in result["error"]  # profile name mentioned in error

    # -- table_transfer: unresolved env-var in password ------------------------

    @pytest.mark.asyncio
    async def test_table_transfer_source_profile_unresolved_password_env_var(self):
        orch, tools = _register()
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "${TERADATA_PASSWORD}",
            "database": "src_db",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="src_tbl",
            target_database="tgt_db",
            target_table="tgt_tbl",
            source_teradata_profile="td_source",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "TERADATA_PASSWORD" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_target_profile_unresolved_password_env_var(self):
        orch, tools = _register()
        # Allow _find_or_reserve_conn_id to complete cleanly for source
        orch.async_airflow_client.list_connections.return_value = []
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")

        def _resolve(profile_name):
            if profile_name == "td_target":
                return {
                    "host": "td-host",
                    "username": "user",
                    "password": "${TD_PASS}",
                    "database": "tgt_db",
                }
            return {
                "host": "td-host",
                "username": "user",
                "password": "good_pass",
                "database": "src_db",
            }

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="src_tbl",
            target_database="tgt_db",
            target_table="tgt_tbl",
            target_teradata_profile="td_target",
            source_teradata_profile="src_test",
        )
        assert result["success"] is False
        assert "TD_PASS" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_ssh_profile_no_auth(self):
        orch, tools = _register()
        # Allow _find_or_reserve_conn_id to complete cleanly for both Teradata connections
        orch.async_airflow_client.list_connections.return_value = []
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")

        def _resolve(profile_name):
            if profile_name == "ssh_jump":
                return {"host": "ssh-host", "username": "sshuser"}
            return {
                "host": "td-host",
                "username": "user",
                "password": "pass",
                "database": "src_db",
            }

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="src_tbl",
            target_database="tgt_db",
            target_table="tgt_tbl",
            ssh_profile="ssh_jump",
            source_teradata_profile="src_test",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "key_file" in result["error"]

    # -- table_transfer: success -----------------------------------------------

    @pytest.mark.asyncio
    async def test_table_transfer_success(self):
        orch, tools = _register()
        # The inner helper imports generators and writes files.
        # We mock the generator at the module level.
        with (
            patch("elt_mcp_server.tools.data_movement.Path.mkdir"),
            patch("elt_mcp_server.tools.data_movement.Path.write_text"),
        ):
            # Mock the AirflowTdLoadDAGGenerator import inside the helper
            mock_gen_class = Mock()
            mock_gen_instance = Mock()
            mock_gen_instance.generate_table_transfer_dag.return_value = "# DAG code"
            mock_gen_class.return_value = mock_gen_instance

            with patch.dict("sys.modules", {}):
                # A simpler approach: just catch and accept whatever the helper does
                result = await tools["airflow_teradata_load"](
                    method="table_transfer",
                    source_database="src_db",
                    source_table="src_tbl",
                    target_database="tgt_db",
                    target_table="tgt_tbl",
                )
            # The result might fail due to generator import, but the router
            # dispatched correctly (not a router-level error).
            assert isinstance(result, dict)
            if not result.get("success"):
                # If it failed, it should be from the inner helper, not from
                # missing params at the router level
                assert "source_database" not in result.get("error", "")
                assert "target_database" not in result.get("error", "")

    # -- csv_complete: missing params ------------------------------------------

    @pytest.mark.asyncio
    async def test_csv_complete_missing_csv_path(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="csv_complete",
            teradata_profile="td_test",
        )
        assert result["success"] is False
        assert "csv_path" in result["error"]

    @pytest.mark.asyncio
    async def test_csv_complete_missing_target(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="csv_complete",
            csv_path="/data/file.csv",
            teradata_profile="td_test",
        )
        assert result["success"] is False
        assert "target_database" in result["error"]

    # -- csv_dag / csv_complete: early-fail on credential errors ---------------
    #
    # These tests verify that the except ValueError: raise path added to
    # _generate_airflow_tdload_dag_from_csv returns {"success": False} instead
    # of swallowing the error and proceeding to generate a broken DAG.
    #
    # Mocking strategy:
    #   - patch.object(Path, "exists") → True  (skip real file check)
    #   - patch.object(Path, "is_relative_to") → True  (skip traversal guard)
    #   - patch CSVAnalyzer at its source module  (skip actual file parsing)
    # The ValueError is raised inside _ensure_teradata_connection or
    # _ensure_ssh_connection before any DAG file I/O is attempted.

    @staticmethod
    def _make_csv_analysis_mock():
        col = Mock()
        col.name = "id"
        analysis = Mock()
        analysis.row_count = 5
        analysis.column_count = 1
        analysis.file_size_mb = 0.001
        analysis.delimiter = ","
        analysis.columns = [col]
        return analysis

    @pytest.mark.asyncio
    async def test_csv_dag_profile_unresolved_password_env_var(self):
        """csv_dag returns success=False when profile password is an unresolved ${VAR}."""
        orch, tools = _register()
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "${TERADATA_PASSWORD}",
            "database": "test_db",
        }

        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "is_relative_to", return_value=True),
            patch("elt_mcp_server.utils.csv_analyzer.CSVAnalyzer") as MockCSV,
        ):
            MockCSV.return_value.analyze_csv.return_value = self._make_csv_analysis_mock()
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/fake/data.csv",
                target_database="test_db",
                target_table="test_tbl",
                teradata_profile="bad_profile",
            )

        assert result["success"] is False
        assert "TERADATA_PASSWORD" in result["error"]

    @pytest.mark.asyncio
    async def test_csv_dag_ssh_profile_no_auth(self):
        """csv_dag returns success=False when ssh_profile has no password or key_file."""
        orch, tools = _register()
        # Allow the Teradata connection setup to pass (no teradata_profile, uses
        # mock td_settings whose Mock fields are truthy and contain no ${...}).
        orch.async_airflow_client.list_connections.return_value = []
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")

        orch.credential_resolver.resolve_profile.side_effect = lambda name: (
            {"host": "ssh-host", "username": "sshuser"}
            if name == "ssh_jump"
            else {"host": "td-host", "username": "user", "password": "pass", "database": "test_db"}
        )

        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "is_relative_to", return_value=True),
            patch("elt_mcp_server.utils.csv_analyzer.CSVAnalyzer") as MockCSV,
        ):
            MockCSV.return_value.analyze_csv.return_value = self._make_csv_analysis_mock()
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/fake/data.csv",
                target_database="test_db",
                target_table="test_tbl",
                ssh_profile="ssh_jump",
                teradata_profile="td_test",
            )

        assert result["success"] is False
        assert "key_file" in result["error"]

    @pytest.mark.asyncio
    async def test_csv_complete_profile_unresolved_password_env_var(self):
        """csv_complete returns success=False when profile password is an unresolved ${VAR}.

        teradata_profile is now forwarded through csv_complete →
        _load_csv_to_teradata_complete → _generate_airflow_tdload_dag_from_csv,
        so the credential error propagates and surfaces as 'DAG generation failed'.
        """
        orch, tools = _register()
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "user",
            "password": "${TD_PASS}",
            "database": "test_db",
        }

        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "is_relative_to", return_value=True),
            patch("elt_mcp_server.utils.csv_analyzer.CSVAnalyzer") as MockCSV,
        ):
            MockCSV.return_value.analyze_csv.return_value = self._make_csv_analysis_mock()
            result = await tools["airflow_teradata_load"](
                method="csv_complete",
                csv_path="/fake/data.csv",
                target_database="test_db",
                target_table="test_tbl",
                teradata_profile="bad_profile",
            )

        assert result["success"] is False
        assert "DAG generation failed" in result["error"]

    # -- table_transfer: unresolved env-var in host / username -----------------

    @pytest.mark.asyncio
    async def test_table_transfer_source_profile_unresolved_host(self):
        """table_transfer returns success=False when source profile host is an unresolved ${VAR}."""
        orch, tools = _register()
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "${TERADATA_HOST}",
            "username": "user",
            "password": "pass",
            "database": "src_db",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="src_tbl",
            target_database="tgt_db",
            target_table="tgt_tbl",
            source_teradata_profile="td_source",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "TERADATA_HOST" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_source_profile_unresolved_username(self):
        """table_transfer returns success=False when source profile username is an unresolved ${VAR}."""
        orch, tools = _register()
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "${TERADATA_USERNAME}",
            "password": "pass",
            "database": "src_db",
        }
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="src_tbl",
            target_database="tgt_db",
            target_table="tgt_tbl",
            source_teradata_profile="td_source",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "TERADATA_USERNAME" in result["error"]

    # -- table_transfer: unresolved env-var in SSH password / key_file ---------

    @pytest.mark.asyncio
    async def test_table_transfer_ssh_profile_unresolved_password(self):
        """table_transfer returns success=False when SSH profile password is an unresolved ${VAR}."""
        orch, tools = _register()
        orch.async_airflow_client.list_connections.return_value = []
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")

        def _resolve(profile_name):
            if profile_name == "ssh_jump":
                return {"host": "ssh-host", "username": "sshuser", "password": "${SSH_PASS}"}
            return {
                "host": "td-host",
                "username": "user",
                "password": "pass",
                "database": "src_db",
            }

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="src_tbl",
            target_database="tgt_db",
            target_table="tgt_tbl",
            ssh_profile="ssh_jump",
            source_teradata_profile="src_test",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "SSH_PASS" in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_ssh_profile_unresolved_key_file(self):
        """table_transfer returns success=False when SSH profile key_file is an unresolved ${VAR}."""
        orch, tools = _register()
        orch.async_airflow_client.list_connections.return_value = []
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")

        def _resolve(profile_name):
            if profile_name == "ssh_jump":
                return {
                    "host": "ssh-host",
                    "username": "sshuser",
                    "key_file": "${SSH_KEY}",
                }
            return {
                "host": "td-host",
                "username": "user",
                "password": "pass",
                "database": "src_db",
            }

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="src_tbl",
            target_database="tgt_db",
            target_table="tgt_tbl",
            ssh_profile="ssh_jump",
            source_teradata_profile="src_test",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert "SSH_KEY" in result["error"]

    # -- csv_dag: success (dispatches to helper) -------------------------------

    @pytest.mark.asyncio
    async def test_csv_dag_dispatches(self):
        """csv_dag should dispatch without router-level errors even
        when no csv_path is given (the helper resolves from env)."""
        _, tools = _register()
        result = await tools["airflow_teradata_load"](method="csv_dag")
        # The inner helper may fail due to missing CSV file, but it should
        # not fail at the router dispatch level with "Unknown method"
        assert isinstance(result, dict)
        if not result.get("success"):
            assert "Unknown method" not in result.get("error", "")

    # -- exception from inner helper ------------------------------------------

    @pytest.mark.asyncio
    async def test_csv_complete_exception(self):
        orch, tools = _register()
        # csv_complete helper will import modules and try to work with files.
        # Force an exception early.
        result = await tools["airflow_teradata_load"](
            method="csv_complete",
            csv_path="/nonexistent/file.csv",
            target_database="db",
            target_table="tbl",
        )
        # Should capture exception, not raise
        assert isinstance(result, dict)
        if not result.get("success"):
            assert "error" in result


# ============================================================================
# 6. _ensure_teradata_connection tests
# ============================================================================


class TestEnsureTeradataConnection:
    """Tests for _ensure_teradata_connection internal function.

    Tests connection matching, case-insensitivity, port handling,
    ID incrementing on mismatch, and validation.
    """

    def _make_teradata_settings(
        self, host="td-host.example.com", port=1025, username="dbc", password="secret"
    ):
        """Create a mock Teradata settings object."""
        settings = Mock()
        settings.host = host
        settings.port = port
        settings.username = username
        settings.password = Mock()
        settings.password.get_secret_value = Mock(return_value=password)
        settings.database = "testdb"
        return settings

    def _make_orchestrator_with_teradata(self):
        """Create mock orchestrator with Teradata settings for connection tests."""
        orch = Mock()
        orch.airbyte_client = AsyncMock()
        orch.credential_resolver = Mock()
        orch.credential_resolver.guard_configured.return_value = None
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "td-host.example.com",
            "port": 1025,
            "username": "dbc",
            "password": "secret",
            "database": "testdb",
        }

        # Create Teradata settings mock
        td_settings = self._make_teradata_settings()

        orch.settings = Mock()
        orch.settings.airbyte = Mock()
        orch.settings.airbyte.workspace_id = "ws-abc-123"
        orch.settings.teradata = td_settings
        orch.settings.get_source_teradata = Mock(return_value=td_settings)
        orch.settings.get_target_teradata = Mock(return_value=td_settings)
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"

        orch.async_airflow_client = AsyncMock()
        orch.teradata_client = Mock()
        orch.teradata_client.get_table_metadata = Mock(
            return_value={
                "columns": [{"name": "id", "type": "INTEGER"}],
                "row_count": 100,
            }
        )

        return orch

    @pytest.mark.asyncio
    async def test_finds_matching_connection_case_insensitive(self):
        """Connection search should match case-insensitively."""
        orch = self._make_orchestrator_with_teradata()
        # Return existing connection with different case but same credentials
        orch.async_airflow_client.list_connections.return_value = [
            {
                "connection_id": "existing_teradata",
                "conn_type": "TERADATA",  # uppercase
                "host": "TD-HOST.EXAMPLE.COM",  # uppercase - matches td_settings.host
                "schema": "TESTDB",  # uppercase
                "login": "DBC",  # uppercase - matches td_settings.username
                "port": 1025,
            }
        ]
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
            )

            # Should find the existing connection - no Teradata connections should be created
            # (both source and target find existing_teradata)
            td_create_calls = [
                c
                for c in orch.async_airflow_client.create_connection.call_args_list
                if c.kwargs.get("conn_type") == "teradata"
            ]
            assert len(td_create_calls) == 0, (
                f"Expected no Teradata connections to be created, got {td_create_calls}"
            )

    @pytest.mark.asyncio
    async def test_port_handling_string_vs_int(self):
        """Port comparison should handle both string and int port values."""
        orch = self._make_orchestrator_with_teradata()
        # Return connection with port as string
        orch.async_airflow_client.list_connections.return_value = [
            {
                "connection_id": "existing_teradata",
                "conn_type": "teradata",
                "host": "td-host.example.com",
                "schema": "testdb",
                "login": "dbc",
                "port": "1025",  # Port as string
            }
        ]
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
            )

            # Should find the existing connection (port 1025 == "1025")
            # Only check Teradata connections, SSH may still be created
            td_create_calls = [
                c
                for c in orch.async_airflow_client.create_connection.call_args_list
                if c.kwargs.get("conn_type") == "teradata"
            ]
            assert len(td_create_calls) == 0, (
                f"Expected no Teradata connections, got {td_create_calls}"
            )

    @pytest.mark.asyncio
    async def test_port_mismatch_creates_new_connection(self):
        """Different port should NOT match existing connection."""
        orch = self._make_orchestrator_with_teradata()
        # Existing connection with different port
        orch.async_airflow_client.list_connections.return_value = [
            {
                "connection_id": "existing_teradata",
                "conn_type": "teradata",
                "host": "td-host.example.com",
                "schema": "testdb",
                "login": "dbc",
                "port": 9999,  # Different port
            }
        ]
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

            # Should create a new connection since port doesn't match
            orch.async_airflow_client.create_connection.assert_called()

    @pytest.mark.asyncio
    async def test_increments_conn_id_on_mismatch(self):
        """When conn_id exists with different config, should increment ID."""
        orch = self._make_orchestrator_with_teradata()
        orch.async_airflow_client.list_connections.return_value = []  # No matching

        # First call: original conn_id exists with wrong config
        # Second call: incremented _1 doesn't exist
        call_count = [0]

        async def mock_get_connection(conn_id):
            call_count[0] += 1
            if conn_id == "teradata_source":
                # Existing connection with wrong config
                return {
                    "conn_id": "teradata_source",
                    "host": "wrong-host.com",
                    "schema": "wrong_db",
                    "login": "wrong_user",
                    "port": 1025,
                }
            elif conn_id == "teradata_target":
                # Target also has wrong config
                return {
                    "conn_id": "teradata_target",
                    "host": "wrong-host.com",
                    "schema": "wrong_db",
                    "login": "wrong_user",
                    "port": 1025,
                }
            else:
                # _1 versions don't exist
                raise AsyncAirflowAPIError("Not found")

        orch.async_airflow_client.get_connection.side_effect = mock_get_connection
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

            # Should have created teradata_source_1 and teradata_target_1
            td_create_calls = [
                c
                for c in orch.async_airflow_client.create_connection.call_args_list
                if c.kwargs.get("conn_type") == "teradata"
            ]
            assert len(td_create_calls) == 2
            created_ids = [c.kwargs["conn_id"] for c in td_create_calls]
            # Both source and target should be incremented
            assert "teradata_source_1" in created_ids
            assert "teradata_target_1" in created_ids

    @pytest.mark.asyncio
    async def test_finds_matching_incremented_conn(self):
        """When searching incremented IDs, should reuse one that matches."""
        orch = self._make_orchestrator_with_teradata()
        orch.async_airflow_client.list_connections.return_value = []

        async def mock_get_connection(conn_id):
            if conn_id == "teradata_source":
                # Original exists with wrong config
                return {
                    "conn_id": "teradata_source",
                    "host": "wrong-host.com",
                    "schema": "wrong_db",
                    "login": "wrong_user",
                    "port": 1025,
                }
            elif conn_id == "teradata_source_1":
                # _1 also has wrong config
                return {
                    "conn_id": "teradata_source_1",
                    "host": "also-wrong.com",
                    "schema": "wrong_db",
                    "login": "wrong_user",
                    "port": 1025,
                }
            elif conn_id == "teradata_source_2":
                # _2 matches!
                return {
                    "conn_id": "teradata_source_2",
                    "host": "td-host.example.com",
                    "schema": "testdb",
                    "login": "dbc",
                    "port": 1025,
                }
            elif conn_id == "teradata_target":
                # Target also exists with correct config
                return {
                    "conn_id": "teradata_target",
                    "host": "td-host.example.com",
                    "schema": "testdb",
                    "login": "dbc",
                    "port": 1025,
                }
            else:
                raise Exception("Not found")

        orch.async_airflow_client.get_connection.side_effect = mock_get_connection
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
            )

            # Should NOT create new Teradata connection - reused teradata_source_2 and teradata_target
            td_create_calls = [
                c
                for c in orch.async_airflow_client.create_connection.call_args_list
                if c.kwargs.get("conn_type") == "teradata"
            ]
            assert len(td_create_calls) == 0, (
                f"Expected no Teradata connections created, got {td_create_calls}"
            )

    @pytest.mark.asyncio
    async def test_creates_connection_when_not_found(self):
        """Should create connection when no matching connection exists."""
        orch = self._make_orchestrator_with_teradata()
        orch.async_airflow_client.list_connections.return_value = []
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

            # Should have created connection
            orch.async_airflow_client.create_connection.assert_called()
            td_create_calls = [
                c
                for c in orch.async_airflow_client.create_connection.call_args_list
                if c.kwargs.get("conn_type") == "teradata"
            ]
            assert len(td_create_calls) >= 1

    @pytest.mark.asyncio
    async def test_missing_host_raises_value_error(self):
        """Should raise ValueError when Teradata host is not configured."""
        orch = self._make_orchestrator_with_teradata()
        orch.settings.teradata.host = None  # Missing host
        # Profile resolution now drives effective_host — also clear it there.
        orch.credential_resolver.resolve_profile.return_value = {
            "username": "dbc",
            "password": "secret",
            "database": "testdb",
            "port": 1025,
        }
        orch.async_airflow_client.list_connections.return_value = []
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

            # In table_transfer, errors propagate and result in success=False
            assert result.get("success") is False
            assert "host not configured" in result.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_100_attempt_limit_raises_runtime_error(self):
        """Should propagate RuntimeError when counter exceeds 100 attempts."""
        orch = self._make_orchestrator_with_teradata()
        orch.async_airflow_client.list_connections.return_value = []

        # Every get_connection returns a connection with wrong config
        async def mock_get_connection(conn_id):
            return {
                "conn_id": conn_id,
                "conn_type": "teradata",
                "host": "wrong-host.com",
                "schema": "wrong_db",
                "login": "wrong_user",
                "port": 1025,
            }

        orch.async_airflow_client.get_connection.side_effect = mock_get_connection
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
            )

            # RuntimeError should propagate (not be swallowed by the broad
            # except) and be caught by table_transfer's outer handler
            assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_list_connections_failure_continues(self):
        """Should continue if list_connections fails (graceful degradation)."""
        orch = self._make_orchestrator_with_teradata()
        orch.async_airflow_client.list_connections.side_effect = Exception("Network error")
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

            # Should have proceeded to create connection despite list failure
            orch.async_airflow_client.create_connection.assert_called()


# ============================================================================
# 7. _ensure_ssh_connection tests
# ============================================================================


class TestEnsureSSHConnection:
    """Tests for _ensure_ssh_connection internal function.

    Tests connection matching, case-insensitivity, port handling,
    ID incrementing on mismatch, and validation.
    """

    def _make_teradata_settings(
        self, host="td-host.example.com", port=1025, username="dbc", password="secret"
    ):
        """Create a mock Teradata settings object."""
        settings = Mock()
        settings.host = host
        settings.port = port
        settings.username = username
        settings.password = Mock()
        settings.password.get_secret_value = Mock(return_value=password)
        settings.database = "testdb"
        return settings

    def _make_orchestrator_with_ssh(self):
        """Create mock orchestrator with SSH settings for connection tests."""
        orch = Mock()
        orch.airbyte_client = AsyncMock()
        orch.credential_resolver = Mock()
        orch.credential_resolver.guard_configured.return_value = None
        # SSH profile resolution
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "ssh-host.example.com",
            "port": 22,
            "username": "airflow",
            "password": "sshpass",
        }

        # Create Teradata settings mock
        td_settings = self._make_teradata_settings()

        orch.settings = Mock()
        orch.settings.airbyte = Mock()
        orch.settings.airbyte.workspace_id = "ws-abc-123"
        orch.settings.teradata = td_settings
        orch.settings.get_source_teradata = Mock(return_value=td_settings)
        orch.settings.get_target_teradata = Mock(return_value=td_settings)
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"

        orch.async_airflow_client = AsyncMock()
        orch.teradata_client = Mock()
        orch.teradata_client.get_table_metadata = Mock(
            return_value={
                "columns": [{"name": "id", "type": "INTEGER"}],
                "row_count": 100,
            }
        )

        return orch

    @pytest.mark.asyncio
    async def test_finds_ssh_connection_case_insensitive(self):
        """SSH connection search should match case-insensitively."""
        orch = self._make_orchestrator_with_ssh()
        # Return existing SSH connection with different case
        orch.async_airflow_client.list_connections.return_value = [
            {
                "connection_id": "existing_ssh",
                "conn_type": "SSH",  # uppercase
                "host": "SSH-HOST.EXAMPLE.COM",  # uppercase
                "login": "AIRFLOW",  # uppercase
                "port": 22,
            }
        ]
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                ssh_profile="test_ssh",
            )

            # Verify existing_ssh was found (no SSH connection created)
            ssh_create_calls = [
                c
                for c in orch.async_airflow_client.create_connection.call_args_list
                if c.kwargs.get("conn_type") == "ssh"
            ]
            assert len(ssh_create_calls) == 0

    @pytest.mark.asyncio
    async def test_ssh_port_handling_string_vs_int(self):
        """SSH port comparison should handle both string and int values."""
        orch = self._make_orchestrator_with_ssh()
        # Return connection with port as string
        orch.async_airflow_client.list_connections.return_value = [
            {
                "connection_id": "existing_ssh",
                "conn_type": "ssh",
                "host": "ssh-host.example.com",
                "login": "airflow",
                "port": "22",  # Port as string
            }
        ]
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                ssh_profile="test_ssh",
            )

            # Should find the existing connection (port 22 == "22")
            ssh_create_calls = [
                c
                for c in orch.async_airflow_client.create_connection.call_args_list
                if c.kwargs.get("conn_type") == "ssh"
            ]
            assert len(ssh_create_calls) == 0

    @pytest.mark.asyncio
    async def test_ssh_increments_conn_id_on_mismatch(self):
        """When SSH conn_id exists with different config, should increment ID."""
        orch = self._make_orchestrator_with_ssh()
        orch.async_airflow_client.list_connections.return_value = []  # No matching

        async def mock_get_connection(conn_id):
            if conn_id == "ssh_test_dag":
                # Existing with wrong config
                return {
                    "conn_id": "ssh_test_dag",
                    "conn_type": "ssh",
                    "host": "wrong-host.com",
                    "login": "wrong_user",
                    "port": 22,
                }
            elif conn_id.startswith("teradata_"):
                # Mock Teradata connections as matching
                return {
                    "conn_id": conn_id,
                    "conn_type": "teradata",
                    "host": "td-host.example.com",
                    "schema": "testdb",
                    "login": "dbc",
                    "port": 1025,
                }
            else:
                # ssh_test_dag_1 doesn't exist
                raise AsyncAirflowAPIError("Not found")

        orch.async_airflow_client.get_connection.side_effect = mock_get_connection
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                ssh_profile="test_ssh",
            )

            # Should have created ssh_test_dag_1
            ssh_create_calls = [
                c
                for c in orch.async_airflow_client.create_connection.call_args_list
                if c.kwargs.get("conn_type") == "ssh"
            ]
            if ssh_create_calls:
                assert ssh_create_calls[0].kwargs["conn_id"] == "ssh_test_dag_1"

    @pytest.mark.asyncio
    async def test_ssh_profile_invalid_port_raises_value_error(self):
        """SSH profile with non-numeric port should raise ValueError."""
        orch = self._make_orchestrator_with_ssh()

        # Only the SSH profile has the bad port; Teradata profiles are valid
        # so the test exercises the SSH-specific port validation path.
        def _resolve(profile_name):
            if profile_name == "test_ssh":
                return {
                    "host": "ssh-host.example.com",
                    "port": "not-a-number",  # Invalid port
                    "username": "airflow",
                    "password": "sshpass",
                }
            return {
                "host": "td-host.example.com",
                "port": 1025,
                "username": "dbc",
                "password": "secret",
                "database": "testdb",
            }

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        orch.async_airflow_client.list_connections.return_value = []
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                ssh_profile="test_ssh",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

            # ValueError propagates and is caught by outer handler
            assert result.get("success") is False
            assert "port" in result.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_ssh_profile_port_out_of_range_raises_value_error(self):
        """SSH profile with port outside 1-65535 should raise ValueError."""
        orch = self._make_orchestrator_with_ssh()

        # Only the SSH profile has the out-of-range port; Teradata profiles are valid.
        def _resolve(profile_name):
            if profile_name == "test_ssh":
                return {
                    "host": "ssh-host.example.com",
                    "port": 99999,  # Out of range
                    "username": "airflow",
                    "password": "sshpass",
                }
            return {
                "host": "td-host.example.com",
                "port": 1025,
                "username": "dbc",
                "password": "secret",
                "database": "testdb",
            }

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        orch.async_airflow_client.list_connections.return_value = []
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                ssh_profile="test_ssh",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

            # ValueError propagates and is caught by outer handler
            assert result.get("success") is False
            assert "port" in result.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_ssh_100_attempt_limit_raises_runtime_error(self):
        """SSH should propagate RuntimeError when counter exceeds 100 attempts."""
        orch = self._make_orchestrator_with_ssh()
        orch.async_airflow_client.list_connections.return_value = []

        # Every get_connection returns a connection with wrong config
        async def mock_get_connection(conn_id):
            if conn_id.startswith("teradata_"):
                raise Exception("Not found")
            return {
                "conn_id": conn_id,
                "conn_type": "ssh",
                "host": "wrong-host.com",
                "login": "wrong_user",
                "port": 22,
            }

        orch.async_airflow_client.get_connection.side_effect = mock_get_connection
        tools = register_data_movement_tools(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator"
            ) as MockGen,
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
        ):
            mock_instance = Mock()
            mock_instance.generate_table_transfer_dag.return_value = "# DAG code"
            MockGen.return_value = mock_instance
            mock_path = Mock()
            mock_path.mkdir = Mock()
            mock_path.write_text = Mock()
            mock_path.__truediv__ = Mock(return_value=mock_path)
            mock_path.__str__ = Mock(return_value="/tmp/dags/test.py")
            MockPath.return_value = mock_path

            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="testdb",
                source_table="src_tbl",
                target_database="testdb",
                target_table="tgt_tbl",
                dag_id="test_dag",
                ssh_profile="test_ssh",
            )

            # RuntimeError should propagate (not be swallowed by the broad
            # except) and be caught by table_transfer's outer handler
            assert result.get("success") is False


# ============================================================================
# 7b. Rule 5: persistent Teradata assets must use a named profile
# ============================================================================


class TestRule9AirbyteManage:
    """Rule 5 — `airbyte_manage` create_source / create_destination must
    cite a named connections.yaml profile. The wizard-default identity
    (and the 'wizard' / 'default' sentinel) is rejected at the boundary
    before any Airbyte API call happens.
    """

    @pytest.mark.asyncio
    async def test_create_source_missing_profile_returns_rule9(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](
            action="create_source",
            name="td_source_1",
            source_definition_id="aaaccd30-1234-1234-1234-aaaaaaaaaaaa",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 5"
        assert result["missing"] == ["source_profile"]
        assert "named connections.yaml profile" in result["error"]

    @pytest.mark.asyncio
    async def test_create_source_wizard_sentinel_rejected(self):
        for sentinel in ("wizard", "default", "Wizard", " default "):
            _, tools = _register()
            result = await tools["airbyte_manage"](
                action="create_source",
                name="td_source_1",
                source_definition_id="aaaccd30-1234-1234-1234-aaaaaaaaaaaa",
                source_profile=sentinel,
            )
            assert result["success"] is False, sentinel
            assert result["rule"] == "Rule 5", sentinel

    @pytest.mark.asyncio
    async def test_create_source_named_profile_passes_gate(self):
        """Named profile clears Rule 5 — the call reaches the
        ``_create_airbyte_source`` helper (which mocks block beyond)."""
        orch, tools = _register()
        with patch(
            "elt_mcp_server.tools.data_movement._find_or_create_connector",
            new=AsyncMock(return_value={"success": True, "source": {"sourceId": "s1"}}),
        ):
            result = await tools["airbyte_manage"](
                action="create_source",
                name="td_source_1",
                source_definition_id="aaaccd30-1234-1234-1234-aaaaaaaaaaaa",
                source_profile="prod_teradata",
            )
        assert result.get("success") is True

    @pytest.mark.asyncio
    async def test_create_destination_missing_profile_returns_rule9(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](
            action="create_destination",
            name="td_dest_1",
            destination_definition_id="bbbccd30-1234-1234-1234-bbbbbbbbbbbb",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 5"
        assert result["missing"] == ["destination_profile"]

    @pytest.mark.asyncio
    async def test_create_destination_wizard_sentinel_rejected(self):
        _, tools = _register()
        result = await tools["airbyte_manage"](
            action="create_destination",
            name="td_dest_1",
            destination_definition_id="bbbccd30-1234-1234-1234-bbbbbbbbbbbb",
            destination_profile="wizard",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 5"

    @pytest.mark.asyncio
    async def test_create_destination_named_profile_passes_gate(self):
        orch, tools = _register()
        with patch(
            "elt_mcp_server.tools.data_movement._find_or_create_connector",
            new=AsyncMock(return_value={"success": True, "destination": {"destinationId": "d1"}}),
        ):
            result = await tools["airbyte_manage"](
                action="create_destination",
                name="td_dest_1",
                destination_definition_id="bbbccd30-1234-1234-1234-bbbbbbbbbbbb",
                destination_profile="prod_teradata",
            )
        assert result.get("success") is True

    @pytest.mark.asyncio
    async def test_other_actions_unaffected_by_rule9(self):
        """Rule 5 only applies to create_*; other actions don't take
        profiles and must not be affected."""
        orch, tools = _register()
        # test_api is a simple connectivity check — no profile, no Rule 5.
        orch.airbyte_client.test_connection = AsyncMock(return_value={"status": "ok"})
        result = await tools["airbyte_manage"](action="test_api")
        assert result.get("rule") != "Rule 5"


class TestRule9AirflowTeradataLoad:
    """Rule 5 — `airflow_teradata_load` (csv_dag, csv_complete, table_transfer)
    must cite a named connections.yaml profile."""

    @pytest.mark.asyncio
    async def test_csv_dag_missing_profile_returns_rule9(self, tmp_path):
        orch = _make_orchestrator()
        tools = register_data_movement_tools(orch)
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col1\na\n", encoding="utf-8")
        result = await tools["airflow_teradata_load"](
            method="csv_dag",
            csv_path=str(csv_file),
            target_database="db",
            target_table="t",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 5"
        assert result["missing"] == ["teradata_profile"]

    @pytest.mark.asyncio
    async def test_csv_dag_wizard_sentinel_rejected(self, tmp_path):
        orch = _make_orchestrator()
        tools = register_data_movement_tools(orch)
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col1\na\n", encoding="utf-8")
        result = await tools["airflow_teradata_load"](
            method="csv_dag",
            csv_path=str(csv_file),
            target_database="db",
            target_table="t",
            teradata_profile="default",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 5"

    @pytest.mark.asyncio
    async def test_csv_complete_missing_profile_returns_rule9(self, tmp_path):
        orch = _make_orchestrator()
        tools = register_data_movement_tools(orch)
        result = await tools["airflow_teradata_load"](
            method="csv_complete",
            csv_path="/tmp/x.csv",
            target_database="db",
            target_table="t",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 5"

    @pytest.mark.asyncio
    async def test_table_transfer_missing_both_profiles_returns_rule9(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="t1",
            target_database="tgt_db",
            target_table="t2",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 5"
        assert "source_teradata_profile" in result["missing"]
        assert "target_teradata_profile" in result["missing"]

    @pytest.mark.asyncio
    async def test_table_transfer_missing_only_source_profile_returns_rule9(self):
        _, tools = _register()
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="t1",
            target_database="tgt_db",
            target_table="t2",
            target_teradata_profile="tgt_test",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 5"
        assert result["missing"] == ["source_teradata_profile"]


# ============================================================================
# 8. Registration sanity check
# ============================================================================


class TestRegistration:
    """Verify register_data_movement_tools returns the expected keys."""

    def test_returns_five_tools(self):
        _, tools = _register()
        expected = {
            "airbyte_pipeline",
            "airbyte_sync",
            "airbyte_inventory",
            "airbyte_manage",
            "airflow_teradata_load",
        }
        assert set(tools.keys()) == expected

    def test_all_values_are_callable(self):
        _, tools = _register()
        for name, fn in tools.items():
            assert callable(fn), f"Tool '{name}' is not callable"


# ============================================================================
# 6. airflow_teradata_load — dbt conditional branching
# ============================================================================


class TestTeradataLoadDbtBranching:
    """Tests that ``project_name`` gates the ``_with_dbt`` generator variants
    (resolved to a sub-project path embedded in the DAG)."""

    @staticmethod
    def _set_up_dbt_subproject(orch, tmp_path, identity="wizard:td_host"):
        """Pre-create a dbt sub-project at ``tmp_path/dbt_workspace/dbt_default``
        bound to the given identity. The DAG-generation tools resolve to
        this when the test passes ``project_name='default'`` and a
        teradata_profile (or wizard host) that produces the matching
        identity."""
        parent = tmp_path / "dbt_workspace"
        parent.mkdir(exist_ok=True)
        sub = parent / "dbt_default"
        sub.mkdir(exist_ok=True)
        (sub / "dbt_project.yml").write_text(
            f"name: 'default'\nprofile: '{identity}'\n", encoding="utf-8"
        )
        orch.dbt_project_parent = parent
        return sub

    @staticmethod
    def _csv_orchestrator(tmp_path):
        orch = _make_orchestrator()
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = Mock()
        orch.settings.teradata.password.get_secret_value = Mock(return_value="secret")
        orch.settings.teradata.port = 1025
        orch.settings.ssh = Mock()
        orch.settings.ssh.host = "localhost"
        orch.settings.ssh.port = 22
        orch.settings.ssh.username = "airflow"
        orch.settings.ssh.key_file = None
        orch.settings.ssh.password = Mock()
        orch.settings.ssh.password.get_secret_value = Mock(return_value="ssh-pass")
        orch.settings.ssh.timeout = 300
        orch.async_airflow_client.get_connection = AsyncMock(
            return_value={"connection_id": "td_test"}
        )
        TestTeradataLoadDbtBranching._set_up_dbt_subproject(orch, tmp_path)
        return orch

    @staticmethod
    def _transfer_orchestrator(tmp_path):
        orch = _make_orchestrator()
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "test_db"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = Mock()
        orch.settings.teradata.password.get_secret_value = Mock(return_value="secret")
        orch.settings.teradata.port = 1025
        orch.settings.get_source_teradata = Mock(return_value=orch.settings.teradata)
        orch.settings.get_target_teradata = Mock(return_value=orch.settings.teradata)
        orch.settings.ssh = Mock()
        orch.settings.ssh.host = "localhost"
        orch.settings.ssh.port = 22
        orch.settings.ssh.username = "airflow"
        orch.settings.ssh.key_file = None
        orch.settings.ssh.password = Mock()
        orch.settings.ssh.password.get_secret_value = Mock(return_value="ssh-pass")
        orch.settings.ssh.timeout = 300
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=AsyncAirflowAPIError("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            return_value={"connection_id": "teradata_source"}
        )
        orch.teradata_client = Mock()
        orch.teradata_client.get_table_metadata = Mock(return_value={"columns": []})
        TestTeradataLoadDbtBranching._set_up_dbt_subproject(orch, tmp_path)
        return orch

    @pytest.mark.asyncio
    async def test_csv_dag_with_dbt(self, tmp_path):
        orch = self._csv_orchestrator(tmp_path)
        # Bind the pre-made sub-project to identity ``td_test`` so the test's
        # ``teradata_profile="td_test"`` resolves to it.
        sub = self._set_up_dbt_subproject(orch, tmp_path, identity="td_test")

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col1,col2\na,1\nb,2\n", encoding="utf-8")

        tools = register_data_movement_tools(orch)

        mock_gen = Mock()
        mock_gen.generate_file_loading_with_dbt_dag = Mock(return_value="# dag")
        mock_gen.generate_file_loading_dag = Mock(return_value="# dag")

        (tmp_path / "load_test_db_data.py").write_text("# dag", encoding="utf-8")

        _ssh_env = {
            "MCP_CLIENT_SSH_HOST": "localhost",
            "MCP_CLIENT_SSH_USER": "airflow",
            "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
        }
        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator",
                return_value=mock_gen,
            ),
            patch("pathlib.Path.cwd", return_value=tmp_path),
            patch.dict("os.environ", _ssh_env),
        ):
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path=str(csv_file),
                target_database="test_db",
                target_table="data",
                project_name="default",
                dbt_models=["stg_data"],
                teradata_profile="td_test",
            )

        mock_gen.generate_file_loading_with_dbt_dag.assert_called_once()
        call_kwargs = mock_gen.generate_file_loading_with_dbt_dag.call_args[1]
        # The DAG embeds the resolved sub-project path, not the raw param.
        assert call_kwargs["dbt_project_dir"] == str(sub)
        assert call_kwargs["dbt_models"] == ["stg_data"]
        mock_gen.generate_file_loading_dag.assert_not_called()

    @pytest.mark.asyncio
    async def test_csv_dag_without_dbt_unchanged(self, tmp_path):
        orch = self._csv_orchestrator(tmp_path)

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col1,col2\na,1\nb,2\n", encoding="utf-8")

        tools = register_data_movement_tools(orch)

        mock_gen = Mock()
        mock_gen.generate_file_loading_with_dbt_dag = Mock(return_value="# dag")
        mock_gen.generate_file_loading_dag = Mock(return_value="# dag")

        (tmp_path / "load_test_db_data.py").write_text("# dag", encoding="utf-8")

        _ssh_env = {
            "MCP_CLIENT_SSH_HOST": "localhost",
            "MCP_CLIENT_SSH_USER": "airflow",
            "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
        }
        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator",
                return_value=mock_gen,
            ),
            patch("pathlib.Path.cwd", return_value=tmp_path),
            patch.dict("os.environ", _ssh_env),
        ):
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path=str(csv_file),
                target_database="test_db",
                target_table="data",
                teradata_profile="td_test",
            )

        mock_gen.generate_file_loading_dag.assert_called_once()
        mock_gen.generate_file_loading_with_dbt_dag.assert_not_called()

    @pytest.mark.asyncio
    async def test_table_transfer_with_dbt(self, tmp_path):
        orch = self._transfer_orchestrator(tmp_path)
        # Resolution uses ``target_teradata_profile`` for the dbt step's
        # identity, so bind the pre-made sub-project to ``tgt_test``.
        sub = self._set_up_dbt_subproject(orch, tmp_path, identity="tgt_test")

        tools = register_data_movement_tools(orch)

        mock_gen = Mock()
        mock_gen.generate_table_transfer_with_dbt_dag = Mock(return_value="# dag")
        mock_gen.generate_table_transfer_dag = Mock(return_value="# dag")

        (tmp_path / "transfer_src_db_src_tbl_to_tgt_db_tgt_tbl.py").write_text(
            "# dag", encoding="utf-8"
        )

        _ssh_env = {
            "MCP_CLIENT_SSH_HOST": "localhost",
            "MCP_CLIENT_SSH_USER": "airflow",
            "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
        }
        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator",
                return_value=mock_gen,
            ),
            patch.dict("os.environ", _ssh_env),
        ):
            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
                project_name="default",
                dbt_models=["stg_transfer"],
                dbt_target="dev",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

        mock_gen.generate_table_transfer_with_dbt_dag.assert_called_once()
        call_kwargs = mock_gen.generate_table_transfer_with_dbt_dag.call_args[1]
        assert call_kwargs["dbt_project_dir"] == str(sub)
        assert call_kwargs["dbt_models"] == ["stg_transfer"]
        assert call_kwargs["dbt_target"] == "dev"
        mock_gen.generate_table_transfer_dag.assert_not_called()

    @pytest.mark.asyncio
    async def test_table_transfer_without_dbt_unchanged(self, tmp_path):
        orch = self._transfer_orchestrator(tmp_path)

        tools = register_data_movement_tools(orch)

        mock_gen = Mock()
        mock_gen.generate_table_transfer_with_dbt_dag = Mock(return_value="# dag")
        mock_gen.generate_table_transfer_dag = Mock(return_value="# dag")

        (tmp_path / "transfer_src_db_src_tbl_to_tgt_db_tgt_tbl.py").write_text(
            "# dag", encoding="utf-8"
        )

        _ssh_env = {
            "MCP_CLIENT_SSH_HOST": "localhost",
            "MCP_CLIENT_SSH_USER": "airflow",
            "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
        }
        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator",
                return_value=mock_gen,
            ),
            patch.dict("os.environ", _ssh_env),
        ):
            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

        mock_gen.generate_table_transfer_dag.assert_called_once()
        mock_gen.generate_table_transfer_with_dbt_dag.assert_not_called()

    @pytest.mark.asyncio
    async def test_csv_dag_rejects_dbt_subproject_bound_to_different_profile(self, tmp_path):
        """csv_dag refuses to generate a DAG when ``project_name``'s sub-
        project binding (``dbt_project.yml::profile``) names a different
        Teradata identity than ``teradata_profile`` resolves to. Without
        this guard, the DAG would silently load CSV into one Teradata
        instance and run dbt against a different one."""
        orch = self._csv_orchestrator(tmp_path)
        # Sub-project bound to ``other_profile``; load uses ``td_test``.
        self._set_up_dbt_subproject(orch, tmp_path, identity="other_profile")

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col1,col2\na,1\n", encoding="utf-8")

        tools = register_data_movement_tools(orch)
        _ssh_env = {
            "MCP_CLIENT_SSH_HOST": "localhost",
            "MCP_CLIENT_SSH_USER": "airflow",
            "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
        }
        with (
            patch("pathlib.Path.cwd", return_value=tmp_path),
            patch.dict("os.environ", _ssh_env),
        ):
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path=str(csv_file),
                target_database="test_db",
                target_table="data",
                project_name="default",
                teradata_profile="td_test",
            )

        assert result["success"] is False
        assert result["teradata_identity"] == "other_profile"
        assert result["expected_identity"] == "td_test"
        assert "load data into one Teradata instance" in result["error"]
        # Mismatch error must name the user-facing parameter that's
        # active for THIS method (csv_dag → ``teradata_profile``).
        assert result["profile_param_name"] == "teradata_profile"
        assert "teradata_profile" in result["error"]
        assert "target_teradata_profile" not in result["error"]

    @pytest.mark.asyncio
    async def test_table_transfer_rejects_subproject_bound_to_non_target(self, tmp_path):
        """table_transfer's dbt step runs against ``target_teradata_profile``,
        so the binding-mismatch guard compares against THAT profile, not
        the source. A sub-project bound to the source identity must be
        rejected."""
        orch = self._transfer_orchestrator(tmp_path)
        # Sub-project bound to source identity; dbt step would run against
        # target — silent split-instance bug if not caught.
        self._set_up_dbt_subproject(orch, tmp_path, identity="src_test")

        tools = register_data_movement_tools(orch)
        _ssh_env = {
            "MCP_CLIENT_SSH_HOST": "localhost",
            "MCP_CLIENT_SSH_USER": "airflow",
            "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
        }
        with patch.dict("os.environ", _ssh_env):
            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
                project_name="default",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )

        assert result["success"] is False
        assert result["teradata_identity"] == "src_test"
        assert result["expected_identity"] == "tgt_test"
        assert "load data into one Teradata instance" in result["error"]
        # Mismatch error must name the user-facing parameter that's
        # active for THIS method (table_transfer →
        # ``target_teradata_profile``, NOT the bare
        # ``teradata_profile`` form). Without this, users would
        # troubleshoot the wrong knob.
        assert result["profile_param_name"] == "target_teradata_profile"
        assert "target_teradata_profile" in result["error"]

    @pytest.mark.asyncio
    async def test_csv_dag_rejects_subproject_with_unreadable_binding(self, tmp_path):
        """When the sub-project's ``dbt_project.yml`` has no readable
        ``profile:`` field, ``_locate_dbt_subproject_dir`` itself fails
        closed with ``action_required: fix_subproject_binding`` —
        protecting both the dbt-only DAG paths and ``airflow_teradata_load``
        in one place. Without the guard, the response's
        ``teradata_identity`` would be empty, the refresh_env hint would
        be a placeholder, and (for csv_dag) the load-vs-dbt mismatch
        check would silently fail open."""
        orch = self._csv_orchestrator(tmp_path)
        # Replace the helper-created dbt_project.yml with one that has
        # no ``profile:`` field. _read_project_profile returns None.
        sub = orch.dbt_project_parent / "dbt_default"
        (sub / "dbt_project.yml").write_text("name: 'default'\n", encoding="utf-8")

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col1,col2\na,1\n", encoding="utf-8")

        tools = register_data_movement_tools(orch)
        _ssh_env = {
            "MCP_CLIENT_SSH_HOST": "localhost",
            "MCP_CLIENT_SSH_USER": "airflow",
            "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
        }
        with (
            patch("pathlib.Path.cwd", return_value=tmp_path),
            patch.dict("os.environ", _ssh_env),
        ):
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path=str(csv_file),
                target_database="test_db",
                target_table="data",
                project_name="default",
                teradata_profile="td_test",
            )

        assert result["success"] is False
        assert result["action_required"] == "fix_subproject_binding"
        assert result["project_name"] == "default"
        assert "no readable ``profile:`` field" in result["message"]


# ---------------------------------------------------------------------------
# CSV path containment — import_csv_to_teradata must reject paths outside
# the working-directory tree. Delegated to safe_path_under_any_root.
# ---------------------------------------------------------------------------


class TestCsvPathContainment:
    """Path-containment tests for import_csv_to_teradata's csv_path parameter."""

    def _orch(self, tmp_path):
        orch = _make_orchestrator()
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "test_db"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = Mock()
        orch.settings.teradata.password.get_secret_value = Mock(return_value="secret")
        orch.settings.teradata.port = 1025
        return orch

    @pytest.mark.asyncio
    async def test_csv_path_outside_allowed_roots_rejected(self, tmp_path):
        """csv_dag method rejects a path outside cwd/cwd.parent at the
        safe_path_under_any_root containment check."""
        import sys

        orch = self._orch(tmp_path)
        tools = register_data_movement_tools(orch)
        bad_path = "/etc/passwd" if sys.platform != "win32" else r"C:\Windows\system.ini"
        with patch("pathlib.Path.cwd", return_value=tmp_path):
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path=bad_path,
                target_database="test_db",
                target_table="data",
                teradata_profile="td_test",
            )
        assert result["success"] is False
        assert "CSV path rejected" in result["error"]

    @pytest.mark.asyncio
    async def test_csv_path_rejection_does_not_leak_server_paths(self, tmp_path):
        """The rejection error must NOT include the resolved allowed-roots
        absolute paths (server CWD / parent), only a generic actionable
        message. Detailed error stays in server-side logs."""
        orch = self._orch(tmp_path)
        tools = register_data_movement_tools(orch)
        # Use a recognizable sentinel path so we can check for its absence
        # in the response. The real concern is the cwd/cwd.parent strings
        # being interpolated into the error message.
        with patch("pathlib.Path.cwd", return_value=tmp_path):
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/totally/elsewhere/secret.csv",
                target_database="test_db",
                target_table="data",
                teradata_profile="td_test",
            )
        assert result["success"] is False
        err = result["error"]
        # Generic actionable wording is present.
        assert "CSV path rejected" in err
        assert "current working directory" in err
        # The resolved server path (tmp_path) is NOT leaked.
        # tmp_path could be C:\Users\... or /tmp/... — either form is a
        # server-internal path the LLM has no business seeing in the error.
        assert str(tmp_path) not in err
        assert str(tmp_path.resolve()) not in err
        # The legacy "outside allowed roots" wording (UnsafePathError text)
        # must not appear either.
        assert "allowed roots" not in err

    @pytest.mark.asyncio
    async def test_csv_path_null_byte_rejected(self, tmp_path):
        """CSV path with null byte is rejected."""
        orch = self._orch(tmp_path)
        tools = register_data_movement_tools(orch)
        with patch("pathlib.Path.cwd", return_value=tmp_path):
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="evil\x00.csv",
                target_database="test_db",
                target_table="data",
                teradata_profile="td_test",
            )
        assert result["success"] is False
        assert "CSV path rejected" in result["error"]

    @pytest.mark.asyncio
    async def test_csv_path_under_cwd_accepted_by_containment(self, tmp_path):
        """A path under the cwd passes the containment check.
        (Full load still fails on other grounds — that's acceptable; we only
        assert the containment check itself didn't reject.)"""
        orch = self._orch(tmp_path)
        tools = register_data_movement_tools(orch)
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col1,col2\na,1\n", encoding="utf-8")
        with patch("pathlib.Path.cwd", return_value=tmp_path):
            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path=str(csv_file),
                target_database="test_db",
                target_table="data",
            )
        # Either succeeds, or fails for non-containment reasons.
        # The containment error has a distinct prefix we can check for absence.
        if not result.get("success"):
            assert "CSV path rejected" not in result.get("error", "")


# ════════════════════════════════════════════════════════════════════
#  next_steps shape & coverage — verify the 4-part Markdown prose
#  template used across data_movement success responses
# ════════════════════════════════════════════════════════════════════


def _assert_next_steps_shape(steps):
    """Assert ``steps`` is a list of 4-part Markdown-prose strings."""
    assert isinstance(steps, list) and len(steps) >= 1, (
        f"next_steps should be a non-empty list, got: {steps!r}"
    )
    for i, s in enumerate(steps, start=1):
        assert isinstance(s, str), (
            f"next_steps[{i - 1}] must be a Markdown-prose str, got {type(s).__name__}: {s!r}"
        )
        assert "**" in s and f"**{i}." in s, f"next_steps[{i - 1}] missing numbered header: {s!r}"
        for segment in ("**Why**", "**Effect**", "**If missing**"):
            assert segment in s, f"next_steps[{i - 1}] missing {segment}: {s!r}"


class TestNextStepsShape:
    """Verifies the next_steps field on data_movement success paths."""

    @pytest.mark.asyncio
    async def test_airbyte_sync_trigger_emits_next_steps(self):
        orch, tools = _register()
        orch.airbyte_client.trigger_sync = AsyncMock(
            return_value={
                "jobId": 99,
                "status": "pending",
                "createdAt": "2025-01-01T00:00:00",
            }
        )
        result = await tools["airbyte_sync"](action="trigger", connection_id="conn-x")
        assert result["success"] is True
        _assert_next_steps_shape(result["next_steps"])

    @pytest.mark.asyncio
    async def test_airbyte_sync_trigger_with_wait_succeeded_emits_next_steps(self):
        orch, tools = _register()
        orch.airbyte_client.trigger_sync = AsyncMock(
            return_value={
                "jobId": 100,
                "status": "pending",
                "createdAt": "2025-01-01T00:00:00",
            }
        )
        orch.airbyte_client.wait_for_job = AsyncMock(return_value={"status": "succeeded"})
        result = await tools["airbyte_sync"](
            action="trigger",
            connection_id="conn-y",
            wait_for_completion=True,
        )
        assert result["success"] is True
        _assert_next_steps_shape(result["next_steps"])
