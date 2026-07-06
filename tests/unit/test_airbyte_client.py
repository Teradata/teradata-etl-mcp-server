"""Unit tests for Airbyte client."""

import asyncio
import logging
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import httpx
import pytest
from httpx import HTTPStatusError, RequestError

from elt_mcp_server.clients.airbyte_client import (
    AirbyteAPIError,
    AirbyteClient,
    AirbyteConnectionError,
    AirbyteRateLimitExceeded,
    AirbyteResponseTooLarge,
    AirbyteSyncError,
    CircuitBreakerOpen,
    RateLimiter,
    to_public_api_sync_mode,
)
from elt_mcp_server.clients.async_airflow_client import AsyncAirflowAPIError
from elt_mcp_server.tools.data_movement import (
    DiscoveryCache,
    _baseline_normalize_config,
    _coerce_to_type,
    _configs_equivalent,
    _extract_stream_names,
    _find_or_create_connector,
    _fuzzy_token_score,
    _get_connector_spec,
    _is_config_subset,
    _levenshtein_distance,
    _mask_sensitive_data,
    _normalize_stream_item,
    _normalize_with_spec,
    _score_stream_v2,
    _shape_config_to_spec,
    _suggest_stream_names,
    _validate_cursor_fields,
    _validate_stream_names,
    _validate_sync_modes,
    register_data_movement_tools,
)


@pytest.fixture(autouse=True)
def _bypass_csv_path_security_check(monkeypatch):
    """CSV tests mock ``data_movement.Path`` to control ``csv_file.exists()``.

    The security-fixes commit moved the CSV trust-boundary check from inline
    ``Path(csv_path).resolve()`` in ``data_movement`` to the shared
    ``safe_path_under_any_root`` helper in ``utils.file_operations``. The
    helper resolves paths through ``file_operations.Path`` (not
    ``data_movement.Path``), so the tests' Path patching no longer intercepts
    the containment check — causing ``UnsafePathError`` on mock paths and
    returning ``"DAG generation failed"``.

    Rebind the helper in ``data_movement`` to a thin shim that returns
    ``data_movement.Path(user_path)`` — the already-mocked Path — so the
    existing test fixtures work unchanged. This only replaces the binding in
    ``data_movement``, not the canonical helper in ``utils.file_operations``.
    """
    from elt_mcp_server.tools import data_movement

    monkeypatch.setattr(
        data_movement,
        "safe_path_under_any_root",
        lambda user_path, _allowed_roots: data_movement.Path(user_path),
    )


class TestAirbyteClient:
    """Test suite for AirbyteClient."""

    @pytest.fixture
    def client_config(self):
        """Test client configuration."""
        return {
            "url": "http://localhost:8000",
            "username": "airbyte",
            "password": "password",
            "workspace_id": "ws1",
        }

    @pytest.fixture
    def client(self, client_config):
        """Create AirbyteClient instance."""
        return AirbyteClient(**client_config)

    @pytest.fixture
    def mock_response(self):
        """Create mock HTTP response."""
        response = Mock()
        response.status_code = 200
        response.content = b"{}"
        response.json.return_value = {}
        response.raise_for_status = Mock()
        return response

    def _http_response(self, data: Any, status_code: int = 200) -> Mock:
        """Helper to build a realistic mock httpx response."""
        resp = Mock()
        resp.status_code = status_code
        resp.content = b"{}" if status_code != 204 else b""
        resp.json.return_value = data
        resp.raise_for_status = Mock()
        resp.headers = {"Content-Type": "application/json"}
        return resp

    # Connection Tests

    @pytest.mark.asyncio
    async def test_test_connection_success(self, client):
        """Test successful connection validation."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(return_value=self._http_response({"available": True})),
        ):
            result = await client.test_connection()
            assert isinstance(result, dict)
            assert result["connected"] is True

    @pytest.mark.asyncio
    async def test_test_connection_failure(self, client):
        """Test connection validation failure."""
        with patch.object(
            client.client, "request", new=AsyncMock(side_effect=RequestError("Connection failed"))
        ):
            result = await client.test_connection()
            assert isinstance(result, dict)
            assert result["connected"] is False

    # Workspace Tests

    @pytest.mark.asyncio
    async def test_list_workspaces(self, client):
        """Test listing workspaces."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "data": [
                            {"workspaceId": "ws1", "name": "Default"},
                            {"workspaceId": "ws2", "name": "Production"},
                        ]
                    }
                )
            ),
        ):
            result = await client.list_workspaces()
            assert len(result) == 2
            assert result[0]["name"] == "Default"
            assert result[1]["workspaceId"] == "ws2"

    @pytest.mark.asyncio
    async def test_get_workspace(self, client):
        """Test getting workspace details."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {"workspaceId": "ws1", "name": "Default", "initialSetupComplete": True}
                )
            ),
        ):
            result = await client.get_workspace("ws1")
            assert result["workspaceId"] == "ws1"
            assert result["initialSetupComplete"] is True

    # Source Tests

    @pytest.mark.asyncio
    async def test_list_sources(self, client):
        """Test listing sources."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "data": [
                            {
                                "sourceId": "src1",
                                "name": "Postgres Source",
                                "sourceName": "postgres",
                            },
                            {"sourceId": "src2", "name": "MySQL Source", "sourceName": "mysql"},
                        ]
                    }
                )
            ),
        ):
            result = await client.list_sources("ws1")
            assert len(result) == 2
            assert result[0]["sourceName"] == "postgres"
            assert result[1]["name"] == "MySQL Source"

    @pytest.mark.asyncio
    async def test_get_source(self, client):
        """Test getting source details."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "sourceId": "src1",
                        "name": "Postgres Source",
                        "sourceName": "postgres",
                        "configuration": {"host": "localhost", "port": 5432, "database": "mydb"},
                    }
                )
            ),
        ):
            result = await client.get_source("src1")
            assert result["sourceId"] == "src1"
            assert result["configuration"]["database"] == "mydb"

    @pytest.mark.asyncio
    async def test_create_source(self, client):
        """Test creating a source."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response({"sourceId": "new_src", "name": "New Source"})
            ),
        ):
            result = await client.create_source(
                "ws1",
                "postgres-def-id",
                "New Source",
                {"host": "localhost", "database": "testdb"},
            )
            assert result is not None
            assert result["sourceId"] == "new_src"

    @pytest.mark.asyncio
    async def test_update_source(self, client):
        """Test updating a source."""
        get_resp = self._http_response(
            {"name": "Old Source", "configuration": {"host": "old-host"}}
        )
        patch_resp = self._http_response({"sourceId": "src1", "name": "Updated Source"})
        with patch.object(
            client.client, "request", new=AsyncMock(side_effect=[get_resp, patch_resp])
        ):
            result = await client.update_source(
                "src1",
                name="Updated Source",
                connection_configuration={"host": "new-host"},
            )
            assert isinstance(result, dict)
            assert result["sourceId"] == "src1"

    @pytest.mark.asyncio
    async def test_delete_source(self, client):
        """Test deleting a source."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(return_value=self._http_response({}, status_code=204)),
        ):
            result = await client.delete_source("src1")
            assert result is True

    # Destination Tests

    @pytest.mark.asyncio
    async def test_list_destinations(self, client):
        """Test listing destinations."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "data": [
                            {
                                "destinationId": "dst1",
                                "name": "Snowflake",
                                "destinationName": "snowflake",
                            },
                            {
                                "destinationId": "dst2",
                                "name": "BigQuery",
                                "destinationName": "bigquery",
                            },
                        ]
                    }
                )
            ),
        ):
            result = await client.list_destinations("ws1")
            assert len(result) == 2
            assert result[0]["destinationName"] == "snowflake"
            assert result[1]["name"] == "BigQuery"

    @pytest.mark.asyncio
    async def test_get_destination(self, client):
        """Test getting destination details."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "destinationId": "dst1",
                        "name": "Snowflake Dest",
                        "destinationName": "snowflake",
                        "configuration": {"host": "account.snowflake.com", "database": "warehouse"},
                    }
                )
            ),
        ):
            result = await client.get_destination("dst1")
            assert result["destinationId"] == "dst1"
            assert result["configuration"]["database"] == "warehouse"

    @pytest.mark.asyncio
    async def test_create_destination(self, client):
        """Test creating a destination."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(return_value=self._http_response({"destinationId": "new_dst"})),
        ):
            result = await client.create_destination(
                "ws1",
                "snowflake-def-id",
                "New Destination",
                {"host": "account.snowflake.com"},
            )
            assert result is not None
            assert result["destinationId"] == "new_dst"

    # Connection Tests (Airbyte Connections)

    @pytest.mark.asyncio
    async def test_list_connections(self, client):
        """Test listing connections."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "data": [
                            {
                                "connectionId": "conn1",
                                "name": "Postgres to Snowflake",
                                "sourceId": "src1",
                                "destinationId": "dst1",
                                "status": "active",
                            },
                            {
                                "connectionId": "conn2",
                                "name": "MySQL to BigQuery",
                                "sourceId": "src2",
                                "destinationId": "dst2",
                                "status": "inactive",
                            },
                        ]
                    }
                )
            ),
        ):
            result = await client.list_connections("ws1")
            assert len(result) == 2
            assert result[0]["name"] == "Postgres to Snowflake"
            assert result[1]["status"] == "inactive"

    @pytest.mark.asyncio
    async def test_get_connection(self, client):
        """Test getting connection details."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "connectionId": "conn1",
                        "name": "Test Connection",
                        "sourceId": "src1",
                        "destinationId": "dst1",
                        "status": "active",
                        "syncCatalog": {
                            "streams": [{"stream": {"name": "users"}, "config": {"selected": True}}]
                        },
                        "schedule": {"units": 24, "timeUnit": "hours"},
                    }
                )
            ),
        ):
            result = await client.get_connection("conn1")
            assert result["connectionId"] == "conn1"
            assert result["status"] == "active"
            assert len(result["syncCatalog"]["streams"]) == 1

    @pytest.mark.asyncio
    async def test_create_connection(self, client):
        """Test creating a connection."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response({"connectionId": "new_conn", "status": "active"})
            ),
        ):
            conn_config = {
                "name": "New Connection",
                "sourceId": "src1",
                "destinationId": "dst1",
                "syncCatalog": {"streams": []},
                "schedule": {"units": 24, "timeUnit": "hours"},
            }
            result = await client.create_connection(raw_payload=conn_config)
            assert result is not None
            assert result["connectionId"] == "new_conn"

    @pytest.mark.asyncio
    async def test_update_connection_with_configurations(self, client):
        """Test updating a connection with Public API 'configurations' payload."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response({"connectionId": "conn1", "status": "active"})
            ),
        ):
            result = await client.update_connection(
                "conn1",
                configurations={"streams": [{"name": "users", "syncMode": "full_refresh_append"}]},
            )
            assert result["connectionId"] == "conn1"

    @pytest.mark.asyncio
    async def test_delete_connection(self, client):
        """Test deleting a connection."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(return_value=self._http_response({}, status_code=204)),
        ):
            result = await client.delete_connection("conn1")
            assert result is True

    # Sync Operations Tests

    @pytest.mark.asyncio
    async def test_trigger_sync(self, client):
        """Test triggering a sync job."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "job": {
                            "id": "job123",
                            "status": "pending",
                            "createdAt": "2025-12-31T12:00:00Z",
                        }
                    }
                )
            ),
        ):
            result = await client.trigger_sync("conn1")
            assert result is not None
            assert result["job"]["id"] == "job123"
            assert result["job"]["status"] == "pending"

    @pytest.mark.asyncio
    async def test_get_job_status(self, client):
        """Test getting sync job status."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "job": {
                            "id": "job123",
                            "status": "running",
                            "startedAt": "2025-12-31T12:00:00Z",
                            "updatedAt": "2025-12-31T12:05:00Z",
                        }
                    }
                )
            ),
        ):
            result = await client.get_job_status("job123")
            assert result["job"]["id"] == "job123"
            assert result["job"]["status"] == "running"

    @pytest.mark.asyncio
    async def test_get_job_status_completed(self, client):
        """Test getting completed sync job status."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "job": {
                            "id": "job123",
                            "status": "succeeded",
                            "startedAt": "2025-12-31T12:00:00Z",
                            "completedAt": "2025-12-31T12:30:00Z",
                            "bytesSync": 1048576,
                            "recordsSync": 10000,
                        }
                    }
                )
            ),
        ):
            result = await client.get_job_status("job123")
            assert result["job"]["status"] == "succeeded"
            assert result["job"]["bytesSync"] == 1048576
            assert result["job"]["recordsSync"] == 10000

    @pytest.mark.asyncio
    async def test_list_jobs(self, client):
        """Test listing sync jobs."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "data": [
                            {"id": "job1", "status": "succeeded", "connectionId": "conn1"},
                            {"id": "job2", "status": "running", "connectionId": "conn1"},
                            {"id": "job3", "status": "failed", "connectionId": "conn1"},
                        ]
                    }
                )
            ),
        ):
            result = await client.list_jobs("conn1")
            assert len(result) == 3
            assert result[0]["status"] == "succeeded"
            assert result[2]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_get_sync_progress(self, client):
        """Test getting sync progress."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "job": {
                            "id": "job123",
                            "status": "running",
                            "bytesSync": 524288,
                            "recordsSync": 5000,
                            "streams": [
                                {"streamName": "users", "recordsSync": 3000},
                                {"streamName": "orders", "recordsSync": 2000},
                            ],
                        }
                    }
                )
            ),
        ):
            result = await client.get_job_status("job123")
            assert result["job"]["bytesSync"] == 524288
            assert len(result["job"]["streams"]) == 2

    # Stream Discovery Tests

    @pytest.mark.asyncio
    async def test_discover_schema(self, client):
        """Test discovering source schema with combined sync modes from Public API v1."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "data": [
                            {
                                "name": "users",
                                "syncModes": [
                                    "full_refresh_overwrite",
                                    "full_refresh_append",
                                    "incremental_append",
                                    "incremental_deduped_history",
                                ],
                            },
                            {
                                "name": "orders",
                                "syncModes": ["full_refresh_overwrite", "full_refresh_append"],
                            },
                        ]
                    }
                )
            ),
        ):
            result = await client.discover_schema("src1")
            assert "catalog" in result
            assert len(result["catalog"]["streams"]) == 2
            assert result["catalog"]["streams"][0]["stream"]["name"] == "users"
            # Combined modes should be normalized to simple modes
            users_modes = result["catalog"]["streams"][0]["stream"]["supportedSyncModes"]
            assert "incremental" in users_modes
            assert "full_refresh" in users_modes
            orders_modes = result["catalog"]["streams"][1]["stream"]["supportedSyncModes"]
            assert "full_refresh" in orders_modes
            assert "incremental" not in orders_modes

    @pytest.mark.asyncio
    async def test_discover_schema_null_sync_modes(self, client):
        """Test that null syncModes defaults to both full_refresh and incremental."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "data": [
                            {
                                "name": "customer",
                                "syncModes": None,
                                "defaultCursorField": [],
                                "sourceDefinedPrimaryKey": [["custkey"]],
                            },
                        ]
                    }
                )
            ),
        ):
            result = await client.discover_schema("src1")
            modes = result["catalog"]["streams"][0]["stream"]["supportedSyncModes"]
            assert "full_refresh" in modes
            assert "incremental" in modes

    # Definition Tests

    @pytest.mark.asyncio
    async def test_list_source_definitions(self, client):
        """Test listing available source definitions."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "data": [
                            {"sourceDefinitionId": "postgres-id", "name": "Postgres"},
                            {"sourceDefinitionId": "mysql-id", "name": "MySQL"},
                        ]
                    }
                )
            ),
        ):
            result = await client.list_source_definitions()
            assert len(result) == 2
            assert result[0]["name"] == "Postgres"

    @pytest.mark.asyncio
    async def test_list_destination_definitions(self, client):
        """Test listing available destination definitions."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response(
                    {
                        "data": [
                            {"destinationDefinitionId": "snowflake-id", "name": "Snowflake"},
                            {"destinationDefinitionId": "bigquery-id", "name": "BigQuery"},
                        ]
                    }
                )
            ),
        ):
            result = await client.list_destination_definitions()
            assert len(result) == 2
            assert result[1]["name"] == "BigQuery"

    # Error Handling Tests

    @pytest.mark.asyncio
    async def test_handle_404_error(self, client):
        """Test handling 404 not found errors."""
        bad_resp = self._http_response({}, status_code=404)
        bad_resp.raise_for_status.side_effect = HTTPStatusError(
            "Not Found", request=Mock(), response=Mock(status_code=404)
        )
        with patch.object(client.client, "request", new=AsyncMock(return_value=bad_resp)):
            with pytest.raises(AirbyteAPIError):
                await client.get_connection("non_existent")

    @pytest.mark.asyncio
    async def test_handle_network_error(self, client):
        """Test handling network errors."""
        with patch.object(
            client.client, "request", new=AsyncMock(side_effect=RequestError("Network unreachable"))
        ):
            with pytest.raises(AirbyteConnectionError):
                await client.list_connections("ws1")

    @pytest.mark.asyncio
    async def test_handle_timeout(self, client):
        """Test handling timeout errors."""
        with patch.object(
            client.client, "request", new=AsyncMock(side_effect=RequestError("Request timeout"))
        ):
            with pytest.raises(AirbyteConnectionError):
                await client.trigger_sync("conn1")

    @pytest.mark.asyncio
    async def test_handle_invalid_json(self, client):
        """Test handling invalid JSON responses."""
        bad_resp = self._http_response({}, status_code=200)
        bad_resp.json.side_effect = ValueError("Invalid JSON")
        with patch.object(client.client, "request", new=AsyncMock(return_value=bad_resp)):
            with pytest.raises(ValueError):
                await client.get_connection("conn1")

    # Stream Configuration Tests

    # Statistics Tests

    @pytest.mark.asyncio
    async def test_get_connection_statistics(self, client):
        """Test getting connection statistics."""
        with patch.object(client, "list_jobs") as mock_list_jobs:
            mock_list_jobs.return_value = [
                {"status": "succeeded", "bytesSync": 1000000, "recordsSync": 10000},
                {"status": "succeeded", "bytesSync": 2000000, "recordsSync": 20000},
                {"status": "failed", "bytesSync": 0, "recordsSync": 0},
            ]
            result = await client.get_connection_sync_history("conn1")
            assert "recent_syncs" in result
            assert len(result["recent_syncs"]) == 3

    # Batch Operations Tests

    @pytest.mark.asyncio
    async def test_batch_trigger_syncs(self, client):
        """Test triggering multiple syncs."""
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response({"job": {"id": "job123", "status": "pending"}})
            ),
        ):
            connection_ids = ["conn1", "conn2", "conn3"]
            results = []
            for conn_id in connection_ids:
                result = await client.trigger_sync(conn_id)
                results.append(result)
            assert len(results) == 3


class TestNormalizeStreamItem:
    """Tests for _normalize_stream_item helper."""

    def test_snake_to_camel(self):
        item = {"name": "t", "sync_mode": "incremental", "destination_sync_mode": "append"}
        result = _normalize_stream_item(item)
        assert result["syncMode"] == "incremental"
        assert result["destinationSyncMode"] == "append"
        assert "sync_mode" not in result
        assert "destination_sync_mode" not in result

    def test_cursor_and_pk(self):
        item = {"name": "t", "cursor_field": ["id"], "primary_key": [["pk"]]}
        result = _normalize_stream_item(item)
        assert result["cursorField"] == ["id"]
        assert result["primaryKey"] == [["pk"]]

    def test_default_selected_true(self):
        result = _normalize_stream_item({"name": "t"})
        assert result["selected"] is True

    def test_default_selected_false(self):
        result = _normalize_stream_item({"name": "t"}, default_selected=False)
        assert "selected" not in result


class TestValidateSyncModes:
    """Tests for _validate_sync_modes helper."""

    def test_valid_streams(self):
        streams = [{"name": "t", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}]
        assert _validate_sync_modes(streams) is None

    def test_missing_sync_mode(self):
        streams = [{"name": "t", "destinationSyncMode": "overwrite"}]
        result = _validate_sync_modes(streams)
        assert result is not None
        assert result["action_required"] == "clarify_sync_configuration"
        assert "t" in result["missing_details"]["syncMode"]

    def test_missing_dest_sync_mode(self):
        streams = [{"name": "t", "syncMode": "full_refresh"}]
        result = _validate_sync_modes(streams)
        assert result is not None
        assert "t" in result["missing_details"]["destinationSyncMode"]

    def test_snake_case_accepted(self):
        streams = [{"name": "t", "sync_mode": "incremental", "destination_sync_mode": "append"}]
        assert _validate_sync_modes(streams) is None


class TestValidateCursorFields:
    """Tests for _validate_cursor_fields async helper."""

    @pytest.mark.asyncio
    async def test_non_incremental_skip(self):
        streams = [{"name": "t", "syncMode": "full_refresh"}]
        result = await _validate_cursor_fields(streams, "src1", AsyncMock())
        assert result is None

    @pytest.mark.asyncio
    async def test_missing_cursor(self):
        mock_client = AsyncMock()
        mock_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [{"stream": {"name": "t", "propertyFields": [["id"], ["updated_at"]]}}]
            }
        }
        streams = [{"name": "t", "syncMode": "incremental"}]
        result = await _validate_cursor_fields(streams, "src1", mock_client)
        assert result is not None
        assert result["streams"]["t"]["issue"] == "missing"
        assert "id" in result["streams"]["t"]["available_columns"]

    @pytest.mark.asyncio
    async def test_invalid_cursor(self):
        mock_client = AsyncMock()
        mock_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [{"stream": {"name": "t", "propertyFields": [["id"], ["updated_at"]]}}]
            }
        }
        streams = [{"name": "t", "syncMode": "incremental", "cursorField": "bad_col"}]
        result = await _validate_cursor_fields(streams, "src1", mock_client)
        assert result is not None
        assert result["streams"]["t"]["issue"] == "invalid"
        assert result["streams"]["t"]["provided"] == "bad_col"

    @pytest.mark.asyncio
    async def test_valid_cursor(self):
        mock_client = AsyncMock()
        mock_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [{"stream": {"name": "t", "propertyFields": [["id"], ["updated_at"]]}}]
            }
        }
        streams = [{"name": "t", "syncMode": "incremental", "cursorField": "updated_at"}]
        result = await _validate_cursor_fields(streams, "src1", mock_client)
        assert result is None


class TestFindOrCreateConnector:
    """Tests for _find_or_create_connector async helper."""

    def _make_orchestrator(self, sources=None, destinations=None, spec=None):
        """Build a mock orchestrator with configured airbyte_client."""
        client = AsyncMock()
        client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "def1", "name": "Postgres"}
        ]
        client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "def2", "name": "Snowflake"}
        ]
        client.list_sources.return_value = sources or []
        client.list_destinations.return_value = destinations or []
        client.get_source.side_effect = lambda sid: next(
            (s for s in (sources or []) if s.get("sourceId") == sid), {}
        )
        client.get_destination.side_effect = lambda did: next(
            (d for d in (destinations or []) if d.get("destinationId") == did), {}
        )
        client._get_workspace_id.return_value = "ws1"
        client.create_source.return_value = {"sourceId": "new_src", "name": "NewSrc"}
        client.create_destination.return_value = {"destinationId": "new_dst", "name": "NewDst"}
        orch = Mock()
        orch.airbyte_client = client
        return orch

    @pytest.mark.asyncio
    async def test_name_reuse(self):
        orch = self._make_orchestrator(
            sources=[
                {
                    "sourceId": "s1",
                    "name": "MyPG",
                    "definitionId": "def1",
                    "configuration": {"host": "h"},
                }
            ]
        )
        result = await _find_or_create_connector("source", "MyPG", "def1", {"host": "h"}, orch)
        assert result["success"] is True
        assert result["reused"] is True
        assert result["source"]["sourceId"] == "s1"

    @pytest.mark.asyncio
    async def test_create_new_source(self):
        orch = self._make_orchestrator(sources=[])
        result = await _find_or_create_connector("source", "Fresh", "def1", {"host": "h"}, orch)
        assert result["success"] is True
        assert result["reused"] is False
        assert result["source"]["sourceId"] == "new_src"

    @pytest.mark.asyncio
    async def test_name_reuse_destination(self):
        orch = self._make_orchestrator(
            destinations=[
                {
                    "destinationId": "d1",
                    "name": "MySF",
                    "definitionId": "def2",
                    "configuration": {"host": "sf"},
                }
            ]
        )
        result = await _find_or_create_connector(
            "destination", "MySF", "def2", {"host": "sf"}, orch
        )
        assert result["success"] is True
        assert result["reused"] is True
        assert result["destination"]["destinationId"] == "d1"

    @pytest.mark.asyncio
    async def test_create_new_destination(self):
        orch = self._make_orchestrator(destinations=[])
        result = await _find_or_create_connector(
            "destination", "FreshDst", "def2", {"host": "sf"}, orch
        )
        assert result["success"] is True
        assert result["reused"] is False
        assert result["destination"]["destinationId"] == "new_dst"


class TestToPublicApiSyncMode:
    """Tests for to_public_api_sync_mode."""

    def test_incremental_append(self):
        assert to_public_api_sync_mode("incremental", "append") == "incremental_append"

    def test_incremental_dedup(self):
        assert (
            to_public_api_sync_mode("incremental", "append_dedup") == "incremental_deduped_history"
        )

    def test_full_refresh_overwrite(self):
        assert to_public_api_sync_mode("full_refresh", "overwrite") == "full_refresh_overwrite"

    def test_full_refresh_append(self):
        assert to_public_api_sync_mode("full_refresh", "append") == "full_refresh_append"

    def test_none_defaults(self):
        result = to_public_api_sync_mode(None, None)
        assert result == "full_refresh_append"

    def test_dedup_fallback_full_refresh(self):
        result = to_public_api_sync_mode("full_refresh", "deduped_history")
        assert result == "full_refresh_overwrite"


class TestIsConfigSubset:
    """Tests for _is_config_subset."""

    def test_exact_match(self):
        assert _is_config_subset({"a": 1}, {"a": 1}) is True

    def test_subset(self):
        assert _is_config_subset({"a": 1}, {"a": 1, "b": 2}) is True

    def test_not_subset(self):
        assert _is_config_subset({"a": 1, "c": 3}, {"a": 1, "b": 2}) is False

    def test_nested(self):
        assert _is_config_subset({"a": {"x": 1}}, {"a": {"x": 1, "y": 2}}) is True

    def test_lists(self):
        assert _is_config_subset({"a": [1, 2]}, {"a": [1, 2]}) is True
        assert _is_config_subset({"a": [1, 2]}, {"a": [1, 3]}) is False


class TestBuildConfiguredCatalog:
    """Tests for AirbyteClient.build_configured_catalog."""

    def _make_client(self):
        return AirbyteClient(url="http://localhost:8000", workspace_id="ws1")

    @pytest.mark.asyncio
    async def test_basic_build(self):
        client = self._make_client()
        discovery_result = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "users",
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "propertyFields": [["id"], ["name"]],
                        }
                    }
                ]
            }
        }
        with patch.object(
            client, "discover_source_schema", new=AsyncMock(return_value=discovery_result)
        ):
            result = await client.build_configured_catalog(
                source_id="src1",
                selected_streams=[
                    {
                        "name": "users",
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "overwrite",
                    }
                ],
            )
            assert len(result["streams"]) == 1
            assert result["streams"][0]["config"]["syncMode"] == "full_refresh"

    @pytest.mark.asyncio
    async def test_cursor_propagation(self):
        client = self._make_client()
        discovery_result = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "orders",
                            "supportedSyncModes": ["incremental"],
                            "propertyFields": [["id"], ["updated_at"]],
                        }
                    }
                ]
            }
        }
        with patch.object(
            client, "discover_source_schema", new=AsyncMock(return_value=discovery_result)
        ):
            result = await client.build_configured_catalog(
                source_id="src1",
                selected_streams=[
                    {
                        "name": "orders",
                        "syncMode": "incremental",
                        "destinationSyncMode": "append",
                        "cursorField": "updated_at",
                    }
                ],
            )
            assert result["streams"][0]["config"]["cursorField"] == ["updated_at"]

    @pytest.mark.asyncio
    async def test_invalid_cursor_raises(self):
        client = self._make_client()
        discovery_result = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "orders",
                            "supportedSyncModes": ["incremental"],
                            "propertyFields": [["id"], ["updated_at"]],
                        }
                    }
                ]
            }
        }
        with patch.object(
            client, "discover_source_schema", new=AsyncMock(return_value=discovery_result)
        ):
            with pytest.raises(ValueError, match="Invalid cursor field"):
                await client.build_configured_catalog(
                    source_id="src1",
                    selected_streams=[
                        {
                            "name": "orders",
                            "syncMode": "incremental",
                            "destinationSyncMode": "append",
                            "cursorField": "nonexistent",
                        }
                    ],
                )

    @pytest.mark.asyncio
    async def test_pk_propagation(self):
        client = self._make_client()
        discovery_result = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "users",
                            "supportedSyncModes": ["full_refresh"],
                            "propertyFields": [["id"], ["name"]],
                        }
                    }
                ]
            }
        }
        with patch.object(
            client, "discover_source_schema", new=AsyncMock(return_value=discovery_result)
        ):
            result = await client.build_configured_catalog(
                source_id="src1",
                selected_streams=[
                    {
                        "name": "users",
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "overwrite",
                        "primaryKey": [["id"]],
                    }
                ],
            )
            assert result["streams"][0]["config"]["primaryKey"] == [["id"]]


# ============================================================
# Additional AirbyteClient Tests
# ============================================================


class TestAirbyteClientAdditional:
    """Additional tests for untested AirbyteClient methods."""

    def _http_response(self, data: Any, status_code: int = 200) -> Mock:
        resp = Mock()
        resp.status_code = status_code
        resp.content = b"{}" if status_code != 204 else b""
        resp.json.return_value = data
        resp.raise_for_status = Mock()
        resp.headers = {"Content-Type": "application/json"}
        return resp

    @pytest.fixture
    def client(self):
        return AirbyteClient(url="http://localhost:8000", workspace_id="ws1")

    # --- Destination CRUD ---

    @pytest.mark.asyncio
    async def test_update_destination(self, client):
        get_resp = self._http_response({"name": "Old", "configuration": {"host": "old"}})
        patch_resp = self._http_response({"destinationId": "dst1", "name": "New"})
        with patch.object(
            client.client, "request", new=AsyncMock(side_effect=[get_resp, patch_resp])
        ):
            result = await client.update_destination(
                "dst1", name="New", connection_configuration={"host": "new"}
            )
            assert result["destinationId"] == "dst1"
            assert result["name"] == "New"

    @pytest.mark.asyncio
    async def test_delete_destination(self, client):
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(return_value=self._http_response({}, status_code=204)),
        ):
            result = await client.delete_destination("dst1")
            assert result is True

    # --- Health ---

    @pytest.mark.asyncio
    async def test_get_health_success(self, client):
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(return_value=self._http_response({"text": "ok"})),
        ):
            result = await client.get_health()
            assert result["connected"] is True

    @pytest.mark.asyncio
    async def test_get_health_failure(self, client):
        with patch.object(
            client.client, "request", new=AsyncMock(side_effect=RequestError("down"))
        ):
            result = await client.get_health()
            assert result["connected"] is False

    # --- find_definition_id_by_name ---

    @pytest.mark.asyncio
    async def test_find_definition_id_by_name_source(self, client):
        defs_resp = self._http_response({"data": [{"id": "pg-id", "name": "Postgres"}]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=defs_resp)):
            result = await client.find_definition_id_by_name("source", "postgres")
            assert result == "pg-id"

    @pytest.mark.asyncio
    async def test_find_definition_id_by_name_not_found(self, client):
        defs_resp = self._http_response({"data": [{"id": "pg-id", "name": "Postgres"}]})
        # Server returns no match, registry also returns no match
        registry_data = {"sources": [{"sourceDefinitionId": "pg-id", "name": "Postgres"}]}
        with patch.object(client.client, "request", new=AsyncMock(return_value=defs_resp)):
            with patch.object(
                client, "fetch_connector_registry", new=AsyncMock(return_value=registry_data)
            ):
                result = await client.find_definition_id_by_name("source", "nonexistent-xyz")
                assert result is None

    @pytest.mark.asyncio
    async def test_find_definition_id_by_name_destination(self, client):
        defs_resp = self._http_response({"data": [{"id": "sf-id", "name": "Snowflake"}]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=defs_resp)):
            result = await client.find_definition_id_by_name("destination", "snowflake")
            assert result == "sf-id"

    @pytest.mark.asyncio
    async def test_find_definition_id_by_name_prefers_exact_over_substring(self, client):
        """'Postgres' must pick the Postgres connector, not 'AlloyDB for PostgreSQL'."""
        defs_resp = self._http_response({"data": [
            {"id": "alloydb-id", "name": "AlloyDB for PostgreSQL"},
            {"id": "pg-id", "name": "Postgres"},
            {"id": "pg-strict-id", "name": "Postgres Strict Encrypt"},
        ]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=defs_resp)):
            assert await client.find_definition_id_by_name("source", "postgres") == "pg-id"
            assert await client.find_definition_id_by_name("source", "Postgres") == "pg-id"
            assert await client.find_definition_id_by_name("source", "alloydb") == "alloydb-id"

    @pytest.mark.asyncio
    async def test_find_definition_id_by_name_ties_break_on_list_order(self, client):
        """Two connectors with identical score AND name length → earlier in the list wins.

        Both entries have names of length 4 that substring-match 'foo' (score 1).
        UUIDs are chosen so that lex-max comparison would pick the second entry, which
        contradicts list order — if the old UUID-based tie-break were still in place,
        this assertion would fail. With explicit list-order tie-break, the first entry
        wins regardless of UUID values.
        """
        defs_resp = self._http_response({"data": [
            {"id": "aaa-first-id", "name": "foox"},
            {"id": "zzz-second-id", "name": "yfoo"},
        ]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=defs_resp)):
            assert await client.find_definition_id_by_name("source", "foo") == "aaa-first-id"

    @pytest.mark.asyncio
    async def test_find_definition_id_by_name_substring_fallback(self, client):
        """When no exact/word match exists, substring still resolves."""
        defs_resp = self._http_response({"data": [
            {"id": "alloydb-id", "name": "AlloyDB for PostgreSQL"},
            {"id": "pg-id", "name": "PostgreSQL"},
        ]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=defs_resp)):
            # 'postgres' is a substring of 'postgresql'; shortest match wins
            assert await client.find_definition_id_by_name("source", "postgres") == "pg-id"

    @pytest.mark.asyncio
    async def test_find_definition_id_by_name_registry_uses_definitionId(self, client):
        """Registry items carrying the UUID under 'definitionId' must still resolve.

        Server-installed lookup returns no match (empty list), so the code falls back
        to the OSS registry cache. Registry entries here use 'definitionId' instead of
        'sourceDefinitionId' — without including 'definitionId' in registry_id_keys,
        this would return None.
        """
        empty_installed = self._http_response({"data": []})
        registry_items = [
            {"definitionId": "def-registry-pg", "name": "Postgres"},
            {"definitionId": "def-registry-mysql", "name": "MySQL"},
        ]
        with patch.object(client.client, "request", new=AsyncMock(return_value=empty_installed)):
            with patch.object(
                client,
                "list_source_definitions_registry",
                new=AsyncMock(return_value=registry_items),
            ):
                result = await client.find_definition_id_by_name("source", "Postgres")
                assert result == "def-registry-pg"

    @pytest.mark.asyncio
    async def test_find_definition_id_by_name_ignores_whitespace_in_name(self, client):
        """Whitespace in API-returned names must not skew the length-based tie-break.

        Both entries substring-match 'foo' (score 1). Stripped lengths are both 4
        (same tier), so list-order must decide — earlier entry wins. If we used raw
        length, the whitespace-padded entry would be treated as longer and lose to
        the other, but only coincidentally; the bug would surface on patterns where
        the raw-longer entry happened to come first. Assert stripped-length parity.
        """
        defs_resp = self._http_response({"data": [
            {"id": "first", "name": "  foox  "},  # raw len 8, stripped 4
            {"id": "second", "name": "yfoo"},     # raw len 4, stripped 4
        ]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=defs_resp)):
            # Both now have stripped length 4 → list-order wins → 'first'.
            # Under the old raw-length rule, 'second' (len 4) would beat 'first' (len 8).
            assert await client.find_definition_id_by_name("source", "foo") == "first"

    # --- OAuth2 token refresh on 401 ---

    @pytest.mark.asyncio
    async def test_make_request_refreshes_token_on_401(self):
        """A 401 response triggers one token refresh + retry with the new token."""
        oauth_client = AirbyteClient(
            base_url="http://localhost:8000",
            client_id="cid",
            client_secret="csec",
            workspace_id="ws1",
        )
        oauth_client._access_token = "stale-token"

        unauthorized = Mock()
        unauthorized.status_code = 401
        unauthorized.content = b'{"message":"Unauthorized"}'
        unauthorized.headers = {"Content-Type": "application/json"}
        unauthorized.json.return_value = {"message": "Unauthorized"}
        unauthorized.raise_for_status = Mock(
            side_effect=HTTPStatusError(
                "401 Unauthorized", request=Mock(), response=unauthorized
            )
        )

        ok = Mock()
        ok.status_code = 200
        ok.content = b'{"ok":true}'
        ok.headers = {"Content-Type": "application/json"}
        ok.json.return_value = {"ok": True}
        ok.raise_for_status = Mock()

        request_mock = AsyncMock(side_effect=[unauthorized, ok])
        obtain_mock = AsyncMock(return_value="fresh-token")

        with patch.object(oauth_client, "_obtain_token", new=obtain_mock):
            client_http = oauth_client._create_http_client()
            client_http.request = request_mock
            oauth_client._client = client_http
            result = await oauth_client._make_request("GET", "/sources")

        assert result == {"ok": True}
        assert obtain_mock.await_count == 1
        assert oauth_client._access_token == "fresh-token"
        assert client_http.headers["Authorization"] == "Bearer fresh-token"
        assert request_mock.await_count == 2

    @pytest.mark.asyncio
    async def test_make_request_does_not_loop_on_persistent_401(self):
        """If the refreshed token also gets 401, we raise instead of looping forever."""
        oauth_client = AirbyteClient(
            base_url="http://localhost:8000",
            client_id="cid",
            client_secret="csec",
            workspace_id="ws1",
        )
        oauth_client._access_token = "stale-token"

        def make_401():
            r = Mock()
            r.status_code = 401
            r.content = b'{"message":"Unauthorized"}'
            r.headers = {"Content-Type": "application/json"}
            r.json.return_value = {"message": "Unauthorized"}
            r.raise_for_status = Mock(
                side_effect=HTTPStatusError(
                    "401 Unauthorized", request=Mock(), response=r
                )
            )
            return r

        request_mock = AsyncMock(side_effect=[make_401(), make_401()])
        obtain_mock = AsyncMock(return_value="still-bad-token")

        with patch.object(oauth_client, "_obtain_token", new=obtain_mock):
            client_http = oauth_client._create_http_client()
            client_http.request = request_mock
            oauth_client._client = client_http
            with pytest.raises(AirbyteAPIError):
                await oauth_client._make_request("GET", "/sources")

        # Refresh attempted exactly once, two HTTP calls total
        assert obtain_mock.await_count == 1
        assert request_mock.await_count == 2

    @pytest.mark.asyncio
    async def test_make_request_concurrent_401s_refresh_token_once(self):
        """Two coroutines that both hit 401 must coalesce into a single refresh call.

        Without the refresh lock, both would race into ``_obtain_token()`` and fire
        redundant token-endpoint requests. Under the lock + re-check, the second
        coroutine observes the already-refreshed token and skips its own refresh.
        """
        oauth_client = AirbyteClient(
            base_url="http://localhost:8000",
            client_id="cid",
            client_secret="csec",
            workspace_id="ws1",
        )
        oauth_client._access_token = "stale-token"

        def make_401():
            r = Mock()
            r.status_code = 401
            r.content = b'{"message":"Unauthorized"}'
            r.headers = {"Content-Type": "application/json"}
            r.json.return_value = {"message": "Unauthorized"}
            r.raise_for_status = Mock(
                side_effect=HTTPStatusError(
                    "401 Unauthorized", request=Mock(), response=r
                )
            )
            return r

        def make_ok(payload):
            r = Mock()
            r.status_code = 200
            r.content = b'{"ok":true}'
            r.headers = {"Content-Type": "application/json"}
            r.json.return_value = payload
            r.raise_for_status = Mock()
            return r

        # First request from each coroutine → 401; subsequent (post-refresh) → 200.
        # Tracked by endpoint URL so per-call ordering doesn't matter.
        seen_401 = {"/api/public/v1/sources": False, "/api/public/v1/destinations": False}

        async def request_side_effect(method, url, **kwargs):
            if url in seen_401 and not seen_401[url]:
                seen_401[url] = True
                return make_401()
            return make_ok({"url": url})

        request_mock = AsyncMock(side_effect=request_side_effect)

        refresh_count = {"n": 0}

        async def slow_obtain_token():
            refresh_count["n"] += 1
            # Sleep yields control, letting the second coroutine reach the lock while
            # the first is still inside _obtain_token. Tests the coalescing path.
            await asyncio.sleep(0.05)
            return f"fresh-token-{refresh_count['n']}"

        with patch.object(oauth_client, "_obtain_token", side_effect=slow_obtain_token):
            client_http = oauth_client._create_http_client()
            client_http.request = request_mock
            oauth_client._client = client_http

            results = await asyncio.gather(
                oauth_client._make_request("GET", "/sources"),
                oauth_client._make_request("GET", "/destinations"),
            )

        assert {"url": "/api/public/v1/sources"} in results
        assert {"url": "/api/public/v1/destinations"} in results
        # Only ONE refresh happened; the second 401 handler found the new token already set.
        assert refresh_count["n"] == 1
        # 4 HTTP calls total (2 × 401 + 2 × 200).
        assert request_mock.await_count == 4

    @pytest.mark.asyncio
    async def test_make_request_refreshes_token_with_retry_attempts_one(self):
        """401 refresh must retry in-place — not consume the only retry slot.

        With retry_attempts=1 and a 401 on the single attempt, the pre-fix code
        would refresh, `continue`, exit the loop, and mis-raise AirbyteConnectionError.
        The in-place retry must succeed and return the post-refresh response.
        """
        oauth_client = AirbyteClient(
            base_url="http://localhost:8000",
            client_id="cid",
            client_secret="csec",
            workspace_id="ws1",
            retry_attempts=1,
        )
        oauth_client._access_token = "stale-token"

        unauthorized = Mock()
        unauthorized.status_code = 401
        unauthorized.content = b'{"message":"Unauthorized"}'
        unauthorized.headers = {"Content-Type": "application/json"}
        unauthorized.json.return_value = {"message": "Unauthorized"}
        unauthorized.raise_for_status = Mock(
            side_effect=HTTPStatusError(
                "401 Unauthorized", request=Mock(), response=unauthorized
            )
        )

        ok = Mock()
        ok.status_code = 200
        ok.content = b'{"ok":true}'
        ok.headers = {"Content-Type": "application/json"}
        ok.json.return_value = {"ok": True}
        ok.raise_for_status = Mock()

        request_mock = AsyncMock(side_effect=[unauthorized, ok])
        obtain_mock = AsyncMock(return_value="fresh-token")

        with patch.object(oauth_client, "_obtain_token", new=obtain_mock):
            client_http = oauth_client._create_http_client()
            client_http.request = request_mock
            oauth_client._client = client_http
            result = await oauth_client._make_request("GET", "/sources")

        assert result == {"ok": True}
        assert obtain_mock.await_count == 1
        assert request_mock.await_count == 2

    # --- get_source_by_name / get_destination_by_name ---

    @pytest.mark.asyncio
    async def test_get_source_by_name_found(self, client):
        list_resp = self._http_response({"data": [{"sourceId": "s1", "name": "MyPG"}]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=list_resp)):
            result = await client.get_source_by_name("MyPG")
            assert result["sourceId"] == "s1"

    @pytest.mark.asyncio
    async def test_get_source_by_name_not_found(self, client):
        list_resp = self._http_response({"data": [{"sourceId": "s1", "name": "MyPG"}]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=list_resp)):
            result = await client.get_source_by_name("NotExist")
            assert result is None

    @pytest.mark.asyncio
    async def test_get_destination_by_name_found(self, client):
        list_resp = self._http_response({"data": [{"destinationId": "d1", "name": "MySF"}]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=list_resp)):
            result = await client.get_destination_by_name("MySF")
            assert result["destinationId"] == "d1"

    @pytest.mark.asyncio
    async def test_get_destination_by_name_not_found(self, client):
        list_resp = self._http_response({"data": [{"destinationId": "d1", "name": "MySF"}]})
        with patch.object(client.client, "request", new=AsyncMock(return_value=list_resp)):
            result = await client.get_destination_by_name("Missing")
            assert result is None

    # --- get_job_logs ---

    @pytest.mark.asyncio
    async def test_get_job_logs(self, client):
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(return_value=self._http_response({"logLines": ["line1", "line2"]})),
        ):
            result = await client.get_job_logs(123)
            assert result["logLines"] == ["line1", "line2"]

    # --- wait_for_job ---

    @pytest.mark.asyncio
    async def test_wait_for_job_succeeded(self, client):
        with patch.object(
            client,
            "get_job_status",
            new=AsyncMock(return_value={"status": "succeeded", "jobId": 1}),
        ):
            result = await client.wait_for_job(1, poll_interval=0)
            assert result["status"] == "succeeded"

    @pytest.mark.asyncio
    async def test_wait_for_job_failed(self, client):
        with patch.object(
            client, "get_job_status", new=AsyncMock(return_value={"status": "failed", "jobId": 1})
        ):
            with pytest.raises(AirbyteSyncError, match="failed"):
                await client.wait_for_job(1, poll_interval=0)

    @pytest.mark.asyncio
    async def test_wait_for_job_timeout(self, client):
        with patch.object(
            client, "get_job_status", new=AsyncMock(return_value={"status": "running"})
        ):
            with pytest.raises(AirbyteSyncError, match="timed out"):
                await client.wait_for_job(1, timeout=0, poll_interval=0)

    @pytest.mark.asyncio
    async def test_wait_for_job_cancelled(self, client):
        with patch.object(
            client,
            "get_job_status",
            new=AsyncMock(return_value={"status": "cancelled", "jobId": 1}),
        ):
            with pytest.raises(AirbyteSyncError, match="cancelled"):
                await client.wait_for_job(1, poll_interval=0)

    # --- get_source_id / get_destination_id / get_connection_id ---

    @pytest.mark.asyncio
    async def test_get_source_id(self, client):
        ws_resp = self._http_response({"data": [{"workspaceId": "ws1"}]})
        src_resp = self._http_response({"data": [{"sourceId": "s1", "name": "MyPG"}]})
        with patch.object(client.client, "request", new=AsyncMock(side_effect=[ws_resp, src_resp])):
            result = await client.get_source_id("MyPG")
            assert result == "s1"

    @pytest.mark.asyncio
    async def test_get_source_id_not_found(self, client):
        ws_resp = self._http_response({"data": [{"workspaceId": "ws1"}]})
        src_resp = self._http_response({"data": []})
        with patch.object(client.client, "request", new=AsyncMock(side_effect=[ws_resp, src_resp])):
            result = await client.get_source_id("Unknown")
            assert result is None

    @pytest.mark.asyncio
    async def test_get_destination_id(self, client):
        ws_resp = self._http_response({"data": [{"workspaceId": "ws1"}]})
        dst_resp = self._http_response({"data": [{"destinationId": "d1", "name": "MySF"}]})
        with patch.object(client.client, "request", new=AsyncMock(side_effect=[ws_resp, dst_resp])):
            result = await client.get_destination_id("MySF")
            assert result == "d1"

    @pytest.mark.asyncio
    async def test_get_connection_id(self, client):
        ws_resp = self._http_response({"data": [{"workspaceId": "ws1"}]})
        conn_resp = self._http_response({"data": [{"connectionId": "c1", "name": "MyConn"}]})
        with patch.object(
            client.client, "request", new=AsyncMock(side_effect=[ws_resp, conn_resp])
        ):
            result = await client.get_connection_id("MyConn")
            assert result == "c1"

    # --- create_workspace ---

    @pytest.mark.asyncio
    async def test_create_workspace(self, client):
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response({"workspaceId": "ws-new", "name": "TestWS"})
            ),
        ):
            result = await client.create_workspace("TestWS")
            assert result["workspaceId"] == "ws-new"

    # --- discover_schema alias ---

    @pytest.mark.asyncio
    async def test_discover_schema_alias(self, client):
        with patch.object(
            client,
            "discover_source_schema",
            new=AsyncMock(return_value={"catalog": {"streams": []}}),
        ) as mock_dss:
            result = await client.discover_schema("src1")
            mock_dss.assert_called_once_with("src1")
            assert result == {"catalog": {"streams": []}}

    # --- find_source_by_config / find_destination_by_config ---

    @pytest.mark.asyncio
    async def test_find_source_by_config_found(self, client):
        with patch.object(
            client,
            "list_sources",
            new=AsyncMock(
                return_value=[
                    {
                        "sourceId": "s1",
                        "name": "MyPG",
                        "configuration": {"host": "localhost", "port": 5432},
                    }
                ]
            ),
        ):
            result = await client.find_source_by_config({"host": "localhost", "port": 5432})
            assert result["sourceId"] == "s1"

    @pytest.mark.asyncio
    async def test_find_source_by_config_not_found(self, client):
        with patch.object(
            client,
            "list_sources",
            new=AsyncMock(
                return_value=[
                    {"sourceId": "s1", "name": "MyPG", "configuration": {"host": "other"}}
                ]
            ),
        ):
            result = await client.find_source_by_config({"host": "localhost"})
            assert result is None

    @pytest.mark.asyncio
    async def test_find_destination_by_config_found(self, client):
        with patch.object(
            client,
            "list_destinations",
            new=AsyncMock(
                return_value=[
                    {"destinationId": "d1", "name": "MySF", "configuration": {"host": "sf.com"}}
                ]
            ),
        ):
            result = await client.find_destination_by_config({"host": "sf.com"})
            assert result["destinationId"] == "d1"

    @pytest.mark.asyncio
    async def test_find_source_by_config_name_filter(self, client):
        with patch.object(
            client,
            "list_sources",
            new=AsyncMock(
                return_value=[
                    {"sourceId": "s1", "name": "MyPG", "configuration": {"host": "localhost"}},
                    {"sourceId": "s2", "name": "Other", "configuration": {"host": "localhost"}},
                ]
            ),
        ):
            result = await client.find_source_by_config({"host": "localhost"}, name="MyPG")
            assert result["sourceId"] == "s1"

    # --- create_source_if_not_exists / create_destination_if_not_exists ---

    @pytest.mark.asyncio
    async def test_create_source_if_not_exists_reuses(self, client):
        existing = {"sourceId": "s1", "name": "MyPG", "configuration": {"host": "h"}}
        with patch.object(client, "find_source_by_config", new=AsyncMock(return_value=existing)):
            result = await client.create_source_if_not_exists(
                "ws1", "pg-def", "MyPG", {"host": "h"}
            )
            assert result["sourceId"] == "s1"

    @pytest.mark.asyncio
    async def test_create_source_if_not_exists_creates(self, client):
        created = {"sourceId": "new-s", "name": "MyPG"}
        with patch.object(client, "find_source_by_config", new=AsyncMock(return_value=None)):
            with patch.object(client, "create_source", new=AsyncMock(return_value=created)):
                result = await client.create_source_if_not_exists(
                    "ws1", "pg-def", "MyPG", {"host": "h"}
                )
                assert result["sourceId"] == "new-s"

    @pytest.mark.asyncio
    async def test_create_destination_if_not_exists_reuses(self, client):
        existing = {"destinationId": "d1", "name": "MySF", "configuration": {"host": "sf"}}
        with patch.object(
            client, "find_destination_by_config", new=AsyncMock(return_value=existing)
        ):
            result = await client.create_destination_if_not_exists(
                "ws1", "sf-def", "MySF", {"host": "sf"}
            )
            assert result["destinationId"] == "d1"

    @pytest.mark.asyncio
    async def test_create_destination_if_not_exists_creates(self, client):
        created = {"destinationId": "new-d", "name": "MySF"}
        with patch.object(client, "find_destination_by_config", new=AsyncMock(return_value=None)):
            with patch.object(client, "create_destination", new=AsyncMock(return_value=created)):
                result = await client.create_destination_if_not_exists(
                    "ws1", "sf-def", "MySF", {"host": "sf"}
                )
                assert result["destinationId"] == "new-d"

    # --- close / context manager ---

    @pytest.mark.asyncio
    async def test_close(self, client):
        mock_inner = AsyncMock()
        client._client = mock_inner
        await client.close()
        mock_inner.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, client):
        async with client as c:
            assert c is client

    # --- _get_workspace_id ---

    @pytest.mark.asyncio
    async def test_get_workspace_id_explicit(self, client):
        result = await client._get_workspace_id()
        assert result == "ws1"

    @pytest.mark.asyncio
    async def test_get_workspace_id_auto_resolve(self):
        client = AirbyteClient(url="http://localhost:8000")
        ws_resp = Mock()
        ws_resp.status_code = 200
        ws_resp.content = b"{}"
        ws_resp.json.return_value = {"data": [{"workspaceId": "auto-ws"}]}
        ws_resp.raise_for_status = Mock()
        ws_resp.headers = {"Content-Type": "application/json"}
        with patch.object(client.client, "request", new=AsyncMock(return_value=ws_resp)):
            result = await client._get_workspace_id()
            assert result == "auto-ws"

    # --- create_connection with streams ---

    @pytest.mark.asyncio
    async def test_create_connection_with_streams(self, client):
        with patch.object(
            client.client,
            "request",
            new=AsyncMock(
                return_value=self._http_response({"connectionId": "c1", "status": "active"})
            ),
        ):
            result = await client.create_connection(
                source_id="src1",
                destination_id="dst1",
                name="Test",
                streams=[
                    {
                        "name": "users",
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "overwrite",
                        "selected": True,
                    }
                ],
            )
            assert result["connectionId"] == "c1"

    # --- Registry methods ---

    @pytest.mark.asyncio
    async def test_list_source_definitions_registry(self, client):
        reg = {"sources": [{"sourceDefinitionId": "pg", "name": "Postgres"}], "destinations": []}
        with patch.object(client, "fetch_connector_registry", new=AsyncMock(return_value=reg)):
            result = await client.list_source_definitions_registry()
            assert len(result) == 1
            assert result[0]["name"] == "Postgres"

    @pytest.mark.asyncio
    async def test_list_destination_definitions_registry(self, client):
        reg = {
            "sources": [],
            "destinations": [{"destinationDefinitionId": "sf", "name": "Snowflake"}],
        }
        with patch.object(client, "fetch_connector_registry", new=AsyncMock(return_value=reg)):
            result = await client.list_destination_definitions_registry()
            assert len(result) == 1
            assert result[0]["name"] == "Snowflake"

    # --- fetch_connector_registry ---

    @pytest.mark.asyncio
    async def test_fetch_connector_registry_cache(self, client):
        client._registry_cache = {"sources": [], "destinations": []}
        result = await client.fetch_connector_registry()
        assert result == {"sources": [], "destinations": []}

    @pytest.mark.asyncio
    async def test_fetch_connector_registry_fresh(self, client):
        reg_data = {"sources": [{"name": "PG"}], "destinations": []}
        resp = Mock()
        resp.json.return_value = reg_data
        resp.raise_for_status = Mock()
        with patch.object(client.client, "get", new=AsyncMock(return_value=resp)):
            result = await client.fetch_connector_registry(force_refresh=True)
            assert result["sources"][0]["name"] == "PG"

    # --- get_connection_sync_history with statistics ---

    @pytest.mark.asyncio
    async def test_get_connection_sync_history_stats(self, client):
        with patch.object(
            client,
            "list_jobs",
            new=AsyncMock(
                return_value=[
                    {"status": "succeeded", "createdAt": "2025-12-01"},
                    {"status": "failed", "createdAt": "2025-11-01"},
                ]
            ),
        ):
            result = await client.get_connection_sync_history("conn1")
            assert result["total_syncs"] == 2
            assert result["statistics"]["succeeded"] == 1
            assert result["statistics"]["failed"] == 1
            assert result["success_rate"] == 50.0

    # --- wait_for_job_completion alias ---

    @pytest.mark.asyncio
    async def test_wait_for_job_completion_alias(self, client):
        with patch.object(
            client, "wait_for_job", new=AsyncMock(return_value={"status": "succeeded"})
        ) as mock_wfj:
            result = await client.wait_for_job_completion(1, timeout=300, poll_interval=5)
            mock_wfj.assert_called_once_with(1, 300, 5, 3)
            assert result["status"] == "succeeded"

    # --- get_job_info alias ---

    @pytest.mark.asyncio
    async def test_get_job_info_alias(self, client):
        with patch.object(
            client, "get_job_status", new=AsyncMock(return_value={"jobId": 1})
        ) as mock_gjs:
            result = await client.get_job_info(1)
            mock_gjs.assert_called_once_with(1)
            assert result["jobId"] == 1


# ============================================================
# Data Movement Helper Tests
# ============================================================


class TestMaskSensitiveData:
    """Tests for _mask_sensitive_data."""

    def test_masks_password(self):
        result = _mask_sensitive_data({"host": "h", "password": "secret123"})
        assert result["host"] == "h"
        assert result["password"] == "***MASKED***"

    def test_masks_nested(self):
        result = _mask_sensitive_data({"conn": {"api_key": "abc", "host": "h"}})
        assert result["conn"]["api_key"] == "***MASKED***"
        assert result["conn"]["host"] == "h"

    def test_masks_in_list(self):
        result = _mask_sensitive_data({"items": [{"token": "t"}, {"name": "n"}]})
        assert result["items"][0]["token"] == "***MASKED***"
        assert result["items"][1]["name"] == "n"

    def test_non_dict_passthrough(self):
        assert _mask_sensitive_data("string") == "string"

    def test_masks_secret_key(self):
        result = _mask_sensitive_data({"client_secret": "x", "name": "n"})
        assert result["client_secret"] == "***MASKED***"


class TestGetConnectorSpec:
    """Tests for _get_connector_spec."""

    @pytest.mark.asyncio
    async def test_returns_none_no_definition_id(self):
        result = await _get_connector_spec(Mock(), "source", None)
        assert result == (None, None)

    @pytest.mark.asyncio
    async def test_returns_spec_for_source(self):
        orch = Mock()
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {
                "sourceDefinitionId": "pg-id",
                "name": "Postgres",
                "spec": {"connectionSpecification": {"type": "object", "properties": {}}},
            }
        ]
        result = await _get_connector_spec(orch, "source", "pg-id")
        name, spec = result
        assert name == "Postgres"
        assert spec == {"type": "object", "properties": {}}

    @pytest.mark.asyncio
    async def test_returns_none_not_found(self):
        orch = Mock()
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "other-id", "spec": {"connectionSpecification": {}}}
        ]
        result = await _get_connector_spec(orch, "source", "pg-id")
        assert result == (None, None)

    @pytest.mark.asyncio
    async def test_returns_spec_for_destination(self):
        orch = Mock()
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {
                "destinationDefinitionId": "sf-id",
                "name": "Snowflake",
                "spec": {"connectionSpecification": {"type": "object"}},
            }
        ]
        result = await _get_connector_spec(orch, "destination", "sf-id")
        name, spec = result
        assert name == "Snowflake"
        assert spec == {"type": "object"}

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        orch = Mock()
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.list_source_definitions_registry.side_effect = Exception("fail")
        result = await _get_connector_spec(orch, "source", "pg-id")
        assert result == (None, None)


class TestBaselineNormalizeConfig:
    """Tests for _baseline_normalize_config."""

    def test_lowercase_keys(self):
        result = _baseline_normalize_config({"Host": "h", "Port": "5432"})
        assert "host" in result
        assert result["port"] == 5432

    def test_removes_secrets(self):
        result = _baseline_normalize_config({"host": "h", "password": "s", "token": "t"})
        assert "host" in result
        assert "password" not in result
        assert "token" not in result

    def test_coerces_numeric_strings(self):
        result = _baseline_normalize_config({"port": "5432"})
        assert result["port"] == 5432

    def test_alias_map(self):
        result = _baseline_normalize_config({"dbname": "mydb"})
        assert result["database"] == "mydb"

    def test_host_strips_protocol(self):
        result = _baseline_normalize_config({"host": "https://example.com"})
        assert result["host"] == "example.com"

    def test_ssl_mode_string_to_dict(self):
        result = _baseline_normalize_config({"ssl_mode": "require"})
        assert result["ssl_mode"] == {"mode": "require"}

    def test_non_dict_passthrough(self):
        assert _baseline_normalize_config("string") == "string"


class TestCoerceToType:
    """Tests for _coerce_to_type."""

    def test_string_to_integer(self):
        assert _coerce_to_type("42", "integer") == 42

    def test_string_to_number(self):
        assert _coerce_to_type("3.14", "number") == 3.14

    def test_string_to_boolean_true(self):
        assert _coerce_to_type("true", "boolean") is True

    def test_string_to_boolean_false(self):
        assert _coerce_to_type("false", "boolean") is False

    def test_int_to_string(self):
        assert _coerce_to_type(42, "string") == "42"

    def test_none_passthrough(self):
        assert _coerce_to_type(None, "integer") is None

    def test_list_type(self):
        assert _coerce_to_type("5", ["integer", "null"]) == 5


class TestNormalizeWithSpec:
    """Tests for _normalize_with_spec."""

    def test_none_schema_uses_baseline(self):
        result = _normalize_with_spec({"Host": "h", "password": "s"}, None)
        assert "host" in result
        assert "password" not in result

    def test_coerces_primitive_types(self):
        schema = {"type": "integer"}
        assert _normalize_with_spec("42", schema) == 42

    def test_object_with_properties(self):
        schema = {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "integer"},
            },
        }
        result = _normalize_with_spec({"host": "h", "port": "5432"}, schema)
        assert result["host"] == "h"
        assert result["port"] == 5432

    def test_skips_airbyte_secret_fields(self):
        schema = {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "password": {"type": "string", "airbyte_secret": True},
            },
        }
        result = _normalize_with_spec({"host": "h", "password": "s"}, schema)
        assert "host" in result
        assert "password" not in result

    def test_array_normalization(self):
        schema = {"type": "array", "items": {"type": "integer"}}
        result = _normalize_with_spec(["1", "2", "3"], schema)
        assert result == [1, 2, 3]


class TestShapeConfigToSpec:
    """Tests for _shape_config_to_spec."""

    def test_no_schema_returns_copy(self):
        cfg = {"host": "h"}
        result = _shape_config_to_spec(cfg, None)
        assert result == {"host": "h"}
        assert result is not cfg

    def test_basic_shaping(self):
        schema = {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "integer"},
            },
        }
        result = _shape_config_to_spec({"Host": "h", "Port": "5432"}, schema)
        assert result["host"] == "h"
        assert result["port"] == 5432

    def test_non_dict_config(self):
        result = _shape_config_to_spec(None, {"type": "object"})
        assert result == {}


class TestConfigsEquivalent:
    """Tests for _configs_equivalent."""

    def test_equal_configs(self):
        assert _configs_equivalent({"host": "h", "port": 5432}, {"host": "h", "port": 5432}) is True

    def test_different_configs(self):
        assert _configs_equivalent({"host": "h"}, {"host": "other"}) is False

    def test_with_spec(self):
        spec = {
            "type": "object",
            "properties": {"host": {"type": "string"}, "port": {"type": "integer"}},
        }
        assert (
            _configs_equivalent({"host": "h", "port": "5432"}, {"host": "h", "port": 5432}, spec)
            is True
        )

    def test_exception_returns_false(self):
        # Trigger an exception in normalization — pass non-iterable to baseline
        assert _configs_equivalent(None, {"host": "h"}) is False


# ============================================================
# Data Movement Tool Function Tests
# ============================================================


class TestDataMovementTools:
    """Tests for nested tool functions inside register_data_movement_tools."""

    def _make_orchestrator(self):
        """Build a mock orchestrator."""
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    # --- trigger_airbyte_sync ---

    @pytest.mark.asyncio
    async def test_trigger_airbyte_sync_success(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.trigger_sync.return_value = {
            "jobId": 42,
            "status": "pending",
            "createdAt": "2025-01-01",
        }
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="trigger", connection_id="c1")
        assert result["success"] is True
        assert result["job_id"] == 42

    @pytest.mark.asyncio
    async def test_trigger_airbyte_sync_error(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.trigger_sync.side_effect = Exception("API error")
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="trigger", connection_id="c1")
        assert result["success"] is False
        assert "API error" in result["error"]

    # --- get_sync_status ---

    @pytest.mark.asyncio
    async def test_get_sync_status_basic(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_job_status.return_value = {
            "jobId": 42,
            "status": "succeeded",
            "startTime": "t1",
            "lastUpdatedAt": "t2",
            "bytesSynced": 100,
            "rowsSynced": 10,
        }
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="get_status", job_id=42)
        assert result["job_id"] == 42
        assert result["status"] == "succeeded"
        assert result["bytes_synced"] == 100

    @pytest.mark.asyncio
    async def test_get_sync_status_with_logs(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_job_status.return_value = {
            "jobId": 42,
            "status": "running",
            "startTime": "t1",
            "lastUpdatedAt": "t2",
        }
        orch.airbyte_client.get_job_logs.return_value = {"logLines": ["log1", "log2"]}
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="get_status", job_id=42, include_logs=True)
        assert result["logs"] == ["log1", "log2"]
        assert result["log_count"] == 2

    @pytest.mark.asyncio
    async def test_get_sync_status_logs_404_shows_api_warning(self):
        """When get_job_logs returns 404, warning references the unsupported API endpoint."""
        orch = self._make_orchestrator()
        orch.airbyte_client.get_job_status.return_value = {
            "jobId": 42,
            "status": "succeeded",
            "startTime": "t1",
            "lastUpdatedAt": "t2",
            "bytesSynced": 1024,
            "rowsSynced": 50,
        }
        orch.airbyte_client.get_job_logs.side_effect = AirbyteAPIError(
            "Airbyte API error (404): Not Found"
        )
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="get_status", job_id=42, include_logs=True)
        # Status fields are preserved
        assert result["job_id"] == 42
        assert result["status"] == "succeeded"
        assert result["bytes_synced"] == 1024
        assert result["records_synced"] == 50
        # Logs gracefully degraded with API-specific message
        assert result["logs"] == []
        assert result["log_count"] == 0
        assert "Public API v1" in result["logs_warning"]

    @pytest.mark.asyncio
    async def test_get_sync_status_logs_405_shows_api_warning(self):
        """When get_job_logs returns 405, warning references the unsupported API endpoint."""
        orch = self._make_orchestrator()
        orch.airbyte_client.get_job_status.return_value = {
            "jobId": 42,
            "status": "succeeded",
            "startTime": "t1",
            "lastUpdatedAt": "t2",
        }
        orch.airbyte_client.get_job_logs.side_effect = AirbyteAPIError(
            "Airbyte API error (405): Method Not Allowed"
        )
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="get_status", job_id=42, include_logs=True)
        assert result["job_id"] == 42
        assert result["status"] == "succeeded"
        assert result["logs"] == []
        assert result["log_count"] == 0
        assert "Public API v1" in result["logs_warning"]

    @pytest.mark.asyncio
    async def test_get_sync_status_logs_non404_shows_error_reason(self):
        """When get_job_logs fails for non-404 reasons, warning includes the error reason."""
        orch = self._make_orchestrator()
        orch.airbyte_client.get_job_status.return_value = {
            "jobId": 42,
            "status": "succeeded",
            "startTime": "t1",
            "lastUpdatedAt": "t2",
        }
        orch.airbyte_client.get_job_logs.side_effect = AirbyteConnectionError(
            "Connection refused"
        )
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="get_status", job_id=42, include_logs=True)
        # Status fields are preserved
        assert result["job_id"] == 42
        assert result["status"] == "succeeded"
        # Warning includes the actual error, not the Public API v1 message
        assert result["logs"] == []
        assert result["log_count"] == 0
        assert "Public API v1" not in result["logs_warning"]
        assert "Could not fetch job logs" in result["logs_warning"]

    @pytest.mark.asyncio
    async def test_get_sync_status_error(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_job_status.side_effect = Exception("not found")
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="get_status", job_id=999)
        assert result["success"] is False

    # --- list_connectors ---

    @pytest.mark.asyncio
    async def test_list_connectors_both(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.list_source_definitions.return_value = [{"name": "Postgres"}]
        orch.airbyte_client.list_destination_definitions.return_value = [{"name": "Snowflake"}]
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="connectors", connector_type="both")
        assert result["success"] is True
        assert result["source_count"] == 1
        assert result["destination_count"] == 1

    @pytest.mark.asyncio
    async def test_list_connectors_with_search(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.list_source_definitions.return_value = [
            {"name": "Postgres"},
            {"name": "MySQL"},
        ]
        orch.airbyte_client.list_destination_definitions.return_value = []
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="connectors", connector_type="source", search_term="post"
        )
        assert result["source_count"] == 1
        assert result["sources"][0]["name"] == "Postgres"

    @pytest.mark.asyncio
    async def test_list_connectors_error(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.list_source_definitions.side_effect = Exception("fail")
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="connectors", connector_type="source")
        assert result["success"] is False

    # --- list_airbyte_connections ---

    @pytest.mark.asyncio
    async def test_list_airbyte_connections(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.list_connections.return_value = [
            {
                "connectionId": "c1",
                "name": "Conn1",
                "sourceId": "s1",
                "destinationId": "d1",
                "status": "active",
            }
        ]
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="connections")
        assert result["success"] is True
        assert result["connection_count"] == 1

    # --- list_airbyte_sources ---

    @pytest.mark.asyncio
    async def test_list_airbyte_sources(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.list_sources.return_value = [{"sourceId": "s1", "name": "PG"}]
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="sources")
        assert result["success"] is True
        assert result["source_count"] == 1

    # --- list_airbyte_destinations ---

    @pytest.mark.asyncio
    async def test_list_airbyte_destinations(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.list_destinations.return_value = [{"destinationId": "d1", "name": "SF"}]
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="destinations")
        assert result["success"] is True
        assert result["destination_count"] == 1

    # --- list_streams ---

    @pytest.mark.asyncio
    async def test_list_streams(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "users",
                            "supportedSyncModes": ["full_refresh"],
                            "sourceDefinedCursor": False,
                            "defaultCursorField": [],
                            "namespace": None,
                        }
                    }
                ]
            }
        }
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="streams", source_id="src1")
        assert result["success"] is True
        assert result["stream_count"] == 1
        assert result["streams"][0]["name"] == "users"

    # --- update_airbyte_connection ---

    @pytest.mark.asyncio
    async def test_update_connection_manual_schedule(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.return_value = {"connectionId": "c1", "status": "active"}
        orch.airbyte_client.update_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="update", connection_id="c1", schedule_type="manual"
        )
        assert result["success"] is True
        assert "schedule" in result["updated_fields"]

    @pytest.mark.asyncio
    async def test_update_connection_cron_schedule(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.return_value = {"connectionId": "c1"}
        orch.airbyte_client.update_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="update", connection_id="c1", schedule_type="cron", schedule_cron="0 0 * * *"
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_update_connection_cron_missing_expression(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.return_value = {"connectionId": "c1"}
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="update", connection_id="c1", schedule_type="cron"
        )
        assert result["success"] is False
        assert "schedule_cron is required" in result["error"]

    @pytest.mark.asyncio
    async def test_update_connection_no_fields(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.return_value = {"connectionId": "c1"}
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](action="update", connection_id="c1")
        assert result["success"] is False
        assert "No update fields" in result["error"]

    @pytest.mark.asyncio
    async def test_update_connection_unsupported_schedule(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.return_value = {"connectionId": "c1"}
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="update", connection_id="c1", schedule_type="weekly"
        )
        assert result["success"] is False

    # --- create_intelligent_airbyte_pipeline (sync mode clarification) ---

    # --- create_intelligent_airbyte_pipeline ---

    @pytest.mark.asyncio
    async def test_intelligent_pipeline_missing_sync(self):
        orch = self._make_orchestrator()
        orch.credential_resolver = Mock()
        orch.credential_resolver.guard_configured.return_value = None
        orch.credential_resolver.resolve_profile.return_value = {"host": "h"}
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="snowflake",
            destination_profile="test_dest",
            streams=[{"name": "t"}],
            connection_name="Test",
        )
        assert result["success"] is False
        assert result["action_required"] == "clarify_sync_configuration"

    # --- wait_for_sync_completion ---

    @pytest.mark.asyncio
    async def test_wait_for_sync_completion(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.wait_for_job.return_value = {
            "job": {"status": "succeeded", "createdAt": "t1", "startedAt": "t2", "updatedAt": "t3"},
            "attempts": [{"totalStats": {"bytesEmitted": 100, "recordsEmitted": 10}}],
        }
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="wait", job_id=42)
        assert result["success"] is True
        assert result["status"] == "succeeded"

    @pytest.mark.asyncio
    async def test_wait_for_sync_completion_error(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.wait_for_job.side_effect = Exception("timeout")
        tools = self._register(orch)
        result = await tools["airbyte_sync"](action="wait", job_id=42)
        assert result["success"] is False

    # --- create_airbyte_source / create_airbyte_destination (thin wrappers) ---

    @pytest.mark.asyncio
    async def test_create_airbyte_source_wrapper(self):
        orch = self._make_orchestrator()
        orch.credential_resolver = Mock()
        orch.credential_resolver.guard_configured.return_value = None
        orch.credential_resolver.resolve_profile.return_value = {"host": "h"}
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def", "name": "Postgres"}
        ]
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "new-s", "name": "PG"}
        tools = self._register(orch)
        result = await tools["airbyte_manage"](
            action="create_source",
            name="PG",
            source_definition_id="pg-def",
            source_profile="test_source",
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_create_airbyte_destination_wrapper(self):
        orch = self._make_orchestrator()
        orch.credential_resolver = Mock()
        orch.credential_resolver.guard_configured.return_value = None
        orch.credential_resolver.resolve_profile.return_value = {"host": "sf"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def", "name": "Snowflake"}
        ]
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {
            "destinationId": "new-d",
            "name": "SF",
        }
        tools = self._register(orch)
        result = await tools["airbyte_manage"](
            action="create_destination",
            name="SF",
            destination_definition_id="sf-def",
            destination_profile="test_dest",
        )
        assert result["success"] is True

    # --- get_airbyte_connection_details ---

    @pytest.mark.asyncio
    async def test_get_airbyte_connection_details(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.return_value = {
            "connectionId": "c1",
            "name": "Test",
            "sourceId": "s1",
            "destinationId": "d1",
            "status": "active",
            "scheduleType": "manual",
            "configurations": {"streams": [{"name": "t", "syncMode": "full_refresh_overwrite"}]},
        }
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="connection_details", connection_id="c1"
        )
        assert result["success"] is True
        assert result["connection"]["connectionId"] == "c1"


# ============================================================
# Additional Data Movement Tool Tests (Internal Functions)
# ============================================================


class TestSelectStreamsFromIntent:
    """Tests for select_streams_from_intent (exercises _build_stream_index,
    _intent_keywords, _score_stream, _is_restricted, _choose_sync_mode)."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _discovery_result(self, streams):
        """Build a discover_source_schema return value."""
        catalog_streams = []
        for s in streams:
            catalog_streams.append(
                {
                    "stream": {
                        "name": s["name"],
                        "namespace": s.get("namespace", ""),
                        "supportedSyncModes": s.get("supportedSyncModes", ["full_refresh"]),
                        "json_schema": {
                            "properties": {c: {"type": "string"} for c in s.get("columns", [])},
                        },
                        "description": s.get("description", ""),
                    },
                    "config": {
                        "supported_sync_modes": s.get("supportedSyncModes", ["full_refresh"]),
                    },
                }
            )
        return {"catalog": {"streams": catalog_streams}}

    @pytest.mark.asyncio
    async def test_basic_keyword_matching(self):
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {"name": "customers", "columns": ["id", "name"]},
                {"name": "orders", "columns": ["id", "customer_id"]},
                {"name": "logs", "columns": ["id", "message"]},
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams", source_id="s1", prompt="sync customer data"
        )
        assert result["success"] is True
        selected_names = [s["name"] for s in result["selected_streams"]]
        assert "customers" in selected_names

    @pytest.mark.asyncio
    async def test_synonym_expansion(self):
        """'customer' expands to 'clients', 'accounts', 'users' etc."""
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {"name": "clients", "columns": ["id"]},
                {"name": "unrelated", "columns": ["id"]},
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams", source_id="s1", prompt="sync customer info"
        )
        assert result["success"] is True
        selected_names = [s["name"] for s in result["selected_streams"]]
        assert "clients" in selected_names

    @pytest.mark.asyncio
    async def test_no_match_returns_error(self):
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {"name": "widgets", "columns": ["id"]},
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams", source_id="s1", prompt="sync customer data"
        )
        # widgets doesn't match customer; but "customer" synonym "client" could match.
        # Actually widgets has no overlap — should fail.
        assert result["success"] is False
        assert "No streams selected" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_pii_restriction_low_sensitivity(self):
        """Streams with PII columns are excluded when max_sensitivity is 'low'."""
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {"name": "customers", "columns": ["id", "name", "email", "phone"]},
                {"name": "orders", "columns": ["id", "amount"]},
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams",
            source_id="s1",
            prompt="sync customer and order data",
            policy={"max_sensitivity": "low"},
        )
        assert result["success"] is True
        selected_names = [s["name"] for s in result["selected_streams"]]
        assert "customers" not in selected_names  # has PII
        assert "orders" in selected_names

    @pytest.mark.asyncio
    async def test_limit_param(self):
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {"name": "customers", "columns": ["id"]},
                {"name": "customer_orders", "columns": ["id"]},
                {"name": "customer_logs", "columns": ["id"]},
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams", source_id="s1", prompt="sync customer data", limit=1
        )
        assert result["success"] is True
        assert result["count"] == 1

    @pytest.mark.asyncio
    async def test_schema_filter(self):
        """Only include streams from specified schemas/namespaces."""
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {"name": "sales_customers", "namespace": "sales", "columns": ["id"]},
                {"name": "hr_employees", "namespace": "hr", "columns": ["id", "customer_id"]},
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams",
            source_id="s1",
            prompt="sync customer data",
            schemas=["sales"],
        )
        assert result["success"] is True
        selected_names = [s["name"] for s in result["selected_streams"]]
        assert "sales_customers" in selected_names

    @pytest.mark.asyncio
    async def test_incremental_mode_preferred(self):
        """Streams supporting incremental should get incremental sync mode."""
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {
                    "name": "orders",
                    "columns": ["id", "amount"],
                    "supportedSyncModes": ["full_refresh", "incremental"],
                },
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams", source_id="s1", prompt="sync order data"
        )
        assert result["success"] is True
        stream = result["selected_streams"][0]
        assert stream["syncMode"] == "incremental"

    @pytest.mark.asyncio
    async def test_disallowed_streams_excluded(self):
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {"name": "customers", "columns": ["id"]},
                {"name": "customer_secrets", "columns": ["id"]},
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams",
            source_id="s1",
            prompt="sync customer data",
            policy={"disallowed_streams": ["customer_secrets"]},
        )
        assert result["success"] is True
        selected_names = [s["name"] for s in result["selected_streams"]]
        assert "customer_secrets" not in selected_names

    @pytest.mark.asyncio
    async def test_disallowed_namespace_excluded(self):
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {"name": "customers", "namespace": "public", "columns": ["id"]},
                {"name": "customers", "namespace": "internal", "columns": ["id"]},
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams",
            source_id="s1",
            prompt="sync customer data",
            policy={"disallowed_namespaces": ["internal"]},
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_exception_returns_error(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.discover_source_schema.side_effect = Exception("Discovery failed")
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams", source_id="s1", prompt="sync customer data"
        )
        assert result["success"] is False
        assert "Discovery failed" in result["error"]

    @pytest.mark.asyncio
    async def test_column_matching_adds_score(self):
        """A stream with matching column names should be selected even if name doesn't match."""
        orch = self._make_orchestrator()
        disc = self._discovery_result(
            [
                {"name": "tbl_abc", "columns": ["customer_id", "customer_name", "revenue"]},
                {"name": "tbl_xyz", "columns": ["widget_id"]},
            ]
        )
        orch.airbyte_client.discover_source_schema.return_value = disc
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams", source_id="s1", prompt="sync customer data"
        )
        assert result["success"] is True
        selected_names = [s["name"] for s in result["selected_streams"]]
        assert "tbl_abc" in selected_names


class TestCreateAirbyteConnection:
    """Tests for the internal create_airbyte_connection function (via create_intelligent_airbyte_pipeline)."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {"host": "localhost"}
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    @pytest.mark.asyncio
    async def test_full_pipeline_creation(self):
        """Full successful pipeline creation path."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = [
            "pg-def-id",  # source definition
            "sf-def-id",  # destination definition
        ]
        # Source creation - spec lookup, list, create
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        # Destination creation
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        # Connection creation
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}, "updated_at": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf",
        )
        assert result["success"] is True
        assert result["connection_id"] == "c1"

    @pytest.mark.asyncio
    async def test_pipeline_source_def_not_found(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = None
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="UnknownDB",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="test",
        )
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_pipeline_destination_def_not_found(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", None]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {"name": "t1", "json_schema": {"properties": {"id": {}}}},
                        "config": {},
                    }
                ]
            }
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="UnknownDest",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="test",
        )
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_pipeline_reuses_existing_connection(self):
        """When an existing connection matches source+dest+streams+schedule, reuse it."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        # Source exists already
        src_data = {
            "sourceId": "s1",
            "name": "PG",
            "sourceDefinitionId": "pg-def-id",
            "configuration": {"host": "h"},
        }
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = [src_data]
        orch.airbyte_client.get_source.return_value = src_data
        # Destination exists already
        dst_data = {
            "destinationId": "d1",
            "name": "SF",
            "destinationDefinitionId": "sf-def-id",
            "configuration": {"host": "sf"},
        }
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = [dst_data]
        orch.airbyte_client.get_destination.return_value = dst_data
        # Connection exists already matching source+dest+streams
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {"name": "t1", "json_schema": {"properties": {"id": {}}}},
                        "config": {},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = [
            {
                "connectionId": "c-existing",
                "name": "old-conn",
                "sourceId": "s1",
                "destinationId": "d1",
            }
        ]
        orch.airbyte_client.get_connection.return_value = {
            "connectionId": "c-existing",
            "scheduleType": "manual",
            "status": "active",
            "configurations": {"streams": [{"name": "t1", "syncMode": "full_refresh_overwrite"}]},
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf",
        )
        assert result["success"] is True
        assert result["connection_reused"] is True
        assert result["connection_id"] == "c-existing"

    @pytest.mark.asyncio
    async def test_pipeline_with_cron_schedule(self):
        """Verify schedule_cron is forwarded to create_connection."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}, "updated_at": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf-cron",
            schedule_type="cron",
            schedule_cron="0 2 * * *",
        )
        assert result["success"] is True
        # Verify cron params were forwarded to create_connection via raw_payload
        call_kwargs = orch.airbyte_client.create_connection.call_args
        raw_payload = call_kwargs.kwargs.get("raw_payload", {})
        assert raw_payload["schedule"]["scheduleType"] == "cron"
        assert raw_payload["schedule"]["cronExpression"] == "0 0 2 * * ?"

    @pytest.mark.asyncio
    async def test_airflow_orchestrated_overrides_cron_to_manual(self):
        """When airflow_orchestrated=True, cron schedule is overridden to manual."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}, "updated_at": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf-airflow",
            schedule_type="cron",
            schedule_cron="0 0 * * *",
            airflow_orchestrated=True,
        )
        assert result["success"] is True
        assert result["airflow_orchestrated"] is True
        assert result["schedule_type"] == "manual"
        assert result["intended_schedule_cron"] == "0 0 * * *"
        assert "advisory" in result
        assert "airflow_orchestrated=True" in result["advisory"]
        # Verify the Airbyte connection was created with manual schedule
        call_kwargs = orch.airbyte_client.create_connection.call_args
        raw_payload = call_kwargs.kwargs.get("raw_payload", {})
        assert raw_payload["schedule"]["scheduleType"] == "manual"

    @pytest.mark.asyncio
    async def test_airflow_orchestrated_normalizes_quartz_to_unix_cron(self):
        """6-field Quartz cron is normalized to 5-field Unix cron for Airflow compatibility."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf-quartz",
            schedule_cron="0 0 2 ? * *",
            airflow_orchestrated=True,
        )
        assert result["success"] is True
        # 6-field Quartz "0 0 2 ? * *" -> 5-field Unix "0 2 * * *"
        assert result["intended_schedule_cron"] == "0 2 * * *"
        assert "0 2 * * *" in result["advisory"]

    @pytest.mark.asyncio
    async def test_airflow_orchestrated_auto_updates_existing_cron_connection(self):
        """When airflow_orchestrated=True and existing connection has cron, auto-update to manual."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        # Existing connection with cron schedule and matching streams
        orch.airbyte_client.list_connections.return_value = [
            {"connectionId": "existing-c1", "sourceId": "s1", "destinationId": "d1", "name": "PG-SF"}
        ]
        orch.airbyte_client.get_connection.return_value = {
            "connectionId": "existing-c1",
            "name": "PG-SF",
            "status": "active",
            "schedule": {"scheduleType": "cron", "cronExpression": "0 0 3 ? * *"},
            "configurations": {
                "streams": [
                    {"name": "t1", "syncMode": "full_refresh_overwrite"}
                ]
            },
        }
        orch.airbyte_client.update_connection.return_value = {
            "connectionId": "existing-c1",
            "schedule": {"scheduleType": "manual"},
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="PG-SF",
            schedule_cron="0 3 * * *",
            airflow_orchestrated=True,
        )
        # Should succeed — not return clarification error
        assert result["success"] is True
        assert result.get("clarification_needed") is not True
        assert result["connection_id"] == "existing-c1"
        assert result["reused"] is True
        assert result["schedule_updated"] is True
        assert result["previous_schedule"] == "cron"
        assert result["airflow_orchestrated"] is True
        assert result["schedule_type"] == "manual"
        # Verify update_connection was called to set manual
        orch.airbyte_client.update_connection.assert_called_once_with(
            "existing-c1", schedule={"scheduleType": "manual"}
        )

    @pytest.mark.asyncio
    async def test_airflow_orchestrated_without_cron(self):
        """When airflow_orchestrated=True but no schedule_cron, connection is manual with no advisory."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf-manual",
            airflow_orchestrated=True,
        )
        assert result["success"] is True
        assert result["airflow_orchestrated"] is True
        assert result["schedule_type"] == "manual"
        assert "intended_schedule_cron" not in result
        assert "advisory" not in result

    @pytest.mark.asyncio
    async def test_airflow_orchestrated_false_preserves_cron(self):
        """When airflow_orchestrated=False (default), cron schedule is passed through normally."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}, "updated_at": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf-cron-default",
            schedule_type="cron",
            schedule_cron="0 0 * * *",
            airflow_orchestrated=False,
        )
        assert result["success"] is True
        assert "airflow_orchestrated" not in result
        # Verify cron was passed through to create_connection
        call_kwargs = orch.airbyte_client.create_connection.call_args
        raw_payload = call_kwargs.kwargs.get("raw_payload", {})
        assert raw_payload["schedule"]["scheduleType"] == "cron"

    @pytest.mark.asyncio
    async def test_airflow_orchestrated_with_schedule_type_cron_no_cron_expr(self):
        """airflow_orchestrated=True with schedule_type='cron' but no schedule_cron should still force manual."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf-edge",
            schedule_type="cron",
            airflow_orchestrated=True,
        )
        assert result["success"] is True
        assert result["airflow_orchestrated"] is True
        assert result["schedule_type"] == "manual"
        assert "intended_schedule_cron" not in result
        assert "advisory" not in result
        # Verify manual was sent, not cron
        call_kwargs = orch.airbyte_client.create_connection.call_args
        raw_payload = call_kwargs.kwargs.get("raw_payload", {})
        assert raw_payload["schedule"]["scheduleType"] == "manual"

    @pytest.mark.asyncio
    async def test_pipeline_with_namespace(self):
        """Verify namespace_definition and namespace_format are forwarded to create_connection."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}, "updated_at": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf-ns",
            namespace_definition="source",
            namespace_format="${SOURCE_NAMESPACE}",
        )
        assert result["success"] is True
        # Verify namespace params were forwarded to create_connection via raw_payload
        call_kwargs = orch.airbyte_client.create_connection.call_args
        raw_payload = call_kwargs.kwargs.get("raw_payload", {})
        assert raw_payload.get("namespaceDefinition") == "source"
        assert raw_payload.get("namespaceFormat") == "${SOURCE_NAMESPACE}"

    @pytest.mark.asyncio
    async def test_pipeline_stream_name_mismatch_returns_clarification(self):
        """Stream name 'customers' (plural) should fail when source only has 'customer' (singular)."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = [
            "pg-def-id",  # source definition
        ]
        # Source creation mocks
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        # Discovery returns "customer" (singular)
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {"stream": {"name": "customer", "supportedSyncModes": ["full_refresh"]}}
                ]
            }
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {
                    "name": "customers",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                }
            ],
            connection_name="pg-to-sf",
        )
        assert result["success"] is False
        assert result["action_required"] == "clarify_stream_names"
        assert "customers" in result["unmatched_streams"]
        assert "customer" in result["unmatched_streams"]["customers"]["suggestions"]

    @pytest.mark.asyncio
    async def test_pipeline_stream_name_exact_match_passes(self):
        """Stream name 'customer' matching discovered 'customer' should proceed normally."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = [
            "pg-def-id",  # source definition
            "sf-def-id",  # destination definition
        ]
        # Source creation mocks
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        # Discovery returns "customer" (exact match)
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {"stream": {"name": "customer", "supportedSyncModes": ["full_refresh"]}}
                ]
            }
        }
        # Destination creation mocks
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        # Connection creation mocks
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "customer"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "customer", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf",
        )
        assert result["success"] is True
        assert result["connection_id"] == "c1"

    @pytest.mark.asyncio
    async def test_pipeline_create_empty_connection_name_generates_default(self):
        """When connection_name is empty, a default is generated from source/destination names."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = [
            "pg-def-id",
            "sf-def-id",
        ]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {
            "destinationId": "d1",
            "name": "SF",
        }
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "overwrite",
                    },
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="",
        )
        assert result["success"] is True
        # Verify the generated name was passed to create_connection
        payload = orch.airbyte_client.create_connection.call_args[1]["raw_payload"]
        assert payload["name"] == "PG \u2192 SF"

    @pytest.mark.asyncio
    async def test_pipeline_create_whitespace_connection_name_generates_default(self):
        """When connection_name is whitespace-only, a default is generated."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        # Whitespace-only connection_name should be treated like empty
        # and auto-generated at the router level, not rejected
        orch.airbyte_client.find_definition_id_by_name.side_effect = [
            "pg-def-id",
            "sf-def-id",
        ]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {
            "destinationId": "d1",
            "name": "SF",
        }
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "json_schema": {"properties": {"id": {}}},
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "overwrite",
                    },
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="   ",
        )
        assert result["success"] is True
        payload = orch.airbyte_client.create_connection.call_args[1]["raw_payload"]
        assert payload["name"] == "PG \u2192 SF"

    @pytest.mark.asyncio
    async def test_pipeline_create_non_string_connection_name_rejected(self):
        """When connection_name is not a string, a clear validation error is returned."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            connection_name=123,
        )
        assert result["success"] is False
        assert "must be a string" in result["error"]

    @pytest.mark.asyncio
    async def test_pipeline_create_none_connection_name_rejected(self):
        """When connection_name is explicitly None, a clear validation error is returned."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            connection_name=None,
        )
        assert result["success"] is False
        assert "must be a string" in result["error"]


class TestSelectStreamsForConnection:
    """Tests for select_streams_for_connection (internal closure, tested through update_airbyte_connection with configurations)."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    @pytest.mark.asyncio
    async def test_update_connection_with_configurations(self):
        """update_airbyte_connection passes configurations to the API."""
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.return_value = {"connectionId": "c1"}
        orch.airbyte_client.update_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        configs = {"streams": [{"name": "t1", "syncMode": "full_refresh_overwrite"}]}
        result = await tools["airbyte_pipeline"](
            action="update", connection_id="c1", configurations=configs
        )
        assert result["success"] is True
        orch.airbyte_client.update_connection.assert_called_once()
        call_kwargs = orch.airbyte_client.update_connection.call_args
        assert "configurations" in call_kwargs.kwargs or "configurations" in (
            call_kwargs[1] if len(call_kwargs) > 1 else {}
        )

    @pytest.mark.asyncio
    async def test_update_connection_status(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.return_value = {"connectionId": "c1"}
        orch.airbyte_client.update_connection.return_value = {
            "connectionId": "c1",
            "status": "inactive",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="update", connection_id="c1", status="inactive"
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_update_connection_namespace(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.return_value = {"connectionId": "c1"}
        orch.airbyte_client.update_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="update",
            connection_id="c1",
            namespace_definition="source",
            namespace_format="${SOURCE_NAMESPACE}",
        )
        assert result["success"] is True


class TestSearchConnectors:
    """Tests for AirbyteClient.search_connectors method."""

    @pytest.fixture
    def client(self):
        return AirbyteClient(
            url="http://localhost:8000", username="user", password="pass", workspace_id="ws1"
        )

    @pytest.mark.asyncio
    async def test_search_both(self, client):
        client.list_source_definitions = AsyncMock(
            return_value=[{"name": "Postgres"}, {"name": "MySQL"}, {"name": "MongoDB"}]
        )
        client.list_destination_definitions = AsyncMock(
            return_value=[{"name": "Snowflake"}, {"name": "Postgres"}]
        )
        result = await client.search_connectors("postgres")
        assert len(result["sources"]) == 1
        assert result["sources"][0]["name"] == "Postgres"
        assert len(result["destinations"]) == 1
        assert result["destinations"][0]["name"] == "Postgres"

    @pytest.mark.asyncio
    async def test_search_source_only(self, client):
        client.list_source_definitions = AsyncMock(
            return_value=[{"name": "Postgres"}, {"name": "MySQL"}]
        )
        result = await client.search_connectors("mysql", connector_type="source")
        assert len(result["sources"]) == 1
        assert result["destinations"] == []

    @pytest.mark.asyncio
    async def test_search_destination_only(self, client):
        client.list_destination_definitions = AsyncMock(
            return_value=[{"name": "Snowflake"}, {"name": "BigQuery"}]
        )
        result = await client.search_connectors("snow", connector_type="destination")
        assert result["sources"] == []
        assert len(result["destinations"]) == 1

    @pytest.mark.asyncio
    async def test_search_no_results(self, client):
        client.list_source_definitions = AsyncMock(return_value=[{"name": "Postgres"}])
        client.list_destination_definitions = AsyncMock(return_value=[{"name": "Snowflake"}])
        result = await client.search_connectors("oracle")
        assert len(result["sources"]) == 0
        assert len(result["destinations"]) == 0

    @pytest.mark.asyncio
    async def test_search_case_insensitive(self, client):
        client.list_source_definitions = AsyncMock(return_value=[{"name": "PostgreSQL"}])
        client.list_destination_definitions = AsyncMock(return_value=[])
        result = await client.search_connectors("POSTGRESQL")
        assert len(result["sources"]) == 1

    @pytest.mark.asyncio
    async def test_search_exception_propagates(self, client):
        client.list_source_definitions = AsyncMock(side_effect=Exception("API down"))
        with pytest.raises(Exception, match="API down"):
            await client.search_connectors("postgres")


class TestGenerateAirflowDags:
    """Tests for DAG generation tool functions — parameter validation and error handling."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    @pytest.mark.asyncio
    async def test_csv_dag_no_csv_path_no_env(self):
        """generate_airflow_tdload_dag_from_csv returns error when csv_path is None and env not set."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        with patch.dict("os.environ", {}, clear=True):
            with patch("os.getenv", return_value=None):
                result = await tools["airflow_teradata_load"](method="csv_dag", teradata_profile="td_test")
                assert result["success"] is False
                assert result.get("action_required") == "ask_csv_path"

    @pytest.mark.asyncio
    async def test_csv_dag_no_target_database(self):
        """Returns error when target_database is not provided and not in settings."""
        orch = self._make_orchestrator()
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = None
        tools = self._register(orch)
        result = await tools["airflow_teradata_load"](method="csv_dag", csv_path="/tmp/test.csv", teradata_profile="td_test")
        assert result["success"] is False
        assert (
            "target_database" in result["error"].lower() or "TERADATA_DATABASE" in result["error"]
        )

    @pytest.mark.asyncio
    async def test_csv_dag_file_not_found(self):
        """Returns error when CSV file does not exist."""
        orch = self._make_orchestrator()
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "testdb"
        tools = self._register(orch)
        result = await tools["airflow_teradata_load"](
            method="csv_dag", csv_path="/nonexistent/file.csv", target_database="testdb",
            teradata_profile="td_test",
        )
        assert result["success"] is False
        assert "not found" in result["error"].lower() or "nonexistent" in result["error"]

    @pytest.mark.asyncio
    async def test_load_csv_complete_gen_failure(self):
        """load_csv_to_teradata_complete returns error when DAG generation fails."""
        orch = self._make_orchestrator()
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = None
        tools = self._register(orch)
        result = await tools["airflow_teradata_load"](
            method="csv_complete",
            csv_path="/nonexistent/data.csv",
            target_database="testdb",
            target_table="tbl",
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_table_transfer_dag_exception(self):
        """generate_airflow_tdload_table_transfer_dag catches exceptions."""
        orch = self._make_orchestrator()
        orch.airflow_client = Mock()
        orch.airflow_client.get_connection.side_effect = Exception("Connection not found")
        tools = self._register(orch)
        result = await tools["airflow_teradata_load"](
            method="table_transfer",
            source_database="src_db",
            source_table="src_tbl",
            target_database="tgt_db",
            target_table="tgt_tbl",
        )
        assert result["success"] is False


class TestGenerateCsvDagFull:
    """Comprehensive tests for generate_airflow_tdload_dag_from_csv with mocked
    CSVAnalyzer, AirflowTdLoadDAGGenerator, and filesystem operations."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        # Settings
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "testdb"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = "secret"
        orch.settings.teradata.port = 1025
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"
        # Airflow client
        orch.airflow_client = Mock()
        orch.airflow_client.get_connection = Mock(return_value={"conn_id": "teradata_default"})
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _mock_csv_analysis(self):
        """Build a mock CSVAnalysis object."""
        col = Mock()
        col.name = "id"
        col.inferred_teradata_type = "INTEGER"
        analysis = Mock()
        analysis.row_count = 100
        analysis.column_count = 2
        analysis.file_size_mb = 0.5
        analysis.delimiter = ","
        analysis.columns = [col]
        return analysis

    @pytest.mark.asyncio
    async def test_successful_dag_generation(self):
        """Full successful path with all mocks in place."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()
        mock_gen = Mock()
        mock_gen.generate_file_loading_dag.return_value = "# DAG code"

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG code",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
        ):
            # Make Path(csv_path).exists() return True
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "customers"
            mock_csv_file.name = "customers.csv"
            # Make Path(dags_folder) return a mock
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/load_testdb_customers.py"))
            )
            MockPath.side_effect = lambda x: (
                mock_csv_file if "customers" in str(x) else mock_dags_folder
            )

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/customers.csv",
                target_database="testdb",
                target_table="customers",
                dag_id="load_testdb_customers",
                teradata_profile="td_test",
            )
            assert result["success"] is True
            assert result["dag_id"] == "load_testdb_customers"
            assert result["target_database"] == "testdb"
            assert result["target_table"] == "customers"
            assert result["csv_analysis"]["row_count"] == 100
            assert result["operator_type"] == "TdLoadOperator"
            assert result["validation_tasks"] == 3

    @pytest.mark.asyncio
    async def test_auto_generate_table_name(self):
        """Table name auto-generated from CSV filename."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "My-Sales Data (2024)"
            mock_csv_file.name = "My-Sales Data (2024).csv"
            mock_csv_file.resolve.return_value = mock_csv_file
            mock_csv_file.__str__ = Mock(return_value="/data/My-Sales Data (2024).csv")
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: (
                mock_csv_file if "Sales" in str(x) else mock_dags_folder
            )

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/My-Sales Data (2024).csv",
                target_database="testdb",
                teradata_profile="td_test",
            )
            assert result["success"] is True
            # Table name should be sanitized: lowercase, underscores, no special chars
            assert result["target_table"] == "my_sales_data_2024"

    @pytest.mark.asyncio
    async def test_auto_generate_table_name_with_prefix(self):
        """Table name auto-generated with prefix."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "orders"
            mock_csv_file.name = "orders.csv"
            mock_csv_file.resolve.return_value = mock_csv_file
            mock_csv_file.__str__ = Mock(return_value="/data/orders.csv")
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: (
                mock_csv_file if "orders" in str(x) else mock_dags_folder
            )

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/orders.csv",
                target_database="testdb",
                table_prefix="stg_",
                teradata_profile="td_test",
            )
            assert result["success"] is True
            assert result["target_table"] == "stg_orders"

    @pytest.mark.asyncio
    async def test_no_validations(self):
        """generate_validations=False skips validation queries."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                generate_validations=False,
                teradata_profile="td_test",
            )
            assert result["success"] is True
            assert result["validation_tasks"] == 0

    @pytest.mark.asyncio
    async def test_teradata_conn_creation_when_not_exists(self):
        """Teradata connection is created when get_connection raises."""
        orch = self._make_orchestrator()
        # get_connection raises on first call (teradata_default), succeeds on second (ssh)
        call_count = [0]

        def side_effect_get_conn(*args, **kwargs):
            call_count[0] += 1
            conn_id = kwargs.get("connection_id", args[0] if args else "")
            if conn_id == "teradata_default":
                raise Exception("Not found")
            return {"conn_id": conn_id}

        orch.airflow_client.get_connection = side_effect_get_conn
        orch.airflow_client.create_connection = Mock()

        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            # asyncio.to_thread delegates to the function — simulate get_connection raising
            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                teradata_profile="td_test",
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_ssh_conn_creation_when_not_exists(self):
        """SSH connection is created when get_connection raises for SSH."""
        orch = self._make_orchestrator()

        def side_effect_get_conn(*args, **kwargs):
            conn_id = kwargs.get("connection_id", args[0] if args else "")
            if "ssh" in str(conn_id):
                raise Exception("Not found")
            return {"conn_id": conn_id}

        orch.airflow_client.get_connection = side_effect_get_conn
        orch.airflow_client.create_connection = Mock()

        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ", {"MCP_CLIENT_SSH_USER": "testuser", "MCP_CLIENT_SSH_HOST": "10.0.0.1"}
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                teradata_profile="td_test",
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_csv_path_from_env(self):
        """csv_path=None returns action_required asking the user to provide it."""
        orch = self._make_orchestrator()
        tools = self._register(orch)

        result = await tools["airflow_teradata_load"](method="csv_dag", teradata_profile="td_test")
        assert result["success"] is False
        assert result["action_required"] == "ask_csv_path"

    @pytest.mark.asyncio
    async def test_target_database_from_settings(self):
        """target_database falls back to settings.teradata.database."""
        orch = self._make_orchestrator()
        orch.settings.teradata.database = "from_settings_db"
        tools = self._register(orch)

        with patch("elt_mcp_server.tools.data_movement.Path") as MockPath:
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = False
            mock_csv_file.resolve.return_value = mock_csv_file
            mock_csv_file.__str__ = Mock(return_value="/nonexistent.csv")
            MockPath.return_value = mock_csv_file

            result = await tools["airflow_teradata_load"](method="csv_dag", csv_path="/nonexistent.csv", teradata_profile="td_test")
            # File doesn't exist, but target_database was resolved from settings
            assert result["success"] is False
            assert "not found" in result["error"]


class TestLoadCsvToTeradataComplete:
    """Comprehensive tests for load_csv_to_teradata_complete."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "testdb"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = "secret"
        orch.settings.teradata.port = 1025
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"
        orch.airflow_client = Mock()
        orch.airflow_client.get_connection = Mock(return_value={"conn_id": "ok"})
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _mock_csv_analysis(self):
        col = Mock()
        col.name = "id"
        col.inferred_teradata_type = "INTEGER"
        analysis = Mock()
        analysis.row_count = 50
        analysis.column_count = 1
        analysis.file_size_mb = 0.1
        analysis.delimiter = ","
        analysis.columns = [col]
        return analysis

    @pytest.mark.asyncio
    async def test_generation_only_no_deploy(self):
        """Default: deploy_to_airflow=False, just generates DAG."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            result = await tools["airflow_teradata_load"](
                method="csv_complete",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                deploy_to_airflow=False,
                teradata_profile="td_test",
            )
            assert result["success"] is True
            assert result["dag_id"] is not None
            # Steps: generate (success), deploy (skipped), trigger (skipped)
            steps = result["details"]["steps"]
            assert steps[0]["name"] == "generate_dag"
            assert steps[0]["result"]["success"] is True
            deploy_step = next(s for s in steps if s["name"] == "deploy_dag")
            assert deploy_step["result"].get("skipped") is True

    @pytest.mark.asyncio
    async def test_generation_failure_stops_workflow(self):
        """If DAG generation fails, workflow stops immediately."""
        orch = self._make_orchestrator()
        tools = self._register(orch)

        # Make csv_path not exist → generation will fail
        result = await tools["airflow_teradata_load"](
            method="csv_complete",
            csv_path="/nonexistent/data.csv",
            target_database="testdb",
            target_table="tbl",
            teradata_profile="td_test",
        )
        assert result["success"] is False
        assert "DAG generation failed" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_deploy_with_connection_failure(self):
        """Deploy fails when Teradata connection creation fails."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        _td_mock_1 = AsyncMock(return_value={"success": False, "error": "No TD config"})
        _ssh_mock_1 = AsyncMock(return_value={"success": True})

        async def _conn_dispatch_1(**kwargs):
            if kwargs.get("action") == "create_teradata":
                return await _td_mock_1(**kwargs)
            return await _ssh_mock_1(**kwargs)

        pm_tools_dict = {
            "airflow_connections": _conn_dispatch_1,
            "pipeline_deploy": AsyncMock(return_value={"success": True}),
        }

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
            patch(
                "elt_mcp_server.tools.airflow_pipeline_management.register_pipeline_tools",
                return_value=pm_tools_dict,
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            result = await tools["airflow_teradata_load"](
                method="csv_complete",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                deploy_to_airflow=True,
            )
            assert result["success"] is False
            assert "Teradata connection" in result["error"]

    @pytest.mark.asyncio
    async def test_deploy_with_ssh_failure(self):
        """Deploy fails when SSH connection creation fails."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        _td_mock_2 = AsyncMock(return_value={"success": True})
        _ssh_mock_2 = AsyncMock(return_value={"success": False, "error": "SSH config missing"})

        async def _conn_dispatch_2(**kwargs):
            if kwargs.get("action") == "create_teradata":
                return await _td_mock_2(**kwargs)
            return await _ssh_mock_2(**kwargs)

        pm_tools_dict = {
            "airflow_connections": _conn_dispatch_2,
            "pipeline_deploy": AsyncMock(return_value={"success": True}),
        }

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
            patch(
                "elt_mcp_server.tools.airflow_pipeline_management.register_pipeline_tools",
                return_value=pm_tools_dict,
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            result = await tools["airflow_teradata_load"](
                method="csv_complete",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                deploy_to_airflow=True,
                teradata_profile="td_test",
            )
            assert result["success"] is False
            assert "SSH connection" in result["error"]

    @pytest.mark.asyncio
    async def test_deploy_success_no_trigger(self):
        """Full deploy succeeds but trigger_after_deploy=False."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        _td_mock_3 = AsyncMock(return_value={"success": True})
        _ssh_mock_3 = AsyncMock(return_value={"success": True})

        async def _conn_dispatch_3(**kwargs):
            if kwargs.get("action") == "create_teradata":
                return await _td_mock_3(**kwargs)
            return await _ssh_mock_3(**kwargs)

        pm_tools_dict = {
            "airflow_connections": _conn_dispatch_3,
            "pipeline_deploy": AsyncMock(return_value={"success": True}),
        }

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
            patch(
                "elt_mcp_server.tools.airflow_pipeline_management.register_pipeline_tools",
                return_value=pm_tools_dict,
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            result = await tools["airflow_teradata_load"](
                method="csv_complete",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                deploy_to_airflow=True,
                trigger_after_deploy=False,
                teradata_profile="td_test",
            )
            assert result["success"] is True
            assert result["workflow_steps_completed"] >= 2

    @pytest.mark.asyncio
    async def test_deploy_failure_stops_workflow(self):
        """If deployment fails, workflow stops."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        _td_mock_4 = AsyncMock(return_value={"success": True})
        _ssh_mock_4 = AsyncMock(return_value={"success": True})

        async def _conn_dispatch_4(**kwargs):
            if kwargs.get("action") == "create_teradata":
                return await _td_mock_4(**kwargs)
            return await _ssh_mock_4(**kwargs)

        pm_tools_dict = {
            "airflow_connections": _conn_dispatch_4,
            "pipeline_deploy": AsyncMock(return_value={"success": False, "error": "Deploy failed"}),
        }

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
            patch(
                "elt_mcp_server.tools.airflow_pipeline_management.register_pipeline_tools",
                return_value=pm_tools_dict,
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            result = await tools["airflow_teradata_load"](
                method="csv_complete",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                deploy_to_airflow=True,
                teradata_profile="td_test",
            )
            assert result["success"] is False
            assert "deployment failed" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_deploy_and_trigger_success(self):
        """Full workflow: generate + deploy + trigger all succeed."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        _td_mock_5 = AsyncMock(return_value={"success": True})
        _ssh_mock_5 = AsyncMock(return_value={"success": True})

        async def _conn_dispatch_5(**kwargs):
            if kwargs.get("action") == "create_teradata":
                return await _td_mock_5(**kwargs)
            return await _ssh_mock_5(**kwargs)

        pm_tools_dict = {
            "airflow_connections": _conn_dispatch_5,
            "pipeline_deploy": AsyncMock(
                return_value={
                    "success": True,
                    "dag_triggered": True,
                    "dag_run_id": "run-1",
                    "trigger_info": {},
                }
            ),
        }

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
            patch(
                "elt_mcp_server.tools.airflow_pipeline_management.register_pipeline_tools",
                return_value=pm_tools_dict,
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            result = await tools["airflow_teradata_load"](
                method="csv_complete",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                deploy_to_airflow=True,
                trigger_after_deploy=True,
                teradata_profile="td_test",
            )
            assert result["success"] is True
            # Verify trigger step was captured
            trigger_step = next(
                (s for s in result["details"]["steps"] if s["name"] == "trigger_dag"), None
            )
            assert trigger_step is not None
            assert trigger_step["result"]["success"] is True

    @pytest.mark.asyncio
    async def test_deploy_trigger_separate_step(self):
        """When deploy doesn't trigger, trigger happens in separate step."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        _td_mock_6 = AsyncMock(return_value={"success": True})
        _ssh_mock_6 = AsyncMock(return_value={"success": True})

        async def _conn_dispatch_6(**kwargs):
            if kwargs.get("action") == "create_teradata":
                return await _td_mock_6(**kwargs)
            return await _ssh_mock_6(**kwargs)

        pm_tools_dict = {
            "airflow_connections": _conn_dispatch_6,
            "pipeline_deploy": AsyncMock(
                return_value={
                    "success": True,
                    "dag_triggered": False,
                }
            ),
        }
        oe_tools_dict = {
            "dag_trigger": AsyncMock(return_value={"success": True, "dag_run_id": "run-2"}),
        }

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
            patch(
                "elt_mcp_server.tools.airflow_pipeline_management.register_pipeline_tools",
                return_value=pm_tools_dict,
            ),
            patch(
                "elt_mcp_server.tools.orchestration_execution.register_orchestration_tools",
                return_value=oe_tools_dict,
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            result = await tools["airflow_teradata_load"](
                method="csv_complete",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                deploy_to_airflow=True,
                trigger_after_deploy=True,
                teradata_profile="td_test",
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_trigger_failure(self):
        """Trigger step fails after successful deploy."""
        orch = self._make_orchestrator()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        _td_mock_7 = AsyncMock(return_value={"success": True})
        _ssh_mock_7 = AsyncMock(return_value={"success": True})

        async def _conn_dispatch_7(**kwargs):
            if kwargs.get("action") == "create_teradata":
                return await _td_mock_7(**kwargs)
            return await _ssh_mock_7(**kwargs)

        pm_tools_dict = {
            "airflow_connections": _conn_dispatch_7,
            "pipeline_deploy": AsyncMock(return_value={"success": True, "dag_triggered": False}),
        }
        oe_tools_dict = {
            "dag_trigger": AsyncMock(return_value={"success": False, "error": "DAG not found"}),
        }

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread", new_callable=lambda: AsyncMock),
            patch(
                "elt_mcp_server.tools.airflow_pipeline_management.register_pipeline_tools",
                return_value=pm_tools_dict,
            ),
            patch(
                "elt_mcp_server.tools.orchestration_execution.register_orchestration_tools",
                return_value=oe_tools_dict,
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            result = await tools["airflow_teradata_load"](
                method="csv_complete",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                deploy_to_airflow=True,
                trigger_after_deploy=True,
                teradata_profile="td_test",
            )
            assert result["success"] is False
            assert "trigger failed" in result["error"].lower()


class TestTableTransferDagFull:
    """Comprehensive tests for generate_airflow_tdload_table_transfer_dag."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        orch.settings = Mock()
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"
        # Source Teradata settings
        td_settings = Mock()
        td_settings.host = "td-source-host"
        td_settings.username = "dbc"
        td_settings.password = Mock()
        td_settings.password.get_secret_value.return_value = "secret"
        td_settings.port = 1025
        orch.settings.get_source_teradata.return_value = td_settings
        # Airflow client: single AsyncMock for both sync and async code paths
        airflow_client = AsyncMock()
        airflow_client.list_connections.return_value = []
        airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        airflow_client.create_connection = AsyncMock()
        orch.airflow_client = airflow_client
        orch.async_airflow_client = airflow_client
        # Teradata client
        orch.teradata_client = Mock()
        orch.teradata_client.get_table_metadata = Mock(
            return_value={
                "columns": [
                    {"name": "id", "type": "I"},
                    {"name": "name", "type": "CV", "length": 100},
                ],
                "row_count": 1000,
            }
        )
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    @pytest.mark.asyncio
    async def test_successful_table_transfer(self):
        """Full successful table transfer DAG generation."""
        orch = self._make_orchestrator()
        tools = self._register(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_table_transfer_dag",
                return_value="# DAG",
            ),
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ", {"MCP_CLIENT_SSH_HOST": "10.0.0.1", "MCP_CLIENT_SSH_USER": "testuser", "MCP_CLIENT_SSH_PASSWORD": "ssh-pass"}
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
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
                dag_id="transfer_test",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )
            assert result["success"] is True
            assert result["dag_id"] == "transfer_test"
            assert result["source_database"] == "src_db"
            assert result["target_database"] == "tgt_db"
            assert result["operator_type"] == "TdLoadOperator"
            assert result["validation_tasks"] == 2

    @pytest.mark.asyncio
    async def test_auto_generated_dag_id(self):
        """DAG ID auto-generated from source/target names."""
        orch = self._make_orchestrator()
        tools = self._register(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_table_transfer_dag",
                return_value="# DAG",
            ),
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ", {"MCP_CLIENT_SSH_HOST": "10.0.0.1", "MCP_CLIENT_SSH_USER": "testuser", "MCP_CLIENT_SSH_PASSWORD": "ssh-pass"}
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
                source_database="SRC_DB",
                source_table="SRC_TBL",
                target_database="TGT_DB",
                target_table="TGT_TBL",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )
            assert result["success"] is True
            assert result["dag_id"] == "transfer_src_db_src_tbl_to_tgt_db_tgt_tbl"

    @pytest.mark.asyncio
    async def test_source_conn_creation(self):
        """Source connection is created when it doesn't exist."""
        orch = self._make_orchestrator()
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        orch.async_airflow_client.create_connection = AsyncMock()
        tools = self._register(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_table_transfer_dag",
                return_value="# DAG",
            ),
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ", {"MCP_CLIENT_SSH_HOST": "10.0.0.1", "MCP_CLIENT_SSH_USER": "testuser", "MCP_CLIENT_SSH_PASSWORD": "ssh-pass"}
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
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
                dag_id="transfer_test",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_source_conn_creation_failure(self):
        """Returns error when source connection creation fails."""
        orch = self._make_orchestrator()
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        orch.async_airflow_client.create_connection.side_effect = Exception("Auth error")
        tools = self._register(orch)

        with patch("asyncio.to_thread") as mock_thread:

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
            )
            assert result["success"] is False

    @pytest.mark.asyncio
    async def test_no_validations(self):
        """generate_validations=False skips validation queries."""
        orch = self._make_orchestrator()
        tools = self._register(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_table_transfer_dag",
                return_value="# DAG",
            ),
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ", {"MCP_CLIENT_SSH_HOST": "10.0.0.1", "MCP_CLIENT_SSH_USER": "testuser", "MCP_CLIENT_SSH_PASSWORD": "ssh-pass"}
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
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
                dag_id="transfer_test",
                generate_validations=False,
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )
            assert result["success"] is True
            assert result["validation_tasks"] == 0

    @pytest.mark.asyncio
    async def test_with_custom_profile(self):
        """Custom target_teradata_profile overrides settings."""
        orch = self._make_orchestrator()
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "custom-host",
            "username": "custom_user",
            "password": "custom_pass",
            "port": 1025,
        }
        orch.credential_resolver = resolver
        # All connections created fresh (get_connection returns 404)
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        orch.async_airflow_client.create_connection = AsyncMock()
        tools = self._register(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_table_transfer_dag",
                return_value="# DAG",
            ),
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ", {"MCP_CLIENT_SSH_HOST": "10.0.0.1", "MCP_CLIENT_SSH_USER": "testuser", "MCP_CLIENT_SSH_PASSWORD": "ssh-pass"}
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
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
                dag_id="transfer_test",
                target_teradata_profile="custom_target",
                source_teradata_profile="src_test",
            )
            assert result["success"] is True


class TestTriggerSyncWithWait:
    """Additional trigger_airbyte_sync tests for wait_for_completion path."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    @pytest.mark.asyncio
    async def test_trigger_sync_with_wait(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.trigger_sync.return_value = {
            "jobId": 42,
            "status": "pending",
            "createdAt": "2025-01-01",
        }
        orch.airbyte_client.wait_for_job.return_value = {"status": "succeeded"}
        tools = self._register(orch)
        result = await tools["airbyte_sync"](
            action="trigger", connection_id="c1", wait_for_completion=True
        )
        assert result["success"] is True
        assert result["status"] == "succeeded"
        assert result["final_status"]["status"] == "succeeded"
        orch.airbyte_client.wait_for_job.assert_called_once_with(42)


class TestListStreamsEdgeCases:
    """Additional edge case tests for list_streams."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    @pytest.mark.asyncio
    async def test_list_streams_no_catalog(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.discover_source_schema.return_value = {}
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="streams", source_id="s1")
        assert result["success"] is False
        assert "catalog" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_list_streams_empty_streams(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.discover_source_schema.return_value = {"catalog": {"streams": []}}
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="streams", source_id="s1")
        assert result["success"] is True
        assert result["stream_count"] == 0
        assert result["streams"] == []

    @pytest.mark.asyncio
    async def test_list_streams_error(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.discover_source_schema.side_effect = Exception("Connection timeout")
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="streams", source_id="s1")
        assert result["success"] is False
        assert "Connection timeout" in result["error"]

    @pytest.mark.asyncio
    async def test_list_streams_with_details(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "users",
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "sourceDefinedCursor": True,
                            "defaultCursorField": ["updated_at"],
                            "namespace": "public",
                        },
                        "config": {},
                    }
                ]
            }
        }
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="streams", source_id="s1")
        assert result["success"] is True
        assert result["stream_count"] == 1
        s = result["streams"][0]
        assert s["name"] == "users"
        assert "incremental" in s["supported_sync_modes"]
        assert s["source_defined_cursor"] is True
        assert s["default_cursor_field"] == ["updated_at"]
        assert s["namespace"] == "public"


class TestGetConnectionDetailsEdgeCases:
    """Additional tests for get_airbyte_connection_details."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    @pytest.mark.asyncio
    async def test_connection_details_error(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.get_connection.side_effect = Exception("Not found")
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="connection_details", connection_id="bad-id"
        )
        assert result["success"] is False
        assert "Not found" in result["error"]
        assert result["connection_id"] == "bad-id"


class TestListConnectionsEdgeCases:
    """Additional tests for list_airbyte_connections."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        orch.settings = Mock()
        orch.settings.airbyte = Mock()
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    @pytest.mark.asyncio
    async def test_list_connections_no_workspace_id(self):
        orch = self._make_orchestrator()
        orch.settings.airbyte.workspace_id = None
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="connections")
        assert result["success"] is False
        assert "Workspace ID" in result["error"]

    @pytest.mark.asyncio
    async def test_list_connections_error(self):
        orch = self._make_orchestrator()
        orch.settings.airbyte.workspace_id = "ws1"
        orch.airbyte_client.list_connections.side_effect = Exception("timeout")
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](list_type="connections")
        assert result["success"] is False
        assert "timeout" in result["error"]


# ── Phase 1 Tests ──────────────────────────────────────────────────────


class TestDiscoveryCache:
    """Tests for the DiscoveryCache class."""

    @pytest.mark.asyncio
    async def test_cache_miss_calls_api(self):
        client = AsyncMock()
        client.discover_source_schema.return_value = {"catalog": {"streams": []}}
        cache = DiscoveryCache(client)
        result = await cache.get("src1")
        assert result == {"catalog": {"streams": []}}
        client.discover_source_schema.assert_awaited_once_with("src1")

    @pytest.mark.asyncio
    async def test_cache_hit_skips_api(self):
        client = AsyncMock()
        client.discover_source_schema.return_value = {
            "catalog": {"streams": [{"stream": {"name": "users"}}]}
        }
        cache = DiscoveryCache(client)
        await cache.get("src1")
        await cache.get("src1")
        # Only one API call despite two get() calls
        client.discover_source_schema.assert_awaited_once_with("src1")

    @pytest.mark.asyncio
    async def test_different_source_ids_separate(self):
        client = AsyncMock()
        client.discover_source_schema.side_effect = [
            {"catalog": {"streams": [{"stream": {"name": "a"}}]}},
            {"catalog": {"streams": [{"stream": {"name": "b"}}]}},
        ]
        cache = DiscoveryCache(client)
        r1 = await cache.get("src1")
        r2 = await cache.get("src2")
        assert r1 != r2
        assert client.discover_source_schema.await_count == 2

    @pytest.mark.asyncio
    async def test_invalidate_specific(self):
        client = AsyncMock()
        client.discover_source_schema.return_value = {"catalog": {"streams": []}}
        cache = DiscoveryCache(client)
        await cache.get("src1")
        cache.invalidate("src1")
        await cache.get("src1")
        assert client.discover_source_schema.await_count == 2

    @pytest.mark.asyncio
    async def test_invalidate_all(self):
        client = AsyncMock()
        client.discover_source_schema.return_value = {"catalog": {"streams": []}}
        cache = DiscoveryCache(client)
        await cache.get("src1")
        await cache.get("src2")
        assert client.discover_source_schema.await_count == 2
        cache.invalidate()
        # Both entries cleared; re-fetching src1 triggers a new API call
        await cache.get("src1")
        assert client.discover_source_schema.await_count == 3

    def test_peek_returns_none_for_uncached(self):
        client = AsyncMock()
        cache = DiscoveryCache(client)
        assert cache.peek("src1") is None

    @pytest.mark.asyncio
    async def test_peek_returns_cached_value(self):
        client = AsyncMock()
        expected = {"catalog": {"streams": []}}
        client.discover_source_schema.return_value = expected
        cache = DiscoveryCache(client)
        await cache.get("src1")
        assert cache.peek("src1") == expected

    @pytest.mark.asyncio
    async def test_api_error_propagates(self):
        client = AsyncMock()
        client.discover_source_schema.side_effect = Exception("API timeout")
        cache = DiscoveryCache(client)
        with pytest.raises(Exception, match="API timeout"):
            await cache.get("src1")


class TestLevenshteinDistance:
    """Tests for _levenshtein_distance."""

    def test_identical_strings(self):
        assert _levenshtein_distance("abc", "abc") == 0

    def test_empty_strings(self):
        assert _levenshtein_distance("", "") == 0

    def test_one_empty(self):
        assert _levenshtein_distance("abc", "") == 3
        assert _levenshtein_distance("", "xyz") == 3

    def test_substitution(self):
        assert _levenshtein_distance("cat", "car") == 1

    def test_insertion(self):
        assert _levenshtein_distance("cat", "cats") == 1

    def test_deletion(self):
        assert _levenshtein_distance("cats", "cat") == 1

    def test_completely_different(self):
        assert _levenshtein_distance("abc", "xyz") == 3


class TestFuzzyTokenScore:
    """Tests for _fuzzy_token_score."""

    def test_exact_match(self):
        assert _fuzzy_token_score("customers", "customers") == 1.0

    def test_token_match_in_compound_name(self):
        score = _fuzzy_token_score("nation", "nation_code")
        assert score == 0.95

    def test_no_false_positive_nation_national_id(self):
        """'nation' should NOT get a high score against 'national_id'."""
        score = _fuzzy_token_score("nation", "national_id")
        # 'nation' is not an exact token in ['national', 'id'], so no 0.95
        # It's a prefix of 'national' so it gets 0.85
        assert score < 0.95
        assert score == 0.85

    def test_prefix_match(self):
        score = _fuzzy_token_score("cust", "customers")
        assert score == 0.85

    def test_fuzzy_typo(self):
        score = _fuzzy_token_score("custmer", "customer")
        # Levenshtein distance of 1, high ratio → fuzzy score
        assert score > 0.5

    def test_no_match(self):
        score = _fuzzy_token_score("zzzzz", "customers")
        assert score == 0.0

    def test_short_keyword_safety(self):
        # Keywords shorter than 3 chars don't get prefix match
        score = _fuzzy_token_score("cu", "customers")
        # No exact token, no prefix (len < 3), fuzzy ratio too low
        assert score < 0.85

    def test_empty_inputs(self):
        assert _fuzzy_token_score("", "customers") == 0.0
        assert _fuzzy_token_score("test", "") == 0.0


class TestScoreStreamV2:
    """Tests for _score_stream_v2."""

    def test_exact_name_match_high_score(self):
        item = {"name": "customers", "namespace": "", "description": "", "columns": [], "tags": []}
        score = _score_stream_v2(item, ["customers"])
        assert score >= 4.0  # 1.0 * 4.0 weight

    def test_namespace_contributes(self):
        item = {"name": "other", "namespace": "sales", "description": "", "columns": [], "tags": []}
        score = _score_stream_v2(item, ["sales"])
        assert score >= 2.0  # namespace weight

    def test_column_match(self):
        item = {
            "name": "data",
            "namespace": "",
            "description": "",
            "columns": ["customer_id", "amount"],
            "tags": [],
        }
        score = _score_stream_v2(item, ["customer"])
        assert score > 0  # column match contributes

    def test_no_match_zero_score(self):
        item = {
            "name": "users",
            "namespace": "public",
            "description": "User table",
            "columns": ["id"],
            "tags": [],
        }
        score = _score_stream_v2(item, ["zzzznothing"])
        assert score == 0.0


class TestSuggestStreamNames:
    """Tests for _suggest_stream_names."""

    def test_substring_match(self):
        names = ["customers", "orders", "customer_addresses", "products"]
        result = _suggest_stream_names("customer", names)
        # customers and customer_addresses should be ranked first
        assert "customers" in result[:2]
        assert "customer_addresses" in result[:2]

    def test_fuzzy_match(self):
        names = ["customers", "orders", "products"]
        result = _suggest_stream_names("custmers", names)
        assert result[0] == "customers"  # close levenshtein match

    def test_no_match_returns_available(self):
        names = ["alpha", "beta", "gamma"]
        result = _suggest_stream_names("zzzzz", names)
        assert len(result) <= 5
        assert len(result) > 0  # still returns some suggestions

    def test_empty_input(self):
        result = _suggest_stream_names("", ["a", "b"])
        assert len(result) <= 5

    def test_max_suggestions(self):
        names = [f"stream_{i}" for i in range(20)]
        result = _suggest_stream_names("stream", names, max_suggestions=3)
        assert len(result) == 3


class TestExtractStreamNames:
    """Tests for _extract_stream_names."""

    def test_normal_discovery(self):
        disc = {
            "catalog": {
                "streams": [
                    {"stream": {"name": "users"}},
                    {"stream": {"name": "orders"}},
                ]
            }
        }
        assert _extract_stream_names(disc) == ["users", "orders"]

    def test_empty_discovery(self):
        assert _extract_stream_names({}) == []
        assert _extract_stream_names(None) == []

    def test_missing_name_skipped(self):
        disc = {
            "catalog": {
                "streams": [
                    {"stream": {"name": "users"}},
                    {"stream": {}},
                ]
            }
        }
        assert _extract_stream_names(disc) == ["users"]


# ── Phase 2 Tests ──────────────────────────────────────────────────────


class TestCachedValidation:
    """Tests for cache integration into validators and build_configured_catalog."""

    def _discovery_result(self):
        return {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "orders",
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "propertyFields": [["id"], ["created_at"], ["amount"]],
                        },
                        "config": {},
                    },
                    {
                        "stream": {
                            "name": "customers",
                            "supportedSyncModes": ["full_refresh"],
                            "propertyFields": [["id"], ["name"], ["email"]],
                        },
                        "config": {},
                    },
                ]
            }
        }

    @pytest.mark.asyncio
    async def test_validate_cursor_fields_uses_cache(self):
        """When discovery_cache is provided, no direct API call is made."""
        client = AsyncMock()
        cache = DiscoveryCache(client)
        # Pre-populate cache
        client.discover_source_schema.return_value = self._discovery_result()
        await cache.get("src1")
        client.discover_source_schema.reset_mock()

        streams = [{"name": "orders", "syncMode": "incremental", "cursorField": "created_at"}]
        result = await _validate_cursor_fields(streams, "src1", client, discovery_cache=cache)
        assert result is None  # valid cursor
        client.discover_source_schema.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_validate_stream_names_uses_cache(self):
        """When discovery_cache is provided, no direct API call is made."""
        client = AsyncMock()
        cache = DiscoveryCache(client)
        client.discover_source_schema.return_value = self._discovery_result()
        await cache.get("src1")
        client.discover_source_schema.reset_mock()

        streams = [
            {"name": "orders", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
        ]
        result = await _validate_stream_names(streams, "src1", client, discovery_cache=cache)
        assert result is None  # name found
        client.discover_source_schema.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_build_configured_catalog_uses_discovery_result(self):
        """When discovery_result is passed, no discover_source_schema call is made."""
        config = {
            "url": "http://localhost:8000",
            "username": "airbyte",
            "password": "password",
            "workspace_id": "ws1",
        }
        client = AirbyteClient(**config)
        disc = self._discovery_result()
        selected = [
            {"name": "orders", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
        ]
        with patch.object(client, "discover_source_schema", new=AsyncMock()) as mock_discover:
            result = await client.build_configured_catalog("src1", selected, discovery_result=disc)
            mock_discover.assert_not_awaited()
            assert len(result.get("streams", [])) == 1
            assert result["streams"][0]["stream"]["name"] == "orders"

    @pytest.mark.asyncio
    async def test_backward_compat_without_cache(self):
        """Existing callers without cache/discovery_result still work."""
        client = AsyncMock()
        client.discover_source_schema.return_value = self._discovery_result()

        streams = [{"name": "orders", "syncMode": "incremental", "cursorField": "created_at"}]
        result = await _validate_cursor_fields(streams, "src1", client)
        assert result is None
        client.discover_source_schema.assert_awaited_once_with("src1")


# ── Phase 3 Tests ──────────────────────────────────────────────────────


class TestPreviewPipeline:
    """Tests for the preview_pipeline tool."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {"host": "localhost"}
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _standard_discovery(self):
        return {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "customers",
                            "namespace": "public",
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "json_schema": {"properties": {"id": {}, "name": {}, "email": {}}},
                        },
                        "config": {},
                    },
                    {
                        "stream": {
                            "name": "orders",
                            "namespace": "public",
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "json_schema": {
                                "properties": {"id": {}, "customer_id": {}, "amount": {}}
                            },
                        },
                        "config": {},
                    },
                    {
                        "stream": {
                            "name": "products",
                            "namespace": "public",
                            "supportedSyncModes": ["full_refresh"],
                            "json_schema": {"properties": {"id": {}, "sku": {}, "price": {}}},
                        },
                        "config": {},
                    },
                ]
            }
        }

    @pytest.mark.asyncio
    async def test_preview_with_existing_source(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = "pg-def-id"
        orch.airbyte_client.list_sources.return_value = [
            {"sourceId": "src1", "name": "My PG Source"},
        ]
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="preview",
            source_name="My PG Source",
            source_type="postgres",
            source_profile="test_source",
            streams=[
                {
                    "name": "customers",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                },
                {"name": "orders", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
            ],
        )
        assert result["success"] is True
        assert result["preview"] is True
        assert result["source_exists"] is True
        assert result["matched_count"] == 2
        assert not result["has_issues"]

    @pytest.mark.asyncio
    async def test_preview_invalid_source_type(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = None
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="preview",
            source_name="My Source",
            source_type="nonexistent_db",
            source_profile="test_source",
        )
        assert result["success"] is False
        assert result["preview"] is True
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_preview_no_source_exists(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = "pg-def-id"
        orch.airbyte_client.list_sources.return_value = []
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="preview",
            source_name="Missing Source",
            source_type="postgres",
            source_profile="test_source",
        )
        assert result["success"] is False
        assert result["source_exists"] is False

    @pytest.mark.asyncio
    async def test_preview_intent_based_matching(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = "pg-def-id"
        orch.airbyte_client.list_sources.return_value = [
            {"sourceId": "src1", "name": "PG Source"},
        ]
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="preview",
            source_name="PG Source",
            source_type="postgres",
            source_profile="test_source",
            intent="customer data",
        )
        assert result["success"] is True
        assert result["matched_count"] > 0
        # "customers" should be matched
        matched_names = [s["name"] for s in result["matched_streams"]]
        assert "customers" in matched_names

    @pytest.mark.asyncio
    async def test_preview_unmatched_streams_get_suggestions(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = "pg-def-id"
        orch.airbyte_client.list_sources.return_value = [
            {"sourceId": "src1", "name": "PG Source"},
        ]
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="preview",
            source_name="PG Source",
            source_type="postgres",
            source_profile="test_source",
            streams=[
                {
                    "name": "custmers",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                },
            ],
        )
        assert result["success"] is True
        assert result["has_issues"] is True
        assert result["matched_count"] == 0
        assert any("custmers" in issue for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_preview_wildcard_streams(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = "pg-def-id"
        orch.airbyte_client.list_sources.return_value = [
            {"sourceId": "src1", "name": "PG Source"},
        ]
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="preview",
            source_name="PG Source",
            source_type="postgres",
            source_profile="test_source",
            streams=[{"name": "*", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}],
        )
        assert result["success"] is True
        assert result["matched_count"] == 1  # wildcard passed through

    @pytest.mark.asyncio
    async def test_preview_destination_type_validation(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = [
            "pg-def-id",  # source type valid
            None,  # destination type invalid
        ]
        orch.airbyte_client.list_sources.return_value = [
            {"sourceId": "src1", "name": "PG Source"},
        ]
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="preview",
            source_name="PG Source",
            source_type="postgres",
            source_profile="test_source",
            streams=[
                {
                    "name": "customers",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                }
            ],
            destination_type="bad_destination",
        )
        assert result["success"] is True
        assert result["has_issues"] is True
        assert any("bad_destination" in issue for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_preview_exception_handling(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = Exception("API down")
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="preview",
            source_name="Source",
            source_type="postgres",
            source_profile="test_source",
        )
        assert result["success"] is False
        assert result["preview"] is True
        assert "API down" in result["error"]


# ── Phase 4 Tests ──────────────────────────────────────────────────────


class TestIntelligentPipelineWithIntent:
    """Tests for intent/dry_run/policy support in create_intelligent_airbyte_pipeline."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {"host": "localhost"}
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _setup_source_creation(self, orch):
        """Configure mocks for source definition lookup + source creation."""
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}

    def _setup_destination_creation(self, orch):
        """Configure mocks for destination definition lookup + destination creation."""
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}

    def _standard_discovery(self):
        return {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "customers",
                            "namespace": "public",
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "json_schema": {"properties": {"id": {}, "name": {}, "email": {}}},
                        },
                        "config": {"supported_sync_modes": ["full_refresh", "incremental"]},
                    },
                    {
                        "stream": {
                            "name": "orders",
                            "namespace": "public",
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "json_schema": {
                                "properties": {"id": {}, "customer_id": {}, "amount": {}}
                            },
                        },
                        "config": {"supported_sync_modes": ["full_refresh", "incremental"]},
                    },
                    {
                        "stream": {
                            "name": "products",
                            "namespace": "public",
                            "supportedSyncModes": ["full_refresh"],
                            "json_schema": {"properties": {"id": {}, "sku": {}, "price": {}}},
                        },
                        "config": {"supported_sync_modes": ["full_refresh"]},
                    },
                ]
            }
        }

    @pytest.mark.asyncio
    async def test_intent_auto_selects_streams(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        self._setup_source_creation(orch)
        self._setup_destination_creation(orch)
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "customers"},
                    "config": {"syncMode": "incremental", "destinationSyncMode": "append"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            connection_name="pg-to-sf",
            intent="customer data",
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_intent_no_match_returns_clarification(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = "pg-def-id"
        self._setup_source_creation(orch)
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            connection_name="pg-to-sf",
            intent="zzzznothing",
        )
        assert result["success"] is False
        assert "keywords" in result
        assert "available_streams" in result

    @pytest.mark.asyncio
    async def test_dry_run_stops_before_destination(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = "pg-def-id"
        self._setup_source_creation(orch)
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            connection_name="pg-to-sf",
            streams=[
                {
                    "name": "customers",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                }
            ],
            dry_run=True,
        )
        assert result["success"] is True
        assert result["dry_run"] is True
        assert result["stream_count"] == 1
        # Destination should NOT have been created
        orch.airbyte_client.create_destination.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_backward_compat_explicit_streams(self):
        """Existing callers with explicit streams should work identically."""
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        self._setup_source_creation(orch)
        self._setup_destination_creation(orch)
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "customers"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {
                    "name": "customers",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                }
            ],
            connection_name="pg-to-sf",
        )
        assert result["success"] is True
        assert result["connection_id"] == "c1"

    @pytest.mark.asyncio
    async def test_neither_streams_nor_intent_returns_error(self):
        orch = self._make_orchestrator()
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            connection_name="pg-to-sf",
        )
        assert result["success"] is False
        assert "Either" in result["error"]

    @pytest.mark.asyncio
    async def test_intent_with_policy_filtering(self):
        orch = self._make_orchestrator()
        orch.airbyte_client.find_definition_id_by_name.return_value = "pg-def-id"
        self._setup_source_creation(orch)
        orch.airbyte_client.discover_source_schema.return_value = self._standard_discovery()
        tools = self._register(orch)
        # Intent matches customers but dry_run so we can inspect the matched streams
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            connection_name="pg-to-sf",
            intent="customer orders",
            dry_run=True,
        )
        assert result["success"] is True
        assert result["dry_run"] is True
        stream_names = [s["name"] for s in result["streams"]]
        assert "customers" in stream_names
        assert "orders" in stream_names


class TestCachePassthrough:
    """Verify discover_source_schema is called minimal times through the full pipeline flow."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {"host": "localhost"}
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _setup_full_pipeline(self, orch):
        """Set up all mocks for a complete pipeline creation."""
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = []
        orch.airbyte_client.create_source.return_value = {"sourceId": "s1", "name": "PG"}
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "sf-def-id", "name": "Snowflake"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = []
        orch.airbyte_client.create_destination.return_value = {"destinationId": "d1", "name": "SF"}
        discovery = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "t1",
                            "supportedSyncModes": ["full_refresh"],
                            "json_schema": {"properties": {"id": {}, "name": {}}},
                            "propertyFields": [["id"], ["name"]],
                        },
                        "config": {"syncMode": "full_refresh"},
                    }
                ]
            }
        }
        orch.airbyte_client.discover_source_schema.return_value = discovery
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "t1"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                }
            ]
        }
        orch.airbyte_client.list_connections.return_value = []
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c1",
            "status": "active",
        }

    @pytest.mark.asyncio
    async def test_discover_called_once_full_pipeline(self):
        """Full pipeline: discover_source_schema should be called exactly once via cache."""
        orch = self._make_orchestrator()
        self._setup_full_pipeline(orch)
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            streams=[
                {"name": "t1", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}
            ],
            connection_name="pg-to-sf",
        )
        assert result["success"] is True
        # Only 1 discover call despite validation + build_configured_catalog
        assert orch.airbyte_client.discover_source_schema.await_count == 1

    @pytest.mark.asyncio
    async def test_discover_called_once_with_intent(self):
        """Intent-based pipeline: discover_source_schema should be called once."""
        orch = self._make_orchestrator()
        self._setup_full_pipeline(orch)
        # Intent needs 2 find_definition_id_by_name calls (source + destination)
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "sf-def-id"]
        tools = self._register(orch)
        await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="SF",
            destination_type="Snowflake",
            destination_profile="test_dest",
            connection_name="pg-to-sf",
            intent="all tables",
        )
        # If intent didn't match, still only 1 discover call
        assert orch.airbyte_client.discover_source_schema.await_count == 1

    @pytest.mark.asyncio
    async def test_select_streams_from_intent_single_discover(self):
        """select_streams_from_intent should call discover only once via cache."""
        orch = self._make_orchestrator()
        discovery = {
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "customers",
                            "namespace": "public",
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "json_schema": {"properties": {"id": {}, "name": {}}},
                        },
                        "config": {"supported_sync_modes": ["full_refresh", "incremental"]},
                    }
                ]
            }
        }
        orch.airbyte_client.discover_source_schema.return_value = discovery
        tools = self._register(orch)
        result = await tools["airbyte_inventory"](
            list_type="select_streams",
            source_id="s1",
            prompt="customer data",
        )
        assert result["success"] is True
        # Only 1 discover call (was 2 before: _build_stream_index + inline)
        assert orch.airbyte_client.discover_source_schema.await_count == 1


class TestConnectionReuseScheduleUpdate:
    """Tests for connection reuse when streams match but schedule differs."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {"host": "localhost"}
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _setup_reuse_scenario(
        self, orch, existing_schedule_type="manual", request_schedule_type="cron"
    ):
        """Set up mocks for a connection reuse scenario with matching streams."""
        orch.airbyte_client.find_definition_id_by_name.side_effect = ["pg-def-id", "td-def-id"]
        src_data = {
            "sourceId": "s1",
            "name": "PG",
            "sourceDefinitionId": "pg-def-id",
            "configuration": {"host": "h"},
        }
        orch.airbyte_client.list_source_definitions_registry.return_value = [
            {"sourceDefinitionId": "pg-def-id", "name": "Postgres"}
        ]
        orch.airbyte_client.get_source_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_sources.return_value = [src_data]
        orch.airbyte_client.get_source.return_value = src_data
        dst_data = {
            "destinationId": "d1",
            "name": "TD",
            "destinationDefinitionId": "td-def-id",
            "configuration": {"host": "td"},
        }
        orch.airbyte_client.list_destination_definitions_registry.return_value = [
            {"destinationDefinitionId": "td-def-id", "name": "Teradata"}
        ]
        orch.airbyte_client.get_destination_definition_specification.return_value = {
            "connectionSpecification": {
                "type": "object",
                "properties": {"host": {"type": "string"}},
            }
        }
        orch.airbyte_client.list_destinations.return_value = [dst_data]
        orch.airbyte_client.get_destination.return_value = dst_data
        orch.airbyte_client.discover_source_schema.return_value = {
            "catalog": {
                "streams": [
                    {
                        "stream": {"name": "customer", "json_schema": {"properties": {"id": {}}}},
                        "config": {},
                    },
                    {
                        "stream": {"name": "nation", "json_schema": {"properties": {"id": {}}}},
                        "config": {},
                    },
                ]
            }
        }
        orch.airbyte_client.build_configured_catalog.return_value = {
            "streams": [
                {
                    "stream": {"name": "customer"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                },
                {
                    "stream": {"name": "nation"},
                    "config": {"syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
                },
            ]
        }
        # Existing connection with matching streams
        orch.airbyte_client.list_connections.return_value = [
            {
                "connectionId": "c-existing",
                "name": "pg-td-conn",
                "sourceId": "s1",
                "destinationId": "d1",
            }
        ]
        orch.airbyte_client.get_connection.return_value = {
            "connectionId": "c-existing",
            "schedule": {"scheduleType": existing_schedule_type},
            "status": "active",
            "configurations": {
                "streams": [
                    {"name": "customer", "syncMode": "full_refresh_overwrite"},
                    {"name": "nation", "syncMode": "full_refresh_overwrite"},
                ]
            },
        }
        orch.airbyte_client.update_connection.return_value = {
            "connectionId": "c-existing",
            "status": "active",
        }

    @pytest.mark.asyncio
    async def test_reuse_with_same_schedule(self):
        """Connection with same streams AND same schedule should reuse without update."""
        orch = self._make_orchestrator()
        self._setup_reuse_scenario(
            orch, existing_schedule_type="manual", request_schedule_type="manual"
        )
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="TD",
            destination_type="Teradata",
            destination_profile="test_dest",
            streams=[
                {
                    "name": "customer",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                },
                {"name": "nation", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
            ],
            connection_name="pg-td-conn",
        )
        assert result["success"] is True
        assert result["connection_reused"] is True
        assert result["connection_id"] == "c-existing"
        # No schedule update should happen
        orch.airbyte_client.update_connection.assert_not_awaited()
        # No new connection should be created
        orch.airbyte_client.create_connection.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reuse_with_schedule_update_cron(self):
        """Connection with same streams but different schedule returns clarification (H3)."""
        orch = self._make_orchestrator()
        self._setup_reuse_scenario(orch, existing_schedule_type="manual")
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="TD",
            destination_type="Teradata",
            destination_profile="test_dest",
            streams=[
                {
                    "name": "customer",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                },
                {"name": "nation", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
            ],
            connection_name="pg-td-conn",
            schedule_type="cron",
            schedule_cron="0 2 * * *",
        )
        # H3: Schedule mismatch now returns clarification instead of silent mutation
        assert result["success"] is False
        assert result.get("clarification_needed") is True
        assert result["existing_connection_id"] == "c-existing"
        assert result["current_schedule"] == "manual"
        assert result["requested_schedule"] == "cron"
        # No silent update should happen
        orch.airbyte_client.update_connection.assert_not_awaited()
        # No new connection should be created
        orch.airbyte_client.create_connection.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reuse_reports_schedule_updated_flag(self):
        """Reused connection with schedule change returns clarification (H3)."""
        orch = self._make_orchestrator()
        self._setup_reuse_scenario(orch, existing_schedule_type="basic")
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="TD",
            destination_type="Teradata",
            destination_profile="test_dest",
            streams=[
                {
                    "name": "customer",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                },
                {"name": "nation", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
            ],
            connection_name="pg-td-conn",
            schedule_type="cron",
            schedule_cron="0 2 * * *",
        )
        # H3: Schedule mismatch now returns clarification
        assert result["success"] is False
        assert result.get("clarification_needed") is True
        assert result["current_schedule"] == "basic"
        assert result["requested_schedule"] == "cron"
        # No silent update
        orch.airbyte_client.update_connection.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_reuse_when_streams_differ(self):
        """Connection with different streams should NOT reuse — creates new connection."""
        orch = self._make_orchestrator()
        self._setup_reuse_scenario(orch, existing_schedule_type="manual")
        # Override: existing connection has only 'customer' stream, not 'nation'
        orch.airbyte_client.get_connection.return_value = {
            "connectionId": "c-existing",
            "scheduleType": "manual",
            "status": "active",
            "configurations": {
                "streams": [
                    {"name": "customer", "syncMode": "full_refresh_overwrite"},
                ]
            },
        }
        orch.airbyte_client.create_connection.return_value = {
            "connectionId": "c-new",
            "status": "active",
        }
        tools = self._register(orch)
        result = await tools["airbyte_pipeline"](
            action="create",
            source_name="PG",
            source_type="Postgres",
            source_profile="test_source",
            destination_name="TD",
            destination_type="Teradata",
            destination_profile="test_dest",
            streams=[
                {
                    "name": "customer",
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                },
                {"name": "nation", "syncMode": "full_refresh", "destinationSyncMode": "overwrite"},
            ],
            connection_name="pg-td-conn",
        )
        assert result["success"] is True
        # Streams don't match, so a new connection should be created
        orch.airbyte_client.create_connection.assert_awaited_once()
        # No schedule update should happen on the existing connection
        orch.airbyte_client.update_connection.assert_not_awaited()


# ============================================================================
# Hardening tests for data_movement.py changes (C1-C4, H1, M1, C2, C3/H2)
# ============================================================================


class TestCsvDagConnectionIdDerivation:
    """C4: Connection IDs derived from dag_id to avoid collisions."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "testdb"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = "secret"
        orch.settings.teradata.port = 1025
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"
        # Airflow client: single AsyncMock for both sync and async code paths
        airflow_client = AsyncMock()
        airflow_client.get_connection.return_value = {"conn_id": "ok"}
        orch.airflow_client = airflow_client
        orch.async_airflow_client = airflow_client
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _mock_csv_analysis(self):
        col = Mock()
        col.name = "id"
        col.inferred_teradata_type = "INTEGER"
        analysis = Mock()
        analysis.row_count = 100
        analysis.column_count = 1
        analysis.file_size_mb = 0.5
        analysis.delimiter = ","
        analysis.columns = [col]
        analysis.has_header = True
        return analysis

    @pytest.mark.asyncio
    async def test_conn_ids_derived_from_dag_id(self):
        """When teradata_conn_id/ssh_conn_id not provided, they default to td_{dag_id}/ssh_{dag_id}."""
        orch = self._make_orchestrator()
        checked_conn_ids = []

        async def tracking_get(*args, **kwargs):
            conn_id = kwargs.get("conn_id", args[0] if args else "")
            checked_conn_ids.append(conn_id)
            return {"conn_id": conn_id}

        orch.async_airflow_client.get_connection = AsyncMock(side_effect=tracking_get)
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "sales"
            mock_csv_file.name = "sales.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: (
                mock_csv_file if "sales" in str(x) else mock_dags_folder
            )

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/sales.csv",
                target_database="testdb",
                target_table="sales",
                dag_id="load_testdb_sales",
                teradata_profile="td_test",
            )
            assert result["success"] is True
            # C4: verify connection IDs derived from dag_id were checked
            assert "td_load_testdb_sales" in checked_conn_ids
            assert "ssh_load_testdb_sales" in checked_conn_ids
            # C2: warnings/connections_valid keys exist
            assert "connections_valid" in result
            assert "warnings" in result

    @pytest.mark.asyncio
    async def test_explicit_conn_ids_used(self):
        """When conn IDs explicitly provided, they are used as-is."""
        orch = self._make_orchestrator()
        checked_conn_ids = []

        async def tracking_get(*args, **kwargs):
            conn_id = kwargs.get("conn_id", args[0] if args else "")
            checked_conn_ids.append(conn_id)
            return {"conn_id": conn_id}

        orch.async_airflow_client.get_connection = AsyncMock(side_effect=tracking_get)
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                dag_id="my_dag",
                teradata_conn_id="my_custom_td_conn",
                ssh_conn_id="my_custom_ssh_conn",
                teradata_profile="td_test",
            )
            assert result["success"] is True
            # Explicit conn IDs should be used (not td_my_dag/ssh_my_dag)
            assert "my_custom_td_conn" in checked_conn_ids
            assert "my_custom_ssh_conn" in checked_conn_ids


class TestCsvDagConnectionWarnings:
    """C2: Connection failures tracked in warnings and connections_valid."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "testdb"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = "secret"
        orch.settings.teradata.port = 1025
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"
        # Airflow client: single AsyncMock for both sync and async code paths
        airflow_client = AsyncMock()
        airflow_client.list_connections.return_value = []
        orch.airflow_client = airflow_client
        orch.async_airflow_client = airflow_client
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _mock_csv_analysis(self):
        col = Mock()
        col.name = "id"
        col.inferred_teradata_type = "INTEGER"
        analysis = Mock()
        analysis.row_count = 100
        analysis.column_count = 1
        analysis.file_size_mb = 0.5
        analysis.delimiter = ","
        analysis.columns = [col]
        analysis.has_header = True
        return analysis

    @pytest.mark.asyncio
    async def test_td_conn_failure_sets_warnings(self):
        """When Teradata connection fails, connections_valid=False and warnings populated."""
        orch = self._make_orchestrator()
        # Both get_connection and create_connection fail for TD
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=AsyncAirflowAPIError("Not found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(side_effect=Exception("Auth error"))
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                dag_id="test_dag",
                teradata_profile="td_test",
            )
            # DAG still generated (C2: warnings instead of hard fail)
            assert result["success"] is True
            assert result["connections_valid"] is False
            assert len(result["warnings"]) >= 1
            assert any("Teradata connection" in w for w in result["warnings"])

    @pytest.mark.asyncio
    async def test_successful_conns_valid_true(self):
        """When all connections succeed, connections_valid=True and warnings empty."""
        orch = self._make_orchestrator()

        # Return matching credentials for both TD and SSH connections
        async def side_effect_get_conn(*args, **kwargs):
            conn_id = kwargs.get("conn_id") or (args[0] if args else "")
            if "ssh" in str(conn_id):
                return {"conn_id": conn_id, "host": "10.0.0.1", "login": "testuser", "port": 22}
            return {"conn_id": conn_id, "host": "td-host", "schema": "testdb", "login": "dbc"}

        orch.async_airflow_client.get_connection = AsyncMock(side_effect=side_effect_get_conn)
        orch.async_airflow_client.create_connection = AsyncMock()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ", {"MCP_CLIENT_SSH_HOST": "10.0.0.1", "MCP_CLIENT_SSH_USER": "testuser", "MCP_CLIENT_SSH_PASSWORD": "ssh-pass"}
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                dag_id="test_dag",
                teradata_profile="td_test",
            )
            assert result["success"] is True
            assert result["connections_valid"] is True
            assert result["warnings"] == []


class TestCsvDagSshHostRequired:
    """H1: MCP_CLIENT_SSH_HOST must be set explicitly."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "testdb"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = "secret"
        orch.settings.teradata.port = 1025
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"
        async_airflow_client = AsyncMock()
        async_airflow_client.list_connections.return_value = []
        async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        orch.airflow_client = async_airflow_client
        orch.async_airflow_client = async_airflow_client
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _mock_csv_analysis(self):
        col = Mock()
        col.name = "id"
        col.inferred_teradata_type = "INTEGER"
        analysis = Mock()
        analysis.row_count = 100
        analysis.column_count = 1
        analysis.file_size_mb = 0.5
        analysis.delimiter = ","
        analysis.columns = [col]
        analysis.has_header = True
        return analysis

    @pytest.mark.asyncio
    async def test_missing_ssh_host_produces_warning(self):
        """Without MCP_CLIENT_SSH_HOST, SSH connection creation fails and warning added."""
        orch = self._make_orchestrator()

        def side_effect_get_conn(*args, **kwargs):
            conn_id = kwargs.get("connection_id", args[0] if args else "")
            if "ssh" in str(conn_id):
                raise Exception("Not found")
            return {"conn_id": conn_id}

        orch.airflow_client.get_connection = side_effect_get_conn
        orch.airflow_client.create_connection = Mock()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
            # Clear all SSH env vars
            patch.dict("os.environ", {}, clear=True),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                dag_id="test_dag",
                teradata_profile="td_test",
            )
            # DAG generation succeeds but SSH warning present
            assert result["success"] is True
            assert result["connections_valid"] is False
            assert any("SSH connection" in w for w in result["warnings"])


class TestCsvDagSshPortValidation:
    """M1: MCP_CLIENT_SSH_PORT validated as integer in range."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "testdb"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = "secret"
        orch.settings.teradata.port = 1025
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"
        async_airflow_client = AsyncMock()
        async_airflow_client.list_connections.return_value = []
        async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        orch.airflow_client = async_airflow_client
        orch.async_airflow_client = async_airflow_client
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _mock_csv_analysis(self):
        col = Mock()
        col.name = "id"
        col.inferred_teradata_type = "INTEGER"
        analysis = Mock()
        analysis.row_count = 100
        analysis.column_count = 1
        analysis.file_size_mb = 0.5
        analysis.delimiter = ","
        analysis.columns = [col]
        analysis.has_header = True
        return analysis

    @pytest.mark.asyncio
    async def test_invalid_ssh_port_produces_warning(self):
        """Non-numeric SSH port causes SSH connection failure captured in warnings."""
        orch = self._make_orchestrator()

        def side_effect_get_conn(*args, **kwargs):
            conn_id = kwargs.get("connection_id", args[0] if args else "")
            if "ssh" in str(conn_id):
                raise Exception("Not found")
            return {"conn_id": conn_id}

        orch.airflow_client.get_connection = side_effect_get_conn
        orch.airflow_client.create_connection = Mock()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ",
                {
                    "MCP_CLIENT_SSH_HOST": "10.0.0.1",
                    "MCP_CLIENT_SSH_PORT": "not_a_number",
                    "MCP_CLIENT_SSH_USER": "testuser",
                },
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                dag_id="test_dag",
                teradata_profile="td_test",
            )
            # SSH port validation failure captured in warnings
            assert result["connections_valid"] is False
            assert any("SSH connection" in w for w in result["warnings"])

    @pytest.mark.asyncio
    async def test_port_out_of_range_produces_warning(self):
        """SSH port > 65535 causes validation failure."""
        orch = self._make_orchestrator()

        def side_effect_get_conn(*args, **kwargs):
            conn_id = kwargs.get("connection_id", args[0] if args else "")
            if "ssh" in str(conn_id):
                raise Exception("Not found")
            return {"conn_id": conn_id}

        orch.airflow_client.get_connection = side_effect_get_conn
        orch.airflow_client.create_connection = Mock()
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ",
                {
                    "MCP_CLIENT_SSH_HOST": "10.0.0.1",
                    "MCP_CLIENT_SSH_PORT": "99999",
                    "MCP_CLIENT_SSH_USER": "testuser",
                },
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                dag_id="test_dag",
                teradata_profile="td_test",
            )
            assert result["connections_valid"] is False
            assert any("SSH connection" in w for w in result["warnings"])


class TestCsvDagStrictSsh:
    """C1: strict_ssh parameter controls SSH host-key verification."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "testdb"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = "secret"
        orch.settings.teradata.port = 1025
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"
        # Airflow client: single AsyncMock for both sync and async code paths
        airflow_client = AsyncMock()
        airflow_client.list_connections.return_value = []
        orch.airflow_client = airflow_client
        orch.async_airflow_client = airflow_client
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    def _mock_csv_analysis(self):
        col = Mock()
        col.name = "id"
        col.inferred_teradata_type = "INTEGER"
        analysis = Mock()
        analysis.row_count = 100
        analysis.column_count = 1
        analysis.file_size_mb = 0.5
        analysis.delimiter = ","
        analysis.columns = [col]
        analysis.has_header = True
        return analysis

    @pytest.mark.asyncio
    async def test_strict_ssh_default_no_host_key_check_false(self):
        """By default (strict_ssh=True), no_host_key_check is False in SSH extra."""
        orch = self._make_orchestrator()
        created_connections = {}

        async def side_effect_get_conn(*args, **kwargs):
            conn_id = kwargs.get("conn_id", args[0] if args else "")
            if "ssh" in str(conn_id):
                raise AsyncAirflowAPIError("Not found")
            return {"conn_id": conn_id, "host": "td-host", "schema": "testdb", "login": "dbc"}

        async def side_effect_create_conn(**kwargs):
            created_connections[kwargs.get("conn_id", "")] = kwargs

        orch.async_airflow_client.get_connection = AsyncMock(side_effect=side_effect_get_conn)
        orch.async_airflow_client.create_connection = AsyncMock(side_effect=side_effect_create_conn)
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ",
                {
                    "MCP_CLIENT_SSH_HOST": "10.0.0.1",
                    "MCP_CLIENT_SSH_PORT": "22",
                    "MCP_CLIENT_SSH_USER": "testuser",
                    "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
                },
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                dag_id="test_dag",
                # strict_ssh defaults to True,
                teradata_profile="td_test",
            )
            assert result["success"] is True

            # Find the SSH connection creation call
            ssh_calls = [k for k in created_connections if "ssh" in k]
            assert len(ssh_calls) > 0, "SSH connection should have been created"
            ssh_conn = created_connections[ssh_calls[0]]
            assert ssh_conn["extra"]["no_host_key_check"] is False

    @pytest.mark.asyncio
    async def test_strict_ssh_false_allows_no_host_key_check(self):
        """strict_ssh=False sets no_host_key_check=True in SSH extra."""
        orch = self._make_orchestrator()
        created_connections = {}

        async def side_effect_get_conn(*args, **kwargs):
            conn_id = kwargs.get("conn_id", args[0] if args else "")
            if "ssh" in str(conn_id):
                raise AsyncAirflowAPIError("Not found")
            return {"conn_id": conn_id, "host": "td-host", "schema": "testdb", "login": "dbc"}

        async def side_effect_create_conn(**kwargs):
            created_connections[kwargs.get("conn_id", "")] = kwargs

        orch.async_airflow_client.get_connection = AsyncMock(side_effect=side_effect_get_conn)
        orch.async_airflow_client.create_connection = AsyncMock(side_effect=side_effect_create_conn)
        tools = self._register(orch)
        mock_analysis = self._mock_csv_analysis()

        with (
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch(
                "elt_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=mock_analysis,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_file_loading_dag",
                return_value="# DAG",
            ),
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ",
                {
                    "MCP_CLIENT_SSH_HOST": "10.0.0.1",
                    "MCP_CLIENT_SSH_PORT": "22",
                    "MCP_CLIENT_SSH_USER": "testuser",
                    "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
                },
            ),
        ):
            mock_csv_file = Mock()
            mock_csv_file.exists.return_value = True
            mock_csv_file.stem = "data"
            mock_csv_file.name = "data.csv"
            mock_dags_folder = Mock()
            mock_dags_folder.__truediv__ = Mock(
                return_value=Mock(__str__=Mock(return_value="/tmp/dags/test.py"))
            )
            MockPath.side_effect = lambda x: mock_csv_file if "data" in str(x) else mock_dags_folder

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="csv_dag",
                csv_path="/data/data.csv",
                target_database="testdb",
                target_table="tbl",
                dag_id="test_dag",
                strict_ssh=False,
                teradata_profile="td_test",
            )
            assert result["success"] is True

            ssh_calls = [k for k in created_connections if "ssh" in k]
            assert len(ssh_calls) > 0, "SSH connection should have been created"
            ssh_conn = created_connections[ssh_calls[0]]
            assert ssh_conn["extra"]["no_host_key_check"] is True


class TestEnsureTeradataConnection:
    """C3/H2: _ensure_teradata_connection raises RuntimeError on creation failure."""

    def _make_orchestrator(self):
        orch = Mock()
        client = AsyncMock()
        orch.airbyte_client = client
        client._get_workspace_id.return_value = "ws1"
        orch.settings = Mock()
        orch.settings.teradata = Mock()
        orch.settings.teradata.database = "testdb"
        orch.settings.teradata.host = "td-host"
        orch.settings.teradata.username = "dbc"
        orch.settings.teradata.password = "secret"
        orch.settings.teradata.port = 1025
        orch.settings.pipeline = Mock()
        orch.settings.pipeline.dags_output_dir = "/tmp/dags"
        # Airflow client: single AsyncMock for both sync and async code paths
        airflow_client = AsyncMock()
        airflow_client.list_connections.return_value = []
        orch.airflow_client = airflow_client
        orch.async_airflow_client = airflow_client
        resolver = Mock()
        resolver.guard_configured.return_value = None
        resolver.resolve_profile.return_value = {
            "host": "td-host",
            "username": "dbc",
            "password": "secret",
            "port": 1025,
            "database": "testdb",
        }
        orch.credential_resolver = resolver
        return orch

    def _register(self, orch):
        return register_data_movement_tools(orch)

    @pytest.mark.asyncio
    async def test_conn_creation_failure_returns_error(self):
        """_ensure_teradata_connection raises RuntimeError → propagates as success=False."""
        orch = self._make_orchestrator()
        orch.async_airflow_client.get_connection.side_effect = AsyncAirflowAPIError("Not found")
        orch.async_airflow_client.create_connection.side_effect = Exception("Permission denied")
        tools = self._register(orch)

        with patch("asyncio.to_thread") as mock_thread:

            async def thread_side_effect(fn, *args, **kwargs):
                return fn(*args, **kwargs)

            mock_thread.side_effect = thread_side_effect

            result = await tools["airflow_teradata_load"](
                method="table_transfer",
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
                dag_id="transfer_test",
            )
            assert result["success"] is False
            assert "error" in result

    @pytest.mark.asyncio
    async def test_existing_conn_with_mismatch_still_succeeds(self):
        """Existing connection with mismatched config: creates new incremented conn ID."""
        orch = self._make_orchestrator()

        # Return mismatched connection for base IDs, 404 for incremented IDs
        async def side_effect_get_conn(*args, **kwargs):
            conn_id = kwargs.get("conn_id") or (args[0] if args else "")
            # Base teradata IDs return mismatched config
            if conn_id in ("teradata_source", "teradata_target"):
                return {
                    "conn_id": conn_id,
                    "host": "different-host",
                    "schema": "different_db",
                    "login": "different_user",
                }
            # Incremented IDs and SSH return 404 (will be created)
            raise AsyncAirflowAPIError("Not found")

        orch.async_airflow_client.get_connection = AsyncMock(side_effect=side_effect_get_conn)
        orch.async_airflow_client.create_connection = AsyncMock()
        orch.teradata_client = Mock()
        orch.teradata_client.get_table_metadata = Mock(
            return_value={
                "columns": [{"name": "id", "type": "I"}],
                "row_count": 100,
            }
        )
        tools = self._register(orch)

        with (
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.__init__",
                return_value=None,
            ),
            patch(
                "elt_mcp_server.generators.airflow_tdload_dag_generator.AirflowTdLoadDAGGenerator.generate_table_transfer_dag",
                return_value="# DAG",
            ),
            patch("elt_mcp_server.tools.data_movement.Path") as MockPath,
            patch("asyncio.to_thread") as mock_thread,
            patch.dict(
                "os.environ", {"MCP_CLIENT_SSH_HOST": "10.0.0.1", "MCP_CLIENT_SSH_USER": "testuser", "MCP_CLIENT_SSH_PASSWORD": "ssh-pass"}
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
                source_database="src_db",
                source_table="src_tbl",
                target_database="tgt_db",
                target_table="tgt_tbl",
                dag_id="transfer_test",
                source_teradata_profile="src_test",
                target_teradata_profile="tgt_test",
            )
            # Should succeed — mismatch causes new incremented conn ID creation
            assert result["success"] is True


# ===========================================================================
# Hardening tests (merged from test_airbyte_client_hardening.py)
# ===========================================================================

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client(**overrides) -> AirbyteClient:
    """Create an AirbyteClient with sensible test defaults."""
    defaults = {
        "base_url": "http://localhost:8000",
        "workspace_id": "ws-test",
        "circuit_breaker_enabled": False,
        "rate_limit_rps": None,
    }
    defaults.update(overrides)
    return AirbyteClient(**defaults)


def _http_response(
    status_code: int = 200,
    json_data: dict | None = None,
    content: bytes = b"",
    headers: dict | None = None,
) -> httpx.Response:
    """Build a real httpx.Response (not a mock) so raise_for_status() works."""
    if json_data is not None:
        import json as _json

        content = _json.dumps(json_data).encode()
        headers = headers or {}
        headers.setdefault("Content-Type", "application/json")

    resp = httpx.Response(
        status_code=status_code,
        content=content,
        headers=headers or {},
        request=httpx.Request("GET", "http://test"),
    )
    return resp


# ===========================================================================
# TestRateLimiter (Hardening)
# ===========================================================================


class TestRateLimiterHardening:
    """Tests for the Airbyte RateLimiter dataclass."""

    @pytest.mark.asyncio
    async def test_acquire_token_immediately(self):
        limiter = RateLimiter(rate=10.0, burst=5)
        result = await limiter.acquire(timeout=1.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_depletes_tokens(self):
        limiter = RateLimiter(rate=10.0, burst=3)
        for _ in range(3):
            await limiter.acquire(timeout=1.0)
        status = limiter.get_status()
        assert status["available_tokens"] < 1

    @pytest.mark.asyncio
    async def test_acquire_timeout_raises(self):
        limiter = RateLimiter(rate=0.01, burst=1)
        await limiter.acquire()
        limiter._tokens = 0
        with pytest.raises(AirbyteRateLimitExceeded):
            await limiter.acquire(timeout=0.05)

    @pytest.mark.asyncio
    async def test_token_refill(self):
        limiter = RateLimiter(rate=1000.0, burst=5)
        for _ in range(5):
            await limiter.acquire(timeout=1.0)
        # Tokens depleted — simulate time passing
        limiter._last_update = time.monotonic() - 1.0  # 1s ago
        result = await limiter.acquire(timeout=0.01)
        assert result is True


# ===========================================================================
# TestAirbyteClientInit (Hardening)
# ===========================================================================


class TestAirbyteClientInitHardening:
    """Tests for AirbyteClient initialization with hardening params."""

    def test_default_hardening_settings(self):
        """Test production defaults using direct construction (no _make_client overrides)."""
        client = AirbyteClient(base_url="http://localhost:8000", workspace_id="ws-test")
        assert client._retry_attempts == 3
        assert client._retry_backoff == 1.0
        assert client._max_response_size_bytes == 10 * 1024 * 1024
        assert client._rate_limiter is not None  # rate limiter enabled by default
        assert client._circuit_breaker is not None  # circuit breaker enabled by default

    def test_custom_hardening_settings(self):
        client = _make_client(
            retry_attempts=5,
            retry_backoff=2.0,
            max_response_size_bytes=1024,
            rate_limit_rps=20.0,
            rate_limit_burst=15,
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=10,
            circuit_breaker_timeout=120.0,
            max_connections=50,
            max_keepalive_connections=10,
        )
        assert client._retry_attempts == 5
        assert client._retry_backoff == 2.0
        assert client._max_response_size_bytes == 1024
        assert client._rate_limiter is not None
        assert client._rate_limiter.rate == 20.0
        assert client._rate_limiter.burst == 15
        assert client._circuit_breaker is not None
        assert client._max_connections == 50
        assert client._max_keepalive_connections == 10

    def test_invalid_retry_backoff_raises(self):
        with pytest.raises(ValueError, match="retry_backoff must be positive"):
            _make_client(retry_backoff=0)
        with pytest.raises(ValueError, match="retry_backoff must be positive"):
            _make_client(retry_backoff=-1.0)

    def test_invalid_max_response_size_raises(self):
        with pytest.raises(ValueError, match="max_response_size_bytes must be positive"):
            _make_client(max_response_size_bytes=0)
        with pytest.raises(ValueError, match="max_response_size_bytes must be positive"):
            _make_client(max_response_size_bytes=-100)

    def test_disable_rate_limiter(self):
        client = _make_client(rate_limit_rps=None)
        assert client._rate_limiter is None

        client2 = _make_client(rate_limit_rps=0)
        assert client2._rate_limiter is None

    def test_backward_compatible_kwargs(self):
        """Existing callers using only positional/old keyword args still work."""
        client = AirbyteClient(
            url="http://localhost:8000",
            username="airbyte",
            password="password",
            workspace_id="ws1",
        )
        assert client.base_url == "http://localhost:8000"
        assert client._retry_attempts == 3  # default


# ===========================================================================
# TestResponseSizeLimit (Hardening)
# ===========================================================================


class TestResponseSizeLimitHardening:
    """Tests for response size limit enforcement."""

    @pytest.mark.asyncio
    async def test_response_too_large_raises(self):
        client = _make_client(max_response_size_bytes=100)
        large_body = b"x" * 200
        mock_resp = _http_response(
            status_code=200,
            content=large_body,
            headers={"Content-Type": "text/plain"},
        )
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=mock_resp)
        client._client = mock_http

        with pytest.raises(AirbyteResponseTooLarge, match="200 bytes exceeds limit"):
            await client._make_request("GET", "/test")

    @pytest.mark.asyncio
    async def test_response_within_limit_ok(self):
        client = _make_client(max_response_size_bytes=1000)
        mock_resp = _http_response(status_code=200, json_data={"ok": True})
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=mock_resp)
        client._client = mock_http

        result = await client._make_request("GET", "/test")
        assert result == {"ok": True}


# ===========================================================================
# TestRetryLogic (Hardening)
# ===========================================================================


class TestRetryLogicHardening:
    """Tests for retry behavior in _make_request."""

    @pytest.mark.asyncio
    async def test_503_get_retried(self):
        """GET on 503 should be retried up to retry_attempts times."""
        client = _make_client(retry_attempts=3, retry_backoff=0.01)
        fail_resp = _http_response(status_code=503, content=b"unavailable")
        ok_resp = _http_response(status_code=200, json_data={"ok": True})

        mock_http = AsyncMock()
        mock_http.request = AsyncMock(side_effect=[fail_resp, ok_resp])
        client._client = mock_http

        result = await client._make_request("GET", "/test")
        assert result == {"ok": True}
        assert mock_http.request.call_count == 2

    @pytest.mark.asyncio
    async def test_retry_exhausted_raises(self):
        """All retries exhausted on 503 GET should raise via raise_for_status."""
        client = _make_client(retry_attempts=2, retry_backoff=0.01)
        fail_resp = _http_response(status_code=503, content=b"unavailable")
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=fail_resp)
        client._client = mock_http

        # After 2 attempts with 503, the last attempt calls raise_for_status
        # which raises HTTPStatusError, caught and wrapped as AirbyteAPIError
        with pytest.raises(AirbyteAPIError) as exc_info:
            await client._make_request("GET", "/test")
        assert mock_http.request.call_count == 2
        # Verify the error chains from HTTPStatusError (raise_for_status path)
        assert isinstance(exc_info.value.__cause__, HTTPStatusError)
        assert exc_info.value.__cause__.response.status_code == 503

    @pytest.mark.asyncio
    async def test_post_not_retried_on_502(self):
        """POST should NOT be retried on 502 (not idempotent)."""
        client = _make_client(retry_attempts=3, retry_backoff=0.01)
        fail_resp = _http_response(status_code=502, content=b"bad gateway")
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=fail_resp)
        client._client = mock_http

        with pytest.raises(AirbyteAPIError) as exc_info:
            await client._make_request("POST", "/test", json={"data": 1})
        # Only one attempt — no retry for POST; error flows through raise_for_status
        assert mock_http.request.call_count == 1
        assert isinstance(exc_info.value.__cause__, HTTPStatusError)
        assert exc_info.value.__cause__.response.status_code == 502

    @pytest.mark.asyncio
    async def test_post_lowercase_not_retried_on_502(self):
        """Lowercase post should NOT be retried on 502 (case normalization)."""
        client = _make_client(retry_attempts=3, retry_backoff=0.01)
        fail_resp = _http_response(status_code=502, content=b"bad gateway")
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=fail_resp)
        client._client = mock_http

        with pytest.raises(AirbyteAPIError) as exc_info:
            await client._make_request("post", "/test", json={"data": 1})
        assert mock_http.request.call_count == 1
        assert isinstance(exc_info.value.__cause__, HTTPStatusError)

    @pytest.mark.asyncio
    async def test_post_mixed_case_not_retried_on_502(self):
        """Mixed-case Post should NOT be retried on 502 (case normalization)."""
        client = _make_client(retry_attempts=3, retry_backoff=0.01)
        fail_resp = _http_response(status_code=502, content=b"bad gateway")
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=fail_resp)
        client._client = mock_http

        with pytest.raises(AirbyteAPIError) as exc_info:
            await client._make_request("Post", "/test", json={"data": 1})
        assert mock_http.request.call_count == 1
        assert isinstance(exc_info.value.__cause__, HTTPStatusError)

    @pytest.mark.asyncio
    async def test_429_retried_regardless_of_method(self):
        """429 should be retried even for POST."""
        client = _make_client(retry_attempts=3, retry_backoff=0.01)
        rate_limited_resp = _http_response(
            status_code=429,
            content=b"too many requests",
            headers={"Retry-After": "0.01"},
        )
        ok_resp = _http_response(status_code=200, json_data={"created": True})

        mock_http = AsyncMock()
        mock_http.request = AsyncMock(side_effect=[rate_limited_resp, ok_resp])
        client._client = mock_http

        result = await client._make_request("POST", "/test", json={"data": 1})
        assert result == {"created": True}
        assert mock_http.request.call_count == 2


# ===========================================================================
# TestCircuitBreaker (Hardening)
# ===========================================================================


class TestCircuitBreakerHardening:
    """Tests for circuit breaker integration in _make_request."""

    @pytest.mark.asyncio
    async def test_open_circuit_raises(self):
        """Request should fail immediately when circuit breaker is open."""
        client = _make_client(circuit_breaker_enabled=True)
        # Force circuit breaker open
        for _ in range(10):
            client._circuit_breaker.record_failure()

        with pytest.raises(CircuitBreakerOpen, match="circuit breaker is open"):
            await client._make_request("GET", "/test")

    @pytest.mark.asyncio
    async def test_record_success_on_ok(self):
        """Successful request should record success on circuit breaker."""
        client = _make_client(circuit_breaker_enabled=True)
        mock_resp = _http_response(status_code=200, json_data={"ok": True})
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=mock_resp)
        client._client = mock_http

        cb_mock = MagicMock()
        cb_mock.is_available = True
        client._circuit_breaker = cb_mock

        await client._make_request("GET", "/test")
        cb_mock.record_success.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [400, 401, 403, 404, 429])
    async def test_4xx_not_counted_as_failure(self, status_code):
        """Client errors (4xx) should NOT be recorded as circuit breaker failures."""
        client = _make_client(circuit_breaker_enabled=True)
        resp = _http_response(status_code=status_code, json_data={"message": "err"})
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=resp)
        client._client = mock_http

        cb_mock = MagicMock()
        cb_mock.is_available = True
        client._circuit_breaker = cb_mock

        with pytest.raises(AirbyteAPIError):
            await client._make_request("GET", "/test")

        cb_mock.record_failure.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [500, 502, 503])
    async def test_5xx_counted_as_failure(self, status_code):
        """Server errors (5xx) SHOULD be recorded as circuit breaker failures."""
        client = _make_client(circuit_breaker_enabled=True)
        resp = _http_response(status_code=status_code, json_data={"message": "err"})
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=resp)
        client._client = mock_http

        cb_mock = MagicMock()
        cb_mock.is_available = True
        client._circuit_breaker = cb_mock

        with pytest.raises(AirbyteAPIError):
            await client._make_request("GET", "/test")

        cb_mock.record_failure.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_error_records_failure_once(self):
        """Circuit breaker failure should be recorded once per request, not per attempt."""
        client = _make_client(
            retry_attempts=3,
            retry_backoff=0.01,
            circuit_breaker_enabled=True,
        )
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
        client._client = mock_http

        cb_mock = MagicMock()
        cb_mock.is_available = True
        client._circuit_breaker = cb_mock

        with pytest.raises(AirbyteConnectionError):
            await client._make_request("GET", "/test")

        # 3 attempts, but only 1 failure recorded (after retries exhausted)
        assert mock_http.request.call_count == 3
        cb_mock.record_failure.assert_called_once()


# ===========================================================================
# TestClientInitLock (Hardening)
# ===========================================================================


class TestClientInitLockHardening:
    """Tests for race-safe client initialization."""

    @pytest.mark.asyncio
    async def test_concurrent_get_client_creates_one(self):
        """Multiple concurrent _get_client calls should create only one httpx client."""
        client = _make_client()
        assert client._client is None

        try:
            results = await asyncio.gather(
                client._get_client(),
                client._get_client(),
                client._get_client(),
            )
            # All should return the same instance
            assert results[0] is results[1] is results[2]
            assert client._client is not None
        finally:
            await client.close()


# ===========================================================================
# TestConnectionPoolLimits (Hardening)
# ===========================================================================


class TestConnectionPoolLimitsHardening:
    """Tests for httpx connection pool limits."""

    @pytest.mark.asyncio
    async def test_pool_limits_configured(self):
        client = _make_client(max_connections=50, max_keepalive_connections=10)
        try:
            with patch(
                "elt_mcp_server.clients.airbyte_client.httpx.AsyncClient"
            ) as mock_async_client:
                mock_client_instance = AsyncMock()
                mock_async_client.return_value = mock_client_instance

                http_client = await client._get_client()

                assert http_client is mock_client_instance
                _, kwargs = mock_async_client.call_args
                limits = kwargs.get("limits")
                assert isinstance(limits, httpx.Limits)
                assert limits.max_connections == 50
                assert limits.max_keepalive_connections == 10
        finally:
            await client.close()


# ===========================================================================
# TestSafeErrorMessage (Hardening)
# ===========================================================================


class TestSafeErrorMessageHardening:
    """Tests for safe_error_message usage in _make_request."""

    @pytest.mark.asyncio
    async def test_api_error_uses_safe_error_message(self):
        """API errors should use safe_error_message to sanitize credentials."""
        client = _make_client()
        # 500 error with credential in detail body
        resp_500 = _http_response(
            status_code=500,
            json_data={"message": "Internal error with password=secret123"},
        )
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=resp_500)
        client._client = mock_http

        with pytest.raises(AirbyteAPIError) as exc_info:
            await client._make_request("GET", "/test")
        error_text = str(exc_info.value)
        # Status code should be present
        assert "500" in error_text
        # Credential value must NOT leak through
        assert "secret123" not in error_text

    @pytest.mark.asyncio
    async def test_connection_error_sanitized(self):
        """Connection errors should use safe_error_message."""
        client = _make_client(retry_attempts=1)
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(
            side_effect=httpx.ConnectError("Connection to secret-host:5432 failed")
        )
        client._client = mock_http

        with pytest.raises(AirbyteConnectionError) as exc_info:
            await client._make_request("GET", "/test")
        assert "Cannot connect to Airbyte" in str(exc_info.value)


# ===========================================================================
# TestSilentExceptionLogging (Hardening)
# ===========================================================================


class TestSilentExceptionLoggingHardening:
    """Tests that previously silent exception handlers now log warnings."""

    @pytest.mark.asyncio
    async def test_get_source_id_logs_warning(self, caplog):
        client = _make_client()
        client._client = AsyncMock()
        with patch.object(client, "list_workspaces", new_callable=AsyncMock) as mock_ws:
            mock_ws.side_effect = RuntimeError("connection lost")
            with caplog.at_level(logging.WARNING):
                result = await client.get_source_id("my-source")
            assert result is None
            assert "Error searching for source" in caplog.text

    @pytest.mark.asyncio
    async def test_get_destination_id_logs_warning(self, caplog):
        client = _make_client()
        client._client = AsyncMock()
        with patch.object(client, "list_workspaces", new_callable=AsyncMock) as mock_ws:
            mock_ws.side_effect = RuntimeError("connection lost")
            with caplog.at_level(logging.WARNING):
                result = await client.get_destination_id("my-dest")
            assert result is None
            assert "Error searching for destination" in caplog.text

    @pytest.mark.asyncio
    async def test_get_connection_id_logs_warning(self, caplog):
        client = _make_client()
        client._client = AsyncMock()
        with patch.object(client, "list_workspaces", new_callable=AsyncMock) as mock_ws:
            mock_ws.side_effect = RuntimeError("connection lost")
            with caplog.at_level(logging.WARNING):
                result = await client.get_connection_id("my-conn")
            assert result is None
            assert "Error searching for connection" in caplog.text


# ===========================================================================
# TestDeadCodeRemoval (Hardening)
# ===========================================================================


class TestDeadCodeRemovalHardening:
    """Test that dead code has been removed from list_jobs."""

    @pytest.mark.asyncio
    async def test_list_jobs_uses_only_params(self):
        """list_jobs should send query params, not a JSON body."""
        client = _make_client()
        mock_resp = _http_response(status_code=200, json_data={"data": [{"jobId": 1}]})
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=mock_resp)
        client._client = mock_http

        await client.list_jobs(config_type="sync", config_id="conn-123")

        call_kwargs = mock_http.request.call_args
        # Should have params, NOT json body
        assert "params" in call_kwargs.kwargs
        assert "json" not in call_kwargs.kwargs


# ===========================================================================
# TestWaitForJobMonotonic (Hardening)
# ===========================================================================


class TestWaitForJobMonotonicHardening:
    """Test that wait_for_job uses time.monotonic instead of time.time."""

    @pytest.mark.asyncio
    async def test_uses_monotonic(self):
        """wait_for_job should use time.monotonic for timeout tracking."""
        client = _make_client()
        mock_resp = _http_response(
            status_code=200,
            json_data={"status": "succeeded", "jobId": 42},
        )
        mock_http = AsyncMock()
        mock_http.request = AsyncMock(return_value=mock_resp)
        client._client = mock_http

        with patch("elt_mcp_server.clients.airbyte_client.time") as mock_time:
            mock_time.monotonic = MagicMock(side_effect=[100.0, 100.5])
            # time.time should NOT be called for timeout logic
            mock_time.time = MagicMock(side_effect=AssertionError("should use monotonic"))

            result = await client.wait_for_job(job_id=42, timeout=60)
            assert result["status"] == "succeeded"
            assert mock_time.monotonic.call_count >= 1


# ===========================================================================
# TestGetClientStatus (Hardening)
# ===========================================================================


class TestGetClientStatusHardening:
    """Tests for observability methods."""

    def test_get_client_status_keys(self):
        client = _make_client(
            rate_limit_rps=10.0,
            circuit_breaker_enabled=True,
        )
        status = client.get_client_status()
        expected_keys = {
            "base_url",
            "retry_attempts",
            "retry_backoff",
            "max_response_size_bytes",
            "max_connections",
            "max_keepalive_connections",
            "rate_limiter",
            "circuit_breaker",
            "client_initialized",
        }
        assert set(status.keys()) == expected_keys
        assert status["base_url"] == "http://localhost:8000"
        assert status["client_initialized"] is False
        assert status["rate_limiter"] is not None
        assert status["circuit_breaker"] is not None

    @pytest.mark.asyncio
    async def test_close_clears_client_initialized(self):
        """After close(), client_initialized should be False."""
        client = _make_client()
        mock_http = AsyncMock()
        mock_http.aclose = AsyncMock()
        client._client = mock_http

        # Before close, client_initialized is True
        assert client.get_client_status()["client_initialized"] is True

        await client.close()

        # After close, client_initialized is False
        assert client.get_client_status()["client_initialized"] is False
        assert client._client is None
