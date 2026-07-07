"""Live integration tests for AsyncAirflowClient.

These tests hit a real Airflow instance defined via environment variables.

Non-destructive only: version, health, list, and read-only introspection.

Skip by default unless AIRFLOW_BASE_URL is set and reachable.
"""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncGenerator

import pytest

from teradata_etl_mcp_server.clients.async_airflow_client import (
    AsyncAirflowClient,
    AsyncAirflowClientError,
)


def _host_is_reachable(host: str, port: int, timeout: float = 2.0) -> bool:
    """Check if a host:port is reachable."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _should_run_live() -> bool:
    """Determine if live tests should run."""
    url = os.getenv("AIRFLOW_BASE_URL")
    if not url:
        return False
    try:
        import urllib.parse as up

        p = up.urlparse(url)
        if not p.hostname:
            return False
        port = p.port or (443 if p.scheme == "https" else 80)
        return _host_is_reachable(p.hostname, port)
    except Exception:
        return False


pytestmark = pytest.mark.integration

live = pytest.mark.skipif(
    not _should_run_live(),
    reason="Live Airflow not reachable or AIRFLOW_BASE_URL not configured",
)


@pytest.fixture(scope="function")
async def async_live_client() -> AsyncGenerator[AsyncAirflowClient, None]:
    """Create AsyncAirflowClient for live testing."""
    base_url = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
    username = os.getenv("AIRFLOW_USERNAME", "admin")
    password = os.getenv("AIRFLOW_PASSWORD", "admin")
    auth_manager = os.getenv("AIRFLOW_AUTH_MANAGER", "basic")

    client = AsyncAirflowClient(
        base_url=base_url,
        username=username,
        password=password,
        auth_manager=auth_manager,
        timeout=30,
        rate_limit_rps=10.0,
        rate_limit_burst=20,
        max_connections=50,
        max_response_size_bytes=10 * 1024 * 1024,
        circuit_breaker_enabled=True,
        circuit_breaker_threshold=5,
        circuit_breaker_timeout=30.0,
    )
    yield client
    await client.close()


class TestAsyncAirflowClientLiveBasics:
    """Basic connectivity and health tests for AsyncAirflowClient."""

    @live
    @pytest.mark.asyncio
    async def test_test_connection(self, async_live_client: AsyncAirflowClient):
        """Test connection to Airflow."""
        result = await async_live_client.test_connection()
        assert isinstance(result, dict)
        assert result.get("connected") is True

    @live
    @pytest.mark.asyncio
    async def test_connection_returns_version(self, async_live_client: AsyncAirflowClient):
        """Test that connection result includes API version."""
        result = await async_live_client.test_connection()
        assert "version" in result
        assert result["version"] in ("v1", "v2")

    @live
    @pytest.mark.asyncio
    async def test_connection_returns_health(self, async_live_client: AsyncAirflowClient):
        """Test that connection result includes health status."""
        result = await async_live_client.test_connection()
        assert "health" in result
        # Health should have component status
        health = result["health"]
        assert isinstance(health, dict)

    @live
    @pytest.mark.asyncio
    async def test_client_status(self, async_live_client: AsyncAirflowClient):
        """Test getting client status (sync method)."""
        status = async_live_client.get_client_status()
        assert isinstance(status, dict)
        assert "base_url" in status
        assert "rate_limiter" in status
        assert "circuit_breaker" in status


class TestAsyncAirflowClientLiveDAGs:
    """DAG listing and introspection tests."""

    @live
    @pytest.mark.asyncio
    async def test_list_dags(self, async_live_client: AsyncAirflowClient):
        """Test listing DAGs."""
        dags = await async_live_client.list_dags(limit=10)
        assert isinstance(dags, list)

    @live
    @pytest.mark.asyncio
    async def test_list_dags_with_pagination(self, async_live_client: AsyncAirflowClient):
        """Test listing DAGs with pagination."""
        page1 = await async_live_client.list_dags(limit=5, offset=0)
        assert isinstance(page1, list)
        assert len(page1) <= 5

    @live
    @pytest.mark.asyncio
    async def test_get_dag_if_exists(self, async_live_client: AsyncAirflowClient):
        """Test getting a specific DAG if any exist."""
        dags = await async_live_client.list_dags(limit=1)
        if not dags:
            pytest.skip("No DAGs available to inspect")

        dag_id = dags[0].get("dag_id")
        if dag_id:
            dag = await async_live_client.get_dag(dag_id)
            assert dag.get("dag_id") == dag_id


class TestAsyncAirflowClientLiveDAGRuns:
    """DAG run listing and introspection tests."""

    @live
    @pytest.mark.asyncio
    async def test_list_dag_runs(self, async_live_client: AsyncAirflowClient):
        """Test listing DAG runs for existing DAGs."""
        dags = await async_live_client.list_dags(limit=5)
        if not dags:
            pytest.skip("No DAGs available to inspect runs")

        dag_id = dags[0].get("dag_id")
        runs = await async_live_client.list_dag_runs(dag_id, limit=5)
        assert isinstance(runs, list)

    @live
    @pytest.mark.asyncio
    async def test_get_dag_run_if_exists(self, async_live_client: AsyncAirflowClient):
        """Test getting a specific DAG run if any exist."""
        dags = await async_live_client.list_dags(limit=5)
        if not dags:
            pytest.skip("No DAGs available")

        for dag in dags:
            dag_id = dag.get("dag_id")
            runs = await async_live_client.list_dag_runs(dag_id, limit=1)
            if runs:
                run_id = runs[0].get("dag_run_id")
                if run_id:
                    run = await async_live_client.get_dag_run(dag_id, run_id)
                    assert run.get("dag_run_id") == run_id
                    return

        pytest.skip("No DAG runs found to inspect")


class TestAsyncAirflowClientLiveConnections:
    """Connection listing tests (read-only)."""

    @live
    @pytest.mark.asyncio
    async def test_list_connections(self, async_live_client: AsyncAirflowClient):
        """Test listing Airflow connections."""
        try:
            connections = await async_live_client.list_connections()
            assert isinstance(connections, list)
        except AsyncAirflowClientError:
            pytest.skip("Connections endpoint may be restricted")


class TestAsyncAirflowClientLiveRateLimiter:
    """Tests for rate limiter functionality."""

    @live
    @pytest.mark.asyncio
    async def test_rate_limiter_status(self, async_live_client: AsyncAirflowClient):
        """Test getting rate limiter status."""
        status = async_live_client.get_rate_limiter_status()
        if status:
            assert "rate_rps" in status
            assert "burst" in status


class TestAsyncAirflowClientLiveCircuitBreaker:
    """Tests for circuit breaker functionality."""

    @live
    @pytest.mark.asyncio
    async def test_circuit_breaker_status(self, async_live_client: AsyncAirflowClient):
        """Test getting circuit breaker status."""
        status = async_live_client.get_circuit_breaker_status()
        if status:
            assert "state" in status


class TestAsyncAirflowClientLiveCleanup:
    """Tests for resource cleanup."""

    @live
    @pytest.mark.asyncio
    async def test_close_client(self, async_live_client: AsyncAirflowClient):
        """Test closing the client."""
        client = AsyncAirflowClient(
            base_url=os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080"),
            username=os.getenv("AIRFLOW_USERNAME", "admin"),
            password=os.getenv("AIRFLOW_PASSWORD", "admin"),
            timeout=10,
        )
        await client.test_connection()
        await client.close()
        assert client._client is None
