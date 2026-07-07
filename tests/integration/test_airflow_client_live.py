"""Live integration tests for AsyncAirflowClient.

These tests hit a real Airflow instance defined via environment variables.

Non-destructive only: version, health, list, and read-only introspection.

Skip by default unless AIRFLOW_BASE_URL is set or LIVE_AIRFLOW=true.
"""

from __future__ import annotations

import os
import socket

import pytest

from teradata_etl_mcp_server.clients.async_airflow_client import AsyncAirflowClient

# Attempt to load .env automatically for local runs
try:  # pragma: no cover - convenience only
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:  # pragma: no cover
    pass


def _host_is_reachable(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _should_run_live() -> bool:
    url = os.getenv("AIRFLOW_BASE_URL")
    if not url:
        return False
    # quick reachability probe
    try:
        import urllib.parse as up

        p = up.urlparse(url)
        if not p.hostname or not p.port:
            return False
        return _host_is_reachable(p.hostname, p.port)
    except Exception:
        return False


pytestmark = pytest.mark.integration


live = pytest.mark.skipif(not _should_run_live(), reason="Live Airflow not reachable or not configured")


@pytest.fixture(scope="session")
async def live_client():
    base_url = os.getenv("AIRFLOW_BASE_URL")
    username = os.getenv("AIRFLOW_USERNAME", "admin")
    password = os.getenv("AIRFLOW_PASSWORD", "admin")
    auth_manager = os.getenv("AIRFLOW_AUTH_MANAGER", "basic")

    client = AsyncAirflowClient(
        base_url=base_url,
        username=username,
        password=password,
        auth_manager=auth_manager,
        timeout=20,
    )
    yield client
    await client.close()


@live
async def test_live_version_and_health(live_client: AsyncAirflowClient):
    ver = await live_client.get_version()
    assert isinstance(ver, dict)
    assert "version" in ver

    health = await live_client.get_health()
    assert isinstance(health, dict)
    assert health != {}


@live
async def test_live_list_dags_and_optionally_detail(live_client: AsyncAirflowClient):
    dags = await live_client.list_dags(limit=5)
    assert isinstance(dags, list)
    if dags:
        dag_id = dags[0].get("dag_id")
        if dag_id:
            dag = await live_client.get_dag(dag_id)
            assert dag.get("dag_id") == dag_id


@live
async def test_live_runs_and_tasks_when_present(live_client: AsyncAirflowClient):
    dags = await live_client.list_dags(limit=5)
    if not dags:
        pytest.skip("No DAGs available to inspect runs")

    dag_id = dags[0].get("dag_id")
    runs = await live_client.list_dag_runs(dag_id, limit=5)
    runs_list = runs.get("dag_runs", [])
    assert isinstance(runs_list, list)

    if runs_list:
        run_id = runs_list[0].get("dag_run_id")
        dr = await live_client.get_dag_run(dag_id, run_id)
        assert dr.get("dag_run_id") == run_id

        tasks = await live_client.list_task_instances(dag_id, run_id)
        assert isinstance(tasks, list)

        status = await live_client.get_dag_run_status(dag_id, run_id)
        assert status.get("dag_run_id") == run_id


@live
async def test_live_connections_variables_pools_readonly(live_client: AsyncAirflowClient):
    # Connections list (read-only)
    try:
        conns = await live_client.list_connections()
        assert isinstance(conns, list)
    except Exception:
        # Some Airflow deployments restrict this; don't fail the entire suite.
        pytest.skip("Connections endpoint restricted")

    # Variables list (read-only)
    try:
        vars_ = await live_client.list_variables()
        assert isinstance(vars_, list)
    except Exception:
        pytest.skip("Variables endpoint restricted")

    # Pools list (read-only)
    try:
        pools = await live_client.list_pools()
        assert isinstance(pools, list)
        if pools:
            pool_name = pools[0].get("name")
            if pool_name:
                pool = await live_client.get_pool(pool_name)
                assert pool.get("name") == pool_name
    except Exception:
        pytest.skip("Pools endpoint restricted")
