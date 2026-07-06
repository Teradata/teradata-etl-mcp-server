"""Live integration tests for AirflowOrchestrator.

Tests the WorkflowOrchestratorProtocol implementation with a real Airflow instance.

Skip by default unless AIRFLOW_BASE_URL is set and reachable.
"""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncGenerator

import pytest

from elt_mcp_server.clients.async_airflow_client import AsyncAirflowClient
from elt_mcp_server.workflow import (
    AirflowOrchestrator,
    OrchestratorHealth,
    TaskRun,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowState,
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
async def async_client() -> AsyncGenerator[AsyncAirflowClient, None]:
    """Create AsyncAirflowClient for testing."""
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
        circuit_breaker_enabled=True,
    )
    yield client
    await client.close()


@pytest.fixture(scope="function")
async def orchestrator(
    async_client: AsyncAirflowClient,
) -> AsyncGenerator[AirflowOrchestrator, None]:
    """Create AirflowOrchestrator for testing."""
    yield AirflowOrchestrator(client=async_client)


class TestOrchestratorBasics:
    """Basic orchestrator functionality tests."""

    @live
    @pytest.mark.asyncio
    async def test_backend_name(self, orchestrator: AirflowOrchestrator):
        """Test that backend name is 'airflow'."""
        assert orchestrator.backend_name == "airflow"

    @live
    @pytest.mark.asyncio
    async def test_get_health(self, orchestrator: AirflowOrchestrator):
        """Test getting orchestrator health status."""
        health = await orchestrator.get_health()

        assert isinstance(health, OrchestratorHealth)
        assert health.backend == "airflow"
        assert health.connected is True

        health_dict = health.to_dict()
        assert isinstance(health_dict, dict)
        assert health_dict["backend"] == "airflow"


class TestOrchestratorListWorkflows:
    """Tests for listing workflows (DAGs)."""

    @live
    @pytest.mark.asyncio
    async def test_list_workflows(self, orchestrator: AirflowOrchestrator):
        """Test listing workflows returns WorkflowDefinition objects."""
        workflows = await orchestrator.list_workflows(limit=10)

        assert isinstance(workflows, list)
        for workflow in workflows:
            assert isinstance(workflow, WorkflowDefinition)
            assert workflow.workflow_id
            assert workflow.name

            workflow_dict = workflow.to_dict()
            assert isinstance(workflow_dict, dict)
            assert "workflow_id" in workflow_dict

    @live
    @pytest.mark.asyncio
    async def test_list_workflows_with_limit(self, orchestrator: AirflowOrchestrator):
        """Test listing workflows with limit."""
        page1 = await orchestrator.list_workflows(limit=5)
        assert isinstance(page1, list)
        assert len(page1) <= 5


class TestOrchestratorWorkflowDetails:
    """Tests for getting workflow details via list."""

    @live
    @pytest.mark.asyncio
    async def test_get_workflow_details(self, orchestrator: AirflowOrchestrator):
        """Test getting workflow details from list."""
        workflows = await orchestrator.list_workflows(limit=1)
        if not workflows:
            pytest.skip("No workflows available to inspect")

        workflow = workflows[0]
        assert isinstance(workflow, WorkflowDefinition)
        assert workflow.workflow_id
        assert workflow.name


class TestOrchestratorListWorkflowRuns:
    """Tests for listing workflow runs (DAG runs)."""

    @live
    @pytest.mark.asyncio
    async def test_list_workflow_runs(self, orchestrator: AirflowOrchestrator):
        """Test listing workflow runs for a workflow."""
        workflows = await orchestrator.list_workflows(limit=5)
        if not workflows:
            pytest.skip("No workflows available")

        for workflow in workflows:
            runs = await orchestrator.list_workflow_runs(
                workflow_id=workflow.workflow_id,
                limit=5,
            )
            assert isinstance(runs, list)
            for run in runs:
                assert isinstance(run, WorkflowRun)
                assert run.workflow_id == workflow.workflow_id
                assert isinstance(run.state, WorkflowState)

            if runs:
                return

        pytest.skip("No workflow runs found in any workflow")


class TestOrchestratorGetWorkflowRun:
    """Tests for getting individual workflow runs."""

    @live
    @pytest.mark.asyncio
    async def test_get_workflow_run(self, orchestrator: AirflowOrchestrator):
        """Test getting a specific workflow run."""
        workflows = await orchestrator.list_workflows(limit=5)
        if not workflows:
            pytest.skip("No workflows available")

        for workflow in workflows:
            runs = await orchestrator.list_workflow_runs(
                workflow_id=workflow.workflow_id,
                limit=1,
            )
            if runs:
                run_id = runs[0].run_id
                run = await orchestrator.get_workflow_run(
                    workflow_id=workflow.workflow_id,
                    run_id=run_id,
                )

                assert isinstance(run, WorkflowRun)
                assert run.run_id == run_id
                assert run.workflow_id == workflow.workflow_id
                return

        pytest.skip("No workflow runs found")


class TestOrchestratorTaskRuns:
    """Tests for task run functionality."""

    @live
    @pytest.mark.asyncio
    async def test_get_task_runs(self, orchestrator: AirflowOrchestrator):
        """Test getting task runs for a workflow run."""
        workflows = await orchestrator.list_workflows(limit=5)
        if not workflows:
            pytest.skip("No workflows available")

        for workflow in workflows:
            runs = await orchestrator.list_workflow_runs(
                workflow_id=workflow.workflow_id,
                limit=3,
            )
            for run in runs:
                tasks = await orchestrator.get_task_runs(
                    workflow_id=workflow.workflow_id,
                    run_id=run.run_id,
                )
                if tasks:
                    assert isinstance(tasks, list)
                    for task in tasks:
                        assert isinstance(task, TaskRun)
                        assert task.workflow_id == workflow.workflow_id
                        assert task.run_id == run.run_id

                    return

        pytest.skip("No task runs found")


class TestOrchestratorStateMapping:
    """Tests for workflow state mapping."""

    @live
    @pytest.mark.asyncio
    async def test_state_mapping_consistency(self, orchestrator: AirflowOrchestrator):
        """Test that all states are properly mapped to WorkflowState enum."""
        workflows = await orchestrator.list_workflows(limit=10)
        if not workflows:
            pytest.skip("No workflows available")

        all_states = set()
        for workflow in workflows:
            runs = await orchestrator.list_workflow_runs(
                workflow_id=workflow.workflow_id,
                limit=10,
            )
            for run in runs:
                all_states.add(run.state)

        for state in all_states:
            assert isinstance(state, WorkflowState)


class TestOrchestratorCircuitBreaker:
    """Tests for circuit breaker integration."""

    @live
    @pytest.mark.asyncio
    async def test_get_circuit_breaker_status(self, orchestrator: AirflowOrchestrator):
        """Test getting circuit breaker status through orchestrator."""
        status = orchestrator.get_circuit_breaker_status()
        if status:
            assert isinstance(status, dict)
            assert "state" in status


class TestOrchestratorRateLimiter:
    """Tests for rate limiter integration."""

    @live
    @pytest.mark.asyncio
    async def test_get_rate_limiter_status(self, orchestrator: AirflowOrchestrator):
        """Test getting rate limiter status through orchestrator."""
        status = orchestrator.get_rate_limiter_status()
        if status:
            assert isinstance(status, dict)
            assert "rate_rps" in status


class TestOrchestratorSerialization:
    """Tests for data class serialization."""

    @live
    @pytest.mark.asyncio
    async def test_workflow_run_serialization(self, orchestrator: AirflowOrchestrator):
        """Test WorkflowRun serializes correctly for MCP response."""
        workflows = await orchestrator.list_workflows(limit=5)
        if not workflows:
            pytest.skip("No workflows available")

        for workflow in workflows:
            runs = await orchestrator.list_workflow_runs(
                workflow_id=workflow.workflow_id,
                limit=1,
            )
            if runs:
                run = runs[0]
                run_dict = run.to_dict()

                assert "run_id" in run_dict
                assert "workflow_id" in run_dict
                assert "state" in run_dict
                assert isinstance(run_dict["state"], str)
                return

        pytest.skip("No workflow runs found")

    @live
    @pytest.mark.asyncio
    async def test_health_serialization(self, orchestrator: AirflowOrchestrator):
        """Test OrchestratorHealth serializes correctly for MCP response."""
        health = await orchestrator.get_health()
        health_dict = health.to_dict()

        assert "connected" in health_dict
        assert "backend" in health_dict
        assert isinstance(health_dict["connected"], bool)
        assert isinstance(health_dict["backend"], str)
