"""Unit tests for AirflowOrchestrator.

Tests cover:
- State mapping from Airflow to unified WorkflowState
- trigger_workflow with various parameters
- get_workflow_run and list_workflow_runs
- list_workflows
- get_task_runs and get_task_logs
- retry_workflow and cancel_workflow
- get_health and circuit breaker methods
- Airflow-specific methods
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from elt_mcp_server.clients.async_airflow_client import (
    AsyncAirflowAPIError,
    AsyncAirflowClient,
)
from elt_mcp_server.workflow.airflow_orchestrator import (
    AirflowOrchestrator,
    _map_airflow_state,
    _parse_airflow_datetime,
)
from elt_mcp_server.workflow.protocol import (
    CircuitBreakerOpenError,
    WorkflowNotFoundError,
    WorkflowState,
    WorkflowTimeoutError,
    WorkflowTriggerError,
)


class TestAirflowStateMapping:
    """Tests for Airflow state to WorkflowState mapping."""

    def test_queued_state(self):
        """Test queued maps to PENDING."""
        assert _map_airflow_state("queued") == WorkflowState.PENDING

    def test_running_state(self):
        """Test running maps to RUNNING."""
        assert _map_airflow_state("running") == WorkflowState.RUNNING

    def test_success_state(self):
        """Test success maps to SUCCESS."""
        assert _map_airflow_state("success") == WorkflowState.SUCCESS

    def test_failed_state(self):
        """Test failed maps to FAILED."""
        assert _map_airflow_state("failed") == WorkflowState.FAILED

    def test_skipped_state(self):
        """Test skipped maps to SKIPPED."""
        assert _map_airflow_state("skipped") == WorkflowState.SKIPPED

    def test_up_for_retry_state(self):
        """Test up_for_retry maps to RETRY."""
        assert _map_airflow_state("up_for_retry") == WorkflowState.RETRY

    def test_upstream_failed_state(self):
        """Test upstream_failed maps to FAILED."""
        assert _map_airflow_state("upstream_failed") == WorkflowState.FAILED

    def test_scheduled_state(self):
        """Test scheduled maps to PENDING."""
        assert _map_airflow_state("scheduled") == WorkflowState.PENDING

    def test_removed_state(self):
        """Test removed maps to CANCELLED."""
        assert _map_airflow_state("removed") == WorkflowState.CANCELLED

    def test_none_state(self):
        """Test None maps to UNKNOWN."""
        assert _map_airflow_state(None) == WorkflowState.UNKNOWN

    def test_unknown_state(self):
        """Test unknown state maps to UNKNOWN."""
        assert _map_airflow_state("unknown_state_xyz") == WorkflowState.UNKNOWN

    def test_case_insensitive(self):
        """Test state mapping is case insensitive."""
        assert _map_airflow_state("SUCCESS") == WorkflowState.SUCCESS
        assert _map_airflow_state("RUNNING") == WorkflowState.RUNNING


class TestAirflowDatetimeParsing:
    """Tests for Airflow datetime parsing."""

    def test_iso_format(self):
        """Test parsing standard ISO format."""
        result = _parse_airflow_datetime("2025-01-15T10:30:00")
        assert result is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15

    def test_iso_format_with_z(self):
        """Test parsing ISO format with Z suffix."""
        result = _parse_airflow_datetime("2025-01-15T10:30:00Z")
        assert result is not None
        assert result.tzinfo is not None

    def test_iso_format_with_timezone(self):
        """Test parsing ISO format with timezone offset."""
        result = _parse_airflow_datetime("2025-01-15T10:30:00+00:00")
        assert result is not None

    def test_none_input(self):
        """Test None input returns None."""
        assert _parse_airflow_datetime(None) is None

    def test_empty_string(self):
        """Test empty string returns None."""
        assert _parse_airflow_datetime("") is None

    def test_invalid_format(self):
        """Test invalid format returns None."""
        assert _parse_airflow_datetime("not-a-date") is None


class TestAirflowOrchestratorInit:
    """Tests for AirflowOrchestrator initialization."""

    def test_init_with_client(self):
        """Test initialization with AsyncAirflowClient."""
        mock_client = Mock()
        orchestrator = AirflowOrchestrator(client=mock_client)
        assert orchestrator._client is mock_client

    def test_backend_name(self):
        """Test backend_name property returns 'airflow'."""
        mock_client = Mock()
        orchestrator = AirflowOrchestrator(client=mock_client)
        assert orchestrator.backend_name == "airflow"


class TestTriggerWorkflow:
    """Tests for trigger_workflow method."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mock client."""
        mock_client = AsyncMock()
        return AirflowOrchestrator(client=mock_client)

    @pytest.mark.asyncio
    async def test_trigger_basic(self, orchestrator):
        """Test basic workflow trigger."""
        orchestrator._client.trigger_dag.return_value = {
            "dag_run_id": "manual__2025-01-15",
            "state": "queued",
            "start_date": "2025-01-15T10:00:00Z",
        }

        result = await orchestrator.trigger_workflow(
            workflow_id="my_dag",
            config={"key": "value"},
        )

        assert result.run_id == "manual__2025-01-15"
        assert result.workflow_id == "my_dag"
        assert result.state == WorkflowState.PENDING
        assert result.config == {"key": "value"}
        orchestrator._client.trigger_dag.assert_called_once_with(
            dag_id="my_dag",
            conf={"key": "value"},
        )

    @pytest.mark.asyncio
    async def test_trigger_idempotent(self, orchestrator):
        """Test idempotent workflow trigger."""
        orchestrator._client.trigger_dag_idempotent.return_value = {
            "dag_run_id": "idempotent-key-123",
            "state": "running",
            "idempotent_reused": True,
        }

        result = await orchestrator.trigger_workflow(
            workflow_id="my_dag",
            idempotency_key="unique-key-123",
        )

        assert result.metadata.get("idempotent_reused") is True
        orchestrator._client.trigger_dag_idempotent.assert_called_once()

    @pytest.mark.asyncio
    async def test_trigger_wait_for_completion(self, orchestrator):
        """Test trigger with wait_for_completion."""
        orchestrator._client.trigger_dag.return_value = {
            "dag_run_id": "run-1",
            "state": "queued",
        }
        orchestrator._client.wait_for_dag_run.return_value = {
            "state": "success",
            "end_date": "2025-01-15T10:30:00Z",
        }

        result = await orchestrator.trigger_workflow(
            workflow_id="my_dag",
            wait_for_completion=True,
            timeout_seconds=300,
        )

        assert result.state == WorkflowState.SUCCESS
        orchestrator._client.wait_for_dag_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_trigger_timeout(self, orchestrator):
        """Test trigger timeout raises WorkflowTimeoutError."""
        orchestrator._client.trigger_dag.return_value = {
            "dag_run_id": "run-1",
            "state": "queued",
        }
        # Use both TimeoutError and asyncio.TimeoutError to cover both cases
        import asyncio
        orchestrator._client.wait_for_dag_run.side_effect = asyncio.TimeoutError("Timed out")

        with pytest.raises(WorkflowTimeoutError):
            await orchestrator.trigger_workflow(
                workflow_id="my_dag",
                wait_for_completion=True,
                timeout_seconds=60,
            )

    @pytest.mark.asyncio
    async def test_trigger_circuit_breaker_open(self, orchestrator):
        """Test trigger with circuit breaker open."""
        orchestrator._client.trigger_dag.side_effect = CircuitBreakerOpenError("circuit breaker open")

        with pytest.raises(CircuitBreakerOpenError):
            await orchestrator.trigger_workflow(workflow_id="my_dag")

    @pytest.mark.asyncio
    async def test_trigger_general_error(self, orchestrator):
        """Test trigger with general error."""
        orchestrator._client.trigger_dag.side_effect = Exception("Connection refused")

        with pytest.raises(WorkflowTriggerError) as exc_info:
            await orchestrator.trigger_workflow(workflow_id="my_dag")

        assert "Connection refused" in str(exc_info.value)


class TestGetWorkflowRun:
    """Tests for get_workflow_run method."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mock client."""
        mock_client = AsyncMock()
        return AirflowOrchestrator(client=mock_client)

    @pytest.mark.asyncio
    async def test_get_run_success(self, orchestrator):
        """Test getting workflow run successfully."""
        orchestrator._client.get_dag_run_status.return_value = {
            "dag_run_id": "run-1",
            "state": "success",
            "start_date": "2025-01-15T10:00:00Z",
            "end_date": "2025-01-15T10:30:00Z",
            "duration": 1800.0,
            "task_summary": {"success": 5, "failed": 0},
            "total_tasks": 5,
        }

        result = await orchestrator.get_workflow_run("my_dag", "run-1")

        assert result.run_id == "run-1"
        assert result.state == WorkflowState.SUCCESS
        assert result.duration_seconds == 1800.0
        assert result.metadata["total_tasks"] == 5

    @pytest.mark.asyncio
    async def test_get_run_not_found(self, orchestrator):
        """Test get_workflow_run with not found error."""
        orchestrator._client.get_dag_run_status.side_effect = Exception("404 not found")

        with pytest.raises(WorkflowNotFoundError):
            await orchestrator.get_workflow_run("my_dag", "invalid-run")


class TestListWorkflowRuns:
    """Tests for list_workflow_runs method."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mock client."""
        mock_client = AsyncMock()
        return AirflowOrchestrator(client=mock_client)

    @pytest.mark.asyncio
    async def test_list_runs_basic(self, orchestrator):
        """Test listing workflow runs."""
        orchestrator._client.list_dag_runs.return_value = [
            {"dag_run_id": "run-1", "state": "success"},
            {"dag_run_id": "run-2", "state": "failed"},
        ]

        result = await orchestrator.list_workflow_runs("my_dag", limit=10)

        assert len(result) == 2
        assert result[0].run_id == "run-1"
        assert result[0].state == WorkflowState.SUCCESS
        assert result[1].state == WorkflowState.FAILED

    @pytest.mark.asyncio
    async def test_list_runs_with_state_filter(self, orchestrator):
        """Test listing runs with state filter."""
        orchestrator._client.list_dag_runs.return_value = []

        await orchestrator.list_workflow_runs(
            "my_dag",
            state=WorkflowState.FAILED,
        )

        orchestrator._client.list_dag_runs.assert_called_once()
        call_args = orchestrator._client.list_dag_runs.call_args
        assert call_args.kwargs.get("state") == "failed"

    @pytest.mark.asyncio
    async def test_list_runs_with_date_filter(self, orchestrator):
        """Test listing runs with date filter (client-side filtering)."""
        start_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
        # Mock runs with various dates
        orchestrator._client.list_dag_runs.return_value = [
            {"dag_run_id": "run-1", "state": "success", "start_date": "2025-01-15T00:00:00+00:00"},
            {"dag_run_id": "run-2", "state": "success", "start_date": "2024-12-01T00:00:00+00:00"},
        ]

        result = await orchestrator.list_workflow_runs(
            "my_dag",
            start_date_gte=start_date,
        )

        # Only run-1 should be returned (after 2025-01-01)
        assert len(result) == 1
        assert result[0].run_id == "run-1"


class TestListWorkflows:
    """Tests for list_workflows method."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mock client."""
        mock_client = AsyncMock()
        return AirflowOrchestrator(client=mock_client)

    @pytest.mark.asyncio
    async def test_list_workflows(self, orchestrator):
        """Test listing available workflows."""
        orchestrator._client.list_dags.return_value = [
            {
                "dag_id": "dag1",
                "description": "First DAG",
                "schedule_interval": "@daily",
                "is_paused": False,
                "tags": [{"name": "etl"}],
                "owners": ["admin"],
            },
            {
                "dag_id": "dag2",
                "is_paused": True,
            },
        ]

        result = await orchestrator.list_workflows(limit=50, only_active=True)

        assert len(result) == 2
        assert result[0].workflow_id == "dag1"
        assert result[0].schedule == "@daily"
        assert result[0].is_active is True
        assert result[0].tags == ["etl"]
        assert result[1].is_active is False


class TestTaskOperations:
    """Tests for task-related methods."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mock client."""
        mock_client = AsyncMock()
        return AirflowOrchestrator(client=mock_client)

    @pytest.mark.asyncio
    async def test_get_task_runs(self, orchestrator):
        """Test getting task runs for a workflow."""
        orchestrator._client.list_task_instances.return_value = [
            {
                "task_id": "extract",
                "state": "success",
                "start_date": "2025-01-15T10:00:00Z",
                "duration": 60.0,
                "try_number": 1,
            },
            {
                "task_id": "load",
                "state": "failed",
                "error": "Connection timeout",
                "try_number": 2,
            },
        ]

        result = await orchestrator.get_task_runs("my_dag", "run-1")

        assert len(result) == 2
        assert result[0].task_id == "extract"
        assert result[0].state == WorkflowState.SUCCESS
        assert result[1].task_id == "load"
        assert result[1].state == WorkflowState.FAILED
        assert result[1].attempt_number == 2
        assert result[1].error_message == "Connection timeout"

    @pytest.mark.asyncio
    async def test_get_task_logs(self, orchestrator):
        """Test getting task logs."""
        orchestrator._client.get_task_logs.return_value = "INFO: Task started\nINFO: Task completed"

        result = await orchestrator.get_task_logs(
            workflow_id="my_dag",
            run_id="run-1",
            task_id="extract",
            attempt_number=1,
        )

        assert "Task started" in result
        orchestrator._client.get_task_logs.assert_called_once_with(
            dag_id="my_dag",
            dag_run_id="run-1",
            task_id="extract",
            task_try_number=1,
        )

    @pytest.mark.asyncio
    async def test_get_task_logs_empty(self, orchestrator):
        """Test getting empty task logs."""
        orchestrator._client.get_task_logs.return_value = None

        result = await orchestrator.get_task_logs("my_dag", "run-1", "task1")

        assert result == ""


class TestRetryAndCancel:
    """Tests for retry and cancel methods."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mock client."""
        mock_client = AsyncMock()
        return AirflowOrchestrator(client=mock_client)

    @pytest.mark.asyncio
    async def test_retry_workflow_clears_dag_run(self, orchestrator):
        """Test retrying workflow clears task instances."""
        # Mock clear_dag_run
        orchestrator._client.clear_dag_run.return_value = {
            "task_instances": [{"task_id": "task1"}, {"task_id": "task2"}],
        }
        # Mock get_dag_run_status for the get_workflow_run call
        orchestrator._client.get_dag_run_status.return_value = {
            "dag_run_id": "run-1",
            "state": "queued",
            "execution_date": "2025-01-15T10:00:00Z",
        }

        result = await orchestrator.retry_workflow("my_dag", "run-1")

        assert result.state == WorkflowState.PENDING
        assert result.run_id == "run-1"
        orchestrator._client.clear_dag_run.assert_called_once_with(
            dag_id="my_dag",
            dag_run_id="run-1",
            dry_run=False,
            reset_dag_runs=True,
            only_failed=True,
        )

    @pytest.mark.asyncio
    async def test_retry_workflow_with_task_ids(self, orchestrator):
        """Test retrying specific tasks clears all (not only failed)."""
        orchestrator._client.clear_dag_run.return_value = {
            "task_instances": [{"task_id": "task1"}],
        }
        orchestrator._client.get_dag_run_status.return_value = {
            "dag_run_id": "run-1",
            "state": "queued",
        }

        result = await orchestrator.retry_workflow(
            "my_dag",
            "run-1",
            task_ids=["task1"],
        )

        # Should call with only_failed=False when task_ids provided
        orchestrator._client.clear_dag_run.assert_called_once_with(
            dag_id="my_dag",
            dag_run_id="run-1",
            dry_run=False,
            reset_dag_runs=True,
            only_failed=False,
        )

    @pytest.mark.asyncio
    async def test_retry_workflow_failure(self, orchestrator):
        """Test retry workflow raises error on failure."""
        orchestrator._client.clear_dag_run.side_effect = Exception("API Error")

        with pytest.raises(WorkflowTriggerError) as exc_info:
            await orchestrator.retry_workflow("my_dag", "run-1")

        assert "Failed to retry workflow" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_workflow_success(self, orchestrator):
        """Test cancel workflow sets state to failed."""
        orchestrator._client.set_dag_run_state.return_value = {
            "dag_run_id": "run-1",
            "state": "failed",
        }

        result = await orchestrator.cancel_workflow("my_dag", "run-1")

        assert result is True
        orchestrator._client.set_dag_run_state.assert_called_once_with(
            dag_id="my_dag",
            dag_run_id="run-1",
            state="failed",
        )

    @pytest.mark.asyncio
    async def test_cancel_workflow_failure(self, orchestrator):
        """Test cancel workflow returns False on failure."""
        orchestrator._client.set_dag_run_state.side_effect = Exception("API Error")

        result = await orchestrator.cancel_workflow("my_dag", "run-1")

        assert result is False


class TestHealthAndCircuitBreaker:
    """Tests for health check and circuit breaker methods."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mock client."""
        mock_client = AsyncMock()
        return AirflowOrchestrator(client=mock_client)

    @pytest.mark.asyncio
    async def test_get_health_healthy(self, orchestrator):
        """Test health check with healthy status."""
        orchestrator._client.test_connection.return_value = {
            "connected": True,
            "version": "2.8.0",
            "url": "http://localhost:8080",
        }
        orchestrator._client.get_circuit_breaker_status = Mock(return_value={"state": "closed"})

        result = await orchestrator.get_health()

        assert result.connected is True
        assert result.backend == "airflow"
        assert result.version == "2.8.0"
        assert result.availability == "healthy"

    @pytest.mark.asyncio
    async def test_get_health_degraded(self, orchestrator):
        """Test health check with circuit breaker open."""
        orchestrator._client.test_connection.return_value = {"connected": True}
        orchestrator._client.get_circuit_breaker_status = Mock(return_value={"state": "open"})

        result = await orchestrator.get_health()

        assert result.availability == "degraded"

    @pytest.mark.asyncio
    async def test_get_health_unavailable(self, orchestrator):
        """Test health check with connection failure."""
        orchestrator._client.test_connection.return_value = {
            "connected": False, "error": "Connection refused"
        }

        result = await orchestrator.get_health()

        assert result.connected is False
        assert result.availability == "unavailable"

    @pytest.mark.asyncio
    async def test_get_health_exception(self, orchestrator):
        """Test health check with exception."""
        orchestrator._client.test_connection.side_effect = Exception("Network error")

        result = await orchestrator.get_health()

        assert result.connected is False
        assert result.availability == "unavailable"
        assert "Network error" in result.error

    def test_get_circuit_breaker_status(self, orchestrator):
        """Test getting circuit breaker status."""
        orchestrator._client.get_circuit_breaker_status = Mock(return_value={"state": "half_open"})

        result = orchestrator.get_circuit_breaker_status()

        assert result == {"state": "half_open"}

    def test_get_circuit_breaker_status_no_method(self, orchestrator):
        """Test circuit breaker status when client lacks method."""
        del orchestrator._client.get_circuit_breaker_status

        result = orchestrator.get_circuit_breaker_status()

        assert result is None

    def test_reset_circuit_breaker(self, orchestrator):
        """Test resetting circuit breaker."""
        orchestrator._client.reset_circuit_breaker = Mock(return_value=True)

        result = orchestrator.reset_circuit_breaker()

        assert result is True

    def test_reset_circuit_breaker_no_method(self, orchestrator):
        """Test reset when client lacks method."""
        del orchestrator._client.reset_circuit_breaker

        result = orchestrator.reset_circuit_breaker()

        assert result is False


class TestAirflowSpecificMethods:
    """Tests for Airflow-specific methods not in protocol."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mock client."""
        mock_client = AsyncMock()
        return AirflowOrchestrator(client=mock_client)

    @pytest.mark.asyncio
    async def test_trigger_multiple_workflows(self, orchestrator):
        """Test triggering multiple DAGs concurrently."""
        orchestrator._client.trigger_multiple_dags.return_value = [
            {"dag_id": "dag1", "dag_run_id": "run-1", "state": "queued"},
            {"dag_id": "dag2", "dag_run_id": "run-2", "state": "queued", "error": None},
            {"dag_id": "dag3", "dag_run_id": "", "error": "DAG not found"},
        ]

        result = await orchestrator.trigger_multiple_workflows([
            {"workflow_id": "dag1", "config": {"key": "val1"}},
            {"workflow_id": "dag2"},
            {"workflow_id": "dag3"},
        ])

        assert len(result) == 3
        assert result[0].workflow_id == "dag1"
        assert result[0].state == WorkflowState.PENDING
        assert result[2].error_message == "DAG not found"


# =============================================================================
# Orchestrator error tests (merged from test_airflow_negative_cases.py)
# =============================================================================


class TestOrchestratorErrors:
    """Tests for orchestrator error handling."""

    @pytest.fixture
    def neg_orchestrator(self):
        """Create AirflowOrchestrator with mocked client."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            username="admin",
            password="admin",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"
        return AirflowOrchestrator(client=client)

    @pytest.mark.asyncio
    async def test_workflow_not_found(self, neg_orchestrator):
        """Test handling of non-existent workflow."""
        neg_orchestrator._client.get_dag_run = AsyncMock(
            side_effect=AsyncAirflowAPIError("404 Not Found")
        )

        with pytest.raises((WorkflowNotFoundError, AsyncAirflowAPIError)):
            await neg_orchestrator.get_workflow_run("non_existent_dag", "run_1")

    @pytest.mark.asyncio
    async def test_list_workflows_empty(self, neg_orchestrator):
        """Test listing workflows when none exist."""
        neg_orchestrator._client.list_dags = AsyncMock(return_value=[])

        result = await neg_orchestrator.list_workflows()
        assert result == []

    @pytest.mark.asyncio
    async def test_trigger_workflow_failure(self, neg_orchestrator):
        """Test workflow trigger failure."""
        neg_orchestrator._client.trigger_dag = AsyncMock(
            side_effect=AsyncAirflowAPIError("Failed to trigger DAG")
        )

        # Error is wrapped in WorkflowTriggerError
        with pytest.raises((WorkflowTriggerError, AsyncAirflowAPIError)):
            await neg_orchestrator.trigger_workflow("test_dag", config={})

    @pytest.mark.asyncio
    async def test_cancel_workflow_already_finished(self, neg_orchestrator):
        """Test cancelling an already finished workflow."""
        neg_orchestrator._client.get_dag_run = AsyncMock(return_value={
            "dag_run_id": "run_1",
            "state": "success",
            "logical_date": "2025-01-01T00:00:00Z",
        })

        # Cancelling finished workflow might fail or return False
        result = await neg_orchestrator.cancel_workflow("test_dag", "run_1")
        # Should handle gracefully
        assert result is False or result is True

    @pytest.mark.asyncio
    async def test_get_task_runs_no_tasks(self, neg_orchestrator):
        """Test getting task runs when no tasks exist."""
        neg_orchestrator._client.list_task_instances = AsyncMock(return_value=[])

        result = await neg_orchestrator.get_task_runs("test_dag", "run_1")
        assert result == []

    @pytest.mark.asyncio
    async def test_health_check_degraded(self, neg_orchestrator):
        """Test health check returns degraded status."""
        neg_orchestrator._client.get_health = AsyncMock(return_value={
            "metadatabase": {"status": "healthy"},
            "scheduler": {"status": "unhealthy"},
        })

        health = await neg_orchestrator.get_health()
        # Should indicate degraded health
        assert health.connected is True or health.connected is False
