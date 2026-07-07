"""Tests for orchestration_execution.py hardening changes.

Covers:
- M3: get_task_logs truncation at 100KB boundary
- M4: retry_failed_tasks returns success=False for per-task retry
- safe_error_message integration in error responses
- All orchestration tools: trigger_dag_run, get_dag_run_status, list_dag_runs, etc.

NOTE: The implementation now uses async_airflow_client (native async), not airflow_client
with asyncio.to_thread. Tests must mock async_airflow_client methods as AsyncMock.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from teradata_etl_mcp_server.tools.orchestration_execution import register_orchestration_tools


def _make_orchestrator():
    """Create a mock orchestrator with async_airflow_client."""
    orch = Mock()
    orch.async_airflow_client = AsyncMock()
    orch.airflow_client = Mock()
    return orch


# =============================================================================
# Test: trigger_dag_run
# =============================================================================


class TestTriggerDagRun:
    """Tests for trigger_dag_run tool."""

    @pytest.mark.asyncio
    async def test_trigger_dag_run_success(self):
        """Successfully trigger a DAG run."""
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag = AsyncMock(
            return_value={
                "dag_run_id": "manual__2025-01-01T00:00:00",
                "execution_date": "2025-01-01T00:00:00",
                "state": "queued",
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](mode="run", pipeline_name="test_dag")

        assert result["success"] is True
        assert result["pipeline_name"] == "test_dag"
        assert result["dag_run_id"] == "manual__2025-01-01T00:00:00"
        assert result["state"] == "queued"
        assert "triggered_at" in result

    @pytest.mark.asyncio
    async def test_trigger_dag_run_with_config(self):
        """Trigger DAG run with configuration parameters."""
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag = AsyncMock(
            return_value={
                "dag_run_id": "manual__2025-01-01T00:00:00",
                "state": "queued",
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](mode="run",
            pipeline_name="test_dag",
            config={"param1": "value1", "param2": 123},
        )

        assert result["success"] is True
        orch.async_trigger_airflow_dag.assert_called_once()
        call_kwargs = orch.async_trigger_airflow_dag.call_args[1]
        assert call_kwargs["conf"] == {"param1": "value1", "param2": 123}

    @pytest.mark.asyncio
    async def test_trigger_dag_run_wait_for_completion(self):
        """Trigger DAG run with wait_for_completion."""
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag = AsyncMock(
            return_value={
                "dag_run_id": "manual__2025-01-01T00:00:00",
                "state": "success",
                "final_status": {"state": "success", "end_date": "2025-01-01T01:00:00"},
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](mode="run",
            pipeline_name="test_dag",
            wait_for_completion=True,
        )

        assert result["success"] is True
        assert result["final_status"]["state"] == "success"

    @pytest.mark.asyncio
    async def test_trigger_dag_run_failure(self):
        """Handle trigger failure gracefully."""
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag = AsyncMock(
            side_effect=RuntimeError("DAG not found")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](mode="run", pipeline_name="missing_dag")

        assert result["success"] is False
        assert "RuntimeError" in result["error"]
        assert "DAG not found" in result["error"]


# =============================================================================
# Test: trigger_dag_run with custom dag_run_id
# =============================================================================


class TestTriggerRunDagRunId:
    """Tests for dag_trigger mode='run' with custom dag_run_id."""

    @pytest.mark.asyncio
    async def test_trigger_run_with_custom_dag_run_id(self):
        """Custom dag_run_id is passed through to async_trigger_airflow_dag."""
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag = AsyncMock(
            return_value={
                "dag_run_id": "custom_id",
                "execution_date": "2025-01-01T00:00:00",
                "state": "queued",
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="run", pipeline_name="dag1", dag_run_id="custom_id"
        )

        assert result["success"] is True
        assert result["dag_run_id"] == "custom_id"
        orch.async_trigger_airflow_dag.assert_called_once_with(
            dag_id="dag1", conf=None, wait_for_completion=False, dag_run_id="custom_id"
        )

    @pytest.mark.asyncio
    async def test_trigger_run_without_dag_run_id(self):
        """Omitting dag_run_id passes None through to async_trigger_airflow_dag."""
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag = AsyncMock(
            return_value={
                "dag_run_id": "manual__2025-01-01T00:00:00",
                "execution_date": "2025-01-01T00:00:00",
                "state": "queued",
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](mode="run", pipeline_name="dag1")

        assert result["success"] is True
        orch.async_trigger_airflow_dag.assert_called_once_with(
            dag_id="dag1", conf=None, wait_for_completion=False, dag_run_id=None
        )


# =============================================================================
# Test: get_dag_run_status
# =============================================================================


class TestGetDagRunStatus:
    """Tests for get_dag_run_status tool."""

    @pytest.mark.asyncio
    async def test_get_dag_run_status_with_dag_run_id(self):
        """Get status with explicit dag_run_id."""
        orch = _make_orchestrator()
        orch.async_get_dag_run_status = AsyncMock(
            return_value={
                "dag_run_id": "run1",
                "state": "success",
                "execution_date": "2025-01-01T00:00:00",
                "start_date": "2025-01-01T00:00:01",
                "end_date": "2025-01-01T00:05:00",
                "duration": 299,
                "task_summary": {"success": 5, "failed": 0},
                "total_tasks": 5,
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="run_status",
            pipeline_name="test_dag",
            dag_run_id="run1",
        )

        assert result["pipeline_name"] == "test_dag"
        assert result["dag_run_id"] == "run1"
        assert result["state"] == "success"
        assert result["total_tasks"] == 5

    @pytest.mark.asyncio
    async def test_get_dag_run_status_without_dag_run_id(self):
        """Get status for latest run when dag_run_id not provided."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_dag_runs = AsyncMock(
            return_value=[{"dag_run_id": "latest_run"}]
        )
        orch.async_get_dag_run_status = AsyncMock(
            return_value={
                "dag_run_id": "latest_run",
                "state": "running",
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="run_status", pipeline_name="test_dag")

        assert result["dag_run_id"] == "latest_run"

    @pytest.mark.asyncio
    async def test_get_dag_run_status_no_runs_found(self):
        """Handle case when no DAG runs exist."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_dag_runs = AsyncMock(return_value=[])
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="run_status", pipeline_name="test_dag")

        assert "error" in result
        assert "No DAG runs found" in result["error"]

    @pytest.mark.asyncio
    async def test_get_dag_run_status_error(self):
        """Handle error gracefully."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_dag_runs = AsyncMock(
            side_effect=ConnectionError("API unavailable")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="run_status", pipeline_name="test_dag")

        assert "error" in result
        assert "ConnectionError" in result["error"]


# =============================================================================
# Test: list_dag_runs
# =============================================================================


class TestListDagRuns:
    """Tests for list_dag_runs tool."""

    @pytest.mark.asyncio
    async def test_list_dag_runs_success(self):
        """List DAG runs successfully."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_dag_runs = AsyncMock(
            return_value=[
                {
                    "dag_run_id": "run1",
                    "state": "success",
                    "execution_date": "2025-01-01",
                    "start_date": "2025-01-01T00:00:00",
                    "end_date": "2025-01-01T00:05:00",
                    "duration": 300,
                },
                {
                    "dag_run_id": "run2",
                    "state": "failed",
                    "execution_date": "2025-01-02",
                },
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="list_runs", pipeline_name="test_dag")

        assert result["pipeline_name"] == "test_dag"
        assert result["total_count"] == 2
        assert len(result["dag_runs"]) == 2
        assert result["dag_runs"][0]["dag_run_id"] == "run1"

    @pytest.mark.asyncio
    async def test_list_dag_runs_with_filters(self):
        """List DAG runs with filtering."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_dag_runs = AsyncMock(return_value=[])
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="list_runs",
            pipeline_name="test_dag",
            limit=5,
            state="success",
            start_date_gte="2025-01-01",
            start_date_lte="2025-01-31",
        )

        assert result["filters_applied"]["limit"] == 5
        assert result["filters_applied"]["state"] == "success"
        orch.async_airflow_client.list_dag_runs.assert_called_with(
            dag_id="test_dag",
            limit=5,
            state="success",
            execution_date_gte="2025-01-01",
            execution_date_lte="2025-01-31",
        )

    @pytest.mark.asyncio
    async def test_list_dag_runs_error(self):
        """Handle error gracefully."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_dag_runs = AsyncMock(
            side_effect=ConnectionError("timeout")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="list_runs", pipeline_name="dag1")

        assert "error" in result
        assert "ConnectionError" in result["error"]


# =============================================================================
# Test: monitor_pipeline_execution
# =============================================================================


class TestMonitorPipelineExecution:
    """Tests for monitor_pipeline_execution tool."""

    @pytest.mark.asyncio
    async def test_monitor_pipeline_success(self):
        """Monitor pipeline with all details."""
        orch = _make_orchestrator()
        orch.get_pipeline_status_async = AsyncMock(
            return_value={
                "is_paused": False,
                "last_run": {"state": "success", "execution_date": "2025-01-01"},
                "recent_runs": [
                    {"dag_run_id": "run1", "state": "success"},
                ],
                "statistics": {
                    "success_rate": 0.95,
                    "average_duration": 300,
                    "total_runs": 100,
                    "failed_runs": 5,
                },
            }
        )
        orch.async_airflow_client.list_task_instances = AsyncMock(
            return_value=[
                {"task_id": "task1", "state": "success", "duration": 60},
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="monitor_execution",
            pipeline_name="test_dag",
            include_task_details=True,
            include_performance_metrics=True,
        )

        assert result["pipeline_name"] == "test_dag"
        assert result["is_paused"] is False
        assert result["current_status"] == "success"
        assert result["performance_metrics"]["success_rate"] == 0.95
        assert "current_tasks" in result

    @pytest.mark.asyncio
    async def test_monitor_pipeline_without_task_details(self):
        """Monitor pipeline without task details."""
        orch = _make_orchestrator()
        orch.get_pipeline_status_async = AsyncMock(
            return_value={
                "is_paused": False,
                "last_run": {"state": "running"},
                "recent_runs": [],
                "statistics": {},
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="monitor_execution",
            pipeline_name="test_dag",
            include_task_details=False,
            include_performance_metrics=False,
        )

        assert result["pipeline_name"] == "test_dag"
        assert "current_tasks" not in result
        assert "performance_metrics" not in result

    @pytest.mark.asyncio
    async def test_monitor_pipeline_status_unavailable(self):
        """Handle unavailable pipeline status."""
        orch = _make_orchestrator()
        orch.get_pipeline_status_async = AsyncMock(return_value=None)
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="monitor_execution", pipeline_name="test_dag")

        assert "error" in result
        assert "Pipeline status unavailable" in result["error"]

    @pytest.mark.asyncio
    async def test_monitor_pipeline_error(self):
        """Handle error gracefully."""
        orch = _make_orchestrator()
        orch.get_pipeline_status_async = AsyncMock(
            side_effect=Exception("API error")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="monitor_execution", pipeline_name="test_dag")

        assert "error" in result


# =============================================================================
# Test: get_airflow_health
# =============================================================================


class TestGetAirflowHealth:
    """Tests for get_airflow_health tool."""

    @pytest.mark.asyncio
    async def test_get_airflow_health_success(self):
        """Get health status successfully."""
        orch = _make_orchestrator()
        orch.async_get_airflow_health = AsyncMock(
            return_value={
                "connected": True,
                "availability": "available",
                "api_version": "v2",
                "circuit_breaker": {"state": "closed", "is_available": True},
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["airflow_admin"](action="health")

        assert result["connected"] is True
        assert result["availability_message"] == "All systems operational"

    @pytest.mark.asyncio
    async def test_get_airflow_health_circuit_open(self):
        """Get health when circuit breaker is open."""
        orch = _make_orchestrator()
        orch.async_get_airflow_health = AsyncMock(
            return_value={
                "connected": False,
                "availability": "degraded",
                "circuit_breaker": {
                    "state": "open",
                    "time_until_recovery": 30,
                },
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["airflow_admin"](action="health")

        assert "Circuit breaker is OPEN" in result["availability_message"]

    @pytest.mark.asyncio
    async def test_get_airflow_health_half_open(self):
        """Get health when circuit breaker is half-open."""
        orch = _make_orchestrator()
        orch.async_get_airflow_health = AsyncMock(
            return_value={
                "connected": True,
                "circuit_breaker": {"state": "half_open"},
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["airflow_admin"](action="health")

        assert "testing recovery" in result["availability_message"]

    @pytest.mark.asyncio
    async def test_get_airflow_health_no_circuit_breaker(self):
        """Get health without circuit breaker."""
        orch = _make_orchestrator()
        orch.async_get_airflow_health = AsyncMock(
            return_value={
                "connected": True,
                "availability": "available",
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["airflow_admin"](action="health")

        assert result["circuit_breaker"] is None
        assert result["availability_message"] == "All systems operational"

    @pytest.mark.asyncio
    async def test_get_airflow_health_disconnected(self):
        """Get health when disconnected."""
        orch = _make_orchestrator()
        orch.async_get_airflow_health = AsyncMock(
            return_value={"connected": False, "availability": "unavailable"}
        )
        tools = register_orchestration_tools(orch)

        result = await tools["airflow_admin"](action="health")

        assert result["connected"] is False
        assert result["error"] == "Connection failed"

    @pytest.mark.asyncio
    async def test_get_airflow_health_error(self):
        """Handle error gracefully."""
        orch = _make_orchestrator()
        orch.async_get_airflow_health = AsyncMock(
            side_effect=Exception("Network error")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["airflow_admin"](action="health")

        assert result["connected"] is False
        assert result["availability"] == "unknown"
        assert "error" in result


# =============================================================================
# Test: reset_airflow_circuit_breaker
# =============================================================================


class TestResetAirflowCircuitBreaker:
    """Tests for reset_airflow_circuit_breaker tool."""

    @pytest.mark.asyncio
    async def test_reset_circuit_breaker_success(self):
        """Successfully reset circuit breaker."""
        orch = _make_orchestrator()
        # The implementation uses async_airflow_client, not airflow_client
        orch.async_airflow_client.reset_circuit_breaker = Mock(return_value=True)
        orch.async_airflow_client.get_circuit_breaker_status = Mock(
            return_value={"state": "closed", "is_available": True}
        )
        tools = register_orchestration_tools(orch)

        result = await tools["airflow_admin"](action="reset_circuit_breaker")

        assert result["success"] is True
        assert "reset to CLOSED" in result["message"]
        assert result["circuit_breaker"]["state"] == "closed"

    @pytest.mark.asyncio
    async def test_reset_circuit_breaker_not_enabled(self):
        """Handle circuit breaker not enabled."""
        orch = _make_orchestrator()
        # The implementation uses async_airflow_client, not airflow_client
        orch.async_airflow_client.reset_circuit_breaker = Mock(return_value=False)
        tools = register_orchestration_tools(orch)

        result = await tools["airflow_admin"](action="reset_circuit_breaker")

        assert result["success"] is False
        assert "not enabled" in result["message"]


# =============================================================================
# Test: get_task_logs (M3 truncation tests)
# =============================================================================


class TestGetTaskLogsTruncation:
    """M3: get_task_logs truncates oversized logs to 100KB."""

    @pytest.mark.asyncio
    async def test_short_logs_not_truncated(self):
        """Logs under 100KB are returned in full."""
        orch = _make_orchestrator()
        short_logs = "x" * 1000
        orch.async_airflow_client.get_task_logs = AsyncMock(return_value=short_logs)
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="task_logs",
            pipeline_name="dag1", dag_run_id="run1", task_id="task1"
        )
        assert result["truncated"] is False
        assert result["log_length"] == 1000
        assert result["total_length"] == 1000
        assert result["logs"] == short_logs

    @pytest.mark.asyncio
    async def test_get_task_logs_called_without_full_content(self):
        """get_task_logs is called with positional args only — no full_content kwarg."""
        orch = _make_orchestrator()
        orch.async_airflow_client.get_task_logs = AsyncMock(return_value="log output")
        tools = register_orchestration_tools(orch)

        await tools["dag_monitor"](
            query="task_logs",
            pipeline_name="my_dag",
            dag_run_id="run_123",
            task_id="my_task",
            try_number=2,
        )
        orch.async_airflow_client.get_task_logs.assert_called_once_with(
            "my_dag", "run_123", "my_task", 2
        )

    @pytest.mark.asyncio
    async def test_logs_at_exactly_100kb_not_truncated(self):
        """Logs at exactly 100KB boundary are not truncated."""
        orch = _make_orchestrator()
        exact_logs = "x" * 100_000
        orch.async_airflow_client.get_task_logs = AsyncMock(return_value=exact_logs)
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="task_logs",
            pipeline_name="dag1", dag_run_id="run1", task_id="task1"
        )
        assert result["truncated"] is False
        assert result["log_length"] == 100_000
        assert result["total_length"] == 100_000

    @pytest.mark.asyncio
    async def test_logs_over_100kb_truncated(self):
        """Logs over 100KB are truncated and metadata reflects it."""
        orch = _make_orchestrator()
        oversized_logs = "x" * 200_000
        orch.async_airflow_client.get_task_logs = AsyncMock(return_value=oversized_logs)
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="task_logs",
            pipeline_name="dag1", dag_run_id="run1", task_id="task1"
        )
        assert result["truncated"] is True
        assert result["log_length"] == 100_000
        assert result["total_length"] == 200_000
        assert len(result["logs"]) == 100_000

    @pytest.mark.asyncio
    async def test_none_logs_handled(self):
        """None logs handled gracefully."""
        orch = _make_orchestrator()
        orch.async_airflow_client.get_task_logs = AsyncMock(return_value=None)
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="task_logs",
            pipeline_name="dag1", dag_run_id="run1", task_id="task1"
        )
        assert result["truncated"] is False
        assert result["total_length"] == 0
        assert result["log_length"] == 0

    @pytest.mark.asyncio
    async def test_error_uses_safe_error_message(self):
        """Errors use safe_error_message with exception type."""
        orch = _make_orchestrator()
        orch.async_airflow_client.get_task_logs = AsyncMock(
            side_effect=ConnectionError("password=secret123 refused")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="task_logs",
            pipeline_name="dag1", dag_run_id="run1", task_id="task1"
        )
        assert "error" in result
        assert "ConnectionError" in result["error"]
        # Credentials should be redacted
        assert "secret123" not in result["error"]


# =============================================================================
# Test: retry_failed_tasks (M4 tests)
# =============================================================================


class TestRetryFailedTasksM4:
    """M4: retry_failed_tasks supports per-task retry via clear_dag_run(task_ids=...)."""

    @pytest.mark.asyncio
    async def test_per_task_retry_clears_specific_tasks(self):
        """Passing task_ids clears only those failed tasks."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_task_instances = AsyncMock(
            return_value=[
                {"task_id": "task_a", "state": "failed"},
                {"task_id": "task_b", "state": "failed"},
                {"task_id": "task_c", "state": "success"},
            ]
        )
        orch.async_airflow_client.clear_dag_run = AsyncMock()
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="retry_failed",
            pipeline_name="dag1",
            dag_run_id="run1",
            task_ids=["task_a"],
        )
        assert result["success"] is True
        assert result["retried_count"] == 1
        assert result["retry_results"][0]["task_id"] == "task_a"
        assert result["retry_results"][0]["status"] == "queued"
        assert "Cleared 1 specific failed task(s)" in result["message"]
        assert result["total_failed"] == 2
        orch.async_airflow_client.clear_dag_run.assert_called_once_with(
            "dag1", "run1", task_ids=["task_a"]
        )

    @pytest.mark.asyncio
    async def test_per_task_retry_filters_to_failed_only(self):
        """task_ids containing non-failed tasks retries only the failed ones."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_task_instances = AsyncMock(
            return_value=[
                {"task_id": "task_a", "state": "failed"},
                {"task_id": "task_b", "state": "failed"},
                {"task_id": "task_c", "state": "success"},
            ]
        )
        orch.async_airflow_client.clear_dag_run = AsyncMock()
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="retry_failed",
            pipeline_name="dag1",
            dag_run_id="run1",
            task_ids=["task_a", "task_c"],
        )
        assert result["success"] is True
        assert result["retried_count"] == 1
        assert len(result["retry_results"]) == 1
        assert result["retry_results"][0]["task_id"] == "task_a"

    @pytest.mark.asyncio
    async def test_per_task_retry_no_matching_failed(self):
        """task_ids with no matching failed tasks returns success with no-op message."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_task_instances = AsyncMock(
            return_value=[
                {"task_id": "task_a", "state": "failed"},
                {"task_id": "task_c", "state": "success"},
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="retry_failed",
            pipeline_name="dag1",
            dag_run_id="run1",
            task_ids=["task_c"],
        )
        assert result["success"] is True
        assert result["retried_count"] == 0
        assert "No failed tasks" in result["message"]

    @pytest.mark.asyncio
    async def test_retry_all_failed_tasks_succeeds(self):
        """Retrying all failed tasks (no task_ids) clears DAG run."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_task_instances = AsyncMock(
            return_value=[
                {"task_id": "task_a", "state": "failed"},
                {"task_id": "task_b", "state": "success"},
            ]
        )
        orch.async_airflow_client.clear_dag_run = AsyncMock()
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="retry_failed",
            pipeline_name="dag1",
            dag_run_id="run1",
        )
        assert result["success"] is True
        assert result["retried_count"] == 1
        assert result["retry_results"][0]["task_id"] == "task_a"
        assert result["retry_results"][0]["status"] == "queued"
        assert "Cleared DAG run" in result["message"]

    @pytest.mark.asyncio
    async def test_no_failed_tasks_reports_success(self):
        """No failed tasks → success=True with appropriate message."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_task_instances = AsyncMock(
            return_value=[
                {"task_id": "task_a", "state": "success"},
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="retry_failed",
            pipeline_name="dag1",
            dag_run_id="run1",
        )
        assert result["success"] is True
        assert result["total_failed"] == 0
        assert result["retried_count"] == 0
        assert "No failed tasks" in result["message"]

    @pytest.mark.asyncio
    async def test_clear_dag_run_failure(self):
        """When clear_dag_run fails, tasks get error status."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_task_instances = AsyncMock(
            return_value=[
                {"task_id": "task_a", "state": "failed"},
            ]
        )
        orch.async_airflow_client.clear_dag_run = AsyncMock(
            side_effect=Exception("API error")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="retry_failed",
            pipeline_name="dag1",
            dag_run_id="run1",
        )
        assert result["success"] is False
        assert result["retried_count"] == 0
        assert result["retry_results"][0]["status"] == "error"

    @pytest.mark.asyncio
    async def test_retry_failed_tasks_exception(self):
        """Handle exception during retry gracefully."""
        orch = _make_orchestrator()
        orch.async_airflow_client.list_task_instances = AsyncMock(
            side_effect=Exception("Network error")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="retry_failed",
            pipeline_name="dag1",
            dag_run_id="run1",
        )
        assert result["success"] is False
        assert "error" in result


# =============================================================================
# Test: dag_trigger — idempotent mode
# =============================================================================


class TestTriggerDagRunIdempotent:
    """Tests for dag_trigger mode='idempotent'."""

    @pytest.mark.asyncio
    async def test_idempotent_new_run(self):
        """Trigger a new DAG run with idempotency key."""
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag_idempotent = AsyncMock(
            return_value={
                "dag_run_id": "manual__2025-06-01T00:00:00",
                "state": "queued",
                "idempotent_reused": False,
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="idempotent",
            pipeline_name="test_dag",
            idempotency_key="batch-2025-06-01",
        )

        assert result["success"] is True
        assert result["idempotent_reused"] is False
        assert "Created new DAG run" in result["message"]
        assert result["idempotency_key"] == "batch-2025-06-01"

    @pytest.mark.asyncio
    async def test_idempotent_reused_run(self):
        """Reuse an existing DAG run for the same idempotency key."""
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag_idempotent = AsyncMock(
            return_value={
                "dag_run_id": "manual__2025-06-01T00:00:00",
                "state": "success",
                "idempotent_reused": True,
            }
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="idempotent",
            pipeline_name="test_dag",
            idempotency_key="batch-2025-06-01",
        )

        assert result["success"] is True
        assert result["idempotent_reused"] is True
        assert "Reused existing DAG run" in result["message"]

    @pytest.mark.asyncio
    async def test_idempotent_missing_key(self):
        """Missing idempotency_key returns error."""
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="idempotent",
            pipeline_name="test_dag",
        )

        assert result["success"] is False
        assert "idempotency_key" in result["error"]

    @pytest.mark.asyncio
    async def test_idempotent_missing_pipeline(self):
        """Missing pipeline_name returns error."""
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="idempotent",
            idempotency_key="key1",
        )

        assert result["success"] is False
        assert "pipeline_name" in result["error"]

    @pytest.mark.asyncio
    async def test_idempotent_failure(self):
        """Handle exception from idempotent trigger."""
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag_idempotent = AsyncMock(
            side_effect=RuntimeError("conflict")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="idempotent",
            pipeline_name="test_dag",
            idempotency_key="key1",
        )

        assert result["success"] is False
        assert "RuntimeError" in result["error"]


# =============================================================================
# Test: dag_trigger — multiple mode
# =============================================================================


class TestTriggerDagRunMultiple:
    """Tests for dag_trigger mode='multiple'."""

    @pytest.mark.asyncio
    async def test_multiple_success(self):
        """Trigger multiple DAGs concurrently."""
        orch = _make_orchestrator()
        orch.async_trigger_multiple_dags = AsyncMock(
            return_value=[
                {"dag_run_id": "run_a", "dag_id": "dag_a"},
                {"dag_run_id": "run_b", "dag_id": "dag_b"},
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="multiple",
            dag_configs=[
                {"dag_id": "dag_a", "conf": {}},
                {"dag_id": "dag_b", "conf": {}},
            ],
        )

        assert result["success"] is True
        assert result["total_triggered"] == 2
        assert result["total_failed"] == 0

    @pytest.mark.asyncio
    async def test_multiple_empty_list(self):
        """Empty dag_configs returns success with zero triggered."""
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](mode="multiple", dag_configs=[])

        assert result["success"] is True
        assert result["total_triggered"] == 0

    @pytest.mark.asyncio
    async def test_multiple_defaults_to_empty(self):
        """Omitting dag_configs defaults to empty list."""
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](mode="multiple")

        assert result["success"] is True
        assert result["total_triggered"] == 0

    @pytest.mark.asyncio
    async def test_multiple_partial_failure(self):
        """Some DAGs fail while others succeed."""
        orch = _make_orchestrator()
        orch.async_trigger_multiple_dags = AsyncMock(
            return_value=[
                {"dag_run_id": "run_a", "dag_id": "dag_a"},
                {"error": "DAG not found", "dag_id": "dag_b"},  # no dag_run_id
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="multiple",
            dag_configs=[
                {"dag_id": "dag_a"},
                {"dag_id": "dag_b"},
            ],
        )

        assert result["success"] is False
        assert result["total_triggered"] == 1
        assert result["total_failed"] == 1

    @pytest.mark.asyncio
    async def test_multiple_exception(self):
        """Handle exception from trigger_multiple_dags."""
        orch = _make_orchestrator()
        orch.async_trigger_multiple_dags = AsyncMock(
            side_effect=Exception("batch error")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_trigger"](
            mode="multiple",
            dag_configs=[{"dag_id": "dag_a"}],
        )

        assert result["success"] is False
        assert "error" in result


# =============================================================================
# Test: dag_monitor — list_dags query
# =============================================================================


class TestListDags:
    """Tests for dag_monitor query='list_dags'."""

    @pytest.mark.asyncio
    async def test_list_dags_success(self):
        """List available DAGs."""
        orch = _make_orchestrator()
        orch.async_list_dags = AsyncMock(
            return_value=[
                {"dag_id": "dag_a", "is_paused": False},
                {"dag_id": "dag_b", "is_paused": True},
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="list_dags")

        assert result["total_count"] == 2
        assert len(result["dags"]) == 2

    @pytest.mark.asyncio
    async def test_list_dags_with_tags(self):
        """List DAGs filtered by tags."""
        orch = _make_orchestrator()
        orch.async_list_dags = AsyncMock(
            return_value=[
                {"dag_id": "dag_a", "is_paused": False, "tags": [{"name": "teradata"}]},
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="list_dags", tags=["teradata"])

        orch.async_list_dags.assert_called_once_with(limit=10, only_active=True, tags=["teradata"])
        assert result["total_count"] == 1

    @pytest.mark.asyncio
    async def test_list_dags_without_tags(self):
        """List DAGs without tag filter passes tags=None."""
        orch = _make_orchestrator()
        orch.async_list_dags = AsyncMock(
            return_value=[
                {"dag_id": "dag_a", "is_paused": False},
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="list_dags")

        orch.async_list_dags.assert_called_once_with(limit=10, only_active=True, tags=None)
        assert result["total_count"] == 1

    @pytest.mark.asyncio
    async def test_list_dags_tags_in_filters(self):
        """Verify the response filters dict includes the tags value."""
        orch = _make_orchestrator()
        orch.async_list_dags = AsyncMock(
            return_value=[
                {"dag_id": "dag_a", "is_paused": False},
            ]
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="list_dags", tags=["teradata", "daily"])

        assert result["filters"]["tags"] == ["teradata", "daily"]

    @pytest.mark.asyncio
    async def test_list_dags_error(self):
        """Handle error from list_dags."""
        orch = _make_orchestrator()
        orch.async_list_dags = AsyncMock(
            side_effect=ConnectionError("timeout")
        )
        tools = register_orchestration_tools(orch)

        result = await tools["dag_monitor"](query="list_dags")

        assert "error" in result
        assert "ConnectionError" in result["error"]


# =============================================================================
# Test: null guards and invalid values
# =============================================================================


class TestNullGuards:
    """Tests for null guards and invalid dispatch values across all 3 routers."""

    @pytest.mark.asyncio
    async def test_dag_trigger_none_mode(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["dag_trigger"](mode=None, pipeline_name="dag1")
        assert result["success"] is False
        assert "non-empty string" in result["error"]

    @pytest.mark.asyncio
    async def test_dag_trigger_empty_mode(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["dag_trigger"](mode="", pipeline_name="dag1")
        assert result["success"] is False
        assert "non-empty string" in result["error"]

    @pytest.mark.asyncio
    async def test_dag_trigger_invalid_mode(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["dag_trigger"](mode="bogus", pipeline_name="dag1")
        assert result["success"] is False
        assert "Unknown mode" in result["error"]

    @pytest.mark.asyncio
    async def test_dag_monitor_none_query(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["dag_monitor"](query=None, pipeline_name="dag1")
        assert result["success"] is False
        assert "non-empty string" in result["error"]

    @pytest.mark.asyncio
    async def test_dag_monitor_empty_query(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["dag_monitor"](query="  ", pipeline_name="dag1")
        assert result["success"] is False
        assert "non-empty string" in result["error"]

    @pytest.mark.asyncio
    async def test_dag_monitor_invalid_query(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["dag_monitor"](query="bogus", pipeline_name="dag1")
        assert result["success"] is False
        assert "Unknown query" in result["error"]

    @pytest.mark.asyncio
    async def test_airflow_admin_none_action(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["airflow_admin"](action=None)
        assert result["success"] is False
        assert "non-empty string" in result["error"]

    @pytest.mark.asyncio
    async def test_airflow_admin_invalid_action(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["airflow_admin"](action="restart")
        assert result["success"] is False
        assert "Unknown action" in result["error"]


# =============================================================================
# Test: parameter validation
# =============================================================================


class TestParameterValidation:
    """Tests for numeric bounds validation."""

    @pytest.mark.asyncio
    async def test_dag_monitor_limit_zero(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["dag_monitor"](query="list_runs", pipeline_name="dag1", limit=0)
        assert result["success"] is False
        assert "limit" in result["error"]

    @pytest.mark.asyncio
    async def test_dag_monitor_try_number_zero(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["dag_monitor"](
            query="task_logs", pipeline_name="dag1",
            dag_run_id="run1", task_id="t1", try_number=0,
        )
        assert result["success"] is False
        assert "try_number" in result["error"]

    @pytest.mark.asyncio
    async def test_dag_trigger_run_missing_pipeline(self):
        orch = _make_orchestrator()
        tools = register_orchestration_tools(orch)
        result = await tools["dag_trigger"](mode="run")
        assert result["success"] is False
        assert "pipeline_name" in result["error"]


# ════════════════════════════════════════════════════════════════════
#  next_steps shape & coverage — verify the 4-part Markdown prose
#  template used across orchestration_execution success responses
# ════════════════════════════════════════════════════════════════════


def _assert_next_steps_shape(steps):
    """Assert ``steps`` is a list of 4-part Markdown-prose strings."""
    assert isinstance(steps, list) and len(steps) >= 1, (
        f"next_steps should be a non-empty list, got: {steps!r}"
    )
    for i, s in enumerate(steps, start=1):
        assert isinstance(s, str), (
            f"next_steps[{i - 1}] must be a Markdown-prose str, "
            f"got {type(s).__name__}: {s!r}"
        )
        assert "**" in s and f"**{i}." in s, (
            f"next_steps[{i - 1}] missing numbered header: {s!r}"
        )
        for segment in ("**Why**", "**Effect**", "**If missing**"):
            assert segment in s, (
                f"next_steps[{i - 1}] missing {segment}: {s!r}"
            )


class TestNextStepsShape:
    """Verifies the next_steps field on dag_trigger success responses."""

    @pytest.mark.asyncio
    async def test_trigger_run_no_wait_emits_next_steps(self):
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag = AsyncMock(
            return_value={
                "dag_run_id": "run-1",
                "execution_date": "2025-01-01T00:00:00",
                "state": "queued",
            }
        )
        tools = register_orchestration_tools(orch)
        result = await tools["dag_trigger"](mode="run", pipeline_name="dag1")
        assert result["success"] is True
        _assert_next_steps_shape(result["next_steps"])

    @pytest.mark.asyncio
    async def test_trigger_run_wait_success_emits_next_steps(self):
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag = AsyncMock(
            return_value={
                "dag_run_id": "run-2",
                "state": "success",
                "final_status": "success",
            }
        )
        tools = register_orchestration_tools(orch)
        result = await tools["dag_trigger"](
            mode="run", pipeline_name="dag1", wait_for_completion=True
        )
        assert result["success"] is True
        _assert_next_steps_shape(result["next_steps"])

    @pytest.mark.asyncio
    async def test_trigger_run_wait_failed_emits_next_steps(self):
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag = AsyncMock(
            return_value={
                "dag_run_id": "run-3",
                "state": "failed",
                "final_status": "failed",
            }
        )
        tools = register_orchestration_tools(orch)
        result = await tools["dag_trigger"](
            mode="run", pipeline_name="dag1", wait_for_completion=True
        )
        assert result["success"] is True
        _assert_next_steps_shape(result["next_steps"])

    @pytest.mark.asyncio
    async def test_trigger_idempotent_new_run_emits_next_steps(self):
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag_idempotent = AsyncMock(
            return_value={
                "dag_run_id": "idem-1",
                "idempotent_reused": False,
                "state": "queued",
            }
        )
        tools = register_orchestration_tools(orch)
        result = await tools["dag_trigger"](
            mode="idempotent",
            pipeline_name="dag1",
            idempotency_key="key-1",
        )
        assert result["success"] is True
        _assert_next_steps_shape(result["next_steps"])

    @pytest.mark.asyncio
    async def test_trigger_idempotent_reused_emits_next_steps(self):
        orch = _make_orchestrator()
        orch.async_trigger_airflow_dag_idempotent = AsyncMock(
            return_value={
                "dag_run_id": "idem-2",
                "idempotent_reused": True,
                "state": "running",
            }
        )
        tools = register_orchestration_tools(orch)
        result = await tools["dag_trigger"](
            mode="idempotent",
            pipeline_name="dag1",
            idempotency_key="key-2",
        )
        assert result["success"] is True
        _assert_next_steps_shape(result["next_steps"])
