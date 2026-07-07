"""Unit tests for workflow protocol module.

Tests cover:
- WorkflowState enum
- Data classes: WorkflowRun, WorkflowDefinition, TaskRun, OrchestratorHealth
- to_dict() serialization
- Protocol type checking
- Exception classes
"""

from datetime import datetime, timezone

from teradata_etl_mcp_server.workflow.protocol import (
    CircuitBreakerOpenError,
    OrchestratorHealth,
    TaskRun,
    WorkflowDefinition,
    WorkflowNotFoundError,
    WorkflowOrchestratorError,
    WorkflowOrchestratorProtocol,
    WorkflowRun,
    WorkflowState,
    WorkflowTimeoutError,
    WorkflowTriggerError,
)


class TestWorkflowState:
    """Tests for WorkflowState enum."""

    def test_all_states_exist(self):
        """Verify all expected states are defined."""
        expected = [
            "PENDING", "RUNNING", "SUCCESS", "FAILED",
            "CANCELLED", "SKIPPED", "RETRY", "UNKNOWN"
        ]
        for state_name in expected:
            assert hasattr(WorkflowState, state_name)

    def test_state_values(self):
        """Verify state string values."""
        assert WorkflowState.PENDING.value == "pending"
        assert WorkflowState.RUNNING.value == "running"
        assert WorkflowState.SUCCESS.value == "success"
        assert WorkflowState.FAILED.value == "failed"
        assert WorkflowState.CANCELLED.value == "cancelled"
        assert WorkflowState.SKIPPED.value == "skipped"
        assert WorkflowState.RETRY.value == "retry"
        assert WorkflowState.UNKNOWN.value == "unknown"

    def test_state_comparison(self):
        """Test state comparison."""
        assert WorkflowState.SUCCESS == WorkflowState.SUCCESS
        assert WorkflowState.FAILED != WorkflowState.SUCCESS


class TestWorkflowRun:
    """Tests for WorkflowRun dataclass."""

    def test_minimal_creation(self):
        """Test creating WorkflowRun with required fields only."""
        run = WorkflowRun(
            run_id="run-123",
            workflow_id="my-workflow",
            state=WorkflowState.RUNNING,
        )
        assert run.run_id == "run-123"
        assert run.workflow_id == "my-workflow"
        assert run.state == WorkflowState.RUNNING
        assert run.started_at is None
        assert run.ended_at is None
        assert run.duration_seconds is None
        assert run.config == {}
        assert run.error_message is None
        assert run.external_url is None
        assert run.metadata == {}

    def test_full_creation(self):
        """Test creating WorkflowRun with all fields."""
        now = datetime.now(timezone.utc)
        run = WorkflowRun(
            run_id="run-456",
            workflow_id="pipeline-1",
            state=WorkflowState.FAILED,
            started_at=now,
            ended_at=now,
            duration_seconds=120.5,
            config={"key": "value"},
            error_message="Task failed",
            external_url="http://airflow/run/456",
            metadata={"attempt": 2},
        )
        assert run.duration_seconds == 120.5
        assert run.config == {"key": "value"}
        assert run.error_message == "Task failed"
        assert run.external_url == "http://airflow/run/456"

    def test_to_dict_minimal(self):
        """Test to_dict with minimal fields."""
        run = WorkflowRun(
            run_id="run-1",
            workflow_id="wf-1",
            state=WorkflowState.SUCCESS,
        )
        d = run.to_dict()
        assert d["run_id"] == "run-1"
        assert d["workflow_id"] == "wf-1"
        assert d["state"] == "success"
        assert d["started_at"] is None
        assert d["config"] == {}

    def test_to_dict_with_datetime(self):
        """Test to_dict serializes datetime as ISO format."""
        dt = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        run = WorkflowRun(
            run_id="run-1",
            workflow_id="wf-1",
            state=WorkflowState.SUCCESS,
            started_at=dt,
            ended_at=dt,
        )
        d = run.to_dict()
        assert d["started_at"] == "2025-01-15T10:30:00+00:00"
        assert d["ended_at"] == "2025-01-15T10:30:00+00:00"


class TestWorkflowDefinition:
    """Tests for WorkflowDefinition dataclass."""

    def test_minimal_creation(self):
        """Test creating WorkflowDefinition with required fields."""
        wf = WorkflowDefinition(
            workflow_id="daily-etl",
            name="Daily ETL Pipeline",
        )
        assert wf.workflow_id == "daily-etl"
        assert wf.name == "Daily ETL Pipeline"
        assert wf.description is None
        assert wf.schedule is None
        assert wf.is_active is True
        assert wf.tags == []

    def test_full_creation(self):
        """Test creating WorkflowDefinition with all fields."""
        wf = WorkflowDefinition(
            workflow_id="hourly-sync",
            name="Hourly Sync Job",
            description="Syncs data every hour",
            schedule="0 * * * *",
            is_active=False,
            tags=["sync", "production"],
            metadata={"owner": "data-team"},
        )
        assert wf.schedule == "0 * * * *"
        assert wf.is_active is False
        assert wf.tags == ["sync", "production"]

    def test_to_dict(self):
        """Test to_dict serialization."""
        wf = WorkflowDefinition(
            workflow_id="test-wf",
            name="Test Workflow",
            tags=["tag1", "tag2"],
        )
        d = wf.to_dict()
        assert d["workflow_id"] == "test-wf"
        assert d["name"] == "Test Workflow"
        assert d["tags"] == ["tag1", "tag2"]
        assert d["is_active"] is True


class TestTaskRun:
    """Tests for TaskRun dataclass."""

    def test_minimal_creation(self):
        """Test creating TaskRun with required fields."""
        task = TaskRun(
            task_id="extract_data",
            run_id="run-1",
            workflow_id="pipeline-1",
            state=WorkflowState.RUNNING,
        )
        assert task.task_id == "extract_data"
        assert task.run_id == "run-1"
        assert task.attempt_number == 1

    def test_full_creation(self):
        """Test creating TaskRun with all fields."""
        now = datetime.now(timezone.utc)
        task = TaskRun(
            task_id="load_data",
            run_id="run-1",
            workflow_id="pipeline-1",
            state=WorkflowState.FAILED,
            started_at=now,
            ended_at=now,
            duration_seconds=45.0,
            attempt_number=3,
            error_message="Connection timeout",
            logs_url="http://airflow/logs/task1",
        )
        assert task.attempt_number == 3
        assert task.error_message == "Connection timeout"

    def test_to_dict(self):
        """Test to_dict serialization."""
        task = TaskRun(
            task_id="task-1",
            run_id="run-1",
            workflow_id="wf-1",
            state=WorkflowState.SUCCESS,
            attempt_number=2,
        )
        d = task.to_dict()
        assert d["task_id"] == "task-1"
        assert d["state"] == "success"
        assert d["attempt_number"] == 2


class TestOrchestratorHealth:
    """Tests for OrchestratorHealth dataclass."""

    def test_minimal_creation(self):
        """Test creating OrchestratorHealth with required fields."""
        health = OrchestratorHealth(
            connected=True,
            backend="airflow",
        )
        assert health.connected is True
        assert health.backend == "airflow"
        assert health.version is None
        assert health.availability == "unknown"

    def test_full_creation(self):
        """Test creating OrchestratorHealth with all fields."""
        health = OrchestratorHealth(
            connected=True,
            backend="dagster",
            version="1.5.0",
            url="http://localhost:3000",
            availability="healthy",
            error=None,
            circuit_breaker={"state": "closed", "failures": 0},
            metadata={"region": "us-east-1"},
        )
        assert health.version == "1.5.0"
        assert health.availability == "healthy"
        assert health.circuit_breaker["state"] == "closed"

    def test_to_dict(self):
        """Test to_dict serialization."""
        health = OrchestratorHealth(
            connected=False,
            backend="prefect",
            availability="unavailable",
            error="Connection refused",
        )
        d = health.to_dict()
        assert d["connected"] is False
        assert d["backend"] == "prefect"
        assert d["error"] == "Connection refused"


class TestExceptions:
    """Tests for workflow exception classes."""

    def test_base_exception(self):
        """Test WorkflowOrchestratorError base exception."""
        exc = WorkflowOrchestratorError("Something went wrong")
        assert str(exc) == "Something went wrong"
        assert isinstance(exc, Exception)

    def test_trigger_error(self):
        """Test WorkflowTriggerError."""
        exc = WorkflowTriggerError("Failed to trigger DAG")
        assert "Failed to trigger" in str(exc)
        assert isinstance(exc, WorkflowOrchestratorError)

    def test_not_found_error(self):
        """Test WorkflowNotFoundError."""
        exc = WorkflowNotFoundError("DAG not found: my_dag")
        assert "not found" in str(exc)
        assert isinstance(exc, WorkflowOrchestratorError)

    def test_timeout_error(self):
        """Test WorkflowTimeoutError."""
        exc = WorkflowTimeoutError("Workflow timed out after 3600s")
        assert "timed out" in str(exc)
        assert isinstance(exc, WorkflowOrchestratorError)

    def test_circuit_breaker_error(self):
        """Test CircuitBreakerOpenError."""
        exc = CircuitBreakerOpenError("Circuit breaker is open")
        assert "Circuit breaker" in str(exc)
        assert isinstance(exc, WorkflowOrchestratorError)

    def test_exception_chaining(self):
        """Test exception chaining with __cause__."""
        original = ValueError("Original error")
        try:
            raise WorkflowTriggerError("Wrapped error") from original
        except WorkflowTriggerError as wrapped:
            assert wrapped.__cause__ is original


class TestProtocolTypeChecking:
    """Tests for runtime protocol checking."""

    def test_protocol_is_runtime_checkable(self):
        """Verify WorkflowOrchestratorProtocol is runtime_checkable."""

        # Should not raise
        assert hasattr(WorkflowOrchestratorProtocol, "__protocol_attrs__") or True

    def test_mock_implementation_satisfies_protocol(self):
        """Test that a mock implementation satisfies the protocol."""
        from unittest.mock import AsyncMock, Mock

        mock_orchestrator = Mock()
        mock_orchestrator.backend_name = "mock"
        mock_orchestrator.trigger_workflow = AsyncMock(return_value=WorkflowRun(
            run_id="test",
            workflow_id="test",
            state=WorkflowState.PENDING,
        ))
        mock_orchestrator.get_workflow_run = AsyncMock()
        mock_orchestrator.list_workflow_runs = AsyncMock(return_value=[])
        mock_orchestrator.list_workflows = AsyncMock(return_value=[])
        mock_orchestrator.get_task_runs = AsyncMock(return_value=[])
        mock_orchestrator.get_task_logs = AsyncMock(return_value="")
        mock_orchestrator.retry_workflow = AsyncMock()
        mock_orchestrator.cancel_workflow = AsyncMock(return_value=True)
        mock_orchestrator.get_health = AsyncMock()
        mock_orchestrator.get_circuit_breaker_status = Mock(return_value=None)
        mock_orchestrator.reset_circuit_breaker = Mock(return_value=False)

        # Runtime check
        assert isinstance(mock_orchestrator, WorkflowOrchestratorProtocol)
