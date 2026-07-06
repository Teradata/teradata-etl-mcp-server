"""Workflow orchestrator protocol for pluggable orchestration backends.

This module defines the abstract protocol that all workflow orchestrators
(Airflow, Dagster, Prefect, etc.) must implement to be used with the
ELT MCP Server.

The protocol provides a unified interface for:
- Triggering workflow executions
- Monitoring workflow status
- Managing workflow runs
- Health checks

Example:
    # Using the protocol with dependency injection
    orchestrator: WorkflowOrchestratorProtocol = AirflowOrchestrator(client)
    result = await orchestrator.trigger_workflow("my_pipeline", config={"key": "value"})
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class WorkflowState(Enum):
    """Unified workflow execution states across all orchestrators.

    Maps to backend-specific states:
    - Airflow: queued, running, success, failed, etc.
    - Dagster: QUEUED, STARTED, SUCCESS, FAILURE, etc.
    - Prefect: Pending, Running, Completed, Failed, etc.
    """

    PENDING = "pending"      # Queued/waiting to run
    RUNNING = "running"      # Currently executing
    SUCCESS = "success"      # Completed successfully
    FAILED = "failed"        # Execution failed
    CANCELLED = "cancelled"  # Manually cancelled
    SKIPPED = "skipped"      # Skipped (upstream failure, etc.)
    RETRY = "retry"          # Scheduled for retry
    UNKNOWN = "unknown"      # State cannot be determined


@dataclass
class WorkflowRun:
    """Unified workflow run representation.

    Provides a consistent structure for workflow run information
    regardless of the underlying orchestrator.
    """

    run_id: str
    workflow_id: str
    state: WorkflowState
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_seconds: float | None = None
    config: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None
    external_url: str | None = None  # Link to orchestrator UI
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "state": self.state.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "config": self.config,
            "error_message": self.error_message,
            "external_url": self.external_url,
            "metadata": self.metadata,
        }


@dataclass
class WorkflowDefinition:
    """Unified workflow definition representation."""

    workflow_id: str
    name: str
    description: str | None = None
    schedule: str | None = None  # Cron expression or interval
    is_active: bool = True
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "description": self.description,
            "schedule": self.schedule,
            "is_active": self.is_active,
            "tags": self.tags,
            "metadata": self.metadata,
        }


@dataclass
class TaskRun:
    """Unified task/step run representation."""

    task_id: str
    run_id: str
    workflow_id: str
    state: WorkflowState
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_seconds: float | None = None
    attempt_number: int = 1
    error_message: str | None = None
    logs_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "task_id": self.task_id,
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "state": self.state.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "attempt_number": self.attempt_number,
            "error_message": self.error_message,
            "logs_url": self.logs_url,
        }


@dataclass
class OrchestratorHealth:
    """Unified orchestrator health status."""

    connected: bool
    backend: str  # "airflow", "dagster", "prefect"
    version: str | None = None
    url: str | None = None
    availability: str = "unknown"  # "healthy", "degraded", "unavailable"
    error: str | None = None
    circuit_breaker: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "connected": self.connected,
            "backend": self.backend,
            "version": self.version,
            "url": self.url,
            "availability": self.availability,
            "error": self.error,
            "circuit_breaker": self.circuit_breaker,
            "metadata": self.metadata,
        }


@runtime_checkable
class WorkflowOrchestratorProtocol(Protocol):
    """Protocol defining the interface for workflow orchestrators.

    All workflow orchestrator implementations (Airflow, Dagster, Prefect)
    must implement this protocol to be compatible with the ELT MCP Server.

    This enables:
    - Swapping orchestrators without changing tool implementations
    - Testing with mock orchestrators
    - Supporting multiple orchestrators in the same deployment
    """

    @property
    def backend_name(self) -> str:
        """Get the backend orchestrator name (e.g., 'airflow', 'dagster')."""
        ...

    async def trigger_workflow(
        self,
        workflow_id: str,
        config: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        wait_for_completion: bool = False,
        timeout_seconds: int = 3600,
    ) -> WorkflowRun:
        """Trigger a workflow execution.

        Args:
            workflow_id: Unique identifier for the workflow (DAG ID, job name, flow ID)
            config: Optional configuration/parameters to pass to the workflow
            idempotency_key: Optional key for idempotent triggering
            wait_for_completion: Whether to wait for the workflow to complete
            timeout_seconds: Timeout for waiting (if wait_for_completion=True)

        Returns:
            WorkflowRun with execution details

        Raises:
            WorkflowTriggerError: If workflow cannot be triggered
        """
        ...

    async def get_workflow_run(
        self,
        workflow_id: str,
        run_id: str,
    ) -> WorkflowRun:
        """Get details of a specific workflow run.

        Args:
            workflow_id: Workflow identifier
            run_id: Run identifier

        Returns:
            WorkflowRun with current status

        Raises:
            WorkflowNotFoundError: If workflow or run not found
        """
        ...

    async def list_workflow_runs(
        self,
        workflow_id: str,
        limit: int = 10,
        state: WorkflowState | None = None,
        start_date_gte: datetime | None = None,
        start_date_lte: datetime | None = None,
    ) -> list[WorkflowRun]:
        """List recent workflow runs with optional filtering.

        Args:
            workflow_id: Workflow identifier
            limit: Maximum number of runs to return
            state: Filter by workflow state
            start_date_gte: Filter runs starting after this date
            start_date_lte: Filter runs starting before this date

        Returns:
            List of WorkflowRun objects
        """
        ...

    async def list_workflows(
        self,
        limit: int = 100,
        only_active: bool = True,
    ) -> list[WorkflowDefinition]:
        """List available workflows.

        Args:
            limit: Maximum number of workflows to return
            only_active: Only return active/unpaused workflows

        Returns:
            List of WorkflowDefinition objects
        """
        ...

    async def get_task_runs(
        self,
        workflow_id: str,
        run_id: str,
    ) -> list[TaskRun]:
        """Get task/step runs for a workflow execution.

        Args:
            workflow_id: Workflow identifier
            run_id: Run identifier

        Returns:
            List of TaskRun objects
        """
        ...

    async def get_task_logs(
        self,
        workflow_id: str,
        run_id: str,
        task_id: str,
        attempt_number: int = 1,
    ) -> str:
        """Get logs for a specific task execution.

        Args:
            workflow_id: Workflow identifier
            run_id: Run identifier
            task_id: Task identifier
            attempt_number: Which attempt to get logs for

        Returns:
            Log content as string
        """
        ...

    async def retry_workflow(
        self,
        workflow_id: str,
        run_id: str,
        task_ids: list[str] | None = None,
    ) -> WorkflowRun:
        """Retry a failed workflow or specific tasks.

        Args:
            workflow_id: Workflow identifier
            run_id: Run identifier to retry
            task_ids: Optional list of specific tasks to retry

        Returns:
            WorkflowRun for the retry execution
        """
        ...

    async def cancel_workflow(
        self,
        workflow_id: str,
        run_id: str,
    ) -> bool:
        """Cancel a running workflow.

        Args:
            workflow_id: Workflow identifier
            run_id: Run identifier to cancel

        Returns:
            True if cancellation was successful
        """
        ...

    async def get_health(self) -> OrchestratorHealth:
        """Get orchestrator health status.

        Returns:
            OrchestratorHealth with connection status and details
        """
        ...

    def get_circuit_breaker_status(self) -> dict[str, Any] | None:
        """Get circuit breaker status if enabled.

        Returns:
            Circuit breaker status dict or None if not enabled
        """
        ...

    def reset_circuit_breaker(self) -> bool:
        """Reset the circuit breaker to closed state.

        Returns:
            True if reset was successful
        """
        ...


class WorkflowOrchestratorBase(ABC):
    """Abstract base class for workflow orchestrator implementations.

    Provides common functionality and enforces the protocol interface.
    Implementations should inherit from this class.
    """

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """Get the backend orchestrator name."""
        ...

    @abstractmethod
    async def trigger_workflow(
        self,
        workflow_id: str,
        config: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        wait_for_completion: bool = False,
        timeout_seconds: int = 3600,
    ) -> WorkflowRun:
        """Trigger a workflow execution."""
        ...

    @abstractmethod
    async def get_workflow_run(
        self,
        workflow_id: str,
        run_id: str,
    ) -> WorkflowRun:
        """Get details of a specific workflow run."""
        ...

    @abstractmethod
    async def list_workflow_runs(
        self,
        workflow_id: str,
        limit: int = 10,
        state: WorkflowState | None = None,
        start_date_gte: datetime | None = None,
        start_date_lte: datetime | None = None,
    ) -> list[WorkflowRun]:
        """List recent workflow runs."""
        ...

    @abstractmethod
    async def list_workflows(
        self,
        limit: int = 100,
        only_active: bool = True,
    ) -> list[WorkflowDefinition]:
        """List available workflows."""
        ...

    @abstractmethod
    async def get_task_runs(
        self,
        workflow_id: str,
        run_id: str,
    ) -> list[TaskRun]:
        """Get task runs for a workflow execution."""
        ...

    @abstractmethod
    async def get_task_logs(
        self,
        workflow_id: str,
        run_id: str,
        task_id: str,
        attempt_number: int = 1,
    ) -> str:
        """Get logs for a specific task."""
        ...

    @abstractmethod
    async def retry_workflow(
        self,
        workflow_id: str,
        run_id: str,
        task_ids: list[str] | None = None,
    ) -> WorkflowRun:
        """Retry a failed workflow or specific tasks."""
        ...

    @abstractmethod
    async def cancel_workflow(
        self,
        workflow_id: str,
        run_id: str,
    ) -> bool:
        """Cancel a running workflow."""
        ...

    @abstractmethod
    async def get_health(self) -> OrchestratorHealth:
        """Get orchestrator health status."""
        ...

    def get_circuit_breaker_status(self) -> dict[str, Any] | None:
        """Get circuit breaker status. Override if circuit breaker is used."""
        return None

    def reset_circuit_breaker(self) -> bool:
        """Reset circuit breaker. Override if circuit breaker is used."""
        return False


# Exceptions for workflow operations
class WorkflowOrchestratorError(Exception):
    """Base exception for workflow orchestrator errors."""
    pass


class WorkflowTriggerError(WorkflowOrchestratorError):
    """Raised when a workflow cannot be triggered."""
    pass


class WorkflowNotFoundError(WorkflowOrchestratorError):
    """Raised when a workflow or run is not found."""
    pass


class WorkflowTimeoutError(WorkflowOrchestratorError):
    """Raised when a workflow operation times out."""
    pass


class CircuitBreakerOpenError(WorkflowOrchestratorError):
    """Raised when circuit breaker is open and blocking requests."""
    pass
