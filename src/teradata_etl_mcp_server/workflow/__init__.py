"""Workflow orchestrator package for pluggable orchestration backends.

This package provides a unified interface for workflow orchestration,
supporting multiple backends like Apache Airflow (with potential for future
extensions to Dagster, Prefect, etc.).

The architecture uses the Protocol pattern to enable:
- Swapping orchestrators without changing tool implementations
- Testing with mock orchestrators
- Supporting multiple orchestrators simultaneously

Available Orchestrators:
    - AirflowOrchestrator: Full implementation for Apache Airflow

Example:
    from teradata_etl_mcp_server.workflow import (
        WorkflowOrchestratorProtocol,
        AirflowOrchestrator,
        WorkflowState,
    )

    # Create orchestrator
    orchestrator: WorkflowOrchestratorProtocol = AirflowOrchestrator(client)

    # Trigger workflow (works with any backend)
    run = await orchestrator.trigger_workflow("my_pipeline", config={"key": "value"})

    # Check status
    status = await orchestrator.get_workflow_status("my_pipeline", run.run_id)
    if status.state == WorkflowState.SUCCESS:
        print("Workflow completed successfully!")
"""

from .airflow_orchestrator import AirflowOrchestrator
from .protocol import (
    # Exceptions
    CircuitBreakerOpenError,
    # Data classes
    OrchestratorHealth,
    TaskRun,
    WorkflowDefinition,
    WorkflowNotFoundError,
    # Protocol and base
    WorkflowOrchestratorBase,
    WorkflowOrchestratorError,
    WorkflowOrchestratorProtocol,
    WorkflowRun,
    WorkflowState,
    WorkflowTimeoutError,
    WorkflowTriggerError,
)

__all__ = [
    # Protocol and base class
    "WorkflowOrchestratorProtocol",
    "WorkflowOrchestratorBase",
    # Data classes
    "WorkflowState",
    "WorkflowRun",
    "WorkflowDefinition",
    "TaskRun",
    "OrchestratorHealth",
    # Implementations
    "AirflowOrchestrator",
    # Exceptions
    "WorkflowOrchestratorError",
    "WorkflowTriggerError",
    "WorkflowNotFoundError",
    "WorkflowTimeoutError",
    "CircuitBreakerOpenError",
]


def create_orchestrator(
    backend: str,
    **kwargs,
) -> WorkflowOrchestratorProtocol:
    """Factory function to create workflow orchestrators.

    Args:
        backend: Backend name ("airflow")
        **kwargs: Backend-specific configuration

    Returns:
        Configured WorkflowOrchestratorProtocol instance

    Raises:
        ValueError: If backend is not supported

    Example:
        # For Airflow
        orchestrator = create_orchestrator(
            backend="airflow",
            client=async_airflow_client,
        )
    """
    backend_lower = backend.lower()

    if backend_lower == "airflow":
        client = kwargs.get("client")
        if client is None:
            raise ValueError("AirflowOrchestrator requires 'client' (AsyncAirflowClient)")
        return AirflowOrchestrator(client=client)

    else:
        supported = ["airflow"]
        raise ValueError(
            f"Unsupported backend: {backend}. Supported backends: {supported}"
        )
