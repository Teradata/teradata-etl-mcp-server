"""Airflow workflow orchestrator implementation.

This module provides the Airflow-specific implementation of the
WorkflowOrchestratorProtocol, adapting the AsyncAirflowClient to
the unified workflow interface.

Example:
    from teradata_etl_mcp_server.clients.async_airflow_client import AsyncAirflowClient
    from teradata_etl_mcp_server.workflow import AirflowOrchestrator

    client = AsyncAirflowClient(base_url="http://localhost:8080", ...)
    orchestrator = AirflowOrchestrator(client)

    run = await orchestrator.trigger_workflow("my_dag", config={"date": "2024-01-01"})
"""

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .protocol import (
    CircuitBreakerOpenError,
    OrchestratorHealth,
    TaskRun,
    WorkflowDefinition,
    WorkflowNotFoundError,
    WorkflowOrchestratorBase,
    WorkflowRun,
    WorkflowState,
    WorkflowTimeoutError,
    WorkflowTriggerError,
)

if TYPE_CHECKING:
    from ..clients.async_airflow_client import AsyncAirflowClient

logger = logging.getLogger(__name__)


# Mapping from Airflow states to unified WorkflowState
_AIRFLOW_STATE_MAP: dict[str, WorkflowState] = {
    # DAG run states
    "queued": WorkflowState.PENDING,
    "running": WorkflowState.RUNNING,
    "success": WorkflowState.SUCCESS,
    "failed": WorkflowState.FAILED,
    "skipped": WorkflowState.SKIPPED,
    "up_for_retry": WorkflowState.RETRY,
    "up_for_reschedule": WorkflowState.PENDING,
    "upstream_failed": WorkflowState.FAILED,
    "scheduled": WorkflowState.PENDING,
    # Task instance states
    "none": WorkflowState.PENDING,
    "removed": WorkflowState.CANCELLED,
    "restarting": WorkflowState.RETRY,
    "deferred": WorkflowState.PENDING,
}

# Reverse mapping from WorkflowState to preferred Airflow state (for filtering)
# Note: Multiple Airflow states map to same WorkflowState, so we pick canonical ones
_WORKFLOW_TO_AIRFLOW_STATE: dict[WorkflowState, str] = {
    WorkflowState.PENDING: "queued",
    WorkflowState.RUNNING: "running",
    WorkflowState.SUCCESS: "success",
    WorkflowState.FAILED: "failed",
    WorkflowState.SKIPPED: "skipped",
    WorkflowState.RETRY: "up_for_retry",
    WorkflowState.CANCELLED: "removed",
}


def _map_airflow_state(airflow_state: str | None) -> WorkflowState:
    """Map Airflow state string to unified WorkflowState."""
    if not airflow_state:
        return WorkflowState.UNKNOWN
    return _AIRFLOW_STATE_MAP.get(airflow_state.lower(), WorkflowState.UNKNOWN)


def _parse_airflow_datetime(dt_str: str | None) -> datetime | None:
    """Parse Airflow datetime string to datetime object."""
    if not dt_str:
        return None
    try:
        # Airflow typically returns ISO format
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        return datetime.fromisoformat(dt_str)
    except (ValueError, TypeError) as e:
        logger.warning("Failed to parse Airflow datetime '%s': %s", dt_str, e)
        return None


class AirflowOrchestrator(WorkflowOrchestratorBase):
    """Airflow implementation of the workflow orchestrator protocol.

    Adapts the AsyncAirflowClient to provide a unified workflow interface.
    Airflow-specific concepts are mapped as follows:

    - workflow_id → dag_id
    - run_id → dag_run_id
    - task_id → task_id
    - config → conf (DAG run configuration)
    """

    def __init__(self, client: "AsyncAirflowClient"):
        """Initialize Airflow orchestrator.

        Args:
            client: Configured AsyncAirflowClient instance
        """
        self._client = client
        logger.info("Initialized AirflowOrchestrator")

    @property
    def backend_name(self) -> str:
        """Get the backend name."""
        return "airflow"

    async def trigger_workflow(
        self,
        workflow_id: str,
        config: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        wait_for_completion: bool = False,
        timeout_seconds: int = 3600,
    ) -> WorkflowRun:
        """Trigger an Airflow DAG run.

        Args:
            workflow_id: DAG ID to trigger
            config: Configuration to pass as DAG run conf
            idempotency_key: Key for idempotent triggering (uses deterministic dag_run_id)
            wait_for_completion: Whether to wait for DAG completion
            timeout_seconds: Timeout for waiting

        Returns:
            WorkflowRun with DAG run details
        """
        try:
            logger.info("Triggering Airflow DAG: %s", workflow_id)

            if idempotency_key:
                # Use idempotent trigger
                result = await self._client.trigger_dag_idempotent(
                    dag_id=workflow_id,
                    idempotency_key=idempotency_key,
                    conf=config,
                )
            else:
                # Standard trigger
                result = await self._client.trigger_dag(
                    dag_id=workflow_id,
                    conf=config,
                )

            run_id = result.get("dag_run_id")

            # Optionally wait for completion
            if wait_for_completion and run_id:
                try:
                    final_result = await self._client.wait_for_dag_run(
                        dag_id=workflow_id,
                        dag_run_id=run_id,
                        timeout_seconds=timeout_seconds,
                    )
                    result.update(final_result)
                except TimeoutError as e:
                    raise WorkflowTimeoutError(
                        f"Workflow {workflow_id} timed out after {timeout_seconds}s"
                    ) from e

            # Build unified WorkflowRun
            if not run_id:
                raise WorkflowTriggerError(
                    f"Airflow API returned no dag_run_id for workflow {workflow_id}"
                )
            workflow_run = WorkflowRun(
                run_id=run_id,
                workflow_id=workflow_id,
                state=_map_airflow_state(result.get("state")),
                started_at=_parse_airflow_datetime(result.get("start_date")),
                ended_at=_parse_airflow_datetime(result.get("end_date")),
                duration_seconds=result.get("duration"),
                config=config or {},
                external_url=result.get("external_url"),
                metadata={
                    "execution_date": result.get("execution_date"),
                    "idempotent_reused": result.get("idempotent_reused", False),
                    "logical_date": result.get("logical_date"),
                },
            )

            logger.info("Triggered workflow run: %s", run_id)
            return workflow_run

        except WorkflowTimeoutError:
            # Re-raise timeout errors directly
            raise
        except CircuitBreakerOpenError:
            # Propagate circuit breaker errors without modification
            raise
        except Exception as e:
            raise WorkflowTriggerError(f"Failed to trigger workflow {workflow_id}: {e}") from e

    async def get_workflow_run(
        self,
        workflow_id: str,
        run_id: str,
    ) -> WorkflowRun:
        """Get details of a specific DAG run.

        Args:
            workflow_id: DAG ID
            run_id: DAG run ID

        Returns:
            WorkflowRun with current status
        """
        try:
            result = await self._client.get_dag_run_status(workflow_id, run_id)

            # Get task summary for metadata
            task_summary = result.get("task_summary", {})

            return WorkflowRun(
                run_id=result.get("dag_run_id", run_id),
                workflow_id=workflow_id,
                state=_map_airflow_state(result.get("state")),
                started_at=_parse_airflow_datetime(result.get("start_date")),
                ended_at=_parse_airflow_datetime(result.get("end_date")),
                duration_seconds=result.get("duration"),
                metadata={
                    "execution_date": result.get("execution_date"),
                    "task_summary": task_summary,
                    "total_tasks": result.get("total_tasks", 0),
                },
            )

        except Exception as e:
            if "not found" in str(e).lower() or "404" in str(e):
                raise WorkflowNotFoundError(f"Workflow run {workflow_id}/{run_id} not found") from e
            raise

    async def list_workflow_runs(
        self,
        workflow_id: str,
        limit: int = 10,
        state: WorkflowState | None = None,
        start_date_gte: datetime | None = None,
        start_date_lte: datetime | None = None,
    ) -> list[WorkflowRun]:
        """List recent DAG runs.

        Args:
            workflow_id: DAG ID
            limit: Maximum runs to return
            state: Filter by state
            start_date_gte: Filter by start date (>=)
            start_date_lte: Filter by start date (<=)

        Returns:
            List of WorkflowRun objects
        """
        # Map unified state back to Airflow state for filtering
        airflow_state = _WORKFLOW_TO_AIRFLOW_STATE.get(state) if state else None

        # Note: start_date_gte/start_date_lte filtering not supported by current client
        # These parameters are accepted for protocol compatibility but not passed to client
        runs = await self._client.list_dag_runs(
            dag_id=workflow_id,
            limit=limit,
            state=airflow_state,
        )

        # Apply date filtering client-side if needed
        filtered_runs = runs
        if start_date_gte or start_date_lte:
            filtered_runs = []
            for run in runs:
                run_start = _parse_airflow_datetime(run.get("start_date"))
                if run_start:
                    if start_date_gte and run_start < start_date_gte:
                        continue
                    if start_date_lte and run_start > start_date_lte:
                        continue
                filtered_runs.append(run)

        return [
            WorkflowRun(
                run_id=run.get("dag_run_id", ""),
                workflow_id=workflow_id,
                state=_map_airflow_state(run.get("state")),
                started_at=_parse_airflow_datetime(run.get("start_date")),
                ended_at=_parse_airflow_datetime(run.get("end_date")),
                duration_seconds=run.get("duration"),
                metadata={
                    "execution_date": run.get("execution_date"),
                },
            )
            for run in filtered_runs
        ]

    async def list_workflows(
        self,
        limit: int = 100,
        only_active: bool = True,
    ) -> list[WorkflowDefinition]:
        """List available DAGs.

        Args:
            limit: Maximum DAGs to return
            only_active: Only return unpaused DAGs

        Returns:
            List of WorkflowDefinition objects
        """
        dags = await self._client.list_dags(limit=limit, only_active=only_active)

        return [
            WorkflowDefinition(
                workflow_id=dag.get("dag_id", ""),
                name=dag.get("dag_id", ""),
                description=dag.get("description"),
                schedule=dag.get("schedule_interval"),
                is_active=not dag.get("is_paused", False),
                tags=[t.get("name", "") for t in dag.get("tags", []) if isinstance(t, dict)],
                metadata={
                    "file_token": dag.get("file_token"),
                    "owners": dag.get("owners", []),
                },
            )
            for dag in dags
        ]

    async def get_task_runs(
        self,
        workflow_id: str,
        run_id: str,
    ) -> list[TaskRun]:
        """Get task instances for a DAG run.

        Args:
            workflow_id: DAG ID
            run_id: DAG run ID

        Returns:
            List of TaskRun objects
        """
        tasks = await self._client.list_task_instances(workflow_id, run_id)

        return [
            TaskRun(
                task_id=task.get("task_id", ""),
                run_id=run_id,
                workflow_id=workflow_id,
                state=_map_airflow_state(task.get("state")),
                started_at=_parse_airflow_datetime(task.get("start_date")),
                ended_at=_parse_airflow_datetime(task.get("end_date")),
                duration_seconds=task.get("duration"),
                attempt_number=task.get("try_number", 1),
                error_message=task.get("error") if task.get("state") == "failed" else None,
            )
            for task in tasks
        ]

    async def get_task_logs(
        self,
        workflow_id: str,
        run_id: str,
        task_id: str,
        attempt_number: int = 1,
    ) -> str:
        """Get logs for a task instance.

        Args:
            workflow_id: DAG ID
            run_id: DAG run ID
            task_id: Task ID
            attempt_number: Try number

        Returns:
            Log content as string
        """
        logs = await self._client.get_task_logs(
            dag_id=workflow_id,
            dag_run_id=run_id,
            task_id=task_id,
            task_try_number=attempt_number,
        )
        return logs or ""

    async def retry_workflow(
        self,
        workflow_id: str,
        run_id: str,
        task_ids: list[str] | None = None,
    ) -> WorkflowRun:
        """Retry tasks in a DAG run by clearing task instances.

        Uses Airflow's clearTaskInstances API to reset tasks,
        which will automatically re-queue them for execution.

        Note: When task_ids is provided, ALL tasks are cleared (not just failed ones).
        When task_ids is None, only failed tasks are cleared for retry.

        Args:
            workflow_id: DAG ID
            run_id: DAG run ID to retry
            task_ids: If provided, clears all tasks (not just failed ones).
                     If None, only failed tasks are cleared for retry.

        Returns:
            WorkflowRun with updated status after clearing

        Raises:
            WorkflowTriggerError: If retry fails
        """
        try:
            # When task_ids is specified, we clear all tasks (not just failed)
            # This allows retrying from a specific point even if tasks succeeded
            only_failed = task_ids is None
            logger.info(
                "Retrying workflow %s/%s (only_failed=%s)",
                workflow_id, run_id, only_failed
            )

            # Clear task instances to trigger retry
            result = await self._client.clear_dag_run(
                dag_id=workflow_id,
                dag_run_id=run_id,
                dry_run=False,
                reset_dag_runs=True,
                only_failed=only_failed,
            )

            cleared_count = len(result.get("task_instances", []))
            logger.info("Cleared %d task instances for retry", cleared_count)

            # Get updated status
            return await self.get_workflow_run(workflow_id, run_id)

        except Exception as e:
            logger.error("Failed to retry workflow %s/%s: %s", workflow_id, run_id, e)
            raise WorkflowTriggerError(f"Failed to retry workflow: {e}") from e

    async def cancel_workflow(
        self,
        workflow_id: str,
        run_id: str,
    ) -> bool:
        """Cancel a running DAG run by setting its state to 'failed'.

        Args:
            workflow_id: DAG ID
            run_id: DAG run ID to cancel

        Returns:
            True if cancellation was successful
        """
        try:
            logger.info("Cancelling workflow %s/%s", workflow_id, run_id)

            await self._client.set_dag_run_state(
                dag_id=workflow_id,
                dag_run_id=run_id,
                state="failed",
            )

            logger.info("Cancelled workflow %s/%s", workflow_id, run_id)
            return True

        except Exception as e:
            logger.error("Failed to cancel workflow %s/%s: %s", workflow_id, run_id, e)
            return False

    async def get_health(self) -> OrchestratorHealth:
        """Get Airflow health status.

        Returns:
            OrchestratorHealth with connection details
        """
        try:
            conn_status = await self._client.test_connection()
            cb_status = self.get_circuit_breaker_status()

            availability = "healthy"
            if cb_status:
                cb_state = cb_status.get("state", "unknown")
                if cb_state == "open":
                    availability = "degraded"
                elif cb_state == "half_open":
                    availability = "recovering"

            if not conn_status.get("connected"):
                availability = "unavailable"

            return OrchestratorHealth(
                connected=conn_status.get("connected", False),
                backend="airflow",
                version=conn_status.get("version"),
                url=conn_status.get("url"),
                availability=availability,
                error=conn_status.get("error"),
                circuit_breaker=cb_status,
            )

        except Exception as e:
            logger.error("Failed to get Airflow health: %s", e)
            return OrchestratorHealth(
                connected=False,
                backend="airflow",
                availability="unavailable",
                error=str(e),
            )

    def get_circuit_breaker_status(self) -> dict[str, Any] | None:
        """Get circuit breaker status from underlying client."""
        if hasattr(self._client, "get_circuit_breaker_status"):
            return self._client.get_circuit_breaker_status()
        return None

    def reset_circuit_breaker(self) -> bool:
        """Reset circuit breaker on underlying client."""
        if hasattr(self._client, "reset_circuit_breaker"):
            return self._client.reset_circuit_breaker()
        return False

    def get_rate_limiter_status(self) -> dict[str, Any] | None:
        """Get rate limiter status from underlying client."""
        if hasattr(self._client, "get_rate_limiter_status"):
            return self._client.get_rate_limiter_status()
        return None

    def get_client_status(self) -> dict[str, Any] | None:
        """Get comprehensive client status for monitoring."""
        if hasattr(self._client, "get_client_status"):
            return self._client.get_client_status()
        return None

    # ==================== Airflow-Specific Methods ====================
    # These methods are not part of the protocol but provide Airflow-specific
    # functionality when needed.

    async def trigger_multiple_workflows(
        self,
        workflow_configs: list[dict[str, Any]],
    ) -> list[WorkflowRun]:
        """Trigger multiple DAGs concurrently (Airflow-specific).

        Args:
            workflow_configs: List of dicts with 'workflow_id' and optional 'config'

        Returns:
            List of WorkflowRun results
        """
        # Convert to Airflow format
        dag_configs = [
            {"dag_id": wc["workflow_id"], "conf": wc.get("config")}
            for wc in workflow_configs
        ]

        results = await self._client.trigger_multiple_dags(dag_configs)

        return [
            WorkflowRun(
                run_id=r.get("dag_run_id", ""),
                workflow_id=r.get("dag_id", ""),
                state=_map_airflow_state(r.get("state")),
                error_message=r.get("error"),
                metadata={"error": r.get("error")} if r.get("error") else {},
            )
            for r in results
        ]
