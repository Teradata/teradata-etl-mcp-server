"""Orchestration execution and monitoring tools.

This module provides MCP tools for triggering, monitoring, and managing
pipeline execution in Airflow — consolidated into three router tools.

Architecture:
    All tools use native async calls via the async_airflow_client property
    on the PipelineOrchestrator.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Literal

from ..orchestrator import PipelineOrchestrator
from ..response_sanitizer import safe_error_message, sanitize_response
from ..utils.validators import validate_airflow_identifier, validate_dag_run_id

logger = logging.getLogger(__name__)


def register_orchestration_tools(orchestrator: PipelineOrchestrator) -> dict[str, Any]:
    """Register orchestration execution tools.

    Args:
        orchestrator: Pipeline orchestrator instance

    Returns:
        Dictionary of tool functions
    """

    # ══════════════════════════════════════════════════════════════
    #  Private helpers (original implementations preserved)
    # ══════════════════════════════════════════════════════════════

    async def _trigger_run(
        pipeline_name: str,
        config: dict[str, Any] | None = None,
        wait_for_completion: bool = False,
        dag_run_id: str | None = None,
    ) -> dict[str, Any]:
        run_result = await orchestrator.async_trigger_airflow_dag(
            dag_id=pipeline_name, conf=config, wait_for_completion=wait_for_completion,
            dag_run_id=dag_run_id,
        )
        dag_run_id = run_result.get("dag_run_id")
        result = {
            "success": True,
            "pipeline_name": pipeline_name,
            "dag_run_id": dag_run_id,
            "execution_date": run_result.get("execution_date"),
            "state": run_result.get("state", "queued"),
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
        if wait_for_completion and run_result.get("final_status"):
            result["final_status"] = run_result.get("final_status")
            final_state = (str(result["final_status"]) or "").lower()
            if final_state in {"success", "succeeded"}:
                result["next_steps"] = [
                    (
                        f"**1. Inspect what landed**: "
                        f"`dag_monitor(query='list_runs', "
                        f"pipeline_name='{pipeline_name}', limit=1)`. "
                        f"**Why**: a green state alone doesn't show row "
                        f"counts or task durations; downstream consumers "
                        f"need to see the metrics. **Effect**: returns "
                        f"per-task timing + state for the run. **If "
                        f"missing**: skip if you trust the DAG's "
                        f"validation tasks."
                    ),
                    (
                        "**2. Build downstream dbt models** (optional): "
                        "`dbt_execute(command='run')`. **Why**: a "
                        "successful load is the trigger for any dbt "
                        "transformations that depend on the new data. "
                        "**Effect**: dbt-teradata refreshes views/tables. "
                        "**If missing**: skip if no dbt project consumes "
                        "the loaded tables."
                    ),
                ]
            elif final_state in {"failed", "upstream_failed"}:
                result["next_steps"] = [
                    (
                        f"**1. Pull task logs**: "
                        f"`dag_monitor(query='task_logs', "
                        f"pipeline_name='{pipeline_name}', "
                        f"dag_run_id='{dag_run_id}', "
                        f"task_id='<failed_task>')`. **Why**: a failed run "
                        f"blocks downstream consumers; logs surface the "
                        f"underlying error (TPT/SSH/dbt). **Effect**: "
                        f"returns the task's stdout/stderr from Airflow. "
                        f"**If missing**: triage from the Airflow UI."
                    ),
                    (
                        f"**2. Retry after fixing**: "
                        f"`dag_trigger(mode='retry_failed', "
                        f"pipeline_name='{pipeline_name}', "
                        f"dag_run_id='{dag_run_id}')`. **Why**: clears "
                        f"failed task instances so they re-run with the "
                        f"fix in place. **Effect**: the failed tasks are "
                        f"queued for re-execution. **If missing**: "
                        f"trigger a fresh run with "
                        f"``dag_trigger(mode='run', ...)``."
                    ),
                ]
        elif not wait_for_completion and dag_run_id:
            result["next_steps"] = [
                (
                    f"**1. Poll for completion**: "
                    f"`dag_monitor(query='run_status', "
                    f"pipeline_name='{pipeline_name}', "
                    f"dag_run_id='{dag_run_id}')` until the state is "
                    f"terminal (success/failed). **Why**: there is no "
                    f"blocking-wait tool for an existing dag_run_id; "
                    f"the trigger returned immediately and downstream "
                    f"steps depend on knowing the terminal state. "
                    f"**Effect**: returns the current run state and "
                    f"per-task summary. **If missing**: on the next "
                    f"manual trigger, pass ``wait_for_completion=True`` "
                    f"to ``dag_trigger(mode='run', ...)`` so the call "
                    f"blocks until terminal."
                ),
            ]
        return result

    async def _trigger_idempotent(
        pipeline_name: str,
        idempotency_key: str,
        config: dict[str, Any] | None = None,
        wait_for_completion: bool = False,
    ) -> dict[str, Any]:
        run_result = await orchestrator.async_trigger_airflow_dag_idempotent(
            dag_id=pipeline_name, idempotency_key=idempotency_key, conf=config
        )
        dag_run_id = run_result.get("dag_run_id")
        result = {
            "success": True,
            "pipeline_name": pipeline_name,
            "dag_run_id": dag_run_id,
            "idempotency_key": idempotency_key,
            "idempotent_reused": run_result.get("idempotent_reused", False),
            "state": run_result.get("state", "queued"),
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
        if run_result.get("idempotent_reused"):
            result["message"] = f"Reused existing DAG run {dag_run_id}"
        else:
            result["message"] = f"Created new DAG run {dag_run_id}"
        if wait_for_completion and not run_result.get("idempotent_reused"):
            try:
                final_result = await orchestrator.async_airflow_client.wait_for_dag_run(
                    dag_id=pipeline_name, dag_run_id=dag_run_id, timeout_seconds=3600
                )
                result["final_status"] = final_result.get("state")
                result["duration_seconds"] = final_result.get("duration_seconds")
            except TimeoutError:
                result["final_status"] = "timeout"
        if run_result.get("idempotent_reused"):
            result["next_steps"] = [
                (
                    f"**1. Check the existing run's status**: "
                    f"`dag_monitor(query='run_status', "
                    f"pipeline_name='{pipeline_name}', "
                    f"dag_run_id='{dag_run_id}')`. **Why**: idempotency "
                    f"reused the prior run; the user wants to know "
                    f"whether that run succeeded or is still in flight. "
                    f"**Effect**: returns state + per-task summary. **If "
                    f"missing**: skip if the user only cared that the "
                    f"trigger was a no-op."
                ),
                (
                    f"**2. Force a fresh run** (only if needed): "
                    f"`dag_trigger(mode='run', "
                    f"pipeline_name='{pipeline_name}')`. **Why**: the "
                    f"reused run may be stale; a deliberate re-run "
                    f"bypasses idempotency. **Effect**: creates a new "
                    f"DAG run with a fresh logical date. **If missing**: "
                    f"skip if reuse was intentional."
                ),
            ]
        else:
            result["next_steps"] = [
                (
                    f"**1. Poll for the run to finish**: "
                    f"`dag_monitor(query='run_status', "
                    f"pipeline_name='{pipeline_name}', "
                    f"dag_run_id='{dag_run_id}')` until the state is "
                    f"terminal (success/failed). **Why**: a fresh "
                    f"idempotent trigger started the DAG, but there is "
                    f"no blocking-wait tool for an existing dag_run_id "
                    f"— polling is the only path to a confirmed terminal "
                    f"state. **Effect**: returns the current run state "
                    f"and per-task summary. **If missing**: on the next "
                    f"call pass ``wait_for_completion=True`` to "
                    f"``dag_trigger(mode='idempotent', "
                    f"idempotency_key='{idempotency_key}')`` so the call "
                    f"blocks until terminal."
                ),
                (
                    f"**2. Reuse the same key for retries**: pass "
                    f"``idempotency_key='{idempotency_key}'`` on the "
                    f"next call. **Why**: that's the whole point of the "
                    f"idempotency contract — repeated calls map to one "
                    f"run. **Effect**: subsequent calls return this "
                    f"dag_run_id instead of creating duplicates. **If "
                    f"missing**: skip if your caller already manages "
                    f"keys."
                ),
            ]
        return result

    async def _trigger_multiple(dag_configs: list[dict[str, Any]]) -> dict[str, Any]:
        if not dag_configs:
            return {
                "success": True,
                "total_triggered": 0,
                "results": [],
                "message": "No DAGs to trigger",
            }
        results = await orchestrator.async_trigger_multiple_dags(dag_configs)
        successful = sum(1 for r in results if r.get("dag_run_id"))
        failed = len(results) - successful
        response = {
            "success": failed == 0,
            "total_triggered": successful,
            "total_failed": failed,
            "results": results,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
        triggered_dag_ids = [r.get("dag_id") for r in results if r.get("dag_run_id")]
        if successful > 0:
            response["next_steps"] = [
                (
                    f"**1. Watch all triggered DAGs**: for each DAG id in "
                    f"{triggered_dag_ids!r}, run "
                    f"`dag_monitor(query='run_status', pipeline_name=<id>, "
                    f"dag_run_id=<from_results>)`. **Why**: ``multiple`` "
                    f"only kicks off the DAGs; downstream consumers need "
                    f"to know which finished green. **Effect**: returns "
                    f"per-DAG state. **If missing**: poll the Airflow UI."
                ),
                (
                    "**2. Re-trigger any failures**: for each entry where "
                    "``dag_run_id`` is missing, retry with "
                    "`dag_trigger(mode='run', pipeline_name=<id>)`. "
                    "**Why**: a partial fan-out leaves the system in an "
                    "inconsistent state. **Effect**: kicks off the "
                    "missing DAGs individually so you can inspect "
                    "errors. **If missing**: skip if ``total_failed=0``."
                ),
            ]
        return response

    async def _retry_failed(
        pipeline_name: str,
        dag_run_id: str,
        task_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        tasks = await orchestrator.async_airflow_client.list_task_instances(
            pipeline_name, dag_run_id
        )
        failed_tasks = [t.get("task_id") for t in tasks if t.get("state") == "failed"]
        if task_ids is not None:
            tasks_to_retry = [t for t in failed_tasks if t in task_ids]
        else:
            tasks_to_retry = failed_tasks
        retry_results: list[dict[str, Any]] = []
        message: str | None = None
        retried_count = 0
        if not tasks_to_retry:
            message = "No failed tasks to retry"
        else:
            try:
                await orchestrator.async_airflow_client.clear_dag_run(
                    pipeline_name, dag_run_id, task_ids=tasks_to_retry if task_ids is not None else None
                )
                retried_count = len(tasks_to_retry)
                for t in tasks_to_retry:
                    retry_results.append({"task_id": t, "status": "queued"})
                if task_ids is not None:
                    message = f"Cleared {retried_count} specific failed task(s) for retry"
                else:
                    message = "Cleared DAG run to retry failed tasks"
            except Exception as e:
                for t in tasks_to_retry:
                    retry_results.append(
                        {"task_id": t, "status": "error", "error": safe_error_message(e)}
                    )
                message = "Failed to clear DAG run: %s" % safe_error_message(e)
        result = {
            "success": retried_count > 0 or not tasks_to_retry,
            "pipeline_name": pipeline_name,
            "dag_run_id": dag_run_id,
            "total_failed": len(failed_tasks),
            "retried_count": retried_count,
            "retry_results": retry_results,
        }
        if message:
            result["message"] = message
        if retried_count > 0:
            result["next_steps"] = [
                (
                    f"**1. Watch the retry**: "
                    f"`dag_monitor(query='run_status', "
                    f"pipeline_name='{pipeline_name}', "
                    f"dag_run_id='{dag_run_id}')`. **Why**: clearing "
                    f"failed tasks queued them for re-execution; the user "
                    f"needs to know whether the retry succeeded. "
                    f"**Effect**: returns the updated per-task state. "
                    f"**If missing**: poll the Airflow UI."
                ),
                (
                    f"**2. Pull task logs if it fails again**: "
                    f"`dag_monitor(query='task_logs', "
                    f"pipeline_name='{pipeline_name}', "
                    f"dag_run_id='{dag_run_id}', task_id='<task>')`. "
                    f"**Why**: a second failure usually means the root "
                    f"cause wasn't addressed. **Effect**: returns the "
                    f"task's stdout/stderr. **If missing**: skip if the "
                    f"retry succeeds."
                ),
            ]
        return result

    async def _get_run_status(
        pipeline_name: str,
        dag_run_id: str | None = None,
    ) -> dict[str, Any]:
        if not dag_run_id:
            runs = await orchestrator.async_airflow_client.list_dag_runs(
                dag_id=pipeline_name, limit=1
            )
            if not runs:
                return {
                    "error": "No DAG runs found for the specified pipeline",
                    "pipeline_name": pipeline_name,
                }
            dag_run_id = runs[0].get("dag_run_id")
        status = await orchestrator.async_get_dag_run_status(pipeline_name, dag_run_id)
        return {
            "pipeline_name": pipeline_name,
            "dag_run_id": status.get("dag_run_id"),
            "state": status.get("state"),
            "execution_date": status.get("execution_date"),
            "start_date": status.get("start_date"),
            "end_date": status.get("end_date"),
            "duration": status.get("duration"),
            "task_summary": status.get("task_summary", {}),
            "total_tasks": status.get("total_tasks", 0),
        }

    async def _list_runs(
        pipeline_name: str,
        limit: int = 10,
        state: str | None = None,
        start_date_gte: str | None = None,
        start_date_lte: str | None = None,
    ) -> dict[str, Any]:
        runs = await orchestrator.async_airflow_client.list_dag_runs(
            dag_id=pipeline_name,
            limit=limit,
            state=state,
            execution_date_gte=start_date_gte,
            execution_date_lte=start_date_lte,
        )
        dag_runs = [
            {
                "dag_run_id": run.get("dag_run_id"),
                "state": run.get("state"),
                "execution_date": run.get("execution_date"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "duration": run.get("duration"),
            }
            for run in runs
        ]
        return {
            "pipeline_name": pipeline_name,
            "total_count": len(dag_runs),
            "dag_runs": dag_runs,
            "filters_applied": {
                "state": state,
                "start_date_gte": start_date_gte,
                "start_date_lte": start_date_lte,
                "limit": limit,
            },
        }

    async def _list_dags(limit: int = 100, only_active: bool = True, tags: list[str] | None = None) -> dict[str, Any]:
        dags = await orchestrator.async_list_dags(limit=limit, only_active=only_active, tags=tags)
        formatted_dags = [
            {
                "dag_id": dag.get("dag_id"),
                "is_paused": dag.get("is_paused"),
                "description": dag.get("description"),
                "schedule_interval": dag.get("schedule_interval"),
                "tags": dag.get("tags", []),
            }
            for dag in dags
        ]
        return {
            "success": True,
            "total_count": len(formatted_dags),
            "dags": formatted_dags,
            "filters": {"only_active": only_active, "limit": limit, "tags": tags},
        }

    async def _get_task_logs(
        pipeline_name: str,
        dag_run_id: str,
        task_id: str,
        try_number: int = 1,
    ) -> dict[str, Any]:
        logs = await orchestrator.async_airflow_client.get_task_logs(
            pipeline_name, dag_run_id, task_id, try_number
        )
        _MAX_LOG_BYTES = 100_000
        total_length = len(logs or "")
        truncated = False
        if total_length > _MAX_LOG_BYTES:
            logs = logs[:_MAX_LOG_BYTES]
            truncated = True
        return {
            "pipeline_name": pipeline_name,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "try_number": try_number,
            "logs": logs,
            "log_length": len(logs or ""),
            "total_length": total_length,
            "truncated": truncated,
        }

    async def _monitor_execution(
        pipeline_name: str,
        include_task_details: bool = True,
        include_performance_metrics: bool = True,
    ) -> dict[str, Any]:
        status = await orchestrator.get_pipeline_status_async(dag_id=pipeline_name)
        if not isinstance(status, dict):
            return {"error": "Pipeline status unavailable", "pipeline_name": pipeline_name}
        result: dict[str, Any] = {
            "pipeline_name": pipeline_name,
            "is_paused": status.get("is_paused"),
            "current_status": (status.get("last_run") or {}).get("state"),
            "last_execution_date": (status.get("last_run") or {}).get("execution_date"),
        }
        if include_performance_metrics:
            stats = status.get("statistics", {})
            result["performance_metrics"] = {
                "success_rate": stats.get("success_rate"),
                "average_duration": stats.get("average_duration"),
                "total_runs": stats.get("total_runs"),
                "failed_runs": stats.get("failed_runs"),
            }
        recent_runs = status.get("recent_runs", [])
        result["recent_runs"] = recent_runs[:5]
        if include_task_details and recent_runs:
            latest_run = recent_runs[0]
            dag_run_id = latest_run.get("dag_run_id")
            if dag_run_id:
                try:
                    tasks = await orchestrator.async_airflow_client.list_task_instances(
                        pipeline_name, dag_run_id
                    )
                    result["current_tasks"] = [
                        {
                            "task_id": t.get("task_id"),
                            "state": t.get("state"),
                            "start_date": t.get("start_date"),
                            "duration": t.get("duration"),
                        }
                        for t in tasks
                    ]
                except Exception as e:
                    logger.debug("Failed to fetch task instances: %s", e)
        return result

    async def _get_health() -> dict[str, Any]:
        try:
            result = await orchestrator.async_get_airflow_health()
        except Exception as e:
            logger.error("Failed to get Airflow health: %s", e, exc_info=True)
            return {"connected": False, "availability": "unknown", "error": str(e)}
        cb_status = result.get("circuit_breaker")
        if cb_status:
            cb_state = cb_status.get("state", "unknown")
            if cb_state == "open":
                result["availability_message"] = (
                    f"Circuit breaker is OPEN. Requests blocked for "
                    f"{cb_status.get('time_until_recovery', 'unknown')}s. "
                    "Service may be unhealthy."
                )
            elif cb_state == "half_open":
                result["availability_message"] = "Circuit breaker is testing recovery"
            else:
                result["availability_message"] = "All systems operational"
        else:
            result["circuit_breaker"] = None
            if result.get("connected"):
                result["availability_message"] = "All systems operational"
        if not result.get("connected") and "error" not in result:
            result["error"] = "Connection failed"
        return result

    async def _reset_circuit_breaker() -> dict[str, Any]:
        success = orchestrator.async_airflow_client.reset_circuit_breaker()
        if success:
            new_status = orchestrator.async_airflow_client.get_circuit_breaker_status()
            return {
                "success": True,
                "message": "Circuit breaker reset to CLOSED state",
                "circuit_breaker": new_status,
            }
        return {"success": False, "message": "Circuit breaker is not enabled on this client"}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 1: dag_trigger
    # ══════════════════════════════════════════════════════════════

    async def dag_trigger(
        mode: Literal["run", "idempotent", "multiple", "retry_failed"],
        pipeline_name: str | None = None,
        config: dict[str, Any] | None = None,
        wait_for_completion: bool = False,
        idempotency_key: str | None = None,
        dag_configs: list[dict[str, Any]] | None = None,
        dag_run_id: str | None = None,
        task_ids: list[str] | None = None,
        # Aliases for pipeline_name (LLMs often use dag_id instead)
        dag_id: str | None = None,
        dag_name: str | None = None,
    ) -> dict[str, Any]:
        """Trigger Airflow DAG runs in various modes.

        IMPORTANT: Use 'pipeline_name' for the DAG ID (not 'dag_id').

        ELT Pipeline Workflow — Sequential Prompts Required:
          This tool handles ONLY DAG triggering/monitoring.
          Before triggering, ensure the DAG has been deployed via pipeline_deploy.
          After data transfer completes, the user should separately:
          1. Generate dbt models: dbt_generate_model(model_type='staging', ...)
          2. Execute dbt: dbt_execute(command='run', models=[...])
          Each step should be a separate user prompt.

        Args:
            mode: One of:
                - "run"          — Trigger a single DAG run immediately.
                - "idempotent"   — Trigger with idempotency guarantee (dedup by key).
                - "multiple"     — Trigger several DAGs concurrently.
                - "retry_failed" — Clear and retry failed tasks in a DAG run.
            pipeline_name: DAG ID (required for run, idempotent, retry_failed). Use this, not 'dag_id'.
            config: Optional runtime configuration dict for the DAG.
            wait_for_completion: Wait for the run to finish (default False).
            idempotency_key: Unique key for idempotent mode.
            dag_configs: List of {dag_id, conf} dicts for multiple mode.
            dag_run_id: DAG run ID (required for retry_failed).
            task_ids: Specific task IDs to retry (optional for retry_failed).

        Returns:
            Dictionary with trigger results.
        """
        if not pipeline_name:
            pipeline_name = dag_id or dag_name
        if not isinstance(mode, str) or not mode.strip():
            return {"success": False, "error": "Parameter 'mode' must be a non-empty string."}
        mode = mode.strip().lower()
        if pipeline_name:
            err = validate_airflow_identifier(pipeline_name, "pipeline_name")
            if err:
                return {"success": False, "error": err}
        if dag_run_id:
            err = validate_dag_run_id(dag_run_id, "dag_run_id")
            if err:
                return {"success": False, "error": err}
        try:
            if mode == "run":
                if not pipeline_name:
                    return {"success": False, "error": "pipeline_name is required for mode='run'."}
                return sanitize_response(
                    await _trigger_run(pipeline_name, config, wait_for_completion, dag_run_id)
                )
            elif mode == "idempotent":
                if not pipeline_name or not idempotency_key:
                    return {
                        "success": False,
                        "error": "pipeline_name and idempotency_key required for mode='idempotent'.",
                    }
                return sanitize_response(
                    await _trigger_idempotent(
                        pipeline_name, idempotency_key, config, wait_for_completion
                    )
                )
            elif mode == "multiple":
                return sanitize_response(await _trigger_multiple(dag_configs or []))
            elif mode == "retry_failed":
                if not pipeline_name or not dag_run_id:
                    return {
                        "success": False,
                        "error": "pipeline_name and dag_run_id required for mode='retry_failed'.",
                    }
                return sanitize_response(await _retry_failed(pipeline_name, dag_run_id, task_ids))
            else:
                return {
                    "success": False,
                    "error": f"Unknown mode '{mode}'. Valid modes: run, idempotent, multiple, retry_failed",
                }
        except Exception as e:
            logger.error("dag_trigger(%s) failed: %s", mode, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 2: dag_monitor
    # ══════════════════════════════════════════════════════════════

    async def dag_monitor(
        query: Literal["run_status", "list_runs", "list_dags", "task_logs", "monitor_execution"],
        pipeline_name: str | None = None,
        dag_run_id: str | None = None,
        task_id: str | None = None,
        try_number: int = 1,
        limit: int = 10,
        state: str | None = None,
        start_date_gte: str | None = None,
        start_date_lte: str | None = None,
        only_active: bool = True,
        tags: list[str] | None = None,
        include_task_details: bool = True,
        include_performance_metrics: bool = True,
        # Aliases for pipeline_name (LLMs often use dag_id instead)
        dag_id: str | None = None,
        dag_name: str | None = None,
        # Aliases for tags
        filter_tags: list[str] | None = None,
        dag_tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """Query Airflow DAG run status, history, task logs, and monitoring data.

        IMPORTANT: Use 'pipeline_name' for the DAG ID (not 'dag_id').
        Use 'tags' for tag filtering (not 'filter_tags').

        Args:
            query: One of:
                - "run_status"         — Get status of a specific or latest DAG run.
                - "list_runs"          — List recent DAG runs with filtering.
                - "list_dags"          — List available DAGs from Airflow.
                - "task_logs"          — Get logs for a specific task instance.
                - "monitor_execution"  — Comprehensive monitoring with metrics.
            pipeline_name: DAG ID (required for most queries). Use this, not 'dag_id'.
            dag_run_id: Specific DAG run ID (for run_status and task_logs).
            task_id: Task ID (required for task_logs).
            try_number: Task attempt number for task_logs (default 1).
            limit: Max results for list_runs/list_dags (default 10).
            state: Filter by state for list_runs.
            start_date_gte: Filter runs >= this date (ISO) for list_runs.
            start_date_lte: Filter runs <= this date (ISO) for list_runs.
            only_active: Only active DAGs for list_dags (default True).
            tags: Filter DAGs by Airflow tags for list_dags (e.g., ['teradata', 'daily']).
            include_task_details: Include task breakdown for monitor_execution (default True).
            include_performance_metrics: Include perf stats for monitor_execution (default True).

        Returns:
            Dictionary with monitoring results.
        """
        if not pipeline_name:
            pipeline_name = dag_id or dag_name
        if not tags:
            tags = filter_tags or dag_tags
        if not isinstance(query, str) or not query.strip():
            return {"success": False, "error": "Parameter 'query' must be a non-empty string."}
        if limit < 1:
            return {"success": False, "error": "Parameter 'limit' must be >= 1."}
        if try_number < 1:
            return {"success": False, "error": "Parameter 'try_number' must be >= 1."}
        query = query.strip().lower()
        if pipeline_name:
            err = validate_airflow_identifier(pipeline_name, "pipeline_name")
            if err:
                return {"success": False, "error": err}
        if dag_run_id:
            err = validate_dag_run_id(dag_run_id, "dag_run_id")
            if err:
                return {"success": False, "error": err}
        if task_id:
            err = validate_airflow_identifier(task_id, "task_id")
            if err:
                return {"success": False, "error": err}
        try:
            if query == "run_status":
                if not pipeline_name:
                    return {
                        "success": False,
                        "error": "pipeline_name required for query='run_status'.",
                    }
                return sanitize_response(await _get_run_status(pipeline_name, dag_run_id))
            elif query == "list_runs":
                if not pipeline_name:
                    return {
                        "success": False,
                        "error": "pipeline_name required for query='list_runs'.",
                    }
                return sanitize_response(
                    await _list_runs(pipeline_name, limit, state, start_date_gte, start_date_lte)
                )
            elif query == "list_dags":
                return sanitize_response(await _list_dags(limit=limit, only_active=only_active, tags=tags))
            elif query == "task_logs":
                if not pipeline_name or not dag_run_id or not task_id:
                    return {
                        "success": False,
                        "error": "pipeline_name, dag_run_id, and task_id required for query='task_logs'.",
                    }
                return sanitize_response(
                    await _get_task_logs(pipeline_name, dag_run_id, task_id, try_number)
                )
            elif query == "monitor_execution":
                if not pipeline_name:
                    return {
                        "success": False,
                        "error": "pipeline_name required for query='monitor_execution'.",
                    }
                return sanitize_response(
                    await _monitor_execution(
                        pipeline_name, include_task_details, include_performance_metrics
                    )
                )
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown query '{query}'. "
                        "Valid queries: run_status, list_runs, list_dags, task_logs, monitor_execution"
                    ),
                }
        except Exception as e:
            logger.error("dag_monitor(%s) failed: %s", query, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 3: airflow_admin
    # ══════════════════════════════════════════════════════════════

    async def airflow_admin(
        action: Literal["health", "reset_circuit_breaker"],
    ) -> dict[str, Any]:
        """Airflow administrative operations.

        Args:
            action: One of:
                - "health"                 — Get Airflow health and circuit breaker status.
                - "reset_circuit_breaker"  — Reset circuit breaker to closed state.

        Returns:
            Dictionary with health or reset status.
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        action = action.strip().lower()
        try:
            if action == "health":
                return sanitize_response(await _get_health())
            elif action == "reset_circuit_breaker":
                return sanitize_response(await _reset_circuit_breaker())
            else:
                return {
                    "success": False,
                    "error": f"Unknown action '{action}'. Valid actions: health, reset_circuit_breaker",
                }
        except Exception as e:
            logger.error("airflow_admin(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ── Return router tools ────────────────────────────────────────
    return {
        "dag_trigger": dag_trigger,
        "dag_monitor": dag_monitor,
        "airflow_admin": airflow_admin,
    }
