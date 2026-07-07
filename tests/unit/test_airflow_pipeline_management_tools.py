"""Comprehensive tests for all router tools in airflow_pipeline_management.

Covers: pipeline_status, pipeline_control, pipeline_deploy, pipeline_validate, airflow_connections.
"""

from __future__ import annotations

import os
import tempfile
import types
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
import yaml

from teradata_etl_mcp_server.clients.airbyte_client import (
    AirbyteAPIError,
    AirbyteConnectionError,
    CircuitBreakerOpen,
)
from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
    _validate_dbt_target,
    register_pipeline_tools,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_orchestrator(tmp_path: Any = None, **overrides: Any) -> Mock:
    """Build a minimal Mock orchestrator with standard defaults.

    When ``tmp_path`` is provided, pre-creates a per-Teradata-profile
    sub-project layout at ``tmp_path/dbt_project/dbt_default/`` bound to
    identity ``wizard:td_host`` (matching the default settings.teradata.host
    below). ``orch.dbt_project_parent`` is set to the parent so the
    resolver-driven DAG-generation tools (create_dbt_dag, create_sync_dag
    with project_name) can resolve sub-projects.
    """
    orch = Mock()
    orch.async_airflow_client = AsyncMock()
    orch.credential_resolver = Mock()
    orch.settings = Mock()

    # Teradata settings
    orch.settings.teradata = Mock()
    orch.settings.teradata.host = "td-host"
    orch.settings.teradata.username = "dbc"
    orch.settings.teradata.password = Mock()
    orch.settings.teradata.password.get_secret_value = Mock(return_value="secret")
    orch.settings.teradata.database = "test_db"
    orch.settings.teradata.port = 1025

    # Airbyte settings
    orch.settings.airbyte = Mock()
    orch.settings.airbyte.base_url = "http://localhost:8000"
    orch.settings.airbyte.client_id = "test-client-id"
    orch.settings.airbyte.token_url = "http://localhost:8000/token"
    orch.settings.airbyte.client_secret = Mock()
    orch.settings.airbyte.client_secret.get_secret_value = Mock(return_value="test-secret")

    # SSH settings
    orch.settings.ssh = Mock()
    orch.settings.ssh.host = "localhost"
    orch.settings.ssh.port = 22
    orch.settings.ssh.username = "airflow"
    orch.settings.ssh.key_file = None
    orch.settings.ssh.password = Mock()
    orch.settings.ssh.password.get_secret_value = Mock(return_value="ssh-pass")
    orch.settings.ssh.timeout = 300

    # Airflow settings
    orch.settings.airflow = Mock()
    orch.settings.airflow.base_url = "http://localhost:8080"
    orch.settings.airflow.default_owner = "teradata_etl_mcp_server"
    orch.settings.airflow.remote_host = None
    orch.settings.airflow.remote_user = None
    orch.settings.airflow.remote_ssh_key = None
    orch.settings.airflow.remote_password = None
    orch.settings.airflow.remote_port = 22
    orch.settings.airflow.remote_ssh_key_passphrase = None
    orch.settings.airflow.dag_folder = "/opt/airflow/dags"

    # Pipeline settings
    orch.settings.pipeline = Mock()
    orch.settings.pipeline.dags_output_dir = "/tmp/airflow_dags"

    # DAG generator (for create_sync_dag)
    orch.airflow_dag_generator = Mock()
    orch.airflow_dag_generator.generate_dag = Mock(return_value="# generated DAG code")
    orch.airflow_dag_generator.validate_dag_file = Mock(return_value={"valid": True})

    # Per-Teradata-profile dbt sub-project setup. The DAG-generation tools
    # resolve to a real sub-project under ``dbt_project_parent`` via the
    # same resolver as the runtime dbt_* tools.
    if tmp_path is not None:
        parent = tmp_path / "dbt_project"
        parent.mkdir(parents=True, exist_ok=True)
        sub = parent / "dbt_default"
        sub.mkdir(exist_ok=True)
        (sub / "dbt_project.yml").write_text(
            "name: 'default'\nprofile: 'wizard:td_host'\n", encoding="utf-8"
        )
        (sub / "models").mkdir(exist_ok=True)
        orch.dbt_project_parent = parent
        # settings.dbt.project_dir is the parent container.
        orch.settings.dbt = types.SimpleNamespace(project_dir=parent)
    else:
        orch.dbt_project_parent = Path("/nonexistent")
        orch.settings.dbt = types.SimpleNamespace(project_dir=Path("/nonexistent"))

    for key, val in overrides.items():
        setattr(orch, key, val)

    return orch


@pytest.fixture
def orch(tmp_path) -> Mock:
    return _make_orchestrator(tmp_path=tmp_path)


@pytest.fixture
def tools(orch: Mock) -> dict[str, Any]:
    return register_pipeline_tools(orch)


# ═══════════════════════════════════════════════════════════════════════════
#  1. pipeline_status
# ═══════════════════════════════════════════════════════════════════════════

class TestPipelineStatusNullGuard:
    """Null/empty action parameter must be rejected."""

    @pytest.mark.asyncio
    async def test_none_action(self, tools):
        result = await tools["pipeline_status"](action=None)
        assert result["success"] is False
        assert "action" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_empty_string_action(self, tools):
        result = await tools["pipeline_status"](action="")
        assert result["success"] is False
        assert "action" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_whitespace_only_action(self, tools):
        result = await tools["pipeline_status"](action="   ")
        assert result["success"] is False
        assert "action" in result["error"].lower()


class TestPipelineStatusInvalidAction:
    @pytest.mark.asyncio
    async def test_unknown_action(self, tools):
        result = await tools["pipeline_status"](action="bogus")
        assert result["success"] is False
        assert "Unknown action" in result["error"]
        assert "bogus" in result["error"]


class TestPipelineStatusGetStatus:
    @pytest.mark.asyncio
    async def test_success(self, orch, tools):
        orch.get_pipeline_status_async = AsyncMock(return_value={
            "is_paused": False,
            "last_run": {"state": "success", "execution_date": "2026-01-01"},
            "recent_runs": [{"dag_run_id": "run1", "state": "success"}],
            "statistics": {"total_runs": 10, "success_rate": 0.9},
        })
        result = await tools["pipeline_status"](action="get_status", pipeline_name="my_dag")
        assert result["success"] is True
        assert result["pipeline_name"] == "my_dag"
        assert result["is_paused"] is False
        assert "recent_runs" in result

    @pytest.mark.asyncio
    async def test_missing_pipeline_name(self, tools):
        result = await tools["pipeline_status"](action="get_status")
        assert result["success"] is False
        assert "pipeline_name" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.get_pipeline_status_async = AsyncMock(side_effect=RuntimeError("API down"))
        result = await tools["pipeline_status"](action="get_status", pipeline_name="my_dag")
        assert result["success"] is False


class TestPipelineStatusListPipelines:
    @pytest.mark.asyncio
    async def test_success_include_paused(self, orch, tools):
        """Default (include_paused=True) returns both active and paused DAGs."""
        orch.async_airflow_client.list_dags = AsyncMock(return_value=[
            {"dag_id": "dag_a", "is_paused": False, "tags": ["prod"], "schedule_interval": "@daily",
             "last_parsed_time": "2026-01-01T00:00:00"},
            {"dag_id": "dag_b", "is_paused": True, "tags": [], "schedule_interval": None,
             "last_parsed_time": None},
        ])
        result = await tools["pipeline_status"](action="list_pipelines")
        assert result["success"] is True
        assert result["total_count"] == 2
        assert len(result["pipelines"]) == 2
        # Inactive DAGs must always be excluded regardless of include_paused.
        orch.async_airflow_client.list_dags.assert_awaited_once_with(only_active=True, tags=None)

    @pytest.mark.asyncio
    async def test_exclude_paused_filters_client_side(self, orch, tools):
        """include_paused=False filters out paused DAGs from the API response."""
        orch.async_airflow_client.list_dags = AsyncMock(return_value=[
            {"dag_id": "dag_a", "is_paused": False, "tags": ["prod"], "schedule_interval": "@daily",
             "last_parsed_time": "2026-01-01T00:00:00"},
            {"dag_id": "dag_b", "is_paused": True, "tags": [], "schedule_interval": None,
             "last_parsed_time": None},
        ])
        result = await tools["pipeline_status"](action="list_pipelines", include_paused=False)
        assert result["success"] is True
        assert result["total_count"] == 1
        assert result["pipelines"][0]["pipeline_name"] == "dag_a"
        # Inactive DAGs must always be excluded regardless of include_paused.
        orch.async_airflow_client.list_dags.assert_awaited_once_with(only_active=True, tags=None)

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.async_airflow_client.list_dags = AsyncMock(side_effect=RuntimeError("timeout"))
        result = await tools["pipeline_status"](action="list_pipelines")
        assert result["success"] is False


class TestPipelineStatusCheckDagExists:
    @pytest.mark.asyncio
    async def test_success_exists(self, orch, tools):
        orch.async_airflow_client.get_dag = AsyncMock(return_value={
            "dag_id": "check_me", "is_paused": False, "fileloc": "/opt/airflow/dags/check_me.py",
        })
        result = await tools["pipeline_status"](action="check_dag_exists", dag_id="check_me")
        assert result["success"] is True
        assert result["exists"] is True
        assert result["dag_id"] == "check_me"

    @pytest.mark.asyncio
    async def test_success_not_exists(self, orch, tools):
        orch.async_airflow_client.get_dag = AsyncMock(return_value=None)
        result = await tools["pipeline_status"](action="check_dag_exists", dag_id="missing_dag")
        assert result["success"] is True
        assert result["exists"] is False

    @pytest.mark.asyncio
    async def test_missing_dag_id_and_pipeline_name(self, tools):
        result = await tools["pipeline_status"](action="check_dag_exists")
        assert result["success"] is False
        assert "dag_id" in result["error"].lower() or "pipeline_name" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_falls_back_to_pipeline_name(self, orch, tools):
        orch.async_airflow_client.get_dag = AsyncMock(return_value={
            "dag_id": "fallback", "is_paused": True, "fileloc": "/dags/fallback.py",
        })
        result = await tools["pipeline_status"](
            action="check_dag_exists", pipeline_name="fallback"
        )
        assert result["success"] is True
        assert result["exists"] is True

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.async_airflow_client.get_dag = AsyncMock(side_effect=ConnectionError("unreachable"))
        result = await tools["pipeline_status"](action="check_dag_exists", dag_id="x")
        assert result["success"] is False


# ═══════════════════════════════════════════════════════════════════════════
#  2. pipeline_control
# ═══════════════════════════════════════════════════════════════════════════

class TestPipelineControlNullGuard:
    @pytest.mark.asyncio
    async def test_none_action(self, tools):
        result = await tools["pipeline_control"](action=None, pipeline_name="dag1")
        assert result["success"] is False
        assert "action" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_empty_string_action(self, tools):
        result = await tools["pipeline_control"](action="", pipeline_name="dag1")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_whitespace_only_action(self, tools):
        result = await tools["pipeline_control"](action="  ", pipeline_name="dag1")
        assert result["success"] is False


class TestPipelineControlInvalidAction:
    @pytest.mark.asyncio
    async def test_unknown_action(self, tools):
        result = await tools["pipeline_control"](action="restart", pipeline_name="dag1")
        assert result["success"] is False
        assert "Unknown action" in result["error"]
        assert "restart" in result["error"]


class TestPipelineControlPause:
    @pytest.mark.asyncio
    async def test_success(self, orch, tools):
        orch.async_airflow_client.pause_dag = AsyncMock(return_value=None)
        result = await tools["pipeline_control"](action="pause", pipeline_name="test_dag")
        assert result["success"] is True
        assert result["is_paused"] is True
        assert result["pipeline_name"] == "test_dag"
        orch.async_airflow_client.pause_dag.assert_awaited_once_with(dag_id="test_dag")

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.async_airflow_client.pause_dag = AsyncMock(side_effect=RuntimeError("fail"))
        result = await tools["pipeline_control"](action="pause", pipeline_name="dag1")
        assert result["success"] is False


class TestPipelineControlResume:
    @pytest.mark.asyncio
    async def test_success(self, orch, tools):
        orch.async_airflow_client.unpause_dag = AsyncMock(return_value=None)
        result = await tools["pipeline_control"](action="resume", pipeline_name="test_dag")
        assert result["success"] is True
        assert result["is_paused"] is False
        assert result["pipeline_name"] == "test_dag"
        orch.async_airflow_client.unpause_dag.assert_awaited_once_with(dag_id="test_dag")

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.async_airflow_client.unpause_dag = AsyncMock(side_effect=RuntimeError("fail"))
        result = await tools["pipeline_control"](action="resume", pipeline_name="dag1")
        assert result["success"] is False


class TestPipelineControlUpdateSchedule:
    @pytest.mark.asyncio
    async def test_missing_new_schedule(self, tools):
        result = await tools["pipeline_control"](action="update_schedule", pipeline_name="dag1")
        assert result["success"] is False
        assert "new_schedule" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_success(self, orch, tools, tmp_path):
        """Update schedule works on MCP-generated DAGs using Airflow 2.4+ schedule= param."""
        dag_file = tmp_path / "test_dag.py"
        dag_file.write_text(
            'dag = DAG("test_dag", schedule="@daily")\n', encoding="utf-8"
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        # Re-register so the closure captures the updated settings
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="test_dag", new_schedule="@hourly"
        )
        assert result["success"] is True
        assert result["old_schedule"] == "@daily"
        assert result["new_schedule"] == "@hourly"
        updated = (tmp_path / "test_dag.py").read_text()
        assert 'schedule="@hourly"' in updated

    @pytest.mark.asyncio
    async def test_success_legacy_schedule_interval(self, orch, tools, tmp_path):
        """Update schedule also works on legacy DAGs using schedule_interval= param."""
        dag_file = tmp_path / "legacy_dag.py"
        dag_file.write_text(
            'dag = DAG("legacy_dag", schedule_interval="@daily")\n', encoding="utf-8"
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="legacy_dag", new_schedule="@weekly"
        )
        assert result["success"] is True
        updated = (tmp_path / "legacy_dag.py").read_text()
        assert 'schedule_interval="@weekly"' in updated

    @pytest.mark.asyncio
    async def test_legacy_schedule_interval_to_cron_expression(self, orch, tools, tmp_path):
        """Cron expressions with special chars (*, space) are written verbatim into schedule_interval=."""
        dag_file = tmp_path / "legacy_cron_dag.py"
        dag_file.write_text(
            'dag = DAG("legacy_cron_dag", schedule_interval="@daily")\n', encoding="utf-8"
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="legacy_cron_dag", new_schedule="0 8 * * *"
        )
        assert result["success"] is True
        assert result["old_schedule"] == "@daily"
        assert result["new_schedule"] == "0 8 * * *"
        updated = (tmp_path / "legacy_cron_dag.py").read_text()
        assert 'schedule_interval="0 8 * * *"' in updated
        assert "\\" not in updated  # no backslash escaping of * or spaces

    @pytest.mark.asyncio
    async def test_legacy_schedule_interval_from_cron_expression(self, orch, tools, tmp_path):
        """old_schedule is correctly extracted when the existing schedule_interval is a cron expression."""
        dag_file = tmp_path / "legacy_cron_dag.py"
        dag_file.write_text(
            'dag = DAG("legacy_cron_dag", schedule_interval="0 8 * * *")\n', encoding="utf-8"
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="legacy_cron_dag", new_schedule="@hourly"
        )
        assert result["success"] is True
        assert result["old_schedule"] == "0 8 * * *"
        assert result["new_schedule"] == "@hourly"
        updated = (tmp_path / "legacy_cron_dag.py").read_text()
        assert 'schedule_interval="@hourly"' in updated

    @pytest.mark.asyncio
    async def test_success_with_cron_expression(self, orch, tools, tmp_path):
        """Cron expressions with special characters (*, space) are written verbatim — not escaped."""
        dag_file = tmp_path / "cron_dag.py"
        dag_file.write_text('dag = DAG("cron_dag", schedule="@daily")\n', encoding="utf-8")
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="cron_dag", new_schedule="0 8 * * *"
        )
        assert result["success"] is True
        assert result["old_schedule"] == "@daily"
        assert result["new_schedule"] == "0 8 * * *"
        updated = (tmp_path / "cron_dag.py").read_text()
        # Verify the cron expression is written cleanly — no backslash escaping
        assert 'schedule="0 8 * * *"' in updated
        assert "\\" not in updated

    @pytest.mark.asyncio
    async def test_regex_fallback_cron_with_comma(self, orch, tools, tmp_path):
        """Regex fallback handles commas in cron fields (e.g. '0,30 * * * *').

        The file is given a deliberate syntax error (missing closing paren) so the
        AST path cannot parse it and the regex fallback is used instead.  The old
        pattern excluded commas from the character class, causing count==0 and a
        'Could not find schedule' error for valid comma-containing cron expressions.
        """
        dag_file = tmp_path / "comma_cron.py"
        # Missing closing ')' → SyntaxError → forces regex fallback
        dag_file.write_text(
            'dag = DAG("comma_cron", schedule="0,30 * * * *"\n',
            encoding="utf-8",
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="comma_cron", new_schedule="@daily"
        )
        assert result["success"] is True
        assert result["new_schedule"] == "@daily"
        assert result["old_schedule"] == "0,30 * * * *"
        updated = (tmp_path / "comma_cron.py").read_text()
        assert 'schedule="@daily"' in updated

    @pytest.mark.asyncio
    async def test_ast_ignores_schedule_in_comment_and_docstring(self, orch, tools, tmp_path):
        """AST-based replacement only modifies the DAG() constructor arg, not comments/strings."""
        content = (
            '# Old schedule="@weekly" kept for reference\n'
            'dag = DAG("precise_dag", schedule="@daily")\n'
            '"""schedule="@monthly" appears in docstring too"""\n'
        )
        dag_file = tmp_path / "precise_dag.py"
        dag_file.write_text(content, encoding="utf-8")
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="precise_dag", new_schedule="@hourly"
        )
        assert result["success"] is True
        updated = (tmp_path / "precise_dag.py").read_text()
        # Only the DAG() constructor arg was changed
        assert 'schedule="@hourly"' in updated
        # Comment and docstring occurrences must remain unchanged
        assert 'schedule="@weekly"' in updated
        assert 'schedule="@monthly"' in updated

    @pytest.mark.asyncio
    async def test_no_schedule_param_returns_error(self, orch, tools, tmp_path):
        """Returns clear error when DAG file has neither schedule nor schedule_interval."""
        dag_file = tmp_path / "bad_dag.py"
        dag_file.write_text('dag = DAG("bad_dag")\n', encoding="utf-8")
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="bad_dag", new_schedule="@daily"
        )
        assert result["success"] is False
        assert "schedule" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_no_schedule_param_with_none_new_schedule_returns_error(self, orch, tools, tmp_path):
        """DAG with no schedule keyword and new_schedule='None' must not return a false no_op.

        Without the count > 0 guard on the idempotency check, old_schedule_value stays
        None and _normalize(None) == _normalize('None') evaluates to True, causing the
        function to return success=True, no_op=True even though no parameter was found.
        """
        dag_file = tmp_path / "bad_dag.py"
        dag_file.write_text('dag = DAG("bad_dag")\n', encoding="utf-8")
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="bad_dag", new_schedule="None"
        )
        assert result["success"] is False
        assert result.get("no_op") is not True
        assert "schedule" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        # DAG file does not exist so helper returns success=False
        orch.settings.pipeline.dags_output_dir = "/nonexistent"
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="dag1", new_schedule="@hourly"
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_old_schedule_extracted_correctly(self, orch, tools, tmp_path):
        """old_schedule in response reflects the actual value from the file, not a placeholder."""
        dag_file = tmp_path / "test_dag.py"
        dag_file.write_text('dag = DAG("test_dag", schedule="@weekly")\n', encoding="utf-8")
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="test_dag", new_schedule="@daily"
        )
        assert result["success"] is True
        assert result["old_schedule"] == "@weekly"
        assert result["new_schedule"] == "@daily"

    @pytest.mark.asyncio
    async def test_idempotent_no_write_when_schedule_unchanged(self, orch, tools, tmp_path):
        """Returns success with no_op=True and auto_deploy_skipped when auto_deploy=False."""
        dag_file = tmp_path / "test_dag.py"
        dag_file.write_text('dag = DAG("test_dag", schedule="@daily")\n', encoding="utf-8")
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        with patch("pathlib.Path.write_text") as mock_write:
            result = await t["pipeline_control"](
                action="update_schedule", pipeline_name="test_dag", new_schedule="@daily"
            )
        assert result["success"] is True
        assert result.get("no_op") is True
        assert result.get("auto_deploy_skipped") is True  # deploy was not requested
        assert "auto_deploy" not in result               # deploy block was not entered
        mock_write.assert_not_called()                   # file not rewritten

    @pytest.mark.asyncio
    async def test_idempotent_no_op_still_deploys_when_auto_deploy_true(self, orch, tools, tmp_path):
        """When schedule is unchanged but auto_deploy=True, deploy still runs (force-sync)."""
        dag_file = tmp_path / "test_dag.py"
        dag_file.write_text('dag = DAG("test_dag", schedule="@daily")\n', encoding="utf-8")
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        with patch("pathlib.Path.write_text") as mock_write:
            result = await t["pipeline_control"](
                action="update_schedule", pipeline_name="test_dag",
                new_schedule="@daily", auto_deploy=True,
            )
        assert result["success"] is True
        assert result.get("no_op") is True
        mock_write.assert_not_called()                   # file still not rewritten
        assert "auto_deploy" in result                   # deploy was attempted
        assert result["auto_deploy"]["triggered"] is True
        assert "auto_deploy_skipped" not in result       # skipped key absent when triggered

    @pytest.mark.asyncio
    async def test_auto_deploy_not_in_response_when_disabled(self, orch, tools, tmp_path):
        """auto_deploy key is absent from response when auto_deploy=False."""
        dag_file = tmp_path / "test_dag.py"
        dag_file.write_text('dag = DAG("test_dag", schedule="@daily")\n', encoding="utf-8")
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="test_dag",
            new_schedule="@hourly", auto_deploy=False,
        )
        assert result["success"] is True
        assert "auto_deploy" not in result  # not triggered when auto_deploy=False

    @pytest.mark.asyncio
    async def test_auto_deploy_flag_in_response_when_no_ssh_config(self, orch, tools, tmp_path):
        """auto_deploy=True is reflected in response even when deploy cannot connect."""
        dag_file = tmp_path / "test_dag.py"
        dag_file.write_text('dag = DAG("test_dag", schedule="@daily")\n', encoding="utf-8")
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="test_dag",
            new_schedule="@hourly", auto_deploy=True,
        )
        assert result["success"] is True          # file edit succeeded
        assert "auto_deploy" in result            # deploy was attempted
        assert result["auto_deploy"]["triggered"] is True
        assert 'schedule="@hourly"' in (tmp_path / "test_dag.py").read_text()

    @pytest.mark.asyncio
    async def test_remote_fallback_error_when_no_ssh_config(self, orch, tools, tmp_path):
        """When local file missing and SSH not configured, returns combined error message."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="missing_dag", new_schedule="@daily"
        )
        assert result["success"] is False
        assert "remote fetch" in result["error"].lower() or "not found" in result["error"].lower()
        assert "suggestion" in result

    @pytest.mark.asyncio
    async def test_strict_host_key_checking_param_accepted(self, orch, tools, tmp_path):
        """strict_host_key_checking=False is accepted and forwarded without raising TypeError."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        # No SSH env vars → _fetch_dag_from_remote returns success=False with credential error
        result = await t["pipeline_control"](
            action="update_schedule",
            pipeline_name="missing_dag",
            new_schedule="@daily",
            strict_host_key_checking=False,
        )
        assert result["success"] is False
        # Must NOT be a TypeError about unexpected keyword argument
        assert "unexpected keyword" not in result.get("error", "").lower()
        assert "suggestion" in result

    @pytest.mark.asyncio
    async def test_multiline_schedule_replaced(self, orch, tools, tmp_path):
        """AST multi-line replacement collapses a timedelta(...) spread across lines into a single schedule value."""
        dag_file = tmp_path / "timedelta_dag.py"
        dag_file.write_text(
            'dag = DAG("timedelta_dag", schedule=timedelta(\n    days=1\n))\n',
            encoding="utf-8",
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="timedelta_dag", new_schedule="@daily"
        )
        assert result["success"] is True
        assert result["new_schedule"] == "@daily"
        updated = (tmp_path / "timedelta_dag.py").read_text()
        # Multi-line timedelta must be fully replaced by the single-line preset
        assert 'schedule="@daily"' in updated
        assert "timedelta(" not in updated
        # Replacement must not introduce extra blank lines from the deleted lines
        assert updated.count("\n") == 1

    @pytest.mark.asyncio
    async def test_multiple_dag_calls_modifies_first_in_source_order(self, orch, tools, tmp_path):
        """When a file contains multiple DAG() calls, the first one in source order is modified."""
        dag_file = tmp_path / "multi_dag.py"
        dag_file.write_text(
            'first = DAG("first_dag", schedule="@daily")\n'
            'second = DAG("second_dag", schedule="@weekly")\n',
            encoding="utf-8",
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_control"](
            action="update_schedule", pipeline_name="multi_dag", new_schedule="@hourly"
        )
        assert result["success"] is True
        assert result["old_schedule"] == "@daily"
        updated = (tmp_path / "multi_dag.py").read_text()
        # Only the first DAG()'s schedule is changed
        assert 'schedule="@hourly"' in updated
        assert 'schedule="@weekly"' in updated  # second DAG unchanged


class TestPipelineControlDelete:
    @pytest.mark.asyncio
    async def test_success(self, orch, tools):
        orch.async_airflow_client.delete_dag = AsyncMock(return_value=None)
        result = await tools["pipeline_control"](action="delete", pipeline_name="test_dag", confirm=True)
        assert result["success"] is True
        assert result["pipeline_name"] == "test_dag"
        orch.async_airflow_client.delete_dag.assert_awaited_once_with(dag_id="test_dag")

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.async_airflow_client.delete_dag = AsyncMock(
            side_effect=RuntimeError("cannot delete")
        )
        result = await tools["pipeline_control"](action="delete", pipeline_name="dag1", confirm=True)
        assert "pipeline_name" in result

    @pytest.mark.asyncio
    async def test_delete_pipeline_no_confirm_returns_warning(self, orch, tools):
        orch.async_airflow_client.delete_dag = AsyncMock(return_value=None)
        result = await tools["pipeline_control"](action="delete", pipeline_name="test_dag")
        assert result["success"] is False
        assert result["requires_confirmation"] is True
        assert result["action"] == "delete"
        assert result["pipeline_name"] == "test_dag"
        assert "hint" in result
        orch.async_airflow_client.delete_dag.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_pipeline_no_confirm_shows_file_impact(self, tools):
        result = await tools["pipeline_control"](
            action="delete", pipeline_name="test_dag", delete_dag_file=True
        )
        assert result["requires_confirmation"] is True
        assert "BE DELETED" in result["detail"]

    @pytest.mark.asyncio
    async def test_delete_warns_when_ssh_not_configured(self, orch, tools):
        orch.async_airflow_client.delete_dag = AsyncMock(return_value=None)
        orch.settings.airflow.remote_host = None
        orch.settings.airflow.remote_user = None
        result = await tools["pipeline_control"](
            action="delete", pipeline_name="test_dag", confirm=True
        )
        assert result["success"] is True
        assert any("SSH not configured" in w for w in result["warnings"])
        assert "remote_dag_file" in result

    @pytest.mark.asyncio
    async def test_delete_removes_remote_dag_via_sftp(self, orch, tools):
        orch.async_airflow_client.delete_dag = AsyncMock(return_value=None)
        orch.settings.airflow.remote_host = "airflow-host"
        orch.settings.airflow.remote_user = "airflow"
        orch.settings.airflow.remote_ssh_key = "/path/to/key"
        orch.settings.airflow.remote_ssh_key_passphrase = None

        mock_sftp = Mock()
        mock_ssh_client = Mock()
        mock_ssh_client.open_sftp.return_value = mock_sftp

        mock_paramiko = Mock()
        mock_paramiko.SSHClient.return_value = mock_ssh_client
        mock_paramiko.AutoAddPolicy.return_value = Mock()
        mock_paramiko.RejectPolicy.return_value = Mock()

        orch.settings.airflow.remote_ssh_key = None
        orch.settings.airflow.remote_password = Mock()
        orch.settings.airflow.remote_password.get_secret_value = Mock(return_value="pass")

        with patch.dict("sys.modules", {"paramiko": mock_paramiko}):
            result = await tools["pipeline_control"](
                action="delete", pipeline_name="test_dag", confirm=True
            )

        assert result["success"] is True
        assert "remote_dag_file" in result["deleted_components"]
        mock_sftp.remove.assert_called_once_with("/opt/airflow/dags/test_dag.py")

    @pytest.mark.asyncio
    async def test_delete_handles_remote_file_not_found(self, orch, tools):
        orch.async_airflow_client.delete_dag = AsyncMock(return_value=None)
        orch.settings.airflow.remote_host = "airflow-host"
        orch.settings.airflow.remote_user = "airflow"
        orch.settings.airflow.remote_ssh_key = None
        orch.settings.airflow.remote_password = Mock()
        orch.settings.airflow.remote_password.get_secret_value = Mock(return_value="pass")
        orch.settings.airflow.remote_ssh_key_passphrase = None

        mock_sftp = Mock()
        mock_sftp.remove.side_effect = FileNotFoundError("No such file")
        mock_ssh_client = Mock()
        mock_ssh_client.open_sftp.return_value = mock_sftp

        mock_paramiko = Mock()
        mock_paramiko.SSHClient.return_value = mock_ssh_client
        mock_paramiko.AutoAddPolicy.return_value = Mock()
        mock_paramiko.RejectPolicy.return_value = Mock()

        with patch.dict("sys.modules", {"paramiko": mock_paramiko}):
            result = await tools["pipeline_control"](
                action="delete", pipeline_name="test_dag", confirm=True
            )

        assert result["success"] is True
        assert any("not found" in w for w in result["warnings"])
        assert "remote_dag_file" not in result["deleted_components"]

    @pytest.mark.asyncio
    async def test_delete_handles_sftp_failure(self, orch, tools):
        orch.async_airflow_client.delete_dag = AsyncMock(return_value=None)
        orch.settings.airflow.remote_host = "airflow-host"
        orch.settings.airflow.remote_user = "airflow"
        orch.settings.airflow.remote_ssh_key = None
        orch.settings.airflow.remote_password = Mock()
        orch.settings.airflow.remote_password.get_secret_value = Mock(return_value="pass")
        orch.settings.airflow.remote_ssh_key_passphrase = None

        mock_ssh_client = Mock()
        mock_ssh_client.connect.side_effect = OSError("Connection refused")

        mock_paramiko = Mock()
        mock_paramiko.SSHClient.return_value = mock_ssh_client
        mock_paramiko.AutoAddPolicy.return_value = Mock()
        mock_paramiko.RejectPolicy.return_value = Mock()

        with patch.dict("sys.modules", {"paramiko": mock_paramiko}):
            result = await tools["pipeline_control"](
                action="delete", pipeline_name="test_dag", confirm=True
            )

        assert result["success"] is True
        assert any("Failed to delete remote" in w for w in result["warnings"])

    @pytest.mark.asyncio
    async def test_delete_no_confirm_shows_sftp_ready(self, orch, tools):
        orch.settings.airflow.remote_host = "airflow-host"
        orch.settings.airflow.remote_user = "airflow"
        orch.settings.airflow.remote_ssh_key = "/path/to/key"
        result = await tools["pipeline_control"](
            action="delete", pipeline_name="test_dag"
        )
        assert result["requires_confirmation"] is True
        assert "attempted via SFTP" in result["detail"]

    @pytest.mark.asyncio
    async def test_delete_no_confirm_shows_no_credentials(self, orch, tools):
        orch.settings.airflow.remote_host = "airflow-host"
        orch.settings.airflow.remote_user = "airflow"
        orch.settings.airflow.remote_ssh_key = None
        orch.settings.airflow.remote_password = None
        result = await tools["pipeline_control"](
            action="delete", pipeline_name="test_dag"
        )
        assert result["requires_confirmation"] is True
        assert "credentials not configured" in result["detail"]

    @pytest.mark.asyncio
    async def test_delete_no_confirm_shows_no_ssh(self, orch, tools):
        orch.settings.airflow.remote_host = None
        orch.settings.airflow.remote_user = None
        result = await tools["pipeline_control"](
            action="delete", pipeline_name="test_dag"
        )
        assert result["requires_confirmation"] is True
        assert "SSH not configured" in result["detail"]


# ═══════════════════════════════════════════════════════════════════════════
#  3. pipeline_deploy
# ═══════════════════════════════════════════════════════════════════════════

class TestPipelineDeployNullGuard:
    @pytest.mark.asyncio
    async def test_none_action(self, tools):
        result = await tools["pipeline_deploy"](action=None)
        assert result["success"] is False
        assert "action" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_empty_string_action(self, tools):
        result = await tools["pipeline_deploy"](action="")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_whitespace_only_action(self, tools):
        result = await tools["pipeline_deploy"](action="   ")
        assert result["success"] is False


class TestPipelineDeployInvalidAction:
    @pytest.mark.asyncio
    async def test_unknown_action(self, tools):
        result = await tools["pipeline_deploy"](action="rollback")
        assert result["success"] is False
        assert "Unknown action" in result["error"]


class TestPipelineDeployParameterValidation:
    """Validate remote_port (1-65535) and max_wait_seconds (>= 1)."""

    @pytest.mark.asyncio
    async def test_remote_port_zero(self, tools):
        result = await tools["pipeline_deploy"](action="deploy_dags", remote_port=0)
        assert result["success"] is False
        assert "remote_port" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_remote_port_negative(self, tools):
        result = await tools["pipeline_deploy"](action="deploy_dags", remote_port=-1)
        assert result["success"] is False
        assert "remote_port" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_remote_port_too_large(self, tools):
        result = await tools["pipeline_deploy"](action="deploy_dags", remote_port=70000)
        assert result["success"] is False
        assert "remote_port" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_remote_port_boundary_low(self, tools):
        """remote_port=1 is valid; should not fail on port validation."""
        result = await tools["pipeline_deploy"](action="deploy_dags", remote_port=1)
        # Should pass port validation (may fail on other issues inside deploy_dags)
        assert "remote_port" not in result.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_remote_port_boundary_high(self, tools):
        """remote_port=65535 is valid."""
        result = await tools["pipeline_deploy"](action="deploy_dags", remote_port=65535)
        assert "remote_port" not in result.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_max_wait_seconds_zero(self, tools):
        result = await tools["pipeline_deploy"](action="deploy_dags", max_wait_seconds=0)
        assert result["success"] is False
        assert "max_wait_seconds" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_max_wait_seconds_negative(self, tools):
        result = await tools["pipeline_deploy"](action="deploy_dags", max_wait_seconds=-5)
        assert result["success"] is False
        assert "max_wait_seconds" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_max_wait_seconds_one_is_valid(self, tools):
        """max_wait_seconds=1 should be accepted by the guard."""
        result = await tools["pipeline_deploy"](action="deploy_dags", max_wait_seconds=1)
        assert "max_wait_seconds" not in result.get("error", "").lower()


class TestPipelineDeployDeployComplete:
    @pytest.mark.asyncio
    async def test_missing_pipeline_name(self, tools):
        result = await tools["pipeline_deploy"](action="deploy_complete")
        assert result["success"] is False
        assert "pipeline_name" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_success_dag_file_not_found(self, orch, tools):
        """deploy_complete runs but dag file does not exist -- should still return result."""
        orch.settings.airflow.remote_host = "remote"
        orch.settings.airflow.remote_user = "user"
        orch.settings.airflow.remote_password = "pw"
        result = await tools["pipeline_deploy"](
            action="deploy_complete", pipeline_name="my_pipeline"
        )
        # The helper returns a result dict (may be success with failed components)
        assert "pipeline_name" in result or "error" in result


class TestPipelineDeployCreateSyncDag:
    @pytest.mark.asyncio
    async def test_missing_dag_id(self, tools):
        result = await tools["pipeline_deploy"](
            action="create_sync_dag", connection_id="conn123"
        )
        assert result["success"] is False
        assert "dag_id" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_missing_connection_id(self, tools):
        result = await tools["pipeline_deploy"](
            action="create_sync_dag", dag_id="sync_dag"
        )
        assert result["success"] is False
        assert "connection_id" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_success(self, orch, tmp_path):
        """create_sync_dag generates a DAG file via the generator."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        # The inner helper calls _create_airflow_airbyte_connection which calls
        # get_connection -- make it raise 404 to proceed to creation
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        # Airbyte client for schedule check
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "manual"},
        })

        # The DAG generator writes a file and returns code
        dag_file = tmp_path / "sync_test.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_test",
            connection_id="airbyte-conn-123",
            output_filename="sync_test.py",
        )
        assert result["success"] is True
        assert result["dag_id"] == "sync_test"
        assert result["airflow_connection_status"] == "created"
        assert result["airbyte_schedule_check"] == "already_manual"

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch):
        """Exception inside generator should be caught."""
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
        })

        # Airbyte client for schedule check
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "manual"},
        })

        orch.airflow_dag_generator.generate_dag = Mock(side_effect=RuntimeError("template err"))

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="fail_dag",
            connection_id="conn1",
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_create_sync_dag_auto_sets_cron_to_manual(self, orch, tmp_path):
        """When Airbyte connection has a cron schedule, auto-set to manual and report override."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        # Mock airbyte_client with cron schedule
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "cron", "cronExpression": "0 0 2 ? * *"},
        })
        orch.airbyte_client.update_connection = AsyncMock(return_value={
            "connectionId": "airbyte-conn-456",
            "schedule": {"scheduleType": "manual"},
        })

        dag_file = tmp_path / "sync_warn.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_warn",
            connection_id="airbyte-conn-456",
            output_filename="sync_warn.py",
        )
        assert result["success"] is True
        assert result["airflow_connection_status"] == "created"
        assert result["airbyte_schedule_check"] == "overridden_to_manual"
        # Verify auto-fix details
        override = result["schedule_override"]
        assert override["schedule_overridden"] is True
        assert override["previous_schedule_type"] == "cron"
        assert override["previous_cron_expression"] == "0 0 2 ? * *"
        assert override["new_schedule_type"] == "manual"
        assert "duplicate runs" in override["reason"]
        orch.airbyte_client.update_connection.assert_called_once_with(
            "airbyte-conn-456", schedule={"scheduleType": "manual"}
        )
        assert any("manual" in n for n in result["warnings"])

    @pytest.mark.asyncio
    async def test_create_sync_dag_detects_top_level_schedule_type(self, orch, tmp_path):
        """Cron schedule at top level (no nested 'schedule' dict) is still detected."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        # Top-level scheduleType, no nested schedule dict
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "scheduleType": "cron",
            "cronExpression": "0 0 4 ? * *",
        })
        orch.airbyte_client.update_connection = AsyncMock(return_value={})

        dag_file = tmp_path / "sync_toplevel.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_toplevel",
            connection_id="conn-top",
            output_filename="sync_toplevel.py",
        )
        assert result["success"] is True
        assert result["airbyte_schedule_check"] == "overridden_to_manual"
        assert result["schedule_override"]["previous_cron_expression"] == "0 0 4 ? * *"
        orch.airbyte_client.update_connection.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_sync_dag_handles_schedule_none(self, orch, tmp_path):
        """When Airbyte returns schedule: None, no crash and no override."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": None,
        })

        dag_file = tmp_path / "sync_null.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_null",
            connection_id="conn-null",
            output_filename="sync_null.py",
        )
        assert result["success"] is True
        # Falls through to default "manual" — no override needed
        assert result["airbyte_schedule_check"] == "already_manual"
        assert "schedule_override" not in result
        orch.airbyte_client.update_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_sync_dag_no_override_on_manual_connection(self, orch, tmp_path):
        """When Airbyte connection is already manual, no override should occur."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "manual"},
        })

        dag_file = tmp_path / "sync_ok.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_ok",
            connection_id="airbyte-conn-789",
            output_filename="sync_ok.py",
        )
        assert result["success"] is True
        assert result["airflow_connection_status"] == "created"
        assert result["airbyte_schedule_check"] == "already_manual"
        assert "schedule_override" not in result
        assert "airbyte_schedule_error" not in result
        orch.airbyte_client.update_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_sync_dag_airflow_down(self, orch, tmp_path):
        """When Airflow is unreachable, DAG is created with failed connection status."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        # Airflow completely down
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("Connection refused")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            side_effect=Exception("Connection refused")
        )

        # Airbyte up with manual schedule
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "manual"},
        })

        dag_file = tmp_path / "sync_noairflow.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_noairflow",
            connection_id="conn-1",
            output_filename="sync_noairflow.py",
        )
        assert result["success"] is True
        assert result["airflow_connection_status"] == "failed"
        assert result["airflow_connection_error"] is not None
        assert result["airbyte_schedule_check"] == "already_manual"
        # Notes should explain the Airflow failure
        assert any("fail at runtime" in n for n in result["warnings"])

    @pytest.mark.asyncio
    async def test_create_sync_dag_airbyte_down(self, orch, tmp_path):
        """When Airbyte API is unreachable, DAG is created with unreachable schedule check."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        # Airflow up
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        # Airbyte down (connectivity error)
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(
            side_effect=AirbyteConnectionError("Connection refused")
        )

        dag_file = tmp_path / "sync_noairbyte.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_noairbyte",
            connection_id="conn-2",
            output_filename="sync_noairbyte.py",
        )
        assert result["success"] is True
        assert result["airflow_connection_status"] == "created"
        assert result["airbyte_schedule_check"] == "unreachable"
        assert result["airbyte_schedule_error"] is not None
        assert "Could not reach Airbyte API" in result["airbyte_schedule_error"]
        assert "Verify manually" in result["airbyte_schedule_error"]
        assert any("duplicate runs" in n for n in result["warnings"])

    @pytest.mark.asyncio
    async def test_create_sync_dag_both_down(self, orch, tmp_path):
        """When both Airflow and Airbyte are down, DAG is created with both failures reported."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        # Both down
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("Connection refused")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            side_effect=Exception("Connection refused")
        )
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(
            side_effect=CircuitBreakerOpen("Circuit breaker open")
        )

        dag_file = tmp_path / "sync_bothdown.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_bothdown",
            connection_id="conn-3",
            output_filename="sync_bothdown.py",
        )
        assert result["success"] is True
        assert result["airflow_connection_status"] == "failed"
        assert result["airflow_connection_error"] is not None
        assert result["airbyte_schedule_check"] == "unreachable"
        assert result["airbyte_schedule_error"] is not None

    @pytest.mark.asyncio
    async def test_create_sync_dag_airbyte_get_ok_update_fails(self, orch, tmp_path):
        """When Airbyte get_connection works but update_connection fails, report specific error."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        # Airbyte get works (cron), but update fails
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "cron", "cronExpression": "0 0 3 ? * *"},
        })
        orch.airbyte_client.update_connection = AsyncMock(
            side_effect=Exception("403 Forbidden")
        )

        dag_file = tmp_path / "sync_updatefail.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_updatefail",
            connection_id="conn-4",
            output_filename="sync_updatefail.py",
        )
        assert result["success"] is True
        assert result["airflow_connection_status"] == "created"
        assert result["airbyte_schedule_check"] == "update_failed"
        assert "schedule_override" not in result
        assert result["airbyte_schedule_error"] is not None
        assert "0 0 3 ? * *" in result["airbyte_schedule_error"]
        assert "duplicate runs" in result["airbyte_schedule_error"]

    @pytest.mark.asyncio
    async def test_create_sync_dag_airbyte_api_error(self, orch, tmp_path):
        """When Airbyte returns an API error (e.g. 404), report api_error not unreachable."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        # Airbyte reachable but connection not found
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(
            side_effect=AirbyteAPIError("404: Connection not found")
        )

        dag_file = tmp_path / "sync_notfound.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_notfound",
            connection_id="conn-missing",
            output_filename="sync_notfound.py",
        )
        assert result["success"] is True
        assert result["airbyte_schedule_check"] == "api_error"
        assert result["airbyte_schedule_error"] is not None
        assert "may not exist" in result["airbyte_schedule_error"]
        assert "Verify manually" in result["airbyte_schedule_error"]

    @pytest.mark.asyncio
    async def test_create_sync_dag_airbyte_unexpected_error(self, orch, tmp_path):
        """When an unexpected (non-Airbyte) error occurs, report check_failed."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        # Unexpected non-Airbyte error
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(
            side_effect=RuntimeError("Unexpected internal error")
        )

        dag_file = tmp_path / "sync_unexpected.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_unexpected",
            connection_id="conn-5",
            output_filename="sync_unexpected.py",
        )
        assert result["success"] is True
        assert result["airbyte_schedule_check"] == "check_failed"
        assert result["airbyte_schedule_error"] is not None
        assert "Unexpected error" in result["airbyte_schedule_error"]
        assert "Verify manually" in result["airbyte_schedule_error"]

    @pytest.mark.asyncio
    async def test_create_sync_dag_gen_failure_does_not_mutate_schedule(self, orch, tmp_path):
        """If DAG generation fails, Airbyte schedule must NOT be changed to manual."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        # Airbyte reports cron schedule
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "cron", "cronExpression": "0 0 2 ? * *"},
        })
        orch.airbyte_client.update_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "manual"},
        })

        # DAG generation explodes
        orch.airflow_dag_generator.generate_dag = Mock(
            side_effect=RuntimeError("Template rendering failed")
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_gen_fail",
            connection_id="conn-6",
            output_filename="sync_gen_fail.py",
        )
        assert result["success"] is False
        # The schedule override must NOT have been applied
        orch.airbyte_client.update_connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_sync_dag_invalid_dag_does_not_mutate_schedule(self, orch, tmp_path):
        """If DAG validation fails, Airbyte schedule must NOT be changed to manual."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })

        # Airbyte reports cron schedule
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "cron", "cronExpression": "0 0 2 ? * *"},
        })
        orch.airbyte_client.update_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "manual"},
        })

        # DAG generates but fails validation
        dag_file = tmp_path / "sync_invalid.py"
        dag_file.write_text("# bad dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# bad dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": False, "syntax_error": "SyntaxError: invalid syntax"}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_invalid",
            connection_id="conn-7",
            output_filename="sync_invalid.py",
        )
        # DAG was generated (success=True) but is invalid
        assert result["success"] is True
        assert result["syntax_valid"] is False
        # The schedule override must NOT have been applied
        orch.airbyte_client.update_connection.assert_not_called()
        assert result["airbyte_schedule_check"] == "skipped_dag_invalid"
        assert result["airbyte_schedule_error"] is not None
        assert "DAG is invalid" in result["airbyte_schedule_error"]
        assert "NOT changed to manual" in result["airbyte_schedule_error"]

    @pytest.mark.asyncio
    async def test_create_sync_dag_airflow_failed_does_not_mutate_schedule(self, orch, tmp_path):
        """If Airflow connection setup failed, Airbyte schedule must NOT be changed."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)

        # Airflow connection setup fails
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("Connection refused")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            side_effect=Exception("Connection refused")
        )

        # Airbyte reports cron schedule
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "cron", "cronExpression": "0 0 2 ? * *"},
        })
        orch.airbyte_client.update_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "manual"},
        })

        # DAG generates and validates OK
        dag_file = tmp_path / "sync_airflow_fail.py"
        dag_file.write_text("# dag code", encoding="utf-8")
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# dag code")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_airflow_fail",
            connection_id="conn-8",
            output_filename="sync_airflow_fail.py",
        )
        assert result["success"] is True
        assert result["airflow_connection_status"] == "failed"
        # The schedule override must NOT have been applied
        orch.airbyte_client.update_connection.assert_not_called()
        assert result["airbyte_schedule_check"] == "skipped_airflow_failed"
        assert result["airbyte_schedule_error"] is not None
        assert "Airflow connection setup failed" in result["airbyte_schedule_error"]
        assert "NOT changed to manual" in result["airbyte_schedule_error"]


class TestPipelineDeployDeployDags:
    @pytest.mark.asyncio
    async def test_deploy_dags_dry_run_no_local_dir(self, orch, tools):
        """deploy_dags with non-existent local dir returns validation errors."""
        orch.settings.pipeline.dags_output_dir = "/nonexistent_path_xyz"
        t = register_pipeline_tools(orch)
        result = await t["pipeline_deploy"](
            action="deploy_dags",
            remote_host="host",
            remote_user="user",
            remote_dags_dir="/opt/airflow/dags",
            ssh_key_path="/fake/key",
            dry_run=True,
        )
        assert result["success"] is False
        assert "errors" in result

    @pytest.mark.asyncio
    async def test_deploy_dags_dry_run_success(self, orch, tools, tmp_path):
        """deploy_dags with dry_run=True and a valid local dir returns plan."""
        dag_file = tmp_path / "my_dag.py"
        dag_file.write_text("from airflow import DAG\n", encoding="utf-8")
        key_file = tmp_path / "fake_key"
        key_file.write_text("KEY", encoding="utf-8")

        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_deploy"](
            action="deploy_dags",
            remote_host="host",
            remote_user="user",
            remote_dags_dir="/opt/airflow/dags",
            ssh_key_path=str(key_file),
            dry_run=True,
            validate_imports=False,
        )
        assert result["success"] is True
        assert result["deployed"] == 0
        assert "plan" in result

    @pytest.mark.asyncio
    async def test_deploy_dags_bare_name_blocked_by_validate_imports(self, orch, tmp_path):
        """deploy_dags with validate_imports=True must reject a DAG containing a bare-name expression."""
        dag_file = tmp_path / "bad_dag.py"
        dag_file.write_text(
            "from airflow import DAG\nasdfsdf\ndag = DAG('test', schedule_interval=None)",
            encoding="utf-8",
        )
        key_file = tmp_path / "fake_key"
        key_file.write_text("KEY", encoding="utf-8")

        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        t = register_pipeline_tools(orch)
        result = await t["pipeline_deploy"](
            action="deploy_dags",
            remote_host="host",
            remote_user="user",
            remote_dags_dir="/opt/airflow/dags",
            ssh_key_path=str(key_file),
            dry_run=True,
            validate_imports=True,
        )
        assert result["success"] is False
        assert any("asdfsdf" in err for err in result.get("errors", []))


# ═══════════════════════════════════════════════════════════════════════════
#  4. airflow_connections
# ═══════════════════════════════════════════════════════════════════════════

class TestAirflowConnectionsNullGuard:
    @pytest.mark.asyncio
    async def test_none_action(self, tools):
        result = await tools["airflow_connections"](action=None)
        assert result["success"] is False
        assert "action" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_empty_string_action(self, tools):
        result = await tools["airflow_connections"](action="")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_whitespace_only_action(self, tools):
        result = await tools["airflow_connections"](action="   ")
        assert result["success"] is False


class TestAirflowConnectionsInvalidAction:
    @pytest.mark.asyncio
    async def test_unknown_action(self, tools):
        result = await tools["airflow_connections"](action="delete")
        assert result["success"] is False
        assert "Unknown action" in result["error"]
        assert "delete" in result["error"]


class TestAirflowConnectionsList:
    @pytest.mark.asyncio
    async def test_success(self, orch, tools):
        orch.async_airflow_client.list_connections = AsyncMock(return_value=[
            {"connection_id": "td_default", "conn_type": "teradata"},
            {"connection_id": "ab_default", "conn_type": "airbyte"},
        ])
        result = await tools["airflow_connections"](action="list")
        assert result["success"] is True
        assert result["total_count"] == 2
        assert len(result["connections"]) == 2

    @pytest.mark.asyncio
    async def test_list_with_prefix_filter(self, orch, tools):
        orch.async_airflow_client.list_connections = AsyncMock(return_value=[
            {"connection_id": "td_default", "conn_type": "teradata"},
            {"connection_id": "ab_default", "conn_type": "airbyte"},
        ])
        result = await tools["airflow_connections"](action="list", conn_id_prefix="td")
        assert result["success"] is True
        assert result["total_count"] == 1
        assert result["connections"][0]["connection_id"] == "td_default"

    @pytest.mark.asyncio
    async def test_list_with_conn_type_filter(self, orch, tools):
        orch.async_airflow_client.list_connections = AsyncMock(return_value=[
            {"connection_id": "td_default", "conn_type": "teradata"},
            {"connection_id": "ab_default", "conn_type": "airbyte"},
        ])
        result = await tools["airflow_connections"](action="list", conn_type="airbyte")
        assert result["success"] is True
        assert result["total_count"] == 1

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.async_airflow_client.list_connections = AsyncMock(
            side_effect=RuntimeError("connection refused")
        )
        result = await tools["airflow_connections"](action="list")
        assert result["success"] is False


class TestAirflowConnectionsCreateTeradata:
    @pytest.mark.asyncio
    async def test_success_new_connection(self, orch, tools):
        # get_connection raises 404 -> proceed to create
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "teradata_default",
            "conn_type": "teradata",
            "host": "td-host",
        })
        result = await tools["airflow_connections"](action="create_teradata")
        assert result["success"] is True
        assert result["created"] is True
        assert result["connection_id"] == "teradata_default"
        # Regression: must not use connection_id= keyword (IDE-25607)
        call = orch.async_airflow_client.create_connection.call_args
        assert "connection_id" not in call.kwargs, "create_connection must not use connection_id= keyword"
        assert "conn_id" in call.kwargs or len(call.args) >= 1, "conn_id must be passed positionally or as conn_id="

    @pytest.mark.asyncio
    async def test_success_reuse_existing(self, orch, tools):
        """If connection already exists with matching config, reuse it."""
        orch.async_airflow_client.get_connection = AsyncMock(return_value={
            "connection_id": "teradata_default",
            "host": "td-host",
            "schema": "test_db",
            "login": "dbc",
            "port": 1025,
        })
        result = await tools["airflow_connections"](action="create_teradata")
        assert result["success"] is True
        assert result["reused"] is True

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            side_effect=RuntimeError("Airflow down")
        )
        result = await tools["airflow_connections"](action="create_teradata")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_custom_connection_id(self, orch, tools):
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "my_td",
            "conn_type": "teradata",
        })
        result = await tools["airflow_connections"](
            action="create_teradata", connection_id="my_td"
        )
        assert result["success"] is True
        assert result["connection_id"] == "my_td"


class TestAirflowConnectionsCreateAirbyte:
    @pytest.mark.asyncio
    async def test_success_new_connection(self, orch, tools):
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })
        result = await tools["airflow_connections"](action="create_airbyte")
        assert result["success"] is True
        assert result["created"] is True
        assert result["connection_id"] == "airbyte_default"
        # Regression: must not use connection_id= keyword (IDE-25607)
        call = orch.async_airflow_client.create_connection.call_args
        assert "connection_id" not in call.kwargs, "create_connection must not use connection_id= keyword"
        assert "conn_id" in call.kwargs or len(call.args) >= 1, "conn_id must be passed positionally or as conn_id="

    @pytest.mark.asyncio
    async def test_success_reuse_existing(self, orch, tools):
        orch.async_airflow_client.get_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "host": "http://localhost:8000/api/public/v1/",
            "login": "test-client-id",
            "schema": "http://localhost:8000/token",
        })
        result = await tools["airflow_connections"](action="create_airbyte")
        assert result["success"] is True
        assert result["reused"] is True

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            side_effect=RuntimeError("failed")
        )
        result = await tools["airflow_connections"](action="create_airbyte")
        assert result["success"] is False


class TestAirflowConnectionsCreateSSH:
    _SSH_ENV = {
        "MCP_CLIENT_SSH_HOST": "localhost",
        "MCP_CLIENT_SSH_USER": "airflow",
        "MCP_CLIENT_SSH_PASSWORD": "ssh-pass",
    }

    @pytest.mark.asyncio
    async def test_success_new_connection(self, orch, tools):
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "ssh_localhost",
            "conn_type": "ssh",
            "host": "localhost",
        })
        orch.async_airflow_client.test_airflow_connection = AsyncMock(return_value={
            "status": "success",
            "message": "Connection test passed",
        })
        with patch.dict(os.environ, self._SSH_ENV):
            result = await tools["airflow_connections"](action="create_ssh")
        assert result["success"] is True
        assert result["created"] is True
        assert result["connection_id"] == "ssh_localhost"
        assert result["test_status"] == "success"
        # Regression: must not use connection_id= keyword (IDE-25607)
        call = orch.async_airflow_client.create_connection.call_args
        assert "connection_id" not in call.kwargs, "create_connection must not use connection_id= keyword"
        assert "conn_id" in call.kwargs or len(call.args) >= 1, "conn_id must be passed positionally or as conn_id="
        test_kwargs = orch.async_airflow_client.test_airflow_connection.call_args.kwargs
        assert "connection_payload" in test_kwargs, "test_airflow_connection must be called with connection_payload="

    @pytest.mark.asyncio
    async def test_success_reuse_existing(self, orch, tools):
        orch.async_airflow_client.get_connection = AsyncMock(return_value={
            "connection_id": "ssh_localhost",
            "conn_type": "ssh",
            "host": "localhost",
            "login": "airflow",
            "port": 22,
            "extra": {"key_file": ""},
        })
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MCP_CLIENT_SSH_HOST", None)
            os.environ.pop("MCP_CLIENT_SSH_USER", None)
            os.environ.pop("MCP_CLIENT_SSH_PORT", None)
            os.environ.pop("MCP_CLIENT_SSH_KEY_PATH", None)
            os.environ.pop("MCP_CLIENT_SSH_PASSWORD", None)
            result = await tools["airflow_connections"](action="create_ssh")
        assert result["success"] is True
        assert result["reused"] is True

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch, tools):
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            side_effect=RuntimeError("SSH fail")
        )
        with patch.dict(os.environ, self._SSH_ENV):
            result = await tools["airflow_connections"](action="create_ssh")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_test_connection_failure_still_succeeds(self, orch, tools):
        """Connection creation succeeds but test fails -- still success=True with warning."""
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "ssh_localhost",
            "conn_type": "ssh",
        })
        orch.async_airflow_client.test_airflow_connection = AsyncMock(return_value={
            "status": "failed",
            "message": "Connection refused",
        })
        with patch.dict(os.environ, self._SSH_ENV):
            result = await tools["airflow_connections"](action="create_ssh")
        assert result["success"] is True
        assert result["test_status"] == "failed"
        assert "warning" in result
        test_kwargs = orch.async_airflow_client.test_airflow_connection.call_args.kwargs
        assert "connection_payload" in test_kwargs, "test_airflow_connection must be called with connection_payload="

    @pytest.mark.asyncio
    async def test_test_connection_exception_still_succeeds(self, orch, tools):
        """If test_airflow_connection throws, creation still succeeds with test_status=error."""
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "ssh_localhost",
            "conn_type": "ssh",
        })
        orch.async_airflow_client.test_airflow_connection = AsyncMock(
            side_effect=RuntimeError("test boom")
        )
        with patch.dict(os.environ, self._SSH_ENV):
            result = await tools["airflow_connections"](action="create_ssh")
        assert result["success"] is True
        assert result["test_status"] == "error"
        test_kwargs = orch.async_airflow_client.test_airflow_connection.call_args.kwargs
        assert "connection_payload" in test_kwargs, "test_airflow_connection must be called with connection_payload="


# ═══════════════════════════════════════════════════════════════════════════
#  5. Cross-cutting: action case-insensitivity
# ═══════════════════════════════════════════════════════════════════════════

class TestActionCaseInsensitivity:
    @pytest.mark.asyncio
    async def test_pipeline_status_uppercase(self, orch, tools):
        orch.async_airflow_client.list_dags = AsyncMock(return_value=[])
        result = await tools["pipeline_status"](action="LIST_PIPELINES")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_pipeline_control_mixed_case(self, orch, tools):
        orch.async_airflow_client.pause_dag = AsyncMock(return_value=None)
        result = await tools["pipeline_control"](action="Pause", pipeline_name="dag1")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_airflow_connections_upper(self, orch, tools):
        orch.async_airflow_client.list_connections = AsyncMock(return_value=[])
        result = await tools["airflow_connections"](action="LIST")
        assert result["success"] is True


# ═══════════════════════════════════════════════════════════════════════════
#  6. Registration returns expected tools
# ═══════════════════════════════════════════════════════════════════════════

class TestRegistration:
    def test_register_returns_all_five_tools(self, tools):
        expected = {
            "pipeline_status",
            "pipeline_control",
            "pipeline_deploy",
            "pipeline_validate",
            "airflow_connections",
        }
        assert expected == set(tools.keys())

    def test_all_tools_are_callable(self, tools):
        for name, fn in tools.items():
            assert callable(fn), f"Tool '{name}' is not callable"


# ═══════════════════════════════════════════════════════════════════════════
# pipeline_validate tests (merged from test_validate_pipeline_configuration_tool.py)
# ═══════════════════════════════════════════════════════════════════════════


class _ValidateMockOrchestrator:
    """Mock orchestrator for pipeline_validate tests."""

    def __init__(self, validation_result: dict[str, Any]):
        self._validation_result = validation_result
        # Optional client mocks retained for compatibility
        self.teradata_client = object()
        self.airflow_client = object()
        self.airbyte_client = object()
        self.dbt_client = object()

        # Mocks required by the DAG syntax check in _validate_pipeline_configuration.
        # Create a real empty temp dir via TemporaryDirectory, then reference a
        # subdirectory of it that was never created. A freshly-made empty directory
        # has no children, so _nonexistent is guaranteed not to exist without relying
        # on UUIDs or assertions. Storing the TemporaryDirectory on self ensures it
        # is cleaned up when the orchestrator instance is discarded.
        self._tmpdir = tempfile.TemporaryDirectory()
        _nonexistent = Path(self._tmpdir.name) / "dags"
        _pipeline_ns = types.SimpleNamespace(dags_output_dir=str(_nonexistent))
        self.settings = types.SimpleNamespace(pipeline=_pipeline_ns)
        self.airflow_dag_generator = object()  # never called when dag_file does not exist

    async def async_validate_pipeline_configuration(
        self, pipeline_config: dict[str, Any]
    ) -> dict[str, Any]:
        # Allow tests to override checks/valid via provided payload
        return self._validation_result


class TestPipelineValidate:
    """Tests for the pipeline_validate tool."""

    @pytest.mark.asyncio
    async def test_generic_connectivity_success(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={
                "valid": True,
                "checks": {
                    "teradata": "OK",
                    "airflow": "OK",
                    "airbyte": "OK",
                    "tpt": "OK",
                    "dbt": "SKIPPED: dbt not required",
                },
                "errors": [],
                "warnings": [],
            }
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {"pipeline_name": "connectivity_check", "require_dbt": False}
        result = await tools["pipeline_validate"](payload)
        assert result["valid"] is True
        assert "connections" in result["checks"]
        assert result["checks"]["connections"]["teradata"] == "OK"
        assert any("Unknown or unspecified source_type" in w for w in result["warnings"])

    @pytest.mark.asyncio
    async def test_missing_pipeline_name_is_error(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        result = await tools["pipeline_validate"]({})
        assert result["valid"] is False
        assert any("Missing required field: pipeline_name" == err for err in result["errors"])

    @pytest.mark.asyncio
    async def test_whitespace_only_pipeline_name_is_error(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        result = await tools["pipeline_validate"]({"pipeline_name": "   "})
        assert result["valid"] is False
        assert any("pipeline_name cannot be blank" == err for err in result["errors"])

    @pytest.mark.asyncio
    async def test_non_string_pipeline_name_is_error(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        result = await tools["pipeline_validate"]({"pipeline_name": 42})
        assert result["valid"] is False
        assert any("pipeline_name must be a string, got int" == err for err in result["errors"])

    @pytest.mark.asyncio
    async def test_airbyte_source_ids_and_unavailable_warning(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={
                "valid": True,
                "checks": {"airbyte": "UNAVAILABLE: service down"},
                "errors": [],
                "warnings": [],
            }
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {
            "pipeline_name": "airbyte_check",
            "source_type": "airbyte",
            "connection_id": "abcd-1234",
            "require_dbt": False,
        }
        result = await tools["pipeline_validate"](payload)
        assert result["valid"] is True
        assert result["checks"]["airbyte_connection_id"] == "provided"
        assert any("Airbyte service unavailable" in w for w in result["warnings"])

    @pytest.mark.asyncio
    async def test_csv_like_source_missing_input_file_path_error_propagated(self):
        """For csv-like sources, missing input_file_path is caught by the orchestrator and surfaces in the tool result."""
        orchestrator = _ValidateMockOrchestrator(
            validation_result={
                "valid": False,
                "checks": {"input_file": "FAILED: input_file_path not provided"},
                "errors": ["TPT: input file path missing; provide 'input_file_path'"],
                "warnings": [],
            }
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {
            "pipeline_name": "csv_check",
            "source_type": "csv",
            # input_file_path intentionally omitted — orchestrator reports the error
            "require_dbt": False,
        }
        result = await tools["pipeline_validate"](payload)
        assert result["valid"] is False
        assert any("input_file_path" in e for e in result["errors"])
        assert result["checks"]["connections"]["input_file"] == "FAILED: input_file_path not provided"

    @pytest.mark.asyncio
    async def test_deprecated_files_field_returns_error(self):
        """Using the deprecated 'files' list field must fail with a clear migration error."""
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {
            "pipeline_name": "old_style",
            "source_type": "csv",
            "files": ["/tmp/input.csv"],
        }
        result = await tools["pipeline_validate"](payload)
        assert result["valid"] is False
        assert any("files" in e and "input_file_path" in e for e in result["errors"])

    @pytest.mark.asyncio
    async def test_schedule_presence_not_validated(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {"pipeline_name": "sched_check", "schedule": "@daily"}
        result = await tools["pipeline_validate"](payload)
        assert result["valid"] is True
        assert result["checks"]["schedule_format"] == "NOT_VALIDATED"
        assert any("Schedule format validation not implemented" in w for w in result["warnings"])

    @pytest.mark.asyncio
    async def test_target_schema_presence_check(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {"pipeline_name": "schema_check", "target_schema": "analytics"}
        result = await tools["pipeline_validate"](payload)
        assert result["valid"] is True
        assert result["checks"]["target_schema"] == "provided"

    @pytest.mark.asyncio
    async def test_source_type_inference_input_file_path(self):
        """input_file_path presence infers source_type='file', matching the orchestrator."""
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {"pipeline_name": "infer_files", "input_file_path": "/tmp/input.csv"}
        result = await tools["pipeline_validate"](payload)
        assert result["checks"]["source_type"] == "file"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("empty_value", [None, ""])
    async def test_source_type_inference_empty_input_file_path_not_classified_as_file(
        self, empty_value
    ):
        """Empty/None input_file_path must not infer source_type='file' (mirrors orchestrator)."""
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {"pipeline_name": "empty_path", "input_file_path": empty_value}
        result = await tools["pipeline_validate"](payload)
        assert result["checks"]["source_type"] != "file"

    @pytest.mark.asyncio
    async def test_source_type_inference_airbyte(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {"pipeline_name": "infer_airbyte", "connection_id": "abcd"}
        result = await tools["pipeline_validate"](payload)
        assert result["checks"]["source_type"] == "airbyte"

    @pytest.mark.asyncio
    async def test_connections_invalid_propagates_errors(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={
                "valid": False,
                "checks": {"airflow": "FAILED"},
                "errors": ["Airflow: connection failed"],
                "warnings": [],
            }
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {"pipeline_name": "bad_conn"}
        result = await tools["pipeline_validate"](payload)
        assert result["valid"] is False
        assert any("Airflow: connection failed" in e for e in result["errors"])

    @pytest.mark.asyncio
    async def test_unknown_source_type_emits_warning(self):
        orchestrator = _ValidateMockOrchestrator(
            validation_result={"valid": True, "checks": {}, "errors": [], "warnings": []}
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {"pipeline_name": "unknown_source"}
        result = await tools["pipeline_validate"](payload)
        assert any("Unknown or unspecified source_type" in w for w in result["warnings"])

    @pytest.mark.asyncio
    async def test_teradata_tables_no_longer_validates_metadata(self):
        # Even if teradata_tables is provided, the tool should not attempt metadata checks
        orchestrator = _ValidateMockOrchestrator(
            validation_result={
                "valid": True,
                "checks": {
                    "teradata": "OK",
                    "airflow": "OK",
                    "airbyte": "SKIPPED: not required for Teradata tables",
                    "tpt": "SKIPPED: not required for Teradata tables",
                    "dbt": "SKIPPED: dbt not required",
                },
                "errors": [],
                "warnings": [],
            }
        )
        tools = register_pipeline_tools(orchestrator)
        payload = {
            "pipeline_name": "teradata_tables_check",
            "source_type": "teradata_tables",
            "source_database": "SalesDB",
            "source_tables": ["Orders", "Customers"],
            "validate_source_tables": True,
            "require_dbt": False,
        }
        result = await tools["pipeline_validate"](payload)
        assert result["valid"] is True
        # No table_* keys should be present in checks anymore
        assert not any(k.startswith("table_") for k in result["checks"].keys())


# ═══════════════════════════════════════════════════════════════════════════
#  pipeline_deploy — create_dbt_dag action
# ═══════════════════════════════════════════════════════════════════════════


class TestPipelineDeployCreateDbtDag:
    @pytest.mark.asyncio
    async def test_missing_dag_id(self, tools):
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag", project_name="default"
        )
        assert result["success"] is False
        assert "dag_id" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_missing_project_name_returns_action_required(self, orch):
        """When ``project_name`` isn't supplied, the helper returns
        ``ask_project_name`` immediately. ``project_name`` is the only
        locator for the dbt sub-project — there's no implicit default."""
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag", dag_id="my_dbt_dag"
        )
        assert result["success"] is False
        assert result.get("action_required") == "ask_project_name"

    @pytest.mark.asyncio
    async def test_success(self, orch, tmp_path):
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        dag_file = tmp_path / "my_dbt_output.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="daily_dbt",
            project_name="default",
            dbt_models=["stg_orders"],
            dbt_target="dev",
            owner="my_team",
            tags=["dbt", "nightly"],
            output_filename="my_dbt_output.py",
        )
        assert result["success"] is True
        assert result["dag_id"] == "daily_dbt"
        sub = orch.dbt_project_parent / "dbt_default"
        assert result["dbt_project_dir"] == str(sub)
        assert result["teradata_identity"] == "wizard:td_host"
        assert result["dbt_models"] == ["stg_orders"]
        assert result["dbt_target"] == "dev"

        orch.airflow_dag_generator.generate_dbt_only_dag.assert_called_once()
        call_kwargs = orch.airflow_dag_generator.generate_dbt_only_dag.call_args[1]
        assert call_kwargs["project_dir"] == str(sub)
        assert call_kwargs["models"] == ["stg_orders"]
        assert call_kwargs["target"] == "dev"
        assert call_kwargs["owner"] == "my_team"
        assert call_kwargs["tags"] == ["dbt", "nightly"]
        assert call_kwargs["output_filename"] == "my_dbt_output.py"

        validate_path = orch.airflow_dag_generator.validate_dag_file.call_args[0][0]
        assert str(validate_path).endswith("my_dbt_output.py")

    @pytest.mark.asyncio
    async def test_error_propagation(self, orch):
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(
            side_effect=RuntimeError("template error")
        )

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="fail_dag",
            project_name="default",
        )
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_returns_scaffold_first_when_subproject_missing(self, orch):
        """``project_name`` for a sub-project that doesn't exist on disk →
        the helper refuses to bake an unscaffolded path into a DAG and
        tells the LLM to scaffold first. No ``teradata_identity`` is
        returned because we have no ``dbt_project.yml`` to read."""
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="my_dbt_dag",
            project_name="not_yet_scaffolded",
        )
        assert result["success"] is False
        assert result["action_required"] == "scaffold_subproject_first"
        # No identity binding to read; field is omitted (or empty) at this point.
        assert not result.get("teradata_identity")

    @pytest.mark.asyncio
    async def test_legacy_layout_returns_error_at_dag_creation(self, orch):
        """Pre-multi-project layout (``dbt_project.yml`` at the parent
        root, no sub-project dirs) — DAG creation refuses with the
        legacy-layout migration error directly. Without this check the
        helper would fall through to ``scaffold_subproject_first``, but
        ``dbt_project(action='create_structure')`` also rejects the
        legacy layout, so the LLM would dead-end. Mirrors the same
        check in ``_resolve_dbt_subproject``."""
        # Write a legacy single-project dbt_project.yml at the parent root.
        (orch.dbt_project_parent / "dbt_project.yml").write_text(
            "name: legacy\nprofile: legacy\n", encoding="utf-8"
        )
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="my_dbt_dag",
            project_name="default",
        )
        assert result["success"] is False
        assert "legacy single-project dbt layout" in result["error"]

    @pytest.mark.asyncio
    async def test_project_name_collides_with_parent_dir_returns_rename_project(
        self, orch
    ):
        """``project_name='project'`` would produce sub-project dir
        ``dbt_project/`` inside parent container ``dbt_project/``,
        which silently nests. Same rejection that
        ``dbt_project(action='create_structure')`` returns — surface
        it here too so following the scaffold hint can't dead-end."""
        # The fixture's parent is named ``dbt_project/``.
        assert orch.dbt_project_parent.name == "dbt_project"
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="my_dbt_dag",
            project_name="project",
        )
        assert result["success"] is False
        assert result["action_required"] == "rename_project"
        assert result["rejected_project_name"] == "project"
        assert result["collision_with"] == "dbt_project"
        assert isinstance(result["suggested_project_names"], list)
        assert len(result["suggested_project_names"]) >= 1

    @pytest.mark.asyncio
    async def test_project_name_with_dbt_prefix_collides_with_parent_dir(
        self, orch
    ):
        """``project_name='dbt_project'`` slugifies to ``dbt_project``,
        the leading-``dbt_`` strip yields ``project``, and
        ``dbt_project`` (the eventual sub-project basename) equals the
        parent container's name. Caught by the same collision check as
        ``project_name='project'``."""
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="my_dbt_dag",
            project_name="dbt_project",
        )
        assert result["success"] is False
        assert result["action_required"] == "rename_project"
        assert result["rejected_project_name"] == "dbt_project"
        assert result["collision_with"] == "dbt_project"

    @pytest.mark.asyncio
    async def test_subproject_with_unreadable_profile_returns_fix_binding(
        self, orch
    ):
        """When the sub-project exists but its ``dbt_project.yml`` has no
        readable ``profile:`` field, ``_locate_dbt_subproject_dir`` fails
        closed with ``action_required: fix_subproject_binding``.

        Without this guard, the response's ``teradata_identity`` would be
        empty, the refresh_env hint would carry a ``<your_profile>``
        placeholder, and the dbt task at runtime would fail anyway (the
        scaffolded ``.env`` is missing/broken alongside the binding).
        Failing closed at DAG-creation time tells the LLM to repair the
        sub-project before baking a DAG against it."""
        # Overwrite the fixture's healthy dbt_project.yml with one that
        # has no ``profile:`` field. _read_project_profile returns None.
        sub = orch.dbt_project_parent / "dbt_default"
        (sub / "dbt_project.yml").write_text("name: 'default'\n", encoding="utf-8")
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="broken_binding_dag",
            project_name="default",
        )
        assert result["success"] is False
        assert result["action_required"] == "fix_subproject_binding"
        assert result["project_name"] == "default"
        assert "no readable ``profile:`` field" in result["message"]
        assert "create_structure" in result["message"]

    @pytest.mark.asyncio
    async def test_fix_subproject_binding_call_snippet_uses_slug_not_raw_name(
        self, orch
    ):
        """The ``fix_subproject_binding`` response embeds a
        ``dbt_project(action='create_structure', project_name='...', ...)``
        call snippet inside Markdown backticks. ``project_name`` is
        user input, so a value containing apostrophes / backticks /
        control chars would break the rendered Python syntax or
        terminate the inline-code span — same class of bug as the
        ``_refresh_env_call_hint`` identity escape, just on a
        different code path. Defense: interpolate the SLUG
        (alphanumeric+``_``, always safe) instead of the raw value.
        Slug is also the canonical form ``create_structure`` would
        normalize to anyway."""
        # Pre-create a sub-project at the slug name with no readable
        # ``profile:`` field, so resolution reaches the
        # fix_subproject_binding branch. Caller's raw ``project_name``
        # is ``\"O'Reilly`Workshop\"`` — full slug+strip pipeline:
        # lowercase → ``o'reilly`workshop`` → non-alnum→``_`` →
        # ``o_reilly_workshop`` → no ``dbt_`` prefix to strip → final
        # slug ``o_reilly_workshop``. The on-disk dir must match.
        parent = orch.dbt_project_parent
        sub = parent / "dbt_o_reilly_workshop"
        sub.mkdir()
        (sub / "dbt_project.yml").write_text("name: 'broken'\n", encoding="utf-8")
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="snippet_safety_dag",
            project_name="O'Reilly`Workshop",  # apostrophe + backtick
        )
        assert result["success"] is False
        assert result["action_required"] == "fix_subproject_binding"
        # Call snippet must show the slug, not the raw caller input.
        assert "project_name='o_reilly_workshop'" in result["message"]
        # Raw value with the apostrophe MUST NOT appear inside the
        # Markdown-rendered call snippet — would break Python syntax
        # AND terminate the inline-code span via the backtick.
        assert "O'Reilly" not in result["message"]
        assert "`Workshop" not in result["message"]
        # The structured ``project_name`` field still carries the
        # caller's original value (informational, not a snippet).
        assert result["project_name"] == "O'Reilly`Workshop"

    @pytest.mark.asyncio
    async def test_subproject_dir_exists_but_yml_missing_returns_repair(
        self, orch
    ):
        """Partial-state recovery: when the sub-project DIR exists but
        its ``dbt_project.yml`` is missing (interrupted scaffold,
        manual delete, etc.), the helper returns
        ``action_required: repair_subproject`` — distinct from
        ``scaffold_subproject_first`` (which assumes the dir doesn't
        exist yet). Same recovery family but different message: the
        partial-state case mentions the dir-exists fact and offers
        both re-scaffold and delete-then-rescaffold paths."""
        sub = orch.dbt_project_parent / "dbt_default"
        # Sub-project dir exists (fixture pre-created it) but the yml
        # is gone.
        assert sub.exists(), "fixture should pre-create dbt_default/"
        (sub / "dbt_project.yml").unlink()
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="partial_state_dag",
            project_name="default",
        )
        assert result["success"] is False
        assert result["action_required"] == "repair_subproject"
        assert result["project_name"] == "default"
        # Message must distinguish from the scaffold-from-scratch case.
        assert "dbt_default" in result["message"]
        assert "is missing" in result["message"]
        # Both recovery paths should be mentioned.
        assert "create_structure" in result["message"]
        assert "delete the directory" in result["message"]

    @pytest.mark.asyncio
    async def test_refresh_env_hint_escapes_apostrophe_in_identity(
        self, orch, tmp_path
    ):
        """``_refresh_env_call_hint`` interpolates the binding identity
        into a Python call snippet inside backticks. Apostrophes in
        the identity (pathological YAML content) would break Python
        syntax if passed through naively. Defensive escape via
        ``repr()`` produces a valid Python string literal — it picks
        double quotes when the value contains a single quote, which
        is fine inside the surrounding ``action='refresh_env', ...``
        call (Python allows mixed quote styles in a single call).
        Reproduce by hand-editing ``dbt_project.yml`` to a profile
        name with an apostrophe; assert the rendered hint shows
        ``teradata_profile=\"o'reilly\"`` (double-quoted)."""
        sub = orch.dbt_project_parent / "dbt_default"
        # Hand-craft a dbt_project.yml with a profile name containing
        # an apostrophe. (Real scaffolding wouldn't produce this, but
        # YAML's permissive enough to allow it via manual edit.)
        (sub / "dbt_project.yml").write_text(
            'name: "default"\nprofile: "o\'reilly"\n', encoding="utf-8"
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "escape_test.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="escape_test",
            project_name="default",
            output_filename="escape_test.py",
        )
        assert result["success"] is True, result
        assert result["teradata_identity"] == "o'reilly"
        # NOTE: filter on a step-5-specific phrase, not bare
        # ``refresh_env``. ``tmp_path`` includes the pytest test name
        # in its path, which interpolates into step 1 — so a loose
        # ``"refresh_env" in s`` match would return step 1 here.
        step5 = next(s for s in result["next_steps"] if "After credential rotation" in s)
        # ``repr()`` switches to double quotes when the value has a
        # single quote — produces ``"o'reilly"`` (valid Python).
        assert 'teradata_profile="o\'reilly"' in step5
        # Raw apostrophe in identity must NOT appear inside a
        # single-quoted Python string in the call snippet (that's the
        # broken syntactic form the escape exists to prevent).
        assert "teradata_profile='o'reilly'" not in step5

    @pytest.mark.asyncio
    async def test_refresh_env_hint_handles_control_chars_in_identity(
        self, orch, tmp_path
    ):
        """YAML allows newlines/tabs/etc. in string values via quoting
        and block scalars. ``_refresh_env_call_hint`` must produce a
        valid Python literal for any content — ``repr()`` handles
        every control char (\\n, \\r, \\t, etc.) automatically."""
        sub = orch.dbt_project_parent / "dbt_default"
        # Quoted YAML string with embedded \\n and \\t escape sequences.
        (sub / "dbt_project.yml").write_text(
            'name: "default"\nprofile: "line1\\nline2\\t!"\n', encoding="utf-8"
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "ctrl_test.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="ctrl_test",
            project_name="default",
            output_filename="ctrl_test.py",
        )
        assert result["success"] is True, result
        assert result["teradata_identity"] == "line1\nline2\t!"
        step5 = next(s for s in result["next_steps"] if "After credential rotation" in s)
        # Newline and tab must be displayed as escape sequences, not
        # raw control chars (which would split the snippet across
        # multiple lines / bake hidden whitespace).
        assert "teradata_profile='line1\\nline2\\t!'" in step5
        # Raw newline must not appear inside the call snippet —
        # would break the Markdown inline-code rendering.
        assert "line1\nline2" not in step5

    @pytest.mark.asyncio
    async def test_refresh_env_hint_named_profile_with_wizard_prefix_not_misclassified(
        self, orch, tmp_path
    ):
        """A connections.yaml profile literally named ``wizard:prod``
        is legal — only the bare ``wizard``/``default`` names are
        reserved as wizard sentinels. ``_refresh_env_call_hint`` MUST
        compare against the exact computed current synthetic sentinel
        (``wizard:<slug(settings.teradata.host)>``), not the
        ``wizard:`` prefix; otherwise a named profile with a colon-
        suffix matching ``wizard:`` would be misclassified as the
        synthetic sentinel and the hint would tell the user to OMIT
        ``teradata_profile``, silently refreshing wizard-default creds
        instead of the named profile.

        Setup: settings.teradata.host = ``td_host`` (live sentinel =
        ``wizard:td_host``). Sub-project bound to ``wizard:prod`` (a
        DIFFERENT value — a user-defined profile name, NOT the live
        sentinel). The hint MUST take the named-profile branch."""
        sub = orch.dbt_project_parent / "dbt_named_with_prefix"
        sub.mkdir()
        (sub / "dbt_project.yml").write_text(
            "name: 'named_with_prefix'\nprofile: 'wizard:prod'\n",
            encoding="utf-8",
        )
        # Live wizard sentinel computed from the fixture's host
        # (``td-host`` → slugified to ``td_host``) is ``wizard:td_host``,
        # which is NOT what this sub-project is bound to.
        assert orch.settings.teradata.host == "td-host"
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "wizard_prefix_test.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="wizard_prefix_test",
            project_name="named_with_prefix",
            output_filename="wizard_prefix_test.py",
        )
        assert result["success"] is True, result
        assert result["teradata_identity"] == "wizard:prod"
        step5 = next(s for s in result["next_steps"] if "After credential rotation" in s)
        # Named-profile branch: hint passes the binding name as
        # ``teradata_profile`` (single-quoted by repr() since the
        # value has no single-quote chars).
        assert "teradata_profile='wizard:prod'" in step5
        # Omit-form why_extra MUST NOT appear — that would indicate
        # the hint mistakenly took the wizard-sentinel branch.
        assert "Omit ``teradata_profile``" not in step5

    @pytest.mark.asyncio
    async def test_refresh_env_hint_strips_backticks_from_identity(
        self, orch, tmp_path
    ):
        """Backticks in the identity would terminate the surrounding
        Markdown inline-code span. ``repr()`` doesn't escape backticks
        for Python (they're not special there), so the helper strips
        them defensively before passing to repr()."""
        sub = orch.dbt_project_parent / "dbt_default"
        (sub / "dbt_project.yml").write_text(
            'name: "default"\nprofile: "tick`name"\n', encoding="utf-8"
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "tick_test.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="tick_test",
            project_name="default",
            output_filename="tick_test.py",
        )
        assert result["success"] is True, result
        # The on-disk binding still has the backtick (we don't
        # rewrite the file), but the rendered hint must not.
        assert result["teradata_identity"] == "tick`name"
        step5 = next(s for s in result["next_steps"] if "After credential rotation" in s)
        # Backtick stripped from the displayed value.
        assert "teradata_profile='tickname'" in step5
        assert "`" not in step5.split("teradata_profile=")[1].split(")")[0]

    @pytest.mark.asyncio
    async def test_refresh_env_hint_slugifies_project_name(
        self, orch, tmp_path
    ):
        """``project_name`` interpolated into the refresh_env hint is
        normalized via ``slugify_dir_name`` to mirror the resolver's
        normalization. This makes the hint reference the canonical
        on-disk name AND prevents single-quote / non-alphanumeric
        injection from crafted user input. Caller passes
        ``project_name='Default Workspace'``; the hint must show
        ``project_name='default_workspace'``."""
        # Rename the fixture's sub-project to dbt_default_workspace/
        # so resolution succeeds for the slugified form.
        sub = orch.dbt_project_parent / "dbt_default_workspace"
        sub.mkdir()
        (sub / "dbt_project.yml").write_text(
            "name: 'default_workspace'\nprofile: 'wizard:td_host'\n",
            encoding="utf-8",
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "slug_test.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="slug_test",
            project_name="Default Workspace",  # space + caps → slug
            output_filename="slug_test.py",
        )
        assert result["success"] is True, result
        # See note in the apostrophe-escape test re: matching step 5
        # via "After credential rotation" rather than "refresh_env".
        step5 = next(s for s in result["next_steps"] if "After credential rotation" in s)
        # Hint shows the canonical slug, not the raw caller-supplied form.
        assert "project_name='default_workspace'" in step5
        assert "project_name='Default Workspace'" not in step5

    @pytest.mark.asyncio
    async def test_project_name_with_dbt_prefix_resolves_to_same_subproject(
        self, orch, tmp_path
    ):
        """``project_name='dbt_default'`` and ``project_name='default'``
        both resolve to ``dbt_default/`` — mirrors the dedup logic in
        ``_resolve_dbt_subproject`` (``dbt_management.py``). Without this
        a user who scaffolded with ``'default'`` and then called DAG
        creation with the on-disk form ``'dbt_default'`` would get a
        spurious ``scaffold_subproject_first``."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "dedup_test.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        # Pre-made fixture has dbt_default/. Pass 'dbt_default' (the
        # on-disk form) and assert it resolves correctly.
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="dedup_test",
            project_name="dbt_default",  # leading dbt_ should be stripped
            output_filename="dedup_test.py",
        )
        assert result["success"] is True, result
        sub = orch.dbt_project_parent / "dbt_default"
        assert result["dbt_project_dir"] == str(sub)
        assert result["teradata_identity"] == "wizard:td_host"

    @pytest.mark.asyncio
    async def test_create_dbt_dag_ignores_teradata_profile_param(
        self, orch, tmp_path
    ):
        """The dbt-DAG path no longer needs ``teradata_profile``; it is
        accepted on the router for shape consistency but ignored. The
        response's ``teradata_identity`` reflects the sub-project's
        on-disk binding (read from ``dbt_project.yml::profile``), NOT
        whatever profile name the caller passed."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "ignore_profile.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        # Pass a profile name that does NOT match the sub-project's binding.
        # The sub-project ``dbt_default/`` is bound to ``wizard:td_host``;
        # we pass ``some_other_profile`` and expect the response to still
        # report ``teradata_identity == 'wizard:td_host'``.
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="ignore_profile_dag",
            project_name="default",
            teradata_profile="some_other_profile",
            output_filename="ignore_profile.py",
        )
        assert result["success"] is True, result
        assert result["teradata_identity"] == "wizard:td_host"

    @pytest.mark.asyncio
    async def test_next_steps_refresh_hint_omits_profile_for_wizard_binding(
        self, orch, tmp_path
    ):
        """When the sub-project is bound to the wizard sentinel form
        (``wizard:<host_slug>``), the refresh_env hint in next_steps
        must OMIT ``teradata_profile`` — the colon-suffix form would be
        treated as a named profile by ``resolve_teradata_auth`` and
        fail. Only the literal ``"wizard"``/``"default"`` sentinels fold
        to wizard-default."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "wizard_refresh.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="wizard_refresh_dag",
            project_name="default",  # binding: wizard:td_host (per fixture)
            output_filename="wizard_refresh.py",
        )
        assert result["success"] is True, result
        assert result["teradata_identity"] == "wizard:td_host"
        # Find the step-5 refresh_env hint in next_steps.
        step5 = next(s for s in result["next_steps"] if "refresh_env" in s)
        # Critical: the call form must NOT pass the synthetic identity
        # as ``teradata_profile``. ``'wizard:td_host'`` should not appear
        # in the call snippet at all.
        assert "teradata_profile='wizard:td_host'" not in step5
        assert "teradata_profile='wizard'" not in step5  # not the literal sentinel either
        # And the call must omit ``teradata_profile`` entirely so refresh_env
        # folds to wizard-default.
        assert "dbt_project(action='refresh_env', project_name='default')" in step5
        # The why-extra explains why.
        assert "Omit ``teradata_profile``" in step5

    @pytest.mark.asyncio
    async def test_next_steps_refresh_hint_passes_profile_for_named_binding(
        self, orch, tmp_path
    ):
        """When the sub-project is bound to a named profile (e.g.
        ``"prod"``), the refresh_env hint must pass that name as
        ``teradata_profile``."""
        # Override the fixture's wizard-bound sub-project with a
        # named-profile-bound one.
        named_sub = orch.dbt_project_parent / "dbt_named"
        named_sub.mkdir()
        (named_sub / "dbt_project.yml").write_text(
            "name: 'named'\nprofile: 'prod'\n", encoding="utf-8"
        )
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "named_refresh.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="named_refresh_dag",
            project_name="named",
            output_filename="named_refresh.py",
        )
        assert result["success"] is True, result
        assert result["teradata_identity"] == "prod"
        step5 = next(s for s in result["next_steps"] if "refresh_env" in s)
        # The call form must pass the binding name as teradata_profile.
        assert (
            "dbt_project(action='refresh_env', project_name='named', "
            "teradata_profile='prod')"
        ) in step5

    @pytest.mark.asyncio
    async def test_create_dbt_dag_does_not_push_wizard_creds_to_airflow_variables(
        self, orch, tmp_path
    ):
        """Rule 5: the MCP server no longer pushes Teradata creds into
        Airflow Variables. Whoever owns the Airflow worker must provision
        ``TERADATA_*`` env vars out-of-band. This test asserts the
        cred-push removal at airflow_pipeline_management.py:1442-1471 by
        confirming ``set_variable`` is never called for any TERADATA_* key
        even when ``use_ssh_for_dbt=True`` (the path that previously
        triggered the push)."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.dags_folder = tmp_path
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dbt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        # set_variable returns a real value so it doesn't crash if called.
        orch.async_airflow_client.set_variable = AsyncMock(return_value={"key": "x"})
        # Force SSH branch — the only one that previously pushed creds.
        orch.settings.airflow.remote_host = "remote-airflow.example.com"

        dag_file = tmp_path / "dbt_out.py"
        dag_file.write_text("# dbt dag", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="dbt_dag_no_push",
            project_name="default",
            use_ssh_for_dbt=True,
            output_filename="dbt_out.py",
        )
        # Either success or some unrelated failure — but no TERADATA_*
        # variable should have been created in either case.
        teradata_var_calls = [
            c for c in orch.async_airflow_client.set_variable.call_args_list
            if str(c.kwargs.get("key", "")).startswith("TERADATA_")
            or (c.args and str(c.args[0]).startswith("TERADATA_"))
        ]
        assert teradata_var_calls == [], (
            f"Wizard cred-push was supposed to be removed but set_variable "
            f"was called for TERADATA_* keys: {teradata_var_calls}"
        )
        # And the generator's dbt_env should be None — DAG won't read
        # TERADATA_* variables from Airflow.
        gen_kwargs = orch.airflow_dag_generator.generate_dbt_only_dag.call_args.kwargs
        assert gen_kwargs.get("dbt_env") is None, (
            f"Expected dbt_env=None (no Airflow Variables referenced) but "
            f"got {gen_kwargs.get('dbt_env')!r}"
        )


# ═══════════════════════════════════════════════════════════════════════════
#  pipeline_deploy — provision_teradata_variables was REMOVED
#  TERADATA_* now flows via the per-sub-project .env file written by
#  dbt_project(action='create_structure'|'refresh_env'); the DAG runs
#  `dotenv run -- dbt ...` and reads the file at task time.
# ═══════════════════════════════════════════════════════════════════════════


class TestProvisionTeradataVariablesRemoved:
    """The legacy ``provision_teradata_variables`` action no longer
    exists. Calling it must surface a clear ``Unknown action`` error so
    the LLM can self-correct to the new ``.env``-based path. No Airflow
    Variables get pushed."""

    @pytest.mark.asyncio
    async def test_action_no_longer_accepted(self):
        orch = _make_orchestrator()
        orch.async_airflow_client.set_variable = AsyncMock(
            return_value={"key": "ok"},
        )
        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="provision_teradata_variables",
            teradata_profile="prod",
        )
        assert result["success"] is False
        # Error message should help the agent self-correct.
        err = result["error"].lower()
        assert "unknown action" in err
        assert "provision_teradata_variables" in err
        # The message lists the surviving actions; agent uses that for retry.
        assert "create_dbt_dag" in err
        # Critical: no Variables were pushed.
        orch.async_airflow_client.set_variable.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
#  pipeline_deploy — create_sync_dag with project_name (ELT, dbt step included)
# ═══════════════════════════════════════════════════════════════════════════


class TestPipelineDeployCreateSyncDagWithDbt:
    @pytest.mark.asyncio
    async def test_create_sync_dag_with_dbt(self, orch, tmp_path):
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.generate_elt_pipeline_dag = Mock(return_value="# elt dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        # AsyncMock is required here: the deploy path awaits these methods, and
        # without explicit return values the default AsyncMock would return
        # MagicMocks that get ``.get()``-ed as if dicts. That produces unawaited
        # coroutines whose internal call-list grows until the test runs out of
        # memory (see RuntimeWarning: coroutine '_execute_mock_call' was never
        # awaited).
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            return_value={"connection_id": "airbyte_default", "conn_type": "airbyte"}
        )

        dag_file = tmp_path / "elt_output.py"
        dag_file.write_text("# elt dag", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="elt_daily",
            connection_id="conn-1",
            project_name="default",
            source_name="postgres",
            target_schema="raw",
            tags=["elt", "postgres"],
            output_filename="elt_output.py",
            run_dbt_tests=False,
            generate_dbt_docs=True,
        )
        assert result["success"] is True
        assert result["dag_id"] == "elt_daily"
        sub = orch.dbt_project_parent / "dbt_default"
        assert result["dbt_project_dir"] == str(sub)
        assert result["teradata_identity"] == "wizard:td_host"
        assert result["connection_id"] == "conn-1"

        orch.airflow_dag_generator.generate_elt_pipeline_dag.assert_called_once()
        call_kwargs = orch.airflow_dag_generator.generate_elt_pipeline_dag.call_args[1]
        assert call_kwargs["source_name"] == "postgres"
        assert call_kwargs["target_schema"] == "raw"
        assert call_kwargs["extract_config"]["connection_id"] == "conn-1"
        # The transform config now references the resolved sub-project path.
        assert call_kwargs["transform_config"]["project_dir"] == str(sub)
        assert call_kwargs["tags"] == ["elt", "postgres"]
        assert call_kwargs["output_filename"] == "elt_output.py"
        assert call_kwargs["run_dbt_tests"] is False
        assert call_kwargs["generate_dbt_docs"] is True

        validate_path = orch.airflow_dag_generator.validate_dag_file.call_args[0][0]
        assert str(validate_path).endswith("elt_output.py")

    @pytest.mark.asyncio
    async def test_create_sync_dag_with_dbt_does_not_push_wizard_creds(
        self, orch, tmp_path
    ):
        """Rule 5 mirror: same removal applies to ``_create_elt_dag``
        (called by create_sync_dag when ``project_name`` is supplied).
        Asserts no TERADATA_* Airflow Variables are pushed even with
        ``use_ssh_for_dbt=True``."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.generate_elt_pipeline_dag = Mock(return_value="# elt")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            return_value={"connection_id": "airbyte_default", "conn_type": "airbyte"}
        )
        orch.async_airflow_client.set_variable = AsyncMock(return_value={"key": "x"})
        orch.settings.airflow.remote_host = "remote-airflow.example.com"

        dag_file = tmp_path / "elt_no_push.py"
        dag_file.write_text("# elt", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="elt_no_push",
            connection_id="conn-1",
            project_name="default",
            source_name="postgres",
            target_schema="raw",
            use_ssh_for_dbt=True,
            output_filename="elt_no_push.py",
        )
        teradata_var_calls = [
            c for c in orch.async_airflow_client.set_variable.call_args_list
            if str(c.kwargs.get("key", "")).startswith("TERADATA_")
            or (c.args and str(c.args[0]).startswith("TERADATA_"))
        ]
        assert teradata_var_calls == [], (
            f"Wizard cred-push removal did not apply to _create_elt_dag — "
            f"set_variable was called for TERADATA_* keys: {teradata_var_calls}"
        )
        # transform_config.env_from_variables must be None too.
        gen_kwargs = orch.airflow_dag_generator.generate_elt_pipeline_dag.call_args.kwargs
        transform_config = gen_kwargs.get("transform_config", {})
        assert transform_config.get("env_from_variables") is None, (
            f"Expected env_from_variables=None in transform_config, got "
            f"{transform_config.get('env_from_variables')!r}"
        )

    @pytest.mark.asyncio
    async def test_create_sync_dag_with_dbt_ignores_teradata_profile(
        self, orch, tmp_path
    ):
        """Mirror of the dbt-only test: the dbt branch of create_sync_dag
        accepts ``teradata_profile`` on the router for shape consistency
        but ignores it. The response's ``teradata_identity`` reflects the
        sub-project's on-disk binding."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.generate_elt_pipeline_dag = Mock(
            return_value="# elt"
        )
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(
            return_value={"connection_id": "airbyte_default", "conn_type": "airbyte"},
        )

        dag_file = tmp_path / "elt_ignore_profile.py"
        dag_file.write_text("# elt", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="elt_ignore_profile",
            connection_id="conn-1",
            project_name="default",
            teradata_profile="some_other_profile",  # ignored
            source_name="postgres",
            target_schema="raw",
            output_filename="elt_ignore_profile.py",
        )
        assert result["success"] is True, result
        assert result["teradata_identity"] == "wizard:td_host"

    @pytest.mark.asyncio
    async def test_create_sync_dag_without_dbt_unchanged(self, orch, tmp_path):
        """Without ``project_name``, create_sync_dag routes to the
        Airbyte-only path (no dbt step)."""
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.async_airflow_client.get_connection = AsyncMock(
            side_effect=Exception("404 Not Found")
        )
        orch.async_airflow_client.create_connection = AsyncMock(return_value={
            "connection_id": "airbyte_default",
            "conn_type": "airbyte",
        })
        orch.airbyte_client = AsyncMock()
        orch.airbyte_client.get_connection = AsyncMock(return_value={
            "schedule": {"scheduleType": "manual"},
        })
        orch.airflow_dag_generator.generate_dag = Mock(return_value="# airbyte dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )

        dag_file = tmp_path / "sync_test.py"
        dag_file.write_text("# dag code", encoding="utf-8")

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="sync_test",
            connection_id="airbyte-conn-123",
            output_filename="sync_test.py",
        )
        assert result["success"] is True
        orch.airflow_dag_generator.generate_dag.assert_called_once()
        assert not hasattr(orch.airflow_dag_generator.generate_elt_pipeline_dag, "called") or \
            not orch.airflow_dag_generator.generate_elt_pipeline_dag.called


# ═══════════════════════════════════════════════════════════════════════════
#  SSH profile auto-detection: always prompt, never silently auto-select
# ═══════════════════════════════════════════════════════════════════════════


class TestSSHProfileAutoDetectionPrompt:

    @pytest.mark.asyncio
    async def test_create_dbt_dag_explicit_ssh_profile_bypasses(self, orch, tmp_path):
        orch.credential_resolver.is_configured = True
        orch.settings.pipeline.dags_output_dir = str(tmp_path)
        orch.airflow_dag_generator.generate_dbt_only_dag = Mock(return_value="# dag")
        orch.airflow_dag_generator.validate_dag_file = Mock(
            return_value={"valid": True, "syntax_error": None}
        )
        dag_file = tmp_path / "test_dag.py"
        dag_file.write_text("# dag", encoding="utf-8")

        orch.async_airflow_client.get_connection = AsyncMock(return_value={
            "connection_id": "ssh_default",
            "host": "localhost",
            "login": "user",
            "port": 22,
            "extra": {},
        })

        tools = register_pipeline_tools(orch)
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="test_dag",
            project_name="default",
            use_ssh_for_dbt=True,
            ssh_profile="my_ssh",
        )
        assert result["success"] is True
        orch.credential_resolver.find_ssh_profiles.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_ssh_connection_explicit_profile_bypasses(self, orch, tools):
        orch.credential_resolver.is_configured = True
        orch.credential_resolver.guard_configured.return_value = None
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "ssh-host",
            "port": 22,
            "username": "user",
            "password": "pass",
        }
        orch.async_airflow_client.get_connection = AsyncMock(return_value={
            "connection_id": "ssh_localhost",
            "host": "ssh-host",
            "login": "user",
            "port": 22,
            "extra": {},
        })

        result = await tools["airflow_connections"](
            action="create_ssh", ssh_profile="my_ssh"
        )
        assert result["success"] is True
        orch.credential_resolver.find_ssh_profiles.assert_not_called()


# ---------------------------------------------------------------------------
# TestDbtTargetValidation
# ---------------------------------------------------------------------------

class TestDbtTargetValidation:
    """Tests for _validate_dbt_target helper."""

    def _write_yaml(self, path, data):
        with open(path, "w") as f:
            yaml.dump(data, f)

    def test_valid_target_returns_none(self, tmp_path):
        self._write_yaml(tmp_path / "profiles.yml", {
            "myprofile": {"outputs": {"dev": {"type": "teradata"}}},
        })
        self._write_yaml(tmp_path / "dbt_project.yml", {"profile": "myprofile"})

        result = _validate_dbt_target(str(tmp_path), "dev")
        assert result is None

    def test_invalid_target_returns_error(self, tmp_path):
        self._write_yaml(tmp_path / "profiles.yml", {
            "myprofile": {"outputs": {"dev": {"type": "teradata"}}},
        })
        self._write_yaml(tmp_path / "dbt_project.yml", {"profile": "myprofile"})

        result = _validate_dbt_target(str(tmp_path), "prod")
        assert result is not None
        assert result["success"] is False
        assert "Valid targets: dev" in result["error"]
        assert "'myprofile'" in result["error"]

    def test_missing_profiles_yml_returns_none(self, tmp_path):
        self._write_yaml(tmp_path / "dbt_project.yml", {"profile": "myprofile"})

        result = _validate_dbt_target(str(tmp_path), "prod")
        assert result is None

    def test_missing_dbt_project_yml_returns_none(self, tmp_path):
        self._write_yaml(tmp_path / "profiles.yml", {
            "myprofile": {"outputs": {"dev": {}}},
        })

        result = _validate_dbt_target(str(tmp_path), "prod")
        assert result is None

    def test_malformed_yaml_returns_none(self, tmp_path):
        (tmp_path / "profiles.yml").write_text(": invalid: yaml: [")
        self._write_yaml(tmp_path / "dbt_project.yml", {"profile": "myprofile"})

        result = _validate_dbt_target(str(tmp_path), "prod")
        assert result is None

    def test_profile_not_in_profiles_yml_returns_none(self, tmp_path):
        self._write_yaml(tmp_path / "profiles.yml", {
            "other_profile": {"outputs": {"dev": {}}},
        })
        self._write_yaml(tmp_path / "dbt_project.yml", {"profile": "myprofile"})

        result = _validate_dbt_target(str(tmp_path), "prod")
        assert result is None


# ---------------------------------------------------------------------------
# Path containment — output_filename must stay inside dags_output_dir.
# The DAG-write path is already transparently protected by SafeFileWriter;
# these tests verify the tool-layer rejection (clean MCP-boundary error).
# ---------------------------------------------------------------------------


class TestOutputFilenameValidation:
    """Tool-layer containment for output_filename in create_*_dag tools."""

    @pytest.fixture
    def orchestrator_with_tmp_dags(self, tmp_path):
        # tmp_path is shared between dags_output_dir and the dbt sub-project
        # parent — both stay inside the test's tmp dir.
        orch = _make_orchestrator(tmp_path=tmp_path)
        orch.settings.pipeline.dags_output_dir = tmp_path
        return orch

    @pytest.fixture
    def tools(self, orchestrator_with_tmp_dags):
        return register_pipeline_tools(orchestrator_with_tmp_dags)

    @pytest.mark.asyncio
    async def test_create_sync_dag_rejects_traversal_output_filename(self, tools):
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="test_dag",
            connection_id="conn-1",
            output_filename="../../../../tmp/pwn.py",
        )
        assert result["success"] is False
        assert "invalid output_filename" in result["error"]

    @pytest.mark.asyncio
    async def test_create_sync_dag_rejects_absolute_output_filename(self, tools):
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="test_dag",
            connection_id="conn-1",
            output_filename="/etc/cron.d/pwn",
        )
        assert result["success"] is False
        assert "invalid output_filename" in result["error"]

    @pytest.mark.asyncio
    async def test_create_dbt_dag_rejects_traversal_output_filename(self, tools):
        result = await tools["pipeline_deploy"](
            action="create_dbt_dag",
            dag_id="test_dbt_dag",
            project_name="default",
            output_filename="../../escape.py",
        )
        assert result["success"] is False
        assert "invalid output_filename" in result["error"]

    @pytest.mark.asyncio
    async def test_create_elt_dag_rejects_traversal_output_filename(self, tools):
        # The create_elt_dag path is reached via action=create_sync_dag
        # when project_name is supplied (selecting a dbt sub-project).
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="test_elt_dag",
            connection_id="conn-1",
            project_name="default",
            output_filename="../../escape.py",
        )
        assert result["success"] is False
        assert "invalid output_filename" in result["error"]

    @pytest.mark.asyncio
    async def test_create_sync_dag_rejects_null_byte(self, tools):
        result = await tools["pipeline_deploy"](
            action="create_sync_dag",
            dag_id="test_dag",
            connection_id="conn-1",
            output_filename="safe\x00.py",
        )
        assert result["success"] is False
        assert "invalid output_filename" in result["error"]


class TestValidateOutputFilenameHelper:
    """Unit tests for the module-level _validate_output_filename helper."""

    def test_applies_default_filename(self, tmp_path):
        from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
            _validate_output_filename,
        )
        result = _validate_output_filename(None, "my_dag", tmp_path)
        assert result == "my_dag.py"

    def test_accepts_safe_caller_filename(self, tmp_path):
        from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
            _validate_output_filename,
        )
        result = _validate_output_filename("custom.py", "my_dag", tmp_path)
        assert result == "custom.py"

    def test_rejects_parent_traversal(self, tmp_path):
        from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
            _validate_output_filename,
        )
        with pytest.raises(ValueError, match="invalid output_filename"):
            _validate_output_filename("../../escape.py", "my_dag", tmp_path)

    def test_rejects_absolute_path(self, tmp_path):
        import sys

        from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
            _validate_output_filename,
        )
        abs_path = "/etc/cron.d/pwn" if sys.platform != "win32" else r"C:\Windows\pwn.py"
        with pytest.raises(ValueError, match="invalid output_filename"):
            _validate_output_filename(abs_path, "my_dag", tmp_path)

    def test_rejects_null_byte(self, tmp_path):
        from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
            _validate_output_filename,
        )
        with pytest.raises(ValueError, match="invalid output_filename"):
            _validate_output_filename("safe\x00.py", "my_dag", tmp_path)


# NOTE: ``_validate_dbt_project_dir`` and its tests
# (``TestValidateDbtProjectDirHelper``, ``TestCreateDbtDagDbtProjectDirBoundary``)
# were removed. The DAG-generation tools no longer accept a raw
# ``dbt_project_dir`` string from the LLM — ``project_name`` (a slug
# constrained to live under ``orchestrator.dbt_project_parent``) is the only
# way to identify a sub-project, so the trust boundary is enforced by the
# resolver instead of a path-validation helper.


# ═══════════════════════════════════════════════════════════════════════════
#  SSH key/password missing — direct the agent to ASK THE USER, not to .env
# ═══════════════════════════════════════════════════════════════════════════


class TestSSHCredentialMissingMessages:
    """The deploy-credentials validation block must direct the agent to
    Setup Wizard / ssh_profile / asking the user — never to writing
    ``.env`` itself."""

    def test_ssh_error_messages_direct_to_user_or_profile(self):
        """Source-level smoke: the SSH-key/password missing strings in
        airflow_pipeline_management.py mention Setup Wizard +
        ``ssh_profile`` and explicitly tell the agent not to edit .env.
        The previous self-serve hints are gone."""
        from pathlib import Path

        src = Path(__file__).parent.parent.parent / (
            "src/teradata_etl_mcp_server/tools/airflow_pipeline_management.py"
        )
        text = src.read_text(encoding="utf-8")

        # Old self-serve wording is gone (parenthetical "set ... in .env"
        # at the end of the SSH validation messages).
        assert "AIRFLOW_REMOTE_SSH_KEY in .env)" not in text
        assert "AIRFLOW_REMOTE_PASSWORD in .env)" not in text
        # New wording present.
        assert "Ask the user to update AIRFLOW_REMOTE_SSH_KEY" in text
        assert "Ask the user to update AIRFLOW_REMOTE_PASSWORD" in text
        assert "ssh_profile" in text
        # Explicit no-self-serve directive.
        assert "must not create or edit .env" in text


# ═══════════════════════════════════════════════════════════════════════════
#  Finding #6 — _configure_ssh_host_key_policy soft-default warning
# ═══════════════════════════════════════════════════════════════════════════


class TestSSHHostKeyPolicyWarning:
    """Tests for the shared SSH host-key policy helper that replaces the five
    previously-inline policy-setting blocks with a single uniform warning."""

    def test_warning_fires_when_strict_false(self, caplog):
        """strict=False → AutoAddPolicy + loud uniform WARNING naming the caller."""
        import logging

        import paramiko as _paramiko

        from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
            _configure_ssh_host_key_policy,
        )

        mock_ssh = Mock()
        with caplog.at_level(logging.WARNING, logger="teradata_etl_mcp_server.tools.airflow_pipeline_management"):
            _configure_ssh_host_key_policy(
                mock_ssh, False, context="test_caller"
            )

        # Policy set on the client.
        mock_ssh.set_missing_host_key_policy.assert_called_once()
        policy_arg = mock_ssh.set_missing_host_key_policy.call_args[0][0]
        assert isinstance(policy_arg, _paramiko.AutoAddPolicy)

        # Uniform warning emitted, naming the call site and mentioning MITM.
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "MITM" in r.getMessage() and "test_caller" in r.getMessage()
            for r in warnings
        ), f"Expected MITM warning naming 'test_caller', got: {[r.getMessage() for r in warnings]}"

    def test_no_warning_when_strict_true(self, caplog):
        """strict=True → RejectPolicy and no MITM warning in caplog."""
        import logging

        import paramiko as _paramiko

        from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
            _configure_ssh_host_key_policy,
        )

        mock_ssh = Mock()
        with caplog.at_level(logging.WARNING, logger="teradata_etl_mcp_server.tools.airflow_pipeline_management"):
            _configure_ssh_host_key_policy(
                mock_ssh, True, context="strict_caller"
            )

        # Loaded system known_hosts, then RejectPolicy applied.
        mock_ssh.load_system_host_keys.assert_called_once()
        mock_ssh.set_missing_host_key_policy.assert_called_once()
        policy_arg = mock_ssh.set_missing_host_key_policy.call_args[0][0]
        assert isinstance(policy_arg, _paramiko.RejectPolicy)

        # No MITM warning (the only WARNING path is the non-strict branch).
        assert not any(
            "MITM" in r.getMessage() for r in caplog.records
        )

    def test_helper_tolerates_missing_system_known_hosts(self, caplog):
        """load_system_host_keys failing under strict=True must still end in
        RejectPolicy — never fall back to AutoAddPolicy when the caller asked
        for strict."""
        import logging

        import paramiko as _paramiko

        from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
            _configure_ssh_host_key_policy,
        )

        mock_ssh = Mock()
        mock_ssh.load_system_host_keys.side_effect = OSError("known_hosts corrupted")
        with caplog.at_level(logging.WARNING, logger="teradata_etl_mcp_server.tools.airflow_pipeline_management"):
            _configure_ssh_host_key_policy(
                mock_ssh, True, context="failing_known_hosts"
            )

        # Still RejectPolicy under strict — fail-safe.
        policy_arg = mock_ssh.set_missing_host_key_policy.call_args[0][0]
        assert isinstance(policy_arg, _paramiko.RejectPolicy)
        # The load-failure is surfaced as a WARNING.
        assert any(
            "failing_known_hosts" in r.getMessage() and "known_hosts" in r.getMessage()
            for r in caplog.records
        )
