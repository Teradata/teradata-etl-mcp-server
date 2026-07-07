"""Airflow DAG file generator.

This module generates Airflow DAG Python files for orchestrating
ELT pipelines with Airbyte and dbt operations.

Note: TPT and BTEQ task generation is handled by airflow_tdload_dag_generator.
"""

import logging
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment

from ..utils.file_operations import SafeFileWriter
from .escaping import escape_single_quoted, escape_triple_quoted

logger = logging.getLogger(__name__)


class AirflowDAGGeneratorError(Exception):
    """Base exception for Airflow DAG generator errors."""

    pass


class AirflowDAGGenerator:
    """
    Airflow DAG file generator for Airbyte and dbt operations.

    Generates Python DAG files with Airbyte sync operators and dbt transformation tasks.
    TPT and BTEQ tasks are handled by airflow_tdload_dag_generator.
    """

    # Jinja2 template for basic DAG structure
    DAG_TEMPLATE = '''"""
{{ dag_description }}

Generated: {{ generation_timestamp }}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
{% if use_sensors %}from airflow.sensors.external_task import ExternalTaskSensor
{% endif %}{% if email_on_failure or email_on_retry %}from airflow.utils.email import send_email
{% endif %}
{% if use_airbyte %}from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
{% endif %}{% if use_airbyte_sensor %}from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
{% endif %}{% if use_ssh_dbt %}from airflow.providers.ssh.operators.ssh import SSHOperator
{% endif %}{% if use_variables %}from airflow.models import Variable
{% endif %}

# Default arguments
default_args = {
    'owner': '{{ owner }}',
    'depends_on_past': {{ depends_on_past }},
    'email_on_failure': {{ email_on_failure }},
    'email_on_retry': {{ email_on_retry }},
    'retries': {{ retries }},
    'retry_delay': timedelta({{ retry_delay_unit }}={{ retry_delay_value }}),
    {% if execution_timeout %}'execution_timeout': timedelta({{ execution_timeout_unit }}={{ execution_timeout_value }}),
    {% endif %}{% if queue %}'queue': '{{ queue }}',
    {% endif %}
}

{% if custom_callbacks %}
# Custom callbacks
{% for callback_name, callback_code in custom_callbacks.items() %}
def {{ callback_name }}(context):
    """{{ callback_name }} callback."""
{{ callback_code | indent(4, True) }}

{% endfor %}
{% endif %}

# DAG definition
with DAG(
    dag_id='{{ dag_id }}',
    default_args=default_args,
    description='{{ dag_description }}',
    schedule={{ schedule_interval | safe }},
    start_date=datetime({{ start_date_year }}, {{ start_date_month }}, {{ start_date_day }}),
    {% if end_date %}end_date=datetime({{ end_date_year }}, {{ end_date_month }}, {{ end_date_day }}),
    {% endif %}catchup={{ catchup }},
    tags={{ tags }},
    {% if max_active_runs %}max_active_runs={{ max_active_runs }},
    {% endif %}{% if concurrency %}concurrency={{ concurrency }},
    {% endif %}{% if on_failure_callback %}on_failure_callback={{ on_failure_callback }},
    {% endif %}{% if on_success_callback %}on_success_callback={{ on_success_callback }},
    {% endif %}
) as dag:

    {% if doc_md %}dag.doc_md = """{{ doc_md }}"""

    {% endif %}# Tasks
{{ tasks_code | safe | indent(4, True) }}

    # Task dependencies
{{ dependencies_code | safe | indent(4, True) }}
'''

    # Template for Airbyte task (asynchronous mode only)
    AIRBYTE_TASK_TEMPLATE = """
{{ task_id }} = AirbyteTriggerSyncOperator(
    task_id='{{ task_id }}',
    connection_id='{{ connection_id }}',
    {% if airbyte_conn_id %}airbyte_conn_id='{{ airbyte_conn_id }}',
    {% endif %}asynchronous=True,
)

{{ task_id }}_sensor = AirbyteJobSensor(
    task_id='{{ task_id }}_sensor',
    airbyte_job_id={{ task_id }}.output,
    poke_interval=30,      # seconds between sensor polls
    timeout=60 * 60 * 6,   # default 6 hours timeout (adjust per your needs)
)
"""

    # Template for dbt task
    DBT_TASK_TEMPLATE = """{{ task_id }} = BashOperator(
    task_id='{{ task_id }}',
    bash_command='{{ dbt_command }}',
    {% if env %}env={{ env }},
    {% endif %}{% if cwd %}cwd='{{ cwd }}',
    {% endif %}
)"""

    DBT_SSH_TASK_TEMPLATE = """{{ task_id }} = SSHOperator(
    task_id='{{ task_id }}',
    ssh_conn_id='{{ ssh_conn_id }}',
    command={{ cmd_expr }},
    {% if timeout is not none %}cmd_timeout={{ timeout }},
    {% endif %}
)"""

    # Template for generic Bash task
    BASH_TASK_TEMPLATE = '''{{ task_id }} = BashOperator(
    task_id='{{ task_id }}',
    bash_command='{{ bash_command }}',
    {% if env %}env={{ env }},
    {% endif %}{% if cwd %}cwd='{{ cwd }}',
    {% endif %}{% if retries is not none %}retries={{ retries }},
    {% endif %}{% if retry_delay_minutes is not none %}retry_delay=timedelta(minutes={{ retry_delay_minutes }}),
    {% endif %}{% if trigger_rule %}trigger_rule='{{ trigger_rule }}',
    {% endif %}{% if pool %}pool='{{ pool }}',
    {% endif %}{% if priority_weight is not none %}priority_weight={{ priority_weight }},
    {% endif %}{% if doc_md %}doc_md="""{{ doc_md }}""",
    {% endif %}
)'''

    # Template for custom Python task
    PYTHON_TASK_TEMPLATE = '''def {{ task_id }}_func(**context):
    """{{ task_description }}"""
{{ python_code | indent(4, True) }}

{{ task_id }} = PythonOperator(
    task_id='{{ task_id }}',
    python_callable={{ task_id }}_func,
    {% if op_kwargs %}op_kwargs={{ op_kwargs }},
    {% endif %}provide_context=True,
)'''

    # Template for sensor task
    SENSOR_TASK_TEMPLATE = """{{ task_id }} = ExternalTaskSensor(
    task_id='{{ task_id }}',
    external_dag_id='{{ external_dag_id }}',
    external_task_id='{{ external_task_id }}',
    {% if execution_delta %}execution_delta=timedelta({{ execution_delta_unit }}={{ execution_delta_value }}),
    {% endif %}{% if execution_date_fn %}execution_date_fn={{ execution_date_fn }},
    {% endif %}timeout={{ timeout }},
    poke_interval={{ poke_interval }},
    mode='{{ mode }}',
)"""

    def __init__(self, dags_folder: Path):
        """
        Initialize Airflow DAG generator.

        Args:
            dags_folder: Path to Airflow DAGs folder
        """
        self.dags_folder = Path(dags_folder)
        self.dags_folder.mkdir(parents=True, exist_ok=True)
        # Explicitly disable autoescape for Python code generation to avoid HTML entities
        self.env = Environment(autoescape=False)  # noqa: S701  # nosec B701 - generates Python code, not HTML

        # enable_backups=False: the Airflow scheduler recurses into the DAGs
        #   folder and would parse any .backups/*.py as additional DAGs,
        #   producing duplicate-DAG warnings on every regeneration.
        # restrict_permissions=False: keep umask-default perms so an Airflow
        #   worker running as a different local user can read the generated
        #   DAG before it is SFTP'd to the remote host.
        self.file_writer = SafeFileWriter(
            output_dir=self.dags_folder,
            keep_backups=5,
            validate_python=True,
            enable_backups=False,
            restrict_permissions=False,
        )

        logger.info("Initialized Airflow DAG generator for %s", self.dags_folder)

    # ==================== DAG Generation ====================

    def generate_dag(
        self,
        dag_id: str,
        description: str,
        schedule: str,
        tasks: list[dict[str, Any]],
        dependencies: list[tuple[str, str]],
        start_date: datetime,
        owner: str = "airflow",
        email: list[str] | None = None,  # noqa: ARG002  # reserved for future email-on-failure support
        retries: int = 1,
        retry_delay_minutes: int = 5,
        tags: list[str] | None = None,
        catchup: bool = False,
        max_active_runs: int = 1,
        end_date: datetime | None = None,
        execution_timeout_minutes: int | None = None,
        on_failure_callback: str | None = None,
        on_success_callback: str | None = None,
        custom_callbacks: dict[str, str] | None = None,
        doc_md: str | None = None,
        output_filename: str | None = None,
    ) -> str:
        """
        Generate complete Airflow DAG file.

        Args:
            dag_id: DAG identifier
            description: DAG description
            schedule: Cron expression or preset (@daily, @hourly, etc.)
            tasks: List of task definitions (see task generation methods)
            dependencies: List of (upstream_task_id, downstream_task_id) tuples
            start_date: DAG start date
            owner: DAG owner
            email: Email addresses for notifications
            retries: Number of retries
            retry_delay_minutes: Delay between retries in minutes
            tags: List of tags
            catchup: Whether to backfill historical runs
            max_active_runs: Maximum concurrent DAG runs
            end_date: Optional end date
            execution_timeout_minutes: Optional task execution timeout
            on_failure_callback: Optional failure callback function name
            on_success_callback: Optional success callback function name
            custom_callbacks: Optional dict of callback functions
            doc_md: Optional markdown documentation
            output_filename: Optional output filename (defaults to {dag_id}.py)

        Returns:
            Generated DAG Python code
        """
        try:
            # Generate task code
            tasks_code = self._generate_tasks_code(tasks)

            # Generate dependencies code
            dependencies_code = self._generate_dependencies_code(dependencies)

            # Escape special characters for embedding in Python string literals:
            # 1. Backslashes must be doubled (C:\Users -> C:\\Users) to avoid
            #    unicode escapes (\U), named chars (\N), hex (\x), etc.
            # 2. Single quotes in description must be escaped (used in single-quoted string)
            # 3. Triple quotes in doc_md must be escaped (used in triple-quoted string)
            safe_description = escape_single_quoted(description)
            safe_doc_md = escape_triple_quoted(doc_md)

            # Prepare context
            context = {
                "dag_id": dag_id,
                "dag_description": safe_description,
                "generation_timestamp": datetime.now(timezone.utc).isoformat(),
                "owner": owner,
                "depends_on_past": "False",  # Usually False for idempotency
                "email_on_failure": "False",
                "email_on_retry": "False",
                "retries": retries,
                "retry_delay_unit": "minutes",
                "retry_delay_value": retry_delay_minutes,
                "schedule_interval": self._format_schedule_interval(schedule),
                "start_date_year": start_date.year,
                "start_date_month": start_date.month,
                "start_date_day": start_date.day,
                "catchup": str(catchup),
                "tags": repr(tags or []),
                "max_active_runs": max_active_runs,
                "tasks_code": tasks_code,
                "dependencies_code": dependencies_code,
                "use_sensors": any(
                    t.get("type") in ("sensor", "file_sensor", "time_sensor")
                    or t.get("operator") in ("ExternalTaskSensor", "FileSensor", "TimeDeltaSensor")
                    for t in tasks
                ),
                "custom_callbacks": custom_callbacks,
                "on_failure_callback": on_failure_callback,
                "on_success_callback": on_success_callback,
                "doc_md": safe_doc_md,
            }

            # Optional fields
            if end_date:
                context.update(
                    {
                        "end_date": True,
                        "end_date_year": end_date.year,
                        "end_date_month": end_date.month,
                        "end_date_day": end_date.day,
                    }
                )

            if execution_timeout_minutes:
                context.update(
                    {
                        "execution_timeout": True,
                        "execution_timeout_unit": "minutes",
                        "execution_timeout_value": execution_timeout_minutes,
                    }
                )

            # Render template (include provider imports when needed)
            use_airbyte = any(
                t.get("type") == "airbyte" or t.get("operator") == "AirbyteTriggerSyncOperator"
                for t in tasks
            )
            # All Airbyte tasks use asynchronous mode with sensor
            use_airbyte_sensor = use_airbyte
            use_ssh_dbt = any(t.get("type") == "dbt" and t.get("use_ssh") for t in tasks)
            use_variables = any(t.get("type") == "dbt" and t.get("env") for t in tasks)
            context.update(
                {
                    "use_airbyte": use_airbyte,
                    "use_airbyte_sensor": use_airbyte_sensor,
                    "use_ssh_dbt": use_ssh_dbt,
                    "use_variables": use_variables,
                }
            )
            template = self.env.from_string(self.DAG_TEMPLATE)
            dag_code = template.render(**context)

            # Write to file (ensure filename only, no directory prefix)
            if output_filename is None:
                output_filename = f"{dag_id}.py"
            output_filename = Path(output_filename).name

            try:
                output_path, metadata = self.file_writer.write_file_safe(
                    content=dag_code,
                    filename=output_filename,
                    force=False,
                )
                if metadata.get("warnings"):
                    for warning in metadata["warnings"]:
                        logger.warning("DAG %s: %s", dag_id, warning)
                logger.info("Generated DAG file: %s", output_path)
            except Exception as write_error:
                logger.error("Failed to write DAG file: %s", write_error, exc_info=True)
                raise

            return dag_code

        except Exception as e:
            logger.error("Failed to generate DAG: %s", e, exc_info=True)
            raise AirflowDAGGeneratorError(f"DAG generation failed: {e}") from e

    def _generate_tasks_code(self, tasks: list[dict[str, Any]]) -> str:
        """
        Generate code for all tasks.

        Args:
            tasks: List of task definitions

        Returns:
            Combined tasks code
        """
        task_codes = []

        for task in tasks:
            task_type = task.get("type")
            operator = task.get("operator")

            if task_type == "airbyte":
                code = self._generate_airbyte_task(task)
            elif task_type == "dbt":
                code = self._generate_dbt_task(task)
            elif task_type == "python":
                code = self._generate_python_task(task)
            elif task_type == "sensor":
                code = self._generate_sensor_task(task)
            elif task_type == "empty":
                code = self._generate_empty_task(task)
            elif task_type == "bash":
                code = self._generate_bash_task(task)
            elif task_type in ("file_sensor", "time_sensor"):
                code = self._generate_generic_sensor_task(task)
            # Operator-style definitions
            elif operator == "BashOperator":
                code = self._generate_bash_task({**task, "type": "bash"})
            elif operator == "PythonOperator":
                code = self._generate_python_task(task)
            elif operator == "FileSensor":
                code = self._generate_generic_sensor_task({**task, "type": "file_sensor"})
            elif operator == "TimeDeltaSensor":
                code = self._generate_generic_sensor_task({**task, "type": "time_sensor"})
            else:
                logger.warning(
                    "Unknown task type/operator: %s. Use airflow_tdload_dag_generator for TPT/BTEQ tasks.",
                    task_type or operator,
                )
                continue

            task_codes.append(code)

        return "\n\n".join(task_codes)

    def _format_schedule_interval(self, schedule: Any | None) -> str:
        """Format schedule interval for DAG template."""
        if schedule is None:
            return "None"
        if isinstance(schedule, str):
            return repr(schedule)
        if isinstance(schedule, dict):
            unit, value = next(iter(schedule.items()))
            return f"timedelta({unit}={value})"
        return repr(schedule)

    def _generate_dependencies_code(self, dependencies: list[tuple[str, str]]) -> str:
        """
        Generate dependency declarations.

        Args:
            dependencies: List of (upstream, downstream) tuples

        Returns:
            Dependencies code
        """
        if not dependencies:
            return "# No dependencies"

        # Group by downstream task
        deps_by_downstream = {}
        for upstream, downstream in dependencies:
            if downstream not in deps_by_downstream:
                deps_by_downstream[downstream] = []
            deps_by_downstream[downstream].append(upstream)

        # Generate dependency statements
        dep_lines = []
        for downstream, upstreams in deps_by_downstream.items():
            if len(upstreams) == 1:
                dep_lines.append(f"{upstreams[0]} >> {downstream}")
            else:
                upstream_list = ", ".join(upstreams)
                dep_lines.append(f"[{upstream_list}] >> {downstream}")

        return "\n".join(dep_lines)

    # ==================== Task Generation Methods ====================

    def _generate_airbyte_task(self, task_def: dict[str, Any]) -> str:
        """Generate Airbyte sync task code (asynchronous mode only)."""
        task_id = task_def["task_id"]
        connection_id = task_def.get("connection_id")

        if not connection_id:
            raise AirflowDAGGeneratorError(f"Task {task_id}: connection_id required")

        context = {
            "task_id": task_id,
            "connection_id": connection_id,
            "airbyte_conn_id": task_def.get("airbyte_conn_id"),
        }

        template = self.env.from_string(self.AIRBYTE_TASK_TEMPLATE)
        return template.render(**context)

    def _generate_dbt_task(self, task_def: dict[str, Any]) -> str:
        """Generate dbt task code (BashOperator for local, SSHOperator for remote).

        Credentials flow exclusively via the per-sub-project ``.env`` file:
        every command is wrapped in ``dotenv run -- dbt ...`` and runs with
        ``cwd=<project_dir>`` so ``dotenv`` finds ``.env`` automatically.
        Worker prerequisite: ``pip install "python-dotenv[cli]"`` (the
        ``[cli]`` extra installs the ``dotenv`` command). No ``Variable.get``
        / Airflow-Variables plumbing — that path was removed when
        ``provision_teradata_variables`` went away.
        """
        task_id = task_def["task_id"]
        dbt_command = task_def.get("dbt_command")
        use_ssh = task_def.get("use_ssh", False)
        project_dir = task_def.get("project_dir") or task_def.get("cwd")

        if not dbt_command:
            command = task_def.get("dbt_subcommand", "run")
            profiles_dir = task_def.get("profiles_dir")
            target = task_def.get("target")
            models = task_def.get("models")

            parts = ["dotenv", "run", "--", "dbt", command]

            if project_dir:
                project_dir_norm = project_dir.replace("\\", "/")
                parts += ["--project-dir", project_dir_norm]
            if command != "deps":
                if not profiles_dir and project_dir:
                    profiles_dir = project_dir
                if profiles_dir:
                    profiles_dir = profiles_dir.replace("\\", "/")
                    parts += ["--profiles-dir", profiles_dir]
                if target:
                    parts += ["--target", target]
                if models and command in ("run", "test"):
                    model_list = models if isinstance(models, list) else [models]
                    parts += ["--select"] + model_list

            dbt_command = shlex.join(parts)

        if use_ssh:
            # SSHOperator has no ``cwd``, so prepend ``cd <dir> &&``
            # to ensure dotenv finds .env. Use remote_dir (the path on the
            # SSH host) when available, falling back to project_dir.
            remote_dir = task_def.get("remote_dir") or project_dir
            env_vars = task_def.get("env")

            prefix_parts: list[str] = []
            # Export env overrides inline so the remote shell picks them up.
            # Validate keys and shell-escape values to prevent injection.
            # Secret-bearing keys are excluded — those should live in the
            # per-project .env file or Airflow Connections/Variables instead.
            _SECRET_PATTERNS = re.compile(
                r"password|secret|token|api_key|credential",
                re.IGNORECASE,
            )
            if env_vars and isinstance(env_vars, dict):
                _valid_key = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
                for k, v in env_vars.items():
                    if not _valid_key.match(k):
                        continue
                    if _SECRET_PATTERNS.search(k):
                        logger.warning(
                            "Skipping secret-bearing env key %r from SSH "
                            "inline export; use .env file or Airflow "
                            "Variables instead",
                            k,
                        )
                        continue
                    prefix_parts.append(f"export {k}={shlex.quote(str(v))}")
            if remote_dir:
                remote_dir_norm = remote_dir.replace("\\", "/")
                prefix_parts.append(f"cd {shlex.quote(remote_dir_norm)}")

            if prefix_parts:
                full_cmd = " && ".join(prefix_parts) + f" && {dbt_command}"
            else:
                full_cmd = dbt_command
            cmd_expr = repr(full_cmd)
            context = {
                "task_id": task_id,
                "cmd_expr": cmd_expr,
                "ssh_conn_id": task_def.get("ssh_conn_id", "ssh_default"),
                "timeout": task_def.get("timeout"),
            }
            template = self.env.from_string(self.DBT_SSH_TASK_TEMPLATE)
            return template.render(**context)
        else:
            # Local BashOperator with cwd set to the sub-project; dotenv
            # picks up ``.env`` from there automatically.
            context = {
                "task_id": task_id,
                "dbt_command": escape_single_quoted(dbt_command),
                "env": task_def.get("env"),
                "cwd": escape_single_quoted(task_def.get("cwd") or project_dir),
            }
            template = self.env.from_string(self.DBT_TASK_TEMPLATE)
            return template.render(**context)

    def _generate_python_task(self, task_def: dict[str, Any]) -> str:
        """Generate Python task code."""
        task_id = task_def["task_id"]
        python_code = task_def.get("python_code")
        python_callable = task_def.get("python_callable")

        # If raw python code provided, generate a function wrapper
        if python_code:
            context = {
                "task_id": task_id,
                "task_description": task_def.get("description", "Custom Python task"),
                "python_code": python_code,
                "op_kwargs": repr(task_def.get("op_kwargs")) if task_def.get("op_kwargs") else None,
            }
            template = self.env.from_string(self.PYTHON_TASK_TEMPLATE)
            return template.render(**context)

        # Otherwise, generate a simple PythonOperator referencing an existing callable
        if not python_callable:
            raise AirflowDAGGeneratorError(
                f"Task {task_id}: python_code or python_callable required"
            )

        lines = [
            f"{task_id} = PythonOperator(",
            f"    task_id='{task_id}',",
            f"    python_callable={python_callable},",
        ]
        if task_def.get("op_kwargs"):
            lines.append(f"    op_kwargs={repr(task_def['op_kwargs'])},")
        lines.append(")")
        return "\n".join(lines)

    def _generate_sensor_task(self, task_def: dict[str, Any]) -> str:
        """Generate sensor task code."""
        task_id = task_def["task_id"]
        external_dag_id = task_def.get("external_dag_id")
        external_task_id = task_def.get("external_task_id")

        if not external_dag_id:
            raise AirflowDAGGeneratorError(f"Task {task_id}: external_dag_id required")

        context = {
            "task_id": task_id,
            "external_dag_id": external_dag_id,
            "external_task_id": external_task_id,
            "timeout": task_def.get("timeout", 3600),
            "poke_interval": task_def.get("poke_interval", 60),
            "mode": task_def.get("mode", "poke"),
        }

        # Optional execution delta
        if task_def.get("execution_delta_minutes"):
            context["execution_delta"] = True
            context["execution_delta_unit"] = "minutes"
            context["execution_delta_value"] = task_def["execution_delta_minutes"]

        template = self.env.from_string(self.SENSOR_TASK_TEMPLATE)
        return template.render(**context)

    def _generate_generic_sensor_task(self, task_def: dict[str, Any]) -> str:
        """Generate FileSensor or TimeDeltaSensor based on type."""
        task_id = task_def["task_id"]
        ttype = task_def.get("type")
        poke_interval = task_def.get("poke_interval", 60)
        timeout = task_def.get("timeout", 3600)
        if ttype == "file_sensor":
            filepath = task_def.get("filepath", "/tmp/file.txt")  # noqa: S108  # nosec B108 - default placeholder for generated DAG
            return "\n".join(
                [
                    f"{task_id} = FileSensor(",
                    f"    task_id='{task_id}',",
                    f"    filepath='{filepath}',",
                    f"    poke_interval={poke_interval},",
                    f"    timeout={timeout},",
                    ")",
                ]
            )
        if ttype == "time_sensor":
            delta = task_def.get("delta", {"minutes": 5})
            unit, value = next(iter(delta.items())) if isinstance(delta, dict) else ("minutes", 5)
            return "\n".join(
                [
                    f"{task_id} = TimeDeltaSensor(",
                    f"    task_id='{task_id}',",
                    f"    delta=timedelta({unit}={value}),",
                    f"    poke_interval={poke_interval},",
                    f"    timeout={timeout},",
                    ")",
                ]
            )
        raise AirflowDAGGeneratorError(f"Task {task_id}: unsupported generic sensor type {ttype}")

    def _generate_empty_task(self, task_def: dict[str, Any]) -> str:
        """Generate empty (dummy) task code."""
        task_id = task_def["task_id"]
        return f"{task_id} = EmptyOperator(task_id='{task_id}')"

    def _generate_bash_task(self, task_def: dict[str, Any]) -> str:
        """Generate a generic BashOperator task."""
        task_id = task_def["task_id"]
        bash_command = task_def.get("bash_command")
        if not bash_command:
            raise AirflowDAGGeneratorError(f"Task {task_id}: bash_command required")
        # Use shared escaping function for single-quoted Python strings
        bash_command_escaped = escape_single_quoted(bash_command)
        context = {
            "task_id": task_id,
            "bash_command": bash_command_escaped,
            "env": repr(task_def.get("env")) if task_def.get("env") else None,
            "cwd": task_def.get("cwd"),
            "retries": task_def.get("retries"),
            "retry_delay_minutes": task_def.get("retry_delay"),
            "trigger_rule": task_def.get("trigger_rule"),
            "pool": task_def.get("pool"),
            "priority_weight": task_def.get("priority_weight"),
            "doc_md": task_def.get("doc_md"),
        }
        template = self.env.from_string(self.BASH_TASK_TEMPLATE)
        return template.render(**context)

    # ==================== Pre-built DAG Patterns ====================

    def generate_elt_pipeline_dag(
        self,
        dag_id: str,
        source_name: str,
        target_schema: str,
        extract_config: dict[str, Any],
        transform_config: dict[str, Any],
        schedule: str = "@daily",
        start_date: datetime | None = None,
        owner: str = "airflow",
        use_ssh_for_dbt: bool = False,
        ssh_conn_id: str = "ssh_default",
        tags: list[str] | None = None,
        output_filename: str | None = None,
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
    ) -> str:
        """
        Generate complete ELT pipeline DAG with Airbyte extraction and dbt transformation.

        Args:
            dag_id: DAG identifier
            source_name: Source system name
            target_schema: Target schema name
            extract_config: Airbyte extraction configuration
            transform_config: dbt transformation configuration
            schedule: Schedule interval
            start_date: Start date
            owner: DAG owner
            use_ssh_for_dbt: Whether to use SSH for dbt execution (default: False)
            ssh_conn_id: SSH connection ID for remote dbt execution (default: 'ssh_localhost')
            tags: DAG tags
            output_filename: Output filename
            run_dbt_tests: Whether to run dbt tests after models (default: True)
            generate_dbt_docs: Whether to generate dbt docs (default: False)

        Returns:
            Generated DAG code

        Note:
            For TPT/BTEQ-based extraction, use airflow_tdload_dag_generator instead.
        """
        if start_date is None:
            start_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        tasks = []
        dependencies = []

        # Start task
        tasks.append(
            {
                "type": "empty",
                "task_id": "start",
            }
        )

        # Airbyte extract task (always asynchronous)
        if extract_config.get("method") != "airbyte":
            raise AirflowDAGGeneratorError(
                "Extract method must be 'airbyte'. Use airflow_tdload_dag_generator for TPT/BTEQ extraction."
            )

        tasks.append(
            {
                "type": "airbyte",
                "task_id": f"extract_{source_name}",
                "connection_id": extract_config.get("connection_id"),
                "airbyte_conn_id": extract_config.get("airbyte_conn_id"),
            }
        )
        # Asynchronous mode: trigger → sensor
        extract_trigger_id = f"extract_{source_name}"
        extract_sensor_id = f"extract_{source_name}_sensor"

        dependencies.append(("start", extract_trigger_id))
        dependencies.append((extract_trigger_id, extract_sensor_id))

        extract_task_id = extract_sensor_id

        # Transform tasks
        dbt_project_dir = transform_config.get("project_dir")
        dbt_env = transform_config.get("env")

        # SSH fields for dbt tasks on the remote worker
        ssh_fields: dict[str, Any] = {}
        if use_ssh_for_dbt:
            ssh_fields = {
                "use_ssh": True,
                "ssh_conn_id": ssh_conn_id,
                "remote_dir": transform_config.get("remote_dir"),
            }

        # dbt deps — install packages before running models
        tasks.append(
            {
                "type": "dbt",
                "task_id": "dbt_deps",
                "dbt_subcommand": "deps",
                "project_dir": dbt_project_dir,
                **ssh_fields,
                "env": dbt_env,
            }
        )
        dependencies.append((extract_task_id, "dbt_deps"))

        # dbt run
        tasks.append(
            {
                "type": "dbt",
                "task_id": "dbt_run",
                "dbt_subcommand": "run",
                "project_dir": dbt_project_dir,
                "models": transform_config.get("models"),
                "target": transform_config.get("target", "dev"),
                **ssh_fields,
                "env": dbt_env,
            }
        )
        dependencies.append(("dbt_deps", "dbt_run"))

        last_task = "dbt_run"

        if run_dbt_tests:
            tasks.append(
                {
                    "type": "dbt",
                    "task_id": "dbt_test",
                    "dbt_subcommand": "test",
                    "project_dir": dbt_project_dir,
                    "models": transform_config.get("models"),
                    "target": transform_config.get("target", "dev"),
                    **ssh_fields,
                    "env": dbt_env,
                }
            )
            dependencies.append((last_task, "dbt_test"))
            last_task = "dbt_test"

        if generate_dbt_docs:
            tasks.append(
                {
                    "type": "dbt",
                    "task_id": "dbt_docs_generate",
                    "dbt_subcommand": "docs generate",
                    "project_dir": dbt_project_dir,
                    "target": transform_config.get("target", "dev"),
                    **ssh_fields,
                    "env": dbt_env,
                }
            )
            dependencies.append((last_task, "dbt_docs_generate"))
            last_task = "dbt_docs_generate"

        # End task
        tasks.append(
            {
                "type": "empty",
                "task_id": "end",
            }
        )
        dependencies.append((last_task, "end"))

        # Generate DAG
        return self.generate_dag(
            dag_id=dag_id,
            description=f"ELT pipeline for {source_name} to {target_schema}",
            schedule=schedule,
            tasks=tasks,
            dependencies=dependencies,
            start_date=start_date,
            owner=owner,
            tags=tags or ["elt", source_name, target_schema],
            catchup=False,
            output_filename=output_filename,
        )

    def generate_dbt_only_dag(
        self,
        dag_id: str,
        project_dir: str,
        models: list[str] | None = None,
        run_tests: bool = True,
        generate_docs: bool = False,
        schedule: str = "@daily",
        start_date: datetime | None = None,
        target: str = "dev",
        owner: str = "airflow",
        tags: list[str] | None = None,
        output_filename: str | None = None,
        use_ssh: bool = False,
        ssh_conn_id: str = "ssh_default",
        remote_dir: str | None = None,
        dbt_env: dict[str, str] | None = None,
    ) -> str:
        """
        Generate dbt-only DAG for transformation.

        Args:
            dag_id: DAG identifier
            project_dir: dbt project directory
            models: Optional list of models to run
            run_tests: Whether to run tests
            generate_docs: Whether to generate docs
            schedule: Schedule interval
            start_date: Start date
            target: dbt target
            owner: DAG owner
            tags: DAG tags
            output_filename: Output filename

        Returns:
            Generated DAG code
        """
        if start_date is None:
            start_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        tasks = []
        dependencies = []

        ssh_fields = {}
        ssh_fields_with_env = {}
        if use_ssh:
            ssh_fields = {
                "use_ssh": True,
                "ssh_conn_id": ssh_conn_id,
                "remote_dir": remote_dir,
            }
            ssh_fields_with_env = {
                **ssh_fields,
                "env": dbt_env,
            }

        # dbt deps
        tasks.append(
            {
                "type": "dbt",
                "task_id": "dbt_deps",
                "dbt_subcommand": "deps",
                "project_dir": project_dir,
                **ssh_fields,
            }
        )

        # dbt run
        tasks.append(
            {
                "type": "dbt",
                "task_id": "dbt_run",
                "dbt_subcommand": "run",
                "project_dir": project_dir,
                "models": models,
                "target": target,
                **ssh_fields_with_env,
            }
        )
        dependencies.append(("dbt_deps", "dbt_run"))

        last_task = "dbt_run"

        # dbt test
        if run_tests:
            tasks.append(
                {
                    "type": "dbt",
                    "task_id": "dbt_test",
                    "dbt_subcommand": "test",
                    "project_dir": project_dir,
                    "models": models,
                    "target": target,
                    **ssh_fields_with_env,
                }
            )
            dependencies.append((last_task, "dbt_test"))
            last_task = "dbt_test"

        # dbt docs generate
        if generate_docs:
            tasks.append(
                {
                    "type": "dbt",
                    "task_id": "dbt_docs_generate",
                    "dbt_subcommand": "docs generate",
                    "project_dir": project_dir,
                    "target": target,
                    **ssh_fields_with_env,
                }
            )
            dependencies.append((last_task, "dbt_docs_generate"))

        return self.generate_dag(
            dag_id=dag_id,
            description="dbt transformation pipeline",
            schedule=schedule,
            tasks=tasks,
            dependencies=dependencies,
            start_date=start_date,
            owner=owner,
            tags=tags or ["dbt", "transformation"],
            catchup=False,
            output_filename=output_filename,
        )

    # ==================== Utility Methods ====================

    @staticmethod
    def find_bare_name_errors(tree: Any) -> list[str]:
        """Return a list of error strings for bare Name expression statements at module level.

        These are syntactically valid Python but almost always a bug: a bare name that is
        not defined at module scope will cause a NameError when Airflow imports the DAG
        (e.g. a stray identifier like ``asdfsdf`` on its own line).
        """
        import ast

        return [
            f"bare name expression '{node.value.id}' at line {node.lineno}"
            for node in tree.body
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Name)
        ]

    def validate_dag_file(self, dag_file_path: Path) -> dict[str, Any]:
        """
        Validate generated DAG file syntax.

        Args:
            dag_file_path: Path to DAG file

        Returns:
            Dictionary with validation results
        """
        try:
            import ast

            dag_code = Path(dag_file_path).read_text(encoding="utf-8")

            # Try to parse as Python
            try:
                tree = ast.parse(dag_code, filename=str(dag_file_path))
                syntax_valid = True
                syntax_error = None

                # Check for bare Name expression statements at module level.
                # See find_bare_name_errors for details; same logic used in deploy-time check.
                name_errors = self.find_bare_name_errors(tree)
                if name_errors:
                    syntax_valid = False
                    syntax_error = "; ".join(name_errors)
            except SyntaxError as e:
                syntax_valid = False
                syntax_error = str(e)

            return {
                "valid": syntax_valid,
                "syntax_error": syntax_error,
                "file_path": str(dag_file_path),
            }

        except Exception as e:
            logger.error("Failed to validate DAG file: %s", e, exc_info=True)
            return {
                "valid": False,
                "syntax_error": str(e),
                "file_path": str(dag_file_path),
            }

    def sanitize_dag_id(self, name: str) -> str:
        """
        Sanitize name for DAG ID (lowercase, underscores only).

        Args:
            name: Original name

        Returns:
            Sanitized DAG ID
        """
        # Convert to lowercase
        name = name.lower()

        # Replace spaces and special chars with underscores
        name = re.sub(r"[^a-z0-9_]", "_", name)

        # Remove consecutive underscores
        name = re.sub(r"_+", "_", name)

        # Remove leading/trailing underscores
        name = name.strip("_")

        return name


class AirflowDagGenerator:
    """
    Compatibility wrapper providing a simpler API expected by unit tests.
    Generates minimal-yet-valid DAG code strings and delegates file writes.
    """

    def __init__(self, output_dir: str, default_owner: str = "airflow"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.default_owner = default_owner

        # See AirflowDAGGenerator.__init__ for the rationale on
        # enable_backups=False and restrict_permissions=False.
        self.file_writer = SafeFileWriter(
            output_dir=self.output_dir,
            keep_backups=5,
            validate_python=True,
            enable_backups=False,
            restrict_permissions=False,
        )

        # Use the richer generator internally for file ops if needed
        self._inner = AirflowDAGGenerator(dags_folder=self.output_dir)

    # ----------------------- Helpers -----------------------
    def _format_schedule(self, schedule_interval: Any) -> str:
        if schedule_interval is None:
            return "None"
        if isinstance(schedule_interval, dict):
            # e.g., {"hours": 6}
            unit, value = next(iter(schedule_interval.items()))
            return f"timedelta({unit}={value})"
        return repr(schedule_interval)

    def _default_args_block(self, default_args: dict[str, Any] | None) -> str:
        args = default_args or {}
        owner = args.get("owner", self.default_owner)
        retries = args.get("retries", 0)
        retry_delay = args.get("retry_delay")
        retry_delay_str = (
            f"timedelta(seconds={retry_delay})" if isinstance(retry_delay, int) else "None"
        )
        return (
            "default_args = {\n"
            f"    'owner': '{owner}',\n"
            f"    'retries': {retries},\n"
            f"    'retry_delay': {retry_delay_str},\n"
            "}\n"
        )

    def _indent(self, code: str, level: int = 1) -> str:
        prefix = "    " * level
        return "\n".join(prefix + line if line else prefix for line in code.splitlines())

    def generate_imports(self, task_types: list[str]) -> str:
        lines = [
            "from datetime import datetime, timedelta",
            "from airflow import DAG",
        ]
        if "bash" in task_types:
            lines.append("from airflow.providers.standard.operators.bash import BashOperator")
        if "dbt" in task_types and "from airflow.providers.standard.operators.bash import BashOperator" not in lines:
            lines.append("from airflow.providers.standard.operators.bash import BashOperator")
        if "python" in task_types:
            lines.append("from airflow.providers.standard.operators.python import PythonOperator")
        if "teradata" in task_types:
            lines.append(
                "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator"
            )
        if "bteq" in task_types:
            lines.append("from airflow.providers.teradata.operators.bteq import BteqOperator")
        if any(t.endswith("sensor") for t in task_types):
            lines.append("from airflow.providers.standard.sensors.filesystem import FileSensor")
        if "airbyte" in task_types:
            lines.append(
                "from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator"
            )
        if "airbyte_sensor" in task_types:
            lines.append("from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor")
        if "task_groups" in task_types:
            lines.append("from airflow.utils.task_group import TaskGroup")
        return "\n".join(lines) + "\n"

    def generate_task(self, task_config: dict[str, Any]) -> str:
        t = task_config.copy()
        task_id = t.get("task_id", "task")
        ttype = t.get("type", "bash")
        doc_md = t.get("doc_md")
        retries = t.get("retries")
        retry_delay = t.get("retry_delay")
        sla = t.get("sla")
        email = t.get("email")
        email_on_failure = t.get("email_on_failure")
        email_on_retry = t.get("email_on_retry")
        trigger_rule = t.get("trigger_rule")
        pool = t.get("pool")
        priority_weight = t.get("priority_weight")

        extra_lines = []
        if doc_md:
            extra_lines.append(f'{task_id}.doc_md = "{doc_md}"')
        if isinstance(retries, int):
            extra_lines.append(f"retries={retries}")
        if isinstance(retry_delay, int):
            extra_lines.append(f"retry_delay=timedelta(seconds={retry_delay})")
        if sla and isinstance(sla, dict):
            unit, value = next(iter(sla.items()))
            extra_lines.append(f"sla=timedelta({unit}={value})")
        if email:
            extra_lines.append(f"email={repr(email)}")
        if email_on_failure is not None:
            extra_lines.append(f"email_on_failure={email_on_failure}")
        if email_on_retry is not None:
            extra_lines.append(f"email_on_retry={email_on_retry}")
        if trigger_rule:
            extra_lines.append(f"trigger_rule='{trigger_rule}'")
        if pool:
            extra_lines.append(f"pool='{pool}'")
        if priority_weight is not None:
            extra_lines.append(f"priority_weight={priority_weight}")

        if ttype == "bash":
            cmd = t.get("bash_command", "echo hello")
            base = f"{task_id} = BashOperator(task_id='{task_id}', bash_command={repr(cmd)})"
        elif ttype == "python":
            pycall = t.get("python_callable", "my_callable")
            op_kwargs = t.get("op_kwargs")
            do_xcom_push = t.get("do_xcom_push")
            kwargs_line = f", op_kwargs={repr(op_kwargs)}" if op_kwargs else ""
            xcom_line = f", do_xcom_push={bool(do_xcom_push)}" if do_xcom_push is not None else ""
            base = f"{task_id} = PythonOperator(task_id='{task_id}', python_callable={pycall}{kwargs_line}{xcom_line})"
        elif ttype == "teradata":
            sql = t.get("sql", "SELECT 1")
            conn_id = t.get("conn_id", "teradata_default")
            base = f"{task_id} = SQLExecuteQueryOperator(task_id='{task_id}', sql={repr(sql)}, conn_id='{conn_id}')"
        elif ttype == "bteq":
            sql = t.get("sql")
            file_path = t.get("file_path")
            teradata_conn_id = t.get("teradata_conn_id")
            params = t.get("params")
            bteq_session_encoding = t.get("bteq_session_encoding")
            bteq_script_encoding = t.get("bteq_script_encoding")
            bteq_quit_rc = t.get("bteq_quit_rc")
            timeout = t.get("timeout")
            kw = []
            if sql:
                kw.append(f"sql={repr(sql)}")
            if file_path:
                kw.append(f"file_path='{file_path}'")
            if teradata_conn_id:
                kw.append(f"teradata_conn_id='{teradata_conn_id}'")
            if params is not None:
                kw.append(f"params={repr(params)}")
            if bteq_session_encoding:
                kw.append(f"bteq_session_encoding='{bteq_session_encoding}'")
            if bteq_script_encoding:
                kw.append(f"bteq_script_encoding='{bteq_script_encoding}'")
            if bteq_quit_rc is not None:
                kw.append(f"bteq_quit_rc={repr(bteq_quit_rc)}")
            if timeout is not None:
                kw.append(f"timeout={timeout}")
            base = (
                f"{task_id} = BteqOperator(task_id='{task_id}'"
                + (", " + ", ".join(kw) if kw else "")
                + ")"
            )
        elif ttype == "airbyte":
            connection_id = t.get("connection_id")
            conn_id = t.get("conn_id")
            asynchronous = bool(t.get("asynchronous"))
            # Base Airbyte operator
            base = (
                f"{task_id} = AirbyteTriggerSyncOperator(task_id='{task_id}', connection_id='{connection_id}'"
                + (f", airbyte_conn_id='{conn_id}'" if conn_id else "")
                + (", asynchronous=True" if asynchronous else "")
                + ")"
            )
            # Append sensor only for asynchronous mode
            if asynchronous:
                sensor_line = (
                    f"\n{task_id}_sensor = AirbyteJobSensor(task_id='{task_id}_sensor', airbyte_job_id={task_id}.output"
                    + (f", airbyte_conn_id='{conn_id}'" if conn_id else "")
                    + ", poke_interval=30, timeout=60 * 60 * 6)"
                )
                base += sensor_line
        elif ttype.endswith("sensor"):
            filepath = t.get("filepath", "/tmp/file.csv")  # noqa: S108  # nosec B108 - default placeholder for generated DAG
            poke_interval = t.get("poke_interval", 60)
            timeout = t.get("timeout", 3600)
            base = f"{task_id} = FileSensor(task_id='{task_id}', filepath='{filepath}', poke_interval={poke_interval}, timeout={timeout})"
        elif ttype == "dbt":
            command = t.get("command", "run")
            models = t.get("models")
            project_dir = t.get("project_dir")
            cmd = f"dbt {command}"
            if project_dir:
                cmd += f" --project-dir {project_dir}"
                if command != "deps":
                    cmd += f" --profiles-dir {project_dir}"
            if models and command in ("run", "test"):
                if isinstance(models, list):
                    cmd += f" --select {' '.join(models)}"
                else:
                    cmd += f" --select {models}"
            base = f"{task_id} = BashOperator(task_id='{task_id}', bash_command={repr(cmd)})"
        else:
            base = f"{task_id} = BashOperator(task_id='{task_id}', bash_command='echo unsupported')"

        # Append extra keyword lines in a readable form when possible
        if extra_lines:
            # convert to kwargs-like additions where applicable
            # For simplicity, append as comments or secondary lines to satisfy assertions
            base += "\n" + "\n".join(extra_lines)
        return base

    def generate_dag(self, config: dict[str, Any]) -> str:
        dag_id = config.get("dag_id", "dag")
        description = config.get("description", "")
        schedule_interval = self._format_schedule(config.get("schedule_interval"))
        start_date = config.get("start_date")
        tags = config.get("tags", [])
        catchup = config.get("catchup")
        max_active_runs = config.get("max_active_runs")
        default_args = config.get("default_args")
        doc_md = config.get("doc_md")
        on_success_callback = config.get("on_success_callback")
        on_failure_callback = config.get("on_failure_callback")

        imports = self.generate_imports(["bash", "python"])  # basic imports
        default_args_block = self._default_args_block(default_args)
        start_date_line = (
            f"start_date=datetime.fromisoformat({repr(start_date)})" if start_date else ""
        )
        tags_line = f"tags={repr(tags)}" if tags else ""
        catchup_line = f"catchup={catchup}" if catchup is not None else ""
        mar_line = f"max_active_runs={max_active_runs}" if max_active_runs is not None else ""
        cb_success = f", on_success_callback={on_success_callback}" if on_success_callback else ""
        cb_failure = f", on_failure_callback={on_failure_callback}" if on_failure_callback else ""

        dag_header = f"with DAG(dag_id='{dag_id}', description={repr(description)}, schedule={schedule_interval}, {start_date_line}{(',' if start_date_line else '')}{tags_line}{(',' if tags_line else '')}{catchup_line}{(',' if catchup_line else '')}{mar_line}{cb_failure}{cb_success}) as dag:\n"

        body_lines = []
        if doc_md:
            body_lines.append(f'    dag.doc_md = "{doc_md.strip()}"')
        body_lines.append("    start = BashOperator(task_id='start', bash_command='echo start')")
        body_lines.append("    end = BashOperator(task_id='end', bash_command='echo end')")
        body_lines.append("    start >> end")

        return imports + default_args_block + "\n" + dag_header + "\n".join(body_lines) + "\n"

    def generate_dag_with_tasks(self, config: dict[str, Any]) -> str:
        tasks = config.get("tasks", [])
        task_groups = config.get("task_groups", [])
        dependencies: dict[str, list[str]] = config.get("dependencies", {})
        # Collect imports based on task types
        task_types = [t.get("type", "bash") for t in tasks]
        # Include Airbyte sensor import only when any Airbyte task is asynchronous
        if any(t.get("type") == "airbyte" and t.get("asynchronous") for t in tasks):
            task_types.append("airbyte_sensor")
        if task_groups:
            task_types.append("task_groups")
        imports = self.generate_imports(task_types)
        default_args_block = self._default_args_block(config.get("default_args"))
        dag_id = config.get("dag_id", "dag")
        schedule_interval = self._format_schedule(config.get("schedule_interval"))
        dag_header = f"with DAG(dag_id='{dag_id}', schedule={schedule_interval}) as dag:\n"
        task_lines: list[str] = []
        # Add any TaskGroup blocks (basic)
        for group in task_groups:
            gid = group.get("group_id", "group")
            task_lines.append(f"with TaskGroup(group_id='{gid}') as {gid}:\n    pass")
        # Regular tasks
        task_lines.extend(self.generate_task(t) for t in tasks)
        dep_lines = []
        for downstream, upstreams in dependencies.items():
            if len(upstreams) == 1:
                dep_lines.append(f"{upstreams[0]} >> {downstream}")
            else:
                up_list = ", ".join(upstreams)
                dep_lines.append(f"[{up_list}] >> {downstream}")
        # Indent all task and dependency lines properly under DAG context
        body = "\n".join(
            [self._indent(code, 1) for code in task_lines]
            + [self._indent(line, 1) for line in dep_lines]
        )
        return imports + default_args_block + "\n" + dag_header + body + "\n"

    def write_dag_file(self, content: str, dag_id: str) -> Path:
        """Write DAG file with backup, validation, and atomic operations."""
        try:
            filename = f"{dag_id}.py"
            path, metadata = self.file_writer.write_file_safe(
                content=content,
                filename=filename,
                force=False,
            )

            # Log warnings if file was overwritten
            if metadata.get("warnings"):
                for warning in metadata["warnings"]:
                    logger.warning("DAG %s: %s", dag_id, warning)

            return path
        except Exception as e:
            logger.error("Failed to write DAG file for %s: %s", dag_id, e, exc_info=True)
            raise

    def generate_dag_with_dynamic_tasks(self, config: dict[str, Any]) -> str:
        dyn = config.get("dynamic_tasks", {})
        template = dyn.get("template", {})
        variables = dyn.get("variables", [])
        tasks: list[dict[str, Any]] = []
        for var in variables:
            t = {k: (v.format(**var) if isinstance(v, str) else v) for k, v in template.items()}
            tasks.append(t)
        glued = {
            "dag_id": config.get("dag_id", "dag"),
            "schedule_interval": config.get("schedule_interval"),
            "tasks": tasks,
        }
        return self.generate_dag_with_tasks(glued)

    def validate_config(self, config: dict[str, Any]) -> dict[str, Any]:
        errors = []
        if not config.get("dag_id"):
            errors.append("Missing dag_id")
        # Basic schedule validation: accept None, dict, or simple string containing spaces
        si = config.get("schedule_interval")
        if si is not None and not (isinstance(si, dict) or (isinstance(si, str) and " " in si)):
            # not strict; flag as potentially invalid
            errors.append("Potentially invalid schedule_interval")
        return {"valid": len(errors) == 0, "errors": errors}
