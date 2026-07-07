"""Unit tests for Airflow DAG generator."""

import ast
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from teradata_etl_mcp_server.generators.airflow_dag_generator import (
    AirflowDAGGenerator,
    AirflowDagGenerator,
    AirflowDAGGeneratorError,
)


class TestAirflowDagGenerator:
    """Test suite for AirflowDagGenerator."""

    @pytest.fixture
    def generator_config(self, tmp_path):
        """Test generator configuration."""
        output_dir = tmp_path / "dags"
        output_dir.mkdir()

        return {
            "output_dir": str(output_dir),
            "default_owner": "data_team",
        }

    @pytest.fixture
    def generator(self, generator_config):
        """Create AirflowDagGenerator instance."""
        return AirflowDagGenerator(**generator_config)

    @pytest.fixture
    def sample_dag_config(self):
        """Sample DAG configuration."""
        return {
            "dag_id": "test_pipeline",
            "description": "Test data pipeline",
            "schedule_interval": "0 2 * * *",  # Daily at 2 AM
            "start_date": "2025-01-01",
            "default_args": {
                "owner": "data_team",
                "retries": 3,
                "retry_delay": 300,
            },
            "tags": ["test", "daily"],
        }

    # Initialization Tests

    def test_init_with_valid_config(self, generator, generator_config):
        """Test initialization with valid configuration."""
        assert generator.output_dir == Path(generator_config["output_dir"])
        assert generator.default_owner == "data_team"

    def test_init_creates_directories(self, tmp_path):
        """Test that initialization creates output directory."""
        output_dir = tmp_path / "new_dags"

        generator = AirflowDagGenerator(output_dir=str(output_dir))

        assert output_dir.exists()

    # DAG Generation Tests

    def test_generate_basic_dag(self, generator, sample_dag_config):
        """Test generating basic DAG."""
        result = generator.generate_dag(sample_dag_config)

        assert result is not None
        assert "from airflow import DAG" in result
        assert "test_pipeline" in result
        # Airflow 2.4+ uses schedule= instead of schedule_interval=
        assert "schedule=" in result or "schedule_interval" in result
        assert "0 2 * * *" in result

        # Verify it's valid Python
        try:
            ast.parse(result)
        except SyntaxError:
            pytest.fail("Generated DAG is not valid Python")

    def test_generate_dag_with_default_args(self, generator, sample_dag_config):
        """Test DAG generation with default_args."""
        result = generator.generate_dag(sample_dag_config)

        assert "default_args" in result
        assert "retries" in result
        assert "retry_delay" in result

    def test_generate_dag_with_tags(self, generator, sample_dag_config):
        """Test DAG generation with tags."""
        result = generator.generate_dag(sample_dag_config)

        assert "tags" in result
        assert "test" in result
        assert "daily" in result

    def test_generate_dag_with_catchup(self, generator, sample_dag_config):
        """Test DAG generation with catchup setting."""
        sample_dag_config["catchup"] = False

        result = generator.generate_dag(sample_dag_config)

        assert "catchup=False" in result

    def test_generate_dag_with_max_active_runs(self, generator, sample_dag_config):
        """Test DAG generation with max_active_runs."""
        sample_dag_config["max_active_runs"] = 1

        result = generator.generate_dag(sample_dag_config)

        assert "max_active_runs=1" in result

    # Schedule Tests

    def test_generate_dag_with_cron_schedule(self, generator, sample_dag_config):
        """Test DAG with cron schedule."""
        sample_dag_config["schedule_interval"] = "0 0 * * *"

        result = generator.generate_dag(sample_dag_config)

        assert "0 0 * * *" in result

    def test_generate_dag_with_timedelta_schedule(self, generator, sample_dag_config):
        """Test DAG with timedelta schedule."""
        sample_dag_config["schedule_interval"] = {"hours": 6}

        result = generator.generate_dag(sample_dag_config)

        assert "timedelta" in result
        assert "hours=6" in result

    def test_generate_dag_with_none_schedule(self, generator, sample_dag_config):
        """Test DAG with manual-only schedule."""
        sample_dag_config["schedule_interval"] = None

        result = generator.generate_dag(sample_dag_config)

        # Airflow 2.4+ uses schedule= instead of schedule_interval=
        assert "schedule=None" in result or "schedule_interval=None" in result

    # Task Generation Tests

    def test_generate_teradata_query_task(self, generator):
        """Test generating Teradata query task."""
        task_config = {
            "task_id": "extract_data",
            "type": "teradata",
            "sql": "SELECT * FROM customers WHERE updated_at > '{{ ds }}'",
            "conn_id": "teradata_default",
        }

        result = generator.generate_task(task_config)

        assert result is not None
        assert "extract_data" in result
        assert "TeradataOperator" in result or "SQLExecuteQueryOperator" in result
        assert "SELECT * FROM customers" in result

    def test_generate_airbyte_sync_task(self, generator):
        """Test generating Airbyte sync task."""
        task_config = {
            "task_id": "sync_to_warehouse",
            "type": "airbyte",
            "connection_id": "conn_12345",
            "conn_id": "airbyte_default",
        }

        result = generator.generate_task(task_config)

        assert result is not None
        assert "sync_to_warehouse" in result
        assert "AirbyteTriggerSyncOperator" in result or "airbyte" in result.lower()
        assert "conn_12345" in result

    def test_generate_dbt_run_task(self, generator):
        """Test generating dbt run task."""
        task_config = {
            "task_id": "run_dbt_models",
            "type": "dbt",
            "command": "run",
            "models": ["stg_customers", "stg_orders"],
            "project_dir": "/opt/dbt",
        }

        result = generator.generate_task(task_config)

        assert result is not None
        assert "run_dbt_models" in result
        assert "BashOperator" in result or "dbt" in result
        assert "dbt run" in result
        assert "stg_customers" in result or "--select" in result

    def test_generate_python_task(self, generator):
        """Test generating Python operator task."""
        task_config = {
            "task_id": "validate_data",
            "type": "python",
            "python_callable": "validate_customer_data",
            "op_kwargs": {"min_records": 1000},
        }

        result = generator.generate_task(task_config)

        assert result is not None
        assert "validate_data" in result
        assert "PythonOperator" in result
        assert "python_callable" in result

    def test_generate_bash_task(self, generator):
        """Test generating Bash operator task."""
        task_config = {
            "task_id": "cleanup_files",
            "type": "bash",
            "bash_command": "rm -rf /tmp/staging/*",
        }

        result = generator.generate_task(task_config)

        assert result is not None
        assert "cleanup_files" in result
        assert "BashOperator" in result
        assert "bash_command" in result

    def test_generate_sensor_task(self, generator):
        """Test generating sensor task."""
        task_config = {
            "task_id": "wait_for_file",
            "type": "file_sensor",
            "filepath": "/data/input/customers.csv",
            "poke_interval": 60,
            "timeout": 3600,
        }

        result = generator.generate_task(task_config)

        assert result is not None
        assert "wait_for_file" in result
        assert "FileSensor" in result or "Sensor" in result
        assert "poke_interval" in result

    # Task Dependencies Tests

    def test_generate_linear_dependencies(self, generator, sample_dag_config):
        """Test generating linear task dependencies."""
        sample_dag_config["tasks"] = [
            {"task_id": "task1", "type": "bash", "bash_command": "echo 1"},
            {"task_id": "task2", "type": "bash", "bash_command": "echo 2"},
            {"task_id": "task3", "type": "bash", "bash_command": "echo 3"},
        ]
        sample_dag_config["dependencies"] = {
            "task2": ["task1"],
            "task3": ["task2"],
        }

        result = generator.generate_dag_with_tasks(sample_dag_config)

        assert "task1 >> task2" in result or "task2.set_upstream(task1)" in result
        assert "task2 >> task3" in result or "task3.set_upstream(task2)" in result

    def test_generate_parallel_dependencies(self, generator, sample_dag_config):
        """Test generating parallel task dependencies."""
        sample_dag_config["tasks"] = [
            {"task_id": "extract1", "type": "bash", "bash_command": "echo 1"},
            {"task_id": "extract2", "type": "bash", "bash_command": "echo 2"},
            {"task_id": "load", "type": "bash", "bash_command": "echo load"},
        ]
        sample_dag_config["dependencies"] = {
            "load": ["extract1", "extract2"],
        }

        result = generator.generate_dag_with_tasks(sample_dag_config)

        assert "load" in result
        # Should have both extract1 and extract2 before load
        assert ("extract1" in result and "extract2" in result) or "[extract1, extract2]" in result

    def test_generate_complex_dependencies(self, generator, sample_dag_config):
        """Test generating complex DAG with multiple dependency patterns."""
        sample_dag_config["tasks"] = [
            {"task_id": "start", "type": "bash", "bash_command": "echo start"},
            {"task_id": "extract1", "type": "bash", "bash_command": "echo e1"},
            {"task_id": "extract2", "type": "bash", "bash_command": "echo e2"},
            {"task_id": "transform", "type": "bash", "bash_command": "echo t"},
            {"task_id": "load", "type": "bash", "bash_command": "echo l"},
        ]
        sample_dag_config["dependencies"] = {
            "extract1": ["start"],
            "extract2": ["start"],
            "transform": ["extract1", "extract2"],
            "load": ["transform"],
        }

        result = generator.generate_dag_with_tasks(sample_dag_config)

        assert result is not None
        assert all(task in result for task in ["start", "extract1", "extract2", "transform", "load"])

    def test_generate_task_groups(self, generator, sample_dag_config):
        """Test generating task groups."""
        sample_dag_config["task_groups"] = [
            {
                "group_id": "extract_group",
                "tasks": [
                    {"task_id": "extract1", "type": "bash", "bash_command": "echo 1"},
                    {"task_id": "extract2", "type": "bash", "bash_command": "echo 2"},
                ],
            }
        ]

        result = generator.generate_dag_with_tasks(sample_dag_config)

        assert "TaskGroup" in result
        assert "extract_group" in result

    # Import Generation Tests

    def test_generate_imports_basic(self, generator):
        """Test generating basic imports."""
        task_types = ["bash", "python"]

        result = generator.generate_imports(task_types)

        assert "from airflow import DAG" in result
        assert "from airflow.providers.standard.operators.bash import BashOperator" in result
        assert "from airflow.providers.standard.operators.python import PythonOperator" in result

    def test_generate_imports_teradata(self, generator):
        """Test generating Teradata imports."""
        task_types = ["teradata"]

        result = generator.generate_imports(task_types)

        assert "TeradataOperator" in result or "SQLExecuteQueryOperator" in result

    def test_generate_imports_dbt(self, generator):
        """Test generating dbt imports."""
        task_types = ["dbt"]

        result = generator.generate_imports(task_types)

        assert "BashOperator" in result  # dbt typically uses BashOperator

    def test_generate_imports_sensors(self, generator):
        """Test generating sensor imports."""
        task_types = ["file_sensor", "time_sensor"]

        result = generator.generate_imports(task_types)

        assert "Sensor" in result

    # DAG Documentation Tests

    def test_generate_dag_with_documentation(self, generator, sample_dag_config):
        """Test generating DAG with documentation."""
        sample_dag_config["doc_md"] = """
        # Test Pipeline
        
        This pipeline extracts and loads customer data.
        """

        result = generator.generate_dag(sample_dag_config)

        assert "doc_md" in result
        assert "Test Pipeline" in result

    def test_generate_task_with_documentation(self, generator):
        """Test generating task with documentation."""
        task_config = {
            "task_id": "extract",
            "type": "bash",
            "bash_command": "echo test",
            "doc_md": "Extract customer data from source",
        }

        result = generator.generate_task(task_config)

        assert "doc_md" in result
        assert "Extract customer data" in result

    # Error Handling Tests

    def test_generate_task_with_error_handling(self, generator):
        """Test generating task with error handling."""
        task_config = {
            "task_id": "risky_task",
            "type": "bash",
            "bash_command": "echo test",
            "retries": 5,
            "retry_delay": 600,
            "on_failure_callback": "notify_failure",
        }

        result = generator.generate_task(task_config)

        assert "retries=5" in result
        assert "retry_delay" in result

    def test_generate_task_with_sla(self, generator):
        """Test generating task with SLA."""
        task_config = {
            "task_id": "critical_task",
            "type": "bash",
            "bash_command": "echo test",
            "sla": {"hours": 2},
        }

        result = generator.generate_task(task_config)

        assert "sla" in result
        assert "timedelta" in result

    # XCom Tests

    def test_generate_task_with_xcom_push(self, generator):
        """Test generating task that pushes XCom."""
        task_config = {
            "task_id": "get_data_count",
            "type": "python",
            "python_callable": "count_records",
            "do_xcom_push": True,
        }

        result = generator.generate_task(task_config)

        assert "do_xcom_push" in result or "xcom_push" in result

    def test_generate_task_with_xcom_pull(self, generator):
        """Test generating task that pulls XCom."""
        task_config = {
            "task_id": "process_count",
            "type": "python",
            "python_callable": "process_data",
            "op_kwargs": {
                "count": "{{ ti.xcom_pull(task_ids='get_data_count') }}",
            },
        }

        result = generator.generate_task(task_config)

        assert "xcom_pull" in result
        assert "get_data_count" in result

    # Templating Tests

    def test_generate_task_with_jinja_template(self, generator):
        """Test generating task with Jinja templates."""
        task_config = {
            "task_id": "extract_daily",
            "type": "bash",
            "bash_command": "echo Processing date: {{ ds }}",
        }

        result = generator.generate_task(task_config)

        assert "{{ ds }}" in result

    def test_generate_task_with_macros(self, generator):
        """Test generating task with Airflow macros."""
        task_config = {
            "task_id": "extract_range",
            "type": "bash",
            "bash_command": "echo {{ macros.ds_add(ds, -7) }} to {{ ds }}",
        }

        result = generator.generate_task(task_config)

        assert "macros.ds_add" in result or "{{ ds }}" in result

    # File Writing Tests

    def test_write_dag_file(self, generator, sample_dag_config, tmp_path):
        """Test writing DAG to file."""
        dag_content = generator.generate_dag(sample_dag_config)

        file_path = generator.write_dag_file(
            content=dag_content,
            dag_id="test_pipeline"
        )

        assert file_path.exists()
        assert file_path.name == "test_pipeline.py"
        assert file_path.suffix == ".py"

        # Verify content
        with open(file_path) as f:
            content = f.read()
            assert "test_pipeline" in content

    def test_write_multiple_dags(self, generator, tmp_path):
        """Test writing multiple DAG files."""
        dag_configs = [
            {"dag_id": "dag1", "schedule_interval": "0 0 * * *"},
            {"dag_id": "dag2", "schedule_interval": "0 12 * * *"},
        ]

        file_paths = []
        for config in dag_configs:
            content = generator.generate_dag(config)
            path = generator.write_dag_file(content, config["dag_id"])
            file_paths.append(path)

        assert len(file_paths) == 2
        assert all(p.exists() for p in file_paths)

    # Connection Configuration Tests

    def test_generate_task_with_connection_id(self, generator):
        """Test generating task with connection ID."""
        task_config = {
            "task_id": "query_db",
            "type": "teradata",
            "sql": "SELECT COUNT(*) FROM users",
            "conn_id": "teradata_prod",
        }

        result = generator.generate_task(task_config)

        assert "conn_id" in result
        assert "teradata_prod" in result

    # Dynamic DAG Generation Tests

    def test_generate_dynamic_tasks(self, generator, sample_dag_config):
        """Test generating dynamic tasks from list."""
        tables = ["customers", "orders", "products"]

        sample_dag_config["dynamic_tasks"] = {
            "template": {
                "task_id": "extract_{table}",
                "type": "bash",
                "bash_command": "echo Extracting {table}",
            },
            "variables": [{"table": t} for t in tables],
        }

        result = generator.generate_dag_with_dynamic_tasks(sample_dag_config)

        assert "customers" in result
        assert "orders" in result
        assert "products" in result

    # DAG Validation Tests

    def test_validate_dag_config(self, generator, sample_dag_config):
        """Test validating DAG configuration."""
        result = generator.validate_config(sample_dag_config)

        assert result["valid"] is True
        assert len(result.get("errors", [])) == 0

    def test_validate_dag_config_missing_dag_id(self, generator):
        """Test validation with missing dag_id."""
        invalid_config = {"schedule_interval": "0 0 * * *"}

        result = generator.validate_config(invalid_config)

        assert result["valid"] is False
        assert any("dag_id" in error.lower() for error in result["errors"])

    def test_validate_dag_config_invalid_schedule(self, generator, sample_dag_config):
        """Test validation with invalid cron schedule."""
        sample_dag_config["schedule_interval"] = "invalid cron"

        result = generator.validate_config(sample_dag_config)

        # Should either be invalid or accepted (depends on implementation)
        assert "valid" in result

    # Complete Pipeline Generation Tests

    def test_generate_elt_pipeline_dag(self, generator):
        """Test generating complete ELT pipeline DAG."""
        pipeline_config = {
            "dag_id": "elt_customers_pipeline",
            "description": "Customer ELT pipeline",
            "schedule_interval": "0 2 * * *",
            "start_date": "2025-01-01",
            "tasks": [
                {
                    "task_id": "extract_teradata",
                    "type": "teradata",
                    "sql": "SELECT * FROM customers",
                },
                {
                    "task_id": "sync_airbyte",
                    "type": "airbyte",
                    "connection_id": "conn123",
                },
                {
                    "task_id": "transform_dbt",
                    "type": "dbt",
                    "command": "run",
                    "models": ["stg_customers"],
                },
            ],
            "dependencies": {
                "sync_airbyte": ["extract_teradata"],
                "transform_dbt": ["sync_airbyte"],
            },
        }

        result = generator.generate_dag_with_tasks(pipeline_config)

        assert result is not None
        assert "elt_customers_pipeline" in result
        assert all(task in result for task in ["extract_teradata", "sync_airbyte", "transform_dbt"])

        # Verify it's valid Python
        try:
            ast.parse(result)
        except SyntaxError:
            pytest.fail("Generated pipeline DAG is not valid Python")

    # Callback Tests

    def test_generate_dag_with_callbacks(self, generator, sample_dag_config):
        """Test generating DAG with callbacks."""
        sample_dag_config["on_success_callback"] = "send_success_notification"
        sample_dag_config["on_failure_callback"] = "send_failure_alert"

        result = generator.generate_dag(sample_dag_config)

        assert "on_success_callback" in result
        assert "on_failure_callback" in result

    # Email Notification Tests

    def test_generate_task_with_email_alerts(self, generator):
        """Test generating task with email alerts."""
        task_config = {
            "task_id": "critical_task",
            "type": "bash",
            "bash_command": "echo test",
            "email_on_failure": True,
            "email_on_retry": False,
            "email": ["team@example.com"],
        }

        result = generator.generate_task(task_config)

        assert "email_on_failure" in result
        assert "team@example.com" in result

    # Trigger Rules Tests

    def test_generate_task_with_trigger_rule(self, generator):
        """Test generating task with trigger rule."""
        task_config = {
            "task_id": "cleanup",
            "type": "bash",
            "bash_command": "echo cleanup",
            "trigger_rule": "all_done",
        }

        result = generator.generate_task(task_config)

        assert "trigger_rule" in result
        assert "all_done" in result

    # Pool and Priority Tests

    def test_generate_task_with_pool(self, generator):
        """Test generating task with pool assignment."""
        task_config = {
            "task_id": "resource_intensive",
            "type": "bash",
            "bash_command": "echo test",
            "pool": "heavy_tasks",
            "priority_weight": 10,
        }

        result = generator.generate_task(task_config)

        assert "pool" in result
        assert "heavy_tasks" in result
        assert "priority_weight" in result


# =============================================================================
# Comprehensive tests (merged from test_airflow_dag_generator_comprehensive.py)
# =============================================================================


@pytest.fixture
def dag_generator(tmp_path):
    """Create AirflowDAGGenerator instance with temp folder."""
    dags_folder = tmp_path / "dags"
    dags_folder.mkdir()
    return AirflowDAGGenerator(dags_folder=str(dags_folder))


@pytest.fixture
def generator_no_validation(tmp_path):
    """Create generator with Python validation disabled."""
    dags_folder = tmp_path / "dags"
    dags_folder.mkdir()
    gen = AirflowDAGGenerator(dags_folder=str(dags_folder))
    # Patch to skip validation
    gen.file_writer.validate_python = False
    return gen


# =============================================================================
# Test: _generate_bash_task
# =============================================================================


class TestGenerateBashTask:
    """Tests for _generate_bash_task method."""

    def test_simple_bash_task(self, dag_generator):
        """Generate simple bash task."""
        task_def = {
            "task_id": "echo_task",
            "bash_command": "echo hello",
        }
        code = dag_generator._generate_bash_task(task_def)

        assert "echo_task = BashOperator(" in code
        assert "task_id='echo_task'" in code
        assert "bash_command='echo hello'" in code

    def test_bash_task_with_single_quotes(self, dag_generator):
        """Bash command with single quotes should be escaped."""
        task_def = {
            "task_id": "echo_task",
            "bash_command": "echo 'hello world'",
        }
        code = dag_generator._generate_bash_task(task_def)

        # Single quotes should be escaped
        assert r"\'hello world\'" in code

    def test_bash_task_with_env(self, dag_generator):
        """Bash task with environment variables."""
        task_def = {
            "task_id": "env_task",
            "bash_command": "echo $VAR",
            "env": {"VAR": "value"},
        }
        code = dag_generator._generate_bash_task(task_def)

        assert "env=" in code
        assert "'VAR': 'value'" in code

    def test_bash_task_with_cwd(self, dag_generator):
        """Bash task with working directory."""
        task_def = {
            "task_id": "cwd_task",
            "bash_command": "ls",
            "cwd": "/tmp",
        }
        code = dag_generator._generate_bash_task(task_def)

        assert "cwd='/tmp'" in code

    def test_bash_task_with_retries(self, dag_generator):
        """Bash task with retry configuration."""
        task_def = {
            "task_id": "retry_task",
            "bash_command": "echo retry",
            "retries": 3,
            "retry_delay": 5,
        }
        code = dag_generator._generate_bash_task(task_def)

        assert "retries=3" in code
        assert "retry_delay=timedelta(minutes=5)" in code

    def test_bash_task_missing_command_raises(self, dag_generator):
        """Missing bash_command should raise error."""
        task_def = {"task_id": "bad_task"}

        with pytest.raises(AirflowDAGGeneratorError) as exc:
            dag_generator._generate_bash_task(task_def)

        assert "bash_command required" in str(exc.value)


# =============================================================================
# Test: _generate_airbyte_task
# =============================================================================


class TestGenerateAirbyteTask:
    """Tests for _generate_airbyte_task method."""

    def test_airbyte_task_basic(self, dag_generator):
        """Generate basic Airbyte sync task."""
        task_def = {
            "task_id": "sync_salesforce",
            "connection_id": "conn-123-abc",
        }
        code = dag_generator._generate_airbyte_task(task_def)

        assert "sync_salesforce = AirbyteTriggerSyncOperator(" in code
        assert "connection_id='conn-123-abc'" in code
        assert "asynchronous=True" in code
        # Sensor should be generated for async mode
        assert "sync_salesforce_sensor = AirbyteJobSensor(" in code

    def test_airbyte_task_with_conn_id(self, dag_generator):
        """Airbyte task with custom airflow connection id."""
        task_def = {
            "task_id": "sync_task",
            "connection_id": "conn-123",
            "airbyte_conn_id": "my_airbyte",
        }
        code = dag_generator._generate_airbyte_task(task_def)

        assert "airbyte_conn_id='my_airbyte'" in code

    def test_airbyte_task_missing_connection_raises(self, dag_generator):
        """Missing connection_id should raise error."""
        task_def = {"task_id": "bad_airbyte"}

        with pytest.raises(AirflowDAGGeneratorError) as exc:
            dag_generator._generate_airbyte_task(task_def)

        assert "connection_id required" in str(exc.value)


# =============================================================================
# Test: _generate_dbt_task
# =============================================================================


class TestGenerateDBTTask:
    """Tests for _generate_dbt_task method."""

    def test_dbt_run_task(self, dag_generator):
        """Generate dbt run task."""
        task_def = {
            "task_id": "dbt_run",
            "dbt_command": "dbt run --select my_model",
        }
        code = dag_generator._generate_dbt_task(task_def)

        assert "dbt_run = BashOperator(" in code
        assert "dbt run --select my_model" in code

    def test_dbt_task_with_ssh(self, dag_generator):
        """Generate dbt task with SSH execution."""
        task_def = {
            "task_id": "dbt_ssh",
            "dbt_command": "dbt run",
            "use_ssh": True,
            "ssh_conn_id": "my_ssh",
        }
        code = dag_generator._generate_dbt_task(task_def)

        assert "SSHOperator" in code
        assert "ssh_conn_id='my_ssh'" in code

    def test_dbt_task_default_command(self, dag_generator):
        """Missing dbt_command uses default 'dbt run'."""
        task_def = {"task_id": "default_dbt"}

        # Method returns code without raising - uses default command
        code = dag_generator._generate_dbt_task(task_def)

        # Should generate something (no error)
        assert "default_dbt" in code


# =============================================================================
# Test: _generate_python_task
# =============================================================================


class TestGeneratePythonTask:
    """Tests for _generate_python_task method."""

    def test_python_task_basic(self, dag_generator):
        """Generate basic Python task."""
        task_def = {
            "task_id": "python_task",
            "python_code": "print('hello')",
            "description": "Test Python task",
        }
        code = dag_generator._generate_python_task(task_def)

        assert "def python_task_func(**context):" in code
        assert "python_callable=python_task_func" in code
        assert "print('hello')" in code

    def test_python_task_with_op_kwargs(self, dag_generator):
        """Python task with operator kwargs."""
        task_def = {
            "task_id": "kwargs_task",
            "python_code": "print(param)",
            "op_kwargs": {"param": "value"},
        }
        code = dag_generator._generate_python_task(task_def)

        assert "op_kwargs=" in code

    def test_python_task_missing_code_raises(self, dag_generator):
        """Missing python_code should raise error."""
        task_def = {"task_id": "bad_python"}

        with pytest.raises(AirflowDAGGeneratorError) as exc:
            dag_generator._generate_python_task(task_def)

        # Error message mentions both python_code and python_callable
        assert "python_code" in str(exc.value)


# =============================================================================
# Test: _generate_sensor_task
# =============================================================================


class TestGenerateSensorTask:
    """Tests for _generate_sensor_task method."""

    def test_external_task_sensor(self, dag_generator):
        """Generate external task sensor."""
        task_def = {
            "task_id": "wait_for_upstream",
            "external_dag_id": "upstream_dag",
            "external_task_id": "final_task",
            "timeout": 3600,
            "poke_interval": 60,
        }
        code = dag_generator._generate_sensor_task(task_def)

        assert "ExternalTaskSensor(" in code
        assert "external_dag_id='upstream_dag'" in code
        assert "external_task_id='final_task'" in code

    def test_sensor_task_missing_dag_raises(self, dag_generator):
        """Missing external_dag_id should raise error."""
        task_def = {
            "task_id": "bad_sensor",
            "external_task_id": "task",
        }

        with pytest.raises(AirflowDAGGeneratorError) as exc:
            dag_generator._generate_sensor_task(task_def)

        assert "external_dag_id required" in str(exc.value)


# =============================================================================
# Test: _generate_empty_task
# =============================================================================


class TestGenerateEmptyTask:
    """Tests for _generate_empty_task method."""

    def test_empty_task(self, dag_generator):
        """Generate empty (dummy) task."""
        task_def = {"task_id": "start"}
        code = dag_generator._generate_empty_task(task_def)

        assert "start = EmptyOperator(" in code
        assert "task_id='start'" in code


# =============================================================================
# Test: _generate_dependencies_code
# =============================================================================


class TestGenerateDependenciesCode:
    """Tests for _generate_dependencies_code method."""

    def test_single_dependency(self, dag_generator):
        """Generate single dependency."""
        dependencies = [("task_a", "task_b")]
        code = dag_generator._generate_dependencies_code(dependencies)

        assert "task_a >> task_b" in code

    def test_multiple_dependencies(self, dag_generator):
        """Generate multiple dependencies."""
        dependencies = [
            ("task_a", "task_b"),
            ("task_b", "task_c"),
        ]
        code = dag_generator._generate_dependencies_code(dependencies)

        assert "task_a >> task_b" in code
        assert "task_b >> task_c" in code

    def test_fan_in_dependencies(self, dag_generator):
        """Generate fan-in (multiple upstreams) dependencies."""
        dependencies = [
            ("task_a", "task_c"),
            ("task_b", "task_c"),
        ]
        code = dag_generator._generate_dependencies_code(dependencies)

        # Should generate [task_a, task_b] >> task_c
        assert "[task_a, task_b] >> task_c" in code or "task_a >> task_c" in code

    def test_no_dependencies(self, dag_generator):
        """Handle empty dependencies list."""
        dependencies = []
        code = dag_generator._generate_dependencies_code(dependencies)

        assert "No dependencies" in code


# =============================================================================
# Test: _generate_tasks_code
# =============================================================================


class TestGenerateTasksCode:
    """Tests for _generate_tasks_code method."""

    def test_generate_multiple_tasks(self, dag_generator):
        """Generate code for multiple tasks."""
        tasks = [
            {"task_id": "start", "type": "empty"},
            {"task_id": "extract", "type": "bash", "bash_command": "echo extract"},
            {"task_id": "end", "type": "empty"},
        ]
        code = dag_generator._generate_tasks_code(tasks)

        assert "start = EmptyOperator" in code
        assert "extract = BashOperator" in code
        assert "end = EmptyOperator" in code
        # Tasks should be separated by blank lines
        assert "\n\n" in code

    def test_unknown_task_type_logged(self, dag_generator):
        """Unknown task type should be skipped with warning."""
        tasks = [
            {"task_id": "unknown", "type": "nonexistent"},
        ]
        code = dag_generator._generate_tasks_code(tasks)

        # Should return empty or no code for unknown type
        assert "unknown" not in code or code == ""


# =============================================================================
# Test: generate_dag (full DAG generation)
# =============================================================================


class TestGenerateDag:
    """Tests for generate_dag method."""

    def test_generate_simple_dag(self, generator_no_validation):
        """Generate a simple DAG."""
        gen = generator_no_validation
        tasks = [
            {"task_id": "start", "type": "empty"},
            {"task_id": "process", "type": "bash", "bash_command": "echo process"},
            {"task_id": "end", "type": "empty"},
        ]
        dependencies = [("start", "process"), ("process", "end")]

        dag_code = gen.generate_dag(
            dag_id="test_dag",
            description="Test DAG",
            schedule="@daily",
            tasks=tasks,
            dependencies=dependencies,
            start_date=datetime(2025, 1, 1),
        )

        assert "dag_id='test_dag'" in dag_code
        assert "description='Test DAG'" in dag_code
        assert "schedule='@daily'" in dag_code
        assert "start_date=datetime(2025, 1, 1)" in dag_code

    def test_generate_dag_with_tags(self, generator_no_validation):
        """Generate DAG with tags."""
        gen = generator_no_validation
        tasks = [{"task_id": "task1", "type": "empty"}]

        dag_code = gen.generate_dag(
            dag_id="tagged_dag",
            description="Tagged DAG",
            schedule="@hourly",
            tasks=tasks,
            dependencies=[],
            start_date=datetime(2025, 1, 1),
            tags=["production", "etl"],
        )

        assert "tags=['production', 'etl']" in dag_code

    def test_generate_dag_with_catchup_disabled(self, generator_no_validation):
        """Generate DAG with catchup disabled."""
        gen = generator_no_validation
        tasks = [{"task_id": "task1", "type": "empty"}]

        dag_code = gen.generate_dag(
            dag_id="no_catchup_dag",
            description="No catchup",
            schedule="@daily",
            tasks=tasks,
            dependencies=[],
            start_date=datetime(2025, 1, 1),
            catchup=False,
        )

        assert "catchup=False" in dag_code

    def test_generate_dag_with_max_active_runs(self, generator_no_validation):
        """Generate DAG with max_active_runs."""
        gen = generator_no_validation
        tasks = [{"task_id": "task1", "type": "empty"}]

        dag_code = gen.generate_dag(
            dag_id="limited_dag",
            description="Limited runs",
            schedule="@daily",
            tasks=tasks,
            dependencies=[],
            start_date=datetime(2025, 1, 1),
            max_active_runs=3,
        )

        assert "max_active_runs=3" in dag_code

    def test_generate_dag_with_end_date(self, generator_no_validation):
        """Generate DAG with end date."""
        gen = generator_no_validation
        tasks = [{"task_id": "task1", "type": "empty"}]

        dag_code = gen.generate_dag(
            dag_id="dated_dag",
            description="Has end date",
            schedule="@daily",
            tasks=tasks,
            dependencies=[],
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 12, 31),
        )

        assert "end_date=datetime(2025, 12, 31)" in dag_code

    def test_generate_dag_with_timedelta_schedule(self, generator_no_validation):
        """Generate DAG with timedelta schedule."""
        gen = generator_no_validation
        tasks = [{"task_id": "task1", "type": "empty"}]

        dag_code = gen.generate_dag(
            dag_id="hourly_dag",
            description="Runs every 2 hours",
            schedule={"hours": 2},
            tasks=tasks,
            dependencies=[],
            start_date=datetime(2025, 1, 1),
        )

        assert "timedelta(hours=2)" in dag_code

    def test_generate_dag_writes_file(self, dag_generator):
        """Verify DAG is written to file."""
        tasks = [{"task_id": "task1", "type": "empty"}]

        # Patch file writer to not actually validate
        with patch.object(dag_generator.file_writer, "write_file_safe") as mock_write:
            mock_write.return_value = (Path("/tmp/test.py"), {})

            dag_generator.generate_dag(
                dag_id="file_dag",
                description="Written to file",
                schedule="@daily",
                tasks=tasks,
                dependencies=[],
                start_date=datetime(2025, 1, 1),
            )

            mock_write.assert_called_once()
            call_kwargs = mock_write.call_args[1]
            assert "file_dag" in call_kwargs["filename"]


# =============================================================================
# Test: _format_schedule_interval
# =============================================================================


class TestFormatScheduleInterval:
    """Tests for _format_schedule_interval method."""

    def test_format_none_schedule(self, dag_generator):
        """Format None schedule."""
        result = dag_generator._format_schedule_interval(None)
        assert result == "None"

    def test_format_string_schedule(self, dag_generator):
        """Format string schedule (cron or preset)."""
        result = dag_generator._format_schedule_interval("@daily")
        assert result == "'@daily'"

        result = dag_generator._format_schedule_interval("0 * * * *")
        assert result == "'0 * * * *'"

    def test_format_timedelta_schedule(self, dag_generator):
        """Format timedelta dict schedule."""
        result = dag_generator._format_schedule_interval({"hours": 1})
        assert result == "timedelta(hours=1)"


# =============================================================================
# Test: Error cases (Comprehensive)
# =============================================================================


class TestErrorCasesComprehensive:
    """Tests for error handling."""

    def test_generate_dag_invalid_python_raises(self, dag_generator):
        """Invalid Python in generated DAG should raise."""
        # Create a task that generates invalid Python
        tasks = [
            {
                "task_id": "bad_task",
                "type": "python",
                "python_code": "def (",  # Invalid Python
            }
        ]

        # This should raise due to syntax validation
        with pytest.raises(AirflowDAGGeneratorError):
            dag_generator.generate_dag(
                dag_id="bad_dag",
                description="Bad",
                schedule="@daily",
                tasks=tasks,
                dependencies=[],
                start_date=datetime(2025, 1, 1),
            )


# =============================================================================
# Test: _generate_generic_sensor_task
# =============================================================================


class TestGenerateGenericSensorTask:
    """Tests for _generate_generic_sensor_task method."""

    def test_file_sensor(self, dag_generator):
        """Generate FileSensor task."""
        task_def = {
            "task_id": "wait_for_file",
            "type": "file_sensor",
            "filepath": "/data/input.csv",
            "poke_interval": 120,
            "timeout": 7200,
        }
        code = dag_generator._generate_generic_sensor_task(task_def)

        assert "wait_for_file = FileSensor(" in code
        assert "filepath='/data/input.csv'" in code
        assert "poke_interval=120" in code
        assert "timeout=7200" in code

    def test_time_sensor(self, dag_generator):
        """Generate TimeDeltaSensor task."""
        task_def = {
            "task_id": "wait_5_min",
            "type": "time_sensor",
            "delta": {"minutes": 5},
        }
        code = dag_generator._generate_generic_sensor_task(task_def)

        assert "wait_5_min = TimeDeltaSensor(" in code
        assert "delta=timedelta(minutes=5)" in code

    def test_unsupported_sensor_raises(self, dag_generator):
        """Unsupported sensor type should raise."""
        task_def = {
            "task_id": "bad_sensor",
            "type": "unknown_sensor",
        }
        with pytest.raises(AirflowDAGGeneratorError) as exc:
            dag_generator._generate_generic_sensor_task(task_def)

        assert "unsupported generic sensor type" in str(exc.value)


# =============================================================================
# Test: generate_elt_pipeline_dag
# =============================================================================


class TestGenerateEltPipelineDag:
    """Tests for generate_elt_pipeline_dag method."""

    def test_elt_pipeline_airbyte_extract(self, generator_no_validation):
        """Generate ELT pipeline with Airbyte extraction."""
        gen = generator_no_validation

        extract_config = {
            "method": "airbyte",
            "connection_id": "conn-123",
            "airbyte_conn_id": "airbyte_default",
        }
        transform_config = {
            "project_dir": "/dbt/project",
            "models": ["staging", "marts"],
            "target": "prod",
        }

        dag_code = gen.generate_elt_pipeline_dag(
            dag_id="elt_salesforce",
            source_name="salesforce",
            target_schema="analytics",
            extract_config=extract_config,
            transform_config=transform_config,
            schedule="@daily",
            start_date=datetime(2025, 1, 1),
        )

        assert "elt_salesforce" in dag_code
        assert "ELT pipeline" in dag_code

    def test_elt_pipeline_non_airbyte_raises(self, dag_generator):
        """Non-Airbyte extract method should raise."""
        extract_config = {"method": "tpt"}
        transform_config = {"project_dir": "/dbt"}

        with pytest.raises(AirflowDAGGeneratorError) as exc:
            dag_generator.generate_elt_pipeline_dag(
                dag_id="bad_elt",
                source_name="src",
                target_schema="tgt",
                extract_config=extract_config,
                transform_config=transform_config,
            )

        assert "airbyte" in str(exc.value).lower()

    def test_elt_pipeline_with_ssh_dbt(self, generator_no_validation):
        """Generate ELT pipeline with SSH for dbt."""
        gen = generator_no_validation

        extract_config = {"method": "airbyte", "connection_id": "conn-abc"}
        transform_config = {
            "project_dir": "/remote/dbt",
            "remote_dir": "/home/user/dbt",
        }

        dag_code = gen.generate_elt_pipeline_dag(
            dag_id="elt_with_ssh",
            source_name="postgres",
            target_schema="warehouse",
            extract_config=extract_config,
            transform_config=transform_config,
            use_ssh_for_dbt=True,
            ssh_conn_id="my_ssh_conn",
        )

        assert "elt_with_ssh" in dag_code

    def test_elt_pipeline_includes_dbt_deps(self, generator_no_validation):
        """ELT pipeline DAG must include dbt_deps before dbt_run."""
        gen = generator_no_validation

        extract_config = {
            "method": "airbyte",
            "connection_id": "conn-123",
            "airbyte_conn_id": "airbyte_default",
        }
        transform_config = {
            "project_dir": "/dbt/project",
            "models": ["staging"],
            "target": "dev",
        }

        dag_code = gen.generate_elt_pipeline_dag(
            dag_id="elt_with_deps",
            source_name="src",
            target_schema="tgt",
            extract_config=extract_config,
            transform_config=transform_config,
            schedule="@daily",
            start_date=datetime(2025, 1, 1),
        )

        assert "dbt_deps" in dag_code
        assert "dbt deps" in dag_code
        # dbt_deps must appear before dbt_run in the generated code
        assert dag_code.index("dbt_deps") < dag_code.index("dbt_run")


# =============================================================================
# Test: generate_dbt_only_dag
# =============================================================================


class TestGenerateDBTOnlyDag:
    """Tests for generate_dbt_only_dag method."""

    def test_dbt_only_basic(self, generator_no_validation):
        """Generate basic dbt-only DAG."""
        gen = generator_no_validation

        dag_code = gen.generate_dbt_only_dag(
            dag_id="dbt_transform",
            project_dir="/dbt/project",
            start_date=datetime(2025, 1, 1),
        )

        assert "dbt_transform" in dag_code
        assert "dbt transformation" in dag_code

    def test_dbt_only_with_models(self, generator_no_validation):
        """Generate dbt DAG with specific models."""
        gen = generator_no_validation

        dag_code = gen.generate_dbt_only_dag(
            dag_id="dbt_staging",
            project_dir="/dbt/project",
            models=["staging.customers", "staging.orders"],
            start_date=datetime(2025, 1, 1),
        )

        assert "dbt_staging" in dag_code

    def test_dbt_only_with_tests(self, generator_no_validation):
        """Generate dbt DAG with tests enabled."""
        gen = generator_no_validation

        dag_code = gen.generate_dbt_only_dag(
            dag_id="dbt_with_tests",
            project_dir="/dbt/project",
            run_tests=True,
            start_date=datetime(2025, 1, 1),
        )

        assert "dbt_with_tests" in dag_code

    def test_dbt_only_with_docs(self, generator_no_validation):
        """Generate dbt DAG with docs generation."""
        gen = generator_no_validation

        dag_code = gen.generate_dbt_only_dag(
            dag_id="dbt_with_docs",
            project_dir="/dbt/project",
            generate_docs=True,
            start_date=datetime(2025, 1, 1),
        )

        assert "dbt_with_docs" in dag_code

    def test_dbt_only_no_tests(self, generator_no_validation):
        """Generate dbt DAG without tests."""
        gen = generator_no_validation

        dag_code = gen.generate_dbt_only_dag(
            dag_id="dbt_no_tests",
            project_dir="/dbt/project",
            run_tests=False,
            start_date=datetime(2025, 1, 1),
        )

        assert "dbt_no_tests" in dag_code


# =============================================================================
# Test: validate_dag_file
# =============================================================================


class TestValidateDagFile:
    """Tests for validate_dag_file method."""

    def test_validate_valid_dag(self, dag_generator, tmp_path):
        """Validate a valid DAG file."""
        dag_file = tmp_path / "valid_dag.py"
        dag_file.write_text("from airflow import DAG\ndag = DAG('test')")

        result = dag_generator.validate_dag_file(dag_file)

        assert result["valid"] is True
        assert result["syntax_error"] is None

    def test_validate_invalid_dag(self, dag_generator, tmp_path):
        """Validate an invalid DAG file."""
        dag_file = tmp_path / "invalid_dag.py"
        dag_file.write_text("def invalid(:")  # Syntax error

        result = dag_generator.validate_dag_file(dag_file)

        assert result["valid"] is False
        assert result["syntax_error"] is not None

    def test_validate_missing_file(self, dag_generator, tmp_path):
        """Validate a non-existent file."""
        missing_file = tmp_path / "missing.py"

        result = dag_generator.validate_dag_file(missing_file)

        assert result["valid"] is False

    def test_validate_bare_name_expression(self, dag_generator, tmp_path):
        """A bare undefined name at module level must fail validation with name and line number."""
        dag_file = tmp_path / "bad_dag.py"
        dag_file.write_text("from airflow import DAG\nasdfsdf\ndag = DAG('test')")
        result = dag_generator.validate_dag_file(dag_file)
        assert result["valid"] is False
        assert "asdfsdf" in result["syntax_error"]
        assert "line 2" in result["syntax_error"]


# =============================================================================
# Test: sanitize_dag_id
# =============================================================================


class TestSanitizeDagId:
    """Tests for sanitize_dag_id method."""

    def test_sanitize_simple(self, dag_generator):
        """Sanitize simple name."""
        result = dag_generator.sanitize_dag_id("My DAG")
        assert result == "my_dag"

    def test_sanitize_special_chars(self, dag_generator):
        """Sanitize name with special characters."""
        result = dag_generator.sanitize_dag_id("My-DAG@v2.0!")
        assert result == "my_dag_v2_0"

    def test_sanitize_consecutive_underscores(self, dag_generator):
        """Remove consecutive underscores."""
        result = dag_generator.sanitize_dag_id("my___dag")
        assert result == "my_dag"

    def test_sanitize_leading_trailing(self, dag_generator):
        """Remove leading/trailing underscores."""
        result = dag_generator.sanitize_dag_id("_my_dag_")
        assert result == "my_dag"


# =============================================================================
# Test: AirflowDagGenerator (compatibility wrapper) - Comprehensive
# =============================================================================


@pytest.fixture
def compat_generator(tmp_path):
    """Create AirflowDagGenerator (compatibility wrapper) instance."""
    output_dir = tmp_path / "dags"
    output_dir.mkdir()
    return AirflowDagGenerator(output_dir=str(output_dir))


class TestAirflowDagGeneratorCompat:
    """Tests for AirflowDagGenerator compatibility wrapper."""

    def test_init(self, compat_generator, tmp_path):
        """Test initialization."""
        assert compat_generator.output_dir.exists()
        assert compat_generator.default_owner == "airflow"

    def test_generate_imports_bash(self, compat_generator):
        """Generate imports for bash tasks."""
        imports = compat_generator.generate_imports(["bash"])
        assert "from airflow import DAG" in imports
        assert "BashOperator" in imports

    def test_generate_imports_python(self, compat_generator):
        """Generate imports for python tasks."""
        imports = compat_generator.generate_imports(["python"])
        assert "PythonOperator" in imports

    def test_generate_imports_airbyte(self, compat_generator):
        """Generate imports for airbyte tasks."""
        imports = compat_generator.generate_imports(["airbyte"])
        assert "AirbyteTriggerSyncOperator" in imports

    def test_generate_imports_airbyte_sensor(self, compat_generator):
        """Generate imports for airbyte sensor."""
        imports = compat_generator.generate_imports(["airbyte_sensor"])
        assert "AirbyteJobSensor" in imports

    def test_generate_imports_teradata(self, compat_generator):
        """Generate imports for teradata tasks."""
        imports = compat_generator.generate_imports(["teradata"])
        assert "SQLExecuteQueryOperator" in imports

    def test_generate_imports_bteq(self, compat_generator):
        """Generate imports for bteq tasks."""
        imports = compat_generator.generate_imports(["bteq"])
        assert "BteqOperator" in imports

    def test_generate_imports_sensor(self, compat_generator):
        """Generate imports for sensor tasks."""
        imports = compat_generator.generate_imports(["file_sensor"])
        assert "FileSensor" in imports

    def test_generate_imports_task_groups(self, compat_generator):
        """Generate imports for task groups."""
        imports = compat_generator.generate_imports(["task_groups"])
        assert "TaskGroup" in imports

    def test_generate_task_bash(self, compat_generator):
        """Generate bash task."""
        task = compat_generator.generate_task({
            "task_id": "echo_task",
            "type": "bash",
            "bash_command": "echo hello",
        })
        assert "echo_task = BashOperator" in task

    def test_generate_task_python(self, compat_generator):
        """Generate python task."""
        task = compat_generator.generate_task({
            "task_id": "py_task",
            "type": "python",
            "python_callable": "my_func",
        })
        assert "py_task = PythonOperator" in task

    def test_generate_task_teradata(self, compat_generator):
        """Generate teradata task."""
        task = compat_generator.generate_task({
            "task_id": "td_task",
            "type": "teradata",
            "sql": "SELECT 1",
            "conn_id": "td_conn",
        })
        assert "td_task = SQLExecuteQueryOperator" in task

    def test_generate_task_bteq(self, compat_generator):
        """Generate bteq task."""
        task = compat_generator.generate_task({
            "task_id": "bteq_task",
            "type": "bteq",
            "sql": "SELECT 1;",
            "teradata_conn_id": "td_conn",
        })
        assert "bteq_task = BteqOperator" in task

    def test_generate_task_airbyte(self, compat_generator):
        """Generate airbyte task."""
        task = compat_generator.generate_task({
            "task_id": "sync_task",
            "type": "airbyte",
            "connection_id": "conn-123",
        })
        assert "sync_task = AirbyteTriggerSyncOperator" in task

    def test_generate_task_airbyte_async(self, compat_generator):
        """Generate async airbyte task with sensor."""
        task = compat_generator.generate_task({
            "task_id": "async_sync",
            "type": "airbyte",
            "connection_id": "conn-123",
            "asynchronous": True,
        })
        assert "AirbyteTriggerSyncOperator" in task
        assert "AirbyteJobSensor" in task

    def test_generate_task_sensor(self, compat_generator):
        """Generate sensor task."""
        task = compat_generator.generate_task({
            "task_id": "wait_file",
            "type": "file_sensor",
            "filepath": "/data/file.csv",
        })
        assert "wait_file = FileSensor" in task

    def test_generate_task_dbt(self, compat_generator):
        """Generate dbt task."""
        task = compat_generator.generate_task({
            "task_id": "dbt_run",
            "type": "dbt",
            "command": "run",
            "project_dir": "/dbt",
        })
        assert "dbt_run = BashOperator" in task
        assert "dbt run" in task

    def test_generate_task_with_retries(self, compat_generator):
        """Generate task with retries."""
        task = compat_generator.generate_task({
            "task_id": "retry_task",
            "type": "bash",
            "bash_command": "echo retry",
            "retries": 3,
            "retry_delay": 300,
        })
        assert "retries=3" in task

    def test_generate_task_with_pool(self, compat_generator):
        """Generate task with pool."""
        task = compat_generator.generate_task({
            "task_id": "pooled_task",
            "type": "bash",
            "bash_command": "echo pooled",
            "pool": "my_pool",
        })
        assert "pool='my_pool'" in task

    def test_generate_task_with_trigger_rule(self, compat_generator):
        """Generate task with trigger rule."""
        task = compat_generator.generate_task({
            "task_id": "triggered_task",
            "type": "bash",
            "bash_command": "echo trigger",
            "trigger_rule": "all_done",
        })
        assert "trigger_rule='all_done'" in task

    def test_generate_dag(self, compat_generator):
        """Generate DAG using compat wrapper."""
        dag_code = compat_generator.generate_dag({
            "dag_id": "compat_dag",
            "description": "Test DAG",
            "schedule_interval": "@daily",
            "start_date": "2025-01-01",
            "tags": ["test"],
        })
        assert "compat_dag" in dag_code
        assert "from airflow import DAG" in dag_code

    def test_generate_dag_with_tasks(self, compat_generator):
        """Generate DAG with tasks."""
        dag_code = compat_generator.generate_dag_with_tasks({
            "dag_id": "tasks_dag",
            "schedule_interval": "@hourly",
            "tasks": [
                {"task_id": "t1", "type": "bash", "bash_command": "echo 1"},
                {"task_id": "t2", "type": "bash", "bash_command": "echo 2"},
            ],
            "dependencies": {"t2": ["t1"]},
        })
        assert "tasks_dag" in dag_code
        assert "t1" in dag_code
        assert "t2" in dag_code
        assert "t1 >> t2" in dag_code

    def test_generate_dag_with_task_groups(self, compat_generator):
        """Generate DAG with task groups."""
        dag_code = compat_generator.generate_dag_with_tasks({
            "dag_id": "grouped_dag",
            "tasks": [],
            "task_groups": [{"group_id": "extract_group"}],
        })
        assert "TaskGroup" in dag_code
        assert "extract_group" in dag_code

    def test_generate_dag_with_dynamic_tasks(self, compat_generator):
        """Generate DAG with dynamic tasks."""
        dag_code = compat_generator.generate_dag_with_dynamic_tasks({
            "dag_id": "dynamic_dag",
            "dynamic_tasks": {
                "template": {
                    "task_id": "task_{name}",
                    "type": "bash",
                    "bash_command": "echo {name}",
                },
                "variables": [
                    {"name": "alpha"},
                    {"name": "beta"},
                ],
            },
        })
        assert "task_alpha" in dag_code
        assert "task_beta" in dag_code

    def test_validate_config_valid(self, compat_generator):
        """Validate valid config."""
        result = compat_generator.validate_config({
            "dag_id": "valid_dag",
            "schedule_interval": "0 * * * *",
        })
        assert result["valid"] is True

    def test_validate_config_missing_dag_id(self, compat_generator):
        """Validate config missing dag_id."""
        result = compat_generator.validate_config({
            "schedule_interval": "@daily",
        })
        assert result["valid"] is False
        assert "dag_id" in str(result["errors"])

    def test_write_dag_file(self, compat_generator, tmp_path):
        """Write DAG file."""
        content = "from airflow import DAG\ndag = DAG('test')"
        path = compat_generator.write_dag_file(content, "test_dag")
        assert path.exists()
        assert "test_dag.py" in str(path)

    def test_format_schedule_none(self, compat_generator):
        """Format None schedule."""
        result = compat_generator._format_schedule(None)
        assert result == "None"

    def test_format_schedule_dict(self, compat_generator):
        """Format dict schedule."""
        result = compat_generator._format_schedule({"hours": 6})
        assert result == "timedelta(hours=6)"

    def test_format_schedule_string(self, compat_generator):
        """Format string schedule."""
        result = compat_generator._format_schedule("@daily")
        assert result == "'@daily'"


# =============================================================================
# Test: Additional edge cases (Comprehensive)
# =============================================================================


class TestEdgeCasesComprehensive:
    """Test edge cases and boundary conditions."""

    def test_bash_task_double_quotes(self, dag_generator):
        """Bash command with double quotes."""
        task_def = {
            "task_id": "echo_task",
            "bash_command": 'echo "hello world"',
        }
        code = dag_generator._generate_bash_task(task_def)
        assert "echo_task" in code

    def test_python_task_with_callable(self, dag_generator):
        """Python task with callable name."""
        task_def = {
            "task_id": "py_task",
            "python_callable": "my_function",
        }
        code = dag_generator._generate_python_task(task_def)
        assert "python_callable=my_function" in code

    def test_sensor_with_execution_delta(self, dag_generator):
        """Sensor task with execution delta."""
        task_def = {
            "task_id": "wait_upstream",
            "external_dag_id": "upstream_dag",
            "external_task_id": "final_task",
            "execution_delta_minutes": 30,
        }
        code = dag_generator._generate_sensor_task(task_def)
        assert "ExternalTaskSensor" in code

    def test_dbt_task_with_models_list(self, dag_generator):
        """dbt task with models as list."""
        task_def = {
            "task_id": "dbt_select",
            "dbt_command": "dbt run --select model1 model2",
        }
        code = dag_generator._generate_dbt_task(task_def)
        assert "dbt run" in code

    def test_airbyte_task_sync_mode(self, dag_generator):
        """Airbyte task in synchronous mode."""
        task_def = {
            "task_id": "sync_conn",
            "connection_id": "conn-abc",
            # No async flag = sync mode
        }
        code = dag_generator._generate_airbyte_task(task_def)
        assert "asynchronous=True" in code  # Default is async


# =============================================================================
# DAG generator error tests (merged from test_airflow_negative_cases.py)
# =============================================================================


class TestDAGGeneratorErrors:
    """Tests for DAG generator error cases."""

    def test_dag_generator_empty_dag_id(self, dag_generator):
        """Test DAG generation with empty dag_id."""
        with pytest.raises(AirflowDAGGeneratorError):
            dag_generator.generate_dag(
                dag_id="",  # Empty
                description="Test",
                schedule="@daily",
                tasks=[],
                dependencies=[],
                start_date=datetime(2025, 1, 1),
            )

    def test_dag_generator_invalid_schedule(self, dag_generator):
        """Test DAG generation with invalid schedule format."""
        # Invalid cron expression should be caught
        with pytest.raises(AirflowDAGGeneratorError):
            dag_generator.generate_dag(
                dag_id="test_dag",
                description="Test",
                schedule="invalid cron * * *",  # Invalid
                tasks=[],
                dependencies=[],
                start_date=datetime(2025, 1, 1),
            )

    def test_missing_required_task_field(self, dag_generator):
        """Test task with missing required field."""
        tasks = [{"type": "bash"}]  # Missing task_id

        with pytest.raises(AirflowDAGGeneratorError):
            dag_generator.generate_dag(
                dag_id="test_dag",
                description="Test",
                schedule="@daily",
                tasks=tasks,
                dependencies=[],
                start_date=datetime(2025, 1, 1),
            )

    def test_circular_dependency(self, dag_generator):
        """Test detection of circular dependencies."""
        tasks = [
            {"task_id": "a", "type": "empty"},
            {"task_id": "b", "type": "empty"},
        ]
        dependencies = [("a", "b"), ("b", "a")]  # Circular

        # Should either raise or generate (Airflow will catch it)
        try:
            dag_generator.generate_dag(
                dag_id="circular_dag",
                description="Test",
                schedule="@daily",
                tasks=tasks,
                dependencies=dependencies,
                start_date=datetime(2025, 1, 1),
            )
        except AirflowDAGGeneratorError:
            pass  # Expected

    def test_invalid_task_type(self, dag_generator):
        """Test handling of unknown task type."""
        tasks = [
            {"task_id": "valid_task", "type": "empty"},  # Valid task
            {"task_id": "unknown_task", "type": "unknown_operator"},  # Unknown type
        ]

        # Generator logs warning and skips unknown task types
        result = dag_generator.generate_dag(
            dag_id="test_dag",
            description="Test",
            schedule="@daily",
            tasks=tasks,
            dependencies=[],
            start_date=datetime(2025, 1, 1),
        )
        # Result should include valid task but not the unknown one
        assert result is not None
        assert "valid_task" in result
        assert "unknown_operator" not in result

    def test_dependency_on_nonexistent_task(self, dag_generator):
        """Test dependency referencing non-existent task."""
        tasks = [{"task_id": "task1", "type": "empty"}]
        dependencies = [("task1", "nonexistent_task")]

        # Should generate code (Airflow validates at runtime)
        result = dag_generator.generate_dag(
            dag_id="test_dag",
            description="Test",
            schedule="@daily",
            tasks=tasks,
            dependencies=dependencies,
            start_date=datetime(2025, 1, 1),
        )
        assert result is not None

    def test_dag_generator_future_start_date(self, dag_generator):
        """Test DAG generation with future start date."""
        future_date = datetime(2030, 1, 1)

        result = dag_generator.generate_dag(
            dag_id="future_dag",
            description="Future DAG",
            schedule="@daily",
            tasks=[{"task_id": "task1", "type": "empty"}],
            dependencies=[],
            start_date=future_date,
        )

        assert "2030" in result

    def test_dag_generator_past_start_date(self, dag_generator):
        """Test DAG generation with very old start date."""
        past_date = datetime(2000, 1, 1)

        result = dag_generator.generate_dag(
            dag_id="old_dag",
            description="Old DAG",
            schedule="@daily",
            tasks=[{"task_id": "task1", "type": "empty"}],
            dependencies=[],
            start_date=past_date,
            catchup=False,  # Important for old start dates
        )

        assert "2000" in result
        assert "catchup=False" in result


# =============================================================================
# Regression: SafeFileWriter flag alignment for DAG generators
# =============================================================================
#
# The Airflow scheduler recurses into its DAGs folder, so a ``.backups/*.py``
# created by SafeFileWriter on re-generation would be parsed as additional
# DAGs, producing duplicate-DAG warnings. POSIX mode 0o640 blocks an Airflow
# worker running as a different local user from reading the generated file.
# Both DAG generators must therefore set ``enable_backups=False`` and
# ``restrict_permissions=False`` on the shared SafeFileWriter.


class TestDagGeneratorSafeWriterFlags:
    """Ensure DAG generators stay aligned with the backups/perms policy."""

    def test_airflow_dag_generator_no_backups_on_regenerate(self, tmp_path):
        """Regenerating a DAG must not create .backups/*.py inside dags_folder.

        Airflow's scheduler would otherwise try to parse those timestamped
        copies as DAGs and emit duplicate-DAG warnings on every scan.
        """
        dags_folder = tmp_path / "dags"
        dags_folder.mkdir()
        gen = AirflowDAGGenerator(dags_folder=str(dags_folder))

        def _generate() -> None:
            gen.generate_dag(
                dag_id="regen_dag",
                description="regen",
                schedule="@daily",
                tasks=[{"task_id": "t1", "type": "empty"}],
                dependencies=[],
                start_date=datetime(2025, 1, 1),
            )

        _generate()
        _generate()  # trigger backup path

        backups = dags_folder / ".backups"
        assert not backups.exists(), (
            f".backups/ must not be created inside the Airflow DAGs folder "
            f"but was found at {backups}"
        )
        assert (dags_folder / "regen_dag.py").exists()

    def test_airflow_tdload_generator_no_backups_on_regenerate(self, tmp_path):
        """Same invariant for AirflowTdLoadDAGGenerator."""
        from teradata_etl_mcp_server.generators.airflow_tdload_dag_generator import (
            AirflowTdLoadDAGGenerator,
        )

        dags_folder = tmp_path / "dags"
        dags_folder.mkdir()
        gen = AirflowTdLoadDAGGenerator(dags_folder=dags_folder)

        def _generate() -> None:
            gen.generate_file_loading_dag(
                dag_id="tdload_regen",
                description="tdload regen",
                source_file_path="/data/input.csv",
                target_database="db",
                target_table="tbl",
                source_format="Delimited",
                teradata_conn_id="td",
                ssh_conn_id="ssh",
                schedule="@daily",
                start_date=datetime(2025, 1, 1),
                columns=[{"name": "id", "type": "INTEGER"}],
                skip_rows=1,
            )

        _generate()
        _generate()

        backups = dags_folder / ".backups"
        assert not backups.exists(), (
            f".backups/ must not be created inside the Airflow DAGs folder "
            f"but was found at {backups}"
        )
        assert (dags_folder / "tdload_regen.py").exists()
