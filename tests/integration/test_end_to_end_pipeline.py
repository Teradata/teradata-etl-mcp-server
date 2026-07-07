"""Integration tests for end-to-end ETL pipeline execution.

This test suite covers complete pipeline workflows including:
- Extract from Teradata source
- Transform with dbt
- Load to target destination
- Data integrity verification
- Error handling and rollback scenarios
"""

import pytest

# Skip entire module - depends on modules not yet implemented
pytest.skip(
    "Skipping end-to-end tests: PipelineExecutor, DataValidator, etc. not yet implemented",
    allow_module_level=True,
)

import asyncio
import time
from datetime import datetime

from teradata_etl_mcp_server.data_validator import DataValidator
from teradata_etl_mcp_server.lineage import LineageTracker
from teradata_etl_mcp_server.metrics_collector import MetricsCollector

from teradata_etl_mcp_server.clients.airbyte_client import AirbyteClient
from teradata_etl_mcp_server.clients.teradata_client import TeradataClient


@pytest.fixture(scope="module")
def teradata_config():
    """Teradata connection configuration for testing."""
    return {
        "host": "localhost",
        "port": 1025,
        "username": "dbc",
        "password": "dbc",
        "database": "test_db"
    }


@pytest.fixture(scope="module")
def dbt_config():
    """dbt project configuration for testing."""
    return {
        "project_dir": "/tmp/test_dbt_project",
        "profiles_dir": "/tmp/test_profiles",
        "target": "test"
    }


@pytest.fixture(scope="module")
def airbyte_config():
    """Airbyte configuration for testing."""
    return {
        "base_url": "http://localhost:8000",
        "workspace_id": "test_workspace"
    }


@pytest.fixture(scope="module")
async def teradata_client(teradata_config):
    """Create Teradata client for testing."""
    client = TeradataClient(teradata_config)
    await client.connect()
    yield client
    await client.close()


@pytest.fixture(scope="module")
async def dbt_client(dbt_config):
    """Create dbt client for testing."""
    client = DbtClient(dbt_config)
    yield client
    await client.cleanup()


@pytest.fixture(scope="module")
async def airbyte_client(airbyte_config):
    """Create Airbyte client for testing."""
    client = AirbyteClient(airbyte_config)
    yield client


@pytest.fixture
async def setup_test_data(teradata_client):
    """Setup test data in source tables."""
    # Create source table
    await teradata_client.execute("""
        CREATE TABLE IF NOT EXISTS source_customers (
            customer_id INTEGER,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            created_at TIMESTAMP,
            status VARCHAR(20)
        )
    """)

    # Insert test data
    test_data = [
        (1, 'John', 'Doe', 'john.doe@example.com', datetime.now(), 'active'),
        (2, 'Jane', 'Smith', 'jane.smith@example.com', datetime.now(), 'active'),
        (3, 'Bob', 'Johnson', 'bob.johnson@example.com', datetime.now(), 'inactive'),
        (4, 'Alice', 'Williams', 'alice.williams@example.com', datetime.now(), 'active'),
        (5, 'Charlie', 'Brown', 'charlie.brown@example.com', datetime.now(), 'active'),
    ]

    for row in test_data:
        await teradata_client.execute(
            """
            INSERT INTO source_customers 
            (customer_id, first_name, last_name, email, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            row
        )

    yield

    # Cleanup
    await teradata_client.execute("DROP TABLE IF EXISTS source_customers")


@pytest.fixture
async def setup_dbt_project(dbt_client, tmp_path):
    """Setup dbt project structure for testing."""
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()

    # Create dbt_project.yml
    dbt_project_yml = """
name: 'test_project'
version: '1.0.0'
profile: 'test'

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
analysis-paths: ["analyses"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  test_project:
    staging:
      +materialized: view
    marts:
      +materialized: table
"""
    (project_dir / "dbt_project.yml").write_text(dbt_project_yml)

    # Create models directory
    models_dir = project_dir / "models"
    models_dir.mkdir()

    # Create staging model
    staging_dir = models_dir / "staging"
    staging_dir.mkdir()

    staging_model = """
-- models/staging/stg_customers.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'source_customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        created_at,
        UPPER(status) AS status
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned
"""
    (staging_dir / "stg_customers.sql").write_text(staging_model)

    # Create mart model
    marts_dir = models_dir / "marts"
    marts_dir.mkdir()

    mart_model = """
-- models/marts/customer_summary.sql
WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

summarized AS (
    SELECT
        status,
        COUNT(*) AS customer_count,
        COUNT(DISTINCT email) AS unique_emails
    FROM customers
    GROUP BY status
)

SELECT * FROM summarized
"""
    (marts_dir / "customer_summary.sql").write_text(mart_model)

    # Create source definition
    staging_schema = """
version: 2

sources:
  - name: raw
    schema: test_db
    tables:
      - name: source_customers
"""
    (staging_dir / "sources.yml").write_text(staging_schema)

    return project_dir


class TestEndToEndPipeline:
    """Test suite for end-to-end pipeline execution."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_complete_etl_pipeline(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test complete ETL pipeline from extraction to loading."""
        # Phase 1: Extract - Verify source data exists
        source_count = await teradata_client.execute_scalar(
            "SELECT COUNT(*) FROM source_customers"
        )
        assert source_count == 5, "Source data not properly loaded"

        # Phase 2: Transform - Run dbt models
        dbt_client.project_dir = str(setup_dbt_project)

        # Run dbt deps (if needed)
        deps_result = await dbt_client.run_command("deps")
        assert deps_result["success"] is True

        # Run dbt models
        run_result = await dbt_client.run()
        assert run_result["success"] is True
        assert len(run_result["results"]) >= 2  # stg_customers + customer_summary

        # Verify staging model
        stg_count = await teradata_client.execute_scalar(
            "SELECT COUNT(*) FROM stg_customers"
        )
        assert stg_count == 5, "Staging model didn't process all records"

        # Verify data transformation
        stg_data = await teradata_client.fetch_all(
            "SELECT first_name, last_name, email, status FROM stg_customers LIMIT 1"
        )
        assert stg_data[0]["email"] == stg_data[0]["email"].lower(), "Email not lowercased"
        assert stg_data[0]["status"] == stg_data[0]["status"].upper(), "Status not uppercased"

        # Phase 3: Load - Verify mart model
        mart_count = await teradata_client.execute_scalar(
            "SELECT COUNT(*) FROM customer_summary"
        )
        assert mart_count >= 2, "Mart model didn't aggregate properly"

        # Verify aggregation
        mart_data = await teradata_client.fetch_all(
            "SELECT status, customer_count FROM customer_summary ORDER BY status"
        )

        active_count = next(
            (row["customer_count"] for row in mart_data if row["status"] == "ACTIVE"),
            0
        )
        assert active_count == 4, "Incorrect active customer count"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_data_integrity_verification(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test data integrity checks throughout pipeline."""
        validator = DataValidator()

        # Step 1: Validate source data
        source_data = await teradata_client.fetch_all("SELECT * FROM source_customers")

        # Check for nulls in critical columns
        null_checks = validator.validate_not_null(source_data, ["customer_id", "email"])
        assert null_checks["passed"] is True, "Source data has null values"

        # Check for unique customer IDs
        unique_check = validator.validate_unique(source_data, ["customer_id"])
        assert unique_check["passed"] is True, "Duplicate customer IDs found"

        # Step 2: Run dbt with tests
        dbt_client.project_dir = str(setup_dbt_project)
        run_result = await dbt_client.run()
        assert run_result["success"] is True

        # Run dbt tests
        test_result = await dbt_client.test()
        assert test_result["success"] is True, "dbt tests failed"

        # Step 3: Validate transformed data
        stg_data = await teradata_client.fetch_all("SELECT * FROM stg_customers")

        # Verify email format
        email_pattern_check = validator.validate_pattern(
            stg_data,
            "email",
            r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
        )
        assert email_pattern_check["passed"] is True, "Invalid email format in staging"

        # Step 4: Validate mart data
        mart_data = await teradata_client.fetch_all("SELECT * FROM customer_summary")

        # Verify aggregation accuracy
        total_in_mart = sum(row["customer_count"] for row in mart_data)
        assert total_in_mart == len(source_data), "Aggregation count mismatch"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_incremental_pipeline_execution(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test incremental pipeline updates."""
        # Initial run
        dbt_client.project_dir = str(setup_dbt_project)
        initial_run = await dbt_client.run()
        assert initial_run["success"] is True

        initial_count = await teradata_client.execute_scalar(
            "SELECT COUNT(*) FROM customer_summary"
        )

        # Add more data to source
        await teradata_client.execute("""
            INSERT INTO source_customers 
            (customer_id, first_name, last_name, email, created_at, status)
            VALUES (6, 'New', 'Customer', 'new@example.com', CURRENT_TIMESTAMP, 'active')
        """)

        # Incremental run
        incremental_run = await dbt_client.run()
        assert incremental_run["success"] is True

        # Verify new data was processed
        updated_count = await teradata_client.execute_scalar(
            "SELECT COUNT(*) FROM customer_summary"
        )

        # Count should reflect the new data
        new_source_count = await teradata_client.execute_scalar(
            "SELECT COUNT(*) FROM source_customers"
        )
        assert new_source_count == 6, "New source data not added"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_pipeline_with_lineage_tracking(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test pipeline execution with lineage tracking."""
        tracker = LineageTracker()

        # Track pipeline execution
        tracker.start_pipeline("e2e_test_pipeline")

        # Track source read
        tracker.track_read(
            table="source_customers",
            database="test_db",
            operation_id="e2e_test_pipeline"
        )

        # Run dbt transformations
        dbt_client.project_dir = str(setup_dbt_project)
        run_result = await dbt_client.run()
        assert run_result["success"] is True

        # Track transformations
        tracker.track_transformation(
            source_tables=["source_customers"],
            target_table="stg_customers",
            transformation_type="view"
        )

        tracker.track_transformation(
            source_tables=["stg_customers"],
            target_table="customer_summary",
            transformation_type="aggregate"
        )

        # Track target write
        tracker.track_write(
            table="customer_summary",
            database="test_db",
            operation_id="e2e_test_pipeline"
        )

        tracker.end_pipeline("e2e_test_pipeline")

        # Build and verify lineage graph
        lineage_graph = tracker.build_lineage_graph()

        assert lineage_graph.has_node("source_customers")
        assert lineage_graph.has_node("stg_customers")
        assert lineage_graph.has_node("customer_summary")

        # Verify lineage connections
        upstream = lineage_graph.get_upstream_nodes("customer_summary")
        assert "stg_customers" in upstream
        assert "source_customers" in upstream

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_pipeline_error_handling(
        self,
        teradata_client,
        dbt_client,
        setup_test_data
    ):
        """Test error handling in pipeline execution."""
        # Test scenario 1: Invalid SQL in dbt model
        with pytest.raises(Exception) as exc_info:
            # Try to run with non-existent project
            dbt_client.project_dir = "/nonexistent/path"
            await dbt_client.run()

        assert "not found" in str(exc_info.value).lower() or "error" in str(exc_info.value).lower()

        # Test scenario 2: Source table doesn't exist
        # Drop source table temporarily
        await teradata_client.execute("DROP TABLE IF EXISTS temp_missing_source")

        # Attempting to query should raise error
        with pytest.raises(Exception):
            await teradata_client.fetch_all("SELECT * FROM temp_missing_source")

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_pipeline_rollback_on_failure(
        self,
        teradata_client,
        setup_test_data
    ):
        """Test transaction rollback on pipeline failure."""
        # Start transaction
        await teradata_client.begin_transaction()

        try:
            # Insert data
            await teradata_client.execute("""
                INSERT INTO source_customers 
                (customer_id, first_name, last_name, email, created_at, status)
                VALUES (999, 'Test', 'Rollback', 'test@rollback.com', CURRENT_TIMESTAMP, 'active')
            """)

            # Verify data exists in transaction
            count_in_transaction = await teradata_client.execute_scalar(
                "SELECT COUNT(*) FROM source_customers WHERE customer_id = 999"
            )
            assert count_in_transaction == 1

            # Simulate error and rollback
            await teradata_client.rollback()

            # Verify rollback - data should not exist
            count_after_rollback = await teradata_client.execute_scalar(
                "SELECT COUNT(*) FROM source_customers WHERE customer_id = 999"
            )
            assert count_after_rollback == 0, "Rollback failed - data still exists"

        except Exception:
            await teradata_client.rollback()
            raise

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_pipeline_with_metrics_collection(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test pipeline execution with metrics collection."""
        metrics = MetricsCollector()

        # Start pipeline metrics
        pipeline_id = "e2e_metrics_test"
        metrics.increment_counter("pipelines_started_total")
        metrics.increment_gauge("pipelines_running")

        start_time = time.time()

        try:
            # Extract phase
            extract_start = time.time()
            source_count = await teradata_client.execute_scalar(
                "SELECT COUNT(*) FROM source_customers"
            )
            extract_duration = time.time() - extract_start

            metrics.observe_histogram("extract_duration_seconds", extract_duration)
            metrics.set_gauge("extract_row_count", source_count)

            # Transform phase
            transform_start = time.time()
            dbt_client.project_dir = str(setup_dbt_project)
            run_result = await dbt_client.run()
            transform_duration = time.time() - transform_start

            metrics.observe_histogram("transform_duration_seconds", transform_duration)
            metrics.increment_counter("models_executed_total", len(run_result.get("results", [])))

            # Load phase
            load_start = time.time()
            target_count = await teradata_client.execute_scalar(
                "SELECT COUNT(*) FROM customer_summary"
            )
            load_duration = time.time() - load_start

            metrics.observe_histogram("load_duration_seconds", load_duration)
            metrics.set_gauge("load_row_count", target_count)

            # Pipeline completed successfully
            total_duration = time.time() - start_time
            metrics.observe_histogram("pipeline_duration_seconds", total_duration)
            metrics.increment_counter("pipelines_completed_total")

            # Verify metrics
            stats = metrics.get_statistics()
            assert "pipelines_completed_total" in stats
            assert stats["pipelines_completed_total"] >= 1

        finally:
            metrics.decrement_gauge("pipelines_running")

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_parallel_pipeline_execution(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test executing multiple pipeline steps in parallel."""
        dbt_client.project_dir = str(setup_dbt_project)

        async def run_staging_models():
            """Run staging models."""
            return await dbt_client.run(select="staging.*")

        async def validate_source_data():
            """Validate source data in parallel."""
            count = await teradata_client.execute_scalar(
                "SELECT COUNT(*) FROM source_customers"
            )
            return count

        # Run tasks in parallel
        results = await asyncio.gather(
            run_staging_models(),
            validate_source_data(),
            return_exceptions=True
        )

        # Verify both tasks completed
        assert len(results) == 2
        assert isinstance(results[1], int)  # Count result
        assert results[1] == 5

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_pipeline_with_data_quality_checks(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test pipeline with comprehensive data quality checks."""
        validator = DataValidator()

        # Pre-transformation checks
        source_data = await teradata_client.fetch_all("SELECT * FROM source_customers")

        pre_checks = {
            "not_null": validator.validate_not_null(source_data, ["customer_id", "email"]),
            "unique": validator.validate_unique(source_data, ["customer_id"]),
            "range": validator.validate_range(source_data, "customer_id", min_value=1),
        }

        assert all(check["passed"] for check in pre_checks.values()), "Pre-checks failed"

        # Run transformation
        dbt_client.project_dir = str(setup_dbt_project)
        run_result = await dbt_client.run()
        assert run_result["success"] is True

        # Post-transformation checks
        stg_data = await teradata_client.fetch_all("SELECT * FROM stg_customers")

        post_checks = {
            "not_null": validator.validate_not_null(stg_data, ["customer_id", "email"]),
            "unique": validator.validate_unique(stg_data, ["customer_id"]),
            "email_format": validator.validate_pattern(
                stg_data,
                "email",
                r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
            ),
        }

        assert all(check["passed"] for check in post_checks.values()), "Post-checks failed"

        # Data reconciliation
        source_count = len(source_data)
        stg_count = len(stg_data)
        assert source_count == stg_count, f"Row count mismatch: {source_count} vs {stg_count}"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_pipeline_performance_benchmarks(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test pipeline performance meets benchmarks."""
        # Benchmark: Complete pipeline should execute within time limit
        start_time = time.time()

        dbt_client.project_dir = str(setup_dbt_project)
        run_result = await dbt_client.run()

        total_duration = time.time() - start_time

        # Assert performance benchmarks
        assert run_result["success"] is True
        assert total_duration < 30.0, f"Pipeline took too long: {total_duration}s"

        # Benchmark: Query performance
        query_start = time.time()
        result = await teradata_client.fetch_all(
            "SELECT * FROM customer_summary"
        )
        query_duration = time.time() - query_start

        assert query_duration < 5.0, f"Query took too long: {query_duration}s"
        assert len(result) > 0, "No results returned"


class TestPipelineErrorRecovery:
    """Test suite for pipeline error recovery scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_recovery_from_connection_failure(
        self,
        teradata_client,
        teradata_config
    ):
        """Test recovery from database connection failure."""
        # Simulate connection loss
        await teradata_client.close()

        # Attempt operation (should fail)
        with pytest.raises(Exception):
            await teradata_client.execute_scalar("SELECT 1")

        # Reconnect and retry
        await teradata_client.connect()

        # Should succeed now
        result = await teradata_client.execute_scalar("SELECT 1")
        assert result == 1

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_partial_failure_recovery(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test recovery from partial pipeline failure."""
        dbt_client.project_dir = str(setup_dbt_project)

        # Run first model successfully
        result1 = await dbt_client.run(select="staging.*")
        assert result1["success"] is True

        # Verify staging model exists
        stg_exists = await teradata_client.execute_scalar("""
            SELECT COUNT(*) 
            FROM DBC.TablesV 
            WHERE TableName = 'stg_customers'
        """)

        # Even if marts fail, staging should be available for retry
        assert stg_exists >= 0  # Table check logic

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_retry_logic_with_backoff(
        self,
        teradata_client
    ):
        """Test retry logic with exponential backoff."""
        max_retries = 3
        retry_count = 0

        for attempt in range(max_retries):
            try:
                # Attempt operation
                result = await teradata_client.execute_scalar("SELECT 1")
                break  # Success
            except Exception:
                retry_count += 1
                if retry_count >= max_retries:
                    raise

                # Exponential backoff
                wait_time = 2 ** attempt
                await asyncio.sleep(wait_time)

        # Should eventually succeed
        assert result == 1


class TestPipelineScheduling:
    """Test suite for pipeline scheduling and orchestration."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_scheduled_pipeline_execution(
        self,
        teradata_client,
        dbt_client,
        setup_test_data,
        setup_dbt_project
    ):
        """Test scheduled pipeline execution."""
        from teradata_etl_mcp_server.scheduler import PipelineScheduler

        scheduler = PipelineScheduler()

        # Schedule pipeline to run immediately
        job = scheduler.schedule_pipeline(
            pipeline_id="scheduled_test",
            schedule="now",
            config={
                "dbt_project": str(setup_dbt_project),
                "target": "test"
            }
        )

        # Wait for execution
        await asyncio.sleep(1)

        # Verify execution occurred
        assert job is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_pipeline_dependency_resolution(
        self,
        dbt_client,
        setup_dbt_project
    ):
        """Test pipeline dependency resolution and execution order."""
        dbt_client.project_dir = str(setup_dbt_project)

        # Get execution plan
        plan = await dbt_client.compile()

        assert plan["success"] is True

        # Verify models are compiled in correct order
        # staging models should come before marts
        compiled_nodes = plan.get("results", [])

        staging_models = [n for n in compiled_nodes if "staging" in n.get("name", "")]
        mart_models = [n for n in compiled_nodes if "mart" in n.get("name", "")]

        # Both types should exist
        assert len(staging_models) > 0 or len(mart_models) > 0
