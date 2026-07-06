"""Integration tests for DBT Generator with real project structure."""

from pathlib import Path

import pytest

from elt_mcp_server.generators.dbt_generator import DBTGenerator


@pytest.fixture
def teradata_config():
    """Teradata configuration for testing."""
    return {
        "host": "localhost",
        "port": 1025,
        "username": "user",
        "password": "password",
        "database": "database_name",
    }


@pytest.fixture
def dbt_project_dir(tmp_path):
    """Create a temporary dbt project directory."""
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()
    return project_dir


@pytest.fixture
def dbt_generator(dbt_project_dir):
    """Create DBTGenerator instance."""
    return DBTGenerator(project_dir=dbt_project_dir)


class TestProjectStructureCreation:
    """Tests for creating dbt project structure."""

    @pytest.mark.integration
    def test_create_basic_project_structure(self, dbt_generator, dbt_project_dir):
        """Test creating basic project structure with all layers."""
        result = dbt_generator.create_project_structure(
            project_name="test_project",
            include_staging=True,
            include_intermediate=True,
            include_marts=True,
            include_snapshots=False,
            include_tests=True,
            include_macros=True,
            staging_materialization="view",
            intermediate_materialization="view",
            marts_materialization="table"
        )

        # Verify structure was created
        assert result is not None
        assert "created_paths" in result
        assert "folders" in result["created_paths"]
        assert "files" in result["created_paths"]

        # Check folders exist
        models_dir = dbt_project_dir / "models"
        assert models_dir.exists()
        assert (models_dir / "staging").exists()
        assert (models_dir / "intermediate").exists()
        assert (models_dir / "marts").exists()

        # Verify config files
        assert len(result["created_paths"]["folders"]) >= 4  # models + staging + intermediate + marts
        assert len(result["created_paths"]["files"]) > 0

    @pytest.mark.integration
    def test_create_project_with_mart_subfolders(self, dbt_generator, dbt_project_dir):
        """Test creating project structure with mart business domain subfolders."""
        result = dbt_generator.create_project_structure(
            project_name="test_project",
            include_marts=True,
            mart_subfolders=["finance", "marketing", "sales"],
            marts_materialization="table"
        )

        models_dir = dbt_project_dir / "models"
        marts_dir = models_dir / "marts"

        # Verify mart subfolders were created
        assert (marts_dir / "finance").exists()
        assert (marts_dir / "marketing").exists()
        assert (marts_dir / "sales").exists()

    @pytest.mark.integration
    def test_create_project_generates_dbt_config(self, dbt_generator, dbt_project_dir):
        """Test that project structure includes proper dbt_project.yml."""
        result = dbt_generator.create_project_structure(
            project_name="analytics_project",
            include_staging=True,
            include_intermediate=True,
            include_marts=True
        )

        # Should have created structure (files list should not be empty)
        assert len(result["created_paths"]["files"]) > 0

        # Verify directories exist
        assert (dbt_project_dir / "models" / "staging").exists()
        assert (dbt_project_dir / "models" / "intermediate").exists()
        assert (dbt_project_dir / "models" / "marts").exists()


class TestIntermediateModelGeneration:
    """Tests for generating intermediate models."""

    @pytest.mark.integration
    def test_generate_simple_intermediate_model(self, dbt_generator, dbt_project_dir):
        """Test generating basic intermediate model with joins."""
        # First create project structure
        dbt_generator.create_project_structure(
            project_name="test_project",
            include_intermediate=True
        )

        sql = dbt_generator.generate_intermediate_model(
            model_name="int_orders_customers",
            source_models=["stg_orders", "stg_customers"],
            join_logic=[
                {
                    "model": "stg_customers",
                    "type": "left",
                    "on": "stg_orders.customer_id = stg_customers.customer_id"
                }
            ],
            select_columns=[
                "stg_orders.order_id",
                "stg_orders.order_date",
                "stg_customers.customer_name",
                "stg_customers.email"
            ],
            materialization="view",
            output_path=Path("models/intermediate/int_orders_customers.sql")
        )

        # Verify SQL content
        assert sql is not None
        assert "config(" in sql
        assert "materialized='view'" in sql
        assert "with stg_orders as" in sql
        assert "with stg_customers as" in sql or "stg_customers as" in sql
        assert "left join" in sql.lower()
        assert "ref('stg_orders')" in sql
        assert "ref('stg_customers')" in sql

        # Verify file was created
        output_file = dbt_project_dir / "models/intermediate/int_orders_customers.sql"
        assert output_file.exists()
        assert output_file.read_text() == sql

    @pytest.mark.integration
    def test_generate_intermediate_model_with_aggregation(self, dbt_generator, dbt_project_dir):
        """Test generating intermediate model with group by."""
        dbt_generator.create_project_structure(
            project_name="test_project",
            include_intermediate=True
        )

        sql = dbt_generator.generate_intermediate_model(
            model_name="int_customer_orders_summary",
            source_models=["stg_orders"],
            select_columns=[
                "customer_id",
                "count(*) as order_count",
                "sum(total_amount) as total_spent"
            ],
            group_by=["customer_id"],
            materialization="view",
            output_path=Path("models/intermediate/int_customer_orders_summary.sql")
        )

        assert sql is not None
        assert "group by" in sql.lower()
        assert "customer_id" in sql
        assert "order_count" in sql
        assert "total_spent" in sql

        # Verify file exists
        output_file = dbt_project_dir / "models/intermediate/int_customer_orders_summary.sql"
        assert output_file.exists()

    @pytest.mark.integration
    def test_generate_intermediate_model_with_hooks(self, dbt_generator, dbt_project_dir):
        """Test generating intermediate model with pre/post hooks."""
        dbt_generator.create_project_structure(
            project_name="test_project",
            include_intermediate=True
        )

        sql = dbt_generator.generate_intermediate_model(
            model_name="int_orders_enriched",
            source_models=["stg_orders"],
            select_columns=["*"],
            materialization="table",
            post_hook="COLLECT STATISTICS ON {{ this }}",
            output_path=Path("models/intermediate/int_orders_enriched.sql")
        )

        assert sql is not None
        assert "post-hook" in sql
        assert "COLLECT STATISTICS" in sql

        # Verify file exists
        output_file = dbt_project_dir / "models/intermediate/int_orders_enriched.sql"
        assert output_file.exists()


class TestMartModelGeneration:
    """Tests for generating mart models (facts and dimensions)."""

    @pytest.mark.integration
    def test_generate_fact_table(self, dbt_generator, dbt_project_dir):
        """Test generating fact table with measures."""
        dbt_generator.create_project_structure(
            project_name="test_project",
            include_marts=True
        )

        sql = dbt_generator.generate_mart_model(
            model_name="fct_orders",
            model_type="fact",
            source_models=["int_orders_customers"],
            dimension_columns=[
                "order_date",
                "customer_id",
                "product_id"
            ],
            measure_columns=[
                {"name": "total_orders", "agg": "count(*)"},
                {"name": "total_revenue", "agg": "sum(order_amount)"},
                {"name": "avg_order_value", "agg": "avg(order_amount)"}
            ],
            grain="One row per order",
            materialization="table",
            post_hook="COLLECT STATISTICS ON {{ this }}",
            output_path=Path("models/marts/fct_orders.sql")
        )

        # Verify fact table content
        assert sql is not None
        assert "config(" in sql
        assert "materialized='table'" in sql
        assert "tags=[\"fact\", \"mart\"]" in sql
        assert "Grain: One row per order" in sql
        assert "total_orders" in sql
        assert "total_revenue" in sql
        assert "avg_order_value" in sql
        assert "group by" in sql.lower()
        assert "post-hook" in sql

        # Verify file was created
        output_file = dbt_project_dir / "models/marts/fct_orders.sql"
        assert output_file.exists()
        assert output_file.read_text() == sql

    @pytest.mark.integration
    def test_generate_dimension_table(self, dbt_generator, dbt_project_dir):
        """Test generating dimension table."""
        dbt_generator.create_project_structure(
            project_name="test_project",
            include_marts=True
        )

        sql = dbt_generator.generate_mart_model(
            model_name="dim_customers",
            model_type="dimension",
            source_models=["int_customer_enriched"],
            dimension_columns=[
                "customer_id",
                "customer_name",
                "email",
                "customer_segment",
                "created_at"
            ],
            grain="One row per customer",
            materialization="table",
            output_path=Path("models/marts/dim_customers.sql")
        )

        # Verify dimension table content
        assert sql is not None
        assert "config(" in sql
        assert "materialized='table'" in sql
        assert "tags=[\"dimension\", \"mart\"]" in sql
        assert "Grain: One row per customer" in sql
        assert "select distinct" in sql.lower()
        assert "customer_id" in sql
        assert "customer_name" in sql

        # Verify file was created
        output_file = dbt_project_dir / "models/marts/dim_customers.sql"
        assert output_file.exists()

    @pytest.mark.integration
    def test_generate_mart_in_subfolder(self, dbt_generator, dbt_project_dir):
        """Test generating mart model in business domain subfolder."""
        dbt_generator.create_project_structure(
            project_name="test_project",
            include_marts=True,
            mart_subfolders=["finance"]
        )

        sql = dbt_generator.generate_mart_model(
            model_name="fct_revenue",
            model_type="fact",
            source_models=["int_transactions"],
            dimension_columns=["transaction_date", "account_id"],
            measure_columns=[
                {"name": "total_revenue", "agg": "sum(amount)"}
            ],
            materialization="table",
            output_path=Path("models/marts/finance/fct_revenue.sql")
        )

        assert sql is not None

        # Verify file in subfolder
        output_file = dbt_project_dir / "models/marts/finance/fct_revenue.sql"
        assert output_file.exists()


class TestEndToEndWorkflow:
    """End-to-end tests for complete dbt project generation workflow."""

    @pytest.mark.integration
    def test_complete_project_setup(self, dbt_generator, dbt_project_dir):
        """Test creating complete project with all layers and models."""
        # Step 1: Create project structure
        structure_result = dbt_generator.create_project_structure(
            project_name="analytics_project",
            include_staging=True,
            include_intermediate=True,
            include_marts=True,
            include_tests=True,
            staging_materialization="view",
            intermediate_materialization="view",
            marts_materialization="table"
        )

        assert structure_result is not None
        assert len(structure_result["created_paths"]["folders"]) >= 4

        # Step 2: Generate staging model
        # Note: source_name="raw" is a logical grouping in dbt, not a Teradata schema
        # All tables are in dbt_dev database
        staging_sql = dbt_generator.generate_staging_model(
            model_name="stg_customers",
            source_name="raw",  # Logical source name for grouping
            table_name="customers",  # Actual table in dbt_dev
            columns=["customer_id", "first_name", "last_name", "email", "created_at"],
            output_path=Path("models/staging/stg_customers.sql")
        )
        assert staging_sql is not None
        assert (dbt_project_dir / "models/staging/stg_customers.sql").exists()

        # Step 3: Generate intermediate model
        intermediate_sql = dbt_generator.generate_intermediate_model(
            model_name="int_customer_enriched",
            source_models=["stg_customers"],
            select_columns=[
                "customer_id",
                "first_name || ' ' || last_name as full_name",
                "email",
                "created_at"
            ],
            materialization="view",
            output_path=Path("models/intermediate/int_customer_enriched.sql")
        )
        assert intermediate_sql is not None
        assert (dbt_project_dir / "models/intermediate/int_customer_enriched.sql").exists()

        # Step 4: Generate mart (dimension) model
        mart_sql = dbt_generator.generate_mart_model(
            model_name="dim_customers",
            model_type="dimension",
            source_models=["int_customer_enriched"],
            dimension_columns=[
                "customer_id",
                "full_name",
                "email",
                "created_at"
            ],
            grain="One row per customer",
            materialization="table",
            output_path=Path("models/marts/dim_customers.sql")
        )
        assert mart_sql is not None
        assert (dbt_project_dir / "models/marts/dim_customers.sql").exists()

        # Verify all files exist in proper structure
        models_dir = dbt_project_dir / "models"
        assert (models_dir / "staging").exists()
        assert (models_dir / "intermediate").exists()
        assert (models_dir / "marts").exists()
        assert (models_dir / "staging/stg_customers.sql").exists()
        assert (models_dir / "intermediate/int_customer_enriched.sql").exists()
        assert (models_dir / "marts/dim_customers.sql").exists()

    @pytest.mark.integration
    def test_project_with_multiple_marts_and_facts(self, dbt_generator, dbt_project_dir):
        """Test creating project with multiple fact and dimension tables."""
        # Create structure with business domains
        dbt_generator.create_project_structure(
            project_name="enterprise_analytics",
            include_staging=True,
            include_intermediate=True,
            include_marts=True,
            mart_subfolders=["sales", "finance"],
            marts_materialization="table"
        )

        # Generate sales fact
        sales_fact = dbt_generator.generate_mart_model(
            model_name="fct_sales",
            model_type="fact",
            source_models=["int_orders"],
            dimension_columns=["order_date", "customer_id", "product_id"],
            measure_columns=[
                {"name": "total_sales", "agg": "sum(sales_amount)"},
                {"name": "order_count", "agg": "count(*)"}
            ],
            materialization="table",
            output_path=Path("models/marts/sales/fct_sales.sql")
        )

        # Generate finance fact
        finance_fact = dbt_generator.generate_mart_model(
            model_name="fct_revenue",
            model_type="fact",
            source_models=["int_transactions"],
            dimension_columns=["transaction_date", "account_id"],
            measure_columns=[
                {"name": "total_revenue", "agg": "sum(amount)"}
            ],
            materialization="table",
            output_path=Path("models/marts/finance/fct_revenue.sql")
        )

        # Verify both facts exist in correct subfolders
        assert (dbt_project_dir / "models/marts/sales/fct_sales.sql").exists()
        assert (dbt_project_dir / "models/marts/finance/fct_revenue.sql").exists()
        assert sales_fact is not None
        assert finance_fact is not None
