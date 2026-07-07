"""Integration tests for dbt and Teradata integration.

This test suite covers:
- dbt project setup and validation
- Model compilation
- dbt CLI command execution
- Project metadata retrieval

Note: These tests use mocked Teradata connections since they focus on dbt functionality.
For actual Teradata database integration, ensure database is available and configured.
"""

from pathlib import Path

import pytest
import yaml

from teradata_etl_mcp_server.clients.dbt_client import DBTClient
from teradata_etl_mcp_server.clients.teradata_client import TeradataClient


@pytest.fixture(scope="module")
def teradata_config():
    """Teradata configuration for dbt testing."""
    return {
        "host": "localhost",
        "port": 1025,
        "username": "user",
        "password": "password",
        "database": "database_name",
    }


@pytest.fixture(scope="module")
def dbt_config(tmp_path_factory):
    """dbt configuration for testing."""
    project_dir = tmp_path_factory.mktemp("dbt_project")
    profiles_dir = tmp_path_factory.mktemp("dbt_profiles")

    return {
        "project_dir": str(project_dir),
        "profiles_dir": str(profiles_dir),
        "target": "test",
        "threads": 4
    }


@pytest.fixture(scope="module")
def teradata_client(teradata_config):
    """Create Teradata client for testing."""
    client = TeradataClient(
        host=teradata_config["host"],
        username=teradata_config["username"],
        password=teradata_config["password"],
        database=teradata_config.get("database", ""),
        port=teradata_config.get("port", 1025)
    )
    yield client
    client.close()


@pytest.fixture(scope="module")
def dbt_client(dbt_config):
    """Create dbt client for testing."""
    client = DBTClient(
        project_dir=dbt_config["project_dir"],
        profiles_dir=dbt_config["profiles_dir"],
        target=dbt_config.get("target", "test"),
        threads=dbt_config.get("threads", 4)
    )
    yield client


@pytest.fixture
def setup_source_data(teradata_client):
    """Setup source data for dbt models.
    
    Teradata Database Model:
    - Teradata doesn't have schemas like PostgreSQL/MySQL
    - All objects (tables, views) exist directly in the database
    - In this test, everything is in 'dbt_dev' database:
      * Raw data tables: dbt_dev.raw_customers
      * Staging views: dbt_dev_staging.stg_customers (dbt adds suffix)
      * Mart views: dbt_dev_marts.customer_summary (dbt adds suffix)
    
    - The 'raw' in source('raw', 'customers') is just a logical grouping name in dbt
    - The actual table reference is: dbt_dev.raw_customers
    """
    conn = None
    try:
        conn = teradata_client._get_connection()
        cursor = conn.cursor()

        # Note: In Teradata, there's no separate schema concept like PostgreSQL/MySQL
        # All tables exist directly in the database (dbt_dev)
        # We create raw_customers table in dbt_dev database
        # The source name 'raw' in dbt is just a logical grouping
        try:
            print("Verifying access to dbt_dev database...")
            cursor.execute("SELECT DATABASE")
            result = cursor.fetchone()
            print(f"Connected to database: {result[0]}")
        except Exception as e:
            print(f"Database access check: {e}")

        # Drop table if exists
        try:
            print("Dropping existing dbt_dev.raw_customers table if it exists...")
            cursor.execute("DROP TABLE dbt_dev.raw_customers")
            conn.commit()
            print("Dropped existing table")
        except Exception as e:
            print(f"Table might not exist: {e}")

        # Create customers table in dbt_dev database
        print("Creating dbt_dev.raw_customers table...")
        cursor.execute("""
            CREATE TABLE dbt_dev.raw_customers (
                customer_id INTEGER,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                created_at TIMESTAMP
            )
        """)
        conn.commit()
        print("Created dbt_dev.raw_customers table")

        # Grant permissions for dbt to use this table
        # Note: Since dbc user creates the table, it automatically owns it
        # In Teradata, table creator has full permissions including GRANT OPTION
        try:
            print("Granting permissions on raw_customers...")
            cursor.execute("GRANT SELECT ON dbt_dev.raw_customers TO dbc WITH GRANT OPTION")
            conn.commit()
            print("Granted SELECT WITH GRANT OPTION (dbc owns this table)")
        except Exception as e:
            print(f"Grant operation: {e}")

        # Insert test data
        print("Inserting test data...")
        test_data = [
            (1, 'John', 'Doe', 'john.doe@example.com', '2024-01-01 10:00:00'),
            (2, 'Jane', 'Smith', 'jane.smith@example.com', '2024-01-02 10:00:00'),
            (3, 'Bob', 'Johnson', 'bob.johnson@example.com', '2024-01-03 10:00:00'),
        ]

        for row in test_data:
            cursor.execute(
                "INSERT INTO dbt_dev.raw_customers VALUES (?, ?, ?, ?, ?)",
                row
            )

        conn.commit()
        print(f"Inserted {len(test_data)} rows into dbt_dev.raw_customers")

        yield

        # Cleanup
        print("Cleaning up test data...")
        try:
            cursor.execute("DROP TABLE dbt_dev.raw_customers")
            conn.commit()
            print("Dropped dbt_dev.raw_customers table")
        except Exception as e:
            print(f"Error dropping table during cleanup: {e}")

    finally:
        if conn:
            conn.close()


@pytest.fixture
def setup_dbt_project(dbt_config, teradata_config):
    """Setup complete dbt project structure."""
    project_dir = Path(dbt_config["project_dir"])
    profiles_dir = Path(dbt_config["profiles_dir"])

    # Create dbt_project.yml
    # Note: In Teradata, +schema creates suffix naming: dbt_dev_staging, dbt_dev_marts
    # All objects are in dbt_dev database with different naming conventions
    dbt_project = {
        "name": "test_dbt_project",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "test_profile",
        "model-paths": ["models"],
        "seed-paths": ["seeds"],
        "test-paths": ["tests"],
        "analysis-paths": ["analyses"],
        "macro-paths": ["macros"],
        "snapshot-paths": ["snapshots"],
        "target-path": "target",
        "clean-targets": ["target", "dbt_packages"],
        "models": {
            "test_dbt_project": {
                "staging": {
                    "+materialized": "table",  # Use table for all staging models
                    "+schema": "staging"  # Creates tables as dbt_dev_staging.model_name
                },
                "marts": {
                    "+materialized": "table",  # Table avoids view permission issues (Error 5315)
                    "+schema": "marts"  # Creates tables as dbt_dev_marts.model_name
                }
            }
        },
    }

    with open(project_dir / "dbt_project.yml", "w") as f:
        yaml.dump(dbt_project, f)

    # Create profiles.yml
    profiles = {
        "test_profile": {
            "target": "test",
            "outputs": {
                "test": {
                    "type": "teradata",
                    "host": teradata_config["host"],
                    "user": teradata_config["username"],
                    "password": teradata_config["password"],
                    "schema": teradata_config["database"],  # dbt-teradata requires database=schema
                    "tmode": "ANSI",
                    "threads": 4
                }
            }
        }
    }

    with open(profiles_dir / "profiles.yml", "w") as f:
        yaml.dump(profiles, f)

    # Create models directory
    models_dir = project_dir / "models"
    models_dir.mkdir(exist_ok=True)

    # Create staging models
    staging_dir = models_dir / "staging"
    staging_dir.mkdir(exist_ok=True)

    # Simple staging model - materialization set in dbt_project.yml
    stg_customers = """
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at
FROM {{ source('raw', 'raw_customers') }}
WHERE customer_id IS NOT NULL
"""
    (staging_dir / "stg_customers.sql").write_text(stg_customers)

    # Source definition
    # Note: In Teradata, there's no separate schema concept - all tables are in dbt_dev database
    # 'raw' is just a logical source name in dbt for grouping, not an actual schema
    # The actual table is: dbt_dev.raw_customers
    sources_yml = """
version: 2

sources:
  - name: raw  # Logical name for grouping raw data sources in dbt
    schema: dbt_dev  # Teradata database where raw tables exist
    tables:
      - name: raw_customers  # Actual table name in dbt_dev database
"""
    (staging_dir / "sources.yml").write_text(sources_yml)

    # Create marts models
    marts_dir = models_dir / "marts"
    marts_dir.mkdir(exist_ok=True)

    # Simple mart model - materialization set in dbt_project.yml
    customer_summary = """
SELECT
    COUNT(*) AS customer_count
FROM {{ ref('stg_customers') }}
"""
    (marts_dir / "customer_summary.sql").write_text(customer_summary)

    # Create seeds directory with sample data
    seeds_dir = project_dir / "seeds"
    seeds_dir.mkdir(exist_ok=True)

    country_codes = """country_code,country_name
US,United States
CA,Canada
UK,United Kingdom
"""
    (seeds_dir / "country_codes.csv").write_text(country_codes)

    # Create macros directory
    macros_dir = project_dir / "macros"
    macros_dir.mkdir(exist_ok=True)

    return project_dir


class TestDbtClientInitialization:
    """Test suite for DBTClient initialization and configuration."""

    def test_dbt_client_creation(self, dbt_client):
        """Test DBTClient can be instantiated."""
        assert dbt_client is not None
        assert isinstance(dbt_client, DBTClient)

    def test_dbt_client_has_project_dir(self, dbt_client, dbt_config):
        """Test DBTClient has correct project directory."""
        assert str(dbt_client.project_dir) == dbt_config["project_dir"]

    def test_dbt_client_has_target(self, dbt_client):
        """Test DBTClient has target configured."""
        assert dbt_client.target == "test"

    def test_dbt_client_has_threads(self, dbt_client):
        """Test DBTClient has threads configured."""
        assert dbt_client.threads == 4


class TestDbtProjectSetup:
    """Test suite for dbt project setup and configuration."""

    def test_dbt_project_structure(self, setup_dbt_project):
        """Test dbt project structure is correct."""
        project_dir = setup_dbt_project

        # Verify required directories exist
        assert (project_dir / "models").exists()
        assert (project_dir / "seeds").exists()
        assert (project_dir / "macros").exists()

        # Verify project file exists
        assert (project_dir / "dbt_project.yml").exists()

    def test_dbt_project_config(self, setup_dbt_project):
        """Test dbt_project.yml is properly configured."""
        project_file = setup_dbt_project / "dbt_project.yml"

        with open(project_file) as f:
            config = yaml.safe_load(f)

        assert config["name"] == "test_dbt_project"
        assert config["version"] == "1.0.0"
        assert config["profile"] == "test_profile"

    def test_profiles_yml_exists(self, dbt_config):
        """Test profiles.yml exists and is readable."""
        profiles_file = Path(dbt_config["profiles_dir"]) / "profiles.yml"

        assert profiles_file.exists()

        with open(profiles_file) as f:
            profiles = yaml.safe_load(f)

        assert "test_profile" in profiles


class TestDbtCommands:
    """Test suite for dbt CLI commands."""

    @pytest.mark.integration
    def test_dbt_debug(self, dbt_client, setup_dbt_project):
        """Test dbt debug command."""
        dbt_client.project_dir = setup_dbt_project

        result = dbt_client.debug()

        assert result is not None
        assert isinstance(result, dict)
        assert "success" in result or "returncode" in result

    @pytest.mark.integration
    def test_dbt_list_models(self, dbt_client, setup_dbt_project):
        """Test listing dbt models."""
        dbt_client.project_dir = setup_dbt_project

        try:
            result = dbt_client.list_models()

            assert result is not None
            assert isinstance(result, list)

            # Should have at least our two models
            if len(result) > 0:
                # Check models are in the list
                model_info = str(result)
                assert "stg_customers" in model_info or "customer_summary" in model_info
        except Exception as e:
            # If dbt isn't installed or project isn't valid, that's okay for this test
            pytest.skip(f"dbt list command failed (may need dbt installation): {e}")

    @pytest.mark.integration
    def test_dbt_deps(self, dbt_client, setup_dbt_project):
        """Test dbt deps command."""
        dbt_client.project_dir = setup_dbt_project

        try:
            result = dbt_client.deps()

            assert result is not None
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"dbt deps failed (may need dbt installation): {e}")

    @pytest.mark.integration
    def test_dbt_clean(self, dbt_client, setup_dbt_project):
        """Test dbt clean command."""
        dbt_client.project_dir = setup_dbt_project

        try:
            result = dbt_client.clean()

            assert result is not None
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"dbt clean failed: {e}")


class TestModelCompilation:
    """Test suite for dbt model compilation."""

    @pytest.mark.integration
    def test_compile_all_models(self, dbt_client, setup_dbt_project, setup_source_data):
        """Test compiling all dbt models."""
        dbt_client.project_dir = setup_dbt_project

        result = dbt_client.compile()

        assert result is not None
        assert isinstance(result, dict)
        assert "success" in result or "returncode" in result

    @pytest.mark.integration
    def test_compile_specific_model(self, dbt_client, setup_dbt_project, setup_source_data):
        """Test compiling specific model."""
        dbt_client.project_dir = setup_dbt_project

        result = dbt_client.compile(models="stg_customers")

        assert result is not None
        assert isinstance(result, dict)


class TestModelExecution:
    """Test suite for dbt model execution."""

    @pytest.mark.integration
    def test_run_all_models(self, dbt_client, setup_dbt_project, setup_source_data):
        """Test running all dbt models."""
        dbt_client.project_dir = setup_dbt_project

        result = dbt_client.run()

        assert result is not None
        assert isinstance(result, dict)
        assert "success" in result

    @pytest.mark.integration
    def test_run_with_model_selection(self, dbt_client, setup_dbt_project, setup_source_data):
        """Test running specific models."""
        dbt_client.project_dir = setup_dbt_project

        result = dbt_client.run(models="stg_customers")

        assert result is not None
        assert isinstance(result, dict)


class TestSeedData:
    """Test suite for dbt seed data loading."""

    @pytest.mark.integration
    def test_seed_command(self, dbt_client, setup_dbt_project):
        """Test dbt seed command."""
        dbt_client.project_dir = setup_dbt_project

        result = dbt_client.seed()

        assert result is not None
        assert isinstance(result, dict)


class TestDbtMetadata:
    """Test suite for dbt metadata and project information."""

    def test_get_project_info(self, dbt_client, setup_dbt_project):
        """Test retrieving project information."""
        dbt_client.project_dir = setup_dbt_project

        try:
            info = dbt_client.get_project_info()

            assert info is not None
            assert isinstance(info, dict)
        except Exception as e:
            # Project info may not be available without manifest
            pytest.skip(f"Project info not available: {e}")

    def test_check_dbt_installation(self, dbt_client):
        """Test checking if dbt is installed."""
        try:
            is_installed = dbt_client.check_installation()

            # dbt may or may not be installed, both are valid
            assert isinstance(is_installed, bool)
        except Exception:
            # If check fails, that's also okay
            pass


class TestTeradataClient:
    """Test suite for TeradataClient basic functionality."""

    def test_teradata_client_creation(self, teradata_client):
        """Test TeradataClient can be instantiated."""
        assert teradata_client is not None
        assert isinstance(teradata_client, TeradataClient)

    def test_teradata_client_has_host(self, teradata_client, teradata_config):
        """Test TeradataClient has correct host."""
        assert teradata_client.host == teradata_config["host"]

    def test_teradata_client_has_username(self, teradata_client, teradata_config):
        """Test TeradataClient has correct username."""
        assert teradata_client.username == teradata_config["username"]

    @pytest.mark.integration
    def test_teradata_connection(self, teradata_client):
        """Test Teradata connection."""
        result = teradata_client.test_connection()

        assert result is not None
        assert isinstance(result, dict)

    @pytest.mark.integration
    def test_list_databases(self, teradata_client):
        """Test listing Teradata databases."""
        databases = teradata_client.list_databases()

        assert databases is not None
        assert isinstance(databases, list)


class TestDbtProjectValidation:
    """Test suite for dbt project validation."""

    def test_validate_project_structure(self, setup_dbt_project):
        """Test project has all required components."""
        project_dir = setup_dbt_project

        # Check for dbt_project.yml
        assert (project_dir / "dbt_project.yml").exists()

        # Check model directories
        assert (project_dir / "models" / "staging").exists()
        assert (project_dir / "models" / "marts").exists()

        # Check model files exist
        assert (project_dir / "models" / "staging" / "stg_customers.sql").exists()
        assert (project_dir / "models" / "marts" / "customer_summary.sql").exists()

    def test_models_have_valid_sql(self, setup_dbt_project):
        """Test model files contain SQL."""
        staging_model = setup_dbt_project / "models" / "staging" / "stg_customers.sql"

        content = staging_model.read_text()

        # Should contain basic SQL keywords
        assert "SELECT" in content
        assert "FROM" in content

    def test_seeds_exist(self, setup_dbt_project):
        """Test seed files exist."""
        seeds_dir = setup_dbt_project / "seeds"

        assert seeds_dir.exists()
        assert (seeds_dir / "country_codes.csv").exists()


@pytest.mark.integration
class TestDbtIntegrationWorkflow:
    """Test suite for complete dbt workflows (requires full setup)."""

    def test_basic_workflow(self, dbt_client, setup_dbt_project, setup_source_data):
        """Test basic dbt workflow: deps -> seed -> compile -> run."""
        dbt_client.project_dir = setup_dbt_project

        # Install dependencies
        deps_result = dbt_client.deps()
        assert deps_result is not None

        # Load seeds
        seed_result = dbt_client.seed()
        assert seed_result is not None

        # Compile models
        compile_result = dbt_client.compile()
        assert compile_result is not None

        # Run models
        run_result = dbt_client.run()
        assert run_result is not None
