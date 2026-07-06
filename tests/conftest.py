"""Pytest configuration and shared fixtures for all tests.

This module provides shared fixtures and configuration for:
- Test client instances (Teradata, dbt, Airbyte, Airflow)
- Test databases and schemas
- Common test data
- Mock services
- Temporary directories
- Async test support
"""

import asyncio
import os

import pytest

# Load environment variables from .env file BEFORE any other imports
# This ensures all tests have access to configured credentials
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv is not installed; tests rely on environment variables being set directly
    pass
import json
from datetime import datetime
from pathlib import Path

from elt_mcp_server.clients.airbyte_client import AirbyteClient
from elt_mcp_server.clients.async_airflow_client import AsyncAirflowClient
from elt_mcp_server.clients.dbt_client import DBTClient

# Import client classes
from elt_mcp_server.clients.teradata_client import TeradataClient
from elt_mcp_server.utils.validators import DataValidator


# Optional service stubs (modules deleted or not present in src) to avoid ImportError
class PluginManager:  # minimal stub — module deleted
    def __init__(self, config=None):
        self.config = config or {}


class SchemaRegistry:  # minimal stub
    pass

class ConflictResolver:  # minimal stub
    pass


# ============================================================================
# Pytest Configuration
# ============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "requires_teradata: mark test as requiring Teradata connection"
    )
    config.addinivalue_line(
        "markers", "requires_dbt: mark test as requiring dbt installation"
    )
    config.addinivalue_line(
        "markers", "requires_airbyte: mark test as requiring Airbyte"
    )
    config.addinivalue_line(
        "markers", "requires_airflow: mark test as requiring Airflow"
    )


# ============================================================================
# Async Event Loop Configuration
# ============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# ============================================================================
# Configuration Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def teradata_config():
    """Teradata configuration from environment or defaults."""
    return {
        "host": os.getenv("TERADATA_HOST", "localhost"),
        "username": os.getenv("TERADATA_USER", "dbc"),
        "password": os.getenv("TERADATA_PASSWORD", "dbc"),
        "database": os.getenv("TERADATA_DATABASE", "test_db"),
        "port": int(os.getenv("TERADATA_PORT", "1025"))
    }


@pytest.fixture(scope="session")
def dbt_config(tmp_path_factory):
    """dbt configuration for testing."""
    dbt_project_dir = tmp_path_factory.mktemp("dbt_project")
    dbt_profiles_dir = tmp_path_factory.mktemp("dbt_profiles")

    return {
        "project_dir": str(dbt_project_dir),
        "profiles_dir": str(dbt_profiles_dir),
        "target": os.getenv("DBT_TARGET", "dev"),
        "threads": int(os.getenv("DBT_THREADS", "4"))
    }


@pytest.fixture(scope="session")
def airbyte_config():
    """Airbyte configuration from environment or defaults."""
    return {
        "base_url": os.getenv("AIRBYTE_URL", "http://localhost:8000"),
        "api_key": os.getenv("AIRBYTE_API_KEY", "test_api_key"),
        "workspace_id": os.getenv("AIRBYTE_WORKSPACE_ID", "default")
    }


@pytest.fixture(scope="session")
def airflow_config():
    """Airflow configuration from environment or defaults."""
    return {
        "base_url": os.getenv("AIRFLOW_URL", "http://localhost:8080"),
        "username": os.getenv("AIRFLOW_USER", "admin"),
        "password": os.getenv("AIRFLOW_PASSWORD", "admin"),
        "api_version": "v1"
    }


@pytest.fixture(scope="session")
def plugin_config(tmp_path_factory):
    """Plugin system configuration."""
    plugin_dir = tmp_path_factory.mktemp("plugins")

    return {
        "plugin_dir": str(plugin_dir),
        "enabled_plugins": [],
        "auto_reload": True,
        "reload_interval": 5,
        "sandbox_enabled": True,
        "max_execution_time": 300
    }


# ============================================================================
# Client Fixtures
# ============================================================================

@pytest.fixture(scope="module")
async def teradata_client(teradata_config):
    """Create Teradata client instance."""
    client = TeradataClient(**teradata_config)

    try:
        await client.connect()
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="module")
async def dbt_client(dbt_config):
    """Create dbt client instance."""
    client = DBTClient(
        project_dir=Path(dbt_config["project_dir"]),
        profiles_dir=Path(dbt_config["profiles_dir"]),
        target=dbt_config.get("target", "dev"),
        threads=int(dbt_config.get("threads", 4)),
    )

    # Setup dbt project structure
    await setup_dbt_project(dbt_config)

    yield client

    await client.close()


@pytest.fixture(scope="module")
async def airbyte_client(airbyte_config):
    """Create Airbyte client instance."""
    client = AirbyteClient(
        base_url=airbyte_config["base_url"],
        username=airbyte_config.get("username", "airbyte"),
        password=airbyte_config.get("password", "password"),
        timeout=60,
    )
    yield client
    await client.close()


@pytest.fixture(scope="module")
async def airflow_client(airflow_config):
    """Create async Airflow client instance."""
    client = AsyncAirflowClient(
        base_url=airflow_config["base_url"],
        username=airflow_config.get("username", ""),
        password=airflow_config.get("password", ""),
        timeout=30,
    )
    yield client
    await client.close()


# ============================================================================
# Service Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def data_validator():
    """Create data validator instance."""
    return DataValidator()


@pytest.fixture(scope="module")
def schema_registry():
    """Create schema registry instance."""
    return SchemaRegistry()


@pytest.fixture(scope="module")
def conflict_resolver():
    """Create conflict resolver instance."""
    return ConflictResolver()


@pytest.fixture(scope="module")
def plugin_manager(plugin_config):
    """Create plugin manager instance."""
    return PluginManager(plugin_config)


# ============================================================================
# Database Fixtures
# ============================================================================

@pytest.fixture(scope="module")
async def test_database(teradata_client, teradata_config):
    """Create and setup test database."""
    db_name = teradata_config["database"]

    # Create database
    try:
        await teradata_client.execute(
            f"CREATE DATABASE {db_name} AS PERMANENT = 60e6;"
        )
    except Exception:
        # Database might already exist
        pass

    yield db_name

    # Cleanup
    try:
        await teradata_client.execute(f"DROP DATABASE {db_name};")
    except Exception:
        pass


@pytest.fixture
async def test_schema(teradata_client, test_database):
    """Create test schema/tables for a test."""
    schema_name = f"test_schema_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # Create schema (in Teradata, this is a database)
    await teradata_client.execute(
        f"CREATE DATABASE {schema_name} AS PERMANENT = 10e6;"
    )

    yield schema_name

    # Cleanup
    try:
        await teradata_client.execute(f"DROP DATABASE {schema_name};")
    except Exception:
        pass


@pytest.fixture
async def test_tables(teradata_client, test_database):
    """Create standard test tables."""
    db = test_database
    tables = []

    # Customers table
    table_name = f"{db}.test_customers"
    await teradata_client.execute(f"""
        CREATE TABLE {table_name} (
            customer_id INTEGER,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(200),
            country VARCHAR(50),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)
    tables.append(table_name)

    # Orders table
    table_name = f"{db}.test_orders"
    await teradata_client.execute(f"""
        CREATE TABLE {table_name} (
            order_id INTEGER,
            customer_id INTEGER,
            order_date DATE,
            amount DECIMAL(10,2),
            status VARCHAR(50),
            created_at TIMESTAMP
        )
    """)
    tables.append(table_name)

    # Products table
    table_name = f"{db}.test_products"
    await teradata_client.execute(f"""
        CREATE TABLE {table_name} (
            product_id INTEGER,
            product_name VARCHAR(200),
            category VARCHAR(100),
            price DECIMAL(10,2),
            stock_quantity INTEGER
        )
    """)
    tables.append(table_name)

    yield tables

    # Cleanup
    for table in tables:
        try:
            await teradata_client.execute(f"DROP TABLE {table};")
        except Exception:
            pass


# ============================================================================
# Test Data Fixtures
# ============================================================================

@pytest.fixture
def sample_customers():
    """Sample customer data for testing."""
    return [
        {
            "customer_id": 1,
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@example.com",
            "country": "USA"
        },
        {
            "customer_id": 2,
            "first_name": "Jane",
            "last_name": "Smith",
            "email": "jane.smith@example.com",
            "country": "UK"
        },
        {
            "customer_id": 3,
            "first_name": "Bob",
            "last_name": "Johnson",
            "email": "bob.johnson@example.com",
            "country": "Canada"
        }
    ]


@pytest.fixture
def sample_orders():
    """Sample order data for testing."""
    return [
        {
            "order_id": 1,
            "customer_id": 1,
            "order_date": "2025-01-15",
            "amount": 100.00,
            "status": "completed"
        },
        {
            "order_id": 2,
            "customer_id": 1,
            "order_date": "2025-02-20",
            "amount": 150.00,
            "status": "completed"
        },
        {
            "order_id": 3,
            "customer_id": 2,
            "order_date": "2025-03-10",
            "amount": 200.00,
            "status": "pending"
        }
    ]


@pytest.fixture
def sample_products():
    """Sample product data for testing."""
    return [
        {
            "product_id": 1,
            "product_name": "Laptop",
            "category": "Electronics",
            "price": 999.99,
            "stock_quantity": 50
        },
        {
            "product_id": 2,
            "product_name": "Mouse",
            "category": "Electronics",
            "price": 29.99,
            "stock_quantity": 200
        },
        {
            "product_id": 3,
            "product_name": "Desk",
            "category": "Furniture",
            "price": 299.99,
            "stock_quantity": 25
        }
    ]


# ============================================================================
# dbt Project Setup Fixtures
# ============================================================================

async def setup_dbt_project(dbt_config):
    """Setup basic dbt project structure."""
    project_dir = Path(dbt_config["project_dir"])
    profiles_dir = Path(dbt_config["profiles_dir"])

    # Create directories
    (project_dir / "models").mkdir(parents=True, exist_ok=True)
    (project_dir / "models" / "staging").mkdir(exist_ok=True)
    (project_dir / "models" / "marts").mkdir(exist_ok=True)
    (project_dir / "tests").mkdir(exist_ok=True)
    (project_dir / "seeds").mkdir(exist_ok=True)
    (project_dir / "snapshots").mkdir(exist_ok=True)
    (project_dir / "macros").mkdir(exist_ok=True)
    profiles_dir.mkdir(parents=True, exist_ok=True)

    # Create dbt_project.yml
    dbt_project_yml = {
        "name": "test_project",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "test_profile",
        "model-paths": ["models"],
        "analysis-paths": ["analyses"],
        "test-paths": ["tests"],
        "seed-paths": ["seeds"],
        "macro-paths": ["macros"],
        "snapshot-paths": ["snapshots"],
        "target-path": "target",
        "clean-targets": ["target", "dbt_packages"],
        "models": {
            "test_project": {
                "staging": {
                    "materialized": "view"
                },
                "marts": {
                    "materialized": "table"
                }
            }
        }
    }

    with open(project_dir / "dbt_project.yml", "w") as f:
        import yaml
        yaml.dump(dbt_project_yml, f)

    # Create profiles.yml
    profiles_yml = {
        "test_profile": {
            "target": dbt_config["target"],
            "outputs": {
                dbt_config["target"]: {
                    "type": "teradata",
                    "host": os.getenv("TERADATA_HOST", "localhost"),
                    "user": os.getenv("TERADATA_USER", "dbc"),
                    "password": os.getenv("TERADATA_PASSWORD", "dbc"),
                    "database": os.getenv("TERADATA_DATABASE", "test_db"),
                    "schema": "analytics",
                    "threads": dbt_config["threads"]
                }
            }
        }
    }

    with open(profiles_dir / "profiles.yml", "w") as f:
        import yaml
        yaml.dump(profiles_yml, f)


@pytest.fixture
def dbt_project_structure(dbt_config):
    """Create a complete dbt project structure with sample models."""
    project_dir = Path(dbt_config["project_dir"])

    # Sample staging model
    staging_model = """
    SELECT
        customer_id,
        TRIM(first_name) as first_name,
        TRIM(last_name) as last_name,
        LOWER(email) as email,
        created_at
    FROM {{ source('raw', 'raw_customers') }}
    """

    (project_dir / "models" / "staging" / "stg_customers.sql").write_text(staging_model)

    # Sample marts model
    marts_model = """
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        COUNT(*) as order_count
    FROM {{ ref('stg_customers') }}
    GROUP BY customer_id, first_name, last_name, email
    """

    (project_dir / "models" / "marts" / "customer_summary.sql").write_text(marts_model)

    # Sample source definition
    sources_yml = {
        "version": 2,
        "sources": [
            {
                "name": "raw",
                "database": os.getenv("TERADATA_DATABASE", "test_db"),
                "tables": [
                    {"name": "raw_customers"}
                ]
            }
        ]
    }

    with open(project_dir / "models" / "sources.yml", "w") as f:
        import yaml
        yaml.dump(sources_yml, f)

    return project_dir


# ============================================================================
# Temporary Directory Fixtures
# ============================================================================

@pytest.fixture
def temp_dir(tmp_path):
    """Provide temporary directory for test artifacts."""
    yield tmp_path
    # Cleanup handled by tmp_path


@pytest.fixture
def temp_file(tmp_path):
    """Create temporary file for testing."""
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("test content")
    yield temp_file


@pytest.fixture
def temp_json_file(tmp_path):
    """Create temporary JSON file for testing."""
    temp_file = tmp_path / "test_data.json"
    data = {"key": "value", "number": 42}
    temp_file.write_text(json.dumps(data))
    yield temp_file


@pytest.fixture
def temp_csv_file(tmp_path):
    """Create temporary CSV file for testing."""
    temp_file = tmp_path / "test_data.csv"
    csv_content = "id,name,value\n1,test1,100\n2,test2,200\n"
    temp_file.write_text(csv_content)
    yield temp_file


# ============================================================================
# Mock Service Fixtures
# ============================================================================

@pytest.fixture
def mock_teradata_client():
    """Mock Teradata client for unit tests."""
    from unittest.mock import AsyncMock, Mock

    mock_client = Mock()
    mock_client.connect = AsyncMock()
    mock_client.close = AsyncMock()
    mock_client.execute = AsyncMock(return_value=None)
    mock_client.query = AsyncMock(return_value=[])
    mock_client.get_table_schema = AsyncMock(return_value={"columns": []})

    return mock_client


@pytest.fixture
def mock_dbt_client():
    """Mock dbt client for unit tests."""
    from unittest.mock import AsyncMock, Mock

    mock_client = Mock()
    mock_client.run = AsyncMock(return_value={"success": True})
    mock_client.test = AsyncMock(return_value={"passed": True})
    mock_client.compile = AsyncMock(return_value={"compiled": True})
    mock_client.parse_project = AsyncMock(return_value={"nodes": {}})

    return mock_client


@pytest.fixture
def mock_airbyte_client():
    """Mock Airbyte client for unit tests."""
    from unittest.mock import AsyncMock, Mock

    mock_client = Mock()
    mock_client.create_connection = AsyncMock(return_value={"connectionId": "test_conn"})
    mock_client.trigger_sync = AsyncMock(return_value={"jobId": "test_job"})
    mock_client.get_sync_status = AsyncMock(return_value={"status": "completed"})

    return mock_client


@pytest.fixture
def mock_airflow_client():
    """Mock Airflow client for unit tests."""
    from unittest.mock import AsyncMock, Mock

    mock_client = Mock()
    mock_client.list_dags = AsyncMock(return_value=[])
    mock_client.trigger_dag = AsyncMock(return_value={"dag_run_id": "test_run"})
    mock_client.get_dag_run_status = AsyncMock(return_value={"state": "success"})

    return mock_client


# ============================================================================
# Utility Fixtures
# ============================================================================

@pytest.fixture
def assert_async():
    """Helper for async assertions."""
    async def _assert(coro, expected=True):
        result = await coro
        assert result == expected

    return _assert


@pytest.fixture
def wait_for_condition():
    """Wait for condition to become true."""
    async def _wait(condition_func, timeout=10, interval=0.5):
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < timeout:
            if await condition_func():
                return True
            await asyncio.sleep(interval)

        return False

    return _wait


@pytest.fixture
def capture_logs(caplog):
    """Capture logs during test execution."""
    import logging
    caplog.set_level(logging.INFO)
    yield caplog


# ============================================================================
# Performance Testing Fixtures
# ============================================================================

@pytest.fixture
def benchmark_timer():
    """Timer for performance benchmarking."""
    import time

    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None

        def start(self):
            self.start_time = time.time()

        def stop(self):
            self.end_time = time.time()

        @property
        def elapsed(self):
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return None

    return Timer()


# ============================================================================
# Cleanup Fixtures
# ============================================================================

@pytest.fixture(autouse=True)
async def cleanup_after_test():
    """Cleanup resources after each test."""
    yield
    # Cleanup code runs after test
    await asyncio.sleep(0.1)  # Allow async tasks to complete


# ============================================================================
# Skip Markers for Missing Services
# ============================================================================

def pytest_collection_modifyitems(config, items):
    """Modify test collection to skip tests based on available services."""
    # Check for service availability
    skip_teradata = pytest.mark.skip(reason="Teradata not available")
    skip_dbt = pytest.mark.skip(reason="dbt not available")
    skip_airbyte = pytest.mark.skip(reason="Airbyte not available")
    skip_airflow = pytest.mark.skip(reason="Airflow not available")

    for item in items:
        if "requires_teradata" in item.keywords:
            if not is_teradata_available():
                item.add_marker(skip_teradata)

        if "requires_dbt" in item.keywords:
            if not is_dbt_available():
                item.add_marker(skip_dbt)

        if "requires_airbyte" in item.keywords:
            if not is_airbyte_available():
                item.add_marker(skip_airbyte)

        if "requires_airflow" in item.keywords:
            if not is_airflow_available():
                item.add_marker(skip_airflow)


def is_teradata_available():
    """Check if Teradata is available."""
    # Check environment variable or try connection
    return os.getenv("TERADATA_AVAILABLE", "false").lower() == "true"


def is_dbt_available():
    """Check if dbt is available."""
    try:
        import dbt
        return True
    except ImportError:
        return False


def is_airbyte_available():
    """Check if Airbyte is available."""
    return os.getenv("AIRBYTE_AVAILABLE", "false").lower() == "true"


def is_airflow_available():
    """Check if Airflow is available."""
    return os.getenv("AIRFLOW_AVAILABLE", "false").lower() == "true"
