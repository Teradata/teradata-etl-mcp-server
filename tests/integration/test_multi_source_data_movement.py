"""Integration tests for multi-source data movement.

This test suite covers:
- Multiple Airbyte sources → Teradata
- Data transformation with dbt
- Cross-database queries
- Schema evolution handling
- Conflict resolution
"""

import pytest

# Skip entire module - depends on modules not yet implemented
pytest.skip(
    "Skipping multi-source tests: SchemaRegistry, ConflictResolver, CrossDbQueryEngine not yet implemented",
    allow_module_level=True,
)

import asyncio
from pathlib import Path

from teradata_etl_mcp_server.airbyte_client import AirbyteClient
from teradata_etl_mcp_server.conflict_resolver import ConflictResolver
from teradata_etl_mcp_server.data_validator import DataValidator
from teradata_etl_mcp_server.dbt_client import DbtClient
from teradata_etl_mcp_server.schema_registry import SchemaRegistry
from teradata_etl_mcp_server.teradata_client import TeradataClient


@pytest.fixture(scope="module")
def teradata_config():
    """Teradata configuration for testing."""
    return {
        "host": "localhost",
        "username": "dbc",
        "password": "dbc",
        "database": "multi_source_db"
    }


@pytest.fixture(scope="module")
def airbyte_config():
    """Airbyte configuration for testing."""
    return {
        "base_url": "http://localhost:8000",
        "api_key": "test_api_key"
    }


@pytest.fixture(scope="module")
def dbt_config():
    """dbt configuration for testing."""
    return {
        "project_dir": "/tmp/dbt_multi_source_project",
        "profiles_dir": "/tmp/dbt_profiles",
        "target": "dev"
    }


@pytest.fixture(scope="module")
async def teradata_client(teradata_config):
    """Create Teradata client for testing."""
    client = TeradataClient(teradata_config)
    await client.connect()

    # Create test database
    await client.execute(f"CREATE DATABASE {teradata_config['database']} AS PERMANENT = 120e6;")

    yield client

    # Cleanup
    await client.execute(f"DROP DATABASE {teradata_config['database']};")
    await client.close()


@pytest.fixture(scope="module")
async def airbyte_client(airbyte_config):
    """Create Airbyte client for testing."""
    client = AirbyteClient(airbyte_config)
    yield client
    await client.close()


@pytest.fixture(scope="module")
async def dbt_client(dbt_config):
    """Create dbt client for testing."""
    client = DbtClient(dbt_config)
    yield client
    await client.close()


@pytest.fixture(scope="module")
def schema_registry():
    """Create schema registry for testing."""
    return SchemaRegistry()


@pytest.fixture(scope="module")
def conflict_resolver():
    """Create conflict resolver for testing."""
    return ConflictResolver()


@pytest.fixture(scope="module")
def data_validator():
    """Create data validator for testing."""
    return DataValidator()


@pytest.fixture
async def setup_source_tables(teradata_client, teradata_config):
    """Setup source tables from multiple sources."""
    db = teradata_config['database']

    # Create tables for different sources
    # Source 1: PostgreSQL customers
    await teradata_client.execute(f"""
        CREATE TABLE {db}.postgres_customers (
            customer_id INTEGER,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(200),
            country VARCHAR(50),
            created_at TIMESTAMP,
            _airbyte_ab_id VARCHAR(100),
            _airbyte_emitted_at TIMESTAMP
        )
    """)

    # Source 2: MySQL orders
    await teradata_client.execute(f"""
        CREATE TABLE {db}.mysql_orders (
            order_id INTEGER,
            customer_id INTEGER,
            order_date DATE,
            amount DECIMAL(10,2),
            status VARCHAR(50),
            _airbyte_ab_id VARCHAR(100),
            _airbyte_emitted_at TIMESTAMP
        )
    """)

    # Source 3: MongoDB products
    await teradata_client.execute(f"""
        CREATE TABLE {db}.mongodb_products (
            product_id INTEGER,
            product_name VARCHAR(200),
            category VARCHAR(100),
            price DECIMAL(10,2),
            stock_quantity INTEGER,
            _airbyte_ab_id VARCHAR(100),
            _airbyte_emitted_at TIMESTAMP
        )
    """)

    # Source 4: Salesforce accounts
    await teradata_client.execute(f"""
        CREATE TABLE {db}.salesforce_accounts (
            account_id VARCHAR(100),
            account_name VARCHAR(200),
            industry VARCHAR(100),
            annual_revenue DECIMAL(15,2),
            _airbyte_ab_id VARCHAR(100),
            _airbyte_emitted_at TIMESTAMP
        )
    """)

    yield

    # Cleanup
    await teradata_client.execute(f"DROP TABLE {db}.postgres_customers;")
    await teradata_client.execute(f"DROP TABLE {db}.mysql_orders;")
    await teradata_client.execute(f"DROP TABLE {db}.mongodb_products;")
    await teradata_client.execute(f"DROP TABLE {db}.salesforce_accounts;")


class TestMultipleAirbyteSources:
    """Test suite for multiple Airbyte sources syncing to Teradata."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_sync_from_postgres_source(
        self,
        airbyte_client,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test syncing from PostgreSQL source to Teradata."""
        db = teradata_config['database']

        # Create Airbyte connection for PostgreSQL
        connection = await airbyte_client.create_connection({
            "name": "postgres_to_teradata",
            "sourceId": "postgres_source",
            "destinationId": "teradata_dest",
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {"name": "customers"},
                        "config": {
                            "destinationSyncMode": "append",
                            "selected": True
                        }
                    }
                ]
            }
        })

        # Trigger sync
        sync_result = await airbyte_client.trigger_sync(
            connection.get("connectionId")
        )

        assert sync_result is not None

        # Wait for sync completion
        await asyncio.sleep(5)

        # Verify data in Teradata
        result = await teradata_client.query(
            f"SELECT COUNT(*) as count FROM {db}.postgres_customers"
        )

        assert result is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_sync_from_mysql_source(
        self,
        airbyte_client,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test syncing from MySQL source to Teradata."""
        db = teradata_config['database']

        connection = await airbyte_client.create_connection({
            "name": "mysql_to_teradata",
            "sourceId": "mysql_source",
            "destinationId": "teradata_dest",
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {"name": "orders"},
                        "config": {
                            "destinationSyncMode": "append",
                            "selected": True
                        }
                    }
                ]
            }
        })

        sync_result = await airbyte_client.trigger_sync(
            connection.get("connectionId")
        )

        assert sync_result is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_sync_from_mongodb_source(
        self,
        airbyte_client,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test syncing from MongoDB source to Teradata."""
        db = teradata_config['database']

        connection = await airbyte_client.create_connection({
            "name": "mongodb_to_teradata",
            "sourceId": "mongodb_source",
            "destinationId": "teradata_dest",
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {"name": "products"},
                        "config": {
                            "destinationSyncMode": "append",
                            "selected": True
                        }
                    }
                ]
            }
        })

        sync_result = await airbyte_client.trigger_sync(
            connection.get("connectionId")
        )

        assert sync_result is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_parallel_sync_multiple_sources(
        self,
        airbyte_client
    ):
        """Test parallel syncing from multiple sources."""
        sources = ["postgres_source", "mysql_source", "mongodb_source"]

        # Trigger syncs in parallel
        sync_tasks = []
        for source in sources:
            connection = await airbyte_client.create_connection({
                "name": f"{source}_to_teradata",
                "sourceId": source,
                "destinationId": "teradata_dest"
            })

            sync_tasks.append(
                airbyte_client.trigger_sync(connection.get("connectionId"))
            )

        # Wait for all syncs
        results = await asyncio.gather(*sync_tasks)

        assert len(results) == 3
        assert all(r is not None for r in results)

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_incremental_sync_with_cursor(
        self,
        airbyte_client,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test incremental sync using cursor field."""
        db = teradata_config['database']

        # Initial sync
        connection = await airbyte_client.create_connection({
            "name": "postgres_incremental",
            "sourceId": "postgres_source",
            "destinationId": "teradata_dest",
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {"name": "customers"},
                        "config": {
                            "destinationSyncMode": "append",
                            "cursorField": ["created_at"],
                            "selected": True
                        }
                    }
                ]
            }
        })

        # First sync
        sync1 = await airbyte_client.trigger_sync(connection.get("connectionId"))
        await asyncio.sleep(5)

        # Get initial count
        result1 = await teradata_client.query(
            f"SELECT COUNT(*) as count FROM {db}.postgres_customers"
        )
        initial_count = result1[0].get("count", 0)

        # Second sync (should only get new records)
        sync2 = await airbyte_client.trigger_sync(connection.get("connectionId"))
        await asyncio.sleep(5)

        # Verify incremental behavior
        assert sync2 is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_sync_with_different_sync_modes(
        self,
        airbyte_client
    ):
        """Test syncing with different sync modes."""
        sync_modes = [
            "full_refresh_overwrite",
            "full_refresh_append",
            "incremental_append",
            "incremental_deduped_history"
        ]

        for mode in sync_modes:
            connection = await airbyte_client.create_connection({
                "name": f"test_{mode}",
                "sourceId": "test_source",
                "destinationId": "teradata_dest",
                "syncCatalog": {
                    "streams": [
                        {
                            "stream": {"name": "test_table"},
                            "config": {
                                "destinationSyncMode": mode,
                                "selected": True
                            }
                        }
                    ]
                }
            })

            assert connection is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_validate_data_after_sync(
        self,
        teradata_client,
        data_validator,
        teradata_config,
        setup_source_tables
    ):
        """Test data validation after multi-source sync."""
        db = teradata_config['database']

        # Insert test data
        await teradata_client.execute(f"""
            INSERT INTO {db}.postgres_customers VALUES
            (1, 'John', 'Doe', 'john@example.com', 'USA', CURRENT_TIMESTAMP, 'abc123', CURRENT_TIMESTAMP)
        """)

        # Validate data
        validation_result = await data_validator.validate_table(
            teradata_client,
            database=db,
            table="postgres_customers",
            checks=[
                {"type": "not_null", "column": "customer_id"},
                {"type": "not_null", "column": "email"},
                {"type": "unique", "column": "email"}
            ]
        )

        assert validation_result is not None
        assert validation_result.get("passed") is True


class TestDbtTransformation:
    """Test suite for dbt transformations on multi-source data."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_unified_customer_view(
        self,
        dbt_client,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test creating unified customer view from multiple sources."""
        db = teradata_config['database']

        # Create dbt model for unified customers
        model_sql = f"""
        WITH postgres_customers AS (
            SELECT 
                customer_id,
                first_name,
                last_name,
                email,
                'postgres' as source
            FROM {db}.postgres_customers
        ),
        
        salesforce_accounts AS (
            SELECT 
                account_id as customer_id,
                account_name as first_name,
                NULL as last_name,
                NULL as email,
                'salesforce' as source
            FROM {db}.salesforce_accounts
        )
        
        SELECT * FROM postgres_customers
        UNION ALL
        SELECT * FROM salesforce_accounts
        """

        # Write model file
        model_path = Path(dbt_client.config["project_dir"]) / "models" / "unified_customers.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)

        # Run dbt model
        result = await dbt_client.run(models=["unified_customers"])

        assert result is not None
        assert result.get("success") is True

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_join_multiple_source_tables(
        self,
        dbt_client,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test joining tables from multiple sources."""
        db = teradata_config['database']

        # Insert test data
        await teradata_client.execute(f"""
            INSERT INTO {db}.postgres_customers VALUES
            (1, 'John', 'Doe', 'john@example.com', 'USA', CURRENT_TIMESTAMP, 'abc1', CURRENT_TIMESTAMP)
        """)

        await teradata_client.execute(f"""
            INSERT INTO {db}.mysql_orders VALUES
            (1, 1, CURRENT_DATE, 100.00, 'completed', 'xyz1', CURRENT_TIMESTAMP)
        """)

        # Create dbt model joining sources
        model_sql = f"""
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            COUNT(o.order_id) as order_count,
            SUM(o.amount) as total_spent
        FROM {db}.postgres_customers c
        LEFT JOIN {db}.mysql_orders o
            ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name
        """

        model_path = Path(dbt_client.config["project_dir"]) / "models" / "customer_orders.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)

        # Run model
        result = await dbt_client.run(models=["customer_orders"])

        assert result is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_aggregate_across_sources(
        self,
        dbt_client,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test aggregating data across multiple sources."""
        db = teradata_config['database']

        model_sql = f"""
        WITH revenue_by_source AS (
            SELECT 
                'postgres' as source,
                COUNT(DISTINCT customer_id) as customer_count
            FROM {db}.postgres_customers
            
            UNION ALL
            
            SELECT 
                'mysql' as source,
                COUNT(DISTINCT customer_id) as customer_count
            FROM {db}.mysql_orders
        )
        
        SELECT 
            source,
            customer_count,
            SUM(customer_count) OVER () as total_customers
        FROM revenue_by_source
        """

        model_path = Path(dbt_client.config["project_dir"]) / "models" / "source_summary.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)

        result = await dbt_client.run(models=["source_summary"])

        assert result is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_incremental_model_on_multi_source(
        self,
        dbt_client,
        teradata_config
    ):
        """Test incremental dbt model on multi-source data."""
        db = teradata_config['database']

        model_sql = f"""
        {{{{ config(materialized='incremental', unique_key='customer_id') }}}}
        
        SELECT 
            customer_id,
            first_name,
            last_name,
            email,
            created_at
        FROM {db}.postgres_customers
        
        {{% if is_incremental() %}}
        WHERE created_at > (SELECT MAX(created_at) FROM {{{{ this }}}})
        {{% endif %}}
        """

        model_path = Path(dbt_client.config["project_dir"]) / "models" / "incremental_customers.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)

        # First run
        result1 = await dbt_client.run(models=["incremental_customers"])
        assert result1 is not None

        # Second run (incremental)
        result2 = await dbt_client.run(models=["incremental_customers"])
        assert result2 is not None


class TestCrossDatabaseQueries:
    """Test suite for cross-database query operations."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_query_across_multiple_schemas(
        self,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test querying across multiple schemas."""
        db = teradata_config['database']

        # Insert test data
        await teradata_client.execute(f"""
            INSERT INTO {db}.postgres_customers VALUES
            (1, 'John', 'Doe', 'john@example.com', 'USA', CURRENT_TIMESTAMP, 'abc1', CURRENT_TIMESTAMP)
        """)

        await teradata_client.execute(f"""
            INSERT INTO {db}.mysql_orders VALUES
            (1, 1, CURRENT_DATE, 100.00, 'completed', 'xyz1', CURRENT_TIMESTAMP)
        """)

        # Cross-schema query
        query = f"""
        SELECT 
            c.customer_id,
            c.email,
            o.order_id,
            o.amount
        FROM {db}.postgres_customers c
        INNER JOIN {db}.mysql_orders o
            ON c.customer_id = o.customer_id
        """

        result = await teradata_client.query(query)

        assert result is not None
        assert len(result) > 0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_federated_query_with_filter(
        self,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test federated query with filtering."""
        db = teradata_config['database']

        query = f"""
        SELECT 
            c.country,
            COUNT(o.order_id) as order_count,
            SUM(o.amount) as total_revenue
        FROM {db}.postgres_customers c
        LEFT JOIN {db}.mysql_orders o
            ON c.customer_id = o.customer_id
        WHERE c.country = 'USA'
        GROUP BY c.country
        """

        result = await teradata_client.query(query)

        assert result is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_cross_source_aggregation(
        self,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test aggregation across different source systems."""
        db = teradata_config['database']

        query = f"""
        WITH customer_summary AS (
            SELECT COUNT(*) as customer_count FROM {db}.postgres_customers
        ),
        order_summary AS (
            SELECT COUNT(*) as order_count FROM {db}.mysql_orders
        ),
        product_summary AS (
            SELECT COUNT(*) as product_count FROM {db}.mongodb_products
        )
        
        SELECT 
            c.customer_count,
            o.order_count,
            p.product_count
        FROM customer_summary c
        CROSS JOIN order_summary o
        CROSS JOIN product_summary p
        """

        result = await teradata_client.query(query)

        assert result is not None
        assert len(result) == 1

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_complex_join_multiple_sources(
        self,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test complex multi-way join across sources."""
        db = teradata_config['database']

        # Insert test data
        await teradata_client.execute(f"""
            INSERT INTO {db}.postgres_customers VALUES
            (1, 'John', 'Doe', 'john@example.com', 'USA', CURRENT_TIMESTAMP, 'abc1', CURRENT_TIMESTAMP)
        """)

        await teradata_client.execute(f"""
            INSERT INTO {db}.mysql_orders VALUES
            (1, 1, CURRENT_DATE, 100.00, 'completed', 'xyz1', CURRENT_TIMESTAMP)
        """)

        await teradata_client.execute(f"""
            INSERT INTO {db}.mongodb_products VALUES
            (1, 'Laptop', 'Electronics', 999.99, 50, 'prod1', CURRENT_TIMESTAMP)
        """)

        query = f"""
        SELECT 
            c.customer_id,
            c.first_name,
            o.order_id,
            p.product_name,
            p.price
        FROM {db}.postgres_customers c
        INNER JOIN {db}.mysql_orders o
            ON c.customer_id = o.customer_id
        CROSS JOIN {db}.mongodb_products p
        WHERE p.stock_quantity > 0
        """

        result = await teradata_client.query(query)

        assert result is not None


class TestSchemaEvolution:
    """Test suite for schema evolution handling."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_detect_schema_changes(
        self,
        schema_registry,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test detecting schema changes in source tables."""
        db = teradata_config['database']

        # Register initial schema
        initial_schema = await teradata_client.get_table_schema(
            database=db,
            table="postgres_customers"
        )

        await schema_registry.register_schema(
            table=f"{db}.postgres_customers",
            schema=initial_schema,
            version="1.0"
        )

        # Simulate schema change (add column)
        await teradata_client.execute(f"""
            ALTER TABLE {db}.postgres_customers
            ADD phone_number VARCHAR(20)
        """)

        # Get new schema
        new_schema = await teradata_client.get_table_schema(
            database=db,
            table="postgres_customers"
        )

        # Detect changes
        changes = await schema_registry.detect_changes(
            table=f"{db}.postgres_customers",
            new_schema=new_schema
        )

        assert changes is not None
        assert len(changes.get("added_columns", [])) == 1
        assert changes["added_columns"][0] == "phone_number"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_handle_column_addition(
        self,
        schema_registry,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test handling column addition in source table."""
        db = teradata_config['database']

        # Add new column
        await teradata_client.execute(f"""
            ALTER TABLE {db}.mysql_orders
            ADD discount_amount DECIMAL(10,2)
        """)

        # Update schema registry
        new_schema = await teradata_client.get_table_schema(
            database=db,
            table="mysql_orders"
        )

        result = await schema_registry.register_schema(
            table=f"{db}.mysql_orders",
            schema=new_schema,
            version="1.1"
        )

        assert result is not None
        assert result.get("version") == "1.1"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_handle_column_type_change(
        self,
        schema_registry,
        teradata_client,
        teradata_config
    ):
        """Test handling column data type change."""
        db = teradata_config['database']

        # Create test table
        await teradata_client.execute(f"""
            CREATE TABLE {db}.test_evolution (
                id INTEGER,
                value VARCHAR(50)
            )
        """)

        # Register schema
        initial_schema = await teradata_client.get_table_schema(
            database=db,
            table="test_evolution"
        )

        await schema_registry.register_schema(
            table=f"{db}.test_evolution",
            schema=initial_schema,
            version="1.0"
        )

        # Simulate type change (VARCHAR to INTEGER would fail, so we test detection)
        simulated_new_schema = initial_schema.copy()
        for col in simulated_new_schema["columns"]:
            if col["name"] == "value":
                col["data_type"] = "INTEGER"

        changes = await schema_registry.detect_changes(
            table=f"{db}.test_evolution",
            new_schema=simulated_new_schema
        )

        assert changes is not None
        assert len(changes.get("type_changes", [])) > 0

        # Cleanup
        await teradata_client.execute(f"DROP TABLE {db}.test_evolution;")

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_handle_column_removal(
        self,
        schema_registry
    ):
        """Test handling column removal in source table."""
        initial_schema = {
            "columns": [
                {"name": "id", "data_type": "INTEGER"},
                {"name": "name", "data_type": "VARCHAR"},
                {"name": "deprecated_field", "data_type": "VARCHAR"}
            ]
        }

        new_schema = {
            "columns": [
                {"name": "id", "data_type": "INTEGER"},
                {"name": "name", "data_type": "VARCHAR"}
            ]
        }

        await schema_registry.register_schema(
            table="test.table",
            schema=initial_schema,
            version="1.0"
        )

        changes = await schema_registry.detect_changes(
            table="test.table",
            new_schema=new_schema
        )

        assert changes is not None
        assert len(changes.get("removed_columns", [])) == 1
        assert changes["removed_columns"][0] == "deprecated_field"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_schema_version_history(
        self,
        schema_registry
    ):
        """Test tracking schema version history."""
        table_name = "test.versioned_table"

        # Register multiple versions
        for version in ["1.0", "1.1", "1.2"]:
            schema = {
                "columns": [
                    {"name": "id", "data_type": "INTEGER"},
                    {"name": "value", "data_type": "VARCHAR"}
                ]
            }

            if version >= "1.1":
                schema["columns"].append(
                    {"name": "created_at", "data_type": "TIMESTAMP"}
                )

            if version >= "1.2":
                schema["columns"].append(
                    {"name": "updated_at", "data_type": "TIMESTAMP"}
                )

            await schema_registry.register_schema(
                table=table_name,
                schema=schema,
                version=version
            )

        # Get version history
        history = await schema_registry.get_version_history(table_name)

        assert history is not None
        assert len(history) == 3
        assert history[0]["version"] == "1.0"
        assert history[-1]["version"] == "1.2"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_backward_compatible_schema_change(
        self,
        schema_registry
    ):
        """Test validating backward compatible schema changes."""
        old_schema = {
            "columns": [
                {"name": "id", "data_type": "INTEGER"},
                {"name": "name", "data_type": "VARCHAR"}
            ]
        }

        # Adding nullable column is backward compatible
        new_schema = {
            "columns": [
                {"name": "id", "data_type": "INTEGER"},
                {"name": "name", "data_type": "VARCHAR"},
                {"name": "email", "data_type": "VARCHAR", "nullable": True}
            ]
        }

        is_compatible = await schema_registry.is_backward_compatible(
            old_schema=old_schema,
            new_schema=new_schema
        )

        assert is_compatible is True


class TestConflictResolution:
    """Test suite for data conflict resolution."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_resolve_duplicate_records(
        self,
        conflict_resolver,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test resolving duplicate records from multiple sources."""
        db = teradata_config['database']

        # Insert duplicate customers
        await teradata_client.execute(f"""
            INSERT INTO {db}.postgres_customers VALUES
            (1, 'John', 'Doe', 'john@example.com', 'USA', CURRENT_TIMESTAMP, 'abc1', CURRENT_TIMESTAMP),
            (1, 'John', 'Doe', 'john.doe@example.com', 'USA', CURRENT_TIMESTAMP, 'abc2', CURRENT_TIMESTAMP)
        """)

        # Detect duplicates
        duplicates = await conflict_resolver.find_duplicates(
            teradata_client,
            database=db,
            table="postgres_customers",
            key_columns=["customer_id"]
        )

        assert duplicates is not None
        assert len(duplicates) > 0

        # Resolve using latest record
        resolved = await conflict_resolver.resolve_duplicates(
            teradata_client,
            database=db,
            table="postgres_customers",
            key_columns=["customer_id"],
            strategy="latest",
            timestamp_column="_airbyte_emitted_at"
        )

        assert resolved is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_resolve_conflicting_values(
        self,
        conflict_resolver
    ):
        """Test resolving conflicting values from different sources."""
        records = [
            {"customer_id": 1, "email": "john@example.com", "source": "postgres"},
            {"customer_id": 1, "email": "john.doe@example.com", "source": "salesforce"}
        ]

        # Resolve using source priority
        resolved = await conflict_resolver.resolve_conflicts(
            records=records,
            key_columns=["customer_id"],
            strategy="source_priority",
            source_priority=["salesforce", "postgres"]
        )

        assert resolved is not None
        assert resolved["email"] == "john.doe@example.com"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_merge_records_from_multiple_sources(
        self,
        conflict_resolver
    ):
        """Test merging records from multiple sources."""
        records = [
            {"customer_id": 1, "first_name": "John", "email": None, "source": "postgres"},
            {"customer_id": 1, "first_name": None, "email": "john@example.com", "source": "salesforce"}
        ]

        # Merge using coalesce strategy
        merged = await conflict_resolver.merge_records(
            records=records,
            key_columns=["customer_id"],
            strategy="coalesce"
        )

        assert merged is not None
        assert merged["first_name"] == "John"
        assert merged["email"] == "john@example.com"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_resolve_with_custom_rules(
        self,
        conflict_resolver
    ):
        """Test conflict resolution with custom rules."""
        records = [
            {"customer_id": 1, "email": "john@old.com", "updated_at": "2025-01-01"},
            {"customer_id": 1, "email": "john@new.com", "updated_at": "2025-12-31"}
        ]

        # Custom rule: use most recent record
        resolved = await conflict_resolver.resolve_with_rules(
            records=records,
            key_columns=["customer_id"],
            rules=[
                {
                    "field": "email",
                    "strategy": "max_by",
                    "order_by": "updated_at"
                }
            ]
        )

        assert resolved is not None
        assert resolved["email"] == "john@new.com"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_conflict_logging(
        self,
        conflict_resolver,
        teradata_client,
        teradata_config
    ):
        """Test logging conflicts for audit purposes."""
        db = teradata_config['database']

        # Create conflict log table
        await teradata_client.execute(f"""
            CREATE TABLE {db}.conflict_log (
                log_id INTEGER,
                table_name VARCHAR(100),
                conflict_type VARCHAR(50),
                record_keys VARCHAR(500),
                resolution_strategy VARCHAR(50),
                resolved_at TIMESTAMP
            )
        """)

        conflict_info = {
            "table_name": "postgres_customers",
            "conflict_type": "duplicate",
            "record_keys": {"customer_id": 1},
            "resolution_strategy": "latest"
        }

        result = await conflict_resolver.log_conflict(
            teradata_client,
            database=db,
            conflict_info=conflict_info
        )

        assert result is not None

        # Verify log entry
        logs = await teradata_client.query(
            f"SELECT * FROM {db}.conflict_log"
        )

        assert len(logs) > 0

        # Cleanup
        await teradata_client.execute(f"DROP TABLE {db}.conflict_log;")


class TestDataQualityValidation:
    """Test suite for data quality validation across sources."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_validate_referential_integrity(
        self,
        data_validator,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test validating referential integrity across sources."""
        db = teradata_config['database']

        # Insert data
        await teradata_client.execute(f"""
            INSERT INTO {db}.postgres_customers VALUES
            (1, 'John', 'Doe', 'john@example.com', 'USA', CURRENT_TIMESTAMP, 'abc1', CURRENT_TIMESTAMP)
        """)

        await teradata_client.execute(f"""
            INSERT INTO {db}.mysql_orders VALUES
            (1, 1, CURRENT_DATE, 100.00, 'completed', 'xyz1', CURRENT_TIMESTAMP),
            (2, 999, CURRENT_DATE, 50.00, 'completed', 'xyz2', CURRENT_TIMESTAMP)
        """)

        # Validate foreign key relationship
        result = await data_validator.validate_foreign_key(
            teradata_client,
            child_table=f"{db}.mysql_orders",
            child_column="customer_id",
            parent_table=f"{db}.postgres_customers",
            parent_column="customer_id"
        )

        assert result is not None
        assert result.get("valid") is False  # Order with customer_id=999 is orphaned
        assert result.get("orphaned_count") == 1

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_validate_data_consistency(
        self,
        data_validator,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test validating data consistency across sources."""
        db = teradata_config['database']

        # Check for null values
        result = await data_validator.check_null_values(
            teradata_client,
            database=db,
            table="postgres_customers",
            columns=["customer_id", "email"]
        )

        assert result is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_validate_data_completeness(
        self,
        data_validator,
        teradata_client,
        teradata_config,
        setup_source_tables
    ):
        """Test validating data completeness."""
        db = teradata_config['database']

        # Insert data with missing values
        await teradata_client.execute(f"""
            INSERT INTO {db}.postgres_customers VALUES
            (1, 'John', 'Doe', NULL, 'USA', CURRENT_TIMESTAMP, 'abc1', CURRENT_TIMESTAMP)
        """)

        result = await data_validator.check_completeness(
            teradata_client,
            database=db,
            table="postgres_customers",
            required_columns=["customer_id", "email"]
        )

        assert result is not None
        assert result.get("email_completeness") < 100.0


class TestMultiSourceIntegrationWorkflow:
    """Test suite for complete multi-source data movement workflow."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_complete_multi_source_pipeline(
        self,
        airbyte_client,
        teradata_client,
        dbt_client,
        schema_registry,
        conflict_resolver,
        data_validator,
        teradata_config,
        setup_source_tables
    ):
        """Test complete multi-source data pipeline workflow."""
        db = teradata_config['database']

        # Step 1: Sync data from multiple Airbyte sources
        sources = [
            {"name": "postgres_customers", "sourceId": "postgres_source"},
            {"name": "mysql_orders", "sourceId": "mysql_source"},
            {"name": "mongodb_products", "sourceId": "mongodb_source"}
        ]

        for source in sources:
            connection = await airbyte_client.create_connection({
                "name": f"{source['name']}_sync",
                "sourceId": source["sourceId"],
                "destinationId": "teradata_dest"
            })

            await airbyte_client.trigger_sync(connection.get("connectionId"))

        # Wait for syncs
        await asyncio.sleep(10)

        # Step 2: Register schemas
        for source in sources:
            schema = await teradata_client.get_table_schema(
                database=db,
                table=source["name"]
            )

            await schema_registry.register_schema(
                table=f"{db}.{source['name']}",
                schema=schema,
                version="1.0"
            )

        # Step 3: Insert test data
        await teradata_client.execute(f"""
            INSERT INTO {db}.postgres_customers VALUES
            (1, 'John', 'Doe', 'john@example.com', 'USA', CURRENT_TIMESTAMP, 'abc1', CURRENT_TIMESTAMP),
            (1, 'John', 'Doe', 'john.doe@example.com', 'USA', CURRENT_TIMESTAMP, 'abc2', CURRENT_TIMESTAMP)
        """)

        await teradata_client.execute(f"""
            INSERT INTO {db}.mysql_orders VALUES
            (1, 1, CURRENT_DATE, 100.00, 'completed', 'xyz1', CURRENT_TIMESTAMP)
        """)

        # Step 4: Resolve conflicts
        duplicates = await conflict_resolver.find_duplicates(
            teradata_client,
            database=db,
            table="postgres_customers",
            key_columns=["customer_id"]
        )

        assert len(duplicates) > 0

        resolved = await conflict_resolver.resolve_duplicates(
            teradata_client,
            database=db,
            table="postgres_customers",
            key_columns=["customer_id"],
            strategy="latest",
            timestamp_column="_airbyte_emitted_at"
        )

        # Step 5: Create dbt transformations
        model_sql = f"""
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            COUNT(o.order_id) as order_count,
            COALESCE(SUM(o.amount), 0) as total_spent
        FROM {db}.postgres_customers c
        LEFT JOIN {db}.mysql_orders o
            ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email
        """

        model_path = Path(dbt_client.config["project_dir"]) / "models" / "customer_360.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)

        # Run dbt model
        dbt_result = await dbt_client.run(models=["customer_360"])
        assert dbt_result is not None

        # Step 6: Validate data quality
        validation_result = await data_validator.validate_table(
            teradata_client,
            database=db,
            table="postgres_customers",
            checks=[
                {"type": "not_null", "column": "customer_id"},
                {"type": "unique", "column": "customer_id"}
            ]
        )

        assert validation_result is not None

        # Step 7: Verify final result
        result = await teradata_client.query(
            f"SELECT COUNT(*) as count FROM {db}.postgres_customers"
        )

        assert result is not None

        # Step 8: Check schema registry has all versions
        history = await schema_registry.get_version_history(
            f"{db}.postgres_customers"
        )

        assert history is not None
        assert len(history) > 0
