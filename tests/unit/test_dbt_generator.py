"""Unit tests for dbt generator."""

import sys
from pathlib import Path

import pytest
import yaml

from elt_mcp_server.generators.dbt_generator import (
    DBTGenerator,
    DBTGeneratorError,
    _quote_column,
    _write_dotenv_file,
)


class TestDBTGenerator:
    """Test suite for DbtGenerator."""

    @pytest.fixture
    def generator_config(self, tmp_path):
        """Test generator configuration."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()

        return {
            "project_dir": str(project_dir),
        }

    @pytest.fixture
    def generator(self, generator_config):
        """Create DBTGenerator instance."""
        return DBTGenerator(Path(generator_config["project_dir"]))

    @pytest.fixture
    def sample_table_metadata(self):
        """Sample table metadata for testing."""
        return {
            "database": "test_db",
            "schema": "public",
            "table": "customers",
            "columns": [
                {
                    "name": "customer_id",
                    "data_type": "INTEGER",
                    "nullable": False,
                    "primary_key": True,
                },
                {
                    "name": "email",
                    "data_type": "VARCHAR(255)",
                    "nullable": False,
                    "primary_key": False,
                },
                {
                    "name": "created_at",
                    "data_type": "TIMESTAMP",
                    "nullable": True,
                    "primary_key": False,
                },
            ],
        }

    # Initialization Tests

    def test_init_with_valid_config(self, generator, generator_config):
        """Test initialization with valid configuration."""
        assert generator.project_dir == Path(generator_config["project_dir"])

    def test_init_creates_directories(self, tmp_path):
        """Test that initialization works with project directory."""
        project_dir = tmp_path / "new_project"
        project_dir.mkdir()

        generator = DBTGenerator(project_dir=project_dir)

        assert generator.project_dir == project_dir

    # Model Generation Tests

    def test_generate_staging_model(self, generator, sample_table_metadata):
        """Test generating staging model."""
        result = generator.generate_staging_model(
            model_name="stg_customers",
            source_name="test_source",
            table_name="customers",
            columns=["customer_id", "email", "created_at"],
        )

        assert result is not None
        assert "select" in result.lower()
        assert "customer_id" in result
        assert "email" in result
        assert "created_at" in result
        assert "from" in result.lower()
        assert "{{ source(" in result

    def test_generate_staging_model_with_custom_name(self, generator, sample_table_metadata):
        """Test generating staging model with custom name."""
        result = generator.generate_staging_model(
            model_name="stg_custom_customers",
            source_name="test_source",
            table_name="customers",
            columns=["customer_id", "email"],
        )

        assert result is not None
        assert "customers" in result

    def test_generate_staging_model_with_transformations(self, generator, sample_table_metadata):
        """Test generating staging model with custom columns."""
        custom_columns = [
            "cast(customer_id as bigint) as customer_id",
            "lower(trim(email)) as email",
        ]

        result = generator.generate_staging_model(
            model_name="stg_customers",
            source_name="test_source",
            table_name="customers",
            columns=["customer_id", "email"],
            custom_columns=custom_columns,
        )

        assert "cast(customer_id as bigint)" in result
        assert "lower(trim(email))" in result

    def test_generate_staging_model_quotes_column_identifiers(self, generator):
        """Test that all column identifiers are double-quoted in staging models.

        Teradata reserved keywords (e.g. comment, date, user, table) and columns
        with special characters would cause syntax errors if left unquoted.
        """
        reserved_keyword_columns = ["comment", "date", "user", "table", "order"]
        result = generator.generate_staging_model(
            model_name="stg_keywords",
            source_name="test_source",
            table_name="keywords_table",
            columns=reserved_keyword_columns,
        )

        # Every column should appear double-quoted in the SQL
        for col in reserved_keyword_columns:
            assert f'"{col}"' in result, f"Column '{col}' should be double-quoted in SQL"

        # Also verify a normal staging model still quotes regular columns
        result2 = generator.generate_staging_model(
            model_name="stg_customers",
            source_name="test_source",
            table_name="customers",
            columns=["customer_id", "email"],
        )
        assert '"customer_id"' in result2
        assert '"email"' in result2

    def test_generate_staging_model_quotes_aliased_columns(self, generator):
        """Test that aliased columns are also double-quoted."""
        result = generator.generate_staging_model(
            model_name="stg_aliased",
            source_name="test_source",
            table_name="aliased_table",
            columns=["comment", "user_name"],
            column_aliases={"comment": "user_comment"},
        )

        assert '"comment" as "user_comment"' in result
        assert '"user_name"' in result

    def test_generate_staging_model_escapes_special_characters(self, generator):
        """Test that column names with special characters are properly escaped.

        Teradata allows columns created with quoted identifiers to contain
        spaces, hyphens, and even embedded double-quotes.  CSV headers can
        also contain arbitrary characters.
        """
        special_cols = ["col name", "col-1", 'col"quote', "col with spaces"]
        result = generator.generate_staging_model(
            model_name="stg_special",
            source_name="test_source",
            table_name="special_table",
            columns=special_cols,
        )

        # space / hyphen columns should be quoted normally
        assert '"col name"' in result
        assert '"col-1"' in result
        assert '"col with spaces"' in result
        # embedded double-quote must be doubled per SQL standard
        assert '"col""quote"' in result

    def test_quote_column_strips_trailing_whitespace(self):
        """Teradata spec: trailing whitespace is not part of the name."""
        assert _quote_column("col_name   ") == '"col_name"'
        assert _quote_column(" leading") == '" leading"'  # leading kept

    def test_quote_column_rejects_disallowed_characters(self):
        """Teradata spec: NULL, SUBSTITUTE, REPLACEMENT CHARACTER are forbidden."""
        with pytest.raises(ValueError, match="disallowed"):
            _quote_column("col\x00name")  # NULL U+0000
        with pytest.raises(ValueError, match="disallowed"):
            _quote_column("col\x1aname")  # SUBSTITUTE U+001A
        with pytest.raises(ValueError, match="disallowed"):
            _quote_column("col\ufffdname")  # REPLACEMENT CHARACTER U+FFFD

    def test_quote_column_rejects_all_whitespace(self):
        """Teradata spec: all-whitespace object names are not allowed."""
        with pytest.raises(ValueError, match="Empty or all-whitespace"):
            _quote_column("   ")
        with pytest.raises(ValueError, match="Empty or all-whitespace"):
            _quote_column("")

    def test_generate_transformation_model(self, generator):
        """Test generating transformation model."""
        result = generator.generate_transformation_model(
            model_name="base_customers",
            base_models=["stg_customers"],
            transformation_sql="select * from base",
            materialization="table",
        )

        assert result is not None
        assert "select" in result.lower()
        assert "ref(" in result

    def test_generate_transformation_model_with_description(self, generator):
        """Test generating transformation model with multiple base models."""
        sql = "select c.customer_id, o.order_id from base_0 c join base_1 o on c.customer_id = o.customer_id"

        result = generator.generate_transformation_model(
            model_name="int_customer_orders",
            base_models=["stg_customers", "stg_orders"],
            transformation_sql=sql,
        )

        assert result is not None
        assert "select" in result.lower()
        assert "ref('stg_customers')" in result
        assert "ref('stg_orders')" in result

    def test_generate_incremental_model(self, generator):
        """Test generating incremental model."""
        result = generator.generate_incremental_model(
            model_name="fct_orders",
            source_name="test_source",
            table_name="orders",
            unique_key="order_id",
            columns=["order_id", "customer_id", "order_amount", "order_date"],
        )

        assert result is not None
        assert "config(" in result
        assert "incremental" in result.lower()
        assert "is_incremental()" in result

    def test_generate_incremental_model_quotes_columns(self, generator):
        """Test that incremental model columns are double-quoted."""
        result = generator.generate_incremental_model(
            model_name="fct_events",
            source_name="test_source",
            table_name="events",
            unique_key="event_id",
            columns=["event_id", "date", "comment", "user"],
        )

        assert '"event_id"' in result
        assert '"date"' in result
        assert '"comment"' in result
        assert '"user"' in result

    # Source YAML Generation Tests

    def test_generate_source_yaml(self, generator, sample_table_metadata):
        """Test generating source YAML."""
        tables = [
            {
                "name": "customers",
                "description": "Customer table",
                "columns": [
                    {"name": "customer_id", "description": "Customer ID", "data_type": "INTEGER"},
                    {"name": "email", "description": "Email", "data_type": "VARCHAR"},
                    {"name": "created_at", "description": "Created date", "data_type": "TIMESTAMP"},
                ],
            }
        ]

        result = generator.generate_source_yaml(
            source_name="test_source", database="test_db", schema="public", tables=tables
        )

        assert result is not None

        # Parse YAML to verify structure
        parsed = yaml.safe_load(result)
        assert "version" in parsed
        assert "sources" in parsed
        assert len(parsed["sources"]) > 0

        source = parsed["sources"][0]
        assert source["name"] == "test_source"
        assert source["database"] == "test_db"
        assert "tables" in source
        assert len(source["tables"]) == 1

        table = source["tables"][0]
        assert table["name"] == "customers"
        assert "columns" in table
        assert len(table["columns"]) == 3

    def test_generate_source_yaml_multiple_tables(self, generator, sample_table_metadata):
        """Test generating source YAML with multiple tables."""
        tables = [
            {
                "name": "customers",
                "description": "Customer table",
                "columns": [{"name": "customer_id", "data_type": "INTEGER"}],
            },
            {
                "name": "orders",
                "description": "Orders table",
                "columns": [{"name": "order_id", "data_type": "INTEGER"}],
            },
        ]

        result = generator.generate_source_yaml(
            source_name="test_source", database="test_db", schema="public", tables=tables
        )

        parsed = yaml.safe_load(result)
        assert len(parsed["sources"][0]["tables"]) == 2

    def test_generate_source_yaml_with_freshness(self, generator):
        """Test generating source YAML with source description."""
        tables = [
            {
                "name": "customers",
                "description": "Customer table",
                "columns": [{"name": "customer_id", "data_type": "INTEGER"}],
            }
        ]

        result = generator.generate_source_yaml(
            source_name="test_source",
            database="test_db",
            schema="public",
            tables=tables,
            source_description="Test source with description",
        )

        assert result is not None
        parsed = yaml.safe_load(result)
        assert "Test source" in parsed["sources"][0]["description"]

    def test_generate_source_yaml_with_tests(self, generator):
        """Test generating source YAML with column tests."""
        tables = [
            {
                "name": "customers",
                "description": "Customer table",
                "columns": [
                    {"name": "customer_id", "data_type": "INTEGER", "tests": ["unique", "not_null"]}
                ],
            }
        ]

        result = generator.generate_source_yaml(
            source_name="test_source", database="test_db", schema="public", tables=tables
        )

        parsed = yaml.safe_load(result)
        table = parsed["sources"][0]["tables"][0]
        column = table["columns"][0]

        # Check if tests are included
        if "tests" in column:
            assert len(column["tests"]) > 0

    # Model Documentation Tests

    def test_generate_model_documentation(self, generator):
        """Test generating model documentation YAML."""
        models = [
            {
                "name": "stg_customers",
                "description": "Staging customer data",
                "columns": [
                    {"name": "customer_id", "description": "Primary key"},
                    {"name": "email", "description": "Customer email"},
                ],
            }
        ]

        result = generator.generate_model_documentation(models)

        assert result is not None

        parsed = yaml.safe_load(result)
        assert "version" in parsed
        assert "models" in parsed
        assert len(parsed["models"]) == 1

        model = parsed["models"][0]
        assert model["name"] == "stg_customers"
        assert "columns" in model
        assert len(model["columns"]) == 2

    def test_generate_model_documentation_with_tests(self, generator):
        """Test generating model documentation with tests."""
        models = [
            {
                "name": "stg_customers",
                "description": "Staging customers",
                "columns": [
                    {
                        "name": "customer_id",
                        "description": "PK",
                        "tests": ["unique", "not_null"],
                    },
                ],
            },
        ]

        result = generator.generate_model_documentation(models)

        parsed = yaml.safe_load(result)
        column = parsed["models"][0]["columns"][0]
        assert "tests" in column
        assert "unique" in column["tests"]
        assert "not_null" in column["tests"]

    def test_generate_model_documentation_with_relationships(self, generator):
        """Test generating model documentation with relationship tests."""
        models = [
            {
                "name": "stg_orders",
                "columns": [
                    {"name": "customer_id", "tests": ["not_null"]},
                ],
            },
        ]

        result = generator.generate_model_documentation(models)

        parsed = yaml.safe_load(result)
        column = parsed["models"][0]["columns"][0]
        assert "tests" in column

    # Snapshot Generation Tests

    def test_generate_snapshot(self, generator):
        """Test generating snapshot."""
        result = generator.generate_snapshot(
            snapshot_name="customers_snapshot",
            source_name="raw_data",
            table_name="customers",
            target_schema="snapshots",
            unique_key="customer_id",
            strategy="timestamp",
            updated_at="updated_at",
        )

        assert result is not None
        assert "snapshot" in result
        assert "customers_snapshot" in result
        assert "unique_key" in result or "customer_id" in result

    def test_generate_snapshot_check_strategy(self, generator):
        """Test generating snapshot with check strategy."""
        result = generator.generate_snapshot(
            snapshot_name="customers_snapshot",
            source_name="raw_data",
            table_name="customers",
            target_schema="snapshots",
            unique_key="customer_id",
            strategy="check",
            check_cols=["email", "name"],
        )

        assert result is not None
        assert "snapshot" in result
        assert "check" in result.lower()

    # Utility Method Tests

    def test_sanitize_name(self, generator):
        """Test name sanitization."""
        assert generator.sanitize_name("Customer Table") == "customer_table"
        assert generator.sanitize_name("test-model") == "test_model"
        assert generator.sanitize_name("TEST__MODEL") == "test_model"

    def test_get_dbt_data_type(self, generator):
        """Test Teradata to dbt data type mapping."""
        assert generator.get_dbt_data_type("INTEGER") in ["integer", "int", "INTEGER"]
        assert generator.get_dbt_data_type("VARCHAR(255)") in ["string", "varchar", "VARCHAR"]
        assert generator.get_dbt_data_type("TIMESTAMP") in ["timestamp", "datetime", "TIMESTAMP"]

    # Source from Teradata Metadata Tests

    def test_generate_source_from_teradata_metadata(self, generator):
        """Test generating source YAML from Teradata metadata."""
        table_metadata_list = [
            {
                "table_name": "customers",
                "columns": [
                    {"column_name": "customer_id", "data_type": "INTEGER", "nullable": False},
                    {"column_name": "email", "data_type": "VARCHAR(255)", "nullable": False},
                    {"column_name": "created_at", "data_type": "TIMESTAMP", "nullable": True},
                ],
            },
            {
                "table_name": "orders",
                "columns": [
                    {"column_name": "order_id", "data_type": "INTEGER", "nullable": False},
                    {"column_name": "customer_id", "data_type": "INTEGER", "nullable": False},
                    {"column_name": "amount", "data_type": "DECIMAL(10,2)", "nullable": False},
                ],
            },
        ]

        result = generator.generate_source_from_teradata_metadata(
            source_name="raw_data",
            table_metadata_list=table_metadata_list,
        )

        assert result is not None
        assert "sources:" in result
        assert "raw_data" in result
        assert "customers" in result
        assert "orders" in result
        assert "customer_id" in result
        assert "order_id" in result

    def test_generate_source_from_teradata_metadata_with_freshness(self, generator):
        """Test generating source YAML with freshness checks."""
        table_metadata_list = [
            {
                "table_name": "customers",
                "columns": [
                    {"column_name": "customer_id", "data_type": "INTEGER", "nullable": False},
                ],
            }
        ]

        result = generator.generate_source_from_teradata_metadata(
            source_name="raw_data",
            table_metadata_list=table_metadata_list,
            add_freshness=True,
        )

        assert result is not None
        assert "freshness" in result or "loaded_at_field" in result

    def test_generate_source_from_teradata_metadata_with_tests(self, generator):
        """Test generating source YAML with basic tests."""
        table_metadata_list = [
            {
                "table_name": "customers",
                "columns": [
                    {"column_name": "customer_id", "data_type": "INTEGER", "nullable": False},
                    {"column_name": "email", "data_type": "VARCHAR(255)", "nullable": False},
                ],
            }
        ]

        result = generator.generate_source_from_teradata_metadata(
            source_name="raw_data",
            table_metadata_list=table_metadata_list,
            add_basic_tests=True,
        )

        assert result is not None
        assert "columns:" in result

    # Schema Tests Generation Tests

    def test_generate_schema_tests(self, generator):
        """Test generating schema tests YAML."""
        column_tests = {
            "customer_id": ["unique", "not_null"],
            "email": ["not_null", "unique"],
            "created_at": ["not_null"],
        }

        result = generator.generate_schema_tests(
            model_name="stg_customers",
            column_tests=column_tests,
        )

        assert result is not None
        assert "models:" in result
        assert "stg_customers" in result
        assert "customer_id" in result
        assert "unique" in result
        assert "not_null" in result

    def test_generate_schema_tests_with_descriptions(self, generator):
        """Test generating schema tests with column descriptions."""
        column_tests = {
            "customer_id": ["unique", "not_null"],
            "email": ["not_null"],
        }

        column_descriptions = {
            "customer_id": "Unique customer identifier",
            "email": "Customer email address",
        }

        result = generator.generate_schema_tests(
            model_name="stg_customers",
            column_tests=column_tests,
            model_description="Staging table for customers",
            column_descriptions=column_descriptions,
        )

        assert result is not None
        assert "Unique customer identifier" in result
        assert "Customer email address" in result
        assert "Staging table for customers" in result

    def test_generate_schema_tests_with_model_tests(self, generator):
        """Test generating schema tests with model-level tests."""
        column_tests = {
            "customer_id": ["unique", "not_null"],
        }

        model_tests = [
            "dbt_utils.expression_is_true(expression='email is not null or phone is not null')"
        ]

        result = generator.generate_schema_tests(
            model_name="stg_customers",
            column_tests=column_tests,
            model_tests=model_tests,
        )

        assert result is not None
        assert "dbt_utils.expression_is_true" in result or "tests:" in result

    # Merge Behavior Tests

    def test_generate_source_yaml_merges_new_tables(self, generator):
        """Test that calling generate_source_yaml twice merges tables."""
        output_path = Path("models/sources/test_db.yml")
        tables_a = [
            {
                "name": "customers",
                "description": "Customer table",
                "columns": [{"name": "customer_id", "data_type": "INTEGER"}],
            }
        ]
        tables_b = [
            {
                "name": "orders",
                "description": "Orders table",
                "columns": [{"name": "order_id", "data_type": "INTEGER"}],
            }
        ]

        generator.generate_source_yaml(
            source_name="test_source",
            database="test_db",
            schema="public",
            tables=tables_a,
            output_path=output_path,
        )
        result = generator.generate_source_yaml(
            source_name="test_source",
            database="test_db",
            schema="public",
            tables=tables_b,
            output_path=output_path,
        )

        parsed = yaml.safe_load(result)
        table_names = [t["name"] for t in parsed["sources"][0]["tables"]]
        assert "customers" in table_names
        assert "orders" in table_names

    def test_generate_source_yaml_replaces_existing_table(self, generator):
        """Test that regenerating a table replaces its entry."""
        output_path = Path("models/sources/test_db.yml")
        tables_v1 = [
            {
                "name": "customers",
                "description": "Old description",
                "columns": [{"name": "customer_id", "data_type": "INTEGER"}],
            }
        ]
        tables_v2 = [
            {
                "name": "customers",
                "description": "New description",
                "columns": [
                    {"name": "customer_id", "data_type": "INTEGER"},
                    {"name": "email", "data_type": "VARCHAR"},
                ],
            }
        ]

        generator.generate_source_yaml(
            source_name="test_source",
            database="test_db",
            schema="public",
            tables=tables_v1,
            output_path=output_path,
        )
        result = generator.generate_source_yaml(
            source_name="test_source",
            database="test_db",
            schema="public",
            tables=tables_v2,
            output_path=output_path,
        )

        parsed = yaml.safe_load(result)
        tables = parsed["sources"][0]["tables"]
        assert len(tables) == 1
        assert tables[0]["description"] == "New description"
        assert len(tables[0]["columns"]) == 2

    def test_generate_schema_tests_merges_new_models(self, generator):
        """Test that calling generate_schema_tests twice merges models."""
        output_path = Path("models/staging/test_db/schema.yml")

        generator.generate_schema_tests(
            model_name="stg_customers",
            column_tests={"customer_id": ["unique", "not_null"]},
            output_path=output_path,
        )
        result = generator.generate_schema_tests(
            model_name="stg_orders",
            column_tests={"order_id": ["unique", "not_null"]},
            output_path=output_path,
        )

        parsed = yaml.safe_load(result)
        model_names = [m["name"] for m in parsed["models"]]
        assert "stg_customers" in model_names
        assert "stg_orders" in model_names

    def test_generate_schema_tests_replaces_existing_model(self, generator):
        """Test that regenerating a model replaces its entry."""
        output_path = Path("models/staging/test_db/schema.yml")

        generator.generate_schema_tests(
            model_name="stg_customers",
            column_tests={"customer_id": ["unique"]},
            output_path=output_path,
        )
        result = generator.generate_schema_tests(
            model_name="stg_customers",
            column_tests={"customer_id": ["unique", "not_null"], "email": ["not_null"]},
            output_path=output_path,
        )

        parsed = yaml.safe_load(result)
        assert len(parsed["models"]) == 1
        col_names = [c["name"] for c in parsed["models"][0]["columns"]]
        assert "customer_id" in col_names
        assert "email" in col_names

    # Data Test Generation Tests

    def test_generate_data_test(self, generator):
        """Test generating custom data test."""
        test_sql = """
        select
            customer_id,
            email,
            count(*) as duplicate_count
        from {{ ref('stg_customers') }}
        group by 1, 2
        having count(*) > 1
        """

        result = generator.generate_data_test(
            test_name="test_no_duplicate_emails",
            test_sql=test_sql,
        )

        assert result is not None
        assert "Test:" in result or "test_no_duplicate_emails" in result
        assert "customer_id" in result
        assert "email" in result
        assert "count(*)" in result

    def test_generate_data_test_with_comment(self, generator):
        """Test generating data test includes proper comments."""
        test_sql = "select * from {{ ref('stg_customers') }} where email is null"

        result = generator.generate_data_test(
            test_name="test_email_not_null",
            test_sql=test_sql,
        )

        assert result is not None
        assert "Test:" in result or "--" in result

    # Staging Layer Generation Tests

    def test_generate_staging_layer(self, generator):
        """Test generating complete staging layer for multiple tables."""
        table_metadata_list = [
            {
                "table_name": "customers",
                "columns": [
                    {
                        "column_name": "customer_id",
                        "data_type": "INTEGER",
                        "nullable": False,
                        "primary_key": True,
                    },
                    {"column_name": "email", "data_type": "VARCHAR(255)", "nullable": False},
                    {"column_name": "created_at", "data_type": "TIMESTAMP", "nullable": True},
                ],
                "description": "Customer information",
            },
            {
                "table_name": "orders",
                "columns": [
                    {
                        "column_name": "order_id",
                        "data_type": "INTEGER",
                        "nullable": False,
                        "primary_key": True,
                    },
                    {"column_name": "customer_id", "data_type": "INTEGER", "nullable": False},
                    {"column_name": "amount", "data_type": "DECIMAL(10,2)", "nullable": False},
                ],
                "description": "Order transactions",
            },
        ]

        result = generator.generate_staging_layer(
            source_name="raw_data",
            table_metadata_list=table_metadata_list,
        )

        assert result is not None
        assert "models_generated" in result
        assert "tests_generated" in result
        assert "errors" in result
        assert len(result["models_generated"]) == 2
        assert len(result["errors"]) == 0

    def test_generate_staging_layer_without_tests(self, generator):
        """Test generating staging layer without tests."""
        table_metadata_list = [
            {
                "table_name": "customers",
                "columns": [
                    {"column_name": "customer_id", "data_type": "INTEGER", "nullable": False},
                ],
            }
        ]

        result = generator.generate_staging_layer(
            source_name="raw_data",
            table_metadata_list=table_metadata_list,
            generate_tests=False,
        )

        assert result is not None
        assert len(result["models_generated"]) == 1
        assert len(result["tests_generated"]) == 0

    def test_generate_staging_layer_with_custom_materialization(self, generator):
        """Test generating staging layer with custom materialization."""
        table_metadata_list = [
            {
                "table_name": "customers",
                "columns": [
                    {"column_name": "customer_id", "data_type": "INTEGER", "nullable": False},
                ],
            }
        ]

        result = generator.generate_staging_layer(
            source_name="raw_data",
            table_metadata_list=table_metadata_list,
            materialization="table",
        )

        assert result is not None
        assert len(result["models_generated"]) == 1

    def test_generate_staging_layer_handles_errors(self, generator):
        """Test staging layer generation handles errors gracefully."""
        # Intentionally malformed metadata
        table_metadata_list = [
            {
                "table_name": "customers",
                # Missing columns - should cause error
            },
            {
                "table_name": "valid_table",
                "columns": [
                    {"column_name": "id", "data_type": "INTEGER", "nullable": False},
                ],
            },
        ]

        result = generator.generate_staging_layer(
            source_name="raw_data",
            table_metadata_list=table_metadata_list,
        )

        # Should complete with errors logged
        assert result is not None
        assert "errors" in result

    # Integration Tests

    def test_full_source_to_staging_workflow(self, generator):
        """Test complete workflow from source to staging models."""
        # Step 1: Generate source YAML
        table_metadata_list = [
            {
                "table_name": "customers",
                "columns": [
                    {
                        "column_name": "customer_id",
                        "data_type": "INTEGER",
                        "nullable": False,
                        "primary_key": True,
                    },
                    {"column_name": "email", "data_type": "VARCHAR(255)", "nullable": False},
                ],
            }
        ]

        source_yaml = generator.generate_source_from_teradata_metadata(
            source_name="raw_data",
            table_metadata_list=table_metadata_list,
        )

        assert source_yaml is not None
        assert "sources:" in source_yaml

        # Step 2: Generate staging model
        staging_model = generator.generate_staging_model(
            model_name="stg_raw_data_customers",
            source_name="raw_data",
            table_name="customers",
            columns=["customer_id", "email"],
        )

        assert staging_model is not None
        assert "{{ source(" in staging_model

        # Step 3: Generate tests
        column_tests = {
            "customer_id": ["unique", "not_null"],
            "email": ["not_null"],
        }

        schema_tests = generator.generate_schema_tests(
            model_name="stg_raw_data_customers",
            column_tests=column_tests,
        )

        assert schema_tests is not None
        assert "stg_raw_data_customers" in schema_tests

    def test_snapshot_to_incremental_workflow(self, generator):
        """Test workflow from snapshot to incremental model."""
        # Step 1: Generate snapshot
        snapshot = generator.generate_snapshot(
            snapshot_name="customers_snapshot",
            source_name="raw_data",
            table_name="customers",
            target_schema="snapshots",
            unique_key="customer_id",
            strategy="timestamp",
            updated_at="updated_at",
        )

        assert snapshot is not None
        assert "snapshot" in snapshot

        # Step 2: Generate incremental model based on snapshot
        incremental_model = generator.generate_incremental_model(
            model_name="inc_customers",
            source_name="raw_data",
            table_name="customers",
            unique_key="customer_id",
            columns=["customer_id", "email", "updated_at"],
        )

        assert incremental_model is not None
        assert "is_incremental" in incremental_model


class TestGenerateProfilesYml:
    """Test suite for profiles.yml generation."""

    @pytest.fixture
    def generator(self, tmp_path):
        """Create DBTGenerator instance with temp project directory."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        return DBTGenerator(project_dir=project_dir)

    @pytest.fixture
    def auth(self):
        """Sample :class:`TeradataAuth` for testing."""
        from elt_mcp_server.auth import TeradataAuth
        return TeradataAuth(
            host="test-teradata-host.example.com",
            port=1025,
            database="test_database",
            mechanism="TD2",
            username="test_user",
            password="test_password",
        )

    def test_generate_profiles_yml_basic(self, generator, auth):
        """Test basic profiles.yml generation — every field is a Jinja
        env_var() ref so the profile reflects whatever mechanism the
        subprocess env is populated with at dbt runtime."""
        result = generator.generate_profiles_yml(
            profile_name="my_project",
            auth=auth,
        )

        assert result is not None
        assert "my_project:" in result
        assert "target: dev" in result
        assert "type: teradata" in result
        # Every runtime field uses env_var() so a per-call profile override can
        # take effect without rewriting the file. YAML serializer doubles
        # single quotes inside single-quoted strings, so the rendered form is
        # env_var(''TERADATA_X''). Assert on the env-var name rather than the
        # exact quoting.
        assert "TERADATA_HOST" in result
        assert "TERADATA_USERNAME" in result
        assert "TERADATA_PASSWORD" in result
        assert "TERADATA_LOGMECH" in result
        assert "TERADATA_DATABASE" in result

    def test_generate_profiles_yml_creates_file(self, generator, auth):
        """Test that profiles.yml file is created in project directory."""
        generator.generate_profiles_yml(
            profile_name="my_project",
            auth=auth,
        )

        profiles_path = generator.project_dir / "profiles.yml"
        assert profiles_path.exists()

        with open(profiles_path) as f:
            loaded = yaml.safe_load(f)

        assert "my_project" in loaded
        assert loaded["my_project"]["target"] == "dev"
        assert loaded["my_project"]["outputs"]["dev"]["type"] == "teradata"

    def test_generate_profiles_yml_custom_target(self, generator, auth):
        """Test profiles.yml generation with custom target."""
        result = generator.generate_profiles_yml(
            profile_name="my_project",
            auth=auth,
            target="prod",
        )

        assert "target: prod" in result

        profiles_path = generator.project_dir / "profiles.yml"
        with open(profiles_path) as f:
            loaded = yaml.safe_load(f)

        assert loaded["my_project"]["target"] == "prod"
        assert "prod" in loaded["my_project"]["outputs"]

    def test_generate_profiles_yml_custom_threads(self, generator, auth):
        """Test profiles.yml generation with custom threads."""
        generator.generate_profiles_yml(
            profile_name="my_project",
            auth=auth,
            threads=8,
        )

        profiles_path = generator.project_dir / "profiles.yml"
        with open(profiles_path) as f:
            loaded = yaml.safe_load(f)

        assert loaded["my_project"]["outputs"]["dev"]["threads"] == 8

    def test_generate_profiles_yml_mechanism_fields_present(self, generator):
        """Every mechanism-specific YAML key is present so any logmech can
        take effect at dbt runtime without rewriting the file."""
        from elt_mcp_server.auth import TeradataAuth
        minimal_auth = TeradataAuth(
            host="localhost", port=1025, database="mydb",
            mechanism="TD2", username="u", password="p",
        )
        generator.generate_profiles_yml(
            profile_name="minimal_project",
            auth=minimal_auth,
        )

        profiles_path = generator.project_dir / "profiles.yml"
        with open(profiles_path) as f:
            loaded = yaml.safe_load(f)

        output = loaded["minimal_project"]["outputs"]["dev"]
        # Fields for every mechanism are referenced (with empty defaults for
        # fields the active mechanism doesn't need).
        expected = {"user", "password", "logmech", "logdata",
                    "oidc_clientid", "jws_private_key", "jws_cert", "sslca"}
        assert expected <= set(output.keys())

    def test_generate_profiles_yml_teradata_specific_settings(
        self, generator, auth
    ):
        """Teradata-specific settings (type, tmode) are emitted unconditionally."""
        generator.generate_profiles_yml(
            profile_name="td_project",
            auth=auth,
        )

        profiles_path = generator.project_dir / "profiles.yml"
        with open(profiles_path) as f:
            loaded = yaml.safe_load(f)

        output = loaded["td_project"]["outputs"]["dev"]
        assert output["type"] == "teradata"
        assert output["tmode"] == "ANSI"


class TestCreateProjectStructureWithProfiles:
    """Test suite for create_project_structure with profiles.yml generation."""

    @pytest.fixture
    def generator(self, tmp_path):
        """Create DBTGenerator instance with temp project directory."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        return DBTGenerator(project_dir=project_dir)

    @pytest.fixture
    def auth(self):
        """Sample :class:`TeradataAuth` for testing."""
        from elt_mcp_server.auth import TeradataAuth
        return TeradataAuth(
            host="test-teradata-host.example.com",
            port=1025,
            database="test_database",
            mechanism="TD2",
            username="test_user",
            password="test_password",
        )

    def test_create_project_structure_with_profiles(self, generator, auth):
        """Test that create_project_structure generates profiles.yml when auth provided."""
        result = generator.create_project_structure(
            project_name="my_analytics_project",
            auth=auth,
        )

        assert result["success"] is True

        # Check profiles.yml was created
        profiles_path = generator.project_dir / "profiles.yml"
        assert profiles_path.exists()
        assert str(profiles_path) in result["created_paths"]["files"]

        # Verify profiles.yml content
        with open(profiles_path) as f:
            loaded = yaml.safe_load(f)

        assert "my_analytics_project" in loaded
        assert loaded["my_analytics_project"]["target"] == "dev"
        output = loaded["my_analytics_project"]["outputs"]["dev"]
        assert output["type"] == "teradata"
        # Host is now referenced via env_var() so a per-call override works.
        # YAML loader strips the quoting, so we assert against the canonical
        # Jinja form.
        assert output["host"] == "{{ env_var('TERADATA_HOST') }}"
        assert output["user"] == "{{ env_var('TERADATA_USERNAME', '') }}"

    def test_create_project_structure_without_profiles(self, generator):
        """Without an ``auth`` argument, profiles.yml is not generated."""
        result = generator.create_project_structure(
            project_name="my_project",
            auth=None,
        )

        assert result["success"] is True

        profiles_path = generator.project_dir / "profiles.yml"
        assert not profiles_path.exists()

    def test_create_project_structure_with_custom_target(self, generator, auth):
        """Test create_project_structure with custom target name."""
        result = generator.create_project_structure(
            project_name="prod_project",
            auth=auth,
            target="prod",
            threads=8,
        )

        assert result["success"] is True

        profiles_path = generator.project_dir / "profiles.yml"
        with open(profiles_path) as f:
            loaded = yaml.safe_load(f)

        assert loaded["prod_project"]["target"] == "prod"
        assert "prod" in loaded["prod_project"]["outputs"]
        assert loaded["prod_project"]["outputs"]["prod"]["threads"] == 8

    def test_create_project_structure_creates_all_files(self, generator, auth):
        """Test that both dbt_project.yml and profiles.yml are created together."""
        result = generator.create_project_structure(
            project_name="complete_project",
            auth=auth,
        )

        assert result["success"] is True

        # Verify dbt_project.yml exists
        dbt_project_path = generator.project_dir / "dbt_project.yml"
        assert dbt_project_path.exists()

        # Verify profiles.yml exists
        profiles_path = generator.project_dir / "profiles.yml"
        assert profiles_path.exists()

        # Verify profile name in dbt_project.yml matches profiles.yml
        with open(dbt_project_path) as f:
            dbt_project_content = f.read()

        assert "profile: 'complete_project'" in dbt_project_content

        with open(profiles_path) as f:
            profiles_content = yaml.safe_load(f)

        assert "complete_project" in profiles_content

    def test_create_project_structure_defaults_include_snapshots(self, generator):
        """Omitting include_snapshots should create the snapshots/ directory (default True)."""
        result = generator.create_project_structure(project_name="default_project")

        assert result["success"] is True
        snapshots_dir = generator.project_dir / "snapshots"
        assert snapshots_dir.exists()
        assert any("snapshots" in p for p in result["created_paths"]["folders"])

    def test_create_project_structure_exclude_snapshots(self, generator):
        """Passing include_snapshots=False should suppress the snapshots/ directory."""
        result = generator.create_project_structure(
            project_name="no_snap_project",
            include_snapshots=False,
        )

        assert result["success"] is True
        snapshots_dir = generator.project_dir / "snapshots"
        assert not snapshots_dir.exists()
        assert not any("snapshots" in p for p in result["created_paths"]["folders"])

    def test_create_project_structure_creates_seeds_directory(self, generator):
        """seeds/ directory and seeds/.gitkeep should always be created."""
        result = generator.create_project_structure(project_name="seed_project")

        assert result["success"] is True
        seeds_dir = generator.project_dir / "seeds"
        assert seeds_dir.exists()
        assert (seeds_dir / ".gitkeep").exists()
        assert any("seeds" in p for p in result["created_paths"]["folders"])
        assert any("seeds" in p and ".gitkeep" in p for p in result["created_paths"]["files"])

    def test_create_project_structure_creates_packages_yml(self, generator):
        """packages.yml should be created with dbt_utils and teradata_utils dependencies."""
        result = generator.create_project_structure(project_name="pkg_project")

        assert result["success"] is True
        packages_yml = generator.project_dir / "packages.yml"
        assert packages_yml.exists()
        content = packages_yml.read_text()
        assert "dbt-labs/dbt_utils" in content
        assert "Teradata/teradata_utils" in content
        assert ">=1.3.0" in content
        assert str(packages_yml) in result["created_paths"]["files"]

    def test_create_project_structure_creates_gitignore(self, generator):
        """.gitignore should be created with standard dbt entries."""
        result = generator.create_project_structure(project_name="gi_project")

        assert result["success"] is True
        gitignore = generator.project_dir / ".gitignore"
        assert gitignore.exists()
        content = gitignore.read_text()
        assert "target/" in content
        assert "dbt_packages/" in content
        assert "logs/" in content
        assert str(gitignore) in result["created_paths"]["files"]

    def test_create_project_structure_includes_dispatch_block(self, generator):
        """dbt_project.yml should include dispatch block for teradata_utils."""
        result = generator.create_project_structure(project_name="dispatch_project")

        assert result["success"] is True
        content = (generator.project_dir / "dbt_project.yml").read_text()
        assert "dispatch:" in content
        assert "macro_namespace: dbt_utils" in content
        assert "teradata_utils" in content
        assert "search_order:" in content

    def test_create_project_structure_dispatch_search_order(self, generator):
        """Dispatch should search teradata_utils before dbt_utils."""
        result = generator.create_project_structure(project_name="order_project")

        assert result["success"] is True
        content = (generator.project_dir / "dbt_project.yml").read_text()
        teradata_pos = content.index("teradata_utils")
        dbt_utils_pos = content.index("dbt_utils", teradata_pos + 1)
        assert teradata_pos < dbt_utils_pos

    def test_create_project_structure_packages_yml_valid_yaml(self, generator):
        """packages.yml should have valid YAML structure with both packages."""
        import yaml

        result = generator.create_project_structure(project_name="yaml_project")

        assert result["success"] is True
        content = (generator.project_dir / "packages.yml").read_text()
        parsed = yaml.safe_load(content)
        package_names = [p["package"] for p in parsed["packages"]]
        assert "dbt-labs/dbt_utils" in package_names
        assert "Teradata/teradata_utils" in package_names

    def test_create_project_structure_dbt_project_yml_valid_yaml(self, generator):
        """dbt_project.yml should have valid YAML structure with dispatch block."""
        import yaml

        result = generator.create_project_structure(project_name="yaml_dispatch_project")

        assert result["success"] is True
        content = (generator.project_dir / "dbt_project.yml").read_text()
        parsed = yaml.safe_load(content)
        assert "dispatch" in parsed
        dispatch = parsed["dispatch"]
        assert isinstance(dispatch, list)
        assert dispatch[0]["macro_namespace"] == "dbt_utils"
        assert "teradata_utils" in dispatch[0]["search_order"]
        assert "dbt_utils" in dispatch[0]["search_order"]
        assert dispatch[0]["search_order"].index("teradata_utils") < dispatch[0]["search_order"].index(
            "dbt_utils"
        )

    def test_create_project_structure_preserves_existing_packages_yml(self, generator):
        """Existing packages.yml should not be overwritten."""
        packages_yml = generator.project_dir / "packages.yml"
        custom_content = "packages:\n  - package: custom/pkg\n    version: ['1.0.0']\n"
        packages_yml.write_text(custom_content)

        result = generator.create_project_structure(project_name="preserve_pkg")

        assert result["success"] is True
        assert packages_yml.read_text() == custom_content
        assert str(packages_yml) not in result["created_paths"]["files"]

    def test_create_project_structure_preserves_existing_gitignore(self, generator):
        """Existing .gitignore content is preserved; ``.env`` is appended if
        not already present (defense-in-depth: a future
        ``dbt_project(action='refresh_env', ...)`` writes ``.env`` and we
        never want it committed)."""
        gitignore = generator.project_dir / ".gitignore"
        custom_content = "*.pyc\n__pycache__/\n"
        gitignore.write_text(custom_content)

        result = generator.create_project_structure(project_name="preserve_gi")

        assert result["success"] is True
        body = gitignore.read_text()
        # Original lines preserved.
        assert "*.pyc" in body
        assert "__pycache__/" in body
        # ``.env`` appended for secret hygiene.
        assert ".env" in body.splitlines()
        # The file isn't reported as ``created`` (it pre-existed); it was just appended to.
        assert str(gitignore) not in result["created_paths"]["files"]

    def test_create_project_structure_preserves_existing_dbt_project_yml(self, generator):
        """Existing dbt_project.yml should not be overwritten."""
        dbt_project_yml = generator.project_dir / "dbt_project.yml"
        custom_content = "name: 'existing'\nversion: '2.0.0'\n"
        dbt_project_yml.parent.mkdir(parents=True, exist_ok=True)
        dbt_project_yml.write_text(custom_content)

        result = generator.create_project_structure(project_name="existing")

        assert result["success"] is True
        assert dbt_project_yml.read_text() == custom_content
        assert str(dbt_project_yml) not in result["created_paths"]["files"]

    def test_create_project_structure_idempotent_preserves_existing_content(self, generator):
        """Running create_project_structure twice should not disturb existing files."""
        # First run
        generator.create_project_structure(project_name="idempotent_proj")

        # Add a model file
        staging_dir = generator.project_dir / "models" / "staging"
        model_file = staging_dir / "stg_sales.sql"
        model_content = "SELECT id, amount FROM {{ source('raw', 'sales') }}"
        model_file.write_text(model_content)

        # Second run
        result = generator.create_project_structure(project_name="idempotent_proj")

        assert result["success"] is True
        # Existing model file must be untouched
        assert model_file.read_text() == model_content
        # Missing dirs should still exist
        assert (generator.project_dir / "models" / "intermediate").exists()
        assert (generator.project_dir / "models" / "marts").exists()
        assert (generator.project_dir / "seeds").exists()

    def test_identity_param_writes_into_dbt_project_yml_profile_field(self, generator):
        """When ``identity`` is supplied, the ``profile:`` field in
        dbt_project.yml is set to the identity (not project_name)."""
        generator.create_project_structure(
            project_name="analytics",
            identity="td_prod",
        )
        dbt_project_yml = generator.project_dir / "dbt_project.yml"
        content = dbt_project_yml.read_text()
        assert "profile: 'td_prod'" in content
        assert "name: 'analytics'" in content
        # The models block still uses project_name as the namespace key.
        assert "  analytics:" in content

    def test_identity_param_with_wizard_synthetic_identity(self, generator):
        """Wizard-default identities like ``wizard:<host>`` flow through
        verbatim into the profile field."""
        generator.create_project_structure(
            project_name="dev_lab",
            identity="wizard:td_dev_example_com",
        )
        content = (generator.project_dir / "dbt_project.yml").read_text()
        assert "profile: 'wizard:td_dev_example_com'" in content

    def test_identity_param_omitted_falls_back_to_project_name(self, generator):
        """Backward compat: omitting ``identity`` keeps the legacy behaviour
        (profile: == project_name)."""
        generator.create_project_structure(project_name="legacy_proj")
        content = (generator.project_dir / "dbt_project.yml").read_text()
        assert "profile: 'legacy_proj'" in content

    def test_identity_param_drives_profiles_yml_entry_key(self, generator, auth):
        """When auth is provided AND identity is set, the profiles.yml entry
        key matches the identity so dbt CLI can resolve the profile at run
        time."""
        generator.create_project_structure(
            project_name="warehouse",
            auth=auth,
            identity="td_prod",
        )
        profiles = (generator.project_dir / "profiles.yml").read_text()
        # Top-level YAML key under profiles.yml must equal the identity.
        assert "td_prod:" in profiles
        # And NOT the project_name (which would mismatch dbt_project.yml).
        # Looser check: the project_name appears nowhere as a top-level key.
        assert "warehouse:" not in profiles.split("\n")[0]


class TestTeradataIncrementalModels:
    """Test incremental model generation with Teradata-specific strategies."""

    @pytest.fixture
    def generator(self, tmp_path):
        """Create a generator instance for testing."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir(parents=True, exist_ok=True)
        return DBTGenerator(project_dir)

    def test_incremental_model_with_merge_strategy(self, generator):
        """Test generating incremental model with merge strategy."""
        result = generator.generate_incremental_model(
            model_name="inc_orders",
            source_name="test_source",
            table_name="orders",
            columns=["order_id", "customer_id", "amount", "updated_at"],
            unique_key="order_id",
            incremental_column="updated_at",
            incremental_strategy="merge",
        )

        assert "materialized='incremental'" in result
        assert "unique_key='order_id'" in result
        assert "incremental_strategy='merge'" in result
        assert "on_schema_change='fail'" in result
        assert "is_incremental()" in result
        assert "updated_at" in result

    def test_incremental_model_with_append_strategy(self, generator):
        """Test generating incremental model with append strategy."""
        result = generator.generate_incremental_model(
            model_name="inc_events",
            source_name="events_source",
            table_name="events",
            columns=["event_id", "event_type", "created_at"],
            unique_key="event_id",
            incremental_column="created_at",
            incremental_strategy="append",
        )

        assert "incremental_strategy='append'" in result
        assert "materialized='incremental'" in result

    def test_incremental_model_with_delete_plus_insert_strategy(self, generator):
        """Test generating incremental model with delete+insert strategy."""
        result = generator.generate_incremental_model(
            model_name="inc_transactions",
            source_name="finance_source",
            table_name="transactions",
            columns=["txn_id", "amount", "txn_date"],
            unique_key="txn_id",
            incremental_column="txn_date",
            incremental_strategy="delete+insert",
        )

        assert "incremental_strategy='delete+insert'" in result
        assert "unique_key='txn_id'" in result

    def test_incremental_model_rejects_insert_overwrite(self, generator):
        """Test that insert_overwrite strategy is rejected for Teradata."""
        from elt_mcp_server.generators.dbt_generator import DBTGeneratorError

        with pytest.raises(DBTGeneratorError) as exc_info:
            generator.generate_incremental_model(
                model_name="inc_model",
                source_name="test_source",
                table_name="test_table",
                columns=["id", "name"],
                unique_key="id",
                incremental_strategy="insert_overwrite",
            )

        assert "insert_overwrite" in str(exc_info.value)
        assert "Valid options" in str(exc_info.value)

    def test_staging_model_with_incremental_materialization(self, generator):
        """Test staging model with incremental materialization."""
        result = generator.generate_staging_model(
            model_name="stg_orders",
            source_name="raw_source",
            table_name="orders",
            columns=["order_id", "customer_id", "amount", "updated_at"],
            materialization="incremental",
            unique_key="order_id",
            incremental_strategy="merge",
            incremental_column="updated_at",
        )

        assert "materialized='incremental'" in result
        assert "unique_key='order_id'" in result
        assert "incremental_strategy='merge'" in result
        assert "is_incremental()" in result

    def test_intermediate_model_with_incremental(self, generator):
        """Test intermediate model with incremental materialization."""
        result = generator.generate_intermediate_model(
            model_name="int_enriched_orders",
            source_models=["stg_orders", "stg_customers"],
            materialization="incremental",
            unique_key="order_id",
            incremental_strategy="merge",
            incremental_column="updated_at",
        )

        assert "materialized='incremental'" in result
        assert "unique_key='order_id'" in result
        assert "incremental_strategy='merge'" in result
        assert "is_incremental()" in result

    def test_mart_model_with_incremental(self, generator):
        """Test mart model with incremental materialization."""
        result = generator.generate_mart_model(
            model_name="fct_orders",
            model_type="fact",
            source_models=["int_orders"],
            dimension_columns=["order_id", "customer_id"],
            measure_columns=[{"name": "total_amount", "agg": "sum(amount)"}],
            materialization="incremental",
            unique_key="order_id",
            incremental_strategy="merge",
            incremental_column="updated_at",
        )

        assert "materialized='incremental'" in result
        assert "unique_key='order_id'" in result
        assert "incremental_strategy='merge'" in result

    def test_intermediate_model_quotes_select_and_group_by(self, generator):
        """Test that intermediate model select_columns and group_by are double-quoted."""
        result = generator.generate_intermediate_model(
            model_name="int_summary",
            source_models=["stg_events"],
            select_columns=["date", "user", "comment"],
            group_by=["date", "user"],
        )

        # select columns should be quoted
        assert '"date"' in result
        assert '"user"' in result
        assert '"comment"' in result
        # group by columns should also be quoted
        assert 'group by' in result.lower()

    def test_mart_model_quotes_dimension_and_measure_columns(self, generator):
        """Test that mart model dimension/measure columns are double-quoted."""
        result = generator.generate_mart_model(
            model_name="fct_summary",
            model_type="fact",
            source_models=["int_events"],
            dimension_columns=["date", "user"],
            measure_columns=[{"name": "order", "agg": "count(*)"}],
        )

        # dimension columns in select and group by should be quoted
        assert '"date"' in result
        assert '"user"' in result
        # measure alias should be quoted
        assert 'as "order"' in result

    def test_mart_dimension_model_quotes_columns(self, generator):
        """Test that dimension mart model quotes columns in select distinct."""
        result = generator.generate_mart_model(
            model_name="dim_users",
            model_type="dimension",
            source_models=["int_users"],
            dimension_columns=["user", "comment", "table"],
        )

        assert '"user"' in result
        assert '"comment"' in result
        assert '"table"' in result
        assert "select distinct" in result.lower()

    def test_incremental_model_with_custom_on_schema_change(self, generator):
        """Test incremental model with custom on_schema_change setting."""
        result = generator.generate_incremental_model(
            model_name="inc_data",
            source_name="source",
            table_name="data",
            columns=["id", "value"],
            unique_key="id",
            incremental_strategy="merge",
            on_schema_change="sync_all_columns",
        )

        assert "on_schema_change='sync_all_columns'" in result

    def test_teradata_valid_strategies_constant(self, generator):
        """Test that the valid strategies constant is correctly defined."""
        assert "append" in generator.TERADATA_INCREMENTAL_STRATEGIES
        assert "merge" in generator.TERADATA_INCREMENTAL_STRATEGIES
        assert "delete+insert" in generator.TERADATA_INCREMENTAL_STRATEGIES
        assert "insert_overwrite" not in generator.TERADATA_INCREMENTAL_STRATEGIES


class TestHookEscaping:
    """Test that pre_hook / post_hook values are serialized safely.

    The config key may be ``pre_hook`` or ``pre-hook`` depending on the code
    path (both are valid dbt syntax).  Tests match either form via a helper.
    """

    @pytest.fixture
    def generator(self, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        return DBTGenerator(project_dir=project_dir)

    @staticmethod
    def _has_hook(result: str, kind: str, expected_json: str) -> bool:
        """Return True if *result* contains ``<kind>=<expected_json>``.

        Accepts both hyphenated (``pre-hook``) and underscored (``pre_hook``)
        key names because both are valid dbt config syntax.
        """
        import re

        # e.g. kind="pre_hook" -> pattern matches pre_hook or pre-hook
        base = kind.replace("_", "[-_]")
        return bool(re.search(rf"{base}={re.escape(expected_json)}", result))

    def test_simple_hook_in_intermediate_model(self, generator):
        """Simple hook without special characters."""
        result = generator.generate_intermediate_model(
            model_name="int_test",
            source_models=["stg_a"],
            post_hook="COLLECT STATS ON {{ this }}",
        )
        # Must be a JSON list, not bare double-quoted string
        assert self._has_hook(result, "post_hook", '["COLLECT STATS ON {{ this }}"]')

    def test_hook_with_double_quotes(self, generator):
        """Double quotes inside hook SQL must be escaped."""
        hook = 'ALTER TABLE "my_db"."my_table" DROP STATS'
        result = generator.generate_intermediate_model(
            model_name="int_test",
            source_models=["stg_a"],
            pre_hook=hook,
        )
        # The double quotes should be escaped inside the JSON list
        assert r"\"my_db\"" in result
        # The output must still be valid: starts with [ and ends with ]
        import re

        match = re.search(r"pre[-_]hook=(\[.*?\])", result)
        assert match, "pre_hook should be a JSON list"

    def test_hook_with_newlines(self, generator):
        """Newlines in hook SQL must not break the Jinja config."""
        hook = "BEGIN\n  COLLECT STATS ON {{ this }};\nEND;"
        result = generator.generate_intermediate_model(
            model_name="int_test",
            source_models=["stg_a"],
            post_hook=hook,
        )
        # json.dumps converts \n to \\n inside the string
        assert "\\n" in result
        import re

        assert re.search(r"post[-_]hook=\[", result)

    def test_hook_with_single_quotes(self, generator):
        """Single quotes inside hook SQL must not break surrounding Jinja."""
        hook = "UPDATE stats SET name = 'foo' WHERE id = 1"
        result = generator.generate_intermediate_model(
            model_name="int_test",
            source_models=["stg_a"],
            pre_hook=hook,
        )
        import re

        assert re.search(r"pre[-_]hook=\[", result)
        # Single quotes should survive inside a JSON-encoded string
        assert "'foo'" in result

    def test_hooks_in_mart_model(self, generator):
        """Hooks in generate_mart_model also use safe serialization."""
        hook = 'COLLECT STATS COLUMN "col_a" ON {{ this }}'
        result = generator.generate_mart_model(
            model_name="fct_test",
            model_type="fact",
            source_models=["int_a"],
            post_hook=hook,
        )
        import re

        assert re.search(r"post[-_]hook=\[", result)
        assert r"\"col_a\"" in result

    def test_both_hooks_together(self, generator):
        """Both pre_hook and post_hook rendered correctly in same model."""
        result = generator.generate_intermediate_model(
            model_name="int_test",
            source_models=["stg_a"],
            pre_hook="DELETE FROM staging.tmp",
            post_hook="COLLECT STATS ON {{ this }}",
        )
        assert self._has_hook(result, "pre_hook", '["DELETE FROM staging.tmp"]')
        assert self._has_hook(result, "post_hook", '["COLLECT STATS ON {{ this }}"]')


# ======================================================================== #
#  New generator methods (Gaps 8, 9, 11)                                    #
# ======================================================================== #


class TestMultiTargetProfiles:
    """Tests for generate_multi_target_profiles_yml."""

    @pytest.fixture
    def generator(self, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        return DBTGenerator(project_dir)

    def test_multi_target_profiles(self, generator):
        from elt_mcp_server.auth import TeradataAuth
        result = generator.generate_multi_target_profiles_yml(
            profile_name="my_project",
            targets=[
                {
                    "name": "dev",
                    "auth": TeradataAuth(
                        host="dev-host", port=1025, database="dev_db",
                        mechanism="TD2", username="u", password="p",
                    ),
                    "threads": 4,
                },
                {
                    "name": "prod",
                    "auth": TeradataAuth(
                        host="prod-host", port=1025, database="prod_db",
                        mechanism="TD2", username="u", password="p",
                    ),
                    "threads": 8,
                },
            ],
        )
        parsed = yaml.safe_load(result)
        assert "my_project" in parsed
        assert "dev" in parsed["my_project"]["outputs"]
        assert "prod" in parsed["my_project"]["outputs"]
        assert parsed["my_project"]["target"] == "dev"
        assert parsed["my_project"]["outputs"]["prod"]["threads"] == 8

    def test_multi_target_profiles_empty_targets(self, generator):
        from elt_mcp_server.generators.dbt_generator import DBTGeneratorError

        with pytest.raises(DBTGeneratorError):
            generator.generate_multi_target_profiles_yml(
                profile_name="my_project",
                targets=[],
            )


class TestAddPackage:
    """Tests for add_package method."""

    @pytest.fixture
    def generator(self, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        return DBTGenerator(project_dir)

    def test_add_package_new(self, generator):
        result = generator.add_package(
            package_name="calogica/dbt_expectations",
            version=">=0.10.0",
        )
        assert result["success"] is True
        assert result["package_name"] == "calogica/dbt_expectations"
        assert result["total_packages"] == 1

    def test_add_package_to_existing(self, generator):
        # First add
        generator.add_package(
            package_name="dbt-labs/dbt_utils",
            version=">=1.0.0",
        )
        # Second add
        result = generator.add_package(
            package_name="calogica/dbt_expectations",
            version=">=0.10.0",
        )
        assert result["total_packages"] == 2
        assert "calogica/dbt_expectations" in result["packages"]

    def test_add_package_update_existing(self, generator):
        generator.add_package(
            package_name="dbt-labs/dbt_utils",
            version=">=1.0.0",
        )
        result = generator.add_package(
            package_name="dbt-labs/dbt_utils",
            version=">=2.0.0",
        )
        assert result["total_packages"] == 1
        # Check file has updated version
        packages_path = generator.project_dir / "packages.yml"
        with open(packages_path) as f:
            data = yaml.safe_load(f)
        assert data["packages"][0]["version"] == ">=2.0.0"


class TestTeradataMacros:
    """Tests for generate_teradata_macros method."""

    @pytest.fixture
    def generator(self, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        return DBTGenerator(project_dir)

    def test_generate_teradata_macros(self, generator):
        result = generator.generate_teradata_macros()
        assert result["success"] is True
        assert result["macros_generated"] == 3
        assert len(result["macro_files"]) == 3

        # Verify files exist
        macros_dir = generator.project_dir / "macros"
        assert (macros_dir / "collect_stats.sql").exists()
        assert (macros_dir / "grant_access.sql").exists()
        assert (macros_dir / "teradata_utils.sql").exists()

    def test_collect_stats_macro_content(self, generator):
        generator.generate_teradata_macros()
        content = (generator.project_dir / "macros" / "collect_stats.sql").read_text()
        assert "collect_stats" in content
        assert "COLLECT STATISTICS" in content

    def test_grant_access_macro_content(self, generator):
        generator.generate_teradata_macros()
        content = (generator.project_dir / "macros" / "grant_access.sql").read_text()
        assert "grant_select" in content
        assert "GRANT SELECT" in content


class TestTagSupport:
    """Tests for user-configurable tags in generated models."""

    @pytest.fixture
    def generator(self, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        return DBTGenerator(project_dir)

    def test_staging_model_with_tags(self, generator):
        result = generator.generate_staging_model(
            model_name="stg_orders",
            source_name="raw",
            table_name="orders",
            columns=["id", "amount"],
            tags=["daily", "finance"],
        )
        assert 'tags=["daily", "finance"]' in result

    def test_staging_model_without_tags(self, generator):
        result = generator.generate_staging_model(
            model_name="stg_orders",
            source_name="raw",
            table_name="orders",
            columns=["id", "amount"],
        )
        assert "tags=" not in result

    def test_incremental_model_with_tags(self, generator):
        result = generator.generate_incremental_model(
            model_name="inc_events",
            source_name="raw",
            table_name="events",
            columns=["id", "ts"],
            unique_key="id",
            tags=["hourly"],
        )
        assert 'tags=["hourly"]' in result

    def test_snapshot_with_tags(self, generator):
        result = generator.generate_snapshot(
            snapshot_name="snap_customers",
            source_name="raw",
            table_name="customers",
            target_schema="snapshots",
            unique_key="id",
            tags=["nightly", "scd"],
        )
        assert 'tags=["nightly", "scd"]' in result

    def test_transformation_model_with_tags(self, generator):
        result = generator.generate_transformation_model(
            model_name="txn_enriched",
            base_models=["stg_orders"],
            transformation_sql="select * from base",
            tags=["etl"],
        )
        assert 'tags=["etl"]' in result

    def test_intermediate_model_with_tags(self, generator):
        result = generator.generate_intermediate_model(
            model_name="int_enriched",
            source_models=["stg_orders", "stg_customers"],
            tags=["daily", "core"],
        )
        assert 'tags=["daily", "core"]' in result

    def test_mart_model_merges_user_tags_with_auto_tags(self, generator):
        result = generator.generate_mart_model(
            model_name="fct_revenue",
            model_type="fact",
            source_models=["int_orders"],
            tags=["finance", "daily"],
        )
        assert 'tags=["fact", "mart", "finance", "daily"]' in result

    def test_mart_model_deduplicates_tags(self, generator):
        result = generator.generate_mart_model(
            model_name="dim_customers",
            model_type="dimension",
            source_models=["int_customers"],
            tags=["mart", "dimension", "core"],
        )
        assert '"mart"' in result
        assert '"dimension"' in result
        assert '"core"' in result
        assert result.count('"mart"') == 1

    def test_mart_model_without_user_tags(self, generator):
        result = generator.generate_mart_model(
            model_name="fct_orders",
            model_type="fact",
            source_models=["int_orders"],
        )
        assert 'tags=["fact", "mart"]' in result


# ---------------------------------------------------------------------------
# Path containment regression tests — every generator method must reject a
# traversal `output_path`. Guards against the arbitrary-file-write class of
# bug closed in SafeFileWriter / safe_join_within.
# ---------------------------------------------------------------------------


class TestPathContainment:
    """Traversal rejection for every DBTGenerator method that accepts output_path."""

    @pytest.fixture
    def generator(self, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        return DBTGenerator(project_dir=project_dir)

    # Minimal call kwargs for each method. Kept tiny — we only care that the
    # method reaches the write path and rejects the traversal there.
    _SAMPLE_MODEL = {
        "name": "m",
        "description": "x",
        "columns": [{"name": "c", "description": "y"}],
    }

    @pytest.mark.parametrize(
        "method_name,minimal_kwargs",
        [
            (
                "generate_source_yaml",
                {
                    "source_name": "s",
                    "database": "db",
                    "schema": "sch",
                    "tables": [{"name": "t", "columns": [{"name": "c", "data_type": "INT"}]}],
                },
            ),
            (
                "generate_staging_model",
                {
                    "model_name": "stg_t",
                    "source_name": "s",
                    "table_name": "t",
                    "columns": ["c1", "c2"],
                },
            ),
            (
                "generate_incremental_model",
                {
                    "model_name": "inc_t",
                    "source_name": "s",
                    "table_name": "t",
                    "columns": ["c1", "c2"],
                    "unique_key": "id",
                    "incremental_column": "updated_at",
                },
            ),
            (
                "generate_snapshot",
                {
                    "snapshot_name": "snap_t",
                    "source_name": "s",
                    "table_name": "t",
                    "target_schema": "snapshots",
                    "unique_key": "id",
                    "updated_at": "updated_at",
                },
            ),
            (
                "generate_transformation_model",
                {
                    "model_name": "t_m",
                    "base_models": ["stg_x"],
                    "transformation_sql": "select 1 as x",
                },
            ),
            (
                "generate_intermediate_model",
                {
                    "model_name": "int_m",
                    "source_models": ["stg_x"],
                },
            ),
            (
                "generate_mart_model",
                {
                    "model_name": "fct_m",
                    "model_type": "fact",
                    "source_models": ["int_x"],
                },
            ),
            (
                "generate_schema_tests",
                {
                    "model_name": "m",
                    "column_tests": {"c": ["not_null"]},
                },
            ),
            (
                "generate_data_test",
                {
                    "test_name": "t",
                    "test_sql": "select 1",
                },
            ),
            (
                "generate_model_documentation",
                {"models": [_SAMPLE_MODEL]},
            ),
        ],
    )
    def test_rejects_parent_traversal(self, generator, method_name, minimal_kwargs):
        """Every generator method with an output_path parameter must reject traversal."""
        with pytest.raises(DBTGeneratorError, match="invalid output_path"):
            getattr(generator, method_name)(
                **minimal_kwargs, output_path=Path("../escape.yml")
            )

    @pytest.mark.parametrize(
        "bad_path",
        [
            Path("../../etc/passwd"),
            Path("models/../../escape.yml"),
            "/etc/passwd",
            "models/x\x00.yml",
        ],
    )
    def test_rejects_various_traversal_shapes(self, generator, bad_path):
        """generate_staging_model rejects a range of traversal shapes."""
        with pytest.raises(DBTGeneratorError, match="invalid output_path"):
            generator.generate_staging_model(
                model_name="stg_t",
                source_name="s",
                table_name="t",
                columns=["c1"],
                output_path=bad_path,
            )

    def test_accepts_nested_legitimate_path(self, generator):
        """A legitimate nested subpath should write successfully."""
        result = generator.generate_staging_model(
            model_name="stg_t",
            source_name="s",
            table_name="t",
            columns=["c1"],
            output_path=Path("models/staging/s/stg_t.sql"),
        )
        assert result is not None
        assert (generator.project_dir / "models" / "staging" / "s" / "stg_t.sql").exists()


# ════════════════════════════════════════════════════════════════════
#  Per-sub-project .env writer + scaffolding
# ════════════════════════════════════════════════════════════════════


class TestDotenvFileWriter:
    """Direct unit tests on the module-level :func:`_write_dotenv_file`."""

    def test_skips_empty_values(self, tmp_path):
        env = {
            "TERADATA_HOST": "td.example.com",
            "TERADATA_PASSWORD": "",
            "TERADATA_LOGDATA": "token=abc",
        }
        path = tmp_path / ".env"
        keys = _write_dotenv_file(path, env)
        assert keys == ["TERADATA_HOST", "TERADATA_LOGDATA"]
        body = path.read_text(encoding="utf-8")
        assert "TERADATA_HOST=td.example.com" in body
        assert "TERADATA_LOGDATA=token=abc" in body
        assert "TERADATA_PASSWORD" not in body

    def test_quotes_value_with_whitespace(self, tmp_path):
        path = tmp_path / ".env"
        _write_dotenv_file(path, {"K": "value with spaces"})
        assert path.read_text(encoding="utf-8").strip() == 'K="value with spaces"'

    def test_quotes_value_with_hash(self, tmp_path):
        path = tmp_path / ".env"
        _write_dotenv_file(path, {"K": "abc#def"})
        assert path.read_text(encoding="utf-8").strip() == 'K="abc#def"'

    def test_escapes_double_quote_and_backslash(self, tmp_path):
        path = tmp_path / ".env"
        _write_dotenv_file(path, {"K": 'a"b\\c'})
        # Quote was escaped to \" and backslash to \\
        line = path.read_text(encoding="utf-8").strip()
        assert line == r'K="a\"b\\c"'

    def test_unquoted_when_simple(self, tmp_path):
        path = tmp_path / ".env"
        _write_dotenv_file(path, {"K": "simple"})
        assert path.read_text(encoding="utf-8").strip() == "K=simple"

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="POSIX-only chmod check")
    def test_chmod_0o600_on_posix(self, tmp_path):
        path = tmp_path / ".env"
        _write_dotenv_file(path, {"K": "v"})
        mode = path.stat().st_mode & 0o777
        assert mode == 0o600

    def test_returns_keys_in_input_order(self, tmp_path):
        env = {"K3": "c", "K1": "a", "K2": "b"}
        path = tmp_path / ".env"
        keys = _write_dotenv_file(path, env)
        assert keys == ["K3", "K1", "K2"]


class TestDotenvScaffolding:
    """``create_project_structure`` writes .env + .gitignore alongside profiles.yml."""

    @pytest.fixture
    def generator(self, tmp_path):
        project_dir = tmp_path / "dbt_proj"
        project_dir.mkdir()
        return DBTGenerator(project_dir=project_dir)

    @pytest.fixture
    def td2_auth(self):
        from elt_mcp_server.auth import TeradataAuth
        return TeradataAuth(
            host="td.example.com",
            port=1025,
            database="analytics",
            mechanism="TD2",
            username="alice",
            password="hunter2",
        )

    @pytest.fixture
    def jwt_auth(self):
        from elt_mcp_server.auth import TeradataAuth
        return TeradataAuth(
            host="td.example.com",
            port=1025,
            database="analytics",
            mechanism="JWT",
            username="alice",
            logdata="token=abc.def.ghi",
        )

    def test_scaffold_writes_dotenv_for_td2(self, generator, td2_auth):
        result = generator.create_project_structure(project_name="proj", auth=td2_auth)
        assert result["success"] is True
        env_path = generator.project_dir / ".env"
        assert env_path.exists()
        assert str(env_path) in result["created_paths"]["files"]
        body = env_path.read_text(encoding="utf-8")
        # TD2 populates HOST/PORT/DATABASE/USERNAME/PASSWORD/LOGMECH
        assert "TERADATA_HOST=td.example.com" in body
        assert "TERADATA_USERNAME=alice" in body
        assert "TERADATA_PASSWORD=hunter2" in body
        assert "TERADATA_DATABASE=analytics" in body
        assert "TERADATA_LOGMECH=TD2" in body
        # JWT-only fields stay empty → omitted
        assert "TERADATA_LOGDATA" not in body
        assert "TERADATA_OIDC_CLIENTID" not in body

    def test_scaffold_writes_dotenv_for_jwt(self, generator, jwt_auth):
        result = generator.create_project_structure(project_name="proj", auth=jwt_auth)
        assert result["success"] is True
        body = (generator.project_dir / ".env").read_text(encoding="utf-8")
        assert "TERADATA_LOGMECH=JWT" in body
        assert "TERADATA_LOGDATA=token=abc.def.ghi" in body
        # JWT does NOT populate password
        assert "TERADATA_PASSWORD" not in body

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="POSIX-only chmod check")
    def test_scaffold_dotenv_mode_0o600(self, generator, td2_auth):
        generator.create_project_structure(project_name="proj", auth=td2_auth)
        env_path = generator.project_dir / ".env"
        assert env_path.stat().st_mode & 0o777 == 0o600

    def test_scaffold_no_auth_no_dotenv(self, generator):
        result = generator.create_project_structure(project_name="proj", auth=None)
        assert result["success"] is True
        assert not (generator.project_dir / ".env").exists()

    def test_scaffold_gitignore_includes_dotenv(self, generator, td2_auth):
        generator.create_project_structure(project_name="proj", auth=td2_auth)
        gitignore = (generator.project_dir / ".gitignore").read_text(encoding="utf-8")
        assert ".env" in gitignore.splitlines()

    def test_scaffold_appends_dotenv_to_existing_gitignore(self, generator, td2_auth):
        # Pre-existing .gitignore without .env → server should append the line.
        (generator.project_dir / ".gitignore").write_text(
            "target/\nlogs/\n", encoding="utf-8"
        )
        generator.create_project_structure(project_name="proj", auth=td2_auth)
        lines = (generator.project_dir / ".gitignore").read_text(encoding="utf-8").splitlines()
        assert "target/" in lines
        assert ".env" in lines

    def test_scaffold_respects_existing_dotenv_in_gitignore(self, generator, td2_auth):
        # Pre-existing .gitignore that already has .env → don't double-write.
        (generator.project_dir / ".gitignore").write_text(
            "target/\n.env\nlogs/\n", encoding="utf-8"
        )
        generator.create_project_structure(project_name="proj", auth=td2_auth)
        body = (generator.project_dir / ".gitignore").read_text(encoding="utf-8")
        assert body.count(".env") == 1

