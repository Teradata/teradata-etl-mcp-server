"""Tests for Airflow TdLoad DAG generator, including delimiter handling."""

import ast

import pytest

from elt_mcp_server.generators.airflow_tdload_dag_generator import (
    AirflowTdLoadDAGGenerator,
    AirflowTdLoadDAGGeneratorError,
)
from elt_mcp_server.utils.csv_analyzer import CSVAnalyzer


@pytest.fixture
def dag_generator(tmp_path):
    return AirflowTdLoadDAGGenerator(dags_folder=tmp_path)


@pytest.fixture
def pipe_delimited_file(tmp_path):
    """Create a pipe-delimited data file (the exact format that was failing)."""
    content = (
        "order_id|order_date|customer_id|product_code|quantity|unit_price|shipping_cost|tax_amount|total_amount|order_status\n"
        "ORD9001|2025-01-30|CUST5001|PROD-A123|5|99.99|15.00|35.00|549.95|shipped\n"
        "ORD9002|2025-01-30|CUST5002|PROD-B456|2|149.99|20.00|21.00|340.98|processing\n"
        "ORD9003|2025-01-30|CUST5003|PROD-C789|1|299.99|25.00|23.25|348.24|delivered\n"
    )
    p = tmp_path / "orders.txt"
    p.write_text(content, encoding="utf-8")
    return str(p)


@pytest.fixture
def comma_csv_file(tmp_path):
    """Create a standard comma-delimited CSV file."""
    content = "id,name,amount,status\n1,Alice,100.50,active\n2,Bob,200.75,inactive\n"
    p = tmp_path / "data.csv"
    p.write_text(content, encoding="utf-8")
    return str(p)


class TestSanitizeIdentifier:
    """Tests for SQL identifier validation."""

    def test_valid_identifier(self):
        assert AirflowTdLoadDAGGenerator._sanitize_identifier("order_id") == "order_id"

    def test_valid_identifier_with_hash(self):
        assert AirflowTdLoadDAGGenerator._sanitize_identifier("temp#table") == "temp#table"

    def test_pipe_in_identifier_raises(self):
        """This was the original bug — entire pipe-delimited header treated as one identifier."""
        with pytest.raises(AirflowTdLoadDAGGeneratorError, match="Disallowed characters"):
            AirflowTdLoadDAGGenerator._sanitize_identifier("order_id|order_date|customer_id")

    def test_empty_identifier_raises(self):
        with pytest.raises(AirflowTdLoadDAGGeneratorError, match="cannot be empty"):
            AirflowTdLoadDAGGenerator._sanitize_identifier("")

    def test_digit_start_raises(self):
        with pytest.raises(AirflowTdLoadDAGGeneratorError, match="Cannot start with a digit"):
            AirflowTdLoadDAGGenerator._sanitize_identifier("1column")


class TestPipeDelimitedEndToEnd:
    """End-to-end test: pipe-delimited file → analyze → generate DAG."""

    def test_pipe_file_generates_valid_dag(self, dag_generator, pipe_delimited_file):
        """The exact scenario that was failing before the fix."""
        analyzer = CSVAnalyzer(sample_rows=100)
        analysis = analyzer.analyze_csv(pipe_delimited_file, delimiter="|")

        columns_for_ddl = [
            {
                "name": col.name,
                "type": col.inferred_teradata_type,
                "nullable": True,
            }
            for col in analysis.columns
        ]

        dag_code = dag_generator.generate_file_loading_dag(
            dag_id="test_pipe_load",
            description="Test pipe-delimited loading",
            source_file_path=pipe_delimited_file,
            target_database="test_db",
            target_table="orders",
            delimiter="|",
            teradata_conn_id="teradata_default",
            ssh_conn_id="ssh_default",
            columns=columns_for_ddl,
            skip_rows=1,
        )

        # DAG code must be valid Python (this was the compile failure)
        ast.parse(dag_code)

        # Verify delimiter is in the generated code
        assert "source_text_delimiter='|'" in dag_code

        # Verify individual column names appear (not the whole header)
        assert "order_id" in dag_code
        assert "order_status" in dag_code
        # The pipe-joined header must NOT appear
        assert "order_id|order_date" not in dag_code

    def test_pipe_file_auto_detected_generates_valid_dag(self, dag_generator, pipe_delimited_file):
        """Auto-detection of pipe delimiter should also produce valid DAG."""
        analyzer = CSVAnalyzer(sample_rows=100)
        # delimiter=None triggers auto-detection
        analysis = analyzer.analyze_csv(pipe_delimited_file)

        assert analysis.delimiter == "|"
        assert analysis.column_count == 10

        columns_for_ddl = [
            {
                "name": col.name,
                "type": col.inferred_teradata_type,
                "nullable": True,
            }
            for col in analysis.columns
        ]

        dag_code = dag_generator.generate_file_loading_dag(
            dag_id="test_pipe_auto",
            description="Test pipe auto-detect",
            source_file_path=pipe_delimited_file,
            target_database="test_db",
            target_table="orders",
            delimiter=analysis.delimiter,
            teradata_conn_id="teradata_default",
            ssh_conn_id="ssh_default",
            columns=columns_for_ddl,
            skip_rows=1 if analysis.has_header else 0,
        )

        ast.parse(dag_code)
        assert "source_text_delimiter='|'" in dag_code


class TestCommaDelimitedStillWorks:
    """Regression tests: comma-delimited files must still work."""

    def test_comma_csv_generates_valid_dag(self, dag_generator, comma_csv_file):
        analyzer = CSVAnalyzer(sample_rows=100)
        analysis = analyzer.analyze_csv(comma_csv_file, delimiter=",")

        columns_for_ddl = [
            {
                "name": col.name,
                "type": col.inferred_teradata_type,
                "nullable": True,
            }
            for col in analysis.columns
        ]

        dag_code = dag_generator.generate_file_loading_dag(
            dag_id="test_comma_load",
            description="Test comma loading",
            source_file_path=comma_csv_file,
            target_database="test_db",
            target_table="data_table",
            delimiter=",",
            teradata_conn_id="teradata_default",
            ssh_conn_id="ssh_default",
            columns=columns_for_ddl,
            skip_rows=1,
        )

        ast.parse(dag_code)
        assert "source_text_delimiter=','" in dag_code
        assert "id" in dag_code
        assert "name" in dag_code


class TestFileLoadingDag:
    """General tests for generate_file_loading_dag."""

    def test_skip_rows_generates_tdload_options(self, dag_generator, comma_csv_file):
        dag_code = dag_generator.generate_file_loading_dag(
            dag_id="test_skip",
            description="Test skip rows",
            source_file_path=comma_csv_file,
            target_database="test_db",
            target_table="test_table",
            skip_rows=1,
            columns=[{"name": "col1", "type": "VARCHAR(50)", "nullable": True}],
        )
        ast.parse(dag_code)
        assert "--FileReaderSkipRows 1" in dag_code

    def test_no_skip_rows_no_tdload_options(self, dag_generator, comma_csv_file):
        dag_code = dag_generator.generate_file_loading_dag(
            dag_id="test_no_skip",
            description="Test no skip",
            source_file_path=comma_csv_file,
            target_database="test_db",
            target_table="test_table",
            skip_rows=0,
            columns=[{"name": "col1", "type": "VARCHAR(50)", "nullable": True}],
        )
        ast.parse(dag_code)
        assert "--FileReaderSkipRows" not in dag_code


class TestConvertTeradataTypeToSql:
    """Tests for _convert_teradata_type_to_sql type code expansion."""

    def test_known_type_code(self):
        assert AirflowTdLoadDAGGenerator._convert_teradata_type_to_sql("CV") == "VARCHAR"

    def test_known_type_code_with_length(self):
        assert (
            AirflowTdLoadDAGGenerator._convert_teradata_type_to_sql("CV", length=500)
            == "VARCHAR(500)"
        )

    def test_known_decimal_with_precision_and_scale(self):
        assert (
            AirflowTdLoadDAGGenerator._convert_teradata_type_to_sql("D", precision=15, scale=2)
            == "DECIMAL(15,2)"
        )

    def test_already_expanded_varchar_with_length_preserved(self):
        """VARCHAR(1000) must NOT be downgraded to VARCHAR(255)."""
        assert (
            AirflowTdLoadDAGGenerator._convert_teradata_type_to_sql("VARCHAR(1000)")
            == "VARCHAR(1000)"
        )

    def test_already_expanded_decimal_with_params_preserved(self):
        """DECIMAL(15,2) must NOT be downgraded to VARCHAR(255)."""
        assert (
            AirflowTdLoadDAGGenerator._convert_teradata_type_to_sql("DECIMAL(15,2)")
            == "DECIMAL(15,2)"
        )

    def test_already_expanded_char_preserved(self):
        assert AirflowTdLoadDAGGenerator._convert_teradata_type_to_sql("CHAR(10)") == "CHAR(10)"

    def test_bare_unknown_alphanumeric_type_preserved(self):
        """A bare alphanumeric type name like INTEGER passes through."""
        assert AirflowTdLoadDAGGenerator._convert_teradata_type_to_sql("INTEGER") == "INTEGER"

    def test_injection_attempt_falls_back(self):
        """SQL injection via type code falls back to VARCHAR(255)."""
        assert (
            AirflowTdLoadDAGGenerator._convert_teradata_type_to_sql("VARCHAR(1); DROP TABLE x--")
            == "VARCHAR(255)"
        )

    def test_empty_string_falls_back(self):
        assert AirflowTdLoadDAGGenerator._convert_teradata_type_to_sql("") == "VARCHAR(255)"

    def test_validation_bteq_file(self, dag_generator, comma_csv_file):
        dag_code = dag_generator.generate_file_loading_dag(
            dag_id="test_val_file",
            description="Test with validation file",
            source_file_path=comma_csv_file,
            target_database="test_db",
            target_table="test_table",
            validation_bteq_file="/path/to/validate.bteq",
            columns=[{"name": "col1", "type": "VARCHAR(50)", "nullable": True}],
        )
        ast.parse(dag_code)
        assert "validate_load" in dag_code
        assert "/path/to/validate.bteq" in dag_code
