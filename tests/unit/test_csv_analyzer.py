"""Tests for CSV analyzer with delimiter detection."""

import pytest

from elt_mcp_server.utils.csv_analyzer import CSVAnalyzer


@pytest.fixture
def analyzer():
    return CSVAnalyzer(sample_rows=100)


@pytest.fixture
def pipe_delimited_file(tmp_path):
    """Create a pipe-delimited data file."""
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
def comma_delimited_file(tmp_path):
    """Create a comma-delimited CSV file."""
    content = (
        "id,name,amount,status\n"
        "1,Alice,100.50,active\n"
        "2,Bob,200.75,inactive\n"
        "3,Carol,300.00,active\n"
    )
    p = tmp_path / "data.csv"
    p.write_text(content, encoding="utf-8")
    return str(p)


@pytest.fixture
def tab_delimited_file(tmp_path):
    """Create a tab-delimited data file."""
    content = "id\tname\tamount\n1\tAlice\t100.50\n2\tBob\t200.75\n"
    p = tmp_path / "data.tsv"
    p.write_text(content, encoding="utf-8")
    return str(p)


@pytest.fixture
def semicolon_delimited_file(tmp_path):
    """Create a semicolon-delimited data file."""
    content = "id;name;amount;status\n1;Alice;100.50;active\n2;Bob;200.75;inactive\n"
    p = tmp_path / "data.csv"
    p.write_text(content, encoding="utf-8")
    return str(p)


class TestDelimiterDetection:
    """Tests for auto-detecting file delimiters."""

    def test_detect_pipe_delimiter(self, analyzer, pipe_delimited_file):
        detected = analyzer.detect_delimiter(pipe_delimited_file)
        assert detected == "|"

    def test_detect_comma_delimiter(self, analyzer, comma_delimited_file):
        detected = analyzer.detect_delimiter(comma_delimited_file)
        assert detected == ","

    def test_detect_tab_delimiter(self, analyzer, tab_delimited_file):
        detected = analyzer.detect_delimiter(tab_delimited_file)
        assert detected == "\t"

    def test_detect_semicolon_delimiter(self, analyzer, semicolon_delimited_file):
        detected = analyzer.detect_delimiter(semicolon_delimited_file)
        assert detected == ";"

    def test_fallback_on_detection_failure(self, analyzer, tmp_path):
        """When detection fails, should default to comma."""
        p = tmp_path / "single_column.txt"
        p.write_text("value\n1\n2\n3\n", encoding="utf-8")
        detected = analyzer.detect_delimiter(str(p))
        assert detected == ","


class TestAnalyzeWithPipeDelimiter:
    """Tests for analyzing pipe-delimited files."""

    def test_analyze_pipe_file_with_explicit_delimiter(self, analyzer, pipe_delimited_file):
        """When delimiter='|' is explicitly passed, columns should be parsed correctly."""
        result = analyzer.analyze_csv(pipe_delimited_file, delimiter="|")
        assert result.column_count == 10
        assert result.delimiter == "|"
        col_names = [c.name for c in result.columns]
        assert "order_id" in col_names
        assert "order_status" in col_names

    def test_analyze_pipe_file_with_auto_detection(self, analyzer, pipe_delimited_file):
        """When delimiter=None (auto), pipe should be detected and columns parsed."""
        result = analyzer.analyze_csv(pipe_delimited_file, delimiter=None)
        assert result.column_count == 10
        assert result.delimiter == "|"
        col_names = [c.name for c in result.columns]
        assert "order_id" in col_names
        assert "total_amount" in col_names

    def test_analyze_pipe_file_default_delimiter(self, analyzer, pipe_delimited_file):
        """Default delimiter=None should auto-detect pipe."""
        result = analyzer.analyze_csv(pipe_delimited_file)
        assert result.column_count == 10
        assert result.delimiter == "|"

    def test_pipe_file_row_count(self, analyzer, pipe_delimited_file):
        result = analyzer.analyze_csv(pipe_delimited_file, delimiter="|")
        assert result.row_count == 3

    def test_pipe_file_has_header(self, analyzer, pipe_delimited_file):
        result = analyzer.analyze_csv(pipe_delimited_file, delimiter="|")
        assert result.has_header is True


class TestAnalyzeWithCommaDelimiter:
    """Ensure comma-delimited files still work after changes."""

    def test_explicit_comma(self, analyzer, comma_delimited_file):
        result = analyzer.analyze_csv(comma_delimited_file, delimiter=",")
        assert result.column_count == 4
        assert result.delimiter == ","

    def test_auto_detect_comma(self, analyzer, comma_delimited_file):
        result = analyzer.analyze_csv(comma_delimited_file)
        assert result.column_count == 4
        assert result.delimiter == ","

    def test_column_names_comma(self, analyzer, comma_delimited_file):
        result = analyzer.analyze_csv(comma_delimited_file, delimiter=",")
        col_names = [c.name for c in result.columns]
        assert col_names == ["id", "name", "amount", "status"]


class TestAnalyzeWithTabDelimiter:
    """Tests for tab-delimited files."""

    def test_explicit_tab(self, analyzer, tab_delimited_file):
        result = analyzer.analyze_csv(tab_delimited_file, delimiter="\t")
        assert result.column_count == 3

    def test_auto_detect_tab(self, analyzer, tab_delimited_file):
        result = analyzer.analyze_csv(tab_delimited_file)
        assert result.column_count == 3
        assert result.delimiter == "\t"


class TestEmptyFile:
    """Tests for empty CSV files."""

    def test_analyze_empty_file(self, analyzer, tmp_path):
        """Empty file should return a CSVAnalysis with zero rows/columns."""
        p = tmp_path / "empty.csv"
        p.write_text("", encoding="utf-8")
        result = analyzer.analyze_csv(str(p))
        assert result.row_count == 0
        assert result.column_count == 0
        assert result.columns == []
        assert result.has_header is False
        assert result.file_size_bytes == 0
        assert result.estimated_load_time_seconds == 0.0

    def test_detect_header_empty_file(self, analyzer, tmp_path):
        """_detect_header should return False for an empty file."""
        from pathlib import Path

        p = tmp_path / "empty.csv"
        p.write_text("", encoding="utf-8")
        assert analyzer.detect_header(Path(p), ",", "utf-8") is False

    def test_detect_header_single_empty_line(self, analyzer, tmp_path):
        """A file with only a newline has an empty header row with 0 columns."""
        from pathlib import Path

        p = tmp_path / "newline_only.csv"
        p.write_text("\n", encoding="utf-8")
        assert analyzer.detect_header(Path(p), ",", "utf-8") is False

    def test_detect_header_all_empty_cells(self, analyzer, tmp_path):
        """A single row of empty cells (e.g. ',,') is not a header."""
        from pathlib import Path

        p = tmp_path / "empty_cells.csv"
        p.write_text(",,\n", encoding="utf-8")
        assert analyzer.detect_header(Path(p), ",", "utf-8") is False

    def test_detect_header_all_whitespace_cells(self, analyzer, tmp_path):
        """A single row of whitespace-only cells is not a header."""
        from pathlib import Path

        p = tmp_path / "whitespace_cells.csv"
        p.write_text("  ,  ,  \n", encoding="utf-8")
        assert analyzer.detect_header(Path(p), ",", "utf-8") is False

    def test_detect_header_real_single_row(self, analyzer, tmp_path):
        """A single row with real column names should still be detected as header."""
        from pathlib import Path

        p = tmp_path / "header_only.csv"
        p.write_text("id,name,value\n", encoding="utf-8")
        assert analyzer.detect_header(Path(p), ",", "utf-8") is True


class TestColumnNameValidation:
    """Ensure column names from pipe-delimited files are valid SQL identifiers."""

    def test_pipe_columns_are_individual_names(self, analyzer, pipe_delimited_file):
        """The critical bug: pipe-delimited header must NOT be treated as one big column name."""
        result = analyzer.analyze_csv(pipe_delimited_file, delimiter="|")
        for col in result.columns:
            # Each column name should not contain pipe characters
            assert "|" not in col.name, f"Column name contains pipe: {col.name!r}"
            # Each column name should be a simple identifier
            assert col.name.replace("_", "").isalnum(), f"Invalid column name: {col.name!r}"
