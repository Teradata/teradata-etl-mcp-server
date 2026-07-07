"""CSV analysis and schema inference utilities.

This module provides functionality to analyze CSV files and infer their structure,
including column types, data quality, and optimal loading parameters.
"""

import csv
import io
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


logger = logging.getLogger(__name__)


@dataclass
class CSVColumn:
    """Metadata for a CSV column."""

    name: str
    inferred_teradata_type: str
    sample_values: list[str]
    null_count: int
    unique_count: int
    max_length: int | None = None


@dataclass
class CSVAnalysis:
    """Complete analysis of CSV file."""

    file_path: str
    file_size_bytes: int
    file_size_mb: float
    row_count: int
    column_count: int
    columns: list[CSVColumn]
    delimiter: str
    encoding: str
    has_header: bool
    estimated_load_time_seconds: float


class CSVAnalyzer:
    """Analyzes CSV files to infer structure and recommend loading strategies."""

    def __init__(self, sample_rows: int = 1000):
        """
        Initialize CSV analyzer.

        Args:
            sample_rows: Number of rows to sample for analysis
        """
        self.sample_rows = sample_rows

    def detect_delimiter(
        self,
        file_path: str,
        encoding: str = "utf-8",
    ) -> str:
        """
        Auto-detect the delimiter used in a delimited file.

        Uses csv.Sniffer on a sample of the file. Falls back to comma if
        detection fails.

        Args:
            file_path: Path to the delimited file
            encoding: File encoding

        Returns:
            Detected delimiter character
        """
        try:
            with open(file_path, encoding=encoding, newline="") as f:
                sample = f.read(8192)
            if not sample.strip():
                logger.debug("Empty file, defaulting delimiter to comma")
                return ","
            dialect = csv.Sniffer().sniff(sample, delimiters=",|\t;")
            detected = dialect.delimiter
            logger.info("Auto-detected delimiter: %r", detected)
            return detected
        except (csv.Error, OSError, UnicodeDecodeError):
            logger.warning("Delimiter auto-detection failed, defaulting to comma")
            return ","

    def analyze_csv(
        self,
        file_path: str,
        delimiter: str | None = None,
        encoding: str = "utf-8",
    ) -> CSVAnalysis:
        """
        Analyze CSV file structure and infer Teradata types.

        Args:
            file_path: Path to CSV file
            delimiter: CSV delimiter. If None, auto-detected from file content.
            encoding: File encoding (default: utf-8)

        Returns:
            CSVAnalysis object with complete file analysis
        """
        file_path_obj = Path(file_path)

        if not file_path_obj.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        # Auto-detect delimiter if not explicitly provided
        if delimiter is None:
            delimiter = self.detect_delimiter(file_path, encoding)
        else:
            if not isinstance(delimiter, str) or len(delimiter) != 1:
                raise ValueError(
                    f"CSV delimiter must be a single-character string, got {delimiter!r}"
                )

        # Get file size
        file_size_bytes = file_path_obj.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)

        # Short-circuit for empty files
        if file_size_bytes == 0:
            logger.info("Empty CSV file: %s", file_path)
            return CSVAnalysis(
                file_path=str(file_path_obj),
                file_size_bytes=0,
                file_size_mb=0.0,
                row_count=0,
                column_count=0,
                columns=[],
                delimiter=delimiter,
                encoding=encoding,
                has_header=False,
                estimated_load_time_seconds=0.0,
            )

        logger.info(
            "Analyzing CSV file: %s (%.2f MB, delimiter=%r)", file_path, file_size_mb, delimiter
        )

        # Use pandas if available for better type inference
        if PANDAS_AVAILABLE:
            return self._analyze_with_pandas(
                file_path_obj, file_size_bytes, file_size_mb, delimiter, encoding
            )
        else:
            return self._analyze_with_csv(
                file_path_obj, file_size_bytes, file_size_mb, delimiter, encoding
            )

    def detect_header(
        self,
        file_path: Path,
        delimiter: str,
        encoding: str,
    ) -> bool:
        """Detect if CSV file has a header row.

        Uses ``csv.reader`` configured with the known *delimiter* so that
        column splitting is identical to how the file will actually be
        parsed.  (``csv.Sniffer.has_header()`` re-sniffs the delimiter
        internally and may pick a different one, causing wrong column
        splits on non-comma files.)

        The algorithm mirrors CPython's ``Sniffer.has_header``: determine
        a consistent type for each column from data rows, then check
        whether the first row's values differ from those types.

        Args:
            file_path: Path to CSV file
            delimiter: CSV delimiter used for column splitting
            encoding: File encoding

        Returns:
            True if header is detected, False otherwise
        """
        try:
            with open(file_path, encoding=encoding, newline="") as f:
                sample = "".join(f.readline() for _ in range(21))

            rdr = csv.reader(io.StringIO(sample), delimiter=delimiter)
            try:
                header = next(rdr)
            except StopIteration:
                return False  # empty file — no header

            columns = len(header)
            if columns == 0:
                return False

            # Determine a consistent type per column from data rows.
            # Numeric columns store the type (int/float/complex);
            # string columns store the string length (an int value).
            col_types: dict[int, Any] = dict.fromkeys(range(columns))
            checked = 0
            for row in rdr:
                if checked >= 20:
                    break
                checked += 1
                if len(row) != columns:
                    continue
                for col in list(col_types):
                    cell = row[col]
                    for num_type in (int, float, complex):
                        try:
                            num_type(cell)
                            cell_type: Any = num_type
                            break
                        except (ValueError, OverflowError):
                            pass
                    else:
                        cell_type = len(cell)

                    if col_types[col] is None:
                        col_types[col] = cell_type
                    elif cell_type != col_types[col]:
                        del col_types[col]

            if checked == 0:
                # Single-row file — assume header only if the row has
                # meaningful content (not all empty/whitespace cells).
                return any(cell.strip() for cell in header)

            # Score: +1 when header value differs from the column type,
            #        -1 when it matches.
            score = 0
            for col, col_type in col_types.items():
                if col_type is None:
                    continue
                if isinstance(col_type, type):
                    # Numeric column — does the header parse as that type?
                    try:
                        col_type(header[col])
                        score -= 1
                    except (ValueError, TypeError):
                        score += 1
                else:
                    # String column — compare lengths
                    score += 1 if len(header[col]) != col_type else -1

            has_header = score > 0
            logger.info(
                "Header detection: %s", "Header found" if has_header else "No header detected"
            )
            return has_header
        except Exception as e:
            logger.warning("Header detection failed: %s. Assuming header exists.", e)
            return True

    def _analyze_with_pandas(
        self,
        file_path: Path,
        file_size_bytes: int,
        file_size_mb: float,
        delimiter: str,
        encoding: str,
    ) -> CSVAnalysis:
        """Analyze CSV using pandas for accurate type inference."""
        # Detect if file has header
        has_header = self.detect_header(file_path, delimiter, encoding)

        # Read sample for analysis
        if has_header:
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=encoding,
                nrows=self.sample_rows,
            )
        else:
            # No header - use default column names
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=encoding,
                nrows=self.sample_rows,
                header=None,
            )
            # Generate column names: col_1, col_2, etc.
            df.columns = [f"col_{i + 1}" for i in range(len(df.columns))]

        # Get total row count (fast estimation)
        with open(file_path, encoding=encoding) as f:
            row_count = sum(1 for _ in f)
            if has_header:
                row_count -= 1  # Subtract header

        # Analyze each column
        columns = []
        for col_name in df.columns:
            col_data = df[col_name]

            # Infer Teradata type
            td_type = self._infer_teradata_type(col_data)

            # Get sample values
            sample_values = col_data.dropna().head(5).astype(str).tolist()

            # Get statistics
            null_count = col_data.isna().sum()
            unique_count = col_data.nunique()

            # Calculate max length for string columns
            max_length = None
            if pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
                max_length = col_data.astype(str).str.len().max()

            columns.append(
                CSVColumn(
                    name=col_name,
                    inferred_teradata_type=td_type,
                    sample_values=sample_values,
                    null_count=int(null_count),
                    unique_count=int(unique_count),
                    max_length=int(max_length) if max_length else None,
                )
            )

        # Estimate load time (rough estimate: 100MB/sec for TPT)
        estimated_load_time = file_size_mb / 100.0 * 60  # seconds

        return CSVAnalysis(
            file_path=str(file_path),
            file_size_bytes=file_size_bytes,
            file_size_mb=file_size_mb,
            row_count=row_count,
            column_count=len(columns),
            columns=columns,
            delimiter=delimiter,
            encoding=encoding,
            has_header=has_header,
            estimated_load_time_seconds=estimated_load_time,
        )

    def _analyze_with_csv(
        self,
        file_path: Path,
        file_size_bytes: int,
        file_size_mb: float,
        delimiter: str,
        encoding: str,
    ) -> CSVAnalysis:
        """Analyze CSV using standard library (fallback if pandas not available)."""
        # Detect if file has header
        has_header = self.detect_header(file_path, delimiter, encoding)

        columns_data = {}
        row_count = 0

        with open(file_path, encoding=encoding, newline="") as f:
            if has_header:
                reader = csv.DictReader(f, delimiter=delimiter)
                fieldnames = reader.fieldnames
            else:
                # No header - read first row to get column count
                first_row = next(csv.reader(f, delimiter=delimiter))
                f.seek(0)
                # Generate column names: col_1, col_2, etc.
                fieldnames = [f"col_{i + 1}" for i in range(len(first_row))]
                reader = csv.DictReader(f, delimiter=delimiter, fieldnames=fieldnames)

            # Initialize column tracking
            for col_name in fieldnames:
                columns_data[col_name] = {
                    "values": [],
                    "null_count": 0,
                    "max_length": 0,
                }

            # Read sample rows
            for i, row in enumerate(reader):
                if i >= self.sample_rows:
                    break

                row_count += 1

                for col_name, value in row.items():
                    if value is None or value.strip() == "":
                        columns_data[col_name]["null_count"] += 1
                    else:
                        columns_data[col_name]["values"].append(value)
                        columns_data[col_name]["max_length"] = max(
                            columns_data[col_name]["max_length"], len(value)
                        )

        # Infer types for each column
        columns = []
        for col_name, col_info in columns_data.items():
            td_type = self._infer_type_from_values(col_info["values"])

            columns.append(
                CSVColumn(
                    name=col_name,
                    inferred_teradata_type=td_type,
                    sample_values=col_info["values"][:5],
                    null_count=col_info["null_count"],
                    unique_count=len(set(col_info["values"])),
                    max_length=col_info["max_length"] if td_type.startswith("VARCHAR") else None,
                )
            )

        # Estimate load time
        estimated_load_time = file_size_mb / 100.0 * 60

        return CSVAnalysis(
            file_path=str(file_path),
            file_size_bytes=file_size_bytes,
            file_size_mb=file_size_mb,
            row_count=row_count,
            column_count=len(columns),
            columns=columns,
            delimiter=delimiter,
            encoding=encoding,
            has_header=has_header,
            estimated_load_time_seconds=estimated_load_time,
        )

    def _infer_teradata_type(self, series: "pd.Series") -> str:
        """
        Infer Teradata type from pandas Series.

        Args:
            series: Pandas Series

        Returns:
            Teradata type string
        """
        import pandas as pd

        # Integer types
        if pd.api.types.is_integer_dtype(series):
            max_val = series.max() if len(series) > 0 else 0
            if max_val <= 127:
                return "BYTEINT"
            elif max_val <= 32767:
                return "SMALLINT"
            elif max_val <= 2147483647:
                return "INTEGER"
            else:
                return "BIGINT"

        # Float types
        if pd.api.types.is_float_dtype(series):
            return "DECIMAL(15,2)"  # Default decimal

        # Date/Time types
        if pd.api.types.is_datetime64_any_dtype(series):
            return "TIMESTAMP"

        # Boolean
        if pd.api.types.is_bool_dtype(series):
            return "BYTEINT"  # Store as 0/1

        # String types (default)
        max_len = series.astype(str).str.len().max()
        if pd.isna(max_len) or max_len == 0:
            return "VARCHAR(255)"

        # Adjust length with buffer
        recommended_len = min(int(max_len * 1.5), 64000)

        if recommended_len <= 255:
            return f"VARCHAR({max(recommended_len, 50)})"
        else:
            return f"VARCHAR({recommended_len})"

    def _infer_type_from_values(self, values: list[str]) -> str:
        """
        Infer Teradata type from sample values (fallback without pandas).

        Args:
            values: List of sample values

        Returns:
            Teradata type string
        """
        if not values:
            return "VARCHAR(255)"

        # Try integer
        try:
            max_val = max(int(v) for v in values if v)
            if max_val <= 127:
                return "BYTEINT"
            elif max_val <= 32767:
                return "SMALLINT"
            else:
                return "INTEGER"
        except (ValueError, TypeError):
            pass

        # Try float
        try:
            for v in values:
                if v:
                    float(v)
            return "DECIMAL(15,2)"
        except (ValueError, TypeError):
            pass

        # Try date
        for date_format in ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]:
            try:
                for v in values[:5]:  # Check first 5
                    datetime.strptime(v, date_format)
                return "DATE"
            except (ValueError, TypeError):
                continue

        # Default to VARCHAR
        max_len = max((len(v) for v in values if v), default=0)
        recommended_len = min(int(max_len * 1.5), 64000)
        return f"VARCHAR({max(recommended_len, 50)})"

    def get_tpt_column_definitions(self, analysis: CSVAnalysis) -> list[dict[str, str]]:
        """
        Convert CSV analysis to TPT column definitions.

        Args:
            analysis: CSV analysis result

        Returns:
            List of column definitions for TPT generator
        """
        columns = []
        for col in analysis.columns:
            col_def = {
                "name": col.name,
                "type": col.inferred_teradata_type,
            }

            # Add format for date/time columns
            if "DATE" in col.inferred_teradata_type:
                col_def["format"] = "YYYY-MM-DD"
            elif "TIMESTAMP" in col.inferred_teradata_type:
                col_def["format"] = "YYYY-MM-DDBHH:MI:SS"

            columns.append(col_def)

        return columns

    def match_to_teradata_table(
        self,
        analysis: CSVAnalysis,
        table_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Match CSV columns to Teradata table structure.

        Args:
            analysis: CSV analysis result
            table_metadata: Teradata table metadata

        Returns:
            Dictionary with match results and warnings
        """
        csv_columns = {col.name.lower() for col in analysis.columns}
        table_columns = {col["column_name"].lower() for col in table_metadata.get("columns", [])}

        # Find mismatches
        missing_in_csv = table_columns - csv_columns
        extra_in_csv = csv_columns - table_columns

        # Check type compatibility
        type_warnings = []
        for csv_col in analysis.columns:
            table_col = next(
                (
                    c
                    for c in table_metadata.get("columns", [])
                    if c["column_name"].lower() == csv_col.name.lower()
                ),
                None,
            )

            if table_col:
                csv_type = csv_col.inferred_teradata_type.upper()
                table_type = table_col["data_type"].upper()

                # Simple type compatibility check
                if not self._types_compatible(csv_type, table_type):
                    type_warnings.append(
                        {
                            "column": csv_col.name,
                            "csv_type": csv_type,
                            "table_type": table_type,
                            "warning": f"Type mismatch: CSV has {csv_type}, table has {table_type}",
                        }
                    )

        return {
            "compatible": len(missing_in_csv) == 0 and len(type_warnings) == 0,
            "missing_in_csv": list(missing_in_csv),
            "extra_in_csv": list(extra_in_csv),
            "type_warnings": type_warnings,
        }

    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two Teradata types are compatible."""
        # Extract base types
        base1 = type1.split("(")[0].strip()
        base2 = type2.split("(")[0].strip()

        # Numeric types are generally compatible
        numeric_types = {
            "BYTEINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "DECIMAL",
            "NUMERIC",
            "FLOAT",
            "DOUBLE",
        }
        if base1 in numeric_types and base2 in numeric_types:
            return True

        # String types are compatible
        string_types = {"CHAR", "VARCHAR", "NCHAR", "NVARCHAR"}
        if base1 in string_types and base2 in string_types:
            return True

        # Exact match
        return base1 == base2


# Convenience function
def analyze_csv_file(file_path: str, **kwargs) -> CSVAnalysis:
    """
    Convenience function to analyze a CSV file.

    Args:
        file_path: Path to CSV file
        **kwargs: Additional arguments for CSVAnalyzer

    Returns:
        CSVAnalysis object
    """
    analyzer = CSVAnalyzer()
    return analyzer.analyze_csv(file_path, **kwargs)
