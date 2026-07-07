"""Unit tests for Teradata client.

Tests are aligned with the actual production API in teradata_client.py.
The production code creates a new connection per method call via _get_connection(),
so we mock teradatasql.connect and pandas.read_sql at that boundary.
"""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from teradata_etl_mcp_server.auth import TeradataAuth
from teradata_etl_mcp_server.clients.teradata_client import (
    TeradataClient,
    TeradataConnectionError,
    TeradataQueryError,
    _quote_identifier,
    _safe_int,
    _serialize_value,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(columns, rows):
    """Build a pandas DataFrame from column names and row tuples."""
    return pd.DataFrame(rows, columns=columns)


# ---------------------------------------------------------------------------
# TestTeradataClient — tests for the public methods
# ---------------------------------------------------------------------------

class TestTeradataClient:
    """Test suite for TeradataClient public methods."""

    @pytest.fixture
    def mock_conn(self):
        """A mock teradatasql connection object."""
        conn = MagicMock()
        conn.close = MagicMock()
        return conn

    @pytest.fixture
    def client(self):
        """Create a TeradataClient with mocked dependency checks."""
        with (
            patch("teradata_etl_mcp_server.clients.teradata_client._check_teradatasql"),
            patch("teradata_etl_mcp_server.clients.teradata_client._check_pandas"),
        ):
            return TeradataClient(auth=TeradataAuth(
                host="test-host.teradata.com",
                port=1025,
                database="test_db",
                mechanism="TD2",
                username="test_user",
                password="test_password",
            ))

    # ---- Initialization tests ----

    def test_init_stores_attributes(self, client):
        """__init__ stores host, username, database, port, etc."""
        assert client.host == "test-host.teradata.com"
        assert client.username == "test_user"
        assert client.password == "test_password"
        assert client.database == "test_db"
        assert client.port == 1025
        assert client._connected is False

    def test_init_default_values(self):
        """__init__ uses correct defaults for optional params."""
        with (
            patch("teradata_etl_mcp_server.clients.teradata_client._check_teradatasql"),
            patch("teradata_etl_mcp_server.clients.teradata_client._check_pandas"),
        ):
            c = TeradataClient(auth=TeradataAuth(
                host="h", port=1025, database="",
                mechanism="TD2", username="u", password="p",
            ))
        assert c.database == ""
        assert c.port == 1025
        assert c.charset == "UTF8"
        assert c.query_timeout == 300

    # ---- _get_connection tests ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_connection_success(self, mock_tdsql, client, mock_conn):
        """_get_connection calls teradatasql.connect with correct params."""
        mock_tdsql.connect.return_value = mock_conn

        conn = client._get_connection()

        assert conn is mock_conn
        mock_tdsql.connect.assert_called_once_with(
            host="test-host.teradata.com",
            dbs_port="1025",
            encryptdata="true",
            logmech="TD2",
            connect_timeout=str(client.query_timeout * 1000),
            request_timeout=str(client.query_timeout * 1000),
            database="test_db",
            user="test_user",
            password="test_password",
        )

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_connection_failure_raises_connection_error(self, mock_tdsql, client):
        """_get_connection wraps exceptions in TeradataConnectionError."""
        mock_tdsql.connect.side_effect = Exception("Network unreachable")

        with pytest.raises(TeradataConnectionError, match="Cannot connect to Teradata"):
            client._get_connection()

    # ---- test_connection ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_test_connection_success(self, mock_tdsql, mock_pd, client, mock_conn):
        """test_connection returns success dict with version and time."""
        mock_tdsql.connect.return_value = mock_conn

        version_df = _make_df(["version"], [("17.10.03.01 ",)])
        ts_df = _make_df(["ts"], [("2026-02-18 10:00:00",)])
        mock_pd.read_sql.side_effect = [version_df, ts_df]

        result = client.test_connection()

        assert result["connected"] is True
        assert result["host"] == "test-host.teradata.com"
        assert result["version"] == "17.10.03.01"
        assert "current_time" in result
        assert client._connected is True
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_test_connection_failure(self, mock_tdsql, client):
        """test_connection returns error dict on failure."""
        mock_tdsql.connect.side_effect = Exception("Connection refused")

        result = client.test_connection()

        assert result["connected"] is False
        assert "error" in result

    # ---- list_databases ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_list_databases_success(self, mock_tdsql, mock_pd, client, mock_conn):
        """list_databases returns list of stripped database names."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(
            ["DataBaseName"], [("db_one  ",), ("db_two  ",), ("db_three",)]
        )

        result = client.list_databases()

        assert result == ["db_one", "db_two", "db_three"]
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_list_databases_empty(self, mock_tdsql, mock_pd, client, mock_conn):
        """list_databases returns empty list when no databases accessible."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(["DataBaseName"], [])

        result = client.list_databases()

        assert result == []

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_list_databases_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """list_databases raises TeradataQueryError on failure."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("Access denied")

        with pytest.raises(TeradataQueryError, match="List databases failed"):
            client.list_databases()

    # ---- check_database_exists ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_check_database_exists_found(self, mock_tdsql, mock_pd, client, mock_conn):
        """check_database_exists returns True when database is found."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(["cnt"], [(1,)])

        result = client.check_database_exists("prod_db")

        assert result is True
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_check_database_exists_not_found(self, mock_tdsql, mock_pd, client, mock_conn):
        """check_database_exists returns False when database count is 0."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(["cnt"], [(0,)])

        result = client.check_database_exists("fake_db")

        assert result is False
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_check_database_exists_error_raises(self, mock_tdsql, client):
        """check_database_exists raises TeradataQueryError when a query error occurs."""
        mock_tdsql.connect.side_effect = Exception("Network error")

        with pytest.raises(TeradataQueryError, match="Check database exists failed"):
            client.check_database_exists("any_db")

    # ---- list_tables ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_list_tables_success(self, mock_tdsql, mock_pd, client, mock_conn):
        """list_tables returns list of table dicts."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(
            ["TableName", "TableKind", "CommentString", "CreateTimeStamp"],
            [
                ("customers ", "T ", "Customer table", "2025-01-01 00:00:00"),
                ("orders    ", "T ", None, "2025-01-02 00:00:00"),
                ("vw_report ", "V ", "Reporting view", None),
            ],
        )
        # Need pd.notna to work on real values — use real pd, only mock read_sql
        mock_pd.notna = pd.notna

        result = client.list_tables("test_db")

        assert len(result) == 3
        assert result[0]["table"] == "customers"
        assert result[0]["type"] == "T"
        assert result[0]["description"] == "Customer table"
        assert result[1]["description"] is None
        assert result[2]["created_at"] is None
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_list_tables_with_type_filter(self, mock_tdsql, mock_pd, client, mock_conn):
        """list_tables passes table_type filter to query."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(
            ["TableName", "TableKind", "CommentString", "CreateTimeStamp"],
            [("vw_sales ", "V ", None, None)],
        )
        mock_pd.notna = pd.notna

        result = client.list_tables("test_db", table_type="V")

        assert len(result) == 1
        assert result[0]["type"] == "V"
        # Verify the params tuple includes the table_type
        call_args = mock_pd.read_sql.call_args
        assert call_args[1]["params"] == ("test_db", "V")

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_list_tables_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """list_tables raises TeradataQueryError on failure."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("DB error")

        with pytest.raises(TeradataQueryError, match="List tables failed"):
            client.list_tables("test_db")

    # ---- execute_query ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_execute_query_success(self, mock_tdsql, mock_pd, client, mock_conn):
        """execute_query returns list of row dicts."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.isna = pd.isna
        mock_pd.read_sql.return_value = _make_df(
            ["id", "name"], [(1, "Alice"), (2, "Bob")]
        )

        result = client.execute_query("SELECT id, name FROM users")

        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "Alice"}
        assert result[1] == {"id": 2, "name": "Bob"}
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_execute_query_with_params(self, mock_tdsql, mock_pd, client, mock_conn):
        """execute_query converts dict params to tuple."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.isna = pd.isna
        mock_pd.read_sql.return_value = _make_df(["id"], [(42,)])

        result = client.execute_query(
            "SELECT id FROM users WHERE id = ?",
            params={"id": 42},
        )

        assert len(result) == 1
        assert result[0]["id"] == 42
        # Verify params were passed as a tuple
        call_args = mock_pd.read_sql.call_args
        assert call_args[1]["params"] == (42,)

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_execute_query_empty_result(self, mock_tdsql, mock_pd, client, mock_conn):
        """execute_query returns empty list for no rows."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(["id"], [])

        result = client.execute_query("SELECT id FROM users WHERE 1=0")

        assert result == []

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_execute_query_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """execute_query raises TeradataQueryError on failure."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("Syntax error")

        with pytest.raises(TeradataQueryError, match="Query failed"):
            client.execute_query("INVALID SQL")

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_execute_query_serializes_special_types(self, mock_tdsql, mock_pd, client, mock_conn):
        """execute_query serializes datetime, Decimal, bytes to JSON-safe types."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.isna = pd.isna
        mock_pd.read_sql.return_value = _make_df(
            ["ts", "amount", "data"],
            [(datetime(2025, 1, 1, 12, 0), Decimal("99.99"), b"hello")],
        )

        result = client.execute_query("SELECT ts, amount, data FROM t")

        assert result[0]["ts"] == "2025-01-01T12:00:00"
        assert result[0]["amount"] == 99.99
        assert result[0]["data"] == "hello"

    # ---- get_table_metadata ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_table_metadata_success(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_table_metadata returns full metadata dict."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        # Columns query
        columns_df = _make_df(
            [
                "ColumnName", "ColumnType", "ColumnLength",
                "DecimalTotalDigits", "DecimalFractionalDigits",
                "Nullable", "DefaultValue", "CommentString",
                "ColumnId", "ColumnFormat", "UpperCaseFlag", "IdColType",
            ],
            [
                ("id       ", "I ", 4, 10, 0, "N", None, "Primary key", 1, "-(10)9", "N", None),
                ("name     ", "CV", 100, None, None, "Y", None, None, 2, "X(100)", "N", None),
            ],
        )

        # Primary key query
        pk_df = _make_df(["ColumnName"], [("id       ",)])

        # Index query
        idx_df = _make_df(
            ["IndexName", "IndexType", "UniqueFlag", "ColumnName"],
            [("idx_name", "S", "N", "name     ")],
        )

        # Table info query
        table_df = _make_df(
            [
                "TableKind", "CreatorName", "CreateTimeStamp",
                "LastAlterTimeStamp", "CommentString",
                "ProtectionType", "JournalFlag",
            ],
            [("T ", "DBA ", "2025-01-01 00:00:00", "2025-06-01 00:00:00", "Customer table", "N", "N")],
        )

        mock_pd.read_sql.side_effect = [columns_df, pk_df, idx_df, table_df]

        result = client.get_table_metadata("test_db", "customers")

        assert result["database"] == "test_db"
        assert result["table"] == "customers"
        assert result["column_count"] == 2
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "id"
        assert result["columns"][0]["type"] == "I"
        assert result["columns"][0]["nullable"] is False
        assert result["columns"][0]["description"] == "Primary key"
        assert result["columns"][1]["name"] == "name"
        assert result["columns"][1]["nullable"] is True
        assert result["primary_keys"] == ["id"]
        assert len(result["indexes"]) == 1
        assert result["indexes"][0]["name"] == "idx_name"
        assert result["table_type"] == "T"
        assert result["creator"] == "DBA"
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_table_metadata_not_found(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_table_metadata raises when table not found (empty columns)."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(
            [
                "ColumnName", "ColumnType", "ColumnLength",
                "DecimalTotalDigits", "DecimalFractionalDigits",
                "Nullable", "DefaultValue", "CommentString",
                "ColumnId", "ColumnFormat", "UpperCaseFlag", "IdColType",
            ],
            [],
        )

        with pytest.raises(TeradataQueryError, match="not found or no access"):
            client.get_table_metadata("test_db", "nonexistent")

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_table_metadata_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_table_metadata wraps errors in TeradataQueryError."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("Permission denied")

        with pytest.raises(TeradataQueryError, match="Metadata extraction failed"):
            client.get_table_metadata("test_db", "customers")

    # ---- profile_table ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_profile_table_numeric_column(self, mock_tdsql, mock_pd, client, mock_conn):
        """profile_table returns min/max/avg/std_dev for numeric columns."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        # Column list query
        col_df = _make_df(
            ["ColumnName", "ColumnType"],
            [("amount  ", "D ")],
        )

        # Stats for numeric column
        stat_df = _make_df(
            ["min_value", "max_value", "avg_value", "std_dev", "distinct_count", "non_null_count"],
            [(Decimal("1.00"), Decimal("999.99"), Decimal("250.50"), Decimal("100.25"), 500, 1000)],
        )

        mock_pd.read_sql.side_effect = [col_df, stat_df]

        result = client.profile_table("test_db", "orders")

        assert result["database"] == "test_db"
        assert result["table"] == "orders"
        assert result["sample_size"] is None
        assert len(result["column_profiles"]) == 1
        profile = result["column_profiles"][0]
        assert profile["column_name"] == "amount"
        assert profile["data_type"] == "D"
        assert profile["min"] == pytest.approx(1.0)
        assert profile["max"] == pytest.approx(999.99)
        assert profile["distinct_count"] == 500
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_profile_table_string_column(self, mock_tdsql, mock_pd, client, mock_conn):
        """profile_table returns length stats for string columns."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        col_df = _make_df(["ColumnName", "ColumnType"], [("name    ", "CV")])
        stat_df = _make_df(
            ["min_length", "max_length", "avg_length", "distinct_count", "non_null_count"],
            [(3, 50, 15.5, 900, 1000)],
        )
        mock_pd.read_sql.side_effect = [col_df, stat_df]

        result = client.profile_table("test_db", "customers")

        profile = result["column_profiles"][0]
        assert profile["min_length"] == 3
        assert profile["max_length"] == 50
        assert profile["avg_length"] == pytest.approx(15.5)

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_profile_table_date_column(self, mock_tdsql, mock_pd, client, mock_conn):
        """profile_table returns min/max for date columns."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        col_df = _make_df(["ColumnName", "ColumnType"], [("created ", "DA")])
        stat_df = _make_df(
            ["min_value", "max_value", "distinct_count", "non_null_count"],
            [("2020-01-01", "2025-12-31", 365, 1000)],
        )
        mock_pd.read_sql.side_effect = [col_df, stat_df]

        result = client.profile_table("test_db", "events")

        profile = result["column_profiles"][0]
        assert profile["min"] == "2020-01-01"
        assert profile["max"] == "2025-12-31"

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_profile_table_with_sample_size(self, mock_tdsql, mock_pd, client, mock_conn):
        """profile_table passes sample_size into the query."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        col_df = _make_df(["ColumnName", "ColumnType"], [("id      ", "I ")])
        stat_df = _make_df(
            ["min_value", "max_value", "avg_value", "std_dev", "distinct_count", "non_null_count"],
            [(1, 100, 50.0, 25.0, 100, 100)],
        )
        mock_pd.read_sql.side_effect = [col_df, stat_df]

        result = client.profile_table("test_db", "t", sample_size=1000)

        assert result["sample_size"] == 1000
        # Verify SAMPLE clause in the stats query
        stats_query = mock_pd.read_sql.call_args_list[1][0][0]
        assert "SAMPLE 1000" in stats_query

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_profile_table_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """profile_table raises TeradataQueryError on failure."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("No access")

        with pytest.raises(TeradataQueryError, match="Profiling failed"):
            client.profile_table("test_db", "t")

    # ---- preview_data ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_preview_data_top(self, mock_tdsql, mock_pd, client, mock_conn):
        """preview_data uses TOP N by default."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.isna = pd.isna
        mock_pd.read_sql.return_value = _make_df(
            ["id", "name"], [(1, "Alice"), (2, "Bob")]
        )

        result = client.preview_data("test_db", "customers", limit=2)

        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "Alice"}
        query = mock_pd.read_sql.call_args[0][0]
        assert "TOP 2" in query
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_preview_data_sample(self, mock_tdsql, mock_pd, client, mock_conn):
        """preview_data uses SAMPLE when sample=True."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(
            ["id", "name"], [(5, "Eve"), (3, "Charlie")]
        )

        result = client.preview_data("test_db", "customers", limit=10, sample=True)

        assert len(result) == 2
        query = mock_pd.read_sql.call_args[0][0]
        assert "SAMPLE 10" in query

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_preview_data_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """preview_data raises TeradataQueryError on failure."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("Table not found")

        with pytest.raises(TeradataQueryError, match="Preview failed"):
            client.preview_data("test_db", "missing_table")

    # ---- get_column_statistics ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_column_statistics_all_columns(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_column_statistics returns stats for all columns by default."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        # First call: column list
        col_df = _make_df(
            ["ColumnName", "ColumnType"],
            [("id      ", "I "), ("name    ", "CV")],
        )
        # Second call: stats for id
        id_stats_df = _make_df(
            [
                "column_name", "total_count", "non_null_count", "null_count",
                "null_percentage", "distinct_count", "cardinality_percentage",
            ],
            [("id", 1000, 1000, 0, Decimal("0.00"), 1000, Decimal("100.00"))],
        )
        # Third call: stats for name
        name_stats_df = _make_df(
            [
                "column_name", "total_count", "non_null_count", "null_count",
                "null_percentage", "distinct_count", "cardinality_percentage",
            ],
            [("name", 1000, 950, 50, Decimal("5.00"), 800, Decimal("80.00"))],
        )

        mock_pd.read_sql.side_effect = [col_df, id_stats_df, name_stats_df]

        result = client.get_column_statistics("test_db", "customers")

        assert len(result) == 2
        assert result[0]["column_name"] == "id"
        assert result[0]["total_rows"] == 1000
        assert result[0]["null_count"] == 0
        assert result[1]["column_name"] == "name"
        assert result[1]["null_percentage"] == pytest.approx(5.0)
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_column_statistics_single_column(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_column_statistics with column_name only analyzes that column."""
        mock_tdsql.connect.return_value = mock_conn

        stat_df = _make_df(
            [
                "column_name", "total_count", "non_null_count", "null_count",
                "null_percentage", "distinct_count", "cardinality_percentage",
            ],
            [("id", 500, 500, 0, Decimal("0.00"), 500, Decimal("100.00"))],
        )
        mock_pd.read_sql.side_effect = [stat_df]

        result = client.get_column_statistics("test_db", "customers", column_name="id")

        assert len(result) == 1
        assert result[0]["column_name"] == "id"
        # No column list query should have been made
        assert mock_pd.read_sql.call_count == 1

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_column_statistics_with_sample(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_column_statistics includes SAMPLE clause when sample_size given."""
        mock_tdsql.connect.return_value = mock_conn

        stat_df = _make_df(
            [
                "column_name", "total_count", "non_null_count", "null_count",
                "null_percentage", "distinct_count", "cardinality_percentage",
            ],
            [("id", 100, 100, 0, Decimal("0.00"), 100, Decimal("100.00"))],
        )
        mock_pd.read_sql.side_effect = [stat_df]

        client.get_column_statistics("test_db", "t", column_name="id", sample_size=500)

        stats_query = mock_pd.read_sql.call_args_list[0][0][0]
        assert "SAMPLE 500" in stats_query

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_column_statistics_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_column_statistics raises TeradataQueryError on outer failure."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("Timeout")

        with pytest.raises(TeradataQueryError, match="Column statistics failed"):
            client.get_column_statistics("test_db", "t")

    # ---- estimate_table_size ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_estimate_table_size_success(self, mock_tdsql, mock_pd, client, mock_conn):
        """estimate_table_size returns size and row count."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        size_df = _make_df(
            ["SizeMB", "SizeGB"],
            [(Decimal("512.50"), Decimal("0.50"))],
        )
        count_df = _make_df(["row_count"], [(1000000,)])

        mock_pd.read_sql.side_effect = [size_df, count_df]

        result = client.estimate_table_size("test_db", "big_table")

        assert result["size_mb"] == pytest.approx(512.5)
        assert result["size_gb"] == pytest.approx(0.5)
        assert result["row_count"] == 1000000
        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_estimate_table_size_returns_defaults_on_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """estimate_table_size returns zeros and error key on failure."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("No access")

        result = client.estimate_table_size("test_db", "t")

        assert result["size_mb"] == 0.0
        assert result["size_gb"] == 0.0
        assert "error" in result

    # ---- search_metadata ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_search_metadata_table(self, mock_tdsql, mock_pd, client, mock_conn):
        """search_metadata with search_type='table' returns table results."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        df = _make_df(
            ["database_name", "table_name", "table_kind", "description", "created_at"],
            [("test_db ", "customers ", "T ", "Customer table", "2025-01-01 00:00:00")],
        )
        mock_pd.read_sql.return_value = df

        result = client.search_metadata("customer", search_type="table")

        assert len(result) == 1
        assert result[0]["type"] == "table"
        assert result[0]["table"] == "customers"
        # Verify search term was wrapped with %
        params = mock_pd.read_sql.call_args[1]["params"]
        assert params[0] == "%customer%"
        # Verify TableKind filter is present to exclude views
        sql = mock_pd.read_sql.call_args[0][0]
        assert "TableKind" in sql
        assert "'T'" in sql

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_search_metadata_column(self, mock_tdsql, mock_pd, client, mock_conn):
        """search_metadata with search_type='column' returns column results."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        df = _make_df(
            ["database_name", "table_name", "column_name", "column_type", "description"],
            [("test_db ", "orders ", "customer_id ", "I ", None)],
        )
        mock_pd.read_sql.return_value = df

        result = client.search_metadata("customer", search_type="column")

        assert len(result) == 1
        assert result[0]["type"] == "column"
        assert result[0]["column"] == "customer_id"

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_search_metadata_with_database_filter(self, mock_tdsql, mock_pd, client, mock_conn):
        """search_metadata passes database_name filter."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna
        mock_pd.read_sql.return_value = _make_df(
            ["database_name", "table_name", "table_kind", "description", "created_at"],
            [],
        )

        client.search_metadata("cust", search_type="table", database_name="prod_db")

        params = mock_pd.read_sql.call_args[1]["params"]
        assert params == ("%cust%", "prod_db")
        # Verify TableKind filter is present to exclude views
        sql = mock_pd.read_sql.call_args[0][0]
        assert "TableKind" in sql
        assert "'T'" in sql

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_search_metadata_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """search_metadata raises TeradataQueryError on failure."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("Search failed")

        with pytest.raises(TeradataQueryError, match="Search failed"):
            client.search_metadata("test")

    # ---- get_table_lineage ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_table_lineage_no_query_log(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_table_lineage returns empty lineage when query log unavailable."""
        mock_tdsql.connect.return_value = mock_conn
        # Query log access check fails
        mock_pd.read_sql.side_effect = Exception("No access to DBQLObjTbl")

        result = client.get_table_lineage("test_db", "orders")

        assert result["query_log_available"] is False
        assert result["upstream"] == []
        assert result["downstream"] == []

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_table_lineage_with_results(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_table_lineage returns upstream/downstream tables."""
        mock_tdsql.connect.return_value = mock_conn

        # Query log access check succeeds
        log_check_df = _make_df(["cnt"], [(0,)])
        # Upstream query
        upstream_df = _make_df(
            ["database_name", "table_name", "query_count"],
            [("test_db ", "customers ", 50)],
        )
        # Downstream query
        downstream_df = _make_df(
            ["database_name", "table_name", "query_count"],
            [("test_db ", "report_sales ", 10)],
        )

        mock_pd.read_sql.side_effect = [log_check_df, upstream_df, downstream_df]

        result = client.get_table_lineage("test_db", "orders", direction="both")

        assert result["query_log_available"] is True
        assert len(result["upstream"]) == 1
        assert result["upstream"][0]["table"] == "customers"
        assert len(result["downstream"]) == 1
        assert result["downstream"][0]["table"] == "report_sales"

    # ---- detect_schema_changes ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_detect_schema_changes_no_change(self, mock_tdsql, mock_pd, client, mock_conn):
        """detect_schema_changes reports no changes when schemas match."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.notna = pd.notna

        baseline = {
            "columns": [
                {"name": "id", "type": "I", "nullable": False, "length": 4},
            ],
            "primary_keys": ["id"],
            "indexes": [],
        }

        # Mock get_table_metadata to return same structure
        with patch.object(client, "get_table_metadata", return_value={
            "columns": [
                {"name": "id", "type": "I", "nullable": False, "length": 4},
            ],
            "primary_keys": ["id"],
            "indexes": [],
        }):
            result = client.detect_schema_changes("test_db", "t", baseline)

        assert result["schema_changed"] is False
        assert result["columns_added"] == []
        assert result["columns_removed"] == []
        assert result["columns_modified"] == []

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_detect_schema_changes_column_added(self, mock_tdsql, mock_pd, client, mock_conn):
        """detect_schema_changes detects added columns."""
        baseline = {
            "columns": [{"name": "id", "type": "I", "nullable": False}],
            "primary_keys": ["id"],
            "indexes": [],
        }

        with patch.object(client, "get_table_metadata", return_value={
            "columns": [
                {"name": "id", "type": "I", "nullable": False},
                {"name": "email", "type": "CV", "nullable": True},
            ],
            "primary_keys": ["id"],
            "indexes": [],
        }):
            result = client.detect_schema_changes("test_db", "t", baseline)

        assert result["schema_changed"] is True
        assert "email" in result["columns_added"]

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_detect_schema_changes_column_removed(self, mock_tdsql, mock_pd, client, mock_conn):
        """detect_schema_changes detects removed columns."""
        baseline = {
            "columns": [
                {"name": "id", "type": "I", "nullable": False},
                {"name": "old_col", "type": "CV", "nullable": True},
            ],
            "primary_keys": ["id"],
            "indexes": [],
        }

        with patch.object(client, "get_table_metadata", return_value={
            "columns": [{"name": "id", "type": "I", "nullable": False}],
            "primary_keys": ["id"],
            "indexes": [],
        }):
            result = client.detect_schema_changes("test_db", "t", baseline)

        assert result["schema_changed"] is True
        assert "old_col" in result["columns_removed"]

    # ---- get_amp_count ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_amp_count_success(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_amp_count returns the AMP count."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(["amp_count"], [(64,)])

        result = client.get_amp_count()

        assert result == 64

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_amp_count_defaults_to_2_on_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """get_amp_count returns 2 as fallback on failure."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("Query failed")

        result = client.get_amp_count()

        assert result == 2

    # ---- close ----

    def test_close(self, client):
        """close sets _connected to False."""
        client._connected = True

        client.close()

        assert client._connected is False

    # ---- context manager ----

    def test_context_manager_enter(self, client):
        """__enter__ returns self."""
        result = client.__enter__()
        assert result is client

    def test_context_manager_exit(self, client):
        """__exit__ calls close."""
        client._connected = True
        client.__exit__(None, None, None)
        assert client._connected is False

    def test_context_manager_exit_on_error(self, client):
        """__exit__ still closes even on exception."""
        client._connected = True
        try:
            with client:
                raise ValueError("boom")
        except ValueError:
            pass
        assert client._connected is False

    # ---- connection cleanup per method ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_connection_closed_after_list_databases(self, mock_tdsql, mock_pd, client, mock_conn):
        """Each method creates and closes its own connection."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(["DataBaseName"], [("db1",)])

        client.list_databases()

        mock_conn.close.assert_called_once()

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_connection_closed_after_execute_query_error(self, mock_tdsql, mock_pd, client, mock_conn):
        """Connection is closed even when query raises an error."""
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.side_effect = Exception("Fail")

        with pytest.raises(TeradataQueryError):
            client.execute_query("BAD SQL")

        mock_conn.close.assert_called_once()

    # ---- engine (legacy) property ----

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_engine_property_returns_connection(self, mock_tdsql, client, mock_conn):
        """engine property creates a new connection (backward compat)."""
        mock_tdsql.connect.return_value = mock_conn

        result = client.engine

        assert result is mock_conn


# ---------------------------------------------------------------------------
# TestQuoteIdentifier
# ---------------------------------------------------------------------------

class TestQuoteIdentifier:
    """Tests for _quote_identifier SQL injection prevention."""

    def test_valid_simple_name(self):
        assert _quote_identifier("my_table") == '"my_table"'

    def test_valid_with_dollar_and_hash(self):
        assert _quote_identifier("TD$temp#1") == '"TD$temp#1"'

    def test_valid_with_leading_underscore(self):
        assert _quote_identifier("_staging") == '"_staging"'

    def test_strips_trailing_whitespace(self):
        assert _quote_identifier("my_table  ") == '"my_table"'

    def test_rejects_leading_whitespace(self):
        with pytest.raises(ValueError, match="Invalid Teradata identifier"):
            _quote_identifier("  my_table  ")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError, match="Empty or all-whitespace identifier"):
            _quote_identifier("")

    def test_rejects_whitespace_only(self):
        with pytest.raises(ValueError, match="Empty or all-whitespace identifier"):
            _quote_identifier("   ")

    def test_rejects_injection_with_semicolon(self):
        with pytest.raises(ValueError, match="Invalid Teradata identifier"):
            _quote_identifier("users; DROP TABLE x--")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError, match="Invalid Teradata identifier"):
            _quote_identifier("my table")

    def test_rejects_double_quotes(self):
        with pytest.raises(ValueError, match="Invalid Teradata identifier"):
            _quote_identifier('my"table')

    def test_rejects_129_char_name(self):
        with pytest.raises(ValueError, match="Invalid Teradata identifier"):
            _quote_identifier("a" * 129)

    def test_accepts_128_char_name(self):
        name = "a" * 128
        assert _quote_identifier(name) == f'"{name}"'


# ---------------------------------------------------------------------------
# TestSafeInt
# ---------------------------------------------------------------------------

class TestSafeInt:
    """Tests for _safe_int numeric validation."""

    def test_valid_value(self):
        assert _safe_int(100, "limit") == 100

    def test_rejects_zero_with_default_minimum(self):
        with pytest.raises(ValueError, match="must be >= 1"):
            _safe_int(0, "limit")

    def test_rejects_none(self):
        with pytest.raises(ValueError, match="must not be None"):
            _safe_int(None, "limit")

    def test_rejects_negative(self):
        with pytest.raises(ValueError, match="must be >= 1"):
            _safe_int(-5, "limit")

    def test_custom_minimum(self):
        assert _safe_int(0, "offset", minimum=0) == 0

    def test_casts_string_int(self):
        assert _safe_int("42", "limit") == 42


# ---------------------------------------------------------------------------
# TestSerializeValue
# ---------------------------------------------------------------------------

class TestSerializeValue:
    """Tests for _serialize_value JSON serialization helper."""

    def test_datetime_to_isoformat(self):
        assert _serialize_value(datetime(2025, 1, 1, 12, 0)) == "2025-01-01T12:00:00"

    def test_date_to_isoformat(self):
        assert _serialize_value(date(2025, 6, 15)) == "2025-06-15"

    def test_decimal_to_float(self):
        assert _serialize_value(Decimal("99.99")) == 99.99

    def test_bytes_to_string(self):
        assert _serialize_value(b"hello") == "hello"

    def test_none_passthrough(self):
        assert _serialize_value(None) is None

    def test_int_passthrough(self):
        assert _serialize_value(42) == 42

    def test_string_passthrough(self):
        assert _serialize_value("hello") == "hello"

    def test_bool_passthrough(self):
        assert _serialize_value(True) is True

    def test_unknown_type_to_str(self):
        """Non-standard types are converted via str()."""
        result = _serialize_value(complex(1, 2))
        assert result == "(1+2j)"


# ---------------------------------------------------------------------------
# TestSQLInjectionPrevention — integration-level injection tests
# ---------------------------------------------------------------------------

class TestSQLInjectionPrevention:
    """Smoke tests ensuring SQL injection attempts raise ValueError via TeradataQueryError."""

    @pytest.fixture
    def client(self):
        with (
            patch("teradata_etl_mcp_server.clients.teradata_client._check_teradatasql"),
            patch("teradata_etl_mcp_server.clients.teradata_client._check_pandas"),
        ):
            return TeradataClient(auth=TeradataAuth(
                host="test-host",
                port=1025,
                database="test_db",
                mechanism="TD2",
                username="test_user",
                password="test_password",
            ))

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_profile_table_rejects_injection_in_database(
        self, mock_tdsql, mock_pd, client
    ):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(
            ["ColumnName", "ColumnType"], [("id ", "I ")]
        )

        with pytest.raises(TeradataQueryError, match="Invalid Teradata identifier"):
            client.profile_table(
                database_name="db; DROP TABLE x--",
                table_name="valid_table",
            )

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_preview_data_rejects_injection_in_table(
        self, mock_tdsql, mock_pd, client
    ):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn

        with pytest.raises(TeradataQueryError, match="Invalid Teradata identifier"):
            client.preview_data(
                database_name="valid_db",
                table_name="t UNION SELECT 1--",
            )

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_get_column_statistics_rejects_injection(
        self, mock_tdsql, mock_pd, client
    ):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn

        with pytest.raises(TeradataQueryError, match="Invalid Teradata identifier"):
            client.get_column_statistics(
                database_name="bad name",
                table_name="valid_table",
            )

    @patch("teradata_etl_mcp_server.clients.teradata_client.pd")
    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_profile_table_rejects_sample_size_zero(
        self, mock_tdsql, mock_pd, client
    ):
        """sample_size=0 must raise, not silently behave as 'no sample'."""
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn
        mock_pd.read_sql.return_value = _make_df(
            ["ColumnName", "ColumnType"], [("id ", "I ")]
        )

        with pytest.raises(TeradataQueryError, match="must be >= 1"):
            client.profile_table(
                database_name="valid_db",
                table_name="valid_table",
                sample_size=0,
            )


class TestExtractTeradataErrorCode:
    def test_extracts_code_from_standard_format(self):
        assert TeradataClient._extract_teradata_error_code("[Error 3807] Object does not exist") == 3807

    def test_extracts_code_from_multiline(self):
        msg = "some prefix\n[Error 2652] table is being loaded\nmore text"
        assert TeradataClient._extract_teradata_error_code(msg) == 2652

    def test_returns_none_for_no_match(self):
        assert TeradataClient._extract_teradata_error_code("no error code here") is None

    def test_returns_none_for_empty_string(self):
        assert TeradataClient._extract_teradata_error_code("") is None


class TestExecuteStatements:

    @pytest.fixture
    def client(self):
        with (
            patch("teradata_etl_mcp_server.clients.teradata_client._check_teradatasql"),
            patch("teradata_etl_mcp_server.clients.teradata_client._check_pandas"),
        ):
            return TeradataClient(auth=TeradataAuth(
                host="testhost", port=1025, database="testdb",
                mechanism="TD2", username="user", password="pass",
            ))

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_single_ddl_statement(self, mock_tdsql, client):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.description = None
        mock_cursor.rowcount = -1

        result = client.execute_statements(["CREATE TABLE t (id INT)"])

        assert result["success"] is True
        assert result["returncode"] == 0
        assert result["statement_count"] == 1
        assert len(result["results"]) == 1
        assert result["results"][0]["type"] == "ok"

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_multiple_ddl_statements(self, mock_tdsql, client):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.description = None
        mock_cursor.rowcount = -1

        stmts = ["CREATE TABLE a (id INT)", "CREATE TABLE b (id INT)"]
        result = client.execute_statements(stmts)

        assert result["success"] is True
        assert result["statement_count"] == 2
        assert len(result["results"]) == 2
        assert mock_cursor.execute.call_count == 2

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_select_returns_structured_data(self, mock_tdsql, client):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.description = [("col1", None), ("col2", None)]
        mock_cursor.fetchall.return_value = [("a", 1), ("b", 2)]

        result = client.execute_statements(["SELECT col1, col2 FROM t"])

        assert result["success"] is True
        assert len(result["results"]) == 1
        rs = result["results"][0]
        assert rs["type"] == "result_set"
        assert rs["columns"] == ["col1", "col2"]
        assert rs["row_count"] == 2
        assert rs["rows"][0] == {"col1": "a", "col2": 1}
        assert rs["rows"][1] == {"col1": "b", "col2": 2}

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_mixed_ddl_and_select(self, mock_tdsql, client):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value

        call_count = [0]
        descriptions = [None, [("cnt", None)]]
        fetchall_result = [(42,)]

        def fake_execute(stmt):
            mock_cursor.description = descriptions[call_count[0]]
            if descriptions[call_count[0]]:
                mock_cursor.fetchall.return_value = fetchall_result
            mock_cursor.rowcount = -1
            call_count[0] += 1

        mock_cursor.execute.side_effect = fake_execute

        result = client.execute_statements([
            "CREATE TABLE t (id INT)",
            "SELECT COUNT(*) AS cnt FROM t",
        ])

        assert result["success"] is True
        assert result["results"][0]["type"] == "ok"
        assert result["results"][1]["type"] == "result_set"
        assert result["results"][1]["rows"][0] == {"cnt": 42}

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_error_list_tolerates_known_code(self, mock_tdsql, client):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value

        mock_cursor.execute.side_effect = Exception("[Error 3803] Table already exists")

        result = client.execute_statements(
            ["CREATE TABLE t (id INT)"],
            error_list=[3803],
        )

        assert result["success"] is True
        assert len(result["tolerated_errors"]) == 1
        assert result["tolerated_errors"][0]["error_code"] == 3803

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_error_list_does_not_tolerate_unknown_code(self, mock_tdsql, client):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value

        mock_cursor.execute.side_effect = Exception("[Error 3807] Object does not exist")

        result = client.execute_statements(
            ["DROP TABLE nonexistent"],
            error_list=[3803],
        )

        assert result["success"] is False
        assert result["returncode"] == 3807

    @patch("teradata_etl_mcp_server.clients.teradata_client.teradatasql")
    def test_error_without_error_list_fails(self, mock_tdsql, client):
        mock_conn = MagicMock()
        mock_tdsql.connect.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value

        mock_cursor.execute.side_effect = Exception("[Error 3807] Object does not exist")

        result = client.execute_statements(["DROP TABLE nonexistent"])

        assert result["success"] is False
        assert result["returncode"] == 3807
        assert "3807" in result["stderr"]

    def test_empty_statement_list_raises(self, client):
        with pytest.raises(ValueError, match="non-empty"):
            client.execute_statements([])
