"""Functional tests for the metadata discovery router tools (teradata_discover, teradata_analyze)."""

import unittest.mock
from unittest.mock import MagicMock

import pytest

from elt_mcp_server.tools.metadata_discovery import register_metadata_tools


@pytest.fixture
def mock_orchestrator():
    """Create mock orchestrator with teradata_client."""
    orchestrator = MagicMock()

    # Mock teradata_client methods
    orchestrator.teradata_client = MagicMock()
    orchestrator.teradata_client.get_table_metadata = MagicMock(
        return_value={
            "table_name": "customers",
            "database": "test_db",
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR(100)", "nullable": True},
            ],
            "row_count": 1000,
        }
    )
    orchestrator.teradata_client.list_tables = MagicMock(
        return_value=[
            {"table": "customers", "type": "T"},
            {"table": "orders", "type": "T"},
            {"table": "products", "type": "T"},
        ]
    )
    orchestrator.teradata_client.estimate_table_size = MagicMock(
        return_value={
            "size_mb": 150.5,
            "row_count": 10000,
            "avg_row_size_bytes": 158,
        }
    )
    orchestrator.teradata_client.get_column_statistics = MagicMock(
        return_value={
            "column_name": "email",
            "null_percentage": 5.2,
            "distinct_count": 9500,
            "cardinality": 0.95,
        }
    )
    orchestrator.teradata_client.get_table_lineage = MagicMock(
        return_value={
            "upstream": [
                {"database": "test_db", "table": "source_table1", "query_count": 5},
                {"database": "test_db", "table": "source_table2", "query_count": 3},
            ],
            "downstream": [
                {"database": "test_db", "table": "target_table1", "query_count": 2},
            ],
            "query_log_available": True,
        }
    )
    orchestrator.teradata_client.search_metadata = MagicMock(
        return_value=[
            {
                "table": "customer_data",
                "database": "prod",
                "type": "T",
                "table_type": "TABLE",
                "description": None,
                "created_at": "2025-01-01",
            },
            {
                "table": "customer_profile",
                "database": "staging",
                "type": "T",
                "table_type": "TABLE",
                "description": None,
                "created_at": "2025-01-02",
            },
        ]
    )
    orchestrator.profile_source_table = MagicMock(
        return_value={
            "columns": {
                "id": {"min": 1, "max": 10000, "avg": 5000.5, "distinct": 10000},
                "amount": {"min": 0.0, "max": 9999.99, "avg": 150.25, "distinct": 8500},
            }
        }
    )
    orchestrator.teradata_client.profile_table = MagicMock(
        return_value={
            "database": "test_db",
            "table_name": "customers",
            "profile": {
                "id": {"min": 1, "max": 10000, "avg": 5000.5, "distinct": 10000},
                "amount": {"min": 0.0, "max": 9999.99, "avg": 150.25, "distinct": 8500},
            },
        }
    )
    orchestrator.teradata_client.preview_data = MagicMock(
        return_value=[
            {"id": 1, "name": "John Doe", "email": "john@example.com"},
            {"id": 2, "name": "Jane Smith", "email": "jane@example.com"},
            {"id": 3, "name": "Bob Johnson", "email": "bob@example.com"},
        ]
    )
    orchestrator.teradata_client.detect_schema_changes = MagicMock(
        return_value={
            "has_changes": True,
            "added_columns": ["new_column"],
            "removed_columns": [],
            "modified_columns": ["updated_column"],
        }
    )
    orchestrator.teradata_client.check_database_exists = MagicMock(return_value=True)

    return orchestrator


class TestRequiredMetadataTools:
    """Test the 2 required router tools are registered."""

    @pytest.mark.asyncio
    async def test_all_required_tools_registered(self, mock_orchestrator):
        """Verify the two router tools are registered."""
        tools = register_metadata_tools(mock_orchestrator)

        assert "teradata_discover" in tools, "Router tool 'teradata_discover' not registered"
        assert "teradata_analyze" in tools, "Router tool 'teradata_analyze' not registered"
        assert callable(tools["teradata_discover"])
        assert callable(tools["teradata_analyze"])
        assert len(tools) == 2

    @pytest.mark.asyncio
    async def test_teradata_discover_invalid_action(self, mock_orchestrator):
        """Unknown action returns error."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](action="invalid_action", database="test_db")
        assert result["success"] is False
        assert "Unknown action" in result["error"]

    @pytest.mark.asyncio
    async def test_teradata_analyze_invalid_type(self, mock_orchestrator):
        """Unknown analysis_type returns error."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="invalid_type", database="test_db", table_name="t"
        )
        assert result["success"] is False
        assert "Unknown analysis_type" in result["error"]


class TestTeradataDiscover:
    """Tests for the teradata_discover router tool."""

    @pytest.mark.asyncio
    async def test_discover_tables(self, mock_orchestrator):
        """Test discover_tables action."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](action="discover_tables", database="test_db")
        assert "tables" in result or "table_count" in result

    @pytest.mark.asyncio
    async def test_enumerate_tables(self, mock_orchestrator):
        """Test enumerate_tables action (lightweight list)."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](action="enumerate_tables", database="test_db")
        assert "tables" in result

    @pytest.mark.asyncio
    async def test_search_metadata(self, mock_orchestrator):
        """Test search_metadata action."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](
            action="search_metadata",
            database="test_db",
            search_term="customer%",
        )
        assert "tables" in result
        assert "total_matches" in result
        assert result["total_matches"] >= 2

    @pytest.mark.asyncio
    async def test_search_metadata_missing_term(self, mock_orchestrator):
        """search_metadata without search_term returns error."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](action="search_metadata", database="test_db")
        assert result["success"] is False
        assert "search_term" in result["error"]

    @pytest.mark.asyncio
    async def test_discover_tables_via_table_name_alias(self, mock_orchestrator):
        """Verify table_name parameter is accepted as alias for table_pattern."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](
            action="discover_tables", database="test_db", table_name="sales_raw_tpt"
        )
        assert "tables" in result or "table_count" in result
        # Verify the alias was forwarded as the search term
        mock_orchestrator.teradata_client.search_metadata.assert_called_once_with(
            search_term="sales_raw_tpt",
            search_type="table",
            database_name="test_db",
        )

    @pytest.mark.asyncio
    async def test_table_name_ignored_when_table_pattern_set(self, mock_orchestrator):
        """table_name does NOT override an explicit table_pattern."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](
            action="discover_tables",
            database="test_db",
            table_pattern="order%",
            table_name="sales_raw_tpt",
        )
        assert "tables" in result or "table_count" in result
        mock_orchestrator.teradata_client.search_metadata.assert_called_once_with(
            search_term="order%",
            search_type="table",
            database_name="test_db",
        )

    @pytest.mark.asyncio
    async def test_table_pattern_whitespace_is_stripped(self, mock_orchestrator):
        """table_pattern with surrounding whitespace is stripped before use."""
        tools = register_metadata_tools(mock_orchestrator)
        await tools["teradata_discover"](
            action="discover_tables",
            database="test_db",
            table_pattern="  order%  ",
        )
        mock_orchestrator.teradata_client.search_metadata.assert_called_once_with(
            search_term="order%",
            search_type="table",
            database_name="test_db",
        )

    @pytest.mark.asyncio
    async def test_table_pattern_non_string_returns_error(self, mock_orchestrator):
        """A non-string table_pattern returns a validation error."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](
            action="discover_tables",
            database="test_db",
            table_pattern=123,  # type: ignore[arg-type]
        )
        assert result["success"] is False
        assert "table_pattern" in result["error"]

    @pytest.mark.asyncio
    async def test_table_name_non_string_returns_error(self, mock_orchestrator):
        """A non-string table_name returns a validation error."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](
            action="discover_tables",
            database="test_db",
            table_name=42,  # type: ignore[arg-type]
        )
        assert result["success"] is False
        assert "table_name" in result["error"]

    @pytest.mark.asyncio
    async def test_discover_tables_nopi_table(self, mock_orchestrator):
        """Regression: discover_tables returns NoPI tables (TableKind='O')."""
        mock_orchestrator.teradata_client.search_metadata = MagicMock(
            return_value=[
                {
                    "table": "sales_raw_tpt",
                    "database": "staging_db",
                    "type": "table",
                    "table_type": "O",
                    "description": None,
                    "created_at": "2025-06-01",
                },
            ]
        )
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](
            action="discover_tables", database="staging_db", table_name="sales_raw_tpt"
        )
        assert result["table_count"] == 1
        assert result["tables"][0]["table_name"] == "sales_raw_tpt"
        assert result["tables"][0]["table_type"] == "O"

    @pytest.mark.asyncio
    async def test_discover_requires_database(self, mock_orchestrator):
        """Non-test_connection actions require database."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](action="discover_tables")
        assert result["success"] is False
        assert "database" in result["error"]

    @pytest.mark.asyncio
    async def test_discover_tables_nonexistent_database(self, mock_orchestrator):
        """discover_tables returns error when database does not exist."""
        mock_orchestrator.teradata_client.check_database_exists = MagicMock(return_value=False)
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](action="discover_tables", database="fake_db")
        assert result["success"] is False
        assert "fake_db" in result["error"]
        assert "does not exist" in result["error"]

    @pytest.mark.asyncio
    async def test_enumerate_tables_nonexistent_database(self, mock_orchestrator):
        """enumerate_tables returns error when database does not exist."""
        mock_orchestrator.teradata_client.check_database_exists = MagicMock(return_value=False)
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](action="enumerate_tables", database="fake_db")
        assert result["success"] is False
        assert "fake_db" in result["error"]

    @pytest.mark.asyncio
    async def test_search_metadata_nonexistent_database(self, mock_orchestrator):
        """search_metadata returns error when database does not exist."""
        mock_orchestrator.teradata_client.check_database_exists = MagicMock(return_value=False)
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](
            action="search_metadata", database="fake_db", search_term="customer%"
        )
        assert result["success"] is False
        assert "fake_db" in result["error"]

    @pytest.mark.asyncio
    async def test_unknown_action_returns_error_without_db_roundtrip(self, mock_orchestrator):
        """Unknown action returns an error before any DB existence check."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](action="invalid_action", database="prod_db")
        assert result["success"] is False
        assert "Unknown action" in result["error"]
        mock_orchestrator.teradata_client.check_database_exists.assert_not_called()

    @pytest.mark.asyncio
    async def test_discover_tables_strips_database_whitespace(self, mock_orchestrator):
        """Trailing/leading whitespace on database is stripped before use."""
        tools = register_metadata_tools(mock_orchestrator)
        await tools["teradata_discover"](action="discover_tables", database="prod_db ")
        mock_orchestrator.teradata_client.check_database_exists.assert_called_with("prod_db")

    @pytest.mark.asyncio
    async def test_test_connection(self, mock_orchestrator):
        """Test test_connection action."""
        mock_orchestrator.teradata_client.test_connection = MagicMock(
            return_value={"connected": True, "version": "17.20.00.08"}
        )
        mock_orchestrator.teradata_client.host = "td-server.example.com"
        mock_orchestrator.teradata_client.database = "prod_db"
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](action="test_connection")
        assert result["success"] is True
        assert result["status"] == "connected"


class TestTeradataAnalyze:
    """Tests for the teradata_analyze router tool."""

    @pytest.mark.asyncio
    async def test_describe_table(self, mock_orchestrator):
        """Test describe_table analysis."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="describe_table",
            database="test_db",
            table_name="customers",
        )
        assert "database" in result
        assert "table_name" in result
        assert result["database"] == "test_db"
        assert result["table_name"] == "customers"

    @pytest.mark.asyncio
    async def test_profile_table(self, mock_orchestrator):
        """Test profile_table analysis."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="profile_table",
            database="test_db",
            table_name="customers",
        )
        assert "database" in result
        assert "table_name" in result
        assert "profile" in result
        mock_orchestrator.teradata_client.profile_table.assert_called()

    @pytest.mark.asyncio
    async def test_estimate_size(self, mock_orchestrator):
        """Test estimate_size analysis."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="estimate_size",
            database="test_db",
            table_name="customers",
        )
        assert "size_mb" in result
        assert result["size_mb"] == 150.5

    @pytest.mark.asyncio
    async def test_analyze_column(self, mock_orchestrator):
        """Test analyze_column analysis."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="analyze_column",
            database="test_db",
            table_name="customers",
            column_name="email",
        )
        assert result["database"] == "test_db"
        assert result["table_name"] == "customers"
        assert "columns" in result

    @pytest.mark.asyncio
    async def test_analyze_dependencies(self, mock_orchestrator):
        """Test analyze_dependencies (lineage) analysis."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="analyze_dependencies",
            database="test_db",
            table_name="customers",
        )
        assert "database" in result
        assert "table_name" in result
        assert "upstream_tables" in result
        assert "downstream_tables" in result
        assert len(result["upstream_tables"]) == 2
        assert len(result["downstream_tables"]) == 1
        mock_orchestrator.teradata_client.get_table_lineage.assert_called()

    @pytest.mark.asyncio
    async def test_preview_data(self, mock_orchestrator):
        """Test preview_data analysis."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="preview_data",
            database="test_db",
            table_name="customers",
            limit=10,
        )
        assert "rows" in result
        assert len(result["rows"]) == 3
        assert "id" in result["rows"][0]
        assert "name" in result["rows"][0]

    @pytest.mark.asyncio
    async def test_compare_structure_with_baseline(self, mock_orchestrator):
        """Test compare_structure with baseline metadata."""
        tools = register_metadata_tools(mock_orchestrator)
        baseline = {"columns": [{"name": "id"}]}
        result = await tools["teradata_analyze"](
            analysis_type="compare_structure",
            database="test_db",
            table_name="customers",
            baseline_metadata=baseline,
        )
        assert result["has_changes"] is True

    @pytest.mark.asyncio
    async def test_compare_structure_no_baseline(self, mock_orchestrator):
        """Test compare_structure without baseline returns current metadata."""
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="compare_structure",
            database="test_db",
            table_name="customers",
            baseline_metadata=None,
        )
        assert "current_metadata" in result
        assert "changes" in result


class TestMetadataToolsErrorHandling:
    """Test error handling for metadata tools."""

    @pytest.mark.asyncio
    async def test_describe_table_error(self, mock_orchestrator):
        """Test describe_table error handling."""
        mock_orchestrator.teradata_client.get_table_metadata = MagicMock(
            side_effect=Exception("Table not found")
        )
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="describe_table",
            database="test_db",
            table_name="missing_table",
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_estimate_size_error(self, mock_orchestrator):
        """Test estimate_size error handling."""
        mock_orchestrator.teradata_client.estimate_table_size = MagicMock(
            side_effect=Exception("Permission denied")
        )
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="estimate_size",
            database="test_db",
            table_name="customers",
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_analyze_column_error(self, mock_orchestrator):
        """Test analyze_column error handling."""
        mock_orchestrator.teradata_client.get_column_statistics = MagicMock(
            side_effect=Exception("Column not found")
        )
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="analyze_column",
            database="test_db",
            table_name="customers",
            column_name="missing_column",
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_analyze_dependencies_error(self, mock_orchestrator):
        """Test analyze_dependencies error handling."""
        mock_orchestrator.teradata_client.get_table_lineage = MagicMock(
            side_effect=Exception("Lineage analysis failed")
        )
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="analyze_dependencies",
            database="test_db",
            table_name="customers",
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_search_metadata_error(self, mock_orchestrator):
        """Test search_metadata error handling."""
        mock_orchestrator.teradata_client.search_metadata = MagicMock(
            side_effect=Exception("Search failed")
        )
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_discover"](
            action="search_metadata",
            database="test_db",
            search_term="customer%",
        )
        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_profile_table_error(self, mock_orchestrator):
        """Test profile_table error handling."""
        mock_orchestrator.teradata_client.profile_table = MagicMock(
            side_effect=Exception("Profiling failed")
        )
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="profile_table",
            database="test_db",
            table_name="customers",
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_preview_data_error(self, mock_orchestrator):
        """Test preview_data error handling."""
        mock_orchestrator.teradata_client.preview_data = MagicMock(
            side_effect=Exception("Query timeout")
        )
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="preview_data",
            database="test_db",
            table_name="huge_table",
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_compare_structure_error(self, mock_orchestrator):
        """Test compare_structure error handling."""
        mock_orchestrator.teradata_client.get_table_metadata = MagicMock(
            side_effect=Exception("Table not found")
        )
        tools = register_metadata_tools(mock_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="compare_structure",
            database="test_db",
            table_name="customers",
        )
        assert "error" in result


class TestTeradataConnectionTool:
    """Tests for the test_connection action of teradata_discover."""

    @pytest.fixture
    def mock_orchestrator_with_settings(self):
        """Create mock orchestrator with settings for connection test."""
        orchestrator = MagicMock()
        orchestrator.teradata_client = MagicMock()
        orchestrator.teradata_client.host = "td-server.example.com"
        orchestrator.teradata_client.database = "prod_db"
        return orchestrator

    @pytest.mark.asyncio
    async def test_teradata_connection_success(self, mock_orchestrator_with_settings):
        """Test successful Teradata connection check."""
        mock_orchestrator_with_settings.teradata_client.test_connection.return_value = {
            "connected": True,
            "version": "17.20.00.08",
        }
        tools = register_metadata_tools(mock_orchestrator_with_settings)
        result = await tools["teradata_discover"](action="test_connection")
        assert result["success"] is True
        assert result["status"] == "connected"
        assert result["host"] == "td-server.example.com"
        assert result["database"] == "prod_db"
        assert result["version"] == "17.20.00.08"

    @pytest.mark.asyncio
    async def test_teradata_connection_failure_exception(self, mock_orchestrator_with_settings):
        """Test Teradata connection when test_connection raises an exception."""
        mock_orchestrator_with_settings.teradata_client.test_connection.side_effect = Exception(
            "Connection refused"
        )
        tools = register_metadata_tools(mock_orchestrator_with_settings)
        result = await tools["teradata_discover"](action="test_connection")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_teradata_connection_connected_false(self, mock_orchestrator_with_settings):
        """Test Teradata connection when test_connection returns connected=False."""
        mock_orchestrator_with_settings.teradata_client.test_connection.return_value = {
            "connected": False,
            "error": "Authentication failed for user dbc",
        }
        tools = register_metadata_tools(mock_orchestrator_with_settings)
        result = await tools["teradata_discover"](action="test_connection")
        assert result["success"] is False
        assert result["status"] == "failed"

    @pytest.mark.asyncio
    async def test_test_connection_with_nonexistent_database(
        self, mock_orchestrator_with_settings
    ):
        """test_connection with a database param that doesn't exist returns error."""
        mock_orchestrator_with_settings.teradata_client.test_connection.return_value = {
            "connected": True,
            "version": "17.20.00.08",
        }
        mock_orchestrator_with_settings.teradata_client.check_database_exists = MagicMock(
            return_value=False
        )
        tools = register_metadata_tools(mock_orchestrator_with_settings)
        result = await tools["teradata_discover"](
            action="test_connection", database="test_fake"
        )
        assert result["success"] is False
        assert result["status"] == "failed"
        assert "test_fake" in result["error"]
        assert "does not exist" in result["error"]

    @pytest.mark.asyncio
    async def test_test_connection_with_existing_database(self, mock_orchestrator_with_settings):
        """test_connection with a valid database param succeeds and reports that database."""
        mock_orchestrator_with_settings.teradata_client.test_connection.return_value = {
            "connected": True,
            "version": "17.20.00.08",
        }
        mock_orchestrator_with_settings.teradata_client.check_database_exists = MagicMock(
            return_value=True
        )
        tools = register_metadata_tools(mock_orchestrator_with_settings)
        result = await tools["teradata_discover"](
            action="test_connection", database="real_db"
        )
        assert result["success"] is True
        assert result["database"] == "real_db"

    @pytest.mark.asyncio
    async def test_test_connection_no_database_uses_profile_default(
        self, mock_orchestrator_with_settings
    ):
        """test_connection without database param reports the client's default database."""
        mock_orchestrator_with_settings.teradata_client.test_connection.return_value = {
            "connected": True,
            "version": "17.20.00.08",
        }
        tools = register_metadata_tools(mock_orchestrator_with_settings)
        result = await tools["teradata_discover"](action="test_connection")
        assert result["success"] is True
        assert result["database"] == "prod_db"


class TestTeradataProfile:
    """Tests for teradata_profile parameter on both router tools."""

    @pytest.fixture
    def profile_orchestrator(self):
        """Create mock orchestrator with credential_resolver for profile tests."""
        from pydantic import SecretStr

        orch = MagicMock()
        orch.teradata_client = MagicMock()
        # Settings must carry a valid TD2 identity for the no-profile path.
        orch.settings.teradata.host = "default-host"
        orch.settings.teradata.port = 1025
        orch.settings.teradata.database = "default_db"
        orch.settings.teradata.username = "default_user"
        orch.settings.teradata.password = SecretStr("default_pass")
        orch.settings.teradata.logmech = "TD2"
        orch.settings.teradata.logdata = SecretStr("")
        orch.settings.teradata.oidc_clientid = ""
        orch.settings.teradata.jws_private_key = ""
        orch.settings.teradata.jws_cert = ""
        orch.settings.teradata.sslca = ""
        orch.credential_resolver = MagicMock()
        orch.credential_resolver.is_configured = False
        orch.credential_resolver.guard_configured.return_value = None
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "profile-host.example.com",
            "username": "profile_user",
            "password": "profile_pass",
            "default_schema": "profile_db",
            "port": "1025",
        }
        return orch

    # ── Profile resolution & client construction ──────────────────

    @pytest.mark.asyncio
    async def test_discover_with_profile_resolves_credentials(self, profile_orchestrator):
        """teradata_discover with teradata_profile calls resolve_profile and constructs client."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "td-profile.example.com",
            "username": "prof_user",
            "password": "prof_pass",
            "database": "prof_db",
            "port": "2025",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_client = MagicMock()
            mock_client.host = "td-profile.example.com"
            mock_client.database = "prof_db"
            mock_client.test_connection.return_value = {
                "connected": True,
                "version": "17.20",
            }
            mock_td.return_value = mock_client
            result = await tools["teradata_discover"](
                action="test_connection", teradata_profile="my_profile"
            )
        profile_orchestrator.credential_resolver.resolve_profile.assert_called_once_with(
            "my_profile"
        )
        mock_td.assert_called_once()
        called_auth = mock_td.call_args[1]["auth"]
        assert called_auth.host == "td-profile.example.com"
        assert called_auth.username == "prof_user"
        assert called_auth.password == "prof_pass"
        assert called_auth.database == "prof_db"
        assert called_auth.port == 2025
        assert called_auth.mechanism == "TD2"
        assert result["success"] is True
        assert result["host"] == "td-profile.example.com"
        assert result["database"] == "prof_db"

    @pytest.mark.asyncio
    async def test_analyze_with_profile_resolves_credentials(self, profile_orchestrator):
        """teradata_analyze with teradata_profile calls resolve_profile and constructs client."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "td-profile.example.com",
            "username": "prof_user",
            "password": "prof_pass",
            "database": "prof_db",
            "port": "1025",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_client = MagicMock()
            mock_client.estimate_table_size.return_value = {"size_mb": 42.0}
            mock_td.return_value = mock_client
            result = await tools["teradata_analyze"](
                analysis_type="estimate_size",
                database="test_db",
                table_name="t",
                teradata_profile="my_profile",
            )
        profile_orchestrator.credential_resolver.resolve_profile.assert_called_once_with(
            "my_profile"
        )
        mock_td.assert_called_once()
        called_auth = mock_td.call_args[1]["auth"]
        assert called_auth.host == "td-profile.example.com"
        assert called_auth.username == "prof_user"
        assert called_auth.password == "prof_pass"
        assert called_auth.database == "prof_db"
        assert called_auth.port == 1025
        assert called_auth.mechanism == "TD2"
        assert result["size_mb"] == 42.0

    # ── Guard failure ─────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_discover_guard_failure_returns_error(self, profile_orchestrator):
        """teradata_discover returns guard error when connections.yaml is misconfigured."""
        profile_orchestrator.credential_resolver.guard_configured.return_value = {
            "success": False,
            "error": "connections.yaml not found",
        }
        tools = register_metadata_tools(profile_orchestrator)
        result = await tools["teradata_discover"](
            action="test_connection", teradata_profile="bad_profile"
        )
        assert result["success"] is False
        assert "connections.yaml" in result["error"]
        profile_orchestrator.credential_resolver.resolve_profile.assert_not_called()

    @pytest.mark.asyncio
    async def test_analyze_guard_failure_returns_error(self, profile_orchestrator):
        """teradata_analyze returns guard error when connections.yaml is misconfigured."""
        profile_orchestrator.credential_resolver.guard_configured.return_value = {
            "success": False,
            "error": "connections.yaml not found",
        }
        tools = register_metadata_tools(profile_orchestrator)
        result = await tools["teradata_analyze"](
            analysis_type="describe_table",
            database="db",
            table_name="t",
            teradata_profile="bad_profile",
        )
        assert result["success"] is False
        assert "connections.yaml" in result["error"]
        profile_orchestrator.credential_resolver.resolve_profile.assert_not_called()

    # ── No profile → default client ──────────────────────────────

    @pytest.mark.asyncio
    async def test_no_profile_uses_default_client(self, profile_orchestrator):
        """Without teradata_profile, the default orchestrator client is used."""
        profile_orchestrator.teradata_client.host = "default-host"
        profile_orchestrator.teradata_client.database = "default_db"
        profile_orchestrator.teradata_client.test_connection.return_value = {
            "connected": True,
            "version": "17.20",
        }
        tools = register_metadata_tools(profile_orchestrator)
        result = await tools["teradata_discover"](action="test_connection")
        assert result["success"] is True
        assert result["host"] == "default-host"
        profile_orchestrator.credential_resolver.resolve_profile.assert_not_called()
        profile_orchestrator.credential_resolver.guard_configured.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_profile_does_not_construct_new_client(self, profile_orchestrator):
        """Without teradata_profile, no new TeradataClient is constructed."""
        profile_orchestrator.teradata_client.estimate_table_size.return_value = {"size_mb": 10.0}
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            await tools["teradata_analyze"](
                analysis_type="estimate_size",
                database="db",
                table_name="t",
            )
        mock_td.assert_not_called()
        profile_orchestrator.teradata_client.estimate_table_size.assert_called_once()

    # ── Database / schema key resolution ──────────────────────────

    @pytest.mark.asyncio
    async def test_profile_database_key_takes_priority(self, profile_orchestrator):
        """When profile has 'database', 'schema', and 'default_schema', 'database' wins."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "h",
            "username": "u",
            "password": "p",
            "database": "db_value",
            "schema": "schema_value",
            "default_schema": "default_schema_value",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_td.return_value = MagicMock(
                estimate_table_size=MagicMock(return_value={"size_mb": 1.0})
            )
            await tools["teradata_analyze"](
                analysis_type="estimate_size",
                database="db",
                table_name="t",
                teradata_profile="p",
            )
        assert mock_td.call_args[1]["auth"].database == "db_value"

    @pytest.mark.asyncio
    async def test_profile_schema_key_fallback(self, profile_orchestrator):
        """When profile has no 'database' but has 'schema', that is used."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "h",
            "username": "u",
            "password": "p",
            "schema": "schema_value",
            "default_schema": "default_schema_value",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_td.return_value = MagicMock(
                estimate_table_size=MagicMock(return_value={"size_mb": 1.0})
            )
            await tools["teradata_analyze"](
                analysis_type="estimate_size",
                database="db",
                table_name="t",
                teradata_profile="p",
            )
        assert mock_td.call_args[1]["auth"].database == "schema_value"

    @pytest.mark.asyncio
    async def test_profile_default_schema_fallback(self, profile_orchestrator):
        """When profile has only 'default_schema', that is used for database."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "h",
            "username": "u",
            "password": "p",
            "default_schema": "my_schema",
            "port": "1025",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_td.return_value = MagicMock(
                estimate_table_size=MagicMock(return_value={"size_mb": 1.0})
            )
            await tools["teradata_analyze"](
                analysis_type="estimate_size",
                database="db",
                table_name="t",
                teradata_profile="p",
            )
        assert mock_td.call_args[1]["auth"].database == "my_schema"

    @pytest.mark.asyncio
    async def test_profile_no_database_keys_defaults_empty(self, profile_orchestrator):
        """When profile has none of database/schema/default_schema, empty string is passed."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "h",
            "username": "u",
            "password": "p",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_td.return_value = MagicMock(
                estimate_table_size=MagicMock(return_value={"size_mb": 1.0})
            )
            await tools["teradata_analyze"](
                analysis_type="estimate_size",
                database="db",
                table_name="t",
                teradata_profile="p",
            )
        assert mock_td.call_args[1]["auth"].database == ""

    # ── Port handling ─────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_profile_custom_port_converted_to_int(self, profile_orchestrator):
        """Profile port string is converted to int for TeradataClient."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "h",
            "username": "u",
            "password": "p",
            "database": "d",
            "port": "9999",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_td.return_value = MagicMock(
                estimate_table_size=MagicMock(return_value={"size_mb": 1.0})
            )
            await tools["teradata_analyze"](
                analysis_type="estimate_size",
                database="db",
                table_name="t",
                teradata_profile="p",
            )
        assert mock_td.call_args[1]["auth"].port == 9999

    @pytest.mark.asyncio
    async def test_profile_missing_port_defaults_to_1025(self, profile_orchestrator):
        """When profile omits port, default 1025 is used."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "h",
            "username": "u",
            "password": "p",
            "database": "d",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_td.return_value = MagicMock(
                estimate_table_size=MagicMock(return_value={"size_mb": 1.0})
            )
            await tools["teradata_analyze"](
                analysis_type="estimate_size",
                database="db",
                table_name="t",
                teradata_profile="p",
            )
        assert mock_td.call_args[1]["auth"].port == 1025

    # ── Profile client isolation ──────────────────────────────────

    @pytest.mark.asyncio
    async def test_profile_client_used_instead_of_default(self, profile_orchestrator):
        """When teradata_profile is given, the default orchestrator client is NOT called."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "h",
            "username": "u",
            "password": "p",
            "database": "d",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_client = MagicMock()
            mock_client.estimate_table_size.return_value = {"size_mb": 5.0}
            mock_td.return_value = mock_client
            result = await tools["teradata_analyze"](
                analysis_type="estimate_size",
                database="db",
                table_name="t",
                teradata_profile="p",
            )
        mock_client.estimate_table_size.assert_called_once()
        profile_orchestrator.teradata_client.estimate_table_size.assert_not_called()
        assert result["size_mb"] == 5.0

    # ── Profile with non-test_connection discover actions ─────────

    @pytest.mark.asyncio
    async def test_discover_tables_with_profile(self, profile_orchestrator):
        """discover_tables action uses profile-based client."""
        profile_orchestrator.credential_resolver.resolve_profile.return_value = {
            "host": "h",
            "username": "u",
            "password": "p",
            "database": "d",
        }
        tools = register_metadata_tools(profile_orchestrator)
        with unittest.mock.patch(
            "elt_mcp_server.tools.metadata_discovery.TeradataClient"
        ) as mock_td:
            mock_client = MagicMock()
            mock_client.search_metadata.return_value = [
                {"table": "t1", "table_type": "TABLE", "description": None, "created_at": None},
            ]
            mock_client.estimate_table_size.return_value = {"size_mb": 10.0, "size_gb": 0.01}
            mock_td.return_value = mock_client
            result = await tools["teradata_discover"](
                action="discover_tables",
                database="test_db",
                teradata_profile="p",
            )
        mock_client.search_metadata.assert_called_once()
        profile_orchestrator.teradata_client.search_metadata.assert_not_called()
        assert result["table_count"] == 1

    # ── resolve_profile exception propagation ─────────────────────

    @pytest.mark.asyncio
    async def test_resolve_profile_exception_returns_error(self, profile_orchestrator):
        """When resolve_profile raises, the error is caught and returned."""
        profile_orchestrator.credential_resolver.resolve_profile.side_effect = KeyError(
            "Profile 'missing' not found in connections.yaml"
        )
        tools = register_metadata_tools(profile_orchestrator)
        result = await tools["teradata_discover"](
            action="test_connection", teradata_profile="missing"
        )
        assert result["success"] is False
        assert "error" in result


class TestTeradataProfileAutoDetection:
    """Tests for automatic Teradata profile detection when no profile is specified."""

    @pytest.fixture
    def profile_orchestrator(self):
        orch = MagicMock()
        orch.teradata_client = MagicMock()
        orch.credential_resolver = MagicMock()
        orch.credential_resolver.guard_configured.return_value = None
        return orch

    @pytest.mark.asyncio
    async def test_no_profile_configured_no_td_profiles_uses_default(self, profile_orchestrator):
        """When configured but no Teradata profiles found, use default client."""
        profile_orchestrator.credential_resolver.is_configured = True
        profile_orchestrator.credential_resolver.find_teradata_profiles.return_value = []
        profile_orchestrator.teradata_client.host = "default-host"
        profile_orchestrator.teradata_client.database = "default_db"
        profile_orchestrator.teradata_client.test_connection.return_value = {
            "connected": True,
            "version": "17.20",
        }

        tools = register_metadata_tools(profile_orchestrator)
        result = await tools["teradata_discover"](action="test_connection")

        assert result["success"] is True
        assert result["host"] == "default-host"
        profile_orchestrator.teradata_client.test_connection.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_profile_not_configured_uses_default(self, profile_orchestrator):
        """When credential_resolver is not configured, use default client without listing profiles."""
        profile_orchestrator.credential_resolver.is_configured = False
        profile_orchestrator.teradata_client.host = "default-host"
        profile_orchestrator.teradata_client.database = "default_db"
        profile_orchestrator.teradata_client.test_connection.return_value = {
            "connected": True,
            "version": "17.20",
        }

        tools = register_metadata_tools(profile_orchestrator)
        result = await tools["teradata_discover"](action="test_connection")

        assert result["success"] is True
        assert result["host"] == "default-host"
        profile_orchestrator.credential_resolver.find_teradata_profiles.assert_not_called()


class TestTeradataProfileSentinelHandling:
    """Sentinel folding for ``_resolve_client``: ``"wizard"``, ``"default"``,
    and whitespace-only strings must take the orchestrator-default path
    and NOT call ``guard_configured()`` (which would require
    connections.yaml even for wizard-default usage)."""

    @pytest.fixture
    def orch(self):
        o = MagicMock()
        o.teradata_client = MagicMock()
        o.teradata_client.host = "wizard-host"
        o.teradata_client.database = "wizard_db"
        o.teradata_client.test_connection.return_value = {
            "connected": True,
            "version": "17.20",
        }
        o.credential_resolver = MagicMock()
        # If guard_configured() is called, the test fails — these
        # sentinels must skip it.
        o.credential_resolver.guard_configured.side_effect = AssertionError(
            "guard_configured() must not be called for sentinel profiles"
        )
        return o

    @pytest.mark.parametrize("sentinel", [
        "wizard", "default",
        "Wizard", "WIZARD", "DEFAULT",
        "  wizard  ", "\tdefault\n",
        "  ",  # whitespace-only
    ])
    @pytest.mark.asyncio
    async def test_sentinel_profile_uses_orchestrator_default(self, orch, sentinel):
        tools = register_metadata_tools(orch)
        result = await tools["teradata_discover"](
            action="test_connection",
            teradata_profile=sentinel,
        )
        assert result["success"] is True, sentinel
        assert result["host"] == "wizard-host", sentinel
        # guard_configured side-effect would have raised if called.

    @pytest.mark.asyncio
    async def test_real_profile_still_invokes_guard(self, orch):
        """Sanity: a non-sentinel profile name still goes through the
        guard_configured / resolve path (we just disabled the side-effect
        for this assertion)."""
        orch.credential_resolver.guard_configured.side_effect = None
        orch.credential_resolver.guard_configured.return_value = None
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "prod-host", "port": 1025, "database": "prod_db",
            "username": "u", "password": "p", "logmech": "TD2",
        }
        tools = register_metadata_tools(orch)
        await tools["teradata_discover"](
            action="test_connection",
            teradata_profile="prod",
        )
        orch.credential_resolver.guard_configured.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
