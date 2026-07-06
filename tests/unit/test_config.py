"""Unit tests for Configuration Management (Settings / load_settings API)."""

import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr, ValidationError

from elt_mcp_server.config import (
    AirbyteSettings,
    AirflowSettings,
    DBTSettings,
    MCPServerSettings,
    PipelineSettings,
    Settings,
    TeradataSettings,
    TTUSettings,
    get_settings,
    load_settings,
    reset_settings,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_env(**overrides) -> dict[str, str]:
    """Return the minimal env-var dict required to construct Settings().

    Only Teradata fields are required; Airflow/Airbyte/dbt are all optional.
    """
    env = {
        "TERADATA_HOST": "localhost",
        "TERADATA_USERNAME": "dbc",
        "TERADATA_PASSWORD": "dbc",
    }
    env.update(overrides)
    return env


def _make_settings(**overrides) -> Settings:
    """Create a Settings object with minimal valid sub-settings."""
    defaults = dict(
        teradata=TeradataSettings(
            host="localhost", username="dbc", password=SecretStr("dbc"),
        ),
        airflow=AirflowSettings(
            base_url="http://localhost:8080",
            username="admin",
            password=SecretStr("admin"),
        ),
    )
    defaults.update(overrides)
    return Settings(**defaults)


# ===========================================================================
# TeradataSettings
# ===========================================================================


class TestTeradataSettings:
    """Test suite for TeradataSettings."""

    def test_basic_construction(self):
        ts = TeradataSettings(host="db.example.com", username="user", password=SecretStr("pass"))
        assert ts.host == "db.example.com"
        assert ts.username == "user"
        assert ts.password.get_secret_value() == "pass"

    @patch.dict(os.environ, {}, clear=True)
    def test_defaults(self):
        ts = TeradataSettings(host="h", username="u", password=SecretStr("p"))
        assert ts.port == 1025
        assert ts.logmech == "TD2"
        assert ts.database == ""
        assert ts.pool_size == 5
        assert ts.query_timeout == 300
        assert ts.charset == "UTF8"

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_required_host(self):
        with pytest.raises(ValidationError):
            TeradataSettings(username="u", password=SecretStr("p"))

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_required_password(self):
        with pytest.raises(ValidationError):
            TeradataSettings(host="h", username="u")

    def test_custom_optional_fields(self):
        ts = TeradataSettings(
            host="h", username="u", password=SecretStr("p"),
            logmech="LDAP", charset="ASCII", port=2025,
        )
        assert ts.logmech == "LDAP"
        assert ts.charset == "ASCII"
        assert ts.port == 2025

    @patch.dict(os.environ, {"TERADATA_HOST": "env-host", "TERADATA_USERNAME": "env-user", "TERADATA_PASSWORD": "env-pass"}, clear=False)
    def test_loads_from_env_vars(self):
        ts = TeradataSettings()
        assert ts.host == "env-host"
        assert ts.username == "env-user"
        assert ts.password.get_secret_value() == "env-pass"


# ===========================================================================
# AirflowSettings
# ===========================================================================


class TestAirflowSettings:
    """Test suite for AirflowSettings."""

    def test_basic_construction(self):
        af = AirflowSettings(base_url="http://airflow:8080", username="admin", password=SecretStr("admin"))
        assert af.base_url == "http://airflow:8080"
        assert af.username == "admin"

    def test_trailing_slash_stripped(self):
        af = AirflowSettings(base_url="http://airflow:8080/", username="u", password=SecretStr("p"))
        assert af.base_url == "http://airflow:8080"

    @patch.dict(os.environ, {}, clear=True)
    def test_defaults(self):
        af = AirflowSettings(base_url="http://x", username="u", password=SecretStr("p"))
        assert af.auth_manager == "simple"
        assert af.timeout == 30
        assert af.default_owner == "elt_mcp_server"
        assert af.default_retries == 1
        assert af.dag_folder == "/opt/airflow/dags"

    @patch.dict(os.environ, {}, clear=True)
    def test_all_fields_optional(self):
        """base_url/username/password are optional — server starts without Airflow config."""
        af = AirflowSettings()
        assert af.base_url is None
        assert af.username is None
        assert af.password is None

    def test_with_dag_folder(self):
        af = AirflowSettings(
            base_url="http://x", username="u", password=SecretStr("p"),
            dag_folder="/opt/airflow/dags",
        )
        assert af.dag_folder == "/opt/airflow/dags"

    @patch.dict(os.environ, {}, clear=True)
    def test_validate_base_url_none(self):
        """validate_base_url returns None when base_url is not set."""
        af = AirflowSettings()
        assert af.base_url is None

    @patch.dict(os.environ, {
        "AIRFLOW_REMOTE_HOST": "airflow-server.example.com",
        "AIRFLOW_REMOTE_USER": "deploy",
        "AIRFLOW_REMOTE_SSH_KEY": "/home/deploy/.ssh/id_rsa",
        "AIRFLOW_REMOTE_PASSWORD": "secret",
    }, clear=True)
    def test_remote_fields_loaded_from_env(self):
        """AIRFLOW_REMOTE_* env vars are picked up by AirflowSettings remote_* fields."""
        af = AirflowSettings()
        assert af.remote_host == "airflow-server.example.com"
        assert af.remote_user == "deploy"
        assert af.remote_ssh_key == "/home/deploy/.ssh/id_rsa"
        assert af.remote_password.get_secret_value() == "secret"


# ===========================================================================
# AirbyteSettings
# ===========================================================================


class TestAirbyteSettings:
    """Test suite for AirbyteSettings."""

    @patch.dict(os.environ, {}, clear=True)
    def test_defaults_no_required_fields(self):
        ab = AirbyteSettings()
        assert ab.enabled is False
        assert ab.base_url is None
        assert ab.client_id is None
        assert ab.client_secret is None

    def test_with_auth(self):
        ab = AirbyteSettings(
            base_url="http://airbyte:8000",
            client_id="my_client",
            client_secret=SecretStr("my_secret"),
        )
        assert ab.client_id == "my_client"
        assert ab.client_secret.get_secret_value() == "my_secret"

    def test_with_workspace(self):
        ab = AirbyteSettings(workspace_id="ws_123")
        assert ab.workspace_id == "ws_123"


# ===========================================================================
# DBTSettings
# ===========================================================================


class TestDBTSettings:
    """Test suite for DBTSettings."""

    @patch.dict(os.environ, {}, clear=True)
    def test_defaults(self):
        dbt = DBTSettings()
        assert dbt.project_dir == Path("./dbt_project")
        assert dbt.profiles_dir is None
        assert dbt.target == "dev"
        assert dbt.threads == 4
        assert dbt.command_timeout == 300

    def test_custom_target(self):
        dbt = DBTSettings(target="prod")
        assert dbt.target == "prod"

    def test_path_validation(self):
        dbt = DBTSettings(project_dir="/custom/path", profiles_dir="/custom/profiles")
        assert dbt.project_dir == Path("/custom/path")
        assert dbt.profiles_dir == Path("/custom/profiles")

    def test_command_timeout_min(self):
        with pytest.raises(ValidationError):
            DBTSettings(command_timeout=0)


# ===========================================================================
# PipelineSettings
# ===========================================================================


class TestPipelineSettings:
    """Test suite for PipelineSettings."""

    def test_defaults(self):
        ps = PipelineSettings()
        assert ps.default_schedule_interval == "@daily"
        assert ps.generate_dbt_by_default is True
        assert ps.validate_before_deploy is True
        assert ps.enable_data_quality_checks is True


# ===========================================================================
# MCPServerSettings
# ===========================================================================


class TestMCPServerSettings:
    """Test suite for MCPServerSettings."""

    def test_defaults(self):
        mcp = MCPServerSettings()
        assert mcp.log_level == "INFO"
        assert mcp.max_concurrent_requests == 10

    def test_invalid_log_level(self):
        with pytest.raises(ValidationError):
            MCPServerSettings(log_level="TRACE")


# ===========================================================================
# Settings (main aggregate)
# ===========================================================================


class TestSettings:
    """Test suite for the root Settings class."""

    def test_construction_with_explicit_sub_settings(self):
        s = _make_settings()
        assert s.teradata.host == "localhost"
        assert s.airflow.base_url == "http://localhost:8080"
        assert s.environment == "development"

    @patch.dict(os.environ, {}, clear=True)
    def test_defaults_for_optional_sub_settings(self):
        s = _make_settings()
        assert s.airbyte.enabled is False
        assert s.dbt.target == "dev"

    @patch.dict(os.environ, _minimal_env(ENVIRONMENT="production"), clear=True)
    def test_loads_from_environment(self):
        s = Settings()
        assert s.environment == "production"
        assert s.teradata.host == "localhost"

    @patch.dict(os.environ, _minimal_env(), clear=True)
    def test_starts_without_airflow_env_vars(self):
        """Server starts when no AIRFLOW_* env vars are set — Airflow is optional."""
        s = Settings()
        assert s.airflow.base_url is None
        assert s.airflow.username is None
        assert s.airflow.password is None

    def test_to_dict_masks_secrets(self):
        s = _make_settings()
        d = s.to_dict(include_secrets=False)
        assert d["teradata"]["password"] == "***MASKED***"
        assert d["airflow"]["password"] == "***MASKED***"

    def test_to_dict_includes_secrets_when_requested(self):
        s = _make_settings()
        d = s.to_dict(include_secrets=True)
        # SecretStr is serialized by pydantic model_dump
        assert d["teradata"]["password"] is not None
        assert d["teradata"]["password"] != "***MASKED***"

    def test_get_connection_string_teradata(self):
        s = _make_settings()
        conn = s.get_connection_string("teradata")
        assert "localhost" in conn
        assert "dbc" in conn

    def test_get_connection_string_airflow(self):
        s = _make_settings()
        assert s.get_connection_string("airflow") == "http://localhost:8080"

    def test_get_connection_string_unknown_raises(self):
        s = _make_settings()
        with pytest.raises(ValueError, match="Unknown service"):
            s.get_connection_string("unknown")


# ===========================================================================
# Settings cross-field validation (model_validator)
# ===========================================================================


class TestSettingsValidation:
    """Test cross-setting validators on Settings."""

    def test_airbyte_enabled_requires_base_url(self):
        with pytest.raises(ValidationError, match="AIRBYTE_BASE_URL"):
            _make_settings(airbyte=AirbyteSettings(enabled=True, base_url=""))

    def test_valid_airflow_simple_auth_with_token_endpoint(self):
        """simple auth is valid when token_endpoint is set (the default)."""
        s = _make_settings(
            airflow=AirflowSettings(
                base_url="http://x",
                username="u",
                password=SecretStr("p"),
                auth_manager="simple",
                token_endpoint="/auth/token",
            ),
        )
        assert s.airflow.auth_manager == "simple"

    def test_valid_airflow_simple_auth_with_access_token(self):
        s = _make_settings(
            airflow=AirflowSettings(
                base_url="http://x",
                username="u",
                password=SecretStr("p"),
                auth_manager="simple",
                token_endpoint="",
                access_token=SecretStr("my-token"),
            ),
        )
        assert s.airflow.access_token.get_secret_value() == "my-token"


# ===========================================================================
# Teradata source / target helpers
# ===========================================================================


class TestTeradataSourceTarget:
    """Test source/target Teradata helpers on Settings."""

    def test_get_source_teradata_falls_back_to_main(self):
        s = _make_settings()
        assert s.get_source_teradata().host == "localhost"

    def test_get_target_teradata_falls_back_to_main(self):
        s = _make_settings()
        assert s.get_target_teradata().host == "localhost"

    def test_get_source_teradata_uses_override(self):
        src = TeradataSettings(host="src-host", username="u", password=SecretStr("p"))
        s = _make_settings(teradata_source=src)
        assert s.get_source_teradata().host == "src-host"

    def test_is_teradata_to_teradata_false_by_default(self):
        s = _make_settings()
        assert s.is_teradata_to_teradata() is False

    def test_is_teradata_to_teradata_true(self):
        src = TeradataSettings(host="src-host", username="u", password=SecretStr("p"))
        tgt = TeradataSettings(host="tgt-host", username="u", password=SecretStr("p"))
        s = _make_settings(teradata_source=src, teradata_target=tgt)
        assert s.is_teradata_to_teradata() is True

    @patch.dict(os.environ, {
        **_minimal_env(),
        "TERADATA_SOURCE_HOST": "env-src",
        "TERADATA_SOURCE_USERNAME": "src-u",
        "TERADATA_SOURCE_PASSWORD": "src-p",
    }, clear=True)
    def test_source_from_env_vars(self):
        s = Settings()
        assert s.teradata_source is not None
        assert s.teradata_source.host == "env-src"


# ===========================================================================
# load_settings / get_settings / reset_settings
# ===========================================================================


class TestLoadSettings:
    """Test the module-level settings management functions."""

    @pytest.fixture(autouse=True)
    def _reset(self):
        """Ensure clean state between tests."""
        reset_settings()
        yield
        reset_settings()

    @patch.dict(os.environ, _minimal_env(), clear=True)
    def test_load_settings_returns_settings(self):
        s = load_settings()
        assert isinstance(s, Settings)
        assert s.teradata.host == "localhost"

    @patch.dict(os.environ, _minimal_env(), clear=True)
    def test_load_settings_caches(self):
        s1 = load_settings()
        s2 = load_settings()
        assert s1 is s2

    @patch.dict(os.environ, _minimal_env(), clear=True)
    def test_load_settings_force_reload(self):
        s1 = load_settings()
        s2 = load_settings(force_reload=True)
        assert s1 is not s2

    @patch.dict(os.environ, _minimal_env(), clear=True)
    def test_get_settings_auto_loads(self):
        s = get_settings()
        assert isinstance(s, Settings)

    @patch.dict(os.environ, _minimal_env(), clear=True)
    def test_reset_settings_clears_cache(self):
        s1 = load_settings()
        reset_settings()
        s2 = load_settings()
        assert s1 is not s2


# ===========================================================================
# Environment variable integration
# ===========================================================================


class TestEnvironmentVariableLoading:
    """Test that env-var prefixes work correctly across sub-settings."""

    @patch.dict(os.environ, {
        **_minimal_env(),
        "TERADATA_PORT": "2025",
        "TERADATA_LOGMECH": "LDAP",
        "AIRFLOW_TIMEOUT": "60",
        "DBT_TARGET": "prod",
    }, clear=True)
    def test_sub_settings_from_env(self):
        s = Settings()
        assert s.teradata.port == 2025
        assert s.teradata.logmech == "LDAP"
        assert s.airflow.timeout == 60
        assert s.dbt.target == "prod"

    @patch.dict(os.environ, {
        **_minimal_env(),
        "MCP_LOG_LEVEL": "DEBUG",
    }, clear=True)
    def test_mcp_settings_from_env(self):
        s = Settings()
        assert s.mcp.log_level == "DEBUG"


# ===========================================================================
# validate_connectivity — Airflow branch
# ===========================================================================


def _make_airflow_client_mock(connected: bool, error: str | None = None) -> MagicMock:
    """Return a mock AsyncAirflowClient whose test_connection returns the given result."""
    result = {"connected": connected}
    if error:
        result["error"] = error
    mock_client = MagicMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.test_connection = AsyncMock(return_value=result)
    return mock_client


# Suppress teradatasql so the Teradata branch is always skipped in these tests.
_NO_TERADATASQL = patch.dict(sys.modules, {"teradatasql": None})


class TestValidateConnectivityAirflow:
    """Unit tests for the Airflow branch of Settings.validate_connectivity."""

    _PATCH_CLIENT = "elt_mcp_server.clients.async_airflow_client.AsyncAirflowClient"

    def test_airflow_not_configured_is_skipped(self):
        """When base_url is not set, Airflow check is skipped and valid stays True."""
        with patch.dict(os.environ, {}, clear=False):
            # Remove any AIRFLOW_* env vars so AirflowSettings() defaults to base_url=None
            for key in list(os.environ):
                if key.startswith("AIRFLOW_"):
                    os.environ.pop(key)
            s = _make_settings(airflow=AirflowSettings())
        with _NO_TERADATASQL:
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["airflow"]["status"] == "skipped"
        assert result["valid"] is True
        assert result["errors"] == []

    def test_airflow_connected_true_sets_ok(self):
        """When test_connection returns connected=True, status is ok and valid stays True."""
        s = _make_settings()
        mock_client = _make_airflow_client_mock(connected=True)
        with patch(self._PATCH_CLIENT, return_value=mock_client), _NO_TERADATASQL:
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["airflow"]["status"] == "ok"
        assert result["valid"] is True
        assert result["errors"] == []
        assert "latency_ms" in result["services"]["airflow"]

    def test_airflow_connected_false_sets_degraded_and_valid_true(self):
        """When test_connection returns connected=False, status is degraded but valid stays True."""
        s = _make_settings()
        mock_client = _make_airflow_client_mock(connected=False, error="Auth failed")
        with patch(self._PATCH_CLIENT, return_value=mock_client), _NO_TERADATASQL:
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["airflow"]["status"] == "degraded"
        assert result["valid"] is True
        assert result["errors"] == []
        assert any("Auth failed" in w for w in result["warnings"])

    def test_airflow_exception_sets_degraded_and_valid_true(self):
        """When AsyncAirflowClient raises, status is degraded but valid stays True."""
        s = _make_settings()
        with patch(self._PATCH_CLIENT, side_effect=RuntimeError("Network timeout")), _NO_TERADATASQL:
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["airflow"]["status"] == "degraded"
        assert result["valid"] is True
        assert result["errors"] == []
        assert result["warnings"]

    def test_airflow_timeout_sets_degraded_and_valid_true(self):
        """When the Airflow check times out, status is degraded but valid stays True."""
        import asyncio as _asyncio

        s = _make_settings()
        with patch("asyncio.wait_for", side_effect=_asyncio.TimeoutError()), _NO_TERADATASQL:
            result = s.validate_connectivity(timeout=1)

        assert result["services"]["airflow"]["status"] == "degraded"
        assert result["valid"] is True
        assert result["errors"] == []
        assert result["warnings"]

    def test_airflow_incomplete_providers_skips_missing_check(self):
        """When get_providers returns incomplete=True, missing provider check is skipped."""
        s = _make_settings()
        mock_client = _make_airflow_client_mock(connected=True)
        incomplete_providers = {"providers": [], "total_entries": 100, "incomplete": True}
        mock_client.get_providers = AsyncMock(return_value=incomplete_providers)
        with patch(self._PATCH_CLIENT, return_value=mock_client), _NO_TERADATASQL:
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["airflow"]["status"] == "ok"
        assert "missing_providers" not in result["services"]["airflow"]
        assert result["services"]["airflow"]["provider_discovery_incomplete"] is True
        assert any("incomplete" in w.lower() for w in result["warnings"])

    def test_airflow_complete_providers_checks_missing(self):
        """When get_providers returns complete list, missing provider check runs."""
        s = _make_settings()
        mock_client = _make_airflow_client_mock(connected=True)
        complete_providers = {
            "providers": [{"package_name": "apache-airflow-providers-ssh"}],
            "total_entries": 1,
        }
        mock_client.get_providers = AsyncMock(return_value=complete_providers)
        with patch(self._PATCH_CLIENT, return_value=mock_client), _NO_TERADATASQL:
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["airflow"]["status"] == "ok"
        assert "missing_providers" in result["services"]["airflow"]


# ===========================================================================
# validate_connectivity — Teradata branch
# ===========================================================================


def _make_teradatasql_mock(
    version: str | None = "17.00.01",
    connect_raises: Exception | None = None,
    version_query_raises: Exception | None = None,
) -> MagicMock:
    """Build a mock teradatasql module for validate_connectivity tests."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    # First fetchone is for SELECT 1 (result discarded); second is for VERSION query.
    if version_query_raises:
        mock_cursor.fetchone.side_effect = [MagicMock(), version_query_raises]
        mock_cursor.execute.side_effect = [None, version_query_raises]
    else:
        mock_cursor.fetchone.side_effect = [MagicMock(), (version,) if version else None]

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_td = MagicMock()
    if connect_raises:
        mock_td.connect.side_effect = connect_raises
    else:
        mock_td.connect.return_value = mock_conn
    return mock_td


class TestValidateConnectivityTeradata:
    """Unit tests for the Teradata branch of Settings.validate_connectivity."""

    def _settings_no_airflow(self) -> Settings:
        """Settings with Airflow skipped so only the Teradata branch is exercised."""
        return _make_settings(airflow=AirflowSettings())

    def test_teradata_not_installed_is_skipped(self):
        """When teradatasql is absent, Teradata check is skipped and valid stays True."""
        s = self._settings_no_airflow()
        with patch.dict(sys.modules, {"teradatasql": None}):
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["teradata"]["status"] == "skipped"
        assert result["valid"] is True
        assert any("teradatasql" in w for w in result["warnings"])

    def test_teradata_connected_with_version(self):
        """Successful connection with version query returns status=ok and version string."""
        s = self._settings_no_airflow()
        mock_td = _make_teradatasql_mock(version="17.00.01")
        with patch.dict(sys.modules, {"teradatasql": mock_td}):
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["teradata"]["status"] == "ok"
        assert "17.00.01" in result["services"]["teradata"]["message"]
        assert result["valid"] is True
        assert result["errors"] == []

    def test_teradata_connected_version_query_fails(self):
        """If DBC.DBCInfoV is inaccessible, status is still ok with version=unknown."""
        s = self._settings_no_airflow()
        mock_td = _make_teradatasql_mock(version_query_raises=Exception("Permission denied"))
        with patch.dict(sys.modules, {"teradatasql": mock_td}):
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["teradata"]["status"] == "ok"
        assert "unknown" in result["services"]["teradata"]["message"]
        assert result["valid"] is True
        assert result["errors"] == []

    def test_teradata_connection_failure_sets_error_and_valid_false(self):
        """Teradata connection failure sets valid=False — Teradata is a required service."""
        s = self._settings_no_airflow()
        mock_td = _make_teradatasql_mock(connect_raises=Exception("Host unreachable"))
        with patch.dict(sys.modules, {"teradatasql": mock_td}):
            result = s.validate_connectivity(timeout=5)

        assert result["services"]["teradata"]["status"] == "error"
        assert result["valid"] is False
        assert any("Teradata" in e for e in result["errors"])
        assert result["warnings"] == []


# ===========================================================================
# TTUSettings
# ===========================================================================


class TestTTUSettings:
    """Tests for TTUSettings defaults and environment variable loading."""

    @patch.dict(os.environ, {}, clear=False)
    def test_ttu_settings_defaults(self):
        """All TTU defaults should be sensible."""
        for key in list(os.environ):
            if key.startswith("TTU_"):
                os.environ.pop(key)

        def mock_exists(self_path):
            if "17.20" in str(self_path) and "bin" in str(self_path):
                return True
            return False

        with patch.object(Path, "exists", mock_exists):
            s = TTUSettings()

        assert s.enabled is False
        assert s.ttu_version == "17.20"
        assert "tbuild" in s.tpt_binary_path
        assert "bteq" in s.bteq_binary_path
        assert "tdload" in s.tdload_binary_path
        assert s.scripts_dir == Path("./ttu_scripts")
        assert s.command_timeout == 600
        assert s.tpt_error_limit == 1

    @patch.dict(os.environ, {
        "TTU_ENABLED": "true",
        "TTU_TPT_BINARY_PATH": "/opt/teradata/bin/tbuild",
        "TTU_BTEQ_BINARY_PATH": "/opt/teradata/bin/bteq",
        "TTU_TDLOAD_BINARY_PATH": "/opt/teradata/bin/tdload",
        "TTU_SCRIPTS_DIR": "/tmp/ttu_scripts",
        "TTU_COMMAND_TIMEOUT": "900",
        "TTU_TPT_ERROR_LIMIT": "5",
    }, clear=False)
    @patch("shutil.which", side_effect=lambda p: p if p.startswith("/opt/teradata") else None)
    def test_ttu_settings_from_env(self, _mock_which):
        """TTU settings should load from TTU_* environment variables."""
        s = TTUSettings()
        assert s.enabled is True
        assert s.tpt_binary_path == "/opt/teradata/bin/tbuild"
        assert s.bteq_binary_path == "/opt/teradata/bin/bteq"
        assert s.tdload_binary_path == "/opt/teradata/bin/tdload"
        assert s.scripts_dir == Path("/tmp/ttu_scripts")
        assert s.command_timeout == 900
        assert s.tpt_error_limit == 5

    @patch.dict(os.environ, {"TTU_ENABLED": "true"}, clear=False)
    @patch("platform.system", return_value="Linux")
    def test_ttu_auto_detect_finds_installed_version(self, _mock_platform):
        """Auto-detection should find an installed version when default doesn't exist."""
        for key in list(os.environ):
            if key.startswith("TTU_") and key != "TTU_ENABLED":
                os.environ.pop(key)

        parent_path = Path("/opt/teradata/client")

        def mock_exists(self_path):
            path_str = str(self_path)
            if "20.00" in path_str and "bin" in path_str:
                return True
            return False

        def mock_is_file(self_path):
            path_str = str(self_path)
            if "20.00" in path_str and "bin" in path_str:
                return True
            return False

        def mock_is_dir(self_path):
            if self_path == parent_path:
                return True
            return False

        def mock_iterdir(self_path):
            if self_path == parent_path:
                mock_dir = MagicMock(spec=Path)
                mock_dir.is_dir.return_value = True
                mock_dir.name = "20.00"
                mock_bin = MagicMock(spec=Path)
                mock_bin.is_dir.return_value = True
                mock_dir.__truediv__ = MagicMock(return_value=mock_bin)
                return [mock_dir]
            return iter([])

        def mock_which(name):
            if "20.00" in name and "bin" in name:
                return name
            return None

        with patch.object(Path, "exists", mock_exists), \
             patch.object(Path, "is_file", mock_is_file), \
             patch.object(Path, "is_dir", mock_is_dir), \
             patch.object(Path, "iterdir", mock_iterdir), \
             patch("shutil.which", mock_which):
            s = TTUSettings()

        assert s.ttu_version == "20.00"
        assert "20.00" in s.tpt_binary_path

    @patch.dict(os.environ, {"TTU_ENABLED": "true"}, clear=False)
    @patch("platform.system", return_value="Linux")
    def test_ttu_auto_detect_picks_highest_version(self, _mock_platform):
        """Auto-detection should pick the highest version when multiple are installed."""
        for key in list(os.environ):
            if key.startswith("TTU_") and key != "TTU_ENABLED":
                os.environ.pop(key)

        parent_path = Path("/opt/teradata/client")

        def mock_exists(self_path):
            path_str = str(self_path)
            if "20.00" in path_str and "bin" in path_str:
                return True
            return False

        def mock_is_file(self_path):
            path_str = str(self_path)
            if "20.00" in path_str and "bin" in path_str:
                return True
            return False

        def mock_is_dir(self_path):
            if self_path == parent_path:
                return True
            return False

        def mock_iterdir(self_path):
            if self_path == parent_path:
                dirs = []
                for v in ["17.10", "20.00", "19.00"]:
                    d = MagicMock(spec=Path)
                    d.is_dir.return_value = True
                    d.name = v
                    mock_bin = MagicMock(spec=Path)
                    mock_bin.is_dir.return_value = True
                    d.__truediv__ = MagicMock(return_value=mock_bin)
                    dirs.append(d)
                return dirs
            return iter([])

        def mock_which(name):
            if "20.00" in name and "bin" in name:
                return name
            return None

        with patch.object(Path, "exists", mock_exists), \
             patch.object(Path, "is_file", mock_is_file), \
             patch.object(Path, "is_dir", mock_is_dir), \
             patch.object(Path, "iterdir", mock_iterdir), \
             patch("shutil.which", mock_which):
            s = TTUSettings()

        assert s.ttu_version == "20.00"

    @patch.dict(os.environ, {"TTU_ENABLED": "true"}, clear=False)
    @patch("platform.system", return_value="Linux")
    def test_ttu_auto_detect_no_versions_falls_back(self, _mock_platform):
        """When no TTU versions are found, should fall back to bare binary names."""
        for key in list(os.environ):
            if key.startswith("TTU_") and key != "TTU_ENABLED":
                os.environ.pop(key)

        parent_path = Path("/opt/teradata/client")

        def mock_exists(self_path):
            return False

        def mock_is_dir(self_path):
            if self_path == parent_path:
                return False
            return False

        with patch.object(Path, "exists", mock_exists), \
             patch.object(Path, "is_dir", mock_is_dir):
            s = TTUSettings()

        assert s.tpt_binary_path == "tbuild"
        assert s.bteq_binary_path == "bteq"
        assert s.tdload_binary_path == "tdload"

    @patch.dict(os.environ, {"TTU_ENABLED": "true", "TTU_TTU_VERSION": "19.00"}, clear=False)
    @patch("platform.system", return_value="Linux")
    def test_ttu_explicit_version_not_overridden(self, _mock_platform):
        """Explicit TTU_TTU_VERSION should not be overridden by auto-detection."""
        for key in list(os.environ):
            if key.startswith("TTU_") and key not in ("TTU_TTU_VERSION", "TTU_ENABLED"):
                os.environ.pop(key)

        parent_path = Path("/opt/teradata/client")

        def mock_exists(self_path):
            return False

        def mock_is_dir(self_path):
            if self_path == parent_path:
                return True
            return False

        def mock_iterdir(self_path):
            if self_path == parent_path:
                d = MagicMock(spec=Path)
                d.is_dir.return_value = True
                d.name = "20.00"
                mock_bin = MagicMock(spec=Path)
                mock_bin.is_dir.return_value = True
                d.__truediv__ = MagicMock(return_value=mock_bin)
                return [d]
            return iter([])

        with patch.object(Path, "exists", mock_exists), \
             patch.object(Path, "is_dir", mock_is_dir), \
             patch.object(Path, "iterdir", mock_iterdir):
            s = TTUSettings()

        assert s.ttu_version == "19.00"

    @patch.dict(os.environ, {"TTU_ENABLED": "true", "TTU_TTU_VERSION": "17.20"}, clear=False)
    @patch("platform.system", return_value="Linux")
    def test_ttu_explicit_default_version_not_overridden(self, _mock_platform):
        """Explicit TTU_TTU_VERSION=17.20 should not trigger auto-detection."""
        for key in list(os.environ):
            if key.startswith("TTU_") and key not in ("TTU_TTU_VERSION", "TTU_ENABLED"):
                os.environ.pop(key)

        parent_path = Path("/opt/teradata/client")

        def mock_exists(self_path):
            return False

        def mock_is_dir(self_path):
            if self_path == parent_path:
                return True
            return False

        def mock_iterdir(self_path):
            if self_path == parent_path:
                d = MagicMock(spec=Path)
                d.is_dir.return_value = True
                d.name = "20.00"
                mock_bin = MagicMock(spec=Path)
                mock_bin.is_dir.return_value = True
                d.__truediv__ = MagicMock(return_value=mock_bin)
                return [d]
            return iter([])

        with patch.object(Path, "exists", mock_exists), \
             patch.object(Path, "is_dir", mock_is_dir), \
             patch.object(Path, "iterdir", mock_iterdir):
            s = TTUSettings()

        assert s.ttu_version == "17.20"

    @patch.dict(os.environ, _minimal_env(), clear=True)
    def test_settings_includes_ttu(self):
        """Main Settings should include ttu as a sub-setting."""
        s = Settings()
        assert hasattr(s, "ttu")
        assert isinstance(s.ttu, TTUSettings)
