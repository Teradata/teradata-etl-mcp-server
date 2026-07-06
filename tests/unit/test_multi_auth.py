"""Integration-style tests for multi-auth flow through the clients.

Renderer-level wire-format tests (BTEQ script lines, teradatasql kwargs, etc.)
live in :mod:`tests.unit.test_teradata_auth`. This file focuses on the
client-level glue: ``TeradataAuth`` → client method → subprocess / driver.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elt_mcp_server.auth import TeradataAuth
from elt_mcp_server.clients.teradata_client import TeradataClient
from elt_mcp_server.clients.ttu_client import TTUClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ttu_client(tmp_path: Path) -> TTUClient:
    """TTUClient is stateless w.r.t. identity — auth is passed per call."""
    return TTUClient(
        scripts_dir=tmp_path / "scripts",
        command_timeout=30,
    )


def _make_auth(mechanism: str = "TD2", **overrides) -> TeradataAuth:
    """Build a :class:`TeradataAuth` with valid per-mechanism defaults."""
    defaults: dict = {
        "host": "testhost.example.com",
        "port": 1025,
        "database": "testdb",
    }
    if mechanism in ("TD2", "LDAP"):
        defaults.update(username="testuser", password="testpass")
    elif mechanism == "JWT":
        defaults.update(username="testuser", logdata="eyJhbGci.x.y")
    elif mechanism == "SECRET":
        defaults.update(oidc_clientid="my_client", logdata="the_secret")
    elif mechanism == "BEARER":
        defaults.update(
            oidc_clientid="my_client",
            jws_private_key="/etc/keys/priv.pem",
        )
    defaults["mechanism"] = mechanism
    defaults.update(overrides)
    return TeradataAuth(**defaults)


def _make_td_client(mechanism: str = "TD2", **overrides) -> TeradataClient:
    auth = _make_auth(mechanism=mechanism, **overrides)
    with patch("elt_mcp_server.clients.teradata_client.teradatasql", MagicMock()):
        with patch("elt_mcp_server.clients.teradata_client.pd", MagicMock()):
            return TeradataClient(auth=auth)


# ===========================================================================
# TTUClient — BTEQ script build integrates TeradataAuth.render_for_bteq()
# ===========================================================================


class TestBteqPreparedScript:
    """Verifies that TeradataAuth's render_for_bteq output is prepended to the
    user script with the expected LOGOFF/EXIT footer."""

    def test_td2(self, tmp_path):
        client = _make_ttu_client(tmp_path)
        auth = _make_auth("TD2")
        script = client._prepare_bteq_script(auth, "SELECT 1;")
        assert ".LOGON testhost.example.com/testuser,testpass" in script
        assert ".LOGOFF" in script
        assert ".EXIT" in script

    def test_ldap(self, tmp_path):
        client = _make_ttu_client(tmp_path)
        auth = _make_auth("LDAP")
        script = client._prepare_bteq_script(auth, "SELECT 1;")
        assert ".SET LOGMECH LDAP" in script
        assert ".LOGON testhost.example.com/testuser,testpass" in script

    def test_jwt(self, tmp_path):
        client = _make_ttu_client(tmp_path)
        auth = _make_auth("JWT", logdata="token=eyJinner")
        script = client._prepare_bteq_script(auth, "SELECT 1;")
        assert ".SET LOGMECH JWT" in script
        assert ".LOGDATA token=eyJinner" in script
        assert ".LOGON testhost.example.com/" in script

    def test_secret(self, tmp_path):
        client = _make_ttu_client(tmp_path)
        auth = _make_auth(
            "SECRET", oidc_clientid="my-client-id", logdata="my-client-secret"
        )
        script = client._prepare_bteq_script(auth, "CREATE TABLE t1 (id INT);")
        assert ".SET LOGMECH CRED" in script
        assert "client_id=my-client-id" in script
        assert "client_secret=my-client-secret" in script

    def test_bearer(self, tmp_path):
        client = _make_ttu_client(tmp_path)
        auth = _make_auth(
            "BEARER",
            oidc_clientid="bearer-id",
            jws_private_key="key.pem",
            jws_cert="cert.pem",
            sslca="ca.pem",
        )
        script = client._prepare_bteq_script(auth, "SELECT 1;")
        assert ".SET LOGMECH BEARER" in script
        assert "oidc_clientid=bearer-id" in script
        assert "jws_private_key=key.pem" in script
        assert "SSLCA=ca.pem" in script


# ===========================================================================
# TeradataClient — _get_connection passes the right kwargs to teradatasql
# ===========================================================================


class TestTeradataClientConnection:
    """Verifies the auth identity reaches teradatasql.connect() through
    TeradataAuth.render_for_teradatasql()."""

    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_td2(self, mock_td):
        client = _make_td_client("TD2")
        client._get_connection()
        call_kwargs = mock_td.connect.call_args[1]
        assert call_kwargs["logmech"] == "TD2"
        assert call_kwargs["user"] == "testuser"
        assert call_kwargs["password"] == "testpass"
        assert "logdata" not in call_kwargs

    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_ldap(self, mock_td):
        client = _make_td_client("LDAP")
        client._get_connection()
        call_kwargs = mock_td.connect.call_args[1]
        assert call_kwargs["logmech"] == "LDAP"
        assert call_kwargs["user"] == "testuser"
        assert call_kwargs["password"] == "testpass"

    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_jwt(self, mock_td):
        client = _make_td_client("JWT", logdata="token=eyJ.abc.def")
        client._get_connection()
        call_kwargs = mock_td.connect.call_args[1]
        assert call_kwargs["logmech"] == "JWT"
        assert call_kwargs["logdata"] == "token=eyJ.abc.def"
        assert "user" not in call_kwargs
        assert "password" not in call_kwargs

    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_secret(self, mock_td):
        client = _make_td_client(
            "SECRET", oidc_clientid="cid", logdata="csecret"
        )
        client._get_connection()
        call_kwargs = mock_td.connect.call_args[1]
        assert call_kwargs["logmech"] == "SECRET"
        assert call_kwargs["oidc_clientid"] == "cid"
        assert call_kwargs["logdata"] == "csecret"
        assert "user" not in call_kwargs

    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_bearer(self, mock_td):
        client = _make_td_client(
            "BEARER",
            oidc_clientid="bid",
            jws_private_key="key.pem",
            jws_cert="cert.pem",
        )
        client._get_connection()
        call_kwargs = mock_td.connect.call_args[1]
        assert call_kwargs["logmech"] == "BEARER"
        assert call_kwargs["oidc_clientid"] == "bid"
        assert call_kwargs["jws_private_key"] == "key.pem"
        assert call_kwargs["jws_cert"] == "cert.pem"
        assert "user" not in call_kwargs

    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_empty_database_not_passed(self, mock_td):
        client = _make_td_client("TD2", database="")
        client._get_connection()
        call_kwargs = mock_td.connect.call_args[1]
        assert "database" not in call_kwargs

    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_nonempty_database_passed(self, mock_td):
        client = _make_td_client("TD2", database="mydb")
        client._get_connection()
        call_kwargs = mock_td.connect.call_args[1]
        assert call_kwargs["database"] == "mydb"


# ===========================================================================
# AirbyteClient OAuth2 token (unchanged by the refactor)
# ===========================================================================


class TestAirbyteClientAuth:
    @pytest.mark.asyncio
    async def test_obtain_token_success(self):
        from elt_mcp_server.clients.airbyte_client import AirbyteClient

        with patch("elt_mcp_server.clients.airbyte_client.httpx") as mock_httpx:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"access_token": "test_token_123"}
            mock_response.raise_for_status = MagicMock()

            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_httpx.AsyncClient.return_value = mock_client

            client = AirbyteClient(
                base_url="http://localhost:8000",
                client_id="test_id",
                client_secret="test_secret",
            )
            token = await client._obtain_token()
            assert token == "test_token_123"

    @pytest.mark.asyncio
    async def test_obtain_token_no_credentials(self):
        from elt_mcp_server.clients.airbyte_client import AirbyteClient

        client = AirbyteClient(
            base_url="http://localhost:8000",
            client_id=None,
            client_secret=None,
        )
        token = await client._obtain_token()
        assert token is None

    @pytest.mark.asyncio
    async def test_obtain_token_failure(self):
        from elt_mcp_server.clients.airbyte_client import AirbyteClient

        with patch("elt_mcp_server.clients.airbyte_client.httpx") as mock_httpx:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock()
            mock_client.post = AsyncMock(side_effect=Exception("Connection refused"))
            mock_httpx.AsyncClient.return_value = mock_client

            client = AirbyteClient(
                base_url="http://localhost:8000",
                client_id="test_id",
                client_secret="test_secret",
            )
            token = await client._obtain_token()
            assert token is None


# ===========================================================================
# JDBC URL logging (credentials masked in log output)
# ===========================================================================


class TestJdbcUrlLogging:
    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_td2_masks_password(self, mock_td, caplog):
        import logging
        with caplog.at_level(logging.INFO):
            client = _make_td_client("TD2")
            client._get_connection()
        assert "jdbc:teradata://testhost.example.com/" in caplog.text
        assert "LOGMECH=TD2" in caplog.text
        assert "USER=testuser" in caplog.text
        assert "PASSWORD=***" in caplog.text
        assert "testpass" not in caplog.text

    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_jwt_masks_logdata(self, mock_td, caplog):
        import logging
        with caplog.at_level(logging.INFO):
            client = _make_td_client("JWT", logdata="token=eyJsecret")
            client._get_connection()
        assert "LOGMECH=JWT" in caplog.text
        assert "LOGDATA=***" in caplog.text
        assert "eyJsecret" not in caplog.text

    @patch("elt_mcp_server.clients.teradata_client.teradatasql")
    def test_bearer_shows_file_paths(self, mock_td, caplog):
        import logging
        with caplog.at_level(logging.INFO):
            client = _make_td_client(
                "BEARER",
                oidc_clientid="bid",
                jws_private_key="key.pem",
                jws_cert="cert.pem",
            )
            client._get_connection()
        assert "OIDC_CLIENTID=bid" in caplog.text
        assert "JWS_PRIVATE_KEY=key.pem" in caplog.text
        assert "JWS_CERT=cert.pem" in caplog.text


# ===========================================================================
# Config tests (TeradataSettings pick up per-mechanism env vars)
# ===========================================================================


class TestConfigAuthFields:
    def test_teradata_defaults(self):
        from elt_mcp_server.config import TeradataSettings

        with patch.dict("os.environ", {
            "TERADATA_HOST": "h",
            "TERADATA_USERNAME": "u",
            "TERADATA_PASSWORD": "p",
        }):
            ts = TeradataSettings()
            assert ts.logmech == "TD2"
            assert ts.logdata.get_secret_value() == ""
            assert ts.oidc_clientid == ""
            assert ts.jws_private_key == ""
            assert ts.jws_cert == ""
            assert ts.sslca == ""

    def test_teradata_jwt_from_env(self):
        from elt_mcp_server.config import TeradataSettings

        with patch.dict("os.environ", {
            "TERADATA_HOST": "h",
            "TERADATA_USERNAME": "u",
            "TERADATA_PASSWORD": "p",
            "TERADATA_LOGMECH": "JWT",
            "TERADATA_LOGDATA": "token=eyJ...",
        }):
            ts = TeradataSettings()
            assert ts.logmech == "JWT"
            assert ts.logdata.get_secret_value() == "token=eyJ..."

    def test_teradata_bearer_from_env(self):
        from elt_mcp_server.config import TeradataSettings

        with patch.dict("os.environ", {
            "TERADATA_HOST": "h",
            "TERADATA_USERNAME": "u",
            "TERADATA_PASSWORD": "p",
            "TERADATA_LOGMECH": "BEARER",
            "TERADATA_OIDC_CLIENTID": "bid",
            "TERADATA_JWS_PRIVATE_KEY": "key.pem",
            "TERADATA_JWS_CERT": "cert.pem",
            "TERADATA_SSLCA": "ca.pem",
        }):
            ts = TeradataSettings()
            assert ts.logmech == "BEARER"
            assert ts.oidc_clientid == "bid"
            assert ts.jws_private_key == "key.pem"
            assert ts.jws_cert == "cert.pem"
            assert ts.sslca == "ca.pem"

    def test_airflow_enabled_field(self):
        from elt_mcp_server.config import AirflowSettings

        with patch.dict("os.environ", {
            "AIRFLOW_ENABLED": "true",
            "AIRFLOW_BASE_URL": "http://airflow:8080",
        }):
            af = AirflowSettings()
            assert af.enabled is True

    def test_airbyte_client_fields(self):
        from elt_mcp_server.config import AirbyteSettings

        with patch.dict("os.environ", {
            "AIRBYTE_ENABLED": "true",
            "AIRBYTE_BASE_URL": "http://airbyte:8000",
            "AIRBYTE_CLIENT_ID": "cid",
            "AIRBYTE_CLIENT_SECRET": "csec",
        }):
            ab = AirbyteSettings()
            assert ab.enabled is True
            assert ab.client_id == "cid"
            assert ab.client_secret.get_secret_value() == "csec"
