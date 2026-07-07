"""Unit tests for TTU tools (ttu_execute router)."""

from unittest.mock import MagicMock, patch

from teradata_etl_mcp_server.clients.ttu_client import TTUNotInstalledError
from teradata_etl_mcp_server.tools.ttu_tools import _detect_mload_lock, register_ttu_tools

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_orchestrator():
    """Build a mock orchestrator with mock clients.

    The settings mock must carry valid TD2 fields so
    ``resolve_teradata_auth`` can construct a real :class:`TeradataAuth`.
    """
    from pydantic import SecretStr

    orch = MagicMock()
    orch.ttu_client = MagicMock()
    orch.teradata_client = MagicMock()
    orch.settings.ttu.ttu_version = "17.20"
    # Give resolve_teradata_auth a valid TD2 identity to build from.
    orch.settings.teradata.host = "testhost"
    orch.settings.teradata.port = 1025
    orch.settings.teradata.database = "testdb"
    orch.settings.teradata.username = "testuser"
    orch.settings.teradata.password = SecretStr("testpass")
    orch.settings.teradata.logmech = "TD2"
    orch.settings.teradata.logdata = SecretStr("")
    orch.settings.teradata.oidc_clientid = ""
    orch.settings.teradata.jws_private_key = ""
    orch.settings.teradata.jws_cert = ""
    orch.settings.teradata.sslca = ""
    return orch


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTTUTools:
    def test_register_returns_dict(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)
        assert "ttu_execute" in tools
        assert callable(tools["ttu_execute"])

    async def test_check_installation_action(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        with patch(
            "teradata_etl_mcp_server.tools.ttu_tools.TTUClient.check_installation",
            return_value={"tbuild_installed": True, "bteq_installed": False, "tdload_installed": True, "any_installed": True},
        ):
            result = await tools["ttu_execute"](action="check_installation")

        assert result["success"] is True
        assert result["tbuild_installed"] is True

    async def test_execute_ddl_action(self):
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True,
            "job_name": "test_job",
            "returncode": 0,
            "stdout": "OK",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql_statements=["CREATE TABLE t (id INT)"],
            job_name="test_job",
        )

        assert result["success"] is True

    async def test_execute_ddl_via_sql_string(self):
        """Verify the sql (str) alias is normalized to sql_statements."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True,
            "job_name": "ddl_job",
            "returncode": 0,
            "stdout": "OK",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql="CREATE TABLE t (id INT)",
        )

        assert result["success"] is True
        # Verify the client received a list
        call_kwargs = orch.teradata_client.execute_statements.call_args[1]
        assert call_kwargs["sql_statements"] == ["CREATE TABLE t (id INT)"]

    async def test_execute_ddl_via_sql_multiple_statements(self):
        """Verify semicolon-separated sql string is split into a list."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True,
            "job_name": "ddl_job",
            "returncode": 0,
            "stdout": "OK",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql="CREATE TABLE a (id INT); CREATE TABLE b (id INT);",
        )

        assert result["success"] is True
        call_kwargs = orch.teradata_client.execute_statements.call_args[1]
        assert call_kwargs["sql_statements"] == [
            "CREATE TABLE a (id INT)",
            "CREATE TABLE b (id INT)",
        ]

    async def test_execute_ddl_sql_statements_takes_precedence(self):
        """sql_statements takes precedence over sql when both are provided."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True, "job_name": "j", "returncode": 0, "stdout": "", "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql_statements=["DROP TABLE x"],
            sql="CREATE TABLE y (id INT)",
            confirm=True,
        )

        assert result["success"] is True
        call_kwargs = orch.teradata_client.execute_statements.call_args[1]
        assert call_kwargs["sql_statements"] == ["DROP TABLE x"]

    async def test_execute_sql_action_routes_to_run_query(self):
        """Verify 'execute_sql' is accepted as alias for 'run_query'."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True, "returncode": 0, "stdout": "OK", "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_sql",
            sql="SELECT * FROM staging_db.t",
        )

        assert result["success"] is True
        orch.teradata_client.execute_statements.assert_called_once()

    async def test_run_sql_action_routes_to_run_query(self):
        """Verify 'run_sql' is accepted as alias for 'run_query'."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True, "returncode": 0, "stdout": "OK", "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](action="run_sql", sql="SELECT 1")

        assert result["success"] is True
        orch.teradata_client.execute_statements.assert_called_once()

    async def test_query_action_routes_to_run_query(self):
        """Verify 'query' is accepted as alias for 'run_query'."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True, "returncode": 0, "stdout": "OK", "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="query",
            sql="SELECT COUNT(*) FROM staging_db.t",
        )

        assert result["success"] is True
        orch.teradata_client.execute_statements.assert_called_once()

    async def test_check_action_routes_to_run_query(self):
        """Verify 'check' is accepted as alias for 'run_query'."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True, "returncode": 0, "stdout": "OK", "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="check",
            sql="SELECT 1 FROM dbc.TablesV WHERE DatabaseName='staging_db' AND TableName='t'",
        )

        assert result["success"] is True
        orch.teradata_client.execute_statements.assert_called_once()

    async def test_ddl_action_alias(self):
        """Verify 'ddl' is accepted as alias for 'execute_ddl'."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True, "job_name": "j", "returncode": 0, "stdout": "", "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](action="ddl", sql="ALTER TABLE t ADD col INT")

        assert result["success"] is True

    async def test_execute_ddl_rejects_select(self):
        """execute_ddl guards against SELECT statements and suggests execute_bteq."""
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql="SELECT * FROM staging_db.t",
        )

        assert result["success"] is False
        assert "run_query" in result["error"]
        orch.teradata_client.execute_statements.assert_not_called()

    async def test_execute_ddl_rejects_show(self):
        """execute_ddl guards against SHOW statements."""
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql="SHOW TABLE staging_db.t",
        )

        assert result["success"] is False
        assert "run_query" in result["error"]

    async def test_bteq_accepts_sql_statements(self):
        """execute_bteq normalizes sql_statements list into a script string."""
        orch = _make_mock_orchestrator()
        orch.ttu_client.execute_bteq.return_value = {
            "success": True, "returncode": 0, "stdout": "OK", "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_bteq",
            sql_statements=["SELECT 1", "SELECT 2"],
        )

        assert result["success"] is True
        call_kwargs = orch.ttu_client.execute_bteq.call_args[1]
        assert "SELECT 1" in call_kwargs["script"]
        assert "SELECT 2" in call_kwargs["script"]

    async def test_execute_ddl_missing_sql(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](action="execute_ddl")

        assert result["success"] is False
        assert "sql" in result["error"]

    async def test_load_data_action(self):
        orch = _make_mock_orchestrator()
        orch.ttu_client.execute_tdload.return_value = {
            "success": True,
            "job_name": "load_job",
            "mode": "file_to_table",
            "returncode": 0,
            "stdout": "OK",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="file_to_table",
            source_file_name="/data/input.csv",
            target_table="test_db.my_table",
        )

        assert result["success"] is True

    async def test_load_data_missing_mode(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](action="load_data")

        assert result["success"] is False
        assert "mode" in result["error"]

    async def test_cross_instance_table_to_table_rejects_non_td2_source(self):
        """When ``target_profile`` is given for ``table_to_table`` and the
        primary (source) profile uses JWT/SECRET/BEARER, the tool must
        return a clean error — tdload's Source* shim can only carry
        TD2/LDAP credentials, and silently proceeding would emit an empty
        SourceUserPassword and make tdload prompt on stdin.

        Regression guard for the Copilot-flagged bug.
        """
        orch = _make_mock_orchestrator()
        orch.credential_resolver.guard_configured.return_value = None

        # Two profiles: source=JWT on one host, target=TD2 on another.
        # Rule 4 requires both sides explicit; the cross-instance gate then
        # rejects the non-TD2/LDAP source.
        def _resolve(name):
            if name == "jwt_source":
                return {
                    "host": "src.example.com",
                    "username": "dbs_u",
                    "logdata": "eyJabc.def.ghi",
                    "port": 1025,
                    "logmech": "JWT",
                }
            if name == "td2_target":
                return {
                    "host": "tgt.example.com",
                    "username": "tgt_u",
                    "password": "tgt_p",
                    "database": "tgtdb",
                    "port": 1025,
                    "logmech": "TD2",
                }
            raise ValueError(f"unknown profile {name!r}")

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="srcdb.src",
            target_table="tgtdb.dst",
            teradata_profile="jwt_source",
            target_profile="td2_target",
        )
        assert result["success"] is False
        assert "JWT" in result["error"]
        # No tdload invocation because we bailed at the boundary.
        orch.ttu_client.execute_tdload.assert_not_called()

    async def test_create_table_honours_target_host_override_file_to_table(self):
        """The pre-create DDL must run against the SAME instance tdload
        writes to. When the caller supplies ``target_host`` (with or
        without ``target_username``/``target_password``), the CREATE
        TABLE connection must reflect that override — otherwise the
        table is created on one host and data loaded into another.

        Regression guard for the Copilot-flagged bug on the
        ``file_to_table`` DDL client ignoring target_* overrides.
        """
        from unittest.mock import patch as _patch
        orch = _make_mock_orchestrator()
        # Stub out the CSV analyzer so we don't touch disk.
        fake_col = MagicMock(name="col_id")
        fake_col.name = "id"
        fake_col.inferred_teradata_type = "INTEGER"
        fake_analysis = MagicMock(columns=[fake_col])
        orch.ttu_client.execute_tdload.return_value = {
            "success": True,
            "returncode": 0,
            "stdout": "Load complete",
            "stderr": "",
        }

        captured_auths: list = []

        class _FakeTdClient:
            def __init__(self, auth=None, **_kw):
                captured_auths.append(auth)
            def execute_statements(self, *args, **kwargs):
                return {"success": True}

        tools = register_ttu_tools(orch)
        with (
            _patch(
                "teradata_etl_mcp_server.utils.csv_analyzer.CSVAnalyzer.analyze_csv",
                return_value=fake_analysis,
            ),
            _patch(
                "teradata_etl_mcp_server.clients.teradata_client.TeradataClient",
                _FakeTdClient,
            ),
        ):
            result = await tools["ttu_execute"](
                action="load_data",
                mode="file_to_table",
                source_file_name="/tmp/fake.csv",
                target_table="tgtdb.t",
                create_table_if_not_exists=True,
                target_host="override.example.com",
                target_username="override_u",
                target_password="override_p",
            )
        assert result["success"] is True
        # The DDL client was built with a TeradataAuth reflecting the
        # explicit target_* overrides (not the wizard default).
        assert captured_auths, "TeradataClient never constructed for DDL"
        ddl_auth = captured_auths[-1]
        assert ddl_auth.host == "override.example.com"
        assert ddl_auth.username == "override_u"
        assert ddl_auth.password == "override_p"

    async def test_same_host_different_identity_rejects_non_td2_source(self):
        """If ``target_profile`` resolves to the **same** host as
        ``primary_auth`` but a different identity (user/mechanism),
        tdload still needs separate Source*/Target* credentials via the
        shim.  The Source* shim only carries TD2/LDAP, so a JWT source
        must be rejected — even on the same host.

        Regression guard: prior code only triggered the identity swap on
        different hosts, silently using source credentials for Target*
        when hosts matched but users differed.
        """
        orch = _make_mock_orchestrator()
        orch.credential_resolver.guard_configured.return_value = None

        # Source (JWT) and target (TD2) profiles both on td.example.com —
        # same host, different identity.
        def _resolve(name):
            if name == "jwt_source":
                return {
                    "host": "td.example.com",
                    "username": "dbs_u",
                    "logdata": "eyJabc.def.ghi",
                    "port": 1025,
                    "logmech": "JWT",
                }
            if name == "same_host_different_user":
                return {
                    "host": "td.example.com",
                    "username": "other_u",
                    "password": "other_p",
                    "database": "otherdb",
                    "port": 1025,
                    "logmech": "TD2",
                }
            raise ValueError(f"unknown profile {name!r}")

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="srcdb.src",
            target_table="tgtdb.dst",
            teradata_profile="jwt_source",
            target_profile="same_host_different_user",
        )
        # JWT source can't go through the Source* shim.
        assert result["success"] is False
        assert "JWT" in result["error"]
        orch.ttu_client.execute_tdload.assert_not_called()

    async def test_cross_instance_gate_honours_target_host_override(self):
        """Caller passes ``target_profile=X`` (resolves to host A) plus
        ``target_host=B`` (override). The cross-instance gate must compare
        against the EFFECTIVE target host (B), not the raw resolved host (A),
        otherwise it can spuriously reject same-host targets or fail to
        detect cross-host targets.

        Scenario: primary_auth.host = ``td.example.com`` (JWT). target_profile
        resolves to host=``td.example.com``. target_host=``other.example.com``
        overrides → effective target is cross-host → gate must trigger and
        reject the non-TD2/LDAP primary cleanly.
        """
        orch = _make_mock_orchestrator()
        orch.credential_resolver.guard_configured.return_value = None

        def _resolve(name):
            if name == "jwt_source":
                return {
                    "host": "td.example.com",
                    "username": "dbs_u",
                    "logdata": "eyJabc.def.ghi",
                    "port": 1025,
                    "logmech": "JWT",
                }
            if name == "td2_target_same_host":
                # Resolved host MATCHES primary, but target_host override
                # below moves the effective target to a different host.
                return {
                    "host": "td.example.com",
                    "username": "td_u",
                    "password": "td_p",
                    "database": "tdb",
                    "port": 1025,
                    "logmech": "TD2",
                }
            raise ValueError(f"unknown profile {name!r}")

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="srcdb.src",
            target_table="tgtdb.dst",
            teradata_profile="jwt_source",
            target_profile="td2_target_same_host",
            target_host="other.example.com",  # override → effective target
        )
        # The identities differ (JWT vs TD2) → needs_identity_swap
        # triggers → non-TD2/LDAP primary (JWT) is rejected.
        assert result["success"] is False
        assert "JWT" in result["error"]
        # tdload was NEVER invoked — gate fired before the subprocess layer.
        orch.ttu_client.execute_tdload.assert_not_called()

    async def test_target_host_override_back_to_same_host_still_checks_mechanism(self):
        """``target_profile`` resolves to a DIFFERENT host than primary,
        but ``target_host`` overrides it back to the SAME host. The hosts
        now match, but identities still differ (JWT vs TD2). The
        ``needs_identity_swap`` gate must still fire and reject the
        non-TD2/LDAP source.
        """
        orch = _make_mock_orchestrator()
        orch.credential_resolver.guard_configured.return_value = None

        def _resolve(name):
            if name == "jwt_source":
                return {
                    "host": "td.example.com",
                    "username": "dbs_u",
                    "logdata": "eyJabc.def.ghi",
                    "port": 1025,
                    "logmech": "JWT",
                }
            if name == "td2_target_other_host":
                return {
                    "host": "other.example.com",  # different host…
                    "username": "td_u",
                    "password": "td_p",
                    "database": "tdb",
                    "port": 1025,
                    "logmech": "TD2",
                }
            raise ValueError(f"unknown profile {name!r}")

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="srcdb.src",
            target_table="tgtdb.dst",
            teradata_profile="jwt_source",
            target_profile="td2_target_other_host",
            target_host="td.example.com",  # …override pulls it back to same
        )
        # Identities still differ (JWT vs TD2) → rejected.
        assert result["success"] is False
        assert "JWT" in result["error"]
        orch.ttu_client.execute_tdload.assert_not_called()

    async def test_same_host_different_td2_users_swaps_identity(self):
        """TD2 source + TD2 target on the **same** host but different users.
        The identity swap must route Target* to the target user and Source*
        to the source user — otherwise tdload's target-side pre-check
        authenticates as the wrong user.

        This is the core regression test for the same-host credential
        routing bug.
        """
        orch = _make_mock_orchestrator()
        orch.credential_resolver.guard_configured.return_value = None

        def _resolve(name):
            if name == "reader":
                return {
                    "host": "td.example.com",
                    "username": "reader_u",
                    "password": "reader_p",
                    "database": "srcdb",
                    "port": 1025,
                    "logmech": "TD2",
                }
            if name == "writer":
                return {
                    "host": "td.example.com",
                    "username": "writer_u",
                    "password": "writer_p",
                    "database": "tgtdb",
                    "port": 1025,
                    "logmech": "TD2",
                }
            raise ValueError(f"unknown profile {name!r}")

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        orch.ttu_client.execute_tdload.return_value = {
            "success": True,
            "returncode": 0,
            "stdout": "Load complete",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="srcdb.src",
            target_table="tgtdb.dst",
            teradata_profile="reader",
            target_profile="writer",
        )
        assert result["success"] is True, result
        orch.ttu_client.execute_tdload.assert_called_once()
        call_kwargs = orch.ttu_client.execute_tdload.call_args.kwargs
        # auth= should be the TARGET identity (writer)
        assert call_kwargs["auth"].username == "writer_u"
        assert call_kwargs["auth"].password == "writer_p"
        # Source* shim kwargs carry the SOURCE identity (reader)
        assert call_kwargs["source_host"] == "td.example.com"
        assert call_kwargs["source_username"] == "reader_u"
        assert call_kwargs["source_password"] == "reader_p"
        # source_mechanism tells the job-var builder the shim is TD2
        assert call_kwargs["source_mechanism"] == "TD2"

    async def test_same_identity_both_profiles_no_swap(self):
        """When both ``teradata_profile`` and ``target_profile`` resolve to
        the exact same identity, ``needs_identity_swap`` is False —
        no Source* shim kwargs emitted, auth stays as primary_auth.
        """
        orch = _make_mock_orchestrator()
        orch.credential_resolver.guard_configured.return_value = None

        def _resolve(name):
            # Both profiles resolve to the same identity.
            return {
                "host": "td.example.com",
                "username": "same_u",
                "password": "same_p",
                "database": "db",
                "port": 1025,
                "logmech": "TD2",
            }

        orch.credential_resolver.resolve_profile.side_effect = _resolve
        orch.ttu_client.execute_tdload.return_value = {
            "success": True,
            "returncode": 0,
            "stdout": "Load complete",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="db.src",
            target_table="db.dst",
            teradata_profile="profile_a",
            target_profile="profile_b",
        )
        assert result["success"] is True, result
        call_kwargs = orch.ttu_client.execute_tdload.call_args.kwargs
        # No identity swap — Source* shim kwargs should not be present.
        assert call_kwargs.get("source_host") is None
        assert call_kwargs.get("source_username") is None
        assert call_kwargs.get("source_password") is None
        assert call_kwargs.get("source_mechanism") is None

    async def test_table_to_table_rule7_missing_source_profile_returns_error(self):
        """Rule 4: ``table_to_table`` requires BOTH ``teradata_profile`` and
        ``target_profile`` to be explicitly named by the caller. Missing
        source → clean structured error before any auth resolution, so the
        LLM can ask the user which connection to use."""
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="src.t",
            target_table="tgt.t",
            target_profile="prod",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 4"
        assert "teradata_profile (source connection)" in result["missing"]
        assert "source connection" in result["error"]
        orch.ttu_client.execute_tdload.assert_not_called()

    async def test_table_to_table_rule7_missing_target_profile_returns_error(self):
        """Mirror of the source check — missing target is equally invalid."""
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="src.t",
            target_table="tgt.t",
            teradata_profile="dev",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 4"
        assert "target_profile (target connection)" in result["missing"]
        orch.ttu_client.execute_tdload.assert_not_called()

    async def test_table_to_table_rule7_missing_both_profiles(self):
        """Both omitted → error lists both missing params so the LLM knows
        what to ask the user for."""
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="src.t",
            target_table="tgt.t",
        )
        assert result["success"] is False
        assert result["rule"] == "Rule 4"
        assert len(result["missing"]) == 2
        assert "teradata_profile (source connection)" in result["missing"]
        assert "target_profile (target connection)" in result["missing"]
        orch.ttu_client.execute_tdload.assert_not_called()

    async def test_table_to_table_accepts_wizard_sentinel(self):
        """``teradata_profile="wizard"`` is the explicit-confirmation sentinel
        for the Settings-default identity. The Rule 4 gate must accept it
        (truthy string) and the resolver must fold it to the wizard auth.
        Proves the LLM can record an explicit "use wizard" choice without
        naming a real profile."""
        orch = _make_mock_orchestrator()
        orch.credential_resolver.guard_configured.return_value = None
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "tgt.example.com",
            "username": "tgt_u",
            "password": "tgt_p",
            "database": "tgtdb",
            "port": 1025,
            "logmech": "TD2",
        }
        orch.ttu_client.execute_tdload.return_value = {
            "success": True,
            "returncode": 0,
            "stdout": "Load complete",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="load_data",
            mode="table_to_table",
            source_table="src.t",
            target_table="tgt.t",
            teradata_profile="wizard",   # explicit wizard confirmation for source
            target_profile="td2_target",  # named profile for target
        )
        # Neither a Rule 4 reject nor a cross-instance mechanism reject.
        assert result.get("success") is True, result
        orch.ttu_client.execute_tdload.assert_called_once()
        # Resolver was called only for the named profile, not for the
        # sentinel — the sentinel folds to the Settings-default branch.
        orch.credential_resolver.resolve_profile.assert_called_once_with("td2_target")

    async def test_execute_bteq_action(self):
        orch = _make_mock_orchestrator()
        orch.ttu_client.execute_bteq.return_value = {
            "success": True,
            "returncode": 0,
            "stdout": "1 row selected",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_bteq",
            script="SELECT CURRENT_DATE;",
        )

        assert result["success"] is True

    async def test_execute_bteq_missing_script(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](action="execute_bteq")

        assert result["success"] is False
        assert "script" in result["error"]

    # ------------------------------------------------------------------ #
    #  Rule 6: connection_source tagging on failure                      #
    # ------------------------------------------------------------------ #

    async def test_failure_tagged_wizard_when_no_profile_named(self):
        """Reproduces the production bug from server logs.txt 2026-04-23:
        the tdload subprocess returns ``success=False`` (e.g. CLIv2 rejected
        the wizard's misconfigured auth). The tagger annotates with
        ``connection_source="wizard"`` and a Rule-8 hint so the LLM stops
        instead of pivoting to a profile.
        """
        orch = _make_mock_orchestrator()
        # Stub the actual call path used by ``action='execute_ddl'`` BEFORE
        # invoking the tool, so the test deterministically exercises the
        # Rule-6 tagging path without an unstubbed-MagicMock first call
        # masking real failures.
        orch.teradata_client.execute_statements.return_value = {
            "success": False,
            "error": "wizard JWT auth lacks username",
        }
        # ``execute_tdload`` isn't actually used by ``execute_ddl`` in this
        # path, but stubbing it documents the production-trace context the
        # test reproduces (tdload-style auth-missing failures).
        orch.ttu_client.execute_tdload.return_value = {
            "success": False,
            "returncode": 12,
            "stdout": "Syntax error in the job variable file. "
                      "Value must be specified for variable 'TARGETUSERNAME'.",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql_statements=["CREATE TABLE t (id INT)"],
        )
        assert result["success"] is False
        assert result["connection_source"] == "wizard"
        assert "Rule 6" in result["wizard_failure_hint"]
        assert "do NOT" in result["wizard_failure_hint"]

    async def test_failure_tagged_profile_when_profile_named(self):
        """Profile-resolved failure tags ``connection_source=profile:<name>``
        and DOES NOT carry the wizard_failure_hint (Rule 6 is wizard-only)."""
        orch = _make_mock_orchestrator()
        orch.credential_resolver.guard_configured.return_value = None
        # Resolve to a valid TD2 profile so auth resolution succeeds.
        orch.credential_resolver.resolve_profile.return_value = {
            "host": "h",
            "port": 1025,
            "username": "u",
            "password": "p",
            "logmech": "TD2",
        }
        # The downstream client returns failure (e.g. SQL syntax error).
        orch.teradata_client.execute_statements.return_value = {
            "success": False,
            "error": "syntax error near 'CREATE'",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql_statements=["CREATE TABLE t (id INT)"],
            teradata_profile="prod",
        )
        assert result["success"] is False
        assert result["connection_source"] == "profile:prod"
        assert "wizard_failure_hint" not in result

    async def test_wizard_sentinel_tagged_as_wizard_on_failure(self):
        """Passing the explicit-confirmation sentinel ``"wizard"`` (or
        ``"default"``) is semantically the same as silence — failures are
        tagged ``connection_source="wizard"`` so Rule 6 still applies."""
        from pydantic import SecretStr
        orch = _make_mock_orchestrator()
        orch.settings.teradata.logmech = "JWT"
        orch.settings.teradata.username = ""
        orch.settings.teradata.password = SecretStr("")
        orch.settings.teradata.logdata = SecretStr("eyJabc.def.ghi")
        orch.credential_resolver.guard_configured.return_value = None
        tools = register_ttu_tools(orch)

        for sentinel in ("wizard", "default", "Wizard", "  default  "):
            result = await tools["ttu_execute"](
                action="execute_ddl",
                sql_statements=["CREATE TABLE t (id INT)"],
                teradata_profile=sentinel,
            )
            # Sentinel resolves to wizard → JWT-no-username → fails
            # somewhere in the auth/tdload chain; whichever code path
            # produced the failure, the tag must read "wizard".
            if not result["success"]:
                assert result["connection_source"] == "wizard", sentinel

    async def test_success_response_not_tagged(self):
        """``_tag_failure`` is a no-op for successful responses — the
        connection_source field should NOT appear on success."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True,
            "returncode": 0,
            "stdout": "OK",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)
        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql_statements=["CREATE TABLE t (id INT)"],
        )
        assert result["success"] is True
        assert "connection_source" not in result
        assert "wizard_failure_hint" not in result

    async def test_unknown_action_returns_error(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](action="nonexistent_action")

        assert result["success"] is False
        assert "Unknown action" in result["error"]

    async def test_response_sanitized(self):
        """Verify credentials are stripped from the response."""
        orch = _make_mock_orchestrator()
        orch.ttu_client.execute_bteq.return_value = {
            "success": True,
            "returncode": 0,
            "stdout": "Result with password=s3cret in output",
            "stderr": "",
            "password": "s3cret",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_bteq",
            script="SELECT 1;",
        )

        # sanitize_response masks keys named 'password'
        assert result.get("password") != "s3cret"

    async def test_exception_returns_safe_error(self):
        """Verify exceptions are caught and returned as safe errors."""
        orch = _make_mock_orchestrator()
        orch.ttu_client.execute_bteq.side_effect = Exception("Connection refused")
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_bteq",
            script="SELECT 1;",
        )

        assert result["success"] is False
        assert "error" in result

    async def test_execute_ddl_drop_no_confirm_returns_warning(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql="DROP TABLE x",
        )

        assert result["success"] is False
        assert result["requires_confirmation"] is True
        assert result["action"] == "execute_ddl"
        assert "DROP TABLE x" in result["destructive_statements"][0]
        orch.teradata_client.execute_statements.assert_not_called()

    async def test_execute_ddl_truncate_no_confirm_returns_warning(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql="TRUNCATE TABLE x",
        )

        assert result["success"] is False
        assert result["requires_confirmation"] is True
        orch.teradata_client.execute_statements.assert_not_called()

    async def test_execute_ddl_delete_rejected_as_dml(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql="DELETE FROM x",
        )

        assert result["success"] is False
        assert "DML" in result["error"]
        assert "run_query" in result["error"]
        orch.teradata_client.execute_statements.assert_not_called()

    async def test_execute_ddl_create_no_confirm_executes(self):
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True, "job_name": "j", "returncode": 0, "stdout": "", "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql="CREATE TABLE t (id INT)",
        )

        assert result["success"] is True
        orch.teradata_client.execute_statements.assert_called_once()

    async def test_execute_ddl_mixed_no_confirm_returns_warning(self):
        orch = _make_mock_orchestrator()
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql_statements=["CREATE TABLE t (id INT)", "DROP TABLE old_t"],
        )

        assert result["success"] is False
        assert result["requires_confirmation"] is True
        assert len(result["destructive_statements"]) == 1
        assert "DROP TABLE old_t" in result["destructive_statements"][0]
        orch.teradata_client.execute_statements.assert_not_called()

    async def test_execute_bteq_falls_back_to_teradatasql(self):
        """When BTEQ is not installed, execute_bteq falls back to teradatasql."""
        orch = _make_mock_orchestrator()
        orch.ttu_client.execute_bteq.side_effect = TTUNotInstalledError("BTEQ not found")
        orch.teradata_client.execute_statements.return_value = {
            "success": True, "returncode": 0, "stdout": "OK", "stderr": "",
            "results": [], "statement_count": 1, "tolerated_errors": [],
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_bteq",
            script="SELECT CURRENT_DATE;",
        )

        assert result["success"] is True
        orch.teradata_client.execute_statements.assert_called_once()
        call_kwargs = orch.teradata_client.execute_statements.call_args[1]
        assert call_kwargs["sql_statements"] == ["SELECT CURRENT_DATE"]

    async def test_execute_ddl_always_uses_teradatasql(self):
        """execute_ddl always uses teradatasql directly, never TPT."""
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": True, "returncode": 0, "stdout": "OK", "stderr": "",
            "results": [], "statement_count": 1, "tolerated_errors": [],
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_ddl",
            sql="CREATE TABLE t (id INT)",
        )

        assert result["success"] is True
        orch.teradata_client.execute_statements.assert_called_once()
        orch.ttu_client.execute_tpt_ddl.assert_not_called()

    async def test_execute_bteq_uses_bteq_when_available(self):
        """When BTEQ is installed, execute_bteq uses ttu_client, not teradatasql."""
        orch = _make_mock_orchestrator()
        orch.ttu_client.execute_bteq.return_value = {
            "success": True, "returncode": 0, "stdout": "OK", "stderr": "",
        }
        tools = register_ttu_tools(orch)

        result = await tools["ttu_execute"](
            action="execute_bteq",
            script="SELECT 1;",
        )

        assert result["success"] is True
        orch.ttu_client.execute_bteq.assert_called_once()
        orch.teradata_client.execute_statements.assert_not_called()


class TestMloadLockDetection:
    def test_detect_lock_failure_2652(self):
        result = {
            "success": False,
            "stdout": "*** Failure 2652 Table sales_db.customers is being loaded",
            "stderr": "",
        }
        lock_info = _detect_mload_lock(result)
        assert lock_info is not None
        assert lock_info["lock_detected"] is True
        assert lock_info["table"] == "sales_db.customers"
        assert lock_info["requires_confirmation"] is True
        assert len(lock_info["remediation"]["steps"]) == 4

    def test_detect_lock_case_insensitive(self):
        result = {
            "success": False,
            "stdout": "*** failure 2583 TABLE Sales_DB.Orders is being loaded",
            "stderr": "",
        }
        lock_info = _detect_mload_lock(result)
        assert lock_info is not None
        assert lock_info["table"] == "Sales_DB.Orders"

    def test_detect_lock_quoted_table(self):
        result = {
            "success": False,
            "stdout": '*** Failure 2652 Table "my_db.my_table" is being loaded',
            "stderr": "",
        }
        lock_info = _detect_mload_lock(result)
        assert lock_info is not None
        assert lock_info["table"] == "my_db.my_table"

    def test_detect_lock_special_chars_in_name(self):
        result = {
            "success": False,
            "stdout": "*** Failure 2652 Table sales$db.tbl#1 is being loaded",
            "stderr": "",
        }
        lock_info = _detect_mload_lock(result)
        assert lock_info is not None
        assert lock_info["table"] == "sales$db.tbl#1"

    def test_detect_lock_placeholder_db_when_no_table(self):
        result = {
            "success": False,
            "stdout": "MLOAD lock detected on target",
            "stderr": "",
        }
        lock_info = _detect_mload_lock(result)
        assert lock_info is not None
        assert lock_info["table"] == "<table_name>"
        assert "<database_name>" in lock_info["remediation"]["steps"][0]["sql"]

    def test_no_lock_on_success(self):
        result = {
            "success": True,
            "stdout": "Job completed successfully",
            "stderr": "",
        }
        assert _detect_mload_lock(result) is None

    def test_no_lock_on_other_failure(self):
        result = {
            "success": False,
            "stdout": "*** Failure 3807 Object does not exist",
            "stderr": "",
        }
        assert _detect_mload_lock(result) is None

    def test_detect_lock_teradatasql_error_2652(self):
        result = {
            "success": False,
            "stdout": "",
            "stderr": "[Error 2652] Table sales_db.orders is being loaded",
        }
        lock_info = _detect_mload_lock(result)
        assert lock_info is not None
        assert lock_info["lock_detected"] is True

    def test_detect_lock_teradatasql_error_2583(self):
        result = {
            "success": False,
            "stdout": "",
            "stderr": "[Error 2583] Table staging_db.items already has a MultiLoad",
        }
        lock_info = _detect_mload_lock(result)
        assert lock_info is not None
        assert lock_info["lock_detected"] is True

    async def test_bteq_returns_lock_info(self):
        orch = _make_mock_orchestrator()
        orch.ttu_client.execute_bteq.return_value = {
            "success": False,
            "stdout": "*** Failure 2652 Table test_db.locked_tbl is being loaded",
            "stderr": "",
            "bteq_errors": ["*** Failure 2652"],
        }
        tools = register_ttu_tools(orch)
        result = await tools["ttu_execute"](action="execute_bteq", script="SELECT 1;")
        assert result["lock_detected"] is True
        assert result["table"] == "test_db.locked_tbl"

    async def test_ddl_returns_lock_info(self):
        orch = _make_mock_orchestrator()
        orch.teradata_client.execute_statements.return_value = {
            "success": False,
            "returncode": 8,
            "stdout": "apply phase not complete for table prod_db.events",
            "stderr": "",
        }
        tools = register_ttu_tools(orch)
        result = await tools["ttu_execute"](
            action="execute_ddl", sql="CREATE TABLE prod_db.events (id INT)"
        )
        assert result["lock_detected"] is True
        assert result["table"] == "prod_db.events"

    async def test_load_data_returns_lock_info(self):
        orch = _make_mock_orchestrator()
        orch.ttu_client.execute_tdload.return_value = {
            "success": False,
            "returncode": 8,
            "stdout": "*** Failure 2652 Table warehouse.inventory is being loaded",
            "stderr": "",
            "mode": "file_to_table",
        }
        tools = register_ttu_tools(orch)
        result = await tools["ttu_execute"](
            action="load_data",
            mode="file_to_table",
            source_file_name="/data/input.csv",
            target_table="warehouse.inventory",
        )
        assert result["lock_detected"] is True
        assert result["table"] == "warehouse.inventory"
