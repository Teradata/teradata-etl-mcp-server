"""Unit tests for TTU (Teradata Tools & Utilities) client."""

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from elt_mcp_server.auth import TeradataAuth
from elt_mcp_server.clients.ttu_client import (
    TTUClient,
    TTUCommandError,
    TTUNotInstalledError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(tmp_path: Path, **overrides) -> TTUClient:
    """Create a TTUClient with test defaults. TTUClient no longer holds
    identity — auth is passed per-call via :func:`_make_auth`."""
    defaults = {
        "scripts_dir": tmp_path / "scripts",
        "command_timeout": 30,
    }
    defaults.update(overrides)
    return TTUClient(**defaults)


def _make_auth(mechanism: str = "TD2") -> TeradataAuth:
    """Build a :class:`TeradataAuth` matching the TD2 fixture used across
    the pre-refactor tests."""
    return TeradataAuth(
        host="testhost.example.com",
        port=1025,
        database="",
        mechanism=mechanism,
        username="testuser",
        password="testpass123",
    )


def _mock_popen(returncode=0, stdout=b"OK", stderr=b""):
    """Create a mock Popen object."""
    mock_proc = MagicMock()
    mock_proc.communicate.return_value = (stdout, stderr)
    mock_proc.returncode = returncode
    return mock_proc


# ---------------------------------------------------------------------------
# Tests: check_installation
# ---------------------------------------------------------------------------


class TestCheckInstallation:
    @patch("shutil.which")
    def test_check_installation_found(self, mock_which):
        mock_which.side_effect = lambda x: f"/usr/bin/{x}" if "tbuild" in x or "bteq" in x or "tdload" in x else None
        result = TTUClient.check_installation(version="17.20")

        assert result["tbuild_installed"] is True
        assert result["bteq_installed"] is True
        assert result["tdload_installed"] is True
        assert result["any_installed"] is True
        assert result["version"] == "17.20"
        assert result["platform"] in ("Windows", "Linux", "Darwin")

    @patch("shutil.which", return_value=None)
    def test_check_installation_not_found(self, mock_which):
        result = TTUClient.check_installation(version="17.20")

        assert result["tbuild_installed"] is False
        assert result["bteq_installed"] is False
        assert result["tdload_installed"] is False
        assert result["any_installed"] is False
        assert "install_dir" in result

    def test_get_default_install_dir_linux(self):
        with patch("platform.system", return_value="Linux"):
            path = TTUClient.get_default_install_dir("17.20")
            assert path == Path("/opt/teradata/client/17.20/bin")

    def test_get_default_install_dir_windows(self):
        with patch("platform.system", return_value="Windows"):
            path = TTUClient.get_default_install_dir("17.10")
            assert path == Path(r"C:\Program Files\Teradata\Client\17.10\bin")

    def test_get_default_install_dir_macos(self):
        with patch("platform.system", return_value="Darwin"):
            path = TTUClient.get_default_install_dir("17.20")
            assert path == Path("/Library/Application Support/Teradata/client/17.20/bin")

    def test_get_binary_search_paths(self):
        with patch("platform.system", return_value="Linux"):
            paths = TTUClient.get_binary_search_paths("tbuild", "17.20")
            assert Path(paths[0]) == Path("/opt/teradata/client/17.20/bin/tbuild")
            assert paths[1] == "tbuild"

    @patch("shutil.which")
    def test_check_installation_uses_version(self, mock_which):
        """check_installation should search versioned paths for the given version."""
        calls = []
        mock_which.side_effect = lambda x: (calls.append(x), None)[1]

        TTUClient.check_installation(version="20.00")

        # Should have searched for versioned paths containing the version string
        joined = " ".join(calls)
        assert "20.00" in joined


# ---------------------------------------------------------------------------
# Tests: execute_tpt_ddl
# ---------------------------------------------------------------------------


class TestExecuteTPTDDL:
    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tbuild")
    def test_execute_tpt_ddl_success(self, mock_which, mock_popen, tmp_path):
        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"Job completed successfully")

        client = _make_client(tmp_path)
        result = client.execute_tpt_ddl(
            auth=_make_auth(),
            sql_statements=["CREATE TABLE test_db.my_table (id INT)"],
            job_name="test_job",
        )

        assert result["success"] is True
        assert result["job_name"] == "test_job"
        assert "Job completed successfully" in result["stdout"]
        mock_popen.assert_called_once()

    @patch("shutil.which", return_value=None)
    def test_execute_tpt_ddl_binary_not_found(self, mock_which, tmp_path):
        client = _make_client(tmp_path)

        with pytest.raises(TTUNotInstalledError, match="tbuild"):
            client.execute_tpt_ddl(auth=_make_auth(), sql_statements=["SELECT 1"])

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tbuild")
    def test_execute_tpt_ddl_timeout(self, mock_which, mock_popen, tmp_path):
        mock_proc = MagicMock()
        # First call raises TimeoutExpired, second call (cleanup) returns empty
        mock_proc.communicate.side_effect = [
            subprocess.TimeoutExpired(cmd="tbuild", timeout=30),
            (b"", b""),
        ]
        mock_proc.kill = Mock()
        mock_popen.return_value = mock_proc

        client = _make_client(tmp_path, command_timeout=1)

        with pytest.raises(TTUCommandError, match="timed out"):
            client.execute_tpt_ddl(auth=_make_auth(), sql_statements=["SELECT 1"])

    @patch("shutil.which", return_value="/usr/bin/tbuild")
    def test_execute_tpt_ddl_invalid_input(self, mock_which, tmp_path):
        client = _make_client(tmp_path)

        with pytest.raises(ValueError, match="non-empty list"):
            client.execute_tpt_ddl(auth=_make_auth(), sql_statements=[])


# ---------------------------------------------------------------------------
# Tests: execute_tdload
# ---------------------------------------------------------------------------


class TestExecuteTdload:
    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_execute_tdload_file_to_table(self, mock_which, mock_popen, tmp_path):
        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"Load complete")

        client = _make_client(tmp_path)
        result = client.execute_tdload(
            auth=_make_auth(),
            mode="file_to_table",
            source_file_name="/data/input.csv",
            target_table="test_db.my_table",
        )

        assert result["success"] is True
        assert result["mode"] == "file_to_table"

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_execute_tdload_table_to_file(self, mock_which, mock_popen, tmp_path):
        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"Export complete")

        client = _make_client(tmp_path)
        result = client.execute_tdload(
            auth=_make_auth(),
            mode="table_to_file",
            source_table="test_db.my_table",
            target_file_name="/data/output.csv",
        )

        assert result["success"] is True
        assert result["mode"] == "table_to_file"

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_execute_tdload_table_to_table(self, mock_which, mock_popen, tmp_path):
        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"Copy complete")

        client = _make_client(tmp_path)
        result = client.execute_tdload(
            auth=_make_auth(),
            mode="table_to_table",
            source_table="source_db.src_table",
            target_table="target_db.tgt_table",
        )

        assert result["success"] is True
        assert result["mode"] == "table_to_table"

    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_execute_tdload_invalid_mode(self, mock_which, tmp_path):
        client = _make_client(tmp_path)

        with pytest.raises(ValueError, match="Invalid mode"):
            client.execute_tdload(auth=_make_auth(), mode="invalid_mode")


class TestExecuteTdloadSaveTptScript:
    """``save_tpt_script=True`` captures the TPT script tdload generates
    internally under ``$TWB_ROOT/jobs/<job_name>/`` and copies a
    sanitized version into ``scripts_dir``. Captured only on success."""

    @staticmethod
    def _setup_twb_with_tpt(
        twb_root: Path, job_name: str, tpt_body: str
    ) -> Path:
        """Pre-create the TWB job directory with a fake TPT script."""
        job_dir = twb_root / "jobs" / job_name
        job_dir.mkdir(parents=True)
        tpt_file = job_dir / f"{job_name}.tpt"
        tpt_file.write_text(tpt_body, encoding="utf-8")
        return tpt_file

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_capture_tpt_when_present(
        self, mock_which, mock_popen, tmp_path, monkeypatch
    ):
        """Happy path: tdload succeeds, the TPT exists under TWB_ROOT,
        capture copies it to scripts_dir and returns the path."""
        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"OK")
        twb_root = tmp_path / "twb"
        monkeypatch.setenv("TWB_ROOT", str(twb_root))
        # Pre-create the TPT artifact at the deterministic job_name we'll pass.
        job_name = "elt_tdload_capturetest"
        tpt_body = (
            "DEFINE JOB AcmeMartLoad\n"
            "  ApplyName = 'load_op',\n"
            "  SourceTbl = 'src_table';\n"
        )
        self._setup_twb_with_tpt(twb_root, job_name, tpt_body)

        client = _make_client(tmp_path)
        result = client.execute_tdload(
            auth=_make_auth(),
            mode="file_to_table",
            source_file_name="/data/input.csv",
            target_table="test_db.my_table",
            save_tpt_script=True,
            job_name=job_name,
        )

        assert result["success"] is True
        assert "tpt_script_path" in result
        captured = Path(result["tpt_script_path"])
        assert captured.parent == (tmp_path / "scripts")
        assert captured.name == f"{job_name}.tpt"
        assert captured.exists()
        assert tpt_body in captured.read_text(encoding="utf-8")

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_capture_skipped_when_no_tpt_file(
        self, mock_which, mock_popen, tmp_path, monkeypatch
    ):
        """Empty job dir → no `tpt_script_path` field, no exception."""
        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"OK")
        twb_root = tmp_path / "twb"
        # Create the job dir but no .tpt inside it.
        (twb_root / "jobs" / "elt_tdload_emptyjob").mkdir(parents=True)
        monkeypatch.setenv("TWB_ROOT", str(twb_root))

        client = _make_client(tmp_path)
        result = client.execute_tdload(
            auth=_make_auth(),
            mode="file_to_table",
            source_file_name="/data/input.csv",
            target_table="test_db.my_table",
            save_tpt_script=True,
            job_name="elt_tdload_emptyjob",
        )

        assert result["success"] is True
        assert "tpt_script_path" not in result

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_capture_skipped_when_tdload_failed(
        self, mock_which, mock_popen, tmp_path, monkeypatch
    ):
        """Failed tdload run → no capture even if TPT file exists."""
        mock_popen.return_value = _mock_popen(returncode=12, stdout=b"oops")
        twb_root = tmp_path / "twb"
        monkeypatch.setenv("TWB_ROOT", str(twb_root))
        job_name = "elt_tdload_failedrun"
        self._setup_twb_with_tpt(
            twb_root, job_name, "DEFINE JOB Failed; ...\n"
        )

        client = _make_client(tmp_path)
        result = client.execute_tdload(
            auth=_make_auth(),
            mode="file_to_table",
            source_file_name="/data/input.csv",
            target_table="test_db.my_table",
            save_tpt_script=True,
            job_name=job_name,
        )

        assert result["success"] is False
        assert "tpt_script_path" not in result

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_capture_sanitizes_inline_creds(
        self, mock_which, mock_popen, tmp_path, monkeypatch
    ):
        """Defense in depth — if the TPT script contains a literal
        password match, it must be replaced before persistence. Real
        tdload-generated TPTs typically pull creds from env, but we
        scrub regardless."""
        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"OK")
        twb_root = tmp_path / "twb"
        monkeypatch.setenv("TWB_ROOT", str(twb_root))
        job_name = "elt_tdload_sanitytest"
        # Inject the auth's password verbatim into the fake TPT so the
        # sanitizer has something to scrub.
        tpt_body = (
            "DEFINE JOB AcmeMartLoad\n"
            "  UserName = 'testuser',\n"
            "  UserPassword = 'testpass123';\n"
        )
        self._setup_twb_with_tpt(twb_root, job_name, tpt_body)

        client = _make_client(tmp_path)
        result = client.execute_tdload(
            auth=_make_auth(),
            mode="file_to_table",
            source_file_name="/data/input.csv",
            target_table="test_db.my_table",
            save_tpt_script=True,
            job_name=job_name,
        )

        assert result["success"] is True
        captured_text = Path(result["tpt_script_path"]).read_text(
            encoding="utf-8"
        )
        # The inline credential MUST NOT survive the sanitizer.
        assert "testpass123" not in captured_text
        assert "<PASSWORD>" in captured_text


# ---------------------------------------------------------------------------
# Tests: tdload job-var rendering honours mechanism-specific invariants
# ---------------------------------------------------------------------------


class TestTdloadJobVarRendering:
    """Tests the job-var dict content for each mechanism × mode combo.

    Without these, regressions in :meth:`TTUClient._prepare_tdload_job_var`
    sail through the mode-level execution tests (which only assert exit
    status), because subprocess is mocked and the actual job-var content
    never gets inspected.
    """

    def test_td2_file_to_table_renders_user_password(self, tmp_path):
        from elt_mcp_server.auth import TeradataAuth
        client = _make_client(tmp_path)
        auth = TeradataAuth(
            host="h", port=1025, database="db",
            mechanism="TD2", username="u", password="p",
        )
        content = client._prepare_tdload_job_var(
            auth, "file_to_table",
            source_file_name="/data/x.csv",
            target_table="db.tbl",
        )
        assert "TargetUserName='u'" in content
        assert "TargetUserPassword='p'" in content
        assert "TargetTdpId='h'" in content
        # TD2 is implicit — no TargetLogonMech emitted.
        assert "TargetLogonMech" not in content

    def test_jwt_file_to_table_omits_password(self, tmp_path):
        """JWT must NOT emit TargetUserPassword — tdload would prompt on
        stdin and hang. See reference doc matrix."""
        from elt_mcp_server.auth import TeradataAuth
        client = _make_client(tmp_path)
        auth = TeradataAuth(
            host="h", port=1025, database="db",
            mechanism="JWT", username="dbs_u", logdata="eyJhbGci.x.y",
        )
        content = client._prepare_tdload_job_var(
            auth, "file_to_table",
            source_file_name="/data/x.csv",
            target_table="db.tbl",
        )
        assert "TargetLogonMech='JWT'" in content
        assert "TargetLogonMechData='token=eyJhbGci.x.y'" in content
        assert "TargetUserName='dbs_u'" in content
        # Absence is the safety-critical assertion here.
        assert "TargetUserPassword" not in content

    def test_secret_file_to_table_uses_clientid_as_username(self, tmp_path):
        """SECRET wire form (tdload argv): user=client_id, LogonMechData=bare secret.
        Distinct from BTEQ's OIDC-grant form."""
        from elt_mcp_server.auth import TeradataAuth
        client = _make_client(tmp_path)
        auth = TeradataAuth(
            host="h", port=1025, database="db",
            mechanism="SECRET", oidc_clientid="client-id-1", logdata="the-secret",
        )
        content = client._prepare_tdload_job_var(
            auth, "file_to_table",
            source_file_name="/data/x.csv",
            target_table="db.tbl",
        )
        assert "TargetLogonMech='SECRET'" in content
        assert "TargetUserName='client-id-1'" in content
        assert "TargetLogonMechData='the-secret'" in content
        assert "TargetUserPassword" not in content

    def test_bearer_rejected_on_tdload(self, tmp_path):
        """BEARER requires CLIv2 config file (clispb.dat), can't go on argv."""
        from elt_mcp_server.auth import AuthUnsupportedError, TeradataAuth
        client = _make_client(tmp_path)
        auth = TeradataAuth(
            host="h", port=1025, database="db",
            mechanism="BEARER",
            oidc_clientid="c", jws_private_key="/k",
        )
        with pytest.raises(AuthUnsupportedError, match="BEARER"):
            client._prepare_tdload_job_var(
                auth, "file_to_table",
                source_file_name="/data/x.csv",
                target_table="db.tbl",
            )

    def test_table_to_table_jwt_target_rejects_target_password_override(
        self, tmp_path
    ):
        """The TD2-shim ``target_password`` kwarg must NOT clobber the
        renderer's JWT output — otherwise tdload would try to use the
        injected password and break the JWT logon.

        Regression guard for the post-review bug.
        """
        from elt_mcp_server.auth import TeradataAuth
        client = _make_client(tmp_path)
        jwt_target = TeradataAuth(
            host="h", port=1025, database="db",
            mechanism="JWT", username="dbs_u", logdata="eyJx.y.z",
        )
        content = client._prepare_tdload_job_var(
            jwt_target, "table_to_table",
            source_host="src.example.com",
            source_username="src_u",
            source_password="src_p",
            target_password="leftover_td2_password",  # legacy kwarg — must be ignored
            source_table="srcdb.src",
            target_table="dstdb.dst",
        )
        assert "TargetLogonMech='JWT'" in content
        assert "TargetLogonMechData='token=eyJx.y.z'" in content
        # The legacy target_password kwarg must be ignored for non-TD2.
        assert "leftover_td2_password" not in content
        assert "TargetUserPassword" not in content

    def test_table_to_table_cross_instance_td2(self, tmp_path):
        """Cross-instance TD2 still uses the legacy Source* kwarg shim."""
        from elt_mcp_server.auth import TeradataAuth
        client = _make_client(tmp_path)
        td2_target = TeradataAuth(
            host="tgt.example.com", port=1025, database="tgtdb",
            mechanism="TD2", username="tgt_u", password="tgt_p",
        )
        content = client._prepare_tdload_job_var(
            td2_target, "table_to_table",
            source_host="src.example.com",
            source_username="src_u",
            source_password="src_p",
            source_table="srcdb.src",
            target_table="tgtdb.dst",
        )
        assert "SourceTdpId='src.example.com'" in content
        assert "SourceUserName='src_u'" in content
        assert "SourceUserPassword='src_p'" in content
        assert "TargetTdpId='tgt.example.com'" in content
        assert "TargetUserName='tgt_u'" in content
        assert "TargetUserPassword='tgt_p'" in content

    def test_cross_instance_jwt_target_with_td2_source_shim_honored(
        self, tmp_path
    ):
        """When target_auth is JWT/SECRET but source_* kwargs carry TD2
        source credentials, the shim must fire based on ``source_mechanism``
        (the SOURCE mechanism), not ``auth.mechanism`` (the TARGET).

        Prior gate used ``auth.mechanism in ('TD2', 'LDAP')`` which checks
        the TARGET's mechanism — wrong for cross-instance where auth IS
        target and source_* kwargs carry a different mechanism's identity.

        Regression guard for the Copilot-flagged bug.
        """
        from elt_mcp_server.auth import TeradataAuth
        client = _make_client(tmp_path)
        jwt_target = TeradataAuth(
            host="tgt.example.com", port=1025, database="tgtdb",
            mechanism="JWT", username="tgt_dbs_u", logdata="eyJtgt.x.y",
        )
        content = client._prepare_tdload_job_var(
            jwt_target, "table_to_table",
            source_mechanism="TD2",  # tool layer passes this for cross-instance
            source_host="src.example.com",
            source_username="src_u",
            source_password="src_p",
            source_table="srcdb.src",
            target_table="tgtdb.dst",
        )
        # Source uses TD2 shim (from kwargs, not from target_auth rendering).
        assert "SourceTdpId='src.example.com'" in content
        assert "SourceUserName='src_u'" in content
        assert "SourceUserPassword='src_p'" in content
        assert "SourceLogonMech" not in content
        # Target uses JWT from auth (no password, LogonMech=JWT).
        assert "TargetLogonMech='JWT'" in content
        assert "TargetLogonMechData='token=eyJtgt.x.y'" in content
        assert "TargetUserName='tgt_dbs_u'" in content
        assert "TargetUserPassword" not in content

    def test_execute_tpt_ddl_script_includes_logonmech_attributes(
        self, tmp_path
    ):
        """The TPT DDL script references ``LogonMech`` and ``LogonMechData``
        attributes (per TPT Reference B035-2436 Chapter 4) so tbuild can
        log on with LDAP/JWT/SECRET — not just TD2. Env vars populated by
        ``render_for_tdload`` flow into the script's ``@LogonMech`` /
        ``@LogonMechData`` references.

        Regression guard for the TPT DDL non-TD2 support.
        """
        client = _make_client(tmp_path)
        script = client._prepare_tpt_ddl_script(
            ["CREATE TABLE t (id INT)"], "test_job", error_list=None,
        )
        assert "TdpId = @TdpId" in script
        assert "UserName = @UserName" in script
        assert "UserPassword = @UserPassword" in script
        assert "LogonMech = @LogonMech" in script
        assert "LogonMechData = @LogonMechData" in script

    @pytest.mark.parametrize(
        "mechanism,extra",
        [
            ("TD2", {"username": "u", "password": "p"}),
            ("LDAP", {"username": "ldap_u", "password": "ldap_p"}),
            ("JWT", {"username": "jwt_u", "logdata": "eyJabc.x.y"}),
            ("SECRET", {"oidc_clientid": "cid", "logdata": "the_secret"}),
        ],
    )
    def test_execute_tpt_ddl_env_carries_logonmech_for_each_mechanism(
        self, tmp_path, mechanism, extra, monkeypatch
    ):
        """render_for_tdload's env output populates the tbuild subprocess
        env with the right ``LogonMech``/``LogonMechData`` for each
        mechanism. Verified via the auth.render_for_tdload() contract;
        tbuild itself is not spawned in this test."""
        from elt_mcp_server.auth import TeradataAuth
        auth = TeradataAuth(
            host="h", port=1025, database="db",
            mechanism=mechanism, **extra,
        )
        rendering = auth.render_for_tdload()
        assert "LogonMech" in rendering.env_vars
        assert rendering.env_vars["LogonMech"] == mechanism
        # JWT/SECRET populate LogonMechData with the token / client secret;
        # TD2/LDAP have empty LogonMechData (TD2 uses no external data;
        # LDAP's CLIv2 default packages user/pw itself).
        if mechanism == "JWT":
            assert rendering.env_vars["LogonMechData"].startswith("token=")
        elif mechanism == "SECRET":
            assert rendering.env_vars["LogonMechData"] == "the_secret"
        else:
            assert rendering.env_vars["LogonMechData"] == ""

    def test_execute_tpt_ddl_still_rejects_bearer(self, tmp_path):
        """BEARER still rejected via render_for_tdload — requires CLIv2
        config (clispb.dat with jws_private_key/jws_cert) that cannot be
        expressed via TPT attributes or argv."""
        from elt_mcp_server.auth import AuthUnsupportedError, TeradataAuth
        client = _make_client(tmp_path)
        bearer_auth = TeradataAuth(
            host="h", port=1025, database="db",
            mechanism="BEARER",
            oidc_clientid="c", jws_private_key="/k",
        )
        with pytest.raises(AuthUnsupportedError, match="BEARER"):
            client.execute_tpt_ddl(
                auth=bearer_auth,
                sql_statements=["CREATE TABLE t (id INT)"],
            )

    def test_table_to_file_jwt_source_ignores_td2_shim_kwargs(self, tmp_path):
        """Non-TD2 auth for the source side must use the renderer's output
        (no ``SourceUserPassword`` for JWT). The TD2-shim kwargs
        ``source_host``/``source_username``/``source_password`` are only
        honored for TD2/LDAP; for JWT/SECRET/BEARER they are ignored in
        favour of ``auth.render_for_tdload(prefix='Source')``.

        Regression guard for the Copilot-flagged bug.
        """
        from elt_mcp_server.auth import TeradataAuth
        client = _make_client(tmp_path)
        jwt_source = TeradataAuth(
            host="src.example.com", port=1025, database="srcdb",
            mechanism="JWT", username="dbs_u", logdata="eyJabc.def.ghi",
        )
        content = client._prepare_tdload_job_var(
            jwt_source, "table_to_file",
            source_table="srcdb.tbl",
            target_file_name="/tmp/out.csv",
            # Legacy TD2-shim kwargs that must be IGNORED for JWT.
            source_host="ignored_host",
            source_username="ignored_u",
            source_password="leftover_td2_password",
        )
        assert "SourceLogonMech='JWT'" in content
        assert "SourceLogonMechData='token=eyJabc.def.ghi'" in content
        assert "SourceUserName='dbs_u'" in content
        # Critical: no SourceUserPassword for JWT (would prompt/hang tdload).
        assert "SourceUserPassword" not in content
        # And legacy kwargs must not leak in.
        assert "ignored_host" not in content
        assert "ignored_u" not in content
        assert "leftover_td2_password" not in content


# ---------------------------------------------------------------------------
# Tests: execute_bteq
# ---------------------------------------------------------------------------


class TestExecuteBTEQ:
    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/bteq")
    def test_execute_bteq_success(self, mock_which, mock_popen, tmp_path):
        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"SELECT result\n1 row")

        client = _make_client(tmp_path)
        result = client.execute_bteq(auth=_make_auth(), script="SELECT CURRENT_DATE;")

        assert result["success"] is True
        # Verify .LOGON was injected via stdin
        call_args = mock_popen.return_value.communicate.call_args
        stdin_data = call_args[1].get("input") or call_args[0][0]
        stdin_text = stdin_data.decode("utf-8") if isinstance(stdin_data, bytes) else stdin_data
        assert ".LOGON" in stdin_text
        assert ".EXIT" in stdin_text

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/bteq")
    def test_execute_bteq_detects_errors(self, mock_which, mock_popen, tmp_path):
        mock_popen.return_value = _mock_popen(
            returncode=0,
            stdout=b"*** Failure 3807 Object 'missing_table' does not exist.\n*** Error",
        )

        client = _make_client(tmp_path)
        result = client.execute_bteq(auth=_make_auth(), script="SELECT * FROM missing_table;")

        assert result["success"] is False
        assert "bteq_errors" in result
        assert len(result["bteq_errors"]) >= 1

    @patch("shutil.which", return_value=None)
    def test_execute_bteq_binary_not_found(self, mock_which, tmp_path):
        client = _make_client(tmp_path)

        with pytest.raises(TTUNotInstalledError, match="bteq"):
            client.execute_bteq(auth=_make_auth(), script="SELECT 1;")


# ---------------------------------------------------------------------------
# Tests: Security
# ---------------------------------------------------------------------------


class TestSecurity:
    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tbuild")
    def test_secure_delete_called(self, mock_which, mock_popen, tmp_path):
        """Verify temp file is removed after execution."""
        mock_popen.return_value = _mock_popen(returncode=0)

        client = _make_client(tmp_path)
        import glob
        import tempfile as _tempfile

        isolated_tmp = tmp_path / "tempfiles"
        isolated_tmp.mkdir()
        _orig_mkstemp = _tempfile.mkstemp

        def _isolated_mkstemp(prefix="tmp", suffix="", dir=None):
            return _orig_mkstemp(prefix=prefix, suffix=suffix, dir=str(isolated_tmp))

        with patch("elt_mcp_server.clients.ttu_client.tempfile.mkstemp", side_effect=_isolated_mkstemp):
            client.execute_tpt_ddl(auth=_make_auth(), sql_statements=["SELECT 1"])

        remaining = glob.glob(os.path.join(str(isolated_tmp), "tpt_ddl_*"))
        assert len(remaining) == 0

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tbuild")
    def test_credentials_not_in_exceptions(self, mock_which, mock_popen, tmp_path):
        """Verify credential strings are absent from raised errors."""
        mock_proc = MagicMock()
        mock_proc.communicate.side_effect = Exception("Connection to testpass123 failed")
        mock_popen.return_value = mock_proc

        client = _make_client(tmp_path)

        with pytest.raises(TTUCommandError) as exc_info:
            client.execute_tpt_ddl(auth=_make_auth(), sql_statements=["SELECT 1"])

        assert "testpass123" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# Tests: save_script
# ---------------------------------------------------------------------------


class TestSaveScript:
    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tbuild")
    def test_save_script_creates_file(self, mock_which, mock_popen, tmp_path):
        """Verify save_script=True writes a script file to scripts_dir."""
        mock_popen.return_value = _mock_popen(returncode=0)

        client = _make_client(tmp_path)
        result = client.execute_tpt_ddl(
            auth=_make_auth(),
            sql_statements=["CREATE TABLE t (id INT)"],
            save_script=True,
        )

        assert "script_path" in result
        script_path = Path(result["script_path"])
        assert script_path.exists()

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tbuild")
    def test_save_script_strips_credentials(self, mock_which, mock_popen, tmp_path):
        """Verify saved script uses TPT variable references, not real credentials."""
        mock_popen.return_value = _mock_popen(returncode=0)

        client = _make_client(tmp_path)
        result = client.execute_tpt_ddl(
            auth=_make_auth(),
            sql_statements=["CREATE TABLE t (id INT)"],
            save_script=True,
        )

        script_content = Path(result["script_path"]).read_text()
        # Credentials should never appear in saved scripts
        assert "testpass123" not in script_content
        # TPT variable references should be used instead of embedded credentials
        assert "@UserPassword" in script_content
        assert "@UserName" in script_content
        assert "@TdpId" in script_content

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tbuild")
    def test_save_script_false_no_file(self, mock_which, mock_popen, tmp_path):
        """Verify save_script=False does not leave script files in scripts_dir."""
        mock_popen.return_value = _mock_popen(returncode=0)

        scripts_dir = tmp_path / "scripts"
        client = _make_client(tmp_path, scripts_dir=scripts_dir)
        client.execute_tpt_ddl(auth=_make_auth(), sql_statements=["SELECT 1"], save_script=False)

        if scripts_dir.exists():
            files = list(scripts_dir.iterdir())
            assert len(files) == 0

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tbuild")
    def test_save_script_returns_path(self, mock_which, mock_popen, tmp_path):
        """Verify result dict includes script_path when save_script=True."""
        mock_popen.return_value = _mock_popen(returncode=0)

        client = _make_client(tmp_path)
        result = client.execute_tpt_ddl(
            auth=_make_auth(),
            sql_statements=["SELECT 1"],
            save_script=True,
        )

        assert "script_path" in result
        assert result["script_path"].endswith(".tpt")

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/bteq")
    def test_save_script_bteq(self, mock_which, mock_popen, tmp_path):
        """Verify save_script works for BTEQ scripts."""
        mock_popen.return_value = _mock_popen(returncode=0)

        client = _make_client(tmp_path)
        result = client.execute_bteq(auth=_make_auth(), script="SELECT 1;", save_script=True)

        assert "script_path" in result
        script_content = Path(result["script_path"]).read_text()
        assert "testpass123" not in script_content
        assert "<PASSWORD>" in script_content


# ---------------------------------------------------------------------------
# Tests: _run_subprocess env strip (Copilot-flagged env leak)
# ---------------------------------------------------------------------------


class TestRunSubprocessEnvStrip:
    """``_run_subprocess`` must strip CLIv2 identity env vars from the
    inherited parent shell before merging ``env_override`` — otherwise a
    stale ``UserPassword`` exported in the user's shell would shadow a JWT
    renderer that deliberately omits it, making tdload prompt on stdin
    or reject the logon.
    """

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_parent_shell_userpassword_is_stripped_for_jwt(
        self, mock_which, mock_popen, tmp_path, monkeypatch
    ):
        # Simulate a user who exported TD2 creds in their shell.
        monkeypatch.setenv("UserPassword", "shell_stale_pw")
        monkeypatch.setenv("LogonMechData", "stale_logdata")
        monkeypatch.setenv("UNRELATED_VAR", "keep_me")

        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"OK")

        client = _make_client(tmp_path)
        jwt_auth = TeradataAuth(
            host="td.example.com",
            port=1025,
            database="",
            mechanism="JWT",
            username="jwt_user",
            logdata="eyJabc.payload.sig",
        )
        client.execute_tdload(
            auth=jwt_auth,
            mode="file_to_table",
            source_file_name="/data/x.csv",
            target_table="db.t",
        )

        mock_popen.assert_called_once()
        env = mock_popen.call_args.kwargs["env"]
        # Stale TD2 password must not leak into the JWT subprocess.
        assert env.get("UserPassword") != "shell_stale_pw"
        assert "UserPassword" not in env  # renderer omits it for JWT
        # Renderer's LogonMechData wins over parent shell's stale one.
        assert env["LogonMechData"] == "token=eyJabc.payload.sig"
        assert env["LogonMech"] == "JWT"
        assert env["UserName"] == "jwt_user"
        # Unrelated parent-shell vars are still inherited.
        assert env.get("UNRELATED_VAR") == "keep_me"

    @patch("subprocess.Popen")
    @patch("shutil.which", return_value="/usr/bin/tdload")
    def test_parent_shell_identity_keys_stripped_even_for_td2(
        self, mock_which, mock_popen, tmp_path, monkeypatch
    ):
        # A user with a different stale UserPassword in their shell —
        # auth override must win unconditionally.
        monkeypatch.setenv("UserPassword", "stale_pw")
        monkeypatch.setenv("UserName", "stale_user")

        mock_popen.return_value = _mock_popen(returncode=0, stdout=b"OK")

        client = _make_client(tmp_path)
        td2_auth = _make_auth()  # password=testpass123, user=testuser
        client.execute_tdload(
            auth=td2_auth,
            mode="file_to_table",
            source_file_name="/data/x.csv",
            target_table="db.t",
        )

        mock_popen.assert_called_once()
        env = mock_popen.call_args.kwargs["env"]
        assert env["UserPassword"] == "testpass123"
        assert env["UserName"] == "testuser"
