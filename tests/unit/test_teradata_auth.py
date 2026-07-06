"""Unit tests for :class:`TeradataAuth` — the architectural invariant.

Covers: construction/validation per mechanism, and every renderer method's
exact wire format. If a renderer output changes shape, this suite catches
it before the client/tool/generator code built on top silently drifts.
"""

from __future__ import annotations

import pytest

from elt_mcp_server.auth import AuthUnsupportedError, TeradataAuth

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _td2() -> TeradataAuth:
    return TeradataAuth(
        host="td.example.com", port=1025, database="testdb",
        mechanism="TD2", username="dbc", password="dbc_pw",
    )


def _ldap() -> TeradataAuth:
    return TeradataAuth(
        host="td.example.com", port=1025, database="testdb",
        mechanism="LDAP", username="ldap_user", password="ldap_pw",
    )


def _jwt() -> TeradataAuth:
    return TeradataAuth(
        host="td.example.com", port=1025, database="testdb",
        mechanism="JWT", username="dbs_user", logdata="eyJhbGciOi.payload.sig",
    )


def _secret() -> TeradataAuth:
    return TeradataAuth(
        host="lake.example.com", port=443, database="lake_db",
        mechanism="SECRET", oidc_clientid="my_client", logdata="the_client_secret",
    )


def _bearer() -> TeradataAuth:
    return TeradataAuth(
        host="lake.example.com", port=443, database="lake_db",
        mechanism="BEARER",
        oidc_clientid="my_client",
        jws_private_key="/etc/secrets/jws_priv.pem",
        jws_cert="/etc/secrets/jws_cert.pem",
        sslca="/etc/ssl/cacerts.pem",
    )


# ---------------------------------------------------------------------------
# Validation (__post_init__)
# ---------------------------------------------------------------------------


class TestValidation:
    def test_unknown_mechanism_rejected(self):
        with pytest.raises(ValueError, match="Unknown mechanism"):
            TeradataAuth(host="h", port=1025, database="", mechanism="NTLM")  # type: ignore[arg-type]

    @pytest.mark.parametrize("mech", ["TD2", "LDAP"])
    def test_td2_ldap_require_username_and_password(self, mech):
        with pytest.raises(ValueError, match=f"{mech} requires username"):
            TeradataAuth(host="h", port=1025, database="", mechanism=mech, password="p")
        with pytest.raises(ValueError, match=f"{mech} requires password"):
            TeradataAuth(host="h", port=1025, database="", mechanism=mech, username="u")

    def test_jwt_requires_logdata(self):
        with pytest.raises(ValueError, match="JWT requires logdata"):
            TeradataAuth(host="h", port=1025, database="", mechanism="JWT", username="u")

    def test_jwt_logdata_gets_token_prefix(self):
        a = TeradataAuth(
            host="h", port=1025, database="", mechanism="JWT",
            username="u", logdata="eyJhbGci.x.y",
        )
        assert a.logdata == "token=eyJhbGci.x.y"

    def test_jwt_logdata_with_existing_prefix_unchanged(self):
        a = TeradataAuth(
            host="h", port=1025, database="", mechanism="JWT",
            username="u", logdata="token=eyJhbGci.x.y",
        )
        assert a.logdata == "token=eyJhbGci.x.y"

    def test_secret_requires_clientid_and_secret(self):
        with pytest.raises(ValueError, match="SECRET requires oidc_clientid"):
            TeradataAuth(host="h", port=1025, database="", mechanism="SECRET", logdata="s")
        with pytest.raises(ValueError, match="SECRET requires logdata"):
            TeradataAuth(host="h", port=1025, database="", mechanism="SECRET", oidc_clientid="c")

    def test_bearer_requires_clientid_and_private_key(self):
        with pytest.raises(ValueError, match="BEARER requires oidc_clientid"):
            TeradataAuth(
                host="h", port=1025, database="", mechanism="BEARER",
                jws_private_key="/k",
            )
        with pytest.raises(ValueError, match="BEARER requires jws_private_key"):
            TeradataAuth(
                host="h", port=1025, database="", mechanism="BEARER",
                oidc_clientid="c",
            )

    def test_immutable(self):
        a = _td2()
        with pytest.raises(Exception):  # dataclass frozen → FrozenInstanceError
            a.username = "other"  # type: ignore[misc]

    @pytest.mark.parametrize("raw", ["None", "none", "NONE", " None ", "\tnone\n"])
    def test_database_string_none_normalised_to_empty(self, raw: str):
        """pydantic env-loading (and .env files) can surface the literal
        string ``"None"`` when the operator leaves a default-database field
        blank. The pre-refactor ``TeradataClient`` silently mapped that to
        empty; the normalisation now lives on ``TeradataAuth`` so every
        renderer (teradatasql/tdload/dbt) gets the same treatment.
        """
        a = TeradataAuth(
            host="h", port=1025, database=raw,
            mechanism="TD2", username="u", password="p",
        )
        assert a.database == ""

    def test_database_whitespace_is_stripped(self):
        a = TeradataAuth(
            host="h", port=1025, database="  my_db  ",
            mechanism="TD2", username="u", password="p",
        )
        assert a.database == "my_db"

    def test_database_real_value_preserved(self):
        a = TeradataAuth(
            host="h", port=1025, database="analytics",
            mechanism="TD2", username="u", password="p",
        )
        assert a.database == "analytics"


# ---------------------------------------------------------------------------
# render_for_tdload
# ---------------------------------------------------------------------------


class TestRenderForTdload:
    def test_td2(self):
        r = _td2().render_for_tdload()
        assert r.job_var_entries == {
            "TargetUserName": "dbc",
            "TargetUserPassword": "dbc_pw",
        }
        # TPT DDL operator references @LogonMech/@LogonMechData — they must be
        # present in the env even for TD2 so the script substitutes to empty.
        assert r.env_vars == {
            "TdpId": "td.example.com",
            "UserName": "dbc",
            "UserPassword": "dbc_pw",
            "LogonMech": "TD2",
            "LogonMechData": "",
        }
        assert "TargetLogonMech" not in r.job_var_entries  # TD2 is implicit

    def test_ldap(self):
        r = _ldap().render_for_tdload()
        assert r.job_var_entries == {
            "TargetUserName": "ldap_user",
            "TargetUserPassword": "ldap_pw",
            "TargetLogonMech": "LDAP",
        }
        assert r.env_vars == {
            "TdpId": "td.example.com",
            "UserName": "ldap_user",
            "UserPassword": "ldap_pw",
            "LogonMech": "LDAP",
            "LogonMechData": "",
        }

    def test_jwt_omits_password(self):
        r = _jwt().render_for_tdload()
        assert r.job_var_entries == {
            "TargetUserName": "dbs_user",
            "TargetLogonMech": "JWT",
            "TargetLogonMechData": "token=eyJhbGciOi.payload.sig",
        }
        # Critical: no TargetUserPassword — CLIv2 requires LOGON WITH NULL
        # PASSWORD for JWT, and a stray password makes tdload prompt on stdin.
        assert "TargetUserPassword" not in r.job_var_entries
        assert "UserPassword" not in r.env_vars
        assert r.env_vars == {
            "TdpId": "td.example.com",
            "UserName": "dbs_user",
            "LogonMech": "JWT",
            "LogonMechData": "token=eyJhbGciOi.payload.sig",
        }

    def test_secret_uses_clientid_as_username_bare_secret_as_logdata(self):
        r = _secret().render_for_tdload()
        # tdload's wire form: -u <client_id>, --LogonMech SECRET,
        # --LogonMechData <bare-secret>. (Different from BTEQ's OIDC-grant form.)
        assert r.job_var_entries == {
            "TargetUserName": "my_client",
            "TargetLogonMech": "SECRET",
            "TargetLogonMechData": "the_client_secret",
        }
        assert "TargetUserPassword" not in r.job_var_entries
        assert "UserPassword" not in r.env_vars
        assert r.env_vars == {
            "TdpId": "lake.example.com",
            "UserName": "my_client",
            "LogonMech": "SECRET",
            "LogonMechData": "the_client_secret",
        }

    def test_bearer_raises_unsupported(self):
        with pytest.raises(AuthUnsupportedError, match="tdload cannot accept BEARER"):
            _bearer().render_for_tdload()

    def test_jwt_without_username_raises_at_render(self):
        """tdload's CLIv2 syntactically requires TargetUserName even for
        JWT (the user the token was issued for). ``__post_init__`` does
        not require ``username`` for JWT (BTEQ/teradatasql work without
        it), so the check is render-scoped — render_for_tdload raises a
        clean AuthUnsupportedError instead of letting tdload fail with
        "Value must be specified for variable 'TARGETUSERNAME'".

        Regression guard for the wizard-misconfig bug seen in production
        logs (server logs.txt 2026-04-23 02:48).
        """
        a = TeradataAuth(
            host="td.example.com", port=1025, database="dbtest1",
            mechanism="JWT",
            username="",  # wizard left it blank
            logdata="eyJhbGciOi.payload.sig",
        )
        with pytest.raises(AuthUnsupportedError, match="tdload with JWT requires a username"):
            a.render_for_tdload()
        # Other consumers still work — the check is tdload-specific.
        # render_for_bteq emits ".LOGON host/" for JWT (no username), and
        # render_for_teradatasql doesn't include the user kwarg for JWT.
        assert ".LOGON td.example.com/" in "\n".join(a.render_for_bteq())
        sql_kwargs = a.render_for_teradatasql()
        assert "user" not in sql_kwargs

    def test_secret_without_clientid_raises_at_render(self):
        """Defensive: SECRET maps oidc_clientid onto TargetUserName, so an
        empty oidc_clientid would produce the same tdload error as JWT
        without username. ``__post_init__`` already requires oidc_clientid,
        so this only fires if construction validation is bypassed (e.g.
        via dataclasses.replace) — guards against future regressions.
        """
        a = _secret()
        # Bypass __post_init__ via object.__setattr__ to simulate the case.
        object.__setattr__(a, "oidc_clientid", "")
        with pytest.raises(AuthUnsupportedError, match="tdload with SECRET requires oidc_clientid"):
            a.render_for_tdload()

    def test_source_prefix(self):
        """Cross-instance transfers use Source*/Target* pairs."""
        r = _ldap().render_for_tdload(prefix="Source")
        assert r.job_var_entries == {
            "SourceUserName": "ldap_user",
            "SourceUserPassword": "ldap_pw",
            "SourceLogonMech": "LDAP",
        }


# ---------------------------------------------------------------------------
# render_for_bteq
# ---------------------------------------------------------------------------


class TestRenderForBteq:
    def test_td2(self):
        lines = _td2().render_for_bteq()
        assert lines == [".LOGON td.example.com/dbc,dbc_pw"]

    def test_ldap(self):
        lines = _ldap().render_for_bteq()
        assert lines == [
            ".SET LOGMECH LDAP",
            ".LOGON td.example.com/ldap_user,ldap_pw",
        ]

    def test_jwt(self):
        lines = _jwt().render_for_bteq()
        assert lines == [
            ".SET LOGMECH JWT",
            ".LOGDATA token=eyJhbGciOi.payload.sig",
            ".LOGON td.example.com/",
        ]

    def test_secret_uses_oidc_grant_form(self):
        """BTEQ uses the OIDC client_credentials grant form — distinct
        from tdload's bare-secret form."""
        lines = _secret().render_for_bteq()
        assert lines[0] == ".SET LOGMECH CRED"
        logdata_line = next(ln for ln in lines if ln.startswith(".LOGDATA"))
        assert "grant_type=client_credentials" in logdata_line
        assert "scope=openid" in logdata_line
        assert "client_id=my_client" in logdata_line
        assert "client_secret=the_client_secret" in logdata_line
        assert lines[-1] == ".LOGON lake.example.com/"

    def test_secret_with_sslca_emits_connectstring(self):
        a = TeradataAuth(
            host="lake", port=443, database="",
            mechanism="SECRET", oidc_clientid="c", logdata="s",
            sslca="/tmp/ca.pem",
        )
        lines = a.render_for_bteq()
        assert ".CONNECTSTRING SSLCA=/tmp/ca.pem" in lines

    def test_bearer(self):
        lines = _bearer().render_for_bteq()
        assert lines[0] == ".SET LOGMECH BEARER"
        connect = next(ln for ln in lines if ln.startswith(".CONNECTSTRING"))
        assert "oidc_clientid=my_client" in connect
        assert "jws_private_key=/etc/secrets/jws_priv.pem" in connect
        assert "jws_cert=/etc/secrets/jws_cert.pem" in connect
        assert "SSLCA=/etc/ssl/cacerts.pem" in connect
        assert lines[-1] == ".LOGON lake.example.com/"


# ---------------------------------------------------------------------------
# render_for_teradatasql
# ---------------------------------------------------------------------------


class TestRenderForTeradatasql:
    def test_td2(self):
        p = _td2().render_for_teradatasql()
        assert p["host"] == "td.example.com"
        assert p["dbs_port"] == "1025"
        assert p["logmech"] == "TD2"
        assert p["database"] == "testdb"
        assert p["user"] == "dbc"
        assert p["password"] == "dbc_pw"

    def test_ldap(self):
        p = _ldap().render_for_teradatasql()
        assert p["logmech"] == "LDAP"
        assert p["user"] == "ldap_user"
        assert p["password"] == "ldap_pw"

    def test_jwt(self):
        p = _jwt().render_for_teradatasql()
        assert p["logmech"] == "JWT"
        assert p["logdata"] == "token=eyJhbGciOi.payload.sig"
        assert "user" not in p  # JWT passes just logdata; driver reads token from it
        assert "password" not in p

    def test_secret(self):
        p = _secret().render_for_teradatasql()
        assert p["logmech"] == "SECRET"
        assert p["oidc_clientid"] == "my_client"
        assert p["logdata"] == "the_client_secret"
        assert "user" not in p

    def test_bearer(self):
        p = _bearer().render_for_teradatasql()
        assert p["logmech"] == "BEARER"
        assert p["oidc_clientid"] == "my_client"
        assert p["jws_private_key"] == "/etc/secrets/jws_priv.pem"
        assert p["jws_cert"] == "/etc/secrets/jws_cert.pem"
        assert p["sslca"] == "/etc/ssl/cacerts.pem"

    def test_no_database_omitted(self):
        a = TeradataAuth(host="h", port=1025, database="",
                         mechanism="TD2", username="u", password="p")
        p = a.render_for_teradatasql()
        assert "database" not in p


# ---------------------------------------------------------------------------
# render_for_dbt_env
# ---------------------------------------------------------------------------


class TestRenderForDbtEnv:
    """The dbt env-var renderer must return ALL TERADATA_* keys (with empty
    defaults for unused ones) so a merge into the subprocess env cannot let
    stale parent-shell values shadow the resolved identity."""

    _EXPECTED_KEYS = {
        "TERADATA_HOST", "TERADATA_PORT", "TERADATA_DATABASE",
        "TERADATA_LOGMECH",
        "TERADATA_USERNAME", "TERADATA_PASSWORD",
        "TERADATA_LOGDATA",
        "TERADATA_OIDC_CLIENTID",
        "TERADATA_JWS_PRIVATE_KEY", "TERADATA_JWS_CERT", "TERADATA_SSLCA",
    }

    @pytest.mark.parametrize("auth_fn", [_td2, _ldap, _jwt, _secret, _bearer])
    def test_all_keys_present_regardless_of_mechanism(self, auth_fn):
        env = auth_fn().render_for_dbt_env()
        assert set(env.keys()) == self._EXPECTED_KEYS

    def test_td2(self):
        env = _td2().render_for_dbt_env()
        assert env["TERADATA_LOGMECH"] == "TD2"
        assert env["TERADATA_USERNAME"] == "dbc"
        assert env["TERADATA_PASSWORD"] == "dbc_pw"
        assert env["TERADATA_LOGDATA"] == ""
        assert env["TERADATA_OIDC_CLIENTID"] == ""

    def test_jwt_password_is_empty(self):
        env = _jwt().render_for_dbt_env()
        assert env["TERADATA_LOGMECH"] == "JWT"
        assert env["TERADATA_USERNAME"] == "dbs_user"
        assert env["TERADATA_PASSWORD"] == ""  # critical — must actively clear
        assert env["TERADATA_LOGDATA"] == "token=eyJhbGciOi.payload.sig"

    def test_bearer_populates_jws_fields(self):
        env = _bearer().render_for_dbt_env()
        assert env["TERADATA_LOGMECH"] == "BEARER"
        assert env["TERADATA_OIDC_CLIENTID"] == "my_client"
        assert env["TERADATA_JWS_PRIVATE_KEY"] == "/etc/secrets/jws_priv.pem"
        assert env["TERADATA_JWS_CERT"] == "/etc/secrets/jws_cert.pem"
        assert env["TERADATA_SSLCA"] == "/etc/ssl/cacerts.pem"
        assert env["TERADATA_USERNAME"] == ""
        assert env["TERADATA_PASSWORD"] == ""


# ---------------------------------------------------------------------------
# render_for_dbt_profile_yaml
# ---------------------------------------------------------------------------


class TestRenderForDbtProfileYaml:
    def test_body_uses_env_var_refs_for_mechanism_agnostic_profile(self):
        body = _td2().render_for_dbt_profile_yaml()
        assert body["type"] == "teradata"
        assert body["host"] == "{{ env_var('TERADATA_HOST') }}"
        assert body["logmech"] == "{{ env_var('TERADATA_LOGMECH', 'TD2') }}"
        assert body["logdata"] == "{{ env_var('TERADATA_LOGDATA', '') }}"
        assert body["jws_private_key"] == "{{ env_var('TERADATA_JWS_PRIVATE_KEY', '') }}"

    def test_body_has_all_mechanism_fields(self):
        """The profile must reference every TERADATA_* var so any mechanism
        can take effect without rewriting profiles.yml."""
        body = _jwt().render_for_dbt_profile_yaml()
        expected_refs = {
            "host", "port", "schema", "logmech",
            "user", "password", "logdata", "oidc_clientid",
            "jws_private_key", "jws_cert", "sslca",
        }
        assert expected_refs <= set(body.keys())

    def test_port_emitted_as_string_literal(self):
        """dbt-teradata's profile schema accepts ``port: '1025'`` (string)
        but rejects ``port: 1025`` (int) and Jinja ``as_number`` (float)."""
        td = _td2()
        body = td.render_for_dbt_profile_yaml()
        assert body["port"] == str(td.port)
        assert isinstance(body["port"], str)
        assert "{{" not in body["port"] and "env_var" not in body["port"]
