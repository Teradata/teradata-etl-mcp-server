"""Unit tests for the auth resolver's precedence gate.

Precedence rule (from the plan):
- If ``teradata_profile`` is named, that profile is used *entirely* — its
  mechanism and mechanism-specific fields are one coherent identity,
  no mixing with Settings.
- Otherwise, Settings-derived default is used.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from teradata_etl_mcp_server.auth import (
    TeradataAuth,
    build_teradata_auth_from_profile,
    build_teradata_auth_from_settings,
    resolve_teradata_auth,
)


def _mk_settings(**overrides) -> SimpleNamespace:
    """Build a TeradataSettings-like namespace. Defaults to a valid TD2 setup."""
    pw = Mock()
    pw.get_secret_value = Mock(return_value=overrides.pop("_password", "wizard_pw"))
    ld = Mock()
    ld.get_secret_value = Mock(return_value=overrides.pop("_logdata", ""))
    base = dict(
        host="td.example.com",
        port=1025,
        database="testdb",
        username="wizard_user",
        password=pw,
        logmech="TD2",
        logdata=ld,
        oidc_clientid="",
        jws_private_key="",
        jws_cert="",
        sslca="",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


# ---------------------------------------------------------------------------
# build_teradata_auth_from_settings
# ---------------------------------------------------------------------------


class TestBuildFromSettings:
    def test_td2_default(self):
        a = build_teradata_auth_from_settings(_mk_settings())
        assert a.mechanism == "TD2"
        assert a.username == "wizard_user"
        assert a.password == "wizard_pw"

    def test_jwt(self):
        a = build_teradata_auth_from_settings(
            _mk_settings(
                logmech="JWT",
                username="dbs_user",
                _logdata="eyJhbGci.x.y",
            ),
        )
        assert a.mechanism == "JWT"
        assert a.logdata == "token=eyJhbGci.x.y"

    def test_bearer(self):
        a = build_teradata_auth_from_settings(
            _mk_settings(
                logmech="BEARER",
                oidc_clientid="c",
                jws_private_key="/k",
                jws_cert="/c",
                sslca="/ca",
            ),
        )
        assert a.mechanism == "BEARER"
        assert a.oidc_clientid == "c"

    def test_lowercase_mechanism_coerced(self):
        a = build_teradata_auth_from_settings(
            _mk_settings(logmech="ldap", username="u", _password="p"),
        )
        assert a.mechanism == "LDAP"


# ---------------------------------------------------------------------------
# build_teradata_auth_from_profile
# ---------------------------------------------------------------------------


class TestBuildFromProfile:
    def test_ldap_profile(self):
        a = build_teradata_auth_from_profile({
            "host": "td.prod.example.com",
            "port": 1025,
            "database": "prod_db",
            "username": "prof_user",
            "password": "prof_pw",
            "logmech": "LDAP",
        })
        assert a.mechanism == "LDAP"
        assert a.host == "td.prod.example.com"
        assert a.database == "prod_db"

    def test_profile_without_mechanism_defaults_to_td2(self):
        """Legacy-style profile with just host/user/password → TD2.
        Does NOT inherit mechanism from Settings (pre-release, no legacy mix)."""
        a = build_teradata_auth_from_profile({
            "host": "h", "port": 1025,
            "username": "u", "password": "p",
        })
        assert a.mechanism == "TD2"

    def test_profile_schema_key_aliases_database(self):
        a = build_teradata_auth_from_profile({
            "host": "h", "username": "u", "password": "p",
            "schema": "legacy_schema_key",
        })
        assert a.database == "legacy_schema_key"

    def test_profile_mechanism_key_accepted(self):
        """Profiles may use `mechanism` instead of `logmech`."""
        a = build_teradata_auth_from_profile({
            "host": "h", "username": "u", "password": "p",
            "mechanism": "LDAP",
        })
        assert a.mechanism == "LDAP"

    def test_jwt_profile(self):
        a = build_teradata_auth_from_profile({
            "host": "h", "username": "dbs", "logmech": "JWT",
            "logdata": "eyJhbGci.x.y",
        })
        assert a.mechanism == "JWT"
        assert a.logdata == "token=eyJhbGci.x.y"


# ---------------------------------------------------------------------------
# resolve_teradata_auth — the precedence gate
# ---------------------------------------------------------------------------


class TestResolveTeradataAuth:
    def test_no_profile_returns_settings_default(self):
        resolver = Mock()
        auth = resolve_teradata_auth(
            settings=_mk_settings(),
            credential_resolver=resolver,
            teradata_profile=None,
        )
        assert auth.mechanism == "TD2"
        assert auth.username == "wizard_user"
        resolver.resolve_profile.assert_not_called()

    def test_empty_profile_string_treated_as_none(self):
        resolver = Mock()
        auth = resolve_teradata_auth(
            settings=_mk_settings(),
            credential_resolver=resolver,
            teradata_profile="",
        )
        assert auth.mechanism == "TD2"
        resolver.resolve_profile.assert_not_called()

    def test_profile_wins_fully_over_wizard(self):
        """Wizard set to JWT with a token; caller names an LDAP profile.
        Resolver must return the LDAP identity — the wizard's JWT token
        must NOT bleed into the result."""
        resolver = Mock()
        resolver.resolve_profile.return_value = {
            "host": "td.example.com", "port": 1025,
            "username": "ldap_user", "password": "ldap_pw",
            "logmech": "LDAP",
        }
        settings = _mk_settings(
            logmech="JWT",
            username="dbs_user",
            _logdata="eyJhbGci.x.y",
        )
        auth = resolve_teradata_auth(
            settings=settings,
            credential_resolver=resolver,
            teradata_profile="my_ldap",
        )
        assert auth.mechanism == "LDAP"
        assert auth.username == "ldap_user"
        assert auth.password == "ldap_pw"
        assert auth.logdata == ""  # wizard's JWT must not leak into LDAP identity
        resolver.resolve_profile.assert_called_once_with("my_ldap")

    def test_profile_error_propagates(self):
        """Unknown profile name → resolver raises → propagates so the tool
        layer can surface a clean error."""
        resolver = Mock()
        resolver.resolve_profile.side_effect = ValueError("Unknown profile 'xyz'")
        with pytest.raises(ValueError, match="Unknown profile"):
            resolve_teradata_auth(
                settings=_mk_settings(),
                credential_resolver=resolver,
                teradata_profile="xyz",
            )

    def test_wizard_sentinel_resolves_to_settings_default(self):
        """``"wizard"`` and ``"default"`` are explicit-confirmation sentinels
        meaning "use the Settings-default identity". They must short-circuit
        the profile-resolution path — profiles named literally "wizard" or
        "default" are reserved by the connection-selection policy.
        """
        resolver = Mock()
        for sentinel in ("wizard", "default"):
            auth = resolve_teradata_auth(
                settings=_mk_settings(),
                credential_resolver=resolver,
                teradata_profile=sentinel,
            )
            assert auth.mechanism == "TD2", sentinel
            assert auth.username == "wizard_user", sentinel
        resolver.resolve_profile.assert_not_called()

    def test_wizard_sentinel_case_insensitive_and_trimmed(self):
        """All-caps, mixed-case, and padded forms of the sentinels still
        fold to the Settings-default branch."""
        resolver = Mock()
        for sentinel in ("Wizard", "WIZARD", "  default  ", "\tDefault\n", ""):
            auth = resolve_teradata_auth(
                settings=_mk_settings(),
                credential_resolver=resolver,
                teradata_profile=sentinel,
            )
            assert auth.username == "wizard_user", sentinel
        resolver.resolve_profile.assert_not_called()

    def test_profile_with_bearer_wins_over_wizard_td2(self):
        resolver = Mock()
        resolver.resolve_profile.return_value = {
            "host": "lake", "port": 443, "database": "lake_db",
            "logmech": "BEARER",
            "oidc_clientid": "c", "jws_private_key": "/k",
            "jws_cert": "/crt", "sslca": "/ca",
        }
        auth: TeradataAuth = resolve_teradata_auth(
            settings=_mk_settings(),  # wizard TD2
            credential_resolver=resolver,
            teradata_profile="my_bearer",
        )
        assert auth.mechanism == "BEARER"
        assert auth.username == ""   # profile didn't set it → empty, not wizard's
        assert auth.password == ""

    def test_whitespace_padded_profile_name_is_stripped(self):
        """`_normalize_profile_name`'s docstring promises whitespace strip;
        padded profile names like ``'  prod  '`` must reach the resolver
        as ``'prod'`` (the YAML key form), not raw with whitespace."""
        resolver = Mock()
        resolver.resolve_profile.return_value = {
            "host": "h", "port": 1025, "database": "d",
            "username": "u", "password": "p", "logmech": "TD2",
        }
        auth = resolve_teradata_auth(
            settings=_mk_settings(),
            credential_resolver=resolver,
            teradata_profile="  prod  ",
        )
        # The resolver must have been called with the STRIPPED name; the
        # YAML key in connections.yaml is "prod", not "  prod  ".
        resolver.resolve_profile.assert_called_once_with("prod")
        assert auth.host == "h"

    def test_whitespace_padded_sentinel_still_folds_to_default(self):
        """Whitespace-padded sentinels (e.g. ``'  Wizard  '``,
        ``'\\tdefault\\n'``) continue to fold to the Settings default
        even after the strip — the strip happens BEFORE the sentinel
        check so the order of operations is preserved."""
        resolver = Mock()
        for sentinel in ("  wizard  ", "  Wizard  ", "\tdefault\n", "  DEFAULT  "):
            auth = resolve_teradata_auth(
                settings=_mk_settings(),
                credential_resolver=resolver,
                teradata_profile=sentinel,
            )
            assert auth.username == "wizard_user", sentinel
        resolver.resolve_profile.assert_not_called()
