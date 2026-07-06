"""Tests for the credential resolver module."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from elt_mcp_server.credential_resolver import CredentialResolver, ProfileSummary


@pytest.fixture
def sample_profiles():
    """Return a sample connections.yaml content dict."""
    return {
        "version": "1",
        "profiles": {
            "my_postgres": {
                "host": "pg-host.example.com",
                "port": 5432,
                "database": "testdb",
                "username": "testuser",
                "password": "secretpw",
                "schemas": ["public"],
                "description": "Test Postgres DB",
            },
            "prod_teradata": {
                "host": "td-host.example.com",
                "port": 1025,
                "username": "dbc",
                "password": "dbcpw",
                "default_schema": "analytics",
                "description": "Production Teradata",
            },
            "ssh_conn": {
                "host": "worker.example.com",
                "port": 22,
                "username": "airflow",
                "key_file": "~/.ssh/id_rsa",
                "description": "SSH connection",
            },
        },
        "aliases": {
            "postgres": "my_postgres",
            "teradata": "prod_teradata",
        },
    }


@pytest.fixture
def connections_file(sample_profiles, tmp_path):
    """Write a sample connections.yaml and return its path."""
    file_path = tmp_path / "connections.yaml"
    with open(file_path, "w") as f:
        yaml.dump(sample_profiles, f)
    return file_path


@pytest.fixture
def resolver(connections_file):
    """Create a CredentialResolver with the sample file."""
    return CredentialResolver(settings=None, connections_file=connections_file)


class TestCredentialResolverInit:
    def test_loads_profiles_from_file(self, resolver):
        profiles = resolver.list_profiles()
        assert len(profiles) == 3

    def test_no_file_returns_empty(self):
        resolver = CredentialResolver(settings=None, connections_file=Path("/nonexistent"))
        assert resolver.list_profiles() == []

    def test_none_file_searches_defaults(self):
        with patch.object(CredentialResolver, "_find_connections_file", return_value=None):
            resolver = CredentialResolver(settings=None)
            assert resolver.list_profiles() == []


class TestResolveProfile:
    def test_resolve_existing_profile(self, resolver):
        profile = resolver.resolve_profile("my_postgres")
        assert profile["host"] == "pg-host.example.com"
        assert profile["port"] == 5432
        assert profile["username"] == "testuser"
        assert profile["password"] == "secretpw"
        assert profile["database"] == "testdb"

    def test_resolve_strips_description(self, resolver):
        profile = resolver.resolve_profile("my_postgres")
        assert "description" not in profile

    def test_resolve_alias(self, resolver):
        profile = resolver.resolve_profile("postgres")
        assert profile["host"] == "pg-host.example.com"

    def test_resolve_unknown_raises_valueerror(self, resolver):
        with pytest.raises(ValueError, match="Unknown connection profile"):
            resolver.resolve_profile("nonexistent")

    def test_resolve_returns_deep_copy(self, resolver):
        p1 = resolver.resolve_profile("my_postgres")
        p2 = resolver.resolve_profile("my_postgres")
        p1["host"] = "modified"
        assert p2["host"] == "pg-host.example.com"


class TestEnvVarInterpolation:
    def test_interpolates_env_vars(self, tmp_path):
        profiles = {
            "version": "1",
            "profiles": {
                "test_db": {
                    "host": "localhost",
                    "password": "${TEST_DB_PASSWORD}",
                    "description": "Test DB",
                },
            },
        }
        file_path = tmp_path / "connections.yaml"
        with open(file_path, "w") as f:
            yaml.dump(profiles, f)

        with patch.dict(os.environ, {"TEST_DB_PASSWORD": "resolved_secret"}):
            resolver = CredentialResolver(settings=None, connections_file=file_path)
            profile = resolver.resolve_profile("test_db")
            assert profile["password"] == "resolved_secret"

    def test_numeric_env_var_coerced_to_int(self, tmp_path):
        """port: ${TEST_DB_PORT} should resolve to int, not str, matching ``port: 1025``."""
        profiles = {
            "version": "1",
            "profiles": {
                "test_db": {
                    "host": "localhost",
                    "port": "${TEST_DB_PORT}",
                    "description": "Test DB",
                },
            },
        }
        file_path = tmp_path / "connections.yaml"
        with open(file_path, "w") as f:
            yaml.dump(profiles, f)

        with patch.dict(os.environ, {"TEST_DB_PORT": "1025"}):
            resolver = CredentialResolver(settings=None, connections_file=file_path)
            profile = resolver.resolve_profile("test_db")
            assert profile["port"] == 1025
            assert isinstance(profile["port"], int)

    def test_numeric_password_stays_string(self, tmp_path):
        """password: ${TEST_NUMERIC_PWD} whose value is all-digits must NOT be coerced to int."""
        profiles = {
            "version": "1",
            "profiles": {
                "test_db": {
                    "host": "localhost",
                    "password": "${TEST_NUMERIC_PWD}",
                    "description": "Test DB",
                },
            },
        }
        file_path = tmp_path / "connections.yaml"
        with open(file_path, "w") as f:
            yaml.dump(profiles, f)

        with patch.dict(os.environ, {"TEST_NUMERIC_PWD": "12345678"}):
            resolver = CredentialResolver(settings=None, connections_file=file_path)
            profile = resolver.resolve_profile("test_db")
            assert profile["password"] == "12345678"
            assert isinstance(profile["password"], str)

    def test_partial_env_var_stays_string(self, tmp_path):
        """Mixed strings like ``db-${ENV}-host`` should stay as strings."""
        profiles = {
            "version": "1",
            "profiles": {
                "test_db": {
                    "host": "db-${TEST_ENV_NAME}-host",
                    "description": "Test DB",
                },
            },
        }
        file_path = tmp_path / "connections.yaml"
        with open(file_path, "w") as f:
            yaml.dump(profiles, f)

        with patch.dict(os.environ, {"TEST_ENV_NAME": "prod"}):
            resolver = CredentialResolver(settings=None, connections_file=file_path)
            profile = resolver.resolve_profile("test_db")
            assert profile["host"] == "db-prod-host"
            assert isinstance(profile["host"], str)

    def test_unset_env_var_leaves_placeholder(self, tmp_path):
        profiles = {
            "version": "1",
            "profiles": {
                "test_db": {
                    "host": "localhost",
                    "password": "${UNSET_VAR_12345}",
                    "description": "Test DB",
                },
            },
        }
        file_path = tmp_path / "connections.yaml"
        with open(file_path, "w") as f:
            yaml.dump(profiles, f)

        # Ensure the var is not set
        env = os.environ.copy()
        env.pop("UNSET_VAR_12345", None)
        with patch.dict(os.environ, env, clear=True):
            resolver = CredentialResolver(settings=None, connections_file=file_path)
            profile = resolver.resolve_profile("test_db")
            assert profile["password"] == "${UNSET_VAR_12345}"


class TestListProfiles:
    def test_returns_summaries_without_secrets(self, resolver):
        profiles = resolver.list_profiles()
        names = {p.name for p in profiles}
        assert "my_postgres" in names
        assert "prod_teradata" in names
        assert "ssh_conn" in names

        for p in profiles:
            assert isinstance(p, ProfileSummary)
            assert hasattr(p, "description")
            # Should NOT have password, host, etc.
            assert not hasattr(p, "password")
            assert not hasattr(p, "host")

    def test_descriptions_are_correct(self, resolver):
        profiles = {p.name: p for p in resolver.list_profiles()}
        assert profiles["my_postgres"].description == "Test Postgres DB"
        assert profiles["prod_teradata"].description == "Production Teradata"


class TestIsConfigured:
    def test_configured_when_file_and_profiles_exist(self, resolver):
        assert resolver.is_configured is True

    def test_not_configured_when_no_file(self):
        resolver = CredentialResolver(settings=None, connections_file=Path("/nonexistent"))
        assert resolver.is_configured is False

    def test_not_configured_when_file_has_no_profiles(self, tmp_path):
        file_path = tmp_path / "connections.yaml"
        with open(file_path, "w") as f:
            yaml.dump({"version": "1", "profiles": {}}, f)
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        assert resolver.is_configured is False


class TestResolveProfileErrors:
    def test_error_when_no_file_found(self):
        resolver = CredentialResolver(settings=None, connections_file=Path("/nonexistent"))
        with pytest.raises(ValueError, match="Action required"):
            resolver.resolve_profile("any_profile")

    def test_error_says_do_not_create(self):
        resolver = CredentialResolver(settings=None, connections_file=Path("/nonexistent"))
        with pytest.raises(ValueError, match="Do not create this file yourself"):
            resolver.resolve_profile("any_profile")

    def test_error_says_ask_the_user(self):
        resolver = CredentialResolver(settings=None, connections_file=Path("/nonexistent"))
        with pytest.raises(ValueError, match="Ask the user"):
            resolver.resolve_profile("any_profile")

    def test_error_when_file_has_no_profiles(self, tmp_path):
        file_path = tmp_path / "connections.yaml"
        with open(file_path, "w") as f:
            yaml.dump({"version": "1", "profiles": {}}, f)
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        with pytest.raises(ValueError, match="Action required"):
            resolver.resolve_profile("any_profile")

    def test_error_when_yaml_invalid(self, tmp_path):
        file_path = tmp_path / "connections.yaml"
        file_path.write_text(":\n  bad: [yaml\n  unclosed", encoding="utf-8")
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        with pytest.raises(ValueError, match="could not be parsed"):
            resolver.resolve_profile("any_profile")

    def test_yaml_parse_error_does_not_leak_file_content(self, tmp_path):
        """YAML with a password line that triggers a parse error must not
        expose the raw line content in the error message."""
        file_path = tmp_path / "connections.yaml"
        file_path.write_text(
            "profiles:\n  db:\n    password: hunter2\n    bad: [unclosed\n",
            encoding="utf-8",
        )
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        with pytest.raises(ValueError) as exc_info:
            resolver.resolve_profile("any_profile")
        error_msg = str(exc_info.value)
        assert "hunter2" not in error_msg
        assert "YAML syntax error" in error_msg or "could not be parsed" in error_msg

    def test_error_when_root_not_mapping(self, tmp_path):
        file_path = tmp_path / "connections.yaml"
        file_path.write_text("- just\n- a\n- list\n", encoding="utf-8")
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        with pytest.raises(ValueError, match="could not be parsed"):
            resolver.resolve_profile("any_profile")

    def test_error_lists_available_profiles(self, resolver):
        with pytest.raises(ValueError, match="Available profiles:"):
            resolver.resolve_profile("nonexistent_profile")


class TestGuardConfigured:
    def test_returns_none_when_configured(self, resolver):
        assert resolver.guard_configured() is None

    def test_returns_error_dict_when_no_file(self):
        resolver = CredentialResolver(settings=None, connections_file=Path("/nonexistent"))
        result = resolver.guard_configured()
        assert result is not None
        assert result["success"] is False
        assert "Action required" in result["error"]
        # Agent must NOT be invited to write .env or connections.yaml.
        assert "must NOT" in result["error"]
        assert "Ask the user" in result["error"]
        assert "setup_instructions" in result
        # The mixed-message ".env credentials work" note has been dropped.
        assert "note" not in result

    def test_returns_error_dict_when_no_profiles(self, tmp_path):
        file_path = tmp_path / "connections.yaml"
        with open(file_path, "w") as f:
            yaml.dump({"version": "1", "profiles": {}}, f)
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        result = resolver.guard_configured()
        assert result is not None
        assert result["success"] is False
        assert "Action required" in result["error"]
        # Note removed — single source of truth in ``error`` + setup_instructions.
        assert "note" not in result

    def test_setup_instructions_direct_to_user_not_self_serve(self):
        """``setup_instructions`` for the missing-yaml path must each
        direct the agent to ASK THE USER (or call reload after the user
        confirms), and explicitly say the agent must not create the
        files itself."""
        resolver = CredentialResolver(settings=None, connections_file=Path("/nonexistent"))
        result = resolver.guard_configured()
        instructions = result["setup_instructions"]
        # Every actionable step is user-directed or runs only after user
        # confirms. Verify by joining and asserting the no-self-serve
        # directive is present.
        joined = "\n".join(instructions)
        assert "ASK THE USER" in joined
        assert "Do NOT create" in joined

    def test_returns_parse_error_when_yaml_invalid(self, tmp_path):
        file_path = tmp_path / "connections.yaml"
        file_path.write_text(":\n  bad: [yaml\n  unclosed", encoding="utf-8")
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        result = resolver.guard_configured()
        assert result is not None
        assert result["success"] is False
        assert "could not be parsed" in result["error"]
        assert "fix the YAML formatting" in result["error"]
        # Should include safe line/column metadata, not raw exception text
        assert "line" in result["error"]

    def test_parse_error_does_not_leak_file_content(self, tmp_path):
        """Malformed YAML containing secrets must not expose them in the error."""
        file_path = tmp_path / "connections.yaml"
        file_path.write_text(
            "profiles:\n  db:\n    password: hunter2\n    bad: [unclosed\n",
            encoding="utf-8",
        )
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        result = resolver.guard_configured()
        assert result is not None
        assert result["success"] is False
        assert "hunter2" not in result["error"]
        assert "YAML syntax error" in result["error"]

    def test_raw_exception_stored_for_server_logging(self, tmp_path):
        """Raw YAML exception should be stored for server-side debugging."""
        file_path = tmp_path / "connections.yaml"
        file_path.write_text(":\n  bad: [yaml\n  unclosed", encoding="utf-8")
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        assert resolver._load_exception is not None
        assert isinstance(resolver._load_exception, Exception)

    def test_structural_error_uses_load_error_not_exception(self, tmp_path):
        """Structural validation errors should use _load_error, not _load_exception."""
        file_path = tmp_path / "connections.yaml"
        file_path.write_text("- just\n- a\n- list\n", encoding="utf-8")
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        assert resolver._load_exception is None
        assert resolver._load_error is not None

    def test_safe_parse_error_yaml_with_mark(self):
        """YAML errors with problem_mark should report line/column."""
        import yaml

        try:
            yaml.safe_load(":\n  bad: [yaml\n  unclosed")
        except yaml.YAMLError as exc:
            msg = CredentialResolver._safe_parse_error(exc)
            assert "YAML syntax error at line" in msg
            assert "column" in msg

    def test_safe_parse_error_permission(self):
        """PermissionError should say 'permission denied', not 'YAML syntax error'."""
        msg = CredentialResolver._safe_parse_error(PermissionError("denied"))
        assert "permission denied" in msg
        assert "YAML syntax" not in msg

    def test_safe_parse_error_unicode(self):
        """UnicodeDecodeError should say 'encoding error', not 'YAML syntax error'."""
        exc = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid byte")
        msg = CredentialResolver._safe_parse_error(exc)
        assert "encoding error" in msg
        assert "YAML syntax" not in msg

    def test_safe_parse_error_oserror(self):
        """Generic OSError should say 'file I/O error', not 'YAML syntax error'."""
        msg = CredentialResolver._safe_parse_error(OSError("disk fail"))
        assert "I/O error" in msg
        assert "YAML syntax" not in msg

    def test_safe_parse_error_unknown(self):
        """Unknown exceptions should use generic fallback."""
        msg = CredentialResolver._safe_parse_error(RuntimeError("something"))
        assert "see server logs" in msg
        assert "YAML syntax" not in msg

    def test_returns_parse_error_when_root_not_mapping(self, tmp_path):
        file_path = tmp_path / "connections.yaml"
        file_path.write_text("- just\n- a\n- list\n", encoding="utf-8")
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        result = resolver.guard_configured()
        assert result is not None
        assert result["success"] is False
        assert "could not be parsed" in result["error"]
        assert "YAML mapping" in result["error"]

    def test_returns_parse_error_when_profiles_not_mapping(self, tmp_path):
        file_path = tmp_path / "connections.yaml"
        file_path.write_text("profiles:\n  - not_a_mapping\n", encoding="utf-8")
        resolver = CredentialResolver(settings=None, connections_file=file_path)
        result = resolver.guard_configured()
        assert result is not None
        assert result["success"] is False
        assert "could not be parsed" in result["error"]
        assert "must be a mapping" in result["error"]


class TestFilePathSearched:
    def test_searched_locations_populated(self):
        resolver = CredentialResolver(settings=None, connections_file=Path("/nonexistent"))
        # When an explicit path is given it goes directly to _load,
        # but _file_path_searched should be a list
        assert isinstance(resolver._file_path_searched, list)

    def test_searched_locations_from_discovery(self, tmp_path):
        # Ensure no connections.yaml is found anywhere
        with patch.object(CredentialResolver, "_find_connections_file", return_value=None):
            resolver = CredentialResolver(settings=None)
            assert isinstance(resolver._file_path_searched, list)


class TestReload:
    def test_reload_picks_up_changes(self, connections_file, resolver):
        # Initially 3 profiles
        assert len(resolver.list_profiles()) == 3

        # Update the file
        new_content = {
            "version": "1",
            "profiles": {
                "new_profile": {
                    "host": "new-host",
                    "password": "newpw",
                    "description": "New",
                },
            },
        }
        with open(connections_file, "w") as f:
            yaml.dump(new_content, f)

        resolver.reload()
        profiles = resolver.list_profiles()
        assert len(profiles) == 1
        assert profiles[0].name == "new_profile"

    def test_reload_resets_configured_state(self, connections_file, resolver):
        assert resolver.is_configured is True

        # Overwrite with empty profiles
        with open(connections_file, "w") as f:
            yaml.dump({"version": "1", "profiles": {}}, f)

        resolver.reload()
        assert resolver.is_configured is False


class TestFindSSHProfiles:
    def _make_resolver(self, profiles_dict, tmp_path):
        content = {"version": "1", "profiles": profiles_dict}
        file_path = tmp_path / "connections.yaml"
        with open(file_path, "w") as f:
            yaml.dump(content, f)
        return CredentialResolver(settings=None, connections_file=file_path)

    def test_finds_by_name_pattern(self, tmp_path):
        resolver = self._make_resolver(
            {
                "airflow_ssh": {"host": "h1", "port": 22, "description": "SSH 1"},
                "ssh_prod": {"host": "h2", "port": 22, "description": "SSH 2"},
                "my_postgres": {"host": "pg", "port": 5432, "description": "PG"},
            },
            tmp_path,
        )
        results = resolver.find_ssh_profiles()
        names = {p.name for p in results}
        assert "airflow_ssh" in names
        assert "ssh_prod" in names
        assert "my_postgres" not in names

    def test_finds_by_key_file_and_port(self, tmp_path):
        resolver = self._make_resolver(
            {
                "my_worker": {
                    "host": "worker.example.com",
                    "port": 22,
                    "key_file": "~/.ssh/id_rsa",
                    "description": "Worker",
                },
            },
            tmp_path,
        )
        results = resolver.find_ssh_profiles()
        assert len(results) == 1
        assert results[0].name == "my_worker"

    def test_excludes_non_ssh(self, tmp_path):
        resolver = self._make_resolver(
            {
                "my_postgres": {"host": "pg", "port": 5432, "description": "PG"},
                "teradata_prod": {"host": "td", "port": 1025, "description": "TD"},
            },
            tmp_path,
        )
        results = resolver.find_ssh_profiles()
        assert len(results) == 0

    def test_empty_when_no_match(self, tmp_path):
        resolver = self._make_resolver(
            {"generic": {"host": "h", "port": 8080, "description": "Generic"}},
            tmp_path,
        )
        results = resolver.find_ssh_profiles()
        assert results == []

    def test_name_with_remote(self, tmp_path):
        resolver = self._make_resolver(
            {"remote_exec": {"host": "r1", "port": 22, "description": "Remote exec"}},
            tmp_path,
        )
        results = resolver.find_ssh_profiles()
        assert len(results) == 1
        assert results[0].name == "remote_exec"

    def test_excludes_db_profile_with_key_file(self, tmp_path):
        resolver = self._make_resolver(
            {
                "test": {
                    "host": "db-host",
                    "port": 22,
                    "key_file": "~/.ssh/id_rsa",
                    "database": "mydb",
                    "description": "DB with SSH tunnel",
                },
            },
            tmp_path,
        )
        results = resolver.find_ssh_profiles()
        assert len(results) == 0
