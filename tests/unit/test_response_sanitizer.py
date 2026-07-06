"""Tests for the response sanitizer module."""

import json

from elt_mcp_server.response_sanitizer import (
    MASK_VALUE,
    _should_mask_key,
    _tokenize_key,
    safe_error_message,
    sanitize_response,
)


class TestSafeErrorMessage:
    """Tests for safe_error_message — H4a credential redaction helper."""

    def test_includes_exception_type(self):
        exc = ValueError("something went wrong")
        msg = safe_error_message(exc)
        assert msg.startswith("ValueError: ")

    def test_preserves_message(self):
        exc = RuntimeError("disk full")
        msg = safe_error_message(exc)
        assert "disk full" in msg

    def test_redacts_password(self):
        exc = Exception("connection failed: password=s3cret123 host=db")
        msg = safe_error_message(exc)
        assert "s3cret123" not in msg
        assert "password=***REDACTED***" in msg

    def test_redacts_token(self):
        exc = Exception("auth failed: token=abc123xyz")
        msg = safe_error_message(exc)
        assert "abc123xyz" not in msg
        assert "token=***REDACTED***" in msg

    def test_redacts_secret(self):
        exc = Exception("secret: my_super_secret_value")
        msg = safe_error_message(exc)
        assert "my_super_secret_value" not in msg
        assert "secret=***REDACTED***" in msg

    def test_redacts_key(self):
        exc = Exception("api key=AKIAI1234567890")
        msg = safe_error_message(exc)
        assert "AKIAI1234567890" not in msg

    def test_redacts_credential(self):
        exc = Exception("bad credential=xyz789")
        msg = safe_error_message(exc)
        assert "xyz789" not in msg
        assert "credential=***REDACTED***" in msg

    def test_no_credential_no_redaction(self):
        exc = Exception("file not found: /tmp/data.csv")
        msg = safe_error_message(exc)
        assert msg == "Exception: file not found: /tmp/data.csv"

    def test_with_context(self):
        exc = ValueError("bad input")
        msg = safe_error_message(exc, context="trigger_dag")
        assert msg == "trigger_dag: ValueError: bad input"

    def test_empty_context(self):
        exc = ValueError("bad input")
        msg = safe_error_message(exc, context="")
        assert msg == "ValueError: bad input"

    def test_multiple_credentials_redacted(self):
        exc = Exception("password=abc token=def key=ghi")
        msg = safe_error_message(exc)
        assert "abc" not in msg
        assert "def" not in msg
        assert "ghi" not in msg
        assert msg.count("***REDACTED***") == 3

    def test_case_insensitive_redaction(self):
        exc = Exception("PASSWORD=secret123")
        msg = safe_error_message(exc)
        assert "secret123" not in msg

    def test_custom_exception_type(self):
        class MyCustomError(Exception):
            pass

        exc = MyCustomError("custom failure")
        msg = safe_error_message(exc)
        assert msg.startswith("MyCustomError: ")
        assert "custom failure" in msg


class TestTokenizeKey:
    def test_snake_case(self):
        assert _tokenize_key("access_token") == ["access", "token"]

    def test_camel_case(self):
        assert _tokenize_key("accessToken") == ["access", "token"]

    def test_kebab_case(self):
        assert _tokenize_key("client-secret") == ["client", "secret"]

    def test_single_word(self):
        assert _tokenize_key("password") == ["password"]

    def test_upper_camel(self):
        assert _tokenize_key("ConnectionConfiguration") == ["connection", "configuration"]

    def test_mixed_separators(self):
        assert _tokenize_key("my_apiKey") == ["my", "api", "key"]


class TestShouldMaskKey:
    # --- Keys that MUST be masked ---

    def test_password_key(self):
        assert _should_mask_key("password") is True

    def test_mixed_case(self):
        assert _should_mask_key("Password") is True
        assert _should_mask_key("PASSWORD") is True

    def test_prefixed_password(self):
        assert _should_mask_key("db_password") is True
        assert _should_mask_key("dbPassword") is True

    def test_secret_key(self):
        assert _should_mask_key("client_secret") is True
        assert _should_mask_key("clientSecret") is True

    def test_bare_secret(self):
        assert _should_mask_key("secret") is True

    def test_token_key(self):
        assert _should_mask_key("access_token") is True
        assert _should_mask_key("accessToken") is True
        assert _should_mask_key("auth_token") is True
        assert _should_mask_key("bearer_token") is True
        assert _should_mask_key("refresh_token") is True

    def test_bare_token(self):
        assert _should_mask_key("token") is True

    def test_api_key(self):
        assert _should_mask_key("api_key") is True
        assert _should_mask_key("apiKey") is True
        assert _should_mask_key("apikey") is True

    def test_private_key(self):
        assert _should_mask_key("private_key") is True
        assert _should_mask_key("privateKey") is True
        assert _should_mask_key("ssh_key") is True

    def test_credential_key(self):
        assert _should_mask_key("credential") is True
        assert _should_mask_key("credentials") is True

    def test_connection_configuration(self):
        assert _should_mask_key("connection_configuration") is True
        assert _should_mask_key("connectionConfiguration") is True

    def test_non_string_key(self):
        assert _should_mask_key(42) is False
        assert _should_mask_key(None) is False

    # --- Keys that must NOT be masked (false-positive prevention) ---

    def test_safe_keys(self):
        assert _should_mask_key("host") is False
        assert _should_mask_key("port") is False
        assert _should_mask_key("database") is False
        assert _should_mask_key("success") is False
        assert _should_mask_key("connection_id") is False

    def test_token_url_not_masked(self):
        assert _should_mask_key("token_url") is False
        assert _should_mask_key("tokenUrl") is False
        assert _should_mask_key("token_endpoint") is False
        assert _should_mask_key("token_type") is False
        assert _should_mask_key("tokenType") is False

    def test_secrets_manager_not_masked(self):
        assert _should_mask_key("secrets_manager") is False
        assert _should_mask_key("secretsManager") is False
        assert _should_mask_key("secret_version") is False

    def test_password_metadata_not_masked(self):
        assert _should_mask_key("password_reset_url") is False
        assert _should_mask_key("passwordResetUrl") is False
        assert _should_mask_key("password_policy") is False

    def test_non_sensitive_key_compounds_not_masked(self):
        assert _should_mask_key("primary_key") is False
        assert _should_mask_key("foreign_key") is False
        assert _should_mask_key("sort_key") is False
        assert _should_mask_key("partition_key") is False


class TestSanitizeResponse:
    def test_none_input(self):
        assert sanitize_response(None) is None

    def test_scalar_unchanged(self):
        assert sanitize_response("hello") == "hello"
        assert sanitize_response(42) == 42

    def test_masks_password_in_flat_dict(self):
        result = sanitize_response({"host": "localhost", "password": "secret"})
        assert result["host"] == "localhost"
        assert result["password"] == MASK_VALUE

    def test_masks_nested_password(self):
        result = sanitize_response(
            {
                "connection": {
                    "host": "localhost",
                    "password": "secret",
                },
            }
        )
        assert result["connection"]["host"] == "localhost"
        assert result["connection"]["password"] == MASK_VALUE

    def test_masks_connection_configuration(self):
        result = sanitize_response(
            {
                "success": True,
                "connection_configuration": {
                    "host": "db.example.com",
                    "password": "secret",
                },
            }
        )
        assert result["connection_configuration"] == MASK_VALUE

    def test_masks_in_list_of_dicts(self):
        result = sanitize_response(
            {
                "connections": [
                    {"name": "conn1", "password": "pw1"},
                    {"name": "conn2", "password": "pw2"},
                ],
            }
        )
        for conn in result["connections"]:
            assert conn["password"] == MASK_VALUE

    def test_preserves_non_sensitive_keys(self):
        result = sanitize_response(
            {
                "success": True,
                "host": "localhost",
                "port": 5432,
                "message": "Created successfully",
            }
        )
        assert result["success"] is True
        assert result["host"] == "localhost"
        assert result["port"] == 5432

    def test_deep_copy_does_not_mutate_original(self):
        original = {"password": "secret", "host": "localhost"}
        sanitize_response(original)
        assert original["password"] == "secret"

    def test_multiple_sensitive_keys(self):
        result = sanitize_response(
            {
                "password": "pw",
                "access_token": "tok",
                "client_secret": "sec",
                "api_key": "key",
            }
        )
        assert result["password"] == MASK_VALUE
        assert result["access_token"] == MASK_VALUE
        assert result["client_secret"] == MASK_VALUE
        assert result["api_key"] == MASK_VALUE

    def test_deeply_nested(self):
        result = sanitize_response(
            {
                "level1": {
                    "level2": {
                        "level3": {
                            "password": "deep_secret",
                        },
                    },
                },
            }
        )
        assert result["level1"]["level2"]["level3"]["password"] == MASK_VALUE


class TestJsonStringSanitization:
    """Tests for secrets embedded inside JSON-encoded strings."""

    def test_json_object_string_with_sensitive_key_is_masked(self):
        embedded = json.dumps({"host": "db.example.com", "password": "s3cret"})
        result = sanitize_response({"extra": embedded})
        # The value must still be a string (serialised JSON), not a dict
        assert isinstance(result["extra"], str)
        parsed = json.loads(result["extra"])
        assert parsed["host"] == "db.example.com"
        assert parsed["password"] == MASK_VALUE

    def test_json_array_string_with_sensitive_key_is_masked(self):
        embedded = json.dumps([{"token": "abc123", "name": "conn1"}])
        result = sanitize_response({"items": embedded})
        assert isinstance(result["items"], str)
        parsed = json.loads(result["items"])
        assert parsed[0]["token"] == MASK_VALUE
        assert parsed[0]["name"] == "conn1"

    def test_invalid_json_object_string_returned_unchanged(self):
        """For non-secret keys, invalid JSON strings pass through unchanged."""
        bad = "{not valid json!!"
        result = sanitize_response({"info": bad})
        assert result["info"] == bad

    def test_invalid_json_in_scalar_secret_key_is_masked(self):
        """The 'extra' key is a known scalar-secret key. Unparseable values
        are masked because they may embed credentials in non-JSON format."""
        bad = "{not valid json!!"
        result = sanitize_response({"extra": bad})
        assert result["extra"] == MASK_VALUE

    def test_invalid_json_array_string_returned_unchanged(self):
        bad = "[broken, json"
        result = sanitize_response({"data": bad})
        assert result["data"] == bad

    def test_non_json_string_not_starting_with_brace_unchanged(self):
        plain = "just a normal string"
        result = sanitize_response({"msg": plain})
        assert result["msg"] == plain

    def test_json_string_with_leading_whitespace_is_masked(self):
        embedded = '  {"password": "s3cret", "host": "db.example.com"}'
        result = sanitize_response({"extra": embedded})
        assert isinstance(result["extra"], str)
        parsed = json.loads(result["extra"])
        assert parsed["password"] == MASK_VALUE
        assert parsed["host"] == "db.example.com"

    def test_json_string_with_leading_newline_is_masked(self):
        embedded = '\n[{"token": "abc123", "name": "conn1"}]'
        result = sanitize_response({"items": embedded})
        assert isinstance(result["items"], str)
        parsed = json.loads(result["items"])
        assert parsed[0]["token"] == MASK_VALUE
        assert parsed[0]["name"] == "conn1"

    def test_nested_json_string_with_multiple_sensitive_keys(self):
        embedded = json.dumps(
            {
                "api_key": "key123",
                "client_secret": "sec456",
                "endpoint": "https://api.example.com",
            }
        )
        result = sanitize_response({"config": embedded})
        parsed = json.loads(result["config"])
        assert parsed["api_key"] == MASK_VALUE
        assert parsed["client_secret"] == MASK_VALUE
        assert parsed["endpoint"] == "https://api.example.com"
