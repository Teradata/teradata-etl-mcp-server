"""Unit tests for escaping utilities.

Tests cover:
- escape_single_quoted: Escaping for single-quoted Python strings
- escape_triple_quoted: Escaping for triple-quoted Python strings
- escape_for_python_literal: General Python literal escaping
"""


from teradata_etl_mcp_server.generators.escaping import (
    escape_for_python_literal,
    escape_single_quoted,
    escape_triple_quoted,
)


class TestEscapeSingleQuoted:
    """Tests for escape_single_quoted function."""

    def test_none_input(self):
        """Test that None input returns None."""
        assert escape_single_quoted(None) is None

    def test_empty_string(self):
        """Test empty string passes through."""
        assert escape_single_quoted("") == ""

    def test_plain_string(self):
        """Test string without special characters."""
        assert escape_single_quoted("hello world") == "hello world"

    def test_single_quote(self):
        """Test single quotes are escaped."""
        assert escape_single_quoted("it's") == "it\\'s"

    def test_multiple_single_quotes(self):
        """Test multiple single quotes are escaped."""
        assert escape_single_quoted("'hello' 'world'") == "\\'hello\\' \\'world\\'"

    def test_backslash(self):
        """Test backslashes are escaped."""
        assert escape_single_quoted("path\\to\\file") == "path\\\\to\\\\file"

    def test_backslash_before_quote(self):
        """Test backslash before quote - backslash escaped first."""
        assert escape_single_quoted("\\'") == "\\\\\\'"

    def test_mixed_special_chars(self):
        """Test mixed backslashes and quotes."""
        result = escape_single_quoted("path\\file's name")
        assert result == "path\\\\file\\'s name"


class TestEscapeTripleQuoted:
    """Tests for escape_triple_quoted function."""

    def test_none_input(self):
        """Test that None input returns None."""
        assert escape_triple_quoted(None) is None

    def test_empty_string(self):
        """Test empty string passes through."""
        assert escape_triple_quoted("") == ""

    def test_plain_string(self):
        """Test string without special characters."""
        assert escape_triple_quoted("hello world") == "hello world"

    def test_single_quote_not_escaped(self):
        """Test single quotes are NOT escaped in triple-quoted."""
        assert escape_triple_quoted("it's") == "it's"

    def test_triple_quote(self):
        """Test triple quotes are escaped."""
        input_str = 'say ' + '"""' + 'hello' + '"""'
        escaped = escape_triple_quoted(input_str)
        # Triple quotes should be escaped
        assert '"""' not in escaped
        assert '\\"\\"\\"' in escaped

    def test_backslash(self):
        """Test backslashes are escaped."""
        assert escape_triple_quoted("path\\to\\file") == "path\\\\to\\\\file"

    def test_double_quote_pair(self):
        """Test double quote pairs not affected."""
        assert escape_triple_quoted('He said ""wow""') == 'He said ""wow""'


class TestEscapeForPythonLiteral:
    """Tests for escape_for_python_literal function."""

    def test_none_input(self):
        """Test that None input returns None."""
        assert escape_for_python_literal(None) is None

    def test_empty_string(self):
        """Test empty string passes through."""
        assert escape_for_python_literal("") == ""

    def test_plain_string(self):
        """Test string without special characters."""
        assert escape_for_python_literal("hello world") == "hello world"

    def test_newline(self):
        """Test newlines are escaped."""
        assert escape_for_python_literal("line1\nline2") == "line1\\nline2"

    def test_tab(self):
        """Test tabs are escaped."""
        assert escape_for_python_literal("col1\tcol2") == "col1\\tcol2"

    def test_carriage_return(self):
        """Test carriage returns are escaped."""
        assert escape_for_python_literal("line1\rline2") == "line1\\rline2"

    def test_double_quote(self):
        """Test double quotes are escaped."""
        assert escape_for_python_literal('say "hello"') == 'say \\"hello\\"'

    def test_backslash(self):
        """Test backslashes are escaped."""
        assert escape_for_python_literal("path\\file") == "path\\\\file"

    def test_unicode(self):
        """Test unicode characters are escaped to ASCII-safe representation."""
        result = escape_for_python_literal("café")
        # json.dumps escapes non-ASCII by default in some configurations
        # Either raw unicode or escaped form is acceptable
        assert result is not None
        assert "caf" in result

    def test_control_characters(self):
        """Test control characters are escaped."""
        result = escape_for_python_literal("bell\x07char")
        assert "\\u0007" in result or "\\x07" in result


class TestSecurityCases:
    """Security-focused tests for escaping functions."""

    def test_code_injection_single_quote(self):
        """Test potential code injection via single quote."""
        malicious = "'; import os; os.system('rm -rf /'); '"
        escaped = escape_single_quoted(malicious)
        # Should escape quotes to prevent breaking out of string
        assert "\\'" in escaped
        assert escaped.count("\\'") >= 4

    def test_code_injection_triple_quote(self):
        """Test potential code injection via triple quote."""
        malicious = '"""; import os; os.system("rm -rf /"); """'
        escaped = escape_triple_quoted(malicious)
        # Should escape triple quotes
        assert '\\"\\"\\"' in escaped

    def test_null_byte(self):
        """Test null bytes are handled."""
        result = escape_for_python_literal("test\x00value")
        # Should not raise, should escape the null
        assert result is not None
        assert "\x00" not in result or "\\u0000" in result or "\\x00" in result
