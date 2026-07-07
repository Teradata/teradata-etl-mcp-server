"""Shared string escaping utilities for code generation.

This module provides consistent escaping functions for generating
Python code strings in DAG templates.
"""

import json


def escape_single_quoted(value: str | None) -> str | None:
    """Escape a string for use in single-quoted Python strings.

    Escapes backslashes first, then single quotes.

    Args:
        value: String to escape, or None

    Returns:
        Escaped string, or None if input was None
    """
    if value is None:
        return None
    return value.replace("\\", "\\\\").replace("'", "\\'")


def escape_triple_quoted(value: str | None) -> str | None:
    """Escape a string for use in triple-quoted Python strings.

    Escapes backslashes first, then triple quotes.

    Args:
        value: String to escape, or None

    Returns:
        Escaped string, or None if input was None
    """
    if value is None:
        return None
    return value.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')


def escape_for_python_literal(value: str | None) -> str | None:
    """Escape a string for safe inclusion in generated Python code.

    Uses json.dumps() for robust escaping of all special characters,
    then strips the surrounding quotes.

    Args:
        value: String to escape, or None

    Returns:
        Escaped string suitable for Python string literals, or None
    """
    if value is None:
        return None
    # json.dumps handles all escaping, strip the quotes it adds
    return json.dumps(value)[1:-1]
