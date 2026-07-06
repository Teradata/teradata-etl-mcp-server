"""Response sanitizer to strip credentials from tool responses.

Applied to tool response dicts before returning to the LLM so that
passwords, tokens, and other secrets are never exposed.
"""

import copy
import json
import re
from typing import Any

MASK_VALUE = "***REDACTED***"

# Regex for splitting camelCase into word boundaries
_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")

# Single-token sensitive words: mask when this token appears as the LAST
# word in a key (e.g. "password", "db_password", "dbPassword" all match).
_SENSITIVE_SUFFIX_TOKENS: frozenset[str] = frozenset(
    {
        "password",
        "secret",
        "token",
        "credential",
        "credentials",
    }
)

# Multi-token compound patterns: mask when the key's trailing tokens
# exactly match the tuple (e.g. ("api", "key") matches "api_key",
# "apiKey", and "my_api_key" but NOT "primary_key").
_SENSITIVE_COMPOUNDS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("api", "key"),
        ("apikey",),
        ("private", "key"),
        ("ssh", "key"),
        ("secret", "key"),
        ("connection", "configuration"),
        ("connectionconfiguration",),
        ("db", "url"),
        ("database", "url"),
        ("connection", "string"),
        ("connectionstring",),
        ("jdbc", "url"),
    }
)

# Keys whose scalar string values may embed secrets (e.g. Airflow connection
# ``extra`` field often contains a JSON string with tokens/passwords).
# These are always sanitized as if they contained JSON.
_SCALAR_SECRET_KEYS: frozenset[str] = frozenset({"extra"})


# Credential pattern with word boundaries to avoid false positives
# Uses \b for word boundaries and allows common delimiters (=, :, space)
_CRED_PATTERN = re.compile(
    r"\b(password|token|secret|key|credential)\b[=:\s]+\S+", re.IGNORECASE
)


def safe_error_message(exc: Exception, context: str = "") -> str:
    """Sanitised error message: includes exception type, redacts credentials."""
    type_name = type(exc).__name__
    msg = _CRED_PATTERN.sub(r"\1=***REDACTED***", str(exc))
    prefix = f"{context}: " if context else ""
    return f"{prefix}{type_name}: {msg}"


def sanitize_response(response: Any) -> Any:
    """Deep-clone and mask any sensitive fields in a tool response.

    - Recursively traverses dicts and lists.
    - Masks values for keys matching sensitive patterns (boundary-aware, case-insensitive).
    - Returns a deep copy — the original is never mutated.
    """
    if response is None:
        return response
    cloned = copy.deepcopy(response)
    return _sanitize_recursive(cloned)


def _tokenize_key(key: str) -> list[str]:
    """Split a key into lowercase word tokens.

    Handles ``snake_case``, ``kebab-case``, and ``camelCase``::

        "access_token"  → ["access", "token"]
        "accessToken"   → ["access", "token"]
        "token-url"     → ["token", "url"]
    """
    # Insert underscore at camelCase boundaries, then split on _ and -
    expanded = _CAMEL_BOUNDARY_RE.sub("_", key)
    return [t.lower() for t in re.split(r"[_\-]+", expanded) if t]


def _should_mask_key(key: Any) -> bool:
    """Check whether a dict key represents a sensitive field.

    Uses boundary-aware matching so that ``db_password`` and ``accessToken``
    are masked, but ``token_url``, ``secrets_manager``, and ``primary_key``
    are not.
    """
    if not isinstance(key, str):
        return False
    tokens = _tokenize_key(key)
    if not tokens:
        return False
    # Suffix-token match: last word is a known sensitive token
    if tokens[-1] in _SENSITIVE_SUFFIX_TOKENS:
        return True
    # Compound suffix match: trailing N tokens form a known compound
    token_tuple = tuple(tokens)
    for compound in _SENSITIVE_COMPOUNDS:
        n = len(compound)
        if len(tokens) >= n and token_tuple[-n:] == compound:
            return True
    return False


def _sanitize_recursive(obj: Any) -> Any:
    """Walk the structure in-place and mask sensitive values."""
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            if _should_mask_key(key):
                obj[key] = MASK_VALUE
            elif isinstance(key, str) and key.lower() in _SCALAR_SECRET_KEYS:
                # Keys known to embed secrets in scalar strings — always
                # attempt JSON parse and sanitize, mask if unparseable.
                val = obj[key]
                if isinstance(val, str):
                    stripped = val.lstrip()
                    if stripped.startswith(("{", "[")):
                        try:
                            parsed = json.loads(val)
                            obj[key] = json.dumps(_sanitize_recursive(parsed))
                        except (json.JSONDecodeError, ValueError):
                            obj[key] = MASK_VALUE
                    elif stripped:
                        obj[key] = MASK_VALUE
                else:
                    obj[key] = _sanitize_recursive(val)
            else:
                obj[key] = _sanitize_recursive(obj[key])
        return obj

    if isinstance(obj, list):
        for i, item in enumerate(obj):
            obj[i] = _sanitize_recursive(item)
        return obj

    # Strings — attempt JSON parse to catch embedded secrets
    # (e.g. Airflow connection "extra" stored as a JSON string)
    if isinstance(obj, str) and obj.lstrip().startswith(("{", "[")):
        try:
            parsed = json.loads(obj)
        except (json.JSONDecodeError, ValueError):
            return obj
        sanitized = _sanitize_recursive(parsed)
        return json.dumps(sanitized)

    return obj
