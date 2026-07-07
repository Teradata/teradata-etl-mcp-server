"""Credential resolver for connection profiles.

Loads connection profiles from connections.yaml and/or environment variables.
The LLM never sees credentials — it only references profile names.
The server resolves actual credentials server-side before making API calls.
"""

import copy
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Pattern for ${ENV_VAR} interpolation
_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")

# Profile keys that are semantically numeric. Only these keys are coerced from
# str to int/float after ${VAR} substitution.  All other fields (passwords,
# tokens, IDs, host names, …) are left as strings even when the resolved value
# consists entirely of digits.
# port must always be int — passing 22.0 to Paramiko/teradatasql raises TypeError.
# timeout accepts int or float (e.g. 30.5s is valid for most clients).
_INT_PROFILE_KEYS: frozenset[str] = frozenset({"port"})
_NUMERIC_PROFILE_KEYS: frozenset[str] = frozenset({"timeout"})


@dataclass
class ProfileSummary:
    """Summary of a connection profile — NO secrets."""

    name: str
    description: str = ""


class CredentialResolver:
    """Resolves connection credentials from profiles defined in connections.yaml.

    Profiles store only credential/connection data (host, port, username,
    password, etc.).  The LLM determines connector type dynamically and
    passes it as a separate tool parameter.
    """

    def __init__(
        self,
        settings: Any = None,
        connections_file: Path | None = None,
    ) -> None:
        self._settings = settings
        self._profiles: dict[str, dict[str, Any]] = {}
        self._aliases: dict[str, str] = {}
        self._file_found: bool = False
        self._load_error: str | None = None
        self._load_exception: Exception | None = None
        self._file_path_searched: list[str] = []
        if connections_file is not None:
            self._connections_file = connections_file
            self._file_path_searched = [str(connections_file)]
        else:
            self._connections_file = self._find_connections_file()
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def is_configured(self) -> bool:
        """Return ``True`` if a connections file was found and has at least one profile."""
        return self._file_found and bool(self._profiles)

    def guard_configured(self) -> dict[str, Any] | None:
        """Return an LLM-directed error dict if not configured, or ``None`` if OK.

        Tools call this at the top of their ``try`` block for early exit::

            guard = orchestrator.credential_resolver.guard_configured()
            if guard:
                return guard
        """
        if self.is_configured:
            return None
        safe_msg = self._safe_load_error_message
        if safe_msg:
            redacted_file = self._redact_path(str(self._connections_file))
            return {
                "success": False,
                "error": (
                    f"Action required: connections.yaml at "
                    f"{redacted_file} could not be parsed: "
                    f"{safe_msg}. "
                    "Do not create files or use placeholder credentials. "
                    "Ask the user to fix the YAML formatting, "
                    "then call connection_profiles(action='reload')."
                ),
                "searched_locations": self.searched_locations_redacted,
                "setup_instructions": [
                    "1. Show the user the exact parse error above",
                    "2. Ask the user to fix the YAML formatting in connections.yaml",
                    "3. Call connection_profiles(action='reload') to pick up changes",
                ],
            }
        return {
            "success": False,
            "error": (
                "Action required: connections.yaml is not configured. "
                "Named connection profiles (used by the teradata_profile, airbyte_profile, "
                "ssh_profile parameters) require a connections.yaml file. "
                "The agent must NOT create connections.yaml or .env, must NOT use "
                "placeholder credentials, and must NOT shell out to write either file. "
                "Ask the user to copy connections.yaml.example to connections.yaml and "
                "edit it with their real credentials, then call "
                "connection_profiles(action='reload')."
            ),
            "searched_locations": self.searched_locations_redacted,
            "setup_instructions": [
                "1. ASK THE USER to copy connections.yaml.example to connections.yaml.",
                "2. ASK THE USER to edit it with their real credentials.",
                "3. Once the user confirms the file is saved, call "
                    "connection_profiles(action='reload') to pick up changes.",
                "4. Do NOT create connections.yaml or .env yourself, and do NOT use "
                    "placeholder credentials.",
            ],
        }

    @property
    def searched_locations_redacted(self) -> list[str]:
        """Return searched paths with the home directory replaced by ``~``."""
        return [self._redact_path(p) for p in self._file_path_searched]

    @staticmethod
    def _redact_path(path_str: str) -> str:
        """Replace the user's home directory with ``~`` to avoid leaking usernames."""
        home = str(Path.home())
        if path_str.startswith(home):
            return "~" + path_str[len(home) :]
        return path_str

    @staticmethod
    def _safe_parse_error(exc: Exception) -> str:
        """Return a safe, accurate error message for a ``_load()`` exception.

        Classifies the exception so users get correct remediation guidance:
        - YAML parse errors → line/column metadata (no source line content)
        - Permission errors → "permission denied"
        - Encoding errors → "encoding error"
        - Other I/O errors → "could not read file"
        - Anything else → generic fallback
        """
        # YAML parse errors (may embed the offending source line with secrets)
        if hasattr(exc, "problem_mark") and exc.problem_mark is not None:
            mark = exc.problem_mark
            return f"YAML syntax error at line {mark.line + 1}, column {mark.column + 1}"
        if isinstance(exc, yaml.YAMLError):
            return "YAML syntax error (see server logs for details)"
        # File I/O errors
        if isinstance(exc, PermissionError):
            return "Could not read connections.yaml (permission denied)"
        if isinstance(exc, UnicodeDecodeError):
            return "Could not read connections.yaml (encoding error — expected UTF-8)"
        if isinstance(exc, OSError):
            return "Could not read connections.yaml (file I/O error)"
        return "Failed to load connections.yaml (see server logs for details)"

    @property
    def _safe_load_error_message(self) -> str | None:
        """Return a sanitized load-error message safe for tool-facing responses.

        For exceptions captured during ``_load()`` (``_load_exception``):
        classifies the error type (YAML, permission, encoding, I/O) and
        returns a safe message via ``_safe_parse_error``.
        For structural validation errors (``_load_error``): returns the
        hardcoded message as-is.
        Returns ``None`` when no load error occurred.
        """
        if self._load_exception is not None:
            return self._safe_parse_error(self._load_exception)
        return self._load_error

    def resolve_profile(self, profile_name: str) -> dict[str, Any]:
        """Return the full credential dict for a profile.

        The ``description`` key is stripped — it is metadata, not a credential.
        All ``${ENV_VAR}`` placeholders are resolved at load time.

        Raises ``ValueError`` with actionable guidance when:
        - No ``connections.yaml`` file was found
        - The file was found but contains no profiles
        - The requested profile name does not exist
        """
        if not self._file_found:
            redacted = self.searched_locations_redacted
            searched = ", ".join(redacted) if redacted else "none"
            raise ValueError(
                "Action required: connections.yaml not found. "
                "Do not create this file yourself or use placeholder credentials. "
                "Ask the user to create connections.yaml by copying "
                "connections.yaml.example and editing it with their real credentials. "
                f"Searched locations: {searched}"
            )
        if not self._profiles:
            redacted_file = self._redact_path(str(self._connections_file))
            safe_msg = self._safe_load_error_message
            if safe_msg:
                raise ValueError(
                    f"Action required: connections.yaml at "
                    f"{redacted_file} could not be parsed: "
                    f"{safe_msg}. "
                    "Do not create files or use placeholder credentials. "
                    "Ask the user to fix the YAML formatting in connections.yaml."
                )
            raise ValueError(
                f"Action required: connections.yaml at {redacted_file} "
                "contains no profiles. Do not add profiles yourself. "
                "Ask the user to add at least one profile under the 'profiles:' key "
                "with their real credentials."
            )
        resolved_name = self._resolve_name(profile_name)
        if resolved_name not in self._profiles:
            available = ", ".join(sorted(self._profiles.keys()))
            raise ValueError(
                f"Unknown connection profile '{profile_name}'. Available profiles: {available}"
            )
        # Deep-copy so callers cannot mutate the internal cache
        profile = copy.deepcopy(self._profiles[resolved_name])
        profile.pop("description", None)
        return profile

    def list_profiles(self) -> list[ProfileSummary]:
        """Return profile names and descriptions only — NO secrets."""
        summaries: list[ProfileSummary] = []
        for name, data in self._profiles.items():
            summaries.append(
                ProfileSummary(
                    name=name,
                    description=data.get("description", ""),
                )
            )
        return summaries

    _TD_NAME_PATTERN = re.compile(r"(?:^|[_\-])td(?:[_\-]|$)|teradata", re.IGNORECASE)

    def find_teradata_profiles(self) -> list[ProfileSummary]:
        """Return profiles that look like Teradata connections.

        A profile is considered Teradata-like if its name contains
        'teradata' or 'td' as a delimited token (e.g. ``dev_td``,
        ``td_prod``), or if its raw port value is 1025.
        Uses raw profile data to avoid deep-copying secrets.
        """
        results: list[ProfileSummary] = []
        try:
            for name, data in self._profiles.items():
                if self._TD_NAME_PATTERN.search(name) or data.get("host") and str(data.get("port", "")) == "1025":
                    results.append(
                        ProfileSummary(
                            name=name,
                            description=data.get("description", ""),
                        )
                    )
        except Exception:
            logger.debug("Failed to list Teradata profiles", exc_info=True)
        return results

    _SSH_NAME_PATTERN = re.compile(r"(?:^|[_\-])ssh(?:[_\-]|$)|remote", re.IGNORECASE)

    def find_ssh_profiles(self) -> list[ProfileSummary]:
        """Return profiles that look like SSH connections."""
        results: list[ProfileSummary] = []
        try:
            for name, data in self._profiles.items():
                if self._SSH_NAME_PATTERN.search(name) or (
                    data.get("key_file")
                    and str(data.get("port", "")) == "22"
                    and not data.get("database")
                    and not data.get("schemas")
                ):
                    results.append(ProfileSummary(name=name, description=data.get("description", "")))
        except Exception:
            logger.debug("Failed to list SSH profiles", exc_info=True)
        return results

    def reload(self) -> None:
        """Re-read ``connections.yaml`` (e.g. after the user edits it)."""
        self._profiles.clear()
        self._aliases.clear()
        self._file_found = False
        self._load_error = None
        self._load_exception = None
        # Re-discover if the current path is missing/gone; keep it if still valid
        if self._connections_file is None or not self._connections_file.is_file():
            old_path = str(self._connections_file) if self._connections_file else None
            self._connections_file = self._find_connections_file()
            # Preserve the original explicit path in searched locations
            if old_path and old_path not in self._file_path_searched:
                self._file_path_searched.insert(0, old_path)
        self._load()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_connections_file(self) -> Path | None:
        """Locate the connections file using a priority search order:

        1. ``CONNECTIONS_FILE`` environment variable
        2. ``connections.yaml`` in the current working directory
        3. ``settings.security.connections_file`` (if settings provided)

        Populates ``_file_path_searched`` with every path checked.
        """
        searched: list[str] = []

        # 1. Env var
        env_path = os.environ.get("CONNECTIONS_FILE")
        if env_path:
            p = Path(env_path)
            searched.append(str(p))
            if p.is_file():
                self._file_path_searched = searched
                return p

        # 2. CWD
        cwd_path = Path.cwd() / "connections.yaml"
        searched.append(str(cwd_path))
        if cwd_path.is_file():
            self._file_path_searched = searched
            return cwd_path

        # 3. Settings
        if self._settings is not None:
            try:
                sf = self._settings.security.connections_file
                if sf:
                    searched.append(str(sf))
                    if Path(sf).is_file():
                        self._file_path_searched = searched
                        return Path(sf)
            except AttributeError:
                pass

        self._file_path_searched = searched
        return None

    def _load(self) -> None:
        """Load profiles from the YAML file (if found)."""
        self._load_error = None
        self._load_exception = None

        if self._connections_file is None or not self._connections_file.is_file():
            self._file_found = False
            logger.info("No connections.yaml found — credential resolver has no profiles")
            return

        self._file_found = True

        logger.info("Loading connection profiles from %s", self._connections_file)
        try:
            with open(self._connections_file, encoding="utf-8") as fh:
                raw = yaml.safe_load(fh)
        except Exception as exc:
            # Log the full exception (may contain file content) server-side only.
            # Store the raw exception for server-side use; tool-facing responses
            # derive safe messages via _safe_load_error_message.
            logger.error("Failed to parse connections.yaml", exc_info=True)
            self._load_exception = exc
            return

        if not isinstance(raw, dict):
            self._load_error = "root must be a YAML mapping (key: value), not a scalar or list"
            logger.error("connections.yaml: %s", self._load_error)
            return

        # Load profiles
        profiles = raw.get("profiles", {})
        if not isinstance(profiles, dict):
            self._load_error = "'profiles' key must be a mapping"
            logger.error("connections.yaml: %s", self._load_error)
            return

        for name, data in profiles.items():
            if not isinstance(data, dict):
                logger.warning("Skipping profile '%s' — value must be a mapping", name)
                continue
            self._profiles[name] = self._interpolate_env_vars(copy.deepcopy(data))

        # Load aliases
        aliases = raw.get("aliases", {})
        if isinstance(aliases, dict):
            for alias, target in aliases.items():
                if isinstance(target, str):
                    self._aliases[alias] = target

        logger.info(
            "Loaded %d profile(s) and %d alias(es)", len(self._profiles), len(self._aliases)
        )

    def _resolve_name(self, name: str) -> str:
        """Resolve aliases to the canonical profile name."""
        return self._aliases.get(name, name)

    def _interpolate_env_vars(self, value: Any, _key: str | None = None) -> Any:
        """Recursively replace ``${ENV_VAR}`` patterns with environment values.

        ``_key`` carries the parent dict key so that numeric coercion is only
        applied to known numeric fields: ``_INT_PROFILE_KEYS`` (e.g. ``port``)
        are coerced to ``int``; ``_NUMERIC_PROFILE_KEYS`` (e.g. ``timeout``)
        accept ``int`` or ``float``.  All other fields such as ``password`` or
        ``access_token`` remain strings even when the resolved env var value
        consists entirely of digits.
        """
        if isinstance(value, str):

            def _replacer(match: re.Match) -> str:
                var_name = match.group(1)
                env_val = os.environ.get(var_name)
                if env_val is None:
                    logger.warning(
                        "Environment variable '%s' referenced in connections.yaml is not set",
                        var_name,
                    )
                    return match.group(0)  # leave placeholder as-is
                return env_val

            result = _ENV_VAR_PATTERN.sub(_replacer, value)

            # When the entire YAML value is a single ``${VAR}`` reference, YAML
            # had to parse it as a string even if the env var holds a number.
            # Only coerce to int/float for keys that are semantically numeric
            # (e.g. ``port``) to avoid turning passwords, tokens, or IDs that
            # happen to be all-digits into unwanted numeric types.
            if _ENV_VAR_PATTERN.fullmatch(value):
                stripped = result.strip() if isinstance(result, str) else result
                if _key in _INT_PROFILE_KEYS:
                    try:
                        return int(stripped)
                    except (ValueError, TypeError):
                        pass
                elif _key in _NUMERIC_PROFILE_KEYS:
                    try:
                        return int(stripped)
                    except (ValueError, TypeError):
                        try:
                            return float(stripped)
                        except (ValueError, TypeError):
                            pass

            return result

        if isinstance(value, dict):
            return {k: self._interpolate_env_vars(v, _key=k) for k, v in value.items()}

        if isinstance(value, list):
            return [self._interpolate_env_vars(item, _key=_key) for item in value]

        return value
