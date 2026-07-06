"""Validation utilities for connections, configurations, and pipelines.

This module provides validation functions for testing connections,
validating pipeline configurations, and ensuring data integrity.
"""

import logging
import re
from typing import Any

from ..response_sanitizer import safe_error_message

logger = logging.getLogger(__name__)


_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_.\- ]+$")
_SAFE_AIRFLOW_ID = re.compile(r"^[a-zA-Z0-9_.\-]+$")
_SAFE_DAG_RUN_ID = re.compile(r"^[a-zA-Z0-9_.\-:+]+$")
_SAFE_TERADATA_ID = re.compile(r"^[a-zA-Z_$#][a-zA-Z0-9_$#]{0,127}$")
_SLUG_REPLACE = re.compile(r"[^a-z0-9]+")


def slugify_dir_name(name: str, *, max_length: int = 48) -> str:
    """Convert a user-supplied label to a filesystem-safe identifier.

    Lowercases, replaces every run of non-alphanumeric chars with one ``_``,
    strips leading/trailing ``_``, truncates to ``max_length``. Returns
    ``""`` when the input slugifies to nothing — caller is responsible for
    treating that as invalid input.
    """
    if not isinstance(name, str):
        return ""
    s = _SLUG_REPLACE.sub("_", name.lower()).strip("_")
    return s[:max_length].rstrip("_")


def validate_identifier(value: str, field_name: str, max_length: int = 256) -> str | None:
    """Validate database/table/pipeline identifiers. Returns error string or None."""
    if not isinstance(value, str):
        return f"{field_name} must be a string, got {type(value).__name__}"
    if not value or not value.strip():
        return f"{field_name} must be a non-empty string"
    if value != value.strip():
        return f"{field_name} must not have leading or trailing whitespace"
    if len(value) > max_length:
        return f"{field_name} exceeds max length ({max_length})"
    if "\x00" in value:
        return f"{field_name} contains null bytes"
    if not _SAFE_IDENTIFIER.match(value):
        return f"{field_name} contains invalid characters"
    return None


def validate_dag_run_id(value: str, field_name: str = "dag_run_id", max_length: int = 500) -> str | None:
    """Validate Airflow dag_run_id (allows :, + for ISO timestamps)."""
    if not isinstance(value, str):
        return f"{field_name} must be a string, got {type(value).__name__}"
    if not value:
        return f"{field_name} must be a non-empty string"
    if value != value.strip():
        return f"{field_name} must not have leading or trailing whitespace"
    if len(value) > max_length:
        return f"{field_name} exceeds max length ({max_length})"
    if "\x00" in value:
        return f"{field_name} contains null bytes"
    if not _SAFE_DAG_RUN_ID.match(value):
        return f"{field_name} contains invalid characters"
    return None


def validate_airflow_identifier(value: str, field_name: str, max_length: int = 256) -> str | None:
    """Validate Airflow DAG ID / task ID (no spaces allowed)."""
    if not isinstance(value, str):
        return f"{field_name} must be a string, got {type(value).__name__}"
    if not value:
        return f"{field_name} must be a non-empty string"
    if value != value.strip():
        return f"{field_name} must not have leading or trailing whitespace"
    if len(value) > max_length:
        return f"{field_name} exceeds max length ({max_length})"
    if "\x00" in value:
        return f"{field_name} contains null bytes"
    if not _SAFE_AIRFLOW_ID.match(value):
        return f"{field_name} contains invalid characters"
    return None


_TERADATA_DISALLOWED = frozenset(
    "\x00\x1a\ufffd\ufa6c\ufa6f\ufad0\ufad1\ufad5\ufad6\ufad7"
)


def validate_teradata_identifier(value: str, field_name: str) -> str | None:
    """Validate Teradata database/table identifier (allows $, #)."""
    if not isinstance(value, str):
        return f"{field_name} must be a string, got {type(value).__name__}"
    if not value or not value.rstrip():
        return f"{field_name} must be a non-empty string"
    if value != value.lstrip():
        return f"{field_name} must not have leading whitespace"
    value = value.rstrip()
    if _TERADATA_DISALLOWED & set(value):
        return f"{field_name} contains disallowed Teradata characters"
    if not _SAFE_TERADATA_ID.match(value):
        return f"{field_name} contains invalid characters for a Teradata identifier"
    return None


def validate_input_size(
    value: str | dict | list, field_name: str, max_bytes: int = 1_048_576
) -> str | None:
    """Validate input size. Returns error string or None."""
    import json as _json

    size = (
        len(_json.dumps(value).encode("utf-8", errors="replace"))
        if isinstance(value, (dict, list))
        else len(value.encode("utf-8", errors="replace"))
    )
    if size > max_bytes:
        return f"{field_name} exceeds max size ({max_bytes} bytes)"
    return None


class ValidationError(Exception):
    """Custom exception for validation errors."""

    pass


class ConnectionValidator:
    """Validator for testing system connections."""

    @staticmethod
    def validate_teradata_connection(
        host: str,
        user: str,
        password: str,
        database: str | None = None,
        port: int = 1025,
    ) -> tuple[bool, str | None]:
        """
        Validate Teradata connection.

        Args:
            host: Teradata host
            user: Username
            password: Password
            database: Default database
            port: Port number

        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Validate basic parameters
            if not host or not user or not password:
                return False, "Host, user, and password are required"

            if not isinstance(port, int) or port <= 0 or port > 65535:
                return False, f"Invalid port number: {port}"

            # TODO: Attempt actual connection
            # from teradatasql import connect
            # conn = connect(host=host, user=user, password=password, database=database)
            # conn.close()

            logger.info("Teradata connection validation passed for %s", host)
            return True, None

        except Exception as e:
            logger.error("Teradata connection validation failed: %s", e, exc_info=True)
            return False, safe_error_message(e)

    @staticmethod
    def validate_airflow_connection(
        base_url: str,
        username: str | None = None,
        password: str | None = None,
    ) -> tuple[bool, str | None]:
        """
        Validate Airflow API connection.

        Args:
            base_url: Airflow base URL
            username: Optional username for auth
            password: Optional password for auth

        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Validate URL format
            if not base_url.startswith(("http://", "https://")):
                return False, "Base URL must start with http:// or https://"

            # Remove trailing slash
            base_url = base_url.rstrip("/")

            # TODO: Test actual connection
            # import httpx
            # auth = (username, password) if username and password else None
            # response = httpx.get(f"{base_url}/api/v1/health", auth=auth)
            # response.raise_for_status()

            logger.info("Airflow connection validation passed for %s", base_url)
            return True, None

        except Exception as e:
            logger.error("Airflow connection validation failed: %s", e, exc_info=True)
            return False, safe_error_message(e)

    @staticmethod
    def validate_airbyte_connection(
        base_url: str,
        username: str | None = None,
        password: str | None = None,
    ) -> tuple[bool, str | None]:
        """
        Validate Airbyte API connection.

        Args:
            base_url: Airbyte base URL
            username: Optional username for auth
            password: Optional password for auth

        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Validate URL format
            if not base_url.startswith(("http://", "https://")):
                return False, "Base URL must start with http:// or https://"

            # Remove trailing slash
            base_url = base_url.rstrip("/")

            # TODO: Test actual connection
            # import httpx
            # auth = (username, password) if username and password else None
            # response = httpx.get(f"{base_url}/api/v1/health", auth=auth)
            # response.raise_for_status()

            logger.info("Airbyte connection validation passed for %s", base_url)
            return True, None

        except Exception as e:
            logger.error("Airbyte connection validation failed: %s", e, exc_info=True)
            return False, safe_error_message(e)

    @staticmethod
    def validate_dbt_installation(
        project_dir: str,
        profiles_dir: str | None = None,
    ) -> tuple[bool, str | None]:
        """
        Validate dbt installation and configuration.

        Args:
            project_dir: dbt project directory
            profiles_dir: dbt profiles directory

        Returns:
            Tuple of (success, error_message)
        """
        try:
            from pathlib import Path

            # Check project directory exists
            project_path = Path(project_dir)
            if not project_path.exists():
                return False, f"Project directory not found: {project_dir}"

            # Check for dbt_project.yml
            if not (project_path / "dbt_project.yml").exists():
                return False, f"dbt_project.yml not found in {project_dir}"

            # Check profiles directory
            if profiles_dir:
                profiles_path = Path(profiles_dir)
                if not profiles_path.exists():
                    return False, f"Profiles directory not found: {profiles_dir}"

                if not (profiles_path / "profiles.yml").exists():
                    return False, f"profiles.yml not found in {profiles_dir}"

            logger.info("dbt installation validation passed for %s", project_dir)
            return True, None

        except Exception as e:
            logger.error("dbt installation validation failed: %s", e, exc_info=True)
            return False, safe_error_message(e)


def _normalize_quartz_dom_dow(dom: str, dow: str, expr: str) -> tuple[str, str]:
    """Ensure exactly one of dom/dow is '?' as Quartz requires.

    If both already have '?', or one already has '?', return as-is.
    If neither has '?', normalize '*' → '?' on the appropriate field.
    Raises ValueError when both are non-wildcard (e.g. dom=15, dow=1).
    """
    if "?" in (dom, dow):
        # Already has a '?' — check the other isn't also non-wildcard
        if dom != "?" and dow != "?" and dom != "*" and dow != "*":
            raise ValueError(
                f"Quartz cron does not support both day-of-month ({dom}) and "
                f"day-of-week ({dow}) as non-wildcard: {expr!r}"
            )
        return dom, dow

    # Neither field is '?' — need to add one
    dom_is_wildcard = dom == "*"
    dow_is_wildcard = dow == "*"

    if not dom_is_wildcard and not dow_is_wildcard:
        raise ValueError(
            f"Quartz cron does not support both day-of-month ({dom}) and "
            f"day-of-week ({dow}) as non-wildcard. Use '*' for one of them: {expr!r}"
        )
    if not dow_is_wildcard and dom_is_wildcard:
        dom = "?"
    else:
        dow = "?"
    return dom, dow


def to_quartz_cron(expr: str) -> str:
    """Convert a Unix cron (5-field) expression to Quartz cron (6-field).

    If already 6 fields, validates and normalizes the dom/dow interplay
    (ensuring one is '?' as Quartz requires).
    """
    parts = expr.strip().split()
    if len(parts) == 6:
        sec, minute, hour, dom, month, dow = parts
        dom, dow = _normalize_quartz_dom_dow(dom, dow, expr)
        return f"{sec} {minute} {hour} {dom} {month} {dow}"
    if len(parts) != 5:
        raise ValueError(f"Expected 5 or 6 cron fields, got {len(parts)}: {expr!r}")
    minute, hour, dom, month, dow = parts
    dom, dow = _normalize_quartz_dom_dow(dom, dow, expr)
    return f"0 {minute} {hour} {dom} {month} {dow}"


class PipelineValidator:
    """Validator for pipeline configurations."""

    @staticmethod
    def validate_pipeline_name(name: str) -> tuple[bool, str | None]:
        """
        Validate pipeline name format.

        Args:
            name: Pipeline name

        Returns:
            Tuple of (valid, error_message)
        """
        if not name:
            return False, "Pipeline name cannot be empty"

        if len(name) > 250:
            return False, "Pipeline name too long (max 250 characters)"

        # Check for valid characters (alphanumeric, underscore, hyphen)
        if not re.match(r"^[a-zA-Z0-9_-]+$", name):
            return (
                False,
                "Pipeline name can only contain letters, numbers, underscores, and hyphens",
            )

        # Must start with letter
        if not name[0].isalpha():
            return False, "Pipeline name must start with a letter"

        return True, None

    @staticmethod
    def validate_schedule(schedule: str, *, allow_quartz: bool = False) -> tuple[bool, str | None]:
        """
        Validate a cron schedule expression.

        By default validates Airflow-style 5-field Unix cron (and Airflow
        presets like ``@daily``).  Pass ``allow_quartz=True`` to also accept
        6-field Quartz cron expressions (used by Airbyte).

        Args:
            schedule: Schedule string (cron or preset)
            allow_quartz: If True, accept 6-field Quartz cron in addition
                to 5-field Unix cron. Defaults to False (Airflow-strict).

        Returns:
            Tuple of (valid, error_message)
        """
        # Check for preset schedules
        # Normalize @-prefixed presets to lowercase for case-insensitive matching
        presets = [
            "@once",
            "@hourly",
            "@daily",
            "@weekly",
            "@monthly",
            "@yearly",
            "@continuous",
            None,
            "None",
            "none",
            "",
        ]

        normalized = (
            schedule.lower()
            if schedule and (schedule.startswith("@") or schedule.lower() == "none")
            else schedule
        )
        if normalized in presets:
            return True, None

        # Validate cron expression (basic validation)
        if schedule:
            parts = schedule.split()
            allowed_lengths = (5, 6) if allow_quartz else (5,)
            if len(parts) not in allowed_lengths:
                if allow_quartz:
                    return False, "Cron expression must have 5 parts (Unix) or 6 parts (Quartz)"
                return False, "Cron expression must have 5 parts (minute hour dom month dow)"

            # Basic validation of each part
            for _i, part in enumerate(parts):
                if part not in ["*", "?"]:
                    # Check if it's a number, range, list, or step
                    if not re.match(r"^[\d,\-*/]+$", part):
                        return False, f"Invalid cron expression part: {part}"

            # Validate Quartz-specific constraints (dom/dow interplay)
            if allow_quartz:
                try:
                    to_quartz_cron(schedule)
                except ValueError as e:
                    return False, str(e)

        return True, None

    @staticmethod
    def validate_table_list(tables: list[str]) -> tuple[bool, str | None]:
        """
        Validate list of table names.

        Args:
            tables: List of table names

        Returns:
            Tuple of (valid, error_message)
        """
        if not tables:
            return False, "Table list cannot be empty"

        if not isinstance(tables, list):
            return False, "Tables must be a list"

        # Validate each table name
        for table in tables:
            if not isinstance(table, str):
                return False, f"Table name must be a string: {table}"

            if not table:
                return False, "Table name cannot be empty"

            # Check for SQL injection attempts
            if any(char in table for char in [";", "--", "/*", "*/"]):
                return False, f"Invalid characters in table name: {table}"

            # Basic table name format validation
            if not re.match(r"^[a-zA-Z0-9_]+$", table):
                return False, f"Invalid table name format: {table}"

        return True, None

    @staticmethod
    def validate_pipeline_config(config: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate complete pipeline configuration.

        Args:
            config: Pipeline configuration dictionary

        Returns:
            Tuple of (valid, list_of_errors)
        """
        errors = []

        # Required fields
        required_fields = ["pipeline_name", "source_database", "source_tables"]
        for field in required_fields:
            if field not in config:
                errors.append(f"Missing required field: {field}")

        # Validate pipeline name
        if "pipeline_name" in config:
            valid, error = PipelineValidator.validate_pipeline_name(config["pipeline_name"])
            if not valid:
                errors.append(f"pipeline_name: {error}")

        # Validate schedule
        if "schedule" in config:
            valid, error = PipelineValidator.validate_schedule(config["schedule"])
            if not valid:
                errors.append(f"schedule: {error}")

        # Validate source tables
        if "source_tables" in config:
            valid, error = PipelineValidator.validate_table_list(config["source_tables"])
            if not valid:
                errors.append(f"source_tables: {error}")

        # Validate target schema
        if "target_schema" in config:
            schema = config["target_schema"]
            if not isinstance(schema, str) or not schema:
                errors.append("target_schema must be a non-empty string")

        return len(errors) == 0, errors


class DataValidator:
    """Validator for data quality and integrity."""

    @staticmethod
    def validate_column_name(name: str) -> tuple[bool, str | None]:
        """
        Validate column name format.

        Args:
            name: Column name

        Returns:
            Tuple of (valid, error_message)
        """
        if not name:
            return False, "Column name cannot be empty"

        if len(name) > 128:
            return False, "Column name too long (max 128 characters)"

        # Check for valid characters
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", name):
            return (
                False,
                "Column name must start with letter and contain only letters, numbers, and underscores",
            )

        # Reserved keywords (basic list)
        reserved = ["select", "from", "where", "insert", "update", "delete", "drop", "create"]
        if name.lower() in reserved:
            return False, f"Column name cannot be a reserved keyword: {name}"

        return True, None

    @staticmethod
    def validate_data_type(data_type: str) -> tuple[bool, str | None]:
        """
        Validate data type format.

        Args:
            data_type: Data type string

        Returns:
            Tuple of (valid, error_message)
        """
        if not data_type:
            return False, "Data type cannot be empty"

        # Common data types (case-insensitive)
        valid_types = [
            "integer",
            "int",
            "bigint",
            "smallint",
            "tinyint",
            "decimal",
            "numeric",
            "float",
            "double",
            "real",
            "char",
            "varchar",
            "text",
            "clob",
            "date",
            "time",
            "timestamp",
            "datetime",
            "boolean",
            "bool",
            "json",
            "jsonb",
            "xml",
            "binary",
            "varbinary",
            "blob",
        ]

        # Extract base type (before parentheses)
        base_type = data_type.split("(")[0].strip().lower()

        if base_type not in valid_types:
            # Check for parameterized types
            if not re.match(r"^[a-zA-Z]+(\(\d+(,\s*\d+)?\))?$", data_type):
                return False, f"Invalid data type format: {data_type}"

        return True, None

    @staticmethod
    def validate_row_count(
        count: int, min_rows: int = 0, max_rows: int | None = None
    ) -> tuple[bool, str | None]:
        """
        Validate row count is within acceptable range.

        Args:
            count: Row count to validate
            min_rows: Minimum acceptable rows
            max_rows: Maximum acceptable rows (optional)

        Returns:
            Tuple of (valid, error_message)
        """
        if not isinstance(count, int):
            return False, "Row count must be an integer"

        if count < min_rows:
            return False, f"Row count {count} is below minimum {min_rows}"

        if max_rows is not None and count > max_rows:
            return False, f"Row count {count} exceeds maximum {max_rows}"

        return True, None

    @staticmethod
    def validate_null_rate(
        null_count: int, total_count: int, max_null_rate: float = 0.5
    ) -> tuple[bool, str | None]:
        """
        Validate null rate is within acceptable threshold.

        Args:
            null_count: Number of null values
            total_count: Total number of values
            max_null_rate: Maximum acceptable null rate (0.0 to 1.0)

        Returns:
            Tuple of (valid, error_message)
        """
        if total_count == 0:
            return False, "Total count cannot be zero"

        null_rate = null_count / total_count

        if null_rate > max_null_rate:
            return False, f"Null rate {null_rate:.2%} exceeds threshold {max_null_rate:.2%}"

        return True, None


class ConfigValidator:
    """Validator for system configuration."""

    @staticmethod
    def validate_environment_vars(required_vars: list[str]) -> tuple[bool, list[str]]:
        """
        Validate required environment variables are set.

        Args:
            required_vars: List of required variable names

        Returns:
            Tuple of (valid, list_of_missing_vars)
        """
        import os

        missing = []
        for var in required_vars:
            if not os.getenv(var):
                missing.append(var)

        return len(missing) == 0, missing

    @staticmethod
    def validate_file_path(path: str, must_exist: bool = False) -> tuple[bool, str | None]:
        """
        Validate file path format and existence.

        Args:
            path: File path to validate
            must_exist: Whether file must already exist

        Returns:
            Tuple of (valid, error_message)
        """
        from pathlib import Path

        if not path:
            return False, "Path cannot be empty"

        try:
            path_obj = Path(path)

            if must_exist and not path_obj.exists():
                return False, f"Path does not exist: {path}"

            # Check for invalid characters
            if any(char in str(path_obj) for char in ["<", ">", "|", "\0"]):
                return False, f"Invalid characters in path: {path}"

            return True, None

        except Exception as e:
            return False, f"Invalid path format: {e}"

    @staticmethod
    def validate_port(port: int) -> tuple[bool, str | None]:
        """
        Validate port number.

        Args:
            port: Port number

        Returns:
            Tuple of (valid, error_message)
        """
        if not isinstance(port, int):
            return False, "Port must be an integer"

        if port < 1 or port > 65535:
            return False, f"Port must be between 1 and 65535, got {port}"

        # Check for privileged ports (optional warning)
        if port < 1024:
            logger.warning("Port %d is a privileged port (< 1024)", port)

        return True, None

    @staticmethod
    def validate_url(url: str) -> tuple[bool, str | None]:
        """
        Validate URL format.

        Args:
            url: URL to validate

        Returns:
            Tuple of (valid, error_message)
        """
        if not url:
            return False, "URL cannot be empty"

        # Basic URL validation
        url_pattern = re.compile(
            r"^https?://"  # http:// or https://
            r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"  # domain
            r"localhost|"  # localhost
            r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # IP
            r"(?::\d+)?"  # optional port
            r"(?:/?|[/?]\S+)$",
            re.IGNORECASE,
        )

        if not url_pattern.match(url):
            return False, f"Invalid URL format: {url}"

        return True, None


def validate_all_connections(config: dict[str, Any]) -> dict[str, Any]:
    """
    Validate all system connections.

    Args:
        config: Configuration dictionary with connection details

    Returns:
        Dictionary with validation results for each connection
    """
    results = {
        "valid": True,
        "checks": {},
    }

    # Validate Teradata connection
    if "teradata" in config:
        td_config = config["teradata"]
        valid, error = ConnectionValidator.validate_teradata_connection(
            host=td_config.get("host", ""),
            user=td_config.get("user", ""),
            password=td_config.get("password", ""),
            database=td_config.get("database"),
            port=td_config.get("port", 1025),
        )
        results["checks"]["teradata"] = {
            "valid": valid,
            "error": error,
        }
        if not valid:
            results["valid"] = False

    # Validate Airflow connection
    if "airflow" in config:
        af_config = config["airflow"]
        valid, error = ConnectionValidator.validate_airflow_connection(
            base_url=af_config.get("base_url", ""),
            username=af_config.get("username"),
            password=af_config.get("password"),
        )
        results["checks"]["airflow"] = {
            "valid": valid,
            "error": error,
        }
        if not valid:
            results["valid"] = False

    # Validate Airbyte connection
    if "airbyte" in config:
        ab_config = config["airbyte"]
        valid, error = ConnectionValidator.validate_airbyte_connection(
            base_url=ab_config.get("base_url", ""),
            username=ab_config.get("username"),
            password=ab_config.get("password"),
        )
        results["checks"]["airbyte"] = {
            "valid": valid,
            "error": error,
        }
        if not valid:
            results["valid"] = False

    # Validate dbt installation
    if "dbt" in config:
        dbt_config = config["dbt"]
        valid, error = ConnectionValidator.validate_dbt_installation(
            project_dir=dbt_config.get("project_dir", ""),
            profiles_dir=dbt_config.get("profiles_dir"),
        )
        results["checks"]["dbt"] = {
            "valid": valid,
            "error": error,
        }
        if not valid:
            results["valid"] = False

    return results
