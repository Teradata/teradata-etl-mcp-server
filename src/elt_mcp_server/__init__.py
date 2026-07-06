"""ELT MCP Server package metadata."""

from __future__ import annotations

try:
    # Prefer distribution metadata when installed
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _dist_version

    __version__ = _dist_version("elt-mcp-server")
except Exception:
    # Fallback to pyproject version to avoid import errors during local dev
    __version__ = "0.1.0"

# Short description for CLI `version` command
__description__ = "Unified MCP server for ELT operations with Teradata, Airbyte, Airflow, and dbt"
