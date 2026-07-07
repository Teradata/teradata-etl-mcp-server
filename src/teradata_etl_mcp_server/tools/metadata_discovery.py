"""Teradata metadata discovery and analysis tools.

This module provides MCP tools for discovering, analyzing, and profiling
Teradata table metadata — consolidated into two router tools.
"""

import asyncio
import logging
from typing import Any, Literal

from ..auth import is_explicit_profile, resolve_teradata_auth
from ..clients.teradata_client import TeradataClient
from ..orchestrator import PipelineOrchestrator
from ..response_sanitizer import safe_error_message, sanitize_response
from ..utils.validators import validate_teradata_identifier

logger = logging.getLogger(__name__)


def register_metadata_tools(orchestrator: PipelineOrchestrator) -> dict[str, Any]:
    """Register metadata discovery tools.

    Args:
        orchestrator: Pipeline orchestrator instance

    Returns:
        Dictionary of tool functions
    """

    # ══════════════════════════════════════════════════════════════
    #  Client resolution helper
    # ══════════════════════════════════════════════════════════════

    def _resolve_client(
        teradata_profile: str | None,
    ) -> tuple[TeradataClient | None, dict[str, Any] | None]:
        """Return (client, error_dict). error_dict is non-None on resolver
        failure (missing connections.yaml / unknown profile / YAML parse
        error).

        When no profile is named, the orchestrator's default TeradataClient
        (bound to the wizard's identity) is reused. When a profile IS named,
        a fresh TeradataClient is constructed from the profile's
        :class:`TeradataAuth` so the profile's mechanism and all
        mechanism-specific fields flow through to the driver.
        """
        # Sentinel folding: ``"wizard"``/``"default"`` and whitespace-only
        # values are documented "no-profile" markers (see
        # ``_normalize_profile_name``); they must take the orchestrator-
        # default path and NOT call ``guard_configured()`` (which would
        # require connections.yaml even for wizard-default usage).
        if not is_explicit_profile(teradata_profile):
            return orchestrator.teradata_client, None
        guard = orchestrator.credential_resolver.guard_configured()
        if guard:
            return None, guard
        try:
            auth = resolve_teradata_auth(
                settings=orchestrator.settings.teradata,
                credential_resolver=orchestrator.credential_resolver,
                teradata_profile=teradata_profile,
            )
        except ValueError as e:
            return None, {"success": False, "error": str(e)}
        return TeradataClient(auth=auth), None

    # ══════════════════════════════════════════════════════════════
    #  Private helpers (original implementations preserved)
    # ══════════════════════════════════════════════════════════════

    async def _test_connection(
        client: TeradataClient, database: str | None = None
    ) -> dict[str, Any]:
        info = await asyncio.to_thread(client.test_connection)
        if not isinstance(info, dict) or not info.get("connected"):
            error = (
                info.get("error", "Unknown error") if isinstance(info, dict) else "Unknown error"
            )
            return {
                "success": False,
                "status": "failed",
                "host": client.host,
                "error": error,
                "message": "Cannot reach Teradata. Check host, credentials, and network.",
            }
        if database:
            database = database.strip()
            exists = await asyncio.to_thread(client.check_database_exists, database)
            if not exists:
                return {
                    "success": False,
                    "status": "failed",
                    "host": client.host,
                    "database": database,
                    "version": info.get("version", "unknown"),
                    "error": f"Database '{database}' does not exist or is not accessible.",
                    "message": (
                        f"Host connectivity is fine, but database '{database}' was not found or is not accessible."
                    ),
                }
        return {
            "success": True,
            "status": "connected",
            "host": client.host,
            "database": database or client.database,
            "version": info.get("version", "unknown"),
            "message": "Teradata connection is healthy",
        }

    async def _discover_tables(
        client: TeradataClient,
        database: str,
        table_pattern: str = "%",
        include_size_estimates: bool = True,
    ) -> dict[str, Any]:
        tables_raw = await asyncio.to_thread(
            client.search_metadata,
            search_term=table_pattern,
            search_type="table",
            database_name=database,
        )
        tables = []
        for table_info in tables_raw:
            table_name = table_info.get("table") or table_info.get("table_name")
            table_data = {
                "table_name": table_name,
                "table_type": table_info.get("table_type"),
                "description": table_info.get("description"),
                "created_at": table_info.get("created_at"),
            }
            if include_size_estimates:
                try:
                    size_info = await asyncio.to_thread(
                        client.estimate_table_size, database, table_name
                    )
                    table_data["size_mb"] = size_info.get("size_mb")
                    table_data["size_gb"] = size_info.get("size_gb")
                except Exception as e:
                    logger.warning("Failed to get size for %s: %s", table_name, e)
                    table_data["size_mb"] = None
                    table_data["size_gb"] = None
            tables.append(table_data)
        return {"database": database, "table_count": len(tables), "tables": tables}

    async def _enumerate_tables(
        client: TeradataClient, database: str, table_pattern: str = "%"
    ) -> dict[str, Any]:
        def _get_tables():
            import fnmatch

            all_tables = client.list_tables(database)
            pattern = table_pattern.replace("%", "*").replace("_", "?")
            return [t for t in all_tables if fnmatch.fnmatchcase(t["table"], pattern)]

        tables = await asyncio.to_thread(_get_tables)
        return {"database": database, "table_count": len(tables), "tables": tables}

    async def _search_metadata(
        client: TeradataClient,
        database: str,
        search_term: str,
        search_scope: str = "all",
    ) -> dict[str, Any]:
        tables = []
        columns = []
        if search_scope in ["tables", "all"]:
            tables_raw = await asyncio.to_thread(
                client.search_metadata,
                search_term=search_term,
                search_type="table",
                database_name=database,
            )
            tables = [
                {
                    "table_name": t.get("table") or t.get("table_name"),
                    "table_type": t.get("table_type"),
                    "description": t.get("description"),
                }
                for t in tables_raw
            ]
        if search_scope in ["columns", "all"]:
            columns_raw = await asyncio.to_thread(
                client.search_metadata,
                search_term=search_term,
                search_type="column",
                database_name=database,
            )
            columns = [
                {
                    "table_name": c.get("table") or c.get("table_name"),
                    "column_name": c.get("column") or c.get("column_name"),
                    "data_type": c.get("column_type") or c.get("data_type"),
                    "description": c.get("description"),
                }
                for c in columns_raw
            ]
        total_matches = len(tables) + len(columns)
        return {
            "database": database,
            "search_term": search_term,
            "search_scope": search_scope,
            "tables": tables,
            "columns": columns,
            "total_matches": total_matches,
        }

    async def _describe_table(
        client: TeradataClient,
        database: str,
        table_name: str,
        include_statistics: bool = False,
    ) -> dict[str, Any]:
        metadata = await asyncio.to_thread(
            client.get_table_metadata,
            database,
            table_name,
            include_statistics,
        )
        size_info = await asyncio.to_thread(client.estimate_table_size, database, table_name)
        metadata.update(size_info)
        return metadata

    async def _profile_table(
        client: TeradataClient,
        database: str,
        table_name: str,
        sample_size: int = 10000,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            client.profile_table,
            database_name=database,
            table_name=table_name,
            sample_size=sample_size,
        )

    async def _compare_structure(
        client: TeradataClient,
        database: str,
        table_name: str,
        baseline_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current_metadata = await asyncio.to_thread(
            client.get_table_metadata, database, table_name, False
        )
        if not baseline_metadata:
            return {
                "database": database,
                "table_name": table_name,
                "current_metadata": current_metadata,
                "changes": "No baseline provided for comparison",
            }
        current_cols: dict[str, dict[str, Any]] = {}
        for col in current_metadata.get("columns", []):
            col_name = col.get("name")
            if col_name:
                current_cols[col_name] = col
        baseline_cols: dict[str, dict[str, Any]] = {}
        for col in baseline_metadata.get("columns", []):
            col_name = col.get("name")
            if col_name:
                baseline_cols[col_name] = col
        added = [name for name in current_cols if name not in baseline_cols]
        removed = [name for name in baseline_cols if name not in current_cols]
        modified = []
        for col_name in current_cols:
            if col_name in baseline_cols:
                curr = current_cols[col_name]
                base = baseline_cols[col_name]
                if curr.get("type") != base.get("type"):
                    modified.append(
                        {
                            "column": col_name,
                            "old_type": base.get("type"),
                            "new_type": curr.get("type"),
                        }
                    )
        return {
            "database": database,
            "table_name": table_name,
            "columns_added": added,
            "columns_removed": removed,
            "columns_modified": modified,
            "has_changes": bool(added or removed or modified),
        }

    async def _analyze_column(
        client: TeradataClient,
        database: str,
        table_name: str,
        column_name: str | None = None,
    ) -> dict[str, Any]:
        stats_list = await asyncio.to_thread(
            client.get_column_statistics,
            database,
            table_name,
            column_name,
        )
        return {
            "database": database,
            "table_name": table_name,
            "column_count": len(stats_list),
            "columns": stats_list,
        }

    async def _estimate_size(
        client: TeradataClient, database: str, table_name: str
    ) -> dict[str, Any]:
        return await asyncio.to_thread(client.estimate_table_size, database, table_name)

    async def _analyze_dependencies(
        client: TeradataClient,
        database: str,
        table_name: str,
        include_upstream: bool = True,
        include_downstream: bool = True,
    ) -> dict[str, Any]:
        lineage = await asyncio.to_thread(client.get_table_lineage, database, table_name)
        return {
            "database": database,
            "table_name": table_name,
            "upstream_tables": lineage.get("upstream", []) if include_upstream else [],
            "downstream_tables": lineage.get("downstream", []) if include_downstream else [],
            "query_log_available": lineage.get("query_log_available", False),
        }

    async def _preview_data(
        client: TeradataClient,
        database: str,
        table_name: str,
        limit: int = 100,
        sample_method: str = "top",
    ) -> dict[str, Any]:
        rows = await asyncio.to_thread(client.preview_data, database, table_name, limit)
        return {
            "database": database,
            "table_name": table_name,
            "limit": limit,
            "row_count": len(rows),
            "rows": rows,
            "sample_method": sample_method,
        }

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 1: teradata_discover
    # ══════════════════════════════════════════════════════════════

    async def teradata_discover(
        action: Literal[
            "test_connection", "discover_tables", "enumerate_tables", "search_metadata"
        ],
        database: str | None = None,
        table_pattern: str = "%",
        table_name: str | None = None,
        search_term: str | None = None,
        search_scope: str = "all",
        include_size_estimates: bool = True,
        teradata_profile: str | None = None,
    ) -> dict[str, Any]:
        """Discover Teradata tables, enumerate objects, search metadata, or test connectivity.

        Connection: follows the server's wizard-vs-profile selection policy
        (see the server ``instructions``). Default is the wizard connection
        unless the user names a profile via ``teradata_profile``.

        Args:
            action: One of:
                - "test_connection"   — Test Teradata connectivity (no other params needed).
                - "discover_tables"   — Find tables matching a pattern with optional size estimates.
                - "enumerate_tables"  — Lightweight table listing (name + type only).
                - "search_metadata"   — Wildcard search across tables and/or columns.
            database: Database name (required for all actions except test_connection).
            table_pattern: SQL LIKE pattern for table names (default '%'). Used by
                discover_tables and enumerate_tables.
            table_name: Alias for table_pattern -- use this when looking up a specific table
                by name rather than a pattern. Only used when table_pattern is left as
                its default '%'; an explicit table_pattern takes precedence.
            search_term: Search term for search_metadata action.
            search_scope: Scope for search_metadata: 'tables', 'columns', or 'all' (default 'all').
            include_size_estimates: Include size estimates in discover_tables (default True).
            teradata_profile: Profile name from connections.yaml for Teradata
                credentials. Falls back to .env TERADATA_* settings when not provided.

        Returns:
            Dictionary with discovery results.
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        action = action.strip().lower()
        # Normalize whitespace on table_pattern and table_name up front.
        if not isinstance(table_pattern, str):
            return {"success": False, "error": "Parameter 'table_pattern' must be a string."}
        table_pattern = table_pattern.strip() or "%"
        if table_name is not None and not isinstance(table_name, str):
            return {"success": False, "error": "Parameter 'table_name' must be a string."}
        if table_name:
            table_name = table_name.strip()
        # table_name is an alias for table_pattern when no explicit pattern was given.
        if table_name and table_pattern == "%":
            table_pattern = table_name

        _all_valid_actions = {
            "test_connection",
            "discover_tables",
            "enumerate_tables",
            "search_metadata",
        }
        if action not in _all_valid_actions:
            return {
                "success": False,
                "error": (
                    f"Unknown action '{action}'. "
                    "Valid actions: test_connection, discover_tables, "
                    "enumerate_tables, search_metadata"
                ),
            }

        try:
            client, guard = _resolve_client(teradata_profile)
            if guard:
                return guard

            if database is not None:
                if not isinstance(database, str):
                    return {
                        "success": False,
                        "error": "Parameter 'database' must be a string.",
                    }
                database = database.strip()
                err = validate_teradata_identifier(database, "database")
                if err:
                    return {"success": False, "error": err}
            # Validate table_name only when it is the active pattern.
            if table_name and table_pattern == table_name:
                err = validate_teradata_identifier(table_name, "table_name")
                if err:
                    return {"success": False, "error": err}

            if action == "test_connection":
                return sanitize_response(await _test_connection(client, database))

            if not database:
                return {
                    "success": False,
                    "error": "Parameter 'database' is required for this action.",
                }

            if action == "search_metadata":
                if search_term is None:
                    return {
                        "success": False,
                        "error": "Parameter 'search_term' is required for search_metadata.",
                    }
                if not isinstance(search_term, str):
                    return {
                        "success": False,
                        "error": "Parameter 'search_term' must be a string.",
                    }
                search_term = search_term.strip()
                if not search_term:
                    return {
                        "success": False,
                        "error": "Parameter 'search_term' is required for search_metadata.",
                    }

            db_exists = await asyncio.to_thread(client.check_database_exists, database)
            if not db_exists:
                return {
                    "success": False,
                    "error": f"Database '{database}' does not exist or is not accessible.",
                }

            if action == "discover_tables":
                return sanitize_response(
                    await _discover_tables(client, database, table_pattern, include_size_estimates)
                )
            elif action == "enumerate_tables":
                return sanitize_response(await _enumerate_tables(client, database, table_pattern))
            elif action == "search_metadata":
                return sanitize_response(
                    await _search_metadata(client, database, search_term, search_scope)
                )
        except Exception as e:
            logger.error("teradata_discover(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 2: teradata_analyze
    # ══════════════════════════════════════════════════════════════

    async def teradata_analyze(
        analysis_type: Literal[
            "describe_table",
            "profile_table",
            "compare_structure",
            "analyze_column",
            "estimate_size",
            "analyze_dependencies",
            "preview_data",
        ],
        database: str,
        table_name: str,
        include_statistics: bool = False,
        sample_size: int = 10000,
        column_name: str | None = None,
        baseline_metadata: dict[str, Any] | None = None,
        include_upstream: bool = True,
        include_downstream: bool = True,
        limit: int = 100,
        sample_method: str = "top",
        teradata_profile: str | None = None,
    ) -> dict[str, Any]:
        """Analyze a specific Teradata table — describe, profile, compare, or preview.

        Connection: follows the server's wizard-vs-profile selection policy
        (see the server ``instructions``). Default is the wizard connection
        unless the user names a profile via ``teradata_profile``.

        Args:
            analysis_type: One of:
                - "describe_table"        — Full column metadata and size estimate.
                - "profile_table"         — Statistical profiling (min/max/nulls/cardinality).
                - "compare_structure"     — Detect schema drift against a baseline.
                - "analyze_column"        — Column-level stats (null %, cardinality).
                - "estimate_size"         — Storage size estimate only.
                - "analyze_dependencies"  — Upstream/downstream lineage from query logs.
                - "preview_data"          — Sample rows from the table.
            database: Database name.
            table_name: Table name.
            include_statistics: Include column statistics in describe_table (default False).
            sample_size: Row sample size for profile_table (default 10000).
            column_name: Specific column for analyze_column (None = all columns).
            baseline_metadata: Baseline schema dict for compare_structure.
            include_upstream: Include upstream tables in analyze_dependencies (default True).
            include_downstream: Include downstream tables in analyze_dependencies (default True).
            limit: Row count for preview_data (default 100).
            sample_method: 'top' or 'sample' for preview_data (default 'top').
            teradata_profile: Profile name from connections.yaml for Teradata
                credentials. Falls back to .env TERADATA_* settings when not provided.

        Returns:
            Dictionary with analysis results.
        """
        if not isinstance(analysis_type, str) or not analysis_type.strip():
            return {
                "success": False,
                "error": "Parameter 'analysis_type' must be a non-empty string.",
            }
        if sample_size < 1:
            return {"success": False, "error": "Parameter 'sample_size' must be >= 1."}
        if limit < 1:
            return {"success": False, "error": "Parameter 'limit' must be >= 1."}
        analysis_type = analysis_type.strip().lower()
        try:
            client, guard = _resolve_client(teradata_profile)
            if guard:
                return guard

            err = validate_teradata_identifier(database, "database")
            if err:
                return {"success": False, "error": err}
            err = validate_teradata_identifier(table_name, "table_name")
            if err:
                return {"success": False, "error": err}

            if analysis_type == "describe_table":
                return sanitize_response(
                    await _describe_table(client, database, table_name, include_statistics)
                )
            elif analysis_type == "profile_table":
                return sanitize_response(
                    await _profile_table(client, database, table_name, sample_size)
                )
            elif analysis_type == "compare_structure":
                return sanitize_response(
                    await _compare_structure(client, database, table_name, baseline_metadata)
                )
            elif analysis_type == "analyze_column":
                return sanitize_response(
                    await _analyze_column(client, database, table_name, column_name)
                )
            elif analysis_type == "estimate_size":
                return sanitize_response(await _estimate_size(client, database, table_name))
            elif analysis_type == "analyze_dependencies":
                return sanitize_response(
                    await _analyze_dependencies(
                        client, database, table_name, include_upstream, include_downstream
                    )
                )
            elif analysis_type == "preview_data":
                return sanitize_response(
                    await _preview_data(client, database, table_name, limit, sample_method)
                )
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown analysis_type '{analysis_type}'. "
                        "Valid types: describe_table, profile_table, compare_structure, "
                        "analyze_column, estimate_size, analyze_dependencies, preview_data"
                    ),
                }
        except Exception as e:
            logger.error("teradata_analyze(%s) failed: %s", analysis_type, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ── Return router tools ────────────────────────────────────────
    return {
        "teradata_discover": teradata_discover,
        "teradata_analyze": teradata_analyze,
    }
