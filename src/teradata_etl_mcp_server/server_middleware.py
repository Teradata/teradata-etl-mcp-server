"""FastMCP middleware: parameter aliases + literal-enum error enrichment.

Two agent-friendliness fixes folded into one middleware:

1. **Alias rewrite (pre-validation).** The LLM often guesses natural-
   language parameter names (``query`` instead of ``sql``, ``csv_file``
   instead of ``source_file_name``). The middleware rewrites those
   guesses to canonical kwarg names before Pydantic validation runs,
   so the underlying tool sees what it expects without the LLM's call
   being rejected.

2. **Literal-enum error enrichment (post-validation).** Pydantic's
   default message for an invalid Literal value is "Input should be
   'foo', 'bar' or 'baz'", but FastMCP wraps that in a generic
   ValidationError that just says "must be equal to one of the allowed
   values" — without listing them. The middleware catches the
   ValidationError, pulls the expected set from ``error['ctx']['expected']``,
   and returns a tool-result error message naming the field and the
   allowed values so the LLM can self-correct in one round trip.

Aliases are deliberately NOT part of the JSON schema — clients see the
canonical names only. The middleware accepts the alias on the way in,
which is sufficient for an LLM that's pattern-matching from natural
language.
"""

from __future__ import annotations

from typing import Any

from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import (
    CallNext,
    Middleware,
    MiddlewareContext,
)
from fastmcp.tools.tool import ToolResult
from mcp.types import CallToolRequestParams
from pydantic import ValidationError

# Per-tool alias map: alias name → canonical kwarg name.
#
# Aliases are not part of the JSON schema (clients still see canonical
# names) but the middleware accepts them and rewrites before validation.
#
# Adding a new alias: confirm the canonical name is an actual kwarg on
# the registered tool — there's a sanity test for this.
TOOL_PARAM_ALIASES: dict[str, dict[str, str]] = {
    "ttu_execute": {
        "query": "sql",
        "statement": "sql",
        "queries": "sql_statements",
        "csv_file": "source_file_name",
        "csv_path": "source_file_name",
        "file_path": "source_file_name",
        "input_file": "source_file_name",
        "output_file": "target_file_name",
        # ``table`` defaults to the target side; if the user also passes
        # ``source_table`` or ``target_table`` explicitly, the conflict
        # check below errors out cleanly.
        "table": "target_table",
        "destination_table": "target_table",
        "delimiter": "source_text_delimiter",
        "bteq_script": "script",
    },
    "dbt_execute": {"model": "models"},
    "dbt_generate_model": {
        "source_db": "source_database",
        "tables": "source_tables",
        "table": "source_table",
        "schema": "target_schema",
        "database": "target_database",
        # ``models`` (plural) in mart/intermediate context maps to
        # ``source_models``; the staging branch uses ``source_tables``
        # which has its own alias above.
        "models": "source_models",
        "columns": "select_columns",
        "filter": "where_clause",
        "where": "where_clause",
        "primary_key": "unique_key",
        "project": "project_name",
    },
    "dbt_project": {
        "name": "project_name",
        "project": "project_name",
        # The CSV-loading actions take ``csv_files: list[str]`` — plural.
        # Single-file aliases would require value-shape transformation
        # (str → [str]) which the rename-only normalizer doesn't do.
        # Users pass ``csv_files=['<path>']`` directly.
    },
    "dbt_info": {"model": "model_name", "project": "project_name"},
    "dbt_docs": {"project": "project_name"},
    "airflow_teradata_load": {
        "csv_file": "csv_path",
        "csv_file_path": "csv_path",
        "file_path": "csv_path",
        "table": "target_table",
        "destination_table": "target_table",
        "database": "target_database",
        "models": "dbt_models",
        "project": "project_name",
    },
    "pipeline_deploy": {
        "dag_file_path": "dag_file",
        "filename": "output_filename",
        "output_file": "output_filename",
        "files": "csv_files",
        "dags_dir": "local_dags_dir",
        "ssh_key": "ssh_key_path",
        "key_path": "ssh_key_path",
        "project": "project_name",
        "models": "dbt_models",
    },
    "pipeline_control": {"schedule": "new_schedule", "cron": "new_schedule"},
    "pipeline_validate": {
        "config": "pipeline_config",
        "configuration": "pipeline_config",
    },
    "airflow_connections": {"conn_id": "connection_id", "type": "conn_type"},
    "dag_trigger": {
        "configuration": "config",
        "tasks": "task_ids",
        "run_id": "dag_run_id",
    },
    "dag_monitor": {"task": "task_id", "run_id": "dag_run_id"},
    "teradata_discover": {
        "db": "database",
        "schema": "database",
        "pattern": "table_pattern",
        "search": "search_term",
    },
    "teradata_analyze": {
        "db": "database",
        "schema": "database",
        "table": "table_name",
        "column": "column_name",
    },
    "airbyte_pipeline": {
        "name": "connection_name",
        "cron": "schedule_cron",
        "cron_expression": "schedule_cron",
    },
    "airbyte_sync": {"job": "job_id"},
    "airbyte_inventory": {
        "type": "list_type",
        "search": "search_term",
        "query": "search_term",
    },
}


def _normalize_aliases(tool_name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Rewrite alias kwargs to canonical names.

    When the caller passes BOTH the alias and the canonical name with
    non-None values, raise ``ValueError`` — the middleware translates
    that to a clean tool-level error rather than silently dropping
    one of them.

    A canonical that is present but explicitly ``None`` is treated as
    "not supplied" and the alias takes effect.
    """
    aliases = TOOL_PARAM_ALIASES.get(tool_name)
    if not aliases:
        return args
    out = dict(args)
    for alias, canonical in aliases.items():
        if alias not in out:
            continue
        canonical_present = (
            canonical in out and out[canonical] is not None
        )
        alias_value = out[alias]
        if canonical_present and alias_value is not None:
            raise ValueError(
                f"Tool '{tool_name}' received both '{alias}' (alias) and "
                f"'{canonical}' (canonical) — pass only one."
            )
        if not canonical_present:
            out[canonical] = alias_value
        out.pop(alias, None)
    return out


def _format_validation_error(exc: ValidationError) -> str:
    """Format a Pydantic ``ValidationError`` for the tool-result text.

    ``literal_error`` cases get the allowed values appended; other
    error types get a field-name + raw message line. Messages are
    joined with ``; `` so a multi-error ValidationError surfaces every
    field at once.
    """
    parts: list[str] = []
    for err in exc.errors():
        loc = err.get("loc") or ("argument",)
        field = ".".join(str(p) for p in loc)
        if err.get("type") == "literal_error":
            expected = (err.get("ctx") or {}).get("expected", "<unknown>")
            parts.append(
                f"Invalid value for '{field}': {err.get('msg', '')}. "
                f"Allowed values: {expected}."
            )
        else:
            parts.append(f"{field}: {err.get('msg', '')}")
    return "; ".join(parts)


class ParamAliasingAndEnumErrorEnrichmentMiddleware(Middleware):
    """FastMCP middleware combining alias rewrite + enum error enrichment.

    See module docstring for the rationale. The middleware is invisible
    when the LLM uses canonical kwargs and there are no validation
    errors — existing tests written against canonical names continue
    to pass without changes.
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        # Phase 1 — alias rewrite (pre-validation). Conflict between
        # alias and canonical raises ``ValueError``; we re-raise as
        # ``ToolError`` so FastMCP/lowlevel-server marks the response
        # ``isError=True`` with our message.
        params = context.message
        tool_name = getattr(params, "name", None)
        args = getattr(params, "arguments", None)
        if tool_name and isinstance(args, dict):
            try:
                params.arguments = _normalize_aliases(tool_name, args)
            except ValueError as e:
                raise ToolError(str(e)) from e

        # Phase 2 — run the tool, enrich Pydantic ``ValidationError``.
        # The lowlevel MCP server's request handler catches this and
        # produces ``isError=True`` with the formatted message — letting
        # the LLM self-correct in one round trip.
        try:
            return await call_next(context)
        except ValidationError as e:
            raise ToolError(_format_validation_error(e)) from e
