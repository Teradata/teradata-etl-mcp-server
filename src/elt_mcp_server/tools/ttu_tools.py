"""TTU (Teradata Tools & Utilities) management tools.

This module provides MCP tools for executing local TPT (tbuild, tdload)
and BTEQ operations against Teradata databases.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from ..auth import TeradataAuth, resolve_teradata_auth
from ..clients.ttu_client import TTUClient, TTUNotInstalledError
from ..orchestrator import PipelineOrchestrator
from ..response_sanitizer import safe_error_message, sanitize_response

logger = logging.getLogger(__name__)

_MLOAD_LOCK_PATTERNS = [
    re.compile(r"table.*is being loaded", re.IGNORECASE),
    re.compile(r"MLOAD.*lock", re.IGNORECASE),
    re.compile(r"HUT.*lock", re.IGNORECASE),
    re.compile(r"Failure\s+2652", re.IGNORECASE),
    re.compile(r"Failure\s+2583", re.IGNORECASE),
    re.compile(r"\[Error\s+2652\]", re.IGNORECASE),
    re.compile(r"\[Error\s+2583\]", re.IGNORECASE),
    re.compile(r"apply phase.*not.*complete", re.IGNORECASE),
    re.compile(r"acquisition phase.*not.*complete", re.IGNORECASE),
    re.compile(r"already.*has.*a.*MultiLoad", re.IGNORECASE),
]

_TABLE_NAME_RE = re.compile(
    r'(?i)table\s+(?:"([^"]+\.[^"]+)"'
    r"|'([^']+\.[^']+)'"
    r"|([A-Za-z_$#][A-Za-z0-9_$#]*\.[A-Za-z_$#][A-Za-z0-9_$#]*))"
)

_SAFE_TD_NAME = re.compile(
    r"^[A-Za-z_$#][A-Za-z0-9_$#]{0,127}\.[A-Za-z_$#][A-Za-z0-9_$#]{0,127}$"
)


def _detect_mload_lock(result: dict[str, Any]) -> dict[str, Any] | None:
    output = (result.get("stdout", "") + "\n" + result.get("stderr", "")).strip()
    for pattern in _MLOAD_LOCK_PATTERNS:
        match = pattern.search(output)
        if match:
            matched_line = match.group(0)
            table_match = _TABLE_NAME_RE.search(output)
            raw_name = (
                next((g for g in table_match.groups() if g), None)
                if table_match
                else None
            )
            if raw_name and _SAFE_TD_NAME.match(raw_name):
                table_name = raw_name
                db_name = table_name.split(".")[0]
            else:
                table_name = "<table_name>"
                db_name = "<database_name>"
            return {
                "success": False,
                "error": f"MLOAD/TPT lock detected on {table_name}: {matched_line}",
                "lock_detected": True,
                "requires_confirmation": True,
                "action": "mload_lock_remediation",
                "hint": "Use the SQL in remediation steps with confirm=True to release the lock.",
                "table": table_name,
                "remediation": {
                    "description": (
                        "The target table is locked by an active or incomplete "
                        "MLOAD/TPT operation. The lock may persist after job "
                        "failure or termination."
                    ),
                    "steps": [
                        {
                            "step": 1,
                            "action": "Check for active loading sessions",
                            "sql": (
                                "SELECT * FROM DBC.SessionTbl "
                                f"WHERE DatabaseName = '{db_name}';"
                            ),
                        },
                        {
                            "step": 2,
                            "action": "Release the MLOAD lock (requires confirm=True)",
                            "sql": f"RELEASE MLOAD {table_name};",
                        },
                        {
                            "step": 3,
                            "action": "Drop error tables if they exist (requires confirm=True)",
                            "sql": (
                                f"DROP TABLE {table_name}_ET;\n"
                                f"DROP TABLE {table_name}_UV;\n"
                                f"DROP TABLE {table_name}_WT;\n"
                                f"DROP TABLE {table_name}_LOG;"
                            ),
                        },
                        {
                            "step": 4,
                            "action": "Retry the original operation after lock is released",
                        },
                    ],
                    "notes": [
                        "RELEASE MLOAD requires appropriate privileges on the table.",
                        "Dropping error tables alone may not release the lock in all cases.",
                        "If the above steps fail, contact a DBA to kill the blocking session.",
                        "Automatic lock timeout is system-dependent and may take hours.",
                    ],
                },
            }
    return None


_WIZARD_PROFILE_NAMES: frozenset[str] = frozenset({"wizard", "default", ""})


def _connection_source(teradata_profile: str | None) -> str:
    """Return the tag identifying which connection a tool call used.

    ``"wizard"`` when no profile was named (Rule 1 default) or when the
    LLM passed an explicit confirmation sentinel (``"wizard"``/``"default"``);
    otherwise ``"profile:<name>"`` mirroring the resolver's precedence.

    Used by :func:`_tag_failure` to mark a failed tool response so the
    downstream LLM can see — both in prose and as a machine-checkable
    field — that the failure used the wizard identity and Rule 6 forbids
    silently pivoting to a connections.yaml profile.
    """
    if teradata_profile is None:
        return "wizard"
    stripped = teradata_profile.strip()
    if stripped.lower() in _WIZARD_PROFILE_NAMES:
        return "wizard"
    return f"profile:{stripped}"


def _tag_failure(response: dict[str, Any], teradata_profile: str | None) -> dict[str, Any]:
    """Annotate a failed tool response with the connection source.

    Successful responses pass through unchanged. Failures get
    ``connection_source`` (``"wizard"`` or ``"profile:<name>"``) and, when
    the source is ``"wizard"``, a ``wizard_failure_hint`` repeating Rule 6
    in prose so even an LLM that ignores the structured field still sees
    the rule alongside the error.

    Idempotent — won't overwrite a pre-set ``connection_source`` (e.g. if
    a deeper helper already tagged the response).
    """
    if response.get("success", True):
        return response
    response.setdefault("connection_source", _connection_source(teradata_profile))
    if response["connection_source"] == "wizard":
        response.setdefault(
            "wizard_failure_hint",
            (
                "This failure used the wizard-default Teradata connection "
                "(no teradata_profile was named). Per Rule 6 in the server "
                "instructions: report this error to the user verbatim and "
                "stop — do NOT scan connections.yaml or retry with a "
                "different profile unless the user explicitly names one in "
                "their next prompt."
            ),
        )
    return response


def _apply_target_overrides(
    base: TeradataAuth,
    target_host: str | None,
    target_username: str | None,
    target_password: str | None,
) -> TeradataAuth:
    """Project the tool-level ``target_*`` override kwargs onto ``base``.

    The tdload layer already honours these overrides on the CLIv2 wire
    (``_prepare_tdload_job_var``: ``TargetTdpId`` replaced unconditionally;
    ``TargetUserName``/``TargetUserPassword`` replaced only when the base
    mechanism is TD2/LDAP so JWT/SECRET/BEARER renderer output isn't
    clobbered). The pre-create DDL client must mirror those semantics so
    CREATE TABLE lands on the same instance/user as the subsequent
    tdload run — otherwise the table is created in one place and data
    loaded into another.

    Returns ``base`` unchanged when no override is supplied.
    """
    if not (target_host or target_username or target_password):
        return base

    # ``target_host`` always wins — tdload uses it for ``TargetTdpId``
    # regardless of mechanism (see ttu_client.py:706).
    new_host = target_host or base.host

    # Username/password overrides mirror the tdload-side gate: only
    # applied when the base is TD2/LDAP. For JWT/SECRET/BEARER the
    # renderer's output wins on the wire, so we keep the base identity
    # on the DDL client too — otherwise DDL would authenticate as a TD2
    # user that tdload won't actually use.
    if base.mechanism in ("TD2", "LDAP") and (target_username or target_password):
        return dataclasses.replace(
            base,
            host=new_host,
            username=target_username or base.username,
            password=target_password or base.password,
        )
    return dataclasses.replace(base, host=new_host)


def register_ttu_tools(orchestrator: PipelineOrchestrator) -> dict[str, Callable]:
    """Register TTU tools and return a mapping of tool names to callables.

    Args:
        orchestrator: The pipeline orchestrator instance.

    Returns:
        Dictionary mapping tool names to async callables.
    """

    async def ttu_execute(
        action: Literal[
            "execute_ddl",
            "load_data",
            "execute_bteq",
            "run_query",
            "check_installation",
            "ddl",
            "execute_sql",
            "run_sql",
            "query",
            "select",
            "check",
            "validate",
        ],
        # execute_ddl params — accepts sql (str) or sql_statements (list[str])
        sql_statements: list[str] | None = None,
        sql: str | None = None,
        job_name: str | None = None,
        error_list: list[int] | None = None,
        # load_data params
        mode: str | None = None,
        source_file_name: str | None = None,
        target_table: str | None = None,
        target_file_name: str | None = None,
        source_table: str | None = None,
        select_stmt: str | None = None,
        insert_stmt: str | None = None,
        source_format: str | None = None,
        target_format: str | None = None,
        source_text_delimiter: str | None = None,
        target_text_delimiter: str | None = None,
        target_host: str | None = None,
        target_username: str | None = None,
        target_password: str | None = None,
        skip_header_rows: int | None = None,
        create_table_if_not_exists: bool = False,
        tdload_options: str | None = None,
        tdload_job_var_file: str | None = None,
        # execute_bteq params
        script: str | None = None,
        timeout: int | None = None,
        # check_installation params
        version: str | None = None,
        # connection profile overrides
        teradata_profile: str | None = None,
        target_profile: str | None = None,
        # common
        save_script: bool = False,
        save_tpt_script: bool = False,
        confirm: bool = False,
    ) -> dict[str, Any]:
        """Run Teradata operations directly — no Airflow required.

        Connection: follows the server's wizard-vs-profile selection policy
        (see the server ``instructions``). Default is the wizard connection
        unless the user names a profile via ``teradata_profile``. For
        ``mode="table_to_table"``, Rule 4 applies — both ``teradata_profile``
        (source) and ``target_profile`` (target) must be explicitly supplied;
        pass ``"wizard"`` to confirm the wizard default for either side.

        USE THIS TOOL (not airflow_teradata_load) when the user wants to:
        - Load a local CSV/file into Teradata directly (ad-hoc, one-off, or interactive)
        - Export data from Teradata to a local file
        - Copy a table between Teradata instances
        - Execute DDL (CREATE TABLE, ALTER, DROP) via teradatasql direct connection
        - Run SQL queries, check if a table/object exists, validate data, run stored procedures
        - Run BTEQ scripts or ad-hoc SQL queries (with teradatasql fallback if BTEQ not installed)
        - Check TTU installation status

        Execution methods vary by action:
        - execute_ddl: runs via teradatasql direct connection (no TTU binary required)
        - execute_bteq: runs via BTEQ binary, falls back to teradatasql if BTEQ not installed
        - load_data: runs via tdload binary (TTU required)
        - check_installation: checks TTU binary availability

        This tool does NOT involve Airflow, DAG generation, or SSH. Credentials are
        injected server-side and never exposed to the caller.

        Use airflow_teradata_load instead ONLY when the user explicitly asks to
        create an Airflow DAG for scheduled/recurring loads or deploy a pipeline to Airflow.

        Use dbt_project(action='create_from_csv') instead ONLY when the user
        explicitly wants the CSV scaffolded as a dbt source (mentions "dbt",
        "staging model", "sources.yml", "seed file", or "build a dbt project").
        For plain "load this CSV" requests with no dbt mention, this tool
        (ttu_execute) is the correct choice — create_from_csv would wrap the
        load in dbt scaffolding the user did not ask for.

        ACTION ROUTING GUIDE — read carefully before choosing an action:

        - run_query (aliases: execute_sql, run_sql, query, select, check, validate):
          Run ANY SQL query via teradatasql direct connection (no BTEQ needed).
          Use this for SELECT, SHOW, HELP, CALL, existence checks, data validation,
          row counts, stored procedures, and any read/query operation. This is the
          DEFAULT action for running SQL. Required: sql (str). Optional: timeout (int).

        - execute_bteq: Run SQL via BTEQ binary (TTU required). Use only when BTEQ-specific
          features are needed. Required: script (str). Optional: timeout (int), save_script (bool).

        - execute_ddl (alias: ddl): Execute ONLY DDL statements (CREATE, ALTER, DROP,
          RENAME, GRANT, REVOKE) via teradatasql direct connection. Do NOT use for
          SELECT or queries — use run_query instead. Required: sql (str) or
          sql_statements (list[str]). Optional: error_list (list[int]).

        - load_data: Load/export data via tdload (local file ↔ Teradata, or cross-instance).
          Required: mode (str: file_to_table|table_to_file|table_to_table).
          Mode file_to_table: source_file_name, target_table.
          Mode table_to_file: source_table or select_stmt, target_file_name.
          Mode table_to_table: source_table or select_stmt, target_table.
          Optional: insert_stmt, source_format, target_format, source_text_delimiter,
                    target_text_delimiter, skip_header_rows (int, e.g. 1 to skip CSV header),
                    create_table_if_not_exists (bool, auto-creates target table; for
                        file_to_table infers schema from CSV, for table_to_table copies
                        schema from source table),
                    tdload_options (str, extra CLI flags appended to tdload command, e.g. "-c UTF8"),
                    tdload_job_var_file (str, path to user-provided job var file — skips auto-generation),
                    job_name,
                    save_script (bool, save the job-variable file that drove tdload — sanitized; path returned in ``script_path``),
                    save_tpt_script (bool, capture the TPT script tdload generated internally under ``$TWB_ROOT/jobs/<job_name>/`` after a successful run — sanitized; path returned in ``tpt_script_path``).

        - check_installation: Check which TTU binaries are available on the system.
          Optional: version (str, default from settings — e.g. '17.20').

        CONNECTION PROFILES (connections.yaml):

        teradata_profile: The primary connection profile. How it maps depends on the action:
            - execute_ddl / execute_bteq: the Teradata connection to use.
            - file_to_table: the TARGET Teradata instance (source is a local file).
            - table_to_file: the SOURCE Teradata instance (target is a local file).
            - table_to_table: the SOURCE Teradata instance. If target_profile is not
              set, also used as the target (same-instance copy).

        target_profile: Optional second connection profile, only for table_to_table
            cross-instance copies. Specifies the TARGET Teradata instance. When omitted,
            the target defaults to teradata_profile (or the server default).
            Ignored for all other actions and modes.

            Examples:
            - Same-instance copy: teradata_profile="prod" (source=prod, target=prod).
            - Cross-instance copy: teradata_profile="prod", target_profile="staging"
              (source=prod, target=staging).
            - Explicit target creds override target_profile: target_host, target_username,
              target_password params take precedence when set.

        confirm: DESTRUCTIVE ACTION SAFETY PROTOCOL — This is a two-turn operation for
            destructive statements (DROP, DELETE, TRUNCATE):
            Turn 1: Call with confirm=False (default). If destructive SQL is detected,
            the tool returns a preview listing the statements. Display this to the user.
            STOP. Do NOT call this tool again in the same turn. End your response and
            wait for the user's next message.
            Turn 2: ONLY after the user explicitly replies with approval (e.g., "yes",
            "proceed", "confirm"), call again with confirm=True.
            NEVER set confirm=True in the same turn as the preview call.
            NEVER set confirm=True without the user's explicit approval message.
            Non-destructive DDL (CREATE, ALTER, GRANT, etc.) and read queries
            (SELECT, SHOW) do not require confirmation. DML statements (INSERT, UPDATE,
            MERGE) are rejected by execute_ddl — use execute_bteq instead.
        """
        try:
            # Normalize action aliases
            _action_aliases = {
                # DDL-only alias
                "ddl": "execute_ddl",
                # Query aliases → teradatasql direct (no BTEQ needed)
                "execute_sql": "run_query",
                "run_sql": "run_query",
                "query": "run_query",
                "select": "run_query",
                "check": "run_query",
                "validate": "run_query",
            }
            action = _action_aliases.get(action, action)

            # Rule 4 gate — table_to_table data transfer must have both
            # source and target connections explicitly named by the caller.
            # The server refuses to silently default to the wizard identity
            # for either side; the LLM is expected to ask the user which
            # connection to use for source and target before invoking.
            # Pass the literal ``"wizard"`` (or ``"default"``) sentinel to
            # explicitly confirm the wizard-default connection — see the
            # server ``instructions`` for the full policy. Other modes
            # (file_to_table, table_to_file, execute_*) keep Rule 1/2
            # defaults: missing profile → wizard fallback.
            if action == "load_data" and mode == "table_to_table":
                missing: list[str] = []
                if not teradata_profile:
                    missing.append("teradata_profile (source connection)")
                if not target_profile:
                    missing.append("target_profile (target connection)")
                if missing:
                    return {
                        "success": False,
                        "rule": "Rule 4",
                        "missing": missing,
                        "error": (
                            "Rule 4: table_to_table requires explicit source "
                            "AND target connection choices — the server does "
                            "not assume which connection to use. Missing: "
                            + ", ".join(missing)
                            + ". Ask the user which connection to use for "
                            "each side (the wizard default or a specific "
                            "profile from connections.yaml), then retry with "
                            "both parameters set. Pass the literal 'wizard' "
                            "to explicitly confirm the wizard-default "
                            "connection."
                        ),
                    }

            # Precedence: if a profile is named it wins fully; otherwise the
            # wizard default is used. The resolver builds a TeradataAuth —
            # mechanism-specific fields travel together as one coherent
            # identity, no field-level mixing with Settings.
            if teradata_profile or target_profile:
                guard = orchestrator.credential_resolver.guard_configured()
                if guard:
                    return guard
            try:
                primary_auth: TeradataAuth = resolve_teradata_auth(
                    settings=orchestrator.settings.teradata,
                    credential_resolver=orchestrator.credential_resolver,
                    teradata_profile=teradata_profile,
                )
                target_auth: TeradataAuth | None = (
                    resolve_teradata_auth(
                        settings=orchestrator.settings.teradata,
                        credential_resolver=orchestrator.credential_resolver,
                        teradata_profile=target_profile,
                    )
                    if target_profile
                    else None
                )
            except ValueError as e:
                # Tag with the resolved connection source so the LLM (per
                # Rule 6) doesn't silently pivot to a connections.yaml
                # profile when the wizard auth is misconfigured. The
                # ValueError commonly comes from TeradataAuth.__post_init__
                # rejecting the wizard's incomplete settings.
                return _tag_failure(
                    {"success": False, "error": str(e)},
                    teradata_profile,
                )
            # ``teradata_profile_used`` tracks whether the caller explicitly
            # requested a profile (not the wizard default) — relevant for the
            # per-action constructors below that decide between the
            # orchestrator's default clients and a fresh, auth-bound client.
            # Use truthiness (not ``is not None``) so an empty string behaves
            # the same way ``resolve_teradata_auth`` does: no profile named.
            teradata_profile_used = bool(teradata_profile)

            if action == "check_installation":
                ttu_version = version or orchestrator.settings.ttu.ttu_version
                result = TTUClient.check_installation(version=ttu_version)
                return sanitize_response({"success": True, **result})

            elif action == "execute_ddl":
                # Normalize: accept sql (str) as alias for sql_statements (list)
                if not sql_statements and sql:
                    sql_statements = [s.strip() for s in sql.split(";") if s.strip()]
                if not sql_statements or not isinstance(sql_statements, list):
                    return {
                        "success": False,
                        "error": (
                            "sql_statements (list[str]) or sql (str) is required. "
                            "Provide one or more SQL statements to execute."
                        ),
                    }
                # Guard: detect SELECT/query statements that belong in execute_bteq
                _query_prefixes = ("select", "sel ", "show", "help", "call", "exec")
                _dml_prefixes = ("delete", "insert", "update", "merge")
                _comment_re = re.compile(r"^(\s*/\*.*?\*/\s*|--[^\n]*\n)*", re.DOTALL)
                for stmt in sql_statements:
                    lower = _comment_re.sub("", stmt).strip().lower()
                    if lower.startswith(_query_prefixes):
                        return {
                            "success": False,
                            "error": (
                                f"execute_ddl does not support query statements "
                                f"(found: '{stmt[:60]}...'). "
                                f"Use action='run_query' with sql parameter instead."
                            ),
                        }
                    if lower.startswith(_dml_prefixes):
                        return {
                            "success": False,
                            "error": (
                                f"execute_ddl does not support DML statements "
                                f"(found: '{stmt[:60]}...'). "
                                f"Use action='run_query' with sql parameter instead."
                            ),
                        }
                # Guard: destructive DDL requires explicit confirmation
                _destructive_prefixes = ("drop", "truncate")
                destructive_stmts = [
                    s for s in sql_statements
                    if _comment_re.sub("", s).strip().lower().startswith(_destructive_prefixes)
                ]
                if destructive_stmts and not confirm:
                    return {
                        "success": False,
                        "requires_confirmation": True,
                        "action": "execute_ddl",
                        "warning": f"Found {len(destructive_stmts)} destructive DDL statement(s).",
                        "destructive_statements": [s[:120] for s in destructive_stmts],
                        "hint": "Re-call with confirm=True to execute these statements.",
                    }
                ignored_params = []
                if job_name:
                    ignored_params.append("job_name")
                if save_script:
                    ignored_params.append("save_script")
                if save_tpt_script:
                    ignored_params.append("save_tpt_script")
                if teradata_profile_used:
                    from ..clients.teradata_client import TeradataClient
                    td_client = TeradataClient(auth=primary_auth)
                else:
                    td_client = orchestrator.teradata_client
                result = await asyncio.to_thread(
                    td_client.execute_statements,
                    sql_statements=sql_statements,
                    error_list=error_list,
                )
                if ignored_params:
                    result["ignored_params"] = ignored_params
                    result["ignored_params_note"] = (
                        f"Parameters {ignored_params} are not supported by execute_ddl "
                        f"(DDL runs via teradatasql, not TPT)."
                    )
                if not result.get("success"):
                    lock_info = _detect_mload_lock(result)
                    if lock_info:
                        return _tag_failure(lock_info, teradata_profile)
                return _tag_failure(sanitize_response(result), teradata_profile)

            elif action == "load_data":
                if not mode:
                    return {
                        "success": False,
                        "error": "mode is required (file_to_table, table_to_file, table_to_table)",
                    }
                _valid_modes = ("file_to_table", "table_to_file", "table_to_table")
                if mode not in _valid_modes:
                    return {
                        "success": False,
                        "error": f"Invalid mode '{mode}'. Must be one of: {', '.join(_valid_modes)}",
                    }
                if mode == "file_to_table":
                    missing = []
                    if not source_file_name:
                        missing.append("source_file_name")
                    if not target_table:
                        missing.append("target_table")
                    if missing:
                        return {
                            "success": False,
                            "error": f"mode 'file_to_table' requires: {', '.join(missing)}",
                        }
                elif mode == "table_to_file":
                    if not source_table and not select_stmt:
                        return {
                            "success": False,
                            "error": "mode 'table_to_file' requires source_table or select_stmt",
                        }
                    if not target_file_name:
                        return {
                            "success": False,
                            "error": "mode 'table_to_file' requires target_file_name",
                        }
                elif mode == "table_to_table":
                    if not source_table and not select_stmt:
                        return {
                            "success": False,
                            "error": "mode 'table_to_table' requires source_table or select_stmt",
                        }
                    if not target_table:
                        return {
                            "success": False,
                            "error": "mode 'table_to_table' requires target_table",
                        }
                # Auth-per-mode semantics:
                #   file_to_table:  primary_auth → target (destination Teradata)
                #   table_to_file:  primary_auth → source (query-side Teradata)
                #   table_to_table: primary_auth → source;
                #                   target_auth  → target (falls back to primary_auth)
                #
                # ``execute_tdload(auth=...)`` treats its auth parameter as the
                # "primary identity" — see ``_prepare_tdload_job_var`` for how
                # each mode renders it. For cross-instance table_to_table, we
                # swap the primary to ``target_auth`` and pass the source
                # identity through the legacy TD2-shim kwargs. Mixed-mechanism
                # cross-instance is a future enhancement.
                source_host: str | None = None
                source_username: str | None = None
                source_password: str | None = None
                effective_auth: TeradataAuth = primary_auth
                # Layer the tool-level target_* overrides onto target_auth
                # BEFORE comparing identities so the gate matches what tdload
                # will actually use on the wire.
                effective_target_auth = (
                    _apply_target_overrides(
                        target_auth,
                        target_host=target_host,
                        target_username=target_username,
                        target_password=target_password,
                    )
                    if target_auth is not None
                    else None
                )
                # Dual-identity: target_profile resolves to a different
                # identity than teradata_profile.  Even on the same host
                # (different users), tdload needs separate Source*/Target*
                # credentials.  Without this swap the Target* entries
                # (built from ``auth``) carry the SOURCE identity, so
                # tdload's target-side pre-check authenticates as the
                # wrong user.  The Source* shim kwargs can only carry
                # TD2/LDAP credentials, so non-TD2/LDAP source identities
                # are rejected at the boundary.
                needs_identity_swap = (
                    mode == "table_to_table"
                    and effective_target_auth is not None
                    and not primary_auth.same_identity_as(effective_target_auth)
                )
                if needs_identity_swap:
                    assert effective_target_auth is not None  # narrow for type-checker
                    if primary_auth.mechanism not in ("TD2", "LDAP"):
                        return {
                            "success": False,
                            "error": (
                                "table_to_table with distinct source and "
                                "target profiles requires a TD2/LDAP source "
                                f"mechanism. Source mechanism "
                                f"'{primary_auth.mechanism}' is not "
                                "supported — tdload's Source* job-var shim "
                                "only carries TD2/LDAP credentials. Either "
                                "use TD2/LDAP for the source profile, or "
                                "omit target_profile to use the same "
                                "identity for source and target."
                            ),
                        }
                    if not (primary_auth.username and primary_auth.password):
                        return {
                            "success": False,
                            "error": (
                                "Source profile for table_to_table is "
                                "missing username or password. Both are "
                                "required for the TD2/LDAP Source* shim."
                            ),
                        }
                    effective_auth = effective_target_auth
                    source_host = primary_auth.host
                    source_username = primary_auth.username
                    source_password = primary_auth.password

                # Auto-detect CSV header for file_to_table when not explicitly set
                if mode == "file_to_table" and source_file_name and skip_header_rows is None:
                    try:
                        from ..utils.csv_analyzer import CSVAnalyzer
                        analyzer = CSVAnalyzer()
                        has_header = analyzer.detect_header(
                            Path(source_file_name),
                            delimiter=source_text_delimiter or ",",
                            encoding="utf-8",
                        )
                        if has_header:
                            skip_header_rows = 1
                            logger.info("CSV header detected, auto-setting skip_header_rows=1")
                    except Exception as hdr_err:
                        logger.debug("CSV header detection failed: %s", hdr_err)

                # Pre-create target table if requested
                precreate_warning: str | None = None

                if create_table_if_not_exists and mode == "file_to_table" and source_file_name and target_table:
                    try:
                        from ..clients.teradata_client import TeradataClient
                        from ..utils.csv_analyzer import CSVAnalyzer

                        analyzer = CSVAnalyzer()
                        analysis = analyzer.analyze_csv(
                            source_file_name,
                            delimiter=source_text_delimiter or ",",
                        )
                        _ident_re = re.compile(
                            r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$"
                        )
                        if not _ident_re.match(target_table.strip()):
                            raise ValueError(f"Invalid target_table identifier: {target_table!r}")
                        quoted_cols = [
                            f'"{col.name.replace(chr(34), chr(34)+chr(34))}"'
                            for col in analysis.columns
                        ]
                        col_defs = ", ".join(
                            f'{qc} {col.inferred_teradata_type}'
                            for qc, col in zip(quoted_cols, analysis.columns)
                        )
                        create_sql = f"CREATE TABLE {target_table.strip()} ({col_defs})"
                        # For file_to_table, primary_auth is the target
                        # identity. Layer the tool-level target_* overrides
                        # on top so DDL lands on the same instance/user as
                        # the subsequent tdload run (which honours the same
                        # kwargs on its argv / job vars).
                        ddl_auth = _apply_target_overrides(
                            primary_auth,
                            target_host=target_host,
                            target_username=target_username,
                            target_password=target_password,
                        )
                        if teradata_profile_used or ddl_auth is not primary_auth:
                            td_client = TeradataClient(auth=ddl_auth)
                        else:
                            td_client = orchestrator.teradata_client
                        ddl_result = await asyncio.to_thread(
                            td_client.execute_statements,
                            sql_statements=[create_sql],
                            error_list=[3803],
                        )
                        if ddl_result.get("success"):
                            logger.info("Pre-created target table: %s", target_table)
                        else:
                            precreate_warning = f"Pre-create table failed: {ddl_result}"
                            logger.warning(precreate_warning)
                    except Exception as ct_err:
                        precreate_warning = f"Could not pre-create table {target_table}: {ct_err}"
                        logger.warning(precreate_warning)

                if create_table_if_not_exists and mode == "table_to_table" and target_table:
                    try:
                        from ..clients.teradata_client import TeradataClient

                        # For table_to_table: primary_auth = source,
                        # target_auth (or primary_auth) = target. Layer the
                        # tool-level target_* overrides on top so the DDL
                        # client and the cross-instance host compare both
                        # see the same effective target identity that
                        # tdload will actually use.
                        effective_target_auth = _apply_target_overrides(
                            target_auth or primary_auth,
                            target_host=target_host,
                            target_username=target_username,
                            target_password=target_password,
                        )
                        source_ref = source_table or select_stmt
                        if not source_ref:
                            raise ValueError("table_to_table requires source_table or select_stmt")

                        _ident_re = re.compile(
                            r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$"
                        )
                        if not _ident_re.match(target_table.strip()):
                            raise ValueError(f"Invalid target_table identifier: {target_table!r}")
                        if source_table and not _ident_re.match(source_table.strip()):
                            raise ValueError(f"Invalid source_table identifier: {source_table!r}")

                        is_cross_instance = (
                            primary_auth.host
                            and effective_target_auth.host
                            and primary_auth.host != effective_target_auth.host
                        )

                        if is_cross_instance:
                            if not source_table:
                                raise ValueError(
                                    "create_table_if_not_exists with cross-instance "
                                    "table_to_table requires source_table (not select_stmt) "
                                    "so the source schema can be retrieved via SHOW TABLE"
                                )
                            if teradata_profile_used:
                                src_client = TeradataClient(auth=primary_auth)
                            else:
                                src_client = orchestrator.teradata_client
                            show_result = await asyncio.to_thread(
                                src_client.execute_query,
                                sql=f"SHOW TABLE {source_table}",
                            )
                            if show_result:
                                raw_ddl = show_result[0].get(
                                    list(show_result[0].keys())[0], ""
                                )
                                def _rewrite_table_name(m: re.Match) -> str:
                                    qualifier = m.group(1) or ""
                                    if qualifier:
                                        return f"CREATE {qualifier} TABLE {target_table}"
                                    return f"CREATE TABLE {target_table}"

                                create_sql = re.sub(
                                    r"CREATE\s+(SET|MULTISET)?\s*TABLE\s+\S+",
                                    _rewrite_table_name,
                                    raw_ddl,
                                    count=1,
                                    flags=re.IGNORECASE,
                                )
                            else:
                                raise ValueError(f"SHOW TABLE {source_table} returned no results")
                        else:
                            if source_table:
                                create_sql = (
                                    f"CREATE TABLE {target_table}"
                                    f" AS {source_table} WITH NO DATA"
                                )
                            else:
                                create_sql = (
                                    f"CREATE TABLE {target_table}"
                                    f" AS ({select_stmt}) WITH NO DATA"
                                )

                        if target_auth is not None or teradata_profile_used:
                            tgt_client = TeradataClient(auth=effective_target_auth)
                        else:
                            tgt_client = orchestrator.teradata_client
                        ddl_result = await asyncio.to_thread(
                            tgt_client.execute_statements,
                            sql_statements=[create_sql],
                            error_list=[3803],
                        )
                        if ddl_result.get("success"):
                            logger.info("Pre-created target table: %s", target_table)
                        else:
                            precreate_warning = f"Pre-create table failed: {ddl_result}"
                            logger.warning(precreate_warning)
                    except Exception as ct_err:
                        precreate_warning = f"Could not pre-create table {target_table}: {ct_err}"
                        logger.warning(precreate_warning)

                # Build kwargs for execute_tdload
                # Auto-generate INSERT with quoted columns for file_to_table
                # to handle Teradata reserved keyword column names (e.g., date, comment, type)
                if not insert_stmt and mode == "file_to_table" and source_file_name:
                    try:
                        import csv
                        with open(source_file_name, newline="", encoding="utf-8") as f:
                            reader = csv.reader(f, delimiter=source_text_delimiter or ",")
                            headers = next(reader)
                        if headers:
                            cleaned = [h.strip().replace(chr(34), chr(34)+chr(34)) for h in headers]
                            quoted_cols = [f'"{c}"' for c in cleaned]
                            col_list = ", ".join(quoted_cols)
                            val_list = ", ".join([f":{c.replace(' ', '_')}" for c in cleaned])
                            insert_stmt = f"INSERT INTO {target_table.strip()} ({col_list}) VALUES ({val_list})"
                            logger.info("Auto-generated INSERT with quoted columns from CSV header")
                    except Exception as csv_err:
                        logger.debug("Could not auto-generate INSERT from CSV: %s", csv_err)

                tdload_kwargs: dict[str, Any] = {}
                for key, val in [
                    ("source_file_name", source_file_name),
                    ("target_table", target_table),
                    ("target_file_name", target_file_name),
                    ("source_table", source_table),
                    ("select_stmt", select_stmt),
                    ("insert_stmt", insert_stmt),
                    ("source_format", source_format),
                    ("target_format", target_format),
                    ("source_text_delimiter", source_text_delimiter),
                    ("target_text_delimiter", target_text_delimiter),
                    ("target_host", target_host),
                    ("target_username", target_username),
                    ("target_password", target_password),
                    ("source_host", source_host),
                    ("source_username", source_username),
                    ("source_password", source_password),
                    ("skip_header_rows", skip_header_rows),
                    ("job_name", job_name),
                ]:
                    if val is not None and not (isinstance(val, bool) and val is False):
                        tdload_kwargs[key] = val

                # source_mechanism: when the identity swap is active,
                # ``effective_auth`` is the target identity. Tell the
                # job-var builder which mechanism the source_* shim kwargs
                # represent so it gates correctly.
                src_mechanism: str | None = None
                if needs_identity_swap:
                    src_mechanism = primary_auth.mechanism
                result = await asyncio.to_thread(
                    orchestrator.ttu_client.execute_tdload,
                    auth=effective_auth,
                    mode=mode,
                    save_script=save_script,
                    save_tpt_script=save_tpt_script,
                    tdload_options=tdload_options,
                    tdload_job_var_file=tdload_job_var_file,
                    source_mechanism=src_mechanism,
                    **tdload_kwargs,
                )
                if precreate_warning:
                    result["precreate_warning"] = precreate_warning
                if not result.get("success"):
                    lock_info = _detect_mload_lock(result)
                    if lock_info:
                        return _tag_failure(lock_info, teradata_profile)
                return _tag_failure(sanitize_response(result), teradata_profile)

            elif action == "run_query":
                query_sql = sql or script
                if not query_sql and sql_statements:
                    query_sql = ";\n".join(sql_statements)
                if not query_sql:
                    return {"success": False, "error": "sql (str) or script (str) is required"}
                try:
                    if teradata_profile_used:
                        from ..clients.teradata_client import TeradataClient
                        td_client = TeradataClient(auth=primary_auth)
                    else:
                        td_client = orchestrator.teradata_client
                    stmts = [s.strip() for s in query_sql.split(";") if s.strip()]
                    result = await asyncio.to_thread(
                        td_client.execute_statements,
                        sql_statements=stmts,
                        timeout=timeout,
                    )
                except Exception as e:
                    result = {"success": False, "error": str(e)}
                return _tag_failure(sanitize_response(result), teradata_profile)

            elif action == "execute_bteq":
                # Normalize: accept sql/sql_statements as alias for script
                # (common when LLM sends action="execute_sql" with sql param)
                if not script and sql:
                    script = sql
                elif not script and sql_statements:
                    script = ";\n".join(sql_statements) + ";"
                if not script or not isinstance(script, str):
                    return {
                        "success": False,
                        "error": "script (str) or sql (str) is required",
                    }
                _destructive_prefixes = ("drop", "delete", "truncate")
                _comment_re = re.compile(r"^(\s*/\*.*?\*/\s*|--[^\n]*\n)*", re.DOTALL)
                stmts = [s.strip() for s in script.split(";") if s.strip()]
                destructive_stmts = [
                    s for s in stmts
                    if _comment_re.sub("", s).strip().lower().startswith(_destructive_prefixes)
                ]
                if destructive_stmts and not confirm:
                    return {
                        "success": False,
                        "requires_confirmation": True,
                        "action": "execute_bteq",
                        "warning": f"Found {len(destructive_stmts)} destructive statement(s).",
                        "destructive_statements": [s[:120] for s in destructive_stmts],
                        "hint": "Re-call with confirm=True to execute these statements.",
                    }
                try:
                    # TTUClient is stateless w.r.t. identity — just pass auth
                    # to the per-call method. No need to rebuild the client
                    # when a profile is named.
                    result = await asyncio.to_thread(
                        orchestrator.ttu_client.execute_bteq,
                        auth=primary_auth,
                        script=script,
                        timeout=timeout,
                        save_script=save_script,
                    )
                except TTUNotInstalledError:
                    logger.info("BTEQ not installed, using teradatasql fallback")
                    if save_script:
                        logger.warning("save_script is not supported in teradatasql fallback mode")
                    if teradata_profile_used:
                        from ..clients.teradata_client import TeradataClient
                        td_client = TeradataClient(auth=primary_auth)
                    else:
                        td_client = orchestrator.teradata_client
                    result = await asyncio.to_thread(
                        td_client.execute_statements,
                        sql_statements=stmts,
                        timeout=timeout,
                    )
                    result["fallback"] = True
                    result["fallback_note"] = "Executed via teradatasql (BTEQ not installed)"
                    if save_script:
                        result["save_script_ignored"] = True
                if not result.get("success"):
                    lock_info = _detect_mload_lock(result)
                    if lock_info:
                        return _tag_failure(lock_info, teradata_profile)
                return _tag_failure(sanitize_response(result), teradata_profile)

            else:
                return {"success": False, "error": f"Unknown action '{action}'"}

        except Exception as e:
            logger.error("TTU tool error (action=%s): %s", action, e, exc_info=True)
            return _tag_failure(
                {"success": False, "error": safe_error_message(e)},
                teradata_profile,
            )

    return {"ttu_execute": ttu_execute}
