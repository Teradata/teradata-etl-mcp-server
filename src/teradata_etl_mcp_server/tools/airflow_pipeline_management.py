"""Airflow pipeline management and deployment tools (router-tool pattern).

This module provides MCP tools for managing Airflow pipelines, connections,
and deployments — consolidated into five router tools.
"""

import asyncio
import contextlib
import logging
import os
import shlex
from datetime import timezone
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import yaml

from ..clients.airbyte_client import (
    AirbyteAPIError,
    AirbyteConnectionError,
    CircuitBreakerOpen,
)
from ..orchestrator import PipelineOrchestrator
from ..response_sanitizer import safe_error_message, sanitize_response
from ..utils.file_operations import (
    UnsafePathError,
    safe_join_within,
)
from ..utils.validators import slugify_dir_name, validate_identifier
from .dbt_management import _ProjectResolution, _collision_response, _read_project_profile
from .utils import UNRESOLVED_ENV_VAR

logger = logging.getLogger(__name__)


def _configure_ssh_host_key_policy(
    ssh: Any,
    strict_host_key_checking: bool,
    *,
    context: str,
) -> None:
    """Apply host-key policy to a paramiko ``SSHClient`` with a uniform MITM warning.

    With ``strict=True``: load system known_hosts and use ``RejectPolicy`` —
    unknown hosts fail the handshake. With ``strict=False``: log a loud WARNING
    naming the call site, then use ``AutoAddPolicy``. The warning is
    deliberately uniform across call sites so a grep on ``'SSH host-key'`` in
    logs surfaces every exposed path.

    ``context`` should be a short identifier of the calling operation
    (e.g. ``'deploy_dags'``, ``'fetch_dag'``, ``'delete_pipeline'``) — it
    appears in the warning so operators can tell which path is unsafe.
    """
    import paramiko

    if strict_host_key_checking:
        try:
            ssh.load_system_host_keys()
        except Exception as host_key_err:
            logger.warning(
                "Could not load system host keys for %s (corrupted known_hosts?): %s",
                context,
                host_key_err,
            )
        ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
    else:
        logger.warning(
            "SSH host-key verification is DISABLED for %s "
            "(strict_host_key_checking=False). This exposes you to MITM "
            "attacks. Add the remote host to ~/.ssh/known_hosts and pass "
            "strict_host_key_checking=True for production deployments.",
            context,
        )
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # noqa: S507  # nosec B507


def _validate_output_filename(
    output_filename: str | None, dag_id: str, dags_dir: Path
) -> str:
    """Validate caller-supplied DAG filename stays inside ``dags_dir``.

    Applies the default ``{dag_id}.py`` when ``output_filename`` is None.
    Delegates the containment check to :func:`safe_join_within`, giving clean
    MCP-boundary rejection before any expensive work begins.

    Returns:
        The validated filename (with default applied).

    Raises:
        ValueError: If the resolved path escapes ``dags_dir``.
    """
    candidate = output_filename or f"{dag_id}.py"
    try:
        safe_join_within(dags_dir.resolve(), candidate)
    except UnsafePathError as e:
        raise ValueError(f"invalid output_filename: {e}") from e
    return candidate


def _refresh_env_call_hint(
    orchestrator: Any,
    project_name: str | None,
    identity: str | None,
) -> dict[str, str]:
    """Build the right ``dbt_project(action='refresh_env', ...)`` hint
    for the identity a sub-project is bound to.

    The sub-project's ``dbt_project.yml::profile`` field stores either
    a connections.yaml profile name (e.g. ``'prod'``) or the synthetic
    wizard sentinel ``'wizard:<slug(settings.teradata.host)>'``.
    ``resolve_teradata_auth`` only folds the LITERAL strings
    ``'wizard'`` / ``'default'`` / empty/whitespace to the wizard-
    default branch — colon-suffix forms are NOT reserved, so users
    are free to define named profiles like ``wizard:prod`` in
    connections.yaml. To distinguish the synthetic sentinel from a
    user-defined name with the same prefix, this helper compares the
    binding against the EXACT current sentinel (recomputed from
    ``settings.teradata.host``) — prefix matching would misclassify
    a legitimate named profile and silently route refresh_env to
    wizard-default creds.

    So the LLM-facing instruction has to differ:
      - Identity matches the live synthetic sentinel exactly → call
        refresh_env WITHOUT ``teradata_profile``; refresh_env folds an
        absent value to the wizard-default identity from Settings.
      - Anything else (named profile, OR a stale sentinel from a
        since-changed wizard host) → pass the binding string as
        ``teradata_profile``. A stale sentinel will fail at
        ``resolve_profile`` lookup, prompting the user to re-scaffold.

    Returns ``{"call": "<formatted call>", "why_extra": "<inline note>"}``
    so the caller can inject both into a ``next_steps`` Markdown string.

    Both interpolated values are sanitized for safe display inside a
    backtick-wrapped Python call snippet:
      - ``project_name`` is run through ``slugify_dir_name`` (mirrors
        the resolver's normalization). The slug is alphanumeric+``_``,
        so it's syntactically safe inside single quotes AND it is the
        canonical name the user would pass to ``refresh_env`` anyway
        (since ``create_structure`` slugifies on the way in).
      - ``identity`` comes from ``dbt_project.yml::profile``; YAML
        permits arbitrary string content (newlines, tabs, embedded
        quotes, backticks) via quoting and block scalars. The value
        is rendered as a Python string literal via ``repr()`` so the
        snippet stays syntactically valid for any content. Backticks
        are stripped first because ``repr()`` wouldn't escape them
        and they would terminate the surrounding inline-code span in
        the rendered ``next_steps`` Markdown.
    """
    if project_name:
        slug = slugify_dir_name(project_name)
        if slug.startswith("dbt_"):
            slug = slug[4:]
        pn = slug or "<project_name>"
    else:
        pn = "<project_name>"
    # Compute the live synthetic wizard sentinel for exact-match
    # detection. Mirrors ``_resolve_teradata_identity`` in
    # ``tools/dbt_management.py``: ``wizard:<slug(settings.teradata.host)>``
    # when the wizard host is configured, else None.
    host = (orchestrator.settings.teradata.host or "").strip()
    host_slug = slugify_dir_name(host) if host else ""
    current_wizard_sentinel = f"wizard:{host_slug}" if host_slug else None
    if identity and identity == current_wizard_sentinel:
        return {
            "call": f"dbt_project(action='refresh_env', project_name='{pn}')",
            "why_extra": (
                "(Omit ``teradata_profile`` — this sub-project is bound to "
                "the wizard-default identity, which refresh_env infers from "
                "Settings. The synthetic ``wizard:<host_slug>`` value in "
                "``teradata_identity`` is NOT a real profile name; passing "
                "it would trigger a connections.yaml lookup and fail.)"
            ),
        }
    # repr() emits a valid Python literal for any string (handling
    # backslash, quotes, newlines, tabs, control chars). It picks
    # single OR double quotes based on content — a value with ``'``
    # comes back as ``"o'reilly"``, which is syntactically fine inside
    # the surrounding ``action='refresh_env', ...`` snippet (Python
    # allows mixed quote styles within a single call). Strip backticks
    # first since repr() wouldn't escape them and they'd break the
    # outer Markdown inline-code span.
    profile_arg = repr((identity or "<your_profile>").replace("`", ""))
    return {
        "call": (
            f"dbt_project(action='refresh_env', project_name='{pn}', "
            f"teradata_profile={profile_arg})"
        ),
        "why_extra": (
            "The ``teradata_profile`` for refresh is the named profile "
            "this sub-project is bound to (returned in this response as "
            "``teradata_identity``)."
        ),
    }


def _locate_dbt_subproject_dir(
    orchestrator: Any,
    project_name: str | None,
) -> tuple[Path, str] | dict[str, Any]:
    """Locate an existing dbt sub-project directory by name, for DAG generation.

    Post-``.env``-migration, the dbt task in a generated Airflow DAG runs
    ``dotenv run -- dbt ...`` against the per-sub-project ``.env``. The
    Teradata-identity binding (``dbt_project.yml::profile``) is no longer
    load-bearing for DAG runtime — it's surfaced in the response so the
    caller can drive ``dbt_project(action='refresh_env')`` correctly
    after credential rotation. **Note**: the binding has two shapes and
    they require different ``refresh_env`` calls:
      - Named profile (e.g. ``"prod"``) → pass it as ``teradata_profile``.
      - Wizard sentinel (``"wizard:<host_slug>"``) → OMIT
        ``teradata_profile`` so refresh_env folds to the wizard default.
        Only the literal sentinels ``"wizard"``/``"default"``/empty fold;
        the colon-suffix form is treated as an explicit named profile by
        ``resolve_teradata_auth`` and would fail. See
        :func:`_refresh_env_call_hint` for the formatting helper that
        encodes this rule.

    Returns ``(project_dir, identity)`` on success — ``identity`` is read
    from the sub-project's ``dbt_project.yml::profile`` field and is a
    non-empty string by construction (either a named profile like
    ``'prod'`` or the wizard-default sentinel ``'wizard:<host_slug>'``).
    The ``fix_subproject_binding`` branch below catches the
    missing/unreadable case so callers can rely on the invariant.

    Returns an ``action_required`` response dict the caller should pass
    through:
      - ``ask_project_name`` when ``project_name`` is missing or
        slugifies to nothing.
      - ``rename_project`` when ``project_name`` would collide with the
        parent container's basename (e.g. ``project_name='project'`` →
        ``dbt_project/dbt_project/``). Mirrors the same rejection that
        ``dbt_project(action='create_structure')`` would apply if the
        caller followed a ``scaffold_subproject_first`` hint, so the
        LLM doesn't take a wasted round-trip into a dead end.
      - ``error`` (with ``"legacy single-project dbt layout"`` text)
        when ``<parent>/dbt_project.yml`` exists. Same rationale —
        scaffolding would refuse to migrate a legacy layout, so DAG
        generation surfaces the migration error directly.
      - ``scaffold_subproject_first`` when the sub-project directory
        is absent (and the layout is healthy). Indicates a true
        nothing-exists-yet state; recovery is to call
        ``dbt_project(action='create_structure', ...)``.
      - ``repair_subproject`` when the sub-project DIR exists but its
        ``dbt_project.yml`` is missing (partial scaffold, manual
        delete, or interrupted ``create_structure``). Distinct from
        ``scaffold_subproject_first`` because the dir is non-empty —
        the LLM/user picks between idempotent re-scaffold and
        delete-and-rescaffold.
      - ``fix_subproject_binding`` when the sub-project's
        ``dbt_project.yml`` exists but has no readable ``profile:``
        field (missing, empty, or unparseable). Without this guard the
        response would carry an empty ``teradata_identity`` and a
        placeholder ``refresh_env`` hint, and the dbt task at runtime
        would fail anyway — the ``.env`` was scaffolded alongside the
        broken binding. Tells the LLM to re-scaffold or fix the
        ``profile:`` field by hand before retrying.

    Other resolver-status branches (``ambiguous``, ``conflict``,
    ``needs_name``, ``no_identity``) are unreachable from this lookup-
    only path — they only matter when an identity is being inferred at
    scaffold time.
    """
    if not project_name:
        return {
            "success": False,
            "action_required": "ask_project_name",
            "message": (
                "Which dbt sub-project should this DAG run? Pass "
                "``project_name=<name>`` matching an existing sub-project "
                "under ``<workspace>/dbt_project/dbt_<name>/``."
            ),
        }
    parent = orchestrator.dbt_project_parent
    # Refuse if a legacy single-project ``dbt_project.yml`` sits at the
    # parent root. Scaffolding refuses this layout (returns the same
    # error from ``_resolve_dbt_subproject``), so surfacing it here
    # avoids sending the LLM through ``scaffold_subproject_first`` →
    # scaffold-fails-with-legacy-error round trip.
    if (parent / "dbt_project.yml").exists():
        return {
            "success": False,
            "error": (
                f"Detected legacy single-project dbt layout at "
                f"{parent}/dbt_project.yml. The new layout puts each "
                f"Teradata profile in its own sub-project under "
                f"{parent}/dbt_<name>/. Move or delete the legacy "
                f"files, then call this action again."
            ),
        }
    slug = slugify_dir_name(project_name)
    # Mirror the dedup logic from ``_resolve_dbt_subproject`` (in
    # ``tools/dbt_management.py``) so ``project_name="dbt_test"`` and
    # ``project_name="test"`` both resolve to the same ``dbt_test/`` dir.
    # Without this, a user who scaffolded with ``"test"`` and then asked
    # to create a DAG with ``"dbt_test"`` (the on-disk form) would get a
    # spurious ``scaffold_subproject_first``.
    if slug.startswith("dbt_"):
        slug = slug[4:]
    if not slug:
        return {
            "success": False,
            "action_required": "ask_project_name",
            "message": (
                f"project_name '{project_name}' slugifies to an empty "
                "name. Pass a non-empty alphanumeric identifier (e.g. "
                "``project_name='analytics'``)."
            ),
        }
    # Reject names that would produce a sub-project directory whose
    # basename equals the parent container's name (e.g. parent
    # ``dbt_project/``, slug ``project`` → ``dbt_project/dbt_project/``).
    # ``dbt_project(action='create_structure')`` rejects these with
    # ``rename_project``; surface the same response here so following
    # the scaffold hint won't dead-end. Reuse ``_collision_response`` so
    # the suggestions and wording stay in lock-step with the scaffold path.
    if f"dbt_{slug}" == parent.name:
        return _collision_response(
            orchestrator,
            _ProjectResolution(status="name_collision", collision_with=parent.name),
            project_name,
        )
    project_dir = parent / f"dbt_{slug}"
    dbt_project_yml = project_dir / "dbt_project.yml"
    if not project_dir.exists():
        return {
            "success": False,
            "action_required": "scaffold_subproject_first",
            "message": (
                f"No dbt sub-project named '{project_name}' (resolved to "
                f"directory ``dbt_{slug}/``) exists yet. Run "
                "dbt_project(action='create_structure', project_name=..., "
                "teradata_profile=...) first to scaffold it, then call "
                "this DAG-generation action again."
            ),
        }
    if not dbt_project_yml.exists():
        # Sub-project dir exists on disk but the file that defines it is
        # missing — partial scaffold, manual delete, or interrupted
        # ``create_structure``. Recovery is different from the
        # nothing-exists-yet case: ``create_structure`` is idempotent
        # and will recreate the file in place, but the user might
        # prefer to delete the half-baked dir and start fresh. Surface
        # the dir-exists fact so the LLM/user can pick.
        return {
            "success": False,
            "action_required": "repair_subproject",
            "message": (
                f"Sub-project directory ``dbt_{slug}/`` exists but its "
                "``dbt_project.yml`` is missing (partial scaffold, "
                "manual deletion, or interrupted ``create_structure``). "
                "Re-run ``dbt_project(action='create_structure', "
                "project_name=..., teradata_profile=...)`` to recreate "
                "the missing file in place, OR delete the directory "
                "first and re-scaffold from scratch — then retry this "
                "DAG-generation action."
            ),
            "project_name": project_name,
        }
    identity = _read_project_profile(dbt_project_yml)
    if not identity:
        # ``_read_project_profile`` returns None on read/parse failure
        # AND when ``profile:`` is missing/empty/non-string. Either way,
        # we can't honestly populate the response's ``teradata_identity``
        # field or build a refresh_env hint, and the dbt task at runtime
        # would fail to find creds via the per-sub-project ``.env``
        # anyway (the .env was scaffolded alongside the missing/broken
        # binding). Fail closed so the LLM repairs the sub-project
        # before the DAG is baked.
        return {
            "success": False,
            "action_required": "fix_subproject_binding",
            "message": (
                f"Sub-project ``dbt_{slug}/`` exists but its "
                "``dbt_project.yml`` has no readable ``profile:`` field "
                "(missing, empty, or unparseable). The Teradata-identity "
                "binding can't be determined, so DAG generation would "
                "produce a response with empty ``teradata_identity`` and "
                "a placeholder refresh_env hint. Re-scaffold with "
                "``dbt_project(action='create_structure', "
                # Interpolate the SLUG (alphanumeric+``_``), not the raw
                # caller-supplied ``project_name``. Same reasoning as
                # ``_refresh_env_call_hint``: raw input could contain
                # quotes, backticks, or control chars that break the
                # rendered call snippet's Python syntax or terminate
                # the surrounding Markdown inline-code span. The slug
                # is also what ``create_structure`` would normalize to
                # internally — so this IS the canonical form the user
                # should pass.
                f"project_name='{slug}', teradata_profile=...)`` "
                "to overwrite the broken file, OR fix the ``profile:`` "
                "field by hand to match the intended Teradata identity, "
                "then retry."
            ),
            "project_name": project_name,
        }
    return project_dir, identity


def _validate_dbt_target(
    project_dir: str,
    target: str,
    profiles_dir: str | None = None,
) -> dict[str, Any] | None:
    """Validate that a dbt target exists in profiles.yml.

    Returns None if valid or if profiles.yml is inaccessible.
    Returns an error dict if the target is invalid.
    """
    try:
        resolved_profiles_dir = profiles_dir or project_dir
        profiles_path = Path(resolved_profiles_dir) / "profiles.yml"
        if not profiles_path.exists():
            return None

        project_path = Path(project_dir) / "dbt_project.yml"
        if not project_path.exists():
            return None

        with open(profiles_path) as f:
            profiles_config = yaml.safe_load(f)
        with open(project_path) as f:
            project_config = yaml.safe_load(f)

        if not isinstance(profiles_config, dict) or not isinstance(project_config, dict):
            return None

        profile_name = project_config.get("profile")
        if not profile_name:
            return None

        profile_data = profiles_config.get(profile_name)
        if not isinstance(profile_data, dict):
            return None

        outputs = profile_data.get("outputs", {})
        if not outputs or target in outputs:
            return None

        return {
            "success": False,
            "error": (
                f"The profile '{profile_name}' does not have a target named '{target}'. "
                f"Valid targets: {', '.join(sorted(outputs.keys()))}"
            ),
        }
    except Exception:
        return None


def register_pipeline_tools(orchestrator: PipelineOrchestrator) -> dict[str, Any]:
    """
    Register pipeline management tools.

    Args:
        orchestrator: Pipeline orchestrator instance

    Returns:
        Dictionary of tool functions
    """

    async def _list_airflow_connections(
        conn_id_prefix: str | None = None,
        conn_type: str | None = None,
    ) -> dict[str, Any]:
        """
        List existing Airflow connections.

        Supports optional filtering by connection id prefix and connection type.

        Args:
            conn_id_prefix: Only include connections whose `connection_id` starts with this prefix
            conn_type: Only include connections whose `conn_type` matches

        Returns:
            Dictionary containing total count and list of connections
        """
        try:
            logger.info("Listing Airflow connections")

            connections = await orchestrator.async_airflow_client.list_connections()

            # Apply filters if provided
            def _match_conn(c: dict[str, Any]) -> bool:
                ok = True
                if conn_id_prefix:
                    ok = ok and str(c.get("connection_id", "")).startswith(str(conn_id_prefix))
                if conn_type:
                    ok = ok and str(c.get("conn_type", "")).lower() == str(conn_type).lower()
                return ok

            filtered = [c for c in connections if _match_conn(c)]

            result = {
                "success": True,
                "total_count": len(filtered),
                "connections": filtered,
                "filters": {
                    "conn_id_prefix": conn_id_prefix,
                    "conn_type": conn_type,
                },
            }

            logger.info("Found %d Airflow connection(s)", len(filtered))
            return sanitize_response(result)

        except Exception as e:
            logger.error("Failed to list Airflow connections: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to list Airflow connections. Check server logs for details.",
                "total_count": 0,
                "connections": [],
            }

    async def _create_airflow_teradata_connection(
        connection_id: str = "teradata_default",
        teradata_profile: str | None = None,
    ) -> dict[str, Any]:
        """
        Create an Airflow connection for Teradata database.

        Uses the default Teradata connection credentials. The LLM never handles passwords.
        If a connection with the given `connection_id` already exists with
        matching configuration, it will be reused.

        Args:
            connection_id: Airflow connection ID (default: 'teradata_default')
            teradata_profile: Optional. Only needed to target a different Teradata
                system. Omit for normal use — default credentials are used.

        Returns:
            Dictionary with creation status and connection details
        """
        try:
            logger.info("Ensuring Teradata connection in Airflow: %s", connection_id)

            settings = orchestrator.settings

            # Resolve credentials from profile or .env settings
            if teradata_profile:
                guard = orchestrator.credential_resolver.guard_configured()
                if guard:
                    return guard
                profile = orchestrator.credential_resolver.resolve_profile(teradata_profile)
                resolved_host = profile.get("host", settings.teradata.host)
                resolved_database = (
                    profile.get("database")
                    or profile.get("schema")
                    or profile.get("default_schema")
                    or settings.teradata.database
                )
                resolved_username = profile.get("username", settings.teradata.username)
                settings_password = (
                    settings.teradata.password.get_secret_value()
                    if hasattr(settings.teradata.password, "get_secret_value")
                    else settings.teradata.password
                )
                resolved_password = profile.get("password", settings_password)
                resolved_port = profile.get("port", settings.teradata.port or 1025)
                if "password" not in profile and settings_password:
                    logger.warning(
                        "Profile '%s' has no 'password' field; falling back to TERADATA_PASSWORD from .env",
                        teradata_profile,
                    )
            else:
                resolved_host = settings.teradata.host
                resolved_database = settings.teradata.database
                resolved_username = settings.teradata.username
                resolved_password = (
                    settings.teradata.password.get_secret_value()
                    if hasattr(settings.teradata.password, "get_secret_value")
                    else settings.teradata.password
                )
                resolved_port = settings.teradata.port or 1025

            # Check if connection already exists and validate if details match
            final_connection_id = connection_id

            try:
                existing = await orchestrator.async_airflow_client.get_connection(
                    final_connection_id
                )

                # Validate if existing connection matches the provided details
                existing_host = existing.get("host", "")
                existing_schema = existing.get("schema", "")
                existing_login = existing.get("login", "")
                existing_port = existing.get("port", 0)

                # Check if key fields match
                host_match = existing_host == str(resolved_host)
                schema_match = existing_schema == str(resolved_database)
                login_match = existing_login == str(resolved_username)
                port_match = existing_port == int(resolved_port)

                if host_match and schema_match and login_match and port_match:
                    logger.info(
                        "Teradata connection '%s' already exists with matching details",
                        final_connection_id,
                    )
                    return sanitize_response(
                        {
                            "success": True,
                            "created": False,
                            "reused": True,
                            "connection_id": final_connection_id,
                            "connection": existing,
                            "host": resolved_host,
                            "database": resolved_database,
                            "port": resolved_port,
                            "message": f"Connection '{final_connection_id}' already exists with matching configuration",
                        }
                    )
                else:
                    # Details don't match, create new connection with incremented ID
                    logger.warning(
                        "Teradata connection '%s' exists but details don't match. "
                        "Host: %s, Schema: %s, Login: %s, Port: %s",
                        final_connection_id,
                        host_match,
                        schema_match,
                        login_match,
                        port_match,
                    )

                    # Find available connection ID by incrementing
                    base_id = final_connection_id
                    counter = 1
                    while True:
                        new_id = f"{base_id}_{counter}"
                        try:
                            check_conn = await orchestrator.async_airflow_client.get_connection(
                                new_id
                            )
                            # If connection exists, check if it matches
                            check_host = check_conn.get("host", "")
                            check_schema = check_conn.get("schema", "")
                            check_login = check_conn.get("login", "")
                            check_port = check_conn.get("port", 0)

                            if (
                                check_host == str(resolved_host)
                                and check_schema == str(resolved_database)
                                and check_login == str(resolved_username)
                                and check_port == int(resolved_port)
                            ):
                                logger.info("Found matching Teradata connection: %s", new_id)
                                return sanitize_response(
                                    {
                                        "success": True,
                                        "created": False,
                                        "reused": True,
                                        "connection_id": new_id,
                                        "connection": check_conn,
                                        "host": resolved_host,
                                        "database": resolved_database,
                                        "port": resolved_port,
                                        "message": f"Reusing existing connection '{new_id}' with matching configuration",
                                    }
                                )
                            else:
                                # Connection exists but doesn't match, try next ID
                                counter += 1
                        except Exception as inner_error:
                            # Connection doesn't exist (404 error), use this ID
                            error_msg = str(inner_error).lower()
                            if (
                                "404" in error_msg
                                or "not found" in error_msg
                                or "was not found" in error_msg
                                or "does not exist" in error_msg
                            ):
                                final_connection_id = new_id
                                break
                            else:
                                # Unexpected error, log and try next
                                logger.warning(
                                    "Unexpected error checking %s: %s", new_id, inner_error
                                )
                                counter += 1

                        # Safety limit
                        if counter > 100:
                            raise ValueError(
                                "Too many connection ID variations, please use a different base connection_id"
                            )

            except Exception as check_error:
                # Connection doesn't exist (404 error), proceed with creation
                error_msg = str(check_error).lower()
                if (
                    "404" in error_msg
                    or "not found" in error_msg
                    or "was not found" in error_msg
                    or "does not exist" in error_msg
                ):
                    logger.info(
                        "Connection '%s' does not exist (404), creating new one",
                        final_connection_id,
                    )
                else:
                    # Unexpected error during check
                    logger.warning(
                        "Unexpected error checking connection existence: %s", check_error
                    )

            # Build extra with auth settings
            resolved_logmech = settings.teradata.logmech or "TD2"
            td_extra: dict[str, Any] = {}
            if resolved_logmech != "TD2":
                td_extra["logmech"] = resolved_logmech
                logdata = settings.teradata.logdata
                if logdata and hasattr(logdata, "get_secret_value"):
                    logdata = logdata.get_secret_value()
                if logdata:
                    td_extra["logdata"] = str(logdata)
                if settings.teradata.oidc_clientid:
                    td_extra["oidc_clientid"] = settings.teradata.oidc_clientid
                if settings.teradata.sslca:
                    td_extra["sslca"] = settings.teradata.sslca

            # Create connection
            created = await orchestrator.async_airflow_client.create_connection(
                conn_id=final_connection_id,
                conn_type="teradata",
                host=resolved_host,
                schema=resolved_database,
                login=resolved_username,
                password=resolved_password,
                port=resolved_port,
                extra=td_extra if td_extra else None,
            )

            logger.info("Created Airflow Teradata connection: %s", final_connection_id)

            return sanitize_response(
                {
                    "success": True,
                    "created": True,
                    "reused": False,
                    "connection_id": final_connection_id,
                    "connection": created,
                    "host": resolved_host,
                    "database": resolved_database,
                    "port": resolved_port,
                    "message": f"Successfully created Teradata connection '{final_connection_id}'",
                }
            )

        except Exception as e:
            logger.error("Failed to create Teradata connection: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to create Teradata connection. Check server logs for details.",
                "connection_id": connection_id,
            }

    async def _create_airflow_airbyte_connection(
        connection_id: str = "airbyte_default",
    ) -> dict[str, Any]:
        """
        Create an Airflow connection for the Airbyte provider.

        All credentials (host, client_id, client_secret, token_url) are
        resolved from `.env` Airbyte settings. The LLM never handles secrets.

        If a connection with the given `connection_id` already exists with
        matching configuration, it will be reused.

        Args:
            connection_id: Airflow connection id (defaults to 'airbyte_default')

        Returns:
            Dictionary with creation status and connection details
        """
        try:
            logger.info("Ensuring Airbyte connection in Airflow: %s", connection_id)

            settings = orchestrator.settings

            # All credentials resolved from .env settings — never from LLM
            # OAuth2 field mapping:
            # - host: Full server URL
            # - login: client_id
            # - schema: token_url
            # - password: client_secret

            resolved_host = settings.airbyte.base_url or "http://localhost:8000"
            if not resolved_host.endswith("/api/public/v1/"):
                if not resolved_host.endswith("/"):
                    resolved_host += "/"
                if not resolved_host.endswith("api/public/v1/"):
                    resolved_host += "api/public/v1/"

            resolved_client_id = settings.airbyte.client_id
            resolved_token_url = settings.airbyte.token_url
            resolved_client_secret = (
                settings.airbyte.client_secret.get_secret_value()
                if (
                    settings.airbyte.client_secret
                    and hasattr(settings.airbyte.client_secret, "get_secret_value")
                )
                else settings.airbyte.client_secret
            )

            # Check if connection already exists and validate if details match
            final_connection_id = connection_id

            try:
                existing = await orchestrator.async_airflow_client.get_connection(
                    final_connection_id
                )

                # Validate if existing connection matches the provided details
                existing_host = existing.get("host", "")
                existing_login = existing.get("login", "")
                existing_schema = existing.get("schema", "")

                # Check if key fields match
                host_match = existing_host == str(resolved_host)
                login_match = (
                    existing_login == str(resolved_client_id) if resolved_client_id else True
                )
                schema_match = (
                    existing_schema == str(resolved_token_url) if resolved_token_url else True
                )

                if host_match and login_match and schema_match:
                    logger.info(
                        "Airflow connection '%s' already exists with matching details",
                        final_connection_id,
                    )
                    return sanitize_response(
                        {
                            "success": True,
                            "created": False,
                            "reused": True,
                            "connection_id": final_connection_id,
                            "connection": existing,
                            "message": f"Connection '{final_connection_id}' already exists with matching configuration",
                        }
                    )
                else:
                    # Details don't match, create new connection with incremented ID
                    logger.warning(
                        "Connection '%s' exists but details don't match. "
                        "Host: %s, Login: %s, Schema: %s",
                        final_connection_id,
                        host_match,
                        login_match,
                        schema_match,
                    )

                    # Find available connection ID by incrementing
                    base_id = final_connection_id
                    counter = 1
                    while True:
                        new_id = f"{base_id}_{counter}"
                        try:
                            check_conn = await orchestrator.async_airflow_client.get_connection(
                                new_id
                            )
                            # If connection exists, check if it matches
                            check_host = check_conn.get("host", "")
                            check_login = check_conn.get("login", "")
                            check_schema = check_conn.get("schema", "")

                            if (
                                check_host == str(resolved_host)
                                and (
                                    check_login == str(resolved_client_id)
                                    if resolved_client_id
                                    else True
                                )
                                and (
                                    check_schema == str(resolved_token_url)
                                    if resolved_token_url
                                    else True
                                )
                            ):
                                logger.info("Found matching connection: %s", new_id)
                                return sanitize_response(
                                    {
                                        "success": True,
                                        "created": False,
                                        "reused": True,
                                        "connection_id": new_id,
                                        "connection": check_conn,
                                        "message": f"Reusing existing connection '{new_id}' with matching configuration",
                                    }
                                )
                            else:
                                # Connection exists but doesn't match, try next ID
                                counter += 1
                        except Exception as check_error:
                            # Connection doesn't exist (404 error), use this ID
                            error_msg = str(check_error).lower()
                            if (
                                "404" in error_msg
                                or "not found" in error_msg
                                or "does not exist" in error_msg
                            ):
                                logger.info(
                                    "Connection '%s' not found (404), will create it",
                                    new_id,
                                )
                            else:
                                logger.debug(
                                    "Error checking connection '%s': %s", new_id, error_msg
                                )
                            final_connection_id = new_id
                            break

            except Exception as get_error:
                # Connection doesn't exist (404 error: "The Connection with connection_id: `xxx` was not found")
                error_msg = str(get_error).lower()
                if "404" in error_msg or "not found" in error_msg or "does not exist" in error_msg:
                    logger.info(
                        "Connection '%s' does not exist (404), will create new one",
                        final_connection_id,
                    )
                else:
                    logger.warning(
                        "Error checking connection '%s': %s",
                        final_connection_id,
                        error_msg,
                    )

            # Create Airbyte connection using OAuth2 field mapping
            # host = full server URL
            # login = client_id
            # schema = token_url
            # password = client_secret
            # port = None
            # extra = None
            created = await orchestrator.async_airflow_client.create_connection(
                conn_id=final_connection_id,
                conn_type="airbyte",
                host=str(resolved_host),
                login=str(resolved_client_id) if resolved_client_id else None,
                schema=str(resolved_token_url) if resolved_token_url else None,
                password=str(resolved_client_secret) if resolved_client_secret else None,
                port=None,
                extra=None,
            )

            logger.info("Created Airflow Airbyte connection: %s", final_connection_id)
            return sanitize_response(
                {
                    "success": True,
                    "created": True,
                    "reused": False,
                    "connection_id": final_connection_id,
                    "connection": created,
                    "notes": [
                        f"Use '{final_connection_id}' as airbyte_conn_id when generating DAGs.",
                    ],
                }
            )

        except Exception as e:
            logger.error("Failed to create Airbyte connection: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to create Airbyte connection. Check server logs for details.",
                "connection_id": connection_id,
            }

    async def _create_airflow_ssh_connection(
        connection_id: str = "ssh_default",
        ssh_profile: str | None = None,
        timeout: int | None = None,
        strict_ssh: bool = False,
    ) -> dict[str, Any]:
        """Create an Airflow SSH connection for runtime execution.

        This connection is used by Airflow to SSH back to the MCP client
        machine to run BTEQ, TdLoad, or dbt commands.

        Credentials are resolved from:
        1. A connection profile (connections.yaml) if ssh_profile is provided.
        2. MCP_CLIENT_SSH_* environment variables as fallback.

        Args:
            connection_id: Connection identifier in Airflow (default: 'ssh_localhost')
            ssh_profile: Connection profile name from connections.yaml.
            timeout: SSH command timeout in seconds (default: 300)
            strict_ssh: Whether to enforce strict SSH host key checking. If True
                (default), host keys must be known/verified. If False, host key
                checking is relaxed to allow connecting to hosts whose keys are
                not already trusted.

        Returns:
            Dictionary with creation status and connection details.
        """
        try:
            logger.info("Creating Airflow SSH connection: %s", connection_id)

            if ssh_profile:
                guard = orchestrator.credential_resolver.guard_configured()
                if guard:
                    return guard
                profile = orchestrator.credential_resolver.resolve_profile(ssh_profile)
                resolved_host = profile.get("host")
                resolved_port = profile.get("port", 22)
                resolved_username = profile.get("username")
                resolved_key_file = profile.get("key_file")
                resolved_password = profile.get("password")
            else:
                resolved_host = os.getenv("MCP_CLIENT_SSH_HOST")
                raw_port = os.getenv("MCP_CLIENT_SSH_PORT", "22")
                resolved_username = os.getenv("MCP_CLIENT_SSH_USER")
                resolved_key_file = os.getenv("MCP_CLIENT_SSH_KEY_PATH")
                resolved_password = os.getenv("MCP_CLIENT_SSH_PASSWORD")
                resolved_port = raw_port

            try:
                resolved_port = int(resolved_port)
                if not (1 <= resolved_port <= 65535):
                    raise ValueError("out of range")
            except (ValueError, TypeError) as port_err:
                raise ValueError(
                    f"SSH port must be a valid port number (1-65535), "
                    f"got '{resolved_port}': {port_err}"
                ) from port_err

            resolved_timeout = timeout or 300

            source = f"profile '{ssh_profile}'" if ssh_profile else "environment variables"

            if not resolved_host or (
                isinstance(resolved_host, str) and UNRESOLVED_ENV_VAR.search(resolved_host)
            ):
                try:
                    existing = await orchestrator.async_airflow_client.get_connection(connection_id)
                    if (existing.get("conn_type") or "").lower() == "ssh":
                        logger.info("Reusing existing SSH connection: %s", connection_id)
                        return sanitize_response({
                            "success": True,
                            "created": False,
                            "reused": True,
                            "connection_id": connection_id,
                            "host": existing.get("host"),
                            "port": existing.get("port", 22),
                            "username": existing.get("login"),
                            "message": f"Reusing existing connection '{connection_id}'",
                        })
                except Exception:
                    pass
                raise ValueError(
                    f"SSH host not configured for connection '{connection_id}'. "
                    "Provide an ssh_profile from connections.yaml or set MCP_CLIENT_SSH_HOST."
                )
            if not resolved_username or (
                isinstance(resolved_username, str) and UNRESOLVED_ENV_VAR.search(resolved_username)
            ):
                raise ValueError(
                    f"SSH username not configured for connection '{connection_id}'. "
                    "Provide an ssh_profile from connections.yaml or set MCP_CLIENT_SSH_USER."
                )
            if not resolved_key_file and not resolved_password:
                raise ValueError(
                    f"SSH connection '{connection_id}' has no password or key_file configured "
                    f"(from {source}). Authentication will fail at runtime."
                )
            if isinstance(resolved_password, str):
                m = UNRESOLVED_ENV_VAR.search(resolved_password)
                if m:
                    raise ValueError(
                        f"SSH password not configured for connection '{connection_id}'. "
                        f"Set {m.group(1)} environment variable or provide password directly "
                        f"(from {source})."
                    )
            if isinstance(resolved_key_file, str):
                m = UNRESOLVED_ENV_VAR.search(resolved_key_file)
                if m:
                    raise ValueError(
                        f"SSH key_file not configured for connection '{connection_id}'. "
                        f"Set {m.group(1)} environment variable or provide key_file path directly "
                        f"(from {source})."
                    )

            extras: dict[str, Any] = {
                "timeout": resolved_timeout,
            }
            if resolved_key_file:
                extras["key_file"] = resolved_key_file
            if not strict_ssh or resolved_host in ("localhost", "127.0.0.1", "::1"):
                extras["no_host_key_check"] = True
            else:
                extras["no_host_key_check"] = False

            # Check if a matching connection already exists in Airflow
            final_connection_id = connection_id
            match_fields = {
                "host": str(resolved_host).lower().strip(),
                "login": str(resolved_username).lower().strip(),
            }

            try:
                _page_size = 100
                _offset = 0
                while True:
                    page = await orchestrator.async_airflow_client.list_connections(
                        limit=_page_size, offset=_offset,
                    )
                    for conn in page:
                        if (conn.get("conn_type") or "").lower() != "ssh":
                            continue
                        raw_port = conn.get("port")
                        try:
                            conn_port = int(raw_port) if raw_port is not None else 22
                        except (ValueError, TypeError):
                            conn_port = 22
                        if conn_port != resolved_port:
                            continue
                        conn_extras = conn.get("extra", {}) or {}
                        if isinstance(conn_extras, str):
                            import json as _json
                            with contextlib.suppress(Exception):
                                conn_extras = _json.loads(conn_extras)
                        if not isinstance(conn_extras, dict):
                            conn_extras = {}
                        conn_key_file = conn_extras.get("key_file", "")
                        if resolved_key_file and conn_key_file != resolved_key_file:
                            continue
                        if all(
                            (conn.get(k) or "").lower().strip() == v
                            for k, v in match_fields.items()
                        ):
                            matched_id = conn.get("connection_id") or conn.get("conn_id")
                            logger.info("Reusing existing SSH connection: %s", matched_id)
                            return sanitize_response({
                                "success": True,
                                "created": False,
                                "reused": True,
                                "connection_id": matched_id,
                                "host": resolved_host,
                                "port": resolved_port,
                                "username": resolved_username,
                                "authentication": "key_file" if resolved_key_file else "password",
                                "message": f"Reusing existing connection '{matched_id}'",
                            })
                    if len(page) < _page_size:
                        break
                    _offset += _page_size
            except Exception as list_err:
                logger.warning("Could not search existing connections: %s", list_err)

            # Check if the requested ID is available
            try:
                existing = await orchestrator.async_airflow_client.get_connection(final_connection_id)
                existing_conn_type = (existing.get("conn_type") or "").lower() if isinstance(existing, dict) else ""
                existing_host = (existing.get("host") or "").lower().strip() if isinstance(existing, dict) else ""
                existing_login = (existing.get("login") or "").lower().strip() if isinstance(existing, dict) else ""
                raw_existing_port = existing.get("port") if isinstance(existing, dict) else None
                try:
                    existing_port = int(raw_existing_port) if raw_existing_port is not None else 22
                except (ValueError, TypeError):
                    existing_port = 22
                existing_extras = existing.get("extra", {}) or {} if isinstance(existing, dict) else {}
                if isinstance(existing_extras, str):
                    import json as _json
                    with contextlib.suppress(Exception):
                        existing_extras = _json.loads(existing_extras)
                if not isinstance(existing_extras, dict):
                    existing_extras = {}
                existing_key_file = existing_extras.get("key_file", "")
                if (
                    existing_conn_type == "ssh"
                    and existing_host == match_fields["host"]
                    and existing_login == match_fields["login"]
                    and existing_port == resolved_port
                    and (not resolved_key_file or existing_key_file == resolved_key_file)
                ):
                    logger.info("Reusing existing SSH connection: %s", final_connection_id)
                    return sanitize_response({
                        "success": True,
                        "created": False,
                        "reused": True,
                        "connection_id": final_connection_id,
                        "host": resolved_host,
                        "port": resolved_port,
                        "username": resolved_username,
                        "authentication": "key_file" if resolved_key_file else "password",
                        "message": f"Reusing existing connection '{final_connection_id}'",
                    })
                # ID taken with different config — find an available one
                counter = 1
                while counter <= 100:
                    new_id = f"{connection_id}_{counter}"
                    try:
                        await orchestrator.async_airflow_client.get_connection(new_id)
                        counter += 1
                    except Exception as probe_err:
                        probe_msg = str(probe_err).lower()
                        if any(s in probe_msg for s in ("404", "not found", "does not exist")):
                            final_connection_id = new_id
                            break
                        raise
                else:
                    raise ValueError(
                        "Too many connection ID variations, use a different base connection_id"
                    )
            except Exception as check_err:
                error_msg = str(check_err).lower()
                if not any(
                    s in error_msg for s in ("404", "not found", "does not exist")
                ):
                    logger.warning("Error checking connection existence: %s", check_err)

            created = await orchestrator.async_airflow_client.create_connection(
                conn_id=final_connection_id,
                conn_type="ssh",
                host=str(resolved_host),
                login=str(resolved_username),
                password=str(resolved_password) if resolved_password else None,
                port=resolved_port,
                extra=extras,
            )

            logger.info("Created Airflow SSH connection: %s", final_connection_id)

            test_status = "not_tested"
            test_message = "Connection test was skipped"
            test_result = None
            try:
                test_result = await orchestrator.async_airflow_client.test_airflow_connection(
                    connection_payload=created,
                )
                if isinstance(test_result, dict):
                    if test_result.get("status") in ("success", True):
                        test_status = "success"
                        test_message = str(test_result.get("message", "Connection test passed"))
                    else:
                        test_status = "failed"
                        test_message = str(test_result.get("message", "Connection test failed"))
                else:
                    test_status = "not_tested"
                    test_message = "Connection test returned unexpected result"
            except Exception as test_error:
                test_status = "error"
                test_message = "Connection test encountered an error."
                logger.error("SSH connection test failed: %s", test_error, exc_info=True)

            safe_test_result = None
            if isinstance(test_result, dict):
                safe_test_result = dict(test_result)
            safe_created = dict(created) if isinstance(created, dict) else None
            result = {
                "success": True,
                "created": True,
                "reused": False,
                "connection_id": final_connection_id,
                "connection": safe_created,
                "host": resolved_host,
                "port": resolved_port,
                "username": resolved_username,
                "authentication": "key_file" if resolved_key_file else "password",
                "no_host_key_check": extras.get("no_host_key_check", False),
                "test_status": test_status,
                "test_message": test_message,
                "test_result": safe_test_result,
            }
            if test_status != "success":
                result["warning"] = (
                    f"Connection was created but the connectivity test {test_status}. "
                    "Verify SSH credentials and host reachability before using this connection."
                )
            return sanitize_response(result)

        except Exception as e:
            logger.error("Failed to create SSH connection: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "connection_id": connection_id,
            }

    async def _create_airbyte_sync_dag(
        dag_id: str,
        connection_id: str,
        airbyte_conn_id: str | None = None,
        schedule: str = "@daily",
        owner: str | None = None,
        start_date_iso: str | None = None,
        tags: list[str] | None = None,
        email: list[str] | None = None,
        output_filename: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a minimal Airflow DAG to trigger an Airbyte sync.

        Generates a DAG with:
        - `start` (EmptyOperator)
        - `airbyte_sync` (AirbyteTriggerSyncOperator)
        - Optional `airbyte_sync_sensor` when `asynchronous=True`
        - `end` (EmptyOperator)

        The DAG is written to the configured local DAGs directory
        (`PIPELINE_DAGS_OUTPUT_DIR`). Use `deploy_dags_to_airflow` to
        copy it to the remote Airflow server.

        Args:
            dag_id: DAG identifier to create
            connection_id: Airbyte connection ID to sync
            airbyte_conn_id: Airflow connection ID for Airbyte provider (default 'airbyte_default' if omitted)
            schedule: DAG schedule interval (cron or preset)
            owner: DAG owner (defaults to settings.airflow.default_owner)
            start_date_iso: Optional ISO start date (YYYY-MM-DD or full ISO)
            tags: Optional DAG tags
            email: Optional notification email list
            output_filename: Optional file name (defaults to {dag_id}.py)

        Returns:
            Dictionary with creation results including file path and validation
        """
        try:
            logger.info("Generating Airbyte sync DAG: %s", dag_id)

            # Resolve defaults from orchestrator settings
            settings = orchestrator.settings
            dag_owner = owner or getattr(settings.airflow, "default_owner", "teradata_etl_mcp_server")
            ab_conn_id = airbyte_conn_id or "airbyte_default"

            # Validate output_filename against the DAGs directory (path containment).
            # Catches traversal at the MCP boundary with a clean error, before any
            # connection or generator work begins.
            dags_dir = Path(
                getattr(settings.pipeline, "dags_output_dir", Path("./airflow_dags"))
            )
            try:
                output_filename = _validate_output_filename(output_filename, dag_id, dags_dir)
            except ValueError as e:
                return {"success": False, "error": str(e)}

            # --- (A) Ensure Airflow connection for Airbyte provider ---
            actual_ab_conn_id = ab_conn_id
            airflow_connection_status = "unknown"
            airflow_connection_error = None
            try:
                logger.info("Validating Airbyte connection: %s", ab_conn_id)

                # Use create_airflow_airbyte_connection which resolves
                # all credentials from .env settings
                conn_result = await _create_airflow_airbyte_connection(
                    connection_id=ab_conn_id,
                )

                if conn_result.get("success"):
                    actual_ab_conn_id = conn_result.get("connection_id", ab_conn_id)
                    if conn_result.get("created"):
                        airflow_connection_status = "created"
                        logger.info("Created new Airbyte connection: %s", actual_ab_conn_id)
                    else:
                        airflow_connection_status = "reused"
                        logger.info("Reusing existing Airbyte connection: %s", actual_ab_conn_id)
                else:
                    airflow_connection_status = "failed"
                    airflow_connection_error = conn_result.get("error", "Unknown error")
                    logger.warning(
                        "Failed to create/validate connection: %s",
                        airflow_connection_error,
                    )

            except Exception as ab_error:
                airflow_connection_status = "failed"
                airflow_connection_error = safe_error_message(ab_error)
                logger.warning(
                    "Failed to create Airflow connection for Airbyte provider: %s. "
                    "DAG will be generated but may fail at runtime. "
                    "Ensure Airflow is reachable (AIRFLOW_BASE_URL) and create "
                    "the '%s' connection manually if needed",
                    ab_error,
                    ab_conn_id,
                )

            # --- (B) Read Airbyte schedule (non-mutating check) ---
            # The actual override (C) is deferred until after DAG generation
            # succeeds, so a template/validation failure won't leave the
            # Airbyte connection in manual with no DAG to trigger it.
            schedule_override_info = None
            airbyte_schedule_check = "unknown"
            airbyte_schedule_error = None
            _airbyte_current_type: str | None = None
            _airbyte_previous_cron: str | None = None
            try:
                airbyte_conn_detail = await orchestrator.airbyte_client.get_connection(
                    connection_id
                )
                conn_data = airbyte_conn_detail or {}
                airbyte_schedule = conn_data.get("schedule") or {}
                _airbyte_current_type = (
                    airbyte_schedule.get("scheduleType")
                    or conn_data.get("scheduleType")
                    or "manual"
                )

                if _airbyte_current_type == "cron":
                    _airbyte_previous_cron = (
                        airbyte_schedule.get("cronExpression")
                        or conn_data.get("cronExpression")
                        or "unknown"
                    )
                    airbyte_schedule_check = "needs_override"
                elif _airbyte_current_type == "manual":
                    airbyte_schedule_check = "already_manual"
                else:
                    airbyte_schedule_check = "checked"
            except (AirbyteConnectionError, CircuitBreakerOpen) as conn_err:
                airbyte_schedule_check = "unreachable"
                airbyte_schedule_error = (
                    f"Could not reach Airbyte API to check connection schedule: "
                    f"{safe_error_message(conn_err)}. Verify manually that connection "
                    f"'{connection_id}' is set to manual to avoid duplicate runs."
                )
                logger.warning(
                    "Could not reach Airbyte API to check schedule for %s: %s",
                    connection_id,
                    conn_err,
                )
            except AirbyteAPIError as api_err:
                airbyte_schedule_check = "api_error"
                airbyte_schedule_error = (
                    f"Airbyte API error while checking connection '{connection_id}': "
                    f"{safe_error_message(api_err)}. The connection may not exist. "
                    f"Verify manually that the connection is set to manual to avoid "
                    f"duplicate runs."
                )
                logger.warning(
                    "Airbyte API error checking schedule for %s: %s",
                    connection_id,
                    api_err,
                )
            except Exception as schedule_err:
                airbyte_schedule_check = "check_failed"
                airbyte_schedule_error = (
                    f"Unexpected error checking Airbyte connection schedule: "
                    f"{safe_error_message(schedule_err)}. Verify manually that connection "
                    f"'{connection_id}' is set to manual to avoid duplicate runs."
                )
                logger.warning(
                    "Unexpected error checking Airbyte schedule for %s: %s",
                    connection_id,
                    schedule_err,
                )

            # Resolve start date
            from datetime import datetime

            if start_date_iso:
                try:
                    try:
                        start_dt = datetime.fromisoformat(start_date_iso)
                    except Exception:
                        start_dt = datetime.strptime(start_date_iso, "%Y-%m-%d")
                except Exception:
                    start_dt = datetime.now(tz=timezone.utc).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
            else:
                start_dt = datetime.now(tz=timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )

            # Build minimal tasks and dependencies
            tasks: list[dict[str, Any]] = []
            deps: list[tuple[str, str]] = []

            tasks.append({"type": "empty", "task_id": "start"})

            tasks.append(
                {
                    "type": "airbyte",
                    "task_id": "airbyte_sync",
                    "connection_id": connection_id,
                    "airbyte_conn_id": actual_ab_conn_id,  # Use the validated/created connection ID
                }
            )

            # Set up dependencies: start -> airbyte_sync -> [sensor] -> end
            deps.append(("start", "airbyte_sync"))

            # Sensor automatically gets job_id from airbyte_sync.output via XComArgs
            deps.append(("airbyte_sync", "airbyte_sync_sensor"))
            downstream_sync_target = "airbyte_sync_sensor"

            tasks.append({"type": "empty", "task_id": "end"})
            deps.append((downstream_sync_target, "end"))

            # Generate DAG code via generator. output_filename has been
            # normalized and containment-validated by _validate_output_filename
            # at the top of the handler, so it is always non-None here.
            _dag_code = orchestrator.airflow_dag_generator.generate_dag(
                dag_id=dag_id,
                description=f"Airbyte sync DAG for connection {connection_id}",
                schedule=schedule,
                tasks=tasks,
                dependencies=deps,
                start_date=start_dt,
                owner=dag_owner,
                email=email,
                tags=(tags or ["airbyte", dag_id]),
                output_filename=output_filename,
            )

            # Re-validate via safe_join_within against the same dags_dir used
            # at the top of the handler — keeps a single containment base for
            # the whole create_sync_dag flow.
            dag_path = safe_join_within(dags_dir, output_filename)
            validation = orchestrator.airflow_dag_generator.validate_dag_file(dag_path)

            # --- (C) Apply deferred schedule override now that DAG is generated ---
            # Only mutate the Airbyte connection when ALL prerequisites are met:
            #   1. DAG file exists and passed syntax validation
            #   2. Airflow connection was successfully created/reused
            # Without both, setting Airbyte to manual would leave no trigger.
            dag_valid = dag_path.exists() and validation.get("valid", False)
            airflow_ready = airflow_connection_status in ("created", "reused")
            can_override = airbyte_schedule_check == "needs_override" and _airbyte_previous_cron
            if can_override and dag_valid and airflow_ready:
                try:
                    await orchestrator.airbyte_client.update_connection(
                        connection_id, schedule={"scheduleType": "manual"}
                    )
                    airbyte_schedule_check = "overridden_to_manual"
                    logger.info(
                        "Set Airbyte connection %s to manual (was cron: %s) "
                        "to prevent dual-scheduling with Airflow DAG '%s'",
                        connection_id,
                        _airbyte_previous_cron,
                        dag_id,
                    )
                    schedule_override_info = {
                        "schedule_overridden": True,
                        "previous_schedule_type": "cron",
                        "previous_cron_expression": _airbyte_previous_cron,
                        "new_schedule_type": "manual",
                        "reason": (
                            f"Airbyte connection schedule changed from cron "
                            f"('{_airbyte_previous_cron}') to manual to prevent duplicate "
                            f"runs. Airflow DAG '{dag_id}' will now be the sole "
                            f"trigger for this connection."
                        ),
                    }
                except Exception as update_err:
                    airbyte_schedule_check = "update_failed"
                    airbyte_schedule_error = (
                        f"Airbyte connection '{connection_id}' has cron schedule "
                        f"('{_airbyte_previous_cron}') but failed to set it to manual: "
                        f"{safe_error_message(update_err)}. This will cause duplicate runs. "
                        f"Set it to manual via airbyte_pipeline(action='update', "
                        f"connection_id='{connection_id}', schedule_type='manual')."
                    )
                    logger.warning(
                        "Failed to update Airbyte schedule to manual for %s: %s",
                        connection_id,
                        update_err,
                    )
            elif can_override and not dag_valid:
                airbyte_schedule_check = "skipped_dag_invalid"
                airbyte_schedule_error = (
                    f"Airbyte connection '{connection_id}' has cron schedule "
                    f"('{_airbyte_previous_cron}') but the generated DAG is invalid, "
                    f"so the schedule was NOT changed to manual. Fix the DAG and "
                    f"re-run, or set it to manual via airbyte_pipeline(action='update', "
                    f"connection_id='{connection_id}', schedule_type='manual')."
                )
                logger.warning(
                    "Skipping Airbyte schedule override for %s: DAG validation failed",
                    connection_id,
                )
            elif can_override and not airflow_ready:
                airbyte_schedule_check = "skipped_airflow_failed"
                airbyte_schedule_error = (
                    f"Airbyte connection '{connection_id}' has cron schedule "
                    f"('{_airbyte_previous_cron}') but the Airflow connection setup "
                    f"failed, so the schedule was NOT changed to manual. Resolve "
                    f"the Airflow connection issue first, then re-run or set it "
                    f"to manual via airbyte_pipeline(action='update', "
                    f"connection_id='{connection_id}', schedule_type='manual')."
                )
                logger.warning(
                    "Skipping Airbyte schedule override for %s: "
                    "Airflow connection setup failed (status=%s)",
                    connection_id,
                    airflow_connection_status,
                )

            result = {
                "success": True,
                "dag_id": dag_id,
                "output_file": str(dag_path),
                "airbyte_conn_id": actual_ab_conn_id,
                "syntax_valid": validation.get("valid", True),
                "syntax_error": validation.get("syntax_error"),
                "airflow_connection_status": airflow_connection_status,
                "airbyte_schedule_check": airbyte_schedule_check,
                "next_steps": [
                    f"**1. Deploy the DAG**: "
                    f"`pipeline_deploy(action='deploy_dags', "
                    f"pipeline_name='{dag_id}')`. **Why**: this is an "
                    f"Airbyte-only sync DAG; Airflow can't see it until the "
                    f"file is on the remote server. **Effect**: SFTPs the "
                    f"local DAG to the Airflow ``dags`` folder. **If "
                    f"missing**: if SSH isn't configured, ask the user to "
                    f"set ``AIRFLOW_REMOTE_SSH_KEY`` / "
                    f"``AIRFLOW_REMOTE_HOST`` via the Setup Wizard.",
                    f"**2. Trigger a sync**: "
                    f"`dag_trigger(mode='run', dag_id='{dag_id}')`. **Why**: "
                    f"a manual trigger validates the Airbyte connection "
                    f"({actual_ab_conn_id}) end-to-end before the cron "
                    f"schedule fires. **Effect**: starts the AirbyteTrigger "
                    f"task, which calls Airbyte's API to begin a sync job. "
                    f"**If missing**: if the Airbyte connection isn't "
                    f"healthy, the task fails immediately — check "
                    f"`airbyte_pipeline(action='check_health', "
                    f"connection_name=...)` first.",
                ],
            }

            warnings: list[str] = []
            if airflow_connection_error:
                result["airflow_connection_error"] = airflow_connection_error
                warnings.append(
                    f"Airflow connection setup failed: {airflow_connection_error}. "
                    f"DAG was generated but will fail at runtime until the "
                    f"Airflow connection '{actual_ab_conn_id}' is created."
                )

            if schedule_override_info:
                result["schedule_override"] = schedule_override_info
                warnings.append(schedule_override_info["reason"])

            if airbyte_schedule_error:
                result["airbyte_schedule_error"] = airbyte_schedule_error
                warnings.append(airbyte_schedule_error)

            if warnings:
                result["warnings"] = warnings

            logger.info("Generated Airbyte sync DAG at %s", dag_path)
            return result

        except Exception as e:
            logger.error("Failed to generate Airbyte sync DAG: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to generate Airbyte sync DAG. Check server logs for details.",
                "dag_id": dag_id,
            }

    async def _create_dbt_dag(
        dag_id: str,
        project_name: str | None = None,
        dbt_models: list[str] | None = None,
        dbt_target: str = "dev",
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
        schedule: str = "@daily",
        owner: str | None = None,
        tags: list[str] | None = None,
        output_filename: str | None = None,
        use_ssh_for_dbt: bool | None = None,
        ssh_conn_id: str = "ssh_default",
        ssh_profile: str | None = None,
    ) -> dict[str, Any]:
        try:
            logger.info("Generating dbt-only DAG: %s", dag_id)

            settings = orchestrator.settings

            # Validate output_filename against the DAGs directory (path containment).
            dags_dir = Path(
                getattr(settings.pipeline, "dags_output_dir", Path("./airflow_dags"))
            )
            try:
                output_filename = _validate_output_filename(output_filename, dag_id, dags_dir)
            except ValueError as e:
                return {"success": False, "error": str(e), "dag_id": dag_id}

            # Locate the sub-project by ``project_name``. The Teradata-
            # identity binding is informational only — the dbt task in
            # the generated DAG runs ``dotenv run -- dbt ...`` against
            # the per-sub-project ``.env``, which is the cred source.
            resolved = _locate_dbt_subproject_dir(orchestrator, project_name)
            if isinstance(resolved, dict):
                resolved["dag_id"] = dag_id
                return resolved
            sub_project_dir, identity = resolved
            resolved_dbt_dir = str(sub_project_dir)

            # Auto-detect: use SSH when remote host is configured
            if use_ssh_for_dbt is None:
                use_ssh_for_dbt = bool(
                    settings.airflow.remote_host or os.getenv("MCP_CLIENT_SSH_HOST")
                )
                if use_ssh_for_dbt:
                    logger.info("Auto-enabled SSH for dbt (remote Airflow host configured)")
            dag_owner = owner or getattr(settings.airflow, "default_owner", "teradata_etl_mcp_server")

            # Ensure SSH connection exists in Airflow when SSH execution is requested
            if use_ssh_for_dbt:
                logger.info(
                    "Ensuring Airflow SSH connection '%s' for dbt execution", ssh_conn_id
                )
                ssh_result = await _create_airflow_ssh_connection(
                    connection_id=ssh_conn_id,
                    ssh_profile=ssh_profile,
                )
                if not ssh_result.get("success"):
                    return {
                        "success": False,
                        "error": (
                            f"Failed to create SSH connection '{ssh_conn_id}' in Airflow: "
                            f"{ssh_result.get('error', 'unknown error')}"
                        ),
                        "dag_id": dag_id,
                    }
                ssh_conn_id = ssh_result.get("connection_id", ssh_conn_id)
                logger.info("SSH connection '%s' ready for dbt DAG", ssh_conn_id)

            if not use_ssh_for_dbt:
                target_error = _validate_dbt_target(resolved_dbt_dir, dbt_target)
                if target_error:
                    target_error["dag_id"] = dag_id
                    return target_error

            # resolved_dbt_dir is the sub-project path bound to ``identity``.

            # Rule 5: the MCP server no longer pushes Teradata creds into
            # Airflow Variables. The dbt step inside the generated DAG
            # expects whoever owns the Airflow worker to provision the
            # ``TERADATA_*`` env vars out-of-band (Airflow Variables /
            # Connection extras / sealed secrets / etc.). Baking the
            # wizard-default identity into Airflow's DB here would expose
            # dev credentials in production environments.

            _dag_code = orchestrator.airflow_dag_generator.generate_dbt_only_dag(
                dag_id=dag_id,
                project_dir=resolved_dbt_dir,
                dbt_env=None,
                models=dbt_models,
                run_tests=run_dbt_tests,
                generate_docs=generate_dbt_docs,
                schedule=schedule,
                target=dbt_target,
                owner=dag_owner,
                tags=tags,
                output_filename=output_filename,
                use_ssh=use_ssh_for_dbt,
                ssh_conn_id=ssh_conn_id,
            )

            # output_filename already validated at the top of the function.
            # Use the same dags_dir as the up-front validation for one base.
            dag_path = safe_join_within(dags_dir, output_filename)

            validation = orchestrator.airflow_dag_generator.validate_dag_file(dag_path)

            # Shape the ``refresh_env`` hint to match the identity binding:
            # ``wizard:<host_slug>`` is a synthetic identity (a "wizard-default
            # bound to host X" marker), NOT a connections.yaml profile name.
            # Passing it as ``teradata_profile`` would trigger a named-profile
            # lookup and fail (only the literal sentinels ``"wizard"`` /
            # ``"default"`` fold to wizard-default; ``wizard:<...>`` does not).
            # For wizard-bound sub-projects, refresh_env should be called
            # WITHOUT ``teradata_profile`` so it infers the identity from
            # Settings. For named profile bindings (e.g. ``"prod"``), the
            # binding string IS the profile name to pass.
            _refresh_form = _refresh_env_call_hint(orchestrator, project_name, identity)

            result = {
                "success": True,
                "dag_id": dag_id,
                "output_file": str(dag_path),
                "dbt_project_dir": resolved_dbt_dir,
                "teradata_identity": identity,
                "dbt_models": dbt_models,
                "dbt_target": dbt_target,
                "schedule": schedule,
                "use_ssh": use_ssh_for_dbt,
                "ssh_conn_id": ssh_conn_id if use_ssh_for_dbt else None,
                "syntax_valid": validation.get("valid", True),
                "syntax_error": validation.get("syntax_error"),
                "next_steps": [
                    (
                        f"**1. Sync the dbt sub-project to the Airflow worker** "
                        f"(if not done yet): copy ``{resolved_dbt_dir}`` "
                        f"(including its ``.env``) to the same path on the "
                        f"Airflow worker. **Why**: the generated DAG runs "
                        f"``dotenv run -- dbt ...`` from this directory and "
                        f"reads ``TERADATA_*`` from the per-sub-project "
                        f"``.env`` file. The DAG deploy step ships only the "
                        f"``.py`` file, not the dbt project. **Effect**: the "
                        f"worker has both the DAG and the dbt project + .env "
                        f"to source from. **If missing**: the dbt task will "
                        f"fail with ``Could not find profiles.yml`` or "
                        f"``dotenv: command not found``."
                    ),
                    (
                        "**2. Install python-dotenv[cli] on the Airflow "
                        "worker** (one-time): "
                        "``pip install \"python-dotenv[cli]\"``. **Why**: "
                        "the DAG invokes ``dotenv run -- dbt ...`` to load "
                        "``.env`` cross-platform; the CLI ships with the "
                        "``[cli]`` extra. **Effect**: the ``dotenv`` command "
                        "becomes available on the worker's PATH. **If "
                        "missing**: every dbt task fails with ``dotenv: "
                        "command not found``."
                    ),
                    (
                        f"**3. Deploy the DAG to Airflow**: "
                        f"`pipeline_deploy(action='deploy_dags', "
                        f"pipeline_name='{dag_id}')`. **Why**: the DAG file "
                        f"is local-only; Airflow can't see it until it's "
                        f"SFTP'd to the remote ``dags`` folder. **Effect**: "
                        f"copies ``{output_filename}`` to the remote server "
                        f"and (if requested) waits for Airflow to parse it. "
                        f"**If missing**: if SSH isn't configured, ask the "
                        f"user to set ``AIRFLOW_REMOTE_SSH_KEY`` / "
                        f"``AIRFLOW_REMOTE_HOST`` via the Setup Wizard, OR "
                        f"pass ``ssh_profile`` naming a profile in "
                        f"connections.yaml."
                    ),
                    (
                        f"**4. Trigger a run**: "
                        f"`dag_trigger(mode='run', dag_id='{dag_id}')`. "
                        f"**Why**: deploying alone doesn't schedule anything; "
                        f"the DAG runs on its cron (``{schedule}``) OR on "
                        f"explicit trigger. **Effect**: starts a manual run; "
                        f"tasks execute on the Airflow worker. **If "
                        f"missing**: if the DAG isn't yet visible after "
                        f"deploy (parse delay), run "
                        f"`pipeline_status(action='dag', dag_id='{dag_id}')` "
                        f"to confirm Airflow has parsed the file."
                    ),
                    (
                        f"**5. After credential rotation**: "
                        f"`{_refresh_form['call']}` and re-sync the sub-"
                        f"project to the worker. **Why**: the ``.env`` is a "
                        f"one-shot snapshot; rotated credentials don't "
                        f"propagate automatically. {_refresh_form['why_extra']} "
                        f"**Effect**: overwrites .env locally; you still need "
                        f"to copy the file to the worker. **If missing**: dbt "
                        f"will use stale credentials until the next refresh."
                    ),
                ],
            }

            logger.info("Generated dbt-only DAG at %s", dag_path)
            return result

        except Exception as e:
            logger.error("Failed to generate dbt-only DAG: %s", e, exc_info=True)
            return {
                "success": False,
                "error": f"Failed to generate dbt-only DAG: {safe_error_message(e)}",
                "dag_id": dag_id,
            }

    async def _create_elt_dag(
        dag_id: str,
        connection_id: str,
        airbyte_conn_id: str | None = None,
        source_name: str | None = None,
        target_schema: str | None = None,
        project_name: str | None = None,
        dbt_models: list[str] | None = None,
        dbt_target: str = "dev",
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
        use_ssh_for_dbt: bool | None = None,
        schedule: str = "@daily",
        owner: str | None = None,
        tags: list[str] | None = None,
        output_filename: str | None = None,
    ) -> dict[str, Any]:
        try:
            logger.info("Generating ELT pipeline DAG: %s", dag_id)

            settings = orchestrator.settings

            # Validate output_filename against the DAGs directory (path containment).
            dags_dir = Path(
                getattr(settings.pipeline, "dags_output_dir", Path("./airflow_dags"))
            )
            try:
                output_filename = _validate_output_filename(output_filename, dag_id, dags_dir)
            except ValueError as e:
                return {"success": False, "error": str(e), "dag_id": dag_id}

            # Locate the dbt sub-project by ``project_name``. Same as in
            # ``_create_dbt_dag``: identity binding is informational; the
            # cred source at task runtime is the per-sub-project ``.env``.
            resolved = _locate_dbt_subproject_dir(orchestrator, project_name)
            if isinstance(resolved, dict):
                resolved["dag_id"] = dag_id
                return resolved
            sub_project_dir, identity = resolved
            resolved_dbt_dir = str(sub_project_dir)

            # Auto-detect: use SSH when remote host is configured
            if use_ssh_for_dbt is None:
                use_ssh_for_dbt = bool(
                    settings.airflow.remote_host or os.getenv("MCP_CLIENT_SSH_HOST")
                )
                if use_ssh_for_dbt:
                    logger.info("Auto-enabled SSH for dbt (remote Airflow host configured)")

            dag_owner = owner or getattr(settings.airflow, "default_owner", "teradata_etl_mcp_server")
            ab_conn_id = airbyte_conn_id or "airbyte_default"
            ssh_conn_id = "ssh_default"
            resolved_source_name = source_name or "source"
            resolved_target_schema = target_schema or "public"

            # Auto-create Airbyte connection in Airflow
            logger.info("Ensuring Airflow Airbyte connection: %s", ab_conn_id)
            ab_result = await _create_airflow_airbyte_connection(connection_id=ab_conn_id)
            if ab_result.get("success"):
                ab_conn_id = ab_result.get("connection_id", ab_conn_id)
                logger.info("Airbyte connection ready: %s", ab_conn_id)
            else:
                logger.warning("Could not create Airbyte connection: %s", ab_result.get("error"))

            # Auto-create SSH connection in Airflow for dbt
            if use_ssh_for_dbt:
                logger.info("Ensuring Airflow SSH connection: %s", ssh_conn_id)
                ssh_result = await _create_airflow_ssh_connection(connection_id=ssh_conn_id)
                if not ssh_result.get("success"):
                    return {
                        "success": False,
                        "error": (
                            f"Failed to create SSH connection '{ssh_conn_id}' in Airflow: "
                            f"{ssh_result.get('error', 'unknown error')}"
                        ),
                        "dag_id": dag_id,
                    }
                ssh_conn_id = ssh_result.get("connection_id", ssh_conn_id)
                logger.info("SSH connection ready: %s", ssh_conn_id)

            extract_config = {
                "method": "airbyte",
                "connection_id": connection_id,
                "airbyte_conn_id": ab_conn_id,
            }

            # resolved_dbt_dir already computed & validated above.

            # Rule 5: same removal as ``_create_dbt_dag`` — the MCP server
            # no longer pushes Teradata creds into Airflow Variables for
            # the dbt portion of an ELT DAG. The Airflow worker is
            # responsible for provisioning ``TERADATA_*`` env vars
            # out-of-band (e.g. Airflow Connection extras, Variables, or
            # sealed secrets).

            transform_config = {
                "project_dir": resolved_dbt_dir,
                "models": dbt_models,
                "target": dbt_target,
                "env_from_variables": None,
            }

            if not use_ssh_for_dbt:
                target_error = _validate_dbt_target(resolved_dbt_dir, dbt_target)
                if target_error:
                    target_error["dag_id"] = dag_id
                    return target_error

            _dag_code = orchestrator.airflow_dag_generator.generate_elt_pipeline_dag(
                dag_id=dag_id,
                source_name=resolved_source_name,
                target_schema=resolved_target_schema,
                extract_config=extract_config,
                transform_config=transform_config,
                schedule=schedule,
                owner=dag_owner,
                use_ssh_for_dbt=use_ssh_for_dbt,
                ssh_conn_id=ssh_conn_id,
                tags=tags,
                output_filename=output_filename,
                run_dbt_tests=run_dbt_tests,
                generate_dbt_docs=generate_dbt_docs,
            )

            # output_filename already validated at the top of the function.
            # Use the same dags_dir as the up-front validation for one base.
            dag_path = safe_join_within(dags_dir, output_filename)

            validation = orchestrator.airflow_dag_generator.validate_dag_file(dag_path)

            # See ``_create_dbt_dag`` for the rationale — wizard:<host_slug>
            # bindings need a different refresh_env hint than named profiles.
            _refresh_form = _refresh_env_call_hint(orchestrator, project_name, identity)

            result = {
                "success": True,
                "dag_id": dag_id,
                "output_file": str(dag_path),
                "connection_id": connection_id,
                "dbt_project_dir": resolved_dbt_dir,
                "teradata_identity": identity,
                "dbt_models": dbt_models,
                "dbt_target": dbt_target,
                "schedule": schedule,
                "syntax_valid": validation.get("valid", True),
                "syntax_error": validation.get("syntax_error"),
                "airflow_connections": {
                    "airbyte": ab_conn_id,
                    "ssh": ssh_conn_id if use_ssh_for_dbt else None,
                },
                "next_steps": [
                    (
                        f"**1. Sync the dbt sub-project to the Airflow worker** "
                        f"(if not done yet): copy ``{resolved_dbt_dir}`` "
                        f"(including its ``.env``) to the same path on the "
                        f"Airflow worker. **Why**: the dbt half of this ELT "
                        f"DAG runs ``dotenv run -- dbt ...`` from this "
                        f"directory and reads ``TERADATA_*`` from the per-"
                        f"sub-project ``.env``. The deploy step ships only "
                        f"the ``.py`` file. **Effect**: the worker has "
                        f"everything dbt needs. **If missing**: dbt task "
                        f"fails with ``Could not find profiles.yml``."
                    ),
                    (
                        "**2. Install python-dotenv[cli] on the Airflow "
                        "worker** (one-time): "
                        "``pip install \"python-dotenv[cli]\"``. **Why**: "
                        "the dbt task invokes ``dotenv run --`` to load "
                        "``.env`` cross-platform. **Effect**: the ``dotenv`` "
                        "command becomes available on the worker. **If "
                        "missing**: every dbt task fails with ``dotenv: "
                        "command not found``."
                    ),
                    (
                        f"**3. Deploy the DAG**: "
                        f"`pipeline_deploy(action='deploy_dags', "
                        f"pipeline_name='{dag_id}')`. **Why**: combines an "
                        f"Airbyte sync ({connection_id}) with dbt; both "
                        f"halves require the DAG file on the Airflow server. "
                        f"**Effect**: SFTPs ``{output_filename}`` to the "
                        f"remote ``dags`` folder. **If missing**: if SSH "
                        f"isn't configured, ask the user to set "
                        f"``AIRFLOW_REMOTE_SSH_KEY`` / ``AIRFLOW_REMOTE_HOST`` "
                        f"via the Setup Wizard."
                    ),
                    (
                        f"**4. Trigger a run**: "
                        f"`dag_trigger(mode='run', dag_id='{dag_id}')`. "
                        f"**Why**: the DAG schedule is ``{schedule}``; a "
                        f"manual trigger lets you validate the chained "
                        f"Airbyte→dbt flow before waiting. **Effect**: kicks "
                        f"off Airbyte sync first, then the dbt task chain. "
                        f"**If missing**: if the Airbyte connection "
                        f"({connection_id}) isn't healthy, the sync task "
                        f"fails — check "
                        f"`airbyte_pipeline(action='check_health')` first."
                    ),
                    (
                        f"**5. After credential rotation**: "
                        f"`{_refresh_form['call']}` and re-sync the sub-"
                        f"project to the worker. **Why**: the ``.env`` is a "
                        f"one-shot snapshot; rotated credentials don't "
                        f"propagate automatically. {_refresh_form['why_extra']} "
                        f"**Effect**: overwrites .env locally; you still need "
                        f"to copy the file to the worker. **If missing**: dbt "
                        f"will use stale credentials until refresh."
                    ),
                ],
            }

            logger.info("Generated ELT pipeline DAG at %s", dag_path)
            return result

        except Exception as e:
            logger.error("Failed to generate ELT pipeline DAG: %s", e, exc_info=True)
            return {
                "success": False,
                "error": f"Failed to generate ELT pipeline DAG: {safe_error_message(e)}",
                "dag_id": dag_id,
            }

    async def _deploy_dags_to_airflow(
        pipeline_name: str | None = None,
        local_dags_dir: str | None = None,
        remote_host: str | None = None,
        remote_user: str | None = None,
        remote_port: int | None = None,
        remote_dags_dir: str | None = None,
        auth_method: str = "key",
        ssh_key_path: str | None = None,
        strict_host_key_checking: bool = False,
        dry_run: bool = False,
        wait_for_dag_loaded: bool = False,
        max_wait_seconds: int = 360,
        trigger_after_deploy: bool = False,
        trigger_config: dict[str, Any] | None = None,
        validate_imports: bool = True,
        create_backup: bool = True,
        rollback_on_failure: bool = True,
    ) -> dict[str, Any]:
        """
        Deploy generated Airflow DAG(s) to a remote Airflow server over SSH/SFTP.

        ⚠️ DESIGN NOTE: This tool performs infrastructure deployment via SSH/SFTP,
        which extends beyond typical MCP server scope. Consider these alternatives:

        **Recommended Deployment Options:**
        1. **Manual Copy** - Review generated DAG, manually copy to Airflow (safest)
        2. **CI/CD Pipeline** - Use GitHub Actions/GitLab CI for controlled deployment
        3. **Explicit Deployment** - Use this tool with full awareness of SSH operations

        **Why This Matters:**
        - MCP servers should focus on code generation, not infrastructure orchestration
        - Automatic deployment can hide important security and configuration decisions
        - SSH credential management creates security implications

        **Safety Features (v2.1):**
        - validate_imports=True: Validates Python imports before deployment
        - create_backup=True: Backs up existing DAGs before overwrite
        - rollback_on_failure=True: Restores backup if deployment causes errors
        - strict_host_key_checking: Enforces SSH host key verification (opt-in;
          default False — every connection logs a uniform WARNING when False)

        Behavior:
        - If pipeline_name is provided, deploys only {pipeline_name}.py
        - Otherwise, deploys all .py files in local_dags_dir
        - SSH settings are taken from parameters or fallbacks from environment and settings
        - Returns immediately after file transfer (no polling by default)

        Environment fallbacks (checked in this order where relevant):
        - Host: AIRFLOW_REMOTE_HOST, parsed from AIRFLOW_BASE_URL
        - User: AIRFLOW_REMOTE_USER
        - Port: AIRFLOW_REMOTE_PORT (int)
        - Key: AIRFLOW_REMOTE_SSH_KEY
        - Password: AIRFLOW_REMOTE_PASSWORD
        - Remote DAGs dir: AIRFLOW_DAG_FOLDER (default /opt/airflow/dags)
        - Local DAGs dir: PIPELINE_DAGS_OUTPUT_DIR from settings, else ./airflow_dags

        Args:
            pipeline_name: Optional DAG id to deploy a single file {pipeline_name}.py
            local_dags_dir: Local directory containing generated DAGs
            remote_host: SSH hostname or IP
            remote_user: SSH username
            remote_port: SSH port; if None, falls back to AIRFLOW_REMOTE_PORT / settings default (22)
            remote_dags_dir: Remote Airflow DAGs directory
            auth_method: "key" or "password" (default "key")
            ssh_key_path: Path to private key for key auth
            strict_host_key_checking: Enforce known_hosts checking (default False).
                Leave False for single-user/dev use where hosts are trusted; set
                True in production after provisioning the target host key in
                ~/.ssh/known_hosts. Regardless of setting, a WARNING is logged
                on every SSH connection made with strict_host_key_checking=False.
            dry_run: If True, do not transfer files; only report plan
            wait_for_dag_loaded: Wait for Airflow to discover the DAG after deployment
            max_wait_seconds: Maximum time to wait for DAG to appear (default 360)
            trigger_after_deploy: Automatically trigger the DAG after successful deployment
            trigger_config: Optional configuration parameters to pass to the triggered DAG run
            validate_imports: Validate Python imports locally before deployment (default True)
            create_backup: Backup existing DAGs on remote before overwrite (default True)
            rollback_on_failure: Restore backup if DAG fails to load (default True)

        Returns:
            Dictionary containing deployment summary
        """
        from urllib.parse import urlparse

        logger.info("Preparing DAG deployment to Airflow via SSH/SFTP")

        # Resolve settings/env defaults
        settings = orchestrator.settings

        def _env(*names: str, default: str | None = None) -> str | None:
            for n in names:
                v = os.getenv(n)
                if v:
                    return v
            return default

        # Local DAGs directory
        local_dir = Path(
            local_dags_dir
            or str(getattr(settings.pipeline, "dags_output_dir", Path("./airflow_dags")))
        )
        local_dir = local_dir if local_dir.is_absolute() else Path.cwd() / local_dir

        # Remote host defaults from settings (reads AIRFLOW_REMOTE_HOST) or parsed from base_url
        host = remote_host or settings.airflow.remote_host
        if not host:
            # Try parsing from configured base_url
            try:
                base_url = settings.airflow.base_url
                if base_url:
                    host = urlparse(base_url).hostname
            except Exception:
                host = None

        # Remote user
        user = remote_user or settings.airflow.remote_user

        # Remote port: explicit caller value takes precedence; fall back to settings
        remote_port = remote_port if remote_port is not None else settings.airflow.remote_port
        if not (1 <= remote_port <= 65535):
            return {
                "success": False,
                "error": f"SSH port {remote_port} is out of range (1–65535). "
                "Check the 'remote_port' parameter or AIRFLOW_REMOTE_PORT.",
            }

        # Remote DAGs directory
        remote_dir = remote_dags_dir or settings.airflow.dag_folder
        remote_dir_posix = str(PurePosixPath(remote_dir))

        # Auth resolution - credentials sourced from settings (reads AIRFLOW_REMOTE_* from env)
        key_path = ssh_key_path or settings.airflow.remote_ssh_key
        key_passphrase = (
            settings.airflow.remote_ssh_key_passphrase.get_secret_value()
            if settings.airflow.remote_ssh_key_passphrase
            else None
        )
        pwd = (
            settings.airflow.remote_password.get_secret_value()
            if settings.airflow.remote_password
            else None
        )

        # Expand ~ in key path to absolute path
        if key_path:
            key_path = str(Path(key_path).expanduser().resolve())

        # Decide auth method based on available credentials
        if auth_method:
            # Explicit method provided by user
            method = auth_method.lower().strip()
        elif key_path:
            # Key file available, prefer key-based auth
            method = "key"
        elif pwd:
            # Password available, use password auth
            method = "password"
        else:
            # No credentials found, default to key and let validation catch it
            method = "key"

        # Validate inputs
        problems: list[str] = []
        if not local_dir.exists() or not local_dir.is_dir():
            problems.append(f"Local DAGs directory not found: {local_dir}")
        if not remote_dir_posix:
            problems.append(
                "Remote DAGs directory not provided (set remote_dags_dir or AIRFLOW_DAG_FOLDER)"
            )
        if not host:
            problems.append("Remote host not provided and not inferable from environment")
        if not user:
            problems.append("Remote user not provided and not found in environment")
        if method == "key" and not key_path:
            problems.append(
                "SSH key path required for key-based auth. "
                "Ask the user to update AIRFLOW_REMOTE_SSH_KEY via the Setup Wizard, "
                "OR pass ``ssh_profile`` naming a profile in connections.yaml. "
                "The agent must not create or edit .env."
            )
        if method == "key" and key_path and not Path(key_path).exists():
            problems.append(
                f"SSH key file not found at {key_path}. "
                "Ask the user to update AIRFLOW_REMOTE_SSH_KEY via the Setup Wizard, "
                "OR pass ``ssh_profile`` naming a profile in connections.yaml. "
                "The agent must not create or edit .env."
            )
        if method == "password" and not pwd:
            problems.append(
                "SSH password is required for password-based auth and not configured. "
                "Ask the user to update AIRFLOW_REMOTE_PASSWORD via the Setup Wizard, "
                "OR pass ``ssh_profile`` naming a profile in connections.yaml. The "
                "agent must not create or edit .env."
            )

        # Build file list
        files_to_deploy: list[Path] = []
        if local_dir.exists():
            if pipeline_name:
                candidate = local_dir / f"{pipeline_name}.py"
                if candidate.exists():
                    files_to_deploy.append(candidate)
                else:
                    problems.append(
                        f"DAG file not found for pipeline '{pipeline_name}': {candidate}"
                    )
            else:
                files_to_deploy = [p for p in local_dir.glob("*.py") if p.is_file()]
                if not files_to_deploy:
                    problems.append(f"No .py files found in {local_dir}")

        plan = {
            "remote_host": host,
            "remote_user": user,
            "remote_port": remote_port,
            "remote_dir": remote_dir_posix,
            "auth_method": method,
            "local_dir": str(local_dir),
            "file_count": len(files_to_deploy),
            "files": [str(p) for p in files_to_deploy],
            "dry_run": dry_run,
            "validate_imports": validate_imports,
            "create_backup": create_backup,
            "rollback_on_failure": rollback_on_failure,
        }

        # Validate Python imports locally before deployment
        if validate_imports and files_to_deploy:
            import ast
            import sys

            from teradata_etl_mcp_server.generators.airflow_dag_generator import AirflowDAGGenerator

            validation_errors: list[str] = []
            for dag_file in files_to_deploy:
                try:
                    content = dag_file.read_text(encoding="utf-8")
                    # Parse the AST to check syntax
                    tree = ast.parse(content, filename=str(dag_file))

                    # Check for bare Name expression statements at module level.
                    # See AirflowDAGGenerator.find_bare_name_errors for details.
                    for bare_name_error in AirflowDAGGenerator.find_bare_name_errors(tree):
                        validation_errors.append(
                            f"{dag_file.name}: {bare_name_error}"
                            f" - will cause NameError at import time if the name is not defined"
                        )

                    # Extract import statements
                    imports: list[str] = []
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Import):
                            for alias in node.names:
                                imports.append(alias.name.split(".")[0])
                        elif isinstance(node, ast.ImportFrom) and node.module:
                            imports.append(node.module.split(".")[0])

                    # Check if critical Airflow imports are present
                    airflow_imports = [i for i in imports if i.startswith("airflow")]
                    if not airflow_imports:
                        validation_errors.append(
                            f"{dag_file.name}: No airflow imports found - may not be a valid DAG"
                        )

                    # Try to verify imports are resolvable (best effort)
                    missing_imports: list[str] = []
                    for imp in set(imports):
                        if imp in sys.stdlib_module_names:
                            continue
                        try:
                            __import__(imp)
                        except ImportError:
                            # Not critical - Airflow environment may have different packages
                            if imp not in ("airflow",):  # airflow may not be installed locally
                                missing_imports.append(imp)

                    if missing_imports:
                        logger.warning(
                            "%s: Some imports not locally available (may be OK on Airflow): %s",
                            dag_file.name,
                            missing_imports,
                        )

                except SyntaxError as se:
                    validation_errors.append(
                        f"{dag_file.name}: Python syntax error at line {se.lineno}: {se.msg}"
                    )
                except Exception as ve:
                    validation_errors.append(f"{dag_file.name}: Validation error: {ve}")

            if validation_errors:
                logger.error("DAG validation failed: %s", validation_errors)
                return {
                    "success": False,
                    "errors": validation_errors,
                    "plan": plan,
                    "validation_failed": True,
                }

            logger.info("DAG validation passed for %d file(s)", len(files_to_deploy))

        if problems:
            logger.error("DAG deployment validation failed: %s", problems)
            return {
                "success": False,
                "errors": problems,
                "plan": plan,
            }

        if dry_run:
            logger.info("Dry run requested; not transferring files")
            return {
                "success": True,
                "deployed": 0,
                "plan": plan,
                "message": "Dry run - no files transferred",
            }

        # Perform SSH/SFTP transfer
        try:
            try:
                import paramiko
            except Exception as ie:
                raise RuntimeError(
                    "Paramiko is required for SSH/SFTP deployment. Install with: pip install paramiko"
                ) from ie

            ssh = paramiko.SSHClient()
            _configure_ssh_host_key_policy(
                ssh, strict_host_key_checking, context="deploy_dags"
            )

            connect_kwargs: dict[str, Any] = {
                "hostname": host,
                "username": user,
                "port": remote_port,
                "timeout": 30,
            }
            if method == "key":
                if not Path(key_path).exists():
                    raise RuntimeError(f"SSH key file not found: {key_path}")
                connect_kwargs["key_filename"] = key_path
                if key_passphrase:
                    connect_kwargs["passphrase"] = key_passphrase
            else:
                connect_kwargs["password"] = pwd

            logger.info("Connecting to %s@%s:%s ...", user, host, remote_port)
            ssh.connect(**connect_kwargs)
            sftp = ssh.open_sftp()

            # Track backups for potential rollback
            backup_files: list[dict[str, str]] = []
            deployed: list[dict[str, Any]] = []

            try:
                # Expand ~ in remote directory path via SFTP (no shell interpolation)
                expanded_remote_dir = remote_dir_posix
                if remote_dir_posix.startswith("~/") or remote_dir_posix == "~":
                    # Use SFTP normalize on "." to get the remote home directory
                    try:
                        home_dir = sftp.normalize(".")
                        if remote_dir_posix == "~":
                            expanded_remote_dir = home_dir
                        else:
                            expanded_remote_dir = home_dir + remote_dir_posix[1:]
                        logger.info(
                            "Expanded remote dir: %s -> %s",
                            remote_dir_posix,
                            expanded_remote_dir,
                        )
                    except Exception:
                        logger.warning("Could not expand ~, using as-is: %s", remote_dir_posix)

                # Ensure remote directory exists
                def _ensure_remote_dir(path_str: str):
                    _, stdout_ch, stderr_ch = ssh.exec_command(  # nosec B601
                        f"mkdir -p {shlex.quote(path_str)}",
                        timeout=15,
                    )
                    stdout_ch.channel.recv_exit_status()  # Wait for command to complete
                    logger.info("Ensured remote dir: %s", path_str)

                _ensure_remote_dir(expanded_remote_dir)

                # Create backup directory if needed
                backup_dir = str(PurePosixPath(expanded_remote_dir) / ".dag_backups")
                if create_backup:
                    _ensure_remote_dir(backup_dir)

                # Create backups of existing files before overwrite
                if create_backup:
                    from datetime import datetime as dt

                    backup_timestamp = dt.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                    for src in files_to_deploy:
                        dest = str(PurePosixPath(expanded_remote_dir) / src.name)
                        backup_path = str(
                            PurePosixPath(backup_dir) / f"{src.stem}_{backup_timestamp}{src.suffix}"
                        )
                        try:
                            # Check if file exists on remote
                            sftp.stat(dest)
                            # File exists, create backup
                            ssh.exec_command(  # nosec B601
                                f"cp {shlex.quote(dest)} {shlex.quote(backup_path)}",
                                timeout=30,
                            )
                            backup_files.append({"original": dest, "backup": backup_path})
                            logger.info("Backed up %s -> %s", dest, backup_path)
                        except OSError:
                            # File doesn't exist, no backup needed
                            pass

                # Deploy files
                for src in files_to_deploy:
                    dest = str(PurePosixPath(expanded_remote_dir) / src.name)
                    logger.info("SFTP put: local=%s -> remote=%s", str(src), dest)
                    if not src.exists():
                        raise FileNotFoundError(f"Local DAG file not found: {src}")
                    sftp.put(str(src), dest)
                    deployed.append({"source": str(src), "destination": dest})
                    logger.info("Uploaded %s -> %s", src, dest)

                # Best-effort: update a fixed marker file to ensure scheduler notices changes
                # M5: Use fixed name to avoid accumulating timestamped marker files
                try:
                    marker = str(PurePosixPath(expanded_remote_dir) / ".last_deployed")
                    ssh.exec_command(  # nosec B601
                        f"/usr/bin/touch {shlex.quote(marker)} || touch {shlex.quote(marker)}",
                        timeout=15,  # M6: Prevent hanging on unresponsive commands
                    )
                    logger.info("Updated marker file: %s", marker)
                except Exception:  # noqa: S110  # nosec B110
                    pass
            finally:
                with contextlib.suppress(Exception):
                    sftp.close()
                with contextlib.suppress(Exception):
                    ssh.close()

            result = {
                "success": True,
                "deployed": len(deployed),
                "remote_host": host,
                "remote_dir": expanded_remote_dir,
                "auth_method": method,
                "files": deployed,
                "backups_created": len(backup_files),
                "backup_files": backup_files,
            }

            # Wait for DAG to be loaded by Airflow if requested
            if wait_for_dag_loaded and pipeline_name:
                logger.info("Waiting for DAG '%s' to be loaded by Airflow...", pipeline_name)
                try:
                    wait_result = await orchestrator.async_airflow_client.wait_for_dag(
                        dag_id=pipeline_name,
                        max_wait_seconds=max_wait_seconds,
                        poll_interval=2,
                    )
                    result["dag_loaded"] = True
                    result["dag_load_time"] = wait_result.get("elapsed_seconds")
                    result["dag_load_attempts"] = wait_result.get("attempts")
                    result["dag_info"] = wait_result.get("dag")
                    logger.info("DAG '%s' successfully loaded by Airflow", pipeline_name)
                except TimeoutError as te:
                    logger.warning(
                        "DAG '%s' not loaded within %ds: %s",
                        pipeline_name,
                        max_wait_seconds,
                        te,
                    )
                    result["dag_loaded"] = False
                    result["warning"] = (
                        f"DAG deployed but not yet recognized by Airflow. "
                        f"It may take up to {max_wait_seconds}s for the scheduler to parse new DAG files."
                    )

                    # Rollback if requested and we have backups
                    if rollback_on_failure and backup_files:
                        logger.warning("Initiating rollback due to DAG load timeout...")
                        result["rollback_attempted"] = True
                        try:
                            # Reconnect for rollback
                            ssh_rollback = paramiko.SSHClient()
                            _configure_ssh_host_key_policy(
                                ssh_rollback,
                                strict_host_key_checking,
                                context="deploy_dags_rollback",
                            )
                            ssh_rollback.connect(**connect_kwargs)

                            try:
                                for backup in backup_files:
                                    ssh_rollback.exec_command(  # nosec B601
                                        f"cp {shlex.quote(backup['backup'])} {shlex.quote(backup['original'])}",
                                        timeout=30,
                                    )
                                    logger.info(
                                        "Rolled back %s from %s",
                                        backup["original"],
                                        backup["backup"],
                                    )
                                result["rollback_success"] = True
                                result["rollback_message"] = (
                                    f"Restored {len(backup_files)} file(s) from backup"
                                )
                            finally:
                                with contextlib.suppress(Exception):
                                    ssh_rollback.close()
                        except Exception as rollback_error:
                            logger.error("Rollback failed: %s", rollback_error, exc_info=True)
                            result["rollback_success"] = False
                            result["rollback_error"] = str(rollback_error)

                except Exception as wait_error:
                    logger.error(
                        "Error while waiting for DAG to load: %s", wait_error, exc_info=True
                    )
                    result["dag_loaded"] = False
                    result["dag_load_error"] = "Failed to check DAG load status. Check server logs."
            elif wait_for_dag_loaded and not pipeline_name:
                result["dag_loaded"] = "skipped"
                result["warning"] = (
                    "Cannot wait for DAG - multiple files deployed. Specify pipeline_name to wait for specific DAG."
                )

            # Always attempt to trigger DAG if requested and pipeline_name is provided
            if trigger_after_deploy and pipeline_name:
                logger.info("Triggering DAG run for '%s'...", pipeline_name)
                try:
                    trigger_result = await orchestrator.async_airflow_client.trigger_dag(
                        dag_id=pipeline_name,
                        conf=trigger_config or {},
                    )
                    result["dag_triggered"] = True
                    result["dag_run_id"] = trigger_result.get("dag_run_id")
                    result["trigger_info"] = trigger_result
                    logger.info(
                        "DAG run triggered successfully: %s",
                        trigger_result.get("dag_run_id"),
                    )
                except Exception as trigger_error:
                    logger.error("Failed to trigger DAG: %s", trigger_error, exc_info=True)
                    result["dag_triggered"] = False
                    result["trigger_error"] = (
                        "Failed to trigger DAG. Check server logs for details."
                    )
                    result["warning"] = (
                        "DAG deployed but trigger failed. Check server logs for details."
                    )

            # Attach next_steps when the deploy succeeded so the LLM can
            # chain the typical follow-on calls (trigger, monitor) without
            # guessing.
            if result.get("success"):
                _trigger_done = result.get("dag_triggered")
                triggered_run_id = result.get("dag_run_id") or "<run_id>"
                _name = pipeline_name or "<dag_id>"
                result["next_steps"] = [
                    (
                        f"**1. Verify Airflow has parsed the DAG**: "
                        f"`pipeline_status(action='dag', dag_id='{_name}')`. "
                        f"**Why**: deploy SFTPs the file but Airflow's scheduler "
                        f"polls the dags folder on its own interval (default ~30s). "
                        f"**Effect**: returns the DAG's parse status, schedule, "
                        f"and is_paused flag. **If missing**: if the DAG isn't "
                        f"yet listed, wait 30-60s and retry; if it stays missing, "
                        f"check Airflow scheduler logs for parse errors."
                    ),
                    (
                        f"**2. Trigger a manual run**: "
                        f"`dag_trigger(mode='run', dag_id='{_name}')`. **Why**: "
                        f"deployment doesn't schedule a run; the DAG runs on its "
                        f"own cron OR on explicit trigger. **Effect**: starts a "
                        f"manual run on the Airflow worker; tasks execute "
                        f"immediately. **If missing**: skip if you triggered via "
                        f"``trigger_after_deploy=True`` already (the response's "
                        f"``dag_triggered`` field shows whether that fired)."
                    )
                    if not _trigger_done
                    else (
                        f"**1. Monitor the auto-triggered run**: "
                        f"`dag_monitor(action='status', dag_id='{_name}', "
                        f"dag_run_id='{triggered_run_id}')`. **Why**: "
                        f"``trigger_after_deploy=True`` already started a run "
                        f"({triggered_run_id}); track it to completion. "
                        f"**Effect**: returns task states and overall run state. "
                        f"**If missing**: if the run errored, the response carries "
                        f"a ``trigger_error`` field — inspect that and re-trigger "
                        f"with `dag_trigger(mode='run', dag_id='{_name}')`."
                    ),
                ]

            return result

        except Exception as e:
            logger.error("DAG deployment failed: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "DAG deployment failed. Check server logs for details.",
                "plan": plan,
            }

    async def _get_pipeline_status(
        pipeline_name: str,
        include_run_history: bool = True,
    ) -> dict[str, Any]:
        """
        Get current status and details of a pipeline.

        Retrieves pipeline configuration, execution status, recent runs,
        and performance statistics from Airflow.

        Args:
            pipeline_name: Pipeline identifier (DAG ID)
            include_run_history: Whether to include recent run history

        Returns:
            Dictionary with pipeline status information
        """
        try:
            logger.info("Getting status for pipeline: %s", pipeline_name)

            status = await orchestrator.get_pipeline_status_async(dag_id=pipeline_name)

            result = {
                "success": True,
                "pipeline_name": pipeline_name,
                "is_paused": status.get("is_paused"),
                "last_run": status.get("last_run"),
                "statistics": status.get("statistics", {}),
            }

            if include_run_history:
                result["recent_runs"] = status.get("recent_runs", [])

            logger.info("Retrieved status for %s", pipeline_name)

            return result

        except Exception as e:
            logger.error("Failed to get pipeline status: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to get pipeline status. Check server logs for details.",
                "pipeline_name": pipeline_name,
            }

    async def _check_dag_exists(
        dag_id: str,
    ) -> dict[str, Any]:
        """
        Check if a DAG exists in Airflow without polling or blocking.

        \u2705 **RECOMMENDED ALTERNATIVE** to wait_for_dag_loaded polling in deploy_dags_to_airflow.

        **Why Use This:**
        - Immediate response (no polling/waiting)
        - Follows MCP principle of stateless operations
        - User controls when to check (explicit vs automatic)
        - Can be called multiple times if needed

        **Typical Workflow:**
        1. Deploy DAG: `deploy_dags_to_airflow(pipeline_name="my_dag")`
        2. Check status: `check_dag_exists(dag_id="my_dag")`
        3. If not found, wait 30s and check again (user controlled)
        4. Trigger when ready: `trigger_dag_run(dag_id="my_dag")`

        Args:
            dag_id: DAG identifier to check

        Returns:
            Dictionary with exists status, DAG info if found, and helpful next steps
        """
        try:
            logger.info("Checking if DAG '%s' exists in Airflow...", dag_id)

            # Use Airflow API to check for DAG
            dag_info = await orchestrator.async_airflow_client.get_dag(dag_id=dag_id)

            if dag_info:
                return {
                    "success": True,
                    "exists": True,
                    "dag_id": dag_id,
                    "dag_info": dag_info,
                    "is_paused": dag_info.get("is_paused", False),
                    "file_path": dag_info.get("fileloc"),
                    "next_steps": [
                        "DAG is loaded and ready",
                        f"Unpause: mcp_tool('pause_pipeline', pipeline_name='{dag_id}')"
                        if dag_info.get("is_paused")
                        else "DAG is active",
                        f"Trigger: mcp_tool('trigger_dag_run', dag_id='{dag_id}')",
                    ],
                }
            else:
                return {
                    "success": True,
                    "exists": False,
                    "dag_id": dag_id,
                    "message": "DAG not found in Airflow",
                    "reasons": [
                        "Airflow hasn't parsed the DAG file yet (can take up to dag_dir_list_interval, default 5 min)",
                        "DAG file has syntax errors (check Airflow logs)",
                        "DAG file not in correct location",
                        "Airflow scheduler not running",
                    ],
                    "next_steps": [
                        "Wait 30-60 seconds and check again",
                        "Check Airflow scheduler logs for parsing errors",
                        "Verify DAG file was deployed to correct directory",
                        f"Try: mcp_tool('check_dag_exists', dag_id='{dag_id}') again",
                    ],
                }

        except Exception as e:
            logger.error("Error checking DAG existence: %s", e, exc_info=True)
            return {
                "success": False,
                "exists": False,
                "dag_id": dag_id,
                "error": "Error checking DAG existence. Check server logs for details.",
                "suggestion": "DAG might not exist or Airflow API is unreachable",
            }

    async def _trigger_dag_run(
        dag_id: str,
        conf: dict[str, Any] | None = None,
        wait_for_completion: bool = False,
        timeout_seconds: int = 3600,
    ) -> dict[str, Any]:
        """
        Explicitly trigger a DAG run with full user control.

        \u2705 **RECOMMENDED ALTERNATIVE** to trigger_after_deploy automatic triggering.

        **Why Use This:**
        - Explicit user action (no hidden automation)
        - User reviews deployment before triggering
        - Can pass runtime configuration parameters
        - Clear separation: generation \u2192 deployment \u2192 triggering

        **Typical Workflow:**
        1. Generate DAG: `generate_airflow_tdload_dag_from_csv(...)`
        2. Review generated DAG code
        3. Deploy DAG: `deploy_dags_to_airflow(pipeline_name="my_dag")`
        4. Verify DAG loaded: `check_dag_exists(dag_id="my_dag")`
        5. Trigger execution: `trigger_dag_run(dag_id="my_dag")` \u2190 YOU ARE HERE

        **Runtime Configuration:**
        Pass parameters to DAG at runtime via `conf` parameter:
        ```python
        trigger_dag_run(
            dag_id="my_dag",
            conf={
                "source_file": "/data/updated_customers.csv",
                "full_refresh": True
            }
        )
        ```

        Args:
            dag_id: DAG identifier to trigger
            conf: Optional runtime configuration parameters (passed as dag_run.conf)
            wait_for_completion: Wait for DAG run to complete (default: False for async)
            timeout_seconds: Maximum wait time if wait_for_completion=True

        Returns:
            Dictionary with trigger results, run ID, and status tracking URL
        """
        try:
            logger.info("Triggering DAG run for '%s'...", dag_id)

            # Check if DAG exists first
            dag_check = await _check_dag_exists(dag_id=dag_id)
            if not dag_check.get("exists"):
                return {
                    "success": False,
                    "error": f"DAG '{dag_id}' does not exist in Airflow",
                    "suggestion": "Deploy the DAG first using deploy_dags_to_airflow or check DAG name",
                    "check_result": dag_check,
                }

            # Check if DAG is paused
            if dag_check.get("is_paused"):
                return {
                    "success": False,
                    "error": f"DAG '{dag_id}' is paused",
                    "suggestion": f"Unpause the DAG first: mcp_tool('resume_pipeline', pipeline_name='{dag_id}')",
                }

            # Trigger the DAG
            trigger_result = await orchestrator.async_airflow_client.trigger_dag(
                dag_id=dag_id, conf=conf or {}
            )

            dag_run_id = trigger_result.get("dag_run_id")

            result = {
                "success": True,
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "state": trigger_result.get("state", "queued"),
                "conf": conf or {},
                "triggered_at": trigger_result.get("execution_date"),
                "airflow_ui_url": f"{orchestrator.settings.airflow.base_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}",
                "next_steps": [
                    f"Monitor run: mcp_tool('get_dag_run_status', dag_id='{dag_id}', run_id='{dag_run_id}')",
                    f"View logs: {orchestrator.settings.airflow.base_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}",
                    "Check pipeline status: mcp_tool('get_pipeline_status', pipeline_name='"
                    + dag_id
                    + "')",
                ],
            }

            # Wait for completion if requested
            if wait_for_completion:
                logger.info(
                    "Waiting for DAG run %s to complete (timeout: %ds)...",
                    dag_run_id,
                    timeout_seconds,
                )
                try:
                    final_status = await orchestrator.async_airflow_client.wait_for_dag_run(
                        dag_id=dag_id,
                        dag_run_id=dag_run_id,
                        timeout_seconds=timeout_seconds,
                    )
                    result["final_state"] = final_status.get("state")
                    result["duration_seconds"] = final_status.get("duration_seconds")
                    result["completed"] = True
                except TimeoutError:
                    result["completed"] = False
                    result["timeout"] = True
                    result["message"] = f"DAG run did not complete within {timeout_seconds}s"

            logger.info("Successfully triggered DAG run: %s", dag_run_id)
            return result

        except Exception as e:
            logger.error("Failed to trigger DAG run: %s", e, exc_info=True)
            return {
                "success": False,
                "dag_id": dag_id,
                "error": "Failed to trigger DAG run. Check server logs for details.",
            }

    async def _get_dag_run_status(
        dag_id: str,
        run_id: str,
    ) -> dict[str, Any]:
        """
        Get the status of a specific DAG run.

        **Use After Triggering:**
        After triggering a DAG with `trigger_dag_run`, use this to check execution status.

        Args:
            dag_id: DAG identifier
            run_id: DAG run ID (returned from trigger_dag_run)

        Returns:
            Dictionary with run state, duration, task statuses, and logs URL
        """
        try:
            logger.info("Getting status for DAG run %s/%s...", dag_id, run_id)

            run_info = await orchestrator.async_airflow_client.get_dag_run(
                dag_id=dag_id, dag_run_id=run_id
            )

            return {
                "success": True,
                "dag_id": dag_id,
                "dag_run_id": run_id,
                "state": run_info.get("state"),
                "start_date": run_info.get("start_date"),
                "end_date": run_info.get("end_date"),
                "duration_seconds": run_info.get("duration"),
                "conf": run_info.get("conf", {}),
                "airflow_ui_url": f"{orchestrator.settings.airflow.base_url}/dags/{dag_id}/grid?dag_run_id={run_id}",
            }

        except Exception as e:
            logger.error("Failed to get DAG run status: %s", e, exc_info=True)
            return {
                "success": False,
                "dag_id": dag_id,
                "dag_run_id": run_id,
                "error": "Failed to get DAG run status. Check server logs for details.",
            }

    async def _list_pipelines(
        include_paused: bool = True,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        List all Airflow pipelines (DAGs) with filtering and comprehensive metadata.

        Retrieves complete pipeline inventory including status, schedules, tags, owners, and run history.
        Perfect for discovering existing pipelines, monitoring pipeline portfolio, finding specific DAGs
        by name or tags, or checking which pipelines are active vs paused.

        Use this to: View all data pipelines, find pipelines by tag, check pipeline status, inventory DAGs.

        Args:
            include_paused: Include paused pipelines in results (default: True).
                Inactive DAGs (whose DAG file no longer exists on disk) are always excluded.
            tags: Filter pipelines by tags (e.g., ['production', 'daily'] returns only matching pipelines)

        Returns:
            Dictionary with total count and list of pipelines with metadata (schedule, owner, tags, state)
        """
        try:
            logger.info("Listing all pipelines")

            # Get DAGs from Airflow without blocking.
            # only_active filters by is_active (DAG file present), not is_paused,
            # so paused filtering is applied client-side below.
            dags = await orchestrator.async_airflow_client.list_dags(
                only_active=True,
                tags=tags,
            )

            if not include_paused:
                dags = [dag for dag in dags if not dag.get("is_paused", False)]

            # Format results
            pipelines = []
            for dag in dags:
                pipeline_info = {
                    "pipeline_name": dag.get("dag_id"),
                    "is_paused": dag.get("is_paused"),
                    "tags": dag.get("tags", []),
                    "schedule": dag.get("schedule_interval"),
                    "last_run_date": dag.get("last_parsed_time"),
                }
                pipelines.append(pipeline_info)

            result = {
                "success": True,
                "total_count": len(pipelines),
                "pipelines": pipelines,
            }

            logger.info("Listed %d pipelines", len(pipelines))

            return result

        except Exception as e:
            logger.error("Failed to list pipelines: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to list pipelines. Check server logs for details.",
                "total_count": 0,
                "pipelines": [],
            }

    async def _fetch_dag_from_remote(
        pipeline_name: str,
        local_path: Path,
        strict_host_key_checking: bool = False,
    ) -> dict[str, Any]:
        """Pull a DAG file from the remote Airflow server via SFTP."""
        from urllib.parse import urlparse

        def _env(*names: str, default: str | None = None) -> str | None:
            for n in names:
                v = os.getenv(n)
                if v:
                    return v
            return default

        settings = orchestrator.settings

        host: str | None = settings.airflow.remote_host
        if not host:
            try:
                base_url = settings.airflow.base_url
                if base_url:
                    host = urlparse(base_url).hostname
            except Exception as exc:
                logger.debug("Could not parse Airflow base URL for SSH host: %s", exc)

        user: str | None = settings.airflow.remote_user
        port: int = settings.airflow.remote_port
        # Guarantee a non-None str so callers can safely call .startswith() etc.
        remote_dir: str = settings.airflow.dag_folder
        key_path: str | None = settings.airflow.remote_ssh_key
        if key_path:
            key_path = str(Path(key_path).expanduser().resolve())
        key_passphrase: str | None = (
            settings.airflow.remote_ssh_key_passphrase.get_secret_value()
            if settings.airflow.remote_ssh_key_passphrase
            else None
        )
        pwd: str | None = (
            settings.airflow.remote_password.get_secret_value()
            if settings.airflow.remote_password
            else None
        )

        if not host or not user:
            return {
                "success": False,
                "error": "Cannot fetch from remote: SSH host/user not configured "
                "(set AIRFLOW_REMOTE_HOST and AIRFLOW_REMOTE_USER)",
            }
        if not key_path and not pwd:
            return {
                "success": False,
                "error": "Cannot fetch from remote: no SSH credentials "
                "(set AIRFLOW_REMOTE_SSH_KEY or AIRFLOW_REMOTE_PASSWORD)",
            }
        if key_path and not Path(key_path).exists():
            return {
                "success": False,
                "error": f"SSH key file not found: {key_path}",
            }

        try:
            import paramiko
        except ImportError:
            return {
                "success": False,
                "error": "paramiko required for remote fetch. Install with: pip install paramiko",
            }

        # Pre-initialise so static analysers see it is always bound before use.
        remote_file: str = str(PurePosixPath(remote_dir) / f"{pipeline_name}.py")

        ssh = paramiko.SSHClient()
        try:
            _configure_ssh_host_key_policy(
                ssh, strict_host_key_checking, context="fetch_dag"
            )

            connect_kwargs: dict[str, Any] = {
                "hostname": host,
                "username": user,
                "port": port,
                "timeout": 30,
            }
            if key_path:
                connect_kwargs["key_filename"] = key_path
                if key_passphrase:
                    connect_kwargs["passphrase"] = key_passphrase
            else:
                connect_kwargs["password"] = pwd

            ssh.connect(**connect_kwargs)
            sftp = ssh.open_sftp()
            try:
                # Expand ~ in remote_dir the same way _deploy_dags_to_airflow does
                expanded_remote_dir = remote_dir
                if remote_dir.startswith("~/") or remote_dir == "~":
                    try:
                        home_dir = sftp.normalize(".")
                        expanded_remote_dir = (
                            home_dir if remote_dir == "~" else home_dir + remote_dir[1:]
                        )
                        logger.info(
                            "Expanded remote dir: %s -> %s", remote_dir, expanded_remote_dir
                        )
                    except Exception as tilde_exc:
                        logger.warning(
                            "Could not expand ~ in remote dir, using as-is (%s): %s",
                            remote_dir,
                            tilde_exc,
                        )

                remote_file = str(PurePosixPath(expanded_remote_dir) / f"{pipeline_name}.py")
                logger.info("SFTP get: %s -> %s", remote_file, local_path)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                sftp.get(remote_file, str(local_path))
            finally:
                sftp.close()
        finally:
            # Always close the SSH transport, even if connect() itself raised.
            ssh.close()

        logger.info("Fetched remote DAG %s -> %s", remote_file, local_path)
        return {"success": True, "remote_file": remote_file}

    # _fetch_dag_from_remote uses only try/finally blocks for resource cleanup and does
    # not swallow SSH/SFTP errors. Any exception it raises is caught by the caller below,
    # which adds actionable guidance (e.g. known_hosts hint) before returning an error dict.

    async def _update_pipeline_schedule(
        pipeline_name: str,
        new_schedule: str,
        auto_deploy: bool = False,
        wait_for_reload: bool = False,
        strict_host_key_checking: bool = False,
    ) -> dict[str, Any]:
        """
        Update pipeline execution schedule by modifying DAG file and triggering Airflow reload.

        Modifies the schedule or schedule_interval parameter in the DAG Python file for cron-based or preset schedules, preserving all other configuration.
        Airflow automatically detects file changes and reloads the DAG with new schedule.

        Common schedules: @daily, @hourly, @weekly, @monthly, or cron expressions like '0 8 * * *'.
        Perfect for: Adjusting pipeline timing, changing from daily to hourly, updating cron schedules.

        Note:
            When a DAG file contains multiple DAG() constructor calls, only the first
            one in source order has its schedule updated. Files with a single DAG()
            call — the standard Airflow convention — are unaffected by this constraint.

        Args:
            pipeline_name: Pipeline identifier (DAG ID); used to locate the DAG file
                (``<pipeline_name>.py``), not to match the dag_id argument inside the file
            new_schedule: New schedule (cron expression or Airflow preset: @daily, @hourly,
                @weekly, @monthly). Pass the string "None" to disable scheduling.
            auto_deploy: If True, push the DAG to the remote Airflow server via SFTP/SSH
                after the local file is written (default False). When the schedule is
                already correct (no_op), the deploy still runs — this allows force-syncing
                a remote Airflow where the file may have been deleted or overwritten.
            wait_for_reload: If True and auto_deploy is True, wait for Airflow to acknowledge
                the reloaded DAG before returning (default False).
            strict_host_key_checking: Enforce SSH host-key verification when
                fetching the remote DAG file (default False — every connection
                logs a uniform WARNING when False). Set True for production
                after adding the target host to ~/.ssh/known_hosts.

        Returns:
            Dictionary with update results including success status, file path, and deployment instructions
        """
        try:
            import re

            from ..utils.validators import PipelineValidator

            # Validate schedule input before regex operations
            if new_schedule is not None and new_schedule.lower() != "none":
                valid, error = PipelineValidator.validate_schedule(new_schedule)
                if not valid:
                    return {
                        "success": False,
                        "pipeline_name": pipeline_name,
                        "error": f"Invalid schedule: {error}",
                    }

            # Validate pipeline name to prevent path traversal
            valid, error = PipelineValidator.validate_pipeline_name(pipeline_name)
            if not valid:
                return {
                    "success": False,
                    "pipeline_name": pipeline_name,
                    "error": f"Invalid pipeline name: {error}",
                }

            logger.info("Updating schedule for %s to %s", pipeline_name, new_schedule)

            settings = orchestrator.settings
            dags_dir = Path(getattr(settings.pipeline, "dags_output_dir", Path("./airflow_dags")))
            dag_file = dags_dir / f"{pipeline_name}.py"

            # Attempt remote SFTP pull when local file is absent
            fetched_from_remote = False
            if not dag_file.exists():
                try:
                    fetch_result = await _fetch_dag_from_remote(
                        pipeline_name, dag_file, strict_host_key_checking=strict_host_key_checking
                    )
                except Exception as fetch_exc:
                    logger.error(
                        "SFTP fetch for '%s' failed: %s",
                        pipeline_name,
                        fetch_exc,
                        exc_info=True,
                    )
                    exc_str = str(fetch_exc)
                    # Mirror _fetch_dag_from_remote's host resolution: settings first,
                    # then fall back to parsing the Airflow base URL.
                    _ssh_host: str | None = settings.airflow.remote_host
                    if not _ssh_host:
                        try:
                            from urllib.parse import urlparse as _urlparse

                            _base_url = settings.airflow.base_url
                            if _base_url:
                                _ssh_host = _urlparse(_base_url).hostname
                        except Exception as exc:
                            logger.debug(
                                "Could not parse Airflow base URL for known_hosts hint: %s", exc
                            )
                    _ssh_host_str: str = _ssh_host or "<HOST>"
                    known_hosts_hint = (
                        f" The remote host may not be in known_hosts — run: "
                        f"ssh-keyscan {_ssh_host_str} >> ~/.ssh/known_hosts"
                        if "known_hosts" in exc_str or "host key" in exc_str.lower()
                        else ""
                    )
                    return {
                        "success": False,
                        "pipeline_name": pipeline_name,
                        "error": (
                            f"DAG file not found locally ({dag_file}). "
                            f"Remote fetch failed: {fetch_exc}{known_hosts_hint}"
                        ),
                        "suggestion": "Configure AIRFLOW_REMOTE_HOST, AIRFLOW_REMOTE_USER, "
                        "AIRFLOW_REMOTE_SSH_KEY and AIRFLOW_DAG_FOLDER, "
                        "or generate the DAG first.",
                    }
                if not fetch_result["success"]:
                    return {
                        "success": False,
                        "pipeline_name": pipeline_name,
                        "error": (
                            f"DAG file not found locally ({dag_file}). "
                            f"Attempted remote fetch but failed: {fetch_result['error']}"
                        ),
                        "suggestion": "Configure AIRFLOW_REMOTE_HOST, AIRFLOW_REMOTE_USER, "
                        "AIRFLOW_REMOTE_SSH_KEY and AIRFLOW_DAG_FOLDER, "
                        "or generate the DAG first.",
                    }
                fetched_from_remote = True

            # Read DAG file
            content = dag_file.read_text(encoding="utf-8")

            # Idempotency normalizer — defined here, used after schedule extraction.
            def _normalize(s: str | None) -> str:
                """Return a canonical form for schedule comparison.

                Three distinct classes:
                - Python None or string "None"/"none"  → "none"  (disabled schedule)
                - empty / whitespace-only string        → ""      (invalid; never matches)
                - Airflow preset (starts with "@")      → lowercased (@Daily == @daily)
                - any other value (cron, timedelta, TZ) → stripped of surrounding quotes,
                                                          case preserved (timezone names
                                                          and cron fields are case-sensitive)
                """
                if s is None:
                    return "none"
                stripped = s.strip().strip("\"'")
                if not stripped:
                    return ""
                if stripped.lower() == "none":
                    return "none"
                if stripped.startswith("@"):
                    return stripped.lower()
                return stripped

            # ── Schedule extraction and replacement ─────────────────────────────
            # Primary path: AST-based.
            #   • Precise: operates only on the actual DAG() constructor keyword.
            #   • Ignores schedule= occurrences in comments, docstrings, or other
            #     assignments that appear before the DAG() call in the file.
            #   • Handles all valid Python value types (str, None, timedelta, …).
            # Fallback path: regex.
            #   • Used only when the file cannot be parsed (syntax errors, etc.).
            #   • May match the first schedule= occurrence anywhere in the file;
            #     kept solely as a best-effort recovery mechanism.
            import ast as _ast

            old_schedule_value: str | None = None
            updated_content: str = content
            count: int = 0
            ast_success: bool = False

            try:
                tree = _ast.parse(content)
                lines_list = content.splitlines(keepends=True)

                # Collect and sort DAG() calls by source position so we always
                # operate on the first one in the file.  ast.walk() does not
                # guarantee source order, which would be non-deterministic when
                # a file contains multiple DAG() constructor calls.
                dag_calls: list[_ast.Call] = sorted(
                    (
                        node
                        for node in _ast.walk(tree)
                        if isinstance(node, _ast.Call)
                        and (
                            (isinstance(node.func, _ast.Name) and node.func.id == "DAG")
                            or (isinstance(node.func, _ast.Attribute) and node.func.attr == "DAG")
                        )
                    ),
                    key=lambda n: (n.lineno, n.col_offset),
                )

                for ast_node in dag_calls:
                    for kw in ast_node.keywords:
                        if kw.arg is None or kw.arg not in ("schedule", "schedule_interval"):
                            continue

                        # Extract the existing schedule value for idempotency / audit.
                        val = kw.value
                        if isinstance(val, _ast.Constant):
                            raw_old = str(val.value) if val.value is not None else None
                        else:
                            # Complex expression (timedelta, cron_presets, etc.)
                            raw_old = _ast.unparse(val)
                        old_schedule_value = raw_old

                        # Build the replacement source text.
                        if new_schedule is None or new_schedule.lower() == "none":
                            replacement_src = "None"
                        else:
                            replacement_src = f'"{new_schedule}"'

                        # Splice replacement_src into the source at the exact character
                        # range reported by the AST (lineno is 1-indexed; col_offset is 0-indexed).
                        # end_lineno/end_col_offset are None for some synthetic AST nodes;
                        # fall through to the regex path when they are absent.
                        if val.end_lineno is None or val.end_col_offset is None:
                            continue
                        s_line = val.lineno - 1
                        s_col = val.col_offset
                        e_line = val.end_lineno - 1
                        e_col = val.end_col_offset

                        if s_line == e_line:
                            orig = lines_list[s_line]
                            lines_list[s_line] = orig[:s_col] + replacement_src + orig[e_col:]
                        else:
                            lines_list[s_line] = (
                                lines_list[s_line][:s_col]
                                + replacement_src
                                + lines_list[e_line][e_col:]
                            )
                            del lines_list[s_line + 1 : e_line + 1]

                        updated_content = "".join(lines_list)
                        count = 1
                        ast_success = True
                        break  # modify only the first matching DAG() call

                    if ast_success:
                        break

            except SyntaxError as syn_err:
                logger.warning(
                    "DAG file '%s' has syntax errors; falling back to regex for schedule "
                    "replacement (may match schedule= in comments or docstrings): %s",
                    dag_file.name,
                    syn_err,
                )

            # Regex fallback — only runs when the AST could not parse the file.
            # NOTE: may spuriously match schedule= in comments or docstrings.
            if not ast_success:
                # Two-branch pattern:
                #   branch 1 — quoted value:   group 2=quote, group 3=content (non-greedy,
                #              allows commas and all cron chars inside the quotes)
                #   branch 2 — unquoted value: group 4=content (None, @daily, timedelta…)
                # Group 1 (the keyword + "=") is always present and used in the replacement.
                _pattern = (
                    r"(schedule(?:_interval)?\s*=\s*)"
                    r"(?:(['\"])(.*?)\2|([^'\",\)\s]+))"
                )
                m = re.search(_pattern, content)
                if m:
                    raw = (m.group(3) if m.group(2) else m.group(4) or "").strip()
                    old_schedule_value = raw
                if new_schedule is None or new_schedule.lower() == "none":
                    _repl = r"\1None"
                else:
                    # Only backslashes need escaping in regex replacement strings;
                    # do NOT use re.escape() — it escapes *, space, etc. which would
                    # corrupt cron expressions (e.g. "0 8 * * *" → "0\ 8\ \*\ \*\ \*").
                    _safe = new_schedule.replace("\\", "\\\\")
                    _repl = f'\\1"{_safe}"'
                updated_content, count = re.subn(_pattern, _repl, content)

            # ────────────────────────────────────────────────────────────────────
            # Idempotency check — skip write when schedule already matches.
            # NOTE: even on no_op, auto_deploy is still honoured so the caller can
            # force-sync a remote Airflow where the file may be out of date.
            # Guard: only short-circuit when a schedule keyword was actually found
            # (count > 0). Without the guard, a DAG with no schedule parameter leaves
            # old_schedule_value=None; passing new_schedule="None" would make
            # _normalize(None) == _normalize("None") true and return a false no_op.
            if count > 0 and _normalize(old_schedule_value) == _normalize(new_schedule):
                # Schedule is already correct — skip the file write but still honour
                # auto_deploy. The user may be force-syncing a remote that is out of
                # sync (e.g. the DAG file was accidentally deleted on the server).
                no_op_result: dict[str, Any] = {
                    "success": True,
                    "pipeline_name": pipeline_name,
                    "old_schedule": old_schedule_value,
                    "new_schedule": new_schedule,
                    "dag_file": str(dag_file),
                    "fetched_from_remote": fetched_from_remote,
                    "no_op": True,
                    "message": (
                        f"Schedule already set to '{new_schedule}'. "
                        "Local copy downloaded from remote — no schedule changes required."
                        if fetched_from_remote
                        else f"Schedule already set to '{new_schedule}', no file changes made."
                    ),
                }
                if auto_deploy:
                    deploy_result = await _deploy_dags_to_airflow(
                        pipeline_name=pipeline_name,
                        wait_for_dag_loaded=wait_for_reload,
                        max_wait_seconds=180,
                    )
                    no_op_result["auto_deploy"] = {
                        "triggered": True,
                        "success": deploy_result.get("success"),
                        "remote_host": deploy_result.get("remote_host"),
                        "remote_dir": deploy_result.get("remote_dir"),
                        "dag_loaded": deploy_result.get("dag_loaded"),
                        "error": deploy_result.get("error"),
                    }
                    if not deploy_result.get("success"):
                        no_op_result["warning"] = (
                            "Schedule was already correct but auto-deploy failed. "
                            "Run pipeline_deploy(action='deploy_dags', pipeline_name=...) to push to Airflow."
                        )
                else:
                    no_op_result["auto_deploy_skipped"] = True
                    if fetched_from_remote:
                        no_op_result["next_steps"] = [
                            "Schedule is already correct on the remote server.",
                            "A local copy has been downloaded to: " + str(dag_file),
                        ]
                return no_op_result

            if count == 0:
                return {
                    "success": False,
                    "pipeline_name": pipeline_name,
                    "error": "Could not find schedule or schedule_interval parameter in DAG file",
                    "dag_file": str(dag_file),
                    "suggestion": "DAG file may have non-standard format. Update manually or regenerate.",
                }

            # Write updated content
            dag_file.write_text(updated_content, encoding="utf-8")

            default_next_steps: list[str] = []
            if fetched_from_remote and not auto_deploy:
                default_next_steps.append(
                    "Note: DAG was fetched from remote and modified locally — "
                    "deploy is required to sync changes back to Airflow."
                )
            default_next_steps += [
                "Run pipeline_deploy(action='deploy_dags', pipeline_name=...) to push to Airflow.",
                "Verify in Airflow UI that the schedule has been updated.",
            ]

            result: dict[str, Any] = {
                "success": True,
                "pipeline_name": pipeline_name,
                "old_schedule": old_schedule_value,
                "new_schedule": new_schedule,
                "dag_file": str(dag_file),
                "fetched_from_remote": fetched_from_remote,
                "message": (
                    f"Schedule updated in local copy of {pipeline_name}.py — "
                    "deploy required to sync changes to Airflow."
                    if fetched_from_remote
                    else f"Schedule updated successfully. Airflow will auto-reload {pipeline_name}.py"
                ),
                "next_steps": default_next_steps,
            }
            if fetched_from_remote and not auto_deploy:
                result["warning"] = (
                    "DAG was fetched from remote and modified locally — "
                    "deploy is required to sync changes back to Airflow."
                )

            logger.info("Updated schedule for %s to %s", pipeline_name, new_schedule)

            # Auto-deploy to remote Airflow if requested (Change 4)
            if auto_deploy:
                deploy_result = await _deploy_dags_to_airflow(
                    pipeline_name=pipeline_name,
                    wait_for_dag_loaded=wait_for_reload,
                    max_wait_seconds=180,
                )
                result["auto_deploy"] = {
                    "triggered": True,
                    "success": deploy_result.get("success"),
                    "remote_host": deploy_result.get("remote_host"),
                    "remote_dir": deploy_result.get("remote_dir"),
                    "dag_loaded": deploy_result.get("dag_loaded"),
                    "error": deploy_result.get("error"),
                }
                if deploy_result.get("success"):
                    result["next_steps"] = [
                        "DAG file deployed to remote Airflow server automatically.",
                        "Verify in Airflow UI that the new schedule is active.",
                    ]
                    if fetched_from_remote:
                        result["message"] = (
                            f"Schedule updated in {pipeline_name}.py and synced "
                            "to remote Airflow server."
                        )
                else:
                    result["warning"] = (
                        "DAG was fetched from remote, modified locally, but auto-deploy failed — "
                        "run pipeline_deploy(action='deploy_dags', pipeline_name=...) to push the updated schedule to Airflow."
                        if fetched_from_remote
                        else "Schedule updated in local file but auto-deploy failed. "
                        "Run pipeline_deploy(action='deploy_dags', pipeline_name=...) to push to Airflow."
                    )

            return result

        except Exception as e:
            logger.error("Failed to update schedule: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to update schedule. Check server logs for details.",
                "pipeline_name": pipeline_name,
            }

    async def _pause_pipeline(
        pipeline_name: str,
    ) -> dict[str, Any]:
        """
        Airflow: Pause DAG (clear label)

        Pause a pipeline to prevent execution.

        - Action: Sets the Airflow DAG to paused state, preventing
          scheduled and manual runs until resumed.
        - Parameter: `pipeline_name` (DAG ID as string)
        - Example: pause_pipeline(pipeline_name="example_teradata")

        Returns:
            Dictionary with pause operation results: success, pipeline_name, is_paused, message
        """
        try:
            logger.info("Pausing pipeline: %s", pipeline_name)

            await orchestrator.async_airflow_client.pause_dag(dag_id=pipeline_name)

            response = {
                "success": True,
                "pipeline_name": pipeline_name,
                "is_paused": True,
                "message": f"Pipeline {pipeline_name} has been paused",
            }

            logger.info("Paused pipeline: %s", pipeline_name)

            return response

        except Exception as e:
            logger.error("Failed to pause pipeline: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to pause pipeline. Check server logs for details.",
                "pipeline_name": pipeline_name,
            }

    async def _resume_pipeline(
        pipeline_name: str,
    ) -> dict[str, Any]:
        """
        Airflow: Resume DAG (clear label)

        Resume a paused pipeline.

        - Action: Unpauses the Airflow DAG, allowing scheduled and
          manual execution to proceed.
        - Parameter: `pipeline_name` (DAG ID as string)
        - Example: resume_pipeline(pipeline_name="example_teradata")

        Returns:
            Dictionary with resume operation results: success, pipeline_name, is_paused, message
        """
        try:
            logger.info("Resuming pipeline: %s", pipeline_name)

            await orchestrator.async_airflow_client.unpause_dag(dag_id=pipeline_name)

            response = {
                "success": True,
                "pipeline_name": pipeline_name,
                "is_paused": False,
                "message": f"Pipeline {pipeline_name} has been resumed",
            }

            logger.info("Resumed pipeline: %s", pipeline_name)

            return response

        except Exception as e:
            logger.error("Failed to resume pipeline: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to resume pipeline. Check server logs for details.",
                "pipeline_name": pipeline_name,
            }

    async def _delete_pipeline(
        pipeline_name: str,
        delete_dag_file: bool = False,
        delete_dbt_models: bool = False,
        strict_host_key_checking: bool = False,
        remote_host: str | None = None,
        remote_user: str | None = None,
        remote_port: int | None = None,
        ssh_key_path: str | None = None,
        remote_dags_dir: str | None = None,
    ) -> dict[str, Any]:
        """
        Delete pipeline and optionally remove all associated files and artifacts.

        Removes pipeline from Airflow orchestration and optionally cleans up DAG files and dbt models.
        Use this to completely remove a pipeline and free up resources.
        WARNING: Deletion is permanent. Backup important configurations before deleting.

        Perfect for: Removing obsolete pipelines, cleaning up test pipelines, decommissioning data flows.

        Args:
            pipeline_name: Pipeline identifier (DAG ID) to delete
            delete_dag_file: Delete the DAG Python file from local dags directory (default: False for safety)
            delete_dbt_models: Delete associated dbt models from dbt project (default: False for safety)
            strict_host_key_checking: Enforce SSH host-key verification for
                remote SFTP file deletion (default False — every connection
                logs a uniform WARNING when False). Set True for production
                after adding the target host to ~/.ssh/known_hosts.
            remote_host: SSH host for remote DAG file deletion (falls back to AIRFLOW_REMOTE_HOST
                or hostname from AIRFLOW_BASE_URL).
            remote_user: SSH username (falls back to AIRFLOW_REMOTE_USER).
            remote_port: SSH port (falls back to AIRFLOW_REMOTE_PORT, default 22).
            ssh_key_path: Path to SSH private key (falls back to AIRFLOW_REMOTE_SSH_KEY).
            remote_dags_dir: Remote DAGs directory (falls back to AIRFLOW_DAG_FOLDER).

        Returns:
            Dictionary with deletion results including what was deleted and any warnings
        """
        try:
            from ..utils.validators import PipelineValidator

            # Validate pipeline name to prevent path traversal / injection
            valid, error = PipelineValidator.validate_pipeline_name(pipeline_name)
            if not valid:
                return {
                    "success": False,
                    "error": f"Invalid pipeline name: {error}",
                    "pipeline_name": pipeline_name,
                }

            logger.info("Deleting pipeline: %s", pipeline_name)

            results = {
                "success": False,
                "pipeline_name": pipeline_name,
                "deleted_components": [],
                "warnings": [],
                "deleted_files": [],
            }

            # Delete DAG from Airflow
            try:
                await orchestrator.async_airflow_client.delete_dag(dag_id=pipeline_name)
                results["deleted_components"].append("airflow_dag_metadata")
                logger.info("Deleted Airflow DAG metadata for %s", pipeline_name)
            except Exception as e:
                error_str = str(e).lower()
                if "404" in error_str or "not found" in error_str:
                    results["deleted_components"].append("airflow_dag_metadata")
                    logger.info(
                        "DAG metadata for %s already removed (404). Treating as success.",
                        pipeline_name,
                    )
                else:
                    logger.error(
                        "Failed to delete Airflow DAG metadata for %s: %s",
                        pipeline_name,
                        e,
                        exc_info=True,
                    )
                    results["warnings"].append(
                        "Failed to delete Airflow DAG metadata. Check server logs."
                    )

            # Delete DAG file if requested
            if delete_dag_file:
                try:
                    settings = orchestrator.settings
                    dags_dir = Path(
                        getattr(settings.pipeline, "dags_output_dir", Path("./airflow_dags"))
                    )
                    dag_file = dags_dir / f"{pipeline_name}.py"

                    if dag_file.exists():
                        dag_file.unlink()
                        results["deleted_components"].append("dag_file")
                        results["deleted_files"].append(str(dag_file))
                        logger.info("Deleted DAG file: %s", dag_file)
                    else:
                        results["warnings"].append(f"DAG file not found: {dag_file}")
                except Exception as e:
                    logger.error(
                        "Failed to delete DAG file for %s: %s", pipeline_name, e, exc_info=True
                    )
                    results["warnings"].append("Failed to delete DAG file. Check server logs.")

            # Delete dbt models if requested
            if delete_dbt_models:
                try:
                    settings = orchestrator.settings
                    # ``settings.dbt.project_dir`` is the parent container of
                    # per-Teradata-profile sub-projects. Each sub-project lives
                    # at ``parent/dbt_<name>/`` with its own ``models/`` tree.
                    # Walk every sub-project so a pipeline whose models live in
                    # any of them is reachable.
                    dbt_project_parent = Path(
                        getattr(settings.dbt, "project_dir", Path("./dbt_project"))
                    )

                    # Look for models related to this pipeline
                    # Exact match or standard dbt layer prefix patterns only
                    deleted_model_count = 0
                    name_lower = pipeline_name.lower()

                    def _matches_pipeline(stem: str) -> bool:
                        """Check if file stem matches pipeline name exactly or by dbt convention."""
                        s = stem.lower()
                        return (
                            s == name_lower
                            or s.startswith(f"{name_lower}_")
                            or s.startswith(f"stg_{name_lower}")
                            or s.startswith(f"int_{name_lower}")
                        )

                    sub_projects = (
                        [p for p in dbt_project_parent.iterdir() if p.is_dir() and p.name.startswith("dbt_")]
                        if dbt_project_parent.exists()
                        else []
                    )
                    for sub in sub_projects:
                        for models_subdir in ["staging", "intermediate", "marts"]:
                            models_dir = sub / "models" / models_subdir
                            if not models_dir.exists():
                                continue

                            # Find files matching pipeline name pattern (exact/prefix only)
                            for model_file in models_dir.rglob("*.sql"):
                                if _matches_pipeline(model_file.stem):
                                    model_file.unlink()
                                    results["deleted_files"].append(str(model_file))
                                    deleted_model_count += 1
                                    logger.info("Deleted dbt model: %s", model_file)

                            # Also check for YAML files
                            for yaml_file in models_dir.rglob("*.yml"):
                                if _matches_pipeline(yaml_file.stem):
                                    yaml_file.unlink()
                                    results["deleted_files"].append(str(yaml_file))
                                    deleted_model_count += 1
                                    logger.info("Deleted dbt YAML: %s", yaml_file)

                    if deleted_model_count > 0:
                        results["deleted_components"].append(
                            f"dbt_models ({deleted_model_count} files)"
                        )
                    else:
                        results["warnings"].append(
                            f"No dbt models found matching '{pipeline_name}' pattern"
                        )

                except Exception as e:
                    logger.error(
                        "Failed to delete dbt models for %s: %s", pipeline_name, e, exc_info=True
                    )
                    results["warnings"].append("Failed to delete dbt models. Check server logs.")

            remote_dag_folder = (
                remote_dags_dir
                or getattr(orchestrator.settings.airflow, "dag_folder", "/opt/airflow/dags")
            )
            remote_dag_path = str(
                PurePosixPath(remote_dag_folder) / f"{pipeline_name}.py"
            )
            results["remote_dag_file"] = remote_dag_path

            _host = remote_host or orchestrator.settings.airflow.remote_host
            if not _host:
                try:
                    from urllib.parse import urlparse as _urlparse

                    base_url = orchestrator.settings.airflow.base_url
                    if base_url:
                        _host = _urlparse(base_url).hostname
                except Exception:
                    _host = None
            _user = remote_user or orchestrator.settings.airflow.remote_user
            _key = ssh_key_path or orchestrator.settings.airflow.remote_ssh_key
            _port = (
                remote_port
                if remote_port is not None
                else orchestrator.settings.airflow.remote_port
            )
            pwd = (
                orchestrator.settings.airflow.remote_password.get_secret_value()
                if orchestrator.settings.airflow.remote_password
                else None
            )
            has_credentials = bool(_key or pwd)

            if _host and _user and has_credentials:
                ssh = None
                try:
                    import paramiko

                    ssh = paramiko.SSHClient()
                    _configure_ssh_host_key_policy(
                        ssh, strict_host_key_checking, context="delete_pipeline"
                    )
                    connect_kwargs: dict[str, Any] = {
                        "hostname": _host,
                        "username": _user,
                        "port": _port,
                        "timeout": 30,
                        "allow_agent": False,
                        "look_for_keys": False,
                    }
                    if _key:
                        resolved_key = str(Path(_key).expanduser().resolve())
                        if Path(resolved_key).exists():
                            connect_kwargs["key_filename"] = resolved_key
                            passphrase = orchestrator.settings.airflow.remote_ssh_key_passphrase
                            if passphrase:
                                connect_kwargs["passphrase"] = passphrase.get_secret_value()
                        elif pwd:
                            logger.warning(
                                "SSH key file not found: %s — falling back to password auth",
                                resolved_key,
                            )
                            connect_kwargs["password"] = pwd
                        else:
                            results["warnings"].append(
                                f"SSH key file not found ({resolved_key}) and no password "
                                f"configured — cannot delete remote DAG file."
                            )
                            raise FileNotFoundError(f"SSH key not found: {resolved_key}")
                    else:
                        connect_kwargs["password"] = pwd

                    ssh.connect(**connect_kwargs)
                    sftp = ssh.open_sftp()
                    try:
                        expanded_dag_path = remote_dag_path
                        if remote_dag_folder.startswith("~/") or remote_dag_folder == "~":
                            try:
                                home_dir = sftp.normalize(".")
                                if remote_dag_folder == "~":
                                    expanded_dag_path = f"{home_dir}/{pipeline_name}.py"
                                else:
                                    expanded_dag_path = (
                                        f"{home_dir}{remote_dag_folder[1:]}/{pipeline_name}.py"
                                    )
                            except Exception:
                                logger.warning("Could not expand ~ in remote path")
                        sftp.remove(expanded_dag_path)
                        results["deleted_components"].append("remote_dag_file")
                        results["deleted_files"].append(expanded_dag_path)
                        logger.info("Deleted remote DAG file: %s", expanded_dag_path)
                    except (FileNotFoundError, OSError) as rm_err:
                        import errno as _errno

                        if isinstance(rm_err, FileNotFoundError) or getattr(rm_err, "errno", None) == _errno.ENOENT:
                            results["warnings"].append(
                                f"Remote DAG file not found: {expanded_dag_path}"
                            )
                        else:
                            raise
                    finally:
                        try:
                            sftp.close()
                        except Exception:
                            pass
                except ImportError:
                    results["warnings"].append(
                        "paramiko not installed — cannot remove remote DAG file via SFTP. "
                        f"Manually delete: {remote_dag_path}"
                    )
                except Exception as ssh_err:
                    logger.error(
                        "Failed to delete remote DAG file %s: %s",
                        remote_dag_path, ssh_err, exc_info=True,
                    )
                    results["warnings"].append(
                        f"Failed to delete remote DAG file via SFTP: {remote_dag_path}. "
                        f"Manually delete the file to prevent DAG re-registration."
                    )
                finally:
                    if ssh:
                        try:
                            ssh.close()
                        except Exception:
                            pass
            elif _host and _user:
                results["warnings"].append(
                    f"SSH credentials not configured (AIRFLOW_REMOTE_SSH_KEY or "
                    f"AIRFLOW_REMOTE_PASSWORD) — cannot remove remote DAG file: "
                    f"{remote_dag_path}."
                )
            else:
                results["warnings"].append(
                    f"SSH not configured (AIRFLOW_REMOTE_HOST/AIRFLOW_REMOTE_USER) — "
                    f"cannot remove remote DAG file: {remote_dag_path}. "
                    f"Airflow's scheduler will re-register the DAG from this file."
                )

            results["success"] = len(results["deleted_components"]) > 0

            results["message"] = (
                f"Pipeline '{pipeline_name}' deletion completed. "
                f"Components removed: {', '.join(results['deleted_components'])}"
            )

            if len(results["deleted_files"]) > 0:
                results["message"] += f" ({len(results['deleted_files'])} files deleted)"

            if results["warnings"]:
                results["message"] += (
                    f". WARNING: {len(results['warnings'])} issue(s) — "
                    f"see 'warnings' field for details."
                )

            logger.info("Deleted pipeline: %s", pipeline_name)
            return results

        except Exception as e:
            logger.error("Failed to delete pipeline: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to delete pipeline. Check server logs for details.",
                "pipeline_name": pipeline_name,
            }

    async def _validate_pipeline_configuration(
        pipeline_config: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Validate pipeline configuration before creation (source-agnostic).

        This pre-flight check is designed for ELT pipelines that move data
        from any supported source into Teradata, orchestrated by Airflow,
        with optional transformations (e.g., dbt). It avoids assuming
        Teradata is the source and only validates source specifics when
        the relevant fields are provided.

        Performs:
        - Connections: Teradata (destination), Airflow (required), Airbyte (if used), dbt (if used)
        - Required fields: minimal (pipeline_name)
            - Source-specific checks (optional):
                - airbyte: confirm service availability; optional IDs presence
                - csv/file/csv_file/tpt_file: orchestrator verifies input_file_path is
                  provided and the file exists (singular path, not a list)
        - Schedule: presence noted; format validation not implemented

        Args:
                        pipeline_config: Flexible dictionary. Recommended keys:
                            - pipeline_name: required
                            - source_type: one of ["airbyte", "csv", "csv_file", "file",
                              "tpt_file"] (optional)
                            - For airbyte: connection_id/source_id/destination_id (optional)
                            - For all file-based sources (csv/csv_file/file/tpt_file):
                              input_file_path: str. When source_type is explicitly set,
                              validation fails with an explicit error if this is missing or
                              empty. When source_type is omitted, a non-empty value is needed
                              to trigger file validation (empty/None skips file checks).
                              Do not use "files: List[str]".
                            - target_schema: Teradata destination schema (optional)
                            - schedule: cron or preset string (optional)

        Returns:
            Dictionary with validation results
        """
        try:
            logger.info("Validating pipeline configuration")

            results = {
                "valid": True,
                "checks": {},
                "errors": [],
                "warnings": [],
            }

            # Validate connections (destination/orchestration readiness)
            # Pass pipeline_config to enable source-aware checks (Airbyte vs TPT)
            validation = await orchestrator.async_validate_pipeline_configuration(pipeline_config)
            results["checks"]["connections"] = validation["checks"]

            if not validation["valid"]:
                results["valid"] = False
                results["errors"].extend(validation["errors"])

            # Minimal required field(s) for a generic ELT pipeline.
            # Check both key presence and non-empty value so that pipeline_name=""
            # is rejected here rather than silently bypassing downstream validation.
            required_fields = ["pipeline_name"]
            for field in required_fields:
                value = pipeline_config.get(field)
                if value is None:
                    results["valid"] = False
                    results["errors"].append(f"Missing required field: {field}")
                elif not isinstance(value, str):
                    results["valid"] = False
                    results["errors"].append(
                        f"{field} must be a string, got {type(value).__name__}"
                    )
                elif not value.strip():
                    results["valid"] = False
                    results["errors"].append(f"{field} cannot be blank")

            # Reject the deprecated "files" list field so callers get a clear error
            # instead of silently bypassing input_file_path validation.
            if "files" in pipeline_config:
                results["valid"] = False
                results["errors"].append(
                    "Deprecated field 'files' is not supported; "
                    "use 'input_file_path' (str) instead."
                )

            # Determine source_type (optional) and run appropriate checks.
            # File inference mirrors the orchestrator exactly (truthy input_file_path → "file").
            # Airbyte inference includes source_connector (the orchestrator's key) plus
            # common Airbyte ID fields that callers may supply without source_connector.
            source_type = (pipeline_config.get("source_type") or "").lower()
            if not source_type:
                if pipeline_config.get("input_file_path"):
                    source_type = "file"
                elif any(
                    pipeline_config.get(k)
                    for k in [
                        "source_connector",
                        "connection_id",
                        "source_id",
                        "destination_id",
                        "airbyte",
                    ]
                ):
                    source_type = "airbyte"
                else:
                    source_type = "unknown"
            results["checks"]["source_type"] = source_type

            csv_like_sources = {"file", "csv", "csv_file", "tpt_file"}
            known_sources = {"airbyte"} | csv_like_sources

            # Source-specific validations (optional)
            if source_type == "airbyte":
                # Airbyte connectivity was already assessed in connections; record IDs if present
                for key in ("connection_id", "source_id", "destination_id"):
                    if key in pipeline_config:
                        results["checks"][f"airbyte_{key}"] = "provided"
                if validation["checks"].get("airbyte", "UNAVAILABLE").startswith("UNAVAILABLE"):
                    results["warnings"].append(
                        "Airbyte service unavailable; skip source validation"
                    )
            # csv_like_sources: input_file_path presence and existence are validated by the
            # orchestrator (async_validate_pipeline_configuration) and surfaced in
            # results["checks"]["connections"]["input_file"]. Nothing additional here.
            elif source_type not in known_sources:
                results["warnings"].append(
                    "Unknown or unspecified source_type; skipping source validation"
                )

            # Validate schedule format
            if "schedule" in pipeline_config:
                results["checks"]["schedule_format"] = "NOT_VALIDATED"
                results["warnings"].append("Schedule format validation not implemented")

            # Destination schema presence
            if pipeline_config.get("target_schema"):
                results["checks"]["target_schema"] = "provided"
            else:
                results["warnings"].append(
                    "Target schema not provided; dbt/Teradata mapping may require it"
                )

            # DAG syntax check — detects Python syntax errors in the generated DAG file.
            # validate_dag_file catches Python syntax errors and bare-name expression errors;
            # other runtime errors are not caught.
            _raw_name = pipeline_config.get("pipeline_name")
            dag_pipeline_name = _raw_name.strip() if isinstance(_raw_name, str) else None
            if dag_pipeline_name:  # blank/None/non-str already caught by required-field check above
                from ..utils.validators import PipelineValidator

                name_valid, name_error = PipelineValidator.validate_pipeline_name(dag_pipeline_name)
                if not name_valid:
                    results["valid"] = False
                    results["errors"].append(f"Invalid pipeline name: {name_error}")
                else:
                    settings = orchestrator.settings
                    dags_dir = Path(
                        getattr(settings.pipeline, "dags_output_dir", Path("./airflow_dags"))
                    )
                    try:
                        dag_file = safe_join_within(
                            dags_dir.resolve(), f"{dag_pipeline_name}.py"
                        )
                    except UnsafePathError as e:
                        results["valid"] = False
                        results["errors"].append(
                            f"pipeline_name resolves outside the configured DAG directory: {e}"
                        )
                        dag_file = None
                    if dag_file is not None and dag_file.exists():
                        dag_syntax = orchestrator.airflow_dag_generator.validate_dag_file(dag_file)
                        if dag_syntax["valid"]:
                            results["checks"]["dag_syntax"] = "OK"
                        else:
                            results["checks"]["dag_syntax"] = "SYNTAX_ERROR"
                            results["valid"] = False
                            results["errors"].append(
                                f"DAG file syntax error: {dag_syntax['syntax_error']}"
                            )
                    else:
                        results["checks"]["dag_syntax"] = "NOT_FOUND"

            logger.info("Validation complete: valid=%s", results["valid"])

            return results

        except Exception as e:
            logger.error("Failed to validate configuration: %s", e, exc_info=True)
            return {
                "valid": False,
                "error": "Failed to validate configuration. Check server logs for details.",
                "checks": {},
                "errors": ["Failed to validate configuration. Check server logs for details."],
            }

    async def _deploy_complete_pipeline(
        pipeline_name: str,
        dag_file: str | None = None,
        tpt_dir: str = "tpt_scripts",
        bteq_dir: str = "bteq_scripts",
        dbt_dir: str = "dbt_project",
        csv_files: list[str] | None = None,
        remote_base: str | None = None,
        strict_host_key_checking: bool = False,
    ) -> dict[str, Any]:
        """
        Deploy complete pipeline to Airflow server.

        Deploys all pipeline components including DAG, TPT scripts,
        BTEQ scripts, dbt project, and CSV source files to the remote Airflow server.
        Also updates TPT scripts to reference the remote CSV file paths.

        Args:
            pipeline_name: Name of the pipeline (used to locate DAG file)
            dag_file: Optional path to DAG file (defaults to airflow_dags/{pipeline_name}.py)
            tpt_dir: Local directory containing TPT scripts
            bteq_dir: Local directory containing BTEQ scripts
            dbt_dir: Local directory containing dbt project
            csv_files: Optional list of CSV file paths to deploy (auto-detected from TPT scripts if not provided)
            remote_base: Optional base path on remote server (defaults to /opt/airflow)
            strict_host_key_checking: Enforce SSH host-key verification on the
                paramiko transfer step (default False). Previously this path
                hardcoded permissive policy regardless of the caller's value;
                with this flag honored, set True only after adding the target
                host to ~/.ssh/known_hosts.

        Returns:
            Dictionary with deployment results for all components
        """
        import re
        import shutil

        try:
            import paramiko  # noqa: F401
            from paramiko import SSHClient

            has_paramiko = True
        except ImportError:
            has_paramiko = False

        try:
            logger.info("Deploying complete pipeline: %s", pipeline_name)

            results = {
                "success": True,
                "pipeline_name": pipeline_name,
                "deployed_components": [],
                "failed_components": [],
                "deployment_details": {},
            }

            # Get configuration
            remote_host = orchestrator.settings.airflow.remote_host
            remote_user = orchestrator.settings.airflow.remote_user
            remote_password = orchestrator.settings.airflow.remote_password
            remote_base = remote_base or "/opt/airflow"

            # Determine DAG file path
            if not dag_file:
                dag_file = f"airflow_dags/{pipeline_name}.py"

            dag_path = Path(dag_file)
            if not dag_path.exists():
                results["failed_components"].append("dag")
                results["deployment_details"]["dag"] = f"File not found: {dag_file}"

            # Extract CSV file paths from TPT scripts if not provided
            if csv_files is None:
                csv_files = []
                tpt_path = Path(tpt_dir)
                if tpt_path.exists():
                    for tpt_file in tpt_path.glob("*.tpt"):
                        try:
                            content = tpt_file.read_text()
                            # Extract DirectoryPath and FileName from TPT script
                            dir_match = re.search(
                                r"VARCHAR\s+DirectoryPath\s*=\s*'([^']+)'", content
                            )
                            file_match = re.search(r"VARCHAR\s+FileName\s*=\s*'([^']+)'", content)

                            if dir_match and file_match:
                                csv_path = Path(dir_match.group(1)) / file_match.group(1)
                                if csv_path.exists():
                                    csv_files.append(str(csv_path))
                                    logger.info("Detected CSV file from TPT: %s", csv_path)
                        except Exception as e:
                            logger.warning("Failed to parse TPT file %s: %s", tpt_file, e)

            # Helper function to update TPT scripts with remote paths
            def update_tpt_remote_paths(tpt_content: str, remote_data_path: str) -> str:
                """Update DirectoryPath in TPT script to point to remote location."""
                # Replace DirectoryPath with remote path
                updated = re.sub(
                    r"(VARCHAR\s+DirectoryPath\s*=\s*)'[^']+'",
                    rf"\1'{remote_data_path}'",
                    tpt_content,
                )
                return updated

            # Helper function to copy files using paramiko (for password auth)
            async def paramiko_copy(local_path: Path, remote_path: str, is_directory: bool = False):
                """Copy files using paramiko SSH/SFTP with password authentication."""
                if not has_paramiko or not remote_password:
                    return False

                try:

                    def _transfer():
                        ssh = SSHClient()
                        _configure_ssh_host_key_policy(
                            ssh,
                            strict_host_key_checking,
                            context="deploy_complete_pipeline",
                        )
                        if strict_host_key_checking:
                            # Hedge: this path previously hardcoded AutoAddPolicy
                            # and ignored the caller's strict flag. Surface
                            # that behavior change in logs so an operator who
                            # sees a connection failure here knows the cause.
                            logger.info(
                                "Enforcing SSH host-key verification in "
                                "deploy_complete_pipeline transfer path. This path "
                                "previously ignored strict_host_key_checking; if "
                                "connection fails, add the host to "
                                "~/.ssh/known_hosts."
                            )
                        ssh.connect(
                            remote_host,
                            username=remote_user,
                            password=remote_password.get_secret_value(),
                            timeout=30,
                            look_for_keys=False,  # Don't look for keys
                            allow_agent=False,  # Don't use SSH agent
                        )
                        try:
                            # Create remote directory
                            ssh.exec_command(f"mkdir -p {shlex.quote(remote_path)}", timeout=15)  # nosec B601

                            sftp = ssh.open_sftp()
                            try:
                                if is_directory:
                                    # Copy all files in directory
                                    for item in local_path.rglob("*"):
                                        if item.is_file():
                                            rel_path = item.relative_to(local_path)
                                            remote_file = f"{remote_path}/{rel_path}".replace(
                                                "\\", "/"
                                            )

                                            # Create remote subdirectories if needed
                                            remote_dir = "/".join(remote_file.split("/")[:-1])
                                            ssh.exec_command(
                                                f"mkdir -p {shlex.quote(remote_dir)}", timeout=15
                                            )  # nosec B601

                                            sftp.put(str(item), remote_file)
                                else:
                                    # Copy single file
                                    remote_file = f"{remote_path}/{local_path.name}"
                                    sftp.put(str(local_path), remote_file)
                            finally:
                                sftp.close()
                        finally:
                            ssh.close()
                        return True

                    return await asyncio.to_thread(_transfer)

                except Exception as e:
                    logger.warning("Paramiko transfer failed: %s", e)
                    return False

            # Helper function to deploy directory
            async def deploy_directory(
                local_dir: str, remote_subdir: str, component_name: str, process_tpt: bool = False
            ):
                local_path = Path(local_dir)
                if not local_path.exists():
                    logger.warning("%s directory not found: %s", component_name, local_dir)
                    results["failed_components"].append(component_name)
                    results["deployment_details"][component_name] = (
                        f"Directory not found: {local_dir}"
                    )
                    return

                remote_path = f"{remote_base}/{remote_subdir}"
                remote_data_path = f"{remote_base}/data"

                if remote_host and remote_user:
                    try:
                        # Process TPT files to update paths before copying
                        temp_dir = None
                        if process_tpt:
                            temp_dir = Path(f"{local_dir}_temp")
                            temp_dir.mkdir(exist_ok=True)

                            try:
                                for tpt_file in local_path.glob("*.tpt"):
                                    content = tpt_file.read_text()
                                    updated_content = update_tpt_remote_paths(
                                        content, remote_data_path
                                    )
                                    temp_file = temp_dir / tpt_file.name
                                    temp_file.write_text(updated_content)

                                local_path = temp_dir
                            except Exception as e:
                                logger.warning("Failed to update TPT paths: %s", e)
                                if temp_dir and temp_dir.exists():
                                    shutil.rmtree(temp_dir)
                                    temp_dir = None

                        # Use paramiko for deployment (cross-platform)
                        success = False
                        if remote_password and has_paramiko:
                            success = await paramiko_copy(
                                local_path, remote_path, is_directory=True
                            )
                        else:
                            logger.error("Paramiko not available or password not configured")

                        if not success:
                            results["failed_components"].append(component_name)
                            results["deployment_details"][component_name] = (
                                "Failed: Paramiko deployment failed"
                            )

                        # Clean up temp directory
                        if temp_dir and temp_dir.exists():
                            shutil.rmtree(temp_dir)

                        if success:
                            results["deployed_components"].append(component_name)
                            results["deployment_details"][component_name] = (
                                f"Deployed to {remote_path}"
                            )

                    except Exception as e:
                        logger.error("Failed to deploy %s: %s", component_name, e, exc_info=True)
                        results["failed_components"].append(component_name)
                        results["deployment_details"][component_name] = (
                            "Error: deployment failed. Check server logs."
                        )

                else:
                    # Local deployment
                    try:
                        target_dir = Path(remote_path)
                        target_dir.mkdir(parents=True, exist_ok=True)

                        for item in local_path.rglob("*"):
                            if item.is_file():
                                content = None

                                # Process TPT files
                                if process_tpt and item.suffix == ".tpt":
                                    content = item.read_text()
                                    content = update_tpt_remote_paths(content, remote_data_path)

                                rel_path = item.relative_to(local_path)
                                target_file = target_dir / rel_path
                                target_file.parent.mkdir(parents=True, exist_ok=True)

                                if content:
                                    target_file.write_text(content)
                                else:
                                    await asyncio.to_thread(shutil.copy2, item, target_file)

                        results["deployed_components"].append(component_name)
                        results["deployment_details"][component_name] = f"Copied to {remote_path}"

                    except Exception as e:
                        logger.error("Failed to deploy %s: %s", component_name, e, exc_info=True)
                        results["failed_components"].append(component_name)
                        results["deployment_details"][component_name] = (
                            "Error: deployment failed. Check server logs."
                        )

            # Helper function to deploy individual files (like CSV)
            async def deploy_files(file_paths: list[str], remote_subdir: str, component_name: str):
                if not file_paths:
                    return

                remote_path = f"{remote_base}/{remote_subdir}"

                if remote_host and remote_user:
                    try:
                        # Copy each file
                        success_count = 0
                        for file_path in file_paths:
                            file_obj = Path(file_path)
                            if not file_obj.exists():
                                logger.warning("File not found: %s", file_path)
                                continue

                            # Use paramiko for deployment (cross-platform)
                            file_success = False
                            if remote_password and has_paramiko:
                                file_success = await paramiko_copy(
                                    file_obj, remote_path, is_directory=False
                                )
                            else:
                                logger.warning(
                                    "Paramiko not available or password not configured for %s",
                                    file_path,
                                )

                            if file_success:
                                success_count += 1

                        if success_count > 0:
                            results["deployed_components"].append(component_name)
                            results["deployment_details"][component_name] = (
                                f"Deployed {success_count}/{len(file_paths)} file(s) to {remote_path}"
                            )
                        else:
                            results["failed_components"].append(component_name)
                            results["deployment_details"][component_name] = (
                                f"Failed to deploy files to {remote_path}"
                            )

                    except Exception as e:
                        logger.error("Failed to deploy %s: %s", component_name, e, exc_info=True)
                        results["failed_components"].append(component_name)
                        results["deployment_details"][component_name] = (
                            "Error: deployment failed. Check server logs."
                        )
                else:
                    # Local deployment
                    try:
                        target_dir = Path(remote_path)
                        target_dir.mkdir(parents=True, exist_ok=True)

                        for file_path in file_paths:
                            file_obj = Path(file_path)
                            if file_obj.exists():
                                target_file = target_dir / file_obj.name
                                await asyncio.to_thread(shutil.copy2, file_obj, target_file)

                        results["deployed_components"].append(component_name)
                        results["deployment_details"][component_name] = (
                            f"Copied {len(file_paths)} file(s) to {remote_path}"
                        )

                    except Exception as e:
                        logger.error("Failed to deploy %s: %s", component_name, e, exc_info=True)
                        results["failed_components"].append(component_name)
                        results["deployment_details"][component_name] = (
                            "Error: deployment failed. Check server logs."
                        )

            # Deploy CSV source files first
            if csv_files:
                await deploy_files(csv_files, "data", "csv_data")

            # Deploy DAG file
            if dag_path.exists():
                # Inline DAG deployment
                try:
                    if remote_host and remote_user:
                        dag_target = orchestrator.settings.airflow.dag_folder
                        # Use paramiko for deployment (cross-platform)
                        success = False
                        if remote_password and has_paramiko:
                            success = await paramiko_copy(dag_path, dag_target, is_directory=False)
                        else:
                            logger.error("Paramiko not available or password not configured")

                        if not success:
                            results["failed_components"].append("dag")
                            results["deployment_details"]["dag"] = (
                                "Failed: Paramiko deployment failed"
                            )
                        else:
                            results["deployed_components"].append("dag")
                            results["deployment_details"]["dag"] = f"Deployed to {dag_target}"
                    else:
                        # Local deployment — write to the pipeline output dir, not the
                        # remote dag_folder (which defaults to /opt/airflow/dags and
                        # may not exist or be writable on a dev machine).
                        target_dir = Path(orchestrator.settings.pipeline.dags_output_dir)
                        target_dir.mkdir(parents=True, exist_ok=True)
                        target_file = target_dir / dag_path.name
                        await asyncio.to_thread(shutil.copy2, dag_path, target_file)
                        results["deployed_components"].append("dag")
                        results["deployment_details"]["dag"] = f"Copied to {target_file}"

                except Exception as e:
                    logger.error("Failed to deploy dag: %s", e, exc_info=True)
                    results["failed_components"].append("dag")
                    results["deployment_details"]["dag"] = (
                        "Error: deployment failed. Check server logs."
                    )

            # Deploy TPT scripts (with path updates)
            await deploy_directory(tpt_dir, "tpt_scripts", "tpt", process_tpt=True)

            # Deploy BTEQ scripts
            await deploy_directory(bteq_dir, "bteq_scripts", "bteq")

            # Deploy dbt project
            await deploy_directory(dbt_dir, "dbt_project", "dbt")

            # Set overall success
            results["success"] = len(results["failed_components"]) == 0

            if results["success"]:
                results["message"] = (
                    f"Complete pipeline deployed successfully: {', '.join(results['deployed_components'])}"
                )
            else:
                results["message"] = (
                    f"Partial deployment. Failed: {', '.join(results['failed_components'])}"
                )

            logger.info("Pipeline deployment complete: %s", results["message"])

            return results

        except Exception as e:
            logger.error("Failed to deploy complete pipeline: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to deploy complete pipeline. Check server logs for details.",
                "pipeline_name": pipeline_name,
            }

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 1: pipeline_status
    # ══════════════════════════════════════════════════════════════

    async def pipeline_status(
        action: Literal["get_status", "list_pipelines", "check_dag_exists"],
        pipeline_name: str | None = None,
        dag_id: str | None = None,
        include_run_history: bool = True,
        include_paused: bool = True,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """Query Airflow pipeline status, list pipelines, or check DAG existence.

        Args:
            action: One of:
                - "get_status"      — Get current status and details of a pipeline.
                - "list_pipelines"  — List all Airflow pipelines (DAGs) with filtering.
                - "check_dag_exists" — Check if a DAG exists in Airflow (no polling).
            pipeline_name: Pipeline identifier (DAG ID). Required for get_status.
            dag_id: DAG identifier. Required for check_dag_exists.
            include_run_history: Include recent run history in get_status (default True).
            include_paused: Include paused pipelines in list_pipelines (default True).
                Inactive DAGs (whose DAG file no longer exists) are always excluded.
            tags: Filter pipelines by tags in list_pipelines.

        Returns:
            Dictionary with pipeline status or listing results.
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        action = action.strip().lower()
        if pipeline_name:
            err = validate_identifier(pipeline_name, "pipeline_name")
            if err:
                return {"success": False, "error": err}
        try:
            if action == "get_status":
                if not pipeline_name:
                    return {
                        "success": False,
                        "error": "Parameter 'pipeline_name' is required for get_status.",
                    }
                return await _get_pipeline_status(pipeline_name, include_run_history)
            elif action == "list_pipelines":
                return await _list_pipelines(include_paused, tags)
            elif action == "check_dag_exists":
                _dag_id = dag_id or pipeline_name
                if not _dag_id:
                    return {
                        "success": False,
                        "error": "Parameter 'dag_id' or 'pipeline_name' is required for check_dag_exists.",
                    }
                return await _check_dag_exists(_dag_id)
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown action '{action}'. "
                        "Valid actions: get_status, list_pipelines, check_dag_exists"
                    ),
                }
        except Exception as e:
            logger.error("pipeline_status(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 2: pipeline_control
    # ══════════════════════════════════════════════════════════════

    async def pipeline_control(
        action: Literal["update_schedule", "pause", "resume", "delete"],
        pipeline_name: str,
        new_schedule: str | None = None,
        delete_dag_file: bool = False,
        delete_dbt_models: bool = False,
        auto_deploy: bool = False,
        wait_for_reload: bool = False,
        strict_host_key_checking: bool = False,
        confirm: bool = False,
        remote_host: str | None = None,
        remote_user: str | None = None,
        remote_port: int | None = None,
        ssh_key_path: str | None = None,
        remote_dags_dir: str | None = None,
    ) -> dict[str, Any]:
        """Control Airflow pipeline lifecycle — update schedule, pause, resume, or delete.

        Args:
            action: One of:
                - "update_schedule" — Update pipeline execution schedule.
                - "pause"           — Pause a pipeline to prevent execution.
                - "resume"          — Resume a paused pipeline.
                - "delete"          — Delete pipeline and optionally remove files.
            pipeline_name: Pipeline identifier (DAG ID). Required for all actions.
            new_schedule: New schedule (cron expression or Airflow preset: @daily, @hourly,
                @weekly, @monthly). Pass the string "None" to disable scheduling. Required
                for update_schedule; Python None (omitting the parameter) is rejected.
            delete_dag_file: Delete the DAG file when deleting (default False).
            delete_dbt_models: Delete associated dbt models when deleting (default False).
            auto_deploy: Automatically deploy the updated DAG to the remote Airflow server
                after a successful schedule update (default False). Requires SSH credentials.
            wait_for_reload: Wait for Airflow to acknowledge the reloaded DAG after
                auto-deploy (default False). Only relevant when auto_deploy=True.
            strict_host_key_checking: Enforce SSH host-key verification for
                update_schedule (remote DAG fetch) and delete (remote DAG file
                removal via SFTP). Default False — every connection logs a
                uniform WARNING when False. Set True for production after
                adding the target host to ~/.ssh/known_hosts.
            confirm: DESTRUCTIVE ACTION SAFETY PROTOCOL — This is a two-turn operation:
                Turn 1: Call with confirm=False (default). Display the returned preview
                to the user. STOP. Do NOT call this tool again in the same turn. End your
                response and wait for the user's next message.
                Turn 2: ONLY after the user explicitly replies with approval (e.g., "yes",
                "proceed", "confirm"), call again with confirm=True.
                NEVER set confirm=True in the same turn as the preview call.
                NEVER set confirm=True without the user's explicit approval message.
            remote_host: SSH host for remote DAG file deletion (for delete action).
                Falls back to AIRFLOW_REMOTE_HOST or hostname from AIRFLOW_BASE_URL.
            remote_user: SSH username for remote DAG file deletion.
            remote_port: SSH port (default 22).
            ssh_key_path: Path to SSH private key for remote DAG file deletion.
            remote_dags_dir: Remote DAGs directory path.

        Returns:
            Dictionary with operation results.
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        action = action.strip().lower()
        err = validate_identifier(pipeline_name, "pipeline_name")
        if err:
            return {"success": False, "error": err}
        try:
            if action == "update_schedule":
                if not new_schedule:
                    return {
                        "success": False,
                        "error": "Parameter 'new_schedule' is required for update_schedule.",
                    }
                return await _update_pipeline_schedule(
                    pipeline_name,
                    new_schedule,
                    auto_deploy=auto_deploy,
                    wait_for_reload=wait_for_reload,
                    strict_host_key_checking=strict_host_key_checking,
                )
            elif action == "pause":
                return await _pause_pipeline(pipeline_name)
            elif action == "resume":
                return await _resume_pipeline(pipeline_name)
            elif action == "delete":
                if not confirm:
                    _remote_host = remote_host or orchestrator.settings.airflow.remote_host
                    if not _remote_host:
                        try:
                            from urllib.parse import urlparse as _urlparse

                            _base = orchestrator.settings.airflow.base_url
                            if _base:
                                _remote_host = _urlparse(_base).hostname
                        except Exception:
                            pass
                    _has_host = bool(_remote_host)
                    _has_user = bool(
                        remote_user or orchestrator.settings.airflow.remote_user
                    )
                    _has_key = bool(
                        ssh_key_path or orchestrator.settings.airflow.remote_ssh_key
                    )
                    _has_pwd = bool(
                        orchestrator.settings.airflow.remote_password
                        and orchestrator.settings.airflow.remote_password.get_secret_value()
                    )
                    _sftp_ready = _has_host and _has_user and (_has_key or _has_pwd)
                    _dag_folder = (
                        remote_dags_dir
                        or orchestrator.settings.airflow.dag_folder
                    )
                    remote_dag = str(
                        PurePosixPath(_dag_folder) / f"{pipeline_name}.py"
                    )
                    if _sftp_ready:
                        remote_detail = "be attempted via SFTP"
                    elif _has_host and _has_user:
                        remote_detail = "NOT be deleted (SSH credentials not configured)"
                    else:
                        remote_detail = "NOT be deleted (SSH not configured)"
                    return {
                        "success": False,
                        "requires_confirmation": True,
                        "action": "delete",
                        "pipeline_name": pipeline_name,
                        "warning": f"This will delete pipeline '{pipeline_name}' from Airflow.",
                        "delete_dag_file": delete_dag_file,
                        "delete_dbt_models": delete_dbt_models,
                        "detail": (
                            f"Airflow DAG metadata will be deleted. "
                            f"Remote DAG file ({remote_dag}) will {remote_detail}. "
                            f"Local DAG file will {'BE DELETED' if delete_dag_file else 'be preserved'}. "
                            f"dbt models will {'BE DELETED' if delete_dbt_models else 'be preserved'}."
                        ),
                        "hint": "Re-call with confirm=True to proceed.",
                    }
                return await _delete_pipeline(
                    pipeline_name, delete_dag_file, delete_dbt_models,
                    strict_host_key_checking=strict_host_key_checking,
                    remote_host=remote_host,
                    remote_user=remote_user,
                    remote_port=remote_port,
                    ssh_key_path=ssh_key_path,
                    remote_dags_dir=remote_dags_dir,
                )
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown action '{action}'. "
                        "Valid actions: update_schedule, pause, resume, delete"
                    ),
                }
        except Exception as e:
            logger.error("pipeline_control(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 3: pipeline_deploy
    # ══════════════════════════════════════════════════════════════

    async def pipeline_deploy(
        action: Literal[
            "deploy_complete",
            "deploy_dags",
            "create_sync_dag",
            "create_dbt_dag",
        ],
        pipeline_name: str | None = None,
        # deploy_complete params
        dag_file: str | None = None,
        tpt_dir: str = "tpt_scripts",
        bteq_dir: str = "bteq_scripts",
        dbt_dir: str = "dbt_project",
        csv_files: list[str] | None = None,
        remote_base: str | None = None,
        # deploy_dags params
        local_dags_dir: str | None = None,
        remote_host: str | None = None,
        remote_user: str | None = None,
        remote_port: int | None = None,
        remote_dags_dir: str | None = None,
        auth_method: str = "key",
        ssh_key_path: str | None = None,
        strict_host_key_checking: bool = False,
        dry_run: bool = False,
        wait_for_dag_loaded: bool = False,
        max_wait_seconds: int = 360,
        trigger_after_deploy: bool = False,
        trigger_config: dict[str, Any] | None = None,
        validate_imports: bool = True,
        create_backup: bool = True,
        rollback_on_failure: bool = True,
        # create_sync_dag params
        dag_id: str | None = None,
        connection_id: str | None = None,
        airbyte_conn_id: str | None = None,
        schedule: str = "@daily",
        schedule_interval: str | None = None,
        owner: str | None = None,
        start_date_iso: str | None = None,
        tags: list[str] | None = None,
        email: list[str] | None = None,
        output_filename: str | None = None,
        # dbt DAG params (create_dbt_dag, and create_sync_dag when project_name is set)
        # ``project_name`` is the SOLE locator for the dbt sub-project
        # under ``<workspace>/dbt_project/dbt_<name>/``. The dbt task at
        # runtime reads creds from the per-sub-project ``.env`` written
        # by ``dbt_project(action='create_structure')`` — no Airflow
        # Variables, no Airflow Teradata Connection, no name resolution
        # via ``teradata_profile``.
        project_name: str | None = None,
        dbt_models: list[str] | None = None,
        dbt_target: str = "dev",
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
        # create_elt_dag params (create_sync_dag with project_name)
        source_name: str | None = None,
        target_schema: str | None = None,
        use_ssh_for_dbt: bool | None = None,
        ssh_conn_id: str = "ssh_default",
        ssh_profile: str | None = None,
        # Vestigial param: accepted on the router for shape consistency
        # with prompts that pre-date the ``.env`` migration, but IGNORED
        # by the dbt-DAG actions (``create_dbt_dag`` / ``create_sync_dag``
        # with ``project_name``). No other ``pipeline_deploy`` action
        # consumes it either. Safe to omit; safe to pass.
        teradata_profile: str | None = None,
    ) -> dict[str, Any]:
        """Deploy pipelines and generate Airflow DAGs — including standalone dbt transformation DAGs.

        Connection: follows the server's wizard-vs-profile selection policy
        (see the server ``instructions``). Pass ``ssh_profile`` to name the
        SSH identity from ``connections.yaml`` used for SFTP-deploying DAGs
        to the remote Airflow server. Default is the wizard-configured SSH
        identity unless the user names a profile.

        To generate a standalone Airflow DAG that runs dbt transformations on a schedule,
        use action='create_dbt_dag' with dag_id, project_name, and schedule parameters.
        ``project_name`` selects the per-Teradata-profile dbt sub-project under
        ``<workspace>/dbt_project/dbt_<name>/``; the sub-project must already exist
        (scaffold it via ``dbt_project(action='create_structure')`` first).

        ELT Pipeline Workflow — Sequential Prompts Required:
          This tool handles DAG generation and deployment to Airflow.
          Before deploying, data transfer must be configured (via airbyte_pipeline
          or airflow_teradata_load). After deploying, the user must separately:
          1. Trigger execution: dag_trigger(mode='run', dag_id='...', ...)
          2. Generate dbt models: dbt_generate_model(model_type='staging', ...)
          3. Execute dbt: dbt_execute(command='run', models=[...])
          Each step should be a separate user prompt.

        Args:
            action: One of:
                - "deploy_complete" — Deploy all pipeline components (DAG, TPT, BTEQ, dbt, CSV).
                - "deploy_dags"    — Deploy DAG files to remote Airflow server via SSH/SFTP.
                - "create_sync_dag" — Generate an Airflow DAG to trigger an Airbyte sync.
                    When project_name is set, generates a combined Airbyte+dbt ELT pipeline
                    that runs dbt transformations against the resolved sub-project after sync.
                    NOTE: ``teradata_profile`` is NOT required for the dbt step (and is ignored
                    if passed). ``project_name`` is the only locator; the dbt task reads creds
                    from the sub-project's per-instance ``.env``.
                - "create_dbt_dag" — Generate a standalone Airflow DAG for dbt transformations.
                    Required: dag_id, project_name. Optional: schedule, dbt_models, dbt_target,
                    run_dbt_tests, generate_dbt_docs, owner, tags, output_filename, use_ssh_for_dbt.
                    use_ssh_for_dbt is auto-detected when AIRFLOW_REMOTE_HOST or MCP_CLIENT_SSH_HOST is configured.
                    NOTE: TERADATA_* credentials flow via the per-sub-project ``.env`` written by
                    dbt_project(action='create_structure'). The DAG runs ``dotenv run -- dbt ...``
                    and reads the .env at task time — no Airflow Variables are involved.
                    ``teradata_profile`` is NOT required at this action and is ignored if passed;
                    ``project_name`` is the only locator. The Teradata identity that owns the
                    sub-project is returned in the response as ``teradata_identity``.
                    For refresh_env after credential rotation, the call shape depends on the
                    binding form of ``teradata_identity``:
                      - Named profile (e.g. ``"prod"``): pass it as ``teradata_profile``, e.g.
                        ``dbt_project(action='refresh_env', project_name=..., teradata_profile='prod')``.
                      - Wizard sentinel (``"wizard:<host_slug>"``): OMIT ``teradata_profile``
                        (refresh_env infers the wizard-default identity from Settings). The
                        ``wizard:<...>`` form is a synthetic identity, NOT a connections.yaml
                        profile name; passing it would trigger a named-profile lookup and fail.
                    Worker prerequisite: ``pip install "python-dotenv[cli]"``. The dbt sub-project
                    (including .env) must be on the Airflow worker filesystem at the same path
                    the MCP server wrote it; after credential rotation, re-sync it to the worker.
            pipeline_name: Pipeline name. Required for deploy_complete and deploy_dags.
            dag_file: Path to DAG file (deploy_complete).
            tpt_dir: TPT scripts directory (deploy_complete, default 'tpt_scripts').
            bteq_dir: BTEQ scripts directory (deploy_complete, default 'bteq_scripts').
            dbt_dir: dbt project directory (deploy_complete, default 'dbt_project').
            csv_files: CSV file paths to deploy (deploy_complete).
            remote_base: Remote base path (deploy_complete).
            local_dags_dir: Local DAGs directory (deploy_dags).
            remote_host: Remote Airflow host (deploy_dags).
            remote_user: SSH username (deploy_dags).
            remote_port: SSH port (deploy_dags); omit to use AIRFLOW_REMOTE_PORT setting (default 22).
            remote_dags_dir: Remote DAGs directory (deploy_dags).
            auth_method: SSH auth method (deploy_dags, default 'key').
            ssh_key_path: SSH key path (deploy_dags).
            strict_host_key_checking: Verify SSH host keys (deploy_dags, default False).
            dry_run: Preview without deploying (deploy_dags, default False).
            wait_for_dag_loaded: Wait for Airflow to parse DAG (deploy_dags, default False).
            max_wait_seconds: Max wait for DAG parsing (deploy_dags, default 360).
            trigger_after_deploy: Trigger DAG after deploy (deploy_dags, default False).
            trigger_config: Config for trigger (deploy_dags).
            validate_imports: Validate DAG imports (deploy_dags, default True).
            create_backup: Backup existing DAG (deploy_dags, default True).
            rollback_on_failure: Rollback on failure (deploy_dags, default True).
            dag_id: DAG identifier (create_sync_dag).
            connection_id: Airbyte connection ID (create_sync_dag).
            airbyte_conn_id: Airflow connection ID for Airbyte (create_sync_dag).
            schedule: DAG schedule interval (create_sync_dag, default '@daily').
            owner: DAG owner (create_sync_dag).
            start_date_iso: Start date ISO string (create_sync_dag).
            tags: DAG tags (create_sync_dag).
            email: Alert email addresses (create_sync_dag).
            output_filename: Output filename (create_sync_dag, create_dbt_dag).
            project_name: Name of the per-Teradata-profile dbt sub-project under
                ``<workspace>/dbt_project/dbt_<name>/`` (create_dbt_dag; also
                create_sync_dag to produce a combined Airbyte+dbt ELT pipeline).
                Slugified to ``dbt_<slug(project_name)>/``; the sub-project must
                already exist (scaffold via ``dbt_project(action='create_structure')``
                first). This is the SOLE locator — ``teradata_profile`` is NOT
                involved in sub-project resolution.
            dbt_models: Specific dbt models to run (create_dbt_dag, create_sync_dag).
            dbt_target: dbt target profile (default: 'dev').
            run_dbt_tests: Run dbt tests after models (default: True).
            generate_dbt_docs: Generate dbt docs (default: False).
            source_name: Source system name (create_sync_dag with project_name).
            target_schema: Target schema name (create_sync_dag with project_name).
            use_ssh_for_dbt: Use SSH for remote dbt execution (default: False).
                Set to True when dbt runs on a remote Airflow server (recommended for
                production). When True, ensures the SSH connection exists in Airflow
                and generates SSHOperator tasks instead of BashOperator.
            ssh_conn_id: Airflow connection ID for SSH (default: 'ssh_default').
                Used with use_ssh_for_dbt=True and create_dbt_dag.
            ssh_profile: Connection profile name from connections.yaml for SSH credentials.
                Used with use_ssh_for_dbt=True to resolve host, username, password/key.
            teradata_profile: Vestigial on this router. Accepted on the signature
                for shape consistency with pre-``.env``-migration prompts, but
                IGNORED by every ``pipeline_deploy`` action (including create_dbt_dag
                and create_sync_dag). The dbt sub-project's ``.env`` — written by
                ``dbt_project(action='create_structure')`` — is the cred source at
                DAG runtime; ``project_name`` is the only locator at DAG-creation
                time. Safe to omit, safe to pass.

        Returns:
            Dictionary with deployment or creation results.
        """
        schedule = schedule_interval or schedule
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        if remote_port is not None and not (1 <= remote_port <= 65535):
            return {
                "success": False,
                "error": "Parameter 'remote_port' must be between 1 and 65535.",
            }
        if max_wait_seconds < 1:
            return {"success": False, "error": "Parameter 'max_wait_seconds' must be >= 1."}
        action = action.strip().lower()
        try:
            if action == "deploy_complete":
                if not pipeline_name:
                    return {
                        "success": False,
                        "error": "Parameter 'pipeline_name' is required for deploy_complete.",
                    }
                return await _deploy_complete_pipeline(
                    pipeline_name,
                    dag_file,
                    tpt_dir,
                    bteq_dir,
                    dbt_dir,
                    csv_files,
                    remote_base,
                    strict_host_key_checking=strict_host_key_checking,
                )
            elif action == "deploy_dags":
                return await _deploy_dags_to_airflow(
                    pipeline_name=pipeline_name,
                    local_dags_dir=local_dags_dir,
                    remote_host=remote_host,
                    remote_user=remote_user,
                    remote_port=remote_port,
                    remote_dags_dir=remote_dags_dir,
                    auth_method=auth_method,
                    ssh_key_path=ssh_key_path,
                    strict_host_key_checking=strict_host_key_checking,
                    dry_run=dry_run,
                    wait_for_dag_loaded=wait_for_dag_loaded,
                    max_wait_seconds=max_wait_seconds,
                    trigger_after_deploy=trigger_after_deploy,
                    trigger_config=trigger_config,
                    validate_imports=validate_imports,
                    create_backup=create_backup,
                    rollback_on_failure=rollback_on_failure,
                )
            elif action == "create_sync_dag":
                if not dag_id:
                    return {
                        "success": False,
                        "error": "Parameter 'dag_id' is required for create_sync_dag.",
                    }
                if not connection_id:
                    return {
                        "success": False,
                        "error": "Parameter 'connection_id' is required for create_sync_dag.",
                    }
                # Include dbt step when caller named a sub-project. Without
                # ``project_name`` the DAG is Airbyte-only. ``teradata_profile``
                # is not forwarded — the dbt task runs ``dotenv run -- dbt ...``
                # against the per-sub-project ``.env`` and ``project_name``
                # is the only locator needed at DAG-generation time.
                if project_name:
                    return await _create_elt_dag(
                        dag_id=dag_id,
                        connection_id=connection_id,
                        airbyte_conn_id=airbyte_conn_id,
                        source_name=source_name,
                        target_schema=target_schema,
                        project_name=project_name,
                        dbt_models=dbt_models,
                        dbt_target=dbt_target,
                        run_dbt_tests=run_dbt_tests,
                        generate_dbt_docs=generate_dbt_docs,
                        use_ssh_for_dbt=use_ssh_for_dbt,
                        schedule=schedule,
                        owner=owner,
                        tags=tags,
                        output_filename=output_filename,
                    )
                return await _create_airbyte_sync_dag(
                    dag_id=dag_id,
                    connection_id=connection_id,
                    airbyte_conn_id=airbyte_conn_id,
                    schedule=schedule,
                    owner=owner,
                    start_date_iso=start_date_iso,
                    tags=tags,
                    email=email,
                    output_filename=output_filename,
                )
            elif action == "create_dbt_dag":
                if not dag_id:
                    return {
                        "success": False,
                        "error": "Parameter 'dag_id' is required for create_dbt_dag.",
                    }
                # ``project_name`` is the only locator for the dbt sub-
                # project — the dbt task runs ``dotenv run -- dbt ...``
                # against the per-sub-project ``.env`` and does not need
                # ``teradata_profile`` at DAG-generation time. The
                # parameter is accepted on the router for shape
                # consistency with other actions but ignored here.
                return await _create_dbt_dag(
                    dag_id=dag_id,
                    project_name=project_name,
                    dbt_models=dbt_models,
                    dbt_target=dbt_target,
                    run_dbt_tests=run_dbt_tests,
                    generate_dbt_docs=generate_dbt_docs,
                    schedule=schedule,
                    owner=owner,
                    tags=tags,
                    output_filename=output_filename,
                    use_ssh_for_dbt=use_ssh_for_dbt,
                    ssh_conn_id=ssh_conn_id,
                    ssh_profile=ssh_profile,
                )
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown action '{action}'. "
                        "Valid actions: deploy_complete, deploy_dags, create_sync_dag, "
                        "create_dbt_dag"
                    ),
                }
        except Exception as e:
            logger.error("pipeline_deploy(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 4: pipeline_validate (standalone)
    # ══════════════════════════════════════════════════════════════

    async def pipeline_validate(
        pipeline_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate a pipeline configuration — invoke this tool instead of reading files.

        This is the single authoritative pre-flight check for ELT pipelines. When asked
        to validate a pipeline configuration, always call this tool first and report its
        structured result. Do not manually read DAG files, CSV source files, or
        connections.yaml, and do not perform your own validation logic — those approaches
        produce inaccurate reports and miss errors that this tool catches (e.g. a missing
        input_file_path for file-based pipelines). DAG file syntax is outside this tool's
        scope and should not be reported as a validation finding.

        Checks performed: Teradata connectivity, Airflow reachability, Airbyte availability
        (when used), required fields, and source-specific constraints.

        Args:
            pipeline_config: Pipeline configuration dictionary. Keys:
                - pipeline_name (required)
                - source_type: "airbyte" | "csv" | "csv_file" | "file" | "tpt_file"
                  (optional, inferred when omitted)
                - For airbyte sources: connection_id, source_id, destination_id (optional)
                - For all file-based sources (csv/csv_file/file/tpt_file):
                  input_file_path (str) — path to the source file. When source_type is
                  explicitly set to a csv-like value, validation fails with a clear error
                  if input_file_path is missing or empty. When source_type is omitted,
                  a non-empty input_file_path is required for file validation to be
                  triggered (empty/None causes the source to be inferred as unknown and
                  skips file checks). Do not use "files: List[str]".
                - target_schema: Teradata destination schema (optional)
                - schedule: cron expression or preset (optional)

        Returns:
            Dictionary with validation results and any issues found.
        """
        try:
            return await _validate_pipeline_configuration(pipeline_config)
        except Exception as e:
            logger.error("pipeline_validate failed: %s", e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 5: airflow_connections
    # ══════════════════════════════════════════════════════════════

    async def airflow_connections(
        action: Literal["list", "create_teradata", "create_airbyte", "create_ssh"],
        connection_id: str | None = None,
        conn_id_prefix: str | None = None,
        conn_type: str | None = None,
        teradata_profile: str | None = None,
        ssh_profile: str | None = None,
        timeout: int | None = None,
        strict_ssh: bool = True,
    ) -> dict[str, Any]:
        """Manage Airflow connections — list, create Teradata, Airbyte, or SSH connections.

        Credentials are resolved server-side from the default Teradata connection.
        The LLM never handles passwords or secrets. Just call the action directly
        without specifying a profile — the server uses the configured credentials.

        Args:
            action: One of:
                - "list"             — List existing Airflow connections.
                - "create_teradata"  — Create an Airflow connection for Teradata.
                  Just call this directly — no profile needed.
                - "create_airbyte"   — Create an Airflow connection for Airbyte.
                - "create_ssh"       — Create an Airflow SSH connection.
            connection_id: Airflow connection ID. Defaults vary by action.
            conn_id_prefix: Filter prefix for list action.
            conn_type: Filter by connection type for list action.
            teradata_profile: Optional. Only needed when targeting a different
                Teradata system than the default. Omit for normal use.
            ssh_profile: Optional. Only needed when targeting a different
                SSH host than the default. Omit for normal use.
            timeout: SSH command timeout in seconds for create_ssh.
            strict_ssh: Whether to enforce strict SSH host key checking for
                create_ssh. If True (default), host keys must be known/verified.
                If False, host key checking is relaxed to allow connecting to
                hosts whose keys are not already trusted.

        Returns:
            Dictionary with connection list or creation results.
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        action = action.strip().lower()
        try:
            if action == "list":
                return await _list_airflow_connections(conn_id_prefix, conn_type)
            elif action == "create_teradata":
                _conn_id = connection_id or "teradata_default"
                return await _create_airflow_teradata_connection(_conn_id, teradata_profile)
            elif action == "create_airbyte":
                _conn_id = connection_id or "airbyte_default"
                return await _create_airflow_airbyte_connection(_conn_id)
            elif action == "create_ssh":
                _conn_id = connection_id or "ssh_localhost"
                return await _create_airflow_ssh_connection(_conn_id, ssh_profile, timeout, strict_ssh)
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown action '{action}'. "
                        "Valid actions: list, create_teradata, create_airbyte, create_ssh"
                    ),
                }
        except Exception as e:
            logger.error("airflow_connections(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ── Return router tools ────────────────────────────────────────
    return {
        "pipeline_status": pipeline_status,
        "pipeline_control": pipeline_control,
        "pipeline_deploy": pipeline_deploy,
        "pipeline_validate": pipeline_validate,
        "airflow_connections": airflow_connections,
    }
