"""dbt management and operations tools.

This module provides MCP tools for managing dbt projects, running models,
tests, and generating documentation.
"""

import asyncio
import logging
import re
import shlex
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import yaml

from ..auth import TeradataAuth, is_explicit_profile, resolve_teradata_auth
from ..generators.dbt_generator import _write_dotenv_file
from ..orchestrator import PipelineOrchestrator
from ..response_sanitizer import safe_error_message, sanitize_response
from ..storage.metadata_store import MetadataEntry
from ..utils.validators import slugify_dir_name

logger = logging.getLogger(__name__)


async def _auto_install_deps(orchestrator: "PipelineOrchestrator") -> None:
    project_dir = Path(orchestrator.dbt_client.project_dir)
    packages_file = project_dir / "packages.yml"
    dbt_packages_dir = project_dir / "dbt_packages"
    if packages_file.exists() and (
        not dbt_packages_dir.exists() or not any(dbt_packages_dir.iterdir())
    ):
        logger.info("packages.yml found but dbt_packages not installed; running dbt deps")
        await asyncio.to_thread(orchestrator.dbt_client.deps)


# Matches safe dbt identifiers: letters, digits, and underscores only.
# Rejects path separators, dots, and any other character that could allow
# a user-supplied name to escape the intended output directory.
_DBT_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_dbt_identifier(field: str, value: object) -> dict[str, Any] | None:
    """Return an error dict if *value* is not a safe dbt identifier, or None when valid.

    Used in router dispatch branches to produce clear, field-specific errors
    before any async work begins.  Catches both non-string values (which would
    otherwise raise TypeError inside re.match) and strings containing characters
    that could break generated Jinja/SQL.
    """
    if not isinstance(value, str):
        return {
            "success": False,
            "error": (
                f"{field} must be a string, got {type(value).__name__}; "
                "use only letters, digits, and underscores (^[A-Za-z0-9_]+$)"
            ),
        }
    if not _DBT_NAME_RE.match(value):
        return {
            "success": False,
            "error": (
                f"{field} contains invalid characters (got {value!r}); "
                "use only letters, digits, and underscores (^[A-Za-z0-9_]+$)"
            ),
        }
    return None




def _require_dbt_database(auth: TeradataAuth | None) -> dict[str, Any] | None:
    """Return an error response if the resolved Teradata auth has no
    default database/schema set.

    dbt-teradata renders the profile's ``schema:`` field via
    ``{{ env_var('TERADATA_DATABASE') }}``; an empty value produces the
    cryptic Teradata error 3706 ("Blank name in quotation marks") at the
    ``create_schema`` macro — well before model materialization. Catching
    it here turns a confusing runtime failure into a clear tool-level
    error that names the wizard field and the recovery steps.

    Returns ``None`` when ``auth`` is ``None`` (the caller has already
    handled missing-auth) or when ``auth.database`` is non-empty after
    stripping whitespace. Otherwise returns the error response.
    """
    if auth is None:
        return None
    if auth.database and auth.database.strip():
        return None
    return {
        "success": False,
        "action_required": "set_teradata_database",
        "error": (
            "Teradata default database is not configured. dbt requires a "
            "default database for model materialization (rendered as the "
            "``schema:`` field in profiles.yml). The agent must NOT create "
            "or edit the ``.env`` file — credentials are user-managed. Ask "
            "the user to set this via one of:\n"
            "  1. Setup Wizard → Teradata → Database (then Save and Reload).\n"
            "  2. Add 'database: <your_db>' to a named profile in "
            "connections.yaml and call connection_profiles(action='reload').\n"
            "Surface this message to the user and wait for their action."
        ),
    }


# ── per-Teradata-profile dbt sub-project resolution ──────────────────
#
# Each Teradata identity (named profile OR wizard-default-keyed-by-host)
# gets its own dbt sub-project under ``<workspace>/dbt_project/dbt_<name>/``.
# The user picks ``<name>`` on first creation; we never auto-derive it.
# Subsequent calls find the existing sub-project by walking
# ``dbt_*/dbt_project.yml`` and matching the ``profile:`` field against the
# resolved identity. See ``_ProjectResolution`` for the resolver contract.


def _resolve_teradata_identity(
    orchestrator: PipelineOrchestrator,
    teradata_profile: str | None,
) -> str | None:
    """Resolve the stable identity used in dbt_project.yml::profile.

    - Named profile (per ``is_explicit_profile``) → returned verbatim.
    - Wizard-default → ``wizard:<slug(settings.teradata.host)>``.
    - Wizard-default with no host configured → ``None`` (caller errors).

    The identity is what binds a sub-project to its credentials and what
    the reverse-lookup matches against. When the wizard host changes,
    the synthetic identity changes, the lookup misses, and the user is
    prompted for a new project name — this is the conflict-prevention
    property.
    """
    if is_explicit_profile(teradata_profile):
        return teradata_profile
    host = (orchestrator.settings.teradata.host or "").strip()
    slug = slugify_dir_name(host)
    return f"wizard:{slug}" if slug else None


_ProjectStatus = Literal[
    "existing",
    "needs_name",
    "will_create",
    "ambiguous",
    "conflict",
    "no_identity",
    "legacy_layout",
    "name_collision",
]


@dataclass(frozen=True)
class _ProjectResolution:
    """Outcome of resolving which dbt sub-project a tool call targets.

    ``status`` selects the branch:
      - ``existing``       — ``project_dir`` is the sub-project to use.
      - ``will_create``    — ``project_dir`` is the path to scaffold.
      - ``needs_name``     — caller returns ``ask_project_name``.
      - ``ambiguous``      — caller returns ``disambiguate_project_name``;
                              ``matches`` lists the candidate sub-project paths.
      - ``conflict``       — target dir exists but bound to a different
                              identity (``existing_identity`` populated).
      - ``no_identity``    — wizard-default with no Teradata host configured.
      - ``legacy_layout``  — pre-multi-project ``dbt_project.yml`` at parent
                              root; refuse and tell the user to migrate.
      - ``name_collision`` — the resulting sub-project directory name would
                              equal the parent container's name (e.g. parent
                              ``dbt_project/``, slug ``project`` →
                              ``dbt_project/dbt_project/``). ``collision_with``
                              names the parent basename. Caller surfaces as
                              ``action_required: rename_project`` with safe
                              alternatives.
    """

    status: _ProjectStatus
    project_dir: Path | None = None
    identity: str | None = None
    matches: tuple[Path, ...] = field(default_factory=tuple)
    existing_identity: str | None = None  # populated on ``conflict``
    collision_with: str | None = None  # populated on ``name_collision``


def _suggest_safe_project_names(
    orchestrator: PipelineOrchestrator | Any,
    rejected_name: str | None,
) -> list[str]:
    """Return 2-3 candidate ``project_name`` values that won't collide
    with the parent container's basename.

    Suggestions are ordered most-specific → most-generic:
      1. Workspace basename slug (e.g. ``~/teradata-etl-mcp-workspace`` →
         ``teradata_etl_mcp_workspace``) — most personalized to this user.
      2. ``analytics`` — generic dbt convention.
      3. The rejected name with a domain suffix (e.g. ``project_data``)
         when the rejected name itself was specific enough to seed.
    """
    suggestions: list[str] = []
    try:
        workspace = Path(orchestrator.settings.workspace_dir)
        ws_slug = slugify_dir_name(workspace.name)
        if ws_slug and ws_slug not in {"dbt_project", "project"}:
            suggestions.append(ws_slug)
    except Exception:
        pass
    if "analytics" not in suggestions:
        suggestions.append("analytics")
    if rejected_name:
        rejected_slug = slugify_dir_name(rejected_name)
        if rejected_slug and rejected_slug not in {"dbt_project", "project"}:
            candidate = f"{rejected_slug}_data"
            if candidate not in suggestions:
                suggestions.append(candidate)
    return suggestions[:3]


def _missing_project_name_response(
    orchestrator: PipelineOrchestrator | Any,
    action: str,
) -> dict[str, Any]:
    """Build the response for ``action_required: "ask_project_name"`` when
    a ``dbt_project`` action that needs ``project_name`` was called
    without one.

    Echoes a concrete suggestion (workspace basename slug, then a generic
    fallback) so the LLM can either prompt the user or self-resolve in
    one round trip rather than asking the user for an unspecified name.
    """
    suggestions = _suggest_safe_project_names(orchestrator, rejected_name=None)
    primary = suggestions[0] if suggestions else "analytics"
    example = f"dbt_project(action='{action}', project_name='{primary}', ...)"
    return {
        "success": False,
        "action_required": "ask_project_name",
        "error": (
            f"project_name is required for action '{action}'. "
            f"It becomes a sub-folder under "
            f"<workspace>/dbt_project/dbt_<slug>/. Choose a short "
            f"snake_case identifier (e.g. {primary!r}). "
            f"Example call: {example}"
        ),
        "suggested_project_names": suggestions,
        "naming_rules": (
            "Lowercase + non-alphanumeric chars become underscores; "
            "leading 'dbt_' is auto-stripped (so 'dbt_test' and 'test' "
            "both resolve to dbt_test/); a name that would equal the "
            "parent container ('dbt_project') is rejected."
        ),
    }


def _collision_response(
    orchestrator: PipelineOrchestrator | Any,
    resolution: "_ProjectResolution",
    rejected_name: str | None,
) -> dict[str, Any]:
    """Translate a ``name_collision`` resolution into a tool response.

    The error message and ``suggested_project_names`` give the LLM a
    concrete next call without another round trip to the user.
    """
    suggestions = _suggest_safe_project_names(orchestrator, rejected_name)
    parent_name = resolution.collision_with or "dbt_project"
    return {
        "success": False,
        "action_required": "rename_project",
        "error": (
            f"project_name='{rejected_name}' would create the sub-project "
            f"'{parent_name}' inside its own parent container "
            f"('{parent_name}/'), which is visually ambiguous and silently "
            f"nests. Choose a name distinct from the parent. Suggestions: "
            f"{', '.join(repr(s) for s in suggestions)}."
        ),
        "rejected_project_name": rejected_name,
        "collision_with": parent_name,
        "suggested_project_names": suggestions,
    }


def _get_project_defaults(
    orchestrator: PipelineOrchestrator | Any,
    teradata_profile: str | None = None,
) -> dict[str, Any]:
    """Implementation of ``dbt_info(info_type='project_defaults')``.

    Read-only inspection of what a subsequent
    ``dbt_project(action='create_structure')`` call would see and
    suggest. Lets the LLM check the workspace state before guessing
    a project_name.
    """
    parent = Path(orchestrator.dbt_project_parent)
    workspace_dir: str | None = None
    try:
        workspace_dir = str(orchestrator.settings.workspace_dir)
    except Exception:
        pass

    suggestions = _suggest_safe_project_names(orchestrator, rejected_name=None)
    default_project_name = suggestions[0] if suggestions else "analytics"

    # Existing sub-projects under the parent — name + bound identity.
    existing_subprojects: list[dict[str, str | None]] = []
    if parent.exists():
        for sub in sorted(parent.iterdir()):
            if not sub.is_dir() or not sub.name.startswith("dbt_"):
                continue
            sub_yml = sub / "dbt_project.yml"
            bound_identity = _read_project_profile(sub_yml) if sub_yml.exists() else None
            existing_subprojects.append({"sub_project": sub.name, "identity": bound_identity})

    identity = _resolve_teradata_identity(orchestrator, teradata_profile)

    # Reserved names: any slug that would equal the parent basename.
    # Concretely ``project`` and ``dbt_project`` for the default parent.
    parent_slug = parent.name[4:] if parent.name.startswith("dbt_") else parent.name
    reserved_names = sorted({"project", "dbt_project", parent.name, parent_slug})

    return {
        "success": True,
        "info_type": "project_defaults",
        "workspace_dir": workspace_dir,
        "dbt_project_parent": str(parent),
        "default_project_name": default_project_name,
        "suggested_project_names": suggestions,
        "reserved_names": reserved_names,
        "teradata_identity": identity,
        "existing_subprojects": existing_subprojects,
        "naming_rules": (
            "project_name becomes <workspace>/dbt_project/dbt_<slug>/. "
            "Slugified: lowercase + non-alnum→underscore. Leading 'dbt_' "
            "is auto-stripped. Reserved names rejected with "
            "action_required='rename_project'."
        ),
    }


def _read_project_profile(dbt_project_yml: Path) -> str | None:
    """Read the ``profile:`` field from a ``dbt_project.yml``. Return
    ``None`` on any read/parse failure — caller treats as "no match"."""
    try:
        with open(dbt_project_yml, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if isinstance(data, dict):
            value = data.get("profile")
            if isinstance(value, str) and value:
                return value
    except (OSError, yaml.YAMLError):
        pass
    return None


def _resolve_dbt_subproject(
    parent: Path,
    identity: str | None,
    project_name: str | None,
) -> _ProjectResolution:
    """Pick which dbt sub-project a tool call should operate on.

    See ``_ProjectResolution`` for the status semantics. Algorithm:

    1. Refuse if a legacy single-project ``dbt_project.yml`` sits at
       ``parent`` itself.
    2. Refuse early if no identity could be resolved.
    3. If ``project_name`` is provided, target ``parent/dbt_<slug>/``:
       - exists with matching profile → ``existing``
       - exists with different profile → ``conflict``
       - missing → ``will_create``
    4. Else (no ``project_name``), walk ``parent/dbt_*/dbt_project.yml``
       and collect those whose ``profile:`` equals ``identity``:
       - 0 matches → ``needs_name``
       - 1 match  → ``existing``
       - >1 match → ``ambiguous``
    """
    if (parent / "dbt_project.yml").exists():
        return _ProjectResolution(status="legacy_layout", identity=identity)

    if identity is None:
        return _ProjectResolution(status="no_identity")

    if project_name is not None:
        slug = slugify_dir_name(project_name)
        # Strip a leading ``dbt_`` so passing ``project_name="dbt_test"``
        # and ``project_name="test"`` both produce ``dbt_test/`` rather
        # than the redundant ``dbt_dbt_test/``.
        if slug.startswith("dbt_"):
            slug = slug[4:]
        if not slug:
            # Caller will surface this as a validation error; reuse the
            # ``conflict`` channel keeps the resolver's surface narrow.
            return _ProjectResolution(
                status="conflict",
                identity=identity,
                existing_identity=None,
            )
        # Reject names that would produce a sub-project directory whose
        # basename equals the parent container's name. ``parent`` is
        # typically ``<workspace>/dbt_project/``; with slug ``project``
        # the target would be ``<workspace>/dbt_project/dbt_project/``,
        # visually indistinguishable from the parent. Catch both
        # ``project_name="project"`` (slug→project) and
        # ``project_name="dbt_project"`` (slug→dbt_project→strip→project).
        if f"dbt_{slug}" == parent.name:
            return _ProjectResolution(
                status="name_collision",
                identity=identity,
                collision_with=parent.name,
            )
        target = parent / f"dbt_{slug}"
        target_yml = target / "dbt_project.yml"
        if target_yml.exists():
            existing_id = _read_project_profile(target_yml)
            if existing_id == identity:
                return _ProjectResolution(
                    status="existing",
                    project_dir=target,
                    identity=identity,
                )
            return _ProjectResolution(
                status="conflict",
                project_dir=target,
                identity=identity,
                existing_identity=existing_id,
            )
        return _ProjectResolution(
            status="will_create",
            project_dir=target,
            identity=identity,
        )

    # No project_name → scan for sub-projects bound to this identity.
    if not parent.exists():
        return _ProjectResolution(status="needs_name", identity=identity)

    matches: list[Path] = []
    for sub in parent.iterdir():
        if not sub.is_dir() or not sub.name.startswith("dbt_"):
            continue
        sub_yml = sub / "dbt_project.yml"
        if not sub_yml.exists():
            continue
        if _read_project_profile(sub_yml) == identity:
            matches.append(sub)

    if not matches:
        return _ProjectResolution(status="needs_name", identity=identity)
    if len(matches) == 1:
        return _ProjectResolution(
            status="existing",
            project_dir=matches[0],
            identity=identity,
        )
    return _ProjectResolution(
        status="ambiguous",
        identity=identity,
        matches=tuple(sorted(matches)),
    )


def _autocorrect_columns(
    provided_columns: list[str],
    metadata: dict[str, Any],
) -> tuple[list[str], dict[str, Any] | None]:
    """Auto-correct hallucinated column names against real metadata.

    - Keeps valid columns (case-insensitive match)
    - Drops invalid columns
    - If ALL invalid, falls back to all columns from metadata
    - Returns (corrected_columns, corrections_applied_or_None)
    """
    real_cols = {c["name"].lower(): c["name"] for c in metadata.get("columns", [])}
    valid = [real_cols[c.lower()] for c in provided_columns if c.lower() in real_cols]
    invalid = [c for c in provided_columns if c.lower() not in real_cols]

    if not invalid:
        return provided_columns, None  # No corrections needed

    corrections: dict[str, Any] = {"removed_columns": invalid}
    if valid:
        corrections["kept_columns"] = valid
        corrections["action"] = "removed_invalid_columns"
        return valid, corrections
    else:
        all_cols = [c["name"] for c in metadata["columns"]]
        corrections["action"] = "replaced_all_with_metadata"
        corrections["available_columns"] = all_cols
        return all_cols, corrections


def _autocorrect_single_column(
    column_name: str,
    metadata: dict[str, Any],
    field_name: str,
) -> tuple[str | None, dict[str, Any] | None]:
    """Auto-correct a single column name. Returns (corrected_name, correction_or_None)."""
    real_cols = {c["name"].lower(): c["name"] for c in metadata.get("columns", [])}
    if column_name.lower() in real_cols:
        return real_cols[column_name.lower()], None  # Valid, fix case if needed

    # Column doesn't exist — attempt smart fallback
    correction: dict[str, Any] = {"original": column_name, "field": field_name}
    if field_name == "unique_key":
        pks = metadata.get("primary_keys", [])
        if pks:
            correction["action"] = "replaced_with_primary_key"
            correction["replacement"] = pks[0]
            return pks[0], correction
    # For incremental_column/updated_at, look for timestamp-type columns
    if field_name in ("incremental_column", "updated_at"):
        ts_types = {"timestamp", "date", "time"}
        for col in metadata.get("columns", []):
            col_type = (col.get("type") or col.get("data_type") or "").lower()
            if any(t in col_type for t in ts_types):
                correction["action"] = "replaced_with_timestamp_column"
                correction["replacement"] = col["name"]
                return col["name"], correction
    # Can't auto-correct — signal unresolvable error
    correction["action"] = "unresolvable"
    correction["available_columns"] = sorted(c["name"] for c in metadata.get("columns", []))
    return None, correction  # None signals the caller must error out


def register_dbt_tools(orchestrator: PipelineOrchestrator) -> dict[str, Any]:
    """
    Register dbt management tools.

    Args:
        orchestrator: Pipeline orchestrator instance

    Returns:
        Dictionary of tool functions
    """

    # ------------------------------------------------------------------ #
    #  Metadata resolution helpers (anti-hallucination)                   #
    # ------------------------------------------------------------------ #

    async def _resolve_source_metadata(
        source_name: str,
        table_name: str,
        teradata_profile: str | None = None,
    ) -> dict[str, Any] | None:
        """Resolve a dbt source_name/table_name to real Teradata metadata.

        Best-effort, never raises. Returns metadata dict or None.
        """
        try:
            project_dir = orchestrator.dbt_generator.project_dir
            if not isinstance(project_dir, Path):
                return None

            models_dir = project_dir / "models"

            database = None
            identifier = table_name  # default: table_name is the real table name

            # Scan source YAML files for a matching source
            if models_dir.is_dir():
                try:
                    for yml_path in models_dir.rglob("*.yml"):
                        try:
                            with open(yml_path) as f:
                                content = yaml.safe_load(f)
                            if not content or not isinstance(content, dict):
                                continue
                            for source in content.get("sources", []):
                                if source.get("name") != source_name:
                                    continue
                                database = source.get("database", source_name)
                                for tbl in source.get("tables", []):
                                    if tbl.get("name", "").lower() == table_name.lower():
                                        identifier = tbl.get("identifier", table_name)
                                        break
                                break
                        except Exception:
                            continue
                except Exception:
                    pass

            # Fallback: use source_name as database
            if not database:
                database = source_name

            metadata = await asyncio.to_thread(
                orchestrator.teradata_client.get_table_metadata,
                database,
                identifier,
                False,  # include_stats=False
            )
            return metadata
        except Exception:
            logger.debug(
                "Could not resolve metadata for source=%s table=%s",
                source_name,
                table_name,
            )
            return None

    async def _resolve_upstream_model_columns(
        model_names: list[str],
    ) -> dict[str, list[str]]:
        """Best-effort column resolution for dbt models referenced via ref().

        Tries manifest then catalog. Returns model_name → column_names mapping.
        """
        result: dict[str, list[str]] = {}
        try:
            manifest = await asyncio.to_thread(orchestrator.dbt_client.get_manifest)
            catalog = await asyncio.to_thread(orchestrator.dbt_client.get_catalog)
        except Exception:
            return result

        for name in model_names:
            # Try manifest nodes
            if manifest and isinstance(manifest.get("nodes"), dict):
                for node_id, node in manifest["nodes"].items():
                    if node.get("name") == name and node_id.startswith("model."):
                        cols = node.get("columns", {})
                        if isinstance(cols, dict) and cols:
                            result[name] = list(cols.keys())
                            break
            # Try catalog if not found in manifest
            if name not in result and catalog and isinstance(catalog.get("nodes"), dict):
                for _node_id, node in catalog["nodes"].items():
                    if node.get("metadata", {}).get("name") == name:
                        cols = node.get("columns", {})
                        if isinstance(cols, dict) and cols:
                            result[name] = list(cols.keys())
                            break
        return result

    # ------------------------------------------------------------------ #
    #  Runtime history persistence & estimation helpers                    #
    # ------------------------------------------------------------------ #

    def _persist_dbt_run_history(
        result_summary: list[dict[str, Any]],
        command: str,
        max_history: int = 50,
    ) -> None:
        try:
            index_key = "dbt_model_history:_index"
            existing_index_entry = orchestrator.metadata_store.get_metadata(index_key)
            index: dict[str, str] = (
                existing_index_entry.value
                if existing_index_entry and isinstance(existing_index_entry.value, dict)
                else {}
            )

            now = datetime.now(timezone.utc)
            now_iso = now.isoformat()

            for result in result_summary:
                unique_id = result.get("unique_id")
                execution_time = result.get("execution_time")
                if not unique_id or execution_time is None:
                    continue

                model_name = unique_id.rsplit(".", 1)[-1] if "." in unique_id else unique_id
                index[unique_id] = model_name

                history_key = f"dbt_model_history:{unique_id}"
                existing_entry = orchestrator.metadata_store.get_metadata(history_key)
                history: list[dict[str, Any]] = (
                    existing_entry.value
                    if existing_entry and isinstance(existing_entry.value, list)
                    else []
                )

                adapter_response = result.get("adapter_response", {})
                rows_affected = (
                    adapter_response.get("rows_affected")
                    if isinstance(adapter_response, dict)
                    else None
                )

                history.append(
                    {
                        "execution_time": float(execution_time),
                        "status": result.get("status", "unknown"),
                        "command": command,
                        "timestamp": now_iso,
                        "rows_affected": rows_affected,
                    }
                )

                if len(history) > max_history:
                    history = history[-max_history:]

                orchestrator.metadata_store.store_metadata(
                    MetadataEntry(
                        key=history_key,
                        value=history,
                        timestamp=now,
                        ttl_seconds=None,
                        tags=["dbt", "run_history"],
                    )
                )

            orchestrator.metadata_store.store_metadata(
                MetadataEntry(
                    key=index_key,
                    value=index,
                    timestamp=now,
                    ttl_seconds=None,
                    tags=["dbt", "run_history"],
                )
            )
        except Exception:
            logger.warning("Failed to persist dbt run history", exc_info=True)

    async def _estimate_model_runtime(
        model_name: str | None = None,
    ) -> dict[str, Any]:
        index_key = "dbt_model_history:_index"
        index_entry = await asyncio.to_thread(orchestrator.metadata_store.get_metadata, index_key)
        if not index_entry or not isinstance(index_entry.value, dict) or not index_entry.value:
            return {
                "success": True,
                "models": [],
                "model_count": 0,
                "message": "No runtime history found. Run dbt models first to collect timing data.",
            }

        index: dict[str, str] = index_entry.value

        if model_name:
            search = model_name.lower()
            index = {
                uid: name
                for uid, name in index.items()
                if search in name.lower() or search in uid.lower()
            }

        models_stats: list[dict[str, Any]] = []
        total_avg = 0.0

        for unique_id, short_name in index.items():
            history_key = f"dbt_model_history:{unique_id}"
            entry = await asyncio.to_thread(orchestrator.metadata_store.get_metadata, history_key)
            if not entry or not isinstance(entry.value, list) or not entry.value:
                continue

            history: list[dict[str, Any]] = entry.value
            times = [h["execution_time"] for h in history if "execution_time" in h]
            if not times:
                continue

            sorted_times = sorted(times)
            avg = statistics.mean(times)
            total_avg += avg

            mid = len(sorted_times) // 2
            if len(sorted_times) % 2 == 0 and len(sorted_times) > 1:
                median = (sorted_times[mid - 1] + sorted_times[mid]) / 2
            else:
                median = sorted_times[mid]

            p95_idx = int(len(sorted_times) * 0.95)
            p95 = sorted_times[min(p95_idx, len(sorted_times) - 1)]

            trend = "stable"
            if len(times) >= 4:
                half = len(times) // 2
                first_half_avg = statistics.mean(times[:half])
                second_half_avg = statistics.mean(times[half:])
                if first_half_avg > 0:
                    change = (second_half_avg - first_half_avg) / first_half_avg
                    if change > 0.15:
                        trend = "degrading"
                    elif change < -0.15:
                        trend = "improving"

            last_entry = history[-1]

            models_stats.append(
                {
                    "unique_id": unique_id,
                    "model_name": short_name,
                    "average_seconds": round(avg, 2),
                    "median_seconds": round(median, 2),
                    "min_seconds": round(sorted_times[0], 2),
                    "max_seconds": round(sorted_times[-1], 2),
                    "p95_seconds": round(p95, 2),
                    "run_count": len(times),
                    "last_run": last_entry.get("timestamp"),
                    "last_status": last_entry.get("status"),
                    "trend": trend,
                }
            )

        models_stats.sort(key=lambda m: m["average_seconds"], reverse=True)

        return {
            "success": True,
            "models": models_stats,
            "total_estimated_seconds": round(total_avg, 2),
            "model_count": len(models_stats),
        }

    async def _clear_runtime_history() -> dict[str, Any]:
        index_key = "dbt_model_history:_index"
        index_entry = await asyncio.to_thread(orchestrator.metadata_store.get_metadata, index_key)
        count = 0
        if index_entry and isinstance(index_entry.value, dict):
            for unique_id in index_entry.value:
                history_key = f"dbt_model_history:{unique_id}"
                await asyncio.to_thread(orchestrator.metadata_store.delete_metadata, history_key)
                count += 1
        await asyncio.to_thread(orchestrator.metadata_store.delete_metadata, index_key)
        return {
            "success": True,
            "message": "Runtime history cleared",
            "models_cleared": count,
        }

    # ------------------------------------------------------------------ #
    #  Private helper implementations (original functions, verbatim)      #
    # ------------------------------------------------------------------ #

    async def _run_dbt_models(
        models: list[str] | None = None,
        select: str | None = None,
        exclude: str | None = None,
        full_refresh: bool = False,
        vars: dict[str, Any] | None = None,
        threads: int | None = None,
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Run dbt models.

        Executes dbt run command with model selection, full refresh,
        and variable overrides.

        Args:
            models: Specific model names to run
            select: dbt selection syntax (e.g., "tag:daily", "model+")
            exclude: dbt exclusion syntax
            full_refresh: Force full refresh of incremental models
            vars: Variables to pass to dbt (--vars)
            threads: Number of threads to use

        Returns:
            Dictionary with run results
        """
        try:
            logger.info("Running dbt models")

            await _auto_install_deps(orchestrator)

            # Execute via dbt client directly (orchestrator doesn't support all parameters)
            results = await asyncio.to_thread(
                orchestrator.dbt_client.run,
                models=models or select,  # 'select' parameter maps to 'models' in dbt_client
                exclude=exclude,
                full_refresh=full_refresh,
                vars=vars,
                threads=threads,
                auth=auth,
            )

            # Parse results
            result_summary = results.get("results", [])
            success_count = sum(1 for r in result_summary if r.get("status") == "success")
            error_count = sum(1 for r in result_summary if r.get("status") == "error")
            skipped_count = sum(1 for r in result_summary if r.get("status") == "skipped")

            # Build per-model detail lists
            models_succeeded = [
                r.get("unique_id") for r in result_summary if r.get("status") == "success"
            ]
            models_failed = [
                {
                    "model": r.get("unique_id"),
                    "message": r.get("message"),
                }
                for r in result_summary
                if r.get("status") == "error"
            ]
            per_model_timing = [
                {
                    "model": r.get("unique_id"),
                    "execution_time": r.get("execution_time"),
                    "rows_affected": r.get("adapter_response", {}).get("rows_affected")
                    if isinstance(r.get("adapter_response"), dict)
                    else None,
                }
                for r in result_summary
            ]
            elapsed = results.get("elapsed_time", 0)
            failed_names = ", ".join((f.get("model") or "unknown") for f in models_failed)
            summary = f"{success_count}/{len(result_summary)} models succeeded" + (
                f". Failures: {failed_names}" if models_failed else ""
            )

            response = {
                "success": error_count == 0,
                "total_models": len(result_summary),
                "succeeded": success_count,
                "errored": error_count,
                "skipped": skipped_count,
                "execution_time": elapsed,
                "summary": summary,
                "models_succeeded": models_succeeded,
                "models_failed": models_failed,
                "per_model_timing": per_model_timing,
                "results": result_summary,
            }

            # Add error details if any (kept for backward compat)
            if error_count > 0:
                response["errors"] = models_failed

            # Chained guidance — only on a clean run.
            if error_count == 0:
                response["next_steps"] = [
                    (
                        "**1. Run schema/data tests**: "
                        "`dbt_execute(command='test')`. **Why**: ``dbt run`` "
                        "materializes views/tables but doesn't assert "
                        "correctness; the schema YAMLs include not_null / "
                        "unique tests that must pass before promoting. "
                        "**Effect**: dbt-teradata runs every test defined in "
                        "``models/**/schema.yml``. **If missing**: skip if you "
                        "already use ``dbt_execute(command='build')`` which "
                        "runs run+test in one pass."
                    ),
                    (
                        "**2. Generate dbt docs** (optional): "
                        "`dbt_docs(action='generate')`. **Why**: docs surface "
                        "the lineage graph + column descriptions; useful to "
                        "share with stakeholders or audit before scheduling. "
                        "**Effect**: dbt builds ``target/manifest.json`` + "
                        "``catalog.json`` and the response returns a ``dbt "
                        "docs serve`` command for local viewing. **If "
                        "missing**: skip for ad-hoc runs; do this once before "
                        "scheduling production."
                    ),
                    (
                        "**3. Schedule the run in Airflow** (production): "
                        "`pipeline_deploy(action='create_dbt_dag', "
                        "dag_id='<id>', project_name=<project>)`. **Why**: "
                        "manual ``dbt run`` is for development; production "
                        "runs need a cron. **Effect**: generates an Airflow "
                        "DAG that re-runs this sub-project on the schedule "
                        "you pick. **If missing**: skip for one-off / "
                        "development runs."
                    ),
                ]

            logger.info("dbt run complete: %d succeeded, %d errored", success_count, error_count)

            await asyncio.to_thread(_persist_dbt_run_history, result_summary, "run")

            return response

        except Exception as e:
            logger.error("Failed to run dbt models: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _test_dbt_models(
        models: list[str] | None = None,
        select: str | None = None,
        exclude: str | None = None,
        data: bool = True,
        schema: bool = True,
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Run dbt tests.

        Executes dbt test command to validate data quality and
        schema constraints.

        Args:
            models: Specific models to test
            select: dbt selection syntax
            exclude: dbt exclusion syntax
            data: Include data tests
            schema: Include schema tests

        Returns:
            Dictionary with test results
        """
        try:
            logger.info("Running dbt tests")

            await _auto_install_deps(orchestrator)

            # Build test command arguments
            test_args = {}
            if models:
                test_args["models"] = models
            if select:
                test_args["select"] = select
            if exclude:
                test_args["exclude"] = exclude

            # Execute tests
            results = await asyncio.to_thread(orchestrator.dbt_client.test, auth=auth, **test_args)

            # Parse test results
            test_results = results.get("results", [])
            passed_count = sum(1 for r in test_results if r.get("status") == "pass")
            failed_count = sum(1 for r in test_results if r.get("status") in ["fail", "error"])
            warned_count = sum(1 for r in test_results if r.get("status") == "warn")

            response = {
                "success": failed_count == 0,
                "total_tests": len(test_results),
                "passed": passed_count,
                "failed": failed_count,
                "warned": warned_count,
                "execution_time": results.get("elapsed_time"),
                "results": test_results,
            }

            # Add failure details
            if failed_count > 0:
                response["failures"] = [
                    {
                        "test": r.get("unique_id"),
                        "message": r.get("message"),
                        "failures": r.get("failures"),
                    }
                    for r in test_results
                    if r.get("status") in ["fail", "error"]
                ]

            # Chained guidance — only when every test passed.
            if failed_count == 0 and len(test_results) > 0:
                response["next_steps"] = [
                    (
                        "**1. Generate dbt docs** (optional): "
                        "`dbt_docs(action='generate')`. **Why**: passing tests "
                        "is the right moment to publish lineage + descriptions "
                        "for stakeholders. **Effect**: dbt writes "
                        "``target/manifest.json`` + ``catalog.json`` and the "
                        "response returns a ``dbt docs serve`` command. **If "
                        "missing**: skip for ad-hoc runs; do this before "
                        "scheduling production."
                    ),
                    (
                        "**2. Schedule the run in Airflow** (production): "
                        "`pipeline_deploy(action='create_dbt_dag', "
                        "dag_id='<id>', project_name=<project>)`. **Why**: "
                        "passing tests means the sub-project is ready to be "
                        "scheduled. **Effect**: generates an Airflow DAG that "
                        "runs ``dbt run`` + ``dbt test`` on the schedule you "
                        "pick. **If missing**: skip for development workflows "
                        "where ``dbt_execute`` runs are sufficient."
                    ),
                ]

            logger.info("dbt tests complete: %d passed, %d failed", passed_count, failed_count)

            await asyncio.to_thread(_persist_dbt_run_history, test_results, "test")

            return response

        except Exception as e:
            logger.error("Failed to run dbt tests: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _generate_dbt_docs(
        compile_first: bool = True,
        port: int = 8080,
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Generate dbt documentation.

        Creates dbt documentation site with model lineage, descriptions,
        and column details.

        Args:
            compile_first: Whether to compile project before generating docs
            port: Port to include in the returned serve_command
            auth: Optional Teradata identity for the subprocess env.

        Returns:
            Dictionary with documentation generation results
        """
        try:
            logger.info("Generating dbt documentation")

            # Optionally compile first
            if compile_first:
                logger.info("Compiling dbt project before generating docs")
                await asyncio.to_thread(orchestrator.dbt_client.compile, auth=auth)

            # Generate docs
            results = await asyncio.to_thread(orchestrator.dbt_client.docs_generate, auth=auth)

            # Surface failures from alternate client implementations that return
            # success=False / non-zero returncode instead of raising.
            if not results.get("success", True):
                logger.error(
                    "dbt docs generate failed (rc=%s): %s",
                    results.get("returncode"),
                    results.get("stderr", ""),
                )
                return {
                    "success": False,
                    "returncode": results.get("returncode"),
                    "stderr": results.get("stderr"),
                    "error": "dbt docs generate exited with a non-zero return code.",
                }

            # Build the serve command the user should run in a terminal.
            # shlex.quote is applied to every value so paths with spaces or
            # shell metacharacters are safe to copy-paste.
            dbt_client = orchestrator.dbt_client
            parts = [
                "dbt",
                "docs",
                "serve",
                "--project-dir",
                shlex.quote(str(dbt_client.project_dir)),
            ]
            if dbt_client.profiles_dir:
                parts.extend(["--profiles-dir", shlex.quote(str(dbt_client.profiles_dir))])
            parts.extend(["--target", shlex.quote(str(dbt_client.target)), "--port", str(port)])
            serve_command = " ".join(parts)

            target_dir = Path(dbt_client.project_dir) / "target"
            catalog_path = target_dir / "catalog.json"
            manifest_path = target_dir / "manifest.json"

            if not catalog_path.exists():
                logger.warning("catalog.json not found at %s after docs generate", catalog_path)
            if not manifest_path.exists():
                logger.warning("manifest.json not found at %s after docs generate", manifest_path)

            response = {
                "success": True,
                "catalog_path": str(catalog_path),
                "manifest_path": str(manifest_path),
                "returncode": results.get("returncode"),
                "stdout": results.get("stdout"),
                "stderr": results.get("stderr"),
                "serve_command": serve_command,
                "message": (
                    f"Documentation generated. Run `{serve_command}` in a terminal, "
                    f"then open http://localhost:{port} in your browser."
                ),
                "next_steps": [
                    (
                        f"**1. Serve the docs locally**: run "
                        f"`{serve_command}` in a terminal, then open "
                        f"http://localhost:{port} in your browser. **Why**: "
                        f"docs are static JSON until ``dbt docs serve`` mounts "
                        f"them on a local web server. **Effect**: dbt-teradata "
                        f"hosts the lineage graph + column descriptions. **If "
                        f"missing**: skip if you only need the raw "
                        f"``manifest.json`` / ``catalog.json`` artifacts for "
                        f"another tool."
                    ),
                    (
                        "**2. Schedule the project in Airflow**: "
                        "`pipeline_deploy(action='create_dbt_dag', "
                        "dag_id='<id>', project_name=<project>)`. **Why**: "
                        "docs in hand means the project is review-ready and "
                        "can be promoted to production. **Effect**: generates "
                        "an Airflow DAG that runs ``dbt run`` + ``dbt test`` "
                        "on the schedule you pick. **If missing**: skip if "
                        "the project is for one-off analysis only."
                    ),
                ],
            }

            logger.info("dbt documentation generated")

            return response

        except Exception as e:
            logger.error("Failed to generate dbt docs: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _list_dbt_models(
        model_type: str | None = None,
        include_sources: bool = False,
        include_tests: bool = False,
    ) -> dict[str, Any]:
        """
        List dbt models in the project.

        Retrieves all models, sources, and tests from the dbt project
        with metadata and dependencies.

        Args:
            model_type: Filter by type (staging, intermediate, marts, etc.)
            include_sources: Include dbt sources
            include_tests: Include dbt tests

        Returns:
            Dictionary with model list and metadata
        """
        try:
            logger.info("Listing dbt models")

            result = {
                "models": [],
                "sources": [],
                "tests": [],
            }

            # Get models
            models = await asyncio.to_thread(orchestrator.dbt_client.list_models)

            # Filter by type if specified
            if model_type:
                models = [m for m in models if model_type.lower() in m.get("path", "").lower()]

            result["models"] = [
                {
                    "name": m.get("name"),
                    "path": m.get("path"),
                    "type": "model",  # resource_type not in returned dict
                    "materialization": m.get("materialized"),  # Already at top level
                    "depends_on": m.get("depends_on", []),  # Already a list
                }
                for m in models
            ]

            # Get sources if requested
            if include_sources:
                sources = await asyncio.to_thread(orchestrator.dbt_client.list_sources)
                result["sources"] = [
                    {
                        "name": f"{s.get('source_name')}.{s.get('name')}",
                        "database": s.get("database"),
                        "schema": s.get("schema"),
                        "identifier": s.get("identifier"),
                    }
                    for s in sources
                ]

            # Get tests if requested
            if include_tests:
                tests = await asyncio.to_thread(orchestrator.dbt_client.list_tests)
                result["tests"] = [
                    {
                        "name": t.get("name"),
                        "test_type": t.get("test_type"),  # Already at top level
                        "model": t.get("depends_on", []),  # This is the list of nodes it depends on
                    }
                    for t in tests
                ]

            result["total_models"] = len(result["models"])
            result["total_sources"] = len(result["sources"])
            result["total_tests"] = len(result["tests"])

            logger.info("Listed %d dbt models", result["total_models"])

            return result

        except Exception as e:
            logger.error("Failed to list dbt models: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "models": [],
                "sources": [],
                "tests": [],
            }

    async def _compile_dbt_project(
        models: list[str] | None = None,
        select: str | None = None,
        exclude: str | None = None,
        parse_only: bool = False,
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Compile dbt project.

        Compiles dbt models to SQL without executing them, useful for
        validation and SQL review.

        Args:
            models: Specific models to compile
            select: dbt selection syntax
            exclude: dbt exclusion syntax
            parse_only: Only parse project without compiling

        Returns:
            Dictionary with compilation results
        """
        try:
            logger.info("Compiling dbt project")

            # Build compile arguments
            compile_args = {}
            if models:
                compile_args["models"] = models
            if select:
                compile_args["select"] = select
            if exclude:
                compile_args["exclude"] = exclude

            # Execute compilation
            results = await asyncio.to_thread(
                orchestrator.dbt_client.compile, auth=auth, **compile_args
            )

            # Parse results
            compiled_models = results.get("results", [])
            success_count = sum(1 for r in compiled_models if r.get("status") == "success")
            error_count = sum(1 for r in compiled_models if r.get("status") == "error")

            response = {
                "success": error_count == 0,
                "total_models": len(compiled_models),
                "compiled": success_count,
                "errored": error_count,
                "execution_time": results.get("elapsed_time"),
            }

            # Add compiled SQL paths
            response["compiled_models"] = [
                {
                    "model": r.get("unique_id"),
                    "compiled_path": r.get("compiled_path"),
                }
                for r in compiled_models
                if r.get("status") == "success"
            ]

            # Add error details
            if error_count > 0:
                response["errors"] = [
                    {
                        "model": r.get("unique_id"),
                        "message": r.get("message"),
                    }
                    for r in compiled_models
                    if r.get("status") == "error"
                ]

            # Chained guidance — only on a clean compile.
            if error_count == 0 and len(compiled_models) > 0:
                response["next_steps"] = [
                    (
                        "**1. Run the compiled models**: "
                        "`dbt_execute(command='run')`. **Why**: ``compile`` "
                        "rendered the SQL dbt will execute; the next step is "
                        "to materialize it in Teradata. **Effect**: "
                        "dbt-teradata creates / refreshes views and tables in "
                        "the configured database. **If missing**: skip if you "
                        "needed compiled SQL only for review."
                    ),
                    (
                        "**2. Run + test in one step**: "
                        "`dbt_execute(command='build')`. **Why**: ``build`` "
                        "runs models and their tests in dependency order; "
                        "preferred over separate ``run`` + ``test`` for "
                        "production-ready promotion. **Effect**: dbt "
                        "materializes models then runs schema/data tests. "
                        "**If missing**: use ``run`` + ``test`` separately if "
                        "you want finer control over failures."
                    ),
                ]

            logger.info(
                "dbt compilation complete: %d compiled, %d errored",
                success_count,
                error_count,
            )

            return response

        except Exception as e:
            logger.error("Failed to compile dbt project: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _generate_dbt_models_from_source(
        source_database: str,
        source_tables: list[str],
        target_schema: str,
        model_type: str = "staging",
        include_tests: bool = True,
        target_database: str | None = None,
        select_columns: list[str] | None = None,
        dry_run: bool = False,
        tags: list[str] | None = None,
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Generate dbt transformation models from Teradata source tables for data analysis and reporting.

        Creates dbt staging/transformation models from loaded tables with automatic SQL generation.
        Generates source YAML definitions, model SQL files with column selection, and optional data quality tests.
        Use this after loading data (from CSV or other sources) to create structured transformations ready for analysis.

        Works with any business data: transactions, events, entities, metrics, or dimensional data.

        Args:
            source_database: Source database name (where data was loaded)
            source_tables: List of source table names to transform
            target_schema: Target schema for transformed models
            model_type: Type of models - 'staging' for basic transformations, 'incremental' for large datasets
            include_tests: Generate data quality tests (null checks, uniqueness, referential integrity)
            target_database: Optional target database for materialized models
            auth: Pre-resolved TeradataAuth credentials (optional)

        Returns:
            Dictionary with generation results including model paths and test files
        """
        try:
            logger.info("Generating dbt models from %s", source_database)

            profiles_path = orchestrator.dbt_generator.project_dir / "profiles.yml"
            if not profiles_path.exists():
                if auth:
                    profile_name = orchestrator.dbt_generator.project_dir.name
                    dbt_project_yml = orchestrator.dbt_generator.project_dir / "dbt_project.yml"
                    if dbt_project_yml.exists():
                        with open(dbt_project_yml) as f:
                            project_config = yaml.safe_load(f)
                        if project_config and project_config.get("profile"):
                            profile_name = project_config["profile"]
                    await asyncio.to_thread(
                        orchestrator.dbt_generator.generate_profiles_yml,
                        profile_name=profile_name,
                        auth=auth,
                    )
                    logger.info("Auto-generated profiles.yml for profile: %s", profile_name)

            generated_artifacts = {
                "sources": [],
                "models": [],
                "tests": [],
            }

            if auth is not None:
                td_client = orchestrator.teradata_client_for(auth)
            else:
                td_client = orchestrator.teradata_client

            # Collect all table metadata first
            all_table_metadata = []
            for table in source_tables:
                try:
                    # Get table metadata using asyncio.to_thread to avoid blocking stdio
                    metadata = await asyncio.to_thread(
                        td_client.get_table_metadata,
                        source_database,
                        table,
                        False,  # include_stats=False
                    )

                    all_table_metadata.append(metadata)
                except Exception as e:
                    logger.error("Failed to get metadata for %s: %s", table, e, exc_info=True)
                    generated_artifacts["errors"] = generated_artifacts.get("errors", [])
                    generated_artifacts["errors"].append(
                        {
                            "table": table,
                            "error": safe_error_message(e),
                        }
                    )

            # Generate single source YAML with all tables
            if all_table_metadata:
                try:
                    source_yaml_path = f"models/sources/{source_database}.yml"
                    await asyncio.to_thread(
                        orchestrator.dbt_generator.generate_source_from_teradata_metadata,
                        source_name=source_database,
                        table_metadata_list=all_table_metadata,
                        output_path=None if dry_run else Path(source_yaml_path),
                    )

                    for metadata in all_table_metadata:
                        table_name = metadata.get("table") or metadata.get("table_name")
                        if not table_name:
                            logger.warning(
                                "Skipping metadata with missing table name: %s", metadata
                            )
                            continue

                        generated_artifacts["sources"].append(
                            {
                                "table": table_name,
                                "yaml_path": source_yaml_path,
                            }
                        )
                except Exception as e:
                    logger.error("Failed to generate source YAML: %s", e, exc_info=True)

            # Generate models for each table
            for metadata in all_table_metadata:
                table = metadata.get("table") or metadata.get("table_name")
                if not table:
                    logger.warning("Skipping metadata with missing table name: %s", metadata)
                    continue

                try:
                    # Extract column names from metadata with validation
                    column_names = []
                    for col in metadata.get("columns", []):
                        col_name = col.get("name") or col.get("column_name")
                        if not col_name:
                            logger.warning(
                                "Skipping column with missing name in table %s: %s",
                                table,
                                col,
                            )
                            continue
                        column_names.append(col_name)

                    if select_columns is not None:
                        allowed = {c.lower() for c in select_columns}
                        column_names = [c for c in column_names if c.lower() in allowed]
                        if not column_names:
                            raise ValueError(
                                f"No columns matched the provided select_columns for table '{table}'. "
                                "Verify that the column names exist in the source table."
                            )

                    # Generate and save model SQL
                    model_sql_path = f"models/staging/{source_database}/stg_{table}.sql"
                    effective_path = None if dry_run else Path(model_sql_path)
                    if model_type.lower() == "staging":
                        config_options = {}
                        if target_database:
                            config_options["database"] = target_database

                        model_sql = await asyncio.to_thread(
                            orchestrator.dbt_generator.generate_staging_model,
                            model_name=f"stg_{table}",
                            source_name=source_database,
                            table_name=table,
                            columns=column_names,
                            config_options=config_options if config_options else None,
                            output_path=effective_path,
                            tags=tags,
                        )
                    elif model_type.lower() == "incremental":
                        model_sql = await asyncio.to_thread(
                            orchestrator.dbt_generator.generate_incremental_model,
                            model_name=f"inc_{table}",
                            source_name=source_database,
                            table_name=table,
                            unique_key=(metadata.get("primary_keys", [None])[0] or "id"),
                            columns=column_names,
                            output_path=effective_path,
                            tags=tags,
                        )
                    else:
                        config_options = {}
                        if target_database:
                            config_options["database"] = target_database

                        model_sql = await asyncio.to_thread(
                            orchestrator.dbt_generator.generate_staging_model,
                            model_name=f"stg_{table}",
                            source_name=source_database,
                            table_name=table,
                            columns=column_names,
                            config_options=config_options if config_options else None,
                            output_path=effective_path,
                            tags=tags,
                        )

                    generated_artifacts["models"].append(
                        {
                            "table": table,
                            "model_name": f"stg_{table}",
                            "model_path": str(
                                orchestrator.dbt_generator.project_dir / model_sql_path
                            ),
                            "sql_preview": model_sql,
                        }
                    )

                    # Generate tests if requested
                    if include_tests:
                        selected_column_names_lower = {c.lower() for c in column_names}
                        inferred = orchestrator.dbt_generator.infer_tests_from_metadata(metadata)
                        column_tests = {
                            k: v
                            for k, v in inferred.items()
                            if k.lower() in selected_column_names_lower
                        }
                        column_descriptions = {}
                        for col in metadata.get("columns", []):
                            col_name = col.get("name") or col.get("column_name")
                            if not col_name:
                                continue
                            if col_name.lower() not in selected_column_names_lower:
                                continue
                            if col.get("description"):
                                column_descriptions[col_name] = col.get("description")

                        test_yaml_path = f"models/staging/{source_database}/schema.yml"
                        await asyncio.to_thread(
                            orchestrator.dbt_generator.generate_schema_tests,
                            model_name=f"stg_{table}",
                            column_tests=column_tests,
                            column_descriptions=column_descriptions,
                            model_description=f"Staging model for {table}",
                            output_path=None if dry_run else Path(test_yaml_path),
                        )

                        generated_artifacts["tests"].append(
                            {
                                "model": f"stg_{table}",
                                "test_path": str(
                                    orchestrator.dbt_generator.project_dir / test_yaml_path
                                ),
                            }
                        )

                except Exception as e:
                    logger.error("Failed to generate artifacts for %s: %s", table, e, exc_info=True)
                    generated_artifacts["errors"] = generated_artifacts.get("errors", [])
                    generated_artifacts["errors"].append(
                        {
                            "table": table,
                            "error": safe_error_message(e),
                        }
                    )

            result = {
                "success": len(generated_artifacts.get("errors", [])) == 0,
                "source_database": source_database,
                "target_schema": target_schema,
                "sources_generated": len(generated_artifacts["sources"]),
                "models_generated": len(generated_artifacts["models"]),
                "tests_generated": len(generated_artifacts["tests"]),
                "artifacts": generated_artifacts,
            }
            if dry_run:
                result["dry_run"] = True
            elif result["success"]:
                model_names = [
                    m.get("model_name")
                    for m in generated_artifacts.get("models", [])
                    if m.get("model_name")
                ]
                models_arg = repr(model_names) if model_names else "<list of model names>"
                result["next_steps"] = [
                    (
                        f"**1. Materialize the new models**: "
                        f"`dbt_execute(command='run', models={models_arg})`. "
                        f"**Why**: the SQL files exist on disk but Teradata "
                        f"views/tables aren't created until ``dbt run``. **Effect**: "
                        f"dbt-teradata creates the views in the target schema "
                        f"(``{target_schema}``). **If missing**: if "
                        f"TERADATA_DATABASE is empty in profiles.yml, the run "
                        f"errors with Teradata 3706 — set it via the Setup Wizard "
                        f"first."
                    ),
                    (
                        f"**2. Validate with tests**: "
                        f"`dbt_execute(command='test', models={models_arg})`. "
                        f"**Why**: schema.yml carries not_null tests for every "
                        f"column; running them confirms the materialized data "
                        f"matches the source contract. **Effect**: dbt runs each "
                        f"test as a SELECT against Teradata; failures surface "
                        f"row counts. **If missing**: skip if you used "
                        f"``dbt_execute(command='build')`` instead of run, which "
                        f"runs both."
                    ),
                ]

            logger.info("Generated %d dbt models", result["models_generated"])

            return result

        except Exception as e:
            logger.error("Failed to generate dbt models: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "source_database": source_database,
            }

    async def _build_dbt_project(
        models: list[str] | None = None,
        select: str | None = None,
        exclude: str | None = None,
        full_refresh: bool = False,
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Execute dbt build command (run + test combined).

        Builds and tests models in a single command, more efficient
        than running separately.

        Args:
            models: Specific model names to build
            select: dbt selection syntax
            exclude: dbt exclusion syntax
            full_refresh: Force full refresh of incremental models

        Returns:
            Dictionary with build results
        """
        try:
            logger.info("Building dbt project")

            await _auto_install_deps(orchestrator)

            # Build arguments
            build_args = {}
            if models:
                build_args["models"] = models
            if select:
                build_args["select"] = select
            if exclude:
                build_args["exclude"] = exclude
            if full_refresh:
                build_args["full_refresh"] = full_refresh

            results = await asyncio.to_thread(
                orchestrator.dbt_client.build, auth=auth, **build_args
            )

            # Parse results
            result_summary = results.get("results", [])
            success_count = sum(1 for r in result_summary if r.get("status") == "success")
            error_count = sum(1 for r in result_summary if r.get("status") == "error")

            response = {
                "success": error_count == 0,
                "total_nodes": len(result_summary),
                "succeeded": success_count,
                "errored": error_count,
                "execution_time": results.get("elapsed_time"),
                "results": result_summary,
            }

            # Chained guidance — only on a clean build.
            if error_count == 0 and len(result_summary) > 0:
                response["next_steps"] = [
                    (
                        "**1. Generate dbt docs** (optional): "
                        "`dbt_docs(action='generate')`. **Why**: ``dbt build`` "
                        "ran every model + test successfully — a good moment "
                        "to publish the lineage graph. **Effect**: dbt writes "
                        "``target/manifest.json`` + ``catalog.json`` and the "
                        "response returns a ``dbt docs serve`` command. **If "
                        "missing**: skip for ad-hoc runs."
                    ),
                    (
                        "**2. Schedule the project in Airflow**: "
                        "`pipeline_deploy(action='create_dbt_dag', "
                        "dag_id='<id>', project_name=<project>)`. **Why**: "
                        "a green ``dbt build`` is the standard promotion gate "
                        "to production. **Effect**: generates an Airflow DAG "
                        "that re-runs the project on the schedule you pick. "
                        "**If missing**: skip for development workflows."
                    ),
                ]

            logger.info("dbt build complete: %d succeeded, %d errored", success_count, error_count)

            await asyncio.to_thread(_persist_dbt_run_history, result_summary, "build")

            return response

        except Exception as e:
            logger.error("Failed to build dbt project: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _run_dbt_snapshot(
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Execute dbt snapshot command.

        Executes snapshot models to track slowly changing dimensions (SCD Type 2).

        Returns:
            Dictionary with snapshot results
        """
        try:
            logger.info("Running dbt snapshots")

            results = await asyncio.to_thread(orchestrator.dbt_client.snapshot, auth=auth)

            # Parse results
            result_summary = results.get("results", [])
            success_count = sum(1 for r in result_summary if r.get("status") == "success")
            error_count = sum(1 for r in result_summary if r.get("status") == "error")

            response = {
                "success": error_count == 0,
                "total_snapshots": len(result_summary),
                "succeeded": success_count,
                "errored": error_count,
                "results": result_summary,
            }

            # Chained guidance — only on a clean snapshot run.
            if error_count == 0 and len(result_summary) > 0:
                response["next_steps"] = [
                    (
                        "**1. Run downstream models that read snapshots**: "
                        "`dbt_execute(command='run', select='+snapshot:*')`. "
                        "**Why**: snapshots capture SCD-2 state; the marts "
                        "that join against them need to refresh too. "
                        "**Effect**: dbt rebuilds every model whose lineage "
                        "depends on a snapshot. **If missing**: skip if you "
                        "schedule snapshots independently of models."
                    ),
                    (
                        "**2. Schedule the snapshot in Airflow**: "
                        "`pipeline_deploy(action='create_dbt_dag', "
                        "dag_id='<id>', project_name=<project>, "
                        "schedule='@daily')`. **Why**: SCD-2 history is only "
                        "useful when captured on a regular cadence. "
                        "**Effect**: Airflow runs ``dbt snapshot`` on the "
                        "schedule you pick. **If missing**: skip if you only "
                        "need an ad-hoc historical snapshot."
                    ),
                ]

            logger.info("dbt snapshot complete: %d succeeded", success_count)

            await asyncio.to_thread(_persist_dbt_run_history, result_summary, "snapshot")

            return response

        except Exception as e:
            logger.error("Failed to run dbt snapshots: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _seed_dbt_data(
        select: str | None = None,
        full_refresh: bool = False,
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Execute dbt seed command.

        Loads CSV files from the data/ directory into the database.

        Args:
            select: Optional seed selection
            full_refresh: Force reload of seed data

        Returns:
            Dictionary with seed results
        """
        try:
            logger.info("Loading dbt seed data")

            results = await asyncio.to_thread(
                orchestrator.dbt_client.seed,
                select=select,
                full_refresh=full_refresh,
                auth=auth,
            )

            # Parse results
            result_summary = results.get("results", [])
            success_count = sum(1 for r in result_summary if r.get("status") == "success")
            error_count = sum(1 for r in result_summary if r.get("status") == "error")

            response = {
                "success": error_count == 0,
                "total_seeds": len(result_summary),
                "loaded": success_count,
                "errored": error_count,
                "results": result_summary,
            }

            # Chained guidance — only when seeds loaded cleanly.
            if error_count == 0 and len(result_summary) > 0:
                response["next_steps"] = [
                    (
                        "**1. Run models that depend on seeds**: "
                        "`dbt_execute(command='run', select='+seed:*')`. "
                        "**Why**: seeds populate reference tables; the models "
                        "that join against them must refresh to reflect new "
                        "values. **Effect**: dbt re-runs every model "
                        "downstream of a seed. **If missing**: skip if you "
                        "loaded seeds for ad-hoc analysis only."
                    ),
                    (
                        "**2. Validate with tests**: "
                        "`dbt_execute(command='test', select='seed:*')`. "
                        "**Why**: seed schema YAMLs may declare not_null / "
                        "unique / accepted_values; running tests confirms the "
                        "loaded data matches expectations. **Effect**: "
                        "dbt-teradata runs every test attached to seed nodes. "
                        "**If missing**: skip if your seeds have no tests."
                    ),
                ]

            logger.info("dbt seed complete: %d loaded", success_count)

            await asyncio.to_thread(_persist_dbt_run_history, result_summary, "seed")

            return response

        except Exception as e:
            logger.error("Failed to load seed data: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _clean_dbt_project(
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Execute dbt clean command.

        Removes the target/ directory and all compiled artifacts.

        Returns:
            Dictionary with clean results
        """
        try:
            logger.info("Cleaning dbt project")

            results = await asyncio.to_thread(orchestrator.dbt_client.clean, auth=auth)

            response = {
                "success": results.get("success", False),
                "message": "Target directory cleaned successfully",
            }

            logger.info("dbt clean complete")

            return response

        except Exception as e:
            logger.error("Failed to clean dbt project: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _debug_dbt_connection(
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Run dbt debug to validate the dbt project configuration.

        Checks that dbt can connect to its target warehouse and that
        profiles.yml, dbt_project.yml, and dependencies are correct.
        This does NOT test the MCP server's own Teradata connection —
        use test_teradata_connection for that.

        Returns:
            Dictionary with debug information
        """
        try:
            logger.info("Running dbt debug")

            results = await asyncio.to_thread(orchestrator.dbt_client.debug, auth=auth)

            response = {
                "success": results.get("connection_ok", False),
                "connection_ok": results.get("connection_ok", False),
                "returncode": results.get("returncode"),
                "output": results.get("stdout", ""),
                "errors": results.get("stderr", ""),
                "message": "Connection successful"
                if results.get("connection_ok")
                else "Connection failed - check output for details",
            }

            # Chained guidance — only when the connection check passed.
            if results.get("connection_ok"):
                response["next_steps"] = [
                    (
                        "**1. Compile the project**: "
                        "`dbt_execute(command='compile')`. **Why**: a green "
                        "``dbt debug`` confirms credentials + dependencies; "
                        "compiling validates that every Jinja ref/source "
                        "resolves and the SQL generator is happy. **Effect**: "
                        "dbt writes ``target/manifest.json`` + compiled SQL "
                        "without touching the warehouse. **If missing**: skip "
                        "if you are about to call ``dbt run`` directly."
                    ),
                    (
                        "**2. Run the project**: "
                        "`dbt_execute(command='run')`. **Why**: compile + "
                        "debug both passed, so the next concrete step is to "
                        "materialize models in Teradata. **Effect**: "
                        "dbt-teradata creates/updates views and tables in the "
                        "configured database. **If missing**: skip if you "
                        "wanted only a connection sanity-check."
                    ),
                ]

            logger.info("dbt debug complete: connection_ok=%s", response["connection_ok"])

            return response

        except Exception as e:
            logger.error("Failed to run dbt debug: %s", e, exc_info=True)
            return {
                "success": False,
                "connection_ok": False,
                "error": safe_error_message(e),
            }

    async def _install_dbt_deps(
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Execute dbt deps command.

        Installs dbt package dependencies from packages.yml.

        Returns:
            Dictionary with installation results
        """
        try:
            logger.info("Installing dbt dependencies")

            results = await asyncio.to_thread(orchestrator.dbt_client.deps, auth=auth)

            response = {
                "success": results.get("success", False),
                "message": "Dependencies installed successfully",
                "output": results.get("stdout", ""),
            }

            if results.get("success"):
                response["next_steps"] = [
                    (
                        "**1. Verify package macros load**: "
                        "`dbt_execute(command='parse')`. **Why**: ``dbt deps`` "
                        "downloaded packages but macros only resolve once dbt "
                        "re-parses the project. **Effect**: dbt rebuilds "
                        "``target/manifest.json`` including the new packages. "
                        "**If missing**: skip if you call ``run`` / ``build`` "
                        "next — those re-parse implicitly."
                    ),
                    (
                        "**2. Smoke-test the connection**: "
                        "`dbt_execute(command='debug')`. **Why**: confirms "
                        "the project still connects after a deps change; "
                        "package conflicts can break adapter macros. "
                        "**Effect**: dbt validates profiles.yml + connects "
                        "to Teradata. **If missing**: skip if you have a "
                        "passing run already."
                    ),
                ]

            logger.info("dbt deps complete")

            return response

        except Exception as e:
            logger.error("Failed to install dbt dependencies: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _parse_dbt_project(
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Execute dbt parse command.

        Parses the project and writes manifest.json to the target directory
        without compiling SQL or connecting to the warehouse.

        Returns:
            Dictionary with parse results
        """
        try:
            logger.info("Parsing dbt project")

            results = await asyncio.to_thread(orchestrator.dbt_client.parse, auth=auth)

            response = {
                "success": results.get("success", False),
                "manifest_path": results.get("manifest_path"),
                "message": (
                    "Project parsed successfully"
                    if results.get("success")
                    else "Parse failed — check output for errors"
                ),
                "output": results.get("stdout", ""),
            }
            if results.get("stderr"):
                response["errors"] = results["stderr"]

            if results.get("success"):
                response["next_steps"] = [
                    (
                        "**1. Compile to SQL**: "
                        "`dbt_execute(command='compile')`. **Why**: ``parse`` "
                        "validated Jinja + ref/source resolution; ``compile`` "
                        "renders the actual SQL dbt will execute. **Effect**: "
                        "dbt writes ``target/compiled/**.sql`` for review. "
                        "**If missing**: skip if you only needed an updated "
                        "manifest for tooling."
                    ),
                    (
                        "**2. Run the project**: "
                        "`dbt_execute(command='run')`. **Why**: parse + "
                        "compile both succeeded — the project is ready to "
                        "materialize. **Effect**: dbt-teradata creates / "
                        "refreshes views and tables in the configured "
                        "database. **If missing**: skip if you only wanted "
                        "the updated manifest."
                    ),
                ]

            logger.info("dbt parse complete: success=%s", response["success"])

            return response

        except Exception as e:
            logger.error("Failed to parse dbt project: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _validate_dbt_project(
        auth: TeradataAuth | None = None,
    ) -> dict[str, Any]:
        """
        Validate dbt project configuration and structure.

        Checks project files, connection, and compilation.

        Returns:
            Dictionary with validation results
        """
        try:
            logger.info("Validating dbt project")

            # validate_project runs dbt debug + dbt compile which both need a
            # live Teradata connection. Use the pre-resolved auth so those
            # subcommands get credentials via the sanitized env.
            results = await asyncio.to_thread(
                orchestrator.dbt_client.validate_project, auth=auth
            )

            response = {
                "valid": results.get("valid", False),
                "issues": results.get("issues", []),
                "warnings": results.get("warnings", []),
                "project_dir": results.get("project_dir"),
                "target": results.get("target"),
            }

            logger.info("dbt project validation: valid=%s", response["valid"])

            return response

        except Exception as e:
            logger.error("Failed to validate dbt project: %s", e, exc_info=True)
            return {
                "valid": False,
                "issues": [safe_error_message(e)],
                "warnings": [],
            }

    async def _get_dbt_model_sql(
        model_name: str,
    ) -> dict[str, Any]:
        """
        Get compiled SQL for a specific model.

        Args:
            model_name: Name of the model

        Returns:
            Dictionary with compiled SQL
        """
        try:
            logger.info("Getting compiled SQL for model: %s", model_name)

            sql = await asyncio.to_thread(orchestrator.dbt_client.get_model_sql, model_name)

            if sql:
                response = {
                    "success": True,
                    "model_name": model_name,
                    "compiled_sql": sql,
                }
            else:
                response = {
                    "success": False,
                    "model_name": model_name,
                    "error": "Compiled SQL not found",
                }

            return response

        except Exception as e:
            logger.error("Failed to get model SQL: %s", e, exc_info=True)
            return {
                "success": False,
                "model_name": model_name,
                "error": safe_error_message(e),
            }

    async def _get_dbt_project_info() -> dict[str, Any]:
        """
        Get dbt project information and statistics.

        Returns:
            Dictionary with project details
        """
        try:
            logger.info("Getting dbt project info")

            info = await asyncio.to_thread(orchestrator.dbt_client.get_project_info)

            response = {
                "success": True,
                "name": info.get("name"),
                "version": info.get("version"),
                "profile": info.get("profile"),
                "project_dir": info.get("project_dir"),
                "target": info.get("target"),
                "model_count": info.get("model_count"),
                "source_count": info.get("source_count"),
                "test_count": info.get("test_count"),
            }

            logger.info("Retrieved project info for: %s", response["name"])

            return response

        except Exception as e:
            logger.error("Failed to get project info: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _get_dbt_version() -> dict[str, Any]:
        """
        Get installed dbt version.

        Returns:
            Dictionary with version information
        """
        try:
            logger.info("Getting dbt version")

            version = await asyncio.to_thread(orchestrator.dbt_client.get_dbt_version)

            response = {
                "success": True,
                "version": version,
            }

            logger.info("dbt version: %s", version)

            return response

        except Exception as e:
            logger.error("Failed to get dbt version: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _get_dbt_manifest() -> dict[str, Any]:
        """
        Read and return dbt manifest.json.

        Contains complete project metadata, models, sources, and dependencies.

        Returns:
            Dictionary with manifest data
        """
        try:
            logger.info("Reading dbt manifest")

            manifest = await asyncio.to_thread(orchestrator.dbt_client.get_manifest)

            if manifest:
                response = {
                    "success": True,
                    "manifest": manifest,
                    "metadata": {
                        "generated_at": manifest.get("metadata", {}).get("generated_at"),
                        "dbt_version": manifest.get("metadata", {}).get("dbt_version"),
                    },
                }
            else:
                response = {
                    "success": False,
                    "error": "Manifest not found",
                }

            return response

        except Exception as e:
            logger.error("Failed to read manifest: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _get_dbt_catalog() -> dict[str, Any]:
        """
        Read and return dbt catalog.json.

        Contains database catalog information from docs generation.

        Returns:
            Dictionary with catalog data
        """
        try:
            logger.info("Reading dbt catalog")

            catalog = await asyncio.to_thread(orchestrator.dbt_client.get_catalog)

            if catalog:
                response = {
                    "success": True,
                    "catalog": catalog,
                    "metadata": {
                        "generated_at": catalog.get("metadata", {}).get("generated_at"),
                    },
                }
            else:
                response = {
                    "success": False,
                    "error": "Catalog not found",
                }

            return response

        except Exception as e:
            logger.error("Failed to read catalog: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _get_dbt_run_results() -> dict[str, Any]:
        """
        Read and return dbt run_results.json.

        Contains results from the last dbt execution.

        Returns:
            Dictionary with run results
        """
        try:
            logger.info("Reading dbt run results")

            run_results = await asyncio.to_thread(orchestrator.dbt_client.get_run_results)

            if run_results:
                response = {
                    "success": True,
                    "run_results": run_results,
                    "metadata": {
                        "generated_at": run_results.get("metadata", {}).get("generated_at"),
                    },
                }
            else:
                response = {
                    "success": False,
                    "error": "Run results not found",
                }

            return response

        except Exception as e:
            logger.error("Failed to read run results: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _get_dbt_project_config() -> dict[str, Any]:
        """
        Read and return dbt_project.yml configuration.

        Returns:
            Dictionary with project config
        """
        try:
            logger.info("Reading dbt project config")

            config = await asyncio.to_thread(orchestrator.dbt_client.get_project_config)

            if config:
                response = {
                    "success": True,
                    "config": config,
                    "name": config.get("name"),
                    "version": config.get("version"),
                }
            else:
                response = {
                    "success": False,
                    "error": "Project config not found",
                }

            return response

        except Exception as e:
            logger.error("Failed to read project config: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _get_dbt_profiles_config() -> dict[str, Any]:
        """
        Read and return profiles.yml configuration.

        Sensitive fields (password, token, etc.) are automatically redacted
        before returning to the caller.

        Returns:
            Dictionary with profiles config (credentials masked)
        """
        try:
            logger.info("Reading dbt profiles config")

            config = await asyncio.to_thread(orchestrator.dbt_client.get_profiles_config)

            if config:
                response = {
                    "success": True,
                    "config": config,
                }
            else:
                response = {
                    "success": False,
                    "error": "Profiles config not found",
                }

            return sanitize_response(response)

        except Exception as e:
            logger.error("Failed to read profiles config: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _check_dbt_installation() -> dict[str, Any]:
        """
        Check if dbt and dbt-teradata adapter are installed and accessible.

        Uses a static check that does NOT require a configured dbt project,
        so it works even when dbt is not installed or no project exists.

        Returns:
            Dictionary with installation status, versions, and plugins
        """
        try:
            from ..clients.dbt_client import DBTClient

            logger.info("Checking dbt installation")

            # Static method — no DBTClient instance needed, avoids catch-22
            install_info = await asyncio.to_thread(DBTClient.check_installation)
            installed = install_info.get("installed", False)

            response: dict[str, Any] = {
                "installed": installed,
                "dbt_version": install_info.get("dbt_version"),
                "teradata_installed": install_info.get("teradata_installed", False),
                "teradata_version": install_info.get("teradata_version"),
                "plugins": install_info.get("plugins", {}),
            }

            if installed:
                response["message"] = "dbt is installed and accessible"
                if not install_info.get("teradata_installed"):
                    response["message"] = (
                        "dbt is installed but dbt-teradata adapter is missing. "
                        "Install with: pip install dbt-teradata"
                    )
            else:
                response["message"] = "dbt is not installed. Install with: pip install dbt-teradata"

            logger.info(
                "dbt installation check: installed=%s, teradata=%s",
                installed,
                install_info.get("teradata_installed"),
            )

            return response

        except Exception as e:
            logger.error("Failed to check dbt installation: %s", e, exc_info=True)
            return {
                "installed": False,
                "error": safe_error_message(e),
            }

    async def _generate_intermediate_models(
        source_models: list[str],
        model_name: str,
        join_logic: list[dict[str, Any]] | None = None,
        select_columns: list[str] | None = None,
        where_clause: str | None = None,
        group_by: list[str] | None = None,
        materialization: str = "view",
        post_hook: str | None = None,
        unique_key: str | None = None,
        incremental_strategy: str | None = None,
        incremental_column: str | None = None,
        on_schema_change: str = "fail",
        dry_run: bool = False,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Generate intermediate transformation models that join and enrich multiple data tables.

        Creates business logic layer models that combine staging models with joins, filters, and aggregations.
        Used to prepare enriched datasets by combining related tables before creating final reporting models.
        Applies business rules, data enrichment, and transformations in the middle layer of the dbt project.

        Common use cases: Joining related entities, enriching with lookup data, applying business logic, creating reusable datasets.

        Args:
            source_models: List of staging models to combine
            model_name: Model name describing the transformation (e.g., 'int_entities_enriched')
            join_logic: Join specifications [{"model": "stg_table_a", "type": "left", "on": "a.id = b.ref_id"}]
            select_columns: Columns to include in final output
            where_clause: Filter conditions for data subset
            group_by: Aggregation columns for summary calculations
            materialization: Storage strategy - 'view', 'table', or 'incremental'
            post_hook: Optional post-processing for optimization
            unique_key: Required for incremental with merge/delete+insert - column(s) for merge key
            incremental_strategy: For Teradata: 'append', 'merge', or 'delete+insert'
            incremental_column: Column used for incremental filtering (e.g., 'updated_at')
            on_schema_change: How to handle schema changes ('fail', 'ignore', 'append_new_columns', 'sync_all_columns')

        Returns:
            Dictionary with generation results including model path and configuration
        """
        try:
            logger.info("Generating intermediate model: %s", model_name)

            # Determine output path
            output_path = Path(f"models/intermediate/{model_name}.sql")

            # Generate model
            generated_sql = await asyncio.to_thread(
                orchestrator.dbt_generator.generate_intermediate_model,
                model_name=model_name,
                source_models=source_models,
                join_logic=join_logic,
                select_columns=select_columns,
                where_clause=where_clause,
                group_by=group_by,
                materialization=materialization,
                post_hook=post_hook,
                output_path=None if dry_run else output_path,
                unique_key=unique_key,
                incremental_strategy=incremental_strategy,
                incremental_column=incremental_column,
                on_schema_change=on_schema_change,
                tags=tags,
            )

            result = {
                "success": True,
                "model_name": model_name,
                "model_path": None
                if dry_run
                else str(orchestrator.dbt_generator.project_dir / output_path),
                "materialization": materialization,
                "source_models": source_models,
                "generated_sql": generated_sql,
            }
            if dry_run:
                result["dry_run"] = True

            # Add incremental config to result if applicable
            if materialization == "incremental":
                result["incremental_config"] = {
                    "unique_key": unique_key,
                    "incremental_strategy": incremental_strategy,
                    "incremental_column": incremental_column,
                    "on_schema_change": on_schema_change,
                }

            # Auto-generate companion schema tests
            if not dry_run:
                try:
                    column_tests: dict[str, list[str]] = {}
                    if unique_key:
                        column_tests[unique_key] = ["unique", "not_null"]
                    if select_columns:
                        for col in select_columns:
                            if col not in column_tests:
                                column_tests[col] = ["not_null"]
                    if column_tests:
                        test_path = Path(f"models/intermediate/{model_name}_schema.yml")
                        await asyncio.to_thread(
                            orchestrator.dbt_generator.generate_schema_tests,
                            model_name=model_name,
                            column_tests=column_tests,
                            model_description=f"Intermediate model: {model_name}",
                            output_path=test_path,
                        )
                        result["test_path"] = str(
                            orchestrator.dbt_generator.project_dir / test_path
                        )
                except Exception as te:
                    logger.warning("Failed to generate tests for %s: %s", model_name, te)

            if not dry_run:
                result["next_steps"] = [
                    (
                        f"**1. Materialize the model**: "
                        f"`dbt_execute(command='run', models=[{model_name!r}])`. "
                        f"**Why**: the SQL was written to disk but Teradata "
                        f"views/tables for ``{model_name}`` don't exist until "
                        f"``dbt run`` materializes them. **Effect**: "
                        f"dbt-teradata creates the {materialization} in the "
                        f"configured database. **If missing**: skip if you "
                        f"plan to run a full ``dbt build`` shortly."
                    ),
                    (
                        f"**2. Validate with tests**: "
                        f"`dbt_execute(command='test', models=[{model_name!r}])`. "
                        f"**Why**: the auto-generated schema YAML wires "
                        f"not_null/unique tests on the join key + selected "
                        f"columns; running them is the cheapest way to "
                        f"surface broken joins or NULL leakage. **Effect**: "
                        f"dbt-teradata runs every test attached to "
                        f"``{model_name}``. **If missing**: skip if you set "
                        f"``include_tests=False`` upstream."
                    ),
                ]

            logger.info("Generated intermediate model: %s", model_name)

            return result

        except Exception as e:
            logger.error("Failed to generate intermediate model: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _generate_mart_models(
        source_models: list[str],
        model_name: str,
        model_type: str,
        dimension_columns: list[str] | None = None,
        measure_columns: list[dict[str, str]] | None = None,
        grain: str | None = None,
        materialization: str = "table",
        post_hook: str | None = None,
        mart_category: str = "core",
        unique_key: str | None = None,
        incremental_strategy: str | None = None,
        incremental_column: str | None = None,
        on_schema_change: str = "fail",
        dry_run: bool = False,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Generate business reporting models (data marts) for analytics, dashboards, and BI tools.

        Creates final-layer data models optimized for business analysis and reporting.
        Supports both dimension tables (descriptive attributes) and fact tables (measurable events/transactions).
        Generates SQL with aggregations, metrics, and optimized materialization for query performance.

        Perfect for creating business-ready datasets that answer analytical questions and power dashboards.

        Args:
            source_models: List of upstream staging/intermediate models to build from
            model_name: Mart model name (e.g., 'dim_entities', 'fct_events', 'rpt_summary')
            model_type: Model type - 'dimension' for descriptive/reference data, 'fact' for transactional/event data
            dimension_columns: Descriptive columns for analysis and filtering
            measure_columns: Metrics to calculate [{"name": "total_amount", "agg": "sum(amount)"}]
            grain: Data granularity description (e.g., "One row per entity", "One row per event")
            materialization: Storage strategy ('table', 'view', or 'incremental')
            post_hook: Optional post-processing (e.g., "COLLECT STATS ON {{ this }}")
            mart_category: Business domain folder (core/finance/marketing/sales/operations)
            unique_key: Required for incremental with merge/delete+insert - column(s) for merge key
            incremental_strategy: For Teradata: 'append', 'merge', or 'delete+insert'
            incremental_column: Column used for incremental filtering (e.g., 'updated_at')
            on_schema_change: How to handle schema changes ('fail', 'ignore', 'append_new_columns', 'sync_all_columns')

        Returns:
            Dictionary with generation results including SQL path and model configuration
        """
        try:
            logger.info("Generating mart model: %s (%s)", model_name, model_type)

            # Determine output path
            output_path = Path(f"models/marts/{mart_category}/{model_name}.sql")

            # Generate model
            generated_sql = await asyncio.to_thread(
                orchestrator.dbt_generator.generate_mart_model,
                model_name=model_name,
                model_type=model_type,
                source_models=source_models,
                dimension_columns=dimension_columns,
                measure_columns=measure_columns,
                grain=grain,
                materialization=materialization,
                post_hook=post_hook,
                output_path=None if dry_run else output_path,
                unique_key=unique_key,
                incremental_strategy=incremental_strategy,
                incremental_column=incremental_column,
                on_schema_change=on_schema_change,
                tags=tags,
            )

            result = {
                "success": True,
                "model_name": model_name,
                "model_type": model_type,
                "model_path": None
                if dry_run
                else str(orchestrator.dbt_generator.project_dir / output_path),
                "materialization": materialization,
                "grain": grain,
                "source_models": source_models,
                "generated_sql": generated_sql,
            }
            if dry_run:
                result["dry_run"] = True

            # Add incremental config to result if applicable
            if materialization == "incremental":
                result["incremental_config"] = {
                    "unique_key": unique_key,
                    "incremental_strategy": incremental_strategy,
                    "incremental_column": incremental_column,
                    "on_schema_change": on_schema_change,
                }

            # Auto-generate companion schema tests
            if not dry_run:
                try:
                    column_tests: dict[str, list[str]] = {}
                    if unique_key:
                        column_tests[unique_key] = ["unique", "not_null"]
                    if dimension_columns:
                        for col in dimension_columns:
                            if col not in column_tests:
                                column_tests[col] = ["not_null"]
                    if column_tests:
                        test_path = Path(f"models/marts/{mart_category}/{model_name}_schema.yml")
                        await asyncio.to_thread(
                            orchestrator.dbt_generator.generate_schema_tests,
                            model_name=model_name,
                            column_tests=column_tests,
                            model_description=f"Mart model: {model_name} ({model_type})",
                            output_path=test_path,
                        )
                        result["test_path"] = str(
                            orchestrator.dbt_generator.project_dir / test_path
                        )
                except Exception as te:
                    logger.warning("Failed to generate tests for %s: %s", model_name, te)

            if not dry_run:
                result["next_steps"] = [
                    (
                        f"**1. Materialize the mart**: "
                        f"`dbt_execute(command='run', models=[{model_name!r}])`. "
                        f"**Why**: the SQL was written to disk but Teradata "
                        f"views/tables for ``{model_name}`` don't exist until "
                        f"``dbt run`` materializes them. **Effect**: "
                        f"dbt-teradata creates the {materialization} in the "
                        f"configured database under "
                        f"``models/marts/{mart_category}/``. **If missing**: "
                        f"skip if you plan to run a full ``dbt build`` shortly."
                    ),
                    (
                        f"**2. Validate with tests**: "
                        f"`dbt_execute(command='test', models=[{model_name!r}])`. "
                        f"**Why**: the auto-generated schema YAML wires "
                        f"not_null/unique tests on the unique_key + dimension "
                        f"columns — passing tests is the standard gate before "
                        f"exposing a mart to BI tools. **Effect**: dbt-teradata "
                        f"runs every test attached to ``{model_name}``. **If "
                        f"missing**: skip if the mart has no auto-generated "
                        f"tests (no unique_key + no dimension_columns)."
                    ),
                ]

            logger.info("Generated mart model: %s", model_name)

            return result

        except Exception as e:
            logger.error("Failed to generate mart model: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _create_dbt_project_structure(
        project_name: str,
        include_staging: bool = True,
        include_intermediate: bool = True,
        include_marts: bool = True,
        mart_subfolders: list[str] | None = None,
        include_snapshots: bool = True,
        staging_materialization: str = "view",
        intermediate_materialization: str = "view",
        marts_materialization: str = "table",
        teradata_profile: str | None = None,
        target: str = "dev",
        threads: int = 4,
    ) -> dict[str, Any]:
        """
        Create standard dbt project folder structure.

        Sets up a production-ready dbt project structure with
        staging, intermediate, and marts layers following dbt best practices.
        Also generates profiles.yml in the project directory using the default
        Teradata connection credentials.

        Args:
            project_name: Name of the dbt sub-project (becomes
                ``<workspace>/dbt_project/dbt_<slug>/``). Slugified —
                lowercase + non-alnum→underscore; leading ``dbt_`` is
                auto-stripped. Reserved names (``project``, ``dbt_project``,
                or any slug colliding with the parent container's basename)
                are rejected with ``action_required: rename_project``.
            include_staging: Create staging folder
            include_intermediate: Create intermediate folder
            include_marts: Create marts folder
            mart_subfolders: Optional list of business domain subfolders for marts.
                           Default: None (no subfolders are created).
                           Only provide this parameter if the user explicitly requests specific subfolders.
                           Example values (only if user requests): ["finance", "marketing"], ["sales", "operations"].
            include_snapshots: Create snapshots folder
            staging_materialization: Materialization for staging models (default: 'view').
                                    Options: 'view', 'table', 'ephemeral'. View is recommended for
                                    lightweight transformations that should stay fresh with source data.
            intermediate_materialization: Materialization for intermediate models (default: 'view').
                                         Options: 'view', 'table', 'ephemeral'. Choose 'table' for
                                         complex joins or large datasets to improve query performance.
            marts_materialization: Materialization for marts models (default: 'table').
                                  Options: 'table', 'view', 'incremental'. Table is recommended for
                                  final data products that are frequently queried by BI tools.
            teradata_profile: Connection profile name from connections.yaml for Teradata credentials.
                             If not provided, auto-detects a Teradata profile (td_source, prod_teradata, etc.)
                             or falls back to environment variables. Used to generate profiles.yml.
            target: dbt target environment name (default: 'dev')
            threads: Number of threads for parallel execution (default: 4)

        Returns:
            Dictionary with created paths including profiles.yml
        """
        try:
            logger.info("Creating dbt project structure: %s", project_name)

            # Resolve the Teradata identity via the precedence gate (profile
            # wins if named, else wizard default keyed by host).
            identity = _resolve_teradata_identity(orchestrator, teradata_profile)
            if identity is None:
                return {
                    "success": False,
                    "error": (
                        "No Teradata host is configured. Set TERADATA_HOST via "
                        "the wizard (or .env), or pass a named teradata_profile "
                        "from connections.yaml."
                    ),
                }

            # Resolve which dbt sub-project to scaffold under the parent.
            resolution = _resolve_dbt_subproject(
                parent=orchestrator.dbt_project_parent,
                identity=identity,
                project_name=project_name,
            )
            if resolution.status == "legacy_layout":
                return {
                    "success": False,
                    "error": (
                        "Detected legacy single-project dbt layout at "
                        f"{orchestrator.dbt_project_parent}/dbt_project.yml. "
                        "The new layout puts each Teradata profile in its own "
                        f"sub-project under {orchestrator.dbt_project_parent}/"
                        "dbt_<name>/. Move or delete the legacy files, then "
                        "call dbt_project(action='create_structure', ...) again."
                    ),
                }
            if resolution.status == "name_collision":
                return _collision_response(orchestrator, resolution, project_name)
            if resolution.status == "conflict":
                target_name = (
                    resolution.project_dir.name
                    if resolution.project_dir
                    else f"dbt_{slugify_dir_name(project_name)}"
                )
                return {
                    "success": False,
                    "error": (
                        f"Sub-project '{target_name}' already exists but is "
                        f"bound to identity '{resolution.existing_identity}', "
                        f"not '{resolution.identity}'. Choose a different "
                        "project_name."
                    ),
                }
            assert resolution.project_dir is not None  # ``existing`` or ``will_create``
            # Pin the cached generator to the resolved sub-project for this call.
            orchestrator.dbt_generator = orchestrator.dbt_generator_for(resolution.project_dir)

            if teradata_profile:
                guard = orchestrator.credential_resolver.guard_configured()
                if guard:
                    return guard
            try:
                auth = resolve_teradata_auth(
                    settings=orchestrator.settings.teradata,
                    credential_resolver=orchestrator.credential_resolver,
                    teradata_profile=teradata_profile,
                )
            except ValueError as e:
                return {"success": False, "error": str(e)}
            # dbt requires a default database for model materialization.
            # Refuse to scaffold a profiles.yml that would otherwise render
            # an empty schema and fail at ``create_schema`` later.
            if (db_err := _require_dbt_database(auth)) is not None:
                return db_err

            result = await asyncio.to_thread(
                orchestrator.dbt_generator.create_project_structure,
                project_name=project_name,
                include_staging=include_staging,
                include_intermediate=include_intermediate,
                include_marts=include_marts,
                mart_subfolders=mart_subfolders,
                include_snapshots=include_snapshots,
                staging_materialization=staging_materialization,
                intermediate_materialization=intermediate_materialization,
                marts_materialization=marts_materialization,
                auth=auth,
                target=target,
                threads=threads,
                identity=identity,
            )

            result["project_dir"] = str(orchestrator.dbt_generator.project_dir)
            result["teradata_identity"] = identity

            # Add usage hint for profiles.yml
            if auth is not None:
                profiles_path = orchestrator.dbt_generator.project_dir / "profiles.yml"
                result["profiles_yml_path"] = str(profiles_path)
                result["usage_hint"] = (
                    f"Use 'dbt run --profiles-dir {profiles_path.parent}' "
                    f"or set DBT_PROFILES_DIR={profiles_path.parent}"
                )

            # Chained guidance for the LLM: scaffold is just the start.
            result["next_steps"] = [
                (
                    f"**1. Generate models**: "
                    f"`dbt_generate_model(model_type='staging', "
                    f"source_database='<db>', source_tables=['<t1>', '<t2>'], "
                    f"project_name='{project_name}', "
                    f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                    f"**Why**: the scaffold creates folders + dbt_project.yml + "
                    f"profiles.yml but no models — nothing materializes until "
                    f"models exist. **Effect**: writes "
                    f"``models/staging/<db>/stg_<table>.sql`` plus a sources YAML "
                    f"under the new sub-project. **If missing**: if the user "
                    f"hasn't picked source tables yet, run "
                    f"`teradata_discover(action='find', database='<db>')` first "
                    f"to enumerate candidates."
                ),
                (
                    f"**2. Run the models**: "
                    f"`dbt_execute(command='run', project_name='{project_name}', "
                    f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                    f"**Why**: model SQL files exist on disk but Teradata views/"
                    f"tables aren't created until ``dbt run`` materializes them. "
                    f"**Effect**: dbt-teradata creates views (or tables, per "
                    f"materialization) in the configured ``schema:`` database. "
                    f"**If missing**: if ``schema:`` is empty in profiles.yml the "
                    f"run errors with Teradata 3706 — ask the user to set "
                    f"TERADATA_DATABASE via the Setup Wizard first."
                ),
                (
                    f"**3. Schedule the dbt step in Airflow** (optional): "
                    f"`pipeline_deploy(action='create_dbt_dag', "
                    f"dag_id='<id>', project_name='{project_name}', "
                    f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                    f"**Why**: ad-hoc ``dbt run`` is fine for development; "
                    f"production deployments want a recurring DAG. **Effect**: "
                    f"generates an Airflow DAG that ``dbt run``s this "
                    f"sub-project on a cron. **If missing**: skip this step "
                    f"entirely if the user is iterating locally."
                ),
            ]

            logger.info(
                "Created project structure with %d folders",
                len(result["created_paths"]["folders"]),
            )

            return result

        except Exception as e:
            logger.error("Failed to create project structure: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    def _csv_filename_to_table_name(filename: str) -> str:
        """Derive a safe Teradata/dbt table name from a CSV filename."""
        name = Path(filename).stem
        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        name = name.lower().strip("_")
        if not name:
            raise ValueError(f"Cannot derive table name from filename: {filename}")
        return name

    async def _create_dbt_project_from_csv(
        project_name: str,
        csv_files: list[str],
        approach: str | None = None,
        target_database: str | None = None,
        delimiter: str | None = None,
        teradata_profile: str | None = None,
        target: str = "dev",
        threads: int = 4,
        include_tests: bool = True,
    ) -> dict[str, Any]:
        """Create a dbt project from CSV files.

        Phase 1 (approach is None): Analyze CSVs and return available loading
        approaches based on detected infrastructure.

        Phase 2 (approach is set): Execute the chosen approach to scaffold the
        dbt project and load / reference the CSV data.

        Args:
            project_name: Name of the dbt project
            csv_files: List of CSV file paths
            approach: Loading approach ('dbt_seed', 'tpt_local', 'tpt_airflow')
            target_database: Target Teradata database (required for tpt_* approaches)
            delimiter: CSV delimiter character
            teradata_profile: Connection profile name from connections.yaml
            target: dbt target environment name
            threads: Number of threads for parallel execution
            include_tests: Generate data quality tests

        Returns:
            Dictionary with discovery info or execution results
        """
        try:
            # Validate CSV paths
            for csv_path in csv_files:
                p = Path(csv_path)
                if p.suffix.lower() != ".csv":
                    return {
                        "success": False,
                        "error": f"File must have .csv extension: {csv_path}",
                    }
                if not await asyncio.to_thread(p.exists):
                    return {
                        "success": False,
                        "error": f"CSV file not found: {csv_path}",
                    }

            # Analyze CSVs
            from ..utils.csv_analyzer import CSVAnalyzer

            analyzer = CSVAnalyzer()
            csv_analyses = []
            for csv_path in csv_files:
                analysis = await asyncio.to_thread(
                    analyzer.analyze_csv,
                    file_path=csv_path,
                    delimiter=delimiter,
                )
                csv_analyses.append(analysis)

            # Check infrastructure availability
            ttu_available = orchestrator.settings.ttu.enabled
            airflow_available = orchestrator.settings.airflow.base_url is not None

            # Phase 1: Discovery
            if approach is None:
                available_approaches = [
                    {
                        "name": "dbt_seed",
                        "description": (
                            "Copy CSVs to dbt seeds/ directory and run dbt seed. "
                            "Best for small reference datasets."
                        ),
                    },
                ]
                if ttu_available:
                    available_approaches.append(
                        {
                            "name": "tpt_local",
                            "description": (
                                "Load CSVs into Teradata tables via local tdload, "
                                "then generate dbt project with source YAML + staging models."
                            ),
                        },
                    )
                if ttu_available and airflow_available:
                    available_approaches.append(
                        {
                            "name": "tpt_airflow",
                            "description": (
                                "Generate Airflow DAG(s) using TdLoadOperator to load CSVs "
                                "into Teradata, then generate dbt project with source YAML "
                                "+ staging models."
                            ),
                        },
                    )

                csv_summary = [
                    {
                        "file": a.file_path,
                        "rows": a.row_count,
                        "columns": a.column_count,
                        "size_mb": round(a.file_size_mb, 2),
                    }
                    for a in csv_analyses
                ]

                # Auto-select if only dbt_seed is available
                if len(available_approaches) == 1:
                    approach = "dbt_seed"
                else:
                    return {
                        "success": True,
                        "phase": "discovery",
                        "available_approaches": available_approaches,
                        "csv_summary": csv_summary,
                        "message": (
                            "Multiple loading approaches available. Ask user which to use."
                        ),
                    }

            # Phase 2: Execution
            valid_approaches = ["dbt_seed", "tpt_local", "tpt_airflow"]
            if approach not in valid_approaches:
                return {
                    "success": False,
                    "error": (
                        f"Invalid approach '{approach}'. "
                        f"Valid approaches: {', '.join(valid_approaches)}"
                    ),
                }

            if approach == "dbt_seed":
                # Scaffold project
                scaffold_result = await _create_dbt_project_structure(
                    project_name=project_name,
                    teradata_profile=teradata_profile,
                    target=target,
                    threads=threads,
                )
                if not scaffold_result.get("success"):
                    return scaffold_result

                # Copy CSVs to seeds/
                import shutil

                seeds_dir = orchestrator.dbt_generator.project_dir / "seeds"
                await asyncio.to_thread(seeds_dir.mkdir, parents=True, exist_ok=True)
                copied_files = []
                for csv_path in csv_files:
                    dest = seeds_dir / Path(csv_path).name
                    await asyncio.to_thread(shutil.copy2, csv_path, str(dest))
                    copied_files.append(str(dest))

                # Run dbt seed
                seed_result = await _seed_dbt_data(full_refresh=True)

                response = {
                    "success": seed_result.get("success", False),
                    "project_name": project_name,
                    "approach": "dbt_seed",
                    "scaffold": scaffold_result,
                    "seed_files": copied_files,
                    "seed_result": seed_result,
                    "summary": (
                        f"Project '{project_name}' created with "
                        f"{len(copied_files)} CSV seed files loaded via dbt seed."
                    ),
                }
                if seed_result.get("success"):
                    seed_table_names = [
                        _csv_filename_to_table_name(Path(p).name) for p in copied_files
                    ]
                    response["next_steps"] = [
                        (
                            f"**1. Build models that reference the seeds**: "
                            f"`dbt_execute(command='run', "
                            f"project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                            f"**Why**: seeds populated reference tables but "
                            f"the project has no other models yet — running "
                            f"the project is a no-op until you add staging / "
                            f"marts. **Effect**: dbt-teradata recompiles and "
                            f"materializes anything you add under "
                            f"``models/``. **If missing**: skip if you only "
                            f"needed the raw seed tables loaded."
                        ),
                        (
                            f"**2. Validate seeds with tests**: "
                            f"`dbt_execute(command='test', "
                            f"select={seed_table_names!r}, "
                            f"project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                            f"**Why**: seed schema YAMLs may declare "
                            f"not_null/unique/accepted_values; running tests "
                            f"confirms the loaded data matches expectations. "
                            f"**Effect**: dbt-teradata runs every test "
                            f"attached to seed nodes. **If missing**: skip if "
                            f"your seeds have no tests."
                        ),
                    ]
                return response

            elif approach == "tpt_local":
                if not target_database:
                    return {
                        "success": False,
                        "error": ("target_database is required for approach 'tpt_local'"),
                    }

                # Scaffold project
                scaffold_result = await _create_dbt_project_structure(
                    project_name=project_name,
                    teradata_profile=teradata_profile,
                    target=target,
                    threads=threads,
                )
                if not scaffold_result.get("success"):
                    return scaffold_result

                # Load each CSV into Teradata via tdload. Resolve auth once
                # (honours teradata_profile) and thread it into execute_tdload.
                if teradata_profile:
                    guard = orchestrator.credential_resolver.guard_configured()
                    if guard:
                        return guard
                try:
                    tdload_auth = resolve_teradata_auth(
                        settings=orchestrator.settings.teradata,
                        credential_resolver=orchestrator.credential_resolver,
                        teradata_profile=teradata_profile,
                    )
                except ValueError as e:
                    return {
                        "success": False,
                        "error": str(e),
                    }
                table_names = []
                load_results = []
                for csv_path, _analysis in zip(csv_files, csv_analyses, strict=True):
                    table_name = _csv_filename_to_table_name(
                        Path(csv_path).name,
                    )
                    table_names.append(table_name)
                    load_result = await asyncio.to_thread(
                        orchestrator.ttu_client.execute_tdload,
                        auth=tdload_auth,
                        mode="file_to_table",
                        source_file_name=csv_path,
                        target_table=f"{target_database}.{table_name}",
                        source_text_delimiter=delimiter or ",",
                    )
                    load_results.append(
                        {"table": table_name, "result": load_result},
                    )

                # Generate dbt models from the now-loaded Teradata tables
                target_schema = (
                    await asyncio.to_thread(
                        orchestrator.dbt_client.get_target_schema,
                    )
                    or "staging"
                )
                staging_result = await _generate_dbt_models_from_source(
                    source_database=target_database,
                    source_tables=table_names,
                    target_schema=target_schema,
                    model_type="staging",
                    include_tests=include_tests,
                    auth=tdload_auth,
                )

                response = {
                    "success": staging_result.get("success", False),
                    "project_name": project_name,
                    "approach": "tpt_local",
                    "scaffold": scaffold_result,
                    "load_results": load_results,
                    "staging": staging_result,
                    "summary": (
                        f"Project '{project_name}' created: "
                        f"{len(table_names)} CSVs loaded into "
                        f"{target_database} via tdload, "
                        f"{staging_result.get('models_generated', 0)} "
                        f"staging models generated."
                    ),
                }
                if staging_result.get("success"):
                    staging_model_names = [f"stg_{t}" for t in table_names]
                    response["next_steps"] = [
                        (
                            f"**1. Run the staging models**: "
                            f"`dbt_execute(command='run', "
                            f"models={staging_model_names!r}, "
                            f"project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                            f"**Why**: tdload populated the raw tables in "
                            f"``{target_database}``; staging models were "
                            f"generated on disk but views/tables for them "
                            f"don't exist in Teradata until ``dbt run`` "
                            f"materializes them. **Effect**: dbt-teradata "
                            f"creates views/tables for the staging models. "
                            f"**If missing**: skip if you only needed the raw "
                            f"tables loaded."
                        ),
                        (
                            f"**2. Validate with tests**: "
                            f"`dbt_execute(command='test', "
                            f"models={staging_model_names!r}, "
                            f"project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                            f"**Why**: not_null/unique tests on staging models "
                            f"are the cheapest way to catch CSV-quality "
                            f"problems. **Effect**: dbt-teradata runs every "
                            f"test attached to the staging models. **If "
                            f"missing**: skip if you set ``include_tests=False``."
                        ),
                    ]
                return response

            else:  # tpt_airflow
                if not target_database:
                    return {
                        "success": False,
                        "error": ("target_database is required for approach 'tpt_airflow'"),
                    }

                # Scaffold project
                scaffold_result = await _create_dbt_project_structure(
                    project_name=project_name,
                    teradata_profile=teradata_profile,
                    target=target,
                    threads=threads,
                )
                if not scaffold_result.get("success"):
                    return scaffold_result

                from ..generators.airflow_tdload_dag_generator import (
                    AirflowTdLoadDAGGenerator,
                )

                dags_output_dir = Path(
                    orchestrator.settings.pipeline.dags_output_dir,
                )
                dag_generator = AirflowTdLoadDAGGenerator(
                    dags_folder=dags_output_dir,
                )

                table_names = []
                dag_paths = []
                for csv_path, analysis in zip(csv_files, csv_analyses, strict=True):
                    table_name = _csv_filename_to_table_name(
                        Path(csv_path).name,
                    )
                    table_names.append(table_name)

                    columns = analyzer.get_tpt_column_definitions(analysis)
                    dag_id = f"load_{table_name}_from_csv"
                    await asyncio.to_thread(
                        dag_generator.generate_file_loading_dag,
                        dag_id=dag_id,
                        description=(
                            f"Load {Path(csv_path).name} into {target_database}.{table_name}"
                        ),
                        source_file_path=csv_path,
                        target_database=target_database,
                        target_table=table_name,
                        delimiter=delimiter or ",",
                        columns=columns,
                        skip_rows=1,
                        tags=["csv_loading", "dbt_project", project_name],
                    )
                    dag_paths.append(
                        str(dags_output_dir / f"{dag_id}.py"),
                    )

                # Generate source YAML referencing future tables
                tables_meta = [
                    {
                        "name": tbl,
                        "description": f"Table loaded from CSV: {Path(csv).name}",
                        "columns": [
                            {"name": col.name, "description": ""} for col in analysis.columns
                        ],
                    }
                    for tbl, csv, analysis in zip(
                        table_names,
                        csv_files,
                        csv_analyses,
                        strict=True,
                    )
                ]
                source_yaml_path = f"models/sources/{target_database}.yml"
                await asyncio.to_thread(
                    orchestrator.dbt_generator.generate_source_yaml,
                    source_name=target_database,
                    database=target_database,
                    schema=target_database,
                    tables=tables_meta,
                    output_path=Path(source_yaml_path),
                )

                # Generate staging models
                staging_model_paths = []
                for tbl, analysis in zip(table_names, csv_analyses, strict=True):
                    model_name = f"stg_{tbl}"
                    col_names = [col.name for col in analysis.columns]
                    output_path = Path(
                        f"models/staging/{model_name}.sql",
                    )
                    await asyncio.to_thread(
                        orchestrator.dbt_generator.generate_staging_model,
                        model_name=model_name,
                        source_name=target_database,
                        table_name=tbl,
                        columns=col_names,
                        output_path=output_path,
                    )
                    staging_model_paths.append(str(output_path))

                staging_model_names = [f"stg_{t}" for t in table_names]
                return {
                    "success": True,
                    "project_name": project_name,
                    "approach": "tpt_airflow",
                    "scaffold": scaffold_result,
                    "dag_paths": dag_paths,
                    "source_yaml_path": source_yaml_path,
                    "staging_model_paths": staging_model_paths,
                    "summary": (
                        f"Project '{project_name}' created: "
                        f"{len(dag_paths)} Airflow DAGs generated, "
                        f"{len(staging_model_paths)} staging models created."
                    ),
                    "next_steps": [
                        (
                            "**1. Deploy the load DAGs to Airflow**: "
                            "`pipeline_deploy(action='deploy_dags')`. "
                            "**Why**: DAG files were written locally but "
                            "Airflow won't see them until they are SFTP'd to "
                            "the Airflow ``dags_folder``. **Effect**: copies "
                            "the generated DAGs to the Airflow server and "
                            "they appear in the UI on the next scheduler "
                            "scan. **If missing**: skip if Airflow is "
                            "configured to read from the local DAGs folder "
                            "directly."
                        ),
                        (
                            f"**2. Trigger the load DAGs**: for each DAG id "
                            f"in {[Path(p).stem for p in dag_paths]!r}, run "
                            f"`dag_trigger(mode='run', pipeline_name=<id>)`. "
                            f"**Why**: deployment makes DAGs visible but "
                            f"data only lands once they execute; tdload runs "
                            f"on the Airflow worker. **Effect**: TPT loads "
                            f"the CSVs into "
                            f"``{target_database}.<table>``. **If "
                            f"missing**: skip if you have a separate cron "
                            f"already scheduled."
                        ),
                        (
                            f"**3. Run the staging models**: "
                            f"`dbt_execute(command='run', "
                            f"models={staging_model_names!r}, "
                            f"project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                            f"**Why**: staging model SQL was generated but "
                            f"Teradata views/tables for them don't exist "
                            f"until ``dbt run``. **Effect**: dbt-teradata "
                            f"materializes the staging models in the "
                            f"configured database. **If missing**: skip if "
                            f"the user wants to inspect the loaded raw "
                            f"tables first."
                        ),
                    ],
                }

        except Exception as e:
            logger.error(
                "Failed to create dbt project from CSV: %s",
                e,
                exc_info=True,
            )
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _generate_dbt_profiles_yml(
        profile_name: str,
        auth: TeradataAuth,
        target: str = "dev",
        threads: int = 4,
    ) -> dict[str, Any]:
        """
        Generate dbt profiles.yml in the project directory using credentials from connections.yaml.

        Creates a profiles.yml file in the dbt project directory (not the default ~/.dbt location).
        This enables project-specific profiles that can be used with `dbt run --profiles-dir .`.

        Credentials are resolved from connections.yaml — the LLM never handles passwords.

        Args:
            profile_name: Name of the dbt profile (should match 'profile' in dbt_project.yml)
            auth: Pre-resolved TeradataAuth credentials
            target: dbt target environment name (default: 'dev')
            threads: Number of threads for parallel execution (default: 4)

        Returns:
            Dictionary with generation results including file path and profile configuration
        """
        try:
            logger.info("Generating profiles.yml for profile: %s", profile_name)

            # Generate profiles.yml — credentials stay server-side.
            await asyncio.to_thread(
                orchestrator.dbt_generator.generate_profiles_yml,
                profile_name=profile_name,
                auth=auth,
                target=target,
                threads=threads,
            )

            profiles_path = orchestrator.dbt_generator.project_dir / "profiles.yml"

            return {
                "success": True,
                "profile_name": profile_name,
                "target": target,
                "profiles_path": str(profiles_path),
                "message": (
                    f"Generated profiles.yml at {profiles_path}. "
                    f"Use 'dbt run --profiles-dir {profiles_path.parent}' to use this profile."
                ),
                "usage_hint": (
                    f"Set DBT_PROFILES_DIR={profiles_path.parent} or use --profiles-dir flag"
                ),
                "next_steps": [
                    (
                        "**1. Verify the profile connects**: "
                        "`dbt_execute(command='debug')`. **Why**: a freshly "
                        "generated profiles.yml should be sanity-checked "
                        "against Teradata before relying on it for runs. "
                        "**Effect**: dbt validates the host/port/user from "
                        "the profile and opens a test connection. **If "
                        "missing**: skip if you trust the credentials and "
                        "are about to run ``dbt build`` next."
                    ),
                    (
                        "**2. Run the project**: "
                        "`dbt_execute(command='run')`. **Why**: profiles.yml "
                        "is the missing piece — runs that previously failed "
                        "with ``Could not find profile`` should now work. "
                        "**Effect**: dbt-teradata materializes models in the "
                        "configured database. **If missing**: skip if you "
                        "only generated profiles.yml for an external dbt "
                        "invocation."
                    ),
                ],
            }

        except Exception as e:
            logger.error("Failed to generate profiles.yml: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _generate_incremental_model_advanced(
        source_name: str,
        table_name: str,
        model_name: str,
        columns: list[str],
        unique_key: str,
        incremental_column: str = "updated_at",
        incremental_strategy: str = "merge",
        on_schema_change: str = "fail",
        post_hook: str | None = None,
        dry_run: bool = False,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Generate advanced incremental model with multiple strategies.

        Creates incremental models with support for merge, append,
        delete+insert strategies and schema change handling.

        NOTE: dbt-teradata only supports: 'append', 'merge', 'delete+insert'.
        'insert_overwrite' is NOT supported by Teradata.

        Args:
            source_name: Source name in sources.yml
            table_name: Table name in source
            model_name: Model name (e.g., 'inc_transactions')
            columns: List of column names
            unique_key: Unique key for merge operations
            incremental_column: Column for incremental logic (default: updated_at)
            incremental_strategy: Strategy ('append', 'merge', 'delete+insert')
            on_schema_change: How to handle schema changes ('fail', 'ignore', 'append_new_columns', 'sync_all_columns')
            post_hook: Optional post-hook for stats

        Returns:
            Dictionary with generation results
        """
        try:
            logger.info("Generating advanced incremental model: %s", model_name)

            # Determine output path
            output_path = Path(f"models/staging/{source_name}/{model_name}.sql")

            # Generate model
            generated_sql = await asyncio.to_thread(
                orchestrator.dbt_generator.generate_incremental_model,
                model_name=model_name,
                source_name=source_name,
                table_name=table_name,
                columns=columns,
                unique_key=unique_key,
                incremental_column=incremental_column,
                incremental_strategy=incremental_strategy,
                on_schema_change=on_schema_change,
                config_options={"post-hook": post_hook} if post_hook else None,
                output_path=None if dry_run else output_path,
                tags=tags,
            )

            result = {
                "success": True,
                "model_name": model_name,
                "model_path": None
                if dry_run
                else str(orchestrator.dbt_generator.project_dir / output_path),
                "incremental_strategy": incremental_strategy,
                "on_schema_change": on_schema_change,
                "unique_key": unique_key,
                "generated_sql": generated_sql,
            }
            if dry_run:
                result["dry_run"] = True

            # Auto-generate companion schema tests
            if not dry_run:
                try:
                    column_tests: dict[str, list[str]] = {
                        unique_key: ["unique", "not_null"],
                    }
                    if incremental_column and incremental_column != unique_key:
                        column_tests[incremental_column] = ["not_null"]
                    test_path = Path(f"models/staging/{source_name}/{model_name}_schema.yml")
                    await asyncio.to_thread(
                        orchestrator.dbt_generator.generate_schema_tests,
                        model_name=model_name,
                        column_tests=column_tests,
                        model_description=f"Incremental model: {model_name}",
                        output_path=test_path,
                    )
                    result["test_path"] = str(orchestrator.dbt_generator.project_dir / test_path)
                except Exception as te:
                    logger.warning("Failed to generate tests for %s: %s", model_name, te)

            if not dry_run:
                result["next_steps"] = [
                    (
                        f"**1. Materialize the model** (first full load): "
                        f"`dbt_execute(command='run', models=[{model_name!r}], "
                        f"full_refresh=True)`. **Why**: incremental models need "
                        f"a full-refresh on first run to populate the target "
                        f"table; subsequent runs apply only the deltas. "
                        f"**Effect**: dbt-teradata creates the target table and "
                        f"loads all rows. **If missing**: ``run`` without "
                        f"``full_refresh`` will fail because the target table "
                        f"doesn't exist yet."
                    ),
                    (
                        f"**2. Subsequent incremental runs**: "
                        f"`dbt_execute(command='run', models=[{model_name!r}])`. "
                        f"**Why**: now that the target table exists, dbt only "
                        f"applies rows where ``{incremental_column}`` is newer "
                        f"than the previous high-watermark using the "
                        f"``{incremental_strategy}`` strategy. **Effect**: "
                        f"dbt-teradata merges new/changed rows into the target "
                        f"table. **If missing**: skip if you intend to do only "
                        f"a one-time full load."
                    ),
                ]

            logger.info("Generated incremental model: %s", model_name)

            return result

        except Exception as e:
            logger.error("Failed to generate incremental model: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _generate_snapshot_model(
        source_name: str,
        table_name: str,
        model_name: str,
        target_schema: str,
        unique_key: str,
        strategy: str = "timestamp",
        updated_at: str | None = "updated_at",
        check_cols: list[str] | None = None,
        dry_run: bool = False,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        try:
            logger.info("Generating snapshot model: %s", model_name)
            output_path = Path(f"snapshots/{model_name}.sql")
            generated_sql = await asyncio.to_thread(
                orchestrator.dbt_generator.generate_snapshot,
                snapshot_name=model_name,
                source_name=source_name,
                table_name=table_name,
                target_schema=target_schema,
                unique_key=unique_key,
                strategy=strategy,
                updated_at=updated_at,
                check_cols=check_cols,
                output_path=None if dry_run else output_path,
                tags=tags,
            )
            logger.info("Generated snapshot model: %s", model_name)
            result = {
                "success": True,
                "model_name": model_name,
                "model_path": None
                if dry_run
                else str(orchestrator.dbt_generator.project_dir / output_path),
                "strategy": strategy,
                "unique_key": unique_key,
                "generated_sql": generated_sql,
            }
            if dry_run:
                result["dry_run"] = True

            # Auto-generate companion schema tests
            if not dry_run:
                try:
                    column_tests: dict[str, list[str]] = {
                        unique_key: ["unique", "not_null"],
                    }
                    if strategy == "timestamp" and updated_at:
                        column_tests[updated_at] = ["not_null"]
                    test_path = Path(f"snapshots/{model_name}_schema.yml")
                    await asyncio.to_thread(
                        orchestrator.dbt_generator.generate_schema_tests,
                        model_name=model_name,
                        column_tests=column_tests,
                        model_description=f"Snapshot model: {model_name}",
                        output_path=test_path,
                    )
                    result["test_path"] = str(orchestrator.dbt_generator.project_dir / test_path)
                except Exception as te:
                    logger.warning("Failed to generate tests for %s: %s", model_name, te)

            if not dry_run:
                result["next_steps"] = [
                    (
                        f"**1. Capture the first snapshot**: "
                        f"`dbt_execute(command='snapshot')`. **Why**: the "
                        f"snapshot SQL was written but no SCD-2 history "
                        f"rows exist until ``dbt snapshot`` is executed. "
                        f"**Effect**: dbt-teradata creates the snapshot "
                        f"target table in ``{target_schema}`` and seeds it "
                        f"with the current state of "
                        f"``{source_name}.{table_name}``. **If missing**: "
                        f"downstream models that reference the snapshot "
                        f"will fail with an ``Object not found`` error."
                    ),
                    (
                        "**2. Schedule recurring snapshots in Airflow**: "
                        "`pipeline_deploy(action='create_dbt_dag', "
                        "dag_id='<id>', schedule='@daily')`. **Why**: "
                        "SCD-2 history is only useful when captured on a "
                        "regular cadence; an ad-hoc snapshot loses change "
                        "events between runs. **Effect**: Airflow runs "
                        "``dbt snapshot`` on the schedule you pick. **If "
                        "missing**: skip if you only need a one-time "
                        "snapshot of current state."
                    ),
                ]

            return result
        except Exception as e:
            logger.error("Failed to generate snapshot model: %s", e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ------------------------------------------------------------------ #
    #  Router 1: dbt_execute                                              #
    # ------------------------------------------------------------------ #

    async def dbt_execute(
        command: Literal[
            "run",
            "test",
            "build",
            "compile",
            "snapshot",
            "seed",
            "clean",
            "deps",
            "parse",
            "debug",
            "ls",
        ],
        models: list[str] | None = None,
        select: str | None = None,
        exclude: str | None = None,
        full_refresh: bool = False,
        vars: dict[str, Any] | None = None,
        threads: int | None = None,
        data: bool = True,
        schema: bool = True,
        parse_only: bool = False,
        teradata_profile: str | None = None,
        project_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Execute dbt commands: run, test, build, compile, parse, snapshot, seed, clean, debug, deps.

        IMPORTANT: dbt sub-project resolution is automatic. The tool looks
        up the sub-project bound to the current Teradata identity. If no
        sub-project exists, the tool returns
        ``action_required="ask_project_name"`` — generate models first
        with ``dbt_generate_model``, or ask the user which existing
        sub-project to target and pass ``project_name``. If multiple
        sub-projects share the identity, the tool returns
        ``action_required="disambiguate_project_name"`` with a
        ``candidates`` list — ask the user which one.

        Connection: follows the server's wizard-vs-profile selection policy
        (see the server ``instructions``). Default is the wizard connection
        unless the user names a profile via ``teradata_profile``.

        Pass ``teradata_profile`` to use a specific connections.yaml profile
        for this invocation (mechanism and all fields come from the profile).
        Omit to use the wizard-populated default identity.

        IMPORTANT: Never write or suggest raw dbt CLI shell commands (e.g. "dbt run ...",
        "dbt compile --parse-only", "dbt parse", "dbt test ..."). Always call THIS tool instead.
        Note: "dbt compile --parse-only" is not a valid dbt CLI flag — use command='parse' via
        this tool, which calls the real "dbt parse" command.

        PARSE / VALIDATE intent → always use command='parse':
          Use command='parse' when the user says any of: "parse the dbt project", "parse dbt",
          "run dbt parse", "validate dbt syntax", "validate dbt models", "check if models are valid",
          "validate project structure", "check dbt YAML", "refresh the manifest",
          "regenerate manifest.json", or "check dbt project".
          command='parse' writes manifest.json without compiling SQL or connecting to the warehouse.
          It is faster and safer than compile. No extra params needed.
          Do NOT use command='compile' with parse_only=True for parse/validate prompts — use
          command='parse' instead.

        Workflow note: When a user asks to "generate models AND run them" or "create end-to-end":
          1. First call dbt_generate_model to create the model files.
          2. Then call dbt_execute(command='run', models=[<list of model_name values from
             the generation result's artifacts.models[].model_name field>]) to materialize them.
          Do NOT attempt to write model SQL manually — always use dbt_generate_model first.

        Valid values for 'command':
          - "parse"    : Parse the project and write manifest.json. No extra params.
                         Use for: "parse the dbt project", "validate dbt syntax",
                         "check if models are valid", "refresh the manifest", "check dbt YAML".
          - "run"      : Run dbt models.
                         Use 'models' (list[str]) for explicit model names: ["stg_customers"].
                         Use 'select' (str) for dbt graph/tag syntax: "tag:daily", "path:models/staging+".
                         Do NOT provide both — 'models' takes precedence and 'select' is ignored.
                         Params: models, select, exclude, full_refresh, vars, threads
          - "test"     : Run dbt tests. Params: models, select, exclude, data, schema
          - "build"    : Build dbt project (run + test). Params: models, select, exclude, full_refresh
          - "compile"  : Compile dbt project to SQL (renders Jinja, requires warehouse).
                         If parse_only=True is passed, redirects to 'parse' behaviour
                         (writes manifest.json, no SQL rendering, no warehouse connection).
                         Params: models, select, exclude, parse_only
          - "snapshot" : Run dbt snapshots (SCD Type 2). No extra params.
          - "seed"     : Load CSV seed data. Params: select, full_refresh
          - "clean"    : Remove target/ directory. No extra params.
          - "debug"    : Validate dbt project configuration. No extra params.
          - "deps"     : Install dbt package dependencies. No extra params.

        Args:
            command: The dbt command to execute (see valid values above)
            models: Specific model names (for run, test, build, compile)
            select: dbt selection syntax (for run, test, build, compile, seed)
            exclude: dbt exclusion syntax (for run, test, build, compile)
            full_refresh: Force full refresh (for run, build, seed)
            vars: Variables to pass to dbt --vars (for run)
            threads: Number of threads (for run)
            data: Include data tests (for test)
            schema: Include schema tests (for test)
            parse_only: When True with command='compile', redirects to dbt parse (no SQL rendering)

        Returns:
            Dictionary with command execution results
        """
        if not isinstance(command, str) or not command.strip():
            return {"success": False, "error": "Parameter 'command' must be a non-empty string."}
        if threads is not None and threads < 1:
            return {"success": False, "error": "Parameter 'threads' must be >= 1."}
        command = command.strip().lower()
        try:
            valid_commands = [
                "run",
                "test",
                "build",
                "compile",
                "snapshot",
                "seed",
                "clean",
                "debug",
                "deps",
                "parse",
            ]
            if command not in valid_commands:
                return {
                    "success": False,
                    "error": (
                        f"Invalid command '{command}'. Valid commands: {', '.join(valid_commands)}"
                    ),
                }

            # ── Resolve per-Teradata-profile dbt sub-project ────────
            # Same pattern as dbt_generate_model. ``deps``/``clean`` operate
            # on filesystem only and don't need a specific sub-project, but
            # they still need ONE — pin to the resolved sub-project so they
            # touch the right ``dbt_packages/`` and ``target/`` directories.
            identity = _resolve_teradata_identity(orchestrator, teradata_profile)
            resolution = _resolve_dbt_subproject(
                parent=orchestrator.dbt_project_parent,
                identity=identity,
                project_name=project_name,
            )
            if resolution.status == "legacy_layout":
                return {
                    "success": False,
                    "error": (
                        "Detected legacy single-project dbt layout at "
                        f"{orchestrator.dbt_project_parent}/dbt_project.yml. "
                        "The new layout puts each Teradata profile in its own "
                        f"sub-project under {orchestrator.dbt_project_parent}/"
                        "dbt_<name>/. Move or delete the legacy files."
                    ),
                }
            if resolution.status == "no_identity":
                return {
                    "success": False,
                    "error": (
                        "No Teradata host is configured. Set TERADATA_HOST via "
                        "the wizard (or .env), or pass a named teradata_profile."
                    ),
                }
            if resolution.status == "needs_name":
                return {
                    "success": False,
                    "action_required": "ask_project_name",
                    "message": (
                        "No dbt sub-project exists yet for Teradata identity "
                        f"'{resolution.identity}'. Generate models first with "
                        "dbt_generate_model, or ask the user which existing "
                        "sub-project to operate on and pass project_name."
                    ),
                    "teradata_identity": resolution.identity,
                }
            if resolution.status == "ambiguous":
                return {
                    "success": False,
                    "action_required": "disambiguate_project_name",
                    "message": (
                        "Multiple dbt sub-projects exist for Teradata identity "
                        f"'{resolution.identity}'. Ask the user which project "
                        "to operate on, then call again with project_name set."
                    ),
                    "teradata_identity": resolution.identity,
                    "candidates": [p.name for p in resolution.matches],
                }
            if resolution.status == "name_collision":
                return _collision_response(orchestrator, resolution, project_name)
            if resolution.status == "conflict":
                return {
                    "success": False,
                    "error": (
                        f"Sub-project for project_name='{project_name}' is "
                        f"bound to identity '{resolution.existing_identity}', "
                        f"not '{resolution.identity}'. Choose a different "
                        "project_name or teradata_profile."
                    ),
                }
            assert resolution.project_dir is not None
            orchestrator.dbt_client = orchestrator.dbt_client_for(resolution.project_dir)
            orchestrator.dbt_generator = orchestrator.dbt_generator_for(resolution.project_dir)

            # Resolve auth once at the tool boundary — profile wins fully if
            # named, else wizard default. Each handler forwards ``auth`` to
            # its dbt_client call so the subprocess env carries the right
            # TERADATA_* values. Clean-fail commands that connect to the DB
            # (everything except ``clean``/``deps``/``parse``) when auth can't
            # be resolved — DBTClient would otherwise strip TERADATA_* and
            # leave dbt with no credentials, producing a confusing
            # driver-level error downstream.
            db_connecting_commands = {
                "run",
                "test",
                "build",
                "compile",
                "snapshot",
                "seed",
                "debug",
            }
            if teradata_profile:
                guard = orchestrator.credential_resolver.guard_configured()
                if guard:
                    return guard
            try:
                auth = resolve_teradata_auth(
                    settings=orchestrator.settings.teradata,
                    credential_resolver=orchestrator.credential_resolver,
                    teradata_profile=teradata_profile,
                )
            except ValueError as e:
                if command in db_connecting_commands:
                    return {"success": False, "error": str(e)}
                auth = None
            # dbt requires a default database for model materialization.
            # ``clean``/``deps``/``parse`` don't connect, so they bypass.
            if command in db_connecting_commands:
                if (db_err := _require_dbt_database(auth)) is not None:
                    return db_err

            if command == "run":
                return await _run_dbt_models(
                    models=models,
                    select=select,
                    exclude=exclude,
                    full_refresh=full_refresh,
                    vars=vars,
                    threads=threads,
                    auth=auth,
                )
            elif command == "test":
                return await _test_dbt_models(
                    models=models,
                    select=select,
                    exclude=exclude,
                    data=data,
                    schema=schema,
                    auth=auth,
                )
            elif command == "build":
                return await _build_dbt_project(
                    models=models,
                    select=select,
                    exclude=exclude,
                    full_refresh=full_refresh,
                    auth=auth,
                )
            elif command == "compile":
                if parse_only:
                    return await _parse_dbt_project(auth=auth)
                return await _compile_dbt_project(
                    models=models,
                    select=select,
                    exclude=exclude,
                    auth=auth,
                )
            elif command == "snapshot":
                return await _run_dbt_snapshot(auth=auth)
            elif command == "seed":
                return await _seed_dbt_data(
                    select=select,
                    full_refresh=full_refresh,
                    auth=auth,
                )
            elif command == "clean":
                return await _clean_dbt_project(auth=auth)
            elif command == "debug":
                return await _debug_dbt_connection(auth=auth)
            elif command == "deps":
                return await _install_dbt_deps(auth=auth)
            elif command == "parse":
                return await _parse_dbt_project(auth=auth)

        except Exception as e:
            logger.error("Failed to execute dbt command '%s': %s", command, e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    # ------------------------------------------------------------------ #
    #  Router 2: dbt_docs                                                 #
    # ------------------------------------------------------------------ #

    async def dbt_docs(
        action: Literal["generate", "generate_schema"],
        compile_first: bool = True,
        port: int = 8080,
        # generate_schema params
        models: list[dict[str, Any]] | None = None,
        teradata_profile: str | None = None,
        project_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Generate dbt documentation and schema YAML.

        Connection: follows the server's wizard-vs-profile selection policy
        (see the server ``instructions``). Default is the wizard connection
        unless the user names a profile via ``teradata_profile``. Only the
        ``generate`` action connects to Teradata; ``generate_schema`` is
        pure YAML serialisation.

        Valid values for 'action':
          - "generate"        : Generate dbt documentation site. Params: compile_first, port
          - "generate_schema" : Generate schema YAML with model/column descriptions.
                                Required: models (list of model definition dicts).
                                Each model dict should have: name, description, columns
                                (list of {name, description, data_type, tests}).
                                The schema YAML is always written to a server-derived
                                location: ``<project_dir>/models/_generated/schema.yml``.

        Args:
            action: The documentation action to perform (see valid values above)
            compile_first: Whether to compile project before generating docs (for generate)
            port: Port to include in the returned serve_command (for generate)
            models: List of model definition dicts (for generate_schema). Each dict:
                    {"name": "model_name", "description": "...",
                     "columns": [{"name": "col", "description": "...", "tests": ["not_null"]}]}
            teradata_profile: Optional connections.yaml profile name for the
                Teradata identity. Honored for ``generate`` (which runs a dbt
                subprocess); silently ignored for ``generate_schema`` which
                is pure YAML serialization and does not connect to Teradata.

        Returns:
            Dictionary with documentation results
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        if not (1 <= port <= 65535):
            return {"success": False, "error": "Parameter 'port' must be between 1 and 65535."}
        action = action.strip().lower()
        try:
            valid_actions = ["generate", "generate_schema"]
            if action not in valid_actions:
                return {
                    "success": False,
                    "error": (
                        f"Invalid action '{action}'. Valid actions: {', '.join(valid_actions)}"
                    ),
                }

            # Resolve sub-project (same pattern as dbt_execute /
            # dbt_generate_model). ``generate_schema`` is pure YAML and
            # technically doesn't need to connect, but it still writes
            # under ``<project_dir>/models/_generated/schema.yml`` so it
            # needs a sub-project context.
            identity = _resolve_teradata_identity(orchestrator, teradata_profile)
            resolution = _resolve_dbt_subproject(
                parent=orchestrator.dbt_project_parent,
                identity=identity,
                project_name=project_name,
            )
            if resolution.status == "legacy_layout":
                return {
                    "success": False,
                    "error": (
                        "Detected legacy single-project dbt layout at "
                        f"{orchestrator.dbt_project_parent}/dbt_project.yml. "
                        "Move or delete the legacy files and create a sub-project."
                    ),
                }
            if resolution.status == "no_identity":
                return {
                    "success": False,
                    "error": (
                        "No Teradata host is configured. Set TERADATA_HOST or "
                        "pass a named teradata_profile."
                    ),
                }
            if resolution.status == "needs_name":
                return {
                    "success": False,
                    "action_required": "ask_project_name",
                    "message": (
                        "No dbt sub-project exists for Teradata identity "
                        f"'{resolution.identity}'. Generate models first or "
                        "pass project_name to target an existing sub-project."
                    ),
                    "teradata_identity": resolution.identity,
                }
            if resolution.status == "ambiguous":
                return {
                    "success": False,
                    "action_required": "disambiguate_project_name",
                    "message": (
                        "Multiple dbt sub-projects exist for identity "
                        f"'{resolution.identity}'. Pass project_name."
                    ),
                    "teradata_identity": resolution.identity,
                    "candidates": [p.name for p in resolution.matches],
                }
            if resolution.status == "name_collision":
                return _collision_response(orchestrator, resolution, project_name)
            if resolution.status == "conflict":
                return {
                    "success": False,
                    "error": (
                        f"Sub-project for project_name='{project_name}' is bound "
                        f"to '{resolution.existing_identity}', not "
                        f"'{resolution.identity}'."
                    ),
                }
            assert resolution.project_dir is not None
            orchestrator.dbt_client = orchestrator.dbt_client_for(resolution.project_dir)
            orchestrator.dbt_generator = orchestrator.dbt_generator_for(resolution.project_dir)

            if action == "generate":
                # dbt docs generate introspects the Teradata catalog via
                # teradatasql, so auth is required. Fail fast here — same
                # contract as the DB-connecting commands in dbt_execute.
                if teradata_profile:
                    guard = orchestrator.credential_resolver.guard_configured()
                    if guard:
                        return guard
                try:
                    auth = resolve_teradata_auth(
                        settings=orchestrator.settings.teradata,
                        credential_resolver=orchestrator.credential_resolver,
                        teradata_profile=teradata_profile,
                    )
                except ValueError as e:
                    return {"success": False, "error": str(e)}
                return await _generate_dbt_docs(
                    compile_first=compile_first,
                    port=port,
                    auth=auth,
                )

            elif action == "generate_schema":
                if not models:
                    return {
                        "success": False,
                        "error": "models is required for action 'generate_schema'",
                    }
                server_output_path = Path("models/_generated/schema.yml")
                yaml_content = await asyncio.to_thread(
                    orchestrator.dbt_generator.generate_model_documentation,
                    models=models,
                    output_path=server_output_path,
                )
                return {
                    "success": True,
                    "generated_yaml": yaml_content,
                    "output_path": str(server_output_path),
                    "models_documented": len(models),
                }

        except Exception as e:
            logger.error("Failed to perform dbt docs action '%s': %s", action, e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    # ------------------------------------------------------------------ #
    #  Router 3: dbt_info                                                 #
    # ------------------------------------------------------------------ #

    async def dbt_info(
        info_type: Literal[
            "version",
            "project_info",
            "model_sql",
            "manifest",
            "catalog",
            "run_results",
            "project_config",
            "profiles_config",
            "check_installation",
            "list_models",
            "validate_project",
            "runtime_estimate",
            "clear_runtime_history",
            "project_defaults",
        ],
        model_name: str | None = None,
        model_type: str | None = None,
        include_sources: bool = False,
        include_tests: bool = False,
        teradata_profile: str | None = None,
        project_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve dbt project information, metadata, and configuration.

        IMPORTANT — MANDATORY TOOL USE POLICY:
          Never read dbt-related files directly and never run dbt shell commands,
          even if you already know the file path or directory. This applies to ALL
          of the following — regardless of context:
            - dbt_project.yml         → use info_type="project_config"
            - profiles.yml            → use info_type="profiles_config"
            - target/manifest.json    → use info_type="manifest"
            - target/catalog.json     → use info_type="catalog"
            - target/run_results.json → use info_type="run_results"
            - `dbt --version`         → use info_type="version"
            - `dbt ls` / `dbt list`   → use info_type="list_models"
            - Any dbt project file    → use the appropriate info_type below

          Direct file reads bypass credential masking, return unstructured data,
          and are not reproducible across sub-project layouts. Always call this tool.

        USE THIS TOOL when the user asks any of:
          "tell me about my dbt project", "show my dbt project", "project details",
          "project overview", "describe the project", "what's in my project",
          "what models do I have", "list my models", "show models", "list all models",
          "show staging models", "show mart models", "show intermediate models",
          "what tests do I have", "list tests", "show tests",
          "what sources are configured", "show sources",
          "show project config", "what's in dbt_project.yml",
          "show profiles", "what's my database connection",
          "show the manifest", "lineage", "dependencies", "model dependencies",
          "show catalog", "column types", "schema info",
          "show run results", "last run details",
          "is dbt installed", "what version of dbt", "check dbt installation",
          "is my project valid", "validate my project",
          "how long do my models take", "model runtime", "runtime estimate".

        RECOMMENDED SEQUENCE for a project overview:
          1. dbt_info(info_type="project_defaults")    — discover sub-projects (always safe, no auth)
          2. dbt_info(info_type="project_info")        — name, version, counts
             dbt_info(info_type="list_models",         — full model + source + test list
                      include_sources=True,
                      include_tests=True)
             (Steps 2-3 can run in parallel)
          Add dbt_info(info_type="project_config") only if user asks specifically about config.

          DO NOT call validate_project unless user explicitly asks "is my project valid" or
          is debugging failures — it runs live dbt debug + dbt compile against Teradata.

        Valid values for 'info_type':
          - "project_defaults"      : Discover workspace layout and existing sub-projects.
                                      Returns: dbt_project_parent, workspace_dir,
                                      default_project_name, suggested_project_names,
                                      reserved_names, teradata_identity, existing_subprojects.
                                      Project-independent — always safe to call first.
          - "project_info"          : Overview: name, version, profile, target,
                                      model_count, source_count, test_count.
          - "list_models"           : All models with materialization and dependencies.
                                      Params: model_type (layer filter e.g. "staging"),
                                      include_sources, include_tests.
          - "model_sql"             : Compiled SQL for a specific model (triggers dbt compile).
                                      Params: model_name (required).
          - "project_config"        : Full dbt_project.yml as structured dict.
          - "profiles_config"       : profiles.yml contents — credentials masked automatically.
          - "manifest"              : Full manifest.json — lineage, dependencies, node graph.
                                      Requires prior dbt compile or run.
          - "catalog"               : Full catalog.json — column types, schema.
                                      Requires prior dbt docs generate.
          - "run_results"           : Full run_results.json — last run status per model.
                                      Requires prior dbt run.
          - "validate_project"      : Runs dbt debug + dbt compile against live Teradata.
                                      Only call when user explicitly asks to validate.
          - "runtime_estimate"      : Historical runtime stats (average, median, p95, trend).
                                      Params: model_name (optional filter).
          - "clear_runtime_history" : Purge all stored runtime history records.
          - "version"               : Installed dbt version. Project-independent.
          - "check_installation"    : dbt + teradata adapter status + plugin list.
                                      Project-independent.

        Args:
            info_type: The type of information to retrieve (see valid values above)
            model_name: Model name — required for model_sql, optional filter for runtime_estimate
            model_type: Filter models by layer: "staging", "intermediate", "marts", etc.
            include_sources: Include dbt sources in list_models output (default False)
            include_tests: Include dbt tests in list_models output (default False)
            teradata_profile: Named connections.yaml profile for sub-project resolution.
                              Omit to use the wizard-populated default identity.
            project_name: Target a specific sub-project by name when multiple exist.
                          Omit when only one sub-project exists (auto-resolved).

        Returns:
            Dictionary with requested information
        """
        if not isinstance(info_type, str) or not info_type.strip():
            return {"success": False, "error": "Parameter 'info_type' must be a non-empty string."}
        info_type = info_type.strip().lower()
        try:
            valid_info_types = [
                "version",
                "project_info",
                "model_sql",
                "manifest",
                "catalog",
                "run_results",
                "project_config",
                "profiles_config",
                "check_installation",
                "list_models",
                "validate_project",
                "runtime_estimate",
                "clear_runtime_history",
                "project_defaults",
            ]
            if info_type not in valid_info_types:
                return {
                    "success": False,
                    "error": (
                        f"Invalid info_type '{info_type}'. "
                        f"Valid info_types: {', '.join(valid_info_types)}"
                    ),
                }

            # Project-independent info_types short-circuit before
            # sub-project resolution. ``project_defaults`` is read-only
            # (just inspects settings + filesystem) so it bypasses the
            # resolver too — its whole purpose is to tell the LLM what
            # the resolver WOULD do given the current state.
            project_independent = {
                "version",
                "check_installation",
                "project_defaults",
            }
            if info_type not in project_independent:
                identity = _resolve_teradata_identity(orchestrator, teradata_profile)
                resolution = _resolve_dbt_subproject(
                    parent=orchestrator.dbt_project_parent,
                    identity=identity,
                    project_name=project_name,
                )
                if resolution.status == "legacy_layout":
                    return {
                        "success": False,
                        "error": (
                            "Detected legacy single-project dbt layout at "
                            f"{orchestrator.dbt_project_parent}/dbt_project.yml. "
                            "Move or delete the legacy files."
                        ),
                    }
                if resolution.status == "no_identity":
                    return {
                        "success": False,
                        "error": (
                            "No Teradata host is configured. Set TERADATA_HOST or "
                            "pass a named teradata_profile."
                        ),
                    }
                if resolution.status == "needs_name":
                    return {
                        "success": False,
                        "action_required": "ask_project_name",
                        "message": (
                            "No dbt sub-project exists for Teradata identity "
                            f"'{resolution.identity}'. Pass project_name to "
                            "target an existing sub-project."
                        ),
                        "teradata_identity": resolution.identity,
                    }
                if resolution.status == "ambiguous":
                    return {
                        "success": False,
                        "action_required": "disambiguate_project_name",
                        "message": (
                            "Multiple dbt sub-projects exist for identity "
                            f"'{resolution.identity}'. Pass project_name."
                        ),
                        "teradata_identity": resolution.identity,
                        "candidates": [p.name for p in resolution.matches],
                    }
                if resolution.status == "name_collision":
                    return _collision_response(orchestrator, resolution, project_name)
                if resolution.status == "conflict":
                    return {
                        "success": False,
                        "error": (
                            f"Sub-project for project_name='{project_name}' is "
                            f"bound to '{resolution.existing_identity}', not "
                            f"'{resolution.identity}'."
                        ),
                    }
                assert resolution.project_dir is not None
                orchestrator.dbt_client = orchestrator.dbt_client_for(resolution.project_dir)
                orchestrator.dbt_generator = orchestrator.dbt_generator_for(resolution.project_dir)

            if info_type == "version":
                return await _get_dbt_version()
            elif info_type == "project_info":
                return await _get_dbt_project_info()
            elif info_type == "model_sql":
                if not model_name:
                    return {
                        "success": False,
                        "error": "model_name is required for info_type 'model_sql'",
                    }
                return await _get_dbt_model_sql(model_name=model_name)
            elif info_type == "manifest":
                return await _get_dbt_manifest()
            elif info_type == "catalog":
                return await _get_dbt_catalog()
            elif info_type == "run_results":
                return await _get_dbt_run_results()
            elif info_type == "project_config":
                return await _get_dbt_project_config()
            elif info_type == "profiles_config":
                return await _get_dbt_profiles_config()
            elif info_type == "check_installation":
                return await _check_dbt_installation()
            elif info_type == "list_models":
                return await _list_dbt_models(
                    model_type=model_type,
                    include_sources=include_sources,
                    include_tests=include_tests,
                )
            elif info_type == "validate_project":
                if teradata_profile:
                    guard = orchestrator.credential_resolver.guard_configured()
                    if guard:
                        return guard
                try:
                    validate_auth = resolve_teradata_auth(
                        settings=orchestrator.settings.teradata,
                        credential_resolver=orchestrator.credential_resolver,
                        teradata_profile=teradata_profile,
                    )
                except ValueError as e:
                    return {"success": False, "error": str(e)}
                return await _validate_dbt_project(auth=validate_auth)
            elif info_type == "runtime_estimate":
                return await _estimate_model_runtime(model_name=model_name)
            elif info_type == "clear_runtime_history":
                return await _clear_runtime_history()
            elif info_type == "project_defaults":
                return _get_project_defaults(orchestrator, teradata_profile=teradata_profile)

        except Exception as e:
            logger.error("Failed to get dbt info '%s': %s", info_type, e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    # ------------------------------------------------------------------ #
    #  Router 4: dbt_generate_model                                       #
    # ------------------------------------------------------------------ #

    async def dbt_generate_model(
        model_type: Literal["staging", "intermediate", "mart", "incremental", "snapshot"],
        # staging params
        source_database: str | None = None,
        source_tables: list[str] | None = None,
        source_table: str | None = None,
        target_schema: str | None = None,
        include_tests: bool = True,
        target_database: str | None = None,
        # intermediate params
        source_models: list[str] | None = None,
        model_name: str | None = None,
        join_logic: list[dict[str, Any]] | None = None,
        select_columns: list[str] | None = None,
        where_clause: str | None = None,
        group_by: list[str] | None = None,
        materialization: str | None = None,
        post_hook: str | None = None,
        unique_key: str | None = None,
        incremental_strategy: str | None = None,
        incremental_column: str | None = None,
        on_schema_change: str = "fail",
        # mart params
        dimension_columns: list[str] | None = None,
        measure_columns: list[dict[str, str]] | None = None,
        grain: str | None = None,
        mart_category: str = "core",
        # incremental params
        source_name: str | None = None,
        table_name: str | None = None,
        columns: list[str] | None = None,
        # snapshot params
        snapshot_strategy: str = "timestamp",
        updated_at: str | None = "updated_at",
        check_cols: list[str] | None = None,
        # dry-run mode
        dry_run: bool = False,
        # tags
        tags: list[str] | None = None,
        # LLM-hallucinated aliases (accepted to prevent validation errors)
        action: str | None = None,
        teradata_profile: str | None = None,
        # per-Teradata-profile sub-project selection
        project_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Generate dbt transformation models of various types.

        IMPORTANT — This tool does NOT accept an 'action' parameter.
        That belongs to other tools (dbt_project, teradata_discover).
        Do NOT pass action= here.

        IMPORTANT: dbt sub-project resolution is automatic. The tool looks
        up the sub-project bound to the current Teradata identity (a
        named connections.yaml profile, or the wizard-default keyed by
        host). If no sub-project exists yet, the tool returns
        ``action_required="ask_project_name"`` — relay the prompt to the
        user and use their answer verbatim as ``project_name``. Do NOT
        invent a name from the profile name, schema name, or anything else.

        IMPORTANT: If multiple sub-projects exist for the same Teradata
        identity, the tool returns
        ``action_required="disambiguate_project_name"`` with a
        ``candidates`` list — ask the user which one to operate on,
        then call again with ``project_name`` set.

        Required parameter: model_type (one of: staging, intermediate, mart, incremental, snapshot)

        Quick reference — exact parameter names per model_type:
          staging:      model_type='staging', source_database=str, source_tables=[str] (or source_table=str)
          intermediate: model_type='intermediate', source_models=[str], model_name=str
          mart:         model_type='mart', source_models=[str], model_name=str
          incremental:  model_type='incremental', source_name=str, table_name=str, model_name=str
          snapshot:     model_type='snapshot', source_name=str, table_name=str, model_name=str

        ELT Pipeline Workflow — Sequential Prompts Required:
          This tool handles ONLY the transformation layer (dbt model generation).
          It does NOT configure data extraction, deploy DAGs, or trigger pipelines.
          For end-to-end ELT pipelines, guide the user through these steps sequentially
          (one prompt per step):

          Step 1 — Discover source metadata:
            teradata_discover(action='describe', database='...', tables=['...'])
          Step 2 — Configure data transfer (choose one):
            a) Airbyte: airbyte_pipeline(action='create', source_type='...', ...)
            b) CSV/TPT: airflow_teradata_load(method='csv_dag', ...)
            c) Table transfer: airflow_teradata_load(method='table_transfer', ...)
          Step 3 — Generate Airflow DAG (if Airbyte):
            pipeline_deploy(action='create_sync_dag', connection_id='...', ...)
          Step 4 — Deploy to Airflow:
            pipeline_deploy(action='deploy_dags', pipeline_name='...', ...)
          Step 5 — Trigger data transfer:
            dag_trigger(mode='run', dag_id='...', ...)
          Step 6 — Generate dbt models (THIS TOOL):
            dbt_generate_model(model_type='staging', source_database='...', ...)
          Step 7 — Execute dbt transformations:
            dbt_execute(command='run', models=[...])

          IMPORTANT: Do NOT generate dbt models before data transfer is configured
          and deployed. Each step should be a separate user prompt in the same session.

        Valid values for 'model_type':
          - "staging"      : Generate staging models from Teradata source tables.
                             Required: source_database, source_tables (or source_table for a single table).
                             Optional: target_schema, include_tests, target_database, select_columns.
                             target_schema: auto-resolved from profiles.yml (outputs[target].schema)
                               if omitted; falls back to 'staging' if profile is unavailable.
                             source_tables: bare table names WITHOUT the database prefix —
                               e.g., source_database='sales_db', source_tables=['raw_customers'].
                             select_columns: restrict generated model to specific columns
                               (e.g., ['customer_id','email','status']); omit to include all.
                               Matching is case-insensitive. Column order in the generated
                               model follows the source table schema, not the order of
                               select_columns.
                             Workflow: after generating, call dbt_execute(command='run',
                               models=[<model names from result artifacts.models[].model_name>])
                               to materialize the models.
          - "intermediate" : Generate SQL transformation models that JOIN, FILTER, or AGGREGATE
                             existing dbt models. Choose this when the prompt describes:
                             filtering rows (WHERE conditions), joining models, grouping/aggregating,
                             or adding calculated columns/expressions.
                             NOT for reading raw source tables directly.
                             Required: source_models (list of upstream dbt model names), model_name.
                             Optional: where_clause (e.g., "status='premium' OR lifetime_value > 3000"),
                                       join_logic, select_columns, group_by,
                                       materialization (default: 'view'), post_hook, unique_key,
                                       incremental_strategy, incremental_column, on_schema_change.
          - "mart"         : Generate final business-facing reporting models (dimensions or facts).
                             Choose this when the prompt describes a "reporting table", "dashboard
                             model", or final aggregated output for analytics.
                             Prefix model_name with 'dim_' for dimension models or 'fct_' for
                             fact models — the type is inferred automatically from the prefix.
                             Required: source_models, model_name.
                             Optional: dimension_columns, measure_columns, grain,
                                       materialization (default: 'table'), post_hook, mart_category,
                                       unique_key, incremental_strategy, incremental_column, on_schema_change.
          - "incremental"  : Generate an incremental model (merge/append) for large datasets.
                             Required: source_name, table_name, model_name.
                             Optional (auto-discovered from metadata when omitted):
                               - columns: auto-populated from Teradata metadata if omitted.
                                 Plain column names ONLY — SQL expressions not supported.
                                 Invalid columns are silently dropped; response includes
                                 corrections_applied.
                               - unique_key: auto-detected from primary key if omitted.
                             Other optional: incremental_column (default: 'updated_at'),
                                       incremental_strategy (default: 'merge'),
                                       on_schema_change (default: 'fail'), post_hook.
          - "snapshot"     : Generate a dbt snapshot model. Column auto-correction applies:
                             unique_key, updated_at, and check_cols are validated against
                             real metadata and auto-corrected when possible.
                             Params: source_name, table_name,
                             model_name, target_schema (auto-resolved if omitted), unique_key,
                             snapshot_strategy ("timestamp"|"check"), updated_at, check_cols

        Args:
            model_type: The type of model to generate (see valid values above)
            source_database: Source database name (for staging)
            source_tables: Bare table names WITHOUT the database prefix (for staging).
                           Pass ['raw_customers','raw_orders'], not ['sales_db.raw_customers'].
            target_schema: Target schema for generated models (for staging, snapshot).
                           Auto-resolved from the active dbt profile if not provided;
                           falls back to 'staging' (staging) or 'snapshots' (snapshot).
                           Must contain only letters, digits, and underscores (^[A-Za-z0-9_]+$).
            include_tests: Generate data quality tests (for staging)
            target_database: Optional target database (for staging)
            source_models: List of upstream models (for intermediate, mart)
            model_name: Name for the generated model (for intermediate, mart, incremental, snapshot).
                        Must contain only letters, digits, and underscores (^[A-Za-z0-9_]+$).
            join_logic: Join specifications (for intermediate)
            select_columns: Column filter (for staging and intermediate). For staging: restricts
                            the generated model to only the listed columns. Pass None (or omit)
                            to include all columns. An empty list [] is rejected as an error.
                            Column order follows the source table schema, not the order of
                            select_columns — e.g., select_columns=['D','B'] on a table with
                            columns [A,B,C,D] produces [B,D].
            where_clause: Filter conditions (for intermediate)
            group_by: Aggregation columns (for intermediate)
            materialization: Storage strategy (for intermediate, mart)
            post_hook: Post-processing hook (for intermediate, mart, incremental)
            unique_key: Merge key column(s) (for intermediate, mart, incremental, snapshot).
                        Must contain only letters, digits, and underscores (^[A-Za-z0-9_]+$).
            incremental_strategy: Strategy for incremental (for intermediate, mart, incremental)
            incremental_column: Column for incremental filtering (for intermediate, mart, incremental)
            on_schema_change: Schema change handling (for intermediate, mart, incremental)
            dimension_columns: Descriptive columns (for mart)
            measure_columns: Metrics to calculate (for mart)
            grain: Data granularity description (for mart)
            mart_category: Business domain folder (for mart).
                           Must contain only letters, digits, and underscores (^[A-Za-z0-9_]+$).
            source_name: Source name in sources.yml (for incremental, snapshot).
                         Must contain only letters, digits, and underscores (^[A-Za-z0-9_]+$).
            table_name: Table name in source (for incremental, snapshot).
                        Must contain only letters, digits, and underscores (^[A-Za-z0-9_]+$).
            columns: List of column names (for incremental)
            snapshot_strategy: Snapshot strategy ('timestamp' or 'check'; default 'timestamp')
            updated_at: Timestamp column for timestamp strategy (default 'updated_at').
                        Must be a non-empty string when snapshot_strategy='timestamp';
                        must contain only letters, digits, and underscores (^[A-Za-z0-9_]+$);
                        ignored and cleared for snapshot_strategy='check'.
            check_cols: Column list for check strategy; None or ["all"] checks every column.
                        Each entry must contain only letters, digits, and underscores (^[A-Za-z0-9_]+$).
                        If "all" is present it must be the sole entry — ["all", "col1"] is rejected.
            dry_run: When True, generate SQL in memory but skip file writes. Returns
                     generated_sql and metadata_validation without creating any files.
                     Useful for previewing what would be generated before committing.
            tags: Optional list of dbt tags for selective execution (e.g., ['daily', 'finance']).
                  Tags appear in the generated {{ config() }} block. For mart models, user tags
                  are merged with the auto-generated tags (model_type, 'mart').

        Returns:
            Dictionary with model generation results. When dry_run=True, includes
            'generated_sql' with the SQL content and 'dry_run': True.
        """
        if source_table and not source_tables:
            source_tables = [source_table]
        if not isinstance(model_type, str) or not model_type.strip():
            return {"success": False, "error": "Parameter 'model_type' must be a non-empty string."}
        if select_columns is not None and not select_columns:
            return {
                "success": False,
                "error": "select_columns must not be empty; pass None to include all columns.",
            }
        model_type = model_type.strip().lower()
        try:
            valid_model_types = ["staging", "intermediate", "mart", "incremental", "snapshot"]
            if model_type not in valid_model_types:
                return {
                    "success": False,
                    "error": (
                        f"Invalid model_type '{model_type}'. "
                        f"Valid model_types: {', '.join(valid_model_types)}"
                    ),
                }

            # Resolve Teradata auth once for all model generation operations
            if teradata_profile:
                guard = orchestrator.credential_resolver.guard_configured()
                if guard:
                    return guard
            try:
                auth = resolve_teradata_auth(
                    settings=orchestrator.settings.teradata,
                    credential_resolver=orchestrator.credential_resolver,
                    teradata_profile=teradata_profile,
                )
            except ValueError as e:
                return {"success": False, "error": str(e)}

            # ── Resolve per-Teradata-profile dbt sub-project ────────
            #
            # Each Teradata identity (named ``connections.yaml`` profile or
            # wizard-default keyed by host) gets its own sub-project under
            # ``<workspace>/dbt_project/dbt_<name>/``. We never auto-derive
            # ``project_name`` from the profile name — the user picks it on
            # first creation. Subsequent calls reuse the existing sub-project
            # by matching ``dbt_project.yml::profile`` against the identity.
            #
            # Dry-run skips resolution and scaffolding (no filesystem side
            # effects); callers may dry-run before workspace setup.
            if not dry_run:
                identity = _resolve_teradata_identity(orchestrator, teradata_profile)
                resolution = _resolve_dbt_subproject(
                    parent=orchestrator.dbt_project_parent,
                    identity=identity,
                    project_name=project_name,
                )
                if resolution.status == "legacy_layout":
                    return {
                        "success": False,
                        "error": (
                            "Detected legacy single-project dbt layout at "
                            f"{orchestrator.dbt_project_parent}/dbt_project.yml. "
                            "The new layout puts each Teradata profile in its own "
                            f"sub-project under {orchestrator.dbt_project_parent}/"
                            "dbt_<name>/. Move or delete the legacy files, then "
                            "call dbt_generate_model again with project_name set."
                        ),
                    }
                if resolution.status == "no_identity":
                    return {
                        "success": False,
                        "error": (
                            "No Teradata host is configured. Set TERADATA_HOST via "
                            "the wizard (or .env), or pass a named teradata_profile "
                            "from connections.yaml."
                        ),
                    }
                if resolution.status == "needs_name":
                    return {
                        "success": False,
                        "action_required": "ask_project_name",
                        "message": (
                            "No dbt sub-project exists yet for Teradata identity "
                            f"'{resolution.identity}'. Ask the user what to name "
                            "the new dbt sub-project (e.g. 'analytics', "
                            "'warehouse_prod', 'sales'), then call this tool again "
                            "with project_name set. Do NOT invent a name."
                        ),
                        "teradata_identity": resolution.identity,
                        "directory_layout_preview": (
                            f"{orchestrator.dbt_project_parent}/dbt_<your_choice>/"
                        ),
                    }
                if resolution.status == "ambiguous":
                    return {
                        "success": False,
                        "action_required": "disambiguate_project_name",
                        "message": (
                            "Multiple dbt sub-projects exist for Teradata identity "
                            f"'{resolution.identity}'. Ask the user which project "
                            "to operate on, then call again with project_name set."
                        ),
                        "teradata_identity": resolution.identity,
                        "candidates": [p.name for p in resolution.matches],
                    }
                if resolution.status == "name_collision":
                    return _collision_response(orchestrator, resolution, project_name)
                if resolution.status == "conflict":
                    target_name = (
                        resolution.project_dir.name
                        if resolution.project_dir
                        else f"dbt_{slugify_dir_name(project_name or '')}"
                    )
                    return {
                        "success": False,
                        "error": (
                            f"Sub-project '{target_name}' already exists but is "
                            f"bound to identity '{resolution.existing_identity}', "
                            f"not '{resolution.identity}'. Choose a different "
                            "project_name."
                        ),
                    }
                # ``existing`` or ``will_create`` — pin the cached generator
                # and client to the resolved sub-project so existing helper
                # code reads/writes the right paths. Re-pinning on every
                # call is cheap and self-correcting (next call resolves
                # again).
                assert resolution.project_dir is not None
                orchestrator.dbt_generator = orchestrator.dbt_generator_for(resolution.project_dir)
                # Scaffold or fill in any missing pieces.
                expected_dirs = [
                    "models/staging",
                    "models/intermediate",
                    "models/marts",
                    "snapshots",
                    "tests",
                    "macros",
                    "seeds",
                ]
                project_dir = resolution.project_dir
                dbt_project_yml = project_dir / "dbt_project.yml"
                needs_scaffold = (
                    resolution.status == "will_create"
                    or not dbt_project_yml.exists()
                    or any(not (project_dir / d).exists() for d in expected_dirs)
                )
                if needs_scaffold:
                    # Refuse to scaffold if no Teradata default database is
                    # configured — the rendered profiles.yml would have an
                    # empty ``schema:`` and dbt would fail at create_schema.
                    if (db_err := _require_dbt_database(auth)) is not None:
                        return db_err

                    # Strip the ``dbt_`` prefix from the slug to derive the
                    # dbt project ``name:`` field.
                    scaffold_project_name = project_dir.name
                    if scaffold_project_name.startswith("dbt_"):
                        scaffold_project_name = scaffold_project_name[4:]
                    logger.info(
                        "Scaffolding sub-project %s with identity %s",
                        project_dir,
                        resolution.identity,
                    )
                    scaffold_result = await asyncio.to_thread(
                        orchestrator.dbt_generator.create_project_structure,
                        project_name=scaffold_project_name,
                        identity=resolution.identity,
                        auth=auth,
                    )
                    if not scaffold_result.get("success"):
                        return {
                            "success": False,
                            "error": "Auto-scaffold failed: "
                            + scaffold_result.get("error", "unknown"),
                        }

                # Pin dbt_client to project dir after scaffold completes
                orchestrator.dbt_client = orchestrator.dbt_client_for(resolution.project_dir)

            if model_type == "staging":
                if not source_database:
                    return {
                        "success": False,
                        "error": "source_database is required for model_type 'staging'",
                    }
                if not source_tables:
                    return {
                        "success": False,
                        "error": "source_tables is required for model_type 'staging'",
                    }
                if target_schema is None or target_schema == "":
                    target_schema = (
                        await asyncio.to_thread(orchestrator.dbt_client.get_target_schema)
                        or "staging"
                    )
                if err := _validate_dbt_identifier("target_schema", target_schema):
                    return err
                return await _generate_dbt_models_from_source(
                    source_database=source_database,
                    source_tables=source_tables,
                    target_schema=target_schema,
                    model_type="staging",
                    include_tests=include_tests,
                    target_database=target_database,
                    select_columns=select_columns,
                    dry_run=dry_run,
                    tags=tags,
                    auth=auth,
                )

            elif model_type == "intermediate":
                if not source_models:
                    return {
                        "success": False,
                        "error": "source_models is required for model_type 'intermediate'",
                    }
                if not model_name:
                    return {
                        "success": False,
                        "error": "model_name is required for model_type 'intermediate'",
                    }
                if err := _validate_dbt_identifier("model_name", model_name):
                    return err
                if unique_key is not None:
                    if err := _validate_dbt_identifier("unique_key", unique_key):
                        return err
                if incremental_column is not None:
                    if err := _validate_dbt_identifier("incremental_column", incremental_column):
                        return err
                # --- Best-effort upstream model column auto-correction ---
                int_corrections: list[dict[str, Any]] = []
                upstream_cols = await _resolve_upstream_model_columns(source_models)
                if upstream_cols:
                    all_upstream = []
                    for cols in upstream_cols.values():
                        all_upstream.extend(cols)
                    all_upstream_lower = {c.lower(): c for c in all_upstream}

                    if select_columns:
                        valid_sc = [
                            all_upstream_lower.get(c.lower(), c)
                            for c in select_columns
                            if c.lower() in all_upstream_lower
                        ]
                        invalid_sc = [
                            c for c in select_columns if c.lower() not in all_upstream_lower
                        ]
                        if invalid_sc:
                            int_corrections.append(
                                {
                                    "field": "select_columns",
                                    "action": "removed_invalid_columns",
                                    "removed_columns": invalid_sc,
                                    "kept_columns": valid_sc,
                                }
                            )
                            select_columns = valid_sc if valid_sc else None

                    if group_by:
                        valid_gb = [
                            all_upstream_lower.get(c.lower(), c)
                            for c in group_by
                            if c.lower() in all_upstream_lower
                        ]
                        invalid_gb = [c for c in group_by if c.lower() not in all_upstream_lower]
                        if invalid_gb:
                            int_corrections.append(
                                {
                                    "field": "group_by",
                                    "action": "removed_invalid_columns",
                                    "removed_columns": invalid_gb,
                                    "kept_columns": valid_gb,
                                }
                            )
                            group_by = valid_gb if valid_gb else None

                int_result = await _generate_intermediate_models(
                    source_models=source_models,
                    model_name=model_name,
                    join_logic=join_logic,
                    select_columns=select_columns,
                    where_clause=where_clause,
                    group_by=group_by,
                    materialization=materialization or "view",
                    post_hook=post_hook,
                    unique_key=unique_key,
                    incremental_strategy=incremental_strategy,
                    incremental_column=incremental_column,
                    on_schema_change=on_schema_change,
                    dry_run=dry_run,
                    tags=tags,
                )
                if int_result.get("success") and int_corrections:
                    int_result["corrections_applied"] = int_corrections
                if int_result.get("success") and upstream_cols:
                    int_result["available_columns"] = upstream_cols
                return int_result

            elif model_type == "mart":
                if not source_models:
                    return {
                        "success": False,
                        "error": "source_models is required for model_type 'mart'",
                    }
                if not model_name:
                    return {
                        "success": False,
                        "error": "model_name is required for model_type 'mart'",
                    }
                if err := _validate_dbt_identifier("model_name", model_name):
                    return err
                if err := _validate_dbt_identifier("mart_category", mart_category):
                    return err
                if unique_key is not None:
                    if err := _validate_dbt_identifier("unique_key", unique_key):
                        return err
                if incremental_column is not None:
                    if err := _validate_dbt_identifier("incremental_column", incremental_column):
                        return err
                # Determine mart model type from the model_name prefix or default to 'dimension'
                mart_model_type = "dimension"
                if model_name.startswith("fct_"):
                    mart_model_type = "fact"
                elif model_name.startswith("dim_"):
                    mart_model_type = "dimension"
                # --- Best-effort upstream model column auto-correction ---
                mart_corrections: list[dict[str, Any]] = []
                mart_upstream_cols = await _resolve_upstream_model_columns(source_models)
                if mart_upstream_cols and dimension_columns:
                    all_upstream = []
                    for cols in mart_upstream_cols.values():
                        all_upstream.extend(cols)
                    all_upstream_lower = {c.lower(): c for c in all_upstream}

                    valid_dc = [
                        all_upstream_lower.get(c.lower(), c)
                        for c in dimension_columns
                        if c.lower() in all_upstream_lower
                    ]
                    invalid_dc = [
                        c for c in dimension_columns if c.lower() not in all_upstream_lower
                    ]
                    if invalid_dc:
                        mart_corrections.append(
                            {
                                "field": "dimension_columns",
                                "action": "removed_invalid_columns",
                                "removed_columns": invalid_dc,
                                "kept_columns": valid_dc,
                            }
                        )
                        dimension_columns = valid_dc if valid_dc else None

                mart_result = await _generate_mart_models(
                    source_models=source_models,
                    model_name=model_name,
                    model_type=mart_model_type,
                    dimension_columns=dimension_columns,
                    measure_columns=measure_columns,
                    grain=grain,
                    materialization=materialization or "table",
                    post_hook=post_hook,
                    mart_category=mart_category,
                    unique_key=unique_key,
                    incremental_strategy=incremental_strategy,
                    incremental_column=incremental_column,
                    on_schema_change=on_schema_change,
                    dry_run=dry_run,
                    tags=tags,
                )
                if mart_result.get("success") and mart_corrections:
                    mart_result["corrections_applied"] = mart_corrections
                if mart_result.get("success") and mart_upstream_cols:
                    mart_result["available_columns"] = mart_upstream_cols
                return mart_result

            elif model_type == "incremental":
                if not source_name:
                    return {
                        "success": False,
                        "error": "source_name is required for model_type 'incremental'",
                    }
                if not table_name:
                    return {
                        "success": False,
                        "error": "table_name is required for model_type 'incremental'",
                    }
                if not model_name:
                    return {
                        "success": False,
                        "error": "model_name is required for model_type 'incremental'",
                    }
                if err := _validate_dbt_identifier("model_name", model_name):
                    return err
                if err := _validate_dbt_identifier("source_name", source_name):
                    return err
                if err := _validate_dbt_identifier("table_name", table_name):
                    return err

                # --- Metadata-driven auto-correction ---
                corrections_applied: list[dict[str, Any]] = []
                metadata = await _resolve_source_metadata(
                    source_name, table_name, teradata_profile=teradata_profile
                )
                metadata_info: dict[str, Any] | None = None

                if metadata:
                    real_col_names = [c["name"] for c in metadata.get("columns", [])]
                    pks = metadata.get("primary_keys", [])
                    metadata_info = {
                        "validated": True,
                        "available_columns": real_col_names,
                        "primary_keys": pks,
                    }

                    # Auto-discover columns if not provided
                    if not columns:
                        columns = real_col_names
                        corrections_applied.append(
                            {
                                "field": "columns",
                                "action": "auto_discovered_from_metadata",
                                "columns": real_col_names,
                            }
                        )
                    else:
                        # Auto-correct provided columns
                        columns, col_correction = _autocorrect_columns(columns, metadata)
                        if col_correction:
                            col_correction["field"] = "columns"
                            corrections_applied.append(col_correction)

                    # Auto-detect unique_key from PK if not provided
                    if not unique_key:
                        if pks:
                            unique_key = pks[0]
                            corrections_applied.append(
                                {
                                    "field": "unique_key",
                                    "action": "auto_detected_from_primary_key",
                                    "value": pks[0],
                                }
                            )
                    else:
                        # Auto-correct provided unique_key
                        corrected_uk, uk_correction = _autocorrect_single_column(
                            unique_key, metadata, "unique_key"
                        )
                        if corrected_uk is None:
                            available = uk_correction["available_columns"]
                            return {
                                "success": False,
                                "error": (
                                    f"unique_key '{unique_key}' not found in table "
                                    f"'{table_name}' and could not be auto-corrected "
                                    f"(no primary key detected). "
                                    f"Available columns: {available}"
                                ),
                            }
                        if uk_correction:
                            uk_correction["field"] = "unique_key"
                            corrections_applied.append(uk_correction)
                        unique_key = corrected_uk

                    # Auto-correct incremental_column if provided
                    if incremental_column:
                        corrected_ic, ic_correction = _autocorrect_single_column(
                            incremental_column, metadata, "incremental_column"
                        )
                        if corrected_ic is None:
                            available = ic_correction["available_columns"]
                            return {
                                "success": False,
                                "error": (
                                    f"incremental_column '{incremental_column}' not found "
                                    f"in table '{table_name}' and could not be "
                                    f"auto-corrected (no timestamp column detected). "
                                    f"Available columns: {available}"
                                ),
                            }
                        if ic_correction:
                            ic_correction["field"] = "incremental_column"
                            corrections_applied.append(ic_correction)
                        incremental_column = corrected_ic
                else:
                    metadata_info = {"validated": False}

                # Fall back to requiring columns/unique_key when metadata unavailable
                if not columns:
                    return {
                        "success": False,
                        "error": "columns is required for model_type 'incremental'",
                    }
                if not unique_key:
                    return {
                        "success": False,
                        "error": "unique_key is required for model_type 'incremental'",
                    }
                if err := _validate_dbt_identifier("unique_key", unique_key):
                    return err
                if incremental_column is not None:
                    if err := _validate_dbt_identifier("incremental_column", incremental_column):
                        return err
                result = await _generate_incremental_model_advanced(
                    source_name=source_name,
                    table_name=table_name,
                    model_name=model_name,
                    columns=columns,
                    unique_key=unique_key,
                    incremental_column=incremental_column or "updated_at",
                    incremental_strategy=incremental_strategy or "merge",
                    on_schema_change=on_schema_change,
                    post_hook=post_hook,
                    dry_run=dry_run,
                    tags=tags,
                )
                if result.get("success") and corrections_applied:
                    result["corrections_applied"] = corrections_applied
                if result.get("success") and metadata_info:
                    result["metadata_validation"] = metadata_info
                return result

            elif model_type == "snapshot":
                if not source_name:
                    return {
                        "success": False,
                        "error": "source_name is required for model_type 'snapshot'",
                    }
                if not table_name:
                    return {
                        "success": False,
                        "error": "table_name is required for model_type 'snapshot'",
                    }
                if not model_name:
                    return {
                        "success": False,
                        "error": "model_name is required for model_type 'snapshot'",
                    }
                if not unique_key:
                    return {
                        "success": False,
                        "error": "unique_key is required for model_type 'snapshot'",
                    }
                if err := _validate_dbt_identifier("model_name", model_name):
                    return err
                if err := _validate_dbt_identifier("source_name", source_name):
                    return err
                if err := _validate_dbt_identifier("table_name", table_name):
                    return err
                if not isinstance(snapshot_strategy, str) or not snapshot_strategy.strip():
                    return {
                        "success": False,
                        "error": "snapshot_strategy must be a non-empty string ('timestamp' or 'check')",
                    }
                snapshot_strategy = snapshot_strategy.strip().lower()
                valid_strategies = ["timestamp", "check"]
                if snapshot_strategy not in valid_strategies:
                    return {
                        "success": False,
                        "error": (
                            f"Invalid snapshot_strategy '{snapshot_strategy}'. "
                            f"Valid values: {', '.join(valid_strategies)}"
                        ),
                    }
                if snapshot_strategy == "timestamp" and (
                    not isinstance(updated_at, str) or not updated_at.strip()
                ):
                    return {
                        "success": False,
                        "error": (
                            "updated_at is required for snapshot_strategy 'timestamp'; "
                            "pass a non-empty column name or switch to snapshot_strategy='check'"
                        ),
                    }
                if snapshot_strategy == "timestamp":
                    updated_at = updated_at.strip()  # type: ignore[union-attr]
                if snapshot_strategy == "check":
                    if check_cols is not None:
                        if not isinstance(check_cols, list):
                            return {
                                "success": False,
                                "error": (
                                    "check_cols must be a list of column names or None; "
                                    "received a non-list value"
                                ),
                            }
                        if not check_cols:
                            return {
                                "success": False,
                                "error": (
                                    "check_cols must not be empty; "
                                    'pass None or ["all"] to check every column'
                                ),
                            }
                        if "all" in check_cols and check_cols != ["all"]:
                            return {
                                "success": False,
                                "error": (
                                    "'all' must be the only entry in check_cols; "
                                    'pass ["all"] to check every column, '
                                    "or a list of specific column names without 'all'"
                                ),
                            }
                        stripped_cols: list[str] = []
                        for col in check_cols:
                            if not isinstance(col, str) or not col.strip():
                                return {
                                    "success": False,
                                    "error": (
                                        "each entry in check_cols must be a non-empty string"
                                    ),
                                }
                            col = col.strip()
                            if err := _validate_dbt_identifier("check_cols entry", col):
                                return err
                            stripped_cols.append(col)
                        check_cols = stripped_cols
                    updated_at = None

                # --- Metadata-driven auto-correction for snapshot ---
                snap_corrections: list[dict[str, Any]] = []
                snap_metadata = await _resolve_source_metadata(
                    source_name, table_name, teradata_profile=teradata_profile
                )
                snap_metadata_info: dict[str, Any] | None = None

                if snap_metadata:
                    real_col_names = [c["name"] for c in snap_metadata.get("columns", [])]
                    pks = snap_metadata.get("primary_keys", [])
                    snap_metadata_info = {
                        "validated": True,
                        "available_columns": real_col_names,
                        "primary_keys": pks,
                    }

                    # Auto-correct unique_key
                    corrected_uk, uk_correction = _autocorrect_single_column(
                        unique_key, snap_metadata, "unique_key"
                    )
                    if corrected_uk is None:
                        available = uk_correction["available_columns"]
                        return {
                            "success": False,
                            "error": (
                                f"unique_key '{unique_key}' not found in table "
                                f"'{table_name}' and could not be auto-corrected "
                                f"(no primary key detected). "
                                f"Available columns: {available}"
                            ),
                        }
                    if uk_correction:
                        uk_correction["field"] = "unique_key"
                        snap_corrections.append(uk_correction)
                    unique_key = corrected_uk

                    # Auto-correct updated_at (timestamp strategy only)
                    if snapshot_strategy == "timestamp" and updated_at:
                        corrected_ua, ua_correction = _autocorrect_single_column(
                            updated_at, snap_metadata, "updated_at"
                        )
                        if corrected_ua is None:
                            available = ua_correction["available_columns"]
                            return {
                                "success": False,
                                "error": (
                                    f"updated_at '{updated_at}' not found in table "
                                    f"'{table_name}' and could not be auto-corrected "
                                    f"(no timestamp column detected). "
                                    f"Available columns: {available}"
                                ),
                            }
                        if ua_correction:
                            ua_correction["field"] = "updated_at"
                            snap_corrections.append(ua_correction)
                        updated_at = corrected_ua

                    # Auto-correct check_cols (check strategy only, not ["all"])
                    if (
                        snapshot_strategy == "check"
                        and check_cols is not None
                        and check_cols != ["all"]
                    ):
                        check_cols, cc_correction = _autocorrect_columns(check_cols, snap_metadata)
                        if cc_correction:
                            cc_correction["field"] = "check_cols"
                            snap_corrections.append(cc_correction)
                            # If all check_cols were invalid and fell back to all metadata
                            # columns, that's still valid — we replaced with real columns.
                            # But if the correction shows all were removed with no fallback,
                            # that means the original list was fully hallucinated.
                            if cc_correction.get("action") == "replaced_all_with_metadata":
                                # All check_cols were invalid — fall back to all columns
                                pass  # check_cols already set to all metadata columns
                else:
                    snap_metadata_info = {"validated": False}

                # Validate unique_key identifier (after potential auto-correction)
                if err := _validate_dbt_identifier("unique_key", unique_key):
                    return err
                # Validate updated_at identifier (after potential auto-correction)
                if snapshot_strategy == "timestamp" and updated_at:
                    if err := _validate_dbt_identifier("updated_at", updated_at):
                        return err

                if target_schema is None or target_schema == "":
                    target_schema = (
                        await asyncio.to_thread(orchestrator.dbt_client.get_target_schema)
                        or "snapshots"
                    )
                if err := _validate_dbt_identifier("target_schema", target_schema):
                    return err
                snap_result = await _generate_snapshot_model(
                    source_name=source_name,
                    table_name=table_name,
                    model_name=model_name,
                    target_schema=target_schema,
                    unique_key=unique_key,
                    strategy=snapshot_strategy,
                    updated_at=updated_at,
                    check_cols=check_cols,
                    dry_run=dry_run,
                    tags=tags,
                )
                if snap_result.get("success") and snap_corrections:
                    snap_result["corrections_applied"] = snap_corrections
                if snap_result.get("success") and snap_metadata_info:
                    snap_result["metadata_validation"] = snap_metadata_info
                return snap_result

        except Exception as e:
            logger.error("Failed to generate dbt model type '%s': %s", model_type, e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    # ------------------------------------------------------------------ #
    #  Router 5: dbt_project                                              #
    # ------------------------------------------------------------------ #

    async def dbt_project(
        action: Literal[
            "create_structure",
            "generate_profiles",
            "create_from_source",
            "create_from_csv",
            "add_package",
            "generate_teradata_macros",
            "refresh_env",
        ],
        project_name: str | None = None,
        include_staging: bool = True,
        include_intermediate: bool = True,
        include_marts: bool = True,
        mart_subfolders: list[str] | None = None,
        include_snapshots: bool = True,
        staging_materialization: str = "view",
        intermediate_materialization: str = "view",
        marts_materialization: str = "table",
        teradata_profile: str | None = None,
        target: str = "dev",
        threads: int = 4,
        profile_name: str | None = None,
        # create_from_source params
        source_database: str | None = None,
        source_tables: list[str] | None = None,
        include_tests: bool = True,
        # multi-target profiles
        targets: list[dict[str, Any]] | None = None,
        # package management
        package_name: str | None = None,
        package_version: str | None = None,
        # create_from_csv params
        csv_files: list[str] | None = None,
        approach: str | None = None,
        target_database: str | None = None,
        delimiter: str | None = None,
    ) -> dict[str, Any]:
        """
        Manage dbt project structure, profiles, packages, and macros.

        IMPORTANT: ``create_structure`` and ``create_from_source`` create
        a per-Teradata-profile sub-project under
        ``<workspace>/dbt_project/dbt_<slug(project_name)>/``. The sub-
        project is bound to the resolved Teradata identity (a named
        connections.yaml profile, or ``wizard:<slug(host)>`` for the
        wizard default). If the target directory already exists with a
        DIFFERENT identity binding, the tool refuses with a conflict
        error — choose a different ``project_name`` or change the
        ``teradata_profile``.

        Connection: follows the server's wizard-vs-profile selection policy
        (see the server ``instructions``). Default is the wizard connection
        unless the user names a profile via ``teradata_profile``. The profile
        shapes the ``profiles.yml`` generated by ``create_structure`` /
        ``generate_profiles`` and the credentials used by ``debug`` /
        ``create_from_source`` / ``create_from_csv`` when they connect.

        NOTE: This tool uses 'action' parameter. To generate models, use dbt_generate_model instead
        (which uses 'model_type', not 'action').

        Valid values for 'action':
          - "create_structure"   : Create standard dbt project folder structure.
                                   Required: project_name.
                                   Optional: include_staging, include_intermediate, include_marts,
                                             mart_subfolders, include_snapshots, staging_materialization,
                                             intermediate_materialization, marts_materialization,
                                             teradata_profile, target, threads.
          - "generate_profiles"  : Generate dbt profiles.yml using default Teradata credentials.
                                   Required: profile_name.
                                   Optional: teradata_profile, target, threads, targets.
                                   If 'targets' is provided, generates multi-environment profiles.yml.
                                   targets: list of dicts, each with 'name' and 'teradata_profile' keys,
                                   e.g. [{"name": "dev", "teradata_profile": "td_dev"},
                                         {"name": "prod", "teradata_profile": "td_prod"}].
          - "create_from_source" : Single-call project bootstrap. Chains: scaffold → profiles →
                                   source YAML → bulk staging models → schema tests.
                                   Source discovery is NOT included — use teradata_discover first.
                                   Required: project_name, source_database, source_tables.
                                   Optional: teradata_profile, target, threads, include_tests.
          - "add_package"        : Add a dbt package to packages.yml.
                                   Required: package_name.
                                   Optional: package_version.
          - "create_from_csv"    : Create dbt project from CSV files (sources,
                                   staging models, tests). The CSV gets exposed
                                   as a dbt source.
                                   DO NOT USE for plain "load this CSV into
                                   Teradata" requests — use ``ttu_execute(
                                   action='load_data', mode='file_to_table')``
                                   instead, which loads via TPT/tdload directly
                                   with no dbt scaffolding.
                                   USE THIS ACTION ONLY when the user explicitly
                                   wants the CSV(s) as part of a dbt project
                                   (mentions "dbt", "staging models", "sources.yml",
                                   "seed", or asks to "build a dbt project around"
                                   the data).
                                   Phase 1: pass csv_files + project_name (no approach)
                                     → returns available approaches based on infrastructure.
                                   Phase 2: pass approach ("dbt_seed"|"tpt_local"|"tpt_airflow")
                                     → executes the chosen approach.
                                   Required: project_name, csv_files.
                                   Optional: approach, target_database, delimiter,
                                             teradata_profile, target, threads, include_tests.
          - "generate_teradata_macros" : Generate common Teradata-specific utility macros
                                        (collect_stats, grant_access, teradata_utils).
                                        No extra params required.
          - "refresh_env"        : Overwrite the existing sub-project's ``.env`` file with current
                                   credentials from the bound Teradata identity. Use this AFTER
                                   rotating credentials in connections.yaml or the wizard so that
                                   ``dotenv run -- dbt ...`` (and the Airflow worker) pick up the
                                   new values. The response lists the env-var KEY NAMES that were
                                   written (e.g. ``["TERADATA_HOST", "TERADATA_PASSWORD", ...]``);
                                   the LLM never sees the values.
                                   Required: project_name, teradata_profile.

        Args:
            action: The project action to perform (see valid values above)
            project_name: Name of the dbt sub-project (required for
                create_structure, create_from_source, create_from_csv).
                Becomes a sub-folder under
                ``<workspace>/dbt_project/dbt_<slug>/`` after slugification:
                  - lowercased; non-alphanumeric chars become underscores
                  - leading ``dbt_`` is auto-stripped (so ``project_name='dbt_test'``
                    and ``project_name='test'`` both resolve to ``dbt_test/``)
                Reserved names rejected (would visually collide with the
                parent container ``dbt_project/``):
                  - ``'project'``, ``'dbt_project'`` and any other slug whose
                    final form would equal the parent's basename.
                Prefer a short snake_case identifier specific to this
                Teradata identity (e.g. ``'analytics'``, ``'warehouse_prod'``,
                ``'sales_lake'``).
            include_staging: Create staging folder (for create_structure)
            include_intermediate: Create intermediate folder (for create_structure)
            include_marts: Create marts folder (for create_structure)
            mart_subfolders: Business domain subfolders for marts (for create_structure)
            include_snapshots: Create snapshots folder (for create_structure)
            staging_materialization: Materialization for staging models (for create_structure)
            intermediate_materialization: Materialization for intermediate models (for create_structure)
            marts_materialization: Materialization for marts models (for create_structure)
            teradata_profile: Connection profile name from connections.yaml
            target: dbt target environment name (for create_structure, generate_profiles)
            threads: Number of threads for parallel execution (for create_structure, generate_profiles)
            profile_name: Name of the dbt profile (for generate_profiles)
            source_database: Source database name (for create_from_source)
            source_tables: List of source table names (for create_from_source)
            include_tests: Generate data quality tests (for create_from_source)
            targets: List of target environments for multi-target profiles (for generate_profiles)
            package_name: Package to add (for add_package), e.g. 'calogica/dbt_expectations'
            package_version: Version constraint (for add_package), e.g. '>=0.10.0'
            csv_files: List of CSV file paths (for create_from_csv)
            approach: Loading approach (for create_from_csv): 'dbt_seed', 'tpt_local', 'tpt_airflow'
            target_database: Target Teradata database (for create_from_csv with tpt_local/tpt_airflow)
            delimiter: CSV delimiter character (for create_from_csv, default: ',')

        Returns:
            Dictionary with project action results
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        action = action.strip().lower()
        try:
            valid_actions = [
                "create_structure",
                "generate_profiles",
                "create_from_source",
                "create_from_csv",
                "add_package",
                "generate_teradata_macros",
                "refresh_env",
            ]
            if action not in valid_actions:
                return {
                    "success": False,
                    "error": (
                        f"Invalid action '{action}'. Valid actions: {', '.join(valid_actions)}"
                    ),
                }

            if action == "create_structure":
                if not project_name:
                    return _missing_project_name_response(orchestrator, "create_structure")
                return await _create_dbt_project_structure(
                    project_name=project_name,
                    include_staging=include_staging,
                    include_intermediate=include_intermediate,
                    include_marts=include_marts,
                    mart_subfolders=mart_subfolders,
                    include_snapshots=include_snapshots,
                    staging_materialization=staging_materialization,
                    intermediate_materialization=intermediate_materialization,
                    marts_materialization=marts_materialization,
                    teradata_profile=teradata_profile,
                    target=target,
                    threads=threads,
                )

            elif action == "generate_profiles":
                if not profile_name:
                    return {
                        "success": False,
                        "error": "profile_name is required for action 'generate_profiles'",
                    }

                # Multi-target profiles
                if targets:
                    resolved_targets = []
                    for t in targets:
                        t_name = t.get("name")
                        t_profile = t.get("teradata_profile")
                        if not t_name:
                            return {
                                "success": False,
                                "error": "Each target must have a 'name' key",
                            }
                        if t_profile:
                            guard = orchestrator.credential_resolver.guard_configured()
                            if guard:
                                return guard
                        try:
                            t_auth = resolve_teradata_auth(
                                settings=orchestrator.settings.teradata,
                                credential_resolver=orchestrator.credential_resolver,
                                teradata_profile=t_profile,
                            )
                        except ValueError as e:
                            return {
                                "success": False,
                                "error": (
                                    f"Could not resolve credentials for target '{t_name}' "
                                    f"(profile: {t_profile}): {str(e)}"
                                ),
                            }
                        resolved_targets.append(
                            {
                                "name": t_name,
                                "auth": t_auth,
                                "threads": t.get("threads", threads),
                            }
                        )

                    await asyncio.to_thread(
                        orchestrator.dbt_generator.generate_multi_target_profiles_yml,
                        profile_name=profile_name,
                        targets=resolved_targets,
                    )
                    profiles_path = orchestrator.dbt_generator.project_dir / "profiles.yml"
                    return {
                        "success": True,
                        "profile_name": profile_name,
                        "targets": [t["name"] for t in resolved_targets],
                        "profiles_path": str(profiles_path),
                        "message": (
                            f"Generated multi-target profiles.yml at {profiles_path} "
                            f"with targets: {[t['name'] for t in resolved_targets]}"
                        ),
                        "next_steps": [
                            (
                                f"**1. Sanity-check each target**: for every "
                                f"target in {[t['name'] for t in resolved_targets]!r}, "
                                f"run `dbt_execute(command='debug', target='<target>')`. "
                                f"**Why**: a multi-target profile is only useful "
                                f"if every target connects; mismatched creds in "
                                f"one target can be hidden behind passing runs in "
                                f"another. **Effect**: dbt validates and connects "
                                f"to each target. **If missing**: skip if you "
                                f"only intend to use one of the configured "
                                f"targets right now."
                            ),
                            (
                                "**2. Run against the default target**: "
                                "`dbt_execute(command='run')`. **Why**: the "
                                "profile is wired in — the next step is to "
                                "materialize models on the default target. "
                                "**Effect**: dbt-teradata creates views/tables "
                                "using the first target's credentials. **If "
                                "missing**: skip if you only generated the "
                                "profile for an external dbt invocation."
                            ),
                        ],
                    }

                # Single-target profiles (original behavior)
                if teradata_profile:
                    guard = orchestrator.credential_resolver.guard_configured()
                    if guard:
                        return guard
                try:
                    profiles_auth = resolve_teradata_auth(
                        settings=orchestrator.settings.teradata,
                        credential_resolver=orchestrator.credential_resolver,
                        teradata_profile=teradata_profile,
                    )
                except ValueError as e:
                    return {"success": False, "error": str(e)}
                return await _generate_dbt_profiles_yml(
                    profile_name=profile_name,
                    auth=profiles_auth,
                    target=target,
                    threads=threads,
                )

            elif action == "create_from_source":
                if not project_name:
                    return _missing_project_name_response(orchestrator, "create_from_source")
                if not source_database:
                    return {
                        "success": False,
                        "error": "source_database is required for action 'create_from_source'",
                    }
                if not source_tables:
                    return {
                        "success": False,
                        "error": "source_tables is required for action 'create_from_source'",
                    }

                # Resolve Teradata auth for source generation
                if teradata_profile:
                    guard = orchestrator.credential_resolver.guard_configured()
                    if guard:
                        return guard
                try:
                    source_auth = resolve_teradata_auth(
                        settings=orchestrator.settings.teradata,
                        credential_resolver=orchestrator.credential_resolver,
                        teradata_profile=teradata_profile,
                    )
                except ValueError as e:
                    return {"success": False, "error": str(e)}

                # Step 1: Scaffold project structure
                scaffold_result = await _create_dbt_project_structure(
                    project_name=project_name,
                    teradata_profile=teradata_profile,
                    target=target,
                    threads=threads,
                )
                if not scaffold_result.get("success"):
                    return scaffold_result

                # Step 2: Generate staging models from source tables
                target_schema = (
                    await asyncio.to_thread(orchestrator.dbt_client.get_target_schema) or "staging"
                )
                staging_result = await _generate_dbt_models_from_source(
                    source_database=source_database,
                    source_tables=source_tables,
                    target_schema=target_schema,
                    model_type="staging",
                    include_tests=include_tests,
                    auth=source_auth,
                )

                response = {
                    "success": staging_result.get("success", False),
                    "project_name": project_name,
                    "scaffold": scaffold_result,
                    "staging": staging_result,
                    "summary": (
                        f"Project '{project_name}' bootstrapped with "
                        f"{staging_result.get('models_generated', 0)} staging models "
                        f"from {source_database}"
                    ),
                }
                if staging_result.get("success"):
                    staging_model_names = [f"stg_{t}" for t in source_tables]
                    response["next_steps"] = [
                        (
                            f"**1. Run the staging models**: "
                            f"`dbt_execute(command='run', "
                            f"models={staging_model_names!r}, "
                            f"project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                            f"**Why**: staging SQL was generated on disk but "
                            f"Teradata views/tables for them don't exist until "
                            f"``dbt run`` materializes them. **Effect**: "
                            f"dbt-teradata creates views/tables for each "
                            f"``stg_<table>`` model. **If missing**: skip if "
                            f"the user wants to inspect the generated SQL "
                            f"first."
                        ),
                        (
                            f"**2. Validate with tests**: "
                            f"`dbt_execute(command='test', "
                            f"models={staging_model_names!r}, "
                            f"project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                            f"**Why**: not_null tests catch missing-column / "
                            f"NULL-leakage problems early. **Effect**: "
                            f"dbt-teradata runs every test attached to the "
                            f"staging models. **If missing**: skip if you set "
                            f"``include_tests=False``."
                        ),
                        (
                            f"**3. Schedule the project in Airflow** "
                            f"(production): "
                            f"`pipeline_deploy(action='create_dbt_dag', "
                            f"dag_id='<id>', project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile or '<profile>'}')`. "
                            f"**Why**: ad-hoc runs are fine for development; "
                            f"production wants a recurring DAG. **Effect**: "
                            f"generates an Airflow DAG that re-runs this "
                            f"sub-project on the schedule you pick. **If "
                            f"missing**: skip for one-off / development runs."
                        ),
                    ]
                return response

            elif action == "add_package":
                if not package_name:
                    return {
                        "success": False,
                        "error": "package_name is required for action 'add_package'",
                    }
                result = await asyncio.to_thread(
                    orchestrator.dbt_generator.add_package,
                    package_name=package_name,
                    version=package_version,
                )
                if result.get("success"):
                    result["next_steps"] = [
                        (
                            f"**1. Install the package**: "
                            f"`dbt_execute(command='deps')`. **Why**: "
                            f"``packages.yml`` was updated but dbt won't "
                            f"download ``{package_name}`` until ``dbt deps`` "
                            f"runs. **Effect**: dbt fetches the package into "
                            f"``dbt_packages/`` so its macros are usable. "
                            f"**If missing**: any ref to the package's "
                            f"macros will fail with ``Compilation Error: "
                            f"Macro not found``."
                        ),
                        (
                            "**2. Verify the project still parses**: "
                            "`dbt_execute(command='parse')`. **Why**: a new "
                            "package can introduce macro-name collisions or "
                            "Jinja conflicts that only surface at parse "
                            "time. **Effect**: dbt re-parses the project "
                            "with the package loaded. **If missing**: skip "
                            "if your next step is ``run`` / ``build`` — "
                            "those re-parse implicitly."
                        ),
                    ]
                return sanitize_response(result)

            elif action == "generate_teradata_macros":
                result = await asyncio.to_thread(
                    orchestrator.dbt_generator.generate_teradata_macros,
                )
                if isinstance(result, dict) and result.get("success", True):
                    result["next_steps"] = [
                        (
                            "**1. Re-parse to load the new macros**: "
                            "`dbt_execute(command='parse')`. **Why**: macros "
                            "are picked up on the next dbt parse; until "
                            "then, models that call them will fail. "
                            "**Effect**: dbt re-reads the macro files in "
                            "``macros/`` so they become callable from "
                            "models. **If missing**: skip if your next "
                            "step is ``run`` / ``build`` — those re-parse "
                            "implicitly."
                        ),
                        (
                            "**2. Use the macros in models**: reference them "
                            "as ``{{ <macro_name>(...) }}`` inside your dbt "
                            "SQL files. **Why**: writing the macro file is "
                            "only useful once a model invokes it. "
                            "**Effect**: the macro is rendered into the "
                            "compiled SQL on the next ``dbt compile`` / "
                            "``run``. **If missing**: skip if you generated "
                            "macros only for cross-project reuse."
                        ),
                    ]
                return result

            elif action == "create_from_csv":
                if not project_name:
                    return _missing_project_name_response(orchestrator, "create_from_csv")
                if not csv_files:
                    return {
                        "success": False,
                        "error": "csv_files is required for action 'create_from_csv'",
                    }
                return sanitize_response(
                    await _create_dbt_project_from_csv(
                        project_name=project_name,
                        csv_files=csv_files,
                        approach=approach,
                        target_database=target_database,
                        delimiter=delimiter,
                        teradata_profile=teradata_profile,
                        target=target,
                        threads=threads,
                        include_tests=include_tests,
                    )
                )

            elif action == "refresh_env":
                if not project_name:
                    return _missing_project_name_response(orchestrator, "refresh_env")
                # Resolve which sub-project to refresh. Only ``existing`` is
                # acceptable — refresh_env never creates a new sub-project.
                identity = _resolve_teradata_identity(orchestrator, teradata_profile)
                if identity is None:
                    return {
                        "success": False,
                        "error": (
                            "No Teradata host is configured. Ask the user to "
                            "set TERADATA_HOST via the Setup Wizard, OR pass "
                            "a named teradata_profile from connections.yaml. "
                            "The agent must not edit .env."
                        ),
                    }
                resolution = _resolve_dbt_subproject(
                    parent=orchestrator.dbt_project_parent,
                    identity=identity,
                    project_name=project_name,
                )
                if resolution.status == "name_collision":
                    return _collision_response(orchestrator, resolution, project_name)
                if resolution.status != "existing":
                    return {
                        "success": False,
                        "action_required": "scaffold_subproject_first",
                        "error": (
                            f"refresh_env requires an existing sub-project; "
                            f"resolver returned status='{resolution.status}'. "
                            f"Run dbt_project(action='create_structure', "
                            f"project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile}') first."
                        ),
                        "resolution_status": resolution.status,
                    }
                assert resolution.project_dir is not None
                project_dir = resolution.project_dir
                if teradata_profile:
                    guard = orchestrator.credential_resolver.guard_configured()
                    if guard:
                        return guard
                try:
                    auth = resolve_teradata_auth(
                        settings=orchestrator.settings.teradata,
                        credential_resolver=orchestrator.credential_resolver,
                        teradata_profile=teradata_profile,
                    )
                except ValueError as e:
                    return {
                        "success": False,
                        "error": (
                            f"Could not resolve Teradata auth for profile "
                            f"'{teradata_profile}': {str(e)}"
                        ),
                    }
                # ``env_dict`` holds CREDENTIAL VALUES — scoped tightly to
                # this block, never logged, never returned, never assigned
                # to the response. ``_write_dotenv_file`` writes the file
                # and returns ONLY the list of keys actually written.
                env_dict = auth.render_for_dbt_env()
                keys_skipped_empty = [k for k, v in env_dict.items() if not v]
                dotenv_path = project_dir / ".env"
                try:
                    keys_written = await asyncio.to_thread(
                        _write_dotenv_file, dotenv_path, env_dict
                    )
                finally:
                    # Drop the value-bearing dict eagerly even if the write
                    # failed. ``del`` is a hint to the GC, not a hard
                    # erase, but it removes the only local reference so
                    # the response builder below cannot accidentally
                    # capture it.
                    del env_dict
                logger.info(
                    "Refreshed .env at %s with %d TERADATA_* keys",
                    dotenv_path,
                    len(keys_written),
                )
                response = {
                    "success": True,
                    "project_name": project_name,
                    "project_dir": str(project_dir),
                    "teradata_profile": teradata_profile,
                    "dotenv_path": str(dotenv_path),
                    "keys_written": keys_written,
                    "keys_skipped_empty": keys_skipped_empty,
                    "drift_warning": (
                        "The .env file is now decoupled from connections.yaml. "
                        "Re-run dbt_project(action='refresh_env', ...) after "
                        "the next credential rotation. The MCP server does not "
                        "watch connections.yaml or the wizard for changes."
                    ),
                    "next_steps": [
                        (
                            f"**1. Verify the connection**: "
                            f"`dbt_execute(command='debug', "
                            f"project_name='{project_name}', "
                            f"teradata_profile='{teradata_profile}')`. "
                            f"**Why**: a refresh after rotation is the moment "
                            f"to confirm dbt connects with the new credentials. "
                            f"**Effect**: dbt re-runs the connection check "
                            f"using the updated .env values. **If missing**: "
                            f"the next ``dbt run`` may fail with an auth error."
                        ),
                        (
                            f"**2. Re-sync the sub-project to the Airflow "
                            f"worker**: copy ``{project_dir}`` (including the "
                            f"refreshed ``.env``) to the same path on the "
                            f"Airflow worker. **Why**: the Airflow DAG runs "
                            f"``dotenv run -- dbt ...`` from the worker's copy "
                            f"of the sub-project; without re-syncing, the "
                            f"worker keeps using the stale credentials. "
                            f"**Effect**: subsequent DAG runs pick up the new "
                            f"values. **If missing**: skip if you only run dbt "
                            f"locally and not via Airflow."
                        ),
                        (
                            "**3. Repeat after the next rotation**: "
                            "`dbt_project(action='refresh_env', ...)` again. "
                            "**Why**: the .env is a one-shot snapshot — no "
                            "automatic sync from connections.yaml. **Effect**: "
                            "overwrites .env with current values. **If "
                            "missing**: dbt and Airflow will use stale "
                            "credentials until you refresh."
                        ),
                    ],
                }
                return sanitize_response(response)

        except Exception as e:
            logger.error("Failed to perform dbt project action '%s': %s", action, e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    # Return only the 5 router tool functions
    return {
        "dbt_execute": dbt_execute,
        "dbt_docs": dbt_docs,
        "dbt_info": dbt_info,
        "dbt_generate_model": dbt_generate_model,
        "dbt_project": dbt_project,
    }
