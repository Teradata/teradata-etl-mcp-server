# ruff: noqa: S608
"""Data movement and transfer tools.

This module provides MCP tools for managing data transfers using:
- Airbyte connections for ELT pipelines
- Airflow TdLoadOperator for Teradata data loading
- BteqOperator for Teradata validation queries
"""

import asyncio
import logging
import re
from pathlib import Path
from typing import Annotated, Any, Literal, Optional

from pydantic import Field

from ..auth import is_explicit_profile
from ..clients.airbyte_client import (
    AirbyteAPIError,
    AirbyteClientError,
    to_public_api_sync_mode,
)
from ..clients.async_airflow_client import AsyncAirflowAPIError
from ..orchestrator import PipelineOrchestrator
from ..response_sanitizer import safe_error_message, sanitize_response
from ..utils.file_operations import UnsafePathError, safe_path_under_any_root
from ..utils.validators import PipelineValidator, to_quartz_cron, validate_identifier
from .airflow_pipeline_management import _locate_dbt_subproject_dir
from .dbt_management import _resolve_teradata_identity
from .utils import UNRESOLVED_ENV_VAR as _UNRESOLVED_ENV_VAR

logger = logging.getLogger(__name__)


def _maybe_resolve_dbt_path(
    orchestrator: Any,
    project_name: str | None,
    teradata_profile: str | None,
    *,
    profile_param_name: str = "teradata_profile",
) -> str | dict[str, Any]:
    """Optional-dbt resolver for ``airflow_teradata_load``.

    Most TdLoad DAG flows are dbt-optional: the user gets a CSV-load /
    table-transfer DAG without a dbt step unless they ask for one. We
    treat ``project_name=None`` as "no dbt step" (returning the empty
    string the inner generators expect), and only run the sub-project
    locator when a project_name is supplied.

    Unlike the dbt-only DAG creation paths, ``airflow_teradata_load``
    DOES need a load-step Teradata profile here for a binding-mismatch
    guard. The TdLoad task runs against the load profile's Airflow
    Connection; the dbt task runs against the sub-project's ``.env``.
    If the sub-project's binding (``dbt_project.yml::profile``) names a
    DIFFERENT identity than the load profile resolves to, the generated
    DAG would silently load CSV into one Teradata instance and transform
    a different one. Refuse it.

    The user-facing parameter name differs by method:
      - csv_dag / csv_complete → ``teradata_profile`` (load and dbt
        run against the same Teradata).
      - table_transfer → ``target_teradata_profile`` (dbt step runs
        against the load TARGET, not the source).

    Callers pass the active param's NAME via ``profile_param_name`` so
    the mismatch error tells the user which parameter to fix —
    misnaming ``target_teradata_profile`` as ``teradata_profile`` for
    table_transfer would send the user looking for the wrong knob.

    The check is symmetric with the prior ``_resolve_dag_dbt_subproject``
    behavior (``conflict`` status), preserved on this surface only —
    the dbt-only DAG paths legitimately drop the load profile because
    there is no separate load step to mismatch against.

    Returns:
        - ``""`` when ``project_name`` is None — no dbt step.
        - resolved sub-project path as ``str`` on success.
        - response ``dict`` (action_required / error) on resolution failure
          OR binding mismatch; caller returns it verbatim.
    """
    if project_name is None:
        return ""
    resolved = _locate_dbt_subproject_dir(orchestrator, project_name)
    if isinstance(resolved, dict):
        # ``_locate_dbt_subproject_dir`` already fails closed when the
        # sub-project's ``dbt_project.yml::profile`` is missing/unreadable
        # (returns ``action_required: fix_subproject_binding``). So by the
        # time we get past this branch, ``identity`` is always a non-empty
        # string and the mismatch comparison below is well-defined.
        return resolved
    sub_project_dir, identity = resolved
    expected_identity = _resolve_teradata_identity(orchestrator, teradata_profile)
    if expected_identity and identity and expected_identity != identity:
        return {
            "success": False,
            "error": (
                f"Sub-project for project_name='{project_name}' is bound to "
                f"Teradata identity '{identity}', but the load step's "
                f"{profile_param_name} resolves to identity "
                f"'{expected_identity}'. The generated DAG would load data "
                "into one Teradata instance and run dbt against another. "
                "Either choose a different project_name (matching this "
                f"load's {profile_param_name} binding) or pass a "
                f"{profile_param_name} that matches the sub-project's binding."
            ),
            "teradata_identity": identity,
            "expected_identity": expected_identity,
            "project_name": project_name,
            "profile_param_name": profile_param_name,
        }
    return str(sub_project_dir)


# Regex for valid Teradata identifiers (alphanumeric, _, #, $; must not start with a digit).
_TD_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]{0,127}$")


def _quote_id(name: str) -> str:
    """Validate and double-quote a Teradata database/table identifier.

    Follows Teradata object naming rules:
    - Trailing whitespace is stripped (not part of the name per spec).
    - All-whitespace / empty names are rejected.
    - Disallowed characters (NULL, SUBSTITUTE, REPLACEMENT CHARACTER,
      select compatibility ideographs) are rejected.
    - Names must match the standard unquoted identifier pattern
      (prevents SQL injection via crafted names).
    """
    if not name or not name.rstrip():
        raise ValueError(f"Empty or all-whitespace identifier: {name!r}")
    name = name.rstrip()
    if _TD_DISALLOWED_CHARS.search(name):
        raise ValueError(f"Identifier contains disallowed Teradata characters: {name!r}")
    if not _TD_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid Teradata identifier: {name!r}")
    return f'"{name}"'


# Characters that Teradata unconditionally forbids inside any object name.
_TD_DISALLOWED_CHARS = re.compile("[\u0000\u001a\ufffd\ufa6c\ufa6f\ufad0\ufad1\ufad5\ufad6\ufad7]")


def _quote_column(name: str) -> str:
    """Double-quote a column identifier for safe use in Teradata SQL.

    Follows Teradata object naming rules:
    - Trailing whitespace is stripped (not part of the name per spec).
    - All-whitespace / empty names are rejected.
    - Disallowed characters (NULL U+0000, SUBSTITUTE U+001A,
      REPLACEMENT CHARACTER U+FFFD, and select compatibility ideographs)
      are rejected.
    - Embedded double-quotes are escaped by doubling them (U+0022 -> U+0022 U+0022).
    """
    if not name or not name.rstrip():
        raise ValueError(f"Empty or all-whitespace column name: {name!r}")
    name = name.rstrip()
    if _TD_DISALLOWED_CHARS.search(name):
        raise ValueError(f"Column name contains disallowed Teradata characters: {name!r}")
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


class DiscoveryCache:
    """Request-scoped cache for discover_source_schema calls.

    Avoids redundant API calls within a single pipeline creation flow.
    Created and discarded within one tool call — no TTL needed.
    """

    def __init__(self, airbyte_client: Any):
        self._client = airbyte_client
        self._cache: dict[str, dict[str, Any]] = {}

    async def get(self, source_id: str) -> dict[str, Any]:
        if source_id not in self._cache:
            self._cache[source_id] = await self._client.discover_source_schema(source_id)
        return self._cache[source_id]

    def invalidate(self, source_id: str | None = None) -> None:
        if source_id:
            self._cache.pop(source_id, None)
        else:
            self._cache.clear()

    def peek(self, source_id: str) -> dict[str, Any] | None:
        return self._cache.get(source_id)


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Pure-Python edit distance. O(m*n) time, O(min(m,n)) space."""
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            current_row.append(
                min(
                    previous_row[j + 1] + 1,
                    current_row[j] + 1,
                    previous_row[j] + (c1 != c2),
                )
            )
        previous_row = current_row
    return previous_row[-1]


def _fuzzy_token_score(keyword: str, target: str) -> float:
    """Word-boundary-aware matching score (0.0–1.0).

    Prevents false positives like "nation" matching "national_id" by splitting
    the target on word boundaries and matching against individual tokens.

    Scoring tiers:
    - 1.0:  exact match (keyword == target)
    - 0.95: keyword is an exact token in underscore-split name
    - 0.85: keyword is a prefix of a token
    - 0.5–0.8: Levenshtein-based fuzzy (ratio >= 0.7)
    - 0.0:  no match
    """
    kw = keyword.lower().strip()
    tgt = target.lower().strip()

    if not kw or not tgt:
        return 0.0

    # Exact full-string match
    if kw == tgt:
        return 1.0

    # Split target into tokens on common delimiters
    tokens = re.split(r"[_\-\s.]+", tgt)

    # Exact token match
    if kw in tokens:
        return 0.95

    # Prefix match on any token
    for tok in tokens:
        if tok.startswith(kw) and len(kw) >= 3:
            return 0.85

    # Levenshtein-based fuzzy match against each token
    best_ratio = 0.0
    for tok in tokens:
        max_len = max(len(kw), len(tok))
        if max_len == 0:
            continue
        dist = _levenshtein_distance(kw, tok)
        ratio = 1.0 - dist / max_len
        if ratio > best_ratio:
            best_ratio = ratio

    # Also check against the full target string
    full_max = max(len(kw), len(tgt))
    if full_max > 0:
        full_ratio = 1.0 - _levenshtein_distance(kw, tgt) / full_max
        if full_ratio > best_ratio:
            best_ratio = full_ratio

    if best_ratio >= 0.7:
        return 0.5 + 0.3 * ((best_ratio - 0.7) / 0.3)

    return 0.0


def _score_stream_v2(item: dict[str, Any], kws: list[str]) -> float:
    """Score a stream against keywords using fuzzy token matching.

    Weighted scoring:
    - Stream name:  weight 4.0
    - Namespace:    weight 2.0
    - Description:  weight 2.0
    - Column names: weight 1.0 (best match among all columns)
    - Tags:         weight 1.0
    """
    name = item.get("name") or ""
    ns = item.get("namespace") or ""
    desc = item.get("description") or ""
    cols = [str(c) for c in item.get("columns") or []]
    tags = [str(t) for t in item.get("tags") or []]

    score = 0.0
    for k in kws:
        # Name match (weight 4.0)
        name_score = _fuzzy_token_score(k, name)
        score += name_score * 4.0

        # Namespace match (weight 2.0)
        if ns:
            ns_score = _fuzzy_token_score(k, ns)
            score += ns_score * 2.0

        # Description match (weight 2.0)
        if desc:
            desc_score = _fuzzy_token_score(k, desc)
            score += desc_score * 2.0

        # Column names (weight 1.0, best match)
        if cols:
            best_col = max(_fuzzy_token_score(k, c) for c in cols)
            score += best_col * 1.0

        # Tags (weight 1.0)
        if tags:
            best_tag = max(_fuzzy_token_score(k, t) for t in tags)
            score += best_tag * 1.0

    return score


def _suggest_stream_names(
    target: str,
    available_names: list[str],
    max_suggestions: int = 5,
) -> list[str]:
    """Suggest closest stream names for an unmatched target.

    Uses Levenshtein distance + fuzzy token scoring to find the best matches.
    Returns up to ``max_suggestions`` names sorted by similarity.
    """
    if not target or not available_names:
        return available_names[:max_suggestions] if available_names else []

    scored: list[tuple[str, float]] = []
    for name in available_names:
        # Combine levenshtein ratio and fuzzy token score
        max_len = max(len(target), len(name))
        lev_ratio = (
            1.0 - _levenshtein_distance(target.lower(), name.lower()) / max_len if max_len else 0.0
        )
        token_score = _fuzzy_token_score(target, name)
        combined = max(lev_ratio, token_score)
        scored.append((name, combined))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [name for name, _ in scored[:max_suggestions]]


def _extract_stream_names(disc: dict[str, Any]) -> list[str]:
    """Extract stream names from a discovery result."""
    catalog = (disc or {}).get("catalog", {})
    return [
        e.get("stream", {}).get("name")
        for e in catalog.get("streams", [])
        if e.get("stream", {}).get("name")
    ]


def _normalize_stream_item(item: dict[str, Any], default_selected: bool = True) -> dict[str, Any]:
    """Normalize a stream selection dict: snake_case keys to camelCase."""
    out = dict(item)
    if "sync_mode" in out and "syncMode" not in out:
        out["syncMode"] = out.pop("sync_mode")
    if "destination_sync_mode" in out and "destinationSyncMode" not in out:
        out["destinationSyncMode"] = out.pop("destination_sync_mode")
    if "cursor_field" in out and "cursorField" not in out:
        out["cursorField"] = out.pop("cursor_field")
    if "primary_key" in out and "primaryKey" not in out:
        out["primaryKey"] = out.pop("primary_key")
    if default_selected:
        out.setdefault("selected", True)
    return out


def _validate_sync_modes(streams: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Check that every stream has explicit syncMode and destinationSyncMode.

    Checks both camelCase and snake_case variants.
    Returns a clarification dict if anything is missing, or None if valid.
    """
    missing_sync = [
        s.get("name") for s in streams if not s.get("syncMode") and not s.get("sync_mode")
    ]
    missing_dest = [
        s.get("name")
        for s in streams
        if not s.get("destinationSyncMode") and not s.get("destination_sync_mode")
    ]
    if not missing_sync and not missing_dest:
        return None
    clarification: dict[str, Any] = {
        "success": False,
        "action_required": "clarify_sync_configuration",
        "message": (
            "Sync configuration is missing for one or more streams. "
            "Please ask the user how they want to sync before proceeding."
        ),
        "missing_details": {},
        "valid_options": {
            "syncMode": {
                "full_refresh": "Re-reads all data from the source every sync. Simpler but slower for large tables.",
                "incremental": "Only syncs new or updated records since the last sync. Faster for large tables, requires a cursor field (e.g., updated_at).",
            },
            "destinationSyncMode": {
                "overwrite": "Replaces all data in the destination table each sync.",
                "append": "Adds new records to the destination without modifying existing ones.",
                "append_dedup": "Adds new records and deduplicates based on primary key (only with incremental syncMode).",
            },
        },
        "example_stream": {
            "name": "customer",
            "syncMode": "incremental",
            "destinationSyncMode": "append_dedup",
        },
    }
    if missing_sync:
        clarification["missing_details"]["syncMode"] = missing_sync
    if missing_dest:
        clarification["missing_details"]["destinationSyncMode"] = missing_dest
    return clarification


async def _validate_cursor_fields(
    streams: list[dict[str, Any]],
    source_id: str,
    airbyte_client: Any,
    discovery_cache: Optional["DiscoveryCache"] = None,
) -> dict[str, Any] | None:
    """Validate cursor fields for incremental streams against discovered schema.

    Returns a clarification dict if any cursor is missing/invalid, or None if valid.
    """
    incr_streams = [
        s
        for s in streams
        if (s.get("syncMode") or s.get("sync_mode") or "").lower() == "incremental"
    ]
    if not incr_streams or not source_id:
        return None

    if discovery_cache:
        disc = await discovery_cache.get(source_id)
    else:
        disc = await airbyte_client.discover_source_schema(source_id)
    disc_catalog = (disc or {}).get("catalog", {})
    disc_streams = disc_catalog.get("streams", [])
    columns_by_stream: dict[str, list[str]] = {}
    for ds in disc_streams:
        ds_stream = ds.get("stream", {})
        ds_name = ds_stream.get("name")
        prop_fields = ds_stream.get("propertyFields") or []
        cols = []
        for pf in prop_fields:
            if isinstance(pf, list) and pf:
                cols.append(pf[0])
            elif isinstance(pf, str):
                cols.append(pf)
        if ds_name and cols:
            columns_by_stream[ds_name] = sorted(cols)

    missing_cursor_info: dict[str, Any] = {}
    for s in incr_streams:
        s_name = s.get("name")
        cursor = s.get("cursorField") or s.get("cursor_field")
        available_cols = columns_by_stream.get(s_name, [])
        if not cursor:
            missing_cursor_info[s_name] = {
                "issue": "missing",
                "available_columns": available_cols,
            }
        elif available_cols:
            cursor_name = cursor[0] if isinstance(cursor, list) else cursor
            if cursor_name not in available_cols:
                missing_cursor_info[s_name] = {
                    "issue": "invalid",
                    "provided": cursor_name,
                    "available_columns": available_cols,
                }

    if not missing_cursor_info:
        return None
    return {
        "success": False,
        "action_required": "clarify_cursor_field",
        "message": (
            "Incremental sync requires a valid cursor field to track which "
            "records have been synced. Please ask the user which column "
            "should be used as the cursor field for each stream listed below."
        ),
        "streams": missing_cursor_info,
        "explanation": (
            "A cursor field is a column that Airbyte uses to determine which "
            "records are new or updated since the last sync. It should be a "
            "column that increases monotonically over time (e.g., a timestamp "
            "or auto-incrementing ID)."
        ),
    }


async def _validate_stream_names(
    streams: list[dict[str, Any]],
    source_id: str,
    airbyte_client: Any,
    discovery_cache: Optional["DiscoveryCache"] = None,
) -> dict[str, Any] | None:
    """Validate that requested stream names exist in the source schema.

    Returns a clarification dict if any stream names don't match, or None if all valid.
    Uses case-insensitive matching and suggests close matches for typos.
    """
    if not streams or not source_id:
        return None

    # Skip wildcard streams
    named_streams = [s for s in streams if s.get("name") and s.get("name") != "*"]
    if not named_streams:
        return None

    if discovery_cache:
        disc = await discovery_cache.get(source_id)
    else:
        disc = await airbyte_client.discover_source_schema(source_id)
    disc_catalog = (disc or {}).get("catalog", {})
    disc_streams = disc_catalog.get("streams", [])
    available_names = []
    for ds in disc_streams:
        ds_stream = ds.get("stream", {})
        ds_name = ds_stream.get("name")
        if ds_name:
            available_names.append(ds_name)

    available_lower = {n.lower(): n for n in available_names}
    unmatched: dict[str, Any] = {}
    for s in named_streams:
        s_name = s.get("name")
        if s_name in available_names:
            continue  # exact match
        # Try case-insensitive match
        if s_name.lower() in available_lower:
            continue  # will match case-insensitively in build_configured_catalog
        # No match — find close suggestions using fuzzy matching
        suggestions = _suggest_stream_names(s_name, available_names)
        unmatched[s_name] = {
            "issue": "not_found",
            "suggestions": suggestions,
        }

    if not unmatched:
        return None
    return {
        "success": False,
        "action_required": "clarify_stream_names",
        "message": (
            "Some requested stream names were not found in the source. "
            "Please verify the stream names and try again with the correct names."
        ),
        "unmatched_streams": unmatched,
        "available_streams": sorted(available_names),
    }


async def _find_or_create_connector(
    connector_type: str,
    name: str,
    definition_id: str,
    config: dict[str, Any],
    orchestrator: "PipelineOrchestrator",
) -> dict[str, Any]:
    """Find an existing connector by name/config or create a new one.

    Works for both sources and destinations. Returns a dict with
    ``success``, ``source``/``destination``, and ``reused`` keys.

    Args:
        connector_type: ``"source"`` or ``"destination"``.
        name: Connector name.
        definition_id: Connector definition UUID.
        config: The user-provided configuration dict.
        orchestrator: The pipeline orchestrator (provides airbyte_client).
    """
    client = orchestrator.airbyte_client
    is_source = connector_type == "source"
    entity_key = "source" if is_source else "destination"
    id_key = "sourceId" if is_source else "destinationId"
    def_id_keys = (
        ["definitionId", "sourceDefinitionId"]
        if is_source
        else ["definitionId", "destinationDefinitionId"]
    )

    _, spec = await _get_connector_spec(orchestrator, connector_type, definition_id)

    # Look up connector type name for spec-aware shaping
    if is_source:
        defs = await client.list_source_definitions_registry()
        def_match_key = "sourceDefinitionId"
    else:
        defs = await client.list_destination_definitions_registry()
        def_match_key = "destinationDefinitionId"
    connector_type_name = None
    for d in defs:
        if d.get(def_match_key) == definition_id:
            connector_type_name = d.get("name") or d.get("dockerRepository", "")
            break

    prepared_config = (
        _shape_config_to_spec(config, spec, connector_type_name) if spec else dict(config)
    )
    try:
        requested_norm = (
            _normalize_with_spec(prepared_config, spec)
            if spec
            else _baseline_normalize_config(prepared_config)
        )
    except Exception as e:
        logger.warning("Config normalization failed for requested %s: %s", connector_type, e)
        requested_norm = None  # Will not attempt config-based reuse

    existing = await (client.list_sources() if is_source else client.list_destinations())
    for item in existing:
        try:
            full = await (
                client.get_source(item.get(id_key))
                if is_source
                else client.get_destination(item.get(id_key))
            )
        except Exception:
            full = item
        cfg = (full or {}).get("configuration") or (full or {}).get("connectionConfiguration") or {}
        item_def_id = None
        for dk in def_id_keys:
            item_def_id = (full or {}).get(dk) or item.get(dk)
            if item_def_id:
                break

        # Name match
        if (
            (full or item).get("name")
            and name
            and (str((full or item).get("name")).strip().lower() == str(name).strip().lower())
            and (not item_def_id or item_def_id == definition_id)
        ):
            logger.info(
                "Reusing %s by name '%s' (ID %s) with matching definition.",
                entity_key,
                name,
                (full or item).get(id_key),
            )
            return {"success": True, entity_key: full or item, "reused": True}

        # Config match
        if not item_def_id or item_def_id == definition_id:
            try:
                existing_norm = (
                    _normalize_with_spec(cfg, spec) if spec else _baseline_normalize_config(cfg)
                )
            except Exception as e:
                logger.warning(
                    "Config normalization failed for existing %s '%s': %s",
                    connector_type,
                    (full or item).get("name", "?"),
                    e,
                )
                existing_norm = None
            if existing_norm is not None and requested_norm is not None:
                if existing_norm == requested_norm:
                    logger.info(
                        "Reusing existing %s '%s' with ID %s (normalized configuration match).",
                        entity_key,
                        (full or item).get("name"),
                        (full or item).get(id_key),
                    )
                    return {"success": True, entity_key: full or item, "reused": True}
                if _is_config_subset(requested_norm, existing_norm):
                    logger.info(
                        "Reusing existing %s '%s' with ID %s (subset configuration match).",
                        entity_key,
                        (full or item).get("name"),
                        (full or item).get(id_key),
                    )
                    return {"success": True, entity_key: full or item, "reused": True}
                logger.info(
                    "%s '%s' config mismatch. Existing: %s, Requested: %s",
                    entity_key.title(),
                    (full or item).get("name"),
                    _mask_sensitive_data(existing_norm),
                    _mask_sensitive_data(requested_norm),
                )

    # No match — create new
    workspace_id = await client._get_workspace_id()
    if is_source:
        created = await client.create_source(
            name=name,
            source_definition_id=definition_id,
            connection_configuration=prepared_config,
            workspace_id=workspace_id,
        )
    else:
        created = await client.create_destination(
            name=name,
            destination_definition_id=definition_id,
            connection_configuration=prepared_config,
            workspace_id=workspace_id,
        )
    logger.info("Successfully created %s '%s' with ID: %s", entity_key, name, created.get(id_key))
    return {"success": True, entity_key: created, "reused": False}


def _mask_sensitive_data(config: dict[str, Any]) -> dict[str, Any]:
    """Recursively masks sensitive keys in a dictionary for safe logging."""
    if not isinstance(config, dict):
        return config
    sensitive_keys = [
        "password",
        "token",
        "secret",
        "key",
        "auth",
        "bearer",
        "credential",
        "cert",
        "private",
        "apikey",
        "api_key",
        "access_token",
        "refresh_token",
        "client_secret",
    ]
    masked_config = {}
    for k, v in config.items():
        if any(sens_key in k.lower() for sens_key in sensitive_keys):
            masked_config[k] = "***MASKED***"
        elif isinstance(v, dict):
            masked_config[k] = _mask_sensitive_data(v)
        elif isinstance(v, list):
            masked_config[k] = [
                _mask_sensitive_data(item) if isinstance(item, dict) else item for item in v
            ]
        else:
            masked_config[k] = v
    return masked_config


async def _get_connector_spec(
    orchestrator: PipelineOrchestrator,
    connector_type: str,
    definition_id: str | None,
) -> tuple[str | None, dict[str, Any] | None]:
    """Retrieve (name, connectionSpecification) for a connector definition from cached registry.

    Falls back to (None, None) if not found so callers can use baseline normalization.
    """
    if not definition_id:
        return None, None
    try:
        ct = connector_type.lower()
        if ct.startswith("src") or ct == "source":
            items = await orchestrator.airbyte_client.list_source_definitions_registry()
            keys = ["sourceDefinitionId", "definitionId"]
        else:
            items = await orchestrator.airbyte_client.list_destination_definitions_registry()
            keys = ["destinationDefinitionId", "definitionId"]
            logger.info("Looking up destination spec for definition ID: %s", definition_id)
        for it in items:
            if any(it.get(k) == definition_id for k in keys):
                spec_obj = it.get("spec") or {}
                return it.get("name"), spec_obj.get("connectionSpecification")
        return None, None
    except Exception:
        return None, None


def _is_config_subset(requested: Any, existing: Any) -> bool:
    """Check if all fields in requested match in existing.

    Existing may contain extra fields added by Airbyte as defaults (e.g. ssl: False,
    replication_method: {method: Standard}). These are ignored as long as every
    field the user provided is present with the same value.
    """
    if isinstance(requested, dict) and isinstance(existing, dict):
        for key, val in requested.items():
            if key not in existing:
                return False
            if not _is_config_subset(val, existing[key]):
                return False
        return True
    if isinstance(requested, list) and isinstance(existing, list):
        if len(requested) != len(existing):
            return False
        return all(_is_config_subset(r, e) for r, e in zip(requested, existing, strict=False))
    return requested == existing


def _baseline_normalize_config(config: dict[str, Any]) -> dict[str, Any]:
    """Baseline, schema-agnostic normalization for robust comparisons.

    - Lowercase keys; apply a minimal generic alias map (username<->user, pwd->password)
    - Remove secret fields at any depth (password, token, secret, etc.)
    - Coerce numeric strings to int when safe
    - Normalize ssl_mode: string -> {"mode": <value>}
    - For host-like keys: trim whitespace and protocol
    """
    if not isinstance(config, dict):
        return config
    secret_keys = {
        "password",
        "pwd",
        "secret",
        "token",
        "access_token",
        "api_key",
        "client_secret",
    }
    alias_map = {
        "pwd": "password",  # nosec B105
        "schema_name": "schema",
        "dbname": "database",
        "db": "database",
        "tdpid": "host",
    }

    def normalize_dict(d: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, v in d.items():
            lk = (k or "").lower()
            if lk in secret_keys:
                continue
            lk = alias_map.get(lk, lk)
            if lk == "ssl_mode":
                if isinstance(v, str):
                    out[lk] = {"mode": v}
                    continue
                if isinstance(v, dict) and "mode" in v:
                    out[lk] = {"mode": v.get("mode")}
                    continue
            if lk == "host" and isinstance(v, str):
                hv = v.strip()
                if "://" in hv:
                    hv = hv.split("://", 1)[1]
                out[lk] = hv
                continue
            if isinstance(v, dict):
                out[lk] = normalize_dict(v)
            elif isinstance(v, list):
                out[lk] = [
                    normalize_dict(i) if isinstance(i, dict) else _coerce_scalar(i) for i in v
                ]
            else:
                out[lk] = _coerce_scalar(v)
        return out

    def _coerce_scalar(v: Any) -> Any:
        if isinstance(v, str) and v.isdigit():
            try:
                return int(v)
            except Exception:
                return v
        return v

    out = normalize_dict(config)
    return out


def _coerce_to_type(value: Any, type_decl: Any) -> Any:
    """Coerce a value to a JSON Schema primitive type when reasonable."""
    if value is None or type_decl is None:
        return value
    types: tuple[str, ...]
    types = tuple(type_decl) if isinstance(type_decl, list) else (str(type_decl),)
    v = value
    try:
        if "integer" in types and isinstance(v, str) and v.isdigit():
            return int(v)
        if "number" in types and isinstance(v, str):
            return float(v)
        if "boolean" in types and isinstance(v, str):
            low = v.strip().lower()
            if low in ("true", "1", "yes"):
                return True
            if low in ("false", "0", "no"):
                return False
        if "string" in types and not isinstance(v, str):
            return str(v)
    except Exception:
        return value
    return value


def _normalize_with_spec(config: Any, schema: dict[str, Any] | None) -> Any:
    """Normalize config guided by a connector JSON Schema (connectionSpecification).

    - Drops fields marked airbyte_secret
    - Coerces primitives to declared types when safe
    - Handles oneOf/anyOf by selecting a matching branch using required keys/const enums
    - Normalizes ssl_mode-like patterns where string may map to object with const discriminator
    - Preserves unknown keys with baseline normalization
    """
    if schema is None:
        return (
            _baseline_normalize_config(config if isinstance(config, dict) else {"value": config})
            if isinstance(config, dict)
            else config
        )

    def _branch_score_for_dict(val: dict[str, Any], branch: dict[str, Any]) -> int:
        props = branch.get("properties", {}) if isinstance(branch, dict) else {}
        score = 0
        for k in props or {}:
            if k in val or k.lower() in [kk.lower() for kk in val]:
                score += 1
        for pname, ps in (props or {}).items():
            if isinstance(ps, dict):
                const = ps.get("const")
                enum = ps.get("enum")
                if (
                    const is not None
                    and str(val.get(pname, val.get(pname.lower(), "")).lower())
                    == str(const).lower()
                ):
                    score += 2
                if isinstance(enum, list) and str(
                    val.get(pname, val.get(pname.lower(), "")).lower()
                ) in [str(e).lower() for e in enum]:
                    score += 2
        return score

    def _select_branch(schema_obj: dict[str, Any], value: Any) -> dict[str, Any] | None:
        one_of = schema_obj.get("oneOf") if isinstance(schema_obj, dict) else None
        any_of = schema_obj.get("anyOf") if isinstance(schema_obj, dict) else None
        candidates = None
        if isinstance(one_of, list) and one_of:
            candidates = one_of
        elif isinstance(any_of, list) and any_of:
            candidates = any_of
        if not candidates:
            return None
        if isinstance(value, dict):
            best = None
            best_score = -1
            for br in candidates:
                sc = _branch_score_for_dict(value, br)
                if sc > best_score:
                    best = br
                    best_score = sc
            return best or candidates[0]
        if isinstance(value, str):
            for br in candidates:
                props = br.get("properties", {})
                for _, ps in (props or {}).items():
                    if isinstance(ps, dict):
                        const = ps.get("const")
                        enum = ps.get("enum")
                        if const and str(const).lower() == value.lower():
                            return br
                        if isinstance(enum, list) and value.lower() in [
                            str(e).lower() for e in enum
                        ]:
                            return br
            return candidates[0]
        return candidates[0]

    typ = schema.get("type") if isinstance(schema, dict) else None
    if typ in ("string", "integer", "number", "boolean") or (
        isinstance(typ, list) and any(t in ("string", "integer", "number", "boolean") for t in typ)
    ):
        return _coerce_to_type(config, typ)
    if (
        isinstance(config, dict)
        and isinstance(schema, dict)
        and (
            schema.get("type") == "object"
            or "properties" in schema
            or "oneOf" in schema
            or ("anyOf" in schema)
        )
    ):
        chosen = _select_branch(schema, config) or schema
        props = chosen.get("properties", {})
        out: dict[str, Any] = {}
        key_map = {(k or "").lower(): k for k in config}
        for pname, pschema in (props or {}).items():
            ck = pname if pname in config else key_map.get(pname.lower())
            found_val = config.get(ck) if ck else None
            if (
                found_val is None
                and isinstance(pschema, dict)
                and (
                    pschema.get("type") == "object"
                    or "properties" in pschema
                    or "oneOf" in pschema
                    or ("anyOf" in pschema)
                )
            ):
                sub_candidates: dict[str, Any] = {}
                if "oneOf" in pschema and isinstance(pschema["oneOf"], list):
                    for br in pschema["oneOf"]:
                        for spname, sps in (br.get("properties", {}) or {}).items():
                            sub_candidates.setdefault(spname, sps)
                elif "anyOf" in pschema and isinstance(pschema["anyOf"], list):
                    for br in pschema["anyOf"]:
                        for spname, sps in (br.get("properties", {}) or {}).items():
                            sub_candidates.setdefault(spname, sps)
                else:
                    sub_candidates = pschema.get("properties", {}) or {}
                collected: dict[str, Any] = {}
                for spname in sub_candidates or {}:
                    spk = spname if spname in config else key_map.get(spname.lower())
                    if spk is not None and spk in config:
                        collected[spname] = config.get(spk)
                if collected:
                    found_val = collected
            if isinstance(pschema, dict) and pschema.get("airbyte_secret") is True:
                continue
            if found_val is None:
                continue
            if isinstance(pschema, dict) and ("oneOf" in pschema or "anyOf" in pschema):
                br = (
                    _select_branch(pschema, found_val)
                    or (pschema.get("oneOf") or pschema.get("anyOf") or [{}])[0]
                )
                bprops = br.get("properties", {})
                if not isinstance(found_val, dict):
                    placed = False
                    for bpname, bps in (bprops or {}).items():
                        const = (bps or {}).get("const")
                        enum = (bps or {}).get("enum")
                        if const or enum:
                            out[pname] = {bpname: found_val}
                            placed = True
                            break
                    if not placed:
                        fp = next(iter((bprops or {}).keys()), None)
                        out[pname] = {fp: found_val} if fp else {"value": found_val}
                else:
                    shaped_child: dict[str, Any] = {}
                    for bpname, bps in (bprops or {}).items():
                        if isinstance(bps, dict) and bps.get("airbyte_secret") is True:
                            continue
                        if bpname in found_val:
                            shaped_child[bpname] = _normalize_with_spec(found_val[bpname], bps)
                        else:
                            const = (bps or {}).get("const")
                            default = (bps or {}).get("default")
                            if const is not None:
                                shaped_child[bpname] = const
                            elif default is not None:
                                shaped_child[bpname] = default
                    out[pname] = shaped_child
                continue
            out[pname] = _normalize_with_spec(found_val, pschema)
        known = set((props or {}).keys())
        for k, v in config.items():
            if k not in known and k.lower() not in [p.lower() for p in known]:
                if isinstance(v, dict):
                    out[(k or "").lower()] = _baseline_normalize_config(v)
                elif isinstance(v, list):
                    out[(k or "").lower()] = [
                        _baseline_normalize_config(i) if isinstance(i, dict) else i for i in v
                    ]
                else:
                    out[(k or "").lower()] = v
        return out
    if isinstance(config, list) and isinstance(schema, dict) and (schema.get("type") == "array"):
        item_schema = schema.get("items")
        return [_normalize_with_spec(i, item_schema) for i in config]
    return config


def _shape_config_to_spec(
    config: dict[str, Any], schema: dict[str, Any] | None, connector_type: str | None = None
) -> dict[str, Any]:
    """Transform user-provided connector configuration to match the connector's JSON Schema spec.

    This function reformats configurations to align with connector specifications defined in
    the Airbyte OSS registry (https://connectors.airbyte.com/files/registries/v0/oss_registry.json).

    Generic rules (applied to all connectors):
    - Case-insensitive property matching with alias support (pwd→password, dbname→database)
    - Automatic nesting of flattened properties into their parent objects
    - oneOf/anyOf branch selection using discriminators (const/enum values)
    - Type coercion for primitives (string→int, string→bool when safe)
    - Respects additionalProperties: unknown keys only preserved if explicitly allowed

    Teradata-specific transformations:
    - database ≡ schema: Both treated as synonyms, always output as 'schema' property
    - Flat credentials → logmech object: {username, password} → {logmech: {auth_type: "TD2", username, password}}
    - Prevents field duplication by tracking used keys

    Args:
        config: User-provided configuration dictionary (potentially flat or misaligned)
        schema: JSON Schema from connector spec (connectionSpecification)
        connector_type: Connector name/type for connector-specific handling (e.g., 'teradata', 'postgres')

    Returns:
        Reformatted configuration matching the connector's expected structure
    """
    if not isinstance(config, dict) or schema is None:
        return dict(config or {})

    # Simple alias map (non-circular)
    alias_map = {
        "pwd": "password",  # nosec B105
        "schema_name": "schema",
        "default_schema": "schema",
        "dbname": "database",
        "db": "database",
    }

    # Detect if this is Teradata
    is_teradata = connector_type and "teradata" in connector_type.lower()

    def _key_lookup(d: dict[str, Any], name: str) -> str | None:
        if name in d:
            return name
        low = name.lower()
        for k in list(d.keys()):
            if k == name:
                return k
            if k.lower() == low:
                return k
            if alias_map.get(k.lower()) == low:
                return k
        return None

    def _select_branch_generic(val: Any, candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not candidates:
            return None
        if isinstance(val, str):
            for br in candidates:
                props = br.get("properties", {}) if isinstance(br, dict) else {}
                for _, ps in (props or {}).items():
                    const = (ps or {}).get("const")
                    enum = (ps or {}).get("enum")
                    if const and str(const).lower() == val.lower():
                        return br
                    if isinstance(enum, list) and str(val).lower() in [
                        str(e).lower() for e in enum
                    ]:
                        return br
            return candidates[0]
        if isinstance(val, dict):
            best = None
            best_score = -1
            for br in candidates:
                props = br.get("properties", {}) if isinstance(br, dict) else {}
                score = 0
                for k in props or {}:
                    if k in val or k.lower() in [kk.lower() for kk in val]:
                        score += 1
                for pname, ps in (props or {}).items():
                    const = (ps or {}).get("const")
                    enum = (ps or {}).get("enum")
                    if (
                        const is not None
                        and str(val.get(pname, val.get(pname.lower(), "")).lower())
                        == str(const).lower()
                    ):
                        score += 2
                    if isinstance(enum, list) and str(
                        val.get(pname, val.get(pname.lower(), "")).lower()
                    ) in [str(e).lower() for e in enum]:
                        score += 2
                if score > best_score:
                    best = br
                    best_score = score
            return best or candidates[0]
        return candidates[0]

    def _shape(d: dict[str, Any], sch: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(sch, dict):
            return d
        if "oneOf" in sch and isinstance(sch["oneOf"], list):
            chosen = _select_branch_generic(d, sch["oneOf"]) or sch["oneOf"][0]
            return _shape(d, chosen)
        if sch.get("type") == "object" or "properties" in sch:
            props = sch.get("properties", {})
            out: dict[str, Any] = {}
            used_keys: set[str] = set()

            # Special handling for Teradata ONLY: database -> schema mapping and logmech handling
            db_schema_synonyms = []
            logmech_override = None
            logmech_used_keys = set()
            if is_teradata:
                # Teradata: database and schema are synonyms - treat as schema
                # The Teradata spec uses 'schema', not 'database'
                # If user provides database OR schema (or both), map to 'schema' property
                if "schema" in props:
                    # Get value from database, schema, default_schema, dbname, or db (first non-None)
                    db_val = (
                        d.get("database")
                        or d.get("schema")
                        or d.get("default_schema")
                        or d.get("dbname")
                        or d.get("db")
                    )
                    if db_val:
                        db_schema_synonyms = [("schema", db_val)]
                        # Mark all database-related keys as used to prevent duplication in output
                        for k in ["database", "default_schema", "dbname", "db"]:
                            if k in d:
                                logmech_used_keys.add(k)

                # Handle logmech: if flat username/password are provided but logmech object is expected
                if "logmech" in props and "logmech" not in d:
                    # Check if username and password are provided at top level
                    username = d.get("username") or d.get("user")
                    password = d.get("password") or d.get("pwd")
                    if username and password:
                        # Create logmech object with TD2 auth
                        logmech_override = {
                            "auth_type": "TD2",
                            "username": username,
                            "password": password,
                        }
                        # Mark the flat credential fields as used so they don't get copied to output
                        for k in ["username", "user", "password", "pwd"]:
                            if k in d:
                                logmech_used_keys.add(k)

            # Add Teradata-specific used keys to the main set
            used_keys.update(logmech_used_keys)

            for pname, pschema in props.items():
                # Check for Teradata logmech override
                if is_teradata and pname == "logmech" and logmech_override is not None:
                    val = logmech_override
                    src_key = "logmech"
                # Check if this property should use a synonym value (Teradata schema mapping)
                elif (
                    synonym_val := next((v for k, v in db_schema_synonyms if k == pname), None)
                ) is not None:
                    val = synonym_val
                    src_key = pname
                else:
                    src_key = _key_lookup(d, pname)
                    val = d.get(src_key) if src_key else None
                if (
                    val is None
                    and isinstance(pschema, dict)
                    and (pschema.get("type") == "object" or "properties" in pschema)
                ):
                    sub_props: dict[str, Any] = {}
                    if "oneOf" in pschema and isinstance(pschema["oneOf"], list):
                        for br in pschema["oneOf"]:
                            for spname, spschema in (br.get("properties", {}) or {}).items():
                                sub_props.setdefault(spname, spschema)
                    elif "anyOf" in pschema and isinstance(pschema["anyOf"], list):
                        for br in pschema["anyOf"]:
                            for spname, spschema in (br.get("properties", {}) or {}).items():
                                sub_props.setdefault(spname, spschema)
                    else:
                        sub_props = pschema.get("properties", {})
                    sub_obj: dict[str, Any] = {}
                    for spname, spschema in sub_props.items():  # noqa: B007
                        sp_key = _key_lookup(d, spname)
                        if sp_key is not None:
                            sub_obj[spname] = d.get(sp_key)
                            used_keys.add(sp_key)
                    if sub_obj:
                        val = sub_obj
                if (
                    isinstance(pschema, dict)
                    and "oneOf" in pschema
                    and (val is not None)
                    and (not isinstance(val, dict))
                ):
                    branch = _select_branch_generic(val, pschema.get("oneOf", []))
                    if branch:
                        bprops = branch.get("properties", {})
                        placed = False
                        for bpname, bps in bprops.items():
                            if isinstance(bps, dict):
                                const = bps.get("const")
                                enum = bps.get("enum")
                                if const or enum:
                                    out[pname] = {bpname: val}
                                    placed = True
                                    break
                        if not placed:
                            fp = next(iter(bprops.keys()), None)
                            out[pname] = {fp: val} if fp else {"value": val}
                        continue
                if isinstance(val, dict) and isinstance(pschema, dict):
                    chosen_child = pschema
                    if "oneOf" in pschema and isinstance(pschema["oneOf"], list):
                        chosen_child = (
                            _select_branch_generic(val, pschema["oneOf"]) or pschema["oneOf"][0]
                        )
                    elif "anyOf" in pschema and isinstance(pschema["anyOf"], list):
                        chosen_child = (
                            _select_branch_generic(val, pschema["anyOf"]) or pschema["anyOf"][0]
                        )
                    shaped_child: dict[str, Any] = {}
                    for bpname, bps in (chosen_child.get("properties", {}) or {}).items():
                        if bpname in val:
                            shaped_child[bpname] = _shape(val[bpname], bps)
                        else:
                            const = (bps or {}).get("const")
                            default = (bps or {}).get("default")
                            if const is not None:
                                shaped_child[bpname] = const
                            elif default is not None:
                                shaped_child[bpname] = default
                    out[pname] = shaped_child
                elif val is not None:
                    out[pname] = _coerce_to_type(
                        val, pschema.get("type") if isinstance(pschema, dict) else None
                    )
                if src_key:
                    used_keys.add(src_key)

            # Only preserve unknown keys if schema explicitly allows additionalProperties
            allows_additional = sch.get("additionalProperties", False)
            if allows_additional is True:
                for k, v in d.items():
                    if k not in used_keys and _key_lookup(d, k) not in used_keys:
                        out[k] = v
            return out
        return d

    return _shape(dict(config), dict(schema))


def _configs_equivalent(
    a: dict[str, Any], b: dict[str, Any], spec: dict[str, Any] | None = None
) -> bool:
    try:
        if spec:
            return _normalize_with_spec(a, spec) == _normalize_with_spec(b, spec)
        return _baseline_normalize_config(a) == _baseline_normalize_config(b)
    except Exception:
        return False


def register_data_movement_tools(orchestrator: PipelineOrchestrator) -> dict[str, Any]:
    """
    Register data movement tools.

    Args:
        orchestrator: Pipeline orchestrator instance

    Returns:
        Dictionary of tool functions
    """

    _cached_pm_tools: dict[str, Any] | None = None

    def _get_pipeline_tools() -> dict[str, Any]:
        nonlocal _cached_pm_tools
        if _cached_pm_tools is None:
            from . import airflow_pipeline_management

            _cached_pm_tools = airflow_pipeline_management.register_pipeline_tools(orchestrator)
        return _cached_pm_tools

    def _normalize_sync_catalog(
        catalog: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        if not catalog:
            return []
        streams = catalog.get("streams", []) or []
        norm = []
        for entry in streams:
            stream = entry.get("stream") or {}
            cfg = entry.get("config") or {}
            name = stream.get("name") or entry.get("name")
            if not name:
                continue
            item = {
                "name": name,
                "syncMode": cfg.get("syncMode") or entry.get("syncMode"),
                "destinationSyncMode": cfg.get("destinationSyncMode")
                or entry.get("destinationSyncMode"),
                "selected": cfg.get(
                    "selected",
                    True if entry.get("selected") is None else entry.get("selected"),
                ),
                "cursorField": tuple(cfg.get("cursorField") or entry.get("cursorField") or []),
                "primaryKey": tuple(
                    tuple(pk) for pk in cfg.get("primaryKey") or entry.get("primaryKey") or []
                ),
            }
            norm.append(item)
        norm.sort(key=lambda x: x["name"])
        return norm

    async def _create_airbyte_connection(
        connection_name: str,
        source_id: str | None = None,
        destination_id: str | None = None,
        source_name: str | None = None,
        destination_name: str | None = None,
        streams: list[dict[str, Any]] | None = None,
        selected_streams: list[dict[str, Any]] | None = None,
        schedule_type: str | None = None,
        schedule_cron: str | None = None,
        namespace_definition: str = "destination",
        namespace_format: str | None = None,
        _discovery_cache: Optional["DiscoveryCache"] = None,
    ) -> dict[str, Any]:
        """
        Create an Airbyte connection between source and destination.

        Sets up a connection with specified streams, sync mode, and
        scheduling configuration.

        IMPORTANT SYNC MODE HANDLING:
        - Do NOT assume or fill in default values for 'syncMode' and 'destinationSyncMode'.
        - ONLY include 'syncMode' and 'destinationSyncMode' in a stream if the user has
          EXPLICITLY stated their sync preferences (e.g., "incremental sync", "append mode",
          "full refresh", "overwrite", etc.).
        - If the user has NOT mentioned how they want to sync, do NOT include 'syncMode' or
          'destinationSyncMode' in the stream objects. The tool will return a clarification
          request so the user can be asked about their sync preferences.
        - Valid syncMode values (when user specifies): 'full_refresh', 'incremental'
        - Valid destinationSyncMode values (when user specifies): 'overwrite', 'append', 'append_dedup'

        Args:
            source_name: Source connection name
            destination_name: Destination connection name
            connection_name: Name for the new connection
            streams: List of stream configurations (name required; syncMode and destinationSyncMode
                     only if the user explicitly specified them)
            schedule_type: "manual" or "cron". Defaults to manual when omitted.
                Passing "manual" with schedule_cron is a conflict error.
            schedule_cron: Cron expression (e.g. "0 2 * * *"). If provided
                without schedule_type, implies cron. Required when
                schedule_type is "cron".
            namespace_definition: Namespace handling (source, destination, custom). Defaults to 'destination'.
            namespace_format: Custom namespace format if applicable

        Returns:
            Dictionary with connection creation results, or clarification request if sync config missing
        """
        try:
            if not isinstance(connection_name, str) or not connection_name.strip():
                return {
                    "success": False,
                    "error": "Parameter 'connection_name' must be a non-empty string.",
                }
            connection_name = connection_name.strip()
            logger.info("Creating Airbyte connection: %s", connection_name)
            if not source_id and source_name:
                src = await orchestrator.airbyte_client.get_source_by_name(source_name)
                source_id = src.get("sourceId") if src else None
            if not destination_id and destination_name:
                dst = await orchestrator.airbyte_client.get_destination_by_name(destination_name)
                destination_id = dst.get("destinationId") if dst else None
            if not source_id or not destination_id:
                missing = []
                if not source_id:
                    missing.append(
                        f"source {source_name!r}"
                        if source_name
                        else "source (no name or ID provided)"
                    )
                if not destination_id:
                    missing.append(
                        f"destination {destination_name!r}"
                        if destination_name
                        else "destination (no name or ID provided)"
                    )
                return {
                    "success": False,
                    "error": f"Not found: {', '.join(missing)}",
                    "connection_name": connection_name,
                }
            selected = selected_streams if selected_streams is not None else streams or []
            logger.info("Configuring %s streams for connection before normalization", selected)

            selected = [_normalize_stream_item(s) for s in selected]
            logger.info("Selected %d streams for connection", len(selected))
            logger.debug("Selected streams detail: %s", selected)

            # Validate that every stream has explicit sync configuration
            non_wildcard = [s for s in selected if s.get("name") != "*"]
            if non_wildcard:
                sync_clarification = _validate_sync_modes(non_wildcard)
                if sync_clarification:
                    return sync_clarification

            # Validate cursor field for incremental streams using schema discovery
            cursor_clarification = await _validate_cursor_fields(
                non_wildcard,
                source_id,
                orchestrator.airbyte_client,
                discovery_cache=_discovery_cache,
            )
            if cursor_clarification:
                return cursor_clarification

            # Validate that requested stream names exist in the source
            stream_name_clarification = await _validate_stream_names(
                non_wildcard,
                source_id,
                orchestrator.airbyte_client,
                discovery_cache=_discovery_cache,
            )
            if stream_name_clarification:
                return stream_name_clarification

            if any(s.get("name") == "*" for s in selected):
                logger.info("Expanding wildcard stream selection '*' to all available streams")
                wildcard = next((s for s in selected if s.get("name") == "*"), {})
                if _discovery_cache:
                    disc = await _discovery_cache.get(source_id)
                else:
                    disc = await orchestrator.airbyte_client.discover_source_schema(source_id)
                catalog = (disc or {}).get("catalog") or {}
                available = catalog.get("streams", []) or []
                expanded: list[dict[str, Any]] = []
                for entry in available:
                    stream_obj = entry.get("stream", {})
                    sname = stream_obj.get("name") or entry.get("name")
                    if not sname:
                        continue
                    item = {
                        "name": sname,
                        "syncMode": wildcard.get("syncMode", "full_refresh"),
                        "destinationSyncMode": wildcard.get("destinationSyncMode", "overwrite"),
                        "selected": True,
                    }
                    if wildcard.get("cursorField"):
                        item["cursorField"] = wildcard.get("cursorField")
                    if wildcard.get("primaryKey"):
                        item["primaryKey"] = wildcard.get("primaryKey")
                    expanded.append(item)
                selected = expanded
            logger.info("Configuring %d streams for connection", len(selected))
            _cached_disc = (await _discovery_cache.get(source_id)) if _discovery_cache else None
            sync_catalog = await orchestrator.airbyte_client.build_configured_catalog(
                source_id=source_id,
                selected_streams=selected,
                discovery_result=_cached_disc,
            )
            logger.info(
                "Built configured sync catalog for connection with %d streams",
                len(sync_catalog.get("streams", [])),
            )
            logger.debug("Sync catalog detail: %s", sync_catalog)
            # Validate schedule params before reuse checks so that
            # contradictions and missing fields always fail, even if an
            # existing connection would match.
            st_lower = str(schedule_type or "").lower()
            if st_lower == "cron" and not schedule_cron:
                return {
                    "success": False,
                    "error": "schedule_cron is required when schedule_type is 'cron'",
                }
            if st_lower == "manual" and schedule_cron:
                return {
                    "success": False,
                    "error": (
                        "Conflicting parameters: schedule_type is 'manual' "
                        "but schedule_cron was provided. Remove schedule_cron "
                        "or set schedule_type to 'cron'."
                    ),
                }
            expected_schedule_type = "cron" if schedule_cron else "manual"
            # Build expected stream info for comparison
            expected_stream_info = sorted(
                [
                    (
                        entry.get("stream", {}).get("name"),
                        to_public_api_sync_mode(
                            entry.get("config", {}).get("syncMode", "full_refresh"),
                            entry.get("config", {}).get("destinationSyncMode", "overwrite"),
                        ).lower(),
                    )
                    for entry in sync_catalog.get("streams", [])
                    if entry.get("stream", {}).get("name")
                ]
            )
            existing_connections = await orchestrator.airbyte_client.list_connections()
            for c in existing_connections:
                # Match on sourceId + destinationId (the actual endpoints),
                # regardless of connection name (which the LLM may vary)
                if c.get("sourceId") == source_id and c.get("destinationId") == destination_id:
                    full = await orchestrator.airbyte_client.get_connection(c.get("connectionId"))
                    full_schedule_type = (
                        full.get("schedule", {}).get("scheduleType")
                        or full.get("scheduleType")
                        or "manual"
                    )
                    same_schedule = full_schedule_type == expected_schedule_type
                    full_config = full.get("configurations", {})
                    full_streams = full_config.get("streams", [])
                    # Build comparable tuples of (name, combinedSyncMode) for existing connection
                    existing_stream_info = sorted(
                        [
                            (s.get("name"), (s.get("syncMode") or "").lower())
                            for s in full_streams
                            if s.get("name")
                        ]
                    )
                    same_streams = existing_stream_info == expected_stream_info
                    logger.info(
                        "Connection reuse check: existing='%s' streams=%s, "
                        "expected='%s' streams=%s, match=%s, schedule_match=%s",
                        c.get("name"),
                        existing_stream_info,
                        connection_name,
                        expected_stream_info,
                        same_streams,
                        same_schedule,
                    )
                    if same_streams:
                        conn_id = c.get("connectionId")
                        # H3: If schedule differs, return clarification instead of
                        # silently mutating a live pipeline's schedule.
                        if not same_schedule:
                            return {
                                "success": False,
                                "clarification_needed": True,
                                "message": (
                                    f"Connection '{c.get('name')}' exists with schedule "
                                    f"'{full_schedule_type}'. Requested: '{expected_schedule_type}'. "
                                    f"Call update_airbyte_connection to change it explicitly."
                                ),
                                "existing_connection_id": conn_id,
                                "connection_name": c.get("name"),
                                "current_schedule": full_schedule_type,
                                "requested_schedule": expected_schedule_type,
                            }
                        logger.info(
                            "Reusing existing connection '%s' (ID %s) — matches source, "
                            "destination, streams, and sync modes.",
                            c.get("name"),
                            conn_id,
                        )
                        return {
                            "success": True,
                            "connection_name": c.get("name"),
                            "connection_id": conn_id,
                            "source_id": source_id,
                            "destination_id": destination_id,
                            "streams_configured": len(selected),
                            "schedule_type": expected_schedule_type,
                            "status": full.get("status"),
                            "reused": True,
                            "next_steps": [
                                (
                                    f"**1. Trigger a sync now** (optional): "
                                    f"`airbyte_sync(action='trigger', "
                                    f"connection_id='{conn_id}')`. **Why**: "
                                    f"the connection already existed but may "
                                    f"not have been run recently; an explicit "
                                    f"trigger refreshes the destination "
                                    f"tables. **Effect**: Airbyte launches a "
                                    f"sync job and returns a job_id. **If "
                                    f"missing**: skip if "
                                    f"``schedule_type='cron'`` already covers "
                                    f"the cadence."
                                ),
                                (
                                    f"**2. Check pipeline health**: "
                                    f"`airbyte_pipeline(action='check_health', "
                                    f"connection_id='{conn_id}')`. **Why**: "
                                    f"reuse means we did not validate the "
                                    f"endpoints in this call; a health probe "
                                    f"surfaces silently-broken sources or "
                                    f"destinations. **Effect**: returns the "
                                    f"connection-level status plus per-"
                                    f"endpoint reachability. **If missing**: "
                                    f"skip if you trust the existing "
                                    f"connection."
                                ),
                            ],
                        }
            api_streams: list[dict[str, Any]] = []
            logger.info("Before iterating through sync catalog streams to build API streams")
            logger.debug("Sync catalog streams: %s", sync_catalog.get("streams", []))
            for entry in sync_catalog.get("streams", []):
                stream = entry.get("stream", {})
                config = entry.get("config", {})
                s_mode = config.get("syncMode", "full_refresh")
                d_mode = config.get("destinationSyncMode", "overwrite")
                combined_mode = to_public_api_sync_mode(s_mode, d_mode)
                stream_conf: dict[str, Any] = {
                    "name": stream.get("name"),
                    "syncMode": combined_mode,
                }
                if stream.get("namespace"):
                    stream_conf["namespace"] = stream.get("namespace")
                if config.get("cursorField"):
                    stream_conf["cursorField"] = config.get("cursorField")
                if config.get("primaryKey"):
                    stream_conf["primaryKey"] = config.get("primaryKey")
                logger.debug("Adding stream configuration for API: %s", stream_conf)
                api_streams.append(stream_conf)
            workspace_id = await orchestrator.airbyte_client._get_workspace_id()
            raw_payload: dict[str, Any] = {
                "workspaceId": workspace_id,
                "sourceId": source_id,
                "destinationId": destination_id,
                "name": connection_name,
                "namespaceDefinition": namespace_definition,
                "status": "active",
            }
            if namespace_format:
                raw_payload["namespaceFormat"] = namespace_format
            # Note: schedule_type="cron" without schedule_cron is already
            # rejected above (before reuse checks).
            if schedule_cron:
                raw_payload["schedule"] = {
                    "scheduleType": "cron",
                    "cronExpression": to_quartz_cron(schedule_cron),
                }
            else:
                raw_payload["schedule"] = {"scheduleType": "manual"}
            logger.info("Using 'configurations' field in connection creation payload")
            logger.debug(
                "Connection raw payload before creation: %s",
                _mask_sensitive_data(raw_payload) if isinstance(raw_payload, dict) else raw_payload,
            )
            logger.debug("Connection sync catalog before creation: %s", sync_catalog)
            logger.debug("Connection API streams before creation: %s", api_streams)
            raw_payload["configurations"] = {"streams": api_streams}
            raw_payload["syncCatalog"] = sync_catalog
            connection = await orchestrator.airbyte_client.create_connection(
                raw_payload=raw_payload
            )
            result = {
                "success": True,
                "connection_name": connection_name,
                "connection_id": connection.get("connectionId"),
                "source_id": source_id,
                "destination_id": destination_id,
                "streams_configured": len(selected),
                "schedule_type": schedule_type,
                "status": connection.get("status"),
                "next_steps": [
                    (
                        f"**1. Trigger the first sync**: "
                        f"`airbyte_sync(action='trigger', "
                        f"connection_id='{connection.get('connectionId')}')`. "
                        f"**Why**: the connection is configured but no rows "
                        f"land in the destination until a sync runs. "
                        f"**Effect**: Airbyte launches a sync job and "
                        f"returns a job_id you can wait on. **If missing**: "
                        f"skip if you set ``schedule_type='cron'`` and want "
                        f"Airbyte to run it on the cron itself."
                    ),
                    (
                        "**2. Build dbt staging on the destination data**: "
                        "`dbt_generate_model(model_type='staging', "
                        "source_database='<destination_db>', "
                        "source_tables=['<table>'])`. **Why**: raw Airbyte "
                        "tables are unstructured; a staging layer adds "
                        "renaming, casting, and tests for downstream "
                        "models. **Effect**: writes ``models/staging/"
                        "stg_<table>.sql`` plus a sources YAML. **If "
                        "missing**: skip if the consumer reads raw Airbyte "
                        "tables directly."
                    ),
                ],
            }
            logger.info("Airbyte connection created: %s", result.get("connection_id"))
            return result
        except Exception as e:
            logger.error("Failed to create Airbyte connection: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "connection_name": connection_name,
            }

    async def _trigger_airbyte_sync(
        connection_id: str, wait_for_completion: bool = False
    ) -> dict[str, Any]:
        """
        Trigger an Airbyte sync job.

        Starts a data sync for the specified connection, optionally
        waiting for completion and returning job results.

        Args:
            connection_id: Connection name or ID
            wait_for_completion: Whether to wait for sync to complete

        Returns:
            Dictionary with sync job information
        """
        try:
            logger.info("Triggering Airbyte sync: %s", connection_id)
            job_info = await orchestrator.airbyte_client.trigger_sync(connection_id=connection_id)
            job_id = job_info.get("jobId")
            result = {
                "success": True,
                "connection_id": connection_id,
                "job_id": job_id,
                "status": job_info.get("status", "pending"),
                "created_at": job_info.get("createdAt"),
            }
            if wait_for_completion and job_id:
                logger.info("Waiting for sync job %s to complete", job_id)
                final_status = await orchestrator.airbyte_client.wait_for_job(job_id)
                result["final_status"] = final_status
                result["status"] = final_status.get("status")
            if job_id:
                if wait_for_completion:
                    final_state = (result.get("status") or "").lower()
                    if final_state in {"succeeded", "success"}:
                        result["next_steps"] = [
                            (
                                "**1. Inspect what landed**: "
                                "`teradata_discover(action='list_tables', "
                                "database='<destination_db>')` to confirm "
                                "row counts. **Why**: a green Airbyte job "
                                "means the records flowed; verifying the "
                                "target tables catches schema drift / empty "
                                "loads early. **Effect**: returns the "
                                "current table list and row counts in the "
                                "Airbyte destination database. **If "
                                "missing**: skip if downstream dbt models "
                                "will surface the issue."
                            ),
                            (
                                "**2. Build dbt staging on the new data**: "
                                "`dbt_generate_model(model_type='staging', "
                                "source_database='<destination_db>', "
                                "source_tables=['<table>'])`. **Why**: raw "
                                "Airbyte tables are unstructured; staging "
                                "adds renaming, casting, and tests. "
                                "**Effect**: writes ``models/staging/"
                                "stg_<table>.sql`` plus a sources YAML. **If "
                                "missing**: skip if you only needed raw "
                                "replication."
                            ),
                        ]
                    elif final_state in {"failed", "error", "cancelled"}:
                        result["next_steps"] = [
                            (
                                f"**1. Get sync logs**: "
                                f"`airbyte_sync(action='status', "
                                f"job_id={job_id}, include_logs=True)`. "
                                f"**Why**: a failed sync blocks downstream "
                                f"dbt; logs surface schema mismatches, "
                                f"credential issues, or rate-limit errors. "
                                f"**Effect**: returns the job's stderr / "
                                f"trace lines. **If missing**: you'll have "
                                f"to debug from the Airbyte UI."
                            ),
                            (
                                "**2. Re-trigger after fixing**: "
                                "`airbyte_sync(action='trigger', "
                                "connection_id='<id>')`. **Why**: once the "
                                "underlying issue is corrected, the next "
                                "incremental sync will pick up where this "
                                "one stopped. **Effect**: kicks off another "
                                "Airbyte job. **If missing**: the data gap "
                                "stays until the next scheduled sync."
                            ),
                        ]
                else:
                    result["next_steps"] = [
                        (
                            f"**1. Wait for job completion**: "
                            f"`airbyte_sync(action='wait', job_id={job_id})`. "
                            f"**Why**: the sync was kicked off "
                            f"asynchronously; downstream dbt steps need to "
                            f"know whether it succeeded before they run. "
                            f"**Effect**: blocks until Airbyte returns a "
                            f"terminal state for the job. **If missing**: "
                            f"poll ``airbyte_sync(action='status', "
                            f"job_id={job_id})`` instead."
                        ),
                        (
                            "**2. Run dbt against the destination**: "
                            "`dbt_execute(command='run')`. **Why**: once "
                            "the sync is done, the next concrete step is "
                            "to materialize models on the freshly-loaded "
                            "data. **Effect**: dbt-teradata creates / "
                            "refreshes views and tables in the configured "
                            "database. **If missing**: skip if you only "
                            "needed raw replication."
                        ),
                    ]
            logger.info("Airbyte sync triggered: %s", job_id)
            return result
        except Exception as e:
            logger.error("Failed to trigger Airbyte sync: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "connection_id": connection_id,
            }

    async def _get_sync_status(job_id: int, include_logs: bool = False) -> dict[str, Any]:
        """
        Get status of an Airbyte sync job.

        Retrieves job status, progress, and optionally logs for
        monitoring and debugging.

        Args:
            job_id: Airbyte job ID
            include_logs: Whether to include job logs

        Returns:
            Dictionary with job status information
        """
        try:
            logger.info("Getting sync status for job: %s", job_id)
            job_info = await orchestrator.airbyte_client.get_job_status(job_id=job_id)
            result = {
                "job_id": job_info.get("jobId"),
                "status": job_info.get("status"),
                "created_at": job_info.get("startTime"),
                "started_at": job_info.get("startTime"),
                "updated_at": job_info.get("lastUpdatedAt"),
                "bytes_synced": job_info.get("bytesSynced", 0),
                "records_synced": job_info.get("rowsSynced", 0),
            }
            if include_logs:
                try:
                    logs = await orchestrator.airbyte_client.get_job_logs(job_id=job_id)
                    result["logs"] = logs.get("logLines", [])
                    result["log_count"] = len(logs.get("logLines", []))
                except AirbyteClientError as log_err:
                    logger.warning("Could not fetch job logs for job %s: %s", job_id, log_err)
                    result["logs"] = []
                    result["log_count"] = 0
                    err_msg = str(log_err)
                    if isinstance(log_err, AirbyteAPIError) and (
                        "(404)" in err_msg or "(405)" in err_msg
                    ):
                        result["logs_warning"] = (
                            "Job logs are not available via the Airbyte Public API v1. "
                            "Use the Airbyte UI to view detailed job logs."
                        )
                    else:
                        result["logs_warning"] = (
                            f"Could not fetch job logs: {safe_error_message(log_err)}"
                        )
            logger.info("Retrieved status for job %s: %s", job_id, result["status"])
            return result
        except Exception as e:
            logger.error("Failed to get sync status: %s", e, exc_info=True)
            return {"success": False, "error": safe_error_message(e), "job_id": job_id}

    async def _list_airbyte_connectors(
        connector_type: str = "both",
        search_term: str | None = None,
        limit: int = 100,  # noqa: ARG001
        offset: int = 0,  # noqa: ARG001
    ) -> dict[str, Any]:
        """
        List available Airbyte connectors (sources and destinations).

        Args:
            connector_type: Type of connector ('source', 'destination', or 'both')
            search_term: Optional search term to filter connectors by name

        Returns:
            Dictionary with connector listing results
        """
        try:
            logger.info("Listing %s connectors with search term: '%s'", connector_type, search_term)
            sources = []
            destinations = []

            if connector_type in ("source", "both"):
                sources = await orchestrator.airbyte_client.list_source_definitions()

            if connector_type in ("destination", "both"):
                destinations = await orchestrator.airbyte_client.list_destination_definitions()

            # Filter results by search term if provided
            if search_term:
                search_lower = search_term.lower()
                if connector_type in ("source", "both"):
                    sources = [s for s in sources if search_lower in s.get("name", "").lower()]
                if connector_type in ("destination", "both"):
                    destinations = [
                        d for d in destinations if search_lower in d.get("name", "").lower()
                    ]

            return {
                "success": True,
                "connector_type": connector_type,
                "search_term": search_term,
                "source_count": len(sources),
                "destination_count": len(destinations),
                "sources": sources,
                "destinations": destinations,
            }

        except Exception as e:
            logger.error("Failed to list connectors: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
            }

    async def _list_airbyte_connections() -> dict[str, Any]:
        """
        List all Airbyte connections (sync pipelines) in the configured workspace.

        Use this tool when the user asks to see, list, or browse their Airbyte
        connections, pipelines, or syncs. Returns connection IDs, names, source
        and destination details, status, and schedule for every connection.

        Returns:
            Dictionary with connection_count and a list of all connections.
        """
        try:
            ws_id = orchestrator.settings.airbyte.workspace_id
            if not ws_id:
                return {
                    "success": False,
                    "error": "Workspace ID not found in settings. Please set AIRBYTE_WORKSPACE_ID in your .env file.",
                }
            logger.info("Listing connections for workspace: %s", ws_id)
            connections = await orchestrator.airbyte_client.list_connections()
            return {
                "success": True,
                "workspace_id": ws_id,
                "connection_count": len(connections),
                "connections": connections,
            }
        except Exception as e:
            logger.error("Failed to list connections: %s", e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    async def _get_airbyte_connection_details(connection_id: str) -> dict[str, Any]:
        """
        Get detailed configuration for an Airbyte connection.

        Args:
            connection_id: The ID of the connection to retrieve.

        Returns:
            Dictionary with detailed connection configuration.
        """
        try:
            logger.info("Getting details for connection: %s", connection_id)
            connection_details = await orchestrator.airbyte_client.get_connection(
                connection_id=connection_id
            )
            return {"success": True, "connection": connection_details}
        except Exception as e:
            logger.error("Failed to get connection details: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "connection_id": connection_id,
            }

    async def _list_airbyte_sources() -> dict[str, Any]:
        """
        List all configured sources in the Airbyte workspace.

        Returns:
            Dictionary with a list of configured sources.
        """
        try:
            logger.info("Listing configured Airbyte sources.")
            sources = await orchestrator.airbyte_client.list_sources()
            return {
                "success": True,
                "source_count": len(sources),
                "sources": [
                    {
                        "name": s.get("name"),
                        "sourceId": s.get("sourceId"),
                        "sourceName": s.get("sourceName"),
                    }
                    for s in sources
                ],
            }
        except Exception as e:
            logger.error("Failed to list configured sources: %s", e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    async def _list_airbyte_destinations() -> dict[str, Any]:
        """
        List all configured destinations in the Airbyte workspace.

        Returns:
            Dictionary with a list of configured destinations.
        """
        try:
            logger.info("Listing configured Airbyte destinations.")
            destinations = await orchestrator.airbyte_client.list_destinations()
            return {
                "success": True,
                "destination_count": len(destinations),
                "destinations": [
                    {
                        "name": d.get("name"),
                        "destinationId": d.get("destinationId"),
                        "destinationName": d.get("destinationName"),
                    }
                    for d in destinations
                ],
            }
        except Exception as e:
            logger.error("Failed to list configured destinations: %s", e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    async def _list_streams(source_id: str) -> dict[str, Any]:
        """
        Discover and list all available streams for a given Airbyte source.

        This tool connects to the source, discovers its schema, and returns a
        list of all available data streams that can be synced.

        Args:
            source_id: The unique ID of the source to discover streams from.

        Returns:
            A dictionary containing the list of discovered streams and their configurations.
        """
        try:
            logger.info("Discovering streams for source ID: %s", source_id)
            schema_info = await orchestrator.airbyte_client.discover_source_schema(
                source_id=source_id
            )
            if "catalog" not in schema_info:
                return {
                    "success": False,
                    "source_id": source_id,
                    "error": "Schema discovery did not return a catalog. Check Airbyte server logs.",
                    "response": schema_info,
                }
            catalog = schema_info.get("catalog", {})
            streams = catalog.get("streams", [])
            if not streams:
                return {
                    "success": True,
                    "source_id": source_id,
                    "stream_count": 0,
                    "streams": [],
                    "message": "Schema discovery succeeded, but no streams were found for this source.",
                }
            formatted_streams = []
            for stream_data in streams:
                stream_config = stream_data.get("stream", {})
                formatted_streams.append(
                    {
                        "name": stream_config.get("name"),
                        "supported_sync_modes": stream_config.get("supportedSyncModes", []),
                        "source_defined_cursor": stream_config.get("sourceDefinedCursor"),
                        "default_cursor_field": stream_config.get("defaultCursorField", []),
                        "namespace": stream_config.get("namespace"),
                    }
                )
            return {
                "success": True,
                "source_id": source_id,
                "stream_count": len(formatted_streams),
                "streams": formatted_streams,
            }
        except Exception as e:
            logger.error("Failed to discover source streams: %s", e, exc_info=True)
            return {"success": False, "error": safe_error_message(e), "source_id": source_id}

    async def _select_streams_for_connection(
        connection_id: str, selected_streams: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Update an existing connection's selected streams using the 'configurations' object.

        Note: This helper builds the internal configured catalog and converts it
        into the public API 'configurations' payload. For most use-cases,
        prefer calling `update_airbyte_connection(connection_id, configurations={...})`
        directly, consolidating stream updates under the general updater.
        """
        logger.info(
            "Selecting streams for connection: %s using configurations object",
            connection_id,
        )
        logger.debug("Selected streams (raw): %s", selected_streams)
        try:
            logger.info("Selecting streams for connection: %s", connection_id)
            connection = await orchestrator.airbyte_client.get_connection(connection_id)
            logger.debug(
                "Current connection details retrieved: %s",
                _mask_sensitive_data(connection) if isinstance(connection, dict) else connection,
            )
            source_id = connection.get("sourceId")
            if not source_id:
                return {
                    "success": False,
                    "error": "Connection missing sourceId",
                    "connection_id": connection_id,
                }

            normalized_selected = [
                _normalize_stream_item(s, default_selected=False) for s in selected_streams or []
            ]
            logger.debug("Selected streams (normalized): %s", normalized_selected)
            sync_catalog = await orchestrator.airbyte_client.build_configured_catalog(
                source_id=source_id, selected_streams=normalized_selected
            )
            logger.info(
                "Built internal sync catalog with %d streams",
                len(sync_catalog.get("streams", [])),
            )
            api_streams = []
            for entry in sync_catalog.get("streams", []):
                stream = entry.get("stream", {})
                config = entry.get("config", {})
                s_mode = config.get("syncMode", "full_refresh")
                d_mode = config.get("destinationSyncMode", "overwrite")
                combined_mode = to_public_api_sync_mode(s_mode, d_mode)
                stream_conf = {"name": stream.get("name"), "syncMode": combined_mode}
                if config.get("cursorField"):
                    stream_conf["cursorField"] = config.get("cursorField")
                if config.get("primaryKey"):
                    stream_conf["primaryKey"] = config.get("primaryKey")
                api_streams.append(stream_conf)
            updated = await orchestrator.airbyte_client.update_connection(
                connection_id=connection_id, configurations={"streams": api_streams}
            )
            result = {
                "success": True,
                "connection_id": connection_id,
                "streams_configured": len(api_streams),
                "status": updated.get("status"),
            }
            logger.info("Updated selected streams for connection %s", connection_id)
            return result
        except Exception as e:
            logger.error("Failed to select streams for connection: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "connection_id": connection_id,
            }

    async def _update_airbyte_connection(
        connection_id: str,
        schedule_type: str | None = None,
        schedule_cron: str | None = None,
        namespace_definition: str | None = None,
        namespace_format: str | None = None,
        status: str | None = None,
        configurations: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Update an existing Airbyte connection's schedule and configuration.

        This provides a concise, general-purpose updater for common connection
        fields without re-provisioning source/destination.

        Args:
            connection_id: The connection to update.
            schedule_type: "manual" or "cron". Required for schedule changes.
                Passing "manual" with schedule_cron is a conflict error.
            schedule_cron: Cron expression (e.g. "0 2 * * *"). Required when
                schedule_type is "cron". If provided without schedule_type,
                implies cron scheduling.
            namespace_definition: One of Airbyte's namespace strategies (e.g., "source", "destination").
            namespace_format: Optional namespace format string.
            status: Optional connection status (e.g., "active", "inactive").
            configurations: Optional configurations object (e.g., {"streams": [...]}) to update selection or modes.

        Returns:
            Result dict with update status.
        """
        try:
            logger.info("Updating connection: %s", connection_id)
            update_kwargs: dict[str, Any] = {}
            if schedule_type:
                st = str(schedule_type).lower().strip()
                if st == "manual":
                    if schedule_cron:
                        return {
                            "success": False,
                            "error": (
                                "Conflicting parameters: schedule_type is 'manual' "
                                "but schedule_cron was provided. Remove schedule_cron "
                                "or set schedule_type to 'cron'."
                            ),
                            "connection_id": connection_id,
                        }
                    update_kwargs["schedule"] = {"scheduleType": "manual"}
                elif st == "cron":
                    if not schedule_cron:
                        return {
                            "success": False,
                            "error": "schedule_cron is required when schedule_type is 'cron'",
                            "connection_id": connection_id,
                        }
                    update_kwargs["schedule"] = {
                        "scheduleType": "cron",
                        "cronExpression": to_quartz_cron(schedule_cron),
                    }
                else:
                    return {
                        "success": False,
                        "error": f"Unsupported schedule_type: {schedule_type}",
                        "connection_id": connection_id,
                    }
            elif schedule_cron:
                # schedule_cron without explicit schedule_type implies cron
                update_kwargs["schedule"] = {
                    "scheduleType": "cron",
                    "cronExpression": to_quartz_cron(schedule_cron),
                }
            if namespace_definition is not None:
                update_kwargs["namespaceDefinition"] = namespace_definition
            if namespace_format is not None:
                update_kwargs["namespaceFormat"] = namespace_format
            if status is not None:
                update_kwargs["status"] = status
            if configurations is not None:
                update_kwargs["configurations"] = configurations
            if not update_kwargs:
                return {
                    "success": False,
                    "error": "No update fields were provided",
                    "connection_id": connection_id,
                }
            updated = await orchestrator.airbyte_client.update_connection(
                connection_id=connection_id, **update_kwargs
            )
            result = {
                "success": True,
                "connection_id": connection_id,
                "updated_fields": list(update_kwargs.keys()),
                "status": (updated or {}).get("status"),
            }
            logger.info("Connection %s updated: %s", connection_id, result["updated_fields"])
            return result
        except Exception as e:
            logger.error("Failed to update Airbyte connection: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "connection_id": connection_id,
            }

    async def _wait_for_sync_completion(
        job_id: int, timeout: int = 3600, poll_interval: int = 10
    ) -> dict[str, Any]:
        """
        Wait for an Airbyte sync job to complete and return final status.

        Args:
            job_id: Airbyte job ID
            timeout: Maximum seconds to wait
            poll_interval: Seconds between polling attempts

        Returns:
            Dictionary with the final job status and basic metrics.
        """
        try:
            logger.info("Waiting for Airbyte job to complete: %s", job_id)
            final_status = await orchestrator.airbyte_client.wait_for_job(
                job_id=job_id, timeout=timeout, poll_interval=poll_interval
            )
            result = {
                "success": True,
                "job_id": job_id,
                "status": final_status.get("job", {}).get("status"),
                "created_at": final_status.get("job", {}).get("createdAt"),
                "started_at": final_status.get("job", {}).get("startedAt"),
                "updated_at": final_status.get("job", {}).get("updatedAt"),
                "bytes_synced": final_status.get("attempts", [{}])[-1]
                .get("totalStats", {})
                .get("bytesEmitted", 0),
                "records_synced": final_status.get("attempts", [{}])[-1]
                .get("totalStats", {})
                .get("recordsEmitted", 0),
            }
            logger.info("Job %s finished with status: %s", job_id, result["status"])
            return result
        except Exception as e:
            logger.error("Failed while waiting for sync completion: %s", e, exc_info=True)
            return {"success": False, "error": safe_error_message(e), "job_id": job_id}

    async def _create_airbyte_source(
        name: str, source_definition_id: str, source_profile: str | None = None
    ) -> dict[str, Any]:
        """Create a new Airbyte source, validating by configuration to avoid duplicates.

        Credentials are resolved server-side — the LLM never handles passwords or API keys.
        Uses default Teradata credentials unless a specific profile is provided.

        Args:
            name: Display name for the source
            source_definition_id: Airbyte source definition ID
            source_profile: Optional profile name from connections.yaml. If not provided,
                uses the default Teradata connection credentials.
        """
        try:
            if not source_profile:
                # Defensive: the airbyte_manage router enforces Rule 5
                # upstream and rejects callers without a named profile.
                # Reaching here means a future call site bypassed the
                # boundary — fail loudly rather than silently fall back
                # to the wizard identity.
                raise ValueError(
                    "_create_airbyte_source called without source_profile. "
                    "Rule 5 requires a named connections.yaml profile for "
                    "Airbyte source creation; the wizard-default fallback "
                    "was removed. Pass source_profile through the "
                    "airbyte_manage router."
                )
            guard = orchestrator.credential_resolver.guard_configured()
            if guard:
                return guard
            connection_configuration = orchestrator.credential_resolver.resolve_profile(
                source_profile
            )
            result = await _find_or_create_connector(
                "source",
                name,
                source_definition_id,
                connection_configuration,
                orchestrator,
            )
            if result.get("success") and result.get("source_id"):
                result["next_steps"] = [
                    (
                        f"**1. Discover available streams**: "
                        f"`airbyte_inventory(action='list_streams', "
                        f"source_id='{result.get('source_id')}')`. "
                        f"**Why**: a source by itself moves no data; you "
                        f"need to know which streams are available before "
                        f"wiring a connection. **Effect**: Airbyte performs "
                        f"schema discovery and returns the stream catalog. "
                        f"**If missing**: skip if the user already knows "
                        f"the stream names."
                    ),
                    (
                        f"**2. Create the destination + connection**: "
                        f"`airbyte_manage(action='create_destination', ...)` "
                        f"then `airbyte_pipeline(action='create', "
                        f"source_id='{result.get('source_id')}', "
                        f"destination_id='<dest>', "
                        f"streams=[{{'name': '<stream>', "
                        f"'syncMode': '<mode>'}}])`. **Why**: the source is "
                        f"only useful when paired with a destination + "
                        f"explicit stream selection. **Effect**: creates the "
                        f"Airbyte connection that orchestrates the data "
                        f"flow. **If missing**: data stays in the source — "
                        f"nothing lands."
                    ),
                ]
            return sanitize_response(result)
        except ValueError as e:
            return {"success": False, "error": safe_error_message(e)}
        except Exception as e:
            logger.error("Failed to create Airbyte source: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to create Airbyte source. Check server logs for details.",
            }

    async def _create_airbyte_destination(
        name: str,
        destination_definition_id: str,
        destination_profile: str | None = None,
    ) -> dict[str, Any]:
        """Create a new Airbyte destination, validating by configuration to avoid duplicates.

        Credentials are resolved server-side — the LLM never handles passwords or API keys.
        Uses default Teradata credentials unless a specific profile is provided.

        Args:
            name: Display name for the destination
            destination_definition_id: Airbyte destination definition ID
            destination_profile: Optional profile name from connections.yaml. If not provided,
                uses the default Teradata connection credentials.
        """
        try:
            if not destination_profile:
                # Defensive: airbyte_manage router enforces Rule 5 upstream.
                # See _create_airbyte_source for rationale.
                raise ValueError(
                    "_create_airbyte_destination called without "
                    "destination_profile. Rule 5 requires a named "
                    "connections.yaml profile for Airbyte destination "
                    "creation; the wizard-default fallback was removed. "
                    "Pass destination_profile through the airbyte_manage "
                    "router."
                )
            guard = orchestrator.credential_resolver.guard_configured()
            if guard:
                return guard
            connection_configuration = orchestrator.credential_resolver.resolve_profile(
                destination_profile
            )
            result = await _find_or_create_connector(
                "destination",
                name,
                destination_definition_id,
                connection_configuration,
                orchestrator,
            )
            if result.get("success") and result.get("destination_id"):
                result["next_steps"] = [
                    (
                        f"**1. Wire the connection**: "
                        f"`airbyte_pipeline(action='create', "
                        f"source_id='<source>', "
                        f"destination_id='{result.get('destination_id')}', "
                        f"streams=[{{'name': '<stream>', "
                        f"'syncMode': '<mode>'}}])`. **Why**: a destination "
                        f"alone receives no data; the connection ties it to "
                        f"a source + stream selection. **Effect**: Airbyte "
                        f"creates the connection so syncs can be triggered. "
                        f"**If missing**: data stays in the source side — "
                        f"nothing lands here."
                    ),
                    (
                        f"**2. Sanity-check the destination**: "
                        f"`airbyte_pipeline(action='check_destination_health', "
                        f"destination_id='{result.get('destination_id')}')`. "
                        f"**Why**: discovery surfaces credential / network "
                        f"issues before the first sync attempts to write. "
                        f"**Effect**: Airbyte issues a CHECK against the "
                        f"destination and returns reachability. **If "
                        f"missing**: skip if you trust the configuration."
                    ),
                ]
            return sanitize_response(result)
        except ValueError as e:
            return {"success": False, "error": safe_error_message(e)}
        except Exception as e:
            logger.error("Failed to create Airbyte destination: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed to create Airbyte destination. Check server logs for details.",
            }

    async def _create_intelligent_airbyte_pipeline(
        source_name: str,
        source_type: str,
        source_profile: str | None = None,
        destination_name: str = "",
        destination_type: str = "",
        destination_profile: str | None = None,
        streams: list[dict[str, Any]] | None = None,
        connection_name: str = "",
        schedule_type: str | None = None,
        schedule_cron: str | None = None,
        namespace_definition: str = "destination",
        namespace_format: str | None = None,
        intent: str | None = None,
        policy: dict[str, Any] | None = None,
        dry_run: bool = False,
        airflow_orchestrated: bool = False,
    ) -> dict[str, Any]:
        """
        Create an Airbyte pipeline by reusing or creating source, destination, and connection.

        Credentials are resolved server-side — the LLM never handles passwords.
        Uses default Teradata credentials unless a specific profile is provided.

        Args:
            source_name: Display name for the Airbyte source
            source_type: Connector type name (e.g., "Postgres", "MySQL")
                — determined by the LLM from the user's prompt
            source_profile: Optional profile name from connections.yaml for source credentials.
                If not provided, uses the default Teradata connection.
            destination_name: Display name for the Airbyte destination
            destination_type: Connector type name (e.g., "Teradata", "BigQuery")
                — determined by the LLM from the user's prompt
            destination_profile: Optional profile name from connections.yaml for destination
                credentials. If not provided, uses the default Teradata connection.
            streams: List of stream configurations (name, syncMode, etc.)
            connection_name: Optional connection name
            schedule_type: "manual" or "cron". Defaults to manual when omitted.
                Passing "manual" with schedule_cron is a conflict error.
            schedule_cron: Cron expression (e.g. "0 2 * * *"). If provided
                without schedule_type, implies cron. Required when
                schedule_type is "cron".
            namespace_definition: Namespace handling ("destination", "source", "custom")
            namespace_format: Custom namespace format
            intent: Natural language description of desired streams
            policy: Sync policy configuration
            dry_run: If True, validate without creating resources

        IMPORTANT SYNC MODE HANDLING:
        - Do NOT assume or fill in default values for 'syncMode' and 'destinationSyncMode'.
        - ONLY include 'syncMode' and 'destinationSyncMode' in a stream if the user has
          EXPLICITLY stated their sync preferences (e.g., "incremental sync", "append mode",
          "full refresh", "overwrite", etc.).
        - Valid syncMode values (when user specifies): 'full_refresh', 'incremental'
        - Valid destinationSyncMode values (when user specifies): 'overwrite', 'append', 'append_dedup'
        - 'cursorField' (required when syncMode is 'incremental'): column name for tracking changes

        SCHEDULING:
        - schedule_type: "manual" or "cron". Defaults to manual when omitted.
          Passing "manual" with schedule_cron is a conflict error.
        - schedule_cron: Required when schedule_type is "cron". Standard cron expression,
          e.g. "0 2 * * *" for daily at 02:00 UTC. If provided without explicit
          schedule_type, implies cron scheduling.
        - airflow_orchestrated: When True, forces schedule_type="manual" on the
          Airbyte connection regardless of schedule_cron. If an existing connection
          with a cron schedule matches, it is auto-updated to manual instead of
          returning a clarification error. The cron expression is preserved in the
          response as 'intended_schedule_cron' for the Airflow DAG.

        Returns a dict with connection details, or a clarification request if sync config is missing.
        """
        try:

            def _resolve_or_default(profile_name: str | None) -> dict[str, str]:
                if profile_name:
                    guard = orchestrator.credential_resolver.guard_configured()
                    if guard:
                        raise ValueError(guard.get("error", "connections.yaml not configured"))
                    return orchestrator.credential_resolver.resolve_profile(profile_name)
                td = orchestrator.settings.teradata
                password = (
                    td.password.get_secret_value()
                    if hasattr(td.password, "get_secret_value")
                    else td.password
                )
                return {
                    "host": td.host,
                    "username": td.username,
                    "password": password,
                    "database": td.database,
                    "port": str(td.port),
                }

            source_connection_configuration = _resolve_or_default(source_profile)
            destination_connection_configuration = _resolve_or_default(destination_profile)

            # Require either streams or intent
            if not streams and not intent:
                return {
                    "success": False,
                    "error": "Either 'streams' or 'intent' must be provided.",
                }

            # If streams provided, validate sync modes upfront
            if streams:
                sync_clarification = _validate_sync_modes(streams)
                if sync_clarification:
                    return sync_clarification

            src_def_id = await orchestrator.airbyte_client.find_definition_id_by_name(
                "source", source_type
            )
            if not src_def_id:
                return {
                    "success": False,
                    "error": f"Source definition for type '{source_type}' not found.",
                }
            src_res = await _find_or_create_connector(
                "source",
                source_name,
                src_def_id,
                source_connection_configuration,
                orchestrator,
            )
            if not src_res.get("success"):
                return src_res
            source_id = (src_res.get("source") or {}).get("sourceId")
            if not source_id:
                return {
                    "success": False,
                    "error": "Failed to resolve sourceId after source creation.",
                }

            # Create DiscoveryCache for the whole flow
            discovery_cache = DiscoveryCache(orchestrator.airbyte_client)

            if streams:
                # Validate cursor fields and stream names with cache
                cursor_clarification = await _validate_cursor_fields(
                    streams,
                    source_id,
                    orchestrator.airbyte_client,
                    discovery_cache=discovery_cache,
                )
                if cursor_clarification:
                    return cursor_clarification

                stream_name_clarification = await _validate_stream_names(
                    streams,
                    source_id,
                    orchestrator.airbyte_client,
                    discovery_cache=discovery_cache,
                )
                if stream_name_clarification:
                    return stream_name_clarification
            elif intent:
                # Intent-based stream selection
                disc = await discovery_cache.get(source_id)
                catalog = (disc or {}).get("catalog") or {}
                # Build stream index inline from cached discovery
                index_streams = []
                for entry in catalog.get("streams", []):
                    s = entry.get("stream", {})
                    sname = s.get("name") or entry.get("name")
                    ns = s.get("namespace") or ""
                    description = s.get("description") or (entry.get("description") or "")
                    schema = s.get("json_schema") or s.get("schema") or {}
                    props = (schema or {}).get("properties") or {}
                    columns = [str(col) for col in props] if isinstance(props, dict) else []
                    tags = []
                    if ns:
                        tags.append(ns.lower())
                    index_streams.append(
                        {
                            "name": sname,
                            "namespace": ns,
                            "description": description,
                            "columns": columns,
                            "tags": tags,
                            "raw": entry,
                        }
                    )

                kws = _intent_keywords(intent, extra_synonyms=(policy or {}).get("synonyms"))
                ranked = []
                for item in index_streams:
                    score = _score_stream_v2(item, kws)
                    if score > 0:
                        ranked.append((item, score))
                ranked.sort(key=lambda x: x[1], reverse=True)

                if not ranked:
                    available_names = _extract_stream_names(disc)
                    return {
                        "success": False,
                        "error": "No streams matched intent",
                        "keywords": kws,
                        "available_streams": available_names,
                    }

                # Auto-build streams list with _choose_sync_mode
                by_name = {
                    (s.get("stream", {}) or {}).get("name") or s.get("name"): s
                    for s in catalog.get("streams", [])
                }
                streams = []
                for item, _sc in ranked:
                    entry = by_name.get(item.get("name"))
                    if not entry:
                        continue
                    sync_mode = _choose_sync_mode(entry)
                    stream_def = {
                        "name": item.get("name"),
                        "syncMode": sync_mode,
                        "destinationSyncMode": "append",
                        "selected": True,
                    }
                    # For incremental, include default cursor if available
                    if sync_mode == "incremental":
                        default_cursor = entry.get("stream", {}).get(
                            "defaultCursorField"
                        ) or entry.get("config", {}).get("defaultCursorField")
                        if default_cursor:
                            stream_def["cursorField"] = default_cursor
                        else:
                            # No cursor available — fall back to full_refresh
                            stream_def["syncMode"] = "full_refresh"
                            stream_def["destinationSyncMode"] = "overwrite"
                    streams.append(stream_def)

            # dry_run: return preview without creating destination or connection
            if dry_run:
                available_names = _extract_stream_names(await discovery_cache.get(source_id))
                return {
                    "success": True,
                    "dry_run": True,
                    "source_id": source_id,
                    "source_reused": src_res.get("reused", False),
                    "streams": streams,
                    "stream_count": len(streams),
                    "available_streams": available_names,
                    "message": (
                        f"Dry run: {len(streams)} streams validated. "
                        "Source ready. Destination and connection NOT created."
                    ),
                }

            dst_def_id = await orchestrator.airbyte_client.find_definition_id_by_name(
                "destination", destination_type
            )
            logger.info(
                "Resolved destination definition ID: %s for type: %s",
                dst_def_id,
                destination_type,
            )
            if not dst_def_id:
                return {
                    "success": False,
                    "error": f"Destination definition for type '{destination_type}' not found.",
                }
            dst_res = await _find_or_create_connector(
                "destination",
                destination_name,
                dst_def_id,
                destination_connection_configuration,
                orchestrator,
            )
            if not dst_res.get("success"):
                return dst_res
            destination_id = (dst_res.get("destination") or {}).get("destinationId")
            if not destination_id:
                return {
                    "success": False,
                    "error": "Failed to resolve destinationId after destination creation.",
                }
            # When Airflow will orchestrate this connection, override to manual.
            # Normalize to 5-field Unix cron for Airflow compatibility (Airbyte
            # uses 6-field Quartz cron internally).
            intended_schedule_cron = schedule_cron
            if intended_schedule_cron:
                _parts = intended_schedule_cron.strip().split()
                if len(_parts) == 6:
                    # Strip leading seconds field and replace Quartz '?' with '*'
                    intended_schedule_cron = " ".join(p if p != "?" else "*" for p in _parts[1:])
            if airflow_orchestrated:
                schedule_type = "manual"
                schedule_cron = None

            conn_res = await _create_airbyte_connection(
                source_id=source_id,
                destination_id=destination_id,
                connection_name=connection_name,
                streams=streams,
                schedule_type=schedule_type,
                schedule_cron=schedule_cron,
                namespace_definition=namespace_definition,
                namespace_format=namespace_format,
                _discovery_cache=discovery_cache,
            )

            # When airflow_orchestrated and reuse logic found a schedule mismatch,
            # auto-update the existing connection to manual instead of returning
            # a clarification error.
            if (
                airflow_orchestrated
                and conn_res.get("clarification_needed")
                and conn_res.get("current_schedule") != "manual"
            ):
                existing_id = conn_res.get("existing_connection_id")
                if existing_id:
                    await orchestrator.airbyte_client.update_connection(
                        existing_id, schedule={"scheduleType": "manual"}
                    )
                    logger.info(
                        "Auto-updated existing connection %s to manual "
                        "(was '%s') because airflow_orchestrated=True",
                        existing_id,
                        conn_res.get("current_schedule"),
                    )
                    conn_res = {
                        "success": True,
                        "connection_name": conn_res.get("connection_name"),
                        "connection_id": existing_id,
                        "source_id": source_id,
                        "destination_id": destination_id,
                        "schedule_type": "manual",
                        "reused": True,
                        "schedule_updated": True,
                        "previous_schedule": conn_res.get("current_schedule"),
                    }

            # Enhance response with detailed reuse information
            if conn_res.get("success"):
                conn_res["source_reused"] = src_res.get("reused", False)
                conn_res["destination_reused"] = dst_res.get("reused", False)
                conn_res["connection_reused"] = conn_res.get("reused", False)

                # Add summary message for clarity
                components_created = []
                components_reused = []

                if src_res.get("reused"):
                    components_reused.append("source")
                else:
                    components_created.append("source")

                if dst_res.get("reused"):
                    components_reused.append("destination")
                else:
                    components_created.append("destination")

                if conn_res.get("reused"):
                    components_reused.append("connection")
                else:
                    components_created.append("connection")

                conn_res["summary"] = {
                    "created": components_created,
                    "reused": components_reused,
                    "all_new": len(components_created) == 3,
                    "all_reused": len(components_reused) == 3,
                }

                if airflow_orchestrated:
                    conn_res["airflow_orchestrated"] = True
                    conn_res["schedule_type"] = "manual"
                    if intended_schedule_cron:
                        conn_res["intended_schedule_cron"] = intended_schedule_cron
                        conn_res["advisory"] = (
                            f"Airbyte connection set to manual scheduling because "
                            f"airflow_orchestrated=True. Use '{intended_schedule_cron}' "
                            f"as the Airflow DAG schedule when calling "
                            f"pipeline_deploy(action='create_sync_dag')."
                        )
                    # Override next_steps so the agent wires the Airflow DAG
                    # rather than triggering an Airbyte schedule directly.
                    conn_res["next_steps"] = [
                        (
                            f"**1. Generate the Airflow sync DAG**: "
                            f"`pipeline_deploy(action='create_sync_dag', "
                            f"dag_id='<id>', "
                            f"connection_id='{conn_res.get('connection_id')}'"
                            f"{', schedule=' + repr(intended_schedule_cron) if intended_schedule_cron else ''})`. "
                            f"**Why**: ``airflow_orchestrated=True`` means "
                            f"Airbyte was set to manual on purpose; Airflow "
                            f"is the source of cadence + retries. **Effect**: "
                            f"writes a DAG that triggers this Airbyte "
                            f"connection on the schedule you pick. **If "
                            f"missing**: the connection will only run when "
                            f"manually triggered."
                        ),
                        (
                            "**2. Deploy the DAG**: "
                            "`pipeline_deploy(action='deploy_dags')`. "
                            "**Why**: a generated DAG is local until SFTP'd "
                            "to the Airflow server. **Effect**: copies the "
                            "DAG to the Airflow ``dags_folder`` and the "
                            "scheduler picks it up. **If missing**: skip if "
                            "Airflow reads the local DAGs folder directly."
                        ),
                    ]

            return sanitize_response(conn_res)
        except ValueError as e:
            return {"success": False, "error": safe_error_message(e)}
        except Exception as e:
            logger.error("Failed during intelligent pipeline creation: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Failed during intelligent pipeline creation. Check server logs for details.",
            }

    async def _build_stream_index(
        source_id: str, schemas: list[str] | None = None
    ) -> dict[str, Any]:
        """Build a searchable index of streams with metadata for matching."""
        disc = await orchestrator.airbyte_client.discover_source_schema(source_id)
        catalog = (disc or {}).get("catalog") or {}
        streams = []
        for entry in catalog.get("streams", []):
            s = entry.get("stream", {})
            name = s.get("name") or entry.get("name")
            ns = s.get("namespace") or ""
            if schemas and ns and (ns not in schemas):
                continue
            description = s.get("description") or (entry.get("description") or "")
            schema = s.get("json_schema") or s.get("schema") or {}
            props = (schema or {}).get("properties") or {}
            columns = [str(col) for col in props] if isinstance(props, dict) else []
            tags = []
            if ns:
                tags.append(ns.lower())
            streams.append(
                {
                    "name": name,
                    "namespace": ns,
                    "description": description,
                    "columns": columns,
                    "tags": tags,
                    "raw": entry,
                }
            )
        return {"streams": streams}

    def _intent_keywords(
        prompt: str, extra_synonyms: dict[str, list[str]] | None = None
    ) -> list[str]:
        text = (prompt or "").lower()
        tokens = [t for t in [x.strip() for x in re.split("[^a-zA-Z0-9_]+", text)] if t]
        stop = {
            "sync",
            "transfer",
            "move",
            "load",
            "ingest",
            "data",
            "table",
            "tables",
            "stream",
            "streams",
            "from",
            "to",
            "into",
            "and",
            "or",
            "the",
            "a",
            "an",
            "of",
            "on",
            "last",
            "year",
            "month",
            "week",
        }
        tokens = [t for t in tokens if t not in stop and len(t) > 2]
        synonyms = {
            "customer": [
                "customers",
                "client",
                "clients",
                "account",
                "accounts",
                "user",
                "users",
            ],
            "order": ["orders", "sales", "sales_order", "order_history", "purchases"],
            "payment": ["payments", "transactions", "billing"],
            "product": ["products", "items", "sku", "skus", "catalog"],
            "supplier": ["suppliers", "vendor", "vendors"],
            "inventory": ["stock", "warehouse"],
            "finance": ["ledger", "gl", "billing", "invoices", "payments"],
            "marketing": ["campaign", "campaigns", "leads", "lead"],
            "support": ["tickets", "cases"],
            "security": ["audit", "logs", "activity"],
            "reference": ["lookup", "dictionary", "codes", "mappings", "master"],
        }
        if extra_synonyms:
            for k, v in extra_synonyms.items():
                synonyms.setdefault(k, [])
                synonyms[k].extend(v)
        expanded = set(tokens)
        for base, syns in synonyms.items():
            if base in expanded:
                for s in syns:
                    expanded.add(s)
        return list(expanded)

    def _score_stream(item: dict[str, Any], kws: list[str]) -> int:
        name = (item.get("name") or "").lower()
        ns = (item.get("namespace") or "").lower()
        desc = (item.get("description") or "").lower()
        cols = [str(c).lower() for c in item.get("columns") or []]
        tags = [str(t).lower() for t in item.get("tags") or []]
        score = 0
        for k in kws:
            if k in name:
                score += 3
            if k and ns and (k in ns):
                score += 2
            if k and desc and (k in desc):
                score += 2
            if any(k in c for c in cols):
                score += 1
            if any(k in t for t in tags):
                score += 1
        return score

    def _is_restricted(item: dict[str, Any], policy: dict[str, Any] | None) -> bool:
        markers = {
            "email",
            "ssn",
            "social_security",
            "phone",
            "mobile",
            "dob",
            "birth",
            "address",
            "credit",
            "card",
            "pan",
            "tax",
            "tin",
            "national_id",
            "passport",
        }
        cols = [str(c).lower() for c in item.get("columns") or []]
        base_restricted = any(any(p in c for p in markers) for c in cols)
        if not policy:
            return base_restricted
        extra = {str(x).lower() for x in policy.get("restricted_markers") or []}
        if extra:
            base_restricted = base_restricted or any(any(p in c for p in extra) for c in cols)
        return base_restricted

    def _choose_sync_mode(entry: dict[str, Any]) -> str:
        modes = (
            entry.get("config", {}).get("supported_sync_modes")
            or entry.get("stream", {}).get("supported_sync_modes")
            or []
        )
        lower = [str(m).lower() for m in modes]
        return "incremental" if "incremental" in lower else "full_refresh"

    async def _select_streams_from_intent(
        source_id: str,
        prompt: str,
        schemas: list[str] | None = None,
        policy: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """
        Hybrid, generic intent-based stream selection for any business domain.

        - Build index from discovery (name, namespace, description, columns)
        - Parse intent into keywords (+ optional synonyms from policy)
        - Rank streams deterministically by keyword matches across metadata
        - Enforce policy: allowed/disallowed streams/namespaces, sensitivity
        - Return `selected_streams` for Airbyte configured catalog
        """
        try:
            # Use DiscoveryCache to avoid redundant API calls
            cache = DiscoveryCache(orchestrator.airbyte_client)
            disc = await cache.get(source_id)
            catalog = (disc or {}).get("catalog") or {}

            # Build stream index inline from cached discovery
            index_streams = []
            for entry in catalog.get("streams", []):
                s = entry.get("stream", {})
                sname = s.get("name") or entry.get("name")
                ns = s.get("namespace") or ""
                if schemas and ns and (ns not in schemas):
                    continue
                description = s.get("description") or (entry.get("description") or "")
                schema_obj = s.get("json_schema") or s.get("schema") or {}
                props = (schema_obj or {}).get("properties") or {}
                columns = [str(col) for col in props] if isinstance(props, dict) else []
                tags = []
                if ns:
                    tags.append(ns.lower())
                index_streams.append(
                    {
                        "name": sname,
                        "namespace": ns,
                        "description": description,
                        "columns": columns,
                        "tags": tags,
                        "raw": entry,
                    }
                )

            kws = _intent_keywords(prompt, extra_synonyms=(policy or {}).get("synonyms"))
            ranked = []
            for item in index_streams:
                score = _score_stream_v2(item, kws)
                if score > 0:
                    ranked.append((item, score))
            ranked.sort(key=lambda x: x[1], reverse=True)
            by_name = {
                (s.get("stream", {}) or {}).get("name") or s.get("name"): s
                for s in catalog.get("streams", [])
            }
            allowed: list[dict[str, Any]] = []
            allowed_namespaces = {
                str(x).lower() for x in (policy or {}).get("allowed_namespaces", [])
            }
            disallowed_namespaces = {
                str(x).lower() for x in (policy or {}).get("disallowed_namespaces", [])
            }
            disallowed_streams = {
                str(x).lower() for x in (policy or {}).get("disallowed_streams", [])
            }
            max_sens = str((policy or {}).get("max_sensitivity", "high")).lower()
            max_select = int((policy or {}).get("max_streams", limit or 0) or 0)
            for item, _score in ranked:
                name = item.get("name")
                ns = (item.get("namespace") or "").lower()
                if name is None or name.lower() in disallowed_streams:
                    continue
                if allowed_namespaces and ns and (ns not in allowed_namespaces):
                    continue
                if disallowed_namespaces and ns and (ns in disallowed_namespaces):
                    continue
                restricted = _is_restricted(item, policy)
                if restricted and max_sens in ("low", "medium"):
                    continue
                entry = by_name.get(name)
                if not entry:
                    continue
                entry_ns = (entry.get("stream", {}) or {}).get("namespace") or ""
                if schemas and entry_ns and (entry_ns not in schemas):
                    continue
                sync_mode = _choose_sync_mode(entry)
                allowed.append(
                    {
                        "name": name,
                        "syncMode": sync_mode,
                        "destinationSyncMode": "append",
                        "selected": True,
                    }
                )
                if max_select and len(allowed) >= max_select:
                    break
            if not allowed:
                return {
                    "success": False,
                    "error": "No streams selected",
                    "keywords": kws,
                }
            return {
                "success": True,
                "selected_streams": allowed,
                "keywords": kws,
                "count": len(allowed),
            }
        except Exception as e:
            logger.error("Intent-based selection failed: %s", e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    async def _generate_airflow_tdload_dag_from_csv(
        csv_path: str | None = None,
        target_database: str | None = None,
        target_table: str | None = None,
        dag_id: str | None = None,
        teradata_conn_id: str | None = None,
        ssh_conn_id: str | None = None,
        delimiter: str | None = None,
        source_format: str = "Delimited",
        schedule: str | None = None,
        generate_validations: bool = True,
        table_prefix: str = "",
        error_limit: int = 100,
        session_count: int = 4,
        owner: str = "data_engineer",
        email: list[str] | None = None,  # noqa: ARG001
        tags: list[str] | None = None,
        strict_ssh: bool = True,
        teradata_profile: str | None = None,
        ssh_profile: str | None = None,
        dbt_project_dir: str | None = None,
        dbt_models: list[str] | None = None,
        dbt_target: str = "prod",
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
    ) -> dict[str, Any]:
        """
        Generate Airflow DAG using TdLoadOperator for loading CSV file to Teradata.

        This is the NEW recommended approach replacing TPT script generation.
        Uses Airflow's TdLoadOperator for file loading and BteqOperator for validation.

        Credentials are resolved from connection profiles (connections.yaml)
        or from .env settings. The LLM never handles passwords.

        Args:
            csv_path: Path to the CSV file. If not provided, the agent will ask the user.
            target_database: Target Teradata database (uses TERADATA_DATABASE from .env if None)
            target_table: Target table name (auto-generated from CSV if None)
            dag_id: DAG identifier (auto-generated if None)
            teradata_conn_id: Airflow connection ID for Teradata (derived from dag_id if None)
            ssh_conn_id: Airflow connection ID for SSH (derived from dag_id if None)
            delimiter: Delimiter character (e.g., ',' for CSV, '|' for pipe-delimited).
                Auto-detected from file content if not specified.
            source_format: Source file format (default: 'Delimited')
            schedule: Cron expression or preset (@daily, @hourly, None for manual)
            generate_validations: Generate validation tasks using BteqOperator
            table_prefix: Prefix for auto-generated table name
            error_limit: Maximum errors before failure
            session_count: Number of parallel loading sessions
            owner: DAG owner
            email: Email addresses for notifications
            tags: List of tags for the DAG
            strict_ssh: Enforce SSH host-key verification (default True).
                Set to False only for trusted networks with explicit awareness.
            teradata_profile: Profile name from connections.yaml for Teradata
                credentials. Falls back to .env TERADATA_* settings.
            ssh_profile: Profile name from connections.yaml for SSH credentials.
                Falls back to MCP_CLIENT_SSH_* environment variables.

        Returns:
            Dictionary with DAG generation results including file path and code
        """
        try:
            from datetime import datetime

            from ..generators.airflow_tdload_dag_generator import (
                AirflowTdLoadDAGGenerator,
            )
            from ..utils.csv_analyzer import CSVAnalyzer

            # Resolve parameters from environment if not provided
            if csv_path is None:
                return {
                    "success": False,
                    "action_required": "ask_csv_path",
                    "message": "Please provide the full path to the CSV file.",
                }

            if target_database is None:
                target_database = orchestrator.settings.teradata.database
                if not target_database:
                    return {
                        "success": False,
                        "error": "target_database not provided and TERADATA_DATABASE not set in .env",
                    }

            # Reject path traversal: CSV must be under CWD or its parent.
            # Delegates to the shared safe_path_under_any_root helper so the
            # trust-boundary check lives in one place (utils/file_operations.py).
            try:
                csv_file = safe_path_under_any_root(csv_path, [Path.cwd(), Path.cwd().parent])
            except UnsafePathError as e:
                # Don't surface ``UnsafePathError``'s message — it embeds the
                # resolved allowed-roots (server CWD + parent), which leaks
                # filesystem layout to the LLM. Log the detail server-side
                # and return a generic message that's still actionable.
                logger.warning("CSV path rejected: %s", e)
                return {
                    "success": False,
                    "error": (
                        "CSV path rejected: the file must be within the "
                        "server's current working directory or its parent."
                    ),
                }
            csv_path = str(csv_file)
            if not csv_file.exists():
                return {
                    "success": False,
                    "error": f"CSV file not found: {csv_path}",
                }

            # Auto-generate table name if not provided
            if target_table is None:
                base_name = csv_file.stem
                target_table = re.sub("[^a-z0-9_]", "_", base_name.lower())
                target_table = re.sub("_+", "_", target_table)
                target_table = target_table.strip("_")
                if table_prefix:
                    target_table = f"{table_prefix}{target_table}"
                logger.info("Auto-generated table name: %s", target_table)

            # Auto-generate DAG ID if not provided
            if dag_id is None:
                dag_id = f"load_{target_database}_{target_table}".lower()
                logger.info("Auto-generated DAG ID: %s", dag_id)

            # C4: Derive connection IDs from dag_id to avoid collisions in shared Airflow
            if teradata_conn_id is None:
                teradata_conn_id = f"td_{dag_id}"
            if ssh_conn_id is None:
                ssh_conn_id = f"ssh_{dag_id}"

            # Analyze CSV for validation queries
            analyzer = CSVAnalyzer(sample_rows=1000)
            csv_analysis = analyzer.analyze_csv(str(csv_path), delimiter=delimiter)

            # Use the resolved delimiter from analysis (may have been auto-detected)
            delimiter = csv_analysis.delimiter

            logger.info(
                "CSV analysis complete: %d rows, %d columns, %.2f MB, delimiter=%r",
                csv_analysis.row_count,
                csv_analysis.column_count,
                csv_analysis.file_size_mb,
                delimiter,
            )

            # Track connection creation warnings (C2)
            connection_warnings: list[str] = []
            connections_valid = True

            # Resolve Teradata credentials from profile or .env
            td_settings = orchestrator.settings.teradata
            td_host_override = None
            td_username_override = None
            td_password_override = None
            td_port_override = None
            if teradata_profile:
                guard = orchestrator.credential_resolver.guard_configured()
                if guard:
                    raise ValueError(
                        f"Cannot resolve Teradata profile '{teradata_profile}': "
                        f"{guard.get('error', 'connections.yaml not configured')}"
                    )
                td_profile = orchestrator.credential_resolver.resolve_profile(teradata_profile)
                td_host_override = td_profile.get("host")
                td_username_override = td_profile.get("username")
                td_password_override = td_profile.get("password")
                if td_password_override is None:
                    logger.warning(
                        "Teradata profile '%s' has no 'password' field; "
                        "falling back to TERADATA_PASSWORD from .env",
                        teradata_profile,
                    )
                raw_profile_port = td_profile.get("port")
                if raw_profile_port is not None:
                    td_port_override = int(raw_profile_port)

            # Ensure Teradata connection exists
            try:
                teradata_conn_id = await _ensure_teradata_connection(
                    conn_id=teradata_conn_id,
                    database=target_database,
                    td_settings=td_settings,
                    host_override=td_host_override,
                    username_override=td_username_override,
                    password_override=td_password_override,
                    port_override=td_port_override,
                )
            except ValueError:
                raise  # configuration error — outer handler logs and returns failure dict
            except Exception as td_error:
                logger.warning("Failed to ensure Teradata connection: %s", td_error, exc_info=True)
                connection_warnings.append(
                    f"Teradata connection creation failed: {td_error}. DAG will fail at runtime."
                )
                connections_valid = False

            # Ensure SSH connection exists for TdLoadOperator
            try:
                ssh_conn_id = await _ensure_ssh_connection(
                    conn_id=ssh_conn_id,
                    ssh_profile=ssh_profile,
                    strict_ssh=strict_ssh,
                )
            except ValueError as ssh_error:
                if ssh_profile:
                    raise
                logger.warning("Failed to ensure SSH connection: %s", ssh_error, exc_info=True)
                connection_warnings.append(
                    f"SSH connection creation failed: {ssh_error}. DAG will fail at runtime."
                )
                connections_valid = False
            except Exception as ssh_error:
                logger.warning("Failed to ensure SSH connection: %s", ssh_error, exc_info=True)
                connection_warnings.append(
                    f"SSH connection creation failed: {ssh_error}. DAG will fail at runtime."
                )
                connections_valid = False

            # Prepare validation queries if requested
            validation_queries = None
            if generate_validations:
                q_db = _quote_id(target_database)
                q_tbl = _quote_id(target_table)
                q_col = _quote_column(csv_analysis.columns[0].name)
                validation_queries = [
                    {
                        "name": "row_count_check",
                        "sql": f"""
                            -- Row count validation
                            SELECT COUNT(*) as row_count
                            FROM {q_db}.{q_tbl};

                            -- Expected approximately {csv_analysis.row_count} rows from CSV
                            .IF ACTIVITYCOUNT = 0 THEN .QUIT 12;
                            """,
                    },
                    {
                        "name": "null_check_first_column",
                        "sql": f"""
                            -- Null count check for first column
                            SELECT
                                '{csv_analysis.columns[0].name.replace("'", "''")}' as column_name,
                                COUNT(*) as null_count
                            FROM {q_db}.{q_tbl}
                            WHERE {q_col} IS NULL;
                            """,
                    },
                    {
                        "name": "data_quality_check",
                        "sql": f"""
                            -- Sample data check - get first 10 rows
                            SELECT TOP 10 *
                            FROM {q_db}.{q_tbl};

                            .IF ACTIVITYCOUNT = 0 THEN .QUIT 12;
                            """,
                    },
                ]

            # Generate Airflow DAG
            dags_folder = Path(orchestrator.settings.pipeline.dags_output_dir)
            dag_generator = AirflowTdLoadDAGGenerator(dags_folder=dags_folder)

            # Prepare column information for CREATE TABLE
            columns_for_ddl = [
                {
                    "name": col.name,
                    "type": col.inferred_teradata_type,
                    "nullable": True,  # CSV columns are nullable by default
                }
                for col in csv_analysis.columns
            ]

            _csv_doc_md = f"""
# CSV to Teradata Data Loading

**Source File:** `{csv_path}`
**Target Table:** `{target_database}.{target_table}`
**Rows:** {csv_analysis.row_count}
**Columns:** {csv_analysis.column_count}

## Operator Used
- **TdLoadOperator**: Enterprise-grade Teradata loading using TPT API
- **BteqOperator**: Validation queries

## Configuration
- **Delimiter:** `{delimiter}`
- **Error Limit:** {error_limit}
- **Sessions:** {session_count}
"""
            _csv_common_kwargs = dict(
                dag_id=dag_id,
                description=f"Load data from {csv_file.name} to {target_database}.{target_table} using TdLoadOperator",
                source_file_path=str(csv_path),
                target_database=target_database,
                target_table=target_table,
                delimiter=delimiter,
                source_format=source_format,
                teradata_conn_id=teradata_conn_id,
                ssh_conn_id=ssh_conn_id,
                schedule=schedule,
                start_date=datetime(2024, 1, 1),
                validation_queries=validation_queries,
                owner=owner,
                tags=tags or ["teradata", "csv_loading", "tdload"],
                columns=columns_for_ddl,
                skip_rows=1 if csv_analysis.has_header else 0,
                doc_md=_csv_doc_md,
            )

            if dbt_project_dir:
                dag_generator.generate_file_loading_with_dbt_dag(
                    **_csv_common_kwargs,
                    dbt_project_dir=dbt_project_dir,
                    dbt_models=dbt_models,
                    dbt_target=dbt_target,
                    run_dbt_tests=run_dbt_tests,
                    generate_dbt_docs=generate_dbt_docs,
                )
            else:
                dag_generator.generate_file_loading_dag(**_csv_common_kwargs)

            dag_file_path = dags_folder / f"{dag_id}.py"

            result = {
                "success": True,
                "dag_id": dag_id,
                "dag_file_path": str(dag_file_path),
                "target_database": target_database,
                "target_table": target_table,
                "csv_path": csv_path,
                "csv_analysis": {
                    "row_count": csv_analysis.row_count,
                    "column_count": csv_analysis.column_count,
                    "file_size_mb": round(csv_analysis.file_size_mb, 2),
                    "delimiter": csv_analysis.delimiter,
                },
                "operator_type": "TdLoadOperator",
                "connections_valid": connections_valid,
                "warnings": connection_warnings,
                "validation_tasks": (len(validation_queries) if validation_queries else 0),
                "message": f"DAG generated successfully: {dag_id}",
                "next_steps": [
                    (
                        f"**1. Deploy the DAG to Airflow**: "
                        f"`pipeline_deploy(action='deploy_dags', "
                        f"pipeline_name='{dag_id}')`. **Why**: the DAG file "
                        f"was written locally but Airflow won't see it "
                        f"until SFTP'd to the server's ``dags_folder``. "
                        f"**Effect**: copies ``{dag_id}.py`` to the Airflow "
                        f"server and the scheduler picks it up on the next "
                        f"scan. **If missing**: skip if Airflow already "
                        f"reads the local DAGs folder."
                    ),
                    (
                        f"**2. Trigger the DAG**: "
                        f"`dag_trigger(mode='run', pipeline_name='{dag_id}')`. "
                        f"**Why**: deployment makes the DAG visible but "
                        f"data only lands once it executes. **Effect**: "
                        f"Airflow runs the TdLoadOperator, loading "
                        f"``{csv_path}`` into ``{target_database}."
                        f"{target_table}``. **If missing**: skip if "
                        f"``schedule`` was set and the next cron tick is "
                        f"acceptable."
                    ),
                    (
                        f"**3. Build dbt staging on the loaded data** "
                        f"(optional): `dbt_generate_model("
                        f"model_type='staging', "
                        f"source_database='{target_database}', "
                        f"source_tables=[{target_table!r}])`. **Why**: raw "
                        f"loaded tables benefit from a staging layer that "
                        f"adds renaming, casting, and tests. **Effect**: "
                        f"writes ``models/staging/stg_{target_table}.sql`` "
                        f"plus a sources YAML. **If missing**: skip if "
                        f"downstream consumers read the raw table directly."
                    ),
                ],
                "manual_commands": {
                    "cli_trigger": f"airflow dags trigger {dag_id}",
                    "cli_test": f"airflow dags test {dag_id} 2024-01-01",
                },
            }

            logger.info("Airflow TdLoad DAG generated: %s", dag_file_path)
            return result

        except Exception as e:
            logger.error("Failed to generate Airflow TdLoad DAG from CSV: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "csv_path": csv_path,
                "target_database": target_database,
                "target_table": target_table,
            }

    async def _load_csv_to_teradata_complete(
        csv_path: str,
        target_database: str,
        target_table: str,
        delimiter: str | None = None,
        teradata_conn_id: str | None = None,
        ssh_conn_id: str | None = None,
        schedule: str | None = None,
        generate_validations: bool = True,
        error_limit: int = 100,
        session_count: int = 4,
        owner: str = "data_engineer",
        email: list[str] | None = None,
        tags: list[str] | None = None,
        deploy_to_airflow: bool = False,
        trigger_after_deploy: bool = False,
        teradata_profile: str | None = None,
        ssh_profile: str | None = None,
        dbt_project_dir: str | None = None,
        dbt_models: list[str] | None = None,
        dbt_target: str = "prod",
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
    ) -> dict[str, Any]:
        """
        COMPLETE WORKFLOW: Load CSV data into Teradata via Airflow.

        ⚠️ DESIGN RECOMMENDATION: This "all-in-one" approach violates MCP principles
        of separation between code generation and infrastructure deployment.

        **Recommended Workflow (Better Design):**
        1. Use `generate_airflow_tdload_dag_from_csv` to create DAG file (generation)
        2. Review the generated DAG code (user control)
        3. Choose deployment method:
           - Manual: Copy DAG to Airflow dags folder
           - CI/CD: Commit to Git, let CI/CD deploy
           - Explicit: Use `deploy_dags_to_airflow` with full awareness
        4. Use `trigger_dag_run` to execute (explicit action)

        **Why This Matters:**
        - Automatic deployment hides security decisions (SSH credentials, file transfer)
        - Reduces user control and visibility
        - MCP servers should assist, not orchestrate infrastructure
        - Industry tools (Terraform, dbt) generate plans but don't auto-apply

        **New Defaults (v2.0):**
        - deploy_to_airflow=False (generation only, user chooses deployment)
        - trigger_after_deploy=False (explicit triggering required)

        ⭐ USE THIS TOOL when the user asks to:
        - "Load CSV to Teradata" (now generates DAG, suggests deployment options)
        - "Create Airflow DAG for CSV loading" (focuses on generation)
        - "Set up CSV to Teradata pipeline" (provides setup guidance)

        This tool can orchestrate the workflow if needed, but encourages step-by-step:
        1. ✅ Analyze CSV file and generate Airflow DAG with TdLoadOperator
        2. ⚠️ Optionally deploy to Airflow (set deploy_to_airflow=True)
        3. ⚠️ Optionally trigger execution (set trigger_after_deploy=True)

        Args:
            csv_path: Path to CSV file (relative or absolute)
            target_database: Target Teradata database/schema
            target_table: Target Teradata table name
            delimiter: Delimiter character (e.g., ',' or '|'). Auto-detected if not specified.
            teradata_conn_id: Airflow Teradata connection ID
            ssh_conn_id: Airflow SSH connection ID for remote execution
            schedule: Cron expression for scheduling (None for manual trigger only)
            generate_validations: Whether to include validation queries
            error_limit: Maximum errors allowed during loading
            session_count: Number of parallel sessions
            owner: DAG owner
            email: Email addresses for notifications
            tags: DAG tags
            deploy_to_airflow: Whether to deploy to Airflow (default: True)
            trigger_after_deploy: Whether to trigger DAG after deployment (default: True)

        Returns:
            Dictionary with complete workflow results including generation, deployment, and execution status

        Example usage:
            User: "Load customers.csv into sales_db.customers table"
            → Call this tool with csv_path="customers.csv", target_database="sales_db", target_table="customers"
        """
        try:
            logger.info(
                "Starting complete CSV to Teradata workflow: %s -> %s.%s",
                csv_path,
                target_database,
                target_table,
            )

            results = {"steps": []}

            # Step 1: Generate DAG
            logger.info("Step 1/3: Generating Airflow TdLoad DAG...")
            gen_result = await _generate_airflow_tdload_dag_from_csv(
                csv_path=csv_path,
                target_database=target_database,
                target_table=target_table,
                delimiter=delimiter,
                teradata_conn_id=teradata_conn_id,
                ssh_conn_id=ssh_conn_id,
                schedule=schedule,
                generate_validations=generate_validations,
                error_limit=error_limit,
                session_count=session_count,
                owner=owner,
                email=email,
                tags=tags,
                teradata_profile=teradata_profile,
                ssh_profile=ssh_profile,
                dbt_project_dir=dbt_project_dir,
                dbt_models=dbt_models,
                dbt_target=dbt_target,
                run_dbt_tests=run_dbt_tests,
                generate_dbt_docs=generate_dbt_docs,
            )

            results["steps"].append({"step": 1, "name": "generate_dag", "result": gen_result})

            if not gen_result.get("success"):
                return {
                    "success": False,
                    "error": "DAG generation failed",
                    "details": results,
                }

            dag_id = gen_result.get("dag_id")

            # Step 1.5: Ensure Airflow connections exist before deployment
            if deploy_to_airflow:
                logger.info("Step 1.5: Ensuring required Airflow connections exist...")

                pm_tools = _get_pipeline_tools()

                # Create Teradata connection if it doesn't exist
                logger.info("Ensuring Teradata connection '%s' exists...", teradata_conn_id)
                teradata_conn_result = await pm_tools["airflow_connections"](
                    action="create_teradata", connection_id=teradata_conn_id
                )
                results["steps"].append(
                    {
                        "step": "1.5a",
                        "name": "ensure_teradata_connection",
                        "result": teradata_conn_result,
                    }
                )

                if not teradata_conn_result.get("success"):
                    logger.warning(
                        "Failed to create Teradata connection: %s",
                        teradata_conn_result.get("error"),
                    )
                    return {
                        "success": False,
                        "error": f"Failed to create Teradata connection '{teradata_conn_id}': {teradata_conn_result.get('error')}",
                        "details": results,
                        "suggestion": "Please create the Teradata connection manually in Airflow UI or check your configuration.",
                    }

                # Create SSH connection if it doesn't exist
                logger.info("Ensuring SSH connection '%s' exists...", ssh_conn_id)
                ssh_conn_result = await pm_tools["airflow_connections"](
                    action="create_ssh", connection_id=ssh_conn_id
                )
                results["steps"].append(
                    {"step": "1.5b", "name": "ensure_ssh_connection", "result": ssh_conn_result}
                )

                if not ssh_conn_result.get("success"):
                    logger.warning(
                        "Failed to create SSH connection: %s",
                        ssh_conn_result.get("error"),
                    )
                    return {
                        "success": False,
                        "error": f"Failed to create SSH connection '{ssh_conn_id}': {ssh_conn_result.get('error')}",
                        "details": results,
                        "suggestion": "Please create the SSH connection manually in Airflow UI or check your SSH configuration.",
                    }

                logger.info("All required Airflow connections are ready")

            # Step 2: Deploy to Airflow
            if deploy_to_airflow:
                logger.info("Step 2/3: Deploying DAG '%s' to Airflow...", dag_id)

                pm_tools = _get_pipeline_tools()

                deploy_result = await pm_tools["pipeline_deploy"](
                    action="deploy_dags",
                    pipeline_name=dag_id,
                    wait_for_dag_loaded=True,
                    trigger_after_deploy=trigger_after_deploy,
                )

                results["steps"].append({"step": 2, "name": "deploy_dag", "result": deploy_result})

                if not deploy_result.get("success"):
                    return {
                        "success": False,
                        "error": "DAG deployment failed",
                        "details": results,
                    }

                # If trigger was handled by deploy, mark step 3 as completed
                if trigger_after_deploy and deploy_result.get("dag_triggered"):
                    results["steps"].append(
                        {
                            "step": 3,
                            "name": "trigger_dag",
                            "result": {
                                "success": True,
                                "dag_run_id": deploy_result.get("dag_run_id"),
                                "message": "DAG triggered during deployment",
                                "trigger_info": deploy_result.get("trigger_info"),
                            },
                        }
                    )
                    trigger_handled = True
                else:
                    trigger_handled = False
            else:
                results["steps"].append(
                    {
                        "step": 2,
                        "name": "deploy_dag",
                        "result": {"skipped": True, "reason": "deploy_to_airflow=False"},
                    }
                )
                trigger_handled = False

            # Step 3: Trigger execution (only if not already triggered by deployment)
            if trigger_after_deploy and deploy_to_airflow and not trigger_handled:
                logger.info("Step 3/3: Triggering DAG '%s' execution...", dag_id)

                # Import orchestration tools to access dag_trigger
                from . import orchestration_execution

                oe_tools = orchestration_execution.register_orchestration_tools(orchestrator)

                trigger_result = await oe_tools["dag_trigger"](mode="run", pipeline_name=dag_id)
                results["steps"].append(
                    {"step": 3, "name": "trigger_dag", "result": trigger_result}
                )

                if not trigger_result.get("success"):
                    return {
                        "success": False,
                        "error": "DAG trigger failed",
                        "details": results,
                    }
            elif not (trigger_after_deploy and deploy_to_airflow):
                results["steps"].append(
                    {
                        "step": 3,
                        "name": "trigger_dag",
                        "result": {
                            "skipped": True,
                            "reason": "trigger_after_deploy=False or deploy_to_airflow=False",
                        },
                    }
                )

            response: dict[str, Any] = {
                "success": True,
                "dag_id": dag_id,
                "csv_path": csv_path,
                "target_database": target_database,
                "target_table": target_table,
                "connections_created": {
                    "teradata": teradata_conn_id,
                    "ssh": ssh_conn_id,
                },
                "workflow_steps_completed": len(
                    [s for s in results["steps"] if not s["result"].get("skipped")]
                ),
                "message": (
                    f"Complete workflow finished successfully. "
                    f"Airflow connections {teradata_conn_id} + {ssh_conn_id} ready; "
                    f"DAG '{dag_id}' generated"
                    + (" and deployed" if deploy_to_airflow else " (deployment skipped)")
                    + (" and triggered" if trigger_after_deploy and deploy_to_airflow else ".")
                ),
                "details": results,
            }
            if deploy_to_airflow and trigger_after_deploy:
                response["next_steps"] = [
                    (
                        f"**1. Watch the DAG run**: poll "
                        f"`dag_monitor(query='run_status', "
                        f"pipeline_name='{dag_id}')` until the state is "
                        f"terminal (success/failed). **Why**: the DAG was "
                        f"triggered but the load is async; downstream "
                        f"steps (dbt, BI) need a green run to be useful, "
                        f"and there is no blocking-wait tool for an "
                        f"existing dag_run_id. **Effect**: returns the "
                        f"current run state and per-task summary. **If "
                        f"missing**: on the next manual run pass "
                        f"``wait_for_completion=True`` to "
                        f"``dag_trigger(mode='run', ...)``."
                    ),
                    (
                        f"**2. Build dbt staging on the loaded table**: "
                        f"`dbt_generate_model(model_type='staging', "
                        f"source_database='{target_database}', "
                        f"source_tables=[{target_table!r}])`. **Why**: raw "
                        f"loaded tables benefit from a staging layer that "
                        f"renames, casts, and tests. **Effect**: writes "
                        f"``models/staging/stg_{target_table}.sql`` plus a "
                        f"sources YAML. **If missing**: skip if downstream "
                        f"reads the raw table directly."
                    ),
                ]
            elif deploy_to_airflow:
                response["next_steps"] = [
                    (
                        f"**1. Trigger the DAG**: "
                        f"`dag_trigger(mode='run', pipeline_name='{dag_id}')`. "
                        f"**Why**: deployment makes the DAG visible to "
                        f"Airflow but the load only runs once you trigger "
                        f"it (or the schedule fires). **Effect**: Airflow "
                        f"executes the TdLoadOperator and validation "
                        f"queries. **If missing**: skip if a schedule was "
                        f"set and the next cron tick is acceptable."
                    ),
                    (
                        f"**2. Build dbt staging on the loaded table**: "
                        f"`dbt_generate_model(model_type='staging', "
                        f"source_database='{target_database}', "
                        f"source_tables=[{target_table!r}])`. **Why**: raw "
                        f"loaded tables benefit from a staging layer. "
                        f"**Effect**: writes "
                        f"``models/staging/stg_{target_table}.sql`` plus a "
                        f"sources YAML. **If missing**: skip if downstream "
                        f"reads the raw table directly."
                    ),
                ]
            else:
                response["next_steps"] = [
                    (
                        f"**1. Deploy the DAG**: "
                        f"`pipeline_deploy(action='deploy_dags', "
                        f"pipeline_name='{dag_id}')`. **Why**: "
                        f"``deploy_to_airflow=False`` means the DAG file is "
                        f"local only; Airflow won't see it until SFTP'd. "
                        f"**Effect**: copies ``{dag_id}.py`` to the Airflow "
                        f"server. **If missing**: skip if Airflow already "
                        f"reads the local DAGs folder."
                    ),
                    (
                        f"**2. Trigger the DAG**: "
                        f"`dag_trigger(mode='run', pipeline_name='{dag_id}')`. "
                        f"**Why**: deployment alone doesn't load any data. "
                        f"**Effect**: Airflow executes the TdLoadOperator. "
                        f"**If missing**: skip if you scheduled the DAG."
                    ),
                ]
            return response

        except Exception as e:
            logger.error("Failed to complete CSV to Teradata workflow: %s", e, exc_info=True)
            return {
                "success": False,
                "error": safe_error_message(e),
                "csv_path": csv_path,
                "target_database": target_database,
                "target_table": target_table,
                "details": results if "results" in locals() else {},
            }

    async def _find_or_reserve_conn_id(
        conn_type: str,
        conn_id: str,
        match_fields: dict[str, str],
        default_port: int,
    ) -> tuple[str, bool]:
        """Search Airflow for an existing connection matching credentials.

        Searches all Airflow connections of the given ``conn_type`` for one whose
        host/schema/login (as specified in *match_fields*) and port match the
        desired values.  If a match is found the existing connection ID is
        returned.  Otherwise falls back to ``conn_id``, incrementing with a
        ``_N`` suffix when that ID is already taken by a non-matching connection.

        Note: password is intentionally excluded from matching because the
        Airflow REST API redacts passwords in ``GET`` responses (returns
        empty string or ``***``).  Matching relies on host/login/schema/port
        which together uniquely identify the target system.  If a password
        rotation occurs, the existing Airflow connection must be updated
        out-of-band (UI / CLI) rather than re-created here.

        Returns:
            ``(conn_id, already_exists)`` — *already_exists* is ``True`` when a
            matching connection was found and no creation is needed.
        """
        effective_port = default_port

        # --- Stage 3: broad search across all connections of this type ---
        logger.info(
            "Searching for existing %s connection matching %s",
            conn_type,
            match_fields,
        )
        try:
            all_connections = await orchestrator.async_airflow_client.list_connections(limit=500)
            for conn in all_connections:
                if (conn.get("conn_type") or "").lower() != conn_type:
                    continue

                # Compare every field in match_fields (case-insensitive)
                raw_port = conn.get("port")
                try:
                    conn_port = int(raw_port) if raw_port is not None else default_port
                except (ValueError, TypeError):
                    conn_port = default_port

                if conn_port != effective_port:
                    continue

                if all((conn.get(k) or "").lower().strip() == v for k, v in match_fields.items()):
                    matched_id = conn.get("connection_id") or conn.get("conn_id")
                    logger.info(
                        "Found existing %s connection '%s' matching credentials",
                        conn_type,
                        matched_id,
                    )
                    return matched_id, True

            logger.info("No existing %s connection matches the credentials", conn_type)
        except Exception as list_err:
            logger.warning(
                "Could not list Airflow connections to search for match: %s",
                list_err,
            )

        # --- Stage 4: check requested conn_id, increment on mismatch ---
        logger.info("Checking if %s connection '%s' exists", conn_type, conn_id)
        try:
            existing = await orchestrator.async_airflow_client.get_connection(conn_id)
        except AsyncAirflowAPIError:
            # Connection doesn't exist (404) — will be created by the caller
            logger.debug("Connection '%s' does not exist yet, will create", conn_id)
            return conn_id, False

        logger.info("Connection '%s' already exists", conn_id)

        raw_port = existing.get("port")
        try:
            existing_port = int(raw_port) if raw_port is not None else default_port
        except (ValueError, TypeError):
            existing_port = default_port

        if existing_port == effective_port and all(
            (existing.get(k) or "").lower().strip() == v for k, v in match_fields.items()
        ):
            return conn_id, True

        # Mismatch — find an available incremented ID
        logger.info(
            "%s connection '%s' exists with different config. Will create a new connection.",
            conn_type,
            conn_id,
        )
        base_conn_id = conn_id
        counter = 1
        while True:
            new_conn_id = f"{base_conn_id}_{counter}"
            try:
                check_conn = await orchestrator.async_airflow_client.get_connection(new_conn_id)
            except AsyncAirflowAPIError:
                # Connection doesn't exist (404) — use this ID
                conn_id = new_conn_id
                break

            raw_port = check_conn.get("port")
            try:
                check_port = int(raw_port) if raw_port is not None else default_port
            except (ValueError, TypeError):
                check_port = default_port

            if check_port == effective_port and all(
                (check_conn.get(k) or "").lower().strip() == v for k, v in match_fields.items()
            ):
                logger.info("Found matching %s connection: %s", conn_type, new_conn_id)
                return new_conn_id, True
            counter += 1
            if counter > 100:
                raise RuntimeError(
                    f"Could not find available {conn_type} connection ID after 100 attempts"
                )

        return conn_id, False

    async def _ensure_teradata_connection(
        conn_id: str,
        database: str,
        td_settings,
        host_override: str | None = None,
        username_override: str | None = None,
        password_override: str | None = None,
        port_override: int | None = None,
    ) -> str:
        """Ensure a Teradata Airflow connection exists with correct config.

        First searches for an existing Teradata connection in Airflow that matches
        the credentials. If found, returns that connection ID. If not found,
        creates a new connection.

        Returns:
            The connection ID of the matching or newly created connection.

        Raises:
            RuntimeError: If the connection cannot be created.
        """

        effective_host = host_override or td_settings.host
        effective_login = username_override or td_settings.username
        effective_password = password_override or (
            td_settings.password.get_secret_value()
            if hasattr(td_settings.password, "get_secret_value")
            else td_settings.password
        )
        effective_port = port_override or td_settings.port or 1025

        # Validate required fields
        if not effective_host:
            raise ValueError(
                f"Teradata host not configured for connection '{conn_id}'. "
                "Provide a teradata_profile from connections.yaml or set TERADATA_HOST."
            )
        m = _UNRESOLVED_ENV_VAR.search(str(effective_host))
        if m:
            raise ValueError(
                f"Teradata host not configured for connection '{conn_id}'. "
                f"Set {m.group(1)} environment variable or provide host directly in profile."
            )
        if not effective_login:
            raise ValueError(
                f"Teradata username not configured for connection '{conn_id}'. "
                "Provide a teradata_profile from connections.yaml or set TERADATA_USERNAME."
            )
        m = _UNRESOLVED_ENV_VAR.search(str(effective_login))
        if m:
            raise ValueError(
                f"Teradata username not configured for connection '{conn_id}'. "
                f"Set {m.group(1)} environment variable or provide username directly in profile."
            )
        if not database:
            raise ValueError(
                f"Teradata database/schema not configured for connection '{conn_id}'. "
                "Provide a database parameter."
            )
        if not effective_password:
            raise ValueError(
                f"Teradata password not configured for connection '{conn_id}'. "
                "Provide a teradata_profile from connections.yaml or set TERADATA_PASSWORD."
            )
        m = _UNRESOLVED_ENV_VAR.search(str(effective_password))
        if m:
            var_name = m.group(1)
            raise ValueError(
                f"Teradata password not configured for connection '{conn_id}'. "
                f"Set {var_name} environment variable or provide password directly in profile."
            )

        match_fields = {
            "host": effective_host.lower().strip(),
            "schema": database.lower().strip(),
            "login": effective_login.lower().strip(),
        }
        conn_id, already_exists = await _find_or_reserve_conn_id(
            "teradata",
            conn_id,
            match_fields,
            default_port=effective_port,
        )
        if already_exists:
            return conn_id

        # Create the connection
        logger.info("Creating Teradata connection '%s'...", conn_id)
        try:
            await orchestrator.async_airflow_client.create_connection(
                conn_id=conn_id,
                conn_type="teradata",
                host=effective_host,
                schema=database,
                login=effective_login,
                password=effective_password,
                port=effective_port,
            )
        except Exception as e:
            raise RuntimeError(f"Cannot create Airflow connection '{conn_id}': {e}") from e
        logger.info("Created connection '%s' successfully", conn_id)
        return conn_id

    async def _ensure_ssh_connection(
        conn_id: str,
        ssh_profile: str | None = None,
        strict_ssh: bool = True,
    ) -> str:
        """Ensure an SSH Airflow connection exists for runtime execution.

        Delegates to the shared _create_airflow_ssh_connection() in
        airflow_pipeline_management, which handles credential resolution,
        duplicate detection, and Airflow API calls.

        Args:
            conn_id: Airflow SSH connection ID.
            ssh_profile: Profile name from connections.yaml.
            strict_ssh: When False, sets no_host_key_check=True on the connection.

        Returns:
            The connection ID of the matching or newly created connection.
        """
        pm_tools = _get_pipeline_tools()
        result = await pm_tools["airflow_connections"](
            action="create_ssh",
            connection_id=conn_id,
            ssh_profile=ssh_profile,
            strict_ssh=strict_ssh,
        )
        if not result.get("success"):
            error_msg = result.get("error", "Unknown error")
            raise ValueError(f"Cannot create Airflow SSH connection '{conn_id}': {error_msg}")
        return result.get("connection_id", conn_id)

    async def _generate_airflow_tdload_table_transfer_dag(
        source_database: str,
        source_table: str,
        target_database: str,
        target_table: str,
        dag_id: str | None = None,
        source_teradata_conn_id: str = "teradata_source",
        target_teradata_conn_id: str = "teradata_target",
        ssh_conn_id: str | None = None,
        schedule: str | None = None,
        generate_validations: bool = True,
        error_limit: int = 100,
        session_count: int = 4,
        owner: str = "data_engineer",
        email: list[str] | None = None,  # noqa: ARG001
        tags: list[str] | None = None,
        source_teradata_profile: str | None = None,
        target_teradata_profile: str | None = None,
        ssh_profile: str | None = None,
        strict_ssh: bool = True,
        dbt_project_dir: str | None = None,
        dbt_models: list[str] | None = None,
        dbt_target: str = "prod",
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
    ) -> dict[str, Any]:
        """
        Generate Airflow DAG using TdLoadOperator for Teradata table-to-table transfer.

        This is the NEW recommended approach for data transfer between Teradata tables.
        Uses Airflow's TdLoadOperator and BteqOperator.

        Credentials are resolved from connection profiles (connections.yaml)
        or from .env settings. The LLM never handles passwords.

        Args:
            source_database: Source Teradata database/schema
            source_table: Source Teradata table name
            target_database: Target Teradata database/schema
            target_table: Target Teradata table name
            dag_id: DAG identifier (auto-generated if None)
            source_teradata_conn_id: Airflow connection ID for source Teradata
            target_teradata_conn_id: Airflow connection ID for target Teradata
            ssh_conn_id: Airflow connection ID for SSH
            schedule: Cron expression or preset (@daily, @hourly, None for manual)
            generate_validations: Generate validation tasks using BteqOperator
            error_limit: Maximum errors before failure
            session_count: Number of parallel loading sessions
            owner: DAG owner
            email: Email addresses for notifications
            tags: List of tags for the DAG
            source_teradata_profile: Profile name from connections.yaml for source
                Teradata credentials. Falls back to .env TERADATA_SOURCE_* settings.
            target_teradata_profile: Profile name from connections.yaml for target
                Teradata credentials. Falls back to .env TERADATA_TARGET_* settings.
            ssh_profile: Profile name from connections.yaml for SSH credentials.
                Falls back to MCP_CLIENT_SSH_* environment variables.
            strict_ssh: Enforce SSH host-key verification (default True).
                Set to False only for trusted networks.

        Returns:
            Dictionary with DAG generation results including file path and code
        """
        try:
            from datetime import datetime

            from ..generators.airflow_tdload_dag_generator import (
                AirflowTdLoadDAGGenerator,
            )

            # Auto-generate DAG ID if not provided
            if dag_id is None:
                dag_id = f"transfer_{source_database}_{source_table}_to_{target_database}_{target_table}".lower()
                logger.info("Auto-generated DAG ID: %s", dag_id)

            # C4: Derive SSH connection ID from dag_id if not provided
            if ssh_conn_id is None:
                ssh_conn_id = f"ssh_{dag_id}"

            # Resolve source credentials from profile or .env
            source_td_settings = orchestrator.settings.teradata
            source_host_override = None
            source_username_override = None
            source_password_override = None
            source_port_override = None
            if source_teradata_profile:
                guard = orchestrator.credential_resolver.guard_configured()
                if guard:
                    return guard
                src_profile = orchestrator.credential_resolver.resolve_profile(
                    source_teradata_profile
                )
                source_host_override = src_profile.get("host")
                source_username_override = src_profile.get("username")
                source_password_override = src_profile.get("password")
                raw_src_port = src_profile.get("port")
                if raw_src_port is not None:
                    source_port_override = int(raw_src_port)

            source_teradata_conn_id = await _ensure_teradata_connection(
                conn_id=source_teradata_conn_id,
                database=source_database,
                td_settings=source_td_settings,
                host_override=source_host_override,
                username_override=source_username_override,
                password_override=source_password_override,
                port_override=source_port_override,
            )

            # Resolve target credentials from profile or .env
            target_td_settings = orchestrator.settings.teradata
            target_host_override = None
            target_username_override = None
            target_password_override = None
            target_port_override = None
            if target_teradata_profile:
                guard = orchestrator.credential_resolver.guard_configured()
                if guard:
                    return guard
                tgt_profile = orchestrator.credential_resolver.resolve_profile(
                    target_teradata_profile
                )
                target_host_override = tgt_profile.get("host")
                target_username_override = tgt_profile.get("username")
                target_password_override = tgt_profile.get("password")
                raw_tgt_port = tgt_profile.get("port")
                if raw_tgt_port is not None:
                    target_port_override = int(raw_tgt_port)

            target_teradata_conn_id = await _ensure_teradata_connection(
                conn_id=target_teradata_conn_id,
                database=target_database,
                td_settings=target_td_settings,
                host_override=target_host_override,
                username_override=target_username_override,
                password_override=target_password_override,
                port_override=target_port_override,
            )

            # Ensure SSH connection exists for TdLoadOperator
            ssh_conn_id = await _ensure_ssh_connection(
                conn_id=ssh_conn_id,
                ssh_profile=ssh_profile,
                strict_ssh=strict_ssh,
            )

            # Get source table metadata for schema generation
            logger.info("Fetching source table metadata from %s.%s", source_database, source_table)
            source_metadata = await asyncio.to_thread(
                orchestrator.teradata_client.get_table_metadata,
                source_database,
                source_table,
                include_stats=False,
            )

            # Prepare validation queries if requested.
            validation_queries = None
            if generate_validations:
                q_src_db = _quote_id(source_database)
                q_src_tbl = _quote_id(source_table)
                q_tgt_db = _quote_id(target_database)
                q_tgt_tbl = _quote_id(target_table)
                validation_queries = [
                    {
                        "name": "source_row_count",
                        "sql": f"""
-- Validating source table row count
SELECT COUNT(*) as source_row_count FROM {q_src_db}.{q_src_tbl};
.IF ACTIVITYCOUNT = 0 THEN .QUIT 12;
""",
                        "teradata_conn_id": source_teradata_conn_id,
                    },
                    {
                        "name": "target_row_count",
                        "sql": f"""
-- Validating target table row count
SELECT COUNT(*) as target_row_count FROM {q_tgt_db}.{q_tgt_tbl};
.IF ACTIVITYCOUNT = 0 THEN .QUIT 12;
""",
                        "teradata_conn_id": target_teradata_conn_id,
                    },
                ]

            # Generate Airflow DAG
            dags_folder = Path(orchestrator.settings.pipeline.dags_output_dir)
            dag_generator = AirflowTdLoadDAGGenerator(dags_folder=dags_folder)

            _transfer_doc_md = f"""
# Teradata Table-to-Table Data Transfer

**Source:** `{source_database}.{source_table}`
**Target:** `{target_database}.{target_table}`

## Operator Used
- **TdLoadOperator**: Enterprise-grade Teradata table transfer using TPT API
- **BteqOperator**: Validation queries

## Configuration
- **Error Limit:** {error_limit}
- **Sessions:** {session_count}
"""
            _transfer_common_kwargs = dict(
                dag_id=dag_id,
                description=f"Transfer data from {source_database}.{source_table} to {target_database}.{target_table} using TdLoadOperator",
                source_database=source_database,
                source_table=source_table,
                target_database=target_database,
                target_table=target_table,
                source_metadata=source_metadata,
                source_teradata_conn_id=source_teradata_conn_id,
                target_teradata_conn_id=target_teradata_conn_id,
                ssh_conn_id=ssh_conn_id,
                schedule=schedule,
                start_date=datetime(2024, 1, 1),
                validation_queries=validation_queries,
                owner=owner,
                tags=tags or ["teradata", "table_transfer", "tdload"],
                doc_md=_transfer_doc_md,
            )

            if dbt_project_dir:
                dag_generator.generate_table_transfer_with_dbt_dag(
                    **_transfer_common_kwargs,
                    dbt_project_dir=dbt_project_dir,
                    dbt_models=dbt_models,
                    dbt_target=dbt_target,
                    run_dbt_tests=run_dbt_tests,
                    generate_dbt_docs=generate_dbt_docs,
                )
            else:
                dag_generator.generate_table_transfer_dag(**_transfer_common_kwargs)

            dag_file_path = dags_folder / f"{dag_id}.py"

            result = {
                "success": True,
                "dag_id": dag_id,
                "dag_file_path": str(dag_file_path),
                "source_database": source_database,
                "source_table": source_table,
                "target_database": target_database,
                "target_table": target_table,
                "operator_type": "TdLoadOperator",
                "validation_tasks": (len(validation_queries) if validation_queries else 0),
                "message": f"Airflow DAG generated successfully. Deploy to Airflow and trigger using: airflow dags trigger {dag_id}",
                "next_steps": [
                    (
                        f"**1. Deploy the DAG**: "
                        f"`pipeline_deploy(action='deploy_dags', "
                        f"pipeline_name='{dag_id}')`. **Why**: the DAG file "
                        f"is local until SFTP'd to the Airflow server's "
                        f"``dags_folder``. **Effect**: copies "
                        f"``{dag_id}.py`` to the Airflow server. **If "
                        f"missing**: skip if Airflow already reads the "
                        f"local DAGs folder."
                    ),
                    (
                        f"**2. Trigger the DAG**: "
                        f"`dag_trigger(mode='run', pipeline_name='{dag_id}')`. "
                        f"**Why**: deployment alone doesn't move any rows; "
                        f"the transfer runs only when Airflow executes the "
                        f"DAG. **Effect**: TdLoadOperator copies "
                        f"``{source_database}.{source_table}`` -> "
                        f"``{target_database}.{target_table}``. **If "
                        f"missing**: skip if a ``schedule`` was set."
                    ),
                    (
                        f"**3. Verify the row counts**: "
                        f"`teradata_query(query='SELECT COUNT(*) FROM "
                        f"{target_database}.{target_table}')`. **Why**: a "
                        f"green DAG run isn't proof of equal row counts; "
                        f"validate before downstream consumers read the "
                        f"target. **Effect**: returns the row count of the "
                        f"target table. **If missing**: skip if the DAG's "
                        f"validation tasks already enforce equality."
                    ),
                ],
            }

            logger.info("Airflow TdLoad table transfer DAG generated: %s", dag_file_path)
            return result

        except ValueError as e:
            return {
                "success": False,
                "error": safe_error_message(e),
                "source_database": source_database,
                "source_table": source_table,
                "target_database": target_database,
                "target_table": target_table,
            }
        except Exception as e:
            logger.error(
                "Failed to generate Airflow TdLoad table transfer DAG: %s", e, exc_info=True
            )
            return {
                "success": False,
                "error": "Failed to generate Airflow TdLoad table transfer DAG. Check server logs for details.",
                "source_database": source_database,
                "source_table": source_table,
                "target_database": target_database,
                "target_table": target_table,
            }

    async def _preview_pipeline(
        source_name: str,
        source_type: str,
        source_profile: str,  # noqa: ARG001 — kept in signature for API consistency
        streams: list[dict[str, Any]] | None = None,
        intent: str | None = None,
        destination_type: str | None = None,
        policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Preview a pipeline without creating any resources.

        Validates source type, checks for existing source, discovers streams,
        and validates stream names or matches intent — all without creating
        a destination or connection.

        Credentials are resolved from a connection profile (connections.yaml).

        Args:
            source_name: Name of the Airbyte source to preview
            source_type: Connector type (e.g., "Postgres") — LLM determines dynamically
            source_profile: Profile name from connections.yaml for source credentials
            streams: Optional list of stream configurations to validate
            intent: Natural language description of desired streams
            destination_type: Optional destination type for context
            policy: Sync policy configuration
        """
        try:
            # 1. Resolve source definition
            src_def_id = await orchestrator.airbyte_client.find_definition_id_by_name(
                "source", source_type
            )
            if not src_def_id:
                return {
                    "success": False,
                    "preview": True,
                    "error": f"Source definition for type '{source_type}' not found.",
                }

            # 2. Find existing source by name (NO creation)
            existing_sources = await orchestrator.airbyte_client.list_sources()
            source = None
            for s in existing_sources:
                if (
                    s.get("name")
                    and str(s.get("name")).strip().lower() == str(source_name).strip().lower()
                ):
                    source = s
                    break

            if not source:
                return {
                    "success": False,
                    "preview": True,
                    "source_exists": False,
                    "message": (
                        f"No existing source named '{source_name}' found. "
                        "Create the source first using create_airbyte_source or "
                        "use create_intelligent_airbyte_pipeline for end-to-end creation."
                    ),
                }

            source_id = source.get("sourceId")

            # 3. Discovery via cache
            cache = DiscoveryCache(orchestrator.airbyte_client)
            disc = await cache.get(source_id)
            available_names = _extract_stream_names(disc)

            matched_streams: list[dict[str, Any]] = []
            issues: list[str] = []

            # 4. If streams provided: validate each
            if streams:
                for s in streams:
                    s_name = s.get("name")
                    if not s_name or s_name == "*":
                        matched_streams.append(s)
                        continue
                    if s_name in available_names or s_name.lower() in {
                        n.lower() for n in available_names
                    }:
                        matched_streams.append(s)
                    else:
                        suggestions = _suggest_stream_names(s_name, available_names)
                        issues.append(
                            f"Stream '{s_name}' not found. Did you mean: {suggestions[:3]}"
                        )

            # 5. If intent provided: run scoring
            elif intent:
                kws = _intent_keywords(intent, extra_synonyms=(policy or {}).get("synonyms"))
                index = await _build_stream_index(source_id)
                ranked = []
                for item in index.get("streams") or []:
                    score = _score_stream_v2(item, kws)
                    if score > 0:
                        ranked.append((item, score))
                ranked.sort(key=lambda x: x[1], reverse=True)
                for item, _sc in ranked:
                    matched_streams.append(
                        {
                            "name": item.get("name"),
                            "score": round(_sc, 2),
                            "selected": True,
                        }
                    )
                if not matched_streams:
                    issues.append(
                        f"No streams matched intent '{intent}' with keywords {kws}. "
                        f"Available streams: {available_names}"
                    )

            # 6. Check destination_type validity if provided
            if destination_type:
                dst_def_id = await orchestrator.airbyte_client.find_definition_id_by_name(
                    "destination", destination_type
                )
                if not dst_def_id:
                    issues.append(
                        f"Destination definition for type '{destination_type}' not found."
                    )

            return sanitize_response(
                {
                    "success": True,
                    "preview": True,
                    "source_exists": True,
                    "source_id": source_id,
                    "available_streams": available_names,
                    "matched_streams": matched_streams,
                    "matched_count": len(matched_streams),
                    "issues": issues,
                    "has_issues": len(issues) > 0,
                    "message": (
                        f"Preview: {len(matched_streams)} streams would be synced "
                        f"from {len(available_names)} available."
                        + (f" Issues: {'; '.join(issues)}" if issues else " No issues.")
                    ),
                }
            )
        except Exception as e:
            logger.error("Preview pipeline failed: %s", e, exc_info=True)
            return {"success": False, "preview": True, "error": safe_error_message(e)}

    async def _test_airbyte_connection() -> dict[str, Any]:
        """
        Test connectivity to the Airbyte instance.

        Calls the Airbyte /health endpoint to verify the MCP server
        can reach the configured Airbyte API. Use this to check
        whether Airbyte is available before creating pipelines or
        triggering syncs.

        Returns:
            Dictionary with connection status, Airbyte URL, and health details
        """
        try:
            logger.info("Testing Airbyte connection")
            health = await orchestrator.airbyte_client.get_health()

            if not isinstance(health, dict) or not health.get("connected"):
                error = (
                    health.get("error", "Unknown error")
                    if isinstance(health, dict)
                    else "Unknown error"
                )
                return {
                    "success": False,
                    "status": "failed",
                    "url": orchestrator.settings.airbyte.base_url,
                    "error": error,
                    "message": "Cannot reach Airbyte. Check the URL, credentials, and network.",
                }

            return {
                "success": True,
                "status": "connected",
                "url": orchestrator.settings.airbyte.base_url,
                "airbyte_status": health.get("status", "unknown"),
                "message": "Airbyte connection is healthy",
            }
        except Exception as e:
            logger.error("Airbyte connection test failed: %s", e, exc_info=True)
            return {
                "success": False,
                "status": "failed",
                "url": getattr(
                    getattr(orchestrator.settings, "airbyte", None),
                    "base_url",
                    "unknown",
                ),
                "error": safe_error_message(e),
                "message": "Cannot reach Airbyte. Check the URL, credentials, and network.",
            }

    async def _check_airbyte_source_connection(source_id: str) -> dict[str, Any]:
        """
        Test whether an Airbyte source can connect to its upstream system.

        Verifies the source by fetching its details and attempting schema
        discovery.  If Airbyte can discover streams the source is healthy.

        Args:
            source_id: The UUID of the Airbyte source to test

        Returns:
            Dictionary with connection check status and discovered stream count
        """
        try:
            logger.info("Checking source connection: %s", source_id)

            # Fetch source metadata to confirm it exists
            source_info = await orchestrator.airbyte_client.get_source(source_id)
            source_name = (
                source_info.get("name", source_id) if isinstance(source_info, dict) else source_id
            )

            # Attempt schema discovery — this actually connects to the upstream system
            schema = await orchestrator.airbyte_client.discover_source_schema(source_id)
            streams = (
                schema.get("catalog", {}).get("streams", []) if isinstance(schema, dict) else []
            )

            return {
                "success": True,
                "source_id": source_id,
                "source_name": source_name,
                "status": "connected",
                "stream_count": len(streams),
                "message": (
                    f"Source '{source_name}' is reachable — discovered {len(streams)} stream(s)"
                ),
            }
        except Exception as e:
            logger.error("Source connection check failed for %s: %s", source_id, e, exc_info=True)
            return {
                "success": False,
                "source_id": source_id,
                "status": "failed",
                "error": safe_error_message(e),
                "message": "Source connection check failed. Verify the source configuration and credentials.",
            }

    async def _check_airbyte_destination_connection(destination_id: str) -> dict[str, Any]:
        """
        Test whether an Airbyte destination is configured and reachable.

        Fetches the destination details from Airbyte to confirm it exists
        and is properly configured.

        Args:
            destination_id: The UUID of the Airbyte destination to test

        Returns:
            Dictionary with connection check status
        """
        try:
            logger.info("Checking destination connection: %s", destination_id)

            dest_info = await orchestrator.airbyte_client.get_destination(destination_id)
            dest_name = (
                dest_info.get("name", destination_id)
                if isinstance(dest_info, dict)
                else destination_id
            )
            dest_type = (
                dest_info.get("destinationType", "unknown")
                if isinstance(dest_info, dict)
                else "unknown"
            )

            return {
                "success": True,
                "destination_id": destination_id,
                "destination_name": dest_name,
                "destination_type": dest_type,
                "status": "configured",
                "message": (f"Destination '{dest_name}' ({dest_type}) is configured in Airbyte"),
            }
        except Exception as e:
            logger.error(
                "Destination connection check failed for %s: %s", destination_id, e, exc_info=True
            )
            return {
                "success": False,
                "destination_id": destination_id,
                "status": "failed",
                "error": safe_error_message(e),
                "message": "Destination check failed. Verify the destination ID and Airbyte configuration.",
            }

    async def _check_airbyte_pipeline_health(connection_id: str) -> dict[str, Any]:
        """
        Run a full health check on an Airbyte pipeline (connection).

        Given a connection ID, checks all components end-to-end:
        1. Airbyte API reachability
        2. Source connectivity (via schema discovery)
        3. Destination configuration
        4. Last sync job status

        Use this to answer questions like "is my Airbyte pipeline healthy?"

        Args:
            connection_id: The UUID of the Airbyte connection to check

        Returns:
            Dictionary with per-component health status and overall result
        """
        checks: dict[str, Any] = {}
        errors: list[str] = []

        # 1. Airbyte API health
        try:
            health = await orchestrator.airbyte_client.get_health()
            if isinstance(health, dict) and health.get("connected"):
                checks["airbyte_api"] = {"status": "ok"}
            else:
                checks["airbyte_api"] = {"status": "failed"}
                errors.append("Airbyte API is not reachable")
        except Exception as e:
            checks["airbyte_api"] = {"status": "failed", "error": safe_error_message(e)}
            errors.append(f"Airbyte API: {safe_error_message(e)}")

        # 2. Connection details (source ID + destination ID)
        source_id = None
        destination_id = None
        connection_name = connection_id
        try:
            conn = await orchestrator.airbyte_client.get_connection(connection_id)
            source_id = conn.get("sourceId")
            destination_id = conn.get("destinationId")
            connection_name = conn.get("name", connection_id)
            conn_status = conn.get("status", "unknown")
            checks["connection"] = {
                "status": "ok",
                "name": connection_name,
                "connection_status": conn_status,
                "schedule_type": (
                    conn.get("schedule", {}).get("scheduleType")
                    or conn.get("scheduleType")
                    or "unknown"
                ),
            }
        except Exception as e:
            checks["connection"] = {"status": "failed", "error": safe_error_message(e)}
            errors.append(f"Connection: {safe_error_message(e)}")

        # 3. Source connectivity (schema discovery)
        if source_id:
            try:
                source_info = await orchestrator.airbyte_client.get_source(source_id)
                source_name = (
                    source_info.get("name", source_id)
                    if isinstance(source_info, dict)
                    else source_id
                )
                schema = await orchestrator.airbyte_client.discover_source_schema(source_id)
                streams = (
                    schema.get("catalog", {}).get("streams", []) if isinstance(schema, dict) else []
                )
                checks["source"] = {
                    "status": "ok",
                    "source_id": source_id,
                    "name": source_name,
                    "stream_count": len(streams),
                }
            except Exception as e:
                checks["source"] = {
                    "status": "failed",
                    "source_id": source_id,
                    "error": safe_error_message(e),
                }
                errors.append(f"Source: {safe_error_message(e)}")
        else:
            checks["source"] = {"status": "skipped", "reason": "No source ID found on connection"}

        # 4. Destination
        if destination_id:
            try:
                dest_info = await orchestrator.airbyte_client.get_destination(destination_id)
                dest_name = (
                    dest_info.get("name", destination_id)
                    if isinstance(dest_info, dict)
                    else destination_id
                )
                checks["destination"] = {
                    "status": "ok",
                    "destination_id": destination_id,
                    "name": dest_name,
                    "destination_type": dest_info.get("destinationType", "unknown")
                    if isinstance(dest_info, dict)
                    else "unknown",
                }
            except Exception as e:
                checks["destination"] = {
                    "status": "failed",
                    "destination_id": destination_id,
                    "error": safe_error_message(e),
                }
                errors.append(f"Destination: {safe_error_message(e)}")
        else:
            checks["destination"] = {
                "status": "skipped",
                "reason": "No destination ID found on connection",
            }

        # 5. Last sync job
        try:
            jobs = await orchestrator.airbyte_client.list_jobs(
                config_type="sync", config_id=connection_id
            )
            if jobs:
                jobs.sort(
                    key=lambda j: j.get("createdAt") or j.get("startTime") or "", reverse=True
                )
                latest = jobs[0]
                job_status = latest.get("status", "unknown")
                checks["last_sync"] = {
                    "status": "ok" if job_status in ("succeeded", "running") else "warning",
                    "job_id": latest.get("jobId"),
                    "job_status": job_status,
                    "started_at": latest.get("startTime"),
                    "bytes_synced": latest.get("bytesSynced", 0),
                    "rows_synced": latest.get("rowsSynced", 0),
                }
                if job_status == "failed":
                    errors.append(f"Last sync failed (job {latest.get('jobId')})")
                elif job_status not in ("succeeded", "running"):
                    errors.append(f"Last sync status is '{job_status}' (job {latest.get('jobId')})")
            else:
                checks["last_sync"] = {
                    "status": "info",
                    "message": "No sync jobs found for this connection",
                }
        except Exception as e:
            checks["last_sync"] = {"status": "failed", "error": safe_error_message(e)}
            errors.append(f"Last sync check failed: {safe_error_message(e)}")

        all_ok = all(c.get("status") in ("ok", "info", "skipped") for c in checks.values())

        # Derive error details from failed checks if errors list is still empty
        if not all_ok and not errors:
            for check_name, check_result in checks.items():
                status = check_result.get("status")
                if status not in ("ok", "info", "skipped"):
                    detail = check_result.get("error") or check_result.get("message") or status
                    errors.append(f"{check_name}: {detail}")

        return {
            "success": all_ok,
            "connection_id": connection_id,
            "connection_name": connection_name,
            "overall_status": "healthy" if all_ok else ("unhealthy" if errors else "degraded"),
            "checks": checks,
            "errors": errors,
            "message": (
                f"Pipeline '{connection_name}' is healthy — all checks passed"
                if all_ok
                else f"Pipeline '{connection_name}' has issues: {'; '.join(errors)}"
            ),
        }

    async def _delete_airbyte_source(source_id: str) -> dict[str, Any]:
        """
        Delete an Airbyte source by its ID.

        Permanently removes the source definition from Airbyte.
        Any connections using this source will also be affected.

        Args:
            source_id: The UUID of the Airbyte source to delete

        Returns:
            Dictionary with deletion result
        """
        try:
            logger.info("Deleting Airbyte source: %s", source_id)
            await orchestrator.airbyte_client.delete_source(source_id)
            return {
                "success": True,
                "source_id": source_id,
                "message": f"Source {source_id} deleted successfully",
            }
        except Exception as e:
            logger.error("Failed to delete source %s: %s", source_id, e, exc_info=True)
            return {
                "success": False,
                "source_id": source_id,
                "error": safe_error_message(e),
            }

    async def _delete_airbyte_destination(destination_id: str) -> dict[str, Any]:
        """
        Delete an Airbyte destination by its ID.

        Permanently removes the destination definition from Airbyte.
        Any connections using this destination will also be affected.

        Args:
            destination_id: The UUID of the Airbyte destination to delete

        Returns:
            Dictionary with deletion result
        """
        try:
            logger.info("Deleting Airbyte destination: %s", destination_id)
            await orchestrator.airbyte_client.delete_destination(destination_id)
            return {
                "success": True,
                "destination_id": destination_id,
                "message": f"Destination {destination_id} deleted successfully",
            }
        except Exception as e:
            logger.error("Failed to delete destination %s: %s", destination_id, e, exc_info=True)
            return {
                "success": False,
                "destination_id": destination_id,
                "error": safe_error_message(e),
            }

    async def _delete_airbyte_connection(connection_id: str) -> dict[str, Any]:
        """
        Delete an Airbyte connection by its ID.

        Permanently removes the connection (sync pipeline) from Airbyte.
        The underlying source and destination are not deleted.

        Args:
            connection_id: The UUID of the Airbyte connection to delete

        Returns:
            Dictionary with deletion result
        """
        try:
            logger.info("Deleting Airbyte connection: %s", connection_id)
            await orchestrator.airbyte_client.delete_connection(connection_id)
            return {
                "success": True,
                "connection_id": connection_id,
                "message": f"Connection {connection_id} deleted successfully",
            }
        except Exception as e:
            logger.error("Failed to delete connection %s: %s", connection_id, e, exc_info=True)
            return {
                "success": False,
                "connection_id": connection_id,
                "error": safe_error_message(e),
            }

    async def _get_affected_connections_for_source(source_id: str) -> list[dict]:
        try:
            connections = await orchestrator.airbyte_client.list_connections()
            return [
                {"connection_id": c.get("connectionId"), "name": c.get("name", "")}
                for c in connections
                if c.get("sourceId") == source_id
            ]
        except Exception:
            return []

    async def _get_affected_connections_for_destination(destination_id: str) -> list[dict]:
        try:
            connections = await orchestrator.airbyte_client.list_connections()
            return [
                {"connection_id": c.get("connectionId"), "name": c.get("name", "")}
                for c in connections
                if c.get("destinationId") == destination_id
            ]
        except Exception:
            return []

    def _spec_to_yaml_template(
        spec: dict[str, Any],
        connector_name: str,
        connector_type: str,
    ) -> tuple[str, list[str], list[str], list[str]]:
        """Generate YAML template, required/optional/secret field lists from connectionSpecification.

        Returns: (yaml_string, required_fields, optional_fields, secret_fields)
        """
        required_set = set(spec.get("required", []))
        properties = spec.get("properties", {})

        required_fields = []
        optional_fields = []
        secret_fields = []

        for prop_name, prop_schema in properties.items():
            if prop_name in required_set:
                required_fields.append(prop_name)
            else:
                optional_fields.append(prop_name)
            if prop_schema.get("airbyte_secret"):
                secret_fields.append(prop_name)

        required_fields.sort()
        optional_fields.sort()
        secret_fields.sort()

        profile_name = connector_name.lower().replace(" ", "_") + "_profile"
        yaml_lines = [
            f"# Profile template for: {connector_name} {connector_type}",
            "# Add this under 'profiles:' in your connections.yaml",
            "",
            f"{profile_name}:",
        ]

        for field in required_fields:
            schema = properties[field]
            placeholder = _get_placeholder(field, schema, secret=field in secret_fields)
            yaml_lines.append(f"  {field}: {placeholder}")

        for field in optional_fields:
            schema = properties[field]
            placeholder = _get_placeholder(field, schema, secret=field in secret_fields)
            yaml_lines.append(f"  # {field}: {placeholder}")

        yaml_str = "\n".join(yaml_lines)
        return yaml_str, required_fields, optional_fields, secret_fields

    def _get_placeholder(field_name: str, schema: dict[str, Any], secret: bool = False) -> str:
        """Generate a placeholder value for a schema field."""
        if secret:
            return f"${{{field_name.upper()}}}"

        if "default" in schema:
            default = schema["default"]
            if isinstance(default, str):
                return f'"{default}"'
            return str(default)

        field_type = schema.get("type", "string")
        if field_type == "integer":
            return "0"
        elif field_type == "boolean":
            return "false"
        elif field_type == "array":
            return "[]"
        elif field_type == "object":
            if "oneOf" in schema:
                first_option = schema["oneOf"][0]
                if isinstance(first_option, dict) and "properties" in first_option:
                    pairs = ", ".join(
                        f"{k}: YOUR_{k.upper()}"
                        for k in first_option.get("properties", {}).keys()
                    )
                    return "{" + pairs + "}"
            return "{}"

        return f"YOUR_{field_name.upper()}"

    async def _get_profile_template(
        name: str | None,
        source_definition_id: str | None,
        destination_definition_id: str | None,
    ) -> dict[str, Any]:
        """Generate profile templates for Airbyte sources/destinations."""
        if not name and not source_definition_id and not destination_definition_id:
            return {
                "success": False,
                "error": "get_profile_template requires 'name' (connector name) or 'source_definition_id'/'destination_definition_id'.",
            }

        targets: list[tuple[str, str, str | None]] = []

        if source_definition_id:
            targets.append(("source", source_definition_id, name))
        if destination_definition_id:
            targets.append(("destination", destination_definition_id, name))

        if not targets and name:
            src_id, dst_id = await asyncio.gather(
                orchestrator.airbyte_client.find_definition_id_by_name("source", name),
                orchestrator.airbyte_client.find_definition_id_by_name("destination", name),
                return_exceptions=True,
            )
            if isinstance(src_id, Exception) and isinstance(dst_id, Exception):
                return {
                    "success": False,
                    "error": f"Unable to look up connector '{name}': {src_id}. Check that Airbyte is configured (AIRBYTE_BASE_URL) and reachable.",
                }
            if isinstance(src_id, Exception):
                src_id = None
            if isinstance(dst_id, Exception):
                dst_id = None

            if src_id:
                targets.append(("source", src_id, name))
            if dst_id:
                targets.append(("destination", dst_id, name))

        if not targets:
            return {
                "success": False,
                "error": f"Connector '{name}' not found in the Airbyte OSS registry. Use airbyte_inventory(list_type='connectors') to list available connectors.",
            }

        results = []
        for connector_type, definition_id, provided_name in targets:
            registry_name, spec = await _get_connector_spec(orchestrator, connector_type, definition_id)
            if spec is None:
                return {
                    "success": False,
                    "error": f"Could not fetch connector specification for {connector_type} {definition_id}. Airbyte registry may be unavailable.",
                }

            connector_name = provided_name or registry_name or definition_id
            yaml_template, req_fields, opt_fields, secret_fields = _spec_to_yaml_template(
                spec, connector_name, connector_type
            )

            results.append(
                {
                    "connector_type": connector_type,
                    "connector_name": connector_name,
                    "definition_id": definition_id,
                    "profile_template": yaml_template,
                    "required_fields": req_fields,
                    "optional_fields": opt_fields,
                    "secret_fields": secret_fields,
                    "instructions": (
                        "Next, you can:\n"
                        "1. Add the above YAML block to your connections.yaml under 'profiles:'\n"
                        "2. After saving the file, reload connections.yaml using the ETL MCP server setup wizard\n"
                        f"3. Ask Copilot to create an Airbyte {connector_type} using the profile you just added"
                    ),
                }
            )

        return {"success": True, "results": results}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 1: airbyte_pipeline
    # ══════════════════════════════════════════════════════════════

    async def airbyte_pipeline(
        action: Literal["create", "update", "preview", "check_health"],
        # create params
        source_name: str | None = None,
        source_type: str | None = None,
        source_profile: str | None = None,
        destination_name: str | None = None,
        destination_type: str | None = None,
        destination_profile: str | None = None,
        streams: list[dict[str, Any]] | None = None,
        connection_name: str = "",
        schedule_type: str | None = None,
        schedule_cron: str | None = None,
        airflow_orchestrated: bool = False,
        namespace_definition: str = "destination",
        namespace_format: str | None = None,
        intent: str | None = None,
        policy: dict[str, Any] | None = None,
        dry_run: bool = False,
        # update params
        connection_id: str | None = None,
        status: str | None = None,
        configurations: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create, update, preview, or health-check Airbyte pipelines.

        ELT Pipeline Workflow — Sequential Prompts Required:
          This tool handles ONLY data transfer configuration via Airbyte.
          After creating a pipeline, the user must separately:
          1. Generate a sync DAG: pipeline_deploy(action='create_sync_dag', ...)
          2. Deploy to Airflow: pipeline_deploy(action='deploy_dags', ...)
          3. Trigger execution: dag_trigger(mode='run', dag_id='...', ...)
          4. Generate dbt models: dbt_generate_model(model_type='staging', ...)
          5. Execute dbt: dbt_execute(command='run', models=[...])
          Each step should be a separate user prompt.

        Args:
            action: One of:
                - "create"       — Create an Airbyte pipeline (source + destination + connection).
                - "update"       — Update an existing Airbyte connection.
                - "preview"      — Preview pipeline without creating resources.
                - "check_health" — Full health check on an Airbyte pipeline.
            source_name: Display name for source (create/preview).
            source_type: Connector type name e.g. "Postgres" (create/preview).
            source_profile: Optional profile from connections.yaml for source credentials.
                If not provided, uses the default Teradata connection.
            destination_name: Display name for destination (create).
            destination_type: Connector type name e.g. "Teradata" (create/preview).
            destination_profile: Optional profile from connections.yaml for destination credentials.
                If not provided, uses the default Teradata connection.
            streams: List of stream configurations (create/preview).
            connection_name: Optional connection name (create).
            schedule_type: "manual" or "cron" (create/update). For update,
                passing "manual" with schedule_cron is a conflict error.
            schedule_cron: Cron expression (create/update). Required when
                schedule_type is "cron". If provided alone, implies cron.
            airflow_orchestrated: Set True when this connection will be triggered by
                an Airflow DAG (AirbyteTriggerSyncOperator). For new connections,
                forces schedule_type="manual". For existing connections that already
                have a cron schedule, auto-updates them to manual. The cron
                expression is returned in the response as 'intended_schedule_cron'
                for use as the DAG schedule.
            namespace_definition: Namespace handling (create/update).
            namespace_format: Custom namespace format (create/update).
            intent: Natural language description of desired streams (create/preview).
            policy: Sync policy configuration (create/preview).
            dry_run: Validate without creating resources (create).
            connection_id: Connection ID (update/check_health). Required for those actions.
            status: Connection status for update (e.g., "active", "inactive").
            configurations: Configurations object for update.

        Returns:
            Dictionary with pipeline operation results.
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        action = action.strip().lower()
        if connection_name and isinstance(connection_name, str):
            connection_name = connection_name.strip()
        if connection_name:
            if not isinstance(connection_name, str):
                return {
                    "success": False,
                    "error": "Parameter 'connection_name' must be a string.",
                }
            err = validate_identifier(connection_name, "connection_name")
            if err:
                return {"success": False, "error": err}
        if source_name:
            err = validate_identifier(source_name, "source_name")
            if err:
                return {"success": False, "error": err}
        if destination_name:
            err = validate_identifier(destination_name, "destination_name")
            if err:
                return {"success": False, "error": err}
        try:
            # Validate cron expression for Airbyte (accepts 5-field Unix or 6-field Quartz)
            if schedule_cron:
                valid, cron_err = PipelineValidator.validate_schedule(
                    schedule_cron, allow_quartz=True
                )
                if not valid:
                    return {"success": False, "error": f"Invalid cron expression: {cron_err}"}
            if action == "create":
                if not source_name or not source_type:
                    return {
                        "success": False,
                        "error": "Parameters 'source_name', 'source_type' are required for create.",
                    }
                if not destination_name or not destination_type:
                    return {
                        "success": False,
                        "error": "Parameters 'destination_name', 'destination_type' are required for create.",
                    }
                if not isinstance(connection_name, str):
                    return {
                        "success": False,
                        "error": "Parameter 'connection_name' must be a string.",
                    }
                connection_name = connection_name.strip()
                if not connection_name:
                    connection_name = f"{source_name} → {destination_name}"
                return await _create_intelligent_airbyte_pipeline(
                    source_name=source_name,
                    source_type=source_type,
                    source_profile=source_profile,
                    destination_name=destination_name,
                    destination_type=destination_type,
                    destination_profile=destination_profile,
                    streams=streams,
                    connection_name=connection_name,
                    schedule_type=schedule_type,
                    schedule_cron=schedule_cron,
                    namespace_definition=namespace_definition,
                    namespace_format=namespace_format,
                    intent=intent,
                    policy=policy,
                    dry_run=dry_run,
                    airflow_orchestrated=airflow_orchestrated,
                )
            elif action == "update":
                if not connection_id:
                    return {
                        "success": False,
                        "error": "Parameter 'connection_id' is required for update.",
                    }
                return await _update_airbyte_connection(
                    connection_id=connection_id,
                    schedule_type=schedule_type,
                    schedule_cron=schedule_cron,
                    namespace_definition=namespace_definition
                    if namespace_definition != "destination"
                    else None,
                    namespace_format=namespace_format,
                    status=status,
                    configurations=configurations,
                )
            elif action == "preview":
                if not source_name or not source_type:
                    return {
                        "success": False,
                        "error": "Parameters 'source_name', 'source_type' are required for preview.",
                    }
                return await _preview_pipeline(
                    source_name=source_name,
                    source_type=source_type,
                    source_profile=source_profile,
                    streams=streams,
                    intent=intent,
                    destination_type=destination_type,
                    policy=policy,
                )
            elif action == "check_health":
                if not connection_id:
                    return {
                        "success": False,
                        "error": "Parameter 'connection_id' is required for check_health.",
                    }
                return await _check_airbyte_pipeline_health(connection_id)
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown action '{action}'. "
                        "Valid actions: create, update, preview, check_health"
                    ),
                }
        except Exception as e:
            logger.error("airbyte_pipeline(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 2: airbyte_sync
    # ══════════════════════════════════════════════════════════════

    async def airbyte_sync(
        action: Literal["trigger", "get_status", "wait"],
        connection_id: str | None = None,
        job_id: int | None = None,
        wait_for_completion: bool = False,
        include_logs: bool = False,
        timeout: int = 3600,
        poll_interval: int = 10,
    ) -> dict[str, Any]:
        """Trigger, monitor, or wait for Airbyte sync jobs.

        Args:
            action: One of:
                - "trigger"    — Trigger a new sync for a connection.
                - "get_status" — Get status of a sync job.
                - "wait"       — Wait for a sync job to complete.
            connection_id: Airbyte connection ID. Required for trigger.
            job_id: Airbyte job ID. Required for get_status and wait.
            wait_for_completion: Wait for sync to complete on trigger (default False).
            include_logs: Include logs in get_status (default False).
            timeout: Max seconds to wait (default 3600).
            poll_interval: Seconds between polls (default 10).

        Returns:
            Dictionary with sync job information.
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        if timeout < 1:
            return {"success": False, "error": "Parameter 'timeout' must be >= 1."}
        if poll_interval < 1:
            return {"success": False, "error": "Parameter 'poll_interval' must be >= 1."}
        action = action.strip().lower()
        try:
            if action == "trigger":
                if not connection_id:
                    return {
                        "success": False,
                        "error": "Parameter 'connection_id' is required for trigger.",
                    }
                return await _trigger_airbyte_sync(connection_id, wait_for_completion)
            elif action == "get_status":
                if job_id is None:
                    return {
                        "success": False,
                        "error": "Parameter 'job_id' is required for get_status.",
                    }
                return await _get_sync_status(job_id, include_logs)
            elif action == "wait":
                if job_id is None:
                    return {"success": False, "error": "Parameter 'job_id' is required for wait."}
                return await _wait_for_sync_completion(job_id, timeout, poll_interval)
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown action '{action}'. Valid actions: trigger, get_status, wait"
                    ),
                }
        except Exception as e:
            logger.error("airbyte_sync(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 3: airbyte_inventory
    # ══════════════════════════════════════════════════════════════

    async def airbyte_inventory(
        list_type: Literal[
            "connectors",
            "connections",
            "connection_details",
            "sources",
            "destinations",
            "streams",
            "select_streams",
        ],
        connector_type: str = "both",
        search_term: str | None = None,
        connection_id: str | None = None,
        source_id: str | None = None,
        prompt: str | None = None,
        schemas: list[str] | None = None,
        policy: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """List Airbyte connectors, connections, sources, destinations, or streams.

        Args:
            list_type: One of:
                - "connectors"          — List available connector definitions.
                - "connections"         — List all Airbyte connections.
                - "connection_details"  — Get details for a specific connection.
                - "sources"             — List all configured sources.
                - "destinations"        — List all configured destinations.
                - "streams"             — Discover streams for a source.
                - "select_streams"      — Intent-based stream selection.
            connector_type: 'source', 'destination', or 'both' (connectors).
            search_term: Filter connectors by name (connectors).
            connection_id: Connection ID (connection_details).
            source_id: Source ID (streams, select_streams).
            prompt: Natural language intent for stream selection (select_streams).
            schemas: Filter schemas for stream selection (select_streams).
            policy: Sync policy for stream selection (select_streams).
            limit: Max streams to return (select_streams).

        Returns:
            Dictionary with inventory results.
        """
        if not isinstance(list_type, str) or not list_type.strip():
            return {"success": False, "error": "Parameter 'list_type' must be a non-empty string."}
        list_type = list_type.strip().lower()
        try:
            if list_type == "connectors":
                return await _list_airbyte_connectors(connector_type, search_term)
            elif list_type == "connections":
                return await _list_airbyte_connections()
            elif list_type == "connection_details":
                if not connection_id:
                    return {
                        "success": False,
                        "error": "Parameter 'connection_id' is required for connection_details.",
                    }
                return await _get_airbyte_connection_details(connection_id)
            elif list_type == "sources":
                return await _list_airbyte_sources()
            elif list_type == "destinations":
                return await _list_airbyte_destinations()
            elif list_type == "streams":
                if not source_id:
                    return {
                        "success": False,
                        "error": "Parameter 'source_id' is required for streams.",
                    }
                return await _list_streams(source_id)
            elif list_type == "select_streams":
                if not source_id:
                    return {
                        "success": False,
                        "error": "Parameter 'source_id' is required for select_streams.",
                    }
                if not prompt:
                    return {
                        "success": False,
                        "error": "Parameter 'prompt' is required for select_streams.",
                    }
                return await _select_streams_from_intent(source_id, prompt, schemas, policy, limit)
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown list_type '{list_type}'. "
                        "Valid types: connectors, connections, connection_details, sources, destinations, streams, select_streams"
                    ),
                }
        except Exception as e:
            logger.error("airbyte_inventory(%s) failed: %s", list_type, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 4: airbyte_manage
    # ══════════════════════════════════════════════════════════════

    async def airbyte_manage(
        action: Literal[
            "create_source",
            "create_destination",
            "delete_source",
            "delete_destination",
            "delete_connection",
            "test_api",
            "check_source",
            "check_destination",
            "get_profile_template",
        ],
        # create params
        name: str | None = None,
        source_definition_id: str | None = None,
        destination_definition_id: str | None = None,
        source_profile: str | None = None,
        destination_profile: str | None = None,
        # delete/check params
        source_id: str | None = None,
        destination_id: str | None = None,
        connection_id: str | None = None,
        confirm: bool = False,
    ) -> dict[str, Any]:
        """Create, delete, or test Airbyte sources, destinations, and connections.

        Connection: follows the server's wizard-vs-profile selection policy
        (see the server ``instructions``). For ``create_source`` /
        ``create_destination``, Rule 5 applies — a named ``connections.yaml``
        profile (via ``source_profile`` / ``destination_profile``) is
        REQUIRED; the wizard-default identity and the ``'wizard'``/``'default'``
        sentinel are rejected.

        Credentials are resolved server-side from the default Teradata connection.
        The LLM never handles passwords or API keys. Connection profiles from
        connections.yaml can optionally be used to target different systems.

        Args:
            action: One of:
                - "create_source"       — Create an Airbyte source.
                - "create_destination"  — Create an Airbyte destination.
                - "delete_source"       — Delete an Airbyte source.
                - "delete_destination"  — Delete an Airbyte destination.
                - "delete_connection"   — Delete an Airbyte connection.
                - "test_api"            — Test Airbyte API connectivity.
                - "check_source"        — Check source connection health.
                - "check_destination"   — Check destination configuration.
                - "get_profile_template" — Get a template for a connections.yaml profile.
                    For this action, pass EITHER:
                    - name="ConnectorName" (e.g., "Postgres", "Snowflake", "MySQL")
                    - source_definition_id="..." (for sources)
                    - destination_definition_id="..." (for destinations)
            name: Display name for create_source/create_destination/get_profile_template.
                For get_profile_template, extract the connector name from user request
                (e.g., if user says "create a Postgres source", pass name="Postgres").
            source_definition_id: Airbyte source definition ID (create_source, get_profile_template).
            destination_definition_id: Airbyte destination definition ID (create_destination, get_profile_template).
            source_profile: Optional profile from connections.yaml (create_source).
                If not provided, uses the default Teradata connection.
            destination_profile: Optional profile from connections.yaml (create_destination).
                If not provided, uses the default Teradata connection.
            source_id: Source ID (delete_source, check_source).
            destination_id: Destination ID (delete_destination, check_destination).
            connection_id: Connection ID (delete_connection).
            confirm: Must be True to execute delete actions. When False (default),
                returns a preview of what will be deleted.

        Returns:
            Dictionary with operation results.
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        action = action.strip().lower()
        try:
            if action == "create_source":
                if not name or not source_definition_id:
                    return {
                        "success": False,
                        "error": "Parameters 'name', 'source_definition_id' are required for create_source.",
                    }
                # Rule 5: persistent Airbyte connectors must reference a
                # named connections.yaml profile. Wizard-default identity
                # (and 'wizard'/'default' sentinel) is rejected here — see
                # the server instructions for the full rule.
                if not is_explicit_profile(source_profile):
                    return {
                        "success": False,
                        "rule": "Rule 5",
                        "missing": ["source_profile"],
                        "error": (
                            "Rule 5: creating an Airbyte source requires a "
                            "named connections.yaml profile. The wizard-"
                            "default Teradata connection is for local use "
                            "only and is rejected here. Ask the user which "
                            "profile to use, then retry with "
                            "source_profile=<name>."
                        ),
                    }
                return await _create_airbyte_source(name, source_definition_id, source_profile)
            elif action == "create_destination":
                if not name or not destination_definition_id:
                    return {
                        "success": False,
                        "error": "Parameters 'name', 'destination_definition_id' are required for create_destination.",
                    }
                # Rule 5 (mirror of create_source): the wizard-default is
                # rejected for Airbyte destinations too.
                if not is_explicit_profile(destination_profile):
                    return {
                        "success": False,
                        "rule": "Rule 5",
                        "missing": ["destination_profile"],
                        "error": (
                            "Rule 5: creating an Airbyte destination requires "
                            "a named connections.yaml profile. The wizard-"
                            "default Teradata connection is for local use "
                            "only and is rejected here. Ask the user which "
                            "profile to use, then retry with "
                            "destination_profile=<name>."
                        ),
                    }
                return await _create_airbyte_destination(
                    name, destination_definition_id, destination_profile
                )
            elif action == "delete_source":
                if not source_id:
                    return {
                        "success": False,
                        "error": "Parameter 'source_id' is required for delete_source.",
                    }
                if not confirm:
                    affected = await _get_affected_connections_for_source(source_id)
                    return {
                        "success": False,
                        "requires_confirmation": True,
                        "action": "delete_source",
                        "source_id": source_id,
                        "warning": f"This will permanently delete Airbyte source '{source_id}'.",
                        "cascade_warning": (
                            f"{len(affected)} connection(s) reference this source and will break."
                            if affected
                            else "No connections currently reference this source."
                        ),
                        "affected_connections": affected,
                        "hint": "Re-call with confirm=True to proceed.",
                    }
                return await _delete_airbyte_source(source_id)
            elif action == "delete_destination":
                if not destination_id:
                    return {
                        "success": False,
                        "error": "Parameter 'destination_id' is required for delete_destination.",
                    }
                if not confirm:
                    affected = await _get_affected_connections_for_destination(destination_id)
                    return {
                        "success": False,
                        "requires_confirmation": True,
                        "action": "delete_destination",
                        "destination_id": destination_id,
                        "warning": f"This will permanently delete Airbyte destination '{destination_id}'.",
                        "cascade_warning": (
                            f"{len(affected)} connection(s) reference this destination and will break."
                            if affected
                            else "No connections currently reference this destination."
                        ),
                        "affected_connections": affected,
                        "hint": "Re-call with confirm=True to proceed.",
                    }
                return await _delete_airbyte_destination(destination_id)
            elif action == "delete_connection":
                if not connection_id:
                    return {
                        "success": False,
                        "error": "Parameter 'connection_id' is required for delete_connection.",
                    }
                if not confirm:
                    return {
                        "success": False,
                        "requires_confirmation": True,
                        "action": "delete_connection",
                        "connection_id": connection_id,
                        "warning": f"This will permanently delete Airbyte connection '{connection_id}'. The underlying source and destination will not be affected.",
                        "hint": "Re-call with confirm=True to proceed.",
                    }
                return await _delete_airbyte_connection(connection_id)
            elif action == "test_api":
                return await _test_airbyte_connection()
            elif action == "check_source":
                if not source_id:
                    return {
                        "success": False,
                        "error": "Parameter 'source_id' is required for check_source.",
                    }
                return await _check_airbyte_source_connection(source_id)
            elif action == "check_destination":
                if not destination_id:
                    return {
                        "success": False,
                        "error": "Parameter 'destination_id' is required for check_destination.",
                    }
                return await _check_airbyte_destination_connection(destination_id)
            elif action == "get_profile_template":
                return await _get_profile_template(
                    name=name,
                    source_definition_id=source_definition_id,
                    destination_definition_id=destination_definition_id,
                )
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown action '{action}'. "
                        "Valid actions: create_source, create_destination, delete_source, "
                        "delete_destination, delete_connection, test_api, check_source, "
                        "check_destination, get_profile_template"
                    ),
                }
        except Exception as e:
            logger.error("airbyte_manage(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ══════════════════════════════════════════════════════════════
    #  Router Tool 5: airflow_teradata_load
    # ══════════════════════════════════════════════════════════════

    async def airflow_teradata_load(
        method: Literal["csv_dag", "csv_complete", "table_transfer"],
        # csv_dag params
        csv_path: str | None = None,
        target_database: Annotated[
            str | None,
            Field(
                description=(
                    "Target Teradata database (csv_dag, csv_complete, table_transfer). "
                    "For table_transfer, optional when target_teradata_profile is provided "
                    "— auto-resolved from the profile's 'database', 'schema', or "
                    "'default_schema' key (first non-empty value wins)."
                )
            ),
        ] = None,
        target_table: str | None = None,
        dag_id: str | None = None,
        teradata_conn_id: str | None = None,
        ssh_conn_id: str | None = None,
        delimiter: str | None = None,
        source_format: str = "Delimited",
        schedule: str | None = None,
        generate_validations: bool = True,
        table_prefix: str = "",
        error_limit: int = 100,
        session_count: int = 4,
        owner: str = "data_engineer",
        email: list[str] | None = None,
        tags: list[str] | None = None,
        strict_ssh: bool = True,
        teradata_profile: str | None = None,
        ssh_profile: str | None = None,
        # csv_complete extras
        deploy_to_airflow: bool = False,
        trigger_after_deploy: bool = False,
        # table_transfer params
        source_database: Annotated[
            str | None,
            Field(
                description=(
                    "Source Teradata database (table_transfer). Optional when "
                    "source_teradata_profile is provided — auto-resolved from the "
                    "profile's 'database', 'schema', or 'default_schema' key "
                    "(first non-empty value wins). Required if no profile is given "
                    "or the profile omits all three keys."
                )
            ),
        ] = None,
        source_table: str | None = None,
        source_teradata_conn_id: str = "teradata_source",
        target_teradata_conn_id: str = "teradata_target",
        source_teradata_profile: Annotated[
            str | None,
            Field(
                description=(
                    "Source Teradata connection profile from connections.yaml "
                    "(table_transfer). Supplies credentials and, when source_database "
                    "is omitted, auto-resolves it from the profile's 'database', "
                    "'schema', or 'default_schema' key (first non-empty value wins)."
                )
            ),
        ] = None,
        target_teradata_profile: Annotated[
            str | None,
            Field(
                description=(
                    "Target Teradata connection profile from connections.yaml "
                    "(table_transfer). Supplies credentials and, when target_database "
                    "is omitted, auto-resolves it from the profile's 'database', "
                    "'schema', or 'default_schema' key (first non-empty value wins)."
                )
            ),
        ] = None,
        # dbt transformation params (optional)
        # ``project_name`` selects which dbt sub-project under
        # ``<workspace>/dbt_project/dbt_<name>/`` the DAG runs against.
        # Combine with ``teradata_profile`` (or wizard-default) to identify
        # the sub-project — same resolver as the runtime dbt_* tools.
        # Omit ``project_name`` to generate the DAG without a dbt step.
        project_name: str | None = None,
        dbt_models: list[str] | None = None,
        dbt_target: str = "prod",
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
    ) -> dict[str, Any]:
        """Generate and deploy Airflow DAGs for scheduled Teradata data loading.

        Connection: follows the server's wizard-vs-profile selection policy
        (see the server ``instructions``). Rule 5 applies — Airflow DAGs
        that run against Teradata MUST cite a named ``connections.yaml``
        profile via ``teradata_profile`` (for csv_dag / csv_complete) or
        ``source_teradata_profile`` AND ``target_teradata_profile`` (for
        table_transfer). The wizard-default identity and the
        ``'wizard'``/``'default'`` sentinel are rejected. ``ssh_profile``
        (for remote TTU / SFTP) follows the standard wizard-default behaviour.

        ELT Pipeline Workflow — Sequential Prompts Required:
          This tool generates Airflow DAGs for Teradata data loading (CSV or table transfer).
          After generating the DAG, the user must separately:
          1. Deploy to Airflow: pipeline_deploy(action='deploy_dags', ...)
          2. Trigger execution: dag_trigger(mode='run', dag_id='...', ...)
          3. Generate dbt models: dbt_generate_model(model_type='staging', ...)
          4. Execute dbt: dbt_execute(command='run', models=[...])
          Each step should be a separate user prompt.

        Use this tool ONLY when the user explicitly asks to create an Airflow DAG,
        deploy a pipeline to Airflow, or set up a scheduled/recurring data load
        through Airflow orchestration.

        Keywords that indicate this tool: "Airflow DAG", "schedule", "deploy to Airflow",
        "recurring pipeline", "cron", "orchestrate with Airflow".

        For direct/immediate/one-off data loading into Teradata (no Airflow),
        use the ttu_execute tool instead — it runs tdload/tbuild/bteq locally.

        Args:
            method: One of:
                - "csv_dag"        — Generate Airflow DAG for CSV-to-Teradata loading.
                - "table_transfer" — Generate Airflow DAG for Teradata table-to-table transfer.
                - "csv_complete"   — Complete workflow: generate DAG + optional deploy + trigger.
            csv_path: Path to CSV file (csv_dag, csv_complete).
            target_database: Target Teradata database (csv_dag, csv_complete, table_transfer).
                For table_transfer, optional when target_teradata_profile is provided —
                auto-resolved from the profile's 'database', 'schema', or 'default_schema'
                key (first non-empty value wins). Required for csv_dag/csv_complete and for
                table_transfer when no profile is given or the profile omits all three keys.
            target_table: Target table name (csv_dag, csv_complete, table_transfer).
            dag_id: DAG identifier (csv_dag, table_transfer).
            teradata_conn_id: Airflow Teradata connection ID (csv_dag, csv_complete).
            ssh_conn_id: Airflow SSH connection ID.
            delimiter: Delimiter character (csv_dag, csv_complete).
            source_format: Source file format (csv_dag, default 'Delimited').
            schedule: Cron expression or preset.
            generate_validations: Generate validation tasks (default True).
            table_prefix: Prefix for auto-generated table name (csv_dag).
            error_limit: Max errors before failure (default 100).
            session_count: Parallel loading sessions (default 4).
            owner: DAG owner (default 'data_engineer').
            email: Alert email addresses.
            tags: DAG tags.
            strict_ssh: Strict SSH host key checking (csv_dag, default True).
            teradata_profile: Teradata profile from connections.yaml (csv_dag).
            ssh_profile: SSH profile from connections.yaml for SSH connection credentials.
            deploy_to_airflow: Deploy after generation (csv_complete, default False).
            trigger_after_deploy: Trigger after deploy (csv_complete, default False).
            source_database: Source Teradata database (table_transfer). Optional when
                source_teradata_profile is provided — auto-resolved from the profile's
                'database', 'schema', or 'default_schema' key (first non-empty value wins).
                Required if no profile is given or the profile omits all three keys.
            source_table: Source table name (table_transfer).
            source_teradata_conn_id: Source Teradata connection ID (table_transfer).
            target_teradata_conn_id: Target Teradata connection ID (table_transfer).
            source_teradata_profile: Source Teradata connection profile name from
                connections.yaml (table_transfer). Supplies credentials and, when
                source_database is omitted, auto-resolves the database via the profile's
                'database', 'schema', or 'default_schema' key (first non-empty value wins).
            target_teradata_profile: Target Teradata connection profile name from
                connections.yaml (table_transfer). Supplies credentials and, when
                target_database is omitted, auto-resolves the database via the profile's
                'database', 'schema', or 'default_schema' key (first non-empty value wins).
            project_name: Name of the dbt sub-project to run after loading
                (e.g. ``analytics`` resolves to
                ``<workspace>/dbt_project/dbt_analytics/``). When set, the
                generated DAG includes dbt transformation tasks. The
                sub-project must already exist (scaffold it first via
                ``dbt_project(action='create_structure')`` or
                ``dbt_generate_model``). Omit to generate a load-only DAG
                with no dbt step.
            dbt_models: Specific dbt models to run (default: all).
            dbt_target: dbt target profile (default: 'prod').
            run_dbt_tests: Run dbt tests after models (default: True).
            generate_dbt_docs: Generate dbt docs (default: False).

        Returns:
            Dictionary with DAG generation and optional deployment results.
        """
        if not isinstance(method, str) or not method.strip():
            return {"success": False, "error": "Parameter 'method' must be a non-empty string."}
        if error_limit < 1:
            return {"success": False, "error": "Parameter 'error_limit' must be >= 1."}
        if session_count < 1:
            return {"success": False, "error": "Parameter 'session_count' must be >= 1."}
        if schedule:
            valid, sched_err = PipelineValidator.validate_schedule(schedule)
            if not valid:
                return {"success": False, "error": f"Invalid schedule: {sched_err}"}
        method = method.strip().lower()
        try:
            if method == "csv_dag":
                # Rule 5: persistent Airflow DAGs that run against Teradata
                # MUST cite a named connections.yaml profile. Wizard-default
                # (and 'wizard'/'default' sentinel) is rejected here.
                if not is_explicit_profile(teradata_profile):
                    return {
                        "success": False,
                        "rule": "Rule 5",
                        "missing": ["teradata_profile"],
                        "error": (
                            "Rule 5: airflow_teradata_load(method='csv_dag') "
                            "requires a named connections.yaml profile. The "
                            "wizard-default Teradata connection is for local "
                            "use only and is rejected here. Ask the user "
                            "which profile to use, then retry with "
                            "teradata_profile=<name>."
                        ),
                    }
                # Resolve the optional dbt sub-project before generation so
                # the DAG references a real ``dbt_<name>/`` path (and
                # action_required responses surface to the LLM rather than
                # being baked into a scheduled DAG).
                resolved_dbt_path = _maybe_resolve_dbt_path(
                    orchestrator,
                    project_name,
                    teradata_profile,
                    profile_param_name="teradata_profile",
                )
                if isinstance(resolved_dbt_path, dict):
                    return resolved_dbt_path
                return await _generate_airflow_tdload_dag_from_csv(
                    csv_path=csv_path,
                    target_database=target_database,
                    target_table=target_table,
                    dag_id=dag_id,
                    teradata_conn_id=teradata_conn_id,
                    ssh_conn_id=ssh_conn_id,
                    delimiter=delimiter,
                    source_format=source_format,
                    schedule=schedule,
                    generate_validations=generate_validations,
                    table_prefix=table_prefix,
                    error_limit=error_limit,
                    session_count=session_count,
                    owner=owner,
                    email=email,
                    tags=tags,
                    strict_ssh=strict_ssh,
                    teradata_profile=teradata_profile,
                    ssh_profile=ssh_profile,
                    dbt_project_dir=resolved_dbt_path,
                    dbt_models=dbt_models,
                    dbt_target=dbt_target,
                    run_dbt_tests=run_dbt_tests,
                    generate_dbt_docs=generate_dbt_docs,
                )
            elif method == "table_transfer":
                # Rule 5: cross-instance table transfer requires named
                # profiles for BOTH source and target. Wizard-default and
                # the 'wizard'/'default' sentinel are rejected here.
                missing: list[str] = []
                if not is_explicit_profile(source_teradata_profile):
                    missing.append("source_teradata_profile")
                if not is_explicit_profile(target_teradata_profile):
                    missing.append("target_teradata_profile")
                if missing:
                    return {
                        "success": False,
                        "rule": "Rule 5",
                        "missing": missing,
                        "error": (
                            "Rule 5: airflow_teradata_load(method='table_transfer') "
                            "requires named connections.yaml profiles for BOTH "
                            "source and target. The wizard-default Teradata "
                            "connection is for local use only and is rejected "
                            "here. Missing: " + ", ".join(missing) + ". Ask "
                            "the user which profile to use for each side, "
                            "then retry with both parameters set."
                        ),
                    }

                def _db_from_profile(profile: dict) -> str | None:
                    """Return the first non-blank database name from a resolved profile.

                    Tries the keys 'database', 'schema', and 'default_schema' in order.
                    Values are coerced to str before stripping so non-string truthy values
                    (e.g. an int parsed from YAML) do not raise AttributeError.
                    """
                    for key in ("database", "schema", "default_schema"):
                        val = str(profile.get(key) or "").strip()
                        if val:
                            return val
                    return None

                # Normalize caller-supplied values first so whitespace-only strings are
                # treated as absent — this allows profile auto-resolution to fire even
                # when the caller passes a whitespace-only database/table name.
                # str() coercion mirrors _db_from_profile and guards against non-string
                # types (e.g. int) that JSON tool arguments may deliver at runtime.
                source_database = str(source_database or "").strip() or None
                source_table = str(source_table or "").strip() or None
                target_database = str(target_database or "").strip() or None
                target_table = str(target_table or "").strip() or None

                # Auto-resolve source_database from profile when not explicitly provided
                if not source_database and source_teradata_profile:
                    guard = orchestrator.credential_resolver.guard_configured()
                    if guard:
                        return guard
                    source_database = _db_from_profile(
                        orchestrator.credential_resolver.resolve_profile(source_teradata_profile)
                    )

                # Auto-resolve target_database from profile when not explicitly provided
                if not target_database and target_teradata_profile:
                    guard = orchestrator.credential_resolver.guard_configured()
                    if guard:
                        return guard
                    target_database = _db_from_profile(
                        orchestrator.credential_resolver.resolve_profile(target_teradata_profile)
                    )

                if not source_database:
                    return {
                        "success": False,
                        "error": (
                            "Parameter 'source_database' is required for table_transfer. "
                            "Provide it explicitly or add a 'database', 'schema', or "
                            "'default_schema' field to the "
                            f"'{source_teradata_profile}' connection profile."
                            if source_teradata_profile
                            else "Parameter 'source_database' is required for table_transfer."
                        ),
                    }
                if not source_table:
                    return {
                        "success": False,
                        "error": "Parameter 'source_table' is required for table_transfer.",
                    }
                if not target_database:
                    return {
                        "success": False,
                        "error": (
                            "Parameter 'target_database' is required for table_transfer. "
                            "Provide it explicitly or add a 'database', 'schema', or "
                            "'default_schema' field to the "
                            f"'{target_teradata_profile}' connection profile."
                            if target_teradata_profile
                            else "Parameter 'target_database' is required for table_transfer."
                        ),
                    }
                if not target_table:
                    return {
                        "success": False,
                        "error": "Parameter 'target_table' is required for table_transfer.",
                    }
                # Resolve the optional dbt sub-project. The dbt step (if
                # present) runs against the TARGET Teradata, so resolve
                # using ``target_teradata_profile`` for the identity.
                # ``profile_param_name`` is the user-facing knob name —
                # mismatch errors must direct the user to the right
                # parameter for this method.
                resolved_dbt_path = _maybe_resolve_dbt_path(
                    orchestrator,
                    project_name,
                    target_teradata_profile,
                    profile_param_name="target_teradata_profile",
                )
                if isinstance(resolved_dbt_path, dict):
                    return resolved_dbt_path
                return await _generate_airflow_tdload_table_transfer_dag(
                    source_database=source_database,
                    source_table=source_table,
                    target_database=target_database,
                    target_table=target_table,
                    dag_id=dag_id,
                    source_teradata_conn_id=source_teradata_conn_id,
                    target_teradata_conn_id=target_teradata_conn_id,
                    ssh_conn_id=ssh_conn_id,
                    schedule=schedule,
                    generate_validations=generate_validations,
                    error_limit=error_limit,
                    session_count=session_count,
                    owner=owner,
                    email=email,
                    tags=tags,
                    source_teradata_profile=source_teradata_profile,
                    target_teradata_profile=target_teradata_profile,
                    ssh_profile=ssh_profile,
                    strict_ssh=strict_ssh,
                    dbt_project_dir=resolved_dbt_path,
                    dbt_models=dbt_models,
                    dbt_target=dbt_target,
                    run_dbt_tests=run_dbt_tests,
                    generate_dbt_docs=generate_dbt_docs,
                )
            elif method == "csv_complete":
                if not csv_path:
                    return {
                        "success": False,
                        "error": "Parameter 'csv_path' is required for csv_complete.",
                    }
                if not target_database or not target_table:
                    return {
                        "success": False,
                        "error": "Parameters 'target_database' and 'target_table' are required for csv_complete.",
                    }
                # Rule 5: csv_complete is csv_dag + deploy + trigger; the
                # generated DAG runs against Teradata, so it must cite a
                # named profile.
                if not is_explicit_profile(teradata_profile):
                    return {
                        "success": False,
                        "rule": "Rule 5",
                        "missing": ["teradata_profile"],
                        "error": (
                            "Rule 5: airflow_teradata_load(method='csv_complete') "
                            "requires a named connections.yaml profile. The "
                            "wizard-default Teradata connection is for local "
                            "use only and is rejected here. Ask the user "
                            "which profile to use, then retry with "
                            "teradata_profile=<name>."
                        ),
                    }
                # Resolve the optional dbt sub-project for csv_complete.
                resolved_dbt_path = _maybe_resolve_dbt_path(
                    orchestrator,
                    project_name,
                    teradata_profile,
                    profile_param_name="teradata_profile",
                )
                if isinstance(resolved_dbt_path, dict):
                    return resolved_dbt_path
                return await _load_csv_to_teradata_complete(
                    csv_path=csv_path,
                    target_database=target_database,
                    target_table=target_table,
                    delimiter=delimiter,
                    teradata_conn_id=teradata_conn_id,
                    ssh_conn_id=ssh_conn_id,
                    schedule=schedule,
                    generate_validations=generate_validations,
                    error_limit=error_limit,
                    session_count=session_count,
                    owner=owner,
                    email=email,
                    tags=tags,
                    deploy_to_airflow=deploy_to_airflow,
                    trigger_after_deploy=trigger_after_deploy,
                    teradata_profile=teradata_profile,
                    ssh_profile=ssh_profile,
                    dbt_project_dir=resolved_dbt_path,
                    dbt_models=dbt_models,
                    dbt_target=dbt_target,
                    run_dbt_tests=run_dbt_tests,
                    generate_dbt_docs=generate_dbt_docs,
                )
            else:
                return {
                    "success": False,
                    "error": (
                        f"Unknown method '{method}'. "
                        "Valid methods: csv_dag, table_transfer, csv_complete"
                    ),
                }
        except Exception as e:
            logger.error("airflow_teradata_load(%s) failed: %s", method, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    # ── Return router tools ────────────────────────────────────────
    return {
        "airbyte_pipeline": airbyte_pipeline,
        "airbyte_sync": airbyte_sync,
        "airbyte_inventory": airbyte_inventory,
        "airbyte_manage": airbyte_manage,
        "airflow_teradata_load": airflow_teradata_load,
    }
