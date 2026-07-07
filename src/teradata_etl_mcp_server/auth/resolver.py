"""Compose :class:`TeradataAuth` from the server's configuration sources.

Precedence (definitive):

1. **If the caller names a ``teradata_profile``**: the profile from
   ``connections.yaml`` wins *entirely*. Mechanism AND its associated fields
   (logdata, oidc_clientid, jws_*, sslca) come from the profile as one
   coherent identity. No field-level mixing with Settings.

2. **Otherwise**: the wizard-populated :class:`TeradataSettings` (the
   server's default identity) is used.

Calling code does not consume profile dicts directly — it always goes
through :func:`resolve_teradata_auth`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .teradata_auth import TeradataAuth

if TYPE_CHECKING:
    from ..config import TeradataSettings
    from ..credential_resolver import CredentialResolver


# Sentinel profile names that explicitly confirm the wizard/Settings-default
# connection. The LLM passes one of these through ``teradata_profile`` /
# ``target_profile`` when the user has confirmed the wizard default (vs. simply
# omitting the parameter, which is silence). See Rule 4 in the server
# ``instructions`` — ``ttu_execute(mode="table_to_table")`` gates on "both
# sides explicitly confirmed" and treats ``None`` as "not confirmed".
_WIZARD_SENTINELS: frozenset[str] = frozenset({"wizard", "default", ""})


def _normalize_profile_name(name: str | None) -> str | None:
    """Fold wizard/default sentinels to ``None``.

    ``None`` and empty/whitespace-only strings pass through as ``None`` so the
    precedence gate in :func:`resolve_teradata_auth` takes the Settings-default
    branch. Case-insensitive; surrounding whitespace is stripped. Non-sentinel
    names pass through unchanged (preserving case — profile names are
    case-sensitive in ``connections.yaml``).

    Reserving ``"wizard"`` and ``"default"`` means a profile literally named
    either of those strings in ``connections.yaml`` is unreachable by name.
    Documented in the connection-selection policy.
    """
    if name is None:
        return None
    raw = name.strip()
    if raw.lower() in _WIZARD_SENTINELS:
        return None
    return raw


def is_explicit_profile(name: str | None) -> bool:
    """Return ``True`` iff the caller named a real profile.

    A real profile is anything that is not ``None``, not an empty/whitespace
    string, and not the ``"wizard"`` / ``"default"`` sentinel (case-insensitive).

    Used by tools that enforce **Rule 5** — "this asset must cite a named
    `connections.yaml` profile" (Airbyte connector creation, Airflow DAG
    generation that runs against Teradata). The wizard sentinel is rejected
    here because the whole point of Rule 5 is to disallow baking the
    wizard-default identity into a deployed asset.

    Implemented on top of :func:`_normalize_profile_name` so the sentinel
    list stays in one place.
    """
    return _normalize_profile_name(name) is not None


def _secret(value: Any) -> str:
    """Unwrap a pydantic ``SecretStr`` or return the value as a string."""
    if value is None:
        return ""
    get_secret = getattr(value, "get_secret_value", None)
    if callable(get_secret):
        return get_secret() or ""
    return str(value)


def build_teradata_auth_from_settings(settings: TeradataSettings) -> TeradataAuth:
    """Compose a :class:`TeradataAuth` from wizard-populated Settings.

    This is the **default** identity — used whenever no ``teradata_profile``
    is named. The client factory builds this once at startup and stashes it
    on the orchestrator, but re-calling this function reflects any
    out-of-band updates to ``.env`` (e.g. a rerun of the setup wizard for a
    fresh JWT token).
    """
    mechanism = (settings.logmech or "TD2").upper()
    return TeradataAuth(
        host=settings.host,
        port=int(settings.port),
        database=settings.database or "",
        mechanism=mechanism,  # type: ignore[arg-type]  # runtime-validated in __post_init__
        username=settings.username or "",
        password=_secret(settings.password),
        logdata=_secret(settings.logdata),
        oidc_clientid=settings.oidc_clientid or "",
        jws_private_key=settings.jws_private_key or "",
        jws_cert=settings.jws_cert or "",
        sslca=settings.sslca or "",
    )


def _profile_mechanism(profile: dict[str, Any]) -> str:
    """Extract the mechanism from a profile dict, defaulting to TD2.

    Accepts either ``mechanism`` or ``logmech`` as the key since
    ``connections.yaml`` profiles use ``logmech`` historically and pydantic
    projections may use ``mechanism``. Case-insensitive.
    """
    raw = profile.get("mechanism") or profile.get("logmech") or "TD2"
    return str(raw).upper()


def build_teradata_auth_from_profile(profile: dict[str, Any]) -> TeradataAuth:
    """Compose a :class:`TeradataAuth` from a ``connections.yaml`` profile dict.

    The profile is expected to be the output of
    :meth:`CredentialResolver.resolve_profile` — a flat dict with any of
    ``host``, ``port``, ``database``/``schema``, ``username``, ``password``,
    ``logmech``/``mechanism``, ``logdata``, ``oidc_clientid``,
    ``jws_private_key``, ``jws_cert``, ``sslca``.

    The profile owns the entire auth identity for this call — no fallback
    into Settings for mechanism-specific fields. Missing required fields
    for the declared mechanism raise ``ValueError`` through
    :class:`TeradataAuth`'s validation.
    """
    mechanism = _profile_mechanism(profile)
    database = (
        profile.get("database")
        or profile.get("schema")
        or profile.get("default_schema")
        or ""
    )
    return TeradataAuth(
        host=str(profile.get("host", "")),
        port=int(profile.get("port", 1025) or 1025),
        database=str(database),
        mechanism=mechanism,  # type: ignore[arg-type]
        username=str(profile.get("username", "") or ""),
        password=str(profile.get("password", "") or ""),
        logdata=str(profile.get("logdata", "") or ""),
        oidc_clientid=str(profile.get("oidc_clientid", "") or ""),
        jws_private_key=str(profile.get("jws_private_key", "") or ""),
        jws_cert=str(profile.get("jws_cert", "") or ""),
        sslca=str(profile.get("sslca", "") or ""),
    )


def resolve_teradata_auth(
    settings: TeradataSettings,
    credential_resolver: CredentialResolver,
    teradata_profile: str | None,
) -> TeradataAuth:
    """The canonical precedence gate.

    If ``teradata_profile`` is a non-empty string, resolve it through
    ``credential_resolver`` and return a profile-derived
    :class:`TeradataAuth`. Otherwise return the Settings-derived default.

    Any error from the resolver (unknown profile name, missing
    connections.yaml, YAML parse error) propagates — the tool layer
    surfaces it as ``{"success": False, "error": ...}``.

    The sentinel strings ``"wizard"`` / ``"default"`` (case-insensitive,
    trimmed) fold to the Settings-default branch so the LLM can record an
    explicit "use wizard" confirmation without naming a real profile.
    """
    teradata_profile = _normalize_profile_name(teradata_profile)
    if teradata_profile:
        profile = credential_resolver.resolve_profile(teradata_profile)
        return build_teradata_auth_from_profile(profile)
    return build_teradata_auth_from_settings(settings)
