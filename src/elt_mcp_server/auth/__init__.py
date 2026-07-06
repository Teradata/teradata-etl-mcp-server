"""Auth primitives for the MCP server.

The single source of truth for "who logs on to Teradata and how" is
:class:`TeradataAuth`. Every consumer — tdload, bteq, teradatasql, dbt
today; Airflow Connections and Airbyte connector configs in the future —
renders the same ``TeradataAuth`` into its own wire format through a
dedicated method on the class. No client, tool, or generator reaches for
individual ``logmech`` / ``logdata`` / ``oidc_clientid`` fields.

Typical flow::

    auth = resolve_teradata_auth(
        settings=orchestrator.settings.teradata,
        credential_resolver=orchestrator.credential_resolver,
        teradata_profile=teradata_profile,   # None → wizard default
    )
    client.execute_tdload(..., auth=auth)
"""

from .resolver import (
    build_teradata_auth_from_profile,
    build_teradata_auth_from_settings,
    is_explicit_profile,
    resolve_teradata_auth,
)
from .teradata_auth import AuthUnsupportedError, TdloadRendering, TeradataAuth

__all__ = [
    "AuthUnsupportedError",
    "TdloadRendering",
    "TeradataAuth",
    "build_teradata_auth_from_profile",
    "build_teradata_auth_from_settings",
    "is_explicit_profile",
    "resolve_teradata_auth",
]
