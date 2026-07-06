"""Connection profile tools for LLM discovery.

Provides MCP tools that let the LLM see which connection profiles are
available (names and descriptions only) without ever exposing credentials.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from ..response_sanitizer import safe_error_message, sanitize_response

logger = logging.getLogger(__name__)


def register_connection_profile_tools(
    orchestrator: Any,
) -> dict[str, Callable[..., Awaitable[dict[str, Any]]]]:
    """Register connection-profile MCP tools.

    Returns a dict of ``{tool_name: async_callable}`` following the same
    pattern used by all other tool modules in this package.
    """

    # ── private helpers ────────────────────────────────────────────

    async def _list(orchestrator_ref: Any) -> dict[str, Any]:
        resolver = orchestrator_ref.credential_resolver
        guard = resolver.guard_configured()
        if guard:
            guard["profiles"] = []
            return guard
        profiles = resolver.list_profiles()
        return {
            "success": True,
            "profiles": [{"name": p.name, "description": p.description} for p in profiles],
            "total": len(profiles),
            "note": (
                "Use profile names when creating pipelines or connections. "
                "Credentials are resolved server-side and never exposed."
            ),
        }

    async def _reload(orchestrator_ref: Any) -> dict[str, Any]:
        resolver = orchestrator_ref.credential_resolver
        await asyncio.to_thread(resolver.reload)
        guard = resolver.guard_configured()
        if guard:
            guard["profiles_loaded"] = 0
            return guard
        profiles = resolver.list_profiles()
        return {
            "success": True,
            "profiles_loaded": len(profiles),
            "message": "Connection profiles reloaded successfully.",
        }

    # ── router tool ────────────────────────────────────────────────

    async def connection_profiles(
        action: Literal["list", "reload"],
    ) -> dict[str, Any]:
        """Manage connection profiles from connections.yaml.

        IMPORTANT: You do NOT need profiles for normal operations. The server
        already has a default Teradata connection configured via the Setup
        Wizard or .env file. Just use ttu_execute or teradata_discover
        directly — no profile needed.

        Only use this tool when the user explicitly asks about profiles,
        connections.yaml, or wants to work with a specific named environment
        (e.g., "use the prod profile", "list my connection profiles").

        Args:
            action: One of:
                - "list"   — List available profiles (names + descriptions only).
                - "reload" — Reload profiles from connections.yaml after edits.

        Returns:
            Dictionary with profile information or reload status.
        """
        if not isinstance(action, str) or not action.strip():
            return {"success": False, "error": "Parameter 'action' must be a non-empty string."}
        action = action.strip().lower()
        try:
            if action == "list":
                return sanitize_response(await _list(orchestrator))
            elif action == "reload":
                return sanitize_response(await _reload(orchestrator))
            else:
                return {
                    "success": False,
                    "error": f"Unknown action '{action}'. Valid actions: list, reload",
                }
        except Exception as e:
            logger.error("connection_profiles(%s) failed: %s", action, e, exc_info=True)
            return {"success": False, "error": safe_error_message(e)}

    return {
        "connection_profiles": connection_profiles,
    }
