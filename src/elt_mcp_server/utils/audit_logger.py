"""Structured audit logging for MCP tool invocations."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path


class AuditLogger:
    """Lightweight structured audit logger that writes JSON Lines entries."""

    def __init__(self, log_file: Path, enabled: bool = False):
        self._enabled = enabled
        self._logger: logging.Logger | None = None
        if enabled:
            self._logger = logging.getLogger("elt_mcp_server.audit")
            self._logger.propagate = False
            already_attached = any(
                isinstance(h, logging.FileHandler)
                and Path(getattr(h, "baseFilename", "")).resolve() == Path(log_file).resolve()
                for h in self._logger.handlers
            )
            if not already_attached:
                handler = logging.FileHandler(log_file, encoding="utf-8")
                handler.setFormatter(logging.Formatter("%(message)s"))
                self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)

    def close(self) -> None:
        """Close and remove any file handlers attached to this audit logger."""
        if not self._enabled or not self._logger:
            return
        for handler in list(self._logger.handlers):
            if isinstance(handler, logging.FileHandler):
                handler.close()
                self._logger.removeHandler(handler)

    def log_tool_call(
        self,
        tool_name: str,
        action: str | None,
        params: dict,
        result_success: bool,
        duration_ms: float,
    ) -> None:
        """Log a tool invocation as a single JSON Lines entry.

        Only parameter *keys* are recorded — values are never written
        to avoid leaking credentials or PII.
        """
        if not self._enabled or not self._logger:
            return
        entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "event": "tool_call",
            "tool": tool_name,
            "action": action,
            "params_keys": sorted(params.keys()),
            "success": result_success,
            "duration_ms": round(duration_ms, 2),
        }
        self._logger.info(json.dumps(entry))
