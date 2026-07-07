"""Tests for the FastMCP middleware: alias rewrite + literal-enum error enrichment."""

from __future__ import annotations

import inspect
from typing import Any, Literal
from unittest.mock import AsyncMock, Mock

import pytest
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from mcp.types import CallToolRequestParams, TextContent
from pydantic import TypeAdapter, ValidationError

from teradata_etl_mcp_server.server_middleware import (
    TOOL_PARAM_ALIASES,
    ParamAliasingAndEnumErrorEnrichmentMiddleware,
    _format_validation_error,
    _normalize_aliases,
)


def _make_context(tool_name: str, arguments: dict[str, Any]) -> Mock:
    """Build a minimal MiddlewareContext mock with a CallToolRequestParams.

    The middleware only reads ``context.message.name`` and
    ``context.message.arguments`` and writes back to
    ``context.message.arguments``.
    """
    ctx = Mock()
    ctx.message = CallToolRequestParams(name=tool_name, arguments=dict(arguments))
    return ctx


# ---------------------------------------------------------------------------
# _normalize_aliases (pure)
# ---------------------------------------------------------------------------


class TestNormalizeAliases:
    def test_alias_rewritten_when_only_alias_present(self):
        out = _normalize_aliases("ttu_execute", {"query": "SELECT 1"})
        assert out == {"sql": "SELECT 1"}

    def test_canonical_passthrough_unchanged(self):
        out = _normalize_aliases("ttu_execute", {"sql": "SELECT 1"})
        assert out == {"sql": "SELECT 1"}

    def test_unaliased_tool_passes_through(self):
        out = _normalize_aliases("connection_profiles", {"action": "list"})
        assert out == {"action": "list"}

    def test_alias_collision_raises_value_error(self):
        with pytest.raises(ValueError, match="received both 'query' .* and 'sql'"):
            _normalize_aliases("ttu_execute", {"query": "A", "sql": "B"})

    def test_alias_with_explicit_none_canonical_takes_effect(self):
        """Canonical=None is treated as 'not supplied'; alias wins."""
        out = _normalize_aliases("ttu_execute", {"sql": None, "query": "X"})
        assert out == {"sql": "X"}

    def test_alias_only_when_canonical_missing_uses_alias(self):
        out = _normalize_aliases(
            "dbt_generate_model", {"tables": ["customers", "orders"]}
        )
        assert out == {"source_tables": ["customers", "orders"]}


# ---------------------------------------------------------------------------
# _format_validation_error (pure)
# ---------------------------------------------------------------------------


class TestFormatValidationError:
    def test_literal_error_message_lists_allowed_values(self):
        adapter = TypeAdapter(Literal["foo", "bar", "baz"])
        try:
            adapter.validate_python("invalid")
        except ValidationError as e:
            msg = _format_validation_error(e)
        assert "Allowed values:" in msg
        assert "'foo'" in msg
        assert "'bar'" in msg
        assert "'baz'" in msg

    def test_non_literal_error_includes_field_name_no_enum_text(self):
        from pydantic import BaseModel

        class M(BaseModel):
            x: int

        try:
            M(x="not a number")
        except ValidationError as e:
            msg = _format_validation_error(e)
        assert "x:" in msg
        assert "Allowed values:" not in msg

    def test_multi_error_messages_joined(self):
        from pydantic import BaseModel

        class M(BaseModel):
            x: int
            y: Literal["a", "b"]

        try:
            M(x="bad", y="c")
        except ValidationError as e:
            msg = _format_validation_error(e)
        assert "x:" in msg
        assert "Allowed values:" in msg  # for y
        assert "; " in msg  # multi-error separator


# ---------------------------------------------------------------------------
# Middleware integration (alias rewrite phase + error enrichment phase)
# ---------------------------------------------------------------------------


class TestMiddleware:
    @pytest.fixture
    def middleware(self) -> ParamAliasingAndEnumErrorEnrichmentMiddleware:
        return ParamAliasingAndEnumErrorEnrichmentMiddleware()

    @pytest.mark.asyncio
    async def test_alias_rewritten_before_call_next(self, middleware):
        """The wrapped tool sees the canonical kwarg, never the alias."""
        seen_args: dict[str, Any] = {}

        async def call_next(ctx):
            seen_args.update(ctx.message.arguments)
            return ToolResult(content=[TextContent(type="text", text="ok")])

        ctx = _make_context("ttu_execute", {"action": "execute_sql", "query": "SELECT 1"})
        await middleware.on_call_tool(ctx, call_next)
        assert "sql" in seen_args
        assert seen_args["sql"] == "SELECT 1"
        assert "query" not in seen_args

    @pytest.mark.asyncio
    async def test_alias_collision_raises_tool_error(self, middleware):
        """Conflict between alias and canonical surfaces as ToolError."""
        call_next = AsyncMock(
            return_value=ToolResult(content=[TextContent(type="text", text="ok")])
        )
        ctx = _make_context(
            "ttu_execute",
            {"action": "execute_sql", "sql": "A", "query": "B"},
        )
        with pytest.raises(ToolError, match="received both 'query' .* and 'sql'"):
            await middleware.on_call_tool(ctx, call_next)
        # call_next must NOT have been invoked.
        call_next.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_canonical_only_call_passes_through(self, middleware):
        """No alias → no rewrite, no error, call_next sees args unchanged."""
        seen_args: dict[str, Any] = {}

        async def call_next(ctx):
            seen_args.update(ctx.message.arguments)
            return ToolResult(content=[TextContent(type="text", text="ok")])

        ctx = _make_context("ttu_execute", {"action": "execute_sql", "sql": "SELECT 1"})
        result = await middleware.on_call_tool(ctx, call_next)
        assert seen_args == {"action": "execute_sql", "sql": "SELECT 1"}
        assert isinstance(result, ToolResult)

    @pytest.mark.asyncio
    async def test_unaliased_tool_pass_through(self, middleware):
        """Tool not in TOOL_PARAM_ALIASES is unaffected."""
        seen_args: dict[str, Any] = {}

        async def call_next(ctx):
            seen_args.update(ctx.message.arguments)
            return ToolResult(content=[TextContent(type="text", text="ok")])

        ctx = _make_context("connection_profiles", {"action": "list"})
        await middleware.on_call_tool(ctx, call_next)
        assert seen_args == {"action": "list"}

    @pytest.mark.asyncio
    async def test_validation_error_enriched_to_tool_error_with_allowed_values(
        self, middleware
    ):
        """A ValidationError raised downstream is caught and re-raised as
        a ToolError whose message lists the allowed enum values."""
        adapter = TypeAdapter(Literal["foo", "bar"])

        async def call_next(ctx):
            adapter.validate_python("invalid")  # raises ValidationError

        ctx = _make_context("dbt_project", {"action": "invalid"})
        with pytest.raises(ToolError) as excinfo:
            await middleware.on_call_tool(ctx, call_next)
        msg = str(excinfo.value)
        assert "Allowed values:" in msg
        assert "'foo'" in msg
        assert "'bar'" in msg

    @pytest.mark.asyncio
    async def test_non_validation_exception_propagates_unchanged(self, middleware):
        """Non-ValidationError exceptions pass through; the middleware
        only handles validation errors."""

        async def call_next(ctx):
            raise RuntimeError("downstream blew up")

        ctx = _make_context("ttu_execute", {"sql": "SELECT 1"})
        with pytest.raises(RuntimeError, match="downstream blew up"):
            await middleware.on_call_tool(ctx, call_next)


# ---------------------------------------------------------------------------
# Sanity check: every aliased canonical name is a real kwarg on the tool
# ---------------------------------------------------------------------------


class TestAliasTableConsistency:
    """Verify TOOL_PARAM_ALIASES doesn't have typos: every canonical
    target must be an actual kwarg on the registered tool."""

    @staticmethod
    def _build_tool_kwargs() -> dict[str, set[str]]:
        """Register every tool and collect its kwargs by tool name.

        Returns ``{tool_name: {kwarg_name, ...}, ...}``.
        """
        from unittest.mock import MagicMock

        from teradata_etl_mcp_server.tools.airflow_pipeline_management import (
            register_pipeline_tools,
        )
        from teradata_etl_mcp_server.tools.connection_profiles import (
            register_connection_profile_tools,
        )
        from teradata_etl_mcp_server.tools.data_movement import register_data_movement_tools
        from teradata_etl_mcp_server.tools.dbt_management import register_dbt_tools
        from teradata_etl_mcp_server.tools.metadata_discovery import register_metadata_tools
        from teradata_etl_mcp_server.tools.orchestration_execution import (
            register_orchestration_tools,
        )
        from teradata_etl_mcp_server.tools.ttu_tools import register_ttu_tools

        orch = MagicMock()
        # The registration functions only use the orchestrator as a
        # closure target; they don't touch attributes during registration.
        registries = {}
        for register in (
            register_pipeline_tools,
            register_connection_profile_tools,
            register_data_movement_tools,
            register_dbt_tools,
            register_metadata_tools,
            register_orchestration_tools,
            register_ttu_tools,
        ):
            registries.update(register(orch))

        out: dict[str, set[str]] = {}
        for tool_name, fn in registries.items():
            try:
                sig = inspect.signature(fn)
            except (TypeError, ValueError):
                continue
            out[tool_name] = set(sig.parameters)
        return out

    def test_every_canonical_target_is_real_kwarg(self):
        kwargs_by_tool = self._build_tool_kwargs()
        problems: list[str] = []
        for tool_name, alias_map in TOOL_PARAM_ALIASES.items():
            if tool_name not in kwargs_by_tool:
                problems.append(
                    f"TOOL_PARAM_ALIASES references unknown tool '{tool_name}'"
                )
                continue
            available = kwargs_by_tool[tool_name]
            for alias, canonical in alias_map.items():
                if canonical not in available:
                    problems.append(
                        f"{tool_name}: alias '{alias}' → canonical '{canonical}' "
                        f"but '{canonical}' is not a kwarg (available: "
                        f"{sorted(available)})"
                    )
        assert not problems, "\n".join(problems)
