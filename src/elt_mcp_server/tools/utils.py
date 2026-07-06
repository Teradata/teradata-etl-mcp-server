"""Utility functions and decorators for MCP tools.

This module provides common utilities for tool implementations including:
- Unified error handling decorator
- Response formatting helpers
- Validation utilities
"""

import asyncio
import functools
import logging
import re
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, ParamSpec, TypeVar, overload

from ..response_sanitizer import safe_error_message

logger = logging.getLogger(__name__)

UNRESOLVED_ENV_VAR = re.compile(r"\$\{([^}]+)\}")

P = ParamSpec("P")
T = TypeVar("T")


@overload
def tool_error_handler(
    tool_name: str | None = None,
    *,
    include_traceback: bool = False,
) -> Callable[
    [Callable[P, Coroutine[Any, Any, T]]], Callable[P, Awaitable[dict[str, Any]]]
]: ...


@overload
def tool_error_handler(
    tool_name: str | None = None,
    *,
    include_traceback: bool = False,
) -> Callable[[Callable[P, T]], Callable[P, dict[str, Any]]]: ...


def tool_error_handler(
    tool_name: str | None = None,
    *,
    include_traceback: bool = False,
) -> Callable[[Callable[P, T]], Callable[P, dict[str, Any] | Awaitable[dict[str, Any]]]]:
    """
    Decorator for unified error handling in MCP tools.

    Automatically catches exceptions and returns a standardized error response:
    {"success": False, "error": "sanitized error message"}

    On success, ensures the response includes {"success": True}.

    Args:
        tool_name: Optional tool name for logging (defaults to function name)
        include_traceback: Whether to include traceback in logs (default False)

    Returns:
        Decorated function with unified error handling

    Example:
        @tool_error_handler("my_tool")
        async def my_tool(param: str) -> dict[str, Any]:
            # ... tool logic ...
            return {"data": result}
    """

    def decorator(func: Callable[P, T]) -> Callable[P, dict[str, Any]]:
        name = tool_name or func.__name__

        @functools.wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
            try:
                result = await func(*args, **kwargs)
                # Ensure success flag is present
                if isinstance(result, dict):
                    if "success" not in result:
                        result["success"] = True
                    return result
                return {"success": True, "data": result}
            except Exception as e:
                logger.error(
                    "Tool '%s' failed: %s",
                    name,
                    str(e),
                    exc_info=include_traceback,
                )
                return {
                    "success": False,
                    "error": safe_error_message(e),
                    "tool": name,
                }

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
            try:
                result = func(*args, **kwargs)
                # Ensure success flag is present
                if isinstance(result, dict):
                    if "success" not in result:
                        result["success"] = True
                    return result
                return {"success": True, "data": result}
            except Exception as e:
                logger.error(
                    "Tool '%s' failed: %s",
                    name,
                    str(e),
                    exc_info=include_traceback,
                )
                return {
                    "success": False,
                    "error": safe_error_message(e),
                    "tool": name,
                }

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


def format_success_response(
    message: str,
    data: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """
    Format a successful tool response.

    Args:
        message: Success message
        data: Optional data payload
        **extra: Additional fields to include

    Returns:
        Formatted success response dict
    """
    response = {"success": True, "message": message}
    if data:
        response["data"] = data
    response.update(extra)
    return response


def format_error_response(
    error: str | Exception,
    tool_name: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """
    Format an error tool response.

    Args:
        error: Error message or exception
        tool_name: Optional tool name for context
        **extra: Additional fields to include

    Returns:
        Formatted error response dict
    """
    error_msg = safe_error_message(error) if isinstance(error, Exception) else error
    response: dict[str, Any] = {"success": False, "error": error_msg}
    if tool_name:
        response["tool"] = tool_name
    response.update(extra)
    return response


def validate_required_params(
    params: dict[str, Any],
    required: list[str],
) -> tuple[bool, str | None]:
    """
    Validate that required parameters are present and non-empty.

    Args:
        params: Dictionary of parameters to validate
        required: List of required parameter names

    Returns:
        Tuple of (is_valid, error_message)
    """
    missing = []
    for param in required:
        value = params.get(param)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(param)

    if missing:
        return False, f"Missing required parameters: {', '.join(missing)}"
    return True, None
