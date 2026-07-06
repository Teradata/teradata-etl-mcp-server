"""Airbyte API client for connector and sync operations.

This module provides a comprehensive wrapper around the Airbyte API
for managing connectors, sources, destinations, and sync operations.
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
from httpx import HTTPStatusError, RequestError

from ..response_sanitizer import safe_error_message
from ..storage.metadata_store import MetadataEntry, MetadataStore
from ..utils.circuit_breaker import CircuitBreakerBase, CircuitBreakerFactory
from ..utils.tls import build_tls_context
from ..utils.validators import to_quartz_cron

logger = logging.getLogger(__name__)


def to_public_api_sync_mode(source_mode: str, dest_mode: str) -> str:
    """Map (source_sync_mode, destination_sync_mode) to a valid Airbyte Public API v1 combined sync mode.

    Valid combined modes: full_refresh_overwrite, full_refresh_append,
    incremental_append, incremental_deduped_history.
    """
    s = (source_mode or "full_refresh").lower()
    d = (dest_mode or "append").lower()
    if s == "incremental":
        if d in ("append_dedup", "deduped_history", "overwrite_dedup"):
            return "incremental_deduped_history"
        return "incremental_append"
    # full_refresh — dedup modes are not valid, fall back to overwrite
    if d in ("append_dedup", "deduped_history", "overwrite_dedup"):
        return "full_refresh_overwrite"
    if d == "append":
        return "full_refresh_append"
    return "full_refresh_overwrite"


class AirbyteClientError(Exception):
    """Base exception for Airbyte client errors."""

    pass


class AirbyteConnectionError(AirbyteClientError):
    """Raised when connection to Airbyte fails."""

    pass


class AirbyteAPIError(AirbyteClientError):
    """Raised when Airbyte API returns an error."""

    pass


class AirbyteSyncError(AirbyteClientError):
    """Raised when sync operation fails."""

    pass


class AirbyteRateLimitExceeded(AirbyteClientError):
    """Raised when rate limit is exceeded."""

    pass


class AirbyteResponseTooLarge(AirbyteClientError):
    """Raised when response exceeds size limit."""

    pass


class CircuitBreakerOpen(AirbyteClientError):
    """Raised when circuit breaker is open and requests are blocked."""

    pass


# Module defaults for production hardening
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF = 1.0
DEFAULT_RATE_LIMIT_RPS = 10.0
DEFAULT_MAX_RESPONSE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB
_MIN_RATE_LIMIT_RPS = 0.1  # minimum 1 request per 10 seconds
_MAX_RATE_LIMIT_RPS = 1000.0  # reasonable upper bound

# Only retry idempotent methods for transient server errors (502/503/504).
# 429 (rate limit) is always retried regardless of method.
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


@dataclass
class RateLimiter:
    """Token bucket rate limiter for controlling request rate.

    Coroutine-safe implementation for use within a single asyncio event loop
    using asyncio locks.
    """

    rate: float  # requests per second
    burst: int = 10  # max burst size

    _tokens: float = field(init=False)
    _last_update: float = field(init=False)
    _lock: asyncio.Lock | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self._tokens = float(self.burst)
        self._last_update = time.monotonic()
        # Lock is created lazily in acquire() to avoid "no running event loop"
        # errors when the RateLimiter is instantiated outside an async context.

    async def acquire(self, timeout: float = 30.0) -> bool:
        """Acquire a token, waiting if necessary.

        Args:
            timeout: Maximum time to wait for a token

        Returns:
            True if token acquired

        Raises:
            AirbyteRateLimitExceeded: If cannot acquire within timeout
        """
        if self._lock is None:
            self._lock = asyncio.Lock()
        start_time = time.monotonic()
        while True:
            # Check timeout early so post-sleep lock waits don't silently overrun
            if time.monotonic() - start_time >= timeout:
                raise AirbyteRateLimitExceeded(
                    f"Rate limit exceeded: could not acquire token within {timeout}s"
                )
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_update
                self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
                self._last_update = now

                if self._tokens >= 1:
                    self._tokens -= 1
                    return True

            elapsed_total = time.monotonic() - start_time
            if elapsed_total >= timeout:
                raise AirbyteRateLimitExceeded(
                    f"Rate limit exceeded: could not acquire token within {timeout}s"
                )

            sleep_time = min(
                1.0 / max(self.rate, _MIN_RATE_LIMIT_RPS),
                timeout - elapsed_total,
                1.0,  # primary cap: at low rates (e.g. 0.1 RPS) the token interval is 10s, so this dominates
            )
            if sleep_time <= 0:
                raise AirbyteRateLimitExceeded(
                    f"Rate limit exceeded: could not acquire token within {timeout}s"
                )
            await asyncio.sleep(sleep_time)

    def get_status(self) -> dict[str, Any]:
        """Get rate limiter status."""
        return {
            "rate_rps": self.rate,
            "burst": self.burst,
            "available_tokens": self._tokens,
        }


class AirbyteClient:
    """
    Comprehensive Airbyte API client.

    Provides methods for connector management, source/destination operations,
    connection management, and sync orchestration.
    """

    def __init__(
        self,
        base_url: str | None = None,
        workspace_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        token_url: str = "/api/public/v1/applications/token",
        timeout: int = 60,
        metadata_store: MetadataStore | None = None,
        *,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        retry_backoff: float = DEFAULT_RETRY_BACKOFF,
        max_response_size_bytes: int = DEFAULT_MAX_RESPONSE_SIZE_BYTES,
        rate_limit_rps: float | None = DEFAULT_RATE_LIMIT_RPS,
        rate_limit_burst: int = 10,
        circuit_breaker_enabled: bool = True,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0,
        redis_url: str | None = None,
        max_connections: int = 100,
        max_keepalive_connections: int = 20,
        **kwargs: Any,
    ):
        """
        Initialize Airbyte client.

        Args:
            base_url: Airbyte server URL (host only, e.g., http://localhost:8000).
                      The client will automatically call the Public API under '/api/public/v1'. Also accepts 'url' via kwargs.
            workspace_id: The default workspace ID for all operations. If not provided, the first workspace will be used.
            client_id: OAuth2 Client ID for token-based auth
            client_secret: OAuth2 Client Secret for token-based auth
            token_url: Endpoint to obtain access token (default: /api/public/v1/applications/token)
            timeout: Request timeout in seconds
            retry_attempts: Max retry attempts for transient errors
            retry_backoff: Base backoff time in seconds between retries
            max_response_size_bytes: Max response size before raising error
            rate_limit_rps: Requests per second limit (None to disable)
            rate_limit_burst: Max burst size for rate limiter
            circuit_breaker_enabled: Whether to enable circuit breaker
            circuit_breaker_threshold: Failures before opening circuit
            circuit_breaker_timeout: Seconds before trying recovery
            redis_url: Optional Redis URL for distributed circuit breaker
            max_connections: Max HTTP connection pool size
            max_keepalive_connections: Max keepalive connections in pool
        """

        if not base_url and "url" in kwargs:
            base_url = kwargs.get("url")
        if not base_url:
            raise ValueError("Airbyte base_url (or url) is required")
        self.base_url = base_url.rstrip("/")
        # Workspace can be lazily resolved
        self.workspace_id: str | None = workspace_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self._access_token: str | None = None
        self.timeout = timeout

        self._client: httpx.AsyncClient | None = None
        self._client_lock: asyncio.Lock | None = None
        self._token_refresh_lock: asyncio.Lock | None = None
        self._resolved_workspace_id: str | None = None
        self._registry_url: str = (
            "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"
        )
        self._registry_cache: dict[str, Any] | None = None
        self._metadata_store: MetadataStore | None = metadata_store

        # Hardening configuration
        self._retry_attempts = max(1, retry_attempts)
        if retry_backoff <= 0:
            raise ValueError(f"retry_backoff must be positive, got {retry_backoff}")
        self._retry_backoff = retry_backoff
        if max_response_size_bytes <= 0:
            raise ValueError(
                f"max_response_size_bytes must be positive, got {max_response_size_bytes}"
            )
        self._max_response_size_bytes = max_response_size_bytes
        self._max_connections = max_connections
        self._max_keepalive_connections = max_keepalive_connections

        # Rate limiter (clamped to [_MIN_RATE_LIMIT_RPS, _MAX_RATE_LIMIT_RPS])
        self._rate_limiter: RateLimiter | None = None
        if rate_limit_rps and rate_limit_rps > 0:
            clamped_rps = max(_MIN_RATE_LIMIT_RPS, min(rate_limit_rps, _MAX_RATE_LIMIT_RPS))
            self._rate_limiter = RateLimiter(rate=clamped_rps, burst=rate_limit_burst)

        # Circuit breaker
        self._circuit_breaker: CircuitBreakerBase | None = None
        if circuit_breaker_enabled:
            self._circuit_breaker = CircuitBreakerFactory.create(
                name="airbyte",
                redis_url=redis_url,
                failure_threshold=circuit_breaker_threshold,
                recovery_timeout=circuit_breaker_timeout,
            )

        # Log authentication method being used
        auth_method = "OAuth2 client credentials" if client_id else "anonymous"
        if self.workspace_id:
            logger.info(
                "Initialized Airbyte client for %s using %s on workspace %s",
                self.base_url,
                auth_method,
                self.workspace_id,
            )
        else:
            logger.info(
                "Initialized Airbyte client for %s using %s (workspace will be resolved)",
                self.base_url,
                auth_method,
            )

    async def _get_workspace_id(self) -> str:
        """
        Ensure a workspace ID is available, resolving to the first available
        workspace if not explicitly provided.

        Returns:
            Resolved workspace ID
        """
        if self.workspace_id:
            return self.workspace_id
        if self._resolved_workspace_id:
            return self._resolved_workspace_id
        try:
            response = await self._make_request("GET", "/workspaces")
            workspaces = response.get("data", response if isinstance(response, list) else [])
            if not workspaces:
                raise AirbyteAPIError("No Airbyte workspaces available to select by default")
            first = workspaces[0]
            self._resolved_workspace_id = first.get("workspaceId")
            logger.info("Resolved default Airbyte workspace: %s", self._resolved_workspace_id)
            return self._resolved_workspace_id
        except Exception as e:
            logger.error("Failed to resolve default workspace: %s", e, exc_info=True)
            raise

    def _create_http_client(self) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient with authentication and pool limits."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        auth = None
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"

        return httpx.AsyncClient(
            base_url=self.base_url,
            auth=auth,
            headers=headers,
            timeout=self.timeout,
            verify=build_tls_context(),
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=self._max_connections,
                max_keepalive_connections=self._max_keepalive_connections,
            ),
        )

    @property
    def client(self) -> httpx.AsyncClient:
        """Get HTTP client (lazy, not race-safe — kept for backward compatibility)."""
        if self._client is None:
            self._client = self._create_http_client()
        return self._client

    async def _refresh_access_token(
        self, client: httpx.AsyncClient, stale_token: str | None
    ) -> str | None:
        """Refresh the OAuth2 access token under a lock, coalescing concurrent 401 handlers.

        If another coroutine already refreshed the token (i.e., ``self._access_token``
        differs from the ``stale_token`` the caller observed), reuse that new value and
        skip hitting the token endpoint. The active client's default Authorization
        header is updated while the lock is held.
        """
        if self._token_refresh_lock is None:
            self._token_refresh_lock = asyncio.Lock()
        async with self._token_refresh_lock:
            if self._access_token and self._access_token != stale_token:
                client.headers["Authorization"] = f"Bearer {self._access_token}"
                return self._access_token
            new_token = await self._obtain_token()
            if new_token:
                self._access_token = new_token
                client.headers["Authorization"] = f"Bearer {new_token}"
            return new_token

    async def _obtain_token(self) -> str | None:
        """Obtain access token using OAuth2 client credentials."""
        if not self.client_id or not self.client_secret:
            return None
        try:
            async with httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                verify=build_tls_context(),
            ) as temp_client:
                resp = await temp_client.post(
                    self.token_url,
                    json={
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                    },
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                token = data.get("access_token") or data.get("token")
                if token:
                    logger.info("Obtained Airbyte access token via client credentials")
                    return token
                logger.warning("Token response missing access_token field")
                return None
        except Exception as e:
            logger.error("Failed to obtain Airbyte token from %s%s: %s", self.base_url, self.token_url, e)
            return None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get HTTP client with async lock for race-safe initialization."""
        if self._client is not None:
            return self._client

        # Create lock lazily in the running event loop to avoid cross-loop issues
        if self._client_lock is None:
            self._client_lock = asyncio.Lock()

        async with self._client_lock:
            if self._client is not None:
                return self._client
            if self.client_id and self.client_secret and not self._access_token:
                self._access_token = await self._obtain_token()
            self._client = self._create_http_client()
        return self._client

    async def _make_request(self, method: str, endpoint: str, **kwargs) -> dict[str, Any]:
        """
        Make an API request with retry, rate limiting, and circuit breaker.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            **kwargs: Additional arguments for the request

        Returns:
            Response JSON data

        Raises:
            AirbyteAPIError: If the request fails after retries
            AirbyteConnectionError: If connection fails after retries
            CircuitBreakerOpen: If circuit breaker is open
            AirbyteRateLimitExceeded: If rate limit cannot be acquired
            AirbyteResponseTooLarge: If response exceeds size limit
        """
        # Circuit breaker check
        if self._circuit_breaker and not self._circuit_breaker.is_available:
            raise CircuitBreakerOpen(
                "Airbyte circuit breaker is open — service appears unavailable"
            )

        # Rate limiting
        if self._rate_limiter:
            await self._rate_limiter.acquire(timeout=float(self.timeout))

        url = f"/api/public/v1{endpoint}"
        logger.info("%s %s", method, url)
        client = await self._get_client()

        last_exception: Exception | None = None
        token_refreshed = False
        for attempt in range(1, self._retry_attempts + 1):
            try:
                response = await client.request(method, url, **kwargs)
                logger.debug("Response : %s", response)

                # Handle 401 (expired/invalid token) — refresh once and retry in-place.
                # OAuth2 client-credentials tokens have a short TTL and are baked into
                # the client's default headers, so a long-lived process will eventually
                # start 401-ing until the token is refreshed. We reissue the request
                # inside the same iteration so we don't consume a retry attempt — that
                # matters when a 401 hits on the final attempt (or retry_attempts=1).
                if (
                    response.status_code == 401
                    and self.client_id
                    and self.client_secret
                    and not token_refreshed
                ):
                    logger.info(
                        "Received 401 on %s %s, refreshing Airbyte access token and retrying",
                        method,
                        url,
                    )
                    stale = self._access_token
                    new_token = await self._refresh_access_token(client, stale)
                    if new_token:
                        token_refreshed = True
                        response = await client.request(method, url, **kwargs)
                        logger.debug("Post-refresh response: %s", response)
                    else:
                        logger.warning("Token refresh failed; surfacing original 401")

                # Handle 429 (rate limited by server) — always retry regardless of method
                if response.status_code == 429:
                    try:
                        retry_after = min(
                            float(response.headers.get("Retry-After", self._retry_backoff)),
                            120.0,
                        )
                    except (ValueError, TypeError):
                        retry_after = self._retry_backoff
                    if attempt < self._retry_attempts:
                        logger.warning(
                            "Rate limited (429) on %s %s, retrying in %.1fs (attempt %d/%d)",
                            method,
                            url,
                            retry_after,
                            attempt,
                            self._retry_attempts,
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    # Final attempt exhausted — fall through to raise_for_status()
                    # which triggers HTTPStatusError handler where circuit breaker
                    # records the failure

                # Handle retryable server errors for idempotent methods only
                if (
                    response.status_code in (502, 503, 504)
                    and method.upper() in _IDEMPOTENT_METHODS
                    and attempt < self._retry_attempts
                ):
                    backoff = self._retry_backoff * (2 ** (attempt - 1))
                    logger.warning(
                        "Server error %d on %s %s, retrying in %.1fs (attempt %d/%d)",
                        response.status_code,
                        method,
                        url,
                        backoff,
                        attempt,
                        self._retry_attempts,
                    )
                    await asyncio.sleep(backoff)
                    continue

                response.raise_for_status()

                # Response size check
                content_length = len(response.content) if response.content else 0
                if content_length > self._max_response_size_bytes:
                    raise AirbyteResponseTooLarge(
                        f"Response size {content_length} bytes exceeds limit "
                        f"of {self._max_response_size_bytes} bytes"
                    )

                # Record success
                if self._circuit_breaker:
                    self._circuit_breaker.record_success()

                if response.content:
                    ct = (response.headers.get("Content-Type") or "").lower()
                    if "json" in ct:
                        return response.json()
                    try:
                        return response.json()
                    except Exception:
                        return {"text": response.text.strip()}
                return {}

            except HTTPStatusError as e:
                # Only count server errors (5xx) as circuit breaker failures.
                # Client errors (4xx) indicate config/auth/request issues, not
                # service degradation, and should not open the circuit.
                if self._circuit_breaker and e.response.status_code >= 500:
                    self._circuit_breaker.record_failure()

                error_msg = safe_error_message(
                    e,
                    context=f"Airbyte API error ({e.response.status_code})",
                )
                try:
                    error_detail = e.response.json()
                    detail_msg = error_detail.get("message", str(error_detail))
                    # Sanitize detail through safe_error_message to prevent credential leaks
                    error_msg = safe_error_message(
                        ValueError(detail_msg),
                        context=f"Airbyte API error ({e.response.status_code})",
                    )
                except Exception:
                    pass

                logger.error("%s", error_msg, exc_info=True)
                raise AirbyteAPIError(error_msg) from e

            except (RequestError, OSError) as e:
                last_exception = e

                if attempt < self._retry_attempts:
                    backoff = self._retry_backoff * (2 ** (attempt - 1))
                    logger.warning(
                        "Connection error on %s %s (attempt %d/%d): %s, retrying in %.1fs",
                        method,
                        url,
                        attempt,
                        self._retry_attempts,
                        safe_error_message(e),
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue

                # Only record circuit breaker failure when retries are exhausted
                if self._circuit_breaker:
                    self._circuit_breaker.record_failure()

                logger.error("Airbyte connection error: %s", safe_error_message(e), exc_info=True)
                raise AirbyteConnectionError(
                    f"Cannot connect to Airbyte: {safe_error_message(e)}"
                ) from e

        # Should not reach here, but safety net
        raise AirbyteConnectionError(
            f"Request failed after {self._retry_attempts} attempts"
        ) from last_exception

    # ==================== Connector Registry (OSS) ====================

    async def fetch_connector_registry(self, force_refresh: bool = False) -> dict[str, Any]:
        """
        Fetch the Airbyte OSS connector registry, optionally from cache.

        Args:
            force_refresh: If True, bypass cache and refetch

        Returns:
            Registry JSON as a dict
        """
        if self._registry_cache is not None and not force_refresh:
            return self._registry_cache
        # Try to load from metadata store first, unless forced refresh
        key = "airbyte_oss_registry"
        if self._metadata_store and not force_refresh:
            try:
                entry = self._metadata_store.get_metadata(key)
                if entry and entry.value:
                    self._registry_cache = entry.value
                    return self._registry_cache
            except Exception as e:
                logger.warning("Failed reading registry from metadata store: %s", e)
        try:
            # Use absolute URL; httpx client can handle it even with base_url
            client = await self._get_client()
            resp = await client.get(self._registry_url)
            resp.raise_for_status()
            self._registry_cache = resp.json()
            # Persist to metadata store for future runs
            if self._metadata_store:
                try:
                    self._metadata_store.store_metadata(
                        MetadataEntry(
                            key=key,
                            value=self._registry_cache,
                            timestamp=datetime.now(timezone.utc),
                            ttl_seconds=None,
                            tags=["airbyte", "registry"],
                        )
                    )
                except Exception as se:
                    logger.warning("Failed storing registry in metadata store: %s", se)
            return self._registry_cache
        except Exception as e:
            logger.error("Failed to fetch connector registry: %s", e, exc_info=True)
            # As a fallback, try metadata store if available
            if self._metadata_store:
                entry = self._metadata_store.get_metadata(key)
                if entry and entry.value:
                    logger.info("Using cached registry from metadata store due to fetch failure")
                    self._registry_cache = entry.value
                    return self._registry_cache
            raise

    async def list_source_definitions_registry(self) -> list[dict[str, Any]]:
        reg = await self.fetch_connector_registry()
        return reg.get("sources", [])

    async def list_destination_definitions_registry(self) -> list[dict[str, Any]]:
        reg = await self.fetch_connector_registry()
        return reg.get("destinations", [])

    @staticmethod
    def _rank_definition_match(display_name: str, needle: str) -> int:
        """Score how well a connector display name matches a user-supplied keyword.

        Higher is better; 0 = no match. Picks exact > first-token > any-token > substring
        so "Postgres" picks the "Postgres" connector instead of "AlloyDB for PostgreSQL".
        """
        disp = (display_name or "").strip().lower()
        n = (needle or "").strip().lower()
        if not disp or not n:
            return 0
        if disp == n:
            return 4
        tokens = re.findall(r"[a-z0-9]+", disp)
        if tokens and tokens[0] == n:
            return 3
        if n in tokens:
            return 2
        if n in disp:
            return 1
        return 0

    def _best_definition_match(
        self,
        items: list[dict[str, Any]],
        needle: str,
        id_keys: list[str],
        name_keys: tuple[str, ...] = ("name", "title"),
    ) -> str | None:
        """Return the best-scoring definition ID from ``items`` for ``needle``.

        Tie-break order (deterministic, stable):
          1. Higher match score.
          2. Shorter display name (more specific beats broader names).
          3. Earlier position in ``items`` (preserves the caller's/API's order).

        The definition ID is never part of the comparison, so we never fall back
        to an implicit lexicographic UUID sort.
        """
        best_key: tuple[int, int, int] | None = None  # (score, -len(name), -index)
        best_id: str | None = None
        for idx, it in enumerate(items):
            raw = next((it.get(k) for k in name_keys if it.get(k)), "") or ""
            # Use the stripped name for both scoring and length, so whitespace in the
            # API response cannot skew the "shorter name wins" tie-break relative to
            # the score (which is computed on the stripped value).
            disp = raw.strip() if isinstance(raw, str) else ""
            score = self._rank_definition_match(disp, needle)
            if score <= 0:
                continue
            did = next((it.get(k) for k in id_keys if it.get(k)), None)
            if not did:
                continue
            key = (score, -len(disp), -idx)
            if best_key is None or key > best_key:
                best_key = key
                best_id = did
        return best_id

    async def find_definition_id_by_name(self, connector_type: str, name: str) -> str | None:
        """
        Find a definitionId for a source/destination by human-readable name.
        Tries the server's installed definitions first, then falls back to the OSS registry cache.
        Returns None if not found.
        """
        t = connector_type.lower()
        is_source = t.startswith("src") or t == "source"

        # 1) Try server-installed definitions first
        try:
            installed = (
                await self.list_source_definitions() if is_source
                else await self.list_destination_definitions()
            )
            match = self._best_definition_match(installed, name, id_keys=["id"])
            if match:
                return match
        except Exception as e:
            logger.warning("Error looking up %s definitions for '%s': %s", connector_type, name, e)

        # 2) Fall back to OSS registry cache.
        # Registry entries may carry the definition UUID under any of:
        #   - sourceDefinitionId / destinationDefinitionId (Airbyte registry shape)
        #   - definitionId (used elsewhere in this repo for registry items)
        #   - id (generic fallback)
        if is_source:
            items = await self.list_source_definitions_registry()
            registry_id_keys = ["sourceDefinitionId", "definitionId", "id"]
        else:
            items = await self.list_destination_definitions_registry()
            registry_id_keys = ["destinationDefinitionId", "definitionId", "id"]
        return self._best_definition_match(items, name, id_keys=registry_id_keys)

    async def get_health(self) -> dict[str, Any]:
        """
        Check Airbyte health status.

        Returns:
            Health status dictionary
        """
        try:
            result = await self._make_request("GET", "/health")
            status_val = result.get("text")
            return {
                "connected": True,
                "url": self.base_url,
                "status": status_val,
            }
        except Exception as e:
            logger.error("Health check failed: %s", e, exc_info=True)
            return {
                "connected": False,
                "url": self.base_url,
                "error": safe_error_message(e),
            }

    async def test_connection(self) -> dict[str, Any]:
        """
        Test connection to Airbyte.

        Returns:
            Dictionary with connection status
        """
        return await self.get_health()

    # ==================== Connector Registry Operations ====================

    async def list_source_definitions(
        self, workspace_id: str | None = None
    ) -> list[dict[str, Any]]:
        """
        List all available source connector definitions.

        Args:
            workspace_id: Workspace ID to scope results. If omitted, the client's default workspace is used.
            limit: Number of definitions to retrieve
            offset: Starting offset for pagination

        Returns:
            List of source connector definitions
        """
        try:
            # Resolve workspace and use workspace-scoped Public API endpoint
            ws_id = workspace_id or await self._get_workspace_id()
            response = await self._make_request("GET", f"/workspaces/{ws_id}/definitions/sources")
            items = response.get("data", response if isinstance(response, list) else [])
            logger.info("Retrieved %d source definitions", len(items))
            return items
        except Exception as e:
            logger.error("Failed to list source definitions: %s", e, exc_info=True)
            raise

    async def list_destination_definitions(
        self, workspace_id: str | None = None
    ) -> list[dict[str, Any]]:
        """
        List all available destination connector definitions.

        Returns:
            List of destination connector definitions
        """
        try:
            ws_id = workspace_id or await self._get_workspace_id()
            response = await self._make_request(
                "GET", f"/workspaces/{ws_id}/definitions/destinations"
            )
            items = response.get("data", response if isinstance(response, list) else [])
            logger.info("Retrieved %d destination definitions", len(items))
            return items
        except Exception as e:
            logger.error("Failed to list destination definitions: %s", e, exc_info=True)
            raise

    async def search_connectors(
        self,
        search_term: str,
        connector_type: str = "both",
    ) -> dict[str, Any]:
        """
        Search for connectors by name.

        Args:
            search_term: Search term
            connector_type: 'source', 'destination', or 'both'

        Returns:
            Dictionary with matching sources and destinations
        """
        results = {
            "sources": [],
            "destinations": [],
        }

        try:
            search_lower = search_term.lower()

            if connector_type in ("source", "both"):
                sources = await self.list_source_definitions()
                results["sources"] = [
                    s for s in sources if search_lower in s.get("name", "").lower()
                ]

            if connector_type in ("destination", "both"):
                destinations = await self.list_destination_definitions()
                results["destinations"] = [
                    d for d in destinations if search_lower in d.get("name", "").lower()
                ]

            logger.info(
                "Found %d sources and %d destinations",
                len(results["sources"]),
                len(results["destinations"]),
            )
            return results

        except Exception as e:
            logger.error("Connector search failed: %s", e, exc_info=True)
            raise

    # ==================== Workspace Operations ====================

    async def list_workspaces(self) -> list[dict[str, Any]]:
        """
        List all workspaces.

        Returns:
            List of workspace dictionaries
        """
        try:
            response = await self._make_request("GET", "/workspaces")
            workspaces = response.get("data", response if isinstance(response, list) else [])
            logger.info("Retrieved %d workspaces", len(workspaces))
            return workspaces

        except Exception as e:
            logger.error("Failed to list workspaces: %s", e, exc_info=True)
            raise

    async def create_workspace(
        self,
        name: str,
        organization_id: str | None = None,
        notifications: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Create a new workspace via the Public API.

        Args:
            name: Workspace name (required).
            organization_id: Optional organization UUID. If your account has multiple organizations,
                you must provide this. If you have a single organization, the API will infer it.
            notifications: Optional notifications configuration object to enable event emails.
                Example: {"failure": {"email": {"enabled": True}}, ...}

        Returns:
            Created workspace dictionary (includes at least workspaceId).
        """
        try:
            payload: dict[str, Any] = {"name": name}
            if organization_id:
                payload["organizationId"] = organization_id
            if notifications is not None:
                if not isinstance(notifications, dict):
                    raise ValueError("notifications must be a dictionary when provided")
                payload["notifications"] = notifications

            result = await self._make_request(
                "POST",
                "/workspaces",
                json=payload,
            )
            logger.info("Created workspace '%s' with id %s", name, result.get("workspaceId"))
            return result
        except Exception as e:
            logger.error("Failed to create workspace '%s': %s", name, e, exc_info=True)
            raise

    # ==================== Convenience Lookup Helpers ====================

    async def get_source_id(self, name: str) -> str | None:
        """Find a source ID by name across all accessible workspaces."""
        try:
            workspaces = await self.list_workspaces()
            for ws in workspaces:
                ws_id = ws.get("workspaceId")
                if not ws_id:
                    continue
                try:
                    sources = await self.list_sources(workspace_ids=ws_id)
                    for s in sources:
                        if s.get("name") == name:
                            return s.get("sourceId")
                except Exception as e:
                    logger.warning("Error listing sources in workspace %s: %s", ws_id, e)
                    continue
            return None
        except Exception as e:
            logger.warning("Error searching for source '%s': %s", name, e)
            return None

    async def get_destination_id(self, name: str) -> str | None:
        """Find a destination ID by name across all accessible workspaces."""
        try:
            workspaces = await self.list_workspaces()
            for ws in workspaces:
                ws_id = ws.get("workspaceId")
                if not ws_id:
                    continue
                try:
                    dests = await self.list_destinations(workspace_ids=ws_id)
                    for d in dests:
                        if d.get("name") == name:
                            return d.get("destinationId")
                except Exception as e:
                    logger.warning("Error listing destinations in workspace %s: %s", ws_id, e)
                    continue
            return None
        except Exception as e:
            logger.warning("Error searching for destination '%s': %s", name, e)
            return None

    async def get_connection_id(self, name: str) -> str | None:
        """Find a connection ID by name across all accessible workspaces."""
        try:
            workspaces = await self.list_workspaces()
            for ws in workspaces:
                ws_id = ws.get("workspaceId")
                if not ws_id:
                    continue
                try:
                    conns = await self.list_connections(workspace_ids=ws_id)
                    for c in conns:
                        if c.get("name") == name:
                            return c.get("connectionId")
                except Exception as e:
                    logger.warning("Error listing connections in workspace %s: %s", ws_id, e)
                    continue
            return None
        except Exception as e:
            logger.warning("Error searching for connection '%s': %s", name, e)
            return None

    async def get_workspace(self, workspace_id: str) -> dict[str, Any]:
        """
        Get workspace details.

        Args:
            workspace_id: Workspace ID

        Returns:
            Workspace information
        """
        if not workspace_id:
            raise ValueError("workspace_id is required and cannot be empty")
        try:
            result = await self._make_request(
                "GET",
                f"/workspaces/{workspace_id}",
            )
            logger.debug("Retrieved workspace: %s", workspace_id)
            return result
        except Exception as e:
            logger.error("Failed to get workspace: %s", e, exc_info=True)
            raise

    # ==================== Source Operations ====================

    async def list_sources(
        self,
        workspace_ids: list[str] | str | None = None,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        List sources per Public API.

        Args:
            workspace_ids: Optional single UUID string or list of UUIDs to filter. If None, lists sources from all accessible workspaces.
            include_deleted: Whether to include deleted sources.
            limit: Page size (1-100, default 100).
            offset: Pagination offset (>=0).

        Returns:
            List of source dictionaries
        """
        try:
            params: dict[str, Any] = {"limit": limit, "offset": offset}
            if include_deleted:
                params["includeDeleted"] = True
            if workspace_ids:
                if isinstance(workspace_ids, list):
                    params["workspaceIds"] = ",".join(workspace_ids)
                else:
                    params["workspaceIds"] = workspace_ids
            else:
                # Default to the client's workspace if not provided
                try:
                    ws_id = await self._get_workspace_id()
                    params["workspaceIds"] = ws_id
                except Exception as we:
                    logger.warning(
                        "Could not resolve default workspaceId for listing sources: %s", we
                    )
            response = await self._make_request(
                "GET",
                "/sources",
                params=params,
            )
            data = response.get("data", response if isinstance(response, list) else [])
            logger.info("Retrieved %d sources", len(data))
            return data

        except Exception as e:
            logger.error("Failed to list sources: %s", e, exc_info=True)
            raise

    async def get_source(self, source_id: str) -> dict[str, Any]:
        """
        Get source details.

        Args:
            source_id: Source ID

        Returns:
            Source information
        """

        if not source_id:
            raise ValueError("source_id is required and cannot be empty")
        try:
            result = await self._make_request(
                "GET",
                f"/sources/{source_id}",
            )
            logger.debug("Retrieved source: %s", source_id)
            return result
        except Exception as e:
            logger.error("Failed to get source: %s", e, exc_info=True)
            raise

    async def create_source(
        self,
        workspace_id: str,
        source_definition_id: str | None,
        name: str,
        connection_configuration: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Create a new source.

        Args:
            workspace_id: Workspace ID
            source_definition_id: Source definition ID
            name: Source name
            connection_configuration: Source-specific configuration

        Returns:
            Created source information
        """
        try:
            # Public API expects 'configuration' and optional 'definitionId'
            payload: dict[str, Any] = {
                "workspaceId": workspace_id,
                "name": name,
                "configuration": connection_configuration,
            }
            if source_definition_id:
                payload["definitionId"] = source_definition_id
            result = await self._make_request(
                "POST",
                "/sources",
                json=payload,
            )
            logger.info("Created source: %s", name)
            return result
        except Exception as e:
            logger.error("Failed to create source: %s", e, exc_info=True)
            raise

    async def update_source(
        self,
        source_id: str,
        name: str | None = None,
        connection_configuration: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Update an existing source.

        Args:
            source_id: Source ID
            name: Optional new name
            connection_configuration: Optional new configuration

        Returns:
            Updated source information
        """
        try:
            # Get current source
            current = await self.get_source(source_id)

            payload = {
                "name": name or current.get("name"),
                "configuration": connection_configuration or current.get("configuration"),
            }

            result = await self._make_request(
                "PATCH",
                f"/sources/{source_id}",
                json=payload,
            )
            logger.info("Updated source: %s", source_id)
            return result
        except Exception as e:
            logger.error("Failed to update source: %s", e, exc_info=True)
            raise

    async def delete_source(self, source_id: str) -> bool:
        """
        Delete a source.

        Args:
            source_id: Source ID

        Returns:
            True if successful
        """
        try:
            await self._make_request(
                "DELETE",
                f"/sources/{source_id}",
            )
            logger.info("Deleted source: %s", source_id)
            return True
        except Exception as e:
            logger.error("Failed to delete source: %s", e, exc_info=True)
            raise

    async def discover_source_schema(
        self,
        source_id: str,
    ) -> dict[str, Any]:
        """
        Discover schema from source.

        Args:
            source_id: Source ID

        Returns:
            Discovered schema information
        """
        try:
            resp = await self._make_request(
                "GET",
                "/streams",
                params={"sourceId": source_id},
            )
            # Normalize to expected ConfiguredAirbyteCatalog shape with rich stream metadata
            # The Public API wraps results in {"data": [...]}, so extract the data array first
            logger.info("Raw response for source %s: %s", source_id, resp)
            if isinstance(resp, dict):
                items = resp.get("data", [])
            elif isinstance(resp, list):
                items = resp
            else:
                items = []
            normalized_streams: list[dict[str, Any]] = []
            for it in items:
                name = it.get("streamName") or it.get("name")
                raw_sync_modes = it.get("syncModes") or it.get("supportedSyncModes") or []
                logger.info("Processing stream: %s", name)
                logger.info("Raw sync modes for stream %s: %s", name, raw_sync_modes)
                # The Public API v1 returns combined modes (e.g., "full_refresh_overwrite",
                # "incremental_append") but the rest of the codebase works with simple modes
                # ("full_refresh", "incremental"). Normalize combined → simple so that
                # build_configured_catalog can match user-requested modes correctly.
                simple_modes: set = set()
                for mode in raw_sync_modes:
                    logger.info("Normalizing sync modes for stream %s: %s", name, mode)
                    if mode.startswith("incremental"):
                        simple_modes.add("incremental")
                    elif mode.startswith("full_refresh"):
                        simple_modes.add("full_refresh")
                    else:
                        simple_modes.add(mode)
                # When the API returns syncModes as null/empty, it is not constraining
                # available modes — allow both full_refresh and incremental so that
                # user-requested incremental sync is not silently blocked.
                sync_modes = list(simple_modes) if simple_modes else ["full_refresh", "incremental"]
                stream_obj: dict[str, Any] = {
                    "name": name,
                    "supportedSyncModes": sync_modes,
                }
                # Carry through optional but useful fields when available
                if it.get("jsonSchema") is not None:
                    stream_obj["jsonSchema"] = it.get("jsonSchema")
                if it.get("schema") is not None and stream_obj.get("jsonSchema") is None:
                    stream_obj["jsonSchema"] = it.get("schema")
                if it.get("defaultCursorField") is not None:
                    stream_obj["defaultCursorField"] = it.get("defaultCursorField")
                if it.get("sourceDefinedCursor") is not None:
                    stream_obj["sourceDefinedCursor"] = it.get("sourceDefinedCursor")
                if it.get("namespace") is not None:
                    stream_obj["namespace"] = it.get("namespace")
                # primary key representation varies; preserve when present
                if it.get("sourceDefinedPrimaryKey") is not None:
                    stream_obj["sourceDefinedPrimaryKey"] = it.get("sourceDefinedPrimaryKey")
                if (
                    it.get("primaryKey") is not None
                    and stream_obj.get("sourceDefinedPrimaryKey") is None
                ):
                    stream_obj["sourceDefinedPrimaryKey"] = it.get("primaryKey")
                # Preserve available columns so cursor field / primary key can be validated
                if it.get("propertyFields") is not None:
                    stream_obj["propertyFields"] = it.get("propertyFields")

                normalized_streams.append({"stream": stream_obj})
            result = {"catalog": {"streams": normalized_streams}}
            logger.info("Discovered %d streams from source", len(normalized_streams))
            return result
        except Exception as e:
            logger.error("Failed to discover source schema: %s", e, exc_info=True)
            raise

    # Aliases for compatibility with various callers/tests
    async def discover_schema(self, source_id: str) -> dict[str, Any]:
        return await self.discover_source_schema(source_id)

    # ==================== Destination Operations ====================

    async def list_destinations(
        self,
        workspace_ids: list[str] | str | None = None,
        include_deleted: bool = False,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        List destinations per Public API.

        Returns:
            List of destination dictionaries
        """
        try:
            params: dict[str, Any] = {"limit": limit, "offset": offset}
            if include_deleted:
                params["includeDeleted"] = True
            if workspace_ids:
                if isinstance(workspace_ids, list):
                    params["workspaceIds"] = ",".join(workspace_ids)
                else:
                    params["workspaceIds"] = workspace_ids
            else:
                # Default to the client's workspace if not provided
                try:
                    ws_id = await self._get_workspace_id()
                    params["workspaceIds"] = ws_id
                except Exception as we:
                    logger.warning(
                        "Could not resolve default workspaceId for listing destinations: %s", we
                    )
            response = await self._make_request(
                "GET",
                "/destinations",
                params=params,
            )
            data = response.get("data", response if isinstance(response, list) else [])
            logger.info("Retrieved %d destinations", len(data))
            return data

        except Exception as e:
            logger.error("Failed to list destinations: %s", e, exc_info=True)
            raise

    async def get_destination(self, destination_id: str) -> dict[str, Any]:
        """
        Get destination details.

        Args:
            destination_id: Destination ID

        Returns:
            Destination information
        """
        try:
            result = await self._make_request(
                "GET",
                f"/destinations/{destination_id}",
            )
            logger.debug("Retrieved destination: %s", destination_id)
            return result
        except Exception as e:
            logger.error("Failed to get destination: %s", e, exc_info=True)
            raise

    async def create_destination(
        self,
        workspace_id: str,
        destination_definition_id: str | None,
        name: str,
        connection_configuration: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Create a new destination.

        Args:
            workspace_id: Workspace ID
            destination_definition_id: Destination definition ID
            name: Destination name
            connection_configuration: Destination-specific configuration

        Returns:
            Created destination information
        """
        try:
            payload: dict[str, Any] = {
                "workspaceId": workspace_id,
                "name": name,
                "configuration": connection_configuration,
            }
            if destination_definition_id:
                payload["definitionId"] = destination_definition_id
            result = await self._make_request(
                "POST",
                "/destinations",
                json=payload,
            )
            logger.info("Created destination: %s", name)
            return result
        except Exception as e:
            logger.error("Failed to create destination: %s", e, exc_info=True)
            raise

    async def update_destination(
        self,
        destination_id: str,
        name: str | None = None,
        connection_configuration: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Update an existing destination.

        Args:
            destination_id: Destination ID
            name: Optional new name
            connection_configuration: Optional new configuration

        Returns:
            Updated destination information
        """
        try:
            # Get current destination
            current = await self.get_destination(destination_id)

            payload = {
                "name": name or current.get("name"),
                "configuration": connection_configuration or current.get("configuration"),
            }

            result = await self._make_request(
                "PATCH",
                f"/destinations/{destination_id}",
                json=payload,
            )
            logger.info("Updated destination: %s", destination_id)
            return result
        except Exception as e:
            logger.error("Failed to update destination: %s", e, exc_info=True)
            raise

    async def delete_destination(self, destination_id: str) -> bool:
        """
        Delete a destination.

        Args:
            destination_id: Destination ID

        Returns:
            True if successful
        """
        try:
            await self._make_request(
                "DELETE",
                f"/destinations/{destination_id}",
            )
            logger.info("Deleted destination: %s", destination_id)
            return True
        except Exception as e:
            logger.error("Failed to delete destination: %s", e, exc_info=True)
            raise

    # ==================== Connection Operations ====================

    async def list_connections(
        self,
        workspace_ids: list[str] | str | None = None,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        List all connections in the configured workspace.

        Returns:
            List of connection dictionaries
        """
        try:
            params: dict[str, Any] = {"limit": limit, "offset": offset}
            if include_deleted:
                params["includeDeleted"] = True
            if workspace_ids:
                if isinstance(workspace_ids, list):
                    params["workspaceIds"] = ",".join(workspace_ids)
                else:
                    params["workspaceIds"] = workspace_ids
            else:
                try:
                    ws_id = await self._get_workspace_id()
                    params["workspaceIds"] = ws_id
                except Exception as we:
                    logger.warning(
                        "Could not resolve default workspaceId for listing connections: %s", we
                    )
            response = await self._make_request(
                "GET",
                "/connections",
                params=params,
            )
            connections = response.get("data", response if isinstance(response, list) else [])
            logger.info("Retrieved %d connections", len(connections))
            return connections

        except Exception as e:
            logger.error("Failed to list connections: %s", e, exc_info=True)
            raise

    async def get_connection(self, connection_id: str) -> dict[str, Any]:
        """
        Get connection details.

        Args:
            connection_id: Connection ID

        Returns:
            Connection information
        """
        try:
            result = await self._make_request(
                "GET",
                f"/connections/{connection_id}",
            )
            logger.debug("Retrieved connection: %s", connection_id)
            return result
        except Exception as e:
            logger.error("Failed to get connection: %s", e, exc_info=True)
            raise

    async def create_connection(
        self,
        source_id: str | None = None,
        destination_id: str | None = None,
        name: str | None = None,
        streams: list[dict[str, Any]] | None = None,
        schedule_type: str | None = None,
        schedule_cron: str | None = None,
        namespace_definition: str = "destination",
        namespace_format: str | None = None,
        sync_catalog: dict[str, Any] | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Create a new connection using the Airbyte Public API structure.
        Matches the user's payload: uses 'configurations' and combined 'syncMode'.
        """
        try:
            if raw_payload is not None:
                payload = raw_payload
            else:
                # 1. Base Fields
                payload = {
                    "sourceId": source_id,
                    "destinationId": destination_id,
                    "name": name,
                    "namespaceDefinition": namespace_definition,
                    "status": "active",
                }

                # 2. Schedule (Nested Object)
                st = (schedule_type or "").lower()
                if st == "manual" and schedule_cron:
                    raise ValueError(
                        "Conflicting parameters: schedule_type is 'manual' "
                        "but schedule_cron was provided."
                    )
                if st == "cron" and not schedule_cron:
                    raise ValueError("schedule_cron is required when schedule_type is 'cron'")
                if schedule_cron:
                    payload["schedule"] = {
                        "scheduleType": "cron",
                        "cronExpression": to_quartz_cron(schedule_cron),
                    }
                else:
                    payload["schedule"] = {"scheduleType": "manual"}

                if namespace_format:
                    payload["namespaceFormat"] = namespace_format

                # 3. Configurations (The Public API Field)
                api_streams = []

                # Handle inputs from your script
                input_list = streams or (sync_catalog.get("streams") if sync_catalog else [])

                for item in input_list:
                    # Flatten if nested (just in case)
                    candidate = item["stream"].copy() if "stream" in item else item.copy()
                    if "config" in item:
                        candidate.update(item["config"])

                    # Build the Stream Object
                    if candidate.get("selected", True):
                        # COMBINE MODES: "full_refresh" + "append" -> "full_refresh_append"
                        s_mode = candidate.get("syncMode", "full_refresh")
                        d_mode = candidate.get("destinationSyncMode", "append")
                        combined_mode = to_public_api_sync_mode(s_mode, d_mode)
                        logger.debug("Using combined sync mode: %s", combined_mode)

                        stream_conf = {
                            "name": candidate.get("name"),
                            "syncMode": combined_mode,  # <--- Matches your payload snippet
                        }

                        # NAMESPACE: Required for Postgres/Snowflake
                        if candidate.get("namespace"):
                            stream_conf["namespace"] = candidate.get("namespace")

                        # Optional Fields
                        if candidate.get("cursorField"):
                            stream_conf["cursorField"] = candidate.get("cursorField")
                        if candidate.get("primaryKey"):
                            stream_conf["primaryKey"] = candidate.get("primaryKey")

                        api_streams.append(stream_conf)

                if api_streams:
                    payload["configurations"] = {"streams": api_streams}

            logger.debug("Creating connection payload (keys: %s)", list(payload.keys()))

            result = await self._make_request("POST", "/connections", json=payload)
            logger.info("Successfully created connection: %s", result.get("connectionId"))
            return result
        except Exception as e:
            logger.error("Failed to create connection: %s", e, exc_info=True)
            raise

    async def update_connection(self, connection_id: str, **kwargs) -> dict[str, Any]:
        """
        Update an existing connection using Airbyte Public API.

        Args:
            connection_id: Connection ID
            **kwargs: Fields to update. KEY CHANGE: Use 'configurations' instead of 'syncCatalog'.
        """
        try:
            # STRICT Public API allowed fields
            allowed_fields = {
                "name",
                "status",
                "configurations",  # <--- REPLACED syncCatalog with configurations
                "schedule",
                "namespaceDefinition",
                "namespaceFormat",
                "prefix",
                "dataResidency",
                "nonBreakingSchemaUpdatesBehavior",
            }

            payload: dict[str, Any] = {k: v for k, v in kwargs.items() if k in allowed_fields}
            logger.info("Update payload: %s", payload)
            # Debug log to verify correct payload structure
            logger.debug("Update payload for connection %s: %s", connection_id, payload.keys())

            if not payload:
                return await self.get_connection(connection_id)

            result = await self._make_request(
                "PATCH",
                f"/connections/{connection_id}",
                json=payload,
            )
            logger.info("Updated connection: %s", connection_id)
            return result
        except Exception as e:
            logger.error("Failed to update connection: %s", e, exc_info=True)
            raise

    async def delete_connection(self, connection_id: str) -> bool:
        """
        Delete a connection.

        Args:
            connection_id: Connection ID

        Returns:
            True if successful
        """
        try:
            await self._make_request(
                "DELETE",
                f"/connections/{connection_id}",
            )
            logger.info("Deleted connection: %s", connection_id)
            return True
        except Exception as e:
            logger.error("Failed to delete connection: %s", e, exc_info=True)
            raise

    # ==================== Sync Operations ====================

    async def trigger_sync(
        self,
        connection_id: str,
    ) -> dict[str, Any]:
        """
        Trigger a manual sync for a connection.

        Args:
            connection_id: Connection ID

        Returns:
            Job information
        """
        try:
            result = await self._make_request(
                "POST",
                "/jobs",
                json={"jobType": "sync", "connectionId": connection_id},
            )

            job_id = result.get("jobId")
            logger.info("Triggered sync for connection %s, job: %s", connection_id, job_id)
            return result
        except Exception as e:
            logger.error("Failed to trigger sync: %s", e, exc_info=True)
            raise

    async def get_job_status(self, job_id: int) -> dict[str, Any]:
        """
        Get job status.

        Args:
            job_id: Job ID

        Returns:
            Job status information
        """
        try:
            result = await self._make_request(
                "GET",
                f"/jobs/{job_id}",
            )
            logger.debug("Retrieved job status: %s", job_id)
            return result
        except Exception as e:
            logger.error("Failed to get job status: %s", e, exc_info=True)
            raise

    # Compatibility aliases
    async def get_job_info(self, job_id: int) -> dict[str, Any]:
        return await self.get_job_status(job_id)

    async def get_job_logs(self, job_id: int) -> dict[str, Any]:
        """
        Get logs for a specific job.

        Note: The Airbyte Public API v1 does not include a ``/jobs/{jobId}/logs``
        endpoint, so this call will typically fail with a 404 or 405
        (AirbyteAPIError).  Other AirbyteClientError subclasses
        (AirbyteConnectionError, AirbyteRateLimitExceeded, CircuitBreakerOpen)
        may also be raised by the underlying request.  Callers should catch
        AirbyteClientError and handle each case appropriately.

        Args:
            job_id: Job ID

        Returns:
            Dictionary containing the logs

        Raises:
            AirbyteClientError: On any client-level failure (API error,
                connection issue, rate limit, circuit breaker, etc.).
        """
        try:
            result = await self._make_request(
                "GET",
                f"/jobs/{job_id}/logs",
            )
            logger.debug("Retrieved logs for job: %s", job_id)
            return result
        except Exception as e:
            logger.debug("Failed to get job logs: %s", e, exc_info=True)
            raise

    async def list_jobs(
        self,
        config_type: str = "sync",
        config_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        List jobs.

        Args:
            config_type: Job config type (sync, check_connection, discover_schema)
            config_id: Optional config ID to filter by

        Returns:
            List of job dictionaries
        """
        try:
            response = await self._make_request(
                "GET",
                "/jobs",
                params={
                    "jobType": config_type,
                    **({"connectionId": config_id} if config_id else {}),
                },
            )
            jobs = response.get("data", response if isinstance(response, list) else [])
            logger.info("Retrieved %d jobs", len(jobs))
            return jobs

        except Exception as e:
            logger.error("Failed to list jobs: %s", e, exc_info=True)
            raise

    async def wait_for_job(
        self,
        job_id: int,
        timeout: int = 3600,
        poll_interval: int = 10,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """
        Wait for a job to complete.

        Args:
            job_id: Job ID
            timeout: Maximum time to wait in seconds
            poll_interval: Time between status checks in seconds
            max_retries: Maximum number of retries for transient errors

        Returns:
            Final job status

        Raises:
            AirbyteSyncError: If job fails or times out
            AirbyteConnectionError: If unable to check job status after retries
        """
        start_time = time.monotonic()
        retry_count = 0

        while True:
            if time.monotonic() - start_time > timeout:
                raise AirbyteSyncError(f"Job {job_id} timed out after {timeout}s")

            try:
                status = await self.get_job_status(job_id)
                job_status = status.get("status")
                # Reset retry count on successful status check
                retry_count = 0

                if job_status in ("succeeded", "failed", "cancelled"):
                    logger.info("Job %d completed with status: %s", job_id, job_status)

                    if job_status == "failed":
                        raise AirbyteSyncError(f"Job {job_id} failed")
                    elif job_status == "cancelled":
                        raise AirbyteSyncError(f"Job {job_id} was cancelled")

                    return status

                logger.debug("Job %d status: %s, waiting...", job_id, job_status)
                await asyncio.sleep(poll_interval)

            except AirbyteSyncError:
                raise
            except Exception as e:
                retry_count += 1
                logger.error(
                    "Error checking job status (attempt %d/%d): %s",
                    retry_count,
                    max_retries,
                    e,
                    exc_info=True,
                )

                if retry_count >= max_retries:
                    raise AirbyteConnectionError(
                        f"Failed to get job status after {max_retries} retries"
                    ) from e

                await asyncio.sleep(poll_interval)

    async def wait_for_job_completion(
        self,
        job_id: int,
        timeout: int = 3600,
        poll_interval: int = 10,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        return await self.wait_for_job(job_id, timeout, poll_interval, max_retries)

    async def get_connection_sync_history(
        self,
        connection_id: str,
        limit: int = 10,
    ) -> dict[str, Any]:
        """
        Get sync history for a connection.

        Args:
            connection_id: Connection ID
            limit: Number of recent syncs to retrieve

        Returns:
            Dictionary with sync history and statistics
        """
        try:
            jobs = await self.list_jobs(config_type="sync", config_id=connection_id)

            # Sort by creation time
            jobs.sort(key=lambda x: x.get("createdAt", ""), reverse=True)

            recent_jobs = jobs[:limit]

            # Calculate statistics
            stats = {
                "succeeded": 0,
                "failed": 0,
                "running": 0,
                "cancelled": 0,
            }

            for job in recent_jobs:
                status = job.get("status", "").lower()
                if status in stats:
                    stats[status] += 1

            success_rate = stats["succeeded"] / len(recent_jobs) * 100 if recent_jobs else 0

            return {
                "connection_id": connection_id,
                "total_syncs": len(jobs),
                "recent_syncs": recent_jobs,
                "statistics": stats,
                "success_rate": round(success_rate, 2),
            }

        except Exception as e:
            logger.error("Failed to get sync history: %s", e, exc_info=True)
            raise

    # ==================== Utility Methods ====================

    async def close(self):
        """Close HTTP client and clean up resources."""
        if self._client:
            try:
                await self._client.aclose()
                self._client = None
                logger.info("Airbyte client closed")
            except Exception as e:
                logger.error("Error closing client: %s", e, exc_info=True)

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    # ==================== Observability ====================

    def get_rate_limiter_status(self) -> dict[str, Any] | None:
        """Get rate limiter status, or None if disabled."""
        if self._rate_limiter:
            return self._rate_limiter.get_status()
        return None

    def get_circuit_breaker_status(self) -> dict[str, Any] | None:
        """Get circuit breaker status, or None if disabled."""
        if self._circuit_breaker:
            return self._circuit_breaker.get_status()
        return None

    def get_client_status(self) -> dict[str, Any]:
        """Get comprehensive client configuration and status."""
        return {
            "base_url": self.base_url,
            "retry_attempts": self._retry_attempts,
            "retry_backoff": self._retry_backoff,
            "max_response_size_bytes": self._max_response_size_bytes,
            "max_connections": self._max_connections,
            "max_keepalive_connections": self._max_keepalive_connections,
            "rate_limiter": self.get_rate_limiter_status(),
            "circuit_breaker": self.get_circuit_breaker_status(),
            "client_initialized": self._client is not None,
        }

    # ==================== Idempotent Helpers ====================

    async def find_source_by_config(
        self,
        connection_configuration: dict[str, Any],
        name: str | None = None,
    ) -> dict[str, Any] | None:
        """Find an existing source matching the given configuration (and optional name)."""
        sources = await self.list_sources()
        for s in sources:
            if name and s.get("name", "").lower() != name.lower():
                continue
            if s.get("configuration") == connection_configuration:
                return s
        return None

    async def find_destination_by_config(
        self,
        connection_configuration: dict[str, Any],
        name: str | None = None,
    ) -> dict[str, Any] | None:
        """Find an existing destination matching the given configuration (and optional name)."""
        destinations = await self.list_destinations()
        for d in destinations:
            if name and d.get("name", "").lower() != name.lower():
                continue
            if d.get("configuration") == connection_configuration:
                return d
        return None

    async def create_source_if_not_exists(
        self,
        workspace_id: str | None,
        source_definition_id: str,
        name: str,
        connection_configuration: dict[str, Any],
    ) -> dict[str, Any]:
        existing = await self.find_source_by_config(connection_configuration, name=name)
        if existing:
            return existing
        return await self.create_source(
            workspace_id=workspace_id or await self._get_workspace_id(),
            source_definition_id=source_definition_id,
            name=name,
            connection_configuration=connection_configuration,
        )

    async def create_destination_if_not_exists(
        self,
        workspace_id: str | None,
        destination_definition_id: str,
        name: str,
        connection_configuration: dict[str, Any],
    ) -> dict[str, Any]:
        existing = await self.find_destination_by_config(connection_configuration, name=name)
        if existing:
            return existing
        return await self.create_destination(
            workspace_id=workspace_id or await self._get_workspace_id(),
            destination_definition_id=destination_definition_id,
            name=name,
            connection_configuration=connection_configuration,
        )

    async def build_configured_catalog(
        self,
        source_id: str,
        selected_streams: list[dict[str, Any]],
        discovery_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Build a ConfiguredAirbyteCatalog for the given source and selected streams.

        selected_streams: list of dicts with keys like name, syncMode, destinationSyncMode,
        cursorField, primaryKey, selected.
        discovery_result: optional pre-fetched discovery result to avoid redundant API calls.
        """
        if discovery_result:
            discovery = discovery_result
        else:
            discovery = await self.discover_source_schema(source_id)
        catalog = discovery.get("catalog", {})
        available = catalog.get("streams", [])
        logger.info(
            "Building configured catalog for source %s with %d selected streams",
            source_id,
            len(selected_streams),
        )
        logger.info("Available streams from discovery: %s", available)
        logger.info("Selected streams provided: %s", selected_streams)

        # Map selections by stream name for quick lookup
        selection_by_name = {s.get("name"): s for s in selected_streams if s.get("name")}

        configured_streams: list[dict[str, Any]] = []
        for stream_entry in available:
            stream = stream_entry.get("stream", {})
            name = stream.get("name")
            sel = selection_by_name.get(name)
            if not sel:
                continue  # only include selected streams

            config: dict[str, Any] = {"selected": sel.get("selected", True)}

            # Sync mode handling with cursor fallback for incremental
            sync_mode = sel.get("syncMode")
            logger.info("Configuring stream '%s' with selected sync mode: %s", name, sync_mode)
            dest_mode = sel.get("destinationSyncMode")
            logger.info(
                "Configuring stream '%s' with selected destination sync mode: %s",
                name,
                dest_mode,
            )

            supported = stream.get("supportedSyncModes") or []
            if sync_mode and (sync_mode in supported or not supported):
                # Use the user-requested mode if it's explicitly supported,
                # or if the source didn't report any modes (let the API validate)
                config["syncMode"] = sync_mode
            elif supported:
                config["syncMode"] = supported[0]
            else:
                config["syncMode"] = sync_mode or "full_refresh"
            dest_mode = dest_mode or "append"
            config["destinationSyncMode"] = dest_mode

            # Build set of valid column names from propertyFields for validation
            property_fields = stream.get("propertyFields") or []
            valid_columns = set()
            for pf in property_fields:
                if isinstance(pf, list) and pf:
                    valid_columns.add(pf[0])
                elif isinstance(pf, str):
                    valid_columns.add(pf)

            # Propagate cursorField from user selection or source defaults
            cursor = sel.get("cursorField") or sel.get("cursor_field")
            if cursor:
                cursor_list = cursor if isinstance(cursor, list) else [cursor]
                if not valid_columns and cursor_list:
                    logger.warning(
                        "Stream '%s' has no propertyFields; cannot validate cursor field '%s'",
                        name,
                        cursor_list[0],
                    )
                # Validate cursor field against actual columns
                if valid_columns and cursor_list:
                    cursor_name = cursor_list[0]
                    if cursor_name not in valid_columns:
                        sorted_cols = sorted(valid_columns)
                        raise ValueError(
                            f"Invalid cursor field '{cursor_name}' for stream '{name}'. "
                            f"Valid columns are: {sorted_cols}. "
                            f"Please ask the user to choose a valid cursor field from the available columns."
                        )
                config["cursorField"] = cursor_list
            elif stream.get("defaultCursorField"):
                config["cursorField"] = stream.get("defaultCursorField")

            # Propagate primaryKey from user selection or source defaults
            pk = sel.get("primaryKey") or sel.get("primary_key")
            if pk:
                pk_list = pk if isinstance(pk, list) else [[pk]]
                if not valid_columns and pk_list:
                    logger.warning(
                        "Stream '%s' has no propertyFields; cannot validate primary key",
                        name,
                    )
                # Validate primary key fields against actual columns
                if valid_columns and pk_list:
                    for pk_entry in pk_list:
                        pk_name = (
                            pk_entry[0] if isinstance(pk_entry, list) and pk_entry else pk_entry
                        )
                        if isinstance(pk_name, str) and pk_name not in valid_columns:
                            sorted_cols = sorted(valid_columns)
                            raise ValueError(
                                f"Invalid primary key field '{pk_name}' for stream '{name}'. "
                                f"Valid columns are: {sorted_cols}. "
                                f"Please ask the user to choose a valid primary key from the available columns."
                            )
                config["primaryKey"] = pk_list
            elif stream.get("sourceDefinedPrimaryKey"):
                config["primaryKey"] = stream.get("sourceDefinedPrimaryKey")

            configured_streams.append({"stream": stream, "config": config})

        # Defense-in-depth: warn if any requested streams were silently dropped
        matched_names = {s["stream"]["name"] for s in configured_streams}
        requested_names = set(selection_by_name.keys())
        dropped = requested_names - matched_names
        if dropped:
            logger.warning(
                "Requested streams not found in source schema (dropped): %s. Available streams: %s",
                sorted(dropped),
                [s.get("stream", {}).get("name") for s in available],
            )

        return {"streams": configured_streams}

    async def get_source_by_name(self, name: str) -> dict[str, Any] | None:
        """
        Find a source by name in the configured workspace.

        Args:
            name: The name of the source to find.

        Returns:
            The source dictionary if found, otherwise None.
        """
        try:
            logger.debug("Searching for source '%s' in workspace '%s'", name, self.workspace_id)
            sources = await self.list_sources()
            for source in sources:
                if source.get("name", "").lower() == name.lower():
                    logger.info("Found source '%s' with ID %s", name, source["sourceId"])
                    return source
            logger.warning("Source '%s' not found in workspace '%s'", name, self.workspace_id)
            return None
        except Exception as e:
            logger.error("Failed to get source by name '%s': %s", name, e, exc_info=True)
            raise

    async def get_destination_by_name(self, name: str) -> dict[str, Any] | None:
        """
        Find a destination by name in the configured workspace.

        Args:
            name: The name of the destination to find.

        Returns:
            The destination dictionary if found, otherwise None.
        """
        try:
            logger.debug(
                "Searching for destination '%s' in workspace '%s'", name, self.workspace_id
            )
            destinations = await self.list_destinations()
            for destination in destinations:
                if destination.get("name", "").lower() == name.lower():
                    logger.info(
                        "Found destination '%s' with ID %s", name, destination["destinationId"]
                    )
                    return destination
            logger.warning("Destination '%s' not found in workspace '%s'", name, self.workspace_id)
            return None
        except Exception as e:
            logger.error("Failed to get destination by name '%s': %s", name, e, exc_info=True)
            raise
