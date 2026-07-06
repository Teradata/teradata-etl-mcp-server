"""Async Apache Airflow REST API client.

This module provides an async wrapper around the Airflow REST API
for DAG management, execution monitoring, and orchestration operations.

Designed for high-concurrency scenarios with native async/await support.

Production Features:
- Connection pooling with configurable limits
- Rate limiting to prevent API overload
- Response size limits to prevent memory exhaustion
- Circuit breaker for resilience
- Automatic retry with exponential backoff
- Token caching with TTL
"""

import asyncio
import hashlib
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from ..response_sanitizer import safe_error_message
from ..utils.circuit_breaker import CircuitBreakerBase, CircuitBreakerFactory
from ..utils.tls import build_tls_context

logger = logging.getLogger(__name__)

# Default limits for production safety
DEFAULT_MAX_CONNECTIONS = 100
DEFAULT_MAX_KEEPALIVE_CONNECTIONS = 20
DEFAULT_MAX_RESPONSE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB
DEFAULT_RATE_LIMIT_RPS = 10.0  # requests per second


class AsyncAirflowClientError(Exception):
    """Base exception for async Airflow client errors."""

    pass


class AsyncAirflowConnectionError(AsyncAirflowClientError):
    """Raised when connection to Airflow fails."""

    pass


class AsyncAirflowAPIError(AsyncAirflowClientError):
    """Raised when Airflow API returns an error."""

    pass


class CircuitBreakerOpen(AsyncAirflowClientError):
    """Raised when circuit breaker is open and requests are blocked."""

    pass


class RateLimitExceeded(AsyncAirflowClientError):
    """Raised when rate limit is exceeded."""

    pass


class ResponseTooLarge(AsyncAirflowClientError):
    """Raised when response exceeds size limit."""

    pass


RECOMMENDED_AIRFLOW_PROVIDERS: dict[str, str] = {
    "apache-airflow-providers-ssh": "DAG deployment via SSH/SFTP",
    "apache-airflow-providers-airbyte": "Airbyte-based data sources (not needed for file or Teradata sources)",
    "apache-airflow-providers-teradata": "Teradata source/target operators",
}


def check_missing_providers(providers_response: dict[str, Any]) -> list[tuple[str, str]]:
    """Return (package_name, purpose) for each recommended provider not in the response.

    Args:
        providers_response: Response from the /providers endpoint containing
            a list of installed provider dicts with 'package_name' field.

    Returns:
        List of (package_name, purpose) tuples for recommended providers that are not installed.
    """
    installed = {(p.get("package_name") or "").replace("-", "_") for p in providers_response.get("providers", [])}
    return [
        (name, purpose)
        for name, purpose in RECOMMENDED_AIRFLOW_PROVIDERS.items()
        if name.replace("-", "_") not in installed
    ]


@dataclass
class RateLimiter:
    """Token bucket rate limiter for controlling request rate.

    Thread-safe implementation using asyncio locks.
    """

    rate: float  # requests per second
    burst: int = 10  # max burst size

    _tokens: float = field(init=False)
    _last_update: float = field(init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def __post_init__(self) -> None:
        self._tokens = float(self.burst)
        self._last_update = time.time()

    async def acquire(self, timeout: float = 30.0) -> bool:
        """Acquire a token, waiting if necessary.

        Args:
            timeout: Maximum time to wait for a token

        Returns:
            True if token acquired, False if timeout

        Raises:
            RateLimitExceeded: If cannot acquire within timeout
        """
        start_time = time.time()
        while True:
            async with self._lock:
                now = time.time()
                # Add tokens based on elapsed time
                elapsed = now - self._last_update
                self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
                self._last_update = now

                if self._tokens >= 1:
                    self._tokens -= 1
                    return True

            # Check timeout before sleeping
            elapsed_total = time.time() - start_time
            if elapsed_total >= timeout:
                raise RateLimitExceeded(
                    f"Rate limit exceeded: could not acquire token within {timeout}s"
                )

            # Calculate sleep time, capped by remaining timeout
            sleep_time = min(1.0 / max(self.rate, 0.001), timeout - elapsed_total, 1.0)
            if sleep_time <= 0:
                raise RateLimitExceeded(
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


@dataclass
class TokenCacheEntry:
    """Token cache entry with TTL support."""

    token: str
    expires_at: float  # Unix timestamp
    created_at: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        """Check if token has expired."""
        return time.time() >= self.expires_at

    @property
    def time_until_expiry(self) -> float:
        """Seconds until token expires (negative if already expired)."""
        return self.expires_at - time.time()


class AsyncAirflowClient:
    """
    Async Apache Airflow REST API client.

    Provides async methods for DAG management, execution monitoring,
    task operations, and general Airflow orchestration.

    Features:
    - Native async/await support with httpx.AsyncClient
    - Connection pooling with configurable limits
    - Rate limiting to prevent API overload
    - Response size limits to prevent memory exhaustion
    - Circuit breaker pattern for resilience
    - Token caching with TTL
    - Automatic retry with exponential backoff
    - Idempotent DAG triggers
    """

    # Class-level token cache with TTL and per-key locks.
    # Async locks are lazily created to avoid binding to an event loop at import time.
    # _GUARD_INIT_LOCK is a threading.Lock used to atomically create the async guard.
    _TOKEN_CACHE: dict[tuple, TokenCacheEntry] = {}
    _TOKEN_CACHE_LOCKS: dict[tuple, asyncio.Lock] = {}
    _TOKEN_CACHE_LOCKS_GUARD: asyncio.Lock | None = None
    _TOKEN_CACHE_LOCKS_GUARD_LOOP_ID: int | None = None
    _GUARD_INIT_LOCK = threading.Lock()
    DEFAULT_TOKEN_TTL_SECONDS: float = 55 * 60  # 55 minutes

    def __init__(
        self,
        base_url: str,
        username: str | None = None,
        password: str | None = None,
        timeout: int = 30,
        *,
        auth_manager: str = "basic",
        token_endpoint: str = "auth/token",  # noqa: S107 - not a password
        max_page_limit: int = 100,
        retry_attempts: int = 2,
        retry_backoff: float = 0.5,
        circuit_breaker_enabled: bool = True,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0,
        redis_url: str | None = None,
        # Production hardening options
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        max_keepalive_connections: int = DEFAULT_MAX_KEEPALIVE_CONNECTIONS,
        max_response_size_bytes: int = DEFAULT_MAX_RESPONSE_SIZE_BYTES,
        rate_limit_rps: float | None = DEFAULT_RATE_LIMIT_RPS,
        rate_limit_burst: int = 10,
    ):
        """
        Initialize async Airflow client.

        Args:
            base_url: Airflow API base URL (e.g., http://localhost:8080)
            username: Airflow username
            password: Airflow password
            timeout: Request timeout in seconds
            auth_manager: Authentication manager ("basic" supported)
            token_endpoint: Endpoint for token acquisition
            max_page_limit: Upper bound for page size
            retry_attempts: Number of retry attempts for transient errors
            retry_backoff: Base backoff time in seconds for retries
            circuit_breaker_enabled: Enable circuit breaker for resilience
            circuit_breaker_threshold: Failures before opening circuit
            circuit_breaker_timeout: Seconds before attempting recovery
            redis_url: Optional Redis URL for distributed circuit breaker
            max_connections: Maximum concurrent connections (default 100)
            max_keepalive_connections: Maximum keepalive connections (default 20)
            max_response_size_bytes: Maximum response size in bytes (default 10MB)
            rate_limit_rps: Rate limit in requests per second (None to disable)
            rate_limit_burst: Maximum burst size for rate limiting
        """
        if not base_url:
            raise ValueError("AsyncAirflowClient requires `base_url`.")
        if not (base_url.startswith("http://") or base_url.startswith("https://")):
            raise ValueError("`base_url` must start with http:// or https://")

        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self.auth_manager = auth_manager
        self.token_endpoint = token_endpoint
        self.max_page_limit = max_page_limit
        self.retry_attempts = max(0, int(retry_attempts))
        self.retry_backoff = max(0.0, float(retry_backoff))

        # Production hardening settings
        self.max_connections = max_connections
        self.max_keepalive_connections = max_keepalive_connections
        self.max_response_size_bytes = max_response_size_bytes

        # Rate limiter
        self._rate_limiter: RateLimiter | None = None
        if rate_limit_rps and rate_limit_rps > 0:
            self._rate_limiter = RateLimiter(rate=rate_limit_rps, burst=rate_limit_burst)
            logger.info(
                "Rate limiting enabled: %.1f rps, burst=%d",
                rate_limit_rps,
                rate_limit_burst,
            )

        # Circuit breaker (in-memory or Redis-backed)
        self._circuit_breaker_enabled = circuit_breaker_enabled
        self._circuit_breaker: CircuitBreakerBase | None = None
        if circuit_breaker_enabled:
            self._circuit_breaker = CircuitBreakerFactory.create(
                name=f"airflow_{base_url.replace('://', '_').replace('/', '_')}",
                redis_url=redis_url,
                failure_threshold=circuit_breaker_threshold,
                recovery_timeout=circuit_breaker_timeout,
            )

        self._client: httpx.AsyncClient | None = None
        self._client_init_lock: asyncio.Lock | None = None
        self._client_init_lock_loop_id: int | None = None
        self._access_token: str | None = None
        self._resolved_api_version: str | None = None

        logger.info(
            "Initialized AsyncAirflowClient for %s (circuit_breaker=%s, rate_limit=%s)",
            base_url,
            circuit_breaker_enabled,
            f"{rate_limit_rps}rps" if rate_limit_rps else "disabled",
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client with connection pooling."""
        if self._client is not None:
            return self._client

        # Lazily create the init lock; recreate when the event loop changes
        # (asyncio.Lock is bound to the loop it was created in).
        loop_id = id(asyncio.get_running_loop())
        if self._client_init_lock is None or self._client_init_lock_loop_id != loop_id:
            self._client_init_lock = asyncio.Lock()
            self._client_init_lock_loop_id = loop_id

        async with self._client_init_lock:
            if self._client is not None:  # double-check after acquiring lock
                return self._client

            # Detect API version
            await self._detect_api_version()

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            # Configure connection limits for production safety
            limits = httpx.Limits(
                max_connections=self.max_connections,
                max_keepalive_connections=self.max_keepalive_connections,
            )

            # Verify certificates and enforce TLS 1.2+ when the base URL uses HTTPS
            client_kwargs: dict[str, Any] = {
                "base_url": self.base_url,
                "headers": headers,
                "timeout": self.timeout,
                "verify": build_tls_context(),
                "follow_redirects": True,
                "limits": limits,
            }

            # Auth strategy based on API version
            if (self._resolved_api_version or "v1") == "v2":
                await self._ensure_token()
                if self._access_token:
                    headers["Authorization"] = f"Bearer {self._access_token}"
            else:
                if self.username and self.password:
                    client_kwargs["auth"] = (self.username, self.password)

            self._client = httpx.AsyncClient(**client_kwargs)
            logger.debug(
                "Created HTTP client with limits: max_conn=%d, keepalive=%d",
                self.max_connections,
                self.max_keepalive_connections,
            )

        return self._client

    async def _detect_api_version(self) -> None:
        """Detect Airflow API version."""
        if self._resolved_api_version:
            return

        try:
            async with httpx.AsyncClient(timeout=10, verify=build_tls_context()) as temp_client:
                # Try v2 endpoint first
                try:
                    resp = await temp_client.get(f"{self.base_url}/api/v2/version")
                    if resp.status_code == 200:
                        self._resolved_api_version = "v2"
                        logger.info("Detected Airflow API v2")
                        return
                except Exception:
                    logger.debug("v2 API not available, trying v1")

                # Fallback to v1
                try:
                    resp = await temp_client.get(f"{self.base_url}/api/v1/version")
                    if resp.status_code == 200:
                        self._resolved_api_version = "v1"
                        logger.info("Detected Airflow API v1")
                        return
                except Exception:
                    logger.debug("v1 API endpoint check failed")

                # Default to v1
                self._resolved_api_version = "v1"
                logger.warning("Could not detect API version, defaulting to v1")
        except Exception as e:
            logger.warning("API version detection failed: %s, defaulting to v1", e)
            self._resolved_api_version = "v1"

    async def _get_token_lock(self) -> asyncio.Lock:
        """Get or create a per-key lock for token acquisition.

        Keys include the running event loop id so that locks created in one
        loop are never reused in another (asyncio.Lock is loop-bound).
        The guard lock is lazily created on first use and recreated when the
        event loop changes, tracked via _TOKEN_CACHE_LOCKS_GUARD_LOOP_ID
        (avoids relying on private asyncio internals).
        """
        loop_id = id(asyncio.get_running_loop())
        key = (loop_id, self.base_url, self.username)
        lock = self._TOKEN_CACHE_LOCKS.get(key)
        if lock is None:
            # Atomically create/recreate the async guard lock using a threading lock
            if (
                self._TOKEN_CACHE_LOCKS_GUARD is None
                or loop_id != self._TOKEN_CACHE_LOCKS_GUARD_LOOP_ID
            ):
                with self._GUARD_INIT_LOCK:
                    if (
                        self._TOKEN_CACHE_LOCKS_GUARD is None
                        or loop_id != self._TOKEN_CACHE_LOCKS_GUARD_LOOP_ID
                    ):
                        AsyncAirflowClient._TOKEN_CACHE_LOCKS_GUARD = asyncio.Lock()
                        AsyncAirflowClient._TOKEN_CACHE_LOCKS_GUARD_LOOP_ID = loop_id
            async with self._TOKEN_CACHE_LOCKS_GUARD:
                # Double-check after acquiring guard
                lock = self._TOKEN_CACHE_LOCKS.get(key)
                if lock is None:
                    lock = asyncio.Lock()
                    self._TOKEN_CACHE_LOCKS[key] = lock
        return lock

    async def _ensure_token(self) -> None:
        """Ensure access token is available for v2 API."""
        # Fast path: token already set (no lock needed)
        if self._access_token:
            return

        lock = await self._get_token_lock()
        async with lock:
            # Double-check after acquiring lock (another coroutine may have set it)
            if self._access_token:
                return

            # Check class-level cache
            key = (self.base_url, self.username)
            cached = self._TOKEN_CACHE.get(key)
            if cached and not cached.is_expired:
                self._access_token = cached.token
                logger.debug("Using cached token (expires in %.0fs)", cached.time_until_expiry)
                return

            # Obtain new token while holding per-key lock to prevent duplicate requests
            await self._obtain_token()

    async def _obtain_token(self) -> None:
        """Obtain authentication token from Airflow.

        Caller must hold the per-key token lock obtained via _get_token_lock()
        to prevent concurrent token requests for the same (base_url, username).
        """
        if not (self.username and self.password):
            raise AsyncAirflowClientError("Username and password required for token auth")

        try:
            async with httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                verify=build_tls_context(),
            ) as temp_client:
                # Airflow 3 uses form-urlencoded data for /auth/token
                # Airflow 2 may use JSON - try form first, then JSON
                resp = await temp_client.post(
                    self.token_endpoint,
                    data={"username": self.username, "password": self.password},
                )
                if resp.status_code == 415:  # Unsupported Media Type - try JSON
                    resp = await temp_client.post(
                        self.token_endpoint,
                        json={"username": self.username, "password": self.password},
                    )
                resp.raise_for_status()
                data = resp.json()
                token = data.get("access_token") or data.get("token")
                if not token:
                    raise AsyncAirflowAPIError("Token missing in response")

                self._access_token = token

                # Cache with TTL (lock already held by caller)
                key = (self.base_url, self.username)
                self._TOKEN_CACHE[key] = TokenCacheEntry(
                    token=token,
                    expires_at=time.time() + self.DEFAULT_TOKEN_TTL_SECONDS,
                )

                logger.info("Obtained auth token (TTL: %ds)", self.DEFAULT_TOKEN_TTL_SECONDS)

        except httpx.HTTPStatusError as e:
            raise AsyncAirflowAPIError(f"Token request failed: {e.response.status_code}") from e
        except httpx.RequestError as e:
            raise AsyncAirflowConnectionError(f"Cannot reach auth endpoint: {e}") from e

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Make async HTTP request with rate limiting, circuit breaker, and retry logic.

        Args:
            method: HTTP method (GET, POST, PATCH, DELETE)
            endpoint: API endpoint path
            **kwargs: Additional arguments for httpx request

        Returns:
            Response JSON as dictionary

        Raises:
            CircuitBreakerOpen: If circuit breaker is open
            RateLimitExceeded: If rate limit cannot be satisfied
            ResponseTooLarge: If response exceeds size limit
            AsyncAirflowAPIError: If API returns an error
            AsyncAirflowConnectionError: If connection fails
        """
        # Check circuit breaker first
        if self._circuit_breaker and not self._circuit_breaker.is_available:
            status = self._circuit_breaker.get_status()
            raise CircuitBreakerOpen(
                f"Circuit breaker is OPEN. Recovery in {status.get('time_until_recovery', 'unknown')}s"
            )

        # Apply rate limiting
        if self._rate_limiter:
            await self._rate_limiter.acquire(timeout=self.timeout)

        # Ensure client is initialized (this triggers API version detection)
        client = await self._get_client()

        # Construct URL with correct API version
        api_prefix = "/api/v2" if self._resolved_api_version == "v2" else "/api/v1"
        url = f"{api_prefix}{endpoint}"

        attempts = 0
        token_refreshed = False

        while True:
            try:
                if method.upper() == "GET":
                    response = await client.get(url, **kwargs)
                elif method.upper() == "POST":
                    response = await client.post(url, **kwargs)
                elif method.upper() == "PATCH":
                    response = await client.patch(url, **kwargs)
                elif method.upper() == "DELETE":
                    response = await client.delete(url, **kwargs)
                else:
                    response = await client.request(method, url, **kwargs)

                response.raise_for_status()

                # Check response size before parsing
                content_length = len(response.content)
                if content_length > self.max_response_size_bytes:
                    raise ResponseTooLarge(
                        f"Response size {content_length} bytes exceeds limit "
                        f"{self.max_response_size_bytes} bytes"
                    )

                # Record success
                if self._circuit_breaker:
                    self._circuit_breaker.record_success()

                if response.content:
                    return response.json()
                return {}

            except ResponseTooLarge:
                # Don't retry or record as circuit breaker failure
                raise

            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code if e.response else None

                # Handle 401/403 - try token refresh (once per request)
                # For 403, only refresh when Airflow signals an invalid/expired JWT
                # (e.g. after a server restart); genuine permission denials should
                # propagate immediately without hitting the auth endpoint.
                is_jwt_403 = False
                if status_code == 403:
                    response_text = (getattr(e.response, "text", None) or "")[:512]
                    is_jwt_403 = "jwt" in response_text.lower()
                if (
                    (status_code == 401 or is_jwt_403)
                    and self._resolved_api_version == "v2"
                    and not token_refreshed
                ):
                    try:
                        stale_token = self._access_token
                        lock = await self._get_token_lock()
                        async with lock:
                            # Double-check: another coroutine may have already refreshed
                            if self._access_token != stale_token and self._access_token:
                                pass  # Token already refreshed by another coroutine
                            else:
                                self._access_token = None
                                self._TOKEN_CACHE.pop((self.base_url, self.username), None)
                                # Leave the stale Authorization header in place while
                                # _obtain_token() is in flight.  Clearing it here would
                                # create a window where concurrent _make_request calls
                                # send requests with *no* token at all, causing avoidable
                                # 401s.  The header is only cleared in the except block
                                # when re-auth itself fails.
                                await self._obtain_token()
                        # Update client headers with the freshly obtained token
                        if self._client:
                            self._client.headers["Authorization"] = f"Bearer {self._access_token}"
                        token_refreshed = True
                        continue
                    except Exception as token_err:
                        logger.debug("Token refresh failed: %s", token_err)
                        # Re-auth failed: now remove the stale header so subsequent
                        # requests do not keep sending an invalid token.
                        if self._client:
                            self._client.headers.pop("Authorization", None)

                # Handle 429 Too Many Requests
                if status_code == 429:
                    retry_after = e.response.headers.get("Retry-After", "60")
                    try:
                        wait_time = float(retry_after)
                    except ValueError:
                        wait_time = 60.0

                    if attempts < self.retry_attempts:
                        logger.warning(
                            "Rate limited (429), waiting %.1fs before retry (attempt %d/%d)",
                            wait_time,
                            attempts + 1,
                            self.retry_attempts,
                        )
                        await asyncio.sleep(min(wait_time, 120.0))  # Cap at 2 minutes
                        attempts += 1
                        continue
                    else:
                        raise RateLimitExceeded(
                            f"Rate limited by server after {attempts} retries"
                        ) from e

                # Retry on transient errors
                if status_code in (502, 503, 504) and attempts < self.retry_attempts:
                    sleep_time = self.retry_backoff * (2**attempts)
                    logger.warning(
                        "Transient error %s, retrying in %.2fs (attempt %d/%d)",
                        status_code,
                        sleep_time,
                        attempts + 1,
                        self.retry_attempts,
                    )
                    await asyncio.sleep(sleep_time)
                    attempts += 1
                    continue

                # Record failure for circuit breaker only on server errors (5xx).
                # Client errors (4xx) indicate config/auth/request issues, not
                # service degradation, and should not open the circuit.
                if self._circuit_breaker and status_code is not None and status_code >= 500:
                    self._circuit_breaker.record_failure()

                error_msg = f"Airflow API error: {status_code}"
                try:
                    error_detail = e.response.json()
                    error_msg += f" - {error_detail.get('detail', str(error_detail))}"
                except Exception:
                    if e.response:
                        error_msg += f" - {e.response.text[:200]}"

                raise AsyncAirflowAPIError(error_msg) from e

            except httpx.RequestError as e:
                if self._circuit_breaker:
                    self._circuit_breaker.record_failure()
                raise AsyncAirflowConnectionError(f"Connection error: {e}") from e

    # ==================== Health & Status ====================

    async def test_connection(self) -> dict[str, Any]:
        """Test connection to Airflow API."""
        try:
            # Ensure API version is detected first by initializing the client
            await self._get_client()  # This triggers _detect_api_version()

            # Airflow 3 (v2 API) uses /monitor/health, Airflow 2 (v1 API) uses /health
            health_endpoint = "/monitor/health" if self._resolved_api_version == "v2" else "/health"
            result = await self._make_request("GET", health_endpoint)
            return {
                "connected": True,
                "url": self.base_url,
                "version": self._resolved_api_version,
                "health": result,
            }
        except Exception as e:
            return {
                "connected": False,
                "url": self.base_url,
                "error": safe_error_message(e),
            }

    def get_circuit_breaker_status(self) -> dict[str, Any] | None:
        """Get circuit breaker status."""
        if self._circuit_breaker:
            return self._circuit_breaker.get_status()
        return None

    def reset_circuit_breaker(self) -> bool:
        """Reset circuit breaker to closed state."""
        if self._circuit_breaker:
            self._circuit_breaker.reset()
            return True
        return False

    def get_rate_limiter_status(self) -> dict[str, Any] | None:
        """Get rate limiter status."""
        if self._rate_limiter:
            return self._rate_limiter.get_status()
        return None

    def get_client_status(self) -> dict[str, Any]:
        """Get comprehensive client status for monitoring.

        Returns:
            Dictionary with all client configuration and status
        """
        return {
            "base_url": self.base_url,
            "api_version": self._resolved_api_version,
            "timeout_seconds": self.timeout,
            "tls_min_version": "TLSv1_2",
            "max_connections": self.max_connections,
            "max_keepalive_connections": self.max_keepalive_connections,
            "max_response_size_mb": self.max_response_size_bytes / (1024 * 1024),
            "retry_attempts": self.retry_attempts,
            "retry_backoff": self.retry_backoff,
            "circuit_breaker": self.get_circuit_breaker_status(),
            "rate_limiter": self.get_rate_limiter_status(),
        }

    # ==================== Health & Version ====================

    async def get_health(self) -> dict[str, Any]:
        """Get Airflow health status.

        Note: For API v2, the health endpoint is /monitor/health.
        For API v1, it's /health.
        """
        # Ensure API version is detected before choosing endpoint
        await self._get_client()
        health_endpoint = "/monitor/health" if self._resolved_api_version == "v2" else "/health"
        return await self._make_request("GET", health_endpoint)

    async def get_version(self) -> dict[str, Any]:
        """Get Airflow version info."""
        return await self._make_request("GET", "/version")

    async def get_providers(self) -> dict[str, Any]:
        """Get list of installed Airflow providers via the REST API.

        Returns:
            Dictionary with 'providers' list (each with 'package_name', 'description', 'version', etc.)
            and 'total_entries' count.

        Raises:
            AsyncAirflowAPIError: If the providers endpoint returns an error (e.g., 404 on older Airflow).
            AsyncAirflowConnectionError: If the request fails at the connection level.
        """
        page_size = self.max_page_limit
        first = await self._make_request("GET", "/providers", params={"limit": page_size, "offset": 0})
        all_providers = list(first.get("providers", []))
        total = first.get("total_entries", len(all_providers))
        offset = len(all_providers)
        while offset < total:
            page = await self._make_request("GET", "/providers", params={"limit": page_size, "offset": offset})
            batch = page.get("providers", [])
            if not batch:
                logger.warning("Provider discovery incomplete: got %d of %d providers before empty page", len(all_providers), total)
                return {"providers": all_providers, "total_entries": total, "incomplete": True}
            all_providers.extend(batch)
            offset += len(batch)
        return {"providers": all_providers, "total_entries": total}

    # ==================== Pool Operations ====================

    async def list_pools(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List Airflow pools.

        Args:
            limit: Maximum number of pools to return.
            offset: Offset for pagination.

        Returns:
            List of pool dictionaries with name, slots, etc.
        """
        params: dict[str, Any] = {
            "limit": min(limit, self.max_page_limit),
            "offset": offset,
        }
        result = await self._make_request("GET", "/pools", params=params)
        return result.get("pools", [])

    async def list_connections(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List Airflow connections.

        Args:
            limit: Maximum number of connections to return.
            offset: Offset for pagination.

        Returns:
            List of connection dictionaries.
        """
        params: dict[str, Any] = {
            "limit": min(limit, self.max_page_limit),
            "offset": offset,
        }
        result = await self._make_request("GET", "/connections", params=params)
        return result.get("connections", [])

    # ==================== DAG Operations ====================

    async def list_dags(
        self,
        limit: int = 100,
        offset: int = 0,
        tags: list[str] | None = None,
        only_active: bool = True,
    ) -> list[dict[str, Any]]:
        """List DAGs from Airflow."""
        params: dict[str, Any] = {
            "limit": min(limit, self.max_page_limit),
            "offset": offset,
            "only_active": str(only_active).lower(),
        }
        if tags:
            params["tags"] = ",".join(tags)

        result = await self._make_request("GET", "/dags", params=params)
        return result.get("dags", [])

    async def get_dag(self, dag_id: str) -> dict[str, Any]:
        """Get DAG details."""
        return await self._make_request("GET", f"/dags/{dag_id}")

    async def pause_dag(self, dag_id: str, is_paused: bool = True) -> dict[str, Any]:
        """Pause or unpause a DAG."""
        return await self._make_request(
            "PATCH",
            f"/dags/{dag_id}",
            json={"is_paused": is_paused},
        )

    async def unpause_dag(self, dag_id: str) -> dict[str, Any]:
        """Unpause a DAG (convenience method)."""
        return await self.pause_dag(dag_id, is_paused=False)

    async def trigger_dag(
        self,
        dag_id: str,
        conf: dict[str, Any] | None = None,
        logical_date: str | None = None,
        dag_run_id: str | None = None,
        max_retries: int = 12,
        retry_delay: float = 5.0,
        unpause_before_trigger: bool = True,
    ) -> dict[str, Any]:
        """
        Trigger a DAG run.

        Args:
            dag_id: DAG identifier
            conf: Optional configuration to pass to DAG
            logical_date: Optional logical date
            dag_run_id: Optional custom run ID
            max_retries: Max retries for 404 errors (default 12 = ~90s with backoff)
            retry_delay: Initial delay between retries (uses exponential backoff)
            unpause_before_trigger: If True, unpause the DAG before triggering
                (newly deployed DAGs are paused by default)

        Returns:
            DAG run information
        """
        # Unpause the DAG first if requested (new DAGs are paused by default)
        if unpause_before_trigger:
            try:
                dag_info = await self.get_dag(dag_id)
                if dag_info.get("is_paused", False):
                    logger.info("Unpausing DAG %s before trigger", dag_id)
                    await self.pause_dag(dag_id, is_paused=False)
            except AsyncAirflowAPIError as e:
                # DAG might not exist yet, will be handled by retry loop below
                if "404" not in str(e):
                    logger.warning("Could not check/unpause DAG %s: %s", dag_id, e)

        if not logical_date:
            logical_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        payload: dict[str, Any] = {
            "conf": conf or {},
            "logical_date": logical_date,
        }
        if dag_run_id:
            payload["dag_run_id"] = dag_run_id

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                result = await self._make_request("POST", f"/dags/{dag_id}/dagRuns", json=payload)
                logger.info("Triggered DAG: %s, run_id: %s", dag_id, result.get("dag_run_id"))
                return result

            except AsyncAirflowAPIError as e:
                last_error = e
                if "404" in str(e) and attempt < max_retries:
                    # Exponential backoff: 5s, 7.5s, 11.25s, ... capped at 15s
                    current_delay = min(retry_delay * (1.5**attempt), 15.0)
                    logger.warning(
                        "DAG %s not ready (attempt %d/%d), retrying in %.1fs...",
                        dag_id,
                        attempt + 1,
                        max_retries + 1,
                        current_delay,
                    )
                    await asyncio.sleep(current_delay)
                    # Try to unpause again in case DAG just became available
                    if unpause_before_trigger:
                        try:
                            await self.pause_dag(dag_id, is_paused=False)
                        except Exception:
                            pass  # Ignore, will retry
                    logical_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    payload["logical_date"] = logical_date
                else:
                    raise

        raise last_error  # type: ignore

    async def trigger_dag_idempotent(
        self,
        dag_id: str,
        idempotency_key: str,
        conf: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Trigger DAG with idempotency guarantee.

        Uses deterministic dag_run_id to prevent duplicate runs.

        Args:
            dag_id: DAG identifier
            idempotency_key: Unique key for this request
            conf: Optional configuration

        Returns:
            DAG run info with 'idempotent_reused' flag
        """
        # Generate deterministic run ID
        hash_input = f"{dag_id}:{idempotency_key}"
        hash_suffix = hashlib.sha256(hash_input.encode()).hexdigest()[:16]
        dag_run_id = f"idem_{dag_id}_{hash_suffix}"

        # Check if run already exists
        try:
            existing = await self.get_dag_run(dag_id, dag_run_id)
            if existing:
                logger.info("Reusing existing idempotent run: %s", dag_run_id)
                existing["idempotent_reused"] = True
                return existing
        except AsyncAirflowAPIError:
            pass  # Run doesn't exist, create it

        # Create new run
        result = await self.trigger_dag(
            dag_id=dag_id,
            conf=conf,
            dag_run_id=dag_run_id,
        )
        result["idempotent_reused"] = False
        return result

    # ==================== DAG Run Operations ====================

    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        """Get details of a specific DAG run."""
        return await self._make_request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}")

    async def list_dag_runs(
        self,
        dag_id: str,
        limit: int = 25,
        offset: int = 0,
        state: str | None = None,
        execution_date_gte: str | None = None,
        execution_date_lte: str | None = None,
        order_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """List DAG runs.

        Args:
            dag_id: DAG identifier
            limit: Maximum number of runs to return
            offset: Offset for pagination
            state: Filter by run state
            execution_date_gte: Filter runs with execution_date >= this value (ISO 8601)
            execution_date_lte: Filter runs with execution_date <= this value (ISO 8601)
            order_by: Sort field (prefix with - for descending, e.g. '-execution_date')
        """
        params: dict[str, Any] = {
            "limit": min(limit, self.max_page_limit),
            "offset": offset,
        }
        if state:
            params["state"] = state
        if execution_date_gte:
            params["execution_date_gte"] = execution_date_gte
        if execution_date_lte:
            params["execution_date_lte"] = execution_date_lte
        if order_by:
            params["order_by"] = order_by

        result = await self._make_request("GET", f"/dags/{dag_id}/dagRuns", params=params)
        return result.get("dag_runs", [])

    async def get_dag_run_status(
        self,
        dag_id: str,
        dag_run_id: str,
    ) -> dict[str, Any]:
        """Get DAG run status with task summary."""
        # Get run details
        run = await self.get_dag_run(dag_id, dag_run_id)

        # Get task instances
        tasks = await self.list_task_instances(dag_id, dag_run_id)

        # Summarize task states
        task_summary: dict[str, int] = {}
        for task in tasks:
            state = task.get("state", "unknown")
            task_summary[state] = task_summary.get(state, 0) + 1

        return {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "state": run.get("state"),
            "execution_date": run.get("execution_date") or run.get("logical_date"),
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
            "task_summary": task_summary,
            "total_tasks": len(tasks),
        }

    async def update_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
        state: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """
        Update a DAG run's state or note.

        Args:
            dag_id: DAG identifier
            dag_run_id: DAG run identifier
            state: New state (e.g., 'failed', 'success', 'queued')
            note: Optional note to attach to the run

        Returns:
            Updated DAG run details
        """
        payload: dict[str, Any] = {}
        if state:
            payload["state"] = state
        if note is not None:
            payload["note"] = note

        if not payload:
            raise ValueError("At least one of 'state' or 'note' must be provided")

        result = await self._make_request(
            "PATCH",
            f"/dags/{dag_id}/dagRuns/{dag_run_id}",
            json=payload,
        )
        logger.info("Updated DAG run %s/%s: %s", dag_id, dag_run_id, payload)
        return result

    async def set_dag_run_state(
        self,
        dag_id: str,
        dag_run_id: str,
        state: str,
    ) -> dict[str, Any]:
        """
        Set the state of a DAG run.

        Commonly used to mark a run as failed (cancel) or success.

        Args:
            dag_id: DAG identifier
            dag_run_id: DAG run identifier
            state: New state ('failed', 'success', 'queued')

        Returns:
            Updated DAG run details
        """
        return await self.update_dag_run(dag_id, dag_run_id, state=state)

    async def clear_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
        dry_run: bool = False,
        reset_dag_runs: bool = True,
        only_failed: bool = True,
        task_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Clear task instances in a DAG run to retry them.

        This is the standard way to retry failed tasks in Airflow.

        Args:
            dag_id: DAG identifier
            dag_run_id: DAG run identifier
            dry_run: If True, only return what would be cleared
            reset_dag_runs: Reset the DAG run state to 'queued'
            only_failed: Only clear failed task instances

        Returns:
            List of cleared task instances
        """
        # Get the DAG run to find the execution date
        dag_run = await self.get_dag_run(dag_id, dag_run_id)
        execution_date = dag_run.get("logical_date") or dag_run.get("execution_date")

        if not execution_date:
            raise AsyncAirflowAPIError(f"Cannot determine execution date for DAG run {dag_run_id}")

        payload: dict[str, Any] = {
            "dry_run": dry_run,
            "dag_run_id": dag_run_id,
            "reset_dag_runs": reset_dag_runs,
            "only_failed": only_failed,
        }
        if task_ids:
            payload["task_ids"] = task_ids

        result = await self._make_request(
            "POST",
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )

        logger.info(
            "Cleared DAG run %s/%s (dry_run=%s, only_failed=%s): %d task(s)",
            dag_id,
            dag_run_id,
            dry_run,
            only_failed,
            len(result.get("task_instances", [])),
        )
        return result

    async def delete_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
    ) -> dict[str, Any]:
        """
        Delete a DAG run.

        Args:
            dag_id: DAG identifier
            dag_run_id: DAG run identifier

        Returns:
            Empty dict on success
        """
        await self._make_request("DELETE", f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        logger.info("Deleted DAG run %s/%s", dag_id, dag_run_id)
        return {"deleted": True, "dag_id": dag_id, "dag_run_id": dag_run_id}

    # ==================== Task Operations ====================

    async def list_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List task instances for a DAG run."""
        params = {"limit": min(limit, self.max_page_limit)}
        result = await self._make_request(
            "GET",
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            params=params,
        )
        return result.get("task_instances", [])

    async def get_task_instance(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
    ) -> dict[str, Any]:
        """Get task instance details."""
        return await self._make_request(
            "GET",
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}",
        )

    async def get_task_logs(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        task_try_number: int = 1,
    ) -> str:
        """Get task instance logs."""
        try:
            result = await self._make_request(
                "GET",
                f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}",
            )
            return result.get("content", "")
        except Exception as e:
            logger.warning("Failed to get task logs: %s", e)
            return ""

    # ==================== Connection Operations ====================

    async def get_connection(self, conn_id: str) -> dict[str, Any]:
        """Get connection details."""
        return await self._make_request("GET", f"/connections/{conn_id}")

    async def create_connection(
        self,
        conn_id: str,
        conn_type: str,
        host: str | None = None,
        port: int | None = None,
        login: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a new connection."""
        payload: dict[str, Any] = {
            "connection_id": conn_id,
            "conn_type": conn_type,
        }
        if host:
            payload["host"] = host
        if port:
            payload["port"] = port
        if login:
            payload["login"] = login
        if password:
            payload["password"] = password
        if schema:
            payload["schema"] = schema
        if extra:
            import json

            payload["extra"] = json.dumps(extra)

        return await self._make_request("POST", "/connections", json=payload)

    async def update_connection(self, conn_id: str, **kwargs: Any) -> dict[str, Any]:
        """
        Update an existing connection.

        Args:
            conn_id: Connection identifier
            **kwargs: Connection fields to update (conn_type, host, port, login, password, etc.)

        Returns:
            Updated connection information
        """
        return await self._make_request("PATCH", f"/connections/{conn_id}", json=kwargs)

    async def delete_connection(self, conn_id: str) -> dict[str, Any]:
        """Delete a connection."""
        return await self._make_request("DELETE", f"/connections/{conn_id}")

    async def get_variable(self, key: str) -> dict[str, Any]:
        """Get an Airflow variable by key."""
        return await self._make_request("GET", f"/variables/{key}")

    async def set_variable(self, key: str, value: str, description: str = "") -> dict[str, Any]:
        """Create or update an Airflow variable."""
        try:
            await self.get_variable(key)
            return await self._make_request("PATCH", f"/variables/{key}", json={"key": key, "value": value, "description": description})
        except Exception:
            return await self._make_request("POST", "/variables", json={"key": key, "value": value, "description": description})

    async def test_airflow_connection(
        self,
        connection_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Test an Airflow connection using POST /connections/test.

        Args:
            connection_payload: Full connection object to send as the request body.

        Returns:
            Test result dictionary with status and message
        """
        return await self._make_request("POST", "/connections/test", json=connection_payload)

    # ==================== DAG Lifecycle ====================

    async def delete_dag(self, dag_id: str) -> bool:
        """
        Delete a DAG.

        Args:
            dag_id: DAG identifier

        Returns:
            True if successful
        """
        await self._make_request("DELETE", f"/dags/{dag_id}")
        logger.info("Deleted DAG: %s", dag_id)
        return True

    async def wait_for_dag(
        self,
        dag_id: str,
        max_wait_seconds: int = 60,
        poll_interval: float = 2.0,
    ) -> dict[str, Any]:
        """
        Wait for a DAG to appear in Airflow after deployment.

        Polls the Airflow API until the DAG is discovered or timeout occurs.
        Useful after deploying a new DAG file to allow Airflow's scheduler
        to parse and register the DAG.

        Args:
            dag_id: DAG identifier to wait for
            max_wait_seconds: Maximum time to wait in seconds (default: 60)
            poll_interval: Time between polling attempts in seconds (default: 2)

        Returns:
            Dictionary with status and DAG information if found

        Raises:
            TimeoutError: If DAG doesn't appear within max_wait_seconds
        """
        logger.info("Waiting for DAG '%s' to appear in Airflow (max %ds)", dag_id, max_wait_seconds)

        start_time = time.monotonic()
        attempts = 0

        while (time.monotonic() - start_time) < max_wait_seconds:
            attempts += 1
            try:
                dag = await self.get_dag(dag_id)
                elapsed = time.monotonic() - start_time
                logger.info("DAG '%s' found after %.1fs (%d attempts)", dag_id, elapsed, attempts)
                return {
                    "found": True,
                    "dag": dag,
                    "elapsed_seconds": elapsed,
                    "attempts": attempts,
                }
            except AsyncAirflowAPIError as e:
                if "404" in str(e) or "not found" in str(e).lower():
                    logger.debug(
                        "DAG '%s' not found yet (attempt %d), waiting %.1fs...",
                        dag_id,
                        attempts,
                        poll_interval,
                    )
                    await asyncio.sleep(poll_interval)
                else:
                    raise

        elapsed = time.monotonic() - start_time
        error_msg = (
            f"DAG '{dag_id}' did not appear within {max_wait_seconds}s ({attempts} attempts)"
        )
        logger.error(error_msg)
        raise TimeoutError(error_msg)

    async def wait_for_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
        timeout_seconds: int = 3600,
        poll_interval: float = 10.0,
        terminal_states: set[str] | None = None,
    ) -> dict[str, Any]:
        """
        Poll a DAG run until it reaches a terminal state.

        Args:
            dag_id: DAG identifier
            dag_run_id: DAG run identifier
            timeout_seconds: Maximum time to wait in seconds (default: 3600)
            poll_interval: Time between polling attempts in seconds (default: 10)
            terminal_states: Set of states that indicate completion
                (default: {"success", "failed", "skipped"})

        Returns:
            Dictionary with final DAG run state and duration

        Raises:
            TimeoutError: If the DAG run does not complete within timeout_seconds
        """
        if terminal_states is None:
            terminal_states = {"success", "failed", "skipped"}
        else:
            terminal_states = {s.lower() for s in terminal_states}

        logger.info(
            "Waiting for DAG run '%s/%s' to complete (timeout %ds)",
            dag_id,
            dag_run_id,
            timeout_seconds,
        )
        start = time.monotonic()

        while (time.monotonic() - start) < timeout_seconds:
            run = await self.get_dag_run(dag_id, dag_run_id)
            state = (run.get("state") or "").lower()
            if state in terminal_states:
                elapsed = time.monotonic() - start
                run["duration_seconds"] = elapsed
                logger.info(
                    "DAG run '%s/%s' completed with state '%s' in %.1fs",
                    dag_id,
                    dag_run_id,
                    state,
                    elapsed,
                )
                return run
            await asyncio.sleep(poll_interval)

        error_msg = f"DAG run '{dag_id}/{dag_run_id}' did not complete within {timeout_seconds}s"
        logger.error(error_msg)
        raise TimeoutError(error_msg)

    async def get_dag_runs(
        self,
        dag_id: str,
        limit: int = 25,
        offset: int = 0,
        state: str | None = None,
        start_date_gte: str | None = None,
        start_date_lte: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get DAG runs with optional filtering.

        Compatibility wrapper that delegates to list_dag_runs.
        Maps start_date_gte/lte to the Airflow API's execution_date_gte/lte params.
        """
        return await self.list_dag_runs(
            dag_id=dag_id,
            limit=limit,
            offset=offset,
            state=state,
            execution_date_gte=start_date_gte,
            execution_date_lte=start_date_lte,
        )

    async def get_dag_run_history(
        self,
        dag_id: str,
        days: int = 30,
    ) -> dict[str, Any]:
        """
        Get DAG run history with statistics.

        Args:
            dag_id: DAG identifier
            days: Number of days of history to retrieve

        Returns:
            Dictionary with historical run data and statistics
        """
        from datetime import datetime, timedelta, timezone

        # Get recent runs (list_dag_runs returns list[dict] directly)
        runs = await self.list_dag_runs(dag_id, limit=100)

        # Filter by date range
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        recent_runs = []
        for run in runs:
            exec_date_str = run.get("execution_date") or run.get("logical_date")
            if exec_date_str:
                try:
                    exec_date = datetime.fromisoformat(exec_date_str.replace("Z", "+00:00"))
                    if exec_date >= cutoff:
                        recent_runs.append(run)
                except (ValueError, TypeError):
                    recent_runs.append(run)
            else:
                recent_runs.append(run)

        # Calculate statistics
        total = len(recent_runs)
        success = sum(1 for r in recent_runs if r.get("state") == "success")
        failed = sum(1 for r in recent_runs if r.get("state") == "failed")
        running = sum(1 for r in recent_runs if r.get("state") == "running")

        return {
            "dag_id": dag_id,
            "days": days,
            "runs": recent_runs,
            "statistics": {
                "total_runs": total,
                "success": success,
                "failed": failed,
                "running": running,
                "success_rate": round(success / total * 100, 1) if total > 0 else 0,
            },
        }

    # ==================== Concurrent Operations ====================

    async def trigger_multiple_dags(
        self,
        dag_configs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Trigger multiple DAGs concurrently.

        Args:
            dag_configs: List of dicts with 'dag_id' and optional 'conf'

        Returns:
            List of DAG run results
        """
        tasks = [
            self.trigger_dag(
                dag_id=cfg["dag_id"],
                conf=cfg.get("conf"),
            )
            for cfg in dag_configs
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        return [r if isinstance(r, dict) else {"error": str(r), "success": False} for r in results]

    async def get_multiple_dag_statuses(
        self,
        dag_runs: list[tuple[str, str]],
    ) -> list[dict[str, Any]]:
        """
        Get status of multiple DAG runs concurrently.

        Args:
            dag_runs: List of (dag_id, dag_run_id) tuples

        Returns:
            List of status results
        """
        tasks = [self.get_dag_run_status(dag_id, dag_run_id) for dag_id, dag_run_id in dag_runs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        return [r if isinstance(r, dict) else {"error": str(r), "success": False} for r in results]

    # ==================== Cleanup ====================

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "AsyncAirflowClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()
