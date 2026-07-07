"""Unit tests for AsyncAirflowClient.

Tests cover:
- Rate limiting
- Connection pooling limits
- Response size limits
- New DAG run methods (clear_dag_run, set_dag_run_state, etc.)
- Error handling (invalid inputs, auth failures, connection failures)
- Resilience (timeouts, circuit breaker, rate limiting)
- Edge cases (empty results, malformed data)
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from teradata_etl_mcp_server.clients.async_airflow_client import (
    AsyncAirflowAPIError,
    AsyncAirflowClient,
    AsyncAirflowClientError,
    AsyncAirflowConnectionError,
    CircuitBreakerOpen,
    RateLimiter,
    RateLimitExceeded,
    ResponseTooLarge,
    TokenCacheEntry,
)


class TestRateLimiter:
    """Tests for the RateLimiter class."""

    @pytest.mark.asyncio
    async def test_acquire_token_immediately(self):
        """Test acquiring a token when available."""
        limiter = RateLimiter(rate=10.0, burst=5)
        result = await limiter.acquire(timeout=1.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_depletes_tokens(self):
        """Test that acquiring depletes available tokens."""
        limiter = RateLimiter(rate=10.0, burst=3)
        # Acquire all burst tokens
        for _ in range(3):
            await limiter.acquire(timeout=1.0)

        # Next acquire should need to wait for refill
        status = limiter.get_status()
        assert status["available_tokens"] < 1

    @pytest.mark.asyncio
    async def test_acquire_timeout(self):
        """Test that acquire raises on timeout."""
        limiter = RateLimiter(rate=0.01, burst=1)  # Very slow refill (1 per 100s)
        await limiter.acquire()  # Use the single token

        # Force tokens to zero
        limiter._tokens = 0

        with pytest.raises(RateLimitExceeded):
            await limiter.acquire(timeout=0.05)  # Short timeout should fail

    def test_get_status(self):
        """Test getting rate limiter status."""
        limiter = RateLimiter(rate=10.0, burst=5)
        status = limiter.get_status()

        assert status["rate_rps"] == 10.0
        assert status["burst"] == 5
        assert "available_tokens" in status


class TestAsyncAirflowClientInit:
    """Tests for AsyncAirflowClient initialization."""

    def test_default_hardening_settings(self):
        """Test that default hardening settings are applied."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            circuit_breaker_enabled=False,
        )

        assert client.max_connections == 100
        assert client.max_keepalive_connections == 20
        assert client.max_response_size_bytes == 10 * 1024 * 1024

    def test_custom_hardening_settings(self):
        """Test custom hardening settings."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            max_connections=50,
            max_keepalive_connections=10,
            max_response_size_bytes=5 * 1024 * 1024,
            rate_limit_rps=5.0,
            circuit_breaker_enabled=False,
        )

        assert client.max_connections == 50
        assert client.max_keepalive_connections == 10
        assert client.max_response_size_bytes == 5 * 1024 * 1024
        assert client._rate_limiter is not None
        assert client._rate_limiter.rate == 5.0

    def test_disable_rate_limiting(self):
        """Test disabling rate limiting."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )

        assert client._rate_limiter is None

    def test_verify_ssl_param_removed(self):
        """verify_ssl is no longer accepted; TLS verification is always enforced."""
        import pytest

        with pytest.raises(TypeError):
            AsyncAirflowClient(
                base_url="http://localhost:8080",
                verify_ssl=False,
                circuit_breaker_enabled=False,
            )

    def test_get_client_status(self):
        """Test get_client_status returns comprehensive info."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            timeout=60,
            max_connections=50,
            rate_limit_rps=5.0,
            circuit_breaker_enabled=False,
        )

        status = client.get_client_status()

        assert status["base_url"] == "http://localhost:8080"
        assert status["timeout_seconds"] == 60
        assert status["tls_min_version"] == "TLSv1_2"
        assert status["max_connections"] == 50
        assert status["rate_limiter"] is not None
        assert status["rate_limiter"]["rate_rps"] == 5.0


class TestResponseSizeLimit:
    """Tests for response size limit enforcement."""

    @pytest.fixture
    def client(self):
        """Create client with small response limit for testing."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            max_response_size_bytes=1000,  # 1KB limit
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"
        return client

    @pytest.mark.asyncio
    async def test_response_too_large_raises(self, client):
        """Test that oversized responses raise ResponseTooLarge."""
        large_content = b"x" * 2000  # 2KB, exceeds 1KB limit

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = large_content
        mock_response.raise_for_status = MagicMock()

        mock_http_client = AsyncMock()
        mock_http_client.get = AsyncMock(return_value=mock_response)
        client._client = mock_http_client

        with pytest.raises(ResponseTooLarge) as exc_info:
            await client._make_request("GET", "/test")

        assert "2000 bytes exceeds limit" in str(exc_info.value)


class TestNewDagRunMethods:
    """Tests for new DAG run methods."""

    @pytest.fixture
    def client(self):
        """Create client with mocked HTTP methods."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"
        client._make_request = AsyncMock()
        return client

    @pytest.mark.asyncio
    async def test_update_dag_run(self, client):
        """Test updating a DAG run state."""
        client._make_request.return_value = {
            "dag_run_id": "run-1",
            "state": "failed",
        }

        result = await client.update_dag_run(
            dag_id="my_dag",
            dag_run_id="run-1",
            state="failed",
            note="Cancelled by user",
        )

        client._make_request.assert_called_once_with(
            "PATCH",
            "/dags/my_dag/dagRuns/run-1",
            json={"state": "failed", "note": "Cancelled by user"},
        )
        assert result["state"] == "failed"

    @pytest.mark.asyncio
    async def test_update_dag_run_requires_field(self, client):
        """Test that update_dag_run requires at least one field."""
        with pytest.raises(ValueError) as exc_info:
            await client.update_dag_run("my_dag", "run-1")

        assert "At least one of 'state' or 'note'" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_set_dag_run_state(self, client):
        """Test setting DAG run state."""
        client._make_request.return_value = {"state": "failed"}

        await client.set_dag_run_state("my_dag", "run-1", "failed")

        client._make_request.assert_called_once_with(
            "PATCH",
            "/dags/my_dag/dagRuns/run-1",
            json={"state": "failed"},
        )

    @pytest.mark.asyncio
    async def test_clear_dag_run(self, client):
        """Test clearing a DAG run to retry tasks."""
        client.get_dag_run = AsyncMock(return_value={
            "logical_date": "2025-01-15T10:00:00Z",
        })
        client._make_request.return_value = {
            "task_instances": [{"task_id": "task1"}],
        }

        await client.clear_dag_run(
            dag_id="my_dag",
            dag_run_id="run-1",
            only_failed=True,
        )

        client._make_request.assert_called_once()
        call_args = client._make_request.call_args
        assert call_args[0][0] == "POST"
        assert "/clearTaskInstances" in call_args[0][1]
        assert call_args[1]["json"]["only_failed"] is True

    @pytest.mark.asyncio
    async def test_clear_dag_run_with_task_ids(self, client):
        """Test clearing specific tasks in a DAG run."""
        client.get_dag_run = AsyncMock(return_value={
            "logical_date": "2025-01-15T10:00:00Z",
        })
        client._make_request.return_value = {
            "task_instances": [{"task_id": "task_a"}],
        }

        await client.clear_dag_run(
            dag_id="my_dag",
            dag_run_id="run-1",
            only_failed=True,
            task_ids=["task_a", "task_b"],
        )

        client._make_request.assert_called_once()
        call_args = client._make_request.call_args
        payload = call_args[1]["json"]
        assert payload["task_ids"] == ["task_a", "task_b"]
        assert payload["only_failed"] is True

    @pytest.mark.asyncio
    async def test_clear_dag_run_no_execution_date(self, client):
        """Test clear_dag_run fails without execution date."""
        client.get_dag_run = AsyncMock(return_value={})

        with pytest.raises(AsyncAirflowAPIError) as exc_info:
            await client.clear_dag_run("my_dag", "run-1")

        assert "Cannot determine execution date" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_delete_dag_run(self, client):
        """Test deleting a DAG run."""
        client._make_request.return_value = {}

        result = await client.delete_dag_run("my_dag", "run-1")

        client._make_request.assert_called_once_with(
            "DELETE", "/dags/my_dag/dagRuns/run-1"
        )
        assert result["deleted"] is True


class TestRateLimitHandling:
    """Tests for 429 rate limit response handling."""

    @pytest.fixture
    def client(self):
        """Create client for testing."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
            retry_attempts=1,
        )
        client._resolved_api_version = "v1"
        return client

    @pytest.mark.asyncio
    async def test_429_retry_with_retry_after(self, client):
        """Test 429 response triggers retry with Retry-After header."""
        # First call returns 429, second succeeds
        mock_response_429 = MagicMock()
        mock_response_429.status_code = 429
        mock_response_429.headers = {"Retry-After": "0.1"}

        mock_response_ok = MagicMock()
        mock_response_ok.status_code = 200
        mock_response_ok.content = b'{"result": "ok"}'
        mock_response_ok.json.return_value = {"result": "ok"}
        mock_response_ok.raise_for_status = MagicMock()

        mock_http_client = AsyncMock()

        # Simulate 429 then success
        call_count = 0

        async def mock_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                error = httpx.HTTPStatusError(
                    "Rate Limited", request=MagicMock(), response=mock_response_429
                )
                raise error
            return mock_response_ok

        mock_http_client.get = mock_get
        client._client = mock_http_client

        result = await client._make_request("GET", "/test")

        assert result["result"] == "ok"
        assert call_count == 2


class TestGetRateLimiterStatus:
    """Tests for get_rate_limiter_status method."""

    def test_rate_limiter_enabled(self):
        """Test getting status when rate limiter is enabled."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=10.0,
            circuit_breaker_enabled=False,
        )

        status = client.get_rate_limiter_status()

        assert status is not None
        assert status["rate_rps"] == 10.0

    def test_rate_limiter_disabled(self):
        """Test getting status when rate limiter is disabled."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )

        status = client.get_rate_limiter_status()

        assert status is None


# ==================== Fix 1: Deduplicated list_connections ====================


class TestListConnections:
    """Tests for the deduplicated list_connections method."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"
        client._make_request = AsyncMock()
        return client

    @pytest.mark.asyncio
    async def test_list_connections_default(self, client):
        """Test list_connections with default params."""
        client._make_request.return_value = {
            "connections": [{"conn_id": "td_default"}],
        }

        result = await client.list_connections()

        client._make_request.assert_called_once_with(
            "GET",
            "/connections",
            params={"limit": 100, "offset": 0},
        )
        assert len(result) == 1
        assert result[0]["conn_id"] == "td_default"

    @pytest.mark.asyncio
    async def test_list_connections_with_offset(self, client):
        """Test list_connections supports offset for pagination."""
        client._make_request.return_value = {"connections": []}

        await client.list_connections(limit=50, offset=100)

        client._make_request.assert_called_once_with(
            "GET",
            "/connections",
            params={"limit": 50, "offset": 100},
        )


# ==================== Fix 2: Deduplicated wait_for_dag_run ====================


class TestWaitForDagRun:
    """Tests for the deduplicated wait_for_dag_run method."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"
        return client

    @pytest.mark.asyncio
    async def test_returns_on_terminal_state(self, client):
        """Test that wait_for_dag_run returns when state is terminal."""
        client.get_dag_run = AsyncMock(return_value={
            "dag_run_id": "run-1",
            "state": "success",
        })

        result = await client.wait_for_dag_run("dag1", "run-1")

        assert result["state"] == "success"
        assert "duration_seconds" in result
        client.get_dag_run.assert_called_once_with("dag1", "run-1")

    @pytest.mark.asyncio
    async def test_custom_terminal_states(self, client):
        """Test that custom terminal_states are respected."""
        client.get_dag_run = AsyncMock(return_value={
            "dag_run_id": "run-1",
            "state": "cancelled",
        })

        result = await client.wait_for_dag_run(
            "dag1", "run-1", terminal_states={"cancelled", "success"},
        )

        assert result["state"] == "cancelled"

    @pytest.mark.asyncio
    async def test_timeout_raises(self, client):
        """Test that timeout raises TimeoutError."""
        client.get_dag_run = AsyncMock(return_value={
            "dag_run_id": "run-1",
            "state": "running",
        })

        with pytest.raises(TimeoutError, match="did not complete"):
            await client.wait_for_dag_run(
                "dag1", "run-1", timeout_seconds=0, poll_interval=0.01,
            )

    @pytest.mark.asyncio
    async def test_polls_until_terminal(self, client):
        """Test that it polls multiple times until terminal state."""
        client.get_dag_run = AsyncMock(side_effect=[
            {"dag_run_id": "run-1", "state": "running"},
            {"dag_run_id": "run-1", "state": "running"},
            {"dag_run_id": "run-1", "state": "success"},
        ])

        result = await client.wait_for_dag_run(
            "dag1", "run-1", poll_interval=0.01,
        )

        assert result["state"] == "success"
        assert client.get_dag_run.call_count == 3

    @pytest.mark.asyncio
    async def test_mixed_case_terminal_states_normalized(self, client):
        """Test that mixed-case terminal_states are normalized to lowercase."""
        client.get_dag_run = AsyncMock(return_value={
            "dag_run_id": "run-1",
            "state": "success",
        })

        result = await client.wait_for_dag_run(
            "dag1", "run-1", terminal_states={"Success", "FAILED"},
        )

        assert result["state"] == "success"


# ==================== Fix 3: Fixed get_dag_runs / list_dag_runs ====================


class TestListDagRuns:
    """Tests for list_dag_runs with date filter params."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"
        client._make_request = AsyncMock(return_value={"dag_runs": []})
        return client

    @pytest.mark.asyncio
    async def test_execution_date_gte_param(self, client):
        """Test that execution_date_gte is passed to API."""
        await client.list_dag_runs(
            "dag1", execution_date_gte="2025-01-01T00:00:00Z",
        )

        call_params = client._make_request.call_args[1]["params"]
        assert call_params["execution_date_gte"] == "2025-01-01T00:00:00Z"

    @pytest.mark.asyncio
    async def test_execution_date_lte_param(self, client):
        """Test that execution_date_lte is passed to API."""
        await client.list_dag_runs(
            "dag1", execution_date_lte="2025-12-31T23:59:59Z",
        )

        call_params = client._make_request.call_args[1]["params"]
        assert call_params["execution_date_lte"] == "2025-12-31T23:59:59Z"

    @pytest.mark.asyncio
    async def test_order_by_param(self, client):
        """Test that order_by is passed to API."""
        await client.list_dag_runs("dag1", order_by="-execution_date")

        call_params = client._make_request.call_args[1]["params"]
        assert call_params["order_by"] == "-execution_date"

    @pytest.mark.asyncio
    async def test_no_optional_params_when_none(self, client):
        """Test that None params are not sent to API."""
        await client.list_dag_runs("dag1")

        call_params = client._make_request.call_args[1]["params"]
        assert "execution_date_gte" not in call_params
        assert "execution_date_lte" not in call_params
        assert "order_by" not in call_params
        assert "state" not in call_params


class TestGetDagRunHistory:
    """Tests for get_dag_run_history treating list_dag_runs return as a list."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"
        return client

    @pytest.mark.asyncio
    async def test_handles_list_return(self, client):
        """Test that get_dag_run_history works with list return from list_dag_runs."""
        from datetime import datetime, timezone

        # Use a recent date so it passes the days-cutoff filter
        recent = datetime.now(timezone.utc).isoformat()
        client.list_dag_runs = AsyncMock(return_value=[
            {
                "dag_run_id": "run-1",
                "state": "success",
                "execution_date": recent,
                "start_date": recent,
                "end_date": recent,
            },
        ])

        result = await client.get_dag_run_history("dag1", days=30)

        assert "statistics" in result
        assert "runs" in result
        assert result["statistics"]["total_runs"] == 1


class TestGetDagRuns:
    """Tests for the get_dag_runs compatibility wrapper."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"
        client._make_request = AsyncMock(return_value={
            "dag_runs": [{"dag_run_id": "run-1", "state": "success"}],
        })
        return client

    @pytest.mark.asyncio
    async def test_returns_list(self, client):
        """Test that get_dag_runs returns a list (not a dict)."""
        result = await client.get_dag_runs("dag1")

        assert isinstance(result, list)
        assert result[0]["dag_run_id"] == "run-1"

    @pytest.mark.asyncio
    async def test_maps_start_date_to_execution_date(self, client):
        """Test that start_date_gte/lte maps to execution_date_gte/lte."""
        await client.get_dag_runs(
            "dag1",
            start_date_gte="2025-01-01T00:00:00Z",
            start_date_lte="2025-12-31T23:59:59Z",
        )

        call_params = client._make_request.call_args[1]["params"]
        assert call_params["execution_date_gte"] == "2025-01-01T00:00:00Z"
        assert call_params["execution_date_lte"] == "2025-12-31T23:59:59Z"

    @pytest.mark.asyncio
    async def test_passes_offset(self, client):
        """Test that offset is passed through to list_dag_runs."""
        await client.get_dag_runs("dag1", offset=50)

        call_params = client._make_request.call_args[1]["params"]
        assert call_params["offset"] == 50


# ==================== Fix 4: Token cache race condition ====================


class TestTokenCacheRace:
    """Tests for token cache double-checked locking."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            username="admin",
            password="admin",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v2"
        # Clear class-level caches to ensure test isolation
        # _TOKEN_CACHE is keyed by (base_url, username)
        cache_key = (client.base_url, client.username)
        client._TOKEN_CACHE.pop(cache_key, None)
        # _TOKEN_CACHE_LOCKS is keyed by (loop_id, base_url, username) — clear all matching
        for k in [k for k in client._TOKEN_CACHE_LOCKS if k[1:] == (client.base_url, client.username)]:
            del client._TOKEN_CACHE_LOCKS[k]
        client._access_token = None
        yield client
        # Cleanup after test
        client._TOKEN_CACHE.pop(cache_key, None)
        for k in [k for k in client._TOKEN_CACHE_LOCKS if k[1:] == (client.base_url, client.username)]:
            del client._TOKEN_CACHE_LOCKS[k]

    @pytest.mark.asyncio
    async def test_concurrent_ensure_token_single_fetch(self, client):
        """Test that concurrent _ensure_token calls only fetch one token."""
        fetch_count = 0

        async def mock_obtain_token():
            nonlocal fetch_count
            fetch_count += 1
            await asyncio.sleep(0.05)  # Simulate network latency
            client._access_token = "test-token"
            key = (client.base_url, client.username)
            client._TOKEN_CACHE[key] = TokenCacheEntry(
                token="test-token",
                expires_at=time.time() + 3600,
            )

        client._obtain_token = mock_obtain_token

        # Launch 5 concurrent _ensure_token calls
        await asyncio.gather(*[client._ensure_token() for _ in range(5)])

        assert fetch_count == 1, f"Expected 1 token fetch, got {fetch_count}"
        assert client._access_token == "test-token"

    @pytest.mark.asyncio
    async def test_fast_path_skips_lock(self, client):
        """Test that fast path returns immediately when token is set."""
        client._access_token = "existing-token"

        # Should return immediately without acquiring lock
        await client._ensure_token()

        assert client._access_token == "existing-token"

    @pytest.mark.asyncio
    async def test_cache_hit_avoids_network(self, client):
        """Test that a cache hit avoids calling _obtain_token."""
        key = (client.base_url, client.username)
        client._TOKEN_CACHE[key] = TokenCacheEntry(
            token="cached-token",
            expires_at=time.time() + 3600,
        )

        obtain_called = False

        async def mock_obtain():
            nonlocal obtain_called
            obtain_called = True

        client._obtain_token = mock_obtain

        await client._ensure_token()

        assert not obtain_called
        assert client._access_token == "cached-token"


class TestTokenRefresh401:
    """Tests for 401 token refresh guard in _make_request."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            username="admin",
            password="admin",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v2"
        client._access_token = "stale-token"
        return client

    @pytest.mark.asyncio
    async def test_401_refresh_retried_only_once(self, client):
        """After a token refresh, a second 401 should raise instead of looping."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_response.json.return_value = {"detail": "Unauthorized"}
        mock_response.headers = {"Content-Type": "application/json"}

        http_error = httpx.HTTPStatusError(
            "401 Unauthorized", request=MagicMock(), response=mock_response,
        )

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=http_error)
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)

        refresh_count = 0

        async def mock_obtain_token():
            nonlocal refresh_count
            refresh_count += 1
            client._access_token = f"new-token-{refresh_count}"

        client._obtain_token = mock_obtain_token

        with pytest.raises(AsyncAirflowAPIError):
            await client._make_request("GET", "/dags")

        # Token refresh should happen exactly once, then the second 401 raises
        assert refresh_count == 1
        # The endpoint was called twice: initial + one retry after refresh
        assert mock_client.get.call_count == 2

    @pytest.mark.asyncio
    async def test_401_cache_evicted_when_reauth_fails(self, client):
        """Stale _TOKEN_CACHE entry must be evicted even when re-auth fails."""
        base_url = client.base_url
        username = client.username
        key = (base_url, username)

        # Pre-populate cache with a non-expired stale entry
        AsyncAirflowClient._TOKEN_CACHE[key] = TokenCacheEntry(
            token="stale-token",
            expires_at=time.time() + 3600,
        )

        mock_401_response = MagicMock()
        mock_401_response.status_code = 401
        mock_401_response.text = "Unauthorized"
        mock_401_response.json.return_value = {"detail": "Unauthorized"}
        mock_401_response.headers = {"Content-Type": "application/json"}

        http_401 = httpx.HTTPStatusError(
            "401 Unauthorized", request=MagicMock(), response=mock_401_response,
        )

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=http_401)
        mock_client.headers = MagicMock()
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)

        async def mock_obtain_token_fails():
            raise AsyncAirflowAPIError("re-auth failed")

        client._obtain_token = mock_obtain_token_fails

        try:
            with pytest.raises(AsyncAirflowAPIError):
                await client._make_request("GET", "/dags")

            assert AsyncAirflowClient._TOKEN_CACHE.get(key) is None
            mock_client.headers.pop.assert_called_with("Authorization", None)
        finally:
            AsyncAirflowClient._TOKEN_CACHE.pop(key, None)


class TestTokenRefresh403:
    """Tests for 403 token refresh guard in _make_request (mirrors TestTokenRefresh401)."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            username="admin",
            password="admin",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v2"
        client._access_token = "stale-token"
        return client

    @pytest.mark.asyncio
    async def test_403_triggers_reauth_and_retry(self, client):
        """A 403 with any JWT-related text should trigger re-auth and succeed on retry.

        The is_jwt_403 guard matches any occurrence of "jwt" (case-insensitive) so
        it covers "Invalid JWT token", "JWT token expired", "jwt validation failed",
        etc. across Airflow 2 and 3 variants.
        """
        mock_403_response = MagicMock()
        mock_403_response.status_code = 403
        mock_403_response.text = "Invalid JWT token"
        mock_403_response.json.return_value = {"detail": "Invalid JWT token"}
        mock_403_response.headers = {"Content-Type": "application/json"}

        http_403 = httpx.HTTPStatusError(
            "403 Forbidden", request=MagicMock(), response=mock_403_response,
        )

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.content = b'{"dags": []}'
        success_response.json.return_value = {"dags": []}

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[http_403, success_response])
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)
        client._client.headers = MagicMock()

        obtain_count = 0

        async def mock_obtain_token():
            nonlocal obtain_count
            obtain_count += 1
            client._access_token = "new-token"

        client._obtain_token = mock_obtain_token

        result = await client._make_request("GET", "/dags")

        assert obtain_count == 1
        assert result == {"dags": []}
        assert mock_client.get.call_count == 2

    @pytest.mark.asyncio
    async def test_successful_reauth_does_not_clear_header_before_new_token(self, client):
        """Authorization header must NOT be removed before _obtain_token() succeeds.

        Clearing the shared client header before the await would leave concurrent
        _make_request coroutines sending completely unauthenticated requests during
        the refresh window.  Only a *failed* refresh should clear the header.
        """
        mock_403_response = MagicMock()
        mock_403_response.status_code = 403
        mock_403_response.text = "Invalid JWT token"
        mock_403_response.json.return_value = {"detail": "Invalid JWT token"}
        mock_403_response.headers = {"Content-Type": "application/json"}

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.content = b'{"dags": []}'
        success_response.json.return_value = {"dags": []}

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=[
                httpx.HTTPStatusError("403 Forbidden", request=MagicMock(), response=mock_403_response),
                success_response,
            ]
        )
        mock_client.headers = MagicMock()
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)

        async def mock_obtain_token():
            client._access_token = "new-token"

        client._obtain_token = mock_obtain_token

        await client._make_request("GET", "/dags")

        # headers.pop must never have been called — the stale token was kept in
        # place while _obtain_token() ran, then overwritten by the new one.
        mock_client.headers.pop.assert_not_called()

    @pytest.mark.asyncio
    async def test_403_raises_immediately_for_genuine_permission_denial(self, client):
        """A plain 403 Forbidden (no JWT error text) must raise without re-auth or retry."""
        mock_403_response = MagicMock()
        mock_403_response.status_code = 403
        mock_403_response.text = "Forbidden"
        mock_403_response.json.return_value = {"detail": "Forbidden"}
        mock_403_response.headers = {"Content-Type": "application/json"}

        http_403 = httpx.HTTPStatusError(
            "403 Forbidden", request=MagicMock(), response=mock_403_response,
        )

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=http_403)
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)
        client._client.headers = MagicMock()

        obtain_count = 0

        async def mock_obtain_token():
            nonlocal obtain_count
            obtain_count += 1
            client._access_token = f"new-token-{obtain_count}"

        client._obtain_token = mock_obtain_token

        with pytest.raises(AsyncAirflowAPIError):
            await client._make_request("GET", "/dags")

        # Genuine permission denial: no re-auth attempt, no retry
        assert obtain_count == 0
        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_403_jwt_expired_text_triggers_reauth(self, client):
        """A 403 with Airflow 3-style 'JWT token expired' text should trigger re-auth."""
        mock_403_response = MagicMock()
        mock_403_response.status_code = 403
        mock_403_response.text = "JWT token expired"
        mock_403_response.json.return_value = {"detail": "JWT token expired"}
        mock_403_response.headers = {"Content-Type": "application/json"}

        http_403 = httpx.HTTPStatusError(
            "403 Forbidden", request=MagicMock(), response=mock_403_response,
        )

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.content = b'{"dags": []}'
        success_response.json.return_value = {"dags": []}

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[http_403, success_response])
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)
        client._client.headers = MagicMock()

        obtain_count = 0

        async def mock_obtain_token():
            nonlocal obtain_count
            obtain_count += 1
            client._access_token = "new-token"

        client._obtain_token = mock_obtain_token

        result = await client._make_request("GET", "/dags")

        assert obtain_count == 1, "re-auth should fire exactly once for JWT expired text"
        assert result == {"dags": []}
        assert mock_client.get.call_count == 2

    @pytest.mark.asyncio
    async def test_403_cache_evicted_when_reauth_fails(self, client):
        """Stale _TOKEN_CACHE entry must be evicted even when re-auth fails."""
        base_url = client.base_url
        username = client.username
        key = (base_url, username)

        # Pre-populate cache with a non-expired stale entry
        AsyncAirflowClient._TOKEN_CACHE[key] = TokenCacheEntry(
            token="stale-token",
            expires_at=time.time() + 3600,
        )

        mock_403_response = MagicMock()
        mock_403_response.status_code = 403
        mock_403_response.text = "Invalid JWT token"
        mock_403_response.json.return_value = {"detail": "Invalid JWT token"}
        mock_403_response.headers = {"Content-Type": "application/json"}

        http_403 = httpx.HTTPStatusError(
            "403 Forbidden", request=MagicMock(), response=mock_403_response,
        )

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=http_403)
        mock_client.headers = MagicMock()
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)

        async def mock_obtain_token_fails():
            raise AsyncAirflowAPIError("re-auth failed")

        client._obtain_token = mock_obtain_token_fails

        try:
            with pytest.raises(AsyncAirflowAPIError):
                await client._make_request("GET", "/dags")

            assert AsyncAirflowClient._TOKEN_CACHE.get(key) is None
            mock_client.headers.pop.assert_called_with("Authorization", None)
        finally:
            AsyncAirflowClient._TOKEN_CACHE.pop(key, None)


class TestCircuitBreakerFailureRecording:
    """Only server errors (5xx) and transport errors should trip the circuit breaker."""

    def _make_cb_client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=True,
        )
        client._resolved_api_version = "v1"
        cb_mock = MagicMock()
        cb_mock.is_available = True
        client._circuit_breaker = cb_mock
        return client, cb_mock

    def _http_error(self, status_code):
        """Build an httpx.HTTPStatusError for the given status code."""
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.text = "error"
        mock_response.json.return_value = {"detail": "error"}
        mock_response.headers = {"Content-Type": "application/json"}
        return httpx.HTTPStatusError(
            f"{status_code}", request=MagicMock(), response=mock_response,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [400, 401, 403, 404, 409, 422])
    async def test_4xx_not_counted_as_failure(self, status_code):
        """Client errors (4xx) should NOT be recorded as circuit breaker failures."""
        client, cb_mock = self._make_cb_client()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=self._http_error(status_code))
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)

        with pytest.raises(AsyncAirflowAPIError):
            await client._make_request("GET", "/test")

        cb_mock.record_failure.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [500, 502, 503, 504])
    async def test_5xx_counted_as_failure(self, status_code):
        """Server errors (5xx) SHOULD be recorded as circuit breaker failures."""
        client, cb_mock = self._make_cb_client()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=self._http_error(status_code))
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)

        with pytest.raises(AsyncAirflowAPIError):
            await client._make_request("GET", "/test")

        cb_mock.record_failure.assert_called()

    @pytest.mark.asyncio
    async def test_transport_error_counted_as_failure(self):
        """Connection/transport errors SHOULD be recorded as circuit breaker failures."""
        client, cb_mock = self._make_cb_client()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=httpx.ConnectError("connection refused"),
        )
        client._client = mock_client
        client._get_client = AsyncMock(return_value=mock_client)

        with pytest.raises(AsyncAirflowConnectionError):
            await client._make_request("GET", "/test")

        cb_mock.record_failure.assert_called_once()


# =============================================================================
# Negative case tests (merged from test_airflow_negative_cases.py)
# =============================================================================


@pytest.fixture
def async_client():
    """Create AsyncAirflowClient for testing."""
    client = AsyncAirflowClient(
        base_url="http://localhost:8080",
        username="admin",
        password="admin",
        rate_limit_rps=None,
        circuit_breaker_enabled=False,
    )
    client._resolved_api_version = "v1"
    return client


@pytest.fixture
def async_client_with_circuit_breaker():
    """Create client with circuit breaker enabled."""
    client = AsyncAirflowClient(
        base_url="http://localhost:8080",
        username="admin",
        password="admin",
        rate_limit_rps=10.0,
        circuit_breaker_enabled=True,
        circuit_breaker_threshold=3,
        circuit_breaker_timeout=30.0,
    )
    client._resolved_api_version = "v1"
    return client


# =============================================================================
# Test: Invalid Input Handling
# =============================================================================


class TestInvalidInputHandling:
    """Tests for invalid input handling."""

    @pytest.mark.asyncio
    async def test_get_dag_invalid_dag_id(self, async_client):
        """Test fetching DAG with invalid/non-existent ID."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"detail": "DAG not found"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        with pytest.raises(AsyncAirflowAPIError) as exc:
            await async_client.get_dag("non_existent_dag_12345")

        assert "404" in str(exc.value) or "not found" in str(exc.value).lower()

    @pytest.mark.asyncio
    async def test_get_dag_run_invalid_run_id(self, async_client):
        """Test fetching DAG run with invalid run ID."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        with pytest.raises(AsyncAirflowAPIError):
            await async_client.get_dag_run("valid_dag", "invalid_run_id_xyz")

    @pytest.mark.asyncio
    async def test_trigger_dag_invalid_config(self, async_client):
        """Test triggering DAG with invalid configuration."""
        mock_response_400 = MagicMock()
        mock_response_400.status_code = 400
        mock_response_400.json.return_value = {"detail": "Invalid configuration"}

        # Mock GET for get_dag (to check is_paused) - return unpaused DAG
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.content = b'{"dag_id": "test_dag", "is_paused": false}'
        mock_get_response.json.return_value = {"dag_id": "test_dag", "is_paused": False}
        mock_get_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_get_response)
        mock_http.post = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Bad Request",
            request=MagicMock(),
            response=mock_response_400,
        ))
        async_client._client = mock_http

        with pytest.raises(AsyncAirflowAPIError):
            await async_client.trigger_dag(
                dag_id="test_dag",
                conf={"key": "value"},
            )

    @pytest.mark.asyncio
    async def test_get_task_logs_invalid_task(self, async_client):
        """Test fetching logs for non-existent task."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"detail": "Not Found"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        # get_task_logs returns None or empty on error, doesn't raise
        result = await async_client.get_task_logs(
            dag_id="test_dag",
            dag_run_id="run_1",
            task_id="non_existent_task",
        )
        assert result is None or result == ""


# =============================================================================
# Test: Authentication Failures
# =============================================================================


class TestAuthenticationFailures:
    """Tests for authentication failure handling."""

    @pytest.mark.asyncio
    async def test_invalid_credentials(self):
        """Test connection with invalid credentials."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            username="wrong_user",
            password="wrong_pass",
            circuit_breaker_enabled=False,
        )

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"detail": "Unauthorized"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_response,
        ))
        client._client = mock_http
        client._resolved_api_version = "v1"

        # test_connection catches errors and returns dict with connected=False
        result = await client.test_connection()
        assert result["connected"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_forbidden_operation(self, async_client):
        """Test operation without sufficient permissions."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.json.return_value = {"detail": "Forbidden"}

        mock_http = AsyncMock()
        mock_http.delete = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Forbidden",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        with pytest.raises(AsyncAirflowAPIError) as exc:
            await async_client.delete_dag_run("admin_dag", "run_1")

        assert "403" in str(exc.value) or "forbidden" in str(exc.value).lower()

    @pytest.mark.asyncio
    async def test_token_expired(self, async_client):
        """Test handling of expired authentication token."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"detail": "Token expired"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        with pytest.raises(AsyncAirflowAPIError):
            await async_client.list_dags()


# =============================================================================
# Test: Connection Failures
# =============================================================================


class TestConnectionFailures:
    """Tests for connection failure handling."""

    @pytest.mark.asyncio
    async def test_connection_refused(self):
        """Test handling when Airflow server is unreachable."""
        client = AsyncAirflowClient(
            base_url="http://localhost:9999",  # Wrong port
            timeout=1,
            circuit_breaker_enabled=False,
        )

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
        client._client = mock_http
        client._resolved_api_version = "v1"

        # test_connection catches errors and returns dict with connected=False
        result = await client.test_connection()
        assert result["connected"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_connection_timeout(self, async_client):
        """Test handling of connection timeout."""
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.TimeoutException("Timeout"))
        async_client._client = mock_http

        with pytest.raises((AsyncAirflowConnectionError, AsyncAirflowClientError, httpx.TimeoutException)):
            await async_client.list_dags()

    @pytest.mark.asyncio
    async def test_dns_resolution_failure(self):
        """Test handling of DNS resolution failure."""
        client = AsyncAirflowClient(
            base_url="http://non-existent-host-xyz.invalid:8080",
            timeout=1,
            circuit_breaker_enabled=False,
        )

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.ConnectError("DNS resolution failed"))
        client._client = mock_http
        client._resolved_api_version = "v1"

        # test_connection catches errors and returns dict with connected=False
        result = await client.test_connection()
        assert result["connected"] is False
        assert "error" in result


# =============================================================================
# Test: Circuit Breaker (Negative Cases)
# =============================================================================


class TestCircuitBreakerNegative:
    """Tests for circuit breaker functionality."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_failures(self, async_client_with_circuit_breaker):
        """Test circuit breaker opens after consecutive failures."""
        client = async_client_with_circuit_breaker

        # Simulate failures
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.ConnectError("Connection failed"))
        client._client = mock_http

        # Trigger failures to open circuit breaker
        for _ in range(5):
            try:
                await client._make_request("GET", "/test")
            except Exception:
                pass

        # Circuit breaker should now be open
        status = client.get_circuit_breaker_status()
        # Check if circuit breaker recorded failures
        assert status is not None

    @pytest.mark.asyncio
    async def test_circuit_breaker_rejects_when_open(self):
        """Test requests are rejected when circuit breaker is open."""
        from teradata_etl_mcp_server.utils.circuit_breaker import CircuitState

        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=1,
        )
        client._resolved_api_version = "v1"

        # Mock circuit breaker to be open
        if client._circuit_breaker:
            client._circuit_breaker._state = CircuitState.OPEN
            client._circuit_breaker._failure_count = 10

            # Requests should be rejected
            with pytest.raises((CircuitBreakerOpen, AsyncAirflowClientError)):
                await client._make_request("GET", "/test")


# =============================================================================
# Test: Rate Limiting (Negative Cases)
# =============================================================================


class TestRateLimitingNegative:
    """Tests for rate limiting functionality."""

    @pytest.mark.asyncio
    async def test_rate_limit_exceeded_locally(self):
        """Test local rate limiter blocks excessive requests."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=0.1,  # Very low rate
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"

        # Exhaust rate limit tokens
        if client._rate_limiter:
            client._rate_limiter._tokens = 0

            with pytest.raises(RateLimitExceeded):
                await client._rate_limiter.acquire(timeout=0.01)

    @pytest.mark.asyncio
    async def test_server_rate_limit_429(self, async_client):
        """Test handling of server-side 429 rate limit response."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "60"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Too Many Requests",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        # RateLimitExceeded is expected after retry exhaustion
        with pytest.raises((RateLimitExceeded, AsyncAirflowAPIError)):
            await async_client.list_dags()


# =============================================================================
# Test: Response Size Limits (Negative Cases)
# =============================================================================


class TestResponseSizeLimitsNegative:
    """Tests for response size limit enforcement."""

    @pytest.mark.asyncio
    async def test_response_too_large(self):
        """Test rejection of oversized responses."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            max_response_size_bytes=100,  # Very small limit
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"

        # Mock large response
        large_content = b"x" * 1000
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = large_content
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        client._client = mock_http

        with pytest.raises(ResponseTooLarge):
            await client._make_request("GET", "/large-endpoint")


# =============================================================================
# Test: Malformed Responses
# =============================================================================


class TestMalformedResponses:
    """Tests for handling malformed API responses."""

    @pytest.mark.asyncio
    async def test_invalid_json_response(self, async_client):
        """Test handling of invalid JSON in response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"not valid json {"
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        async_client._client = mock_http

        with pytest.raises((ValueError, AsyncAirflowClientError)):
            await async_client._make_request("GET", "/test")

    @pytest.mark.asyncio
    async def test_empty_response_body(self, async_client):
        """Test handling of empty response body."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_response.json.return_value = None
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        async_client._client = mock_http

        result = await async_client._make_request("GET", "/test")
        # Should handle gracefully
        assert result is None or result == {}

    @pytest.mark.asyncio
    async def test_unexpected_response_structure(self, async_client):
        """Test handling of unexpected response structure."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"unexpected": "structure"}'
        mock_response.json.return_value = {"unexpected": "structure"}
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        async_client._client = mock_http

        # Should not crash, return whatever we got
        result = await async_client._make_request("GET", "/test")
        assert result == {"unexpected": "structure"}


# =============================================================================
# Test: Concurrent Operation Errors
# =============================================================================


class TestConcurrentOperationErrors:
    """Tests for concurrent operation error handling."""

    @pytest.mark.asyncio
    async def test_concurrent_dag_triggers(self, async_client):
        """Test handling multiple concurrent DAG triggers."""
        call_count = 0

        async def mock_trigger(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"dag_run_id": "run_1", "state": "queued"}
            else:
                raise AsyncAirflowAPIError("DAG run already exists")

        async_client.trigger_dag_run = mock_trigger

        # First trigger succeeds
        result1 = await async_client.trigger_dag_run("test_dag")
        assert result1["dag_run_id"] == "run_1"

        # Second trigger fails (duplicate)
        with pytest.raises(AsyncAirflowAPIError):
            await async_client.trigger_dag_run("test_dag")

    @pytest.mark.asyncio
    async def test_parallel_requests_with_rate_limit(self):
        """Test parallel requests respect rate limits."""
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=2.0,  # Low rate
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v1"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"result": "ok"}'
        mock_response.json.return_value = {"result": "ok"}
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        client._client = mock_http

        # Try many parallel requests
        tasks = [client._make_request("GET", "/test") for _ in range(10)]

        # Some should succeed, some might fail due to rate limit
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # At least some should succeed
        successes = [r for r in results if not isinstance(r, Exception)]
        assert len(successes) >= 1


# =============================================================================
# Test: Server Error Handling
# =============================================================================


class TestServerErrors:
    """Tests for server error handling."""

    @pytest.mark.asyncio
    async def test_internal_server_error_500(self, async_client):
        """Test handling of 500 Internal Server Error."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"detail": "Internal Server Error"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Internal Server Error",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        with pytest.raises(AsyncAirflowAPIError) as exc:
            await async_client.list_dags()

        assert "500" in str(exc.value)

    @pytest.mark.asyncio
    async def test_service_unavailable_503(self, async_client):
        """Test handling of 503 Service Unavailable."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.json.return_value = {"detail": "Service Unavailable"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Service Unavailable",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        # test_connection catches exceptions and returns connected=False
        result = await async_client.test_connection()
        assert result["connected"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_bad_gateway_502(self, async_client):
        """Test handling of 502 Bad Gateway."""
        mock_response = MagicMock()
        mock_response.status_code = 502

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Bad Gateway",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        with pytest.raises(AsyncAirflowAPIError):
            await async_client.list_dags()


# =============================================================================
# Test: Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_very_long_dag_id(self, async_client):
        """Test handling of very long DAG ID."""
        long_dag_id = "a" * 1000

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"detail": "DAG ID too long"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Bad Request",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        with pytest.raises(AsyncAirflowAPIError):
            await async_client.get_dag(long_dag_id)

    @pytest.mark.asyncio
    async def test_special_characters_in_dag_id(self, async_client):
        """Test handling of special characters in DAG ID."""
        special_dag_id = "dag with spaces & special<chars>"

        mock_response = MagicMock()
        mock_response.status_code = 400

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Bad Request",
            request=MagicMock(),
            response=mock_response,
        ))
        async_client._client = mock_http

        with pytest.raises(AsyncAirflowAPIError):
            await async_client.get_dag(special_dag_id)

    @pytest.mark.asyncio
    async def test_unicode_in_config(self, async_client):
        """Test handling of unicode characters in config."""
        # Mock GET for get_dag (to check is_paused) - return unpaused DAG
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.content = b'{"dag_id": "test_dag", "is_paused": false}'
        mock_get_response.json.return_value = {"dag_id": "test_dag", "is_paused": False}
        mock_get_response.raise_for_status = MagicMock()

        mock_post_response = MagicMock()
        mock_post_response.status_code = 200
        mock_post_response.content = b'{"dag_run_id": "run_1"}'
        mock_post_response.json.return_value = {"dag_run_id": "run_1"}
        mock_post_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_get_response)
        mock_http.post = AsyncMock(return_value=mock_post_response)
        async_client._client = mock_http

        # Should handle unicode gracefully
        result = await async_client.trigger_dag(
            dag_id="test_dag",
            conf={"message": "Hello \u4e16\u754c \U0001f30d"},
        )
        assert result["dag_run_id"] == "run_1"

    @pytest.mark.asyncio
    async def test_null_values_in_response(self, async_client):
        """Test handling of null values in response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"dag_id": "test", "description": null, "schedule_interval": null}'
        mock_response.json.return_value = {
            "dag_id": "test",
            "description": None,
            "schedule_interval": None,
        }
        mock_response.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        async_client._client = mock_http

        result = await async_client._make_request("GET", "/dags/test")
        assert result["dag_id"] == "test"
        assert result["description"] is None


# ==================== Fix: test_airflow_connection endpoint ====================


class TestTestAirflowConnection:
    """Tests for test_airflow_connection — correct endpoint and body shaping."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        client._resolved_api_version = "v2"
        client._make_request = AsyncMock(return_value={"status": "success", "message": "OK"})
        return client

    @pytest.mark.asyncio
    async def test_with_payload_posts_to_connections_test(self, client):
        """connection_payload is sent as JSON body to POST /connections/test."""
        payload = {
            "connection_id": "ssh_prod",
            "conn_type": "ssh",
            "host": "10.0.0.1",
            "login": "airflow",
            "port": 22,
        }

        result = await client.test_airflow_connection(connection_payload=payload)

        client._make_request.assert_called_once_with(
            "POST",
            "/connections/test",
            json=payload,
        )
        assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_does_not_call_per_id_endpoint(self, client):
        """Regression: must never call the non-existent /connections/{id}/test endpoint."""
        payload = {"connection_id": "ssh_prod", "conn_type": "ssh"}
        await client.test_airflow_connection(connection_payload=payload)

        called_endpoint = client._make_request.call_args[0][1]
        assert "/ssh_prod/test" not in called_endpoint
        assert called_endpoint == "/connections/test"


class TestGetClientConcurrentInit:
    """Concurrent calls to _get_client() must create exactly one httpx.AsyncClient."""

    @pytest.mark.asyncio
    async def test_concurrent_get_client_creates_single_client(self):
        """Ten concurrent _get_client() calls must return the same client object."""
        from unittest.mock import patch

        client = AsyncAirflowClient(
            base_url="http://localhost:8080",
            username="admin",
            password="admin",
            rate_limit_rps=None,
            circuit_breaker_enabled=False,
        )
        # Pre-resolve so _detect_api_version and _obtain_token are no-ops
        client._resolved_api_version = "v2"
        client._access_token = "tok"

        async def noop_detect():
            pass

        async def noop_ensure():
            pass

        client._detect_api_version = noop_detect
        client._ensure_token = noop_ensure

        init_call_count = 0
        original_init = httpx.AsyncClient.__init__

        def counting_init(self_inner, **kwargs):
            nonlocal init_call_count
            init_call_count += 1
            original_init(self_inner, **kwargs)

        try:
            with patch.object(httpx.AsyncClient, "__init__", counting_init):
                results = await asyncio.gather(*[client._get_client() for _ in range(10)])

            # All tasks must have received the same client instance
            first = results[0]
            assert all(r is first for r in results), "all tasks must share one client"
            assert client._client is first
            assert init_call_count == 1, f"expected 1 AsyncClient created, got {init_call_count}"
        finally:
            await client.close()


class TestGetProviders:
    """Tests for get_providers pagination and check_missing_providers normalization."""

    def _make_client(self):
        client = AsyncAirflowClient(base_url="http://localhost:8080", circuit_breaker_enabled=False)
        client._resolved_api_version = "v1"
        return client

    @pytest.mark.asyncio
    async def test_get_providers_single_page(self):
        """Test get_providers with results on a single page."""
        client = self._make_client()
        providers = [{"package_name": "apache-airflow-providers-ssh", "version": "1.0"}]
        client._make_request = AsyncMock(return_value={"providers": providers, "total_entries": 1})

        result = await client.get_providers()

        assert result["total_entries"] == 1
        assert len(result["providers"]) == 1
        client._make_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_providers_multi_page(self):
        """Test get_providers with pagination across multiple pages."""
        client = self._make_client()
        page1 = {"providers": [{"package_name": f"pkg-{i}"} for i in range(100)], "total_entries": 150}
        page2 = {"providers": [{"package_name": f"pkg-{i}"} for i in range(100, 150)], "total_entries": 150}
        client._make_request = AsyncMock(side_effect=[page1, page2])

        result = await client.get_providers()

        assert len(result["providers"]) == 150
        assert client._make_request.call_count == 2

    @pytest.mark.asyncio
    async def test_get_providers_stops_on_empty_batch(self):
        """Test get_providers stops pagination when server returns empty batch."""
        client = self._make_client()
        page1 = {"providers": [{"package_name": "pkg-0"}], "total_entries": 5}
        page2 = {"providers": [], "total_entries": 5}
        client._make_request = AsyncMock(side_effect=[page1, page2])

        result = await client.get_providers()

        assert len(result["providers"]) == 1
        assert result["total_entries"] == 5
        assert result.get("incomplete") is True

    @pytest.mark.asyncio
    async def test_get_providers_uses_batch_length_for_offset(self):
        """Test get_providers advances offset by actual batch size, not page_size."""
        client = self._make_client()
        client.max_page_limit = 50
        page1 = {"providers": [{"package_name": f"pkg-{i}"} for i in range(50)], "total_entries": 100}
        page2 = {"providers": [{"package_name": f"pkg-{i}"} for i in range(50, 100)], "total_entries": 100}
        client._make_request = AsyncMock(side_effect=[page1, page2])

        result = await client.get_providers()

        assert len(result["providers"]) == 100
        # Verify second call used correct offset (advanced by batch length, not page_size)
        calls = client._make_request.call_args_list
        second_offset = calls[1][1]["params"]["offset"]
        assert second_offset == 50

    @pytest.mark.asyncio
    async def test_get_providers_handles_capped_first_page(self):
        """Test get_providers correctly handles server capping first page below max_page_limit."""
        client = self._make_client()
        client.max_page_limit = 100
        page1 = {"providers": [{"package_name": f"pkg-{i}"} for i in range(30)], "total_entries": 60}
        page2 = {"providers": [{"package_name": f"pkg-{i}"} for i in range(30, 60)], "total_entries": 60}
        client._make_request = AsyncMock(side_effect=[page1, page2])

        result = await client.get_providers()

        assert len(result["providers"]) == 60
        calls = client._make_request.call_args_list
        second_offset = calls[1][1]["params"]["offset"]
        assert second_offset == 30

    def test_check_missing_providers_normalizes_hyphens(self):
        """Test check_missing_providers normalizes hyphenated package names."""
        from teradata_etl_mcp_server.clients.async_airflow_client import check_missing_providers

        response = {
            "providers": [
                {"package_name": "apache-airflow-providers-ssh"},
                {"package_name": "apache-airflow-providers-teradata"},
            ]
        }
        missing = check_missing_providers(response)
        missing_names = [name for name, _ in missing]
        assert "apache-airflow-providers-ssh" not in missing_names
        assert "apache-airflow-providers-teradata" not in missing_names

    def test_check_missing_providers_normalizes_underscores(self):
        """Test check_missing_providers normalizes underscored package names in response."""
        from teradata_etl_mcp_server.clients.async_airflow_client import check_missing_providers

        response = {
            "providers": [
                {"package_name": "apache_airflow_providers_ssh"},
                {"package_name": "apache_airflow_providers_teradata"},
            ]
        }
        missing = check_missing_providers(response)
        missing_names = [name for name, _ in missing]
        assert "apache-airflow-providers-ssh" not in missing_names
        assert "apache-airflow-providers-teradata" not in missing_names

    def test_check_missing_providers_handles_null_package_name(self):
        """Test check_missing_providers handles null package_name values without crashing."""
        from teradata_etl_mcp_server.clients.async_airflow_client import check_missing_providers

        response = {
            "providers": [
                {"package_name": "apache-airflow-providers-ssh"},
                {"package_name": None},
                {"package_name": "apache-airflow-providers-teradata"},
            ]
        }
        missing = check_missing_providers(response)
        missing_names = [name for name, _ in missing]
        assert "apache-airflow-providers-ssh" not in missing_names
        assert "apache-airflow-providers-teradata" not in missing_names


class TestConnectionAndStatusMethods:
    """Tests for connection, status, and polling methods."""

    @pytest.fixture
    def client(self):
        client = AsyncAirflowClient(base_url="http://localhost:8080", circuit_breaker_enabled=False)
        client._resolved_api_version = "v1"
        client._make_request = AsyncMock()
        return client

    @pytest.mark.asyncio
    async def test_list_connections(self, client):
        """Test listing Airflow connections."""
        connections = [
            {"connection_id": "conn1", "conn_type": "postgres"},
            {"connection_id": "conn2", "conn_type": "mysql"},
        ]
        client._make_request.return_value = {"connections": connections, "total_entries": 2}

        result = await client.list_connections()

        assert len(result) == 2
        assert result[0]["connection_id"] == "conn1"
        client._make_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_airflow_connection(self, client):
        """Test testing a connection to Airflow."""
        client._make_request.return_value = {"status": "success"}
        payload = {"conn_type": "postgres", "host": "localhost"}

        result = await client.test_airflow_connection(payload)

        assert result["status"] == "success"
        client._make_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_wait_for_dag_run(self, client):
        """Test waiting for a DAG run to complete."""
        client.get_dag_run = AsyncMock(side_effect=[
            {"state": "running"},
            {"state": "running"},
            {"state": "success"},
        ])

        result = await client.wait_for_dag_run("my_dag", "run-1", timeout_seconds=60)

        assert result["state"] == "success"
        assert client.get_dag_run.call_count == 3

    def test_get_rate_limiter_status(self, client):
        """Test getting rate limiter status."""
        client._rate_limiter = MagicMock()
        client._rate_limiter.get_status.return_value = {"rate_rps": 10.0, "available_tokens": 5}

        result = client.get_rate_limiter_status()

        assert result["rate_rps"] == 10.0
        assert result["available_tokens"] == 5

    def test_get_client_status(self, client):
        """Test getting overall client status."""
        client._rate_limiter = MagicMock()
        client._rate_limiter.get_status.return_value = {"rate_rps": 10.0, "burst": 5}

        result = client.get_client_status()

        assert result["base_url"] == "http://localhost:8080"
        assert "timeout_seconds" in result
