"""Circuit breaker implementations for resilient service calls.

This module provides circuit breaker patterns for protecting against
cascading failures when calling external services.

Includes:
- InMemoryCircuitBreaker: Single-instance, thread-safe
- RedisCircuitBreaker: Distributed, multi-instance coordination
- CircuitBreakerFactory: Factory for selecting implementation
"""

import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from urllib.parse import urlparse, urlunparse

from elt_mcp_server.response_sanitizer import safe_error_message

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation, requests allowed
    OPEN = "open"  # Failures exceeded threshold, requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreakerBase(ABC):
    """Abstract base class for circuit breaker implementations."""

    @property
    @abstractmethod
    def state(self) -> CircuitState:
        """Get current circuit state."""
        ...

    @property
    @abstractmethod
    def is_available(self) -> bool:
        """Check if requests are allowed."""
        ...

    @abstractmethod
    def record_success(self) -> None:
        """Record a successful request."""
        ...

    @abstractmethod
    def record_failure(self) -> None:
        """Record a failed request."""
        ...

    @abstractmethod
    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        ...

    @abstractmethod
    def get_status(self) -> dict[str, Any]:
        """Get circuit breaker status for monitoring."""
        ...


@dataclass
class InMemoryCircuitBreaker(CircuitBreakerBase):
    """
    In-memory circuit breaker for single-instance deployments.

    Thread-safe implementation using locks. State is not shared
    across multiple server instances.

    States:
    - CLOSED: Normal operation, all requests pass through
    - OPEN: Service unhealthy, requests blocked for recovery_timeout
    - HALF_OPEN: Testing recovery, limited requests allowed
    """

    failure_threshold: int = 5  # Failures before opening circuit
    recovery_timeout: float = 60.0  # Seconds before trying recovery
    half_open_max_calls: int = 3  # Max test calls in half-open state
    name: str = "default"  # Circuit breaker name for logging

    # Internal state (not constructor args)
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float | None = field(default=None, init=False)
    _half_open_calls: int = field(default=0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    @property
    def state(self) -> CircuitState:
        """Get current circuit state, transitioning if needed."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if (
                    self._last_failure_time
                    and (time.time() - self._last_failure_time) >= self.recovery_timeout
                ):
                    logger.info(
                        "[%s] Circuit breaker transitioning to HALF_OPEN for recovery test",
                        self.name,
                    )
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
            return self._state

    @property
    def is_available(self) -> bool:
        """Check if requests are allowed through the circuit."""
        current_state = self.state  # This may trigger state transition
        if current_state == CircuitState.CLOSED:
            return True
        if current_state == CircuitState.HALF_OPEN:
            with self._lock:
                return self._half_open_calls < self.half_open_max_calls
        return False  # OPEN state

    def record_success(self) -> None:
        """Record a successful request, potentially closing the circuit."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                if self._half_open_calls >= self.half_open_max_calls:
                    logger.info(
                        "[%s] Circuit breaker CLOSED after %d successful recovery calls",
                        self.name,
                        self._half_open_calls,
                    )
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._last_failure_time = None
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success (sliding window behavior)
                if self._failure_count > 0:
                    self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed request, potentially opening the circuit."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Any failure during recovery test reopens circuit
                logger.warning("[%s] Circuit breaker OPEN: failure during recovery test", self.name)
                self._state = CircuitState.OPEN
            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self.failure_threshold
            ):
                logger.warning(
                    "[%s] Circuit breaker OPEN: %d failures exceeded threshold %d",
                    self.name,
                    self._failure_count,
                    self.failure_threshold,
                )
                self._state = CircuitState.OPEN

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._last_failure_time = None
            self._half_open_calls = 0
            logger.info("[%s] Circuit breaker manually reset to CLOSED", self.name)

    def get_status(self) -> dict[str, Any]:
        """Get circuit breaker status for monitoring."""
        with self._lock:
            time_until_recovery = None
            if self._state == CircuitState.OPEN and self._last_failure_time:
                elapsed = time.time() - self._last_failure_time
                time_until_recovery = max(0, self.recovery_timeout - elapsed)

            # Compute is_available without calling the property (avoids deadlock)
            current_state = self._state
            if current_state == CircuitState.CLOSED:
                available = True
            elif current_state == CircuitState.HALF_OPEN:
                available = self._half_open_calls < self.half_open_max_calls
            else:  # OPEN
                available = False

            return {
                "name": self.name,
                "type": "in_memory",
                "state": self._state.value,
                "failure_count": self._failure_count,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout_seconds": self.recovery_timeout,
                "time_until_recovery": time_until_recovery,
                "is_available": available,
            }


class RedisCircuitBreaker(CircuitBreakerBase):
    """
    Redis-backed circuit breaker for distributed deployments.

    Shares state across multiple server instances using Redis.
    Falls back to in-memory behavior if Redis is unavailable.

    Redis keys used:
    - cb:{name}:state - Current state (closed/open/half_open)
    - cb:{name}:failure_count - Number of consecutive failures
    - cb:{name}:last_failure - Unix timestamp of last failure
    - cb:{name}:half_open_calls - Number of test calls in half-open
    """

    def __init__(
        self,
        redis_url: str,
        name: str = "default",
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
        key_prefix: str = "cb",
    ):
        """
        Initialize Redis circuit breaker.

        Args:
            redis_url: Redis connection URL (redis://host:port/db)
            name: Circuit breaker name (used in Redis keys)
            failure_threshold: Failures before opening circuit
            recovery_timeout: Seconds before trying recovery
            half_open_max_calls: Max test calls in half-open state
            key_prefix: Prefix for Redis keys
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.key_prefix = key_prefix
        self._redis_url = redis_url
        self._redis: Any = None
        self._redis_available = False
        self._fallback = InMemoryCircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            half_open_max_calls=half_open_max_calls,
            name=f"{name}_fallback",
        )
        self._init_redis()

    def _init_redis(self) -> None:
        """Initialize Redis connection."""
        try:
            import redis

            self._redis = redis.from_url(self._redis_url, decode_responses=True)
            # Test connection
            self._redis.ping()
            self._redis_available = True
            # Mask credentials in Redis URL for logging using urllib.parse
            try:
                parsed = urlparse(self._redis_url)
                if parsed.password:
                    # Replace password with ***
                    masked_netloc = f"{parsed.username}:***@{parsed.hostname}"
                    if parsed.port:
                        masked_netloc += f":{parsed.port}"
                    masked_url = urlunparse((
                        parsed.scheme, masked_netloc, parsed.path,
                        parsed.params, "", "",
                    ))
                elif parsed.query or parsed.fragment:
                    # Strip query params/fragments that might contain credentials
                    masked_url = urlunparse((
                        parsed.scheme, parsed.netloc, parsed.path,
                        parsed.params, "", "",
                    ))
                else:
                    masked_url = self._redis_url
            except Exception:
                # Fallback: always mask to prevent credential exposure
                masked_url = "[redis-url-masked]"
            logger.info(
                "[%s] Redis circuit breaker connected to %s",
                self.name,
                masked_url,
            )
        except ImportError:
            logger.warning(
                "[%s] redis package not installed, falling back to in-memory circuit breaker",
                self.name,
            )
            self._redis_available = False
        except Exception as e:
            logger.warning(
                "[%s] Redis connection failed (%s), falling back to in-memory circuit breaker",
                self.name,
                safe_error_message(e),
            )
            self._redis_available = False

    def _key(self, suffix: str) -> str:
        """Generate Redis key with prefix and name."""
        return f"{self.key_prefix}:{self.name}:{suffix}"

    @property
    def state(self) -> CircuitState:
        """Get current circuit state from Redis."""
        if not self._redis_available:
            return self._fallback.state

        try:
            state_str = self._redis.get(self._key("state")) or "closed"
            current_state = CircuitState(state_str)

            # Check for state transition from OPEN to HALF_OPEN
            if current_state == CircuitState.OPEN:
                last_failure = self._redis.get(self._key("last_failure"))
                if last_failure:
                    elapsed = time.time() - float(last_failure)
                    if elapsed >= self.recovery_timeout:
                        # Atomically transition to half-open
                        pipe = self._redis.pipeline()
                        pipe.set(self._key("state"), CircuitState.HALF_OPEN.value)
                        pipe.set(self._key("half_open_calls"), 0)
                        pipe.execute()
                        logger.info(
                            "[%s] Circuit breaker transitioning to HALF_OPEN for recovery test",
                            self.name,
                        )
                        return CircuitState.HALF_OPEN

            return current_state
        except Exception as e:
            logger.warning("[%s] Redis read failed (%s), using fallback", self.name, e)
            return self._fallback.state

    @property
    def is_available(self) -> bool:
        """Check if requests are allowed through the circuit."""
        if not self._redis_available:
            return self._fallback.is_available

        current_state = self.state
        if current_state == CircuitState.CLOSED:
            return True
        if current_state == CircuitState.HALF_OPEN:
            try:
                half_open_calls = int(self._redis.get(self._key("half_open_calls")) or 0)
                return half_open_calls < self.half_open_max_calls
            except Exception:
                return self._fallback.is_available
        return False

    def record_success(self) -> None:
        """Record a successful request in Redis."""
        if not self._redis_available:
            self._fallback.record_success()
            return

        try:
            state_str = self._redis.get(self._key("state")) or "closed"
            current_state = CircuitState(state_str)

            if current_state == CircuitState.HALF_OPEN:
                # Increment half-open calls and check if we should close
                new_count = self._redis.incr(self._key("half_open_calls"))
                if new_count >= self.half_open_max_calls:
                    # Reset to closed state
                    pipe = self._redis.pipeline()
                    pipe.set(self._key("state"), CircuitState.CLOSED.value)
                    pipe.set(self._key("failure_count"), 0)
                    pipe.delete(self._key("last_failure"))
                    pipe.delete(self._key("half_open_calls"))
                    pipe.execute()
                    logger.info(
                        "[%s] Circuit breaker CLOSED after %d successful recovery calls",
                        self.name,
                        new_count,
                    )
            elif current_state == CircuitState.CLOSED:
                # Decrement failure count (sliding window)
                failure_count = int(self._redis.get(self._key("failure_count")) or 0)
                if failure_count > 0:
                    self._redis.decr(self._key("failure_count"))
        except Exception as e:
            logger.warning("[%s] Redis write failed (%s), using fallback", self.name, e)
            self._fallback.record_success()

    def record_failure(self) -> None:
        """Record a failed request in Redis."""
        if not self._redis_available:
            self._fallback.record_failure()
            return

        try:
            pipe = self._redis.pipeline()
            pipe.incr(self._key("failure_count"))
            pipe.set(self._key("last_failure"), time.time())
            pipe.execute()

            state_str = self._redis.get(self._key("state")) or "closed"
            current_state = CircuitState(state_str)
            failure_count = int(self._redis.get(self._key("failure_count")) or 0)

            if current_state == CircuitState.HALF_OPEN:
                # Any failure during recovery reopens
                self._redis.set(self._key("state"), CircuitState.OPEN.value)
                logger.warning("[%s] Circuit breaker OPEN: failure during recovery test", self.name)
            elif current_state == CircuitState.CLOSED and failure_count >= self.failure_threshold:
                self._redis.set(self._key("state"), CircuitState.OPEN.value)
                logger.warning(
                    "[%s] Circuit breaker OPEN: %d failures exceeded threshold %d",
                    self.name,
                    failure_count,
                    self.failure_threshold,
                )
        except Exception as e:
            logger.warning("[%s] Redis write failed (%s), using fallback", self.name, e)
            self._fallback.record_failure()

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        if not self._redis_available:
            self._fallback.reset()
            return

        try:
            pipe = self._redis.pipeline()
            pipe.set(self._key("state"), CircuitState.CLOSED.value)
            pipe.set(self._key("failure_count"), 0)
            pipe.delete(self._key("last_failure"))
            pipe.delete(self._key("half_open_calls"))
            pipe.execute()
            logger.info("[%s] Circuit breaker manually reset to CLOSED", self.name)
        except Exception as e:
            logger.warning("[%s] Redis write failed (%s), using fallback", self.name, e)
            self._fallback.reset()

    def get_status(self) -> dict[str, Any]:
        """Get circuit breaker status from Redis."""
        if not self._redis_available:
            status = self._fallback.get_status()
            status["type"] = "in_memory_fallback"
            status["redis_available"] = False
            return status

        try:
            state_str = self._redis.get(self._key("state")) or "closed"
            failure_count = int(self._redis.get(self._key("failure_count")) or 0)
            last_failure = self._redis.get(self._key("last_failure"))

            time_until_recovery = None
            if state_str == CircuitState.OPEN.value and last_failure:
                elapsed = time.time() - float(last_failure)
                time_until_recovery = max(0, self.recovery_timeout - elapsed)

            return {
                "name": self.name,
                "type": "redis",
                "redis_available": True,
                "state": state_str,
                "failure_count": failure_count,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout_seconds": self.recovery_timeout,
                "time_until_recovery": time_until_recovery,
                "is_available": self.is_available,
            }
        except Exception as e:
            logger.warning("[%s] Redis read failed (%s), using fallback status", self.name, e)
            status = self._fallback.get_status()
            status["type"] = "in_memory_fallback"
            status["redis_available"] = False
            status["redis_error"] = str(e)
            return status


class CircuitBreakerFactory:
    """
    Factory for creating circuit breaker instances.

    Automatically selects between Redis and in-memory implementations
    based on configuration.
    """

    @staticmethod
    def create(
        name: str = "default",
        redis_url: str | None = None,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
    ) -> CircuitBreakerBase:
        """
        Create a circuit breaker instance.

        Args:
            name: Circuit breaker name
            redis_url: Optional Redis URL for distributed circuit breaker
            failure_threshold: Failures before opening circuit
            recovery_timeout: Seconds before trying recovery
            half_open_max_calls: Max test calls in half-open state

        Returns:
            Circuit breaker instance (Redis if URL provided, else in-memory)
        """
        if redis_url:
            return RedisCircuitBreaker(
                redis_url=redis_url,
                name=name,
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout,
                half_open_max_calls=half_open_max_calls,
            )
        return InMemoryCircuitBreaker(
            name=name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            half_open_max_calls=half_open_max_calls,
        )
