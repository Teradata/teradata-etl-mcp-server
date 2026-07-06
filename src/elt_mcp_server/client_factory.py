"""Client factory for dependency injection.

This module provides a factory pattern for creating client instances,
enabling easy dependency injection.

Supports async clients for Airflow operations.

Usage:
    factory = DefaultClientFactory(settings)
    async_airflow = factory.create_async_airflow_client()
    orchestrator = factory.create_workflow_orchestrator()
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .auth import TeradataAuth
    from .clients.airbyte_client import AirbyteClient
    from .clients.async_airflow_client import AsyncAirflowClient
    from .clients.dbt_client import DBTClient
    from .clients.teradata_client import TeradataClient
    from .clients.ttu_client import TTUClient
    from .config import Settings
    from .storage.metadata_store import MetadataStore
    from .workflow.protocol import WorkflowOrchestratorProtocol

logger = logging.getLogger(__name__)


@runtime_checkable
class ClientFactoryProtocol(Protocol):
    """Protocol for client factory implementations."""

    def create_async_airflow_client(self) -> "AsyncAirflowClient":
        """Create an async Airflow client instance."""
        ...

    def create_airbyte_client(
        self,
        metadata_store: "MetadataStore | None" = None,
    ) -> "AirbyteClient":
        """Create an Airbyte client instance."""
        ...

    def create_teradata_client(self) -> "TeradataClient":
        """Create a Teradata client instance."""
        ...

    def create_dbt_client(self) -> "DBTClient":
        """Create a dbt client instance."""
        ...

    def create_ttu_client(self) -> "TTUClient":
        """Create a TTU client instance."""
        ...

    def create_workflow_orchestrator(self) -> "WorkflowOrchestratorProtocol":
        """Create a workflow orchestrator based on configured backend."""
        ...


class ClientFactoryBase(ABC):
    """Abstract base class for client factories."""

    @abstractmethod
    def create_async_airflow_client(self) -> "AsyncAirflowClient":
        """Create an async Airflow client instance."""
        ...

    @abstractmethod
    def create_airbyte_client(
        self,
        metadata_store: "MetadataStore | None" = None,
    ) -> "AirbyteClient":
        """Create an Airbyte client instance."""
        ...

    @abstractmethod
    def create_teradata_client(self) -> "TeradataClient":
        """Create a Teradata client instance."""
        ...

    @abstractmethod
    def create_dbt_client(self) -> "DBTClient":
        """Create a dbt client instance."""
        ...

    @abstractmethod
    def create_ttu_client(self) -> "TTUClient":
        """Create a TTU client instance."""
        ...

    @abstractmethod
    def create_workflow_orchestrator(self) -> "WorkflowOrchestratorProtocol":
        """Create a workflow orchestrator based on configured backend."""
        ...


class DefaultClientFactory(ClientFactoryBase):
    """
    Default client factory that creates real client instances.

    Uses settings to configure each client with appropriate parameters.
    """

    def __init__(self, settings: "Settings"):
        """
        Initialize the default client factory.

        Args:
            settings: Application settings
        """
        self.settings = settings
        logger.debug("Initialized DefaultClientFactory")

    def create_async_airflow_client(self) -> "AsyncAirflowClient":
        """Create an async Airflow client instance from settings."""
        from .clients.async_airflow_client import AsyncAirflowClient

        password = self.settings.airflow.password
        if hasattr(password, "get_secret_value"):
            password = password.get_secret_value()

        return AsyncAirflowClient(
            base_url=self.settings.airflow.base_url,
            username=self.settings.airflow.username,
            password=password,
            auth_manager=self.settings.airflow.auth_manager,
            token_endpoint=self.settings.airflow.token_endpoint,
            timeout=self.settings.airflow.timeout,
            redis_url=self.settings.mcp.redis_url,
        )

    def create_airbyte_client(
        self,
        metadata_store: "MetadataStore | None" = None,
    ) -> "AirbyteClient":
        """Create an Airbyte client instance from settings."""
        from .clients.airbyte_client import AirbyteClient

        client_secret = self.settings.airbyte.client_secret
        if client_secret and hasattr(client_secret, "get_secret_value"):
            client_secret = client_secret.get_secret_value()

        return AirbyteClient(
            base_url=self.settings.airbyte.base_url,
            client_id=self.settings.airbyte.client_id,
            client_secret=client_secret,
            token_url=self.settings.airbyte.token_url or "/api/public/v1/applications/token",
            workspace_id=self.settings.airbyte.workspace_id,
            metadata_store=metadata_store,
        )

    def default_teradata_auth(self) -> "TeradataAuth":
        """Compose the default :class:`TeradataAuth` from the in-memory
        Teradata settings.

        Tools call this whenever they need the *default* identity (no
        explicit ``teradata_profile`` named). A new auth object is built
        on each call from ``self.settings.teradata``, but this method
        does NOT reload settings from ``.env`` or other external sources
        — ``self.settings`` is bound once when the factory is constructed.
        After the wizard rewrites ``.env`` the user must click "Reload
        Configuration", which restarts the server (and therefore re-runs
        ``Settings()``); only then will out-of-band ``.env`` updates be
        observable here.
        """
        from .auth import build_teradata_auth_from_settings

        return build_teradata_auth_from_settings(self.settings.teradata)

    def create_teradata_client(self) -> "TeradataClient":
        """Create a Teradata client instance bound to the default auth."""
        from .clients.teradata_client import TeradataClient

        return TeradataClient(auth=self.default_teradata_auth())

    def create_dbt_client(self) -> "DBTClient":
        """Create a dbt client instance from settings.

        Auth is passed per-call via :meth:`DBTClient.run` etc., not bound
        at construction — one dbt subprocess can use a different identity
        (from a connections.yaml profile) than another.
        """
        from .clients.dbt_client import DBTClient

        project_dir = Path(self.settings.dbt.project_dir)
        profiles_dir = (
            Path(self.settings.dbt.profiles_dir)
            if self.settings.dbt.profiles_dir
            else None
        )

        return DBTClient(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target=self.settings.dbt.target,
            threads=self.settings.dbt.threads,
            command_timeout=self.settings.dbt.command_timeout,
        )

    def create_ttu_client(self) -> "TTUClient":
        """Create a TTU client instance from settings.

        Auth is passed per-call via :meth:`TTUClient.execute_tdload` etc.,
        not bound at construction — profile overrides work on a per-call
        basis.
        """
        from .clients.ttu_client import TTUClient

        return TTUClient(
            tpt_binary=self.settings.ttu.tpt_binary_path,
            bteq_binary=self.settings.ttu.bteq_binary_path,
            tdload_binary=self.settings.ttu.tdload_binary_path,
            scripts_dir=self.settings.ttu.scripts_dir,
            command_timeout=self.settings.ttu.command_timeout,
            tpt_error_limit=self.settings.ttu.tpt_error_limit,
        )

    def create_workflow_orchestrator(self) -> "WorkflowOrchestratorProtocol":
        """Create a workflow orchestrator based on configured backend.

        The backend is determined by settings.orchestrator.backend.
        Currently only Airflow is supported.

        Returns:
            WorkflowOrchestratorProtocol implementation for the configured backend

        Raises:
            ValueError: If the backend is not supported
        """
        from .workflow import AirflowOrchestrator

        backend = self.settings.orchestrator.backend.lower()

        if backend == "airflow":
            # Create async Airflow client and wrap in orchestrator
            async_client = self.create_async_airflow_client()
            return AirflowOrchestrator(client=async_client)

        else:
            raise ValueError(
                f"Unsupported orchestrator backend: {backend}. "
                "Supported: airflow"
            )


class LazyClientFactory(ClientFactoryBase):
    """
    Lazy client factory that creates clients on first access.

    Wraps another factory and caches created instances.
    """

    def __init__(self, factory: ClientFactoryBase):
        """
        Initialize the lazy client factory.

        Args:
            factory: Underlying factory to use for client creation
        """
        self._factory = factory
        self._async_airflow_client: Any = None
        self._airbyte_client: Any = None
        self._teradata_client: Any = None
        self._dbt_client: Any = None
        self._ttu_client: Any = None
        logger.debug("Initialized LazyClientFactory")

    def create_async_airflow_client(self) -> "AsyncAirflowClient":
        """Get or create an async Airflow client instance."""
        if self._async_airflow_client is None:
            self._async_airflow_client = self._factory.create_async_airflow_client()
        return self._async_airflow_client

    def create_airbyte_client(
        self,
        metadata_store: "MetadataStore | None" = None,
    ) -> "AirbyteClient":
        """Get or create an Airbyte client instance."""
        if self._airbyte_client is None:
            self._airbyte_client = self._factory.create_airbyte_client(metadata_store)
        return self._airbyte_client

    def create_teradata_client(self) -> "TeradataClient":
        """Get or create a Teradata client instance."""
        if self._teradata_client is None:
            self._teradata_client = self._factory.create_teradata_client()
        return self._teradata_client

    def create_dbt_client(self) -> "DBTClient":
        """Get or create a dbt client instance."""
        if self._dbt_client is None:
            self._dbt_client = self._factory.create_dbt_client()
        return self._dbt_client

    def create_ttu_client(self) -> "TTUClient":
        """Get or create a TTU client instance."""
        if self._ttu_client is None:
            self._ttu_client = self._factory.create_ttu_client()
        return self._ttu_client

    def create_workflow_orchestrator(self) -> "WorkflowOrchestratorProtocol":
        """Get or create a workflow orchestrator instance."""
        if not hasattr(self, "_workflow_orchestrator") or self._workflow_orchestrator is None:
            self._workflow_orchestrator = self._factory.create_workflow_orchestrator()
        return self._workflow_orchestrator

    def reset(self) -> None:
        """Reset all cached clients."""
        self._async_airflow_client = None
        self._airbyte_client = None
        self._teradata_client = None
        self._dbt_client = None
        self._ttu_client = None
        if hasattr(self, "_workflow_orchestrator"):
            self._workflow_orchestrator = None
        logger.debug("LazyClientFactory cache reset")
