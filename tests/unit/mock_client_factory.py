"""Mock client factory for testing.

Moved from production client_factory.py — mock logic does not belong in production code.
"""

import logging
from typing import Any

from teradata_etl_mcp_server.client_factory import ClientFactoryBase

logger = logging.getLogger(__name__)


class MockClientFactory(ClientFactoryBase):
    """
    Mock client factory for testing.

    Allows setting mock client instances that will be returned
    by the factory methods.

    Example:
        mock_factory = MockClientFactory()
        mock_airflow = MagicMock(spec=AsyncAirflowClient)
        mock_factory.set_async_airflow_client(mock_airflow)

        orchestrator = PipelineOrchestrator(settings, client_factory=mock_factory)
        # orchestrator.async_airflow_client will now return mock_airflow
    """

    def __init__(self):
        """Initialize the mock client factory."""
        self._async_airflow_client: Any = None
        self._airbyte_client: Any = None
        self._teradata_client: Any = None
        self._dbt_client: Any = None
        self._ttu_client: Any = None
        logger.debug("Initialized MockClientFactory")

    def set_async_airflow_client(self, client: Any) -> "MockClientFactory":
        """Set the mock async Airflow client."""
        self._async_airflow_client = client
        return self

    def set_airbyte_client(self, client: Any) -> "MockClientFactory":
        """Set the mock Airbyte client."""
        self._airbyte_client = client
        return self

    def set_teradata_client(self, client: Any) -> "MockClientFactory":
        """Set the mock Teradata client."""
        self._teradata_client = client
        return self

    def set_dbt_client(self, client: Any) -> "MockClientFactory":
        """Set the mock dbt client."""
        self._dbt_client = client
        return self

    def set_ttu_client(self, client: Any) -> "MockClientFactory":
        """Set the mock TTU client."""
        self._ttu_client = client
        return self

    def create_async_airflow_client(self):
        """Return the mock async Airflow client."""
        if self._async_airflow_client is None:
            raise ValueError("Mock async Airflow client not set. Call set_async_airflow_client() first.")
        return self._async_airflow_client

    def create_airbyte_client(self, metadata_store=None):
        """Return the mock Airbyte client."""
        if self._airbyte_client is None:
            raise ValueError("Mock Airbyte client not set. Call set_airbyte_client() first.")
        return self._airbyte_client

    def create_teradata_client(self):
        """Return the mock Teradata client."""
        if self._teradata_client is None:
            raise ValueError("Mock Teradata client not set. Call set_teradata_client() first.")
        return self._teradata_client

    def create_dbt_client(self):
        """Return the mock dbt client."""
        if self._dbt_client is None:
            raise ValueError("Mock dbt client not set. Call set_dbt_client() first.")
        return self._dbt_client

    def create_ttu_client(self):
        """Return the mock TTU client."""
        if self._ttu_client is None:
            raise ValueError("Mock TTU client not set. Call set_ttu_client() first.")
        return self._ttu_client

    def set_workflow_orchestrator(self, orchestrator: Any) -> "MockClientFactory":
        """Set the mock workflow orchestrator."""
        self._workflow_orchestrator = orchestrator
        return self

    def create_workflow_orchestrator(self):
        """Return the mock workflow orchestrator."""
        if not hasattr(self, "_workflow_orchestrator") or self._workflow_orchestrator is None:
            raise ValueError("Mock workflow orchestrator not set. Call set_workflow_orchestrator() first.")
        return self._workflow_orchestrator
