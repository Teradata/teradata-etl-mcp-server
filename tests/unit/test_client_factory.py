"""Unit tests for ClientFactory workflow orchestrator methods.

Tests cover:
- ClientFactoryProtocol.create_workflow_orchestrator method
- DefaultClientFactory.create_workflow_orchestrator implementation
- MockClientFactory workflow orchestrator support
- LazyClientFactory workflow orchestrator caching
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import SecretStr

from elt_mcp_server.client_factory import (
    ClientFactoryBase,
    DefaultClientFactory,
    LazyClientFactory,
)
from elt_mcp_server.config import (
    AirbyteSettings,
    AirflowSettings,
    DBTSettings,
    OrchestratorSettings,
    PipelineSettings,
    Settings,
    TeradataSettings,
)
from elt_mcp_server.workflow import WorkflowOrchestratorProtocol
from tests.unit.mock_client_factory import MockClientFactory


@pytest.fixture
def minimal_settings(tmp_path):
    """Create minimal settings for testing."""
    return Settings(
        teradata=TeradataSettings(
            host="localhost",
            username="dbc",
            password=SecretStr("dbc"),
            database="test_db",
            port=1025,
        ),
        airflow=AirflowSettings(
            base_url="http://localhost:8080",
            username="admin",
            password=SecretStr("admin"),
        ),
        airbyte=AirbyteSettings(
            enabled=True,
            base_url="http://localhost:8000",
            client_id="airbyte",
            client_secret=SecretStr("password"),
        ),
        dbt=DBTSettings(
            project_dir=tmp_path / "dbt_project",
            profiles_dir=tmp_path / "profiles",
            target="dev",
            threads=4,
        ),
        pipeline=PipelineSettings(
            dags_output_dir=tmp_path / "dags",
        ),
        orchestrator=OrchestratorSettings(backend="airflow"),
    )


class TestClientFactoryProtocol:
    """Tests for ClientFactoryProtocol interface."""

    def test_protocol_defines_create_workflow_orchestrator(self):
        """Verify protocol defines create_workflow_orchestrator method."""
        from elt_mcp_server.client_factory import ClientFactoryProtocol

        # Check method exists in protocol annotations
        assert hasattr(ClientFactoryProtocol, "create_workflow_orchestrator")


class TestDefaultClientFactoryWorkflowOrchestrator:
    """Tests for DefaultClientFactory.create_workflow_orchestrator."""

    def test_create_workflow_orchestrator_airflow(self, minimal_settings):
        """Test creating Airflow workflow orchestrator."""
        minimal_settings.orchestrator.backend = "airflow"
        factory = DefaultClientFactory(minimal_settings)

        # Mock the async airflow client creation
        with patch.object(factory, "create_async_airflow_client") as mock_create:
            mock_client = AsyncMock()
            mock_create.return_value = mock_client

            orchestrator = factory.create_workflow_orchestrator()

            assert orchestrator is not None
            assert orchestrator.backend_name == "airflow"
            mock_create.assert_called_once()

    def test_create_workflow_orchestrator_unsupported_backend(self, minimal_settings):
        """Test creating orchestrator with unsupported backend raises error."""
        # Force an unsupported backend by patching
        with patch.object(minimal_settings.orchestrator, "backend", "unsupported"):
            factory = DefaultClientFactory(minimal_settings)

            with pytest.raises(ValueError) as exc_info:
                factory.create_workflow_orchestrator()

            assert "Unsupported" in str(exc_info.value)


class TestMockClientFactoryWorkflowOrchestrator:
    """Tests for MockClientFactory workflow orchestrator support."""

    def test_set_workflow_orchestrator(self):
        """Test setting mock workflow orchestrator."""
        factory = MockClientFactory()
        mock_orchestrator = Mock(spec=WorkflowOrchestratorProtocol)

        result = factory.set_workflow_orchestrator(mock_orchestrator)

        assert result is factory  # Returns self for chaining
        assert factory._workflow_orchestrator is mock_orchestrator

    def test_create_workflow_orchestrator_returns_mock(self):
        """Test create_workflow_orchestrator returns set mock."""
        factory = MockClientFactory()
        mock_orchestrator = Mock(spec=WorkflowOrchestratorProtocol)
        factory.set_workflow_orchestrator(mock_orchestrator)

        result = factory.create_workflow_orchestrator()

        assert result is mock_orchestrator

    def test_create_workflow_orchestrator_not_set_raises_error(self):
        """Test create_workflow_orchestrator raises error if not set."""
        factory = MockClientFactory()

        with pytest.raises(ValueError) as exc_info:
            factory.create_workflow_orchestrator()

        assert "not set" in str(exc_info.value)
        assert "set_workflow_orchestrator()" in str(exc_info.value)

    def test_chained_setup(self):
        """Test fluent API for setting multiple mocks."""
        mock_airflow = Mock()
        mock_airbyte = Mock()
        mock_orchestrator = Mock(spec=WorkflowOrchestratorProtocol)

        factory = (
            MockClientFactory()
            .set_async_airflow_client(mock_airflow)
            .set_airbyte_client(mock_airbyte)
            .set_workflow_orchestrator(mock_orchestrator)
        )

        assert factory.create_async_airflow_client() is mock_airflow
        assert factory.create_airbyte_client() is mock_airbyte
        assert factory.create_workflow_orchestrator() is mock_orchestrator


class TestLazyClientFactoryWorkflowOrchestrator:
    """Tests for LazyClientFactory workflow orchestrator caching."""

    def test_create_workflow_orchestrator_delegates_to_factory(self, minimal_settings):
        """Test lazy factory delegates to underlying factory."""
        inner_factory = Mock(spec=ClientFactoryBase)
        mock_orchestrator = Mock(spec=WorkflowOrchestratorProtocol)
        inner_factory.create_workflow_orchestrator.return_value = mock_orchestrator

        lazy_factory = LazyClientFactory(inner_factory)
        result = lazy_factory.create_workflow_orchestrator()

        assert result is mock_orchestrator
        inner_factory.create_workflow_orchestrator.assert_called_once()

    def test_create_workflow_orchestrator_caches_result(self, minimal_settings):
        """Test lazy factory caches orchestrator instance."""
        inner_factory = Mock(spec=ClientFactoryBase)
        mock_orchestrator = Mock(spec=WorkflowOrchestratorProtocol)
        inner_factory.create_workflow_orchestrator.return_value = mock_orchestrator

        lazy_factory = LazyClientFactory(inner_factory)

        # Call twice
        result1 = lazy_factory.create_workflow_orchestrator()
        result2 = lazy_factory.create_workflow_orchestrator()

        # Should only delegate once
        assert result1 is result2
        assert inner_factory.create_workflow_orchestrator.call_count == 1

    def test_reset_clears_workflow_orchestrator_cache(self, minimal_settings):
        """Test reset clears cached workflow orchestrator."""
        inner_factory = Mock(spec=ClientFactoryBase)
        mock_orchestrator1 = Mock(spec=WorkflowOrchestratorProtocol)
        mock_orchestrator2 = Mock(spec=WorkflowOrchestratorProtocol)
        inner_factory.create_workflow_orchestrator.side_effect = [
            mock_orchestrator1, mock_orchestrator2
        ]

        lazy_factory = LazyClientFactory(inner_factory)

        result1 = lazy_factory.create_workflow_orchestrator()
        lazy_factory.reset()
        result2 = lazy_factory.create_workflow_orchestrator()

        assert result1 is mock_orchestrator1
        assert result2 is mock_orchestrator2
        assert inner_factory.create_workflow_orchestrator.call_count == 2


class TestOrchestratorSettingsIntegration:
    """Tests for OrchestratorSettings configuration integration."""

    def test_orchestrator_settings_defaults(self):
        """Test OrchestratorSettings default values."""
        settings = OrchestratorSettings()
        assert settings.backend == "airflow"

    def test_orchestrator_settings_in_main_settings(self, tmp_path):
        """Test OrchestratorSettings is part of main Settings."""
        settings = Settings(
            teradata=TeradataSettings(
                host="localhost",
                username="dbc",
                password=SecretStr("dbc"),
                database="test_db",
            ),
            airflow=AirflowSettings(
                base_url="http://localhost:8080",
                username="admin",
                password=SecretStr("admin"),
            ),
            airbyte=AirbyteSettings(
                enabled=True,
                base_url="http://localhost:8000",
                client_id="airbyte",
                client_secret=SecretStr("password"),
            ),
            dbt=DBTSettings(
                project_dir=tmp_path / "dbt",
                profiles_dir=tmp_path / "profiles",
            ),
            pipeline=PipelineSettings(dags_output_dir=tmp_path / "dags"),
            orchestrator=OrchestratorSettings(backend="airflow"),
        )

        assert settings.orchestrator.backend == "airflow"
