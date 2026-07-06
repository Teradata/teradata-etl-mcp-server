"""Unit tests for workflow package factory function.

Tests cover:
- create_orchestrator factory function
- Backend selection ("airflow")
- Error handling for unsupported backends
- Parameter passing to orchestrator constructors
"""

from unittest.mock import AsyncMock

import pytest

from elt_mcp_server.workflow import (
    AirflowOrchestrator,
    WorkflowOrchestratorProtocol,
    create_orchestrator,
)


class TestCreateOrchestratorAirflow:
    """Tests for create_orchestrator with Airflow backend."""

    def test_create_airflow_with_client(self):
        """Test creating Airflow orchestrator with client."""
        mock_client = AsyncMock()

        orchestrator = create_orchestrator(backend="airflow", client=mock_client)

        assert isinstance(orchestrator, AirflowOrchestrator)
        assert orchestrator.backend_name == "airflow"

    def test_create_airflow_case_insensitive(self):
        """Test backend name is case insensitive."""
        mock_client = AsyncMock()

        orchestrator1 = create_orchestrator(backend="AIRFLOW", client=mock_client)
        orchestrator2 = create_orchestrator(backend="Airflow", client=mock_client)

        assert isinstance(orchestrator1, AirflowOrchestrator)
        assert isinstance(orchestrator2, AirflowOrchestrator)

    def test_create_airflow_missing_client_raises_error(self):
        """Test creating Airflow orchestrator without client raises error."""
        with pytest.raises(ValueError) as exc_info:
            create_orchestrator(backend="airflow")

        assert "requires 'client'" in str(exc_info.value)
        assert "AsyncAirflowClient" in str(exc_info.value)


class TestCreateOrchestratorUnsupported:
    """Tests for create_orchestrator with unsupported backends."""

    def test_unsupported_backend_raises_error(self):
        """Test unsupported backend raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            create_orchestrator(backend="unknown")

        assert "Unsupported backend: unknown" in str(exc_info.value)

    def test_unsupported_backend_lists_supported(self):
        """Test error message lists supported backends."""
        with pytest.raises(ValueError) as exc_info:
            create_orchestrator(backend="invalid")

        error_msg = str(exc_info.value)
        assert "airflow" in error_msg


class TestCreateOrchestratorProtocolCompliance:
    """Tests verifying factory returns protocol-compliant orchestrators."""

    def test_airflow_satisfies_protocol(self):
        """Test Airflow orchestrator satisfies protocol."""
        mock_client = AsyncMock()
        orchestrator = create_orchestrator(backend="airflow", client=mock_client)

        assert isinstance(orchestrator, WorkflowOrchestratorProtocol)


class TestWorkflowPackageExports:
    """Tests verifying package exports are accessible."""

    def test_import_protocol(self):
        """Test WorkflowOrchestratorProtocol can be imported."""
        from elt_mcp_server.workflow import WorkflowOrchestratorProtocol
        assert WorkflowOrchestratorProtocol is not None

    def test_import_base_class(self):
        """Test WorkflowOrchestratorBase can be imported."""
        from elt_mcp_server.workflow import WorkflowOrchestratorBase
        assert WorkflowOrchestratorBase is not None

    def test_import_data_classes(self):
        """Test data classes can be imported."""
        from elt_mcp_server.workflow import (
            OrchestratorHealth,
            TaskRun,
            WorkflowDefinition,
            WorkflowRun,
            WorkflowState,
        )
        assert WorkflowState is not None
        assert WorkflowRun is not None
        assert WorkflowDefinition is not None
        assert TaskRun is not None
        assert OrchestratorHealth is not None

    def test_import_exceptions(self):
        """Test exception classes can be imported."""
        from elt_mcp_server.workflow import (
            CircuitBreakerOpenError,
            WorkflowNotFoundError,
            WorkflowOrchestratorError,
            WorkflowTimeoutError,
            WorkflowTriggerError,
        )
        assert WorkflowOrchestratorError is not None
        assert WorkflowTriggerError is not None
        assert WorkflowNotFoundError is not None
        assert WorkflowTimeoutError is not None
        assert CircuitBreakerOpenError is not None

    def test_import_orchestrators(self):
        """Test orchestrator classes can be imported."""
        from elt_mcp_server.workflow import AirflowOrchestrator
        assert AirflowOrchestrator is not None

    def test_import_factory_function(self):
        """Test create_orchestrator can be imported."""
        from elt_mcp_server.workflow import create_orchestrator
        assert create_orchestrator is not None
        assert callable(create_orchestrator)
