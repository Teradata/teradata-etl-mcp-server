"""Unit tests aligned to the current PipelineOrchestrator implementation."""

from pathlib import Path
from unittest.mock import AsyncMock, Mock, MagicMock, patch

import pytest
from pydantic import SecretStr

from elt_mcp_server.config import (
    AirbyteSettings,
    AirflowSettings,
    DBTSettings,
    OrchestratorSettings,
    PipelineSettings,
    Settings,
    TeradataSettings,
)
from elt_mcp_server.orchestrator import PipelineOrchestrator


class TestOrchestrator:
    """Tests for the implemented orchestrator methods."""

    @pytest.fixture
    def settings(self, tmp_path: Path) -> Settings:
        """Minimal valid settings for orchestrator instantiation."""
        return Settings(
            teradata=TeradataSettings(host="localhost", username="dbc", password=SecretStr("dbc"), database="test_db", port=1025),
            airflow=AirflowSettings(base_url="http://localhost:8080", username="admin", password=SecretStr("admin")),
            airbyte=AirbyteSettings(enabled=True, base_url="http://localhost:8000", client_id="airbyte", client_secret=SecretStr("password")),
            dbt=DBTSettings(project_dir=tmp_path / "dbt_project", profiles_dir=tmp_path / "profiles", target="dev", threads=4),
            pipeline=PipelineSettings(dags_output_dir=tmp_path / "dags"),
        )

    @pytest.fixture
    def orchestrator(self, settings: Settings) -> PipelineOrchestrator:
        """Create orchestrator and inject mocked clients/generators."""
        orch = PipelineOrchestrator(settings)
        # Inject mocks to avoid real client interactions
        orch._teradata_client = Mock()
        orch._airbyte_client = Mock()
        orch._dbt_client = Mock()
        orch._dbt_generator = Mock()
        orch._airflow_dag_generator = Mock()
        # Provide missing dbt settings attributes expected by orchestrator via a stub
        class DBTSettingsStub:
            def __init__(self, project_dir, profiles_dir, target, threads):
                self.project_dir = project_dir
                self.profiles_dir = profiles_dir
                self.target = target
                self.threads = threads
                self.enable_freshness_checks = True
                self.default_materialization = "view"
                self.command_timeout = 300
        orch.settings.dbt = DBTSettingsStub(
            project_dir=settings.dbt.project_dir,
            profiles_dir=settings.dbt.profiles_dir,
            target=settings.dbt.target,
            threads=settings.dbt.threads,
        )
        return orch

    # Metadata discovery
    @pytest.mark.asyncio
    async def test_discover_source_tables(self, orchestrator: PipelineOrchestrator):
        orchestrator.teradata_client.search_metadata.return_value = [{"table": "customers"}]
        orchestrator.teradata_client.get_table_metadata.return_value = {
            "table_name": "customers",
            "row_count": 1234,
        }
        orchestrator.teradata_client.estimate_table_size.return_value = {"size_mb": 50}

        tables = await orchestrator.discover_source_tables(database="source_db", table_pattern="cust%")
        assert isinstance(tables, list)
        assert len(tables) == 1
        assert tables[0]["table_name"] == "customers"
        orchestrator.teradata_client.search_metadata.assert_called()

    # Profiling
    def test_profile_source_table(self, orchestrator: PipelineOrchestrator):
        orchestrator.teradata_client.profile_table.return_value = {"columns": 10, "stats": {"null_pct": 0.1}}
        result = orchestrator.profile_source_table(database="db", table_name="t", sample_size=1000)
        assert result["columns"] == 10
        orchestrator.teradata_client.profile_table.assert_called_once()

    # dbt generation
    def test_generate_dbt_sources(self, orchestrator: PipelineOrchestrator):
        orchestrator.dbt_generator.generate_source_from_teradata_metadata.return_value = "yaml"
        res = orchestrator.generate_dbt_sources(
            source_name="src",
            table_metadata_list=[{"table_name": "t1"}],
            output_path="models/staging/src/sources.yml",
        )
        assert res == "yaml"
        orchestrator.dbt_generator.generate_source_from_teradata_metadata.assert_called()

    def test_generate_dbt_staging_models(self, orchestrator: PipelineOrchestrator):
        orchestrator.dbt_generator.generate_staging_layer.return_value = {
            "models_generated": ["stg_t1.sql"],
            "tests_generated": ["stg_t1_tests.yml"],
        }
        res = orchestrator.generate_dbt_staging_models(
            source_name="src",
            table_metadata_list=[{"table_name": "t1"}],
            models_dir="models/staging/src",
        )
        assert "models_generated" in res and "tests_generated" in res
        orchestrator.dbt_generator.generate_staging_layer.assert_called()

    # dbt execution
    def test_execute_dbt_models(self, orchestrator: PipelineOrchestrator):
        orchestrator.dbt_client.run.return_value = {"success": True, "results": []}
        res = orchestrator.execute_dbt_models(models=["m1"], full_refresh=False)
        assert res["success"] is True
        orchestrator.dbt_client.run.assert_called_once()

    # ── Per-Teradata-profile dbt sub-project factories ──────────────

    def test_dbt_project_parent_returns_settings_project_dir(
        self, orchestrator: PipelineOrchestrator, settings: Settings
    ) -> None:
        """``dbt_project_parent`` is the container for per-Teradata-profile
        sub-projects: the settings.dbt.project_dir (typically
        ``<workspace>/dbt_project/``)."""
        assert orchestrator.dbt_project_parent == Path(settings.dbt.project_dir)

    def test_dbt_generator_for_returns_fresh_instance_at_subproject(
        self, orchestrator: PipelineOrchestrator, tmp_path: Path
    ) -> None:
        """The factory builds a fresh DBTGenerator pinned at the supplied
        sub-project path — independent of the cached ``dbt_generator``."""
        from elt_mcp_server.generators.dbt_generator import DBTGenerator

        sub = tmp_path / "dbt_analytics"
        gen = orchestrator.dbt_generator_for(sub)
        assert isinstance(gen, DBTGenerator)
        assert gen.project_dir == sub
        # Cached generator (which is a Mock here) is unchanged.
        assert gen is not orchestrator._dbt_generator

    @staticmethod
    def _make_minimal_dbt_subproject(path: Path) -> Path:
        """Create a minimal valid dbt sub-project on disk so DBTClient's
        constructor checks pass."""
        path.mkdir(parents=True, exist_ok=True)
        (path / "dbt_project.yml").write_text(
            "name: 'test'\nprofile: 'test'\n", encoding="utf-8"
        )
        return path

    def test_dbt_client_for_returns_fresh_instance_at_subproject(
        self, orchestrator: PipelineOrchestrator, tmp_path: Path
    ) -> None:
        """The factory builds a fresh DBTClient pinned at the supplied
        sub-project; profiles_dir defaults to project_dir so per-sub-project
        profiles.yml is found by dbt CLI."""
        from elt_mcp_server.clients.dbt_client import DBTClient

        sub = self._make_minimal_dbt_subproject(tmp_path / "dbt_analytics")
        client = orchestrator.dbt_client_for(sub)
        assert isinstance(client, DBTClient)
        assert client.project_dir == sub
        assert client.profiles_dir == sub

    def test_dbt_client_for_honors_explicit_profiles_dir(
        self, orchestrator: PipelineOrchestrator, tmp_path: Path
    ) -> None:
        """An explicit ``profiles_dir`` overrides the default-to-project_dir
        behaviour — used when callers want a shared parent-level
        profiles.yml across sub-projects."""
        sub = self._make_minimal_dbt_subproject(tmp_path / "dbt_analytics")
        shared_profiles = tmp_path  # parent of all sub-projects
        client = orchestrator.dbt_client_for(sub, profiles_dir=shared_profiles)
        assert client.profiles_dir == shared_profiles

    def test_dbt_generator_setter_pins_to_per_call_instance(
        self, orchestrator: PipelineOrchestrator, tmp_path: Path
    ) -> None:
        """Setting ``orchestrator.dbt_generator`` swaps the cached instance
        — used by tools that resolve a sub-project at call time."""
        from elt_mcp_server.generators.dbt_generator import DBTGenerator

        sub = tmp_path / "dbt_analytics"
        replacement = DBTGenerator(project_dir=sub)
        orchestrator.dbt_generator = replacement
        assert orchestrator.dbt_generator is replacement

    def test_dbt_generator_setter_none_reverts_to_lazy_factory(
        self, orchestrator: PipelineOrchestrator
    ) -> None:
        """Setting ``None`` discards the cache so the next access
        re-creates from settings."""
        orchestrator.dbt_generator = None
        assert orchestrator._dbt_generator is None

    def test_dbt_client_setter_pins_to_per_call_instance(
        self, orchestrator: PipelineOrchestrator, tmp_path: Path
    ) -> None:
        from elt_mcp_server.clients.dbt_client import DBTClient

        sub = self._make_minimal_dbt_subproject(tmp_path / "dbt_analytics")
        replacement = DBTClient(
            project_dir=sub,
            profiles_dir=sub,
            target="dev",
            threads=2,
            command_timeout=60,
        )
        orchestrator.dbt_client = replacement
        assert orchestrator.dbt_client is replacement

    # Airflow DAG trigger - now uses async method
    @pytest.mark.asyncio
    async def test_trigger_airflow_dag(self, orchestrator: PipelineOrchestrator):
        orchestrator._async_airflow_client = Mock()
        orchestrator._async_airflow_client.trigger_dag = AsyncMock(return_value={"dag_run_id": "run_1"})
        orchestrator._async_airflow_client.wait_for_dag_run = AsyncMock(return_value="success")
        res = await orchestrator.async_trigger_airflow_dag(dag_id="dag1", conf={"k": "v"}, wait_for_completion=True, timeout_seconds=5)
        assert res["dag_run_id"] == "run_1"
        assert res["final_status"] == "success"

    # Validation (conditional checks) - now uses async method
    @pytest.mark.asyncio
    async def test_validate_pipeline_configuration_tpt_file(self, orchestrator: PipelineOrchestrator, tmp_path: Path):
        # Create a real temporary file for validation
        input_file = tmp_path / "input.csv"
        input_file.write_text("col1,col2\na,b\n")

        orchestrator.teradata_client.test_connection.return_value = {"connected": True}
        orchestrator._async_airflow_client = Mock()
        orchestrator._async_airflow_client.get_health = AsyncMock(return_value={"metadatabase": {"status": "healthy"}})
        with patch("shutil.which", return_value="C:/path/tbuild"):
            res = await orchestrator.async_validate_pipeline_configuration({
                "source_type": "tpt_file",
                "input_file_path": str(input_file),
            })
        assert res["valid"] is True
        assert res["checks"]["tpt"] == "OK"

    @pytest.mark.asyncio
    async def test_validate_pipeline_configuration_airbyte(self, orchestrator: PipelineOrchestrator):
        orchestrator.teradata_client.test_connection.return_value = {"connected": True}
        orchestrator._async_airflow_client = Mock()
        orchestrator._async_airflow_client.get_health = AsyncMock(return_value={"metadatabase": {"status": "healthy"}})
        # Airbyte check expects status="successful operation" for OK status
        orchestrator.airbyte_client.get_health = AsyncMock(return_value={"connected": True, "status": "successful operation"})
        res = await orchestrator.async_validate_pipeline_configuration({
            "source_type": "airbyte",
            "source_connector": "postgres",
        })
        assert res["valid"] is True
        assert res["checks"]["airbyte"] == "OK"

    @pytest.mark.asyncio
    async def test_validate_pipeline_configuration_teradata_tables(self, orchestrator: PipelineOrchestrator):
        orchestrator.teradata_client.test_connection.return_value = {"connected": True}
        orchestrator._async_airflow_client = Mock()
        orchestrator._async_airflow_client.get_health = AsyncMock(return_value={"metadatabase": {"status": "healthy"}})
        res = await orchestrator.async_validate_pipeline_configuration({
            "source_type": "teradata_tables",
        })
        assert res["valid"] is True
        assert res["checks"]["airbyte"].startswith("SKIPPED")
        assert res["checks"]["tpt"].startswith("SKIPPED")

    # Pipeline status - now uses async method
    @pytest.mark.asyncio
    async def test_get_pipeline_status(self, orchestrator: PipelineOrchestrator):
        orchestrator._async_airflow_client = Mock()
        orchestrator._async_airflow_client.get_dag = AsyncMock(return_value={"is_paused": False})
        orchestrator._async_airflow_client.get_dag_runs = AsyncMock(return_value=[{"dag_run_id": "run_1", "state": "success"}])
        orchestrator._async_airflow_client.get_dag_run_history = AsyncMock(return_value={"statistics": {"success": 10}})
        status = await orchestrator.get_pipeline_status_async("dag1")
        assert status["dag_id"] == "dag1"
        assert status["last_run"]["dag_run_id"] == "run_1"

    # Cleanup
    @pytest.mark.asyncio
    async def test_cleanup(self, orchestrator: PipelineOrchestrator):
        orchestrator._airbyte_client.close = AsyncMock()
        await orchestrator.cleanup()
        orchestrator._teradata_client.close.assert_called_once()
        orchestrator._airbyte_client.close.assert_called_once()


class TestAsyncGetAirflowHealth:
    """Tests for async_get_airflow_health method."""

    @pytest.fixture
    def orchestrator(self) -> PipelineOrchestrator:
        """Create orchestrator with mocked async_airflow_client."""
        settings = Settings(
            teradata=TeradataSettings(
                host="localhost", username="dbc", password=SecretStr("dbc"), database="test_db", port=1025
            ),
        )
        orch = PipelineOrchestrator(settings)
        orch._async_airflow_client = MagicMock()
        return orch

    @pytest.mark.asyncio
    async def test_async_get_airflow_health_connected(self, orchestrator: PipelineOrchestrator):
        """Test health check when Airflow is connected with all recommended providers."""
        from elt_mcp_server.clients.async_airflow_client import RECOMMENDED_AIRFLOW_PROVIDERS

        orchestrator._async_airflow_client.test_connection = AsyncMock(
            return_value={"connected": True, "url": "http://localhost:8080", "version": "2.5.0"}
        )
        orchestrator._async_airflow_client.get_circuit_breaker_status = MagicMock(return_value=None)
        providers = [{"package_name": name} for name in RECOMMENDED_AIRFLOW_PROVIDERS.keys()]
        orchestrator._async_airflow_client.get_providers = AsyncMock(
            return_value={"providers": providers, "total_entries": len(providers)}
        )

        result = await orchestrator.async_get_airflow_health()

        assert result["connected"] is True
        assert result["availability"] == "healthy"
        assert result["providers"]["missing"] == []

    @pytest.mark.asyncio
    async def test_async_get_airflow_health_incomplete_providers(self, orchestrator: PipelineOrchestrator):
        """Test health check when provider discovery is incomplete (truncated)."""
        orchestrator._async_airflow_client.test_connection = AsyncMock(return_value={"connected": True})
        orchestrator._async_airflow_client.get_circuit_breaker_status = MagicMock(return_value=None)
        orchestrator._async_airflow_client.get_providers = AsyncMock(
            return_value={"providers": [], "total_entries": 100, "incomplete": True}
        )

        result = await orchestrator.async_get_airflow_health()

        assert result["connected"] is True
        assert result["providers"]["missing"] == []
        assert result["providers"]["incomplete"] is True
        assert "install_hint" not in result["providers"] or result["providers"]["install_hint"] is None

    @pytest.mark.asyncio
    async def test_async_get_airflow_health_disconnected(self, orchestrator: PipelineOrchestrator):
        """Test health check when Airflow is disconnected."""
        orchestrator._async_airflow_client.test_connection = AsyncMock(return_value={"connected": False})
        orchestrator._async_airflow_client.get_circuit_breaker_status = MagicMock(return_value=None)

        result = await orchestrator.async_get_airflow_health()

        assert result["connected"] is False
        assert result["availability"] == "degraded"
        assert "providers" not in result

    @pytest.mark.asyncio
    async def test_async_get_airflow_health_circuit_breaker_open(self, orchestrator: PipelineOrchestrator):
        """Test health check when circuit breaker is open."""
        orchestrator._async_airflow_client.test_connection = AsyncMock(return_value={"connected": True})
        orchestrator._async_airflow_client.get_circuit_breaker_status = MagicMock(
            return_value={"state": "open", "failure_count": 5}
        )
        orchestrator._async_airflow_client.get_providers = AsyncMock(return_value={"providers": []})

        result = await orchestrator.async_get_airflow_health()

        assert result["availability"] == "degraded"
        assert result["circuit_breaker"]["state"] == "open"


class TestOrchestratorWorkflowProperty:
    """Tests for workflow_orchestrator property."""

    @pytest.fixture
    def settings(self, tmp_path: Path) -> Settings:
        """Settings with orchestrator configuration."""
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

    def test_workflow_orchestrator_lazy_creation(self, settings: Settings):
        """Test workflow_orchestrator property creates orchestrator lazily."""
        from elt_mcp_server.workflow import WorkflowOrchestratorProtocol
        from tests.unit.mock_client_factory import MockClientFactory

        mock_orchestrator = Mock(spec=WorkflowOrchestratorProtocol)
        mock_orchestrator.backend_name = "airflow"

        factory = MockClientFactory()
        factory.set_async_airflow_client(Mock())
        factory.set_airbyte_client(Mock())
        factory.set_teradata_client(Mock())
        factory.set_dbt_client(Mock())
        factory.set_workflow_orchestrator(mock_orchestrator)

        orch = PipelineOrchestrator(settings, client_factory=factory)

        # Access property
        result = orch.workflow_orchestrator

        assert result is mock_orchestrator
        assert result.backend_name == "airflow"

    def test_workflow_orchestrator_cached(self, settings: Settings):
        """Test workflow_orchestrator is cached after first access."""
        from elt_mcp_server.workflow import WorkflowOrchestratorProtocol
        from tests.unit.mock_client_factory import MockClientFactory

        mock_orchestrator = Mock(spec=WorkflowOrchestratorProtocol)

        factory = MockClientFactory()
        factory.set_async_airflow_client(Mock())
        factory.set_airbyte_client(Mock())
        factory.set_teradata_client(Mock())
        factory.set_dbt_client(Mock())
        factory.set_workflow_orchestrator(mock_orchestrator)

        orch = PipelineOrchestrator(settings, client_factory=factory)

        # Access twice
        result1 = orch.workflow_orchestrator
        result2 = orch.workflow_orchestrator

        assert result1 is result2

    def test_workflow_orchestrator_initialized_none(self, settings: Settings):
        """Test _workflow_orchestrator is None at initialization."""
        from tests.unit.mock_client_factory import MockClientFactory

        factory = MockClientFactory()
        factory.set_async_airflow_client(Mock())
        factory.set_airbyte_client(Mock())
        factory.set_teradata_client(Mock())
        factory.set_dbt_client(Mock())

        orch = PipelineOrchestrator(settings, client_factory=factory)

        # Should be None before accessing property
        assert orch._workflow_orchestrator is None


class TestOrchestratorWithDifferentBackends:
    """Tests for PipelineOrchestrator with different orchestrator backends."""

    @pytest.fixture
    def base_settings(self, tmp_path: Path):
        """Base settings without orchestrator configuration."""
        return {
            "teradata": TeradataSettings(
                host="localhost",
                username="dbc",
                password=SecretStr("dbc"),
                database="test_db",
            ),
            "airflow": AirflowSettings(
                base_url="http://localhost:8080",
                username="admin",
                password=SecretStr("admin"),
            ),
            "airbyte": AirbyteSettings(
                enabled=True,
                base_url="http://localhost:8000",
                client_id="airbyte",
                client_secret=SecretStr("password"),
            ),
            "dbt": DBTSettings(
                project_dir=tmp_path / "dbt_project",
                profiles_dir=tmp_path / "profiles",
            ),
            "pipeline": PipelineSettings(dags_output_dir=tmp_path / "dags"),
        }

    def test_orchestrator_with_airflow_backend(self, base_settings):
        """Test orchestrator initialization with Airflow backend."""
        settings = Settings(
            **base_settings,
            orchestrator=OrchestratorSettings(backend="airflow"),
        )
        orch = PipelineOrchestrator(settings)
        assert orch.settings.orchestrator.backend == "airflow"
