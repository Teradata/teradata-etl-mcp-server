"""Main pipeline orchestration logic.

This module coordinates all components (Teradata, Airflow, Airbyte, dbt)
to discover metadata, generate code, and execute ELT pipelines.
"""

import asyncio
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from .auth import TeradataAuth
from .clients.airbyte_client import AirbyteClient
from .clients.dbt_client import DBTClient
from .clients.teradata_client import TeradataClient
from .clients.ttu_client import TTUClient
from .config import Settings
from .generators.airflow_dag_generator import AirflowDAGGenerator
from .generators.dbt_generator import DBTGenerator
from .storage.metadata_store import (
    MetadataEntry,
    MetadataStore,
    SQLiteMetadataStore,
)

if TYPE_CHECKING:
    from .client_factory import ClientFactoryProtocol
    from .clients.async_airflow_client import AsyncAirflowClient
    from .credential_resolver import CredentialResolver
    from .workflow.protocol import WorkflowOrchestratorProtocol

logger = logging.getLogger(__name__)


class PipelineOrchestratorError(Exception):
    """Base exception for pipeline orchestrator errors."""

    pass


class PipelineOrchestrator:
    """
    Main pipeline orchestration coordinator.

    Coordinates metadata extraction, pipeline creation, code generation,
    and execution across Teradata, Airflow, Airbyte, and dbt.

    Supports dependency injection via client_factory for easier testing.
    """

    def __init__(
        self,
        settings: Settings,
        client_factory: "ClientFactoryProtocol | None" = None,
    ):
        """
        Initialize pipeline orchestrator.

        Args:
            settings: Application settings
            client_factory: Optional client factory for dependency injection.
                           If not provided, clients are created directly from settings.
        """
        from .client_factory import ClientFactoryProtocol, DefaultClientFactory, LazyClientFactory

        self.settings = settings

        # Use provided factory or create default with lazy loading
        if client_factory is not None:
            self._client_factory: ClientFactoryProtocol = client_factory
        else:
            self._client_factory = LazyClientFactory(DefaultClientFactory(settings))

        # Initialize clients (lazy loading via factory)
        self._teradata_client: TeradataClient | None = None
        self._airbyte_client: AirbyteClient | None = None
        self._dbt_client: DBTClient | None = None
        self._ttu_client: TTUClient | None = None

        # Initialize generators (lazy loading)
        self._dbt_generator: DBTGenerator | None = None
        self._airflow_dag_generator: AirflowDAGGenerator | None = None

        # Credential resolver (lazy loading)
        self._credential_resolver: CredentialResolver | None = None
        # Metadata store for caching registry and metadata. Path comes from
        # settings.mcp.metadata_db_path which is workspace-resolved by
        # Settings.validate_settings (defaults to <workspace>/.etl-mcp/metadata.db).
        self._metadata_store: MetadataStore | None = SQLiteMetadataStore(
            db_path=str(self.settings.mcp.metadata_db_path)
        )

        # Async client (lazy loading)
        self._async_airflow_client: AsyncAirflowClient | None = None

        # Workflow orchestrator (lazy loading - protocol-based)
        self._workflow_orchestrator: WorkflowOrchestratorProtocol | None = None

        logger.info(
            "Initialized pipeline orchestrator (factory=%s)", type(self._client_factory).__name__
        )

    # ==================== Client Properties (Lazy Loading via Factory) ====================

    @property
    def teradata_client(self) -> TeradataClient:
        """Get or create Teradata client via factory."""
        if self._teradata_client is None:
            self._teradata_client = self._client_factory.create_teradata_client()
        return self._teradata_client

    @property
    def async_airflow_client(self) -> "AsyncAirflowClient":
        """Get or create async Airflow client via factory."""
        if self._async_airflow_client is None:
            self._async_airflow_client = self._client_factory.create_async_airflow_client()
        return self._async_airflow_client

    @property
    def airbyte_client(self) -> AirbyteClient:
        """Get or create Airbyte client via factory."""
        if self._airbyte_client is None:
            self._airbyte_client = self._client_factory.create_airbyte_client(self._metadata_store)
        return self._airbyte_client

    @property
    def dbt_client(self) -> DBTClient:
        """Get or create dbt client via factory."""
        if self._dbt_client is None:
            self._dbt_client = self._client_factory.create_dbt_client()
        return self._dbt_client

    @dbt_client.setter
    def dbt_client(self, value: DBTClient | None) -> None:
        """Pin the cached dbt client (typically to a per-Teradata-profile
        sub-project resolved at tool-call time). Setting ``None`` reverts
        to lazy factory construction on next access."""
        self._dbt_client = value

    @property
    def ttu_client(self) -> TTUClient:
        """Get or create TTU client via factory."""
        if self._ttu_client is None:
            self._ttu_client = self._client_factory.create_ttu_client()
        return self._ttu_client

    @property
    def workflow_orchestrator(self) -> "WorkflowOrchestratorProtocol":
        """Get or create workflow orchestrator via factory.

        The workflow orchestrator provides a unified interface for managing
        workflows across different backends (Airflow, Dagster, Prefect).
        """
        if self._workflow_orchestrator is None:
            self._workflow_orchestrator = self._client_factory.create_workflow_orchestrator()
        return self._workflow_orchestrator

    # ==================== Credential Resolver (Lazy Loading) ====================

    @property
    def credential_resolver(self) -> "CredentialResolver":
        """Get or create the credential resolver."""
        if self._credential_resolver is None:
            import contextlib

            from .credential_resolver import CredentialResolver

            connections_file = None
            with contextlib.suppress(AttributeError):
                connections_file = self.settings.security.connections_file
            self._credential_resolver = CredentialResolver(
                settings=self.settings,
                connections_file=connections_file,
            )
        return self._credential_resolver

    # ==================== Generator Properties (Lazy Loading) ====================

    @property
    def dbt_generator(self) -> DBTGenerator:
        """Get or create dbt generator."""
        if self._dbt_generator is None:
            project_dir = Path(self.settings.dbt.project_dir)
            self._dbt_generator = DBTGenerator(project_dir=project_dir)
        return self._dbt_generator

    @dbt_generator.setter
    def dbt_generator(self, value: DBTGenerator | None) -> None:
        """Pin the cached dbt generator (typically to a per-Teradata-profile
        sub-project resolved at tool-call time). Setting ``None`` reverts
        to lazy construction on next access."""
        self._dbt_generator = value

    # ── Per-Teradata-profile dbt sub-project factories ──────────────
    #
    # The cached ``dbt_client`` / ``dbt_generator`` properties above are
    # pinned to a single ``settings.dbt.project_dir`` and are only correct
    # for the legacy single-project layout. The per-Teradata-profile
    # layout puts each identity's sub-project under
    # ``<settings.dbt.project_dir>/dbt_<name>/`` — see
    # ``dbt_project_parent``. Tools that resolve a sub-project at call
    # time should build a fresh client/generator via the ``*_for``
    # factories below and discard it after the call.

    @property
    def dbt_project_parent(self) -> Path:
        """Parent directory containing per-Teradata-profile dbt sub-projects.

        Each Teradata identity (a named ``connections.yaml`` profile or
        the wizard-default keyed by host) gets its own sub-project under
        this directory: ``dbt_<slug(project_name)>/``. The parent itself
        is NOT a dbt project — running ``dbt run`` directly against it
        will fail.
        """
        return Path(self.settings.dbt.project_dir)

    def dbt_client_for(
        self,
        project_dir: Path,
        profiles_dir: Path | None = None,
    ) -> DBTClient:
        """Build a fresh :class:`DBTClient` pinned to a specific sub-project.

        Used by tools that resolve to a per-Teradata-profile sub-project
        at call time. ``profiles_dir`` defaults to ``project_dir`` so dbt
        finds the per-sub-project ``profiles.yml``.
        """
        return DBTClient(
            project_dir=project_dir,
            profiles_dir=profiles_dir if profiles_dir is not None else project_dir,
            target=self.settings.dbt.target,
            threads=self.settings.dbt.threads,
            command_timeout=self.settings.dbt.command_timeout,
        )

    def dbt_generator_for(self, project_dir: Path) -> DBTGenerator:
        """Build a fresh :class:`DBTGenerator` pinned to a specific sub-project.

        See :meth:`dbt_client_for` for the rationale.
        """
        return DBTGenerator(project_dir=project_dir)

    def teradata_client_for(self, auth: TeradataAuth) -> TeradataClient:
        """Build a fresh :class:`TeradataClient` pinned to specific auth credentials.

        Used by tools that need to connect to a Teradata host other than
        the wizard-default. Creates a new client with the provided auth
        instead of using the lazy-loaded default.
        """
        return TeradataClient(auth=auth)

    @property
    def airflow_dag_generator(self) -> AirflowDAGGenerator:
        """Get or create Airflow DAG generator."""
        if self._airflow_dag_generator is None:
            # Use pipeline.dags_output_dir for local DAG generation path
            dags_folder = Path(self.settings.pipeline.dags_output_dir)
            self._airflow_dag_generator = AirflowDAGGenerator(dags_folder=dags_folder)
        return self._airflow_dag_generator

    @property
    def metadata_store(self) -> MetadataStore:
        """Get metadata store instance."""
        if self._metadata_store is None:
            self._metadata_store = SQLiteMetadataStore(
                db_path=str(self.settings.mcp.metadata_db_path)
            )
        return self._metadata_store

    def preload_airbyte_registry(self) -> bool:
        """
        Fetch and cache the Airbyte OSS connector registry at server startup.
        Stored in metadata store under key 'airbyte_oss_registry'.
        """
        try:
            key = "airbyte_oss_registry"
            existing = self.metadata_store.get_metadata(key)
            if existing and existing.value:
                logger.info("Airbyte registry already present in cache. Skipping preload.")
                return True
            url = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"
            logger.info("Preloading Airbyte OSS connector registry...")
            resp = httpx.get(url, timeout=30.0)
            resp.raise_for_status()
            data = resp.json()
            entry = MetadataEntry(
                key=key,
                value=data,
                timestamp=datetime.now(timezone.utc),
                ttl_seconds=None,
                tags=["airbyte", "registry"],
            )
            self.metadata_store.store_metadata(entry)
            logger.info("Airbyte registry cached successfully.")
            return True
        except Exception as e:
            logger.error("Failed to preload Airbyte registry: %s", e, exc_info=True)
            return False

    # ==================== Metadata & Discovery ====================

    async def discover_source_tables(
        self,
        database: str,
        schema_pattern: str = "%",  # noqa: ARG002
        table_pattern: str = "%",
    ) -> list[dict[str, Any]]:
        """
        Discover source tables from Teradata.

        Args:
            database: Database name
            schema_pattern: Schema pattern (SQL LIKE)
            table_pattern: Table pattern (SQL LIKE)

        Returns:
            List of table metadata dictionaries
        """
        try:
            # Search for matching tables
            tables = self.teradata_client.search_metadata(
                search_term=table_pattern,
                search_type="table",
                database_name=database,
            )

            # Get detailed metadata for each table
            table_metadata_list = []
            for table_info in tables:
                table_name = table_info["table"]  # Changed from "table_name" to "table"

                logger.info("Getting metadata for %s.%s", database, table_name)

                # Get metadata using asyncio.to_thread
                metadata = await asyncio.to_thread(
                    self.teradata_client.get_table_metadata, database, table_name, False
                )

                # Get size estimate using asyncio.to_thread
                size_info = await asyncio.to_thread(
                    self.teradata_client.estimate_table_size, database, table_name
                )
                metadata.update(size_info)

                table_metadata_list.append(metadata)

            logger.info("Discovered %d tables", len(table_metadata_list))

            return table_metadata_list

        except Exception as e:
            logger.error("Failed to discover source tables: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"Table discovery failed: {e}") from e

    def profile_source_table(
        self,
        database: str,
        table_name: str,
        sample_size: int = 10000,
    ) -> dict[str, Any]:
        """
        Profile a source table with statistics.

        Args:
            database: Database name
            table_name: Table name
            sample_size: Sample size for profiling

        Returns:
            Profiling results dictionary
        """
        try:
            logger.info("Profiling table %s.%s", database, table_name)

            profile_results = self.teradata_client.profile_table(
                database_name=database,
                table_name=table_name,
                sample_size=sample_size,
            )

            logger.info("Profiling complete for %s.%s", database, table_name)

            return profile_results

        except Exception as e:
            logger.error("Failed to profile table: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"Table profiling failed: {e}") from e

    # ==================== dbt Model Generation ====================

    def generate_dbt_sources(
        self,
        source_name: str,
        table_metadata_list: list[dict[str, Any]],
        output_path: str = "models/staging/sources.yml",
    ) -> str:
        """
        Generate dbt sources YAML from table metadata.

        Args:
            source_name: Source name for dbt
            table_metadata_list: List of table metadata
            output_path: Output path relative to dbt project

        Returns:
            Generated YAML content
        """
        try:
            logger.info("Generating dbt sources for %s", source_name)

            yaml_content = self.dbt_generator.generate_source_from_teradata_metadata(
                source_name=source_name,
                table_metadata_list=table_metadata_list,
                output_path=Path(output_path),
                add_freshness=self.settings.dbt.enable_freshness_checks,
                add_basic_tests=True,
            )

            logger.info("Generated dbt sources: %s", output_path)

            return yaml_content

        except Exception as e:
            logger.error("Failed to generate dbt sources: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"dbt source generation failed: {e}") from e

    def generate_dbt_staging_models(
        self,
        source_name: str,
        table_metadata_list: list[dict[str, Any]],
        models_dir: str = "models/staging",
    ) -> dict[str, Any]:
        """
        Generate complete dbt staging layer.

        Args:
            source_name: Source name
            table_metadata_list: List of table metadata
            models_dir: Directory for models

        Returns:
            Generation results dictionary
        """
        try:
            logger.info("Generating dbt staging models for %s", source_name)

            results = self.dbt_generator.generate_staging_layer(
                source_name=source_name,
                table_metadata_list=table_metadata_list,
                models_dir=models_dir,
                materialization=self.settings.dbt.default_materialization,
                generate_tests=True,
            )

            logger.info(
                "Generated %d models, %d test files",
                len(results["models_generated"]),
                len(results["tests_generated"]),
            )

            return results

        except Exception as e:
            logger.error("Failed to generate dbt staging models: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"dbt model generation failed: {e}") from e

    # ==================== Pipeline Execution ====================

    def execute_dbt_models(
        self,
        models: list[str] | None = None,
        full_refresh: bool = False,
    ) -> dict[str, Any]:
        """
        Execute dbt models.

        Args:
            models: Optional list of models to run
            full_refresh: Force full refresh

        Returns:
            Execution results
        """
        try:
            logger.info("Executing dbt models")

            result = self.dbt_client.run(
                models=models,
                full_refresh=full_refresh,
            )

            logger.info("dbt run completed: success=%s", result["success"])

            return result

        except Exception as e:
            logger.error("Failed to execute dbt models: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"dbt execution failed: {e}") from e

    # ==================== Async Airflow Operations ====================

    async def async_trigger_airflow_dag(
        self,
        dag_id: str,
        conf: dict[str, Any] | None = None,
        wait_for_completion: bool = False,
        timeout_seconds: int = 3600,
        dag_run_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Trigger Airflow DAG execution (async version).

        Uses AsyncAirflowClient for non-blocking operations.

        Args:
            dag_id: DAG identifier
            conf: Optional configuration parameters
            wait_for_completion: Whether to wait for DAG to complete
            timeout_seconds: Timeout for waiting

        Returns:
            DAG run information
        """
        try:
            logger.info("Async triggering Airflow DAG: %s", dag_id)

            dag_run = await self.async_airflow_client.trigger_dag(
                dag_id=dag_id,
                conf=conf,
                dag_run_id=dag_run_id,
            )

            run_id = dag_run.get("dag_run_id")

            if wait_for_completion and run_id:
                logger.info("Async waiting for DAG run completion: %s", run_id)

                final_status = await self.async_airflow_client.wait_for_dag_run(
                    dag_id=dag_id,
                    dag_run_id=run_id,
                    timeout_seconds=timeout_seconds,
                )

                dag_run["final_status"] = final_status

            logger.info("Async DAG trigger complete: %s", dag_id)

            return dag_run

        except Exception as e:
            logger.error("Failed to async trigger Airflow DAG: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"Async DAG trigger failed: {e}") from e

    async def async_trigger_airflow_dag_idempotent(
        self,
        dag_id: str,
        idempotency_key: str,
        conf: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Trigger Airflow DAG with idempotency guarantee (async version).

        Args:
            dag_id: DAG identifier
            idempotency_key: Unique key for this request
            conf: Optional configuration parameters

        Returns:
            DAG run information with idempotent_reused flag
        """
        try:
            logger.info("Async triggering idempotent DAG: %s (key=%s)", dag_id, idempotency_key)

            result = await self.async_airflow_client.trigger_dag_idempotent(
                dag_id=dag_id,
                idempotency_key=idempotency_key,
                conf=conf,
            )

            logger.info(
                "Async idempotent DAG trigger complete: %s (reused=%s)",
                dag_id,
                result.get("idempotent_reused"),
            )

            return result

        except Exception as e:
            logger.error("Failed to async trigger idempotent DAG: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"Async idempotent DAG trigger failed: {e}") from e

    async def async_get_dag_run_status(
        self,
        dag_id: str,
        dag_run_id: str,
    ) -> dict[str, Any]:
        """
        Get DAG run status (async version).

        Args:
            dag_id: DAG identifier
            dag_run_id: DAG run identifier

        Returns:
            DAG run status with task summary
        """
        try:
            return await self.async_airflow_client.get_dag_run_status(dag_id, dag_run_id)
        except Exception as e:
            logger.error("Failed to get async DAG run status: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"Get DAG status failed: {e}") from e

    async def async_list_dags(
        self,
        limit: int = 100,
        only_active: bool = True,
        tags: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        List DAGs from Airflow (async version).

        Args:
            limit: Maximum number of DAGs to return
            only_active: Only return active DAGs

        Returns:
            List of DAG information
        """
        try:
            return await self.async_airflow_client.list_dags(limit=limit, only_active=only_active, tags=tags)
        except Exception as e:
            logger.error("Failed to list DAGs: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"List DAGs failed: {e}") from e

    async def async_trigger_multiple_dags(
        self,
        dag_configs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Trigger multiple DAGs concurrently (async version).

        Args:
            dag_configs: List of dicts with 'dag_id' and optional 'conf'

        Returns:
            List of DAG run results
        """
        try:
            logger.info("Triggering %d DAGs concurrently", len(dag_configs))
            results = await self.async_airflow_client.trigger_multiple_dags(dag_configs)
            logger.info("Concurrent DAG triggers complete")
            return results
        except Exception as e:
            logger.error("Failed to trigger multiple DAGs: %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"Multiple DAG trigger failed: {e}") from e

    async def async_get_airflow_health(self) -> dict[str, Any]:
        """
        Get Airflow health status (async version).

        Returns:
            Health status with circuit breaker and provider validation info
        """
        try:
            conn_status = await self.async_airflow_client.test_connection()
            cb_status = self.async_airflow_client.get_circuit_breaker_status()

            result = {
                "connected": conn_status.get("connected", False),
                "url": conn_status.get("url"),
                "version": conn_status.get("version"),
            }

            if cb_status:
                result["circuit_breaker"] = cb_status
                state = cb_status.get("state", "unknown")
                if state == "open":
                    result["availability"] = "degraded"
                elif state == "half_open":
                    result["availability"] = "recovering"
                else:
                    result["availability"] = "healthy"
            else:
                result["availability"] = "healthy" if conn_status.get("connected") else "degraded"

            if conn_status.get("connected"):
                try:
                    from .clients.async_airflow_client import (
                        RECOMMENDED_AIRFLOW_PROVIDERS,
                        check_missing_providers,
                    )

                    providers_resp = await asyncio.wait_for(self.async_airflow_client.get_providers(), timeout=15.0)
                    is_incomplete = providers_resp.get("incomplete", False)
                    missing = check_missing_providers(providers_resp) if not is_incomplete else []
                    result["providers"] = {
                        "recommended": list(RECOMMENDED_AIRFLOW_PROVIDERS.keys()),
                        "missing": [n for n, _ in missing],
                        "install_hint": (
                            "pip install " + " ".join(n for n, _ in missing)
                            if missing
                            else None
                        ),
                    }
                    if is_incomplete:
                        result["providers"]["incomplete"] = True
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass

            return result

        except Exception as e:
            logger.error("Failed to get Airflow health: %s", e, exc_info=True)
            return {
                "connected": False,
                "availability": "unknown",
                "error": str(e),
            }

    # ==================== Validation & Testing ====================

    async def async_validate_pipeline_configuration(
        self, pipeline_config: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        Async validation of pipeline configuration and connections with concurrent checks.

        Mirrors `validate_pipeline_configuration` but performs network-bound operations
        concurrently and avoids blocking the event loop.

        Args:
            pipeline_config: Optional configuration dict

        Returns:
            Validation results
        """
        logger.info("Starting async pipeline configuration validation")
        cfg = pipeline_config or {}
        source_type = (cfg.get("source_type") or "").lower()
        source_connector = (cfg.get("source_connector") or "").lower()
        input_file = cfg.get("input_file_path")
        require_dbt = cfg.get("require_dbt", False)

        if not source_type:
            if input_file:
                source_type = "file"
            elif source_connector:
                source_type = "airbyte"
            else:
                source_type = "generic"

        csv_like_sources = {"file", "csv", "csv_file", "tpt_file"}
        teradata_sources = {"teradata", "teradata_tables"}

        results = {
            "valid": True,
            "checks": {},
            "errors": [],
            "warnings": [],
        }

        async def _check_teradata():
            try:
                info = await asyncio.to_thread(self.teradata_client.test_connection)
                if info.get("connected"):
                    results["checks"]["teradata"] = "OK"
                else:
                    results["checks"]["teradata"] = f"FAILED: {info.get('error', 'unknown')}"
                    results["errors"].append(f"Teradata: {info.get('error', 'unknown')}")
                    results["valid"] = False
            except Exception as e:
                results["checks"]["teradata"] = f"FAILED: {e}"
                results["errors"].append(f"Teradata: {e}")
                results["valid"] = False

        async def _check_airflow():
            try:
                # Try health endpoint via async client
                health = await self.async_airflow_client.get_health()
                if (
                    isinstance(health, dict)
                    and health.get("metadatabase", {}).get("status") == "healthy"
                ):
                    results["checks"]["airflow"] = "OK"
                else:
                    results["checks"]["airflow"] = "DEGRADED"
                    results["warnings"].append(
                        "Airflow: Health check shows degraded or unknown status"
                    )
            except Exception:
                # Fallbacks
                try:
                    _ = await self.async_airflow_client.get_version()
                    results["checks"]["airflow"] = "OK"
                    results["warnings"].append(
                        "Airflow: /health unavailable; validated via /version"
                    )
                except Exception:
                    try:
                        _ = await self.async_airflow_client.list_dags(limit=1)
                        results["checks"]["airflow"] = "OK"
                        results["warnings"].append(
                            "Airflow: /health unavailable; validated via DAG listing"
                        )
                    except Exception as e2:
                        results["checks"]["airflow"] = f"FAILED: {e2}"
                        results["errors"].append(f"Airflow: {e2}")
                        results["valid"] = False

        async def _check_airbyte():
            try:
                health = await self.airbyte_client.get_health()
                is_connected = bool(health.get("connected")) if isinstance(health, dict) else False
                status_val = (
                    str(health.get("status", "unknown")).lower()
                    if isinstance(health, dict)
                    else "unknown"
                )

                if is_connected and status_val.lower() in {"successful operation"}:
                    results["checks"]["airbyte"] = "OK"
                elif is_connected:
                    results["checks"]["airbyte"] = "DEGRADED"
                    results["warnings"].append("Airbyte: connected but status is unknown/degraded")
                else:
                    results["checks"]["airbyte"] = "UNAVAILABLE"
                    results["warnings"].append("Airbyte: Service not available")
            except Exception as e:
                results["checks"]["airbyte"] = f"FAILED: {e}"
                results["warnings"].append(f"Airbyte: {e}")

        # Start core checks concurrently
        tasks = [
            _check_teradata(),
            _check_airflow(),
        ]
        # Conditional Airbyte/TPT
        if source_type in csv_like_sources:
            # File path validation
            if not input_file:
                results["checks"]["input_file"] = "FAILED: input_file_path not provided"
                results["errors"].append("TPT: input file path missing; provide 'input_file_path'")
                results["valid"] = False
            else:
                p = Path(str(input_file))
                if not p.exists() or not p.is_file():
                    results["checks"]["input_file"] = (
                        f"FAILED: file not found or not a file: {input_file}"
                    )
                    results["errors"].append(f"TPT: input file not found: {input_file}")
                    results["valid"] = False
                else:
                    results["checks"]["input_file"] = "OK"
            tpt_candidates = ["tbuild", "tbuild.exe"]
            tpt_found = any(shutil.which(cmd) for cmd in tpt_candidates)
            results["checks"]["tpt"] = (
                "OK" if tpt_found else "FAILED: TPT command 'tbuild' not found in PATH"
            )
            if not tpt_found:
                results["errors"].append(
                    "TPT: 'tbuild' not found. Install TTU/TPT and ensure PATH is set."
                )
                results["valid"] = False
            results["checks"]["airbyte"] = "SKIPPED: not required for file source"
        elif source_type in teradata_sources:
            results["checks"]["airbyte"] = "SKIPPED: not required for Teradata tables"
            results["checks"]["tpt"] = "SKIPPED: not required for Teradata tables"
        elif source_type in {"airbyte"} or source_type == "generic":
            tasks.append(_check_airbyte())

        await asyncio.gather(*tasks)
        # dbt validation: if project is not yet created, validate environment instead of project
        if require_dbt:
            try:
                project_dir = Path(self.settings.dbt.project_dir)

                # 1) Environment validation (CLI installed) — uses static method,
                #    does NOT require a DBTClient instance (avoids catch-22 when dbt missing)
                install_info = await asyncio.to_thread(DBTClient.check_installation)
                install_ok = install_info.get("installed", False)
                if install_ok:
                    results["checks"]["dbt"] = "ENV_OK"
                    if not install_info.get("teradata_installed"):
                        results["warnings"].append(
                            "dbt is installed but dbt-teradata adapter is missing. "
                            "Install with: pip install dbt-teradata"
                        )
                else:
                    results["checks"]["dbt"] = "FAILED: dbt missing"
                    results["errors"].append(
                        "dbt is not installed. Install with: pip install dbt-teradata"
                    )
                    results["valid"] = False

                # 2) Project validation only if dbt installed AND directory exists
                if install_ok and project_dir.exists():
                    # validate_project spawns dbt debug + dbt compile, which
                    # connect to Teradata. Pass the default auth so the
                    # subprocess env carries proper credentials.
                    from .auth import build_teradata_auth_from_settings
                    default_auth = build_teradata_auth_from_settings(
                        self.settings.teradata
                    )
                    validation = await asyncio.to_thread(
                        self.dbt_client.validate_project, auth=default_auth
                    )
                    if validation.get("valid"):
                        results["checks"]["dbt_project"] = "OK"
                        results["checks"]["dbt"] = "OK"
                    else:
                        issues = validation.get("issues", [])
                        results["checks"]["dbt_project"] = f"INVALID: {issues}"
                        results["errors"].extend([f"dbt: {issue}" for issue in issues])
                        results["valid"] = False
                elif not project_dir.exists():
                    results["warnings"].append(
                        f"dbt: project directory not found at {project_dir}; skipped project validation"
                    )

            except Exception as e:
                results["checks"]["dbt"] = f"FAILED: {e}"
                results["errors"].append(f"dbt: {e}")
                results["valid"] = False
        else:
            results["checks"]["dbt"] = "SKIPPED: dbt not required"

        logger.info("Async validation complete: valid=%s", results["valid"])
        return results

    # ==================== Utility Methods ====================

    async def get_pipeline_status_async(self, dag_id: str) -> dict[str, Any]:
        """
        Async variant of pipeline status retrieval using non-blocking calls.

        Args:
            dag_id: DAG identifier

        Returns:
            Pipeline status information
        """
        try:
            logger.info("Getting pipeline status (async): %s", dag_id)
            dag_info = await self.async_airflow_client.get_dag(dag_id)
            runs = await self.async_airflow_client.get_dag_runs(dag_id, limit=5)
            history = await self.async_airflow_client.get_dag_run_history(dag_id)
            status = {
                "dag_id": dag_id,
                "is_paused": dag_info.get("is_paused"),
                "recent_runs": runs,
                "statistics": history.get("statistics", {}),
                "last_run": runs[0] if runs else None,
            }
            return status
        except Exception as e:
            logger.error("Failed to get pipeline status (async): %s", e, exc_info=True)
            raise PipelineOrchestratorError(f"Status retrieval failed: {e}") from e

    async def cleanup(self):
        """Clean up resources and close connections."""
        logger.info("Cleaning up orchestrator resources")

        if self._teradata_client:
            try:
                self._teradata_client.close()
            except Exception as e:
                logger.error("Failed to close Teradata client: %s", e, exc_info=True)

        if self._airbyte_client:
            try:
                await self._airbyte_client.close()
            except Exception as e:
                logger.error("Failed to close Airbyte client: %s", e, exc_info=True)

        if self._async_airflow_client:
            try:
                await self._async_airflow_client.close()
            except Exception as e:
                logger.error("Failed to close Airflow client: %s", e, exc_info=True)

        # Clear metadata store
        if self._metadata_store:
            try:
                self._metadata_store.cleanup_expired()
            except Exception as e:
                logger.error("Failed to cleanup metadata store: %s", e, exc_info=True)

        logger.info("Cleanup complete")
