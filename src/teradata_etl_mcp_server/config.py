"""Configuration management for Teradata ETL MCP Server.

This module handles all configuration and settings for the unified data pipeline
MCP server, including connections to Teradata, Airflow, Airbyte, and dbt.
"""

import logging
import os
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class TeradataSettings(BaseSettings):
    """Teradata database connection settings."""

    host: str = Field(..., description="Teradata database host or IP address")
    username: str = Field(default="", description="Teradata username")
    password: SecretStr = Field(default=SecretStr(""), description="Teradata password")
    database: str = Field(default="", description="Default Teradata database/schema")
    port: int = Field(default=1025, description="Teradata database port")
    logmech: str = Field(
        default="TD2",
        description="Authentication mechanism (TD2, LDAP, JWT, BEARER, SECRET)",
    )
    logdata: SecretStr = Field(
        default=SecretStr(""),
        description="Auth data: client secret for SECRET, token=<jwt> for JWT",
    )
    oidc_clientid: str = Field(
        default="",
        description="OIDC Client ID for SECRET and BEARER mechanisms",
    )
    jws_private_key: str = Field(
        default="",
        description="Path to JWS private key file for BEARER mechanism",
    )
    jws_cert: str = Field(
        default="",
        description="Path to JWS certificate file for BEARER mechanism",
    )
    sslca: str = Field(
        default="",
        description="Path to SSL CA certificate for BEARER/SECRET mechanisms",
    )

    # Connection pool settings
    pool_size: int = Field(default=5, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Maximum overflow connections")
    pool_timeout: int = Field(default=30, description="Pool connection timeout in seconds")

    # Query settings
    query_timeout: int = Field(default=300, description="Query timeout in seconds")
    charset: str = Field(default="UTF8", description="Character set for connections")

    model_config = SettingsConfigDict(env_prefix="TERADATA_")

    @model_validator(mode="after")
    def validate_auth_fields(self) -> "TeradataSettings":
        """Validate required fields based on authentication mechanism."""
        mech = self.logmech.upper()
        if mech in ("TD2", "LDAP"):
            if not self.username:
                raise ValueError(f"{mech} auth requires TERADATA_USERNAME")
            if not self.password.get_secret_value():
                raise ValueError(f"{mech} auth requires TERADATA_PASSWORD")
        elif mech == "JWT":
            if not self.logdata.get_secret_value():
                raise ValueError("JWT auth requires TERADATA_LOGDATA (token)")
        elif mech == "SECRET":
            if not self.oidc_clientid:
                raise ValueError("SECRET auth requires TERADATA_OIDC_CLIENTID")
            if not self.logdata.get_secret_value():
                raise ValueError("SECRET auth requires TERADATA_LOGDATA (client secret)")
        elif mech == "BEARER":
            if not self.oidc_clientid:
                raise ValueError("BEARER auth requires TERADATA_OIDC_CLIENTID")
        return self


class AirflowSettings(BaseSettings):
    """Apache Airflow connection settings."""

    enabled: bool = Field(default=False, description="Whether Airflow is enabled")
    base_url: str | None = Field(
        default=None, description="Airflow API base URL (e.g., http://localhost:8080)"
    )
    username: str | None = Field(default=None, description="Airflow username")
    password: SecretStr | None = Field(default=None, description="Airflow password")
    auth_manager: Literal["simple", "basic"] = Field(
        default="simple",
        description="Authentication manager for Airflow API ('simple' for token auth, 'basic' for basic auth)",
    )
    token_endpoint: str = Field(
        default="/auth/token",
        description="Endpoint to obtain API access token when using 'simple' auth manager",
    )
    access_token: SecretStr | None = Field(
        default=None,
        description="Pre-provided Bearer access token for Airflow API (skips token endpoint call)",
    )

    # API settings
    timeout: int = Field(default=30, description="API request timeout in seconds")

    # DAG settings
    dag_folder: str = Field(
        default="/opt/airflow/dags", description="Remote DAG folder path on the Airflow server"
    )
    default_owner: str = Field(default="teradata_etl_mcp_server", description="Default DAG owner")
    default_pool: str = Field(default="default_pool", description="Default execution pool")
    default_retries: int = Field(default=1, description="Default number of retries")
    default_retry_delay_minutes: int = Field(
        default=5, description="Default retry delay in minutes"
    )

    # Remote deployment settings (optional)
    remote_host: str | None = Field(
        default=None, description="Remote Airflow host for DAG deployment (e.g., hostname or IP)"
    )
    remote_user: str | None = Field(default=None, description="SSH username for remote deployment")
    remote_ssh_key: str | None = Field(
        default=None, description="Path to SSH private key for authentication"
    )
    remote_password: SecretStr | None = Field(
        default=None, description="SSH password for remote deployment (if not using key-based auth)"
    )
    remote_port: int = Field(default=22, ge=1, le=65535, description="SSH port for DAG deployment")
    remote_ssh_key_passphrase: SecretStr | None = Field(
        default=None, description="Passphrase for SSH key"
    )

    model_config = SettingsConfigDict(env_prefix="AIRFLOW_")

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str | None) -> str | None:
        """Ensure base URL doesn't end with trailing slash."""
        if v is None:
            return v
        return v.rstrip("/")

    @field_validator("dag_folder")
    @classmethod
    def validate_dag_folder(cls, v: str) -> str:
        """Reject empty or whitespace-only dag_folder values."""
        if not v or not v.strip():
            raise ValueError("AIRFLOW_DAG_FOLDER must not be empty")
        return v.strip()


class AirbyteSettings(BaseSettings):
    """Airbyte connection settings."""

    enabled: bool = Field(default=False, description="Whether Airbyte is enabled")
    base_url: str | None = Field(default=None, description="Airbyte API base URL")
    client_id: str | None = Field(
        default=None,
        description="OAuth2 Client ID for Airbyte authentication",
    )
    client_secret: SecretStr | None = Field(
        default=None,
        description="OAuth2 Client Secret for Airbyte authentication",
    )
    token_url: str | None = Field(
        default="/api/public/v1/applications/token",
        description="Token endpoint for OAuth2 client credentials flow",
    )
    workspace_id: str | None = Field(
        default=None,
        description="Default Airbyte workspace ID. If omitted, the first available workspace will be used",
    )

    # Sync settings
    timeout: int = Field(default=60, description="API request timeout in seconds")
    default_namespace: str = Field(
        default="default", description="Default namespace for connections"
    )

    model_config = SettingsConfigDict(env_prefix="AIRBYTE_")


class DBTSettings(BaseSettings):
    """dbt (Data Build Tool) settings."""

    # Project settings
    project_dir: Path = Field(
        default=Path("dbt_project"),
        description=(
            "Path to dbt project directory. Relative paths resolve under "
            "Settings.workspace_dir; absolute paths are used as-is."
        ),
    )
    profiles_dir: Path | None = Field(
        default=None, description="Path to dbt profiles directory (defaults to ~/.dbt)"
    )
    target: str = Field(default="dev", description="dbt target environment")

    # Execution settings
    threads: int = Field(default=4, description="Number of threads for dbt execution")
    command_timeout: int = Field(
        default=300,
        description="Default subprocess timeout in seconds for dbt commands",
        ge=1,
    )

    model_config = SettingsConfigDict(env_prefix="DBT_")

    @field_validator("project_dir", "profiles_dir")
    @classmethod
    def validate_path(cls, v: Path | None) -> Path | None:
        """Convert string to Path if needed."""
        if v is None:
            return v
        if isinstance(v, str):
            return Path(v)
        return v


TTU_DEFAULT_VERSION = "17.20"


def _default_workspace_dir() -> Path:
    """Default workspace directory: ``~/teradata-etl-mcp-workspace`` with a temp-dir
    fallback when ``Path.home()`` is unavailable."""
    import tempfile

    try:
        return Path.home() / "teradata-etl-mcp-workspace"
    except (RuntimeError, OSError):
        return Path(tempfile.gettempdir()) / "teradata-etl-mcp-workspace"


class TTUSettings(BaseSettings):
    """Teradata Tools & Utilities (TTU) settings for local TPT/BTEQ execution.

    TTU binaries are installed in platform-specific directories:
      - Windows: C:\\Program Files\\Teradata\\Client\\<version>\\bin
      - Linux:   /opt/teradata/client/<version>/bin
      - macOS:   /Library/Application Support/Teradata/client/<version>/bin

    Set ``ttu_version`` to have default binary paths resolved automatically
    for the current platform.  Explicit ``*_binary_path`` settings always
    take precedence over auto-detected paths.
    """

    enabled: bool = Field(default=False, description="Whether TTU tools are enabled")
    ttu_version: str = Field(
        default=TTU_DEFAULT_VERSION,
        description="TTU version directory name (e.g. '17.20', '17.10'). "
        "Used to build the default installation path.",
    )
    tpt_binary_path: str = Field(
        default="", description="Path to tbuild binary (auto-detected if empty)"
    )
    bteq_binary_path: str = Field(
        default="", description="Path to bteq binary (auto-detected if empty)"
    )
    tdload_binary_path: str = Field(
        default="", description="Path to tdload binary (auto-detected if empty)"
    )
    scripts_dir: Path = Field(
        default=Path("ttu_scripts"),
        description=(
            "Directory for generated TTU scripts. Relative paths resolve "
            "under Settings.workspace_dir; absolute paths are used as-is."
        ),
    )
    command_timeout: int = Field(default=600, ge=1, description="Subprocess timeout in seconds")
    tpt_error_limit: int = Field(default=1, ge=0, description="TPT error limit for DDL operations")

    model_config = SettingsConfigDict(env_prefix="TTU_")

    @model_validator(mode="after")
    def resolve_binary_paths(self) -> "TTUSettings":
        """Fill in binary paths from platform-specific defaults when not explicitly set."""
        import platform as _platform

        system = _platform.system()
        version = self.ttu_version

        if system == "Windows":
            base = Path(rf"C:\Program Files\Teradata\Client\{version}\bin")
        elif system == "Darwin":
            base = Path(f"/Library/Application Support/Teradata/client/{version}/bin")
        else:  # Linux and others
            base = Path(f"/opt/teradata/client/{version}/bin")

        version_explicitly_set = "ttu_version" in self.model_fields_set
        all_paths_set = all([self.tpt_binary_path, self.bteq_binary_path, self.tdload_binary_path])
        needs_auto_detect = (
            self.enabled
            and not all_paths_set
            and version == TTU_DEFAULT_VERSION
            and not version_explicitly_set
            and not base.exists()
        )
        if needs_auto_detect:
            parent = base.parent.parent
            try:
                if parent.is_dir():
                    candidates = []
                    for entry in parent.iterdir():
                        if entry.is_dir():
                            parts = entry.name.split(".")
                            if all(p.isdigit() for p in parts):
                                bin_dir = entry / "bin"
                                if bin_dir.is_dir():
                                    candidates.append(entry.name)
                    if candidates:
                        candidates.sort(
                            key=lambda v: tuple(int(p) for p in v.split(".")),
                            reverse=True,
                        )
                        version = candidates[0]
                        self.ttu_version = version
                        logger.info(
                            "Auto-detected TTU version %s (default %s not found at %s)",
                            version, TTU_DEFAULT_VERSION, base,
                        )
                        if system == "Windows":
                            base = Path(rf"C:\Program Files\Teradata\Client\{version}\bin")
                        elif system == "Darwin":
                            base = Path(f"/Library/Application Support/Teradata/client/{version}/bin")
                        else:
                            base = Path(f"/opt/teradata/client/{version}/bin")
                    else:
                        logger.warning(
                            "TTU default version %s not found at %s and no other versions "
                            "detected in %s. Falling back to bare binary names.",
                            TTU_DEFAULT_VERSION, base, parent,
                        )
                else:
                    logger.warning(
                        "TTU install directory %s does not exist. "
                        "Set TTU_TTU_VERSION or explicit binary paths in .env.",
                        parent,
                    )
            except OSError:
                logger.warning(
                    "Unable to scan TTU install directory %s. "
                    "Falling back to bare binary names.",
                    parent,
                    exc_info=True,
                )

        logger.debug("TTU resolve_binary_paths: platform=%s, version=%s", system, version)
        logger.debug("TTU resolve_binary_paths: base path=%s, exists=%s", base, base.exists())

        defaults = {
            "tpt_binary_path": ("tbuild", base / "tbuild"),
            "bteq_binary_path": ("bteq", base / "bteq"),
            "tdload_binary_path": ("tdload", base / "tdload"),
        }

        import shutil as _shutil

        for field_name, (bare_name, full_path) in defaults.items():
            current = getattr(self, field_name)
            if current and not _shutil.which(current) and not Path(current).exists():
                logger.warning(
                    "TTU %s path '%s' is not a valid binary — ignoring, will auto-detect",
                    field_name, current,
                )
                current = ""
                setattr(self, field_name, current)
            if not current:
                logger.debug(
                    "TTU resolve %s: bare_name=%s, full_path=%s, full_path_exists=%s",
                    field_name, bare_name, full_path, full_path.exists(),
                )
                resolved = _shutil.which(str(full_path))
                logger.debug(
                    "TTU resolve %s: shutil.which(%s) -> %s",
                    field_name, full_path, resolved,
                )
                if resolved:
                    setattr(self, field_name, resolved)
                else:
                    setattr(self, field_name, bare_name)
                logger.debug(
                    "TTU resolve %s: final value=%s",
                    field_name, getattr(self, field_name),
                )
            else:
                logger.debug("TTU resolve %s: already set to %s", field_name, current)

        return self


class PipelineSettings(BaseSettings):
    """Pipeline behavior and optimization settings."""

    # DAG generation
    dags_output_dir: Path = Field(
        default=Path("airflow_dags"),
        description=(
            "Directory to output generated Airflow DAG files. Relative paths "
            "resolve under Settings.workspace_dir; absolute paths are used as-is."
        ),
    )
    default_schedule_interval: str = Field(
        default="@daily", description="Default schedule interval for generated DAGs"
    )
    generate_dbt_by_default: bool = Field(
        default=True, description="Generate dbt models by default when creating pipelines"
    )

    # Validation
    validate_before_deploy: bool = Field(
        default=True, description="Validate pipelines before deployment"
    )

    # Data quality
    enable_data_quality_checks: bool = Field(
        default=True, description="Enable automatic data quality checks"
    )
    dq_null_threshold_percent: float = Field(
        default=5.0, description="Alert threshold for null percentage", ge=0.0, le=100.0
    )
    dq_duplicate_threshold_percent: float = Field(
        default=1.0, description="Alert threshold for duplicate percentage", ge=0.0, le=100.0
    )

    model_config = SettingsConfigDict(env_prefix="PIPELINE_")


class OrchestratorSettings(BaseSettings):
    """Workflow orchestrator settings for pluggable backend support.

    Currently supports Apache Airflow as the workflow orchestration backend.
    The architecture allows for future extension to other backends.
    """

    backend: Literal["airflow"] = Field(
        default="airflow",
        description="Workflow orchestration backend to use",
    )

    model_config = SettingsConfigDict(env_prefix="ORCHESTRATOR_")


class ObservabilitySettings(BaseSettings):
    """Observability and audit logging settings."""

    enable_audit_log: bool = Field(default=False, description="Enable structured audit logging")
    audit_log_file: Path = Field(
        default=Path("logs/audit.jsonl"),
        description=(
            "Audit log file path (JSON Lines). Relative paths resolve "
            "under Settings.workspace_dir; absolute paths are used as-is."
        ),
    )
    enable_lineage_tracking: bool = Field(default=False, description="Enable lineage tracking")
    enable_metrics: bool = Field(default=False, description="Enable metrics collection")

    model_config = SettingsConfigDict(env_prefix="OBSERVABILITY_")


class SecuritySettings(BaseSettings):
    """Security and secrets management settings."""

    # Connection profiles file
    connections_file: Path | None = Field(
        default=None,
        description="Path to connections.yaml file for credential profiles",
    )

    model_config = SettingsConfigDict(env_prefix="SECURITY_")


class MCPServerSettings(BaseSettings):
    """MCP server configuration."""

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", description="Logging level"
    )
    log_file: Path | None = Field(
        default=Path("logs/teradata-etl-mcp-server.log"),
        description=(
            "Log file path. Relative paths resolve under "
            "Settings.workspace_dir; absolute paths are used as-is."
        ),
    )
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string",
    )

    metadata_db_path: Path = Field(
        default=Path(".etl-mcp") / "metadata.db",
        description=(
            "SQLite metadata-store path. Relative paths resolve under "
            "Settings.workspace_dir; absolute paths are used as-is."
        ),
    )

    # Performance
    max_concurrent_requests: int = Field(default=10, ge=1, description="Maximum concurrent requests")
    request_timeout: int = Field(default=300, description="Timeout in seconds for acquiring a concurrency slot (semaphore). Does not limit tool execution time.")

    # Tool filtering
    enabled_tools: list[str] | None = Field(
        default=None,
        description="Allowlist of tool names to register. None = all tools. Env: JSON array or comma-separated string.",
    )

    @field_validator("enabled_tools", mode="before")
    @classmethod
    def _parse_enabled_tools(cls, v: Any) -> list[str] | None:  # noqa: N805
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
            if v.startswith("["):
                import json

                return json.loads(v)
            return [t.strip() for t in v.split(",") if t.strip()]
        return v

    # Input size limits
    max_input_size_bytes: int = Field(
        default=1_048_576,
        description="Max input size per tool call in bytes (1MB). Not currently enforced centrally; reserved for future use.",
    )
    max_sql_length: int = Field(
        default=100_000,
        description="Max SQL statement length in characters. Not currently enforced centrally; reserved for future use.",
    )

    # Distributed features
    redis_url: str | None = Field(
        default=None,
        description="Redis URL for distributed features (circuit breaker, caching). Format: redis://host:port/db",
    )

    # Startup behavior
    validate_on_startup: bool = Field(
        default=True,
        description="Validate connections to enabled services on startup",
    )
    fail_fast_on_startup: bool = Field(
        default=False,
        description="If True, server fails to start when validation fails. If False, logs warnings only.",
    )

    model_config = SettingsConfigDict(env_prefix="MCP_")


class Settings(BaseSettings):
    """Main settings class that aggregates all configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Environment
    environment: Literal["development", "staging", "production"] = Field(
        default="development", description="Current environment"
    )

    # Workspace — root for all server-managed artefact directories.
    # Set ``WORKSPACE_DIR`` to override; defaults to ``~/teradata-etl-mcp-workspace``.
    # Relative path defaults on sub-settings (``dbt.project_dir``,
    # ``ttu.scripts_dir``, ``pipeline.dags_output_dir``, ``mcp.log_file``,
    # ``observability.audit_log_file``, ``mcp.metadata_db_path``) are joined
    # under this directory by ``validate_settings``. Absolute paths are
    # used as-is.
    workspace_dir: Path = Field(
        default_factory=lambda: _default_workspace_dir(),
        description=(
            "Root directory for all server-managed artefacts. Set "
            "WORKSPACE_DIR to override; defaults to ~/teradata-etl-mcp-workspace."
        ),
    )

    # Sub-settings
    teradata: TeradataSettings = Field(default_factory=TeradataSettings)
    airflow: AirflowSettings = Field(default_factory=AirflowSettings)
    airbyte: AirbyteSettings = Field(default_factory=AirbyteSettings)
    dbt: DBTSettings = Field(default_factory=DBTSettings)
    ttu: TTUSettings = Field(default_factory=TTUSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    mcp: MCPServerSettings = Field(default_factory=MCPServerSettings)
    orchestrator: OrchestratorSettings = Field(default_factory=OrchestratorSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)

    # Optional separate source/target Teradata identities for cross-system
    # transfers (e.g. Teradata-to-Teradata via tdload).  When None the main
    # ``teradata`` settings are used as fallback.
    teradata_source: TeradataSettings | None = Field(
        default=None,
        description=(
            "Optional source Teradata identity.  Populated from "
            "TERADATA_SOURCE_* env vars or passed directly."
        ),
    )
    teradata_target: TeradataSettings | None = Field(
        default=None,
        description=(
            "Optional target Teradata identity.  Populated from "
            "TERADATA_TARGET_* env vars or passed directly."
        ),
    )

    @model_validator(mode="after")
    def _populate_source_target_from_env(self) -> "Settings":
        """Build teradata_source / teradata_target from TERADATA_SOURCE_* / TERADATA_TARGET_* env vars."""
        for role, prefix in [
            ("teradata_source", "TERADATA_SOURCE_"),
            ("teradata_target", "TERADATA_TARGET_"),
        ]:
            if getattr(self, role) is not None:
                continue
            env_fields: dict[str, str] = {}
            for key, val in os.environ.items():
                if key.upper().startswith(prefix):
                    field_name = key[len(prefix) :].lower()
                    env_fields[field_name] = val
            if "host" in env_fields:
                try:
                    setattr(self, role, TeradataSettings(**env_fields))
                except Exception:
                    logger.debug(
                        "Could not build %s from env: incomplete or invalid fields",
                        role,
                    )
        return self

    # Convenience accessors ------------------------------------------------

    def get_source_teradata(self) -> TeradataSettings:
        """Return the source Teradata settings, falling back to the main ``teradata``."""
        return self.teradata_source if self.teradata_source is not None else self.teradata

    def get_target_teradata(self) -> TeradataSettings:
        """Return the target Teradata settings, falling back to the main ``teradata``."""
        return self.teradata_target if self.teradata_target is not None else self.teradata

    def is_teradata_to_teradata(self) -> bool:
        """Return True when both source and target Teradata identities are explicitly set."""
        return self.teradata_source is not None and self.teradata_target is not None

    @model_validator(mode="after")
    def validate_settings(self) -> "Settings":
        """Validate cross-setting dependencies."""

        # Resolve workspace-relative path defaults under self.workspace_dir.
        # Absolute paths set explicitly by the operator are left untouched.
        # Then mkdir each so artefacts have a real directory to land in.
        workspace = self.workspace_dir.expanduser().resolve()
        workspace.mkdir(parents=True, exist_ok=True)
        # Update workspace_dir to the resolved + created form so consumers
        # (orchestrator, generators, tools) see the canonical absolute path.
        self.workspace_dir = workspace

        def _resolve_under_workspace(p: Path | None) -> Path | None:
            """Join a relative path under ``workspace`` (no-op for absolute)."""
            if p is None:
                return None
            expanded = Path(p).expanduser()
            return expanded if expanded.is_absolute() else (workspace / expanded)

        # Directory-shaped settings
        self.dbt.project_dir = _resolve_under_workspace(self.dbt.project_dir)  # type: ignore[assignment]
        self.ttu.scripts_dir = _resolve_under_workspace(self.ttu.scripts_dir)  # type: ignore[assignment]
        self.pipeline.dags_output_dir = _resolve_under_workspace(  # type: ignore[assignment]
            self.pipeline.dags_output_dir,
        )
        for dir_field in (
            self.dbt.project_dir,
            self.ttu.scripts_dir,
            self.pipeline.dags_output_dir,
        ):
            if dir_field:
                dir_field.mkdir(parents=True, exist_ok=True)

        # File-shaped settings — resolve under workspace, mkdir parent only.
        self.mcp.log_file = _resolve_under_workspace(self.mcp.log_file)  # type: ignore[assignment]
        self.observability.audit_log_file = _resolve_under_workspace(  # type: ignore[assignment]
            self.observability.audit_log_file,
        )
        self.mcp.metadata_db_path = _resolve_under_workspace(  # type: ignore[assignment]
            self.mcp.metadata_db_path,
        )
        if self.mcp.log_file:
            self.mcp.log_file.parent.mkdir(parents=True, exist_ok=True)
        if self.observability.enable_audit_log:
            self.observability.audit_log_file.parent.mkdir(parents=True, exist_ok=True)
        if self.mcp.metadata_db_path:
            self.mcp.metadata_db_path.parent.mkdir(parents=True, exist_ok=True)
        # Validate Airbyte settings if enabled
        if self.airbyte.enabled and not self.airbyte.base_url:
            raise ValueError("Airbyte is enabled but AIRBYTE_BASE_URL is not set")

        # Validate Airflow token configuration when using simple auth
        if self.airflow.auth_manager == "simple":  # noqa: SIM102
            # Either an access token must be provided, or a token endpoint should be set
            if not self.airflow.access_token and not self.airflow.token_endpoint:
                raise ValueError(
                    "Airflow simple auth requires either AIRFLOW_ACCESS_TOKEN or AIRFLOW_TOKEN_ENDPOINT"
                )

        return self

    def get_connection_string(self, service: str) -> str | None:
        """Get connection string for a service. Returns None if the service is not configured."""
        if service == "teradata":
            return (
                f"teradatasql://{self.teradata.username}:{self.teradata.password.get_secret_value()}"
                f"@{self.teradata.host}:{self.teradata.port}/{self.teradata.database}"
            )
        elif service == "airflow":
            return self.airflow.base_url  # None when Airflow is not configured
        elif service == "airbyte":
            return self.airbyte.base_url
        else:
            raise ValueError(f"Unknown service: {service}")

    def to_dict(self, include_secrets: bool = False) -> dict[str, Any]:
        """
        Convert settings to dictionary.

        Args:
            include_secrets: Whether to include sensitive information

        Returns:
            Dictionary representation of settings
        """
        data = self.model_dump()

        if not include_secrets:
            # Mask sensitive fields
            sensitive_fields = ["password", "token", "webhook_url", "smtp_password"]

            def mask_secrets(obj: Any) -> Any:
                if isinstance(obj, dict):
                    return {
                        k: "***MASKED***"
                        if any(s in k.lower() for s in sensitive_fields)
                        else mask_secrets(v)
                        for k, v in obj.items()
                    }
                elif isinstance(obj, list):
                    return [mask_secrets(item) for item in obj]
                else:
                    return obj

            data = mask_secrets(data)

        return data

    def validate_connectivity(self, timeout: int = 10) -> dict[str, Any]:
        """
        Validate connectivity to configured services.

        This method attempts to connect to each enabled service and reports
        the status. Useful for startup validation and health checks.

        Note: Teradata and Airbyte (when enabled) are required services — failures
        set valid=False. Airflow and Redis are optional — failures generate
        warnings but don't affect the valid flag.

        Args:
            timeout: Connection timeout in seconds

        Returns:
            Dictionary with validation results for each service:
            {
                "valid": bool,  # True if required services (Airflow, Airbyte) are reachable
                "services": {
                    "airflow": {"status": "ok"|"error", "message": str, "latency_ms": float},
                    "airbyte": {...},
                    "teradata": {...},
                    "redis": {...},
                },
                "errors": [str],  # List of error messages
                "warnings": [str],  # List of warning messages
            }
        """
        import time

        from .response_sanitizer import safe_error_message

        results: dict[str, Any] = {
            "valid": True,
            "services": {},
            "errors": [],
            "warnings": [],
        }

        # Validate Airflow
        if not self.airflow.base_url:
            results["services"]["airflow"] = {
                "status": "skipped",
                "message": "Airflow is not configured (AIRFLOW_BASE_URL not set)",
            }
        else:
            try:
                import asyncio

                from .clients.async_airflow_client import AsyncAirflowClient

                base_url = self.airflow.base_url
                username = self.airflow.username
                password = (
                    self.airflow.password.get_secret_value() if self.airflow.password else None
                )
                auth_manager = self.airflow.auth_manager
                token_endpoint = self.airflow.token_endpoint

                async def _check():
                    async with AsyncAirflowClient(
                        base_url=base_url,
                        username=username,
                        password=password,
                        timeout=timeout,
                        auth_manager=auth_manager,
                        token_endpoint=token_endpoint,
                    ) as client:
                        conn = await client.test_connection()
                        if conn.get("connected"):
                            try:
                                conn["providers"] = await asyncio.wait_for(client.get_providers(), timeout=15.0)
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                pass
                        return conn

                start = time.perf_counter()
                try:
                    asyncio.get_running_loop()
                    # Already inside an async event loop (MCP server) —
                    # run the coroutine in a new thread with its own loop.
                    import concurrent.futures

                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                        info = pool.submit(
                            lambda: asyncio.run(asyncio.wait_for(_check(), timeout=timeout)),
                        ).result(timeout=timeout)
                except RuntimeError:
                    # No running loop — safe to use asyncio.run() directly.
                    info = asyncio.run(asyncio.wait_for(_check(), timeout=timeout))
                latency = (time.perf_counter() - start) * 1000
                if info.get("connected"):
                    status_msg = "Airflow API is healthy"
                    missing = []
                    if "providers" in info:
                        if info["providers"].get("incomplete"):
                            status_msg = "Airflow API is healthy but provider discovery was incomplete (timed out or truncated)"
                            results["warnings"].append(status_msg)
                        else:
                            from .clients.async_airflow_client import check_missing_providers
                            missing = check_missing_providers(info["providers"])
                    if missing:
                        names = ", ".join(n for n, _ in missing)
                        install_cmd = "pip install " + " ".join(n for n, _ in missing)
                        status_msg = f"Airflow API is healthy but providers are missing: {names}. Run on the Airflow server: {install_cmd}"
                        results["warnings"].append(status_msg)
                    results["services"]["airflow"] = {
                        "status": "ok",
                        "message": status_msg,
                        "latency_ms": round(latency, 2),
                    }
                    if missing:
                        results["services"]["airflow"]["missing_providers"] = [n for n, _ in missing]
                    if "providers" in info and info["providers"].get("incomplete"):
                        results["services"]["airflow"]["provider_discovery_incomplete"] = True
                else:
                    results["services"]["airflow"] = {
                        "status": "degraded",
                        "message": info.get("error", "Airflow connection check failed"),
                        "latency_ms": round(latency, 2),
                    }
                    results["warnings"].append(
                        f"Airflow connection failed: {info.get('error', 'unknown')}"
                    )
            except Exception as e:
                results["services"]["airflow"] = {
                    "status": "degraded",
                    "message": safe_error_message(e),
                }
                results["warnings"].append(f"Airflow connection failed: {safe_error_message(e)}")

        # Validate Airbyte (if enabled)
        if self.airbyte.enabled:
            try:
                import httpx

                from .utils.tls import build_tls_context

                start = time.perf_counter()
                with httpx.Client(timeout=timeout, verify=build_tls_context()) as client:
                    resp = client.get(f"{self.airbyte.base_url}/api/public/v1/health")
                    latency = (time.perf_counter() - start) * 1000
                    if resp.status_code == 200:
                        results["services"]["airbyte"] = {
                            "status": "ok",
                            "message": "Airbyte API is healthy",
                            "latency_ms": round(latency, 2),
                        }
                    else:
                        results["services"]["airbyte"] = {
                            "status": "degraded",
                            "message": f"Airbyte returned status {resp.status_code}",
                            "latency_ms": round(latency, 2),
                        }
                        results["warnings"].append(
                            f"Airbyte health check returned {resp.status_code}"
                        )
            except Exception as e:
                results["services"]["airbyte"] = {
                    "status": "error",
                    "message": safe_error_message(e),
                }
                results["errors"].append(f"Airbyte connection failed: {safe_error_message(e)}")
                results["valid"] = False
        else:
            results["services"]["airbyte"] = {
                "status": "disabled",
                "message": "Airbyte is not enabled",
            }

        # Validate Teradata
        try:
            # Use teradatasql if available
            import teradatasql

            from .auth import build_teradata_auth_from_settings

            # Build the default TeradataAuth and let it render kwargs — this
            # covers all five mechanisms (TD2/LDAP/JWT/SECRET/BEARER), not
            # just the username+password subset.
            connect_kwargs = build_teradata_auth_from_settings(
                self.teradata
            ).render_for_teradatasql()
            connect_kwargs["encryptdata"] = "true"
            connect_kwargs["connect_timeout"] = str(timeout * 1000)
            connect_kwargs["request_timeout"] = str(timeout * 1000)

            start = time.perf_counter()
            with (
                teradatasql.connect(**connect_kwargs) as conn,
                conn.cursor() as cur,
            ):
                cur.execute("SELECT 1")
                cur.fetchone()
                try:
                    cur.execute("SELECT InfoData FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'")
                    row = cur.fetchone()
                    version = row[0] if row else "unknown"
                except Exception:
                    version = "unknown"
            latency = (time.perf_counter() - start) * 1000
            results["services"]["teradata"] = {
                "status": "ok",
                "message": f"Connected to {self.teradata.host} (version={version})",
                "latency_ms": round(latency, 2),
            }
        except ImportError:
            results["services"]["teradata"] = {
                "status": "skipped",
                "message": "teradatasql package not installed",
            }
            results["warnings"].append("Teradata validation skipped: teradatasql not installed")
        except Exception as e:
            results["services"]["teradata"] = {
                "status": "error",
                "message": safe_error_message(e),
            }
            results["errors"].append(f"Teradata connection failed: {safe_error_message(e)}")
            results["valid"] = False

        # Validate Redis (if configured)
        if self.mcp.redis_url:
            try:
                import redis

                start = time.perf_counter()
                r = redis.from_url(
                    self.mcp.redis_url, decode_responses=True, socket_timeout=timeout
                )
                r.ping()
                latency = (time.perf_counter() - start) * 1000
                results["services"]["redis"] = {
                    "status": "ok",
                    "message": "Redis is reachable",
                    "latency_ms": round(latency, 2),
                }
            except ImportError:
                results["services"]["redis"] = {
                    "status": "skipped",
                    "message": "redis package not installed",
                }
                results["warnings"].append("Redis validation skipped: redis package not installed")
            except Exception as e:
                results["services"]["redis"] = {
                    "status": "degraded",
                    "message": safe_error_message(e),
                }
                # Redis failure is a warning (fallback to in-memory is available)
                results["warnings"].append(f"Redis connection failed: {safe_error_message(e)}")
        else:
            results["services"]["redis"] = {
                "status": "not_configured",
                "message": "Redis URL not configured (using in-memory circuit breaker)",
            }

        return results


# Global settings instance
_settings: Settings | None = None


def load_settings(force_reload: bool = False) -> Settings:
    """
    Load and cache settings.

    Args:
        force_reload: Force reload settings from environment

    Returns:
        Settings instance
    """
    global _settings

    if _settings is None or force_reload:
        _settings = Settings()

    return _settings


def get_settings() -> Settings:
    """
    Get cached settings instance.

    Returns:
        Settings instance
    """
    if _settings is None:
        return load_settings()
    return _settings


def reset_settings():
    """Reset settings cache (useful for testing)."""
    global _settings
    _settings = None
