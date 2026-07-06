"""dbt (Data Build Tool) CLI wrapper and project management.

This module provides a comprehensive wrapper around dbt CLI commands
for model execution, testing, and project management.
"""

import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from ..auth import TeradataAuth

logger = logging.getLogger(__name__)


class DBTClientError(Exception):
    """Base exception for dbt client errors."""

    pass


class DBTCommandError(DBTClientError):
    """Raised when dbt command execution fails."""

    pass


class DBTProjectError(DBTClientError):
    """Raised when dbt project is invalid or not found."""

    pass


class DBTClient:
    """
    Comprehensive dbt CLI wrapper and project management client.

    Provides methods for executing dbt commands, managing projects,
    and parsing dbt outputs.
    """

    def __init__(
        self,
        project_dir: Path,
        profiles_dir: Path | None = None,
        target: str = "dev",
        threads: int = 4,
        command_timeout: int = 300,
    ):
        """
        Initialize dbt client.

        Args:
            project_dir: Path to dbt project directory
            profiles_dir: Optional path to profiles directory (defaults to ~/.dbt)
            target: Target environment (dev, staging, prod)
            threads: Number of threads for parallel execution
            command_timeout: Default subprocess timeout in seconds
        """
        # Validate target
        if not target or not re.match(r"^[a-zA-Z0-9_]+$", target):
            raise ValueError(
                f"Invalid target '{target}': must be non-empty and contain only "
                "alphanumeric characters and underscores"
            )

        # Validate threads
        if not 1 <= threads <= 64:
            raise ValueError(f"Invalid threads {threads}: must be between 1 and 64")

        # Validate command_timeout
        if command_timeout < 1:
            raise ValueError(
                f"Invalid command_timeout {command_timeout}: must be >= 1"
            )

        self.project_dir = Path(project_dir).resolve()
        self.profiles_dir = Path(profiles_dir).resolve() if profiles_dir else None
        self.target = target
        self.threads = threads
        self.command_timeout = command_timeout
        self._file_lock = threading.Lock()

        self._dbt_binary = self._resolve_dbt_binary()

        # Verify project directory exists
        if not self.project_dir.exists():
            raise DBTProjectError(f"Project directory not found: {self.project_dir}")

        # Verify dbt_project.yml exists
        if not (self.project_dir / "dbt_project.yml").exists():
            raise DBTProjectError(
                f"dbt_project.yml not found in {self.project_dir}"
            )

        logger.info("Initialized dbt client for %s", self.project_dir)

    def _build_command(
        self,
        command: str,
        args: list[str] | None = None,
        flags: dict[str, Any] | None = None,
        include_threads: bool = True,
        subcommand_first: bool = False,
        threads_override: int | None = None,
    ) -> list[str]:
        """
        Build dbt command with arguments and flags.

        Args:
            command: dbt command (run, test, build, etc.)
            args: Optional list of arguments
            flags: Optional dictionary of flags
            include_threads: Whether to include --threads flag (some commands don't support it)
            subcommand_first: If True, place args right after command (for 'dbt docs generate')
            threads_override: Per-call thread count; overrides self.threads when provided

        Returns:
            List of command parts
        """
        cmd = [self._dbt_binary, command]

        # For commands like "dbt docs generate", subcommand must come first
        if subcommand_first and args:
            cmd.extend(args)

        # Add standard flags
        cmd.extend(["--project-dir", str(self.project_dir)])

        if self.profiles_dir:
            cmd.extend(["--profiles-dir", str(self.profiles_dir)])

        cmd.extend(["--target", self.target])

        if include_threads:
            effective_threads = threads_override if threads_override is not None else self.threads
            cmd.extend(["--threads", str(effective_threads)])

        # Add custom flags
        if flags:
            for key, value in flags.items():
                if value is True:
                    cmd.append(f"--{key}")
                elif value is not False and value is not None:
                    cmd.extend([f"--{key}", str(value)])

        # Add arguments at the end (unless already added for subcommand_first)
        if args and not subcommand_first:
            cmd.extend(args)

        return cmd

    @staticmethod
    def _compute_env(
        auth: "TeradataAuth | None",
        dotenv_path: "Path | None" = None,
    ) -> dict[str, str]:
        """Compose the subprocess env for a dbt invocation.

        Strips every ``TERADATA_*`` from the inherited shell env (otherwise
        stale wizard-set values could shadow a profile-override identity)
        then merges in ``auth.render_for_dbt_env()`` when an auth is given.
        Adds ``NO_COLOR=1`` to avoid ANSI colour codes disrupting log
        parsing.

        When ``auth is None`` and ``dotenv_path`` points to the project's
        ``.env`` file, ``TERADATA_*`` vars are loaded from that file.  This
        allows commands like ``dbt list`` — which parse ``profiles.yml`` but
        never open a socket — to resolve ``env_var('TERADATA_HOST')`` without
        a real auth object.  ``auth`` always takes precedence over the file.
        """
        base = {k: v for k, v in os.environ.items() if not k.startswith("TERADATA_")}
        base["NO_COLOR"] = "1"
        if auth is not None:
            base.update(auth.render_for_dbt_env())
        elif dotenv_path is not None and Path(dotenv_path).exists():
            from dotenv import dotenv_values  # noqa: PLC0415

            for key, val in dotenv_values(dotenv_path).items():
                if key.startswith("TERADATA_") and val:
                    base[key] = val
        return base

    def _execute_command(
        self,
        command: list[str],
        capture_output: bool = True,
        check: bool = True,
        timeout: int | None = None,
        auth: "TeradataAuth | None" = None,
    ) -> subprocess.CompletedProcess:
        """
        Execute a dbt command.

        Args:
            command: Command to execute
            capture_output: Whether to capture stdout/stderr
            check: Whether to raise exception on non-zero exit
            timeout: Per-command timeout in seconds (defaults to self.command_timeout)
            auth: Optional Teradata identity for the subprocess env. When
                provided, its ``render_for_dbt_env()`` output is merged into
                the subprocess env so ``profiles.yml``'s Jinja ``env_var()``
                calls resolve against this identity instead of whatever was
                in the parent shell.

        Returns:
            CompletedProcess instance

        Raises:
            DBTCommandError: If command fails or times out
        """
        effective_timeout = timeout if timeout is not None else self.command_timeout
        try:
            logger.info("Executing: %s", " ".join(command))

            result = subprocess.run(
                command,
                capture_output=capture_output,
                text=True,
                check=False,
                cwd=self.project_dir,
                timeout=effective_timeout,
                stdin=subprocess.DEVNULL,  # Prevent stdin blocking in MCP stdio mode
                env=self._compute_env(auth, dotenv_path=Path(self.project_dir) / ".env"),
            )

            if check and result.returncode != 0:
                error_msg = f"dbt command failed with exit code {result.returncode}"
                if result.stderr:
                    error_msg += f"\nStderr: {result.stderr}"
                if result.stdout:
                    error_msg += f"\nStdout: {result.stdout}"
                logger.error(error_msg)
                raise DBTCommandError(error_msg)

            return result

        except FileNotFoundError as e:
            logger.error("dbt command not found on PATH: %s", e, exc_info=True)
            raise DBTClientError(
                "dbt is not installed or not on PATH. "
                "Install dbt-teradata with: pip install dbt-teradata"
            ) from e
        except subprocess.TimeoutExpired as e:
            cmd_str = " ".join(command)
            logger.error(
                "dbt command timed out after %d seconds: %s",
                effective_timeout,
                cmd_str,
                exc_info=True,
            )
            raise DBTCommandError(
                f"dbt command timed out after {effective_timeout} seconds: {cmd_str}"
            ) from e
        except subprocess.SubprocessError as e:
            logger.error("Failed to execute dbt command: %s", e, exc_info=True)
            raise DBTCommandError(f"Command execution failed: {e}") from e

    @staticmethod
    def _validate_selection(
        selection: str | list[str], param_name: str = "models"
    ) -> None:
        """Validate model/exclude selection strings.

        Rejects empty strings and null bytes. Intentionally permissive —
        does not restrict valid dbt selectors (tags, graph operators, paths).
        """
        items = [selection] if isinstance(selection, str) else selection
        for item in items:
            if not item or not item.strip():
                raise ValueError(f"Empty string in {param_name} selection")
            if "\x00" in item:
                raise ValueError(
                    f"Null bytes not allowed in {param_name} selection"
                )

    def _parse_run_results(self, results_path: Path | None = None) -> dict[str, Any]:
        """
        Parse dbt run results from JSON file.

        Args:
            results_path: Optional path to run_results.json

        Returns:
            Parsed results dictionary
        """
        if results_path is None:
            results_path = self.project_dir / "target" / "run_results.json"

        if not results_path.exists():
            logger.warning("Run results not found at %s", results_path)
            return {}

        try:
            with self._file_lock, open(results_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error("Failed to parse run results: %s", e, exc_info=True)
            return {}

    def _parse_manifest(self) -> dict[str, Any]:
        """
        Parse dbt manifest.json.

        Returns:
            Parsed manifest dictionary
        """
        manifest_path = self.project_dir / "target" / "manifest.json"

        if not manifest_path.exists():
            logger.warning("Manifest not found at %s", manifest_path)
            return {}

        try:
            with self._file_lock, open(manifest_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error("Failed to parse manifest: %s", e, exc_info=True)
            return {}

    # ==================== Core dbt Commands ====================

    def run(
        self,
        models: str | list[str] | None = None,
        exclude: str | list[str] | None = None,
        selector: str | None = None,
        full_refresh: bool = False,
        vars: dict[str, Any] | None = None,
        threads: int | None = None,
        auth: "TeradataAuth | None" = None,
    ) -> dict[str, Any]:
        """
        Execute dbt run command.

        Args:
            models: Optional model selection (e.g., "model_name", "tag:daily")
            exclude: Optional models to exclude
            selector: Optional selector name
            full_refresh: Force full refresh of incremental models
            vars: Optional variables to pass to dbt
            threads: Per-call thread count; overrides the client default when provided

        Returns:
            Dictionary with execution results
        """
        try:
            if models:
                self._validate_selection(models, "models")
            if exclude:
                self._validate_selection(exclude, "exclude")

            flags = {}

            if models:
                model_list = [models] if isinstance(models, str) else models
                flags["select"] = " ".join(model_list)

            if exclude:
                exclude_list = [exclude] if isinstance(exclude, str) else exclude
                flags["exclude"] = " ".join(exclude_list)

            if selector:
                flags["selector"] = selector

            if full_refresh:
                flags["full-refresh"] = True

            if vars:
                flags["vars"] = json.dumps(vars)

            cmd = self._build_command("run", flags=flags, threads_override=threads)
            result = self._execute_command(cmd, auth=auth)

            # Parse results
            run_results = self._parse_run_results()

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "results": run_results.get("results", [])
                if isinstance(run_results, dict)
                else run_results,
            }

        except Exception as e:
            logger.error("dbt run failed: %s", e, exc_info=True)
            raise

    def test(
        self,
        models: str | list[str] | None = None,
        exclude: str | list[str] | None = None,
        selector: str | None = None,
        auth: "TeradataAuth | None" = None,
    ) -> dict[str, Any]:
        """
        Execute dbt test command.

        Args:
            models: Optional model selection
            exclude: Optional models to exclude
            selector: Optional selector name
            auth: Optional Teradata identity for the subprocess env.

        Returns:
            Dictionary with test results
        """
        try:
            if models:
                self._validate_selection(models, "models")
            if exclude:
                self._validate_selection(exclude, "exclude")

            flags = {}

            if models:
                model_list = [models] if isinstance(models, str) else models
                flags["select"] = " ".join(model_list)

            if exclude:
                exclude_list = [exclude] if isinstance(exclude, str) else exclude
                flags["exclude"] = " ".join(exclude_list)

            if selector:
                flags["selector"] = selector

            cmd = self._build_command("test", flags=flags)
            result = self._execute_command(cmd, check=False, auth=auth)

            # Parse results
            run_results = self._parse_run_results()

            # Count test results
            test_summary = {
                "passed": 0,
                "failed": 0,
                "error": 0,
                "skipped": 0,
            }

            for result_item in run_results.get("results", []):
                status = result_item.get("status", "unknown").lower()
                if status in test_summary:
                    test_summary[status] += 1

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "results": run_results.get("results", [])
                if isinstance(run_results, dict)
                else run_results,
                "test_summary": test_summary,
            }

        except Exception as e:
            logger.error("dbt test failed: %s", e, exc_info=True)
            raise

    def build(
        self,
        models: str | list[str] | None = None,
        exclude: str | list[str] | None = None,
        selector: str | None = None,
        full_refresh: bool = False,
        auth: "TeradataAuth | None" = None,
    ) -> dict[str, Any]:
        """
        Execute dbt build command (run + test).

        Args:
            models: Optional model selection
            exclude: Optional models to exclude
            selector: Optional selector name
            full_refresh: Force full refresh of incremental models

        Returns:
            Dictionary with build results
        """
        try:
            if models:
                self._validate_selection(models, "models")
            if exclude:
                self._validate_selection(exclude, "exclude")

            flags = {}

            if models:
                model_list = [models] if isinstance(models, str) else models
                flags["select"] = " ".join(model_list)

            if exclude:
                exclude_list = [exclude] if isinstance(exclude, str) else exclude
                flags["exclude"] = " ".join(exclude_list)

            if selector:
                flags["selector"] = selector

            if full_refresh:
                flags["full-refresh"] = True

            cmd = self._build_command("build", flags=flags)
            result = self._execute_command(cmd, check=False, auth=auth)

            # Parse results
            run_results = self._parse_run_results()

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "results": run_results.get("results", [])
                if isinstance(run_results, dict)
                else run_results,
            }

        except Exception as e:
            logger.error("dbt build failed: %s", e, exc_info=True)
            raise

    def compile(
        self,
        models: str | list[str] | None = None,
        exclude: str | list[str] | None = None,
        auth: "TeradataAuth | None" = None,
    ) -> dict[str, Any]:
        """
        Execute dbt compile command.

        Args:
            models: Optional model selection
            exclude: Optional models to exclude

        Returns:
            Dictionary with compilation results
        """
        try:
            if models:
                self._validate_selection(models, "models")
            if exclude:
                self._validate_selection(exclude, "exclude")

            flags = {}

            if models:
                model_list = [models] if isinstance(models, str) else models
                flags["select"] = " ".join(model_list)

            if exclude:
                exclude_list = [exclude] if isinstance(exclude, str) else exclude
                flags["exclude"] = " ".join(exclude_list)

            cmd = self._build_command("compile", flags=flags)
            result = self._execute_command(cmd, check=False, auth=auth)

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        except Exception as e:
            logger.error("dbt compile failed: %s", e, exc_info=True)
            raise

    def snapshot(self, auth: "TeradataAuth | None" = None) -> dict[str, Any]:
        """
        Execute dbt snapshot command.

        Returns:
            Dictionary with snapshot results
        """
        try:
            cmd = self._build_command("snapshot")
            result = self._execute_command(cmd, auth=auth)

            run_results = self._parse_run_results()

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "results": run_results.get("results", [])
                if isinstance(run_results, dict)
                else run_results,
            }

        except Exception as e:
            logger.error("dbt snapshot failed: %s", e, exc_info=True)
            raise

    def seed(
        self,
        select: str | None = None,
        full_refresh: bool = False,
        auth: "TeradataAuth | None" = None,
    ) -> dict[str, Any]:
        """
        Execute dbt seed command.

        Args:
            select: Optional seed selection
            full_refresh: Force reload of seed data

        Returns:
            Dictionary with seed results
        """
        try:
            flags = {}

            if select:
                flags["select"] = select

            if full_refresh:
                flags["full-refresh"] = True

            cmd = self._build_command("seed", flags=flags)
            result = self._execute_command(cmd, auth=auth)

            run_results = self._parse_run_results()

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "results": run_results.get("results", [])
                if isinstance(run_results, dict)
                else run_results,
            }

        except Exception as e:
            logger.error("dbt seed failed: %s", e, exc_info=True)
            raise

    def docs_generate(self, auth: "TeradataAuth | None" = None) -> dict[str, Any]:
        """
        Generate dbt documentation.

        Returns:
            Dictionary with generation results
        """
        try:
            cmd = self._build_command("docs", args=["generate"], subcommand_first=True)
            result = self._execute_command(
                cmd, timeout=max(self.command_timeout, 600), auth=auth,
            )

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        except Exception as e:
            logger.error("dbt docs generate failed: %s", e, exc_info=True)
            raise

    def clean(self, auth: "TeradataAuth | None" = None) -> dict[str, Any]:
        """
        Execute dbt clean command (remove target directory).

        Returns:
            Dictionary with clean results
        """
        try:
            cmd = self._build_command("clean", include_threads=False)
            result = self._execute_command(cmd, auth=auth)

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        except Exception as e:
            logger.error("dbt clean failed: %s", e, exc_info=True)
            raise

    def debug(self, auth: "TeradataAuth | None" = None) -> dict[str, Any]:
        """
        Execute dbt debug command (test connection and configuration).

        Returns:
            Dictionary with debug information
        """
        try:
            cmd = self._build_command("debug", include_threads=False)
            result = self._execute_command(cmd, check=False, auth=auth)

            # Check for success indicators in output
            # dbt debug shows "All checks passed!" on success
            # Also check returncode as backup
            stdout_lower = result.stdout.lower()
            connection_ok = (
                "all checks passed" in stdout_lower
                or "connection test: ok" in stdout_lower
                or (result.returncode == 0 and "error" not in stdout_lower)
            )

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "connection_ok": connection_ok,
            }

        except Exception as e:
            logger.error("dbt debug failed: %s", e, exc_info=True)
            raise

    def deps(self, auth: "TeradataAuth | None" = None) -> dict[str, Any]:
        """
        Execute dbt deps command (install dependencies).
        Note: deps command does not support --threads flag.

        Returns:
            Dictionary with deps results
        """
        try:
            # Build command without threads parameter
            cmd = [self._dbt_binary, "deps", "--project-dir", str(self.project_dir)]
            if self.profiles_dir:
                cmd.extend(["--profiles-dir", str(self.profiles_dir)])
            if self.target:
                cmd.extend(["--target", self.target])

            result = self._execute_command(
                cmd, timeout=max(self.command_timeout, 600), auth=auth,
            )

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        except Exception as e:
            logger.error("dbt deps failed: %s", e, exc_info=True)
            raise

    def parse(self, auth: "TeradataAuth | None" = None) -> dict[str, Any]:
        """
        Execute dbt parse command.

        Parses the project and writes manifest.json to the target directory
        without compiling SQL or connecting to the warehouse.
        Useful for validating project structure, refreshing manifest for tooling,
        and fast pre-flight checks before dbt run or dbt build.

        Returns:
            Dictionary with parse results including manifest_path
        """
        try:
            cmd = self._build_command("parse", include_threads=False)
            result = self._execute_command(cmd, auth=auth)

            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "manifest_path": str(self.project_dir / "target" / "manifest.json"),
            }

        except Exception as e:
            logger.error("dbt parse failed: %s", e, exc_info=True)
            raise

    # ==================== Project Management ====================

    def list_models(self) -> list[dict[str, Any]]:
        """
        List all models in the dbt project.

        Returns:
            List of model information dictionaries
        """
        try:
            # Run dbt list command (ls doesn't support --threads)
            cmd = self._build_command(
                "list", flags={"resource-type": "model"}, include_threads=False
            )
            self._execute_command(cmd)

            # Get detailed info from manifest
            manifest = self._parse_manifest()
            nodes = manifest.get("nodes", {})

            models = []
            for node_id, node_data in nodes.items():
                if node_data.get("resource_type") == "model":
                    models.append(
                        {
                            "name": node_data.get("name"),
                            "unique_id": node_id,
                            "package_name": node_data.get("package_name"),
                            "path": node_data.get("path"),
                            "original_file_path": node_data.get("original_file_path"),
                            "database": node_data.get("database"),
                            "schema": node_data.get("schema"),
                            "alias": node_data.get("alias"),
                            "description": node_data.get("description", ""),
                            "materialized": node_data.get("config", {}).get("materialized"),
                            "tags": node_data.get("tags", []),
                            "depends_on": node_data.get("depends_on", {}).get("nodes", []),
                        }
                    )

            logger.info("Found %d models", len(models))
            return models

        except Exception as e:
            logger.error("Failed to list models: %s", e, exc_info=True)
            raise

    def list_sources(self) -> list[dict[str, Any]]:
        """
        List all sources in the dbt project.

        Returns:
            List of source information dictionaries
        """
        try:
            manifest = self._parse_manifest()
            sources = manifest.get("sources", {})

            source_list = []
            for source_id, source_data in sources.items():
                source_list.append(
                    {
                        "name": source_data.get("name"),
                        "source_name": source_data.get("source_name"),
                        "unique_id": source_id,
                        "database": source_data.get("database"),
                        "schema": source_data.get("schema"),
                        "identifier": source_data.get("identifier"),
                        "description": source_data.get("description", ""),
                    }
                )

            logger.info("Found %d sources", len(source_list))
            return source_list

        except Exception as e:
            logger.error("Failed to list sources: %s", e, exc_info=True)
            raise

    def list_tests(self) -> list[dict[str, Any]]:
        """
        List all tests in the dbt project.

        Returns:
            List of test information dictionaries
        """
        try:
            manifest = self._parse_manifest()
            nodes = manifest.get("nodes", {})

            tests = []
            for node_id, node_data in nodes.items():
                if node_data.get("resource_type") == "test":
                    tests.append(
                        {
                            "name": node_data.get("name"),
                            "unique_id": node_id,
                            "test_type": node_data.get("test_metadata", {}).get("name"),
                            "column_name": node_data.get("column_name"),
                            "depends_on": node_data.get("depends_on", {}).get("nodes", []),
                        }
                    )

            logger.info("Found %d tests", len(tests))
            return tests

        except Exception as e:
            logger.error("Failed to list tests: %s", e, exc_info=True)
            raise

    def validate_project(
        self, auth: "TeradataAuth | None" = None
    ) -> dict[str, Any]:
        """
        Validate dbt project configuration and structure.

        Args:
            auth: Optional Teradata identity threaded through to ``dbt debug``
                and ``dbt compile``. Without it, the subprocess env gets
                stripped of any inherited ``TERADATA_*`` and those two dbt
                commands will fail with missing-credential errors.

        Returns:
            Dictionary with validation results
        """
        issues = []
        warnings = []

        try:
            # Check dbt_project.yml exists
            project_yml = self.project_dir / "dbt_project.yml"
            if not project_yml.exists():
                issues.append("dbt_project.yml not found")
            else:
                # Parse and validate project file
                try:
                    with open(project_yml) as f:
                        project_config = yaml.safe_load(f)

                    # Check required fields
                    required_fields = ["name", "version", "profile"]
                    for field in required_fields:
                        if field not in project_config:
                            issues.append(f"Missing required field in dbt_project.yml: {field}")

                except Exception as e:
                    issues.append(f"Failed to parse dbt_project.yml: {e}")

            # Check models directory exists
            models_dir = self.project_dir / "models"
            if not models_dir.exists():
                warnings.append("models directory not found")

            # Run dbt debug to check connection
            debug_result = self.debug(auth=auth)
            if not debug_result.get("connection_ok"):
                issues.append("Database connection failed (run 'dbt debug' for details)")

            # Try to compile project
            try:
                compile_result = self.compile(auth=auth)
                if not compile_result.get("success"):
                    issues.append("Project compilation failed")
            except Exception as e:
                issues.append(f"Compilation check failed: {e}")

            # Check for circular dependencies
            manifest = self._parse_manifest()
            if manifest:
                # TODO: Implement circular dependency detection
                pass

            is_valid = len(issues) == 0

            result = {
                "valid": is_valid,
                "issues": issues,
                "warnings": warnings,
                "project_dir": str(self.project_dir),
                "target": self.target,
            }

            if is_valid:
                logger.info("dbt project validation passed")
            else:
                logger.warning("dbt project validation failed with %d issues", len(issues))

            return result

        except Exception as e:
            logger.error("Project validation failed: %s", e, exc_info=True)
            raise

    def get_model_sql(self, model_name: str) -> str | None:
        """
        Get compiled SQL for a specific model.

        Args:
            model_name: Model name

        Returns:
            Compiled SQL string or None if not found
        """
        try:
            # Compile first to ensure we have latest
            self.compile(models=model_name)

            # Find compiled SQL file
            manifest = self._parse_manifest()
            nodes = manifest.get("nodes", {})

            for _node_id, node_data in nodes.items():
                if (
                    node_data.get("resource_type") == "model"
                    and node_data.get("name") == model_name
                ):
                    compiled_path = node_data.get("compiled_path")
                    if compiled_path:
                        full_path = self.project_dir / compiled_path
                        if full_path.exists():
                            with open(full_path) as f:
                                return f.read()

            logger.warning("Compiled SQL not found for model: %s", model_name)
            return None

        except Exception as e:
            logger.error("Failed to get model SQL: %s", e, exc_info=True)
            return None

    def get_project_info(self) -> dict[str, Any]:
        """
        Get dbt project information.

        Returns:
            Dictionary with project details
        """
        try:
            project_yml = self.project_dir / "dbt_project.yml"

            if not project_yml.exists():
                raise DBTProjectError("dbt_project.yml not found")

            with open(project_yml) as f:
                project_config = yaml.safe_load(f)

            # Get model counts
            models = self.list_models()
            sources = self.list_sources()
            tests = self.list_tests()

            return {
                "name": project_config.get("name"),
                "version": project_config.get("version"),
                "profile": project_config.get("profile"),
                "project_dir": str(self.project_dir),
                "target": self.target,
                "model_count": len(models),
                "source_count": len(sources),
                "test_count": len(tests),
                "config": project_config,
            }

        except Exception as e:
            logger.error("Failed to get project info: %s", e, exc_info=True)
            raise

    # ==================== Utility Methods ====================

    def get_dbt_version(self) -> str:
        """
        Get dbt version.

        Returns:
            Version string
        """
        try:
            result = subprocess.run(
                [self._dbt_binary, "--version"],
                capture_output=True,
                text=True,
                check=True,
                stdin=subprocess.DEVNULL,
                env={**os.environ, "NO_COLOR": "1"},
            )

            # Parse version from output
            for line in result.stdout.split("\n"):
                if "installed version" in line.lower():
                    version = line.split(":")[-1].strip()
                    return version

            return result.stdout.strip()

        except Exception as e:
            logger.error("Failed to get dbt version: %s", e, exc_info=True)
            return "unknown"

    def get_manifest(self) -> dict[str, Any] | None:
        """
        Read and parse manifest.json file.

        Returns:
            Manifest dictionary or None if not found
        """
        try:
            manifest_path = self.project_dir / "target" / "manifest.json"

            if not manifest_path.exists():
                logger.warning("Manifest not found at %s", manifest_path)
                return None

            with open(manifest_path) as f:
                return json.load(f)

        except Exception as e:
            logger.error("Failed to read manifest: %s", e, exc_info=True)
            return None

    def get_catalog(self) -> dict[str, Any] | None:
        """
        Read and parse catalog.json file.

        Returns:
            Catalog dictionary or None if not found
        """
        try:
            catalog_path = self.project_dir / "target" / "catalog.json"

            if not catalog_path.exists():
                logger.warning("Catalog not found at %s", catalog_path)
                return None

            with open(catalog_path) as f:
                return json.load(f)

        except Exception as e:
            logger.error("Failed to read catalog: %s", e, exc_info=True)
            return None

    def get_run_results(self) -> dict[str, Any] | None:
        """
        Read and parse run_results.json file.

        Returns:
            Run results dictionary or None if not found
        """
        try:
            results_path = self.project_dir / "target" / "run_results.json"

            if not results_path.exists():
                logger.warning("Run results not found at %s", results_path)
                return None

            with open(results_path) as f:
                return json.load(f)

        except Exception as e:
            logger.error("Failed to read run results: %s", e, exc_info=True)
            return None

    def get_project_config(self) -> dict[str, Any] | None:
        """
        Read and parse dbt_project.yml file.

        Returns:
            Project config dictionary or None if not found
        """
        try:
            config_path = self.project_dir / "dbt_project.yml"

            if not config_path.exists():
                logger.warning("dbt_project.yml not found at %s", config_path)
                return None

            with open(config_path) as f:
                return yaml.safe_load(f)

        except Exception as e:
            logger.error("Failed to read project config: %s", e, exc_info=True)
            return None

    def get_profiles_config(self) -> dict[str, Any] | None:
        """
        Read and parse profiles.yml file.

        Returns:
            Profiles config dictionary or None if not found
        """
        try:
            if not self.profiles_dir:
                profiles_path = Path.home() / ".dbt" / "profiles.yml"
            else:
                profiles_path = self.profiles_dir / "profiles.yml"

            if not profiles_path.exists():
                logger.warning("profiles.yml not found at %s", profiles_path)
                return None

            with open(profiles_path) as f:
                return yaml.safe_load(f)

        except Exception as e:
            logger.error("Failed to read profiles config: %s", e, exc_info=True)
            return None

    def get_target_schema(self) -> str | None:
        """
        Resolve the schema for the active dbt target from profiles.yml.

        Reads the profile name from dbt_project.yml, then resolves the effective
        target using the following priority:
          1. self.target — the target this client was initialised with
          2. profiles[profile_name]['target'] — the profile's own default target

        Then navigates outputs[effective_target].schema (or .dataset for BigQuery).

        Returns:
            Schema string, or None if not resolvable.
        """
        try:
            project_config = self.get_project_config()
            if not project_config:
                return None
            profile_name = project_config.get("profile")
            if not profile_name:
                return None

            profiles_config = self.get_profiles_config()
            if not profiles_config:
                return None

            profile_data = profiles_config.get(profile_name, {})
            outputs = profile_data.get("outputs", {})

            # Prefer self.target; fall back to the profile's own default target
            effective_target = self.target
            if effective_target not in outputs:
                effective_target = profile_data.get("target", self.target)

            target_config = outputs.get(effective_target, {})
            # 'schema' is the standard field; some adapters use 'dataset'
            raw = target_config.get("schema") or target_config.get("dataset")
            if raw and "{{" in raw:
                env = self._compute_env(
                    auth=None, dotenv_path=Path(self.project_dir) / ".env"
                )
                raw = self._resolve_env_var_expression(raw, env)
            return raw
        except Exception as e:
            logger.warning("Could not resolve target schema from profiles: %s", e)
            return None

    def _resolve_env_var_expression(self, value: str, env: dict[str, str]) -> str | None:
        """Resolve a {{ env_var('KEY', 'default') }} expression using a pre-built env dict.

        Returns the resolved value, the default, or None. Returns the raw value unchanged
        if it does not match the env_var() pattern.
        """
        import re

        pattern = (
            r"\{\{\s*env_var\(['\"]([^'\"]+)['\"]"
            r"(?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*\}\}"
        )
        match = re.fullmatch(pattern, value.strip())
        if not match:
            return value
        key, default = match.group(1), match.group(2) or ""
        if key in env:
            return env[key] or None
        return default or None

    @staticmethod
    def _resolve_dbt_binary() -> str:
        """Locate the dbt binary, preferring PATH and falling back to the
        running Python's venv Scripts/bin directory."""
        binary = shutil.which("dbt")
        if binary:
            return binary
        venv_scripts = Path(sys.executable).parent
        candidate = venv_scripts / ("dbt.exe" if platform.system() == "Windows" else "dbt")
        if candidate.exists():
            return str(candidate)
        return "dbt"

    @staticmethod
    def check_installation() -> dict[str, Any]:
        """
        Check if dbt and dbt-teradata adapter are installed.

        Returns:
            Dictionary with installation status and versions:
            {
                "installed": bool,
                "dbt_version": str or None,
                "teradata_installed": bool,
                "teradata_version": str or None,
                "plugins": dict of plugin_name: version
            }
        """
        try:
            result = subprocess.run(
                [DBTClient._resolve_dbt_binary(), "--version"],
                capture_output=True,
                text=True,
                check=False,
                stdin=subprocess.DEVNULL,
                env={**os.environ, "NO_COLOR": "1"},
            )

            if result.returncode != 0:
                return {
                    "installed": False,
                    "dbt_version": None,
                    "teradata_installed": False,
                    "teradata_version": None,
                    "plugins": {},
                }

            # Parse version output
            output = result.stdout
            dbt_version = None
            plugins = {}

            # Extract dbt core version (looks for "installed version: X.X.X" or "installed: X.X.X")
            for line in output.split("\n"):
                line_lower = line.lower().strip()

                # Get core version
                if "installed version:" in line_lower or (
                    line_lower.startswith("- installed:") and "core:" in output.lower()
                ):
                    version = line.split(":")[-1].strip()
                    if version and version != "unknown":
                        dbt_version = version

                # Get plugins (looks for "- plugin_name: version" under Plugins section)
                if line.strip().startswith("-") and ":" in line:
                    parts = line.strip()[1:].split(":", 1)  # Remove leading "-" and split
                    if len(parts) == 2:
                        plugin_name = parts[0].strip()
                        plugin_version = parts[1].strip()
                        # Only add if it looks like a plugin entry (not "installed:" or "latest:")
                        if plugin_name not in ["installed", "latest"] and plugin_version:
                            plugins[plugin_name] = plugin_version

            teradata_installed = "teradata" in plugins
            teradata_version = plugins.get("teradata")

            result_dict = {
                "installed": True,
                "dbt_version": dbt_version,
                "teradata_installed": teradata_installed,
                "teradata_version": teradata_version,
                "plugins": plugins,
            }

            if teradata_installed:
                logger.info(
                    "dbt %s with teradata adapter %s is installed",
                    dbt_version,
                    teradata_version,
                )
            else:
                logger.warning(
                    "dbt %s is installed but teradata adapter is NOT installed",
                    dbt_version,
                )

            return result_dict

        except FileNotFoundError:
            logger.error("dbt command not found", exc_info=True)
            return {
                "installed": False,
                "dbt_version": None,
                "teradata_installed": False,
                "teradata_version": None,
                "plugins": {},
            }
        except Exception as e:
            logger.error("Failed to check dbt installation: %s", e, exc_info=True)
            return {
                "installed": False,
                "dbt_version": None,
                "teradata_installed": False,
                "teradata_version": None,
                "plugins": {},
            }


# Alias for compatibility with tests
DbtClient = DBTClient
