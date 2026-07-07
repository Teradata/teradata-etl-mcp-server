"""Teradata Tools & Utilities (TTU) client for local TPT/BTEQ execution.

This module provides a subprocess-based client for executing Teradata
TPT (tbuild, tdload) and BTEQ binaries locally, inspired by the
standard Teradata TTU conventions.
"""

from __future__ import annotations

import logging
import os
import platform
import re
import shlex
import shutil
import subprocess
import tempfile
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from ..auth import TeradataAuth

logger = logging.getLogger(__name__)

# CLIv2 identity env vars that tdload/tbuild read when expanding ``@TdpId``
# / ``@UserName`` / etc. in job-variable files and TPT scripts. These are
# scrubbed from the inherited shell env before ``env_override`` is merged
# so a stale parent-shell ``UserPassword`` can't shadow a renderer that
# deliberately omits it (JWT/SECRET/BEARER). See :meth:`TTUClient._run_subprocess`.
_TTU_IDENTITY_ENV_KEYS: frozenset[str] = frozenset(
    {
        "TdpId",
        "UserName",
        "UserPassword",
        "LogonMech",
        "LogonMechData",
    }
)


class TTUClientError(Exception):
    """Base exception for TTU client errors."""


class TTUCommandError(TTUClientError):
    """Raised when a TTU command execution fails."""


class TTUNotInstalledError(TTUClientError):
    """Raised when a required TTU binary is not found."""


class TTUClient:
    """Subprocess-based client for local Teradata TPT/BTEQ execution.

    Wraps tbuild, tdload, and bteq binaries. The client is **stateless with
    respect to identity** — every public method takes a :class:`TeradataAuth`
    that determines logon for that single call. The resolver upstream picks
    whether that identity came from the wizard default or from an explicit
    ``connections.yaml`` profile.
    """

    def __init__(
        self,
        tpt_binary: str = "tbuild",
        bteq_binary: str = "bteq",
        tdload_binary: str = "tdload",
        scripts_dir: Path | None = None,
        command_timeout: int = 600,
        tpt_error_limit: int = 1,
    ):
        self._tpt_binary = tpt_binary
        self._bteq_binary = bteq_binary
        self._tdload_binary = tdload_binary
        self._scripts_dir = Path(scripts_dir) if scripts_dir else Path("./ttu_scripts")
        self._command_timeout = command_timeout
        self._tpt_error_limit = tpt_error_limit
        self._file_lock = threading.Lock()

        logger.info("Initialized TTUClient")

    # ==================== Platform-Aware Installation ====================

    @staticmethod
    def get_default_install_dir(version: str) -> Path:
        """Return the platform-specific TTU installation base directory.

        Args:
            version: TTU version string (e.g. '17.20').

        Returns:
            Path to the ``bin`` directory for the given version.

        Platform defaults:
            Windows: C:\\Program Files\\Teradata\\Client\\<version>\\bin
            Linux:   /opt/teradata/client/<version>/bin
            macOS:   /Library/Application Support/Teradata/client/<version>/bin
        """
        system = platform.system()
        if system == "Windows":
            return Path(rf"C:\Program Files\Teradata\Client\{version}\bin")
        elif system == "Darwin":
            return Path(f"/Library/Application Support/Teradata/client/{version}/bin")
        else:
            return Path(f"/opt/teradata/client/{version}/bin")

    @staticmethod
    def get_binary_search_paths(
        binary_name: str,
        version: str,
    ) -> list[str]:
        """Return an ordered list of candidate paths for a TTU binary.

        The list starts with the versioned install directory for the current
        platform, then falls back to the bare binary name (resolved via PATH).

        Args:
            binary_name: Bare binary name, e.g. ``"tbuild"``.
            version: TTU version string (e.g. '17.20').

        Returns:
            List of candidate path strings, most-specific first.
        """
        install_dir = TTUClient.get_default_install_dir(version)
        return [str(install_dir / binary_name), binary_name]

    # ==================== Public Methods ====================

    @staticmethod
    def check_installation(version: str) -> dict[str, Any]:
        """Check which TTU binaries are available on the system.

        Searches the platform-specific versioned install directory first,
        then falls back to PATH.

        Args:
            version: TTU version string (e.g. '17.20').

        Returns:
            Dict with installation status for each binary plus the
            detected install directory and platform info.
        """
        install_dir = TTUClient.get_default_install_dir(version)

        def _find(name: str) -> str | None:
            for candidate in TTUClient.get_binary_search_paths(name, version):
                found = shutil.which(candidate)
                if found:
                    return found
            return None

        tbuild = _find("tbuild")
        bteq = _find("bteq")
        tdload = _find("tdload")

        return {
            "tbuild_installed": tbuild is not None,
            "tbuild_path": tbuild,
            "bteq_installed": bteq is not None,
            "bteq_path": bteq,
            "tdload_installed": tdload is not None,
            "tdload_path": tdload,
            "any_installed": any([tbuild, bteq, tdload]),
            "version": version,
            "install_dir": str(install_dir),
            "install_dir_exists": install_dir.exists(),
            "platform": platform.system(),
        }

    def execute_tpt_ddl(
        self,
        auth: TeradataAuth,
        sql_statements: list[str],
        job_name: str | None = None,
        error_list: list[int] | None = None,
        save_script: bool = False,
    ) -> dict[str, Any]:
        """Execute TPT DDL statements via tbuild.

        Args:
            auth: Teradata authentication identity for this invocation.
            sql_statements: List of DDL SQL statements to execute.
            job_name: Optional TPT job name. Auto-generated if None.
            error_list: Optional list of error codes to tolerate.
            save_script: If True, save a sanitized copy of the script.

        Returns:
            Execution result dict with success, stdout, stderr keys.

        Raises:
            AuthUnsupportedError: If ``auth.mechanism`` is BEARER (tbuild
                inherits tdload's CLIv2-config requirement).
        """
        if not shutil.which(self._tpt_binary):
            raise TTUNotInstalledError(
                f"TPT binary '{self._tpt_binary}' not found. "
                "Verify TTU_TTU_VERSION matches your installation or set TTU_TPT_BINARY_PATH in .env."
            )

        if not sql_statements or not isinstance(sql_statements, list):
            raise ValueError("sql_statements must be a non-empty list")

        if job_name is None:
            job_name = f"elt_tptddl_{uuid.uuid4().hex[:12]}"

        # tbuild pulls attribute values from subprocess env via @VarName
        # references in the TPT DDL script. ``render_for_tdload`` emits
        # ``TdpId``/``UserName``/``UserPassword``/``LogonMech``/``LogonMechData``
        # appropriately for TD2/LDAP/JWT/SECRET. BEARER requires CLIv2
        # config (clispb.dat) that can't be expressed on argv; ``render_for_tdload``
        # raises AuthUnsupportedError in that case.
        tdload_rendering = auth.render_for_tdload()

        script = self._prepare_tpt_ddl_script(sql_statements, job_name, error_list)

        result: dict[str, Any] = {"success": False, "job_name": job_name}

        if save_script:
            result["script_path"] = str(
                self._save_sanitized_script(script, "tpt_ddl", ".tpt", auth=auth)
            )

        temp_path = self._write_temp_file(script, prefix="tpt_ddl_", suffix=".tpt")
        try:
            cmd = [self._tpt_binary, "-f", str(temp_path), job_name]
            proc_result = self._run_subprocess(
                cmd,
                env_override=tdload_rendering.env_vars,
                sanitize_secrets=self._auth_secrets(auth),
            )
            result.update(proc_result)
            result["success"] = proc_result["returncode"] == 0
        finally:
            self._secure_delete(temp_path)

        return result

    def execute_tdload(
        self,
        auth: TeradataAuth,
        mode: str,
        save_script: bool = False,
        save_tpt_script: bool = False,
        tdload_options: str | None = None,
        tdload_job_var_file: str | None = None,
        source_mechanism: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute tdload for data loading operations.

        Generates a job variable file with credentials and parameters, then
        executes tdload with the -j flag. Credentials and other job variables
        injected via the generated job var file are not placed on the command
        line.

        Note:
            Any values passed via ``tdload_options`` are appended verbatim to the
            tdload command line and may be visible in process listings. Do not
            include credentials or other secrets in ``tdload_options``.

        Args:
            auth: Teradata authentication identity for this invocation. The
                job-variable file and subprocess env are both built from this.
                BEARER raises :class:`AuthUnsupportedError` at the tdload layer
                (CLIv2 requires a config-file path for BEARER, not argv).
            mode: Operation mode - 'file_to_table', 'table_to_file', or 'table_to_table'.
            save_script: If True, save a sanitized copy of the job var file
                (the ``name=value`` config that drove tdload). Returns the
                path in the response's ``script_path`` field.
            save_tpt_script: If True AND tdload succeeds, locate the TPT
                script tdload generated under ``$TWB_ROOT/jobs/<job_name>/``
                and copy a sanitized version into ``scripts_dir``. Returns
                the path in the response's ``tpt_script_path`` field.
                Captured only on success — failed runs may produce partial
                or misleading TPT artifacts. Skipped silently if no TPT is
                found (no ``$TWB_ROOT``, missing job dir, no ``*.tpt``).
            tdload_options: Additional tdload options appended as-is to the command
                (e.g., "-c UTF8", "--TargetInstances 2", "ExportTraceLevel=1").
                This parameter must not be used to pass credentials or other
                sensitive data, since it is exposed on the command line.
            tdload_job_var_file: Path to a user-provided job variable file. When
                provided, skips auto-generation and uses this file directly. The file
                must contain all required job variables including credentials.
            **kwargs: Mode-specific parameters (source_file_name, target_table, etc.).

        Returns:
            Execution result dict.
        """
        if not shutil.which(self._tdload_binary):
            raise TTUNotInstalledError(
                f"tdload binary '{self._tdload_binary}' not found. "
                "Verify TTU_TTU_VERSION matches your installation or set TTU_TDLOAD_BINARY_PATH in .env."
            )

        # Ensure TWB_ROOT is set and logs directory exists
        if not os.environ.get("TWB_ROOT"):
            tdload_path = shutil.which(self._tdload_binary)
            if tdload_path:
                twb_root = str(Path(tdload_path).resolve().parent.parent)
                os.environ["TWB_ROOT"] = twb_root
                logger.info("Auto-detected TWB_ROOT: %s", twb_root)

        twb_root = os.environ.get("TWB_ROOT")
        if twb_root:
            logs_dir = Path(twb_root) / "logs"
            if not logs_dir.exists():
                logs_dir.mkdir(parents=True, exist_ok=True)
                logger.info("Created TTU logs directory: %s", logs_dir)

        valid_modes = {"file_to_table", "table_to_file", "table_to_table"}
        if mode not in valid_modes:
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of: {', '.join(sorted(valid_modes))}"
            )

        self._validate_tdload_params(mode, tdload_job_var_file, **kwargs)

        # Render early so BEARER fails fast with AuthUnsupportedError before
        # any file I/O or env setup happens.
        tdload_rendering = auth.render_for_tdload()

        job_name = kwargs.pop("job_name", f"elt_tdload_{uuid.uuid4().hex[:12]}")
        result: dict[str, Any] = {"success": False, "job_name": job_name, "mode": mode}

        if tdload_job_var_file:
            if not Path(tdload_job_var_file).exists():
                raise FileNotFoundError(f"Job variable file not found: {tdload_job_var_file}")
            cmd = [self._tdload_binary, "-j", tdload_job_var_file]
            skip_rows = kwargs.pop("skip_header_rows", None)
            if skip_rows is not None:
                try:
                    skip_rows_int = int(skip_rows)
                except (TypeError, ValueError):
                    raise ValueError(
                        "skip_header_rows must be a non-negative integer."
                    ) from None
                if skip_rows_int < 0:
                    raise ValueError(
                        "skip_header_rows must be a non-negative integer."
                    )
                if skip_rows_int > 0:
                    cmd.extend(["--SourceSkipRows", str(skip_rows_int)])
            if tdload_options:
                cmd.extend(shlex.split(tdload_options, posix=(platform.system() != "Windows")))
            cmd.append(job_name)
            proc_result = self._run_subprocess(
                cmd,
                env_override=tdload_rendering.env_vars,
                sanitize_secrets=self._auth_secrets(auth),
            )
            result.update(proc_result)
            result["success"] = proc_result["returncode"] == 0
            if save_script:
                result["save_script_ignored"] = True
            if save_tpt_script and result["success"]:
                tpt_path = self._capture_tpt_script(job_name, auth)
                if tpt_path is not None:
                    result["tpt_script_path"] = str(tpt_path)
            return result

        skip_rows = kwargs.pop("skip_header_rows", None)
        job_var_content = self._prepare_tdload_job_var(
            auth, mode, source_mechanism=source_mechanism, **kwargs,
        )

        if save_script:
            result["script_path"] = str(
                self._save_sanitized_script(job_var_content, "tdload_job", ".txt", auth=auth)
            )

        temp_path = self._write_temp_file(job_var_content, prefix="tdload_", suffix=".txt")
        try:
            cmd = [self._tdload_binary, "-j", str(temp_path)]
            if skip_rows is not None:
                try:
                    skip_rows_int = int(skip_rows)
                except (TypeError, ValueError):
                    raise ValueError(
                        "skip_header_rows must be a non-negative integer."
                    ) from None
                if skip_rows_int < 0:
                    raise ValueError(
                        "skip_header_rows must be a non-negative integer."
                    )
                if skip_rows_int > 0:
                    cmd.extend(["--SourceSkipRows", str(skip_rows_int)])
            if tdload_options:
                cmd.extend(shlex.split(tdload_options, posix=(platform.system() != "Windows")))
            cmd.append(job_name)

            proc_result = self._run_subprocess(
                cmd,
                env_override=tdload_rendering.env_vars,
                sanitize_secrets=self._auth_secrets(auth),
                delete_file_after_start=temp_path,
            )
            result.update(proc_result)
            result["success"] = proc_result["returncode"] == 0
        finally:
            self._secure_delete(temp_path)

        if save_tpt_script and result["success"]:
            tpt_path = self._capture_tpt_script(job_name, auth)
            if tpt_path is not None:
                result["tpt_script_path"] = str(tpt_path)

        return result

    def _validate_tdload_params(
        self, mode: str, tdload_job_var_file: str | None, **kwargs: Any
    ) -> None:
        """Validate mandatory parameters per mode before execution."""
        if tdload_job_var_file:
            return

        if mode == "file_to_table":
            if not kwargs.get("source_file_name"):
                raise ValueError("file_to_table requires source_file_name")
            if not kwargs.get("target_table"):
                raise ValueError("file_to_table requires target_table")
        elif mode == "table_to_file":
            if not kwargs.get("source_table") and not kwargs.get("select_stmt"):
                raise ValueError("table_to_file requires source_table or select_stmt")
            if not kwargs.get("target_file_name"):
                raise ValueError("table_to_file requires target_file_name")
        elif mode == "table_to_table":
            if not kwargs.get("source_table") and not kwargs.get("select_stmt"):
                raise ValueError("table_to_table requires source_table or select_stmt")
            if not kwargs.get("target_table"):
                raise ValueError("table_to_table requires target_table")

    def execute_bteq(
        self,
        auth: TeradataAuth,
        script: str,
        timeout: int | None = None,
        save_script: bool = False,
    ) -> dict[str, Any]:
        """Execute a BTEQ script.

        Args:
            auth: Teradata authentication identity for this invocation. The
                BTEQ ``.LOGON`` / ``.CONNECTSTRING`` / ``.LOGDATA`` header is
                rendered from ``auth`` and prepended to ``script``. All five
                mechanisms including BEARER are supported.
            script: BTEQ SQL script to execute.
            timeout: Optional timeout override in seconds.
            save_script: If True, save a sanitized copy of the script.

        Returns:
            Execution result dict.
        """
        if not shutil.which(self._bteq_binary):
            raise TTUNotInstalledError(
                f"BTEQ binary '{self._bteq_binary}' not found. "
                "Verify TTU_TTU_VERSION matches your installation or set TTU_BTEQ_BINARY_PATH in .env."
            )

        full_script = self._prepare_bteq_script(auth, script)

        result: dict[str, Any] = {"success": False}

        if save_script:
            result["script_path"] = str(
                self._save_sanitized_script(full_script, "bteq", ".bteq", auth=auth)
            )

        # BTEQ reads auth from the script itself (.LOGON / .CONNECTSTRING) —
        # no subprocess env vars needed for identity. Pass sanitize_secrets so
        # any error output that echoes credentials gets scrubbed.
        proc_result = self._run_subprocess(
            [self._bteq_binary],
            stdin_data=full_script,
            timeout=timeout,
            sanitize_secrets=self._auth_secrets(auth),
        )
        result.update(proc_result)

        # BTEQ-specific error detection in stdout
        stdout_text = proc_result.get("stdout", "")
        error_indicators = ["*** Failure", "*** Error"]
        detected_errors = [
            line
            for line in stdout_text.splitlines()
            if any(ind in line for ind in error_indicators)
        ]
        if detected_errors:
            result["success"] = False
            result["bteq_errors"] = detected_errors
        else:
            result["success"] = proc_result["returncode"] == 0

        return result

    # ==================== Private Helpers ====================

    def _run_subprocess(
        self,
        cmd: list[str],
        stdin_data: str | None = None,
        timeout: int | None = None,
        delete_file_after_start: Path | None = None,
        env_override: dict[str, str] | None = None,
        sanitize_secrets: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        """Run a subprocess command and capture output.

        Args:
            cmd: Command and args (list, not a shell string).
            stdin_data: Optional string written to the subprocess stdin.
            timeout: Optional per-call timeout override.
            delete_file_after_start: If set, the file is ``_secure_delete``d
                ~2s after subprocess start (lets the child open the job-var
                file before we remove it from disk).
            env_override: Keys to merge into the subprocess env (after
                ``os.environ``). Used by tdload/tbuild to pass CLIv2 identity
                (``TdpId``/``UserName``/``UserPassword``). BTEQ passes ``None``
                because its auth lives in the script itself.
            sanitize_secrets: Values to mask as ``***`` in exception messages.
        """
        effective_timeout = timeout or self._command_timeout

        if cmd and platform.system() == "Windows":
            resolved = shutil.which(cmd[0]) or cmd[0]
            binary_path = Path(resolved)
            engine_exe = binary_path.parent / (binary_path.stem + "exe.exe")
            if engine_exe.exists():
                cmd = [str(engine_exe)] + cmd[1:]

        try:
            # Strip CLIv2 identity keys from the inherited shell env before
            # merging ``env_override`` — otherwise a stale ``UserPassword`` /
            # ``LogonMechData`` / etc. exported in the parent shell would
            # shadow the renderer's output for JWT/SECRET/BEARER (which
            # deliberately omit those keys) and make tdload prompt or reject
            # the logon. ``env_override`` is then the sole source of truth
            # for identity; the renderer fills every key the active mechanism
            # needs, and any key it omits MUST be absent from the subprocess
            # env, not inherited from the parent.
            env = {
                k: v for k, v in os.environ.items() if k not in _TTU_IDENTITY_ENV_KEYS
            }
            if env_override:
                env.update(env_override)
            proc = subprocess.Popen(  # noqa: S603
                cmd,
                stdin=subprocess.PIPE if stdin_data else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )

            if delete_file_after_start:
                import time as _time

                def _delayed_delete() -> None:
                    _time.sleep(2)
                    self._secure_delete(Path(delete_file_after_start))

                threading.Thread(target=_delayed_delete, daemon=True).start()

            stdout_bytes, stderr_bytes = proc.communicate(
                input=stdin_data.encode("utf-8") if stdin_data else None,
                timeout=effective_timeout,
            )

            stdout_text = stdout_bytes.decode("utf-8", errors="replace")
            stderr_text = stderr_bytes.decode("utf-8", errors="replace")

            return {
                "returncode": proc.returncode,
                "stdout": stdout_text,
                "stderr": stderr_text,
            }
        except subprocess.TimeoutExpired as e:
            proc.kill()
            proc.communicate()
            raise TTUCommandError(f"Command timed out after {effective_timeout}s: {cmd[0]}") from e
        except FileNotFoundError as e:
            raise TTUNotInstalledError(f"Binary not found: {cmd[0]}") from e
        except Exception as e:
            # Strip credentials from error messages
            safe_msg = str(e)
            for secret in sanitize_secrets:
                if secret and secret in safe_msg:
                    safe_msg = safe_msg.replace(secret, "***")
            raise TTUCommandError(f"Subprocess failed: {safe_msg}") from e

    @staticmethod
    def _auth_secrets(auth: TeradataAuth) -> tuple[str, ...]:
        """Return the tuple of secret-adjacent strings to scrub from error
        messages for this auth (password, logdata, username, host)."""
        return tuple(
            s for s in (auth.password, auth.logdata, auth.username, auth.host) if s
        )

    def _prepare_tpt_ddl_script(
        self,
        sql_statements: list[str],
        job_name: str,
        error_list: list[int] | None,
    ) -> str:
        """Generate a TPT DDL script from SQL statements."""
        # Clean and escape each SQL statement
        cleaned = [
            stmt.strip().rstrip(";").replace("'", "''")
            for stmt in sql_statements
            if stmt and isinstance(stmt, str) and stmt.strip()
        ]

        if not cleaned:
            raise ValueError("No valid SQL statements found")

        apply_sql = ",\n".join(
            f"            ('{stmt};')" if i > 0 else f"('{stmt};')"
            for i, stmt in enumerate(cleaned)
        )

        if not error_list:
            error_list_stmt = "ErrorList = ['']"
        else:
            error_list_str = ", ".join(f"'{e}'" for e in error_list)
            error_list_stmt = f"ErrorList = [{error_list_str}]"

        # LogonMech / LogonMechData are optional DDL Operator attributes
        # per the TPT Reference (B035-2436 Chapter 4, "DDL Operator
        # Attribute Definitions"). Emitting the @-var refs unconditionally
        # lets tbuild authenticate with any mechanism render_for_tdload
        # supports (TD2/LDAP/JWT/SECRET); BEARER is rejected earlier.
        # For TD2, env populates LogonMech="TD2" and LogonMechData="",
        # which tbuild accepts as the default mechanism.
        return f"""
DEFINE JOB {job_name}
DESCRIPTION 'TPT DDL Operation'
(
APPLY
    {apply_sql}
TO OPERATOR ( $DDL ()
    ATTR
    (
        TdpId = @TdpId,
        UserName = @UserName,
        UserPassword = @UserPassword,
        LogonMech = @LogonMech,
        LogonMechData = @LogonMechData,
        {error_list_stmt}
    )
);
);
"""

    def _prepare_tdload_job_var(
        self,
        auth: TeradataAuth,
        mode: str,
        source_mechanism: str | None = None,
        **kwargs: Any,
    ) -> str:
        """Generate a tdload job variable file.

        The ``auth`` identity is turned into ``Target*`` job-var entries via
        :meth:`TeradataAuth.render_for_tdload`. For cross-instance
        ``table_to_table``, ``Source*`` entries default to the same identity
        unless the caller explicitly passes ``source_host``/
        ``source_username``/``source_password`` kwargs (a TD2/LDAP-only
        override path preserved for the existing tool layer — cross-
        instance non-TD2 source or mixed-mechanism transfer is a future
        enhancement).

        ``source_mechanism`` tells the source-entries helper which
        mechanism the override kwargs represent. For cross-instance
        ``table_to_table`` the tool layer passes ``primary_auth.mechanism``
        (the source auth), so gating the shim works correctly even when
        ``auth`` here is the JWT/SECRET target.
        """
        job_vars: dict[str, str] = {}
        # Target side always comes from auth. BEARER has already been
        # rejected upstream by render_for_tdload in execute_tdload.
        target_entries = auth.render_for_tdload(prefix="Target").job_var_entries
        target_entries["TargetTdpId"] = auth.host

        if mode == "file_to_table":
            target_table = kwargs.get("target_table", "")
            job_vars.update(target_entries)
            job_vars["TargetTable"] = target_table
            job_vars["SourceFileName"] = kwargs.get("source_file_name", "")
            if "." in target_table:
                target_db = target_table.split(".")[0]
                job_vars["TargetWorkingDatabase"] = target_db
            if kwargs.get("insert_stmt"):
                job_vars["InsertStmt"] = kwargs["insert_stmt"]

        elif mode == "table_to_file":
            # Source is the "query side" here — ``auth`` IS the source, so
            # default source_mechanism to auth's mechanism (the pre-existing
            # behavior for this mode).
            source_entries = self._source_entries_for_mode(
                auth, kwargs, source_mechanism=source_mechanism,
            )
            job_vars.update(source_entries)
            job_vars["TargetFileName"] = kwargs.get("target_file_name", "")
            if kwargs.get("source_table"):
                job_vars["SourceTable"] = kwargs["source_table"]
                if "." in kwargs["source_table"]:
                    job_vars["SourceWorkingDatabase"] = kwargs["source_table"].split(".")[0]
            elif kwargs.get("select_stmt"):
                job_vars["SourceSelectStmt"] = kwargs["select_stmt"]

        elif mode == "table_to_table":
            # Cross-instance path: ``auth`` is the target identity; the
            # tool layer passes ``source_mechanism`` matching the source
            # auth (primary_auth.mechanism, validated as TD2/LDAP upstream)
            # so the Source* shim gates on the right mechanism.
            source_entries = self._source_entries_for_mode(
                auth, kwargs, source_mechanism=source_mechanism,
            )
            job_vars.update(source_entries)
            job_vars.update(target_entries)
            # Target host override (TD2-shim only). User/password overrides
            # must NOT clobber the renderer's output for JWT/SECRET — those
            # mechanisms require absent UserPassword; re-introducing one
            # makes tdload prompt on stdin or reject the logon.
            job_vars["TargetTdpId"] = kwargs.get("target_host", auth.host)
            if auth.mechanism in ("TD2", "LDAP"):
                if kwargs.get("target_username"):
                    job_vars["TargetUserName"] = kwargs["target_username"]
                if kwargs.get("target_password"):
                    job_vars["TargetUserPassword"] = kwargs["target_password"]
            job_vars["TargetTable"] = kwargs.get("target_table", "")
            if kwargs.get("source_table"):
                job_vars["SourceTable"] = kwargs["source_table"]
                if "." in kwargs["source_table"]:
                    job_vars["SourceWorkingDatabase"] = kwargs["source_table"].split(".")[0]
            elif kwargs.get("select_stmt"):
                job_vars["SourceSelectStmt"] = kwargs["select_stmt"]
            if kwargs.get("insert_stmt"):
                job_vars["InsertStmt"] = kwargs["insert_stmt"]
            target_table = kwargs.get("target_table", "")
            if "." in target_table:
                job_vars["TargetWorkingDatabase"] = target_table.split(".")[0]

        # Add optional format parameters
        for key, var_name in [
            ("source_format", "SourceFormat"),
            ("target_format", "TargetFormat"),
            ("source_text_delimiter", "SourceTextDelimiter"),
            ("target_text_delimiter", "TargetTextDelimiter"),
        ]:
            if kwargs.get(key):
                job_vars[var_name] = kwargs[key]

        job_vars.setdefault("SourceFormat", "Delimited")
        job_vars.setdefault("TargetFormat", "Delimited")
        job_vars.setdefault("SourceTextDelimiter", ",")
        job_vars.setdefault("TargetTextDelimiter", ",")

        lines = [f"{k}='{str(v).replace(chr(39), chr(39)+chr(39))}'" for k, v in job_vars.items()]
        return ",\n".join(lines)

    def _source_entries_for_mode(
        self,
        auth: TeradataAuth,
        kwargs: dict[str, Any],
        source_mechanism: str | None = None,
    ) -> dict[str, str]:
        """Build ``Source*`` job-var entries for cross-instance modes.

        By default the source identity is the same as ``auth`` (rendered
        via ``auth.render_for_tdload(prefix='Source')``).

        For TD2/LDAP cross-instance, the legacy shim kwargs
        ``source_host`` / ``source_username`` / ``source_password`` may
        override the source side with different credentials.

        ``source_mechanism`` tells the helper *what mechanism the override
        kwargs represent*. This matters in ``table_to_table`` cross-instance
        where ``auth`` is the TARGET identity (potentially JWT/SECRET) but
        the caller-supplied source_* kwargs carry a TD2/LDAP source
        identity. Gating on ``auth.mechanism`` here would be wrong —
        it'd check the target's mechanism when deciding whether to emit
        source kwargs. Falls back to ``auth.mechanism`` when not provided
        (e.g. ``table_to_file`` where auth *is* the source).

        The shim is **only** applied when the source mechanism is TD2/LDAP
        (the shim's wire format). Non-TD2 source with a different target
        mechanism is not expressible on tdload's argv today and should be
        rejected upstream at the tool boundary.
        """
        has_overrides = any(
            k in kwargs for k in ("source_host", "source_username", "source_password")
        )
        effective_source_mech = (source_mechanism or auth.mechanism).upper()
        if has_overrides and effective_source_mech in ("TD2", "LDAP"):
            return {
                "SourceTdpId": kwargs.get("source_host", auth.host),
                "SourceUserName": kwargs.get("source_username", auth.username),
                "SourceUserPassword": kwargs.get("source_password", auth.password),
            }
        entries = auth.render_for_tdload(prefix="Source").job_var_entries
        entries["SourceTdpId"] = auth.host
        return entries

    def _prepare_bteq_script(self, auth: TeradataAuth, script: str) -> str:
        """Build a complete BTEQ script with the auth header + .LOGOFF / .EXIT.

        Auth header lines (``.LOGON``, ``.SET LOGMECH``, ``.CONNECTSTRING``,
        ``.LOGDATA``) come from :meth:`TeradataAuth.render_for_bteq`. All
        five mechanisms supported including BEARER.
        """
        lines = list(auth.render_for_bteq())
        stripped = script.strip()
        if stripped and not stripped.endswith(";"):
            stripped += ";"
        lines.append(stripped)
        lines.append(".LOGOFF")
        lines.append(".EXIT")
        return "\n".join(lines)

    @staticmethod
    def _sanitize_script(script: str, auth: TeradataAuth) -> str:
        """Replace credentials from ``auth`` with placeholders in a script."""
        sanitized = script
        if auth.password:
            sanitized = sanitized.replace(auth.password, "<PASSWORD>")
        if auth.logdata:
            sanitized = sanitized.replace(auth.logdata, "<LOGDATA>")
        if auth.username:
            sanitized = sanitized.replace(auth.username, "<USERNAME>")
        if auth.host:
            sanitized = sanitized.replace(auth.host, "<HOST>")
        return sanitized

    def _save_sanitized_script(
        self, script: str, prefix: str, suffix: str, auth: TeradataAuth
    ) -> Path:
        """Save a sanitized copy of a script to scripts_dir."""
        self._scripts_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}{suffix}"
        path = self._scripts_dir / filename
        sanitized = self._sanitize_script(script, auth)
        with self._file_lock:
            path.write_text(sanitized, encoding="utf-8")
        logger.info("Saved sanitized script to %s", path)
        return path

    def _capture_tpt_script(
        self, job_name: str, auth: TeradataAuth
    ) -> Path | None:
        """Locate the TPT script tdload generated under
        ``$TWB_ROOT/jobs/<job_name>/`` and copy a sanitized version into
        ``scripts_dir``. Returns the destination path or ``None`` if no
        TPT is found (no ``TWB_ROOT``, missing job dir, no ``*.tpt``,
        or read failure). Never raises — capture is best-effort.
        """
        # Sanitize job_name to prevent directory traversal.
        # Reject '.', '..', and any name with non-alphanumeric/underscore/hyphen chars.
        _safe_job_name = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]*$")
        safe_name = Path(job_name).name
        if not safe_name or safe_name != job_name or not _safe_job_name.match(safe_name):
            logger.warning(
                "save_tpt_script: rejecting unsafe job_name %r", job_name
            )
            return None
        twb_root = os.environ.get("TWB_ROOT")
        if not twb_root:
            logger.debug(
                "save_tpt_script requested but TWB_ROOT is not set; "
                "skipping capture for job %s",
                safe_name,
            )
            return None
        job_dir = Path(twb_root) / "jobs" / safe_name
        if not job_dir.is_dir():
            logger.debug(
                "save_tpt_script: TWB job dir %s does not exist", job_dir
            )
            return None
        candidates = sorted(job_dir.glob("*.tpt"))
        if not candidates:
            logger.debug(
                "save_tpt_script: no *.tpt files under %s", job_dir
            )
            return None
        # Prefer the expected filename; fall back to newest by mtime
        expected = job_dir / f"{safe_name}.tpt"
        if expected in candidates:
            src = expected
        else:
            try:
                src = max(candidates, key=lambda p: p.stat().st_mtime)
            except OSError as e:
                logger.warning(
                    "save_tpt_script: could not stat candidates in %s: %s",
                    job_dir,
                    e,
                )
                return None
        try:
            body = src.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            logger.warning(
                "save_tpt_script: could not read %s: %s", src, e
            )
            return None
        # tdload-generated TPT scripts typically don't embed credentials
        # (those flow through the job-var file via env), but defense in
        # depth — run them through the same sanitizer as ``save_script``.
        sanitized = self._sanitize_script(body, auth)
        try:
            self._scripts_dir.mkdir(parents=True, exist_ok=True)
            dest = self._scripts_dir / f"{safe_name}.tpt"
            with self._file_lock:
                dest.write_text(sanitized, encoding="utf-8")
        except OSError as e:
            logger.warning(
                "save_tpt_script: could not write to %s: %s",
                self._scripts_dir,
                e,
            )
            return None
        logger.info("Captured tdload-generated TPT script to %s", dest)
        return dest

    def _write_temp_file(
        self,
        content: str,
        prefix: str = "ttu_",
        suffix: str = ".tmp",
    ) -> Path:
        """Write content to a temp file with restrictive permissions."""
        with self._file_lock:
            fd, tmp_path = tempfile.mkstemp(prefix=prefix, suffix=suffix)
            try:
                os.write(fd, content.encode("utf-8"))
            finally:
                os.close(fd)

            path = Path(tmp_path)
            self._set_file_permissions(path)
            return path

    @staticmethod
    def _set_file_permissions(path: Path) -> None:
        """Set restrictive file permissions (skip on Windows)."""
        if platform.system() != "Windows":
            try:
                os.chmod(path, 0o400)
            except OSError:
                logger.warning("Could not set permissions on %s", path)

    @staticmethod
    def _secure_delete(path: Path) -> None:
        """Securely delete a file using shred if available, else os.remove."""
        try:
            if platform.system() != "Windows" and shutil.which("shred"):
                try:
                    os.chmod(path, 0o600)
                except OSError:
                    logger.debug("Could not relax permissions on %s before shredding", path)
                subprocess.run(  # noqa: S603, S607
                    ["shred", "--force", "--remove", str(path)],
                    check=False,
                    capture_output=True,
                )
                if path.exists():
                    os.remove(path)
            else:
                if path.exists():
                    os.remove(path)
        except OSError as e:
            logger.warning("Could not delete temp file %s: %s", path, e)
