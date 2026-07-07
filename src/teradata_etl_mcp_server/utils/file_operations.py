"""Safe file operations utilities for enterprise-grade file handling.

This module provides utilities for safe file writing with:
- Automatic backups before overwrite
- Atomic write operations (temp → rename)
- Python syntax validation for .py files
- Version management and cleanup
- Collision detection and warnings
"""

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..response_sanitizer import safe_error_message

logger = logging.getLogger(__name__)


class FileOperationError(Exception):
    """Base exception for file operation errors."""

    pass


class UnsafePathError(ValueError):
    """Raised when a caller-supplied path escapes its containment base."""

    pass


def safe_join_within(base: Path, user_path: Path | str) -> Path:
    """Resolve ``user_path`` against ``base`` and reject any path that escapes
    ``base`` via ``..``, absolute paths, null bytes, or symlink targets.

    Use this when building a write target from caller-supplied filename.

    ``base`` is resolved internally, so callers may pass either a relative or
    already-resolved path without affecting containment correctness.

    Args:
        base: Trust-boundary directory.
        user_path: Relative path supplied by a caller.

    Returns:
        The resolved absolute path, guaranteed to be under ``base``.

    Raises:
        UnsafePathError: If ``user_path`` escapes ``base`` or contains a null byte.
    """
    if "\x00" in str(user_path):
        raise UnsafePathError("path contains null byte")
    base = Path(base).resolve()
    target = (base / Path(user_path)).resolve()
    if not target.is_relative_to(base):
        raise UnsafePathError(
            f"path {str(user_path)!r} resolves outside base {str(base)!r}"
        )
    return target


def safe_path_under_any_root(user_path: Path | str, allowed_roots: list[Path]) -> Path:
    """Validate that ``user_path`` resolves under at least one of ``allowed_roots``.

    Unlike :func:`safe_join_within`, this validates an externally-specified
    (possibly absolute) path against a set of allowed roots rather than joining
    a relative path to a single base. Use this for caller-supplied absolute
    paths that must stay within a trust boundary — e.g. reading a user-specified
    CSV from the working directory tree.

    Args:
        user_path: Path supplied by a caller (may be absolute).
        allowed_roots: List of directories any of which is an acceptable ancestor.

    Returns:
        The resolved absolute path.

    Raises:
        UnsafePathError: If ``user_path`` is outside every ``allowed_roots`` entry
            or contains a null byte.
    """
    if "\x00" in str(user_path):
        raise UnsafePathError("path contains null byte")
    target = Path(user_path).resolve()
    resolved_roots = [r.resolve() for r in allowed_roots]
    if not any(target.is_relative_to(r) for r in resolved_roots):
        raise UnsafePathError(
            f"path {str(user_path)!r} outside allowed roots "
            f"{[str(r) for r in resolved_roots]}"
        )
    return target


class SafeFileWriter:
    """
    Enterprise-grade safe file writer with backup and validation.

    Features:
    - Automatic backups before overwriting existing files (opt-out via enable_backups)
    - Atomic write operations using temp files
    - Python syntax validation for .py files
    - Version management (keep last N backups)
    - Restrictive POSIX permissions (opt-out via restrict_permissions)
    - Path containment via safe_join_within
    - Detailed operation logging
    """

    def __init__(
        self,
        output_dir: Path,
        backup_dir: Path | None = None,
        keep_backups: int = 5,
        validate_python: bool = True,
        enable_backups: bool = True,
        restrict_permissions: bool = True,
    ):
        """
        Initialize safe file writer.

        Args:
            output_dir: Primary output directory
            backup_dir: Backup directory (defaults to output_dir/.backups)
            keep_backups: Number of backup versions to retain
            validate_python: Validate Python syntax before writing .py files
            enable_backups: If False, do not create a .backups/ directory and do
                not create timestamped backups on overwrite. Use for generators
                whose outputs live in user-visible directories (dbt projects).
            restrict_permissions: If False, skip POSIX ``chmod 0o640`` after
                write and leave files with umask-default permissions. Use for
                files that must be readable by users/processes other than the
                writing user (dbt files read by Airflow running as a different
                user).
        """
        self.output_dir = Path(output_dir)
        self.enable_backups = enable_backups
        self.restrict_permissions = restrict_permissions
        self.backup_dir = Path(backup_dir) if backup_dir else self.output_dir / ".backups"
        self.keep_backups = keep_backups
        self.validate_python = validate_python

        # Ensure directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.enable_backups:
            self.backup_dir.mkdir(parents=True, exist_ok=True)

    def write_file_safe(
        self,
        content: str,
        filename: str,
        force: bool = False,
    ) -> tuple[Path, dict[str, Any]]:
        """
        Write file with backup, validation, and atomic operations.

        Process:
        1. Validate content (if Python file)
        2. Backup existing file (if exists)
        3. Write to temp file
        4. Atomic rename to target
        5. Cleanup old backups

        Args:
            content: File content to write
            filename: Target filename (relative to output_dir)
            force: Skip validation if True

        Returns:
            Tuple of (written_path, metadata_dict)

        Raises:
            FileOperationError: If validation or write fails
        """
        try:
            try:
                target_path = safe_join_within(self.output_dir.resolve(), filename)
            except UnsafePathError as e:
                raise FileOperationError(f"Filename {filename!r}: {e}") from e

            # Auto-create nested parent directories inside output_dir.
            # Safe because target_path is validated as being within output_dir.
            target_path.parent.mkdir(parents=True, exist_ok=True)

            metadata = {
                "filename": filename,
                "target_path": str(target_path),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "file_existed": target_path.exists(),
                "backup_created": False,
                "backup_path": None,
                "validated": False,
                "warnings": [],
            }
            # Step 1: Validate content
            if self.validate_python and filename.endswith(".py") and not force:
                is_valid, validation_msg = self._validate_python_syntax(content, filename)
                if not is_valid:
                    raise FileOperationError(f"Python syntax validation failed: {validation_msg}")
                metadata["validated"] = True
                logger.debug("Validated Python syntax for %s", filename)

            # Step 2: Backup existing file
            if self.enable_backups and target_path.exists():
                backup_path = self._create_backup(target_path, filename)
                metadata["backup_created"] = True
                metadata["backup_path"] = str(backup_path)
                metadata["warnings"].append(f"Overwrote existing file (backup: {backup_path.name})")
                logger.info("Created backup: %s", backup_path)

            # Step 3: Atomic write (temp → rename)
            self._write_atomic(content, target_path)

            # Step 4: Cleanup old backups
            if self.enable_backups and metadata["backup_created"]:
                self._cleanup_old_backups(filename)

            logger.info("Successfully wrote file: %s", target_path)
            return target_path, metadata

        except FileOperationError:
            raise
        except Exception as e:
            logger.error("Failed to write file %s: %s", filename, e, exc_info=True)
            raise FileOperationError(f"File write failed: {safe_error_message(e)}") from e

    def _validate_python_syntax(self, content: str, filename: str) -> tuple[bool, str]:
        """
        Validate Python code syntax.

        Returns:
            Tuple of (is_valid, message)
        """
        try:
            compile(content, filename, "exec")
            return True, "Valid Python syntax"
        except SyntaxError as e:
            return False, f"Line {e.lineno}: {e.msg}"
        except Exception as e:
            return False, f"Validation error: {safe_error_message(e)}"

    def _validated_relative_filename(self, filename: str) -> Path:
        """Validate ``filename`` is a safe relative path under ``output_dir``
        and return its **canonical** relative form.

        Callers pass ``filename`` to ``write_file_safe`` / ``get_backup_history``
        / ``restore_from_backup``; the lookup methods previously took it at
        face value, which made ``_backup_subdir_for`` derivable from a
        traversal string like ``'../../etc/passwd'`` and let glob/read
        escape ``.backups/``. Running the filename through
        :func:`safe_join_within` reuses the existing trust-boundary check
        (rejects absolute paths, ``..`` segments, null bytes, symlink
        escapes) before any backup I/O happens.

        Non-canonical but in-tree relative paths (e.g. ``a/../x.yml``) are
        accepted by :func:`safe_join_within` because they resolve inside
        ``output_dir``, but returning them verbatim would make
        ``_backup_subdir_for`` mirror an unrelated directory structure —
        two logical writes to the same file would get different backup
        subdirs, breaking retention. We canonicalise by deriving the
        relative form from the resolved absolute target.
        """
        try:
            resolved = safe_join_within(self.output_dir.resolve(), filename)
        except UnsafePathError as e:
            raise FileOperationError(f"Filename {filename!r}: {e}") from e
        try:
            return resolved.relative_to(self.output_dir.resolve())
        except ValueError as e:
            raise FileOperationError(
                f"Filename {filename!r}: resolved path is not under "
                f"output_dir ({self.output_dir.resolve()})."
            ) from e

    def _backup_subdir_for(self, filename: str) -> Path:
        """Return the per-file backup directory, mirroring nested output paths.

        ``write_file_safe`` supports nested filenames like
        ``models/staging/users.yml``. Keeping backups in a single flat
        ``.backups/`` directory would collide any two nested files with the
        same basename — they'd share backup filenames, clobber each other's
        retention history, and make ``restore_from_backup`` ambiguous.
        Mirror the relative parent path under ``.backups/`` so each logical
        file has its own backup folder.

        ``filename`` is validated up front via
        :meth:`_validated_relative_filename` so lookup paths cannot escape
        the backup directory. The final subdir is additionally containment-
        checked against ``self.backup_dir`` as belt-and-suspenders.
        """
        safe_filename = self._validated_relative_filename(filename)
        parent = safe_filename.parent
        subdir = (
            self.backup_dir
            if str(parent) in ("", ".")
            else self.backup_dir / parent
        )
        resolved_subdir = subdir.resolve()
        resolved_backup_root = self.backup_dir.resolve()
        if not resolved_subdir.is_relative_to(resolved_backup_root):
            raise FileOperationError(
                f"Backup subdir for {filename!r} escapes {resolved_backup_root}"
            )
        return subdir

    def _create_backup(self, file_path: Path, filename: str) -> Path:
        """
        Create timestamped backup of existing file.

        Args:
            file_path: Resolved on-disk path of the file being overwritten.
            filename: The *relative* filename the caller passed to
                ``write_file_safe`` — used to derive a per-file backup
                subdirectory so nested paths don't collide in ``.backups/``.

        Returns:
            Path to backup file
        """
        import uuid

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        backup_filename = f"{file_path.stem}_{timestamp}_{uuid.uuid4().hex[:6]}{file_path.suffix}"
        subdir = self._backup_subdir_for(filename)
        subdir.mkdir(parents=True, exist_ok=True)
        backup_path = subdir / backup_filename

        # Copy with metadata preservation
        shutil.copy2(file_path, backup_path)
        return backup_path

    def _write_atomic(self, content: str, target_path: Path) -> None:
        """
        Write file atomically using temp file and rename.

        This ensures either complete write or no change (no partial files).
        """
        import os
        import sys
        import uuid

        temp_path = target_path.with_suffix(f"{target_path.suffix}.{uuid.uuid4().hex[:8]}.tmp")

        try:
            # Write to temp file
            temp_path.write_text(content, encoding="utf-8", newline="\n")

            # Atomic rename (POSIX and Windows)
            temp_path.replace(target_path)

            # M7: Restrict file permissions on POSIX (owner rw, group r, others none).
            # Gated on self.restrict_permissions so callers writing files that must be
            # readable by other users/processes (e.g. dbt files read by Airflow as a
            # different user) can opt out and keep umask-default permissions.
            # chmod failure is non-fatal since file is already written successfully.
            if sys.platform != "win32" and self.restrict_permissions:
                try:
                    os.chmod(target_path, 0o640)
                except OSError as chmod_err:
                    logger.warning("Could not set file permissions on %s: %s", target_path, chmod_err)
        except Exception as e:
            # Cleanup temp file on failure
            if temp_path.exists():
                temp_path.unlink()
            raise FileOperationError(f"Atomic write failed: {safe_error_message(e)}") from e

    def read_file_safe(self, filename: str) -> str | None:
        """
        Read a file under ``output_dir`` with path containment enforced.

        Args:
            filename: Target filename (relative to output_dir)

        Returns:
            File content as a string, or ``None`` if the file does not exist.

        Raises:
            FileOperationError: If ``filename`` resolves outside ``output_dir``.
        """
        try:
            target_path = safe_join_within(self.output_dir.resolve(), filename)
        except UnsafePathError as e:
            raise FileOperationError(f"Filename {filename!r}: {e}") from e
        if not target_path.exists():
            return None
        return target_path.read_text(encoding="utf-8")

    def _cleanup_old_backups(self, filename: str) -> None:
        """
        Remove old backup files, keeping only last N versions.

        Scoped to the per-file backup subdirectory — keeps retention
        per logical file, not per basename across the whole output tree.
        """
        import glob as _glob

        base_name = Path(filename).stem
        suffix = Path(filename).suffix
        subdir = self._backup_subdir_for(filename)
        if not subdir.exists():
            return

        pattern = f"{_glob.escape(base_name)}_*{_glob.escape(suffix)}"
        backups = sorted(
            (p for p in subdir.glob(pattern) if p.is_file() and p.suffix == suffix),
            reverse=True,
        )

        # Remove old backups beyond keep_backups limit
        for old_backup in backups[self.keep_backups :]:
            try:
                old_backup.unlink()
                logger.debug("Removed old backup: %s", old_backup.name)
            except Exception as e:
                logger.warning("Failed to remove old backup %s: %s", old_backup, e, exc_info=True)

    def get_backup_history(self, filename: str) -> list[dict[str, Any]]:
        """
        Get backup history for a specific file.

        Scoped to the per-file backup subdirectory — two nested files with
        the same basename have independent histories.

        Returns:
            List of backup metadata dictionaries
        """
        import glob as _glob

        base_name = Path(filename).stem
        suffix = Path(filename).suffix
        subdir = self._backup_subdir_for(filename)
        if not subdir.exists():
            return []
        pattern = f"{_glob.escape(base_name)}_*{_glob.escape(suffix)}"

        backups = []
        for backup_path in sorted(
            (p for p in subdir.glob(pattern) if p.is_file() and p.suffix == suffix),
            reverse=True,
        ):
            stat = backup_path.stat()
            backups.append(
                {
                    "filename": backup_path.name,
                    "path": str(backup_path),
                    "size_bytes": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                }
            )

        return backups

    def restore_from_backup(self, filename: str, backup_timestamp: str | None = None) -> Path:
        """
        Restore file from backup.

        Args:
            filename: Original filename to restore
            backup_timestamp: Backup timestamp prefix (e.g. YYYYMMDD_HHMMSS or
                            YYYYMMDD_HHMMSS_ffffff). Matches the newest backup
                            whose name starts with the given prefix.
                            If None, restores most recent backup.

        Returns:
            Path to restored file

        Raises:
            FileOperationError: If backup not found or restore fails
        """
        import glob as _glob

        try:
            base_name = Path(filename).stem
            suffix = Path(filename).suffix
            subdir = self._backup_subdir_for(filename)
            if not subdir.exists():
                raise FileOperationError(f"No backups found for {filename}")

            if backup_timestamp:
                import re as _re

                if not _re.fullmatch(r"[0-9_]+", backup_timestamp):
                    raise FileOperationError(
                        f"Invalid backup_timestamp format: '{backup_timestamp}' (only digits and underscores allowed)"
                    )
                safe_prefix = _glob.escape(f"{base_name}_{backup_timestamp}")
                pattern = f"{safe_prefix}*{_glob.escape(suffix)}"
                matches = sorted(
                    (p for p in subdir.glob(pattern) if p.is_file() and p.suffix == suffix),
                    reverse=True,
                )
                if not matches:
                    raise FileOperationError(
                        f"No backup found matching timestamp '{backup_timestamp}' for {filename}"
                    )
                backup_path = matches[0]
            else:
                pattern = f"{_glob.escape(base_name)}_*{_glob.escape(suffix)}"
                backups = sorted(
                    (p for p in subdir.glob(pattern) if p.is_file() and p.suffix == suffix),
                    reverse=True,
                )
                if not backups:
                    raise FileOperationError(f"No backups found for {filename}")
                backup_path = backups[0]

            try:
                target_path = safe_join_within(self.output_dir.resolve(), filename)
            except UnsafePathError as e:
                raise FileOperationError(f"Filename {filename!r}: {e}") from e

            # Create backup of current file before restoring — but only if
            # this writer was constructed with backups enabled. Outputs that
            # explicitly opted out (e.g. an Airflow ``dags/`` folder using
            # ``SafeFileWriter(enable_backups=False)``) must not have a
            # stealth ``.backups/`` directory created behind their back.
            if target_path.exists() and self.enable_backups:
                current_backup = self._create_backup(target_path, filename)
                logger.info("Backed up current version before restore: %s", current_backup)

            # Restore from backup
            shutil.copy2(backup_path, target_path)
            logger.info("Restored %s from %s", filename, backup_path.name)
            return target_path
        except Exception as e:
            raise FileOperationError(f"Restore failed: {safe_error_message(e)}") from e


def cleanup_directory(
    directory: Path,
    pattern: str = "*.py",
    older_than_days: int | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """
    Clean up files in a directory based on age or pattern.

    Args:
        directory: Directory to clean
        pattern: Glob pattern for files to remove
        older_than_days: Remove files older than N days (None = all matching)
        dry_run: If True, only report what would be deleted

    Returns:
        Dictionary with cleanup results
    """
    from datetime import timedelta

    directory = Path(directory)
    if not directory.exists():
        return {"success": False, "error": "Directory not found"}

    files_to_remove = []
    total_size = 0
    cutoff_time = None

    if older_than_days is not None:
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=older_than_days)

    # Find matching files
    for file_path in directory.glob(pattern):
        if not file_path.is_file():
            continue

        # Check age filter
        if cutoff_time:
            file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
            if file_mtime > cutoff_time:
                continue

        file_size = file_path.stat().st_size
        files_to_remove.append(
            {
                "path": str(file_path),
                "name": file_path.name,
                "size_bytes": file_size,
            }
        )
        total_size += file_size

    # Remove files if not dry run
    removed_count = 0
    if not dry_run:
        for file_info in files_to_remove:
            try:
                Path(file_info["path"]).unlink()
                removed_count += 1
                logger.info("Removed: %s", file_info["name"])
            except Exception as e:
                logger.error("Failed to remove %s: %s", file_info["name"], e, exc_info=True)

    return {
        "success": True,
        "dry_run": dry_run,
        "files_found": len(files_to_remove),
        "files_removed": removed_count,
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "files": files_to_remove,
    }
