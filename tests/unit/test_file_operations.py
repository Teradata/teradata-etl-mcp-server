"""Unit tests for utils.file_operations.

Covers:
- safe_join_within (path-join containment)
- safe_path_under_any_root (absolute-path trust-boundary)
- SafeFileWriter new flags (enable_backups, restrict_permissions)
- SafeFileWriter.write_file_safe nested parent auto-creation
- SafeFileWriter.read_file_safe (new helper)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from elt_mcp_server.utils.file_operations import (
    FileOperationError,
    SafeFileWriter,
    UnsafePathError,
    safe_join_within,
    safe_path_under_any_root,
)

# ---------------------------------------------------------------------------
# safe_join_within
# ---------------------------------------------------------------------------


class TestSafeJoinWithin:
    """Tests for the safe_join_within helper."""

    def test_rejects_parent_traversal(self, tmp_path: Path):
        with pytest.raises(UnsafePathError, match="resolves outside"):
            safe_join_within(tmp_path.resolve(), "../../etc/passwd")

    def test_rejects_dot_dot_in_middle(self, tmp_path: Path):
        with pytest.raises(UnsafePathError, match="resolves outside"):
            safe_join_within(tmp_path.resolve(), "models/../../escape.yml")

    def test_rejects_absolute_posix(self, tmp_path: Path):
        with pytest.raises(UnsafePathError, match="resolves outside"):
            safe_join_within(tmp_path.resolve(), "/etc/passwd")

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific path")
    def test_rejects_absolute_windows(self, tmp_path: Path):
        with pytest.raises(UnsafePathError, match="resolves outside"):
            safe_join_within(tmp_path.resolve(), r"C:\Windows\system.yml")

    def test_rejects_null_byte(self, tmp_path: Path):
        with pytest.raises(UnsafePathError, match="null byte"):
            safe_join_within(tmp_path.resolve(), "models/x\x00.yml")

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only symlink test")
    def test_rejects_symlink_escape(self, tmp_path: Path):
        outside = tmp_path / "outside"
        outside.mkdir()
        base = tmp_path / "base"
        base.mkdir()
        (base / "link").symlink_to(outside, target_is_directory=True)
        with pytest.raises(UnsafePathError, match="resolves outside"):
            safe_join_within(base.resolve(), "link/x.yml")

    def test_accepts_nested_relative(self, tmp_path: Path):
        result = safe_join_within(tmp_path.resolve(), "models/staging/foo/bar.sql")
        assert result.is_relative_to(tmp_path.resolve())
        assert result.name == "bar.sql"

    def test_accepts_filename_only(self, tmp_path: Path):
        result = safe_join_within(tmp_path.resolve(), "schema.yml")
        assert result == tmp_path.resolve() / "schema.yml"

    def test_accepts_path_with_nonexistent_parents(self, tmp_path: Path):
        # Parent dirs don't need to exist at validation time
        result = safe_join_within(tmp_path.resolve(), "models/new_subdir/file.yml")
        assert result.is_relative_to(tmp_path.resolve())
        assert not result.parent.exists()

    def test_unresolved_base_is_normalized_internally(self, tmp_path: Path, monkeypatch):
        """safe_join_within must resolve ``base`` internally so callers can pass
        a relative path (e.g. ``Path('.')``) without breaking containment.
        Regression guard for the footgun where a relative base would make every
        ``is_relative_to`` check false-negative after a chdir."""
        subdir = tmp_path / "project"
        subdir.mkdir()
        monkeypatch.chdir(subdir)

        # Relative base — without defensive resolve() inside safe_join_within,
        # `(Path('.') / 'x.yml').resolve()` would not be_relative_to Path('.').
        result = safe_join_within(Path("."), "models/foo.sql")
        assert result.is_relative_to(subdir.resolve())
        assert result.name == "foo.sql"

        # Relative base still rejects traversal.
        with pytest.raises(UnsafePathError, match="resolves outside"):
            safe_join_within(Path("."), "../escape.yml")


# ---------------------------------------------------------------------------
# safe_path_under_any_root
# ---------------------------------------------------------------------------


class TestSafePathUnderAnyRoot:
    """Tests for the safe_path_under_any_root helper."""

    def test_rejects_path_outside_all_roots(self, tmp_path: Path):
        # /etc/passwd is not under tmp_path or its parent
        with pytest.raises(UnsafePathError, match="outside allowed roots"):
            safe_path_under_any_root(
                "/etc/passwd" if sys.platform != "win32" else r"C:\Windows\system.ini",
                [tmp_path],
            )

    def test_accepts_path_inside_a_root(self, tmp_path: Path):
        target = tmp_path / "file.csv"
        target.touch()
        result = safe_path_under_any_root(str(target), [tmp_path])
        assert result == target.resolve()

    def test_accepts_path_inside_a_secondary_root(self, tmp_path: Path):
        primary = tmp_path / "primary"
        primary.mkdir()
        secondary = tmp_path / "secondary"
        secondary.mkdir()
        target = secondary / "file.csv"
        target.touch()
        result = safe_path_under_any_root(str(target), [primary, secondary])
        assert result == target.resolve()

    def test_rejects_null_byte(self, tmp_path: Path):
        with pytest.raises(UnsafePathError, match="null byte"):
            safe_path_under_any_root("x\x00.csv", [tmp_path])

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only symlink test")
    def test_rejects_symlink_escape(self, tmp_path: Path):
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "secret.txt").write_text("secret")
        base = tmp_path / "base"
        base.mkdir()
        (base / "link").symlink_to(outside / "secret.txt")
        with pytest.raises(UnsafePathError, match="outside allowed roots"):
            safe_path_under_any_root(str(base / "link"), [base])


# ---------------------------------------------------------------------------
# SafeFileWriter flags
# ---------------------------------------------------------------------------


class TestSafeFileWriterFlags:
    """Tests for the new SafeFileWriter opt-out flags."""

    def test_default_flags_create_backups_and_backup_dir(self, tmp_path: Path):
        writer = SafeFileWriter(tmp_path, validate_python=False)
        writer.write_file_safe("first", "x.yml")
        writer.write_file_safe("second", "x.yml")
        assert (tmp_path / ".backups").exists()
        backups = list((tmp_path / ".backups").glob("x_*.yml"))
        assert len(backups) == 1

    @pytest.mark.parametrize(
        "bad_filename",
        [
            "../../etc/passwd",      # parent traversal
            "a/../../escape.yml",    # middle .. escape
            "x\x00.yml",             # null byte
        ],
    )
    def test_backup_lookup_rejects_path_traversal(
        self, tmp_path: Path, bad_filename: str
    ):
        """``get_backup_history`` / ``restore_from_backup`` and the backup
        subdirectory resolver must reject filenames that escape the output
        tree. Without validation, a traversal filename could make glob/read
        operate outside ``.backups/``.

        Regression guard for the Copilot-flagged bug.
        """
        writer = SafeFileWriter(tmp_path, validate_python=False)
        # Subdir resolver rejects at the root.
        with pytest.raises(FileOperationError):
            writer._backup_subdir_for(bad_filename)
        # Public API surfaces the same rejection — get_backup_history
        # doesn't silently glob outside .backups/.
        with pytest.raises(FileOperationError):
            writer.get_backup_history(bad_filename)
        with pytest.raises(FileOperationError):
            writer.restore_from_backup(bad_filename)

    def test_nested_paths_with_same_basename_do_not_collide_in_backups(
        self, tmp_path: Path
    ):
        """Two files with the same basename under different nested paths
        must get independent backup histories — otherwise retention
        (``keep_backups``) and ``restore_from_backup`` misidentify files.

        Regression guard for the Copilot-flagged bug.
        """
        writer = SafeFileWriter(tmp_path, validate_python=False)
        # Write twice to each nested location (first write has nothing to
        # back up; second write creates the backup of the first).
        writer.write_file_safe("a1", "models/staging/users.yml")
        writer.write_file_safe("a2", "models/staging/users.yml")
        writer.write_file_safe("b1", "models/marts/users.yml")
        writer.write_file_safe("b2", "models/marts/users.yml")

        staging_backups = list(
            (tmp_path / ".backups" / "models" / "staging").glob("users_*.yml")
        )
        marts_backups = list(
            (tmp_path / ".backups" / "models" / "marts").glob("users_*.yml")
        )
        assert len(staging_backups) == 1, staging_backups
        assert len(marts_backups) == 1, marts_backups

        # Content of each backup reflects the FIRST write in its own tree,
        # not the other tree — proves the histories are independent.
        assert staging_backups[0].read_text() == "a1"
        assert marts_backups[0].read_text() == "b1"

        # Each file's history shows only its own backup.
        staging_history = writer.get_backup_history("models/staging/users.yml")
        marts_history = writer.get_backup_history("models/marts/users.yml")
        assert len(staging_history) == 1
        assert len(marts_history) == 1

        # Restore picks the right ancestor for each file.
        restored_staging = writer.restore_from_backup("models/staging/users.yml")
        assert restored_staging.read_text() == "a1"
        restored_marts = writer.restore_from_backup("models/marts/users.yml")
        assert restored_marts.read_text() == "b1"

    def test_non_canonical_paths_canonicalise_to_same_backup_subdir(
        self, tmp_path: Path
    ):
        """Two writes whose filenames resolve to the same on-disk file
        must share a single backup subdirectory — even if the caller
        supplies non-canonical paths like ``models/staging/../marts/x.yml``.
        Without canonicalisation, ``_backup_subdir_for`` would mirror the
        literal (non-normalised) parent and create a second backup tree,
        breaking retention and ``restore_from_backup``.

        Regression guard for the Copilot-flagged bug on
        ``_validated_relative_filename`` preserving ``..`` segments.
        """
        writer = SafeFileWriter(tmp_path, validate_python=False)
        # First write: canonical nested form, two writes to create a backup.
        writer.write_file_safe("v1", "models/marts/users.yml")
        # Second write uses a non-canonical path that resolves to the same
        # target. The backup history must stay coherent.
        writer.write_file_safe("v2", "models/staging/../marts/users.yml")
        writer.write_file_safe("v3", "models/marts/users.yml")

        # Exactly ONE backup tree exists, at the canonical location.
        marts_subdir = tmp_path / ".backups" / "models" / "marts"
        assert marts_subdir.exists(), (
            f"canonical backup subdir missing: {list((tmp_path / '.backups').rglob('*'))}"
        )
        # No stray subdir mirroring the non-canonical input.
        staging_subdir = tmp_path / ".backups" / "models" / "staging"
        assert not staging_subdir.exists(), (
            f"non-canonical backup subdir leaked: {list(staging_subdir.rglob('*'))}"
        )

        # Both canonical and non-canonical path queries yield the same history.
        history_a = writer.get_backup_history("models/marts/users.yml")
        history_b = writer.get_backup_history(
            "models/staging/../marts/users.yml"
        )
        assert len(history_a) == len(history_b) == 2

    def test_enable_backups_false_skips_backup_dir(self, tmp_path: Path):
        writer = SafeFileWriter(
            tmp_path, enable_backups=False, validate_python=False
        )
        writer.write_file_safe("first", "x.yml")
        writer.write_file_safe("second", "x.yml")
        assert not (tmp_path / ".backups").exists()
        # The file should still be overwritten
        assert (tmp_path / "x.yml").read_text() == "second"

    def test_restore_from_backup_honors_enable_backups_false(self, tmp_path: Path):
        """When ``enable_backups=False``, ``restore_from_backup`` must NOT
        create a stealth ``.backups/`` directory by backing up the current
        file before restoring. The backup-pre-restore step is itself a
        backup; opting out of backups means opting out of that step too.

        Regression for Copilot-flagged bug: outputs that explicitly opted
        out of backups (e.g. Airflow ``dags/`` folders) were getting a
        surprise ``.backups/`` directory the first time
        ``restore_from_backup`` was called.
        """
        # First, populate with a backups-enabled writer so a backup file
        # actually exists to restore from.
        seed_writer = SafeFileWriter(
            tmp_path, enable_backups=True, validate_python=False
        )
        seed_writer.write_file_safe("v1", "x.yml")
        seed_writer.write_file_safe("v2", "x.yml")  # creates a backup of v1
        # Sanity: the seed run produced exactly one backup of x.yml.
        backups_dir = tmp_path / ".backups"
        assert backups_dir.exists()
        # Now snapshot the backups directory contents so we can compare later.
        before = sorted(p.name for p in backups_dir.rglob("*") if p.is_file())

        # Switch to a no-backups writer and restore. The restore itself is
        # legitimate (the backup was created earlier); what must NOT happen
        # is a NEW backup of the current "v2" file before the restore.
        no_backup_writer = SafeFileWriter(
            tmp_path, enable_backups=False, validate_python=False
        )
        no_backup_writer.restore_from_backup("x.yml")

        after = sorted(p.name for p in backups_dir.rglob("*") if p.is_file())
        # Backup contents are unchanged — no new pre-restore backup written.
        assert after == before, (
            f"restore_from_backup created a new backup despite "
            f"enable_backups=False. Before: {before}, after: {after}"
        )
        # Restore itself worked: x.yml is now v1 again.
        assert (tmp_path / "x.yml").read_text() == "v1"

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions only")
    def test_default_restrict_permissions_applies_0o640(self, tmp_path: Path):
        writer = SafeFileWriter(tmp_path, validate_python=False)
        writer.write_file_safe("content", "x.yml")
        mode = (tmp_path / "x.yml").stat().st_mode & 0o777
        assert mode == 0o640

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions only")
    def test_restrict_permissions_false_leaves_umask_default(self, tmp_path: Path):
        writer = SafeFileWriter(
            tmp_path, restrict_permissions=False, validate_python=False
        )
        writer.write_file_safe("content", "x.yml")
        mode = (tmp_path / "x.yml").stat().st_mode & 0o777
        # Not the restrictive 0o640
        assert mode != 0o640

    def test_nested_parents_auto_created(self, tmp_path: Path):
        writer = SafeFileWriter(
            tmp_path, enable_backups=False, validate_python=False
        )
        target, _ = writer.write_file_safe("content", "a/b/c/d.yml")
        assert target == (tmp_path / "a" / "b" / "c" / "d.yml").resolve()
        assert target.read_text() == "content"

    def test_containment_still_enforced(self, tmp_path: Path):
        writer = SafeFileWriter(
            tmp_path, enable_backups=False, validate_python=False
        )
        with pytest.raises(FileOperationError, match="resolves outside"):
            writer.write_file_safe("content", "../escape.yml")


# ---------------------------------------------------------------------------
# SafeFileWriter.read_file_safe
# ---------------------------------------------------------------------------


class TestSafeFileWriterReadFileSafe:
    """Tests for the new read_file_safe method."""

    def test_returns_none_for_missing_file(self, tmp_path: Path):
        writer = SafeFileWriter(
            tmp_path, enable_backups=False, validate_python=False
        )
        assert writer.read_file_safe("missing.yml") is None

    def test_returns_content_for_existing_file(self, tmp_path: Path):
        writer = SafeFileWriter(
            tmp_path, enable_backups=False, validate_python=False
        )
        (tmp_path / "x.yml").write_text("hello world")
        assert writer.read_file_safe("x.yml") == "hello world"

    def test_rejects_traversal(self, tmp_path: Path):
        writer = SafeFileWriter(
            tmp_path, enable_backups=False, validate_python=False
        )
        with pytest.raises(FileOperationError, match="resolves outside"):
            writer.read_file_safe("../../etc/passwd")
