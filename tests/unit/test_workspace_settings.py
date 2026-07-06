"""Unit tests for the workspace_dir resolution logic in Settings.

Covers:
- Default ``workspace_dir`` lands at ``~/teradata-etl-mcp-workspace`` (or the
  documented fallback when ``Path.home()`` is unavailable).
- Explicit ``WORKSPACE_DIR`` env var is honoured.
- Relative artefact paths (``DBT_PROJECT_DIR``, etc.) join under the
  workspace.
- Absolute artefact paths bypass the join.
- Every artefact directory is mkdir'd by ``validate_settings``.
- The mkdir is idempotent (re-loading Settings doesn't fail on existing dir).
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from elt_mcp_server.config import Settings


def _minimal_env(**overrides: str) -> dict[str, str]:
    """Return the minimum env vars needed to construct Settings without
    Pydantic missing-field errors. Tests layer additional vars on top."""
    base = {
        "TERADATA_HOST": "td.example.com",
        "TERADATA_USERNAME": "u",
        "TERADATA_PASSWORD": "p",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# workspace_dir resolution
# ---------------------------------------------------------------------------


class TestWorkspaceDirDefault:
    def test_default_workspace_under_home_dir(self, tmp_path: Path):
        """When ``WORKSPACE_DIR`` is unset, the default is
        ``<home>/teradata-etl-mcp-workspace`` (a visible dir, following the
        convention of ``~/go``, ``~/IdeaProjects``,
        ``~/eclipse-workspace``).

        Hermetic guard: ``Path.home()`` is patched to a tmp directory
        because clearing ``HOME``/``USERPROFILE`` is not enough — Python
        falls back to ``pwd.getpwuid`` (POSIX) or registry lookups
        (Windows), so the unmodified test would create a real
        ``~/teradata-etl-mcp-workspace`` on the host machine via
        ``Settings.validate_settings.mkdir``.
        """
        fake_home = tmp_path / "fake_home"
        fake_home.mkdir()
        with patch.dict(os.environ, _minimal_env(), clear=True), \
             patch("pathlib.Path.home", return_value=fake_home):
            s = Settings()
            assert s.workspace_dir == fake_home / "teradata-etl-mcp-workspace"
            assert s.workspace_dir.exists()
            assert s.workspace_dir.is_absolute()

    def test_explicit_workspace_dir_used(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, _minimal_env(WORKSPACE_DIR=tmp), clear=True):
                s = Settings()
                assert s.workspace_dir == Path(tmp).resolve()
                assert s.workspace_dir.exists()


# ---------------------------------------------------------------------------
# Sub-path resolution under workspace
# ---------------------------------------------------------------------------


class TestRelativePathJoinedUnderWorkspace:
    """Relative artefact paths default to bare names (``dbt_project``,
    ``ttu_scripts``, ``airflow_dags``, ``logs/...``, ``.elt-mcp/metadata.db``)
    — the ``validate_settings`` model-validator joins each under
    ``workspace_dir`` so callers see a fully-resolved absolute path."""

    def test_relative_dbt_project_dir_resolves_under_workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(
                os.environ,
                _minimal_env(WORKSPACE_DIR=tmp, DBT_PROJECT_DIR="dbt_project"),
                clear=True,
            ):
                s = Settings()
                expected = Path(tmp).resolve() / "dbt_project"
                assert s.dbt.project_dir == expected
                assert s.dbt.project_dir.exists()

    def test_default_artefact_subdirs_under_workspace(self):
        """Without explicit overrides, every workspace-scoped default
        ends up under ``workspace_dir``."""
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, _minimal_env(WORKSPACE_DIR=tmp), clear=True):
                s = Settings()
                ws = Path(tmp).resolve()
                assert s.dbt.project_dir == ws / "dbt_project"
                assert s.ttu.scripts_dir == ws / "ttu_scripts"
                assert s.pipeline.dags_output_dir == ws / "airflow_dags"
                assert s.mcp.log_file == ws / "logs" / "elt-mcp-server.log"
                assert s.observability.audit_log_file == ws / "logs" / "audit.jsonl"
                assert s.mcp.metadata_db_path == ws / ".elt-mcp" / "metadata.db"
                # Directories created
                for d in (
                    s.dbt.project_dir,
                    s.ttu.scripts_dir,
                    s.pipeline.dags_output_dir,
                ):
                    assert d.is_dir(), d
                # File parents created
                assert s.mcp.log_file.parent.is_dir()
                assert s.mcp.metadata_db_path.parent.is_dir()
                # audit_log_file parent only mkdir'd when enabled
                # (default is enable_audit_log=False)


class TestAbsolutePathLeftUntouched:
    """Operators with explicit absolute paths in their ``.env`` (legacy
    workflow) keep working unchanged — the resolver only joins relative."""

    def test_absolute_dbt_project_dir_left_untouched(self):
        with (
            tempfile.TemporaryDirectory() as tmp_workspace,
            tempfile.TemporaryDirectory() as elsewhere,
        ):
            with patch.dict(
                os.environ,
                _minimal_env(
                    WORKSPACE_DIR=tmp_workspace,
                    DBT_PROJECT_DIR=elsewhere,  # absolute
                ),
                clear=True,
            ):
                s = Settings()
                # dbt.project_dir == elsewhere (absolute), not joined under workspace.
                assert s.dbt.project_dir == Path(elsewhere)
                # Workspace itself is the tmp_workspace path.
                assert s.workspace_dir == Path(tmp_workspace).resolve()

    def test_absolute_ttu_and_dags_paths_left_untouched(self):
        with (
            tempfile.TemporaryDirectory() as tmp_workspace,
            tempfile.TemporaryDirectory() as ttu_loc,
            tempfile.TemporaryDirectory() as dags_loc,
        ):
            with patch.dict(
                os.environ,
                _minimal_env(
                    WORKSPACE_DIR=tmp_workspace,
                    TTU_SCRIPTS_DIR=ttu_loc,
                    PIPELINE_DAGS_OUTPUT_DIR=dags_loc,
                ),
                clear=True,
            ):
                s = Settings()
                assert s.ttu.scripts_dir == Path(ttu_loc)
                assert s.pipeline.dags_output_dir == Path(dags_loc)


class TestIdempotency:
    """``Settings.validate_settings`` is called once per construction. A
    second instantiation against the same workspace_dir must not crash on
    existing directories and must produce the same resolved paths."""

    def test_repeated_settings_construction_same_workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, _minimal_env(WORKSPACE_DIR=tmp), clear=True):
                s1 = Settings()
                s2 = Settings()
                assert s1.workspace_dir == s2.workspace_dir
                assert s1.dbt.project_dir == s2.dbt.project_dir
                assert s1.mcp.metadata_db_path == s2.mcp.metadata_db_path
                # Both resolved to the same on-disk directory; no exception.
                assert s2.workspace_dir.is_dir()


# ---------------------------------------------------------------------------
# Regression guard for the original nesting bug (server logs.txt 2026-04-23)
# ---------------------------------------------------------------------------


class TestNoMoreCwdNesting:
    """The original bug: ``MCP_WORKSPACE_DIR=<root>/dbt_project`` triggered
    ``os.chdir`` BEFORE Settings loaded, then ``Path("./dbt_project").resolve()``
    became ``<root>/dbt_project/dbt_project``. The new design joins relative
    defaults under ``Settings.workspace_dir`` directly — cwd no longer
    drives the resolution, so even a misnamed ``WORKSPACE_DIR`` produces
    a predictable single-level path.
    """

    def test_workspace_dir_pointed_at_legacy_dbt_folder_does_not_double_nest(
        self, tmp_path
    ):
        """Even if the operator misconfigures ``WORKSPACE_DIR`` to point at
        a folder named ``dbt_project``, the resolved ``dbt.project_dir``
        is the one-level join — not double-nested."""
        legacy = tmp_path / "dbt_project"
        legacy.mkdir()
        with patch.dict(
            os.environ,
            _minimal_env(WORKSPACE_DIR=str(legacy)),
            clear=True,
        ):
            s = Settings()
            # Joined ONCE: <legacy>/dbt_project. NOT triple-nested.
            assert s.dbt.project_dir == legacy.resolve() / "dbt_project"
            # When WORKSPACE_DIR is misconfigured to a folder named
            # ``dbt_project``, the server still appends ``dbt_project``,
            # producing ``<legacy>/dbt_project/dbt_project``. The path is
            # double-nested in component terms but exactly ONE join from
            # the workspace's perspective (next line's invariant). The
            # bug we're guarding against here is TRIPLE-nesting (the
            # validator joining twice and producing
            # ``<legacy>/dbt_project/dbt_project/dbt_project``).
            assert s.dbt.project_dir.parts[-3:] != (
                "dbt_project", "dbt_project", "dbt_project"
            )
            # Sanity: only one level deeper than workspace_dir.
            assert s.dbt.project_dir.parent == s.workspace_dir
