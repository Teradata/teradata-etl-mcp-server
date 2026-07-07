"""Unit tests for dbt client."""

import json
import subprocess
import threading
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from teradata_etl_mcp_server.clients.dbt_client import (
    DBTClient,
    DbtClient,
    DBTClientError,
    DBTCommandError,
    DBTProjectError,
)


class TestDbtClient:
    """Test suite for DbtClient."""

    @pytest.fixture(autouse=True)
    def _mock_dbt_installed(self):
        """Mock shutil.which so dbt appears installed in all tests."""
        with patch("shutil.which", return_value="/usr/bin/dbt"):
            yield

    @pytest.fixture
    def client_config(self, tmp_path):
        """Test client configuration."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        profiles_dir = tmp_path / "profiles"
        profiles_dir.mkdir()

        return {
            "project_dir": str(project_dir),
            "profiles_dir": str(profiles_dir),
            "target": "dev",
        }

    @pytest.fixture
    def client(self, client_config):
        """Create DbtClient instance."""
        return DbtClient(**client_config)

    @pytest.fixture
    def mock_subprocess_result(self):
        """Create mock subprocess result."""
        result = Mock()
        result.returncode = 0
        result.stdout = ""
        result.stderr = ""
        return result

    # Initialization Tests

    def test_init_with_valid_config(self, client, client_config):
        """Test initialization with valid configuration."""
        assert client.project_dir == Path(client_config["project_dir"])
        assert client.profiles_dir == Path(client_config["profiles_dir"])
        assert client.target == "dev"
        assert client.command_timeout == 300

    def test_init_creates_directories(self, tmp_path):
        """Test that initialization requires directories to exist."""
        project_dir = tmp_path / "new_project"
        profiles_dir = tmp_path / "new_profiles"

        # Should raise error when project directory doesn't exist
        with pytest.raises(DBTProjectError):
            DbtClient(
                project_dir=str(project_dir),
                profiles_dir=str(profiles_dir),
                target="dev",
            )

        # Create directory and dbt_project.yml then try again
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )
        client = DbtClient(
            project_dir=str(project_dir),
            profiles_dir=str(profiles_dir),
            target="dev",
        )
        assert client.project_dir == project_dir

    def test_init_custom_command_timeout(self, tmp_path):
        """Test construction with custom command_timeout."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        client = DbtClient(
            project_dir=str(project_dir), target="dev", command_timeout=600
        )
        assert client.command_timeout == 600

    # Validation Tests

    def test_init_invalid_target_empty(self, tmp_path):
        """Test init rejects empty target."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        with pytest.raises(ValueError, match="Invalid target"):
            DbtClient(project_dir=str(project_dir), target="")

    def test_init_invalid_target_special_chars(self, tmp_path):
        """Test init rejects target with special characters."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        with pytest.raises(ValueError, match="Invalid target"):
            DbtClient(project_dir=str(project_dir), target="dev;rm -rf /")

    def test_init_invalid_threads_zero(self, tmp_path):
        """Test init rejects threads=0."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        with pytest.raises(ValueError, match="Invalid threads"):
            DbtClient(project_dir=str(project_dir), target="dev", threads=0)

    def test_init_invalid_threads_too_high(self, tmp_path):
        """Test init rejects threads > 64."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        with pytest.raises(ValueError, match="Invalid threads"):
            DbtClient(project_dir=str(project_dir), target="dev", threads=65)

    def test_init_invalid_command_timeout(self, tmp_path):
        """Test init rejects command_timeout=0."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        with pytest.raises(ValueError, match="Invalid command_timeout"):
            DbtClient(
                project_dir=str(project_dir), target="dev", command_timeout=0
            )

    def test_init_missing_dbt_project_yml(self, tmp_path):
        """Test init raises when directory exists but dbt_project.yml is missing."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()

        with pytest.raises(DBTProjectError, match="dbt_project.yml not found"):
            DbtClient(project_dir=str(project_dir), target="dev")

    # Thread Safety Tests

    def test_file_lock_exists(self, client):
        """Test that _file_lock is a threading.Lock."""
        assert isinstance(client._file_lock, type(threading.Lock()))

    # Selection Validation Tests

    @patch("subprocess.run")
    def test_run_rejects_null_bytes_in_models(self, mock_run, client):
        """Test run rejects models with null bytes."""
        with pytest.raises(ValueError, match="Null bytes"):
            client.run(models=["model\x00evil"])

    @patch("subprocess.run")
    def test_run_rejects_empty_model_strings(self, mock_run, client):
        """Test run rejects empty model strings."""
        with pytest.raises(ValueError, match="Empty string"):
            client.run(models=[""])

    # dbt Run Tests

    @patch("subprocess.run")
    def test_run_models_success(self, mock_run, client, mock_subprocess_result):
        """Test successful dbt run."""
        # Create target directory and write run results
        target_dir = client.project_dir / "target"
        target_dir.mkdir(exist_ok=True)

        results_data = {
            "results": [
                {"status": "success", "unique_id": "model.project.model1"},
                {"status": "success", "unique_id": "model.project.model2"},
            ]
        }

        with open(target_dir / "run_results.json", "w") as f:
            json.dump(results_data, f)

        mock_run.return_value = mock_subprocess_result

        result = client.run()

        assert result["success"] is True
        assert len(result["results"]) == 2
        mock_run.assert_called_once()

        # Verify command. The binary is resolved via shutil.which("dbt"), so the
        # first arg may be a full path (e.g. "/usr/bin/dbt") rather than "dbt".
        # Assert specifically on argv[0] basename so an unrelated arg containing
        # "dbt" (like a project path) cannot satisfy this check.
        call_args = mock_run.call_args[0][0]
        assert Path(call_args[0]).name in ("dbt", "dbt.exe")
        assert "run" in call_args

    @patch("subprocess.run")
    def test_run_specific_models(self, mock_run, client, mock_subprocess_result):
        """Test running specific models."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.run(models=["model1", "model2"])

        call_args = mock_run.call_args[0][0]
        assert "--select" in call_args
        # Models are joined with space
        select_index = call_args.index("--select")
        assert "model1 model2" == call_args[select_index + 1]

    @patch("subprocess.run")
    def test_run_with_full_refresh(self, mock_run, client, mock_subprocess_result):
        """Test dbt run with full refresh."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.run(full_refresh=True)

        call_args = mock_run.call_args[0][0]
        assert "--full-refresh" in call_args

    @patch("subprocess.run")
    def test_run_failure(self, mock_run, client):
        """Test dbt run failure."""
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Error: Model compilation failed"
        mock_result.stdout = json.dumps(
            {"results": [{"status": "error", "message": "Compilation error"}]}
        )
        mock_run.return_value = mock_result

        with pytest.raises(DBTCommandError):
            client.run()

    @patch("subprocess.run")
    def test_run_with_exclude(self, mock_run, client, mock_subprocess_result):
        """Test dbt run with exclusions."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.run(exclude=["staging.*", "test_*"])

        call_args = mock_run.call_args[0][0]
        assert "--exclude" in call_args

    # dbt Test Tests

    @patch("subprocess.run")
    def test_test_models_success(self, mock_run, client, mock_subprocess_result):
        """Test successful dbt test."""
        # Create target directory and write run results
        target_dir = client.project_dir / "target"
        target_dir.mkdir(exist_ok=True)

        results_data = {
            "results": [
                {"status": "pass", "unique_id": "test.project.test1"},
                {"status": "pass", "unique_id": "test.project.test2"},
            ]
        }

        with open(target_dir / "run_results.json", "w") as f:
            json.dump(results_data, f)

        mock_run.return_value = mock_subprocess_result

        result = client.test()

        assert result["success"] is True
        assert len(result["results"]) == 2

        call_args = mock_run.call_args[0][0]
        assert "test" in call_args

    @patch("subprocess.run")
    def test_test_specific_models(self, mock_run, client, mock_subprocess_result):
        """Test running tests for specific models."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.test(models=["model1"])

        call_args = mock_run.call_args[0][0]
        assert "--select" in call_args

    @patch("subprocess.run")
    def test_test_with_failures(self, mock_run, client):
        """Test dbt test with failures."""
        # Create target directory and write run results
        target_dir = client.project_dir / "target"
        target_dir.mkdir(exist_ok=True)

        results_data = {
            "results": [
                {"status": "pass", "unique_id": "test1"},
                {"status": "fail", "unique_id": "test2", "message": "Assertion failed"},
            ]
        }

        with open(target_dir / "run_results.json", "w") as f:
            json.dump(results_data, f)

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        result = client.test()

        assert result["success"] is False
        assert any(r.get("status") == "fail" for r in result.get("results", []))

    # dbt Compile Tests

    @patch("subprocess.run")
    def test_compile_success(self, mock_run, client, mock_subprocess_result):
        """Test successful dbt compile."""
        mock_subprocess_result.stdout = json.dumps(
            {"results": [{"status": "success", "unique_id": "model.project.model1"}]}
        )
        mock_run.return_value = mock_subprocess_result

        result = client.compile()

        assert result["success"] is True

        call_args = mock_run.call_args[0][0]
        assert "compile" in call_args

    @patch("subprocess.run")
    def test_compile_does_not_read_stale_run_results(self, mock_run, client, mock_subprocess_result):
        """Test compile() does NOT read run_results.json (dbt compile doesn't produce it)."""
        target_dir = client.project_dir / "target"
        target_dir.mkdir(exist_ok=True)

        run_results_data = {
            "elapsed_time": 1.23,
            "results": [
                {"status": "success", "unique_id": "model.project.model1"},
            ],
        }
        with open(target_dir / "run_results.json", "w") as f:
            json.dump(run_results_data, f)

        mock_run.return_value = mock_subprocess_result

        result = client.compile()

        assert result["success"] is True
        assert "elapsed_time" not in result
        assert "results" not in result

    @patch("subprocess.run")
    def test_compile_specific_models(self, mock_run, client, mock_subprocess_result):
        """Test compiling specific models."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.compile(models=["model1", "model2"])

        call_args = mock_run.call_args[0][0]
        assert "--select" in call_args

    # dbt Docs Tests

    @patch("subprocess.run")
    def test_docs_generate(self, mock_run, client, mock_subprocess_result):
        """Test dbt docs generate."""
        mock_run.return_value = mock_subprocess_result

        result = client.docs_generate()

        assert result["success"] is True

        call_args = mock_run.call_args[0][0]
        assert "docs" in call_args
        assert "generate" in call_args

    # dbt List Tests

    @patch("subprocess.run")
    def test_list_models(self, mock_run, client):
        """Test listing dbt models."""
        # Create target directory and manifest
        target_dir = client.project_dir / "target"
        target_dir.mkdir(exist_ok=True)

        manifest_data = {
            "nodes": {
                "model.project.model1": {"name": "model1", "resource_type": "model"},
                "model.project.model2": {"name": "model2", "resource_type": "model"},
                "model.project.model3": {"name": "model3", "resource_type": "model"},
            }
        }

        with open(target_dir / "manifest.json", "w") as f:
            json.dump(manifest_data, f)

        # Mock subprocess to return model names
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = (
            "model.project.model1\nmodel.project.model2\nmodel.project.model3"
        )
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        result = client.list_models()

        assert len(result) == 3
        assert any(m["name"] == "model1" for m in result)

    @patch("subprocess.run")
    def test_list_models_loads_teradata_vars_from_dotenv(self, mock_run, client):
        """list_models passes TERADATA_* from project .env to subprocess when auth=None."""
        dotenv_path = client.project_dir / ".env"
        dotenv_path.write_text("TERADATA_HOST=td.example.com\nTERADATA_PORT=1025\n")

        target_dir = client.project_dir / "target"
        target_dir.mkdir(exist_ok=True)
        (target_dir / "manifest.json").write_text('{"nodes": {}}')

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        client.list_models()

        env_passed = mock_run.call_args.kwargs["env"]
        assert env_passed["TERADATA_HOST"] == "td.example.com"
        assert env_passed["TERADATA_PORT"] == "1025"

    @patch("subprocess.run")
    def test_list_models_no_dotenv_strips_teradata_vars(self, mock_run, client, monkeypatch):
        """list_models strips TERADATA_* from env when no .env file and no auth."""
        monkeypatch.setenv("TERADATA_HOST", "should-be-stripped")

        target_dir = client.project_dir / "target"
        target_dir.mkdir(exist_ok=True)
        (target_dir / "manifest.json").write_text('{"nodes": {}}')

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        client.list_models()

        env_passed = mock_run.call_args.kwargs["env"]
        assert "TERADATA_HOST" not in env_passed

    def test_compute_env_loads_dotenv_when_auth_none(self, client, tmp_path):
        """_compute_env loads TERADATA_* from .env file when auth is None."""
        dotenv_path = tmp_path / ".env"
        dotenv_path.write_text(
            "TERADATA_HOST=td.host\nTERADATA_PORT=1025\nTERADATA_DATABASE=mydb\n"
        )

        env = DBTClient._compute_env(None, dotenv_path=dotenv_path)

        assert env["TERADATA_HOST"] == "td.host"
        assert env["TERADATA_PORT"] == "1025"
        assert env["TERADATA_DATABASE"] == "mydb"
        assert env["NO_COLOR"] == "1"

    def test_compute_env_auth_takes_precedence_over_dotenv(self, client, tmp_path):
        """auth.render_for_dbt_env() overrides .env values when both are present."""
        from unittest.mock import Mock

        dotenv_path = tmp_path / ".env"
        dotenv_path.write_text("TERADATA_HOST=from-dotenv\n")

        auth = Mock()
        auth.render_for_dbt_env.return_value = {"TERADATA_HOST": "from-auth"}

        env = DBTClient._compute_env(auth, dotenv_path=dotenv_path)

        assert env["TERADATA_HOST"] == "from-auth"

    def test_compute_env_missing_dotenv_strips_teradata_vars(self, client, tmp_path, monkeypatch):
        """_compute_env strips TERADATA_* when .env absent and auth=None."""
        monkeypatch.setenv("TERADATA_HOST", "should-be-stripped")
        missing_path = tmp_path / "nonexistent.env"

        env = DBTClient._compute_env(None, dotenv_path=missing_path)

        assert "TERADATA_HOST" not in env
        assert env["NO_COLOR"] == "1"

    def test_compute_env_ignores_non_teradata_dotenv_keys(self, client, tmp_path):
        """_compute_env only loads TERADATA_* keys from .env, not other vars."""
        dotenv_path = tmp_path / ".env"
        dotenv_path.write_text("TERADATA_HOST=td.host\nSOME_OTHER_VAR=secret\n")

        env = DBTClient._compute_env(None, dotenv_path=dotenv_path)

        assert env["TERADATA_HOST"] == "td.host"
        assert "SOME_OTHER_VAR" not in env

    def test_get_model_sql_loads_teradata_vars_from_dotenv(self, tmp_path):
        """get_model_sql subprocess receives TERADATA_HOST from project .env."""
        (tmp_path / "dbt_project.yml").write_text("name: test\nversion: '1.0'\nprofile: default\n")
        (tmp_path / ".env").write_text("TERADATA_HOST=db.example.com\nTERADATA_PORT=1025\n")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            client = DBTClient(project_dir=str(tmp_path))
            client._parse_manifest = Mock(return_value={"nodes": {}})
            client.get_model_sql("my_model")
        call_env = mock_run.call_args.kwargs["env"]
        assert call_env.get("TERADATA_HOST") == "db.example.com"
        assert call_env.get("TERADATA_PORT") == "1025"

    def test_get_model_sql_no_dotenv_strips_teradata_vars(self, tmp_path, monkeypatch):
        """get_model_sql subprocess has no TERADATA_HOST when no project .env exists."""
        (tmp_path / "dbt_project.yml").write_text("name: test\nversion: '1.0'\nprofile: default\n")
        monkeypatch.setenv("TERADATA_HOST", "should-be-stripped")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            client = DBTClient(project_dir=str(tmp_path))
            client._parse_manifest = Mock(return_value={"nodes": {}})
            client.get_model_sql("my_model")
        call_env = mock_run.call_args.kwargs["env"]
        assert "TERADATA_HOST" not in call_env

    def test_get_project_info_loads_teradata_vars_from_dotenv(self, tmp_path):
        """get_project_info subprocess receives TERADATA_HOST from project .env."""
        (tmp_path / "dbt_project.yml").write_text("name: test\nversion: '1.0'\nprofile: default\n")
        (tmp_path / ".env").write_text("TERADATA_HOST=db.example.com\n")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            client = DBTClient(project_dir=str(tmp_path))
            client._parse_manifest = Mock(return_value={"nodes": {}, "sources": {}, "metrics": {}})
            client.get_project_info()
        call_env = mock_run.call_args.kwargs["env"]
        assert call_env.get("TERADATA_HOST") == "db.example.com"

    def test_list_sources(self, client):
        """Test listing dbt sources."""
        # Create target directory and manifest
        target_dir = client.project_dir / "target"
        target_dir.mkdir(exist_ok=True)

        manifest_data = {
            "sources": {
                "source.project.db.table1": {"name": "table1", "source_name": "db"},
                "source.project.db.table2": {"name": "table2", "source_name": "db"},
            }
        }

        with open(target_dir / "manifest.json", "w") as f:
            json.dump(manifest_data, f)

        result = client.list_sources()

        assert len(result) == 2

    def test_list_tests(self, client):
        """Test listing dbt tests."""
        # Create target directory and manifest
        target_dir = client.project_dir / "target"
        target_dir.mkdir(exist_ok=True)

        manifest_data = {
            "nodes": {
                "test.project.test1": {"name": "test1", "resource_type": "test"},
                "test.project.test2": {"name": "test2", "resource_type": "test"},
            }
        }

        with open(target_dir / "manifest.json", "w") as f:
            json.dump(manifest_data, f)

        result = client.list_tests()

        assert len(result) == 2

    # dbt Seed Tests

    @patch("subprocess.run")
    def test_seed_success(self, mock_run, client, mock_subprocess_result):
        """Test dbt seed."""
        mock_subprocess_result.stdout = json.dumps(
            {"results": [{"status": "success", "unique_id": "seed.project.seed1"}]}
        )
        mock_run.return_value = mock_subprocess_result

        result = client.seed()

        assert result["success"] is True

        call_args = mock_run.call_args[0][0]
        assert "seed" in call_args

    @patch("subprocess.run")
    def test_seed_specific_seeds(self, mock_run, client, mock_subprocess_result):
        """Test seeding specific seeds."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.seed(select="seed1")

        call_args = mock_run.call_args[0][0]
        assert "--select" in call_args

    # dbt Snapshot Tests

    @patch("subprocess.run")
    def test_snapshot_success(self, mock_run, client, mock_subprocess_result):
        """Test dbt snapshot."""
        mock_subprocess_result.stdout = json.dumps(
            {"results": [{"status": "success", "unique_id": "snapshot.project.snap1"}]}
        )
        mock_run.return_value = mock_subprocess_result

        result = client.snapshot()

        assert result["success"] is True

        call_args = mock_run.call_args[0][0]
        assert "snapshot" in call_args

    # dbt Debug Tests

    @patch("subprocess.run")
    def test_debug(self, mock_run, client):
        """Test dbt debug."""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "All checks passed!"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        result = client.debug()

        assert result["success"] is True
        assert "checks passed" in result.get("stdout", "").lower()

        call_args = mock_run.call_args[0][0]
        assert "debug" in call_args

    # dbt Clean Tests

    @patch("subprocess.run")
    def test_clean(self, mock_run, client, mock_subprocess_result):
        """Test dbt clean."""
        mock_run.return_value = mock_subprocess_result

        result = client.clean()

        assert result["success"] is True

        call_args = mock_run.call_args[0][0]
        assert "clean" in call_args

    # dbt Deps Tests

    @patch("subprocess.run")
    def test_deps_success(self, mock_run, client, mock_subprocess_result):
        """Test dbt deps."""
        mock_run.return_value = mock_subprocess_result

        result = client.deps()

        assert result["success"] is True

        call_args = mock_run.call_args[0][0]
        assert "deps" in call_args

    @patch("subprocess.run")
    def test_parse_success(self, mock_run, client, mock_subprocess_result):
        """Test dbt parse writes manifest and returns success."""
        mock_subprocess_result.stdout = ""
        mock_run.return_value = mock_subprocess_result

        result = client.parse()

        assert result["success"] is True
        call_args = mock_run.call_args[0][0]
        assert "parse" in call_args
        assert "--threads" not in call_args
        assert result["manifest_path"].endswith("manifest.json")

    # Manifest Tests

    def test_get_manifest(self, client):
        """Test reading manifest.json."""
        manifest_path = client.project_dir / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "nodes": {
                "model.project.model1": {
                    "name": "model1",
                    "resource_type": "model",
                }
            }
        }

        with open(manifest_path, "w") as f:
            json.dump(manifest_data, f)

        result = client.get_manifest()

        assert result is not None
        assert "nodes" in result
        assert "model.project.model1" in result["nodes"]

    def test_get_manifest_not_found(self, client):
        """Test getting manifest when file doesn't exist."""
        result = client.get_manifest()

        assert result is None

    def test_get_catalog(self, client):
        """Test reading catalog.json."""
        catalog_path = client.project_dir / "target" / "catalog.json"
        catalog_path.parent.mkdir(parents=True, exist_ok=True)

        catalog_data = {
            "nodes": {
                "model.project.model1": {
                    "columns": {
                        "id": {"type": "integer"},
                        "name": {"type": "varchar"},
                    }
                }
            }
        }

        with open(catalog_path, "w") as f:
            json.dump(catalog_data, f)

        result = client.get_catalog()

        assert result is not None
        assert "nodes" in result

    # Run Results Tests

    def test_get_run_results(self, client):
        """Test reading run_results.json."""
        results_path = client.project_dir / "target" / "run_results.json"
        results_path.parent.mkdir(parents=True, exist_ok=True)

        results_data = {
            "results": [
                {
                    "status": "success",
                    "execution_time": 1.5,
                    "unique_id": "model.project.model1",
                }
            ],
            "elapsed_time": 2.0,
        }

        with open(results_path, "w") as f:
            json.dump(results_data, f)

        result = client.get_run_results()

        assert result is not None
        assert "results" in result
        assert result["results"][0]["status"] == "success"

    # Project Configuration Tests

    def test_get_project_config(self, client):
        """Test reading dbt_project.yml."""
        result = client.get_project_config()

        assert result is not None
        assert result["name"] == "test"

    def test_get_profiles_config(self, client):
        """Test reading profiles.yml."""
        profiles_path = client.profiles_dir / "profiles.yml"

        with open(profiles_path, "w") as f:
            f.write(
                "default:\n"
                "  target: dev\n"
                "  outputs:\n"
                "    dev:\n"
                "      type: postgres\n"
                "      host: localhost\n"
                "      port: 5432\n"
            )

        result = client.get_profiles_config()

        assert result is not None
        assert "default" in result
        assert result["default"]["target"] == "dev"

    def test_get_target_schema_from_schema_field(self, client):
        """Resolves schema from the standard 'schema' field in profiles.yml."""
        profiles_path = client.profiles_dir / "profiles.yml"
        profiles_path.write_text(
            "default:\n"
            "  target: dev\n"
            "  outputs:\n"
            "    dev:\n"
            "      type: teradata\n"
            "      schema: analytics\n"
        )

        result = client.get_target_schema()

        assert result == "analytics"

    def test_get_target_schema_from_dataset_field(self, client):
        """Falls back to 'dataset' field for adapters like BigQuery."""
        profiles_path = client.profiles_dir / "profiles.yml"
        profiles_path.write_text(
            "default:\n"
            "  target: dev\n"
            "  outputs:\n"
            "    dev:\n"
            "      type: bigquery\n"
            "      dataset: my_dataset\n"
        )

        result = client.get_target_schema()

        assert result == "my_dataset"

    def test_get_target_schema_missing_profile_in_project(self, client):
        """Returns None when dbt_project.yml has no 'profile' key."""
        (client.project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\n"  # no 'profile' key
        )

        result = client.get_target_schema()

        assert result is None

    def test_get_target_schema_profiles_not_found(self, client):
        """Returns None when profiles.yml does not exist."""
        # profiles_dir exists but profiles.yml is not written
        result = client.get_target_schema()

        assert result is None

    def test_get_target_schema_falls_back_to_profile_default_target(self, client):
        """When self.target is not in outputs, falls back to the profile's 'target' field."""
        profiles_path = client.profiles_dir / "profiles.yml"
        # client uses 'dev', but outputs only has 'prod'; profile default is 'prod'
        profiles_path.write_text(
            "default:\n"
            "  target: prod\n"
            "  outputs:\n"
            "    prod:\n"
            "      type: teradata\n"
            "      schema: prod_schema\n"
        )

        result = client.get_target_schema()

        assert result == "prod_schema"

    def test_get_target_schema_target_not_in_outputs(self, client):
        """Returns None when neither self.target nor the profile default are in outputs."""
        profiles_path = client.profiles_dir / "profiles.yml"
        # client uses 'dev', profile default is also 'dev' — neither in outputs
        profiles_path.write_text(
            "default:\n"
            "  target: dev\n"
            "  outputs:\n"
            "    prod:\n"
            "      type: teradata\n"
            "      schema: prod_schema\n"
        )

        result = client.get_target_schema()

        assert result is None

    # Error Handling Tests

    @patch("shutil.which")
    def test_command_not_found(self, mock_which, tmp_path):
        """Test handling when dbt command is not found.

        Init should succeed (deferred check), but command execution
        should raise a clear error with install instructions.
        """
        mock_which.return_value = None

        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        # Init should NOT crash — the dbt binary check is deferred
        client = DbtClient(project_dir=str(project_dir), target="dev")
        assert client.project_dir == project_dir

        # But command execution should fail with a clear message
        with patch("subprocess.run", side_effect=FileNotFoundError("dbt")):
            with pytest.raises(DBTClientError, match="not installed or not on PATH"):
                client._execute_command(["dbt", "run"])

    @patch("subprocess.run")
    def test_subprocess_timeout(self, mock_run, client):
        """Test handling subprocess timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired("dbt run", 300)

        with pytest.raises(DBTCommandError, match="timed out after"):
            client.run()

    @patch("subprocess.run")
    def test_invalid_json_output(self, mock_run, client):
        """Test handling invalid JSON output."""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Not valid JSON"
        mock_run.return_value = mock_result

        result = client.run()

        # Should handle gracefully
        assert result is not None

    # Timeout Tests

    @patch("subprocess.run")
    def test_execute_command_uses_default_timeout(
        self, mock_run, client, mock_subprocess_result
    ):
        """Test that subprocess.run receives the default timeout."""
        mock_run.return_value = mock_subprocess_result

        client._execute_command(["dbt", "run"])

        _, kwargs = mock_run.call_args
        assert kwargs["timeout"] == 300

    @patch("subprocess.run")
    def test_execute_command_uses_custom_timeout(
        self, mock_run, client, mock_subprocess_result
    ):
        """Test per-command timeout override."""
        mock_run.return_value = mock_subprocess_result

        client._execute_command(["dbt", "run"], timeout=120)

        _, kwargs = mock_run.call_args
        assert kwargs["timeout"] == 120

    @patch("subprocess.run")
    def test_timeout_expired_gives_clear_message(self, mock_run, client):
        """Test TimeoutExpired produces a clear error message."""
        mock_run.side_effect = subprocess.TimeoutExpired("dbt run", 300)

        with pytest.raises(DBTCommandError, match="timed out after 300 seconds"):
            client._execute_command(["dbt", "run"])

    # Selection Syntax Tests

    @patch("subprocess.run")
    def test_run_with_tag_selection(self, mock_run, client, mock_subprocess_result):
        """Test running models with tag selection."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.run(models=["tag:daily"])

        call_args = mock_run.call_args[0][0]
        assert "tag:daily" in str(call_args)

    @patch("subprocess.run")
    def test_run_with_graph_selection(self, mock_run, client, mock_subprocess_result):
        """Test running models with graph operators."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.run(models=["+model1", "model2+"])

        call_args = mock_run.call_args[0][0]
        assert "+model1" in str(call_args) or "model2+" in str(call_args)

    # Threads and Vars Tests

    @patch("subprocess.run")
    def test_run_with_threads(self, mock_run, client, mock_subprocess_result):
        """Test running with custom thread count."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        # Threads is set at client initialization, not in run()
        client.run()

        call_args = mock_run.call_args[0][0]
        assert "--threads" in call_args
        # Default threads from fixture is 4 (from config)
        assert "4" in call_args

    @patch("subprocess.run")
    def test_run_with_threads_override(self, mock_run, client, mock_subprocess_result):
        """Per-call threads parameter overrides the client-level default."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.run(threads=8)

        call_args = mock_run.call_args[0][0]
        threads_idx = call_args.index("--threads")
        assert call_args[threads_idx + 1] == "8"

    @patch("subprocess.run")
    def test_run_with_vars(self, mock_run, client, mock_subprocess_result):
        """Test running with variables."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        client.run(vars={"start_date": "2025-01-01", "end_date": "2025-12-31"})

        call_args = mock_run.call_args[0][0]
        assert "--vars" in call_args

    # Target Override Tests

    @patch("subprocess.run")
    def test_run_with_target_override(self, mock_run, client, mock_subprocess_result):
        """Test running with target."""
        mock_subprocess_result.stdout = json.dumps({"results": []})
        mock_run.return_value = mock_subprocess_result

        # Target is set at client initialization
        client.run()

        call_args = mock_run.call_args[0][0]
        assert "--target" in call_args
        # Default target from fixture is "dev"
        assert "dev" in call_args

    # Installation Check Tests

    @patch("subprocess.run")
    def test_check_installation_success(self, mock_run, client):
        """Test checking dbt installation."""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "dbt version 1.7.0"
        mock_run.return_value = mock_result

        result = client.check_installation()

        assert result["installed"] is True

        call_args = mock_run.call_args[0][0]
        assert "--version" in call_args

    @patch("subprocess.run")
    def test_check_installation_not_found(self, mock_run, client):
        """Test checking dbt installation when not found."""
        mock_run.side_effect = FileNotFoundError("dbt not found")

        result = client.check_installation()

        assert result["installed"] is False


class TestDbtClientGracefulDegradation:
    """Tests for graceful behavior when dbt is not installed."""

    def test_init_without_dbt_on_path(self, tmp_path):
        """DBTClient can be created even when dbt is not on PATH.

        The shutil.which check was removed from __init__ to break the catch-22
        where check_installation() couldn't be called because __init__ crashed first.
        """
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        # No need to mock shutil.which — dbt doesn't need to be on PATH for init
        client = DbtClient(project_dir=str(project_dir), target="dev")
        assert client.project_dir == project_dir

    def test_check_installation_is_static(self):
        """check_installation works as a static method without an instance."""
        with patch("subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = (
                "Core:\n  - installed: 1.7.0\n\n"
                "Plugins:\n  - teradata: 1.7.0\n"
            )
            mock_run.return_value = mock_result

            # Call as static — no instance needed
            result = DBTClient.check_installation()

            assert result["installed"] is True
            assert result["teradata_installed"] is True

    def test_check_installation_static_dbt_missing(self):
        """Static check_installation returns installed=False when dbt not found."""
        with patch("subprocess.run", side_effect=FileNotFoundError("dbt")):
            result = DBTClient.check_installation()

            assert result["installed"] is False
            assert result["teradata_installed"] is False

    def test_execute_command_clear_error_when_dbt_missing(self, tmp_path):
        """_execute_command gives a clear error when dbt binary is not found."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        client = DbtClient(project_dir=str(project_dir), target="dev")

        with patch("subprocess.run", side_effect=FileNotFoundError("dbt")):
            with pytest.raises(DBTClientError, match="not installed or not on PATH"):
                client._execute_command(["dbt", "run"])

    def test_execute_command_suggests_pip_install(self, tmp_path):
        """Error message includes pip install instruction."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )

        client = DbtClient(project_dir=str(project_dir), target="dev")

        with patch("subprocess.run", side_effect=FileNotFoundError("dbt")):
            with pytest.raises(DBTClientError, match="pip install dbt-teradata"):
                client._execute_command(["dbt", "run"])


class TestResolveEnvVarExpression:
    """Unit tests for DbtClient._resolve_env_var_expression()."""

    @pytest.fixture
    def client(self, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )
        with patch("shutil.which", return_value="/usr/bin/dbt"):
            return DbtClient(project_dir=str(project_dir), target="dev")

    def test_non_jinja_value_returned_unchanged(self, client):
        """Plain strings without {{ }} are returned as-is."""
        assert client._resolve_env_var_expression("my_schema", {}) == "my_schema"

    def test_env_var_present(self, client):
        """Returns the env var value when the key is set."""
        result = client._resolve_env_var_expression(
            "{{ env_var('DBT_SCHEMA') }}", {"DBT_SCHEMA": "prod_schema"}
        )
        assert result == "prod_schema"

    def test_env_var_missing_no_default(self, client):
        """Returns None when env var is absent and no default is provided."""
        result = client._resolve_env_var_expression(
            "{{ env_var('MISSING_VAR') }}", {}
        )
        assert result is None

    def test_env_var_missing_with_default(self, client):
        """Returns the default value when env var is absent but a default is given."""
        result = client._resolve_env_var_expression(
            "{{ env_var('MISSING_VAR', 'fallback_schema') }}", {}
        )
        assert result == "fallback_schema"

    def test_env_var_present_overrides_default(self, client):
        """Env var value takes precedence over the inline default."""
        result = client._resolve_env_var_expression(
            "{{ env_var('DBT_SCHEMA', 'fallback') }}", {"DBT_SCHEMA": "real_schema"}
        )
        assert result == "real_schema"

    def test_env_var_set_to_empty_string_returns_none(self, client):
        """An explicitly empty env var value resolves to None (falsy empty string)."""
        result = client._resolve_env_var_expression(
            "{{ env_var('DBT_SCHEMA') }}", {"DBT_SCHEMA": ""}
        )
        assert result is None


class TestGetTargetSchema:
    """Unit tests for DbtClient.get_target_schema() env_var resolution."""

    @pytest.fixture
    def client(self, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: test\nversion: '1.0'\nprofile: default\n"
        )
        with patch("shutil.which", return_value="/usr/bin/dbt"):
            return DbtClient(project_dir=str(project_dir), target="dev")

    def test_returns_plain_schema(self, client):
        """Returns schema string directly when no Jinja expression is present."""
        client.get_project_config = Mock(return_value={"profile": "my_profile"})
        client.get_profiles_config = Mock(
            return_value={
                "my_profile": {
                    "target": "dev",
                    "outputs": {"dev": {"schema": "analytics"}},
                }
            }
        )
        assert client.get_target_schema() == "analytics"

    def test_resolves_env_var_in_schema(self, client):
        """Resolves {{ env_var(...) }} in the schema field when env var is set."""
        client.get_project_config = Mock(return_value={"profile": "my_profile"})
        client.get_profiles_config = Mock(
            return_value={
                "my_profile": {
                    "target": "dev",
                    "outputs": {"dev": {"schema": "{{ env_var('DBT_SCHEMA') }}"}},
                }
            }
        )
        client._compute_env = Mock(return_value={"DBT_SCHEMA": "resolved_schema"})
        assert client.get_target_schema() == "resolved_schema"

    def test_schema_env_var_missing_returns_none(self, client):
        """Returns None when schema is an env_var expression and the var is not set."""
        client.get_project_config = Mock(return_value={"profile": "my_profile"})
        client.get_profiles_config = Mock(
            return_value={
                "my_profile": {
                    "target": "dev",
                    "outputs": {"dev": {"schema": "{{ env_var('MISSING_VAR') }}"}},
                }
            }
        )
        client._compute_env = Mock(return_value={})
        assert client.get_target_schema() is None

    def test_schema_env_var_uses_default(self, client):
        """Returns the inline default when the env var is absent."""
        client.get_project_config = Mock(return_value={"profile": "my_profile"})
        client.get_profiles_config = Mock(
            return_value={
                "my_profile": {
                    "target": "dev",
                    "outputs": {
                        "dev": {
                            "schema": "{{ env_var('MISSING_VAR', 'default_schema') }}"
                        }
                    },
                }
            }
        )
        client._compute_env = Mock(return_value={})
        assert client.get_target_schema() == "default_schema"

    def test_returns_none_when_no_project_config(self, client):
        """Returns None gracefully when dbt_project.yml cannot be read."""
        client.get_project_config = Mock(return_value=None)
        assert client.get_target_schema() is None

    def test_returns_none_when_no_profiles_config(self, client):
        """Returns None gracefully when profiles.yml cannot be read."""
        client.get_project_config = Mock(return_value={"profile": "my_profile"})
        client.get_profiles_config = Mock(return_value=None)
        assert client.get_target_schema() is None
