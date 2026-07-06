"""Unit tests for Validators.

Tests the actual production API in elt_mcp_server.utils.validators:
- ValidationError exception
- ConnectionValidator (static methods returning tuple[bool, str | None])
- PipelineValidator (static methods returning tuple[bool, str | None] or tuple[bool, list[str]])
- DataValidator (static methods returning tuple[bool, str | None])
- ConfigValidator (static methods returning tuple[bool, str | None] or tuple[bool, list[str]])
- validate_all_connections (module-level function returning dict)
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from elt_mcp_server.utils.validators import (
    ConfigValidator,
    ConnectionValidator,
    DataValidator,
    PipelineValidator,
    ValidationError,
    slugify_dir_name,
    to_quartz_cron,
    validate_airflow_identifier,
    validate_all_connections,
    validate_dag_run_id,
    validate_identifier,
)

# ---------------------------------------------------------------------------
# ValidationError
# ---------------------------------------------------------------------------


class TestValidationError:
    """Tests for the ValidationError exception class."""

    def test_validation_error_is_exception(self):
        """ValidationError inherits from Exception."""
        assert issubclass(ValidationError, Exception)

    def test_validation_error_can_be_raised(self):
        """ValidationError can be raised with a message."""
        with pytest.raises(ValidationError, match="something went wrong"):
            raise ValidationError("something went wrong")

    def test_validation_error_empty_message(self):
        """ValidationError can be raised with an empty message."""
        with pytest.raises(ValidationError):
            raise ValidationError("")


# ---------------------------------------------------------------------------
# ConnectionValidator
# ---------------------------------------------------------------------------


class TestConnectionValidator:
    """Tests for ConnectionValidator static methods."""

    # -- validate_teradata_connection --

    def test_teradata_connection_success(self):
        """Valid parameters return (True, None)."""
        valid, error = ConnectionValidator.validate_teradata_connection(
            host="teradata.example.com",
            user="dbc",
            password="secret",
            database="prod_db",
            port=1025,
        )
        assert valid is True
        assert error is None

    def test_teradata_connection_missing_host(self):
        """Empty host returns failure."""
        valid, error = ConnectionValidator.validate_teradata_connection(
            host="", user="dbc", password="secret"
        )
        assert valid is False
        assert "required" in error.lower()

    def test_teradata_connection_missing_user(self):
        """Empty user returns failure."""
        valid, error = ConnectionValidator.validate_teradata_connection(
            host="td.example.com", user="", password="secret"
        )
        assert valid is False
        assert "required" in error.lower()

    def test_teradata_connection_missing_password(self):
        """Empty password returns failure."""
        valid, error = ConnectionValidator.validate_teradata_connection(
            host="td.example.com", user="dbc", password=""
        )
        assert valid is False
        assert "required" in error.lower()

    def test_teradata_connection_invalid_port_zero(self):
        """Port 0 is invalid."""
        valid, error = ConnectionValidator.validate_teradata_connection(
            host="td.example.com", user="dbc", password="secret", port=0
        )
        assert valid is False
        assert "port" in error.lower()

    def test_teradata_connection_invalid_port_too_high(self):
        """Port above 65535 is invalid."""
        valid, error = ConnectionValidator.validate_teradata_connection(
            host="td.example.com", user="dbc", password="secret", port=70000
        )
        assert valid is False
        assert "port" in error.lower()

    def test_teradata_connection_invalid_port_negative(self):
        """Negative port is invalid."""
        valid, error = ConnectionValidator.validate_teradata_connection(
            host="td.example.com", user="dbc", password="secret", port=-1
        )
        assert valid is False
        assert "port" in error.lower()

    def test_teradata_connection_default_port(self):
        """Default port 1025 is used when not specified."""
        valid, error = ConnectionValidator.validate_teradata_connection(
            host="td.example.com", user="dbc", password="secret"
        )
        assert valid is True
        assert error is None

    def test_teradata_connection_optional_database(self):
        """Database parameter is optional."""
        valid, error = ConnectionValidator.validate_teradata_connection(
            host="td.example.com", user="dbc", password="secret", database=None
        )
        assert valid is True
        assert error is None

    # -- validate_airflow_connection --

    def test_airflow_connection_success_http(self):
        """Valid http URL returns success."""
        valid, error = ConnectionValidator.validate_airflow_connection(
            base_url="http://localhost:8080"
        )
        assert valid is True
        assert error is None

    def test_airflow_connection_success_https(self):
        """Valid https URL returns success."""
        valid, error = ConnectionValidator.validate_airflow_connection(
            base_url="https://airflow.example.com"
        )
        assert valid is True
        assert error is None

    def test_airflow_connection_success_with_auth(self):
        """URL with username/password returns success."""
        valid, error = ConnectionValidator.validate_airflow_connection(
            base_url="http://localhost:8080", username="admin", password="admin"
        )
        assert valid is True
        assert error is None

    def test_airflow_connection_invalid_url(self):
        """URL without http/https prefix fails."""
        valid, error = ConnectionValidator.validate_airflow_connection(
            base_url="ftp://airflow.example.com"
        )
        assert valid is False
        assert "http" in error.lower()

    def test_airflow_connection_bare_hostname(self):
        """Bare hostname without protocol fails."""
        valid, error = ConnectionValidator.validate_airflow_connection(
            base_url="airflow.example.com"
        )
        assert valid is False
        assert "http" in error.lower()

    # -- validate_airbyte_connection --

    def test_airbyte_connection_success(self):
        """Valid http URL returns success."""
        valid, error = ConnectionValidator.validate_airbyte_connection(
            base_url="http://localhost:8000"
        )
        assert valid is True
        assert error is None

    def test_airbyte_connection_success_https(self):
        """Valid https URL returns success."""
        valid, error = ConnectionValidator.validate_airbyte_connection(
            base_url="https://airbyte.example.com"
        )
        assert valid is True
        assert error is None

    def test_airbyte_connection_invalid_url(self):
        """URL without http/https prefix fails."""
        valid, error = ConnectionValidator.validate_airbyte_connection(base_url="invalid-url")
        assert valid is False
        assert "http" in error.lower()

    def test_airbyte_connection_with_auth(self):
        """Username/password are accepted."""
        valid, error = ConnectionValidator.validate_airbyte_connection(
            base_url="http://localhost:8000", username="user", password="pass"
        )
        assert valid is True
        assert error is None

    # -- validate_dbt_installation --

    def test_dbt_installation_success(self):
        """Valid project dir with dbt_project.yml passes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "dbt_project.yml").write_text("name: test")
            valid, error = ConnectionValidator.validate_dbt_installation(project_dir=tmpdir)
            assert valid is True
            assert error is None

    def test_dbt_installation_missing_project_dir(self):
        """Non-existent project dir fails."""
        valid, error = ConnectionValidator.validate_dbt_installation(
            project_dir="/nonexistent/path/to/dbt/project"
        )
        assert valid is False
        assert "not found" in error.lower()

    def test_dbt_installation_missing_dbt_project_yml(self):
        """Project dir without dbt_project.yml fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            valid, error = ConnectionValidator.validate_dbt_installation(project_dir=tmpdir)
            assert valid is False
            assert "dbt_project.yml" in error

    def test_dbt_installation_with_profiles_dir(self):
        """Valid profiles_dir with profiles.yml passes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "dbt_project.yml").write_text("name: test")
            profiles_dir = Path(tmpdir) / "profiles"
            profiles_dir.mkdir()
            (profiles_dir / "profiles.yml").write_text("default: {}")
            valid, error = ConnectionValidator.validate_dbt_installation(
                project_dir=tmpdir, profiles_dir=str(profiles_dir)
            )
            assert valid is True
            assert error is None

    def test_dbt_installation_profiles_dir_missing(self):
        """Non-existent profiles_dir fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "dbt_project.yml").write_text("name: test")
            valid, error = ConnectionValidator.validate_dbt_installation(
                project_dir=tmpdir,
                profiles_dir="/nonexistent/profiles",
            )
            assert valid is False
            assert "not found" in error.lower()

    def test_dbt_installation_profiles_dir_missing_profiles_yml(self):
        """Profiles dir without profiles.yml fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "dbt_project.yml").write_text("name: test")
            profiles_dir = Path(tmpdir) / "profiles"
            profiles_dir.mkdir()
            valid, error = ConnectionValidator.validate_dbt_installation(
                project_dir=tmpdir, profiles_dir=str(profiles_dir)
            )
            assert valid is False
            assert "profiles.yml" in error


# ---------------------------------------------------------------------------
# to_quartz_cron
# ---------------------------------------------------------------------------


class TestToQuartzCron:
    """Tests for the to_quartz_cron conversion function."""

    def test_daily_at_2am(self):
        """Basic daily cron: '0 2 * * *' -> '0 0 2 * * ?'."""
        assert to_quartz_cron("0 2 * * *") == "0 0 2 * * ?"

    def test_every_15_minutes(self):
        """Every 15 min: '*/15 * * * *' -> '0 */15 * * * ?'."""
        assert to_quartz_cron("*/15 * * * *") == "0 */15 * * * ?"

    def test_weekdays_only(self):
        """Weekdays at 2 AM: '0 2 * * 1-5' -> '0 0 2 ? * 1-5' (dom becomes ?)."""
        assert to_quartz_cron("0 2 * * 1-5") == "0 0 2 ? * 1-5"

    def test_monthly_first_day(self):
        """Monthly on 1st: '0 0 1 * *' -> '0 0 0 1 * ?'."""
        assert to_quartz_cron("0 0 1 * *") == "0 0 0 1 * ?"

    def test_six_field_valid_passthrough(self):
        """Already-valid Quartz (6-field with ?) is returned as-is."""
        assert to_quartz_cron("0 0 2 * * ?") == "0 0 2 * * ?"

    def test_six_field_passthrough_with_whitespace(self):
        """Leading/trailing whitespace is stripped for 6-field input."""
        assert to_quartz_cron("  0 0 2 * * ?  ") == "0 0 2 * * ?"

    def test_six_field_dom_question_mark(self):
        """6-field with ? in dom passes through."""
        assert to_quartz_cron("0 0 2 ? * 1-5") == "0 0 2 ? * 1-5"

    def test_six_field_normalizes_missing_question_mark(self):
        """6-field with * * in dom/dow gets dow normalized to ?."""
        assert to_quartz_cron("0 0 2 * * *") == "0 0 2 * * ?"

    def test_six_field_normalizes_dow_specific(self):
        """6-field with dom=* and specific dow gets dom normalized to ?."""
        assert to_quartz_cron("0 0 2 * * 1-5") == "0 0 2 ? * 1-5"

    def test_six_field_normalizes_dom_specific(self):
        """6-field with specific dom and dow=* gets dow normalized to ?."""
        assert to_quartz_cron("0 0 0 15 * *") == "0 0 0 15 * ?"

    def test_six_field_both_non_wildcard_raises(self):
        """6-field with both dom and dow non-wildcard raises ValueError."""
        with pytest.raises(ValueError, match="does not support both day-of-month"):
            to_quartz_cron("0 0 2 15 * 1")

    def test_invalid_field_count_raises(self):
        """Non-5/6-field input raises ValueError."""
        with pytest.raises(ValueError, match="Expected 5 or 6 cron fields"):
            to_quartz_cron("bad")

    def test_too_few_fields_raises(self):
        """3-field input raises ValueError."""
        with pytest.raises(ValueError, match="got 3"):
            to_quartz_cron("0 0 *")

    def test_too_many_fields_raises(self):
        """7-field input raises ValueError."""
        with pytest.raises(ValueError, match="got 7"):
            to_quartz_cron("0 0 0 * * ? 2025")

    def test_both_dom_and_dow_specified_raises(self):
        """5-field with both dom and dow non-wildcard raises ValueError."""
        with pytest.raises(ValueError, match="does not support both day-of-month"):
            to_quartz_cron("0 2 15 * 1")


# ---------------------------------------------------------------------------
# PipelineValidator
# ---------------------------------------------------------------------------


class TestPipelineValidator:
    """Tests for PipelineValidator static methods."""

    # -- validate_pipeline_name --

    def test_pipeline_name_valid(self):
        """Simple alphanumeric name passes."""
        valid, error = PipelineValidator.validate_pipeline_name("my_pipeline_1")
        assert valid is True
        assert error is None

    def test_pipeline_name_with_hyphen(self):
        """Hyphenated name passes."""
        valid, error = PipelineValidator.validate_pipeline_name("etl-pipeline")
        assert valid is True
        assert error is None

    def test_pipeline_name_empty(self):
        """Empty name fails."""
        valid, error = PipelineValidator.validate_pipeline_name("")
        assert valid is False
        assert "empty" in error.lower()

    def test_pipeline_name_too_long(self):
        """Name exceeding 250 chars fails."""
        valid, error = PipelineValidator.validate_pipeline_name("a" * 251)
        assert valid is False
        assert "long" in error.lower()

    def test_pipeline_name_exactly_250(self):
        """Name of exactly 250 chars passes."""
        valid, error = PipelineValidator.validate_pipeline_name("a" * 250)
        assert valid is True
        assert error is None

    def test_pipeline_name_starts_with_number(self):
        """Name starting with a digit fails."""
        valid, error = PipelineValidator.validate_pipeline_name("1_pipeline")
        assert valid is False
        assert "start" in error.lower() or "letter" in error.lower()

    def test_pipeline_name_starts_with_underscore(self):
        """Name starting with underscore fails (must start with letter)."""
        valid, error = PipelineValidator.validate_pipeline_name("_pipeline")
        assert valid is False
        assert "letter" in error.lower()

    def test_pipeline_name_special_characters(self):
        """Name with spaces or special chars fails."""
        valid, error = PipelineValidator.validate_pipeline_name("my pipeline!")
        assert valid is False
        assert "letters" in error.lower() or "only contain" in error.lower()

    # -- validate_schedule --

    def test_schedule_preset_daily(self):
        """Preset @daily passes."""
        valid, error = PipelineValidator.validate_schedule("@daily")
        assert valid is True
        assert error is None

    def test_schedule_preset_once(self):
        """Preset @once passes."""
        valid, error = PipelineValidator.validate_schedule("@once")
        assert valid is True
        assert error is None

    def test_schedule_preset_hourly(self):
        """Preset @hourly passes."""
        valid, error = PipelineValidator.validate_schedule("@hourly")
        assert valid is True
        assert error is None

    def test_schedule_preset_weekly(self):
        """Preset @weekly passes."""
        valid, error = PipelineValidator.validate_schedule("@weekly")
        assert valid is True
        assert error is None

    def test_schedule_preset_monthly(self):
        """Preset @monthly passes."""
        valid, error = PipelineValidator.validate_schedule("@monthly")
        assert valid is True
        assert error is None

    def test_schedule_preset_yearly(self):
        """Preset @yearly passes."""
        valid, error = PipelineValidator.validate_schedule("@yearly")
        assert valid is True
        assert error is None

    def test_schedule_preset_continuous(self):
        """Preset @continuous passes."""
        valid, error = PipelineValidator.validate_schedule("@continuous")
        assert valid is True
        assert error is None

    def test_schedule_empty_string(self):
        """Empty string is a valid preset."""
        valid, error = PipelineValidator.validate_schedule("")
        assert valid is True
        assert error is None

    def test_schedule_none_string(self):
        """String 'None' is a valid preset."""
        valid, error = PipelineValidator.validate_schedule("None")
        assert valid is True
        assert error is None

    def test_schedule_valid_cron(self):
        """Valid 5-part cron expression passes."""
        valid, error = PipelineValidator.validate_schedule("0 0 * * *")
        assert valid is True
        assert error is None

    def test_schedule_valid_cron_complex(self):
        """Complex cron with ranges/steps passes."""
        valid, error = PipelineValidator.validate_schedule("*/15 0-6 1,15 * 1-5")
        assert valid is True
        assert error is None

    def test_schedule_invalid_cron_too_few_parts(self):
        """Cron with fewer than 5 parts fails."""
        valid, error = PipelineValidator.validate_schedule("0 0 *")
        assert valid is False
        assert "5 parts" in error.lower()

    def test_schedule_quartz_cron_rejected_by_default(self):
        """Quartz 6-field cron is rejected without allow_quartz (Airflow-strict)."""
        valid, error = PipelineValidator.validate_schedule("0 0 2 * * ?")
        assert valid is False
        assert "5 parts" in error.lower()

    def test_schedule_quartz_cron_accepted_with_flag(self):
        """Quartz 6-field cron passes when allow_quartz=True (Airbyte)."""
        valid, error = PipelineValidator.validate_schedule("0 0 2 * * ?", allow_quartz=True)
        assert valid is True
        assert error is None

    def test_schedule_invalid_cron_too_many_parts(self):
        """Cron with more than 6 parts fails even with allow_quartz."""
        valid, error = PipelineValidator.validate_schedule("0 0 * * * * *", allow_quartz=True)
        assert valid is False
        assert "5 parts" in error.lower() or "6 parts" in error.lower()

    def test_schedule_quartz_invalid_dom_dow_caught(self):
        """Quartz mode rejects both dom and dow non-wildcard at validation time."""
        valid, error = PipelineValidator.validate_schedule("0 2 15 * 1", allow_quartz=True)
        assert valid is False
        assert "day-of-month" in error.lower()

    def test_schedule_quartz_six_field_invalid_dom_dow_caught(self):
        """Quartz mode rejects 6-field with both dom and dow non-wildcard."""
        valid, error = PipelineValidator.validate_schedule("0 0 2 15 * 1", allow_quartz=True)
        assert valid is False
        assert "day-of-month" in error.lower()

    def test_schedule_invalid_cron_bad_chars(self):
        """Cron with alphabetic chars in non-preset format fails."""
        valid, error = PipelineValidator.validate_schedule("0 0 * * MON")
        assert valid is False
        assert "invalid" in error.lower()

    # -- validate_table_list --

    def test_table_list_valid(self):
        """Simple table names pass."""
        valid, error = PipelineValidator.validate_table_list(["customers", "orders"])
        assert valid is True
        assert error is None

    def test_table_list_empty(self):
        """Empty list fails."""
        valid, error = PipelineValidator.validate_table_list([])
        assert valid is False
        assert "empty" in error.lower()

    def test_table_list_empty_table_name(self):
        """List containing empty string fails."""
        valid, error = PipelineValidator.validate_table_list(["customers", ""])
        assert valid is False
        assert "empty" in error.lower()

    def test_table_list_sql_injection_semicolon(self):
        """Table name with semicolon fails."""
        valid, error = PipelineValidator.validate_table_list(["customers; DROP TABLE"])
        assert valid is False
        assert "invalid" in error.lower()

    def test_table_list_sql_injection_double_dash(self):
        """Table name with -- comment fails."""
        valid, error = PipelineValidator.validate_table_list(["customers--comment"])
        assert valid is False
        assert "invalid" in error.lower()

    def test_table_list_sql_injection_block_comment(self):
        """Table name with /* fails."""
        valid, error = PipelineValidator.validate_table_list(["customers/*"])
        assert valid is False
        assert "invalid" in error.lower()

    def test_table_list_invalid_format_with_dot(self):
        """Table name with dot fails regex."""
        valid, error = PipelineValidator.validate_table_list(["schema.table"])
        assert valid is False
        assert "invalid" in error.lower()

    def test_table_list_non_string_element(self):
        """Non-string element fails."""
        valid, error = PipelineValidator.validate_table_list([123])
        assert valid is False
        assert "string" in error.lower()

    # -- validate_pipeline_config --

    def test_pipeline_config_valid(self):
        """Complete valid config passes."""
        config = {
            "pipeline_name": "etl_pipeline",
            "source_database": "prod_db",
            "source_tables": ["customers", "orders"],
            "schedule": "@daily",
            "target_schema": "staging",
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is True
        assert errors == []

    def test_pipeline_config_missing_pipeline_name(self):
        """Missing pipeline_name returns error."""
        config = {
            "source_database": "prod_db",
            "source_tables": ["customers"],
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is False
        assert any("pipeline_name" in e for e in errors)

    def test_pipeline_config_missing_source_database(self):
        """Missing source_database returns error."""
        config = {
            "pipeline_name": "etl_pipeline",
            "source_tables": ["customers"],
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is False
        assert any("source_database" in e for e in errors)

    def test_pipeline_config_missing_source_tables(self):
        """Missing source_tables returns error."""
        config = {
            "pipeline_name": "etl_pipeline",
            "source_database": "prod_db",
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is False
        assert any("source_tables" in e for e in errors)

    def test_pipeline_config_missing_all_required(self):
        """Missing all required fields returns multiple errors."""
        valid, errors = PipelineValidator.validate_pipeline_config({})
        assert valid is False
        assert len(errors) == 3  # pipeline_name, source_database, source_tables

    def test_pipeline_config_invalid_pipeline_name(self):
        """Invalid pipeline name surfaces in errors."""
        config = {
            "pipeline_name": "123bad",
            "source_database": "prod_db",
            "source_tables": ["customers"],
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is False
        assert any("pipeline_name" in e for e in errors)

    def test_pipeline_config_invalid_schedule(self):
        """Invalid schedule surfaces in errors."""
        config = {
            "pipeline_name": "etl_pipeline",
            "source_database": "prod_db",
            "source_tables": ["customers"],
            "schedule": "bad schedule",
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is False
        assert any("schedule" in e for e in errors)

    def test_pipeline_config_invalid_source_tables(self):
        """Invalid table list surfaces in errors."""
        config = {
            "pipeline_name": "etl_pipeline",
            "source_database": "prod_db",
            "source_tables": [],
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is False
        assert any("source_tables" in e for e in errors)

    def test_pipeline_config_invalid_target_schema_empty(self):
        """Empty target_schema string surfaces in errors."""
        config = {
            "pipeline_name": "etl_pipeline",
            "source_database": "prod_db",
            "source_tables": ["customers"],
            "target_schema": "",
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is False
        assert any("target_schema" in e for e in errors)

    def test_pipeline_config_invalid_target_schema_non_string(self):
        """Non-string target_schema surfaces in errors."""
        config = {
            "pipeline_name": "etl_pipeline",
            "source_database": "prod_db",
            "source_tables": ["customers"],
            "target_schema": 123,
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is False
        assert any("target_schema" in e for e in errors)

    def test_pipeline_config_multiple_errors(self):
        """Multiple invalid fields accumulate all errors."""
        config = {
            "pipeline_name": "123bad",
            "source_database": "prod_db",
            "source_tables": [],
            "schedule": "bad schedule",
            "target_schema": "",
        }
        valid, errors = PipelineValidator.validate_pipeline_config(config)
        assert valid is False
        assert len(errors) >= 3


# ---------------------------------------------------------------------------
# DataValidator
# ---------------------------------------------------------------------------


class TestDataValidator:
    """Tests for DataValidator static methods."""

    # -- validate_column_name --

    def test_column_name_valid(self):
        """Simple alphanumeric column name passes."""
        valid, error = DataValidator.validate_column_name("customer_id")
        assert valid is True
        assert error is None

    def test_column_name_empty(self):
        """Empty name fails."""
        valid, error = DataValidator.validate_column_name("")
        assert valid is False
        assert "empty" in error.lower()

    def test_column_name_too_long(self):
        """Name exceeding 128 chars fails."""
        valid, error = DataValidator.validate_column_name("a" * 129)
        assert valid is False
        assert "long" in error.lower()

    def test_column_name_exactly_128(self):
        """Name of exactly 128 chars passes."""
        valid, error = DataValidator.validate_column_name("a" * 128)
        assert valid is True
        assert error is None

    def test_column_name_starts_with_number(self):
        """Name starting with digit fails."""
        valid, error = DataValidator.validate_column_name("1column")
        assert valid is False
        assert "start with letter" in error.lower()

    def test_column_name_special_characters(self):
        """Name with special chars fails."""
        valid, error = DataValidator.validate_column_name("col-name")
        assert valid is False

    def test_column_name_reserved_keyword_select(self):
        """Reserved keyword 'select' fails."""
        valid, error = DataValidator.validate_column_name("select")
        assert valid is False
        assert "reserved" in error.lower()

    def test_column_name_reserved_keyword_from(self):
        """Reserved keyword 'from' fails."""
        valid, error = DataValidator.validate_column_name("from")
        assert valid is False
        assert "reserved" in error.lower()

    def test_column_name_reserved_keyword_where(self):
        """Reserved keyword 'where' fails."""
        valid, error = DataValidator.validate_column_name("where")
        assert valid is False
        assert "reserved" in error.lower()

    def test_column_name_reserved_keyword_drop(self):
        """Reserved keyword 'drop' fails."""
        valid, error = DataValidator.validate_column_name("drop")
        assert valid is False
        assert "reserved" in error.lower()

    def test_column_name_reserved_keyword_case_insensitive(self):
        """Reserved keywords are checked case-insensitively."""
        valid, error = DataValidator.validate_column_name("SELECT")
        assert valid is False
        assert "reserved" in error.lower()

    # -- validate_data_type --

    def test_data_type_integer(self):
        """'integer' is valid."""
        valid, error = DataValidator.validate_data_type("integer")
        assert valid is True
        assert error is None

    def test_data_type_varchar_with_length(self):
        """'varchar(100)' is valid."""
        valid, error = DataValidator.validate_data_type("varchar(100)")
        assert valid is True
        assert error is None

    def test_data_type_decimal_with_precision(self):
        """'decimal(10,2)' is valid."""
        valid, error = DataValidator.validate_data_type("decimal(10,2)")
        assert valid is True
        assert error is None

    def test_data_type_timestamp(self):
        """'timestamp' is valid."""
        valid, error = DataValidator.validate_data_type("timestamp")
        assert valid is True
        assert error is None

    def test_data_type_boolean(self):
        """'boolean' is valid."""
        valid, error = DataValidator.validate_data_type("boolean")
        assert valid is True
        assert error is None

    def test_data_type_json(self):
        """'json' is valid."""
        valid, error = DataValidator.validate_data_type("json")
        assert valid is True
        assert error is None

    def test_data_type_empty(self):
        """Empty string fails."""
        valid, error = DataValidator.validate_data_type("")
        assert valid is False
        assert "empty" in error.lower()

    def test_data_type_unknown_but_valid_format(self):
        """Unknown type name matching parameterised pattern still passes."""
        # e.g. "NEWTYPE(10)" matches the regex fallback
        valid, error = DataValidator.validate_data_type("NEWTYPE(10)")
        assert valid is True
        assert error is None

    def test_data_type_completely_invalid(self):
        """Gibberish that doesn't match any pattern fails."""
        valid, error = DataValidator.validate_data_type("!!!invalid!!!")
        assert valid is False
        assert "invalid" in error.lower()

    # -- validate_row_count --

    def test_row_count_valid(self):
        """Count within range passes."""
        valid, error = DataValidator.validate_row_count(100)
        assert valid is True
        assert error is None

    def test_row_count_zero(self):
        """Zero is valid with default min_rows=0."""
        valid, error = DataValidator.validate_row_count(0)
        assert valid is True
        assert error is None

    def test_row_count_below_minimum(self):
        """Count below min_rows fails."""
        valid, error = DataValidator.validate_row_count(5, min_rows=10)
        assert valid is False
        assert "below minimum" in error.lower()

    def test_row_count_above_maximum(self):
        """Count above max_rows fails."""
        valid, error = DataValidator.validate_row_count(200, max_rows=100)
        assert valid is False
        assert "exceeds maximum" in error.lower()

    def test_row_count_exactly_at_minimum(self):
        """Count equal to min_rows passes."""
        valid, error = DataValidator.validate_row_count(10, min_rows=10)
        assert valid is True
        assert error is None

    def test_row_count_exactly_at_maximum(self):
        """Count equal to max_rows passes."""
        valid, error = DataValidator.validate_row_count(100, max_rows=100)
        assert valid is True
        assert error is None

    def test_row_count_non_integer(self):
        """Non-integer count fails."""
        valid, error = DataValidator.validate_row_count(10.5)
        assert valid is False
        assert "integer" in error.lower()

    # -- validate_null_rate --

    def test_null_rate_within_threshold(self):
        """Null rate below threshold passes."""
        valid, error = DataValidator.validate_null_rate(10, 100, max_null_rate=0.5)
        assert valid is True
        assert error is None

    def test_null_rate_exceeds_threshold(self):
        """Null rate above threshold fails."""
        valid, error = DataValidator.validate_null_rate(60, 100, max_null_rate=0.5)
        assert valid is False
        assert "exceeds" in error.lower()

    def test_null_rate_zero_total(self):
        """Zero total count fails."""
        valid, error = DataValidator.validate_null_rate(0, 0)
        assert valid is False
        assert "zero" in error.lower()

    def test_null_rate_zero_nulls(self):
        """Zero nulls passes."""
        valid, error = DataValidator.validate_null_rate(0, 100)
        assert valid is True
        assert error is None

    def test_null_rate_at_threshold(self):
        """Null rate exactly at threshold passes (50% with max 0.5)."""
        valid, error = DataValidator.validate_null_rate(50, 100, max_null_rate=0.5)
        assert valid is True
        assert error is None

    def test_null_rate_custom_threshold(self):
        """Custom threshold is respected."""
        valid, error = DataValidator.validate_null_rate(15, 100, max_null_rate=0.1)
        assert valid is False
        assert "exceeds" in error.lower()


# ---------------------------------------------------------------------------
# ConfigValidator
# ---------------------------------------------------------------------------


class TestConfigValidator:
    """Tests for ConfigValidator static methods."""

    # -- validate_environment_vars --

    @patch.dict(os.environ, {"MY_VAR": "value", "OTHER_VAR": "val2"}, clear=True)
    def test_env_vars_all_present(self):
        """All required vars are set."""
        valid, missing = ConfigValidator.validate_environment_vars(["MY_VAR", "OTHER_VAR"])
        assert valid is True
        assert missing == []

    @patch.dict(os.environ, {}, clear=True)
    def test_env_vars_all_missing(self):
        """None of the required vars are set."""
        valid, missing = ConfigValidator.validate_environment_vars(["MISSING_A", "MISSING_B"])
        assert valid is False
        assert "MISSING_A" in missing
        assert "MISSING_B" in missing

    @patch.dict(os.environ, {"PRESENT": "val"}, clear=True)
    def test_env_vars_partially_missing(self):
        """Some vars present, some missing."""
        valid, missing = ConfigValidator.validate_environment_vars(["PRESENT", "ABSENT"])
        assert valid is False
        assert "ABSENT" in missing
        assert "PRESENT" not in missing

    @patch.dict(os.environ, {}, clear=True)
    def test_env_vars_empty_list(self):
        """Empty required list means nothing to check, passes."""
        valid, missing = ConfigValidator.validate_environment_vars([])
        assert valid is True
        assert missing == []

    # -- validate_file_path --

    def test_file_path_valid_no_exist_check(self):
        """Any non-empty path passes without existence check."""
        valid, error = ConfigValidator.validate_file_path("/some/path/file.txt")
        assert valid is True
        assert error is None

    def test_file_path_empty(self):
        """Empty path fails."""
        valid, error = ConfigValidator.validate_file_path("")
        assert valid is False
        assert "empty" in error.lower()

    def test_file_path_must_exist_and_does(self):
        """Existing path with must_exist=True passes."""
        with tempfile.NamedTemporaryFile() as f:
            valid, error = ConfigValidator.validate_file_path(f.name, must_exist=True)
            assert valid is True
            assert error is None

    def test_file_path_must_exist_but_doesnt(self):
        """Non-existent path with must_exist=True fails."""
        valid, error = ConfigValidator.validate_file_path("/nonexistent/file.txt", must_exist=True)
        assert valid is False
        assert "not exist" in error.lower()

    # -- validate_port --

    def test_port_valid(self):
        """Typical port number passes."""
        valid, error = ConfigValidator.validate_port(8080)
        assert valid is True
        assert error is None

    def test_port_minimum(self):
        """Port 1 is valid."""
        valid, error = ConfigValidator.validate_port(1)
        assert valid is True
        assert error is None

    def test_port_maximum(self):
        """Port 65535 is valid."""
        valid, error = ConfigValidator.validate_port(65535)
        assert valid is True
        assert error is None

    def test_port_zero(self):
        """Port 0 is invalid."""
        valid, error = ConfigValidator.validate_port(0)
        assert valid is False
        assert "between 1 and 65535" in error

    def test_port_negative(self):
        """Negative port is invalid."""
        valid, error = ConfigValidator.validate_port(-1)
        assert valid is False
        assert "between 1 and 65535" in error

    def test_port_too_high(self):
        """Port above 65535 is invalid."""
        valid, error = ConfigValidator.validate_port(70000)
        assert valid is False
        assert "between 1 and 65535" in error

    def test_port_non_integer(self):
        """Non-integer port fails."""
        valid, error = ConfigValidator.validate_port("8080")
        assert valid is False
        assert "integer" in error.lower()

    def test_port_privileged_still_valid(self):
        """Privileged port (< 1024) is valid but logs warning."""
        valid, error = ConfigValidator.validate_port(80)
        assert valid is True
        assert error is None

    # -- validate_url --

    def test_url_valid_http(self):
        """Standard http URL passes."""
        valid, error = ConfigValidator.validate_url("http://example.com")
        assert valid is True
        assert error is None

    def test_url_valid_https(self):
        """Standard https URL passes."""
        valid, error = ConfigValidator.validate_url("https://example.com")
        assert valid is True
        assert error is None

    def test_url_valid_localhost(self):
        """Localhost URL passes."""
        valid, error = ConfigValidator.validate_url("http://localhost:8080")
        assert valid is True
        assert error is None

    def test_url_valid_ip(self):
        """IP address URL passes."""
        valid, error = ConfigValidator.validate_url("http://192.168.1.1:3000")
        assert valid is True
        assert error is None

    def test_url_valid_with_path(self):
        """URL with path passes."""
        valid, error = ConfigValidator.validate_url("http://example.com/api/v1")
        assert valid is True
        assert error is None

    def test_url_empty(self):
        """Empty URL fails."""
        valid, error = ConfigValidator.validate_url("")
        assert valid is False
        assert "empty" in error.lower()

    def test_url_no_protocol(self):
        """URL without protocol fails."""
        valid, error = ConfigValidator.validate_url("example.com")
        assert valid is False
        assert "invalid" in error.lower()

    def test_url_ftp_protocol(self):
        """FTP protocol URL fails."""
        valid, error = ConfigValidator.validate_url("ftp://example.com")
        assert valid is False
        assert "invalid" in error.lower()


# ---------------------------------------------------------------------------
# validate_all_connections (module-level function)
# ---------------------------------------------------------------------------


class TestValidateAllConnections:
    """Tests for the validate_all_connections module-level function."""

    def test_all_connections_valid(self):
        """All valid connections return overall valid=True."""
        config = {
            "teradata": {
                "host": "td.example.com",
                "user": "dbc",
                "password": "secret",
                "port": 1025,
            },
            "airflow": {
                "base_url": "http://localhost:8080",
            },
            "airbyte": {
                "base_url": "http://localhost:8000",
            },
        }
        result = validate_all_connections(config)
        assert result["valid"] is True
        assert result["checks"]["teradata"]["valid"] is True
        assert result["checks"]["airflow"]["valid"] is True
        assert result["checks"]["airbyte"]["valid"] is True

    def test_teradata_connection_invalid(self):
        """Invalid teradata config makes overall valid=False."""
        config = {
            "teradata": {
                "host": "",
                "user": "",
                "password": "",
            },
        }
        result = validate_all_connections(config)
        assert result["valid"] is False
        assert result["checks"]["teradata"]["valid"] is False
        assert result["checks"]["teradata"]["error"] is not None

    def test_airflow_connection_invalid(self):
        """Invalid airflow config makes overall valid=False."""
        config = {
            "airflow": {
                "base_url": "not-a-url",
            },
        }
        result = validate_all_connections(config)
        assert result["valid"] is False
        assert result["checks"]["airflow"]["valid"] is False

    def test_airbyte_connection_invalid(self):
        """Invalid airbyte config makes overall valid=False."""
        config = {
            "airbyte": {
                "base_url": "not-a-url",
            },
        }
        result = validate_all_connections(config)
        assert result["valid"] is False
        assert result["checks"]["airbyte"]["valid"] is False

    def test_dbt_connection_invalid(self):
        """Invalid dbt config makes overall valid=False."""
        config = {
            "dbt": {
                "project_dir": "/nonexistent/path",
            },
        }
        result = validate_all_connections(config)
        assert result["valid"] is False
        assert result["checks"]["dbt"]["valid"] is False

    def test_empty_config(self):
        """Empty config means no checks, overall valid=True."""
        result = validate_all_connections({})
        assert result["valid"] is True
        assert result["checks"] == {}

    def test_partial_failure(self):
        """Mix of valid and invalid connections."""
        config = {
            "teradata": {
                "host": "td.example.com",
                "user": "dbc",
                "password": "secret",
            },
            "airflow": {
                "base_url": "not-a-url",
            },
        }
        result = validate_all_connections(config)
        assert result["valid"] is False
        assert result["checks"]["teradata"]["valid"] is True
        assert result["checks"]["airflow"]["valid"] is False

    def test_result_structure(self):
        """Result dict has expected keys."""
        config = {
            "teradata": {
                "host": "td.example.com",
                "user": "dbc",
                "password": "secret",
            },
        }
        result = validate_all_connections(config)
        assert "valid" in result
        assert "checks" in result
        assert "teradata" in result["checks"]
        assert "valid" in result["checks"]["teradata"]
        assert "error" in result["checks"]["teradata"]


# ---------------------------------------------------------------------------
# validate_identifier
# ---------------------------------------------------------------------------


class TestValidateIdentifier:

    @pytest.mark.parametrize("value", ["my_dag", "test-table", "db.schema", "A123", "My Source"])
    def test_valid_identifiers(self, value):
        assert validate_identifier(value, "field") is None

    def test_empty_string(self):
        assert validate_identifier("", "field") is not None

    def test_spaces_only_rejected(self):
        err = validate_identifier("   ", "field")
        assert err is not None

    def test_colon_rejected(self):
        assert validate_identifier("a:b", "field") is not None

    def test_plus_rejected(self):
        assert validate_identifier("a+b", "field") is not None

    def test_null_bytes_rejected(self):
        assert validate_identifier("ok\x00bad", "field") is not None

    def test_exceeds_max_length(self):
        assert validate_identifier("a" * 257, "field") is not None

    def test_non_string_input(self):
        err = validate_identifier(123, "field")
        assert err is not None
        assert "string" in err


# ---------------------------------------------------------------------------
# validate_dag_run_id
# ---------------------------------------------------------------------------


class TestValidateDagRunId:

    @pytest.mark.parametrize(
        "value",
        [
            "manual__2026-03-17T06:23:58.138838+00:00",
            "scheduled__2025-03-23T06:00:00+00:00",
            "simple_run_id",
            "backfill__2025-01-01T00:00:00+00:00",
        ],
    )
    def test_valid_dag_run_ids(self, value):
        assert validate_dag_run_id(value) is None

    def test_empty_string(self):
        assert validate_dag_run_id("") is not None

    def test_null_bytes_rejected(self):
        assert validate_dag_run_id("ok\x00bad") is not None

    def test_exceeds_max_length(self):
        assert validate_dag_run_id("a" * 501) is not None

    def test_shell_injection_rejected(self):
        assert validate_dag_run_id('; rm -rf /') is not None

    def test_non_string_input(self):
        err = validate_dag_run_id(42)
        assert err is not None


# ---------------------------------------------------------------------------
# validate_airflow_identifier
# ---------------------------------------------------------------------------


class TestValidateAirflowIdentifier:

    @pytest.mark.parametrize("value", ["my_dag", "test-dag.v2", "pipeline_123"])
    def test_valid_airflow_ids(self, value):
        assert validate_airflow_identifier(value, "dag_id") is None

    def test_spaces_rejected(self):
        assert validate_airflow_identifier("my dag", "dag_id") is not None

    def test_empty_string(self):
        assert validate_airflow_identifier("", "dag_id") is not None

    def test_null_bytes_rejected(self):
        assert validate_airflow_identifier("ok\x00bad", "dag_id") is not None

    def test_colon_rejected(self):
        assert validate_airflow_identifier("a:b", "dag_id") is not None

    def test_non_string_input(self):
        err = validate_airflow_identifier(123, "dag_id")
        assert err is not None
        assert "string" in err


# ---------------------------------------------------------------------------
# slugify_dir_name
# ---------------------------------------------------------------------------


class TestSlugifyDirName:

    def test_lowercases_and_replaces_non_alnum(self):
        assert slugify_dir_name("Prod-TD.EU_West") == "prod_td_eu_west"

    def test_collapses_runs_of_separators(self):
        assert slugify_dir_name("a---b...c   d") == "a_b_c_d"

    def test_strips_leading_and_trailing_underscores(self):
        assert slugify_dir_name("---hello---") == "hello"

    def test_truncates_to_max_length(self):
        long = "a" * 100
        result = slugify_dir_name(long, max_length=48)
        assert len(result) == 48
        assert result == "a" * 48

    def test_truncate_does_not_leave_trailing_underscore(self):
        # 47 chars then a separator - truncation at 48 would leave "_" tail
        s = ("x" * 47) + "-tail"
        result = slugify_dir_name(s, max_length=48)
        assert not result.endswith("_")

    def test_empty_input_returns_empty_string(self):
        assert slugify_dir_name("") == ""

    def test_only_separators_returns_empty_string(self):
        assert slugify_dir_name("---...   ") == ""

    def test_non_string_input_returns_empty_string(self):
        assert slugify_dir_name(None) == ""  # type: ignore[arg-type]
        assert slugify_dir_name(42) == ""  # type: ignore[arg-type]

    def test_hostname_with_port(self):
        assert slugify_dir_name("td-prod.example.com:1025") == "td_prod_example_com_1025"

    def test_already_safe_passthrough(self):
        assert slugify_dir_name("td_prod") == "td_prod"

    def test_unicode_replaced(self):
        # Non-ASCII alphanumeric chars are not in [a-z0-9] so they get replaced
        result = slugify_dir_name("café_münchen")
        assert all(c in "abcdefghijklmnopqrstuvwxyz0123456789_" for c in result)
        assert result  # non-empty (the ASCII parts survive)
