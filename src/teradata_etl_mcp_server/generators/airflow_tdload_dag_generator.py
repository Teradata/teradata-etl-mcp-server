# ruff: noqa: S608
"""Airflow DAG generator for TdLoadOperator-based data loading.

This module generates Airflow DAG Python files for orchestrating
Teradata data loading using TdLoadOperator and validation using BteqOperator.
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment

from ..utils.file_operations import SafeFileWriter
from .escaping import escape_single_quoted, escape_triple_quoted

logger = logging.getLogger(__name__)


class AirflowTdLoadDAGGeneratorError(Exception):
    """Base exception for Airflow TdLoad DAG generator errors."""

    pass


class AirflowTdLoadDAGGenerator:
    """
    Airflow DAG generator for TdLoadOperator-based data loading.

    Generates Python DAG files with TdLoadOperator for file and table loading,
    and BteqOperator for validation scripts.
    """

    def __init__(self, dags_folder: Path):
        """Initialize the generator with output folder and safe file writer."""
        self.dags_folder = Path(dags_folder)
        self.dags_folder.mkdir(parents=True, exist_ok=True)
        self.env = Environment(autoescape=False)  # noqa: S701  # nosec B701 - generates Python code, not HTML

        # See AirflowDAGGenerator.__init__ for the rationale on
        # enable_backups=False and restrict_permissions=False.
        self.file_writer = SafeFileWriter(
            output_dir=self.dags_folder,
            keep_backups=5,
            validate_python=True,
            enable_backups=False,
            restrict_permissions=False,
        )

    @staticmethod
    def _convert_teradata_type_to_sql(
        col_type: str,
        length: int | None = None,
        precision: int | None = None,
        scale: int | None = None,
    ) -> str:
        """
        Convert Teradata type codes to full SQL type definitions.

        Args:
            col_type: Teradata type code (e.g., 'I', 'CV', 'DA')
            length: Column length for variable types
            precision: Numeric precision
            scale: Numeric scale

        Returns:
            Full SQL type definition
        """
        type_mapping = {
            "I": "INTEGER",
            "I1": "BYTEINT",
            "I2": "SMALLINT",
            "I8": "BIGINT",
            "F": "FLOAT",
            "D": "DECIMAL",
            "DA": "DATE",
            "AT": "TIME",
            "TS": "TIMESTAMP",
            "CF": "CHAR",
            "CV": "VARCHAR",
            "CO": "CLOB",
            "BF": "BYTE",
            "BV": "VARBYTE",
            "BO": "BLOB",
            "N": "NUMBER",
        }

        base_type = type_mapping.get(col_type)
        if base_type is None:
            # Unknown value — could be an already-expanded SQL type like
            # VARCHAR(1000) or DECIMAL(15,2).  Validate via _sanitize_col_type
            # (which accepts alphanumeric names and optional (N) / (N,N)
            # suffixes) before falling back to VARCHAR(255).
            validated = AirflowTdLoadDAGGenerator._sanitize_col_type(col_type)
            if validated != col_type.strip():
                logger.warning(
                    "Unknown Teradata type code %r, defaulting to %s",
                    col_type,
                    validated,
                )
            return validated

        # Add length/precision for types that need it
        if base_type == "VARCHAR" and length:
            return f"VARCHAR({int(length)})"
        elif base_type == "CHAR" and length:
            return f"CHAR({int(length)})"
        elif base_type == "DECIMAL" and precision:
            if scale:
                return f"DECIMAL({int(precision)},{int(scale)})"
            return f"DECIMAL({int(precision)})"
        elif base_type == "VARBYTE" and length:
            return f"VARBYTE({int(length)})"
        elif base_type == "BYTE" and length:
            return f"BYTE({int(length)})"

        return base_type

    @staticmethod
    def _sanitize_identifier(name: str) -> str:
        """Validate a SQL identifier to prevent injection.

        Allows alphanumeric characters, underscores, hash, and dollar signs
        (common in Teradata). Rejects anything that could enable SQL injection.
        """
        if not name:
            raise AirflowTdLoadDAGGeneratorError("SQL identifier cannot be empty")
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_#$")
        if not all(c in allowed for c in name):
            bad = [c for c in name if c not in allowed]
            raise AirflowTdLoadDAGGeneratorError(
                f"Invalid SQL identifier: {name!r}. Disallowed characters: {bad}"
            )
        if name[0].isdigit():
            raise AirflowTdLoadDAGGeneratorError(
                f"Invalid SQL identifier: {name!r}. Cannot start with a digit."
            )
        return name

    @staticmethod
    def _sanitize_task_id(name: str, max_length: int = 64) -> str:
        """Normalize a string into a valid Airflow task_id ([A-Za-z0-9_]+)."""
        sanitized = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
        # Collapse consecutive underscores and strip leading/trailing
        while "__" in sanitized:
            sanitized = sanitized.replace("__", "_")
        sanitized = sanitized.strip("_")
        if not sanitized:
            sanitized = "task"
        # Ensure it doesn't start with a digit
        if sanitized[0].isdigit():
            sanitized = f"t_{sanitized}"
        return sanitized[:max_length]

    # Valid SQL type pattern: type name optionally followed by (N) or (N,N)
    _VALID_COL_TYPE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_ ]*(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?$")

    @classmethod
    def _sanitize_col_type(cls, col_type: str, fallback: str = "VARCHAR(255)") -> str:
        """Validate a column type string against an allowlist pattern.

        Accepts standard SQL types like VARCHAR(255), DECIMAL(15,2), INTEGER,
        TIMESTAMP, etc.  Rejects anything that could enable injection through
        column metadata.
        """
        col_type = col_type.strip()
        if not col_type:
            return fallback
        if cls._VALID_COL_TYPE_RE.match(col_type):
            return col_type
        logger.warning("Invalid column type %r, using fallback %r", col_type, fallback)
        return fallback

    # -------------------------------------------------------------------------
    # Jinja2 Templates
    # -------------------------------------------------------------------------

    DAG_TEMPLATE = '''\
"""
{{ dag_description }}

Generated: {{ generation_timestamp }}
DAG Type: Teradata Data Loading with TdLoadOperator
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
{% if use_tdload %}from airflow.providers.teradata.operators.tpt import TdLoadOperator
{% endif %}{% if use_bteq %}from airflow.providers.teradata.operators.bteq import BteqOperator
{% endif %}{% if use_python %}from airflow.providers.standard.operators.python import PythonOperator
{% endif %}{% if use_bash %}from airflow.providers.standard.operators.bash import BashOperator
{% endif %}

# Default arguments
default_args = {
    'owner': '{{ owner }}',
    'depends_on_past': {{ depends_on_past }},
    'email_on_failure': {{ email_on_failure }},
    'email_on_retry': {{ email_on_retry }},
    'retries': {{ retries }},
    'retry_delay': timedelta(minutes={{ retry_delay_minutes }}){% if execution_timeout_minutes %},
    'execution_timeout': timedelta(minutes={{ execution_timeout_minutes }}){% endif %}
}

{% if params %}
# DAG parameters
params = {{ params }}
{% endif %}

# DAG definition
with DAG(
    dag_id='{{ dag_id }}',
    default_args=default_args,
    description='{{ dag_description }}',
    schedule={{ schedule_interval | safe }},
    start_date=datetime({{ start_date_year }}, {{ start_date_month }}, {{ start_date_day }}),
    catchup={{ catchup }},
    tags={{ tags }},
    max_active_runs={{ max_active_runs }}{% if params %},
    params=params{% endif %}
) as dag:
    {% if doc_md %}
    dag.doc_md = """{{ doc_md }}"""
    {% endif %}

    # Tasks
{{ tasks_code | indent(4, True) }}

    # Task dependencies
{{ dependencies_code | indent(4, True) }}
'''

    TDLOAD_FILE_TASK_TEMPLATE = """\
{{ task_id }} = TdLoadOperator(
    task_id='{{ task_id }}',
    source_file_name={{ source_file_name_repr }},
    target_table='{{ target_table }}',
    source_format='{{ source_format }}',
    {% if source_text_delimiter_repr is defined and source_text_delimiter_repr is not none %}source_text_delimiter={{ source_text_delimiter_repr }},
    {% endif %}{% if tdload_options_repr is defined and tdload_options_repr is not none %}tdload_options={{ tdload_options_repr }},
    {% endif %}teradata_conn_id='{{ teradata_conn_id }}'{% if ssh_conn_id is defined and ssh_conn_id %},
    ssh_conn_id='{{ ssh_conn_id }}'{% endif %}
)"""

    CREATE_TABLE_TASK_TEMPLATE = """{{ task_id }} = BteqOperator(
    task_id='{{ task_id }}',
    sql='''{{ sql }}''',
    teradata_conn_id='{{ teradata_conn_id }}',
    bteq_script_encoding='UTF8'
)"""

    TDLOAD_TABLE_TASK_TEMPLATE = """\
{{ task_id }} = TdLoadOperator(
    task_id='{{ task_id }}',
    source_table='{{ source_table }}',
    target_table='{{ target_table }}',
    teradata_conn_id='{{ source_teradata_conn_id }}',
    target_teradata_conn_id='{{ target_teradata_conn_id }}'{% if ssh_conn_id is defined and ssh_conn_id %},
    ssh_conn_id='{{ ssh_conn_id }}'{% endif %}
)"""

    BTEQ_VALIDATION_TASK_TEMPLATE = '''\
{{ task_id }} = BteqOperator(
    task_id='{{ task_id }}',
    {% if sql is defined and sql %}sql="""{{ sql }}""",
    {% endif %}{% if file_path is defined and file_path %}file_path='{{ file_path }}',
    {% endif %}teradata_conn_id='{{ teradata_conn_id }}'{% if bteq_session_encoding is defined and bteq_session_encoding %},
    bteq_session_encoding='{{ bteq_session_encoding }}'{% endif %}{% if bteq_script_encoding is defined and bteq_script_encoding %},
    bteq_script_encoding='{{ bteq_script_encoding }}'{% endif %}{% if bteq_quit_rc is defined and bteq_quit_rc is not none %},
    bteq_quit_rc={{ bteq_quit_rc }}{% endif %}
)'''

    DBT_TASK_TEMPLATE = """{{ task_id }} = BashOperator(
    task_id='{{ task_id }}',
    bash_command='{{ dbt_command }}',
    {% if cwd %}cwd='{{ cwd }}',
    {% endif %}
)"""

    # -------------------------------------------------------------------------
    # File Loading DAG Generation
    # -------------------------------------------------------------------------

    def generate_file_loading_dag(
        self,
        dag_id: str,
        description: str,
        source_file_path: str,
        target_database: str,
        target_table: str,
        delimiter: str = ",",
        source_format: str = "Delimited",
        teradata_conn_id: str = "teradata_default",
        ssh_conn_id: str = "teradata_ssh_default",
        schedule: str | None = None,
        start_date: datetime | None = None,
        validation_queries: list[dict[str, Any]] | None = None,
        validation_bteq_file: str | None = None,
        owner: str = "data_engineer",
        tags: list[str] | None = None,
        retries: int = 2,
        retry_delay_minutes: int = 5,
        output_filename: str | None = None,
        doc_md: str | None = None,
        columns: list[dict[str, Any]] | None = None,
        skip_rows: int = 0,
    ) -> str:
        """
        Generate Airflow DAG for loading file to Teradata using TdLoadOperator.

        Args:
            dag_id: DAG identifier
            description: DAG description
            source_file_path: Path to source CSV/data file
            target_database: Target Teradata database/schema
            target_table: Target Teradata table name
            delimiter: Single-character field delimiter (default: comma).
                Any character is accepted except newline and null.
            source_format: Source file format (default: 'Delimited')
            teradata_conn_id: Airflow connection ID for Teradata
            ssh_conn_id: Airflow connection ID for SSH
            schedule: Cron expression or preset (@daily, @hourly, etc.)
            start_date: DAG start date
            validation_queries: List of validation SQL queries
            validation_bteq_file: Path to BTEQ validation script file
            owner: DAG owner
            tags: List of tags
            retries: Number of retries for failed tasks
            retry_delay_minutes: Delay between retries in minutes
            output_filename: Optional output filename
            doc_md: Optional markdown documentation
            columns: Optional list of column definitions for CREATE TABLE
            skip_rows: Number of header rows to skip (default: 0, set to 1 for CSV with headers)

        Returns:
            Generated DAG Python code as string
        """
        try:
            # Validate identifiers to prevent SQL injection
            target_database = self._sanitize_identifier(target_database)
            target_table = self._sanitize_identifier(target_table)

            start_date = start_date or datetime(2024, 1, 1)
            tasks = []
            dependencies = []

            # Start task
            tasks.append({"task_id": "start", "type": "empty"})

            # Create database task
            create_database_sql = f"""
                -- Check if database exists
                SELECT DatabaseName FROM DBC.DatabasesV WHERE DatabaseName = '{target_database}';

                -- If database exists (ACTIVITYCOUNT > 0), skip creation
                .IF ACTIVITYCOUNT > 0 THEN .GOTO DB_EXISTS;
                .IF ACTIVITYCOUNT = 0 THEN .GOTO CREATE_DB;

                .LABEL CREATE_DB
                -- Create database if it doesn't exist
                CREATE DATABASE {target_database} AS PERMANENT = 120e6, SPOOL = 120e6;
                .IF ERRORCODE <> 0 THEN .QUIT 12;
                .REMARK 'Database created successfully';
                .GOTO END_SCRIPT;

                .LABEL DB_EXISTS
                .REMARK 'Database {target_database} already exists - skipping creation';

                .LABEL END_SCRIPT
                .LOGOFF;
                .QUIT;
                """
            create_database_task = {
                "task_id": "create_database_if_not_exists",
                "type": "bteq_validation",
                "sql": create_database_sql,
                "teradata_conn_id": teradata_conn_id,
                "bteq_script_encoding": "UTF8",
            }
            tasks.append(create_database_task)
            dependencies.append(("start", "create_database_if_not_exists"))

            # Backup and prepare table task - handles both new and existing tables
            if columns and len(columns) > 0:
                column_defs = []
                for col in columns:
                    col_name = self._sanitize_identifier(col.get("name", ""))
                    col_type = self._sanitize_col_type(
                        col.get("type") or col.get("inferred_teradata_type", "VARCHAR(255)")
                    )
                    nullable = "" if col.get("nullable", True) else "NOT NULL"
                    column_defs.append(f"    {col_name} {col_type} {nullable}".strip())

                column_definitions = ",\n".join(column_defs)
                primary_index_col = self._sanitize_identifier(columns[0].get("name", ""))
            else:
                column_definitions = "    dummy_col VARCHAR(1)"
                primary_index_col = "dummy_col"

            backup_and_prepare_sql = f"""
                -- Check if table exists
                SELECT TableName FROM DBC.TablesV
                WHERE DatabaseName = '{target_database}' AND TableName = '{target_table}';

                -- If table exists (ACTIVITYCOUNT > 0), create backup and truncate
                .IF ACTIVITYCOUNT > 0 THEN .GOTO TABLE_EXISTS;
                .IF ACTIVITYCOUNT = 0 THEN .GOTO CREATE_NEW;

                .LABEL TABLE_EXISTS
                -- Drop existing backup table if present (suppress 3807 if not found)
                .SET ERRORLEVEL 3807 SEVERITY 0
                DROP TABLE {target_database}.{target_table}_bkp;
                .SET ERRORLEVEL 3807 SEVERITY 8

                -- Create backup from existing table
                CREATE TABLE {target_database}.{target_table}_bkp AS {target_database}.{target_table} WITH DATA;
                .IF ERRORCODE <> 0 THEN .QUIT 12;
                .REMARK 'Backup table created: {target_table}_bkp';

                -- Truncate existing table
                DELETE FROM {target_database}.{target_table} ALL;
                .IF ERRORCODE <> 0 THEN .QUIT 12;
                .REMARK 'Existing table truncated';

                .GOTO END_SCRIPT;

                .LABEL CREATE_NEW
                -- Create table if it doesn't exist
                CREATE MULTISET TABLE {target_database}.{target_table}, NO FALLBACK,
                    NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO
                    (
                {column_definitions}
                    )
                PRIMARY INDEX ({primary_index_col});
                .IF ERRORCODE <> 0 THEN .QUIT 12;
                .REMARK 'New table created successfully';

                .LABEL END_SCRIPT
                .LOGOFF;
                .QUIT;
                """
            backup_and_prepare_task = {
                "task_id": "backup_and_prepare_table",
                "type": "create_table",
                "sql": backup_and_prepare_sql,
                "teradata_conn_id": teradata_conn_id,
            }
            tasks.append(backup_and_prepare_task)
            dependencies.append(("create_database_if_not_exists", "backup_and_prepare_table"))

            # Build tdload_options string for advanced TPT parameters
            tdload_options_parts = []
            logger.debug("Building tdload_options for TPT parameters")
            if skip_rows > 0:
                logger.debug("skip_rows: %d", skip_rows)
                tdload_options_parts.append(f"--FileReaderSkipRows {skip_rows}")

            tdload_options = " ".join(tdload_options_parts) if tdload_options_parts else None
            if tdload_options:
                logger.debug("tdload_options: %s", tdload_options)

            # TdLoad task for file loading
            tdload_task = {
                "task_id": "load_file_to_teradata",
                "type": "tdload_file",
                "source_file_name": source_file_path,
                "target_table": f"{target_database}.{target_table}",
                "source_format": source_format,
                "source_text_delimiter": delimiter,
                "tdload_options": tdload_options,
                "teradata_conn_id": teradata_conn_id,
                "ssh_conn_id": ssh_conn_id,
            }
            tasks.append(tdload_task)
            dependencies.append(("backup_and_prepare_table", "load_file_to_teradata"))

            # Validation tasks + end task
            val_tasks, val_deps, last_ids = self._build_validation_tasks_and_deps(
                validation_queries,
                validation_bteq_file,
                default_conn_id=teradata_conn_id,
                upstream_task_id="load_file_to_teradata",
                file_task_id="validate_load",
            )
            tasks.extend(val_tasks)
            dependencies.extend(val_deps)
            tasks.append({"task_id": "end", "type": "empty"})
            for tid in last_ids:
                dependencies.append((tid, "end"))

            # Generate DAG
            return self._generate_dag_internal(
                dag_id=dag_id,
                description=description,
                schedule=schedule,
                tasks=tasks,
                dependencies=dependencies,
                start_date=start_date,
                owner=owner,
                tags=tags or ["teradata", "data_loading", "tdload"],
                retries=retries,
                retry_delay_minutes=retry_delay_minutes,
                output_filename=output_filename,
                doc_md=doc_md,
            )

        except Exception as e:
            logger.error("Failed to generate file loading DAG: %s", e, exc_info=True)
            raise AirflowTdLoadDAGGeneratorError(f"File loading DAG generation failed: {e}") from e

    # -------------------------------------------------------------------------
    # Table Transfer DAG Generation
    # -------------------------------------------------------------------------

    def generate_table_transfer_dag(
        self,
        dag_id: str,
        description: str,
        source_database: str,
        source_table: str,
        target_database: str,
        target_table: str,
        source_metadata: dict[str, Any] | None = None,
        source_teradata_conn_id: str = "teradata_source",
        target_teradata_conn_id: str = "teradata_target",
        ssh_conn_id: str = "teradata_ssh_default",
        schedule: str | None = None,
        start_date: datetime | None = None,
        validation_queries: list[dict[str, Any]] | None = None,
        validation_bteq_file: str | None = None,
        owner: str = "data_engineer",
        tags: list[str] | None = None,
        retries: int = 2,
        retry_delay_minutes: int = 5,
        output_filename: str | None = None,
        doc_md: str | None = None,
    ) -> str:
        """
        Generate Airflow DAG for transferring data between Teradata tables.

        Args:
            dag_id: DAG identifier
            description: DAG description
            source_database: Source Teradata database/schema
            source_table: Source Teradata table name
            target_database: Target Teradata database/schema
            target_table: Target Teradata table name
            source_metadata: Optional source table metadata from describe_table
            source_teradata_conn_id: Airflow connection ID for source Teradata
            target_teradata_conn_id: Airflow connection ID for target Teradata
            ssh_conn_id: Airflow connection ID for SSH
            schedule: Cron expression or preset
            start_date: DAG start date
            validation_queries: List of validation SQL queries
            validation_bteq_file: Path to BTEQ validation script file
            owner: DAG owner
            tags: List of tags
            retries: Number of retries
            retry_delay_minutes: Delay between retries in minutes
            output_filename: Optional output filename
            doc_md: Optional markdown documentation

        Returns:
            Generated DAG Python code as string
        """
        try:
            # Validate identifiers to prevent SQL injection
            source_database = self._sanitize_identifier(source_database)
            source_table = self._sanitize_identifier(source_table)
            target_database = self._sanitize_identifier(target_database)
            target_table = self._sanitize_identifier(target_table)

            start_date = start_date or datetime(2024, 1, 1)
            tasks = []
            dependencies = []

            # Start task
            tasks.append({"task_id": "start", "type": "empty"})

            # Cleanup source error tables
            cleanup_source_task = {
                "task_id": "cleanup_source_error_tables",
                "type": "bteq_validation",
                "sql": f"""
                    -- Drop TPT error tables for source table
                    -- Suppress error 3807 (object doesn't exist) to handle missing tables gracefully

                    .SET ERRORLEVEL 3807 SEVERITY 0

                    DROP TABLE {source_database}.{source_table}_ERR_1;
                    DROP TABLE {source_database}.{source_table}_ERR_2;
                    DROP TABLE {source_database}.{source_table}_ET;
                    DROP TABLE {source_database}.{source_table}_WT;
                    DROP TABLE {source_database}.{source_table}_UV;

                    .SET ERRORLEVEL 3807 SEVERITY 8

                    .REMARK 'Source error tables cleanup completed';
                    """,
                "teradata_conn_id": source_teradata_conn_id,
                "bteq_script_encoding": "UTF8",
            }
            tasks.append(cleanup_source_task)
            dependencies.append(("start", "cleanup_source_error_tables"))

            # Cleanup target error tables
            cleanup_target_task = {
                "task_id": "cleanup_target_error_tables",
                "type": "bteq_validation",
                "sql": f"""
                    -- Drop TPT error tables for target table
                    -- Suppress error 3807 (object doesn't exist) to handle missing tables gracefully

                    .SET ERRORLEVEL 3807 SEVERITY 0

                    DROP TABLE {target_database}.{target_table}_ERR_1;
                    DROP TABLE {target_database}.{target_table}_ERR_2;
                    DROP TABLE {target_database}.{target_table}_ET;
                    DROP TABLE {target_database}.{target_table}_WT;
                    DROP TABLE {target_database}.{target_table}_UV;

                    .SET ERRORLEVEL 3807 SEVERITY 8

                    .REMARK 'Target error tables cleanup completed';
                    """,
                "teradata_conn_id": target_teradata_conn_id,
                "bteq_script_encoding": "UTF8",
            }
            tasks.append(cleanup_target_task)
            dependencies.append(("start", "cleanup_target_error_tables"))

            # Create target table if it doesn't exist
            if source_metadata and source_metadata.get("columns"):
                # Build DDL from metadata
                columns = source_metadata.get("columns", [])
                column_defs = []
                for col in columns:
                    col_name = self._sanitize_identifier(col.get("name", ""))
                    col_type_code = col.get("type", "VARCHAR(255)")
                    col_length = col.get("length")
                    col_precision = col.get("precision")
                    col_scale = col.get("scale")

                    full_type = self._sanitize_col_type(
                        self._convert_teradata_type_to_sql(
                            col_type_code, col_length, col_precision, col_scale
                        )
                    )

                    nullable = "" if col.get("nullable", True) else "NOT NULL"
                    column_defs.append(f"        {col_name} {full_type} {nullable}".strip())

                column_definitions = ",\n".join(column_defs)
                primary_index_col = (
                    self._sanitize_identifier(columns[0].get("name", ""))
                    if columns
                    else "first_column"
                )

                create_table_sql = f"""
                    -- Check if target table exists
                    SELECT TableName FROM DBC.TablesV
                    WHERE DatabaseName = '{target_database}' AND TableName = '{target_table}';

                    -- If table doesn't exist, go to create
                    .IF ACTIVITYCOUNT = 0 THEN .GOTO CREATE_TABLE;

                    -- Table exists, check for schema mismatch (returns 1 row if mismatch, 0 if match)
                    SELECT 1
                    WHERE (SELECT COUNT(*) FROM DBC.ColumnsV WHERE DatabaseName = '{source_database}' AND TableName = '{source_table}')
                    <> (SELECT COUNT(*) FROM DBC.ColumnsV WHERE DatabaseName = '{target_database}' AND TableName = '{target_table}');

                    -- If column counts match (no rows returned), skip recreation
                    .IF ACTIVITYCOUNT = 0 THEN .GOTO TABLE_EXISTS;

                    -- Schema mismatch detected - backup and recreate
                    .REMARK 'Schema mismatch detected - backing up and recreating table';

                    -- Suppress error if backup table doesn't exist
                    .SET ERRORLEVEL 3807 SEVERITY 0
                    DROP TABLE {target_database}.{target_table}_bkp;
                    .SET ERRORLEVEL 3807 SEVERITY 8

                    -- Create backup from existing table
                    CREATE TABLE {target_database}.{target_table}_bkp AS {target_database}.{target_table} WITH DATA;
                    .IF ERRORCODE <> 0 THEN .QUIT 12;
                    .REMARK 'Backup table created: {target_table}_bkp';

                    -- Drop existing table
                    DROP TABLE {target_database}.{target_table};
                    .IF ERRORCODE <> 0 THEN .QUIT 12;
                    .REMARK 'Existing table dropped after backup';

                    .LABEL CREATE_TABLE
                    -- Create table with explicit DDL from source metadata
                    .REMARK 'Creating target table with schema from source metadata';
                    CREATE MULTISET TABLE {target_database}.{target_table}, NO FALLBACK,
                        NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO
                        (
                    {column_definitions}
                        )
                    PRIMARY INDEX ({primary_index_col});
                    .IF ERRORCODE <> 0 THEN .QUIT 12;
                    .REMARK 'Target table created successfully with explicit DDL';
                    .GOTO END_SCRIPT;

                    .LABEL TABLE_EXISTS
                    .REMARK 'Target table {target_database}.{target_table} already exists with matching schema - skipping creation';

                    .LABEL END_SCRIPT
                    .LOGOFF;
                    .QUIT;
                    """
            else:
                # Fallback to CREATE TABLE AS if metadata not available
                create_table_sql = f"""
                    -- Check if target table exists
                    SELECT TableName FROM DBC.TablesV
                    WHERE DatabaseName = '{target_database}' AND TableName = '{target_table}';

                    -- If table doesn't exist, go to create
                    .IF ACTIVITYCOUNT = 0 THEN .GOTO CREATE_TABLE;
                    .IF ACTIVITYCOUNT > 0 THEN .GOTO TABLE_EXISTS;

                    .LABEL CREATE_TABLE
                    -- Create table from source table schema (fallback method)
                    .REMARK 'Creating target table using CREATE TABLE AS from source';
                    CREATE TABLE {target_database}.{target_table} AS
                    {source_database}.{source_table}
                    WITH NO DATA;
                    .IF ERRORCODE <> 0 THEN .QUIT 12;
                    .REMARK 'Target table created successfully';
                    .GOTO END_SCRIPT;

                    .LABEL TABLE_EXISTS
                    .REMARK 'Target table {target_database}.{target_table} already exists - skipping creation';

                    .LABEL END_SCRIPT
                    .LOGOFF;
                    .QUIT;
                    """

            create_table_task = {
                "task_id": "create_target_table_if_not_exists",
                "type": "bteq_validation",
                "sql": create_table_sql,
                "teradata_conn_id": target_teradata_conn_id,
                "bteq_script_encoding": "UTF8",
            }
            tasks.append(create_table_task)
            dependencies.append(
                ("cleanup_source_error_tables", "create_target_table_if_not_exists")
            )
            dependencies.append(
                ("cleanup_target_error_tables", "create_target_table_if_not_exists")
            )

            # TdLoad task for table transfer
            tdload_task = {
                "task_id": "transfer_table_data",
                "type": "tdload_table",
                "source_table": f"{source_database}.{source_table}",
                "target_table": f"{target_database}.{target_table}",
                "source_teradata_conn_id": source_teradata_conn_id,
                "target_teradata_conn_id": target_teradata_conn_id,
                "ssh_conn_id": ssh_conn_id,
            }
            tasks.append(tdload_task)
            dependencies.append(("create_target_table_if_not_exists", "transfer_table_data"))

            # Validation tasks + end task
            val_tasks, val_deps, last_ids = self._build_validation_tasks_and_deps(
                validation_queries,
                validation_bteq_file,
                default_conn_id=target_teradata_conn_id,
                upstream_task_id="transfer_table_data",
                file_task_id="validate_transfer",
            )
            tasks.extend(val_tasks)
            dependencies.extend(val_deps)
            tasks.append({"task_id": "end", "type": "empty"})
            for tid in last_ids:
                dependencies.append((tid, "end"))

            # Generate DAG
            return self._generate_dag_internal(
                dag_id=dag_id,
                description=description,
                schedule=schedule,
                tasks=tasks,
                dependencies=dependencies,
                start_date=start_date,
                owner=owner,
                tags=tags or ["teradata", "data_transfer", "tdload"],
                retries=retries,
                retry_delay_minutes=retry_delay_minutes,
                output_filename=output_filename,
                doc_md=doc_md,
            )

        except Exception as e:
            logger.error("Failed to generate table transfer DAG: %s", e, exc_info=True)
            raise AirflowTdLoadDAGGeneratorError(
                f"Table transfer DAG generation failed: {e}"
            ) from e

    # -------------------------------------------------------------------------
    # Internal Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _build_validation_tasks_and_deps(
        validation_queries: list[dict[str, Any]] | None,
        validation_bteq_file: str | None,
        default_conn_id: str,
        upstream_task_id: str,
        file_task_id: str = "validate_load",
    ) -> tuple[list[dict[str, Any]], list[tuple[str, str]], list[str]]:
        """Build validation tasks, dependencies, and end-task wiring.

        Returns:
            (tasks, dependencies, last_task_ids) where last_task_ids are the
            tasks that should connect to the 'end' node.  If no validations
            are requested, last_task_ids contains only upstream_task_id.
        """
        tasks: list[dict[str, Any]] = []
        dependencies: list[tuple[str, str]] = []
        last_task_ids: list[str] = []

        if not (validation_queries or validation_bteq_file):
            return tasks, dependencies, [upstream_task_id]

        if validation_bteq_file:
            task = {
                "task_id": file_task_id,
                "type": "bteq_validation",
                "file_path": validation_bteq_file,
                "teradata_conn_id": default_conn_id,
                "bteq_script_encoding": "UTF8",
            }
            tasks.append(task)
            dependencies.append((upstream_task_id, file_task_id))
            last_task_ids.append(file_task_id)
        elif validation_queries:
            for idx, query_def in enumerate(validation_queries):
                task_id = f"validate_{idx + 1}_{AirflowTdLoadDAGGenerator._sanitize_task_id(query_def.get('name', 'check'))}"
                task = {
                    "task_id": task_id,
                    "type": "bteq_validation",
                    "sql": query_def.get("sql"),
                    "teradata_conn_id": query_def.get("teradata_conn_id", default_conn_id),
                    "bteq_script_encoding": "UTF8",
                }
                tasks.append(task)
                dependencies.append((upstream_task_id, task_id))
                last_task_ids.append(task_id)

        return tasks, dependencies, last_task_ids

    def _generate_dag_internal(
        self,
        dag_id: str,
        description: str,
        schedule: str | None,
        tasks: list[dict[str, Any]],
        dependencies: list[tuple[str, str]],
        start_date: datetime,
        owner: str,
        tags: list[str],
        retries: int,
        retry_delay_minutes: int,
        output_filename: str | None,
        doc_md: str | None,
        catchup: bool = False,
        max_active_runs: int = 1,
        execution_timeout_minutes: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> str:
        """Internal method to generate DAG code."""
        try:
            tasks_code = self._generate_tasks_code(tasks)
            dependencies_code = self._generate_dependencies_code(dependencies)

            # Escape special characters for embedding in Python string literals:
            # 1. Backslashes must be doubled (C:\Users -> C:\\Users) to avoid
            #    unicode escapes (\U), named chars (\N), hex (\x), etc.
            # 2. Single quotes in description must be escaped (used in single-quoted string)
            # 3. Triple quotes in doc_md must be escaped (used in triple-quoted string)
            # Use shared escaping functions for consistency
            safe_description = escape_single_quoted(description)
            safe_doc_md = escape_triple_quoted(doc_md)

            context = {
                "dag_id": dag_id,
                "dag_description": safe_description,
                "generation_timestamp": datetime.now().isoformat(),
                "owner": owner,
                "depends_on_past": "False",
                "email_on_failure": "False",
                "email_on_retry": "False",
                "retries": retries,
                "retry_delay_minutes": retry_delay_minutes,
                "schedule_interval": self._format_schedule_interval(schedule),
                "start_date_year": start_date.year,
                "start_date_month": start_date.month,
                "start_date_day": start_date.day,
                "catchup": str(catchup),
                "tags": repr(tags),
                "max_active_runs": max_active_runs,
                "tasks_code": tasks_code,
                "dependencies_code": dependencies_code,
                "use_tdload": any(t.get("type") in ("tdload_file", "tdload_table") for t in tasks),
                "use_bteq": any(t.get("type") == "bteq_validation" for t in tasks),
                "use_python": any(t.get("type") == "python" for t in tasks),
                "use_bash": any(t.get("type") == "dbt" for t in tasks),
                "doc_md": safe_doc_md,
                "params": params,
            }

            if execution_timeout_minutes:
                context["execution_timeout_minutes"] = execution_timeout_minutes

            # Render template
            template = self.env.from_string(self.DAG_TEMPLATE)
            dag_code = template.render(**context)

            # Write to file
            if output_filename is None:
                output_filename = f"{dag_id}.py"

            try:
                output_path, metadata = self.file_writer.write_file_safe(
                    content=dag_code,
                    filename=output_filename,
                    force=False,
                )

                if metadata.get("warnings"):
                    for warning in metadata["warnings"]:
                        logger.warning("TdLoad DAG %s: %s", dag_id, warning)

                logger.info("Generated TdLoad DAG file: %s", output_path)
            except Exception as write_error:
                logger.error("Failed to write DAG file: %s", write_error, exc_info=True)
                raise

            return dag_code

        except Exception as e:
            logger.error("Failed to generate DAG: %s", e, exc_info=True)
            raise AirflowTdLoadDAGGeneratorError(f"DAG generation failed: {e}") from e

    def _generate_tasks_code(self, tasks: list[dict[str, Any]]) -> str:
        """Generate code for all tasks."""
        task_codes = []

        for task in tasks:
            task_type = task.get("type")

            if task_type == "tdload_file":
                code = self._generate_tdload_file_task(task)
            elif task_type == "tdload_table":
                code = self._generate_tdload_table_task(task)
            elif task_type == "bteq_validation":
                code = self._generate_bteq_validation_task(task)
            elif task_type == "create_table":
                code = self._generate_create_table_task(task)
            elif task_type == "dbt":
                code = self._generate_dbt_task(task)
            elif task_type == "empty":
                code = self._generate_empty_task(task)
            else:
                logger.warning("Unknown task type: %s", task_type)
                continue

            task_codes.append(code)

        return "\n\n".join(task_codes)

    # Delimiters that would break the generated DAG or cause silent data issues.
    # Newlines split CSV rows, \r is part of line endings, \0 truncates C strings.
    _REJECTED_DELIMITERS = {"\n", "\r", "\0"}

    def _generate_tdload_file_task(self, task: dict[str, Any]) -> str:
        """Generate TdLoadOperator task for file loading."""
        # Pre-escape values that are interpolated into Python string literals
        # so that quotes, backslashes, and special chars cannot break syntax.
        ctx = dict(task)
        ctx["source_file_name_repr"] = repr(ctx.get("source_file_name", ""))

        delimiter = ctx.get("source_text_delimiter")
        if delimiter is not None:
            if len(delimiter) != 1:
                raise AirflowTdLoadDAGGeneratorError(
                    f"Invalid text delimiter: {delimiter!r}. Must be a single character."
                )
            if delimiter in self._REJECTED_DELIMITERS:
                raise AirflowTdLoadDAGGeneratorError(
                    f"Invalid text delimiter: {delimiter!r}. "
                    "Newline (\\n/\\r) and null characters cannot be used as delimiters."
                )
            ctx["source_text_delimiter_repr"] = repr(delimiter)
        else:
            ctx["source_text_delimiter_repr"] = None

        tdload_options = ctx.get("tdload_options")
        ctx["tdload_options_repr"] = repr(tdload_options) if tdload_options else None

        template = self.env.from_string(self.TDLOAD_FILE_TASK_TEMPLATE)
        return template.render(**ctx)

    def _generate_tdload_table_task(self, task: dict[str, Any]) -> str:
        """Generate TdLoadOperator task for table transfer."""
        template = self.env.from_string(self.TDLOAD_TABLE_TASK_TEMPLATE)
        return template.render(**task)

    def _generate_bteq_validation_task(self, task: dict[str, Any]) -> str:
        """Generate BteqOperator task for validation."""
        template = self.env.from_string(self.BTEQ_VALIDATION_TASK_TEMPLATE)
        return template.render(**task)

    def _generate_create_table_task(self, task: dict[str, Any]) -> str:
        """Generate BteqOperator task for CREATE TABLE."""
        template = self.env.from_string(self.CREATE_TABLE_TASK_TEMPLATE)
        return template.render(**task)

    def _generate_empty_task(self, task: dict[str, Any]) -> str:
        """Generate EmptyOperator task."""
        return f"{task['task_id']} = EmptyOperator(task_id='{task['task_id']}')"

    def _generate_dbt_task(self, task: dict[str, Any]) -> str:
        """Generate BashOperator task for dbt command."""
        import shlex

        subcommand = task.get("dbt_subcommand", "run")
        project_dir = task.get("project_dir", "")
        models = task.get("models")
        target = task.get("target")

        cmd_argv = ["dbt"] + subcommand.split()
        if project_dir:
            cmd_argv += ["--project-dir", project_dir]
        if target:
            cmd_argv += ["--target", target]
        if models and subcommand in ("run", "test"):
            model_list = models if isinstance(models, list) else [models]
            cmd_argv += ["--select"] + model_list

        ctx = {
            "task_id": task["task_id"],
            "dbt_command": escape_single_quoted(shlex.join(cmd_argv)),
            "cwd": escape_single_quoted(project_dir) if project_dir else None,
        }
        template = self.env.from_string(self.DBT_TASK_TEMPLATE)
        return template.render(**ctx)

    @staticmethod
    def _append_dbt_tasks(
        tasks: list[dict[str, Any]],
        deps: list[tuple[str, str]],
        last_task_ids: list[str],
        project_dir: str,
        models: list[str] | None = None,
        target: str = "prod",
        run_tests: bool = True,
        gen_docs: bool = False,
    ) -> str:
        """Append dbt deps/run/test/docs tasks and return the final task ID."""
        upstream_ids = list(last_task_ids)

        tasks.append({
            "type": "dbt",
            "task_id": "dbt_deps",
            "dbt_subcommand": "deps",
            "project_dir": project_dir,
        })
        for uid in upstream_ids:
            deps.append((uid, "dbt_deps"))

        tasks.append({
            "type": "dbt",
            "task_id": "dbt_run",
            "dbt_subcommand": "run",
            "project_dir": project_dir,
            "models": models,
            "target": target,
        })
        deps.append(("dbt_deps", "dbt_run"))

        last_task = "dbt_run"

        if run_tests:
            tasks.append({
                "type": "dbt",
                "task_id": "dbt_test",
                "dbt_subcommand": "test",
                "project_dir": project_dir,
                "models": models,
                "target": target,
            })
            deps.append((last_task, "dbt_test"))
            last_task = "dbt_test"

        if gen_docs:
            tasks.append({
                "type": "dbt",
                "task_id": "dbt_docs_generate",
                "dbt_subcommand": "docs generate",
                "project_dir": project_dir,
                "target": target,
            })
            deps.append((last_task, "dbt_docs_generate"))
            last_task = "dbt_docs_generate"

        return last_task

    def generate_file_loading_with_dbt_dag(
        self,
        dag_id: str,
        description: str,
        source_file_path: str,
        target_database: str,
        target_table: str,
        dbt_project_dir: str,
        dbt_models: list[str] | None = None,
        dbt_target: str = "prod",
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
        delimiter: str = ",",
        source_format: str = "Delimited",
        teradata_conn_id: str = "teradata_default",
        ssh_conn_id: str = "teradata_ssh_default",
        schedule: str | None = None,
        start_date: datetime | None = None,
        validation_queries: list[dict[str, Any]] | None = None,
        validation_bteq_file: str | None = None,
        owner: str = "data_engineer",
        tags: list[str] | None = None,
        retries: int = 2,
        retry_delay_minutes: int = 5,
        output_filename: str | None = None,
        doc_md: str | None = None,
        columns: list[dict[str, Any]] | None = None,
        skip_rows: int = 0,
    ) -> str:
        """Generate DAG for file loading to Teradata followed by dbt transformation."""
        try:
            target_database = self._sanitize_identifier(target_database)
            target_table = self._sanitize_identifier(target_table)

            start_date = start_date or datetime(2024, 1, 1)
            tasks: list[dict[str, Any]] = []
            dependencies: list[tuple[str, str]] = []

            tasks.append({"task_id": "start", "type": "empty"})

            create_database_sql = f"""
                SELECT DatabaseName FROM DBC.DatabasesV WHERE DatabaseName = '{target_database}';
                .IF ACTIVITYCOUNT > 0 THEN .GOTO DB_EXISTS;
                .IF ACTIVITYCOUNT = 0 THEN .GOTO CREATE_DB;
                .LABEL CREATE_DB
                CREATE DATABASE {target_database} AS PERMANENT = 120e6, SPOOL = 120e6;
                .IF ERRORCODE <> 0 THEN .QUIT 12;
                .REMARK 'Database created successfully';
                .GOTO END_SCRIPT;
                .LABEL DB_EXISTS
                .REMARK 'Database {target_database} already exists - skipping creation';
                .LABEL END_SCRIPT
                .LOGOFF;
                .QUIT;
                """
            tasks.append({
                "task_id": "create_database_if_not_exists",
                "type": "bteq_validation",
                "sql": create_database_sql,
                "teradata_conn_id": teradata_conn_id,
                "bteq_script_encoding": "UTF8",
            })
            dependencies.append(("start", "create_database_if_not_exists"))

            if columns and len(columns) > 0:
                column_defs = []
                for col in columns:
                    col_name = self._sanitize_identifier(col.get("name", ""))
                    col_type = self._sanitize_col_type(
                        col.get("type") or col.get("inferred_teradata_type", "VARCHAR(255)")
                    )
                    nullable = "" if col.get("nullable", True) else "NOT NULL"
                    column_defs.append(f"    {col_name} {col_type} {nullable}".strip())
                column_definitions = ",\n".join(column_defs)
                primary_index_col = self._sanitize_identifier(columns[0].get("name", ""))
            else:
                column_definitions = "    dummy_col VARCHAR(1)"
                primary_index_col = "dummy_col"

            backup_and_prepare_sql = f"""
                SELECT TableName FROM DBC.TablesV
                WHERE DatabaseName = '{target_database}' AND TableName = '{target_table}';
                .IF ACTIVITYCOUNT > 0 THEN .GOTO TABLE_EXISTS;
                .IF ACTIVITYCOUNT = 0 THEN .GOTO CREATE_NEW;
                .LABEL TABLE_EXISTS
                .SET ERRORLEVEL 3807 SEVERITY 0
                DROP TABLE {target_database}.{target_table}_bkp;
                .SET ERRORLEVEL 3807 SEVERITY 8
                CREATE TABLE {target_database}.{target_table}_bkp AS {target_database}.{target_table} WITH DATA;
                .IF ERRORCODE <> 0 THEN .QUIT 12;
                .REMARK 'Backup table created: {target_table}_bkp';
                DELETE FROM {target_database}.{target_table} ALL;
                .IF ERRORCODE <> 0 THEN .QUIT 12;
                .REMARK 'Existing table truncated';
                .GOTO END_SCRIPT;
                .LABEL CREATE_NEW
                CREATE MULTISET TABLE {target_database}.{target_table}, NO FALLBACK,
                    NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO
                    (
                {column_definitions}
                    )
                PRIMARY INDEX ({primary_index_col});
                .IF ERRORCODE <> 0 THEN .QUIT 12;
                .REMARK 'New table created successfully';
                .LABEL END_SCRIPT
                .LOGOFF;
                .QUIT;
                """
            tasks.append({
                "task_id": "backup_and_prepare_table",
                "type": "create_table",
                "sql": backup_and_prepare_sql,
                "teradata_conn_id": teradata_conn_id,
            })
            dependencies.append(("create_database_if_not_exists", "backup_and_prepare_table"))

            tdload_options_parts = []
            if skip_rows > 0:
                tdload_options_parts.append(f"--FileReaderSkipRows {skip_rows}")
            tdload_options = " ".join(tdload_options_parts) if tdload_options_parts else None

            tasks.append({
                "task_id": "load_file_to_teradata",
                "type": "tdload_file",
                "source_file_name": source_file_path,
                "target_table": f"{target_database}.{target_table}",
                "source_format": source_format,
                "source_text_delimiter": delimiter,
                "tdload_options": tdload_options,
                "teradata_conn_id": teradata_conn_id,
                "ssh_conn_id": ssh_conn_id,
            })
            dependencies.append(("backup_and_prepare_table", "load_file_to_teradata"))

            val_tasks, val_deps, last_ids = self._build_validation_tasks_and_deps(
                validation_queries,
                validation_bteq_file,
                default_conn_id=teradata_conn_id,
                upstream_task_id="load_file_to_teradata",
                file_task_id="validate_load",
            )
            tasks.extend(val_tasks)
            dependencies.extend(val_deps)

            last_dbt_task = self._append_dbt_tasks(
                tasks, dependencies, last_ids,
                project_dir=dbt_project_dir,
                models=dbt_models,
                target=dbt_target,
                run_tests=run_dbt_tests,
                gen_docs=generate_dbt_docs,
            )

            tasks.append({"task_id": "end", "type": "empty"})
            dependencies.append((last_dbt_task, "end"))

            return self._generate_dag_internal(
                dag_id=dag_id,
                description=description,
                schedule=schedule,
                tasks=tasks,
                dependencies=dependencies,
                start_date=start_date,
                owner=owner,
                tags=tags or ["teradata", "data_loading", "tdload", "dbt"],
                retries=retries,
                retry_delay_minutes=retry_delay_minutes,
                output_filename=output_filename,
                doc_md=doc_md,
            )

        except Exception as e:
            logger.error("Failed to generate file loading with dbt DAG: %s", e, exc_info=True)
            raise AirflowTdLoadDAGGeneratorError(
                f"File loading with dbt DAG generation failed: {e}"
            ) from e

    def generate_table_transfer_with_dbt_dag(
        self,
        dag_id: str,
        description: str,
        source_database: str,
        source_table: str,
        target_database: str,
        target_table: str,
        dbt_project_dir: str,
        dbt_models: list[str] | None = None,
        dbt_target: str = "prod",
        run_dbt_tests: bool = True,
        generate_dbt_docs: bool = False,
        source_metadata: dict[str, Any] | None = None,
        source_teradata_conn_id: str = "teradata_source",
        target_teradata_conn_id: str = "teradata_target",
        ssh_conn_id: str = "teradata_ssh_default",
        schedule: str | None = None,
        start_date: datetime | None = None,
        validation_queries: list[dict[str, Any]] | None = None,
        validation_bteq_file: str | None = None,
        owner: str = "data_engineer",
        tags: list[str] | None = None,
        retries: int = 2,
        retry_delay_minutes: int = 5,
        output_filename: str | None = None,
        doc_md: str | None = None,
    ) -> str:
        """Generate DAG for table transfer between Teradata systems followed by dbt transformation."""
        try:
            source_database = self._sanitize_identifier(source_database)
            source_table = self._sanitize_identifier(source_table)
            target_database = self._sanitize_identifier(target_database)
            target_table = self._sanitize_identifier(target_table)

            start_date = start_date or datetime(2024, 1, 1)
            tasks: list[dict[str, Any]] = []
            dependencies: list[tuple[str, str]] = []

            tasks.append({"task_id": "start", "type": "empty"})

            cleanup_source_task = {
                "task_id": "cleanup_source_error_tables",
                "type": "bteq_validation",
                "sql": f"""
                    .SET ERRORLEVEL 3807 SEVERITY 0
                    DROP TABLE {source_database}.{source_table}_ERR_1;
                    DROP TABLE {source_database}.{source_table}_ERR_2;
                    DROP TABLE {source_database}.{source_table}_ET;
                    DROP TABLE {source_database}.{source_table}_WT;
                    DROP TABLE {source_database}.{source_table}_UV;
                    .SET ERRORLEVEL 3807 SEVERITY 8
                    .REMARK 'Source error tables cleanup completed';
                    """,
                "teradata_conn_id": source_teradata_conn_id,
                "bteq_script_encoding": "UTF8",
            }
            tasks.append(cleanup_source_task)
            dependencies.append(("start", "cleanup_source_error_tables"))

            cleanup_target_task = {
                "task_id": "cleanup_target_error_tables",
                "type": "bteq_validation",
                "sql": f"""
                    .SET ERRORLEVEL 3807 SEVERITY 0
                    DROP TABLE {target_database}.{target_table}_ERR_1;
                    DROP TABLE {target_database}.{target_table}_ERR_2;
                    DROP TABLE {target_database}.{target_table}_ET;
                    DROP TABLE {target_database}.{target_table}_WT;
                    DROP TABLE {target_database}.{target_table}_UV;
                    .SET ERRORLEVEL 3807 SEVERITY 8
                    .REMARK 'Target error tables cleanup completed';
                    """,
                "teradata_conn_id": target_teradata_conn_id,
                "bteq_script_encoding": "UTF8",
            }
            tasks.append(cleanup_target_task)
            dependencies.append(("start", "cleanup_target_error_tables"))

            if source_metadata and source_metadata.get("columns"):
                columns = source_metadata.get("columns", [])
                column_defs = []
                for col in columns:
                    col_name = self._sanitize_identifier(col.get("name", ""))
                    col_type_code = col.get("type", "VARCHAR(255)")
                    col_length = col.get("length")
                    col_precision = col.get("precision")
                    col_scale = col.get("scale")
                    full_type = self._sanitize_col_type(
                        self._convert_teradata_type_to_sql(
                            col_type_code, col_length, col_precision, col_scale
                        )
                    )
                    nullable = "" if col.get("nullable", True) else "NOT NULL"
                    column_defs.append(f"        {col_name} {full_type} {nullable}".strip())

                column_definitions = ",\n".join(column_defs)
                primary_index_col = (
                    self._sanitize_identifier(columns[0].get("name", ""))
                    if columns
                    else "first_column"
                )

                create_table_sql = f"""
                    SELECT TableName FROM DBC.TablesV
                    WHERE DatabaseName = '{target_database}' AND TableName = '{target_table}';
                    .IF ACTIVITYCOUNT > 0 THEN .GOTO TABLE_EXISTS;
                    .IF ACTIVITYCOUNT = 0 THEN .GOTO CREATE_TABLE;
                    .LABEL CREATE_TABLE
                    CREATE MULTISET TABLE {target_database}.{target_table}, NO FALLBACK,
                        NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO
                        (
                    {column_definitions}
                        )
                    PRIMARY INDEX ({primary_index_col});
                    .IF ERRORCODE <> 0 THEN .QUIT 12;
                    .REMARK 'Target table created successfully';
                    .GOTO END_SCRIPT;
                    .LABEL TABLE_EXISTS
                    .REMARK 'Target table already exists - skipping creation';
                    .LABEL END_SCRIPT
                    .LOGOFF;
                    .QUIT;
                    """
            else:
                create_table_sql = f"""
                    SELECT TableName FROM DBC.TablesV
                    WHERE DatabaseName = '{target_database}' AND TableName = '{target_table}';
                    .IF ACTIVITYCOUNT > 0 THEN .GOTO TABLE_EXISTS;
                    .IF ACTIVITYCOUNT = 0 THEN .GOTO CREATE_TABLE;
                    .LABEL CREATE_TABLE
                    CREATE TABLE {target_database}.{target_table} AS
                    {source_database}.{source_table}
                    WITH NO DATA;
                    .IF ERRORCODE <> 0 THEN .QUIT 12;
                    .REMARK 'Target table created successfully';
                    .GOTO END_SCRIPT;
                    .LABEL TABLE_EXISTS
                    .REMARK 'Target table already exists - skipping creation';
                    .LABEL END_SCRIPT
                    .LOGOFF;
                    .QUIT;
                    """

            tasks.append({
                "task_id": "create_target_table_if_not_exists",
                "type": "bteq_validation",
                "sql": create_table_sql,
                "teradata_conn_id": target_teradata_conn_id,
                "bteq_script_encoding": "UTF8",
            })
            dependencies.append(
                ("cleanup_source_error_tables", "create_target_table_if_not_exists")
            )
            dependencies.append(
                ("cleanup_target_error_tables", "create_target_table_if_not_exists")
            )

            tasks.append({
                "task_id": "transfer_table_data",
                "type": "tdload_table",
                "source_table": f"{source_database}.{source_table}",
                "target_table": f"{target_database}.{target_table}",
                "source_teradata_conn_id": source_teradata_conn_id,
                "target_teradata_conn_id": target_teradata_conn_id,
                "ssh_conn_id": ssh_conn_id,
            })
            dependencies.append(("create_target_table_if_not_exists", "transfer_table_data"))

            val_tasks, val_deps, last_ids = self._build_validation_tasks_and_deps(
                validation_queries,
                validation_bteq_file,
                default_conn_id=target_teradata_conn_id,
                upstream_task_id="transfer_table_data",
                file_task_id="validate_transfer",
            )
            tasks.extend(val_tasks)
            dependencies.extend(val_deps)

            last_dbt_task = self._append_dbt_tasks(
                tasks, dependencies, last_ids,
                project_dir=dbt_project_dir,
                models=dbt_models,
                target=dbt_target,
                run_tests=run_dbt_tests,
                gen_docs=generate_dbt_docs,
            )

            tasks.append({"task_id": "end", "type": "empty"})
            dependencies.append((last_dbt_task, "end"))

            return self._generate_dag_internal(
                dag_id=dag_id,
                description=description,
                schedule=schedule,
                tasks=tasks,
                dependencies=dependencies,
                start_date=start_date,
                owner=owner,
                tags=tags or ["teradata", "data_transfer", "tdload", "dbt"],
                retries=retries,
                retry_delay_minutes=retry_delay_minutes,
                output_filename=output_filename,
                doc_md=doc_md,
            )

        except Exception as e:
            logger.error("Failed to generate table transfer with dbt DAG: %s", e, exc_info=True)
            raise AirflowTdLoadDAGGeneratorError(
                f"Table transfer with dbt DAG generation failed: {e}"
            ) from e

    def _generate_dependencies_code(self, dependencies: list[tuple[str, str]]) -> str:
        """Generate task dependencies code."""
        if not dependencies:
            return "# No dependencies"

        dep_lines = []
        for upstream, downstream in dependencies:
            dep_lines.append(f"{upstream} >> {downstream}")

        return "\n".join(dep_lines)

    def _format_schedule_interval(self, schedule: str | None) -> str:
        """Format schedule interval for DAG template."""
        if schedule is None:
            return "None"
        return f"'{schedule}'"
