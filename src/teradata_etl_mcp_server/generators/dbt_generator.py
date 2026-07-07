"""dbt model, source, and test generator.

This module generates dbt artifacts (sources, models, tests, documentation)
from Teradata metadata and user specifications.
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from jinja2 import Environment

from ..utils.file_operations import FileOperationError, SafeFileWriter

if TYPE_CHECKING:
    from ..auth import TeradataAuth


# Characters that Teradata unconditionally forbids inside any object name.
_TD_DISALLOWED_CHARS = re.compile(
    "[\u0000\u001a\ufffd\ufa6c\ufa6f\ufad0\ufad1\ufad5\ufad6\ufad7]"
)


def _quote_column(name: str) -> str:
    """Double-quote a column identifier for safe use in Teradata SQL.

    Follows Teradata object naming rules:
    - Trailing whitespace is stripped (not part of the name per spec).
    - All-whitespace / empty names are rejected.
    - Disallowed characters (NULL U+0000, SUBSTITUTE U+001A,
      REPLACEMENT CHARACTER U+FFFD, and select compatibility ideographs)
      are rejected.
    - Embedded double-quotes are escaped by doubling them (U+0022 -> U+0022 U+0022).
    """
    if not name or not name.rstrip():
        raise ValueError(f"Empty or all-whitespace column name: {name!r}")
    name = name.rstrip()
    if _TD_DISALLOWED_CHARS.search(name):
        raise ValueError(
            f"Column name contains disallowed Teradata characters: {name!r}"
        )
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _format_hook_value(hook: str | list[str]) -> str:
    """Serialize a dbt pre/post-hook value safely for Jinja config() calls.

    Hooks are always emitted as a JSON list so that embedded double-quotes,
    single-quotes, newlines, and Jinja expressions (e.g. ``{{ this }}``) are
    preserved without breaking the surrounding Jinja ``config()`` block.

    Examples
    --------
    >>> _format_hook_value("COLLECT STATS ON {{ this }}")
    '["COLLECT STATS ON {{ this }}"]'
    >>> _format_hook_value('ALTER TABLE "my_db"."tbl" SET STATS')
    '["ALTER TABLE \\\\"my_db\\\\".\\\\"tbl\\\\" SET STATS"]'
    """
    items = hook if isinstance(hook, list) else [hook]
    return json.dumps(items)


logger = logging.getLogger(__name__)


_DOTENV_NEEDS_QUOTING = re.compile(r'[\s#"\\]')


def _write_dotenv_file(path: Path, env: dict[str, str]) -> list[str]:
    """Write a dotenv-format file with one ``KEY=VALUE`` per line.

    Empty / falsy values are skipped (so a JWT profile pushes
    ``TERADATA_LOGDATA`` but not ``TERADATA_PASSWORD``, avoiding leaking
    ``""`` into the file). Values containing whitespace, ``#``, ``"``, or
    ``\\`` are wrapped in double quotes and have ``"`` and ``\\`` escaped.
    File mode is set to ``0o600`` on POSIX (skipped on Windows where the
    bits don't map cleanly).

    Returns the list of keys actually written, in input order. Does NOT
    log any value — only key names — to keep credential values off the
    server's stderr stream and out of the LLM's view.
    """
    lines: list[str] = []
    keys_written: list[str] = []
    for key, value in env.items():
        if not value:
            continue
        if _DOTENV_NEEDS_QUOTING.search(value):
            escaped = value.replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f'{key}="{escaped}"')
        else:
            lines.append(f"{key}={value}")
        keys_written.append(key)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if os.name == "posix":
        os.chmod(path, 0o600)
    return keys_written


class DBTGeneratorError(Exception):
    """Base exception for dbt generator errors."""

    pass


class DBTGenerator:
    """
    Comprehensive dbt artifact generator.

    Generates sources, staging models, transformation models, tests,
    and documentation from metadata and specifications.
    """

    # Jinja2 templates for dbt artifacts
    SOURCE_YAML_TEMPLATE = """version: 2

sources:
  - name: {{ source_name }}
    description: "{{ source_description }}"
    database: {{ database }}
    schema: {{ schema }}
    {% if meta %}meta:
{{ meta | indent(6, True) }}
    {% endif %}
    tables:
{% for table in tables %}
      - name: {{ table.name }}
        description: "{{ table.description | default('') }}"
        {% if table.identifier %}identifier: {{ table.identifier }}
        {% endif %}
        {% if table.meta %}meta:
{{ table.meta | indent(10, True) }}
        {% endif %}
        {% if table.freshness %}freshness:
{{ table.freshness | indent(10, True) }}
        {% endif %}
        columns:
{% for column in table.columns %}
          - name: {{ column.name }}
            description: "{{ column.description | default('') }}"
            {% if column.data_type %}data_type: {{ column.data_type }}
            {% endif %}
            {% if column.meta %}meta:
{{ column.meta | indent(14, True) }}
            {% endif %}
            {% if column.tests %}tests:
{% for test in column.tests %}
              - {{ test|to_yaml_test }}
{% endfor %}
            {% endif %}
{% endfor %}
        {% if table.tests %}tests:
{% for test in table.tests %}
          - {{ test|to_yaml_test }}
{% endfor %}
        {% endif %}
{% endfor %}
"""

    SCHEMA_YML_TEMPLATE = """version: 2

models:
{% for model in models %}
  - name: {{ model.name }}
    description: "{{ model.description | default('') }}"
    {% if model.meta %}meta:
{{ model.meta | indent(6, True) }}
    {% endif %}
    columns:
{% for column in model.columns %}
      - name: {{ column.name }}
        description: "{{ column.description | default('') }}"
        {% if column.data_type %}data_type: {{ column.data_type }}
        {% endif %}
        {% if column.meta %}meta:
{{ column.meta | indent(10, True) }}
        {% endif %}
        {% if column.tests %}tests:
{% for test in column.tests %}
          - {{ test|to_yaml_test }}
{% endfor %}
        {% endif %}
{% endfor %}
    {% if model.tests %}tests:
{% for test in model.tests %}
      - {{ test }}
{% endfor %}
    {% endif %}
{% endfor %}
"""

    def __init__(self, project_dir: Path):
        """
        Initialize dbt generator.

        Args:
            project_dir: Path to dbt project directory
        """
        # Resolve once at construction; the containment base is CWD-independent
        # afterwards. Invariant: main.py chdir's before the orchestrator is built
        # and DBTGenerator is lazy-loaded, so CWD is final by the time we run.
        self.project_dir = Path(project_dir).resolve()
        self.project_dir.mkdir(parents=True, exist_ok=True)
        self.env = Environment(autoescape=False)
        self.env.filters["to_yaml_test"] = self._to_yaml_test

        # SafeFileWriter is the single write/read abstraction for this class.
        # Flags selected to preserve current dbt behavior:
        #   - enable_backups=False: no .backups/ dir inside the dbt project
        #   - restrict_permissions=False: keep umask-default perms so multi-user
        #     setups (Airflow-as-another-user running dbt) can read the files
        #   - validate_python=False: dbt writes .sql and .yml, not .py
        self.file_writer = SafeFileWriter(
            self.project_dir,
            enable_backups=False,
            restrict_permissions=False,
            validate_python=False,
        )

        logger.info("Initialized dbt generator for %s", self.project_dir)

    @staticmethod
    def _to_yaml_test(test: Any) -> str:
        """Render a schema test as valid YAML (handles both string and dict tests)."""
        if isinstance(test, str):
            return test
        return yaml.dump(test, default_flow_style=True).strip()

    # ==================== Source Generation ====================

    def generate_source_yaml(
        self,
        source_name: str,
        database: str,
        schema: str,
        tables: list[dict[str, Any]],
        source_description: str = "",
        meta: dict[str, Any] | None = None,
        output_path: Path | None = None,
    ) -> str:
        """
        Generate dbt source YAML from table metadata.

        Args:
            source_name: Source name (e.g., 'raw_teradata')
            database: Database name
            schema: Schema name
            tables: List of table metadata dictionaries
            source_description: Optional source description
            meta: Optional source-level metadata
            output_path: Optional path to write YAML file

        Returns:
            Generated YAML string
        """
        try:
            # Prepare template context
            context = {
                "source_name": source_name,
                "source_description": source_description
                or f"Source tables from {database}.{schema}",
                "database": database,
                "schema": schema,
                "meta": yaml.dump(meta) if meta else None,
                "tables": tables,
            }

            # Render template
            template = self.env.from_string(self.SOURCE_YAML_TEMPLATE)
            yaml_content = template.render(**context)

            # Write to file if path provided, merging with existing content
            if output_path:
                filename = str(output_path)
                try:
                    existing_text = self.file_writer.read_file_safe(filename)
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                if existing_text:
                    try:
                        existing = yaml.safe_load(existing_text)
                        if existing and "sources" in existing and existing["sources"]:
                            new_parsed = yaml.safe_load(yaml_content)
                            new_tables = {t["name"]: t for t in new_parsed["sources"][0]["tables"]}
                            existing_tables = existing["sources"][0].get("tables", [])
                            merged = {t["name"]: t for t in existing_tables}
                            merged.update(new_tables)
                            existing["sources"][0]["tables"] = list(merged.values())
                            yaml_content = yaml.dump(existing, default_flow_style=False, sort_keys=False)
                    except Exception as merge_err:
                        logger.warning("Could not merge with existing source YAML, overwriting: %s", merge_err)

                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=yaml_content,
                        filename=filename,
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated source YAML: %s", output_file)

            return yaml_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate source YAML: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Source YAML generation failed: {e}") from e

    def generate_source_from_teradata_metadata(
        self,
        source_name: str,
        table_metadata_list: list[dict[str, Any]],
        output_path: Path | None = None,
        add_freshness: bool = False,
        add_basic_tests: bool = True,
    ) -> str:
        """
        Generate source YAML from Teradata table metadata.

        Args:
            source_name: Source name
            table_metadata_list: List of metadata dicts from TeradataClient.get_table_metadata()
            output_path: Optional output path
            add_freshness: Whether to add freshness checks
            add_basic_tests: Whether to add basic schema tests

        Returns:
            Generated YAML string
        """
        try:
            if not table_metadata_list:
                raise DBTGeneratorError("No table metadata provided")

            # Extract database and schema from first table
            first_table = table_metadata_list[0]
            database = first_table.get("database", "UNKNOWN")

            # In Teradata, the database serves as the schema.
            # For database systems that separate these concepts, attempt to extract schema from metadata.
            if first_table.get("schema"):
                schema = first_table.get("schema")
            else:
                # Default to database name for Teradata
                schema = database

            # Convert Teradata metadata to dbt source format
            tables = []
            for table_meta in table_metadata_list:
                table_name = table_meta.get("table_name") or table_meta.get("table")

                # Convert columns
                columns = []
                for col in table_meta.get("columns", []):
                    column_def = {
                        "name": _quote_column((col.get("column_name") or col.get("name", "")).lower()),
                        "description": col.get("description", ""),
                        "data_type": col.get("data_type") or col.get("type"),
                        "meta": yaml.dump(
                            {
                                "nullable": col.get("nullable", True),
                                "max_length": col.get("max_length") or col.get("length"),
                            },
                            default_flow_style=False,
                        ),
                    }

                    # Add basic tests
                    if add_basic_tests:
                        tests = []

                        # Not null test for primary keys and non-nullable columns
                        if not col.get("nullable", True):
                            tests.append("not_null")

                        # Unique test for primary key columns
                        if col.get("is_primary_key"):
                            tests.append("unique")

                        if tests:
                            column_def["tests"] = tests

                    columns.append(column_def)

                table_def = {
                    "name": table_name.lower(),
                    "description": table_meta.get("description", ""),
                    "identifier": table_name,  # Original case
                    "columns": columns,
                    "meta": yaml.dump(
                        {
                            "row_count": table_meta.get("row_count"),
                            "size_mb": table_meta.get("size_mb"),
                        },
                        default_flow_style=False,
                    ),
                }

                # Add freshness check if requested
                if add_freshness:
                    table_def["freshness"] = yaml.dump(
                        {
                            "warn_after": {"count": 24, "period": "hour"},
                            "error_after": {"count": 48, "period": "hour"},
                        },
                        default_flow_style=False,
                    )

                tables.append(table_def)

            return self.generate_source_yaml(
                source_name=source_name,
                database=database,
                schema=schema,
                tables=tables,
                output_path=output_path,
            )

        except Exception as e:
            logger.error("Failed to generate source from Teradata metadata: %s", e, exc_info=True)
            raise

    # ==================== Model Generation ====================

    # Valid incremental strategies for dbt-teradata
    TERADATA_INCREMENTAL_STRATEGIES = ["append", "merge", "delete+insert"]

    def generate_staging_model(
        self,
        model_name: str,
        source_name: str,
        table_name: str,
        columns: list[str],
        materialization: str = "view",
        column_aliases: dict[str, str] | None = None,
        custom_columns: list[str] | None = None,
        config_options: dict[str, Any] | None = None,
        output_path: Path | None = None,
        unique_key: str | None = None,
        incremental_strategy: str | None = None,
        incremental_column: str | None = None,
        on_schema_change: str = "fail",
        tags: list[str] | None = None,
    ) -> str:
        """
        Generate dbt staging model SQL.

        Args:
            model_name: Model name (e.g., 'stg_customers')
            source_name: Source name in sources.yml
            table_name: Table name in source
            columns: List of column names to select
            materialization: Materialization strategy (view, table, incremental)
            column_aliases: Optional dict mapping source column -> alias
            custom_columns: Optional list of custom column expressions
            config_options: Optional config block options
            output_path: Optional path to write SQL file
            unique_key: Required for incremental - column(s) for merge key
            incremental_strategy: For Teradata: 'append', 'merge', or 'delete+insert'
            incremental_column: Column used for incremental filtering (e.g., 'updated_at')
            on_schema_change: How to handle schema changes ('fail', 'ignore', 'append_new_columns', 'sync_all_columns')

        Returns:
            Generated SQL string
        """
        try:
            # Validate incremental strategy for Teradata
            if materialization == "incremental":
                if (
                    incremental_strategy
                    and incremental_strategy not in self.TERADATA_INCREMENTAL_STRATEGIES
                ):
                    raise DBTGeneratorError(
                        f"Invalid incremental_strategy '{incremental_strategy}' for Teradata. "
                        f"Valid options: {self.TERADATA_INCREMENTAL_STRATEGIES}"
                    )
                if not unique_key and incremental_strategy in ["merge", "delete+insert"]:
                    raise DBTGeneratorError(
                        f"unique_key is required for incremental_strategy '{incremental_strategy}'"
                    )

            # Format columns with aliases (all identifiers are double-quoted
            # to avoid conflicts with Teradata reserved keywords).
            column_list = []
            for col in columns:
                alias = column_aliases.get(col) if column_aliases else None
                if alias and alias != col:
                    column_list.append(f'{_quote_column(col)} as {_quote_column(alias)}')
                else:
                    column_list.append(_quote_column(col))

            # Add custom columns
            if custom_columns:
                column_list.extend(custom_columns)

            # Add dbt_updated_at for incremental models
            if materialization == "incremental":
                column_list.append("current_timestamp as dbt_updated_at")

            # Format column string with proper indentation
            columns_str = ",\n        ".join(column_list)

            # Build config block
            config_parts = [f"materialized='{materialization}'"]

            # Add incremental config options
            if materialization == "incremental":
                if unique_key:
                    config_parts.append(f"unique_key='{unique_key}'")
                if incremental_strategy:
                    config_parts.append(f"incremental_strategy='{incremental_strategy}'")
                config_parts.append(f"on_schema_change='{on_schema_change}'")

            if tags:
                tags_str = str(tags).replace("'", '"')
                config_parts.append(f"tags={tags_str}")

            # Add additional config options
            if config_options:
                for key, value in config_options.items():
                    if isinstance(value, str):
                        config_parts.append(f"{key}='{value}'")
                    elif isinstance(value, list):
                        list_str = str(value).replace("'", '"')
                        config_parts.append(f"{key}={list_str}")
                    else:
                        config_parts.append(f"{key}={value}")

            config_str = ",\n    ".join(config_parts)

            # Build incremental filter block
            incremental_block = ""
            if materialization == "incremental" and incremental_column:
                incremental_block = f"""
{{% if is_incremental() %}}
    where {_quote_column(incremental_column)} > (select max({_quote_column(incremental_column)}) from {{{{ this }}}})
{{% endif %}}
"""

            # Generate SQL using string formatting
            sql_content = f"""{{{{ config(
    {config_str}
) }}}}

with source as (

    select * from {{{{ source('{source_name}', '{table_name.lower()}') }}}}

),

renamed as (

    select
        {columns_str}

    from source

)

select * from renamed
{incremental_block}"""

            # Write to file if path provided
            if output_path:
                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=sql_content,
                        filename=str(output_path),
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated staging model: %s", output_file)

            return sql_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate staging model: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Staging model generation failed: {e}") from e

    def generate_incremental_model(
        self,
        model_name: str,
        source_name: str,
        table_name: str,
        columns: list[str],
        unique_key: str,
        incremental_column: str = "updated_at",
        incremental_strategy: str = "merge",
        on_schema_change: str = "fail",
        config_options: dict[str, Any] | None = None,
        output_path: Path | None = None,
        tags: list[str] | None = None,
    ) -> str:
        """
        Generate dbt incremental model SQL.

        Args:
            model_name: Model name
            source_name: Source name
            table_name: Table name
            columns: List of columns
            unique_key: Unique key column for incremental logic
            incremental_column: Column to use for incremental logic
            incremental_strategy: For Teradata: 'append', 'merge', or 'delete+insert'
            on_schema_change: How to handle schema changes ('fail', 'ignore', 'append_new_columns', 'sync_all_columns')
            config_options: Optional config options
            output_path: Optional output path

        Returns:
            Generated SQL string
        """
        try:
            # Validate incremental strategy for Teradata
            if incremental_strategy not in self.TERADATA_INCREMENTAL_STRATEGIES:
                raise DBTGeneratorError(
                    f"Invalid incremental_strategy '{incremental_strategy}' for Teradata. "
                    f"Valid options: {self.TERADATA_INCREMENTAL_STRATEGIES}"
                )

            # Format columns (all identifiers are double-quoted
            # to avoid conflicts with Teradata reserved keywords).
            columns_str = ",\n        ".join(_quote_column(col) for col in columns)

            # Build config parts
            config_parts = [
                "materialized='incremental'",
                f"unique_key='{unique_key}'",
                f"incremental_strategy='{incremental_strategy}'",
                f"on_schema_change='{on_schema_change}'",
            ]

            if tags:
                tags_str = str(tags).replace("'", '"')
                config_parts.append(f"tags={tags_str}")

            # Add additional config options
            if config_options:
                for key, value in config_options.items():
                    if isinstance(value, str):
                        config_parts.append(f"{key}='{value}'")
                    elif isinstance(value, list):
                        list_str = str(value).replace("'", '"')
                        config_parts.append(f"{key}={list_str}")
                    else:
                        config_parts.append(f"{key}={value}")

            config_str = ",\n    ".join(config_parts)

            # Generate incremental filter
            incremental_filter = f"""
{{% if is_incremental() %}}
    where {_quote_column(incremental_column)} > (select max({_quote_column(incremental_column)}) from {{{{ this }}}})
{{% endif %}}
"""

            # Generate SQL using string formatting
            sql_content = f"""{{{{ config(
    {config_str}
) }}}}

with source_data as (
    
    select
        {columns_str}
    
    from {{{{ source('{source_name}', '{table_name}') }}}}
    
)

select * from source_data

{incremental_filter.strip()}
"""

            # Write to file if path provided
            if output_path:
                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=sql_content,
                        filename=str(output_path),
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated incremental model: %s", output_file)

            return sql_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate incremental model: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Incremental model generation failed: {e}") from e

    def generate_snapshot(
        self,
        snapshot_name: str,
        source_name: str,
        table_name: str,
        target_schema: str,
        unique_key: str,
        strategy: str = "timestamp",
        updated_at: str | None = "updated_at",
        check_cols: list[str] | None = None,
        output_path: Path | None = None,
        tags: list[str] | None = None,
    ) -> str:
        """
        Generate dbt snapshot SQL.

        Args:
            snapshot_name: Snapshot name
            source_name: Source name
            table_name: Table name
            target_schema: Schema to write snapshot to
            unique_key: Unique key column
            strategy: Snapshot strategy ('timestamp' or 'check')
            updated_at: Updated timestamp column (for timestamp strategy)
            check_cols: Columns to check (for check strategy, or 'all')
            output_path: Optional output path

        Returns:
            Generated SQL string
        """
        try:
            # Strategy-specific config
            if strategy == "timestamp":
                strategy_config = f"updated_at='{updated_at}'"
            elif strategy == "check":
                if check_cols:
                    if check_cols == ["all"]:
                        strategy_config = "check_cols='all'"
                    else:
                        cols_str = "', '".join(check_cols)
                        strategy_config = f"check_cols=['{cols_str}']"
                else:
                    strategy_config = "check_cols='all'"
            else:
                raise DBTGeneratorError(f"Unknown snapshot strategy: {strategy}")

            tags_line = ""
            if tags:
                tags_str = str(tags).replace("'", '"')
                tags_line = f",\n      tags={tags_str}"

            # Generate SQL using string formatting
            sql_content = f"""{{% snapshot {snapshot_name} %}}

{{{{
    config(
      target_schema='{target_schema}',
      unique_key='{unique_key}',
      strategy='{strategy}',
      {strategy_config}{tags_line}
    )
}}}}

select * from {{{{ source('{source_name}', '{table_name}') }}}}

{{% endsnapshot %}}
"""

            # Write to file if path provided
            if output_path:
                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=sql_content,
                        filename=str(output_path),
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated snapshot: %s", output_file)

            return sql_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate snapshot: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Snapshot generation failed: {e}") from e

    def generate_transformation_model(
        self,
        model_name: str,
        base_models: list[str],
        transformation_sql: str,
        materialization: str = "table",
        config_options: dict[str, Any] | None = None,
        output_path: Path | None = None,
        tags: list[str] | None = None,
    ) -> str:
        """
        Generate custom transformation model.

        Args:
            model_name: Model name
            base_models: List of upstream model references
            transformation_sql: Custom SQL logic
            materialization: Materialization strategy
            config_options: Optional config options
            output_path: Optional output path

        Returns:
            Generated SQL string
        """
        try:
            # Build config block
            config_block = f"{{{{ config(materialized='{materialization}'"

            if tags:
                tags_str = str(tags).replace("'", '"')
                config_block += f", tags={tags_str}"

            if config_options:
                for key, value in config_options.items():
                    if isinstance(value, str):
                        config_block += f", {key}='{value}'"
                    else:
                        config_block += f", {key}={value}"

            config_block += ") }}\n\n"

            # Build CTEs for base models
            ctes = []
            for i, model in enumerate(base_models):
                cte_name = f"base_{i}" if len(base_models) > 1 else "base"
                ctes.append(f"{cte_name} as (\n    select * from {{{{ ref('{model}') }}}}\n)")

            cte_block = "with " + ",\n\n".join(ctes) + "\n\n" if ctes else ""

            # Combine parts
            sql_content = config_block + cte_block + transformation_sql

            # Write to file if path provided
            if output_path:
                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=sql_content,
                        filename=str(output_path),
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated transformation model: %s", output_file)

            return sql_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate transformation model: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Transformation model generation failed: {e}") from e

    def generate_intermediate_model(
        self,
        model_name: str,
        source_models: list[str],
        join_logic: list[dict[str, Any]] | None = None,
        select_columns: list[str] | None = None,
        where_clause: str | None = None,
        group_by: list[str] | None = None,
        materialization: str = "view",
        config_options: dict[str, Any] | None = None,
        pre_hook: str | None = None,
        post_hook: str | None = None,
        output_path: Path | None = None,
        unique_key: str | None = None,
        incremental_strategy: str | None = None,
        incremental_column: str | None = None,
        on_schema_change: str = "fail",
        tags: list[str] | None = None,
    ) -> str:
        """
        Generate intermediate model with join logic and business transformations.

        Args:
            model_name: Model name (e.g., 'int_orders_customers')
            source_models: List of upstream model names to join
            join_logic: List of join configs [{"model": "model_name", "type": "left", "on": "col1 = col2"}]
            select_columns: List of columns to select (or * if None)
            where_clause: Optional WHERE filter
            group_by: Optional list of group by columns
            materialization: Materialization strategy ('view', 'table', 'incremental')
            config_options: Optional config options
            pre_hook: Optional pre-hook SQL
            post_hook: Optional post-hook SQL (e.g., "COLLECT STATS ON {{ this }}")
            output_path: Optional output path
            unique_key: Required for incremental - column(s) for merge key
            incremental_strategy: For Teradata: 'append', 'merge', or 'delete+insert'
            incremental_column: Column used for incremental filtering (e.g., 'updated_at')
            on_schema_change: How to handle schema changes ('fail', 'ignore', etc.)

        Returns:
            Generated SQL string
        """
        try:
            # Validate incremental strategy for Teradata
            if materialization == "incremental":
                if (
                    incremental_strategy
                    and incremental_strategy not in self.TERADATA_INCREMENTAL_STRATEGIES
                ):
                    raise DBTGeneratorError(
                        f"Invalid incremental_strategy '{incremental_strategy}' for Teradata. "
                        f"Valid options: {self.TERADATA_INCREMENTAL_STRATEGIES}"
                    )
                if not unique_key and incremental_strategy in ["merge", "delete+insert"]:
                    raise DBTGeneratorError(
                        f"unique_key is required for incremental_strategy '{incremental_strategy}'"
                    )

            # Build config parts
            config_parts = [f"materialized='{materialization}'"]

            # Add incremental config options
            if materialization == "incremental":
                if unique_key:
                    config_parts.append(f"unique_key='{unique_key}'")
                if incremental_strategy:
                    config_parts.append(f"incremental_strategy='{incremental_strategy}'")
                config_parts.append(f"on_schema_change='{on_schema_change}'")

            if tags:
                tags_str = str(tags).replace("'", '"')
                config_parts.append(f"tags={tags_str}")

            if pre_hook:
                config_parts.append(f"pre_hook={_format_hook_value(pre_hook)}")
            if post_hook:
                config_parts.append(f"post_hook={_format_hook_value(post_hook)}")

            if config_options:
                for key, value in config_options.items():
                    if isinstance(value, str):
                        config_parts.append(f"{key}='{value}'")
                    elif isinstance(value, list):
                        list_str = str(value).replace("'", '"')
                        config_parts.append(f"{key}={list_str}")
                    else:
                        config_parts.append(f"{key}={value}")

            config_str = ",\n    ".join(config_parts)

            # Build CTEs for source models
            base_model = source_models[0]
            ctes = [
                f"{self.sanitize_name(base_model)} as (\n    select * from {{{{ ref('{base_model}') }}}}\n)"
            ]

            for model in source_models[1:]:
                ctes.append(
                    f"{self.sanitize_name(model)} as (\n    select * from {{{{ ref('{model}') }}}}\n)"
                )

            cte_block = "with " + ",\n\n".join(ctes) + ",\n\n"

            # Build join logic (quote column identifiers for Teradata reserved keywords)
            select_clause = (
                "select\n    " + ",\n    ".join(_quote_column(c) for c in select_columns)
                if select_columns
                else "select *"
            )

            from_clause = f"from {self.sanitize_name(base_model)}"

            join_clauses = ""
            if join_logic:
                for join in join_logic:
                    join_type = join.get("type", "left").upper()
                    join_model = join.get("model")
                    join_on = join.get("on")
                    join_clauses += (
                        f"\n{join_type} join {self.sanitize_name(join_model)}\n    on {join_on}"
                    )

            # Build WHERE clause
            where_block = f"\nwhere {where_clause}" if where_clause else ""

            # Build GROUP BY clause
            group_by_block = ""
            if group_by:
                group_by_block = "\ngroup by\n    " + ",\n    ".join(_quote_column(c) for c in group_by)

            # Build incremental filter block
            incremental_block = ""
            if materialization == "incremental" and incremental_column:
                incremental_block = f"""
{{% if is_incremental() %}}
    where {_quote_column(incremental_column)} > (select max({_quote_column(incremental_column)}) from {{{{ this }}}})
{{% endif %}}
"""

            # Build final query
            final_query = f"""joined as (

    {select_clause}
    {from_clause}{join_clauses}{where_block}{group_by_block}

)

select * from joined
{incremental_block}"""

            # Combine all parts
            sql_content = f"""{{{{ config(
    {config_str}
) }}}}

{cte_block}{final_query}
"""

            # Write to file if path provided
            if output_path:
                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=sql_content,
                        filename=str(output_path),
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated intermediate model: %s", output_file)

            return sql_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate intermediate model: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Intermediate model generation failed: {e}") from e

    def generate_mart_model(
        self,
        model_name: str,
        model_type: str,
        source_models: list[str],
        dimension_columns: list[str] | None = None,
        measure_columns: list[dict[str, str]] | None = None,
        grain: str | None = None,
        materialization: str = "table",
        config_options: dict[str, Any] | None = None,
        pre_hook: str | None = None,
        post_hook: str | None = None,
        output_path: Path | None = None,
        unique_key: str | None = None,
        incremental_strategy: str | None = None,
        incremental_column: str | None = None,
        on_schema_change: str = "fail",
        tags: list[str] | None = None,
    ) -> str:
        """
        Generate mart model (fact or dimension table) for business users.

        Args:
            model_name: Model name (e.g., 'fct_orders', 'dim_customers')
            model_type: Type of mart ('fact' or 'dimension')
            source_models: List of upstream intermediate models
            dimension_columns: List of dimension columns (for fact tables)
            measure_columns: List of measure definitions [{"name": "total_revenue", "agg": "sum(amount)"}]
            grain: Description of model grain (e.g., "One row per order")
            materialization: Materialization strategy ('table', 'view', 'incremental')
            config_options: Optional config options
            pre_hook: Optional pre-hook SQL
            post_hook: Optional post-hook SQL (e.g., stats collection)
            output_path: Optional output path
            unique_key: Required for incremental - column(s) for merge key
            incremental_strategy: For Teradata: 'append', 'merge', or 'delete+insert'
            incremental_column: Column used for incremental filtering (e.g., 'updated_at')
            on_schema_change: How to handle schema changes ('fail', 'ignore', etc.)

        Returns:
            Generated SQL string
        """
        try:
            # Validate incremental strategy for Teradata
            if materialization == "incremental":
                if (
                    incremental_strategy
                    and incremental_strategy not in self.TERADATA_INCREMENTAL_STRATEGIES
                ):
                    raise DBTGeneratorError(
                        f"Invalid incremental_strategy '{incremental_strategy}' for Teradata. "
                        f"Valid options: {self.TERADATA_INCREMENTAL_STRATEGIES}"
                    )
                if not unique_key and incremental_strategy in ["merge", "delete+insert"]:
                    raise DBTGeneratorError(
                        f"unique_key is required for incremental_strategy '{incremental_strategy}'"
                    )

            # Build config parts
            config_parts = [f"materialized='{materialization}'"]

            # Add incremental config options
            if materialization == "incremental":
                if unique_key:
                    config_parts.append(f"unique_key='{unique_key}'")
                if incremental_strategy:
                    config_parts.append(f"incremental_strategy='{incremental_strategy}'")
                config_parts.append(f"on_schema_change='{on_schema_change}'")

            # Add tags based on model type, merged with user-provided tags
            auto_tags = [model_type, "mart"]
            if tags:
                for t in tags:
                    if t not in auto_tags:
                        auto_tags.append(t)
            all_tags_str = str(auto_tags).replace("'", '"')
            config_parts.append(f"tags={all_tags_str}")

            if pre_hook:
                config_parts.append(f"pre_hook={_format_hook_value(pre_hook)}")
            if post_hook:
                config_parts.append(f"post_hook={_format_hook_value(post_hook)}")

            if config_options:
                for key, value in config_options.items():
                    if isinstance(value, str):
                        config_parts.append(f"{key}='{value}'")
                    elif isinstance(value, list):
                        list_str = str(value).replace("'", '"')
                        config_parts.append(f"{key}={list_str}")
                    else:
                        config_parts.append(f"{key}={value}")

            config_str = ",\n    ".join(config_parts)

            # Build CTEs
            ctes = []
            for model in source_models:
                cte_name = self.sanitize_name(model)
                ctes.append(f"{cte_name} as (\n    select * from {{{{ ref('{model}') }}}}\n)")

            cte_block = "with " + ",\n\n".join(ctes) + ",\n\n" if ctes else ""

            # Build final model based on type
            if model_type.lower() == "fact":
                # Fact table with measures
                select_parts = []

                # Add dimension columns (quote for Teradata reserved keywords)
                if dimension_columns:
                    select_parts.extend(_quote_column(c) for c in dimension_columns)

                # Add measures
                if measure_columns:
                    for measure in measure_columns:
                        measure_expr = measure.get("agg", measure.get("name"))
                        measure_name = measure.get("name")
                        select_parts.append(f"{measure_expr} as {_quote_column(measure_name)}")

                select_clause = (
                    "select\n    " + ",\n    ".join(select_parts) if select_parts else "select *"
                )
                from_clause = f"from {self.sanitize_name(source_models[0])}"

                # Add group by for aggregations
                group_by = ""
                if measure_columns and dimension_columns:
                    group_by = "\ngroup by\n    " + ",\n    ".join(_quote_column(c) for c in dimension_columns)

                final_query = f"""final as (
    
    {select_clause}
    {from_clause}{group_by}
    
)

select * from final"""

            else:  # dimension
                # Dimension table - typically just select distinct
                select_clause = (
                    "select distinct *"
                    if not dimension_columns
                    else "select distinct\n    " + ",\n    ".join(_quote_column(c) for c in dimension_columns)
                )
                from_clause = f"from {self.sanitize_name(source_models[0])}"

                final_query = f"""final as (
    
    {select_clause}
    {from_clause}
    
)

select * from final"""

            # Build incremental filter block
            incremental_block = ""
            if materialization == "incremental" and incremental_column:
                incremental_block = f"""
{{% if is_incremental() %}}
    where {_quote_column(incremental_column)} > (select max({_quote_column(incremental_column)}) from {{{{ this }}}})
{{% endif %}}
"""

            # Add grain comment if provided
            grain_comment = f"-- Grain: {grain}\n\n" if grain else ""

            # Combine all parts
            sql_content = f"""{{{{ config(
    {config_str}
) }}}}

{grain_comment}{cte_block}{final_query}
{incremental_block}"""

            # Write to file if path provided
            if output_path:
                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=sql_content,
                        filename=str(output_path),
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated mart model (%s): %s", model_type, output_file)

            return sql_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate mart model: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Mart model generation failed: {e}") from e

    # ==================== Test Generation ====================

    def generate_schema_tests(
        self,
        model_name: str,
        column_tests: dict[str, list[str | dict[str, Any]]],
        model_tests: list[str | dict[str, Any]] | None = None,
        model_description: str = "",
        column_descriptions: dict[str, str] | None = None,
        output_path: Path | None = None,
    ) -> str:
        """
        Generate schema.yml with tests including severity and relationships.

        Args:
            model_name: Model name
            column_tests: Dict mapping column names to list of tests (str or dict with severity)
            model_tests: Optional list of model-level tests
            model_description: Optional model description
            column_descriptions: Optional dict of column descriptions
            output_path: Optional output path

        Returns:
            Generated YAML string

        Examples:
            column_tests = {
                "id": ["unique", "not_null"],
                "email": [{"not_null": {"severity": "error"}}, {"unique": {"severity": "warn"}}],
                "customer_id": [{"relationships": {"to": "ref('customers')", "field": "id", "severity": "error"}}]
            }
        """
        try:
            # Build column definitions (quote names for Teradata reserved keywords)
            columns = []
            for col_name, tests in column_tests.items():
                col_def = {
                    "name": _quote_column(col_name),
                    "description": column_descriptions.get(col_name, "")
                    if column_descriptions
                    else "",
                    "tests": tests if tests else None,
                }
                columns.append(col_def)

            # Build model definition
            model_def = {
                "name": model_name,
                "description": model_description or "",
                "columns": columns,
            }

            if model_tests:
                model_def["tests"] = model_tests

            # Render template
            context = {"models": [model_def]}

            template = self.env.from_string(self.SCHEMA_YML_TEMPLATE)
            yaml_content = template.render(**context)

            # Write to file if path provided, merging with existing content
            if output_path:
                filename = str(output_path)
                try:
                    existing_text = self.file_writer.read_file_safe(filename)
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                if existing_text:
                    existing = yaml.safe_load(existing_text)
                    if existing and "models" in existing:
                        new_parsed = yaml.safe_load(yaml_content)
                        existing_models = {m["name"]: m for m in existing.get("models", [])}
                        for m in new_parsed.get("models", []):
                            existing_models[m["name"]] = m
                        existing["models"] = list(existing_models.values())
                        yaml_content = yaml.dump(existing, default_flow_style=False, sort_keys=False)

                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=yaml_content,
                        filename=filename,
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated schema tests: %s", output_file)

            return yaml_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate schema tests: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Schema test generation failed: {e}") from e

    def generate_data_test(
        self,
        test_name: str,
        test_sql: str,
        output_path: Path | None = None,
    ) -> str:
        """
        Generate custom data test SQL file.

        Args:
            test_name: Test name (filename will be {test_name}.sql)
            test_sql: SQL query that returns failing rows
            output_path: Optional output path

        Returns:
            Test SQL string
        """
        try:
            # Data tests should return rows that fail the test
            sql_content = f"-- Test: {test_name}\n-- Returns rows that fail the test\n\n{test_sql}"

            # Write to file if path provided
            if output_path:
                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=sql_content,
                        filename=str(output_path),
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated data test: %s", output_file)

            return sql_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate data test: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Data test generation failed: {e}") from e

    def infer_tests_from_metadata(
        self,
        table_metadata: dict[str, Any],
        include_uniqueness: bool = True,
        include_not_null: bool = True,
        include_relationships: bool = False,
        default_severity: str = "error",
    ) -> dict[str, list[str | dict[str, Any]]]:
        """
        Infer appropriate tests from table metadata with severity levels.

        Args:
            table_metadata: Table metadata from TeradataClient
            include_uniqueness: Include unique tests for PKs
            include_not_null: Include not_null tests
            include_relationships: Include relationship tests (requires FK info)
            default_severity: Default severity level ('error' or 'warn')

        Returns:
            Dict mapping column names to list of test configs (str or dict with severity)
        """
        column_tests = {}

        try:
            for col in table_metadata.get("columns", []):
                raw_name = col.get("column_name") or col.get("name") or ""
                col_name = raw_name.lower()

                # Validate column name - skip columns with missing or whitespace-only names
                if not col_name.strip():
                    logger.warning(
                        "Skipping column with missing or whitespace-only name in table metadata. "
                        "Column: %s",
                        col,
                    )
                    continue

                tests = []

                # Primary key columns - always error severity
                # Note: Support both 'is_primary_key' (standard) and 'primary_key' (legacy/alternative)
                # to handle metadata from different sources (TeradataClient, user-provided, etc.)
                if col.get("is_primary_key") or col.get("primary_key"):
                    if include_uniqueness:
                        tests.append({"unique": {"severity": "error"}})
                    if include_not_null:
                        tests.append({"not_null": {"severity": "error"}})

                # Non-nullable columns
                elif not col.get("nullable", True) and include_not_null:
                    tests.append({"not_null": {"severity": default_severity}})

                # Foreign key relationships
                if include_relationships and col.get("is_foreign_key"):
                    fk_info = col.get("foreign_key_info", {})
                    if fk_info:
                        ref_table = fk_info.get("referenced_table", "")
                        ref_column = fk_info.get("referenced_column", "id")
                        # Convert table name to likely model name (stg_tablename)
                        model_ref = f"ref('stg_{ref_table.lower()}')"
                        tests.append(
                            {
                                "relationships": {
                                    "to": model_ref,
                                    "field": ref_column,
                                    "severity": "error",
                                }
                            }
                        )

                if tests:
                    column_tests[col_name] = tests

            return column_tests

        except Exception as e:
            logger.error("Failed to infer tests from metadata: %s", e, exc_info=True)
            return {}

    # ==================== Documentation Generation ====================

    def generate_model_documentation(
        self,
        models: list[dict[str, Any]],
        output_path: Path | None = None,
    ) -> str:
        """
        Generate documentation YAML for multiple models.

        Args:
            models: List of model definition dicts
            output_path: Optional output path

        Returns:
            Generated YAML string
        """
        try:
            context = {"models": models}

            template = self.env.from_string(self.SCHEMA_YML_TEMPLATE)
            yaml_content = template.render(**context)

            # Write to file if path provided
            if output_path:
                try:
                    output_file, _ = self.file_writer.write_file_safe(
                        content=yaml_content,
                        filename=str(output_path),
                    )
                except FileOperationError as e:
                    raise DBTGeneratorError(f"invalid output_path: {e}") from e

                logger.info("Generated model documentation: %s", output_file)

            return yaml_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate model documentation: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Documentation generation failed: {e}") from e

    # ==================== Bulk Generation ====================

    def generate_staging_layer(
        self,
        source_name: str,
        table_metadata_list: list[dict[str, Any]],
        models_dir: str = "models/staging",
        materialization: str = "view",
        generate_tests: bool = True,
    ) -> dict[str, Any]:
        """
        Generate complete staging layer for multiple tables.

        Args:
            source_name: Source name
            table_metadata_list: List of table metadata dicts
            models_dir: Directory for model files (relative to project)
            materialization: Materialization strategy
            generate_tests: Whether to generate schema tests

        Returns:
            Dictionary with generation summary
        """
        try:
            results = {
                "models_generated": [],
                "tests_generated": [],
                "errors": [],
            }

            for table_meta in table_metadata_list:
                table_name = table_meta.get("table_name")
                model_name = f"stg_{source_name}_{table_name}".lower()

                try:
                    # Extract columns
                    columns = [col["column_name"] for col in table_meta.get("columns", [])]

                    # Generate model
                    model_path = Path(models_dir) / source_name / f"{model_name}.sql"
                    self.generate_staging_model(
                        model_name=model_name,
                        source_name=source_name,
                        table_name=table_name,
                        columns=columns,
                        materialization=materialization,
                        output_path=model_path,
                    )

                    results["models_generated"].append(str(model_path))

                    # Generate tests
                    if generate_tests:
                        column_tests = self.infer_tests_from_metadata(table_meta)

                        if column_tests:
                            test_path = Path(models_dir) / source_name / f"_schema_{model_name}.yml"
                            self.generate_schema_tests(
                                model_name=model_name,
                                column_tests=column_tests,
                                model_description=table_meta.get("description", ""),
                                output_path=test_path,
                            )

                            results["tests_generated"].append(str(test_path))

                except Exception as e:
                    error_msg = f"Failed to generate {model_name}: {e}"
                    logger.error("Failed to generate %s: %s", model_name, e, exc_info=True)
                    results["errors"].append(error_msg)

            logger.info(
                "Staging layer generation complete: %d models, %d test files, %d errors",
                len(results["models_generated"]),
                len(results["tests_generated"]),
                len(results["errors"]),
            )

            return results

        except Exception as e:
            logger.error("Failed to generate staging layer: %s", e, exc_info=True)
            raise

    # ==================== Utility Methods ====================

    def sanitize_name(self, name: str) -> str:
        """
        Sanitize name for dbt (lowercase, replace spaces with underscores).

        Args:
            name: Original name

        Returns:
            Sanitized name
        """
        # Convert to lowercase
        name = name.lower()

        # Replace spaces and special chars with underscores
        name = re.sub(r"[^a-z0-9_]", "_", name)

        # Remove consecutive underscores
        name = re.sub(r"_+", "_", name)

        # Remove leading/trailing underscores
        name = name.strip("_")

        return name

    def get_dbt_data_type(self, teradata_type: str) -> str:
        """
        Convert Teradata data type to dbt-friendly name.

        Args:
            teradata_type: Teradata data type

        Returns:
            dbt data type name
        """
        # Normalize type
        td_type = teradata_type.upper().split("(")[0]

        # Type mapping
        type_map = {
            "INTEGER": "integer",
            "INT": "integer",
            "SMALLINT": "integer",
            "BIGINT": "bigint",
            "BYTEINT": "integer",
            "DECIMAL": "numeric",
            "NUMERIC": "numeric",
            "NUMBER": "numeric",
            "FLOAT": "float",
            "REAL": "float",
            "DOUBLE": "float",
            "DOUBLE PRECISION": "float",
            "CHAR": "string",
            "CHARACTER": "string",
            "VARCHAR": "string",
            "CLOB": "string",
            "DATE": "date",
            "TIME": "time",
            "TIMESTAMP": "timestamp",
            "INTERVAL": "string",
            "BLOB": "binary",
            "BYTE": "binary",
            "VARBYTE": "binary",
        }

        return type_map.get(td_type, "string")

    def generate_profiles_yml(
        self,
        profile_name: str,
        auth: "TeradataAuth",
        target: str = "dev",
        threads: int = 4,
    ) -> str:
        """
        Generate dbt profiles.yml for Teradata connection.

        Creates a profiles.yml file in the project directory (not the default ~/.dbt).
        This allows for project-specific profiles that can be version controlled
        (with secrets managed via environment variables).

        The YAML body comes from :meth:`TeradataAuth.render_for_dbt_profile_yaml`,
        which references every relevant TERADATA_* env var via Jinja so the
        profile works for any mechanism the user switches to (via rerunning
        the wizard or passing a different ``teradata_profile`` on dbt tool
        calls). All five mechanisms including BEARER are supported.

        Args:
            profile_name: Name of the dbt profile (should match 'profile' in dbt_project.yml)
            auth: Resolved Teradata authentication identity — determines the
                static ``type``/``tmode`` and the mechanism-specific YAML keys.
            target: dbt target name (default: 'dev')
            threads: Number of threads for parallel execution (default: 4)

        Returns:
            Generated profiles.yml content as string
        """
        try:
            target_config: dict[str, Any] = auth.render_for_dbt_profile_yaml()
            target_config["threads"] = threads

            profiles_config = {
                profile_name: {
                    "target": target,
                    "outputs": {
                        target: target_config,
                    },
                }
            }

            # Generate YAML content — yaml.dump wraps Jinja expressions in
            # quotes which is exactly what dbt expects for env_var() calls.
            yaml_content = yaml.dump(profiles_config, default_flow_style=False, sort_keys=False)

            # Write to file via SafeFileWriter
            profiles_path, _ = self.file_writer.write_file_safe(
                content=yaml_content,
                filename="profiles.yml",
            )

            logger.info("Generated profiles.yml at %s", profiles_path)

            return yaml_content

        except DBTGeneratorError:
            raise
        except FileOperationError as e:
            logger.error("Failed to generate profiles.yml: %s", e, exc_info=True)
            raise DBTGeneratorError(f"profiles.yml generation failed: {e}") from e
        except Exception as e:
            logger.error("Failed to generate profiles.yml: %s", e, exc_info=True)
            raise DBTGeneratorError(f"profiles.yml generation failed: {e}") from e

    def create_project_structure(
        self,
        project_name: str,
        include_staging: bool = True,
        include_intermediate: bool = True,
        include_marts: bool = True,
        mart_subfolders: list[str] | None = None,
        include_snapshots: bool = True,
        include_tests: bool = True,
        include_macros: bool = True,
        include_analysis: bool = False,
        staging_materialization: str = "view",
        intermediate_materialization: str = "view",
        marts_materialization: str = "table",
        auth: "TeradataAuth | None" = None,
        target: str = "dev",
        threads: int = 4,
        *,
        identity: str | None = None,
    ) -> dict[str, Any]:
        """
        Create standard dbt project folder structure following dbt best practices.

        Args:
            project_name: Name of the dbt project
            include_staging: Create staging folder
            include_intermediate: Create intermediate folder
            include_marts: Create marts folder
            mart_subfolders: Optional list of mart subfolder names (business domains).
                            **Default is None - no subfolders are created unless explicitly requested.**
                            Only provide this if the user specifically asks for subfolders.
                            Example values (only if requested): ["finance", "marketing"], ["sales"].
            include_snapshots: Create snapshots folder
            include_tests: Create tests folder
            include_macros: Create macros folder
            include_analysis: Create analysis folder
            staging_materialization: Materialization strategy for staging models (default: 'view').
                                    Common options: 'view', 'table', 'ephemeral'
            intermediate_materialization: Materialization strategy for intermediate models (default: 'view').
                                         Common options: 'view', 'table', 'ephemeral'
            marts_materialization: Materialization strategy for marts models (default: 'table').
                                  Common options: 'table', 'view', 'incremental'
            auth: Optional :class:`TeradataAuth`. When provided, a profiles.yml
                is generated in the project directory with Jinja ``env_var()``
                refs for every mechanism-specific field.
            target: dbt target name for profiles.yml (default: 'dev')
            threads: Number of threads for profiles.yml (default: 4)
            identity: Resolved Teradata identity (named profile or
                ``wizard:<host_slug>``) bound to this sub-project. Becomes the
                ``profile:`` field in ``dbt_project.yml`` and the entry key in
                ``profiles.yml``, enabling reverse lookup of "which sub-project
                belongs to this Teradata identity?". When ``None``, falls back
                to ``project_name`` for backward compatibility with single-
                project layouts.

        Returns:
            Dictionary with created paths
        """
        try:
            created_paths = {
                "folders": [],
                "files": [],
            }

            # Create main models directory
            models_dir = self.project_dir / "models"
            models_dir.mkdir(parents=True, exist_ok=True)
            created_paths["folders"].append(str(models_dir))

            # Create staging layer
            if include_staging:
                staging_dir = models_dir / "staging"
                staging_dir.mkdir(exist_ok=True)
                created_paths["folders"].append(str(staging_dir))

                # Create .gitkeep to ensure folder is tracked
                gitkeep = staging_dir / ".gitkeep"
                gitkeep.touch()
                created_paths["files"].append(str(gitkeep))

            # Create intermediate layer
            if include_intermediate:
                intermediate_dir = models_dir / "intermediate"
                intermediate_dir.mkdir(exist_ok=True)
                created_paths["folders"].append(str(intermediate_dir))

                gitkeep = intermediate_dir / ".gitkeep"
                gitkeep.touch()
                created_paths["files"].append(str(gitkeep))

            # Create marts layer
            if include_marts:
                marts_dir = models_dir / "marts"
                marts_dir.mkdir(exist_ok=True)
                created_paths["folders"].append(str(marts_dir))

                # Create mart subfolders if specified (organized by business domain)
                if mart_subfolders:
                    for subfolder in mart_subfolders:
                        subfolder_path = marts_dir / subfolder
                        subfolder_path.mkdir(exist_ok=True)
                        created_paths["folders"].append(str(subfolder_path))

                        gitkeep = subfolder_path / ".gitkeep"
                        gitkeep.touch()
                        created_paths["files"].append(str(gitkeep))
                else:
                    # No subfolders - create .gitkeep in marts root
                    gitkeep = marts_dir / ".gitkeep"
                    gitkeep.touch()
                    created_paths["files"].append(str(gitkeep))

            # Create snapshots directory
            if include_snapshots:
                snapshots_dir = self.project_dir / "snapshots"
                snapshots_dir.mkdir(exist_ok=True)
                created_paths["folders"].append(str(snapshots_dir))

                gitkeep = snapshots_dir / ".gitkeep"
                gitkeep.touch()
                created_paths["files"].append(str(gitkeep))

            # Create tests directory
            if include_tests:
                tests_dir = self.project_dir / "tests"
                tests_dir.mkdir(exist_ok=True)
                created_paths["folders"].append(str(tests_dir))

                gitkeep = tests_dir / ".gitkeep"
                gitkeep.touch()
                created_paths["files"].append(str(gitkeep))

            # Create macros directory
            if include_macros:
                macros_dir = self.project_dir / "macros"
                macros_dir.mkdir(exist_ok=True)
                created_paths["folders"].append(str(macros_dir))

                gitkeep = macros_dir / ".gitkeep"
                gitkeep.touch()
                created_paths["files"].append(str(gitkeep))

            # Create analysis directory
            if include_analysis:
                analysis_dir = self.project_dir / "analysis"
                analysis_dir.mkdir(exist_ok=True)
                created_paths["folders"].append(str(analysis_dir))

                gitkeep = analysis_dir / ".gitkeep"
                gitkeep.touch()
                created_paths["files"].append(str(gitkeep))

            # Create seeds directory
            seeds_dir = self.project_dir / "seeds"
            seeds_dir.mkdir(exist_ok=True)
            created_paths["folders"].append(str(seeds_dir))

            gitkeep = seeds_dir / ".gitkeep"
            gitkeep.touch()
            created_paths["files"].append(str(gitkeep))

            # The ``profile:`` field in dbt_project.yml MUST match the entry
            # key in profiles.yml. When the caller supplies an explicit
            # identity (e.g. ``wizard:<host_slug>`` or a named connections.yaml
            # profile), use that; otherwise fall back to ``project_name`` for
            # backward compatibility with single-project layouts.
            profile_field = identity if identity is not None else project_name

            # Create dbt_project.yml if it doesn't exist
            dbt_project_yml = self.project_dir / "dbt_project.yml"
            if not dbt_project_yml.exists():
                project_config = f"""name: '{project_name}'
version: '1.0.0'
config-version: 2

profile: '{profile_field}'

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

dispatch:
  - macro_namespace: dbt_utils
    search_order: ['teradata_utils', 'dbt_utils']

models:
  {project_name}:
    staging:
      +materialized: {staging_materialization}
    intermediate:
      +materialized: {intermediate_materialization}
    marts:
      +materialized: {marts_materialization}
"""
                dbt_project_yml, _ = self.file_writer.write_file_safe(
                    content=project_config,
                    filename="dbt_project.yml",
                )

                created_paths["files"].append(str(dbt_project_yml))
                logger.info("Created dbt_project.yml: %s", dbt_project_yml)

            # Generate profiles.yml if auth is provided. The entry key MUST
            # match the ``profile:`` field above so dbt can find the profile.
            dotenv_keys: list[str] = []
            if auth is not None:
                try:
                    self.generate_profiles_yml(
                        profile_name=profile_field,
                        auth=auth,
                        target=target,
                        threads=threads,
                    )
                    profiles_path = self.project_dir / "profiles.yml"
                    created_paths["files"].append(str(profiles_path))
                    logger.info("Created profiles.yml: %s", profiles_path)
                except Exception as e:
                    logger.warning("Failed to create profiles.yml: %s", e)

                # Write .env next to profiles.yml so `dotenv run -- dbt ...`
                # loads TERADATA_* automatically. The dict is scoped to this
                # block; never returned, never logged.
                try:
                    env_dict = auth.render_for_dbt_env()
                    dotenv_path = self.project_dir / ".env"
                    dotenv_keys = _write_dotenv_file(dotenv_path, env_dict)
                    created_paths["files"].append(str(dotenv_path))
                    logger.info(
                        "Wrote .env with %d TERADATA_* keys at %s",
                        len(dotenv_keys),
                        dotenv_path,
                    )
                    # Drop the value-bearing dict eagerly. The list of key
                    # names (``dotenv_keys``) is the only thing that
                    # outlives this block.
                    del env_dict
                except Exception as e:
                    logger.warning("Failed to create .env: %s", e)

            # Create packages.yml if it doesn't exist
            packages_yml = self.project_dir / "packages.yml"
            if not packages_yml.exists():
                packages_content = """packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: Teradata/teradata_utils
    version: [">=1.3.0", "<2.0.0"]
"""
                packages_yml, _ = self.file_writer.write_file_safe(
                    content=packages_content,
                    filename="packages.yml",
                )
                created_paths["files"].append(str(packages_yml))
                logger.info("Created packages.yml: %s", packages_yml)

            # Create .gitignore if it doesn't exist. ``.env`` is included
            # because the scaffold writes a credential-bearing .env next to
            # profiles.yml — checking it in would defeat the per-sub-project
            # secret hygiene model.
            gitignore = self.project_dir / ".gitignore"
            if not gitignore.exists():
                gitignore_content = """target/
dbt_packages/
logs/
.env
"""
                gitignore, _ = self.file_writer.write_file_safe(
                    content=gitignore_content,
                    filename=".gitignore",
                )
                created_paths["files"].append(str(gitignore))
                logger.info("Created .gitignore: %s", gitignore)
            elif ".env" not in gitignore.read_text(encoding="utf-8").splitlines():
                # Pre-existing .gitignore (e.g. user customised before we
                # added .env protection) — append the line so we never write
                # a credential-bearing .env into a repo that doesn't ignore it.
                with gitignore.open("a", encoding="utf-8") as fh:
                    fh.write(".env\n")
                logger.info("Appended .env to existing .gitignore: %s", gitignore)

            logger.info(
                "Created project structure with %d folders and %d files",
                len(created_paths["folders"]),
                len(created_paths["files"]),
            )

            return {
                "success": True,
                "project_name": project_name,
                "created_paths": created_paths,
            }

        except Exception as e:
            logger.error("Failed to create project structure: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Project structure creation failed: {e}") from e

    # ==================== Multi-Target Profiles ====================

    def generate_multi_target_profiles_yml(
        self,
        profile_name: str,
        targets: list[dict[str, Any]],
    ) -> str:
        """
        Generate dbt profiles.yml with multiple target environments.

        Args:
            profile_name: Name of the dbt profile
            targets: List of target definitions, each with:
                - name: Target name (e.g., 'dev', 'staging', 'prod')
                - auth: :class:`TeradataAuth` for this target's identity
                - threads: Optional threads count (default: 4)

        Returns:
            Generated profiles.yml content as string
        """
        try:
            if not targets:
                raise DBTGeneratorError("At least one target is required")

            outputs = {}
            default_target = targets[0]["name"]

            for target_def in targets:
                target_name = target_def["name"]
                target_auth = target_def["auth"]
                threads = target_def.get("threads", 4)

                # Multi-target profiles need per-target host/port/database
                # (the whole point of dev vs prod split), so embed those
                # literally from each auth. User/password/logmech/logdata
                # still flow via env_var() so the caller's identity
                # (wizard default or teradata_profile override) takes effect
                # per invocation regardless of which target is selected.
                body = target_auth.render_for_dbt_profile_yaml()
                body["host"] = target_auth.host
                body["port"] = str(target_auth.port)
                body["schema"] = target_auth.database
                body["threads"] = threads
                outputs[target_name] = body

            profiles_config = {
                profile_name: {
                    "target": default_target,
                    "outputs": outputs,
                }
            }

            yaml_content = yaml.dump(profiles_config, default_flow_style=False, sort_keys=False)

            profiles_path, _ = self.file_writer.write_file_safe(
                content=yaml_content,
                filename="profiles.yml",
            )

            logger.info(
                "Generated multi-target profiles.yml at %s with targets: %s",
                profiles_path,
                [t["name"] for t in targets],
            )

            return yaml_content

        except DBTGeneratorError:
            raise
        except Exception as e:
            logger.error("Failed to generate multi-target profiles.yml: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Multi-target profiles.yml generation failed: {e}") from e

    # ==================== Package Management ====================

    def add_package(
        self,
        package_name: str,
        version: str | None = None,
    ) -> dict[str, Any]:
        """
        Add a dbt package to packages.yml.

        Args:
            package_name: Package name (e.g., 'calogica/dbt_expectations')
            version: Optional version constraint (e.g., '>=0.10.0')

        Returns:
            Dictionary with updated packages list
        """
        try:
            # Read existing packages via SafeFileWriter (path containment enforced)
            existing_text = self.file_writer.read_file_safe("packages.yml")
            if existing_text:
                packages_data = yaml.safe_load(existing_text) or {}
            else:
                packages_data = {}

            packages_list = packages_data.get("packages", [])

            # Check if package already exists
            for pkg in packages_list:
                if pkg.get("package") == package_name:
                    # Update version if different
                    if version:
                        pkg["version"] = version
                    logger.info("Updated existing package: %s", package_name)
                    break
            else:
                # Add new package
                new_pkg: dict[str, Any] = {"package": package_name}
                if version:
                    new_pkg["version"] = version
                packages_list.append(new_pkg)
                logger.info("Added new package: %s", package_name)

            packages_data["packages"] = packages_list

            # Write back via SafeFileWriter
            packages_yaml_content = yaml.dump(
                packages_data, default_flow_style=False, sort_keys=False
            )
            packages_path, _ = self.file_writer.write_file_safe(
                content=packages_yaml_content,
                filename="packages.yml",
            )

            return {
                "success": True,
                "package_name": package_name,
                "version": version,
                "total_packages": len(packages_list),
                "packages": [p.get("package") for p in packages_list],
                "packages_path": str(packages_path),
            }

        except Exception as e:
            logger.error("Failed to add package: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Package addition failed: {e}") from e

    # ==================== Teradata Macros ====================

    def generate_teradata_macros(self) -> dict[str, Any]:
        """
        Generate common Teradata-specific utility macros.

        Creates macro files for Teradata optimizations that aren't
        available in any dbt package.

        Returns:
            Dictionary with created macro files
        """
        try:
            macros_dir = self.project_dir / "macros"
            macros_dir.mkdir(parents=True, exist_ok=True)

            created_files = []

            # 1. Collect Statistics macro
            collect_stats_sql = """{%- macro collect_stats(relation, columns=none) -%}
    {%- if columns -%}
        {%- for col in columns %}
    COLLECT STATISTICS ON {{ relation }} COLUMN ({{ col }});
        {%- endfor %}
    {%- else %}
    COLLECT STATISTICS ON {{ relation }};
    {%- endif -%}
{%- endmacro -%}

{%- macro collect_stats_hook() -%}
    {{ collect_stats(this) }}
{%- endmacro -%}
"""
            collect_stats_path, _ = self.file_writer.write_file_safe(
                content=collect_stats_sql,
                filename="macros/collect_stats.sql",
            )
            created_files.append(str(collect_stats_path))

            # 2. Grant access macro
            grant_access_sql = """{%- macro grant_select(relation, role) -%}
    GRANT SELECT ON {{ relation }} TO {{ role }};
{%- endmacro -%}

{%- macro grant_all(relation, role) -%}
    GRANT ALL ON {{ relation }} TO {{ role }};
{%- endmacro -%}
"""
            grant_access_path, _ = self.file_writer.write_file_safe(
                content=grant_access_sql,
                filename="macros/grant_access.sql",
            )
            created_files.append(str(grant_access_path))

            # 3. Teradata utilities
            td_utils_sql = """{%- macro set_table_type(table_type='MULTISET') -%}
    {#- Controls whether table is SET or MULTISET -#}
    {%- if table_type | upper == 'MULTISET' -%}
        , multiset=true
    {%- endif -%}
{%- endmacro -%}

{%- macro teradata_hash(columns) -%}
    {#- Generate a hash key from multiple columns using Teradata's HASHROW -#}
    HASHROW({{ columns | join(', ') }})
{%- endmacro -%}

{%- macro safe_cast_timestamp(column) -%}
    {#- Safely cast a column to TIMESTAMP, returning NULL on failure -#}
    CASE WHEN {{ column }} IS NOT NULL
         THEN CAST({{ column }} AS TIMESTAMP(6))
         ELSE NULL
    END
{%- endmacro -%}
"""
            td_utils_path, _ = self.file_writer.write_file_safe(
                content=td_utils_sql,
                filename="macros/teradata_utils.sql",
            )
            created_files.append(str(td_utils_path))

            logger.info("Generated %d Teradata macro files", len(created_files))

            return {
                "success": True,
                "macros_generated": len(created_files),
                "macro_files": created_files,
            }

        except Exception as e:
            logger.error("Failed to generate Teradata macros: %s", e, exc_info=True)
            raise DBTGeneratorError(f"Teradata macro generation failed: {e}") from e
