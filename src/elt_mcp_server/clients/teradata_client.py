"""Teradata database client for metadata extraction and operations.

This module provides comprehensive Teradata database operations including:
- Metadata extraction (tables, columns, constraints, indexes)
- Column-level statistics (null percentage, cardinality)
- Table lineage tracking (upstream/downstream dependencies)
- Data profiling (min, max, avg, distribution)
- Schema change detection
- Data preview and sampling
- Query execution
"""

import logging
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from ..auth import TeradataAuth
from ..response_sanitizer import safe_error_message

try:
    import teradatasql
except ImportError:
    teradatasql = None

try:
    import pandas as pd
except ImportError:
    pd = None


logger = logging.getLogger(__name__)


class TeradataClientError(Exception):
    """Base exception for Teradata client errors."""

    pass


class TeradataConnectionError(TeradataClientError):
    """Raised when connection to Teradata fails."""

    pass


class TeradataQueryError(TeradataClientError):
    """Raised when query execution fails."""

    pass


def _serialize_value(val: Any) -> Any:
    """
    Convert non-JSON-serializable values to JSON-safe types.

    Handles: datetime, date, Decimal, bytes, and pandas NA values.

    Args:
        val: Value to serialize

    Returns:
        JSON-serializable value
    """
    if pd is not None and pd.isna(val):
        return None
    elif isinstance(val, (datetime, date)):
        return val.isoformat()
    elif isinstance(val, Decimal):
        return float(val)
    elif isinstance(val, bytes):
        return val.decode("utf-8", errors="ignore")
    elif isinstance(val, (int, float, str, bool)) or val is None:
        return val
    else:
        return str(val)


def _check_teradatasql():
    """Check if teradatasql is available."""
    if teradatasql is None:
        raise ImportError("teradatasql package is required. Install with: pip install teradatasql")


def _check_pandas():
    """Check if pandas is available."""
    if pd is None:
        raise ImportError("pandas package is required. Install with: pip install pandas")


_TERADATA_IDENTIFIER_RE = re.compile(r"^[A-Za-z_$#][A-Za-z0-9_$#]{0,127}$")

# Characters that Teradata unconditionally forbids inside any object name.
_TD_DISALLOWED_CHARS = re.compile(
    "[\u0000\u001a\ufffd\ufa6c\ufa6f\ufad0\ufad1\ufad5\ufad6\ufad7]"
)


def _quote_identifier(name: str) -> str:
    """Validate and double-quote a Teradata SQL identifier.

    Follows Teradata object naming rules:
    - Trailing whitespace is stripped (not part of the name per spec).
    - All-whitespace / empty names are rejected.
    - Disallowed characters (NULL U+0000, SUBSTITUTE U+001A,
      REPLACEMENT CHARACTER U+FFFD, and select compatibility ideographs)
      are rejected.
    - Names must match the standard unquoted identifier pattern
      (prevents SQL injection via crafted names).
    """
    if not name or not name.rstrip():
        raise ValueError(f"Empty or all-whitespace identifier: {name!r}")
    name = name.rstrip()
    if _TD_DISALLOWED_CHARS.search(name):
        raise ValueError(
            f"Identifier contains disallowed Teradata characters: {name!r}"
        )
    if not _TERADATA_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid Teradata identifier: {name!r}")
    return f'"{name}"'


def _safe_int(value: int | None, name: str, minimum: int = 1) -> int:
    """Validate and cast a numeric SQL clause value to int."""
    if value is None:
        raise ValueError(f"{name} must not be None")
    result = int(value)
    if result < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {result}")
    return result


class TeradataClient:
    """
    Comprehensive Teradata database client using native teradatasql driver.

    Provides methods for metadata extraction, profiling, lineage tracking,
    and general database operations using direct teradatasql connections
    with pandas for data handling.
    """

    def __init__(
        self,
        auth: TeradataAuth,
        charset: str = "UTF8",
        pool_size: int = 5,  # kept for compatibility
        max_overflow: int = 10,  # kept for compatibility
        pool_timeout: int = 30,  # kept for compatibility
        query_timeout: int = 300,
    ):
        """
        Initialize Teradata client.

        Args:
            auth: Resolved Teradata authentication identity — carries host,
                port, database, mechanism, and all mechanism-specific fields.
                Built by the resolver from either wizard-populated Settings
                or a ``connections.yaml`` profile.
            charset: Character set (default: UTF8).
            query_timeout: Query execution timeout in seconds.
        """
        _check_teradatasql()
        _check_pandas()

        self.auth = auth
        self.charset = charset
        self.query_timeout = query_timeout

        self._connected = False

        logger.info("Initialized Teradata client for %s", auth.host)

    # Read-only projections onto :class:`TeradataAuth` fields. Kept because
    # many call sites in this file (and tests) refer to ``self.host``,
    # ``self.database``, etc. The source of truth is ``self.auth``.

    @property
    def host(self) -> str:
        return self.auth.host

    @property
    def port(self) -> int:
        return self.auth.port

    @property
    def database(self) -> str:
        return self.auth.database

    @property
    def username(self) -> str:
        return self.auth.username

    @property
    def password(self) -> str:
        return self.auth.password

    @property
    def logmech(self) -> str:
        return self.auth.mechanism

    @property
    def logdata(self) -> str:
        return self.auth.logdata

    @property
    def oidc_clientid(self) -> str:
        return self.auth.oidc_clientid

    @property
    def jws_private_key(self) -> str:
        return self.auth.jws_private_key

    @property
    def jws_cert(self) -> str:
        return self.auth.jws_cert

    @property
    def sslca(self) -> str:
        return self.auth.sslca

    def _get_connection(self):
        """
        Create a new Teradata connection.

        Returns:
            teradatasql connection object

        Raises:
            TeradataConnectionError: If connection cannot be established
        """
        try:
            # Auth portion of the kwargs is the renderer's output; we only
            # add connection-level defaults here.
            connect_params: dict[str, Any] = self.auth.render_for_teradatasql()
            connect_params["encryptdata"] = "true"
            connect_params["connect_timeout"] = str(self.query_timeout * 1000)
            connect_params["request_timeout"] = str(self.query_timeout * 1000)

            # Log JDBC-style connection URL (secrets masked)
            secret_keys = ("password", "logdata", "oidc_clientsecret")
            skip_keys = ("host", "database", "dbs_port", "encryptdata", "connect_timeout", "request_timeout")
            url_params = []
            for k, v in connect_params.items():
                if k in skip_keys:
                    continue
                url_params.append(f"{k.upper()}={'***' if k in secret_keys else v}")
            db_part = f"DATABASE={connect_params.get('database', '')}" if connect_params.get("database") else ""
            jdbc_url = f"jdbc:teradata://{self.host}/{','.join(filter(None, [db_part] + url_params))}"
            logger.info("Teradata connect URL: %s", jdbc_url)

            conn = teradatasql.connect(**connect_params)
            logger.debug("Created new Teradata connection to %s (logmech=%s)", self.host, self.logmech)
            return conn
        except Exception as e:
            logger.error("Failed to connect to Teradata: %s", e, exc_info=True)
            raise TeradataConnectionError(f"Cannot connect to Teradata: {e}") from e

    @property
    def engine(self):
        """Legacy property for backward compatibility. Returns a connection."""
        logger.warning("engine property is deprecated, creating new connection")
        return self._get_connection()

    def test_connection(self) -> dict[str, Any]:
        """
        Test database connection and return server info.

        Returns:
            Dictionary with connection status and server information
        """
        conn = None
        try:
            conn = self._get_connection()

            # Get version
            df = pd.read_sql(
                """
                SELECT 
                    InfoData as version
                FROM DBC.DBCInfoV
                WHERE InfoKey = 'VERSION'
            """,
                conn,
            )

            version = df["version"].iloc[0].strip() if len(df) > 0 else "Unknown"

            # Get current timestamp
            ts_df = pd.read_sql("SELECT CURRENT_TIMESTAMP as ts", conn)
            current_time = str(ts_df["ts"].iloc[0])

            self._connected = True
            logger.info("Connection test successful for %s", self.host)

            return {
                "connected": True,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "version": version,
                "current_time": current_time,
            }
        except Exception as e:
            logger.error("Connection test failed: %s", e, exc_info=True)
            return {
                "connected": False,
                "error": safe_error_message(e),
            }
        finally:
            if conn:
                conn.close()

    def _generate_column_description(self, column_name: str, column_type: str) -> str:
        """
        Generate a basic description from column name and type.

        Args:
            column_name: Column name
            column_type: Teradata column type

        Returns:
            Human-readable description
        """
        # Convert snake_case or camelCase to readable format
        import re

        # Handle camelCase: insert spaces before uppercase letters that follow lowercase letters
        camel_case_converted = re.sub(r"([a-z])([A-Z])", r"\1 \2", column_name)

        # Handle snake_case: replace underscores with spaces
        words = camel_case_converted.replace("_", " ").strip()

        # Capitalize first letter of each word
        readable_name = " ".join(word.capitalize() for word in words.split())

        # Map Teradata types to friendly names
        type_map = {
            "I": "INTEGER",
            "I1": "BYTEINT",
            "I2": "SMALLINT",
            "I8": "BIGINT",
            "D": "DECIMAL",
            "F": "FLOAT",
            "CF": "CHAR",
            "CV": "VARCHAR",
            "CO": "CLOB",
            "DA": "DATE",
            "AT": "TIME",
            "TS": "TIMESTAMP",
            "TZ": "TIMESTAMP WITH TIME ZONE",
            "SZ": "TIMESTAMP WITH TIME ZONE",
            "YR": "INTERVAL YEAR",
            "YM": "INTERVAL YEAR TO MONTH",
            "MO": "INTERVAL MONTH",
            "DY": "INTERVAL DAY",
            "DH": "INTERVAL DAY TO HOUR",
            "DM": "INTERVAL DAY TO MINUTE",
            "DS": "INTERVAL DAY TO SECOND",
            "HR": "INTERVAL HOUR",
            "HM": "INTERVAL HOUR TO MINUTE",
            "HS": "INTERVAL HOUR TO SECOND",
            "MI": "INTERVAL MINUTE",
            "MS": "INTERVAL MINUTE TO SECOND",
            "SC": "INTERVAL SECOND",
            "BO": "BLOB",
            "BV": "VARBYTE",
            "BF": "BYTE",
        }

        friendly_type = type_map.get(column_type, column_type)

        return f"{readable_name} ({friendly_type})"

    def get_table_metadata(
        self,
        database_name: str,
        table_name: str,
        include_stats: bool = False,
    ) -> dict[str, Any]:
        """
        Extract comprehensive metadata for a table.

        Args:
            database_name: Database name
            table_name: Table name
            include_stats: Include table statistics (size, rows)

        Returns:
            Dictionary with complete table metadata

        Raises:
            TeradataQueryError: If metadata extraction fails
        """
        conn = None
        try:
            logger.info("Extracting metadata for %s.%s", database_name, table_name)

            conn = self._get_connection()

            # Get column information using pandas
            columns_df = pd.read_sql(
                """
                SELECT 
                    ColumnName,
                    ColumnType,
                    ColumnLength,
                    DecimalTotalDigits,
                    DecimalFractionalDigits,
                    Nullable,
                    DefaultValue,
                    CommentString,
                    ColumnId,
                    ColumnFormat,
                    UpperCaseFlag,
                    IdColType
                FROM DBC.ColumnsV
                WHERE DatabaseName = ?
                  AND TableName = ?
                ORDER BY ColumnId
            """,
                conn,
                params=(database_name, table_name),
            )

            if columns_df.empty:
                raise TeradataQueryError(
                    f"Table {database_name}.{table_name} not found or no access"
                )

            # Convert dataframe to list of column dicts
            columns = []
            for _, row in columns_df.iterrows():
                col_info = {
                    "name": row["ColumnName"].strip() if pd.notna(row["ColumnName"]) else "",
                    "type": row["ColumnType"].strip() if pd.notna(row["ColumnType"]) else "UNKNOWN",
                    "length": int(row["ColumnLength"]) if pd.notna(row["ColumnLength"]) else None,
                    "nullable": row["Nullable"] == "Y",
                    "position": int(row["ColumnId"]),
                }

                # Add numeric precision info
                if pd.notna(row["DecimalTotalDigits"]):
                    col_info["precision"] = int(row["DecimalTotalDigits"])
                if pd.notna(row["DecimalFractionalDigits"]):
                    col_info["scale"] = int(row["DecimalFractionalDigits"])

                # Add defaults and comments
                if pd.notna(row["DefaultValue"]):
                    col_info["default"] = row["DefaultValue"].strip()

                # Use CommentString if available, otherwise generate basic description
                if pd.notna(row["CommentString"]) and row["CommentString"].strip():
                    col_info["description"] = row["CommentString"].strip()
                else:
                    col_info["description"] = self._generate_column_description(
                        col_info["name"], col_info["type"]
                    )

                if pd.notna(row["ColumnFormat"]):
                    col_info["format"] = row["ColumnFormat"].strip()

                # Identity column info
                if pd.notna(row["IdColType"]) and row["IdColType"].strip():
                    col_info["identity_type"] = row["IdColType"].strip()

                # Case sensitivity
                col_info["case_specific"] = row["UpperCaseFlag"] == "N"

                columns.append(col_info)

            # Get primary key information
            pk_df = pd.read_sql(
                """
                SELECT ColumnName
                FROM DBC.IndicesV
                WHERE DatabaseName = ?
                  AND TableName = ?
                  AND IndexType = 'P'
                ORDER BY ColumnPosition
            """,
                conn,
                params=(database_name, table_name),
            )
            primary_keys = [row["ColumnName"].strip() for _, row in pk_df.iterrows()]

            # Get index information
            idx_df = pd.read_sql(
                """
                SELECT 
                    IndexName,
                    IndexType,
                    UniqueFlag,
                    ColumnName
                FROM DBC.IndicesV
                WHERE DatabaseName = ?
                  AND TableName = ?
                  AND IndexType IN ('S', 'U', 'J', 'H', 'Q', 'V')
                ORDER BY IndexName, ColumnPosition
            """,
                conn,
                params=(database_name, table_name),
            )

            # Group indexes
            indexes = {}
            for _, row in idx_df.iterrows():
                idx_name = (
                    row["IndexName"].strip()
                    if pd.notna(row["IndexName"])
                    else f"idx_{row['IndexType']}"
                )
                if idx_name not in indexes:
                    indexes[idx_name] = {
                        "name": idx_name,
                        "type": row["IndexType"].strip(),
                        "unique": row["UniqueFlag"] == "Y",
                        "columns": [],
                    }
                indexes[idx_name]["columns"].append(row["ColumnName"].strip())

            # Get table-level information
            table_df = pd.read_sql(
                """
                SELECT 
                    TableKind,
                    CreatorName,
                    CreateTimeStamp,
                    LastAlterTimeStamp,
                    CommentString,
                    ProtectionType,
                    JournalFlag
                FROM DBC.TablesV
                WHERE DatabaseName = ?
                  AND TableName = ?
            """,
                conn,
                params=(database_name, table_name),
            )

            metadata = {
                "database": database_name,
                "table": table_name,
                "columns": columns,
                "primary_keys": primary_keys,
                "indexes": list(indexes.values()),
                "column_count": len(columns),
            }

            if not table_df.empty:
                row = table_df.iloc[0]
                metadata["table_type"] = row["TableKind"].strip()
                metadata["creator"] = (
                    row["CreatorName"].strip() if pd.notna(row["CreatorName"]) else None
                )
                metadata["created_at"] = (
                    str(row["CreateTimeStamp"]) if pd.notna(row["CreateTimeStamp"]) else None
                )
                metadata["last_altered"] = (
                    str(row["LastAlterTimeStamp"]) if pd.notna(row["LastAlterTimeStamp"]) else None
                )

                if pd.notna(row["CommentString"]):
                    metadata["description"] = row["CommentString"].strip()
                if pd.notna(row["ProtectionType"]):
                    metadata["protection"] = row["ProtectionType"].strip()

                metadata["journaling_enabled"] = row["JournalFlag"] == "Y"

            # Include statistics if requested
            if include_stats:
                stats = self.estimate_table_size(database_name, table_name)
                metadata["size_mb"] = stats["size_mb"]
                if "row_count" in stats:
                    metadata["row_count"] = stats["row_count"]

            logger.info("Successfully extracted metadata for %s.%s", database_name, table_name)
            return metadata

        except Exception as e:
            logger.error("Failed to extract metadata: %s", e, exc_info=True)
            raise TeradataQueryError(f"Metadata extraction failed: {e}") from e
        finally:
            if conn:
                conn.close()

    def estimate_table_size(
        self,
        database_name: str,
        table_name: str,
    ) -> dict[str, Any]:
        """
        Estimate table size and row count.

        Args:
            database_name: Database name
            table_name: Table name

        Returns:
            Dictionary with size information
        """
        conn = None
        try:
            conn = self._get_connection()

            # Get table size from DBC.TableSizeV
            size_df = pd.read_sql(
                """
                SELECT 
                    CAST(SUM(CurrentPerm) / (1024.0 * 1024.0) AS DECIMAL(18,2)) as SizeMB,
                    CAST(SUM(CurrentPerm) / (1024.0 * 1024.0 * 1024.0) AS DECIMAL(18,2)) as SizeGB
                FROM DBC.TableSizeV
                WHERE DatabaseName = ?
                  AND TableName = ?
            """,
                conn,
                params=(database_name, table_name),
            )

            size_mb = (
                float(size_df["SizeMB"].iloc[0])
                if not size_df.empty and pd.notna(size_df["SizeMB"].iloc[0])
                else 0.0
            )
            size_gb = (
                float(size_df["SizeGB"].iloc[0])
                if not size_df.empty and pd.notna(size_df["SizeGB"].iloc[0])
                else 0.0
            )

            # Try to get row count (but don't fail if not available)
            row_count = None
            try:
                count_df = pd.read_sql(
                    """
                    SELECT SUM(t.RowCount) as row_count
                    FROM DBC.TableSizeV t
                    WHERE t.DatabaseName = ?
                      AND t.TableName = ?
                """,
                    conn,
                    params=(database_name, table_name),
                )
                if not count_df.empty and pd.notna(count_df["row_count"].iloc[0]):
                    row_count = int(count_df["row_count"].iloc[0])
            except Exception as e:
                logger.debug("Failed to get row count from stats: %s", e)

            return {
                "database": database_name,
                "table": table_name,
                "size_mb": size_mb,
                "size_gb": size_gb,
                "row_count": row_count,
            }

        except Exception as e:
            logger.warning("Failed to estimate table size: %s", e)
            return {
                "database": database_name,
                "table": table_name,
                "size_mb": 0.0,
                "size_gb": 0.0,
                "error": safe_error_message(e),
            }
        finally:
            if conn:
                conn.close()

    def get_column_statistics(
        self,
        database_name: str,
        table_name: str,
        column_name: str | None = None,
        sample_size: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get detailed column-level statistics.

        Args:
            database_name: Database name
            table_name: Table name
            column_name: Optional specific column (None for all columns)
            sample_size: Optional sample size for faster analysis

        Returns:
            List of dictionaries with statistics for each column
        """
        conn = None
        try:
            logger.info("Calculating statistics for %s.%s", database_name, table_name)

            conn = self._get_connection()

            # Get column list
            if column_name:
                columns_to_analyze = [(column_name, None)]
            else:
                col_df = pd.read_sql(
                    """
                    SELECT ColumnName, ColumnType
                    FROM DBC.ColumnsV
                    WHERE DatabaseName = ?
                      AND TableName = ?
                    ORDER BY ColumnId
                """,
                    conn,
                    params=(database_name, table_name),
                )
                columns_to_analyze = [
                    (row["ColumnName"].strip(), row["ColumnType"].strip())
                    for _, row in col_df.iterrows()
                ]

            # Build sample clause if needed
            sample_clause = f"SAMPLE {_safe_int(sample_size, 'sample_size')}" if sample_size is not None else ""
            db = _quote_identifier(database_name)
            tbl = _quote_identifier(table_name)

            stats = []
            for col_info in columns_to_analyze:
                col = col_info[0] if isinstance(col_info, tuple) else col_info
                quoted_col = _quote_identifier(col)

                try:
                    # Basic statistics for all columns
                    stat_df = pd.read_sql(
                        f"""
                        SELECT
                            '{col}' as column_name,
                            COUNT(*) as total_count,
                            COUNT({quoted_col}) as non_null_count,
                            COUNT(*) - COUNT({quoted_col}) as null_count,
                            CAST((COUNT(*) - COUNT({quoted_col})) * 100.0 / NULLIFZERO(COUNT(*)) AS DECIMAL(5,2)) as null_percentage,
                            COUNT(DISTINCT {quoted_col}) as distinct_count,
                            CAST(COUNT(DISTINCT {quoted_col}) * 100.0 / NULLIFZERO(COUNT(*)) AS DECIMAL(5,2)) as cardinality_percentage
                        FROM {db}.{tbl} {sample_clause}
                    """,
                        conn,
                    )

                    if not stat_df.empty:
                        row = stat_df.iloc[0]
                        col_stats = {
                            "column_name": row["column_name"],
                            "total_rows": int(row["total_count"]),
                            "non_null_count": int(row["non_null_count"]),
                            "null_count": int(row["null_count"]),
                            "null_percentage": float(row["null_percentage"] or 0),
                            "distinct_count": int(row["distinct_count"]),
                            "cardinality_percentage": float(row["cardinality_percentage"] or 0),
                        }
                        stats.append(col_stats)

                except Exception as e:
                    logger.warning("Failed to get stats for column %s: %s", col, e)
                    stats.append(
                        {
                            "column_name": col,
                            "error": safe_error_message(e),
                        }
                    )

            logger.info("Successfully calculated statistics for %d columns", len(stats))
            return stats

        except Exception as e:
            logger.error("Failed to calculate column statistics: %s", e, exc_info=True)
            raise TeradataQueryError(f"Column statistics failed: {e}") from e
        finally:
            if conn:
                conn.close()

    def get_table_lineage(
        self,
        database_name: str,
        table_name: str,
        direction: str = "both",
        limit: int = 50,
    ) -> dict[str, Any]:
        """
        Identify table dependencies using query log analysis.

        Args:
            database_name: Database name
            table_name: Table name
            direction: 'upstream', 'downstream', or 'both'
            limit: Maximum number of dependencies to return

        Returns:
            Dictionary with upstream and downstream dependencies
        """
        lineage = {
            "database": database_name,
            "table": table_name,
            "upstream": [],
            "downstream": [],
            "query_log_available": False,
        }

        conn = None
        try:
            conn = self._get_connection()

            # Check if we have access to query log
            try:
                pd.read_sql(
                    """
                    SELECT COUNT(*) as cnt
                    FROM DBC.DBQLObjTbl
                    WHERE 1=0
                """,
                    conn,
                )
                lineage["query_log_available"] = True
            except Exception:
                logger.warning("Query log not available or insufficient privileges")
                return lineage

            safe_limit = _safe_int(limit, "limit")

            if direction in ("upstream", "both"):
                # Find tables this table depends on
                try:
                    upstream_df = pd.read_sql(
                        f"""
                        SELECT TOP {safe_limit}
                            ObjectDatabaseName as database_name,
                            ObjectTableName as table_name,
                            COUNT(*) as query_count
                        FROM DBC.DBQLObjTbl
                        WHERE QueryID IN (
                            SELECT DISTINCT QueryID
                            FROM DBC.DBQLObjTbl
                            WHERE ObjectDatabaseName = ?
                              AND ObjectTableName = ?
                              AND ObjectType = 'Tab'
                        )
                        AND ObjectType = 'Tab'
                        AND NOT (ObjectDatabaseName = ? AND ObjectTableName = ?)
                        GROUP BY 1, 2
                        ORDER BY 3 DESC
                    """,
                        conn,
                        params=(database_name, table_name, database_name, table_name),
                    )

                    lineage["upstream"] = [
                        {
                            "database": row["database_name"].strip(),
                            "table": row["table_name"].strip(),
                            "query_count": int(row["query_count"]),
                        }
                        for _, row in upstream_df.iterrows()
                    ]
                except Exception as e:
                    logger.warning("Failed to get upstream lineage: %s", e)

            if direction in ("downstream", "both"):
                # Find tables that depend on this table
                try:
                    downstream_df = pd.read_sql(
                        f"""
                        SELECT TOP {safe_limit}
                            obj2.ObjectDatabaseName as database_name,
                            obj2.ObjectTableName as table_name,
                            COUNT(*) as query_count
                        FROM DBC.DBQLObjTbl obj1
                        INNER JOIN DBC.DBQLObjTbl obj2
                            ON obj1.QueryID = obj2.QueryID
                        WHERE obj1.ObjectDatabaseName = ?
                          AND obj1.ObjectTableName = ?
                          AND obj1.ObjectType = 'Tab'
                          AND obj2.ObjectType = 'Tab'
                          AND NOT (obj2.ObjectDatabaseName = ? AND obj2.ObjectTableName = ?)
                        GROUP BY 1, 2
                        ORDER BY 3 DESC
                    """,
                        conn,
                        params=(database_name, table_name, database_name, table_name),
                    )

                    lineage["downstream"] = [
                        {
                            "database": row["database_name"].strip(),
                            "table": row["table_name"].strip(),
                            "query_count": int(row["query_count"]),
                        }
                        for _, row in downstream_df.iterrows()
                    ]
                except Exception as e:
                    logger.warning("Failed to get downstream lineage: %s", e)

            return lineage

        except Exception as e:
            logger.error("Failed to get table lineage: %s", e, exc_info=True)
            lineage["error"] = safe_error_message(e)
            return lineage
        finally:
            if conn:
                conn.close()

    def search_metadata(
        self,
        search_term: str,
        search_type: str = "table",
        database_name: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Search for tables, columns, or comments matching a pattern.

        Args:
            search_term: Search pattern (wildcards supported with %)
            search_type: 'table', 'column', or 'comment'
            database_name: Optional database filter
            limit: Maximum results to return

        Returns:
            List of matching objects
        """
        conn = None
        try:
            results = []

            conn = self._get_connection()
            safe_limit = _safe_int(limit, "limit")

            if search_type == "table":
                if database_name:
                    df = pd.read_sql(
                        f"""
                        SELECT TOP {safe_limit}
                            DatabaseName as database_name,
                            TableName as table_name,
                            TableKind as table_kind,
                            CommentString as description,
                            CreateTimeStamp as created_at
                        FROM DBC.TablesV
                        WHERE TableName LIKE ?
                          AND DatabaseName = ?
                          AND TableKind IN ('T', 'O')
                        ORDER BY DatabaseName, TableName
                    """,
                        conn,
                        params=(f"%{search_term}%", database_name),
                    )
                else:
                    df = pd.read_sql(
                        f"""
                        SELECT TOP {safe_limit}
                            DatabaseName as database_name,
                            TableName as table_name,
                            TableKind as table_kind,
                            CommentString as description,
                            CreateTimeStamp as created_at
                        FROM DBC.TablesV
                        WHERE TableName LIKE ?
                          AND TableKind IN ('T', 'O')
                        ORDER BY DatabaseName, TableName
                    """,
                        conn,
                        params=(f"%{search_term}%",),
                    )

                results = [
                    {
                        "type": "table",
                        "database": row["database_name"].strip()
                        if pd.notna(row["database_name"])
                        else None,
                        "table": row["table_name"].strip() if pd.notna(row["table_name"]) else None,
                        "table_type": row["table_kind"].strip()
                        if pd.notna(row["table_kind"])
                        else None,
                        "description": row["description"].strip()
                        if pd.notna(row["description"])
                        else None,
                        "created_at": str(row["created_at"])
                        if pd.notna(row["created_at"])
                        else None,
                    }
                    for _, row in df.iterrows()
                ]

            elif search_type == "column":
                if database_name:
                    df = pd.read_sql(
                        f"""
                        SELECT TOP {safe_limit}
                            DatabaseName as database_name,
                            TableName as table_name,
                            ColumnName as column_name,
                            ColumnType as column_type,
                            CommentString as description
                        FROM DBC.ColumnsV
                        WHERE ColumnName LIKE ?
                          AND DatabaseName = ?
                        ORDER BY DatabaseName, TableName, ColumnName
                    """,
                        conn,
                        params=(f"%{search_term}%", database_name),
                    )
                else:
                    df = pd.read_sql(
                        f"""
                        SELECT TOP {safe_limit}
                            DatabaseName as database_name,
                            TableName as table_name,
                            ColumnName as column_name,
                            ColumnType as column_type,
                            CommentString as description
                        FROM DBC.ColumnsV
                        WHERE ColumnName LIKE ?
                        ORDER BY DatabaseName, TableName, ColumnName
                    """,
                        conn,
                        params=(f"%{search_term}%",),
                    )

                results = [
                    {
                        "type": "column",
                        "database": row["database_name"].strip()
                        if pd.notna(row["database_name"])
                        else None,
                        "table": row["table_name"].strip() if pd.notna(row["table_name"]) else None,
                        "column": row["column_name"].strip()
                        if pd.notna(row["column_name"])
                        else None,
                        "column_type": row["column_type"].strip()
                        if pd.notna(row["column_type"])
                        else None,
                        "description": row["description"].strip()
                        if pd.notna(row["description"])
                        else None,
                    }
                    for _, row in df.iterrows()
                ]

            elif search_type == "comment":
                if database_name:
                    df = pd.read_sql(
                        f"""
                        SELECT TOP {safe_limit}
                            DatabaseName as database_name,
                            TableName as table_name,
                            CommentString as description
                        FROM DBC.TablesV
                        WHERE CommentString LIKE ?
                          AND DatabaseName = ?
                        ORDER BY DatabaseName, TableName
                    """,
                        conn,
                        params=(f"%{search_term}%", database_name),
                    )
                else:
                    df = pd.read_sql(
                        f"""
                        SELECT TOP {safe_limit}
                            DatabaseName as database_name,
                            TableName as table_name,
                            CommentString as description
                        FROM DBC.TablesV
                        WHERE CommentString LIKE ?
                        ORDER BY DatabaseName, TableName
                    """,
                        conn,
                        params=(f"%{search_term}%",),
                    )

                results = [
                    {
                        "type": "table",
                        "database": row["database_name"].strip(),
                        "table": row["table_name"].strip(),
                        "description": row["description"].strip()
                        if pd.notna(row["description"])
                        else None,
                    }
                    for _, row in df.iterrows()
                ]

            logger.info("Found %d results for '%s'", len(results), search_term)
            return results

        except Exception as e:
            logger.error("Metadata search failed: %s", e, exc_info=True)
            raise TeradataQueryError(f"Search failed: {e}") from e
        finally:
            if conn:
                conn.close()

    def profile_table(
        self,
        database_name: str,
        table_name: str,
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        """
        Generate comprehensive data profiling summary.

        Args:
            database_name: Database name
            table_name: Table name
            sample_size: Optional sample size for faster profiling

        Returns:
            Dictionary with profiling results for all columns
        """
        conn = None
        try:
            logger.info("Profiling table %s.%s", database_name, table_name)

            conn = self._get_connection()

            # Get column list with types
            col_df = pd.read_sql(
                """
                SELECT ColumnName, ColumnType
                FROM DBC.ColumnsV
                WHERE DatabaseName = ?
                  AND TableName = ?
                ORDER BY ColumnId
            """,
                conn,
                params=(database_name, table_name),
            )

            sample_clause = f"SAMPLE {_safe_int(sample_size, 'sample_size')}" if sample_size is not None else ""
            db = _quote_identifier(database_name)
            tbl = _quote_identifier(table_name)

            profiles = []
            for _, col_row in col_df.iterrows():
                col_name = col_row["ColumnName"].strip()
                col_type = col_row["ColumnType"].strip()
                quoted_col = _quote_identifier(col_name)

                profile = {
                    "column_name": col_name,
                    "data_type": col_type,
                }

                try:
                    # Numeric columns
                    if col_type in ("I", "I1", "I2", "I8", "F", "D", "N"):
                        stat_df = pd.read_sql(
                            f"""
                            SELECT
                                MIN({quoted_col}) as min_value,
                                MAX({quoted_col}) as max_value,
                                AVG({quoted_col}) as avg_value,
                                STDDEV_POP({quoted_col}) as std_dev,
                                COUNT(DISTINCT {quoted_col}) as distinct_count,
                                COUNT({quoted_col}) as non_null_count
                            FROM {db}.{tbl} {sample_clause}
                        """,
                            conn,
                        )
                        if not stat_df.empty:
                            row = stat_df.iloc[0]
                            profile.update(
                                {
                                    "min": float(row["min_value"])
                                    if pd.notna(row["min_value"])
                                    else None,
                                    "max": float(row["max_value"])
                                    if pd.notna(row["max_value"])
                                    else None,
                                    "avg": float(row["avg_value"])
                                    if pd.notna(row["avg_value"])
                                    else None,
                                    "std_dev": float(row["std_dev"])
                                    if pd.notna(row["std_dev"])
                                    else None,
                                    "distinct_count": int(row["distinct_count"]),
                                    "non_null_count": int(row["non_null_count"]),
                                }
                            )

                    # String columns
                    elif col_type in ("CV", "CF", "JN"):
                        stat_df = pd.read_sql(
                            f"""
                            SELECT
                                MIN(CHAR_LENGTH({quoted_col})) as min_length,
                                MAX(CHAR_LENGTH({quoted_col})) as max_length,
                                AVG(CHAR_LENGTH({quoted_col})) as avg_length,
                                COUNT(DISTINCT {quoted_col}) as distinct_count,
                                COUNT({quoted_col}) as non_null_count
                            FROM {db}.{tbl} {sample_clause}
                        """,
                            conn,
                        )
                        if not stat_df.empty:
                            row = stat_df.iloc[0]
                            profile.update(
                                {
                                    "min_length": int(row["min_length"])
                                    if pd.notna(row["min_length"])
                                    else None,
                                    "max_length": int(row["max_length"])
                                    if pd.notna(row["max_length"])
                                    else None,
                                    "avg_length": float(row["avg_length"])
                                    if pd.notna(row["avg_length"])
                                    else None,
                                    "distinct_count": int(row["distinct_count"]),
                                    "non_null_count": int(row["non_null_count"]),
                                }
                            )

                    # Date/Time columns
                    elif col_type in ("DA", "TS", "TZ", "AT", "SZ"):
                        stat_df = pd.read_sql(
                            f"""
                            SELECT
                                MIN({quoted_col}) as min_value,
                                MAX({quoted_col}) as max_value,
                                COUNT(DISTINCT {quoted_col}) as distinct_count,
                                COUNT({quoted_col}) as non_null_count
                            FROM {db}.{tbl} {sample_clause}
                        """,
                            conn,
                        )
                        if not stat_df.empty:
                            row = stat_df.iloc[0]
                            profile.update(
                                {
                                    "min": str(row["min_value"])
                                    if pd.notna(row["min_value"])
                                    else None,
                                    "max": str(row["max_value"])
                                    if pd.notna(row["max_value"])
                                    else None,
                                    "distinct_count": int(row["distinct_count"]),
                                    "non_null_count": int(row["non_null_count"]),
                                }
                            )

                except Exception as e:
                    logger.warning("Failed to profile column %s: %s", col_name, e)
                    profile["error"] = safe_error_message(e)

                profiles.append(profile)

            return {
                "database": database_name,
                "table": table_name,
                "column_profiles": profiles,
                "profiled_at": datetime.now().isoformat(),
                "sample_size": sample_size,
            }

        except Exception as e:
            logger.error("Table profiling failed: %s", e, exc_info=True)
            raise TeradataQueryError(f"Profiling failed: {e}") from e
        finally:
            if conn:
                conn.close()

    def detect_schema_changes(
        self,
        database_name: str,
        table_name: str,
        baseline_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Compare current schema with baseline to detect changes.

        Args:
            database_name: Database name
            table_name: Table name
            baseline_metadata: Previously captured metadata

        Returns:
            Dictionary describing detected changes
        """
        try:
            current_metadata = self.get_table_metadata(database_name, table_name)

            changes = {
                "schema_changed": False,
                "columns_added": [],
                "columns_removed": [],
                "columns_modified": [],
                "primary_key_changed": False,
                "indexes_changed": False,
            }

            baseline_cols = {col["name"]: col for col in baseline_metadata.get("columns", [])}
            current_cols = {col["name"]: col for col in current_metadata.get("columns", [])}

            # Find added columns
            for col_name in current_cols:
                if col_name not in baseline_cols:
                    changes["columns_added"].append(col_name)
                    changes["schema_changed"] = True

            # Find removed columns
            for col_name in baseline_cols:
                if col_name not in current_cols:
                    changes["columns_removed"].append(col_name)
                    changes["schema_changed"] = True

            # Find modified columns
            for col_name in set(baseline_cols.keys()) & set(current_cols.keys()):
                baseline_col = baseline_cols[col_name]
                current_col = current_cols[col_name]

                modifications = {}
                if baseline_col["type"] != current_col["type"]:
                    modifications["type_changed"] = {
                        "from": baseline_col["type"],
                        "to": current_col["type"],
                    }
                if baseline_col["nullable"] != current_col["nullable"]:
                    modifications["nullable_changed"] = {
                        "from": baseline_col["nullable"],
                        "to": current_col["nullable"],
                    }
                if baseline_col.get("length") != current_col.get("length"):
                    modifications["length_changed"] = {
                        "from": baseline_col.get("length"),
                        "to": current_col.get("length"),
                    }

                if modifications:
                    changes["columns_modified"].append(
                        {
                            "column": col_name,
                            "changes": modifications,
                        }
                    )
                    changes["schema_changed"] = True

            # Check primary key changes
            baseline_pks = set(baseline_metadata.get("primary_keys", []))
            current_pks = set(current_metadata.get("primary_keys", []))
            if baseline_pks != current_pks:
                changes["primary_key_changed"] = True
                changes["primary_key_details"] = {
                    "removed": list(baseline_pks - current_pks),
                    "added": list(current_pks - baseline_pks),
                }
                changes["schema_changed"] = True

            # Check index changes
            baseline_idx_names = {idx["name"] for idx in baseline_metadata.get("indexes", [])}
            current_idx_names = {idx["name"] for idx in current_metadata.get("indexes", [])}
            if baseline_idx_names != current_idx_names:
                changes["indexes_changed"] = True
                changes["index_details"] = {
                    "removed": list(baseline_idx_names - current_idx_names),
                    "added": list(current_idx_names - baseline_idx_names),
                }
                changes["schema_changed"] = True

            changes["detected_at"] = datetime.now().isoformat()
            return changes

        except Exception as e:
            logger.error("Schema change detection failed: %s", e, exc_info=True)
            raise TeradataQueryError(f"Change detection failed: {e}") from e

    def list_tables(
        self,
        database_name: str,
        table_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        List all tables in a database.

        Args:
            database_name: Database name
            table_type: Optional filter ('T' for table, 'V' for view, etc.)

        Returns:
            List of table information dictionaries
        """
        conn = None
        try:
            conn = self._get_connection()

            if table_type:
                query = """
                    SELECT 
                        TableName,
                        TableKind,
                        CommentString,
                        CreateTimeStamp
                    FROM DBC.TablesV
                    WHERE DatabaseName = ?
                      AND TableKind = ?
                    ORDER BY TableName
                """
                df = pd.read_sql(query, conn, params=(database_name, table_type))
            else:
                query = """
                    SELECT 
                        TableName,
                        TableKind,
                        CommentString,
                        CreateTimeStamp
                    FROM DBC.TablesV
                    WHERE DatabaseName = ?
                    ORDER BY TableName
                """
                df = pd.read_sql(query, conn, params=(database_name,))

            tables = [
                {
                    "table": row["TableName"].strip(),
                    "type": row["TableKind"].strip(),
                    "description": row["CommentString"].strip()
                    if pd.notna(row["CommentString"])
                    else None,
                    "created_at": str(row["CreateTimeStamp"])
                    if pd.notna(row["CreateTimeStamp"])
                    else None,
                }
                for _, row in df.iterrows()
            ]

            logger.info("Found %d tables in %s", len(tables), database_name)
            return tables

        except Exception as e:
            logger.error("Failed to list tables: %s", e, exc_info=True)
            raise TeradataQueryError(f"List tables failed: {e}") from e
        finally:
            if conn:
                conn.close()

    def list_databases(self) -> list[str]:
        """
        List all accessible databases.

        Returns:
            List of database names
        """
        conn = None
        try:
            conn = self._get_connection()

            df = pd.read_sql(
                """
                SELECT DISTINCT DataBaseName
                FROM DBC.TablesV
                ORDER BY DataBaseName
            """,
                conn,
            )

            databases = [row["DataBaseName"].strip() for _, row in df.iterrows()]

            logger.info("Found %d databases", len(databases))
            return databases

        except Exception as e:
            logger.error("Failed to list databases: %s", e, exc_info=True)
            raise TeradataQueryError(f"List databases failed: {e}") from e
        finally:
            if conn:
                conn.close()

    def check_database_exists(self, database_name: str) -> bool:
        """
        Check whether a database/user exists and is accessible.

        Args:
            database_name: Name of the database to check.

        Returns:
            True if the database exists and is accessible, False otherwise.
        """
        normalized_name = database_name.strip()
        if not normalized_name:
            logger.warning("check_database_exists called with empty database name after stripping")
            return False

        conn = None
        try:
            conn = self._get_connection()
            df = pd.read_sql(
                "SELECT COUNT(*) AS cnt FROM DBC.DatabasesV WHERE DatabaseName = ?",
                conn,
                params=(normalized_name,),
            )
            return int(df["cnt"].iloc[0]) > 0
        except Exception as e:
            logger.error(
                "check_database_exists failed for %r: %s", normalized_name, e, exc_info=True
            )
            raise TeradataQueryError(
                f"Check database exists failed for {normalized_name!r}: {e}"
            ) from e
        finally:
            if conn:
                conn.close()

    def execute_query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return results.

        Args:
            sql: SQL query to execute
            params: Optional query parameters (not supported with pandas read_sql)

        Returns:
            List of dictionaries representing rows (JSON-serializable)
        """
        conn = None
        try:
            conn = self._get_connection()

            if params:
                # Convert dict params to tuple for pandas
                df = pd.read_sql(sql, conn, params=tuple(params.values()))
            else:
                df = pd.read_sql(sql, conn)

            # Convert to dict and ensure JSON serialization
            rows = df.to_dict("records")

            # Serialize all values to ensure JSON compatibility
            serialized_rows = [
                {key: _serialize_value(val) for key, val in row.items()} for row in rows
            ]

            logger.info("Query executed successfully, returned %d rows", len(serialized_rows))
            return serialized_rows

        except Exception as e:
            logger.error("Query execution failed: %s", e, exc_info=True)
            raise TeradataQueryError(f"Query failed: {e}") from e
        finally:
            if conn:
                conn.close()

    def preview_data(
        self,
        database_name: str,
        table_name: str,
        limit: int = 100,
        sample: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Preview table data.

        Args:
            database_name: Database name
            table_name: Table name
            limit: Number of rows to return
            sample: Use SAMPLE for better distribution

        Returns:
            List of row dictionaries (JSON-serializable)
        """
        conn = None
        try:
            conn = self._get_connection()
            db = _quote_identifier(database_name)
            tbl = _quote_identifier(table_name)
            safe_limit = _safe_int(limit, "limit")

            if sample:
                df = pd.read_sql(
                    f"""
                    SELECT *
                    FROM {db}.{tbl}
                    SAMPLE {safe_limit}
                """,
                    conn,
                )
            else:
                df = pd.read_sql(
                    f"""
                    SELECT TOP {safe_limit} *
                    FROM {db}.{tbl}
                """,
                    conn,
                )

            # Convert to dict and ensure JSON serialization
            rows = df.to_dict("records")

            # Serialize all values to ensure JSON compatibility
            serialized_rows = [
                {key: _serialize_value(val) for key, val in row.items()} for row in rows
            ]

            logger.info(
                "Retrieved %d preview rows from %s.%s",
                len(serialized_rows),
                database_name,
                table_name,
            )
            return serialized_rows

        except Exception as e:
            logger.error("Data preview failed: %s", e, exc_info=True)
            raise TeradataQueryError(f"Preview failed: {e}") from e
        finally:
            if conn:
                conn.close()

    def get_amp_count(self) -> int:
        """
        Get the number of AMPs in the Teradata system.

        Returns:
            Number of AMPs

        Raises:
            TeradataQueryError: If AMP count query fails
        """
        conn = None
        try:
            conn = self._get_connection()
            df = pd.read_sql("SELECT HASHAMP() + 1 as amp_count", conn)
            amp_count = int(df["amp_count"].iloc[0])
            logger.info("Teradata system has %d AMPs", amp_count)
            return amp_count
        except Exception as e:
            logger.error("Failed to get AMP count: %s", e, exc_info=True)
            # Default to 2 if query fails
            logger.warning("Defaulting to 2 AMPs")
            return 2
        finally:
            if conn:
                conn.close()

    @staticmethod
    def _extract_teradata_error_code(error_message: str) -> int | None:
        """Extract a Teradata error code from a teradatasql exception message.

        Matches patterns like ``[Error 3807]`` in exception text.
        """
        m = re.search(r"\[Error\s+(\d+)\]", error_message)
        return int(m.group(1)) if m else None

    def execute_statements(
        self,
        sql_statements: list[str],
        error_list: list[int] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """Execute one or more SQL statements via a direct teradatasql connection.

        Supports DDL, DML, SELECT, CALL, and any other statement type.
        Results from SELECT/CALL statements are returned as structured data.

        Args:
            sql_statements: List of SQL statements to execute sequentially.
            error_list: Teradata error codes to tolerate (skip and continue).
            timeout: Per-statement timeout in seconds (overrides client default).

        Returns:
            Dictionary with keys: success, returncode, stdout, stderr, results,
            statement_count, tolerated_errors.

        Raises:
            ValueError: If sql_statements is empty.
        """
        if not sql_statements:
            raise ValueError("sql_statements must be a non-empty list")

        _check_teradatasql()

        results: list[dict[str, Any]] = []
        tolerated_errors: list[dict[str, Any]] = []
        summary_lines: list[str] = []
        executed_count = 0
        conn = None

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            if timeout is not None:
                timeout = int(timeout)
                if timeout <= 0:
                    raise ValueError(f"timeout must be a positive integer, got {timeout}")
                cursor.execute(f"{{fn teradata_request_timeout({timeout})}}")

            for idx, stmt in enumerate(sql_statements):
                stmt = stmt.strip()
                if not stmt:
                    continue
                executed_count += 1
                try:
                    cursor.execute(stmt)

                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        serialized_rows = [
                            {
                                col: _serialize_value(val)
                                for col, val in zip(columns, row)
                            }
                            for row in rows
                        ]
                        results.append({
                            "statement_index": idx,
                            "sql": stmt[:120],
                            "type": "result_set",
                            "columns": columns,
                            "rows": serialized_rows,
                            "row_count": len(rows),
                        })
                        summary_lines.append(
                            f"Statement {idx + 1}: {len(rows)} row(s) returned"
                        )
                    else:
                        rowcount = cursor.rowcount if cursor.rowcount >= 0 else 0
                        results.append({
                            "statement_index": idx,
                            "sql": stmt[:120],
                            "type": "ok",
                            "rows_affected": rowcount,
                        })
                        summary_lines.append(
                            f"Statement {idx + 1}: OK"
                            + (f" ({rowcount} row(s) affected)" if rowcount else "")
                        )

                except Exception as exc:
                    err_msg = str(exc)
                    code = self._extract_teradata_error_code(err_msg)

                    if error_list and code is not None and code in error_list:
                        tolerated_errors.append({
                            "statement_index": idx,
                            "sql": stmt[:120],
                            "error_code": code,
                            "message": err_msg[:200],
                        })
                        summary_lines.append(
                            f"Statement {idx + 1}: tolerated error {code}"
                        )
                        continue

                    return {
                        "success": False,
                        "returncode": code or 1,
                        "stdout": "\n".join(summary_lines),
                        "stderr": err_msg,
                        "results": results,
                        "statement_count": executed_count,
                        "tolerated_errors": tolerated_errors,
                        "failed_statement": stmt[:120],
                    }

            return {
                "success": True,
                "returncode": 0,
                "stdout": "\n".join(summary_lines),
                "stderr": "",
                "results": results,
                "statement_count": executed_count,
                "tolerated_errors": tolerated_errors,
            }

        except TeradataConnectionError:
            raise
        except Exception as e:
            logger.error("execute_statements failed: %s", e, exc_info=True)
            raise TeradataQueryError(f"Statement execution failed: {e}") from e
        finally:
            if conn:
                conn.close()

    def close(self):
        """Close database connections and clean up resources."""
        # No persistent connections to close with native teradatasql
        # Each operation creates and closes its own connection
        self._connected = False
        logger.debug("Teradata client cleanup complete")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
