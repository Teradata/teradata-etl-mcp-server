"""Metadata storage and persistence for execution history and configuration.

This module provides persistent storage for pipeline execution history,
metadata caching, configuration, and performance metrics.
"""

import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class StorageBackend(Enum):
    """Supported storage backends."""

    SQLITE = "sqlite"
    JSON = "json"
    POSTGRESQL = "postgresql"
    MEMORY = "memory"


@dataclass
class ExecutionRecord:
    """Record of a pipeline execution."""

    execution_id: str
    pipeline_name: str
    status: str
    start_time: datetime
    end_time: datetime | None = None
    duration_minutes: float | None = None
    error_message: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data["start_time"] = self.start_time.isoformat() if self.start_time else None
        data["end_time"] = self.end_time.isoformat() if self.end_time else None
        data["metadata"] = json.dumps(self.metadata) if self.metadata else None
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionRecord":
        """Create from dictionary."""
        # Create a copy to avoid modifying original
        data = dict(data)
        # Remove created_at field if present (from database)
        data.pop("created_at", None)
        if isinstance(data.get("start_time"), str):
            start_time = datetime.fromisoformat(data["start_time"])
            # Ensure timezone-aware for comparisons
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            data["start_time"] = start_time
        if isinstance(data.get("end_time"), str):
            end_time = datetime.fromisoformat(data["end_time"])
            # Ensure timezone-aware for comparisons
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            data["end_time"] = end_time
        if isinstance(data.get("metadata"), str):
            data["metadata"] = json.loads(data["metadata"])
        return cls(**data)


@dataclass
class MetadataEntry:
    """Cached metadata entry."""

    key: str
    value: Any
    timestamp: datetime
    ttl_seconds: int | None = None
    tags: list[str] | None = None

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if not self.ttl_seconds:
            return False
        # Ensure timestamp is timezone-aware for comparison
        ts = self.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        return age > self.ttl_seconds


class MetadataStore(ABC):
    """Abstract base class for metadata storage."""

    @abstractmethod
    def record_execution(self, record: ExecutionRecord) -> bool:
        """Record a pipeline execution."""
        pass

    @abstractmethod
    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        """Get execution record by ID."""
        pass

    @abstractmethod
    def get_executions(
        self,
        pipeline_name: str | None = None,
        status: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        """Query execution records."""
        pass

    @abstractmethod
    def store_metadata(self, entry: MetadataEntry) -> bool:
        """Store metadata entry."""
        pass

    @abstractmethod
    def get_metadata(self, key: str) -> MetadataEntry | None:
        """Get metadata entry by key."""
        pass

    @abstractmethod
    def delete_metadata(self, key: str) -> bool:
        """Delete metadata entry."""
        pass

    @abstractmethod
    def cleanup_expired(self) -> int:
        """Remove expired metadata entries."""
        pass


class SQLiteMetadataStore(MetadataStore):
    """SQLite-based metadata storage."""

    def __init__(self, db_path: str = "metadata.db"):
        """
        Initialize SQLite metadata store.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._initialize_db()

    def _initialize_db(self) -> None:
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            # Executions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS executions (
                    execution_id TEXT PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    duration_minutes REAL,
                    error_message TEXT,
                    metadata TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Metadata cache table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metadata_cache (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    ttl_seconds INTEGER,
                    tags TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indices
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_executions_pipeline
                ON executions(pipeline_name)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_executions_status
                ON executions(status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_executions_start_time
                ON executions(start_time)
            """)

            conn.commit()
        finally:
            conn.close()

        logger.info("Initialized SQLite metadata store: %s", self.db_path)

    def record_execution(self, record: ExecutionRecord) -> bool:
        """Record a pipeline execution."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            data = record.to_dict()
            cursor.execute(
                """
                INSERT OR REPLACE INTO executions
                (execution_id, pipeline_name, status, start_time, end_time,
                 duration_minutes, error_message, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    data["execution_id"],
                    data["pipeline_name"],
                    data["status"],
                    data["start_time"],
                    data["end_time"],
                    data["duration_minutes"],
                    data["error_message"],
                    data["metadata"],
                ),
            )

            conn.commit()

            logger.debug("Recorded execution: %s", record.execution_id)
            return True

        except Exception as e:
            logger.error("Failed to record execution: %s", e, exc_info=True)
            return False
        finally:
            if conn is not None:
                conn.close()

    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        """Get execution record by ID."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT * FROM executions WHERE execution_id = ?
            """,
                (execution_id,),
            )

            row = cursor.fetchone()

            if row:
                return ExecutionRecord.from_dict(dict(row))
            return None

        except Exception as e:
            logger.error("Failed to get execution: %s", e, exc_info=True)
            return None
        finally:
            if conn is not None:
                conn.close()

    def get_executions(
        self,
        pipeline_name: str | None = None,
        status: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        """Query execution records."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            query = "SELECT * FROM executions WHERE 1=1"
            params = []

            if pipeline_name:
                query += " AND pipeline_name = ?"
                params.append(pipeline_name)

            if status:
                query += " AND status = ?"
                params.append(status)

            if start_date:
                query += " AND start_time >= ?"
                params.append(start_date.isoformat())

            if end_date:
                query += " AND start_time <= ?"
                params.append(end_date.isoformat())

            query += " ORDER BY start_time DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [ExecutionRecord.from_dict(dict(row)) for row in rows]

        except Exception as e:
            logger.error("Failed to query executions: %s", e, exc_info=True)
            return []
        finally:
            if conn is not None:
                conn.close()

    def store_metadata(self, entry: MetadataEntry) -> bool:
        """Store metadata entry."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT OR REPLACE INTO metadata_cache
                (key, value, timestamp, ttl_seconds, tags)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    entry.key,
                    json.dumps(entry.value),
                    entry.timestamp.isoformat(),
                    entry.ttl_seconds,
                    json.dumps(entry.tags) if entry.tags else None,
                ),
            )

            conn.commit()

            logger.debug("Stored metadata: %s", entry.key)
            return True

        except Exception as e:
            logger.error("Failed to store metadata: %s", e, exc_info=True)
            return False
        finally:
            if conn is not None:
                conn.close()

    def get_metadata(self, key: str) -> MetadataEntry | None:
        """Get metadata entry by key."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT * FROM metadata_cache WHERE key = ?
            """,
                (key,),
            )

            row = cursor.fetchone()

            if row:
                entry = MetadataEntry(
                    key=row["key"],
                    value=json.loads(row["value"]),
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    ttl_seconds=row["ttl_seconds"],
                    tags=json.loads(row["tags"]) if row["tags"] else None,
                )

                # Check if expired
                if entry.is_expired():
                    self.delete_metadata(key)
                    return None

                return entry

            return None

        except Exception as e:
            logger.error("Failed to get metadata: %s", e, exc_info=True)
            return None
        finally:
            if conn is not None:
                conn.close()

    def delete_metadata(self, key: str) -> bool:
        """Delete metadata entry."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("DELETE FROM metadata_cache WHERE key = ?", (key,))

            conn.commit()

            return True

        except Exception as e:
            logger.error("Failed to delete metadata: %s", e, exc_info=True)
            return False
        finally:
            if conn is not None:
                conn.close()

    def cleanup_expired(self) -> int:
        """Remove expired metadata entries."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get all entries with TTL
            cursor.execute("""
                SELECT key, timestamp, ttl_seconds
                FROM metadata_cache
                WHERE ttl_seconds IS NOT NULL
            """)

            rows = cursor.fetchall()
            expired_keys = []

            now = datetime.now(timezone.utc)
            for row in rows:
                timestamp = datetime.fromisoformat(row["timestamp"])
                # Ensure timestamp is timezone-aware for comparison
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                age = (now - timestamp).total_seconds()
                if age > row["ttl_seconds"]:
                    expired_keys.append(row["key"])

            # Delete expired entries
            if expired_keys:
                placeholders = ",".join("?" * len(expired_keys))
                cursor.execute(
                    f"DELETE FROM metadata_cache WHERE key IN ({placeholders})", expired_keys
                )

            conn.commit()

            if expired_keys:
                logger.info("Cleaned up %d expired metadata entries", len(expired_keys))

            return len(expired_keys)

        except Exception as e:
            logger.error("Failed to cleanup expired metadata: %s", e, exc_info=True)
            return 0
        finally:
            if conn is not None:
                conn.close()

    def get_statistics(self) -> dict[str, Any]:
        """Get storage statistics."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Execution counts
            cursor.execute("SELECT COUNT(*) FROM executions")
            total_executions = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM executions WHERE status = 'success'")
            successful_executions = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM executions WHERE status = 'failed'")
            failed_executions = cursor.fetchone()[0]

            # Metadata counts
            cursor.execute("SELECT COUNT(*) FROM metadata_cache")
            total_metadata = cursor.fetchone()[0]

            # Database size
            cursor.execute(
                "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()"
            )
            db_size_bytes = cursor.fetchone()[0]

            return {
                "total_executions": total_executions,
                "successful_executions": successful_executions,
                "failed_executions": failed_executions,
                "total_metadata_entries": total_metadata,
                "database_size_mb": db_size_bytes / (1024 * 1024),
            }

        except Exception as e:
            logger.error("Failed to get statistics: %s", e, exc_info=True)
            return {}
        finally:
            if conn is not None:
                conn.close()


class JSONMetadataStore(MetadataStore):
    """JSON file-based metadata storage."""

    def __init__(self, storage_dir: str = "metadata"):
        """
        Initialize JSON metadata store.

        Args:
            storage_dir: Directory for JSON files
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        self.executions_file = self.storage_dir / "executions.json"
        self.metadata_file = self.storage_dir / "metadata_cache.json"

        logger.info("Initialized JSON metadata store: %s", storage_dir)

    def _read_json(self, filepath: Path) -> dict[str, Any]:
        """Read JSON file."""
        if not filepath.exists():
            return {}
        try:
            with open(filepath) as f:
                return json.load(f)
        except Exception as e:
            logger.error("Failed to read %s: %s", filepath, e, exc_info=True)
            return {}

    def _write_json(self, filepath: Path, data: dict[str, Any]) -> bool:
        """Write JSON file."""
        try:
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2, default=str)
            return True
        except Exception as e:
            logger.error("Failed to write %s: %s", filepath, e, exc_info=True)
            return False

    def record_execution(self, record: ExecutionRecord) -> bool:
        """Record a pipeline execution."""
        executions = self._read_json(self.executions_file)
        executions[record.execution_id] = record.to_dict()
        return self._write_json(self.executions_file, executions)

    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        """Get execution record by ID."""
        executions = self._read_json(self.executions_file)
        data = executions.get(execution_id)
        if data:
            return ExecutionRecord.from_dict(data)
        return None

    def get_executions(
        self,
        pipeline_name: str | None = None,
        status: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        """Query execution records."""
        executions = self._read_json(self.executions_file)

        records = []
        for data in executions.values():
            record = ExecutionRecord.from_dict(data)

            # Apply filters
            if pipeline_name and record.pipeline_name != pipeline_name:
                continue
            if status and record.status != status:
                continue
            if start_date and record.start_time < start_date:
                continue
            if end_date and record.start_time > end_date:
                continue

            records.append(record)

        # Sort by start time descending
        records.sort(key=lambda x: x.start_time, reverse=True)

        return records[:limit]

    def store_metadata(self, entry: MetadataEntry) -> bool:
        """Store metadata entry."""
        metadata = self._read_json(self.metadata_file)
        metadata[entry.key] = {
            "value": entry.value,
            "timestamp": entry.timestamp.isoformat(),
            "ttl_seconds": entry.ttl_seconds,
            "tags": entry.tags,
        }
        return self._write_json(self.metadata_file, metadata)

    def get_metadata(self, key: str) -> MetadataEntry | None:
        """Get metadata entry by key."""
        metadata = self._read_json(self.metadata_file)
        data = metadata.get(key)

        if data:
            entry = MetadataEntry(
                key=key,
                value=data["value"],
                timestamp=datetime.fromisoformat(data["timestamp"]),
                ttl_seconds=data.get("ttl_seconds"),
                tags=data.get("tags"),
            )

            if entry.is_expired():
                self.delete_metadata(key)
                return None

            return entry

        return None

    def delete_metadata(self, key: str) -> bool:
        """Delete metadata entry."""
        metadata = self._read_json(self.metadata_file)
        if key in metadata:
            del metadata[key]
            return self._write_json(self.metadata_file, metadata)
        return False

    def cleanup_expired(self) -> int:
        """Remove expired metadata entries."""
        metadata = self._read_json(self.metadata_file)
        expired_keys = []

        now = datetime.now(timezone.utc)
        for key, data in metadata.items():
            if data.get("ttl_seconds"):
                timestamp = datetime.fromisoformat(data["timestamp"])
                # Ensure timestamp is timezone-aware for comparison
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                age = (now - timestamp).total_seconds()
                if age > data["ttl_seconds"]:
                    expired_keys.append(key)

        for key in expired_keys:
            del metadata[key]

        if expired_keys:
            self._write_json(self.metadata_file, metadata)
            logger.info("Cleaned up %d expired metadata entries", len(expired_keys))

        return len(expired_keys)


def create_metadata_store(
    backend: StorageBackend = StorageBackend.SQLITE, **kwargs
) -> MetadataStore:
    """
    Factory function to create metadata store.

    Args:
        backend: Storage backend type
        **kwargs: Backend-specific configuration

    Returns:
        MetadataStore instance
    """
    if backend == StorageBackend.SQLITE:
        return SQLiteMetadataStore(db_path=kwargs.get("db_path", "metadata.db"))
    elif backend == StorageBackend.JSON:
        return JSONMetadataStore(storage_dir=kwargs.get("storage_dir", "metadata"))
    else:
        raise ValueError(f"Unsupported backend: {backend}")
