"""Unit tests for Metadata Store.

Tests the actual production API:
- MetadataStore (abstract base class)
- SQLiteMetadataStore (SQLite-based storage)
- JSONMetadataStore (JSON file-based storage)
- ExecutionRecord and MetadataEntry dataclasses
- StorageBackend enum
- create_metadata_store factory function
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from elt_mcp_server.storage.metadata_store import (
    ExecutionRecord,
    JSONMetadataStore,
    MetadataEntry,
    MetadataStore,
    SQLiteMetadataStore,
    StorageBackend,
    create_metadata_store,
)

# ============================================================================
# ExecutionRecord Dataclass Tests
# ============================================================================


class TestExecutionRecord:
    """Tests for the ExecutionRecord dataclass."""

    def test_create_minimal_record(self):
        """Test creating a record with only required fields."""
        record = ExecutionRecord(
            execution_id="exec-1",
            pipeline_name="my_pipeline",
            status="running",
            start_time=datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        )
        assert record.execution_id == "exec-1"
        assert record.pipeline_name == "my_pipeline"
        assert record.status == "running"
        assert record.end_time is None
        assert record.duration_minutes is None
        assert record.error_message is None
        assert record.metadata is None

    def test_create_full_record(self):
        """Test creating a record with all fields populated."""
        record = ExecutionRecord(
            execution_id="exec-2",
            pipeline_name="etl_pipeline",
            status="success",
            start_time=datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            duration_minutes=30.0,
            error_message=None,
            metadata={"rows_processed": 1000, "source": "teradata"},
        )
        assert record.duration_minutes == 30.0
        assert record.metadata["rows_processed"] == 1000

    def test_to_dict_serializes_datetimes(self):
        """Test that to_dict converts datetimes to ISO format strings."""
        start = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        record = ExecutionRecord(
            execution_id="exec-1",
            pipeline_name="pipe",
            status="success",
            start_time=start,
            end_time=end,
            metadata={"key": "value"},
        )
        data = record.to_dict()
        assert data["start_time"] == start.isoformat()
        assert data["end_time"] == end.isoformat()
        # metadata is JSON-serialized as a string
        assert data["metadata"] == json.dumps({"key": "value"})

    def test_to_dict_none_end_time(self):
        """Test to_dict when end_time is None."""
        record = ExecutionRecord(
            execution_id="exec-1",
            pipeline_name="pipe",
            status="running",
            start_time=datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        )
        data = record.to_dict()
        assert data["end_time"] is None
        assert data["metadata"] is None

    def test_from_dict_basic(self):
        """Test creating a record from a dictionary."""
        data = {
            "execution_id": "exec-1",
            "pipeline_name": "pipe",
            "status": "success",
            "start_time": "2025-01-15T10:00:00+00:00",
            "end_time": "2025-01-15T10:30:00+00:00",
            "duration_minutes": 30.0,
            "error_message": None,
            "metadata": None,
        }
        record = ExecutionRecord.from_dict(data)
        assert record.execution_id == "exec-1"
        assert record.start_time.year == 2025
        assert record.start_time.tzinfo is not None

    def test_from_dict_with_json_metadata(self):
        """Test from_dict when metadata is a JSON string."""
        data = {
            "execution_id": "exec-1",
            "pipeline_name": "pipe",
            "status": "success",
            "start_time": "2025-01-15T10:00:00+00:00",
            "end_time": None,
            "duration_minutes": None,
            "error_message": None,
            "metadata": '{"rows": 500}',
        }
        record = ExecutionRecord.from_dict(data)
        assert record.metadata == {"rows": 500}

    def test_from_dict_strips_created_at(self):
        """Test that from_dict removes the created_at field from DB rows."""
        data = {
            "execution_id": "exec-1",
            "pipeline_name": "pipe",
            "status": "success",
            "start_time": "2025-01-15T10:00:00+00:00",
            "end_time": None,
            "duration_minutes": None,
            "error_message": None,
            "metadata": None,
            "created_at": "2025-01-15T10:00:00",
        }
        record = ExecutionRecord.from_dict(data)
        assert record.execution_id == "exec-1"

    def test_from_dict_naive_datetime_gets_utc(self):
        """Test that naive datetimes get UTC timezone added."""
        data = {
            "execution_id": "exec-1",
            "pipeline_name": "pipe",
            "status": "done",
            "start_time": "2025-01-15T10:00:00",
            "end_time": "2025-01-15T11:00:00",
            "duration_minutes": None,
            "error_message": None,
            "metadata": None,
        }
        record = ExecutionRecord.from_dict(data)
        assert record.start_time.tzinfo == timezone.utc
        assert record.end_time.tzinfo == timezone.utc

    def test_roundtrip_to_dict_from_dict(self):
        """Test that to_dict -> from_dict preserves data."""
        original = ExecutionRecord(
            execution_id="exec-rt",
            pipeline_name="roundtrip_pipe",
            status="failed",
            start_time=datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 6, 1, 12, 5, 0, tzinfo=timezone.utc),
            duration_minutes=5.0,
            error_message="Connection timeout",
            metadata={"retry_count": 3},
        )
        data = original.to_dict()
        restored = ExecutionRecord.from_dict(data)
        assert restored.execution_id == original.execution_id
        assert restored.pipeline_name == original.pipeline_name
        assert restored.status == original.status
        assert restored.error_message == original.error_message
        assert restored.metadata == original.metadata


# ============================================================================
# MetadataEntry Dataclass Tests
# ============================================================================


class TestMetadataEntry:
    """Tests for the MetadataEntry dataclass."""

    def test_create_entry_without_ttl(self):
        """Test creating an entry without TTL."""
        entry = MetadataEntry(
            key="schema.customers",
            value={"columns": ["id", "name"]},
            timestamp=datetime.now(timezone.utc),
        )
        assert entry.key == "schema.customers"
        assert entry.ttl_seconds is None
        assert entry.tags is None

    def test_create_entry_with_ttl_and_tags(self):
        """Test creating an entry with TTL and tags."""
        entry = MetadataEntry(
            key="cache.query_result",
            value=[1, 2, 3],
            timestamp=datetime.now(timezone.utc),
            ttl_seconds=3600,
            tags=["cache", "query"],
        )
        assert entry.ttl_seconds == 3600
        assert entry.tags == ["cache", "query"]

    def test_is_expired_no_ttl(self):
        """Test that entries without TTL never expire."""
        entry = MetadataEntry(
            key="permanent",
            value="data",
            timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=None,
        )
        assert entry.is_expired() is False

    def test_is_expired_zero_ttl(self):
        """Test that entries with zero TTL never expire (falsy check)."""
        entry = MetadataEntry(
            key="zero_ttl",
            value="data",
            timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=0,
        )
        assert entry.is_expired() is False

    def test_is_expired_old_entry(self):
        """Test that old entries with TTL are expired."""
        entry = MetadataEntry(
            key="old",
            value="stale_data",
            timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=60,
        )
        assert entry.is_expired() is True

    def test_is_expired_fresh_entry(self):
        """Test that recent entries with large TTL are not expired."""
        entry = MetadataEntry(
            key="fresh",
            value="data",
            timestamp=datetime.now(timezone.utc),
            ttl_seconds=86400,
        )
        assert entry.is_expired() is False

    def test_is_expired_naive_timestamp(self):
        """Test is_expired with a naive timestamp (should assume UTC)."""
        old_naive = datetime(2020, 1, 1)
        entry = MetadataEntry(
            key="naive",
            value="data",
            timestamp=old_naive,
            ttl_seconds=60,
        )
        assert entry.is_expired() is True


# ============================================================================
# StorageBackend Enum Tests
# ============================================================================


class TestStorageBackend:
    """Tests for the StorageBackend enum."""

    def test_sqlite_value(self):
        assert StorageBackend.SQLITE.value == "sqlite"

    def test_json_value(self):
        assert StorageBackend.JSON.value == "json"

    def test_postgresql_value(self):
        assert StorageBackend.POSTGRESQL.value == "postgresql"

    def test_memory_value(self):
        assert StorageBackend.MEMORY.value == "memory"


# ============================================================================
# MetadataStore ABC Tests
# ============================================================================


class TestMetadataStoreABC:
    """Tests for the MetadataStore abstract base class."""

    def test_cannot_instantiate_abc(self):
        """Test that MetadataStore cannot be instantiated directly."""
        with pytest.raises(TypeError):
            MetadataStore()

    def test_subclass_must_implement_all_methods(self):
        """Test that a subclass missing methods cannot be instantiated."""

        class IncompleteStore(MetadataStore):
            pass

        with pytest.raises(TypeError):
            IncompleteStore()

    def test_sqlite_is_subclass(self):
        """Test that SQLiteMetadataStore is a subclass of MetadataStore."""
        assert issubclass(SQLiteMetadataStore, MetadataStore)

    def test_json_is_subclass(self):
        """Test that JSONMetadataStore is a subclass of MetadataStore."""
        assert issubclass(JSONMetadataStore, MetadataStore)


# ============================================================================
# SQLiteMetadataStore Tests
# ============================================================================


class TestSQLiteMetadataStore:
    """Test suite for SQLiteMetadataStore."""

    @pytest.fixture
    def store(self, tmp_path):
        """Create SQLiteMetadataStore with a temporary database."""
        db_path = str(tmp_path / "test_metadata.db")
        return SQLiteMetadataStore(db_path=db_path)

    @pytest.fixture
    def sample_record(self):
        """Create a sample ExecutionRecord."""
        return ExecutionRecord(
            execution_id="exec-001",
            pipeline_name="etl_customers",
            status="success",
            start_time=datetime(2025, 6, 1, 10, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 6, 1, 10, 15, 0, tzinfo=timezone.utc),
            duration_minutes=15.0,
            error_message=None,
            metadata={"rows": 5000},
        )

    @pytest.fixture
    def sample_entry(self):
        """Create a sample MetadataEntry."""
        return MetadataEntry(
            key="schema.customers",
            value={"columns": ["id", "name", "email"]},
            timestamp=datetime.now(timezone.utc),
            ttl_seconds=3600,
            tags=["schema", "teradata"],
        )

    # --- Initialization ---

    def test_init_creates_database_file(self, tmp_path):
        """Test that __init__ creates the SQLite database file."""
        db_path = str(tmp_path / "new_metadata.db")
        SQLiteMetadataStore(db_path=db_path)
        assert Path(db_path).exists()

    def test_init_creates_tables(self, tmp_path):
        """Test that __init__ creates executions and metadata_cache tables."""
        db_path = str(tmp_path / "schema_check.db")
        SQLiteMetadataStore(db_path=db_path)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert "executions" in tables
        assert "metadata_cache" in tables

    def test_init_creates_indices(self, tmp_path):
        """Test that __init__ creates the expected indices."""
        db_path = str(tmp_path / "index_check.db")
        SQLiteMetadataStore(db_path=db_path)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indices = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert "idx_executions_pipeline" in indices
        assert "idx_executions_status" in indices
        assert "idx_executions_start_time" in indices

    def test_init_idempotent(self, tmp_path):
        """Test that creating multiple stores on the same DB is safe."""
        db_path = str(tmp_path / "idempotent.db")
        SQLiteMetadataStore(db_path=db_path)
        SQLiteMetadataStore(db_path=db_path)
        # Should not raise

    # --- record_execution ---

    def test_record_execution_success(self, store, sample_record):
        """Test recording a pipeline execution."""
        result = store.record_execution(sample_record)
        assert result is True

    def test_record_execution_and_retrieve(self, store, sample_record):
        """Test recording then retrieving an execution."""
        store.record_execution(sample_record)
        retrieved = store.get_execution(sample_record.execution_id)

        assert retrieved is not None
        assert retrieved.execution_id == "exec-001"
        assert retrieved.pipeline_name == "etl_customers"
        assert retrieved.status == "success"
        assert retrieved.metadata == {"rows": 5000}

    def test_record_execution_replace_on_duplicate_id(self, store):
        """Test that INSERT OR REPLACE works for duplicate execution_id."""
        record1 = ExecutionRecord(
            execution_id="exec-dup",
            pipeline_name="pipe_a",
            status="running",
            start_time=datetime.now(timezone.utc),
        )
        record2 = ExecutionRecord(
            execution_id="exec-dup",
            pipeline_name="pipe_a",
            status="success",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            duration_minutes=5.0,
        )

        store.record_execution(record1)
        store.record_execution(record2)

        retrieved = store.get_execution("exec-dup")
        assert retrieved.status == "success"

    def test_record_execution_with_error(self, store):
        """Test recording a failed execution with an error message."""
        record = ExecutionRecord(
            execution_id="exec-fail",
            pipeline_name="broken_pipe",
            status="failed",
            start_time=datetime.now(timezone.utc),
            error_message="Connection refused",
        )
        result = store.record_execution(record)
        assert result is True

        retrieved = store.get_execution("exec-fail")
        assert retrieved.error_message == "Connection refused"

    def test_record_execution_without_metadata(self, store):
        """Test recording an execution without metadata."""
        record = ExecutionRecord(
            execution_id="exec-nometa",
            pipeline_name="simple_pipe",
            status="success",
            start_time=datetime.now(timezone.utc),
        )
        store.record_execution(record)
        retrieved = store.get_execution("exec-nometa")
        assert retrieved.metadata is None

    # --- get_execution ---

    def test_get_execution_nonexistent(self, store):
        """Test getting a non-existent execution returns None."""
        result = store.get_execution("does-not-exist")
        assert result is None

    # --- get_executions ---

    def test_get_executions_all(self, store):
        """Test getting all executions."""
        for i in range(5):
            record = ExecutionRecord(
                execution_id=f"exec-{i}",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 1, 1 + i, tzinfo=timezone.utc),
            )
            store.record_execution(record)

        results = store.get_executions()
        assert len(results) == 5

    def test_get_executions_filter_by_pipeline(self, store):
        """Test filtering executions by pipeline name."""
        store.record_execution(
            ExecutionRecord(
                execution_id="e1",
                pipeline_name="pipe_a",
                status="success",
                start_time=datetime.now(timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="e2",
                pipeline_name="pipe_b",
                status="success",
                start_time=datetime.now(timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="e3",
                pipeline_name="pipe_a",
                status="failed",
                start_time=datetime.now(timezone.utc),
            )
        )

        results = store.get_executions(pipeline_name="pipe_a")
        assert len(results) == 2
        assert all(r.pipeline_name == "pipe_a" for r in results)

    def test_get_executions_filter_by_status(self, store):
        """Test filtering executions by status."""
        store.record_execution(
            ExecutionRecord(
                execution_id="s1",
                pipeline_name="pipe",
                status="success",
                start_time=datetime.now(timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="s2",
                pipeline_name="pipe",
                status="failed",
                start_time=datetime.now(timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="s3",
                pipeline_name="pipe",
                status="success",
                start_time=datetime.now(timezone.utc),
            )
        )

        results = store.get_executions(status="failed")
        assert len(results) == 1
        assert results[0].execution_id == "s2"

    def test_get_executions_filter_by_date_range(self, store):
        """Test filtering executions by start/end date."""
        store.record_execution(
            ExecutionRecord(
                execution_id="d1",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 1, 10, tzinfo=timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="d2",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 3, 15, tzinfo=timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="d3",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 6, 20, tzinfo=timezone.utc),
            )
        )

        results = store.get_executions(
            start_date=datetime(2025, 2, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 5, 1, tzinfo=timezone.utc),
        )
        assert len(results) == 1
        assert results[0].execution_id == "d2"

    def test_get_executions_with_limit(self, store):
        """Test that the limit parameter is respected."""
        for i in range(10):
            store.record_execution(
                ExecutionRecord(
                    execution_id=f"lim-{i}",
                    pipeline_name="pipe",
                    status="success",
                    start_time=datetime(2025, 1, 1 + i, tzinfo=timezone.utc),
                )
            )

        results = store.get_executions(limit=3)
        assert len(results) == 3

    def test_get_executions_ordered_by_start_time_desc(self, store):
        """Test that results are ordered by start_time descending."""
        store.record_execution(
            ExecutionRecord(
                execution_id="old",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="new",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 12, 31, tzinfo=timezone.utc),
            )
        )

        results = store.get_executions()
        assert results[0].execution_id == "new"
        assert results[1].execution_id == "old"

    def test_get_executions_empty(self, store):
        """Test get_executions on empty store returns empty list."""
        results = store.get_executions()
        assert results == []

    # --- store_metadata ---

    def test_store_metadata_success(self, store, sample_entry):
        """Test storing a metadata entry."""
        result = store.store_metadata(sample_entry)
        assert result is True

    def test_store_metadata_and_retrieve(self, store, sample_entry):
        """Test storing then retrieving a metadata entry."""
        store.store_metadata(sample_entry)
        retrieved = store.get_metadata("schema.customers")

        assert retrieved is not None
        assert retrieved.key == "schema.customers"
        assert retrieved.value == {"columns": ["id", "name", "email"]}
        assert retrieved.tags == ["schema", "teradata"]

    def test_store_metadata_replace_on_duplicate_key(self, store):
        """Test that storing with the same key replaces the entry."""
        entry1 = MetadataEntry(
            key="config.setting",
            value="old_value",
            timestamp=datetime.now(timezone.utc),
        )
        entry2 = MetadataEntry(
            key="config.setting",
            value="new_value",
            timestamp=datetime.now(timezone.utc),
        )

        store.store_metadata(entry1)
        store.store_metadata(entry2)

        retrieved = store.get_metadata("config.setting")
        assert retrieved.value == "new_value"

    # --- get_metadata ---

    def test_get_metadata_nonexistent(self, store):
        """Test getting a non-existent key returns None."""
        result = store.get_metadata("nonexistent.key")
        assert result is None

    def test_get_metadata_expired_entry_returns_none(self, store):
        """Test that expired entries are auto-deleted and return None."""
        entry = MetadataEntry(
            key="expiring.key",
            value="ephemeral",
            timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=60,
        )
        store.store_metadata(entry)

        result = store.get_metadata("expiring.key")
        assert result is None

    def test_get_metadata_non_expired_entry(self, store):
        """Test that non-expired entries are returned."""
        entry = MetadataEntry(
            key="fresh.key",
            value="still_good",
            timestamp=datetime.now(timezone.utc),
            ttl_seconds=86400,
        )
        store.store_metadata(entry)

        result = store.get_metadata("fresh.key")
        assert result is not None
        assert result.value == "still_good"

    def test_get_metadata_no_ttl_never_expires(self, store):
        """Test that entries without TTL never expire."""
        entry = MetadataEntry(
            key="permanent.key",
            value="forever",
            timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=None,
        )
        store.store_metadata(entry)

        result = store.get_metadata("permanent.key")
        assert result is not None
        assert result.value == "forever"

    # --- delete_metadata ---

    def test_delete_metadata_existing(self, store):
        """Test deleting an existing metadata entry."""
        entry = MetadataEntry(
            key="to_delete",
            value="temp",
            timestamp=datetime.now(timezone.utc),
        )
        store.store_metadata(entry)

        result = store.delete_metadata("to_delete")
        assert result is True

        # Verify deletion
        assert store.get_metadata("to_delete") is None

    def test_delete_metadata_nonexistent(self, store):
        """Test deleting a non-existent key still returns True (DELETE succeeds)."""
        result = store.delete_metadata("ghost_key")
        assert result is True

    # --- cleanup_expired ---

    def test_cleanup_expired_removes_old_entries(self, store):
        """Test that cleanup_expired removes entries past their TTL."""
        # Store an expired entry
        expired_entry = MetadataEntry(
            key="old.entry",
            value="stale",
            timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=60,
        )
        store.store_metadata(expired_entry)

        # Store a fresh entry
        fresh_entry = MetadataEntry(
            key="new.entry",
            value="fresh",
            timestamp=datetime.now(timezone.utc),
            ttl_seconds=86400,
        )
        store.store_metadata(fresh_entry)

        count = store.cleanup_expired()
        assert count == 1

        # Expired entry is gone; fresh entry remains
        # Need to directly check via SQL since get_metadata also auto-deletes expired
        conn = sqlite3.connect(store.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT key FROM metadata_cache")
        keys = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert "old.entry" not in keys
        assert "new.entry" in keys

    def test_cleanup_expired_no_ttl_entries_untouched(self, store):
        """Test that entries without TTL are not removed by cleanup."""
        entry = MetadataEntry(
            key="no_ttl",
            value="permanent",
            timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=None,
        )
        store.store_metadata(entry)

        count = store.cleanup_expired()
        assert count == 0

        # Entry still exists
        conn = sqlite3.connect(store.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT key FROM metadata_cache WHERE key = ?", ("no_ttl",))
        assert cursor.fetchone() is not None
        conn.close()

    def test_cleanup_expired_returns_zero_when_none_expired(self, store):
        """Test cleanup returns 0 when no entries are expired."""
        entry = MetadataEntry(
            key="valid",
            value="ok",
            timestamp=datetime.now(timezone.utc),
            ttl_seconds=99999,
        )
        store.store_metadata(entry)

        count = store.cleanup_expired()
        assert count == 0

    def test_cleanup_expired_on_empty_store(self, store):
        """Test cleanup on an empty store returns 0."""
        count = store.cleanup_expired()
        assert count == 0

    # --- get_statistics ---

    def test_get_statistics_empty(self, store):
        """Test statistics on an empty store."""
        stats = store.get_statistics()
        assert stats["total_executions"] == 0
        assert stats["successful_executions"] == 0
        assert stats["failed_executions"] == 0
        assert stats["total_metadata_entries"] == 0
        assert "database_size_mb" in stats

    def test_get_statistics_with_data(self, store):
        """Test statistics with some data."""
        store.record_execution(
            ExecutionRecord(
                execution_id="s1",
                pipeline_name="pipe",
                status="success",
                start_time=datetime.now(timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="s2",
                pipeline_name="pipe",
                status="failed",
                start_time=datetime.now(timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="s3",
                pipeline_name="pipe",
                status="success",
                start_time=datetime.now(timezone.utc),
            )
        )
        store.store_metadata(
            MetadataEntry(
                key="k1",
                value="v1",
                timestamp=datetime.now(timezone.utc),
            )
        )

        stats = store.get_statistics()
        assert stats["total_executions"] == 3
        assert stats["successful_executions"] == 2
        assert stats["failed_executions"] == 1
        assert stats["total_metadata_entries"] == 1

    # --- Error handling ---

    def test_record_execution_handles_db_error(self, tmp_path):
        """Test that record_execution returns False on DB error."""
        db_path = str(tmp_path / "err.db")
        store = SQLiteMetadataStore(db_path=db_path)

        record = ExecutionRecord(
            execution_id="e1",
            pipeline_name="pipe",
            status="ok",
            start_time=datetime.now(timezone.utc),
        )

        # Corrupt the database by making it a directory
        import os

        os.remove(db_path)
        os.makedirs(db_path)

        result = store.record_execution(record)
        assert result is False

        # Cleanup
        os.rmdir(db_path)

    def test_get_execution_handles_db_error(self, tmp_path):
        """Test that get_execution returns None on DB error."""
        db_path = str(tmp_path / "err2.db")
        store = SQLiteMetadataStore(db_path=db_path)

        import os

        os.remove(db_path)
        os.makedirs(db_path)

        result = store.get_execution("anything")
        assert result is None

        os.rmdir(db_path)

    def test_get_executions_handles_db_error(self, tmp_path):
        """Test that get_executions returns [] on DB error."""
        db_path = str(tmp_path / "err3.db")
        store = SQLiteMetadataStore(db_path=db_path)

        import os

        os.remove(db_path)
        os.makedirs(db_path)

        result = store.get_executions()
        assert result == []

        os.rmdir(db_path)

    def test_store_metadata_handles_db_error(self, tmp_path):
        """Test that store_metadata returns False on DB error."""
        db_path = str(tmp_path / "err4.db")
        store = SQLiteMetadataStore(db_path=db_path)

        import os

        os.remove(db_path)
        os.makedirs(db_path)

        entry = MetadataEntry(
            key="k", value="v", timestamp=datetime.now(timezone.utc)
        )
        result = store.store_metadata(entry)
        assert result is False

        os.rmdir(db_path)

    def test_get_statistics_handles_db_error(self, tmp_path):
        """Test that get_statistics returns {} on DB error."""
        db_path = str(tmp_path / "err5.db")
        store = SQLiteMetadataStore(db_path=db_path)

        import os

        os.remove(db_path)
        os.makedirs(db_path)

        result = store.get_statistics()
        assert result == {}

        os.rmdir(db_path)


# ============================================================================
# JSONMetadataStore Tests
# ============================================================================


class TestJSONMetadataStore:
    """Test suite for JSONMetadataStore."""

    @pytest.fixture
    def store(self, tmp_path):
        """Create JSONMetadataStore with a temporary directory."""
        storage_dir = str(tmp_path / "json_metadata")
        return JSONMetadataStore(storage_dir=storage_dir)

    @pytest.fixture
    def sample_record(self):
        """Create a sample ExecutionRecord."""
        return ExecutionRecord(
            execution_id="exec-json-001",
            pipeline_name="json_pipeline",
            status="success",
            start_time=datetime(2025, 6, 1, 10, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 6, 1, 10, 20, 0, tzinfo=timezone.utc),
            duration_minutes=20.0,
            metadata={"format": "json"},
        )

    @pytest.fixture
    def sample_entry(self):
        """Create a sample MetadataEntry."""
        return MetadataEntry(
            key="json.test.key",
            value={"data": [1, 2, 3]},
            timestamp=datetime.now(timezone.utc),
            ttl_seconds=7200,
            tags=["json", "test"],
        )

    # --- Initialization ---

    def test_init_creates_storage_directory(self, tmp_path):
        """Test that __init__ creates the storage directory."""
        storage_dir = str(tmp_path / "new_json_store")
        JSONMetadataStore(storage_dir=storage_dir)
        assert Path(storage_dir).exists()
        assert Path(storage_dir).is_dir()

    def test_init_creates_nested_directory(self, tmp_path):
        """Test that __init__ creates nested directories."""
        storage_dir = str(tmp_path / "a" / "b" / "c")
        JSONMetadataStore(storage_dir=storage_dir)
        assert Path(storage_dir).exists()

    def test_init_idempotent(self, tmp_path):
        """Test creating multiple stores on the same directory is safe."""
        storage_dir = str(tmp_path / "idem_json")
        JSONMetadataStore(storage_dir=storage_dir)
        JSONMetadataStore(storage_dir=storage_dir)
        # No error raised

    def test_init_sets_file_paths(self, tmp_path):
        """Test that __init__ sets executions_file and metadata_file paths."""
        storage_dir = str(tmp_path / "paths_check")
        store = JSONMetadataStore(storage_dir=storage_dir)
        assert store.executions_file == Path(storage_dir) / "executions.json"
        assert store.metadata_file == Path(storage_dir) / "metadata_cache.json"

    # --- record_execution ---

    def test_record_execution_success(self, store, sample_record):
        """Test recording an execution creates the JSON file."""
        result = store.record_execution(sample_record)
        assert result is True
        assert store.executions_file.exists()

    def test_record_execution_and_retrieve(self, store, sample_record):
        """Test recording then retrieving an execution."""
        store.record_execution(sample_record)
        retrieved = store.get_execution("exec-json-001")

        assert retrieved is not None
        assert retrieved.execution_id == "exec-json-001"
        assert retrieved.pipeline_name == "json_pipeline"
        assert retrieved.status == "success"
        assert retrieved.metadata == {"format": "json"}

    def test_record_execution_overwrites_same_id(self, store):
        """Test that recording with the same ID overwrites."""
        record1 = ExecutionRecord(
            execution_id="dup",
            pipeline_name="pipe",
            status="running",
            start_time=datetime.now(timezone.utc),
        )
        record2 = ExecutionRecord(
            execution_id="dup",
            pipeline_name="pipe",
            status="success",
            start_time=datetime.now(timezone.utc),
        )
        store.record_execution(record1)
        store.record_execution(record2)

        retrieved = store.get_execution("dup")
        assert retrieved.status == "success"

    # --- get_execution ---

    def test_get_execution_nonexistent(self, store):
        """Test getting a non-existent execution returns None."""
        assert store.get_execution("nope") is None

    def test_get_execution_from_empty_store(self, store):
        """Test getting execution when no file exists yet."""
        assert store.get_execution("anything") is None

    # --- get_executions ---

    def test_get_executions_all(self, store):
        """Test getting all executions."""
        for i in range(5):
            store.record_execution(
                ExecutionRecord(
                    execution_id=f"je-{i}",
                    pipeline_name="pipe",
                    status="success",
                    start_time=datetime(2025, 1, 1 + i, tzinfo=timezone.utc),
                )
            )
        results = store.get_executions()
        assert len(results) == 5

    def test_get_executions_filter_by_pipeline(self, store):
        """Test filtering by pipeline_name."""
        store.record_execution(
            ExecutionRecord(
                execution_id="a1",
                pipeline_name="alpha",
                status="success",
                start_time=datetime.now(timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="b1",
                pipeline_name="beta",
                status="success",
                start_time=datetime.now(timezone.utc),
            )
        )

        results = store.get_executions(pipeline_name="alpha")
        assert len(results) == 1
        assert results[0].pipeline_name == "alpha"

    def test_get_executions_filter_by_status(self, store):
        """Test filtering by status."""
        store.record_execution(
            ExecutionRecord(
                execution_id="ok",
                pipeline_name="pipe",
                status="success",
                start_time=datetime.now(timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="bad",
                pipeline_name="pipe",
                status="failed",
                start_time=datetime.now(timezone.utc),
            )
        )

        results = store.get_executions(status="failed")
        assert len(results) == 1
        assert results[0].execution_id == "bad"

    def test_get_executions_filter_by_date_range(self, store):
        """Test filtering by date range."""
        store.record_execution(
            ExecutionRecord(
                execution_id="jan",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 1, 15, tzinfo=timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="jun",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 6, 15, tzinfo=timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="dec",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 12, 15, tzinfo=timezone.utc),
            )
        )

        results = store.get_executions(
            start_date=datetime(2025, 3, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
        )
        assert len(results) == 1
        assert results[0].execution_id == "jun"

    def test_get_executions_with_limit(self, store):
        """Test the limit parameter."""
        for i in range(10):
            store.record_execution(
                ExecutionRecord(
                    execution_id=f"lim-{i}",
                    pipeline_name="pipe",
                    status="success",
                    start_time=datetime(2025, 1, 1 + i, tzinfo=timezone.utc),
                )
            )
        results = store.get_executions(limit=4)
        assert len(results) == 4

    def test_get_executions_ordered_desc(self, store):
        """Test that executions are ordered by start_time descending."""
        store.record_execution(
            ExecutionRecord(
                execution_id="first",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
        )
        store.record_execution(
            ExecutionRecord(
                execution_id="last",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 12, 1, tzinfo=timezone.utc),
            )
        )

        results = store.get_executions()
        assert results[0].execution_id == "last"

    def test_get_executions_empty(self, store):
        """Test get_executions on empty store."""
        assert store.get_executions() == []

    # --- store_metadata ---

    def test_store_metadata_success(self, store, sample_entry):
        """Test storing metadata."""
        result = store.store_metadata(sample_entry)
        assert result is True
        assert store.metadata_file.exists()

    def test_store_metadata_and_retrieve(self, store, sample_entry):
        """Test storing then retrieving metadata."""
        store.store_metadata(sample_entry)
        retrieved = store.get_metadata("json.test.key")

        assert retrieved is not None
        assert retrieved.key == "json.test.key"
        assert retrieved.value == {"data": [1, 2, 3]}
        assert retrieved.tags == ["json", "test"]

    def test_store_metadata_overwrites_duplicate_key(self, store):
        """Test that storing with the same key overwrites."""
        entry1 = MetadataEntry(
            key="dup", value="old", timestamp=datetime.now(timezone.utc)
        )
        entry2 = MetadataEntry(
            key="dup", value="new", timestamp=datetime.now(timezone.utc)
        )
        store.store_metadata(entry1)
        store.store_metadata(entry2)

        retrieved = store.get_metadata("dup")
        assert retrieved.value == "new"

    # --- get_metadata ---

    def test_get_metadata_nonexistent(self, store):
        """Test getting a non-existent key returns None."""
        assert store.get_metadata("ghost") is None

    def test_get_metadata_expired_returns_none(self, store):
        """Test that expired entries are auto-deleted and return None."""
        entry = MetadataEntry(
            key="stale",
            value="old",
            timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
            ttl_seconds=60,
        )
        store.store_metadata(entry)
        assert store.get_metadata("stale") is None

    def test_get_metadata_no_ttl_persists(self, store):
        """Test that entries without TTL persist indefinitely."""
        entry = MetadataEntry(
            key="eternal",
            value="forever",
            timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )
        store.store_metadata(entry)

        result = store.get_metadata("eternal")
        assert result is not None
        assert result.value == "forever"

    # --- delete_metadata ---

    def test_delete_metadata_existing(self, store):
        """Test deleting an existing entry."""
        entry = MetadataEntry(
            key="doomed", value="bye", timestamp=datetime.now(timezone.utc)
        )
        store.store_metadata(entry)

        result = store.delete_metadata("doomed")
        assert result is True
        assert store.get_metadata("doomed") is None

    def test_delete_metadata_nonexistent(self, store):
        """Test deleting a non-existent key returns False."""
        result = store.delete_metadata("nope")
        assert result is False

    # --- cleanup_expired ---

    def test_cleanup_expired_removes_old_entries(self, store):
        """Test cleanup removes expired entries."""
        store.store_metadata(
            MetadataEntry(
                key="expired1",
                value="x",
                timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
                ttl_seconds=60,
            )
        )
        store.store_metadata(
            MetadataEntry(
                key="expired2",
                value="y",
                timestamp=datetime(2020, 6, 1, tzinfo=timezone.utc),
                ttl_seconds=120,
            )
        )
        store.store_metadata(
            MetadataEntry(
                key="valid",
                value="z",
                timestamp=datetime.now(timezone.utc),
                ttl_seconds=86400,
            )
        )

        count = store.cleanup_expired()
        assert count == 2

        # Read metadata file directly to check
        data = json.loads(store.metadata_file.read_text())
        assert "expired1" not in data
        assert "expired2" not in data
        assert "valid" in data

    def test_cleanup_expired_no_ttl_untouched(self, store):
        """Test that entries without TTL are not removed."""
        store.store_metadata(
            MetadataEntry(
                key="no_ttl",
                value="safe",
                timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
            )
        )
        count = store.cleanup_expired()
        assert count == 0

    def test_cleanup_expired_empty_store(self, store):
        """Test cleanup on empty store returns 0."""
        assert store.cleanup_expired() == 0

    # --- Data persistence ---

    def test_data_persists_across_instances(self, tmp_path):
        """Test that data persists when a new store is created on the same dir."""
        storage_dir = str(tmp_path / "persist_test")

        store1 = JSONMetadataStore(storage_dir=storage_dir)
        store1.record_execution(
            ExecutionRecord(
                execution_id="persist-1",
                pipeline_name="pipe",
                status="success",
                start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
        )
        store1.store_metadata(
            MetadataEntry(
                key="persist.key",
                value="hello",
                timestamp=datetime.now(timezone.utc),
            )
        )

        # Create a new instance on the same directory
        store2 = JSONMetadataStore(storage_dir=storage_dir)

        exec_record = store2.get_execution("persist-1")
        assert exec_record is not None
        assert exec_record.pipeline_name == "pipe"

        meta_entry = store2.get_metadata("persist.key")
        assert meta_entry is not None
        assert meta_entry.value == "hello"

    # --- Internal helpers ---

    def test_read_json_nonexistent_file(self, store):
        """Test that _read_json returns {} for non-existent file."""
        result = store._read_json(Path("/nonexistent/file.json"))
        assert result == {}

    def test_read_json_corrupt_file(self, store):
        """Test that _read_json returns {} for corrupt JSON."""
        store.metadata_file.write_text("this is not valid json{{{")
        result = store._read_json(store.metadata_file)
        assert result == {}


# ============================================================================
# create_metadata_store Factory Tests
# ============================================================================


class TestCreateMetadataStore:
    """Tests for the create_metadata_store factory function."""

    def test_create_sqlite_store(self, tmp_path):
        """Test creating a SQLite store via factory."""
        db_path = str(tmp_path / "factory.db")
        store = create_metadata_store(StorageBackend.SQLITE, db_path=db_path)
        assert isinstance(store, SQLiteMetadataStore)
        assert store.db_path == db_path

    def test_create_sqlite_store_default_path(self):
        """Test creating a SQLite store with default path."""
        store = create_metadata_store(StorageBackend.SQLITE)
        assert isinstance(store, SQLiteMetadataStore)
        assert store.db_path == "metadata.db"
        # Clean up the default file if created
        Path("metadata.db").unlink(missing_ok=True)

    def test_create_json_store(self, tmp_path):
        """Test creating a JSON store via factory."""
        storage_dir = str(tmp_path / "factory_json")
        store = create_metadata_store(StorageBackend.JSON, storage_dir=storage_dir)
        assert isinstance(store, JSONMetadataStore)
        assert store.storage_dir == Path(storage_dir)

    def test_create_json_store_default_dir(self):
        """Test creating a JSON store with default directory."""
        store = create_metadata_store(StorageBackend.JSON)
        assert isinstance(store, JSONMetadataStore)
        assert store.storage_dir == Path("metadata")
        # Clean up the default directory if created
        import shutil

        shutil.rmtree("metadata", ignore_errors=True)

    def test_create_unsupported_backend_raises(self):
        """Test that unsupported backends raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported backend"):
            create_metadata_store(StorageBackend.POSTGRESQL)

    def test_create_memory_backend_raises(self):
        """Test that memory backend raises ValueError (not implemented)."""
        with pytest.raises(ValueError, match="Unsupported backend"):
            create_metadata_store(StorageBackend.MEMORY)


# ============================================================================
# Cross-Store Consistency Tests
# ============================================================================


class TestCrossStoreConsistency:
    """Tests ensuring SQLite and JSON stores behave consistently."""

    @pytest.fixture
    def sqlite_store(self, tmp_path):
        db_path = str(tmp_path / "cross.db")
        return SQLiteMetadataStore(db_path=db_path)

    @pytest.fixture
    def json_store(self, tmp_path):
        storage_dir = str(tmp_path / "cross_json")
        return JSONMetadataStore(storage_dir=storage_dir)

    def _make_records(self, count=5):
        """Helper to create multiple execution records."""
        records = []
        for i in range(count):
            records.append(
                ExecutionRecord(
                    execution_id=f"cross-{i}",
                    pipeline_name=f"pipe_{i % 2}",
                    status="success" if i % 3 != 0 else "failed",
                    start_time=datetime(2025, 1, 1 + i, tzinfo=timezone.utc),
                    metadata={"index": i},
                )
            )
        return records

    def test_both_stores_record_and_retrieve(self, sqlite_store, json_store):
        """Test that both stores give the same result for record/retrieve."""
        record = ExecutionRecord(
            execution_id="shared-1",
            pipeline_name="shared_pipe",
            status="success",
            start_time=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
            metadata={"source": "test"},
        )

        sqlite_store.record_execution(record)
        json_store.record_execution(record)

        sqlite_result = sqlite_store.get_execution("shared-1")
        json_result = json_store.get_execution("shared-1")

        assert sqlite_result.execution_id == json_result.execution_id
        assert sqlite_result.pipeline_name == json_result.pipeline_name
        assert sqlite_result.status == json_result.status
        assert sqlite_result.metadata == json_result.metadata

    def test_both_stores_filter_by_pipeline(self, sqlite_store, json_store):
        """Test that both stores filter by pipeline consistently."""
        records = self._make_records()
        for record in records:
            sqlite_store.record_execution(record)
            json_store.record_execution(record)

        sqlite_results = sqlite_store.get_executions(pipeline_name="pipe_0")
        json_results = json_store.get_executions(pipeline_name="pipe_0")

        assert len(sqlite_results) == len(json_results)

    def test_both_stores_metadata_roundtrip(self, sqlite_store, json_store):
        """Test that both stores handle metadata entries the same way."""
        entry = MetadataEntry(
            key="shared.config",
            value={"setting": True, "count": 42},
            timestamp=datetime.now(timezone.utc),
            ttl_seconds=3600,
            tags=["shared"],
        )

        sqlite_store.store_metadata(entry)
        json_store.store_metadata(entry)

        sqlite_meta = sqlite_store.get_metadata("shared.config")
        json_meta = json_store.get_metadata("shared.config")

        assert sqlite_meta.value == json_meta.value
        assert sqlite_meta.tags == json_meta.tags
