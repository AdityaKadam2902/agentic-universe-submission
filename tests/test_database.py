"""Tests for database schema, constraints, and idempotency."""

import pytest
import os
import sqlite3
import tempfile
from pathlib import Path

from solution.db import Database, compute_file_hash, transaction
from solution.ingestion.discovery import (
    discover_files,
    discover_files_recursive,
    FileIdempotencyChecker,
    DiscoveredFile,
)


class TestDatabaseInitialization:
    """Tests for database initialization and schema."""

    @pytest.fixture
    def db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()
        yield db
        db.close()

    def test_connection_works(self, db):
        """Test that database connection works."""
        conn = db.connect()
        assert conn is not None

    def test_foreign_keys_enabled(self, db):
        """Test that foreign keys are enabled."""
        conn = db.connect()
        cursor = conn.execute("PRAGMA foreign_keys")
        assert cursor.fetchone()[0] == 1

    def test_tables_created(self, db):
        """Test that all tables are created."""
        conn = db.connect()
        tables = [
            "ingestion_files",
            "lead_batches",
            "leads",
            "rejections",
            "events",
            "event_associations",
            "call_links",
        ]
        for table in tables:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            assert cursor.fetchone() is not None, f"Table {table} not found"

    def test_unique_constraints(self, db):
        """Test that unique constraints exist."""
        conn = db.connect()

        # leads: UNIQUE(workflow_id, phone_hmac)
        cursor = conn.execute("""
            SELECT sql FROM sqlite_master
            WHERE type='table' AND name='leads'
        """)
        sql = cursor.fetchone()["sql"]
        assert "UNIQUE" in sql.upper()
        assert "workflow_id" in sql
        assert "phone_hmac" in sql

        # ingestion_files: UNIQUE(source_path)
        cursor = conn.execute("""
            SELECT sql FROM sqlite_master
            WHERE type='table' AND name='ingestion_files'
        """)
        sql = cursor.fetchone()["sql"]
        assert "UNIQUE" in sql.upper()
        assert "source_path" in sql

    def test_event_immutability_triggers(self, db):
        """Test that triggers prevent UPDATE/DELETE on events."""
        conn = db.connect()

        # Insert a test event
        conn.execute(
            """INSERT INTO events (event_id, workflow_id, event_type, occurred_at_utc, source_line, created_at_utc)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("test_evt_001", "motor_renewal", "call.initiated", "2026-01-01T00:00:00Z", 1, "2026-01-01T00:00:00Z"),
        )

        # Try to UPDATE - should fail
        with pytest.raises(Exception) as exc_info:
            conn.execute(
                "UPDATE events SET occurred_at_utc = '2027-01-01T00:00:00Z' WHERE event_id = 'test_evt_001'"
            )
        error_msg = str(exc_info.value).lower()
        assert "append-only" in error_msg or "not allowed" in error_msg or "error" in error_msg

        # Try to DELETE - should fail
        with pytest.raises(Exception) as exc_info:
            conn.execute(
                "DELETE FROM events WHERE event_id = 'test_evt_001'"
            )
        error_msg = str(exc_info.value).lower()
        assert "append-only" in error_msg or "not allowed" in error_msg or "error" in error_msg


class TestFileHashing:
    """Tests for file hash computation."""

    def test_compute_file_hash(self, tmp_path):
        """Test SHA-256 file hashing."""
        # Create a test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, World!")

        hash1 = compute_file_hash(test_file)
        assert len(hash1) == 64  # SHA-256 hex

        # Same file, same hash
        hash2 = compute_file_hash(test_file)
        assert hash1 == hash2

        # Different content, different hash
        test_file.write_text("Different content")
        hash3 = compute_file_hash(test_file)
        assert hash1 != hash3

    def test_hash_deterministic(self, tmp_path):
        """Test that same content produces same hash regardless of filename."""
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        content = "Same content"

        file1.write_text(content)
        file2.write_text(content)

        assert compute_file_hash(file1) == compute_file_hash(file2)


class TestFileIdempotency:
    """Tests for file-level idempotency."""

    @pytest.fixture
    def db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()
        yield db
        db.close()

    @pytest.fixture
    def test_files(self, tmp_path):
        """Create test data files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create a lead CSV file
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        lead_file = leads_dir / "batch_001.csv"
        lead_file.write_text("customer_name,phone,policy_number\nJohn,9876543210,POL123\n")

        # Create an events JSONL file
        events_dir = data_dir / "events"
        events_dir.mkdir()
        event_file = events_dir / "motor_renewal.jsonl"
        event_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated"}\n')

        return data_dir

    def test_discover_files(self, test_files):
        """Test file discovery."""
        files = discover_files(test_files)
        assert len(files) == 2  # One CSV, one JSONL

        # Check ordering (should be deterministic)
        assert files[0].file_kind == "leads"
        assert files[1].file_kind == "events"

    def test_discover_files_recursive(self, tmp_path):
        """Test recursive file discovery."""
        # Create nested structure
        base = tmp_path / "data"
        drop1 = base / "drop_1"
        drop1_leads = drop1 / "leads" / "motor_renewal"
        drop1_leads.mkdir(parents=True)
        (drop1_leads / "batch.csv").write_text("test\n")

        drop2 = base / "drop_2"
        drop2_events = drop2 / "events"
        drop2_events.mkdir(parents=True)
        (drop2_events / "motor.jsonl").write_text('{"test": true}\n')

        files = discover_files_recursive(base)
        # Should find both files
        assert len(files) == 2

    def test_same_file_noop(self, db, test_files):
        """Test that ingesting the same file twice is a no-op."""
        checker = FileIdempotencyChecker(db)

        # Discover files
        files = discover_files(test_files)
        lead_file = files[0]  # First file is leads

        # First ingestion - should be new
        is_noop, file_id = checker.check_and_record_file(lead_file)
        assert is_noop is False
        assert file_id is not None

        # Simulate successful processing: the ingestion marks the manifest
        # row completed in the same transaction as its data
        checker.complete_file(file_id, {})

        # Second ingestion - should be no-op
        is_noop, file_id2 = checker.check_and_record_file(lead_file)
        assert is_noop is True
        assert file_id == file_id2

    def test_different_path_same_content(self, db, tmp_path):
        """Test that different paths with same content are distinct files."""
        checker = FileIdempotencyChecker(db)

        # Create two files with same content
        file1 = tmp_path / "file1.csv"
        file2 = tmp_path / "file2.csv"
        content = "same,content\n"
        file1.write_text(content)
        file2.write_text(content)

        # Create discovered file objects
        disc1 = DiscoveredFile(
            source_path=file1,
            canonical_path=file1.resolve(),
            content_hash=compute_file_hash(file1),
            file_kind="leads",
            workflow_id="motor_renewal",
            batch_id="batch1",
            relative_path="file1.csv",
        )
        disc2 = DiscoveredFile(
            source_path=file2,
            canonical_path=file2.resolve(),
            content_hash=compute_file_hash(file2),
            file_kind="leads",
            workflow_id="motor_renewal",
            batch_id="batch2",
            relative_path="file2.csv",
        )

        # Both should be new (different paths)
        is_noop1, id1 = checker.check_and_record_file(disc1)
        assert is_noop1 is False

        is_noop2, id2 = checker.check_and_record_file(disc2)
        assert is_noop2 is False
        assert id1 != id2

    def test_changed_file_rejected(self, db, test_files):
        """Test that same path with different content raises error."""
        checker = FileIdempotencyChecker(db)

        files = discover_files(test_files)
        lead_file = files[0]

        # First ingestion
        is_noop, file_id = checker.check_and_record_file(lead_file)
        assert is_noop is False

        # Modify the file - use Path.write_text on source_path
        lead_file.source_path.write_text("modified,content\n")

        # Try to ingest modified file - should raise
        # Need to recompute hash and recreate DiscoveredFile
        from solution.ingestion.discovery import _create_discovered_file
        modified_disc = _create_discovered_file(
            lead_file.source_path,
            test_files,
            "leads",
            "motor_renewal",
            "batch_001",
        )

        with pytest.raises(ValueError) as exc_info:
            checker.check_and_record_file(modified_disc)
        assert "content changed" in str(exc_info.value).lower()


class TestTransactionRollback:
    """Tests for transaction rollback on error."""

    @pytest.fixture
    def db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()
        yield db
        db.close()

    def test_rollback_on_error(self, db):
        """Test that transaction rolls back on error."""
        conn = db.connect()

        # Insert a record
        conn.execute(
            "INSERT INTO leads (workflow_id, phone_hmac, batch_id, created_at_utc, updated_at_utc, attributes_json) VALUES (?, ?, ?, ?, ?, ?)",
            ("motor_renewal", "hmac123", "batch1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "{}"),
        )
        conn.commit()

        # Verify it exists
        cursor = conn.execute("SELECT COUNT(*) as count FROM leads")
        assert cursor.fetchone()["count"] == 1

        # Try a transaction that will fail
        with pytest.raises(Exception):
            with transaction(db):
                conn.execute(
                    "INSERT INTO leads (workflow_id, phone_hmac, batch_id, created_at_utc, updated_at_utc, attributes_json) VALUES (?, ?, ?, ?, ?, ?)",
                    ("motor_renewal", "hmac123", "batch2", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "{}"),
                )
                # This should violate unique constraint
                raise Exception("Intentional error")

        # Verify the insert was rolled back
        cursor = conn.execute("SELECT COUNT(*) as count FROM leads")
        assert cursor.fetchone()["count"] == 1  # Still only 1


class TestLeadOperations:
    """Tests for lead database operations."""

    @pytest.fixture
    def db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()
        yield db
        db.close()

    def test_insert_new_lead(self, db):
        """Test inserting a new lead."""
        conn = db.connect()

        from solution.db import insert_or_merge_lead

        # Create mock PII ciphertext
        pii_ct = b"encrypted_data"

        lead_id, is_new = insert_or_merge_lead(
            db,
            workflow_id="motor_renewal",
            phone_hmac="test_hmac_001",
            batch_id="batch_001",
            external_ref="POL123456",
            attributes={"policy_number": "POL123456", "premium_amount": "123.45"},
            pii_ciphertext=pii_ct,
            conn=conn,
        )

        assert is_new is True
        assert lead_id > 0

        # Verify lead was inserted
        cursor = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
        lead = dict(cursor.fetchone())
        assert lead["workflow_id"] == "motor_renewal"
        assert lead["phone_hmac"] == "test_hmac_001"
        assert lead["batch_id"] == "batch_001"
        assert lead["external_ref"] == "POL123456"

    def test_merge_lead_preserves_batch(self, db):
        """Test that merging a lead preserves the original batch_id."""
        conn = db.connect()

        from solution.db import insert_or_merge_lead

        # Insert first lead
        lead_id1, is_new1 = insert_or_merge_lead(
            db,
            workflow_id="motor_renewal",
            phone_hmac="test_hmac_002",
            batch_id="batch_001",
            external_ref="POL111",
            attributes={"policy_number": "POL111"},
            pii_ciphertext=b"ct1",
            conn=conn,
        )
        assert is_new1 is True

        # "Merge" with new data (simulating duplicate lead)
        lead_id2, is_new2 = insert_or_merge_lead(
            db,
            workflow_id="motor_renewal",
            phone_hmac="test_hmac_002",
            batch_id="batch_002",  # Different batch
            external_ref="POL222",  # New ref
            attributes={"premium_amount": "500.00"},  # New field
            pii_ciphertext=b"ct2",
            conn=conn,
        )
        assert is_new2 is False  # Not new, merged
        assert lead_id1 == lead_id2

        # Verify batch_id preserved from first insert
        cursor = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id1,))
        lead = dict(cursor.fetchone())
        assert lead["batch_id"] == "batch_001"  # Original batch preserved

    def test_merge_only_updates_nonnull(self, db):
        """Test that merge only overwrites non-null values."""
        conn = db.connect()

        from solution.db import insert_or_merge_lead

        # Insert lead with some fields
        lead_id, _ = insert_or_merge_lead(
            db,
            workflow_id="motor_renewal",
            phone_hmac="test_hmac_003",
            batch_id="batch_001",
            external_ref="POL333",
            attributes={"policy_number": "POL333", "premium_amount": "100.00"},
            pii_ciphertext=b"ct",
            conn=conn,
        )

        # Merge with null for premium_amount (should not overwrite)
        lead_id2, _ = insert_or_merge_lead(
            db,
            workflow_id="motor_renewal",
            phone_hmac="test_hmac_003",
            batch_id="batch_002",
            external_ref=None,  # None should not overwrite
            attributes={"premium_amount": None},  # None should not overwrite
            pii_ciphertext=None,  # None should not overwrite
            conn=conn,
        )

        # Verify original values preserved
        cursor = conn.execute("SELECT attributes_json FROM leads WHERE id = ?", (lead_id,))
        lead = dict(cursor.fetchone())
        attrs = __import__('json').loads(lead["attributes_json"])
        assert attrs["policy_number"] == "POL333"  # Preserved
        assert attrs["premium_amount"] == "100.00"  # Preserved (not overwritten with None)


class TestRejectionOperations:
    """Tests for rejection database operations."""

    @pytest.fixture
    def db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()
        yield db
        db.close()

    def test_insert_rejection(self, db):
        """Test inserting a rejection."""
        conn = db.connect()

        from solution.db import insert_rejection

        insert_rejection(
            db,
            workflow_id="motor_renewal",
            batch_id="batch_001",
            row_number=5,
            reason="missing_required:customer_name",
            conn=conn,
        )

        # Verify
        cursor = conn.execute(
            "SELECT * FROM rejections WHERE workflow_id = ? AND batch_id = ? AND row_number = ?",
            ("motor_renewal", "batch_001", 5),
        )
        row = cursor.fetchone()
        assert row is not None
        assert row["reason"] == "missing_required:customer_name"
        assert row["workflow_id"] == "motor_renewal"
        # Verify no PII columns
        assert "phone" not in row.keys()
        assert "customer_name" not in row.keys()


class TestEventOperations:
    """Tests for event database operations."""

    @pytest.fixture
    def db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()
        yield db
        db.close()

    def test_insert_event(self, db):
        """Test inserting an event."""
        conn = db.connect()

        from solution.db import insert_event

        event_id = insert_event(
            db,
            event_id="evt_test_001",
            workflow_id="motor_renewal",
            event_type="call.initiated",
            call_id="call_001",
            occurred_at_utc="2026-11-20T12:00:00Z",
            phone_hmac="phone_hmac_001",
            disposition_code=None,
            source_file_id=1,
            source_line=1,
            conn=conn,
        )

        assert event_id > 0

        # Verify
        cursor = conn.execute("SELECT * FROM events WHERE event_id = ?", ("evt_test_001",))
        event = dict(cursor.fetchone())
        assert event["event_type"] == "call.initiated"
        assert event["call_id"] == "call_001"
        assert event["phone_hmac"] == "phone_hmac_001"
        # Verify no plaintext phone
        assert "phone" not in event.keys()

    def test_event_id_unique(self, db):
        """Test that event_id must be unique."""
        conn = db.connect()

        from solution.db import insert_event

        # Insert first event
        insert_event(
            db,
            event_id="evt_dup_001",
            workflow_id="motor_renewal",
            event_type="call.initiated",
            call_id=None,
            occurred_at_utc="2026-01-01T00:00:00Z",
            phone_hmac=None,
            disposition_code=None,
            source_file_id=1,
            source_line=1,
            conn=conn,
        )

        # Try to insert duplicate - should raise
        with pytest.raises(Exception):  # IntegrityError or similar
            insert_event(
                db,
                event_id="evt_dup_001",  # Same ID
                workflow_id="motor_renewal",
                event_type="call.initiated",
                call_id=None,
                occurred_at_utc="2026-01-01T00:00:00Z",
                phone_hmac=None,
                disposition_code=None,
                source_file_id=1,
                source_line=2,
                conn=conn,
            )


class TestAssociationOperations:
    """Tests for event association and call link operations."""

    @pytest.fixture
    def db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()
        yield db
        db.close()

    def test_set_event_association(self, db):
        """Test setting event association."""
        conn = db.connect()

        from solution.db import set_event_association, get_event_association

        # First insert an event to reference
        conn.execute(
            """INSERT INTO events (event_id, workflow_id, event_type, occurred_at_utc, source_line, created_at_utc)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("evt_assoc_001", "motor_renewal", "call.initiated", "2026-01-01T00:00:00Z", 1, "2026-01-01T00:00:00Z"),
        )

        # Set association
        set_event_association(
            db,
            event_id="evt_assoc_001",
            lead_id=123,
            status="linked",
            conn=conn,
        )

        # Get association
        assoc = get_event_association(db, "evt_assoc_001", conn)
        assert assoc is not None
        assert assoc["lead_id"] == 123
        assert assoc["association_status"] == "linked"

    def test_set_call_link(self, db):
        """Test setting call link."""
        conn = db.connect()

        from solution.db import set_call_link, get_call_link

        # First insert a lead
        conn.execute(
            "INSERT INTO leads (workflow_id, phone_hmac, batch_id, created_at_utc, updated_at_utc, attributes_json) VALUES (?, ?, ?, ?, ?, ?)",
            ("motor_renewal", "hmac_001", "batch1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "{}"),
        )
        lead_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Set call link
        result = set_call_link(
            db,
            workflow_id="motor_renewal",
            call_id="call_001",
            lead_id=lead_id,
            conn=conn,
        )
        assert result is True

        # Get call link
        link = get_call_link(db, "motor_renewal", "call_001", conn)
        assert link is not None
        assert link["lead_id"] == lead_id

    def test_call_link_conflict(self, db):
        """Test that call link conflict is detected."""
        conn = db.connect()

        from solution.db import set_call_link

        # Insert two leads
        conn.execute(
            "INSERT INTO leads (workflow_id, phone_hmac, batch_id, created_at_utc, updated_at_utc, attributes_json) VALUES (?, ?, ?, ?, ?, ?)",
            ("motor_renewal", "hmac_001", "batch1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "{}"),
        )
        lead1_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        conn.execute(
            "INSERT INTO leads (workflow_id, phone_hmac, batch_id, created_at_utc, updated_at_utc, attributes_json) VALUES (?, ?, ?, ?, ?, ?)",
            ("motor_renewal", "hmac_002", "batch1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "{}"),
        )
        lead2_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Set call link for first lead
        set_call_link(
            db,
            workflow_id="motor_renewal",
            call_id="call_001",
            lead_id=lead1_id,
            conn=conn,
        )

        # Try to set call link for different lead - should return False
        result = set_call_link(
            db,
            workflow_id="motor_renewal",
            call_id="call_001",
            lead_id=lead2_id,  # Different lead
            conn=conn,
        )
        assert result is False  # Conflict detected
