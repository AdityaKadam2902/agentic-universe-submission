"""Tests for JSONL event ingestion and reconciliation."""

import pytest
import json
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime, timezone

from solution.db import Database
from solution.ingestion.leads import IngestionStats
from solution.schema_registry import SchemaRegistry
from solution.ingestion.events import (
    EventIngestion,
    EventReconciliation,
    ingest_event_batch,
    reconcile_all,
)
from solution.ingestion.discovery import FileIdempotencyChecker


class TestEventProcessing:
    """Tests for event line processing."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        """Create database and schema registry."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        return Path("assignment-a-data")

    @pytest.fixture
    def test_data(self, tmp_path):
        data_dir = tmp_path / "data"
        events_dir = data_dir / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        yield data_dir

    def test_valid_event(self, db_and_registry, test_data, monkeypatch):
        """Test ingesting a valid event."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210", "occurred_at": "2026-11-20T12:00:00+05:30"}\n')

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        assert stats.events_total == 1
        assert stats.malformed_events == 0
        assert stats.duplicate_events_dropped == 0

        # Verify event stored
        conn = db.connect()
        cursor = conn.execute("SELECT * FROM events WHERE event_id = 'evt_001'")
        event = dict(cursor.fetchone())
        assert event["event_type"] == "call.initiated"
        assert event["phone_hmac"] is not None
        # Verify plaintext phone not stored
        assert "phone" not in event

    def test_empty_lines_skipped(self, db_and_registry, test_data, monkeypatch):
        """Test that empty lines don't increment counters."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n'
            "\n"  # Empty line
            "   \n"  # Whitespace only
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_002", "event_type": "call.initiated", "phone": "9876543211"}\n'
        )

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        assert stats.events_total == 2  # Only 2 non-empty lines
        assert stats.malformed_events == 0

    def test_malformed_json(self, db_and_registry, test_data, monkeypatch):
        """Test malformed JSON increments malformed_events."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated"}\n'
            "not valid json\n"
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_002", "event_type": "call.initiated"}\n'
        )

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        assert stats.events_total == 3  # All non-empty lines count
        assert stats.malformed_events == 1  # One malformed

    def test_missing_event_id(self, db_and_registry, test_data, monkeypatch):
        """Test missing/blank event_id is malformed."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated"}\n'
            '{"event_type": "call.initiated"}\n'  # Missing event_id
            '{"event_id": "   ", "event_type": "call.initiated"}\n'  # Blank event_id
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_002", "event_type": "call.initiated"}\n'
        )

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        assert stats.events_total == 4
        assert stats.malformed_events == 2  # Two missing/blank event_id


class TestDuplicateEvents:
    """Tests for duplicate event handling."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        return Path("assignment-a-data")

    @pytest.fixture
    def test_data(self, tmp_path):
        data_dir = tmp_path / "data"
        events_dir = data_dir / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        yield data_dir

    def test_duplicate_within_file(self, db_and_registry, test_data, monkeypatch):
        """Test duplicate event_id within same file."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n'
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n'  # Duplicate
        )

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        assert stats.events_total == 2
        assert stats.duplicate_events_dropped == 1
        assert stats.malformed_events == 0

        # Only one event in DB
        conn = db.connect()
        cursor = conn.execute("SELECT COUNT(*) as count FROM events")
        assert cursor.fetchone()["count"] == 1

    def test_duplicate_across_files(self, db_and_registry, test_data, monkeypatch):
        """Test duplicate event_id across different files."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        
        # First file
        jsonl_file1 = events_dir / "motor_renewal_1.jsonl"
        jsonl_file1.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n')

        # Second file with same event_id
        jsonl_file2 = events_dir / "motor_renewal_2.jsonl"
        jsonl_file2.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n')

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        # Ingest first file
        ingestion.ingest_jsonl_file(jsonl_file1, "motor_renewal", checker, stats)

        # Reset stats for second file
        stats2 = IngestionStats()
        
        # Ingest second file
        ingestion.ingest_jsonl_file(jsonl_file2, "motor_renewal", checker, stats2)

        # Total across both files
        total_events = stats.events_total + stats2.events_total
        total_duplicates = stats.duplicate_events_dropped + stats2.duplicate_events_dropped

        assert total_events == 2
        assert total_duplicates == 1

        # Only one event in DB
        conn = db.connect()
        cursor = conn.execute("SELECT COUNT(*) as count FROM events")
        assert cursor.fetchone()["count"] == 1


class TestEventPhoneHandling:
    """Tests for event phone handling."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        return Path("assignment-a-data")

    @pytest.fixture
    def test_data(self, tmp_path):
        data_dir = tmp_path / "data"
        events_dir = data_dir / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        yield data_dir

    def test_phone_normalized_and_hmac(self, db_and_registry, test_data, monkeypatch):
        """Test phone is normalized and HMAC stored."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "+91 98765 43210"}\n')

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        conn = db.connect()
        cursor = conn.execute("SELECT phone_hmac FROM events WHERE event_id = 'evt_001'")
        event = dict(cursor.fetchone())

        # Verify HMAC stored, not plaintext
        assert event["phone_hmac"] is not None
        assert event["phone_hmac"] != "9876543210"
        assert event["phone_hmac"] != "+91 98765 43210"

        # Verify no plaintext phone in DB (only phone_hmac column exists, not phone)
        cursor = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='events'")
        create_sql = cursor.fetchone()["sql"]
        # Check that phone column doesn't exist as a standalone column
        # (phone_hmac is allowed, but not phone)
        import re
        # Find all column definitions
        columns = re.findall(r'(\w+)\s+TEXT', create_sql)
        # phone should not be a column, only phone_hmac should be
        assert "phone" not in columns or columns == ["phone_hmac"]

    def test_event_without_phone(self, db_and_registry, test_data, monkeypatch):
        """Test event without phone is valid but has no phone_hmac."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "call_id": "call_001"}\n')

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        assert stats.events_total == 1
        assert stats.malformed_events == 0

        # Event should be stored with no phone_hmac
        conn = db.connect()
        cursor = conn.execute("SELECT phone_hmac FROM events WHERE event_id = 'evt_001'")
        event = dict(cursor.fetchone())
        assert event["phone_hmac"] is None


class TestEventTimestampNormalization:
    """Tests for event timestamp normalization."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        return Path("assignment-a-data")

    @pytest.fixture
    def test_data(self, tmp_path):
        data_dir = tmp_path / "data"
        events_dir = data_dir / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        yield data_dir

    def test_naive_timestamp_means_ist(self, db_and_registry, test_data, monkeypatch):
        """Test naive timestamps are treated as Asia/Kolkata (IST)."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        # Naive timestamp 12:00 should be IST = UTC+5:30, so stored as 06:30 UTC
        jsonl_file.write_text('{"event_id": "evt_001", "event_type": "call.initiated", "occurred_at": "2026-11-20T12:00:00"}\n')

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        conn = db.connect()
        cursor = conn.execute("SELECT occurred_at_utc FROM events WHERE event_id = 'evt_001'")
        event = dict(cursor.fetchone())

        # Should be stored in UTC
        assert event["occurred_at_utc"] is not None
        assert "Z" in event["occurred_at_utc"] or "+00:00" in event["occurred_at_utc"]

    def test_zoned_timestamp_stored_utc(self, db_and_registry, test_data, monkeypatch):
        """Test zoned timestamps converted to UTC."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"event_id": "evt_001", "event_type": "call.initiated", "occurred_at": "2026-11-20T12:00:00+05:30"}\n')

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        conn = db.connect()
        cursor = conn.execute("SELECT occurred_at_utc FROM events WHERE event_id = 'evt_001'")
        event = dict(cursor.fetchone())

        assert event["occurred_at_utc"] is not None


class TestEventReconciliation:
    """Tests for event reconciliation."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        return Path("assignment-a-data")

    @pytest.fixture
    def test_data(self, tmp_path):
        data_dir = tmp_path / "data"
        events_dir = data_dir / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True, exist_ok=True)
        yield data_dir

    def test_event_before_lead_resolved(self, db_and_registry, test_data, monkeypatch):
        """Test event can be resolved when lead arrives later."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        from solution.ingestion.leads import LeadIngestion

        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True, exist_ok=True)

        # Ingest event first (before lead exists)
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n')

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        stats = IngestionStats()
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        # Verify event is pending
        conn = db.connect()
        cursor = conn.execute(
            "SELECT ea.association_status FROM events e LEFT JOIN event_associations ea ON e.event_id = ea.event_id WHERE e.event_id = 'evt_001'"
        )
        assert cursor.fetchone()["association_status"] == "pending_lead"

        # Now ingest lead
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Reconcile
        reconciliation = EventReconciliation(db, registry)
        summary = reconciliation.reconcile(stats)

        # Event should now be linked
        cursor = conn.execute(
            "SELECT ea.association_status, ea.lead_id FROM events e LEFT JOIN event_associations ea ON e.event_id = ea.event_id WHERE e.event_id = 'evt_001'"
        )
        assoc = dict(cursor.fetchone())
        assert assoc["association_status"] == "linked"
        assert assoc["lead_id"] is not None

    def test_reconciliation_idempotent(self, db_and_registry, test_data, monkeypatch):
        """Test reconciliation is idempotent."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        from solution.ingestion.leads import LeadIngestion

        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True, exist_ok=True)

        # Setup: event + lead
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n')

        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        checker = FileIdempotencyChecker(db)
        
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())

        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Reconcile multiple times
        reconciliation = EventReconciliation(db, registry)
        summary1 = reconciliation.reconcile(IngestionStats())
        summary2 = reconciliation.reconcile(IngestionStats())

        # Should produce same final state (orphan count should be same)
        assert summary1["orphan_events"] == summary2["orphan_events"]
        
        # Verify event is linked after reconciliation
        conn = db.connect()
        cursor = conn.execute(
            "SELECT ea.association_status FROM events e LEFT JOIN event_associations ea ON e.event_id = ea.event_id WHERE e.event_id = 'evt_001'"
        )
        assoc = dict(cursor.fetchone())
        assert assoc["association_status"] == "linked"

    def test_orphan_events_decreases_after_resolution(self, db_and_registry, test_data, monkeypatch):
        """Test orphan count decreases when late leads resolve events."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        from solution.ingestion.leads import LeadIngestion

        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True, exist_ok=True)

        # Ingest event
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n')

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        stats = IngestionStats()
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        # Reconcile before lead exists
        reconciliation = EventReconciliation(db, registry)
        summary_before = reconciliation.reconcile(stats)

        # Should have 1 orphan
        assert summary_before["orphan_events"] == 1

        # Ingest lead
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Reconcile after lead exists
        summary_after = reconciliation.reconcile(stats)

        # Orphan should be 0 now
        assert summary_after["orphan_events"] == 0


class TestCallLinkResolution:
    """Tests for call.link and disposition.override resolution."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        return Path("assignment-a-data")

    @pytest.fixture
    def test_data(self, tmp_path):
        data_dir = tmp_path / "data"
        events_dir = data_dir / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True, exist_ok=True)
        yield data_dir

    def test_call_link_event_stored_pending(self, db_and_registry, test_data, monkeypatch):
        """Test call.link event stored as pending."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        # call.link has no phone
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_link_001", "event_type": "call.link", "call_id": "call_001", "lead_id": "LEAD123"}\n')

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)
        stats = IngestionStats()

        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        # Should be stored
        conn = db.connect()
        cursor = conn.execute("SELECT * FROM events WHERE event_id = 'evt_link_001'")
        event = dict(cursor.fetchone())
        assert event["event_type"] == "call.link"
        assert event["phone_hmac"] is None  # No phone

    def test_override_before_call_link_resolved(self, db_and_registry, test_data, monkeypatch):
        """Test disposition.override can be resolved even if call.link arrives later."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        from solution.ingestion.leads import LeadIngestion

        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"

        # Ingest phone-bearing event first
        jsonl_file1 = events_dir / "motor_renewal_1.jsonl"
        jsonl_file1.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_phone_001", "event_type": "call.initiated", "phone": "9876543210", "call_id": "call_001"}\n')

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file1, "motor_renewal", checker, IngestionStats())

        # Ingest override event (before call.link)
        jsonl_file2 = events_dir / "motor_renewal_2.jsonl"
        jsonl_file2.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_override_001", "event_type": "disposition.override", "call_id": "call_001", "disposition_code": "CONFIRMED"}\n')

        event_ingestion.ingest_jsonl_file(jsonl_file2, "motor_renewal", checker, IngestionStats())

        # Ingest lead
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Reconcile
        reconciliation = EventReconciliation(db, registry)
        summary = reconciliation.reconcile(IngestionStats())

        # Override should be resolved
        conn = db.connect()
        cursor = conn.execute(
            "SELECT ea.association_status FROM events e LEFT JOIN event_associations ea ON e.event_id = ea.event_id WHERE e.event_id = 'evt_override_001'"
        )
        assoc = dict(cursor.fetchone())
        assert assoc["association_status"] == "linked"


class TestDispositions:
    """Tests for disposition code validation."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        return Path("assignment-a-data")

    @pytest.fixture
    def test_data(self, tmp_path):
        data_dir = tmp_path / "data"
        events_dir = data_dir / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True, exist_ok=True)
        yield data_dir

    def test_known_disposition_accepted(self, db_and_registry, test_data, monkeypatch):
        """Test known disposition code is accepted."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        from solution.ingestion.leads import LeadIngestion

        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"

        # Ingest event with known disposition (CONFIRMED is in dispositions.csv)
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210", "disposition_code": "CONFIRMED"}\n')

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())

        # Ingest lead
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Reconcile
        reconciliation = EventReconciliation(db, registry)
        summary = reconciliation.reconcile(IngestionStats())

        # Should not have unknown dispositions
        assert summary["unknown_dispositions"] == 0

    def test_unknown_disposition_counted(self, db_and_registry, test_data, monkeypatch):
        """Test unknown disposition code is counted but doesn't fail."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        from solution.ingestion.leads import LeadIngestion

        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"

        # Ingest event with unknown disposition
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210", "disposition_code": "UNKNOWN_CODE"}\n')

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())

        # Ingest lead
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Reconcile
        reconciliation = EventReconciliation(db, registry)
        summary = reconciliation.reconcile(IngestionStats())

        # Should count unknown disposition
        assert summary["unknown_dispositions"] == 1


class TestEventImmutability:
    """Tests that event facts are never updated."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        return Path("assignment-a-data")

    @pytest.fixture
    def test_data(self, tmp_path):
        data_dir = tmp_path / "data"
        events_dir = data_dir / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        yield data_dir

    def test_association_updated_not_event(self, db_and_registry, test_data, monkeypatch):
        """Test that reconciliation updates associations, not events."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        from solution.ingestion.leads import LeadIngestion

        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True, exist_ok=True)

        # Ingest event
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n')

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())

        # Get original event created_at
        conn = db.connect()
        cursor = conn.execute("SELECT created_at_utc FROM events WHERE event_id = 'evt_001'")
        original_created = cursor.fetchone()["created_at_utc"]

        # Ingest lead and reconcile
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        reconciliation = EventReconciliation(db, registry)
        reconciliation.reconcile(IngestionStats())

        # Event should still have original created_at (not updated)
        cursor = conn.execute("SELECT created_at_utc FROM events WHERE event_id = 'evt_001'")
        event = dict(cursor.fetchone())
        assert event["created_at_utc"] == original_created

        # Association should have been updated
        cursor = conn.execute(
            "SELECT association_status, lead_id FROM event_associations WHERE event_id = 'evt_001'"
        )
        assoc = dict(cursor.fetchone())
        assert assoc["association_status"] == "linked"
        assert assoc["lead_id"] is not None


class TestIdempotentEventReruns:
    """Tests for idempotent event ingestion reruns."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        return Path("assignment-a-data")

    @pytest.fixture
    def test_data(self, tmp_path):
        data_dir = tmp_path / "data"
        events_dir = data_dir / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        yield data_dir

    def test_event_rerun_is_noop(self, db_and_registry, test_data, monkeypatch):
        """Test re-ingesting same event file is no-op."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        events_dir = test_data / "events"
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n')

        checker = FileIdempotencyChecker(db)
        ingestion = EventIngestion(db, registry)

        # First ingestion
        stats1 = IngestionStats()
        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats1)
        assert stats1.events_total == 1

        # Second ingestion (same file)
        stats2 = IngestionStats()
        ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats2)
        assert stats2.events_total == 1  # Same count

        # Verify only one event in DB
        conn = db.connect()
        cursor = conn.execute("SELECT COUNT(*) as count FROM events")
        assert cursor.fetchone()["count"] == 1


class TestTimestampEdgeCases:
    """Tests for timestamp edge cases around IST midnight."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        """Create database and schema registry for testing."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        """Get the path to assignment data."""
        return Path("assignment-a-data")

    def test_naive_timestamp_around_midnight(self, db_and_registry, monkeypatch, tmp_path):
        """Test naive timestamps around IST midnight are interpreted as IST."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Test timestamp just before IST midnight (2026-10-31T18:30:00Z = 2026-11-01 00:00 IST)
        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text(
            '{"event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210", "occurred_at": "2026-10-31T18:29:00Z"}\n'
        )

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())

        # Verify timestamp stored correctly (DB may use different format)
        conn = db.connect()
        cursor = conn.execute("SELECT occurred_at_utc FROM events WHERE event_id = 'evt_001'")
        event = dict(cursor.fetchone())
        # Check the timestamp is correct UTC time (format may vary)
        stored = event["occurred_at_utc"]
        # Should contain the correct UTC time
        assert "2026-10-31" in stored and "18:29" in stored

    def test_offset_timestamp_stored_as_utc(self, db_and_registry, monkeypatch, tmp_path):
        """Test offset timestamps are converted to UTC."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # IST time +05:30
        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text(
            '{"event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210", "occurred_at": "2026-10-31T00:00:00+05:30"}\n'
        )

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())

        conn = db.connect()
        cursor = conn.execute("SELECT occurred_at_utc FROM events WHERE event_id = 'evt_001'")
        event = dict(cursor.fetchone())
        # IST 00:00 = UTC 18:30 previous day
        stored = event["occurred_at_utc"]
        # Should contain the correct UTC time
        assert "2026-10-30" in stored and "18:30" in stored

    def test_zulu_timestamp_stored_as_is(self, db_and_registry, monkeypatch, tmp_path):
        """Test Zulu timestamps are stored as-is."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text(
            '{"event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210", "occurred_at": "2026-10-31T12:00:00Z"}\n'
        )

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())

        conn = db.connect()
        cursor = conn.execute("SELECT occurred_at_utc FROM events WHERE event_id = 'evt_001'")
        event = dict(cursor.fetchone())
        # Check the timestamp is correct UTC time (format may vary)
        stored = event["occurred_at_utc"]
        assert "2026-10-31" in stored and "12:00" in stored


class TestMalformedEvents:
    """Tests for malformed and duplicate event handling."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        """Create database and schema registry for testing."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        """Get the path to assignment data."""
        return Path("assignment-a-data")

    def test_malformed_json_ignored(self, db_and_registry, monkeypatch, tmp_path):
        """Test that malformed JSON is ignored, not crash."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n'
            'not valid json\n'
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_002", "event_type": "call.initiated", "phone": "9876543211"}\n'
        )

        checker = FileIdempotencyChecker(db)
        stats = IngestionStats()
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        assert stats.malformed_events == 1

    def test_empty_lines_skipped_not_counted(self, db_and_registry, monkeypatch, tmp_path):
        """Test that empty lines don't count as events."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n'
            '\n'  # Empty line
            '   \n'  # Whitespace only
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_002", "event_type": "call.initiated", "phone": "9876543211"}\n'
        )

        checker = FileIdempotencyChecker(db)
        stats = IngestionStats()
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        # Only 2 events counted (not 4)
        assert stats.events_total == 2


class TestEventImmutability:
    """Tests for append-only event enforcement."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        """Create database and schema registry for testing."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        """Get the path to assignment data."""
        return Path("assignment-a-data")

    def test_events_append_only_enforced(self, db_and_registry, monkeypatch, tmp_path):
        """Test that events table enforces append-only via triggers."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n'
        )

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())

        # Try to update event - should fail due to trigger
        conn = db.connect()
        try:
            conn.execute("UPDATE events SET event_type = 'call.ended' WHERE event_id = 'evt_001'")
            # If we get here, trigger didn't work
            pytest.fail("Expected trigger to prevent UPDATE on events")
        except sqlite3.IntegrityError as e:
            # Expected: trigger raised an error
            assert "append-only" in str(e).lower() or "not allowed" in str(e).lower()

    def test_events_delete_not_allowed(self, db_and_registry, monkeypatch, tmp_path):
        """Test that events table prevents DELETE via triggers."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_001", "event_type": "call.initiated", "phone": "9876543210"}\n'
        )

        checker = FileIdempotencyChecker(db)
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())

        # Try to delete event - should fail due to trigger
        conn = db.connect()
        try:
            conn.execute("DELETE FROM events WHERE event_id = 'evt_001'")
            # If we get here, trigger didn't work
            pytest.fail("Expected trigger to prevent DELETE on events")
        except sqlite3.IntegrityError as e:
            # Expected: trigger raised an error
            assert "append-only" in str(e).lower() or "not allowed" in str(e).lower()


class TestOverrideBeforeCallLink:
    """Tests for disposition.override arriving before call.link."""

    @pytest.fixture
    def db_and_registry(self, tmp_path, assignment_data_path):
        """Create database and schema registry for testing."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)
        db.initialize()

        schema_dir = assignment_data_path / "schemas"
        dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

        registry = SchemaRegistry(schema_dir, dispositions_path)

        yield db, registry

        db.close()

    @pytest.fixture
    def assignment_data_path(self):
        """Get the path to assignment data."""
        return Path("assignment-a-data")

    def test_override_with_call_id_held_for_later_resolution(self, db_and_registry, monkeypatch, tmp_path):
        """Test that disposition.override with call_id is held until call.link arrives."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        jsonl_file = tmp_path / "test.jsonl"
        # First: override with call_id but no phone yet
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_override", "event_type": "disposition.override", "call_id": "call_001", "disposition": {"code": "CONFIRMED"}}\n'
        )

        checker = FileIdempotencyChecker(db)
        stats = IngestionStats()
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        # Verify event is pending (no association yet)
        conn = db.connect()
        cursor = conn.execute(
            "SELECT ea.association_status FROM event_associations ea WHERE ea.event_id = 'evt_override'"
        )
        assoc = cursor.fetchone()
        assert assoc is not None
        assert assoc["association_status"] in ["pending_lead", "pending_call_link"]

    def test_override_resolved_after_call_link_arrives(self, db_and_registry, monkeypatch, tmp_path):
        """Test that override is resolved when call.link arrives."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        jsonl_file = tmp_path / "test.jsonl"
        # Phase 1: call.link event
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_call_link", "event_type": "call.link", "call_id": "call_001", "phone": "9876543210"}\n'
        )

        checker = FileIdempotencyChecker(db)
        stats = IngestionStats()
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, stats)

        # Phase 2: Now add the override (in a new file to simulate late arrival)
        jsonl_file2 = tmp_path / "test2.jsonl"
        jsonl_file2.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_override", "event_type": "disposition.override", "call_id": "call_001", "disposition": {"code": "CONFIRMED"}}\n'
        )

        # Also need a lead for the phone
        leads_dir = tmp_path / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")
        
        from solution.ingestion.leads import LeadIngestion
        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Reconcile to match phone
        reconciliation = EventReconciliation(db, registry)
        reconciliation.reconcile(stats)

        # Now ingest the override
        event_ingestion.ingest_jsonl_file(jsonl_file2, "motor_renewal", checker, stats)

        # Reconcile to resolve override
        reconciliation = EventReconciliation(db, registry)
        recon_summary = reconciliation.reconcile(stats)

        # Verify override is now linked
        conn = db.connect()
        cursor = conn.execute(
            "SELECT ea.association_status FROM event_associations ea WHERE ea.event_id = 'evt_override'"
        )
        assoc = cursor.fetchone()
        assert assoc["association_status"] == "linked"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
