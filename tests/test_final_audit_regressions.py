"""Regression tests for defects found and fixed during the final release audit.

Each test maps to a specific verified defect:
- P0-A: disposition_code (generator contract format) was never extracted.
- P0-B: per-file event counters lost trailing malformed/duplicate lines.
- P1-C: field_warnings were counted for rejected rows.
- P1-D: lead_rows_total only counted accepted rows instead of rows read.
- P1-E: batches with zero accepted leads were missing from batch_summary.csv.
- P2-G: manifest rows left 'pending' by a crash were permanently skipped.
"""

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from solution.db import Database, get_aggregated_stats
from solution.schema_registry import SchemaRegistry
from solution.ingestion.discovery import FileIdempotencyChecker
from solution.ingestion.leads import LeadIngestion, IngestionStats
from solution.ingestion.normalization import normalize_decimal
from solution.ingestion.events import EventIngestion, EventReconciliation
from solution.reporting.writers import generate_reports


TEST_KEYS = {
    "ENCRYPTION_KEY": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
    "HMAC_KEY": "test_hmac_key_for_testing_32_bytes!!",
}


@pytest.fixture
def env_keys(monkeypatch):
    for key, value in TEST_KEYS.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def db_and_registry(tmp_path, assignment_data_path, env_keys):
    db = Database(tmp_path / "test.db")
    db.initialize()
    schema_dir = assignment_data_path / "schemas"
    dispositions_path = schema_dir / "dispositions.csv"
    registry = SchemaRegistry(schema_dir, dispositions_path)
    yield db, registry
    db.close()


@pytest.fixture
def test_data(tmp_path):
    data_dir = tmp_path / "data"
    events_dir = data_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    leads_dir = data_dir / "leads" / "motor_renewal"
    leads_dir.mkdir(parents=True, exist_ok=True)
    yield data_dir


def _ingest_lead(db, registry, csv_file, workflow="motor_renewal", batch="batch"):
    checker = FileIdempotencyChecker(db)
    ingestion = LeadIngestion(db, registry)
    return ingestion.ingest_csv_file(csv_file, workflow, batch, checker)


def _make_discovered(csv_file, workflow, batch=None, kind="leads"):
    from solution.ingestion.discovery import DiscoveredFile, compute_file_hash

    return DiscoveredFile(
        source_path=csv_file,
        canonical_path=csv_file.resolve(),
        content_hash=compute_file_hash(csv_file.resolve()),
        file_kind=kind,
        workflow_id=workflow,
        batch_id=batch,
        relative_path=str(csv_file),
    )


def _ingest_events(db, registry, jsonl_file, workflow="motor_renewal"):
    """Ingest an event file the same way the CLI does: record the manifest
    row first, then process with the file id."""
    checker = FileIdempotencyChecker(db)
    is_noop, file_id = checker.check_and_record_file(
        _make_discovered(jsonl_file, workflow, None, kind="events")
    )
    assert is_noop is False

    event_ingestion = EventIngestion(db, registry)
    stats = IngestionStats()
    event_ingestion.ingest_jsonl_file(jsonl_file, workflow, checker, stats, file_id=file_id)
    return stats


class TestDispositionCodeContract:
    """P0-A: events carry a flat `disposition_code` string (generator format)."""

    def test_disposition_code_stored(self, db_and_registry, test_data):
        db, registry = db_and_registry
        jsonl = test_data / "events" / "motor_renewal.jsonl"
        jsonl.write_text(
            '{"event_id": "evt_1", "event_type": "disposition.set", '
            '"call_id": "call_1", "phone": "9876543210", '
            '"disposition_code": "CONFIRMED", "occurred_at": "2026-11-20T12:00:00+05:30"}\n'
        )
        _ingest_events(db, registry, jsonl)

        conn = db.connect()
        row = conn.execute(
            "SELECT disposition_code FROM events WHERE event_id = 'evt_1'"
        ).fetchone()
        assert row["disposition_code"] == "CONFIRMED"

    def test_disposition_drives_reports(self, db_and_registry, test_data, tmp_path):
        """Effective disposition must reach bucket/success/current_disposition."""
        db, registry = db_and_registry
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"

        jsonl = events_dir / "motor_renewal.jsonl"
        jsonl.write_text(
            '{"event_id": "evt_init", "event_type": "call.initiated", '
            '"call_id": "call_1", "phone": "9876543210", '
            '"occurred_at": "2026-11-20T11:00:00+05:30"}\n'
            '{"event_id": "evt_set", "event_type": "disposition.set", '
            '"call_id": "call_1", "phone": "9876543210", '
            '"disposition_code": "CONFIRMED", "occurred_at": "2026-11-20T12:00:00+05:30"}\n'
        )
        _ingest_events(db, registry, jsonl)

        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")
        _ingest_lead(db, registry, csv_file)

        EventReconciliation(db, registry).reconcile(IngestionStats())

        out_dir = tmp_path / "reports"
        conn = db.connect()
        generate_reports(db, registry, out_dir, get_aggregated_stats(db, conn))

        lead_report = (out_dir / "lead_report.csv").read_text(encoding="utf-8")
        assert "completed" in lead_report
        assert "CONFIRMED" in lead_report
        assert ",true" in lead_report  # ever_success

        batch_summary = (out_dir / "batch_summary.csv").read_text(encoding="utf-8")
        batch_summary_lines = batch_summary.strip().splitlines()
        assert batch_summary_lines[1].endswith(",1,0,0,0,1")  # 1 completed lead

        daily = (out_dir / "daily_overview.csv").read_text(encoding="utf-8")
        daily_lines = daily.strip().splitlines()
        assert daily_lines[1].endswith(",1")  # success_leads = 1

    def test_unknown_disposition_counted_for_policy_feedback(
        self, db_and_registry, test_data
    ):
        """The catalogue only covers motor_renewal; policy_feedback
        disposition events must count as unknown once linked."""
        db, registry = db_and_registry
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "policy_feedback"
        leads_dir.mkdir(parents=True, exist_ok=True)

        jsonl = events_dir / "policy_feedback.jsonl"
        jsonl.write_text(
            '{"event_id": "evt_pf", "event_type": "disposition.set", '
            '"call_id": "call_pf", "phone": "9876543210", '
            '"disposition_code": "CONFIRMED", "occurred_at": "2026-11-20T12:00:00+05:30"}\n'
        )
        _ingest_events(db, registry, jsonl, workflow="policy_feedback")

        csv_file = leads_dir / "batch_pf.csv"
        csv_file.write_text(
            "customer_name,phone,order_id\nJane Roe,9876543210,ORD1\n"
        )
        _ingest_lead(
            db, registry, csv_file, workflow="policy_feedback", batch="batch_pf"
        )

        summary = EventReconciliation(db, registry).reconcile(IngestionStats())
        conn = db.connect()
        stats = get_aggregated_stats(db, conn)
        assert stats["unknown_disposition_events"] == 1


class TestEventFileCounters:
    """P0-B: all non-empty lines must be counted even if the file ends
    with malformed/duplicate lines."""

    def test_trailing_malformed_and_duplicate_lines_counted(
        self, db_and_registry, test_data
    ):
        db, registry = db_and_registry
        jsonl = test_data / "events" / "motor_renewal.jsonl"
        jsonl.write_text(
            '{"event_id": "evt_1", "event_type": "call.initiated", '
            '"call_id": "c1", "phone": "9876543210", '
            '"occurred_at": "2026-11-20T12:00:00+05:30"}\n'
            '{"event_id": "evt_1", "event_type": "call.initiated", '
            '"call_id": "c1", "phone": "9876543210", '
            '"occurred_at": "2026-11-20T12:00:00+05:30"}\n'  # duplicate
            "not json at all\n"  # malformed
            '{"event_id": "", "event_type": "call.initiated"}\n'  # blank id
        )
        stats = _ingest_events(db, registry, jsonl)

        assert stats.events_total == 4
        assert stats.malformed_events == 2
        assert stats.duplicate_events_dropped == 1

        # Persisted per-file stats must match
        conn = db.connect()
        row = conn.execute(
            "SELECT events_total, malformed_events, duplicate_events_dropped "
            "FROM ingestion_files WHERE file_kind = 'events'"
        ).fetchone()
        assert row["events_total"] == 4
        assert row["malformed_events"] == 2
        assert row["duplicate_events_dropped"] == 1

    def test_file_with_only_malformed_lines(self, db_and_registry, test_data):
        db, registry = db_and_registry
        jsonl = test_data / "events" / "motor_renewal.jsonl"
        jsonl.write_text("not json\nstill not json\n")
        stats = _ingest_events(db, registry, jsonl)

        assert stats.events_total == 2
        assert stats.malformed_events == 2

        conn = db.connect()
        row = conn.execute(
            "SELECT events_total, malformed_events, status FROM ingestion_files "
            "WHERE file_kind = 'events'"
        ).fetchone()
        assert row["events_total"] == 2
        assert row["malformed_events"] == 2
        assert row["status"] == "completed"


class TestLeadRowStatistics:
    """P1-C/P1-D: lead_rows_total counts rows read; rejected rows add no
    field warnings."""

    def test_rejected_row_with_invalid_optional_no_warning(
        self, db_and_registry, test_data
    ):
        db, registry = db_and_registry
        csv_file = test_data / "leads" / "motor_renewal" / "batch.csv"
        # Missing required customer_name AND invalid optional premium_amount
        csv_file.write_text(
            "customer_name,phone,policy_number,premium_amount\n"
            ",9876543210,POL1,not-a-number\n"
        )
        stats = _ingest_lead(db, registry, csv_file, batch="batch")

        assert stats.lead_rows_rejected == 1
        assert stats.field_warnings == 0  # rejected rows add no warnings
        assert stats.lead_rows_total == 1  # row was read

    def test_mixed_file_statistics(self, db_and_registry, test_data):
        db, registry = db_and_registry
        csv_file = test_data / "leads" / "motor_renewal" / "batch.csv"
        csv_file.write_text(
            "customer_name,phone,policy_number,premium_amount\n"
            "John Doe,9876543210,POL1,bad\n"  # accepted, 1 warning
            "\n"  # blank, skipped
            "Jane Roe,,POL2\n"  # rejected, 0 warnings
        )
        stats = _ingest_lead(db, registry, csv_file, batch="batch")

        assert stats.lead_rows_total == 2  # non-blank rows read
        assert stats.lead_rows_rejected == 1
        assert stats.field_warnings == 1
        assert stats.duplicate_lead_rows_merged == 0


class TestZeroLeadBatchInSummary:
    """P1-E: ingested batches with zero accepted leads appear in
    batch_summary.csv with total_leads=0."""

    def test_all_rejected_batch_listed(self, db_and_registry, test_data, tmp_path):
        db, registry = db_and_registry
        csv_file = test_data / "leads" / "motor_renewal" / "batch_empty.csv"
        csv_file.write_text("customer_name,phone\nJohn Doe,\n")
        _ingest_lead(db, registry, csv_file, batch="batch_empty")

        conn = db.connect()
        generate_reports(db, registry, tmp_path / "reports", get_aggregated_stats(db, conn))

        content = (tmp_path / "reports" / "batch_summary.csv").read_text(encoding="utf-8")
        lines = content.strip().splitlines()
        assert len(lines) == 2  # header + one batch row
        assert lines[1] == "motor_renewal,batch_empty,0,0,0,0,0"


class TestCrashRecovery:
    """P2-G: a manifest row left 'pending' by a crash is reprocessed."""

    def test_pending_file_reprocessed(self, db_and_registry, test_data):
        db, registry = db_and_registry
        csv_file = test_data / "leads" / "motor_renewal" / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        checker = FileIdempotencyChecker(db)
        is_noop, file_id = checker.check_and_record_file(
            _make_discovered(csv_file, "motor_renewal", "batch")
        )
        assert is_noop is False

        # Simulate crash: manifest row exists but data never processed
        conn = db.connect()
        status = conn.execute(
            "SELECT status FROM ingestion_files WHERE id = ?", (file_id,)
        ).fetchone()["status"]
        assert status == "pending"

        # Restart: ingestion must process the file, not skip it
        stats = _ingest_lead(db, registry, csv_file, batch="batch")
        assert stats.lead_rows_total == 1

        conn = db.connect()
        row = conn.execute(
            "SELECT status, lead_rows_total FROM ingestion_files WHERE id = ?",
            (file_id,),
        ).fetchone()
        assert row["status"] == "completed"
        assert row["lead_rows_total"] == 1

        leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        assert leads == 1


class TestInvalidTimestampHandling:
    """An event with an unparseable timestamp is malformed; it must never
    abort the whole file (previously the entire transaction was lost)."""

    def test_invalid_timestamp_does_not_lose_valid_events(
        self, db_and_registry, test_data
    ):
        db, registry = db_and_registry
        jsonl = test_data / "events" / "motor_renewal.jsonl"
        jsonl.write_text(
            '{"event_id": "evt_ok", "event_type": "call.initiated", '
            '"call_id": "c1", "phone": "9876543210", '
            '"occurred_at": "2026-11-20T12:00:00Z"}\n'
            '{"event_id": "evt_badts", "event_type": "call.initiated", '
            '"call_id": "c2", "phone": "9876543211", '
            '"occurred_at": "definitely-not-a-date"}\n'
        )
        stats = _ingest_events(db, registry, jsonl)

        assert stats.events_total == 2
        assert stats.malformed_events == 1

        conn = db.connect()
        stored = conn.execute(
            "SELECT event_id FROM events"
        ).fetchall()
        assert [r["event_id"] for r in stored] == ["evt_ok"]

    def test_missing_timestamp_is_malformed(self, db_and_registry, test_data):
        db, registry = db_and_registry
        jsonl = test_data / "events" / "motor_renewal.jsonl"
        jsonl.write_text(
            '{"event_id": "evt_nots", "event_type": "call.initiated", '
            '"call_id": "c1", "phone": "9876543210"}\n'
        )
        stats = _ingest_events(db, registry, jsonl)
        assert stats.malformed_events == 1
        assert stats.events_total == 1


class TestNoPlaintextPIIInDatabaseBytes:
    """Security: scan the raw database file for known plaintext PII."""

    def test_db_bytes_free_of_plaintext_pii(self, db_and_registry, test_data):
        db, registry = db_and_registry
        csv_file = test_data / "leads" / "motor_renewal" / "batch.csv"
        csv_file.write_text(
            "customer_name,phone,policy_number\n"
            "Zzyzx Strangename,9876543210,POL1\n"
        )
        _ingest_lead(db, registry, csv_file, batch="batch")

        db_path = db.db_path
        db.close()

        raw = db_path.read_bytes()
        assert b"Zzyzx Strangename" not in raw
        assert b"9876543210" not in raw
        assert b"+919876543210" not in raw

        # Reopen so fixture teardown can close cleanly
        db2 = Database(db_path)
        db2.connect()
        db2.close()


class TestDecimalGroupingValidation:
    """P2-H: malformed comma grouping was silently collapsed into a valid value.

    normalize_decimal previously stripped all commas, so "1,2,3" became 123.
    Grouping must now be valid Western (1,234,567) or Indian (1,23,456) style.
    """

    def test_malformed_grouping_rejected(self):
        assert normalize_decimal("1,2,3") is None
        assert normalize_decimal("12,34") is None
        assert normalize_decimal("1,234,56") is None
        assert normalize_decimal("1,2,3.45") is None

    def test_valid_grouping_still_accepted(self):
        assert normalize_decimal("1,234.56") == Decimal("1234.56")
        assert normalize_decimal("1,234,567.89") == Decimal("1234567.89")
        assert normalize_decimal("1,23,456.78") == Decimal("123456.78")
        assert normalize_decimal("123.45") == Decimal("123.45")


class TestUnknownDispositionSnapshot:
    """P2-I: the CLI reported the final reconcile's delta (always 0 when all
    unknowns were marked during per-file reconciles) instead of the live
    count, showing "Unknown dispositions: 0" while data_quality.json said 6."""

    def test_snapshot_is_live_not_delta(self, db_and_registry, test_data):
        db, registry = db_and_registry
        csv_file = test_data / "leads" / "motor_renewal" / "batch.csv"
        csv_file.write_text(
            "customer_name,phone,policy_number\n"
            "Test Person,9876543210,POL1\n"
        )
        _ingest_lead(db, registry, csv_file, batch="batch")

        jsonl = test_data / "events" / "motor_renewal.jsonl"
        jsonl.write_text(
            '{"event_id": "evt_1", "event_type": "disposition.set", '
            '"call_id": "call_1", "phone": "9876543210", '
            '"disposition_code": "TOTALLY_UNKNOWN_CODE", '
            '"occurred_at": "2026-11-20T12:00:00+05:30"}\n'
        )
        _ingest_events(db, registry, jsonl)

        reconciliation = EventReconciliation(db, registry)
        first = reconciliation.reconcile(IngestionStats())
        assert first["unknown_dispositions"] == 1

        # A second reconcile marks nothing new (delta 0), but the live
        # snapshot must still report the existing ignored associations
        second = reconciliation.reconcile(IngestionStats())
        assert second["unknown_dispositions"] == 0
        assert second["unknown_dispositions_current"] == 1
