"""Tests for report generation and domain projection logic."""

import pytest
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

from solution.db import Database
from solution.schema_registry import SchemaRegistry
from solution.domain.calculations import DomainProjection, LeadFact, CallFact, DailyOverviewFact
from solution.reporting.writers import ReportWriter, generate_reports, mask_phone, format_ist_iso
from solution.ingestion.discovery import FileIdempotencyChecker
from solution.ingestion.leads import LeadIngestion, IngestionStats
from solution.ingestion.events import EventIngestion, EventReconciliation

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))


class TestDomainProjection:
    """Tests for domain projection logic."""
    
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


class TestCallFacts:
    """Tests for call fact derivation."""
    
    def test_initiated_required_for_call(self, db_and_registry, test_data, monkeypatch):
        """Test that a call requires a linked call.initiated event."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest a call.connected event without call.initiated
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_connected_001", "event_type": "call.connected", "call_id": "call_001", "phone": "9876543210"}\n')
        
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
        reconciliation.reconcile(IngestionStats())
        
        # Build projection
        projection = DomainProjection(db, registry)
        lead_facts, calls = projection.build_projection()
        
        # Should have no calls (no initiated event)
        assert len(calls) == 0
    
    def test_connected_logic(self, db_and_registry, test_data, monkeypatch):
        """Test connected flag is set when call.connected exists."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest call.initiated and call.connected
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_initiated_001", "event_type": "call.initiated", "phone": "9876543210", "call_id": "call_001"}\n'
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_connected_001", "event_type": "call.connected", "call_id": "call_001"}\n'
        )
        
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
        reconciliation.reconcile(IngestionStats())
        
        # Build projection
        projection = DomainProjection(db, registry)
        lead_facts, calls = projection.build_projection()
        
        # Should have 1 call, connected
        assert len(calls) == 1
        assert calls[0].connected is True
        assert calls[0].telephony_failed is False
    
    def test_telephony_failure(self, db_and_registry, test_data, monkeypatch):
        """Test telephony_failed flag is set when call.telephony_failed exists."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest call.initiated and call.telephony_failed
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_initiated_001", "event_type": "call.initiated", "phone": "9876543210", "call_id": "call_001"}\n'
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_telephony_failed_001", "event_type": "call.telephony_failed", "call_id": "call_001"}\n'
        )
        
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
        reconciliation.reconcile(IngestionStats())
        
        # Build projection
        projection = DomainProjection(db, registry)
        lead_facts, calls = projection.build_projection()
        
        # Should have 1 call with telephony_failed
        assert len(calls) == 1
        assert calls[0].telephony_failed is True
        assert calls[0].connected is False


class TestEffectiveDisposition:
    """Tests for effective disposition logic."""
    
    def test_latest_override_wins(self, db_and_registry, test_data, monkeypatch):
        """Test latest override takes precedence."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest with multiple dispositions - override after set
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_initiated_001", "event_type": "call.initiated", "phone": "9876543210", "call_id": "call_001"}\n'
            '{"event_id": "evt_set_001", "event_type": "disposition.set", "call_id": "call_001", "disposition_code": "NO_ANSWER", "occurred_at": "2026-11-20T12:00:00+05:30"}\n'
            '{"event_id": "evt_override_001", "event_type": "disposition.override", "call_id": "call_001", "disposition_code": "CONFIRMED", "occurred_at": "2026-11-20T13:00:00+05:30"}\n'
        )
        
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
        reconciliation.reconcile(IngestionStats())
        
        # Build projection
        projection = DomainProjection(db, registry)
        lead_facts, calls = projection.build_projection()
        
        # Should have CONFIRMED as effective disposition (override wins)
        assert len(calls) == 1
        assert calls[0].effective_disposition_code == "CONFIRMED"
    
    def test_override_precedence_over_later_set(self, db_and_registry, test_data, monkeypatch):
        """Test override takes precedence even over later set."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest with override BEFORE set (but override should still win)
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_initiated_001", "event_type": "call.initiated", "phone": "9876543210", "call_id": "call_001"}\n'
            '{"event_id": "evt_override_001", "event_type": "disposition.override", "call_id": "call_001", "disposition_code": "CONFIRMED", "occurred_at": "2026-11-20T12:00:00+05:30"}\n'
            '{"event_id": "evt_set_001", "event_type": "disposition.set", "call_id": "call_001", "disposition_code": "DECLINED", "occurred_at": "2026-11-20T13:00:00+05:30"}\n'
        )
        
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
        reconciliation.reconcile(IngestionStats())
        
        # Build projection
        projection = DomainProjection(db, registry)
        lead_facts, calls = projection.build_projection()
        
        # Should have CONFIRMED as effective disposition (override wins over later set)
        assert len(calls) == 1
        assert calls[0].effective_disposition_code == "CONFIRMED"


class TestLeadBuckets:
    """Tests for lead bucket assignment."""
    
    def test_yet_to_reach_out_bucket(self, db_and_registry, test_data, monkeypatch):
        """Test yet_to_reach_out bucket for leads with no calls."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest lead without any events
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")
        
        checker = FileIdempotencyChecker(db)
        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)
        
        # Build projection
        projection = DomainProjection(db, registry)
        lead_facts, calls = projection.build_projection()
        
        # Should have 1 lead with yet_to_reach_out bucket
        assert len(lead_facts) == 1
        assert lead_facts[0].bucket == "yet_to_reach_out"
        assert lead_facts[0].total_calls == 0
    
    def test_completed_bucket(self, db_and_registry, test_data, monkeypatch):
        """Test completed bucket for leads with terminal disposition."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest with terminal disposition (CONFIRMED is terminal)
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_initiated_001", "event_type": "call.initiated", "phone": "9876543210", "call_id": "call_001"}\n'
            '{"event_id": "evt_set_001", "event_type": "disposition.set", "call_id": "call_001", "disposition_code": "CONFIRMED", "occurred_at": "2026-11-20T12:00:00+05:30"}\n'
        )
        
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
        reconciliation.reconcile(IngestionStats())
        
        # Build projection
        projection = DomainProjection(db, registry)
        lead_facts, calls = projection.build_projection()
        
        # Should have completed bucket
        assert len(lead_facts) == 1
        assert lead_facts[0].bucket == "completed"
    
    def test_in_progress_telephony_failed_bucket(self, db_and_registry, test_data, monkeypatch):
        """Test in_progress_telephony_failed bucket when all calls failed."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest with telephony_failed (no disposition)
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_initiated_001", "event_type": "call.initiated", "phone": "9876543210", "call_id": "call_001"}\n'
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_telephony_failed_001", "event_type": "call.telephony_failed", "call_id": "call_001"}\n'
        )
        
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
        reconciliation.reconcile(IngestionStats())
        
        # Build projection
        projection = DomainProjection(db, registry)
        lead_facts, calls = projection.build_projection()
        
        # Should have in_progress_telephony_failed bucket
        assert len(lead_facts) == 1
        assert lead_facts[0].bucket == "in_progress_telephony_failed"
    
    def test_in_progress_dialed_bucket(self, db_and_registry, test_data, monkeypatch):
        """Test in_progress_dialed bucket for dialed but not completed leads."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest with call.initiated and call.connected but no disposition
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text(
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_initiated_001", "event_type": "call.initiated", "phone": "9876543210", "call_id": "call_001"}\n'
            '{"occurred_at": "2026-11-20T12:00:00+05:30", "event_id": "evt_connected_001", "event_type": "call.connected", "call_id": "call_001"}\n'
        )
        
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
        reconciliation.reconcile(IngestionStats())
        
        # Build projection
        projection = DomainProjection(db, registry)
        lead_facts, calls = projection.build_projection()
        
        # Should have in_progress_dialed bucket
        assert len(lead_facts) == 1
        assert lead_facts[0].bucket == "in_progress_dialed"


class TestReportWriters:
    """Tests for report file writers."""
    
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
    
    def test_batch_summary_headers(self, db_and_registry, test_data, monkeypatch, tmp_path):
        """Test batch_summary.csv has exact headers."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest lead
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")
        
        checker = FileIdempotencyChecker(db)
        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)
        
        # Generate reports
        output_dir = tmp_path / "reports"
        stats = {"lead_rows_total": 1, "lead_rows_rejected": 0, "rejections_by_reason": {}, 
                 "duplicate_lead_rows_merged": 0, "field_warnings": 0,
                 "events_total": 0, "malformed_events": 0, "duplicate_events_dropped": 0,
                 "orphan_events": 0, "unknown_disposition_events": 0}
        
        generate_reports(db, registry, output_dir, stats)
        
        # Check batch_summary.csv
        batch_summary_path = output_dir / "batch_summary.csv"
        content = batch_summary_path.read_text()
        
        assert "workflow_id,batch_id,total_leads,yet_to_reach_out,in_progress_telephony_failed,in_progress_dialed,completed" in content
    
    def test_daily_overview_headers(self, db_and_registry, test_data, monkeypatch, tmp_path):
        """Test daily_overview.csv has exact headers."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        events_dir = test_data / "events"
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest call.initiated and lead
        jsonl_file = events_dir / "motor_renewal.jsonl"
        jsonl_file.write_text('{"event_id": "evt_initiated_001", "event_type": "call.initiated", "phone": "9876543210", "call_id": "call_001", "occurred_at": "2026-11-20T12:00:00+05:30"}\n')
        
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")
        
        checker = FileIdempotencyChecker(db)
        
        event_ingestion = EventIngestion(db, registry)
        event_ingestion.ingest_jsonl_file(jsonl_file, "motor_renewal", checker, IngestionStats())
        
        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)
        
        # Reconcile
        reconciliation = EventReconciliation(db, registry)
        reconciliation.reconcile(IngestionStats())
        
        # Generate reports
        output_dir = tmp_path / "reports"
        stats = {"lead_rows_total": 1, "lead_rows_rejected": 0, "rejections_by_reason": {}, 
                 "duplicate_lead_rows_merged": 0, "field_warnings": 0,
                 "events_total": 1, "malformed_events": 0, "duplicate_events_dropped": 0,
                 "orphan_events": 0, "unknown_disposition_events": 0}
        
        generate_reports(db, registry, output_dir, stats)
        
        # Check daily_overview.csv
        daily_overview_path = output_dir / "daily_overview.csv"
        content = daily_overview_path.read_text()
        
        assert "workflow_id,date,total_calls,connected_calls,connectivity_rate,success_leads" in content
    
    def test_lead_report_headers(self, db_and_registry, test_data, monkeypatch, tmp_path):
        """Test lead_report.csv has exact headers."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest lead
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")
        
        checker = FileIdempotencyChecker(db)
        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)
        
        # Generate reports
        output_dir = tmp_path / "reports"
        stats = {"lead_rows_total": 1, "lead_rows_rejected": 0, "rejections_by_reason": {}, 
                 "duplicate_lead_rows_merged": 0, "field_warnings": 0,
                 "events_total": 0, "malformed_events": 0, "duplicate_events_dropped": 0,
                 "orphan_events": 0, "unknown_disposition_events": 0}
        
        generate_reports(db, registry, output_dir, stats)
        
        # Check lead_report.csv
        lead_report_path = output_dir / "lead_report.csv"
        content = lead_report_path.read_text()
        
        assert "workflow_id,external_ref,batch_id,phone_masked,bucket,total_calls,connected_calls,last_call_at,current_disposition,ever_success" in content
    
    def test_rejections_headers(self, db_and_registry, test_data, monkeypatch, tmp_path):
        """Test rejections.csv has exact headers."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        from solution.ingestion.leads import LeadIngestion
        
        leads_dir = test_data / "leads" / "motor_renewal"
        
        # Ingest lead with missing required field (policy_number missing)
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210\n")  # Missing policy_number
        
        checker = FileIdempotencyChecker(db)
        lead_ingestion = LeadIngestion(db, registry)
        lead_ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)
        
        # Generate reports
        output_dir = tmp_path / "reports"
        stats = {"lead_rows_total": 1, "lead_rows_rejected": 1, "rejections_by_reason": {"missing_required:policy_number": 1},
                 "duplicate_lead_rows_merged": 0, "field_warnings": 0,
                 "events_total": 0, "malformed_events": 0, "duplicate_events_dropped": 0,
                 "orphan_events": 0, "unknown_disposition_events": 0}
        
        generate_reports(db, registry, output_dir, stats)
        
        # Check rejections.csv
        rejections_path = output_dir / "rejections.csv"
        content = rejections_path.read_text()
        
        assert "workflow_id,batch_id,row_number,reason" in content
        assert "missing_required:policy_number" in content
    
    def test_data_quality_json_keys(self, db_and_registry, test_data, monkeypatch, tmp_path):
        """Test data_quality.json has exact keys."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")
        
        # Generate reports with empty state
        output_dir = tmp_path / "reports"
        stats = {"lead_rows_total": 0, "lead_rows_rejected": 0, "rejections_by_reason": {}, 
                 "duplicate_lead_rows_merged": 0, "field_warnings": 0,
                 "events_total": 0, "malformed_events": 0, "duplicate_events_dropped": 0,
                 "orphan_events": 0, "unknown_disposition_events": 0}
        
        generate_reports(db, registry, output_dir, stats)
        
        # Check data_quality.json
        data_quality_path = output_dir / "data_quality.json"
        content = json.loads(data_quality_path.read_text())
        
        expected_keys = {
            "lead_rows_total",
            "lead_rows_rejected", 
            "rejections_by_reason",
            "duplicate_lead_rows_merged",
            "field_warnings",
            "events_total",
            "malformed_events",
            "duplicate_events_dropped",
            "orphan_events",
            "unknown_disposition_events",
        }
        
        assert set(content.keys()) == expected_keys


class TestPhoneMasking:
    """Tests for phone masking."""
    
    def test_mask_phone_format(self):
        """Test phone masking format."""
        # Test with full phone
        assert mask_phone("", "+919876543210") == "+91******3210"
        assert mask_phone("", "+919876543210") == "+91******3210"
        
        # Test with different phone
        assert mask_phone("", "+919999999999") == "+91******9999"


class TestISTFormatting:
    """Tests for IST timestamp formatting."""
    
    def test_format_ist_iso(self):
        """Test IST ISO formatting."""
        # Create a UTC datetime
        utc_dt = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
        
        # Format to IST
        ist_str = format_ist_iso(utc_dt)
        
        # Should be 17:30 IST (UTC+5:30)
        assert "+05:30" in ist_str
        assert "2026-11-20T17:30:00" in ist_str


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
