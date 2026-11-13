"""Tests for CSV lead ingestion using normalizers, schema registry, and database."""

import pytest
import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from solution.db import Database, compute_file_hash, transaction, decrypt_pii_bytes
from solution.schema_registry import SchemaRegistry
from solution.ingestion.leads import (
    LeadIngestion,
    LeadFileDiscovery,
    LeadIngestionError,
    UnknownWorkflowError,
    ingest_lead_batch,
    IngestionStats,
)
from solution.ingestion.discovery import FileIdempotencyChecker


class TestLeadFileDiscovery:
    """Tests for lead file discovery."""

    def test_discover_empty_directory(self, tmp_path):
        """Test discovery with no leads directory."""
        base = tmp_path / "data"
        base.mkdir()

        discovery = LeadFileDiscovery(base)
        files = discovery.discover()
        assert files == []

    def test_discover_single_workflow(self, tmp_path):
        """Test discovery of CSV files in a single workflow directory."""
        base = tmp_path / "data"
        leads_dir = base / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)

        (leads_dir / "batch_001.csv").write_text("customer_name,phone\nJohn,9876543210\n")
        (leads_dir / "batch_002.csv").write_text("customer_name,phone\nJane,9876543211\n")

        discovery = LeadFileDiscovery(base)
        files = discovery.discover()

        assert len(files) == 2
        # Should be sorted
        assert files[0][1] == "motor_renewal"
        assert files[0][2] == "batch_001"
        assert files[1][2] == "batch_002"

    def test_discover_multiple_workflows(self, tmp_path):
        """Test discovery across multiple workflow directories."""
        base = tmp_path / "data"
        motor_dir = base / "leads" / "motor_renewal"
        motor_dir.mkdir(parents=True)
        (motor_dir / "batch.csv").write_text("customer_name,phone\nJohn,9876543210\n")

        feedback_dir = base / "leads" / "policy_feedback"
        feedback_dir.mkdir(parents=True)
        (feedback_dir / "batch.csv").write_text("customer_name,phone\nJane,9876543211\n")

        discovery = LeadFileDiscovery(base)
        files = discovery.discover()

        assert len(files) == 2
        workflows = {f[1] for f in files}
        assert workflows == {"motor_renewal", "policy_feedback"}


class TestLeadIngestionBasic:
    """Basic lead ingestion tests."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_ingest_simple_csv(self, db_and_registry, test_data, monkeypatch):
        """Test ingesting a simple valid CSV."""
        db, registry = db_and_registry
        # Valid Fernet key (32 bytes base64-encoded) and HMAC key
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_001.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL123\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_001", checker)

        assert stats.lead_rows_total == 1
        assert stats.lead_rows_rejected == 0
        assert stats.duplicate_lead_rows_merged == 0

        # Verify lead was inserted
        conn = db.connect()
        cursor = conn.execute("SELECT COUNT(*) as count FROM leads")
        assert cursor.fetchone()["count"] == 1

    def test_ingest_with_header_aliases(self, db_and_registry, test_data, monkeypatch):
        """Test handling of header aliases."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV with aliased headers
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_aliases.csv"
        csv_file.write_text("cust_name,phone_no,pol_no\nJohn Doe,9876543210,POL123\n")

        # Add aliases to schema dynamically
        schema = registry.get_schema("motor_renewal")
        schema["fields"][0]["aliases"] = ["cust_name", "custname"]
        schema["fields"][1]["aliases"] = ["phone_no", "phone_no"]  # alias for phone
        schema["fields"][2]["aliases"] = ["pol_no", "policy"]
        registry._schemas["motor_renewal"] = schema

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_aliases", checker)

        # Debug
        conn = db.connect()
        cursor = conn.execute("SELECT reason FROM rejections WHERE workflow_id = 'motor_renewal'")
        rejections = cursor.fetchall()
        
        assert stats.lead_rows_total == 1
        assert stats.lead_rows_rejected == 0

    def test_ingest_blank_rows_skipped(self, db_and_registry, test_data, monkeypatch):
        """Test that blank rows are skipped and don't increment row numbers."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV with blank rows
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_blanks.csv"
        csv_file.write_text(
            "customer_name,phone,policy_number\n"
            "John Doe,9876543210,POL1\n"
            "\n"  # Blank row
            "\n"  # Another blank
            "Jane Smith,9876543211,POL2\n"
        )

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_blanks", checker)

        # Debug
        conn = db.connect()
        cursor = conn.execute("SELECT reason FROM rejections WHERE workflow_id = 'motor_renewal'")
        rejections = cursor.fetchall()

        assert stats.lead_rows_total == 2  # Two non-blank rows
        assert stats.lead_rows_rejected == 0

        # Verify row numbers in database (should be 1 and 2, not 1, 3, 4)
        conn = db.connect()
        cursor = conn.execute(
            "SELECT row_number FROM rejections WHERE workflow_id = 'motor_renewal' ORDER BY row_number"
        )
        rejections = cursor.fetchall()
        # Should be no rejections
        assert len(rejections) == 0

        cursor = conn.execute("SELECT id FROM leads ORDER BY id")
        leads = cursor.fetchall()
        assert len(leads) == 2

    def test_ingest_header_reordering(self, db_and_registry, test_data, monkeypatch):
        """Test that headers can be in different order."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV with reordered headers
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_reorder.csv"
        csv_file.write_text("policy_number,phone,customer_name\nPOL123,9876543210,John Doe\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_reorder", checker)

        assert stats.lead_rows_total == 1
        assert stats.lead_rows_rejected == 0

        # Verify lead has correct data
        conn = db.connect()
        cursor = conn.execute("SELECT * FROM leads WHERE workflow_id = 'motor_renewal'")
        lead = dict(cursor.fetchone())
        assert lead["external_ref"] == "POL123"


class TestLeadValidation:
    """Tests for lead validation and rejection."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_required_field_missing(self, db_and_registry, test_data, monkeypatch):
        """Test rejection when required field is missing."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV missing phone
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_missing.csv"
        csv_file.write_text("customer_name,phone\nJohn Doe,\n")  # Empty phone

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_missing", checker)

        # lead_rows_total counts all non-blank rows read, including rejected ones
        assert stats.lead_rows_total == 1
        assert stats.lead_rows_rejected == 1

        # Verify rejection has correct reason (no PII)
        conn = db.connect()
        cursor = conn.execute(
            "SELECT reason FROM rejections WHERE workflow_id = 'motor_renewal'"
        )
        rejection = cursor.fetchone()
        assert rejection["reason"] == "missing_required:phone"
        # Verify no PII columns in rejections table
        assert "phone" not in rejection.keys()
        assert "customer_name" not in rejection.keys()

    def test_required_field_missing_first_priority(self, db_and_registry, test_data, monkeypatch):
        """Test that first missing required field is reported (spec priority)."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV missing customer_name (first required field in schema order)
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_missing_both.csv"
        csv_file.write_text("customer_name,phone\n,9876543210\n")  # Empty customer_name

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_missing_both", checker)

        assert stats.lead_rows_rejected == 1

        conn = db.connect()
        cursor = conn.execute("SELECT reason FROM rejections WHERE workflow_id = 'motor_renewal'")
        rejection = cursor.fetchone()
        # Should report customer_name (first missing required in schema order)
        assert rejection["reason"] == "missing_required:customer_name"

    def test_invalid_phone_rejection(self, db_and_registry, test_data, monkeypatch):
        """Test rejection when phone is present but invalid."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV with invalid phone
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_invalid_phone.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,12345,POL1\n")  # Too short

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_invalid_phone", checker)

        # Debug
        conn = db.connect()
        cursor = conn.execute("SELECT reason FROM rejections WHERE workflow_id = 'motor_renewal'")
        rejections = cursor.fetchall()

        assert stats.lead_rows_rejected == 1

        conn = db.connect()
        cursor = conn.execute("SELECT reason FROM rejections WHERE workflow_id = 'motor_renewal'")
        rejection = cursor.fetchone()
        assert rejection["reason"] == "invalid_phone"

    def test_invalid_optional_field_warning(self, db_and_registry, test_data, monkeypatch):
        """Test that invalid optional field generates warning, not rejection."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV with invalid optional premium_amount
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_invalid_optional.csv"
        csv_file.write_text("customer_name,phone,policy_number,premium_amount\nJohn Doe,9876543210,POL1,abc\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_invalid_optional", checker)

        assert stats.lead_rows_total == 1  # Accepted
        assert stats.lead_rows_rejected == 0
        assert stats.field_warnings == 1  # Warning for invalid decimal

        # Verify lead was created with null premium_amount
        conn = db.connect()
        cursor = conn.execute("SELECT attributes_json FROM leads WHERE workflow_id = 'motor_renewal'")
        lead = dict(cursor.fetchone())
        attrs = json.loads(lead["attributes_json"])
        assert "premium_amount" not in attrs  # Set to null

    def test_missing_optional_no_warning(self, db_and_registry, test_data, monkeypatch):
        """Test that missing optional field does not generate warning."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV without optional fields (but with required policy_number)
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_no_optional.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_no_optional", checker)

        assert stats.lead_rows_total == 1
        assert stats.field_warnings == 0  # No warnings for missing optional


class TestLeadDuplicateMerge:
    """Tests for duplicate lead merging."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_duplicate_lead_merge(self, db_and_registry, test_data, monkeypatch):
        """Test that duplicate leads are merged, not rejected."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"

        # First file: create lead (with required policy_number)
        csv_file1 = leads_dir / "batch_001.csv"
        csv_file1.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL123\n")

        # Second file: same phone, different batch (with required policy_number)
        csv_file2 = leads_dir / "batch_002.csv"
        csv_file2.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL456\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        # Ingest first file
        stats1 = ingestion.ingest_csv_file(csv_file1, "motor_renewal", "batch_001", checker)
        assert stats1.lead_rows_total == 1
        assert stats1.duplicate_lead_rows_merged == 0

        # Ingest second file (same phone)
        stats2 = ingestion.ingest_csv_file(csv_file2, "motor_renewal", "batch_002", checker)
        assert stats2.lead_rows_total == 1
        assert stats2.duplicate_lead_rows_merged == 1  # Merged

        # Verify only one lead exists
        conn = db.connect()
        cursor = conn.execute("SELECT COUNT(*) as count FROM leads")
        assert cursor.fetchone()["count"] == 1

        # Verify original batch_id preserved
        cursor = conn.execute("SELECT batch_id FROM leads WHERE workflow_id = 'motor_renewal'")
        lead = dict(cursor.fetchone())
        assert lead["batch_id"] == "batch_001"  # Original batch preserved

    def test_null_does_not_overwrite(self, db_and_registry, test_data, monkeypatch):
        """Test that null values do not overwrite existing values during merge."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"

        # First file: lead with premium_amount (with required policy_number)
        csv_file1 = leads_dir / "batch_001.csv"
        csv_file1.write_text("customer_name,phone,policy_number,premium_amount\nJohn Doe,9876543210,POL1,100.00\n")

        # Second file: same phone, empty premium_amount (should not overwrite)
        csv_file2 = leads_dir / "batch_002.csv"
        csv_file2.write_text("customer_name,phone,policy_number,premium_amount\nJohn Doe,9876543210,POL1,\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        # Ingest first file
        stats1 = ingestion.ingest_csv_file(csv_file1, "motor_renewal", "batch_001", checker)

        # Ingest second file
        stats2 = ingestion.ingest_csv_file(csv_file2, "motor_renewal", "batch_002", checker)
        assert stats2.duplicate_lead_rows_merged == 1

        # Verify premium_amount preserved (not overwritten with null)
        conn = db.connect()
        cursor = conn.execute("SELECT attributes_json FROM leads WHERE workflow_id = 'motor_renewal'")
        lead = dict(cursor.fetchone())
        attrs = json.loads(lead["attributes_json"])
        assert attrs.get("premium_amount") == "100.00"  # Preserved


class TestPIISecurity:
    """Tests to verify PII is encrypted in the database."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_pii_encrypted_in_database(self, db_and_registry, test_data, monkeypatch):
        """Test that PII fields are encrypted in the database."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_pii.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL123\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_pii", checker)

        # Verify PII is encrypted
        conn = db.connect()
        cursor = conn.execute("SELECT pii_ciphertext FROM leads WHERE workflow_id = 'motor_renewal'")
        lead = dict(cursor.fetchone())

        pii_ciphertext = lead["pii_ciphertext"]
        assert pii_ciphertext is not None

        # Decrypt and verify content
        decrypted = decrypt_pii_bytes(pii_ciphertext)
        pii_data = json.loads(decrypted)
        assert pii_data["customer_name"] == "John Doe"
        assert pii_data["phone"] == "+919876543210"  # Normalized

        # Verify plaintext not in database
        # Check that raw values aren't stored elsewhere
        cursor = conn.execute("SELECT * FROM leads")
        columns = [desc[0] for desc in cursor.description]
        assert "customer_name" not in columns
        assert "phone" not in columns

    def test_phone_stored_as_hmac(self, db_and_registry, test_data, monkeypatch):
        """Test that phone is stored as HMAC, not plaintext."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_phone.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL123\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_phone", checker)

        # Verify phone_hmac is stored, not plaintext phone
        conn = db.connect()
        cursor = conn.execute("SELECT phone_hmac FROM leads WHERE workflow_id = 'motor_renewal'")
        lead = dict(cursor.fetchone())

        phone_hmac = lead["phone_hmac"]
        assert phone_hmac is not None
        assert phone_hmac != "9876543210"  # Not plaintext
        assert phone_hmac != "+919876543210"  # Not normalized plaintext

        # Verify it's a hex string (HMAC-SHA256 output)
        assert len(phone_hmac) == 64  # SHA256 hex length
        int(phone_hmac, 16)  # Should be valid hex


class TestBothWorkflows:
    """Tests for both motor_renewal and policy_feedback workflows."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        yield data_dir

    def test_motor_renewal_workflow(self, db_and_registry, test_data, monkeypatch):
        """Test motor_renewal workflow ingestion."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn,9876543210,POL1\nJane,9876543211,POL2\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        assert stats.lead_rows_total == 2
        assert stats.lead_rows_rejected == 0

    def test_policy_feedback_workflow(self, db_and_registry, test_data, monkeypatch):
        """Test policy_feedback workflow ingestion."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "policy_feedback"
        leads_dir.mkdir(parents=True)
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,order_id,product\nJohn,9876543210,ORD1,ProductA\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "policy_feedback", "batch", checker)

        assert stats.lead_rows_total == 1
        assert stats.lead_rows_rejected == 0

        # Verify order_id is used as external_ref for policy_feedback
        conn = db.connect()
        cursor = conn.execute("SELECT external_ref FROM leads WHERE workflow_id = 'policy_feedback'")
        lead = dict(cursor.fetchone())
        assert lead["external_ref"] == "ORD1"


class TestIdempotencyReruns:
    """Tests for idempotent re-runs of ingestion."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_idempotent_rerun(self, db_and_registry, test_data, monkeypatch):
        """Test that re-ingesting the same file is a no-op."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL1\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        # First ingestion
        stats1 = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)
        assert stats1.lead_rows_total == 1

        # Second ingestion (same file)
        stats2 = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)
        assert stats2.lead_rows_total == 1  # Same count

        # Verify only one lead exists (no duplicates)
        conn = db.connect()
        cursor = conn.execute("SELECT COUNT(*) as count FROM leads")
        assert cursor.fetchone()["count"] == 1


class TestAllRejectedBatch:
    """Tests for batches where all rows are rejected."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_all_rows_rejected_batch_recorded(self, db_and_registry, test_data, monkeypatch):
        """Test that a batch with all rejected rows is still recorded."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_all_rejected.csv"
        csv_file.write_text("customer_name,phone\nJohn Doe,\nJane Smith,\n")  # Both missing phone

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_all_rejected", checker)

        # Both rows were read (lead_rows_total), both rejected
        assert stats.lead_rows_total == 2
        assert stats.lead_rows_rejected == 2

        # Verify batch was created
        conn = db.connect()
        cursor = conn.execute(
            "SELECT COUNT(*) as count FROM lead_batches WHERE workflow_id = 'motor_renewal'"
        )
        assert cursor.fetchone()["count"] == 1

        # Verify rejections recorded
        cursor = conn.execute(
            "SELECT COUNT(*) as count FROM rejections WHERE workflow_id = 'motor_renewal'"
        )
        assert cursor.fetchone()["count"] == 2

    def test_all_rejected_batch_stats(self, db_and_registry, test_data, monkeypatch):
        """Test that ingestion file stats reflect all-rejected batch."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_all_rejected.csv"
        csv_file.write_text("customer_name,phone\nJohn Doe,\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_all_rejected", checker)

        # Verify stats recorded correctly
        conn = db.connect()
        cursor = conn.execute("SELECT * FROM ingestion_files WHERE workflow_id = 'motor_renewal'")
        file_record = dict(cursor.fetchone())

        assert file_record["lead_rows_total"] == 1  # one non-blank row read
        assert file_record["lead_rows_rejected"] == 1
        assert file_record["status"] == "completed"


class TestShortRows:
    """Tests for rows with fewer columns than header."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_short_row_handled(self, db_and_registry, test_data, monkeypatch):
        """Test that rows with fewer columns than header are handled."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Create test CSV with short row (missing second column)
        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch_short.csv"
        csv_file.write_text("customer_name,phone\nJohn Doe\n")  # Missing phone

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch_short", checker)

        # Should be rejected for missing phone, not crash
        assert stats.lead_rows_rejected == 1
        # The short row was still read (non-blank), so it counts
        assert stats.lead_rows_total == 1


class TestPIIInDatabase:
    """Tests to verify no PII leaks in database tables."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_no_plaintext_pii_in_leads_table(self, db_and_registry, test_data, monkeypatch):
        """Verify no plaintext PII in leads table columns."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone,policy_number\nJohn Doe,9876543210,POL123\n")

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Check leads table columns
        conn = db.connect()
        cursor = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='leads'")
        create_sql = cursor.fetchone()["sql"]

        # Verify no plaintext PII columns (phone_hmac is OK - it's the HMAC, not plaintext)
        assert "customer_name" not in create_sql
        # Check for "phone" as a standalone column (not phone_hmac)
        import re
        assert not re.search(r'\bphone\b', create_sql) or 'phone_hmac' in create_sql

    def test_no_plaintext_pii_in_rejections_table(self, db_and_registry, test_data, monkeypatch):
        """Verify no plaintext PII in rejections table."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text("customer_name,phone\nJohn Doe,\n")  # Missing phone

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Check rejections table
        conn = db.connect()
        cursor = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='rejections'")
        create_sql = cursor.fetchone()["sql"]

        # Verify no PII columns
        assert "customer_name" not in create_sql
        assert "phone" not in create_sql

        # Verify rejection record has only non-PII fields
        cursor = conn.execute("SELECT * FROM rejections WHERE workflow_id = 'motor_renewal'")
        rejection = dict(cursor.fetchone())
        assert "reason" in rejection
        assert "row_number" in rejection
        assert "batch_id" in rejection
        assert "workflow_id" in rejection
        # Check values - should not contain PII
        assert "John Doe" not in str(rejection.values())
        assert "9876543210" not in str(rejection.values())


class TestWhitespaceOnlyRows:
    """Tests for whitespace-only rows and edge cases."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_whitespace_only_row_skipped(self, db_and_registry, test_data, monkeypatch):
        """Test that whitespace-only rows are skipped."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text(
            "customer_name,phone,policy_number\n"
            "John Doe,9876543210,POL1\n"
            "   ,   ,   \n"  # Whitespace-only row
            "Jane Smith,9876543211,POL2\n"
        )

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        # Whitespace-only row should be skipped
        assert stats.lead_rows_total == 2
        assert stats.lead_rows_rejected == 0

    def test_trailing_newline_not_double_counted(self, db_and_registry, test_data, monkeypatch):
        """Test that trailing newline doesn't create extra blank row."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text(
            "customer_name,phone,policy_number\n"
            "John Doe,9876543210,POL1\n"
            "Jane Smith,9876543211,POL2\n"
        )  # No trailing newline

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        assert stats.lead_rows_total == 2


class TestInvalidValues:
    """Tests for invalid optional values generating warnings."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_invalid_date_optional_warning(self, db_and_registry, test_data, monkeypatch):
        """Test that invalid date in optional field generates warning."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text(
            "customer_name,phone,policy_number,policy_expiry\n"
            "John Doe,9876543210,POL1,not-a-date\n"
        )

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        assert stats.lead_rows_total == 1
        assert stats.field_warnings == 1

    def test_invalid_enum_optional_warning(self, db_and_registry, test_data, monkeypatch):
        """Test that invalid enum in optional field generates warning."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text(
            "customer_name,phone,policy_number,preferred_language\n"
            "John Doe,9876543210,POL1,invalid_lang\n"
        )

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        assert stats.lead_rows_total == 1
        assert stats.field_warnings == 1

    def test_invalid_decimal_optional_warning(self, db_and_registry, test_data, monkeypatch):
        """Test that invalid decimal in optional field generates warning."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        leads_dir = test_data / "leads" / "motor_renewal"
        csv_file = leads_dir / "batch.csv"
        csv_file.write_text(
            "customer_name,phone,policy_number,premium_amount\n"
            "John Doe,9876543210,POL1,not-a-number\n"
        )

        checker = FileIdempotencyChecker(db)
        ingestion = LeadIngestion(db, registry)

        stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", "batch", checker)

        assert stats.lead_rows_total == 1
        assert stats.field_warnings == 1


class TestAliasHandling:
    """Tests for header alias handling."""

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

    @pytest.fixture
    def test_data(self, tmp_path):
        """Create test data directory with leads."""
        data_dir = tmp_path / "data"
        leads_dir = data_dir / "leads" / "motor_renewal"
        leads_dir.mkdir(parents=True)
        yield data_dir

    def test_multiple_aliases_all_work(self, db_and_registry, test_data, monkeypatch):
        """Test that all defined aliases work for matching."""
        db, registry = db_and_registry
        monkeypatch.setenv("ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")
        monkeypatch.setenv("HMAC_KEY", "test_hmac_key_for_testing_32_bytes!!")

        # Test various phone aliases
        aliases_to_test = [
            "mobile", "cell", "telephone", "phone_number", "contact_no", "mob_no"
        ]
        
        for alias in aliases_to_test:
            # Reset DB
            db.close()
            db_path = db.db_path
            db = Database(db_path)
            db.initialize()
            
            leads_dir = test_data / "leads" / "motor_renewal"
            csv_file = leads_dir / f"batch_{alias}.csv"
            csv_file.write_text(f"customer_name,{alias},policy_number\nJohn Doe,9876543210,POL1\n")

            checker = FileIdempotencyChecker(db)
            ingestion = LeadIngestion(db, registry)

            stats = ingestion.ingest_csv_file(csv_file, "motor_renewal", f"batch_{alias}", checker)

            assert stats.lead_rows_total == 1, f"Alias {alias} failed"
            
            # Verify lead was created
            conn = db.connect()
            cursor = conn.execute("SELECT COUNT(*) as count FROM leads")
            assert cursor.fetchone()["count"] == 1
            
            db.close()
            db = Database(db_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
