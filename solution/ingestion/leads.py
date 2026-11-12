"""CSV lead ingestion using Phase 1 normalizers and Phase 2 database/repositories.

Discovers CSV files under leads/<workflow_id>/, validates rows against schema,
handles aliases/duplicates, and persists accepted leads and rejections.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field

from solution.db import (
    Database,
    transaction,
    insert_or_merge_lead,
    insert_rejection,
    get_or_create_batch,
    record_ingestion_file,
    update_ingestion_file_stats,
    decrypt_pii_bytes,
)
from solution.ingestion.discovery import FileIdempotencyChecker, DiscoveredFile, compute_file_hash
from solution.schema_registry import SchemaRegistry
from solution.ingestion.normalization import (
    normalize_header,
    normalize_phone,
    parse_date,
    normalize_decimal,
    normalize_enum,
)
from solution.security.pii import (
    encrypt_pii,
    compute_phone_hmac,
)
from solution.config import ConfigurationError


@dataclass
class IngestionStats:
    """Statistics for a single file ingestion."""
    events_total: int = 0
    malformed_events: int = 0
    duplicate_events_dropped: int = 0
    lead_rows_total: int = 0
    lead_rows_rejected: int = 0
    duplicate_lead_rows_merged: int = 0
    field_warnings: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "events_total": self.events_total,
            "malformed_events": self.malformed_events,
            "duplicate_events_dropped": self.duplicate_events_dropped,
            "lead_rows_total": self.lead_rows_total,
            "lead_rows_rejected": self.lead_rows_rejected,
            "duplicate_lead_rows_merged": self.duplicate_lead_rows_merged,
            "field_warnings": self.field_warnings,
        }


@dataclass
class LeadRow:
    """A parsed and validated lead row."""
    row_number: int  # Logical row number (nonblank rows from 1)
    raw_values: Dict[str, str]  # Original raw values by normalized header
    normalized_values: Dict[str, Any] = field(default_factory=dict)
    is_rejected: bool = False
    rejection_reason: Optional[str] = None
    field_warnings_count: int = 0


class LeadIngestionError(Exception):
    """Raised when lead ingestion encounters an unrecoverable error."""
    pass


class UnknownWorkflowError(LeadIngestionError):
    """Raised when a workflow is not found in the schema registry."""
    pass


class InvalidSchemaError(LeadIngestionError):
    """Raised when a schema is invalid or cannot be loaded."""
    pass


class LeadIngestion:
    """
    Ingests CSV lead files for a specific workflow.

    Uses csv.reader for deterministic header processing, handles aliases,
    normalizes values, validates against schema, and persists leads/rejections.
    """

    def __init__(self, db: Database, schema_registry: SchemaRegistry):
        self.db = db
        self.schema_registry = schema_registry

    def ingest_csv_file(
        self,
        file_path: Path,
        workflow_id: str,
        batch_id: str,
        idempotency_checker: FileIdempotencyChecker,
    ) -> IngestionStats:
        """
        Ingest a single CSV file.

        Discovers the file, checks idempotency, processes rows, and persists results
        transactionally.

        Args:
            file_path: Path to the CSV file (relative or absolute)
            workflow_id: Workflow identifier (from directory name)
            batch_id: Batch identifier (from file stem)
            idempotency_checker: FileIdempotencyChecker instance

        Returns:
            IngestionStats with processing statistics

        Raises:
            UnknownWorkflowError: If workflow_id is not in schema registry
            InvalidSchemaError: If schema is invalid
            ValueError: If file content has changed (idempotency violation)
        """
        # Resolve canonical path
        canonical_path = file_path.resolve()

        # Compute content hash
        content_hash = compute_file_hash(canonical_path)

        # Create discovered file object
        from solution.ingestion.discovery import DiscoveredFile
        discovered = DiscoveredFile(
            source_path=file_path,
            canonical_path=canonical_path,
            content_hash=content_hash,
            file_kind="leads",
            workflow_id=workflow_id,
            batch_id=batch_id,
            relative_path=str(file_path),
        )

        # Check idempotency - raises ValueError if content changed
        is_noop, file_id = idempotency_checker.check_and_record_file(discovered)

        if is_noop:
            # File already processed - fetch existing stats
            stats = self._get_file_stats(file_id)
            return stats

        # Load schema for workflow
        schema = self._load_schema(workflow_id)

        # Process file in transaction
        stats = IngestionStats()
        with transaction(self.db):
            conn = self.db.connect()

            # File already recorded by idempotency checker, just get its ID
            # The idempotency_checker.check_and_record_file already inserted if new

            # Create or get batch
            batch_id_in_db = get_or_create_batch(
                self.db,
                workflow_id,
                batch_id,
                file_id,
                conn,
            )

            # Process CSV rows
            self._process_csv_rows(
                file_path,
                workflow_id,
                batch_id,
                schema,
                conn,
                stats,
            )

            # Update file stats
            update_ingestion_file_stats(self.db, file_id, stats.to_dict(), conn)

        return stats

    def _load_schema(self, workflow_id: str) -> Dict:
        """Load schema for a workflow, raising if unknown."""
        schema = self.schema_registry.get_schema(workflow_id)
        if schema is None:
            raise UnknownWorkflowError(
                f"Unknown workflow '{workflow_id}'. "
                f"Available workflows: {self.schema_registry.list_workflows()}"
            )
        return schema

    def _get_file_stats(self, file_id: int) -> IngestionStats:
        """Get stats for an already-processed file."""
        conn = self.db.connect()
        cursor = conn.execute(
            """SELECT events_total, malformed_events, duplicate_events_dropped,
                      lead_rows_total, lead_rows_rejected, duplicate_lead_rows_merged, field_warnings
               FROM ingestion_files WHERE id = ?""",
            (file_id,),
        )
        row = cursor.fetchone()
        if row:
            stats = IngestionStats()
            stats.events_total = row["events_total"]
            stats.malformed_events = row["malformed_events"]
            stats.duplicate_events_dropped = row["duplicate_events_dropped"]
            stats.lead_rows_total = row["lead_rows_total"]
            stats.lead_rows_rejected = row["lead_rows_rejected"]
            stats.duplicate_lead_rows_merged = row["duplicate_lead_rows_merged"]
            stats.field_warnings = row["field_warnings"]
            return stats
        return IngestionStats()

    def _process_csv_rows(
        self,
        file_path: Path,
        workflow_id: str,
        batch_id: str,
        schema: Dict,
        conn,
        stats: IngestionStats,
    ) -> None:
        """
        Process all rows in a CSV file.

        Uses csv.reader to handle headers deterministically, including duplicates and aliases.
        """
        with open(file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)

            # Read and normalize header
            try:
                raw_header = next(reader)
            except StopIteration:
                # Empty file - nothing to process
                return

            # Normalize each header cell individually
            normalized_header = [normalize_header(cell) for cell in raw_header]

            # Map columns to schema fields with alias resolution
            column_mapping = self._build_column_mapping(normalized_header, schema)

            # Track logical row number (counts nonblank data rows from 1)
            logical_row_number = 0

            for raw_row in reader:
                # Check if row is blank (all cells empty/whitespace)
                if self._is_blank_row(raw_row):
                    continue

                logical_row_number += 1

                # Process row
                self._process_row(
                    raw_row,
                    normalized_header,
                    column_mapping,
                    workflow_id,
                    batch_id,
                    logical_row_number,
                    schema,
                    conn,
                    stats,
                )

    def _is_blank_row(self, row: List[str]) -> bool:
        """Check if a row is entirely blank (all cells empty/whitespace)."""
        if not row:
            return True
        return all(cell.strip() == "" for cell in row)

    def _build_column_mapping(
        self,
        normalized_header: List[str],
        schema: Dict,
    ) -> Dict[str, List[Tuple[int, str]]]:
        """
        Build mapping from schema field names to source column positions.

        Returns dict: field_name -> [(source_index, original_header), ...]

        Policy: If multiple columns map to one field, use the FIRST matching
        source column from left to right (deterministic based on header order).
        This is documented as the standard policy when no repository evidence exists.
        
        Note: Headers are normalized (underscores->spaces), so we need to
        normalize field names for matching too.
        """
        mapping: Dict[str, List[Tuple[int, str]]] = {}

        # Build lookup: normalized header value -> list of (position, original)
        # Normalize header values (replace underscores with spaces for matching)
        header_lookup: Dict[str, List[Tuple[int, str]]] = {}
        for idx, header in enumerate(normalized_header):
            # Also try replacing spaces with underscores for matching
            key = header.lower()
            header_lookup.setdefault(key, []).append((idx, header))
            # Also add underscore version
            underscore_key = key.replace(" ", "_")
            header_lookup.setdefault(underscore_key, []).append((idx, header))

        # For each schema field, find matching columns
        for field_def in schema.get("fields", []):
            field_name = field_def["name"]
            # Normalize field name for matching
            field_normalized = field_name.lower().replace("_", " ")
            field_aliases = [field_normalized] + [
                a.lower().replace("_", " ") for a in field_def.get("aliases", [])
            ]
            # Also try original field name with underscores
            field_aliases.append(field_name.lower())

            matches = []
            for alias in field_aliases:
                if alias in header_lookup:
                    matches.extend(header_lookup[alias])

            if matches:
                # Sort by position and take first
                matches.sort(key=lambda x: x[0])
                mapping[field_name] = matches[:1]  # Take first only
            else:
                mapping[field_name] = []  # No match found

        return mapping

    def _process_row(
        self,
        raw_row: List[str],
        normalized_header: List[str],
        column_mapping: Dict[str, List[Tuple[int, str]]],
        workflow_id: str,
        batch_id: str,
        logical_row_number: int,
        schema: Dict,
        conn,
        stats: IngestionStats,
    ) -> None:
        """
        Process a single CSV row.

        Validates, normalizes, and either accepts (insert/merge lead) or rejects.
        """
        # Build dict of raw values by schema field
        raw_values: Dict[str, str] = {}
        normalized_values: Dict[str, Any] = {}

        # Get field definitions in schema order
        fields = schema.get("fields", [])

        for field_def in fields:
            field_name = field_def["name"]
            field_type = field_def.get("type", "string")
            is_required = field_def.get("required", False)
            is_pii = field_def.get("pii", False)

            # Get raw value for this field
            raw_value = self._get_raw_value_for_field(
                raw_row, normalized_header, column_mapping, field_name
            )
            raw_values[field_name] = raw_value

            # Normalize value based on type
            normalized_value, warning = self._normalize_field_value(
                raw_value, field_type, field_def.get("enum_values", [])
            )

            if warning:
                stats.field_warnings += 1

            normalized_values[field_name] = normalized_value

        # Validate row
        rejection_reason = self._validate_row(
            raw_values, normalized_values, fields, workflow_id, batch_id, logical_row_number
        )

        if rejection_reason:
            # Row is rejected
            stats.lead_rows_rejected += 1
            insert_rejection(
                self.db,
                workflow_id,
                batch_id,
                logical_row_number,
                rejection_reason,
                conn,
            )
            return

        # Row is accepted
        stats.lead_rows_total += 1

        # Check for duplicate lead (same workflow_id + phone_hmac)
        phone = normalized_values.get("phone")
        if phone is None:
            # Phone is required, so this shouldn't happen for accepted rows
            # But handle gracefully
            stats.lead_rows_rejected += 1
            insert_rejection(
                self.db,
                workflow_id,
                batch_id,
                logical_row_number,
                "missing_required:phone",
                conn,
            )
            return

        # Compute phone HMAC
        phone_hmac = compute_phone_hmac(workflow_id, phone)

        # Check if lead already exists
        existing_lead = self._get_existing_lead(workflow_id, phone_hmac, conn)

        if existing_lead:
            # Duplicate - merge
            stats.duplicate_lead_rows_merged += 1
            self._merge_lead(
                existing_lead["id"],
                workflow_id,
                phone_hmac,
                batch_id,
                normalized_values,
                conn,
            )
        else:
            # New lead
            self._insert_new_lead(
                workflow_id,
                phone_hmac,
                batch_id,
                normalized_values,
                fields=fields,
                conn=conn,
            )

    def _get_raw_value_for_field(
        self,
        raw_row: List[str],
        normalized_header: List[str],
        column_mapping: Dict[str, List[Tuple[int, str]]],
        field_name: str,
    ) -> str:
        """
        Get the raw value for a field from the row.

        Uses column_mapping to find the source column. Returns empty string if not found.
        """
        matches = column_mapping.get(field_name, [])
        if not matches:
            return ""

        # Take the first match (deterministic policy: first from left to right)
        source_idx = matches[0][0]
        if source_idx < len(raw_row):
            return raw_row[source_idx]
        return ""

    def _normalize_field_value(
        self,
        raw_value: str,
        field_type: str,
        enum_values: Optional[List[str]] = None,
    ) -> Tuple[Any, bool]:
        """
        Normalize a field value based on its type.

        Returns (normalized_value, warning_flag) where warning_flag indicates
        if a field warning should be incremented (invalid optional value).
        """
        # Trim value
        value = raw_value.strip() if raw_value else ""

        # Empty string is missing/null
        if value == "":
            return None, False

        if field_type == "string":
            return value, False

        elif field_type == "phone":
            # Phone must be valid
            normalized = normalize_phone(value)
            if normalized is None:
                # Invalid phone - return None so validation can reject it
                return None, False
            return normalized, False

        elif field_type == "date":
            parsed = parse_date(value)
            if parsed is None:
                return value, True  # Warning for invalid optional date
            return parsed, False

        elif field_type == "decimal":
            normalized = normalize_decimal(value)
            if normalized is None:
                return None, True  # Warning for invalid optional decimal
            return normalized, False

        elif field_type == "enum":
            normalized = normalize_enum(value, enum_values)
            if normalized is None:
                return None, True  # Warning for invalid optional enum
            return normalized, False

        elif field_type == "timestamp":
            # Timestamps are handled in event ingestion, not leads
            return value, False

        else:
            # Unknown type - treat as string
            return value, False

    def _validate_row(
        self,
        raw_values: Dict[str, str],
        normalized_values: Dict[str, Any],
        fields: List[Dict],
        workflow_id: str,
        batch_id: str,
        row_number: int,
    ) -> Optional[str]:
        """
        Validate a row against schema requirements.

        Returns rejection reason string or None if valid.

        Priority (exact spec order):
        1. First required-missing failure: missing_required:<field_name>
        2. Invalid phone (if present but invalid): invalid_phone
        """
        for field_def in fields:
            field_name = field_def["name"]
            is_required = field_def.get("required", False)
            field_type = field_def.get("type", "string")

            normalized = normalized_values.get(field_name)
            raw = raw_values.get(field_name, "")

            if is_required and (normalized is None and (raw is None or raw.strip() == "")):
                # Required field is missing (either null or empty)
                return f"missing_required:{field_name}"

            if field_type == "phone":
                # Phone validation: if raw value exists but normalization failed
                if raw and raw.strip() and normalized is None:
                    # Phone was present but invalid
                    return "invalid_phone"

        return None

    def _get_existing_lead(
        self,
        workflow_id: str,
        phone_hmac: str,
        conn,
    ) -> Optional[Dict]:
        """Check if a lead already exists for this workflow + phone_hmac."""
        from solution.db import get_lead_by_phone_hmac
        return get_lead_by_phone_hmac(self.db, workflow_id, phone_hmac, conn)

    def _insert_new_lead(
        self,
        workflow_id: str,
        phone_hmac: str,
        batch_id: str,
        normalized_values: Dict[str, Any],
        fields: List[Dict],
        conn,
    ) -> None:
        """Insert a new lead with encrypted PII."""
        # Encrypt PII fields
        pii_fields = [f for f in fields if f.get("pii", False)]
        pii_ciphertext = None

        if pii_fields:
            # Combine all PII into one encrypted payload
            pii_dict = {}
            for field_def in pii_fields:
                field_name = field_def["name"]
                value = normalized_values.get(field_name)
                if value is not None:
                    pii_dict[field_name] = value

            if pii_dict:
                pii_json = json.dumps(pii_dict)
                pii_ciphertext = encrypt_pii(pii_json)

        # Get external_ref if present (check both policy_number and order_id)
        external_ref = normalized_values.get("policy_number") or normalized_values.get("order_id")

        # Non-PII attributes
        attributes = {}
        for field_def in fields:
            field_name = field_def["name"]
            if not field_def.get("pii", False):
                value = normalized_values.get(field_name)
                if value is not None:
                    # Convert Decimal and datetime to string for JSON serialization
                    if hasattr(value, 'quantize'):  # Decimal
                        attributes[field_name] = str(value)
                    elif hasattr(value, 'isoformat'):  # datetime
                        attributes[field_name] = value.strftime("%Y-%m-%d")
                    else:
                        attributes[field_name] = value

        # Insert lead
        insert_or_merge_lead(
            self.db,
            workflow_id=workflow_id,
            phone_hmac=phone_hmac,
            batch_id=batch_id,
            external_ref=external_ref,
            attributes=attributes,
            pii_ciphertext=pii_ciphertext,
            conn=conn,
        )

    def _merge_lead(
        self,
        existing_lead_id: int,
        workflow_id: str,
        phone_hmac: str,
        batch_id: str,
        normalized_values: Dict[str, Any],
        conn,
    ) -> None:
        """
        Merge a duplicate lead row into an existing lead.

        Preserves original batch_id, updates only non-null fields.
        Does NOT overwrite with null values.
        """
        from solution.db import insert_or_merge_lead

        # Get PII fields from schema
        # For this implementation, we'll re-fetch schema
        schema = self._load_schema(workflow_id)
        fields = schema.get("fields", [])
        pii_fields = [f for f in fields if f.get("pii", False)]

        # Build non-null attributes (only update if value is not None)
        attributes = {}
        for field_def in fields:
            field_name = field_def["name"]
            if not field_def.get("pii", False):
                value = normalized_values.get(field_name)
                if value is not None:
                    # Convert Decimal and datetime to string for JSON serialization
                    if hasattr(value, 'quantize'):  # Decimal
                        attributes[field_name] = str(value)
                    elif hasattr(value, 'isoformat'):  # datetime
                        attributes[field_name] = value.strftime("%Y-%m-%d")
                    else:
                        attributes[field_name] = value

        # Re-encrypt PII if any new PII values (non-null)
        pii_ciphertext = None
        has_new_pii = False
        for field_def in pii_fields:
            field_name = field_def["name"]
            value = normalized_values.get(field_name)
            if value is not None:
                has_new_pii = True
                break

        if has_new_pii:
            # Re-fetch existing PII and merge
            cursor = conn.execute(
                "SELECT pii_ciphertext FROM leads WHERE id = ?",
                (existing_lead_id,),
            )
            row = cursor.fetchone()
            existing_pii = row["pii_ciphertext"] if row else None

            if existing_pii:
                # Decrypt, merge, re-encrypt
                existing_pii_dict = json.loads(
                    decrypt_pii_bytes(existing_pii)
                )
                for field_def in pii_fields:
                    field_name = field_def["name"]
                    value = normalized_values.get(field_name)
                    if value is not None:
                        existing_pii_dict[field_name] = value
                pii_ciphertext = encrypt_pii(__import__("json").dumps(existing_pii_dict))
            elif normalized_values.get(pii_fields[0]["name"]) is not None:
                pii_dict = {}
                for field_def in pii_fields:
                    field_name = field_def["name"]
                    value = normalized_values.get(field_name)
                    if value is not None:
                        pii_dict[field_name] = value
                pii_ciphertext = encrypt_pii(__import__("json").dumps(pii_dict))

        external_ref = normalized_values.get("policy_number") or normalized_values.get("order_id")

        insert_or_merge_lead(
            self.db,
            workflow_id=workflow_id,
            phone_hmac=phone_hmac,
            batch_id=batch_id,  # Will be ignored (original preserved)
            external_ref=external_ref,
            attributes=attributes,
            pii_ciphertext=pii_ciphertext,
            conn=conn,
        )


class LeadFileDiscovery:
    """
    Discovers CSV lead files under leads/<workflow_id>/ directories.

    Determines workflow from directory name and batch_id from file stem.
    """

    def __init__(self, base_path: Path):
        self.base_path = base_path.resolve()

    def discover(self) -> List[Tuple[Path, str, str]]:
        """
        Discover all CSV files under leads/<workflow_id>/ directories.

        Returns:
            List of (file_path, workflow_id, batch_id) tuples
        """
        leads_dir = self.base_path / "leads"
        if not leads_dir.exists():
            return []

        discovered: List[Tuple[Path, str, str]] = []

        # Iterate over workflow directories
        for workflow_dir in sorted(leads_dir.iterdir()):
            if not workflow_dir.is_dir():
                continue

            workflow_id = workflow_dir.name

            # Iterate over CSV files in workflow directory
            for csv_file in sorted(workflow_dir.glob("*.csv")):
                batch_id = csv_file.stem  # File name without extension
                discovered.append((csv_file, workflow_id, batch_id))

        return discovered


def ingest_lead_batch(
    db: Database,
    schema_registry: SchemaRegistry,
    data_path: Path,
    idempotency_checker: FileIdempotencyChecker,
) -> Dict[str, IngestionStats]:
    """
    Ingest all lead CSV files in a data drop.

    Discovers files, validates workflows, and ingests each file transactionally.

    Args:
        db: Database instance
        schema_registry: SchemaRegistry instance
        data_path: Path to the data drop directory
        idempotency_checker: FileIdempotencyChecker instance

    Returns:
        Dict mapping (workflow_id, batch_id) to IngestionStats
    """
    ingestion = LeadIngestion(db, schema_registry)
    discovery = LeadFileDiscovery(data_path)

    results: Dict[str, IngestionStats] = {}

    files = discovery.discover()

    if not files:
        return results

    for file_path, workflow_id, batch_id in files:
        try:
            stats = ingestion.ingest_csv_file(
                file_path,
                workflow_id,
                batch_id,
                idempotency_checker,
            )
            key = f"{workflow_id}:{batch_id}"
            results[key] = stats
        except UnknownWorkflowError as e:
            raise LeadIngestionError(str(e))
        except InvalidSchemaError as e:
            raise LeadIngestionError(str(e))

    return results
