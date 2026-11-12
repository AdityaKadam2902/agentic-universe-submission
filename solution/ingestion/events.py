"""JSONL event ingestion with reconciliation.

Separates immutable event facts from mutable association state.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field

from solution.db import (
    Database,
    transaction,
    insert_event,
    get_event_by_id,
    event_exists,
    set_event_association,
    get_event_association,
    set_call_link,
    get_call_link,
    get_lead_by_phone_hmac,
    get_lead_by_external_ref,
    get_batch_lead_count,
    update_ingestion_file_stats,
)
from solution.schema_registry import SchemaRegistry
from solution.ingestion.normalization import normalize_phone, normalize_timestamp
from solution.security.pii import compute_phone_hmac
from solution.ingestion.leads import LeadIngestion, IngestionStats


@dataclass
class EventProcessingResult:
    """Result of processing a single event line."""
    event_id: str = ""
    is_malformed: bool = False
    is_duplicate: bool = False
    is_valid: bool = False
    phone_hmac: Optional[str] = None
    workflow_id: Optional[str] = None
    event_type: Optional[str] = None
    call_id: Optional[str] = None
    occurred_at_utc: Optional[str] = None
    disposition_code: Optional[str] = None
    

class EventIngestion:
    """
    Ingests JSONL event files.

    Events are immutable facts - once stored, they are never updated.
    Association state is tracked separately in event_associations.
    """

    def __init__(self, db: Database, schema_registry: SchemaRegistry):
        self.db = db
        self.schema_registry = schema_registry

    def ingest_jsonl_file(
        self,
        file_path: Path,
        workflow_id: str,
        idempotency_checker,
        stats: IngestionStats,
        file_id: int = None,
    ) -> None:
        """
        Ingest a JSONL event file.

        Args:
            file_path: Path to the JSONL file
            workflow_id: Workflow identifier
            idempotency_checker: FileIdempotencyChecker instance
            stats: IngestionStats to update
            file_id: Optional file ID for stats tracking
        """
        self._file_id = file_id
        
        # Process file in transaction
        with transaction(self.db):
            conn = self.db.connect()

            with open(file_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    # Skip empty/whitespace lines (don't count)
                    if not line.strip():
                        continue

                    # Every non-empty line increments events_total
                    stats.events_total += 1

                    # Parse and validate event
                    result = self._process_event_line(
                        line, workflow_id, conn
                    )

                    if result.is_malformed:
                        stats.malformed_events += 1
                    elif result.is_duplicate:
                        stats.duplicate_events_dropped += 1
                    elif result.is_valid:
                        # Insert event fact (immutable)
                        event_id = insert_event(
                            self.db,
                            event_id=result.event_id,
                            workflow_id=result.workflow_id,
                            event_type=result.event_type,
                            call_id=result.call_id,
                            occurred_at_utc=result.occurred_at_utc,
                            phone_hmac=result.phone_hmac,
                            disposition_code=result.disposition_code,
                            source_file_id=0,  # TODO: track file_id
                            source_line=line_num,
                            conn=conn,
                        )
                        
                        # Update file stats for event ingestion
                        if self._file_id:
                            update_ingestion_file_stats(
                                self.db,
                                file_id=self._file_id,
                                stats={
                                    "events_total": stats.events_total,
                                    "malformed_events": stats.malformed_events,
                                    "duplicate_events_dropped": stats.duplicate_events_dropped,
                                },
                                conn=conn,
                            )

                        # Create pending association
                        set_event_association(
                            self.db,
                            event_id=result.event_id,
                            lead_id=None,
                            status="pending_lead",
                            conn=conn,
                        )

                        # If has phone, try to match to lead
                        if result.phone_hmac:
                            self._try_match_event_to_lead(
                                result.event_id,
                                result.workflow_id,
                                result.phone_hmac,
                                conn,
                            )

                        # If has call_id, track for override resolution
                        if result.call_id and result.event_type == "call.link":
                            self._track_call_link(
                                result.event_id,
                                result.workflow_id,
                                result.call_id,
                                conn,
                            )

    def _process_event_line(
        self,
        line: str,
        workflow_id: str,
        conn,
    ) -> EventProcessingResult:
        """
        Process a single JSONL line.

        Returns EventProcessingResult with validation status.
        """
        result = EventProcessingResult()

        # Try to parse JSON
        try:
            event_data = json.loads(line.strip())
        except json.JSONDecodeError:
            result.is_malformed = True
            return result

        # Check for event_id
        event_id = event_data.get("event_id")
        if not event_id or not str(event_id).strip():
            result.is_malformed = True
            return result

        event_id = str(event_id).strip()

        # Check for duplicates (globally across all files/drops)
        if event_exists(self.db, event_id, conn):
            result.is_duplicate = True
            return result

        # Extract and validate fields
        result.event_id = event_id
        result.workflow_id = workflow_id

        # Event type
        result.event_type = event_data.get("event_type")

        # Call ID
        result.call_id = event_data.get("call_id")

        # Timestamp normalization
        occurred_at = event_data.get("occurred_at")
        if occurred_at:
            result.occurred_at_utc = normalize_timestamp(str(occurred_at))
        else:
            # Use current UTC time
            from datetime import datetime, timezone
            result.occurred_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Phone handling
        phone = event_data.get("phone")
        if phone and str(phone).strip():
            normalized_phone = normalize_phone(str(phone).strip())
            if normalized_phone:
                result.phone_hmac = compute_phone_hmac(
                    workflow_id, normalized_phone
                )

        # Disposition code (if present)
        disposition = event_data.get("disposition")
        if isinstance(disposition, dict):
            result.disposition_code = disposition.get("code")
        elif isinstance(disposition, str) and disposition.strip():
            result.disposition_code = disposition.strip()

        result.is_valid = True
        return result

    def _try_match_event_to_lead(
        self,
        event_id: str,
        workflow_id: str,
        phone_hmac: str,
        conn,
    ) -> bool:
        """
        Try to match an event to an existing lead.

        Returns True if matched, False if pending.
        """
        # Look up lead by phone_hmac
        lead = get_lead_by_phone_hmac(self.db, workflow_id, phone_hmac, conn)

        if lead:
            # Match found - associate event with lead
            set_event_association(
                self.db,
                event_id=event_id,
                lead_id=lead["id"],
                status="linked",
                conn=conn,
            )
            return True
        else:
            # No lead yet - keep pending
            # (may arrive in later drops)
            return False

    def _track_call_link(
        self,
        event_id: str,
        workflow_id: str,
        call_id: str,
        conn,
    ) -> None:
        """
        Track a call.link event for later override resolution.

        call.link events have no phone but link a call_id to a lead.
        We need to track these so disposition.override events can be
        resolved later when the phone-bearing event arrives.
        """
        # Look for a lead with this call_id through events
        # For now, just store the association
        set_event_association(
            self.db,
            event_id=event_id,
            lead_id=None,
            status="pending_call_link",
            conn=conn,
        )


class EventReconciliation:
    """
    Reconciles events with leads after ingestion.

    Handles:
    - Phone-based matching (workflow_id, phone_hmac)
    - Call link resolution for disposition.override events
    - Disposition code validation against catalog
    - Orphan event counting
    """

    def __init__(self, db: Database, schema_registry: SchemaRegistry):
        self.db = db
        self.schema_registry = schema_registry

    def reconcile(self, stats: IngestionStats) -> Dict[str, Any]:
        """
        Reconcile all pending events with available leads.

        This is idempotent - running multiple times produces same result.

        Returns reconciliation summary.
        """
        conn = self.db.connect()
        summary = {
            "events_reconciled": 0,
            "call_links_resolved": 0,
            "unknown_dispositions": 0,
            "orphan_events": 0,
        }

        with transaction(self.db):
            # Step 1: Match pending phone events to leads
            pending_events = self._get_pending_events(conn)
            for event in pending_events:
                if event["phone_hmac"]:
                    lead = get_lead_by_phone_hmac(
                        self.db,
                        event["workflow_id"],
                        event["phone_hmac"],
                        conn,
                    )
                    if lead:
                        set_event_association(
                            self.db,
                            event_id=event["event_id"],
                            lead_id=lead["id"],
                            status="linked",
                            conn=conn,
                        )
                        summary["events_reconciled"] += 1

                        # If this phone-bearing event has a call_id, create a call link
                        # This enables override events to be resolved later
                        if event["call_id"]:
                            set_call_link(
                                self.db,
                                workflow_id=event["workflow_id"],
                                call_id=event["call_id"],
                                lead_id=lead["id"],
                                conn=conn,
                            )

            # Step 1b: Link non-phone events (call.connected, call.ended, etc.) to the same lead
            # via call_id if a call link exists
            non_phone_pending = self._get_pending_non_phone_events(conn)
            for event in non_phone_pending:
                if event["call_id"]:
                    call_link = get_call_link(
                        self.db,
                        event["workflow_id"],
                        event["call_id"],
                        conn,
                    )
                    if call_link:
                        set_event_association(
                            self.db,
                            event_id=event["event_id"],
                            lead_id=call_link["lead_id"],
                            status="linked",
                            conn=conn,
                        )
                        summary["events_reconciled"] += 1

            # Step 2: Resolve call links for call.link events (to enable override resolution)
            call_link_events = self._get_pending_call_link_events(conn)
            for event in call_link_events:
                if event["call_id"]:
                    # Find phone-bearing event for this call_id
                    phone_event = self._find_phone_event_for_call(
                        event["workflow_id"], event["call_id"], conn
                    )
                    if phone_event:
                        # Get the lead from the phone event
                        lead = get_lead_by_phone_hmac(
                            self.db,
                            event["workflow_id"],
                            phone_event["phone_hmac"],
                            conn,
                        )
                        if lead:
                            # Set call link
                            set_call_link(
                                self.db,
                                workflow_id=event["workflow_id"],
                                call_id=event["call_id"],
                                lead_id=lead["id"],
                                conn=conn,
                            )
                            summary["call_links_resolved"] += 1

                            # Also associate the call.link event
                            set_event_association(
                                self.db,
                                event_id=event["event_id"],
                                lead_id=lead["id"],
                                status="linked",
                                conn=conn,
                            )
                            summary["events_reconciled"] += 1

            # Step 3: Resolve disposition.override events via call links
            override_events = self._get_pending_override_events(conn)
            for event in override_events:
                if event["call_id"]:
                    # Check if we have a call link for this call_id
                    call_link = get_call_link(
                        self.db,
                        event["workflow_id"],
                        event["call_id"],
                        conn,
                    )
                    if call_link:
                        # Resolve the override event via the call link
                        set_event_association(
                            self.db,
                            event_id=event["event_id"],
                            lead_id=call_link["lead_id"],
                            status="linked",
                            conn=conn,
                        )
                        summary["events_reconciled"] += 1

                        if event["disposition_code"]:
                            self._validate_disposition(
                                event["workflow_id"],
                                event["disposition_code"],
                                call_link["lead_id"],
                                conn,
                                stats,
                                summary,
                            )

            # Step 4: Mark unknown dispositions on already-linked events
            # After linking, check all linked events for unknown disposition codes
            self._mark_unknown_dispositions(conn, stats, summary)

            # Step 3: Count orphan events
            summary["orphan_events"] = self._count_orphan_events(conn)

        return summary

    def _get_pending_events(self, conn) -> List[Dict]:
        """Get all pending (unresolved) events."""
        cursor = conn.execute(
            """SELECT e.* FROM events e
               LEFT JOIN event_associations ea ON e.event_id = ea.event_id
               WHERE ea.event_id IS NULL 
                  OR ea.association_status IN ('pending_lead', 'pending_call_link')
               ORDER BY e.created_at_utc"""
        )
        return [dict(row) for row in cursor.fetchall()]

    def _get_pending_call_link_events(self, conn) -> List[Dict]:
        """Get pending events that are call.link type."""
        cursor = conn.execute(
            """SELECT e.* FROM events e
               LEFT JOIN event_associations ea ON e.event_id = ea.event_id
               WHERE e.event_type = 'call.link'
               AND (ea.event_id IS NULL OR ea.association_status = 'pending_call_link')"""
        )
        return [dict(row) for row in cursor.fetchall()]

    def _get_pending_override_events(self, conn) -> List[Dict]:
        """Get pending events that are disposition.override type (need call link resolution)."""
        cursor = conn.execute(
            """SELECT e.* FROM events e
               LEFT JOIN event_associations ea ON e.event_id = ea.event_id
               WHERE e.event_type = 'disposition.override'
               AND (ea.event_id IS NULL OR ea.association_status = 'pending_lead')"""
        )
        return [dict(row) for row in cursor.fetchall()]

    def _get_pending_non_phone_events(self, conn) -> List[Dict]:
        """Get pending events that have no phone_hmac but have call_id."""
        cursor = conn.execute(
            """SELECT e.* FROM events e
               LEFT JOIN event_associations ea ON e.event_id = ea.event_id
               WHERE e.phone_hmac IS NULL
               AND e.call_id IS NOT NULL
               AND (ea.event_id IS NULL OR ea.association_status = 'pending_lead')"""
        )
        return [dict(row) for row in cursor.fetchall()]

    def _find_phone_event_for_call(
        self, workflow_id: str, call_id: str, conn
    ) -> Optional[Dict]:
        """Find a phone-bearing event for a given call_id."""
        cursor = conn.execute(
            """SELECT e.* FROM events e
               WHERE e.workflow_id = ?
               AND e.call_id = ?
               AND e.phone_hmac IS NOT NULL""",
            (workflow_id, call_id),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def _validate_disposition(
        self,
        workflow_id: str,
        disposition_code: str,
        lead_id: int,
        conn,
        stats: IngestionStats,
        summary: Dict,
    ) -> None:
        """
        Validate a disposition code against the workflow's catalog.

        Unknown codes increment unknown_disposition_events.
        """
        # Get dispositions for this workflow
        dispositions = self.schema_registry.get_dispositions(workflow_id)
        
        valid_codes = {d["code"] for d in dispositions} if dispositions else set()

        if disposition_code not in valid_codes:
            stats.field_warnings += 1  # Or could be a separate counter
            summary["unknown_dispositions"] += 1
            # Don't affect derived reports - just count it

    def _get_event_association(self, event_id: str, conn) -> Optional[Dict]:
        """Get event association."""
        cursor = conn.execute(
            "SELECT * FROM event_associations WHERE event_id = ?",
            (event_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def _mark_unknown_dispositions(self, conn, stats: IngestionStats, summary: Dict) -> None:
        """Mark events with unknown disposition codes as ignored."""
        # Get all linked events with disposition codes
        cursor = conn.execute(
            """SELECT e.*, ea.association_status FROM events e
               INNER JOIN event_associations ea ON e.event_id = ea.event_id
               WHERE ea.association_status = 'linked'
               AND ea.lead_id IS NOT NULL
               AND e.disposition_code IS NOT NULL"""
        )
        
        for event in [dict(row) for row in cursor.fetchall()]:
            # Check if disposition is unknown
            dispositions = self.schema_registry.get_dispositions(event["workflow_id"])
            valid_codes = {d["code"] for d in dispositions} if dispositions else set()
            
            if event["disposition_code"] not in valid_codes:
                # Check if already marked as ignored
                if event.get("association_status") != "ignored_unknown_disposition":
                    # Mark as ignored
                    conn.execute(
                        "UPDATE event_associations SET association_status = 'ignored_unknown_disposition' WHERE event_id = ?",
                        (event["event_id"],),
                    )
                    summary["unknown_dispositions"] += 1
                    stats.field_warnings += 1

    def _count_orphan_events(self, conn) -> int:
        """
        Count currently unresolved unique valid events.

        Orphan = valid event that is not associated with a lead.
        This is a snapshot, not cumulative.
        """
        cursor = conn.execute(
            """SELECT COUNT(DISTINCT e.event_id) as count FROM events e
               LEFT JOIN event_associations ea ON e.event_id = ea.event_id
               WHERE ea.event_id IS NULL
                  OR ea.association_status IN ('pending_lead', 'pending_call_link')"""
        )
        return cursor.fetchone()["count"]


def ingest_event_batch(
    db: Database,
    schema_registry: SchemaRegistry,
    data_path: Path,
    workflow_id: str,
    idempotency_checker,
    event_stats: IngestionStats,
    file_id: int = None,
) -> EventReconciliation:
    """
    Ingest all JSONL event files for a workflow.

    Args:
        db: Database instance
        schema_registry: SchemaRegistry instance
        data_path: Path to the data drop directory
        workflow_id: Workflow identifier
        idempotency_checker: FileIdempotencyChecker instance
        event_stats: IngestionStats to update
        file_id: Optional file ID for stats tracking

    Returns:
        EventReconciliation instance for further reconciliation
    """
    ingestion = EventIngestion(db, schema_registry)

    events_dir = data_path / "events"
    if not events_dir.exists():
        return EventReconciliation(db, schema_registry)

    # Find JSONL files for this workflow
    jsonl_files = list(events_dir.glob(f"{workflow_id}.jsonl"))

    for jsonl_file in jsonl_files:
        ingestion.ingest_jsonl_file(
            jsonl_file,
            workflow_id,
            idempotency_checker,
            event_stats,
            file_id=file_id,
        )

    return EventReconciliation(db, schema_registry)


def reconcile_all(
    db: Database,
    schema_registry: SchemaRegistry,
) -> Dict[str, Any]:
    """
    Run full reconciliation across all workflows.

    This is idempotent and can be called multiple times.
    """
    reconciliation = EventReconciliation(db, schema_registry)
    stats = IngestionStats()
    return reconciliation.reconcile(stats)
