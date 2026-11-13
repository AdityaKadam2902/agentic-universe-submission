"""Report writers for the five required output files.

Deterministic writers that produce exact output contract.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any

from solution.domain.calculations import (
    DomainProjection,
    LeadFact,
    CallFact,
    DailyOverviewFact,
)


# IST timezone
IST_OFFSET = timedelta(hours=5, minutes=30)
IST = timezone(IST_OFFSET)


def mask_phone(phone_hmac: str, full_phone: Optional[str] = None) -> str:
    """
    Mask phone for display.
    
    Format: +91****** plus last 4 digits.
    """
    if full_phone:
        # Extract last 4 digits
        digits = ''.join(c for c in full_phone if c.isdigit())
        if len(digits) >= 4:
            last_4 = digits[-4:]
            return f"+91******{last_4}"
    # Fallback if no phone available
    return "+91******XX"


def format_ist_iso(dt: datetime) -> str:
    """Format datetime as ISO-8601 IST."""
    if dt is None:
        return ""
    ist_dt = dt.astimezone(IST)
    return ist_dt.isoformat()


def bool_to_str(value: bool) -> str:
    """Convert boolean to lowercase string."""
    return "true" if value else "false"


class ReportWriter:
    """Writes the five required report files."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir.resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_all(
        self,
        projection: DomainProjection,
        lead_facts: List[LeadFact],
        calls: List[CallFact],
        daily_overviews: List[DailyOverviewFact],
        ingestion_stats: Dict[str, Any],
        rejections: List[Dict],
        lead_phones: Dict[int, str],
        batches: List[Dict],
    ) -> None:
        """Write all five report files."""
        self.write_batch_summary(projection, lead_facts, batches)
        self.write_daily_overview(daily_overviews)
        self.write_lead_report(projection, lead_facts, lead_phones)
        self.write_rejections(rejections)
        self.write_data_quality(ingestion_stats)
    
    def write_batch_summary(
        self,
        projection: DomainProjection,
        lead_facts: List[LeadFact],
        batches: List[Dict],
    ) -> None:
        """Write batch_summary.csv.

        Every ingested lead batch appears, including batches whose rows
        were all rejected (represented with total_leads=0).
        """
        path = self.output_dir / "batch_summary.csv"

        # Group leads by (workflow_id, batch_id)
        batches_map: Dict[Tuple[str, str], Dict] = {}

        # Seed with every ingested batch so zero-lead batches still appear
        for batch in batches:
            key = (batch["workflow_id"], batch["batch_id"])
            batches_map.setdefault(key, {
                "workflow_id": batch["workflow_id"],
                "batch_id": batch["batch_id"],
                "total_leads": 0,
                "yet_to_reach_out": 0,
                "in_progress_telephony_failed": 0,
                "in_progress_dialed": 0,
                "completed": 0,
            })

        for lead in lead_facts:
            key = (lead.workflow_id, lead.batch_id)
            if key not in batches_map:
                batches_map[key] = {
                    "workflow_id": lead.workflow_id,
                    "batch_id": lead.batch_id,
                    "total_leads": 0,
                    "yet_to_reach_out": 0,
                    "in_progress_telephony_failed": 0,
                    "in_progress_dialed": 0,
                    "completed": 0,
                }

            batch = batches_map[key]
            batch["total_leads"] += 1

            bucket = lead.bucket
            if bucket == "yet_to_reach_out":
                batch["yet_to_reach_out"] += 1
            elif bucket == "in_progress_telephony_failed":
                batch["in_progress_telephony_failed"] += 1
            elif bucket == "in_progress_dialed":
                batch["in_progress_dialed"] += 1
            elif bucket == "completed":
                batch["completed"] += 1

        # Write CSV
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "workflow_id",
                "batch_id",
                "total_leads",
                "yet_to_reach_out",
                "in_progress_telephony_failed",
                "in_progress_dialed",
                "completed",
            ])

            for key in sorted(batches_map.keys()):
                batch = batches_map[key]
                writer.writerow([
                    batch["workflow_id"],
                    batch["batch_id"],
                    batch["total_leads"],
                    batch["yet_to_reach_out"],
                    batch["in_progress_telephony_failed"],
                    batch["in_progress_dialed"],
                    batch["completed"],
                ])
    
    def write_daily_overview(self, daily_overviews: List[DailyOverviewFact]) -> None:
        """Write daily_overview.csv."""
        path = self.output_dir / "daily_overview.csv"
        
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "workflow_id",
                "date",
                "total_calls",
                "connected_calls",
                "connectivity_rate",
                "success_leads",
            ])
            
            for overview in daily_overviews:
                writer.writerow([
                    overview.workflow_id,
                    overview.date,
                    overview.total_calls,
                    overview.connected_calls,
                    str(overview.connectivity_rate),
                    len(overview.success_leads),
                ])
    
    def write_lead_report(
        self,
        projection: DomainProjection,
        lead_facts: List[LeadFact],
        lead_phones: Dict[int, str],  # lead_id -> decrypted phone
    ) -> None:
        """Write lead_report.csv."""
        path = self.output_dir / "lead_report.csv"
        
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "workflow_id",
                "external_ref",
                "batch_id",
                "phone_masked",
                "bucket",
                "total_calls",
                "connected_calls",
                "last_call_at",
                "current_disposition",
                "ever_success",
            ])
            
            for lead in sorted(lead_facts, key=lambda l: (l.workflow_id, l.external_ref)):
                last_call_at = lead.last_call_at_ist
                last_call_str = format_ist_iso(last_call_at) if last_call_at else ""
                
                current_disp = lead.current_disposition_code or ""
                
                # Get phone for masking
                phone = lead_phones.get(lead.lead_id, "")
                
                writer.writerow([
                    lead.workflow_id,
                    lead.external_ref,
                    lead.batch_id,
                    mask_phone(lead.phone_hmac, phone),
                    lead.bucket,
                    lead.total_calls,
                    lead.connected_calls,
                    last_call_str,
                    current_disp,
                    bool_to_str(lead.ever_success),
                ])
    
    def write_rejections(self, rejections: List[Dict]) -> None:
        """Write rejections.csv (no PII)."""
        path = self.output_dir / "rejections.csv"
        
        # Sort by (workflow_id, batch_id, row_number)
        sorted_rejections = sorted(
            rejections,
            key=lambda r: (r["workflow_id"], r["batch_id"], r["row_number"]),
        )
        
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "workflow_id",
                "batch_id",
                "row_number",
                "reason",
            ])
            
            for rej in sorted_rejections:
                writer.writerow([
                    rej["workflow_id"],
                    rej["batch_id"],
                    rej["row_number"],
                    rej["reason"],
                ])
    
    def write_data_quality(self, stats: Dict[str, Any]) -> None:
        """Write data_quality.json."""
        path = self.output_dir / "data_quality.json"
        
        # Build the exact structure required
        output = {
            "lead_rows_total": stats.get("lead_rows_total", 0),
            "lead_rows_rejected": stats.get("lead_rows_rejected", 0),
            "rejections_by_reason": stats.get("rejections_by_reason", {}),
            "duplicate_lead_rows_merged": stats.get("duplicate_lead_rows_merged", 0),
            "field_warnings": stats.get("field_warnings", 0),
            "events_total": stats.get("events_total", 0),
            "malformed_events": stats.get("malformed_events", 0),
            "duplicate_events_dropped": stats.get("duplicate_events_dropped", 0),
            "orphan_events": stats.get("orphan_events", 0),
            "unknown_disposition_events": stats.get("unknown_disposition_events", 0),
        }
        
        # Sort rejections_by_reason keys deterministically
        if "rejections_by_reason" in output:
            output["rejections_by_reason"] = dict(
                sorted(output["rejections_by_reason"].items())
            )
        
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")


def generate_reports(
    db: "Database",
    schema_registry: "SchemaRegistry",
    output_dir: Path,
    ingestion_stats: Dict[str, Any],
) -> None:
    """
    Generate all five reports from database state.
    
    Args:
        db: Database instance
        schema_registry: SchemaRegistry instance
        output_dir: Directory to write reports to
        ingestion_stats: Aggregated ingestion statistics
    """
    from solution.db import Database
    from solution.security.pii import decrypt_pii
    import json as json_module
    
    projection = DomainProjection(db, schema_registry)
    lead_facts, calls = projection.build_projection()
    daily_overviews = projection.build_daily_overview(calls)
    
    # Get rejections
    conn = db.connect()
    cursor = conn.execute(
        "SELECT workflow_id, batch_id, row_number, reason FROM rejections ORDER BY workflow_id, batch_id, row_number"
    )
    rejections = [dict(row) for row in cursor.fetchall()]

    # Get all ingested lead batches (for zero-lead batch representation)
    cursor = conn.execute(
        "SELECT workflow_id, batch_id FROM lead_batches ORDER BY workflow_id, batch_id"
    )
    batches = [dict(row) for row in cursor.fetchall()]
    
    # Get lead phones for masking (decrypt PII)
    cursor = conn.execute("SELECT id, pii_ciphertext FROM leads")
    lead_phones = {}
    for row in cursor.fetchall():
        if row["pii_ciphertext"]:
            try:
                pii_json = decrypt_pii(row["pii_ciphertext"])
                pii_dict = json_module.loads(pii_json)
                if "phone" in pii_dict:
                    lead_phones[row["id"]] = pii_dict["phone"]
            except Exception:
                pass  # Can't decrypt, skip
    
    writer = ReportWriter(output_dir)
    writer.write_all(
        projection,
        lead_facts,
        calls,
        daily_overviews,
        ingestion_stats,
        rejections,
        lead_phones,
        batches,
    )
