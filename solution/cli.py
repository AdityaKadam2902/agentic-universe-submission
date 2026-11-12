#!/usr/bin/env python3
"""CLI entry point for Agentic Universe Assignment A.

Usage:
    python -m solution.cli ingest <path>
    python -m solution.cli report --out <dir>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Any

from solution.config import load_env, get_db_path, get_data_path
from solution.db import Database
from solution.schema_registry import SchemaRegistry
from solution.ingestion.discovery import FileIdempotencyChecker, discover_files
from solution.ingestion.leads import LeadIngestion, IngestionStats, ingest_lead_batch
from solution.ingestion.events import EventIngestion, EventReconciliation, ingest_event_batch, reconcile_all
from solution.reporting.writers import generate_reports


def _get_ingestion_file_stats(db: Database, file_id: int) -> IngestionStats:
    """Get stats for an already-processed file."""
    conn = db.connect()
    cursor = conn.execute(
        "SELECT events_total, malformed_events, duplicate_events_dropped, "
        "lead_rows_total, lead_rows_rejected, duplicate_lead_rows_merged, field_warnings "
        "FROM ingestion_files WHERE id = ?",
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


def ingest_drop(db: Database, schema_registry: SchemaRegistry, drop_path: Path) -> Dict[str, Any]:
    """Ingest a single drop directory."""
    total_stats = IngestionStats()
    
    # Discover files
    discovered = discover_files(drop_path)
    
    # Separate into lead files and event files
    lead_files = [d for d in discovered if d.file_kind == "leads"]
    event_files = [d for d in discovered if d.file_kind == "events"]

    for discovered_file in lead_files:
        print(f"  Lead file: {discovered_file.source_path.relative_to(drop_path.parent)} -> workflow={discovered_file.workflow_id}, batch={discovered_file.batch_id}")
        try:
            stats = LeadIngestion(db, schema_registry).ingest_csv_file(
                discovered_file.source_path,
                discovered_file.workflow_id,
                discovered_file.batch_id,
                FileIdempotencyChecker(db),
            )
            total_stats.lead_rows_total += stats.lead_rows_total
            total_stats.lead_rows_rejected += stats.lead_rows_rejected
            total_stats.duplicate_lead_rows_merged += stats.duplicate_lead_rows_merged
            total_stats.field_warnings += stats.field_warnings
        except Exception as e:
            print(f"    Error: {e}", file=sys.stderr)

    for discovered_file in event_files:
        print(f"  Event file: {discovered_file.source_path.relative_to(drop_path.parent)} -> workflow={discovered_file.workflow_id}")
        try:
            event_stats = IngestionStats()
            # Get file_id from idempotency checker (recording the file)
            is_noop, file_id = FileIdempotencyChecker(db).check_and_record_file(discovered_file)
            if is_noop:
                # File already processed, skip
                event_stats = _get_ingestion_file_stats(db, file_id)
            else:
                # Ingest events with file_id for stats tracking
                ingest_event_batch(
                    db,
                    schema_registry,
                    drop_path,
                    discovered_file.workflow_id,
                    FileIdempotencyChecker(db),
                    event_stats,
                    file_id=file_id,
                )
            total_stats.events_total += event_stats.events_total
            total_stats.malformed_events += event_stats.malformed_events
            total_stats.duplicate_events_dropped += event_stats.duplicate_events_dropped
            
            # Reconcile after each event file
            reconciliation = EventReconciliation(db, schema_registry)
            recon_summary = reconciliation.reconcile(event_stats)
        except Exception as e:
            print(f"    Error: {e}", file=sys.stderr)
    
    # Final reconciliation - run AFTER all leads from this drop are ingested
    # so late-arriving leads can resolve events that were pending from
    # previous drops. This is idempotent.
    reconciliation = EventReconciliation(db, schema_registry)
    final_stats = IngestionStats()
    recon_summary = reconciliation.reconcile(final_stats)
    
    return {
        "lead_rows_total": total_stats.lead_rows_total,
        "lead_rows_rejected": total_stats.lead_rows_rejected,
        "duplicate_lead_rows_merged": total_stats.duplicate_lead_rows_merged,
        "field_warnings": total_stats.field_warnings,
        "events_total": total_stats.events_total,
        "malformed_events": total_stats.malformed_events,
        "duplicate_events_dropped": total_stats.duplicate_events_dropped,
        "orphan_events": recon_summary.get("orphan_events", 0),
        "unknown_disposition_events": recon_summary.get("unknown_dispositions", 0),
    }


def cmd_ingest(args: argparse.Namespace) -> int:
    """Handle ingest command."""
    load_env()
    
    db_path = get_db_path()
    db = Database(db_path)
    db.initialize()
    
    try:
        schema_dir = Path("assignment-a-data/schemas")
        dispositions_path = schema_dir / "dispositions.csv"
        schema_registry = SchemaRegistry(schema_dir, dispositions_path)
        
        drop_path = args.path.resolve()
        
        if drop_path.is_dir():
            # Check if it's a drop directory or parent folder
            if drop_path.name.startswith("drop_"):
                # Single drop
                print(f"Ingesting drop: {drop_path}")
                stats = ingest_drop(db, schema_registry, drop_path)
                print(f"\nIngestion complete:")
                print(f"  Lead rows: {stats['lead_rows_total']} total, {stats['lead_rows_rejected']} rejected")
                print(f"  Events: {stats['events_total']} total, {stats['malformed_events']} malformed, {stats['duplicate_events_dropped']} duplicates")
                print(f"  Orphan events: {stats['orphan_events']}")
                print(f"  Unknown dispositions: {stats['unknown_disposition_events']}")
            else:
                # Parent folder - process drops in order
                print(f"Ingesting parent folder: {drop_path}")
                drops = sorted([d for d in drop_path.iterdir() if d.is_dir() and d.name.startswith("drop_")])
                all_stats = {}
                for drop in drops:
                    print(f"\nProcessing {drop.name}:")
                    stats = ingest_drop(db, schema_registry, drop)
                    all_stats[drop.name] = stats
                
                # Aggregate stats
                agg = {
                    "lead_rows_total": sum(s["lead_rows_total"] for s in all_stats.values()),
                    "lead_rows_rejected": sum(s["lead_rows_rejected"] for s in all_stats.values()),
                    "duplicate_lead_rows_merged": sum(s["duplicate_lead_rows_merged"] for s in all_stats.values()),
                    "field_warnings": sum(s["field_warnings"] for s in all_stats.values()),
                    "events_total": sum(s["events_total"] for s in all_stats.values()),
                    "malformed_events": sum(s["malformed_events"] for s in all_stats.values()),
                    "duplicate_events_dropped": sum(s["duplicate_events_dropped"] for s in all_stats.values()),
                }
                # Get final orphan/unknown from last reconciliation
                reconciliation = EventReconciliation(db, schema_registry)
                final_stats = IngestionStats()
                recon_summary = reconciliation.reconcile(final_stats)
                agg["orphan_events"] = recon_summary.get("orphan_events", 0)
                agg["unknown_disposition_events"] = recon_summary.get("unknown_dispositions", 0)
                
                print(f"\n=== Total ingestion complete ===")
                print(f"  Lead rows: {agg['lead_rows_total']} total, {agg['lead_rows_rejected']} rejected")
                print(f"  Events: {agg['events_total']} total, {agg['malformed_events']} malformed, {agg['duplicate_events_dropped']} duplicates")
                print(f"  Orphan events: {agg['orphan_events']}")
                print(f"  Unknown dispositions: {agg['unknown_disposition_events']}")
        else:
            print(f"Error: {drop_path} is not a directory", file=sys.stderr)
            return 1
    finally:
        db.close()
    
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    """Handle report command."""
    load_env()
    
    db_path = get_db_path()
    db = Database(db_path)
    
    try:
        # Check if DB exists and has data
        if not db_path.exists():
            print(f"Error: Database not found at {db_path}", file=sys.stderr)
            print("Run 'python -m solution.cli ingest <path>' first.", file=sys.stderr)
            return 1
        
        conn = db.connect()
        cursor = conn.execute("SELECT COUNT(*) as count FROM leads")
        lead_count = cursor.fetchone()["count"]
        
        if lead_count == 0:
            print("Warning: No leads in database. Reports may be empty.", file=sys.stderr)
        
        schema_dir = Path("assignment-a-data/schemas")
        dispositions_path = schema_dir / "dispositions.csv"
        schema_registry = SchemaRegistry(schema_dir, dispositions_path)
        
        # Get aggregated stats
        from solution.db import get_aggregated_stats
        stats = get_aggregated_stats(db, conn)
        
        output_dir = args.out.resolve()
        print(f"Generating reports to: {output_dir}")
        
        generate_reports(db, schema_registry, output_dir, stats)
        
        print(f"\nReports generated:")
        for filename in ["batch_summary.csv", "daily_overview.csv", "lead_report.csv", "rejections.csv", "data_quality.json"]:
            filepath = output_dir / filename
            if filepath.exists():
                print(f"  {filename}")
            else:
                print(f"  {filename} - MISSING", file=sys.stderr)
    
    finally:
        db.close()
    
    return 0


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="solution.cli",
        description="Agentic Universe Assignment A — Interaction Ledger & Reporting Pipeline"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Ingest a data drop")
    ingest_parser.add_argument(
        "path",
        type=Path,
        help="Path to a drop directory or parent folder to ingest"
    )

    # Report command
    report_parser = subparsers.add_parser("report", help="Generate reports")
    report_parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output directory for reports"
    )

    args = parser.parse_args()

    if args.command == "ingest":
        return cmd_ingest(args)
    elif args.command == "report":
        return cmd_report(args)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
