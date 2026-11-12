"""File discovery and idempotency handling.

Discovers CSV/JSONL files in drop directories and handles file-level idempotency
using canonical path + SHA-256 content hash.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import List, Optional, Tuple, Generator
from dataclasses import dataclass

from solution.db import Database, compute_file_hash, transaction


@dataclass
class DiscoveredFile:
    """A discovered file ready for ingestion."""
    source_path: Path
    canonical_path: Path
    content_hash: str
    file_kind: str  # 'leads' or 'events'
    workflow_id: str
    batch_id: Optional[str]  # Only for lead files
    relative_path: str


def discover_files(drop_path: Path) -> List[DiscoveredFile]:
    """
    Discover all CSV and JSONL files in a drop directory.

    For leads: discovers CSV files under leads/<workflow_id>/
    For events: discovers JSONL files under events/

    Returns files in deterministic order (sorted by relative path).
    """
    drop_path = drop_path.resolve()
    if not drop_path.exists():
        raise FileNotFoundError(f"Drop path does not exist: {drop_path}")

    discovered: List[DiscoveredFile] = []

    # Discover lead CSV files
    leads_dir = drop_path / "leads"
    if leads_dir.exists():
        for workflow_dir in sorted(leads_dir.iterdir()):
            if not workflow_dir.is_dir():
                continue
            workflow_id = workflow_dir.name
            for csv_file in sorted(workflow_dir.glob("*.csv")):
                discovered.append(_create_discovered_file(
                    csv_file, drop_path, "leads", workflow_id, csv_file.stem
                ))

    # Discover event JSONL files
    events_dir = drop_path / "events"
    if events_dir.exists():
        for jsonl_file in sorted(events_dir.glob("*.jsonl")):
            workflow_id = jsonl_file.stem
            discovered.append(_create_discovered_file(
                jsonl_file, drop_path, "events", workflow_id, None
            ))

    return discovered


def discover_files_recursive(base_path: Path) -> List[DiscoveredFile]:
    """
    Recursively discover all CSV and JSONL files under a base path.

    Processes files in deterministic order based on relative path.
    """
    base_path = base_path.resolve()
    if not base_path.exists():
        raise FileNotFoundError(f"Base path does not exist: {base_path}")

    discovered: List[DiscoveredFile] = []

    # Find all CSV and JSONL files
    csv_files = sorted(base_path.rglob("*.csv"))
    jsonl_files = sorted(base_path.rglob("*.jsonl"))

    for csv_file in csv_files:
        # Infer workflow and batch from path structure
        # Expected: base/<drop>/leads/<workflow>/<batch>.csv
        rel = csv_file.relative_to(base_path)
        parts = rel.parts

        workflow_id = None
        batch_id = None
        file_kind = "leads"

        # Try to parse path structure
        if len(parts) >= 3 and parts[1] == "leads":
            workflow_id = parts[2]  # leads/<workflow>/...
            batch_id = csv_file.stem

        discovered.append(_create_discovered_file(
            csv_file, base_path, file_kind, workflow_id or "unknown", batch_id
        ))

    for jsonl_file in jsonl_files:
        # Infer workflow from filename
        rel = jsonl_file.relative_to(base_path)
        parts = rel.parts

        workflow_id = None
        file_kind = "events"

        # Try to parse path structure
        if len(parts) >= 2 and parts[1] == "events":
            workflow_id = jsonl_file.stem

        discovered.append(_create_discovered_file(
            jsonl_file, base_path, file_kind, workflow_id or "unknown", None
        ))

    return discovered


def _create_discovered_file(
    file_path: Path,
    base_path: Path,
    file_kind: str,
    workflow_id: str,
    batch_id: Optional[str],
) -> DiscoveredFile:
    """Create a DiscoveredFile from a file path."""
    canonical_path = file_path.resolve()
    content_hash = compute_file_hash(canonical_path)
    relative_path = str(file_path.relative_to(base_path))

    return DiscoveredFile(
        source_path=file_path,
        canonical_path=canonical_path,
        content_hash=content_hash,
        file_kind=file_kind,
        workflow_id=workflow_id,
        batch_id=batch_id,
        relative_path=relative_path,
    )


class FileIdempotencyChecker:
    """Handles file-level idempotency checks and manifest management."""

    def __init__(self, db: Database):
        self.db = db

    def check_and_record_file(
        self,
        discovered: DiscoveredFile,
    ) -> Tuple[bool, Optional[int]]:
        """
        Check if a file has already been processed.

        Returns (is_noop, file_id) where:
        - is_noop: True if file was already processed with same hash (skip)
        - file_id: The ingestion_files record ID if new or existing

        Raises ValueError if same path has different hash (content changed).
        """
        conn = self.db.connect()

        with transaction(self.db):
            # Check if file exists in manifest
            existing = self._get_file_record(discovered.source_path, conn)

            if existing:
                # File exists - check hash
                if existing["content_hash"] == discovered.content_hash:
                    # Same hash - already processed, this is a no-op
                    if existing["status"] == "completed":
                        return True, existing["id"]
                    else:
                        # Should not happen if we always complete in transaction
                        # But handle gracefully
                        return True, existing["id"]
                else:
                    # Same path, different hash - content changed!
                    raise ValueError(
                        f"File content changed for {discovered.source_path}: "
                        f"previous hash {existing['content_hash']} != current hash {discovered.content_hash}. "
                        f"To reset, delete the database and re-ingest."
                    )
            else:
                # New file - record it
                file_id = self._record_new_file(discovered, conn)
                return False, file_id

    def _get_file_record(
        self,
        source_path: Path,
        conn,
    ) -> Optional[Dict]:
        """Get ingestion file record by source path."""
        cursor = conn.execute(
            "SELECT * FROM ingestion_files WHERE source_path = ?",
            (str(source_path),),
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    def _record_new_file(
        self,
        discovered: DiscoveredFile,
        conn,
    ) -> int:
        """Record a new file in the manifest."""
        from solution.db import _utc_now

        cursor = conn.execute(
            """INSERT INTO ingestion_files
               (source_path, canonical_path, content_hash, file_kind, workflow_id, batch_id, file_sequence, processed_at_utc, status, created_at_utc)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
            (
                str(discovered.source_path),
                str(discovered.canonical_path),
                discovered.content_hash,
                discovered.file_kind,
                discovered.workflow_id,
                discovered.batch_id,
                0,
                _utc_now(),
                _utc_now(),
            ),
        )
        return cursor.lastrowid

    def complete_file(
        self,
        file_id: int,
        stats: Dict[str, int],
    ) -> None:
        """Mark a file as completed with its processing stats."""
        conn = self.db.connect()
        with transaction(self.db):
            conn.execute(
                """UPDATE ingestion_files SET
                   events_total = ?,
                   malformed_events = ?,
                   duplicate_events_dropped = ?,
                   lead_rows_total = ?,
                   lead_rows_rejected = ?,
                   duplicate_lead_rows_merged = ?,
                   field_warnings = ?,
                   status = 'completed',
                   processed_at_utc = ?
                   WHERE id = ?""",
                (
                    stats.get("events_total", 0),
                    stats.get("malformed_events", 0),
                    stats.get("duplicate_events_dropped", 0),
                    stats.get("lead_rows_total", 0),
                    stats.get("lead_rows_rejected", 0),
                    stats.get("duplicate_lead_rows_merged", 0),
                    stats.get("field_warnings", 0),
                    _utc_now(),
                    file_id,
                ),
            )
