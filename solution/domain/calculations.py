"""Domain models and projection logic for reports.

Pure functions that derive report facts from linked event/lead data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Set, Tuple, Any
from collections import defaultdict
from pathlib import Path

from solution.db import Database
from solution.schema_registry import SchemaRegistry


# IST timezone (Asia/Kolkata = UTC+5:30)
IST_OFFSET = timedelta(hours=5, minutes=30)
IST = timezone(IST_OFFSET)


@dataclass
class DispositionInfo:
    """Disposition metadata from catalogue."""
    code: str
    is_terminal: bool
    is_success: bool
    display_name: str


@dataclass
class CallFact:
    """Derived fact about a single call."""
    workflow_id: str
    call_id: str
    lead_id: int
    external_ref: str
    initiated_at_utc: datetime
    initiated_at_ist: datetime
    connected: bool = False
    telephony_failed: bool = False
    effective_disposition_code: Optional[str] = None
    effective_disposition: Optional[DispositionInfo] = None


@dataclass
class LeadFact:
    """Derived fact about a single lead."""
    workflow_id: str
    lead_id: int
    external_ref: str
    batch_id: str
    phone_hmac: str
    calls: List[CallFact] = field(default_factory=list)
    
    @property
    def total_calls(self) -> int:
        return len(self.calls)
    
    @property
    def connected_calls(self) -> int:
        return sum(1 for c in self.calls if c.connected)
    
    @property
    def last_call_at_ist(self) -> Optional[datetime]:
        if not self.calls:
            return None
        return max(c.initiated_at_ist for c in self.calls)
    
    @property
    def current_disposition_code(self) -> Optional[str]:
        """Effective disposition on most recent call that has one."""
        # Sort calls by initiated time descending
        sorted_calls = sorted(self.calls, key=lambda c: c.initiated_at_ist, reverse=True)
        for call in sorted_calls:
            if call.effective_disposition_code:
                return call.effective_disposition_code
        return None
    
    @property
    def current_disposition(self) -> Optional[DispositionInfo]:
        code = self.current_disposition_code
        if not code:
            return None
        # Return cached disposition if available
        return getattr(self, '_current_disposition', None)
    
    @current_disposition.setter
    def current_disposition(self, value: Optional[DispositionInfo]) -> None:
        """Set the cached current disposition."""
        self._current_disposition = value
    
    @property
    def ever_success(self) -> bool:
        return any(
            c.effective_disposition and c.effective_disposition.is_success
            for c in self.calls
        )
    
    @property
    def bucket(self) -> str:
        """Determine lead bucket per exact precedence."""
        if not self.calls:
            return "yet_to_reach_out"
        
        # Check if any call has terminal disposition
        has_terminal = any(
            c.effective_disposition and c.effective_disposition.is_terminal
            for c in self.calls
        )
        if has_terminal:
            return "completed"
        
        # Check if all calls have telephony_failed
        if self.calls and all(c.telephony_failed for c in self.calls):
            return "in_progress_telephony_failed"
        
        return "in_progress_dialed"


@dataclass
class DailyOverviewFact:
    """Derived fact for daily overview."""
    workflow_id: str
    date: str  # IST date as YYYY-MM-DD
    total_calls: int = 0
    connected_calls: int = 0
    success_leads: Set[int] = field(default_factory=set)
    
    @property
    def connectivity_rate(self) -> Decimal:
        if self.total_calls == 0:
            return Decimal("0.0000")
        rate = Decimal(self.connected_calls) / Decimal(self.total_calls)
        return rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


class DomainProjection:
    """
    Pure domain projection from database state to report facts.
    
    Reads only linked, report-eligible events and accepted leads.
    """
    
    def __init__(self, db: Database, schema_registry: SchemaRegistry):
        self.db = db
        self.schema_registry = schema_registry
        self.dispositions: Dict[Tuple[str, str], DispositionInfo] = {}
        self._load_dispositions()
    
    def _load_dispositions(self) -> None:
        """Load disposition catalogue for all workflows."""
        for workflow_id in self.schema_registry.list_workflows():
            dispositions = self.schema_registry.get_dispositions(workflow_id)
            if dispositions:
                for d in dispositions:
                    key = (workflow_id, d["code"])
                    self.dispositions[key] = DispositionInfo(
                        code=d["code"],
                        is_terminal=d.get("terminal", False),
                        is_success=d.get("success", False),
                        display_name=d.get("display_name", d["code"]),
                    )
    
    def get_disposition(self, workflow_id: str, code: str) -> Optional[DispositionInfo]:
        """Get disposition info for a workflow and code."""
        return self.dispositions.get((workflow_id, code))
    
    def build_projection(self) -> Tuple[List[LeadFact], List[CallFact]]:
        """
        Build complete projection from database state.
        
        Returns (leads, calls) where leads contain their calls.
        """
        conn = self.db.connect()
        
        # Get all linked events (not pending, not ignored_unknown_disposition)
        events = self._get_linked_events(conn)
        
        # Get all leads
        leads = self._get_all_leads(conn)
        
        # Build call facts from events
        calls = self._build_call_facts(events, leads)
        
        # Build lead facts with their calls
        lead_facts = self._build_lead_facts(leads, calls)
        
        return lead_facts, calls
    
    def _get_linked_events(self, conn) -> List[Dict]:
        """Get all linked events that are report-eligible."""
        cursor = conn.execute(
            """SELECT e.* FROM events e
               INNER JOIN event_associations ea ON e.event_id = ea.event_id
               WHERE ea.association_status = 'linked'
               AND ea.lead_id IS NOT NULL
               AND ea.association_status != 'ignored_unknown_disposition'
               ORDER BY e.occurred_at_utc"""
        )
        events = [dict(row) for row in cursor.fetchall()]
        return events
    
    def _get_all_leads(self, conn) -> List[Dict]:
        """Get all leads."""
        cursor = conn.execute(
            """SELECT * FROM leads ORDER BY workflow_id, external_ref"""
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def _build_call_facts(
        self, events: List[Dict], leads: List[Dict]
    ) -> List[CallFact]:
        """Build call facts from linked events."""
        # Group events by (workflow_id, call_id)
        call_events: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
        call_to_lead: Dict[Tuple[str, str], int] = {}
        call_to_external_ref: Dict[Tuple[str, str], str] = {}
        
        # Build lead lookup
        lead_lookup = {lead["id"]: lead for lead in leads}
        
        for event in events:
            workflow_id = event["workflow_id"]
            call_id = event.get("call_id")
            
            if not call_id:
                continue
            
            key = (workflow_id, call_id)
            call_events[key].append(event)
            
            # Get lead for this event
            assoc = self._get_event_association(event["event_id"], conn=None)
            if assoc and assoc.get("lead_id"):
                lead_id = assoc["lead_id"]
                call_to_lead[key] = lead_id
                if lead_id in lead_lookup:
                    lead = lead_lookup[lead_id]
                    external_ref = lead.get("external_ref") or ""
                    call_to_external_ref[key] = external_ref
        
        # Build call facts
        calls = []
        for (workflow_id, call_id), evts in call_events.items():
            # Require call.initiated
            initiated_events = [
                e for e in evts 
                if e["event_type"] == "call.initiated"
            ]
            
            if not initiated_events:
                continue  # No initiated event, not a reportable call
            
            # Use the first initiated event (there should be only one per call)
            initiated = initiated_events[0]
            initiated_at_utc = self._parse_utc(initiated["occurred_at_utc"])
            initiated_at_ist = initiated_at_utc.astimezone(IST)
            
            lead_id = call_to_lead.get((workflow_id, call_id))
            external_ref = call_to_external_ref.get((workflow_id, call_id), "")
            
            # Check for connected
            connected = any(
                e["event_type"] == "call.connected" for e in evts
            )
            
            # Check for telephony_failed
            telephony_failed = any(
                e["event_type"] == "call.telephony_failed" for e in evts
            )
            
            # Determine effective disposition
            effective_disposition = self._get_effective_disposition(evts, workflow_id)
            
            call_fact = CallFact(
                workflow_id=workflow_id,
                call_id=call_id,
                lead_id=lead_id or 0,
                external_ref=external_ref,
                initiated_at_utc=initiated_at_utc,
                initiated_at_ist=initiated_at_ist,
                connected=connected,
                telephony_failed=telephony_failed,
                effective_disposition_code=effective_disposition.code if effective_disposition else None,
                effective_disposition=effective_disposition,
            )
            
            calls.append(call_fact)
        
        return calls
    
    def _get_event_association(self, event_id: str, conn) -> Optional[Dict]:
        """Get event association (can be called without conn for testing)."""
        if conn is None:
            conn = self.db.connect()
        cursor = conn.execute(
            "SELECT * FROM event_associations WHERE event_id = ?",
            (event_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def _parse_utc(self, utc_str: str) -> datetime:
        """Parse UTC timestamp string to datetime."""
        # Handle Z suffix
        if utc_str.endswith("Z"):
            utc_str = utc_str[:-1] + "+00:00"
        return datetime.fromisoformat(utc_str)
    
    def _get_effective_disposition(
        self, events: List[Dict], workflow_id: str
    ) -> Optional[DispositionInfo]:
        """
        Get effective disposition for a call.
        
        Priority: latest override by occurred_at, then latest set, then none.
        Override takes precedence over sets even if set is later.
        """
        overrides = [
            e for e in events 
            if e["event_type"] == "disposition.override"
        ]
        sets = [
            e for e in events 
            if e["event_type"] == "disposition.set"
        ]
        
        if overrides:
            # Latest override by occurred_at
            latest = max(overrides, key=lambda e: e["occurred_at_utc"])
            code = latest.get("disposition_code")
            if code:
                return self.get_disposition(workflow_id, code)
        
        if sets:
            # Latest set by occurred_at
            latest = max(sets, key=lambda e: e["occurred_at_utc"])
            code = latest.get("disposition_code")
            if code:
                return self.get_disposition(workflow_id, code)
        
        return None
    
    def _build_lead_facts(
        self, leads: List[Dict], calls: List[CallFact]
    ) -> List[LeadFact]:
        """Build lead facts with their associated calls."""
        # Group calls by lead_id
        calls_by_lead: Dict[int, List[CallFact]] = defaultdict(list)
        for call in calls:
            if call.lead_id:
                calls_by_lead[call.lead_id].append(call)
        
        lead_facts = []
        for lead in leads:
            lead_id = lead["id"]
            lead_calls = calls_by_lead.get(lead_id, [])
            
            # Sort calls by initiated time
            lead_calls.sort(key=lambda c: c.initiated_at_ist)
            
            lead_fact = LeadFact(
                workflow_id=lead["workflow_id"],
                lead_id=lead_id,
                external_ref=lead.get("external_ref") or "",
                batch_id=lead["batch_id"],
                phone_hmac=lead["phone_hmac"],
                calls=lead_calls,
            )
            
            # Set current_disposition with actual info
            if lead_fact.current_disposition_code:
                disp = self.get_disposition(
                    lead["workflow_id"], 
                    lead_fact.current_disposition_code
                )
                lead_fact.current_disposition = disp
            
            lead_facts.append(lead_fact)
        
        return lead_facts
    
    def build_daily_overview(self, calls: List[CallFact]) -> List[DailyOverviewFact]:
        """Build daily overview facts from calls."""
        # Group by (workflow_id, IST date)
        daily: Dict[Tuple[str, str], DailyOverviewFact] = {}
        
        for call in calls:
            date_str = call.initiated_at_ist.strftime("%Y-%m-%d")
            key = (call.workflow_id, date_str)
            
            if key not in daily:
                daily[key] = DailyOverviewFact(
                    workflow_id=call.workflow_id,
                    date=date_str,
                )
            
            daily_fact = daily[key]
            daily_fact.total_calls += 1
            if call.connected:
                daily_fact.connected_calls += 1
            if call.effective_disposition and call.effective_disposition.is_success:
                if call.lead_id:
                    daily_fact.success_leads.add(call.lead_id)
        
        return sorted(daily.values(), key=lambda d: (d.workflow_id, d.date))
