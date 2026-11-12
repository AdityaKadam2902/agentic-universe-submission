#!/usr/bin/env python3
"""
Agentic Universe Assignment A — Synthetic Data Generator

Generates adversarial test data for the Interaction Ledger & Reporting Pipeline.
Produces leads CSV files and events JSONL files with realistic edge cases.

Usage:
    python generate_data.py --seed 42 --scale small --out data/
    python generate_data.py --seed 123 --scale large --out data_large/

The generator intentionally creates:
- Header alias variations (batch_003 uses different aliases)
- Blank CSV rows (skipped, not counted)
- Duplicate leads across batches (merge semantics)
- Shuffled JSONL events (not chronological)
- Duplicate event IDs (within and across drops)
- Malformed JSON lines (truncated)
- Orphan events (for rejected/unuploaded leads)
- Human overrides in later drops
- Calls with zero to four calls per lead
"""

import argparse
import csv
import hashlib
import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

# Disposition codes for motor_renewal workflow
MOTOR_DISPOSITIONS = [
    ("NOT_CALLED", False, False),
    ("CONFIRMED", True, True),
    ("DECLINED", True, False),
    ("INTERESTED", False, False),
    ("NOT_ELIGIBLE", True, False),
    ("CALLBACK_REQUESTED", False, False),
    ("APPOINTMENT_SET", False, False),
    ("CONVERTED", True, True),
    ("NO_ANSWER", False, False),
    ("WRONG_NUMBER", True, False),
    ("DNC", True, False),
    ("THIRD_PARTY", True, False),
    ("NOT_AVAILABLE", False, False),
    ("COLLECT_DETAILS", False, False),
]

# First names and last names for generating customer names
FIRST_NAMES = [
    "Aarav", "Vihaan", "Aditya", "Arjun", "Reyansh", "Mohammed", "Ayaan",
    "Sai", "Vivaan", "Krishna", "Ishaan", "Lakshay", "Rudra", "Atharv",
    "Karthik", "Dhruv", "Shaunak", "Nikhil", "Rohan", "Akash"
]

LAST_NAMES = [
    "Sharma", "Verma", "Kumar", "Patel", "Singh", "Gupta", "Joshi",
    "Mehta", "Agarwal", "Bhatt", "Naik", "Iyer", "Reddy", "Nair",
    "Chopra", "Bajaj", "Malhotra", "Kapoor", "Sethi", "Gokhale"
]

PRODUCTS = [
    "Comprehensive", "Third Party", "Comprehensive Plus",
    "Motor Package", "Zero Depreciation"
]

# Date formats for generating varied output
DATE_FORMATS_INPUT = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y"]
DATE_FORMATS_OUTPUT = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y"]


def generate_phone(rng: random.Random) -> str:
    """Generate a valid 10-digit Indian mobile number starting with 6/7/8/9."""
    return str(rng.randint(6, 9)) + "".join(str(rng.randint(0, 9)) for _ in range(9))


def format_phone_variants(phone_digits: str, rng: random.Random) -> str:
    """Create a phone with random formatting that will normalize back."""
    prefix = rng.choice(["", "+91", "91", "0"])
    body = phone_digits
    if rng.random() < 0.3:
        # Add spaces or dashes
        body = f"{phone_digits[:5]} {phone_digits[5:]}" if rng.random() < 0.5 else f"{phone_digits[:4]}-{phone_digits[4:]}"
    if rng.random() < 0.2:
        # Add parentheses
        body = f"({phone_digits[:5]}) {phone_digits[5:]}"
    return prefix + body


def generate_name(rng: random.Random) -> str:
    """Generate a realistic Indian name."""
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    return f"{first} {last}"


def generate_policy_number(rng: random.Random) -> str:
    """Generate a policy number."""
    return f"POL{rng.randint(100000, 999999)}"


def generate_order_id(rng: random.Random) -> str:
    """Generate an order ID."""
    return f"ORD{rng.randint(100000, 999999)}"


def format_decimal(value: Decimal, fmt: str) -> str:
    """Format a decimal value with various prefixes."""
    if fmt == "direct":
        return f"{value:.2f}"
    elif fmt == "rupees":
        return f"Rs {value:.2f}"
    elif fmt == "inr":
        return f"INR {value:.2f}"
    elif fmt == "comma":
        s = f"{value:.2f}"
        parts = s.split(".")
        parts[0] = f"{int(parts[0]):,}"
        return ",".join(parts)
    elif fmt == "mixed":
        s = f"{value:.2f}"
        parts = s.split(".")
        parts[0] = f"Rs. {int(parts[0]):,}"
        return ".".join(parts)
    return str(value)


def format_date(dt: datetime, fmt: str) -> str:
    """Format a datetime with various date formats."""
    if fmt == "iso":
        return dt.strftime("%Y-%m-%d")
    elif fmt == "dmy_slash":
        return dt.strftime("%d/%m/%Y")
    elif fmt == "dmy_dash":
        return dt.strftime("%d-%m-%Y")
    elif fmt == "dmon":
        return dt.strftime("%d-%b-%Y")
    return dt.strftime("%Y-%m-%d")


def generate_timestamp(rng: random.Random, base_date: datetime) -> datetime:
    """Generate a timestamp within a range, possibly with timezone."""
    day_offset = rng.randint(0, 30)
    hour = rng.randint(8, 20)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)

    ts = base_date + timedelta(days=day_offset, hours=hour, minutes=minute, seconds=second)

    # Randomly make it naive (IST), UTC, or offset-aware
    choice = rng.random()
    if choice < 0.5:
        # Naive (will be interpreted as IST)
        return ts.replace(tzinfo=None)
    elif choice < 0.75:
        # UTC
        return ts.replace(tzinfo=timezone.utc)
    else:
        # Offset-aware (IST or similar)
        return ts.replace(tzinfo=timezone(timedelta(hours=5, minutes=30)))


def format_timestamp(ts: datetime) -> str:
    """Format a timestamp for JSON output."""
    if ts.tzinfo is None:
        # Naive - will be interpreted as IST
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    elif ts.tzinfo == timezone.utc:
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        # Offset-aware
        offset = ts.tzinfo.utcoffset(ts)
        total_minutes = int(offset.total_seconds() / 60)
        hours = abs(total_minutes) // 60
        minutes = abs(total_minutes) % 60
        sign = "+" if total_minutes >= 0 else "-"
        return ts.strftime(f"%Y-%m-%dT%H:%M:%S{sign}{hours:02d}:{minutes:02d}")


def generate_lead_row(workflow: str, rng: random.Random, use_aliases: bool = False) -> dict:
    """Generate a single lead row with appropriate field formatting."""
    phone_digits = generate_phone(rng)
    name = generate_name(rng)

    if workflow == "motor_renewal":
        policy = generate_policy_number(rng)
        # Randomly decide which optional fields to include
        has_expiry = rng.random() < 0.7
        has_premium = rng.random() < 0.7
        has_language = rng.random() < 0.5

        row = {}

        # Use aliases for batch_003 style variation (ALIASES ONLY, no mixed columns)
        if use_aliases:
            row["CUSTOMER_NAME"] = name
            row["MOBILE"] = format_phone_variants(phone_digits, rng)
            row["POLICY"] = policy
            if has_expiry:
                row["EXPIRY_DATE"] = format_date(
                    datetime(2026, rng.randint(10, 12), rng.randint(1, 28)),
                    rng.choice(DATE_FORMATS_INPUT)
                )
            if has_premium:
                row["PREMIUM"] = format_decimal(
                    Decimal(str(rng.randint(1000, 50000))) / 100,
                    rng.choice(["direct", "rupees", "inr", "comma", "mixed"])
                )
            if has_language:
                row["LANG"] = rng.choice(["EN", "en", "HI", "hi", "TA"])
            # Do NOT include the standard 'phone' column when using aliases
        elif False:  # This block intentionally left empty to prevent phone column
            pass
        else:
            row["customer_name"] = name
            row["phone"] = format_phone_variants(phone_digits, rng)
            row["policy_number"] = policy
            if has_expiry:
                row["policy_expiry"] = format_date(
                    datetime(2026, rng.randint(10, 12), rng.randint(1, 28)),
                    rng.choice(DATE_FORMATS_INPUT)
                )
            if has_premium:
                row["premium_amount"] = format_decimal(
                    Decimal(str(rng.randint(1000, 50000))) / 100,
                    rng.choice(["direct", "rupees", "inr", "comma", "mixed"])
                )
            if has_language:
                row["preferred_language"] = rng.choice(["EN", "en", "HI", "hi", "TA"])

        return row, phone_digits, policy

    # For policy_feedback, same structure

    else:  # policy_feedback
        order = generate_order_id(rng)
        has_product = rng.random() < 0.6
        has_date = rng.random() < 0.6
        has_language = rng.random() < 0.4

        row = {}

        if use_aliases:
            row["CUSTOMER_NAME"] = name
            row["MOBILE"] = format_phone_variants(phone_digits, rng)
            row["ORDER"] = order
            if has_product:
                row["PRODUCT"] = rng.choice(PRODUCTS)
            if has_date:
                row["ORDER_DATE"] = format_date(
                    datetime(2026, rng.randint(1, 12), rng.randint(1, 28)),
                    rng.choice(DATE_FORMATS_INPUT)
                )
            if has_language:
                row["LANG"] = rng.choice(["EN", "en", "HI", "hi"])
            # Do NOT include the standard 'phone' column when using aliases
        else:
            row["customer_name"] = name
            row["phone"] = format_phone_variants(phone_digits, rng)
            row["order_id"] = order
            if has_product:
                row["product"] = rng.choice(PRODUCTS)
            if has_date:
                row["order_date"] = format_date(
                    datetime(2026, rng.randint(1, 12), rng.randint(1, 28)),
                    rng.choice(DATE_FORMATS_INPUT)
                )
            if has_language:
                row["preferred_language"] = rng.choice(["EN", "en", "HI", "hi"])

        return row, phone_digits, order


def generate_event(workflow: str, call_id: str, event_seq: int, rng: random.Random,
                   occurred_at: datetime, existing_phones: dict = None) -> dict:
    """Generate a single event."""
    event_types = ["call.initiated", "call.connected", "call.ended",
                   "call.telephony_failed", "disposition.set", "disposition.override"]

    # Pick event type based on sequence
    if event_seq == 0:
        event_type = "call.initiated"
    elif event_seq == 1:
        event_type = rng.choice(["call.connected", "call.telephony_failed", "call.ended"])
    elif event_seq == 2:
        event_type = rng.choice(["call.ended", "disposition.set", "disposition.override"])
    else:
        event_type = rng.choice(event_types)

    event = {
        "event_id": f"evt_{workflow[:3]}_{call_id}_{event_seq:03d}",
        "event_type": event_type,
        "call_id": call_id,
        "occurred_at": format_timestamp(occurred_at),
        "workflow_id": workflow,
    }

    # Add phone for non-override events
    if event_type != "disposition.override":
        if existing_phones and call_id in existing_phones:
            phone = existing_phones[call_id]
        else:
            phone = generate_phone(rng)
        event["phone"] = format_phone_variants(phone, rng)

    # Add disposition code for disposition events
    if event_type.startswith("disposition."):
        code = rng.choice([d[0] for d in MOTOR_DISPOSITIONS])
        event["disposition_code"] = code

    return event


def generate_drop(drop_num: int, workflow: str, rng: random.Random,
                  base_date: datetime, num_leads: int, num_calls_per_lead_range: tuple,
                  use_aliases: bool = False, include_blank_rows: bool = True,
                  duplicate_leads: bool = True, include_orphans: bool = True,
                  include_overrides: bool = True, include_malformed: bool = True,
                  include_duplicates: bool = True) -> tuple:
    """
    Generate a drop with leads and events.

    Returns:
        (leads_data, events_data, metadata)
    """
    leads = []
    events = []
    metadata = {
        "leads_generated": 0,
        "calls_generated": 0,
        "duplicate_leads": 0,
        "orphan_phones": [],
        "malformed_event_count": 0,
        "duplicate_event_count": 0,
    }

    # Track phones and their associated call IDs
    phone_to_call = {}
    all_phones = set()
    lead_phones = []

    # Generate leads
    for i in range(num_leads):
        row, phone_digits, ref = generate_lead_row(workflow, rng, use_aliases)
        leads.append(row)
        metadata["leads_generated"] += 1
        lead_phones.append(phone_digits)
        all_phones.add(phone_digits)

        # Decide if this lead gets calls
        if rng.random() < 0.85:  # 85% of leads get at least one call
            num_calls = rng.randint(*num_calls_per_lead_range)
            metadata["calls_generated"] += num_calls

            for call_num in range(num_calls):
                call_id = f"call_{workflow[:3]}_{len(phone_to_call):04d}"
                phone_to_call[call_id] = phone_digits

                # Generate events for this call
                num_events = rng.randint(2, 5)
                base_ts = base_date + timedelta(
                    days=rng.randint(0, 60),
                    hours=rng.randint(8, 20)
                )

                for evt_seq in range(num_events):
                    ts = generate_timestamp(rng, base_ts + timedelta(minutes=evt_seq * rng.randint(1, 10)))
                    evt = generate_event(workflow, call_id, evt_seq, rng, ts, phone_to_call)
                    events.append(evt)

    # Add some duplicate leads (same phone, different batch)
    if duplicate_leads and len(lead_phones) > 3:
        num_duplicates = min(3, len(lead_phones) // 3)
        for _ in range(num_duplicates):
            phone = rng.choice(lead_phones)
            # Use same alias style as the batch
            row, _, ref = generate_lead_row(workflow, rng, use_aliases)
            # Override phone with the duplicate phone (must use same format style)
            if use_aliases:
                row["MOBILE"] = format_phone_variants(phone, rng)
            else:
                row["phone"] = format_phone_variants(phone, rng)
            leads.append(row)
            metadata["duplicate_leads"] += 1

    # Add orphan events (for phones not in leads)
    if include_orphans:
        num_orphans = rng.randint(2, 5)
        for _ in range(num_orphans):
            orphan_phone = generate_phone(rng)
            while orphan_phone in all_phones:
                orphan_phone = generate_phone(rng)
            all_phones.add(orphan_phone)
            metadata["orphan_phones"].append(orphan_phone)

            call_id = f"call_orph_{len(metadata['orphan_phones']):03d}"
            ts = generate_timestamp(rng, base_date + timedelta(days=rng.randint(0, 60)))
            evt = generate_event(workflow, call_id, 0, rng, ts, {call_id: orphan_phone})
            events.append(evt)

    # Add malformed events
    if include_malformed:
        num_malformed = rng.randint(1, 3)
        for i in range(num_malformed):
            ts = generate_timestamp(rng, base_date + timedelta(days=rng.randint(0, 60)))
            if rng.random() < 0.5:
                # Truncated JSON
                full_event = generate_event(workflow, f"call_mal_{i}", 0, rng, ts)
                full_json = json.dumps(full_event)
                events.append(full_json[:len(full_json) // 2])  # Truncate
            else:
                # Missing event_id
                evt = generate_event(workflow, f"call_mal_{i}", 0, rng, ts)
                del evt["event_id"]
                events.append(json.dumps(evt))
            metadata["malformed_event_count"] += 1

    # Add duplicate events (same event_id as existing)
    if include_duplicates and len(events) > 3:
        num_duplicates = rng.randint(1, 3)
        for i in range(num_duplicates):
            src_idx = rng.randint(0, len(events) - 1)
            if isinstance(events[src_idx], dict):
                events.append(events[src_idx].copy())
            else:
                events.append(events[src_idx])
            metadata["duplicate_event_count"] += 1

    # Add human overrides in later drops
    if include_overrides and drop_num == 2:
        num_overrides = rng.randint(2, 4)
        for i in range(num_overrides):
            if events:
                src_idx = rng.randint(0, len(events) - 1)
                if isinstance(events[src_idx], dict) and events[src_idx].get("event_type") in ["call.initiated", "call.connected"]:
                    call_id = events[src_idx]["call_id"]
                    # Find the phone for this call
                    phone = None
                    for e in events:
                        if isinstance(e, dict) and e.get("call_id") == call_id and "phone" in e:
                            phone = e["phone"]
                            break

                    if phone:
                        override_ts = generate_timestamp(rng, base_date + timedelta(days=65))
                        override = {
                            "event_id": f"evt_{workflow[:3]}_{call_id}_override",
                            "event_type": "disposition.override",
                            "call_id": call_id,
                            "occurred_at": format_timestamp(override_ts),
                            "workflow_id": workflow,
                            "disposition_code": rng.choice([d[0] for d in MOTOR_DISPOSITIONS]),
                        }
                        events.append(override)

    # Shuffle events (not chronological)
    rng.shuffle(events)

    # Convert events to JSONL strings
    events_jsonl = []
    for evt in events:
        if isinstance(evt, str):
            events_jsonl.append(evt)  # Already malformed
        else:
            events_jsonl.append(json.dumps(evt))

    # Add blank rows to leads
    if include_blank_rows:
        blank_positions = sorted([rng.randint(1, len(leads)) for _ in range(rng.randint(1, 3))])
        for pos in blank_positions:
            if pos <= len(leads):
                leads.insert(pos, {})  # Empty row

    return leads, events_jsonl, metadata


def write_leads_csv(leads: list, output_path: Path, fieldnames: list = None):
    """Write leads to CSV file."""
    if not fieldnames:
        # Collect all fieldnames from all rows
        fieldnames = []
        for row in leads:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in leads:
            writer.writerow(row)


def write_events_jsonl(events: list, output_path: Path):
    """Write events to JSONL file."""
    with open(output_path, "w", encoding="utf-8") as f:
        for evt in events:
            f.write(evt + "\n")


def generate_data(seed: int, scale: str, out_dir: str):
    """Generate synthetic data for Assignment A."""
    rng = random.Random(seed)

    # Base date for event generation
    base_date = datetime(2026, 10, 1, tzinfo=timezone.utc)

    out_path = Path(out_dir)
    if not out_path.exists():
        out_path.mkdir(parents=True)

    scale_config = {
        "small": {
            "motor_renewal_leads": 8,
            "policy_feedback_leads": 5,
            "calls_per_lead": (1, 3),
            "drops": 2,
        },
        "large": {
            "motor_renewal_leads": 50,
            "policy_feedback_leads": 30,
            "calls_per_lead": (1, 4),
            "drops": 2,
        }
    }

    config = scale_config[scale]

    for drop_num in range(1, config["drops"] + 1):
        drop_path = out_path / f"drop_{drop_num}"
        leads_path = drop_path / "leads"
        events_path = drop_path / "events"

        leads_path.mkdir(parents=True, exist_ok=True)
        events_path.mkdir(parents=True, exist_ok=True)

        # Generate motor_renewal leads
        motor_leads_path = leads_path / "motor_renewal"
        motor_leads_path.mkdir(parents=True, exist_ok=True)

        use_aliases = (drop_num == 2)  # Second drop uses aliases

        motor_leads, motor_events, motor_meta = generate_drop(
            drop_num=drop_num,
            workflow="motor_renewal",
            rng=rng,
            base_date=base_date,
            num_leads=config["motor_renewal_leads"] // config["drops"],
            num_calls_per_lead_range=config["calls_per_lead"],
            use_aliases=use_aliases,
            include_blank_rows=True,
            duplicate_leads=(drop_num == 2),
            include_orphans=True,
            include_overrides=True,
            include_malformed=True,
            include_duplicates=True,
        )

        # Determine batch filename
        if drop_num == 1:
            batch_file = motor_leads_path / "batch_001.csv"
        else:
            batch_file = motor_leads_path / "batch_003.csv"  # Uses aliases

        write_leads_csv(motor_leads, batch_file)
        print(f"Generated {batch_file}: {len(motor_leads)} rows")

        # Write motor_renewal events
        motor_events_file = events_path / "motor_renewal.jsonl"
        write_events_jsonl(motor_events, motor_events_file)
        print(f"Generated {motor_events_file}: {len(motor_events)} lines")

        # Generate policy_feedback leads (only in drop 1 for simplicity)
        if drop_num == 1:
            feedback_leads_path = leads_path / "policy_feedback"
            feedback_leads_path.mkdir(parents=True, exist_ok=True)

            feedback_leads, feedback_events, feedback_meta = generate_drop(
                drop_num=drop_num,
                workflow="policy_feedback",
                rng=rng,
                base_date=base_date,
                num_leads=config["policy_feedback_leads"],
                num_calls_per_lead_range=(1, 2),
                use_aliases=False,
                include_blank_rows=True,
                duplicate_leads=False,
                include_orphans=True,
                include_overrides=False,
                include_malformed=True,
                include_duplicates=True,
            )

            feedback_batch_file = feedback_leads_path / "batch_feedback_001.csv"
            write_leads_csv(feedback_leads, feedback_batch_file)
            print(f"Generated {feedback_batch_file}: {len(feedback_leads)} rows")

            feedback_events_file = events_path / "policy_feedback.jsonl"
            write_events_jsonl(feedback_events, feedback_events_file)
            print(f"Generated {feedback_events_file}: {len(feedback_events)} lines")

    print(f"\nGeneration complete. Seed={seed}, Scale={scale}")
    print(f"Output directory: {out_path.absolute()}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic data for Agentic Universe Assignment A"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    parser.add_argument(
        "--scale", choices=["small", "large"], default="small",
        help="Data scale (default: small)"
    )
    parser.add_argument(
        "--out", type=str, default="data",
        help="Output directory (default: data)"
    )

    args = parser.parse_args()
    generate_data(args.seed, args.scale, args.out)


if __name__ == "__main__":
    main()
