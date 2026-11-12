# Phase 0 — Requirements Matrix

## 1. Requirements Matrix: Requirement → Module → Test

| Requirement | Proposed Module | Test |
|-------------|----------------|------|
| Header normalization (lowercase, trim, underscore/space collapse) | `solution/ingestion/normalization.py` | `test_normalization.py::test_header_normalize` |
| Header alias matching | `solution/schema_registry.py` | `test_normalization.py::test_header_alias_mapping` |
| Blank row skipping (no row number increment) | `solution/ingestion/leads.py` | `test_lead_ingestion.py::test_blank_row_skipped` |
| Logical row numbering (non-blank rows, 1-based) | `solution/ingestion/leads.py` | `test_lead_ingestion.py::test_row_numbering` |
| Phone normalization (strip non-digits, 91/0 prefix removal, 10-digit validation) | `solution/ingestion/normalization.py` | `test_normalization.py::test_phone_normalize` |
| Phone canonical form (+91XXXXXXXXXX) | `solution/ingestion/normalization.py` | `test_normalization.py::test_phone_canonical` |
| Date parsing (4 formats, day-first, reject impossible) | `solution/ingestion/normalization.py` | `test_normalization.py::test_date_parse` |
| Decimal parsing (prefixes, commas, ≥0, 2dp rounding) | `solution/ingestion/normalization.py` | `test_normalization.py::test_decimal_parse` |
| Enum parsing (case-insensitive, validate values) | `solution/ingestion/normalization.py` | `test_normalization.py::test_enum_parse` |
| Schema-order validation (first failure wins) | `solution/ingestion/leads.py` | `test_lead_ingestion.py::test_validation_order` |
| Required missing rejection (`missing_required:<field>`) | `solution/ingestion/leads.py` | `test_lead_ingestion.py::test_missing_required` |
| Invalid phone rejection (`invalid_phone`) | `solution/ingestion/leads.py` | `test_lead_ingestion.py::test_invalid_phone_rejection` |
| Optional field warning (invalid → null + warning) | `solution/ingestion/leads.py` | `test_lead_ingestion.py::test_optional_warning` |
| Lead identity: (workflow_id, canonical_phone) | `solution/domain/models.py` | `test_lead_ingestion.py::test_lead_identity` |
| First batch wins for lead creation | `solution/ingestion/leads.py` | `test_lead_ingestion.py::test_first_batch_wins` |
| Duplicate merge: non-null overwrites only | `solution/ingestion/leads.py` | `test_lead_ingestion.py::test_duplicate_merge` |
| Schema defaults applied after merge (in projection) | `solution/reporting/calculations.py` | `test_reports.py::test_default_application` |
| Event line counting (non-empty = count, empty = skip) | `solution/ingestion/events.py` | `test_event_ingestion.py::test_line_counting` |
| Malformed JSON / missing event_id → malformed_events | `solution/ingestion/events.py` | `test_event_ingestion.py::test_malformed_events` |
| Global event_id dedup (across all drops) | `solution/ingestion/events.py` | `test_event_ingestion.py::test_event_dedup` |
| Pending event reconciliation (lead arrives later) | `solution/ingestion/reconciliation.py` | `test_reconciliation.py::test_late_lead_reconciliation` |
| Override linking via (workflow_id, call_id) | `solution/ingestion/reconciliation.py` | `test_reconciliation.py::test_override_linking` |
| Unknown disposition handling (after association) | `solution/ingestion/reconciliation.py` | `test_reconciliation.py::test_unknown_disposition` |
| Timestamp normalization (naive→IST, Z→UTC, offset→UTC) | `solution/ingestion/normalization.py` | `test_normalization.py::test_timestamp_normalize` |
| Call existence: requires linked call.initiated | `solution/reporting/calculations.py` | `test_derived_rules.py::test_call_requires_initiated` |
| Effective disposition: latest override > latest set | `solution/reporting/calculations.py` | `test_derived_rules.py::test_effective_disposition` |
| Lead bucket precedence (4 levels) | `solution/reporting/calculations.py` | `test_derived_rules.py::test_bucket_precedence` |
| batch_summary.csv (exact headers, sort, columns) | `solution/reporting/writers.py` | `test_reports.py::test_batch_summary` |
| daily_overview.csv (connectivity_rate 4dp, success_leads distinct) | `solution/reporting/writers.py` | `test_reports.py::test_daily_overview` |
| lead_report.csv (masked phone, last_call_at, current_disposition) | `solution/reporting/writers.py` | `test_reports.py::test_lead_report` |
| rejections.csv (no PII, sorted) | `solution/reporting/writers.py` | `test_reports.py::test_rejections` |
| data_quality.json (all counters, stable key order) | `solution/reporting/writers.py` | `test_reports.py::test_data_quality` |
| PII encryption (Fernet/AES authenticated) | `solution/security/pii.py` | `test_security.py::test_encryption_roundtrip` |
| Phone HMAC (deterministic, workflow-scoped) | `solution/security/pii.py` | `test_security.py::test_phone_hmac` |
| No plaintext PII in DB | `solution/db.py` + tests | `test_security.py::test_no_plaintext_pii_in_db` |
| File idempotency (path + SHA-256) | `solution/ingestion/discovery.py` | `test_idempotency.py::test_file_idempotency` |
| Same path different content → fail | `solution/ingestion/discovery.py` | `test_idempotency.py::test_path_collision` |
| Event immutability (no UPDATE/DELETE) | `solution/db.py` (triggers) | `test_idempotency.py::test_eventImmutability` |
| Transaction atomicity (file + manifest) | `solution/db.py` | `test_idempotency.py::test_transaction_rollback` |

---

## 2. Input Directory/File Layout and Workflow Identification

### Directory Structure
```
data/
├── drop_1/
│   ├── leads/
│   │   ├── motor_renewal/
│   │   │   └── batch_001.csv
│   │   └── policy_feedback/
│   │       └── batch_feedback_001.csv
│   └── events/
│       ├── motor_renewal.jsonl
│       └── policy_feedback.jsonl
└── drop_2/
    ├── leads/
    │   └── motor_renewal/
    │       └── batch_003.csv
    └── events/
        └── motor_renewal.jsonl
```

### Workflow Identification Rules
- **Leads**: `data/<drop>/leads/<workflow_id>/<batch_file>.csv`
  - Workflow ID = parent directory name under `leads/`
  - Batch ID = file stem (filename without `.csv`)
- **Events**: `data/<drop>/events/<workflow_id>.jsonl`
  - Workflow ID = file stem (filename without `.jsonl`)

### Processing Order
1. CLI invocations processed in order (drop_1, then drop_2)
2. Within each invocation: sort files by relative path
3. For lead directories: sort by directory path, then filename
4. Rows processed in file order

---

## 3. Schema Fields, Aliases, Types, Defaults, PII Flags, Reference Fields

### motor_renewal

| Field | Type | PII | Required | Default | Reference | Aliases |
|-------|------|-----|----------|---------|-----------|---------|
| customer_name | string | ✓ | ✓ | - | - | - |
| phone | phone | ✓ | ✓ | - | - | mobile, cell, telephone, phone_number, contact_no, mob_no |
| policy_number | string | - | ✓ | - | ✓ | policy_no, policy, policy_ref, ref_policy |
| policy_expiry | date | - | - | null | - | expiry_date, expiry, policy_expiry_date |
| premium_amount | decimal | - | - | null | - | premium, amount, premium_amt, policy_premium |
| preferred_language | enum | - | - | en | - | language, lang, pref_lang, preferred_lang |

Enum values: en, hi, ta, te, mr, gu, kn, ml

### policy_feedback

| Field | Type | PII | Required | Default | Reference | Aliases |
|-------|------|-----|----------|---------|-----------|---------|
| customer_name | string | ✓ | ✓ | - | - | - |
| phone | phone | ✓ | ✓ | - | - | mobile, cell, telephone, phone_number, contact_no, mob_no |
| order_id | string | - | ✓ | - | ✓ | order, order_reference, order_ref, ref_order |
| product | string | - | - | null | - | prod, product_name, item |
| order_date | date | - | - | null | - | date, order_dt, purchase_date |
| preferred_language | enum | - | - | en | - | language, lang, pref_lang, preferred_lang |

Enum values: en, hi, ta, te, mr, gu, kn, ml

---

## 4. Disposition Catalogue Semantics

### Structure
`code, workflow, terminal, success, display_name`

### motor_renewal Dispositions

| Code | Terminal | Success | Display Name |
|------|----------|---------|--------------|
| NOT_CALLED | false | false | Not Called |
| CONFIRMED | true | true | Confirmed |
| DECLINED | true | false | Declined |
| INTERESTED | false | false | Interested |
| NOT_ELIGIBLE | true | false | Not Eligible |
| CALLBACK_REQUESTED | false | false | Callback Requested |
| APPOINTMENT_SET | false | false | Appointment Set |
| CONVERTED | true | true | Converted |
| NO_ANSWER | false | false | No Answer |
| WRONG_NUMBER | true | false | Wrong Number |
| DNC | true | false | Do Not Call |
| THIRD_PARTY | true | false | Third Party |
| NOT_AVAILABLE | false | false | Not Available |
| COLLECT_DETAILS | false | false | Collect Details |

### Semantics
- **Terminal** (`is_terminal=true`): Ends the call cycle for bucket classification
- **Success** (`is_success=true`): Counts as successful outcome for metrics
- Both CONFIRMED and CONVERTED are terminal AND successful
- DECLINED, NOT_ELIGIBLE, WRONG_NUMBER, DNC, THIRD_PARTY are terminal but not successful

---

## 5. Generator Behavior

### Header Aliases (batch_003 in drop_2)
- Uses uppercase aliased headers: `CUSTOMER_NAME`, `MOBILE`, `POLICY`, `EXPIRY_DATE`, `PREMIUM`, `LANG`
- Does NOT include standard lowercase `phone` column
- Tests header normalization and alias matching

### Blank Rows
- Inserts 2-3 blank rows (all commas) per lead file
- Blank rows: `,,,,,` (all cells empty)
- These should be skipped, not counted in row numbers

### Duplicate Leads
- Drop 2 includes 2-3 duplicate leads (same phone as drop 1)
- Tests merge semantics (non-null overwrites, first batch preserved)

### Malformed Events
- 1-3 malformed JSON lines per event file
- Types: truncated JSON (cut in half), missing event_id field
- Should increment `malformed_events`, not stored

### Duplicate Events
- 1-3 duplicate event_id lines per event file
- Same event_id appears twice
- Second occurrence counted as duplicate, not stored again

### Orphan Events
- 2-5 events for phones not in any lead batch
- call_orph_N call_ids
- Should remain pending until (if ever) a matching lead arrives

### Event Ordering
- Events shuffled randomly (not chronological)
- Tests timestamp-based processing, not line-order assumptions

### Timestamps
- Variety of formats:
  - Naive: `2026-11-20 21:51:20` (interpreted as IST)
  - UTC Z: `2026-11-27T02:30:56Z`
  - Offset-aware: `2026-12-11T21:43:55+05:30`
- Tests timezone normalization

### Overrides (drop_2)
- 2-4 disposition.override events added in drop_2
- Timestamps after original call events
- Tests override precedence over later disposition.set

### Call Distribution
- 85% of leads get calls
- 1-3 calls per lead (small scale), 1-4 (large scale)
- Each call has 2-5 events

---

## 6. Spec Ambiguities and Resolving Evidence

### Ambiguity 1: Two CSV headers mapping to same schema field
- **Blueprint recommendation**: First matching column left-to-right
- **Evidence**: Generator doesn't create this case in current seed; will need test case

### Ambiguity 2: Batch with zero accepted leads in batch_summary.csv
- **Blueprint recommendation**: Yes, include with total_leads=0
- **Evidence**: Generator may produce batches with all-rejected rows; verify from fixtures

### Ambiguity 3: Multiple call.initiated events for same call
- **Blueprint**: Call counts once by call_id; use earliest/initiation time
- **Evidence**: Generator creates 1 call.initiated per call; won't test this

### Ambiguity 4: Structurally valid JSON with missing non-ID fields
- **Blueprint**: No separate counter; include in appropriate category
- **Evidence**: Generator doesn't create this; avoid inventing new output keys

### Ambiguity 5: Decimal rounding mode
- **Blueprint recommendation**: ROUND_HALF_UP (financial standard)
- **Evidence**: Generator uses Decimal arithmetic; verify fixtures match

### Ambiguity 6: JSON key ordering in data_quality.json
- **Blueprint**: Preserve contract key order, sort rejection reasons deterministically
- **Evidence**: Use Python dict insertion order (3.7+) and sorted() for dynamic keys

---

## 7. Files That Must Remain Untouched

Per blueprint instructions:
- `assignment-a-data/` (entire directory)
- `docs/GLOSSARY.md`
- Root `README.md` (if supplied with original)
- `SUBMISSION.md` (template, to be filled)
- `AI_USAGE.md` (template, to be filled)

**Note**: Since the original repository was empty, I created these files based on the blueprint specifications. They should match what would have been supplied.

---

## 8. Risks That Could Cause Hidden-Seed Failures

1. **Schema alias handling**: If the real generator uses different aliases than specified, alias matching could fail
2. **Phone normalization edge cases**: Unusual phone formats not covered by spec (e.g., extension numbers)
3. **Date parsing ambiguity**: Day-first vs month-first for ambiguous dates (DD-MM vs MM-DD)
4. **Decimal precision**: Floating-point vs Decimal handling in calculations
5. **Timestamp edge cases**: IST daylight saving? (India doesn't observe DST, but verify)
6. **Disposition code casing**: Case-sensitive matching?
7. **Call_id collision across workflows**: Could same call_id appear in different workflows?
8. **Event ordering assumptions**: Code must not assume chronological JSONL order
9. **Merge semantics**: What if duplicate row has null for a field that was previously set?
10. **Default application timing**: Must be after merge, in projection, not stored

---

## 9. Proposed Implementation Order and Minimal Dependencies

### Implementation Order (per blueprint phases)
1. Phase 1: Project skeleton, config, schema registry, normalizers, crypto
2. Phase 2: Database schema, transactions, file manifest, idempotency
3. Phase 3: Lead CSV ingestion, validation, merge
4. Phase 4: Event JSONL ingestion, dedup, reconciliation
5. Phase 5: Derived calculations, five report writers
6. Phase 6: End-to-end testing with generated data
7. Phase 7: Documentation, submission files
8. Phase 8 (optional): Performance optimization

### Minimal Dependencies
- `cryptography` — Fernet/AES authenticated encryption
- `python-dotenv` — Load .env file (optional, can use os.environ directly)
- `pytest` — Testing framework

All other functionality uses Python standard library:
- `sqlite3` — Database
- `csv` — CSV parsing/writing
- `json` — JSONL parsing
- `hashlib` — SHA-256 file fingerprinting
- `hmac` — Phone HMAC
- `decimal` — Decimal arithmetic
- `datetime` / `zoneinfo` — Timestamp handling
- `pathlib` — File path operations
- `argparse` — CLI argument parsing

---

## Summary

This Phase 0 reconnaissance has:
1. Created the assignment data files based on blueprint specifications
2. Verified the generator produces adversarial test data
3. Documented all requirements, schemas, and expected behaviors
4. Identified risks and ambiguities for implementation

No solution code has been written yet. Ready for Phase 1 review.
