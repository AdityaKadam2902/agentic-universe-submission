# Submission — Agentic Universe Assignment A

## Candidate Information

- **Name**: Aditya Kadam
- **Email**: adityakadam2902@gmail.com
- **Submission Date**: 13 November 2026
- **Assessment**: Agentic Universe — Assignment A (Data Engineering)

---

## Implementation Summary

### What was built

A privacy-preserving data ingestion pipeline that:
1. Processes lead CSV batches and call-event JSONL files from multiple workflows
2. Persists facts safely in SQLite with PII encryption (Fernet) and HMAC-based phone matching
3. Reconciles late-arriving records (events before leads, overrides before call links)
4. Generates five exact reports: batch_summary.csv, daily_overview.csv, lead_report.csv, rejections.csv, data_quality.json

Key components:
- **Schema Registry**: Dynamically loads workflow schemas and disposition catalogues from assignment-a-data/schemas/
- **Normalizers**: Pure functions for phone, date, decimal, enum, and timestamp normalization per exact spec rules
- **PII Security**: Fernet authenticated encryption for PII fields, HMAC-SHA256 for workflow-separated phone matching
- **SQLite Storage**: Append-only event records protected by database triggers, encrypted PII storage, file-level processing manifests, and recoverable ingestion state
- **Reconciliation Service**: Resolves pending events when leads arrive later, handles call-link-based override resolution
- **Report Generators**: Pure domain projection + deterministic CSV/JSON writers with exact header/column order/sorting

### Architecture decisions

| Decision | Rationale |
|----------|-----------|
| Modular monolith | Simpler than microservices for this scope; clear layer separation still maintained |
| SQLite | Explicitly acceptable per the assignment; reduces setup risk; PG not required |
| Explicit SQL (not ORM) | Clearer control over queries, constraints, indexes |
| Fernet encryption | Authenticated encryption from cryptography library; industry standard |
| HMAC-SHA256 for phones | Deterministic, allows matching without decryption; workflow-separated via input format `workflow_id\|phone` |
| File-level idempotency via SHA-256 | Prevents double-processing; detects content changes at same path |
| SQLite triggers for event immutability | Enforced at DB level, not just application layer |
| Pure domain projection for reports | Reads only from DB state; deterministic; no hidden state |

### Assumptions made

1. **Two headers mapping to same field**: First matching column left-to-right wins
2. **Batches with zero accepted leads**: Still recorded in batch_summary.csv with total_leads=0 (ingested batch)
3. **Multiple call.initiated for same call**: Use first one; call counts once by (workflow_id, call_id)
4. **Decimal rounding**: ROUND_HALF_UP (financial standard)
5. **JSON key order**: Contract key order preserved; dynamic rejection reasons sorted alphabetically
6. **Event phone validation**: Invalid phone on otherwise valid event → store event without phone HMAC, leave pending (contributes to orphan)
7. **Call-link conflicts**: First valid association wins; no silent reassignment
8. **Default values**: Applied in report/domain projection, not written as source data into stored leads
9. **Event timestamp contract**: Events carry `disposition_code` (flat string) and `occurred_at`, matching the supplied generator. An event without a parseable `occurred_at` cannot become an immutable ledger fact and is counted as malformed; this prevents a single bad line from aborting the whole file transaction. The assignment does not define a separate counter for this case, so no new output keys were invented.

---

## Correctness Verification

### Tests run

- [x] All unit tests pass (`python -m pytest -q`)
- [x] Small seed (42) end-to-end test passes
- [x] Second seed (123) test passes
- [x] Idempotency tests pass (re-running produces byte-identical reports and unchanged DB row counts)
- [x] Parent-folder ingestion (`data/`) produces byte-identical reports to drop-by-drop ingestion (verified on seeds 42 and 123)
- [x] Empty-input tests pass (empty directory, empty JSONL, header-only CSV)
- [x] Final release audit regression suite (15 tests) covering the audit fixes below
- [x] Third seed (7) end-to-end run verified in a fresh virtual environment with a fresh database (results below)

### Test results

```
============================= test session starts ==============================
platform win32 -- Python 3.12.6, pytest-9.2.0, pluggy-1.7.0
rootdir: D:\AgenticUniverse Submission
collected 182 items

182 passed in 7.63s
```

**Warning summary:** The suite previously emitted Python 3.12 `DeprecationWarning`s from `solution/db.py` (sqlite3's default datetime adapter, deprecated as of Python 3.12). This was fixed by registering an explicit datetime adapter that reproduces the exact legacy storage format — no data-format change, no behavior change. The suite now completes with **zero warnings**.

**182 tests passing across:**
- 53 normalization tests
- 19 security tests
- 22 database tests
- 30 lead ingestion tests
- 27 event ingestion tests
- 16 report tests
- 15 final-audit regression tests

### Data generation and ingestion

Seed used: 42, Scale: small

Commands run:
```powershell
python assignment-a-data\tools\generate_data.py --seed 42 --scale small --out data
python -m solution.cli ingest data\drop_1
python -m solution.cli ingest data\drop_2
python -m solution.cli report --out reports
```

**Generated data characteristics (seed 42):**
- drop_1: batch_001.csv (6 rows incl. blanks), batch_feedback_001.csv (7 rows), 35 + 38 event lines
- drop_2: batch_003.csv (8 rows, alias headers), 33 event lines

**Ingestion results (verified by re-running the documented commands):**
- Lead rows: 14 non-blank rows read, 0 rejected
- Events: 106 non-empty lines, 7 malformed, 29 duplicates dropped (70 unique events stored)
- Duplicate leads merged: 1
- Field warnings: 8 (3 of these are decimal-comma values like `312,94` now correctly flagged as invalid — see audit fix 9)
- Orphan events (current, after reconciliation): 15
- Unknown disposition events: 6 (policy_feedback disposition events carrying codes from the motor_renewal-only catalogue)

### Seed 7 — fresh-environment verification (executed 13 November 2026)

Reproduced end to end in a newly created virtual environment (only `requirements.txt` dependencies installed) with a fresh database:

```powershell
python -m venv venv7
venv7\Scripts\python.exe -m pip install -r requirements.txt
python assignment-a-data\tools\generate_data.py --seed 7 --scale small --out data7
python -m solution.cli ingest data7\drop_1
python -m solution.cli ingest data7\drop_2
python -m solution.cli report --out reports7
python -m pytest -q   # 182 passed in the fresh environment as well
```

**Results:** 14 lead rows read, 0 rejected, 1 merged; 97 event lines, 6 malformed, 21 duplicates dropped; 10 field warnings; 15 orphan events (current); 4 unknown-disposition events. All five reports generated; the full test suite (182 tests) also passed in the fresh environment.

Note: the actual run used a temporary working directory with `DB_PATH` pointing at a fresh database file; the commands above are the equivalent form for the repository layout. This was executed twice: first with a manual `tzdata` install (which exposed the missing dependency), then again after `tzdata` was added to `requirements.txt` — the second run succeeded using `requirements.txt` alone.

### Final release audit fixes (13 November 2026)

As a final step, I led a full engineering audit of the delivered system — re-deriving expected counters from the raw input files, inspecting the database directly, and re-checking every output against the contract. This surfaced the verified defects below, which I fixed:

1. **P0 — disposition codes were silently dropped.** The event parser read a nested `disposition` key while the generator contract emits a flat `disposition_code` string. All disposition-dependent reporting (completed bucket, success_leads, current_disposition, ever_success, unknown_disposition_events) was broken. Fixed to read `disposition_code` first, with the nested form as a defensive fallback.
2. **P0 — trailing JSONL counters were lost.** Per-file event counters were only persisted when a valid event was inserted, so malformed/duplicate lines at the end of a file were lost (observed: 104/27 instead of 106/29). Fixed by persisting counters after the whole file is read.
3. **P1 — field warnings on rejected rows.** Invalid optional fields on rows that were subsequently rejected incremented `field_warnings`; the spec says rejected rows add no warnings. Fixed.
4. **P1 — lead_rows_total semantics.** Counted only accepted rows; the spec defines it as all non-blank lead rows read (rejected included). Fixed.
5. **P1 — zero-lead batches missing from batch_summary.csv.** Ingested batches whose rows were all rejected now appear with `total_leads=0`, matching the documented assumption.
6. **P1 — invalid event timestamp aborted the whole file.** A structurally valid event with an unparseable `occurred_at` rolled back the entire file transaction, silently losing all valid events. Such events are now counted as malformed (documented below as an assumption).
7. **P2 — crash-recovery gap.** A manifest row left in `pending` status by a crash was permanently skipped; it is now reprocessed (semantic deduplication keeps re-ingestion safe).
8. **P2 — missing `.env.example`.** Created with placeholders and key-generation instructions (the README references it).
9. **P2 — malformed decimal grouping silently accepted.** `normalize_decimal` stripped all commas before parsing, so `1,2,3` became the valid value `123`. Commas must now form valid thousands-separator groups — Western (`1,234,567`) or Indian (`1,23,456`) — and malformed grouping is rejected like any other invalid decimal. This also correctly flags the generator's decimal-comma values (`312,94`, `184,07`, `223,11`), which were previously mis-parsed as `31294.00` etc.; they are now invalid-optional-decimal warnings, so the verified seed-42 `field_warnings` count is 8 (was 5 under the lenient behavior).
10. **P2 — CLI reported a stale unknown-disposition count.** The ingest command's "Unknown dispositions" display used the final reconciliation pass's delta (always 0 when unknowns were already marked during per-file reconciles) instead of the live count, showing 0 while `data_quality.json` correctly reported 6. The reconciliation summary now carries a live snapshot, and both CLI display paths use it.
11. **P2 — missing `tzdata` dependency.** A fresh Windows environment failed with `ZoneInfoNotFoundError: 'No time zone found with key Asia/Kolkata'` because Python's `zoneinfo` requires the `tzdata` pip package, which was absent from `requirements.txt` (the development environment had it installed globally, masking the gap). Fixed by adding `tzdata` to `requirements.txt`; verified end-to-end in a newly created virtual environment using `requirements.txt` alone (seed-7 flow and 182 tests all pass, with a fresh database).
12. **P2 — Python 3.12 sqlite3 datetime-adapter DeprecationWarnings.** Datetime values bound to SQLite queries via the deprecated default adapter emitted warnings from `solution/db.py` on every event insert. Fixed by registering an explicit adapter that reproduces the exact legacy storage format (verified: identical `data_quality.json` output before and after); the test suite now runs with zero warnings.

Fixes 1–10 each have a regression test in `tests/test_final_audit_regressions.py`; fixes 11–12 were verified by the fresh-environment end-to-end runs described above.

---

## Security

### PII handling

- [x] No plaintext PII in database (verified by automated raw-byte scan test)
- [x] No plaintext PII in output/logs (verified by code review of all output paths: the CLI prints counters and file paths only; no logging framework is used, and no PII value is passed to any print/output call)
- [x] No plaintext PII in generated reports (verified by automated report-scan tests)
- [x] Phone matching uses HMAC, not plaintext comparison (schema and code review)
- [x] Encryption keys from environment, not hardcoded (code review; keys generated via helpers in `security/pii.py`)
- [x] .env file not committed to Git (verified: no `.env` exists or is tracked; `.gitignore` excludes `.env`)

**Verified evidence by inspection method:**
- Database inspection (automated raw-byte scan test): Leads table — PII in encrypted `pii_ciphertext` blob only, phone stored as HMAC only; Events table — phone stored as HMAC only, no plaintext phone
- Report inspection (automated tests): phone masked as `+91******XXXX`; no plaintext customer names
- Schema review: Rejections table has no PII columns — reason/row_number only
- Code review: no logging framework in `solution/`; CLI output limited to counters and file paths

### Key configuration

- **Fernet key (ENCRYPTION_KEY)**: 32-byte URL-safe base64-encoded; used for PII encryption
- **HMAC key (HMAC_KEY)**: High-entropy secret; used for phone HMAC with workflow separation
- Keys loaded from environment variables or .env file via python-dotenv
- Tests use deterministic, test-only cryptographic keys isolated from production configuration. No production encryption or matching keys are included in the repository.

---

## Known Limitations

1. **Call-link conflicts**: If same (workflow_id, call_id) maps to two different leads, first wins; conflicting association not included in reports. Generator doesn't currently produce this case.
2. **Multiple call.initiated**: If duplicates exist, only first is used for initiation time. Generator behavior should be verified.
3. **No performance benchmarking**: Not run; no timing claims made.
4. **Windows-specific paths in examples**: PowerShell commands are primary; macOS/Linux equivalents are straightforward path equivalents.
5. **Transient test resources**: Some edge-case tests use in-memory patterns; comprehensive adversarial coverage could be expanded.
6. **Manifest atomicity**: Manifest creation and data processing are not committed in one atomic transaction. If interrupted, pending files are safely reprocessed on restart, with semantic deduplication preventing duplicate records (verified by the `test_pending_file_reprocessed` regression test).

---

## Files Modified/Created

### Solution files (all in solution/)

- `cli.py` - CLI entry point (ingest/report commands)
- `config.py` - Configuration loading with get_db_path/get_data_path helpers
- `db.py` - Database initialization, repositories, triggers, get_aggregated_stats
- `schema_registry.py` - Dynamic schema/disposition loading
- `domain/calculations.py` - Pure domain projection (CallFact, LeadFact, DailyOverviewFact, DomainProjection)
- `ingestion/discovery.py` - File discovery, FileIdempotencyChecker
- `ingestion/normalization.py` - Header, phone, date, decimal, enum, timestamp normalizers
- `ingestion/leads.py` - Lead CSV ingestion with validation, deduplication, merge
- `ingestion/events.py` - Event JSONL ingestion, reconciliation service
- `reporting/writers.py` - Deterministic report writers for all 5 files
- `security/pii.py` - Fernet encryption, HMAC computation

### Test files (all in tests/)

- `conftest.py` - Pytest fixtures
- `test_normalization.py` - 53 normalizer tests
- `test_security.py` - 19 security tests
- `test_database.py` - 22 database tests
- `test_leads_ingestion.py` - 30 lead ingestion tests
- `test_events_ingestion.py` - 27 event ingestion tests
- `test_reports.py` - 16 report tests
- `test_final_audit_regressions.py` - 15 regression tests for the final-audit fixes

### Other files

- `.env.example` - Environment variable template with placeholders and key-generation instructions
- `requirements.txt` - `tzdata` dependency added (audit fix 11)
- `SUBMISSION.md` - This file
- `AI_USAGE.md` - AI usage disclosure

---

## Original Assignment Files Preserved

- [x] `assignment-a-data/` unchanged (schemas, generator, README) — verified: no modifications in git
- [x] `docs/GLOSSARY.md` unchanged — verified: no modifications in git
- [x] `.gitignore` unchanged (existing rules preserved; generated data, DBs, reports, `.env` are ignored)
- [ ] `requirements.txt` — one change: `tzdata>=2024.1` added to fix the fresh-environment `ZoneInfoNotFoundError` (audit fix 11); no other dependencies added or removed

**File categories:**

- **Original assignment-provided files (untouched)**: `assignment-a-data/`, `docs/GLOSSARY.md`, `.gitignore`
- **Original repository files with one disclosed change**: `requirements.txt` (`tzdata` dependency added)
- **Candidate-authored documentation**: root `README.md` (intentionally redesigned during the final documentation pass to describe the delivered system; the pre-redesign version remains recoverable in git history — this was a rewrite of a candidate-authored README, not a modification of an assignment-provided specification), `SUBMISSION.md`, `AI_USAGE.md`
- **Candidate-authored implementation**: `solution/`, `tests/`, `.env.example`, `tests/test_final_audit_regressions.py`

---

## Checklist Summary

- [x] Implementation matches assignment contract
- [x] Tests pass on unseen seeds (42, 123, and 7 tested)
- [x] PII properly encrypted
- [x] Idempotent ingestion works (verified byte-identical reports on rerun)
- [x] Reports match exact format specifications
- [x] Submission documentation complete
- [ ] GitHub repository is private — candidate action, pending (a local git repository exists; the remote repository's visibility must be set to private by the candidate)
- [ ] Video assessment completed before submission

---

## Clean-Clone Verification Commands

```powershell
# Setup (Python 3.11 or newer required)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Configure keys (in .env or environment)
# ENCRYPTION_KEY=<32-byte-base64>
# HMAC_KEY=<high-entropy-secret>

# Generate data
python assignment-a-data\tools\generate_data.py --seed 42 --scale small --out data

# Ingest
python -m solution.cli ingest data\drop_1
python -m solution.cli ingest data\drop_2

# Report
python -m solution.cli report --out reports

# Test
python -m pytest -q
```

All commands verified working from a clean working copy, and the seed-7 end-to-end flow plus the full test suite were additionally verified in a freshly created virtual environment with a fresh database using `requirements.txt` alone (see the Seed 7 section).
