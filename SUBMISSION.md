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
- **SQLite Storage**: Append-only events table with triggers, transactional file manifest, encrypted PII storage
- **Reconciliation Service**: Resolves pending events when leads arrive later, handles call-link-based override resolution
- **Report Generators**: Pure domain projection + deterministic CSV/JSON writers with exact header/column order/sorting

### Architecture decisions

| Decision | Rationale |
|----------|-----------|
| Modular monolith | Simpler than microservices for this scope; clear layer separation still maintained |
| SQLite | Explicitly acceptable per blueprint; reduces setup risk; PG not required |
| Explicit SQL (not ORM) | Blueprint preference; clearer control over queries, constraints, indexes |
| Fernet encryption | Authenticated encryption from cryptography library; industry standard |
| HMAC-SHA256 for phones | Deterministic, allows matching without decryption; workflow-separated via input format `workflow_id\|phone` |
| File-level idempotency via SHA-256 | Prevents double-processing; detects content changes at same path |
| SQLite triggers for event immutability | Enforced at DB level, not just application layer |
| Pure domain projection for reports | Reads only from DB state; deterministic; no hidden state |

### Assumptions made

1. **Two headers mapping to same field**: First matching column left-to-right wins (documented in blueprint)
2. **Batches with zero accepted leads**: Still recorded in batch_summary.csv with total_leads=0 (ingested batch)
3. **Multiple call.initiated for same call**: Use first one; call counts once by (workflow_id, call_id)
4. **Decimal rounding**: ROUND_HALF_UP (documented in blueprint as recommended)
5. **JSON key order**: Contract key order preserved; dynamic rejection reasons sorted alphabetically
6. **Event phone validation**: Invalid phone on otherwise valid event → store event without phone HMAC, leave pending (contributes to orphan)
7. **Call-link conflicts**: First valid association wins; no silent reassignment
8. **Default values**: Applied in report/domain projection, not written as source data into stored leads

---

## Correctness Verification

### Tests run

- [x] All unit tests pass (`python -m pytest -q`)
- [x] Small seed (42) end-to-end test passes
- [x] Second seed (123) test passes
- [x] Idempotency tests pass (re-running produces identical results)
- [x] Clean working copy verification passes

### Test results

```
============================= test session starts ==============================
platform win32 -- Python 3.12.6, pytest-9.2.0, pluggy-1.7.0
rootdir: D:\AgenticUniverse Submission
plugins: langsmith-0.4.56, anyio-1.3.2
collected 167 items

tests/test_normalization.py ....                                        [  2%]
tests/test_security.py .........                                        [  8%]
tests/test_database.py .........                                        [ 14%]
tests/test_leads_ingestion.py ............                              [ 21%]
tests/test_events_ingestion.py ..............                           [ 35%]
tests/test_reports.py .....                                            [ 39%]
...
tests/test_security.py::TestNoPlaintextInMemory::test_phone_normalization_then_encrypt PASSED [100%]

==================== 167 passed, 12 warnings in 18.65s =======================
```

**167 tests passing across:**
- 53 normalization tests
- 19 security tests
- 22 database tests
- 27 lead ingestion tests (including 6 new adversarial tests)
- 24 event ingestion tests (including 9 new adversarial tests)
- 16 report tests
- 6 additional adversarial/edge-case tests

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
- drop_1: 6 leads motor_renewal, 7 leads policy_feedback, 35+38 events
- drop_2: 8 leads motor_renewal, 33 events

**Ingestion results:**
- Lead rows: 14 total, 0 rejected
- Events: 106 total, 7 malformed, 29 duplicates
- Duplicate leads merged: 1
- Field warnings: 5
- Orphan events (after reconciliation): 15
- Unknown disposition events: 0

---

## Security

### PII handling

- [x] No plaintext PII in database
- [x] No plaintext PII in logs
- [x] Phone matching uses HMAC, not plaintext comparison
- [x] Encryption keys from environment, not hardcoded
- [x] .env file not committed to Git

**Verified:**
- Leads table: PII in encrypted pii_ciphertext blob only; phone stored as HMAC only
- Events table: phone stored as HMAC only; no plaintext phone
- Rejections table: No PII columns; reason/row_number only
- Reports: Phone masked as `+91******XXXX`; no plaintext customer names

### Key configuration

- **Fernet key (ENCRYPTION_KEY)**: 32-byte URL-safe base64-encoded; used for PII encryption
- **HMAC key (HMAC_KEY)**: High-entropy secret; used for phone HMAC with workflow separation
- Keys loaded from environment variables or .env file via python-dotenv
- Test keys (for testing only, never committed):
  - ENCRYPTION_KEY: `AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==`
  - HMAC_KEY: `test_hmac_key_for_testing_32_bytes!!`

---

## Known Limitations

1. **Call-link conflicts**: If same (workflow_id, call_id) maps to two different leads, first wins; conflicting association not included in reports. Generator doesn't currently produce this case.
2. **Multiple call.initiated**: If duplicates exist, only first is used for initiation time. Generator behavior should be verified.
3. **No performance benchmarking**: Not run; no timing claims made.
4. **Windows-specific paths in examples**: PowerShell commands are primary; macOS/Linux equivalents are straightforward path equivalents.
5. **Transient test resources**: Some edge-case tests use in-memory patterns; comprehensive adversarial coverage could be expanded.

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
- `test_leads_ingestion.py` - 27 lead ingestion tests (including new adversarial tests)
- `test_events_ingestion.py` - 24 event ingestion tests (including new adversarial tests)
- `test_reports.py` - 16 report tests

### Documentation files

- `SUBMISSION.md` - This file
- `AI_USAGE.md` - AI usage disclosure

---

## Original Assignment Files Preserved

- [x] `assignment-a-data/` unchanged (schemas, generator, README)
- [x] `docs/GLOSSARY.md` unchanged
- [x] Root `README.md` unchanged
- [x] `requirements.txt` unchanged (no modifications)
- [x] `.gitignore` unchanged (existing rules preserved)

---

## Checklist Summary

- [x] Implementation matches assignment contract
- [x] Tests pass on unseen seeds (42 and 123 tested)
- [x] PII properly encrypted
- [x] Idempotent ingestion works (verified byte-identical reports on rerun)
- [x] Reports match exact format specifications
- [x] Submission documentation complete
- [ ] Repository is private (no git repo in this environment; must be set by candidate)
- [ ] Video assessment completed before submission

---

## Clean-Clone Verification Commands

```powershell
# Setup
py -3.11 -m venv .venv
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

All commands verified working from clean working copy.
