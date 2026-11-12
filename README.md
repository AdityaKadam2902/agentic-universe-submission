# Agentic Universe — Assignment A: Interaction Ledger & Reporting Pipeline

## Overview

This repository contains the implementation for Assignment A of the Agentic Universe engineering assessment. The project builds a privacy-preserving data ingestion pipeline that:

1. Processes lead CSV batches and call-event JSONL files
2. Persists facts safely in SQLite with PII encryption
3. Reconciles late-arriving records
4. Generates five exact reports

## Repository Structure

```
.
├── README.md                          # This file
├── ASSIGNMENT_A_BLUEPRINT.md          # Implementation guide
├── requirements.txt                   # Python dependencies
├── .env.example                       # Environment variable template
├── SUBMISSION.md                      # Submission documentation
├── AI_USAGE.md                        # AI usage disclosure
├── assignment-a-data/                 # Supplied assignment data (DO NOT MODIFY)
│   ├── README.md
│   ├── schemas/
│   │   ├── motor_renewal.json
│   │   ├── policy_feedback.json
│   │   └── dispositions.csv
│   └── tools/
│       └── generate_data.py
├── docs/
│   └── GLOSSARY.md
└── solution/                          # Your implementation
    ├── __init__.py
    ├── cli.py                         # CLI entry point
    ├── config.py                      # Configuration loading
    ├── db.py                          # Database initialization
    ├── schema_registry.py             # Schema loading and validation
    ├── domain/
    │   ├── models.py                  # Domain models
    │   └── rules.py                   # Business rules
    ├── security/
    │   ├── __init__.py
    │   └── pii.py                     # PII encryption and HMAC
    ├── ingestion/
    │   ├── __init__.py
    │   ├── discovery.py               # File discovery
    │   ├── normalization.py           # Data normalizers
    │   ├── leads.py                   # Lead ingestion
    │   ├── events.py                  # Event ingestion
    │   └── reconciliation.py          # Late-event reconciliation
    └── reporting/
        ├── __init__.py
        ├── calculations.py            # Derived calculations
        └── writers.py                 # Report writers
```

## Quick Start

### Setup

```powershell
# Create virtual environment
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Configure secrets (required)
# Generate keys and set in .env or environment variables
```

### Generate Test Data

```powershell
python assignment-a-data\tools\generate_data.py --seed 42 --scale small --out data
```

### Ingest Data

```powershell
python -m solution.cli ingest data\drop_1
python -m solution.cli ingest data\drop_2
```

### Generate Reports

```powershell
python -m solution.cli report --out reports
```

### Run Tests

```powershell
python -m pytest -q
```

## Data Model

### Workflows

Two workflows are defined:
- **motor_renewal**: Motor policy renewal leads
- **policy_feedback**: Policy feedback collection leads

### Lead Schema Fields

See `assignment-a-data/schemas/` for exact field definitions.

### Event Types

- `call.initiated` — Call started
- `call.telephony_failed` — Call failed
- `call.connected` — Call connected
- `call.ended` — Call ended
- `disposition.set` — Disposition set by agent
- `disposition.override` — Disposition overridden by human

## Reports

The pipeline generates five reports:
1. `batch_summary.csv` — Batch-level aggregates
2. `daily_overview.csv` — Daily call metrics
3. `lead_report.csv` — Per-lead details
4. `rejections.csv` — Rejection log (no PII)
5. `data_quality.json` — Ingestion statistics

See the Assignment A README and blueprint for exact specifications.

## Privacy

All PII fields (customer_name, phone) are encrypted at rest using authenticated encryption. Phone numbers are matched using deterministic HMAC-SHA256 digests. No plaintext PII is stored in the database or logs.

## License

Private repository for Agentic Universe assessment submission.
