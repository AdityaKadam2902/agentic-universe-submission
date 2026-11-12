# Agentic Universe — Assignment A: Interaction Ledger & Reporting Pipeline

## Overview

Build a privacy-preserving data ingestion pipeline that processes lead CSV batches and call-event JSONL files, persists facts safely in SQLite, reconciles late-arriving records, and generates five exact reports.

## Data Layout

```
data/
├── drop_1/
│   ├── leads/
│   │   ├── motor_renewal/
│   │   │   ├── batch_001.csv
│   │   │   └── batch_002.csv
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

### Workflow Identification

- **Leads**: Workflow ID is determined by the parent directory name under `leads/`. Batch ID is the file stem (filename without extension).
  - Example: `data/drop_1/leads/motor_renewal/batch_001.csv` → workflow=`motor_renewal`, batch=`batch_001`
- **Events**: Workflow ID is determined by the JSONL filename stem (without `.jsonl` extension).
  - Example: `data/drop_1/events/motor_renewal.jsonl` → workflow=`motor_renewal`

### Processing Order

1. Process ingestion commands in the order they are invoked
2. Within each invocation, process files in deterministic order:
   - For folders: sort by relative path
   - For leads: sort by directory path then filename
   - For events: sort by filename

## Schemas

Two workflows are defined:

### motor_renewal

| Field | Type | PII | Required | Default | Reference |
|-------|------|-----|----------|---------|-----------|
| customer_name | string | yes | yes | - | no |
| phone | phone | yes | yes | - | no |
| policy_number | string | no | yes | - | yes |
| policy_expiry | date | no | no | null | no |
| premium_amount | decimal | no | no | null | no |
| preferred_language | enum | no | no | en | no |

### policy_feedback

| Field | Type | PII | Required | Default | Reference |
|-------|------|-----|----------|---------|-----------|
| customer_name | string | yes | yes | - | no |
| phone | phone | yes | yes | - | no |
| order_id | string | no | yes | - | yes |
| product | string | no | no | null | no |
| order_date | date | no | no | null | no |
| preferred_language | enum | no | no | en | no |

## Reports

The `report` command generates five files:

1. **batch_summary.csv** — Aggregate metrics per workflow/batch
2. **daily_overview.csv** — Daily call metrics per workflow
3. **lead_report.csv** — Per-lead details
4. **rejections.csv** — Rejection log (no PII)
5. **data_quality.json** — Ingestion statistics

See the implementation blueprint for exact specifications.
