# Glossary — Agentic Universe Assignment A

## Terms

**Workflow**: A business process with a schema defining acceptable lead fields and a disposition catalogue. Examples: `motor_renewal`, `policy_feedback`.

**Lead**: A potential customer record identified by `(workflow_id, canonical_phone)`. Leads are created from CSV batch files and may be updated (merged) by later batches.

**Batch**: A single CSV file containing lead records. Identified by its file stem (filename without extension). The batch_id is permanently attached to the lead created from the first valid row in that batch.

**Call**: A telephone interaction identified by `(workflow_id, call_id)`. A call exists only when a `call.initiated` event is linked to a lead.

**Event**: An immutable fact about a call or disposition. Events are stored in the event ledger and must not be modified.

**Event Type**: Categories of events:
- `call.initiated` — Call started
- `call.telephony_failed` — Call failed at telephony level
- `call.connected` — Call connected to agent
- `call.ended` — Call ended
- `disposition.set` — Agent set a disposition
- `disposition.override` — Human overrode the disposition

**Disposition**: A categorical outcome for a call, defined in `dispositions.csv`. Each disposition has:
- `code`: Unique identifier
- `workflow`: Which workflow it applies to
- `terminal`: Whether this disposition ends the call cycle
- `success`: Whether this disposition is considered successful

**Effective Disposition**: The disposition that applies to a call after considering overrides:
1. Latest `disposition.override` by timestamp (if any exists)
2. Otherwise, latest `disposition.set` by timestamp
3. Otherwise, none

**Lead Bucket**: Classification of a lead based on call activity:
1. `yet_to_reach_out` — No calls
2. `completed` — Any call has a terminal disposition
3. `in_progress_telephony_failed` — All calls have telephony failure
4. `in_progress_dialed` — Otherwise

**PII (Personally Identifiable Information)**: Fields marked `pii: true` in the schema (customer_name, phone). These must be encrypted at rest.

**Phone HMAC**: A deterministic, keyed hash of the canonical phone number, used for matching without revealing the plaintext phone.

**Orphan Event**: An event that cannot yet be associated with a lead. These remain pending and can be resolved when a matching lead arrives in a later drop.

**Unknown Disposition**: A disposition code that is not in the workflow's disposition catalogue. Events with unknown dispositions are excluded from reports after association.

**IST**: India Standard Time (Asia/Kolkata, UTC+5:30). Naive timestamps are interpreted as IST.

**UTC**: Coordinated Universal Time. All timestamps are normalized to UTC for storage and comparison.

**Canonical Phone**: A normalized phone number in the format `+91XXXXXXXXXX` (10 digits after `+91` country code).

## File Types

**CSV**: Comma-separated values file containing lead records.

**JSONL**: JSON Lines file where each line is a valid JSON object representing an event.

**Schema**: JSON file defining the fields, types, aliases, and constraints for a workflow's lead data.

**Dispositions CSV**: CSV file mapping disposition codes to their properties (terminal, success) per workflow.
