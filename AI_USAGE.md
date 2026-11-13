# AI Usage Disclosure — Agentic Universe Assignment A

## Summary

I led the architecture and engineering direction of this project, established the requirements and correctness criteria, evaluated design options, and made and approved the key implementation decisions with AI assistance. I used AI coding assistants as development tools throughout — substantially, and by design. Their output was never treated as automatically correct: I directed their work, reviewed changes, compared behavior against the assignment specification and the supplied data generator, and used regression testing to identify and correct incorrect assumptions.

The result is a system whose architecture, core workflows, correctness invariants, major engineering decisions and testing approach I can explain and defend.

---

## My contribution

### Architecture and design
- Led the design of the **layered modular monolith**: CLI → ingestion orchestrator → pure domain services → repositories → SQLite, with reconciliation and reporting as deterministic projections over an immutable event ledger — evaluating trade-offs and approving the architecture with AI assistance in exploring options.
- Established the **PII strategy**: Fernet authenticated encryption for `pii: true` fields, deterministic workflow-scoped HMAC-SHA256 digests for phone matching, strict separation of encryption and matching keys, and a hard rule that no plaintext PII ever reaches the database or reports.
- Approved the **two-layer event model** (immutable facts + mutable association projection) that makes late reconciliation possible without ever rewriting history.

### Problem solving
- Drove a **final engineering audit** that surfaced and fixed a set of subtle correctness defects (detailed in the next section). Each fix ships with a regression test.
- Resolved specification ambiguities by directing investigation against the supplied generator and fixtures rather than guessing (documented in `SUBMISSION.md`).
- Established the empirical security verification approach — scanning raw database bytes for plaintext PII across multiple phone formats instead of trusting the encryption library's presence.

### Verification
- I directed the verification process, reviewed the actual command outputs, examined the generated test evidence, and checked the implementation against the assignment's schemas and data generator.
- Verification coverage included the full test suite, three independent data seeds (42, 123, 7), byte-identical rerun checks, parent-folder vs drop-by-drop equivalence, and clean-environment setup verification.
- Confirmed all five report outputs against the exact contract: headers, sort order, formatting, timezone semantics, and empty-output behavior.

---

## Major engineering problems I solved

The hardest bugs in this system were not crashes — they were **silent wrong answers**: the pipeline reported success while producing subtly incorrect results. Finding these required reasoning about the *contract*, not just the code.

### 1. A contract mismatch that poisoned every disposition metric
**Symptom:** Every lead reported `ever_success=false`, no bucket ever reached `completed`, `success_leads` was always 0 — despite the generator emitting valid disposition events.
**Why it was hard to spot:** The event parser read a nested `disposition` key while the real generator contract emits a flat `disposition_code` string. Worse, **the test suite encoded the same wrong assumption** — the fixtures used the same imaginary format, so a fully green test suite was *self-confirming* rather than validating the contract.
**How I caught it:** Instead of trusting the tests, I cross-checked the parser against the authoritative source of truth (the supplied generator) and ran the real end-to-end pipeline, then inspected the database directly: 17 disposition events stored, **zero** disposition codes persisted.
**Fix and impact:** Corrected the extractor (with a defensive fallback), re-aligned every test fixture to the true contract, and added regression tests. This single defect had invalidated five of the assignment's core metrics.

### 2. Counters that quietly lost data at file boundaries
**Symptom:** `data_quality.json` reported 104 events ingested; the input files contained 106 non-empty lines.
**Why it was hard to spot:** Per-file counters were only persisted on the code path that inserts a *valid* event. Any file ending with malformed or duplicate lines silently under-reported — off-by-a-few errors that look like rounding but are data loss in an audit trail.
**Fix:** Counters now persist once after the entire file is read, inside the same transaction. Verified against exact line counts on two seeds.

### 3. One bad timestamp poisoning an entire file
**Symptom:** Ingesting a file containing one structurally-valid event with an unparseable `occurred_at` produced **zero** events, zero counters, no manifest entry — the whole file vanished.
**Why it was hard to spot:** The failure was a `NOT NULL` constraint violation rolling back the entire file transaction, swallowed by a catch-all handler.
**Complexity of the decision:** The specification does not explicitly define a separate classification for structurally valid events with unparseable timestamps. An event without a parseable timestamp *cannot* become an immutable ledger fact — the ledger's ordering model depends entirely on UTC-normalized times. I documented the chosen handling (classify as malformed) as an implementation assumption and added regression coverage proving the file-loss scenario was gone.

### 4. A crash-recovery window that could permanently swallow a file
**Failure mode (by construction):** If a process died between the manifest commit and the data transaction, the file was recorded as ingested but contained nothing — and every future rerun **skipped it forever**.
**Why it matters:** In an idempotent-by-design system, the worst failure mode is not reprocessing — it is *silently never processing*.
**Fix:** Pending manifest rows are now reprocessed on restart; semantic deduplication (lead identity, global `event_id`) guarantees the reprocessing is safe. Also fixed a latent `NameError` that would have crashed the recovery path itself.

### 5. Statistical semantics that drifted from the specification
Two related defects in how quality counters were defined:
- `field_warnings` was incremented during normalization **before** validation, so a row rejected for a missing required field also contributed a warning — the spec explicitly says rejected rows add none.
- `lead_rows_total` counted only *accepted* rows, while the spec defines it as all non-blank rows *read*.

Both are the kind of one-line discrepancies that survive casual review but change audit numbers on real data. A third one in the same family: `normalize_decimal` stripped all commas before parsing, so malformed grouping such as `1,2,3` silently became the valid value `123` instead of being rejected. All fixed, with tests covering the mixed accepted/rejected file case and the grouping-validation cases.

### 6. Making invisibility a test, not a hope
Beyond fixing defects, I turned the security claim into an executable fact: regression tests now scan the **raw database bytes** for known plaintext names and twenty phone-format variants, so "no PII at rest" is something the suite proves on every run — not a sentence in a README.

---

## How I used AI

I used AI coding assistants as development tools throughout this project. I remained responsible for the project direction, requirements interpretation, architectural decisions, review of implementation choices, and final acceptance.

AI tools contributed substantially to implementation and test scaffolding. I did not treat their output as automatically correct. I directed their work through explicit requirements, reviewed implementation changes, compared behavior against the original assignment specifications and supplied data generator, and used regression testing to identify and correct incorrect assumptions.

| Area | My responsibility | AI assistance |
|---|---|---|
| Architecture and data model | Led design decisions, evaluated trade-offs and approved the architecture | Helped explore options and draft design proposals |
| Correctness requirements | Interpreted the assignment and established acceptance criteria | Helped map requirements to implementation tasks |
| Specification ambiguities | Directed investigation using the supplied schemas, generator and fixtures; approved documented assumptions | Assisted with source analysis and alternative interpretations |
| Implementation | Directed implementation, reviewed changes and approved corrections | Generated substantial portions of application code |
| Defect investigation | Prioritized issues, evaluated root causes and directed fixes | Assisted with code analysis, debugging and implementation of fixes |
| Testing | Defined important scenarios, reviewed test coverage and evaluated results | Generated test scaffolding and regression tests, and assisted with test execution |
| Security verification | Established the security requirements and reviewed verification evidence | Assisted with security checks and automated test execution |
| Documentation | Reviewed and finalized project documentation and claims | Assisted with drafting and organization |

**Tools used:** Freebuff CLI for AI-assisted implementation, debugging and test generation; OpenCode for independent code review. The models used were those selected in the respective tool sessions.

**Engineering guardrails:**

- No plaintext PII in database storage or generated reports.
- No secrets or cryptographic keys committed to source control.
- No hardcoded dataset-specific outputs or fabricated test results.
- Original assignment specifications and supplied generator files remained untouched.
- Important implementation claims were checked against actual test results and generated data.

AI-assisted development was a substantial part of this project, and I disclose that explicitly. My responsibility was to direct the engineering work, evaluate the generated implementation, validate correctness and understand the resulting system well enough to explain its design and behavior.

---

## Honest assessment

I used AI substantially in this project, and that is exactly how I would work on any production team. What I retained was engineering responsibility: directing the work, checking the AI's mistakes — including the self-confirming test suite described above — validating correctness against the assignment contract, and understanding the system well enough to explain and defend it. I would not submit code I could not explain: every architectural decision, every fix, and every number in this repository's documentation is something I can walk a reviewer through.

**Final verified state**: 182/182 tests passing, three independent seeds, byte-identical idempotent reruns, no plaintext PII in database storage or generated reports.
