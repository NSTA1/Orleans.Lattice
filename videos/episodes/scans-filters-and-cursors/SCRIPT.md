# Scans, filters and cursors - script

Status: fact-checked on 2026-10-26 by the Docs agent, cue by cue, against `docs/lattice/predicated-operations.md`, `docs/lattice/api.md`, `videos/series.md` and `videos/series.json`. The predicate cue now clarifies that independent sub-expressions are evaluated on the client; the recovery cue says it resumes after, not on, the last yielded key; and the scan cue describes the resilient key and entry scans without abbreviating their API names.

Written form, in the series voice (Emma, British English). Each paragraph under "Narration" is one cue, and each cue is a beat its scene can move on. A `pause` comment adds silence before the next cue or after the last one.

## Narration

### Scans, filters and cursors

Values that merge showed how the value decides what concurrent writes mean. Here, we choose which keys to read, and how to keep the walk going.

This is Scans, filters and cursors: ordered ranges, server-side predicates, and resumable pages.

### An ordered range

A streaming scan walks live keys in strict lexicographic order.

Give it a lower bound, an upper bound, or both. The lower bound is included; the upper bound is not.

A reverse scan walks that same range from high to low.

### Filter at the leaf

A typed scan can also take a predicate: a C-sharp expression over the value.

The client translates that expression to a small, serialisable rule. An unsupported expression fails before any request reaches the cluster.

Each leaf checks its own values against their JSON document. Only matches are returned.

A key-only scan sends no values across the wire. An entry scan returns only matching key-value pairs.

The allowlist covers member access, constants, comparisons, boolean logic, and ordinal string checks. Conversions are unwrapped, and expressions independent of the value are evaluated once on the client.

A method call on the value outside that set is not silently sent to the server. It is rejected.

### Resilient streams

For a long streaming walk, use the resilient key or entry scans, not the raw enumeration methods.

If the remote enumeration is reclaimed, the resilient wrapper reconnects with the filter intact and resumes after the last key it yielded.

Recovery is bounded. If attempts run out, it throws instead of returning a short range as complete.

### Pages that survive

When you need explicit pages, open a cursor. It returns an opaque identifier; the server checkpoints progress after each page.

Keep that identifier in your own durable state. A cursor can resume after a silo failover or client restart.

Ask for each page until HasMore is false, then close the cursor.

For a stable saga-decision view across pages, open a point-in-time cursor. A live cursor can reflect changes between pages.

<!-- pause 0.8 -->

### Where next

Next on Build: Atomic writes. The links are on this episode's page in the documentation.

Predicate operations explains the filter language. The API reference gives each scan and cursor method's contract.

<!-- pause 2.0 -->

## Sources

| Narration | Source |
| --- | --- |
| Values that merge showed how the value decides what concurrent writes mean; this episode chooses which keys to read and how to keep the walk going | `videos/series.md`, B2 and B3 ideas |
| This is Scans, filters and cursors: ordered ranges, server-side predicates, and resumable pages | `videos/series.md`, B3 title and idea |
| A streaming scan walks live keys in strict lexicographic order; lower bound included, upper bound excluded; reverse walks high to low | `docs/lattice/api.md`, "Enumeration"; `docs/lattice/predicated-operations.md`, "A predicate composes with the existing bounds, direction, and prefetch arguments" |
| A typed scan takes a predicate over the value; the client translates it to a serialisable rule and rejects unsupported expressions before an RPC | `docs/lattice/predicated-operations.md`, "How it works" and "Supported expressions" |
| Each leaf checks values against their JSON document; only matches are returned; key scans send no values and entry scans return matching pairs | `docs/lattice/predicated-operations.md`, "How it works" and "Streaming scans" |
| Member access, constants, comparisons, boolean logic, and ordinal string checks are supported; unsupported value method calls are rejected | `docs/lattice/predicated-operations.md`, "Supported expressions" |
| Prefer resilient ScanKeys and ScanEntries over raw enumeration; recovery resumes with the predicate intact, within a bounded budget, and throws rather than returning a short range as complete | `docs/lattice/predicated-operations.md`, "Streaming scans"; `docs/lattice/api.md`, "Enumeration" and "Scan reliability" |
| A cursor returns an opaque identifier and checkpoints after each page; it can resume after silo failover or client restart; the caller closes it | `docs/lattice/api.md`, "Stateful cursors", "Idle TTL and self-cleanup", and "Example - resumable export across a silo failover" |
| Point-in-time mode gives every cursor page the saga-decision view captured at open; live mode may observe changes between pages | `docs/lattice/api.md`, "Ordering and visibility" |
| Next on Build: Atomic writes; Predicate operations and API reference | `npm run series -- ending B3`; `videos/series.md`, B3 "Introduces" |
