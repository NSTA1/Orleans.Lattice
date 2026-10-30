# The guarantees - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). Scenes use shared
components, and `npm run timeline` stamps their timings from the narration.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| The guarantees | `title-card` | 1 | The mark, "Evaluate / E2", and the title "The guarantees"; subtitle: "Caller-visible promises, and what survives a silo stop". |
| | | 2 | The subtitle holds while the recap makes the caller-visible contract the focus. |
| Four kinds of promise | `choice-list` | 1 | "Four kinds of promise"; the consistency guide's boundary appears first. |
| | | 2 | Linearizable: one point between invocation and return. |
| | | 3 | Strong consistency: a real point during the call, without missing, duplicate or misattributed entries. |
| | | 4 | Online snapshot: correct per key, not one global instant. |
| Bounded staleness | `choice-list` | 1 | Eventual consistency: bounded staleness, then convergence within a configurable interval. |
| Point operations | `choice-list` | 1 | `GetAsync`, with the default cache TTL of zero. |
| | | 2 | `SetAsync`, with the WAL append as the commit boundary. |
| Batches | `choice-list` | 1 | `GetManyAsync`: per-key consistency, not a global snapshot across unrelated keys. |
| | | 2 | `SetManyAsync`: per-key linearizability; a partial failure remains visible. |
| | | 3 | `SetManyAtomicAsync`: one whole-tree outcome, all changes or none. |
| Scans | `choice-list` | 1 | Ordered scans; no gaps or duplicate keys through topology changes. |
| | | 2 | Bounded topology, reconnect and stalled-page retry budgets. |
| | | 3 | Exhaustion faults; a reopened scan uses a fresh saga-decision view. |
| Snapshots and recovery | `choice-list` | 1 | Snapshot cursor: frozen per-shard baselines across pages; atomic batches stay on one side of capture. |
| | | 2 | WAL append commits the write; the provider determines restart durability. |
| | | 3 | Saga keepalive resumes after reactivation; the decision determines completion or compensation. |
| Where next | `choice-list` | 1 | "Next on Evaluate: The evidence" and "The deep dive: Clocks and version vectors". |
