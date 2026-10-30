# The guarantees - script

Status: fact-checked cue by cue by the Docs agent against the current corpus.
The snapshot-cursor wording is narrowed to avoid implying one wall-clock instant
across independent cross-shard writes. The episode distinguishes the
operation-specific consistency contract from the WAL provider's restart
durability and the atomic saga's recovery.

Written form, in the series voice (Emma, British English). This episode keeps
the reduced speech cadence of 0.92 used by the preceding episodes.

Each paragraph under "Narration" is one cue, and each cue is a beat its scene
can move on. A `pause` comment adds silence before the next cue or after the
last one.

## Narration

<!-- pace 0.92 -->

### The guarantees

The guarantees: what each operation promises its caller, and what remains true when a silo stops.

When Lattice fits named its limits. Here, we make the caller-visible promises precise.

### Four kinds of promise

The consistency guide classifies the operations it lists. A method it does not list is not classified by that table.

Linearizable means a call takes effect at one point between its invocation and return.

Strong consistency means a result reflects a real point during the call, without missing, double-counted or misattributed entries as the tree changes.

An online snapshot is correct per key, but is not guaranteed to be one global instant.

### Bounded staleness

Eventual consistency allows bounded staleness, then converges within a configurable interval.

### Point operations

With the default cache TTL of zero, `GetAsync` is linearizable. A positive cache TTL makes it eventually consistent, with staleness bounded by the TTL plus one refresh.

`SetAsync` is linearizable. Its write-ahead log append is the commit boundary, before the in-memory projection changes.

### Batches

`GetManyAsync` is consistent per key by default, but it is not one snapshot across unrelated keys.

`SetManyAsync` is linearizable per key, not atomic as a batch. On failure, some keys may have committed; sibling writes may still be in flight after the exception, so re-read after they settle.

`SetManyAtomicAsync` is all or nothing, and atomic-visible across the tree. A concurrent reader sees every change, or none.

### Scans

`ScanKeysAsync` and `ScanEntriesAsync` stay ordered, without gaps or duplicates through topology changes.

Topology retries default to three; resilient scans separately reconnect up to eight times, with a smaller budget for stalled pages.

Exhaustion faults rather than returning an incomplete scan. A reconnect or resume captures a fresh saga-decision view, so an atomic saga crossing that boundary can appear pre-commit in earlier results and post-commit in later ones.

### Snapshots and recovery

An online snapshot is not a strict snapshot. A snapshot cursor freezes each shard's baseline during open and uses it on every page. The saga-decision gate keeps an atomic batch on one side of capture. For a multi-page cursor, its baselines are durable before the continuation returns, so it survives silo failover; eviction before the first read can expire it, and it must be reopened.

The WAL append is the commit point, but the provider decides whether it survives a restart. The default in-memory log does not; a durable provider is needed.

If the silo hosting an atomic saga crashes mid-flight, its keepalive resumes it on reactivation. A recorded commit is completed; an undecided saga is compensated once its caller's decision deadline has passed.

### Where next

Next on Evaluate: The evidence. The deep dive on how this works is Clocks and version vectors. The links are on this episode's page in the documentation.

<!-- pause 2.0 -->

## Sources

Each line of narration traces to a source. Where the wording is narrower than
the contract, the scope is stated.

| Narration | Source |
| --- | --- |
| what each operation promises its caller; recap from the prior episode | [Series plan](../../series.md), E2 and E1 |
| the consistency guide classifies listed operations; an unlisted method is not classified | [Consistency guarantees](../../../docs/lattice/consistency.md), introduction |
| linearizable, strongly consistent, online snapshot and eventual consistency | [Consistency guarantees](../../../docs/lattice/consistency.md), "Consistency levels" |
| `GetAsync` under the default cache TTL and with a positive cache TTL | [Consistency guarantees](../../../docs/lattice/consistency.md), "Single-key operations"; "Read-cache staleness" |
| `SetAsync` is linearizable; the WAL append precedes the in-memory projection | [Consistency guarantees](../../../docs/lattice/consistency.md), "Single-key operations"; [Write-ahead log](../../../docs/lattice/wal.md), "Why WAL-as-sole-commit-point" |
| `GetManyAsync` is consistent per key and not a snapshot across unrelated keys | [Consistency guarantees](../../../docs/lattice/consistency.md), "Batch operations" |
| `SetManyAsync` is per-key linearizable and non-atomic; sibling writes may remain in flight after failure | [Consistency guarantees](../../../docs/lattice/consistency.md), "Batch operations" |
| `SetManyAtomicAsync` is all-or-nothing and atomic-visible tree-wide | [Consistency guarantees](../../../docs/lattice/consistency.md), "Batch operations"; [Atomic writes](../../../docs/lattice/atomic-writes.md), "Atomicity Guarantees" |
| scans are ordered, retry topology changes, reconnect or resume stalled pages, and fault when budgets are exhausted | [Consistency guarantees](../../../docs/lattice/consistency.md), "Enumeration" and "Retry exhaustion" |
| online snapshots are per-key correct; snapshot cursors freeze per-shard baselines, preserve atomic saga visibility and survive failover after continuation | [Consistency guarantees](../../../docs/lattice/consistency.md), "Consistency levels" and "Enumeration"; [Snapshot cursors](../../../docs/lattice/snapshot-cursors.md), "Lazy baseline persistence" and "Eviction before the first read" |
| WAL provider controls restart durability; the in-memory provider is not crash-safe | [Write-ahead log](../../../docs/lattice/wal.md), "Why WAL-as-sole-commit-point"; [WAL storage providers](../../../docs/lattice/wal-storage-providers.md), "Registering a provider" |
| a crashed atomic saga resumes; a recorded commit completes and an undecided saga is compensated after its caller's decision deadline | [Atomic writes](../../../docs/lattice/atomic-writes.md), "Atomicity Guarantees", item 3; "Silo restarts"; "Batches whose caller was answered" |
| next Evaluate episode and deep dive | `npm run series -- ending E2` |
