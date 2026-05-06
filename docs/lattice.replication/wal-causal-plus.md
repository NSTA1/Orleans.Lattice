# WAL Design — Causal+ Ready (with Performance Notes)

> **Status:** partially shipped — the entry-schema seam is live as of the
> first causal-plus delivery; receiver-side dep-check, GC predicate, and
> snapshot cut-point remain forward-looking. The shipped pieces are the
> two additive `[Id]` slots on `ReplogEntry` (`VectorClock`,
> `DependencySummary`), the producer-side stamping at the commit-time
> mutation observer, the internal `VectorClockCodec`
> (`EncodeAbsolute` / `EncodeDelta` / `DecodeDelta`), and a diagnostic
> minor-version bump on `ReplicationBatchEnvelope` (alias and
> `WireVersion` unchanged so legacy peers continue to decode the new
> entries with both slots flowing through as `null`). Companion to
> [`wal.md`](wal.md) (the replication-side per-shard WAL grain) and
> [`../lattice/wal.md`](../lattice/wal.md) (the cross-cutting WAL contract).

This document defines the causal+-ready Write-Ahead Log (WAL) for `Orleans.Lattice.Replication`. It extends the existing WAL design without breaking any of its invariants:

- Append-then-apply remains the commit point.
- Per-shard monotonic offsets remain the ordering backbone.
- Origin stamping remains the cycle-break mechanism.
- Replay is still strictly ordered by WAL offset.

Causal+ introduces additional metadata and apply-time rules, but does not alter the WAL's durability or ordering semantics.

---

## 1. WAL entry schema

Each WAL entry is extended to carry causal metadata required for causal+ consistency.

### 1.1 Existing fields (unchanged)

- `TreeId`
- `ShardIndex`
- `Offset` (monotonic per shard)
- `Operation` (Set / Delete / DeleteRange)
- `ValueOrDelta`
- `OriginClusterId`
- `SourceHlc` (Hybrid Logical Clock)
- `Mode` (`ReplicationMode`)

### 1.2 New fields (added for causal+)

#### VectorClock

A per-origin vector clock representing the full causal frontier at commit time.

- Sparse map: `{ origin → hlc }`.
- Encoded compactly (delta-encoded relative to the previous entry on the same shard).
- Backwards compatible: missing field decodes to an empty map.

**Performance note (avoid Mistake 1 — full VC bloat):**

- Store vector clocks **per shard as a baseline**, and encode each entry's VC as a **delta from the previous entry**.
- Use a compact representation (e.g. sorted `(origin, hlc)` pairs with varint encoding).
- Do **not** re-emit the full VC for every entry when only one origin advanced.
- **GC-safety rule:** every entry whose predecessor on the same shard was trimmed by GC, and the first entry of every shipped batch, must carry an **absolute** VC (not a delta). Intermediate entries may delta-encode safely. This preserves the trim-from-the-head invariant in §7 without requiring GC to rewrite surviving entries.

#### DependencySummary

A compact representation of the causal predecessors of this entry.

- Shape 1: the vector clock itself.
- Shape 2 (optional future): a compact hash / Bloom filter of predecessor HLCs.

**Performance note:**

- Start with **"VC as dependency summary"**; only introduce Bloom filters if real-world pressure demands it.
- Keep this field **read-only** and **immutable** to avoid recomputation.

#### AtomicBatchSize / AtomicBatchIndex

Two purely additive `[Id]` slots on `ReplogEntry` (and the corresponding `LatticeMutation` slots on the observer-side surface) that carry the size and zero-based index of the enclosing atomic transaction.

- `AtomicBatchSize` — total number of entries in the enclosing atomic transaction. `0` for non-atomic single-key writes and non-atomic batches; `N` on every per-key emit produced by a `SetManyAtomicAsync` saga of size `N`, including compensation rolls.
- `AtomicBatchIndex` — zero-based position of this entry within the enclosing batch; `0` for non-atomic writes. Within a batch the index covers `0..Size-1` exactly once each, derived deterministically from the saga's per-operation iteration order.
- Sibling membership is keyed by the existing `TransactionId` slot (Core F-044). The receiver detects a complete batch by counting siblings that share an `(originClusterId, transactionId)` against the declared `Size`.
- There is deliberately **no** separate "commit marker" entry. A partially-shipped batch that loses a sibling surfaces as the orphan-timeout case the receiver-side staging buffer already handles, not an indefinite stall waiting on a commit row that never arrives.
- Strictly additive on the wire: legacy peers and entries authored before these slots existed decode both fields as `0`, which a receiver with atomic-batch delivery enabled treats identically to a single-key write. A peer with atomic-batch delivery disabled ignores both slots entirely.
- Receiver-side opt-in is governed by `LatticeReplicationOptions.AtomicBatchDelivery` (per-tree, default `false`). When `false`, the receiver applies each entry as a point write and never consults the metadata; when `true`, the receiver buffers entries with `AtomicBatchSize > 0` until every sibling is in hand and applies the whole batch atomically. Producer-side stamping is unconditional, so flipping a peer to opt-in does not require a producer restart.

**Performance note:**

- Two `int` fields per entry, packed by the Orleans serializer; storage cost is negligible relative to the existing `OriginClusterId` and HLC slots.
- The observer pass-through is two struct-field copies on the commit-time hot path; no allocation, no resolver lookup, no decision branch.
- The slots are unconditional on every emit so a peer flipping atomic-batch delivery on does not require a producer restart or any wire-format change.

---

## 2. WAL append semantics (unchanged)

The WAL remains the commit point:

```text
append(entry);
persist(entry);
apply_to_local_state(entry);
```

Causal+ does not change:

- when entries are appended,
- how entries are persisted,
- how offsets advance,
- or how failures propagate.

**Performance note:**

- All causal+ logic is **off the commit path**.
- Do not introduce dependency checks or buffering into the append path.

---

## 3. WAL replay semantics (extended for causal+)

Replay remains strictly ordered by WAL offset, but apply is now conditional.

### 3.1 Replay loop (new semantics)

```text
for entry in WAL in offset order:
    if is_duplicate(entry):           // HWM check
        continue;

    if dependencies_satisfied(entry): // VC check
        apply(entry);
        advance_local_vector_clock(entry);
        wake_buffered_entries();
    else:
        buffer(entry);
```

**Performance note (avoid Mistake 2 — global locks):**

- This loop runs **per shard**, inside the shard's single-threaded grain.
- Do **not** introduce cross-shard locks or global coordination here.
- All state (`local_vector_clock`, buffers) is shard-local and single-threaded.

---

## 4. Dependency satisfaction

### 4.1 Definition

An entry `E` is safe to apply when:

```text
∀ origin in E.VectorClock:
    local_vector_clock[origin] ≥ E.VectorClock[origin]
```

This ensures:

- If B depends on A, B cannot apply before A.
- If B arrives before A, B is buffered.
- If A arrives later, A applies, then B becomes eligible.

**Performance note (avoid Mistake 4 — recomputing closure):**

- Maintain `local_vector_clock` **incrementally**; never recompute from WAL history.
- The dependency check is just a **map lookup + integer comparison per origin**.
- Keep `local_vector_clock` in memory and persist it alongside the HWM.

### 4.2 Buffering

Each shard maintains:

- a ready queue (dependencies satisfied),
- a blocked queue (dependencies missing).

Blocked entries are keyed by:

- origin,
- missing predecessor clocks.

When local clocks advance, blocked entries are re-evaluated.

**Performance note (avoid Mistake 3 — unbounded buffering):**

- Enforce a **per-shard buffer size cap** (e.g. max N blocked entries or max M bytes).
- When the cap is hit, either:
  - apply **backpressure** to the transport (slow down or temporarily stop pulling), or
  - park the oldest blocked entries via the existing dead-letter pipeline with **explicit metrics + alerts** (never silently drop).
- Use **simple data structures**: per-origin lists or small priority queues; avoid complex global heaps.

---

## 5. Local vector clock

Each shard maintains a local vector clock:

```text
local_vc[origin] = highest applied HLC from that origin
```

This is the natural generalisation of the existing per-origin high-water-mark table — the HWM is the diagonal of the local VC, keyed by the entry's own origin only. Causal+ widens the check to consult **every** origin in the entry's VC, not just the authoring origin.

Updated only after a successful apply. Persisted in the same grain state as the HWM table.

**Performance note:**

- Keep `local_vc` in a **small dictionary** keyed by origin string or interned id.
- Origins are few (clusters), so this is tiny.
- Persist `local_vc` as part of the shard's metadata; no extra grain needed.

---

## 6. Interaction with high-water marks

The existing HWM table remains valid.

Order of checks:

```text
if entry.HLC <= HWM[origin]:
    // duplicate, no work
    return;

if dependencies_satisfied(entry):
    apply(entry);
    advance_local_vector_clock(entry);
    advance_HWM(entry);
    wake_buffered_entries();
else:
    buffer(entry);
```

**Performance note:**

- The HWM check is **O(1)** and remains the first gate.
- This avoids unnecessary dependency checks for duplicates.

---

## 7. Causal-stable frontier

The WAL GC predicate is extended.

### 7.1 Old predicate

```text
min_acked_offset_per_peer
```

### 7.2 New predicate

```text
causal_stable = min_over_all_peers(peer_vector_clock)
```

An entry is GC-eligible when:

- its vector clock ≤ `causal_stable`, **and**
- its WAL offset ≤ `min_acked_offset`.

**Performance note:**

- The number of peers and origins is small; computing `min_over_all_peers` is cheap.
- Cache the computed `causal_stable` frontier and recompute only on **ack updates**, not per entry.

The predicate must consult **every** change-feed consumer's VC — including any future local materialiser — not just remote peers. A lagging consumer must pin the log identically regardless of whether it is remote or in-process.

### 7.3 Blocked-floor (TX-aware GC pin)

Cross-cluster atomic-batch delivery (Phase 9, `AtomicBatchDelivery = true`) introduces a receiver-side staging buffer that holds every key in an in-flight `SetManyAtomicAsync` until the whole batch arrives. While a batch is partially staged, the receiver has **not** acknowledged the buffered entries through its per-origin high-water-mark — the producer's WAL is the authoritative re-ship source if the receiver buffer is lost (e.g. via the orphan-timeout eviction path documented in the dead-letter queue). The GC must therefore not trim past any entry the receiver still needs to recover from buffer state.

The predicate widens to AND in a **strict-less** clause:

```text
entry.Timestamp < blocked_floor
```

where `blocked_floor = min(BlockedAtHlc across reporting consumers)`.

A consumer with a partially-staged batch reports the lowest staged HLC `t` via the blocked-floor overload of `ReportCursorAsync`. The overload accepts `cursor = HybridLogicalClock.Zero` so the applier can register a pin without contributing to the `min(cursor)` branch — `GetMinCursorAsync` skips Zero-cursor consumers symmetrically. Pin updates use **replace semantics**, not monotonic merge: as the buffer admits new transactions the lowest staged HLC can drop, and the registry must reflect the new pin so the GC stops trimming further forward. Reporting `blockedAtHlc: null` releases the pin entirely (used when the buffer drains).

The strict-less comparison is load-bearing: an entry whose `Timestamp` exactly equals the blocked-floor must remain in the WAL because that entry is itself the lowest-staged entry on at least one receiver. A `<=` clause would silently trim it and the receiver could never recover from a buffer loss at that HLC.

Consumers that do not call the blocked-floor overload contribute `null` to the floor and are excluded from `min(...)`. When **no** consumer reports a pin, `blocked_floor` is `null` and the predicate degrades to the §7.2 form above. Backwards-compatible by construction: pre-Phase-9 receivers never stamp a pin and never block the producer GC.

**Cross-cluster propagation.** The receiver-side gRPC server stamps the locally-computed `blocked_floor` onto every outbound `ReplicationAck.BlockedAtHlc` (a new `[Id]` slot on the ack envelope). The producer-side shipper grain reads the slot from each ack and republishes it via `IReplicationCursorRegistry.ReportCursorAsync` under the consumer key `shipper:peer-blocked-floor:{peerId}`. The producer's WAL GC then AND-s the same strict-less clause from §7.3 into its own trim predicate using the per-peer pin, so a partially-staged batch on **any** downstream peer pins the producer's WAL identically to a partially-staged batch on a local in-process consumer. The propagation channel is wire-additive — the new `[Id]` slot on `ReplicationAck` decodes as `null` on legacy producers, which then degrade transparently to the §7.2 frontier without any pinning behaviour.

**Performance note:**

- The blocked-floor is cached behind the same per-tree generation counter as the causal-stable frontier; recompute only on ack / pin updates, not per entry.
- Surfaces on `ReplicationGcReport.BlockedFloor` for dashboards and operator playbooks.

---

## 8. Snapshot semantics

Snapshots must be taken at a causal-stable frontier.

### 8.1 Snapshot cut-point

```text
snapshot_frontier = causal_stable
```

### 8.2 Snapshot contents

Include all entries whose vector clocks are ≤ `snapshot_frontier`.

### 8.3 Incremental catch-up

Incremental replication begins at:

```text
start = snapshot_frontier
```

The receiver pins both the as-of HLC and the snapshot's VC frontier in its persistent metadata, then resumes incremental delivery from that VC. The dependency check in §4 runs from the pinned frontier on the first incremental entry — exactly-once across the snapshot/incremental boundary.

**Performance note:**

- Snapshot cost is dominated by **state scan**, not causal metadata.
- Causal+ only changes the **cut-point**, not the scan algorithm.

---

## 9. Transport requirements (WAL-driven)

The WAL schema drives the transport requirements:

- vector clocks must be transmitted,
- dependency summaries must be transmitted,
- per-origin FIFO must be preserved,
- out-of-order arrivals must be detected and buffered.

The transport does **not** reorder entries.
The transport does **not** need to enforce causal ordering.
The transport only needs to:

- preserve FIFO per origin,
- deliver metadata intact.

**Performance note:**

- Keep transport logic **dumb and fast**; all causal reasoning stays in the apply pipeline.
- Use **one long-lived stream per peer** (as the gRPC push transport already does) to minimise per-batch overhead.

---

## 10. Summary of WAL changes

| Area      | Change                          | Backwards compatible | Performance notes                                |
|-----------|---------------------------------|----------------------|--------------------------------------------------|
| Schema    | Add vector clock                | ✔                    | Delta-encode per shard; absolute on batch / post-trim boundaries |
| Schema    | Add dependency summary          | ✔                    | Start with VC; only optimise if needed           |
| Replay    | Add dependency checks           | ✔                    | Per-shard, single-threaded, O(#origins)          |
| Replay    | Add buffering                   | ✔                    | Cap buffers; backpressure transport              |
| GC        | Causal-stable frontier          | ✔                    | Recompute on ack updates only                    |
| Snapshot  | Cut at causal-stable frontier   | ✔                    | No change to scan algorithm                      |
| Transport | Carry new metadata              | ✔                    | Keep transport simple; no causal logic           |

---

## 11. Non-goals

Causal+ does **not** require:

- changing WAL offset semantics,
- reordering WAL entries,
- rewriting WAL entries,
- changing commit-time capture,
- changing the storage provider contract.

The WAL remains:

- append-only,
- strictly ordered,
- durable,
- the canonical mutation record.

Causal+ only adds:

- richer metadata,
- stricter apply rules,
- stronger GC and snapshot invariants.

With the performance notes above, the design avoids the four common failure modes:

1. **Full vector clock bloat** — use delta encoding with absolute anchors at batch / post-trim boundaries.
2. **Global locks or cross-shard contention** — keep apply per-shard.
3. **Unbounded buffering** — cap, backpressure, and route overflow through the existing dead-letter pipeline with classified reason tags.
4. **Recomputing dependency closure** — maintain the local VC incrementally.

---

## 12. Completeness wave — full coverage of structural & atomic write paths

Sections 1–11 cover the point-write, single-tree, single-shard-per-mutation path. The five replication items R-089–R-093 (and their core-side dependencies F-043–F-046) extend the same causal+ guarantee to every write path the core library exposes, so a host that enables any one of those features still gets full causal+ semantics rather than dropping back to per-origin LWW for that specific path. Cross-tree causality is intentionally out of scope.

### 12.1 Coverage matrix

| Write path                                | Hazard if not covered                                                                                  | Replication item | Core dep |
|-------------------------------------------|--------------------------------------------------------------------------------------------------------|------------------|----------|
| `SetManyAtomic` (multi-key atomic write)  | Per-key VC drift fabricates a dependency graph the writer never authored; remote peer parks key K waiting for key K-1. | R-089            | F-044    |
| Resize / rebalance / compaction           | Structural rewrites stamp fresh VC under maintenance pressure; pollutes the dependency graph and inflates wire traffic. | R-090            | F-045    |
| Shard split / merge / saga shadow-forward | Shadow-forward emit captures a fresh VC against the destination shard rather than preserving the originating commit's frontier. | R-091            | F-046    |
| Multi-shard user write (range delete, multi-leaf saga) | Per-grain VC reads disagree on cross-shard origins because each grain only sees inbound applies routed to it. | R-092            | none     |
| Snapshot / restore (intra-cluster)        | Live HWM table is wiped on restore; receiver replays restored entries against a zeroed VC and either re-parks them or re-merges. | R-093            | F-043    |

### 12.2 Design invariants preserved

Every item in the completeness wave is constrained by the same rules that bound sections 1–10:

- **No commit-path change.** VC capture happens at the existing commit-time observer site (R-080). The atomic-transaction boundary (F-044) is a metadata signal on `LatticeMutation`, not a new commit phase.
- **Append-only, monotonic, durable.** No item rewrites a WAL entry, no item changes offset semantics, no item changes the commit point.
- **Per-shard apply.** R-091's receiver-side dedupe cache and R-092's producer-side VC cache are both per-shard; no cross-shard locks.
- **Wire-additive only.** F-043 / F-045 / F-046 are new `[Id]` slots with decode-as-empty defaults. Legacy peers and legacy persisted state continue to decode and behave identically to today's per-origin-only HWM check.
- **Idempotent under re-delivery.** R-091's `RecentApplyCache<(origin, hlc, key, op)>` LRU is a fast-path optimisation; correctness is still bounded by the per-origin HWM (R-023) plus the dep-check (R-082).

### 12.2.1 R-089 shipped mechanism

The atomic-transaction boundary (`TransactionId`, F-044) is the per-emit signal a future causal+ consumer reads to detect that several entries belong to the same enclosing batch. The producer-side VC capture is the **batch-wide consistency** half: a new `[Id(11)] AtomicWriteState.VectorClock` slot persists the caller's ambient frontier on the saga's first `Prepare` (capture-once, mirroring the `KeyFingerprint` / `TransactionId` / `DeltaKind` precedent — wire-compatible, missing field on legacy persisted state decodes to `null`); the saga grain re-stamps the persisted slot onto `LatticeVectorClockContext.Current` at the head of every `RunSagaAsync` so every per-key `SetAsync` issued during `Execute` reads the identical ambient and the leaf grain stamps it onto the freshly-constructed `LwwValue`. The saga-wide stamp survives crash recovery because the persisted slot is the single source of truth. Compensation rolls override the saga-wide stamp per-key with each `AtomicPreValue.VectorClock` so a rolled-back key re-lands with its original frontier.

### 12.2.2 R-093 shipped mechanism

The intra-cluster snapshot/restore path (operator snapshots a tree, restores it later — same cluster, possibly different timestamp) is the dual of the cross-cluster bootstrap path covered by R-050 / R-084. Cross-cluster bootstrap pins the snapshot's `causalStableFrontier` directly via `PinSnapshotAsync((HLC, VectorClock))` because the wire envelope carries the frontier verbatim from sender to receiver. Intra-cluster restore has no such envelope: the operator workflow tears down the live tree state (including the per-tree `IReplicationHighWaterMarkGrain` rows that compose the local vector clock) and rehydrates the values from a snapshot artefact, but the frontier metadata that drove R-082's dependency check is gone.

R-093 reconstructs that frontier from the values themselves. Core F-043 added `[Id(6)] LwwEntry.VectorClock` (default empty for legacy persisted state) and the core library preserves the slot end-to-end through every persistence / merge / snapshot / restore / bulk-load / compaction path with the same discipline F-036 applied to `OriginClusterId`. The replication package ships `IReplicationLocalVcSeeder.SeedFromTreeAsync(treeName, ct)`: walks every shard's leaf chain via `IShardRootGrain.GetLeftmostLeafIdAsync` → `IBPlusLeafGrain.GetLiveRawEntriesAsync` → `GetNextSiblingAsync` (the canonical leaf-walk pattern from `TreeSnapshotGrain.CopyShardAsync`), accumulates `frontier.MergeFrom(entry.VectorClock)` for every non-null VC slot (pointwise-max — the same accumulator R-082's bounded buffer uses), and seeds both halves of the local vector clock. The durable half goes to `IReplicationHighWaterMarkGrain.PinSnapshotAsync(HybridLogicalClock.Zero, frontier)` so receiver-side `GetVectorAsync` reads the reconstructed frontier across silo restarts; the in-memory half goes to `LocalVectorClockCache.AdvanceForeign(treeName, origin, hlc)` for every `(origin, hlc)` pair in the frontier, so the producer-side cache (R-092) does not require a fresh cold-start RPC after the seed. The pin uses `HybridLogicalClock.Zero` as the `asOfHlc` argument because intra-cluster restore has no snapshot timestamp — the frontier is the authoritative seed; the `asOfHlc` slot is preserved in the call shape only for symmetry with the cross-cluster path.

Non-replicated trees (`IReplicationModeResolver.Resolve(treeName)` returns `null`) are a no-op: the seeder returns `LocalVcSeedReport { SeedApplied = false, EntriesScanned = 0, Frontier = null }` without consulting the tree. Empty trees pin an empty frontier — even an empty tree's HWM grain is consistent post-seed. Partial restores (where the operator restores a subset of values) seed correctly from the surviving subset because the pointwise-max accumulator handles missing-origin components as zero. The seeder is **not** automatic — restore tooling is host-defined and the seeder is a single-call public API the operator runs once per restored tree as a post-restore step, mutually exclusive with the cross-cluster bootstrap path per restore event.

---

## 13. Examples

### 13.1 Simplified causal+ entry sketch

```text
message ReplogEntry {
    Metadata metadata = 1;

    oneof operation {
        // multi-key atomic upsert
        AtomicUpsert upsert = 2;
        // single-key delete
        Delete delete = 3;
    }

    // per-entry causal metadata
    VectorClock vector_clock = 10;
    DependencySummary dependency_summary = 11;
}
```

### 13.2 Causal+ entry lifecycle

1. Initial state:

```text
local_vc = {}
```

2. Produce an entry:

```text
// Set X=1
upsert = {
    "X": 1
};

vector_clock = {
    "A": 5,
    "B": 5
};

// no dependencies
dependency_summary = {};
```

3. Append as offset 123:

```text
append({
    "offset": 123,
    "operation": {
        "upsert": upsert
    },
    "vector_clock": vector_clock,
    "dependency_summary": dependency_summary
});
```

4. On replay, before apply:

```text
WAL: [ ..., { offset: 123, vector_clock: { A: 5, B: 5 } } , ... ]

local_vc: { A: 4, B: 5 } // or { A: 5, B: 4 } -- both are ok

entry: { offset: 123, vector_clock: { A: 5, B: 5 } }

// metadata matches, applies idempotently
```

5. On replay, after apply:

```text
local_vc: { A: 5, B: 5 } // or { A: 5, B: 5 } -- both are ok

// buffered entries with satisfied dependencies are eligible
```

6. Produce a conflicting entry:

```text
// Set A=2
upsert = {
    "A": 2
};

vector_clock = {
    "A": 6, // advance A's clock
    "B": 5
};

// depends on A's prior value
dependency_summary = {
    "A": 5
};
```

7. Append as offset 124:

```text
append({
    "offset": 124,
    "operation": {
        "upsert": upsert
    },
    "vector_clock": vector_clock,
    "dependency_summary": dependency_summary
});
```

8. On replay, before apply:

```text
WAL: [ ..., { offset: 124, vector_clock: { A: 6, B: 5 } } , ... ]

local_vc: { A: 5, B: 5 }

entry: { offset: 124, vector_clock: { A: 6, B: 5 } }

// A's VC is ahead, but the dependency (summary) is satisfied
```

9. On replay, after apply:

```text
local_vc: { A: 6, B: 5 }

// the dependency on A's prior value allows this to apply
// buffered entries with satisfied dependencies are eligible
```

10. Produce an entry with indirect dependency:

```text
// Set B=3 (indirectly dependent on A)
upsert = {
    "B": 3
};

vector_clock = {
    "A": 7, // advance A's clock
    "B": 6
};

// depends on A's new value
dependency_summary = {
    "A": 6
};
```

11. Append as offset 125:

```text
append({
    "offset": 125,
    "operation": {
        "upsert": upsert
    },
    "vector_clock": vector_clock,
    "dependency_summary": dependency_summary
});
```

12. On replay, before apply:

```text
WAL: [ ..., { offset: 125, vector_clock: { A: 7, B: 6 } } , ... ]

local_vc: { A: 6, B: 5 }

entry: { offset: 125, vector_clock: { A: 7, B: 6 } }

// B's VC is ahead, and the dependency (summary) is satisfied
```

13. On replay, after apply:

```text
local_vc: { A: 7, B: 6 }

// the dependency on A's new value allows this to apply
// buffered entries with satisfied dependencies are eligible
```

14. Effects of trim and GC:

```text
// GC trims up to offset 124, entries 123 and 124 are preserved
trim_to_offset = 124;

WAL: [ ... , { offset: 123, vector_clock: { A: 5, B: 5 } } , { offset: 124, vector_clock: { A: 6, B: 5 } } , ... ]

// causal stable is now at 124, any entry with VC <= 124 is GC eligible
```

15. After GC:

```text
WAL: [ ... , { offset: 124, vector_clock: { A: 6, B: 5 } } , ... ]

// entry 123 is gone, but its effects are preserved by entry 124
```

16. Snapshot at causal stable:

```text
// snapshot_frontier is stable at 124
snapshot_frontier = 124;

// any entry with vector_clock <= 124 is included in the snapshot
// this includes the last stable state for all keys
```

17. Incremental replication starts at the snapshot frontier:

```text
start = 124;

// subsequent entries are delivered starting from the snapshot frontier
// the receiver's local VC is updated to 124 for all origins
```

18. End-to-end causal+ replication guarantees:

- causally ordered delivery
- no cycles
- no duplicates
- snapshots include all necessary state
- incremental from the latest stable snapshot

**Performance notes:**

- The above example uses an idealised entry sketch for clarity.
- Actual implementation details may vary, e.g., use of forward deltas or batch encapsulation.
- Focus on the causal relationships and VC/DS semantics.

---
