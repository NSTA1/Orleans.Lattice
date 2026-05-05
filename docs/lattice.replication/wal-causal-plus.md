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

### 12.3 What is intentionally not delivered

- **Atomic visibility cross-cluster.** R-089 preserves the writer's causal frontier across an atomic batch but does not deliver all-or-none cross-replica visibility. A remote reader can observe key 1 and key 3 of a 5-key atomic set before key 2 has been delivered, as long as no causal edge from the reader's prior observations is violated. Atomic visibility is strictly stronger than causal+ and would require a transactional-batch delivery primitive — flagged as a separate follow-on if demand surfaces.
- **Cross-tree causality.** A write to tree A followed by a write to tree B does not carry a cross-tree dependency edge. Each tree has its own independent vector clock; cross-tree causal ordering would require either a cluster-global VC (which does not scale) or a cross-tree dependency graph the user explicitly opts into (out of scope for this design).
- **Maintenance-mutation replay.** R-090 deliberately drops `MutationKind.Maintenance` on the producer; the receiver never sees them. A remote peer that needs to mirror the producer's exact tombstone-compaction state must rebuild it from the user-write replog stream that drove the data into the tree, not from the maintenance events themselves.
