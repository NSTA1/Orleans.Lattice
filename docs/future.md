# Orleans.Lattice — Log-First Durability & Replication Roadmap (WAL-Only Architecture)

> **Status: forward-looking design.** This document describes a v2 direction in which the write-ahead log becomes the sole durability mechanism and storage becomes a materialised projection. It is **not** the current architecture and is **not** scheduled for implementation. Items in `src/lattice/roadmap.md` and `src/lattice.replication/roadmap.md` are the authoritative committed work; this file exists so individual roadmap features can be designed to *not close doors* against this direction.

This roadmap defines the evolution of Orleans.Lattice into a **log‑first CRDT store** where the **write‑ahead log (WAL) is the sole durability mechanism**. The storage provider becomes a **materialised projection**, and cross‑cluster replication becomes a **natural consumer of the WAL**.

This is a new **major version** and replaces the previous StorageOnly persistence model entirely.

---

# 0. Goals

## 0.1 Core Durability Goals
- Make **WAL append** the single authoritative commit point.
- Remove storage writes from the hot path.
- Provide deterministic, causally ordered, idempotent application of CRDT deltas.
- Improve write throughput and reduce latency.
- Enable efficient indexing, projections, and materialised views.
- Support snapshot + incremental restore for fast recovery.

## 0.2 Replication Goals
- Build cross‑cluster replication directly on top of the WAL.
- Provide deterministic convergence using typed CRDT deltas.
- Support streaming, back‑pressure, DLQ, GC, and topology management.
- Ensure replication is optional and additive.

---

# 1. Core Layer (WAL-First Durability)

## 1.1 WAL as the Only Persistence Mode
The system now uses a **single durability model**:

    ApplyDelta → WAL.Append → durable

The storage provider is no longer the commit point.
It becomes a **background projection** of the WAL.

This change does **not** alter the public CRDT or grain APIs.

---

## 1.2 Commit-Time Delta Capture (C‑010)
All CRDT updates emit **typed deltas** at commit time:

- LWW deltas
- OR‑Set deltas
- PN‑Counter deltas
- Version vector deltas
- Opaque fallback (non‑convergent)

This eliminates read‑modify‑write cycles and enables log‑structured storage.

---

## 1.3 Partitioned WAL Grains (C‑020)
Introduce a **per‑shard, single‑writer WAL grain**:

- Append‑only
- No read‑modify‑write
- High throughput
- Linearizable ordering per shard
- Partition count configurable (e.g., 16–128)

This is the core scalability mechanism.

---

## 1.4 WAL Append as the Commit Point (C‑030)
The WAL is the **only** durable commit point.

    ApplyDelta → WAL.Append → (commit)

Materialisation to storage is always:

- asynchronous
- batched
- deterministic

---

## 1.5 High-Water-Mark Table (C‑040)
Track per‑shard apply progress:

- Ensures **exactly‑once** application of WAL entries.
- Enables safe replay after crash or restart.
- Required for both local durability and replication.

---

## 1.6 Background Materialisation (C‑050)
A background worker:

- Reads WAL entries in order
- Applies CRDT merges
- Writes materialised state to the storage provider
- Coalesces checkpoints (e.g., every 100–500 entries)

The storage provider becomes a **projection**, not the source of truth.

---

## 1.7 Snapshot + Incremental Restore (C‑060)
Provide a snapshot provider that:

- Emits a consistent snapshot of materialised state
- Supports chunked, resumable export
- Replays WAL entries after the snapshot point

Used for:

- Fast recovery
- Local testing
- Indexing/materialised view rebuilds

---

# 2. Replication Layer (Cross-Cluster)

Replication is **optional** and built entirely on top of the WAL.

---

## 2.1 Origin-Stamps & HLC (R‑010)
Each WAL entry includes:

- `OriginClusterId`
- Hybrid logical timestamp (HLC)

This provides:

- Deterministic causal ordering
- Cycle breaking
- Idempotent apply across clusters

---

## 2.2 Replication Streams (R‑020)
Introduce gRPC streaming channels:

- Binary framing
- Batching
- Back‑pressure aware
- Per‑shard cursor tracking

This replaces reminder‑driven pull in the sample.

---

## 2.3 Remote Apply Pipeline (R‑030)
Remote clusters:

- Receive typed deltas
- Apply them idempotently using the HWM table
- Materialise via the same background worker as local writes

No special code path for "remote" vs "local".

---

## 2.4 Topology & Filters (R‑040)
Support:

- Per‑tree opt‑in
- Per‑key filters
- Multi‑cluster meshes
- Fan‑out and fan‑in topologies

---

## 2.5 DLQ, GC, and Flow Control (R‑050)
Production‑grade operational features:

- **Dead‑letter queue** for poison entries
- **GC** based on minimum acknowledged cursor
- **Flow control** to prevent WAL backlogs
- **Metrics**: entries behind, bytes behind, apply lag, etc.

---

# 3. API Compatibility Guarantees

The following remain unchanged:

- All CRDT types and semantics
- All grain interfaces
- All public namespaces
- All user‑facing persistence semantics
- All user code that calls `ApplyDelta`, `Merge`, or reads CRDT state

The WAL is now the **only** persistence mechanism.
Replication remains **optional** and **additive**.

No public API breaks.

---

# 4. Implementation Order

1. **C‑010** Commit‑time delta capture
2. **C‑020** Partitioned WAL grains
3. **C‑030** WAL‑first commit path
4. **C‑040** High‑water‑mark table
5. **C‑050** Background materialisation
6. **C‑060** Snapshot + incremental restore
7. **R‑010** Origin stamping + HLC
8. **R‑020** Replication streams
9. **R‑030** Remote apply pipeline
10. **R‑040** Topology + filters
11. **R‑050** DLQ, GC, flow control, metrics

---

# 5. Relationship to today's roadmaps

The core ([`src/lattice/roadmap.md`](../src/lattice/roadmap.md)) and replication ([`src/lattice.replication/roadmap.md`](../src/lattice.replication/roadmap.md)) roadmaps already deliver large parts of this design as additive, non-breaking changes. The current commitment is to ship phases 1–6 of the replication roadmap *without* inverting durability, while keeping the building blocks (per-shard WAL grain, typed deltas, HWM, change feed, snapshot provider) shaped so a future inversion is mechanical rather than architectural.

| Future ID | Today's equivalent |
|---|---|
| C‑010 (commit-time delta capture) | Core grain-side mutation hook (`IMutationObserver`) + replication R-010, extended by replication R-030/R-031 |
| C‑020 (partitioned WAL grains) | Replication R-011 (shipped) — same `IReplogShardGrain` shape, currently used for replication only |
| C‑030 (WAL append = commit) | Replication R-014 `Strict` append mode (planned default) |
| C‑040 (HWM table) | Replication R-023 (per-origin HWM); generalises to local apply in this future |
| C‑050 (background materialisation) | Net-new — not on either committed roadmap |
| C‑060 (snapshot + tail restore) | Replication R-050 (snapshot provider for bootstrap); same primitive, different consumer |
| R‑010..R‑050 (this doc's replication block) | Replication phases 2–6 verbatim |

Net new work specific to this design: C‑050 (background materialiser), the operational story for materialisation lag (cursor pinning, projection-rebuild tooling), and re-deriving tombstone compaction / online splits / online reshard / atomic-write saga as deterministic log operations — see the "what changes" notes in the replication roadmap's **Forward compatibility** section.
