# Orleans.Lattice.Replication — WAL Design (Shard-Scoped, Durable, Turn-Safe)

> **Status:** design specification for Phase 7 of the [`replication roadmap`](../../src/lattice.replication/roadmap.md). Describes the target shape of the per-shard write-ahead log once R-070 (`IWalStorageProvider`) and R-071 (turn-safe batching) land. The current implementation is documented in [`wal.md`](wal.md) and uses Orleans grain persistence rather than the dedicated batching protocol described here.

This document describes the write-ahead log (WAL) design for `Orleans.Lattice.Replication` when using a log-first durability model. The WAL is maintained **per shard**, is **append-only**, and is persisted using a cloud-native backend such as Azure Table Storage.

The design is fully compatible with Orleans' single-threaded turn model and avoids blocking, background threads, or grain-state write amplification.

---

## 1. WAL responsibilities

Each shard has a dedicated `ShardWalGrain` (the Phase 7 evolution of today's `IReplogShardGrain`) responsible for:

- assigning a strictly increasing WAL offset
- buffering WAL entries in memory
- flushing entries in batches (≤ 100, ≤ 4 MB) to durable storage
- guaranteeing durability before acknowledging the caller
- retrying failed flushes
- exposing a sequential log for recovery and replication

The WAL is the **only** durable commit point.

---

## 2. WAL entry structure

Each WAL entry contains:

- `Offset` — monotonic `long`
- `Key` — the CRDT key within the shard
- `Delta` — typed CRDT delta (Phase 3 — see [`deltas.md`](deltas.md))
- `Hlc` — hybrid logical timestamp
- `Origin` — cluster identifier
- `Metadata` — optional correlation data

Entries are serialised into a compact binary payload via the Orleans serializer.

---

## 3. Persistence model

The WAL is persisted using an **insert-only** pattern:

- `PartitionKey = "{TreeId}/{ShardIndex}"`
- `RowKey = zero-padded 19-digit Offset` (lexicographic ordering matches numeric ordering)
- Payload stored as binary properties on the entity

This aligns with Azure Table Storage's batch insert rules:

- up to 100 inserts per batch
- total batch payload ≤ 4 MB
- all entities must share the same `PartitionKey`
- batch insert is atomic

Other backends (Cosmos DB, DynamoDB, file, in-memory) plug in via `IWalStorageProvider` (R-070); the (PartitionKey, RowKey) layout is the canonical implementation but not the contract.

---

## 4. Turn-safe batching model

The WAL grain maintains **in-memory only** buffers:

- `_pendingBatch` — list of WAL entries not yet flushed
- `_pendingAcks` — list of `TaskCompletionSource` for callers
- `_pendingBatchSizeBytes` — total serialised size of the pending batch
- `_inFlightFlush` — the currently running flush task (if any)
- `_nextOffset` — next WAL offset to assign

No grain state is used for pending batches.

### 4.1 `Append(delta)`

1. Assign `Offset = _nextOffset++`.
2. Serialise the WAL entry (or estimate its size) to obtain `entrySizeBytes`.
3. If adding this entry would exceed **either**:
   - `_pendingBatch.Count + 1 > 100`, or
   - `_pendingBatchSizeBytes + entrySizeBytes > 4 MB`,

   then:
   - if `_pendingBatch` is non-empty, start a flush of the current batch
   - start a new batch (reset `_pendingBatch` and `_pendingBatchSizeBytes`)
4. Add the entry to `_pendingBatch`.
5. Increment `_pendingBatchSizeBytes` by `entrySizeBytes`.
6. Create a `TaskCompletionSource` and add it to `_pendingAcks`.
7. If `_inFlightFlush == null`, start a flush.
8. Return the TCS task to the caller.

### 4.2 `Flush()`

1. Capture `_pendingBatch` and `_pendingAcks` into local variables.
2. Capture `_pendingBatchSizeBytes` into a local variable.
3. Clear `_pendingBatch`, `_pendingAcks`, and set `_pendingBatchSizeBytes = 0` for new writes.
4. Issue the storage batch insert asynchronously with the captured entries.
5. When the await resumes (inside the grain turn):
   - on success: complete all TCS for that batch
   - on failure: retry the batch (idempotent because `RowKey` is unique)
6. Set `_inFlightFlush = null`.

### 4.3 Key properties

- The grain never blocks its turn.
- All TCS completions occur inside the grain turn.
- New writes can accumulate while a flush is in flight.
- No duplicate writes occur.
- No grain-state persistence is involved.
- Both **100-operation** and **4 MB** batch limits are respected.

---

## 5. Durability guarantees

A write is acknowledged only after:

- its WAL entry is included in a batch,
- the batch is durably persisted (storage batch insert succeeds),
- the WAL grain resumes and completes the caller's TCS.

This ensures:

- strict durability
- strict ordering
- exactly-once append semantics (per shard, per offset)

---

## 6. Recovery

On activation:

1. Read the last applied offset (HWM) from storage.
2. Query WAL entries with `RowKey > HWM`.
3. Apply deltas to leaf grains in order.
4. Update HWM.
5. Resume normal operation.

Because WAL entries are append-only and CRDT merges are idempotent, replay is deterministic.

---

## 7. Replication

Replication streams consume WAL entries per shard:

- read entries in offset order
- stream them to remote clusters
- remote clusters append them to their own WAL
- remote appliers merge deltas using the same HWM logic

No special replication path is required.

---

## 8. Summary

This WAL design:

- fits Orleans' turn-based concurrency model
- avoids blocking, background threads, and grain-state churn
- guarantees durability before acknowledgement
- respects Azure Table's **100-operation** and **4 MB** batch limits
- supports high-throughput, size-aware batching
- aligns with Lattice's sharding model
- enables deterministic recovery and replication

It is the correct foundation for a log-first CRDT store.

---

## 9. Design review — known flaws and open questions

The design above is the starting point. The following are gaps, sharp edges, and inefficiencies discovered during review. Each is annotated with a recommended resolution; some are blocking for R-070 / R-071, others are acceptable trade-offs that need to be documented rather than fixed.

### 9.1 Blocking issues

#### 9.1.1 Offset assignment can outrun durability (data-loss window)

§4.1 step 1 assigns `Offset = _nextOffset++` *before* the entry is added to `_pendingBatch`. If serialisation in step 2 throws, or step 3 starts a flush of the *previous* batch and the current entry never reaches `_pendingBatch`, the offset is consumed but never persisted — leaving a permanent gap in the log. Recovery (§6) will then either:

- skip the gap (and silently lose every later entry that was appended after activation, because `_nextOffset` was rebuilt from storage and is now lower than the in-memory counter that produced the surviving entries), or
- block on the gap forever.

**Resolution:** assign the offset *after* successful serialisation and *after* the entry is committed to `_pendingBatch`. Treat `_nextOffset` as a reservation that is only made permanent on enqueue. Document the invariant: offsets are dense and gap-free across the lifetime of a partition.

#### 9.1.2 `_nextOffset` recovery on activation is unspecified

§6 describes recovery in terms of the HWM (last *applied* offset on the receiver side), but the WAL grain itself also needs to recover `_nextOffset` from storage at activation. The doc never says how. The natural answer — `_nextOffset = GetHighestOffsetAsync() + 1` — is correct only if §9.1.1's gap-free invariant holds. Make the dependency explicit in the spec.

**Resolution:** R-070's contract includes `GetHighestOffsetAsync` precisely for this; document that `ShardWalGrain.OnActivateAsync` calls it once and assigns `_nextOffset = highest + 1`, and that it is a hard invariant violation (logged + grain refuses to activate) if recovered offsets are not dense.

#### 9.1.3 The 4 MB / 100-entry limit is checked against an *estimate*

§4.1 step 2 says "Serialise the WAL entry (or estimate its size)". An estimate that under-counts by more than the slack on the current batch fails the storage backend's hard limit and the entire batch is rejected — taking every TCS in `_pendingAcks` with it. An estimate that over-counts wastes batch headroom and inflates p99 latency.

**Resolution:** require *exact* serialised size (cache the serialised payload with the entry). The cost is one extra in-memory `byte[]` per pending entry, which is dwarfed by the entry's own value payload. The `IWalStorageProvider` contract (R-070) accepts pre-serialised payloads, so the WAL grain serialises once and hands the bytes to the provider verbatim.

#### 9.1.4 No backpressure on `_pendingBatch` growth during a stalled flush

§4.3 says "new writes can accumulate while a flush is in flight". With no upper bound, a slow storage backend (transient throttling, cold partition, network blip) lets `_pendingBatch` grow without limit while the grain queue also fills with awaiting `Append` callers. The grain's working set blows up; eventual recovery has to flush an arbitrarily large backlog in one go, which then exceeds the 4 MB / 100-entry storage limits and *itself* fails — turning a transient outage into a hard stall.

**Resolution:** cap the in-flight + pending depth (e.g. `LatticeReplicationOptions.WalMaxPendingBatches`, default `4`). When the cap is reached, new `Append` calls must `await` the in-flight flush before being enqueued — exerting backpressure on the upstream `ILattice.SetAsync` caller rather than swallowing the queue. Surface the backpressure event on the `orleans.lattice.replication` meter (R-001 / R-064) so operators see it.

### 9.2 Correctness gaps to clarify

#### 9.2.1 Retry semantics on partial-batch storage failures

§4.2 step 5 says "on failure: retry the batch (idempotent because `RowKey` is unique)". This is true for Azure Table batch inserts (atomic — either all 100 land or none do), but **not** true for backends where multi-row inserts are not atomic. If a batch partially commits and is then retried, the second attempt sees `EntityAlreadyExists` for the persisted rows. Some backends raise this as a fatal error; some treat it as success.

**Resolution:** define the contract on `IWalStorageProvider.AppendBatchAsync` as "all-or-nothing per call". Backends that cannot meet that (DynamoDB cross-partition, Cosmos cross-partition) reject the batch at validation time rather than fragmenting it. R-070 already calls this out — propagate the same wording into the WAL design doc as a normative requirement.

#### 9.2.2 What happens if `Flush` is in flight when the grain deactivates?

The doc never addresses graceful shutdown. If Orleans deactivates the grain (idle timeout, silo drain) while `_inFlightFlush` is awaiting storage, the in-flight batch's TCSs may never complete — callers see their `SetAsync` Task hang until an Orleans-level RPC timeout fires. Worse, if the grain reactivates on another silo before the flush completes, two activations could race the same batch.

**Resolution:** override `OnDeactivateAsync` to await `_inFlightFlush` before returning, and refuse new `Append` calls (return an exception that surfaces as `OrleansException` on the caller). Combine with Orleans' single-activation guarantee — the grain is a single-writer by `[GrainType]`, so only the deactivating activation is responsible for draining its in-flight flush.

#### 9.2.3 Recovery (§6) reads "the HWM" but does not say which one

The HWM machinery built in R-023 is keyed `(tree, originClusterId)` and is a *receiver-side* dedup table. The "last applied offset" §6 talks about is a *materialiser-side* concept — the offset up to which leaf-state has been rebuilt from the WAL. Conflating the two is a wire-format bug waiting to happen: the receiver HWM is HLC-keyed (R-022), the materialiser HWM would be offset-keyed.

**Resolution:** introduce a distinct `ShardMaterialiserState` POCO with `(treeId, shardIndex) → highestAppliedOffset`. Document that this is **only** consulted on the materialiser path (the v2 forward-compat case in [`docs/future.md`](../../future.md)), is local to the silo, and never crosses the wire. Today's replication path does not need it; make the recovery section conditional ("when the WAL is the sole commit point — see [`future.md`](../../future.md)") so it is not implemented prematurely.

### 9.3 Inefficiencies

#### 9.3.1 Single `_inFlightFlush` serialises throughput at the storage RTT

The protocol allows one in-flight flush at a time. Sustained throughput is therefore capped at `(batch_size) / (storage_rtt)`. For Azure Table Storage at ~30 ms RTT and 100-entry batches, that's ~3 300 entries/sec per shard — adequate for most workloads, but a hard ceiling that does not scale with backend parallelism. A dual-flush design (allow up to N concurrent flushes per shard, completed in offset order) would double throughput at the cost of a small in-memory completion-reorder buffer.

**Resolution:** keep single in-flight flush for v1 (R-071) — simplicity wins, and the per-shard cap is not the bottleneck for typical workloads. Re-evaluate as a follow-up under R-063 (partitioned replog) once benchmarks justify it. Document the throughput ceiling in [`api.md`](../lattice/api.md) so operators size shard count accordingly.

#### 9.3.2 Per-entry serialisation cost is paid twice if §9.1.3 is fixed naively

If §9.1.3's resolution is "serialise once into a byte buffer, hand to provider", and the provider then re-serialises into its own envelope (Azure Table entity, gRPC frame), we pay 2× serialisation cost on every entry. The `IWalStorageProvider` contract should accept an `ArraySegment<byte>` payload alongside structured fields (offset, key) so the provider writes the bytes verbatim into its envelope.

**Resolution:** define the `ReplogShardEntry` envelope as `(Offset, Key, byte[] Payload)` where `Payload` is the pre-serialised `ReplogEntry` body. Providers store `Payload` as an opaque `byte[]` column / property / blob. R-070's `AppendBatchAsync` signature changes to accept `(long Offset, string Key, ArraySegment<byte> Payload)` tuples. Add a regression benchmark to BenchmarkDotNet showing single-serialise on the hot path.

#### 9.3.3 `_nextOffset` increment is not crash-safe across silo failover *(in conjunction with §9.1.1)*

If a silo crashes mid-flush, the surviving in-memory `_nextOffset` is lost. The new activation rebuilds `_nextOffset = highestPersisted + 1`, which is correct *if* the in-flight flush either fully landed or fully failed (§9.2.1's all-or-nothing contract). If the flush partially landed *and* the new activation has no way to know, two activations could re-use the same offset.

**Resolution:** rely on Orleans' single-activation guarantee — a partial flush from the dead silo is by definition not "in flight" from the perspective of the new activation, because the new activation can see the persisted prefix. Combined with §9.2.1 (all-or-nothing batches), this is safe. Document the dependency: the WAL design's correctness relies on (a) Orleans single-activation, (b) `IWalStorageProvider.AppendBatchAsync` atomicity.

### 9.4 Open questions

These are not flaws but unresolved choices that R-070 / R-071 must commit to:

- **Compaction boundary:** the design lists `TrimAsync(throughOffsetInclusive)` (R-070) but never says who calls it. The natural caller is the Phase 6 `min(cursor)` GC predicate (R-061), but the contract — does `Trim` fail if `throughOffset` is below the highest already-trimmed offset? does it block until the trim is durable? — is open.
- **Per-shard ordering across partitions:** §3 keys partitions by `(TreeId, ShardIndex)`. Replication consumers (§7) read "in offset order" within a partition, but cross-partition ordering is undefined. This is correct (CRDTs are order-independent up to HLC), but document it explicitly so transitive-replication implementers don't assume a global order.
- **Entry size hard ceiling:** the 4 MB batch limit applies to the whole batch. A single oversized entry (say, a 5 MB `byte[]` value) cannot be appended at all. Today's behaviour: silent failure inside the batching loop. Needed: a hard upstream check in `BPlusLeafGrain.SetCoreAsync` that rejects oversized values with `ArgumentException` before the WAL ever sees them. Tracked separately as a core-library follow-up (file under `G-005` quota / admission control).

---

## 10. Action items pulled from this review

These items feed back into the replication roadmap (Phase 7) as acceptance criteria for R-070 / R-071 — they are *not* new roadmap items, just preconditions for marking those existing items complete:

| § | Required for | Resolution |
|---|---|---|
| 9.1.1 | R-071 | Reserve offset only after enqueue; document gap-free invariant. |
| 9.1.2 | R-071 | `OnActivateAsync` recovers `_nextOffset` via `GetHighestOffsetAsync`. |
| 9.1.3 | R-070 + R-071 | Cache exact serialised bytes per entry; size limits checked against bytes, not estimate. |
| 9.1.4 | R-071 | `WalMaxPendingBatches` cap; backpressure surfaced on the meter. |
| 9.2.1 | R-070 | `AppendBatchAsync` all-or-nothing contract; non-conforming backends reject at validation. |
| 9.2.2 | R-071 | `OnDeactivateAsync` awaits in-flight flush; refuses new `Append`. |
| 9.2.3 | R-070 | Distinguish receiver HWM (HLC, R-023) from materialiser HWM (offset, future-only). |
| 9.3.1 | R-071 (deferred) | Single in-flight flush v1; revisit throughput ceiling under R-063. |
| 9.3.2 | R-070 | Provider receives `ArraySegment<byte>` payload, not `ReplogEntry`. |
| 9.3.3 | R-070 + R-071 | Documented dependency on Orleans single-activation + provider atomicity. |
