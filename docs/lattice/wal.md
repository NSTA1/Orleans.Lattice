# Write-Ahead Log

This document describes how Orleans.Lattice uses a **write-ahead log (WAL)** as the
sole foreground-commit durability boundary for every leaf grain mutation. The WAL
is the primary on-disk truth: in-memory projections, secondary indexes, and
replication consumers are all rebuilt from it on activation or recovery.

If you're looking for a different angle on the WAL:

- For the pluggable storage backend (in-memory vs Azure Table) see
  [`wal-storage-providers.md`](wal-storage-providers.md).
- For how the in-memory projection is rebuilt from the WAL on activation see
  [`projection-rebuild.md`](projection-rebuild.md).
- For the replication-side overlay (per-shard sharded sink, producer-side
  filters, and the `MutationCategory.Maintenance` skip) see
  [`../lattice.replication/wal.md`](../lattice.replication/wal.md).
- For the causal+ entry-schema extension (vector clock + dependency
  summary slots on `WalRecord`) see [`wal-causal-plus.md`](wal-causal-plus.md).

## What the WAL is

The WAL is an **append-only log of `LatticeMutation` envelopes**, partitioned per
shard. Each shard owns a dense, monotonically increasing offset space starting at
zero; offsets are never reused, never reordered, and never gapped.

```text
shard 0:  [0]Set k1   [1]Set k2     [2]Delete k1   [3]Set k3      ...
shard 1:  [0]Set a    [1]DelRange   [2]Set b       [3]Set c       ...
shard 2:  [0]Set x    [1]Set y      [2]Set z       [3]Delete y    ...
```

A `LatticeMutation` carries everything a replay or replication consumer needs to
reconstruct the effect of one foreground operation: the tree id, the operation
kind (`Set` / `Delete` / `DeleteRange`), the key (and optional end-exclusive key
for ranges), the LWW timestamp (an `HybridLogicalClock`), the value bytes (or
tombstone marker), the optional TTL expiry, the origin cluster id, the vector
clock, the optional transaction id, the maintenance category, and the optional
delta payload. The envelope is the wire format and the on-disk format and the
replication payload — there is exactly one shape.

## Why WAL-as-sole-commit-point

Every foreground commit (set / delete / range-delete) appends exactly one
`LatticeMutation` to the WAL **before** the in-memory projection sees the write.
The WAL append is the moment the operation is durable; everything after that is
materialisation.

This is a deliberate trade. The wins:

- **Single durability boundary.** There is exactly one write-ahead step per
  commit. The in-memory projection has no independent durability guarantee;
  it is reconstructed from the WAL on activation.
- **Replay-driven recovery.** Activation rebuilds the in-memory projection from
  the WAL via `ILeafReplayCoordinatorGrain` + `ILeafProjection.Apply`; there is
  no separate snapshot file to keep in sync.
- **Replication coupling.** A peer's change feed and the local commit log are
  the same byte stream. Cross-cluster replication is an additional consumer of
  the same WAL, not a parallel pipeline.

The cost:

- **Cold-activation replay cost is bounded by retention.** A leaf that hasn't
  been activated since the last GC cutoff replays from the trim watermark
  forward. The projection-checkpoint mechanism (below) keeps the typical replay
  to the last few hundred entries.
- **The WAL provider must be durable.** The default `InMemoryWalStorageProvider`
  is fine for tests and single-process samples but is not crash-safe; production
  deployments register a durable provider such as
  `AzureTableWalStorageProvider` via `siloBuilder.AddWalStorage(...)` (or via
  `AddLatticeReplication` for multi-cluster).

## Commit pipeline

Every leaf grain commit follows the same four-step pipeline. The order is
load-bearing — the WAL append must happen before any in-memory state mutates,
and the observer publish must happen after both, inside a commit-log scope.

```text
   build  ─►  wal  ─►  apply  ─►  observer
   (HLC,    (append   (merge      (publish under
    LwwValue, Lattice-  into       LatticeCommitLog-
    Mutation) Mutation) projection) Context scope)
   in-mem    durable    in-mem      in-mem
```

1. **build** — Tick the local hybrid-logical clock and the version vector,
   construct the `LwwValue<byte[]>` for the new entry (or the tombstone), and
   build the `LatticeMutation` envelope. The envelope captures the
   `LatticeOriginContext`, `LatticeVectorClockContext`, `LatticeTransactionContext`,
   `LatticeMaintenanceContext`, and any ambient `LatticeDeltaContext`.

2. **wal** — Resolve the shard's `ICommitLogWriter` and append the mutation. If
   the writer is absent (no WAL provider registered) this step is a no-op, and
   the operation has the same semantics as a pre-WAL Lattice — durable on the
   grain-state provider only. Failures here propagate to the caller before any
   in-memory state has been touched.

3. **apply** — LWW-merge the value into the in-memory `Entries` projection. If
   the leaf's entry count crosses `MaxLeafKeys`, trigger a split. This step is
   the only place that mutates the per-leaf in-memory state on the foreground
   path.

4. **observer** — If any `IMutationObserver` is registered, publish the
   post-commit mutation inside a `LatticeCommitLogContext` scope. The scope
   marker lets a replication-aware observer detect that the source of this
   mutation was the local commit log and short-circuit its loop-prevention
   logic so it doesn't re-append its own input back into the WAL.

The pipeline is implemented in
[`BPlusLeafGrain.CommitSetAsync`](../../src/lattice/BPlusTree/Grains/BPlusLeafGrain.cs)
and the mirror paths for `DeleteAsync` and `DeleteRangeAsync`. Each step records
its elapsed wall-clock duration to the `leaf.commit.duration` histogram tagged
by `step`.

## Durability boundary

The WAL append in step 2 is the foreground durability boundary. Three invariants
follow:

- **A successful commit implies a durable WAL append.** If `SetAsync` returns
  successfully, the mutation is in the WAL and visible to any replay. If the
  WAL append throws, the in-memory projection is untouched, and the caller sees
  the exception.
- **In-memory projection is reconstructable from the WAL alone.** The
  projection has no independent durability guarantee. `PersistAsync` still
  exists for the projection-checkpoint flush (below) and for a few maintenance
  paths (sibling pointer updates, tree-id stamping, compaction bookkeeping),
  but those are not on the foreground commit path and they do not extend the
  durability boundary.
- **Replication consumers see exactly the foreground commit ordering.** A peer
  replicating from this shard sees the same `LatticeMutation` envelopes in the
  same order that the local projection saw them. The WAL is the linearization
  point.

## WAL grain API

The per-shard WAL is owned by the internal `IWalShardGrain`, keyed
`{treeId}/{partition}` where `partition` is `WalPartitionHash.Compute(key, partitions) %
LatticeOptions.WalPartitions` (default `1`). The grain is in the core
`Orleans.Lattice.BPlusTree.Grains` namespace and is the single producer-side
entry point for foreground commits and the read-back source for the
replication change feed.

| Member | Purpose |
|---|---|
| `AppendAsync(WalRecord, CancellationToken)` | Append a captured mutation. Returns the assigned dense per-shard sequence number. |
| `ReadAsync(long fromSequence, int maxEntries, CancellationToken)` | Read a contiguous page from `fromSequence`. Returns a `WalShardPage` with the entries and the `NextSequence` cursor. Validates `fromSequence >= 0` and `maxEntries >= 1`; out-of-range reads return `WalShardPage.Empty(fromSequence)`. |
| `GetNextSequenceAsync(CancellationToken)` | Returns the sequence the next append will use. |
| `GetEntryCountAsync(CancellationToken)` | Returns the total number of entries persisted. |

Saga terminal mutations (`MutationKind.TxCommit` / `TxAbort`) carry their
shard index in `mutation.Key` as a base-10 invariant-culture string; the
commit-log writer maps that shard index to a WAL partition by taking
`shardIndex % WalPartitions`. When the shard count exceeds the partition
count, multiple shards collapse onto the same WAL partition; receivers
dedupe by `TransactionId`, so multiple terminal appends with the same id
are idempotent on the apply side.

## Origin cluster id stamping

Every WAL record carries `OriginClusterId` so multi-site receivers can
attribute the origin and break replication cycles. The stamp comes from
two sources, applied in priority order at the producer-side
`WalRecordConverter.ToWalRecord(...)` call site:

1. **`mutation.OriginClusterId` wins when present.** A remote replay path
   stamps the upstream cluster id onto the mutation before it reaches the
   WAL writer; the converter preserves that value verbatim.
2. **Fallback to the resolver-supplied local cluster id.** When the
   mutation arrives with `OriginClusterId == null` — the foreground commit
   path on a host where the replication observer has not yet stamped — the
   writer asks `ILatticeOriginClusterIdResolver.Resolve(treeId)` for the
   local id.

`ILatticeOriginClusterIdResolver` is a public seam in
`Orleans.Lattice.BPlusTree.Grains`. The core ships
`DefaultLatticeOriginClusterIdResolver` (returns `string.Empty`) so a
single-cluster host gets an empty stamp and downstream consumers ignore
it. Hosts that register `Orleans.Lattice.Replication` get
`ConfiguredLatticeOriginClusterIdResolver` swapped in via the same
remove-then-`TryAdd` pattern that the replication package uses for
`ILatticeMergeModeResolver`. The configured resolver reads
`LatticeReplicationOptions.ClusterId` and caches the per-tree result with
`IOptionsMonitor<T>.OnChange` invalidation, so the commit-time hot path is
a single dictionary lookup.

The same resolver is consulted on the read-back path
(`WalShardGrain.ReadAsync`) so the change feed projects the same origin
the producer recorded — required for the replication-side loop-prevention
filter that drops batches whose `OriginClusterId` matches the local
cluster.

A user who needs to source the local cluster id from somewhere other
than `LatticeReplicationOptions` (e.g. a control plane, environment
variable, or per-tree feature flag) registers a custom
`ILatticeOriginClusterIdResolver` before calling `AddLattice` /
`AddLatticeReplication`; the package registrations use `TryAddSingleton`
and the swap loop only removes the *default* registration, so a
user-supplied resolver is preserved.

## Turn-safe batching protocol

The WAL grain's `AppendAsync` hot path implements a turn-safe batching
protocol. Each call accumulates into an in-memory pending batch held on
the grain instance; a single in-flight flush at a time fans the batch out
to `IWalStorageProvider.AppendBatchAsync` and completes per-caller
`TaskCompletionSource<long>` instances when the provider acknowledges
durability.

```text
AppendAsync(entry)
    │  assigns offset = _nextOffset++
    │  appends WalEntry to _pendingBatch
    │  parks a TCS in _pendingAcks
    │  if no flush in flight → StartFlush()
    ▼
returns await tcs.Task    ◄── completes once provider acks the batch
```

Two batch limits are enforced at append time:

| Option | Default | Trigger |
|---|---|---|
| `WalMaxBatchEntries` | `100` | Adding the new entry would push the pending count above the cap; the current batch is flushed first, then the new entry starts the next batch. |
| `WalMaxBatchBytes` | `4 MB` | Adding the new entry's estimated serialised size would exceed the byte budget; same cutover. The size estimate is `key.Length * 2 + value.Length + 128` bytes — UTF-16 worst case for the key plus a constant envelope overhead. |

Cutovers wait for the in-flight flush before the next batch can start,
which provides natural back-pressure under burst load: a single shard
cannot accumulate more than two batches' worth of pending state at any
instant.

### Activation recovery

On grain activation, `OnActivateAsync` calls
`IWalStorageProvider.GetHighestOffsetAsync` and sets `_nextOffset =
highest + 1`. The persisted log is the single source of truth for the
next-offset counter — the grain holds no Orleans grain state of its own.

### Deactivation drain

`OnDeactivateAsync` awaits any in-flight flush and then triggers (and
awaits) a final flush of any remaining pending entries, so a graceful
deactivation never leaves a caller observing a hung TCS.

### Append-failure semantics

A flush failure is fail-fast for every affected caller:

1. The next-offset counter rolls back to the start of the failed batch
   (`_nextOffset = batch[0].Offset`) so the dense-offset invariant against
   the provider is preserved.
2. Every TCS in the failed batch is faulted with the underlying storage
   exception.
3. Every TCS in the *currently-accumulating* pending batch is also faulted
   — those entries had been assigned offsets above the now-rolled-back
   gap, so their offsets are stale and the calls must restart fresh.
4. The pending batch is reset; subsequent `AppendAsync` calls resume
   cleanly from the rolled-back `_nextOffset`.

This contract makes WAL-append failures observable inline at the
originating writer rather than being silently coalesced into a later
batch.

> **Contributor note — synchronously-completing providers.** `FlushAsync`
> starts with `await Task.Yield()` so the returned `Task` is observably
> incomplete by the time `StartFlush` assigns it to `_inFlightFlush`.
> Without that yield, an `IWalStorageProvider` whose `AppendBatchAsync`
> returns a synchronously-completed task (the in-memory provider does
> this) would run the entire flush body inline before the assignment
> lands; the `finally { _inFlightFlush = null; }` would clear a field
> that was not yet set, then `StartFlush`'s assignment would overwrite
> `null` with the completed task — leaving `_inFlightFlush` permanently
> non-null and every subsequent `AppendAsync` parked on its TCS forever.
> Any future refactor of the flush loop must preserve this invariant.

### What the protocol does *not* do yet

- **Multiple in-flight batches.** The current implementation enforces a
  single in-flight flush per shard. `WalMaxPendingBatches` is reserved
  for a future change that lifts the cap; today it is validated (`>= 1`)
  but not consumed by the grain.
- **Exact-bytes accounting.** `WalMaxBatchBytes` is enforced against an
  estimate (key UTF-16 worst case + value length + 128 byte envelope),
  not the post-serialisation byte count. The estimate is conservative
  for typical payloads; pathological keys or oversized envelopes can
  drift either way. Documented as approximate in the option's XML doc.

## Recovery and rebuild


When a leaf grain activates, it rebuilds its in-memory projection by replaying
the WAL through `ILeafReplayCoordinatorGrain`. Two cases:

- **Tail replay.** The last persisted projection checkpoint is at offset *N*,
  the WAL head is at offset *M*, and `M - N` is bounded by the checkpoint
  interval. The coordinator streams entries `(N, M]` and applies each via
  `ILeafProjection.Apply`. Replay is in-process and typically completes in a
  few milliseconds.
- **Fall-off-log rebuild.** The persisted checkpoint is older than the WAL trim
  watermark — the entries it would replay are no longer available. The
  coordinator falls back to `ILeafProjection.Rebuild`, which reseeds from the
  authoritative store (in current shipped configurations, the leaf's own
  grain-state — which is still maintained for a separate set of background
  paths, not as a foreground durability boundary). Drift detection is described
  in [`projection-rebuild.md`](projection-rebuild.md).

In both cases, the projection that a reader observes after activation is
byte-equivalent to the projection at the moment the leaf last deactivated.

## Projection checkpoint

To keep tail-replay bounded, the leaf flushes a **projection checkpoint**
durably whenever the elapsed wall-clock time since the last flush reaches
`MaterialiserCheckpointInterval` (default: 1 second) **or** the count of
unflushed advances reaches `MaterialiserCheckpointEntries` (default: 1 000),
whichever happens first. The checkpoint is a single grain-state write that
captures the in-memory entries, the local HLC, the version vector, and the
WAL offset of the last applied mutation. On the next activation the replay
coordinator starts from the checkpoint offset rather than from zero.

The checkpoint is **not** an additional durability boundary — it's a replay-cost
optimization. If a checkpoint flush fails, the next activation simply replays
more WAL entries; correctness is unaffected. The checkpoint is also flushed
opportunistically in `OnDeactivateAsync` so a graceful shutdown doesn't lose an
already-pending advance.

## Trim and GC

The WAL grows monotonically and must be trimmed. Trim is driven by
`ILatticeReplicationGc`, a per-tree single-pass collector that advances the
per-shard trim watermark to the largest contiguous prefix that **every**
registered consumer has already acknowledged.

The collector ships in `Orleans.Lattice.Replication` because it was originally
authored to reclaim space behind the change-feed shipper, but the predicate is
expressed against `min(cursor across registered consumers)` — not `min(cursor
across remote peers)` — so the local in-memory projection (the materialiser
that rebuilds the leaf state from the WAL on activation) is just another
consumer. A lagging materialiser pins the log exactly the same way a lagging
remote peer does.

### Predicate

A WAL entry is trim-eligible when the **HLC clause** *and* the **causal-stable
clause** both accept it.

The HLC clause is satisfied when **either** of the following holds:

| Condition | Meaning |
|---|---|
| `entry.Timestamp <= minCursor` | Every registered consumer has reported a cursor at or beyond this entry's HLC. |
| `entry.Timestamp <= ttlCeiling` | The entry's wall-clock component is older than `now - WalRetention` (when configured). |

The causal-stable clause is satisfied when **either** of the following holds:

| Condition | Meaning |
|---|---|
| `causalStable is null` | No consumer has reported a per-origin `VersionVector` through the causal+ overload of `ReportCursorAsync`. The clause degrades to a no-op so the GC behaves identically to the legacy HLC-only predicate. |
| `causalStable.DominatesOrEquals(entry.VectorClock)` | Every consumer that reported a vector has fully observed the entry's causal predecessors. Entries with a `null` `VectorClock` (legacy peers, range deletes, pre-causal+ entries) are treated as the empty frontier and pass automatically. |

The two clauses are AND-ed: the cursor / TTL clause is kept for safety so a
stale or mis-configured causal-stable computation cannot cause the GC to
over-trim past a consumer that is still pinning the HLC half.

`minCursor` is the minimum HLC across all `(treeName, consumerId)` entries
published to the `ILatticeReplicationCursorRegistry`. The cursor branch is
gated on `minCursor > HybridLogicalClock.Zero` so range-delete entries (which
carry `HybridLogicalClock.Zero` by design) are never trimmed under an unset /
zero cursor.

`ttlCeiling` is the hard ceiling configured by
`LatticeReplicationOptions.WalRetention`. When set, a lagging consumer that
pins the log past the ceiling is intentionally allowed to "fall off the log"
so disk usage stays bounded; that consumer detects the gap on its next read
and re-bootstraps via the fall-off-log path described in
[`projection-rebuild.md`](projection-rebuild.md).

The scan is conservative: the first non-eligible entry per shard stops the
walk for that shard. WAL offsets are dense and append-only but HLC
`WallClockTicks` is mostly-monotonic-with-skew, so a stop-at-first-miss walk
preserves correctness while a more aggressive scan would risk trimming an
entry younger than a still-pinned later entry.

### Consumer registration

Every consumer of the change feed — the outbound replication ship loop,
in-process bridges, custom transports, and the local in-memory materialiser —
must publish its acked HLC to the registry so its progress contributes to
`minCursor`. A consumer that never registers does not pin the log; the GC
will trim under it and the consumer must detect the gap on the next read.

```text
// After successfully applying a batch acknowledged through `appliedHlc`,
// the consumer reports its progress. Subsequent reports must be
// monotonically non-decreasing per (treeName, consumerId).
await registry.ReportCursorAsync(
    treeName: "orders",
    consumerId: "peer:site-b",
    cursor: appliedHlc,
    cancellationToken: cancellationToken);

// On graceful shutdown the consumer unregisters so its stale cursor
// stops pinning the log.
await registry.UnregisterAsync("orders", "peer:site-b", cancellationToken);
```

The default `InMemoryReplicationCursorRegistry` is process-local and loses
its state on silo restart. A host that needs cross-restart durability
registers its own `ILatticeReplicationCursorRegistry` implementation via DI
before calling `AddLatticeReplication(...)`.

### Causal-stable frontier

A consumer that has stamped vector clocks on the entries it applies can also
report its full per-origin frontier through the causal+ overload of
`ReportCursorAsync`. The GC then computes `causalStable` as the **pointwise
minimum** of every reported `VersionVector`: an origin is retained in the meet
only when every reporting consumer has named it, and the value at that origin
is the smallest HLC across the reports.

Consumers that only report HLC (the legacy overload) continue to pin the
cursor half of the predicate but are excluded from the meet. When **no**
consumer has reported a vector, `causalStable` is `null` and the GC behaves
identically to the legacy HLC-only predicate.

The frontier is cached in the registry behind a per-tree generation counter
that bumps on every accepted report or unregister, so a high-frequency GC
pass that observes a stable registry reads the frontier in O(1).

A consumer registers a vector by passing the additional `VersionVector`
argument:

```text
await registry.ReportCursorAsync(
    treeName: "orders",
    consumerId: "peer:site-b",
    cursor: appliedHlc,
    vector: appliedFrontier,
    cancellationToken: cancellationToken);
```

The registry takes a defensive clone of the supplied vector, so callers may
continue to mutate their local frontier after the report returns.

### Blocked-floor (TX-aware GC pin) — vestigial

> **Status — vestigial.** The receiver-side staging buffer that
> originally drove this pin (`IReplicationTxBufferGrain`, the
> `LatticeReplicationOptions.AtomicBatchDelivery` opt-in) was retired
> by the WAL repivot. The shipped cross-cluster atomic-visibility path
> ([Consistency: Atomic visibility](consistency.md#atomic-visibility-single-tree-foreground-and-cross-cluster))
> uses per-key prepared mutations and a per-shard `TxCommit` /
> `TxAbort` terminal mark that ride the standard WAL → replication
> transport — every prepared entry is durably anchored on the
> producer's WAL exactly like a non-saga write, so the receiver
> rebuilds its per-tx pending bucket deterministically from WAL
> replay without a separate blocked-floor signal. The pin is therefore
> preserved on the GC predicate as defence-in-depth for any future
> consumer that does report a blocked-floor; in the current shipped
> surface no consumer reports one and the GC degrades to the cursor /
> TTL clauses above. The remainder of this subsection describes the
> predicate as it would behave under such a future consumer.

Under such a hypothetical receiver, the cross-cluster atomic-batch
delivery path would stage every entry of an in-flight
`SetManyAtomicAsync` until the whole batch arrived. While a batch is
partially staged, the receiver has **not** acknowledged the
buffered entries through its per-origin high-water-mark — the producer's WAL
is the authoritative re-ship source if the receiver's buffer state is lost
(e.g. via the orphan-timeout eviction path). The GC must therefore not trim
past any entry the receiver still needs to recover from buffer state.

The GC predicate widens to AND in a **strict-less** clause:

```
entry.Timestamp < blockedFloor
```

where `blockedFloor = min(BlockedAtHlc across reporting consumers)`. A
consumer with a partially-staged batch reports the lowest staged HLC `t` via
the blocked-floor overload of `ReportCursorAsync`:

```text
await registry.ReportCursorAsync(
    treeName: "orders",
    consumerId: "applier:atomic-batch",
    cursor: HybridLogicalClock.Zero,   // applier does not own a cursor
    blockedAtHlc: lowestStagedHlc,     // null releases the pin
    cancellationToken: cancellationToken);
```

The blocked-floor overload accepts `cursor = Zero` because a buffer-pin-only
consumer (typically the receiver-side applier) has no cursor of its own and
must not pollute the GC's `min(cursor)` branch. `GetMinCursorAsync` skips any
consumer reporting a Zero cursor for the same reason. Negative cursors and
negative pins are rejected.

Pin updates use **replace semantics**, not monotonic merge: as the buffer
admits new transactions the lowest staged HLC can drop, and the registry
must reflect the new pin so the GC stops trimming further forward. Reporting
`blockedAtHlc: null` clears the pin entirely — used when the buffer drains.

Consumers that do not call the blocked-floor overload contribute `null` to
the floor and are excluded from `min(...)`. When **no** consumer reports a
pin, `blockedFloor` is `null` and the GC predicate degrades to the existing
`min(cursor)` + causal-stable + TTL clauses alone.

The strict-less comparison is load-bearing: an entry whose `Timestamp`
exactly equals the blocked-floor must remain in the WAL because that entry
is itself the lowest-staged entry on at least one receiver. A `<=` clause
would silently trim it and the receiver could never recover from a buffer
loss at that HLC.

The blocked-floor surfaces as a diagnostic on `ReplicationGcReport.BlockedFloor`,
alongside the existing `MinCursor` / `TtlCeilingHlc` / `CausalStable` slots,
so dashboards can alert on a pin that does not advance for an extended
period (typical signal: a stuck atomic-batch admit waiting on a missing
sibling — see the orphan-timeout operator playbook).

### Scheduling

`ILatticeReplicationGc.RunOnceAsync(treeName)` is a single-pass GC invocation.
The library does **not** install a background timer — the host owns the
cadence so it can integrate with whatever scheduling infrastructure it
already uses (Orleans reminders, hosted services, external schedulers). A
typical inner-loop period is 30 to 60 seconds per replicated tree.

```text
ReplicationGcReport report = await gc.RunOnceAsync(
    treeName: "orders",
    cancellationToken: cancellationToken);

// The report exposes the inputs and the outcome:
//   - report.MinCursor       — minimum cursor across registered consumers, or null
//   - report.TtlCeilingHlc   — TTL ceiling synthesised from WalRetention, or null
//   - report.ShardsScanned   — number of WAL shards walked
//   - report.CausalStable    — pointwise-min VersionVector across consumers, or null
//   - report.EntriesTrimmed  — total entries removed across all shards
```

### Metrics

The GC publishes one counter on the `orleans.lattice.replication` meter:

| Instrument | Tags | Description |
|---|---|---|
| `orleans.lattice.replication.wal.entries_trimmed` | `tree` | Total WAL entries removed by a GC pass. Incremented only when the pass trimmed at least one entry. |

## Relationship to replication

Cross-cluster replication is an **additional consumer** of the same WAL — not
a parallel pipeline. The replication change feed reads `LatticeMutation`
envelopes from the WAL, applies them on the peer cluster via
`IReplicationApplier` (which calls into the same `ILeafProjection.Apply` that
local commit and local replay use), and acknowledges its progress through the
same cursor registry that GC consults.

The single-cluster and multi-cluster code paths are identical up to the point
where replication transports an envelope across a network boundary. There is
no "replication mode" that changes how a foreground commit durabilizes — the
commit always appends to the local WAL, and replication is purely additive.

See [`../lattice.replication/replication-drivers.md`](../lattice.replication/replication-drivers.md)
for the driver-grain scheduling model that consumes the WAL on each peer.

## Configuration

The WAL pipeline exposes a small number of knobs on `LatticeOptions`. Defaults
suit most workloads.

| Option | Default | Purpose |
|---|---|---|
| `MaterialiserCheckpointInterval` | 1 second | Time-driven flush of any pending projection-checkpoint advance. Set to `Timeout.InfiniteTimeSpan` to disable the time trigger and rely solely on the entry-count trigger. |
| `MaterialiserCheckpointEntries` | `1_000` | Entry-count trigger: forces a checkpoint flush once this many advances are pending, regardless of `MaterialiserCheckpointInterval`. Bounds the worst-case replay cost when the steady-state apply rate is high. |
| `MaxLeafReplayEntries` | `10_000` | Upper bound on the entries `ILeafReplayCoordinatorGrain` streams in a single tail replay. A leaf whose backlog exceeds this falls back to the rebuild path indicated by `ProjectionRebuildPolicy`. |
| `LeafProjectionRetention` | 7 days | Age beyond which a persisted checkpoint is considered stale; the next activation falls off-log and rebuilds. Set to `Timeout.InfiniteTimeSpan` to disable the age-based trigger. |
| `ProjectionRebuildPolicy` | `SnapshotThenWal` | Recovery strategy when a fall-off-log trigger fires (snapshot + WAL tail, or full rebuild from the authoritative source). |

The WAL provider itself is registered separately via
`siloBuilder.AddWalStorage(...)` (single-cluster) or via
`siloBuilder.AddLatticeReplication(...)` (multi-cluster). See
[`wal-storage-providers.md`](wal-storage-providers.md) for the provider seam
and the in-memory / Azure Table options.

## Observability

The commit pipeline emits three primary instruments. All are tagged with the
tree id and (for `leaf.commit.duration`) the pipeline step.

| Instrument | Type | Tags | Meaning |
|---|---|---|---|
| `leaf.commit.duration` | histogram (ms) | `tree`, `step` ∈ `{wal, apply, observer}` | Per-step latency of the foreground commit pipeline. The `wal` step is the durability cost; `apply` is in-memory-only; `observer` is the publish under the commit-log scope. |
| `leaf.replay.duration` | histogram (ms) | `tree`, `outcome` ∈ `{tail, rebuild}` | Wall-clock cost of activation-time WAL replay. `tail` is the cheap incremental path; `rebuild` is the fall-off-log path. |
| `leaf.replay.entries` | counter | `tree`, `result` ∈ `{applied, skipped}` | Mutations consumed during replay. `skipped` covers entries below the persisted checkpoint or out-of-order arrivals filtered by LWW. |

The bundled Grafana dashboards consume these instruments directly; see
[`../lattice.dashboards/index.md`](../lattice.dashboards/index.md).

## Related surfaces

- [`wal-storage-providers.md`](wal-storage-providers.md) — pluggable backend
  contract and the in-memory / Azure Table providers.
- [`projection-rebuild.md`](projection-rebuild.md) — drift detection and the
  fall-off-log rebuild path.
- [`tombstone-compaction.md`](tombstone-compaction.md) — how reaped tombstones
  interact with WAL retention.
- [`configuration.md`](configuration.md) — the full `LatticeOptions` surface.
- [`wal-causal-plus.md`](wal-causal-plus.md) — causal+ entry-schema
  extension (vector clock + dependency summary slots on `WalRecord`).
- [`../lattice.replication/wal.md`](../lattice.replication/wal.md) — the
  replication-side overlay: per-shard sharded sink, producer-side filters,
  and the `MutationCategory.Maintenance` skip.
