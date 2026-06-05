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
replication payload - there is exactly one shape.

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
load-bearing - the WAL append must happen before any in-memory state mutates,
and the observer publish must happen after both, inside a commit-log scope.

```text
   build  ─►  wal  ─►  apply  ─►  observer
   (HLC,    (append   (merge      (publish under
    LwwValue, Lattice-  into       LatticeCommitLog-
    Mutation) Mutation) projection) Context scope)
   in-mem    durable    in-mem      in-mem
```

1. **build** - Tick the local hybrid-logical clock and the version vector,
   construct the `LwwValue<byte[]>` for the new entry (or the tombstone), and
   build the `LatticeMutation` envelope. The envelope captures the
   `LatticeOriginContext`, `LatticeVectorClockContext`, `LatticeTransactionContext`,
   `LatticeMaintenanceContext`, and any ambient `LatticeDeltaContext`.

2. **wal** - Resolve the shard's `ICommitLogWriter` and append the mutation. If
   the writer is absent (no WAL provider registered) this step is a no-op, and
   the operation has the same semantics as a pre-WAL Lattice - durable on the
   grain-state provider only. Failures here propagate to the caller before any
   in-memory state has been touched.

3. **apply** - LWW-merge the value into the in-memory entry cache. If
   the leaf's entry count crosses `MaxLeafKeys`, trigger a split. This step is
   the only place that mutates the per-leaf in-memory state on the foreground
   path.

4. **observer** - If any `IMutationObserver` is registered, publish the
   post-commit mutation inside a `LatticeCommitLogContext` scope. The scope
   marker lets a replication-aware observer detect that the source of this
   mutation was the local commit log and short-circuit its loop-prevention
   logic so it doesn't re-append its own input back into the WAL.

The pipeline is implemented in
[`BPlusLeafGrain.CommitSetAsync`](../../src/lattice/BPlusTree/Grains/BPlusLeafGrain.cs)
and the mirror paths for `DeleteAsync`, `DeleteRangeAsync`, `MergeEntriesAsync`,
`MergeManyAsync`, and `CompactTombstonesAsync`. Each step records its elapsed
wall-clock duration to the `leaf.commit.duration` histogram tagged by `step`;
the foreground-write histogram `leaf.write.duration` is tagged by `kind` so
operators can size ordinary writes (`kind=set` / `kind=delete`), the
cross-migration LWW backstop (`kind=backstop`), merge traffic (`kind=merge`),
and tombstone compaction (`kind=compact`) independently.

The merge family (`MergeEntriesAsync` / `MergeManyAsync`) emits one envelope
per accepted entry with `Kind = Set | Delete` and `IsMerge = true`; the
compactor (`CompactTombstonesAsync`) emits one envelope per reaped entry with
`Kind = Tombstone` and `IsMerge = true`. The `IsMerge` flag is a ship-side
metric tag - receivers apply the envelope as an ordinary write regardless of
its value.

## Durability boundary

The WAL append in step 2 is the foreground durability boundary. Three invariants
follow:

- **A successful commit implies a durable WAL append.** If `SetAsync` returns
  successfully, the mutation is in the WAL and visible to any replay. If the
  WAL append throws, the in-memory projection is untouched, and the caller sees
  the exception.
- **In-memory projection is reconstructable from the WAL alone.** The
  projection has no independent durability guarantee. `PersistAsync` still
  exists for the projection-checkpoint flush (below) and for tree-metadata
  paths (sibling pointer updates, tree-id stamping, split lifecycle,
  last-compaction-version snapshotting), but those persist *metadata*, not a
  fallback copy of the entry values. The grain-state row never stores entry
  values as a backup; entry values live in the WAL until they reach a
  durable snapshot.
- **Replication consumers see exactly the foreground commit ordering.** A peer
  replicating from this shard sees the same `LatticeMutation` envelopes in the
  same order that the local projection saw them. The WAL is the linearization
  point.

## WAL grain API

The per-shard WAL is owned by the internal `IWalShardGrain`, keyed
`{treeId}/{partition}` where `partition` is `WalPartitionHash.Compute(key, partitions) %
LatticeOptions.WalPartitions` (default `8`). The grain is in the core
`Orleans.Lattice.BPlusTree.Grains` namespace and is the single producer-side
entry point for foreground commits and the read-back source for the
replication change feed.

| Member | Purpose |
|---|---|
| `AppendAsync(WalRecord, CancellationToken)` | Append a captured mutation. Returns the assigned dense per-shard sequence number. |
| `AppendBatchAsync(IReadOnlyList<WalRecord>, CancellationToken)` | Append a contiguous batch of captured mutations under a single grain hop. Returns the dense per-input offsets (`result[i]` is the offset assigned to `entries[i]`) in input order. Empty input returns an empty list and performs no provider work. The whole batch coalesces into one provider flush when under `WalMaxBatchEntries` / `WalMaxBatchBytes`; over-budget batches cut over across multiple flushes using the same in-flight cap as `AppendAsync`. |
| `ReadAsync(long fromSequence, int maxEntries, CancellationToken)` | Read a contiguous page from `fromSequence`. Returns a `WalShardPage` with the entries and the `NextSequence` cursor. Validates `fromSequence >= 0` and `maxEntries >= 1`; out-of-range reads return `WalShardPage.Empty(fromSequence)`. |
| `GetNextSequenceAsync(CancellationToken)` | Returns the sequence the next append will use. |
| `GetLiveEntryCountAsync(CancellationToken)` | Returns the number of live entries currently persisted, computed as `highest - lowest + 1` against the storage provider. Drops by the trimmed prefix length once `IWalStorageProvider.TrimAsync` runs (driven by `ILatticeWalGc`), so dashboards, alerts, and the back-pressure health check observe the persisted footprint rather than a monotonically-growing offset counter. |
| `GetEntryCountAsync(CancellationToken)` | **Obsolete** trim-unaware diagnostic helper retained for one minor version. Returns `_nextOffset` (the next sequence to be assigned). Use `GetLiveEntryCountAsync` for the trim-aware live count. |

Saga terminal mutations (`MutationKind.TxCommit` / `TxAbort`) carry their
shard index in `mutation.Key` as a base-10 invariant-culture string; the
commit-log writer maps that shard index to a WAL partition by taking
`shardIndex % WalPartitions`. When the shard count exceeds the partition
count, multiple shards collapse onto the same WAL partition; receivers
dedupe by `TransactionId`, so multiple terminal appends with the same id
are idempotent on the apply side.

### Per-tree `WalPartitions` pin resolution on the hot path

`LatticeOptions.WalPartitions` is **pinned per tree** in the tree
registry at first `RegisterAsync` and is tree-immutable thereafter, so
the writer-side routing and the activation-time materialiser can never
disagree on the partition fan-out shape. The pinned value is exposed
through `LatticeOptionsResolver.GetWalPartitionsAsync(treeId)`, a
hot-path-optimised entry point that returns an already-completed
`ValueTask<int>` on a cache hit and falls back to a one-shot
`ILatticeRegistry.GetEntryAsync` grain RPC only on the first call per
tree per silo. The foreground commit-log writer uses this fast path on
every `AppendAsync` / `AppendBatchAsync`, so `WalCommitLogWriter` never
serialises through the cluster-singleton registry activation on the
write path. The full `LatticeOptionsResolver.ResolveAsync` (used by
admin grains and the activation-time materialiser) also populates the
fast-path cache as a side effect, so any tree touched by any caller
becomes cache-warm for subsequent writer calls.

### Activation-time replay under `WalPartitions > 1`

The leaf grain's activation-time materialiser is partition-aware. When
`LatticeOptions.WalPartitions > 1` the activation hook iterates
`[0, WalPartitions)` and, for each partition, runs an independent
fall-off-log classification, slice read loop, and projection-checkpoint
advance. Per-partition state lives on the additive
`LeafNodeState.ProjectionCheckpointOffsetsByPartition` slot (`long[]?`);
partition 0 is mirrored into the legacy scalar
`ProjectionCheckpointOffset` slot for downgrade safety. The per-leaf
saga pending-tx clamp is also partition-scoped: each prepared mutation
records the `(transactionId, partition)` pair it arrived under, and the
projection-checkpoint advance for partition `P` is clamped behind
`(min unresolved prepare offset for P) - 1` so cross-partition offsets
are never compared.

Each leaf reports one cursor per partition to the WAL cursor registry
under consumer ids of the form
`_lattice_materialiser_{treeId}_{leafGrainId}_{partition}`, so the
per-shard WAL GC trims each partition independently against its own
slowest consumer. On the legacy default `WalPartitions = 1` the
unsuffixed `_lattice_materialiser_{treeId}_{leafGrainId}` shape is
preserved so a host that has never enabled multi-partition replay is
wire-compatible with its existing cursor registrations.

## Origin cluster id stamping

Every WAL record carries `OriginClusterId` so multi-site receivers can
attribute the origin and break replication cycles. The stamp comes from
two sources, applied in priority order at the producer-side
`WalRecordConverter.ToWalRecord(...)` call site:

1. **`mutation.OriginClusterId` wins when present.** A remote replay path
   stamps the upstream cluster id onto the mutation before it reaches the
   WAL writer; the converter preserves that value verbatim.
2. **Fallback to the resolver-supplied local cluster id.** When the
   mutation arrives with `OriginClusterId == null` - the foreground commit
   path on a host where the replication observer has not yet stamped - the
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
the producer recorded - required for the replication-side loop-prevention
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
the grain instance; up to `WalMaxPendingBatches` flushes can be in
motion against `IWalStorageProvider.AppendBatchAsync` simultaneously,
each independently completing per-caller `TaskCompletionSource<long>`
instances when the provider acknowledges durability. Offset assignment
remains serialised under the grain turn, so each in-flight flush owns a
strictly-increasing, non-overlapping offset window by construction.

```text
AppendAsync(entry)
    │  assigns offset = _nextOffset++
    │  appends WalEntry to _pendingBatch
    │  parks a TCS in _pendingAcks
    │  if _inFlight.Count == 0     → StartFlush()
    │  if would-overflow batch AND _inFlight.Count < cap → StartFlush()
    │  if would-overflow batch AND _inFlight.Count >= cap → await head
    ▼
returns await tcs.Task    ◄── completes once provider acks the batch
```

Two batch limits are enforced at append time:

| Option | Default | Trigger |
|---|---|---|
| `WalMaxBatchEntries` | `100` | Adding the new entry would push the pending count above the cap; the current batch is flushed first, then the new entry starts the next batch. |
| `WalMaxBatchBytes` | `4 MB` | Adding the new entry's exact serialised size would exceed the byte budget; same cutover. The grain measures every captured `WalRecord` through the registered `IWalRecordSizer` (default: `OrleansBinaryWalRecordSizer`, which serialises through the canonical Orleans-binary codec via a counting `IBufferWriter<byte>` so no payload buffer is materialised). The budget is an exact ceiling, suitable for sizing against backends with hard transactional limits (e.g. the Azure Table Storage 4 MB batch cap). |
| `WalMaxPendingBatches` | `16` | Maximum number of in-flight + just-started flushes the grain holds against the provider concurrently. The pre-6.1.0 default was `1`, which reproduced the original single-in-flight protocol bit-for-bit; the v6.1.0-v6.2.x default of `8` raised pipeline depth so writer-side bursts coalesced against higher-latency durable providers (e.g. Azure Tables). The post-v6.2 default of `16` was measured on Standard_D4as_v5 + Azure Tables Standard at 4,000 keys/s offered load to give a +57% increase in steady-state silo throughput at the 4k:5 rung with no reliability regression; see [WAL tuning](wal-tuning.md) for the storage-account-throughput envelope above which the dual-knob fan-out collapses to `429` throttling. The cap is the only synchronisation point new appends see, so cap values above the steady-state burst depth do not buy further throughput. Set explicitly to `1` to opt back into the legacy strict-serial-per-shard shape. |
| `WalFlushTimeout` | `15 s` | Upper bound on how long a single flush may take before the grain abandons the wait, faults the flush, resynchronises the dense-offset tail from the provider, and drains the chain so callers retry. Set to `Timeout.InfiniteTimeSpan` to restore the historical unbounded await. See [Flush deadline](#flush-deadline). |

Cutovers below the in-flight cap start a fresh flush immediately;
cutovers at the cap await the oldest in-flight flush before starting
another, which provides natural back-pressure under sustained burst
load.

### Flush deadline

Each in-flight flush is bounded by `WalMaxPendingBatches`; the cap is the
only synchronisation point new appends see. If one flush never settles,
its slot never leaves the in-flight chain, the chain saturates at the
cap, and every subsequent append back-pressures behind a flush that can
never complete - a steady-state stall with no fault and no activation
recycle. A provider call can fail to settle for reasons outside the
grain's control: a partition left half-activated by a placement/reshard
race, an SDK retry loop that never gives up, or a backend that simply
stops responding.

`WalFlushTimeout` (default 15 s) bounds the flush so that hang becomes a
recoverable `TimeoutException` routed through the normal
[append-failure path](#append-failure-semantics): the tail is
resynchronised from the provider and the chain drains, so callers that
retry observe a healthy grain.

The bound is enforced in two places, deliberately:

1. The deadline's cancellation token is passed to the provider call, so a
   co-operative provider stops its own work promptly when the deadline
   trips.
2. The grain **also** bounds its own `await` on the provider task with
   `Task.WaitAsync(deadline)`. A provider whose hang does not observe the
   token - a non-cancellable SDK wait, a retry loop that swallows
   cancellation, or a genuinely wedged half-activated partition - would
   otherwise leave the grain awaiting forever even though the deadline has
   fired. Bounding the grain's own wait abandons the un-cancellable
   provider task (its slot is removed and its eventual completion is
   harmlessly unobserved) so the chain drains regardless of whether the
   provider honours cancellation.

Bounding only the *call* (passing the token) is not sufficient on its own;
bounding the *wait* is what makes the deadline wedge-proof against
uncooperative providers.


### Batched leaf write path

Bulk-write entry points on the leaf collapse their per-key WAL grain
hops into a single batched dispatch through
`ICommitLogWriter.AppendManyAsync`, which the default
`WalCommitLogWriter` implementation groups by WAL partition and forwards
to `IWalShardGrain.AppendBatchAsync`. The leaf entry points that flow
through this path are:

| Entry point | Caller |
|---|---|
| `BPlusLeafGrain.SetManyAsync` | Foreground `Lattice.SetManyAsync` / `TypedLattice.SetManyAsync`. |
| `BPlusLeafGrain.MergeEntriesAsync` | Sibling redistribute, snapshot restore, replication-apply, and the bulk-load topology assembly invoked by `ShardRootGrain.BulkLoadAsync` / `BulkLoadRawAsync` / `BulkAppendAsync`. |
| `BPlusLeafGrain.MergeManyAsync` | Cross-shard migration on shard split and online-reshard. |

For an N-key batch routed to a single WAL partition the grain-hop count
drops from O(N) to one. Multi-partition batches fan out one
`AppendBatchAsync` call per touched partition and the writer reassembles
the dense per-input offsets in input order. The whole batch coalesces
into a single provider flush when it fits inside the
`WalMaxBatchEntries` / `WalMaxBatchBytes` window; larger batches cut
over across multiple flushes using the same in-flight cap as
single-entry `AppendAsync`.

The `LeafWriteDuration` histogram records one sample per batched
dispatch on the merge channel (`kind=merge`) rather than one sample per
entry, so percentile reads of the merge channel are not biased by batch
size.

### Activation recovery

On grain activation, `OnActivateAsync` calls
`IWalStorageProvider.GetHighestOffsetAsync` and sets `_nextOffset =
highest + 1`. The persisted log is the single source of truth for the
next-offset counter - the grain holds no Orleans grain state of its own.

### Deactivation drain

`OnDeactivateAsync` awaits every in-flight flush in chronological order
and then triggers (and awaits) a final flush of any remaining pending
entries, so a graceful deactivation never leaves a caller observing a
hung TCS regardless of the configured `WalMaxPendingBatches`.

The drain is bounded by `WalDrainBudget` (default 75 seconds = `5 *
WalFlushTimeout`). At drain entry the per-activation drain
`CancellationTokenSource` is signalled - every in-flight flush has
already linked its per-flush deadline into this source at construction
time, so a co-operative provider's `AppendBatchAsync` cancellation
token cancels in one shot and the flush surfaces a `TimeoutException`
routed through the normal failure handler. The chain is then awaited
to settle naturally for up to the budget; any slot that has not
unlinked when the budget expires is force-faulted with a typed
`TimeoutException` faulted onto every parked ack TCS so callers are
released rather than parking through the rest of host shutdown. The
`orleans.lattice.wal.shard.drain.budget.expirations` counter and
`orleans.lattice.wal.shard.drain.budget.force_faulted_slots` histogram
(both tagged `tree` and `shard`) attribute every budget-driven
force-fault per partition.

This bound defends against the saturating-storage-account wedge: when
the provider call's await is parked behind an SDK retry loop in
pre-attempt back-off, the per-flush `WalFlushTimeout` may not fire
promptly (the SDK observes cancellation only between attempts, not
during back-off), so without the drain budget a chain with N in-flight
slots could hold the deactivation indefinitely. With the budget the
chain settles within bounded time of the SIGTERM regardless of whether
the underlying provider is healthy. Set `WalDrainBudget` to
`InfiniteTimeSpan` to disable the ceiling and restore the historical
unbounded-drain behaviour.

### Append-failure semantics

A flush failure is fail-fast for every affected caller:

1. A *sticky-failure* latch is set the moment any flush in the chain
   throws. New `AppendAsync` calls (and the cutover loop's in-progress
   waiters) short-circuit with that exception until the post-failure
   resync clears the latch, so a fault that already faulted later
   windows is never masked by a fresh successful append.
2. Every TCS in the failed window is faulted with the underlying storage
   exception.
3. Every TCS in *every later in-flight window* is faulted with the same
   exception. Their provider calls may still be in motion - the chain
   waits for them to settle - but their result-setting is short-circuited
   so they never produce a success that contradicts the failure latch.
4. Every TCS in the *currently-accumulating* pending batch is faulted -
   those entries had been assigned offsets above the failed window, so
   their offsets are logically orphaned.
5. Once the chain drains, the grain re-reads
   `IWalStorageProvider.GetHighestOffsetAsync` to recover the provider's
   real tail. Concurrent later flushes may have already committed
   against now-orphaned offset windows; the resync restores the dense-
   offset invariant against the provider rather than against the failed
   window's start. The sticky-failure latch is then cleared and new
   appends resume.

This contract makes WAL-append failures observable inline at the
originating writer rather than being silently coalesced into a later
batch.

> **Contributor note - synchronously-completing providers.** `FlushAsync`
> starts with `await Task.Yield()` so the returned `Task` is observably
> incomplete by the time `StartFlush` stores it on the in-flight slot.
> Without that yield, an `IWalStorageProvider` whose `AppendBatchAsync`
> returns a synchronously-completed task (the in-memory provider does
> this) would run the entire flush body inline before the slot is fully
> initialised, including the chain-remove in the `finally` block - and
> the chain invariant ("every slot in `_inFlight` carries a task that
> completes when its provider call settles") would be violated. Any
> future refactor of the flush loop must preserve this yield.

## Recovery and rebuild


When a leaf grain activates, it rebuilds its in-memory projection by replaying
the WAL through `ILeafReplayCoordinatorGrain`. Three cases:

- **Tail replay.** The last persisted projection checkpoint is at offset *N*,
  the WAL head is at offset *M*, and `M - N` is bounded by the checkpoint
  interval. The coordinator streams entries `(N, M]` and applies each via
  `ILeafProjection.Apply`. Replay is in-process and typically completes in a
  few milliseconds.
- **Fresh-leaf tail replay.** A leaf created mid-run by a split (or by the
  first write to a virgin shard) carries the -1 "nothing applied" sentinel
  in its `ProjectionCheckpointOffset`. The fall-off-log detector exempts the
  sentinel from the replay-budget and trim triggers: the leaf has no
  projection state to lose, and the per-leaf range filter inside the
  materialiser (`ShouldApplyDuringReplay`) drops every WAL entry that falls
  outside this leaf's `[LowKeyInclusive, HighKeyExclusive)` ownership range
  on iteration, so the iteration cost is bounded by the leaf's own range
  rather than by the WAL head. The replay still bounds the per-slice work
  via `ReplaySliceBudget` on the read side.
- **Fall-off-log rebuild.** The persisted checkpoint is older than the WAL trim
  watermark - the entries it would replay are no longer available. The
  coordinator falls back to `ILeafProjection.Rebuild`, which drains the leaf's
  key range from `ILeafSnapshotProvider` (an internal seam over the streaming
  as-of snapshot export), persists the snapshot offset as the new checkpoint,
  then tail-replays the remaining WAL entries past the snapshot offset. The
  default policy is `ProjectionRebuildPolicy.SnapshotThenWal`; alternative
  policies (`FullRebuildFromWal`, `Fail`) are described on the enum and in
  [`projection-rebuild.md`](projection-rebuild.md). The grain-state row holds
  only tree metadata (sibling pointers, tree id, shard index, key range,
  split lifecycle, last-compaction-version) plus the projection checkpoint
  snapshot - it is never the source of truth for entry values.

In all three cases, the projection that a reader observes after activation is
byte-equivalent to the projection at the moment the leaf last deactivated (or
empty, for a freshly-created leaf).

## Projection checkpoint

To keep tail-replay bounded, the leaf flushes a **projection checkpoint**
durably whenever the elapsed wall-clock time since the last flush reaches
`MaterialiserCheckpointInterval` (default: 5 seconds) **or** the count of
unflushed advances reaches `MaterialiserCheckpointEntries` (default: 5 000),
whichever happens first. The checkpoint is a single grain-state write that
captures the in-memory entries, the local HLC, the version vector, and the
WAL offset of the last applied mutation. On the next activation the replay
coordinator starts from the checkpoint offset rather than from zero.

The checkpoint is **not** an additional durability boundary - it's a replay-cost
optimization. If a checkpoint flush fails, the next activation simply replays
more WAL entries; correctness is unaffected. The checkpoint is also flushed
opportunistically in `OnDeactivateAsync` so a graceful shutdown doesn't lose an
already-pending advance.

## Trim and GC

The WAL grows monotonically and must be trimmed. Trim is driven by
`ILatticeWalGc`, a per-tree single-pass collector that advances the
per-shard trim watermark to the largest contiguous prefix that **every**
registered consumer has already acknowledged.

The collector ships in `Orleans.Lattice` so single-cluster deployments
that never call `AddLatticeReplication(...)` still get durable WAL
maintenance. The predicate is expressed against `min(cursor across
registered consumers)` - not `min(cursor across remote peers)` - so the
local in-memory projection (the materialiser that rebuilds the leaf
state from the WAL on activation) is just another consumer. A lagging
materialiser pins the log exactly the same way a lagging remote peer
does.

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
published to the `IWalCursorRegistry`. The cursor branch is
gated on `minCursor > HybridLogicalClock.Zero` so range-delete entries (which
carry `HybridLogicalClock.Zero` by design) are never trimmed under an unset /
zero cursor.

`ttlCeiling` is the hard ceiling configured by
`LatticeOptions.WalRetention` (mirrored from
`LatticeReplicationOptions.WalRetention` on replicated trees). When set,
a lagging consumer that pins the log past the ceiling is intentionally
allowed to "fall off the log" so disk usage stays bounded; that consumer
detects the gap on its next read and re-bootstraps via the fall-off-log
path described in [`projection-rebuild.md`](projection-rebuild.md).

The scan is conservative: the first non-eligible entry per shard stops the
walk for that shard. WAL offsets are dense and append-only but HLC
`WallClockTicks` is mostly-monotonic-with-skew, so a stop-at-first-miss walk
preserves correctness while a more aggressive scan would risk trimming an
entry younger than a still-pinned later entry.

### Consumer registration

Every consumer of the change feed - the outbound replication ship loop,
in-process bridges, custom transports, and the local in-memory materialiser -
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

The default `InMemoryWalCursorRegistry` is process-local and loses
its state on silo restart. A host that needs cross-restart durability
registers its own `IWalCursorRegistry` implementation via DI
before calling `AddWalCursorRegistry(...)` (or `AddLatticeReplication(...)`,
which calls it transitively).

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

### How the retention bounds interact

WAL retention is governed by **three layered bounds**. The GC only ever
removes entries that all applicable bounds agree are safe to drop, so the
effective trim frontier is whichever bound binds first.

| Bound | Knob | Default | What it caps | Can it trim past a live consumer? |
|---|---|---|---|---|
| Consumer frontier | *(none - always on)* | always on | The hard floor: `min(cursor)` across every registered consumer, intersected with the causal-stable frontier. | **No.** This is the durability invariant. |
| Wall-clock TTL | `WalRetention` | `null` (disabled) | Entries older than `now - WalRetention` fall off the log even if a consumer still pins them. | **Yes** - this is the only bound that does. |
| Advisory byte ceiling | `WalMaxRetainedBytes` | `null` (disabled) | Schedules byte-pressure trim work when retained bytes exceed the ceiling, but only *within* the consumer frontier. | **No** - it surfaces an over-threshold signal instead. |

`WalBytePressureReclaimTarget` (default `0.8`) is not itself a bound: it is the
low-water hysteresis fraction of `WalMaxRetainedBytes` that disarms the
byte-pressure policy once a trim has reclaimed enough, so a tree hovering near
the ceiling is not trimmed on every pass. It is inert unless
`WalMaxRetainedBytes` is set.

> **Production caution - set at least one absolute cap.** With every knob at
> its default (`WalRetention = null`, `WalMaxRetainedBytes = null`), the *only*
> active bound is the consumer frontier. The WAL shrinks as consumers catch up,
> but a **permanently lagging or dead consumer pins the log and grows it without
> limit** - the advisory byte ceiling deliberately will not rescue you, because
> it never trims past a live cursor. `WalRetention` is the only knob that trims
> past a stuck consumer, so any deployment where unbounded growth is
> unacceptable should set `WalRetention` (a wall-clock floor on consumer lag)
> and, where a hard size budget matters, `WalMaxRetainedBytes` as well. A
> consumer that falls off the log re-bootstraps via the fall-off-log path in
> [`projection-rebuild.md`](projection-rebuild.md).

### Scheduling

`ILatticeWalGc.RunOnceAsync(treeName)` is a single-pass GC invocation.
The library does **not** install a background timer - the host owns the
cadence so it can integrate with whatever scheduling infrastructure it
already uses (Orleans reminders, hosted services, external schedulers). A
typical inner-loop period is 30 to 60 seconds per replicated tree.

```text
LatticeWalGcReport report = await gc.RunOnceAsync(
    treeName: "orders",
    cancellationToken: cancellationToken);

// The report exposes the inputs and the outcome:
//   - report.MinCursor       - minimum cursor across registered consumers, or null
//   - report.TtlCeilingHlc   - TTL ceiling synthesised from WalRetention, or null
//   - report.ShardsScanned   - number of WAL shards walked
//   - report.CausalStable    - pointwise-min VersionVector across consumers, or null
//   - report.EntriesTrimmed  - total entries removed across all shards
```

### Metrics

The GC publishes one counter on the `orleans.lattice` meter:

| Instrument | Tags | Description |
|---|---|---|
| `orleans.lattice.wal.entries_trimmed` | `tree` | Total WAL entries removed by a GC pass. Incremented only when the pass trimmed at least one entry. |

## Relationship to replication

Cross-cluster replication is an **additional consumer** of the same WAL - not
a parallel pipeline. The replication change feed reads `LatticeMutation`
envelopes from the WAL, applies them on the peer cluster via
`IReplicationApplier` (which calls into the same `ILeafProjection.Apply` that
local commit and local replay use), and acknowledges its progress through the
same cursor registry that GC consults.

The single-cluster and multi-cluster code paths are identical up to the point
where replication transports an envelope across a network boundary. There is
no "replication mode" that changes how a foreground commit durabilizes - the
commit always appends to the local WAL, and replication is purely additive.

See [`../lattice.replication/replication-drivers.md`](../lattice.replication/replication-drivers.md)
for the driver-grain scheduling model that consumes the WAL on each peer.

## Configuration

The WAL pipeline exposes a small number of knobs on `LatticeOptions`. Defaults
suit most workloads.

| Option | Default | Purpose |
|---|---|---|
| `MaterialiserCheckpointInterval` | 5 seconds | Time-driven flush of any pending projection-checkpoint advance. Set to `Timeout.InfiniteTimeSpan` to disable the time trigger and rely solely on the entry-count trigger. |
| `MaterialiserCheckpointEntries` | `5_000` | Entry-count trigger: forces a checkpoint flush once this many advances are pending, regardless of `MaterialiserCheckpointInterval`. Bounds the worst-case replay cost when the steady-state apply rate is high. |
| `MaxLeafReplayEntries` | `10_000` | Upper bound on the entries `ILeafReplayCoordinatorGrain` streams in a single tail replay. A leaf whose backlog exceeds this falls back to the rebuild path indicated by `ProjectionRebuildPolicy`. The budget is skipped for a freshly-created leaf whose persisted `ProjectionCheckpointOffset` is the -1 "nothing applied" sentinel: a fresh leaf has no projection state to lose, and the per-leaf range filter inside the materialiser bounds the actual work by the leaf's own range rather than by the WAL head. |
| `LeafProjectionRetention` | 7 days | Age beyond which a persisted checkpoint is considered stale; the next activation falls off-log and rebuilds. Set to `Timeout.InfiniteTimeSpan` to disable the age-based trigger. |
| `ProjectionRebuildPolicy` | `SnapshotThenWal` | Recovery strategy when a fall-off-log trigger fires (snapshot + WAL tail, or full rebuild from the authoritative source). |
| `WalRetention` | `null` (disabled) | Wall-clock hard ceiling on retention: entries older than `now - WalRetention` fall off the log even if a consumer still pins them. The only knob that trims past a stuck consumer - set it where unbounded growth is unacceptable. See [How the retention bounds interact](#how-the-retention-bounds-interact). |
| `WalMaxRetainedBytes` | `null` (disabled) | Advisory per-tree byte ceiling that schedules byte-pressure trim work, but only within the safe consumer frontier. See [Tree Storage](tree-storage.md#advisory-byte-pressure-wal-retention). |
| `WalBytePressureReclaimTarget` | `0.8` | Low-water hysteresis fraction of `WalMaxRetainedBytes` that disarms the byte-pressure policy after a trim. Inert unless `WalMaxRetainedBytes` is set. |

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

- [`wal-storage-providers.md`](wal-storage-providers.md) - pluggable backend
  contract and the in-memory / Azure Table providers.
- [`projection-rebuild.md`](projection-rebuild.md) - drift detection and the
  fall-off-log rebuild path.
- [`tombstone-compaction.md`](tombstone-compaction.md) - how reaped tombstones
  interact with WAL retention.
- [`configuration.md`](configuration.md) - the full `LatticeOptions` surface.
- [`wal-causal-plus.md`](wal-causal-plus.md) - causal+ entry-schema
  extension (vector clock + dependency summary slots on `WalRecord`).
- [`../lattice.replication/wal.md`](../lattice.replication/wal.md) - the
  replication-side overlay: per-shard sharded sink, producer-side filters,
  and the `MutationCategory.Maintenance` skip.
