# Write-Ahead Log

This document describes how Orleans.Lattice uses a **write-ahead log (WAL)** as the
sole foreground-commit durability boundary for every leaf grain mutation. The WAL
is the primary on-disk truth: in-memory projections, secondary indexes, and
replication consumers are all rebuilt from it on activation or recovery.

If you're looking for a different angle on the WAL:

- For the pluggable storage backend (in-memory, local file, or Azure Table) see
  [`wal-storage-providers.md`](wal-storage-providers.md).
- For how the in-memory projection is rebuilt from the WAL on activation see
  [`projection-rebuild.md`](projection-rebuild.md).
- For the replication-side overlay (partitioned sink, producer-side
  filters, and the `MutationCategory.Maintenance` skip) see
  [`../lattice.replication/wal.md`](../lattice.replication/wal.md).
- For the causal+ entry-schema extension (vector clock + dependency
  summary slots on `WalRecord`) see [`wal-causal-plus.md`](wal-causal-plus.md).

## What the WAL is

The WAL is an **append-only log of `LatticeMutation` envelopes**, split into WAL
partitions. Each partition owns a monotonically increasing offset space starting
at zero, and an acknowledged offset is never reused or reordered. Offsets are
dense in normal operation. A flush that fails leaves its window unacknowledged
and the partition resumes from the provider's durable tail, so that window is
either reassigned (when nothing above it landed) or left as a permanent gap
(when a later concurrent window had already committed), which readers observe
honestly - see [Append-failure semantics](#append-failure-semantics).

```text
partition 0:  [0]Set k1   [1]Set k2     [2]Delete k1   [3]Set k3      ...
partition 1:  [0]Set a    [1]DelRange   [2]Set b       [3]Set c       ...
partition 2:  [0]Set x    [1]Set y      [2]Set z       [3]Delete y    ...
```

A `LatticeMutation` carries everything a replay or replication consumer needs to
reconstruct the effect of one foreground operation: the tree id, the operation
kind (`Set` / `Delete` / `DeleteRange`, plus the `TxCommit` / `TxAbort` saga
terminals and the `Tombstone` compaction reap), the key (and optional
end-exclusive key for ranges), the LWW timestamp (an `HybridLogicalClock`), the
value bytes (or tombstone marker), the optional TTL expiry, the origin cluster
id, the vector clock, the optional transaction id, the maintenance category, the
optional delta payload and the merge mode, together with the atomic-batch
metadata (batch size, index and shard count, and the prepared flag), the merge
and backstop flags, the authoring shard index, the keys a predicate-filtered
range delete matched, the cross-tree operation id and participants, and
whether a prepared write's stamp is its prepare's original stamp (see
[A commit applies each value at its prepare stamp](atomic-writes.md#a-commit-applies-each-value-at-its-prepare-stamp);
the field is additive, so a record written before it existed reads as not
original), and whether the stored value is migrated (see
[A later write the split imports is not dropped over the saga's value](atomic-writes.md#a-later-write-the-split-imports-is-not-dropped-over-the-sagas-value);
also additive, read as not migrated on an older record). The WAL
stores each envelope as its durable twin, `WalRecord` - the shape that is
encoded onto storage and shipped to replication peers, which also carries the
causal+ dependency summary - and a storage provider or `IMutationObserver` sees
it projected back to a `LatticeMutation`.

## Why WAL-as-sole-commit-point

Every foreground commit (set / delete / range-delete) appends exactly one WAL
record **before** the in-memory projection sees the write.
The WAL append is the moment the operation is durable; everything after that is
materialisation.

This is a deliberate trade. The wins:

- **Single durability boundary.** There is exactly one write-ahead step per
  commit. The in-memory projection has no independent durability guarantee;
  it is reconstructed from the WAL on activation.
- **Replay-driven recovery.** Activation rebuilds the in-memory projection from
  the leaf's latest durable snapshot plus a replay of the WAL past the offsets
  that snapshot covers - or of the whole readable WAL when the leaf has no
  usable snapshot. The WAL entry is the only thing a foreground commit
  must make durable before it returns; nothing else holds a second copy of the
  value that has to be kept in step with it.
- **Replication coupling.** A peer's change feed and the local commit log are
  the same byte stream. Cross-cluster replication is an additional consumer of
  the same WAL, not a parallel pipeline.

The cost:

- **Cold-activation replay cost is bounded by snapshot coverage and
  retention.** A cold activation replays the WAL past the offsets the leaf's
  latest snapshot covers, or the whole readable window when it has no
  snapshot, so the size of a replay depends on how recently the leaf captured
  a snapshot (its checkpoint persists drive those captures; see
  [`projection-rebuild.md`](projection-rebuild.md#snapshot-on-fall-off-safety-net))
  and on how much WAL is retained.
- **The WAL provider must be durable.** The default `InMemoryWalStorageProvider`
  is fine for tests and single-process samples but is not crash-safe; production
  deployments register a durable provider through its package's registration
  helper - `AddAzureTableWalStorage` from `Orleans.Lattice.Storage.AzureTable`
  or `AddFileWalStorage` from `Orleans.Lattice.Storage.File`, each of which also
  wires the cursor registry and WAL garbage collector a durable log needs. See
  [`wal-storage-providers.md`](wal-storage-providers.md#implementing-a-custom-provider)
  for what a custom provider registered through `AddWalStorage(...)` must add.

## Commit pipeline

Every leaf grain commit follows the same four-step pipeline. The order is
load-bearing - the WAL append must happen before any in-memory state mutates,
and the observer publish must happen after both, inside a commit-log scope.

```text
   build  -->  wal  -->  apply  -->  observer
   (HLC,       (append   (merge       (publish under
    entry,      the WAL   into         the commit-log
    WAL         record)   projection)  scope)
    record)
   in-mem      durable   in-mem       in-mem
```

1. **build** - Tick the local hybrid-logical clock and the version vector,
   construct the new entry's last-writer-wins value (or the tombstone), and
   build its `WalRecord`. The record captures the ambient `LatticeOriginContext`,
   `LatticeVectorClockContext` and `LatticeDeltaContext`, plus the ambient
   transaction id, maintenance category and atomic-batch stamps.

2. **wal** - Resolve the silo's commit-log writer and append the mutation.
   `AddLattice` always registers the writer, with the in-memory WAL provider as
   its baseline, so on a silo wired through `AddLattice` this step always runs;
   it is skipped only for a leaf that has no tree id yet (one created outside
   `ILattice`, for example by a test harness). Failures here propagate to the
   caller before any in-memory state has been touched.

3. **apply** - LWW-merge the value into the in-memory entry cache. If
   the leaf's entry count crosses `MaxLeafKeys`, or its live state bytes cross
   [`MaxLeafBytes`](configuration.md#maxleafbytes), trigger a split. This step
   is the only place that mutates the per-leaf in-memory state on the
   foreground path.

4. **observer** - If any `IMutationObserver` is registered, publish the
   post-commit mutation inside a `LatticeCommitLogContext` scope. The scope
   marker lets a replication-aware observer detect that the source of this
   mutation was the local commit log and short-circuit its loop-prevention
   logic so it doesn't re-append its own input back into the WAL.

The pipeline is implemented in
[`BPlusLeafGrain.CommitSetAsync`](../../src/lattice/BPlusTree/Grains/BPlusLeafGrain.cs)
and the mirror paths for `DeleteAsync`, `DeleteRangeAsync`, `MergeEntriesAsync`,
`MergeManyAsync`, and `CompactTombstonesAsync`. The `wal`, `apply` and
`observer` steps - plus `digest`, which hands the write's projection-digest
change to the parent internal node - record their elapsed wall-clock duration
to the `orleans.lattice.leaf.commit.duration` histogram tagged by `step`;
`build` is not timed separately. With the default
[`DigestCoalescingWindowMs`](configuration.md#digestcoalescingwindowms)
(5 ms) a write that changes the digest schedules, or joins, a publish the
leaf sends when the window elapses, outside the commit, so the `digest` step
includes the cross-grain publish itself only when the window is `0`. The
`orleans.lattice.leaf.write.duration` histogram times the leaf's grain-state
persists with no `kind` tag, and additionally carries
`kind`-tagged samples that time the WAL appends of merge traffic
(`kind=merge`), tombstone compaction (`kind=compact`) and the cross-migration
LWW backstop (`kind=backstop`). Ordinary set and delete commits carry no `kind`
tag; size them with the per-step commit histogram.

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
- **In-memory projection is reconstructable from the WAL and the leaf's
  snapshot.** The
  projection has no independent durability guarantee. The leaf still persists
  its grain-state row for the projection-checkpoint flush (below) and for
  tree-metadata paths (sibling pointer updates, tree-id stamping, split
  lifecycle, last-compaction-version snapshotting), but those persist
  *metadata*, not a fallback copy of the entry values. The grain-state row never
  stores committed entry values as a backup: apart from the replay ledger
  that records unresolved saga prepares and undrained deferred terminals
  verbatim (see
  [Resumable cold replay](projection-rebuild.md#resumable-cold-replay)), entry
  values live in the WAL until they reach a durable snapshot, after which the
  WAL GC may trim the snapshot-covered prefix. The ledger is empty in the
  steady state, but a backlog of sagas whose terminals never land grows it
  without limit (see
  [Tree Storage](tree-storage.md#sizing-surface-1---leaf-grain-state-row)).
- **Replication consumers see exactly the foreground commit ordering.** A peer
  replicating from this shard sees the same `LatticeMutation` envelopes in the
  same order that the local projection saw them. The WAL is the linearization
  point.

## WAL grain API

Each WAL partition is owned by an internal grain keyed
`{treeId}/{partition}` where `partition` is a stable FNV-1a hash of the key
reduced modulo the tree's pinned `LatticeOptions.WalPartitions` (default `8`). The grain is internal to the core and is the single producer-side
entry point for foreground commits and the read-back source for the
replication change feed.

| Member | Purpose |
|---|---|
| `AppendAsync(WalRecord, CancellationToken)` | Append a captured mutation. Returns the assigned dense per-partition sequence number. |
| `AppendBatchAsync(IReadOnlyList<WalRecord>, CancellationToken)` | Append a contiguous batch of captured mutations under a single grain hop. Returns the dense per-input offsets (`result[i]` is the offset assigned to `entries[i]`) in input order. Empty input returns an empty list and performs no provider work. The whole batch coalesces into one provider flush when under `WalMaxBatchEntries` / `WalMaxBatchBytes`; over-budget batches cut over across multiple flushes using the same in-flight cap as `AppendAsync`. |
| `ReadAsync(long fromSequence, int maxEntries, CancellationToken)` | Read a contiguous page from `fromSequence`, clamped to the durable gap-free prefix: no offset above a lower offset whose flush is still in flight, or whose abandoned flush may still land, is returned, so a cursor-advancing reader never skips a prefix hole a write can still fill. Returns a `WalShardPage` with the entries and the `NextSequence` cursor. Validates `fromSequence >= 0` and `maxEntries >= 1` (throwing `ArgumentOutOfRangeException`); a read at or beyond the durable prefix returns an empty page whose `NextSequence` is `fromSequence`. |
| `ReadFilteredAsync(long fromSequence, long toSequenceInclusive, int maxEntries, WalKeyFilter filter, CancellationToken)` | The leaf replay read (issue #3565). Examines at most `maxEntries` entries of `[fromSequence, toSequenceInclusive]`, clamped like `ReadAsync` to the durable gap-free prefix, and returns those the filter does not exclude, plus the last examined entry routing-only (key and kind, no payload) when it is excluded. `NextSequence` therefore still moves past everything examined, and an empty page still means an empty window. The grain re-applies the rule to whatever the storage provider yields, so no excluded payload crosses the grain boundary. Validates its arguments like `ReadAsync`. |
| `GetNextSequenceAsync(CancellationToken)` | Returns the sequence the next append will use. |
| `GetReadableHeadAsync(CancellationToken)` | Returns the head a reader may resume from: one past the highest sequence a read would expose. Lower than `GetNextSequenceAsync` while a flush is in flight, while an abandoned flush may still land, or while a trailing hole sits above every stored entry (issue #4621). |
| `GetTrimWatermarkAsync(CancellationToken)` | Returns the shard's trim watermark when a reader may trust it, otherwise `null`. See [Abandoned flushes, holes and the trim watermark](#abandoned-flushes-holes-and-the-trim-watermark). |
| `GetLiveEntryCountAsync(CancellationToken)` | Returns the number of live entries currently persisted, computed as `highest - lowest + 1` against the storage provider. Drops by the trimmed prefix length once `IWalStorageProvider.TrimAsync` runs (driven by `ILatticeWalGc`), so it reports the persisted footprint rather than a monotonically-growing offset counter; the state API's change observation reads it to refuse a resume point the GC has already trimmed. |
| `GetEntryCountAsync(CancellationToken)` | **Obsolete** trim-unaware diagnostic helper retained for one minor version. Returns `_nextOffset` (the next sequence to be assigned). Use `GetLiveEntryCountAsync` for the trim-aware live count. |

The grain also serves the replication shipper's bytes-shaped page read (the
same durable gap-free window, each entry carried as the payload bytes the
encoder wrote at append time, read through `IWalStorageProvider.ReadEncodedAsync`),
reports the provider's retained-payload and physical byte sizes for the shard
(`-1` when the provider does not support byte accounting), and fences itself
against appends and retires its activation during a placement move (see
[Moving a partition to another account](wal-storage-providers.md#moving-a-partition-to-another-account)).

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
every `AppendAsync` / `AppendManyAsync`, so `WalCommitLogWriter` never
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
are never compared. A prepare already recorded verbatim in the leaf's
durable replay ledger no longer clamps, because a resumed replay rebuilds
it from the ledger (see
[Resumable cold replay](projection-rebuild.md#resumable-cold-replay)).

Each leaf reports one cursor per partition to the WAL cursor registry
under consumer ids of the form
`_lattice_materialiser_{treeId}_{leafGrainId}_{partition}`, so the
WAL GC trims each partition independently against its own
slowest consumer. With `WalPartitions = 1` the unsuffixed
`_lattice_materialiser_{treeId}_{leafGrainId}` shape is preserved, so a
tree pinned to a single partition stays wire-compatible with its existing
cursor registrations.

### Surviving a full restart

The WAL GC trims each partition through the minimum cursor across the
consumers currently registered in the WAL cursor registry. The default
registry is process-local and is wiped when a silo restarts, so on a
cold start a forward consumer that persists its own cursor (the
replication shipper re-reports its durably-advanced cursor eagerly on
activation) could momentarily be the only registered consumer for a
tree, while a dormant leaf - which re-registers its pin only lazily, on
its next activation and checkpoint flush - is absent. Without a
backstop the GC would compute its trim floor over the forward consumer
alone and trim the WAL past the leaf's durable checkpoint, discarding
the committed-but-not-yet-checkpointed tail the leaf still needs to
replay.

In-memory cursor reporting is always on (the lightweight
`InMemoryLeafCursorReporter` wired by `AddLattice`), but the durable
backstop below is the opt-in layer: it engages only when the host adds
the durable-pin-aware reporter through `AddWalCursorRegistry` (directly,
or transitively via durable WAL storage / views / replication). To close
that window each leaf then also mirrors its checkpoint frontier
into a sharded cluster-wide durable pin store (`WalMaterialiserPinShards`
grain activations per tree, each persisted through the configured grain
storage; the GC reads every shard under the current count plus the legacy
unsuffixed key, so raising the shard count loses no trim floor - lowering
it is covered under
[`WalMaterialiserPinShards`](configuration.md#walmaterialiserpinshards)). The
mirror is fire-and-forget and coalesced off the checkpoint path - a
debounce keeps a busy every-write-checkpoint leaf from issuing a durable
write per write - and because a too-low durable pin only ever retains
*more* WAL, a coalesced or slightly stale pin is always safe. On each
pass the GC consults the durable pins and lowers its HLC trim floor for any
materialiser consumer that is **missing** from the in-memory registry
(a consumer that is present has a fresher in-memory cursor already
folded into that floor; one
registered only with a `Zero` block-pin-only cursor contributed nothing
to it, so it is treated as missing). The checkpoint offsets the pins
record are used for every leaf, present or not: they form the durable
materialiser offset floor, which stops each trim scan and can overrule
the in-memory cursor (see [Predicate](#predicate)). A leaf seeds a
durable `Zero` "block" pin when it is created - before its data becomes
reachable in the WAL - and that pin holds the WAL head for the leaf until it
produces its first checkpoint; the TTL ceiling (`LatticeOptions.WalRetention`) still bounds
growth in that state. Because durable WAL storage is the deployment
shape most exposed to this hazard, `AddAzureTableWalStorage` and
`AddFileWalStorage` automatically wire the cursor registry and WAL GC
seams (both idempotent) so durable storage never ships without the
durable floor.

Two cold-path **retention barriers** turn the durable pin from a
best-effort mirror into an authoritative trim floor, so a write-once
leaf that goes dormant can never have the shared WAL trimmed past its
checkpoint (the "fall off the log" wedge). Neither adds a synchronous
durable write to the steady-state checkpoint path:

- **First real frontier.** The first time a leaf crosses from its `Zero`
  block pin to a real checkpoint frontier it *awaits* a batched report to
  the pin store (once per activation) instead of the fire-and-forget mirror,
  so the floor leaves `Zero` promptly rather than after a debounce window.
  The pin store merges the report at once but coalesces its own durable
  write, so a crash before that write lands leaves the durable pin at its
  last persisted value - at worst the block pin seeded at the leaf's birth -
  which only retains more WAL. Every subsequent advance uses the debounced
  fire-and-forget mirror.
- **Graceful deactivation.** On deactivation, after its final checkpoint
  flush (which publishes the pin it persists) and a snapshot capture for any
  checkpointed partition no snapshot covers yet, the leaf *awaits* the same
  batched pin-store report of its current frontier. It skips that pin-store call, counting
  the skip on `orleans.lattice.leaf.deactivation.barrier.elided`, only when
  a pin this same deactivation already had acknowledged covers every
  partition. So a
  leaf can never go dormant on a clean shutdown (and then have the WAL
  trimmed past its checkpoint across a restart) without leaving a correct
  durable floor behind.

Both writes go through the pin store's monotonic-max merge, so they are
idempotent and never roll a pin backwards, and both swallow transient
failures so neither deactivation nor the checkpoint path is ever blocked.
A write that faults is not recorded as written, so the next report for
those pins retries it rather than being coalesced away.

## Origin cluster id stamping

Every WAL record carries `OriginClusterId` so multi-site receivers can
attribute the origin and break replication cycles. The stamp comes from
two sources, applied in priority order when the silo's commit-log
writer routes the record to its WAL partition:

1. **`mutation.OriginClusterId` wins when present.** A remote replay path
   stamps the upstream cluster id onto the mutation before it reaches the
   WAL writer; the writer preserves that value verbatim.
2. **Fallback to the resolver-supplied local cluster id.** When the
   mutation arrives with no `OriginClusterId` (null or empty) - the foreground commit
   path on a host where the replication observer has not yet stamped - the
   writer asks `ILatticeOriginClusterIdResolver.Resolve(treeId)` for the
   local id.

`ILatticeOriginClusterIdResolver` is a public seam in the `Orleans.Lattice`
namespace. The core ships
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

The WAL grain's append path implements a turn-safe batching protocol:
the exclusive-turn single append and the interleaving batch append that
every single-entry append takes by default (`WalBatchedSingleEntryAppends`,
see [Batched leaf write path](#batched-leaf-write-path)) run the same
cutover protocol. Each call accumulates into an in-memory pending batch held
on the grain instance; up to `WalMaxPendingBatches` flushes can be in
motion against `IWalStorageProvider.AppendEncodedBatchAsync` simultaneously,
each independently completing per-caller `TaskCompletionSource<long>`
instances when the provider acknowledges durability. Offset assignment is
serialised under the shard's internal state gate, so each in-flight flush
owns a strictly-increasing, non-overlapping offset window by construction
even while batch appends interleave.

```text
append(entry)
    |  encodes the record once into a pooled buffer
    |  while adding it would overflow the pending batch:
    |      in-flight flushes <  cap -> start a flush of the pending batch
    |      in-flight flushes >= cap -> await the oldest in-flight flush
    |  assigns offset = next offset, then advances the counter
    |  appends the encoded entry to the pending batch and parks an ack
    |  in-flight flushes < cap -> start a flush
    v
returns the ack          <-- completes once the provider acks the batch
```

The batching limits and flush bounds:

| Option | Default | Trigger |
|---|---|---|
| `WalMaxBatchEntries` | `100` | Adding the new entry would push the pending count above the cap; the current batch is flushed first, then the new entry starts the next batch. |
| `WalMaxBatchBytes` | `4 MB` | Adding the new entry's exact serialised size would exceed the byte budget; same cutover. The grain encodes every captured `WalRecord` once through the registered `IWalRecordEncoder` (default: `OrleansBinaryWalRecordEncoder`, the canonical Orleans-binary codec) into a pooled buffer; the encoded length feeds the byte budget, and the same bytes are handed to the provider's `AppendEncodedBatchAsync` on flush without a second encode. The budget is an exact ceiling for any batch of more than one entry (a single entry larger than the whole budget is flushed alone rather than refused), suitable for sizing against backends with hard transactional limits (e.g. the Azure Table Storage 4 MB batch cap). It bounds the batch, not each entry: on the Azure Table provider the Table service also limits each entry's stored payload to 64 KiB (see [WAL storage providers](wal-storage-providers.md#azuretablewalstorageprovider)). |
| `WalMaxPendingBatches` | `16` | Maximum number of in-flight + just-started flushes the grain holds against the provider concurrently. The pre-6.1.0 default was `1`, which reproduced the original single-in-flight protocol bit-for-bit; the v6.1.0-v6.2.x default of `8` raised pipeline depth so writer-side bursts coalesced against higher-latency durable providers (e.g. Azure Tables). The post-v6.2 default of `16` was measured on Standard_D4as_v5 + Azure Tables Standard at 4,000 keys/s offered load to give a +57% increase in steady-state silo throughput at the 4k:5 rung with no reliability regression; see [WAL tuning](wal-tuning.md) for the storage-account-throughput envelope above which the dual-knob fan-out collapses to `429` throttling. The cap is the only synchronisation point new appends see, so cap values above the steady-state burst depth do not buy further throughput. Set explicitly to `1` to opt back into the legacy strict-serial-per-partition shape. |
| `WalFlushTimeout` | `15 s` | Upper bound on how long a single flush may take before the grain abandons the wait, faults the flush, resynchronises the dense-offset tail from the provider, and drains the chain so callers retry. Set to `Timeout.InfiniteTimeSpan` to restore the historical unbounded await. See [Flush deadline](#flush-deadline). |
| `WalFlushPreflightTimeout` | `5 s` | Upper bound on a flush's preflight - the setup and initial scheduler yield before the provider call is issued - so a flush whose continuation never resumes faults as a `TimeoutException` and its slot drains. See [Flush deadline](#flush-deadline). |
| `WalAppendCoalescingInFlightThreshold` | `4` | In-flight depth at or above which a batch append's final entry stops kicking a flush of its own and coalesces into the pending batch, drained by the follow-on flush that fires when an in-flight flush settles. Self-disabling below the threshold; `0` restores the unconditional final-entry kick. See [WAL tuning](wal-tuning.md). |

Cutovers below the in-flight cap start a fresh flush immediately;
cutovers at the cap await the oldest in-flight flush before starting
another, which provides natural back-pressure under sustained burst
load.

### Flush deadline

The number of in-flight flushes is bounded by `WalMaxPendingBatches`; the
cap is the only synchronisation point new appends see. If one flush never settles,
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

`WalFlushTimeout` arms only once the provider call is issued. The flush's
preflight - the setup and initial scheduler yield that precede the call - has
its own bound, `WalFlushPreflightTimeout` (default 5 s): a flush whose
post-yield continuation never resumes (an activation parked by a reshard or
membership change, a scheduler hogged by non-cooperative work, or a teardown
mid-flush) faults as a `TimeoutException` through the same failure path, so its
slot drains instead of saturating the chain, and the
`orleans.lattice.wal.flush.preflight.timeouts` counter attributes each trip to
the `(tree, shard)`.

### Abandoned flushes, holes and the trim watermark

A flush abandoned at its deadline is still in motion and may land later (issue
#4621). Until its provider call settles, nothing at or above its window is
exposed to a reader - by `ReadAsync`, `ReadShippingAsync`, or the readable head
`GetReadableHeadAsync` that readers resume from - so a late landing is never
below a reader's position. The record of such windows is process-wide, keyed by
provider and shard, so a reactivation of the shard in the same process is held
too. Exposure is also never past the highest offset the shard knows is stored,
plus one: a recovering allocator resumes there, so a trailing hole stays
unexposed until something lands above it, and a reissued offset can never land
below a reader. Once the call settles, its window is final: the entries landed,
and are read in order, or the slot is a permanent hole, because the allocator is
already past it. Offsets are therefore not dense.

A hole directly above a trim point looks exactly like a trim to a reader that
judges by the lowest stored offset. Every provider therefore keeps a **trim
watermark** (`IWalStorageProvider.GetTrimWatermarkAsync`): the highest offset any
trim has trimmed through, persisted before the trim deletes anything, never
returned by a read. An offset at or below it was trimmed; a missing offset above
it is a hole. The in-memory provider raises it under the same lock as the delete,
the file provider records it as its trim marker, and the Azure Table provider in a
per-shard row of the manifest partition. The tail every reader judges fall-off
by - the leaf's prefix-loss check, the WAL subscriber, the fall-off detector, and
the replication shipper's forced-gap check - is one past the watermark.

A reader trusts the watermark only when every silo in the cluster manifest hosts
the build that maintains it: a silo that predates it trims without moving it, and
its trims would read as holes. Until then, and for a third-party provider that
keeps no watermark, readers treat every jump in offsets as a trim, which can
trigger a needless rebuild or re-seed during a rolling upgrade but never skips a
trim.

### Batched leaf write path

Bulk-write entry points on the leaf collapse their per-key WAL grain
hops into a single batched dispatch through
`ICommitLogWriter.AppendManyAsync`, which the default
`WalCommitLogWriter` implementation groups by WAL partition and forwards
to `IWalShardGrain.AppendBatchAsync`. The leaf entry points that flow
through this path are:

| Entry point | Caller |
|---|---|
| `BPlusLeafGrain.SetManyAsync` | Foreground `ILattice.SetManyAsync` / `TypedLatticeExtensions.SetManyAsync`. |
| The leaf's conditional batch write | Foreground `ILattice.SetManyWherePredicateAsync`, and the predicate overloads of `TypedLatticeExtensions.SetManyAsync` that call it. |
| The leaf's batched CRDT delta apply | Foreground `ILattice.ApplyCrdtDeltaManyAsync`, and helpers built on it such as `CrdtLatticeExtensions.EnableManyAsync`. |
| `BPlusLeafGrain.MergeEntriesAsync` | Leaf split completion (the right half moving into the new sibling), and the bulk-load topology assembly invoked by `ShardRootGrain.BulkLoadAsync` / `BulkLoadRawAsync` / `BulkAppendAsync`. |
| `BPlusLeafGrain.MergeManyAsync` | Every shard-level merge: replication apply, snapshot copy and restore, backup restore, tree merge, and cross-shard migration on shard split, online reshard and shard consolidation (including writes shadow-forwarded during a split). |

For an N-key batch routed to a single WAL partition the grain-hop count
drops from O(N) to one. Multi-partition batches fan out one
`AppendBatchAsync` call per touched partition and the writer reassembles
the dense per-input offsets in input order. The whole batch coalesces
into a single provider flush when it fits inside the
`WalMaxBatchEntries` / `WalMaxBatchBytes` window; larger batches cut
over across multiple flushes using the same in-flight cap as
single-entry `AppendAsync`. A partition group of exactly one entry still
takes the WAL grain's interleaving batch append rather than its
exclusive-turn single append (`WalBatchedSingleEntryAppends`, default
`true`), so a wide fan-out of one-entry slices does not serialise on the
partition. The same option routes every point append - the leaf's point
sets and deletes and their conditional forms, CRDT merge apply,
pending-transaction staging and inline saga terminal records - through the
batch append as a one-entry batch, so `WalMaxPendingBatches` pipelining and
`WalAppendCoalescingInFlightThreshold` coalescing engage for them too
instead of each point append holding the partition for a whole provider
round trip. Setting it to `false` restores the exclusive-turn dispatch for
both shapes.

The `LeafWriteDuration` histogram records one sample per batched
dispatch on the merge channel (`kind=merge`) rather than one sample per
entry, so percentile reads of the merge channel are not biased by batch
size.

### Activation recovery

On activation the WAL grain calls `IWalStorageProvider.ReconcileAsync`,
then `IWalStorageProvider.GetHighestOffsetAsync`, and resumes assigning
offsets at `highest + 1`. The persisted log is the single source of truth for the
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
time, so a co-operative provider's `AppendEncodedBatchAsync` cancellation
token cancels in one shot and the flush surfaces a `TimeoutException`
routed through the normal failure handler. The chain is then awaited
to settle naturally for up to the budget; any slot that has not
unlinked when the budget expires is force-faulted with a typed
`TimeoutException` faulted onto every parked ack TCS so callers are
released rather than parking through the rest of host shutdown. The
`orleans.lattice.wal.shard.drain.budget.expirations` counter and
`orleans.lattice.wal.shard.drain.budget.force_faulted_slots` histogram
(both tagged `tree` and `shard`, where `shard` carries the WAL partition index) attribute every budget-driven
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

### Writer-side drain at host shutdown

The shard-grain deactivation drain above bounds the **shard-side**
shutdown surface: it releases callers parked inside
`WalShardGrain.FlushAsync` waiting on a provider call. A symmetric
**writer-side** drain surface exists for callers parked one layer up,
inside `WalCommitLogWriter.PartitionTracker.AcquireAsync` waiting
on the per-(tree, partition) admission semaphore.

The library handles this automatically: `WalCommitLogWriter` exposes
a per-silo drain entry that the host's `StopAsync` lifecycle stage
invokes via a registered `IHostedService`. The drain signals every
parked `AcquireAsync` caller on the owning silo's writer; each
parked caller surfaces a typed `LatticeShuttingDownException` (a
sealed `InvalidOperationException` subclass whose message names
`WalDrainBudget` for grep-attribution and whose `InnerException`
preserves the legacy `TimeoutException(WalDrainBudget)` shape so
existing diagnostic tooling continues to work), and a counter
sample lands on `orleans.lattice.wal.writer.append.drain.releases`
tagged with `(tree, partition)`. Post-drain `AppendAsync` /
`AppendManyAsync` calls on the draining silo fail fast with the
same `LatticeShuttingDownException` rather than blocking on a
drained admission gate. See
[API Reference - Shutdown back-pressure](api.md#shutdown-back-pressure---latticeshuttingdownexception)
for the caller contract.

The drain is **per-silo, local-only**. Each silo process in a
multi-silo cluster has its own `WalCommitLogWriter` singleton with
its own drain state; a drain on silo A does not touch silo B's
admission semaphore and does not interrupt any in-flight
`IWalShardGrain` activation that silo B is dispatching to. Rolling
restarts settle cleanly because each silo drains its own writer
independently when its turn arrives. The writer-side drain
complements the shard-side `WalDrainBudget` force-fault path:
together they bound the silo shutdown end-to-end so a saturated
silo terminates inside its bounded deactivation drain instead of
needing `SIGKILL` (the wedge phenotype documented in
`benchmark/azure-throughput/throughput.md` section 32.6).

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
5. Once the chain drains, the grain calls
   `IWalStorageProvider.ReconcileAsync` and then re-reads
   `IWalStorageProvider.GetHighestOffsetAsync` to recover the provider's
   real tail. Concurrent later flushes may have already committed
   against now-orphaned offset windows; the resync restores the dense-
   offset invariant against the provider rather than against the failed
   window's start. The sticky-failure latch is then cleared and new
   appends resume.
6. If the resync itself fails (or exceeds `WalFlushTimeout`), the latch
   stays set and the grain requests its own deactivation, so the next
   activation re-runs the activation-time reconcile instead of the shard
   refusing every append until the silo restarts.

The faults of steps 2-4 are delivered only after the resync of step 5
has run, so a caller that retries immediately observes a resynchronised
grain.

This contract makes WAL-append failures observable inline at the
originating writer rather than being silently coalesced into a later
batch.

> **Contributor note - synchronously-completing providers.** `FlushAsync`
> starts with `await Task.Yield()` so the returned `Task` is observably
> incomplete by the time `StartFlush` stores it on the in-flight slot.
> Without that yield, an `IWalStorageProvider` whose `AppendEncodedBatchAsync`
> returns a synchronously-completed task (the in-memory provider's does,
> through the default implementation that delegates to its synchronous
> `AppendBatchAsync`) would run the entire flush body inline before the slot
> is fully initialised, including the chain-remove in the `finally` block - and
> the chain invariant ("every slot in `_inFlight` carries a task that
> completes when its provider call settles") would be violated. Any
> future refactor of the flush loop must preserve this yield, which
> `WalFlushPreflightTimeout` bounds.

## Recovery and rebuild


When a leaf grain activates, it rebuilds its in-memory projection by
replaying the WAL through a per-partition replay coordinator, seeding the
cache first from its own latest durable snapshot - when that snapshot is
newer than its partition-0 checkpoint, and also when it is at or behind it
but the cache starts empty or a WAL prefix has been trimmed (see
[Snapshot-on-fall-off safety net](projection-rebuild.md#snapshot-on-fall-off-safety-net)).
Three cases:

- **Tail replay.** The leaf's latest snapshot covers the WAL through offset
  *N* - the checkpoint the capture stamped, to which activation resets the
  leaf's checkpoint - and the newest WAL entry is at offset *M* (the WAL head,
  the next offset to be assigned, is `M + 1`). The leaf reloads the snapshot,
  reads entries `(N, M]` in bounded slices and applies each to its
  projection, so `M - N` grows with the time since the last snapshot
  capture rather than with the checkpoint interval. Replay is in-process and
  typically completes in a few milliseconds.
- **Fresh-leaf tail replay.** A leaf created mid-run by a split (or by the
  first write to a virgin shard) has no checkpoint yet: a never-assigned
  checkpoint reads as the -1 "nothing applied" sentinel on every partition
  (issue #2703). The fall-off-log detector exempts the
  sentinel from the replay-budget and trim triggers: the leaf has no
  projection state to lose, and the per-leaf range filter inside the
  materialiser (`ShouldApplyDuringReplay`) drops every WAL entry that falls
  outside this leaf's `[LowKeyInclusive, HighKeyExclusive)` ownership range,
  so the leaf applies only its own records - and a provider that classifies
  records before decoding them decodes only those (see below). The read
  itself still walks the whole retained window, in `WalReplaySliceBudget`-sized
  slices. A leaf that has checkpointed but has no usable snapshot replays the
  same whole window, because its cache starts empty; for it a separate guard
  compares the oldest readable offset with its durable checkpoint and refuses
  the leaf when an offset it still needs has been trimmed (the genuine-loss
  case below).
- **Genuine loss.** The persisted checkpoint is older than the WAL trim
  watermark - the entries it would replay are no longer available - and no
  snapshot covers the gap. Activation refuses the leaf with
  `LeafProjectionStaleException` under every `ProjectionRebuildPolicy`:
  neither the snapshot-then-WAL recovery beyond the snapshot rehydrate nor the
  full-rebuild path is integrated yet, and replaying only the surviving suffix
  would rebuild the leaf over the lost prefix. See
  [`projection-rebuild.md`](projection-rebuild.md) for the policies and the
  operator remedies. The grain-state row holds only tree metadata (sibling
  and parent pointers, tree id, shard index, key range, moved-away slots,
  split lifecycle, last-compaction-version), the leaf's hybrid-logical clock
  and version vector, the projection checkpoints, the running projection
  hash and its publish sequence, a snapshot-size hint and the replay
  ledger - it is never the source of truth for committed entry values.

In both replay cases, the projection that a reader observes after activation is
byte-equivalent to the projection at the moment the leaf last deactivated (for
a freshly-created leaf, the records the WAL holds for its own range).

### Replay reads are filtered at the source

A WAL partition is shared by every leaf whose keys hash to it, so a leaf
replaying a partition would otherwise read its neighbours' records as well as
its own. The leaf therefore passes its ownership - its key range and, when the
tree's shard map is known, the slots of its shard - down the read path as a
`WalKeyFilter`: the slice reader hands it to `ILeafReplayCoordinatorGrain`,
which reads through `IWalShardGrain.ReadFilteredAsync` and, beneath that,
`IWalStorageProvider.ReadFilteredAsync`. A provider that can classify a record
before decoding it skips every record the leaf does not own, so a replay
allocates in proportion to the leaf's own records rather than to the partition
(issue #3565). See
[`wal-storage-providers.md`](wal-storage-providers.md#filtered-replay-read-readfilteredasync)
for the read's exact contract.

The filter and the leaf's own apply check (`ShouldApplyDuringReplay`) are
built from one capture of the leaf's state, so the filter never drops a record
the leaf would have applied, and the leaf still runs that check on everything
it receives. Each read bounds the entries it examines rather than the entries
it returns, and delivers the last examined entry routing-only when it is
excluded, so the projection checkpoint still advances past a window that held
only other leaves' records. A leaf whose ownership excludes nothing - an
unbounded key range with no shard constraint - reads unfiltered, exactly as
before.

The coordinator's five-second slice cache is keyed by the filter as well as
the window, because a filtered slice is missing every other owner's records.
Leaves with different ownership therefore no longer share one slice read when
they replay the same window back to back. Each makes its own storage read
instead and, with a provider that classifies records before decoding them,
decodes only its own records in it.

## Projection checkpoint

To keep tail-replay bounded, the leaf flushes a **projection checkpoint**
durably whenever the elapsed wall-clock time since the last flush reaches
`MaterialiserCheckpointInterval` (default: 5 seconds) **or** the count of
unflushed advances reaches `MaterialiserCheckpointEntries` (default: 5 000),
whichever happens first. The checkpoint is a single grain-state write of the
leaf's persisted row, which records for each WAL partition the highest offset
the leaf has worked through - replay advances it past other leaves' records as
well as its own - together with the leaf's hybrid-logical clock and
version vector. It does **not** capture entry values: the persisted leaf row
has carried no per-key entries since the leaf-state collapse, and its former
entries slot is reserved; the only mutations it can carry are the unresolved
saga prepares and deferred terminals of the replay ledger (see
[Resumable cold replay](projection-rebuild.md#resumable-cold-replay)). On the
next activation the leaf rehydrates its entries from its latest durable
snapshot and replays the WAL only past the offsets that snapshot covers - the
checkpoint a capture stamps - rather than from zero; a partition no snapshot
covers is replayed from the start of its readable window.

Both triggers are evaluated as each advance is recorded. An advance that
arrives inside the interval and below the entry count - typically the last
partition an activation replay reconciled - would otherwise stay pending on
a resident, write-idle leaf, because no later advance re-asks the question.
The leaf's periodic coverage-lag check (every
[`LeafSnapshotMaxCoverageLagSeconds`](configuration.md#leafsnapshotmaxcoveragelagseconds),
300 seconds by default) re-evaluates the same triggers and commits such an
advance once the interval has elapsed, so a pending advance becomes durable
within the later of the interval and the next check (issue #3608).

The checkpoint is **not** an additional durability boundary - it's a replay-cost
optimization. If a checkpoint flush fails, the leaf rolls the advance back and
keeps it pending for its next flush, and the durable pin it publishes for the
WAL GC never runs ahead of the checkpoint that actually reached storage, so
the GC cannot trim the range the failed write would have covered and the next
activation simply replays more WAL entries; correctness is unaffected. The
checkpoint is also flushed
opportunistically in `OnDeactivateAsync` so a graceful shutdown doesn't lose an
already-pending advance.

## Trim and GC

The WAL grows monotonically and must be trimmed. Trim is driven by
`ILatticeWalGc`, a per-tree single-pass collector that advances the
per-partition trim watermark to the largest contiguous prefix that **every**
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

A WAL entry is trim-eligible only when four independent clauses all accept
it: the **entitlement clause**, the **offset-reader clause**, the
**causal-stable clause**, and the **blocked-floor clause**.

The entitlement clause has two axes. On the HLC axis it is satisfied when
**either** of the following holds:

| Condition | Meaning |
|---|---|
| `entry.Timestamp <= minCursor` | Every registered consumer has reported a cursor at or beyond this entry's HLC. |
| `entry.Timestamp <= ttlCeiling` | The entry's wall-clock component is older than `now - WalRetention` (when configured). |

On the offset axis, when a durable materialiser offset floor is available, the
floor admits every entry at or below it that no consumer outside the floor's
population still needs, so an entry the HLC axis refuses can still be entitled
on durable offset evidence alone. The floor can also refuse an entry that only
the in-memory consumer cursor would admit, because that cursor tracks what a
leaf folded into its cache rather than what it made durable. It never
overrules the TTL ceiling, and a tree with no durable floor evaluates the HLC
axis alone. A pass that cannot read the durable pin or offset census at all
is not a tree with no durable floor: the floor is unknown, so the pass fails
closed, trims nothing on any partition - TTL included - and retries on the
next pass (issue #3576).

The causal-stable clause is satisfied when **either** of the following holds:

| Condition | Meaning |
|---|---|
| `causalStable is null` | No consumer has reported a per-origin `VersionVector` through the causal+ overload of `ReportCursorAsync`. The clause degrades to a no-op so the GC behaves identically to the legacy HLC-only predicate. |
| `causalStable.DominatesOrEquals(entry.VectorClock)` | Every consumer that reported a vector has fully observed the entry's causal predecessors. Entries with a `null` `VectorClock` (legacy peers, range deletes, pre-causal+ entries) are treated as the empty frontier and pass automatically. |

The blocked-floor clause holds back every entry whose HLC is at or after the
lowest buffer pin any consumer reports (see
[Consumer registration](#consumer-registration)), so a buffering receiver
can recover from its staging state; it is inert while no consumer reports a
pin.

A fourth clause bounds the trim by the **read position of every
offset-reading consumer** (issues #4579, #4584). The replication shipper and
every materialised-view maintainer read each partition by offset, so the HLC
cursor they report cannot hold the entries they have not read: a WAL partition
is not HLC-ordered in offset (a silo whose clock trails, a merge that keeps its
source stamp), so an unread entry can carry an HLC at or below a cursor already
reported. That cursor is also visible only to the GC pass on the silo the
consumer runs on, while every silo runs a pass. Each offset-reading consumer
registers with the tree's durable consumer set before it reads the log. On every pass the GC asks each registered consumer for the
lowest offset per partition it has not durably consumed, and refuses any entry
at or above it. The consumer's own persisted position is the answer, so the
bound holds on every silo and across a restart. This clause overrules the
cursor arm and the materialiser offset admission, but not the TTL ceiling,
which stays a bound: a consumer that falls behind it detects the trimmed gap on
its next read. A registered consumer that has read nothing holds the whole
log, and a member whose position cannot be read counts as position 0. Before
the TTL ceiling trims an entry at or past a consumer's position, the pass
durably records that consumer's saga decision-purge hold in the tree's
`IWalPurgeHoldGrain` (issue #4534), so the transaction registry keeps every
decision the consumer's peer may need to be re-seeded with; a failed hold
write skips the trim, and so does a pass that cannot read the set of
registered consumers at all. See
[Decision-purge holds](../lattice.replication/replication-drivers.md#decision-purge-holds).

An incremental backup capture is deliberately not an offset-reading consumer:
the GC may trim past it, and the capture then falls back to a full backup. Its
gap detection, like a view's, is exact by offset and made against what was
actually read. The shared WAL subscriber probes the tail again whenever a read
jumps an offset, so a trim that lands after its pre-read check is reported as a
fall-off rather than read across. A jump the tail has not passed is a hole - a
slot whose flush failed and was never acknowledged - and is read past. The tail
is one past the shard's trim watermark, so a hole directly above a trim point is
not mistaken for the trim (see [Abandoned flushes, holes and the trim
watermark](#abandoned-flushes-holes-and-the-trim-watermark)).

The clauses are AND-ed: the cursor / TTL clause is kept for safety so a
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

The ceiling never overtakes a leaf materialiser. The scan stops at the durable
materialiser offset floor before any arm is consulted, so the ceiling cannot trim
past what a leaf has durably checkpointed or snapshotted. A partition named by a
standing durable block pin admits nothing at all, from the ceiling or from any
consumer cursor, whether or not the leaf is live (issue #4622). A block pin is a
`Zero` frontier that the offset floor does not cover: a data-bearing leaf that has
never checkpointed. Such a leaf replays from the "nothing applied" sentinel on a
cold activation and could not detect a trimmed prefix. Any other leaf pin the offset
floor does not cover caps the partition's ceiling at its frontier: that frontier was
published by an empty release, when the leaf held no row there and had applied
nothing, so every entry the leaf has since written to the partition is stamped above
it, even though the pin store's monotone merge keeps the frontier after that write.
A held or capped partition grows for as long as the hold stands; watch
`orleans.lattice.wal.gc.leaf_pin_hold_age` (see [Metrics](metrics.md)).

The scan is conservative: the first non-eligible entry per shard stops the
walk for that shard, as does the first entry above the partition's durable
materialiser offset floor or at an offset-reading consumer's read position. WAL offsets are dense and append-only but HLC
`WallClockTicks` is mostly-monotonic-with-skew, so a stop-at-first-miss walk
preserves correctness while a more aggressive scan would risk trimming an
entry younger than a still-pinned later entry.

### Consumer registration

Every consumer of the change feed - the outbound replication ship loop,
in-process bridges, custom transports, and the local in-memory materialiser -
must publish its acked HLC to the registry so its progress contributes to
`minCursor`. A consumer that never registers does not pin the log; the GC
will trim under it and the consumer must detect the gap on the next read.
A consumer that buffers entries it cannot apply yet also reports its lowest
buffered HLC through the `blockedAtHlc` overloads of `ReportCursorAsync`; the
lowest such pin across consumers is the blocked floor the predicate holds
back.

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

`AddLattice` always registers both the default `InMemoryWalCursorRegistry`
and a lightweight `InMemoryLeafCursorReporter`, so the registry the GC and
the saturation sampler read is never absent *and* every leaf publishes its
applied frontier into it - the materialiser drain-lag back-pressure is live
for every write workload out of the box, not only on materialiser /
replication hosts. The registry is process-local and loses its state on silo
restart. A host that needs cross-restart durability supplies its own
`IWalCursorRegistry` implementation by passing a factory to
`AddWalCursorRegistry(factory)`, which replaces that in-memory
default regardless of registration order. `AddLatticeReplication(...)` and
the durable storage helpers call it without a factory instead, which keeps
the in-memory registry and adds the durable-pin-aware leaf reporter described
under [Surviving a full restart](#surviving-a-full-restart).

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
| Consumer frontier | *(none - always on)* | always on | The hard floor: `min(cursor)` across every registered consumer (overruled, where available, by the durable materialiser offset floor), intersected with the causal-stable frontier and held below the blocked floor. | **No.** This is the durability invariant. |
| Wall-clock TTL | `WalRetention` | `null` (disabled) | Entries older than `now - WalRetention` fall off the log even if a consumer still pins them. | **Yes** - this is the only bound that does. |
| Advisory byte ceiling | `WalMaxRetainedBytes` | `null` (disabled) | Schedules byte-pressure trim work when the WAL's on-disk occupancy - physical bytes, dead bytes not yet compacted included, or the retained payload for a provider that cannot report physical size - exceeds the ceiling, but only *within* the consumer frontier. | **No** - it surfaces an over-threshold signal instead. |

`WalBytePressureReclaimTarget` (default `0.8`) is not itself a bound: it is the
low-water hysteresis fraction of `WalMaxRetainedBytes` that disarms the
byte-pressure policy once a trim has reclaimed enough, so a tree hovering near
the ceiling is not trimmed on every pass. It is inert unless
`WalMaxRetainedBytes` is set.

`WalMaxRetainedBytes` also accepts a **per-tree override set at runtime**
(issue #3333). The silo-wide option is the default for every tree; a single
tree can pin its own ceiling through `lattice_treeadmin_tree_set_config`
(`applyWalMaxRetainedBytes`), which is persisted on the tree's registry entry
and re-read on every GC pass, so it takes effect on that tree's next pass with
no silo restart. Passing `null` clears the override. Because the ceiling is
advisory in both forms, an override can never trim past the consumer frontier
and so cannot lose data - it only moves where byte-pressure trimming and the
cadence floor engage. See
[Configuration](configuration.md#walmaxretainedbytes).

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

The core library ships a per-silo background scheduler that drives this
pass for **every** registered tree. It is registered by `AddLatticeWalGc`,
which the shipped durable WAL storage packages and `AddLatticeReplication`
call for you, so a durable-WAL host gets bounded WAL retention out of the box -
including for non-replicated trees - and `WalRetention` is effective with no
extra wiring.

The cadence is adaptive and per tree. Each tree's interval moves inside the
band `[WalGcMinInterval, WalGcInterval]` (30 seconds to 1 hour by default). A
pass that trims at least one entry snaps that tree back to the floor, so a
growing log keeps being collected; a pass that reclaims nothing doubles the
tree's interval towards the ceiling, so a quiet tree relaxes and costs little.
A tree whose pass reports `blocked` or `over_ceiling` is held at the floor, and
one that reclaims nothing while its scan still stops on WAL it must retain
relaxes no further than five minutes (clamped into the band); see
[`WalGcMinInterval`](configuration.md#walgcmininterval) for the full rule. A
pass is retention housekeeping, not a latency-sensitive operation - its cost
scales with `trees x WalPartitions` storage reads and runs on every silo -
which is why the quiet-path ceiling is deliberately coarse. A host that needs a
tighter disk bound - a high write rate paired with a small `WalRetention` - can
lower it; set `WalGcInterval` to `TimeSpan.Zero` (or any non-positive value)
to **disable** the built-in scheduler and let the host own the cadence (an
admin trigger, an Orleans reminder, or - for replicated trees - the
replication package's per-tree maintenance grain).

The first pass is **not** run at silo start; it is staggered by a random
offset in `[WalGcStartupDelay / 2, WalGcStartupDelay)` - 15 to 30 seconds by
default, capped at `WalGcInterval`. That lets the silo finish activating
before the scheduler adds scan/trim I/O, and de-correlates the first pass
across silos so a rolling cluster restart does not align every silo's
full-tree fan-out into a correlated I/O storm.

```csharp verify
// Tighten the built-in scheduler on a high-write host, or disable it.
siloBuilder.ConfigureLattice(o => o.WalGcInterval = TimeSpan.FromMinutes(5));
siloBuilder.ConfigureLattice(o => o.WalGcInterval = TimeSpan.Zero); // disable
```

A pass whose cursor floor is held by dormant leaves also drives those leaves
forward: when an unusable durable pin blocks the floor, or a dormant pin the
scheduler can repair holds it, the scheduler touches the leaves behind those
pins in a bounded reactivation sweep so their checkpoints, and with them the
floor, can advance. The sweep's drives share the per-silo WAL replay permits
with leaf activations; see
[Starvation-drive admission](projection-rebuild.md#starvation-drive-admission)
for how a pass sizes its fan-out to that share and what a refused drive costs.
A deleted tree is the exception: before a pass touches leaves to heal its
floor, the scheduler reads the tree's deletion state, and it never reactivates
a deleted tree's leaves. It retires any pin still held against a discarded
copy - an undone resize's destination, whose discard trims the copy's log
itself - and leaves any other deleted tree's floor where its pins hold it;
see
[Discarding an undone resize's copy](tree-deletion.md#discarding-an-undone-resizes-copy).

The scheduler composes with the replication maintenance grain:
`RunOnceAsync` and the underlying `IWalStorageProvider.TrimAsync` are
idempotent, and the pass never trims past the minimum consumer cursor or
the leaf-materialiser checkpoint floor, so a tree collected by both
drivers is trimmed safely and it never over-trims. To drive a
single pass manually instead (or in addition), call `RunOnceAsync`
directly:

```text
LatticeWalGcReport report = await gc.RunOnceAsync(
    treeName: "orders",
    cancellationToken: cancellationToken);

// The report exposes the inputs and the outcome:
//   - report.TreeName        - the tree the pass targeted
//   - report.MinCursor       - minimum cursor across registered consumers, or null
//   - report.TtlCeilingHlc   - TTL ceiling synthesised from WalRetention, or null
//   - report.CausalStable    - pointwise-min VersionVector across consumers, or null
//   - report.BlockedFloor    - lowest buffer pin across consumers, or null
//   - report.ShardsScanned   - WAL partitions whose provider resolved and were
//                              visited; zero means none could be, not an empty WAL
//   - report.EntriesTrimmed  - total entries the pass found eligible and asked the
//                              provider to trim, across all partitions
//   - report.ByteCeiling, RetainedBytesBefore, RetainedBytesAfter,
//     LogicalRetainedBytes, BytePressureTriggered, BytePressureOverThreshold,
//     CeilingUnsatisfiable - the advisory byte-pressure inputs and verdicts
//   - report.CursorFloorState, BlockingConsumerId, BlockingConsumerIds - whether
//     the cursor floor was usable, and which consumers blocked it
//   - report.RetainedBacklog - whether a partition's scan stopped on WAL it had to retain
```

### Reclamation floor-holder read

`ILatticeWalReclamation.GetWalReclamationAsync(treeId)` is the public read for a
tree whose WAL usage is flat and whose reclamation might be idle or wedged. The
MCP tool is `lattice_treeadmin_wal_reclamation`. The report echoes the tree id as
the caller named it, but the probe reads the physical tree that alias resolution
currently targets, because materialiser pins are published under the physical
copy a leaf belongs to.

`PinStoreReadable = false` means the durable pin store did not answer; the other
fields are not measurements, and `IsWedged = false` means "not established", not
"healthy". When the store answers, `PinCount` counts the materialiser pins and
`PinsWithoutOffset` counts the pins still at the `-1` no-offset sentinel. The
`FloorHolder`, when present, is the pin with the lowest usable offset; if no pin
has a usable offset it is one of the `-1` pins, and if the tree has no pins it is
`null`. The holder carries its consumer id, parsed leaf id when available, WAL
partition, pin offset, persisted checkpoint, and state. The state is one of
`CheckpointedUncovered`, `NeverCheckpointed`, `NoDurableState`, `Unreadable`,
`Orphaned`, or `CheckpointedCoverageUnknown`.

`IsWedged` is keyed on that holder, not on WAL growth: it is true exactly when
the holder has a usable offset (`PinOffset >= 0`) and its persisted checkpoint is
still `-1` (`NeverCheckpointed`). That shape will not clear on its own because
the durable pin store merges pins upward and the GC will not drive a leaf with no
proven checkpoint. The same `NeverCheckpointed` state on a holder at offset `-1`
is the benign sentinel that clears when the leaf checkpoints.

### Metrics

The GC and its scheduler publish a family of instruments on the
`orleans.lattice` meter, all catalogued in [Metrics](metrics.md). The ones to
start from:

| Instrument | Tags | Description |
|---|---|---|
| `orleans.lattice.wal.entries_trimmed` | `tree`, `shard` | Counter. WAL entries removed by a GC pass, reported once per WAL partition the pass scanned. The tag is named `shard` for compatibility but its value is a WAL partition index. A partition that was scanned but reclaimed nothing records a zero, so an absent series means the partition was not scanned on this silo. |
| `orleans.lattice.wal.gc.passes` | `tree`, `outcome` | Counter. One count per scheduled pass, by outcome: `reclaimed`, `blocked`, `no_consumer`, `no_partitions`, `idle`, `over_ceiling`, `stranded`, `unclassified` or `failed`. Only `reclaimed` states that WAL came back; every other arm says why nothing was trimmed. `no_partitions` means no pinned WAL provider resolved on this silo (issue #2465), not an empty WAL. It outranks the other non-reclaiming arms and is zero-primed per tree. Inspect WAL placement and provider registration. `ShardsScanned` counts only resolved partitions visited for trimming or compaction; partial resolution keeps the existing outcome and does not imply every partition was examined. With no cursor and no TTL, `no_consumer` names the no-predicate return; `blocked` names an unusable durable pin, while `stranded` can describe a legitimate lagging consumer holding retained WAL. |
| `orleans.lattice.wal.gc.interval` | `tree` | Histogram (seconds). The adaptive interval the scheduler chose for the tree after its latest pass. A series pinned at `WalGcMinInterval` is a tree the scheduler is holding at the floor - reclaiming, blocked, or over its byte ceiling. |
| `orleans.lattice.wal.gc.trim_stop` | `tree`, `shard`, `reason` | Counter. Why each WAL partition's trim scan stopped: `exhausted`, `empty`, `offset_floor`, `cursor_floor`, `causal_frontier`, `block_pin`, `durability_unverified`, `durability_hold` or `durable_offset_refusal`. The `shard` tag carries the WAL partition index. |

The floor-holder diagnostics explain why a retained floor did or did not enter a
repair drive. `orleans.lattice.wal.gc.floor_holder_admission` is tagged by
`tree`, tenant and `status`; `admitted` means the floor-defining candidate
entered the blocked-leaf drive, `blocked` means the floor's own holder could not
be admitted, and `unreached` means the pass exited through the floor-blocked heal
arm before the floor-holder classifier ran. `orleans.lattice.wal.gc.never_checkpointed_pin_offset`
splits `NeverCheckpointed` holders into `offset_absent` (the benign `-1` pin
sentinel) and `offset_usable` (the wedging shape that can hold the offset floor),
tagged by tree, WAL partition, status and tenant; both arms are zero-primed for
evaluated trees.

Orphan-pin removal has its own decision counters. `orleans.lattice.wal.gc.orphan_pin_sweep`
partitions each examined durable pin into `retired`, `retire_failed`, `deferred`,
`refused_malformed_id`, `refused_ambiguous_partition`, `live`, `unresolved` or
`unreadable`; the decision arms also carry a `cause` of `orphaned` or
`no_durable_state`. `orleans.lattice.wal.gc.drive_orphan_pin_retirement` records
the same removal/refusal outcomes when the blocked-leaf drive gets a `NotDriven`
verdict, with `cause="not_driven"`.

Two further GC signals separate a tree that is catching up from one that is
stuck (issue #3149). `orleans.lattice.wal.gc.floor_head_distance` records,
for each shard a pass scanned, how many offsets lie from the first entry the
scan had to retain through the shard's newest entry - zero when the scan
released everything it was offered - and
`orleans.lattice.wal.gc.terminal_breach` counts each pass of a tree that has
been over its byte ceiling, with a usable cursor floor, reclaiming nothing,
for ten consecutive passes, the point at which `over_ceiling` has stopped
being a transient.

## Clock floor (replicated trees)

Each WAL partition of a replicated tree keeps a durable **clock floor** ([#4586](https://github.com/NSTA1/Orleans.Lattice/issues/4586)).

**How it moves.** A replication shipper reads the partition through `ReadShippingAsync`. Each time it reads, the partition checks its floor against `now - ReplicationClockFloorLag`. Once the floor has fallen half a lag behind that target:

1. The partition raises the floor to the target.
2. It persists the new floor in its own grain state (`wal-floor`).
3. Only then does it return the floor to the shipper, paired with its next offset.

That keeps the floor between one and one and a half lags behind the wall clock, at the cost of at most two storage writes per lag per partition while it is being shipped.

**What it refuses.** From then on, the partition refuses any freshly authored local write stamped below the floor, with `WalStampBelowFloorException` (counted on `orleans.lattice.wal.append.floor_refusals`). The check runs under the same state gate that assigns the offset, so a refused write is never assigned one.

**Why.** The floor-and-offset pair is a promise: every fresh local write at or above that offset carries a stamp at or above the floor. So once a peer has acknowledged everything below the offset, it holds every write of this cluster, in that partition, that is stamped below the floor. That is a low watermark that is downward-closed, which the max-HLC high-water mark is not ([#1060](https://github.com/NSTA1/Orleans.Lattice/issues/1060)). The replication package's causal low watermark is built on this promise.

### Which writes the floor governs

The floor governs a stamp minted on this cluster for the write being appended. A **carried** stamp is exempt, because the identity it names was first appended fresh, at a lower offset. The carried stamps are:

- a replicated write of another origin;
- a merge or backstop copy;
- a migrated row, including a saga prepare carried at its original stamp from another shard;
- a record stamped under an HLC override, such as a shadow-forward or a prepared-bucket sweep (`WalRecord.IsCarriedStamp`);
- a tombstone-reap envelope;
- a record with a zero stamp.

Two overrides are minted for the operation itself, so the floor governs them: a range delete's issue stamp, and a caller-supplied idempotency key.

### What a writer sees when its stamp is refused

| Write | On a refusal |
|---|---|
| Single-key `SetAsync` or `DeleteAsync` | The leaf merges its clock past the floor and re-stamps, then commits once in the same grain turn. The caller sees nothing. |
| Typed CRDT delta, or a multi-key batch | Not re-run in the turn, because the fold or another partition may already hold part of it. The leaf merges its clock past the floor, and the caller sees a transient error whose retry is admitted. |
| `DeleteRangeAsync` | The call re-issues a fresh dominating stamp for the remainder of the range. The keys already tombstoned keep theirs, so the delete lands as one HLC per uninterrupted run. A nested range delete keeps its owner's stamp and the refusal propagates to the owner. |
| A write under a `LatticeIdempotencyKey` | Fails with `LatticeIdempotencyKeyExpiredException`. The key's stamp cannot be renewed without breaking its contract (see [Retry Policy](retry-policy.md#key-lifetime-on-replicated-trees)). |

### Rolling upgrades and trees that are not replicated

A partition advances its floor only while every active silo's grain manifest advertises `IWalClockFloorCapable`. That marker is the capability of a build that both enforces the floor and re-stamps a refused write. So during a rolling upgrade, no floor moves until the last older silo has left; the gate opens by itself, and there is no option to forget to enable.

A floor already published stays enforced, because it is durable. If the gate closes again, the floor only stops moving. Downgrading to a build without the marker after a floor was published is unsupported.

A tree that is not replicated is never read by a shipper. Its floor stays zero, and it never refuses a write.

## Relationship to replication

Cross-cluster replication is an **additional consumer** of the same WAL - not
a parallel pipeline. The replication change feed reads `LatticeMutation`
envelopes from the WAL, applies them on the peer cluster through
`IReplicationApplier`, which writes them into the peer tree through its
per-tree apply path, and acknowledges its progress through the same cursor
registry that GC consults.

The single-cluster and multi-cluster code paths are identical up to the point
where replication transports an envelope across a network boundary. There is
no "replication mode" that changes how a foreground commit durabilizes - the
commit always appends to the local WAL, and replication is purely additive.

See [`../lattice.replication/replication-drivers.md`](../lattice.replication/replication-drivers.md)
for the driver-grain scheduling model that consumes the WAL on each peer.

## Configuration

The knobs below shape WAL retention and replay; defaults suit most workloads.
They are a subset: the full WAL option set - batching and pipeline depth,
admission and saturation, GC cadence, and the materialiser pin store - is in
the [Options Reference](configuration.md#options-reference).

| Option | Default | Purpose |
|---|---|---|
| `MaterialiserCheckpointInterval` | 5 seconds | Time-driven flush of any pending projection-checkpoint advance. Set to `Timeout.InfiniteTimeSpan` to disable the time trigger and rely solely on the entry-count trigger. |
| `MaterialiserCheckpointEntries` | `5_000` | Entry-count trigger: forces a checkpoint flush once this many advances are pending, regardless of `MaterialiserCheckpointInterval`. Bounds the worst-case replay cost when the steady-state apply rate is high. |
| `MaxLeafReplayEntries` | `10_000` | Per-leaf replay size above which a cold activation logs a warning and increments `orleans.lattice.leaf.activation_replays_over_budget`. It counts the entries the leaf actually applies after its ownership filter, and it is a cost signal only: the leaf still tail-replays in full, and exceeding it never routes to `ProjectionRebuildPolicy` (issue #1738). |
| `LeafProjectionRetention` | 7 days | Age beyond which a persisted checkpoint is flagged as a cost signal only - the leaf still tail-replays and never falls off-log on age alone. The activation path currently supplies a zero age, so the trigger does not fire from activation today. Set to `Timeout.InfiniteTimeSpan` to disable the age-based trigger. |
| `ProjectionRebuildPolicy` | `SnapshotThenWal` | Recovery strategy consulted only on genuine loss (the WAL trimmed past the checkpoint with no covering snapshot); today every policy refuses the leaf with `LeafProjectionStaleException`. See [`projection-rebuild.md`](projection-rebuild.md). |
| `WalRetention` | `null` (disabled) | Wall-clock hard ceiling on retention: entries older than `now - WalRetention` fall off the log even if a consumer still pins them. The only knob that trims past a stuck consumer - set it where unbounded growth is unacceptable. See [How the retention bounds interact](#how-the-retention-bounds-interact). |
| `WalMaxRetainedBytes` | `null` (disabled) | Advisory per-tree byte ceiling that schedules byte-pressure trim work, but only within the safe consumer frontier. See [Tree Storage](tree-storage.md#advisory-byte-pressure-wal-retention). |
| `WalBytePressureReclaimTarget` | `0.8` | Low-water hysteresis fraction of `WalMaxRetainedBytes` that disarms the byte-pressure policy after a trim. Inert unless `WalMaxRetainedBytes` is set. |
| `ReplicationClockFloorLag` | 60 seconds | How far a replicated tree's [clock floor](#clock-floor-replicated-trees) trails the wall clock. It is also the shortest time an idempotency key stays usable on a replicated tree. Must be between one second and one day. |

The WAL provider itself is registered separately - through a storage package's
helper such as `AddAzureTableWalStorage` or `AddFileWalStorage`, or `siloBuilder.AddWalStorage(...)`
for a custom provider (a replicated host can also supply a per-tree resolver
through `LatticeReplicationOptions.WalStorageProvider`). See
[`wal-storage-providers.md`](wal-storage-providers.md) for the provider seam
and the in-memory, file, and Azure Table providers.

## Observability

The commit pipeline's primary instrument is the per-step latency histogram
below. The leaf write-duration histogram described under
[Commit pipeline](#commit-pipeline), and the WAL writer and shard instruments
catalogued in [Metrics](metrics.md), complete the picture.

| Instrument | Type | Tags | Meaning |
|---|---|---|---|
| `orleans.lattice.wal.append.floor_refusals` | counter (`{entry}`) | `tree`, `shard`, `tenant` | Fresh local writes a replicated tree's partition refused below its [clock floor](#clock-floor-replicated-trees). A sustained rate points at silo clock skew or a write pipeline stalled for longer than `ReplicationClockFloorLag`. |
| `orleans.lattice.leaf.commit.duration` | histogram (ms) | `tree`, `step` (one of `wal`, `apply`, `digest`, `observer`) | Per-step latency of the foreground commit pipeline. The `wal` step is the durability cost; `apply` is the in-memory merge plus any relocation or split it triggers; `digest` hands the write's projection-digest change to the parent internal node - with the default `DigestCoalescingWindowMs` it schedules, or joins, a publish sent when the window elapses, so it includes the cross-grain publish itself only when the window is `0`; `observer` is the publish under the commit-log scope. |

The bundled Grafana dashboards consume these instruments directly; see
[`../lattice.dashboards/README.md`](../lattice.dashboards/README.md).

## Related surfaces

- [`wal-storage-providers.md`](wal-storage-providers.md) - pluggable backend
  contract and the in-memory, file, and Azure Table providers.
- [`projection-rebuild.md`](projection-rebuild.md) - drift detection and the
  fall-off-log rebuild path.
- [`tombstone-compaction.md`](tombstone-compaction.md) - how reaped tombstones
  interact with WAL retention.
- [`configuration.md`](configuration.md) - the full `LatticeOptions` surface.
- [`wal-causal-plus.md`](wal-causal-plus.md) - causal+ entry-schema
  extension (vector clock + dependency summary slots on `WalRecord`).
- [`../lattice.replication/wal.md`](../lattice.replication/wal.md) - the
  replication-side overlay: partitioned sink, producer-side filters,
  and the `MutationCategory.Maintenance` skip.
