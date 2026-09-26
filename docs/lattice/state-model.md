# State Model

This document describes how Orleans.Lattice represents tree state on
disk and in memory, why the layout is shaped the way it is, and what
that means for activation cost, projection-rebuild paths, and CRDT
producer-side mutation cost.

## The three storage layers

A live tree's data is split across three storage layers with
distinct durability boundaries and growth rates:

| Layer | Lives in | Grows with | Durability boundary |
|---|---|---|---|
| Write-ahead log (WAL) | Per-shard `IWalStorageProvider` rows | Total mutation count since last GC | Foreground commit: a mutation is durable once its WAL append returns |
| Leaf state row | `BPlusLeafGrain` persistent state | Fixed-shape topology + checkpoint metadata. **Does not grow** with live-key count. | Periodic checkpoint persist (see [Configuration: `MaterialiserCheckpointInterval` / `MaterialiserCheckpointEntries`](configuration.md)) |
| Snapshot blob | `LeafSnapshotStorageGrain` persistent state | Live-key count * canonical row size | Each snapshot capture - whenever the leaf's durable coverage lags its checkpoint (the WAL GC trims only covered prefixes) and when a checkpoint nears the WAL retention horizon; see [Projection Rebuild: snapshot-on-fall-off safety net](projection-rebuild.md#snapshot-on-fall-off-safety-net) |

The **WAL is canonical.** Everything else is derived. A leaf's
per-activation entry cache is the projection of the WAL, and the
persisted checkpoint offset records how far that projection has been
durably checkpointed; the snapshot blob is a point-in-time image of
the projection, persisted separately both to bound activation cost
and as the durable coverage that lets the WAL GC trim the prefix it
covers.

## Why the leaf state row stays small

A pre-collapse `LeafNodeState` carried the per-key `Entries`
dictionary inline. That coupled the persisted row size to
`MaxLeafKeys` * average per-entry overhead - a leaf could carry
hundreds of KB of LWW state in its state row, which forced the
storage provider's per-row ceiling into the sizing model.

The collapsed leaf state row carries only:

- The owning tree id and the topology fields (sibling pointers,
  parent reference, key range, shard index, split lifecycle -
  including the durable marker of a split still in flight), plus the
  sticky moved-away slot seal an adaptive shard split records.
- The projection-digest XOR fold (`ProjectionHash`, 16 bytes).
- The `ProjectionCheckpointOffset` pointing into the WAL, plus a
  per-partition offset array on a multi-partition tree and a flag
  recording that partition 0's checkpoint was actually assigned rather
  than defaulted.
- The HLC clock and version vector.
- The last-compaction version.
- The durable high-water mark of the digest-publish sequence, so a
  re-activated leaf resumes above every sequence it already emitted.
- A ledger of unresolved replay work - the saga prepares and deferred
  terminals the flush ceiling has advanced past - which is empty in the
  steady state and lets a partition bank forward progress instead of
  re-reading the same WAL range on every activation.
- The byte size the leaf's persisted snapshot last occupied, a hint
  the next activation uses to reserve hydration budget up front.

See [Tree Storage](tree-storage.md) for exact byte-level sizing.

The per-activation entry cache - the actual per-key
last-writer-wins rows - is rebuilt on every activation from the
leaf's snapshot and the WAL, as the next section describes; nothing
in the persisted row holds it. A leaf's in-memory state is therefore
always "the projection through the WAL head at this instant"; the
persisted row is just enough metadata to anchor and bound the next
replay.

## Activation: replay, rehydrate, and the safety net

On every leaf activation, the materialiser runs three steps in
order:

1. **Rehydrate from a usable snapshot.** If the leaf's snapshot
   storage carries a readable blob, the leaf rehydrates its cache
   from the blob's canonical rows and sets each WAL partition's
   persisted checkpoint to exactly the offset the snapshot covers
   for it - advancing the checkpoint when the snapshot is newer,
   and lowering it when the snapshot sits at or behind it, so the
   tail replay rebuilds the rows past the snapshot. The cache is
   empty on a fresh activation, so even an at-or-behind snapshot is
   loaded rather than forcing a whole-window replay. This is also
   the safety net for a WAL trimmed past the persisted checkpoint
   between deactivations: the WAL GC trims only prefixes a snapshot
   covers, so the snapshot can be the only durable copy of that
   prefix. A leaf whose rehydrate lowered a checkpoint captures a
   fresh snapshot once its tail replay has re-advanced that
   checkpoint, in the same activation, so it does not reload the
   same stale snapshot every time.
2. **Choose where the replay starts.** A leaf that rehydrated from a
   snapshot resumes above it (a *warm* activation). A leaf with no
   usable snapshot starts with an empty cache, which the persisted
   checkpoint cannot anchor - replaying only past it would drop
   every entry at or below it - so it replays the whole readable
   WAL window instead (a *cold* activation).
3. **Classify, then replay, each WAL partition.** Before reading a
   partition, the fall-off-log detector classifies the gap between
   the checkpoint and the WAL. Only a WAL trimmed past the
   checkpoint (genuine loss) is fatal; a replay-budget or retention
   overrun against an intact WAL is a non-fatal over-budget verdict
   that replays anyway, and the `LeafSnapshotMargin` proximity check
   raises a snapshot-pending advisory. The materialiser then applies
   the partition's WAL records to the in-memory cache, re-folding
   the projection digest as it goes.

If the detector raised the snapshot-pending advisory, the leaf
finishes the replay and then captures a fresh snapshot once before
it yields the activation turn. While the leaf stays hot, every
`LeafSnapshotReClassifyEveryNCheckpoints` successful checkpoint
persist re-runs the classifier and captures again on advisory.

The activation path therefore tolerates any combination of:

- A fresh leaf with no snapshot and no WAL entries past the
  checkpoint (zero-cost replay).
- A fresh leaf (`ProjectionCheckpointOffset` = -1, the "nothing
  applied" sentinel) joining a WAL partition already populated by
  sibling leaves. The fall-off-log detector does not apply its
  replay budget to the sentinel because the per-leaf range filter
  inside the materialiser drops every WAL entry that falls outside
  this leaf's `[LowKeyInclusive, HighKeyExclusive)` ownership
  range, so the cost of the tail replay is bounded by the leaf's
  own range, not by the WAL head. The trim trigger is also a no-op
  for the sentinel: there is no projection state to lose.
- A leaf whose snapshot is at or behind the persisted checkpoint
  (snapshot rehydrated, each checkpoint lowered to the snapshot's
  coverage, and a tail replay from there rebuilds the rest).
- A leaf whose snapshot is newer than the persisted checkpoint and
  the WAL has been trimmed (snapshot rehydrate, then tail replay
  from the snapshot offset).

The one case it does not tolerate is genuine loss: a checkpoint the
WAL has been trimmed past with no snapshot covering the gap. Under
every `ProjectionRebuildPolicy` value (`SnapshotThenWal`,
`FullRebuildFromWal`, or `Fail`) the activation then fails with
`LeafProjectionStaleException` rather than rebuilding the leaf over
the lost prefix, because the snapshot-then-WAL and full-rebuild
recovery paths are not yet integrated - an operator-driven rebuild
is required. See [Projection Rebuild](projection-rebuild.md).

## CRDT producer-side mutation cost

For CRDT keys (any CRDT merge mode), the WAL record carries the producer's
**typed delta bytes** in `WalRecord.Delta` and omits the full-state `Value`
slot. The receiver-side `ReplicationApplier` decodes the delta and
folds it into the receiver's prior observed state via the
registered `CrdtShape`'s `MergeDelta`.

`ILattice.ApplyCrdtDeltaAsync(key, mode, deltaBytes)` is the
public surface. The typed CRDT accessors wrap this surface and are
the recommended caller-facing seam; for each mutation they read the
key's current state and mint the typed delta from it.

`LwwRegister` keys remain a full-state model: the WAL carries the
canonical post-merge `byte[]` payload in `Value`. Concurrent writers
converge by HLC last-write-wins; no delta-folding is involved.

## Registration: per-tree CRDT shape

The closed-shape CRDT modes resolve through the `CrdtShapeRegistry`'s
global fallback - no per-tree registration is required. `OrMap<TKey, TValue>`
is open-shape (the host picks `TKey` and `TValue`) and **must** be
registered per tree:

```csharp verify
using Orleans.Lattice;

siloBuilder
    .AddLattice((silo, storageName) => silo.AddMemoryGrainStorage(storageName))
    .AddOrMapShape<string, OrSet>("tagged-items");
```

The shape descriptor is installed at silo start via a hosted
service, before the first producer emission or WAL apply runs.
Registering a different `(TKey, TValue)` pair for the same tree is
a configuration error and throws at registration time.

## Related surfaces

- [WAL](wal.md) - the canonical durability layer.
- [Projection Rebuild](projection-rebuild.md) - activation-time
  recovery policy, fall-off-log triggers, and the
  snapshot-on-fall-off safety net.
- [Tree Storage](tree-storage.md) - per-grain row sizing model.
- [Replication: replication modes](../lattice.replication/replication-modes.md) -
  WAL-shape per-mode, delta-only wire format, and receiver-side
  delta-fold semantics.
- `ILattice.ApplyCrdtDeltaAsync` - producer-side typed delta surface.
- `ISiloBuilder.AddOrMapShape<TKey, TValue>(treeName)` - per-tree
  CRDT shape registration for the open-shape `OrMap` mode.
- `LatticeOptions.LeafSnapshotMargin`,
  `LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints` -
  snapshot-capture trigger and cadence.
