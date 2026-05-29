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
| Snapshot blob | `LeafSnapshotStorageGrain` persistent state | Live-key count * canonical row size | Snapshot-on-fall-off capture path; see [Projection Rebuild: snapshot-on-fall-off safety net](projection-rebuild.md#snapshot-on-fall-off-safety-net) |

The **WAL is canonical.** Everything else is derived. A leaf's
per-activation entry cache is the projection of the WAL through the
leaf's `ProjectionCheckpointOffset`; the snapshot blob is a
point-in-time image of that projection persisted separately for
activation-cost reasons.

## Why the leaf state row stays small

A pre-collapse `LeafNodeState` carried the per-key `Entries`
dictionary inline. That coupled the persisted row size to
`MaxLeafKeys` * average per-entry overhead - a leaf could carry
hundreds of KB of LWW state in its state row, which forced the
storage provider's per-row ceiling into the sizing model.

The collapsed leaf state row carries only:

- Topology fields (sibling pointers, parent reference, key range,
  shard index, split lifecycle).
- The projection-digest XOR fold (`ProjectionHash`, 16 bytes).
- The `ProjectionCheckpointOffset` pointing into the WAL.
- The HLC clock and version vector.
- The last-compaction version.

See [Tree Storage](tree-storage.md) for exact byte-level sizing.

The per-activation entry cache - the actual `(key,
LwwValue<byte[]>)` rows - is rebuilt by replaying WAL entries
strictly past `ProjectionCheckpointOffset` on activation. A leaf's
in-memory state is therefore always "the projection through the
WAL head at this instant"; the persisted row is just enough metadata
to bound the next replay.

## Activation: replay, rehydrate, and the safety net

On every leaf activation, the materialiser runs three steps in
order:

1. **Classify the persisted checkpoint.** The fall-off-log detector
   reads `ProjectionCheckpointOffset` and the WAL tail; if either a
   hard trigger fires (WAL trimmed past checkpoint, replay budget
   exceeded, cold past retention) or the `LeafSnapshotMargin`
   advisory fires, the detector returns the matching
   `FallOffLogDecision`.
2. **Prefer a snapshot when newer than the checkpoint.** If
   `LeafSnapshotStorageGrain` carries a blob whose `SnapshotOffset`
   strictly exceeds the persisted `ProjectionCheckpointOffset`, the
   leaf rehydrates its cache from the blob's canonical rows and
   advances the persisted checkpoint to the snapshot offset. This
   is the safety net for the case where the WAL has been trimmed
   past the persisted checkpoint between deactivations.
3. **Tail-replay the WAL.** From the resolved checkpoint forward,
   the materialiser applies WAL records to the in-memory cache,
   re-folding the projection digest as it goes.

If the fall-off-log decision was `SnapshotPending` (advisory only),
the leaf finishes the tail replay and then fires a single
`CaptureSnapshotAsync` to refresh the snapshot grain before it
yields the activation turn. While the leaf stays hot, every
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
- A leaf whose snapshot is older than the persisted checkpoint
  (snapshot ignored, tail replay handles it).
- A leaf whose snapshot is newer than the persisted checkpoint and
  the WAL has been trimmed (snapshot rehydrate, then tail replay
  from the snapshot offset).
- A leaf whose checkpoint has fallen off the WAL retention window
  entirely (the configured `ProjectionRebuildPolicy` -
  `SnapshotThenWal`, `FullRebuildFromWal`, or `Fail` - takes over).

## CRDT producer-side mutation cost

For CRDT keys (modes `OrSet`, `OrMap`, `PnCounter`, `MvRegister`,
`VersionVector`), the WAL record carries the producer's **typed
delta bytes** in `WalRecord.Delta` and omits the full-state `Value`
slot. The receiver-side `ReplicationApplier` decodes the delta and
folds it into the receiver's prior observed state via the
registered `CrdtShape`'s `MergeDelta`.

`ILattice.ApplyCrdtDeltaAsync(key, mode, deltaBytes)` is the
public surface. Typed accessors (`OrSetAccessor`,
`PnCounterAccessor`, `MvRegisterAccessor`, `OrMapAccessor`) wrap
this surface and are the recommended caller-facing seam; they own
the typed delta DTO construction and the producer-side state cache.

`LwwRegister` keys remain a full-state model: the WAL carries the
canonical post-merge `byte[]` payload in `Value`. Concurrent writers
converge by HLC last-write-wins; no delta-folding is involved.

## Registration: per-tree CRDT shape

The closed-shape CRDT modes (`OrSet`, `PnCounter`,
`VersionVector`, `MvRegister`) resolve through the
`CrdtShapeRegistry`'s global fallback - no per-tree registration
is required. `OrMap<TKey, TValue>` is open-shape (the host picks
`TKey` and `TValue`) and **must** be registered per tree:

```csharp verify
using Orleans.Lattice.Primitives;

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
