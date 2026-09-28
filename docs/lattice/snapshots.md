# Snapshots

Orleans.Lattice supports copying a tree into a new destination tree: an offline
snapshot is a point-in-time copy, and an online snapshot keeps mirroring the
source's writes until it completes. Snapshots are useful for backups, creating read-only copies for
analytics, or forking a dataset for experimentation.

## Snapshot Modes

For the consistency contract of each mode, see
[Consistency](consistency.md#maintenance-operations).

### Offline (`SnapshotMode.Offline`)

The source tree is **locked** (its shards marked as deleted) at the start of the
snapshot. Each shard is unlocked individually after its entries have been copied,
so earlier shards become readable again while later shards are still being
processed.

Each shard follows a three-phase pattern:

1. **Lock** (once) - mark every source shard in the copied range (see
   [Requirements](#requirements)) as deleted. The intent is persisted
   before marking so that a crash mid-lock can be recovered.
2. **Copy** - drain live entries from the source shard's leaf chain, sort them,
   and bulk-load into the corresponding destination shard.
3. **Unmark** - restore the source shard to normal operation.

Shards are processed sequentially. Earlier shards become readable again before
later shards are copied.

### Online (`SnapshotMode.Online`)

The source tree **remains available** for reads and writes during the snapshot.
Each shard's live entries are drained, keeping their source HLCs, while the
shadow-forward primitive mirrors the source's live mutations to the destination
shard with the same index; a last-writer-wins merge on the destination
reconciles a mirrored write with the drain's copy of the same key. Typed CRDT
deltas (`ApplyCrdtDeltaAsync`, `ApplyCrdtDeltaManyAsync` and the typed accessors
built on them) are not mirrored, so a delta applied to a source shard after that
shard has been drained does not reach the destination.

When the snapshot completes, it releases the shadow-forward on every source
shard before it reports itself complete, so writes to the source after that
point no longer reach the destination, the destination can be written to or
deleted independently, and the source can be snapshotted online (or resized)
again. The shadow-forward that ResizeAsync runs its internal online snapshot
under is not released here - the resize coordinator carries it on through its
swap, then moves the source shards into their rejecting phase; it clears the
shadow-forward itself only when the resize is undone, and on a completed resize
the old physical tree is soft-deleted and later purged instead.

## Usage

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");

// Offline snapshot - source tree locked during copy
await tree.SnapshotAsync("my-tree-backup", SnapshotMode.Offline);

// Online snapshot - source tree remains available
await tree.SnapshotAsync("my-tree-fork", SnapshotMode.Online);

// Snapshot with custom sizing for the destination tree
await tree.SnapshotAsync("my-tree-compact", SnapshotMode.Offline,
    maxLeafKeys: 256, maxInternalChildren: 128);
```

## Requirements

- **Same shard count (automatic)**: the snapshot registers the destination
  tree itself, pinned to the source tree's shard count, so the two always
  match - there is no destination shard count to configure or mismatch.
  The copy covers source shard indices `0` to `ShardCount - 1`, each into the
  destination shard with the same index, and the destination routes keys by
  the default shard map. A shard that an adaptive split allocated above that
  range is not copied (a split gives its target shard an index above every
  index allocated so far and leaves the pinned shard count unchanged), so a
  snapshot of a tree whose shard map routes keys to such a shard does not
  include the keys that shard holds. The split also leaves the moved keys in
  place on the shard that gave them up - hidden there from reads - and the copy
  includes them, so in the destination a key that already existed when a split
  moved it reads the value it held at that moment rather than its current one.
- **Destination must not exist**: the destination tree ID must not already be
  registered in the tree registry (`InvalidOperationException` otherwise).
  Choose a new tree ID for each snapshot.
- **Destination must differ from the source**: a destination equal to the
  source tree ID is rejected with `ArgumentException`.
- **No reserved namespace**: the destination tree ID must not start with the
  reserved `_lattice_` prefix - the umbrella namespace covering the registry
  tree itself and the `_lattice_replog_` prefix reserved for the
  `Orleans.Lattice.Replication` package's internal write-ahead-log trees - or
  the `sys-` system-data prefix, and must not name another tenant's
  `t/{tenant}/` namespace. `SnapshotAsync` rejects any of them with
  `LatticeReservedTreeNamespaceException` (an `InvalidOperationException`).
- **One snapshot per source at a time**: while a snapshot of the source is in
  flight, a request with different parameters throws
  `InvalidOperationException`; repeating the same request is a no-op.

## Crash Safety

Snapshot progress is persisted in `TreeSnapshotState` after each phase
completion. For offline mode, the snapshot intent is persisted with a **Lock**
phase *before* any source shards are marked as deleted. This ensures that a
crash between intent and shard-marking can be recovered: on restart, the
keepalive reminder re-drives the Lock phase, which idempotently marks shards.

A silo restart mid-snapshot will resume from the last completed
phase via a keepalive reminder. The grain uses the same
reminder + keepalive + grain-timer pattern as tree resize and tombstone
compaction.

Bulk-load operations into the destination shards use a deterministic operation
ID derived from the snapshot's unique operation ID, making retries idempotent.

## Sizing Overrides

Only the shard count is taken from the source tree. The destination's leaf and
internal node sizes are **not** inherited: unless you pass the `maxLeafKeys` and
`maxInternalChildren` parameters, the destination is registered with the library
defaults (128 keys per leaf, 128 children per internal node), even when the
source tree's own sizing differs. Whichever values apply
are pinned in the destination tree's registry entry when the snapshot registers
it. The registry entry is a tree's only source of structural sizing - there is no
`LatticeOptions` sizing setting for it to take priority over - and the sizing can
later be changed only with `ResizeAsync`.

## Tombstoned Keys

Snapshots only copy **live** entries. Keys that have been deleted (tombstoned)
or have expired in the source tree are excluded from the destination. Each copied entry keeps
its source HLC and any remaining time-to-live: an entry with a TTL reappears on
the destination with the same absolute expiry, not a fresh one. The destination tree gets
its own tombstone compaction reminder registered upon snapshot completion.

## Grain Interface

The snapshot is orchestrated by an internal per-source-tree coordinator grain,
keyed by the source tree
ID. That coordinator is **declared `internal`** - external callers
cannot reference or invoke it. The `ILattice` interface delegates to it via
`SnapshotAsync`:

```csharp verify
// Public API - use this
await lattice.SnapshotAsync("my-snapshot", SnapshotMode.Offline);
```

Internally, the coordinator can process all remaining shards synchronously in a
single call - a synchronous entry point used by
integration tests that drive snapshot passes deterministically.

## Relationship to Resize

`ResizeAsync` uses an online snapshot internally to create a new physical tree
with the desired sizing, so the tree keeps serving reads and writes while the
copy runs (live writes are shadow-forwarded to the new tree). After the snapshot
completes, a tree alias is set to redirect reads and writes to the new tree. This
reuses the entire snapshot infrastructure (crash safety, per-shard drain,
idempotent operation IDs) and avoids duplicating drain/rebuild logic. See
[Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree)
for details.

A snapshot of a tree that has been resized copies the tree's live data. The
coordinator resolves the source tree's alias when the snapshot starts and reads
the physical tree it points at for the whole run, rather than the shards under
the logical tree ID, which after a resize hold the retired copy.
