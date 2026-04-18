# Snapshots

Orleans.Lattice supports creating point-in-time snapshots of a tree into a new
destination tree. Snapshots are useful for backups, creating read-only copies for
analytics, or forking a dataset for experimentation.

## Snapshot Modes

### Offline (`SnapshotMode.Offline`)

The source tree is **locked** (all shards marked as deleted) at the start of the
snapshot. Each shard is unlocked individually after its entries have been copied,
so earlier shards become readable again while later shards are still being
processed. This guarantees a fully consistent point-in-time snapshot.

Each shard follows a three-phase pattern:

1. **Lock** (once) — mark all source shards as deleted. The intent is persisted
   before marking so that a crash mid-lock can be recovered.
2. **Copy** — drain live entries from the source shard's leaf chain, sort them,
   and bulk-load into the corresponding destination shard.
3. **Unmark** — restore the source shard to normal operation.

Shards are processed sequentially. Earlier shards become readable again before
later shards are copied.

### Online (`SnapshotMode.Online`)

The source tree **remains available** for reads and writes during the snapshot.
Each shard's live entries are drained independently without locking. The result
is a best-effort point-in-time copy: concurrent mutations between processing
shard 0 and shard 1 may cause minor inconsistencies (a key written after shard 0
was drained but before shard 1 is drained may appear in one but not the other).
The consistency model is similar to a non-repeatable-read isolation level.

## Usage

```csharp
var tree = grainFactory.GetGrain<ILattice>("my-tree");

// Offline snapshot — source tree locked during copy
await tree.SnapshotAsync("my-tree-backup", SnapshotMode.Offline);

// Online snapshot — source tree remains available
await tree.SnapshotAsync("my-tree-fork", SnapshotMode.Online);

// Snapshot with custom sizing for the destination tree
await tree.SnapshotAsync("my-tree-compact", SnapshotMode.Offline,
    maxLeafKeys: 256, maxInternalChildren: 128);
```

## Requirements

- **Same shard count**: the source and destination trees must have the same
  `ShardCount` configuration. The snapshot grain validates this and throws
  `InvalidOperationException` if they differ.
- **Destination must not exist**: the destination tree ID must not already be
  registered in the tree registry. Choose a new tree ID for each snapshot.
- **No system prefix**: the destination tree ID must not start with the reserved
  `_lattice_` prefix.

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

By default, the destination tree inherits the source tree's configured leaf and
internal node sizes. You can override these with the `maxLeafKeys` and
`maxInternalChildren` parameters. These overrides are stored in the tree
registry as a `TreeRegistryEntry` and take priority over `IOptionsMonitor`
configuration.

## Tombstoned Keys

Snapshots only copy **live** entries. Keys that have been deleted (tombstoned)
in the source tree are excluded from the destination. The destination tree gets
its own tombstone compaction reminder registered upon snapshot completion.

## Grain Interface

The snapshot is orchestrated by `ITreeSnapshotGrain`, keyed by the source tree
ID. This grain is **guarded by `InternalGrainGuardFilter`** — external callers
cannot invoke it directly. The `ILattice` interface delegates to it via
`SnapshotAsync`:

```csharp
// Public API — use this
await lattice.SnapshotAsync("my-snapshot", SnapshotMode.Full);
```

Internally, `ITreeSnapshotGrain` exposes `RunSnapshotPassAsync` which processes
all remaining shards synchronously in a single call. This method is used by
integration tests that drive snapshot passes deterministically.

## Relationship to Resize

`ResizeAsync` uses an offline snapshot internally to create a new physical tree
with the desired sizing. After the snapshot completes, a tree alias is set to
redirect reads and writes to the new tree. This reuses the entire snapshot
infrastructure (crash safety, per-shard bulk load, idempotent operation IDs)
and avoids duplicating drain/rebuild logic. See
[Tree Sizing — Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree)
for details.
