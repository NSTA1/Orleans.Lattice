# Snapshots

## What it shows

`SnapshotAsync` **copies a tree** into a new destination tree - an offline
snapshot as a point-in-time copy - which is useful for backups, read-only
analytics forks, or cloning a dataset for experimentation. The copy carries only
live entries (never tombstoned or expired ones) and follows the tree's shard
map, so it covers every physical shard the map routes to, including one an
adaptive split added; an online snapshot mirrors typed CRDT deltas and bulk
appends too (see
[Snapshots](../../docs/lattice/snapshots.md)). This
sample uses `SnapshotMode.Offline`: every source
shard is locked when the copy starts and each is unlocked again once its own
entries have been copied, producing a strictly consistent image.
It then verifies the copy matches the source and shows the two trees are fully
independent (editing one never affects the other).

Switching to `SnapshotMode.Online` is a one-line change (`SnapshotMode.Online`):
the source then stays readable **and writable** throughout the copy, with live
mutations mirrored to the destination. Offline trades source availability (a
shard stays locked until its own copy completes) for the simplest consistency
story; online keeps the source hot.

> **Runtime note:** `SnapshotAsync` runs a crash-safe coordinator driven by a
> two-second grain timer; each tick takes as many steps as fit in a bounded slice
> (`LatticeOptions.BackgroundDrainMaxDuration`, capped at five seconds), and an
> offline copy takes two steps on each shard (copy it, then unlock it). A tree
> defaults to 64 physical shards, so even this 12-key copy walks 64 shards -
> snapshot cost scales with **shard count** as well as key count.

## Run it

```
dotnet run --project samples/Snapshots
```

## Expected output

A progress dot appears once per second while the copy runs; the number of dots
and the total time depend on your machine (the figure shown is illustrative).

```
Silo starting... ready.

Seeding source tree 'orders' with 12 keys...
  source count = 12

Offline snapshot: orders -> orders-backup ... done in 3s.

Verifying the snapshot:
  backup live-key count = 12 (expected 12)
  backup[order:005]     = "amount=50"
  source readable again = True

Independence after editing each tree separately:
  backup[order:005] = "edited-in-backup" (edited)
  source[order:005] = "amount=50" (unchanged)
  source[order:999] = "new-in-source" (new)
  backup[order:999] = <absent> (not in the snapshot)

Done: the offline snapshot produced an independent point-in-time copy.
```

## When to use

- Backups or scheduled point-in-time copies of a tree.
- Forking a dataset into an independent tree for analytics or experimentation
  without touching production data.
- Online mode when the source cannot tolerate any read/write interruption
  during the copy.

## When not to use

- Isolating a **single reader** from concurrent writes - you do not need a whole
  second tree; open a snapshot cursor instead (see
  [SnapshotCursors](../SnapshotCursors)).
- Latency-sensitive paths that need the copy to finish quickly on a
  high-shard-count tree - snapshot time scales with shard count.

## Feature doc

- [Snapshots](../../docs/lattice/snapshots.md)
