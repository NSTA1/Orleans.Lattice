# Snapshots

## What it shows

`SnapshotAsync` makes a **point-in-time copy of an entire tree** into a new
destination tree - useful for backups, read-only analytics forks, or cloning a
dataset for experimentation. This sample uses `SnapshotMode.Offline`: the source
is locked shard by shard during the copy, producing a strictly consistent image.
It then verifies the copy matches the source and shows the two trees are fully
independent (editing one never affects the other).

Switching to `SnapshotMode.Online` is a one-line change (`SnapshotMode.Online`):
the source then stays readable **and writable** throughout the copy, with live
mutations mirrored to the destination. Offline trades brief per-shard
unavailability for the simplest consistency story; online keeps the source hot.

> **Runtime note:** `SnapshotAsync` runs a crash-safe coordinator that copies
> one shard per timer tick. A tree defaults to 64 physical shards, so this
> sample takes a few minutes to complete even though it only holds 12 keys -
> snapshot cost scales with **shard count, not key count**.

## Run it

```
dotnet run --project samples/Snapshots
```

## Expected output

The progress dots appear once per second while the copy runs; the total time
depends on your machine (about 4-5 minutes for the default 64-shard tree).

```
Silo starting... ready.

Seeding source tree 'orders' with 12 keys...
  source count = 12

Offline snapshot: orders -> orders-backup ....[ ~4 minutes of progress dots ].... done in 259s.

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
