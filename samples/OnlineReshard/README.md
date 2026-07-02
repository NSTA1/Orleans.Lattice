# OnlineReshard

## What it shows

Lattice can grow the number of physical shards a tree's key space is spread
across **while the tree stays online** - no downtime, no data loss. This sample
writes 24 keys, kicks off `ReshardAsync(72)`, writes another key *while the
migration is in flight*, and polls until `IsReshardCompleteAsync` reports done.
It measures the distinct physical shard count from the effective `ShardMap`
before and after, and re-reads every key to prove nothing was lost.

## Run it

```
dotnet run --project samples/OnlineReshard
```

## Expected output

```
Silo starting... ready.

== Writing 24 keys ==
  wrote key/000 .. key/023

Starting physical shard count: 64
Resharding online to 72 shards...

  wrote key/live-during-reshard WHILE the migration was in flight
    ...migrating (distinct shards so far: 66)
    ...migrating (distinct shards so far: 70)
    ...migrating (distinct shards so far: 72)

Final physical shard count: 72

== Verifying all keys survived the reshard ==
  original keys intact : 24/24
  mid-migration write   : written mid-migration

[OK] shard count grew 64 -> 72 with zero data loss and no downtime.
```

(The exact intermediate "distinct shards so far" lines vary run to run as the
migration progresses; the start count, final count, and data-intact result are
stable.)

## When to use

- Scaling a hot tree horizontally: spread its key space over more shards to
  relieve load, without taking the tree offline or blocking writes.
- Capacity planning where shard count needs to grow as data volume grows.

## When not to use

- Shrinking shard count. This is a grow-only online migration.
- Expecting instant completion. Resharding is a background migration; poll
  `IsReshardCompleteAsync` rather than assuming the new count is live immediately.

## Feature docs

[docs/lattice/online-reshard.md](../../docs/lattice/online-reshard.md)
