# Diagnostics

## What it shows

`ILattice.DiagnoseAsync` returns a point-in-time health snapshot of a tree without
touching application data paths. The report exposes the shard count, total live keys,
total tombstones, recent split activity, and a per-shard breakdown (B+ tree depth,
live keys, tombstones, tombstone ratio, read/write counts, and current ops/second).
This sample writes ten keys, deletes three (leaving tombstones), then prints a deep
snapshot. A deep report walks each shard's leaf chain so tombstone counts are exact;
a shallow report skips that walk and reports zero tombstones.

## Run it

```
dotnet run --project samples/Diagnostics
```

## Expected output

The tree spreads keys across virtual shards by hashing, so the specific shard indices
and the `ops/s` values (which depend on timing) vary between runs. The totals -
7 live keys and 3 tombstones after 10 writes and 3 deletes - are deterministic.

```
Silo starting... ready.

Writing 10 keys, then deleting 3 (leaving tombstones)...

Tree 'inventory' health snapshot (sampled 2026-07-02T18:34:14.5400810+00:00):
  Deep report:        True
  Shard count:        64 (of 4096 virtual slots)
  Total live keys:    7
  Total tombstones:   3
  Recent splits:      0

Per-shard breakdown (shards with activity only):
  shard 13: depth=1 rootIsLeaf=True live=1 tombstones=0 ratio=0.00 reads=0 writes=1 ops/s=11.4
  shard 14: depth=1 rootIsLeaf=True live=0 tombstones=1 ratio=1.00 reads=0 writes=2 ops/s=17.8
  shard 15: depth=1 rootIsLeaf=True live=1 tombstones=0 ratio=0.00 reads=0 writes=1 ops/s=12.1
  shard 19: depth=1 rootIsLeaf=True live=0 tombstones=1 ratio=1.00 reads=0 writes=2 ops/s=29.9
  shard 21: depth=1 rootIsLeaf=True live=1 tombstones=0 ratio=0.00 reads=0 writes=1 ops/s=16.0
  shard 26: depth=1 rootIsLeaf=True live=1 tombstones=0 ratio=0.00 reads=0 writes=1 ops/s=17.0
  shard 35: depth=1 rootIsLeaf=True live=0 tombstones=1 ratio=1.00 reads=0 writes=2 ops/s=25.3
  shard 43: depth=1 rootIsLeaf=True live=1 tombstones=0 ratio=0.00 reads=0 writes=1 ops/s=13.5
  shard 62: depth=1 rootIsLeaf=True live=1 tombstones=0 ratio=0.00 reads=0 writes=1 ops/s=13.1
  shard 63: depth=1 rootIsLeaf=True live=1 tombstones=0 ratio=0.00 reads=0 writes=1 ops/s=14.3
```

## When to use

- Health probes and dashboards: poll `DiagnoseAsync(deep: false, ...)` at a low rate
  to track live-key growth, tombstone ratio, and per-shard hotness cheaply.
- Post-mortem investigation: run `DiagnoseAsync(deep: true, ...)` to get exact
  tombstone counts and B+ tree depth when diagnosing compaction or split behaviour.

## When not to use

- Do not call `DiagnoseAsync` on the hot path or per request. It is an admin-rate
  API; a deep report walks leaf chains and is comparatively expensive.
- Do not rely on `ops/s` as a precise benchmark - it is a short-window hotness hint,
  not a load-test measurement.

## Feature doc

See [../../docs/lattice/diagnostics.md](../../docs/lattice/diagnostics.md).
