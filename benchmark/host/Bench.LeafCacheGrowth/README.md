# Bench.LeafCacheGrowth

Standalone footprint probe for the unbounded `LeafCacheGrain._cache` mirror,
built for [issue #387](https://github.com/NSTA1/Orleans.Lattice/issues/387) (bound the per-activation read-through cache
size without violating Orleans semantics).

## What the issue asks for

`LeafCacheGrain._cache` mirrors its primary leaf's live entry set 1:1 and grows
monotonically over the activation's lifetime. Per-silo per-tree memory therefore
scales linearly with the touched-leaf entry count, with no operator-side cap.
The issue wants that footprint bounded, **but only if** an eviction policy can be
designed that preserves four correctness contracts (delta-refresh cursor,
pending-key set, moved-away pruning, migrated-entry delegation).

The investigation concluded that:

- **Full-row eviction** (candidate 2) and **GC-pressure eviction** (candidate 3)
  cannot clear the gate cache-locally: once the cache adopts the leaf's head
  delivery cursor, `GetDeltaSinceCursorAsync` never re-ships an evicted key until
  it is rewritten or the leaf epoch flips, so a fully-evicted row produces a
  silent false miss (contract 1 violation).
- **Value-payload-only LRU with metadata retained** (candidate 1) is the only
  candidate that can clear all four contracts, by reusing the existing
  authoritative-delegation path on a payload miss. Its cost is a per-key leaf RPC
  on the evicted fraction of reads.

Whether that tradeoff is worthwhile is empirical. This probe supplies the
**baseline (unbounded)** side of that measurement: how large the mirror actually
gets, and what the steady-state cache-hit read latency is that the candidate
would regress on the evicted fraction.

## What it measures

For each `(entry_count x value_bytes)` point the probe:

1. Activates a **real** `BPlusLeafGrain` (primary leaf) seeded with `entry_count`
   entries of `value_bytes` each, via `SetManyAsync`. No Orleans silo - the
   grains are hand-instantiated with NSubstitute runtime seams, exactly like the
   `Bench.Microbench` harness and the unit-test fakes.
2. Activates a **real** `LeafCacheGrain` in front of it and warms it to a full
   mirror (the first read trips the epoch-mismatch full-snapshot delivery).
3. Runs a uniform-random read workload for a fixed duration, sampling at
   1-second cadence:
   - `Process.WorkingSet64` and `GC.GetTotalMemory(forceFullCollection: false)`;
   - the cache mirror's own `EntryCount` and summed value-payload bytes, read
     through the `LeafCacheGrain.DebugFootprint` diagnostic seam;
   - per-read latency, reported as p50 / p99 via reservoir sampling.

A large `CacheTtl` is configured so that after warmup the steady-state read path
short-circuits the leaf entirely (`RefreshAsync`'s TTL gate), isolating the
cache's own hit-path cost.

## Reading the numbers (important attribution note)

The probe runs the leaf and the cache in **one process with no Orleans
serialization boundary**, so the cache's `byte[]` payloads *alias* the leaf's
source projection. `Process.WorkingSet64` therefore reflects one copy of the
payloads plus two envelope dictionaries, **not** the doubled footprint a real
two-silo deployment pays.

- **`cache_value_bytes`** (from the `DebugFootprint` seam) is the authoritative,
  aliasing-independent size of the cache mirror's payloads. In production that
  many bytes are resident on **every** silo that fronts the leaf with a cache
  activation. This is the per-silo footprint the issue targets. Estimate the deployed cost as
  `silos_fronting_leaf x cache_value_bytes`.
- **`working_set_*` / `gc_total_*`** are whole-process context signals only; they
  understate the deployed footprint because of the in-process aliasing.
- **`read_p50/p99`** are steady-state cache-hit latencies - the baseline the
  value-payload-only LRU candidate would regress on the evicted fraction.

## Running

```powershell
dotnet run --project benchmark/host/Bench.LeafCacheGrowth `
  -c Release
```

Configuration (environment variables; issue defaults shown):

| Variable | Default | Meaning |
|---|---|---|
| `BENCH_LEAFCACHE_ENTRY_COUNTS` | `1000,10000,100000` | Comma list of seeded entry counts. |
| `BENCH_LEAFCACHE_VALUE_BYTES` | `64,1024,65536` | Comma list of per-entry value sizes. |
| `BENCH_LEAFCACHE_DURATION_SECONDS` | `10` | Read-workload duration per cell. |
| `BENCH_LEAFCACHE_SEED_BATCH` | `5000` | Entries per `SetManyAsync` seed call. |
| `BENCH_RESULTS_PATH` | (unset) | If set, the JSON report is written here instead of stdout. |

> **Memory warning.** The full issue matrix includes `100000 x 65536` =
> ~6.1 GB of resident cache payload for that single cell (the process peaks
> around 7 GB of managed heap while it runs). Cells run sequentially with a
> forced GC between them, so the peak is one cell at a time - but size the host
> accordingly or narrow the matrix via the env vars above.

The process exits non-zero if any cell's cache mirror did not match the seeded
entry set (`cache_entry_count != entry_count` or `cache_value_bytes !=
entry_count x value_bytes`), which would mean the full-snapshot delivery did not
populate the mirror and the footprint numbers for that cell are untrustworthy.

## Baseline result (headline)

`cache_value_bytes` tracks `entry_count x value_bytes` exactly - the mirror is a
faithful 1:1 copy with no per-value overhead beyond the bounded LWW envelope.
The dominant cost is the value payload, confirming the framing that the
`byte[]` payload is the unbounded dimension and the envelope metadata
(~tens of bytes/row) is bounded. At the largest production-shaped point
(`100000 x 65536`) the mirror is ~6.1 GB **per silo**, which is the motivation
for bounding it; the candidate-1 value-payload-only LRU is the only eviction
policy that preserves the four correctness contracts while shrinking that
dimension.
