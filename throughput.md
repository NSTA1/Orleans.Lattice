# Orleans.Lattice throughput report

> **Status**: WORK-IN-PROGRESS skeleton populated by [throughput-capture-plan.md](throughput-capture-plan.md). Layer-1 cells fill in after the BDN run completes; Layer-2 cells (except `SetMany`) fill in after the per-mode `azure-throughput` ladders complete.
>
> **Source commit**: `b872262` on branch `throughput` (leaf etag-race fix, BPlusLeafGrainTests.EtagRace.cs). Numbers below are produced against this commit; the chaos suite has not yet run against it, so the report is for informational presentation only — not a release artefact.

## Headline table

| Operation        | Layer 1 — In-process median (µs/op) | Layer 1 — Allocated (B/op) | Layer 1 — Single-thread throughput (op/s) | Layer 2 — Sustained throughput (op/s) | Layer 2 — p50 (ms) | Layer 2 — p99 (ms) |
| ---------------- | ----------------------------------: | -------------------------: | ----------------------------------------: | ------------------------------------: | -----------------: | -----------------: |
| `GetAsync` (point read)         | **0.41**          | 456    | **~2.47 M**       | TBD                              | TBD                | TBD                |
| `SetAsync` (point write)        | **3.97**          | 672    | **~252 k**        | TBD                              | TBD                | TBD                |
| `GetManyAsync` (16 keys/call)   | **8.06**          | 6,968  | **~124 k** calls/s ≈ **~2.0 M** keys/s | TBD                  | TBD                | TBD                |
| `SetManyAsync` (1,000 entries/call, single shard) | **700.5** | 237,972 | **~1.43 k** calls/s ≈ **~1.43 M** entries/s | **12,708** entries/s @ 4,096 entries/call (probe-0) | TBD | TBD |
| `SetManyAtomicAsync` (16 keys/saga) | **188.1**     | 64,653 | **~5.3 k** sagas/s ≈ **~85 k** keys/s | TBD                          | TBD                | TBD                |

**Layer-1 source**: BDN run `2026-05-27T09-02-19Z` on this branch (commit `b872262`), AMD Ryzen 7 PRO 7840U / 16 logical / 8 physical, .NET 10.0.8, in-process toolchain. P50 column shown above; mean and p99 are in the per-op detail table further down. "Single-thread throughput" is the derived `1 / p50`; treat it as the algorithmic ceiling, **not** as a multi-thread scaling claim. Multi-key rows (`GetManyAsync`, `SetManyAsync`, `SetManyAtomicAsync`) show both calls/sec and entries/sec — the entries/sec column is what the audience compares against per-key throughput of the point ops.

**Layer-2 source**: c2-vii probe-0 ladder at the c2-iii operating-point baseline (single silo, 32 shards, real Azure Tables Standard, West Europe). Only `SetManyAsync` measured today; the other four Layer-2 cells require the harness extension described in [throughput-capture-plan.md](throughput-capture-plan.md) steps 2-9.

### Layer-1 detail (full mean / p50 / p99 / allocated, per [BenchmarkDotNet](https://benchmarkdotnet.org) run)

| Benchmark               | Mean (ns) | P50 (ns) | P99 (ns)   | Allocated (B) | Notes |
| ----------------------- | --------: | -------: | ---------: | ------------: | ----- |
| `PointRead`             | 2,801.6   | 405.3    | 41,530.2   | 456           | single key, populated leaf |
| `PointReadWithVersion`  | 2,364.8   | 586.4    | 34,777.4   | 496           | returns `VersionedValue` |
| `PointWrite`            | 5,779.1   | 3,971.4  | 28,731.3   | 672           | single key, populated leaf |
| `PointGetMany`          | 11,015.9  | 8,061.7  | 52,687.0   | 6,968         | 16 keys/call against pre-populated leaf |
| `BulkLoad`              | 815,629.3 | 700,461.1 | 1,888,536.6 | 237,972    | 1,000 entries/call, single leaf |
| `SetMany_4Shards`       | 574,726.1 | 561,256.3 | 691,335.8 | 250,252      | 1,000 entries across 4 shards |
| `SetManyAtomic`         | 263,732.4 | 188,064.0 | 1,057,993.5 | 64,653     | 16 keys/saga, single shard |
| `SetManyAtomic_4Shards` | 162,124.0 | 156,984.4 | 226,395.0 | 97,979       | 16 keys/saga across 4 shards |
| `BulkLoad_DeepTree`     | not extracted | 33,308.5 | 124,526.3 | 15,968    | deep B+tree shape (height ≥ 2) |
| `BulkLoad_DeeperTree`   | 144,844.8 | 148,757.3 | 239,282.3 | 80,185       | 32 entries/call, deeper tree |
| `PointWrite_DeepTree`   | 6,515.5   | 4,170.7  | 37,675.5   | 1,592         | single key, deep tree |
| `PointWrite_DeeperTree` | 13,846.0  | 11,849.5 | 41,452.1   | 6,192         | single key, deeper tree |
| `PointRead_DeeperTree`  | 13,489.3  | 2,245.2  | 116,274.2  | 1,326         | single key, deeper tree |
| `PointReadAtomicTreeIdle` | 2,411.8 | 325.7    | 39,657.7   | 456           | read on a tree configured for atomic writes, no in-flight saga |
| `PointReadAtomicTreeWithActiveSaga` | 2,128.6 | 379.3 | 33,631.0 | 456    | read on the same tree with a concurrent saga in flight |
| `SetManyAtomic_Concurrent_1`  | 146,018.5 | 143,345.8  | 180,348.6 | 63,334    | 16 keys/saga × 1 concurrent saga |
| `SetManyAtomic_Concurrent_4`  | 559,296.7 | 588,283.5  | 957,841.3 | 246,951   | 16 keys/saga × 4 concurrent sagas |
| `SetManyAtomic_Concurrent_16` | 2,489,389.8 | 2,577,123.6 | 3,766,498.8 | 987,509 | 16 keys/saga × 16 concurrent sagas |
| `SetManyAtomic_Concurrent_64` | 10,737,081.3 | 11,968,757.0 | 14,212,772.6 | 3,949,740 | 16 keys/saga × 64 concurrent sagas |

Raw harness JSON: `benchmark/.run/microbench/2026-05-27T09-02-19Z/results.json`.

**Rung context** (Layer 2): single silo, 32 shards, `WalPartitions=8`, `WalMaxPendingBatches=8`, `PhaseTwoCoalescingWindow=5ms`, `PipelinePhaseTwoCommits=true`, `FlushConcurrency=8`, `FlushMs=50`, `BatchSize=4096`, real Azure Tables Standard account (West Europe), producer rung `10000:5` (10,000 vehicle ids × 5 Hz tick = 50,000 events/sec target offered load, 245 B/event), 60-second producer window (`DurationSec=60`), `SteadyAvg` computed over the productive sub-window with drain tail excluded per the c2-vi-bench-timing-fix. Source commit `b872262`.

## Two-layer methodology

Two layers because they answer different questions; either alone misleads.

### Layer 1 — In-process microbench

**Source**: [benchmark/host/Bench.Microbench/LatticeMicroBenchmarks.cs](benchmark/host/Bench.Microbench/LatticeMicroBenchmarks.cs), driven by [BenchmarkDotNet](https://benchmarkdotnet.org). The harness instantiates the lattice's grain layer in-process against an in-memory storage provider; no Orleans RPC, no Azure I/O. BenchmarkDotNet runs O(10⁴–10⁶) iterations per op and computes mean / median / stddev / allocations with very tight error bars.

**Reading**: the algorithmic cost of one call to each `ILattice` method when scheduling and storage are free. The upper bound on what the implementation could theoretically sustain if every other layer were perfect.

**Units**:
- `Mean (µs/op)` — wall-clock time per call (microseconds).
- `Allocated (B/op)` — bytes of managed heap allocation per call. Lower is better for GC pressure under steady-state load.
- `Single-thread throughput (op/s)` — derived `10⁶ / mean_µs`. The number of calls one thread could complete per second if it ran the op back-to-back with no other work.

### Layer 2 — Azure-throughput bench

**Source**: [benchmark/azure-throughput/Silo/Program.cs](benchmark/azure-throughput/Silo/Program.cs) (silo) and [benchmark/azure-throughput/Producer/Program.cs](benchmark/azure-throughput/Producer/Program.cs) (producer), deployed via [benchmark/azure-throughput/scripts/40-ladder.ps1](benchmark/azure-throughput/scripts/40-ladder.ps1) to Azure Container Instances against a real Azure Tables Standard account. The producer streams vehicle telemetry over TCP; the silo's `TcpIngestService` batches events and dispatches them through `ILattice` against the c2-iii operating-point baseline.

**Reading**: end-to-end sustained throughput on a single silo at a realistic operating point with durable storage. The number to quote as "what the system does in production" on a single silo.

**Units**:
- `Sustained throughput (op/s)` — the productive-window steady-state average (drain tail excluded; the bench-timing fix described in the c2-vi-bench-timing-fix memo applies). For multi-key ops (`SetManyAsync`, `GetManyAsync`, `SetManyAtomicAsync`), one "op" is one batched call and the number of entries per batch is documented per row.
- `p50 / p99 (ms)` — per-call wall-clock latency at the 50th / 99th percentile from the `BenchMetrics.LatticeOpDurationMs` histogram over the productive window.

## How each op is dispatched (Layer 2)

The Layer-2 harness uses `BENCH_WORKLOAD_MODE` to select the dispatch shape; all five modes share the same producer event stream:

- **`set-many`** — every producer batch becomes one `ILattice.SetManyAsync(entries)` call. Default. The mode that produced today's `12,708 op/s` headline.
- **`set-many-atomic`** — every producer batch is sliced into 64-key sub-batches; each sub-batch becomes one `ILattice.SetManyAtomicAsync(entries)` saga call. 64 is the realistic atomic-batch size; 4096-key sagas are not an audience-relevant shape.
- **`set-point`** — every entry in the batch becomes one `ILattice.SetAsync(key, value)` call; the silo fans these out in parallel up to `BENCH_FLUSH_CONCURRENCY` (8). The reported "op/s" is **point-writes per second**, not batches per second.
- **`get-point`** — silo pre-seeds `BENCH_VEHICLE_COUNT` keys via `BulkLoadAsync` at startup, then drives one `ILattice.GetAsync(key)` per producer-event; same parallel fan-out shape as `set-point`.
- **`get-many`** — same pre-seed as `get-point`; every producer batch's key list becomes one `ILattice.GetManyAsync(keys)` call.

The pre-seed for the read modes ensures Layer-2 read numbers exercise a populated tree (warm leaf cache, populated WAL, populated grain state). Cold-cache reads are out of scope for this report; a follow-up could add a "between-pre-seed-and-measurement deactivate" lever.

## Configuration baseline (Layer 2)

The c2-iii operating point, validated by the c2-vii probe-0 ladder ([scaling.md](scaling.md) U9p step c2-vii memo). This is the **full reproducible set** — every env-var pinned below is required to reproduce the headline numbers; defaults outside this table either don't affect throughput or are the deploy-script default at [commit b872262](benchmark/azure-throughput/scripts/20-build-and-deploy.ps1).

### Producer (offered-load shape, rung `10000:5`)

| Knob                          | Value | Source       |
| ----------------------------- | ----: | ------------ |
| `BENCH_VEHICLE_COUNT`         | 10000 | rung `N:Hz`  |
| `BENCH_TICK_HZ`               | 5     | rung `N:Hz`  |
| `BENCH_DURATION_SEC`          | 60    | `40-ladder.ps1 -DurationSec` |
| `BENCH_TOTAL_DURATION_SEC`    | 120   | watchdog cap; drain-tail excluded from `SteadyAvg` |
| Producer payload (per event)  | 245 B (p50, range 244–247) | measured `tcp.read.line_bytes` in c2-vii silo logs |

Offered load = `10000 × 5 = 50,000 events/sec` arriving at the silo over TCP. The silo batches every `BENCH_FLUSH_MS=50ms` window (or when the in-memory accumulator reaches `BENCH_BATCH_SIZE=4096` entries, whichever fires first).

### Silo (c2-iii operating-point baseline)

| Knob                                | Value | Source       |
| ----------------------------------- | ----: | ------------ |
| `BENCH_SHARD_COUNT`                 | 32    | c2-ii probe (`shardCount` sweet spot; s=64 regressed -14%) |
| `BENCH_WAL_PARTITIONS`              | 8     | c2-vii probe-0 (best of `WP ∈ {8,16}` sweep) |
| `BENCH_WAL_MAX_PENDING_BATCHES`     | 8     | c2-vii probe-0 (best of `WMP ∈ {8,16}` sweep) |
| `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS` | 5 | c2-vii probe-0 (P2=0 regressed -57%; P2=2 regressed -28%) |
| `BENCH_PIPELINE_PHASE2`             | 1     | c2-ii (true = pipelined phase-2 commits) |
| `BENCH_WAL_ELIMINATE_CANDIDATE_ROW` | false | wire-compat default |
| `BENCH_FLUSH_CONCURRENCY`           | 8     | U9hA (in-silo SetMany flush semaphore) |
| `BENCH_FLUSH_MS`                    | 50    | U9l (TcpIngestService flush-window cadence) |
| `BENCH_BATCH_SIZE`                  | 4096  | U7 default (entries per outgoing `ILattice.SetManyAsync`) |
| `BENCH_RESPONSE_TIMEOUT_SEC`        | 180   | b872262 (Orleans silo+client `ResponseTimeout`) |
| `BENCH_LEAF_STORAGE_KIND`           | `azure` | c2-iii (Azure Table grain storage for leaf/internal/atomic state) |
| `BENCH_LEAF_STORAGE_TABLE`          | `OrleansLatticeGrainState` | default |
| `BENCH_LEAF_STORAGE_NUM_GRAINS`     | 0     | use Azure default (no memory grain partitioning) |
| `BENCH_PHASEA_REPORT_SEC`           | 10    | diagnostic cadence for per-op p50/p99 capture |
| `BENCH_WORKLOAD_MODE`               | varies per row in headline table | new; default `set-many` preserves current behaviour |
| `BENCH_ATOMIC_BATCH_SIZE`           | 64    | new; applies only when `BENCH_WORKLOAD_MODE=set-many-atomic` |

### Silo source

Source commit `b872262` on branch `throughput`. The leaf etag-race fix is required: without it, the c2-iii baseline collapses at `25000:5` (1,158 op/s vs the post-fix 12,044 op/s). See [scaling.md](scaling.md) U9p step c2-vii memo for the diagnosis.

### Library configuration (not deploy-script knobs, but pinned for completeness)

The deploy-script env-vars above are translated into `LatticeOptions` and `AzureTableWalStorageOptions` settings in [benchmark/azure-throughput/Silo/Program.cs](benchmark/azure-throughput/Silo/Program.cs). The library defaults differ from the c2-iii operating point on four axes:

| Library option                                  | Library default | c2-iii operating point | Why different |
| ----------------------------------------------- | --------------: | ---------------------: | ------------- |
| `LatticeOptions.WalPartitions`                  | 1               | 8                      | wire-compat (existing trees pin via manifest) |
| `LatticeOptions.WalMaxPendingBatches`           | 1               | 8                      | wire-compat (in-flight pipeline depth) |
| `AzureTableWalStorageOptions.PhaseTwoCoalescingWindow` | `TimeSpan.Zero` | 5 ms             | wire-compat (drain-on-first-signal default) |
| `AzureTableWalStorageOptions.PipelinePhaseTwoCommits` | `false`   | `true`                 | wire-compat (synchronous phase-2 by default) |

Flipping these library defaults is a recommended follow-up per Phase B2 / B3 / C1 of the [scaling.md](scaling.md) plan ladder, each gated on the chaos suite.

## Caveats footnote

1. **Single silo.** Every Layer-2 number is from a single silo. The documented Azure Tables Standard-account ceiling is 20,000 entities/s per account; the SetMany cell at `12,708 op/s` is ~63% of that ceiling. A second silo with shard fan-out is the next campaign axis.
2. **Pre-fix-vs-post-fix.** Commit `b872262` fixed a leaf-grain etag race that, prior to today, was silently regressing throughput at higher rungs (25000:5 collapsed to 1,158/s; with the fix it reaches 12,044/s). Numbers in this report are post-fix; the chaos suite has not yet been run against `b872262`.
3. **Allocations only meaningful in Layer 1.** Layer 2 includes Orleans serialization, Azure SDK marshalling, and `System.Net.Http` allocations that are not part of the lattice's per-op cost surface. Layer-1 `Allocated (B/op)` is the right column for tracking the library's own GC pressure.
4. **GetMany batch size.** The Layer-2 `GetManyAsync` row is per-call, where one "call" carries the producer-batch's worth of keys (4096 keys at the c2-iii baseline). Per-key throughput is `(batches/sec) × 4096`; the report shows the per-call number because that's the API shape callers actually see.
5. **SetManyAtomic batch size.** Likewise, the atomic row is per-saga-call with 64 keys/saga. Per-key throughput is `(sagas/sec) × 64`. The 64-key shape is closer to real saga usage.

## Provenance / artefacts

Layer 1: BDN JSON results at `benchmark/.run/microbench/<runid>/results.json` (gitignored).

Layer 2: per-mode CSVs under `benchmark/azure-throughput/scripts/`, archived with the naming convention `.ladder-results-c2-vii-mode-{set-many,set-many-atomic,set-point,get-point,get-many}.csv`. The corresponding silo logs under `benchmark/azure-throughput/.run/silo-*.log` (gitignored; large).
