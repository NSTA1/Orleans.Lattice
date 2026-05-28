# Orleans.Lattice throughput report


## Headline table

All latency values are expressed in **milliseconds (ms)**. Sub-microsecond Layer-1 reads show as `<0.001 ms` with the exact nanosecond value in the detail table below; sub-millisecond values are shown to 3 decimal places (e.g. `0.405` ms = 405 µs = 405,000 ns).

| Operation        | Layer 1 - In-process p50 (ms/op) | Layer 1 - Allocated (B/op) | Layer 1 - Single-thread throughput (op/s) | Layer 2 - Sustained throughput (op/s) | Layer 2 - p50 (ms/call) | Layer 2 - p99 (ms/call) |
| ---------------- | -------------------------------: | -------------------------: | ----------------------------------------: | ------------------------------------: | ----------------------: | ----------------------: |
| `GetAsync` (point read)         | **0.000 405**       | 456    | **~2.47 M**       | **45,750** keys/s                                              | **~0.11**¹           | **~0.18**¹             |
| `SetAsync` (point write)        | **0.003 971**       | 672    | **~252 k**        | **202** keys/s (sustained), **16,381** keys/s (burst max)² ¹⁰     | **~58**¹⁰            | **~300**¹⁰              |
| `GetManyAsync` (4,096 keys/call) | **0.008 062** (16 keys) | 6,968 | **~124 k** calls/s ≈ **~2.0 M** keys/s | **178,927** keys/s ≈ **43.6** calls/s (sustained)⁶ | **14.1**           | **68.6**               |
| `SetManyAsync` (4,096 entries/call) | **0.700** (1,000 entries) | 237,972 | **~1.43 k** calls/s ≈ **~1.43 M** entries/s | **13,574** entries/s ≈ **3.3** calls/s (sustained), **24,551** entries/s burst max | **2,012**⁸            | **2,854**⁸             |
| `SetManyAtomicAsync` (64 keys/saga) | **0.188** (16 keys/saga) | 64,653 | **~5.3 k** sagas/s ≈ **~85 k** keys/s | **465** keys/s ≈ **7.3** sagas/s (sustained), **1,793** keys/s burst max³ ⁵ ⁷ ⁹ | **~800**⁴            | **~1,030**⁴            |

¹ Layer-2 `SetAsync` / `GetAsync` are dispatched in a parallel fan-out under `FlushConcurrency=8`. The `DispatchAsync` call carries one full producer batch (4,096 per-key calls); the per-call latency shown is derived by dividing `DispatchAsync` p50/p99 by 4,096. Direct per-call timing would require instrumenting each `ILattice.SetAsync` / `GetAsync` invocation individually, which the bench harness does not currently do.

² Layer-2 `SetAsync` is offered-load-bound at the c2-iii operating point - each per-key call pays a full Orleans grain RPC + WAL append round-trip and the dispatcher idles between batches, so the headline cell does not cleanly attribute to per-call latency. The `SteadyAvg` of 202 keys/s reflects the silo running below its per-call ceiling because the producer's 4,096-entry batch flushed every 50 ms upstream does not keep all `FlushConcurrency=8` slots permanently busy; `SteadyMax` of 16,381 keys/s reflects the moments when in-flight calls were returning in burst. The latency cells in caveat ¹⁰ are the definitive per-call measurement (count > 12,000 samples on the c2-xxviii log).

³ Layer-2 `SetManyAtomicAsync` and `GetManyAsync` are tuned per row to the operating point where each saturates: `SetManyAtomicAsync` at `BENCH_VEHICLE_COUNT=500, BENCH_TICK_HZ=10, BENCH_BATCH_SIZE=256` (5,000 events/s offered, comfortably exceeding the atomic-saga path's measured ~200-800 keys/s capacity without saturating instantly), and `GetManyAsync` at `BENCH_VEHICLE_COUNT=50000, BENCH_TICK_HZ=5, BENCH_BATCH_SIZE=4096` (250,000 events/s offered, comfortably above the read-path ceiling so the silo runs at its actual sustained rate rather than the producer's offered rate). `GetAsync`, `SetAsync`, and `SetManyAsync` rows use the c2-iii baseline `vehicles=10000, batchSize=4096, tickHz=5` shape (50,000 events/s offered). The headline numbers in each row are the silo's productive-window steady-state sustained rate; burst-max columns are dropped for `GetManyAsync` (no meaningful per-call burst structure under a saturating producer).

⁴ Layer-2 `SetManyAtomicAsync` p50/p99 are per **single 64-key saga** at the c2-iii operating point, sourced from the saga-internal phase histograms shipped in scaling.md's c2-xvi / c2-xvii / c2-xxiii memos (`orleans.lattice.saga.prepare.duration` + `orleans.lattice.saga.terminal_decision.duration` + `orleans.lattice.saga.broadcast.duration` + `orleans.lattice.saga.checkpoint.duration` + `orleans.lattice.saga.reminder.duration`). The per-saga p50 of ~800 ms decomposes as: prepare 668 ms (84%) + broadcast 99 ms (12%) + decision 19 ms (2%) + checkpoint 15 ms (2%) + reminder 0.09 ms (<0.1%). Per-key latency: ~13 ms / ~16 ms (p50/p99). The c2-xxiii batched-WAL lift (caveat ⁷) collapsed the broadcast component from ~880 ms p50 to ~99 ms; the c2-xxiv attribution (caveat ⁹) proved the remaining ~668 ms prepare contribution is the `LatticeGrain.SetManyAsync` fanout sub-stage, which c2-xxvi (campaign pause memo) showed is Azure-provider-throughput-bound and not addressable by further cross-leaf WAL coalescing. **Earlier revisions of this row reported p50=7,660 ms / p99=9,510 ms** based on the bench-only `lattice.op.duration_ms` histogram; that histogram measures the dispatcher's per-producer-batch latency, which slices each 256-entry batch into 4 sequential 64-key sagas and awaits them in a loop. The reported numbers were therefore *4 sequential sagas, not one*. The c2-xvii instrumentation campaign closes this attribution gap; the corrected numbers above are the actual per-single-saga cost. The p99 cell is dominated by the prepare-phase p99 (~1,025 ms); per-phase p99s do not simply sum because the phases are sequential and the joint tail tracks the slowest individual phase rather than the sum.

⁵ Phase D1c (sequence-number cursor for `ChangeFeed.Subscribe` + prepared-batch bypasses on the receiver/shipper HLC gates, commit pending on this branch): supersedes Phase D1b. D1b serialised per-shard `SetManyAsync` dispatch inside the saga to keep WAL-partition HLCs monotonic; D1c removes that constraint by recognising that the HLC monotonicity assumption was wrong everywhere it was used. The fix has three parts: (a) the public `IChangeFeed.Subscribe` cursor flips from `HybridLogicalClock` to a per-partition WAL offset (`ChangeFeedCursor`), which is monotonic by construction at the WAL grain; the legacy HLC overload remains for source-compat but its filter becomes a no-op; (b) the receiver-side `ReplicationApplier` bypasses both the per-origin HWM dedup AND the causal-park gate for `IsPrepared && AtomicBatchSize > 0` entries - cross-leaf prepared writes carry independent per-leaf HLCs that interleave non-monotonically on the shared WAL partition and were silently dropped by HLC dedup or deadlocked by causal-park (siblings' VC dependencies referred to each other's per-leaf clocks); (c) the producer-side `ReplicationShipperGrain` bypasses its own `state.Cursor` HLC filter for the same class of entries. With the gates closed at every level the saga can revert to D1's single parallel `lattice.SetManyAsync(slice)` call, restoring most of D1's sustained throughput while preserving D1b's correctness guarantees. Measured at this operating point: **SteadyAvg 216 → 268 keys/s (+24% over D1b, +32% over D1)**, FinalFailed=0 (every saga commits cleanly through cross-cluster replication). The c2-xi memo originally reported a 9.3s → 7.7s per-saga p50 improvement vs D1b but that comparison used the bench-only `lattice.op.duration_ms` histogram which measured 4 sequential sagas not one (see caveat ⁴); the relative improvement is real (D1b's serial per-shard dispatch is structurally slower than D1c's parallel call) but the absolute latency numbers were 4× too large. Regression coverage: 7 unit tests in `ReplicationApplierTests.PreparedNonMonotonicHlc.cs` pin the four bypass conditions against future regression. See `scaling.md` c2-xi memo for the full attribution.

⁶ Phase R1 (cheap revision probe replaces snap2 dictionary fetch on the LatticeGrain reader-side double-checked retry, commit pending on this branch): every `GetManyAsync` (and the `CountAsync` / `CountPerShardAsync` siblings) pre-Phase-R1 paid two `ITxRegistryGrain` grain RPCs - snap1 (dictionary, used to populate the ambient `LatticeRegistrySnapshotContext` for the fan-out) and snap2 (dictionary, compared against snap1 to detect an InFlight→Committed transition that raced the fan-out). R1 adds a monotonic `DecisionsRevision` counter on `TxRegistryState`, bumped under the registry's single-turn token on every observable Decisions mutation, and a `[AlwaysInterleave]` `GetDecisionsRevisionAsync` probe that returns just a `long` instead of the full dict. The post-fan-out stability check is now: probe the revision; if it equals the snap1 revision, snap1 is provably still authoritative and snap2 is elided entirely; on the rare mismatch, fall through to a real `SnapshotWithRevisionAsync` and the existing `IsSnapshotStable` rule. Multi-silo safe (the probe is still a grain RPC to the single-activation registry; the saving is the elided dict payload on the steady-state happy path, not the RPC turn itself). Measured at this operating point (rung `50000:5`, saturating offered load): A=148,462 keys/s (HEAD without R1) → B=**178,927 keys/s** (HEAD with R1), **+20.5%** sustained throughput, FinalFailed=0 on both runs. The `set-many-atomic` regression check at the c2-xi operating point (rung `500:10`) measured 278 keys/s vs c2-xi's 268 (+3.7%, within run-to-run variance and clearly not a regression). Regression coverage: 12 unit tests in `TxRegistryGrainTests.Revision.cs` pin the revision-bump contract + the `[AlwaysInterleave]` attribute against future regression. See `scaling.md` c2-xiv memo for the full attribution and operator-run provenance.

⁷ Phase c2-xxiii (saga terminal-mark WAL append lifted from per-shard to per-saga + batched, commit `fdeca66` on this branch): the saga's terminal broadcast previously executed N parallel `ShardRootGrain.AppendTxTerminalAsync` calls, each of which appended its own single-entry `WalRecord` to the WAL partition for that shard - so N shards produced N serialised single-entry partition transactions (8 shards per WAL partition under the c2-iii `WalPartitions=8` topology, kicking the first arrival into its own batch and parking the rest). The c2-xxii sub-stage attribution showed this WAL append was ~100% of the per-shard envelope (191ms p50). c2-xxiii changes `IShardRootGrain.AppendTxTerminalAsync` to return `Task<WalRecord?>` and adds a wire-compatible `inlineWalAppend: bool = true` parameter; the saga coordinator opts out, collects every touched-shard record from the parallel fan-out, and dispatches them through `ICommitLogWriter.AppendManyAsync` once - which groups by WAL partition and fans out one batched `IWalShardGrain.AppendBatchAsync` per partition in parallel. Direct callers (cross-cluster replay, shadow-forward, retroactive prepared-mutation sweep, unit tests) keep the historical inline shape via the default. Saga still awaits the batched write before returning so the WAL durability invariant (leaves rely on the WAL for cold-reactivation replay) is preserved. Measured at rung `500:10`: A=283 → B=**308 keys/s** (+9% throughput, within the rung's offered-load-bound variance band); the structural win surfaces on the latency histograms - `saga.broadcast.shard.stage.duration phase=wal` p50 **191ms → 0.00ms** (-100%, the saga path no longer touches the WAL on the shard), `saga.broadcast.shard.duration` p50 **191ms → 0.08ms** (-99.96%), saga-side `saga.broadcast.duration` p50 **~960ms → 96ms** (-90%), p99 **~2700ms → 307ms** (-85%). The bench throughput cell understates the win because rung 500:10 is offered-load-bound at 5,000/s; the latency collapse is the headline. See `scaling.md` c2-xxiii memo for the full attribution and the topology-safety audit.

⁸ Phase c2-xxvi re-baseline of `SetManyAsync` at the saturated rung `10000:5` post-c2-xxiii / post-c2-xxiv (`silo-20260528-112016Z.log`): SteadyAvg **13,574 entries/s** (previous c2-vii baseline was 9,758). The +39% throughput jump reflects the cumulative effect of every campaign optimisation through c2-xxiii landing on the same code path. The c2-xxiv sub-stage attribution localised the per-call envelope: gate / route / bucket / events sub-spans are all <8ms p50; the entire 2,012ms p50 / 2,854ms p99 lives in the **fanout** sub-stage (the cross-shard `Task.WhenAll`). The fanout cost decomposes through `shard_root.set_many.local_apply` (1,638ms p50 - max-of-N parallel leaves per shard) → `shard_root.set_many.leaf_rpc` (506ms p50 per leaf) → `wal.append.provider.duration` (138ms p50 per Azure round-trip) and `wal.append.in_flight` saturating at the cap of 8. The c2-xxv probe confirmed raising `WalMaxPendingBatches` to 16 makes this worse (Azure can serve at most ~8 concurrent partition transactions usefully; admitting more compounds the slow-tail wait). c2-xxvi confirmed the writer is already at its natural per-partition coalescing ceiling on this rung (`wal.append.batch_entries` p50 = 8, p90 = 100). The remaining cost is Azure-provider-throughput-bound: a single 4,096-entry batch hits 8 partitions × ~512 entries each, cutting into ~5-6 partition transactions per partition that serialise through the in-flight cap (~6 × 138ms = ~828ms accounts for the leaf_rpc p99 = 1,328ms). See `scaling.md` c2-xxiv / c2-xxv / c2-xxvi memos.

⁹ Layer-2 `SetManyAtomicAsync` throughput at rung `500:10` post-c2-xxiii shows a wide variance band of **308 - 623 keys/s** across 5 ladder runs in this session (samples: 308 / 623 / 468 / 462 / 462). The headline cell uses the arithmetic mean (**465 keys/s**) over those 5 samples; the variance is consistent with the offered-load shape (5,000 keys/s offered against a saga path whose per-saga p50 is ~800ms = ~1.25 sagas/s/saga-grain capacity), where the rung is offered-load-bound and the saga grain pool's activation count + idle-GC timing dominates the steady-state sample. Per the c2-xxvi memo, the next structural improvement target on this row would require either (a) higher Azure provisioning (parallel `WalShardGrain` activations against multiple storage accounts), or (b) a major durability-boundary refactor (relocate the WAL-before-Apply seam from the leaf to the shard) - both well beyond a single instrument-and-lift cycle. The c2-xxiii batched-WAL terminal lift remains the highest-leverage shipped optimisation on this row (-90% saga broadcast p50). Provenance: `silo-20260528-105751Z.log` (c2-xxiv sample), `silo-20260528-113221Z.log` (c2-xxvi sample), `.ladder-results-c2-xxvi-mode-set-many-atomic-sample5.csv`.

¹⁰ Layer-2 `SetAsync` per-call latency at the c2-iii operating point. Earlier revisions of this row reported `p50=~22 ms / p99=~37 ms` derived from `BenchMetrics.LatticeOpDurationMs(DispatchAsync) / 4096` under the `FlushConcurrency=8` fan-out assumption (caveat ¹). The c2-xxvii investigation added a direct `LatticeGrain.SetAsync` envelope histogram (`orleans.lattice.set.duration`) and proved the derivation was wrong: the dispatcher idles between producer batches, so `DispatchAsync` averaged across the full batch under-attributes the per-call cost by a factor of ~3. The current cells (`p50=~58 ms / p99=~300 ms`) are the honest measured envelope at `vehicles=10000, tickHz=5`. Sub-stage attribution at this rung: `set.stage phase=shard` p50 ~60 ms (entire envelope; gate / route / publish are <1 ms each); inside the shard call, `leaf.commit phase=wal` p50 ~36 ms decomposes as Azure provider round-trip ~16 ms + Orleans grain RPC overhead ~20 ms (the leaf↔WalShardGrain hop). The c2-xxviii leaf-side digest coalescing optimisation was originally credited with a -27% envelope p50 win on this row (79 ms → 58 ms), but the c2-xxix audit found the `LatticeOptionsResolver` was silently dropping the `DigestCoalescingWindowMs` field; coalescing therefore never fired on Azure, and the apparent win was misattribution. The latency cells in this row are the same with or without the resolver fix on the c2-iii `10000:5` rung because most leaves on this rung are flat-tree (single-leaf shards) and `PublishDigestUpwardAsync` early-returns without ever calling the parent. A reflective propagation guard (`LatticeOptionsResolverPropagationGuardTests`) now fails the build on any future `LatticeOptions` property added without an explicit propagation decision. Provenance: `silo-20260528-122140Z.log` (the c2-xxviii / c2-xxix sample at `10000:5`), `silo-20260528-144707Z.log` (`25000:5` cross-check), `silo-20260528-145728Z.log` (`1000:5` cross-check); see `scaling.md` c2-xxvii / c2-xxviii / c2-xxix memos for the full attribution chain.

**Layer-1 source**: BDN run `2026-05-27T09-02-19Z` on this branch (commit `b872262`), AMD Ryzen 7 PRO 7840U / 16 logical / 8 physical, .NET 10.0.8, in-process toolchain. P50 column shown above; mean and p99 are in the per-op detail table further down. "Single-thread throughput" is the derived `1 / p50`; treat it as the algorithmic ceiling, **not** as a multi-thread scaling claim. Multi-key rows (`GetManyAsync`, `SetManyAsync`, `SetManyAtomicAsync`) show both calls/sec and entries/sec - the entries/sec column is what the audience compares against per-key throughput of the point ops.

**Layer-2 source**: c2-vii probe-0 ladder + c2-viii step-9 per-mode probes (commits `b872262`, `7fd37a6`) at the c2-iii operating-point baseline (single silo, 32 shards, real Azure Tables Standard, West Europe). The `SetManyAsync` row uses the c2-iii baseline operating point (`vehicles=10000, tickHz=5, batchSize=4096`) and was re-baselined post-c2-xxiii / post-c2-xxiv on `silo-20260528-112016Z.log` (see caveat ⁸). The `SetManyAtomicAsync` row uses the tuned operating point described in caveat ³ and measures Phase c2-xxiii (commit `fdeca66` on this branch); the headline cell is the arithmetic mean of 5 ladder samples taken this session, range 308-623 keys/s, with per-saga phase decomposition from `silo-20260528-113221Z.log` (see caveats ⁴ and ⁹). The `GetManyAsync` row uses the saturating operating point described in caveat ³ and measures Phase R1 (commit pending on this branch; see caveat ⁶) on `silo-20260527-203734Z.log`.

### Layer-1 detail (full mean / p50 / p99 / allocated, per [BenchmarkDotNet](https://benchmarkdotnet.org) run)

All times in **milliseconds**; sub-millisecond values shown to 3 decimal places (or to 6 decimals for sub-microsecond cells). Allocated is bytes per call.

| Benchmark               | Mean (ms) | P50 (ms) | P99 (ms)  | Allocated (B) | Notes |
| ----------------------- | --------: | -------: | --------: | ------------: | ----- |
| `PointRead`             | 0.002 802 | 0.000 405 | 0.041 530 | 456          | single key, populated leaf |
| `PointReadWithVersion`  | 0.002 365 | 0.000 586 | 0.034 777 | 496          | returns `VersionedValue` |
| `PointWrite`            | 0.005 779 | 0.003 971 | 0.028 731 | 672          | single key, populated leaf |
| `PointGetMany`          | 0.011 016 | 0.008 062 | 0.052 687 | 6,968        | 16 keys/call against pre-populated leaf |
| `BulkLoad`              | 0.816     | 0.700    | 1.889     | 237,972       | 1,000 entries/call, single leaf |
| `SetMany_4Shards`       | 0.575     | 0.561    | 0.691     | 250,252       | 1,000 entries across 4 shards |
| `SetManyAtomic`         | 0.264     | 0.188    | 1.058     | 64,653        | 16 keys/saga, single shard |
| `SetManyAtomic_4Shards` | 0.162     | 0.157    | 0.226     | 97,979        | 16 keys/saga across 4 shards |
| `BulkLoad_DeepTree`     | not extracted | 0.033 309 | 0.124 526 | 15,968 | deep B+tree shape (height ≥ 2) |
| `BulkLoad_DeeperTree`   | 0.145     | 0.149    | 0.239     | 80,185        | 32 entries/call, deeper tree |
| `PointWrite_DeepTree`   | 0.006 516 | 0.004 171 | 0.037 676 | 1,592        | single key, deep tree |
| `PointWrite_DeeperTree` | 0.013 846 | 0.011 850 | 0.041 452 | 6,192        | single key, deeper tree |
| `PointRead_DeeperTree`  | 0.013 489 | 0.002 245 | 0.116 274 | 1,326        | single key, deeper tree |
| `PointReadAtomicTreeIdle` | 0.002 412 | 0.000 326 | 0.039 658 | 456        | read on a tree configured for atomic writes, no in-flight saga |
| `PointReadAtomicTreeWithActiveSaga` | 0.002 129 | 0.000 379 | 0.033 631 | 456 | read on the same tree with a concurrent saga in flight |
| `SetManyAtomic_Concurrent_1`  | 0.146 | 0.143 | 0.180  | 63,334       | 16 keys/saga × 1 concurrent saga |
| `SetManyAtomic_Concurrent_4`  | 0.559 | 0.588 | 0.958  | 246,951      | 16 keys/saga × 4 concurrent sagas |
| `SetManyAtomic_Concurrent_16` | 2.489 | 2.577 | 3.766  | 987,509      | 16 keys/saga × 16 concurrent sagas |
| `SetManyAtomic_Concurrent_64` | 10.737 | 11.969 | 14.213 | 3,949,740  | 16 keys/saga × 64 concurrent sagas |

Raw harness JSON: `benchmark/.run/microbench/2026-05-27T09-02-19Z/results.json` (values stored there are in nanoseconds; this table converts to milliseconds by dividing by 1,000,000).

**Rung context** (Layer 2): single silo, 32 shards, `WalPartitions=8`, `WalMaxPendingBatches=8`, `PhaseTwoCoalescingWindow=5ms`, `PipelinePhaseTwoCommits=true`, `FlushConcurrency=8`, `FlushMs=50`, `BatchSize=4096`, real Azure Tables Standard account (West Europe), producer rung `10000:5` (10,000 vehicle ids × 5 Hz tick = 50,000 events/sec target offered load, 245 B/event), 60-second producer window (`DurationSec=60`), `SteadyAvg` computed over the productive sub-window with drain tail excluded per the c2-vi-bench-timing-fix. Source commit `b872262`.

## Two-layer methodology

Two layers because they answer different questions; either alone misleads.

### Layer 1 - In-process microbench

**Source**: [benchmark/host/Bench.Microbench/LatticeMicroBenchmarks.cs](benchmark/host/Bench.Microbench/LatticeMicroBenchmarks.cs), driven by [BenchmarkDotNet](https://benchmarkdotnet.org). The harness instantiates the lattice's grain layer in-process against an in-memory storage provider; no Orleans RPC, no Azure I/O. BenchmarkDotNet runs O(10⁴–10⁶) iterations per op and computes mean / median / stddev / allocations with very tight error bars.

**Reading**: the algorithmic cost of one call to each `ILattice` method when scheduling and storage are free. The upper bound on what the implementation could theoretically sustain if every other layer were perfect.

**Units**:
- `Mean (ms/op)` / `P50 (ms/op)` / `P99 (ms/op)` - wall-clock time per call, in milliseconds. Sub-microsecond cells (e.g. `PointRead` p50 = 0.000 405 ms = 405 ns) keep 6 decimal places; sub-millisecond cells use 3 decimals.
- `Allocated (B/op)` - bytes of managed heap allocation per call. Lower is better for GC pressure under steady-state load.
- `Single-thread throughput (op/s)` - derived `1 / p50` after converting p50 to seconds. The number of calls one thread could complete per second if it ran the op back-to-back with no other work.

### Layer 2 - Azure-throughput bench

**Source**: [benchmark/azure-throughput/Silo/Program.cs](benchmark/azure-throughput/Silo/Program.cs) (silo) and [benchmark/azure-throughput/Producer/Program.cs](benchmark/azure-throughput/Producer/Program.cs) (producer), deployed via [benchmark/azure-throughput/scripts/40-ladder.ps1](benchmark/azure-throughput/scripts/40-ladder.ps1) to Azure Container Instances against a real Azure Tables Standard account. The producer streams vehicle telemetry over TCP; the silo's `TcpIngestService` batches events and dispatches them through `ILattice` against the c2-iii operating-point baseline.

**Reading**: end-to-end sustained throughput on a single silo at a realistic operating point with durable storage. The number to quote as "what the system does in production" on a single silo.

**Units**:
- `Sustained throughput (op/s)` - the productive-window steady-state average (drain tail excluded; the bench-timing fix described in the c2-vi-bench-timing-fix memo applies). For multi-key ops (`SetManyAsync`, `GetManyAsync`, `SetManyAtomicAsync`), one "op" is one batched call and the number of entries per batch is documented per row.
- `p50 / p99 (ms)` - per-call wall-clock latency at the 50th / 99th percentile from the `BenchMetrics.LatticeOpDurationMs` histogram over the productive window.

## How each op is dispatched (Layer 2)

The Layer-2 harness uses `BENCH_WORKLOAD_MODE` to select the dispatch shape; all five modes share the same producer event stream:

- **`set-many`** - every producer batch becomes one `ILattice.SetManyAsync(entries)` call. Default. Post-c2-xxiii / c2-xxiv re-baseline (see caveat ⁸): 13,574 entries/s; the legacy `12,708 op/s` headline from c2-vii is superseded.
- **`set-many-atomic`** - every producer batch is sliced into 64-key sub-batches; each sub-batch becomes one `ILattice.SetManyAtomicAsync(entries)` saga call. 64 is the realistic atomic-batch size; 4096-key sagas are not an audience-relevant shape.
- **`set-point`** - every entry in the batch becomes one `ILattice.SetAsync(key, value)` call; the silo fans these out in parallel up to `BENCH_FLUSH_CONCURRENCY` (8). The reported "op/s" is **point-writes per second**, not batches per second.
- **`get-point`** - silo pre-seeds `BENCH_VEHICLE_COUNT` keys via `BulkLoadAsync` at startup, then drives one `ILattice.GetAsync(key)` per producer-event; same parallel fan-out shape as `set-point`.
- **`get-many`** - same pre-seed as `get-point`; every producer batch's key list becomes one `ILattice.GetManyAsync(keys)` call.

The pre-seed for the read modes ensures Layer-2 read numbers exercise a populated tree (warm leaf cache, populated WAL, populated grain state). Cold-cache reads are out of scope for this report; a follow-up could add a "between-pre-seed-and-measurement deactivate" lever.

## Configuration baseline (Layer 2)

The c2-iii operating point, validated by the c2-vii probe-0 ladder ([scaling.md](scaling.md) U9p step c2-vii memo). This is the **full reproducible set** - every env-var pinned below is required to reproduce the headline numbers; defaults outside this table either don't affect throughput or are the deploy-script default at [commit b872262](benchmark/azure-throughput/scripts/20-build-and-deploy.ps1).

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

1. **Single silo.** Every Layer-2 number is from a single silo. The documented Azure Tables Standard-account ceiling is 20,000 entities/s per account; the `SetManyAsync` cell at `13,574 entries/s` (post-c2-xxvi re-baseline) is ~68% of that ceiling on a fresh ladder (the c2-vii session originally measured 12,708 entries/s, ~63%; the c2-xxvi probe confirmed that further improvement on this row is Azure-provider-throughput-bound, see caveat ⁸). A second silo with shard fan-out is the next campaign axis.
2. **Pre-fix-vs-post-fix.** Commit `b872262` fixed a leaf-grain etag race that, prior to today, was silently regressing throughput at higher rungs (25000:5 collapsed to 1,158/s; with the fix it reaches 12,044/s). Commit `7fd37a6` fixed the read-mode pre-seed (it now uses `SetManyAsync` instead of `BulkLoadAsync`, which rejected once any prior probe had populated the grain-state table). Numbers in this report are post-both-fixes; the chaos suite has not yet been run against either.
3. **Allocations only meaningful in Layer 1.** Layer 2 includes Orleans serialization, Azure SDK marshalling, and `System.Net.Http` allocations that are not part of the lattice's per-op cost surface. Layer-1 `Allocated (B/op)` is the right column for tracking the library's own GC pressure.
4. **Read-mode pre-seed.** The Layer-2 `GetAsync` and `GetManyAsync` rows were measured against a tree pre-populated with `BENCH_VEHICLE_COUNT` keys (10,000) of 245-byte payload, written via `lattice.SetManyAsync` at silo startup. Reads hit a warm leaf cache; cold-cache reads would be slower (a follow-up could add a "deactivate-between-pre-seed-and-measurement" lever).
5. **Atomic-write offered load.** The Layer-2 `SetManyAtomicAsync` row was the only one where the producer's offered load was deliberately lowered (`BENCH_VEHICLE_COUNT=500, BENCH_BATCH_SIZE=256` vs the others' `10000, 4096`). Without this adjustment, the producer saturated the saga path instantly and the silo never reached steady state (the pre-tuning probe measured a meaningless `SteadyAvg=17 keys/s` because almost the entire run was spent on a single first wave of `DispatchAsync` calls returning). The `203 keys/s` figure is the silo's actual sustained atomic-write capacity at the c2-iii operating point with Phase D1 fan-out enabled (see caveat ⁵).
6. **Per-op Layer-2 latency is derived.** The bench harness records latency at the `BenchWorkloadDispatcher.DispatchAsync` boundary, which wraps one full producer batch (4,096 entries for SetMany / GetMany / SetPoint / GetPoint, or 4 sagas × 64 keys for the tuned atomic probe). The headline table's per-op latency divides the dispatcher latency by the call count, which is accurate for the batched ops but only an average for the fan-out point ops (where individual per-key calls can be faster or slower). For exact per-op timing, a future harness change could record per-`ILattice` call directly.
7. **Mode-rung comparability.** All five rows show the silo at sustained steady-state, but they are not strictly equivalent operating points: `SetPoint` saturates at a much lower throughput than `SetMany` because per-key writes carry no batching amortisation. Treat the rows as "what the system delivers under realistic offered load for that op shape" rather than "what the system could deliver if everything else were idle."

## Provenance / artefacts

Layer 1: BDN JSON results at `benchmark/.run/microbench/2026-05-27T09-02-19Z/results.json` (gitignored). 20 benchmarks executed in 20m16s on a developer laptop (AMD Ryzen 7 PRO 7840U, .NET 10.0.8, BenchmarkDotNet 0.15.8 with the in-process toolchain).

Layer 2: per-mode CSVs under `benchmark/azure-throughput/scripts/`:

| Mode               | Results CSV                                              | Phase-A CSV                                              | Silo log                                              |
| ------------------ | -------------------------------------------------------- | -------------------------------------------------------- | ----------------------------------------------------- |
| `set-many`         | `.ladder-results-c2-vii-mode-set-many.csv`               | `.ladder-phaseA-c2-vii-mode-set-many.csv`                | `benchmark/azure-throughput/.run/silo-20260527-100411Z.log` |
| `set-point`        | `.ladder-results-c2-vii-mode-set-point.csv`              | `.ladder-phaseA-c2-vii-mode-set-point.csv`               | `silo-20260527-100754Z.log`                           |
| `get-point`        | `.ladder-results-c2-vii-mode-get-point.csv`              | `.ladder-phaseA-c2-vii-mode-get-point.csv`               | `silo-20260527-101629Z.log`                           |
| `get-many`         | `.ladder-results-c2-vii-mode-get-many.csv`               | `.ladder-phaseA-c2-vii-mode-get-many.csv`                | `silo-20260527-102106Z.log`                           |
| `set-many-atomic` (Phase D1c, commit pending on this branch)        | `.ladder-results-c2-xi-mode-set-many-atomic-D1c.csv`           | `.run/silo-20260527-143915Z.log` (silo-side `phaseA` lattice.op.duration_ms histogram) | `silo-20260527-143915Z.log` |

All `benchmark/.run/silo-*.log` files are gitignored (large; multi-MiB per run). The CSVs are gitignored as well (`.ladder-*` pattern in `benchmark/azure-throughput/scripts/.gitignore`), but their contents are pinned in the headline tables above.
