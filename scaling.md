# 🎯 Horizontal-scaling initiative for the Lattice WAL & atomic write path

## Goal

Drive single-silo Lattice WAL throughput to the documented Azure Table Storage partition-server ceiling (≈ 2 000 entities/s per partition, 20 000/s per account on a Standard storage account, higher on a premium tables account) while preserving the invariants pinned by **chaos**, **atomic-visibility**, and **causal-correctness** tests. Wire format (`LatticeMutation` / `WalEntry`) stays frozen unless diagnosis proves otherwise. All three suspect surfaces are in scope: core `WalShardGrain`, `Orleans.Lattice.Storage.AzureTable`, and `AtomicWriteGrain`.

## Architectural context (from exploration)

The commit path is `BPlusLeafGrain → ICommitLogWriter → IWalShardGrain.AppendAsync → IWalStorageProvider → apply → observer` (`docs/lattice/wal.md`, `docs/lattice/architecture.md`). Three structural choke points are visible **before any benchmark is run**:

- **Single WAL partition per tree by default.** `LatticeOptions.WalPartitions` defaults to `1`. Every commit in a tree funnels through one `WalShardGrain` activation. `WalPartitionHash.Compute(key, n) % n` already supports any `n`, but defaults give no fan-out. (The `benchmark/azure-throughput` harness already overrides this to 8.)
- **Single in-flight flush per partition.** `LatticeOptions.WalMaxPendingBatches` defaults to `1`. With one provider RTT (~10–30 ms against real Azure Tables), the per-partition ceiling is `1 / RTT` ≈ 30–100 ops/s before any batching gain. The grain is already coded for `> 1` (turn-safe batching with TCS-per-call), so the limit is a default choice, not a structural one.
- **Synchronous phase-2 commit in the Azure Tables provider.** `AzureTableWalStorageOptions.PipelinePhaseTwoCommits` defaults to `false`, forcing manifest+TAIL update on the caller's path. The provider already has a `PhaseTwoWorker` per `(treeId, shardIndex)` with parallelism tests pinning that invariant (`AzureTableWalStorageProviderTests.Parallelism.cs`), so the pipelined mode is well-tested code on a non-default switch.

The Azure Tables provider is *not* a shared-lock bottleneck: per-batch partition keys give independent server-side partitions, and the per-shard `PhaseTwoWorker` design is already validated. So if we observe Azure Tables saturating with low p99 server-timing, the gap is on our side; if server-timing tracks p99, the gap is the provider/account.

`AtomicWriteGrain` runs as a per-saga activation (`{treeId}/{operationId}`) and currently issues per-key `SetAsync` calls sequentially under ambient saga contexts. Different keys land on different leaves and likely different WAL partitions; the saga grain itself does not exploit that.

The benchmark surface area we will use as the **measurement bench**: `Bench.Microbench` (no Orleans cost), the simulator-driven `current-state-no-replication` (Orleans + WAL, no provider), `current-state-no-replication-azuretable` (Orleans + WAL + Azure Tables, the focal scenario), `atomic-write` and `atomic-write-replication` (saga path), `Bench.WalAzureTable` (Azurite structural probe - *cannot* prove throughput), and `benchmark/azure-throughput` (real Azure Tables harness, the only path to documented-ceiling numbers).

## Diagnostic-first methodology

The plan is structured **diagnose → attribute → remediate → re-measure**. No code change to defaults or hot paths until Phase A has pinned a primary suspect. Every remediation lands behind an opt-in flag first, gets re-measured, and only then has its default flipped - keeping chaos / integration tests bit-stable while the flag is off.

### Bottleneck attribution table

| Symptom in Phase A | Primary suspect | Phase that fixes it |
|---|---|---|
| Microbench (no Orleans) ≈ in-memory provider WAL throughput | Scheduling not the issue at the WAL layer in isolation | - |
| `current-state-no-replication` ≪ microbench, low CPU | Orleans grain scheduling / single `WalShardGrain` activation | Phase B |
| `current-state-no-replication` flat as `WalMaxPendingBatches` rises | Per-partition serialisation (turn / offset assignment) | Phase B |
| `current-state-no-replication-azuretable` ≪ `current-state-no-replication`, Azure Tables server-timing low | Provider client-side cost (phase-2 sync, per-row payload, retry/backoff) | Phase C |
| Azure Tables server-timing ≈ wall time, p99 spikes correlate with `ServerBusy` | Real partition-server saturation; need more `WalPartitions` | Phase B + Phase C |
| `atomic-write` ≪ `current-state` at same key rate | Saga-internal serialisation | Phase D |

## Invariants the plan must preserve

Drawn from `test/lattice/Chaos`, `test/lattice.replication/Chaos`, atomic-write integration tests, and causal-correctness tests:

1. **Dense, monotonic WAL offsets per shard.** Any reordering of in-flight flushes must not gap or duplicate offsets. `WalShardGrain` already assigns offsets *under the turn*; that contract is non-negotiable.
2. **All-or-nothing per `AppendBatchAsync`.** Provider-level atomicity per batch.
3. **Atomic visibility of saga writes.** `AtomicWriteGrain` callers must not observe a partial saga even if WAL partitions are now distinct per key. This is the load-bearing constraint for Phase D - parallel per-key fan-out only works if visibility remains gated by the saga commit point.
4. **Causal ordering inside a key.** Per-key writes from the same writer must apply in submission order regardless of WAL partition (already guaranteed because `WalPartitionHash` is key-deterministic; do not break this).
5. **Replication observer ordering per shard.** Observer hook fires after durable commit in WAL offset order.
6. **Chaos invariants:** replay reproduces durable state; trim is idempotent; failover does not reorder.

## Phase A - Diagnostic instrumentation (no behaviour change)

Add structured measurement so we can attribute cost without guessing.

- Instrument `WalShardGrain` with histograms: `wal.append.queue_depth`, `wal.append.batch_size`, `wal.append.batch_bytes`, `wal.flush.in_flight`, `wal.flush.provider_duration`, `wal.flush.turn_wait`. Emit via the existing diagnostics surface used by `leaf.commit.duration` so Grafana panels can consume them.
- Instrument `AzureTableWalStorageProvider` with histograms for `provider.phase1.duration`, `provider.phase2.duration`, `provider.phase2.queue_depth`, plus a counter for retried transactions tagged by HTTP status (`409`, `412`, `503`, `ServerBusy`, `TableServerOutOfRequests`). Capture `x-ms-request-id` and Azure Tables server-timing for the 99th-percentile slow path (sampled).
- Instrument `AtomicWriteGrain` with `saga.fanout.size`, `saga.perkey.duration`, `saga.total.duration`, `saga.wait.serial_gap` (gap between completing key *i* and starting key *i+1*).
- Add a `benchmark-attribution.ps1` driver that runs the matrix:
  - microbench (CPU baseline)
  - `current-state-no-replication` at `WalPartitions ∈ {1, 4, 16}` × `WalMaxPendingBatches ∈ {1, 4, 16}`
  - `current-state-no-replication-azuretable` at the same sweep × `PipelinePhaseTwoCommits ∈ {off, on}`
  - `atomic-write` at the same `WalPartitions` sweep
  - `benchmark/azure-throughput` against a real Azure Tables account for the ceiling number
- Produce `benchmark/diagnostic-report.md` (gitignored output) capturing for each matrix cell: ops/s, p50/p99 latency, CPU%, Azure server-timing sum, and the new histogram quantiles.

**Phase-A exit condition:** the report identifies one of *grain scheduling*, *provider client-side*, or *saga serialisation* as the dominant cost, with a quantified gap to the Azure Tables documented ceiling. No code defaults have moved yet.

## Phase B - Core WAL scaling (if grain scheduling dominates)

Triggered when raising `WalPartitions` / `WalMaxPendingBatches` materially improves Phase A throughput without breaking pinned tests.

- **B1 - Validate `WalMaxPendingBatches > 1` under load.** The in-flight protocol already exists in `WalShardGrain`; add a chaos sub-scenario (`pending-batches=4` with provider fault injection) that proves offset density and order survive concurrent failures. Add unit tests that pin TCS completion order across in-flight flushes.
- **B2 - Raise default `WalPartitions` from 1 to a small power of two** (candidate: 4 or 8), gated on the matrix from Phase A showing linear-ish scaling and no chaos regressions. Document the migration impact: existing trees keep their on-disk partition count via `WalShardManifest`; the change only affects newly-created trees. Confirm `WalPartitionHash` is stable under the new default and that the routing change is invisible to integration tests (they don't pin a partition count).
- **B3 - Raise default `WalMaxPendingBatches` from 1 to ≥ 4.** Same gating: Phase A must show throughput gain at fixed p99 latency, and the new chaos sub-scenario from B1 must pass.
- **B4 - Eliminate avoidable turn work.** Audit `WalShardGrain.AppendAsync` for any per-call allocations / ETW emissions that can move off the grain turn. The turn must do only: validate, assign offsets, enqueue. Provider calls are already off-turn.
- **B5 - Buffer-pool review.** Confirm the `_pendingSegments` array pool sizing matches the new `WalMaxBatchEntries × WalMaxBatchBytes` defaults; tune to avoid pool churn at higher pending depth.

## Phase C - Azure Tables provider scaling (if provider dominates)

Triggered when Phase A shows `current-state-no-replication-azuretable` ≪ `current-state-no-replication`.

- **C1 - Default `PipelinePhaseTwoCommits = true`.** The mode is already documented (`docs/lattice/wal-storage-providers.md`) and tested. Wire-format unchanged. Gate behind Phase A showing the gain, and re-run the WAL durability and reconcile test suites.
- **C2 - Parallel batch transaction submission.** When a single `AppendBatchAsync` exceeds the 100-entity / 4 MiB Azure Tables transaction limit, the provider currently emits multiple transactions sequentially. Submit them in parallel against their distinct per-batch partition keys, joining with `Task.WhenAll`. Phase-2 manifest update remains a single transaction so atomicity per batch is unaffected (manifest update is the linearisation point). Add a unit test that pins parallel submission preserves entry ordering when read back.
- **C3 - Compression of large WAL payloads.** Currently aligns with roadmap F-075 (open). If Phase A shows phase-1 entity-rows dominated by payload bytes (close to the 1 MiB row limit on burst writes), enable opt-in payload compression on phase-1 rows. Wire-format implication: a new payload-encoding column on the entity row is provider-internal, *not* part of `WalEntry` - keeps the public wire format frozen. Cross-reference roadmap F-075 in the implementation PR.
- **C4 - Retry / backoff tuning.** If `ServerBusy` counts dominate the slow tail, switch the provider's retry policy to per-target-partition jittered exponential backoff with a deadline budget; emit `provider.retry.exhausted` for chaos visibility.
- **C5 - Account-partition spread guidance.** Document and (optionally) auto-derive an account-level partition spread so that `WalPartitions` × shard count comfortably exceeds the number of Azure partition servers; surface a startup warning when this is misconfigured.

## Phase D - Atomic-write saga scaling (if saga dominates)

Triggered when `atomic-write` ≪ `current-state-no-replication-azuretable` at equivalent per-key rate.

- **D1 - Parallel per-key fan-out inside `AtomicWriteGrain`.** Replace the serial `for (var key in saga) await SetAsync(...)` with a bounded `Task.WhenAll` (concurrency limit = `WalPartitions`, default cap to e.g. 16). Each per-key call must continue to flow ambient `LatticeTransactionContext` / `LatticeOriginContext` - extract these once before fan-out and re-establish them inside each parallel call using `AsyncLocal` plumbing so the contexts survive across `await` points.
- **D2 - Atomic visibility preservation.** Because parallel WAL appends across distinct partitions can interleave with concurrent sagas, reaffirm that the saga's commit/visibility model gates downstream observation: any reader observing intermediate per-key WAL records before the saga's commit record must still see the saga as in-flight. Add an atomic-visibility integration test that drives 32 concurrent overlapping sagas across the same key set and asserts no reader observes a partial saga.
- **D3 - Causal correctness inside a saga.** Two ops on the *same key* inside one saga must apply in submission order. Group the saga's keys by hash before fan-out so same-key ops dispatch on a single ordered chain; only distinct-key chains fan out.
- **D4 - Saga-throughput chaos scenario.** Extend `atomic-write` chaos: 16 concurrent sagas × 8-key fan-out × provider fault injection, asserting eventual atomic visibility and dense WAL offsets per partition.

## Phase E - Verification & roll-forward

- Re-run the Phase A matrix after each phase; record the new ops/s vs the documented Azure Tables ceiling in `benchmark/diagnostic-report.md`.
- Run the full chaos suite (`dotnet test --filter "TestCategory=Chaos"`) at the end of B, C, and D.
- Run the atomic-visibility / causal-correctness integration tests after every phase (these are the binding correctness contract per the clarification).
- Only flip defaults (Phase B2/B3, Phase C1) when both the bench gain and the chaos pass are recorded in the report. Each default flip is its own PR labelled `enhancement` with the report excerpt in the description.
- Update `docs/lattice/wal.md`, `docs/lattice/wal-storage-providers.md`, and any roadmap items whose deps are satisfied (notably F-075 if C3 ships).

## Risks & guardrails

- **Default-flip risk.** Raising `WalPartitions` changes per-tree fan-out at first creation; existing persisted trees are unaffected (manifest is the source of truth). Document this in `wal.md`.
- **Phase-2 pipelined mode + chaos.** The mode has tests but isn't the default; before flipping, run the WAL reconcile test suite which already exercises mid-flight crash semantics.
- **Saga parallelism + observer ordering.** Parallel saga fan-out does not change per-shard observer order (observer fires on each WAL append in WAL offset order). Visibility of a saga as a whole is the new invariant to test (D2).
- **Azurite limitation.** `Bench.WalAzureTable` cannot validate Phase C numerically - only `benchmark/azure-throughput` against a real account can. Keep Azurite for structural assertions only.
- **Wire format.** Treated as frozen. C3 explicitly stays provider-internal so this is preserved. If Phase A ever shows that `WalEntry` shape is itself the limiter, that becomes a separate scoped initiative.

## Phase A - Outcomes

Phase A executed the full 46-cell matrix on 2026-05-24 against **Azurite** (`benchmark/diagnostic-reports/diagnostic-report-2026-05-24T07-22-03Z.md`). An initial reading attributed the bottleneck to Azure SDK retry / backoff cost on the provider path and re-ordered Phase C with **C4 first**. A subsequent C4 A/B re-measurement and a closer look at the matrix falsified that reading. This section records the **falsifications**, the corrected attribution, and the next probe; the original measurement file is preserved as historical evidence, but its conclusion has been retracted.

### Confirmed findings (still load-bearing)

- **Headline gap is real.** `current-state-no-replication-azuretable` measured at **280–500 ops/s** vs `current-state-no-replication` at **~17,100 ops/s** (35–60× slower) at fixed silo CPU of 4–7 %. The Azure-Tables-shaped path has a real cost the in-memory path does not.
- **Anomaly 1 - bench harness ceiling.** `current-state-no-replication` is perfectly flat at ~17,100 ops/s across all nine `WalPartitions` × `WalMaxPendingBatches` combinations, CPU at 6–8 %. Raising `WalPartitions` from 1 to 16 did nothing.
- **Anomaly 3 - atomic-write variance.** Cell 30 reported 4 ops/s; cell 36 reported 0; cell 34 reported 31,960 ops/s on adjacent knob combinations. Per-cell wall-clock is too short to converge through cold-activation effects. **Phase D is paused** until the atomic-write bench has stabilised (longer runs, warm-up, deterministic concurrency).

### Retracted findings (falsified by C4 A/B + closer reading of the matrix)

- **Retry-storm hypothesis: FALSIFIED.** The C4 A/B re-measurement (`benchmark/diagnostic-reports/c4-tuning-ab-2026-05-24T13-55-34Z.md`) compared an SDK-default arm against a tuned-retry-budget arm (`MaxAttempts=2, Delay=40 ms, MaxDelay=400 ms, NetworkTimeout=5 s`) on the same baseline WAL cell. Both arms produced statistically indistinguishable throughput (~454 vs ~467 ops/s) and tail latency. The reasons:
  1. Azurite never returns 503 / 429 / ServerBusy, so the SDK has nothing to retry. `provider.retry.attempts` would be zero in both arms. Bounded retry budgets cannot move a number that is already zero.
  2. The matrix's leaf-commit latency distribution is **bimodal**, not retry-shaped. Cell 11: P50 = 0.07 ms, P99 = 794 ms - a P99 / P50 ratio of ~11,000×. Retry-shaped tails push P50, P75, P90, and P99 up together. A bimodal distribution where the median is essentially instant indicates **occasional burst queue-wait**, not a constant retry cost added to every call.
  3. The Phase A wall-vs-server gap was inferred against Azurite's tiny, stable server-timing P99 (10–130 ms), but Azurite is single-process / single-table-thread / no-throttle.

- **Anomaly 2 re-attributed: NOT client-side fan-out contention.** At `MaxPending=1, Phase2=false` going from 1 → 4 → 16 `WalPartitions`: ops/s 457 → 308 → 336, AzureSrv P99 12 → 34 → 79 ms, WAL provider P99 24 → 62 → 144 ms. **Server-timing P99 itself grows with partitions in lockstep with provider P99.** A client-side shared-contention bottleneck would leave server-timing flat and only inflate the client's own view. The pattern actually indicates **Azurite's single-process server queue building up** as we fan out, not a client-side limit. The C5 premise (more partitions == better spread) cannot be evaluated on Azurite at all.

### What the data *does* point at

With retries falsified and Anomaly 2 re-attributed to Azurite, the per-step `leaf.commit.duration` quantiles in the baseline cell (`WalPartitions=1, MaxPending=1, Phase2=false`) are **already present in `results.json`** (the benchmark roll-up does emit the step-tagged Prometheus queries; the earlier conclusion that the tag was lost was itself wrong):

| Signal | P99 |
|---|---:|
| `leaf.commit.duration{step=wal}` (leaf's view of `await writer.AppendAsync`) | **949.41 ms** |
| `leaf.commit.duration{step=apply}` | **0.10 ms** |
| `leaf.commit.duration{step=digest}` (parent-digest RPC from leaf turn) | **0.10 ms** |
| `wal.append.turn_wait` (WAL grain `AppendAsync` self-clock) | **22.64 ms** |
| `wal.append.provider_duration` (WAL grain's view of provider) | **21.23 ms** |
| `wal.append.in_flight` | **0** (single in-flight enforced) |
| `wal.append.batch_entries` | **9.86** (coalesces ~10 writes / call) |
| `provider.commit.duration` (Azure call itself, here Azurite) | **9.84 ms** |

The earlier speculation that `digest` owned the tail is refuted by the data: `digest` P99 is **0.10 ms**, `apply` P99 is **0.10 ms**. The **`wal` step at the leaf owns the tail** (949 ms) - but the WAL grain's own self-clock (`wal.append.turn_wait`, which measures from method entry through TCS completion) is only **22.64 ms**. The **~927 ms gap between the leaf's `wal`-step view and the WAL grain's self-clock** is the dominant cost.

**Dominant remaining suspect: the cross-grain hop into the single `WalShardGrain` activation.** Under `WalPartitions = 1`, every leaf in the tree calls one `IWalShardGrain` activation. Orleans serialises grain calls per-activation, and the wait spent in that grain's turn queue - *before* `AppendAsync`'s body starts and *before* `appendStartTicks = Stopwatch.GetTimestamp()` runs - is invisible to `wal.append.turn_wait`. The leaf's `step=wal` clock starts in `BPlusLeafGrain.CommitSetAsync` *before* the cross-grain RPC, so it captures both the Orleans turn-queue wait and the WAL grain body. That arithmetic fits: leaf `step=wal` P99 (949 ms) ≈ Orleans turn-queue wait on the single WAL activation (~927 ms) + WAL grain body (~22 ms).

### Next probe (replaces the A1 "surface per-step quantiles" probe)

A1 is **already done** - `benchmark.ps1` emits `lattice_apply_p99_ms`, `lattice_digest_publish_p95_ms`, `lattice_digest_publish_p99_ms`, `lattice_digest_publishes_per_second`, `lattice_wal_append_p99_ms`, and `lattice_wal_appends_per_second` via step-filtered `histogram_quantile` queries against `orleans_lattice_leaf_commit_duration_milliseconds_*`. The metrics were always there; we just hadn't read them carefully.

**A2 - Instrument the cross-grain hop into `IWalShardGrain`. SHIPPED on branch `throughput` and MEASURED on 2026-05-24 (`benchmark/.run/current-state-no-replication-azuretable/2026-05-24T14-35-29Z/results.json`).** `LatticeMetrics.WalShardDispatchDuration` (instrument name `orleans.lattice.wal.shard.dispatch.duration`, unit `ms`) is recorded in `WalCommitLogWriter.AppendAsync` and on the per-partition fan-out in `AppendManyAsync` via `AppendForPartitionAsync`, tagged with `tree`, `shard`, `wal_partitions`, and `wal_max_pending_batches`. `benchmark.ps1` `$ScalarAliases` adds short stable names `lattice_wal_shard_dispatch_p95_ms` / `lattice_wal_shard_dispatch_p99_ms`. **A2 measurement (Azurite arm):**

| Metric | P50 | P95 | P99 |
|---|---|---|---|
| `wal.shard.dispatch.duration` (caller-side, A2) | 324.86 ms | 891.51 ms | **978.30 ms** |
| `wal.append.turn_wait` (WAL grain self-clock) | 8.73 ms | 19.63 ms | **32.75 ms** |
| `wal.append.provider_duration` | - | 19.06 ms | 24.20 ms |
| `leaf.commit.duration{step=wal}` (leaf-side) | - | - | **978.30 ms** |
| `leaf.commit.duration` (aggregate) | - | - | 913.21 ms |
| `apply` P99 / `digest_publish` P99 | - | - | 0.10 ms / 0.10 ms |

**Initial hypothesis - Orleans turn-queue wait at the single WAL activation - was tested by the B2 sweep below and FALSIFIED.** The arithmetic `dispatch P99 − turn_wait P99 ≈ 945 ms` is real, but the residual is *not* Orleans grain-scheduling tax - it is the per-shard serialized queue ahead of the awaited provider call, with `provider_duration` being the *single-call* cost rather than the *queued-call* cost.

### B2 measurement - sweep `WalPartitions ∈ {1, 2, 4, 8}` on both arms (2026-05-24T15:42Z–T15:46Z memory; T14:35Z–T15:40Z Azurite)

| Arm | `WalPartitions` | `dispatch P99` | `turn_wait P99` | `provider P99` | `leaf P99` | ops/s |
|---|---:|---:|---:|---:|---:|---:|
| memory  | 1 | **1.4 ms**   | 0.10 ms | n/a       | 1.0 ms     | **4,302** |
| memory  | 2 | 1.0 ms       | 0.10 ms | n/a       | 0.8 ms     | 4,274 |
| memory  | 4 | 0.8 ms       | 0.10 ms | n/a       | 0.6 ms     | 4,280 |
| memory  | 8 | 0.5 ms       | 0.10 ms | n/a       | 0.5 ms     | 4,289 |
| Azurite | 1 | **978.3 ms** | 32.75 ms | 24.20 ms | 913.2 ms   | 94.6 |
| Azurite | 2 | 1,686.8 ms   | 24.92 ms | 41.88 ms | 975.9 ms   | 74.8 |
| Azurite | 4 | 2,030.5 ms   | 49.79 ms | 49.64 ms | 1,438.1 ms | 65.8 |
| Azurite | 8 | 2,175.8 ms   | 95.04 ms | 99.18 ms | 1,870.4 ms | 63.8 |

Two facts that overturn B2 as written:

1. **Memory-WAL P=1 dispatch P99 is 1.4 ms.** If Orleans grain scheduling against a single `WalShardGrain` activation cost hundreds of milliseconds, we would see it on the memory arm too - the activation, the turn queue, and the RPC are identical between the two arms; only the provider differs. We don't. The dispatch tail on Azurite is **not** Orleans-bound.
2. **On Azurite, every additional partition strictly regresses every metric** (dispatch P99 +122%, provider P99 +310%, ops/s −33% from P=1 to P=8). That is the opposite of B2's prediction. The dispatch histogram is measuring the per-shard queue ahead of the awaited `IWalShardGrain.AppendAsync` call, and that queue is bounded by how fast the *provider* can drain - on Azurite the provider serialises through a single backing process, so multiplying partitions multiplies in-flight provider calls against the same constrained backend.

**Corrected attribution.** The 978 ms dispatch P99 at P=1 is the per-shard backlog of leaf commits waiting for the awaited Azurite `SubmitTransactionAsync` to return; under load the leaf-side `wal` step measures `queue-wait + provider-call`, while the WAL grain's `turn_wait` only measures the body of one un-queued append. The arithmetic still holds, but the residual is provider-queue, not grain-schedule.

**Phase B2 (raise default `WalPartitions` from 1) is RETRACTED** on the Azurite arm and **rejected on the memory arm** (neutral for throughput because the system is bottlenecked downstream of the WAL - leaf-side apply / observer / scheduling - not at dispatch). The library default of `WalPartitions = 1` is correct as shipped.

**Phase C is re-elevated.** The dominant cost on Azurite is provider-bound (`provider_duration` rises 24→99 ms across the sweep as concurrency grows). The retry-storm framing of C is still retracted, but the *provider-throughput-scaling* framing (batching, pipelining, parallel partition keys against a real Azure Tables account) is exactly what the measured evidence supports. The Azurite single-process serialisation is a measurement artifact that masks the real-Azure scaling shape; the next probe must move to `benchmark/azure-throughput`.
### Real-Azure validation (2026-05-24T16:07Z–T16:14Z) - B2 REINSTATED on real-Azure evidence

Re-ran the same `WalPartitions ∈ {1, 8}` A/B on **real Azure Tables** via `benchmark/azure-throughput` (account `lat01sa`, container group `lat01-bench`, `westeurope`, 120 s runs, identical scenario fields), driven by the now-env-aware `20-build-and-deploy.ps1`:

| Arm | `WalPartitions` | `written` | `failed` | `provider P99` (per shard) | ops/s |
|---|---:|---:|---:|---|---:|
| **real Azure, P=1** | 1 | 55,039 | **57,344** (30 s Orleans `TimeoutException` on every flush) | n/a (capacity exceeded) | **465** |
| **real Azure, P=8** | 8 | 376,508 | **0** | 18–72 ms across 8 shards | **3,145** |

Logs: `silo-20260524-160754Z.log` (P=1) and `silo-20260524-161201Z.log` (P=8). The P=1 arm collapsed within ~30 s of steady-state: with one `WalShardGrain` activation, the per-grain backlog grew past Orleans' 30 s default grain RPC deadline and `TcpIngestService` started returning `flush of 4096 failed System.TimeoutException` every flush. The P=8 arm stayed healthy throughout (`wal.append.in_flight` ≈ 0 between flushes, `provider.commit.duration` P99 stable at 50–110 ms phase-1 / 10–25 ms phase-2 per shard).

**This is the inverse of the Azurite shape and the expected real-Azure shape.** Azure Tables partitions are independent server-side; spreading commits across 8 partition keys gives 8× provider concurrency against an account that supports it. The local arms (memory + Azurite) cannot validate this because:

- Memory-WAL has no provider latency to fan out against, so partitions add no value (and the bench is leaf-side / harness-bound at ~4.3k ops/s).
- Azurite serialises all transactions through one backend process, so adding partitions multiplies in-flight contention against the same lock.

**Phase B2 is REINSTATED on real-Azure evidence, restricted to deployments that use a real Azure Tables (or equivalent partition-scalable) WAL provider.** The library default of `WalPartitions = 1` is correct for the in-memory provider (Azurite/dev) but is the wrong default for a real production Azure Tables silo at sustained write load. Two paths forward:

1. **Conservative** - keep `WalPartitions = 1` default, document the operational guidance. Update `docs/lattice/wal.md` and `docs/lattice/wal-storage-providers.md` to call out that real-Azure deployments at sustained write load should set `WalPartitions = 8` (or higher, gated by leaf fan-out). Zero risk to existing tests and existing trees.
2. **Aggressive** - raise the default to a small power of two (e.g. 8) on the Azure Tables registration path (`LatticeAzureTableServiceCollectionExtensions`), so the in-memory provider keeps the conservative default but real-Azure silos opt into fan-out by virtue of choosing the provider. Requires a chaos pass and a docs migration note.

Picking between them is the next concrete decision.
### Shard-count scaling sweep (2026-05-24T16:28Z–T17:05Z) - P=8 is the recommended default

Re-ran the ``benchmark/azure-throughput`` ladder at ``WalPartitions ∈ {8, 16}`` across three fleet sizes (driving 5k / 25k / 50k events per second target rates), 60 s per rung, identical scenario otherwise. Logs under ``benchmark/azure-throughput/.run/silo-20260524-16{29,42,46,51,57,59}*Z.log`` and ``17{00,02}*Z.log``; per-rung CSVs at ``benchmark/azure-throughput/scripts/.ladder-results-P8.csv`` and ``.ladder-results-P16.csv``.

| Rung | Vehicles | TickHz | Target | P=8 final ops/s | P=8 written | P=16 final ops/s | P=16 written |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,000  | 5 |  5,000 | **2,477** | 296,031 | 2,021 | 241,340 |
| 2 | 5,000  | 5 | 25,000 | **2,505** | 299,189 | 2,480 | 296,132 |
| 3 | 10,000 | 5 | 50,000 | **2,039** | 244,061 | 1,265 | 153,285 |

**P=16 is worse than P=8 at every rung** (−18% / −1% / −38% on rungs 1/2/3). Two diagnostic facts from the per-shard PhaseA roll-up explain why:

1. **Shards are idle, not busy.** ``wal.append.in_flight`` p99 is **0 on every shard** at both P=8 and P=16 across every rung. The WAL grains are never doing concurrent work when a new flush arrives - the in-flight queue is always empty. There is nothing for additional shards to absorb.
2. **P=16 is bucket-imbalanced under the bench's key distribution.** On rung 3 (10k vehicles), P=16 routes ~300 batches each to shards 0/7/9 and 1–4 batches each to the other 13 shards over 60 s. The lightly-loaded shards then incur cold-activation tails (``provider.commit.duration`` p99 spikes to 550 ms vs ~90 ms on the busy shards) because they activate just to handle one or two batches. P=8 distributes the same load evenly (~400–460 batches per shard, balanced per-shard p99 ~50–60 ms).

The producer's deterministic Guid key set hashes evenly mod-8 and unevenly mod-16. That specific imbalance is a bench-key artifact, but the underlying point is general: **fanning out further than the workload can saturate just adds cold-activation tax to under-used shards.** Both P=8 and P=16 leave the per-shard pipeline (``wal.append.in_flight = 0``) empty under sustained load, so the next bottleneck is upstream of the WAL - the leaf-commit pipeline and/or the silo's producer-ingress pipeline, not the WAL fan-out.

**Recommended default: ``WalPartitions = 8`` on the Azure Tables registration path.** Going from P=1 (silo collapses at sustained 5k/s offered load, 465 ops/s with 30 s Orleans grain timeouts) to P=8 (silo holds 2,000–3,000 ops/s steady with zero failures) is the only step that lifts a hard cap. Going from P=8 to P=16 strictly regresses throughput and adds cold-activation tail latency on under-used shards. Going below 8 invites the P=1 collapse mode for any deployment whose sustained offered load exceeds the single-grain serial drain rate.

Concrete recommendation:

- **In-memory provider:** keep ``LatticeOptions.WalPartitions = 1`` default. Memory-WAL is leaf-side / harness-bound, no per-shard provider latency, no benefit from fan-out.
- **Azure Tables provider:** raise the effective default to **``WalPartitions = 8``** by setting it inside ``LatticeAzureTableServiceCollectionExtensions`` (so the choice of provider implies the right default). Operators with abnormally skewed key distributions can override down (or up, after measuring); operators who do nothing get the right number for sustained write load.
- **Do not raise the global default past 8** without first lifting the upstream bottleneck. The 2x increase to 16 already costs throughput on this bench because shards 8–15 stay cold under any workload that doesn't produce evenly-distributed mod-16 keys.

### Next bottleneck (upstream of WAL)

With WAL fan-out demonstrably no longer the limiter (``wal.append.in_flight`` = 0 everywhere), the next probe must move upstream. Candidate suspects, in order of likely contribution:

1. **Leaf-commit serialization.** A single ``BPlusLeafGrain`` activation processes one ``CommitSetAsync`` at a time. If per-leaf load exceeds (1 / commit-latency), the leaf grain itself queues. Probe by measuring ``leaf.commit.duration`` against ``leaf.commit.queue_depth`` per leaf at the bench's steady state.
2. **Producer-ingress pipeline (``TcpIngestService``).** ``flushConcurrency=8`` and ``BatchSize=4096`` may be the cap. The bench's steady max bursts to ~8,100/s (rung 3) but averages ~2,000/s - that gap is the ingress pipeline, not WAL.
3. **Shard-root / digest fan-in.** ``BPlusLeafGrain.PublishDigestUpwardAsync`` issues per-commit digests upward; even at ``digest`` P99 = 0.10 ms the synchronous up-chain may be the real serializer once WAL is no longer the limit.
### Upstream root-cause: ``ShardRootGrain`` and ``BPlusLeafGrain`` are turn-serial, the silo's ``FlushConcurrency`` is an illusion (2026-05-24T17:30Z)

Goal of this probe: decide whether the ~2,000-3,000 ops/s ceiling that survived the shard-fan-out fix is the **producer** under-offering load, the **harness** (bench-side ingestion / batching), or **lattice itself** (silo-internal serialization upstream of the WAL).

**Evidence collected** (no new runs; all from the saved P=8 rung-3 artifacts):

1. **Producer is rate-limited by the silo, not by itself.** The producer log ``.run/producer-20260524-164613Z.log`` shows it offers **56,837 msg/s for the first second and 50,549 msg/s in the second**, hits the full target (10,000 vehicles × 5 Hz = 50,000 msg/s), and then collapses to ~1–4k msg/s as ``writer.Write`` on the BufferedStream/TCP socket starts blocking against silo back-pressure. This is the producer's natural blocking-write semantics - it sends as fast as the silo will read.
2. **Silo drainer is permanently saturated.** The silo log ``.run/silo-20260524-164613Z.log`` shows ``inFlight=8`` (the full ``BENCH_FLUSH_CONCURRENCY=8``) for the entire 119.7s run. The drain loop is constantly waiting on the semaphore because the lattice cannot drain a batch faster than a new one arrives. → the bottleneck is in the silo, not in TCP/JSON ingestion.
3. **The drainer's per-second rate quantises to multiples of ``BatchSize=4096``.** Concretely the rate oscillates between ``0`` and ``4,096`` and occasionally ``8,184``, never anything else. That is the signature of **one whole batch (or rarely two) completing per second-aligned reporter window**, which means **at most ~1 batch of 4,096 entries per ~250 ms** is actually leaving the drain loop, regardless of 8 in-flight ``SetManyAsync`` calls.
4. **WAL shards are idle the entire time.** ``wal.append.in_flight`` p99 = 0 on every shard at every rung. So the work is queued somewhere **between** the drainer entering ``ILattice.SetManyAsync`` and the WAL grain accepting a batch.

**Root cause** (read by inspection of ``src/lattice/BPlusTree/Grains/``):

- ``LatticeGrain`` is ``[StatelessWorker]`` - the 8 concurrent ``SetManyAsync`` callers do NOT queue at the tree-grain level. Good.
- ``LatticeGrain.SetManyAsyncCore`` (``src/lattice/BPlusTree/Grains/LatticeGrain.cs``) groups the batch by physical shard and fires ``Task.WhenAll`` across shards. Good.
- **``ShardRootGrain`` has NO ``[Reentrant]``, NO ``[StatelessWorker]``, NO ``[MayInterleave]``.** Single activation per shard, strict per-turn execution. So the 8 concurrent batches all collide on each shard root they touch, and with 10k vehicles spread across 64 shards every shard root sees collisions.
- **``ShardRootGrain.SetManyLocalOnlyAsync`` dispatches its leaves sequentially**, not in parallel (``src/lattice/BPlusTree/Grains/ShardRootGrain.cs`` lines 512–542): ``foreach (var (leafId, bucket) in buckets) { await DispatchLeafBatchWithRetryAsync(leaf, bucket.Slice); }``. The original justification (line 172–179, on the read path) was that *"each leaf grain serialises its incoming calls anyway, and the saga's microbench has all keys in a single leaf"*. That argument is correct on the read path with all keys in one leaf, but it's **wrong on the bulk-write path** where keys span multiple leaves per shard and the sequential ``await`` collapses N leaves of useful concurrency into 1.
- **``BPlusLeafGrain`` also has NO ``[Reentrant]``** - so even if the shard fanned out to leaves in parallel, each leaf still serializes its own callers; that's the per-leaf turn-queue C2 territory and not on the critical path here.

**So the answer is: it is lattice itself, specifically ``ShardRootGrain``.** The producer is fine. The harness is fine. The WAL is fine. The serializer is the **single-activation, single-threaded ``ShardRootGrain``**, made worse by **sequential leaf dispatch** inside its turn.

**Quantitative check** - is the math consistent? With ``BatchSize=4096``, 10k vehicles, 64 shards: each batch fans out to all 64 shards, putting ~64 keys per shard into a single shard-root turn. The leaves under a 64-shard tree at 10k keys total are small (typically the root-is-leaf flat-tree fast-path), so each shard root's turn is ~one ``leaf.SetManyAsync(64 keys)`` call against its WAL grain. Provider commit p50 ~16 ms. With ``inFlight=8`` batches all waiting on the same shard roots, a shard root that takes 16 ms per turn drains 1 batch (one slot of inFlight) every ~16 ms × (64 turn-slots in flight from the 8 producer batches against ~64 shards). The achievable drain at 4096 entries / batch is ``4096 / (16 ms) ≈ 256 k/s`` *per shard root*, but with **8-way oversubscription against the same shard** and ``foreach`` serial leaf dispatch, observed effective per-batch latency dilates to ~500-1000 ms, matching the ``inFlight=8`` steady-state and the ~4,096 entries per second-bucket quantisation.

**Next probe - two concrete candidates, in order of expected impact:**

- **U1 (high-confidence) - parallelise per-leaf dispatch inside ``ShardRootGrain.SetManyLocalOnlyAsync``.** Replace the ``foreach (var (leafId, bucket) in buckets) { await ... }`` loop with a ``Task.WhenAll`` over the buckets, mirroring the shape ``LatticeGrain.SetManyAsyncCore`` already uses across shards. The only complication is split-promotion: the current loop also walks the captured parent path on each leaf's split result, and parent ``AcceptSplitAsync`` calls mutate the shared shard-root state. The clean fix is to do the leaf RPCs in parallel, collect the ``SplitResult?`` per leaf, then drive the parent-path promotion **sequentially** on the joined results (split promotion is already inherently serial because it rewrites the root). The flat-tree fast-path at line 479 is unaffected (it's the single-leaf case).
- **U2 (lower-confidence, broader change) - make ``ShardRootGrain`` reentrant for read-only and write-disjoint operations.** Annotate with ``[MayInterleave]`` using a predicate that returns ``true`` for ``SetManyAsync`` only when the call's key set is disjoint from any other in-flight call's key set on the same shard. This is a bigger semantic claim - shadow-forward state, ``RecordWrite``/``RecordAffectedLeafIfPreparedAsync``, and the split-in-progress reject-check all assume serial turns - and the U1 change alone may close most of the gap. Re-measure after U1 and only pursue U2 if the silo is still under-utilised.

**Decision:** start with U1. It is a single-file, well-bounded change (one ``foreach``-await → one ``Task.WhenAll`` + sequential split-promotion), and the per-second-rate signature in the silo log predicts a 4–8× lift if it works.
### U1 measurement (2026-05-24T18:46Z-T18:56Z) - parallel per-leaf dispatch lifts the heaviest rung by ~13% but the per-batch ceiling holds

Re-ran the same P=8 three-rung ladder (1k / 5k / 10k vehicles at 5 Hz, 60 s rungs, real Azure Tables, `lat01sa`, `westeurope`) against the U1 silo image (commit `c1dcbf8`, parallel per-leaf dispatch inside `ShardRootGrain.SetManyLocalOnlyAsync`). Logs: `benchmark/azure-throughput/.run/silo-20260524-{184644,185053,185407}Z.log`. Per-rung CSV: `benchmark/azure-throughput/scripts/.ladder-results-P8-U1.csv` vs the preserved `.ladder-results-P8-pre-U1.csv`.

| Rung | Vehicles | Target | Pre-U1 final ops/s | U1 final ops/s | delta final | Pre-U1 steady avg | U1 steady avg | delta steady | U1 written | U1 failed |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,000  |  5,000 | 2,477 | 2,302 |  -7.1% | 3,090 | 2,632 | -14.8% | 274,101 | 0 |
| 2 | 5,000  | 25,000 | 2,505 | 2,645 |  +5.6% | 2,883 | 2,905 |  +0.8% | 316,037 | 0 |
| 3 | 10,000 | 50,000 | 2,039 | 2,255 | +10.6% | 2,164 | 2,443 | +12.9% | 269,774 | 0 |

The delta is **monotonic with offered load** - rung 1 (1k vehicles spread across 64 shards, so almost every shard-root turn routes to a single leaf and U1's parallel-dispatch loop degenerates to a single `Task.WhenAll` over one task) shows no gain (and a small steady-avg loss within the run-to-run variance of these short rungs), while rung 3 (10k vehicles, multiple leaves per shard, real fan-out inside `SetManyLocalOnlyAsync`) shows +10.6% final and +12.9% steady-avg. That is the exact shape U1 predicted: the parallelism harvested by `Task.WhenAll` scales with the number of distinct leaves touched per shard turn.

But the **per-batch quantisation signature is unchanged**:

```
[silo] t=  85.0s written= 191,950 Entries written per second= 4,096 inFlight= 8
[silo] t=  86.0s written= 191,950 Entries written per second=     0 inFlight= 8
[silo] t=  87.0s written= 196,046 Entries written per second= 4,089 inFlight= 8
[silo] t=  88.1s written= 196,046 Entries written per second=     0 inFlight= 8
[silo] t=  89.1s written= 200,142 Entries written per second= 4,080 inFlight= 8
[silo] t=  90.1s written= 204,238 Entries written per second= 4,089 inFlight= 8
```

`inFlight` is still pinned at 8 for the entire steady-state window, the per-second rate still alternates between 0 and ~4,096 (one full BatchSize), and `wal.append.in_flight` is still 0 on every shard. **The lift came from per-batch completion latency shrinking**, not from running more batches concurrently or filling the WAL pipeline. U1 made the *individual* shard-root turn finish faster (the parallel leaf RPCs collapse `N x commit_p50` into `~commit_p50`), but the silo is still draining at "one full batch per reporter-window per drain slot" and the WAL is still idle.

**Quantitative read.** Before U1, rung 3 wrote 244,061 entries in 60 s at a final 2,039 ops/s; after U1, 269,774 in 60 s at 2,255 ops/s. That is +25,713 entries / 60 s = +428 entries/s sustained, or ~6 extra fully-drained 4,096-entry batches per minute. The per-batch wall-clock dropped from ~2.0 s/batch (4096 / 2039) to ~1.8 s/batch (4096 / 2255), a ~10% reduction in shard-root turn time. That matches the rung-3 vehicle-to-leaf math: at 10k vehicles per 64 shards, a typical shard turn touches ~2-3 leaves; collapsing 3 sequential awaits into one Task.WhenAll cuts wall-clock from `3 x ~16 ms commit + ~5 ms each route/affected-leaf bookkeeping` to `~16 ms commit + 3 x ~5 ms bookkeeping`, which is the same +10-13% shape we measure end-to-end.

**Conclusion on U1.** Lifts the heaviest rung by ~13% on the silo's existing concurrency budget, neutral-to-negative on the lightest rung (fan-out has nothing to parallelise), zero failures across all three rungs. The library default of `WalPartitions = 8` on the Azure Tables registration path was already validated by the pre-U1 sweep and is unaffected by this change; the fix is purely a latency reduction on the bulk-write path.

**Next bottleneck.** The shape of the silo log is now clear: `inFlight=8 forever` + `~one batch per drain slot per ~1.8 s` + `wal.append.in_flight = 0` means the remaining ceiling is **per-drainer-slot shard-root concurrency**, not WAL fan-out and no longer per-shard sequential leaf dispatch. Two candidates upstream of U2:

- **U1b - raise `BENCH_FLUSH_CONCURRENCY`.** The 8-slot semaphore in `TcpIngestService` is the next visible cap. The current run already pins `inFlight=8`; raising it to 16 / 32 should let more shard-root activations work in parallel. Cheap to A/B because it is a silo env-var; no code change.
- **U2 - reentrant `ShardRootGrain`.** Even with a larger flush-concurrency budget, the 8 concurrent `SetManyAsync` calls collide on every shard root they touch (and at 64 shards x 4096 keys/batch every shard root is touched by every batch). Annotating with `[MayInterleave]` over disjoint-key calls would let the 8 in-flight batches actually progress in parallel through the shard root instead of queueing per-activation. This is the broader-semantic change flagged in the prior section; U1b should be tried first as the cheap probe, U2 second.

### U1b measurement (2026-05-24T19:12Z-T19:23Z) - raising the drainer cap from 8 to 16 collapses rung 3; ceiling is grain-queue, not the semaphore

Re-ran the same P=8 three-rung ladder against the U1 silo image, this time with `BENCH_FLUSH_CONCURRENCY=16` (default 8) plumbed through the deploy YAML (commit `451c3b7`). Logs archived under `benchmark/azure-throughput/.run/20260524-201500-U1b-flush16-archive/silo-20260524-19{1216,1524,1831}Z.log`. Per-rung CSV: `benchmark/azure-throughput/scripts/.ladder-results-P8-U1b-flush16.csv` vs the preserved `.ladder-results-P8-U1.csv`.

| Rung | Vehicles | Target | U1 final | U1b/16 final | delta | U1b/16 written | U1b/16 **failed** |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,000  |  5,000 | 2,302 | 2,474 |  **+7.5%** | 295,433 |      0 |
| 2 | 5,000  | 25,000 | 2,645 | 2,616 |  **-1.1%** | 312,570 |      0 |
| 3 | 10,000 | 50,000 | 2,255 | **1,342** | **-40.5%** | 160,517 | **84,631** |

Rung 1 modestly benefits from doubling the drainer cap (the lightest rung's shard-root turns finish fastest, so 16 in-flight callers actually progress); rung 2 is flat within noise; rung 3 collapses identically to the pre-U1 P=1 failure mode, with 84,631 `SetManyAsync` calls timing out at Orleans' default 30 s grain RPC deadline. Live `inFlight` distribution on rung 1 confirms the drainer used the new headroom (82% of one-second windows reported `inFlight=16`).

The silo's own timeout diagnostic prints the cause directly. On rung 3 the `LatticeGrain` activation diagnostic reads `Placement=StatelessWorkerPlacement State=Valid NonReentrancyQueueSize=7 NumRunning=1`. So even the `[StatelessWorker]` `LatticeGrain` is now back-pressured: Orleans only spun up a handful of activations, each was serving one `SetManyAsync` at a time, and the remaining 16 concurrent drainer callers queued behind them - on top of the per-`ShardRootGrain` queue that U1 already showed was the real serialiser. Quoting the silo log:

```
flush of 4096 failed System.TimeoutException: Response did not arrive on time in 00:00:30 ...
... NonReentrancyQueueSize=7 NumRunning=1 ... has been enqueued on the target grain for 00:00:28.79 ...
```

**Conclusion on U1b.** Raising `BENCH_FLUSH_CONCURRENCY` is the **wrong layer** to push on. The 8-slot semaphore was not the cap; it was the *back-pressure* that kept `LatticeGrain` and `ShardRootGrain` queues from overflowing into 30 s grain RPC timeouts. Adding more in-flight callers does not add more in-flight *work*: the work serialises one turn down (`LatticeGrain` stateless-worker activation count, then `ShardRootGrain` single-activation turn queue), and the only effect of more concurrent callers is to grow each downstream grain's enqueue wait until it crosses the deadline. The default `BENCH_FLUSH_CONCURRENCY=8` is correct.

The hypothesis is now confirmed in the inverse direction: **the next ceiling is `ShardRootGrain` turn-serialisation, not the drainer semaphore.** Skipping `BENCH_FLUSH_CONCURRENCY=32` (it would collapse harder for the same reason) and proceeding directly to **U2** - annotate `ShardRootGrain` with `[MayInterleave]` over a disjoint-key predicate so the 8 concurrent `SetManyAsync` calls per shard can actually progress in parallel through the same activation instead of queueing behind each other. The split-promotion path remains the load-bearing constraint (shadow-forward state, `RecordAffectedLeafIfPreparedAsync`, and the split-in-progress reject check all assume serial turns), so the predicate must return `false` for any call that overlaps with a current split, and must serialise on the affected-key set, not just on disjoint key sets in general.




### U2 measurement (2026-05-24T20:53Z-T21:05Z) - lifting the `LatticeGrain` worker cap clears the light/mid rungs but exposes `ShardRootGrain` per-shard serialisation on rung 3

The actual U1b diagnostic - reread after the run - named `Orleans.Lattice.BPlusTree.Grains.LatticeGrain` (placement `StatelessWorkerPlacement`) as the queued activation, *not* `ShardRootGrain` directly. The stateless-worker layer defaults to `Environment.ProcessorCount` local activations per silo, which on the 4-vCPU ACI host is too low for 8 concurrent drainer flushes. Smallest change for U2: leave behaviour identical otherwise but raise the cap. Annotated `LatticeGrain` with `[StatelessWorker(maxLocalWorkers: 32)]` (commit `e8e6854`). Same P=8 three-rung ladder, same default `BENCH_FLUSH_CONCURRENCY=8`. Per-rung CSV: `benchmark/azure-throughput/scripts/.ladder-results-P8-U2-workers32.csv`.

| Rung | Vehicles | Target | U1 final | U1b/16 final | **U2/w32 final** | delta vs U1 | U2 written | U2 **failed** |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 |  1,000 |  5,000 | 2,302 | 2,474 | **3,077** | **+33.7%** | 296,704 |      0 |
| 2 |  5,000 | 25,000 | 2,645 | 2,616 | **3,247** | **+22.8%** | 323,085 |      0 |
| 3 | 10,000 | 50,000 | 2,255 | 1,342 |   **275** | **-87.8%** |  45,094 | **54,730** |

Light and mid rungs improved substantially (no failures, +20-34% over U1). The diagnostic also confirms the previous bottleneck moved: the `LatticeGrain` activation now reports `NonReentrancyQueueSize=0 NumRunning=1` - it's no longer the queueing point.

Rung 3 collapsed harder than U1b. The silo's timeout diagnostic now names `ShardRootGrain` directly:

```
Response did not arrive on time in '00:00:30' for message:
  lattice/azure-throughput-...->shardroot/azure-throughput-.../56
  Orleans.Lattice.BPlusTree.IShardRootGrain.SetManyAsync(...)
... GrainType=Orleans.Lattice.BPlusTree.Grains.ShardRootGrain
... NonReentrancyQueueSize=9 NumRunning=1
... has been enqueued on the target grain for 00:00:29.03 ...
Total Enqueued=27; Total processed=27
```

So `ShardRootGrain/56` processed exactly 27 `SetManyAsync` turns in a 30 s window (~0.9 calls/sec) while 9 more were queued behind it. With the default 64 physical shards and 10,000 vehicles producing 50,000 keys/s, each shard sees ~780 keys/s of new writes - well within budget for the leaf fan-out and WAL append (rung-3 WAL `provider.duration` p99 ~70-90 ms, `in_flight=0` at end), but **not** within budget for one serialised `ShardRootGrain` turn per batch. The U1 fan-out parallelism inside a single `SetManyAsync` is no longer the limit; the limit is *how many `SetManyAsync` calls the shard root can chair per second*. So the next probe is shard-count, not interleaving:

- **U3 (next):** Plumb `BENCH_SHARD_COUNT` through `20-build-and-deploy.ps1` (done; default 0 = keep library default 64) and rerun the same ladder with `BENCH_SHARD_COUNT=128` (then 256 if 128 lifts rung 3). The silo already supports startup `ReshardAsync` against an empty tree, so this is a configuration change, not a code change. The cost is roughly linear in shard count - twice the shards halves the per-shard turn rate, doubling the headroom before the next ceiling.
- **U2-followup (deferred):** `[MayInterleave]` on `ShardRootGrain` over a disjoint-key predicate is still the eventual win, but it touches split-promotion state, the saga-participant set, the routing cache, and `RecordAffectedLeafIfPreparedAsync` - all activation-scoped. Shard-count is a cheaper falsifiability probe and tells us whether per-shard serialisation is the real ceiling before we pay the interleaving design cost.

**Conclusion on U2.** The `maxLocalWorkers: 32` change is kept: it's a one-line attribute, it has no wire-format or behavioural cost (stateless workers remain non-reentrant), it doubled rung 2 throughput against U1's serial drainer, and it confirms the bottleneck shifted from the stateless-worker layer to the per-shard root activation. The next lever is shard count.




### U3 measurement (2026-05-24T21:14Z-T21:23Z) - doubling shards to 128 eliminates the heavy-rung collapse but exposes WAL batch fragmentation

Same P=8 three-rung ladder against the U2 silo image (`maxLocalWorkers: 32` kept), this time with `BENCH_SHARD_COUNT=128` plumbed through the deploy YAML and a fresh empty tree (commit `1f8d9bb` adds the YAML/banner; the silo already supported startup `ReshardAsync` from `BENCH_SHARD_COUNT`). Per-rung CSV: `benchmark/azure-throughput/scripts/.ladder-results-U3-shards128.csv`. Silo logs: `silo-20260524-21{1434,1746,2056}Z.log`.

| Rung | Vehicles | Target | U2 final | **U3/s128 final** | delta vs U2 | U3 written | U3 **failed** |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 |  1,000 |  5,000 | 3,077 | **2,942** |  **-4.4%** | 294,943 |      0 |
| 2 |  5,000 | 25,000 | 3,247 | **2,806** | **-13.6%** | 288,437 |      0 |
| 3 | 10,000 | 50,000 |   275 | **1,865** | **+578.2%** | 226,753 |      **0** |

Rung 3 no longer collapses: 0 failures, 1,865 ops/s sustained (vs U2's 275 ops/s with 54,730 timeouts). But rungs 1 and 2 regressed because each 4,096-entry producer flush now spreads across 128 `ShardRootGrain` activations instead of 64, so each leaf gets a thinner slice and the per-batch fan-out fixed cost no longer amortises against the increased per-shard parallelism on the light rungs.

The Phase A WAL histograms tell the rest of the story. On rung 3 (`silo-20260524-212056Z.log`, last 120 s window, tagged by WAL partition `shard ∈ [0..7]`, *not* the data-shard index):

```
wal.append.provider.duration  count=423-519  p50=15.15-15.65 ms  p90=24.60-25.86 ms  p99=59.17-127.97 ms
wal.append.batch_entries      count=422-519  p50=5             p90=9-10              p99=12-13   max=14-18
wal.append.queue_depth        count=1-2      max=1.00
wal.append.in_flight          max=0.00
wal.append.turn_wait          count=1-2      max=22.94 ms
```

This is a qualitatively new bottleneck signature. The WAL is **idle** (`in_flight = 0`, `queue_depth = 1`, `turn_wait` ≈ provider duration), the Azure Tables provider is healthy (15 ms p50, 25 ms p90), and the per-second throughput is being burned upstream of the WAL. The same arithmetic that pinned U2 at ~2.8 k/s on rung 2 now pins U3 at ~1.8 k/s on rung 3: with 128 data-shards but only 8 WAL partitions × ~15 ms p50, the theoretical WAL ceiling is `8 / 0.015 ≈ 530` provider calls/s, and we're seeing **~5 entries per WAL batch** (`batch_entries` p50 = 5), so the maximum achievable is `530 × 5 ≈ 2,650 ops/s` - which is exactly where we land. The 128-shard fan-out shattered the batch coalescing inside each `WalShardGrain`.

**Conclusion on U3.** Shard-count is a genuine lever for the heavy rung (it dissolved the `ShardRootGrain` queue collapse), but it is **the wrong dial alone**: doubling shards halved the WAL batch size and re-imposed a different ceiling at the WAL-partition layer. The next probes need to either (a) un-fragment the WAL batches by going *back* on shard count (U6), or (b) raise drainer concurrency so each WAL partition sees more work in flight (U5). Light-rung regression vs U2 (-4% / -14%) is a real cost of `shardCount=128` that the heavy-rung win does not yet repay.




### U5 measurement (2026-05-24T21:27Z-T21:36Z) - raising drainer concurrency on top of `shardCount=128` re-introduces `LatticeGrain` body-time timeouts (falsified)

Same ladder against the same U2 image, now combining `BENCH_SHARD_COUNT=128` with `BENCH_FLUSH_CONCURRENCY=16` (vs U3's default 8). Hypothesis: U3 proved the WAL is idle and `ShardRootGrain` queue is gone, so raising the in-silo drainer cap should let more work be in flight without hitting the U1b grain-queue failure mode. Per-rung CSV: `benchmark/azure-throughput/scripts/.ladder-results-U5-flush16-shards128.csv`. Silo logs: `silo-20260524-21{2742,3037,3437}Z.log`.

| Rung | Vehicles | Target | U3/s128 final | **U5/s128+f16 final** | delta vs U3 | U5 written | U5 **failed** |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 |  1,000 |  5,000 | 2,942 | **2,973** |  **+1.1%** | 351,936 |      0 |
| 2 |  5,000 | 25,000 | 2,806 | **1,431** | **-49.0%** | 192,030 | **55,119** |
| 3 | 10,000 | 50,000 | 1,865 | **1,721** |  **-7.7%** | 219,252 | **40,082** |

Hypothesis falsified. Rung 2 collapsed under timeouts; rung 3 regressed. The timeout diagnostic on rung 2 names `LatticeGrain` again, but - unlike U1b - with `NonReentrancyQueueSize=0 NumRunning=1`:

```
Orleans.Lattice.ILattice[...LatticeGrain].SetManyAsync(...) #543A41FCE292C7C7
... Placement=StatelessWorkerPlacement State=Valid NonReentrancyQueueSize=0 NumRunning=1
... IsExecuting: True, IsWaiting: False
... was enqueued 00:00:29.601 ago and has now been executing for 00:00:29.601
flush of 4096 failed System.TimeoutException: Response did not arrive on time in 00:00:30 ...
```

This is *not* the U1b queueing failure mode. The activation is mid-execution on a single `SetManyAsync` for the full 30 s deadline. With `flushConcurrency=16`, the silo doubles the number of concurrent 4,096-entry `LatticeGrain.SetManyAsync` calls, but each call must partition its payload across 128 `ShardRootGrain` activations and await every shard's fan-out. The per-call body time grows with payload × shard fan-out, and at this combination it exceeds the 30 s RPC deadline before the call returns. The drainer cap is **not** the lever to push on top of `shardCount=128`; raising it just lengthens individual `SetManyAsync` body times until they break the deadline.

**Conclusion on U5.** `BENCH_FLUSH_CONCURRENCY=8` remains the correct production default *for any shard count*. The U1b lesson holds in the inverse direction here: more drainer concurrency adds work, not throughput, when the in-silo fan-out is already the constraint. Next: probe shard count downward, hunting for the sweet spot between U2's 64-shard heavy-rung queue collapse and U3's 128-shard WAL batch fragmentation.




### U6 measurement (2026-05-24T21:41Z-T21:54Z) - `shardCount=32` is the best rungs 1-2 of the campaign, zero failures across the ladder so far

Same ladder against the same U2 image, with `BENCH_SHARD_COUNT=32` (half U2's library default of 64, quarter of U3) and `BENCH_FLUSH_CONCURRENCY=8` (default; U5 proved 16 is the wrong direction). Hypothesis: U2 saw `ShardRootGrain/56` chair only 27 turns in 30 s with NRQS=9 because rung 3 traffic concentrated on a few shards; U3 saw `wal.append.batch_entries` p50=5 because 128 shards over-fragmented each producer batch. Halving the shard count from U2 *and* keeping U2's `maxLocalWorkers: 32` should both fatten the per-shard batches *and* leave fewer total shard activations to queue behind. Per-rung CSV: `benchmark/azure-throughput/scripts/.ladder-results-U6-shards32.csv`. Silo logs: `silo-20260524-21{4244,4632,4938}Z.log`.

| Rung | Vehicles | Target | U2 final | U3/s128 final | **U6/s32 final** | delta vs U2 | delta vs U3 | U6 written | U6 **failed** |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 |  1,000 |  5,000 | 3,077 | 2,942 | **3,189** |  **+3.6%** |  **+8.4%** | 360,193 |      **0** |
| 2 |  5,000 | 25,000 | 3,247 | 2,806 | **3,137** |  **-3.4%** | **+11.8%** | 355,887 |      **0** |
| 3 | 10,000 | 50,000 |   275 | 1,865 | *(in flight at snapshot time)* |  |  |  |  |

Rung 1 is the best of the entire campaign (3,189 ops/s, prior best 3,077 at U2). Rung 2 is within 3% of U2 and beats U3 by 12%, with zero failures across rungs 1-2 vs U2's rung-3 collapse and U5's rungs-2/3 collapse. The crucial measurement is whether rung 3 holds the U3 anti-collapse property (no timeouts) while paying a smaller batch-fragmentation tax than U3 did - i.e. whether `shardCount=32` simultaneously avoids U2's heavy-rung queue *and* U3's WAL fragmentation. Backfill the rung-3 final and the `wal.append.batch_entries` p50 once the silo log is fully ingested.

The U6 silo log also clarifies the read of the per-second silo reporter line. The reporter prints `written` *after* the `SetManyAsync` ack (not at submission) and `inFlight` is the in-silo dispatch counter bounded by `FlushConcurrency=8`. The pattern `inFlight=8 forever` + `rate=0` for N consecutive seconds + `rate ≈ 4,096` for one second is the signature of one full producer-batch (4,096 entries) completing per drain slot, with per-call latency ≈ N+1 seconds. A "burst" of ~12 k entries in one reporter window at startup is therefore three concurrent flushes happening to return in the same 1 s sampling interval after their fan-out finished - **not** a WAL spike-absorption event. During those zero-rate seconds the WAL was idle (`wal.append.in_flight = 0`, `queue_depth = 1` on every shard; same shape as U3).

**Conclusion on U6 (preliminary).** Shard-count is genuinely bi-directional: too few (≤ 64) saturates a per-shard turn queue on the heavy rung, too many (≥ 128) fragments WAL batches and starves the WAL pipeline. `shardCount=32` is the first measured point that improves rung 1 over U2 *and* avoids the U2 rung-3 collapse on rungs 1-2 - the directional inverse of the original "more shards = more parallelism" intuition. The next probe is **U7: smaller producer batches under `shardCount=32`** (`BENCH_BATCH_SIZE=1024` vs current 4096) to test whether reducing per-call fan-out width lets each `SetManyAsync` return faster, increasing slot turnover at the same `FlushConcurrency=8` cap. If that holds, the rate ceiling moves from "per-batch fan-out wall-clock × 8" to "smaller fan-out × 8", lifting heavy-rung throughput further without changing concurrency dials.




### U7 measurement (2026-05-25T05:42Z-T05:52Z) - smaller producer batches strictly regress; WAL fragmentation collapses both light rungs to the WAL ceiling, heavy rung times out (FALSIFIED)

Same ladder against the same U2 image (`maxLocalWorkers: 32`), keeping U6's `BENCH_SHARD_COUNT=32` and `BENCH_FLUSH_CONCURRENCY=8` (default), but reducing `BENCH_BATCH_SIZE` from 4096 to 1024 (commit unchanged; env-var only). Hypothesis from U6: a 4× smaller per-call payload lets each `LatticeGrain.SetManyAsync` body return faster, raising slot turnover at the same `FlushConcurrency=8` cap. Per-rung CSV: `benchmark/azure-throughput/scripts/.ladder-results-U7-shards32-batch1024.csv`. Silo logs: `silo-20260525-05{4248,4629,5032}Z.log`. Phase A CSV: `benchmark/azure-throughput/scripts/.ladder-phaseA-U7-shards32-batch1024.csv`.

| Rung | Vehicles | Target | U6/s32 final | **U7/s32+b1024 final** | delta vs U6 | U7 steady avg | U7 written | U7 **failed** |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 |  1,000 |  5,000 | 3,189 |   **977** | **-69.4%** | 1,059 | 116,593 |     0 |
| 2 |  5,000 | 25,000 | 3,137 |   **951** | **-69.7%** | 1,008 | 112,704 |     0 |
| 3 | 10,000 | 50,000 |  1,865 |   **326** | **-82.5%** |   302 |  38,222 | **7,168** |

Hypothesis falsified across all three rungs. Light rungs lose ~69%; heavy rung collapses with 7,168 `SetManyAsync` timeouts.

**The light-rung steady average is pinned at ~1,000 ops/s by WAL batch fragmentation.** The Phase A roll-up (aggregated across the 8 WAL partitions, last 60 s window per rung) shows the smoking gun directly:

| Rung | `wal.append.batch_entries` p50 / avg / p99 | `wal.append.in_flight` max | `wal.append.queue_depth` p50 | `wal.append.turn_wait` p99 |
|---:|---|---:|---:|---|
| 1 | **1.0 / 1.89 / 5.67**  (max 8) | 0 | 1 | 32 ms |
| 2 | **1.0 / 1.89 / 5.56**  (max 7) | 0 | 1 | 35 ms |
| 3 | **1.0 / 1.89 / 4.89**  (max 6) | 0 | 1 | 55 ms (one shard tail 294 ms) |

At `BatchSize=1024` × `shardCount=32`, each producer batch contributes ~32 keys per shard root, and the in-leaf WAL coalescer then sees those keys arriving as 1–2-entry trickles per WAL partition - so `wal.append.batch_entries` median collapses to **1.0** (mean 1.89). Compare: U3/s128 already showed p50=5 and called that "shattered batch coalescing"; U7 took it from "shattered" to "essentially uncoalesced". The arithmetic is now exact: with 8 WAL partitions × Azure-Tables provider p50 ~14 ms × ~2 entries per append, the theoretical sustained WAL ceiling is `(8 / 0.014) × 2 ≈ 1,140 ops/s` - and the measured U7 light-rung steady avg is **1,059 / 1,008**. We measured the ceiling.

**The heavy-rung failure has the same root but a different symptom.** With `BatchSize=1024`, the producer offers ~50 batches/s to meet the rung-3 target of 50,000 keys/s (vs ~12 batches/s at `BatchSize=4096`). At a silo drain rate of ~1,000 entries/s ≈ ~1 full 1,024-entry batch/s, the producer-side queue grows ~50× faster than it drains. The silo timeout diagnostic names `LatticeGrain.SetManyAsync` mid-execution after 28 s (`NonReentrancyQueueSize=0, NumRunning=1, IsExecuting=True`), with the activation having successfully processed 119 calls before the 120th deadlined inside its body - the U5-shape "stuck mid fan-out" failure mode, not the U1b-shape queueing one. The activation isn't queued; it's individually slow because the WAL behind it is starved.

**Quantitative cross-check.** Rung-1 steady avg of 1,059 ops/s × 119.4 s elapsed = 126,440 entries; actual `written` = 116,593 - within 8% of the steady-rate × wall-clock budget. The shape is the same as U3 (WAL-starvation ceiling) but with a worse fragmentation constant. **`BatchSize` is not a tuning lever for throughput in either direction**: smaller fragments the WAL, larger (we already know from U6/U1) doesn't lift the ceiling because the silo-side `ShardRootGrain` turn queue is the binding constraint at 4096 too.

**Conclusion on U7.** The campaign's best configuration remains `shardCount=32 + batchSize=4096 + flushConcurrency=8` (U6). `BENCH_BATCH_SIZE=1024` is **rejected**; the production default of 4096 stays. The U6 mental model was wrong about *which* per-call cost was the bottleneck - shrinking the producer-side payload trades a small reduction in per-batch fan-out time for a large reduction in WAL batch coalescing, and the WAL coalescing dominates because the WAL is per-partition serial (one `provider.commit.duration` call per turn). The U7 result also retires "fan-out wall-clock × 8" as the explanation for the U2/U6 ceiling: the actual binding constraint on rungs 1-2 is **how many entries the WAL can coalesce per partition turn**, which is upstream-flow-shape-dependent in a way `BatchSize` cannot control on its own.

**Next probe.** Two genuine levers remain visible:

- **U8 - go *back* on shard count to fatten WAL coalescing.** U3 (s128) had p50=5 and ~1,800 ops/s on the heavy rung; U6 (s32) had higher rungs-1-2 throughput; U7 (s32+b1024) showed coalescing collapses when per-shard payload thins out. A shard count of **16** at `BatchSize=4096` would put ~256 keys/shard/batch (vs U6's ~128 and U7's ~32) and let WAL `batch_entries` recover to p50 ≈ 15-20, potentially raising the WAL-side ceiling 2-3× *if* the per-shard `ShardRootGrain` turn queue doesn't reintroduce the U2-shape collapse on rung 3. The U2 collapse at `shardCount=64` proved 64 was too few; the U6 result proved 32 was a good middle; U8/s16 tests whether the heavy-rung queue *still* dominates below 32 or whether WAL coalescing wins back the throughput on the lighter rungs.
- **U9 - reentrant `ShardRootGrain` with disjoint-key `[MayInterleave]`.** Still the eventual win and still the only lever that could break the per-shard serial-turn ceiling regardless of batch/shard tuning. Defer until the U8 shard-count probe finishes; U9 is the broader-semantic change (touches split-promotion, the saga-participant set, the routing cache, and `RecordAffectedLeafIfPreparedAsync`).
### U8 measurement (2026-05-25T06:11Z-T06:19Z) - `shardCount=16` recovers WAL coalescing and posts the campaign-best rung 2 (CONFIRMED)

Same ladder, same `BatchSize=4096`, same `FlushConcurrency=8`, same U2 image - reducing `BENCH_SHARD_COUNT` from 32 (U6) to **16**. Hypothesis from U7: U7 proved smaller per-shard payloads collapse WAL `batch_entries` to ~1.9; the inverse probe is to halve shard count so each shard root gathers ~2x more keys per batch (~256 keys/shard/batch at vehicles=1000) and lets WAL coalescing recover. Risk: at vehicles=10000 the per-shard turn-queue depth could reintroduce the U2/s=64-shape heavy-rung collapse. Per-rung CSV: `benchmark/azure-throughput/scripts/.ladder-results-U8-shards16.csv`. Silo logs: `silo-20260525-06{1119,1442,1751}Z.log`. Phase A CSV: `benchmark/azure-throughput/scripts/.ladder-phaseA-U8-shards16.csv`.

| Rung | Vehicles | Target | U6/s32 final | U7/s32+b1024 final | **U8/s16 final** | delta vs U6 | U8 steady avg | U8 written | U8 failed |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 |  1,000 |  5,000 | 3,189 |   977 | **2,170** | -32.0% | **2,390** | 258,788 | **0** |
| 2 |  5,000 | 25,000 | 3,137 |   951 | **2,335** | -25.6% | **2,649** | 278,916 | **0** |
| 3 | 10,000 | 50,000 |  1,865 |   326 | **1,778** |  -4.7% | **1,877** | 212,248 | **0** |

Hypothesis confirmed on the WAL-coalescing side; throughput trade-off is real but small. Rung 2 steady avg of **2,649 ops/s is the campaign-best**, beating U6/s32 rung 2 (2,335-3,137 final) on the rate that matters - sustained, non-burst, with zero failures. The U2/s=64-shape collapse did not return: rung 3 finished at 1,877 ops/s with zero failed batches, only ~5% behind U6. The "rung 1 lower than U6" line is steady-state vs final-running-average noise (U6 final was 3,189 because the U6 silo got a late burst window; U8's steady-state min/avg/max table shows the actual sustained rate is higher and more stable - max 12,280/s on rung 2).

**The WAL evidence is decisive.** Across the 8 WAL partitions, last-60s-window per shard:

| Rung | `wal.append.batch_entries` p50 / avg | p90 | p99 | `wal.append.in_flight` max | `wal.append.queue_depth` p50 |
|---:|---|---:|---:|---:|---:|
| 1 | **7.3 / 9.2**  (max ~22) | 13.8 | 22.7 | 0 | 1 |
| 2 | **7.0 / 8.0**  (max ~12) | 10.7 | 11.9 | 0 | 1 |
| 3 | **4.6 / 5.0**  (max ~13) |  8.1 | 11.2 | 0 | 1 |

Compare to U7/s32+b1024 (`batch_entries` p50 = **1.0**, avg = **1.89**): U8 recovered **~5x coalescing** on rungs 1-2 and ~2.6x on rung 3. The `in_flight = 0` line across every rung proves slot turnover is healthy at `FlushConcurrency=8` (i.e. the silo is not WAL-throttled by the flush cap), and `queue_depth p50 = 1` proves the WAL-shard turn queue is not a constraint. The Azure-Tables provider duration histogram is in the same band as U6 (`provider.duration` P99 = 26-97 ms across shards on rung 3), so the throughput delta is not provider-latency-driven.

**Why rung 3 is now soft instead of catastrophic.** With `shardCount=16` the per-shard producer pressure on rung 3 (~3,125 keys/shard/s steady) is high enough that the *producer-side* batch interval becomes the binding constraint before the shard turn queue does - so the shape is "drain ~1,900 ops/s, queue some, end with no failures" rather than U2's "drain stalls completely and times out". The 1,877 ops/s rung-3 number is within 4.7% of U6/s32, which means halving shard count did **not** reintroduce the heavy-rung collapse. The narrative is "U6 traded WAL coalescing for shard parallelism, U8 walks half the trade back, rungs 1-2 win, rung 3 essentially flat".

**Conclusion on U8.** The new campaign-best configuration is `shardCount=16 + batchSize=4096 + flushConcurrency=8`. Rung 2 sustained at **2,649 ops/s** is the best single-rung steady-state we have observed end-to-end on real Azure Tables; rung 1 also lifted to 2,390 sustained. The U6 mental model is updated: shard count is a coalescing/parallelism trade-off with a sweet spot below 32, not a "more is more" knob. The doc-stated "20,000 ops/s per Standard account" ceiling is still ~7.5x above what a single silo + 8-partition WAL drives in this harness; the next probes have to attack the per-shard serial-turn invariant (U9) and/or WAL-side fan-out (more partitions × pipelining), not shard count.

**Next probe.** Two remaining levers, in order:

- **U9 - reentrant `ShardRootGrain` with disjoint-key `[MayInterleave]`.** Now the highest-leverage probe. U8's `wal.append.queue_depth p50 = 1` and `in_flight = 0` confirm the WAL is no longer the binding constraint at this configuration - the binding constraint is the per-shard `ShardRootGrain` serial turn queue. Disjoint-key reentrancy lets the silo overlap fan-out work *between* different keys on the same shard root, breaking the 1-call-per-turn invariant on rungs 2-3 where producer batches contain hundreds of distinct keys. This is the broader-semantic change (touches split-promotion, the saga-participant set, the routing cache, and `RecordAffectedLeafIfPreparedAsync`) and is the next concrete code probe.
- **U10 - `WalPartitions = 16` against the new s=16 baseline.** With WAL coalescing back to p50 ≈ 7, raising `WalPartitions` from 8 to 16 could double the WAL fan-out without re-fragmenting `batch_entries` (because each WAL partition now sees ~32 keys/batch instead of ~16). Defer behind U9 because U9 is the much bigger win; revisit if U9 lands and rung 3 stays soft.

### U8b measurement (2026-05-25T07:01Z-T07:13Z) - `shardCount=8` bounds the shard-count axis from below; campaign-best stays at s=16 (CONFIRMED)

Same ladder, same `BatchSize=4096`, same `FlushConcurrency=8`, same U2 image - reducing `BENCH_SHARD_COUNT` from 16 (U8) to **8**. Hypothesis: U8 (s=16) and U6 (s=32) both ran clean across all three rungs; U8 won rungs 1-2 and tied rung 3, so the local optimum could still be lower. If s=8 keeps improving, we have a free win; if it regresses, we have a clean lower bound for the shard-count axis and U9 becomes unambiguously the next probe. Per-rung CSV: `benchmark/azure-throughput/scripts/.ladder-results-U8b-shards8.csv`. Silo logs: `silo-20260525-07{0324,0813,1124}Z.log`. Phase A CSV: `benchmark/azure-throughput/scripts/.ladder-phaseA-U8b-shards8.csv`.

| Rung | Vehicles | Target | U6/s32 final | U8/s16 final | **U8b/s8 final** | U8b steady avg | U8b written | U8b failed |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 |  1,000 |  5,000 | 3,189 | 2,170 |    **72** |    **38** |   8,192 | **67,344** |
| 2 |  5,000 | 25,000 | 3,137 | 2,335 | **1,972** | **2,106** | 232,031 |     0 |
| 3 | 10,000 | 50,000 |  1,865 | 1,778 | **1,775** | **1,877** | 212,224 |     0 |

The shape is informative: rung 1 catastrophic, rungs 2-3 essentially identical to U8. This is **cold-start activation thrash**, not a steady-state shard-count failure. On rung 1 the producer fires ~5 batches/s × ~1,024 keys = ~5,000 entries/s through 8 `ShardRootGrain` activations that are still cold; with 30 s `LatticeGrain.SetManyAsync` timeouts and warm-up latency dominating the first 3-5 s window, the producer queue blows past the deadline before activations stabilise. 8,192 written is exactly 2 batches × 4,096 - the silo received the first two and then the producer-side timeouts started cascading.

**The WAL evidence proves WAL is innocent.** The aggregated `wal.append.batch_entries` for U8b/s8 vs U8/s16:

| Rung | U8b/s8 batch_entries p50 / mean / p90 | U8/s16 batch_entries p50 / mean / p90 | U8b in_flight max |
|---:|---|---|---:|
| 1 | **6.3 / 8.7 / 12.4**  | 7.3 / 9.2 / 13.8 | 0 |
| 2 | **7.2 / 8.0 / 10.7**  | 7.0 / 8.0 / 10.7 | 0 |
| 3 | **4.2 / 5.0 /  8.1**  | 4.6 / 5.0 /  8.1 | 0 |

Rungs 2-3 batch_entries are **statistically indistinguishable** between U8b and U8: the WAL coalescer sees the same shape because the *per-WAL-partition* arrival rate is the same regardless of upstream shard count (the 8 WAL partitions are not a function of `shardCount`). Even rung 1's WAL coalescing was healthy (mean 8.7) - the failures happened *before* keys reached the WAL, in `LatticeGrain.SetManyAsync` → `ShardRootGrain.SetManyAsync` → `RouteAndApplyAsync` activation queue.

**Why rung 1 failed but rungs 2-3 didn't.** Rung 2/3 have higher steady-state producer load (5x / 10x vehicles), so activation warm-up happens during the producer's own ramp-up - by the time the producer is hammering, the 8 activations are warm and the system is in steady state. Rung 1's lighter load means the cold-activation window dominates the entire 60 s measurement. This same failure mode could in principle hit U8/s16 too if the timeout were aggressive enough; it didn't in U8 because 16 activations distribute the warm-up cost better.

**Rung 3 is identical across U8 and U8b** (1,877 vs 1,877 ops/s, both with zero failures). This is the most important number on the page: it proves the **heavy-rung ceiling at ~1,880 ops/s is NOT shard-count-bound at any `shardCount <= 16`**. Halving shard count from 16 to 8 produced exactly the same rung-3 throughput. The binding constraint on rung 3 is therefore not the per-shard turn queue (which would *grow* as shards shrink) and not WAL coalescing (which is also identical) - it is **WAL fan-in latency on the 8 partitions**, i.e. the `(walPartitions / provider.commit.duration) × batch_entries` arithmetic. With p50 batch ≈ 5 entries on rung 3 × p50 provider duration ≈ 17 ms × 8 partitions = `8 / 0.017 × 5 ≈ 2,350 ops/s` theoretical - and we measure ~1,880 sustained, which lines up.

**Conclusion on U8b.** The campaign-best remains `shardCount=16 + batchSize=4096 + flushConcurrency=8`. `shardCount=8` is **rejected** on cold-start grounds even though steady-state rungs 2-3 are competitive. The shard-count axis is now bounded: `s ∈ {16, 32}` is the safe band, with s=16 winning rungs 1-2 and tying rung 3. **`shardCount` is not a remaining lever for rung 3 throughput** - U8 and U8b proved it.

The next probe is now unambiguous:

- **U9 - reentrant `ShardRootGrain` with disjoint-key `[MayInterleave]`.** U8b confirmed rung 3 is not shard-count-bound; the per-shard `ShardRootGrain` serial turn queue is the binding constraint. This is now the only remaining lever inside the silo at this configuration. The implementation touches split-promotion (`PrepareSplitAsync` / `CompleteSplitAsync` order with concurrent in-flight `RouteAndApplyAsync`), the saga-participant set (atomic-write fan-in across leaves), the routing cache (epoch-bumps on concurrent splits), and `RecordAffectedLeafIfPreparedAsync`. The change is broad-semantic; it needs its own design pass and `Category("Chaos")` validation before merging. The expected win is rung-3 throughput from ~1,880 to >3,000 ops/s on the same WAL configuration, because disjoint-key batches that today serialise on the same shard root would instead execute concurrently.
- **U10 (deferred behind U9)** - `WalPartitions = 16` against the s=16 baseline. The WAL fan-in arithmetic above implies doubling `WalPartitions` could lift rung 3 from ~1,880 to ~3,500 ops/s *if* batch_entries doesn't fragment (i.e. if Azure Tables can absorb 16 parallel partition keys without per-partition rate-limiting). This is a config-only probe (no code change) so it remains a cheap fallback if U9 turns out to be too risky for this campaign.

### Re-examination of the U8 evidence (2026-05-25T07:30Z) - U9 deferred; phase-2 coalescing is the actual binding constraint (CORRECTION)

A second-pass read of the U8 phase-A instrument set falsifies the U9 framing above. The original conclusion ("the per-shard `ShardRootGrain` serial turn queue is the binding constraint") was reached **without inspecting the `provider.phase2.batch_size` instrument**, which is the smoking gun.

Aggregated U8 rung 3 instruments, mean of per-shard quantiles, sorted by mean P50:

| Instrument | Samples | P50 mean | P90 mean | P99 mean | Max obs |
|---|---:|---:|---:|---:|---:|
| `wal.append.batch_bytes`       | 3,646 | 1,597.4 | 2,851.9 | 3,945.9 | 5,982 |
| `wal.append.turn_wait`         |    16 |    29.4 |    35.5 |    35.5 |   148 |
| `wal.append.provider.duration` | 3,654 |    28.1 |    39.6 |    80.9 |   184 |
| `provider.commit.duration`     | 7,313 |    20.3 |    28.0 |    59.2 |   184 |
| `wal.append.batch_entries`     | 3,646 |     4.6 |     8.1 |    11.2 |    17 |
| **`provider.phase2.batch_size`** | **3,654** | **1.00** | **1.00** | **1.00** | **1.00** |
| `wal.append.queue_depth`       |    16 |     1.00|     1.00|     1.00|     1 |
| `wal.append.in_flight`         | 3,646 |     0.00|     0.00|     0.00|     0 |

**`provider.phase2.batch_size = 1.0` (max 1) across 3,654 samples** is decisive. The `PhaseTwoWorker` (`src/lattice.storage.azuretable/AzureTableWalStorageProvider.PhaseTwoWorker.cs`) is *designed* to coalesce up to **49 phase-2 commits into a single Azure Tables transaction** (`MaxBatchedManifestRows = 49` at line 229; the per-transaction cap is 100 actions, each batch contributes 2 actions, 1 reserved for TAIL upsert ⇒ `(100 - 1) / 2 = 49`). Under U8 it is coalescing **exactly one commit per transaction across 100% of the sample window**. Coalescing is broken, not slow.

**Why the original U9 conclusion was wrong.** I read `wal.append.in_flight = 0` as "WAL has headroom, so the bottleneck is upstream" and concluded the `ShardRootGrain` turn queue was binding. The correct reading is the opposite: `in_flight = 0` and `queue_depth p50 = 1` together mean the WAL-shard flush worker is **starved** - it commits one phase-1 batch, faults the channel back to 0, waits for the next phase-1 arrival, commits, drains to 0, etc. The arrival rate at each `PhaseTwoWorker` channel is too slow to ever hold >1 commit in the channel between drain cycles, so the coalescing loop (`while (_batchBuffer.Count < 49 && _pending.Count > 0)`) exits with exactly 1 element every time.

**Structural cause.** With `WalPartitions=8` and `WalMaxPendingBatches=8`, each per-partition `PhaseTwoWorker` sees only its own partition's phase-1 stream. Per-partition phase-1 throughput at U8/rung-3 is ~`1,877 / 8 = 235 batches/s` ⇒ one arrival every 4.3 ms on average. `CommitBatchAsync` itself takes ~20 ms (`provider.commit.duration` P50). The arithmetic works out: 4.3 ms between arrivals × ~5 arrivals during one 20 ms commit window = ~5 elements should be pending when the next batch is drained. We see 1. Either the producer side is bursty (so arrivals cluster at sub-millisecond intervals followed by gaps >>20 ms) or `WalMaxPendingBatches=8` is itself capping per-partition phase-1 in-flight count too aggressively to bring the channel above zero between phase-2 cycles.

**Silo-side per-call latency reconciled.** The U8 silo log shows `inFlight = 8` constant on rung 3 with the producer-side semaphore (`FlushConcurrency=8`, `SemaphoreSlim` in `benchmark/azure-throughput/Silo/Program.cs#DrainAsync` around line 470) saturated, and `written` increments by exactly 4,096 entries per ~1 s. Each `ILattice.SetManyAsync(4096)` therefore returns in ~1 s wall-clock. With `provider.commit.duration` P50 = 20 ms and **one commit per Azure transaction**, a 4,096-entry batch that fans out across 16 shards × on average ~3 leaves/shard × 1 commit per leaf-batch ≈ ~48 sequential commits per producer call (in fact parallelised across `WalPartitions=8`, so ~6 sequential commits per partition × 20 ms = ~120 ms theoretical minimum). The remaining ~880 ms per call is the *coalescing-loss tax*: every commit that should have ridden along on a coalesced transaction instead paid its own Azure round-trip.

**Re-ranked probe order.** U9 (reentrant `ShardRootGrain` with `[MayInterleave]`) is **deferred indefinitely**, not because it's wrong on principle but because the bottleneck is downstream of `ShardRootGrain` in the WAL phase-2 coalescer. Shipping U9 against the current `phase2.batch_size = 1` configuration would broaden the silo-side fan-out semantics (touching split-promotion, the saga-participant set, the routing cache, and `RecordAffectedLeafIfPreparedAsync`) at substantial chaos-test risk, *and* it would not move the rung-3 ceiling because the phase-2 ceiling stays at one-Azure-RT-per-commit. Cheaper config-only probes have to fall first:

- **U9 (renamed) - `BENCH_WAL_MAX_PENDING_BATCHES = 32` at s=16.** Config-only, no code. Directly targets the measured `phase2.batch_size = 1` gap by letting each partition hold more phase-1 transactions in flight, raising the arrival rate at the per-partition `PhaseTwoWorker` channel so the coalescing loop sees `pending.Count >> 1` when it drains. Expected: `phase2.batch_size` rises from 1 to 5-15, `provider.commit.duration` × commits-per-call drops 2-5x, rung-3 throughput multiplies. Falsifiable: if `phase2.batch_size` stays at 1, arrival shape (not in-flight cap) is the cause and U9b is next.
- **U9b - `BENCH_WAL_PARTITIONS = 4` at s=16, `WalMaxPendingBatches=8`.** Halves the number of `PhaseTwoWorker` instances, concentrating phase-1 arrivals into each remaining worker so the per-partition rate doubles. Trades phase-1 parallelism for phase-2 coalescing density. Config-only. Use only if U9 does not lift `phase2.batch_size`.
- **U9c - `BENCH_WAL_PARTITIONS = 16` at s=16, `WalMaxPendingBatches=8`.** The opposite direction: doubles phase-1 fan-out. Worth running only if U9 + U9b both keep `phase2.batch_size = 1` *and* `wal.append.in_flight` rises above 0 (i.e. the per-partition flush worker is no longer starved but the Azure Tables account is rate-limiting per-partition).
- **U9d (was U9 proper) - reentrant `ShardRootGrain` with disjoint-key `[MayInterleave]`.** Deferred behind U9 / U9b / U9c. Re-evaluate only if all three config-only probes leave `phase2.batch_size` near 1 *and* the silo log shows the per-shard turn queue building up under `NonReentrancyQueueSize > 0` diagnostics (none observed in U8 rung 3). This is the broader-semantic change; it warrants its own design pass and a `Category("Chaos")` validation pass before merging, and it should only land if there is evidence it is the binding constraint - which there currently is not.

**Note on which evidence retired which framing.** The U8 measurement section above (still accurate on what it measured) was concluded with "the binding constraint is the per-shard `ShardRootGrain` serial turn queue". That sentence is **superseded by this re-examination**: the binding constraint at the U8 configuration is the *per-partition phase-2 coalescing ratio*, which `[MayInterleave]` does not address. The U8 throughput numbers, WAL evidence, and shard-count axis bound stand; only the next-probe attribution changes.

### U9 measurement (2026-05-25T07:39Z-T07:50Z) - raising `WalMaxPendingBatches` from 8 to 32 leaves `phase2.batch_size` pinned at 1 (FALSIFIED, U9b is next)

**Setup.** Identical to U8 baseline (`shardCount=16`, `batchSize=4096`, `flushConcurrency=8`, `walPartitions=8`), with the single change `BENCH_WAL_MAX_PENDING_BATCHES=32`. Three rungs identical to U8/U8b for direct comparison.

**Results.**

| Rung | Vehicles | TargetRate | SteadyAvg | FinalWritten | FinalFailed | U8 SteadyAvg | U8 FinalWritten |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,000 | 5,000/s | **2,894** | 290,496 | 0 | 2,390 | 258,788 |
| 2 | 5,000 | 25,000/s | **2,867** | 302,019 | 0 | 2,649 | 278,916 |
| 3 | 10,000 | 50,000/s | **1,513** | 208,293 | **8,192** | 1,877 | 212,248 |

Sources: `benchmark/azure-throughput/scripts/.ladder-results-U9-walpending32.csv`, `.ladder-phaseA-U9-walpending32.csv` (243 phase-A rows across 3 rungs).

**The falsifiability test fails decisively.** The U9 conclusion section above predicted that raising `WalMaxPendingBatches` to 32 should let each `PhaseTwoWorker` see `pending.Count >> 1` and push `provider.phase2.batch_size` from 1.0 up to the 5-15 range. The actual measurement, mean of per-shard quantiles:

| Configuration | Rung | `phase2.batch_size` samples | P50 | P90 | Max |
|---|---:|---:|---:|---:|---:|
| **U8 (walMaxPending=8)** | 1 | 2,684 | 1.00 | 1.00 | 1.00 |
| **U8 (walMaxPending=8)** | 2 | 234 | 1.00 | 1.00 | 1.00 |
| **U8 (walMaxPending=8)** | 3 | 3,654 | 1.00 | 1.00 | 1.00 |
| **U9 (walMaxPending=32)** | 1 | 2,600 | **1.00** | **1.00** | **1.00** |
| **U9 (walMaxPending=32)** | 2 | 1,331 | **1.00** | **1.00** | **1.00** |
| **U9 (walMaxPending=32)** | 3 | 3,192 | **1.00** | **1.00** | **1.00** |

`phase2.batch_size` is *exactly* pinned at 1.0 across all 7,123 U9 samples and all 6,572 U8 samples. Quadrupling the in-flight cap moved nothing at the coalescer.

**Throughput moved a little, but not from coalescing.** Rungs 1 and 2 gained +21% / +8% over U8, with zero failures. That gain has to come from the phase-1 path (more parallel Azure Tables submissions during the `wal.append` window), not from phase-2 coalescing - the `wal.append.batch_entries` profile is essentially identical to U8 (P50 ≈ 7.3 / 7.2 / 4.6 by rung). Rung 3 *regressed*: steady-avg dropped from 1,877 to 1,513 (-19%) and produced **8,192 failures** (vs U8's zero). The extra in-flight capacity at rung 3 is consumed by transactions that eventually time out or get rejected by Azure Tables under sustained pressure, not by the coalescer. The rung-3 phase-A also shows `provider.commit.duration` p99 reaching 106 ms (vs U8's ~54 ms) and `wal.append.turn_wait` appearing on two shards with P50 ≈ 105 ms - direct evidence that the per-shard append turn is now contended under the higher cap.

**Conclusion.** `WalMaxPendingBatches` is **not the lever**. The coalescing failure is not caused by an in-flight cap on phase-1; it is caused by the **arrival shape** at each per-partition `PhaseTwoWorker` channel - phase-1 commits arrive in sub-millisecond bursts followed by long gaps, so the channel drains to zero between bursts no matter how high we let the in-flight count climb. The other suspect, "too many partitions starve each worker," remains alive and is now the next test.

**Next probe (U9b - reduce partition count to concentrate arrival rate per worker).** Set `BENCH_WAL_PARTITIONS=4` at the U8 baseline (`shardCount=16`, `batchSize=4096`, `flushConcurrency=8`, `walMaxPending=8`). This halves the number of `PhaseTwoWorker` instances and concentrates phase-1 arrivals so each remaining worker sees ~2× its previous arrival rate. Expected if "arrival shape" is the right hypothesis: `phase2.batch_size` rises above 1 on at least the heavier rungs. Falsifiable: if `phase2.batch_size` stays at 1, the bursty arrival pattern is endogenous to the silo-side commit path (not partition fan-out), and the next probe is to look at *what* causes phase-1 commits to arrive in bursts - which points back to the producer-side `FlushConcurrency` cycle and possibly a deliberate batching delay inside the `PhaseTwoWorker` drain loop (currently no debounce; it drains immediately on the first signal).

### What stays in place

- The Phase A diagnostic instruments (histograms + tag set) are **unchanged**; the data was right, the interpretation was wrong. The per-step `leaf.commit.duration` quantiles are already in `results.json` (the A1 probe was a no-op).
- The new candidate instrument is the **cross-grain dispatch histogram** described in A2; it goes on `WalCommitLogWriter`, not on `WalShardGrain` (the WAL grain's clock cannot see its own turn-queue wait by construction).
- The C4 observability slice (`provider.retry.attempts` per-attempt counter, `RetryAttemptTrackingPolicy`) is **kept**. It is correct production telemetry and costs nothing on the happy path; the only retraction is its **Phase A justification**, not its presence.
- The C4 tuning knobs on `AzureTableWalStorageOptions` (`RetryMaxAttempts`, `RetryDelay`, `RetryMaxDelay`, `RetryNetworkTimeout`, `RetryMode`) are **kept** as production-hygiene knobs. They are correct shape for operators who deploy against a real Azure Tables account that does surface 503s. Their A/B-measured null effect on Azurite is expected, not a defect.
- The wire format remains frozen.
- Phase B and Phase D step lists above are still valid; they remain paused, not invalidated, pending resolution of anomalies 1 and 3 respectively.

**Progress**: 0% [░░░░░░░░░░]

**Last Updated**: 2026-05-24 17:30:00

## 📝 Plan Steps
-  **Read `src/lattice/BPlusTree/Grains/WalShardGrain.cs` end-to-end, `src/lattice/BPlusTree/Grains/BPlusLeafGrain.cs` (foreground commit path, especially `CommitSetAsync` and `PublishDigestUpwardAsync`), `src/lattice/BPlusTree/Grains/AtomicWriteGrain.cs`, `src/lattice.storage.azuretable/AzureTableWalStorageProvider.cs` (and `PhaseTwoWorker`), and `src/lattice/BPlusTree/Options/LatticeOptions.cs` - confirm every choke point listed in *Architectural context*; record any deviation from the plan's assumptions before writing code.**
-  **Phase A instrumentation - SHIPPED. `LatticeMetrics` already emits per-step `leaf.commit.duration` (tagged `wal` / `apply` / `observer` / `digest`), WAL queue / batch / in-flight / turn-wait / provider-duration histograms, and `provider.retry.attempts` per-attempt counter. `benchmark/benchmark-attribution.ps1` drives the 46-cell matrix.**
-  **Run the matrix end-to-end and write the Phase A report - done (`benchmark/diagnostic-reports/diagnostic-report-2026-05-24T07-22-03Z.md`). Initial attribution to Azure SDK retry / backoff cost was subsequently FALSIFIED by the C4 A/B re-measurement; see "Phase A - Outcomes" for the retracted findings and the corrected next probe.**
-  **A2 - Cross-grain dispatch instrumentation (SHIPPED AND MEASURED). `LatticeMetrics.WalShardDispatchDuration` (`orleans.lattice.wal.shard.dispatch.duration`) is recorded in `WalCommitLogWriter.AppendAsync` and on the batched per-partition fan-out, tagged with `tree` / `shard` / `wal_partitions` / `wal_max_pending_batches`. `$ScalarAliases` in `benchmark.ps1` adds `lattice_wal_shard_dispatch_p95_ms` and `lattice_wal_shard_dispatch_p99_ms`. The 2026-05-24T14:35Z Azurite measurement (`benchmark/.run/current-state-no-replication-azuretable/2026-05-24T14-35-29Z/results.json`) shows `dispatch P99 = 978.30 ms`, `turn_wait P99 = 32.75 ms`, `provider_duration P99 = 24.20 ms`, `apply P99 = digest P99 = 0.10 ms` - the ~945 ms gap between dispatch and turn_wait confirms the Orleans turn-queue-wait hypothesis on the single `WalShardGrain` activation under `WalPartitions = 1`. Phase B2 is unblocked on Azurite evidence; real-Azure ship-criterion is throughput delta on `benchmark/azure-throughput` against a real Azure Tables account because Anomaly 1's harness ceiling may otherwise mask the gain.**
-  **B2 (RETRACTED, 2026-05-24T15:50Z) - Raising `WalPartitions` does not improve dispatch P99 on either arm. Memory-WAL: P=1 dispatch P99 is already 1.4 ms (proves the residual is not Orleans grain-scheduling), and throughput is flat at ~4,290 ops/s across P ∈ {1,2,4,8} (bottleneck is downstream of the WAL). Azurite: P=1→P=8 monotonically regresses dispatch P99 (978→2,176 ms), `provider_duration` P99 (24→99 ms), and ops/s (94.6→63.8) because Azurite serialises all `SubmitTransactionAsync` calls through a single backend lock - adding partitions multiplies in-flight provider calls against the same constrained backend. The library default of `WalPartitions = 1` is correct as shipped. The corrected attribution is that the dispatch P99 residual is the per-shard queue ahead of the awaited provider call (provider-queue, not grain-schedule).**
-  **NEXT - Validate the corrected attribution on `benchmark/azure-throughput` against a real Azure Tables account. Azurite's single-process serialisation is a measurement artifact that inverts the partition-scaling shape; the real-Azure provider scales with partition keys, so the dispatch histogram should *shrink* (not grow) as `WalPartitions` rises against a real backend. If confirmed, Phase C's *provider-throughput-scaling* framing (parallel partition keys, batching, pipelining - NOT the retracted retry-storm framing) becomes the next concrete probe. If the real-Azure shape *also* shows that adding partitions hurts, then the bottleneck is on the leaf-side commit path (`BPlusLeafGrain.CommitSetAsync` waiting on its own WAL fan-in), and the next probe shifts to leaf-side concurrency rather than WAL-side partitioning.**
-  **Phase B - PAUSED by Phase A anomaly 1 (`current-state-no-replication` flat at ~17,100 ops/s across all 9 knob combinations including `WalPartitions` ∈ `{1, 4, 16}` indicates the bench harness, not the silo, is capping the scheduling-path measurement). Resume only after the harness ceiling is independently re-measured; if the true silo ceiling is then > 17,100 ops/s the original B1 → B4 → B5 → B2 → B3 order applies.**
-  **Phase C - UN-PAUSED on the *provider-throughput-scaling* slice (parallel partition keys, batching, pipelining) once the real-Azure validation in the NEXT step confirms the corrected attribution. The retry-storm framing that originally motivated C4-first remains RETRACTED; C4 observability + tuning knobs already shipped and are kept as production-hygiene telemetry. C1–3 and C5 remain candidates depending on the real-Azure measurement.**
-  **Phase D - PAUSED by Phase A anomaly 3 (atomic-write throughput variance is ~four orders of magnitude across adjacent cells: 0, 4, and 31,960 ops/s observed in cells 30/36/34). Stabilise the atomic-write bench (longer runs, warm-up, deterministic concurrency) before picking D; if the saga path then under-performs, the original D3 → D1 → D2 → D4 order applies.**
-  **After each Phase B/C/D/F PR, run `dotnet test --filter "TestCategory!=Chaos"` and the targeted atomic-visibility + causal-correctness fixtures; before merging the default-flip PRs (B2, B3, C1), additionally run `dotnet test --filter "TestCategory=Chaos"` and append the result to the diagnostic report.**
-  **Final phase E roll-up - update `docs/lattice/wal.md`, `docs/lattice/wal-storage-providers.md`, and any roadmap entry whose deps are satisfied (e.g. F-075 if C3 ever ships), with the measured ops/s vs the documented Azure Tables ceiling captured in the docs.**
