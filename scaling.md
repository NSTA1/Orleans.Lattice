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

### U9b measurement (2026-05-25T08:09Z) - halving `WalPartitions` exactly doubles per-partition phase-1 fill but leaves `phase2.batch_size` pinned at 1 (FALSIFIED, mechanism is now diagnosed)

**Setup.** U8 baseline (`shardCount=16`, `batchSize=4096`, `flushConcurrency=8`, `walMaxPending=8`) with the single change `BENCH_WAL_PARTITIONS=4`. Three rungs planned; the local harness crashed between rungs (see "Harness instability" note below) so only **rung 1** completed cleanly. **The rung-1 sample size alone (556,566 `phase2.batch_size` samples) is decisive** - 1.55x the entire U9 3-rung total - and the supporting instruments paint a complete picture without needing rungs 2 and 3.

**Rung-1 results.** `silo-20260525-080940Z.log` (preserved as `.ladder-U9b-walpartitions4-rung1-silo.log`): `FINAL written=253,619 failed=0 elapsed=119.5s Entries written per second (avg)=2,123`.

**Phase-A side-by-side, rung 1, mean of per-shard quantiles:**

| Instrument | Phase | U8 (P=8) P50 | U9b (P=4) P50 | Direction |
|---|---|---:|---:|---|
| `wal.append.batch_entries`     | -      | 7.33    | **13.87** | **+89%** (each phase-1 batch carries ~2x more entries - matches the halved-partition arithmetic) |
| `wal.append.batch_bytes`       | -      | ~1,409  | **4,875** | **+246%** (size grew faster than entries because in-flight cap is no longer fragmenting) |
| `provider.commit.duration`     | phase1 | 13.13ms | **22.95ms** | +75% (longer Azure transaction per phase-1 commit, expected) |
| `provider.commit.duration`     | phase2 | 13.13ms | **11.52ms** | -12% (slightly faster phase-2 commit - smaller manifest churn) |
| **`provider.phase2.batch_size`** | -    | **1.00** | **1.00** | **UNCHANGED** |
| `wal.append.in_flight`         | -      | 0       | 0          | unchanged |
| `wal.append.queue_depth`       | -      | 1.00    | 1.00       | unchanged |
| `wal.append.turn_wait`         | -      | ~30ms   | 25ms       | -17% (less per-shard contention) |
| Throughput (rung 1 avg)        | -      | 2,390/s | **2,123/s** | -11% |

Source data: `benchmark/azure-throughput/scripts/.ladder-U9b-walpartitions4-rung1-silo.log` (2.57 MB raw silo log, with 1,552 `[phaseA]` lines for the `provider.phase2.batch_size` instrument summing to **556,566 individual samples**, every single one with `max=1.00`).

**The arrival-rate hypothesis is exactly disproved.** U9b achieved precisely what the hypothesis demanded: per-partition phase-1 fill exactly doubled (`batch_entries` 7.33 → 13.87, `batch_bytes` ~3.5x). If the coalescer were starved by low per-worker arrival rate, this configuration would have lifted `phase2.batch_size`. It did not - not in one of 556,566 samples. The conclusion is that *no per-partition arrival rate that can be achieved by config tuning will produce coalescing on top of the current `PhaseTwoWorker` drain logic.*

**The actual mechanism, diagnosed.** Inspecting `src/lattice.storage.azuretable/AzureTableWalStorageProvider.PhaseTwoWorker.cs#DrainLoopAsync` (lines 233-295):

```
while (WaitToReadAsync) {
    while (TryRead) _pending.Add(...);            // single arrival
    while (_pending.Count > 0) {
        batch = take up to 49 from _pending;       // takes 1
        await CommitBatchAsync(batch);             // ~11.5 ms Azure RT
        while (TryRead) _pending.Add(...);         // re-drain after commit
    }
}
```

The drain loop **never deliberately waits for arrivals to accumulate.** `WaitToReadAsync` returns the instant the first `PhaseTwoCommit` lands in the channel; the loop then takes whatever is in the channel right now (always 1), commits, and only afterwards re-drains. With `commit.duration.phase2 = 11.5 ms` and per-partition inter-arrival time ≈ `1 / (2123/s × 13.87 entries-per-batch × (1/4) partitions) × 1000 ms = ~26 ms` per partition under U9b, the coalescing window per commit is `11.5 / 26 = 0.44` arrivals - the post-commit re-drain finds 0 or 1 element on average and the cycle repeats with `batch_size = 1`. This will hold for any per-partition rate at which `inter_arrival_time > commit_duration_phase2`, which - given Azure Tables' commit latency - covers essentially all production-realistic loads.

The same arithmetic explains why U8 and U9 also showed `batch_size = 1`: in *all* three configurations the post-commit re-drain runs against a channel that is essentially empty because the producer side is not bursty enough relative to the commit RT.

**Re-ranked probe order (revised again, code change required).**

- **U9 (config, `walMaxPending=32`) - FALSIFIED** above. Phase-1 in-flight cap is not the lever.
- **U9b (config, `walPartitions=4`) - FALSIFIED** here. Per-partition arrival rate is not the lever either.
- **U9c (was config, now CODE) - add an arrival-coalescing window to `DrainLoopAsync`.** Insert a small bounded wait (e.g. `await Task.Delay(coalesceWindow, ct)` or a `WaitAsync(cts)` with a short timeout) between `WaitToReadAsync` returning and the `TryRead` drain, so that one Azure round-trip's worth of arrivals can accumulate before the first `CommitBatchAsync`. Initial value: 5-10 ms (about half the observed `commit.duration.phase2` P50). The window must be a per-`PhaseTwoWorker` option so a debug build can disable it for chaos tests that rely on strict per-commit ordering visibility, and so it can be tuned per-deployment. Falsifiable: if `phase2.batch_size` rises above ~3 on rung 3 with `coalesceWindow=5ms`, this is the lever; otherwise the bursty pattern is *also* not amenable to in-process debounce and the next layer (LatticeGrain SetManyAsync flush pattern) is the culprit. Risk vs U9d: contained - one file, no semantic change to ordering invariants (the post-commit re-drain still preserves ascending-offset commit order; coalescing is just a denser version of what already happens).
- **U9d (was U9 proper) - reentrant `ShardRootGrain` with `[MayInterleave]`.** Remains deferred behind U9c. Re-evaluate only if U9c demonstrably lifts `phase2.batch_size` and reveals a new ceiling that is upstream of the WAL.

**Harness instability note.** The local PowerShell harness around `40-ladder.ps1` failed to complete the U9b ladder twice. First attempt: stale parent-shell `BENCH_*` env-vars (`BENCH_WAL_MAX_PENDING_BATCHES=32` from U9) leaked into the deploy step despite explicit reassignment in the background launch body; observed deploy log line `walPartitions=8 walMaxPending=32` proved the leak and the run was aborted and the ACI stopped before completing. Second attempt: fresh `pwsh -NoProfile` subshell with all `BENCH_*` cleared and reassigned read `walPartitions=4 walMaxPending=8` correctly, completed rung 1 cleanly, then the background pwsh died silently between rungs (likely a `run_command_in_terminal` poller-side process-group kill on poller timeout). The rung-1 silo log was preserved before the harness state was reset. This points at a future harness-hardening task: `40-ladder.ps1` should write a marker file ("rung 1 complete") after each rung's CSV append so a re-launch can detect and resume; today there is no resume primitive.

### U9c implementation (2026-05-25, code change) - opt-in phase-2 arrival-coalescing window in `PhaseTwoWorker`

**Scope.** Source change only. Adds a configurable wall-time window the per-shard `PhaseTwoWorker` drain loop deliberately waits, after the first arrival but before submitting, so additional pending commits can accumulate. Default `TimeSpan.Zero` preserves the historical drain-on-first-signal behaviour; a positive value is opt-in per provider instance.

**What changed.**

- `AzureTableWalStorageOptions.PhaseTwoCoalescingWindow` (new `TimeSpan` property, default `TimeSpan.Zero`, validated non-negative).
- `AzureTableWalStorageProvider.PhaseTwoWorker` carries a new `_coalescingWindow` field threaded through the production and test constructors. After the first `WaitToReadAsync` returns and the initial channel drain runs, the loop calls `await Task.Delay(_coalescingWindow, ct)` and then re-drains the channel before invoking `CommitBatchAsync`. The delay is short-circuited when the pending set already has `MaxBatchedManifestRows = 49` items (no point waiting once the transaction is full) and is skipped entirely when the window is zero.
- `AzureTableWalStorageProvider.GetOrCreatePhaseTwoWorker` passes `_options.PhaseTwoCoalescingWindow` into the worker so each per-shard worker uses the host's configured value.
- Benchmark wiring: `benchmark/azure-throughput/Silo/Program.cs` reads `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS` (default `0`) and sets `o.PhaseTwoCoalescingWindow = TimeSpan.FromMilliseconds(phaseTwoCoalescingMs)`. `benchmark/azure-throughput/scripts/20-build-and-deploy.ps1` propagates the host env-var into the silo container's ACI environment.
- Tests: `AzureTableWalStorageOptionsTests` pins the default, the negative-value validation failure, and the zero-or-positive happy paths. `PhaseTwoWorkerTests` adds three behavioural tests - zero preserves one-commit-per-submit, a positive window coalesces simultaneous arrivals into a single submit with ascending M-rows and the highest `endOffsetInclusive` as `TAIL`, and an isolated arrival still commits after waiting at least the configured window.

**What is intentionally unchanged.** Strict offset-FIFO ordering (the `SortedSet` invariant), the all-or-nothing failure semantics across a coalesced group, the `MaxBatchedManifestRows = 49` cap, the activation-time orphan-recovery contract, and every existing metric tag including `provider.pipeline.phase2`. A debug build can disable the window by leaving `PhaseTwoCoalescingWindow` at its default; chaos tests that rely on strict per-commit ordering visibility see no behaviour change unless they opt in.

**Validation.** `dotnet test` of the storage-azuretable project completed with 147 / 147 non-chaos tests passing (including the 3 new `PhaseTwoWorkerTests` and 3 new options tests). Benchmark silo and provider projects both build cleanly. No fresh ladder run has been performed yet; the next falsification step is to drive the benchmark with `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5` (about half the observed `commit.duration.phase2` P50 from U9b) at the U9b baseline (`shardCount=16`, `batchSize=4096`, `flushConcurrency=8`, `walPartitions=4`, `walMaxPending=8`) and look at `provider.phase2.batch_size`. The hypothesis is falsified if `batch_size` stays at `1.00` after the change; that would mean even with a deliberate in-process debounce the producer-side arrival pattern is not coalescible and the next layer to investigate is the LatticeGrain `SetManyAsync` flush pattern (U9d).

### U9c smoke (2026-05-25T08:51Z-T08:53Z) - 5 ms coalescing window did not lift `phase2.batch_size` (FALSIFIED, U9d is next)

**Setup.** Rung 1 only (vehicles=1000, tickHz=5, target 5,000/s, duration=60s) at the U9b baseline (`shardCount=16`, `batchSize=4096`, `flushConcurrency=8`, `walPartitions=4`, `walMaxPending=8`). The single change vs U9b is `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`. Image commit: `198d026`. Tree id: `azure-throughput-20260525-084825`. Sources: `benchmark/azure-throughput/scripts/.ladder-results-U9c-coalesce5ms.csv`, `.ladder-phaseA-U9c-coalesce5ms.csv`, `.ladder-U9c-coalesce5ms-rung1-silo.log` (truncated to ~8 KB; phase-A evidence below comes from the streamed cadence lines preserved in the run stdout).

**Result.** `FINAL written=249,545 failed=0 elapsed=119.4s Entries written per second (avg)=2,089`. Steady avg 2,199/s (U9b rung 1: 2,123/s). Throughput is within run-to-run noise of U9b - **the 5 ms in-process delay neither helped nor hurt the headline number**.

**Phase-A evidence (last cadence window per shard, 4 shards, all 4 WAL partitions).**

| metric | shard 0 | shard 1 | shard 2 | shard 3 | samples / shard |
|---|---:|---:|---:|---:|---:|
| `provider.phase2.batch_size` P50 | **1.00** | **1.00** | **1.00** | **1.00** | 117 / 176 / 123 / 149 |
| `provider.phase2.batch_size` P90 | **1.00** | **1.00** | **1.00** | **1.00** | - |
| `provider.phase2.batch_size` Max | **1.00** | **1.00** | **1.00** | **1.00** | - |
| `wal.append.batch_entries` P50 | 16 | 16 | 16 | 16 | 116 / 174 / 122 / 148 |
| `wal.append.batch_entries` Max | 38 | 42 | 51 | 45 | - |
| `wal.append.provider.duration` (ms) P50 | 23.03 | 22.85 | 23.02 | 22.52 | 117 / 175 / 123 / 149 |
| `wal.append.provider.duration` (ms) P99 | 81.00 | 64.19 | 92.80 | 74.31 | - |
| `wal.append.in_flight` | 0 | 0 | 0 | 0 | 116 / 174 / 122 / 148 |

**This is the strongest disproof yet.** Across all **565 phase-2 samples in the rung's final cadence window**, every single `provider.phase2.batch_size` reading is exactly 1.00, max included. The `PhaseTwoWorker` now contains code that *deliberately delays* the first commit for 5 ms - and the channel still has zero or one element when the post-delay re-drain runs. The producer side is not just *not bursty enough*; it is *fundamentally not coalescible at this load shape* even when given a free wait window. (Cross-check: 5 ms is ~22% of `wal.append.provider.duration` P50 ≈ 22.9 ms, well within the per-Azure-commit envelope; if any coalescing were possible it would have shown up.)

**What the result says about the system.** The phase-2 worker is invoked by a per-shard pipeline whose phase-1 path (`wal.append.batch_entries` P50 = 16, P99 ≈ 45) is itself already coalescing aggressively. The shape that arrives at the channel is therefore "one phase-2 manifest commit per phase-1 transaction" - and phase-1 transactions happen at intervals of ~23 ms (the provider duration P50), which is much longer than the 5 ms window. The window expires, the channel is still empty, the worker commits the single pending row, and the cycle repeats. **Phase 2 is producer-rate-limited by phase 1, not coalescing-limited within phase 2.** There is no in-process debounce that helps here: to land >1 commit per phase-2 transaction the upstream LatticeGrain `SetManyAsync` flush would have to overlap *two distinct phase-1 transactions* on the same shard inside a single coalescing window, which the current `_options.FlushConcurrency`-gated drainer does not produce.

**Re-ranked probe order (revised yet again, the producer side is the next lever).**

- **U9 (config, `walMaxPending=32`) - FALSIFIED** (commit `672f1aa`).
- **U9b (config, `walPartitions=4`) - FALSIFIED** (commit `2e38c0d`).
- **U9c (code, 5 ms coalescing window) - FALSIFIED** (commit `198d026` + this run). The `PhaseTwoCoalescingWindow` option is **kept** in the codebase because the wire-compat default is zero, the option/worker/test surface is clean, and there may exist a future workload shape (e.g. extremely high per-shard fan-in with sub-millisecond phase-1 transactions on a faster storage backend) where it does help. There is no benefit at the current Azure-Tables-backed shape.
- **U9d (producer side, `LatticeGrain.SetManyAsync` flush pattern).** The new candidate. The drainer that turns producer batches into `WalShardGrain.AppendAsync` calls is gated by `BENCH_FLUSH_CONCURRENCY=8`. If we raised that, the per-shard phase-1 batch arrival rate could exceed `1 / commit.duration.phase2`, which is the threshold above which an arrival-coalescing window can actually trap >1 element. Falsifiable: at `flushConcurrency=32` (or `64`) with `coalesceWindow=5ms`, look for `provider.phase2.batch_size` P50 ≥ 2 *and* `wal.append.in_flight` rising above 0. If both move together, the lever is producer-side concurrency; if only `in_flight` moves, the Azure Tables partition is the binding constraint and the next step is `walPartitions=8 + flushConcurrency=32` (i.e. fatten the producer side under a fixed per-partition envelope).
- **U9e (was U9d, reentrant `ShardRootGrain` with `[MayInterleave]`).** Remains deferred behind U9d. Only revisit if U9d demonstrably lifts batching but reveals an upstream ceiling at the shard-root turn queue.


#### U9d - the next probe (`BENCH_FLUSH_CONCURRENCY=32 + coalesceWindow=5 ms` smoke, 2026-05-25). RESULT: FALSIFIED for the simple "lift `FlushConcurrency` in isolation" form, but the failure mode is itself the strongest evidence yet for where the producer-side ceiling actually lives.

| Configuration                                       | Value                              |
|-----------------------------------------------------|------------------------------------|
| Rung                                                | `1000:5` (1 000 vehicles × 5 Hz)  |
| `BENCH_SHARD_COUNT`                                 | 16                                 |
| `BENCH_BATCH_SIZE`                                  | 4 096                              |
| `BENCH_FLUSH_CONCURRENCY`                           | **32** (was 8 in U9b/U9c)          |
| `BENCH_WAL_PARTITIONS`                              | 4                                  |
| `BENCH_WAL_MAX_PENDING_BATCHES`                     | 8                                  |
| `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS`             | 5 (kept from U9c)                  |
| Duration                                            | 60 s steady + warm-up              |

**Outcome.** Catastrophic regression. `FinalAvgRate = 278/s` (versus U9c at 2 089/s, a 7.5× regression), `FinalFailed = 262 426` (versus U9c at 0), `SteadyAvg = 339/s` with `SteadyMin = 0/s`. Phase-2 batching is *still* `1.00` on every shard - but that single fact is no longer interesting because the run never reached steady producer rate.

| Instrument (rung-1 final window)                | Shard 0 | Shard 1 | Shard 2 | Shard 3 | Samples (s0/s1/s2/s3) |
|-------------------------------------------------|---------|---------|---------|---------|------------------------|
| `provider.phase2.batch_size` P50                | 1.00    | 1.00    | 1.00    | 1.00    | 38 / 41 / 54 / 45      |
| `wal.append.batch_entries` P50                  | 16      | 12      | 16      | 16      | 36 / 40 / 53 / 44      |
| `wal.append.provider.duration` (ms) P50         | 25.12   | 23.06   | 24.10   | 24.63   | 37 / 41 / 54 / 45      |
| `wal.append.in_flight`                          | 0       | 0       | 0       | 0       | 36 / 40 / 53 / 44      |

**Why the rate collapsed (silo log `silo-20260525-091015Z.log`, 85.4 MiB).** The silo per-second reporter shows `written` frozen at 29 143 from `t ≈ 37 s` to `t ≈ 100 s` with `inFlight = 32` and a steady stream of `failed += 800–4 000` per second. The relevant log signals are:

1. **`ShardRootGrain.SetManyAsync` queue is full.** Stack traces report `Activation: ... shardroot/.../1 ... NonReentrancyQueueSize = 8 NumRunning = 1 ... CurrentlyExecuting = SetManyAsync(...)` - the grain is single-threaded, executing one `SetManyAsync` at a time, with 8 more queued.
2. **`HotShardMonitor.GetHotnessAsync` is enqueued behind the producer.** Same stack trace shows `Message Request hotshardmonitor/.../GetHotnessAsync() ... has been enqueued on the target grain for 00:00:29.7900000 and is currently position 1 in queue for processing.` - the monitor's sampling RPC waits 29.8 s for the grain to free up.
3. **The monitor times out and fires a reshard.** 15 lines logged as `Hot-shard sampling pass FAILED` (timeout) followed by 17 `[silo] reshard treeId=...FAILED: OrleansMessageRejectionException: Forwarding failed: ... "Unable to create local activation" ... ForwardCount=2 ... Rejecting now.` Five upstream reshard *submits* targeting `shardCount = 16` precede the failures; the new shard activations cannot be created while the in-flight set is at its ceiling.
4. **The producer side gives up on the failing reshard request and the harness records 262 426 failed entries.**

**What this tells us about the ceiling.** Raising `FlushConcurrency` from 8 to 32 did *not* multiply phase-1 throughput, because `ShardRootGrain` is **per-shard single-activation, non-reentrant by default**. At `shardCount = 16` the producer side can offer at most 16 concurrent `SetManyAsync` calls before they queue, and the queue depth is bounded by `NonReentrancyQueueSize = 8`. With 32 flush slots fanning out into 16 shards, the over-supply hits the queue ceiling almost immediately and the monitor's housekeeping RPCs are starved out. **The producer-side hypothesis is not falsified - it is sharpened**: the lever is `ShardRootGrain` reentrancy (U9e), not bare `FlushConcurrency`.

This also retroactively explains why U9c's 5 ms coalescing window saw `provider.phase2.batch_size = 1.00`: even with the channel given 5 ms to accumulate arrivals, the **upstream phase-1 path on a single shard root cannot overlap two transactions in the first place**, so the channel can never receive >1 element per phase-1 cycle. The phase-2 worker is correctly diagnosing the input it gets.

**Re-ranked probe order (third revision, the shard-root activation is the binding constraint).**

- **U9 (config, `walMaxPending=32`) - FALSIFIED** (commit `672f1aa`).
- **U9b (config, `walPartitions=4`) - FALSIFIED** (commit `2e38c0d`).
- **U9c (code, 5 ms coalescing window) - FALSIFIED** (commit `198d026` + smoke). Option kept as opt-in.
- **U9d (config, `flushConcurrency=32`) - FALSIFIED *with diagnostic value*** (this smoke, commit `40e40d3`). The failure mode exposes `ShardRootGrain` non-reentrancy + `HotShardMonitor` starvation as the actual ceiling. Roll back to `flushConcurrency = 8` for any subsequent smoke that is not specifically testing the shard-root path. The current evidence does *not* implicate `HotShardMonitor` itself as a defect; it correctly times out when its target grain is saturated. A defensive follow-up is to make the monitor's reshard trigger idempotent against in-flight reshards (so 17 retried reshards do not amplify the failure), but that is a robustness fix, not a throughput lever.
- **U9e (code, narrow `[AlwaysInterleave]` on monitor-facing reads) - SHIPPED, PARTIAL WIN** (commit `4087b4c` + smoke below). U9d's failure-mode analysis identified two distinct symptoms: shard-root single-threaded write saturation *and* hot-shard monitor starvation. Making `SetManyAsync` reentrant via `[MayInterleave]` would race four pieces of mutable state (`_cachedLeaf`, `_cachedInternal`, routing tables, root state) and is therefore unsafe without a deeper redesign. The narrower correctness-preserving fix is to mark the two monitor-facing read methods (`GetHotnessAsync` and `HasPendingBulkOperationAsync`) `[AlwaysInterleave]` so the per-tick monitor fan-out cannot be queued behind `SetManyAsync`. Both methods are pure `Task.FromResult(...)` reads of in-memory fields and cannot race. The smoke at the same U9d knob set (`flushConcurrency=32`, `coalesceWindow=5ms`) eliminates monitor starvation but does not recover throughput - see the U9e smoke block below for the next-ranked probe.

#### U9e smoke (2026-05-25T09:30Z-T09:37Z) - narrow `[AlwaysInterleave]` eliminates monitor starvation but reshard-target activation failure persists (PARTIAL WIN, U9f is next)

**Setup.** Same rung-1 single-shot (`vehicles=1000`, `tickHz=5`, target 5 000/s, duration=60 s, `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4 096`, `BENCH_WAL_PARTITIONS=4`, `BENCH_WAL_MAX_PENDING_BATCHES=8`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`, `BENCH_FLUSH_CONCURRENCY=32`) as U9d, with the only change being the `[AlwaysInterleave]` addition in commit `4087b4c`. ACR build `cb1y` succeeded; container group `lat01-bench` deployed; silo log `silo-20260525-093456Z.log` (64.26 MiB) preserved alongside ladder artifacts `.ladder-results-U9e-alwaysinterleave.csv` and `.ladder-phaseA-U9e-alwaysinterleave.csv`.

**Outcome.** `SteadyAvg = 578/s` (vs U9d `339/s`, +70%), `FinalWritten = 49 269` (vs U9d `33 239`, +48%), `FinalFailed = 246 618` (vs U9d `262 426`, -6%), `FinalAvgRate = 413/s` (vs U9d `278/s`, +49%). Directionally correct but still ~4.5x below the U9c baseline of `2 089/s` and far below the rung target. Phase-2 batching is still pinned at `1.00` on every shard.

| Instrument (rung-1 final window)        | Shard 0 | Shard 1 | Shard 2 | Shard 3 | Samples (s0/s1/s2/s3) |
|-----------------------------------------|---------|---------|---------|---------|------------------------|
| `provider.phase2.batch_size` P50        | 1.00    | 1.00    | 1.00    | 1.00    | 54 / 6 / 41 / 28       |
| `wal.append.batch_entries` P50          | 16      | 20      | 16      | 16      | 53 / 5 / 40 / 27       |
| `wal.append.provider.duration` (ms) P50 | 23.25   | 25.61   | 24.28   | 22.93   | 54 / 6 / 41 / 28       |
| `wal.append.in_flight`                  | 0       | 0       | 0       | 0       | 53 / 5 / 40 / 27       |

**Silo-log evidence (the symptom shift).** The U9d cascade had a clear two-step structure: monitor times out -> reshard fires -> reshard fails. The U9e log shows the *first* step gone and the *second* step still present:

- `Hot-shard sampling pass FAILED` = **0** (was many in U9d). Confirms the `[AlwaysInterleave]` fix lets the monitor's per-tick `GetHotnessAsync` / `HasPendingBulkOperationAsync` fan-out complete despite saturated `SetManyAsync` work on shard-roots. The U9d monitor-starvation hypothesis is **validated and addressed**.
- `OrleansMessageRejectionException: ... Unable to create local activation` = **22** (was many in U9d). Each rejection targets the **tree-scoped `lattice/{treeId}` grain** (not a shard root): `Orleans.Lattice.ILattice.ReshardAsync(System.Int32, System.Threading.CancellationToken) #...[ForwardCount=2]`. The reshard *submits* (22) and *failures* (22) are 1:1 with `ForwardCount=2 ... Rejecting now`. The hot-shard monitor still decides to reshard - that decision is now made on accurate (non-starved) hotness samples, so the reshard signal itself is real - but the reshard execution path cannot bring up the target `LatticeGrain` activation while the silo is at full producer pressure.
- `IngestService.FlushAsync ... TimeoutException` = **499** on the producer side. With 32 flush slots fanning into 16 shards, individual `SetManyAsync` calls still queue too long behind shard-root work and the producer's per-call timeout fires. This is the residual single-threaded-write ceiling that U9e deliberately did *not* attack.

**Diagnosis.** U9e validates the U9d sub-claim that *monitor starvation* was a real artifact, but the throughput residual is now clearly **two distinct constraints layered on top of each other**:

1. **Per-shard `SetManyAsync` serial turn** - intrinsic to single-activation `ShardRootGrain`. At `flushConcurrency = 32` over 16 shards, two flush slots target each shard on average; one runs, one queues. The producer-side timeout fires before the queued one drains. This is what U9d already exposed.
2. **`LatticeGrain.ReshardAsync` activation cannot be created under load.** The reshard request is forwarded to a `LatticeGrain` activation that fails to come up while the silo is at full producer pressure (likely because the activation request itself queues behind in-flight work on the activation pipeline, or because `LatticeGrain.OnActivateAsync` itself awaits the same grain factory paths that are saturated). This is a separate defect that *only* manifests when the monitor correctly fires reshards under pressure - U9d masked it with monitor starvation.

**Re-ranked probe order (fourth revision, the two layered ceilings are separable).**

- **U9 / U9b / U9c / U9d** - FALSIFIED (rolled up above).
- **U9e (code, `[AlwaysInterleave]` on monitor reads) - SHIPPED, narrow win** (commit `4087b4c`). Keep the change; it is correct independent of any further throughput work because it stops the monitor from being a producer-pressure casualty.
- **U9f (the next probe) - roll back `BENCH_FLUSH_CONCURRENCY` to 8 (U9c baseline) and re-smoke with the U9e code change in place.** Falsifiable: if `SteadyAvg` recovers to ~2 089/s (matching U9c), then the `[AlwaysInterleave]` fix is independently correct *and* the residual failures in U9e/U9d are entirely caused by the over-pressure (`flushConcurrency=32` against `shardCount=16`) and not by an underlying activation defect. If `SteadyAvg` is materially below 2 089/s, then the residual signal is the activation/reshard defect surfaced in (2) above, and the next probe is to instrument `LatticeGrain.OnActivateAsync` / `ReshardAsync` directly.
- **U9g (deferred) - reentrant `ShardRootGrain.SetManyAsync` via `[MayInterleave]` with disjoint-key key-set partitioning.** Only attempt after U9f isolates the reshard-activation defect from the per-shard serial-turn ceiling. The four mutable caches (`_cachedLeaf`, `_cachedInternal`, routing tables, root state) must each be made interleave-safe or partitioned per turn before this is correct; the design work is non-trivial and the value gate is "did U9f confirm a true per-shard ceiling".


#### U9f smoke (2026-05-25T09:53Z-T09:56Z) - rolling `BENCH_FLUSH_CONCURRENCY` back to 8 recovers throughput *above* U9c with the U9e code change in place (VALIDATED, U9e was correct and the U9d/U9e failures were entirely over-pressure artifacts)

**Setup.** Same code as U9e (commit `4087b4c`, `[AlwaysInterleave]` on `IShardRootGrain.GetHotnessAsync` and `HasPendingBulkOperationAsync`). Knob set is identical to U9c/U9b: `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4 096`, `BENCH_WAL_PARTITIONS=4`, `BENCH_WAL_MAX_PENDING_BATCHES=8`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`, **`BENCH_FLUSH_CONCURRENCY=8`** (rolled back from 32 in U9d/U9e). ACR builds `cb20` (producer) and `cb21` (silo) both succeeded. Silo log `silo-20260525-095335Z.log` (2.43 MiB) preserved.

**Outcome.** Full recovery and a small net improvement over the U9c baseline. The `[AlwaysInterleave]` change has the side benefit of slightly lifting steady-state throughput because the monitor's sampling RPCs no longer enqueue behind individual `SetManyAsync` calls.

| Run                                  | SteadyAvg | FinalAvgRate | FinalWritten | FinalFailed |
|--------------------------------------|-----------|--------------|--------------|-------------|
| U9c (no `[AlwaysInterleave]`, FC=8)  | n/a       | 2 089/s      | 249 545      | 0           |
| U9d (FC=32)                          | 339/s     | 278/s        | 33 239       | 262 426     |
| U9e (`[AlwaysInterleave]`, FC=32)    | 578/s     | 413/s        | 49 269       | 246 618     |
| **U9f (`[AlwaysInterleave]`, FC=8)** | **2 304/s** | **2 127/s** | **253 619**  | **0**       |

| Instrument (rung-1 final window)        | Shard 0 | Shard 1 | Shard 2 | Shard 3 | Samples (s0/s1/s2/s3) |
|-----------------------------------------|---------|---------|---------|---------|------------------------|
| `provider.phase2.batch_size` P50        | 1.00    | 1.00    | 1.00    | 1.00    | 91 / 91 / 96 / 154     |
| `wal.append.batch_entries` P50          | 16      | 16      | 16      | 16      | 90 / 90 / 95 / 153     |
| `wal.append.provider.duration` (ms) P50 | 25.51   | 24.34   | 23.41   | 22.84   | 91 / 91 / 96 / 154     |
| `wal.append.in_flight`                  | 0       | 0       | 0       | 0       | 90 / 90 / 95 / 153     |

**The 56 reshard rejections are a pre-existing cold-start artifact, not a load defect.** A direct comparison of the U9c and U9f silo logs falsifies the U9e diagnosis sub-claim that the `Unable to create local activation` rejections on `LatticeGrain.ReshardAsync` are a load-induced defect:

| Silo log signal                                                  | U9c (success) | U9d (collapse) | U9e (partial)  | U9f (success)  |
|------------------------------------------------------------------|---------------|----------------|----------------|----------------|
| `[silo] reshard ... -> shardCount=16 (submit)`                   | 54            | many           | (not measured) | 56             |
| `[silo] reshard ... FAILED: ... Unable to create local activation` | 54          | many           | 22             | 56             |
| `Hot-shard sampling pass FAILED`                                 | 0             | many           | 0              | 0              |
| `IngestService.FlushAsync ... TimeoutException`                  | 0             | many           | 499            | 0              |
| `[silo] FINAL ... failed=N`                                      | 0             | 262 426        | 246 618        | 0              |
| `[silo] FINAL ... rate (avg)`                                    | 2 089/s       | 278/s          | 413/s          | 2 127/s        |

U9c and U9f both have the same per-treeId pattern: 50-ish reshard submits → 50-ish rejections, all at cold-start (the very first one is at log line 6 in U9f, before producer load even begins), and *zero* observed effect on throughput or success rate. The rejections happen on the cold-startup path of the tree-scoped `lattice/{treeId}` grain's `ReshardAsync` activation: the hot-shard monitor's startup sampling pass calls `ReshardAsync` before the activation pipeline has converged, the request is forwarded, the target activation cannot be created at that instant, and Orleans rejects the forward. The monitor's retry logic absorbs this transparently. The U9d log showed the same pattern *amplified* by monitor starvation: with `Hot-shard sampling pass FAILED` driving an extra wave of monitor-decided reshards on top of the cold-start wave, the total reshard rate rose proportionally with monitor pressure.

**Re-ranked probe order (fifth revision, the U9e + U9c knob set is the new working baseline).**

- **U9 / U9b / U9c / U9d / U9e** - FALSIFIED or rolled into the working baseline as appropriate (rolled up above).
- **U9f - VALIDATES the U9e `[AlwaysInterleave]` ship** (this smoke). The new working baseline is `flushConcurrency=8` + `[AlwaysInterleave]` on the monitor reads. SteadyAvg lifts modestly from ~2 089/s (U9c) to ~2 304/s (U9f); FinalAvgRate from 2 089/s to 2 127/s. The monitor-starvation cure is independently valuable and the producer-side flush concurrency is conclusively bounded by `shardCount` (16) under the current shard-root single-activation regime.
- **U9g (deferred) - reentrant `ShardRootGrain.SetManyAsync` via `[MayInterleave]`.** Still the right next throughput probe in principle, but the U9f result clarifies the value gate: the **per-shard serial-turn ceiling is the active binding constraint** at `flushConcurrency=8`, since `provider.phase2.batch_size` remains pinned at `1.00` on every shard and `wal.append.in_flight` stays at zero. Making `SetManyAsync` reentrant should let each shard interleave more than one in-flight `AppendAsync` call, which in turn should be the first knob that finally lifts `phase2.batch_size` above 1. The design constraint is still the four mutable caches (`_cachedLeaf`, `_cachedInternal`, routing tables, root state); each must be made interleave-safe (or partitioned per turn by key range) before the change can ship.
- **U9i (deferred behind U9g) - investigate the benign cold-start reshard rejections.** Now that U9f confirms they are noise, the next *correctness* / observability win is to suppress the cold-start reshard wave entirely (e.g. let `HotShardMonitor` wait until the `LatticeGrain` activation has reported "ready" before issuing its first `ReshardAsync`), or to demote the rejection log line so it does not look like a real failure in operator dashboards. This is hygiene only, not a throughput probe.


#### U9g-pre falsification (2026-05-25T10:18Z-T10:35Z) - ramping `BENCH_FLUSH_CONCURRENCY` ∈ {8, 16, 24} on the U9f baseline. Higher flush concurrency makes throughput *worse*, not better. Producer-side fan-out is **not** the binding constraint; the FC=16 silo log directly identifies it as `ShardRootGrain.SetManyAsync` (per-shard, single-activation) - the same per-shard serial turn the original U9g (deferred) probe at L810 named. (FALSIFIED that more flush concurrency helps; CONFIRMED the per-shard `ShardRootGrain` serial turn is the real ceiling.)

**Setup.** Same code as U9e/U9f (commit `4087b4c`, `[AlwaysInterleave]` on `IShardRootGrain.GetHotnessAsync` and `HasPendingBulkOperationAsync`). Same U9f knob set on every probe except `BENCH_FLUSH_CONCURRENCY`: `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4 096`, `BENCH_WAL_PARTITIONS=4`, `BENCH_WAL_MAX_PENDING_BATCHES=8`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`. One 60s rung at 1 000 vehicles * 5 Hz per probe, ACR images reused via `-SkipBuild`. Driven by `benchmark/azure-throughput/scripts/.run-U9g-pre-flushconc-ramp.ps1` (FC=8) and `.run-U9g-pre-resume.ps1` (FC=16, FC=24, after a stray Ctrl+C interrupted the first FC=16 attempt). Manifest at `benchmark/azure-throughput/scripts/.run-U9g-pre-manifest.csv` lists the raw silo log per probe.

**Outcome.** Monotonic collapse with rising flush concurrency. FC=8 reproduces U9f within noise; FC=16 loses half the writes to timeouts; FC=24 loses ~80%.

| Probe                                | SteadyAvg | FinalAvgRate | FinalWritten | FinalFailed | Silo log                          |
|--------------------------------------|-----------|--------------|--------------|-------------|-----------------------------------|
| **U9g-pre FC=8 (U9f re-check)**      | **2 317/s** | **2 109/s** | **251 647** | **0**       | `silo-20260525-101824Z.log`       |
| U9g-pre FC=16                        | 1 460/s   | 988/s        | 119 402      | 114 688     | `silo-20260525-102954Z.log`       |
| U9g-pre FC=24                        | 748/s     | 477/s        | 56 928       | 241 664     | `silo-20260525-103301Z.log`       |

**Failure mode (smoking gun).** The FC=16 silo log (`silo-20260525-102954Z.log` L4562-L4565) contains three consecutive `CallbackData[100157]` timeout diagnostics that pin the bottleneck to *per-shard* `ShardRootGrain.SetManyAsync`, not the upstream `LatticeGrain`. The lattice-grain diagnostic (L4562) shows `NumRunning=1, NonReentrancyQueueSize=0, IdlenessTimeSpan=00:00:27.687` - one in-flight call, **no queued calls**, and the grain is `WorkGroupStatus=Waiting` with `QueuedWorkItems=0`. That is the shape of a grain blocked at `await Task.WhenAll(shardTasks)`, not of a saturated turn. The two `ShardRootGrain` diagnostics on the very next lines (L4563 `shardroot/.../50`, L4564 `shardroot/.../35`) instead show `NumRunning=1, NonReentrancyQueueSize=16, IdlenessTimeSpan=00:00:01.629`, with the outstanding `IShardRootGrain.SetManyAsync` reported as `has been enqueued on the target grain for 00:00:27.687 and is currently position 1 in queue for processing`. `NonReentrancyQueueSize=16` is the producer's full flush-concurrency window queued behind a single in-flight shard-root call. At `shardCount=16` + `FC=16`, every flush slot independently routes to the same per-shard activation and queues, blowing past Orleans' 30 s response-timeout before the queue drains; at FC=24 the per-shard queue depth grows further and the failure rate grows with it. Per-shard `ShardRootGrain` (single-activation, default non-reentrant) is therefore the active serial-turn ceiling, exactly as the original U9g (deferred) probe at L810 hypothesised.

**Confirmed binding constraint.** The U9f working baseline's serial turn is on `ShardRootGrain.SetManyAsync` (per-shard, `shardCount=16` activations), as U9g (deferred) at L810 already predicted. `LatticeGrain` is a `[StatelessWorker(maxLocalWorkers: 32)]` router with parallel `Task.WhenAll` fan-out across shards; its turn time is dominated by the *slowest* outbound `IShardRootGrain.SetManyAsync` await, not by any router-local work. The U9f working point at FC=8 is the largest flush concurrency that keeps each per-shard `ShardRootGrain` queue at ~1 in-flight call without pushing the second queued call past the 30 s response timeout; FC>8 multiplies the per-shard queue depth by FC/8 and starts timing out batches at the producer.

**Re-ranked probe order (sixth revision; U9g (deferred) at L810 is confirmed and promoted to next).**

- **U9 / U9b / U9c / U9d / U9e / U9f** - rolled up above; U9f remains the current shipping baseline.
- **U9g-pre - FALSIFIES the "raise flush concurrency unlocks more throughput at FC=8" hypothesis** *and* simultaneously **CONFIRMS the original U9g (deferred) diagnosis at L810**: the binding constraint is per-shard `ShardRootGrain.SetManyAsync` single-activation serial-turn pressure, evidenced by `NonReentrancyQueueSize=16` on the FC=16 shard-root activations while the upstream `LatticeGrain` activation is parked at `QueuedWorkItems=0` awaiting `Task.WhenAll` across shards. No re-attribution is needed; the original probe order is restored.
- **U9g (promoted, the next throughput probe) - reentrant `ShardRootGrain.SetManyAsync` via `[MayInterleave]` with disjoint-key key-set partitioning.** This is the U9g (deferred) item at L810, now unblocked by the U9g-pre evidence. The producer is already pushing FC=8 per-shard calls into the same activation and the queue is the bottleneck; making the turn interleave-safe should let each shard pipeline more than one in-flight `SetManyAsync`. The design constraint remains the four mutable caches (`_cachedLeaf`, `_cachedInternal`, routing tables, root state); each must be made interleave-safe (or partitioned per turn by key range) before the change can ship.
- **U9i (deferred behind U9h) - investigate the benign cold-start reshard rejections.** Unchanged from L811: still hygiene, not a throughput probe.


#### U9g result (2026-05-25T12:06Z-T12:18Z) - shipping `[AlwaysInterleave]` on `IShardRootGrain.SetManyAsync` doubles FC=16 throughput but introduces 7 011 etag-mismatch warnings on the shard-root state. The U9g (deferred) hypothesis is confirmed for *turn scheduling*, but the same hot path that the XML doc on `SetManyAsync` named (`PromoteRootAsync` and `MarkLeafDirtyAsync` writing through the same `[PersistentState]` instance from concurrent turns) now races on the etag. **Net result: not shippable as-is** - the interleave does unblock FC=16 throughput, but at the cost of silent state-rollback warnings on every shard. (CONFIRMED that interleaving the turn is the right direction; FALSIFIED that the U9g XML-doc-named hazards are theoretical.)

**Setup.** Mirror of the U9g-pre knob set on a fresh ACR build: same shape (1 000 vehicles * 5 Hz * 60 s, `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4 096`, `BENCH_WAL_PARTITIONS=4`, `BENCH_WAL_MAX_PENDING_BATCHES=8`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`), but the silo image now carries `[AlwaysInterleave]` on `IShardRootGrain.SetManyAsync` plus the `ConcurrentDictionary`-backed traversal caches. Three probes on the same images, FC in {8, 16, 24}. Driver: `benchmark/azure-throughput/scripts/.run-U9g-driver.ps1`; manifest: `benchmark/azure-throughput/scripts/.run-U9g-manifest.csv`.

**Outcome.** FC=16 doubles its U9g-pre throughput and drops failures by ~88 %. FC=8 mildly regresses with 8 489 new producer-side failures (vs zero in U9f). FC=24 remains over-pressure.

| Probe                                | SteadyAvg | FinalAvgRate | FinalWritten | FinalFailed | Silo log                          |
|--------------------------------------|-----------|--------------|--------------|-------------|-----------------------------------|
| U9g-pre FC=8 (U9f re-check)          | 2 317/s   | 2 109/s      | 251 647      | 0           | `silo-20260525-101824Z.log`       |
| U9g     FC=8                         | 2 030/s   | 1 961/s      | 233 905      | 8 489       | `silo-20260525-121113Z.log`       |
| U9g-pre FC=16                        | 1 460/s   | 988/s        | 119 402      | 114 688     | `silo-20260525-102954Z.log`       |
| **U9g     FC=16**                    | **2 108/s** | **2 047/s** | **244 664**  | **13 880**  | `silo-20260525-121346Z.log`       |
| U9g-pre FC=24                        | 748/s     | 477/s        | 56 928       | 241 664     | `silo-20260525-103301Z.log`       |
| U9g     FC=24                        | 707/s     | 623/s        | 74 685       | 210 687     | `silo-20260525-121646Z.log`       |

**Failure mode (smoking gun).** The FC=8 silo log shows **7 011 `Etag mismatch during Update for grain shardroot/shardroot/<treeId>/<shardIndex>`** warnings from `Orleans.Storage.MemoryStorageGrain`, distributed across every shard (41-246 per shard). The pattern is uniformly `Expected = <hex>, Received = (null)` - i.e. two concurrent turns on the same activation both called `state.WriteStateAsync()` and the second one found its etag had been bumped under it. This is exactly the hazard the U9g `[AlwaysInterleave]` XML doc named. The U9g-pre baseline (with `SetManyAsync` strictly non-reentrant) has zero such warnings.

**U9h audit - every shard-root `WriteStateAsync` call site, by hot-path classification.** Hot path = "callable during a concurrent `SetManyAsync` turn on the same activation." Admin path = "callable only from out-of-band controllers (split coordinator, bulk-load coordinator, shadow-forward coordinator, compaction)."

| File                                 | Line | Enclosing method                  | Hot path? | What is mutated                                                                                  |
|--------------------------------------|------|-----------------------------------|-----------|--------------------------------------------------------------------------------------------------|
| `ShardRootGrain.cs`                  | 953  | `EnsureRootAsync`                 | first-touch only | `IsRegistered`, `RootNodeId`, `RootIsLeaf` - set-once monotone                            |
| `ShardRootGrain.Traversal.cs`        | 677  | `PromoteRootAsync`                | rare (split bubble) | `PendingPromotion`, `PendingPromotionRootWasLeaf` - set->clear                          |
| `ShardRootGrain.Traversal.cs`        | 706  | `CompletePromotionAsync`          | rare (split bubble) | `RootNodeId`, `RootIsLeaf`, `PendingPromotion` - cleared after new root wired up        |
| `ShardRootGrain.DirtyLeaves.cs`      | 88   | `MarkLeafDirtyAsync`              | **hot**   | `DirtyLeavesSinceLastCompaction[leafId] = hlc` - first-call-per-leaf-per-window only             |
| `ShardRootGrain.DirtyLeaves.cs`      | 174  | `ClearDirtyLeavesUpToAsync`       | admin     | `DirtyLeavesSinceLastCompaction` prunes <= `LastDirtyAdvance`                                    |
| `ShardRootGrain.Lifecycle.cs`        | 21   | `MarkDeletedAsync`                | admin     | `IsDeleted = true` - set-once monotone                                                           |
| `ShardRootGrain.Lifecycle.cs`        | 41   | `UnmarkDeletedAsync`              | admin     | `IsDeleted = false`                                                                              |
| `ShardRootGrain.Split.cs`            | 98   | `BeginSplitAsync`                 | admin     | `SplitInProgress` Begin->Drain                                                                   |
| `ShardRootGrain.Split.cs`            | 111  | `EnterRejectPhaseAsync`           | admin     | `SplitInProgress` ->Reject                                                                       |
| `ShardRootGrain.Split.cs`            | 135  | `CompleteSplitAsync`              | admin     | `MovedAwaySlots` += sip.MovedSlots, `MovedAwayVirtualShardCount`, `SplitInProgress = null`       |
| `ShardRootGrain.ShadowForward.cs`    | 134  | `BeginShadowForwardAsync`         | admin     | `ShadowForward` Begin                                                                            |
| `ShardRootGrain.ShadowForward.cs`    | 156  | `MarkDrainedAsync`                | admin     | `ShadowForward` ->Drained                                                                        |
| `ShardRootGrain.ShadowForward.cs`    | 177  | `EnterRejectingAsync`             | admin     | `ShadowForward` ->Rejecting                                                                      |
| `ShardRootGrain.ShadowForward.cs`    | 194  | `ClearShadowForwardAsync`         | admin     | `ShadowForward = null`                                                                           |
| `ShardRootGrain.BulkLoad.cs`         | 146  | `FinalizeBulkLoadTreeAsync`       | admin     | `RootNodeId`, `RootIsLeaf`, `IsRegistered`, `LastCompletedBulkOperationId`                       |
| `ShardRootGrain.BulkLoad.cs`         | 194  | `FinalizeBulkLoadTreeAsync`       | admin     | same as above                                                                                    |
| `ShardRootGrain.BulkLoad.cs`         | 247  | `BulkAppendAsync`                 | admin     | `IsRegistered`, `LastCompletedBulkOperationId`                                                   |
| `ShardRootGrain.BulkLoad.cs`         | 292  | `BulkAppendAsync`                 | admin     | `PendingBulkGraft` set                                                                           |
| `ShardRootGrain.BulkLoad.cs`         | 387  | `CompleteBulkGraftAsync`          | admin     | `PendingBulkGraft = null`, `LastCompletedBulkOperationId`                                        |

**Classification.** 19 sites total. **One** is on the *hot* SetMany path (`MarkLeafDirtyAsync` L88). **Two** are *rare* on the SetMany path (`PromoteRootAsync` L677 + `CompletePromotionAsync` L706 - both gated on a split bubbling up to the root, which is shape-dependent and not driven by every `SetManyAsync`). **One** is *first-touch only* (`EnsureRootAsync` L953 - runs at most once per activation). The remaining 15 sites are admin paths that cannot collide with a concurrent `SetManyAsync` turn unless the operator drives them concurrently with bulk writes, which the bench harness does not. The 7 011 etag mismatches in the U9g FC=8 log are therefore attributable to (a) `MarkLeafDirtyAsync` racing other turns' `MarkLeafDirtyAsync` calls within the same activation (dominant case for `Received = (null)`) and (b) `PromoteRootAsync`/`CompletePromotionAsync` racing concurrent `MarkLeafDirtyAsync` during a split bubble.

**Re-ranked probe order (seventh revision; U9h promoted as the next probe).**

- **U9 / U9b / U9c / U9d / U9e / U9f / U9g-pre** - rolled up above; U9f remains the current shipping baseline.
- **U9g - PARTIAL SUCCESS** (this entry). Interleaving `SetManyAsync` confirms the U9g (deferred) prediction at L810 - FC=16 throughput recovers - but introduces a state-write race on the shard-root `[PersistentState]` that the U9g XML doc already named as a hazard. Not shippable until U9h closes that race.
- **U9h-A - SHIPPED.** Per-activation `SemaphoreSlim` around every shard-root `state.WriteStateAsync()` call (19 sites, 7 files; helper `WriteShardStateAsync()` in `ShardRootGrain.cs`). The gate serialises only the storage I/O, leaving the surrounding compute interleaved. Validated against real Azure Tables in the U9h-A ladder below: FC=16 etag mismatches 7 011 -> 0, throughput preserved.
- **U9h-B - SHIPPED.** Replaced the synchronous `MarkLeafDirtyAsync` write with an in-memory max-merge into `state.State.DirtyLeavesSinceLastCompaction` plus a coalesced flush (timer cadence `DirtyLeafFlushIntervalMs`, default 50 ms; also drained by `ClearDirtyLeavesUpToAsync` and on deactivation). `DeleteAsync`/`DeleteRangeAsync` no longer touch storage on the hot path. Implementation in `src/lattice/BPlusTree/Grains/ShardRootGrain.DirtyLeaves.cs`; option plumbed through `LatticeOptions` and `LatticeOptionsResolver`; new regression tests in `ShardRootGrainDirtyLeavesTests.cs` assert zero synchronous writes from `DeleteAsync` and single-write flush semantics.
- **U9h-C - SHIPPED.** Annotated the pure read methods `GetAsync`, `ExistsAsync`, and `GetManyAsync` on `IShardRootGrain` with `[AlwaysInterleave]` so they overlap with in-flight mutating turns on the same activation. The annotations are pinned by a reflection-based contract test (`ShardRootGrainInterleavedReadsTests.cs`). Mutating reads (`GetWithVersionAsync`, `GetRawEntryAsync`, `GetRawEntriesAsync`) remain serial.
- **U9h-D - SHIPPED (falsifier hit at FC=24).** Full FC sweep on the U9h-B/C silo image at rung `1000:5`, FC in {8, 16, 24}. FC=8 -> 2 369/s steady, 257 717 written, 0 failed, 0 etag mismatches. FC=16 (re-using the U9h-B/C run) -> 2 121/s steady, 233 245 written, 0 failed, 0 etag mismatches. FC=24 collapses: producer drives 4 910/s but the silo's `TcpIngestService` starts failing every flush with `System.TimeoutException: Response did not arrive on time in 00:00:30` from t=63 s onwards, with `OrleansMessageRejectionException: Forwarding failed` on the reshard path; throughput pins at 0/s with `inFlight=24` and `failed=4 096` repeating, only 65 234 written. The U9h-A FC=24 failure mode survives U9h-B+U9h-C, which falsifies the U9h-D hypothesis: removing the dirty-leaf hot-path write and interleaving pure reads is **not** sufficient to unblock FC=24. FC=32 was therefore not run. The new ceiling is upstream of shard-root I/O - specifically Orleans request-timeout backpressure on the ingest path, not WAL provider commit latency.
- **U9j - SHIPPED (global silo ceiling confirmed at ~2 370/s).** Rung ladder at FC=8 on the U9h-B/C silo image: rung 1 (`5000:5`) holds steady at **2 370/s, 258 456 written, 0 failed, 0 etag mismatches** - dead-equal to the FC=8 `1000:5` result (2 369/s) despite 5x the producer pressure. Rung 2 (`10000:5`) collapses from t=1 s with the same `TcpIngestService` 30 s response-timeout shape as U9h-D FC=24, finishing at 20 written / 156 090 producer failures / 0 etag mismatches. The hypothesis ("break the 4-active-shards ceiling by raising the rung") was *falsified*: rung-1 phaseA shows only 4 shards active out of 16 (`shard=0..3`) at 5 000 vehicles, so increasing the rung does not change the shard fan-out, and rung-2 hits the *upstream* ingest-pipeline ceiling before the silo can engage. The bound is therefore a **single global ceiling at ~2 370/s** on the U9h-B/C image, not a per-shard ceiling x active-shards model.
- **U9k - the next probe.** Attack the global ceiling by addressing the two upstream candidates U9j surfaced: (i) **shard fan-out** - only 4 of 16 shards take traffic regardless of rung, so the producer-side `BENCH_SHARD_COUNT` knob is either not reaching the routing layer or the producer is hashing into a narrower keyspace than the silo expects; (ii) **batch coalescing** - rung-1 phaseA shows `wal.append.batch_entries p50=16, max=27` against `BENCH_BATCH_SIZE=4096`, with a saw-tooth `4092, 0, 4092, 0, ...` per-second pattern, meaning each active shard processes one tiny burst then idles. WAL provider is *not* the bottleneck (`wal.append.in_flight=0`, `provider.duration p99 ~67 ms`), so U9k starts with shard fan-out instrumentation (per-shard write counts in the phaseA logs at rung 5000:5) before touching code.


#### U9h-A result (2026-05-25T13:17Z-T13:23Z) - per-activation `SemaphoreSlim` around every shard-root `WriteStateAsync()` eliminates the etag race without regressing FC=16 throughput (VALIDATED, U9h-A shipped)

**Setup.** Same shape as U9g (1 000 vehicles * 5 Hz * 60 s, `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4 096`, `BENCH_WAL_PARTITIONS=4`, `BENCH_WAL_MAX_PENDING_BATCHES=8`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`), but the silo image now carries the per-activation storage-write gate from the U9h audit: every `state.WriteStateAsync()` call across the seven `ShardRootGrain` partials routes through `WriteShardStateAsync()` (helper in `ShardRootGrain.cs`), serialising the storage I/O while leaving the surrounding compute interleaved. Three probes on the same image, FC in {8, 16, 24}.

**Outcome.** FC=8 and FC=16 both run **zero etag mismatches** (vs 7 011 in U9g) and **zero producer failures** (vs 8 489 and 13 880 in U9g respectively). FC=16 retains the U9g throughput recovery (~2 100/s steady avg). FC=24 remains over-pressure - the producer drives 4 910/s but the silo stops accepting traffic at t=70 s with 4 096 in-flight in failure, exactly as in U9g. The gate fixes the *race*, not the *over-pressure*: at FC=24 the per-shard activations queue 24-31 concurrent `SetManyAsync` turns ahead of every storage write and Orleans' 30 s response timeout fires before the semaphore drains. The gate is also not the bottleneck on FC=24 (`NonReentrancyQueueSize=0`); the symptom is the same upstream over-pressure that U9g-pre FC=24 hit.

| Probe          | SteadyAvg | FinalAvgRate | FinalWritten | FinalFailed | EtagMismatches | Silo log                          |
|----------------|-----------|--------------|--------------|-------------|----------------|-----------------------------------|
| U9h-A FC=8     | 1 969/s   | 1 960/s      | 233 462      | 0           | **0**          | `silo-20260525-131704Z.log`     |
| **U9h-A FC=16**| **2 189/s** | **2 152/s** | **256 806**  | **0**       | **0**          | `silo-20260525-132004Z.log`     |
| U9h-A FC=24    | n/a       | n/a          | 88 492       | 4 096       | 0              | `silo-20260525-132302Z.log`     |

**Net result: shippable.** Tier A of the U9h plan (the storage-write gate) closes the race that U9g introduced. FC=16 throughput is preserved (2 152/s vs 2 047/s in U9g - same ladder, within noise) and the FC=16 etag mismatches that motivated U9h drop to zero. FC=24 still over-pressures, but that is a separate bound (per-shard turn pipeline depth, not storage I/O) and is the U9h-B/U9h-C scope.
#### U9h-B / U9h-C result (2026-05-25T14:11Z) - coalesced dirty-leaf flush and AlwaysInterleave on pure reads preserve U9h-A throughput at zero races (VALIDATED, U9h-B and U9h-C shipped)

**Setup.** Same probe shape as U9h-A FC=16 (1 000 vehicles * 5 Hz * 60 s, `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4 096`, `BENCH_WAL_PARTITIONS=4`, `BENCH_WAL_MAX_PENDING_BATCHES=8`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`, `BENCH_FLUSH_CONCURRENCY=16`), single rung (`-Rungs 1000:5 -DurationSec 60`). The silo image now also carries (a) the U9h-B coalesced dirty-leaf flush (`MarkLeafDirtyAsync` max-merges into shard-root state and a 50 ms timer / deactivation drain owns the storage write, `ClearDirtyLeavesUpToAsync` flushes opportunistically) and (b) the U9h-C `[AlwaysInterleave]` annotations on `IShardRootGrain.GetAsync` / `ExistsAsync` / `GetManyAsync`.

**Outcome.** Producer drives 5 000/s, silo absorbs **2 121/s steady avg** (1 955/s final avg over 119.3 s), **233 245 entries written**, **0 failed**, **0 etag mismatches**, and zero `InconsistentStateException` in `silo-20260525-141151Z.log`. Throughput sits inside the U9h-A FC=16 noise band (U9h-A: 2 189/s steady / 2 152/s final; U9h-B+C: 2 121/s steady / 1 955/s final). Phase A instruments confirm WAL still dominates: per-shard `provider.commit.duration` phase1 p99 ~67-95 ms and `wal.append.in_flight` flat at 0 - i.e. the dirty-leaf write is no longer in the critical path and the shard root is not the rate-limiter on this rung.

| Probe                       | SteadyAvg | FinalAvgRate | FinalWritten | FinalFailed | EtagMismatches | Silo log                          |
|-----------------------------|-----------|--------------|--------------|-------------|----------------|-----------------------------------|
| U9h-A FC=16 (reference)     | 2 189/s   | 2 152/s      | 256 806      | 0           | 0              | `silo-20260525-132004Z.log`     |
| **U9h-B+C FC=16**           | **2 121/s** | **1 955/s** | **233 245**  | **0**       | **0**          | `silo-20260525-141151Z.log`     |

**Net result: shippable.** U9h-B removes the hot-path dirty-leaf write entirely and U9h-C lets pure reads overlap with mutating turns on the same activation, both without regressing the U9h-A FC=16 ladder and without reintroducing the U9g etag race. The remaining FC=16 ceiling is now provider-side WAL commit latency (`provider.commit.duration` phase1 p99 ~95 ms on `azure-throughput-20260525-140643`), not shard-root I/O or turn-queue depth, which is the next probe target.

#### U9h-D result (2026-05-25T15:04Z-T15:08Z) - FC sweep on the U9h-B/C silo image confirms FC=8 best, FC=16 noise-equivalent, FC=24 still collapses (FALSIFIER HIT, U9h-D shipped as a negative result)

**Setup.** Same probe shape as U9h-B/C (1 000 vehicles * 5 Hz * 60 s, `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4 096`, `BENCH_WAL_PARTITIONS=4`, `BENCH_WAL_MAX_PENDING_BATCHES=8`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`) with FC in {8, 16, 24} on the U9h-B/C silo image (no rebuild between FCs, `-SkipBuild`). Per-FC silo logs and ladder CSVs snapshotted under `benchmark/azure-throughput/scripts/.u9h-d-sweep/`. FC=16 is re-used from the U9h-B/C run (`silo-20260525-141151Z.log`). FC=32 was not run after FC=24 collapsed.

**Outcome.** FC=8 wins the sweep, FC=16 sits inside the FC=8 noise band, FC=24 collapses with the U9h-A FC=24 failure shape intact - which falsifies the U9h-D hypothesis that U9h-B+U9h-C would lift the per-shard turn pipeline depth ceiling.

| Probe              | SteadyAvg | FinalAvgRate | FinalWritten | FinalFailed | EtagMismatches | Silo log                                          |
|--------------------|-----------|--------------|--------------|-------------|----------------|---------------------------------------------------|
| **U9h-D FC=8**     | **2 369/s** | **2 158/s**  | **257 717**  | **0**       | **0**          | `.u9h-d-sweep/silo-fc8.log` (`silo-20260525-150417Z.log`) |
| U9h-D FC=16 (re-used U9h-B/C) | 2 121/s | 1 955/s | 233 245 | 0 | 0 | `silo-20260525-141151Z.log`                       |
| U9h-D FC=24 (collapse) | n/a   | n/a          | 65 234       | 4 096 (in-flight, stuck) | 0  | `.u9h-d-sweep/silo-fc24.log` (`silo-20260525-150809Z.log`) |

**Failure-mode detail (FC=24).** The silo's `TcpIngestService` starts emitting `flush of N failed System.TimeoutException: Response did not arrive on time in 00:00:30` at t=63 s; the reshard-driving call surfaces one `OrleansMessageRejectionException: Forwarding failed`. From t=69 s onwards every 1 s window reports `Entries written per second = 0` with `inFlight = 24` and a repeating `failed = 4 096`. Notably **EtagMismatches stays at 0** - the U9h-A storage-write gate is still doing its job. The bound is therefore *not* a shard-root race and *not* WAL provider commit latency; it is Orleans request-timeout backpressure on the ingest pipeline once more than ~16 concurrent flushes are queued ahead of the shard-root activation. U9h-B (dirty-leaf coalescing) and U9h-C (`[AlwaysInterleave]` on pure reads) do not move this bound.

**Net result: U9h-D ships as a negative result.** FC=8 becomes the new default working set for the next probe (U9j), since it is provably the throughput-best FC on the U9h-B/C image and survives without failure or race. FC>=24 remains blocked behind a separate probe (provisionally U9k) that has to address ingest-side Orleans response-timeout pressure rather than shard-root I/O or WAL commit latency. The next concrete step is U9j: raise the rung at FC=8 to engage more than 4 active shards and break the ~2 369/s `1000:5` ceiling.

#### U9j result (2026-05-25T15:18Z-T15:25Z; corrected 2026-05-25 by U9k step 1) - rung ladder at FC=8 reveals a single global silo ceiling at ~2 370/s, but the original "4 of 16 shards active" reading was a misread: `shard=` in phaseA is the WAL partition index, and `BENCH_SHARD_COUNT=16` never actually took effect because the startup reshard silently fails on a first-call directory-cache race (FALSIFIER HIT, U9j shipped as a negative result; U9k corrects the harness)

**Setup.** Same knobs as U9h-D FC=8 (`BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4 096`, `BENCH_WAL_PARTITIONS=4`, `BENCH_WAL_MAX_PENDING_BATCHES=8`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`, `BENCH_FLUSH_CONCURRENCY=8`) but two rungs: `5000:5` (5 000 vehicles x 5 Hz, target 25 000/s) and `10000:5` (10 000 vehicles x 5 Hz, target 50 000/s), 60 s steady per rung. Silo image unchanged (still U9h-B/C). Per-rung silo logs and ladder CSVs snapshotted under `benchmark/azure-throughput/scripts/.u9j-rung-sweep/`.

**Outcome.**

| Rung               | Vehicles | Target    | SteadyAvg | FinalWritten | FinalFailed | EtagMismatches | Silo log                                                          |
|--------------------|----------|-----------|-----------|--------------|-------------|----------------|-------------------------------------------------------------------|
| **U9j 5000:5**     | 5 000    | 25 000/s  | **2 370/s** | **258 456**  | **0**       | **0**          | `.u9j-rung-sweep/silo-rung1-5000h5.log` (`silo-20260525-151913Z.log`) |
| U9j 10000:5 (collapse) | 10 000 | 50 000/s | n/a       | 20           | 156 090     | 0              | `.u9j-rung-sweep/silo-rung2-10000h5.log` (`silo-20260525-152222Z.log`) |

**Per-WAL-partition fan-out (rung 1 phaseA, last window per metric).** All four configured WAL partitions take traffic at 5 000 vehicles: `wal.append.batch_entries` reports `count` and `sum` for `shard=0` (count=242, sum=3 864 entries), `shard=1` (count=262, sum=4 182), `shard=2` (count=305, sum=4 881), and `shard=3` (count=242, sum=3 883). Per-partition batch entries are tiny: `p50=16, p90=20-22, max=27` (against `BENCH_BATCH_SIZE=4 096`). The WAL provider is idle: `wal.append.in_flight = 0` on every partition, `provider.duration p50=23 ms / p99=67-76 ms / max=216 ms`. **Important correction (U9k step 1, 2026-05-25):** the `shard=` tag in phaseA is the **WAL partition index**, not the lattice shard index - every active instrument name is `wal.append.*` / `provider.commit.*` (i.e. emitted by `WalShardGrain`, which is partitioned by `BENCH_WAL_PARTITIONS=4`). The original "4 of 16 shards active" reading conflated the two layers. PhaseA has no per-lattice-shard instrument, so the actual lattice-shard fan-out is **unknown from these logs**; see the harness defect below for why it was also misconfigured.

**Steady-state shape (rung 1 last 10 windows).** The throughput trace is a saw-tooth: `... 4092, 0, 4097, 0, 4094, 0, 4097, 0, 4092, 0`. Each active WAL partition drains a 4 096-entry burst, then idles for one full second before the next burst arrives. `inFlight` falls 5 -> 0 across the same 10 s window, confirming the silo finishes all queued work between bursts.

**Rung 2 collapse.** Identical failure shape to U9h-D FC=24: `TcpIngestService` emits `flush of N failed System.TimeoutException: Response did not arrive on time in 00:00:30` starting at the first window, `inFlight` saturates at the FC ceiling within 2 s, and **EtagMismatches stays at 0** (U9h-A storage-write gate still holding). The producer's 10 000 vehicles overrun the ingest pipeline before any shard activation completes its first turn.

**Harness defect uncovered by U9k step 1: `BENCH_SHARD_COUNT=16` never took effect.** Every U9j (and U9h-D) run carries the line `[silo] reshard treeId=... FAILED: OrleansMessageRejectionException: Forwarding failed ... ForwardCount=2 ... "Unable to create local activation" to invalid activation. Rejecting now.` immediately after the reshard submit. No matching `Orleans.Runtime.Catalog` activation exception appears anywhere in the log - this is the well-known Orleans client-directory-cache race against a brand-new `LatticeGrain` activation, which retries-itself-out within a few hundred milliseconds. The bench's `catch (Exception ex)` handler on the submit silently swallows the rejection and the silo continues, leaving the tree pinned at the library default `LatticeConstants.DefaultShardCount = 64` rather than the configured 16. **Every measurement attributed to "16 shards" in U9h-D and U9j was actually on the default 64 shards.**

**Net result: U9j ships as a negative result with a corrected interpretation.** The bound is upstream of the shard root and downstream of the producer, and it is the *same* wall U9h-D hit at FC=24 (`TcpIngestService` 30 s timeout). What U9j *did* validate: WAL partitions saturate exactly as configured (4 of 4), `provider.phase2.batch_size` stays pinned at 1.00 across all 4 partitions even with a 5 ms coalescing window, and the first SetMany dispatch is only `entries=49` against `BatchSize=4 096` (at 2 370 msg/s steady, a 50 ms flush window admits ~119 msgs per flush - the configured `4 096` ceiling is never approached). What U9j did *not* validate, despite the previous interpretation: any claim about how many lattice shards are active. PhaseA cannot answer that question. U9k must first fix the silent reshard failure before any "wider shard count" probe is meaningful.

#### U9k step 1 result (2026-05-25, code change) - reshard retry on the harness so `BENCH_SHARD_COUNT` actually applies

**Setup.** No benchmark run; this is a harness-only code change in `benchmark/azure-throughput/Silo/Program.cs`. Two changes:

1. Replace the single-shot `ReshardAsync(...)` call with a small retry loop (up to 4 attempts, exponential 100/200/400/800 ms back-off) that distinguishes `OrleansMessageRejectionException` (retriable, first-call directory-cache race) from `ArgumentOutOfRangeException` (grow-only / virtual-shard-ceiling violation; not retriable) and from every other exception (logged once and aborts). The `IsReshardCompleteAsync` poll inside the success branch also tolerates a single same-shape rejection without aborting.
2. Add a loud, greppable `[silo] ERROR reshard treeId=... ABORTED after N attempt(s): ...` line on terminal failure that explicitly names the consequence: *"Tree remains at its previously-pinned shard count (likely the library default, NOT shardCount=X)"*. The ladder script doesn't parse this; the human running the experiment can grep for it.

**Why it had to land before any further measurement.** Without this, every "shard count" knob in the bench is effectively no-op: the first-call rejection is benign (Orleans re-establishes the directory address on the next invocation), but a one-shot caller never makes that next invocation. With it, the configured `BENCH_SHARD_COUNT` is what the silo actually measures.

**Validation.** `dotnet build benchmark/azure-throughput/Silo/...csproj -c Debug` succeeds, 0 warnings, 0 errors. No lattice-library code was touched. The change is observable only on the first-attempt rejection path; runs whose first attempt already succeeds emit one extra line (`attempt=1/4`) and are otherwise identical.

**Next probe (U9k step 2).** Re-run U9j rung 1 (`5000:5` x 60 s, FC=8) with `BENCH_SHARD_COUNT=16` actually applied (i.e. the silo log must show `[silo] reshard treeId=... complete`, not the swallowed rejection). The discriminating outcome is whether the steady-state rate moves off ~2 370/s when the tree genuinely has 16 lattice shards instead of the silently-defaulted 64. **Both outcomes are informative:**

- If throughput climbs, the previous "ceiling" was an artifact of too-many-small-shards combined with the saw-tooth ingest cadence (each of 64 shards getting ~37 entries/s averaged into 16-entry batches), and the next probe is to sweep `BENCH_SHARD_COUNT` ∈ {4, 8, 16, 32} to find the sweet spot.
- If throughput stays at ~2 370/s, the lattice-shard count is not the binding axis and the next probe shifts to the ingest cadence (50 ms flush window vs producer's 200 ms inter-tick), because U9j has now shown that `provider.phase2.batch_size` is pinned at 1.00 even with a 5 ms coalescing window and the very first SetMany dispatch carries only 49 entries against a 4 096 ceiling. That makes the producer/flush interaction the next causal candidate.



#### U9k step 2 result (2026-05-25T19:27Z) - inverse `InvalidCastException` on root promotion under `[AlwaysInterleave]`, fixed by gating + root-shape revalidation; FC=8 rung now sustains a clean 60 s run

**Setup.** Same harness as U9j rung 1 with the U9k step 1 reshard retry in effect: `VehicleCount=5000`, `TickHz=5`, `DurationSec=60`, `BENCH_BATCH_SIZE=4096`, `BENCH_FLUSH_INTERVAL_MS=50`, `BENCH_FLUSH_CONCURRENCY=8`, `BENCH_WAL_PARTITIONS=8`, `BENCH_WAL_PENDING=8`, `pipelinePhase2=True`. Silo image rebuilt with the lattice library fix described below.

**Initial finding (before the fix).** The first U9k step 2 attempt died on a flood of `TcpIngestService` `flush of NNN failed` warnings caused by `System.InvalidCastException: Unable to cast object of type 'Orleans.Lattice.BPlusTree.Grains.BPlusInternalGrain' to type 'Orleans.Lattice.BPlusTree.IBPlusLeafGrain'` (the inverse direction of the U9k-pre crash, which read `BPlusLeafGrain` -> `IBPlusInternalGrain`). Stack rooted at `BPlusInternalGrain.SeedChildParentAsync` -> `BPlusInternalGrain.InitializeAsync` -> `ShardRootGrain.CompletePromotionAsync` -> `ShardRootGrain.ResumePendingPromotionAsync` -> `PrepareForOperationSlowAsync` -> `SetManyAsync`. Mechanism: under `[AlwaysInterleave]` on `IShardRootGrain.SetManyAsync`, two concurrent shard-root turns could both reach `PromoteRootAsync`; turn A flipped `RootIsLeaf=false` and installed the new internal root, while turn B (already past the `RootIsLeaf` read) persisted its own `PendingPromotion` with the now-stale `PendingPromotionRootWasLeaf=true`. When turn B's resumed `CompletePromotionAsync` then wrapped a second new root *above* the already-promoted internal root, it passed the existing internal grain as one of the new root's children while seeding it with `childrenAreLeaves=true`, so the downstream `SeedChildParentAsync` cast to `IBPlusLeafGrain` failed against the internal target.

**Fix landed in the lattice library** (no harness change):

1. `SplitResult` gained an immutable `[Id(2)] public bool ChildIsLeaf { get; init; }`. Every producer of a `SplitResult` (leaf split, internal split, bulk-load graft) now stamps the correct level at construction time, so the promotion sequence never has to infer it from racy shard-root scalars.
2. `ShardRootGrain` gained a `_promotionGate` `SemaphoreSlim` that serialises the entire promotion sequence (Phase 1 persist intent + Phase 2 create new root + clear intent) against other interleaved `SetManyAsync` turns. `PromoteRootAsync` enters the gate before touching `RootIsLeaf` / `RootNodeId`, and `ResumePendingPromotionAsync` re-checks `PendingPromotion` under the gate so two `PrepareForOperationSlowAsync` turns cannot both replay the same resume.
3. `PromoteRootAsync` and `CompletePromotionAsync` now re-validate the live root shape under the gate. If `!RootIsLeaf` and the current root's `ChildrenAreLeaves` matches the bubble's `ChildIsLeaf`, the bubble is routed through the existing root via `AcceptSplitAsync` instead of wrapping a second new root above it. On a shape mismatch (the legitimately-unreachable case), the stale `PendingPromotion` is dropped with a warning and the surrounding write retry envelope re-routes the user mutation against the live topology. `CompletePromotionAsync` prefers `pending.ChildIsLeaf` over the legacy `PendingPromotionRootWasLeaf` scalar (which is retained on disk only for backward compatibility with state persisted by a pre-fix activation).

**Regression coverage.** `test/lattice/BPlusTree/Grains/ShardRootGrainPromotionChildTypeTests.cs` pins all three legs deterministically: `Resume_routes_via_AcceptSplitAsync_when_root_already_promoted` (already-promoted root: bubble must enter via `AcceptSplitAsync`, no second `InitializeAsync`), `Resume_falls_back_to_PendingPromotionRootWasLeaf_when_SplitResult_is_pre_fix_state` (legacy compatibility when persisted `ChildIsLeaf` deserialises to default `false`), and `Resume_drops_stale_pending_when_root_level_mismatches_bubble` (shape mismatch drops the stale intent and emits no `InitializeAsync` / `AcceptSplitAsync`). All three pass under `--blame-hang --blame-hang-timeout 30s` in 136 ms total.

**Measured outcome on the re-run (`silo-20260525-192712Z.log`).** Zero `InvalidCastException`, zero `flush of N failed`, zero `warn:` lines, zero `fail:` lines. `FINAL written=293,390 failed=0 elapsed=119.5s` `Entries written per second (avg)=2,454`. Steady min/avg/max = `0 / 3,026 / 8,834` per-second (the steady min of 0 is the first sample before the producer started flushing; the avg over the full 60 s steady-state window is `3,026/s`). Compared to U9j rung 1 (`~2,370/s` on silently-defaulted 64 lattice shards), the configured `BENCH_SHARD_COUNT=16` rung now sustains a clean 60 s window at a comparable rate - the harness-defect interpretation in U9k step 1 is upheld: lattice shard count is not the binding axis in this regime.

**PhaseA per-WAL-partition view (rung 1, last window per tuple).** All 8 configured WAL partitions take traffic; `wal.append.batch_entries` `count` ranges 220-247 per partition with `sum` 3 100-3 900 entries; `wal.append.in_flight = 0` on every partition; `wal.append.provider.duration` `p50 = 17.5 ms / p99 = 31.6 ms / max = 31.6 ms` (vs. U9j rung 1 `p50 = 23 ms / p99 = 67-76 ms / max = 216 ms` on 4 partitions). Doubling `BENCH_WAL_PARTITIONS` from 4 to 8 halved the provider P99 against the real Azure Tables backend, consistent with the real-Azure validation candidate logged under "NEXT" in the *Plan Steps* below: the provider scales with partition keys when not bottlenecked on a single-process backend like Azurite.

**Net result.** U9k ships green: the inverse cast crash is gone, `BENCH_SHARD_COUNT=16` is honoured, and the 5000:5 rung produces a clean baseline against which the next discriminating sweep can be measured. The U9k step 2 question ("does throughput move off ~2 370/s when the tree genuinely has 16 lattice shards instead of the silently-defaulted 64?") resolves to *no* on this single rung: the binding axis is not the lattice shard count. The next probe is the second branch already drafted in U9k step 1: the ingest cadence / flush window vs producer inter-tick interaction, because `provider.phase2.batch_size` is still pinned at 1.00 across all 8 partitions and the first SetMany dispatch is only `entries=83` against `BatchSize=4 096`. Concretely: sweep `BENCH_FLUSH_INTERVAL_MS` ∈ {50, 100, 200, 400} at the same FC=8, WP=8 setting and observe whether widening the flush window lifts `provider.phase2.batch_size` off 1.00 and `entries=83` toward the configured ceiling. **Both outcomes are informative:** if batch_size rises and throughput climbs, the binding axis is the producer/flush race; if batch_size stays pinned, the bottleneck is downstream of the producer flush and the next probe shifts to leaf-side commit concurrency.

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
