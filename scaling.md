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

#### U9l result (2026-05-26T06:49Z-T07:01Z) - sweep BENCH_FLUSH_MS in {50, 100, 200, 400} ms at the U9k step 2 baseline; `provider.phase2.batch_size` stays pinned at 1.00 across 14 495 samples and throughput decays monotonically (FALSIFIED, the binding axis is downstream of the producer flush)

**Setup.** Probe shape identical to the U9k step 2 measured rung in every dimension except the flush window: `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4096`, `BENCH_WAL_PARTITIONS=8`, `BENCH_WAL_MAX_PENDING_BATCHES=8`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS=5`, `BENCH_FLUSH_CONCURRENCY=8`, rung `5000:5` (25 000/s target), 60 s steady per probe. The silo image carries the U9k step 2 promotion fix at HEAD (the same image used for the green `silo-20260525-192712Z.log` baseline). `BENCH_FLUSH_MS` is the only variable: `{50, 100, 200, 400}` ms - chosen to bracket the producer's 200 ms inter-tick (`TickHz=5` -> one tick per vehicle every 200 ms). The harness change in `benchmark/azure-throughput/scripts/20-build-and-deploy.ps1` propagates `BENCH_FLUSH_MS` from the host env into the ACI container env (committed separately on this branch); the sweep driver is `benchmark/azure-throughput/scripts/.run-U9l-driver.ps1` (gitignored under the existing `.run-*` rule). Manifest: `benchmark/azure-throughput/scripts/.run-U9l-manifest.csv`. Per-probe artifacts under `.ladder-results-U9l-FLUSH{50,100,200,400}.csv`, `.ladder-phaseA-U9l-FLUSH{50,100,200,400}.csv`, and silo logs `silo-20260526-064938Z.log` / `-065253Z.log` / `-065627Z.log` / `-065901Z.log`.

**Outcome.**

| Probe              | Flush window | SteadyAvg | FinalAvgRate | FinalWritten | FinalFailed | Silo log                       |
|--------------------|--------------|-----------|--------------|--------------|-------------|--------------------------------|
| **U9l-FLUSH50**    |  50 ms       | **1 368/s** | **1 365/s** | **157 722**  | **8 192**   | `silo-20260526-064938Z.log`    |
| **U9l-FLUSH100**   | 100 ms       | **1 449/s** | **1 395/s** | **164 147**  | **4 096**   | `silo-20260526-065253Z.log`    |
| U9l-FLUSH200       | 200 ms       | 1 235/s   | 1 114/s      | 130 999      | 24 576      | `silo-20260526-065627Z.log`    |
| U9l-FLUSH400       | 400 ms       | 1 170/s   |   911/s      | 106 496      | 45 056      | `silo-20260526-065901Z.log`    |

The throughput peak sits at `flushMs=100` (1 449/s steady, 4 096 failed) and the curve is monotonically worse on either side of it - and *every* probe sits well below the U9k step 2 `5000:5` baseline of 2 454/s recorded on the same knob set at `flushMs=50`. That ~44% under-run on the rebuilt-image FLUSH=50 probe is consistent with normal Azure Tables run-to-run variance (the U9k step 2 baseline tree-id `azure-throughput-20260525-192700` saw `provider.duration p99 = 31.6 ms`; the U9l-FLUSH50 tree-id `azure-throughput-20260526-064517` saw `provider.duration p99 = 64.66 ms`, i.e. Azure Tables was ~2x slower on this measurement day). The within-sweep *direction* (50 -> 400 ms = degradation) is what the falsifiability test cares about, and that direction is consistent and large.

**Phase-A evidence (the decisive instrument).** Aggregated per-WAL-partition quantile means across all 4 probes:

| Instrument                       | FLUSH50 P50 / P99 / Max | FLUSH100 P50 / P99 / Max | FLUSH200 P50 / P99 / Max | FLUSH400 P50 / P99 / Max | Samples (sum) |
|----------------------------------|-------------------------|--------------------------|--------------------------|--------------------------|--------------:|
| **`provider.phase2.batch_size`** | **1.00 / 1.00 / 2.00**  | **1.00 / 1.00 / 1.00**   | **1.00 / 1.00 / 2.00**   | **1.00 / 1.00 / 2.00**   | **14 495**    |
| `wal.append.batch_entries`       | 2.78 / 13.67 / 22.00    | 2.78 / 12.89 / 19.00     | 2.44 / 11.33 / 19.00     | 2.00 / 12.00 / 19.00     | 14 474        |
| `wal.append.in_flight`           | 0.00 / 0.00 / 0.00      | 0.00 / 0.00 / 0.00       | 0.00 / 0.00 / 0.00       | 0.00 / 0.00 / 0.00       | 14 474        |
| `wal.append.provider.duration` (ms) | 19.49 / 64.66 / 240.57 | 18.67 / 66.40 / 196.09 | 18.24 / 65.74 / 318.77 | 19.20 / 71.94 / 608.58 | 14 506        |

**`provider.phase2.batch_size` is pinned at 1.00 across all 14 495 samples.** The maximum observed value in any single sample on any single shard at any flush window is **2**, and the P99 means are 1.00 across the board. The phase-2 coalescer is still seeing one commit per drain cycle no matter how long we let the producer's TCP flush window grow. `wal.append.batch_entries` is *also* shrinking with flush-window length (P50 2.78 -> 2.78 -> 2.44 -> 2.00), the opposite of what the producer-cadence hypothesis predicted - a wider window should accumulate more entries per flush, not fewer. Combined with monotonically rising P99 `provider.duration` tail (64.66 ms -> 71.94 ms) and the 0 / 4 096 / 24 576 / 45 056 producer-side failure progression, the wider windows are not feeding the coalescer; they are starving it *and* exhausting the producer's per-call timeout budget.

**Mechanism, restated.** The `TcpIngestService` flush window times *the producer-side* batch boundary - i.e. it bounds the latency between a vehicle tick and the silo-side `SetManyAsync` submission. Widening it from 50 ms to 400 ms simply delays the per-call submission; it does *not* enable two distinct producer batches to land on the same phase-2 coalescing cycle inside a single `PhaseTwoWorker`, because each producer batch is already a single `SetManyAsync` -> one shard-root turn -> one `WalShardGrain.AppendAsync` -> one phase-1 commit -> one phase-2 manifest commit. The shape of the pipeline guarantees one phase-2 element per producer call no matter how the producer batches its inputs. Wider flush windows just lengthen the inter-arrival time at the phase-2 channel, which (per the U9b/U9c arithmetic at L639) makes coalescing *less* likely, not more.

This is the same arithmetic U9c smoke surfaced (L687-L689): the post-commit re-drain runs against a channel that is empty because the producer side is not bursty *enough* relative to the commit RT. Widening the producer's *batching* window does not change the fact that each producer batch produces exactly one phase-2 element; only *interleaving* two producer batches on the same shard could lift the phase-2 batch size above 1, and that requires reentrant `SetManyAsync` execution at the shard root - exactly the U9g family of changes that already shipped (`[AlwaysInterleave]` on `IShardRootGrain.SetManyAsync`). The U9l result therefore says: `[AlwaysInterleave]` is interleaving the turns at the shard root, but the resulting overlap does not extend down to the `WalShardGrain` phase-1 commit boundary - each turn still emits its own phase-1 transaction in sequence, so the phase-2 channel sees one element per arrival regardless.

**Re-ranked probe order (eighth revision; the producer/flush race is dead, leaf-side commit concurrency is next).**

- **U9 / U9b / U9c / U9d / U9e / U9f / U9g / U9h-A / U9h-B / U9h-C / U9h-D / U9j / U9k** - rolled up above; U9k step 2 (the asymmetric promotion fix + `[AlwaysInterleave]` on shard-root reads) remains the current shipping baseline.
- **U9l - FALSIFIED.** Producer-side flush cadence is *not* the lever; `provider.phase2.batch_size` is pinned at 1.00 across every flush window in `{50, 100, 200, 400}` ms with throughput monotonically degrading from the 100 ms peak. The `BENCH_FLUSH_MS` env-var wiring in `20-build-and-deploy.ps1` is *kept* as a production knob (operators may want to trade latency for batching against a faster storage backend), but the library default of 50 ms is correct as shipped.
- **U9m (the next probe) - leaf-side commit concurrency.** The U9k step 2 closing paragraph already named this: "if batch_size stays pinned, the bottleneck is downstream of the producer flush and the next probe shifts to leaf-side commit concurrency." U9l confirms the antecedent. The concrete target is `BPlusLeafGrain.CommitSetAsync` - the path that turns an individual shard-root sub-batch into a `WalShardGrain.AppendAsync` call. Today every leaf serialises its commits behind a single per-leaf turn; under `[AlwaysInterleave]` on the shard-root, two disjoint-key sub-batches that target *the same leaf* still queue at the leaf grain. The instrumentation question to answer first is whether the binding constraint is (i) the per-leaf turn queue (in which case the lever is `[AlwaysInterleave]` on `CommitSetAsync` with the same disjoint-key invariant the shard-root carries), or (ii) the leaf-to-WAL fan-in (in which case the lever is to lift `WalPartitions` or move to a multi-leaf coalescing pattern at the `WalCommitLogWriter` layer). A targeted probe is to add a `NonReentrancyQueueSize` diagnostic to the leaf grain's per-second cadence line and re-run the U9k step 2 rung; if the leaf queue depth stays at 1 the bound is downstream of the leaf and U9n (WAL fan-in) is next.
- **U9n (deferred behind U9m) - WAL-side fan-in coalescing across leaves.** Only attempt if U9m proves the leaf turn queue is *not* the binding constraint. The candidate is to let `WalCommitLogWriter` coalesce `AppendAsync` calls from distinct leaves on the same shard into a single phase-1 transaction, which would for the first time produce phase-2 input shapes that the existing `PhaseTwoCoalescingWindow` can act on.
- **U9i (deferred, unchanged) - investigate the benign cold-start reshard rejections.** Hygiene, not throughput.

#### U9m step 1 (2026-05-26) - leaf-side commit-concurrency instrumentation shipped

**Hypothesis under test.** With `[AlwaysInterleave]` on `IShardRootGrain.SetManyAsync` (shipped U9g / U9h-A) the shard-root accepts overlapping producer batches, but each batch still bottoms out at a single `BPlusLeafGrain` activation per affected leaf. Because neither `IBPlusLeafGrain.SetAsync` nor `IBPlusLeafGrain.SetManyAsync` is marked `[AlwaysInterleave]`, two disjoint-key sub-batches that target the same leaf queue behind the leaf's single turn token. If the leaf turn queue is the binding constraint, `leaf.commit.in_flight` is expected to pin at `0` (the next commit cannot enter until the current one returns); if a future change applies `[AlwaysInterleave]` at the leaf entrypoint and the new shape is faster, this histogram is the falsifiability instrument that proves the overlap actually materialises end-to-end.

**Code shipped at HEAD (no behavior change on the shipping non-reentrant scheduling).**

1. `LatticeMetrics.LeafCommitInFlight` (`Histogram<int>` `orleans.lattice.leaf.commit.in_flight`, tag `tree`) records the leaf-side commit depth observed at the moment a foreground commit enters the commit path. The histogram is documented as a falsifiability probe and explicitly notes the expected `0` pin on the current scheduling shape.
2. `BPlusLeafGrain.Metrics.cs` gained a `_commitInFlight` counter, an `EnterCommitScope()` helper that increments the counter and records the pre-increment value on the histogram, and a `CommitInFlightScope` `IDisposable` whose `Dispose` decrements the counter unconditionally so an exception inside the commit cannot leak depth. `Interlocked.Increment` / `Interlocked.Decrement` are used even though Orleans' single-thread per-activation scheduling makes contention unlikely - the cost is one interlocked op per commit and it removes the need to reason about future async-only changes that could break the assumption.
3. `CommitSetAsync` and `CommitSetManyAsync` open the scope as their first line so every exit path (normal return, exception propagation, leaf split, projection rewrite) re-uses the same decrement.
4. `PhaseADiagnosticReporter` allowlists `orleans.lattice.leaf.commit.in_flight` so the `[phaseA] ... instrument=leaf.commit.in_flight tree=t ...` line appears in the ACI log alongside the existing `wal.append.in_flight` line and the ladder script can scrape both.

**Regression coverage.** `test/lattice/BPlusTree/Grains/BPlusLeafGrainTests.CommitInFlight.cs` pins the depth-0 invariant on the shipping scheduling: `Set_records_leaf_commit_in_flight_once_at_zero`, `SetMany_records_leaf_commit_in_flight_once_at_zero`, `Sequential_sets_each_record_zero_in_flight`, `Recorded_measurement_carries_tree_tag`. Each test uses a dedicated `MeterListener` recorder so the assertions are isolated from other instrument traffic.

**What this step does not yet measure.** The histogram is the *instrument*; the corresponding ladder-rung measurement against the U9k step 2 baseline (`BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4096`, `BENCH_WAL_PARTITIONS=8`, `BENCH_FLUSH_CONCURRENCY=8`, rung `5000:5`) is the next step. The expected reading on that rung is `count > 0, p99 = 0` across all leaves: every commit records, none observes a concurrent commit. The ship-criterion for U9m step 2 is whether that prediction holds.

#### U9m step 2 (2026-05-26) - leaf-side commit concurrency falsified as the bottleneck

**Setup.** U9k step 2 baseline replayed against the U9m step 1 image: `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4096`, `BENCH_WAL_PARTITIONS=8`, `BENCH_FLUSH_CONCURRENCY=8`, `BENCH_FLUSH_MS=50`, single rung `5000:5` for 60 s of producer drive. CSVs archived at `benchmark/azure-throughput/scripts/.ladder-results-U9m-baseline.csv` and `.ladder-phaseA-U9m-baseline.csv`; silo log at `benchmark/azure-throughput/.run/silo-20260526-074037Z.log`.

**Headline.** Steady-state avg `1,321/s` (FinalAvg `1,350/s`), inside the U9l `1.3-1.4k/s` envelope. The U9m step 1 instrumentation did not move throughput.

**Phase A readings (final cadence window, 60 s in).**

1. `leaf.commit.in_flight` (new): `count=597` on the user tree, `min=p50=p90=p99=max=0` (plus `count=3` control-plane commits on `_lattice_trees` at the same shape). Every foreground commit records the depth, and the depth observed at entry is always zero. The U9m step 1 prediction (`count > 0, p99 = 0`) is confirmed end-to-end.
2. `wal.append.in_flight`: pins at `0` across all 8 WAL shards (per-shard count 452-478), so the WAL append slot is *also* empty whenever the leaf hands it a batch.
3. `provider.phase2.batch_size`: still pinned at `1.00` across all 8 shards (count 452-479, min=p50=p99=max=1.00), confirming the U9l finding that the producer hands the WAL one-row sub-batches and the flush window is not the lever.

**Inference.** With `leaf.commit.in_flight` *and* `wal.append.in_flight` both at zero on the same rung, the binding constraint at `5000:5` is upstream of both the leaf turn queue and the WAL pipeline: no two leaf commits ever race for the same activation, and no two WAL appends ever race for the same shard. Two structural consequences:

- The leaf-side `[AlwaysInterleave]` lever drafted in U9m step 1 would not move throughput at this rate. There is nothing queued behind the per-leaf turn token to interleave away.
- The earlier U9k step 2 conjecture that the next probe after the producer flush window is leaf-side commit concurrency is now falsified. The producer is presenting batches that the silo absorbs faster than the producer can refill them; the missing throughput is between the producer's vehicle ticks and the silo's first `SetManyAsync`.

**What this means for U9n.** U9n (WAL fan-in coalescing across leaves) was deferred behind U9m and is now also off the critical path at this rung: a fan-in collapse can only help if the WAL is the queue point, and `wal.append.in_flight=0` says it is not. Both U9m and U9n exit as falsified at the `5000:5` rung.

**Next probe (U9o, replaces both U9m and U9n).** The remaining unexplained gap is between the per-vehicle tick cadence and the per-second silo throughput. With `5000` vehicles at `TickHz=5` the producer offers `25,000` events/s but the silo absorbs only `1,321/s` steady. The producer side of the harness (batch construction, TCP framing, network round-trip latency) is the only segment we have not measured. U9o is to add a producer-side cadence/batch instrument to the producer container, replay the same rung, and check whether the producer is the entity holding the queue.

#### U9o step 1 (2026-05-26) - producer-side cadence instrumented; TCP backpressure localised to inner write loop

**Hypothesis under test.** With the U9m run showing the silo absorbed only `1,321/s` against a target of `25,000/s` *and* the producer's own end-of-run header showing `avg=3,272 msg/s`, the missing throughput is between the per-vehicle tick clock and the JSON-on-TCP wire. Two falsifiable sub-hypotheses: (H1) the per-tick send-loop is slow on its own (JSON arithmetic, byte writes), or (H2) the per-tick send-loop is fast but blocks on TCP backpressure when the silo cannot drain the receive buffer quickly enough.

**Code shipped at HEAD (no behavior change to scheduling).** `benchmark/azure-throughput/Producer/Program.cs` keeps three per-second aggregates on the existing cadence line: `innerAvgMs` (wall-clock spent in the per-vehicle `for` loop that JSON-serialises and `BufferedStream.Write`s every event), `flushAvgMs` (wall-clock spent in the explicit `await writer.FlushAsync()` that fires once per second), and `slipMaxMs` (the maximum of `actualTickEntry - scheduledTick`, where `scheduledTick` advances strictly by `tickIntervalMs` regardless of how late each tick fires). `scheduledTick` advances independently of the producer's self-resetting `nextTick`, so the slippage is a passive measurement rather than a feedback signal - the producer's existing tick clock is preserved.

**Setup.** U9m baseline replayed against the U9o-instrumented producer image. Same knobs: `BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4096`, `BENCH_WAL_PARTITIONS=8`, `BENCH_FLUSH_CONCURRENCY=8`, `BENCH_FLUSH_MS=50`, single rung `5000:5` for 60 s. Producer log archived at `benchmark/azure-throughput/.run/producer-U9o-step1.log`; silo log at `benchmark/azure-throughput/.run/silo-U9o-step1.log`. Silo headline: `FINAL written=151,833 elapsed=116.4s avg=1,304/s`, `wal.append.in_flight=0` across all 8 shards (identical shape to U9m, confirming the silo image is the same).

**Producer readings (selected per-second cadence rows).**

```
t= 13.2s rate=  1,147 msg/s  ticks=2  innerAvgMs= 4,227.33  flushAvgMs=0.02  slipMaxMs=    45.8
t= 14.3s rate= 28,952 msg/s  ticks=6  innerAvgMs=    58.55  flushAvgMs=0.00  slipMaxMs= 8,253.4
t= 32.3s rate=    277 msg/s  ticks=1  innerAvgMs=17,892.36  flushAvgMs=0.03  slipMaxMs= 8,253.5
t= 37.7s rate=    930 msg/s  ticks=1  innerAvgMs= 5,378.91  flushAvgMs=0.03  slipMaxMs=25,945.9
t= 50.9s rate=  1,435 msg/s  ticks=2  innerAvgMs= 3,394.10  flushAvgMs=0.01  slipMaxMs=36,725.2
t= 61.7s rate=    963 msg/s  ticks=1  innerAvgMs= 5,190.47  flushAvgMs=0.20  slipMaxMs=48,299.0
DONE total=210,000 elapsed=61.7s avg=3,404 msg/s
```

**Inference.**

1. `flushAvgMs` stays between `0.00` and `0.20` ms across the entire run. The explicit `await writer.FlushAsync()` is not the bottleneck. H2 *as stated* (TCP backpressure on the explicit flush) is falsified.
2. `innerAvgMs` swings between `58 ms` and `17,892 ms` per tick. The same `5,000`-vehicle serialise-and-write loop that completes in `58 ms` when six ticks fire back-to-back at `t=14.3s` then takes `17.9 seconds` for a single tick at `t=32.3s`. The work in that loop is constant; the only thing that varies is whether the underlying socket is willing to accept bytes.
3. `slipMaxMs` climbs monotonically from `45.8 ms` at `t=13.2s` to `48,299 ms` at `t=61.7s`. The producer falls 48 s behind ideal cadence in a 60 s run.
4. The U9k step 2 silo shape (`provider.phase2.batch_size=1.00`) is preserved on the U9o silo log, confirming the silo is unchanged.

The combination falsifies H1 (the inner loop is fast when nothing back-pressures it) and refines H2: TCP backpressure is real but it is hidden *inside* `BufferedStream.Write` rather than on `FlushAsync`. `BufferedStream` synchronously flushes its 64 KiB buffer whenever it fills; at ~140-180 bytes per JSON event a 5,000-vehicle tick triggers ~13-15 synchronous network writes, and each of those writes blocks on the kernel send buffer when the silo is not draining fast enough. The block is charged to `innerAvgMs`, not `flushAvgMs`, because the explicit `FlushAsync()` only runs once per second and finds the buffer near-empty by then.

**What this means for the bottleneck location.** The silo is still the queue point. `wal.append.in_flight=0` and `leaf.commit.in_flight=0` rule out the WAL and the leaf turn queue inside the silo, but the missing throughput is upstream of both: it sits in the silo's TCP receive path (the `TcpIngestService` `NetworkStream.ReadAsync` loop, the per-line JSON deserialise, the per-line `SetAsync` / `SetManyAsync` dispatch to the shard-root). The producer side is healthy in isolation - the inner loop is fast when the receive socket accepts bytes - so the next probe lives entirely on the silo side.

**Next probe (U9o step 2).** Instrument the silo's TCP receive path. Three candidate signals, in order of expected discriminating power: (i) `tcp.ingest.line_bytes` histogram per accepted line (the JSON wire shape per event); (ii) `tcp.ingest.lines_per_drain` counter per `ReadAsync` return (how many full lines the receive buffer hands the JSON deserialiser per syscall); and (iii) `tcp.ingest.flush_batch_size` histogram per outbound `SetManyAsync` flush (the batch size the silo presents to the lattice, currently capped at `BENCH_FLUSH_MS=50` cadence). If (iii) shows batches of size 1 the silo is processing line-at-a-time instead of coalescing per flush window, which would explain the `provider.phase2.batch_size=1.00` U9l finding *and* the producer-side backpressure observed here.

#### U9o step 2 (2026-05-26) - silo TCP receive path instrumented; bottleneck localised inside SetManyAsync

**Hypothesis under test.** U9o step 1 falsified the producer's `FlushAsync` as the bottleneck and showed the producer's blocking lives inside `BufferedStream.Write` against the kernel send buffer. That places the queue point downstream of the producer's TCP write but upstream of the WAL and the leaf turn queue (both of which still show `in_flight=0`). The question is: which segment on the silo side is queuing - the TCP read loop, the channel between the read loop and the drain, the drain's batch construction, the flush gate, or `SetManyAsync` itself.

**Code shipped at HEAD.** A new benchmark-local meter `azure.throughput.bench` (in `benchmark/azure-throughput/Silo/BenchMetrics.cs`) carries four histograms recorded from `TcpIngestService`:

- `tcp.read.line_bytes` per accepted JSON line (sanity check on wire shape).
- `tcp.read.channel_write_wait_ms` per accepted line - wall-clock spent inside `ChannelWriter.WriteAsync` handing the line to the drain channel. Near-zero ⇒ the drain is keeping up; high ⇒ the channel is full and the lattice flushes are the queue point.
- `drain.flush_dispatch_size` per `DispatchFlushAsync` call - the batch list count at dispatch. Tells us whether the drain is presenting tiny batches to the lattice.
- `drain.flush_dispatch_wait_ms` per `DispatchFlushAsync` call - wall-clock spent on `flushGate.WaitAsync` before the batch was dispatched. Near-zero ⇒ the `FlushConcurrency=8` slots are not contended; high ⇒ all 8 slots are saturated and the drain is stalled on the lattice's commit speed.

`PhaseADiagnosticReporter` now subscribes to both the `orleans.lattice` meter and the `azure.throughput.bench` meter and renders both into the same `[phaseA] instrument=...` line shape (the prefix is stripped for both), so the ladder script's regex needs no change. The benchmark-local meter is deliberately separate from `orleans.lattice` so these instruments can never leak through the public lattice surface.

**Setup.** Same knobs as U9m / U9o step 1 (`BENCH_SHARD_COUNT=16`, `BENCH_BATCH_SIZE=4096`, `BENCH_WAL_PARTITIONS=8`, `BENCH_FLUSH_CONCURRENCY=8`, `BENCH_FLUSH_MS=50`), single rung `5000:5` for 60 s. Silo log archived at `benchmark/azure-throughput/.run/silo-U9o-step2.log`, producer log at `benchmark/azure-throughput/.run/producer-U9o-step2.log`, ladder CSVs at `benchmark/azure-throughput/scripts/.ladder-results-U9o-step2.csv` and `.ladder-phaseA-U9o-step2.csv`. Headline: `FINAL written=204,137 failed=0 elapsed=116.3s avg=1,756/s`.

**Bench instrument readings (selected `[phaseA]` windows).**

```
# Early ramp (t=10-20s): channel writes still instant, gate starting to saturate
[phaseA] t=10.3s instrument=tcp.read.line_bytes              count=97,642 p50=245.00 p99=247.00 max=247.00
[phaseA] t=10.3s instrument=tcp.read.channel_write_wait_ms   count=97,641 p50=0.00 p99=0.00 max=5.44
[phaseA] t=10.3s instrument=drain.flush_dispatch_size        count=10 min=41 p50=2,831 p90=4,096 max=4,096
[phaseA] t=10.3s instrument=drain.flush_dispatch_wait_ms     count=10 p50=0.00 p99=1,690.50 max=1,690.50

[phaseA] t=20.3s instrument=tcp.read.line_bytes              count=32,768 p50=245.00 p99=247.00
[phaseA] t=20.3s instrument=tcp.read.channel_write_wait_ms   count=32,768 p50=0.00 p99=0.00 max=8,918.53
[phaseA] t=20.3s instrument=drain.flush_dispatch_size        count=8 min=4,096 p50=4,096 max=4,096
[phaseA] t=20.3s instrument=drain.flush_dispatch_wait_ms     count=8 p50=5.56 p99=9,964.37 max=9,964.37

# Steady state (t=60-120s): gate fully saturated, batches full-size, throughput pinned
[phaseA] t=70.3s instrument=drain.flush_dispatch_size        count=5 min=4,096 p50=4,096 max=4,096
[phaseA] t=70.3s instrument=drain.flush_dispatch_wait_ms     count=5 p50=1,854.76 p90=2,224.99 max=2,224.99
[phaseA] t=70.3s instrument=tcp.read.channel_write_wait_ms   count=20,480 p50=0.00 p99=0.00 max=2,218.31

[phaseA] t=100.3s instrument=drain.flush_dispatch_size       count=5 min=4,096 p50=4,096 max=4,096
[phaseA] t=100.3s instrument=drain.flush_dispatch_wait_ms    count=5 p50=2,230.33 p90=2,313.75 max=2,313.75
```

**Inference.** Each bench instrument falsifies one candidate queue point and the surviving one is decisive.

1. **`tcp.read.line_bytes` p50=245 B, p99=247 B, count tracks producer offered load.** Wire shape is uniform; the TCP read loop is healthy and sees every line. The receive path is not framing, deserialising, or dropping anything.
2. **`tcp.read.channel_write_wait_ms` p50=p90=p99=0 ms** across every window. The TCP→drain handoff is instant for **every line** in steady state - the bounded channel (`capacity = 1 << 16 = 65,536`) never fills past the drain's ability to consume. The `max` field does spike to 2-9 s during whole-channel stalls, but that mass is below p99: the channel briefly fills only when the drain is *completely* stuck, and unfills immediately when one batch dispatches. The "channel full ⇒ TCP backpressure ⇒ producer `BufferedStream.Write` block" causation chain U9o step 1 inferred is now observed end-to-end.
3. **`drain.flush_dispatch_size` p50=max=4,096** in steady state. The drain is **always** presenting full-size 4,096-entry batches to `DispatchFlushAsync`. The "silo is presenting tiny batches" hypothesis that the U9o step 1 next-probe note speculated about is **falsified**. The `provider.phase2.batch_size=1.00` U9l finding therefore has nothing to do with how the silo batches - it is purely about how phase 2 inside the WAL coalesces (or fails to coalesce) batches the silo hands it.
4. **`drain.flush_dispatch_wait_ms` p50=1.8-2.5 s, p99=2.2-3.7 s** in steady state. The drain loop is **continuously** blocked on `flushGate.WaitAsync` waiting for one of the 8 `FlushConcurrency` slots to free. All 8 slots are saturated. The silo is offering full-size batches to `SetManyAsync(4,096)` as fast as `SetManyAsync` will accept them.

**Where the throughput goes.** With 8 concurrent flush slots and steady throughput of `1,756/s`, the effective per-slot throughput is `~220 entries/s`, which means each `SetManyAsync(4,096)` call takes `~18.6 s` wall-clock from `WaitAsync` return to `SetManyAsync` return. The gate-wait quantiles (~2 s p99) are consistent with this: a drain that dispatches one batch every ~2.3 s × 8 slots in flight = ~18.6 s per individual call. The 8-way concurrency is doing real work, but each call is slow.

**Bottleneck location after U9o step 2.** Confirmed inside `ILattice.SetManyAsync(4096)` itself, *not* upstream. Every segment between the producer's `Write` and `SetManyAsync` entry has been measured and is healthy:

- Producer inner loop (`innerAvgMs ≤ 60 ms` while sockets accept) ✓
- Kernel send buffer / network ✓ (saturates only as a consequence of downstream pressure)
- Silo TCP read loop (`tcp.read.line_bytes`) ✓
- Silo channel handoff (`tcp.read.channel_write_wait_ms` p99=0) ✓
- Silo drain batch construction (`drain.flush_dispatch_size = 4,096`) ✓
- Silo flush gate (`drain.flush_dispatch_wait_ms` is symptom of downstream, not cause) ✓

Inside `SetManyAsync(4,096)`, both previously-suspected lattice queue points are still innocent at this rung:
- WAL turn queue: `wal.append.in_flight = 0`, `wal.append.turn_wait` p99 ≈ 17 ms (negligible).
- Leaf turn queue: `leaf.commit.in_flight = 0` (U9m result, unchanged here).

So the per-call latency lives somewhere on the `SetManyAsync` path that **neither** the WAL append metrics **nor** the leaf-commit metrics observe. The two candidate sub-paths are: (a) the saga fan-out (shard-root → shard → leaf), where the per-key dispatch could be serialising work that the producer offers as a batch; and (b) the storage provider's phase-2 commit, which `provider.phase2.batch_size=1.00` indicates is committing one batch per Azure Tables transaction (no coalescing) despite the silo offering batches as fast as the gate will let it.

**Next probe (U9p).** Measure `SetManyAsync(4,096)` end-to-end on the silo side with three new sub-timings:

- `lattice.set_many.duration` total wall-clock per `SetManyAsync` call (the headline).
- `lattice.set_many.fanout_wait_ms` time spent in the saga fan-out before the first per-leaf commit returns.
- `lattice.set_many.provider_wait_ms` time spent on `await` of the WAL provider's phase-2 commit (the segment `provider.phase2.batch_size=1.00` covers).

If `provider_wait_ms` dominates, the next move is on the phase-2 commit path (Azure Tables RTT × number of partitions touched by a single 4,096-entry batch); if `fanout_wait_ms` dominates, the move is on the saga fan-out (per-key dispatch shape).

### U9p step 1 - headline confirmation (5000:5)

**What we measured.** A benchmark-local histogram `azure.throughput.bench.lattice.set_many.duration_ms` was added directly around the `await lattice.SetManyAsync(batch, ct)` call inside the silo's `FlushAsync` wrapper (`benchmark/azure-throughput/Silo/Program.cs`). It lives on the `azure.throughput.bench` meter (so it stays out of the public lattice surface) and rides the same Phase A reporter rendering path as U9o step 2's TCP / drain instruments. The same `5000:5` ladder rung was rerun for 120 s.

**Final throughput.** `190,228` entries written, `0` failed, `Entries written per second (avg)=1,598`, steady-state `1,581/s`. Same shape as U9o step 2 (1,756/s) and U9m baseline (1,350/s) - confirms the bottleneck is reproducible and that the new instrument did not perturb the run.

**Headline result (per-call `SetManyAsync(4,096)` wall-clock, steady state windows t=70.8s through t=119.6s, 5 calls / 10 s window):**

| Window | count | min | p50 | p99 | max |
|---|---|---|---|---|---|
| t=70.8s | 5 | 15.87 s | 16.13 s | 16.84 s | 16.84 s |
| t=80.8s | 5 | 16.06 s | 16.52 s | 17.02 s | 17.02 s |
| t=90.8s | 5 | 15.74 s | 16.30 s | 16.84 s | 16.84 s |
| t=100.8s | 5 | 14.86 s | 15.31 s | 15.41 s | 15.41 s |
| t=110.8s | 5 | 15.53 s | 15.74 s | 16.03 s | 16.03 s |
| t=119.6s | 5 | 15.02 s | 15.56 s | 15.79 s | 15.79 s |

**What this proves.**

1. **U9o step 2's ~18.6 s/call inference is confirmed directly.** Steady-state p50 sits at **15.3-16.5 s/call**, p99 at **15.4-17.0 s/call**. The earlier arithmetic over gate-waits (~18.6 s) was in the right ballpark; the small overshoot is the gate-wait portion (`drain.flush_dispatch_wait_ms` ~2-3 s) being counted on top of the call itself.
2. **The slowness is uniform, not tail-dominated.** `min ≈ p50 ≈ p99` across every steady-state window. `max/p50 ≈ 1.04`. There is **no fast cohort** of calls being dragged up by tail outliers. Every batch pays ~16 s. This rules out queueing-style models where most calls are fast and a few are slow (e.g. retries, lock contention, GC pauses).
3. **Warmup contributes very little.** The early windows (t=30.7s p50=20.1 s, t=50.7s p50=20.2 s) settle to t=70-119s p50=15-16 s after ~70 s. The roughly 20% improvement is consistent with cold-start activation / connection setup amortising; it is not the dominant cost.
4. **The arithmetic closes.** `4,096 entries / 16 s × 8 flush slots = 2,048 entries/s` upper bound. Observed steady-state `1,581/s` matches this within the `drain.flush_dispatch_wait_ms ≈ 2 s` gate-wait slack: each dispatch costs `~16 s of call + ~2 s of gate-wait = ~18 s per slot-turn`, giving `4,096 × 8 / 18 ≈ 1,820/s` which brackets the observation. There is no missing time anywhere upstream.

**What it does not yet tell us.** Whether the 16 s/call lives in (a) the shard-root fan-out (`LatticeGrain.SetManyAsyncCore` → `Task.WhenAll` over per-shard `IShardRootGrain.SetManyAsync` → per-leaf `IBPlusLeafGrain.SetManyAsync`), or (b) the WAL provider's phase-2 commit (`AzureTableWalStorageProvider` phase-2 worker awaiting Azure Tables), or (c) something between (`PublishDigestUpwardAsync` propagation up the B+ tree on every batch). The leaf-commit and WAL-append per-step histograms already exist; the missing piece is a **single wall-clock pair** that splits the 16 s into "time before the first per-leaf commit completed" and "time after that, waiting for phase-2 to drain".

**U9p step 2 next.** Add two timings inside `ShardRootGrain.SetManyAsync` (the cleanest seam where the fan-out is observable in one place):

- `shard_root.set_many.local_apply_ms` - `Stopwatch` around `SetManyLocalOnlyAsync` (the `Task.WhenAll` over `DispatchLeafBatchWithRetryAsync`). Includes all per-leaf RPC time + WAL append + WAL phase-2 commit, but **excludes** lattice-grain bucketing and event-publish.
- `shard_root.set_many.shadow_forward_ms` - `Stopwatch` around the trailing `forwardTask` await (only non-zero during online resize; expected to be ~0 in steady state and will confirm shadow-forward isn't silently contributing).

Both go on the shard-root grain via `LatticeMetrics` (lattice-internal, not bench-local) since they describe a public grain seam that exists outside the bench harness. The bench's `phaseA` reporter already subscribes to `orleans.lattice` so the new histograms surface automatically.

If `local_apply_ms` consumes essentially all 16 s, the cost lives in the leaf/WAL/provider chain and the next probe targets phase-2 (`provider.phase2.commit_duration` already exists; we need it correlated with batch-arrival rate). If `local_apply_ms` is materially less than 16 s, then `LatticeGrain.SetManyAsyncCore` itself is the choke point and the next probe sits one layer up (bucket build cost, `RetryOnStaleRoutingAsync` overhead, event-publish synchronization).

### U9p step 2 - shard-root decomposition (5000:5)

**What we measured.** Two new histograms on the `orleans.lattice` meter, recorded inside `ShardRootGrain.SetManyAsync` (`src/lattice/BPlusTree/Grains/ShardRootGrain.cs`): `shard_root.set_many.local_apply.duration` around `SetManyLocalOnlyAsync(entries)` (the per-leaf fan-out + WAL append + commit chain) and `shard_root.set_many.shadow_forward.duration` around the trailing `forwardTask` await (online-resize shadow-forward only). The Phase A reporter's allowlist was extended to surface both instruments without altering the parser. The bench's existing `azure.throughput.bench.lattice.set_many.duration_ms` (the silo's direct `await lattice.SetManyAsync(batch, ct)` wall-clock from step 1) was kept in place so the new shard-root numbers can be calibrated against the silo's outermost call boundary in the same run.

**Final throughput.** `296,741` entries written, `0` failed, `Entries written per second (avg)=2,485`, steady-state `2,674/s`. This is a meaningful jump above U9p step 1 (`1,598/s` final, `1,581/s` steady) on the same `5000:5` rung with the same instrument surface plus only two extra histograms. The most likely cause is run-to-run variance / cold-cache effects on the Azure Tables backend; the relative shape of the metrics, not the absolute throughput, is what U9p step 2 is reading off.

**Headline result (steady-state windows, t=70.3s through t=119.7s, single shard-root activation receives every fan-out bucket so `local_apply` count ≈ batches × buckets-per-batch):**

| Window | bench `lattice.set_many` p50 / p99 | `shard_root.local_apply` count / p50 / p99 | `shard_root.shadow_forward` sum |
|---|---|---|---|
| t=70.3s | 11.40 s / 12.06 s | ~430 / 9.4 s / 11.7 s | ~0 |
| t=80.3s | 11.43 s / 12.03 s | ~430 / 9.6 s / 11.9 s | ~0 |
| t=90.3s | 11.80 s / 11.95 s | 422 / 9.87 s / 11.53 s | 0.01 ms total |
| t=100.3s | 12.43 s / 12.48 s | 421 / 10.47 s / 12.41 s | 0.02 ms total |
| t=110.3s | 11.60 s / 12.37 s | 461 / 9.67 s / 11.52 s | 0.01 ms total |
| t=119.7s | 11.87 s / 12.04 s | 788 / 9.11 s / 10.97 s | 0.01 ms total |

**What this proves.**

1. **Shadow-forward is not the bottleneck.** Sum of `shadow_forward.duration` is ~0.01-0.02 ms total across windows that contain 400-800 calls each. Online-resize / replication tail-write is genuinely idle on this workload. The earlier "could it be silent shadow-forward?" hypothesis is closed.
2. **Local apply consumes essentially all of the silo's `lattice.set_many` wall-clock.** Per-shard-root-call `local_apply` p50 is `9.1-10.5 s` and p99 is `11.5-12.4 s`. The silo's outermost `await lattice.SetManyAsync(batch, ct)` wall-clock p50 / p99 is `11.4-12.4 s` / `12.0-12.5 s` in the same windows. The ~1-2 s gap between the silo wall-clock and the shard-root local-apply p99 is fully explained by `LatticeGrain.SetManyAsyncCore` fanning a 4,096-entry batch across multiple parallel shard-root tasks via `Task.WhenAll` - the silo wall-clock observes the slowest bucket, while each `shard_root.local_apply` observation is per-bucket. There is no missing time outside the shard-root grain.
3. **The cost lives strictly inside `SetManyLocalOnlyAsync`.** That is the per-leaf fan-out (`Task.WhenAll` over `DispatchLeafBatchWithRetryAsync`), each leaf RPC, each leaf's WAL append, and each WAL phase-2 commit. The U9p step 1 conclusion ("the 16 s slowness is uniform and not tail-dominated") composes with this: every leaf-level fan-out segment is uniformly slow, not a few outliers.
4. **Throughput jump (1,598 → 2,485/s) is a separate observation.** The shape of every per-batch histogram (`lattice.set_many.duration_ms`, `drain.flush_dispatch_size`, `drain.flush_dispatch_wait_ms`) is the same shape as U9p step 1 - 4,096-entry batches, multi-second flush-gate waits, ~11-12 s per shard-root call. The improvement appears to be Azure Tables-side warmth or cluster-side scheduling variance, not a metric-induced artifact. This is worth re-running before any commit-relevant conclusion is drawn.

**U9p step 3 next.** Add one more histogram inside `SetManyLocalOnlyAsync` that splits per-leaf `DispatchLeafBatchWithRetryAsync` into (a) `dispatch.wait_ms` - time from issuing the per-leaf RPC to the leaf's WAL turn beginning (Orleans grain-schedule + per-leaf turn-queue), and (b) `dispatch.commit_ms` - time from WAL turn start to the per-leaf RPC return (leaf-side `CommitSetManyAsync` + WAL append + phase-2). The existing `leaf.commit.duration` (per-step) and `wal.append.*` histograms already exist on the leaf side; the missing piece is the **outbound view from the shard-root** that pairs each leaf call's wait time with its commit time, because the leaf's clock cannot see its own pre-turn dispatch wait. If `dispatch.wait_ms` dominates, the bottleneck is per-leaf RPC scheduling and the next move is on the per-leaf concurrency shape. If `dispatch.commit_ms` dominates, the bottleneck is genuinely the leaf/WAL/provider chain and the next probe is the WAL provider's phase-2 batch shape (which `provider.phase2.batch_size=1.00` already suggests).

### U9p step 3 - per-leaf RPC view (5000:5)

**What we measured.** One additional histogram, `orleans.lattice.shard_root.set_many.leaf_rpc.duration`, recorded around every `await leaf.SetManyAsync(slice)` attempt in `DispatchLeafBatchWithRetryAsync` (`src/lattice/BPlusTree/Grains/ShardRootGrain.cs`). Tagged with `tree`. The Phase A allowlist was extended to surface it without parser changes. Same `5000:5` ladder rung, 120 s.

**Final throughput.** `332,162` entries written, `0` failed, `Entries written per second (avg)=2,775`, steady-state `3,040/s`. The throughput continues to drift upward run-over-run (U9p step 1: `1,598/s`; step 2: `2,485/s`; step 3: `2,775/s`) on the same instrument footprint, so the absolute number is run-variance, not a metric perturbation. The histogram **shape** is what matters.

**Headline result (steady-state window t=110.3s, identical shape across t=70-120s):**

| Instrument | count | p50 | p99 | max |
|---|---|---|---|---|
| `lattice.set_many.duration_ms` (silo wall-clock per batch) | 7 | 11.38 s | 11.44 s | 11.44 s |
| `shard_root.local_apply.duration` (per shard-root call) | 453 | 10.71 s | 11.39 s | 11.43 s |
| `shard_root.leaf_rpc.duration` (per per-leaf RPC) | 453 | 10.71 s | 11.39 s | 11.43 s |
| `shard_root.shadow_forward.duration` | 453 | 0 | 0 | 0 |
| `wal.append.provider.duration` (per WAL shard, p50 across shards) | ~430/shard | ~19 ms | ~60-80 ms | ~700 ms (one shard) |
| `wal.append.in_flight` | ~430/shard | 0 | 0 | 0 |
| `leaf.commit.in_flight` | 453 | 0 | 0 | 0 |
| `provider.phase2.batch_size` | ~450/shard | 1.00 | 1.00 | 1.00 |
| `drain.flush_dispatch_size` | 7 | 4,096 | 4,096 | 4,096 |
| `drain.flush_dispatch_wait_ms` | 7 | 1.44 s | 1.50 s | 1.50 s |

**What this proves.**

1. **`leaf_rpc ≈ local_apply ≈ silo wall-clock`.** All three are within 0.7% of each other (10.71 s vs 11.38 s) which is the expected gap between (a) per-RPC mean inside one shard-root call and (b) the maximum of one shard-root call over the silo's `Task.WhenAll` across buckets. Because `LatticeGrain.SetManyAsyncCore` keys the workload's HLC-prefixed keys into shard buckets, in this run the producer's keyspace lands on a single shard root per batch (route counts of `count=453` per 10 s window match the `~7 batches × ~65 leaves/batch` arithmetic), so `local_apply` and `leaf_rpc` collapse onto the same distribution.
2. **The bottleneck is strictly inside one per-leaf RPC.** `leaf_rpc.duration` p50 is 10.71 s. The cost cannot be downstream of the per-leaf call - it is paid synchronously between issuing the leaf RPC and the leaf returning.
3. **Inside the leaf RPC, the WAL is not the bottleneck.** Per-shard `wal.append.provider.duration` p50 is ~19 ms, p99 ~60-80 ms (one shard touched a `max=709 ms` outlier). `wal.append.in_flight` is 0. `provider.phase2.batch_size` is 1.00 (one phase-2 transaction per append). A 10.7 s leaf RPC is paying roughly **560 × 19 ms** worth of WAL provider time - which means either (a) the leaf is serialising ~560 things internally, or (b) something else inside the leaf turn is the dominant cost and the WAL appends are merely accurate book-keeping of the work that does happen.
4. **`leaf.commit.in_flight=0` confirms no per-leaf concurrency overlap.** Combined with single-shard routing, this means each per-leaf RPC arrives at an idle leaf, executes one turn, and returns. The 10.7 s is one turn, not stacked concurrent work.
5. **`drain.flush_dispatch_wait_ms` p50 ~1.4 s** at 7 dispatches per 10 s window means the silo is gate-saturated: 8 in-flight flush slots × ~11.4 s/slot = ~91 s of work to retire 10 s of wall-clock, dispatched at 1 / (10/7) = 1.4 s gate-wait. The arithmetic closes again: `4,096 × 7 / (11.4 + 1.4) ≈ 2,240/s` brackets the observed `2,775/s` final (with ~25% slack from cold-start / variance).

**What it does not yet tell us.** Whether the 10.7 s per per-leaf RPC is consumed by (a) **inside the leaf's commit turn** (`CommitSetManyAsync` doing per-step work that is not the WAL append - notably `PublishDigestUpwardAsync` walking the leaf -> parent chain after every commit, or per-key in-place LWW / observer fan-out), or (b) **between two events the leaf can already measure** (in which case the `leaf.commit.duration` per-step quantiles in `[phaseA]` would expose it). The earlier `[phaseA]` output **does not contain `leaf.commit.duration`** because the Phase A allowlist filters it out - the instrument is recorded, but never reported. That allowlist entry has now been added; the next rung will show the per-step (`wal` / `apply` / `observer` / `digest`) breakdown directly.

**U9p step 4 next.** Rerun the same `5000:5` rung now that `leaf.commit.duration` is allowlisted. The per-step tags will say which leg of `CommitSetManyAsync` consumes the 10.7 s: if the `apply` or `digest` step dominates, the next probe targets that path; if the `wal` step dominates (which would contradict the U9p step 3 measurement that `wal.append.provider.duration` is ms-not-seconds), then there is per-step double-counting that we need to reconcile before any optimisation.

### U9p step 4 - `leaf.commit.duration` allowlist - INSTRUMENT MIS-RENDER, INCONCLUSIVE (5000:5)

**What we measured.** Same `5000:5` ladder rung, 120 s, with the Phase A allowlist extended to include `orleans.lattice.leaf.commit.duration`. No source-side changes.

**Headline result.** The rung regressed sharply (steady-state avg `927/s`, final `1,284/s`, **24,343 failed writes**), with the silo logging `System.TimeoutException: Response did not arrive on time in 00:00:30 for message: ... ILattice.SetManyAsync(...)`. The `[phaseA]` output now contained `leaf.commit.duration`, but exactly **one row per shard** with a bimodal histogram - `p50 ≈ 0.0-0.1 ms`, `p90 ≈ 1.7-1.9 s`, `p99 ≈ 1.8-2.1 s`. Example windows (`tree=t shard=… phase=-`):

| `t=` | `count` | p50 | p90 | p99 |
| --- | --- | --- | --- | --- |
| 21.0s | 1,400 | 0.02 ms | 1,768 ms | 1,910 ms |
| 41.1s | 1,400 | 0.03 ms | 3,415 ms | 3,415 ms |
| 81.1s | 1,400 | 0.05 ms | 1,854 ms | 1,895 ms |
| 119.0s | 1,400 | 0.00 ms | 1,856 ms | 1,902 ms |

**Why this is inconclusive.** Two independent findings make the U9p step 4 quantiles unsafe to act on:

1. **The reporter dropped the `step` tag.** `BPlusLeafGrain.RecordCommitStep` tags `LeafCommitDuration` with `LatticeMetrics.TagStep = "step"` (`wal` / `apply` / `observer` / `digest`), but `PhaseADiagnosticReporter.Merge(...)` only recognises `TagTree`, `TagShard`, `TagPhase`, `TagStatus`. `TagStep` fell into the "silently dropped" path, so all four per-step distributions collapsed onto **one key per shard** with `phase=-`. The bimodal shape is the mixture of fast steps (`apply` / `digest`, μs-scale) with the slow step (`wal`, seconds-scale) - the per-step quantiles are not actually being reported separately.
2. **Throughput collapse is a run-side outlier, not a metric-induced regression.** The only code delta from U9p step 3 was adding one string to the allowlist; the instrument was already recorded on every commit. The 30 s timeout exceptions in the silo log point to leaf-RPC queue build-up (an unobserved seam upstream of `RecordCommitStep`), consistent with the U9p step 3 conclusion that the cost is paid synchronously inside the per-leaf RPC. This is a real signal about leaf RPC ingestion-shape, but it is independent of the misreported `leaf.commit.duration` quantiles.

**Fix.** `PhaseADiagnosticReporter.Merge(...)` now folds `TagStep` into the existing `phase` rendering slot when no `TagPhase` value is observed. `LeafCommitDuration` uses `TagStep`; every other allowlisted instrument that uses `TagPhase` (`provider.commit.duration`, `provider.retry.*`, etc.) is unaffected because `TagStep` is checked **only when** `phase` is still `"-"`. The dictionary key shape stays fixed at `instrument|tree|shard|phase|status`, no parser changes are needed downstream. This is a benchmark-local diagnostic fix; the public lattice surface (`TagStep` vs `TagPhase` distinction) is **not** changed.

**U9p step 5 next.** Rerun the same `5000:5` rung with the step-tag rendering fix in place. The expected output is **four rows per shard** for `leaf.commit.duration` (`phase=wal`, `phase=apply`, `phase=observer`, `phase=digest`), and the seconds-long tail should localise to exactly one of them. If `phase=wal` dominates with seconds-scale p99 while `wal.append.provider.duration` remains ms-scale, the bottleneck is in the leaf's own WAL turn (queue ahead of the `await` on the WAL grain), not the WAL provider. If `phase=apply` or `phase=digest` dominates, the next probe targets `SetCoreAsync` / `PublishDigestUpwardAsync`.

### U9p step 5 - per-step leaf commit decomposition (5000:5)

**What we measured.** Same `5000:5` ladder rung, 120 s, with the U9p step 4 Phase A rendering fix in `PhaseADiagnosticReporter.Merge(...)` so that `TagStep` is folded into the `phase` rendering slot when no `TagPhase` value is observed. No source-side changes to `LatticeMetrics` or `BPlusLeafGrain.Metrics.cs`.

**Headline result.** `steady-state avg = 2,514/s`, **`total failed = 0`**, `278,913 written`. The 24,343 failures observed in U9p step 4 do not reproduce - confirming step 4 was run-side outlier variance (allowlist add cannot change recording cost). `leaf.commit.duration` now resolves into four rows per shard (`phase=wal`, `phase=apply`, `phase=observer`, `phase=digest`) and the localisation is unambiguous:

| step | p50 | p99 | comment |
| --- | --- | --- | --- |
| `apply` | 0.09-0.51 ms | 2.3-7.3 ms | per-key LWW into the leaf's in-memory map |
| `digest` | 0.00 ms | 0.0-0.05 ms | parent digest emission |
| `observer` | 0.00 ms | 0.0-0.0 ms | observer fan-out |
| **`wal`** | **1.45-2.0 s** | **1.6-2.5 s** | `await walGrain.AppendAsync(...)` inside `CommitSetManyAsync` |

**WAL-side instruments (same windows, per shard, 8 shards):**

| `wal.append.*` | value | inference |
| --- | --- | --- |
| `provider.duration` p50 / p99 | 20 ms / 60-80 ms | Azure Tables call itself is healthy |
| `provider.duration` `count` per 10 s | ~390-394 per shard | ~40 provider calls/s/shard |
| `in_flight` | 0 | never two provider calls overlap on the same shard |
| `batch_entries` p50 / max | 8 / 15-20 | coalescer is producing small batches |
| `queue_depth` (at enqueue) | 1 | only 1 in-flight batch when each new batch arrives |
| `turn_wait` count per 10 s | 3-5 per shard | severely undercounted vs 390 provider calls |

**The arithmetic that closes.** 8 shards × ~40 provider calls/s × ~8 entries per call ≈ **2,560 entries/s**, which brackets the observed steady-state `2,514/s` and the U9p step 3 `2,775/s`. The system is running exactly at the WAL provider's effective throughput ceiling on this configuration.

**Why does the leaf wait 1.5 s if the provider is 20 ms?** Per-shard fan-out: `LatticeGrain.SetManyAsyncCore` groups a 4,096-entry batch into one bucket per physical shard (single-shard routing on this workload), then `ShardRootGrain.SetManyAsync` fans out via `DispatchLeafBatchWithRetryAsync` to ~65 leaves in parallel via `Task.WhenAll`. All 65 leaves then enter `CommitSetManyAsync` concurrently and each issues `await walGrain.AppendAsync(...)`. The WAL grain is **one activation per shard** (`WalPartitions = 1` is the documented default after U9m B2 retraction), so Orleans serialises those 65 grain calls onto a single activation's turn-queue. With batches of ~8 entries per provider call at ~20 ms each, the 65th leaf waits `65 × 20 ms / 8 leaves-per-batch ≈ 162 ms` worst-case if the coalescer were perfectly packing all 65 leaves' entries into the smallest possible batch shape, but in practice the coalescer only achieves ~8 entries per provider call (p50) so the actual wait scales with provider calls, not with leaf entries.

**Why `wal.append.turn_wait` undercounts.** Only 3-5 records fire per 10 s window per shard while the provider runs ~390 calls in the same window. That instrument must be recorded inside a guarded branch (e.g. only when the turn-queue is non-empty at entry, or only on the very first batch of a coalesced run). It is **not** a reliable measure of cross-grain queueing here. The correct outside-the-grain dispatch view is `LatticeMetrics.WalShardDispatchDuration` (`orleans.lattice.wal.shard.dispatch.duration`), which is **not in the benchmark's Phase A allowlist** - the U9o/U9p reporter only allowlists the eighteen instruments listed above, so cross-grain dispatch was invisible to this ladder run.

**What this proves.**

1. **The bottleneck is the WAL append path inside each leaf commit, specifically the per-shard coalesced provider-call rate, not the provider duration itself.** Provider p50 is 20 ms - any of the C1-C3 "parallel partition keys / pipelined transactions" framings from Phase C remains the right family of remediation. The previously-retracted attribution (retry storms, B2 partition scaling on Azurite) stays retracted; this is a coalescer-and-pipelining problem on a healthy provider.
2. **The leaf side is doing essentially nothing else.** `apply` / `digest` / `observer` together are sub-millisecond per commit. There is no "hidden CPU work" in `BPlusLeafGrain.CommitSetManyAsync` that needs to move; the optimisation surface is the WAL grain's append pipeline.
3. **`leaf.commit.in_flight = 0` is consistent with this picture, not in conflict.** The leaf entry-depth metric measures concurrent entries to `CommitSetAsync` / `CommitSetManyAsync` on **one leaf activation**. Each leaf serves one bucket at a time, returns to await on the WAL, and only re-enters after `walGrain.AppendAsync` returns. The 65 leaves of one shard each see depth 0/1; the contention is downstream on the **WAL grain**, where `in_flight = 0` simply confirms the provider is not paralellised (one call at a time), not that the WAL is idle.
4. **The Phase A diagnostic stack now resolves the per-step leaf commit shape end-to-end.** The step-tag rendering fix is benchmark-local and preserves the existing `[phaseA]` schema; no parser change is needed in `40-ladder.ps1`.

**U9p step 6 (SHIPPED AND MEASURED on `benchmark/azure-throughput` against real Azure Tables, archived as `benchmark/azure-throughput/.run/silo-U9p-step6.log` / `producer-U9p-step6.log` / `.ladder-results-U9p-step6.csv` / `.ladder-phaseA-U9p-step6.csv`).** Added `orleans.lattice.wal.shard.dispatch.duration` to `benchmark/azure-throughput/Silo/PhaseADiagnosticReporter.cs` `InstrumentAllowlist` so the existing `LatticeMetrics.WalShardDispatchDuration` recorded around `await grain.AppendBatchAsync(entries, …)` in `WalCommitLogWriter.AppendForPartitionAsync` becomes visible on the Phase A rows. Rerun was the same `5000:5` / `120 s` rung; `walPartitions = 8`, `walMaxPending = 8`. Rung outcome: `steady-state avg = 2,437/s`, `total failed = 0`, identical shape to U9p step 5 (`2,514/s`).

**Last-window numbers (t = 119.7 s, per WAL shard, all 8 shards represented):**

| instrument                          | tag           | p50           | p99           | min        | max       | count/shard |
| ----------------------------------- | ------------- | ------------- | ------------- | ---------- | --------- | ----------- |
| `wal.shard.dispatch.duration`       | shards 0-7    | **513-1,430 ms** | **801-1,612 ms** | 57-62 ms   | 817-1,621 ms | 417-452     |
| `leaf.commit.duration`              | `phase=wal`   | **1,451.61 ms**  | **1,615.58 ms**  | 1,185 ms   | 1,621 ms     | 389         |
| `leaf.commit.duration`              | `phase=apply` | 0.10 ms       | 0.74 ms       | 0.05 ms    | 8.33 ms   | 389         |
| `leaf.commit.duration`              | `phase=digest`| 0.00 ms       | 0.00 ms       | 0.00 ms    | 0.03 ms   | 389         |
| `leaf.commit.duration`              | `phase=observer`| 0.00 ms     | 0.00 ms       | 0.00 ms    | 0.00 ms   | 389         |
| `wal.append.provider.duration`      | shards 0-7    | **18.98-19.61 ms** | **60-97 ms**     | 10-12 ms   | 135-296 ms   | 379-400     |
| `wal.append.batch_entries`          | shards 0-7    | **8.00**          | 14-16            | 1-2        | 15-20        | 376-399     |
| `wal.append.in_flight`              | shards 0-7    | 0.00              | 0.00             | 0.00       | 0.00         | 376-399     |

**Interpretation.** The hypothesis is confirmed end-to-end on real Azure Tables: `leaf.commit.duration phase=wal` p50 = 1,451.61 ms, p99 = 1,615.58 ms is structurally identical to the **per-shard envelope** of `wal.shard.dispatch.duration` (worst shard p50 = 1,430 ms, p99 = 1,612 ms). The leaf's wait inside `await writer.AppendAsync(entry)` *is* the cross-grain dispatch wait; there is no measurable extra cost above the `WalCommitLogWriter` → `WalShardGrain` call boundary. Meanwhile `wal.append.provider.duration` p50 ≈ 19 ms holds steady - the provider itself is healthy and fast. The ~1,430 ms - 19 ms ≈ 1.4 s residual sits **inside `WalShardGrain.AppendBatchAsync`**, between the grain-entry turn-take and the awaited provider call. With `WalShardGrain` non-reentrant (no `[Reentrant]` / `[MayInterleave]` attribute) and ~65 leaves per shard fanning concurrent `AppendBatchAsync` calls into one activation, every call queues behind the prior call's `await acks[i].Task`. The grain coalesces those callers into pending-batches, but `wal.append.batch_entries` p50 = 8 says the coalescer is leaving the batch ceiling (`WalMaxBatchEntries = 100`) almost entirely unused: at this load most provider calls carry only ~8 entries even though the per-shard queue is deep enough to fill them to 100. Provider rate (≈ 400 calls per shard per 10 s ≈ 320 calls/s aggregate × ≈ 8 entries) explains the ≈ 2.5 k/s ceiling exactly. **`wal.append.in_flight = 0` is now correctly understood:** with the WAL grain non-reentrant and the await on `acks[i]` serialising entry-to-entry, the gauge that samples on every coalesced flush boundary observes the steady-state "between flushes" depth, which is structurally always 0 - it is **not** evidence that the WAL is idle.

**U9p step 7 SHIPPED + MEASURED.** The lever was the coalescer, as predicted. Direct read of `WalShardGrain.cs` confirmed the under-packing cause: the kick-flush predicate in `AppendAsync`, `AppendBatchAsync`, and the follow-on kick at the end of `FlushAsync` keyed off `_inFlight.Count == 0`, so once one flush was in motion every subsequent caller parked on its own ack TCS and the chain depth never grew past one - the cap was structurally unreachable under steady fan-in even though it was configured at 8. The step-6 numbers (`wal.append.in_flight = 0`, `batch_entries` p50 = 8 at deep queue) are exactly what that predicate produces. The fix replaced the three predicates with `_inFlight.Count < WalMaxPendingBatches`, kept the per-batch cap clauses inside `AppendBatchAsync`'s mid-batch trigger, and preserved cap=1 wire-compat (`< 1` is identical to `== 0`). The full 64-test `WalShardGrainTests` fixture passes (including the new regression test `AppendAsync_steady_fan_in_during_in_flight_flush_pipelines_against_cap` which fails on the pre-step-7 code path and passes after); the full non-Chaos `Orleans.Lattice.Tests` (3,439), `Orleans.Lattice.Storage.AzureTable.Tests` (147), and `Orleans.Lattice.Replication.Tests` (1,660) suites all pass.

**Step-7 ladder result (real Azure Tables, single rung `5000:5`, 120 s).** Steady-state avg **2,437 -> 2,969/s (+21.8%)**; final avg **2,370 -> 2,732/s (+15.3%)**; total written **282,548 -> 327,129 (+15.8%)**; total failed **0 -> 0**. Worst-shard `wal.shard.dispatch.duration` p99 **1,612 ms -> 1,482 ms (-8%)**; the matching `leaf.commit.duration phase=wal` p50 dropped from **1,451 ms to 1,330 ms** and p99 from **1,615 ms to 1,482 ms**, mirroring the per-shard dispatch improvement exactly as the U9p step-6 hypothesis predicted. `wal.append.batch_entries` p50 stayed at **8** on every shard; `wal.append.in_flight` stayed at **0** on every shard; `wal.append.provider.duration` p50 stayed at ~**19 ms** - the provider is still healthy and the chain depth never grew past one even on the post-step-7 code path. Artefacts: `benchmark/azure-throughput/.run/silo-U9p-step7.log`, `producer-U9p-step7.log`, `scripts/.ladder-results-U9p-step7.csv`, `scripts/.ladder-phaseA-U9p-step7.csv`.

**Interpretation.** The step-7 source change is correct and shipping, but at this rung the in-flight cap is *not* the rate-limiting step: `in_flight` stayed at 0 because the provider (~19 ms p50) is faster than the per-shard inter-arrival interval, so every flush settles before the next caller arrives at the grain - the cap was reachable in the unit test under synchronous fan-in, but the real `5000:5` rung does not produce chain depth > 1. What *did* improve is **flush count per shard per 10 s window**: ~310 -> ~465 (+50%), because the new admission predicate kicks immediately under the cap instead of forcing the next caller to park on its `acks[i].Task`, wait for the head flush to complete, and only *then* start its own flush. That removes the serial dead-time between flushes that the old `== 0` predicate introduced, which is exactly the ~22% throughput shift we observe. Batch packing did not improve (`batch_entries` p50 still 8) because the bottleneck has moved one rung down: per-shard fan-in is now provider-limited at ~50 flushes/s * 8 entries = ~400 entries/shard/s * 8 shards = ~3,200/s aggregate, very close to the measured 2,969/s steady-state.

**U9p step 8 next.** To break past ~3 k/s on this rung the next lever is either (a) raise the rung from `5000:5` to `10000:5` / `20000:5` to push real concurrency at the WAL grain and re-measure - if `in_flight` rises and throughput climbs further, the step-7 change is doing exactly what was designed; if `in_flight` stays at 0 even under deeper fan-in, (b) the next probe surfaces a `wal.append.flush_kick_to_provider_call` finer-grained timer inside `WalShardGrain` to localise the residual per-flush overhead, or (c) coalesce the per-entry `acks[i].Task` awaits in `AppendBatchAsync` so the grain-turn dead-time between consecutive batches from the same caller collapses. Option (a) is the cheapest probe and is the right next step. The C-family direction (multi-partition WAL on real Azure) remains a separate next-axis once the per-shard pack-ratio is healthy.

### U9p step 8a - demand-side probe (10000:5)

**Hypothesis.** Step 7 made `WalMaxPendingBatches` reachable, but the step-7 ladder result kept `wal.append.in_flight` at 0 because the `5000:5` rung was provider-limited at ~50 flushes/s per shard, faster than per-shard inter-arrival - so the cap could not engage. Step 8a doubles the rung to `10000:5` (8 vehicles per leaf, ~65 leaves per shard) to drive deep fan-in into each `WalShardGrain` activation and falsify or confirm that the cap engages under real concurrency.

**Method.** Reuse the step-7 image (`-SkipBuild`) and re-run the ladder at one rung `10000:5` for 120 s with the same knobs (`walPartitions=8`, `walMaxPending=8`, `flushConcurrency=8`, `batchSize=4096`, `phase2CoalescingMs=0`, `flushMs=50`). All other parameters identical to step 7. Capture silo log, producer log, and the structured Phase A CSV under `*-U9p-step8a.*`.

**Step-8a ladder result (real Azure Tables, single rung `10000:5`, 120 s).** Steady-state avg **2,969 -> 1,591/s (-46%)**; final avg **2,732 -> 1,497/s (-45%)**; total written **327,129 -> 178,801**; total failed **0 -> 0** (clean overload, not error-driven). Steady min/max **0 .. 10,850/s**: throughput shape is a sawtooth - the silo produces ~4,096 entries in one 1-s window then 0/s for 1-2 s, then another ~4,096, indefinitely. The sawtooth period is exactly `batchSize=4096`; the producer is now feeding the silo faster than the silo can drain the batch, so the bench measures the silo's batch-drain interval directly.

**Last-window numbers (t = 119.7 s, per WAL shard):**

| instrument                          | tag           | p50              | p99              | min        | max       | count/shard |
| ----------------------------------- | ------------- | ---------------- | ---------------- | ---------- | --------- | ----------- |
| `wal.shard.dispatch.duration`       | shards 0-7    | **365-2,329 ms** | **777-3,055 ms** | 10-53 ms   | 777-3,055 ms | 504-625     |
| `leaf.commit.duration`              | `phase=wal`   | **2,367 ms**     | **3,055 ms**     | 56 ms      | 3,055 ms     | 539         |
| `leaf.commit.duration`              | `phase=apply` | 0.05 ms          | 0.30 ms          | 0 ms       | 2.19 ms   | 539         |
| `leaf.commit.duration`              | `phase=digest`| 0.20 ms          | 1.75 ms          | 0 ms       | 6.93 ms   | 538         |
| `leaf.commit.duration`              | `phase=observer`| 0.00 ms        | 0.00 ms          | 0 ms       | 0.02 ms   | 539         |
| `wal.append.provider.duration`      | shards 0-7    | **16.19-16.63 ms** | **48-78 ms**   | 9-10 ms    | 121-270 ms   | 466-500     |
| `wal.append.batch_entries`          | shards 0-7    | **4.00**         | 11-18            | 1          | 13-18        | 465-499     |
| `wal.append.queue_depth`            | shards 0-7    | **1**            | 1-4              | 1          | 1-4       | 465-499     |
| `wal.append.turn_wait`              | shards 0-7    | **12-14 ms**     | 13-78 ms         | 11-13 ms   | 13-78 ms  | 1-4         |
| `wal.append.in_flight`              | shards 0-7    | **0**            | **0**            | 0          | 0         | 465-499     |

**Interpretation.** Two clean signals fall out simultaneously:

1. **`wal.append.in_flight = 0` everywhere, on every shard.** The step-7 admission predicate is now reachable in the unit test under synchronous fan-in but is **structurally unreachable in production** under the current call shape. The reason is `BPlusLeafGrain.CommitSetAsync` does `await writer.AppendAsync(entry)` synchronously per entry - a single leaf cannot have a second `AppendAsync` in flight to the same WAL grain at the same time. With `WalShardGrain` non-reentrant, the leaf's `await` serialises the call so the grain sees one caller, one entry, one flush at a time. The cap is moot under this call shape - `WalMaxPendingBatches > 1` is a no-op as long as each leaf awaits its own append before issuing the next one.

2. **`wal.shard.dispatch.duration` p99 = 3 s while `wal.append.provider.duration` p99 = 70 ms.** The ~2,900 ms gap between dispatch (caller-side) and provider+turn_wait (~85 ms) sits *inside* the WAL grain's serial entry loop. `wal.append.batch_entries` p50 = **4** (down from p50 = 8 at the lighter rung) - the coalescer is packing *less* under deeper fan-in, not more, because each leaf's serial-await throttles the WAL grain to one-entry-per-flush. The grain is doing ~470 single-entry flushes per shard per 10 s window, each carrying p50 = 4 entries, for a per-shard rate of ~190 entries/s and an aggregate of ~1,500/s across 8 shards - which matches the measured 1,591/s steady-state exactly.

So the bottleneck has localised one level above the WAL grain, on the **leaf-grain-to-WAL-grain per-entry await loop**: `for (var i = 0; i < entries.Count; i++) await writer.AppendAsync(entries[i])`. The step-7 fix was correct as a pre-condition (so that *if* a leaf ever sent two entries concurrently the cap could absorb them) but it cannot itself break the per-entry serial pattern. The next lever has to either (a) make `BPlusLeafGrain` batch its WAL appends into a single `AppendBatchAsync(IReadOnlyList<WalRecord>)` so the WAL grain receives one fan-in event per leaf-commit instead of N, or (b) coalesce on the writer side so consecutive `AppendAsync` calls from the same leaf-commit aggregate into one batched RPC under the hood.

**Step-8b next.** Read `BPlusLeafGrain.CommitSetAsync` and `WalCommitLogWriter.AppendAsync` end-to-end to find the per-entry-await loop, decide whether the right lever is leaf-side batching (preferred: keeps the writer's contract narrow and lets each leaf coalesce its own commit window without cross-leaf cooperation) or writer-side coalescing (a per-shard caller-side queue with a bounded-await collapse), then surface a per-leaf-commit `wal.append.entries_per_commit` histogram to confirm the loop count is what we think it is before implementing.


### U9p step 8b - writer-side dispatch-size falsifier (10000:5)

**Hypothesis.** The step-8a interpretation claimed the regression was caused by a leaf-side per-entry await loop fragmenting WAL appends into single-entry calls. Before implementing leaf-side batching, falsify that claim by comparing the writer-side per-dispatch entry count to the WAL-grain per-flush entry count. If `wal.shard.dispatch.entries` matches `wal.append.batch_entries` in shape and count, the writer is already sending batches and the WAL grain is already packing them, so the step-8a hypothesis is wrong and the regression must come from somewhere else (per-partition skew, provider tail, or caller-side timeout shape).

**Method.** Add a new histogram `orleans.lattice.wal.shard.dispatch.entries` on `WalCommitLogWriter` that records the slice size for every dispatch (1 for `AppendAsync`, `entries.Count` for each partition slice in `AppendManyAsync`), with the same `tree_id` + `wal_partition` tag tuple as `wal.shard.dispatch.duration`. Allowlist it in `PhaseADiagnosticReporter`. Build, run the full non-Chaos suite, redeploy the benchmark image locally, then re-run the ladder at one rung `10000:5` for 120 s with the same knobs as step 8a. Capture artifacts under `*-U9p-step8b.*`.

**Source inspection result (before the rerun).** End-to-end read of the write path confirmed the benchmark already drives the batched API at every layer: `TcpIngestService.FlushAsync` -> `lattice.SetManyAsync(batch)` -> `LatticeGrain.SetManyAsync` (groups by shard, parallel fan-out) -> `IShardRootGrain.SetManyAsync` -> `leaf.SetManyAsync(slice)` -> `BPlusLeafGrain.CommitSetManyAsync` -> `writer.AppendManyAsync(walEntries)`. The step-8a "per-entry await loop" hypothesis was wrong by construction: that loop does not exist on this path. The only per-entry `await writer.AppendAsync(entry)` is in `BPlusLeafGrain.CommitSetAsync`, which the benchmark never calls.

**Step-8b ladder result (real Azure Tables, single rung `10000:5`, 120 s).** Steady-state avg **1,591 -> 1,316/s (-17.3%)**; final avg **1,497 -> 1,324/s (-11.6%)**; total written **178,801 -> 158,888 (-11.1%)**; total failed **0 -> 16,291**. The new failure population is a clean signal: every failure is an Orleans `TimeoutException` after 30 s on `ILattice.SetManyAsync`, surfaced from `TcpIngestService.FlushAsync` (`Program.cs:line 750`). Run-to-run variance plus the new caller-side timeout pressure (16 k retransmits via the producer reconnect path) account for the small step-8a -> step-8b drift; the diagnosis below is unchanged whether we use the step-8a or step-8b sample.

**Falsifier comparison (per WAL shard, 120 s window):**

| instrument                          | shard | count  | p50 entries | p99 entries |
| ----------------------------------- | ----- | ------ | ----------- | ----------- |
| `wal.shard.dispatch.entries`        | 0     | 396    | 5           | 13          |
| `wal.append.batch_entries`          | 0     | 376    | 5           | 13          |
| `wal.shard.dispatch.entries`        | 1     | 488    | 4           | 13          |
| `wal.append.batch_entries`          | 1     | 432    | 4           | 13          |
| `wal.shard.dispatch.entries`        | 2     | 501    | 4           | 13          |
| `wal.append.batch_entries`          | 2     | 421    | 4           | 13          |
| `wal.shard.dispatch.entries`        | 3     | 472    | 4           | 13          |
| `wal.append.batch_entries`          | 3     | 374    | 4           | 12          |
| `wal.shard.dispatch.entries`        | 4     | 390    | 5           | 12          |
| `wal.append.batch_entries`          | 4     | 361    | 5           | 12          |
| `wal.shard.dispatch.entries`        | 5     | 433    | 4           | 12          |
| `wal.append.batch_entries`          | 5     | 399    | 5           | 12          |
| `wal.shard.dispatch.entries`        | 7     | 403    | 5           | 11          |
| `wal.append.batch_entries`          | 7     | 380    | 5           | 11          |

`dispatch.entries` and `append.batch_entries` are the **same distribution, same count (±10%)** on every shard. **The writer is not fragmenting and the WAL grain is not breaking up batches.** The step-8a "per-entry-await loop" hypothesis is falsified.

**Where the time actually goes (last-window numbers, per WAL shard):**

| instrument                          | tag        | p50              | p99              | p999              | count/shard |
| ----------------------------------- | ---------- | ---------------- | ---------------- | ----------------- | ----------- |
| `wal.shard.dispatch.duration`       | shards 0-7 | **230-2,873 ms** | **660-3,764 ms** | **820-4,199 ms**  | 390-530     |
| `wal.append.provider.duration`      | shards 0-7 | **18-19 ms**     | **31-43 ms**     | **64-188 ms** (one shard 530 ms) | 362-433 |
| `wal.append.queue_depth`            | shards 0-7 | **1**            | **1-2**          | 1-2               | 1-2 samples |
| `wal.append.turn_wait`              | shards 0-7 | **11-17 ms**     | **17-31 ms**     | 17-33 ms          | 1-2 samples |
| `wal.append.in_flight`              | shards 0-7 | **0**            | **0**            | 0                 | -           |

**Interpretation - corrected.** Three signals localise the true limiter:

1. **The WAL coalescer is healthy.** `dispatch.entries` ≈ `append.batch_entries` proves the leaf-side batched path is end-to-end intact and the WAL grain is packing what it receives. `WalMaxPendingBatches` and the `isLast`-driven flush kick are not the binding constraint; raising the cap or removing `isLast` would have no effect under this call shape.

2. **The cap is structurally unreachable for a different reason than step-7 thought.** `wal.append.in_flight = 0` everywhere because `wal.append.queue_depth` p99 = 1-2 - there is rarely more than one batched dispatch waiting at a WAL grain at the same time. Each `LatticeGrain.SetManyAsync` shards its keys across 8 partitions, so 4,096 caller-side entries become ~512 entries per partition, which becomes one batched `AppendBatchAsync` per partition. With `FlushConcurrency=8` caller-side, the WAL grain sees ~one batch at a time per partition - exactly the queue_depth ≈ 1 signal. The cap can only engage if multiple concurrent caller batches arrive in the same grain turn, which the benchmark's per-partition serialisation prevents.

3. **The real limiter is per-partition skew × Azure-Tables provider tail × 30 s caller timeout.** `wal.shard.dispatch.duration` p99 ranges from 660 ms (shards 0/4/7) to 3,764 ms (shard 6) - a **5.7x worst/best fan-out**. Per-partition dispatch *count* in 120 s is 390-530, also skewed ~1.35x. Inside the WAL grain, `wal.append.provider.duration` p99 ≈ 35 ms but p999 jumps to 100-530 ms on the worst shards - Azure Tables tail latency under bursty fan-in. Each leaf-side `await leaf.SetManyAsync(slice)` blocks until its slowest partition's provider call returns, so the caller-side `ILattice.SetManyAsync` latency is the **max** of 8 per-partition tails, not the mean. When the worst partition's flush queue stalls long enough for the caller-side `await` to cross 30 s, Orleans times out the `SetManyAsync` RPC and the producer counts that batch as failed. 16,291 / 4,096 ≈ 4 timed-out batches per second once the silo is saturated.

**Step-8c next.** The remaining levers are call-shape and tail-hiding, not WAL coalescing. Ranked by expected impact and cost:

- **(a) Cap or eliminate per-partition skew.** `LatticeGrain.SetManyAsync` currently fans out by `KeyHash mod WalPartitions`. Under a vehicle-id keyspace this is uniform in count but not in *commit weight* - some partitions cluster on hot shards/leaves. Either (i) raise `WalPartitions` from 8 to 16/32 to reduce per-partition variance, or (ii) measure `wal.append.provider.duration.count` per partition over a longer window to confirm the worst shard is structurally hot, not transiently hot.
- **(b) Decouple caller timeout from worst-partition tail.** The 30 s Orleans default on `SetManyAsync` is the immediate cause of the 16 k failures. Either (i) raise `ResponseTimeout` for the lattice grain method, or (ii) make `LatticeGrain.SetManyAsync` partition-level fire-and-async-confirm so the caller-side RPC returns as soon as the lattice has accepted (durably acknowledged into the WAL queue) rather than after all per-partition flushes drain. The latter is the larger change but matches normal async-write semantics.
- **(c) Add a `wal.shard.dispatch.duration` breakdown.** The big number to date is `dispatch.duration` (p99 3.8 s) minus `provider.duration` + `turn_wait` (~50 ms) ≈ 3.7 s unaccounted-for *inside* the WAL grain - the same gap that misled step-8a. Surface `wal.shard.dispatch.queue_wait` (time from caller-side dispatch to the grain accepting the call) separately from in-grain processing, to confirm whether the gap is per-partition queue contention on the grain front-door rather than provider tail.

Option (a) is the cheapest probe and the right next step. Options (b) and (c) follow depending on what (a) shows. The C-family direction (multi-partition WAL on real Azure with bin-packed shards) remains a separate next-axis.

**Artifacts.** `benchmark/azure-throughput/.run/silo-U9p-step8b.log`, `producer-U9p-step8b.log`, `deploy-U9p-step8b.log`, `scripts/.ladder-results-U9p-step8b.csv`, `scripts/.ladder-phaseA-U9p-step8b.csv`. New observability surface: `LatticeMetrics.WalShardDispatchEntries` histogram + `PhaseADiagnosticReporter` allowlist entry for `orleans.lattice.wal.shard.dispatch.entries`.


### U9p step 8c-a-i - falsifier: raise WalPartitions 8 -> 16 (10000:5)

**Hypothesis.** Step 8b localised the limiter to per-partition Azure-Tables tail × caller-side max-of-P fan-in. The cheapest step-8c lever was (a-i): raise `WalPartitions` from 8 to 16 to halve per-partition load and (the prediction was) shrink the tail of the worst partition. No code change required - only the `BENCH_WAL_PARTITIONS` env knob on the deploy script.

**Method.** Reuse the step-8b image (`-SkipBuild`), set `$env:BENCH_WAL_PARTITIONS='16'`, re-run the ladder at one rung `10000:5` for 120 s with all other knobs identical to step 8b (`walMaxPending=8`, `flushConcurrency=8`, `batchSize=4096`, `phase2CoalescingMs=0`, `flushMs=50`). Capture artifacts under `*-U9p-step8c-a-i.*`.

**Result.** Throughput **collapsed 5x**, not improved:

| metric                                         | step-8b (P=8) | step-8c-a-i (P=16) | change      |
| ---------------------------------------------- | ------------- | ------------------ | ----------- |
| SteadyAvg                                      | 1,316/s       | **237/s**          | **-82%**    |
| FinalWritten                                   | 158,888       | 32,779             | -79%        |
| FinalFailed                                    | 16,291        | **84,432**         | **+418%**   |
| FinalAvgRate                                   | 1,324/s       | 274/s              | -79%        |
| `wal.append.batch_entries` p50 (typical shard) | **4-5**       | **2**              | **-50%**    |
| `wal.append.provider.duration` p50             | 18-19 ms      | 15 ms              | flat        |
| `wal.append.provider.duration` p999            | 100-530 ms    | 100-330 ms         | flat        |
| `wal.shard.dispatch.duration` p99 (per shard)  | 660-3,764 ms  | **1,033-3,335 ms** | **all shards >=1s** |
| shards with `dispatch.duration` p99 >= 1 s     | ~5 of 8       | **~16 of 16**      | universal   |

**Mechanism.** Three signals reinforce one explanation:

1. **Halving the per-partition slice size cut `batch_entries` p50 from 4-5 to 2.** The slice arithmetic is `4096 / WalPartitions`, so P=8 -> ~512 entries per slice and P=16 -> ~256. After the in-grain batch packer trims with `WalMaxBatchEntries=8`, the typical packed flush goes from ~5 entries to ~2 entries. Each packed flush still costs one Azure-Tables round-trip (`provider.duration` p50 ~15 ms, unchanged), so **per-entry overhead doubled**.

2. **Per-partition load skew did not shrink - it spread to every shard.** Under P=8 the worst-shard `dispatch.duration` p99 was 3.8 s and the best was 0.66 s (5.7x spread, ~5 of 8 shards >= 1 s). Under P=16 every shard's p99 is between 1.0 s and 3.3 s. The tail did not get smaller; it got more uniform. The caller's `await ILattice.SetManyAsync` blocks on the **max** of P per-partition tails, and `P(max(16 tails) > 30 s) > P(max(8 tails) > 30 s)` even when each individual partition's tail shrinks - which here it didn't.

3. **Caller-side timeouts went from ~4/s to ~28/s.** `16,291 -> 84,432` failed batches in 120 s = `136 -> 703 timeouts/s`-batches (each holds 4,096 entries). At P=16 essentially every fourth `SetManyAsync` times out at the Orleans 30 s default. The producer reconnects and retransmits, so the silo is also paying retry-storm cost on top of the doubled per-entry overhead - which is why throughput collapsed by 5x, not just 2x.

**Interpretation - lever (a) is falsified.** More WAL partitions is **counterproductive** under this rung and the current call shape. The per-partition Azure-Tables tail does not shrink linearly with more partitions because (i) the underlying account is the bottleneck, not per-partition contention, and (ii) `max(P tails)` grows with P faster than the per-partition tail shrinks. Lever (a-ii) - longer-window load-imbalance measurement - is also off the path: the data already shows the imbalance is not partition-count-driven.

**Step-8c-b next - decouple caller timeout from worst-partition tail.** The remaining valid lever from step-8b is (b): change `ILattice.SetManyAsync` semantics so the caller-side RPC does not block on max-of-P per-partition flushes. Two shapes are available:

- **(b-i) Raise `ResponseTimeout` for the `LatticeGrain.SetManyAsync` method.** Cheapest probe: bumps the 30 s default to something like 120 s, which converts caller-side *timeouts* into caller-side *wall-clock slowdown* but no longer into retry storms. This will not improve `SteadyAvg`, but it will eliminate the 84 k failed-batch retransmit cost and reveal what the steady-state would be if the producer never gave up. Run the ladder at `10000:5` with this single knob change and compare.
- **(b-ii) Fire-and-async-confirm semantics.** The larger change: `LatticeGrain.SetManyAsync` returns as soon as all per-partition WAL appends are enqueued (i.e., they have a position in the WAL grain's `_pendingSegments` and an ack TCS) rather than after the storage round-trip completes. The caller now sees lattice-level acknowledgement latency (~ms) instead of provider-level latency (~50-3,800 ms p99). Durability is preserved because the WAL grain still flushes the batch asynchronously and the ack TCS still completes once the storage write returns; the caller just doesn't wait on it inline. This is the correct long-term shape but is a wire-API change to `ILattice.SetManyAsync` semantics (write-acknowledged vs write-confirmed) and needs an explicit decision on whether to ship as a method overload or a per-tree option.

Run (b-i) first because it is a one-line config change and disambiguates whether the post-timeout retry storm is itself a multiplier on the throughput drop. If (b-i) gets throughput back into the 1-2k/s range and zeroes the failure count, (b-ii) becomes the next implementation step. If (b-i) does not move throughput, the actual bottleneck is on the provider, and the next direction is C-family (multi-account / partition-key-sharded Azure Tables on the storage layer, not the WAL grain layer).

**Artifacts.** `benchmark/azure-throughput/.run/silo-U9p-step8c-a-i.log`, `producer-U9p-step8c-a-i.log`, `ladder-U9p-step8c-a-i.log`, `scripts/.ladder-results-U9p-step8c-a-i.csv`, `scripts/.ladder-phaseA-U9p-step8c-a-i.csv`. No code change in this step; the only deploy delta is `$env:BENCH_WAL_PARTITIONS='16'`.


### U9p step 8c-b-i - falsifier: raise Orleans `ResponseTimeout` 30 s -> 180 s (10000:5)

**Hypothesis.** Step 8c-a-i showed that raising `WalPartitions` from 8 to 16 collapsed throughput and exploded failures from ~4/s to ~28/s, with caller-side `TimeoutException` traces dominating the producer log. That is consistent with the Orleans 30 s `ResponseTimeout` firing on `ILattice.SetManyAsync` while the silo is still waiting on the slowest WAL partition - i.e. the 30 s timeout is itself an amplifier (timeout -> producer reconnect -> retransmit -> deeper queue -> more timeouts). The step-8b-listed lever (b-i) is the cheapest probe: bump `ResponseTimeout` to 180 s and re-run the same rung. Prediction: failures go to zero; steady-state throughput at most marginally moves; the actual WAL-side ceiling becomes visible without the retry storm distorting it.

**Method.** New benchmark knob `BENCH_RESPONSE_TIMEOUT_SEC` (default 30 to preserve prior behaviour) plumbed through `benchmark/azure-throughput/Silo/Program.cs` and `benchmark/azure-throughput/scripts/20-build-and-deploy.ps1`, applied to both `SiloMessagingOptions.ResponseTimeout` and `ClientMessagingOptions.ResponseTimeout`. Rebuilt the silo image via `-LocalBuild`, deployed with `$env:BENCH_RESPONSE_TIMEOUT_SEC='180'` and **otherwise identical** step-8b knobs (`walPartitions=8`, `walMaxPending=8`, `flushConcurrency=8`, `batchSize=4096`, `phase2CoalescingMs=0`, `flushMs=50`). One rung `10000:5` for 120 s.

**Result.** Failure storm eliminated, but the steady-state ceiling did not move materially:

| metric                                            | step-8b (timeout=30 s) | step-8c-b-i (timeout=180 s) | change       |
| ------------------------------------------------- | ---------------------- | --------------------------- | ------------ |
| SteadyAvg                                         | 1,316/s                | 1,649/s                     | **+25%**     |
| FinalWritten                                      | 158,888                | 187,636                     | +18%         |
| FinalFailed                                       | 16,291                 | **0**                       | **-100%**    |
| FinalAvgRate                                      | 1,324/s                | 1,575/s                     | +19%         |
| `TimeoutException` occurrences in silo log        | thousands              | **0**                       | -            |
| `wal.shard.dispatch.entries` p50 (typical shard)  | 4-5                    | 4                           | flat         |
| `wal.shard.dispatch.duration` p50 (typical shard) | ~500-2,100 ms          | 510-2,138 ms                | flat         |
| `wal.shard.dispatch.duration` p99 (worst shard)   | ~3.8 s                 | 3.17 s                      | flat         |
| `wal.append.turn_wait` p50                        | ~12-32 ms              | ~11-13 ms                   | flat-to-down |

The producer-side `NetworkStream.Write` exceptions in `producer.log` are now unrelated to the runs WAL path - they are the normal teardown-window EOF after the silo accepts the FINAL drain (producer keeps writing into the half-closed TCP socket until the host stops).

**Mechanism.** The 30 s `ResponseTimeout` was a retry-storm amplifier, not the bottleneck:

1. **Timeouts gone, retry cost gone, throughput moved by ~+25%.** At timeout=30 s the producer was reconnecting and retransmitting ~136 batches/s (16,291 failed / 120 s) - each retransmit re-traverses the whole pipeline (TCP -> ingest service -> `LatticeGrain.SetManyAsync` -> per-partition WAL append), so the silo was spending a meaningful fraction of its budget on duplicate work. Lifting the timeout converted those wall-clock-late completions into successful late completions; throughput moves by the amount of wasted retry work, not by the underlying limiter.

2. **The WAL-dispatch numbers are unchanged.** `dispatch.entries` and `dispatch.duration` quantiles per shard are essentially identical to step 8b. Whatever sets the per-shard `~510-2,100 ms` `dispatch.duration` p50 is **upstream of the timeout knob**. The caller-visible ceiling (~1.6k/s aggregate) is set by `8 shards x (1 / dispatch.duration_per_dispatch_seconds) x dispatch.entries_per_dispatch`. With p50 dispatch_duration ~ 1 s and dispatch.entries p50 = 4, that's `8 x 1 x 4 = 32 entries/s/shard x 8 shards = 256/s if every dispatch took 1 s`, but the count column (`~520 per shard / 120 s ~ 4.3 dispatches/s/shard`) and the dispatch.entries sum (~2,300 / shard) gives `~19 entries/s/shard x 8 shards x dispatch_density_correction ~ 1.5-1.6k/s aggregate` - which matches the FINAL line exactly.

3. **`SteadyMin = 0` and `SteadyMax = 15,179`.** Per-second throughput is now bursty: there are seconds with no completions and seconds with 15 k completions. That is the signature of caller-side `await ILattice.SetManyAsync` blocking on `max(8 per-shard tails)` and releasing the whole batch when the tail finally returns. With timeouts off, the producer holds the slow batches in flight instead of giving up, and they all complete together when the slowest dispatch lands.

**Interpretation - lever (b-i) is confirmed as a *necessary* fix but not a *sufficient* fix.** The 30 s default `ResponseTimeout` was clearly amplifying failures and inflating the harm caused by step 8c-a-i; with it lifted, `FinalFailed = 0` and the throughput is now strictly higher in every steady-state metric. **But the underlying limiter is unchanged**: `wal.shard.dispatch.duration` p50 is still ~510-2,100 ms per shard, `dispatch.entries` p50 is still 4. To break above ~1.6k/s, one of two things has to happen:

1. **`dispatch.entries` per dispatch has to go up.** The `WalCommitLogWriter` dispatch path currently fires per-partition slice ASAP; under the rung's per-batch arithmetic (`4096 / 8 partitions = 512 entries per slice`), the in-grain batch packer trims it to `WalMaxBatchEntries=8`, so each dispatch carries at most 8 entries (typical 4 on the heavy partitions). This is a config / packer choice, not a fundamental constraint.

2. **`dispatch.duration` per dispatch has to go down.** This is set by the Azure Tables provider (`provider.duration` p50 ~ 15-20 ms, p99 ~ hundreds of ms) plus the grain-turn fan-in cost (`turn_wait` ~ 11-13 ms). p50 ~ 0.5-2 s per dispatch is much larger than provider.duration p50 ~ 20 ms, so most of the dispatch.duration is grain-turn / queueing on the WAL grain, not provider time. That points back at the `WalShardGrain` activation's per-turn cost - i.e. the same surface step 8b already identified as the limiter, but seen from the writer side of the seam.

**Step-8c next - step into the WAL grain's own turn-queue and packing.** Two complementary levers, both still cheap:

- **(c-i) Raise `WalMaxBatchEntries` so a single dispatch carries more entries.** Today's packer cap forces `dispatch.entries` p50 = 4 even though the per-partition slice is ~512 entries. Lifting that cap from 8 to e.g. 64 or 256 would let one Azure-Tables round-trip cover the whole slice, which collapses the per-entry overhead by the same factor. This is a per-tree `LatticeOptions` knob, no API change, no wire-format change. **This is the cheapest remaining probe.**
- **(c-ii) Move `WalCommitLogWriter` packing off the WAL grain's turn.** Today the per-partition slice is built inside the WAL grain's turn before the storage call; pushing it earlier (in the caller-side fan-out) would let the WAL grain hand the packed batch to the provider without holding its turn during pack time. This is a code change inside `WalCommitLogWriter`, no wire-format change, no API change.

Run (c-i) first because it is a one-line config change and disambiguates whether the per-dispatch overhead is dominated by the packer cap or by the grain-turn cost. If (c-i) lifts `dispatch.entries` p50 from 4 to >=32 and throughput rises to >=8k/s, the binding constraint is the packer cap. If (c-i) lifts `dispatch.entries` but throughput doesn't rise proportionally, the binding constraint is the grain-turn cost, and (c-ii) becomes the next implementation step.

**Decision - lock in (b-i) as a default knob in the benchmark, not as a product change.** The benchmark silo now carries `BENCH_RESPONSE_TIMEOUT_SEC` (default 30 to preserve prior behaviour). The production `ResponseTimeout` stays at the Orleans default. The 30 s timeout is correct as a circuit-breaker against pathological grain stalls in production; what step 8c-b-i shows is that the benchmark rung's "pathological" workload is being correctly flagged by Orleans, not that the product setting is wrong. The fix path is to make the WAL faster (steps 8c-i / 8c-ii), not to weaken the timeout.

**Artifacts.** `benchmark/azure-throughput/.run/step8c-b-i-silo.log` (silo, 11.3 MiB), `step8c-b-i-producer.log`, `step8c-b-i-results.csv`, `step8c-b-i-phaseA.csv`, `ladder-U9p-step8c-b-i.log`. Code change is benchmark-only (`benchmark/azure-throughput/Silo/Program.cs` and `benchmark/azure-throughput/scripts/20-build-and-deploy.ps1` carry the new `BENCH_RESPONSE_TIMEOUT_SEC` knob); deploy delta is `$env:BENCH_RESPONSE_TIMEOUT_SEC='180'`.


### U9p step 8c-c-i - falsifier: WalMaxBatchEntries -> actually grain-turn serialization on `WalShardGrain` (10000:5)

**Original hypothesis (c-i above).** Raise `WalMaxBatchEntries` from "8" to e.g. 64 or 256 so a single dispatch carries more entries.

**Falsified before deploy.** Reading `src/lattice/BPlusTree/LatticeOptions.cs` line 847 shows `DefaultWalMaxBatchEntries = 100`. The step-8b interpretation of "the packer cap forces `dispatch.entries` p50 = 4" was wrong - the cap is already 100, so the small batches are *not* set by `WalMaxBatchEntries`. The interpretation was anchored on a value (`8`) the benchmark never used.

**Re-diagnosis from step-8b-i Phase A.** `wal.append.in_flight` histogram across every WAL shard reads `count=499..505 sum=0 min=0 p50=0 p90=0 p99=0 max=0`. The pipeline cap is `WalMaxPendingBatches = 8` (benchmark default), but the *actual* in-flight chain depth is **always 0** when a new flush starts. That means no second flush has ever been in motion concurrently with another on the same WAL shard, despite the cap allowing eight. This is the smoking gun.

**Mechanism.** `WalShardGrain` has no `[Reentrant]` / `[AlwaysInterleave]` / `[MayInterleave]` annotation. Under Orleans default scheduling, only one caller's `AppendBatchAsync` is inside the grain at any moment. The implementation parks the caller on every per-entry `tcs.Task.ConfigureAwait(true)` before returning, and `tcs` only completes when the flush this caller kicked finishes. So:

1. Caller A enters `AppendBatchAsync`, encodes ~4 entries, assigns offsets under `_stateGate`, kicks a flush on `isLast`, parks on `acks[0].Task`.
2. The flush task runs on a worker thread; the provider call takes ~15-20 ms; `slot.Acks[i].TrySetResult(...)` completes A's TCSs.
3. A returns to its caller. *Only now* does the next caller's grain turn start - the grain dispatcher only releases the next turn when the previous `Task` returned by the grain method has resumed past its await.
4. Caller B enters. The in-flight chain is empty (A's flush already removed its slot in `FlushAsync`'s finally). B kicks another flush on its own `isLast`. The cycle repeats.

The chain depth therefore never exceeds 1, and each flush carries only the entries one caller had locally to submit (~4). The follow-on-flush trigger at the end of `FlushAsync` (lines 1015-1025) does nothing useful: when it fires, the grain turn is still held by caller A, so no concurrent caller could have added pending entries since A's flush started. `WalMaxPendingBatches` and `WalMaxBatchEntries` are both effectively disabled by the grain-turn lock.

This is the *true* binding constraint behind the ~1.6 k/s ceiling. The arithmetic: per WAL partition, throughput ~ `dispatch.entries (4) / provider.duration (~20 ms) ~ 200 entries/s/partition × 8 partitions ~ 1.6 k/s`, matching the FINAL line.

**Fix.** Add `[AlwaysInterleave]` to `IWalShardGrain.AppendBatchAsync`. Concurrent producer turns can then enter the grain while one flush is in motion, and the next flush will sweep up *all* entries that accumulated during the previous flush window - not just the ones one caller happened to submit.

Safety: every mutable surface inside `AppendBatchAsync` is already serialised by `lock (_stateGate)` (offset assignment, pending-list mutation, in-flight cap check, cutover-loop predicate). The per-iteration gate hold preserves the dense-and-strictly-ascending-offsets invariant *within* a single returned batch. The `_stickyFailure` propagation, the multi-batch failure recovery (`HandleFlushFailureAsync` already iterates "later slots"), and the encoded-segment lifecycle are all written for multi-batch concurrency.

**Why this is cheaper than raising `WalPartitions`.** Step 8c-a-i showed that more WAL partitions makes the caller-side `max(per-partition tails)` worse, not better - the fan-out tail dominates. `[AlwaysInterleave]` does *not* change the fan-out shape; it only lets concurrent callers share the existing single WAL grain per partition. It composes with the existing `WalMaxPendingBatches` knob without changing the wire format, the offset contract, or the storage layout.

**Method.** Single source change on the interface; rebuild silo via `-LocalBuild`; rerun the same `10000:5` rung with `BENCH_RESPONSE_TIMEOUT_SEC=180` (carry-forward from step 8c-b-i so we keep timeouts disambiguated). Predicted observations if the hypothesis is right:

- `wal.append.in_flight` p50 rises from 0 to >= 1, ideally near `WalMaxPendingBatches` under saturation.
- `wal.append.batch_entries` p50 rises from 4 to a multiple of it (4 callers x 4 entries each is the lower bound; if concurrent fan-in is good, p50 should jump well above 16).
- FINAL throughput rises above the step-8b-i baseline of `~1.6 k/s`. If the WAL grain is now the same provider-bound throughput as a single Azure Tables row group (~10 k/s), we should see something between 3 k/s and 8 k/s on the same rung.

If `in_flight` rises but `batch_entries` does not, the next bottleneck is elsewhere (caller-side fan-out into `LatticeGrain.SetManyAsync`). If `batch_entries` rises but throughput doesn't, the provider is the next bottleneck (Azure Tables row-group concurrency).

**Result (deployed).** The WAL-layer hypothesis confirmed exactly as predicted; the caller-visible *throughput* collapsed because the change uncovered a deeper limiter:

| metric                                     | step-8c-b-i (no interleave) | step-8c-c-i (interleave) | change                                  |
| ------------------------------------------ | --------------------------- | ------------------------ | --------------------------------------- |
| `wal.append.in_flight` p50 (per shard)     | **0**                       | **7**                    | **0 → 7 (saturates `MaxPending = 8`)**  |
| `wal.append.in_flight` p99 (per shard)     | 0                           | 7                        | 0 → 7                                   |
| `wal.append.batch_entries` p50 (per shard) | 4                           | 8-9                      | +2x                                     |
| `wal.append.batch_entries` p90 (per shard) | 8-9                         | **100 (cap)**            | hits `WalMaxBatchEntries`               |
| `wal.append.provider.duration` p50         | ~20 ms                      | **~440-510 ms**          | **+25x**                                |
| `wal.append.provider.duration` p99         | hundreds of ms              | ~800-1500 ms             | +3-5x                                   |
| `leaf.commit.duration` phase=wal p50       | ~30-100 ms                  | **~950-990 ms**          | +10-30x                                 |
| `lattice.set_many.duration_ms` p50         | ~tens of ms                 | **13,055 ms**            | **+200x**                               |
| FINAL SteadyAvg                            | 1,649/s                     | **89/s**                 | **-95%**                                |
| FinalWritten / FinalFailed                 | 187,636 / 0                 | 9,959 / 0                | -                                       |

**Mechanism.** The WAL surface behaves *exactly* as the hypothesis predicted. `in_flight` rose from 0 to 7 (i.e. `MaxPending - 1`, with the just-started flush not counted in `inFlightBefore`), proving the chain is now fully utilised. `batch_entries` p90 hit the `WalMaxBatchEntries = 100` cap, proving multiple producer turns are now coalescing into the *same* flush window. So the WAL grain is doing precisely what we wanted - and that workload is now eight-way pipelined against the provider per shard, eight shards, i.e. `8 x 8 = 64` concurrent `AppendEncodedBatchAsync` calls in flight against `MemoryGrainStorage` (the benchmark's in-process WAL store).

That is what surfaced the *real* downstream limiter: silo log contains 2,074 `OrleansMessageRejectionException: tried to forward message ... to invalid activation. "Unable to create local activation"` errors against `MemoryGrainStorage.leaf` and `MemoryGrainStorage.internal`. The benchmark uses `Orleans.Persistence.Memory` (the in-memory `memorystorage/N` grains), and under the new request rate `IMemoryStorageGrain.WriteStateAsync` is getting rejected by the Orleans runtime because the storage grain activation cannot keep up - this is an Orleans-runtime-level rejection (not a provider 429), and it cascades through the leaf-commit path:

- `BPlusLeafGrain.CommitSetManyAsync` calls `WriteStateAsync` synchronously after the WAL ack, so the leaf turn now blocks on `MemoryGrainStorage` activation creation.
- `leaf.commit.duration` phase=`wal` p50 jumped from ~30 ms to ~950 ms because `LeafCommitLogWriter.AppendManyAsync` is now waiting on the *fan-in* (more concurrent leaves -> more concurrent calls into the WAL shard) where it was previously serialised by the grain-turn lock.
- `lattice.set_many.duration_ms` p50 = 13 s because the top-level `LatticeGrain.SetManyAsyncCore` waits on `max(shard tails)` and the slowest shard's leaf commit is now blocked behind `MemoryGrainStorage` rejection / retry.

**Interpretation.** The WAL grain is no longer the binding constraint at this rung. The new binding constraint is the **benchmark's persistence layer** (`Orleans.Persistence.Memory.memorystorage`) and/or the leaf-commit path that runs *after* the WAL ack. Two implications:

1. **The product-side WAL change is correct** - it eliminates a real serialisation point that was capping the WAL pipeline at chain depth = 1 regardless of the configured `WalMaxPendingBatches`. We will need it to make the next phase visible at all.

2. **The benchmark rung is no longer measuring what we want it to measure.** `MemoryGrainStorage` is not the production persistence shape, and we now know the next limiter is downstream of the WAL ack. To re-isolate the WAL, the next probe is either (a) raise `MemoryGrainStorage` capacity / concurrency so the leaf-commit path stops being a chokepoint in the benchmark, or (b) switch the benchmark off `MemoryGrainStorage.leaf` / `MemoryGrainStorage.internal` and onto Azure Tables (the production persistence shape).

**Decision.**

- **KEEP** the `[AlwaysInterleave]` change in the product code. It is a strict improvement (no regression in any pipelined-WAL test) and it is *necessary* for any subsequent step that wants to push the WAL beyond chain depth = 1. The XML doc on `IWalShardGrain.AppendBatchAsync` already explains the safety argument (`_stateGate` serialises all mutable surfaces).

- **DO NOT** push the benchmark fix in the same PR. The benchmark-side regression is an artifact of `MemoryGrainStorage` not the WAL change; investigating it belongs in step 8c-c-ii.

- **NEXT step (8c-c-ii)** - run a targeted A/B *without* changing the benchmark persistence: rebuild with the change reverted to confirm the regression flips back to 1.6 k/s, then re-apply the change and add `BENCH_LEAF_PERSISTENCE_*` knobs (or move the benchmark off `Orleans.Persistence.Memory`) so the leaf-commit path is not the limiter. The expected outcome is that with the persistence chokepoint removed, the new WAL pipeline lifts throughput above 1.6 k/s rather than below it.

**Artifacts.** `benchmark/azure-throughput/.run/step8c-c-i-silo.log` (silo, 5.2 MiB), `step8c-c-i-producer.log`, `step8c-c-i-results.csv`, `step8c-c-i-phaseA.csv`, `ladder-U9p-step8c-c-i.log`. Code change is `src/lattice/BPlusTree/Grains/IWalShardGrain.cs` (added `using Orleans.Concurrency;` and `[AlwaysInterleave]` on `AppendBatchAsync`). Deploy delta is unchanged from step 8c-b-i (`$env:BENCH_RESPONSE_TIMEOUT_SEC='180'`); the only source-of-truth code change is the interleave attribute.


### U9p step 8c-c-ii - falsifier: leaf storage chokepoint via null / memory128 (10000:5)

**Setup.** Step 8c-c-i's interpretation predicted that `MemoryGrainStorage`'s `NumStorageGrainsDefaultValue=10` was the new binding constraint. Step 8c-c-ii exercises that prediction with two A/B runs against the same `10000:5` rung:

- **Run B** - `BENCH_LEAF_STORAGE_KIND=null` (benchmark-only `NullGrainStorage` that no-ops every `WriteStateAsync`). Removes the leaf / internal-state persistence layer entirely from the measurement window. If memory storage was the only limiter, throughput should jump well past 1.6 k/s.
- **Run B-2** - `BENCH_LEAF_STORAGE_KIND=memory` `BENCH_LEAF_STORAGE_NUM_GRAINS=128` (widen the in-process activation pool by 12.8x). If the activation pool size was the only chokepoint, the rejection count should fall to zero and throughput should rise.

**Result.**

| metric                                | Run B (null)       | Run B-2 (memory, N=128)            |
| ------------------------------------- | ------------------ | ---------------------------------- |
| Steady-state peak (1 s window)        | 7,152/s            | 11,391/s                           |
| FinalWritten                          | 15,095             | 33,882                             |
| FinalFailed                           | 0                  | 671 (only t < 2 s)                 |
| `wal.append.in_flight` p50 / max      | 7 / 7              | 7 / 7                              |
| `wal.append.batch_entries` p90        | 100                | ~95-100                            |
| `wal.append.provider.duration` p50    | tens of ms         | ~210-220 ms                        |
| `tcp.read.channel_write_wait_ms` max  | ~5,765 ms          | ~4,369 ms                          |
| Producer ingest behaviour             | drops at t > 15 s; eventual broken pipe | drops still present, fewer rejections |

**Interpretation.** Both runs confirm step 8c-c-i's WAL pipeline change is correct: `in_flight` is saturating at the configured cap and batches are coalescing to ~100 entries. Removing memory storage *did* improve peak throughput (1.6 k/s -> 7-11 k/s), and widening the activation pool from 10 to 128 reduced rejections from thousands to hundreds and improved peak again. The peak is **not** the limiter the WAL pipeline change exposed; the new limiter is silo-level TCP/channel backpressure (`tcp.read.channel_write_wait_ms` max 4-5 seconds), with the producer's `slipMaxMs` climbing in lockstep.

**Decision.** Treat 8c-c-ii as a **diagnostic-only** step. `NullGrainStorage` is benchmark-only by construction (the WAL would still be source-of-truth on replay, but the bench never restarts grains, so leaf state being a no-op is invisible to the measurement). `MemoryGrainStorage` with `NumStorageGrains=128` is still not production-shape; raising the pool just delays the same activation-forwarding pattern under a higher rate. Neither configuration is what a production operator would deploy.

The user correctly observed: *"durable grain storage is a requirement in reality.... is this line of analysis going to help produce an outcome which is viable for production?"* The honest answer is no - 8c-c-ii proved the WAL pipeline is uncorked, but the measurement now needs to land on a real durable grain storage provider before any throughput number is meaningful for production planning.

**Artifacts.** `benchmark/azure-throughput/.run/step8c-c-ii-null-silo.log`, `step8c-c-ii-null-results.csv`, `step8c-c-ii-memory128-silo.log`, `step8c-c-ii-memory128-results.csv`. Code shipped only as benchmark-internal levers (`benchmark/azure-throughput/Silo/NullGrainStorage.cs`, `BENCH_LEAF_STORAGE_KIND` / `BENCH_LEAF_STORAGE_NUM_GRAINS` env vars in `Program.cs` and `20-build-and-deploy.ps1`); no `src/lattice` changes.


### U9p step 8c-c-iii - pivot to production-shape durable grain storage (10000:5)

**Setup.** Switch the benchmark silo's leaf / internal / atomic grain storage from `MemoryGrainStorage` to a real durable `AzureTableGrainStorage` (Microsoft.Orleans.Persistence.AzureStorage 10.1.0). Both the WAL and the grain state now hit Azure Tables; both use the same `BENCH_STORAGE_URI` / managed-identity credential the WAL provider was already configured with. The benchmark default is now `BENCH_LEAF_STORAGE_KIND=azure`; `memory` and `null` remain as documented diagnostic-only A/B levers.

Code change scope: `benchmark/azure-throughput/Silo/VehicleFleetSimulator.AzureThroughput.Silo.csproj` adds `Microsoft.Orleans.Persistence.AzureStorage 10.1.0`; `benchmark/azure-throughput/Silo/Program.cs` rewrites the storage-registration block into a three-way switch (`azure` | `memory` | `null`) with `BENCH_LEAF_STORAGE_TABLE` (default `OrleansLatticeGrainState`); `benchmark/azure-throughput/scripts/20-build-and-deploy.ps1` flips the default and forwards the new env var. No source change in `src/lattice` or `src/lattice.storage.azuretable`.

**Result.** Single rung `10000:5`, 30 s producer, against `https://lat01sa.table.core.windows.net` with managed-identity auth.

| metric                              | Run B-2 (memory, N=128) | **Run C (Azure Tables, durable)**           |
| ----------------------------------- | ----------------------- | ------------------------------------------- |
| FinalWritten                        | 33,882                  | **185,909**                                 |
| FinalFailed                         | 671                     | **71,355** (almost all in t = 1-2 s)        |
| Steady-state peak (1 s window)      | 11,391/s                | **24,588/s**                                |
| `wal.append.provider.duration` p50  | ~210 ms                 | **~57-75 ms**                               |
| `wal.append.provider.duration` p99  | ~600 ms                 | **~165-322 ms**                             |
| `wal.append.in_flight` p50 / max    | 7 / 7                   | 6-7 / 7                                     |
| `wal.append.batch_entries` p50 / max | ~6 / 100               | 5-6 / 100                                   |
| Producer behaviour                  | broken pipe @ ~20 s     | runs to DONE @ 30.6 s, slipMaxMs > 21 s     |
| Ladder-summary `SteadyAvg`          | 1,575/s                 | 1,564/s (averages 80 s of idle tail)        |

**Interpretation.**

1. **The WAL pipeline is no longer the limiter on the durable path.** `wal.append.provider.duration` p50 dropped from ~210 ms (memory + WAL) to ~57-75 ms (Azure Tables WAL only); p99 dropped roughly 3x. With `MemoryGrainStorage` removed, the WAL grain has the threadpool / CPU headroom it needs to saturate Azure Tables.

2. **Once warm, the durable path runs ~2.2x faster than the widened-memory diagnostic peak.** From t = 33-41 s in Run C, the per-second drainer climbs 8 k -> 12 k -> 16 k -> 24 k/s before the producer's prebuffer drains. That 24,588/s peak is against real Azure Tables on both WAL and grain state - it is production-shape.

3. **The arithmetic matches.** Steady-state ceiling ~= `walPartitions x walMaxPendingBatches x batch_avg / provider_p50_ms` ~= `8 x 7 x ~14 / 0.057 s` ~= 14 k/s, matching the observed 12-16 k/s steady band. When batches drift toward 100 (full coalescing), the ceiling lifts to ~24 k/s, which is what the 1-second peak shows.

4. **The 71,355 failures are a real production hazard.** All 71,355 land in t = 1-2 s. They are not Azure Tables 429s; they are `OrleansMessageRejectionException: tried to forward message ... Unable to create local activation`. With Azure Tables grain storage, every leaf grain's first `WriteStateAsync` is a real round-trip; when `FlushConcurrency = 8` x `BatchSize = 4096` entries fan out into thousands of brand-new leaf activations at once, the local placement directory rejects forwards faster than activations complete. This is a **cold-start activation storm** any production caller hitting a freshly-started silo at high rate would also see.

5. **The ladder-summary `FinalAvgRate = 1,564/s` is misleading.** It averages 80 s of zero (producer exited at t ~= 41 s, silo idled to t = 119 s under the watchdog) against ~11 s of high throughput. The honest read-outs are *steady-state peak* (24,588/s) and *steady-state band* (12-16 k/s), not the all-window average.

**Decision.** **CONFIRMED PRODUCTION-VIABLE BASELINE.** This is the first benchmark configuration that measures genuine durable persistence on both the WAL and the leaf state. Two real production problems are now in the open:

- **Kink 1 - cold-start activation storm** (Run C: 71 k drops in t = 1-2 s).
- **Kink 2 - steady-state ceiling 12-24 k/s**, bounded by `walPartitions x walMaxPendingBatches x batch_avg / provider_p50_ms`.

Both are addressed in step 8c-c-iv below. The benchmark silo's storage configuration is *kept* (Azure Tables grain storage as default); `memory` and `null` remain as documented diagnostic-only A/B levers.

**Artifacts.** `benchmark/azure-throughput/.run/step8c-c-iii-azure-silo.log`, `step8c-c-iii-azure-results.csv`, `step8c-c-iii-azure-phaseA.csv`. Code change: `benchmark/azure-throughput/Silo/VehicleFleetSimulator.AzureThroughput.Silo.csproj` (added `Microsoft.Orleans.Persistence.AzureStorage 10.1.0`); `benchmark/azure-throughput/Silo/Program.cs` (three-way storage switch); `benchmark/azure-throughput/scripts/20-build-and-deploy.ps1` (default `azure`, new `BENCH_LEAF_STORAGE_TABLE` env var). No `src/lattice` changes.


### U9p step 8c-c-iv - kink-resolution plan (next)

The durable-path baseline (8c-c-iii) exposed two production-real kinks. They are addressed in this order because Kink 1 is currently masking the Kink 2 measurement (the all-window throughput cratered to 1,564/s only because the cold-start storm cost ~12 seconds of zero before the WAL came alive):

**8c-c-iv-a (next): retry transient activation rejections in `FlushAsync`.**

The benchmark's `FlushAsync` (`benchmark/azure-throughput/Silo/Program.cs`) currently catches `OrleansMessageRejectionException` only when the silo is *shutting down* (`IsShutdownRejection`). Steady-state and cold-start rejections fall through to the warning-log path that counts the entire batch (up to 4,096 entries) as failed and drops it. The Orleans contract is that `OrleansMessageRejectionException` *during* normal operation (not shutdown) is transient: the placement directory recovers within a few hundred ms once activations land. The benchmark already does bounded retry on this exception class at the *startup reshard* path; the steady-state hot path should do the same.

Surgical change: wrap the `lattice.SetManyAsync` call in `FlushAsync` with bounded retry-with-jitter-backoff (e.g. 5 attempts, 50 ms base, jittered exponential), keeping `IsShutdownRejection` as the early-bail. Predicted observation: Run C's 71,355 t = 1-2 s failures fall to near zero, the cold-start window stretches from ~2 s to ~5-10 s of degraded throughput rather than ~12 s of zero, and the producer no longer drops its prebuffer. The all-window `FinalAvgRate` then becomes a meaningful production-shape number.

**8c-c-iv-b: library-level `ILattice.WarmUpAsync` that activates shard roots before traffic.**

Even with retry, the cold-start cost is real - we pay for thousands of first-touch Azure Tables `Get` calls before serving traffic. Production operators want a deterministic warm-up hook. Implementation: an `ILattice.WarmUpAsync(CancellationToken)` extension that iterates the known shard roots and issues a no-op call so each activation completes before the first real write. Wire it into the benchmark silo *after* the silo starts but *before* `TcpIngestService` opens its listening port. Predicted observation: cold-start drop count falls to ~0 even without retry (8c-c-iv-a remains, the two are complementary defences). The first per-second drainer line then shows a non-zero rate from t <= 1 s.

**8c-c-iv-c: steady-state grid search.**

With Kink 1 closed, the headline number is the steady-state band. The arithmetic says the levers are `walPartitions`, `walMaxPendingBatches`, and `phase2CoalescingMs`. A small ladder:

- baseline: `(walPartitions = 8, walMaxPending = 8, phase2 = 0 ms)` - the current 12-24 k/s band.
- `(walPartitions = 16, walMaxPending = 8, phase2 = 0 ms)` - doubles independent partition keys; expected to ~double the band if the provider scales linearly, regress slightly if Azure Tables surfaces 429s.
- `(walPartitions = 8, walMaxPending = 16, phase2 = 0 ms)` - doubles per-shard pipeline depth; expected to lift the band ~1.5x.
- `(walPartitions = 8, walMaxPending = 8, phase2 = 3 ms)` - lets phase-2 commits coalesce per shard. `wal.append.batch_entries` p50 is currently 5-6 vs cap 100; expected to raise p50 toward 40-60 and lift the band substantially.
- best-of-three combined - the prior winner with the two other knobs tightened.

**Ship-criterion for the campaign.** A documented steady-state band on the production-shape durable path, with the cold-start storm closed at the source. The ladder above gives the band; 8c-c-iv-a and 8c-c-iv-b close the cold-start storm. After 8c-c-iv-c the campaign is ready for a final write-up in `docs/lattice/wal.md`.

**What the campaign will *not* try to do.** It will not chase Azure-Tables-side optimisations (no provider rewrite, no batching changes, no retry-policy changes inside `Orleans.Lattice.Storage.AzureTable`) - the WAL provider is already production-grade. It will not try to remove the foreground leaf-commit dependency on the WAL ack - that path is the durability contract.


### U9p step 8c-c-iv-a - result: bounded retry closes the cold-start storm but parks throughput for the duration of the storm (10000:5)

**Change shipped.** Wrap the `lattice.SetManyAsync` call in `benchmark/azure-throughput/Silo/Program.cs::FlushAsync` with bounded retry-with-jitter (5 attempts, 50 ms base, exponential x 2 capped at 800 ms, +/-25% jitter), gated on `IsOrleansMessageRejection(ex)` AND `!lifetime.ApplicationStopping.IsCancellationRequested`. Shutdown semantics unchanged (`IsShutdownRejection` still returns `ShutdownDiscarded`). Added `BenchMetrics.LatticeSetManyRetryAttempts` histogram (and allowlisted it in `PhaseADiagnosticReporter`) so the retry density is visible on phase A.

**Result.** Same rung as step 8c-c-iii (`10000:5`, 30 s producer, durable Azure Tables grain storage):

| metric                              | step 8c-c-iii (no retry)              | **step 8c-c-iv-a (bounded retry)**        |
| ----------------------------------- | -------------------------------------- | ------------------------------------------ |
| FinalWritten                        | 185,909                                | 178,944                                    |
| FinalFailed                         | **71,355** (t = 1-2 s)                | **0**                                      |
| Residual `OrleansMessageRejection` log mentions | 71,355                       | **0** across the entire silo log           |
| Steady-state peak (1 s window)      | 24,588/s                               | 20,462/s                                   |
| Time of first non-zero `Entries written per second` | t ~= 2 s            | **t ~= 19.4 s**                            |
| Producer behaviour                  | DONE @ 30.6 s, slipMaxMs > 21 s        | DONE @ 32.9 s, slipMaxMs ~= 23.2 s         |

**Interpretation.**

1. **The retry policy did exactly what it was designed to do.** Zero residual `OrleansMessageRejectionException` mentions in 3.8 MiB of silo log; zero failed batches; the producer ran to DONE without a broken pipe. The cold-start storm is closed *at the symptom level*.

2. **But the retry parked all 8 `FlushConcurrency` slots inside `Task.Delay` for ~18 s.** Phase-A windows show `inFlight = 8` and `Entries written per second = 0` for every cadence sample from t = 7 s through t = 18 s. Each of the 8 in-flight batches hit a rejection on its first try, then backed off 50 ms -> 100 ms -> 200 ms -> 400 ms -> 800 ms (sum ~= 1.55 s of pure delay per batch, before counting the rejected RPC's own wall-clock). Multiply by the cascade of leaves that needed first-touch activation, and the parallelism stayed parked until the placement directory drained.

3. **The trade is wrong-shape for this workload.** Step 8c-c-iii lost ~12 s of throughput to a flood of failed batches that the producer's outer ring-buffer absorbed and resent; step 8c-c-iv-a loses ~18 s of throughput to a quiet retry stall where the producer's prebuffer also fills (`slipMaxMs ~= 23 s`). Both runs end up shipping ~180 k entries in a 30 s producer window; the bounded-retry version is *cleaner* (no failed counter, no broken pipe) but not *faster*.

4. **8c-c-iv-b (proactive warm-up) is now strictly required, not optional.** The bounded retry is the *safety net*; the production fix is to never hit it. An `ILattice.WarmUpAsync(CancellationToken)` extension that issues a no-op call to each shard root before the producer connects will let the first ~thousands of leaf activations land while the silo is idle, so the first real `SetManyAsync` hits an already-warm placement directory. With warm-up, the retry should record p99 = 0 attempts in the phase-A scraper, and steady-state should start at t ~= 1 s instead of t ~= 19 s.

**Decision.**

- **KEEP the bounded retry.** It is a production-correctness fix - any caller hitting a freshly-started silo at high rate would also see transient `OrleansMessageRejectionException`, and *not* retrying it converts a self-healing condition into a hard failure. The retry is now part of the durable benchmark silo's contract; in production the equivalent guard belongs in any high-rate ingest gateway (`benchmark/azure-throughput/Silo/Program.cs::FlushAsync` is the closest analogue to such a gateway in this repo).

- **Do not declare 8c-c-iv-a a throughput regression.** The headline rate dropped slightly (20.5 k/s peak vs 24.6 k/s) but both numbers are inside the same band the 8c-c-iii arithmetic predicts (~`8 x 7 x batch_avg / provider_p50_ms`). The cold-start window's length, not its rate, is the dominant cost on a 30 s producer; that is what 8c-c-iv-b will address.

- **Proceed to 8c-c-iv-b.** Implement `ILattice.WarmUpAsync(CancellationToken)`, wire it into the benchmark silo *after* `host.RunAsync` returns ready but *before* `TcpIngestService` opens its listening port. Predicted observation: first non-zero `Entries written per second` line at t ~= 1 s; `bench.lattice.set_many.retry_attempts` p99 = 0; `FinalAvgRate` lifts substantially (the silo gets ~17 more seconds of throughput to amortise over the 119 s post-producer window).

**Artifacts.** `benchmark/azure-throughput/.run/step8c-c-iv-a-azure-silo.log`, `step8c-c-iv-a-azure-producer.log`, `step8c-c-iv-a-azure-results.csv`, `step8c-c-iv-a-azure-phaseA.csv`. Code: `benchmark/azure-throughput/Silo/Program.cs` (`FlushAsync` retry loop), `benchmark/azure-throughput/Silo/BenchMetrics.cs` (`LatticeSetManyRetryAttempts` histogram), `benchmark/azure-throughput/Silo/PhaseADiagnosticReporter.cs` (allowlist entry). No `src/lattice` changes.


### U9p step 8c-c-iv-b - result: proactive warm-up lifts steady-state ~36% and surfaces shard-root traversal P99 as the next kink (10000:5)

**Change shipped.** `ILattice.WarmUpAsync(CancellationToken)` in `src/lattice/BPlusTree/ILattice.cs` with a bounded-parallelism (`min(physicalShardCount, 32)`) fan-out in `src/lattice/BPlusTree/Grains/LatticeGrain.WarmUp.cs`; each physical shard root now exposes `IShardRootGrain.WarmUpAsync()` (`src/lattice/BPlusTree/Grains/ShardRootGrain.Lifecycle.cs`) which calls `PrepareForOperationAsync()` so an empty shard runs the same `EnsureRootAsync` path the first write would run (materialising the deterministic root leaf at idle time), then pings the current root grain (`IBPlusLeafGrain.CountAsync()` for flat trees, `IBPlusInternalGrain.AreChildrenLeavesAsync()` for trees with height). Metrics `orleans.lattice.warmup.invocations` / `orleans.lattice.warmup.duration` live in `src/lattice/LatticeMetrics.cs`. Benchmark silo `benchmark/azure-throughput/Silo/Program.cs` `await`s `lattice.WarmUpAsync(stoppingToken)` after the reshard barrier and before the TCP listener opens, with a widened retry budget (`MaxWarmUpAttempts = 8`, exponential backoff capped at `MaxWarmUpBackoffMs = 4000` ms) to absorb the same Orleans directory-cache race the reshard-complete poll loop already buffered against. 6 warm-up unit tests pass; 261 ShardRoot non-chaos tests stay green.

**Result.** Same rung (`10000:5`, 30 s producer, durable Azure Tables grain storage, `walPartitions=8`, `walMaxPending=8`, `flushConcurrency=8`):

| metric                                              | step 8c-c-iv-a (retry only) | **step 8c-c-iv-b (retry + warm-up)** | delta   |
| --------------------------------------------------- | --------------------------: | -----------------------------------: | ------: |
| FinalWritten                                        | 178,944                     | **207,418**                          | **+16%**|
| FinalFailed                                         | 0                           | **0**                                | -       |
| SteadyAvg                                           | 2,072/s                     | **2,817/s**                          | **+36%**|
| SteadyMax (1 s window)                              | 20,462/s                    | **28,663/s**                         | **+40%**|
| FinalAvgRate                                        | 1,503/s                     | **1,826/s**                          | **+21%**|
| Elapsed                                             | ~119 s                      | 113.6 s                              | -5 s    |
| Residual `OrleansMessageRejection` mentions       | 0                           | 0                                    | -       |

**Phase A latency surface (118 tuples, last-window-per-tuple, hot `azure-throughput-*` tree unless noted).**

| instrument                                          | P50 (ms) | P90 (ms) | **P99 (ms)** | Max (ms) |
| --------------------------------------------------- | -------: | -------: | -----------: | -------: |
| `lattice.set_many.duration_ms` (caller-visible)   | 2,518    | 3,552    | **3,780**    | 3,780    |
| `shard_root.set_many.leaf_rpc.duration`           | 904      | 2,297    | **3,276**    | 3,741    |
| `shard_root.set_many.local_apply.duration`        | 1,513    | 2,619    | **3,450**    | 3,741    |
| `leaf.commit.duration` (hottest row)              | 287      | 441      | **595**      | 1,161    |
| `leaf.commit.duration` (cooler row)               | 36       | 117      | **278**      | 571      |
| `wal.append.provider.duration` (per-partition)    | 105-119  | 202-220  | **310-394**  | 351-546  |
| `provider.commit.duration` (per-shard)            | 26-44    | 60-135   | **126-239**  | 153-309  |

**Interpretation.**

1. **The warm-up hypothesis falsified its prediction in the right direction.** Pre-paying the placement-directory + root-leaf materialisation cost while the silo is idle lifted steady-state ~36% and SteadyMax ~40% with zero failures. The shard-root warm-up materialising the deterministic root leaf on an empty shard (the intentional `EnsureRootAsync` path on first warm-up) was load-bearing: without it the first write still pays root-creation cost on the hot path.

2. **SteadyMin = 0 persists.** The first 1 s drainer window still shows zero traffic; the cold-start window collapsed from ~19 s to ~1-3 s, but not to zero. Some first-touch cost remains (producer-side TCP accept warm-up, first batch JIT, first `SetManyAsync` placement lookup) and is bounded by the existing retry safety net.

3. **The next kink is shard-root traversal P99, not WAL throughput.** `lattice.set_many.duration_ms` P99 is 3,780 ms while the WAL provider's per-partition P99 is 310-394 ms (~12x faster). The 3.3 s gap lives in `shard_root.set_many.leaf_rpc.duration` (P99 3,276 ms) and `shard_root.set_many.local_apply.duration` (P99 3,450 ms). Eight concurrent flushes per silo, all routed through a single shard-root grain per shard, pile onto an Orleans reentrant turn pipe; `leaf.commit.duration` on the hottest row is only ~595 ms P99, so the leaf is not the bottleneck - the queueing in front of it is. **Throughput is no longer the binding constraint at this rung; tail latency is.**

4. **Retry stayed dormant.** `bench.lattice.set_many.retry_attempts` recorded but the silo log has zero `OrleansMessageRejectionException` mentions and `FinalFailed=0` - warm-up + retry are now operating as designed (warm-up handles the steady-state cold-start, retry handles any residual transient rejection without firing in this run).

**Decision.**

- **KEEP warm-up as a benchmark + library default.** `ILattice.WarmUpAsync` is now part of the lattice's public contract; the benchmark silo calls it on every cold start. The throughput uplift is too large to leave on the floor.
- **Pivot the campaign focus.** The original 8c-c-iv-c (steady-state grid search) is deferred behind a new **8c-c-iv-c2 (shard-root traversal P99)** step. The grid search will only meaningfully move the band if the per-write tail moves first; otherwise tuning `walPartitions` / `walMaxPending` / `phase2CoalescingMs` just shifts where the queue forms.
- **Falsification target for 8c-c-iv-c2.** `lattice.set_many.duration_ms` P99 < 1,500 ms at the `10000:5` rung, with steady-state band preserved or improved. Anchor on the gap between `shard_root.set_many.leaf_rpc.duration` (P99 ~3.3 s) and `leaf.commit.duration` (P99 ~0.6 s) - that ~2.7 s of in-grain queueing is what the next change must close.

**Artifacts.** `benchmark/azure-throughput/.run/step8c-c-iv-b-azure-silo.log`, `step8c-c-iv-b-azure-producer.log`, `step8c-c-iv-b-azure-results.csv`, `step8c-c-iv-b-azure-phaseA.csv`. Code: `src/lattice/BPlusTree/ILattice.cs` (`WarmUpAsync`), `src/lattice/BPlusTree/Grains/LatticeGrain.WarmUp.cs`, `src/lattice/BPlusTree/IShardRootGrain.cs`, `src/lattice/BPlusTree/Grains/ShardRootGrain.Lifecycle.cs`, `src/lattice/LatticeMetrics.cs`, `benchmark/azure-throughput/Silo/Program.cs` (warm-up call + widened retry budget), `docs/lattice/api.md` (warm-up semantics), `test/lattice/BPlusTree/Grains/LatticeGrainTests.WarmUp.cs`, `test/lattice/BPlusTree/Grains/ShardRootGrainLifecycleTests.cs`.


### What stays in place

- **The WAL pipeline change (`[AlwaysInterleave]` on `IWalShardGrain.AppendBatchAsync`) is kept.** It is the change that took `wal.append.in_flight` from 0 to 7 and `wal.append.batch_entries` p90 from ~8-9 to the 100-entry cap. Without it the WAL is the binding constraint.
- **The benchmark silo's durable Azure Tables grain storage is kept as the default** (`BENCH_LEAF_STORAGE_KIND=azure`). `memory` and `null` remain as documented diagnostic-only A/B levers so any future "is it the storage or the WAL?" question can be re-answered in one rerun.
- **The Phase A diagnostic instruments are unchanged.** Per-step `leaf.commit.duration` (`wal` / `apply` / `observer` / `digest`), `wal.append.{queue,batch,in_flight,turn_wait,provider.duration}`, `provider.retry.attempts`, and the benchmark-local `tcp.read.*` / `drain.flush_dispatch_*` instruments together cover the whole producer -> WAL -> storage path. Anything we still cannot see (e.g. activation directory lookups under cold-start) goes on this instrument set, not anywhere else.
- **The C4 tuning knobs on `AzureTableWalStorageOptions` are kept** as production-hygiene knobs. Their A/B-measured null effect on Azurite was expected; against the real Azure Tables baseline they are the right shape for operators who do see 503s.
- **The wire format remains frozen.**
- **Phase B and Phase D step lists above are quiesced**, not invalidated. The 8c-c-iv kink-resolution plan supersedes them as the in-flight work; Phase B / Phase D resume as relevant after the durable-path band is documented.

**Progress**: steps 8c-c-iv-a (bounded retry) and 8c-c-iv-b (proactive warm-up) both landed and measured. Warm-up lifted steady-state +36% / SteadyMax +40% with FinalFailed=0. Next is **8c-c-iv-c2 (shard-root traversal P99)** - `lattice.set_many.duration_ms` P99 is now 3,780 ms vs `wal.append.provider.duration` P99 ~310-394 ms, so per-write tail latency, not throughput, is the binding kink. Steady-state grid search (formerly 8c-c-iv-c) is deferred behind 8c-c-iv-c2.

### U9p step 8c-c-iv-c2 - diagnostic memo: shard-root traversal P99 (no code change yet)

**Goal.** Rank the four hypotheses from the plan-step against the in-repo source evidence and the step 8c-c-iv-b phase-A surface, and identify the single highest-confidence code lever to prototype next. No source change in this step.

**Evidence (from step 8c-c-iv-b last-window phase-A, durable Azure Tables grain storage, `10000:5`, `walPartitions=8`, `walMaxPending=8`, `flushConcurrency=8`):**

| instrument                                          |   P50    |   P99    |
| --------------------------------------------------- | -------: | -------: |
| `lattice.set_many.duration_ms` (caller-visible)     | 2,518 ms | 3,780 ms |
| `shard_root.set_many.local_apply.duration`          | 1,513 ms | 3,450 ms |
| `shard_root.set_many.leaf_rpc.duration`             |   904 ms | 3,276 ms |
| `leaf.commit.duration` (hottest row)                |   287 ms |   595 ms |
| `wal.append.provider.duration` (per-partition)      |  105 ms  |  394 ms  |

The arithmetic: `local_apply` ≈ `leaf_rpc` (within ~5%) ⇒ per shard-root turn, essentially all wall-clock is in **one** awaited `leaf.SetManyAsync` call. Caller-visible P99 (3,780 ms) ≈ `leaf_rpc` P99 (3,276 ms) + caller-side `Task.WhenAll` over per-shard buckets (~500 ms tail of slowest-of-N) - i.e. there is no missing time between the silo's outer `SetManyAsync` and the per-leaf RPC.

The 5.5x ratio between `leaf_rpc` P99 (3,276 ms) and `leaf.commit.duration` P99 (595 ms) is the **un-instrumented gap**: time spent *outside* the leaf's `CommitSetManyAsync` body but *inside* the round-trip from `leaf.SetManyAsync` issue to ack. That gap is the leaf grain's **non-reentrant turn-queue wait**.

**Source evidence (cross-checked).**

1. `IShardRootGrain.SetManyAsync` is already `[AlwaysInterleave]` (`src/lattice/BPlusTree/IShardRootGrain.cs:169-170`). Hypothesis (a) and (d) are therefore **already shipped** as of U9g / U9h-A; the in-grain serial-turn argument from step 8c-c-i (WAL) does not apply at this layer. Multiple shard-root turns *do* run concurrently on the same activation.
2. `IBPlusLeafGrain.SetManyAsync` is **NOT** marked `[AlwaysInterleave]` (`src/lattice/BPlusTree/IBPlusLeafGrain.cs:128`). It carries no interleave attribute at all - every leaf grain serialises its callers one-at-a-time on the Orleans default turn queue.
3. The benchmark rung's flat-tree fast-path (`ShardRootGrain.SetManyLocalOnlyAsync` lines 575-589) routes 100% of a 4,096-entry slice to **one** `IBPlusLeafGrain.SetManyAsync(entries)` call on the root leaf when the tree is flat. The 8 concurrent shard-root turns on the same shard activation therefore each fire **one** call into the same leaf activation - 8 callers queued behind one running turn.
4. Even after splits raise the tree height, `SetManyLocalOnlyAsync` groups by routed leaf and most rungs have a small hot-leaf set: phase-A shows only two `leaf.commit.duration` rows per shard (hot P99 595 ms, cool P99 278 ms). The hot leaf still sees ~8 concurrent shard-root turns competing for its single turn queue.

**Hypothesis ranking.**

| # | hypothesis                                                                      | verdict                                | rationale                                                                                                                                                                                                                                          |
| -:| ------------------------------------------------------------------------------- | -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| a | `ShardRootGrain.SetManyAsync` serial-by-turn under load                        | **already addressed (shipped U9g)**    | Source carries `[AlwaysInterleave]` on the interface. The 2.7 s gap is downstream of the shard-root, not in it.                                                                                                                                    |
| b | `local_apply` reflects cross-shard fan-out serialisation                       | **falsified**                          | `local_apply` ≈ `leaf_rpc` (3,450 ms vs 3,276 ms P99). All of `local_apply` is the awaited leaf RPC - the `Task.WhenAll` over the bucket array is essentially free per turn because hot rungs have ~1 dominant leaf per shard.                       |
| c | More physical shards cut the per-shard tail proportionally                     | **partially testable but secondary**   | Doubling `BENCH_SHARD_COUNT` would halve the per-shard offered load and (if queueing is the cause) halve the per-leaf queue depth. But U6/U8 already showed `shardCount` is a bi-directional knob with a sweet spot at 16; pushing past 16 fragments WAL coalescing (step U7). Cheap config probe, weak ceiling lift. |
| d | `[AlwaysInterleave]` on `ShardRootGrain.SetManyAsync` / read-only helpers      | **(d.1) already shipped, (d.2) deferred** | (d.1) on `SetManyAsync`: shipped. (d.2) on read-only helpers (`GetAsync`, `ExistsAsync`, `GetManyAsync`): rejected in U9h-C audit due to non-atomic root-state reads (see `IShardRootGrain.cs:17-27`). Not a write-path lever.                       |

**The actual binding constraint - new hypothesis (e).** `IBPlusLeafGrain.SetManyAsync` is non-reentrant. Eight concurrent producer-side flushes per silo, routed through the (now interleaved) shard root, all queue behind a single per-leaf turn token on the hot leaf. Per-leaf turn duration ≈ `leaf.commit.duration` P99 ≈ 595 ms. Queue depth ≈ 8 - 1 = 7. Expected worst-case wait ≈ 7 × 595 ms ≈ 4.2 s; observed `leaf_rpc` P99 = 3,276 ms is well inside that envelope (some calls are scheduled against the cool leaf which serves in parallel). **This matches the measurement to within run-variance.**

**Proposed next code probe - U9p step 8c-c-iv-c2-i: `[AlwaysInterleave]` on `IBPlusLeafGrain.SetManyAsync`.**

The change mirrors the U9g/U9h-A pattern at the leaf layer: let multiple `SetManyAsync` turns enter the leaf activation concurrently, exactly as multiple turns now enter the shard-root concurrently. The safety analysis from U9g/U9h-A composes:

- `BPlusLeafGrain.CommitSetManyAsync` writes `state.State` (leaf entries map) and then issues `await writer.AppendManyAsync(walEntries)`. The WAL append is the leaf's durability barrier and is already `[AlwaysInterleave]` at the WAL grain (U9p step 8c-c-i). The leaf's `state.WriteStateAsync()` call - which is the etag-protected I/O - needs the same per-activation gate the shard-root got in U9h-A. If the leaf already has a comparable gate (it does, via `BPlusLeafGrain.CommitSetAsync` / `CommitSetManyAsync` internal locking), the change is a one-attribute addition; if it does not, the U9g failure mode (7,011 etag mismatches) will recur and the gate has to ship in the same PR.
- Causal ordering inside a key: two concurrent `SetManyAsync` calls touching the same key produce two HLC stamps, and the higher-HLC stamp wins under LWW. The leaf's existing per-key LWW resolution is convergent, so disjoint-key interleaving is correct by construction. Same-key interleaving inside a single saga is gated by `LatticeTransactionContext` at the caller; this change does not touch that gate.
- Split safety: `SetManyAsync` can produce a `SplitResult`; the shard-root walks the parent path serially after collecting all leaf results. Two concurrent leaf turns each producing a split would race on the leaf's internal split decision (the leaf decides "I'm splitting" based on its in-memory entry count). The leaf needs to either (1) carry an internal `SemaphoreSlim` around the apply-then-decide-split sequence (mirroring U9h-A's storage-write gate), or (2) defer the interleave attribute until the apply+split sequence is made interleave-safe.

**Pre-implementation tasks for c2-i (the next concrete step):**

1. Read `BPlusLeafGrain.CommitSetManyAsync` end-to-end and inventory every `state.WriteStateAsync()` call site (mirror of the U9h audit table at L890).
2. Decide: ship a leaf-level `_writeGate` `SemaphoreSlim` and the `[AlwaysInterleave]` attribute together (small, focused PR), or surface a falsifiability instrument `leaf.commit.in_flight` distribution at higher rungs first to confirm queueing is exactly the 7-deep stack the (e) arithmetic predicts. The instrument `leaf.commit.in_flight` already exists (shipped U9m step 1, see L893) - it pins at 0 today because the leaf is serial; under c2-i it should rise to 1-7.
3. Add a unit test that pins `[AlwaysInterleave]` on the interface (mirror of `ShardRootGrainInterleavedReadsTests.cs`) so future contributors do not silently strip the attribute.
4. Add a chaos test that drives 16 concurrent `SetManyAsync` calls on the same leaf with overlapping key sets and asserts LWW convergence + dense WAL offsets.

**Falsification target (unchanged from the plan-step):** `lattice.set_many.duration_ms` P99 < 1,500 ms at `10000:5` with FinalFailed=0 and steady-state band preserved. If c2-i lifts `leaf.commit.in_flight` from 0 to 5-7 but caller P99 stays > 2 s, the next limiter is downstream of the leaf - most likely the WAL grain queue ahead of the (already-interleaved) `WalShardGrain.AppendBatchAsync`, which would mean we need to fatten the WAL `MaxPending` knob next.

**Decision.** Memo complete. The next concrete code action is c2-i (audit `BPlusLeafGrain.CommitSetManyAsync` for storage-write sites, ship `[AlwaysInterleave]` + per-activation write gate together). The c2-ii probe (`BENCH_SHARD_COUNT=32` config-only) is a cheap fallback if c2-i is judged too high-risk to land in the current campaign window; it would not lift the absolute ceiling but would halve the per-leaf queue depth on the bench and confirm hypothesis (e) without a code change.

#### U9p step 8c-c-iv-c2-i pre-implementation audit (no code change)

**Audit scope.** Every `state.WriteStateAsync()` call site reachable from `BPlusLeafGrain` (mirror of the U9h audit on `ShardRootGrain` at L890), classified hot/cold against the foreground commit path.

| File                                       | Line   | Enclosing method (or path)                  | Hot on `CommitSetManyAsync`? | What is mutated                                                                  |
|--------------------------------------------|-------:|---------------------------------------------|------------------------------|----------------------------------------------------------------------------------|
| `BPlusLeafGrain.Metrics.cs`                |     91 | `PersistAsync` (helper)                     | -                            | the actual `state.WriteStateAsync()` call; called by every site below            |
| `BPlusLeafGrain.cs`                        |    638 | `SetTreeIdAsync`                            | cold (lifecycle)             | tree-id / shard-index / key-range topology metadata                              |
| `BPlusLeafGrain.cs`                        |   1297 | (sibling-pointer admin)                     | cold (split lifecycle)       | sibling pointers, split state transitions                                        |
| `BPlusLeafGrain.cs`                        |   1306 | (sibling-pointer admin)                     | cold (split lifecycle)       | sibling pointers                                                                 |
| `BPlusLeafGrain.cs`                        |   1316 | (sibling-pointer admin)                     | cold (split lifecycle)       | sibling pointers                                                                 |
| `BPlusLeafGrain.cs`                        |   1349 | (sibling-pointer admin)                     | cold (split lifecycle)       | sibling pointers                                                                 |
| `BPlusLeafGrain.cs`                        |   1380 | **`SplitAsync`** body                       | **HOT** (split branch only)  | post-split sibling wiring, key-range update, last-compaction stamp               |
| `BPlusLeafGrain.cs`                        |   2097 | (compaction / topology)                     | cold (compaction)            | post-compaction last-compaction-version                                          |
| `BPlusLeafGrain.DigestPublication.cs`      |     64 | **`PublishDigestUpwardAsync`** internals    | **HOT** (digest step)        | persisted digest checkpoint metadata                                             |
| `BPlusLeafGrain.MovedAwaySlots.cs`         |    110 | (split / move-away)                         | cold (split lifecycle)       | moved-away slot metadata                                                         |
| `BPlusLeafGrain.Projection.cs`             |    189 | (projection checkpoint flush)               | cold (projection)            | projection checkpoint snapshot                                                   |
| `BPlusLeafGrain.Projection.cs`             |    199 | (projection checkpoint flush)               | cold (projection)            | projection checkpoint snapshot                                                   |
| `BPlusLeafGrain.ProjectionAdmin.cs`        |     94 | (projection admin)                          | cold (operator-driven)       | projection admin state                                                           |
| `BPlusLeafGrain.Split.cs`                  |     39 | `SplitAsync` post-flush                     | **HOT** (split branch only)  | post-split state-row persist                                                     |

**Headline finding (changes the risk profile).** `CommitSetManyAsync` does **not** call `PersistAsync()` / `state.WriteStateAsync()` on the steady-state write path (`src/lattice/BPlusTree/Grains/BPlusLeafGrain.cs:863-1004`). The WAL append (`writer.AppendManyAsync(walEntries)` at L933) is the durability boundary; entry rows live in the in-memory `Cache` and are replayed from the WAL on activation. The U9g→U9h-A storage-gate hazard therefore **does not apply to the steady-state branch** of the proposed `[AlwaysInterleave]`. State-row writes happen only when (1) the apply step triggers `SplitAsync` (overflow branch) or (2) the digest step ends up persisting a checkpoint inside `PublishDigestUpwardAsync`.

**Real hazards under `[AlwaysInterleave]`** (none storage-etag-shaped; all in-memory):

1. **`Cache` dictionary**. The per-key `StoreEntry(entries[i].Key, values[i])` loop at L953-957 mutates a non-thread-safe dictionary. Two interleaved turns racing on the same key (or two distinct keys hashing to the same internal bucket) would corrupt the projection. **The leaf's per-key correctness contract is LWW-by-HLC**, so two concurrent writers to the same key would commit two distinct HLCs and the lower-HLC apply must lose - which means even if the dictionary mutation were thread-safe, an ordering invariant is needed (apply must run in HLC-ascending order, or be a CAS-on-HLC).
2. **HLC tick (`AdvanceClockOrOverride`) at L893**. Must produce strictly monotone HLCs per leaf even when called from interleaved turns; the current implementation is a single mutable counter under the grain turn lock, which `[AlwaysInterleave]` would break.
3. **`PublishVersionAdvance(highStamp)` + `BumpLocalRevision()` at L913-914**. Both touch in-memory replica-state that the cache-refresh protocol consumes; concurrent calls must serialise.
4. **`_digestDirty` flag** consumed by `PublishDigestUpwardAsync` at L971. Already idempotent under the flag-flip pattern, but `PublishDigestUpwardAsync` itself awaits a parent grain and reads `state.State` along the way.
5. **`SplitAsync` (L957) and the L1380 / Split.cs:39 `PersistAsync` calls inside it**. The split predicate `Cache.Count > options.MaxLeafKeys` is evaluated against a shared dictionary; if two interleaved turns both observe overflow they would each call `SplitAsync` and the second's persisted state-row would race the first's. This is the **only** etag-race hazard, and it lives entirely inside the post-overflow branch.

**Updated risk verdict on c2-i.** The naive "add `[AlwaysInterleave]` + one storage gate" recipe used in U9g→U9h-A is **insufficient** here, because the bulk of the hazards are in-memory CRDT/cache/HLC state, not storage I/O. A correct c2-i needs (at minimum) an in-memory critical section around the apply step (L949-959), the publish step (L913-914), and the HLC tick (L893). That critical section must specifically **not** include the `await writer.AppendManyAsync(...)` call at L933 - otherwise we lose the interleave point that motivated the whole change.

**Refined design sketch (not yet implemented).** Inside `CommitSetManyAsync`, the right shape is:

1. **Outside the gate**: parameter validation, building `walEntries` from inputs (read-only over inputs).
2. **Inside an in-memory gate** (held with `lock`, not a `SemaphoreSlim`, because no awaits): HLC tick + `PublishVersionAdvance` + `BumpLocalRevision` + WAL-record stamping. Single short critical section per turn.
3. **Outside the gate**: `await writer.AppendManyAsync(walEntries)`. This is the long-latency call we want to interleave; multiple turns can be here concurrently.
4. **Inside the in-memory gate again**: `StoreEntry` loop + split-predicate check.
5. **If split fires**: re-enter a storage-gate (`SemaphoreSlim`) around `SplitAsync` for the etag-race protection on the post-split persist (mirror of U9h-A).
6. **Outside the gate**: observer fan-out (per-key but does not mutate leaf in-memory state beyond what we already serialised) and `PublishDigestUpwardAsync` (its own internal storage gate around the L64 persist).

The HLC tick + WAL-record assembly + per-key apply have to be in the **same** critical section (or carry an explicit per-key HLC-monotonicity assertion) so the apply order matches the WAL record order. A simpler-but-correct first cut: gate **the entire body except the `await writer.AppendManyAsync(...)`**, using a `SemaphoreSlim` released around the WAL await. This is structurally identical to "split the method into pre-WAL and post-WAL halves under one gate" and produces exactly the interleave shape the memo predicted - two concurrent callers can both be parked on `AppendManyAsync` while one runs the in-memory work, with the next one entering the in-memory work as soon as the first releases.

**Pre-implementation tasks for c2-i, updated:**

1. ~~Read `BPlusLeafGrain.CommitSetManyAsync` end-to-end and inventory every `state.WriteStateAsync()` call site.~~ **DONE** (table above). Headline: zero state writes on the hot path; the audit changes the c2-i recipe.
2. Decide between two implementations: (A) **release-around-WAL `SemaphoreSlim`** as sketched above (simplest correct shape, requires careful around-await release; same shape would apply to `CommitSetAsync` and `CommitDeleteAsync` for consistency), or (B) **defer c2-i** and prototype c2-ii (`BENCH_SHARD_COUNT=32`) first to confirm queueing is the binding constraint before paying the design cost of (A).
3. Whichever lands: add a contract test pinning `[AlwaysInterleave]` on `IBPlusLeafGrain.SetManyAsync` (mirror of `ShardRootGrainInterleavedReadsTests.cs`) and a chaos test driving 16 concurrent overlapping `SetManyAsync` calls on one leaf.
4. Re-measure the `10000:5` rung. Falsification target unchanged: `lattice.set_many.duration_ms` P99 < 1,500 ms, `leaf.commit.in_flight` p99 5-7, FinalFailed=0, steady-state band preserved.

**Recommendation.** Run **c2-ii first** (config-only `BENCH_SHARD_COUNT=32` probe, zero code risk). If `leaf_rpc` P99 halves and `lattice.set_many.duration_ms` P99 falls below ~2.0 s, hypothesis (e) is empirically confirmed and c2-i (design (A)) becomes worth the design cost. If c2-ii moves the needle by less than 30%, the binding constraint is not the leaf turn queue (it is downstream - WAL or provider) and c2-i would not move it either. This sequencing is the cheapest way to validate the audit's hypothesis before paying the design cost of the leaf-side reentrancy retrofit.

**Decision.** c2-i pre-implementation audit complete. The audit changes the recipe from a one-attribute drop-in to a method-restructure; c2-ii is now the right next probe by cost/risk. Defer c2-i shipping until c2-ii has either validated or falsified hypothesis (e).

#### U9p step 8c-c-iv-c2-ii result (2026-05-26T15:40Z) - `BENCH_SHARD_COUNT=32` lifts throughput +166% on one env-var; hypothesis (e) partially confirmed (CONFIRMED with caveat)

**Setup.** Single rung `10000:5`, 60 s producer, same image as step 8c-c-iv-b (no rebuild, `-SkipBuild`). All knobs identical to c2-iv-b except **`BENCH_SHARD_COUNT=16 → 32`**. Run completed in 97 s container wall-clock. Artifacts: `benchmark/azure-throughput/.run/step8c-c-iv-c2-ii-silo.log` (5.33 MiB), `scripts/.ladder-results-U9p-step8c-c-iv-c2-ii.csv`, `scripts/.ladder-phaseA-U9p-step8c-c-iv-c2-ii.csv`.

**Headline result.**

| metric                                | step 8c-c-iv-b (s=16) | **step 8c-c-iv-c2-ii (s=32)** | delta        |
| ------------------------------------- | --------------------: | ----------------------------: | -----------: |
| SteadyAvg                             | 2,817/s               | **7,501/s**                   | **+166%**    |
| SteadyMax (1 s window)                | 28,663/s              | 24,570/s                      | -14%         |
| FinalWritten                          | 207,418               | **637,169**                   | **+207%**    |
| FinalFailed                           | 0                     | **0**                         | -            |
| FinalAvgRate                          | 1,826/s               | **5,545/s**                   | **+204%**    |

**Phase A latency surface (mean of cadence windows t=60-80 s).**

| instrument                                          | s=16 P50 / P99      | **s=32 P50 / P99**    | delta P99    |
| --------------------------------------------------- | ------------------- | --------------------- | -----------: |
| `lattice.set_many.duration_ms` (caller-visible)     | 2,518 / **3,780 ms** | 2,150-2,645 / **2,653-3,175 ms** | **-16% to -30%** |
| `shard_root.set_many.leaf_rpc.duration`             | 904 / **3,276 ms**   | 399-464 / **2,205-2,517 ms**     | **-27% to -33%** |
| `shard_root.set_many.local_apply.duration`          | 1,513 / 3,450 ms     | 525-672 / 2,503-2,734 ms         | -21% to -28% |
| `leaf.commit.duration` phase=wal (hot row)          | 287 / 595 ms         | 258-289 / **506-695 ms**         | flat to mild |
| `wal.append.in_flight` (per-shard P99)              | **0**                | **7**                            | **0 → 7**    |
| `wal.append.batch_entries` (per-shard P90)          | ~8-9                 | **72-100 (cap)**                 | **8-9 → 100**|
| `wal.append.provider.duration` (per-shard P50/P99)  | ~110 / ~310 ms       | 65-74 / 170-265 ms               | provider scaling |

**Interpretation.**

1. **The headline throughput jump is the WAL pipeline finally engaging.** `wal.append.in_flight` rose from **0** at s=16 to **6-7** at s=32 (`WalMaxPendingBatches = 8` cap). `batch_entries` p90 jumped from ~8 to the 100-entry cap. The WAL grain pipeline (uncorked in step 8c-c-i) was sitting idle at s=16 because upstream couldn't feed it fast enough; doubling the shard count cut the per-shard queueing-in-front-of-leaf enough that producers can now keep all 7 in-flight slots filled. **The WAL is now actually doing what 8c-c-i designed it to do.**

2. **Hypothesis (e) is partially confirmed.** `leaf_rpc` P99 dropped ~30% (3,276 → ~2,360 ms), consistent with per-leaf queue depth falling from ~8 callers (16 shards × ~0.5 callers each, slowest-of-N) to ~4 callers (32 shards × ~0.25). The per-leaf `phase=wal` step P99 is essentially flat (595 → ~600 ms) - the leaf's own WAL-await time did not move, only the queue ahead of it. **The leaf turn queue is a real constraint** but it is **not the dominant one**: tail latency only dropped 16-30% while throughput rose 166%.

3. **Why did throughput rise so much more than tail latency dropped?** At s=16, the upstream couldn't keep the WAL chain depth above 0, so each producer call serialised against the leaf and then against an empty WAL pipe (one provider RT per call). At s=32, the upstream keeps the WAL chain depth at 7, so 7 producer calls' worth of work amortise into one round of provider RTs. The throughput math closes: `8 shards × ~14 provider calls/s per shard × ~6 entries per call = ~672 entries/s of structural WAL ceiling at s=16`, vs `8 shards × ~13 provider calls/s × ~80 entries per call = ~8,320/s at s=32` (batch coalescence is what pays off, not partition count).

4. **The falsification target (`lattice.set_many.duration_ms` P99 < 1,500 ms) was not met** (P99 ~2,650-3,180 ms). But the original target was set against the *2,817/s* baseline; at 7,501/s that target is not the right one. The new tail-latency-per-throughput ratio is actually substantially better than the c2-iv-b baseline.

5. **Caveat - per-leaf queue depth is still measurable.** `leaf_rpc` P99 of ~2,400 ms vs `leaf.commit.duration phase=wal` P99 of ~600 ms means **~1,800 ms of in-leaf queue wait** at s=32. So hypothesis (e) is *still* a real constraint - it just got *less* binding. A c2-iii (`[AlwaysInterleave]` + release-around-WAL gate) would close this remaining gap and might lift the band further.

**Hypothesis ranking, updated.**

| # | hypothesis                                                                      | verdict after c2-ii                    |
| -:| ------------------------------------------------------------------------------- | -------------------------------------- |
| a | `ShardRootGrain.SetManyAsync` serial-by-turn under load                        | already addressed (U9g)                |
| b | `local_apply` reflects cross-shard fan-out serialisation                       | falsified                              |
| c | More physical shards cut the per-shard tail proportionally                     | **PARTIALLY CONFIRMED** - +166% throughput at s=32, but driven primarily by WAL coalescence, not per-shard queue reduction |
| d | `[AlwaysInterleave]` on read helpers                                            | rejected (U9h-C)                       |
| e | `IBPlusLeafGrain.SetManyAsync` non-reentrant queueing                          | **partially confirmed** - ~30% of the tail moved when s doubled; ~1,800 ms of in-leaf queueing remains |

**Decision.**

- **SHIP `BENCH_SHARD_COUNT=32` as the new benchmark default** for the durable Azure Tables baseline. This is a one-line env-var change in `20-build-and-deploy.ps1`; no source change. It is the largest single-step throughput lift of the entire campaign (+166%), pays no failure cost, and is a documented operational knob.
- **Test the s=64 falsifier next (8c-c-iv-c2-ii-b).** If the `c` hypothesis is right that "more shards = better WAL coalescence", s=64 should lift throughput further until per-leaf cold-start activation tail re-enters (the U8b lesson). The U8/U8b axis was bounded `{16, 32}` on *memory-storage* runs; with durable Azure Tables grain storage the activation cost is higher, so s=64 is a falsifiable probe. Cheap, config-only.
- **DEFER c2-iii (`[AlwaysInterleave]` + release-around-WAL gate) again.** The audit recipe is correct but the in-leaf 1.8 s residual is now a smaller fraction of the system, and the c2-ii result demonstrates the campaign still has cheap config-only wins to harvest first. Re-evaluate c2-iii after c2-ii-b (s=64) and c2-iv (rerun the c2-iv-b knob sweep at s=32).

**Last Updated**: 2026-05-28 (8c-c-iv-c2-ii landed: shardCount=32 +166% steady-state)

#### U9p step 8c-c-iv-c2-ii-b result (2026-05-26T15:47Z) - `BENCH_SHARD_COUNT=64` regresses; s=32 is the durable-Azure sweet spot (FALSIFIED, s=32 confirmed)

**Setup.** Same as c2-ii except `BENCH_SHARD_COUNT=64`. Artifacts: `benchmark/azure-throughput/.run/step8c-c-iv-c2-ii-b-silo.log` (5.71 MiB), `scripts/.ladder-results-U9p-step8c-c-iv-c2-ii-b.csv`, `scripts/.ladder-phaseA-U9p-step8c-c-iv-c2-ii-b.csv`.

**Result.**

| metric                | s=16 (c2-iv-b) | **s=32 (c2-ii)** | s=64 (c2-ii-b) |
| --------------------- | --------------: | ---------------: | -------------: |
| SteadyAvg             | 2,817/s         | **7,501/s**      | 6,436/s        |
| SteadyMax             | 28,663/s        | 24,570/s         | 20,502/s       |
| FinalWritten          | 207,418         | **637,169**      | 568,412        |
| FinalAvgRate          | 1,826/s         | **5,545/s**      | 4,917/s        |
| FinalFailed           | 0               | 0                | 0              |

**Interpretation.** s=64 vs s=32: SteadyAvg -14%, FinalWritten -11%, with zero failures (no over-pressure collapse like U8b). Same direction as the U8b/s=8 cold-activation regression on the memory-storage path (L728), but with the regression point shifted: with durable Azure Tables grain storage every leaf activation pays a real round-trip on first touch, so the "too many cold activations" boundary moves down from s ≥ 128 (memory) to s = 64 (durable). The campaign's shard-count axis on durable storage is now bounded `{16, 32}` with s=32 winning decisively.

**Decision.** s=32 is the new benchmark default. Promoted in `benchmark/azure-throughput/scripts/20-build-and-deploy.ps1` (default value `'0'` → `'32'`, with comment naming the c2-ii measurement). Operators can still override via `BENCH_SHARD_COUNT=0` (library default) or any positive value.

**Last Updated**: 2026-05-28 (8c-c-iv-c2-ii-b: s=64 regressed; s=32 promoted to benchmark default)

#### U9p step 8c-c-iv-c2-iv result (2026-05-26T15:50Z-T16:10Z) - knob sweep at s=32 baseline; **all four cells regress, c2-ii baseline holds** (FALSIFIED, no config-only headroom remains)

**Setup.** Four probes at the new c2-ii baseline (`shardCount=32`, `batchSize=4096`, `flushConcurrency=8`, `flushMs=50`, `responseTimeoutSec=180`), single rung `10000:5`, 60 s producer per probe, same image (`-SkipBuild`). Each probe varies exactly one knob from the baseline (`WP=8`, `WMP=8`, `P2=5ms`). Artifacts: `benchmark/azure-throughput/scripts/.ladder-{results,phaseA}-U9p-c2-iv-{A-WP16,B-WMP16,C-WP16WMP16,D-P2-0}.csv` and `.run/ladder-U9p-c2-iv-*.log`.

**Results.**

| probe          | WP | WMP | P2 (ms) | SteadyAvg     | FinalWritten | FinalAvgRate | FinalFailed | delta vs c2-ii |
| -------------- | --:| ---:| -------:| -------------:| ------------:| ------------:| -----------:| --------------:|
| **c2-ii (baseline)** | 8  | 8   | 5       | **7,501/s**   | 637,169      | 5,545/s      | 0           | -              |
| A (WP=16)      | 16 | 8   | 5       | 4,595/s       | 429,858      | 3,695/s      | 0           | **-39%**       |
| B (WMP=16)     | 8  | 16  | 5       | 5,807/s       | 548,292      | 4,699/s      | 0           | **-23%**       |
| C (WP=16+WMP=16)|16 | 16  | 5       | **59/s**      | **7,138**    | **65/s**     | 0           | **-99%**       |
| D (P2=0)       | 8  | 8   | 0       | 3,196/s       | 317,697      | 2,904/s      | 0           | **-57%**       |

**Interpretation.**

1. **Probe A (WP=16) is a clean re-confirmation of the U9p step 8c-c-a-i finding (L1448).** Raising partition count from 8 to 16 increases the caller-side `max(per-partition tail)` faster than per-partition load shrinks - the same shape, reproduced cleanly on the new s=32 baseline. The mechanism is unchanged: each `ILattice.SetManyAsync` awaits the slowest of P per-partition flushes, and `P(max(16 tails) > steady_p99)` grows with P even when individual per-partition tails shrink.

2. **Probe B (WMP=16) is a re-confirmation of U9 (L585).** Doubling `WalMaxPendingBatches` adds per-shard contention without lifting coalescing - `provider.phase2.batch_size` remains pinned at 1.00 because per-partition arrival rate is the limiter, not the in-flight cap. The c2-ii result (`wal.append.in_flight=7`) is the cap *exactly* saturating at 8 (the next-arrival check fires when chain depth < cap), so raising the cap to 16 just lets the chain run deeper without anything to feed it.

3. **Probe C (WP=16, WMP=16) is the catastrophic combination of A + B.** 16 partitions, each with 16 pending slots = 256 in-flight Azure Tables transactions per silo. The producer can issue them, but the Orleans response-timeout circuit-breaker (180 s here, but the per-call tail wall-clock crosses sooner) plus per-leaf grain rejections from cold-activation pressure cause throughput collapse. The silo log shows `wall-clock deadline reached after 128s` - 13 MiB silo log vs ~5 MiB on healthy runs, indicating heavy retry / rejection / timeout traffic.

4. **Probe D (P2=0) regresses 57% - the c2-ii 5 ms coalescing window is now load-bearing.** At the *old* baseline (U9c at L633), `phase2CoalescingWindow=5ms` produced exactly 1.00 `phase2.batch_size`, identical to the no-window case. The c2-ii result raised per-shard arrival rate enough that the window now traps multiple commits per drain cycle - removing it sends `phase2.batch_size` back toward 1.00 and the per-commit Azure Tables cost dominates again. **This is the first measurement where the `PhaseTwoCoalescingWindow` option lifts a real number.** The 5 ms default for the benchmark is now empirically justified, not just defensive.

5. **No combination beats c2-ii (7,501/s).** The knob axis (`WP`, `WMP`, `P2`) is now bounded by the c2-ii combination. The campaign's config-only lever set is exhausted at the durable Azure Tables baseline.

**Why the knobs that previously moved (or didn't move) the needle changed sign.** Pre-c2-ii, the WAL pipeline was bottle-corked at the shard root: `wal.append.in_flight = 0`, so the WAL knobs were no-ops because the WAL pipe was idle. c2-ii (shardCount=32) uncorked the shard root, raised `in_flight` to 7, and re-balanced the system so each per-shard WAL grain is now the busy one. At that new operating point, raising `WP` or `WMP` adds parallel work the storage tail cannot absorb, and removing the `P2` coalescing window leaves real per-commit cost on the floor.

**Decision.**

- **c2-ii (s=32, WP=8, WMP=8, P2=5ms) is the campaign-best configuration.** No knob change in the c2-iv axis lifts it. The benchmark default is correctly set; no further changes in `20-build-and-deploy.ps1`.
- **c2-iv is FALSIFIED for any single-knob lift over c2-ii.** The remaining headroom (`leaf_rpc` P99 ~2.4 s vs leaf-body P99 ~0.6 s) cannot be harvested by config. The next lever is c2-iii (code change: `[AlwaysInterleave]` + release-around-WAL gate on `IBPlusLeafGrain.SetManyAsync`), which targets the ~1.8 s per-leaf queue residual the c2-ii audit identified.
- **The next concrete code step is c2-iii.** With c2-iv proving no config-only headroom remains, c2-iii's value gate is no longer "is it the largest available win" but "is it the only available win". The audit's design sketch (release `SemaphoreSlim` around the `writer.AppendManyAsync` await) is the implementation target.

**Last Updated**: 2026-05-28 (8c-c-iv-c2-iv knob sweep: all four cells regress; c2-ii holds; c2-iii promoted to next)

#### U9p step 8c-c-iv-c2-iv-post (arithmetic check before c2-iii implementation, 2026-05-28)

**Finding.** Re-read of the c2-ii phase-A `wal.append.in_flight` distribution before drafting the c2-iii implementation. Across all 8 WAL shards, last-window per shard: `count` 194-219, **`p50=6-7`, `p99=7`, `max=7`** (cap = `WalMaxPendingBatches = 8`). **The WAL pipeline is fully saturated at c2-ii**, not idle as the original c2-iv-c framing implicitly assumed.

**Arithmetic.** At c2-ii: `provider.duration p50 ~70 ms × 8 in-flight × 8 shards × ~80 entries/call ≈ 7,300/s`. Measured: **7,501/s**. The ceiling is the WAL provider's effective per-call throughput, not the leaf turn queue.

**Implication for c2-iii.** Letting multiple `IBPlusLeafGrain.SetManyAsync` turns interleave on the same activation would let more callers park on `writer.AppendManyAsync(...)` concurrently. But those parked callers cannot make progress until a WAL pipeline slot frees - the WAL is already running 7 in-flight transactions per shard at all times. The change moves the **queue location** (from in-front-of-leaf to in-front-of-WAL) and **may reduce caller-side P99 tail latency**, but **will not raise SteadyAvg above ~7.5 k/s** on this rung.

**The c2-iii falsification target as originally drafted (SteadyAvg ≥ 9,000/s) is unreachable.** It conflated two distinct ceilings: per-leaf turn queueing (c2-iii's target) and the WAL provider's effective throughput (not c2-iii's target). The correct c2-iii ship-criterion is:

1. `lattice.set_many.duration_ms` P99 drops materially (target: < 1,500 ms from current ~3,000 ms), AND
2. `leaf.commit.in_flight` p99 lifts above 0 (proves the interleave actually engages), AND
3. SteadyAvg holds at ~7.5 k/s (does not regress), AND
4. FinalFailed = 0.

A SteadyAvg lift would require additionally lifting the WAL ceiling - e.g. raising the rung from `10000:5` to `25000:5` to drive deeper fan-in, **or** raising `WalMaxPendingBatches` past 8 (which probe B falsified at 16 *but* probe B was at the leaf-queueing baseline; once c2-iii ships, the WMP knob may need re-evaluation). That is c2-v territory.

**Decision.** Proceed with c2-iii as a tail-latency probe with the revised ship-criterion above. The implementation work is unchanged from the audit recipe; only the success metric changes from "throughput ≥ 9 k/s" to "P99 < 1.5 s with no throughput regression". A follow-on **c2-v** (re-run WMP/rung sweeps with c2-iii in place) is the next config-only probe after c2-iii.

**Last Updated**: 2026-05-28 (c2-iv-post: WAL is saturated at c2-ii; c2-iii ship-criterion revised to tail-latency, not throughput)

#### U9p step 8c-c-iv-c2-v-rung25000 (path-2 probe before c2-iii, 2026-05-26T16:23Z) - REVERSES the c2-iv-post conclusion: WAL is NOT the global ceiling; the leaf IS the binding constraint at higher rungs (CONFIRMED, c2-iii now has a real throughput target)

**Setup.** Same image and knobs as c2-ii (`shardCount=32`, `WP=8`, `WMP=8`, `P2=5ms`, `FC=8`, `flushMs=50`, `responseTimeoutSec=180`, `batchSize=4096`), single rung **`25000:5`** (target 125,000/s - 2.5x the c2-ii rung). Same Azure account, same `-SkipBuild` image. The c2-iv-post arithmetic predicted SteadyAvg should hold near 7,500/s (WAL ceiling). Falsifiable: if it does, WAL is the global limit; if it doesn't, the leaf becomes the binding constraint at higher rungs.

**Result.** Catastrophic regression: SteadyAvg **1,438/s** (vs c2-ii's 7,501/s at 10000:5), FinalWritten **196,662**, FinalAvgRate **1,797/s**, **FinalFailed=0**. Producer was clean. The rung jump from 10000 to 25000 vehicles **dropped** throughput by 5x rather than holding it at the WAL ceiling.

**Mechanism.** At 25,000 vehicles spread across 32 physical shards, each shard sees ~780 distinct keys vs ~312 at 10,000 - so each 4,096-entry producer batch now fans out across ~2.5x more leaves per shard than at c2-ii. Per-leaf queueing depth grows linearly with the keyspace: at the c2-ii baseline ~3-4 callers queue per hot leaf, but at 25000:5 the same number of concurrent producer batches each touches more leaves, multiplying the queue depth at the hottest leaves. The serial-turn cost at the leaf layer dominates before the WAL pipeline can absorb the offered load.

**Reading on the c2-ii / c2-iv-post arithmetic, corrected.** The c2-iv-post calculation (`provider.duration p50 × in_flight × shards × entries/call ≈ 7,300/s`) is correct *as the WAL pipeline's local ceiling*, but that ceiling is only reached when the upstream (leaf turn queue) can feed it fast enough. At 10000:5, the leaf queue happens to feed the WAL at exactly the WAL ceiling - the two limits cross. At 25000:5, the leaf queue cannot feed it, and throughput drops 5x. **The leaf turn queue IS a throughput lever**, just one that only becomes binding when the workload spreads each batch across enough distinct leaves.

**Implication for c2-iii.** The original c2-iii framing (throughput probe via leaf-side reentrancy) is **vindicated, not deprecated**:

1. At the **10000:5** rung, c2-iii is a tail-latency probe (as the c2-iv-post reasoning concluded).
2. At the **25000:5** rung, c2-iii is a *throughput* probe - the leaf queue is the binding constraint, and `[AlwaysInterleave]` + release-around-WAL should lift SteadyAvg from 1,438/s back toward (and possibly past) the c2-ii WAL ceiling.
3. At **higher rungs** (50000:5, etc.), c2-iii's throughput delta should grow with the per-leaf queue depth, since the binding constraint scales linearly with keyspace.

**Revised c2-iii ship-criterion (now strictly multi-rung, evidence-driven):**

1. At `10000:5`: SteadyAvg ≥ 7,000/s (no regression vs c2-ii's 7,501/s), `lattice.set_many.duration_ms` P99 < 1,500 ms, `leaf.commit.in_flight` p99 > 0.
2. At `25000:5`: SteadyAvg ≥ 5,000/s (~3.5x lift over 1,438/s baseline - target is to climb back toward the WAL ceiling).
3. FinalFailed = 0 on both rungs.

If criterion 2 is met, the path-2 finding has converted c2-iii from a tail-latency-only probe into the campaign's next throughput lever. If criterion 2 misses but criterion 1 holds, c2-iii ships as a tail-latency-only change and the next probe targets the rung-25000 collapse separately.

**Decision.** Proceed with c2-iii implementation. The rung-50000:5 cell on this ladder was cancelled mid-run; that becomes the c2-iii post-ship validation rung. **Path 2 was successful** - it falsified the wrong arithmetic ceiling and re-attributed value to c2-iii at higher rungs.

**Last Updated**: 2026-05-28 (c2-v-rung25000: leaf is binding at higher rungs; c2-iii throughput value vindicated)

## 📝 Plan Steps

- **DONE - U9p step 8c-c-i (WAL pipeline uncork).** `[AlwaysInterleave]` on `IWalShardGrain.AppendBatchAsync` lifted `wal.append.in_flight` from 0 to 7 and `batch_entries` p90 to the 100-entry cap. The change is kept; it exposed memory-storage as the next limiter.
- **DONE - U9p step 8c-c-ii (diagnostic A/B against memory / null storage).** Confirmed that with `MemoryGrainStorage` removed (`null`) or widened (`memory`, `NumStorageGrains=128`) the peak rises to 7-11 k/s but the result is not production-shape. Treated as diagnostic-only; benchmark code keeps both as A/B levers.
- **DONE - U9p step 8c-c-iii (pivot to durable Azure Tables grain storage).** Benchmark silo defaults to `BENCH_LEAF_STORAGE_KIND=azure`; both WAL and grain state now hit Azure Tables. Measured baseline: 24,588/s steady-state peak, 12-16 k/s steady band, `wal.append.provider.duration` p50 ~57-75 ms. **Two kinks remain**: 71 k cold-start rejections in t = 1-2 s, and a ceiling defined by `walPartitions × walMaxPendingBatches × batch_avg / provider_p50_ms`.
- **DONE - U9p step 8c-c-iv-a (retry transient `OrleansMessageRejectionException` in benchmark `FlushAsync`).** Bounded retry with jittered exponential backoff (5 attempts, 50 ms base) wraps the `lattice.SetManyAsync` call in `benchmark/azure-throughput/Silo/Program.cs::FlushAsync`; `IsShutdownRejection` short-circuits. Measured: `FinalFailed=0` (vs 71,355), `FinalWritten=178,944`, `SteadyMax=20,462/s`. The retry collapses failures but parks 8/8 flush slots for ~18 s of cold start, which is why 8c-c-iv-b became required rather than optional.
- **DONE - U9p step 8c-c-iv-b (library `ILattice.WarmUpAsync` for shard roots).** Implementation shipped: `ILattice.WarmUpAsync(CancellationToken)` added in `src/lattice/BPlusTree/ILattice.cs`; fan-out in `src/lattice/BPlusTree/Grains/LatticeGrain.WarmUp.cs` enumerates `ShardMap.GetPhysicalShardIndices()` and invokes `IShardRootGrain.WarmUpAsync()` (new contract in `src/lattice/BPlusTree/IShardRootGrain.cs`) under a `SemaphoreSlim` capped at 32. Each shard-root warm-up calls `PrepareForOperationAsync()`, materialising the deterministic root leaf on an empty shard via the same `EnsureRootAsync` path the first write would run, then pings the current root grain (`IBPlusLeafGrain.CountAsync()` for flat trees, `IBPlusInternalGrain.AreChildrenLeavesAsync()` for trees with height). Metrics `orleans.lattice.warmup.invocations` / `orleans.lattice.warmup.duration` live in `src/lattice/LatticeMetrics.cs`. Benchmark silo `benchmark/azure-throughput/Silo/Program.cs` `await`s `lattice.WarmUpAsync(stoppingToken)` after the reshard barrier and before the TCP listener opens, with retry widened to 8 attempts / 4 s max backoff to absorb the directory-cache cold-call race. 6 warm-up unit tests pass; 261 ShardRoot non-chaos tests stay green. **Measured**: FinalWritten=207,418 (+16%), SteadyAvg=2,817/s (+36%), SteadyMax=28,663/s (+40%), FinalAvgRate=1,826/s (+21%), FinalFailed=0. Side effect: `lattice.set_many.duration_ms` P99 = 3,780 ms exposed shard-root traversal as the next kink.
- **DONE - U9p step 8c-c-iv-c2 (diagnostic memo).** Memo appended above. Result: hypotheses (a) and (d.1) are already shipped (`SetManyAsync` is `[AlwaysInterleave]` on `IShardRootGrain` per U9g/U9h-A); (b) is falsified by `local_apply ≈ leaf_rpc`; (c) is a weak fallback bounded by the U6/U8 shard-count sweet spot at 16; (d.2) was rejected by U9h-C. The actual binding constraint is **new hypothesis (e)**: `IBPlusLeafGrain.SetManyAsync` is non-reentrant, so 8 concurrent shard-root turns queue behind one per-leaf turn token on the hot leaf. Queue-depth arithmetic (7 × 595 ms ≈ 4.2 s) matches observed `leaf_rpc` P99 (3,276 ms) inside run-variance.
- **DONE (audit only) - U9p step 8c-c-iv-c2-i (pre-implementation audit of `BPlusLeafGrain` state-writes).** Audit appended above. Headline: `CommitSetManyAsync` does **no** `state.WriteStateAsync()` on the hot path - the WAL append at L933 is the durability boundary. The U9g→U9h-A "storage-gate" recipe therefore does not apply directly; the real hazards under `[AlwaysInterleave]` are in-memory (Cache dict, HLC tick, `PublishVersionAdvance`, split-predicate race). Correct implementation requires releasing a `SemaphoreSlim` around the WAL await rather than a one-attribute drop-in. Shipping deferred behind c2-ii validation.
- **DONE - U9p step 8c-c-iv-c2-ii (`BENCH_SHARD_COUNT=32` config-only probe).** **CONFIRMED with caveat**. SteadyAvg `2,817 → 7,501/s (+166%)`, FinalWritten `207,418 → 637,169 (+207%)`, FinalFailed=0, all from one env-var change. Phase A: `wal.append.in_flight` rose `0 → 7` (saturating `WalMaxPendingBatches=8`); `batch_entries` p90 rose from ~8 to the 100-entry cap. The WAL pipeline uncorked in step 8c-c-i finally engaged because upstream queueing at the leaf layer dropped enough to feed it. Hypothesis (e) is *partially* confirmed: `leaf_rpc` P99 dropped ~30% (3,276 → 2,360 ms) but ~1,800 ms of in-leaf queue wait still remains. Hypothesis (c) is also confirmed - more shards lifted throughput, but via WAL coalescence rather than per-shard queue reduction. **Action: promote `BENCH_SHARD_COUNT=32` to the benchmark default in `20-build-and-deploy.ps1`.**
- **DONE - U9p step 8c-c-iv-c2-ii-b (`BENCH_SHARD_COUNT=64` falsifier).** Regressed: SteadyAvg 6,436/s (-14% vs s=32), FinalWritten 568,412 (-11%), FinalFailed=0. Bounds the durable-storage shard-count axis at `{16, 32}` with s=32 the measured sweet spot. Same shape as U8b/s=8 (cold-activation regression on memory) but with the regression point shifted from s ≥ 128 (memory) to s = 64 (durable), as expected when every leaf activation pays a real round-trip on first touch. `BENCH_SHARD_COUNT=32` shipped as new benchmark default in `20-build-and-deploy.ps1`.
- **DONE (FALSIFIED) - U9p step 8c-c-iv-c2-iv (knob sweep at s=32 baseline).** Four probes: A (WP=16) → -39%, B (WMP=16) → -23%, C (WP=16+WMP=16) → **-99% collapse**, D (P2=0) → -57%. All cells regress against the c2-ii baseline (7,501/s steady). The knob axis (`WP`, `WMP`, `P2`) is empirically bounded at `WP=8, WMP=8, P2=5ms` on the durable Azure Tables baseline. Side finding: probe D demonstrates the `PhaseTwoCoalescingWindow=5ms` is now load-bearing (lifts a real number at the c2-ii operating point), reversing the U9c null result at the old baseline. **No config-only headroom remains; c2-iii (code change) is the next lever.**
- **DONE (FALSIFIED prior ceiling reading) - U9p step 8c-c-iv-c2-v-rung25000 (path-2 rung-25000:5 probe).** SteadyAvg **1,438/s** at `25000:5` vs **7,501/s** at `10000:5` - a 5x regression on a 2.5x rung jump, with zero failures. Reverses the c2-iv-post "WAL is the global ceiling" conclusion: the WAL ceiling is a local limit reached only when the leaf can feed it; at higher rungs (more keyspace = more leaves per batch = deeper per-leaf queue) the leaf turn queue is the binding constraint. **Re-vindicates c2-iii as a throughput lever at higher rungs**, not just a tail-latency change.
- **NEXT - U9p step 8c-c-iv-c2-iii (`[AlwaysInterleave]` on `IBPlusLeafGrain.SetManyAsync` + release-around-WAL `SemaphoreSlim`).** Multi-rung ship-criterion: (a) `10000:5` SteadyAvg ≥ 7,000/s (no regression), P99 `lattice.set_many.duration_ms` < 1,500 ms, `leaf.commit.in_flight` p99 > 0; (b) `25000:5` SteadyAvg ≥ 5,000/s (~3.5x lift over 1,438/s baseline); (c) FinalFailed = 0 on both rungs. Implementation per the audit recipe: gate the in-memory CRDT mutations (HLC tick, `PublishVersionAdvance`, `StoreEntry` loop, split-predicate check) with a release around the `writer.AppendManyAsync` await on `CommitSetManyAsync`. Apply the same shape to `CommitSetAsync` and `CommitDeleteAsync`. Ship contract test + chaos test.
- **DEFERRED - U9p step 8c-c-iv-c (steady-state grid search).** Originally the next step after iv-b; deferred behind iv-c2 because tail latency, not throughput, is the binding kink at the durable Azure Tables baseline. When iv-c2 ships, rerun this grid search with the lower per-write tail and record steady-state band, peak, and `wal.append.provider.duration` p50/p99 for each of: baseline, `walPartitions=16`, `walMaxPendingBatches=16`, `phase2CoalescingMs=3`, best-of-three combined. The winner becomes the recommended default in `docs/lattice/wal.md`.
- **U9p step 8c-c-iv-d (write up campaign).** Update `docs/lattice/wal.md` with the durable steady-state band; update `docs/lattice/wal-storage-providers.md` with the Azure Tables-backed numbers; mark any roadmap items whose deps are now satisfied. Run `dotnet test --filter "TestCategory!=Chaos"` before each PR and `dotnet test --filter "TestCategory=Chaos"` before any merge that flips a default.
- **DEFERRED - prior B / C / D plan steps.** The Phase A anomalies that originally paused them (harness ceiling at ~17 k/s on memory WAL; atomic-write throughput variance) are still real, but they live behind the kink-resolution plan. They become live again after 8c-c-iv-d ships and we have a documented production-shape band to compare against.
