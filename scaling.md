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

The benchmark surface area we will use as the **measurement bench**: `Bench.Microbench` (no Orleans cost), the simulator-driven `current-state-no-replication` (Orleans + WAL, no provider), `current-state-no-replication-azuretable` (Orleans + WAL + Azure Tables, the focal scenario), `atomic-write` and `atomic-write-replication` (saga path), `Bench.WalAzureTable` (Azurite structural probe — *cannot* prove throughput), and `benchmark/azure-throughput` (real Azure Tables harness, the only path to documented-ceiling numbers).

## Diagnostic-first methodology

The plan is structured **diagnose → attribute → remediate → re-measure**. No code change to defaults or hot paths until Phase A has pinned a primary suspect. Every remediation lands behind an opt-in flag first, gets re-measured, and only then has its default flipped — keeping chaos / integration tests bit-stable while the flag is off.

### Bottleneck attribution table

| Symptom in Phase A | Primary suspect | Phase that fixes it |
|---|---|---|
| Microbench (no Orleans) ≈ in-memory provider WAL throughput | Scheduling not the issue at the WAL layer in isolation | — |
| `current-state-no-replication` ≪ microbench, low CPU | Orleans grain scheduling / single `WalShardGrain` activation | Phase B |
| `current-state-no-replication` flat as `WalMaxPendingBatches` rises | Per-partition serialisation (turn / offset assignment) | Phase B |
| `current-state-no-replication-azuretable` ≪ `current-state-no-replication`, Azure Tables server-timing low | Provider client-side cost (phase-2 sync, per-row payload, retry/backoff) | Phase C |
| Azure Tables server-timing ≈ wall time, p99 spikes correlate with `ServerBusy` | Real partition-server saturation; need more `WalPartitions` | Phase B + Phase C |
| `atomic-write` ≪ `current-state` at same key rate | Saga-internal serialisation | Phase D |

## Invariants the plan must preserve

Drawn from `test/lattice/Chaos`, `test/lattice.replication/Chaos`, atomic-write integration tests, and causal-correctness tests:

1. **Dense, monotonic WAL offsets per shard.** Any reordering of in-flight flushes must not gap or duplicate offsets. `WalShardGrain` already assigns offsets *under the turn*; that contract is non-negotiable.
2. **All-or-nothing per `AppendBatchAsync`.** Provider-level atomicity per batch.
3. **Atomic visibility of saga writes.** `AtomicWriteGrain` callers must not observe a partial saga even if WAL partitions are now distinct per key. This is the load-bearing constraint for Phase D — parallel per-key fan-out only works if visibility remains gated by the saga commit point.
4. **Causal ordering inside a key.** Per-key writes from the same writer must apply in submission order regardless of WAL partition (already guaranteed because `WalPartitionHash` is key-deterministic; do not break this).
5. **Replication observer ordering per shard.** Observer hook fires after durable commit in WAL offset order.
6. **Chaos invariants:** replay reproduces durable state; trim is idempotent; failover does not reorder.

## Phase A — Diagnostic instrumentation (no behaviour change)

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

## Phase B — Core WAL scaling (if grain scheduling dominates)

Triggered when raising `WalPartitions` / `WalMaxPendingBatches` materially improves Phase A throughput without breaking pinned tests.

- **B1 — Validate `WalMaxPendingBatches > 1` under load.** The in-flight protocol already exists in `WalShardGrain`; add a chaos sub-scenario (`pending-batches=4` with provider fault injection) that proves offset density and order survive concurrent failures. Add unit tests that pin TCS completion order across in-flight flushes.
- **B2 — Raise default `WalPartitions` from 1 to a small power of two** (candidate: 4 or 8), gated on the matrix from Phase A showing linear-ish scaling and no chaos regressions. Document the migration impact: existing trees keep their on-disk partition count via `WalShardManifest`; the change only affects newly-created trees. Confirm `WalPartitionHash` is stable under the new default and that the routing change is invisible to integration tests (they don't pin a partition count).
- **B3 — Raise default `WalMaxPendingBatches` from 1 to ≥ 4.** Same gating: Phase A must show throughput gain at fixed p99 latency, and the new chaos sub-scenario from B1 must pass.
- **B4 — Eliminate avoidable turn work.** Audit `WalShardGrain.AppendAsync` for any per-call allocations / ETW emissions that can move off the grain turn. The turn must do only: validate, assign offsets, enqueue. Provider calls are already off-turn.
- **B5 — Buffer-pool review.** Confirm the `_pendingSegments` array pool sizing matches the new `WalMaxBatchEntries × WalMaxBatchBytes` defaults; tune to avoid pool churn at higher pending depth.

## Phase C — Azure Tables provider scaling (if provider dominates)

Triggered when Phase A shows `current-state-no-replication-azuretable` ≪ `current-state-no-replication`.

- **C1 — Default `PipelinePhaseTwoCommits = true`.** The mode is already documented (`docs/lattice/wal-storage-providers.md`) and tested. Wire-format unchanged. Gate behind Phase A showing the gain, and re-run the WAL durability and reconcile test suites.
- **C2 — Parallel batch transaction submission.** When a single `AppendBatchAsync` exceeds the 100-entity / 4 MiB Azure Tables transaction limit, the provider currently emits multiple transactions sequentially. Submit them in parallel against their distinct per-batch partition keys, joining with `Task.WhenAll`. Phase-2 manifest update remains a single transaction so atomicity per batch is unaffected (manifest update is the linearisation point). Add a unit test that pins parallel submission preserves entry ordering when read back.
- **C3 — Compression of large WAL payloads.** Currently aligns with roadmap F-075 (open). If Phase A shows phase-1 entity-rows dominated by payload bytes (close to the 1 MiB row limit on burst writes), enable opt-in payload compression on phase-1 rows. Wire-format implication: a new payload-encoding column on the entity row is provider-internal, *not* part of `WalEntry` — keeps the public wire format frozen. Cross-reference roadmap F-075 in the implementation PR.
- **C4 — Retry / backoff tuning.** If `ServerBusy` counts dominate the slow tail, switch the provider's retry policy to per-target-partition jittered exponential backoff with a deadline budget; emit `provider.retry.exhausted` for chaos visibility.
- **C5 — Account-partition spread guidance.** Document and (optionally) auto-derive an account-level partition spread so that `WalPartitions` × shard count comfortably exceeds the number of Azure partition servers; surface a startup warning when this is misconfigured.

## Phase D — Atomic-write saga scaling (if saga dominates)

Triggered when `atomic-write` ≪ `current-state-no-replication-azuretable` at equivalent per-key rate.

- **D1 — Parallel per-key fan-out inside `AtomicWriteGrain`.** Replace the serial `for (var key in saga) await SetAsync(...)` with a bounded `Task.WhenAll` (concurrency limit = `WalPartitions`, default cap to e.g. 16). Each per-key call must continue to flow ambient `LatticeTransactionContext` / `LatticeOriginContext` — extract these once before fan-out and re-establish them inside each parallel call using `AsyncLocal` plumbing so the contexts survive across `await` points.
- **D2 — Atomic visibility preservation.** Because parallel WAL appends across distinct partitions can interleave with concurrent sagas, reaffirm that the saga's commit/visibility model gates downstream observation: any reader observing intermediate per-key WAL records before the saga's commit record must still see the saga as in-flight. Add an atomic-visibility integration test that drives 32 concurrent overlapping sagas across the same key set and asserts no reader observes a partial saga.
- **D3 — Causal correctness inside a saga.** Two ops on the *same key* inside one saga must apply in submission order. Group the saga's keys by hash before fan-out so same-key ops dispatch on a single ordered chain; only distinct-key chains fan out.
- **D4 — Saga-throughput chaos scenario.** Extend `atomic-write` chaos: 16 concurrent sagas × 8-key fan-out × provider fault injection, asserting eventual atomic visibility and dense WAL offsets per partition.

## Phase E — Verification & roll-forward

- Re-run the Phase A matrix after each phase; record the new ops/s vs the documented Azure Tables ceiling in `benchmark/diagnostic-report.md`.
- Run the full chaos suite (`dotnet test --filter "TestCategory=Chaos"`) at the end of B, C, and D.
- Run the atomic-visibility / causal-correctness integration tests after every phase (these are the binding correctness contract per the clarification).
- Only flip defaults (Phase B2/B3, Phase C1) when both the bench gain and the chaos pass are recorded in the report. Each default flip is its own PR labelled `enhancement` with the report excerpt in the description.
- Update `docs/lattice/wal.md`, `docs/lattice/wal-storage-providers.md`, and any roadmap items whose deps are satisfied (notably F-075 if C3 ships).

## Risks & guardrails

- **Default-flip risk.** Raising `WalPartitions` changes per-tree fan-out at first creation; existing persisted trees are unaffected (manifest is the source of truth). Document this in `wal.md`.
- **Phase-2 pipelined mode + chaos.** The mode has tests but isn't the default; before flipping, run the WAL reconcile test suite which already exercises mid-flight crash semantics.
- **Saga parallelism + observer ordering.** Parallel saga fan-out does not change per-shard observer order (observer fires on each WAL append in WAL offset order). Visibility of a saga as a whole is the new invariant to test (D2).
- **Azurite limitation.** `Bench.WalAzureTable` cannot validate Phase C numerically — only `benchmark/azure-throughput` against a real account can. Keep Azurite for structural assertions only.
- **Wire format.** Treated as frozen. C3 explicitly stays provider-internal so this is preserved. If Phase A ever shows that `WalEntry` shape is itself the limiter, that becomes a separate scoped initiative.

## Phase A — Outcomes

Phase A executed the full 46-cell matrix on 2026-05-24 against **Azurite** (`benchmark/diagnostic-reports/diagnostic-report-2026-05-24T07-22-03Z.md`). An initial reading attributed the bottleneck to Azure SDK retry / backoff cost on the provider path and re-ordered Phase C with **C4 first**. A subsequent C4 A/B re-measurement and a closer look at the matrix falsified that reading. This section records the **falsifications**, the corrected attribution, and the next probe; the original measurement file is preserved as historical evidence, but its conclusion has been retracted.

### Confirmed findings (still load-bearing)

- **Headline gap is real.** `current-state-no-replication-azuretable` measured at **280–500 ops/s** vs `current-state-no-replication` at **~17,100 ops/s** (35–60× slower) at fixed silo CPU of 4–7 %. The Azure-Tables-shaped path has a real cost the in-memory path does not.
- **Anomaly 1 — bench harness ceiling.** `current-state-no-replication` is perfectly flat at ~17,100 ops/s across all nine `WalPartitions` × `WalMaxPendingBatches` combinations, CPU at 6–8 %. Raising `WalPartitions` from 1 to 16 did nothing.
- **Anomaly 3 — atomic-write variance.** Cell 30 reported 4 ops/s; cell 36 reported 0; cell 34 reported 31,960 ops/s on adjacent knob combinations. Per-cell wall-clock is too short to converge through cold-activation effects. **Phase D is paused** until the atomic-write bench has stabilised (longer runs, warm-up, deterministic concurrency).

### Retracted findings (falsified by C4 A/B + closer reading of the matrix)

- **Retry-storm hypothesis: FALSIFIED.** The C4 A/B re-measurement (`benchmark/diagnostic-reports/c4-tuning-ab-2026-05-24T13-55-34Z.md`) compared an SDK-default arm against a tuned-retry-budget arm (`MaxAttempts=2, Delay=40 ms, MaxDelay=400 ms, NetworkTimeout=5 s`) on the same baseline WAL cell. Both arms produced statistically indistinguishable throughput (~454 vs ~467 ops/s) and tail latency. The reasons:
  1. Azurite never returns 503 / 429 / ServerBusy, so the SDK has nothing to retry. `provider.retry.attempts` would be zero in both arms. Bounded retry budgets cannot move a number that is already zero.
  2. The matrix's leaf-commit latency distribution is **bimodal**, not retry-shaped. Cell 11: P50 = 0.07 ms, P99 = 794 ms — a P99 / P50 ratio of ~11,000×. Retry-shaped tails push P50, P75, P90, and P99 up together. A bimodal distribution where the median is essentially instant indicates **occasional burst queue-wait**, not a constant retry cost added to every call.
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

The earlier speculation that `digest` owned the tail is refuted by the data: `digest` P99 is **0.10 ms**, `apply` P99 is **0.10 ms**. The **`wal` step at the leaf owns the tail** (949 ms) — but the WAL grain's own self-clock (`wal.append.turn_wait`, which measures from method entry through TCS completion) is only **22.64 ms**. The **~927 ms gap between the leaf's `wal`-step view and the WAL grain's self-clock** is the dominant cost.

**Dominant remaining suspect: the cross-grain hop into the single `WalShardGrain` activation.** Under `WalPartitions = 1`, every leaf in the tree calls one `IWalShardGrain` activation. Orleans serialises grain calls per-activation, and the wait spent in that grain's turn queue — *before* `AppendAsync`'s body starts and *before* `appendStartTicks = Stopwatch.GetTimestamp()` runs — is invisible to `wal.append.turn_wait`. The leaf's `step=wal` clock starts in `BPlusLeafGrain.CommitSetAsync` *before* the cross-grain RPC, so it captures both the Orleans turn-queue wait and the WAL grain body. That arithmetic fits: leaf `step=wal` P99 (949 ms) ≈ Orleans turn-queue wait on the single WAL activation (~927 ms) + WAL grain body (~22 ms).

### Next probe (replaces the A1 "surface per-step quantiles" probe)

A1 is **already done** — `benchmark.ps1` emits `lattice_apply_p99_ms`, `lattice_digest_publish_p95_ms`, `lattice_digest_publish_p99_ms`, `lattice_digest_publishes_per_second`, `lattice_wal_append_p99_ms`, and `lattice_wal_appends_per_second` via step-filtered `histogram_quantile` queries against `orleans_lattice_leaf_commit_duration_milliseconds_*`. The metrics were always there; we just hadn't read them carefully.

**A2 — Instrument the cross-grain hop into `IWalShardGrain`. SHIPPED on branch `throughput` and MEASURED on 2026-05-24 (`benchmark/.run/current-state-no-replication-azuretable/2026-05-24T14-35-29Z/results.json`).** `LatticeMetrics.WalShardDispatchDuration` (instrument name `orleans.lattice.wal.shard.dispatch.duration`, unit `ms`) is recorded in `WalCommitLogWriter.AppendAsync` and on the per-partition fan-out in `AppendManyAsync` via `AppendForPartitionAsync`, tagged with `tree`, `shard`, `wal_partitions`, and `wal_max_pending_batches`. `benchmark.ps1` `$ScalarAliases` adds short stable names `lattice_wal_shard_dispatch_p95_ms` / `lattice_wal_shard_dispatch_p99_ms`. **A2 measurement (Azurite arm):**

| Metric | P50 | P95 | P99 |
|---|---|---|---|
| `wal.shard.dispatch.duration` (caller-side, A2) | 324.86 ms | 891.51 ms | **978.30 ms** |
| `wal.append.turn_wait` (WAL grain self-clock) | 8.73 ms | 19.63 ms | **32.75 ms** |
| `wal.append.provider_duration` | — | 19.06 ms | 24.20 ms |
| `leaf.commit.duration{step=wal}` (leaf-side) | — | — | **978.30 ms** |
| `leaf.commit.duration` (aggregate) | — | — | 913.21 ms |
| `apply` P99 / `digest_publish` P99 | — | — | 0.10 ms / 0.10 ms |

**Initial hypothesis — Orleans turn-queue wait at the single WAL activation — was tested by the B2 sweep below and FALSIFIED.** The arithmetic `dispatch P99 − turn_wait P99 ≈ 945 ms` is real, but the residual is *not* Orleans grain-scheduling tax — it is the per-shard serialized queue ahead of the awaited provider call, with `provider_duration` being the *single-call* cost rather than the *queued-call* cost.

### B2 measurement — sweep `WalPartitions ∈ {1, 2, 4, 8}` on both arms (2026-05-24T15:42Z–T15:46Z memory; T14:35Z–T15:40Z Azurite)

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

1. **Memory-WAL P=1 dispatch P99 is 1.4 ms.** If Orleans grain scheduling against a single `WalShardGrain` activation cost hundreds of milliseconds, we would see it on the memory arm too — the activation, the turn queue, and the RPC are identical between the two arms; only the provider differs. We don't. The dispatch tail on Azurite is **not** Orleans-bound.
2. **On Azurite, every additional partition strictly regresses every metric** (dispatch P99 +122%, provider P99 +310%, ops/s −33% from P=1 to P=8). That is the opposite of B2's prediction. The dispatch histogram is measuring the per-shard queue ahead of the awaited `IWalShardGrain.AppendAsync` call, and that queue is bounded by how fast the *provider* can drain — on Azurite the provider serialises through a single backing process, so multiplying partitions multiplies in-flight provider calls against the same constrained backend.

**Corrected attribution.** The 978 ms dispatch P99 at P=1 is the per-shard backlog of leaf commits waiting for the awaited Azurite `SubmitTransactionAsync` to return; under load the leaf-side `wal` step measures `queue-wait + provider-call`, while the WAL grain's `turn_wait` only measures the body of one un-queued append. The arithmetic still holds, but the residual is provider-queue, not grain-schedule.

**Phase B2 (raise default `WalPartitions` from 1) is RETRACTED** on the Azurite arm and **rejected on the memory arm** (neutral for throughput because the system is bottlenecked downstream of the WAL — leaf-side apply / observer / scheduling — not at dispatch). The library default of `WalPartitions = 1` is correct as shipped.

**Phase C is re-elevated.** The dominant cost on Azurite is provider-bound (`provider_duration` rises 24→99 ms across the sweep as concurrency grows). The retry-storm framing of C is still retracted, but the *provider-throughput-scaling* framing (batching, pipelining, parallel partition keys against a real Azure Tables account) is exactly what the measured evidence supports. The Azurite single-process serialisation is a measurement artifact that masks the real-Azure scaling shape; the next probe must move to `benchmark/azure-throughput`.

### What stays in place

- The Phase A diagnostic instruments (histograms + tag set) are **unchanged**; the data was right, the interpretation was wrong. The per-step `leaf.commit.duration` quantiles are already in `results.json` (the A1 probe was a no-op).
- The new candidate instrument is the **cross-grain dispatch histogram** described in A2; it goes on `WalCommitLogWriter`, not on `WalShardGrain` (the WAL grain's clock cannot see its own turn-queue wait by construction).
- The C4 observability slice (`provider.retry.attempts` per-attempt counter, `RetryAttemptTrackingPolicy`) is **kept**. It is correct production telemetry and costs nothing on the happy path; the only retraction is its **Phase A justification**, not its presence.
- The C4 tuning knobs on `AzureTableWalStorageOptions` (`RetryMaxAttempts`, `RetryDelay`, `RetryMaxDelay`, `RetryNetworkTimeout`, `RetryMode`) are **kept** as production-hygiene knobs. They are correct shape for operators who deploy against a real Azure Tables account that does surface 503s. Their A/B-measured null effect on Azurite is expected, not a defect.
- The wire format remains frozen.
- Phase B and Phase D step lists above are still valid; they remain paused, not invalidated, pending resolution of anomalies 1 and 3 respectively.

**Progress**: 0% [░░░░░░░░░░]

**Last Updated**: 2026-05-24 15:50:00

## 📝 Plan Steps
-  **Read `src/lattice/BPlusTree/Grains/WalShardGrain.cs` end-to-end, `src/lattice/BPlusTree/Grains/BPlusLeafGrain.cs` (foreground commit path, especially `CommitSetAsync` and `PublishDigestUpwardAsync`), `src/lattice/BPlusTree/Grains/AtomicWriteGrain.cs`, `src/lattice.storage.azuretable/AzureTableWalStorageProvider.cs` (and `PhaseTwoWorker`), and `src/lattice/BPlusTree/Options/LatticeOptions.cs` — confirm every choke point listed in *Architectural context*; record any deviation from the plan's assumptions before writing code.**
-  **Phase A instrumentation — SHIPPED. `LatticeMetrics` already emits per-step `leaf.commit.duration` (tagged `wal` / `apply` / `observer` / `digest`), WAL queue / batch / in-flight / turn-wait / provider-duration histograms, and `provider.retry.attempts` per-attempt counter. `benchmark/benchmark-attribution.ps1` drives the 46-cell matrix.**
-  **Run the matrix end-to-end and write the Phase A report — done (`benchmark/diagnostic-reports/diagnostic-report-2026-05-24T07-22-03Z.md`). Initial attribution to Azure SDK retry / backoff cost was subsequently FALSIFIED by the C4 A/B re-measurement; see "Phase A — Outcomes" for the retracted findings and the corrected next probe.**
-  **A2 — Cross-grain dispatch instrumentation (SHIPPED AND MEASURED). `LatticeMetrics.WalShardDispatchDuration` (`orleans.lattice.wal.shard.dispatch.duration`) is recorded in `WalCommitLogWriter.AppendAsync` and on the batched per-partition fan-out, tagged with `tree` / `shard` / `wal_partitions` / `wal_max_pending_batches`. `$ScalarAliases` in `benchmark.ps1` adds `lattice_wal_shard_dispatch_p95_ms` and `lattice_wal_shard_dispatch_p99_ms`. The 2026-05-24T14:35Z Azurite measurement (`benchmark/.run/current-state-no-replication-azuretable/2026-05-24T14-35-29Z/results.json`) shows `dispatch P99 = 978.30 ms`, `turn_wait P99 = 32.75 ms`, `provider_duration P99 = 24.20 ms`, `apply P99 = digest P99 = 0.10 ms` — the ~945 ms gap between dispatch and turn_wait confirms the Orleans turn-queue-wait hypothesis on the single `WalShardGrain` activation under `WalPartitions = 1`. Phase B2 is unblocked on Azurite evidence; real-Azure ship-criterion is throughput delta on `benchmark/azure-throughput` against a real Azure Tables account because Anomaly 1's harness ceiling may otherwise mask the gain.**
-  **B2 (RETRACTED, 2026-05-24T15:50Z) — Raising `WalPartitions` does not improve dispatch P99 on either arm. Memory-WAL: P=1 dispatch P99 is already 1.4 ms (proves the residual is not Orleans grain-scheduling), and throughput is flat at ~4,290 ops/s across P ∈ {1,2,4,8} (bottleneck is downstream of the WAL). Azurite: P=1→P=8 monotonically regresses dispatch P99 (978→2,176 ms), `provider_duration` P99 (24→99 ms), and ops/s (94.6→63.8) because Azurite serialises all `SubmitTransactionAsync` calls through a single backend lock — adding partitions multiplies in-flight provider calls against the same constrained backend. The library default of `WalPartitions = 1` is correct as shipped. The corrected attribution is that the dispatch P99 residual is the per-shard queue ahead of the awaited provider call (provider-queue, not grain-schedule).**
-  **NEXT — Validate the corrected attribution on `benchmark/azure-throughput` against a real Azure Tables account. Azurite's single-process serialisation is a measurement artifact that inverts the partition-scaling shape; the real-Azure provider scales with partition keys, so the dispatch histogram should *shrink* (not grow) as `WalPartitions` rises against a real backend. If confirmed, Phase C's *provider-throughput-scaling* framing (parallel partition keys, batching, pipelining — NOT the retracted retry-storm framing) becomes the next concrete probe. If the real-Azure shape *also* shows that adding partitions hurts, then the bottleneck is on the leaf-side commit path (`BPlusLeafGrain.CommitSetAsync` waiting on its own WAL fan-in), and the next probe shifts to leaf-side concurrency rather than WAL-side partitioning.**
-  **Phase B — PAUSED by Phase A anomaly 1 (`current-state-no-replication` flat at ~17,100 ops/s across all 9 knob combinations including `WalPartitions` ∈ `{1, 4, 16}` indicates the bench harness, not the silo, is capping the scheduling-path measurement). Resume only after the harness ceiling is independently re-measured; if the true silo ceiling is then > 17,100 ops/s the original B1 → B4 → B5 → B2 → B3 order applies.**
-  **Phase C — UN-PAUSED on the *provider-throughput-scaling* slice (parallel partition keys, batching, pipelining) once the real-Azure validation in the NEXT step confirms the corrected attribution. The retry-storm framing that originally motivated C4-first remains RETRACTED; C4 observability + tuning knobs already shipped and are kept as production-hygiene telemetry. C1–3 and C5 remain candidates depending on the real-Azure measurement.**
-  **Phase D — PAUSED by Phase A anomaly 3 (atomic-write throughput variance is ~four orders of magnitude across adjacent cells: 0, 4, and 31,960 ops/s observed in cells 30/36/34). Stabilise the atomic-write bench (longer runs, warm-up, deterministic concurrency) before picking D; if the saga path then under-performs, the original D3 → D1 → D2 → D4 order applies.**
-  **After each Phase B/C/D/F PR, run `dotnet test --filter "TestCategory!=Chaos"` and the targeted atomic-visibility + causal-correctness fixtures; before merging the default-flip PRs (B2, B3, C1), additionally run `dotnet test --filter "TestCategory=Chaos"` and append the result to the diagnostic report.**
-  **Final phase E roll-up — update `docs/lattice/wal.md`, `docs/lattice/wal-storage-providers.md`, and any roadmap entry whose deps are satisfied (e.g. F-075 if C3 ever ships), with the measured ops/s vs the documented Azure Tables ceiling captured in the docs.**
