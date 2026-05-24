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

Phase A executed the full 46-cell matrix on 2026-05-24 (`benchmark/diagnostic-reports/diagnostic-report-2026-05-24T07-22-03Z.md`). The dominant suspect is **the Azure Tables provider path**, but the evidence forces a re-ordering of the Phase C step list defined above and surfaces two anomalies that were not in the original attribution table. This section records the findings; the **Phase C — Azure Tables provider scaling** step list is the source of truth for ordering from this point on, and the bullet under `## 📝 Plan Steps` for Phase C has been updated to match.

### Headline gap

- Documented Azure Tables ceiling: 2,000 entities/s/partition, 20,000/s/account on Standard storage.
- Phase A measured `current-state-no-replication-azuretable`: **280–500 ops/s** — i.e. ~2 % of the per-account ceiling and below a single-partition ceiling.
- The provider path is **35–60× slower** than the in-memory WAL path (~17,100 ops/s → ~400 ops/s) at fixed silo CPU of 4–7 %.
- `PipelinePhaseTwoCommits = true` improved throughput by only ~5–15 % across the sweep (e.g. cell 11 → 12: 457 → 495; cell 19 → 20: 285 → 321). C1 alone will not close the gap.

### Anomalies recorded before remediation

**Anomaly 1 — `current-state-no-replication` is perfectly flat at ~17,100 ops/s across all nine knob combinations, including across `WalPartitions ∈ {1, 4, 16}` at fixed `WalMaxPendingBatches`.** CPU stays at 6–8 %. Were `WalShardGrain` scheduling the limiter, raising `WalPartitions` from 1 to 16 should have moved the number. It did not. The 17,100 ops/s figure is therefore most likely a **bench-harness ceiling**, not a silo ceiling. Implication: **Phase B is paused** until the harness ceiling is independently verified; the scheduling signal in the original attribution table cannot be trusted while the harness is the cap.

**Anomaly 2 — More `WalPartitions` makes the azuretable path *worse*, not better.** At `WalMaxPendingBatches = 1, PipelinePhaseTwoCommits = false`: 1 partition → 457 ops/s; 4 partitions → 308 ops/s; 16 partitions → 336 ops/s. `wal.append.turn_wait` p99 also climbs steeply with partitions (14 ms → 50 ms → 188 ms). Real Azure Tables partition-server saturation would scale *up* with `WalPartitions`. This pattern instead indicates a **client-side shared contention point that gets worse as we fan out** (likely candidates: `HttpClient` connection-per-host limit, shared retry / throttle queue, or activation cost amplifying per-call overhead). This **invalidates the C5 premise** (more partitions == better spread) and **strengthens C2 + C4** (true client-side parallelism with bounded backoff). C5 is deferred until C2/C4 land and the partition-vs-throughput curve is re-measured.

**Anomaly 3 — atomic-write variance is too high to attribute saga cost.** Cell 30 reported 4 ops/s; cell 36 reported 0; cell 34 hit 31,960 ops/s. Per-cell wall-clock is too short to converge through cold-activation effects. **Phase D is paused** until the atomic-write bench has stabilised (longer runs, warm-up, deterministic concurrency).

### Wall-time vs server-time signature

The decisive signal: `current-state-no-replication-azuretable` p99 wall latency is **700–1,700 ms** while Azure Tables provider p99 (server-timing) is **10–130 ms** — a **5–100× gap on the slow tail**. This is the canonical signature of **retry storms / SDK backoff**, not of partition-server saturation. The existing `LatticeMetrics.ProviderRetryExhausted` counter fires only when the SDK gives up; we currently have no instrument for retries that ultimately succeed, which is the bucket that would explain a 700 ms wall p99 sitting on top of a 30 ms server p99.

### Dominant suspect & Phase C re-ordering

Dominant suspect: **the Azure Tables provider's client-side path**, specifically retry / throttling cost. Phase C is the correct next phase, but the step ordering changes as follows (this supersedes the order in the **Phase C — Azure Tables provider scaling** section above and is the canonical order for execution):

1. **C4 — Retry / backoff visibility & tuning (first).** Land a per-attempt retry counter (`provider.retry.attempts`) tagged with phase + HTTP status alongside the existing `provider.retry.exhausted`. Drive a focused micro-bench (single-shard, `WalPartitions = 1`, default options) and read the counter to confirm or refute the retry-storm hypothesis. If confirmed, tune the provider's `TableClientOptions.Retry` policy to per-target-partition jittered exponential backoff with a deadline budget; if refuted, re-attribute before moving on.
2. **C2 — Parallel batch transaction submission (second).** Bounded parallel `SubmitTransactionAsync` against distinct per-batch partition keys; phase-2 manifest update stays single-transaction so per-batch atomicity is unaffected.
3. **C1 — Default `PipelinePhaseTwoCommits = true` (third, demoted).** The mode is already tested and well-documented; the data shows it is worth ~10 %, still worth flipping but no longer the unlock.
4. **C3 — Payload compression (conditional).** Only schedule once C4/C2/C1 have landed and a fresh measurement shows phase-1 entity-row payload bytes dominate (Phase A did not probe row sizes). Cross-reference roadmap F-075.
5. **C5 — Partition-spread guidance (deferred / re-scoped).** Anomaly 2 contradicts the original premise; revisit only after C2 + C4 are in and the partition-vs-throughput curve is positive.

### What is *not* changing

- The Phase A diagnostic instruments (histograms + `ProviderRetryExhausted` counter + tag set) are kept verbatim; the new C4 counter is purely additive.
- The wire format remains frozen.
- Phase B and Phase D step lists above are still valid; they are paused, not invalidated, pending resolution of anomalies 1 and 3 respectively.

**Progress**: 0% [░░░░░░░░░░]

**Last Updated**: 2026-05-23 17:50:47

## 📝 Plan Steps
-  **Read `src/lattice/BPlusTree/Grains/WalShardGrain.cs` end-to-end, `src/lattice/BPlusTree/Grains/AtomicWriteGrain.cs`, `src/lattice.storage.azuretable/AzureTableWalStorageProvider.cs` (and `PhaseTwoWorker`), and `src/lattice/BPlusTree/Options/LatticeOptions.cs` — confirm every choke point listed in *Architectural context*; record any deviation from the plan's assumptions in `benchmark/diagnostic-report.md` (gitignored) before writing code.**
-  **Implement Phase A instrumentation in `WalShardGrain`, `AzureTableWalStorageProvider` (incl. `PhaseTwoWorker`), and `AtomicWriteGrain` — emit new histograms and counters through the existing diagnostics surface used by `leaf.commit.duration`; add a tag set for `WalPartitions` / `WalMaxPendingBatches` / `PipelinePhaseTwoCommits` so dashboards can pivot.**
-  **Add `benchmark/benchmark-attribution.ps1` — drive the matrix described in Phase A across microbench, `current-state-no-replication`, `current-state-no-replication-azuretable`, `atomic-write`, `atomic-write-replication`, and `benchmark/azure-throughput`; emit `benchmark/diagnostic-report.md` with ops/s, p50/p99 latency, CPU%, Azure server-timing sum, and histogram quantiles per matrix cell.**
-  **Run the matrix end-to-end and write the Phase A report — pick the dominant suspect; do not proceed to B/C/D until this is recorded.**
-  **Phase B — PAUSED by Phase A anomaly 1 (`current-state-no-replication` flat at ~17,100 ops/s across all 9 knob combinations including `WalPartitions ∈ {1, 4, 16}` indicates the bench harness, not the silo, is capping the scheduling-path measurement). Resume only after the harness ceiling is independently re-measured; if the true silo ceiling is then > 17,100 ops/s the original B1 → B4 → B5 → B2 → B3 order applies.**
-  **Phase C — Azure Tables provider scaling, the dominant suspect picked by Phase A. Execute in the re-ordered sequence recorded in the "Phase A — Outcomes" section: C4 (per-attempt retry visibility & backoff tuning) → C2 (parallel batch transaction submission with the ordering test) → C1 (default `PipelinePhaseTwoCommits = true`, demoted to ~10 % gain) → C3 (opt-in payload compression, conditional on a fresh row-size probe, cross-referenced with roadmap F-075) → C5 (partition-spread guidance, deferred pending C2/C4 re-measurement because Phase A anomaly 2 contradicted the original premise); re-measure after each.**
-  **Phase D — PAUSED by Phase A anomaly 3 (atomic-write throughput variance is ~four orders of magnitude across adjacent cells: 0, 4, and 31,960 ops/s observed in cells 30/36/34). Stabilise the atomic-write bench (longer runs, warm-up, deterministic concurrency) before picking D; if the saga path then under-performs, the original D3 → D1 → D2 → D4 order applies.**
-  **After each Phase B/C/D PR, run `dotnet test --filter "TestCategory!=Chaos"` and the targeted atomic-visibility + causal-correctness fixtures; before merging the default-flip PRs (B2, B3, C1), additionally run `dotnet test --filter "TestCategory=Chaos"` and append the result to the diagnostic report.**
-  **Final phase E roll-up — update `docs/lattice/wal.md`, `docs/lattice/wal-storage-providers.md`, and any roadmap entry whose deps are satisfied (e.g. F-075 if C3 ships), with the measured ops/s vs the documented Azure Tables ceiling captured in the docs.**

