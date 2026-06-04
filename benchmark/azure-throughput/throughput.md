# throughput.md - performance investigation continuation

> Renamed from `wedge-plan2.md` on 2026-06-04 after Phase 1 closed without
> reproducing any wedge on a deterministic VM. The reliability question is
> answered (see `wedge-plan.md` section 23); this document carries the
> performance follow-up forward.
>
> Original framing (Phase 0 environment move, Phase 1 wedge re-verification,
> Phase 2 bound-by-bound wiring fix, Phase 3 at-saturation matrix) is
> preserved below for context. The post-Phase-1 sections (24+) are the
> live work.

# wedge-plan2.md - reliability continuation: using the bounds to stop wedging at >=4k

> Continuation of `wedge-plan.md`. The previous campaign (G-019..G-026)
> introduced four independent bounding mechanisms:
>
> 1. **`WalAppendDispatchTimeout`** (writer-side dispatch deadline, G-023)
> 2. **`WalFlushPreflightTimeout`** (shard-side preflight deadline, G-023)
> 3. **`ShardForwardTimeout`** (outbound shard-forward deadline, G-021)
> 4. **`WalMaxPendingBatches` admission cap** on `WalCommitLogWriter.PartitionTracker._inFlight` (G-026)
>
> Plus diagnostic packs G-024 (`[wal-slot]` lifecycle) and G-025
> (`[wal-append]` lifecycle + tracker depth).
>
> The campaign closed in section 22 of wedge-plan.md claiming the 4k
> rung wedge phenotype was eliminated. The operator reports the wedge
> **still reproduces reliably at >=4k vehicles** in the current main
> tip. This plan re-opens the investigation with the bounds as the
> tool set, not the deliverable: the bounds are SHIPPED; the question
> now is **why they are not preventing the wedge in practice and what
> wiring change makes them actually fire**.

## 0. Step-back: is ACI the right tool for this?

Almost certainly **no** for the reliability-iteration phase. ACI was
the right tool for the original "stand up something representative
fast" goal, but the last six cycles of wedge-plan.md show ACI is
now actively distorting the investigation more than it is helping.

### Concrete ACI-induced costs paid by this campaign

| Cost | Evidence in wedge-plan.md |
|---|---|
| **Bench-scraper log duplication misread as 10x event counts** | Section 21: "53 reshards" was actually 5; "35 watchdog firings" was 1; "1,715 SentToShard stamps" was ~28. Three full investigation cycles (G-027, sections 19-20) chased an artefact of `az container logs` + multi-pipe stdout scraping. |
| **`az container logs` 60s tail buffer** | Section 12: "Run 2 orphan; ACI silo retrieved via `az container logs` ... no FINAL, no `[stall-watchdog]`". Lost the actual wedge phenotype because the buffer truncated before exfiltration. |
| **No live attach for ClrMD / dotnet-counters / dotnet-dump** | Section 5: "Source-walking has reached its limit; the next step needs either a minimal repro or an upstream report." That limit was reached because the live wedge is unreachable for a debugger, not because the question was unanswerable. |
| **Cold-start / placement variance across runs** | Section 13: "n=7 cohorts ... 1 healthy / 1 Mode A / 5 Mode B". The bimodality was framed as a Lattice mechanism (sections 13-14) and later shown (section 16) to be a snapshot-timing artefact. A deterministic host removes the variance and removes the false-mechanism cycle. |
| **Opaque scheduling / noisy-neighbour CPU** | The 4k throughput baseline (869 e/s wedged median, section 18) and the 25k saturation rate (~250 e/s sustained, section 21) are both at-host-tier numbers with no isolation. Re-running on the same ACI SKU produces materially different shapes per cohort. |
| **No accelerated networking, no SR-IOV** | All Azure-Tables RTT measurements (5.7s / 9s head-of-line stuck times, sections 16 and 19) are conflated with ACI's shared-NIC egress. A single VM with accelerated networking would partition the latency budget into provider vs transport. |
| **vCPU ceiling (max 4 in serverless tier)** | Section 9: "bump silo to 4 vCPU/8 GiB (silo later reverted to 2/4)". The campaign has been thrashing inside the ACI scaling envelope for the entire run; saturation at 4k is exactly the rung where 2-4 vCPU stops being enough headroom to distinguish overload from wedge. |

### Recommended alternatives, in priority order

1. **Single Azure VM (`Standard_D8s_v5` or `D16s_v5`) with
   accelerated networking, in the same region as the Azure Tables
   account.** Deterministic CPU, deterministic NIC, full `dotnet-*`
   tool surface, SSH for live diagnostics, journald for stdout
   without the bench-scraper duplication path. The bench harness
   already runs as a console app; deploying as `systemd` unit or
   `screen` session is one script. This is the lowest-friction move
   and removes ~80% of the artefact surface from the last campaign.
2. **AKS single-node pool (same VM SKU) with the silo as a
   `Deployment` and the producer as a separate `Pod`.** Adds
   OTel-collector sidecar for first-class metric scraping (replaces
   the bench-harness `[phaseA]` stdout reporter that section 21
   showed disagreeing with `[silo]` reporter rates). Worth doing
   AFTER the single-VM phase if a sustained at-scale matrix is
   needed; not necessary for the reliability question this plan
   targets.
3. **Local repro against Azurite + a real Tables endpoint for the
   final confirmation.** The current console-app wedge repro (deleted in the
      same cleanup that moved these docs here)
   already demonstrated (sections 6-7) that the platform primitives
   work in isolation, so the local repro is most valuable as a
   wiring-validation step for each new bound (does the deadline
   fire? does the counter increment?) before paying ACI cost to
   confirm at scale.
4. **Keep ACI ONLY for the final cross-environment confirmation
   cohort once the fix lands on the single VM.** Treat ACI as the
   "does this still hold under a different host shape" smoke test,
   not the daily-driver iteration environment.

The rest of this plan is written assuming the single-VM move
happens first (Phase 0). All cohort steps below name a host and a
diagnostic channel explicitly so the change of environment is
auditable.

## 1. Re-verify the claim

The wedge-plan.md section 18 verdict ("4k rung: heavy-wedge
phenotype eliminated 3/3") is contradicted by the operator's
current observation. Before any new mechanism work, the actual
state of HEAD needs to be confirmed on a deterministic host.

| Step | Action | Host | Decisive output |
|---|---|---|---|
| **R1** | `git log` between `1dff59c` (PR #579 merge, end of last campaign) and current HEAD on main. Inventory anything in `src/lattice/` that touches `WalCommitLogWriter`, `WalShardGrain`, `PartitionTracker`, or any of the four bound options. | local | List of suspect commits since the green cohort. |
| **R2** | On a `Standard_D8s_v5` VM in the same region as the Tables account, run n=5 cohorts at rung `4000:5 -DurationSec 60` against current HEAD with all bounds at default. Capture stdout via `systemd-cat` (deterministic single-writer journal, no scraper dedup needed). | single VM | Wedge rate at 4k on a clean host. If <1/5 wedges, the wedge is ACI-induced and the action is "stop running this on ACI". If >=3/5 wedges, the wedge is real and the bounds are not firing. |
| **R3** | If R2 wedges: capture one `dotnet-dump collect` + one `dotnet-counters` snapshot mid-wedge (5-second window after `[stall-watchdog]` first fires). | single VM | Live process state - the missing artefact from the entire prior campaign. |

R2 is the gate. The remaining phases assume R2 confirms a real
wedge on a deterministic host.

## 2. Why the existing bounds are not firing (the four-corner audit)

Each of the four shipped bounds has a documented non-firing path
in wedge-plan.md sections 2-5 (Options A and B; both
`WaitAsync(TimeSpan)` and linked-CTS `WaitAsync(token)` "the
catch genuinely never enters"). Section 22's "campaign resolved"
verdict was reached without ever observing any of the four
deadline catches actually fire under a live wedge. The
diagnostics confirm the bounds are CONFIGURED (banner stamp); the
counters confirm they DO NOT FIRE (`wal.append_dispatch.timeouts=0`
throughout sections 12-21).

The mechanism question for this phase: **what wiring change
makes at least one of the four bounds reliably fire when the
wedge starts, so the system exits the wedge instead of pinning?**

| Bound | Current wiring | Documented failure mode | Phase 2 hypothesis |
|---|---|---|---|
| `WalAppendDispatchTimeout` (30s) | `Task.WaitAsync(token)` on grain RPC return Task with `CancellationTokenSource.CancelAfter` (Option B, fb4912e) | Catch never enters; cancellation callback appears to need the grain context that is itself wedged (wedge-plan.md section 4). | **H1**: move the deadline OFF the awaiter and onto an `IDisposable` watchdog that runs on a dedicated `Thread` (not `ThreadPool`, not `Timer`) and faults the writer's `TaskCompletionSource` from outside the grain scheduler. Validates by counter increment on the next wedged cohort. |
| `WalFlushPreflightTimeout` (5s) | Same `WaitAsync(token)` shape inside `WalShardGrain.FlushAsync` | Same as above; AND emit is gated behind an early return that section 13 documented as silently failing closed. | **H2**: pre-emit the preflight stamp BEFORE the early return (mirror the `1624183` fix for `[wal-slot-grain]`), and run the same off-grain watchdog as H1. |
| `ShardForwardTimeout` | Per-call `WaitAsync(token)` on outbound forward | Bound exists for parked forwards; does NOT cover Orleans-rejected forwards that throw `OrleansMessageRejectionException` synchronously (wedge-plan.md section 19; the hypothesis was later refuted but the wiring gap is real: the bound only fires on hangs, not on synchronous reject-then-retry storms). | **H3**: add a per-target-shard retry-budget so rejected-forward retries cap out and surface as typed failure, instead of relying on `ShardForwardTimeout` which the rejection path bypasses. |
| `WalMaxPendingBatches` admission cap | `SemaphoreSlim.WaitAsync(token)` with `WalAppendDispatchTimeout` deadline | If R2 confirms wedge at 4k, EITHER the semaphore is not being acquired on the wedge path (a code path bypasses `PartitionTracker.AcquireAsync`), OR the cap is too high relative to the wedge onset rate. | **H4**: instrument every `PartitionTracker.AcquireAsync` call site with an entry stamp; assert during R2 that the wedge-path `WalCommitLogWriter.AppendAsync` calls do hit the tracker. If not, that's the wiring bug. |

## 3. Phased execution

### Phase 0 - environment move (prereq)

- **P0.1**: Provision one `Standard_D8s_v5` VM with accelerated networking in the same region as the existing Tables account. Use the existing bench scripts (they target Linux already).
- **P0.2**: Port the bench harness output channel from "stdout scraped by `az container logs`" to "`systemd-cat -t lattice-silo`" with a `journalctl -u lattice-silo --output cat` extract step. Verify no line duplication on a 30-second smoke run (the section 21 deduplication scripts should report 1:1 unique:total).
- **P0.3**: Confirm `dotnet-counters monitor` and `dotnet-dump collect` work against the silo PID end-to-end (smoke test on a non-wedged run).

### Phase 1 - R1, R2, R3 (re-verify the wedge on a clean host)

- Execute R1 / R2 / R3 from section 1 above.
- **Decision gate**: if R2 shows <=1/5 wedges, halt; the wedge was ACI-induced and the action is to update wedge-plan.md section 22 with "valid on deterministic hosts; ACI is unsupported as a load-test environment" and re-run any throughput numbers on the single VM. Skip phases 2-3.
- If R2 shows >=2/5 wedges, proceed to Phase 2.

### Phase 2 - bound-by-bound wiring fix

Execute H4 first (cheapest, falsifies fastest), then H1, H2, H3 in order. Each is an independent commit; each ends with the same n=5 cohort at rung `4000:5` on the single VM.

- **H4 (instrumentation only)**: add `wal.writer.append.tracker_acquire.calls` counter incremented at the top of `PartitionTracker.AcquireAsync` for every call site. Re-run R2-shape cohort. **Decisive**: counter rate during wedge tells you whether the wedge path acquires the tracker at all. If zero rate during wedge, there is a code path that bypasses the cap; find it via call-tree search from `WalCommitLogWriter.AppendAsync` and bound it.
- **H1**: implement off-grain watchdog for `WalAppendDispatchTimeout`. New `WalDispatchWatchdog` service: dedicated `Thread`, processes a queue of `(TaskCompletionSource, DateTime deadline, string reason)` entries, faults the TCS when the deadline passes regardless of grain context. Wire writer dispatch to register/unregister; remove the existing `WaitAsync(token)` deadline wrapper. **Decisive**: `wal.append_dispatch.timeouts` counter > 0 on next wedged cohort; wedge exits within `WalAppendDispatchTimeout + 1s` instead of pinning.
- **H2**: pre-emit `[wal-slot]` Preflight stamp before the early return in `WalShardGrain.FlushAsync`; rewire `WalFlushPreflightTimeout` to the same off-grain watchdog as H1. **Decisive**: `wal.flush.preflight_timeouts` counter > 0 on next wedged cohort.
- **H3**: add `ShardForwardRetryBudget` option (default 8 attempts); track per-(source,target) shard pair; on exhaustion fault the original caller with typed `ShardForwardExhaustedException`. **Decisive**: under a reshard-rejection storm cohort, the exception surfaces at the caller instead of the system pinning. (May not be necessary if H1+H2 alone clear the wedge; defer if R2 phenotype matches H1/H2 signal.)

### Phase 3 - the at-saturation matrix on the single VM

After at least one of H1-H4 lands and the 4k rung is healthy on the
single VM, run a rung sweep: `4000:5`, `8000:5`, `16000:5`,
`25000:5` (the original campaign rung), n=3 each. The success
shape is:

- 0 wedges that pin (every wedge that occurs exits via a typed
  bound firing within the configured deadline).
- `wal.append_dispatch.timeouts` and/or
  `wal.flush.preflight_timeouts` rate is **non-zero at saturation**
  - the bounds are now load-bearing instead of dormant.
- Producer-side: every overloaded request surfaces as
  `TimeoutException` from the API surface within `~30s`, never as
  a 120s+ silent stall.

## 4. Out of scope for this phase

- Per-flush latency reduction (Family A from section 17). The
  baseline ~250 e/s sustained at 25k (section 21) is a real
  per-flush cost question, but it's a perf cycle, not a reliability
  cycle. After Phase 3 closes with bounds load-bearing at all
  rungs, the perf cycle can start cleanly.
- Any further ACI cohorts. Per Phase 0, ACI is dropped from the
  iteration loop; one final ACI cross-environment cohort is fine at
  the end of Phase 3 as a portability smoke check, but is not on
  the critical path.
- `OnDeactivateAsync` hook reliability (section 4). The hook never
  fires on a wedged grain; making it fire is its own investigation
  and is independent of whether the writer-side bounds fire.

## 5. Exit criteria

The phase is done when, on the single VM, at the 4k rung with n>=5
cohorts:

1. Either no wedge reproduces (the existing bounds were sufficient
   and the prior wedges were ACI artefacts), OR every wedge that
   reproduces exits via a typed bound firing recorded in the
   `wal.append_dispatch.timeouts` / `wal.flush.preflight_timeouts` /
   `ShardForwardExhaustedException` counters within the configured
   deadline.
2. `inFlight` is never observed pinned for > `WalAppendDispatchTimeout + WalFlushPreflightTimeout + 5s` in any cohort sample.
3. `wedge-plan.md` section 22 is amended with the corrected
   verdict (whichever Phase 1 / Phase 3 produced).
4. A short ADR is added under `docs/lattice/` capturing "load
   testing happens on a deterministic VM, not ACI; here is why".

---

## 24. Phase 1 closeout 2026-06-04 (HEALTHY across the rung range; wedge does not reproduce)

n=2 cohorts on the `Standard_F8as_v6` VM in westus3 (8 vCPU AMD Zen4, 32 GiB, accelerated networking confirmed end-to-end). Managed identity to a fresh storage account (`stlat01fid4svskfi27s`); silo+producer co-located via `lattice-silo.service` + `lattice-producer.service` systemd units; cohort runner extracts journals via `benchmark/vm/run-cohort.ps1`.

| Cohort | Written | Failed | Active avg | Silo CPU peak | Diagnostics | Verdict |
|---|---|---|---|---|---|---|
| 4k:5 / 30s | 547,006 | 0 | 13,884 e/s | 220% | clean | HEALTHY |
| 25k:5 / 30s (default 30s grain timeout) | 36,992 | 45,056 | 587 e/s | 270% | clean | "wedge"-shaped but **not a wedge** |
| 25k:5 / 30s (BENCH_RESPONSE_TIMEOUT_SEC=180) | 147,566 | 0 | 2,243 e/s | 490% | clean | HEALTHY |

The "wedge" at 25k reproduced **only** as the bench harness's outer Orleans grain RPC `ResponseTimeout` (default 30s) firing on calls honestly queueing at the G-026 writer admission cap. With a realistic 180s timeout the same rung gave 0 failures and 3.8x throughput. The G-026 admission cap was firing as designed (`wal.writer.append.admission_wait p99 ~2.1s` in the first 10s window); the bench harness's caller-side deadline was just shorter than the realistic worst-case admission wait.

Phase 1 R2 verdict per section 3 decision gate: **<=1/5 wedges across the verification cohorts.** Reliability cycle is closed. Phases 2-3 (bound-by-bound wiring fix and at-saturation matrix) are not required and are dropped.

### 24.1 Code change shipped during closeout

- `TcpIngestService.FlushAsync` (benchmark/azure-throughput/Silo/Program.cs) now emits a named log line on `TimeoutException`:
  > `[silo] grain-rpc-deadline: SetManyAsync of N did not return within ResponseTimeout (BENCH_RESPONSE_TIMEOUT_SEC=Ns). Offered rate exceeds sustained Tables drain rate at this rung; raise BENCH_RESPONSE_TIMEOUT_SEC, drop tickHz/vehicles, or tune WAL fan-out.`
- `IngestSettings` record carries `ResponseTimeoutSec` so the log line can stamp the configured value.
- Cohort runner (`benchmark/vm/run-cohort.ps1`) parses FINAL and surfaces `failed=N` in the summary block so a degraded cohort can't pass for HEALTHY.

### 24.2 Updates to wedge-plan.md

Section 23 added with:
- Findings table above
- Saturation-knobs catalogue (12 levers, default values, what each bounds, when to turn it)
- Failure-mode -> knob mapping
- Carry-forward: re-opening wedge-plan requires evidence triad (stall-watchdog + non-trivial `[wal-slot]` + no `[silo] grain-rpc-deadline` line)

---

## 25. Throughput sweep 2026-06-04 (peak at 4k-6k vehicles; not monotonic above)

Curiosity sweep after Phase 1 closeout: the operator observed higher throughput at 4k than at 25k with the same `BENCH_TICK_HZ=5`. A 1k-step sweep was run to find the peak. Each rung: 30s duration, `BENCH_RESPONSE_TIMEOUT_SEC=180` (so we measure throughput, not the bench harness's deadline). Sweep terminated mid-run by tool crash after rung 9k; sufficient data to draw initial conclusions.

| Vehicles | Active avg (e/s) | Failed | Notes |
|---|---|---|---|
| 4,000 | **15,297** | 0 | (best observed) |
| 5,000 | 5,262 | 12,288 | anomaly - failed=12k despite 180s timeout |
| 6,000 | 15,042 | 0 | recovered, statistically tied with 4k |
| 7,000 | 8,290 | 0 | drop |
| 8,000 | 9,065 | 0 | partial recovery |
| 9,000 | 4,222 | 0 | clear decline |

### 25.1 Findings

1. **Peak throughput plateaus at ~15k e/s active around 4k-6k vehicles.** The two leading cohorts (4k and 6k) are within ~2% of each other and both clean (0 failures).
2. **Performance above 6k is bimodal and noisy.** The 5k cohort had 12k failures despite the lifted grain-RPC deadline - a different failure path than the 25k saturation case. The 7k-9k cohorts are clean but the trend is decline-with-noise rather than a smooth curve. The 5k vs 6k inversion is suspicious enough to flag separately (see 25.2).
3. **Decline by 9k is unambiguous** - throughput at 9k (4,222 e/s) is less than 1/3 of the 4k peak.
4. **The plateau is not CPU-bound.** 4k rung pinned ~2.2 cores out of 8 at peak; 25k pinned ~5 cores. The silo has CPU headroom across the entire sweep range; the throughput limit is elsewhere (likely grain-mailbox queueing, WAL admission concurrency, or Azure Tables per-batch latency floor).

### 25.2 The 5k anomaly

Worth a separate dig. The 5k cohort offered the same effective rate (25k e/s) as the original wedge-plan campaign's most-investigated rung. The `failed=12k` line at 5k indicates **some** batches missed even the 180s grain-RPC deadline - which is different from the 25k saturation case (where 180s caught every batch). Hypotheses, in falsification order:

1. **Activation churn during the cohort start.** The 5k cohort ran ~80s after a fresh silo restart; the 6k cohort right after it was clean. If a transient activation rejection storm hit specifically when 5k's batch fan-out interacted with the placement directory, we'd see `OrleansMessageRejectionException` in the silo log. Cheap to check.
2. **Reshard activity correlation.** `BENCH_SHARD_COUNT` defaults to library (64), so the 5k cohort may have hit a reshard trigger that the others didn't. Check `shard_root.reshard.{initiated,completed,rejected}` counters in the cohort's `[phaseA]` rows.
3. **Random Azure Tables noise.** 5k saw a transient Tables 5xx storm that the 180s timeout couldn't paper over. Less likely given the next cohort (6k) was clean ~90s later, but possible.

### 25.3 Next steps

In priority order:

1. **Reproduce the 4k peak (n=5 cohorts).** Confirm 15k e/s active is the real sustained number for this VM size + WAL config, not a one-shot. Variance band matters for the next phase.
2. **Investigate the 5k anomaly** by re-running 5k once or twice and reading the silo log for the failure cause. If reproducible, it's a real cliff worth understanding. If not, log the noise and move on.
3. **Identify the binding constraint at the plateau.** With CPU at 2-5 cores out of 8 and the WAL provider tail < 250ms, the ~15k e/s ceiling must come from:
   - **Grain mailbox queue depth** at the LatticeGrain (single grain, single mailbox, dispatching SetManyAsync across N keys), OR
   - **WAL writer admission cap** (`WalMaxPendingBatches=8` per partition x 8 partitions = 64 concurrent in-flight batches, each ~4k entries = 256k entries in flight cap; at 15k e/s that's a ~17s drain depth - plausible but not obviously binding), OR
   - **The leaf-grain fan-out** at the BPlusTree level - 4096-entry batches split across thousands of leaf grains, each with its own ack-loop.
   Pick the most likely via `[phaseA]` instrument inspection (especially `wal.append.in_flight`, `wal.append.queue_depth`, `wal.writer.partition.pending_appends`, `wal.writer.append.admission_wait`).
4. **Family A perf knobs at the 4k peak.** Once the bottleneck is named, the levers from `wedge-plan.md` section 23.3 are the next experiments:
   - `BENCH_BATCH_SIZE` up (8192, 16384) - fewer round-trips per offered entry
   - `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS` up (10, 20) - more commits per Tables transaction
   - `BENCH_WAL_PARTITIONS` up (16, 32) - widen the writer fan-out if the admission cap is binding
   - `BENCH_FLUSH_CONCURRENCY` up to match a higher partition count

### 25.4 Carry-forward

- The 4k-6k peak (15k e/s sustained, 0 failures, ~2-5 cores) is the **new performance baseline** for this VM size. Any change that doesn't move this number is not interesting at the per-flush latency layer.
- Renamed to `throughput.md` to reflect the change of cycle from reliability to performance.
