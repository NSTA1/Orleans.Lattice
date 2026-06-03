# wedge-plan.md - residual WAL wedge investigation

> Living document. Tracks the state of the residual phase-1/activation WAL
> wedge investigation on the `fix/wedge` branch. Started as a scratch
> attribution plan; promoted alongside the in-process repro (this folder)
> when the next phase moved from one-shot analysis to incremental
> bisecting of "smallest combination that reproduces". Each new candidate
> condition lands as its own commit in this folder, gated behind a
> console-app argument; this plan records the bisect state.
>
> As of 2026-06-03 ~11:30 UTC (commit `fb4912e` on `fix/wedge`), both
> writer-side mitigation attempts (Options A and B) failed in the same
> way - the cancellation never fires regardless of the underlying
> primitive - and the minimal 4-arm repro does NOT reproduce the wedge.
> The bisect of "what additional condition is needed" is the live
> question this plan tracks from here.

## 1. Reproducibility

Wedge reproduces **deterministically** at 4k vehicles / 5 Hz on the
azure-throughput tier after the ladder hardening:

- 3/3 in the attribution cohort (Option A, `WaitAsync(TimeSpan)`).
- 1/1 in the Option-B sanity run (linked-CTS + `WaitAsync(token)`).

Bimodality was an artifact of leaked `BENCH_TREE_ID` and
`BENCH_WAL_ELIMINATE_CANDIDATE_ROW` env vars, both eliminated by the
hardened ladder. Wedge fingerprint: `inFlight=8` pinned for ~120-130s,
~3000-5000 zero-rate samples per run, no flush/preflight/dispatch counter
trips, ~250-540 `reshard ... REJECTED (Forwarding failed)` lines.

## 2. Diagnostic instruments shipped this cycle

| Commit | What it added | Result in cohort |
|---|---|---|
| `bfdc384` | G-023 pack: `WalAppendDispatchTimeout` (30s), `WalFlushPreflightTimeout` (5s), `WalShardDeactivateInFlight` histogram. Three counters, three new options, validator + resolver + tests + docs. | All three counters at 0. Histogram log markers at 0. |
| `68b0b33` | Silo banner stamps both new option defaults so deployment is self-attesting. | Banner confirmed `walAppendDispatchTimeout=default(30s) walFlushPreflightTimeout=default(5s)` on every subsequent cohort - deployment ambiguity closed. |
| `d0852d7` | `Console.WriteLine` inside both writer dispatch-timeout catch blocks (`[wal-dispatch-timeout]`). | Zero `[wal-dispatch-timeout]` lines in cohort log => catch genuinely never entered. |
| `fb4912e` | Option B: replaced `WaitAsync(TimeSpan)` with `CreateLinkedTokenSource(callerToken) + CancelAfter + WaitAsync(linkedToken)`, also flows the linked token into the grain RPC. New `[wal-dispatch-timeout-cts]` diagnostic. | Zero `[wal-dispatch-timeout-cts]` lines. Same failure shape - linked-CTS pattern also does not fire. |

## 3. What the evidence has eliminated

- **Threading**: ~125 idle threadpool workers + alive TimerThread per snapshot.
- **Threadpool starvation**: see above.
- **Bimodal lottery**: hardened ladder gives 3/3 + 1/1 wedge - deterministic.
- **Tree-id / candidate-row leaks**: ladder env-hygiene block neutralised both.
- **`WaitAsync(TimeSpan)` internal timer plumbing alone**: Option B uses a different primitive (`CancellationTokenSource.CancelAfter` + `WaitAsync(token)`) and exhibits the identical failure.
- **`Task.WaitAsync` defect on this Task shape**: the proxy returns vanilla `Task<T>` via `.AsTask()` on the source-generated `InvokeAsync<T>(...).AsTask()` - confirmed in the emitted `Orleans.Lattice.orleans.g.cs`.
- **`OnDeactivateAsync` reaching user code**: Orleans logs 7 `"Some grains failed to deactivate promptly"` warnings but `WalShardDeactivateInFlight` is never recorded => Orleans calls `OnDeactivateAsync`, but it does not reach my hook (the grain context is also wedged for the deactivation turn).

## 4. The surviving hypothesis - upstream layer

Both Options A and B fail in the **same way**: the cancellation completion needs the grain context to run, regardless of which `WaitAsync` variant is in play. The likely common factor:

**`CancellationToken.Register` callbacks on a token whose CTS lifecycle is owned on a SynchronizationContext-captured frame run synchronously inside `Cancel()`, but the awaiter''s continuation rejoins the captured context.** With `.ConfigureAwait(false)` on the writer's await we deliberately drop the writer's own context capture, but the writer is called from `BPlusLeafGrain.cs:1027` / `AtomicWriteGrain.cs:1235` etc. with **default `ConfigureAwait` (= `true`)**, so the caller (a grain) re-captures the grain context as the resume target for the writer's return Task. If the grain context is wedged, the caller's await of the writer's `Task<long>` never resumes, even if the writer's `WaitAsync` internally fires correctly and throws.

But this would still mean **my counter inside the writer's catch SHOULD have incremented** (the catch runs on the threadpool free of context per `.ConfigureAwait(false)`). It did not. So the cancellation callback in the writer's `WaitAsync` is itself not firing - it appears to require the grain context too, despite `.ConfigureAwait(false)` being on the await line.

This is **at the .NET runtime / Orleans grain-task-proxy boundary**. Source-walking has reached its limit; the next step needs either a minimal repro or an upstream report.

## 5. What this cycle did NOT establish

- Whether `Task.WaitAsync(...)` on a task returned by `Orleans` source-generated `.AsTask()` proxy genuinely cannot be cancelled from the threadpool. A minimal console repro outside Lattice would confirm or falsify.
- Whether Orleans 10.1.0's grain RPC machinery captures the caller's SynchronizationContext into the returned Task in a way that affects `WaitAsync`'s callback dispatch. The 10.1.0 source for `GrainReference.InvokeAsync<T>(...)` and `ResponseCompletionSource` would clarify.
- Whether a third-party diagnostic (eg `dotnet-counters monitor System.Threading.Tasks.TplEventSource` attached to the wedged silo) would surface anything new.

## 6. Repro built and incrementally extended - the wedge does NOT reproduce in isolation

The minimal 4-arm baseline plus five incrementally-added scenarios all fire
their `WaitAsync` deadlines cleanly in ~2 seconds against a 2 second budget.
Each scenario adds exactly one of the candidate conditions identified at the
end of the analysis phase; each commit is one scenario:

| Commit | Scenario | Condition tested | Result |
|---|---|---|---|
| `a86839f` | baseline | 4 arms: (caller in Main vs in grain) x (WaitAsync(TimeSpan) vs linked-CTS) | 4/4 fire in 2005-2020 ms |
| `e103d9e` | load | silo-wide load (32 / 64 / 256 concurrent parked dispatches) | 256/256 fire in 2001-2018 ms |
| `8d82a7e` | singleton | DI-singleton helper hop modelling WalCommitLogWriter exactly (.ConfigureAwait(false), WaitAsync(TimeSpan) inside the singleton) | 32/32 fire in 2007-2021 ms |
| `759056c` | chained | callee internal chained back-pressure modelling WalShardGrain.AppendBatchAsync line 585 exactly (single activation, bounded _inFlight chain, every caller parks at await headTask) | 32/32 fire in 2008-2021 ms |
| `a9b2f92` | churn | activation-churn storm (149,504 self-deactivating ping iterations over 30 s + 32 concurrent parked WaitAsync callers) | 32/32 fire in 2008-2019 ms |
| `c468b4c` | messaging | production-mirror messaging options (ResponseTimeout=180s, explicit ClusterOptions) | 32/32 fire in 2008-2020 ms |
| (combined run) | load,singleton,chained,churn,messaging | all five conditions stacked at load-count=64 | 64/64 per scenario, all fire in ~2 s |

**Every individual condition the analysis phase nominated is falsified. The
five conditions stacked are also falsified.** `Task.WaitAsync` against an
Orleans 10.1.0 grain RPC return Task works correctly under every load,
helper-hop, chained-back-pressure, activation-churn, and messaging-config
shape this repro can construct.

## 7. Decisive conclusion - the wedge is Lattice-specific, not an Orleans-platform issue

The repro has exhausted the candidate-causes catalogue we built from the
ACI cohort''s parked-frame histograms and source walks. None reproduce. The
remaining differences between the repro and the real wedge are:

- **The real Lattice production code path** itself - `BPlusLeafGrain.SetManyAsync`
  -> `WalCommitLogWriter.AppendManyAsync` -> `WalShardGrain.AppendBatchAsync` ->
  `AzureTableWalStorageProvider.AppendBatchAsync` -> per-shard phase-2 chain.
  This is hundreds of lines of intra-grain state machine, leaf-locks,
  splits, registry updates, and the actual Azure-Tables-backed WAL.
- **The real Azure Tables backend** - the WAL provider hits real Azure HTTP,
  with all the SNAT / connection-pool / response-task-completion-source
  interactions that the in-memory repro skips entirely.
- **The `reshard ... REJECTED (Forwarding failed)` storm signal that the
  churn scenario could not reproduce** - the real storm appears to be a
  specific consequence of Lattice''s reshard / placement machinery
  (ShardRootGrain reshard messages) rather than generic Orleans
  deactivation, and the repro''s generic DeactivateOnIdle storm produces
  zero such forwarding rejections.

### What this means for the optimisation track

**Refocus on Lattice, not Orleans.** Filing the wedge at `dotnet/orleans`
without a richer repro would be unproductive - the Orleans primitives
demonstrably work in isolation. The next investigation should be:

1. **An incremental approach FROM the Lattice side**: take the real
   `WalCommitLogWriter` + `WalShardGrain` code path and incrementally
   *strip* it back inside the repro until the wedge disappears. The
   condition removed when the wedge clears is the cause. This is the
   reverse-direction bisect of what this repro did, and is materially more
   work but the only remaining path that does not require ClrMD-level
   instrumentation under live Azure load.
2. **Targeted ClrMD probe under the live wedge** to capture
   SyncBlock / Monitor.Wait ownership data (not just parked async frames
   the StallWatchdog already gives us). This would name the specific
   synchronization primitive that''s not making progress at the wedge
   moment.
3. **Stop here for now**. The wedge is a tracked-known-issue with a
   published, reviewable repro that decisively shows what does NOT cause
   it. Future optimisation cycles can pick up from either (1) or (2) when
   a maintainer chooses to invest more time.

### Bonus finding: ORLEANS0014 analyzer

The repro''s build surfaces an `ORLEANS0014` warning - Orleans 10''s analyzer
flags `ConfigureAwait(false)` in grain code. **The same analyzer should be
firing on `WalCommitLogWriter`''s `.ConfigureAwait(false)` calls.** Either
the analyzer is grain-class-only (and `WalCommitLogWriter` is `internal
sealed class` not derived from `Grain` so it slips through), or the
analyzer is suppressed somewhere in `src/lattice/`. Worth confirming as
part of FX-024 (#573) when that gets implemented.
## 8. Standing carry-forward rule (unchanged)

No throughput A/B at the saturation rung on the azure-throughput tier until
the wedge is resolved upstream. The cohort medians at saturation are confounded
by the wedge until then.

## 9. What landed on `fix/wedge` this cycle (in order)

Production / instrumentation:

```
fb4912e  fix: replace WaitAsync(TimeSpan) with linked-CTS deadline on WAL writer dispatch (Option B, ineffective for the wedge but kept - structurally cleaner than the WaitAsync(TimeSpan) shape and the catch filter is more precise)
d0852d7  diag: log inside writer dispatch-timeout catch (kept - cheap permanent diagnostic for any future wedge cohort)
68b0b33  bench: stamp WalAppendDispatchTimeout / WalFlushPreflightTimeout in silo banner (kept - permanent deployment verification)
bfdc384  fix: bound and attribute the residual phase-1/activation WAL wedge (G-023 pack)
af5db4b  docs: add G-023 (residual WAL wedge diagnostic pack) and dedupe G-022
1cd8d55  docs(bench): caveat vertical-scale.md with wedge-lottery risk and post-#568 residual wedge
afb00b1  docs: record vertical-scaling null result; revert silo to 2 vCPU/4 GiB
02a3ecf  ci: clear leaked per-run BENCH_* overrides at azure-throughput ladder startup
24b8f74  docs: add FX-023 (reshard equal-count no-op) to core features index
650d65f  ci: harden azure-throughput ladder against leaked BENCH_TREE_ID; bump silo to 4 vCPU/8 GiB (silo later reverted to 2/4)
fbb04bd  docs: add FX-024 (ConfigureAwait hygiene on WalCommitLogWriter) to core features index
f74ec86  docs: add G-024 (per-shard FlushAsync / reshard diagnostics pack) to core features index
b603c58  feat: per-shard FlushAsync lifecycle / StartFlush / reshard diagnostics (G-024)
```

Repro + bisect (this folder):

```
a86839f  repro: promote the WAL-wedge reproduction harness from .scratch/ to a tracked top-level repro/ folder (4-arm baseline; does not reproduce)
e103d9e  repro(wedge): add 'load' scenario; silo-wide load alone does NOT reproduce
8d82a7e  repro(wedge): add 'singleton' scenario; singleton-helper hop alone does NOT reproduce
759056c  repro(wedge): add 'chained' scenario; callee internal chained back-pressure does NOT reproduce
a9b2f92  repro(wedge): add 'churn' scenario; activation-rejection storm does NOT reproduce
c468b4c  repro(wedge): add 'messaging' scenario; production messaging options do NOT reproduce
```

Plus issues filed: #570 (FX-023 reshard equal-count), #571 (docs: stale SNAT
narrative in Silo/Program.cs), #572 (G-023 wedge diagnostic pack -
implemented by `bfdc384` above), #573 (FX-024 ConfigureAwait hygiene on
WalCommitLogWriter), #574 (G-024 per-shard lifecycle / reshard diagnostics -
implemented by `b603c58` above).

---

## 10. G-024-driven investigation plan

The G-024 diagnostic pack (`b603c58`) provides three new attribution
surfaces that together can name the wedge mechanism in a single ACI cohort:

1. **D1 - `[wal-slot]` log lines per snapshot** (from the extended
   `StallWatchdog`). Each line names a wedged slot's `(tree, shard)`,
   offset range, current `WalFlushStage`, and stuck-at-stage duration.
2. **D2 - `wal.shard.start_flush.calls` counter + `pending_segments`
   histogram per `(tree, shard)`** (from `WalShardGrain.StartFlush`).
   The counter's rate-during-wedge distinguishes "slot leak" from
   "flush kick-off blocked"; the histogram says whether callers are
   still arriving.
3. **D3 - `shard_root.reshard.{initiated,rejected,completed,in_flight}`
   counters per tree** (from `TreeReshardGrain.ReshardAsync`). Directly
   correlates Lattice-side reshard activity with wedge onset.

### 10.1 Steps

| # | Step | Cost | Decisive |
|---|---|---|---|
| **S1** | Verify the G-024 pack ships and is observable: run one fresh `--scenario baseline` from the upstream repro (sanity that the in-tree build still passes the focused tests after `b603c58`). | seconds | yes for build sanity |
| **S2** | Run one ACI cohort at the saturation rung (`-Rungs '4000:5' -DurationSec 45 -LocalBuild`) on `fix/wedge` HEAD (commit `b603c58`). Three things in scope to verify on the FINAL silo log: (a) banner carries the existing `walAppendDispatchTimeout=default(30s)` token (so we know the silo binary contains the G-023 pack; G-024 is a strict superset of G-023 in this branch so the same token is sufficient deployment verification), (b) the new `[wal-slot]` lines appear in every `[stall-watchdog]` block, (c) the wedge reproduces (it should; G-024 is observation-only). | ~5-10 min wall | yes for instrumentation reach |
| **S3** | Parse the `[wal-slot]` lines from the cohort log: group by `(tree, shard, stage)`, compute the histogram of `stuck` durations per stage. The dominant stage names the binding constraint. | seconds (log parse) | yes for D1 attribution |
| **S4** | Diff `wal.shard.start_flush.calls` counter across the wedge window (~t=10s onwards). Either it keeps incrementing (slot-leak class) or it flatlines (kick-off-blocked class) - that single signal categorises the wedge mechanism. | seconds | yes for D2 attribution |
| **S5** | Diff `shard_root.reshard.initiated`/`rejected`/`completed` counters across the wedge window. Either reshard activity rate is non-zero through the wedge (Lattice-side reshard correlates causally) or it is zero (reshard activity falsified as the wedge cause). | seconds | yes for D3 attribution |
| **S6** | Cross-table the three attribution results into the decision table in section 11. The combination resolves the wedge mechanism to one of ~6 well-defined source regions. | minutes (analysis) | yes |
| **S7** | If a single mechanism is named: file the named code locus as the next `feature-dev` fix issue (e.g. "G-025: bound the X await in WalShardGrain.FlushAsync that observation X named"). If multiple equally-plausible mechanisms remain: extend the repro at `repro/wedge-orleans/` to model the specific named pattern and bisect further (reverse-direction bisect from the Lattice side). | varies | depends on S6 |

### 10.2 Bench harness extension required for S2-S5

The G-024 counters are emitted via `Meter.CreateCounter` /
`Meter.CreateHistogram` on `LatticeMetrics.Meter`. The azure-throughput
silo currently exports the existing meter to an OTel sink configured via
env vars (`benchmark/azure-throughput/Silo/Program.cs`). For S4 and S5
the counter values must reach the operator at cohort-read time. Two paths:

- **(a) Use the existing OTel sink**: configure the sink to scrape every
  counter on `LatticeMetrics.Meter` including the four G-024 counters.
  If the sink is already wildcard-subscribed this is zero work; if it
  uses an explicit allow-list the new metric names need adding. Confirm
  during S1.
- **(b) Add periodic `Console.WriteLine` of counter snapshots in
  `Program.cs`**: a 2-second tick that writes
  `[metric] start_flush.calls=N pending_segments_max=N reshard.initiated=N ...`
  to stdout - reusing the existing per-second reporter shape. Lower
  fidelity but no operator-side OTel config required, and the same
  `.run/silo-*.log` exfiltration channel as `[stall-watchdog]` and
  `[wal-slot]`. **Probably the right call** - matches the existing
  bench-harness style and keeps the diagnostic self-contained per cohort.

This is bench-side harness work, in scope for the optimisation agent. If
the existing OTel sink is already wildcard, path (a) is free; otherwise
path (b) is the right cost-vs-effort trade.

## 11. Decision table for the cohort result

The four observable signals from a single saturation-rung cohort
post-`b603c58`:

| Signal | Possible values |
|---|---|
| **L** - dominant `[wal-slot] stage` | `Created`, `Yielded`, `ProviderCallIssued`, `ProviderCallReturned`, `AcksApplied`, `FailureHandled` |
| **C** - `start_flush.calls` rate during wedge | `>0` (rising) or `0` (flat) |
| **P** - `pending_segments` distribution during wedge | growing (callers still arriving) or flat-at-zero |
| **R** - `reshard.{initiated,rejected,completed}` activity during wedge | non-zero or zero |

The cross-table that names the wedge mechanism:

| L (dominant stage) | C (start_flush rate) | Named mechanism | Next fix region |
|---|---|---|---|
| `Created` | `0` (flat) | Slot stamped `Created` in `StartFlush` but `FlushAsync` body never reached - the task is in `_inFlight` but its `await Task.Yield()` continuation never schedules. | `WalShardGrain.FlushAsync` body before the post-yield check; the in-tree `WalFlushPreflightTimeout` deadline is the right shape but is itself dependent on the same continuation running (see section 6 for the original analysis). Likely needs an out-of-grain watchdog deadline rather than a CTS-based one. |
| `Yielded` | `0` (flat) | Slot passed the preflight check but is stuck in the synchronous setup region between `Yielded` and `ProviderCallIssued` (parallel-array materialisation, provider-call CTS construction). Very narrow - should be microseconds. A dominant `Yielded` means a synchronous `Stopwatch.GetTimestamp` or array allocation is taking many seconds; very unlikely. | Open new investigation if this fires - it would be a real surprise. |
| `ProviderCallIssued` | `0` (flat) | Slot is parked inside the provider call's `await providerCall.WaitAsync(deadline.Token)`. **This is the wedge `WalFlushTimeout` was designed to bound, and it ALSO failed to fire** (separately observable via the existing `wal.append_provider_duration` histogram having a tail above 15s while `flush.fail` is zero). The wedge is in the provider call itself or in `Task.WaitAsync(token)` not honouring the CTS - mirrors the writer-side dispatch-deadline non-firing already documented in section 4. | `AzureTableWalStorageProvider.AppendEncodedBatchAsync` internal behaviour, OR investigate why `WaitAsync(token)` doesn't fire on the provider's `Task` shape. |
| `ProviderCallReturned` | `0` (flat) | Slot returned from the provider but is stuck before `AcksApplied` (the success-path ack-set loop) or before the outer `finally` (slot removal). Narrow region - the only operations between are inside the ack-set loop, which is purely synchronous (`TrySetResult`). Very unlikely. | Open new investigation if this fires. |
| `ProviderCallReturned` or `AcksApplied` | `>0` (rising) | Slot is stamped past the provider call AND new flushes are being kicked off; this means the outer `finally` is removing slots normally but the `_inFlight` chain is filling faster than it drains. **A throughput-imbalance / phase-2 conveyor wedge**, NOT a deadlock. | `WalCommitLogWriter.AppendForPartitionAsync` fan-out concurrency; admit-vs-commit imbalance; consider whether `WalMaxPendingBatches` cap interaction with phase-2 commit latency is the constraint. |
| `Created` | `>0` (rising) | New flushes ARE being kicked off but slots stuck at `Created` never advance. **Slot-leak in the `finally`**: the slot is removed from `_inFlight` but somehow the head of the chain stays at a stale slot pointer. | `WalShardGrain.FlushAsync` outer `finally`, or `_inFlight.Remove(node)` semantics under concurrent enumeration. |
| `FailureHandled` | varies | Slot's failure path completed but the slot is still in `_inFlight`. **Failure-handler bug**: `HandleFlushFailureAsync` completed but the slot was not removed. | `WalShardGrain.HandleFlushFailureAsync` and the matching `finally` block. |
| any | any | `R` non-zero through the wedge window AND `start_flush.calls` correlates with reshard activity | Reshard activity is causally implicated (not just the `reshard ... REJECTED` log storm). | `TreeReshardGrain` swap-phase interaction with `WalShardGrain` activation lifecycle. |

The decision tree is intentionally exhaustive across the 6 stages. The
expected outcome is `ProviderCallIssued + flat C + zero R` (the
provider-await is the wedge, reshard correlation only) - if that holds,
the next investigation locus is concrete and small. Any other outcome
materially redirects the investigation.

### 11.1 Carry-forward for the next cycle

The bisect plan in section 10 + the decision table in section 11 carry
the investigation forward. The standing carry-forward rule in section 8
remains in force: no throughput A/B at the saturation rung until the
wedge mechanism is named via this attribution. The G-024 pack is the
mechanism that lifts that rule.

---

## 12. Cohort attempt of 2026-06-03 ~12:00 UTC - bimodal at the saturation rung

The S2 cohort prescribed in section 10 was attempted; the second of three planned runs was **terminated by an in-tool cancellation** and per the operator's standing instruction (treat cancellation as terminal) the third was not started.

### Run 1 (silo-20260603-115719Z.log)

`HEAD = 5ec4459` (b603c58 G-024 pack + 5ec4459 phaseA allowlist). Command:

```
./benchmark/azure-throughput/scripts/40-ladder.ps1 -Rungs '4000:5' -DurationSec 45 -LocalBuild
```

**Outcome: HEALTHY.** First non-wedged 4k cohort in the investigation.

- `FINAL written=723,964 failed=0 elapsed=112.6s Entries written per second (avg)=6,430`.
- Peak rate 20,476 e/s (hit the 20 k/s offered target).
- 51 "wedge samples" all clustered at t=3.1s startup (snapshot artefact); silo drained to `inFlight=0` by t=70s.
- Producer terminated cleanly (`[producer] DONE total=728,000 elapsed=45.1s`); no `Broken pipe`.
- G-024 instrumentation behaved as designed: `wal.shard.start_flush.calls` rose steadily per shard (57 -> 233 -> 641 -> 713 -> 498 -> 243), `wal.shard.pending_segments` peak `min=1 p50=12 p90=100 max=100`, reshards 51 initiated / 51 completed / 0 rejected, `reshard.in_flight` always 0.
- `[stall-watchdog]` and `[wal-slot]` never fired (correct - the watchdog only triggers under a stall).

### Run 2 (orphan; ACI silo retrieved via `az container logs`)

`HEAD` unchanged. Command was the same with `-SkipBuild`. The deploy script was cancelled mid-bounded-wait; the silo + producer kept running in ACI until `az container stop`. The recoverable tail (`az container logs` returns ~62 lines of buffer; no FINAL, no `[stall-watchdog]`):

```
[silo] t=  62.6s written=     48,269 Entries written per second=         0 inFlight=  8
...
[silo] t=  78.6s written=     48,269 Entries written per second=         0 inFlight=  8
```

**Outcome: WEDGED.** The wedge fingerprint (`inFlight=8` pinned, `rate=0`) is present and sustained for at least 17 s before manual stop.

### Run 3

Not executed (per the standing cancellation = terminal rule).

### Verdict

1/2 healthy at the saturation rung post-G-024. The "wedge always reproduces deterministically" claim from section 6 is **falsified by this cohort attempt**: the wedge is **bimodal again**, not deterministic. Prior cohorts had run 4/4 wedged, then 1/1 wedged (cohort 5/5 wedged total) - this cycle adds 1 healthy + 1 wedged, bringing the post-hardened-ladder tally to 6/7 wedged, 1/7 healthy. The bimodality apparently never went away; the "deterministic" claim was a small-sample artefact of three consecutive wedged runs in a row.

### Implications for the investigation

1. **G-024 is NOT a wedge fix.** The instrumentation is observation-only; the one healthy cohort is consistent with the pre-existing ~14% healthy rate (1/7) of saturation-rung cohorts. The change is not statistically distinguishable from no change at this sample size.
2. **The G-024 attribution path remains viable but requires a wedged cohort.** The healthy cohort produced no `[wal-slot]` lines because the watchdog never triggered (correct), so the section 11 decision table still has not been exercised against the wedge.
3. **The investigation now needs cohort discipline** beyond what this cycle has time for: at the empirically observed ~85 % wedge rate, attributing the wedge requires running enough cohorts that at least one wedges AND completes its watchdog window. A single 4k rung that wedges to FINAL + watchdog fires is the minimum useful diagnostic sample.

### Carry-forward for the next cycle

- The G-024 pack ships and is verified observable (`[phaseA]` rows confirmed for all 7 instruments; `[wal-slot]` plumbing verified in source but unexercised in the cohort because the wedge did not reproduce on the cohort that ran to FINAL).
- The next investigation cycle should run the S2 cohort with **n>=3 to ensure at least one wedged run reaches FINAL**, with the wall-clock cap raised to let the StallWatchdog fire on the wedged sample (default trigger is throughput-stall-based; a wedged run should fire it within ~90 s of the stall starting).
- Section 11 decision table is unchanged and stands ready for the next attribution attempt.
- The original "wedge is now deterministic" claim in section 6 should be read as "wedge is bimodal with a low healthy rate, not strictly deterministic, but the wedged-run signal is reproducible" - the bimodality re-emerging here doesn't change the section 7 decisive conclusion (wedge is Lattice-specific).

---

## 13. Cohort attempt 2026-06-03 (G-024 ship + StallWatchdog rework, branch `fix/wedge`)

This section supersedes section 12. The "Run 3 not executed" claim there was correct at the time of writing but is now stale: n=7 cohorts were ultimately executed under the cancellation = retry standing rule. Full audit trail in `benchmark/.run/azure-throughput/POSTMORTEM-2026-06-03-g024-stallwatchdog-rework-and-bimodality.md`.

### Cohort tally (rung `4000:5`, `DurationSec=45`, `-LocalBuild` per run)

| # | Log | Outcome | Steady avg | Written | Mode | Notes |
|---|---|---|---|---|---|---|
| 1 | 115719Z | HEALTHY | 6,430/s | 723,964 | n/a | watchdog never fired (correct) |
| 2 | 123606Z | WEDGED | 221/s | 31,057 | B (inferred from dumpasync; pre-watchdog-fix) | 0 `[wal-slot-grain]` rows (placement bug) |
| 3 | 124016Z | WEDGED | ~0/s | 0 | B (inferred) | 0 rows (placement bug) |
| 4 | 130520Z | WEDGED | 1,283/s | 45,158 | B | 0 rows (post `aeb1b0d` but pre `1624183`) |
| 5 | 132021Z | WEDGED | 1,035/s | 72,670 | **A** | 328 rows `count=1, headNull=False`; one persistent slot per shard |
| 6 | 132807Z | WEDGED | 399/s | 17,012 | B | 441 rows `count=0, headNull=True` |
| 7 | 133637Z | WEDGED | 620/s | 44,254 | B | 306 rows `count=0, headNull=True` |

**Final tally: 1 healthy / 1 Mode A / 5 Mode B.** Mode B is ~71 % of wedged cohorts and is the dominant target for the next cycle.

### Watchdog rework (the durable artefact of this cycle)

The shipped `[wal-slot-grain]` row is the **Mode A vs Mode B classifier** that surfaced the bimodality. Three sequential bugs in the watchdog hid this until commit `1624183`:

1. `b603c58` - literal nested-type-name match for `InFlightFlush` detection. Fixed by field-signature detection in `aeb1b0d`.
2. `aeb1b0d` - `[wal-slot-grain]` emit placed AFTER the `head.IsNull` early return. Fixed in `1624183` by moving the emit before the return.
3. `45e147b` - `[wal-slot-probe]` emit gated by the same early return path. Untested in Mode A (no Mode A cohort reproduced after the probe shipped); observation-only and safe.

Each bug silently failed closed: zero `[wal-slot]` rows on every wedged cohort, no exception trace, no other tell. **Pattern for future watchdog work:** any diagnostic emit intended to be unconditional must precede ALL state-conditional early returns.

## 14. Mode-aware decision table (supersedes section 11)

Section 11's single-axis decision table was written before bimodality was known. It assumed every wedged cohort would have one dominant `[wal-slot]` stage L. That assumption is invalidated by Mode B, where `_inFlight` is empty and there is no L to read.

The next cycle should walk this table:

| `[wal-slot-grain]` dominant tuple | Top async-stall frame | Mode | Implicated layer | Diagnostic pack |
|---|---|---|---|---|
| `count>=1, headNull=False` | `WalShardGrain.AppendBatchAsync await#0` | A | `WalShardGrain.FlushAsync` lifecycle | shard-layer pack (this cycle's `b603c58`) - read dominant stage from `[wal-slot]` and walk it against the per-stage rows in section 11 |
| `count=0, headNull=True` | `WalCommitLogWriter.AppendForPartitionAsync await#0` | B | `WalCommitLogWriter` per-partition append plumbing | NOT INSTRUMENTED in this cycle - file the next-cycle pack (issue #575) and reproduce |
| `count=0, headNull=True` | something other than `WalCommitLogWriter` | C (unknown) | new bisect target | dumpasync top frame names the next instrumentation site |

### Mode A sub-table (unchanged, retained for reference)

The original section 11 stage rows still apply when `[wal-slot]` actually produces a stage observation. Walk that exactly as documented in section 11.

### Mode B sub-table (placeholder until the next cycle ships)

To be filled in once issue #575 instruments `WalCommitLogWriter.AppendForPartitionAsync` with `Enqueued / DequeuedForBatch / SentToShard / Acked / Failed` lifecycle stamps and the next cohort produces `[wal-append]` rows. The expected mapping (to be empirically confirmed):

| Dominant `[wal-append]` stage | Likely mechanism |
|---|---|
| `Enqueued` (entries pile up before any dequeue) | batcher loop never wakes - check batcher task aliveness, `flushMs` timer health |
| `DequeuedForBatch` (entries dequeued but never forwarded) | dispatch-to-shard call wedges - probable shard-grain reentrancy or scheduler starvation |
| `SentToShard` (forwarded but no ack) | wedge collapses back into Mode A territory - re-read the shard-layer pack |
| `Failed` | error-path stall - check failure handler instrumentation |

## 15. Next cycle entry point

- Issue #575 (G-025): `WalCommitLogWriter.AppendForPartitionAsync` instrumentation pack. Shape mirrors the shard-layer pack: lifecycle enum + per-pending stamp + queue-depth histogram + dispatch counter + `[wal-append]` watchdog walker.
- After G-025 ships, run an n=3 cohort at the same rung. Expect `[wal-append]` rows on at least one Mode B wedged run; walk the dominant stage through section 14's Mode B sub-table.
