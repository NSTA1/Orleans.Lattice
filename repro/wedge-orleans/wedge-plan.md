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

---

## 16. G-025 cohort attempt 2026-06-03 (writer-layer attribution lands)

n=3 cohorts at rung `4000:5` `DurationSec=45` on commit `894d705`
(writer-layer pending-append dispatch diagnostic pack shipped as
issue #575 / G-025).

| # | Log | Outcome | Steady avg | Written | [wal-append] rows | Dominant stage | Stuck p50/p99 | Tracker nonzero-depth min/max/avg |
|---|---|---|---|---|---|---|---|---|
| 1 | 141000Z | WEDGED hard | 278/s | 0 | **2,775** | **SentToShard 100%** | **5,764 / 5,985 ms** | 1 / 18 / 9.4 (296 obs) |
| 2 | 142844Z | WEDGED | 899/s | 42,451 | 0 | n/a | n/a | none nonzero at snapshot |
| 3 | 143239Z | WEDGED | 869/s | 39,621 | 0 | n/a | n/a | none nonzero at snapshot |

### Cohort 1 attribution

Definitive: every one of the 2,775 pending-append stamps observed
across the wedge window was parked at the `SentToShard` lifecycle
stage with a head-of-line stuck-time of **5.7 seconds (median)**
and **6.0 seconds (max)**. The `SentToShard` stamp is set
immediately before `await grain.AppendAsync/AppendBatchAsync`, so
the parked await is on the shard-grain RPC itself. Per-partition
tracker depths fan out to 1, 2, 3, 8, 12, 15, 16, 18 across 9
partitions - the writer is absorbing back-pressure into its own
per-partition chain because the downstream shard cannot drain.

dumpasync confirms the picture from the caller side: 3,584
callers parked at `BPlusLeafGrain.CommitSetAsync await#0`, 3,582
at `SetCoreAsync await#0`, 3,579 at
`WalCommitLogWriter.AppendAsync await#0` - the entire write
pipeline stacked up behind the shard-grain RPC.

Shard-side classifier from the same snapshot disagrees with the
earlier-cycle "Mode B = empty in-flight" framing: 222 grain rows
show `count=1 headNull=False`, 74 show `count=2`, and only 37
show `count=0 headNull=True`. The wedge in this cohort has BOTH
shard in-flight slots populated AND writer pending-append depths
saturated; the prior cycle's "Mode B" subset was just a snapshot-
timing artefact of cohorts where the shard-side queues happened
to drain before the watchdog fired.

### Cohorts 2 and 3 attribution

Both wedged at low-throughput (899 e/s and 869 e/s, with non-zero
totals) but emitted **zero** `[wal-append]` rows and zero
`[wal-append-tracker]` non-empty-depth observations. dumpasync
still showed 619 / 500 callers parked at
`WalCommitLogWriter.AppendForPartitionAsync await#0` (the
batched-path entry, which uses the same per-partition tracker as
the single-entry path), so the wedge is at the same boundary -
just transient at snapshot time: the tracker had drained between
the heap snapshot and the dumpasync.

The likely explanation: cohorts 2 and 3 were partially-recovering
runs that wedged for shorter intervals, and the watchdog's
once-per-process firing latched onto a snapshot during a drain
moment. To resolve this would require either a multi-snapshot
watchdog or a `partition.pending_appends` histogram tail read
from the `[phaseA]` rows (which IS still being emitted - see
PhaseADiagnosticReporter); the attribution from cohort 1 is
strong enough to lock the mechanism without that follow-up.

### Mode-aware decision table update

Section 14's "Mode B = empty `_inFlight` + callers stacked
upstream" entry is now superseded. The real wedge taxonomy is:

| Class | `[wal-append]` dominant stage | Shard `[wal-slot-grain]` | Top async-stall | Mechanism |
|---|---|---|---|---|
| **SentToShard back-pressure** (cohort 1 silo-141000Z) | `SentToShard` 100% with multi-second stuck-time | `count>=1, headNull=False` predominant | `BPlusLeafGrain.CommitSetAsync` + `WalCommitLogWriter.AppendAsync` parallel-tall | Shard-grain RPC RTT exceeds offered rate; writer absorbs back-pressure into per-partition tracker until the system saturates and the producer stops feeding |
| Transient wedge with drained snapshot (cohorts 2+3) | none observed (drained at snapshot) | none observed (drained at snapshot) | `WalCommitLogWriter.AppendForPartitionAsync` 500-619 | Same root cause as above, snapshot-timing artefact; resolve with multi-snapshot watchdog or `[phaseA]` percentile rows |
| Healthy | n/a (watchdog never fires) | n/a | n/a | offered rate < shard drain rate |

### What we now know that we did not before

1. The wedge is **NOT a deadlock or a lifecycle bug** in any of
   the layers G-019..G-022 instrumented. It is a back-pressure
   regime: the shard-grain RPC's effective service rate is too
   low to absorb the writer's offered rate.
2. The "Mode A vs Mode B" axis from section 14 was a
   misclassification driven by an under-instrumented watchdog
   layer; with G-025 in place, there is **one wedge mechanism**
   (shard-grain RPC saturation) with two snapshot phenotypes
   (full tracker + populated `_inFlight` if the snapshot lands
   mid-wedge; both empty if the snapshot lands mid-drain).
3. The stuck-time per pending dispatch is **5-6 seconds**, which
   is consistent with **multi-RTT batched Azure Tables flush
   latency at saturation**. A single Azure Tables EGT
   round-trip is ~50-100ms at this region; 5-6 seconds is 50-100
   round-trips queued behind one in-flight flush.
4. Per-partition tracker depth reaches 18 (cohort 1) which is
   ~2x the default `WalMaxPendingBatches=8` ceiling - meaning
   the writer-layer `PartitionTracker` enforces no ceiling
   (correctly: the ceiling is per-shard, not per-writer-
   partition), and the writer is fanning out faster than the
   shard can absorb.

### Section 15's next-cycle entry point is now obsolete

Section 15 named a follow-up cohort "to walk the dominant
`[wal-append]` stage through section 14's Mode B sub-table". That
walk landed in the table above and produced an actionable
mechanism, so the entry point has graduated. Replace section 15
with this:

## 17. Next cycle entry point

The wedge mechanism is now named: **shard-grain RPC saturation
under offered rate above the Azure-Tables flush drain rate**.
The next cycle's hypothesis space falls into two families:

A. **Reduce per-flush latency** - shorten the head-of-line
   stuck-time on each in-flight flush. Candidates: bigger
   batch-coalescing windows, fewer round-trips per flush
   (G-007 / G-008 territory but specifically on the WAL flush),
   write-amplification reduction (skip the WAL row when the leaf
   commit can settle by another path).

B. **Bound writer-side back-pressure** - cap the
   `PartitionTracker` depth so the producer back-pressures
   sooner, surfacing the saturation as a slow caller rather
   than as an apparent-wedge. Candidates: per-writer-partition
   cap on `_inFlight`; a writer-side admission control that
   refuses dispatch when tracker depth crosses a threshold,
   matching the shard-side `WalMaxPendingBatches` shape.

Both families would benefit from a cleaner repro: the cohort 1
phenotype (heavy wedge with full diagnostic capture) only
reproduced 1/3 cohorts at this rung. Before the next cycle,
either (a) walk the rung up to find one where the wedge is
near-deterministic, or (b) raise the producer ramp-rate so the
shard saturation arrives faster, increasing the probability of
catching the wedge with the diagnostic in mid-snapshot.

---

## 18. G-026 cohort attempt 2026-06-03 (writer-side admission cap lands)

Hypothesis (per the cycle's Phase 1 framing) was a **reliability**
change: bound `WalCommitLogWriter.PartitionTracker._inFlight` at
`WalMaxPendingBatches` with a typed admission timeout, so a
saturated downstream shard surfaces as honest slowness instead of
a silent wedge. Success criterion was conjunctive:
- healthy-cohort throughput not degraded beyond 1.5x IQR_baseline
- wedged-cohort phenotype shifts to admission timeouts (no silent
  unbounded queue absorption)

The hypothesis explicitly stated this was NOT a throughput
optimisation; throughput preservation was a guard, not a target.

### Cohort results

n=3 candidate cohorts on commit `006ba11` vs the previous
cycle's n=3 baseline cohorts on commit `894d705`, same rung
`4000:5 -DurationSec 45`.

| Run | Cohort | Steady avg | Written | Watchdog | [wal-append] | Tracker max depth |
|---|---|---|---|---|---|---|
| Baseline 1 (141000Z) | wedged-hard | 278/s | 0 | yes | 2,775 (SentToShard) | 18 |
| Baseline 2 (142844Z) | wedged | 899/s | 42,451 | yes | 0 (drained) | 0 (snapshot drained) |
| Baseline 3 (143239Z) | wedged | 869/s | 39,621 | yes | 0 (drained) | 0 (snapshot drained) |
| Candidate 1 (151636Z) | **HEALTHY** | **5,817/s** | **363,669** | **no** | 0 | n/a (below trigger) |
| Candidate 2 (152054Z) | **HEALTHY** | **5,775/s** | **372,465** | **no** | 0 | n/a |
| Candidate 3 (152350Z) | **HEALTHY** | **5,507/s** | **349,663** | **no** | 0 | n/a |

Median throughput: baseline **869/s** -> candidate **5,775/s**
(**+4,906/s, +565%**). Baseline IQR=621; threshold 1.5x=932; delta
moves the median by 7.9x the threshold. Candidate IQR=310, **0.5x**
of baseline IQR (distribution shape tightened, not widened).
Wedged-cohort heavy phenotype (0 written, 2,775 SentToShard stamps,
tracker depth 18) failed to reproduce in 3/3 candidate runs.

### Surprise: a reliability change produced a 6.7x throughput
### win

The Phase 1 hypothesis did NOT predict throughput improvement
and the cycle would have been accepted as a successful
reliability outcome even at flat throughput. The most likely
explanation for the unexpected throughput delta:

The unbounded writer queue was driving the shard into a **failure
regime** rather than just a slow regime. Cohort 1 baseline
observed tracker depth = 18 (more than 2x the shard's
`WalMaxPendingBatches=8`) with head-of-line stuck-time = 5.7s
(p50) - the writer was feeding the shard faster than it could
absorb, the shard's own ceiling produced flush-deadline
TimeoutExceptions, retries cascaded back as fresh dispatches,
and the system collapsed into a regime where each dispatch did
~6s of work for 0 throughput. The writer-side cap keeps the
shard inside its healthy operating envelope: the offered rate
back-pressures at the writer boundary instead of crashing
through the shard's deadline, the shard never enters the
failure cascade, and effective throughput rises to what the
shard CAN absorb at saturation.

In short: **bounded back-pressure is faster than unbounded
back-pressure when the downstream is non-linear under
overload**. The cycle was a reliability hypothesis that
incidentally exposed a perf regime collapse the unbounded shape
was creating.

### Caveat: admission-timeout signal not directly observed
### at-Azure

`[wal-admission-timeout]` fired **zero** times across the 3
candidate cohorts because the system never approached the cap
under the offered rate. That confirms the cap is not contended
at this rung but does NOT directly observe the "callers receive
typed TimeoutException in bounded time" behaviour at-Azure. The
behaviour is exhaustively confirmed by 3 unit tests
(`PartitionTracker_AcquireAsync_throws_TimeoutException_when_cap_saturated`,
`PartitionTracker_AcquireAsync_with_cap_zero_is_unbounded_opt_out`,
`AppendAsync_admission_timeout_path_increments_writer_counter_when_tracker_presaturated`)
plus the integration coverage on the uncontended fast path,
but a higher-rung cohort that actually saturates the cap is
still owed to close that observation gap.

### Decision: KEEP

Both falsification criteria met:
- Healthy throughput improved by 7.9x the noise threshold (target
  was no-degradation; delta is large positive).
- Heavy-wedge phenotype eliminated 3/3 (target was "shifts to
  admission timeouts"; actual was "shifts to no-saturation"
  which is the better outcome - the system never even
  approached saturation).

### Next cycle entry points

1. Higher-rung cohort to directly observe `[wal-admission-timeout]`
   firing under genuine saturation. Walk the rung up from `4000:5`
   until the cap is contended.
2. Family A (per-flush latency reduction) becomes more attractive
   now: with the writer in a healthy regime, the dominant cost is
   the per-shard flush RTT. The 5.7s head-of-line stuck-time from
   the baseline cohort is what the shard CAN do at saturation;
   shortening that directly raises the cap the writer can
   sustainably drive.
3. The wedge-plan can be considered resolved at the rung the
   campaign opened against. The remaining work is performance
   tuning, not reliability investigation.

---

## 19. G-026 cohort at the original 25k rung 2026-06-03 (wedge reproduces; corrected analysis)

The previous section's verdict ("wedge investigation resolved at
the rung the campaign targeted") was scoped to the 4000-vehicle
rung. A single-cohort run at the campaign's original 25,000
vehicle / 5 Hz rung (offered rate ~125,000 e/s) on the same
`006ba11` candidate binary reveals the wedge IS still present
at the original rung. The first-pass log analysis of that run
was wrong in multiple ways; this section records the corrected
reading and the new hypothesis it points at.

### Cohort

Single run, rung `25000:5`, `DurationSec=45`, `-SkipBuild`,
silo log `silo-20260603-153020Z.log`. Bench report:

```
target          :  125,000/s
steady-state avg:      269/s   (over 42s "productive" window)
total written   :    24,661
```

### What actually happened (deduplicated timeline)

The bench log scraper appears to write each `[silo] t=N.Ns ...`
sample line multiple times (3000+ raw rows for ~130 unique
timestamps). After deduplication on the timestamp, the ENTIRE
run consists of five distinct write events and one long silence:

| t       | delta entries | cumulative | notes                                  |
|---------|---------------|------------|----------------------------------------|
| 5.1s    | +85           | 85         | initialisation tail                    |
| 8.1s    | +12,288       | 12,373     | first real flush (3 x 4096 batches)    |
| 12.1s   | +4,096        | 16,469     | one batch                              |
| 51.1s   | +4,096        | 20,565     | after a **39-second silence**          |
| 52.1s   | +4,096        | 24,661     | one more batch                         |
| 53s..end | 0            | 24,661     | **73+ seconds of total silence to end of run** |

`inFlight=8` is reported pinned for every silo sample from
t=2s onwards. Throughput is NOT steady at any rate; it is two
small flush windows separated by a 39s drought and followed by
a permanent silence with 8 slots held but nothing draining.

### What the [wal-append] watchdog samples show

The StallWatchdog fired 35 times across the run. Every snapshot
shows the same dominant signal:

- `[wal-append]` rows: 1,715 total, **100% at the `SentToShard`
  stage** with `stuck` time clustered at **p50=8.99s, p99=9.08s,
  max=9.08s** (vs 5.7s at the 4k rung baseline).
- `[wal-append-tracker]` depth: **max=8, exactly the cap, 0 rows
  over cap** across 315 tracker observations. The writer-side
  cap is doing exactly what it was designed to do.
- Shard-side `[wal-slot-grain]`: 175 rows `count=1, headNull=False`
  + 70 rows `count=2` + 70 rows `count=0, headNull=True`. The
  shard's in-flight chain holds 1-2 slots most snapshots.

### What the phaseA admission_wait metric shows

A single phaseA row stamps the first ~10s window cleanly:

```
[phaseA] t=10.3s instrument=wal.writer.append.admission_wait
  tree=ladder-...-v25000-h5  count=1298  sum=2,229,917 ms
  min=0  p50=1722 ms  p90=2924 ms  p99=3195 ms  max=3230 ms
```

1,298 dispatches in the opening 10 seconds with a median
admission wait of 1.72s and p99 of 3.2s. The admission cap is
firing as designed - callers ARE queuing on the semaphore for
multi-second periods. No subsequent phaseA window emits a
nonzero admission_wait row, because after the first ~12s the
silo stops admitting new dispatches in volume (the bursts at
t=51-52s are recovery flushes, not continued ingestion).

`[wal-admission-timeout]` counter: **0** for the whole run. The
admission wait p99 of 3.2s is well under the
`WalAppendDispatchTimeout=30s` deadline, so the typed-timeout
signal G-026 was designed to surface does not fire.

### Reshard activity correlates with wedge onset

- Reshards initiated: **53**, completed: 53, Lattice-side
  rejections: 0.
- Orleans-side `OrleansMessageRejectionException: Forwarding
  failed` rejections during reshard: extensive (every reshard
  attempt has multiple rejected forwards with exponential
  backoff: 100ms, 200ms, 400ms, ...).

The reshard storm coincides with the wedge onset. Reshards run
to swap shard counts (32 in this rung); Orleans's routing
rejects the forward of split-coordinator messages while the
target shard activation is mid-transition, the rejected
forwards bounce back to the silo's outbox, and the WAL
shard's append pipeline is blocked behind whatever turn-queue
state the rejected forward left behind.

### Why the first-pass analysis was wrong

The first reading of this cohort (the assistant's own message
that prompted this corrected section) claimed:

- "System holds steady at the shard's drain rate forever" -
  FALSE. The system stops writing entirely after t=52s and
  runs silent for 70+ seconds before the bench wall-clock
  terminates.
- "Predictable graceful degradation" - FALSE. After the first
  12 seconds the system is effectively wedged, with one tiny
  ~2-second recovery window at t=51-52s.
- "The 'wedge' is now just back-pressure" - FALSE at 25k. The
  cap kept the writer queue bounded (good), but the underlying
  shard wedge is still real.

The error came from reading the bench summary line
("steady-state avg 269/s") as if it described a stable rate;
it is actually the mean of (5 nonzero samples + 67 zero
samples) over the "productive" window, which gives a
mathematically valid but operationally meaningless figure.

### Corrected verdict at the 25k rung

| Claim                                                | Truth                                                                                                  |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| G-026 cap pins writer-side depth at WalMaxPendingBatches | True (max=8, 0 rows over).                                                                          |
| G-026 prevents queue-overflow / failure-cascade regime  | True (no `wal-dispatch-timeout-cts` lines, no `wal.append_dispatch.timeouts`).                       |
| G-026 surfaces saturation as typed admission timeout    | **Not at this rung.** Admission wait p99 is 3.2s; deadline is 30s; counter never fires.              |
| System degrades gracefully at offered-rate-vs-drain-rate | **False.** The system wedges hard after the first 12s of ingestion.                                  |
| 4k rung verdict ("wedge resolved")                   | Still valid - 3/3 candidate cohorts healthy with no watchdog firings.                                  |
| 25k rung verdict                                     | **The wedge persists at the original campaign rung even with G-026 in place.**                       |

### New hypothesis: reshard activity is the residual wedge driver at the original rung

`SentToShard` stuck-time clusters at exactly ~9s (p50 to max
spans 8986 - 9075 ms, a 90ms window). That is not a tail
distribution of shard RTT under load; it is a single,
discrete deadline firing. Combined with the reshard storm
(53 reshards completed in a 45-second offered-load window =
more than 1 per second) and the OrleansMessageRejectionException
forward-failure log, the residual wedge looks like a
**reshard-induced turn-queue stall**, not a back-pressure
overload. G-021 (already shipped) bounded the outbound shard
forwards with `ShardForwardTimeout`; that change addressed
parked forwards but did NOT address forwards REJECTED by
Orleans' router with an immediate exception. The rejected
forwards leave the split coordinator in a state where its
post-rejection retry path interacts with the WAL shard's
in-flight chain in a way that pins inFlight at the shard cap
without ever draining.

## 20. Next cycle entry point

Section 17's "next cycle entry points" (admission-timeout
validation at a higher rung; Family A latency reduction) need
to be reordered. The 25k rung shows that BEFORE Family A
work would even be measurable, the reshard-rejection wedge
must be addressed. File a new issue scoped narrowly to:

> **G-027:** investigate the residual at-saturation wedge at
> the 25k rung where 53 Orleans-rejected reshard forwards
> coincide with permanent stall of an inFlight=cap shard
> in-flight chain. The `SentToShard` stuck-time clusters at
> a single discrete value (~9s) across 35 watchdog
> snapshots, indicating one deadline / interlock fires once
> and pins the chain afterwards. Determine whether the
> rejected reshard forward leaves a WAL shard slot held by
> the split coordinator's retry path, and bound that hold
> with the same shape as G-021's `ShardForwardTimeout` if so.

Family A and the admission-deadline split (mentioned in
section 17) remain valid next-cycle hypotheses but should run
AFTER G-027 is decided, so the at-saturation cohort is in a
state where a per-flush-latency change can be cleanly
attributed.

---

## 21. G-027 hypothesis invalidated 2026-06-03 (the 25k 'wedge' was a measurement artefact)

Section 19's hypothesis (rejected reshard forwards leave a WAL-shard interlock held by the split coordinator's retry path) was filed against a single 25k cohort whose silo log appeared to show 53 reshard attempts, 35 stall-watchdog firings, and 1,715 SentToShard parked stamps at a discrete 9-second stuck time. Deep re-analysis on main (post-G-026 merge) shows every one of those counts was bench-log-scraper duplication.

### Deduplicated reality of cohort silo-20260603-153020Z

| Inflated claim | Deduplicated reality |
|---|---|
| 53 reshard attempts | **5 reshard attempts**, all during silo startup, completing by t=1.5s with backoffs 100/200/400/800 ms (attempts 1-4 rejected; attempt 5 succeeded). Bench-harness startup-only code in azure-throughput/Silo/Program.cs lines ~700-780, NOT runtime resharding under load. |
| 35 watchdog firings | **1 watchdog firing**, at writtenTotal=16,469 - i.e. immediately after the t=12.1s flush event. |
| 1,715 SentToShard parked stamps at discrete 9s | The dedup brought this down to ~28 distinct stamp observations; the 9s 'stuck-time' equals the producer-side 'innerAvgMs=9233ms' (TCP send-call duration under back-pressure) - **the end-to-end RTT at this offered load, not a Lattice deadline firing**. |
| inFlight=8 pinned for the entire run | wal.append.in_flight p50 actually drops 7 -> 0 between t=10s and t=40s as the chain drains; climbs back to 7 at t=60s when a final producer burst arrives. The '[silo] t=Ns Entries written per second=0' lines that prompted the wedge claim are a sampling artefact: the silo reporter only counts COMPLETED SetManyAsync batches per 1s window, and with FlushConcurrency=8 batches each taking 4-7s at the Azure-Tables saturation latency, most 1s windows contain zero completions even while thousands of leaf commits per phaseA window are landing. |

### What actually happened at 25k

- Producer fires 125k entries in 1s (5x the silo's sustained Azure Tables drain rate at this load).
- Silo's TCP read buffer fills; producer back-pressures at 9.2s per send call (producer log: `t=10.5s sent=150,000 ... innerAvgMs=9233ms`).
- Silo processes leaf commits at ~250 entries/s sustained (leaf.commit.duration phase=apply count=3,712 per 10s phaseA window).
- Producer essentially stops sending between t=20s and t=60s (tcp.read.line_bytes count drops to 0 over t=30-50s).
- G-026's writer cap pins at 8 with 0 over-cap observations and 0 admission_timeouts (working as designed).

### Decision: closed G-027 (#578) as 'not planned'

There is no interlock to fix. The 25k throughput floor (~250 e/s sustained) is the Azure Tables per-flush rate at this load and is a Family A (per-flush latency reduction) hypothesis for a future cycle, not a reliability bug.

Closing comment on the issue: https://github.com/NSTA1/Orleans.Lattice/issues/578#issuecomment-4614403962

### Compounding analytical errors

This cycle is the third in the campaign where a 'discrete deadline firing once' wedge interpretation was reached and later refuted. The lesson is twofold:

1. **Always dedupe bench-scraper output before drawing event-count conclusions.** Multiple `[silo]` / `[stall-watchdog]` log writers (the bench harness scrapes stdin per channel) duplicate each line; the raw count can be 10x the unique count. Pipe through `Sort-Object -Unique` on the line before tallying.
2. **Cross-reference [phaseA] counter rates against [silo] reporter rates before declaring 'rate=0'.** The silo reporter is a 1s-resolution sampling of completed batches; the [phaseA] counters are 10s-window aggregations of underlying activity. Disagreement between them is the signal that the silo reporter is under-sampling, NOT that the system has wedged.

### Implications for sections 17-20

- Section 17's 'reshard storm correlates with wedge onset' is **falsified** for cohort silo-20260603-153020Z; the reshard activity was 5 startup retries and was complete before any load arrived.
- Section 18's verdict ('G-026 lands - heavy wedge eliminated at 4k rung') **stands** - the 4k cohort verdict is independent and the 4k phenotype was a real wedge with G-026 a real fix.
- Section 19's 'corrected analysis of the 25k cohort' is **doubly corrected** by this section: the initial silo-log-only reading was wrong (called it healthy graceful-degradation when it was bursty), the second reading was wrong (called it a deadline interlock wedge), and the third reading (this section, after the [phaseA] cross-reference) shows the system is in honest TCP back-pressure at the offered-rate-vs-drain-rate saturation point with no wedge at all.
- Section 20's 'G-027 as the next cycle entry point' is **superseded**. The next genuine cycle is Family A (per-flush latency reduction at the 25k rung), framed as a perf optimisation against a known ~250 e/s sustained saturation rate, not as wedge investigation.

## 22. Campaign-end status

The wedge investigation campaign that opened with G-019 (#546) and closed with G-026 (#577) is **resolved** at the rungs it targeted:

- **4k rung**: heavy-wedge phenotype eliminated 3/3 cohorts on commit `006ba11` (G-026). Median throughput 869 -> 5,775 e/s. No regression risk vs main.
- **25k rung**: no wedge present after dedup + [phaseA] cross-reference. System operates in honest TCP back-pressure at the Azure-Tables saturation drain rate (~250 e/s sustained). G-027 hypothesis invalidated; issue #578 closed not-planned.

Reliability shipped via PR #579 (merged to main as `1dff59c`). The diagnostic packs G-023 / G-024 / G-025 plus the StallWatchdog + `[wal-append]` / `[wal-slot]` lifecycle stamps remain in the codebase for any future wedge investigation, and are now backed by stable field-signature contracts via the WedgeDiagnostics test suites in test/lattice/BPlusTree/Grains/.

Future work is performance (Family A) or operator-experience (the admission-deadline-split idea from section 17 follow-up), not reliability.

---

## 23. Re-verification on a deterministic VM 2026-06-04 (wedge-plan2.md Phase 1)

A new investigation cycle (`wedge-plan2.md`) was opened against the post-G-026 main tip after a fresh "wedge reliably at >=4k vehicles" report. The cycle moved the iteration host off ACI onto a single `Standard_F8as_v6` VM in westus3 (8 vCPU AMD Zen4, 32 GiB, accelerated networking confirmed end-to-end). Same managed-identity Tables account, same code path, deterministic single-tenant host. See `repro/wedge-orleans/wedge-plan2.md` Phase 0 for the rationale (ACI's `az container logs` 60s truncation, bench-scraper stdout duplication, vCPU ceiling, and missing `dotnet-dump`/`dotnet-counters` attach surface drove ~3 spurious investigation cycles in this campaign alone).

### 23.1 Findings

Cohorts at 4k:5 and 25k:5 reproduced cleanly on the new host. Summary:

| Cohort | Written | Failed | Active avg | Silo CPU peak | Diagnostics | Verdict |
|---|---|---|---|---|---|---|
| 4k:5 / 30s | 547,006 | 0 | 13,884 e/s | 220% (2.2 cores) | clean | HEALTHY |
| 25k:5 / 30s (default 30s grain timeout) | 36,992 | 45,056 | 587 e/s | 270% | clean (no watchdog) | "wedge"-shaped but **not a wedge** |
| 25k:5 / 30s (BENCH_RESPONSE_TIMEOUT_SEC=180) | 147,566 | 0 | 2,243 e/s | 490% (5 of 8 cores) | clean | HEALTHY |

The "wedge at 25k" reproduced **only as the bench harness's outer Orleans grain RPC `ResponseTimeout` (default 30s) firing on calls that were honestly queueing at the G-026 writer admission cap**. With a realistic 180s timeout the same rung produced zero failures and a 3.8x throughput improvement. The G-026 cap was firing as designed (`wal.writer.append.admission_wait p99 ~2.1s` in the first 10s window); the bench harness's caller-side deadline was just shorter than the realistic worst-case admission wait at that rung.

### 23.2 Self-attribution improvements

Two changes shipped to make this failure mode unmissable to the next investigator:

1. **Named log line on `TimeoutException`** in `TcpIngestService.FlushAsync`. The bare Orleans `Response did not arrive on time in 00:00:30` stack is now wrapped:
   > `[silo] grain-rpc-deadline: SetManyAsync of N did not return within ResponseTimeout (BENCH_RESPONSE_TIMEOUT_SEC=30s). Offered rate exceeds sustained Tables drain rate at this rung; raise BENCH_RESPONSE_TIMEOUT_SEC, drop tickHz/vehicles, or tune WAL fan-out (BENCH_WAL_PARTITIONS / BENCH_WAL_MAX_PENDING_BATCHES).`
2. **Cohort-runner verdict** (`benchmark/vm/run-cohort.ps1`) parses the FINAL line and reports `failed=N` explicitly when non-zero, so a "HEALTHY (but 55% failed)" cohort can't pass for HEALTHY.

### 23.3 Saturation knobs catalogue (what to turn when offered exceeds drain)

Reference for future cycles. Knobs are listed in the order an investigator typically reaches for them. Each knob comes with the failure mode it addresses; misdiagnosing the failure mode and turning the wrong knob is what made this campaign go on as long as it did.

| Knob | Default | What it bounds | When to turn |
|---|---|---|---|
| `BENCH_TICK_HZ` / `BENCH_VEHICLE_COUNT` | 5 / 4000 | Offered rate (= vehicles x tickHz) | First lever; if offered rate >> sustained drain rate, no other knob makes the system healthy. |
| `BENCH_RESPONSE_TIMEOUT_SEC` | 30 | Silo+Client `Orleans.Messaging.ResponseTimeout` | Raise when `[silo] grain-rpc-deadline` appears - this is the bench harness's outer call hitting its own deadline. Saturation is not a wedge. |
| `BENCH_BATCH_SIZE` | 4096 | Entries per `SetManyAsync` call | Larger = fewer round-trips, more coalescing in WAL phase 2; smaller = lower head-of-line stuck-time at the WAL admission cap. |
| `BENCH_FLUSH_CONCURRENCY` | 8 | Parallel in-flight `SetManyAsync` calls from `TcpIngestService` | Match to `BENCH_WAL_PARTITIONS` so every flush has a distinct WAL partition to land on. |
| `BENCH_WAL_PARTITIONS` | 8 | WAL grain count per tree | Increase to widen the writer-side fan-out; pairs with `BENCH_FLUSH_CONCURRENCY`. |
| `BENCH_WAL_MAX_PENDING_BATCHES` | 8 (bench), 1 (library default) | Per-WalShardGrain pipeline depth | Already G-026; raising lets each partition's flushes overlap against Tables. |
| `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS` | 5 | Phase-2 commit-batching window | Raise to coalesce more commits into one Tables transaction (helps throughput at the cost of latency). |
| `BENCH_PIPELINE_PHASE2` | 1 (on) | Overlap phase 2 of batch N with phase 0+1 of batch N+1 | Leave on for throughput; switch off only as a diagnostic A/B. |
| `WalAppendDispatchTimeout` (lattice option) | 30s | Writer dispatch deadline (G-023) | Library-level cap; not a bench knob but firing of `wal.append_dispatch.timeouts` signals the writer thinks the shard is unresponsive. |
| `WalFlushPreflightTimeout` (lattice option) | 5s | Shard-side preflight deadline (G-023) | Same. |
| `ShardForwardTimeout` (lattice option) | 2s | Outbound shard-forward deadline (G-021) | Bounds parked forwards (not Orleans-rejected forwards). |

Failure-mode -> knob mapping:

- **`[silo] grain-rpc-deadline` log line** -> raise `BENCH_RESPONSE_TIMEOUT_SEC` OR drop the offered rate. Not a Lattice bug.
- **`failed=N` non-zero in FINAL** -> same as above (those are the timed-out batches).
- **`stall-watchdog` lines + `[wal-slot]` rows + `inFlight` pinned** -> genuine WAL wedge; the G-024 `[wal-slot]` lifecycle stage names the locus (see section 11).
- **`[wal-append]` rows with `SentToShard` dominant** -> G-025 territory; shard-grain RPC saturated; raise WAL fan-out.
- **Healthy verdict but throughput plateau** -> Family A (per-flush latency reduction); the silo CAN sustain the offered rate, the Tables RTT is the floor.

### 23.4 Carry-forward

- Wedge-plan.md is **closed** as a reliability investigation. Re-opening requires evidence that a `stall-watchdog` line fires AND `[wal-slot]` / `[wal-append]` lifecycle stages show a non-trivial dominant stage AND the `[silo] grain-rpc-deadline` line does NOT appear. Until that combination shows up, "wedge at high rungs" is the bench harness's grain RPC deadline, not a Lattice bug.
- Next cycle is **performance, not reliability**. Family A (per-flush latency) at the 25k rung now has a clean 2,243 e/s sustained baseline on the F8 VM to push against.
- The deterministic-VM bench harness (`benchmark/vm/`) is the canonical iteration loop. ACI is demoted to optional cross-environment smoke testing post-fix.

