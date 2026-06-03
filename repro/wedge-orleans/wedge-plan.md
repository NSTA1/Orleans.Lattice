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

## 6. Repro built and run - the simple repro does NOT reproduce the wedge

A 4-arm in-process repro lives at `repro/wedge-orleans/`:

| Arm | Caller | Pattern | Result |
|---|---|---|---|
| 1.A | Main (no grain context) | `WaitAsync(TimeSpan)` | fired in 2006 ms - OK |
| 1.B | Main (no grain context) | linked CTS + `WaitAsync(token)` | fired in 2008 ms - OK |
| 2.A | Wrapper grain (captures grain context on await) | `WaitAsync(TimeSpan)` | fired in 2010 ms - OK |
| 2.B | Wrapper grain (captures grain context on await) | linked CTS + `WaitAsync(token)` | fired in 2014 ms - OK |

**Verdict: the Orleans 10.1.0 grain-RPC return-task plumbing does NOT, in
isolation, suppress `Task.WaitAsync` cancellation - including from inside a
grain that awaits another grain's blocked Task with default ConfigureAwait
(grain context captured on the await, matching the real-wedge caller shape).**

This eliminates the simplest "Orleans broke `WaitAsync`" theory. The Lattice
wedge therefore depends on at least one additional condition not present in
the minimal repro. Candidates ranked by plausibility:

1. **Silo-wide load shape**: many grains parked simultaneously, not a single
   activation. The repro has 2 grain activations; the real wedge has hundreds
   of parked turns across multiple WalShardGrain activations.
2. **Singleton helper hop**: `WalCommitLogWriter` is DI-singleton, not a grain.
   The grain calls singleton.AppendManyAsync which then awaits a different
   grain. The repro skips the singleton hop entirely - it goes grain-to-grain.
3. **WalShardGrain congestion**: the callee grain has hundreds of pending
   turns parked at `await headTask` (line 585). The repro's blocking grain
   has exactly one parked turn.
4. **`reshard ... REJECTED (Forwarding failed)` storm**: 228-540 per wedged
   run in the real cohort. The repro has none.
5. **Clustering / scheduler config**: `UseLocalhostClustering` may differ from
   ACI's setup. Less likely but unfalsified.

## 7. Escalation path - REVISED post-repro

The simple "file at dotnet/orleans" path is no longer well-evidenced - the
Orleans primitives demonstrably work in the minimal case. The next productive
step is one of:

- **Extended repro**: incrementally add Lattice-specific patterns (singleton
  helper, multiple shard activations, leaf-state locks, simulated reshard
  rejection storm) to the existing `repro/wedge-orleans/` and find the smallest
  combination that reproduces. This is a multi-hour effort but is the only
  path that produces an upstream-actionable report.
- **ClrMD-based lock-ownership probe** under the real wedge: extend the
  StallWatchdog to also dump SyncBlock / Monitor.Wait ownership data, not
  just parked async frames. This would name the specific synchronization
  primitive that's not making progress at the moment the wedge fires. Cost:
  ~50-100 LOC bench-side; needs one ACI cohort to validate.
- **Stop here**. The wedge is now a tracked-known-issue with a published
  reproducible test bed (`repro/wedge-orleans/`) showing what does NOT cause it,
  and four shipped commits adding partial diagnostic coverage. Future cycles
  can pick up from this point.

## 8. Standing carry-forward rule (unchanged)

No throughput A/B at the saturation rung on the azure-throughput tier until
the wedge is resolved upstream. The cohort medians at saturation are confounded
by the wedge until then.

## 9. What landed on `fix/wedge` this cycle (in order)

```
fb4912e  fix: replace WaitAsync(TimeSpan) with linked-CTS deadline on WAL writer dispatch (Option B, ineffective but kept - structurally cleaner than the WaitAsync(TimeSpan) shape and the catch filter is more precise)
d0852d7  diag: log inside writer dispatch-timeout catch (kept - cheap permanent diagnostic for any future wedge cohort)
68b0b33  bench: stamp WalAppendDispatchTimeout / WalFlushPreflightTimeout in silo banner (kept - permanent deployment verification)
bfdc384  fix: bound and attribute the residual phase-1/activation WAL wedge (G-023 pack)
af5db4b  docs: add G-023 (residual WAL wedge diagnostic pack) and dedupe G-022
1cd8d55  docs(bench): caveat vertical-scale.md with wedge-lottery risk and post-#568 residual wedge
afb00b1  docs: record vertical-scaling null result; revert silo to 2 vCPU/4 GiB
02a3ecf  ci: clear leaked per-run BENCH_* overrides at azure-throughput ladder startup
24b8f74  docs: add FX-023 (reshard equal-count no-op) to core features index
650d65f  ci: harden azure-throughput ladder against leaked BENCH_TREE_ID; bump silo to 4 vCPU/8 GiB (silo later reverted to 2/4)
```

Plus issues filed: #570 (FX-023 reshard equal-count), #571 (docs: stale SNAT
narrative in Silo/Program.cs), #572 (G-023 wedge diagnostic pack - implemented
by bfdc384 above).
