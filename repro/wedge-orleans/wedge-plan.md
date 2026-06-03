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
WalCommitLogWriter).
