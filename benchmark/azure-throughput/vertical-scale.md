# Vertical-scaling experiment - baseline & method

This document records the **baseline cohort** for a vertical-scaling experiment on
the real-Azure WAL throughput harness (`benchmark/azure-throughput/`), and the exact
procedure to reproduce it and to run the scaled-up arm.

The question the experiment answers: **does giving the silo container more CPU / memory
move the load-induced wedge onset to a higher offered rate?** The baseline below
establishes *where the wedge starts on the current 2 vCPU / 4 GiB silo*; the scaled arm
re-runs the identical ladder on a larger ACI container and compares the onset rung.

> [!WARNING]
> **n=1 results carry wedge-lottery risk.** The 4k rung sits at the saturation boundary
> of a known **bimodal phase-1/activation wedge** previously tracked as
> [issue #546](https://github.com/NSTA1/Orleans.Lattice/issues/546) (closed by PR #568,
> which shipped `LatticeOptions.ActivationReadyTimeout`, 15 s default). Pre-PR-#568, the
> wedge fired on ~1/3 to ~1/2 of runs at and above the saturation rung *independent of
> the option under test*. PR #568 named the mechanism (an unbounded cross-grain await
> under `_ensureRootGate` in `ShardRootGrain.EnsureRootSlowAsync`) and bounded it, but
> the 4k wedge observed in the cohort below shows a **residual wedge still exists in the
> tree after #568**. Single-run rung verdicts here therefore carry lottery risk. The
> **categorical** finding ("vertical scaling does not move the onset") is robust because
> both arms wedged at 4k; throughput deltas at sub-saturation rungs (2k/3k) should not be
> read as a CPU effect at n=1.

> [!IMPORTANT]
> **Validity caveat - read before trusting the comparison.** The baseline wedge
> (`inFlight=8`, rate collapses to 0) is pinned at the `BENCH_WAL_MAX_PENDING_BATCHES=8`
> ceiling, and the new `orleans.lattice.provider.phase2.commit.timeouts` counter **never
> fired** across any rung (`FinalFailed=0` everywhere). That means the wedge is **not**
> obviously CPU- or memory-bound at the silo - it is a *pending-batch backpressure* stall
> upstream of the phase-2 manifest commit. A vertical-scaling experiment is still **valid**
> as a falsifiable test ("if the wedge is silo-resource-bound, more vCPU/RAM moves the
> onset; if it is bound by the pending-batch ceiling or by Azure Tables partition-server
> throughput, it will not"), but the *expected* outcome on this evidence is that vertical
> scaling alone does **not** move the onset. Treat a null result as the informative result.
> If the scaled arm does move the onset, that is itself a strong signal the stall has a
> silo-side CPU/GC component worth chasing.

## Baseline cohort

### Environment

| Property | Value |
|---|---|
| Branch / commit | `fix/wedge` @ `a69b6b7` |
| Harness | `benchmark/azure-throughput/` (real Azure Storage, two-container ACI group) |
| Region | `westeurope` |
| Storage | `lat01sa` Azure Tables, managed-identity auth (`https://lat01sa.table.core.windows.net`) |
| Silo container | **2.0 vCPU / 4.0 GiB** (ACI `resources.requests`, `20-build-and-deploy.ps1` line ~390) |
| Producer container | 1.0 vCPU / 1.5 GiB |
| Leaf grain storage | `azure` (`OrleansLatticeGrainState` table) |

### Silo configuration (banner, verbatim)

```
batch=4096 flushMs=50 flushConcurrency=8 walPartitions=8 walMaxPending=8
shardCountOverride=32 pipelinePhase2=True eliminateCandidateRow=True
phase2CoalescingMs=5 walNetworkTimeoutSec=0 walPhase2CommitTimeout=default(3s)
totalDurationSec=120 responseTimeoutSec=180 leafStorageKind=azure
leafStorageNumGrains=0 workloadMode=set-many atomicBatchSize=64
```

`walPhase2CommitTimeout=default(3s)` confirms the silo inherited the library default
(`AzureTableWalStorageOptions.DefaultPhaseTwoCommitTimeout`); no
`BENCH_WAL_PHASE2_COMMIT_TIMEOUT_SEC` override was supplied for any rung.

### Results

Two passes were run. The coarse sweep died after rung 3 (terminal-session teardown), but
the ladder persists `.ladder-results.csv` incrementally so the completed rungs are valid.
The bisect pass (2k/3k/4k) ran clean to completion (exit 0).

**Coarse sweep (`-DurationSec 45`, partial - rungs 1-3 only):**

| Vehicles | TickHz | Target/s | SteadyAvg/s | FinalWritten | FinalFailed | Verdict |
|---|---|---|---|---|---|---|
| 1,000 | 5 | 5,000 | 4,953 | 224,000 | 0 | healthy (tracks target) |
| 5,000 | 5 | 25,000 | 116 | 0 | 0 | wedged |
| 10,000 | 5 | 50,000 | 212 | 0 | 0 | wedged |

**Bisect of the 1k-5k band (`-DurationSec 30`, clean run):**

| Vehicles | TickHz | Target/s | SteadyMin | SteadyAvg/s | SteadyMax | FinalWritten | FinalFailed | Verdict |
|---|---|---|---|---|---|---|---|---|
| 2,000 | 5 | 10,000 | 0 | 8,732 | 20,486 | 284,295 | 0 | mostly healthy (transient stalls) |
| 3,000 | 5 | 15,000 | 4,033 | 15,861 | 24,557 | 446,916 | 0 | healthy (best rung) |
| 4,000 | 5 | 20,000 | 0 | 1,273 | 8,062 | 47,448 | 0 | **wedged** |

### Wedge onset

**~4,000 vehicles (~20,000 entries/s offered load).** At 3k the silo sustains target;
at 4k it pins at `inFlight=8` with per-second rate flat-lining to 0 for ~60 s:

```
[silo] t=  135.6s written=47,448 Entries written per second=0 inFlight=8
[silo] t=  140.6s written=47,448 Entries written per second=0 inFlight=8
[silo] FINAL written=47,448 failed=0 elapsed=141.5s Entries written per second (avg)=335
```

### Diagnostic signals at the wedge

- `inFlight=8` == `BENCH_WAL_MAX_PENDING_BATCHES` ceiling -> the stall is in the
  **pending-batch backpressure** path, not the phase-2 manifest commit.
- `orleans.lattice.provider.phase2.commit.timeouts` counter: **0 increments** in every
  rung silo log. The 3 s phase-2 deadline is never the seam this wedge runs through.
- `FinalFailed=0` everywhere - the wedge is a throughput collapse, not an error storm.

### Artefacts (gitignored, on the run box only)

- Bisect ladder stdout: `benchmark/azure-throughput/.run/ladder-bisect-20260603-062951Z.out.log`
- Per-rung silo logs: `benchmark/azure-throughput/.run/silo-20260603-06*.log`
- Results CSV: `benchmark/azure-throughput/scripts/.ladder-results.csv`
- Phase-A scrape CSV: `benchmark/azure-throughput/scripts/.ladder-phaseA.csv`

## Reproducing the baseline

One-time setup (per workstation) if not already provisioned:

```powershell
$env:BENCH_PREFIX = 'lat01'   # or a fresh prefix
./benchmark/azure-throughput/scripts/00-login.ps1
./benchmark/azure-throughput/scripts/10-provision.ps1
```

Run the ladder (clear any leftover per-run overrides first so each rung sets its own
vehicle/tick/duration values):

```powershell
Remove-Item Env:\BENCH_VEHICLE_COUNT, Env:\BENCH_TICK_HZ, Env:\BENCH_DURATION_SEC, `
            Env:\BENCH_TOTAL_DURATION_SEC, Env:\BENCH_WAL_PHASE2_COMMIT_TIMEOUT_SEC `
  -ErrorAction SilentlyContinue

# Bisect the wedge band directly (fast: ~8 min, reuses the ACR image with -SkipBuild).
./benchmark/azure-throughput/scripts/40-ladder.ps1 `
  -Rungs '2000:5','3000:5','4000:5' -DurationSec 30 -SkipBuild

# Or the full coarse sweep from a clean build:
./benchmark/azure-throughput/scripts/40-ladder.ps1 -LocalBuild -DurationSec 45
```

Read the result:

```powershell
Import-Csv benchmark/azure-throughput/scripts/.ladder-results.csv |
  Format-Table Rung,Vehicles,TargetRate,SteadyMin,SteadyAvg,SteadyMax,FinalWritten,FinalFailed -AutoSize
```

> [!NOTE]
> Run the ladder in the **foreground**. Background launches of this harness do not
> reliably stream/attach in this workspace. If a terminal turn is torn down mid-ladder the
> detached process can die after the current rung - re-run the remaining rungs (earlier
> rungs are already persisted in `.ladder-results.csv`).

## Running the scaled (vertical) arm

The silo container size is the only variable to change. It is hard-coded in the ACI YAML
emitted by `20-build-and-deploy.ps1`:

```yaml
# benchmark/azure-throughput/scripts/20-build-and-deploy.ps1  (~line 388)
      - name: silo
        resources:
          requests:
            cpu: 2.0            # baseline -> raise for the scaled arm (e.g. 4.0)
            memoryInGB: 4.0     # baseline -> raise for the scaled arm (e.g. 8.0 or 16.0)
```

Procedure for the scaled arm:

1. Edit `cpu` / `memoryInGB` in the silo container's `resources.requests` block. ACI
   Linux container groups support up to 4 vCPU / 16 GiB per group on the default quota;
   confirm the target region's quota before picking a rung (`az vm list-usage` is the
   wrong API - use `az container create` dry-run or the ACI quota docs).
2. Keep **every other variable identical** to the baseline: the banner knobs above, the
   `-Rungs`/`-DurationSec` ladder arguments, the leaf-storage kind, and the region. The
   container size is the single independent variable.
3. Re-run the **same bisect ladder** (`-Rungs '2000:5','3000:5','4000:5'`) plus one rung
   above the baseline onset (e.g. add `'6000:5','8000:5'`) so a moved onset is observable.
4. Compare the onset rung. Record the scaled cohort in a sibling table here and state the
   decision against the validity caveat:
   - **Onset unchanged (~4k):** wedge is bound by the pending-batch ceiling or Azure
     Tables partition-server throughput, not silo CPU/RAM. Vertical scaling is not the
     lever; pursue the `BENCH_WAL_MAX_PENDING_BATCHES` / phase-1 backpressure path instead.
   - **Onset moved up:** the stall has a silo-side CPU/GC component; profile the flush
      pipeline under load to attribute it.

## Scaled-arm result (4 vCPU / 8 GiB) - NULL RESULT

The scaled arm was run at **4.0 vCPU / 8.0 GiB** (double the baseline), candidate-row
elision held to the library default (`eliminateCandidateRow=True`, matching baseline),
fresh per-rung tree ids, same `-Rungs '2000:5','3000:5','4000:5' -DurationSec 30`.

| Vehicles | TickHz | Target/s | SteadyMin | SteadyAvg/s | SteadyMax | FinalWritten | FinalFailed | Verdict |
|---|---|---|---|---|---|---|---|---|
| 2,000 | 5 | 10,000 | 0 | 4,398 | 28,520 | 205,326 | 0 | healthy (noisier) |
| 3,000 | 5 | 15,000 | 0 | 11,011 | 36,880 | 344,070 | 0 | healthy |
| 4,000 | 5 | 20,000 | 0 | 745 | 4,096 | 23,297 | 0 | **wedged** (`inFlight=8` pinned, rate=0 for ~12s+ tail) |

```
[silo] t=  140.1s written=23,297 Entries written per second=0 inFlight=8
[silo] t=  142.1s written=23,297 Entries written per second=0 inFlight=8
[silo] FINAL written=23,297 failed=0 elapsed=142.5s Entries written per second (avg)=164
```

### Conclusion: vertical scaling does NOT move the wedge onset

The onset stayed at **~4,000 vehicles** - doubling CPU and RAM did not move it. This is
the outcome the validity caveat above predicted: the wedge is pinned at the
`walMaxPending=8` ceiling (`inFlight=8`), so it is bound by **pending-batch backpressure /
Azure Tables partition-server throughput**, not silo CPU/RAM. The bottleneck is not
silo-side compute.

> An earlier, **discarded** 4-core run appeared to clear the 4k wedge, but that run was
> confounded: a leaked `BENCH_WAL_ELIMINATE_CANDIDATE_ROW=false` in the operator shell put
> the silo on the faster candidate-row-elided path, so the apparent win was the WAL knob,
> not the cores. The ladder now clears leaked per-run `BENCH_*` overrides at startup
> (`40-ladder.ps1`) to prevent this class of confound. The table above is the corrected,
> matched comparison.

The lower 2k/3k throughput in the scaled arm vs baseline is **not** read as "4 cores is
slower" - both cohorts are n=1 per rung and this tier has high run-to-run variance (ACI
placement lottery, Azure Tables partition assignment). The robust signal is categorical:
the 4k wedge persists regardless of core count.

### Next lever

Not vertical scaling, and not (yet) the pending-batch ceiling either. The Phase 0
continuity check on this cycle surfaced an existing wedge campaign under
`benchmark/.run/azure-throughput/POSTMORTEM-*.md` whose carry-forward rule is explicit:
**do not resume any Azure-Tables WAL throughput A/B at or above the saturation rung
until the bimodal phase-1/activation wedge is fully resolved.** PR #568
(`ActivationReadyTimeout`) named and bounded one cross-grain await on the activation
hot path and closed [issue #546](https://github.com/NSTA1/Orleans.Lattice/issues/546),
but the 4k wedge observed in the cohort above shows a **residual wedge survives on the
current tree**. The next experiment is therefore not a throughput A/B but a **diagnostic
attribution pass** at the 4k rung: re-attach the existing `StallWatchdog` (ClrMD parked-
state dump) and the `ActivationReadyTimeouts` counter and confirm whether the residual
stall has the same `_ensureRootGate` parked-state signature (in which case `#568` is
incomplete) or a new signature (a different unbounded seam, requiring its own bounded-
deadline fix). A `BENCH_WAL_MAX_PENDING_BATCHES` sweep is only meaningful once the
residual wedge is resolved (otherwise the cohort is a function of the wedge lottery,
not the ceiling).

The silo container has been reverted to the baseline **2.0 vCPU / 4.0 GiB** since vertical
scaling is falsified.

## Teardown

```powershell
# The deploy script stops the container group at the end of each rung (no idle compute
# charge). Tear the whole resource group down when the experiment is complete:
./benchmark/azure-throughput/scripts/90-teardown.ps1
```
