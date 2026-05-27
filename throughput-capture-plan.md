# Throughput-capture plan: headline numbers for the 6-hour meeting

## Goal

Produce a single slide-ready report at [throughput.md](throughput.md) with **headline throughput and timing numbers for every high-level `ILattice` operation** an external audience expects to see:

1. **Point read** — `ILattice.GetAsync`
2. **Point write** — `ILattice.SetAsync`
3. **GetMany** — `ILattice.GetManyAsync`
4. **SetMany** — `ILattice.SetManyAsync`
5. **SetManyAtomic** — `ILattice.SetManyAtomicAsync` (saga-coordinated atomic write)

Numbers are produced in **two layers** because the audience needs both. Layer 1 is the in-process algorithmic cost (CPU, allocations, no Orleans RTT, in-memory provider). Layer 2 is the end-to-end durable cost (Orleans grain RTT + Azure Tables WAL + Azure Tables grain checkpoint). Reporting them side-by-side is non-negotiable: if you quote only Layer 1 the audience will think production runs at that throughput; if you quote only Layer 2 you've hidden where the system spends its time.

This file is the plan; [throughput.md](throughput.md) is the final report. Both live on the `throughput` branch; neither is intended for `main` until the chaos suite has run against [commit b872262](../) (leaf etag-race fix).

## Source-of-truth pointers

- The c2-iii operating-point baseline and the c2-iv-redux knob sweep are documented in [scaling.md](scaling.md) (U9p step c2-vii memo). The Layer-2 SetMany number quoted in [throughput.md](throughput.md) comes from the same probe-0 ladder.
- The Layer-1 harness lives at [benchmark/host/Bench.Microbench/LatticeMicroBenchmarks.cs](benchmark/host/Bench.Microbench/LatticeMicroBenchmarks.cs). Every headline op is already a `[Benchmark]`-attributed method.
- The Layer-2 harness lives at [benchmark/azure-throughput/Silo/Program.cs](benchmark/azure-throughput/Silo/Program.cs). Today it only drives `SetManyAsync`; this plan extends it with a `BENCH_WORKLOAD_MODE` switch that dispatches to the other four ops without any producer-side change.

## Layer 1 — In-process microbench

**What it measures.** Per-op CPU + allocations + in-memory provider time, single-threaded, no Orleans RTT, no Azure Tables I/O. BenchmarkDotNet computes mean / median / stddev / allocations from O(10⁴–10⁶) iterations per op so the numbers have very tight error bars.

**What this tells the audience.** "This is what each `ILattice` operation costs when scheduling and storage are perfect." Useful as an upper bound on production throughput; useful for spotting algorithmic regressions; **not** the number to quote for "production sustained throughput."

**Existing `[Benchmark]` coverage** (no harness changes needed):

| Headline op  | `[Benchmark]` method | Description |
| ------------ | -------------------- | ----------- |
| Point read   | `PointRead`          | Single-key read off a pre-populated leaf |
| Point write  | `PointWrite`         | Single-key write into a pre-populated leaf |
| GetMany      | `Point_GetMany_*` (per-batch-size variants) | Multi-key read against a pre-populated leaf |
| SetMany      | `BulkLoad`, `SetMany_4Shards` | Multi-key write into a single leaf / across 4 shards |
| SetManyAtomic | `SetMany_Atomic`, `SetMany_Atomic_4Shards`, `SetMany_Atomic_Concurrent` | Saga-coordinated atomic multi-key write |

**Run command** (single invocation produces every row we need):

```pwsh
dotnet build benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj -c Release
dotnet run -c Release --no-build --project benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj `
	-- --filter "*Point*|*Bulk*|*SetMany*" --memory --warmupCount 3 --iterationCount 5 --launchCount 1
```

Results land at `benchmark/.run/microbench/<runid>/results.json` plus a stdout table BenchmarkDotNet renders directly. Wall-clock: ~10–15 minutes on a developer laptop.

**Numbers the report extracts per row:**

- `Mean` (ns/op) — the slide's "median time per call" column
- `StdDev` (ns/op) — the error bar
- `Allocated` (B/op) — the GC pressure column
- Derived `1 / Mean` × 10⁹ — the slide's "ops/sec single-threaded" column

## Layer 2 — Azure-throughput bench, extended

**What it measures.** End-to-end through `ILattice` against real Azure Tables (WAL + grain state), single silo, 32 shards, c2-iii operating-point baseline. The producer in [benchmark/azure-throughput/Producer/Program.cs](benchmark/azure-throughput/Producer/Program.cs) generates a sustained event stream over TCP; the silo's `TcpIngestService` batches and dispatches the workload through `ILattice`; the silo's per-second `[silo] Entries written per second=` line is the headline rate; `BenchMetrics` histograms give p50/p99 latency per op.

**What this tells the audience.** "This is what the system sustains end-to-end against durable storage on a single silo at the c2-iii operating point." This is the number to quote when someone asks "how fast does it go in production."

**Current coverage.** Only `SetManyAsync` is wired up. The Layer-2 SetMany number for this report is already in hand from the c2-vii probe-0 ladder (commit `b872262`, 2026-05-27):

| Workload (today)                          | rung    | SteadyAvg | p50 / p99 | FinalFailed |
| ----------------------------------------- | ------- | --------: | --------- | ----------: |
| `SetManyAsync` (32 shards, 4096 entries/batch) | 10000:5 | **12,708 op/s** | (extract from PhaseA histograms) | 0 |

**Extension required.** Add `BENCH_WORKLOAD_MODE` env-var with five values, plumbed end-to-end:

| `BENCH_WORKLOAD_MODE`  | Silo behaviour | Pre-seed | What gets measured |
|------------------------|----------------|----------|---------------------|
| `set-many` (default — unchanged) | dispatch batch via `ILattice.SetManyAsync` | none | current SetMany throughput |
| `set-many-atomic`      | dispatch batch via `ILattice.SetManyAtomicAsync` (smaller batches: see §Atomic batch sizing below) | none | atomic-write saga throughput |
| `set-point`            | for each entry in the batch, fan out per-entry `ILattice.SetAsync(k, v)`, awaited in parallel up to `FlushConcurrency` | none | point-write throughput |
| `get-point`            | for each entry in the batch, fan out per-key `ILattice.GetAsync(k)` against the pre-seeded keyspace | one-shot `BulkLoadAsync` over `BENCH_VEHICLE_COUNT` keys at silo startup | point-read throughput |
| `get-many`             | dispatch the batch's key list via `ILattice.GetManyAsync(keys)` | same pre-seed as `get-point` | get-many throughput |

The producer requires zero changes — it keeps generating vehicle events; the silo decides what to do with each event per mode. The producer's "vehicle id" is the key the silo uses for the read and point-write modes, so the pre-seeded keyspace is exactly the keyspace the producer's events touch.

**Atomic batch sizing.** A 4096-key atomic saga is unrealistic and would dominate the result with the saga grain's startup cost; the audience-relevant number is "what does an atomic batch of N keys cost." The silo respects an optional `BENCH_ATOMIC_BATCH_SIZE` env-var (default `64`) which, when `set-many-atomic` mode is selected, slices the producer's batch into N-sized atomic calls; if absent, the silo falls back to `BENCH_BATCH_SIZE` (4096) for backward compatibility.

**Reporting.** The existing `BenchMetrics.LatticeSetManyDurationMs` histogram is renamed to a generic `BenchMetrics.LatticeOpDurationMs` with a `mode` tag; the per-second log line keeps the same shape (`Entries written per second=...`) so `40-ladder.ps1` parses every mode identically. The PhaseA reporter additionally emits per-mode p50/p99 latency every cadence tick.

## Implementation steps

Each step is independently verifiable. A new session can pick up at any uncompleted step by inspecting the **Done when** clause; do not start step N+1 until step N's Done-when is met. Steps marked `[Azure]` consume real Azure spend; everything else is local.

### Step 1 — Layer-1 microbench run (parallel; no code changes)

- **Action**: from the repo root, execute:
  ```pwsh
  dotnet build benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj -c Release
  dotnet run -c Release --no-build --project benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj `
    -- --filter "*Point*|*Bulk*|*SetMany*" --memory --warmupCount 3 --iterationCount 5 --launchCount 1
  ```
- **Done when**: `benchmark/.run/microbench/<runid>/results.json` exists and contains a row for each of `PointRead`, `PointWrite`, at least one `Point_GetMany_*`, `BulkLoad` (or `SetMany_4Shards`), and at least one `SetMany_Atomic*` variant.
- **Output captured**: the BDN stdout table (screenshot or paste) plus the JSON file.
- **Wall-clock**: ~10–15 min. Can run concurrently with steps 2–7.

### Step 2 — Add `BenchWorkloadMode` enum + parser

- **File**: [benchmark/azure-throughput/Silo/Program.cs](benchmark/azure-throughput/Silo/Program.cs).
- **Action**: declare `internal enum BenchWorkloadMode { SetMany, SetManyAtomic, SetPoint, GetPoint, GetMany }`. Add a static `BenchWorkloadMode ParseWorkloadMode(string? raw)` that maps the env-var string (case-insensitive, kebab-case: `set-many`, `set-many-atomic`, `set-point`, `get-point`, `get-many`) to the enum, defaulting to `SetMany` on null/unknown. Resolve `BENCH_WORKLOAD_MODE` once at silo startup and echo the resolved mode on the same `[silo] settings.*` startup line that already prints the other knobs.
- **Done when**: the silo's startup log line includes `workloadMode=set-many` (or whichever) when the env-var is set; the project builds; no library code touched.

### Step 3 — Extract dispatch into a testable static helper

- **File**: [benchmark/azure-throughput/Silo/Program.cs](benchmark/azure-throughput/Silo/Program.cs).
- **Action**: add a `static Task<int> BenchWorkloadDispatcher.DispatchAsync(BenchWorkloadMode mode, ILattice lattice, List<KeyValuePair<string, byte[]>> batch, int atomicBatchSize, int parallelism, CancellationToken ct)`. The current `TcpIngestService.FlushAsync` body becomes the `SetMany` branch. Add branches for `SetManyAtomic` (slice into `atomicBatchSize` sub-batches; await each as `lattice.SetManyAtomicAsync(...)`), `SetPoint` (per-entry `lattice.SetAsync(k, v)` fan-out under a `SemaphoreSlim(parallelism)`), `GetPoint` (per-entry `lattice.GetAsync(k)` fan-out under the same semaphore; discard return value), and `GetMany` (single `lattice.GetManyAsync(batch.Select(e => e.Key).ToList())` call). Return the number of `ILattice`-visible ops performed (4096 for SetMany, the count of atomic sagas for SetManyAtomic, the entry count for the point modes, 1 for GetMany — this is the value that lands on the existing per-second counter).
- **Done when**: `FlushAsync` is now a thin caller of `DispatchAsync`; the SetMany code path is byte-for-byte equivalent under the `SetMany` mode; no compile errors.

### Step 4 — Rename the latency histogram to per-mode

- **File**: [benchmark/azure-throughput/Silo/BenchMetrics.cs](benchmark/azure-throughput/Silo/BenchMetrics.cs).
- **Action**: rename `BenchMetrics.LatticeSetManyDurationMs` to `BenchMetrics.LatticeOpDurationMs`; the dispatcher in step 3 records into it with an additional `mode` tag (`"set-many"`, `"set-many-atomic"`, `"set-point"`, `"get-point"`, `"get-many"`). The `PhaseADiagnosticReporter` already groups by tag so per-mode p50/p99 surface in the `[phaseA]` lines automatically.
- **Done when**: a probe run with `BENCH_WORKLOAD_MODE=set-many` produces a `[phaseA] ... instrument=lattice.op.duration_ms ... mode=set-many ... p50=... p99=...` line per cadence window.

### Step 5 — Read-mode pre-seed

- **File**: [benchmark/azure-throughput/Silo/Program.cs](benchmark/azure-throughput/Silo/Program.cs).
- **Action**: after `lattice.WarmUpAsync(...)` and before `TcpIngestService` opens its listener, if `workloadMode is GetPoint or GetMany`, run a one-shot `lattice.BulkLoadAsync(seedEntries, ct)` where `seedEntries` is `BENCH_VEHICLE_COUNT` entries of `(key: "v" + i.ToString("D7"), value: byte[245])` with the value filled deterministically (e.g. `i` mod 256 fill). Time and log the pre-seed step on a dedicated `[silo] preseed entries=... elapsedMs=...` line so the ladder script can attribute pre-seed cost separately from steady-state.
- **Done when**: with `BENCH_WORKLOAD_MODE=get-point` set, the silo log emits the `[silo] preseed entries=10000 elapsedMs=...` line before the first `Entries written per second=` line.

### Step 6 — Deploy-script env-var plumbing

- **File**: [benchmark/azure-throughput/scripts/20-build-and-deploy.ps1](benchmark/azure-throughput/scripts/20-build-and-deploy.ps1).
- **Action**: add `$workloadMode = if ($env:BENCH_WORKLOAD_MODE) { $env:BENCH_WORKLOAD_MODE } else { 'set-many' }` and `$atomicBatchSize = if ($env:BENCH_ATOMIC_BATCH_SIZE) { $env:BENCH_ATOMIC_BATCH_SIZE } else { '64' }`. Pass both through the ACI YAML's `environmentVariables` block under the existing `BENCH_*` knobs. Add both to the `[deploy] knobs: ...` echo line.
- **Done when**: `Get-Content .ladder-c2-vii-*.log` from a subsequent ladder shows `workloadMode=set-many atomicBatchSize=64` in the deploy banner.

### Step 7 — Targeted unit test

- **File**: new `test/lattice/Benchmark/BenchWorkloadDispatcherTests.cs` (or under `benchmark/host/Bench.Microbench/` if the silo's `BenchWorkloadDispatcher` is best located there).
- **Action**: with `NSubstitute`, mock `ILattice`. For each `BenchWorkloadMode`, call `DispatchAsync` with a fixed 256-entry batch and assert: (a) the expected `ILattice` method was invoked the expected number of times (1 for SetMany / GetMany; 256 for SetPoint / GetPoint; `256 / atomicBatchSize` for SetManyAtomic), (b) the `parallelism` cap is respected for the point modes (concurrent in-flight count never exceeds `parallelism`), (c) the return value equals the documented op count per the table above.
- **Done when**: `dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~BenchWorkloadDispatcher"` is green with 5+ test cases.

### Step 8 — Build the image once `[Azure]`

- **Action**: from `benchmark/azure-throughput/scripts/`, run `./40-ladder.ps1 -Rungs @('10000:5') -DurationSec 60 -CooldownSec 10 -LocalBuild` with the env-vars in step 9's first sub-step. This builds and pushes the image; the rung is throwaway (overwritten by step 9).
- **Done when**: `az acr repository show-tags --name lat01acr --repository ...` shows a fresh `latest` tag dated within the past few minutes.
- **Wall-clock**: ~3 min. **Real Azure spend.**

### Step 9 — Run five single-rung ladders `[Azure]`

- **Action**: for each `BENCH_WORKLOAD_MODE` value, set the env-var and run `./40-ladder.ps1 -Rungs @('10000:5') -DurationSec 60 -CooldownSec 10 -SkipBuild`. After each ladder, copy `.ladder-results.csv` to `.ladder-results-c2-vii-mode-<mode>.csv` and `.ladder-phaseA.csv` to `.ladder-phaseA-c2-vii-mode-<mode>.csv`. Modes to sweep in order: `set-many` (validates the unchanged path didn't regress), `set-many-atomic`, `set-point`, `get-point`, `get-many`.
- **Done when**: all five `.ladder-results-c2-vii-mode-*.csv` files exist, each with one row, all with `FinalFailed=0` (or with explicit notes in [throughput.md](throughput.md) if a mode failed). Pre-seed line visible in the `get-*` silo logs.
- **Wall-clock**: ~3 min per ladder × 5 = ~15 min. **Real Azure spend.**

### Step 10 — Aggregate into [throughput.md](throughput.md)

- **Action**: for each mode's CSV, extract `SteadyAvg` (the op/s number) and from the matching `.ladder-phaseA-c2-vii-mode-<mode>.csv` extract the latest `instrument=lattice.op.duration_ms mode=<mode>` row's `P50` and `P99` values. Fill the Layer-2 columns in [throughput.md](throughput.md)'s headline table. Note per-op-shape units in the caveat footnote (SetMany row is "batched calls/sec at 4096 entries/call"; SetManyAtomic is "sagas/sec at 64 keys/saga"; SetPoint/GetPoint are "individual ops/sec"; GetMany is "batched calls/sec at 4096 keys/call").
- **Done when**: [throughput.md](throughput.md) has no remaining `TBD` cells; the two-layer table is complete and the caveats footer mentions the per-row batch shape.

## Resumption checklist (for a new session)

A new session resuming this work should, in this order:

1. Read [throughput.md](throughput.md). Every `TBD` cell maps to an unfinished step in this plan.
2. Run `git log --oneline benchmark/azure-throughput/Silo/Program.cs benchmark/azure-throughput/scripts/20-build-and-deploy.ps1 test/lattice/Benchmark/ 2>$null` to see which of steps 2–7 have been committed.
3. Inspect `benchmark/azure-throughput/scripts/` for `.ladder-results-c2-vii-mode-*.csv` files; each file present means the matching mode-sub-step of step 9 is done.
4. Inspect `benchmark/.run/microbench/` for the most recent `results.json`; presence and non-zero size means step 1 is done.
5. Verify `git status` for unstaged changes that may indicate an in-progress step.
6. Start at the lowest-numbered step whose **Done when** clause is not satisfied.

## Decisions baked into this plan

- **Pre-seed keyspace size = `BENCH_VEHICLE_COUNT`** so the read modes hit the same key-population shape the producer is "touching" at the SetMany operating point. Aligns the rungs across modes.
- **Single rung (`10000:5`)** for all five modes. Quickest path to a meeting-ready table; covers the c2-iii operating point that the Layer-2 SetMany number was measured against. If the meeting goes well, follow-up runs at `25000:5` and `50000:5` can fill in the fan-out story.
- **Atomic batch size 64** (vs SetMany's 4096) reflects realistic saga usage. A 4096-key atomic saga isn't an op shape the audience would deploy.
- **No producer changes** — every mode reuses the existing event stream. The "vehicle id" is the key for the point modes; the producer's batch is the key-list for the `get-many` mode.

## Open questions parked for follow-up

- **`Get*` read modes vs cold cache.** The pre-seed runs through `BulkLoadAsync` which warms the leaf cache. A "cold-cache point-read" number would require restarting the silo between pre-seed and measurement, which doubles the Azure spend and isn't audience-critical for this meeting. Quote the warm-cache number with the caveat in [throughput.md](throughput.md)'s footnote.
- **`SetManyAtomic` failure mode under load.** The atomic-write bench at the c2-iv-c2 anomaly notes (see [scaling.md](scaling.md)) showed variance issues at very short runs. A 60 s producer with `BENCH_ATOMIC_BATCH_SIZE=64` should produce O(10⁴) atomic batches, well past the warm-up; if the SteadyAvg is jagged, fall back to a per-batch latency reading rather than ops/sec.
- **Library default flips.** Reiterated from [scaling.md](scaling.md): `WalPartitions=8`, `WalMaxPendingBatches=8`, `PhaseTwoCoalescingWindow=5ms`, `PipelinePhaseTwoCommits=true` are the measured durable Azure Tables sweet spot but remain non-default for wire-compat reasons. Each flip is its own gated PR per the Phase B/C plan ladder. Not in scope for this meeting prep.

## Wall-clock budget (target: well under the 6-hour window)

| Step                                                                    | Estimate     |
| ----------------------------------------------------------------------- | -----------: |
| Plan + report scaffolding (this file + [throughput.md](throughput.md) skeleton) | 15 min (done) |
| Step 1 — Layer-1 microbench run (parallel with steps 2–7)              | 10–15 min    |
| Steps 2–6 — Layer-2 harness extension (silo + deploy script)            | 90 min       |
| Step 7 — Targeted unit test                                             | 20 min       |
| Steps 8–9 — Image rebuild + five single-rung ladders `[Azure]`          | 25 min       |
| Step 10 — Aggregate results into [throughput.md](throughput.md)         | 30 min       |
| **Total elapsed (steps 2–10 with step 1 in parallel)**                  | **~3 h 0 m** |
