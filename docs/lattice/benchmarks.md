---
agent_spec: "docs/agents/api/lattice.json"
---

# Lattice Benchmarks

The Orleans.Lattice benchmark suite is the project's regression alarm. Every
scenario is a fixed, reproducible workload that produces a small set of
summary scalars (commit p99, commits/s, ship/apply latency, cache hit ratio,
GC pressure, microbench timings) which are pushed into a long-lived history
stack and rendered as run-over-run trend lines in Grafana. A regression
caused by a refactor, a dependency bump, or a tuning change shows up as a
visible step in the trend line and the offending commit (`git_sha` is part
of every sample's labels) is one click away.

This document is the user-facing entry point. The authoritative deep-detail
reference - topology diagrams, every scenario knob, history-stack data model,
auto-discovery internals - lives in [`benchmark/README.md`](../../benchmark/README.md).

## Load generation: the vehicle fleet simulator

All docker-compose scenarios drive load through the
[Vehicle Fleet Simulator](../../samples/VehicleFleetSimulator/) - a
standalone Orleans application that simulates a population of vehicles
streaming structured telemetry events (position, speed, fuel level, route
progress) as each vehicle advances along a city graph. The benchmark stack
imports the simulator's HTTP API verbatim and points its telemetry sink at
the lattice under test, so the lattice receives per-vehicle writes at the
rate the simulator emits ticks.

A **fleet** is that simulated population. **Fleet size**
(`BENCH_FLEET_SIZE`) is the number of vehicles seeded; each vehicle becomes
one independent source of per-tick writes, so doubling the fleet size
roughly doubles the offered write load. The `fleetStats` block in every
`results.json` (see [Interpreting results](#interpreting-results) below)
reports the fleet's final state - the `total`, `driving`, `refuelling`,
`idle` and `routeCompleted` vehicle counts.

The `microbench` scenario does **not** drive a fleet. It bypasses the
simulator and the Orleans cluster entirely and exercises the `ILattice`
vertical in-process; the fleet-size calibration step below is consequently
skipped for it.

## Prerequisites

| Tier | Requires |
|---|---|
| `microbench` (in-process BDN, no cluster) | .NET 10 SDK, PowerShell 7+ |
| Every other scenario (docker-compose) | Docker Desktop (or any Compose v2-compatible daemon) + a successful run of `./initialise.ps1` to calibrate fleet size for this host |

`./initialise.ps1` measures the fleet size that stresses this host without
saturating it and writes the result to the gitignored
`benchmark/.fleet-size.config`. The microbench scenario does not drive a
fleet, so the calibration check is skipped for it; every docker-compose
scenario exits early with a friendly pointer at `./initialise.ps1` if the
config is missing. See the
[Calibrating fleet size for this host](../../benchmark/README.md#calibrating-fleet-size-for-this-host)
section of the benchmark stack README for the full calibration procedure.

## Running a benchmark

Every scenario is driven by the same entry point:

```powershell
# From the benchmark/ directory.
./benchmark.ps1 <scenario>
```

The script reads `scenarios/<slug>.env`, picks the right docker-compose
overlay (single-cluster or replication), brings the stack up, seeds the
fleet, runs the warmup + measurement window, captures an auto-discovered
panel of summary scalars into `.run/<scenario>/<run_id>/results.json`, and
opportunistically pushes the same scalars to the long-lived history stack
on `:8428` if it is reachable.

A few commonly-needed variants:

```powershell
# Keep the docker stack up after the run so Grafana stays accessible.
./benchmark.ps1 -Scenario current-state-single-peer -KeepRunning

# One-off run at a non-calibrated fleet size (skip the config-existence check).
./benchmark.ps1 -Scenario current-state-no-replication -FleetSizeOverride 4000 -SkipFleetSizeCheck

# Bring up the long-lived history stack (one-shot; stays up across many runs).
./benchmark.ps1 -OpenHistory

# Backfill prior local runs into the history stack.
./benchmark.ps1 -ImportHistory
```

## Scenarios

The suite ships a broad set of scenarios spanning the lattice-usage profiles
plus a micro-benchmark control. A condensed table is reproduced below; the
authoritative list with per-scenario knobs is in
[`benchmark/benchmark-scenarios.md`](../../benchmark/benchmark-scenarios.md).

| Profile             | Example scenario id                          | What it stresses                                          |
|---------------------|----------------------------------------------|-----------------------------------------------------------|
| Micro               | `microbench`                                 | `ILattice` algorithm cost, no Orleans dispatch            |
| Write-heavy random  | `current-state-no-replication`               | Steady-state per-vehicle current-state overwrites         |
| Write-heavy random  | `skewed-key-shard-splits`                    | Adaptive shard splitting under skewed keys                |
| Write-heavy ordered | `event-log-with-ttl`                         | Append-only event-log keyspace + TTL eviction             |
| Read-heavy          | `read-heavy-random`                          | Nominal 95:5 read:write (see below), random key distribution |
| Read-heavy          | `read-heavy-ordered`                         | Nominal 95:5 read:write (see below), point reads in key order |
| Read-write mix      | `read-write-mix-random`                      | Nominal 50:50 mix (see below), random keys (YCSB-A shape) |
| Read-write mix      | `read-write-mix-ordered`                     | Nominal 50:50 mix (see below), point reads in key order   |
| Durable WAL         | `current-state-no-replication-azuretable`    | Same write topology with Azure Table WAL durable storage  |
| Durable WAL         | `current-state-no-replication-azuretable-no-crow` | As above, with the WAL's phase-0 candidate row elided (`AzureTableWalStorageOptions.EliminateCandidateRowOnHotPath`) |
| Durable WAL         | `current-state-no-replication-azuretable-pipelined` | As above, with pipelined phase-2 commits (`AzureTableWalStorageOptions.PipelinePhaseTwoCommits`) |
| Atomic writes       | `atomic-write`                               | Sustained `SetManyAtomicAsync` saga throughput            |
| Atomic writes       | `atomic-write-replication`                   | Two-cluster bidirectional atomic-saga visibility          |
| Replication         | `current-state-single-peer`                  | Current-state tree, single-peer replication               |
| Replication         | `bidirectional-replication`                  | Two-cluster bidirectional replication                     |
| Replication         | `bidirectional-replication-azuretable`       | Two-cluster bidirectional replication with Azure Table WAL |
| Replication chaos   | `replication-backpressure`                   | Backpressure / catch-up under sender pause                |
| Replication chaos   | `receiver-crash`                             | Receiver crash mid-stream, recovery cost                  |
| Replication control | `observer-no-peer`                           | Observer-off control paired with `current-state-single-peer` |
| Replication control | `replication-key-filter`                     | Per-key replication filter cost vs no-filter baseline      |

The read-heavy and read-write-mix ratios are design-intent labels, not
measured mixes, and they hold only at about 400 vehicles. Each scenario's
`.env` pins the read driver's target rate - 38,000 `GetAsync` calls/s for
`read-heavy-*`, 2,000/s for `read-write-mix-*` - while the write rate scales
with the fleet: every vehicle publishes one telemetry sample every 200 ms and
the Lattice sink writes each one, so each vehicle adds 5 writes/s. At the
`.env` default of 2,000 vehicles the mixes are about 79:21 and about 1:5
reads to writes, and the host-calibrated fleet size that `./initialise.ps1`
writes, which overrides the `.env` value, moves them again. Neither
`-ordered` variant walks a key scan: the read driver pages a sample of up to
4,096 keys from a key cursor, refreshed every 10 seconds, and issues one
`GetAsync` per key, stepping through that sample in key order; the
`-random` variants pick keys from the same sample at random.

## Interpreting results

Every run writes a `results.json` like:

```json
{
  "scenario": "current-state-no-replication",
  "run_id":   "2026-04-30T14-08-41Z",
  "git_sha":  "abc1234",
  "started":  "2026-04-30T14:03:11Z",
  "ended":    "2026-04-30T14:08:41Z",
  "duration_s": 330,
  "config":  { "BENCH_TELEMETRY_SINK": "lattice", "BENCH_FLEET_SIZE": "2000", "...": "..." },
  "metrics": {
    "lattice_commit_p99_ms":              12.3,
    "lattice_commits_per_second":         19847,
    "sink_published_per_second":          2034,
    "sink_dropped_combined_increase":     0,
    "lattice_cache_hit_ratio":            0.94,
    "replication_ship_p95_ms":            4.7,
    "replication_apply_lag_p95_ms":       6.1,
    "...": "~52 auto-discovered keys + curated extras"
  },
  "fleetStats": { "total": 2000, "driving": 2000, "...": "..." }
}
```

The metrics panel is derived from Prometheus's `/api/v1/metadata` endpoint at
capture time, not hard-coded. Adding a new instrument to the lattice source
automatically flows into the next benchmark run; the synthesised key shape
per instrument type is documented in the
[Auto-discovery section](../../benchmark/README.md#auto-discovery-of-metrics)
of the benchmark stack README.

### Cross-run comparison

```powershell
# Latest run per scenario, side-by-side, with delta vs. a reference scenario.
./benchmark.ps1 -Compare -CompareAgainst current-state-no-replication

# Without the delta column.
./benchmark.ps1 -Compare
```

Outputs land in `.run/comparison.md` (markdown, ready to paste into a PR) and
`.run/comparison.csv` (flat for spreadsheet use).

### Trend dashboard

For run-over-run trend visualisation across commits, bring up the history
stack:

```powershell
./benchmark.ps1 -OpenHistory
# ... run scenarios as normal, they auto-push when this is up ...
./benchmark.ps1 -CloseHistory
```

Then visit <http://localhost:3001>. The history Grafana hosts an Overview
dashboard plus one persona dashboard per lattice-usage profile so
each dashboard answers a single regression question without templating-var
juggling. See the
[Trend dashboard section](../../benchmark/README.md#trend-dashboard-history-stack)
of the benchmark stack README for the full dashboard catalogue.

## The `microbench` scenario

`microbench` is the in-process tier - no Docker, no Orleans cluster boot. It
hand-instantiates the full lattice grain vertical (the tree's entry grain,
shard root, leaf, and leaf cache)
and routes `IGrainFactory` calls through an allocation-free fake grain
factory that resolves those real instances (the auxiliary grains the
vertical only calls in passing - the reminder registry, tombstone
compaction, the hot-shard monitor and similar - are NSubstitute mocks), then
exercises a fixed set of `[Benchmark]` methods (point reads/writes, bulk
loads, mixed workloads, atomic-write sagas) via
[BenchmarkDotNet](https://benchmarkdotnet.org/) with the `InProcessEmitToolchain`.

```powershell
./benchmark.ps1 microbench
```

Two CLI knobs scope each run:

| Knob | Values | Effect |
|---|---|---|
| `-Workloads` | Comma-separated BDN `--filter` globs, empty = full suite | E.g. `'*.PointWrite,*.Mixed_70R_30W'` |
| `-Fidelity` | `dry` (default for optimisation work) \| `quick` \| `full` | Iteration count + toolchain (in-process for `dry`/`quick`, forking for `full`) |

Both knobs also accept env-var equivalents (`BENCH_MICROBENCH_WORKLOADS`,
`BENCH_MICROBENCH_FIDELITY`); the CLI flag wins when both are set. The
environment variable also accepts `quick-oop` - the `quick` iteration shape
on the forking toolchain, for configurations BenchmarkDotNet refuses to run
in process - which the `-Fidelity` flag does not. The
committed defaults are `BENCH_MICROBENCH_FIDELITY=quick` and an empty
workload filter (full suite), in `benchmark/scenarios/microbench.env`.
`./benchmark.ps1` applies that file over the calling shell's environment,
so any key the file sets - these two among them, and the
`BENCH_MICROBENCH_PROFILE`, `BENCH_MICROBENCH_PROFILE_TOPN` and
`BENCH_MICROBENCH_PROFILE_NETTRACE_PATH` profiling keys below - is changed
by editing the file; a value exported in the shell is overwritten. A key
the file does not set, such as `BENCH_MICROBENCH_SUITE`, is read from the
shell.

The available workload method names are listed by running
`./benchmark.ps1 microbench -Workloads '*'` and reading the BDN summary
table. The suite currently ships a broad set of `[Benchmark]` methods covering point
reads / writes (`PointRead`, `PointWrite`, `PointReadWithVersion`,
`PointExists`), the other point-write shapes (`PointDelete`, `PointGetOrSet`,
`PointSetIfVersion`, `PointSetWithTtl`, `PointApplyCrdtDelta`) and an empty
range delete (`DeleteRangeAbsent`), multi-key reads (`PointGetMany`, `PointGetMany_BatchSize`
parameterised over batch sizes 1-64), bulk and multi-key writes
(`BulkLoad`, `SetMany_4Shards`, `Mixed_70R_30W`), key/range scans
(`KeyScan_PageOver4Shards`, `EntryScan_PageOver4Shards`, and the server-side
predicate scan `PredicateKeyScan`), deep- and deeper-tree variants of the
point, multi-get and bulk paths, atomic-write sagas (`SetManyAtomic`,
`SetManyAtomic_2Keys`, `SetManyAtomic_64Keys`, `SetManyAtomic_4Shards`,
`SetManyAtomic_Concurrent` parameterised over concurrency 1-64) and their
cross-tree counterparts (`CrossTreeAtomic_2Keys`, `CrossTreeAtomic_64Keys`),
atomic-tree reads (`PointRead_AtomicTreeIdle`,
`PointRead_AtomicTreeWithActiveSaga`), CRDT delta application against a grown
state and on the replication receiver (`CrdtApplyGrowstate`,
`CrdtApplyGrowstateWriter`, `CrdtReceiverApplyPerEntry`,
`CrdtReceiverApplyBatched`), per-operation costs of the CRDT primitives and
version vectors (`OrSet_*`, `OrMap_*`, `Rga_*`, `GSet_*`, `RwSet_*`,
`GCounter_*`, `OrFlag_*`, `RwFlag_*`, `BoundedRegister_*`, `VersionVector_*`,
and the multi-value register and PN-counter `Crdt*` arms), the leaf's
serialized, pipelined and batched WAL commit-dispatch shapes (`LeafQueue_*`),
leaf-cache drain and split-pivot instruments (`LeafCache_*`), WAL-encoder
microbenchmarks (`EncodeWalBatch_AzureTable`, `EncodeWalBatch_AzureTable_Zstd`),
the replication ship-envelope microbenchmarks (`Ship_TypedEnvelope`,
`Ship_FramingOnly`), and a `Noop` control that measures the harness's own
floor.

#### Opt-in suites

The default run drives the cluster-shaped tree workloads above. The same
harness also carries a set of narrower, cluster-free suites, selected with
`BENCH_MICROBENCH_SUITE` (or `--suite`); each one replaces the default suite
for that run rather than adding to it. Most isolate one optimisation, running
the prior shape against the shipped one so the delta is measurable without a
silo, a transport or a storage provider in the loop. The dispatch in
`benchmark/host/Bench.Microbench/Program.cs` is the authoritative list.

| Suite | Covers |
|---|---|
| `observer` | The replication observer's per-commit producer-side work on every locally originating, replication-eligible write. |
| `authdecision` | The warm authorization decision path (`PolicyEvaluator.Evaluate`). |
| `hotpath` | Three steady-state allocation trims on grain hot paths: the shard root's batch-write guard, the batched CRDT receiver fold's ambient scope, and the atomic-write saga prepare's touched-shard set. |
| `hashalloc` | View-maintenance UTF-8 hashing allocation trims (`AggregationRowCodec.Slot`, `AggregationApplier.OperationId`, `ViewMaintainerGrain.ComputeTreeDigestAsync`). |
| `identitydigestalloc` | Three SHA-256 identity-digest allocation trims: the credential-cache key's metadata digest, the Explorer cookie credential's revocation identity, and the content hash of every backup artifact. |
| `repocontexthashstaging` | Four repository-context hash-staging allocation trims: the per-source identifier a vector membership or coverage probe derives, the per-file content hash the context bundler computes, the per-unit reuse receipt and possession tokens, and the declaration digest taken for every symbol extracted during ingest. |
| `ordedup` | Observed-remove reconcile paths - OR-Set live-dot counting and remove de-duplication, OR-Map live-entry counting, and the flag family's disable/enable de-duplication - plus the cost of `BoundedRegister`'s deep-copy clone and candidate measurements for dot-equality order, set construction and the byte-sequence tie-breaker comparison. |
| `mergefold` | The CRDT merge fold: folding an incoming dot delta into an accumulated dot list. |
| `catalog` | Tree-catalog enumeration: per-page and full-pagination cost of `LatticeStateQuery.ListTreesAsync`. |
| `rowcodec` | The aggregation-view row codec's encode and decode paths on the projection write and read path. |
| `replayadmission` | The WAL replay-permit admission gate: the activation-time admission decision every replaying leaf passes (healthy, saturated and stale-mean-expired queues, the last exercising the freshness test on its smoothed-wait arm), the wait fold that feeds that mean, and background starvation-drive admission for a WAL GC sweep drive, a coverage-lag timer drive and a refused drive. |
| `replayownership` | The leaf WAL replay pass-1 ownership gate every replayed record passes: the pre-#3601 `ShouldApply` judgement against `ShouldApply` plus the disjoint range-delete check, over a key-scoped `Set`, a disjoint range delete and an overlapping one. |
| `fanout` | Three read sites that replaced N sequential awaited grain reads with one batched multi-get. Prints a host-independent round-trip census first, also written to a `fanout-roundtrips.json` sidecar; set `BENCH_FANOUT_ROUNDTRIPS_ONLY=true` to skip the latency pass. |
| `crosstree` | The allocation trim to the string sets the cross-tree and view coordination barriers canonicalise on every call. |
| `alloctrims` | Three steady-state allocation trims on warm dictionary and set maintenance paths. |
| `viewdrain` | The view-maintenance drain-classification trims: classifying a drained batch without extra passes over its buffer. |
| `aggiter` | The aggregation applier's direct iteration over freshly materialised dictionaries. |
| `aggfold` | Three trims on the aggregation view's per-contribution fold path: materialising the min / max / set-union inverse rows and the custom-fold rows without building a per-shard dictionary, and composing the operation id straight into UTF-8. |
| `aggsplice` | Three trims on the aggregation view's per-contribution read-modify-write path: splicing one entry into an inverse or fold shard row in place rather than decoding and re-encoding the whole row, and reading a membership row's head without decoding its member. |
| `aggfused` | Three view-maintainer drain-path trims: fusing a same-group inverse or fold re-contribution into one shard-row splice, and keeping a durable-history row's original bytes when reshaping it changes nothing. |
| `aggblock` | The block-copy fast path in the aggregation row splices: the entries either side of the spliced key are copied in at most two blocks instead of re-walked entry by entry, swept over shard sizes with a duplicate-key control. |
| `aggkey` | Source-key handling in the aggregation row splices: sizing and writing an added entry from the already-transcoded key bytes instead of scanning and re-encoding the key again. |
| `aggfoldwrite` | The membership write a custom-fold contribution now skips when the stored row is already byte-identical, counted per contribution as well as timed. |
| `aggshardgather` | The aggregation applier's unsharded gather: reading a group's single slot row directly at the default fanout of 1 instead of through a one-key batched read. |
| `viewmaint` | Three allocation trims on the materialised-view maintainer's warm cross-tree and batch-coalesce paths. |
| `queryproj` | Three allocation trims in the grain-index query executor and the state API's metrics observer. |
| `readpathtrims` | Three steady-state read-path allocation trims (view listing, live-entry reads, snapshot reads). |
| `readpathpresize` | Three steady-state read-path result-list presize trims. |
| `draintrims` | Three allocation trims on the view-maintainer drain path and the replication receiver's causal-apply buffer. |
| `fusiontrims` | Three optimisations to the view-maintainer drain fold and the shared metrics sampler's per-tick work. |
| `slotfolds` | Three hash-probe reductions on slot routing, view-maintainer staging and CRDT delta combining. |
| `reshardfolds` | Three hash-probe reductions - the reshard coordinator's slot histogram, the tenancy record's CRDT merge, the WAL GC's durable-pin union - plus the allocation floor of the WAL GC's offset-plane census. |
| `batchfolds` | Three batch-path reductions: per-shard fan-out bucketing for multi-key reads and writes, the cross-leaf snapshot baseline union, and WAL batch-append partition grouping. |
| `slotgroupfolds` | Three remaining physical-shard partitioning paths (slot grouping, owned-slot lists, bulk fan-out), each "after" lane calling the real production method. |
| `fanoutslots` | Three physical-shard fan-out sites the dense-partitioning sweep had not reached: saga prepare, bulk load and restore. |
| `replicationtrims` | Three allocation reductions on the replication ship-to-apply pipeline (vector-clock encode and decode, the content manifest, the receiver's LRU). |
| `replicationapplytrims` | Three reductions on the replication batch-apply path. |
| `authcompiletrims` | The authorization policy compile path: the whole-ruleset snapshot rebuild the warm decision reads. |
| `tenancycompiletrims` | The tenancy snapshot rebuild over every tenant record. |
| `tenantgatesnapshot` | The steady-state tenant gate paths that answer from the compiled tenant-policy snapshot (auth-gate owned-tree and cross-tenant decisions, the replication isolation gate snapshot hit, and the authority check itself). |
| `ingestapplytrims` | Three reductions on the steady-state reconcile planning and sequential replication-apply paths. |
| `crdtcoalescetrims` | Three allocation reductions on the replication shipper's pre-ship CRDT delta coalescing. |
| `crdtrunfolds` | The complexity of folding a key's run of same-key deltas in the pre-ship CRDT coalescer; read it as a curve. |
| `coalescedefertrims` | Deferred typed-delta deserialisation in the coalescer's first pass, and the grain-index predicate lowering's single-conjunction fast path. |
| `grainindexquerytrims` | Three allocation reductions on the grain-index query path: the AND-intersect pass, the per-property accumulators, and the interval algebra. |
| `grainindexplanfolds` | Output-identical folds on the grain-index plan-and-execute path: reading a comparison's constant side without invoking the expression compiler, distributing AND over OR in place, de-duplicating a union's grains through a span probe, and a non-allocating walk for the planner's parameter-reference check. |
| `applymergefanout` | Three physical-shard fan-out sites that partition a batch into a richer-than-list per-shard slot (replication apply merge, tree merge, saga backstop). |
| `terminalpendingtrim` | Two core accumulator trims: the saga terminal fan-out's per-leaf grouping, and the leaf's per-read pending-key union while a saga is in flight. |
| `statetrims` | Allocation trims on the state API's catalog ordering and metrics delta tick, and on the shard root's raw batch-read bucketing. |
| `stateorder` | The state API's bounded catalog page selection and remaining catalog sort, plus the shard-summary ordering the shared metrics sampler runs on every tick. |
| `applygatetrims` | Three allocation reductions on per-operation paths: the receiver's parallel-apply plan, durable-pin bucketing, and the tag-index tag-set reconcile on every tag-carrying write. |
| `alloctrio` | Three allocation reductions on repeatedly executed paths: the shared metrics sampler's per-tick aggregate map, the tenant-usage snapshot compile behind write admission, and the shard-report ordering of each diagnostics report. |
| `crdtreadtrio` | Three CRDT read-path materialisation trims: the whole-set reads of a grow-only set and a remove-wins set, and the provenance projection behind an entry-level sequence read. |
| `tagrowtrims` | Tag-index membership-row parsing and the aggregation applier's per-shard gathers. |
| `tagindexbatching` | Tag-index round-trip reductions: batched membership-row adds, overlapped removals, a windowed intersection probe, and on the orphan-reconcile and flag-mode add paths a windowed orphan confirmation, overlapped orphan deletes and an overlapped flag-mode enable wave, plus the atomic flag-mode commit's batched row-state read and the covered-marker self-heal's batched marker write. |
| `decisionstamp` | The replication cross-tree decision stamper's per-tree sequence issue and confirm, issued concurrently rather than as P serial grain calls each. |
| `viewrebuildfanout` | Three corpus-sized round-trip reductions: the view-generation clear, the shard purge's internal-node sweep, and the view rebuild's source read. |
| `bulkloadfanout` | Three structural round-trip reductions where a loop awaited one grain call before issuing the next: the bulk-load leaf chain, the shard purge's internal-node pre-walk, and the write-fence fan-out. |
| `fanoutcollapse` | Three serial grain-call chains collapsed so a batch no longer pays one round-trip per item. |
| `roundtripwaves` | Three serial grain-call chains: per-partition WAL head probes, the backup-restore per-shard drain, and the group-atomic set cutover. |
| `partitionwaves` | The three remaining serial per-target waves: the per-partition source-head HLC scan, the producer-designation probe, and the orphan-shadow purge. |
| `batchhoisttrims` | Three per-entry costs a batched write or read wave paid for a batch-invariant result: batch event publication, the WAL route task shape, and a leaf-cache double probe. |
| `condsetmanyadmission` | The declared-span admission step at the front of the conditional batch write path. |
| `orphanedsurvey` | A shard's orphaned-leaf audit against its opt-in full survey, on one 128-key orphan. |
| `blockedcensus` | WAL GC on a blocked tree holding more pins than the diagnostic's eight-id cap: the residual scan the uncapped census needs. |
| `sharetouch` | The launch cost of one WAL GC reactivation pass's touches: the earlier start-every-touch-at-once fan-out against the share-bounded runner, at an unnarrowed width and at a share of three. |
| `detachedtransfer` | Detached-leaf split transfer planning, dictionary construction and donor removal. |
| `leafgetmany` | A leaf multi-get with and without a committed prepared override, resolved against one fixed registry view. |
| `leafrangeread` | Leaf key and entry range reads with and without a prepared transactional write in the range, so the cost of the signal that stops a reused scan page serving a stale transactional outcome is visible against the steady-state read. |
| `leafrangedelete` | A four-key foreground range delete on a freshly attached 2,048-row leaf, the path that now reads and hydrates only the requested range. |
| `crdtcoveragecollapse` | Three observed-remove dot-coverage tests that collapse a single-replica cancelling-dot list to its highest counter: the OR-Set and RW-Set live-member projections and the OR-Set folded-state decode. |
| `crdtdotscantrims` | Three trims on the observed-remove dot primitives: span scans instead of list indexing, liveness reads that stop at the first surviving dot, and the merge-time compaction sweep. |
| `crdtapplyprobetrims` | Three per-call trims on the CRDT delta-apply path: the capturing sizing lambda in the delta-run fold, the MV-register duplicate-dot probe returning an index instead of a nullable entry, and the OR-Flag / RW-Flag dot union walking a span. Read the fold group for allocation and the other two for time. |
| `crdtapplywalktrims` | Three trims on the CRDT delta-apply path: replacing boxed `IReadOnlyList<T>` delta walks with span walks, hoisting the OR-set alternate lookup out of the per-dot loop, and pre-sizing the RGA insert batch and dot index. Read the first and third for allocation, and the second for time. |
| `crdtdeltauniontrims` | Two CRDT delta-apply trims: span-backed flag delta dot unions, and one pooled rental across an OR-Set / RW-Set delta dot keying walk instead of one rental per element. |
| `crdtdecodewalktrims` | Three provenance-decode read-path trims: the version-vector current-value projection keeps keys and clocks together in a pooled sorted window, OR-set / sequence / grow-set delta decoders walk spans instead of interface indexers, and `Rga.ToList` walks the cached backing list instead of a read-only wrapper. |
| `gsetdecodetrims` | Grow-only-set value and provenance decode projections from exactly-sized key windows instead of iterator and list walks. |
| `crdtmergelivenesstrims` | CRDT liveness and merge trims plus frame-backed leaf bisect planning: presized bisect result lists, span dot probes, and one cover-span resolution per walk. |
| `crdtlivenessgatetrims` | CRDT read and merge gates: OR-Map any-live checks, incoming-side dedup sizing, and G-Set merge / clone source-copy handling. |
| `scratchkeywindowtrims` | Pooled scratch key windows at three sort-and-discard sites: OR-set / RW-set folded-state decode, OR-set / RW-set current-value decode, and the single-tree and cross-tree atomic fingerprint paths. Read it primarily for allocation, with low-width controls for rental overhead. |
| `leafdigestscantrims` | Three per-element trims on the leaf read, digest and bisect paths: the range enumerator retiring its lower-bound test once satisfied, the digest transcoding each string field once, and a single-replica vector clock fed without a pooled sort. |
| `leafboundhoistdotspan` | Three read-path trims, each with a baseline, an optimised and a no-gain control lane: the leaf range scan dropping per-row bound re-tests its window already enforces, the key range read sorting only when fresh pending keys were appended, and the provenance decoders' dot scans walking spans instead of the list indexer. |
| `crdtprovenancedecode` | Three CRDT provenance-decode paths an entry-history read runs for every revision: the multi-value register's delta decode and current-value projection, and the OR-Map's folded-state decode and key projection. |
| `ormapfilterhoisttrims` | Three per-item costs: view-projection filter eligibility settled once per projection, the OR-Map delta key surrogate encoded once per dot group, and the OR-Map live-key tombstone test. |
| `historyreadtrims` | Three per-row read-path costs: predicate JSON validation deferred until a row evaluates true, fast-path eligibility settled once instead of per row, and the entry-history per-revision delta wrapper. |
| `dataapicrdtreads` | The three whole-collection CRDT projections the data-plane API runs on every read: an OR-Set, an OR-Map and a remove-wins set. |
| `ormapdotspantranscode` | Three read-path trims, each with a baseline, an optimised and a no-gain control lane: the OR-Map provenance decoder's dot scans walking spans instead of the list indexer, the same change on the OR-Set and RW-Set decoders' state-decode emit loops, and two remaining double UTF-8 transcodes of one string on a routing hash and a cache key map. |
| `ormapkeyorderfoldbox` | The CRDT decode and delta-fold trims: the OR-Map state decoder sorting its distinct keys instead of every event, with and without a pooled per-key scratch, and the boxed enumerators the delta coalescing fold paid per member per delta. |
| `rowtranscodecopytrims` | Three trims on the aggregation row encoder and the CRDT provenance decoders: the row writer transcoding a short string once instead of twice, the multi-value register's current-value projection without an intermediate list, and the flag provenance's constant UTF-8 conversion replaced by a literal. |
| `tagwritetrims` | Tag-index write-path allocation trims: normalizing tag arrays, row-key construction, and desired-tag reconciliation. |
| `leafsnapshotframetrims` | Three trims on the leaf snapshot frame codec that every bounded hydration reads through, each removing a repeated re-validation of the 24-byte frame header: hydration admission reading the header once instead of once per question and once per row, a lower-bound seek reading it once instead of once per binary-search probe, and a block hydration reading it once per block instead of once per row. Every group carries a control lane where the trim can buy little. |
| `vvpresize` | The vector-clock presize in the leaf snapshot row decoder: filling a version vector sized to the row's already-bounded replica count instead of growing it entry by entry. |
| `pooledreturntrims` | Three pooled-buffer and digest-staging trims: returning a pooled UTF-8 staging buffer after clearing only the prefix it wrote rather than the whole rental, transcoding a repository-context reconciler's chunk operation-id parts straight into one pooled UTF-8 buffer before hashing, and comparing and formatting a file content digest without an intermediate string. |
| `digestappendfolds` | Three trims on the leaf projection-digest write path - folding a row's fixed-width fields into one hash append, folding a length prefix into the same append as its body, and sorting a vector clock's replicas together with their clocks so no per-replica lookup follows - plus clearing only the written prefix of a pooled rental on return. |
| `sortcomparertrims` | The comparison delegate a sort reached through an `IComparer<T>` allocates on every call, against a cached comparison, over ordinal string-key sorts, the CRDT provenance decoders' causal member-change sorts and struct-array sorts; read it for bytes. |
| `walbatchbuffertrims` | The transient WAL record batch buffer the leaf's CRDT batch-apply and two merge-batch commit-log dispatches built per call, against a pooled rental above a size threshold, with a below-threshold control lane. |
| `setkeytrims` | The base64 string the OR-set and RW-set delta accessors built on every staged remove only to probe a dictionary, against encoding into a stack or pooled span and probing through an alternate lookup. |
| `replayscanloop` | The leaf replay scan loop bounded by the exclusive WAL head, which made one extra empty slice read past the newest entry per pass, against bounding it by the newest offset (head - 1). |

```powershell
$env:BENCH_MICROBENCH_SUITE = 'catalog'
./benchmark.ps1 microbench
```

The `catalog` suite reports two independent things. It first prints an exact,
deterministic census of the grain round-trips needed to page a catalog end to
end, sweeping tenant counts 1 / 8 / 64 / 256 against both an unscoped
enumeration and a tenant-scoped one, with visibility enforcement on and off;
that census is host-independent and is also written to a
`catalog-roundtrips.json` sidecar next to the run's `results.json`. It then
runs the BenchmarkDotNet latency arms, which compare the per-entry projection
shape against the batched one over identical captured page partitions, plus an
end-to-end arm driving the real `LatticeStateQuery`. Set
`BENCH_CATALOG_ROUNDTRIPS_ONLY=true` to print the census and skip the latency
pass, which takes a few seconds instead of a few minutes.

### Per-method allocation and CPU profiling

When BDN's `MemoryDiagnoser` says a workload allocates 312 B/op but cannot
say *which call sites contributed those bytes*, the microbench harness can
attach an EventPipe-driven per-method profiler. It dumps managed-allocation
and CPU-sample events for the duration of every `[Benchmark]` method into a
`profile.json` sidecar alongside the run's `results.json`, attributing each
event to the deepest named managed stack frame.

Activate via the `-Profile` parameter on `benchmark.ps1`:

```powershell
# Per-method allocation attribution for the Mixed_70R_30W workload:
./benchmark.ps1 microbench -Workloads '*.Mixed_70R_30W' -Fidelity dry -Profile alloc

# CPU samples:
./benchmark.ps1 microbench -Workloads '*.Mixed_70R_30W' -Fidelity dry -Profile cpu

# Both:
./benchmark.ps1 microbench -Workloads '*.Mixed_70R_30W' -Fidelity dry -Profile both
```

Or set `BENCH_MICROBENCH_PROFILE` directly (see
[`benchmark/scenarios/microbench.env`](../../benchmark/scenarios/microbench.env)).

#### Output shape

`profile.json` example (truncated):

```json
{
  "run_id": "2026-05-12T13-46-22Z",
  "git_sha": "e04b8cf",
  "captured_at": "2026-05-12T13:46:52.4321Z",
  "mode": "alloc",
  "duration_ms": 28412,
  "total_allocations_b": 12345678,
  "total_cpu_samples": 0,
  "top_allocators": [
    {
      "method": "Orleans.Lattice.BPlusTree.Grains.LatticeGrain.SetAsync",
      "module": "Orleans.Lattice",
      "alloc_b": 4194304,
      "alloc_pct": 34.0,
      "samples": 0,
      "samples_pct": 0.0
    }
  ],
  "top_cpu": []
}
```

`top_allocators` is sorted descending by `alloc_b`. `top_cpu` is sorted
descending by `samples`. Both lists are bounded by
`BENCH_MICROBENCH_PROFILE_TOPN` (default 50). Unused lists for the requested
mode (`top_cpu` under `-Profile alloc`, `top_allocators` under `-Profile cpu`)
are emitted as empty arrays so consumers can rely on a stable schema.

#### Caveats

- **Profile runs perturb measurements.** The EventPipe session adds per-event
  stack-walking inside the runtime. A profile-enabled run's `results.json`
  is NOT a valid cohort baseline; only the `profile.json` attribution table is.
  The optimisation workflow treats profile-enabled runs as a one-shot
  diagnostic, not as cohort samples - see the optimisation agent's per-method
  profiling section for the recommended flow.
- **`-Fidelity full` is incompatible** with profiling. That fidelity uses BDN's
  forking toolchain, which spawns one child process per `[Benchmark]`. The
  parent's EventPipe session does not see the child's workload activity, so
  the harness refuses to start the profiler in that mode and writes a warning
  to stderr. Use `-Fidelity dry` or `-Fidelity quick` (both use the in-process
  toolchain).
- **Attribution is to the deepest named managed frame that is not
  measurement-substrate noise.** By default the symbolicator skips
  NSubstitute / Castle mock thunks, BenchmarkDotNet engine frames and
  async-builder plumbing and climbs to the nearest remaining frame, so the
  table names lattice callsites rather than the harness. Framework frames
  such as `System.Buffers.ArrayPool` are not filtered and can still be
  attributed. Set `BENCH_MICROBENCH_PROFILE_FILTER_NOISE=false` to attribute
  to the deepest named managed frame regardless, which is useful when
  diagnosing the harness itself.
- **Pre-seed allocations are excluded** by design. The profiler starts at the
  end of `[GlobalSetup]`, after the multi-thousand pre-seed writes complete,
  so the top-N table reflects in-loop benchmark allocations only.

#### Optional raw .nettrace sidecar

Set `BENCH_MICROBENCH_PROFILE_NETTRACE_PATH` to a filesystem path to also emit
the raw `.nettrace` blob alongside the aggregated `profile.json`. Useful for
post-mortem inspection in [PerfView](https://github.com/microsoft/perfview) or
[dotnet-trace](https://learn.microsoft.com/dotnet/core/diagnostics/dotnet-trace).
When the variable is unset, the raw blob is written to a temp file and deleted
on session stop.

## The `azure-throughput` harness (real Azure Tables)

`azure-throughput` is the out-of-band tier for measuring sustained
write-throughput against a **real Azure Storage account** rather than
Azurite or the in-memory WAL. The local docker-compose scenarios are
reproducible and cheap, but Azurite collapses network RTT and does not
model Azure Tables partition-server behaviour or throttling - so any
throughput claim that needs to back a public number, or any WAL hot-
path optimisation that needs realistic Azure-side latency, runs here.

The harness deploys a single Linux VM with accelerated networking into
Azure. Its committed parameters file defaults to Standard_D2as_v5, the
smallest D-family SKU that supports accelerated networking, and
recommends Standard_D4as_v5 for the 4,000-vehicle rung, which is the
default `-VmSize` of `benchmark/performance-report.ps1`. The producer and silo run
as co-located systemd units; the silo authenticates to a real Azure
Tables WAL via the VM's system-assigned managed identity. A cohort
runner script applies env-var drop-ins, restarts the silo, runs the
producer for the configured duration, then collects the silo and
producer journals plus a per-second VM-level CPU/RSS sampler CSV
under `benchmark/.run/azure-throughput/`.

Entry points:

```powershell
# One-time provision (Bicep + cloud-init + first publish).
./benchmark/azure-throughput/scripts/deploy.ps1

# Inner-loop sync + publish + silo restart on the existing VM.
./benchmark/azure-throughput/scripts/update.ps1

# Single cohort at the default 4,000 vehicles / 5 Hz / 45 s rung.
./benchmark/azure-throughput/scripts/run-cohort.ps1

# Rung sweep across multiple offered-load points.
./benchmark/azure-throughput/scripts/ladder.ps1 -Rungs '4000:5','6000:5','8000:5'

# Deallocate the VM when finished (no compute charges; storage + PIP idle).
./benchmark/azure-throughput/scripts/vm.ps1 stop
```

The same harness also has a multi-silo tier on Azure Container Apps:
`scripts/deploy-aca.ps1` provisions the rig and `scripts/run-cohort-aca.ps1`
runs one cohort at a given silo count. It is normally driven end to end by
`benchmark/performance-report.ps1 -Layer3`; see
[Performance: multi-silo scaling guide](performance-multi-silo.md).
A Layer 3 run can also sweep a rig you provisioned yourself. Provision one
first, using a prefix of three to nine lowercase letters or digits (the form
`-ReuseAca` normalises a prefix to), then point the report at it:

```powershell
pwsh benchmark/azure-throughput/scripts/deploy-aca.ps1 -NamePrefix <prefix>
pwsh benchmark/performance-report.ps1 -Layer3 -ReuseAca <prefix>
```

`-DryRun` re-renders the published tables from the last `state.json`
without touching Azure. For a Layer 3 replay,
pass the switch form, `-DryRun -Layer3`: the dry run checks that switch
rather than the resolved layer, so `-Layer 3 -DryRun` replays the
single-silo tables instead of the multi-silo ones.

The harness is **not** driven through `./benchmark.ps1` and does not
push to the local history VictoriaMetrics stack - the result is the
`[silo] FINAL ops=... failed=... elapsed=...` line in the silo
journal plus the headline summary block `run-cohort.ps1` prints to
stdout. Cohort sampling methodology, the full `BENCH_*` saturation-
knobs catalogue, the A/B procedure for WAL optimisations, the VM-SKU
sizing rule, and the cost / auto-shutdown story all live in
[`benchmark/azure-throughput/README.md`](../../benchmark/azure-throughput/README.md)
so they are not duplicated here. The empirical WAL-side findings that
inform the current shipping defaults (and the storage-account-
throughput envelope above which raising `WalMaxPendingBatches`
stops helping) live in
[WAL Tuning](wal-tuning.md).

## Where to go next

- [`benchmark/README.md`](../../benchmark/README.md) - topology, calibration,
  every scenario knob, auto-discovery internals, history-stack data model.
- [`benchmark/benchmark-scenarios.md`](../../benchmark/benchmark-scenarios.md) -
  authoritative scenario plan with every knob enumerated.
- [`benchmark/history/README.md`](../../benchmark/history/README.md) - long-lived
  trend-dashboard stack, label schema, ad-hoc PromQL query path.
- [`benchmark/azure-throughput/README.md`](../../benchmark/azure-throughput/README.md) -
  real-Azure-Tables single-VM harness: topology, knobs, A/B procedure,
  auto-shutdown safety net.
- [`docs/lattice/wal-tuning.md`](wal-tuning.md) - how `WalMaxPendingBatches`
  and `WalPartitions` interact with a durable backend's throughput envelope.
- [`docs/lattice/metrics.md`](metrics.md) - what every `orleans.lattice.*`
  meter measures and what regression each tile catches.