# Isolated cold-start and scale rig

A committed, repeatable rig that measures **how long a RepoContext box takes to
serve its first semantic query after a restart**, on a restored copy of real
durable state, and inspects that durable state offline.

It exists because of decision **D10** of epic
[#1830](https://github.com/NSTA1/Orleans.Lattice/issues/1830): no change in that
epic is accepted on a "looks faster" basis. Every performance claim is a
measurement taken here. Sub-issue: [#1838](https://github.com/NSTA1/Orleans.Lattice/issues/1838).

## Isolation is structural, not careful

Decision **D11**: the rig must be *incapable* of touching a live deployment, not
merely careful about it. Four identities are kept separate, and a fail-closed
guard refuses to start when any of them is violated:

| | Live deployment | This rig |
|---|---|---|
| Compose project | `repocontextcontainer` | `lattice-coldstart` |
| Data volume | `repocontextcontainer_repocontext-data` | `lattice-coldstart-master` (pristine) / `lattice-coldstart-work` (per run) |
| Model-cache volume | `repocontextcontainer_hf-cache` | `lattice-coldstart-hf` |
| Host image tags | `repocontext-mcp:local` | `repocontext-mcp:coldstart-rig` |
| Host port | 8080 | 18080 |

On top of the naming, four structural properties:

- **Nothing is ever built.** `docker-compose.rig.yml` has no `build:` section
  anywhere. The rig applies its own **additional** tag to an image that already
  exists, so it can never rebuild an image or move a live tag.
- **The stack cannot bind the pristine master.** Only the per-run working clone
  is mountable, so a run can never mutate the baseline that makes two runs
  comparable.
- **Every bind mount is read-only.** The guard refuses a writable one.
- **The guard runs twice.** Once over the configuration (operator intent), and
  once over the compose document **Docker actually resolved** - which is what
  would really be bound, after all interpolation and merging.

Prove it to yourself at any time:

```powershell
./scripts/rig.ps1 guard
```

The guard's refusals are covered by the regression suite, including one test per
live identity at both layers:

```powershell
pwsh -File ./scripts/Test-RigHelpers.ps1
```

## Prerequisites

- Docker with Compose v2, and PowerShell 7.
- The two source images already built on the box (the rig only re-tags them):
  `repocontext-mcp:local` and `repocontextcontainer-embedder:latest`. Override
  the sources in `parameters.local.ps1` if yours are named differently.
- A **volume backup tarball** (below).
- `WorkspaceRoot` in the parameters pointing at a directory that contains a
  folder named after the repo id in the restored corpus (default: a directory
  named `lattice`). It is mounted read-only at `/workspace`, exactly as the live
  sample does it, so the restored corpus's registered repository path still
  resolves.

## Files

| Path | Purpose |
|------|---------|
| `docker-compose.rig.yml` | The isolated stack. No build sections, external volumes, rig-only image tags, non-8080 host port. |
| `census-expectations.json` | Known-answer figures for the offline census, quoted by the epic from a specific backup. |
| `sql/*.sql` | The offline grain-state census queries. Committed files, never shell-assembled SQL. |
| `scripts/parameters.ps1` | Default parameters and the isolation contract. |
| `scripts/parameters.local.ps1` | **Gitignored** operator overrides. |
| `scripts/_rig-helpers.ps1` | Pure helpers: config, the isolation guard, the file-WAL framing walk, statistics, log counters. |
| `scripts/_rig-docker.ps1` | Docker, HTTP and stateless-MCP helpers. Every binding operation runs the guard first. |
| `scripts/Test-RigHelpers.ps1` | Regression suite for the guard and the parsers. No Docker, no wall-clock dependence. |
| `scripts/prepare-master.ps1` | Restores a backup tarball into the pristine master volume and applies the rig image tags. |
| `scripts/run-cohort.ps1` | **The one-command run.** Clones the master, runs the restart scenarios, emits `cohort.json`. |
| `scripts/inspect-state.ps1` | Offline durable-state census, with known-answer validation. |
| `scripts/generate-corpus.ps1` | Synthetic scale mode: generate, index and promote a corpus well beyond live size. |
| `scripts/rig.ps1` | Day-to-day helper: `guard`, `tag`, `up`, `down`, `status`, `logs`, `mcp`, `clean`. |

Run artefacts land under `benchmark/.run/coldstart-rig/`, which is gitignored.

## Taking a backup

This is the only step that reads a live volume, it is read-only, and the rig
never does it for you - you run it deliberately, with the live stack up or down:

```powershell
docker run --rm `
  -v repocontextcontainer_repocontext-data:/data:ro `
  -v C:\dev\lattice\.deploy:/backup `
  busybox tar -cf /backup/volume-backup-2026-08-29T1000.tar -C /data .
```

Keep the tarball outside the repository. `.deploy/` is untracked and nothing
from it is ever committed.

## Restoring it

```powershell
cd benchmark/coldstart-rig/scripts
./prepare-master.ps1 -BackupTarball C:\dev\lattice\.deploy\volume-backup-2026-08-29T1000.tar
```

`prepare-master.ps1` is idempotent and does three things:

1. Extracts the tarball to a host staging directory, which is what the offline
   census walks.
2. Loads the same tarball into the rig's **master** volume by untarring it
   inside a throwaway container, so the many small files are written into the
   Docker VM's own filesystem rather than dragged across a host bind mount.
3. Applies the rig's additional image tags.

The master is written once and then only ever cloned. Re-run with `-Force` to
rebuild it from a newer tarball.

## Running a cohort

```powershell
./run-cohort.ps1 -Runs 2 -CohortId baseline
```

Each run clones the master to a fresh working volume, then walks three restart
scenarios in order. They differ materially, so a rig that tested only one would
mislead:

| Scenario | What it does | What it models |
|---|---|---|
| `first-boot` | `compose up` on a freshly restored volume | The very first activation, with no snapshot from a clean shutdown |
| `graceful-restart` | `compose stop` (SIGTERM plus a drain window), then `start` | A planned restart, where shutdown captured a snapshot |
| `sigkill-restart` | `docker kill -s KILL`, then `start` | Container recreation and out-of-memory: no drain, so the snapshot is whatever the last periodic capture left |

Useful flags:

- `-Runs <n>` repeats the whole cohort from a freshly cloned master. Two or more
  runs give the run-to-run spread that says whether the rig is the noise source.
- `-Scenarios first-boot,sigkill-restart` restricts the scenario set.
- `-MasterVolume <name>` measures a different pristine master (for example a
  synthetic scale master).
- `-RepoId` / `-SemanticQuery` change the workload.
- `-SkipClone` reuses the existing working volume (fast smoke test; **not**
  comparable between runs, because the durable state has moved on).
- `-SkipWarmup` skips the discarded warm-up activation (see below).
- `-KeepUp` leaves the stack running afterwards.

### The warm-up phase, and why it exists

The embedding companion loads its model into memory on first use, which on a
cold container is a minute or more. On a `first-boot` scenario that load lands
**inside** the measured window and dominates the run-to-run spread: an early
two-run cohort taken without it produced 208 s and 152 s for the same scenario
from the same master, a 31 percent spread that would swamp most of the deltas
this epic is trying to attribute.

So a cohort pays it once, up front, on a throwaway activation whose numbers are
discarded, and then keeps the **embedder container alive for the whole cohort** -
only the repocontext container is removed and recreated between runs. Pass
`-SkipWarmup` if you specifically want the embedder cold-load inside the
measurement.

Every timing is measured from the container's own `State.StartedAt` as reported
by the Docker daemon, never from when a compose CLI call returned, so CLI
overhead never lands in a headline number and all three scenarios share one zero
point.

### Background indexing is quiesced by default

The rig sets the self-index tick, reconcile interval and full-walk interval to a
day, so a continuous background reconcile neither competes for CPU with the
measured path nor writes to the working volume mid-run. That is what makes two
runs from the same master comparable. Set
`RIG_SELFINDEX_TICK_SECONDS=5` and `RIG_RECONCILE_INTERVAL_SECONDS=5` in the
environment to reproduce the live cadence when the background indexer is itself
the thing under test.

## Reading the output

A cohort writes `benchmark/.run/coldstart-rig/cohorts/<cohortId>/cohort.json`
(and copies it to `cohorts/cohort-latest.json`), plus a per-scenario container
log and `docker stats` CSV.

Top level:

| Key | Meaning |
|---|---|
| `schemaVersion` | Result schema version. Currently `1`. |
| `kind` | `coldstart-rig/cohort`. |
| `cohortId`, `generatedUtc` | Identity of the run. |
| `hostContext` | What the host looked like: `dockerCpus`, `dockerMemoryBytes`, `runningContainers`, `foreignContainers`, `foreignContainerNames[]`, and `contended`. A cohort taken alongside unrelated containers is still valid, but its spread is the **host's** floor, not the rig's - read this before believing a spread figure. |
| `configuration` | Project, port, images, volumes, repo id, query, scenarios, run count. |
| `runs[]` | One entry per run, each with `scenarios[]`. |
| `summary[]` | One entry per scenario, aggregated across runs. |

Per scenario (`runs[].scenarios[]`):

| Key | Meaning |
|---|---|
| `scenario`, `runIndex` | Which scenario, which run. |
| `containerStartedAtUtc` | The zero point every elapsed time below is measured from. |
| `liveSeconds` | Seconds from start to the first 200 on `/health/live`. |
| `readySeconds` | Seconds from start to the first 200 on `/health/ready`. |
| **`firstQuerySeconds`** | **Headline 1.** Seconds from start until the first semantic query returned *successfully, in any retrieval mode*. It is the first tool call issued against the activation, so nothing else has warmed the retrieval path. |
| **`firstSemanticQuerySeconds`** | **Headline 2.** Seconds until a query first answered with `mode: semantic`. `null` when the box never answered semantically within the budget. |
| `semanticAchieved` | Whether the semantic path ever answered in this scenario. |
| `firstQueryAttempts`, `firstQueryDurationMs`, `firstQueryOk`, `firstQueryError` | How the headline was reached. |
| `retrievalMode` | The path that answered the FIRST query: `semantic`, `keyword` or `empty`. |
| `warmQueryMs` | `count`, `min`, `p50`, `p95`, `max`, `mean` for the warm repeats, plus the distinct `modes` they answered in. |
| `listReposMs`, `listReposOk` | A `repocontext_list_repos` call, taken **after** the headline so it cannot warm it. |
| `quiesced`, `quiesceSeconds` | Whether the box settled to consecutive fast answers before this scenario handed over to the next, and when it did. A scenario is chained (the graceful stop and the SIGKILL act on whatever the previous scenario left behind), so handing over a settled box is what stops one run's leftover activation work from landing in the *next* scenario's headline. |
| `peakCpuPercent`, `peakMemoryBytes` | Peak for the host container. |
| `resources[]` | Per-container peak and mean CPU and memory. |
| `logCounters` | `ReplayOverBudgetWarnings`, `ProjectionStaleFailures`, `DroppedMessages`, `CursorPublishFailures`, plus warning/error/total line counts. |
| `logPath`, `statsPath` | The raw log and sampler CSV for the scenario. |

Per scenario summary (`summary[]`):

| Key | Meaning |
|---|---|
| `samples` | How many runs contributed. |
| `firstQuerySeconds[]` | The raw headline samples. |
| `firstQuerySecondsMin` / `Max` / `Mean` | Headline aggregates. |
| `firstQueryRelativeSpreadPct` | `(max - min) / mean * 100`. **This is the comparability figure**: a sub-issue can only attribute a delta larger than this spread. |
| `firstSemanticQuerySeconds[]`, `firstSemanticQuerySecondsMean`, `firstSemanticRelativeSpreadPct` | The same for the semantic path specifically. |
| `semanticAchievedCount` | How many of the samples ever answered semantically. |
| `readySecondsMean`, `readyRelativeSpreadPct` | The same for readiness. |
| `retrievalModes[]` | Distinct retrieval paths seen. |

Diff two cohorts by comparing `summary[].firstQuerySecondsMean` for the same
scenario, and only call a change real when the delta exceeds
`firstQueryRelativeSpreadPct`.

### Two headline numbers, not one

`repocontext_search` answers with `mode: keyword` when the semantic path
**throws**, and on a cold, shattered vector tree the exact-kNN prefix scan can
exceed the Orleans response timeout and do exactly that. A rig that recorded
only "a query succeeded" would report a fast, healthy-looking number for a box
that never answered semantically at all - which is precisely the silent
degradation this epic exists to stop. So the rig records both, and after the
first successful answer it keeps re-asking for up to `SemanticRetryBudgetSec`
so "never became semantic" is a recorded fact rather than an artefact of having
stopped asking.

### Run it on a quiet box

Cold start here is CPU-bound and the measured window is tens of seconds, so
other containers competing for cores widen the run-to-run spread and can be
enough on their own to push the cold semantic path past its internal timeout.
Stop unrelated stacks before a cohort you intend to attribute a change to, and
always read `firstQueryRelativeSpreadPct` before believing a delta.

The rig does not merely warn about this - it **records** it. Every cohort
carries a `hostContext` block naming the unrelated containers that were running,
so a spread taken under contention is visibly the host's floor rather than the
rig's, and `run-cohort.ps1` prints a note when it detects any.

## Offline state census

```powershell
./inspect-state.ps1
```

Reads durable state with the stack **down**, so a state question needs no cold
start and the measurement cannot perturb what it measures. It reports per-tree
and per-shard WAL sizes, WAL data/commit/trim record counts (by walking the
file-WAL framing described in `FileWalRecordFormat`), per-tree leaf counts,
leaf-snapshot rows and bytes per key prefix, per-partition projection
checkpoints, and grain-state size by grain type.

Output: `benchmark/.run/coldstart-rig/census/census-<stamp>.json` plus
`census-latest.json`. Every figure also lands in a flat `metrics` map so two
censuses can be diffed by key:

| Metric key | Meaning |
|---|---|
| `wal.segments`, `wal.totalSizeBytes`, `wal.tornSegments` | Segment-file totals. |
| `wal.dataRecords`, `wal.commitRecords`, `wal.trimRecords` | Framing record counts across every shard. |
| `wal.tree.<treeId>.sizeBytes` / `.dataRecords` / `.trimRecords` | Per tree. |
| `grainState.<grainType>.rows` / `.bytes` | Grain-state census. |
| `grainState.databaseSizeBytes` | Size of the SQLite file. |
| `leafCount.<treeId>` | Physical leaves per tree. |
| `leafSnapshot.prefix.<prefix>.rows` / `.bytes` | Snapshot rows and bytes per key prefix (`vpay`, `vec`, `vmem`, `symbol`, `content`, ...). |
| `checkpoint.<treeId>.p<n>.distinct` / `.max` | Distinct projection checkpoints per tree per partition, and the highest. |

The full per-shard and per-partition detail is in `wal.Shards`,
`wal.Trees`, `leafCountsByTree`, `leafSnapshotsByPrefix` and
`checkpointsByPartition`.

### Known-answer validation

The rig is an instrument, so it is validated against an answer arrived at
independently. `census-expectations.json` pins the figures the epic quoted from
`volume-backup-2026-08-29T1000.tar`, and `inspect-state.ps1` recomputes each one
and reports match or mismatch (exiting non-zero on any mismatch). Point the rig
at a different backup and pass `-SkipExpectations`, or replace the expectations
file with figures for that backup.

**The pinned figures are the PRE-epic baseline.** They describe a deployment
whose WAL had been trimmed once, ever (559,455 data records against exactly 64
trim records, one per shard). Epic #1830 is deliberately changing that: S2
(#1832) moved the first WAL GC pass from 30-60 minutes after start to 15-30
seconds, and made the cadence backlog-driven. So a census taken on a volume
captured from a **post-S2** image will legitimately report a higher trim count
and `DIFFER` on that check.

That is the measurement working, not the rig breaking. When inspecting a
post-change volume, pass `-SkipExpectations` and compare `wal.trimRecords` and
`wal.tree.<treeId>.trimRecords` against the pre-change census instead:

```powershell
./inspect-state.ps1 -SkipExpectations -StagingPath <post-change-staging-dir>
```

### Measuring WAL reclamation

Do it from **durable state, not telemetry**. The RepoContext container exposes
no metrics endpoint, and it is not going to gain one: epic decision D5 requires
every mechanism to be default-on so an existing deployment heals with no
operator action, and S13's acceptance criterion is that the host picks up every
mechanism with no change to its compose file or environment. Adding an exporter
or environment plumbing for `LatticeOptions` would invert that promise, so the
whole `orleans.lattice.wal.gc.*` family is unreachable from any external
instrument by design.

The offline census is the stronger instrument for this question anyway: it
parses the file-WAL framing and counts Trim records per shard on a restored copy
of the volume, so it measures what is actually on disk and cannot be defeated by
a missing exporter, a sampling window, or a dropped metric. It is what
established the "64 trims, one per shard, ever" figure in the first place.

`-SkipWal` skips the framing walk, which is the slow half (roughly 100 seconds
for 105 segments / 559k records). That walk **streams**: it reads each record's
9-byte frame header plus the 8 body bytes it actually needs and seeks past the
payload, so its memory cost is bounded regardless of segment size. Loading
segments whole would be faster today but allocates a large object per segment,
which is the wrong property for a rig whose purpose is to measure much larger
trees.

`-SqliteSource staging|volume|auto` selects where the grain-state database is
read from; the default prefers the master volume, because a Docker volume lives
inside the VM's filesystem and scans far faster than a host bind mount of the
same bytes.

## Synthetic scale mode

The epic exists to support much larger trees, so the rig can build a corpus well
past today's live size (roughly 6,800 files and 73,500 vectors) and measure cold
start there:

```powershell
# 1. Generate a deterministic corpus under the mounted workspace root.
./generate-corpus.ps1 -Stage generate -Files 30000 -SymbolsPerFile 12

# 2. Index it into a dedicated scale working volume (this is the slow stage:
#    embedding runs on CPU in the companion container and takes hours).
./generate-corpus.ps1 -Stage index

# 3. Snapshot the indexed volume as a reusable pristine scale master.
./generate-corpus.ps1 -Stage promote

# 4. Measure cold start at that scale, repeatably, without re-indexing.
./run-cohort.ps1 -MasterVolume lattice-coldstart-scale-master -RepoId coldstart-scale-corpus -Runs 2
```

The generator is seeded, so the same `-Files` / `-SymbolsPerFile` / `-Seed`
always produce a byte-identical corpus and two scale cohorts differ only in the
code under test.

## Cleaning up

```powershell
./rig.ps1 down            # stop and remove the rig's containers, keep volumes
./rig.ps1 clean           # also remove the working volume
./rig.ps1 clean -All      # also remove the master, scale master and model cache
```

None of these can touch a live container or volume: `clean` routes every removal
through the same guard, which refuses any name outside the rig's own prefix.
