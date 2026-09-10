# Local deployment runbook

How the tuned, long-lived local RepoContext container deployment is built, pinned,
configured, verified, rolled back, and rebuilt from nothing.

This document exists because the instructions it carries previously did not exist in
any repository. They lived in a single durable agent-memory entry, and the tuned
configuration lived in a single untracked, gitignored `docker-compose.override.yml`.
Both were load-bearing for epic #2368's gate, and neither had review, history, or a
diff. The memory entry was destroyed by an index reset, and nothing failed, nothing
broke, and the container it described kept running. See issue #2609.

## Scope, and what this is not

This is **local operations**. It does not restate what is already documented
elsewhere, and you should read those first:

- [Container quickstart](container.md) - the product's container behaviour: topology,
  durability profiles, health probing, graceful shutdown, and what each setting means.
- [The container sample](../../samples/RepoContextContainer/README.md) - the
  first-run walkthrough, the mounted workspace, the published port, and choosing an
  embedding companion.

What follows is only the part neither of those covers: how *this* deployment is
produced and operated.

### What this runbook does not establish

**Agreement between this runbook and the tracked compose files says nothing
whatsoever about any running container.** This section is not a disclaimer; it is the
single most important thing on this page, and epic #2368's gate runs 1 and 2 both
failed precisely by ignoring it.

`docker compose up` reads the compose files in its **own working directory**,
regardless of which checkout built the image it starts. The image and the runtime
configuration are two independent inputs, only one of which is obviously
version-controlled, and nothing in `docker compose up`'s output names a branch, a
commit, or a directory. So a candidate image can run under a different checkout's
configuration with no sign of it anywhere. In gate runs 1 and 2 the fix was present in
the checkout and absent from the running container, the runs correctly observed the
absence of its effect, and both concluded the fix itself was absent.

No arrangement of checks over tracked files can catch that, because the repository
agrees with itself perfectly throughout. Only a reading taken from the running process
separates "the source does not carry the fix" from "the source carries it and this
container never received it". That reading is the deployment-provenance assertion
added by #2590 / #2592:

```bash
cd samples/RepoContextContainer
pwsh -File ./scripts/Assert-ContainerProvenance.ps1
```

The same limit applies to this runbook's own guard test, which is discussed under
[How this runbook is kept honest](#how-this-runbook-is-kept-honest).

## The two compose files

| File | Tracked | Loaded | Carries |
| --- | --- | --- | --- |
| `docker-compose.yml` | yes | always | topology, durability, workspace mount, published port, cadence defaults |
| `docker-compose.tuning.yml` | yes | only when named with `-f` | the image pin, the tuned cadence, the GC settings, and the CPU and memory grants |

The tuning overlay is deliberately **not** named `docker-compose.override.yml`. An
override file is loaded automatically and silently, so it would change what every
reader of that directory gets from a plain `docker compose up -d`. The tuning file
changes nothing unless you name it, which is what makes it safe to track.

Naming the files explicitly also **suppresses** compose's automatic pickup of any
`docker-compose.override.yml` that happens to exist in that directory. That is
intentional: it is what makes the deployed configuration reproducible from the
checkout alone rather than from one machine's untracked state.

`docker-compose.override.yml` remains gitignored and remains a legitimate personal
escape hatch. If you use one, record what it changes under
[Local-only deltas](#local-only-deltas), or you have recreated the defect this
document exists to close.

## Build and tag from a known sha

The host image is built on the host from the repository root:

```bash
docker build -f .deploy/Dockerfile -t repocontext-mcp:candidate-<sha> \
  --secret id=nugetcfg,src=%APPDATA%\NuGet\NuGet.Config .
```

The build secret is not optional and not incidental: an in-container NuGet restore
fails behind the corporate TLS proxy, so the restore needs the corporate feed from
`%APPDATA%\NuGet\NuGet.Config`. A build that omits it fails during restore, which
reads as a network fault rather than as a missing secret.

Tag with the **commit sha you built**, not a branch name or a date. The sha is the
only tag that can later be checked against a running container by
`Assert-ContainerProvenance.ps1`.

## Pin and roll back

The base compose file declares `build:` and no `image:`, so nothing resolves an image
for `up -d --no-build` on its own. `docker-compose.tuning.yml` supplies the pin, and
the pin is a fixed tag - `repocontext-mcp:local` - which is *moved* between builds
rather than edited in the file. The deploy step is therefore a retag:

```bash
# 1. Preserve whatever `local` currently points at, so it can be restored.
docker tag repocontext-mcp:local repocontext-mcp:rollback-$(date +%Y%m%d-%H%M)

# 2. Move the pin to the candidate you just built.
docker tag repocontext-mcp:candidate-<sha> repocontext-mcp:local

# 3. Deploy. --no-build is what makes the deployed bits the ones you tagged.
docker compose -f docker-compose.yml -f docker-compose.tuning.yml up -d --no-build
```

Step 1 is the whole rollback story, and skipping it is unrecoverable in the sense
that matters: the displaced build is still on the host but is no longer named, so
you cannot say which of the anonymous layers it was. `docker images` on this host
shows the convention held consistently, as `candidate-<sha>` and
`rollback-<yyyyMMdd-HHmm>` pairs created at the same moment. The oldest recorded
rollback point for the ONNX embedder migration is `repocontext-mcp:rollback-20260904-1510`,
recorded alongside a full configuration backup in `C:\dev\rc-ab\backup-live-config\`.

To roll back:

```bash
docker tag repocontext-mcp:rollback-<yyyyMMdd-HHmm> repocontext-mcp:local
docker compose -f docker-compose.yml -f docker-compose.tuning.yml up -d --no-build
```

A rollback does **not** need a re-index in either direction, and it does not touch the
`/data` volume.

### Rolling back the embedder

The ONNX Runtime companion is the committed default. The original Onyx companion
remains available and is selected by layering a third file:

```bash
docker compose -f docker-compose.yml -f docker-compose.onyx.yml up -d
```

Both serve the same contract on the same port and emit numerically identical vectors,
so switching does not invalidate an existing `/data` volume and needs no re-index. The
evidence for that identity is recorded under
[Provenance of the embedder migration](#provenance-of-the-embedder-migration).

## The settings this deployment declares

Every row below is a setting the **resolved** compose document declares when
`docker-compose.yml` and `docker-compose.tuning.yml` are layered. The table is
machine-checked against that document; see
[How this runbook is kept honest](#how-this-runbook-is-kept-honest).

Values are written as an operator writes them. `mem_limit` resolves to bytes and
`cpus` to a bare number, and the guard normalises both before comparing.

<!-- compose-settings:begin -->

| Service | Setting | Value | Why this value |
| --- | --- | --- | --- |
| `embedder` | `EMBED_PROVIDER` | `cpu` | Base default. `cpu`, or `cuda` on an NVIDIA host started with a device reservation. See the [sample README](../../samples/RepoContextContainer/README.md). |
| `embedder` | `DOTNET_gcServer` | `0` | Workstation GC. Server GC allocates a heap and a dedicated GC thread per core, which on a 16-core host is the main driver of resident set for a latency-insensitive background service. |
| `embedder` | `cpus` | `4.0` | Reduced from an unlimited grant that measured 1014% CPU (about 10 of 16 cores) and made the host unusable for interactive work. Leaves 12 cores free. The ONNX intra-op thread pool is derived from this grant (#2610), so changing it changes the pool. |
| `embedder` | `mem_limit` | `5g` | Measured at 4.08 GiB with no limit, and pinned at 2.486 GiB of a 2560m cap (99.4%) while essentially idle at 0.01% CPU, holding the resident ONNX model at its ceiling with no room to work. 5g clears the measured requirement. |
| `repocontext` | `image` | `repocontext-mcp:local` | The base file declares `build:` and no `image:`, so `up -d --no-build` cannot resolve an image without this pin. The tag is moved between builds; see [Pin and roll back](#pin-and-roll-back). |
| `repocontext` | `LATTICE_DURABILITY` | `local` | Base default: SQLite grain storage and reminders plus the file WAL, no external services. See [container.md](container.md). |
| `repocontext` | `LATTICE_DATA_ROOT` | `/data` | Base default. All durable local state on one named volume, so it survives restart, recreation, and image upgrade. See [container.md](container.md). |
| `repocontext` | `LATTICE_MCP_PORT` | `8080` | Base default. The container-side listener port, which must keep matching the container side of the published port mapping. |
| `repocontext` | `LATTICE_WORKSPACE_ROOT` | `/workspace` | Base default. The read-only workspace root; every path passed to `repocontext_add_repo` must resolve under it. |
| `repocontext` | `LATTICE_EMBEDDING_ENDPOINT` | `http://embedder:9000` | Base default. Points the default embedding provider at the companion container on the private network. |
| `repocontext` | `LATTICE_WAL_PIN_BUCKETS` | `8` | Base opt-in. Splits the retention-floor pin state so an advancing floor rewrites a fraction of the blob; measured pin blobs on this box reached about 1.4 MB rewritten tens of thousands of times. Setting it back to `1` is a safe rollback. |
| `repocontext` | `LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD` | `120s` | Base default (#2589). The drain budget the host is given on SIGTERM, kept in step with compose's own `stop_grace_period`. |
| `repocontext` | `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_DIR` | `/memory-archive` | Base default (#2611). Durable agent memory is archived outside the `/data` volume so a `down -v` cannot take it with the code index. |
| `repocontext` | `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_INTERVAL_SECONDS` | `300` | Base default (#2611). How often the memory archive is refreshed. |
| `repocontext` | `LATTICE_SELFINDEX_TICK_SECONDS` | `30` | Tuned from `5`. The self-index tick drives the out-of-band paged sweep. |
| `repocontext` | `LATTICE_RECONCILE_INTERVAL_SECONDS` | `60` | Tuned from `5`. Every pass walks the workspace, so this is the dominant recurring cost and the source of continuous embedder load. |
| `repocontext` | `LATTICE_RECONCILE_JITTER_SECONDS` | `15` | Tuned from `0`. Non-zero jitter stops passes across repositories phase-locking into simultaneous walks, which is what produced the observed load spikes. |
| `repocontext` | `LATTICE_FULL_WALK_INTERVAL_SECONDS` | `3600` | Tuned from `120`. The full re-stat of every file, and the single heaviest operation. Counted in passes, not wall clock, so it moves with the reconcile interval and jitter above. |
| `repocontext` | `LATTICE_EMBEDDING_GAP_SCAN_INTERVAL_SECONDS` | `3600` | Tuned from `300`. Two membership reads per indexed source, so on a converged repository it dominates a pass. Costs no healing latency: an actual gap forces an immediate in-pass scan regardless. |
| `repocontext` | `DOTNET_gcServer` | `0` | Workstation GC, for the same reason as the embedder above. Measured resident set before the limits was 5.56 GiB. |
| `repocontext` | `DOTNET_PROCESSOR_COUNT` | `16` | Pins the reported processor count to the host core count so the WAL replay concurrency gate stays at the value every prior field measurement was taken against. See [The pool-sizing class](#the-pool-sizing-class). |
| `repocontext` | `cpus` | `6.0` | Bounds a runaway without starving normal operation. This service measured about 92% of one core in steady state before any limit, so 6 is headroom rather than a working limit. |
| `repocontext` | `mem_limit` | `12g` | Sized above the measured 10.2 to 10.4 GiB steady-state plateau, on a 55.7 GiB host. A 4g cap set earlier was about 40% of the known requirement; see [How an undersized memory cap presents](#how-an-undersized-memory-cap-presents). |

<!-- compose-settings:end -->

## The pool-sizing class

Three of the settings above exist for the same underlying reason, and naming that
reason as a **class** is more useful than documenting three coincidences: a runtime
sizes a thread pool, a heap count, or a concurrency gate from the **host core count**,
while the kernel enforces a **fractional cgroup CPU grant**. The pool is then
oversubscribed by the ratio between the two, and nothing in any configuration file
says so.

Three instances in this one deployment:

1. **The WAL replay concurrency gate.** `BPlusLeafGrain` sizes a process-wide
   semaphore from `Environment.ProcessorCount` when the option is left non-positive,
   once, as a structural constant. `Environment.ProcessorCount` *is* cgroup-aware, so
   adding `cpus: 2.0` silently shrank that gate from 16 permits to 2 - an 8x cut to
   leaf-activation concurrency, invisible in configuration and unattributable from the
   logs. `DOTNET_PROCESSOR_COUNT: "16"` decouples the two.
2. **The GC heap count.** Server GC allocates a heap and a GC thread per core. Both
   services set `DOTNET_gcServer: "0"`.
3. **The ONNX intra-op thread pool.** ONNX Runtime sizes its pool from the host core
   count and does **not** consult the cgroup quota. Under the 4.0-CPU grant on this
   16-core host that produced a pool of 16: the kernel throttled 296 of 298
   consecutive scheduling periods, and the pool accumulated 346.3 CPU-seconds stalled
   against 118.8 run. The cost is far worse than proportional, because ONNX Runtime
   synchronises intra-op threads at every operator boundary. Observed embedding rate
   was 1.8 files per minute, projecting about 77 hours for one 8,315-file checkout.

Instance 3 is **fixed** as of #2610: `EMBED_INTRA_THREADS` now defaults to the
enforced cgroup quota read from `/sys/fs/cgroup/cpu.max` rather than to
`Environment.ProcessorCount`, and the server logs its provenance (declared, derived
from the grant, or derived from the processor count) at startup. Note the interaction
with instance 1: `DOTNET_PROCESSOR_COUNT` overrides `Environment.ProcessorCount` and
wins over the quota, so an embedder that copied the `repocontext` environment block
would silently restore the 4x oversubscription. That is why the fix reads the quota
directly.

**The point of naming the class is the fourth instance, which has not been found yet.**
When adding a container limit, or a setting that sizes anything per core, check which
figure the runtime actually reads. `Assert-ContainerProvenance.ps1` and the effective
configuration report (#2593, #2600) exist so the answer is read rather than assumed.

## How an undersized memory cap presents

Worth keeping because the symptom points at the wrong subsystem.

A `mem_limit` below the working set does **not** present as a resource event. .NET
sizes its heap hard limit from the cgroup limit and collects harder as it approaches
it, rather than being OOM-killed at it, so there is no container kill, no restart, no
exit code, and nothing in `docker events`. Instead the runtime throws
`System.OutOfMemoryException` inside a Newtonsoft deserialize of a leaf-snapshot blob,
and it surfaces as a **storage** fault:

```text
AdoNetGrainStorage[200416] Error reading grain state:
GrainType=leaf-snapshot ... System.OutOfMemoryException
  at Newtonsoft.Json.JsonTextReader.ReadData(...)
```

Measured on the 4g cap: 2,929 such lines in one multi-hour run, 756 of them within
seven minutes of a cold start, with the container sitting at 3.5 GiB of 4 GiB (88%).

It compounds. A failed leaf-snapshot read means the leaf cannot rehydrate, so it
activates **cold** and replays its whole WAL window, which costs more memory again.
Observed as 259 cold activations across only 64 distinct leaves (4.05x repeat-cold) -
the exact "same few leaves going cold repeatedly" shape that indicates a snapshot or
rehydrate defect, here caused by a memory cap.

## Restart, drain, and verification

`docker compose restart repocontext` is a full recreation: it evicts the in-memory
projection and forces a WAL replay or cold rebuild on next access. That is the point
of running it - it is the durability proof - but it is not free, and it is what the
cold-start rig measures.

On SIGTERM the host flips readiness to not-ready **first**, then drains: the silo
deactivates and the WAL commit log flushes buffered records before exit, within the
`LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD` budget above. Verify in this order:

```bash
# 1. Liveness: process and silo host alive.
curl -fsS http://localhost:8080/health/live

# 2. Readiness: silo joined, activation-time WAL replay done, durable stores proven
#    reachable, MCP serving. 503 during startup replay AND during drain, so a 503
#    immediately after a restart is expected, not a fault.
curl -fsS http://localhost:8080/health/ready

# 3. Provenance: what this container actually received. See the warning at the top.
pwsh -File ./scripts/Assert-ContainerProvenance.ps1
```

A persistent 503 has its own diagnosis section in the
[sample README](../../samples/RepoContextContainer/README.md); do not skip it in
favour of restarting again, because a restart discards the evidence.

## Recover the deployment from nothing

Assumes only a clone and a Docker daemon.

```bash
# 1. Build the host image from the sha you intend to deploy.
docker build -f .deploy/Dockerfile -t repocontext-mcp:candidate-<sha> \
  --secret id=nugetcfg,src=%APPDATA%\NuGet\NuGet.Config .

# 2. Pin it. (Nothing to preserve on a clean host; on an existing one, save the
#    displaced tag first - see Pin and roll back.)
docker tag repocontext-mcp:candidate-<sha> repocontext-mcp:local

# 3. Bring up the tuned stack. The embedder builds from its own small context;
#    --no-build applies to the pinned host image.
cd samples/RepoContextContainer
docker compose -f docker-compose.yml -f docker-compose.tuning.yml up -d

# 4. Wait for readiness.
curl -fsS http://localhost:8080/health/ready

# 5. Register the workspace repositories over MCP (repocontext_add_repo with a path
#    under /workspace), then watch repocontext_index_status until filesEmbedded
#    reaches filesScanned. Until it does, semantic search answers only over the
#    already-embedded slice, so a missing hit is not evidence of missing code.

# 6. Confirm what you deployed.
pwsh -File ./scripts/Assert-ContainerProvenance.ps1
```

Durable agent memory is archived outside the `/data` volume (#2611), so a
`docker compose down -v` destroys the code index but not the captured decisions,
gotchas, and conventions. The code index rebuilds from the working files; memory does
not rebuild from anything.

## Local-only deltas

Anything this host runs that the tracked overlay does not declare belongs here, so
the running configuration always traces to something in the checkout.

| Delta | Why it is not tracked | Retire when |
| --- | --- | --- |
| `Logging__LogLevel__Orleans.Lattice.Api.Mcp.RepoContext.RepoContextVectorWriter: Debug` | A diagnostic for issue #2252 (distinguishing `Disabled` from `NotReturned`, which is reachable only at Debug), not tuning. It ships commented out in `docker-compose.tuning.yml`. | #2252 closes. |

## Provenance of the embedder migration

The ONNX Runtime companion replaced the Onyx companion as the committed default in
PR #2008. The controlled local A/B behind that decision ran over `psf/requests`
(125 files) with the embedder as the only variable, and its raw results are on this
host under `C:\dev\rc-ab\results\`. The claims and their actual sources:

| Claim | Source file | Figures |
| --- | --- | --- |
| Vectors are numerically equivalent | `embedder-ab.json` | 200 chunks. Passage cosine mean 0.9999688, min 0.9953171; query cosine min 0.999999999998 over 20 queries. |
| Retrieval quality is identical | `comparison.json` | MRR 0.7238095 in both arms; hit@1 0.6, hit@10 0.95 in both; same top-1 on 20 of 20 queries; mean top-10 Jaccard 1.000. |
| Query latency improved | `comparison.json` | Median 122.37 ms to 43.72 ms. |
| Embedding throughput improved about 1.32x | `throughput-paired.json` | Paired ratio median 1.3222 over 6 rounds (min 1.2469, max 1.5173). |

**Do not cite `embedder-ab.json` for the throughput or latency claims.** Its own arm
timings are *unpaired* and show ONNX slower per single embed (mean 930.5 ms against
Onyx 317.4 ms; corpus 134.0 s against 103.5 s), because the two arms ran under
different host contention. The paired study exists precisely because the unpaired one
is not decisive. `embedder-ab.json` is authoritative for the cosine parity study and
nothing else.

The single worst parity chunk (0.9953) is in `AUTHORS.rst`, an accented-name case.
That divergence was root-caused - invariant globalization collapsed accented words to
`[UNK]` - and fixed in PR #2008, so it is a record of a resolved defect, not a
standing caveat.

## How this runbook is kept honest

`LocalDeploymentRunbookComposeParityTests` (in
`test/lattice.api.mcp.repocontext/Docs/`) resolves `docker-compose.yml` and
`docker-compose.tuning.yml` with `docker compose config` and asserts, in both
directions, that the settings table above enumerates exactly what that document
declares. A setting added to either compose file without a table row fails the test,
and a table row naming a setting the merge does not actually produce fails it too.

It evaluates the **resolved** document rather than the raw files on purpose: compose
merge and interpolation decide what a setting resolves to, so a raw-file comparison
can be green about a value the merge discards.

**What a green run of that test establishes: that two tracked files agree with each
other. Nothing else.** It does not establish that any container is running, that a
running container was composed from these files, that it is executing an image built
from this checkout, or that any of these limits are in force anywhere. For those, run
`Assert-ContainerProvenance.ps1` against the container itself.
