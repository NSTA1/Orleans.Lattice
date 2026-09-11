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

## What the box points at

Every other setting in this document describes how the deployment *performs*. This
one decides what it is *about*, and it is the only setting whose misconfiguration
leaves every output surface looking healthy while every answer is wrong.

| Setting | Intended value | Resolves to |
| --- | --- | --- |
| `REPO_PATH` | `C:\dev` | the host directory bound at `/workspace`, read-only |
| indexed repository | `/workspace/lattice` | `C:\dev\lattice` |
| `repoId` | `lattice` | the id every agent session queries |

**Set `REPO_PATH` explicitly. Never let it default in this repository.**

### Where the setting actually lives

`REPO_PATH` is **not** in either compose file and never was. It lives in
`samples/RepoContextContainer/.env`, which Docker Compose auto-loads from the
directory it is **invoked from**, on every `up`, regardless of your shell
environment. That file is gitignored. A tracked
[`.env.example`](../../samples/RepoContextContainer/.env.example) carries the setting
and its warning; copy it to `.env` before starting the stack.

This mechanism is the whole of the root cause, and it is worth being precise about,
because the obvious explanation is wrong. Nobody edited a mount. Nobody removed a
setting. The deployment was configured by **two** untracked files with different jobs:
`docker-compose.override.yml`, which pinned the image, and `.env`, which decided what
the box was about. When the stack was re-composed from a git worktree, the override
was copied across and the `.env` was not, so `REPO_PATH` silently fell back to the
compose default.

**The override survived because it is what people think of as "the config."** Its own
header says it has *"one job only: pin the image"*. It was never a configuration
capture mechanism, so copying it felt complete while leaving behind the setting that
mattered most. Note what that means: the setting **was** written down, in a file that
carried a correct and clearly-worded warning about exactly this failure, a month
before it happened. It vanished anyway, because it was written in a file whose copying
was optional.

### The worktree trap

The base compose file declares the mount as:

```yaml
- ${REPO_PATH:-../../..}:/workspace:ro
```

and documents the default as *this repo's parent directory, so this repo is one
registerable child added at `/workspace/<repo>`*. That description is exactly correct
for an ordinary clone at `C:\dev\lattice`: the grandparent of the compose file is
`C:\dev` and the registerable child is `lattice`.

It is **wrong for a git worktree**, and the difference is invisible. Composed from
`C:\dev\copilot-worktrees\lattice\<worktree>\samples\RepoContextContainer`, `../../..`
resolves to the *worktree collection directory*, so the registerable child is the
**worktree's generated name** rather than the repository's. The default silently
changes meaning according to where the compose file is invoked from, and in this
repository every agent session runs from a worktree.

This is not hypothetical. It is the deployment's observed state: `/workspace` was
bound to `C:\dev\copilot-worktrees\lattice` and the indexed root was
`/workspace/bucket4-merge`, a worktree pinned at an older commit. Every structural
record, every ranked search result, and every `repocontext_context` bundle filed under
repoId `lattice` was about that stale worktree rather than about `C:\dev\lattice`.

**The symptom is silence.** `repocontext_list_repos` reports the expected `repoId`, a
plausible file count, and a healthy converging index; searches return `mode: semantic`
with sensibly ranked hits. Nothing in any output surface distinguishes *indexing the
repository* from *indexing a stale worktree under the repository's name*. It also
makes a documented instruction false against such a deployment:
`.github/instructions/repocontext.instructions.md` tells every agent that in a
worktree the repo id is still the base repository's and the base repository is what is
indexed. Where this trap has fired, a worktree is indexed under the base repository's
id, and an agent following the documented rule gets confidently wrong answers with no
signal available to it that anything is amiss.

### None of this prevents recurrence

Stated plainly, because "documented and guarded" reads as "fixed" and it is not:

- **`.env.example` is not prevention.** Compose does not load it. Someone still has to
  copy it to `.env`, and a person who forgets the `.env` will equally forget to copy
  the example. It makes the omission *discoverable by a reader who is already
  looking*, which is precisely the reader this failure does not have.
- **This runbook is not prevention.** It is a description. Nothing consults it at
  `up` time.
- **The guard test is not prevention.** It checks that this document still says these
  words. See [Why the guard test cannot cover this](#why-the-guard-test-cannot-cover-this).

Prevention requires the running system to make the wrong state *observable*, which is
issue **#2617**: report the indexed root on every `repocontext_list_repos` row, log the
resolved workspace root and every registered root at startup, warn when a registered
root's basename differs from its `repoId`, and assert the indexed root from the
running store in `Assert-ContainerProvenance.ps1`. Until that lands, the only defence
is the two commands below, run by someone who already suspects something.

### Verifying it

Neither check reads a tracked file, and that is the point.

```bash
# 1. What the running container actually has mounted.
docker inspect repocontextcontainer-repocontext-1 \
  --format '{{range .Mounts}}{{.Source}} -> {{.Destination}} ro={{.RW}}{{"\n"}}{{end}}'
```

```text
# 2. What the running store believes its indexed root is. Must NOT report
#    "outside the indexed root".
repocontext_changed(repoId: "lattice", path: "/workspace/lattice")
```

The second is the stronger of the two, because it reads the indexed root out of the
running store rather than out of a file. It therefore also catches the case where the
mount is correct but the *registered* root is still the stale one, which a mount check
alone passes.

Both were exercised against this deployment, before and after its repair, and they do
discriminate the states:

| | Broken | Repaired |
| --- | --- | --- |
| `docker inspect` mount source | `C:\dev\copilot-worktrees\lattice` | `C:\dev` |
| `repocontext_changed` on `/workspace/lattice` | refused: *"outside the indexed root of repository '/workspace/bucket4-merge'"* | returns a file list |

The refusal is the useful part, and it is worth reading closely: it names the indexed
root it is comparing against. That is the one place the wrong state is currently
visible, and it is visible only because the call **failed**. Nothing reports it on
success, which is what #2617 addresses.

### Why the guard test cannot cover this

The compose-settings guard described under
[How this runbook is kept honest](#how-this-runbook-is-kept-honest) does **not** check
`REPO_PATH`, and cannot be made to. This is worth stating plainly rather than leaving
as a gap, because it is the third instance in this document of the same class:

- `docker compose config` faithfully reports `../../..`, which is **correct as
  written and wrong in effect**. A parity test over the resolved document would agree
  with the file and miss the defect entirely.
- The resolved bind source is an **absolute, machine-dependent path**, so any exact
  assertion over it fails on every other machine and in CI.
- There is no machine-independent invariant to assert instead. `../../..` genuinely
  *is* the parent of the repository root in both the clone and the worktree case; the
  path arithmetic is correct in both. The defect is that the registerable child is
  named for a worktree, which is a fact about worktrees that no compose file knows.

This is a **declared-versus-effective** case, the same class as `docker stop -t`
overriding a configured `stop_grace_period`. Only a reading taken from the running
container or the running store settles it, which is what the two commands above do.

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

The host image is built on the host from the repository root, from the **tracked**
Dockerfile `apps/repocontext/Dockerfile`. That is the same build input
`samples/RepoContextContainer/docker-compose.yml` declares - `context: ../..`, which
from that directory resolves to the repository root, and
`dockerfile: apps/repocontext/Dockerfile`, which is relative to that context - so
building by hand and building through compose consume the same file with the same
context. Run from the repository root:

```powershell
$env:GIT_COMMIT = (git rev-parse HEAD)
docker build -f apps/repocontext/Dockerfile `
  -t "repocontext-mcp:candidate-$env:GIT_COMMIT" `
  --build-arg GIT_COMMIT=$env:GIT_COMMIT `
  --secret id=nugetcfg,src=$env:APPDATA\NuGet\NuGet.Config .
```

**`.deploy/` is not a build input.** Nothing under it is tracked - `git ls-files
.deploy` returns zero files - so a `.deploy/Dockerfile` exists only on whichever
machine happened to create one, and a second operator, or the same operator in a
fresh clone, cannot build from it at all. An earlier revision of this section named
it. It is not the file compose declares, and the copy that exists on this host
declares no `ARG GIT_COMMIT` and no `LABEL org.opencontainers.image.revision`, so a
build from it cannot stamp the provenance `Assert-ContainerProvenance.ps1` reads -
and `--build-arg GIT_COMMIT` against a Dockerfile that declares no such `ARG` is not
an error, only a non-fatal "one or more build-args were not consumed" warning in the
build log. If you have a `.deploy/` directory, ignore it.

The build secret is not optional and not incidental: an in-container NuGet restore
fails behind the corporate TLS proxy, so the restore needs the corporate feed from
`%APPDATA%\NuGet\NuGet.Config`. A build that omits it fails during restore, which
reads as a network fault rather than as a missing secret.

Both arguments matter, and they are not the same thing. `--build-arg GIT_COMMIT`
stamps the sha **into the image** as `org.opencontainers.image.revision`, which is
written by the build itself and travels with the image wherever it goes. The
`candidate-<sha>` **tag** is assigned by a person afterwards and can be moved, so
it is a fallback rather than the answer. `Assert-ContainerProvenance.ps1` reads the
label first and falls back to the tag, and if neither resolves it **refuses** -
supply at least one. Omitting both leaves the built commit unknowable, which is the
state that cost this gate eleven hours of measurement against the wrong binary
(issue 2686).

Tag with the **commit sha you built**, not a branch name or a date.

### Read the provenance back before you tag or deploy

The build is not finished until you have read the label out of the image it
produced. Do it at the machine that built it, before the retag in the next section:

```powershell
$stamped = docker inspect "repocontext-mcp:candidate-$env:GIT_COMMIT" `
  --format '{{index .Config.Labels "org.opencontainers.image.revision"}}'
if ($stamped -ne $env:GIT_COMMIT) {
  throw "UNPROVENANCED IMAGE: revision label is '$stamped', expected " +
        "'$env:GIT_COMMIT'. Re-build with GIT_COMMIT exported. Do not tag or deploy."
}
```

**Why this is a step rather than something the build guarantees.**
`apps/repocontext/Dockerfile` declares `ARG GIT_COMMIT=""`, so a build that forgets
to export the variable **succeeds**. It exits 0, produces a runnable image, and
leaves the revision label carrying no commit. The only trace is a line in a build
log, which is the least likely place for it to be noticed, and the image is then
indistinguishable by eye from a good one. Redirecting the `-f` path above fixes the
case where the label *cannot* be stamped; it does nothing about the case where it
simply *was not*, and that second case is the one this deployment has actually been
in: the image running on this host resolves no revision at all.

**Test the value, not the exit code.** `docker inspect --format` prints an empty line
and exits **0** in all three of these cases: the label is present and empty, the label
is absent from the image, and the label name you asked for does not exist at all. The
first two are both unprovenanced, so telling them apart does not matter - but the third
is why this matters to whoever maintains the command above. Mistype
`org.opencontainers.image.revision` and the check still runs, still exits 0, and still
prints nothing, so it would report every image as unprovenanced rather than reporting
its own typo. A check that inspects `$LASTEXITCODE`, or that only looks for a non-zero
exit, passes on all three. Compare the string, and keep the label name exact.

`Assert-ContainerProvenance.ps1` applies the same rule later, against the running
container: it reads this label first, falls back to a `candidate-<sha>` tag, and
refuses when neither channel resolves. Checking here rather than there is what keeps
an unprovenanced image from being tagged, deployed, and measured against before
anyone asks the question.

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

`REPO_PATH` is deliberately **not** in this table. It is not machine-checkable here,
for the reasons set out under
[Why the guard test cannot cover this](#why-the-guard-test-cannot-cover-this), and it
matters more than anything below: a wrong `cpus` makes the box slow, a wrong
`REPO_PATH` makes every answer it gives wrong while it looks healthy.

<!-- compose-settings:begin -->

| Service | Setting | Value | Why this value |
| --- | --- | --- | --- |
| `azurite-backup-sink` | `image` | `mcr.microsoft.com/azure-storage/azurite:latest` | The backup sink, added by the memory-backup work in this bucket. Its storage is a **host bind mount**, deliberately not a compose-managed volume, so `docker compose down -v` cannot reach it. See [container.md](container.md) for what that does and does not survive. |
| `embedder` | `EMBED_PROVIDER` | `cpu` | Base default. `cpu`, or `cuda` on an NVIDIA host started with a device reservation. See the [sample README](../../samples/RepoContextContainer/README.md). |
| `embedder` | `DOTNET_gcServer` | `0` | Workstation GC. Server GC allocates a heap and a dedicated GC thread per core, which on a 16-core host is the main driver of resident set for a latency-insensitive background service. This service also has little managed heap worth collecting in parallel: its footprint is dominated by the resident ONNX model, which is native. |
| `embedder` | `EMBED_INTRA_THREADS` | `4` | Pins the ONNX intra-op thread pool to the `cpus` grant below. ONNX Runtime sizes that pool from host cores and does not consult the cgroup quota, so under a 4.0-CPU grant on a 16-core host it ran 4x oversubscribed and the kernel throttled it in 296 of 298 consecutive scheduling periods during vectorising. #2610 derives this from the cgroup automatically, but the deployed image predates that change, so the value is pinned by hand. See [The pool-sizing class](#the-pool-sizing-class). |
| `embedder` | `cpus` | `4.0` | Reduced from an unlimited grant that measured 1014% CPU (about 10 of 16 cores) and made the host unusable for interactive work. Leaves 12 cores free. The ONNX intra-op pool is sized against this grant, so changing it means revisiting `EMBED_INTRA_THREADS` above. |
| `embedder` | `mem_limit` | `5g` | Measured at 4.08 GiB with no limit, and pinned at 2.486 GiB of a 2560m cap (99.4%) while essentially idle at 0.01% CPU, holding the resident ONNX model at its ceiling with no room to work. 5g clears the measured requirement. |
| `repocontext` | `image` | `repocontext-mcp:local` | The base file declares `build:` and no `image:`, so `up -d --no-build` cannot resolve an image without this pin. The tag is moved between builds; see [Pin and roll back](#pin-and-roll-back). |
| `repocontext` | `LATTICE_DURABILITY` | `local` | Base default: SQLite grain storage and reminders plus the file WAL, no external services. See [container.md](container.md). |
| `repocontext` | `LATTICE_DATA_ROOT` | `/data` | Base default. All durable local state on one named volume, so it survives restart, recreation, and image upgrade. See [container.md](container.md). |
| `repocontext` | `LATTICE_BACKUP_BLOB_CONNECTION_STRING` | `redacted` | Presence of this string is what **enables** backup at all; unset, the container runs with no backup and says so at WARNING rather than being silently indistinguishable from a backed-up one. The value is the fixed, public Azurite development account, published in Microsoft's own documentation and not a secret. It is tracked verbatim in the compose file and deliberately not copied here: reproducing a credential-shaped string in documentation teaches readers to read such strings as unremarkable. |
| `repocontext` | `LATTICE_BACKUP_INCREMENTAL_MINUTES` | `60` | Library default, restated in the deployment so the configured cadence is visible here and not only in code. An initial full capture runs at startup, because manifest validation rejects an incremental with no base. |
| `repocontext` | `LATTICE_BACKUP_FULL_HOURS` | `24` | Library default, restated for the same reason as the row above. |
| `repocontext` | `LATTICE_BACKUP_RETENTION_KEEP_LAST` | `60` | Retention keeps a backup satisfying **either** bound, and always preserves the base chain a retained increment depends on. |
| `repocontext` | `LATTICE_BACKUP_RETENTION_MAX_AGE_DAYS` | `14` | The other half of that pair. |
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
| `repocontext` | `DOTNET_gcServer` | `1` | Server GC, restored in #2596. It was `0`, to hold down resident set. That trade went unmeasured until gate run 2, which put it at 283 whole-process silence gaps of 5s or more, longest 29.3s, totalling 20.9% of wall-clock against a 30s Orleans request timeout; all 127 timed-out calls began executing within 0.5s of enqueue and then froze, so it was stop-the-world pausing rather than queueing. The footprint concern is now addressed by the explicit heap count below instead of by giving up parallel collection. |
| `repocontext` | `DOTNET_GCHeapCount` | `6` | Decouples the collector from `DOTNET_PROCESSOR_COUNT` below, which is pinned to 16 for a completely unrelated reason. Server GC would otherwise take its heap count from that pin and allocate 16 heaps. `6` matches the `cpus` cap, not the processor count. See [The pool-sizing class](#the-pool-sizing-class). |
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
2. **The GC heap count.** Server GC allocates a heap and a GC thread per core, and
   takes that count from `DOTNET_PROCESSOR_COUNT` when it is set. On `repocontext`
   that variable is pinned to 16 for instance 1's reasons, so server GC would
   allocate 16 heaps against a 6.0-CPU grant. This was originally avoided by turning
   server GC off entirely, at the cost measured in gate run 2; #2596 instead pins
   `DOTNET_GCHeapCount: "6"` to the grant, which addresses the footprint directly.
   The embedder keeps `DOTNET_gcServer: "0"`, having no large managed heap.
3. **The ONNX intra-op thread pool.** ONNX Runtime sizes its pool from the host core
   count and does **not** consult the cgroup quota. Under the 4.0-CPU grant on this
   16-core host that produced a pool of 16: the kernel throttled 296 of 298
   consecutive scheduling periods, and the pool accumulated 346.3 CPU-seconds stalled
   against 118.8 run. The cost is far worse than proportional, because ONNX Runtime
   synchronises intra-op threads at every operator boundary. Observed embedding rate
   was 1.8 files per minute, projecting about 77 hours for one 8,315-file checkout.

Instance 3 is **fixed in the source** as of #2610: `EMBED_INTRA_THREADS` now defaults
to the enforced cgroup quota read from `/sys/fs/cgroup/cpu.max` rather than to
`Environment.ProcessorCount`, and the server logs its provenance (declared, derived
from the grant, or derived from the processor count) at startup.

**It is not yet exercised in this deployment.** The tuning overlay sets
`EMBED_INTRA_THREADS: "4"` explicitly, and the deployed embedder image predates
#2610, so the value in force is the declared one and the derived path has never run
here. The pin reproduces on the old image what the fixed image would choose for
itself, which is the right call for the running stack and also means a green
deployment tells you nothing about the fix. Removing the pin, on an image built from
#2610 or later, and confirming the value still lands at 4 with derived-from-grant
provenance, is what would establish it. Until then the mechanism's only evidence is
its unit tests, and what has been verified in production is the manual override it
was built to replace.

Note the interaction
with instance 1: `DOTNET_PROCESSOR_COUNT` overrides `Environment.ProcessorCount` and
wins over the quota, so an embedder that copied the `repocontext` environment block
would silently restore the 4x oversubscription. That is why the fix reads the quota
directly.

**The point of naming the class is the fourth instance, which has not been found yet.**
When adding a container limit, or a setting that sizes anything per core, check which
figure the runtime actually reads. `Assert-ContainerProvenance.ps1` and the effective
configuration report (#2593, #2600) exist so the answer is read rather than assumed.

One caution on the throttling figures quoted in instance 3, because they have already
been misread once: they measure **CPU scatter**, not pool size against grant, and they
cannot corroborate a pool-sizing fix. See
[CPU scatter under a fractional quota](#cpu-scatter-under-a-fractional-quota) for what
that statistic does measure and for the retraction.

## CPU scatter under a fractional quota

This is a **different defect from the pool-sizing class above**, and the two are easy
to conflate because they share a cause upstream (a fractional grant on a wide host)
and a symptom downstream (throttling). Keeping them apart matters, because a
statistic that measures this one was once quoted as evidence about that one.

**The mechanism.** A container given a fractional CPU quota and no `cpuset` is
*entitled* to 4 CPUs but *visible* on all 16. CFS bandwidth control vends quota to
**per-CPU run queues in 5 ms slices** (`kernel.sched_cfs_bandwidth_slice_us`), and a
thread waking on a run queue draws a whole slice whether it then runs for 5 ms or
5 us; the unused remainder is returned only lazily. Threads scattered across many run
queues therefore exhaust the quota by **reservation** rather than by execution, and
the cgroup is throttled while its actual utilisation is a small fraction of its
entitlement. The effect scales with the number of run queues threads can land on,
which is the **visible CPU count**, not the quota.

Note what this is not. The pool-sizing class is about a runtime *creating too many
threads*. This is about *where the threads it creates are allowed to run*, and it
happens at any thread count.

**The measurement** (#2623). Throwaway `alpine` containers, identical synthetic load,
identical `--cpus=4` quota, varying **only** `--cpuset-cpus`. Counters read from the
host cgroup `cpu.stat`.

| cpuset | periods | throttled | ratio | mean CPU | quota used |
| --- | --- | --- | --- | --- | --- |
| `0-15` | 907 | 49 | 5.4% | ~68% | ~17% |
| `0-3` | 953 | **0** | **0.0%** | ~104% | ~26% |

The load confound is **inverted** in that run, which is what makes it decisive: the
arm offering *more* work throttled **zero**, and the arm offering *less* throttled at
17% of its quota. Utilisation cannot produce that ordering; scatter can. A heavier
first experiment reached 47.7% against 0.0% on the same single variable, and both
pinned arms recorded zero throttled periods and zero throttled microseconds.

### Two things this statistic must not be used for

Both are retractions of readings previously made in epic #2368, recorded here so they
are not made again.

1. **It is not a measure of thread-pool oversubscription.** The throttling ratio was
   once quoted (99.3% before, 30.9% after) as evidence that the ONNX intra-op fix in
   [the pool-sizing class](#the-pool-sizing-class) had taken effect. That reading is
   **withdrawn.** The statistic tracks how many CPUs the container can see. A
   container doing no work at all measures approximately 31% on this host, so the
   residual is a **floor, not a remainder**, and must never be read as "some
   oversubscription persists". Establish pool sizing **structurally** - from `/proc`,
   from configuration, or from source - never from this counter.
2. **`cpu.stat` is not untrustworthy under Docker Desktop.** An earlier suspicion that
   it might be is also **withdrawn**: it responds cleanly, deterministically and
   monotonically to CPU scatter across both experiments. Figures read from it are real
   data.

A third reading worth stating positively: throttling at very low CPU utilisation is
**expected** here rather than anomalous, and on its own is not evidence of a defect.

### Enabling it

Pinning is **opt-in and unset by default**, through two variables the base compose
file declares as `${REPOCONTEXT_CPUSET:-}` and `${EMBEDDER_CPUSET:-}`. Unset, Compose
omits the `cpuset` key from the resolved document **entirely** rather than emitting an
empty one, so a stack that ignores them resolves byte-for-byte what it resolved before
the knob existed. Set them in `.env` beside `REPO_PATH`; see
[.env.example](../../samples/RepoContextContainer/.env.example).

```bash
# In samples/RepoContextContainer/.env
REPOCONTEXT_CPUSET=0-5
EMBEDDER_CPUSET=6-9
```

Derive the values rather than copying them:

1. **One range per service, sized to the ceiling of that service's `cpus` grant.**
   `cpus: 6.0` wants six CPUs, `cpus: 4.0` wants four.
2. **The ranges must not overlap**, or you have traded throttling for contention,
   which is a worse deal than the one you started with. The two services are busy
   simultaneously by construction, since the reconcile pass is what feeds the embedder.
3. **Leave headroom** for the host and for any service with no grant.
   `azurite-backup-sink` declares no `cpus`, so it has no quota to be throttled
   against and is deliberately left unpinned.

The values above are for the 16-CPU host the tuning overlay was measured on and are
**not portable**. Docker refuses to start a container whose `cpuset` names a CPU the
host does not have, so a copied value fails loudly at `up` on a smaller machine rather
than silently - that is the good case. The bad case is a host where the ranges are
valid but no longer disjoint from what else runs there.

### When not to enable it

**Not between a measurement run's T0 and its final scrape.** Epic #2368 adopted a
precondition that no service configuration may change inside that window, after a
mid-run service recreation voided gate run 3 for every criterion that spanned it. A
change of this kind lands **before** a run's T0 and **alone**, or not at all. The knob
ships unset precisely so that enabling it is an act on the record at a moment somebody
chose, rather than a default that arrives with a `git pull`.

What it buys, stated without overclaim: **latency jitter and scheduling determinism**.
It licenses **no throughput claim**. The measurement is synthetic load on `alpine`
containers, and its transfer to the ONNX embedder and to the repocontext silo is an
inference rather than a measurement.

### What is deliberately not changed

`DOTNET_PROCESSOR_COUNT: "16"` on the `repocontext` service is a number above the
effective grant and belongs to the same family, but it is **not** touched by this knob
and is not a thread pool: it holds the WAL replay concurrency gate at the value every
prior field measurement on this box was taken against, which is instance 1 of
[the pool-sizing class](#the-pool-sizing-class). Aligning it is a separate change with
a separate blast radius, and it must be measured on its own rather than ridden in on
this one.

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

Assumes only a clone and a Docker daemon. Steps 1 and 2 are shell-specific because
the commit has to survive from the build into the check; the rest is not.

```powershell
# 1. Build the host image from the sha you intend to deploy, from the repository
#    root and from the TRACKED Dockerfile compose declares. `.deploy/` is untracked
#    and is not a build input - see Build and tag from a known sha.
$env:GIT_COMMIT = (git rev-parse HEAD)
docker build -f apps/repocontext/Dockerfile `
  -t "repocontext-mcp:candidate-$env:GIT_COMMIT" `
  --build-arg GIT_COMMIT=$env:GIT_COMMIT `
  --secret id=nugetcfg,src=$env:APPDATA\NuGet\NuGet.Config .

# 2. Verify the build stamped the commit, BEFORE tagging. A missing GIT_COMMIT does
#    not fail the build; it yields an image the provenance gate cannot resolve.
$stamped = docker inspect "repocontext-mcp:candidate-$env:GIT_COMMIT" `
  --format '{{index .Config.Labels "org.opencontainers.image.revision"}}'
if ($stamped -ne $env:GIT_COMMIT) {
  throw "UNPROVENANCED IMAGE: revision label is '$stamped'. Do not tag or deploy."
}
```

```bash
# 3. Pin it. (Nothing to preserve on a clean host; on an existing one, save the
#    displaced tag first - see Pin and roll back.)
docker tag repocontext-mcp:candidate-<sha> repocontext-mcp:local

# 4. Bring up the tuned stack. The embedder builds from its own small context;
#    --no-build applies to the pinned host image.
cd samples/RepoContextContainer
docker compose -f docker-compose.yml -f docker-compose.tuning.yml up -d

# 5. Wait for readiness.
curl -fsS http://localhost:8080/health/ready

# 6. Register the workspace repositories over MCP (repocontext_add_repo with a path
#    under /workspace), then watch repocontext_index_status until filesEmbedded
#    reaches filesScanned. Until it does, semantic search answers only over the
#    already-embedded slice, so a missing hit is not evidence of missing code.

# 7. Confirm what you deployed.
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
| `LATTICE_BACKUP_*` on `repocontext`, and the `azurite-backup-sink` service | **No longer a delta.** These were machine-local when this runbook was first written and are now tracked in `docker-compose.yml` by the memory-backup work in this bucket. They are documented in the table above. | Retired. Kept here only so a reader of an older revision is not left looking for them. |
| `REPO_PATH` | Machine-specific by nature: it names a host path. It is not a delta to be tolerated but a setting to be **set deliberately**, and leaving it to default is the defect described under [The worktree trap](#the-worktree-trap). Its tracked example is [`.env.example`](../../samples/RepoContextContainer/.env.example). | Never. Set it explicitly on every host. |

The list above is only as good as the discipline that maintains it, and nothing
enforces it. A delta that is running and not written down here is indistinguishable
from one that was never applied, which is the failure this document exists to close.
`Assert-ContainerProvenance.ps1` is what reads the running truth back.

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

`LocalDeploymentRunbookHygieneTests` (in `test/lattice/Hygiene/`) resolves
`docker-compose.yml` and `docker-compose.tuning.yml` with `docker compose config` and
asserts, in both directions, that the settings table above enumerates exactly what
that document declares. A setting added to either compose file without a table row
fails the test, and a table row naming a setting the merge does not actually produce
fails it too.

It evaluates the **resolved** document rather than the raw files on purpose: compose
merge and interpolation decide what a setting resolves to, so a raw-file comparison
can be green about a value the merge discards.

The same fixture also holds the **build input** to the one compose declares. Every
`docker build -f` in this document must name the Dockerfile the `repocontext` build
stanza names, that stanza's context must still resolve to the repository root, every
documented build must pass `--build-arg GIT_COMMIT=`, and the document must still tell
the operator to read `org.opencontainers.image.revision` back out of the result. That
guard exists because those two files drifted apart silently and nothing noticed: #2690
moved the tracked build onto `apps/repocontext/Dockerfile` and added the arg that
stamps the label, and this runbook went on pointing at an untracked `.deploy/Dockerfile`
that could not stamp it at all (#2707). It asserts over build *commands*, not over
every occurrence of the word, so the warning above that `.deploy/` is not a build input
is permitted rather than forbidden.

The same fixture holds the **opt-in** guarantee for CPU pinning: it asserts that with
`REPOCONTEXT_CPUSET` and `EMBEDDER_CPUSET` unset the resolved document declares no
`cpuset` on any service, and, textually, that every `cpuset` a tracked compose file
declares is variable-driven with an **empty** default. The second half is what stops a
literal range being hard-coded later, which would make pinning a default rather than a
choice - and would perturb exactly the measurement window this knob is kept unset for.

**What a green run of that test establishes: that two tracked files agree with each
other. Nothing else.** It does not establish that any container is running, that a
running container was composed from these files, that it is executing an image built
from this checkout, or that any of these limits are in force anywhere. For those, run
`Assert-ContainerProvenance.ps1` against the container itself.
