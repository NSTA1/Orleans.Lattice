# RepoContext MCP container - "codebase memory in a box"

This sample runs the RepoContext MCP server as a single,
restart-durable container alongside its embedding companion, and demonstrates the
core durability guarantee end to end:

**start -> add a repo under the mounted workspace -> recall -> restart -> context
is still present.**

The same box also serves the retrieval and token-economics tools for you to use and
test: explainable search, the budgeted context bundle, its reuse economics, and
usage accounting (see [Retrieval and token economics](#retrieval-and-token-economics)).

Two containers, one private network:

- **`repocontext`** - the MCP host image (`apps/repocontext/Dockerfile`). Its
  ONLY application listener is the MCP endpoint on port 8080 (plus the HTTP health
  probes and the Prometheus `/metrics` scrape endpoint). No gRPC facade and no
  Explorer UI are exposed. It runs the default
  `local` durability profile: Orleans ADO.NET grain storage and reminders over a
  single SQLite file, plus the file-backed Lattice WAL - all under `/data`, which
  is a named volume, so state survives `docker compose restart`, `docker compose
  down`, and image upgrades. Zero external services.
- **`embedder`** - the ONNX Runtime model companion
  (`apps/embedding-onnx/Dockerfile`). It stays a SEPARATE container so the MCP
  host keeps its single-listener surface. The host's default embedding provider
  is pointed at it via `LATTICE_EMBEDDING_ENDPOINT`. Its model weights are baked
  into an image layer, so it needs no cache volume and no download on first run.

## Prerequisites

- Docker with Compose v2.
- Memory: budget at least 12 GiB for the host container, and give the Docker VM
  headroom above that. This is much more than a default allocation. Measured on
  a ~8000-file index, steady-state working set settled at about 10.2 GiB; a
  single-variable run differing only in the memory limit produced 756
  OutOfMemoryException and 528 failed grain activations at 4 GiB, and zero of
  each at 12 GiB (issue #2364). Under-provisioning does not present as memory
  pressure: a cgroup limit becomes the .NET GC heap hard limit, so the process is
  never OOM-killed and there is no restart, exit code or resource event. The
  visible symptom is a STORAGE error while reading grain state, because the
  allocation that fails is a leaf-snapshot deserialisation; the leaf then
  activates cold and replays its whole WAL window, raising pressure further. The
  `orleans.lattice.leaf.snapshot.load_failures` counter names the real cause
  directly (`reason=resource_exhausted`). No limit is set in the sample compose
  file on purpose - measure your own corpus rather than copying 12.
- Build context differs per image: the host image's is the REPOSITORY ROOT (it
  ProjectReferences the just-built `src/` bits), so its service sets
  `context: ../..`; the embedder builds from its own `apps/embedding-onnx`
  directory, which has no `src/` dependency and so keeps a small context. Run
  compose from this directory either way.

## The mounted workspace

Set `REPO_PATH` to the absolute path of a directory the box may see. It is mounted
READ-ONLY at `/workspace` inside the container, so the box can never mutate the
code it indexes. This is a *workspace root*, not a single repository: mount a broad
parent and register individual repositories under it at runtime with the
`repocontext_add_repo` tool. It defaults to this repository's parent, so this repo
is one registerable child.

```bash
export REPO_PATH=/absolute/path/to/some/parent    # PowerShell: $env:REPO_PATH="C:\path\to\parent"
```

## The published port

The MCP endpoint and health probes are published on host port **8080** by default.
That port is a common one to already have in use, and a clash shows up only as an
opaque bind failure when you run `up`, so it is overridable with
`REPOCONTEXT_PORT`:

```bash
export REPOCONTEXT_PORT=18080                     # PowerShell: $env:REPOCONTEXT_PORT="18080"
docker compose up -d
```

Only the host side moves; inside the container the listener stays on 8080. Every
`localhost:8080` in the walkthrough below then becomes
`localhost:$REPOCONTEXT_PORT`.

Every path passed to `repocontext_add_repo` is resolved to its real location - `..`
traversal and symlink escape are both defeated - and must resolve under
`/workspace` (set by `LATTICE_WORKSPACE_ROOT`); a path outside it is refused.

## Choosing an embedding companion

The sample brings up the ONNX Runtime companion
([`apps/embedding-onnx`](../../apps/embedding-onnx/README.md)) by default. It
bakes its weights into an image layer, so a cold start needs no model download,
and it selects its accelerator - CPU or NVIDIA - from one build via
`EMBED_PROVIDER`.

The original Onyx companion
([`apps/embedding`](../../apps/embedding/README.md)) remains available as a
fallback, selected with an override file:

```bash
docker compose -f docker-compose.yml -f docker-compose.onyx.yml up -d
```

Nothing else changes in either direction. Both serve the same contract on the
same port, so the service name and `LATTICE_EMBEDDING_ENDPOINT` are identical,
and they produce **numerically identical vectors** (same pinned model revision,
fp32, same tokenizer and pooling), so switching does not invalidate an existing
`/data` volume. The ONNX image is roughly an order of magnitude smaller
(about 1.3 GB against 13 GB).

### Running the embedder on an NVIDIA GPU

The default build is CPU-only, and it stays CPU-only on a GPU host: the `cpu`
flavour has no GPU support compiled in. Enabling the GPU needs all three of these
together, because they do different jobs - the build arg picks a different ONNX
Runtime package, the environment variable binds the accelerator at runtime, and
the reservation is what actually exposes the device to the container. In
`docker-compose.yml` under the `embedder` service:

```yaml
    build:
      args:
        ONNX_FLAVOR: cuda
    environment:
      EMBED_PROVIDER: cuda
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
```

Then rebuild, because a plain `up -d` would reuse the cached CPU image:

```bash
docker compose build embedder
docker compose up -d
```

Requires the NVIDIA Container Toolkit on the host. The `cuda` flavour is a much
larger image but still includes the CPU provider, so it serves CPU hosts too, and
an unrecognised `EMBED_PROVIDER` falls back to the CPU rather than failing to
boot. That fallback is silent by design, so verify what actually bound:

```bash
docker compose exec repocontext curl -s http://embedder:9000/api/health
# {"status":"ok","provider":"Cuda","model":"model.onnx","dimension":768}
```

A `provider` of `Cpu` here means the GPU was not picked up - check the toolkit
and the device reservation. The Onyx fallback companion takes a different route
to the same place (clear `CUDA_VISIBLE_DEVICES` and add the reservation); see
[`apps/embedding`](../../apps/embedding/README.md).

## Walkthrough

From this directory:

```bash
# 1. Start both containers. The host waits for the embedder to become healthy.
docker compose up -d --build

# 2. Wait for the host to come up. /health/live returns 200 once the process and
#    the silo host are alive, which is what the remaining steps actually need.
curl -fsS http://localhost:8080/health/live

#    /health/ready is a stricter, orchestrator-facing probe, and this walkthrough
#    deliberately does NOT gate on it. It is the conjunction of the lifecycle phase
#    AND the vector plane having demonstrated a working semantic query, so it can
#    stay 503 long after the box is up and answering MCP calls. Observe it, but do
#    not wait on it or treat a 503 as a failed deployment - read "Interpreting a
#    persistent 503" below first. The -o/-w form reports the code without failing
#    the shell, which `curl -fsS` would do on any non-2xx.
curl -sS -o /dev/null -w 'ready: %{http_code}\n' http://localhost:8080/health/ready

# 3. Register a repository under the mounted workspace over MCP (repocontext_add_repo
#    with a path under /workspace). Use your MCP client of choice against
#    http://localhost:8080 (the MCP streamable-HTTP endpoint). For example, with
#    the reference `mcp` CLI:
#      mcp call http://localhost:8080 repocontext_add_repo '{"path":"/workspace/my-repo"}'
#    Omit repoId to derive it from the final path segment, or set it explicitly:
#      mcp call http://localhost:8080 repocontext_add_repo '{"path":"/workspace/my-repo","repoId":"demo"}'
#    List what is registered at any time:
#      mcp call http://localhost:8080 repocontext_list_repos '{}'

# 4. Recall: query the box (repocontext_search / repocontext_recall) and confirm it
#    returns the ingested context.

# 5. Restart the container - a FULL recreation that evicts the in-memory projection
#    and forces a WAL replay / cold rebuild on next access.
docker compose restart repocontext
curl -fsS http://localhost:8080/health/live
#    As in step 2, /health/ready may stay 503 after the restart without meaning the
#    restart failed. Step 6, not the probe, is the proof that the data survived.

# 6. Recall again. The context is still present: it was replayed from the WAL and
#    SQLite state on the /data volume, proving durability across a restart.
```

## Retrieval and token economics

Once a repository is registered (step 3 above), the same box exposes the epic's
retrieval and token-economics tools over the same MCP endpoint - no extra service,
no second listener. Every call below targets the `demo` repo id from step 3;
substitute your own. Examples use the reference `mcp` CLI against
`http://localhost:8080`.

```bash
# A. Explainable search. Every hit carries a machine-readable `reasons` array
#    saying WHY it ranked (semantic proximity and matched chunk/symbol, or the
#    specific keyword fields hit - path/name, symbol, tag, topic, content, key),
#    so an agent can justify a selection instead of trusting an opaque score.
mcp call http://localhost:8080 repocontext_search \
  '{"repoId":"demo","query":"where is the readiness health probe wired","k":5}'

# B. Budgeted context bundle. repocontext_context packs the ranked, explained
#    source for a task into ONE response under a HARD token ceiling: the reported
#    `totalTokens` never exceeds `responseBudgetTokens`, and `truncated` /
#    `retryBudgetTokens` say whether more would fit at a larger budget. `detail`
#    trades richness for budget - 'paths' (cheapest) -> 'outline' (declared-symbol
#    skeleton) -> 'slices' (bounded body text, richest), or 'auto' (default) which
#    picks the richest level that fits. Pass a `session` id so the box remembers
#    what it delivered.
mcp call http://localhost:8080 repocontext_context \
  '{"repoId":"demo","task":"explain the readiness health check","responseBudgetTokens":4000,"detail":"auto","session":"agent-1"}'

# C. Reuse economics. Repeat on the SAME `session`. Units the session already
#    holds are suppressed - acknowledged under `reused`, never re-charged and never
#    counted against `top` or the budget - so the second answer pays only for the
#    NEW context. (You can also feed the prior entries' unit receipts back via
#    `seen`, or a whole-file 'path@hash' claim via `known`; the server-side
#    `session` bookkeeping does it for you.)
mcp call http://localhost:8080 repocontext_context \
  '{"repoId":"demo","task":"explain the readiness health check and how drain flips it","responseBudgetTokens":4000,"detail":"auto","session":"agent-1"}'

# D. Usage accounting. repocontext_stats reports the aggregate token economics over
#    a bounded recent window: calls answered, response tokens spent, whole-file
#    reads replaced, and the NET tokens saved by budgeting plus reuse.
mcp call http://localhost:8080 repocontext_stats '{}'
```

With the `embedder` companion healthy, search and the bundle rank semantically;
with no embedding provider bound they degrade to a deterministic keyword rank - the
bundle still answers either way. See
[docs/lattice.api.mcp.repocontext/retrieval-economics.md](../../docs/lattice.api.mcp.repocontext/retrieval-economics.md)
for the full model.

Tear down (state on the named volumes is preserved unless you pass `-v`):

```bash
docker compose down          # keeps the data + model-cache volumes
docker compose down -v       # also deletes durable state (start clean)
```

`down -v` deletes the index **and the authored agent memory**, because both live
on the same `/data` volume. Read
[Agent memory versus the code index](#agent-memory-versus-the-code-index) before
using it: `repocontext_reset_index` rebuilds an index with no loss at all, and
the memory archive that makes `down -v` survivable is bounded by its export
interval rather than complete.

## Agent memory versus the code index

Two kinds of state share the `/data` volume, and only one of them can be
recreated.

| | What it is | If it is destroyed | The safe gesture |
|---|---|---|---|
| **Code index** | Structural, content, symbol, xref, session and vector planes, derived from files on disk | Re-run `repocontext_add_repo`; back in minutes | `repocontext_reset_index` |
| **Agent memory** | Every `repocontext_remember` note, decision, gotcha and glossary entry | Gone; it is the store of record and derives from nothing | Keep an archive (below) |

`docker compose down -v` destroys both. That is the defect behind issue #2601:
the gesture is documented as the ordinary way to start clean, and it silently
takes the irreplaceable half with it. It has already happened once, to epic
#2368's own memory.

### Why the two are not simply on separate volumes

Because they cannot be, and because it would not have helped.

They cannot be: a Lattice tree's durable state spans a WAL root that is one
directory for the whole storage **provider**, and a grain store that is a single
SQLite file shared by every tree. The `/data/wal/repo-context-*` subdirectories
look like separable locations but are a naming convention inside one root, and
the memory tree's pages sit interleaved with every other tree's in
`/data/repocontext.db`. There is no memory-only path to mount elsewhere.

It would not have helped: `docker compose down -v` removes **every** named volume
the project declares, not just the one you had in mind. A second declared volume
dies in the same command as the first.

### What actually protects it

A **bind mount**, `/memory-archive`, which is not a project-declared volume and
so is not removed by `down -v`. The host exports memory there periodically and,
when it starts against an empty store, restores from it.

```bash
# Point the archive at a durable host path (defaults to ./memory-archive).
REPOCONTEXT_MEMORY_ARCHIVE_PATH=~/repocontext-memory docker compose up -d
```

| Variable | Default | Meaning |
|---|---|---|
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_DIR` | `/memory-archive` in this sample; unset (feature off) otherwise | Container path the archive is written to. Unset disables the whole mechanism. |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_INTERVAL_SECONDS` | `300` | Export cadence. This is the size of the window an ungraceful stop loses. Values below 30 are raised to 30. |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_RESTORE` | `auto` | `auto` restores only into an empty store, `always` restores on every start, `off` never restores. |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_STOP_TIMEOUT_SECONDS` | `20` | Budget for the final export during a graceful stop, clamped to 1-60. |

Restore this way by hand at any time - stop the box, put the archive files in
place, start it against an empty store:

```bash
docker compose down -v
ls memory-archive/            # repo-context-memory.snapshot (+ .previous.snapshot)
docker compose up -d
docker compose logs repocontext | grep -i 'memory durability'
```

### What this does not do

It is **not a backup**, and it does not make `down -v` safe.

* Everything written since the last export is lost. The exposure is the export
  interval, plus whatever a non-graceful stop discards. A graceful stop
  (`down`, `down -v`, `stop`) exports once more on the way out and closes most
  of that window; a `kill -9` or a host crash does not.
* It archives **memory only**. The index is not in the archive, by design - it
  rebuilds from source.
* It does not remove the co-location. Memory still shares a volume with
  rebuildable state, and the host says so at startup, at warning level, every
  time. `repocontext_reset_index` remains the correct way to rebuild an index:
  it drops the derived planes and preserves memory outright, with no window at
  all.
* It is not the scheduled whole-store backup being wired up in issue #2602.
  That one owns manifests, retention and operator-driven restore of everything;
  this one owns automatic restore-on-empty for memory alone. Only this
  mechanism restores automatically at startup.

See
[docs/lattice.api.mcp.repocontext/memory-durability.md](../../docs/lattice.api.mcp.repocontext/memory-durability.md)
for the full model.

## Health probing

The runtime image is distroless and shell-less, so probing is HTTP-only - there is
no shell-exec healthcheck:

- `GET /health/live` - process + silo host alive (liveness). This is the probe the
  walkthrough gates on, and the one an orchestrator uses to decide whether to
  restart the container.
- `GET /health/ready` - readiness (routing). On this sample's local durability
  profile it is the **conjunction of two independent components**, and both must be
  healthy for a 200 (the Azure profile adds a third, the scaling-signal check):
  - **lifecycle** - the silo has joined, the activation-time WAL replay is done, the
    durable stores were proven reachable, and MCP is serving;
  - **vector plane** - semantic retrieval has been demonstrated to work. A
    deployment with no embedder bound (keyword-only), and a host with no repository
    registered yet, both count as ready here: there is no vector plane to wait for
    in the first case and nothing to serve in the second.

  So it is not-ready during startup replay and during drain, but those are not the
  only causes, and a sustained 503 is far more likely to be the vector-plane
  component than either of them.

### Interpreting a persistent 503

A `/health/ready` 503 that does not clear, on a container that is otherwise up,
does **not** on its own mean the deployment is broken, and must not be used by
itself as a rollback signal. The endpoint returns a bare `Unhealthy` with no
per-component breakdown, so a 503 is ambiguous until you narrow it. Five steps,
each one ruling out a cause the previous step left open:

1. `curl -fsS http://localhost:8080/health/live`. A 200 says the process and the
   silo host are alive, so whatever is unhealthy is not the process. If this also
   fails, the container really is unhealthy - that is the case to act on.
2. Make any MCP call against `http://localhost:8080` (`repocontext_list_repos` is
   the cheapest). If it answers, the MCP surface is serving, which satisfies the
   lifecycle component and leaves the vector plane as the one holding readiness
   down.
3. Run a `repocontext_search` and read the `retrievalPath` on the result. A value
   of `keyword.vector_plane_unavailable` confirms it: semantic retrieval is
   unavailable and the box has fallen back to deterministic keyword recall.
4. Check the embedder with `docker compose ps`. Step 3 tells you the vector plane
   is at fault but not which side of it, and the two sides need opposite responses.
   An `embedder` container that is missing, exited, or `(unhealthy)` is itself the
   cause, and is directly actionable: restore it and readiness can recover on its
   own. An `embedder` reporting `(healthy)` while readiness stays 503 rules the
   embedder out and places the fault host-side, in the vector plane, where
   restarting the embedder achieves nothing. Use `docker compose ps` rather than
   probing the embedder directly - its port is not published to the host.
5. Separate **"never been ready"** from **"was ready and has since lost it"** on
   `/metrics`. The two need different responses and steps 1 to 4 cannot tell them
   apart:

   ```bash
   curl -fsS http://localhost:8080/metrics \
     | grep -E 'repocontext_retrieval_(ready_seconds|unavailable)'
   ```

   `repocontext_retrieval_ready_seconds_count` is stamped **once per process**, on
   the first transition into a ready phase. Its absence therefore means the
   retrieval plane has never been ready in this container's current lifetime; its
   presence alongside a 503 means the plane was ready and has since lost it. Its
   `phase` label records which phase it first reached (`serving`, `keyword_only`,
   or `nothing_registered`). `repocontext_retrieval_unavailable_total` counts fault
   episodes, and its `cause` label carries the same vocabulary as step 3's
   `retrievalPath`, so it separates a vector plane that cannot serve
   (`keyword.vector_plane_unavailable`) from an index that has drifted from its
   sources (`keyword.index_degraded`).

**Issuing a query yourself does not clear it, and the host is already trying.** A
warmup service issues the same semantic query from application start, retrying with
backoff (2s, doubling to a 30s cap) until the plane answers or shutdown begins. So a
persistent 503 is never "nobody has queried it yet" - it is that warmup failing
repeatedly. In particular, a box that has a repository **registered** but holds no
vectors for it stays not-ready by design: the search reports
`keyword.vector_plane_unavailable`, and running another search by hand returns the
same thing and changes nothing. (A box with **no** repository registered is the
opposite case and reports ready, because there is nothing it could be asked to
serve.)

Readiness also lags a fault on purpose. Once the plane has served, a fault must
persist for **30 seconds** before readiness is revoked, and any successful retrieval
inside that window clears the episode outright - so a 503 can appear up to half a
minute after the fault that caused it, and a brief blip may never surface at all.

In that state **the box is still usable and the whole walkthrough still completes**:
registration, keyword search, `repocontext_context`, and durability across a restart
all work, and steps 3 to 6 demonstrate exactly that. What is degraded is semantic
ranking, not the service. Treat it as a capability to restore, not as a deployment
to roll back.

The same listener also serves `GET /metrics`, a Prometheus text exposition of every
instrument on a meter whose name starts with `orleans.lattice` - the core meter and
every per-package meter, `Orleans.Lattice.Api.Mcp.RepoContext` included. It needs no
second port and no sidecar:

```bash
curl -fsS http://localhost:8080/metrics | head -n 20
```

## Notes on durability and shutdown

- All durable local state (the WAL directory and the SQLite database file) lives
  under `/data`, a named volume. The host fails fast at startup if that path is
  missing or not writable by its non-root UID.
- On `SIGTERM` (a `docker stop` / `restart`) the host flips readiness to not-ready
  first, then drains: the silo deactivates and the WAL commit-log flushes buffered
  records before exit, so an in-flight write is durable after restart.
- **PID 1 is an init process, and that is what makes the `SIGTERM` land at all.**
  `init: true` in `docker-compose.yml` has Docker bind-mount its own static
  `docker-init` binary and run it as PID 1, with the host as its child. It needs
  nothing in the distroless runtime image and changes no application code. Two
  kernel behaviours make it necessary, and both attach to PID 1 rather than to
  the application: no default action is taken for a signal delivered to PID 1
  that PID 1 has installed no handler for, so a well-behaved process can be
  unkillable by `SIGTERM` purely by being PID 1; and PID 1 inherits every
  orphaned descendant and must `wait()` on it, which the .NET host does not do.
  In the epic #2368 gate runs a container reached a state where neither
  `docker kill` nor `docker rm -f` would reap PID 1 and it had to be
  `SIGKILL`ed, costing that run its drain and leaving the next one unbanked
  state to replay (issue #2576). This is **independent of the grace period
  below**: `init` decides whether the drain starts, the grace period decides how
  long it may take, and setting one without the other leaves half the failure in
  place.
- **That drain's budget is 90 seconds and it belongs to the host, not to Docker.**
  The host sets `HostOptions.ShutdownTimeout` to 90s
  (`RepoContextHostBuilder.ShutdownBudget`); Docker's `stop_grace_period` defaults
  to **10 seconds**. The two are enforced independently and the smaller wins, so
  without an explicit `stop_grace_period` the process is `SIGKILL`ed at 10s with
  the drain still running and the 90s is dead configuration (issue #2389). The
  `stop_grace_period: 120s` in `docker-compose.yml` is what makes it reachable.
  If you run this image under your own orchestration you must grant the same
  budget there - Kubernetes has the identical trap under a different name, since
  `terminationGracePeriodSeconds` defaults to 30s.
- The drain reports its own duration, so the budget can be derived rather than
  bisected. `docker logs` carries `RepoContext drain complete in <n>s, consuming
  <p>% of the 90s host shutdown budget`. Read it together with its severity,
  because there are three distinct outcomes and the level is what separates them:
  - **No completion line at all.** The container was killed mid-drain, so
    `stop_grace_period` is smaller than the drain (issue #2389). The exit code
    will not tell you, because a killed container reports `137` and the next
    `docker start` overwrites it.
  - **`drain complete` at `Warning`.** The drain finished but consumed more than
    70% of the budget. Nothing has failed; treat it as a lead indicator, because
    drain time grows with resident state.
  - **`drain ABANDONED after 90s` at `Error`, and the container exits `70`.** The
    *host* stopped waiting and deactivation was abandoned part-way. Raise the
    service's `stop_grace_period` and the `LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD`
    that declares it, together and to the same value; the budget is derived from
    the second and bounded by the first, so raising either alone achieves nothing.
- **The abandoned drain is also visible without reading the log** (issue #2401).
  An orchestrator does not read logs, it reads the exit code, and before #2401
  the abandoned case did not reliably produce a distinctive one. Measured against
  a real generic host, the outcome was not simply zero but *undetermined*: a
  hosted service that absorbed the shutdown cancellation left `RunAsync`
  returning normally, so the process exited `0` and the abandonment was recorded
  as a clean stop; one that rethrew it let the exception escape `RunAsync`
  unhandled, aborting the process in a way indistinguishable from a real crash.
  The host now assigns the code itself when the overrun latches: `0` if the drain
  completed inside the budget, `70` if it was abandoned. `70` is `EX_SOFTWARE` in
  the `sysexits.h` convention, chosen to avoid `0`/`1`/`2`, Docker's reserved
  `125`-`127`, and the `128 + signal` band that holds `137` (`SIGKILL`) and `143`
  (`SIGTERM`) - the neighbouring conditions it exists to be told apart from.
  - Note what this does **not** do. `docker-compose.yml` uses
    `restart: unless-stopped`, under which Docker restarts on any exit code, so
    the code neither triggers nor suppresses a restart. What it changes is what
    is recorded: `docker inspect --format '{{.State.ExitCode}}'` reports `70`,
    `docker ps -a` shows `Exited (70)`, and under Kubernetes the container
    terminates with reason `Error` rather than `Completed`. That is what an alert
    can be written against.
  - There is deliberately no way to turn it off. A switch restoring `0` would
    remove the evidence rather than the problem.
- That last line exists because of issue #2397, and the reason it is needed is
  not obvious. The host raises `ApplicationStopped` **even when the shutdown
  budget expired and it gave up waiting** - so a signal bound only to that event
  reported `drain complete in 90.0s` for a drain that did not complete. The
  failure looked like success. The overrun is now raised by an alarm armed when
  the drain begins, so it is reported at the moment the budget expires rather
  than depending on a callback that may never arrive.
- Drain time scales with resident state: the same 400-file rig drained in 33.9s
  before its vector trees had landed and 67.2s once they had, which is already
  three quarters of the 90s budget. The budget was nevertheless **not** raised,
  because measurements on a live box show the resident set that a drain must
  flush has no observed ceiling (idle-deactivation sweeps ranging from 1 to 4,418
  activations, still climbing between readings). A fixed ceiling on an unbounded
  quantity moves the threshold without changing the failure mode, so #2397
  shipped the diagnostic instead of a new number, and #2402 - which proposed
  raising it - did not ship one either.
- The 90s is no longer written down as an independent constant. Since issue #2402
  the host derives it from the grace period the deployment declares through
  `LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD`, taking 75% of it or all but a
  two-second unwind reserve, whichever is smaller. The declared `120s` in
  `docker-compose.yml` yields exactly the 90s the container has always run with,
  so nothing moved; what changed is that there is one number to set instead of
  two, and a budget larger than the grace period can no longer be expressed. That
  matters because such a budget is not merely useless - the container kills the
  process at the real grace period regardless, so the `ABANDONED` line above is
  armed for an instant that never arrives and is never emitted, which is the
  silent teardown of issue #2389 all over again.
- **The variable declares the grant; it is not the grant.** Nothing inside the
  container can read the real `stop_grace_period`, so a deployment that declares
  `120s` while granting `20s` runs a 90s budget under a 20s guillotine and cannot
  detect it. Keeping the two adjacent in the same compose service is the
  mitigation, and `RepoContextComposeShutdownBudgetTests` asserts they are equal
  here - but that adjacency is **a convention, not an enforcement**. Change them
  together, always.
- None of this bounds the resident set. Raising both values past your own
  observed drain is a legitimate local remedy, but it buys time rather than
  fixing the shape.

## Verifying what you actually deployed

Everything above describes what the tracked compose file declares. Nothing above
establishes that a container now running received any of it.

`docker compose up` reads the compose files in its **own working directory**,
whatever branch built the image it starts, and its output names no branch, no
commit, and no directory. The image and the runtime configuration are therefore
two independent inputs, and only the first is obviously version-controlled. An
operator standing in one checkout can deploy a candidate image under a different
checkout's configuration and see nothing at all to say so.

That is not hypothetical. Two gate runs of epic #2368 did exactly this: the
candidate image ran under the baseline's runtime config, the container's own
compose label resolved to the main checkout, and
`LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD` was absent from the process environment
while sitting merged on the candidate branch. Both runs observed the absence of
the fix's effect and concluded the fix was absent. The observation was real and
correctly made. The discriminator was in a channel nobody was reading.

Note what would **not** have helped. A check comparing tracked files to other
tracked files would have been green throughout both runs, because the repository
agreed with itself perfectly. Only a reading taken from the running process
separates "the source does not carry the fix" from "the source carries it and
this container never received it".

```bash
cd samples/RepoContextContainer
pwsh -File ./scripts/Assert-ContainerProvenance.ps1
```

It takes four readings from the running container and refuses unless all four
agree, printing every value it read either way:

1. **Compose provenance.** The container's own
   `com.docker.compose.project.working_dir` label resolves to the checkout you
   are standing in, every file in `com.docker.compose.project.config_files` lies
   under it and exists on disk, and **the number of those files is the number you
   expected** (default 2, see below). An override file merged in from elsewhere
   is how a resolved document stops matching the tracked one.
2. **Git provenance.** That directory is a git worktree and its HEAD is the
   commit you expect, reported as a value you read rather than an inference you
   make. The expectation is sourced from *your* checkout, never from the one the
   container resolved to; defaulting it to the latter would compare a value
   against itself and pass unconditionally, which is worse than omitting the
   check because it would report as checked.
3. **Image provenance.** The image id the container is executing still matches
   what its image reference resolves to now, which catches a container left in
   place across a rebuild. Ids, never tags: a tag is a mutable pointer, so
   comparing tag to tag compares two names for whatever is current.
4. **Environment provenance.** A candidate-only setting is **present in the
   container's own environment** with the expected value, expectation read from
   your checkout's `docker-compose.yml` and actual read from the running
   process. This is the non-redundant one. Checks 1 to 3 can all pass while an
   override file, an edit, or a stale container leaves the value unset, and it is
   the direct executable form of the warning above that the variable declares the
   grant rather than being it. Durations are compared **parsed, never
   literally**: Compose normalises `120s` to `2m0s`, so a text comparison would
   accuse a correctly configured stack of exactly this defect, and the obvious
   remedy for that accusation is to change a deployment that was already right.

### The count assertion, and why it is not a walk of tracked files

The stack's real deployment is **two** compose files: the tracked
`docker-compose.yml`, which carries a `build:` stanza and no `image:`, and a
`docker-compose.override.yml` that is **untracked and gitignored on purpose**
because it is machine-local. That override is load-bearing. It supplies the
image pin the tracked file does not have, the memory limit, the CPU caps, and
the scan-cadence variables every prior measurement on a given box was taken
against.

So the obvious remedy for a compose-provenance failure - relaunch from the
checkout you meant - **silently drops the override**, leaving no image pin, no
memory limit and a different scan cadence, while the tree looks perfectly
correct and every path the container reports still resolves under the right
directory. Only the count dissents, which is why check 1 asserts it and why the
check reads the container's label rather than walking the repository: **it has to
be able to fail on a file git has never heard of.**

The general form is worth stating, because it is not specific to compose:
*fixing a provenance defect by changing the launch directory is itself a
provenance change, and it is not self-verifying.*

Pass `-ExpectedConfigFileCount 1` if you genuinely mean to run without an
override. Making that an explicit act is the point - dropping the override
should be something you said, not something that happened.

**What a green run does not establish.** That the image was built from the
expected commit (check 3 as defaulted detects a stale container, not a
mislabelled build; pass `-ExpectedImageId` if you need that). That the checkout
was clean when `up` ran, since HEAD is a commit and uncommitted compose edits are
invisible here. That any setting you did not name reached the process. Or
anything whatever about a container you did not name.

This is an operator check and is deliberately **not** wired into CI. It needs a
running container, and a fixture that skipped when Docker was absent would
produce exactly the false green it exists to prevent.

It is also **not** the same instrument as the cold-start rig's
`Assert-RigComposeIsolation`, and neither subsumes the other. That guard
validates the *declaration* - what `docker compose config` resolved - before
anything runs. This validates the *deployment* - what a container already running
actually received. The rig has never had this failure mode, because
`Get-RigComposeFile` pins the file it resolves, so the different-checkout drift
cannot arise there. A green rig guard therefore says nothing about this class,
and the two must not be collapsed. The boundary was drawn deliberately by issue
#2576, whose handoff named both the remedy and its location: a post-up
precondition on the container's own environment.

The adjudication is pure and separated from the acquisition, so the refusing
direction is exercised against fabricated disagreements rather than assumed:

```bash
pwsh -File ./scripts/Test-ContainerProvenance.ps1
```

Every one of the four checks has fixtures it accepts and fixtures it refuses,
two of them reconstructed from the real gate run readings. A check only ever
observed passing is indistinguishable from one that cannot fail, which is the
same reason the suite itself is worth measuring rather than trusting: commit
first, then make one check return no violations unconditionally, re-run, and
confirm the assertions that fail are the ones covering that check and no others.