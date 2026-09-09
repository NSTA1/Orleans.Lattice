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

## Health probing

The runtime image is distroless and shell-less, so probing is HTTP-only - there is
no shell-exec healthcheck:

- `GET /health/live` - process + silo host alive (liveness). This is the probe the
  walkthrough gates on, and the one an orchestrator uses to decide whether to
  restart the container.
- `GET /health/ready` - readiness (routing). It is the **conjunction of two
  independent components**, and both must be healthy for a 200:
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
per-component breakdown, so a 503 is ambiguous until you narrow it. Four steps,
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
  - **`drain ABANDONED after 90s` at `Error`.** The *host* stopped waiting and
    deactivation was abandoned part-way. No `stop_grace_period` can rescue this:
    `RepoContextHostBuilder.ShutdownBudget` is the binding ceiling and must rise,
    with `stop_grace_period` raised to stay strictly greater.
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
  shipped the diagnostic instead of a new number. Raising `ShutdownBudget` past
  your own observed drain is a legitimate local remedy, but it buys time rather
  than fixing the shape.
