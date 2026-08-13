# RepoContext MCP container - "codebase memory in a box"

This sample runs the RepoContext MCP server (issue #1435) as a single,
restart-durable container alongside its embedding companion, and demonstrates the
core durability guarantee end to end:

**start -> add a repo under the mounted workspace -> recall -> restart -> context
is still present.**

Two containers, one private network:

- **`repocontext`** - the MCP host image (`apps/repocontext/Dockerfile`). Its
  ONLY application listener is the MCP endpoint on port 8080 (plus the HTTP health
  probes). No gRPC facade and no Explorer UI are exposed. It runs the default
  `local` durability profile: Orleans ADO.NET grain storage and reminders over a
  single SQLite file, plus the file-backed Lattice WAL - all under `/data`, which
  is a named volume, so state survives `docker compose restart`, `docker compose
  down`, and image upgrades. Zero external services.
- **`embedder`** - the Onyx model-server companion (`apps/embedding/Dockerfile`,
  issue #1440). It stays a SEPARATE container so the MCP host keeps its
  single-listener surface. The host's default embedding provider is pointed at it
  via `LATTICE_EMBEDDING_ENDPOINT`. Its HuggingFace model cache is a named volume.

## Prerequisites

- Docker with Compose v2.
- Build context is the REPOSITORY ROOT (the host image ProjectReferences the
  just-built `src/` bits), so this compose file sets `context: ../..`. Run it from
  this directory.

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

Every path passed to `repocontext_add_repo` is resolved to its real location - `..`
traversal and symlink escape are both defeated - and must resolve under
`/workspace` (set by `LATTICE_WORKSPACE_ROOT`); a path outside it is refused.

## Walkthrough

From this directory:

```bash
# 1. Start both containers. The host waits for the embedder to become healthy.
docker compose up -d --build

# 2. Wait for readiness. /health/ready reports 200 only once the silo has joined,
#    the activation-time WAL replay is done, the durable stores are proven
#    reachable, and MCP is serving. It is 503 during startup replay and during drain.
curl -fsS http://localhost:8080/health/ready

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
curl -fsS http://localhost:8080/health/ready

# 6. Recall again. The context is still present: it was replayed from the WAL and
#    SQLite state on the /data volume, proving durability across a restart.
```

Tear down (state on the named volumes is preserved unless you pass `-v`):

```bash
docker compose down          # keeps the data + model-cache volumes
docker compose down -v       # also deletes durable state (start clean)
```

## Health probing

The runtime image is distroless and shell-less, so probing is HTTP-only - there is
no shell-exec healthcheck:

- `GET /health/live` - process + silo host alive (liveness).
- `GET /health/ready` - silo joined, replay done, stores reachable, MCP serving
  (readiness). Not-ready during startup replay and during drain.

## Notes on durability and shutdown

- All durable local state (the WAL directory and the SQLite database file) lives
  under `/data`, a named volume. The host fails fast at startup if that path is
  missing or not writable by its non-root UID.
- On `SIGTERM` (a `docker stop` / `restart`) the host flips readiness to not-ready
  first, then drains: the silo deactivates and the WAL commit-log flushes buffered
  records before exit, within a generous shutdown budget, so an in-flight write is
  durable after restart.
