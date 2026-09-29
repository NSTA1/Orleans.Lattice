# Repository-context MCP host (`apps/repocontext`)

The container host for the Orleans.Lattice repository-context MCP server -
"codebase memory in a box". It gives an AI coding agent a durable, long-term
memory of a codebase: onboard a repository once, then search it, read it back,
and remember notes about it across restarts.

The image is a self-contained single-silo Orleans host whose ONLY application
listener is the MCP endpoint (plus HTTP health probes, a Prometheus `/metrics`
scrape endpoint and, in the `azure` durability profile only, the scaling-signal
scrape, all on the same port). No gRPC facade and no Explorer UI are exposed.
Under the default `local` durability profile all state - Orleans grain storage and
reminders plus the file-backed Lattice WAL - lives under `LATTICE_DATA_ROOT`
(default `/data`, a volume), so an indexed repository and its remembered context
survive `docker restart`, `docker compose down`, and image upgrades.

Embedding is delegated to a separate companion image (see
[`apps/embedding-onnx`](../embedding-onnx/README.md), the sample's default, or the
Onyx-derived [`apps/embedding`](../embedding/README.md) it can fall back to) so
this host stays MCP-only and never embeds in-process.

## What it wires up

- The `Orleans.Lattice.Api.Mcp.RepoContext` tool group in workspace mode
  (`repocontext_add_repo`, `repocontext_list_repos`, `repocontext_remove_repo`,
  `repocontext_search`, `repocontext_recall`, `repocontext_remember`, and the
  rest), served over the MCP endpoint on `LATTICE_MCP_PORT` (default `8080`). The
  client registers repositories at runtime under the read-only workspace mounted at
  `LATTICE_WORKSPACE_ROOT` (default `/workspace`); a path escaping that root is
  refused.
- The tree-administration tool group (`lattice_treeadmin_*`) on the same MCP
  endpoint, registered with its mutating tree-lifecycle verbs enabled and its
  mutating schema-control verbs disabled, so whole-tree operator verbs such as the
  orphaned-leaf audit and repair are reachable in-process (issue #3287). See
  [MCP tools](../../docs/lattice.api.mcp/tools.md).
- A durability profile selected by `LATTICE_DURABILITY`: `local` by default
  (SQLite grain storage and reminders plus the file WAL); `postgres` moves grain
  storage and reminders to PostgreSQL, and `azure` moves them, the WAL and
  cluster membership to Azure Storage.
- A named-lock lease ceiling of 30 minutes in place of the library's 5-minute
  `LatticeOptions.MaxLockLeaseDuration` default, so a backlog claim taken through
  `repocontext_claim` can outlast a full build-and-test cycle. Set it with
  `LATTICE_MAX_LOCK_LEASE_SECONDS` (default `1800`, accepted `30`-`7200`; a set
  value that is not an integer in that range fails startup).
- Scheduled capture of the durable agent-memory tree into an external blob sink,
  so a gesture that destroys the primary store does not destroy its only copy. It
  is inert unless a sink is configured (the sample configures one), and it is
  probed on `/health/backup`, which feeds neither liveness nor readiness.
- Compaction on the churn trees, a readiness probe that reports `Draining` on
  SIGTERM, and a data-path guard that fails startup if the mount is not writable.

All wiring is composed in `Hosting/RepoContextHostBuilder.cs`, from the helpers
beside it under `Hosting/`, so it is unit-testable; `Program.cs` is the thin
process shell, which also answers the image's exec-form `--healthcheck` probe (a
short HTTP check of the host's own `/health/silo`) without building the host.

The configuration reference - the environment variables, the durability
profiles, the health endpoints, and the instruments the host publishes on
`/metrics`, including the SQLite grain-storage lock attribution and its pin-store
write retry - is the
[container quickstart](../../docs/lattice.api.mcp.repocontext/container.md).

## Try it

The [`samples/RepoContextContainer`](../../samples/RepoContextContainer/README.md)
sample brings this host up alongside the embedding companion with `docker compose`
and walks through the full flow: **start -> add a mounted repo -> search and
recall -> restart -> context is still present.** Start there.
