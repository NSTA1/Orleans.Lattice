# Repository-context MCP host (`apps/repocontext`)

The container host for the Orleans.Lattice repository-context MCP server -
"codebase memory in a box". It gives an AI coding agent a durable, long-term
memory of a codebase: onboard a repository once, then search it, read it back,
and remember notes about it across restarts.

The image is a self-contained single-silo Orleans host whose ONLY application
listener is the MCP endpoint (plus HTTP health probes). No gRPC facade and no
Explorer UI are exposed. All state - Orleans grain storage and reminders plus the
file-backed Lattice WAL - lives under `LATTICE_DATA_ROOT` (default `/data`, a
volume), so an indexed repository and its remembered context survive
`docker restart`, `docker compose down`, and image upgrades.

Embedding is delegated to a separate companion image (see
[`apps/embedding`](../embedding/README.md)) so this host stays MCP-only and never
embeds in-process.

## What it wires up

- The `Orleans.Lattice.Api.Mcp.RepoContext` tool group (`repocontext_bootstrap`,
  `repocontext_search`, `repocontext_recall`, `repocontext_remember`, and the
  rest), served over the MCP endpoint on `LATTICE_MCP_PORT` (default `8080`).
- A durability profile selected by `LATTICE_DURABILITY` (`local` SQLite + file WAL
  by default; `azure`/`postgres` for shared deployments).
- Compaction on the churn trees, a readiness probe that reports `Draining` on
  SIGTERM, and a data-path guard that fails startup if the mount is not writable.

All wiring lives in `Hosting/RepoContextHostBuilder.cs` so it is unit-testable;
`Program.cs` is the thin process shell.

## Try it

The [`samples/RepoContextContainer`](../../samples/RepoContextContainer/README.md)
sample brings this host up alongside the embedding companion with `docker compose`
and walks through the full flow: **start -> bootstrap a mounted repo -> search and
recall -> restart -> context is still present.** Start there.
