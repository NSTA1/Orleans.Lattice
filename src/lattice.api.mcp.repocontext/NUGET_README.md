# Orleans.Lattice.Api.Mcp.RepoContext

Give an AI coding agent a durable, long-term memory of a codebase.

This is an opt-in **repository-context** add-on for the [Orleans.Lattice.Api.Mcp](https://www.nuget.org/packages/Orleans.Lattice.Api.Mcp) MCP server. It exposes a set of Model Context Protocol tools an agent uses to capture, maintain, and retrieve detailed context about a repository - the shape of the code, the decisions and notes it has made, and short-lived working memory - and keeps all of it in durable, conflict-free Lattice storage that survives process restarts.

## Why

An agent starts every session cold: it re-reads files, re-derives structure, and forgets what it learned last time. This package gives it a place to remember. One bootstrap call turns a repository into a queryable baseline; from then on the agent can recall what a file is, search the codebase by meaning, and write back notes and decisions that persist across sessions and restarts.

## What an agent can do with it

- **Onboard a codebase** - `repocontext_bootstrap` walks a repository and records a structural model (every file and its content digest) onto a durable tree. Re-running it is incremental and idempotent: unchanged files are a no-op, changed files update in place, deleted files are pruned, and an interrupted run resumes without duplication. In workspace mode (see below) `repocontext_add_repo` supersedes it, registering a repository under a mounted workspace root and ingesting it in the same call.
- **Search by meaning or keyword** - `repocontext_search` returns the records most relevant to a natural-language query. With an embedding provider bound it runs a semantic (nearest-neighbour) search, approximate by default: it is answered from a persisted approximate index whose recall is bounded rather than complete (published floors of recall@10 >= 0.95 clustered, >= 0.55 adversarially unclustered) in exchange for a query cost that is sub-linear in the corpus and survives a restart. Set `LATTICE_REPOCONTEXT_SEMANTIC_RETRIEVAL=exact` to bind the complete-recall brute-force scan instead. While the approximate index is still building the exact scan answers, so recall stays complete throughout and the response reports the weaker claim rather than over-promising. Without an embedder it degrades fail-closed to deterministic keyword and structural recall, so a query always returns the best available answer instead of failing. Every response carries a `retrievalPath` naming which of the two semantic guarantees, or which keyword cause, served it.
- **Read context back** - `repocontext_recall` fetches a single record by key, `repocontext_scan` pages through an ordered range, and `repocontext_list_topics` enumerates the memory topics that have been captured.
- **Remember decisions and notes** - `repocontext_remember`, `_update`, and `_forget` let the agent write and maintain its own memory and decision entries, each with an optional time-to-live so ephemeral working memory lapses on its own while durable knowledge stays.

## How it stays safe

- **Durable and conflict-free** - every record lives on a dedicated Lattice tree backed by the write-ahead log, so context survives restarts and concurrent writers converge via CRDT merge rather than clobbering each other.
- **Fail-closed and permission-scoped** - it reuses the `Api.Mcp` credential bridge, authorization gate, and permission-scoped tool discovery. The mutating tools are contributed only when the host opts writes in; a read-only caller never sees them, and an unauthenticated caller sees nothing.
- **Read-only over your code** - bootstrap only reads the repository; the store of record is the Lattice trees, never your source.

## Getting started

Register the tools alongside the MCP server, then map the endpoint:

```csharp
builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
builder.Services.AddRepoContextTools(enableWrites: true);
// ...
app.MapLatticeMcp();
```

`AddRepoContextTools()` with writes off offers only the read-only tools; pass `enableWrites: true` to also contribute bootstrap and the memory-writing tools. Bind an `IEmbeddingProvider` to turn on semantic search; without one, search still works in keyword mode.

For a container or multi-tenant host, pass `workspaceMode: true` with a `workspaceRoot`. This mounts a broad parent directory once and lets the client register individual repositories under it dynamically with `repocontext_add_repo`, list them with `repocontext_list_repos`, and drop them with `repocontext_remove_repo` - instead of baking one repository path into configuration. Every added path is resolved to its real location and must sit inside the workspace root, so `..` traversal and symlink escape are refused.

```csharp
builder.Services.AddRepoContextTools(enableWrites: true, workspaceMode: true, workspaceRoot: "/workspace");
```

## Run it as a container - "codebase memory in a box"

The [**RepoContextContainer sample**](https://github.com/NSTA1/Orleans.Lattice/blob/main/samples/RepoContextContainer/README.md) packages all of this into a single restart-durable Docker container (with an embedding companion) that mounts a workspace read-only, registers repositories under it on demand, and serves the tools over MCP with no external services. It is the fastest way to see the whole flow end to end: start, add a mounted repo, search and recall, restart, and the context is still there.

## Learn more

See the [repository-context guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.mcp.repocontext/README.md) for the record model, the full tool catalogue, memory and TTL semantics, and the semantic-search seam.
