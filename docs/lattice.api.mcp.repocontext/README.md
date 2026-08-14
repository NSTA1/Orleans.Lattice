# Orleans.Lattice.Api.Mcp.RepoContext

Optional, opt-in **repository-context** add-on for the [Orleans.Lattice.Api.Mcp](../lattice.api.mcp/README.md) server. It gives an AI agent a durable, conflict-free place to capture and maintain detailed context about a codebase - structural facts, notes, and short-lived working memory - served as Model Context Protocol tools over dedicated Lattice trees.

## What is it?

The repository-context module plugs into the `Orleans.Lattice.Api.Mcp` binding's permission-aware discovery core and contributes a group of `repocontext_*` tools:

- **Onboarding.** `repocontext_bootstrap` walks a repository and lands a structural node plus a content digest for every file on a durable tree, so an agent starts from a populated, queryable baseline instead of empty memory. Re-runs are incremental, idempotent, and resumable. In the container's workspace mode this onboarding is driven per-repository by `repocontext_add_repo` and `repocontext_remove_repo`, with `repocontext_list_repos` enumerating what is registered.
- **Working memory.** `repocontext_remember`, `repocontext_update`, and `repocontext_forget` capture agent-authored notes and decisions under topics, with an optional per-entry time-to-live for memory that lapses on its own.
- **Retrieval.** `repocontext_recall`, `repocontext_scan`, and `repocontext_list_topics` read the context back; `repocontext_search` adds meaning-based retrieval when an embedding provider is bound, degrading fail-closed to a deterministic keyword scan when it is not.
- **Health.** `repocontext_health` proves the surface is registered and reachable for the authenticated caller.

Every record is stored as a CRDT value on a named Lattice tree, so concurrent updates converge without locks, and the whole store inherits Lattice's durability, TTL, and tombstone-compaction behaviour. Nothing here introduces a new storage or expiry mechanism - it composes the core.

## Fail-closed and permission-scoped

The module adds no authorization path of its own. The permission-aware discovery core advertises its tools only to a caller holding one of the data-plane operations that makes the built-in data group usable, and the fail-closed gate enforces the verdict at both advertisement and invocation. The mutating tools (`bootstrap`, `remember`, `update`, `forget`) are contributed only when the host opts writes in via `AddRepoContextTools(enableWrites: true)`; a reader-only caller never sees them.

## Quick Start

Register the module as a companion to `AddLatticeMcp`:

```csharp verify
using Orleans.Lattice.Api.Mcp.RepoContext;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddLatticeMcp(o => o.RequireAuthorization = true);
services.AddRepoContextTools(enableWrites: true);
```

The host must also map the MCP endpoint (`app.MapLatticeMcp()`) and, for `repocontext_search` to run a semantic query, bind an `IEmbeddingProvider` (for example the Onyx provider from `Orleans.Lattice.Api.Mcp.RepoContext`'s embedding companion). Without one, search still answers by keyword.

For a ready-to-run, restart-durable local deployment - "codebase memory in a box" - see the [container quickstart](container.md) and the [container sample](../../samples/RepoContextContainer/README.md).

## Reference

- [Record model](record-model.md) - the named trees, the key grammar, the record families, and the CRDT store-of-record model.
- [Tools](tools.md) - the full `repocontext_*` tool catalogue and each tool's contract.
- [Memory and TTL](memory-and-ttl.md) - agent memory, topics, and per-repository time-to-live policy.
- [Semantic search](semantic-search.md) - the embedding seam, the exact-kNN index, and fail-closed degradation.
- [Container quickstart](container.md) - running the module as a single durable local container.

Related package docs:

- [MCP Server](../lattice.api.mcp/README.md) - the host binding, credential bridge, and authorization gate this module composes.
- [File WAL storage](../lattice.storage.file/README.md) - the cloud-free durable WAL backend the local container uses.
