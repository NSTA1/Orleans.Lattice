# Orleans.Lattice.Api.Mcp.RepoContext

Optional, opt-in **repository-context** add-on for the [Orleans.Lattice.Api.Mcp](https://www.nuget.org/packages/Orleans.Lattice.Api.Mcp) server. It gives an AI agent a durable, conflict-free place to capture and maintain detailed context about a codebase - structural facts, notes, and short-lived working memory - served as Model Context Protocol tools over dedicated Lattice trees.

## What it gives you

- **A codebase memory** - one `repocontext_bootstrap` call walks a repository and lands its structural model (files and their digests) on a durable tree; re-runs are incremental and idempotent.
- **Agent-authored notes and working memory** - `repocontext_remember` / `_update` / `_forget` capture decisions and observations, with an optional per-entry time-to-live for ephemeral memory that lapses on its own.
- **Retrieval** - `repocontext_recall`, `_scan`, and `_list_topics` read the context back; `repocontext_search` adds meaning-based retrieval when an embedding provider is bound, degrading fail-closed to keyword recall when it is not.
- **Fail-closed and permission-scoped** - reuses the `Api.Mcp` credential bridge, authorization gate, and permission-scoped tool discovery. The mutating tools are contributed only when the host opts writes in; a reader-only caller never sees them.

## Getting started

```csharp
builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
builder.Services.AddRepoContextTools(enableWrites: true);
// ...
app.MapLatticeMcp();
```

For a ready-to-run, restart-durable local deployment - "codebase memory in a box" - see the [container sample](https://github.com/NSTA1/Orleans.Lattice/blob/main/samples/RepoContextContainer/README.md).

See the [repository-context guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.mcp.repocontext/README.md) for the record model, the full tool catalogue, memory/TTL semantics, and the semantic-search seam.
