# Memory and TTL

Beyond the structural model of a codebase, the store holds agent-authored **memory**: notes, observations, and decisions an agent captures as it works. Memory is organised under topics and can optionally expire.

## Topics and entries

A memory entry is keyed `repo/{repoId}/mem/{topic}/{id}`. The topic is a free-form grouping (for example `decisions`, `todo`, or a feature name); the id identifies one entry within it. An agent:

- creates or updates entries with `repocontext_remember` (omit `id` to create with a generated id, or supply one to merge in place),
- discovers what topics exist with `repocontext_list_topics` (each topic reports its live entry count),
- reads entries back with `repocontext_recall` (one key) or `repocontext_scan` (a topic or all memory, paged),
- and removes an entry with `repocontext_forget`.

Every write is a CRDT read-merge-write, so two agents (or two turns) that touch the same entry converge rather than clobber each other.

## Time-to-live

Memory can be **ephemeral**. A per-entry TTL turns an entry into working memory that lapses on its own, so short-lived context does not accumulate forever. TTL is not a new mechanism: it surfaces the per-entry expiry Orleans.Lattice core already provides on `ILattice.SetAsync(...)`, which converts a TTL to an absolute UTC expiry at write time. Reads then hide expired entries and background tombstone compaction reaps them.

- `repocontext_remember` accepts an optional `ttlSeconds`. When omitted, a newly created entry inherits the repository's configured default memory TTL if one is set, otherwise it stays durable.
- `repocontext_update` preserves whatever remaining TTL an entry already has.
- `repocontext_forget` can either hard-delete immediately or, with `lapse`, re-write the entry with a short TTL so concurrent readers drain gracefully.
- `repocontext_recall` reports each entry's remaining life, so an agent can tell how long a note has left.

## Per-repository TTL policy

`RepoContextTtlOptions` sets the default policy, bound per repository through the named-options convention (`IOptionsMonitor<RepoContextTtlOptions>.Get(repoId)`), mirroring how the core resolves `LatticeOptions` per tree. The default (unnamed) instance is the fallback.

```csharp verify
using Orleans.Lattice.Api.Mcp.RepoContext;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddRepoContextTools(enableWrites: true);

// Default for every repository: 30-day working memory unless a write overrides it.
services.Configure<RepoContextTtlOptions>(options =>
{
    options.DefaultMemoryTtl = TimeSpan.FromDays(30);
    options.StructuralRecordsNeverExpire = true;
});

// One repository keeps its notes durable by default.
services.Configure<RepoContextTtlOptions>("durable-repo", options =>
{
    options.DefaultMemoryTtl = null;
});
```

| Option | Default | Meaning |
|---|---|---|
| `DefaultMemoryTtl` | `null` | The TTL applied to a memory entry when the writer supplies none. `null` leaves memory durable unless a TTL is given explicitly. When set it must be a positive, finite duration - the core write path and the paired validator reject a non-positive TTL. |
| `StructuralRecordsNeverExpire` | `true` | Guarantees structural records (repo, package, file, symbol) never carry an expiry, so the durable model of the codebase is not silently reaped alongside ephemeral notes. |

The validator runs at first resolve, so an invalid TTL policy fails at startup rather than on the first write.
