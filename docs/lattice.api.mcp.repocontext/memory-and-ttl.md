# Memory and TTL

Beyond the structural model of a codebase, the store holds agent-authored **memory**: notes, observations, and decisions an agent captures as it works. Memory is organised under topics and can optionally expire.

## Topics and entries

A memory entry is keyed `repo/{repoId}/mem/{topic}/{id}`. The topic is a free-form grouping; the id identifies one entry within it. Topics are not enforced, but agents are nudged toward a small, stable vocabulary - `decisions` (design choices with rationale), `gotchas` (non-obvious pitfalls), `conventions` (project norms), `glossary` (domain terms), `todo` (follow-ups), or a stable feature or component name - so related notes stay groupable across sessions instead of fragmenting into synonyms. An agent:

- creates or updates entries with `repocontext_remember` (omit `id` to create with a generated id, or supply one to merge in place),
- discovers what topics exist with `repocontext_list_topics` (each topic reports its live entry count),
- reads entries back with `repocontext_recall` (one key) or `repocontext_scan` (a topic or all memory, paged),
- and removes an entry with `repocontext_forget`.

Every write is a CRDT read-merge-write, so two agents (or two turns) that touch the same entry converge rather than clobber each other.

## Time-to-live

Memory can be **ephemeral**. A per-entry TTL turns an entry into working memory that lapses on its own, so short-lived context does not accumulate forever. TTL is not a new mechanism: it surfaces the per-entry expiry Orleans.Lattice core already provides. A memory entry is written through the core multi-value-register accessor (`MvRegisterAccessor<T>.SetAsync`, backed by the TTL overload of `ILattice.ApplyCrdtDeltaAsync`), which converts a TTL to an absolute UTC expiry at write time. Reads then hide expired entries and background tombstone compaction reaps them.

- `repocontext_remember` accepts an optional `ttlSeconds`. When omitted, a newly created entry inherits the repository's configured default memory TTL if one is set, otherwise it stays durable.
- A memory entry's expiry only ever moves later. The multi-value-register path resolves expiry by keeping the later absolute expiry, with durable as the bottom, so a write that carries a TTL gives a durable entry that expiry and otherwise can extend an entry's life but never shorten it, and a write that carries none - a `repocontext_remember` merge into an existing entry without `ttlSeconds`, for example - leaves the existing expiry unchanged. No tool call returns an ephemeral memory entry to durable. Restoring the container's [memory archive](memory-durability.md#the-memory-archive) can: an entry that is durable in either the archive or the store is restored durable.
- `repocontext_update` preserves whatever remaining TTL an entry already has.
- `repocontext_forget` can either hard-delete immediately or, with `lapse`, re-write the entry with a short TTL (60 seconds unless `lapseSeconds` says otherwise) so concurrent readers drain gracefully. On a memory entry whose value decodes, the lapse is bound by the same later-expiry rule: it lapses a durable entry, but it cannot shorten one whose existing expiry is later than the lapse window. A memory entry whose value cannot be decoded is lapsed with a plain time-to-live write instead, which sets the lapse window exactly, and the result reports it as `undecodable`. Either way the result's `expiresAtUtc` reports the expiry actually in force.
- `repocontext_recall` reports each entry's remaining life, so an agent can tell how long a note has left.

## Per-repository TTL policy

`RepoContextTtlOptions` sets the default policy, bound per repository through the named-options convention (`IOptionsMonitor<RepoContextTtlOptions>.Get(repoId)`), mirroring how the core resolves `LatticeOptions` per tree. A memory write resolves the instance named for its own repository, and a repository with no named configuration resolves the type's defaults - the unnamed instance is never consulted - so apply a policy to every repository with `ConfigureAll` and override individual repositories by name.

```csharp verify
using Orleans.Lattice.Api.Mcp.RepoContext;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddRepoContextTools(enableWrites: true);

// Default for every repository: 30-day working memory unless a write overrides it.
services.ConfigureAll<RepoContextTtlOptions>(options =>
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
| `DefaultMemoryTtl` | `null` | The TTL applied to a newly created memory entry when the writer supplies none; a merge into an existing entry never applies it. `null` leaves memory durable unless a TTL is given explicitly. When set it must be a positive, finite duration - the paired validator rejects a non-positive TTL. The core multi-value-register accessor refuses one as well: its TTL overload throws `ArgumentOutOfRangeException` for a zero or negative TTL rather than writing the entry durable. |
| `StructuralRecordsNeverExpire` | `true` | A declarative policy flag for code that writes structural records (repo, package, file, symbol): while it is set, such a writer must omit any TTL. No code in the package reads it today, so it enforces nothing - the indexing path simply never writes a structural record with an expiry, so the durable model of the codebase is not reaped alongside ephemeral notes, whatever the flag says. It does not stop `repocontext_forget` with `lapse` from lapsing a structural record deliberately. |

The validator runs when a repository's policy is first resolved - the first memory write that creates an entry for that repository without an explicit `ttlSeconds` - and an invalid policy refuses that write rather than being applied. It is not checked at host startup.

## Multi-cluster convergence

Every memory entry is stored as a multi-value register keyed by the writing replica, in a single cluster exactly as in several, and each write is a read-merge-write of the record's own CRDT fields. A single cluster has one replica id, so its register only ever carries one value. Across clusters a whole-record last-writer-wins store would be unsafe: two clusters writing the same memory key concurrently would let one write win outright and silently discard the other whole record - and any CRDT sub-state it carried. The opt-in [`Orleans.Lattice.Api.Mcp.RepoContext.Replication`](../lattice.api.mcp.repocontext.replication/README.md) add-on therefore pins the agent-memory tree's replication merge mode to `MvRegister` and authors each cluster's writes under its own replication id: each cluster mints its own dot, so concurrent writes both survive and are folded back through the record model's own CRDT merge on read. Because the stored shape is the same either way, turning replication on needs no data migration. TTL is preserved through this path - a replicated memory entry keeps the absolute expiry resolved on the writing cluster. Enabling multi-cluster replication changes only how concurrent cross-cluster writes converge; a single-cluster deployment is unaffected and takes no replication dependency.
