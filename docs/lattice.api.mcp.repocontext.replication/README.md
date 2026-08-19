# Orleans.Lattice.Api.Mcp.RepoContext.Replication

Turn on cross-cluster replication for the repository-context store with one guardrailed call.

`Orleans.Lattice.Api.Mcp.RepoContext.Replication` is an opt-in multi-cluster add-on for [`Orleans.Lattice.Api.Mcp.RepoContext`](../lattice.api.mcp.repocontext/README.md). It contributes a single extension method, `EnableRepoContextMultiCluster(...)`, that registers the Lattice replication engine and enrols every repository-context tree for cross-cluster replication under the correct per-tree merge mode - so an operator turns multi-cluster on with one call and cannot misconfigure the convergence rules.

## Why it is a separate package

The repository-context core deliberately does **not** reference `Orleans.Lattice.Replication`. That boundary is what keeps its configuration-only seam free of the replication engine, so a single-cluster deployment never takes that dependency. Enabling multi-cluster has to call into the replication package, so it lives here as an opt-in companion - exactly like the other `*.Replication` and `*.Grpc` add-ons in the family. You take the replication-engine dependency only when you install this package.

## What it does

The helper calls `AddLatticeReplication(...)` with your own replication settings, then merges the reserved repository-context tree-mode map into `LatticeReplicationOptions.ReplicatedTrees` through a `PostConfigureAll`, so the correct modes win regardless of the order in which the host configures its own replicated-trees map.

```csharp verify
using Orleans.Lattice.Api.Mcp.RepoContext.Replication;

siloBuilder.EnableRepoContextMultiCluster(opts =>
{
    opts.ClusterId = "cluster-a";
    // Transport, peers, and secrets are configured exactly as they are for
    // AddLatticeReplication - EnableRepoContextMultiCluster forwards this delegate
    // to it and then layers the repository-context tree-mode map on top.
});
```

`AddLattice(...)` must be registered first, as for any other replication add-on. Pass `enableRuntimeConfig: true` as the optional second argument to also install the runtime per-tree replication-config control plane (off by default).

## The tree-to-mode map

Every repository-context tree is enrolled. The merge mode of each is fixed by **how the store authors that tree's values**, not by taste:

| Tree | Merge mode | Why |
|------|------------|-----|
| Structural, Symbol, Memory | `LwwRegister` | Stores of record, authored as whole last-writer-wins values per key. |
| Content, CrossReference | `LwwRegister` | Rebuildable projections, authored as whole last-writer-wins values. |
| Session | `LwwRegister` | Rebuildable, expirable per-session reuse bookkeeping. |
| VectorPayload, VectorMetadata | `LwwRegister` | Immutable, content-addressed vector projections. |
| **VectorMembership** | **`OrFlag`** (pinned) | Add-wins presence: an embedding indexed on one cluster and pruned on another **must** converge add-wins by CRDT merge, never delete-wins. |

**Membership is pinned.** The vector-membership presence tree is force-enrolled under the add-wins `OrFlag` mode even if the host declared it under a different mode. This is the load-bearing rule: a `LwwRegister` membership tree would let a prune on one cluster win over a concurrent re-embed on another, silently dropping the embedding and degrading retrieval to keyword mode. Every other tree defaults to `LwwRegister` - the mode consistent with its whole-value authoring - but a deliberate per-tree host override is respected (these trees are per-key last-writer-wins or immutable and content-addressed, so a deployment with a single authoritative writer per key may choose a different mode).

The map is deliberately explicit rather than a blanket "everything is last-writer-wins": enrolling a future CRDT-authored tree under `LwwRegister` would reintroduce exactly the silent-loss bug the membership pin exists to prevent. A regression test asserts the enrolment map's keys equal the repository-context layout contract, so adding a tree to the layout without giving it a deliberate replication mode fails the build.

## Topologies

Both common deployment shapes work, because this helper only governs which trees ship and how they converge:

- **Single-indexer (embed once, replicate).** One cluster runs the indexer, computes the expensive embedding index, and replicates all repository-context trees. Passive clusters serve retrieval from the replicated data without re-embedding. The membership tree still converges add-wins, so a passive cluster that later becomes active loses no presence bits.
- **Active-active (every cluster embeds).** Each cluster embeds independently and the membership CRDT reconciles presence add-wins across sites. A source embedded on cluster A and, concurrently, pruned-then-re-embedded on cluster B converges to *present* - never to a missing embedding that would silently degrade a query to keyword mode.

## The gap scanner stays local

The cross-cluster embedding-gap scanner - the maintenance pass that finds sources present in the structural trees but missing a vector - stays local to each cluster. Replication ships the membership, payload, and metadata trees so a peer sees which sources are already embedded; it does not run a remote embed. Enabling multi-cluster replication changes which trees converge across sites, not where embedding work happens.

## See also

- [`Orleans.Lattice.Api.Mcp.RepoContext`](../lattice.api.mcp.repocontext/README.md) - the repository-context store this add-on replicates.
- [`Orleans.Lattice.Replication`](../lattice.replication/README.md) - the replication engine, transport, and per-tree merge-mode contract this package builds on.
