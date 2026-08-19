# Orleans.Lattice.Api.Mcp.RepoContext.Replication

Turn on cross-cluster replication for the [Orleans.Lattice.Api.Mcp.RepoContext](https://www.nuget.org/packages/Orleans.Lattice.Api.Mcp.RepoContext) store with one guardrailed call.

This is an opt-in **multi-cluster** add-on for the repository-context package. It adds a single extension method, `EnableRepoContextMultiCluster(...)`, that registers the Lattice replication engine and enrols every repository-context tree for cross-cluster replication under the correct per-tree merge mode - so an operator cannot get the convergence rules wrong.

## Why a separate package

The repository-context core deliberately does not reference `Orleans.Lattice.Replication`: that zero-dependency boundary is what keeps its config-only seam free, so a single-cluster deployment never pulls in the replication engine. Enabling multi-cluster has to call into the replication package, so it lives here as an opt-in companion - exactly like the other `*.Replication` / `*.Grpc` add-ons. You take the replication-engine dependency only when you install this package.

## What it does

One call replaces the hand-written, easy-to-misconfigure enrolment of every repository-context tree:

```csharp
siloBuilder.EnableRepoContextMultiCluster(opts =>
{
    opts.ClusterId = "cluster-a";
    // transport / peers / secrets configured as normal
});
```

It calls `AddLatticeReplication(...)` with your settings, then merges the repository-context tree-mode map into `ReplicatedTrees`:

- **The vector-membership presence tree is pinned to the add-wins `OrFlag` CRDT.** This is the load-bearing rule: a source embedded on one cluster and pruned on another must converge *add-wins*, or active-active replication silently drops the embedding and degrades retrieval to keyword mode. The pin wins even over a host that declared it otherwise.
- **Every other repository-context tree** - the structural, symbol, and memory stores of record, the rebuildable content and cross-reference projections, the per-session reuse bookkeeping, and the vector payload and metadata projections - defaults to last-writer-wins, the mode matching how those trees are authored. A deliberate per-tree override is respected.

## Topologies

- **Single-indexer** - compute the expensive embedding index once on one cluster and replicate it; passive clusters serve retrieval without re-embedding.
- **Active-active** - every cluster embeds, and the membership CRDT reconciles presence add-wins.

The cross-cluster embedding-gap scanner stays local to each cluster; this helper only governs which trees ship and how they converge.

## Learn more

See the [multi-cluster guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.mcp.repocontext.replication/README.md) for the full tree-mode map and topology walkthroughs.
