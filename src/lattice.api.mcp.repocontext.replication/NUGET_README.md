# Orleans.Lattice.Api.Mcp.RepoContext.Replication

Turn on cross-cluster replication for the [Orleans.Lattice.Api.Mcp.RepoContext](https://www.nuget.org/packages/Orleans.Lattice.Api.Mcp.RepoContext) store with one guardrailed call.

This is an opt-in **multi-cluster** add-on for the repository-context package. It adds a single extension method, `EnableRepoContextMultiCluster(...)`, that registers the Lattice replication engine and enrols every replicated repository-context tree for cross-cluster replication under the correct per-tree merge mode - so an operator cannot get the convergence rules wrong.

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

- **The vector-membership presence tree is pinned to the add-wins `OrFlag` CRDT.** A source embedded on one cluster and pruned on another must converge *add-wins*, or replication silently drops the embedding and degrades retrieval to keyword mode. The pin wins even over a host that declared it otherwise.
- **The agent-memory tree is pinned to the multi-value `MvRegister` mode.** Two clusters writing the same memory key concurrently must both survive and fold back through the memory record's own CRDT merge; last-writer-wins would silently discard one whole record. This pin also wins over a host declaration.
- **Every other replicated tree** - the structural and symbol stores of record, the rebuildable content and cross-reference projections, the per-session reuse bookkeeping, and the vector payload and metadata projections - defaults to last-writer-wins, the mode matching how those trees are authored. The two wholly derived local accelerators, the approximate-index tree and the vector-coverage digest, are not replicated at all: each cluster derives its own.

A startup validator fails fast if memory or membership is enrolled under any other mode, or if a single-writer index-plane tree (structural, symbol, content, cross-reference, vector payload, or vector metadata) is enrolled under anything but last-writer-wins, so a misconfigured topology never reaches serving traffic.

## Topology

There is exactly one valid multi-cluster topology: **single-indexer hub-and-spoke**. One cluster, the hub, walks, reconciles, prunes, and re-embeds; every other cluster runs as a spoke (`LATTICE_REPOCONTEXT_INDEXING_ROLE=spoke`) that serves retrieval from the replicated trees without re-embedding. Active-active *indexing* is not supported: enrolling an index-plane tree under a CRDT mode - the only way to express it - fails the startup validator. Every cluster may still serve reads and accept agent-memory writes, which converge through the pinned modes.

The embedding-gap scanner is part of the hub's index pass, so it stays local to the hub and a spoke never runs it; this helper only governs which trees ship and how they converge.

## Learn more

See the [multi-cluster guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.mcp.repocontext.replication/README.md) for the full tree-mode map and topology walkthroughs.
