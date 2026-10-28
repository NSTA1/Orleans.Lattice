---
agent_spec: "docs/agents/api/mcp.json"
---

# Architecture

`EnableRepoContextMultiCluster` adds no replication machinery of its own. It composes the existing replication engine with the repository-context store and fixes the one decision an operator must not get wrong: the merge mode of each replicated tree. This page describes what the call wires and how the pieces behave at run time; internal collaborators are described by behaviour. For the call's signature see [Public API](api.md); for the full tree-to-mode table and the topology rules see the [package guide](README.md).

## What the call wires

1. **The replication engine.** It calls `AddLatticeReplication(configure, enableRuntimeConfig)` itself, so the commit-time shipping and the receiver's apply seam are in place with the caller's own settings, and the ordering is owned by this package rather than left to the host.
2. **The enrolment merge.** It adds a `PostConfigureAll` over `LatticeReplicationOptions`, which runs after every `Configure` / `ConfigureAll` action - including a host that sets `ReplicatedTrees` after this call - and on every named (per-tree) options instance, because the shipping observer and the merge-mode resolver read options per tree. The merge copies the host's `ReplicatedTrees`, then writes the reserved repository-context entries: the agent-memory tree is forced to `MvRegister` and the vector-membership tree to `OrFlag` whatever the host declared, and every other repository-context tree is added at its default mode (`LwwRegister`) only when the host has not already declared it.
3. **The memory replica identity.** It replaces the store's single-cluster replica identity with one that returns the replication `ClusterId` (falling back to the single-cluster id while that is blank), so every agent-memory CRDT write is authored under this cluster's id. Two clusters writing the same memory key therefore mint distinct dots, and both writes survive the merge.
4. **The topology guard.** It registers an `IValidateOptions<LatticeReplicationOptions>` that engages once any repository-context tree is enrolled and fails validation when the memory tree is not `MvRegister`, the membership tree is not `OrFlag`, or a single-writer index-plane tree (structural, symbol, content, cross-reference, vector payload or vector metadata) is enrolled under anything but `LwwRegister`. A CRDT mode on an index-plane tree would imply more than one concurrent indexer. Of the defaulted trees, only the per-session reuse tree can therefore keep a different host-chosen mode.

## At run time

- **Shipping.** Writes to the enrolled trees are shipped by the replication engine to its peers and applied by each receiver under the tree's resolved merge mode. The approximate-index tree and the vector-coverage digest are not enrolled: each cluster derives its own.
- **Indexing stays single-writer.** Which cluster indexes is decided by the repository-context package's role gate (`LATTICE_REPOCONTEXT_INDEXING_ROLE`, resolved once at startup and fail-closed to `hub`), not by this package. A spoke serves retrieval and memory from the replicated trees and never walks, reconciles, prunes or re-embeds. The guard reads only merge modes, so it cannot detect two clusters both started as `hub`; keeping exactly one hub is the operator's responsibility.
- **Memory and membership converge, never lose.** Concurrent memory writes on different clusters both land and are folded by the memory record's own CRDT merge on read. Membership presence reconciles add-wins, so an embedding added on one cluster is not lost to a prune on another.
- **The gap scanner stays local.** The embedding-gap scan is part of the hub's index pass and never runs remotely; replication only lets peers see which sources are already embedded.

## Seams

| Seam | Owned by | Role here |
|---|---|---|
| `LatticeReplicationOptions` | [`Orleans.Lattice.Replication`](../lattice.replication/README.md) | The only configuration surface: the caller's settings plus the merged repository-context enrolment. |
| `LatticeMergeMode` | `Orleans.Lattice.Replication` | The per-tree convergence rule each enrolled tree is pinned or defaulted to. |
| `IValidateOptions<LatticeReplicationOptions>` | `Microsoft.Extensions.Options` | The hook the topology guard uses to fail a misconfigured topology. |

## Source map

- [Registration](../../src/lattice.api.mcp.repocontext.replication/RepoContextReplicationServiceCollectionExtensions.cs)
- [Enrolment map](../../src/lattice.api.mcp.repocontext.replication/RepoContextReplicatedTrees.cs)
- [Topology guard](../../src/lattice.api.mcp.repocontext.replication/RepoContextTopologyOptionsValidator.cs)
- [Memory replica identity](../../src/lattice.api.mcp.repocontext.replication/ClusterRepoContextReplicaIdentity.cs)

## Related

- [Public API](api.md)
- [Package guide](README.md)
- [`Orleans.Lattice.Api.Mcp.RepoContext` architecture](../lattice.api.mcp.repocontext/architecture.md)
- [Multi-cluster convergence of memory](../lattice.api.mcp.repocontext/memory-and-ttl.md#multi-cluster-convergence)
