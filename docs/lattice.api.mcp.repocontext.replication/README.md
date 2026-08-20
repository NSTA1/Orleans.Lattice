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
| Structural, Symbol | `LwwRegister` | Stores of record, authored as whole last-writer-wins values per key. |
| Content, CrossReference | `LwwRegister` | Rebuildable projections, authored as whole last-writer-wins values. |
| Session | `LwwRegister` | Rebuildable, expirable per-session reuse bookkeeping. |
| VectorPayload, VectorMetadata | `LwwRegister` | Immutable, content-addressed vector projections. |
| **Memory** | **`MvRegister`** (pinned) | Multi-master agent memory: two clusters writing the same memory key concurrently **must** both survive and fold back through the record model's own CRDT merge, never last-writer-wins. |
| **VectorMembership** | **`OrFlag`** (pinned) | Add-wins presence: an embedding indexed on one cluster and pruned on another **must** converge add-wins by CRDT merge, never delete-wins. |

**Two trees are pinned.** The vector-membership presence tree is force-enrolled under the add-wins `OrFlag` mode, and the agent-memory tree under the multi-value `MvRegister` mode, even if the host declared either under a different mode. These are the load-bearing rules. A `LwwRegister` membership tree would let a prune on one cluster win over a concurrent re-embed on another, silently dropping the embedding and degrading retrieval to keyword mode. A `LwwRegister` memory tree would let one of two clusters' concurrent writes to the same memory key win outright, silently discarding the other whole record - and its CRDT sub-state - instead of folding both. Under `MvRegister` each cluster mints its own dot, so concurrent writes both land and are reduced back through the memory record model's own CRDT merge on read. Every other tree defaults to `LwwRegister` - the mode consistent with its whole-value authoring - and the single-writer index-plane trees are pinned there: the startup topology guard fails validation if any of them (structural, symbol, content, cross-reference, or the vector payload and metadata projections) is enrolled under anything other than `LwwRegister`, because a CRDT merge mode on one of them would imply more than one concurrent indexer, which the single-indexer topology forbids.

The map is deliberately explicit rather than a blanket "everything is last-writer-wins": enrolling a future CRDT-authored tree under `LwwRegister` would reintroduce exactly the silent-loss bug the membership pin exists to prevent. A regression test asserts the enrolment map's keys equal the repository-context layout contract, so adding a tree to the layout without giving it a deliberate replication mode fails the build.

## Indexing roles: hub and spoke

Because hub-and-spoke is mandatory (see below), every cluster must be told which role it plays. **This is configured with one environment variable, `LATTICE_REPOCONTEXT_INDEXING_ROLE`, read once at startup:**

| Value | Role | Behaviour |
|-------|------|-----------|
| `hub` (default) | Authoritative indexer | Walks, reconciles, prunes, and re-embeds. This is the original single-cluster behaviour, so an existing deployment is unchanged. |
| `spoke` | Read-only replica | Serves retrieval and memory from the replicated trees. Its self-index grain still activates and answers reads, but never arms its timer or reminder, never walks or reconciles, and never re-embeds - the index pass is inert. |

The value is matched case-insensitively after trimming, and resolved **fail-closed**: an absent, blank, or unrecognised value falls back to `hub`, so a typo can never silently turn a cluster into an inert spoke that indexes nothing. Deploy exactly one cluster as `hub` and the rest as `spoke`.

Three properties of this knob matter operationally:

- **It is per cluster (per process), not per repository.** Indexing ownership is a cluster-level property; there is no per-repository role override.
- **It is startup-fixed.** The role is resolved once when the host is composed and read at grain activation, so changing a cluster's role means restarting it with a different value - it cannot be flipped at runtime. A spoke promoted to hub simply starts indexing on its next activation and inherits every presence bit and memory record without loss (membership converges add-wins, memory multi-master).
- **The environment variable is the supported knob.** The container host and any environment-configured deployment set it directly; it is the intended and only configuration surface for the role.

### Example: a two-cluster deployment

Set the variable in the environment of each cluster's host process - one hub, the rest spokes. `hub` is the default, so the hub can leave it unset, but setting it explicitly documents intent:

```bash
# Cluster A - the indexer
export LATTICE_REPOCONTEXT_INDEXING_ROLE=hub

# Cluster B (and any further clusters) - read-only replicas
export LATTICE_REPOCONTEXT_INDEXING_ROLE=spoke
```

With the container host, the same variable is a normal container environment entry - for example in Docker Compose:

```yaml
services:
  repocontext-hub:      # cluster A: does all indexing
    image: orleans-lattice-repocontext
    environment:
      LATTICE_REPOCONTEXT_INDEXING_ROLE: hub
      # ... cluster id, replication transport/peers, etc.

  repocontext-spoke:    # cluster B: serves replicated reads, never indexes
    image: orleans-lattice-repocontext
    environment:
      LATTICE_REPOCONTEXT_INDEXING_ROLE: spoke
      # ... cluster id, replication transport/peers, etc.
```

An unset, blank, or misspelled value resolves to `hub`, so forgetting the variable on a would-be spoke makes it a (redundant) second indexer rather than silently disabling indexing - deliberately fail-closed. Set exactly one cluster to `hub`.

## Startup topology guard

`EnableRepoContextMultiCluster(...)` also registers a startup validator (`IValidateOptions<LatticeReplicationOptions>`) that fails fast if the resolved per-tree topology is inconsistent with the hub-and-spoke invariant. It engages only once at least one repository-context tree is enrolled (a host replicating unrelated trees is unaffected), and then asserts, for the enrolled repository-context trees:

- **Memory** stays `MvRegister` - a last-writer-wins memory tree drops one of two concurrent cross-cluster writes.
- **VectorMembership** stays `OrFlag` - a last-writer-wins membership tree drops an embedding present on one cluster and pruned on another.
- Every **single-writer index-plane tree** (structural, symbol, content, cross-reference, and the vector payload and metadata projections) stays `LwwRegister`. Enrolling one under a CRDT merge mode implies more than one concurrent indexer - active-active indexing - which the single-indexer topology forbids.

A violation aborts startup with a message naming the offending tree, its declared mode, and the required mode, so a misconfigured topology never reaches serving traffic.

## Hub-and-spoke is the only valid topology

There is exactly one valid multi-cluster indexing topology: **single-indexer hub-and-spoke**. One cluster is the hub and does all indexing; every other cluster is a spoke that serves replicated reads and never indexes. This is not one option among several - it is the only safe arrangement, and the role gate and the startup guard exist to enforce it.

**Active-active indexing is invalid and is rejected at startup.** Letting more than one cluster walk, reconcile, prune, and re-embed the same sources has no ownership arbitration: clusters race to prune and re-add the same membership bits, compute divergent projections, and drive the embedding-gap scanner in a loop. There is deliberately no configuration that turns this on; enrolling a single-writer index-plane tree under a CRDT merge mode - the only way to express "more than one concurrent indexer" - fails the startup topology guard.

"Active-active" therefore only ever describes the **data plane**, never indexing:

- **Indexing (control plane) is always single-writer.** Exactly one hub owns the walk / reconcile / prune / embed work. Spokes replicate its output and serve retrieval without re-embedding. A spoke that is later promoted to hub loses nothing, because membership converges add-wins and memory converges multi-master.
- **Reads and agent-memory writes (data plane) may be served by every cluster.** A spoke still accepts `remember` / `update` / `forget` and serves search and recall from the replicated trees; concurrent memory writes to the same key on different clusters both survive and fold through the memory record model's CRDT merge, and membership presence reconciles add-wins. This is the CRDT data-plane convergence the pinned tree modes provide - it does not make indexing active-active.

## The gap scanner stays local

The embedding-gap scanner - the maintenance pass that finds sources present in the structural trees but missing a vector - is part of the hub's index pass. It stays local to the hub: replication ships the membership, payload, and metadata trees so a peer sees which sources are already embedded, but it does not run a remote embed, and a spoke never runs the scanner at all. Enabling multi-cluster replication changes which trees converge across sites, not where embedding work happens.

## See also

- [`Orleans.Lattice.Api.Mcp.RepoContext`](../lattice.api.mcp.repocontext/README.md) - the repository-context store this add-on replicates.
- [`Orleans.Lattice.Replication`](../lattice.replication/README.md) - the replication engine, transport, and per-tree merge-mode contract this package builds on.
