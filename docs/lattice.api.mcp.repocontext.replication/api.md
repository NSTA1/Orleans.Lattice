---
agent_spec: "docs/agents/api/mcp.json"
---

# Public API

`Orleans.Lattice.Api.Mcp.RepoContext.Replication` has a one-method public surface in the `Orleans.Lattice.Api.Mcp.RepoContext.Replication` namespace. It defines no options type of its own: replication is configured through the replication engine's `LatticeReplicationOptions`, and each cluster's indexing role through the `LATTICE_REPOCONTEXT_INDEXING_ROLE` environment variable the repository-context package reads. The [package guide](README.md) explains the tree-to-mode map and the hub-and-spoke topology; [Architecture](architecture.md) describes what the call wires.

## `EnableRepoContextMultiCluster`

Declared on the static class `RepoContextReplicationServiceCollectionExtensions`:

`public static ISiloBuilder EnableRepoContextMultiCluster(this ISiloBuilder builder, Action<LatticeReplicationOptions> configure, bool enableRuntimeConfig = false)`

| Parameter | Meaning |
|---|---|
| `builder` | The silo builder. `AddLattice(...)` must already be registered on it, as for any replication add-on. Must not be `null`. |
| `configure` | Populates the cluster's own `LatticeReplicationOptions` (cluster id, transport, peers, secrets, per-tree settings) exactly as for `AddLatticeReplication`, to which it is forwarded. The repository-context tree-mode map is merged on top of whatever it sets. Must not be `null`. |
| `enableRuntimeConfig` | Forwarded to `AddLatticeReplication`. `true` also enrols the reserved runtime replication-configuration tree and installs the dynamic replication-config control plane; the `false` default configures replication purely from the static map. |

It returns the same `builder` for chaining and throws `ArgumentNullException` when `builder` or `configure` is `null`. The call:

- registers the replication engine (`AddLatticeReplication`) with the caller's settings;
- enrols every replicated repository-context tree in `LatticeReplicationOptions.ReplicatedTrees` under its required merge mode, pinning the agent-memory tree to `MvRegister` and the vector-membership tree to `OrFlag` over any host declaration;
- authors agent-memory CRDT writes under this cluster's replication `ClusterId`, so concurrent writes to one memory key on two clusters both survive;
- registers a topology validator over `LatticeReplicationOptions` that rejects an enrolment inconsistent with single-indexer hub-and-spoke.

The exact tree-to-mode map, which overrides are honoured, and the guard's rules are in the [package guide](README.md#the-tree-to-mode-map).

## Configuration

| Setting | Where | Reference |
|---|---|---|
| Cluster id, transport, peers, secrets, per-tree replication options | `LatticeReplicationOptions`, through `configure` | [`Orleans.Lattice.Replication`](../lattice.replication/README.md) |
| The cluster's indexing role (`hub` or `spoke`) | `LATTICE_REPOCONTEXT_INDEXING_ROLE` | [Indexing roles](README.md#indexing-roles-hub-and-spoke), [repository-context configuration](../lattice.api.mcp.repocontext/configuration.md#environment-variables-the-package-reads) |

## Related

- [Architecture](architecture.md)
- [Package guide](README.md)
- [`Orleans.Lattice.Api.Mcp.RepoContext` public API](../lattice.api.mcp.repocontext/api.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Mcp.RepoContext.Replication.RepoContextReplicationServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp.repocontext.replication/RepoContextReplicationServiceCollectionExtensions.cs) (line 13).

`public static class RepoContextReplicationServiceCollectionExtensions`

- `public static ISiloBuilder EnableRepoContextMultiCluster(this ISiloBuilder builder, Action<LatticeReplicationOptions> configure, bool enableRuntimeConfig = false)`
