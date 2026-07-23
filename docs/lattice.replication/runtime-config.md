# Runtime replication configuration

Cross-cluster replication can be turned on and off **per tree at runtime**, without a redeploy, and the decision converges across every enrolled peer on its own. This page covers the engine-side machinery; the operator-facing control surface (the facade, its gRPC binding, and the MCP tools) is documented under [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/README.md).

## The configuration is a replicated tree

Replication configuration is not a bespoke store and not a cross-cluster handshake. It is itself a **replicated CRDT system tree**, `LatticeSystemTreeNames.ReplicationConfig` (`sys-replication-config`), dogfooding the exact pattern the engine already uses for its membership and auth-policy system trees.

The tree is an OR-Map keyed by target tree id. Each value is a small composite CRDT record (`LatticeReplicationConfigEntry`):

- **Enablement** is a disable-wins `RwFlag`. Enabling adds an enable dot; disabling adds a disable dot that wins, so a concurrent enable and disable resolves to disabled - the safe direction.
- **Merge mode** is an `MvRegister<LatticeMergeMode>`. Two clusters that concurrently enable the same tree under different modes both survive convergence, so a divergent mode is **detectable** rather than silently overwritten. An `MvRegister` is used deliberately in place of an `LwwRegister`, whose last-writer-wins contract would drop the loser under a concurrent multi-cluster write - exactly the correctness hazard here.

Because the configuration is a converging tree, an operator flips a tree on once, on any cluster, and every peer converges to the same decision through normal replication. Per-cluster propagation is not re-consented; the trust boundary is the existing peer enrolment.

## The static anchor

The config tree must itself replicate before it can carry anything, so it is statically enrolled under a fixed merge mode on every cluster by the opt-in `enableRuntimeConfig` flag on `AddLatticeReplication(...)`, mirroring the sibling `ReplicateLatticeSystemTrees()`. This is the one static anchor the runtime path rests on. A host opts in on the engine call:

```csharp
siloBuilder
    .AddLatticeReplication(/* ... */, enableRuntimeConfig: true);
```

The existing static replicated-tree options map (`LatticeReplicationOptions.ReplicatedTrees`) stays as a **seed and fallback**, so a deployment that configures its replicated set statically is unaffected: static entries still apply, and the runtime tree layers on top.

## The compiled snapshot

A grain call must never sit on the commit hot path, so the config tree is projected into an in-memory snapshot. The compiled-snapshot maintainer observes the config tree's change feed and rebuilds a `treeId -> { enabled, mode, ambiguous }` projection whenever the tree advances, mirroring how the auth stack compiles its policy snapshot. The snapshot is invalidated and recompiled off the change feed, so a read is a lock-light lookup against a fixed epoch, not a grain round-trip.

Two dynamic seams read that snapshot:

- **`IReplicatedTreeMembership`** answers "should this tree replicate right now?" from the snapshot (unioned with the static seed).
- **`ILatticeMergeModeResolver`** answers "under which merge mode?" from the snapshot (falling back to the static seed when the runtime tree has no entry).

The boot-time flag-mode and merge-mode startup guards become runtime precondition checks, so a mode that is only valid with a configured local replica is validated when a tree is enabled, not only at startup.

## Fail-closed ambiguity

When the `MvRegister` holds more than one live mode for a tree, the snapshot marks that tree **ambiguous** and the resolver returns no mode. The commit-time producer then **pauses shipping that tree** until the ambiguity is resolved, rather than silently picking a mode and dead-lettering the loser's data. Resolution is an operator action: disable the tree (which the disable-wins flag settles unambiguously) and re-enable it under the intended single mode.

This is the load-bearing safety property of the whole feature: a divergent multi-cluster mode write can never be silently resolved in favour of one side.

## Enable, disable, and mode changes

The engine authoring seam is `ILatticeReplicationConfigAuthority`, installed only when `AddLatticeReplication(..., enableRuntimeConfig: true)` is called:

- **Enable** fixes the merge mode at enable time. Enabling an already-enabled tree under the same mode is idempotent; under a **different** mode it is rejected (`LatticeReplicationModeChangeRejectedException`), because a mode change would reinterpret every already-shipped value under a new merge algebra. The sanctioned way to change a mode is to disable then re-enable, which re-bootstraps the tree cleanly.
- **Enable on a non-empty tree** composes the existing snapshot bootstrap: when a bootstrap source cluster is named and the tree already holds rows, a receiver-driven snapshot is requested (through `ILatticeBootstrapCoordinator` / `ILatticeReplicationAdmin.RequestSnapshotAsync`) so the peer converges on the pre-existing rows the change feed will not carry.
- **Disable** writes the disable-wins dot. It pauses shipping new mutations; it never purges data already replicated to peers, and it keeps the fixed mode so a later re-enable is a clean re-bootstrap.

## See also

- [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/README.md) - the control facade an operator drives this through.
- [`Orleans.Lattice.Api.Replication.Grpc`](../lattice.api.replication.grpc/README.md) - the remote gRPC binding of that facade.
- [System-Tree Replication](system-tree-replication.md) - the membership / auth-policy system trees this feature dogfoods.
- [Replication Modes](replication-modes.md) - `LatticeMergeMode` selection and the static per-tree opt-in.
- [Snapshot Bootstrap](snapshot-bootstrap.md) - the point-in-time bootstrap an enable-on-non-empty-tree composes with.
