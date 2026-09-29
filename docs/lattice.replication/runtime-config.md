# Runtime replication configuration

Cross-cluster replication can be turned on and off **per tree at runtime**, without a redeploy, and the decision converges across every enrolled peer on its own. This page covers the engine-side machinery; the operator-facing control surface (the facade, its gRPC binding, and the MCP tools) is documented under [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/README.md).

## The configuration is a replicated tree

Replication configuration is not a bespoke store and not a cross-cluster handshake. It is itself a **replicated CRDT system tree**, `LatticeSystemTreeNames.ReplicationConfig` (`sys-replication-config`), dogfooding the exact pattern the engine already uses for its membership and auth-policy system trees.

The tree holds a single OR-Map, stored under the well-known key `LatticeSystemTreeNames.ReplicationConfigMapKey` (`config`) and keyed by target tree id. Each value is a small composite CRDT record (`LatticeReplicationConfigEntry`):

- **Enablement** is a disable-wins `RwFlag`. Enabling adds an enable dot; disabling adds a disable dot that wins, so a concurrent enable and disable resolves to disabled - the safe direction.
- **Merge mode** is an `MvRegister` holding the encoded `LatticeMergeMode`. Two clusters that concurrently enable the same tree under different modes both survive convergence, so a divergent mode is **detectable** rather than silently overwritten. Two clusters that concurrently enable it under the **same** mode also leave two live values, but they agree, so the tree resolves to that mode and keeps replicating. An `MvRegister` is used deliberately in place of an `LwwRegister`, whose last-writer-wins contract would drop the loser under a concurrent multi-cluster write - exactly the correctness hazard here.

Because the configuration is a converging tree, an operator flips a tree on once, on any cluster, and every peer converges to the same decision through normal replication. Per-cluster propagation is not re-consented; the trust boundary is the existing peer enrolment.

## The static anchor

The config tree must itself replicate before it can carry anything, so it is statically enrolled under a fixed merge mode on every cluster by the opt-in `enableRuntimeConfig` flag on `AddLatticeReplication(...)`, mirroring the sibling `ReplicateLatticeSystemTrees()`. This is the one static anchor the runtime path rests on. A host opts in on the engine call:

```csharp verify
siloBuilder.AddLatticeReplication(
    options =>
    {
        options.ClusterId = "site-a";
        options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
        {
            ["catalog"] = LatticeMergeMode.LwwRegister,
        };
        options.ReplicationPeers = new[] { "site-b" };
    },
    enableRuntimeConfig: true);
```

The existing static replicated-tree options map (`LatticeReplicationOptions.ReplicatedTrees`) stays as a **seed and fallback**, so a deployment that configures its replicated set statically is unaffected: static entries still apply, and the runtime tree layers on top.

### Installed apps enrol through the runtime configuration

An [installed app](../lattice.apps/README.md#replication-intent) that declares replication for its trees does not add them to `ReplicatedTrees`. Each installation enrols and unenrols its own tenant-composed trees through `ILatticeReplicationConfigAuthority` as the app is enabled, reconciled, upgraded and uninstalled, so an operator sees app trees in this runtime configuration - under the tree ids the installation actually uses - and not in the static map. App enrolment therefore requires `enableRuntimeConfig: true`; without the runtime configuration there is no authority to enrol through, and an app's replication intent has no effect. An app never disables an enrolment it did not make: a tree an operator had already enrolled before the app claimed it stays enrolled when the app is uninstalled.

## The compiled snapshot

A grain call must never sit on the commit hot path, so the config tree is projected into an in-memory snapshot. The compiled-snapshot maintainer observes commits to the config tree through the core mutation-observer hook (`IMutationObserver`) and rebuilds a `treeId -> { enabled, mode, ambiguous }` projection whenever the tree advances, mirroring how the auth stack compiles its policy snapshot. A commit only schedules a coalesced background rebuild, so the snapshot is eventually consistent - an edit is reflected shortly after it commits - and a read is a lock-light lookup against a fixed epoch, not a grain round-trip. That rebuild runs only on the silo that observes the commit: each silo keeps its own snapshot, the mutation-observer hook fires only on the silo hosting the config-tree grain that commits the change, and the maintainer has no other rebuild trigger after its first build, so on a multi-silo cluster the other silos keep serving the snapshot they built on first use until one of them observes a later commit itself or restarts.

Two dynamic seams read that snapshot:

- **`IReplicatedTreeMembership`** answers "should this tree replicate right now?" from the snapshot (unioned with the static seed).
- **`ILatticeMergeModeResolver`** answers "under which merge mode?" from the snapshot (falling back to the static seed whenever the runtime tree holds no enabled, unambiguous mode for the tree - a runtime-disabled entry included - but never for an ambiguous mode; see [Fail-closed ambiguity](#fail-closed-ambiguity)).

The boot-time flag-mode and merge-mode startup guards become runtime precondition checks, so a mode that is only valid with a configured local replica is validated when a tree is enabled, not only at startup; a failed check - or a host with no `ClusterId` to stamp the enablement dot with - throws `LatticeReplicationPreconditionFailedException`.

## Fail-closed ambiguity

When the `MvRegister` holds live values that decode to more than one **distinct** mode for a tree, the snapshot marks that tree **ambiguous**. Several live values that all carry the same mode - two regions each enabling the tree under the mode the other also chose - are not ambiguous: the tree resolves to that mode. When the modes do diverge, the resolver returns no mode, even when the tree is also declared statically. The commit-time observer then stops nudging the tree's shippers. The per-peer shipper does not currently gate on the resolution, though: it keeps draining the tree's WAL on its phase timer and ships new locally-authored entries with the batch header's mode set to `LwwRegister`. A peer whose own resolution of the tree also returns no mode - the converged case - drops them at its receiver-side enrollment gate (logged, not dead-lettered); a peer that has not converged yet and still resolves a mode applies them when that mode is `LwwRegister` and dead-letters them as a `mode_mismatch` otherwise. That rejection is not a deferral, so the peer still acknowledges the batch and the sender advances its cursor past the dropped entries; they are not re-shipped once the tree resolves to a mode again, so the peer diverges for those writes until a snapshot re-seed (for example an enable that names a bootstrap source cluster) or the anti-entropy repair pipeline brings it back in line. Resolution is an operator action: disable the tree (which the disable-wins flag settles unambiguously) and re-enable it under the intended single mode.

The resolver itself never picks one of the divergent modes: it reports the tree as ambiguous until an operator settles it.

## Enable, disable, and mode changes

The engine authoring seam is `ILatticeReplicationConfigAuthority`, installed only when `AddLatticeReplication(..., enableRuntimeConfig: true)` is called:

- **Enable** fixes the merge mode at enable time. Enabling an already-enabled tree under the same mode is idempotent, and so is enabling it from two regions at once under the same mode: the concurrent enablements converge on that mode rather than on an ambiguous one. Under a **different** mode - or while its mode is ambiguous - it is rejected (`LatticeReplicationModeChangeRejectedException`), because a mode change would reinterpret every already-shipped value under a new merge algebra. The sanctioned way to change a mode is to disable, then re-enable under the new mode; naming a bootstrap source cluster on that enable re-seeds a tree that already holds data (next bullet).
- **Enable on a non-empty tree** composes the existing snapshot bootstrap: when a bootstrap source cluster is named and the local tree already holds rows, the enabling cluster requests a receiver-driven snapshot (through `ILatticeReplicationAdmin.RequestSnapshotAsync`, which drives `ILatticeBootstrapCoordinator`) that pulls the named source cluster's pre-existing rows - which the change feed will not carry - into the local tree. The bootstrap cannot push the local rows to remote peers; they converge on those rows through their own receiver-side bootstrap, such as an operator re-seed on that peer. An enable that names a source over an empty local tree requests no bootstrap. The request takes the rate-limited routine re-seed path, so a repeat within `LatticeReplicationOptions.OperatorReseedMinInterval` (default 1 minute) of the last re-seed that silo honoured for the same tree and source cluster is not dispatched - and the enable result still reports `BootstrapRequested` as `true`.
- **Disable** writes the disable-wins dot. Disabling a tree that is absent or already disabled is an idempotent no-op; authoring a real disable dot needs a configured `ClusterId`, and a host without one throws `LatticeReplicationPreconditionFailedException`. The tree drops out of the runtime enrollment: unless it is also declared statically (see [Reading the effective configuration](#reading-the-effective-configuration)), membership no longer reports it as replicated and the resolver returns no mode. As with an ambiguous tree, the shipper does not currently stop shipping it, and a converged peer drops those entries while the sender advances past them (see [Fail-closed ambiguity](#fail-closed-ambiguity)). Disable never purges data already replicated to peers, and it keeps the entry (with its last mode) in the config tree. A later enable re-fixes the mode to the value it requests; no bootstrap runs unless that enable names a bootstrap source cluster.

## Reading the effective configuration

`ILatticeReplicationConfigAuthority.GetTreeStatusAsync` / `GetAllTreeStatusesAsync` - and therefore the operator-facing control facade, its gRPC binding, and the `lattice_replication_get_config` MCP tool - report the **union of both enrollment sources**, not just the runtime tree. Authoring only ever writes the config OR-Map, but the commit path resolves against the static map too, so a report limited to the OR-Map would tell an operator "no trees replicate here" on an estate configured entirely through `LatticeReplicationOptions.ReplicatedTrees` and demonstrably shipping.

The projection applies exactly the precedence the commit-path merge-mode resolver applies, so the report always describes what the host actually does:

1. An **ambiguous** runtime mode fails closed at resolution - the mode is reported `null` - but shipping does not pause: a converged peer drops the tree's new entries and the sender advances past them (see [Fail-closed ambiguity](#fail-closed-ambiguity)). A static declaration never resolves the ambiguity.
2. Otherwise an **enabled runtime entry with an unambiguous mode** wins, and that mode is reported.
3. Otherwise the **static declaration** is the floor that keeps the tree shipping, and its mode is reported.
4. Otherwise - a runtime-only entry that yields no enabled, unambiguous mode, such as a disabled tree - the entry's own enablement flag and the mode it retains (`null` when it holds none) are reported, with `Source` `Runtime`.

Each status carries a `Source` (`LatticeReplicationEnrollmentSource`) naming which of the two is in force: `Runtime`, `Static`, or `RuntimeAndStatic`. The distinction is operationally load-bearing, because a tree reported `Static` is **not** turned off by a runtime disable - the static map is a floor, so the resolver falls back to it and the tree keeps shipping. Such a tree is disabled by removing it from the deployment configuration.

## See also

- [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/README.md) - the control facade an operator drives this through.
- [`Orleans.Lattice.Api.Replication.Grpc`](../lattice.api.replication.grpc/README.md) - the remote gRPC binding of that facade.
- [System-Tree Replication](system-tree-replication.md) - the membership / auth-policy system trees this feature dogfoods.
- [Replication Modes](replication-modes.md) - `LatticeMergeMode` selection and the static per-tree opt-in.
- [Snapshot Bootstrap](snapshot-bootstrap.md) - the point-in-time bootstrap an enable-on-non-empty-tree composes with.
