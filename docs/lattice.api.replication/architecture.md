# Orleans.Lattice.Api.Replication architecture

This facade is a thin, fail-closed control layer over the replication config authority. It owns three responsibilities and nothing else: resolve and authorize the caller, delegate to the engine, and scope discovery to the caller's grant.

## The single narrowest seam

Every operation authorizes at exactly one choke point before it touches engine state. `EnableReplicationAsync` and `DisableReplicationAsync` authorize the whole target tree for the dedicated `LatticeOperation.Replication` capability through the shared `ILatticeAccessGate`; only on an allow do they call the engine authority. A deny throws `LatticeAuthorizationDeniedException` and the engine is never consulted, so a denied caller cannot author a single config dot.

Because a control operation acts on a whole tree, a partial (key-filtered) allow cannot narrow it and is treated as a deny. This mirrors the whole-tree enforcement the sibling backup facade uses.

## Delegation to the engine authority

The facade holds no replication state. It delegates to `ILatticeReplicationConfigAuthority` in [`Orleans.Lattice.Replication`](../lattice.replication/README.md), which authors the `sys-replication-config` tree:

- **Enable** fixes the merge mode at enable time by writing an add to the enablement `RwFlag` and setting the `MvRegister` mode. Enabling an already-enabled tree under a different mode is rejected with `LatticeReplicationModeChangeRejectedException`; enabling under the same mode is idempotent. A runtime precondition failure (for example a flag-based mode without a configured local replica) surfaces as `LatticeReplicationPreconditionFailedException`.
- **Enable on a non-empty tree** composes the existing snapshot bootstrap: when `bootstrapSourceClusterId` is supplied and the tree already holds rows, the authority requests a receiver-driven snapshot so the peer converges on data the change feed will not carry, then reports `BootstrapRequested = true`.
- **Disable** writes a disable-wins dot to the `RwFlag`. It pauses shipping without purging peer data, and keeps the fixed mode so a later re-enable is a clean re-bootstrap.

## Permission-scoped discovery

`GetReplicationConfigAsync` reads the authority's per-tree status set, then filters it: a tree is included only if the caller passes the same fail-closed authorization the mutating operations use. A per-tree denial is swallowed - the tree is silently omitted - so the report never reveals a tree the caller may not manage, and never throws on a partial grant.

## Fail-closed ambiguity

The merge mode is stored in an `MvRegister`, so two clusters that concurrently enable the same tree under different modes both survive convergence. When the compiled snapshot sees more than one live mode for a tree it marks the tree ambiguous and the resolver returns no mode, which pauses shipping that tree rather than silently picking a mode and dead-lettering the loser's data. The facade surfaces this as `ReplicationTreeConfigEntry.Ambiguous = true` with a null `Mode`. The engine-side detail is documented in [runtime replication configuration](../lattice.replication/runtime-config.md).

## Ordering guard

`AddLatticeReplicationApi()` resolves `ILatticeReplicationConfigAuthority` at registration and throws with an actionable message if it is absent, so a host that forgot `enableRuntimeConfig: true` on `AddLatticeReplication(...)` fails fast rather than at first call.

## See also

- [runtime replication configuration](../lattice.replication/runtime-config.md) - the config tree, static anchor, compiled snapshot, and dynamic seams the facade drives.
- [`Orleans.Lattice.Api.Replication.Grpc`](../lattice.api.replication.grpc/architecture.md) - how the gRPC binding adapts this facade over the wire.
