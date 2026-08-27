# Orleans.Lattice.Api.Replication API reference

The package exposes one registration entry point, one public options type, and a set of model records. The control contract itself, `ILatticeReplicationControl`, is defined in the shared [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) package.

## Registration

| Member | Signature | Purpose |
|---|---|---|
| `AddLatticeReplicationApi` | `ISiloBuilder AddLatticeReplicationApi(this ISiloBuilder builder, Action<LatticeApiReplicationOptions>? configure = null)` | Registers the replication control facade on the silo. Must be called after `AddLatticeReplication(..., enableRuntimeConfig: true)`; calling it first throws at registration with an actionable message. |

## Facade

`ILatticeReplicationControl` (defined in `Orleans.Lattice.Api.Abstractions`) is the single control surface every transport binding adapts over.
Every `treeId` these operations accept is a **tenant-local name**: the facade resolves it to its effective, tenant-scoped id through `ITenantContextResolver.ResolveEffectiveTreeIdAsync` at the entry point and uses that one id for **both** the authorization check and the operation, so a verb can never authorize one tree and act on another. With the tenancy add-on absent the bare name is returned unchanged, so behaviour is byte-for-byte as before; with it registered an unqualified name is scoped into the active tenant's `t/{tenant}/{name}` namespace, an already-qualified or reserved name is returned unchanged, and a caller with no valid active tenant fails closed with a `LatticeTenantAccessDeniedException`. See [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md).

| Operation | Signature | Notes |
|---|---|---|
| Enable replication | `Task<ReplicationEnableResult> EnableReplicationAsync(string treeId, LatticeMergeMode mode, string? bootstrapSourceClusterId = null, CancellationToken cancellationToken = default)` | Authorizes the tree fail-closed, then enables it under the fixed `mode`. Rejects an in-place mode change on an already-enabled tree. When `bootstrapSourceClusterId` is supplied and the tree already holds data, requests a snapshot bootstrap. |
| Disable replication | `Task<ReplicationDisableResult> DisableReplicationAsync(string treeId, CancellationToken cancellationToken = default)` | Authorizes the tree fail-closed, then pauses shipping without purging peer data. Idempotent. |
| Get replication config | `Task<ReplicationConfigReport> GetReplicationConfigAsync(CancellationToken cancellationToken = default)` | Returns a permission-scoped report; trees the caller may not manage are omitted rather than throwing. |

## Model types

All model records live in `Orleans.Lattice.Api.Abstractions` (namespace `Orleans.Lattice.Api.Replication`) and are Orleans-serializable with stable aliases.

### `ReplicationEnableResult`

| Member | Type | Meaning |
|---|---|---|
| `TreeId` | `string` | The tree that was enabled. |
| `Mode` | `LatticeMergeMode` | The merge mode now fixed for the tree. |
| `AlreadyEnabled` | `bool` | `true` when the tree was already enabled under the same mode (idempotent enable). |
| `BootstrapRequested` | `bool` | `true` when a snapshot bootstrap was requested for a non-empty tree. |

### `ReplicationDisableResult`

| Member | Type | Meaning |
|---|---|---|
| `TreeId` | `string` | The tree that was disabled. |
| `AlreadyDisabled` | `bool` | `true` when the tree was already disabled (idempotent disable). |

### `ReplicationConfigReport`

| Member | Type | Meaning |
|---|---|---|
| `Trees` | `IReadOnlyList<ReplicationTreeConfigEntry>` | The per-tree entries the caller is authorized to see. `ReplicationConfigReport.Empty` is the canonical empty report. |

### `ReplicationTreeConfigEntry`

| Member | Type | Meaning |
|---|---|---|
| `TreeId` | `string` | The configured tree. |
| `Enabled` | `bool` | Whether replication is currently enabled for the tree. |
| `Mode` | `LatticeMergeMode?` | The fixed merge mode, or `null` when the mode is ambiguous (see `Ambiguous`). |
| `Ambiguous` | `bool` | `true` when concurrent divergent mode writes have not yet been resolved; while ambiguous the engine pauses shipping the tree rather than picking a mode. |

## Exceptions

| Exception | Raised when |
|---|---|
| `LatticeAuthorizationDeniedException` | The caller is not authorized for the `LatticeOperation.Replication` capability on the target tree. |
| `ArgumentException` | `treeId` is null or empty. |
| `LatticeReplicationModeChangeRejectedException` | An enable would change the merge mode of an already-enabled tree. Carries `CurrentMode`, `RequestedMode`, and `CurrentModeAmbiguous`. (Defined in `Orleans.Lattice.Replication`.) |
| `LatticeReplicationPreconditionFailedException` | A runtime precondition for enabling replication was not met (for example a flag-based merge mode without a configured local replica). (Defined in `Orleans.Lattice.Replication`.) |

## See also

- [Configuration](configuration.md) - the `LatticeApiReplicationOptions` properties.
- [Architecture](architecture.md) - how each operation composes authorization and engine delegation.
