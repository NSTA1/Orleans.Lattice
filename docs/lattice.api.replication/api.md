# Orleans.Lattice.Api.Replication API reference

The package exposes one registration entry point and one public options type. The control contract itself, `ILatticeReplicationControl`, and the model records it returns are defined in the shared [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) package.

## Registration

| Member | Signature | Purpose |
|---|---|---|
| `AddLatticeReplicationApi` | `ISiloBuilder AddLatticeReplicationApi(this ISiloBuilder builder, Action<LatticeApiReplicationOptions>? configure = null)` | Registers the replication control facade on the silo. Must be called after `AddLatticeReplication(..., enableRuntimeConfig: true)`; calling it first throws at registration with an actionable message. |

## Facade

`ILatticeReplicationControl` (defined in `Orleans.Lattice.Api.Abstractions`) is the single control surface every transport binding adapts over.
Every `treeId` these operations accept is a **tenant-local name**: the facade resolves it to its effective, tenant-scoped id through `ITenantContextResolver.ResolveEffectiveTreeIdAsync` at the entry point and uses that one id for **both** the authorization check and the operation, so a verb can never authorize one tree and act on another. With the tenancy add-on absent - or registered, but with no active tenant asserted, which resolves the default tenant - the bare name is returned unchanged, so behaviour is byte-for-byte as before. Under an asserted active tenant an unqualified name is scoped into that tenant's `t/{tenant}/{name}` namespace, and an already-qualified `t/` id or a `_lattice_` system-tree name passes through unchanged (a well-formed foreign `t/{other}/{name}` is left to the tenancy access gate to adjudicate). The call fails closed with a `LatticeTenantAccessDeniedException` when the asserted tenant fails validation against the caller's own membership, or when it names a `sys-` tree or a malformed `t/` id that belongs to no tenant. See [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md).

| Operation | Signature | Notes |
|---|---|---|
| Enable replication | `Task<ReplicationEnableResult> EnableReplicationAsync(string treeId, LatticeMergeMode mode, string? bootstrapSourceClusterId = null, CancellationToken cancellationToken = default)` | Authorizes the tree fail-closed, then enables it under the fixed `mode`. Rejects an in-place mode change on an already-enabled tree. When `bootstrapSourceClusterId` is supplied and the tree already holds data, requests a snapshot bootstrap. |
| Disable replication | `Task<ReplicationDisableResult> DisableReplicationAsync(string treeId, CancellationToken cancellationToken = default)` | Authorizes the tree fail-closed, then disables its runtime enrollment without purging peer data. Idempotent. |
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
| `AlreadyDisabled` | `bool` | `true` when the tree was already disabled or was never configured (idempotent disable). |

### `ReplicationConfigReport`

| Member | Type | Meaning |
|---|---|---|
| `Trees` | `IReadOnlyList<ReplicationTreeConfigEntry>` | The per-tree entries the caller is authorized to see. `ReplicationConfigReport.Empty` is the canonical empty report. |

### `ReplicationTreeConfigEntry`

| Member | Type | Meaning |
|---|---|---|
| `TreeId` | `string` | The enrolled tree. |
| `Enabled` | `bool` | Whether the tree is enrolled: the runtime enablement flag when the runtime entry is in force, and always `true` when the static declaration is - the static map is a floor, so a runtime disable does not remove it. An ambiguous entry reports its runtime flag, because ambiguity wins over the static declaration. |
| `Mode` | `LatticeMergeMode?` | The merge mode in force (for a disabled tree known only to the runtime config, the mode its entry retains), or `null` when no mode has been assigned or the mode is ambiguous (see `Ambiguous`). |
| `Ambiguous` | `bool` | `true` when concurrent divergent mode writes have not yet been resolved. Resolution then fails closed - no mode is picked and `Mode` is `null` - but shipping does not pause. Ambiguity wins over a static declaration. |
| `Source` | `ReplicationEnrollmentSource` | Which enrollment source put the entry in force. Defaults to `Runtime`. |

### `ReplicationEnrollmentSource`

| Value | Meaning |
|---|---|
| `Runtime` | Only the runtime config tree declares the tree, and its entry is in force. The default, so an entry received from a peer predating this member reads as the runtime enrollment the report has always described. |
| `Static` | The static deployment-time replicated-tree map puts the tree in force: either the runtime config tree has no entry for it, or its entry yields no enabled unambiguous mode and the resolver falls back to the static declaration. A runtime disable therefore does not change the mode such a tree resolves to; the deployment configuration does. |
| `RuntimeAndStatic` | Both sources declare the tree and the runtime entry is in force, so the reported mode is the runtime-fixed mode - or none, when that entry's mode is ambiguous. |

## Exceptions

| Exception | Raised when |
|---|---|
| `LatticeAuthorizationDeniedException` | The caller is not authorized for the `LatticeOperation.Replication` capability on the target tree. |
| `ArgumentException` | `treeId` is null or empty. |
| `LatticeTenantAccessDeniedException` | The tenancy add-on is registered and the request asserts an active tenant the caller may not act as (an anonymous caller never can), or, under an asserted tenant, names a `sys-` tree or a malformed `t/` id, so the tenant-local `treeId` cannot be resolved. (Defined in `Orleans.Lattice`.) |
| `LatticeReplicationModeChangeRejectedException` | An enable would change the merge mode of an already-enabled tree, or targets an enabled tree whose mode is currently ambiguous. Carries `CurrentMode`, `RequestedMode`, and `CurrentModeAmbiguous`. (Defined in `Orleans.Lattice.Replication`.) |
| `LatticeReplicationPreconditionFailedException` | A runtime precondition for authoring the change was not met: no local replica id is configured - the config entry's flag dots are stamped with it, so both an enable and the disable of an enabled tree need one - or a flag-based merge mode is requested without one. (Defined in `Orleans.Lattice.Replication`.) |

## See also

- [Configuration](configuration.md) - the `LatticeApiReplicationOptions` properties.
- [Architecture](architecture.md) - how each operation composes authorization and engine delegation.
