# Orleans.Lattice.Api.TenantAdmin

Transport-agnostic **tenant administration** and **region-residency** control
facades for Orleans.Lattice multi-tenancy: one coherent, discoverable, authorized
surface for the tenant lifecycle and each tenant's region residency, plus a
tenant-scoped tree-administration facade. Every transport binding (the
[gRPC service](../lattice.api.tenantadmin.grpc/README.md), the MCP tool group) is a
thin adapter over these surfaces, so the control semantics are written and tested
once and no transport concern leaks into the control logic.

## What is it?

This package is the control plane for the [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md)
companion. It mirrors the [TreeAdmin](../lattice.api.treeadmin/README.md) packaging
convention exactly: the contracts live in `Orleans.Lattice.Api.Abstractions` (under
`TenantAdmin/`), the implementations here, the gRPC binding in a sibling package, and
an MCP `TenantAdmin` tool group. It **composes** the existing TreeAdmin, Schema,
Backup, and Replication facades rather than reimplementing them.

Three facades are exposed:

- **`ILatticeTenantAdmin`** - the tenant lifecycle: create, suspend, resume, delete.
- **`ILatticeTenantRegionAdmin`** - per-tenant region residency: authorize the
  allowed region set, set the residency set within it, read per-region status.
- **`ILatticeTenantScopedTreeAdmin`** - tree administration executed inside a single
  tenant's namespace (create/check/delete/recover/purge trees and manage per-tree
  schema policy), so a delegated tenant admin drives tree lifecycle without reaching
  outside its tenant.

## Core properties

- **Fail-closed authorization.** Every operation authorizes the caller through the
  Lattice access gate before it touches the tenant registry. An unauthenticated
  caller, or one the gate denies, is refused with a
  `LatticeAuthorizationDeniedException` and no change is made. The binding layer
  additionally gates the whole surface behind an explicit opt-in capability, so a
  cluster that does not enable it exposes nothing.
- **Two-tier governance.** Tenant lifecycle and allowed-region authorization are
  **platform-operator** actions (cluster-wide `Admin` on the reserved auth policy
  tree, which the gate's control-plane isolation grants only to a platform operator).
  Setting residency, reading status, and tenant-scoped tree administration are
  **tenant-admin** actions, authorized when the caller is that operator or a live
  admin subject on the tenant record. Both tiers are independent of the data-plane
  `DefaultEffect`, so an unmatched request always resolves to deny even under
  `DefaultEffect = Allow`.
- **Reserved default tenant.** The well-known legacy-adoption `default` tenant can
  never be suspended or deleted; those operations fail closed with a
  `ReservedTenantOperationException`, because it names the cluster's own legacy state.
- **Idempotent lifecycle.** Suspend/resume report whether they changed anything;
  create is not an idempotent upsert (a duplicate id fails closed with
  `TenantAlreadyExistsException`), so it can never reset or reuse another tenant's
  definition.
- **Cascading delete.** Deleting a tenant cascades the delete to every tree the
  tenant owns (each `t/{tenantId}/*` tree is soft-deleted) before the registry record
  is removed.

## Registration

Register the facade on the silo (it requires the `Orleans.Lattice.Tenancy` package):

- `AddLatticeTenantAdminApi(this ISiloBuilder builder, Action<LatticeApiTenantAdminOptions>? configure = null)` -
  registers `ILatticeTenantAdmin` and `ILatticeTenantRegionAdmin`.
- `AddLatticeTenantScopedTreeAdminApi(this ISiloBuilder builder)` - registers
  `ILatticeTenantScopedTreeAdmin`.

## Facade method signatures

### `ILatticeTenantAdmin`

The tenant lifecycle surface (published in `Orleans.Lattice.Api.Abstractions`,
namespace `Orleans.Lattice.Api.TenantAdmin`). Each method corresponds to one gRPC RPC
in the [binding](../lattice.api.tenantadmin.grpc/README.md).

| Method | Signature |
|---|---|
| `CreateTenantAsync` | `Task<TenantCreationResult> CreateTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `SuspendTenantAsync` | `Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `ResumeTenantAsync` | `Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `DeleteTenantAsync` | `Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |

### `ILatticeTenantRegionAdmin`

The per-tenant region-residency surface. Residency is always a subset of the allowed
set; the last resident region can never be removed.

| Method | Signature |
|---|---|
| `AuthorizeAllowedRegionsAsync` | `Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)` |
| `SetResidencyAsync` | `Task<TenantResidencyChangeResult> SetResidencyAsync(string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)` |
| `GetTenantRegionStatusAsync` | `Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(string tenantId, CancellationToken cancellationToken = default)` |

`AuthorizeAllowedRegionsAsync` is an **operator** action; `SetResidencyAsync` and
`GetTenantRegionStatusAsync` are **tenant-admin** actions.

### `ILatticeTenantScopedTreeAdmin`

Tree administration executed inside one tenant's namespace (namespace
`Orleans.Lattice.Api.TenantAdmin`). Names are the tenant's unqualified tree names; the
facade injects the tenant segment.

| Method | Signature |
|---|---|
| `CreateTreeAsync` | `Task<TreeCreationResult> CreateTreeAsync(...)` |
| `CheckTreeExistsAsync` | `Task<TreeExistenceResult> CheckTreeExistsAsync(...)` |
| `DeleteTreeAsync` | `Task<TreeDeletionStatus> DeleteTreeAsync(...)` |
| `RecoverTreeAsync` | `Task<TreeDeletionStatus> RecoverTreeAsync(...)` |
| `PurgeTreeAsync` | `Task<TreeDeletionStatus> PurgeTreeAsync(...)` |
| `GetTreeDeletionStatusAsync` | `Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(...)` |
| `SetSchemaPolicyAsync` | `Task SetSchemaPolicyAsync(...)` |
| `ClearSchemaPolicyAsync` | `Task<bool> ClearSchemaPolicyAsync(...)` |
| `GetSchemaPolicyAsync` | `Task<LatticeSchemaPolicy?> GetSchemaPolicyAsync(...)` |

## Public model types

Results and exceptions live in `Orleans.Lattice.Api.Abstractions` under
`TenantAdmin/Model/`.

| Type | Kind | Purpose |
|---|---|---|
| `TenantCreationResult` | result | The newly created tenant. |
| `TenantStatusChangeResult` | result | Suspend/resume outcome; `Changed` reports whether state moved. |
| `TenantDeletionResult` | result | Deletion outcome, including the count of trees cascaded. |
| `TenantLifecycleStatus` | enum | `Active` / `Suspended`. |
| `TenantRegionAuthorizationResult` | result | The resulting allowed region set. |
| `TenantResidencyChangeResult` | result | The added, removed, and resulting resident regions. |
| `TenantRegionStatusReport` | result | Per-region rows (`TenantRegionStatusDescriptor`), ordered by region id. |
| `TenantRegionStatusDescriptor` | model | One region's allowed flag and lifecycle status. |
| `TenantRegionLifecycleStatus` | enum | `None` / `Provisioning` / `Backfilling` / `Online` / `Draining` / `Offline` / `Removed`. |
| `TenantNotFoundException` | exception | No tenant with that id is registered. |
| `TenantAlreadyExistsException` | exception | A tenant with the same id is already registered. |
| `ReservedTenantOperationException` | exception | Attempted suspend/delete of the reserved `default` tenant. |
| `TenantRegionNotAllowedException` | exception | A residency region is not in the allowed set (or a revoked region is still resident). |
| `TenantLastRegionException` | exception | The change would remove the last resident region. |

## See also

- [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md) - the core multi-tenancy
  companion (isolation, quotas, metering, residency enforcement).
- [`Orleans.Lattice.Api.TenantAdmin.Grpc`](../lattice.api.tenantadmin.grpc/README.md) -
  the code-first gRPC binding and remote client for these facades.
- [`Orleans.Lattice.Api.TreeAdmin`](../lattice.api.treeadmin/README.md) - the sibling
  tree-administration facade this one composes and mirrors.
- [MultiTenancy sample](../../samples/MultiTenancy/README.md).
