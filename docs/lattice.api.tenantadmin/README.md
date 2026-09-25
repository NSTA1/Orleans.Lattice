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

The facades exposed are:

- **`ILatticeTenantAdmin`** - the tenant lifecycle: create, suspend, resume, delete,
  and author per-tenant resource quotas.
- **`ILatticeTenantRegionAdmin`** - per-tenant region residency: authorize the
  allowed region set, set the residency set within it, read per-region status.
- **`ILatticeTenantSelfService`** - the read-only self-awareness counterpart: which
  tenant the caller is operating as, which tenants it may see, and one tenant's
  lifecycle and per-region residency. It holds no lifecycle authority at all, so it
  is safe to expose wherever tenancy is enabled without granting an administrative
  capability.
- **`ILatticeTenantScopedTreeAdmin`** - tree administration executed inside a single
  tenant's namespace (create/check/delete/recover/purge trees and manage per-tree
  schema policy), so a delegated tenant admin drives tree lifecycle without reaching
  outside its tenant.
- **`ILatticeTenantAccessAdmin`** - tenant access administration: list, add, and
  remove a tenant's tenant-admin subjects, so membership can change after creation
  rather than being frozen at the create-time seed.
- **`ILatticeTenantGrantAdmin`** - cross-tenant grant administration: the two-step
  agreement by which one tenant exposes a scope of its data to another (the granting
  tenant offers, the grantee approves or rejects, either party may revoke), plus a
  listing of a tenant's issued and received grants.
- **`ILatticeTenantQuotaUsage`** - the read-only usage-against-quota report: per
  dimension, a tenant's consumption next to its steady-state and burst-adjusted
  ceilings.

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
  Setting residency, reading status, tenant-scoped tree administration, tenant
  access administration, cross-tenant grant administration, and the quota-usage
  read are **tenant-admin** actions, authorized when the caller is that operator or
  a live admin subject on the tenant record (for a grant step, the record of the
  tenant whose side of the agreement the step belongs to). Both tiers are independent of the data-plane
  `DefaultEffect`, so an unmatched request always resolves to deny even under
  `DefaultEffect = Allow`.
- **Reserved default tenant.** The well-known legacy-adoption `default` tenant can
  never be suspended, deleted, or given quotas, have its admin-subject set changed,
  or be named on either side of a cross-tenant grant offer; each fails closed with a
  `ReservedTenantOperationException`, because it names the cluster's own legacy
  state. Resuming it is allowed and is an active no-op, since it can never be
  suspended in the first place.
- **Idempotent lifecycle.** Suspend/resume report whether they changed anything;
  create is not an idempotent upsert (a duplicate id fails closed with
  `TenantAlreadyExistsException`), so it can never reset or reuse another tenant's
  definition.
- **Create seeds admin subjects.** Tenant *visibility* on the read-only
  `ILatticeTenantSelfService` surface resolves from the tenant
  record's admin-subject set, so a tenant created with none is mutable but
  invisible - even to the operator who just created it. `CreateTenantAsync`
  therefore takes an optional `adminSubjects` set and seeds it onto the new
  record. Omit it and the **calling subject** is seeded, so the creator can always
  see what it created; supply one and it is used **verbatim** (the caller is not
  added on top), which is how you hand a tenant to its delegated admins in a
  single call. Every entry must be a non-blank subject id, a blank or `null`
  entry fails closed with an `ArgumentException`, and duplicates collapse. A
  caller that cannot be resolved to a subject (an anonymous or system-origin
  create) seeds nothing rather than inventing an owner; grant access explicitly
  in that case. The seeded set is echoed back on
  `TenantCreationResult.AdminSubjects`. Because membership of that set *is* the
  tenant-admin capability, an explicitly supplied id is validated against the
  upstream identity directory when one is configured and
  `LatticeIdentityDirectoryOptions.ValidationRequired` is set, exactly as the
  authorization-admin facade validates a group member: an unresolvable id is
  refused with a `LatticeDirectoryValidationException` rather than being accepted
  as a dangling grant that whoever later registers that id would inherit. The
  caller-seeded default is not directory-validated - it comes from the
  authenticated caller's own resolved subject, not from the wire.
- **Authorize, then validate, then write.** Every mutating verb first parses the
  tenant id (a purely syntactic step over the caller's own argument, which on create
  also rejects an id shadowing the `sys-` or `_lattice_` reserved namespaces with
  an `ArgumentException`), then authorizes through the fail-closed gate, and only
  then inspects its remaining arguments or touches the registry. So a denied caller
  learns nothing from the admin-subject list it supplied, from whether the tenant
  already exists, or from whether it is the reserved `default` tenant: every one of
  those checks sits behind the gate and cannot be used as an oracle.
- **Cascading delete.** Deleting a tenant cascades the delete to every tree the
  tenant owns (each `t/{tenantId}/*` tree is soft-deleted) before the registry record
  is removed.
- **Quota authoring.** `SetTenantQuotasAsync` replaces a tenant's resource quotas and
  burst allowance in one platform-operator action. Each ceiling (`MaxBytes`,
  `MaxKeys`, `MaxMemoryBytes`, `MaxTreeCount`, `MaxOpsPerSecond`) is `null` for
  unbounded on that dimension; passing `TenantQuotasDescriptor.Unbounded` lifts every
  cap again. `BurstPercent` is the transient headroom above the bounded ceilings and
  must be non-negative (a negative value fails closed with an `ArgumentException`). The
  reserved `default` tenant can never be given quotas. The authored allocation is
  surfaced back on `ILatticeTenantSelfService.GetTenantAsync` (`TenantStatusReport.Quotas`),
  so an operator can confirm it without a follow-up read.

## Registration

Register the facade on the silo (it requires the `Orleans.Lattice.Tenancy` package):

- `AddLatticeTenantAdminApi(this ISiloBuilder builder, Action<LatticeApiTenantAdminOptions>? configure = null)` -
  registers `ILatticeTenantAdmin`, `ILatticeTenantRegionAdmin`,
  `ILatticeTenantAccessAdmin`, `ILatticeTenantGrantAdmin`, and the read-only
  `ILatticeTenantSelfService` and `ILatticeTenantQuotaUsage`, together with the
  fail-closed authorizers they consult and the system-driven region backfill/drain
  promotion driver.
- `AddLatticeTenantScopedTreeAdminApi(this ISiloBuilder builder)` - registers
  `ILatticeTenantScopedTreeAdmin`.

Each call is **order-guarded at registration time**: a misordered call throws an
`InvalidOperationException` with an actionable message rather than failing
obscurely at silo start.

| Call | Must run after | Because |
|---|---|---|
| `AddLatticeTenantAdminApi` | `AddLatticeTenancy()` | The facade operates on the tenancy engine's tenant registry, so it would otherwise have no lifecycle store to act on. |
| `AddLatticeTenantScopedTreeAdminApi` | `AddLatticeTreeAdminApi()` | It delegates the whole-tree lifecycle verbs to that facade. |
| `AddLatticeTenantScopedTreeAdminApi` | `AddLatticeSchemaEnforcement()` / `AddLatticeSchemaApi()` | It delegates the per-tree schema-policy verbs to that facade. |

Each is idempotent: repeating the call layers any supplied configuration delegate
but performs the structural wiring only once.

`LatticeApiTenantAdminOptions` currently exposes no settings - it is the reserved
per-facade options seam, mirroring the sibling control facades - so the `configure`
delegate can be omitted. The knobs that shape tenancy behaviour live on the
[`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md#configuration-reference)
options instead.

## Facade method signatures

### `ILatticeTenantAdmin`

The tenant lifecycle surface (published in `Orleans.Lattice.Api.Abstractions`,
namespace `Orleans.Lattice.Api.TenantAdmin`). Each method corresponds to one gRPC RPC
in the [binding](../lattice.api.tenantadmin.grpc/README.md).

| Method | Signature |
|---|---|
| `CreateTenantAsync` | `Task<TenantCreationResult> CreateTenantAsync(string tenantId, IReadOnlyCollection<string>? adminSubjects = null, CancellationToken cancellationToken = default)` |
| `SuspendTenantAsync` | `Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `ResumeTenantAsync` | `Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `DeleteTenantAsync` | `Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `SetTenantQuotasAsync` | `Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)` |

### `ILatticeTenantRegionAdmin`

The per-tenant region-residency surface. Of the
[region sets](../lattice.tenancy/README.md#the-region-sets) it authors the
operator-owned **allowed** set and the tenant-owned **resident** set, leaving the
physical topology to the deployment. Residency is always a subset of the allowed set;
the last resident region can never be removed.

| Method | Signature |
|---|---|
| `AuthorizeAllowedRegionsAsync` | `Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)` |
| `SetResidencyAsync` | `Task<TenantResidencyChangeResult> SetResidencyAsync(string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)` |
| `GetTenantRegionStatusAsync` | `Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(string tenantId, CancellationToken cancellationToken = default)` |

Each authored set is a **replacement, not a delta**: the supplied collection
becomes the whole set, so a currently-allowed or currently-resident region absent from
it is revoked or drained.

#### Concurrent residency changes

The tenant record is CRDT-merged, and its per-region status is a map keyed by region
id, so two removals of **different** regions do not conflict - the join keeps both. A
guard that only checked before writing would therefore let two concurrent callers each
drop a different region and leave the tenant resident nowhere. `SetResidencyAsync`
closes that by checking the invariant **twice**: once before the write, and again on
the record the registry commits. Because the pre-write check has already refused the
single-writer case, a merged record with no resident region can only mean a concurrent
removal, so the call repairs the regions **it** drained (restoring their prior status
at a strictly later stamp), leaves the other caller's removal standing, and refuses
with `TenantLastRegionException`. Both racing callers are refused and the tenant keeps
at least one resident region; retrying either call afterwards meets the ordinary
pre-write guard.

For the same reason both write operations report the **merged** record rather than the
caller's pre-write view, so a concurrent change from another writer is present in the
returned region set instead of silently absent.

#### Authorization tiers

| Operation | Tier | Who may call it |
|---|---|---|
| `AuthorizeAllowedRegionsAsync` | **Operator only** | Cluster-wide `Admin` on the reserved auth policy tree. A tenant admin is denied - the allowed set is the operator's containment boundary and a tenant must not be able to widen it. |
| `SetResidencyAsync` | **Operator or tenant admin** | That operator, or a live admin subject on the tenant record. |
| `GetTenantRegionStatusAsync` | **Operator or tenant admin** | Same as above. Read-only. |

Both tiers are independent of the data-plane `DefaultEffect`, so an unmatched request
resolves to deny even under `DefaultEffect = Allow`. Every transport binding inherits
this gate rather than re-implementing it, so neither tier can be widened by reaching
the facade over the wire.

#### Domain exceptions

Each failure mode is a distinct exception type so a transport binding can map it to a
specific status rather than an opaque fault:

| Exception | Raised when |
|---|---|
| `TenantNotFoundException` | The tenant is not registered. |
| `TenantRegionNotAllowedException` | Residency was set to a region outside the allowed set, or an allowed region a tenant is still resident in was revoked. |
| `TenantLastRegionException` | The change would remove the tenant's last resident region - either as submitted, or once merged with a concurrent removal (see [Concurrent residency changes](#concurrent-residency-changes)). |
| `LatticeAuthorizationDeniedException` | The caller does not hold the required tier. |

### `ILatticeTenantSelfService`

The read-only tenant self-awareness surface (published in
`Orleans.Lattice.Api.Abstractions`, namespace `Orleans.Lattice.Api.TenantAdmin`). It
never creates, suspends, resumes, or deletes a tenant.

| Method | Signature |
|---|---|
| `GetCurrentTenantAsync` | `Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)` |
| `ListAccessibleTenantsAsync` | `Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)` |
| `GetTenantAsync` | `Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |

`GetCurrentTenantAsync` needs no administrative tier because it reports only the
caller's own context, and a caller with no tenant in context resolves to the reserved
`default` tenant. That is *not* the same as being ungated: `GetCurrentTenantAsync` and
`ListAccessibleTenantsAsync` each re-run the fail-closed tenant resolution first, so a
caller whose asserted active tenant was refused gets a `LatticeTenantAccessDeniedException`
instead of a report for a tenant it does not hold.

`ListAccessibleTenantsAsync` returns, in ascending ordinal tenant-id order, the
tenants the caller is a registered administrator of plus its own current tenant when
that is non-default, so an anonymous or non-privileged caller under the default tenant
gets an empty list. `GetTenantAsync` deliberately unifies "no such tenant" and "you may
not see this tenant" into a single `TenantNotFoundException`, so no caller can probe
for the existence of a tenant outside its authority.

### `ILatticeTenantScopedTreeAdmin`

Tree administration executed inside one tenant's namespace (namespace
`Orleans.Lattice.Api.TenantAdmin`). Names are the tenant's unqualified tree names; the
facade injects the tenant segment.

| Method | Signature |
|---|---|
| `CreateTreeAsync` | `Task<TreeCreationResult> CreateTreeAsync(string name, int? shardCount = null, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)` |
| `CheckTreeExistsAsync` | `Task<TreeExistenceResult> CheckTreeExistsAsync(string name, CancellationToken cancellationToken = default)` |
| `DeleteTreeAsync` | `Task<TreeDeletionStatus> DeleteTreeAsync(string name, CancellationToken cancellationToken = default)` |
| `RecoverTreeAsync` | `Task<TreeDeletionStatus> RecoverTreeAsync(string name, CancellationToken cancellationToken = default)` |
| `PurgeTreeAsync` | `Task<TreeDeletionStatus> PurgeTreeAsync(string name, bool confirm, CancellationToken cancellationToken = default)` |
| `GetTreeDeletionStatusAsync` | `Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(string name, CancellationToken cancellationToken = default)` |
| `SetSchemaPolicyAsync` | `Task SetSchemaPolicyAsync(string name, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)` |
| `ClearSchemaPolicyAsync` | `Task<bool> ClearSchemaPolicyAsync(string name, CancellationToken cancellationToken = default)` |
| `GetSchemaPolicyAsync` | `Task<LatticeSchemaPolicy?> GetSchemaPolicyAsync(string name, CancellationToken cancellationToken = default)` |

Every method on this facade requires an active tenant. With none in scope the
call fails closed with a `TenantScopeRequiredException` (declared in this package,
namespace `Orleans.Lattice.Api.TenantAdmin`) rather than silently operating on the
cluster-global namespace.

### `ILatticeTenantAccessAdmin`

The tenant access-administration surface (published in
`Orleans.Lattice.Api.Abstractions`, namespace `Orleans.Lattice.Api.TenantAdmin`).
Every operation is a **tenant-admin** action - authorized for the platform operator
or a live admin subject of the target tenant - and a caller holding neither is told
*denied*, never *not found*, so the surface cannot be used to enumerate tenants.

| Method | Signature |
|---|---|
| `ListAdminSubjectsAsync` | `Task<TenantAdminSubjectReport> ListAdminSubjectsAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `AddAdminSubjectAsync` | `Task<TenantAdminSubjectChangeResult> AddAdminSubjectAsync(string tenantId, string subjectId, CancellationToken cancellationToken = default)` |
| `RemoveAdminSubjectAsync` | `Task<TenantAdminSubjectChangeResult> RemoveAdminSubjectAsync(string tenantId, string subjectId, CancellationToken cancellationToken = default)` |

Add and remove are idempotent (`Changed` reports whether the set moved). An added
subject id is validated against the upstream identity directory when one is
configured and `ValidationRequired` is set, exactly as an explicit create-time seed
is. Removing a tenant's last admin subject is refused with
`TenantLastAdminSubjectException` - including when two concurrent removals of
different subjects would together empty the set, which is detected on the merged
record and repaired before the refusal - and the reserved `default` tenant's
membership can never be changed.

### `ILatticeTenantGrantAdmin`

The cross-tenant grant surface. A grant is a two-step agreement: an offer creates it
`Pending` and authorizes nothing; only the grantee's approval makes it `Active`.

| Method | Signature |
|---|---|
| `ListGrantsAsync` | `Task<TenantGrantReport> ListGrantsAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `OfferGrantAsync` | `Task<TenantGrantChangeResult> OfferGrantAsync(string granterTenantId, string granteeTenantId, string scope, TenantGrantAccess operations, CancellationToken cancellationToken = default)` |
| `ApproveGrantAsync` | `Task<TenantGrantChangeResult> ApproveGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)` |
| `RejectGrantAsync` | `Task<TenantGrantChangeResult> RejectGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)` |
| `RevokeGrantAsync` | `Task<TenantGrantChangeResult> RevokeGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)` |

Each step is authorized for the platform operator or a live admin subject of one
specific tenant: the **granting** tenant offers, the **grantee** tenant approves or
rejects, **either** party may revoke, and a listing is the listed tenant's own.
`scope` names the granting tenant's data the grant covers (a tree name or tree-name
prefix) and `operations` must not be `TenantGrantAccess.None`. An offer never
requires the grantee to exist and may not name the reserved `default` tenant on
either side. A grant that was never offered - or whose granting tenant is not
registered - is reported identically as `TenantGrantNotFoundException`; asking for
the state a grant is already in is an idempotent no-op, and a transition the
lifecycle forbids (for example approving a rejected or revoked grant) raises
`TenantGrantTransitionException` before any write.

### `ILatticeTenantQuotaUsage`

The read-only usage-against-quota surface.

| Method | Signature |
|---|---|
| `GetQuotaUsageAsync` | `Task<TenantQuotaUsageReport> GetQuotaUsageAsync(string tenantId, CancellationToken cancellationToken = default)` |

The report carries, per dimension (`Bytes`, `Keys`, `MemoryBytes`, `TreeCount`,
`OpsPerSecond`), the consumption, the steady-state ceiling, the burst-adjusted
ceiling, the live overage, and the accrued metered overage, together with the
`EnforcementScope` the figures were read under. A platform operator may read any
tenant and a live tenant admin only its own; an unauthorized tenant and an absent one
are unified into a single `TenantNotFoundException`, so the call cannot probe for
tenant existence. Authoring quotas remains the operator-only `SetTenantQuotasAsync`.

## Public model types

Results and exceptions live in `Orleans.Lattice.Api.Abstractions` under
`TenantAdmin/Model/`.

| Type | Kind | Purpose |
|---|---|---|
| `TenantCreationResult` | result | The newly created tenant, with the admin subjects seeded onto it. |
| `TenantDescriptor` | model | One tenant's identity and lifecycle status, as reported by the read-only self-service surface. |
| `TenantStatusReport` | result | One tenant's read-only lifecycle status, authored `Quotas`, and per-region residency rows. |
| `TenantStatusChangeResult` | result | Suspend/resume outcome; `Changed` reports whether state moved. |
| `TenantDeletionResult` | result | Deletion outcome, including the count of trees cascaded. |
| `TenantQuotasDescriptor` | model | A tenant's per-dimension resource ceilings (`null` = unbounded) and `BurstPercent`; `Unbounded` sentinel and `IsUnbounded` predicate. |
| `TenantQuotasUpdateResult` | result | The tenant id and the quotas now in effect after authoring. |
| `TenantLifecycleStatus` | enum | `Active` / `Suspended`. |
| `TenantRegionAuthorizationResult` | result | The resulting allowed region set. |
| `TenantResidencyChangeResult` | result | The added, removed, and resulting resident regions. |
| `TenantRegionStatusReport` | result | Per-region rows (`TenantRegionStatusDescriptor`), ordered by region id. |
| `TenantRegionStatusDescriptor` | model | One region's allowed flag and lifecycle status. |
| `TenantRegionLifecycleStatus` | enum | `None` / `Provisioning` / `Backfilling` / `Online` / `Draining` / `Offline` / `Removed`. |
| `TenantNotFoundException` | exception | No tenant with that id is registered. |
| `TenantAlreadyExistsException` | exception | A tenant with the same id is already registered. |
| `ReservedTenantOperationException` | exception | Attempted suspend, delete, set-quotas, an admin-subject add / remove, or a cross-tenant grant offer on the reserved `default` tenant. |
| `TenantRegionNotAllowedException` | exception | A residency region is not in the allowed set (or a revoked region is still resident). |
| `TenantLastRegionException` | exception | The change would remove the last resident region, as submitted or once merged with a concurrent removal. |
| `TenantAdminSubjectReport` | result | A tenant's live admin-subject set, in ordinal order. |
| `TenantAdminSubjectChangeResult` | result | An add / remove outcome: the subject, `Changed`, and the resulting admin-subject set. |
| `TenantLastAdminSubjectException` | exception | The removal would leave the tenant with no admin subjects. |
| `TenantGrantDescriptor` | model | One cross-tenant grant: granting and grantee tenant, `Scope`, `Operations`, lifecycle `State`, and `GrantId`. |
| `TenantGrantReport` | result | A tenant's `Issued` and `Received` grants, in every lifecycle state. |
| `TenantGrantChangeResult` | result | A grant step's outcome: the `Grant` as committed and `Changed`. |
| `TenantGrantAccess` | enum | `None` / `Read` / `Write` / `ReadWrite` - what a grant authorizes once active. |
| `TenantGrantLifecycleState` | enum | `Active` / `Pending` / `Rejected` / `Revoked`. |
| `TenantGrantNotFoundException` | exception | No such grant has been offered (reported identically when the granting tenant is not registered). |
| `TenantGrantTransitionException` | exception | The grant's lifecycle forbids the requested transition; carries the current and requested states. |
| `TenantQuotaUsageReport` | result | A tenant's usage against its quotas: one `TenantQuotaDimensionUsage` per dimension, `BurstPercent`, the authored `Quotas`, `HasUsage`, and the `EnforcementScope`. |
| `TenantQuotaDimensionUsage` | model | One dimension's `Usage` (`null` = not measured), `Limit` (`null` = unbounded), `BurstLimit`, live `Overage`, and accrued `MeteredOverage`. |
| `TenantQuotaEnforcementScope` | enum | `GlobalConverged` (the converged cross-cluster total) / `PerCluster` (this cluster's local share only). |

## See also

- [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md) - the core multi-tenancy
  companion (isolation, quotas, metering, residency enforcement).
- [`Orleans.Lattice.Api.TenantAdmin.Grpc`](../lattice.api.tenantadmin.grpc/README.md) -
  the code-first gRPC binding and remote client for these facades.
- [`Orleans.Lattice.Api.TreeAdmin`](../lattice.api.treeadmin/README.md) - the sibling
  tree-administration facade this one composes and mirrors.
- [MultiTenancy sample](../../samples/MultiTenancy/README.md).
