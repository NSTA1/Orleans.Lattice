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
`TenantAdmin/`) - except `ILatticeTenantScopedTreeAdmin` and its
`TenantScopeRequiredException`, which this package declares - the implementations
here, the gRPC binding in a sibling package, and
an MCP `TenantAdmin` tool group. It **composes** the existing tree-administration
facade (`ILatticeTreeAdmin`) and the schema engine's in-process admin
(`ILatticeSchemaAdmin`) rather than reimplementing them.

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
- **`ILatticeTenantDirectoryAdmin`** and **`ILatticeTenantPolicyAdmin`** - delegated
  tenant access administration: a tenant's own groups and member set, and the
  tenant-tier rules on its own trees. Opt-in; see
  [Delegated tenant access administration](#delegated-tenant-access-administration).

The [gRPC binding](../lattice.api.tenantadmin.grpc/README.md) serves every facade
above except `ILatticeTenantScopedTreeAdmin`, which no transport binding exposes. The
MCP binding exposes the self-service reads and, behind its control opt-in, the
lifecycle, quota, and region-residency verbs and the
[delegated tenant access tools](../lattice.api.mcp/tools.md#delegated-tenant-access-tools)
(see the [tenancy guide](../lattice.tenancy/README.md#tenant-aware-surfaces)).

## Core properties

- **Fail-closed authorization.** Every lifecycle, region-residency, access, grant,
  and quota-usage operation authorizes the caller before it changes anything. An
  unauthenticated caller, or one the gate denies, is refused with a
  `LatticeAuthorizationDeniedException` (unified into `TenantNotFoundException` on
  the quota-usage read) and no change is made. The read-only self-service surface
  scopes its answers to the caller instead of refusing, and the tenant-scoped tree
  facade relies on the facades it wraps (see
  [`ILatticeTenantScopedTreeAdmin`](#ilatticetenantscopedtreeadmin)). The bindings
  additionally gate every lifecycle, region-residency, access, grant, and quota-usage
  call they serve behind an explicit opt-in (the gRPC binding's default-deny
  authorizer, the MCP binding's control-tool switch), so a cluster that does not
  enable it exposes none of them; the read-only self-service calls are deliberately
  left outside that opt-in.
- **Two-tier governance.** Tenant lifecycle and allowed-region authorization are
  **platform-operator** actions (cluster-wide `Admin` on the reserved auth policy
  tree, which the gate's control-plane isolation grants only to a platform operator).
  Setting residency, reading status, tenant access administration, cross-tenant
  grant administration, and the quota-usage read are **tenant-admin** actions, authorized when the caller is that operator or
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
  record's admin-subject set (and, with
  [delegated tenant access administration](#delegated-tenant-access-administration)
  on, its member set and group entries too), so a tenant created with none is mutable but
  invisible - even to the operator who just created it. `CreateTenantAsync`
  therefore takes an optional `adminSubjects` set and seeds it onto the new
  record. Omit it (or pass an empty set) and the **calling subject** is seeded, so the
  creator can always see what it created; supply a non-empty one and it is used
  **verbatim** (the caller is not added on top), which is how you hand a tenant to
  its delegated admins in a single call. Every entry must be a non-blank subject id, a blank or `null`
  entry fails closed with an `ArgumentException`, and duplicates collapse. A
  caller that cannot be resolved to a subject (an anonymous or system-origin
  create) seeds nothing rather than inventing an owner; grant access explicitly
  in that case. When a real identity directory provider is registered (anything
  but the default `NullIdentityDirectory`) and
  `LatticeIdentityDirectoryOptions.ValidationRequired` is set, every id in an
  explicitly supplied set must resolve - checked after authorization and before
  the write, exactly as `ILatticeTenantAccessAdmin.AddAdminSubjectAsync` checks an
  added subject - and an id the directory resolves to nothing fails the whole
  create with a `LatticeDirectoryValidationException` (an `ArgumentException`), so
  create cannot be used to grant what add would refuse. The caller-seeded default
  is not checked, since it is the authenticated caller's own resolved subject. The
  seeded set is echoed back on
  `TenantCreationResult.AdminSubjects`.
- **Authorize, then validate, then write.** Every `ILatticeTenantAdmin` lifecycle
  verb first parses the tenant id (a purely syntactic step over the caller's own
  argument, which on create also rejects an id shadowing the `sys-` or `_lattice_`
  reserved namespaces with an `ArgumentException`), then authorizes through the
  fail-closed gate, and only then inspects its remaining arguments or touches the
  registry. The region, access, and grant verbs also check their other arguments'
  syntax (a region set, a subject id, a grant scope and operation set) before the
  gate; their gate reads the tenant record to evaluate admin-subject membership, but
  reports a missing tenant to a non-operator as a denial, and no reserved-tenant
  check runs until it admits the call. So a denied caller learns nothing from the
  admin-subject list it supplied, from whether the tenant already exists, or from
  whether it is the reserved `default` tenant: every one of those checks sits behind
  the gate and cannot be used as an oracle.
- **Cascading delete.** Deleting a tenant first suspends it, so no new
  tenant-scoped admission can race the delete, then cascades the delete to every tree
  the tenant owns (each `t/{tenantId}/*` tree is soft-deleted), then purges the
  tenant's access data - its tenant-tier rules, its tenant groups and their
  membership edges in both directions - before the registry record, and the member
  set with it, is removed. The purge runs whether or not delegated tenant access
  administration is enabled. An interrupted delete therefore leaves a suspended,
  retriable record rather than orphaned trees or access data.
- **Quota authoring.** `SetTenantQuotasAsync` replaces a tenant's resource quotas and
  burst allowance in one platform-operator action. Each ceiling (`MaxBytes`,
  `MaxKeys`, `MaxMemoryBytes`, `MaxTreeCount`, `MaxOpsPerSecond`) is `null` for
  unbounded on that dimension; passing `TenantQuotasDescriptor.Unbounded` lifts every
  resource cap again. The four delegated access caps (`MaxGroups`,
  `MaxMembershipEdges`, `MaxMemberSubjects`, `MaxTenantRules`) are different: `null`
  means their defaults (500, 10000, 5000 and 1000), never unbounded, and
  `Unbounded` does not lift them. A bounded ceiling must be non-negative, and so must `BurstPercent`, the
  transient headroom above the bounded ceilings: a negative value on any of them fails
  closed with an `ArgumentException` and writes nothing. The
  reserved `default` tenant can never be given quotas. The quotas now in effect come
  back on `TenantQuotasUpdateResult.Quotas`, and stay readable on
  `ILatticeTenantSelfService.GetTenantAsync` (`TenantStatusReport.Quotas`) for any
  caller that can see the tenant.
- **Write contention.** Every mutating verb commits through the tenancy registry's
  optimistic read-merge-write, so under sustained contention on one tenant's record
  it can fail with the tenancy package's `TenantRegistryConcurrencyException` (see
  [Store write contention](../lattice.tenancy/README.md#store-write-contention))
  instead of its domain outcome; the write that exhausted its retries is not applied,
  and the call can be retried.

## Registration

Register the facade on the silo (it requires the `Orleans.Lattice.Tenancy` package):

- `AddLatticeTenantAdminApi(this ISiloBuilder builder, Action<LatticeApiTenantAdminOptions>? configure = null)` -
  registers `ILatticeTenantAdmin`, `ILatticeTenantRegionAdmin`,
  `ILatticeTenantAccessAdmin`, `ILatticeTenantGrantAdmin`, the read-only
  `ILatticeTenantSelfService` and `ILatticeTenantQuotaUsage`, and the delegated
  tenant access facades `ILatticeTenantDirectoryAdmin` and
  `ILatticeTenantPolicyAdmin`, together with the
  fail-closed authorizers they consult, and a residency listener that completes the
  drain of the silo's own region: a region dropped by `SetResidencyAsync` moves
  `Draining` -> `Offline` -> `Removed` on its own. A region it adds stays at
  `Provisioning` until an operator advances it with
  `TenantRecord.TryPromoteRegionStatus`, because nothing backfills the tenant's
  existing data into it (see
  [Lifecycle states](../lattice.tenancy/README.md#lifecycle-states)).
- `AddLatticeTenantScopedTreeAdminApi(this ISiloBuilder builder)` - registers
  `ILatticeTenantScopedTreeAdmin`.

Each call is **order-guarded at registration time**: a misordered call throws an
`InvalidOperationException` with an actionable message rather than failing
obscurely at silo start.

| Call | Must run after | Because |
|---|---|---|
| `AddLatticeTenantAdminApi` | `AddLatticeTenancy()` | The facade operates on the tenancy engine's tenant registry, so it would otherwise have no lifecycle store to act on. |
| `AddLatticeTenantScopedTreeAdminApi` | `AddLatticeTreeAdminApi()` | It delegates the whole-tree lifecycle verbs to that facade. |
| `AddLatticeTenantScopedTreeAdminApi` | `AddLatticeSchemaEnforcement()` | It delegates the per-tree schema-policy verbs to the schema engine's in-process `ILatticeSchemaAdmin`, which that call registers. |

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
with `TenantLastRegionException`. Only the caller whose commit observes the emptied
set is refused; the other caller's removal, committed while a region was still
resident, succeeds and stands. Either way the tenant keeps at least one resident
region, and retrying the refused call afterwards meets the ordinary pre-write guard.

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
| `TenantNotFoundException` | The tenant is not registered - reported to a platform operator. A non-operator caller naming an unknown tenant on a tenant-admin-tier verb gets `LatticeAuthorizationDeniedException` instead, so it cannot probe for a tenant's existence. |
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
gets an empty list. It asks the tenant policy engine with the caller's resolved
groups, and so does the accessibility check in `GetTenantAsync`: while delegated
tenant access administration is on, a tenant the caller may act as through a member
entry or a group entry - for example a member only through one of the tenant's
groups - is listed and readable; while it is off, only the exact-id admin set counts.
A group never admits to a tenant that is not group-aware: the reserved `default`
tenant, or a tenant compiled while the feature was off, is reached only through an
exact-id admin entry, even when one of the caller's group ids is in its admin set. `GetTenantAsync` deliberately unifies "no such tenant" and "you may
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

The facade composes the target id from the ambient active-tenant assertion as
supplied and does not use the two-tier gate above. Its tree verbs delegate to
`ILatticeTreeAdmin`, which resolves and authorizes the composed id through the
access gate as it would for any caller - whole-tree `Admin` to create (also checked
here before any quota accounting), `TreeLifecycle` to delete, recover, or purge, and
`Read` to check existence or deletion status - so the caller's membership
validation and tenancy's isolation apply there. Its schema-policy verbs delegate to
the in-process `ILatticeSchemaAdmin`, which performs no authorization of its own (see
[Capability gate](../lattice.schema/README.md#capability-gate)), and the facade adds
no check for them.

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

Add and remove are idempotent (`Changed` reports whether the set moved). When a real
identity directory provider is registered (anything but the default
`NullIdentityDirectory`) and `LatticeIdentityDirectoryOptions.ValidationRequired` is
set, an add of a subject that is not already a member - checked after authorization
and the reserved-tenant check - requires the id to resolve: an id the directory
resolves to nothing is refused with a `LatticeDirectoryValidationException` (an
`ArgumentException`) before the write. Resolution is the only directory check. Unlike
the `UpsertGroupAsync` and `AddMemberAsync` paths of the
[authorization-admin facade](../lattice.api.auth/README.md), the principal's kind is
not checked, so an id that resolves to a group is accepted. While delegated tenant
access administration is off, admin-subject membership is matched against the
caller's own subject id with no group expansion, so such an entry never authorizes
the group's members; while it is on, a group entry admits the group's members (see
[Group-aware tenant-admin check](#group-aware-tenant-admin-check)). Whatever the flag,
an id in the reserved `t/` namespace is admitted only when it is one of the tenant's
own groups and that group exists: another tenant's group, or a malformed `t/` id, is
refused with `TenantAccessConfinementException` (`ForeignTenantGroup`), and an
own-tenant group that does not exist with an `ArgumentException`. Such an entry is
checked in the membership directory instead of the identity directory. A group entry
counts as one entry. Removing a tenant's last admin subject is refused with
`TenantLastAdminSubjectException`, including when two concurrent removals of
different subjects would together empty the set: the guard is re-applied to the
merged record inside the registry's compare-and-set loop, before the write, so the
second racer to commit re-reads the first's removal and is refused with nothing
written. (A host that replaced the built-in tenant registry gets a best-effort
fallback: the guard is checked against a fresh read, and a call that still finds the
set empty after its write re-grants its own entry and is refused.) A removal is
stamped later than the entry it removes as well as the local clock, so it takes
effect even when another silo with a clock running ahead wrote the entry. The
reserved `default` tenant's membership can never be changed.

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
`scope` names the granting tenant's data the grant covers. It is stored as supplied
and matched against the full id of the tree a request names, so write it in the
granting tenant's composed form - a tree id such as `t/acme/orders`, or a prefix of
one that ends on a `/` segment boundary, such as `t/acme/` - because an unqualified
name such as `orders` covers no tree. `operations` must not be
`TenantGrantAccess.None`, and the two tenants must differ: a blank scope, an empty
operation set, or the same tenant on both sides fails with an `ArgumentException`.
An offer never
requires the grantee to exist and may not name the reserved `default` tenant on
either side. A grant that was never offered - or whose granting tenant is not
registered - is reported identically as `TenantGrantNotFoundException`; asking for
the state a grant is already in is an idempotent no-op, and a transition the
lifecycle forbids (for example approving a rejected or revoked grant) raises
`TenantGrantTransitionException` before any write. A step that races the other
party's concurrent transition and loses the merge is refused with the same exception,
carrying the state that won.

The access gate honours an active grant's `Write` operation, but the data plane
refuses a user-origin write that names another tenant's `t/` tree before the gate is
consulted, so through the data surface only a grant's read operations take effect.

### `ILatticeTenantQuotaUsage`

The read-only usage-against-quota surface.

| Method | Signature |
|---|---|
| `GetQuotaUsageAsync` | `Task<TenantQuotaUsageReport> GetQuotaUsageAsync(string tenantId, CancellationToken cancellationToken = default)` |

The report carries, per dimension (`Bytes`, `Keys`, `MemoryBytes`, `TreeCount`,
`OpsPerSecond`), the consumption, the steady-state ceiling, the burst-adjusted
ceiling, the live overage, and the accrued metered overage, together with the
`EnforcementScope` the figures were read under. `OpsPerSecond` carries only its two
ceilings: the engine never samples a sustained operation rate, so its usage is always
`null` (not measured). A platform operator may read any
tenant and a live tenant admin only its own; an unauthorized tenant and an absent one
are unified into a single `TenantNotFoundException`, so the call cannot probe for
tenant existence. Authoring quotas remains the operator-only `SetTenantQuotasAsync`.

## Delegated tenant access administration

Two further facades, published in `Orleans.Lattice.Api.Abstractions` (namespace
`Orleans.Lattice.Api.TenantAdmin`) and implemented here, let a tenant's own
administrators decide who belongs to the tenant, which groups exist inside it, and
who may do what on its trees, without a platform operator in the loop:

- **`ILatticeTenantDirectoryAdmin`** - the tenant's groups, their direct members, and
  the tenant member set.
- **`ILatticeTenantPolicyAdmin`** - tenant-tier rules on the tenant's own trees, a
  layer-aware explanation and effective-permissions view, and the tenant's access
  posture.

What tenant groups, members and tenant-tier rules are, and how the feature is
switched on, is described under
[Delegated tenant access administration](../lattice.tenancy/README.md#delegated-tenant-access-administration)
in the tenancy guide; how tenant-tier rules are evaluated beneath operator rules is in
[The tenant rule layer](../lattice.auth/tenant-layer.md).

### Rules both facades share

- **Order of checks.** Every operation checks its arguments' syntax, then authorizes
  the caller (a platform operator, or an admin of the named tenant directly or through
  a group), then refuses with `TenantAccessAdministrationDisabledException` while
  `LatticeTenancyOptions.DelegatedAccessAdministrationEnabled` is off, then refuses the
  reserved `default` tenant with `ReservedTenantOperationException`. A caller that is
  not authorized is denied whether or not the tenant exists, and learns nothing about
  the cluster's posture. `ILatticeTenantPolicyAdmin.GetPostureAsync` is the one
  operation that skips the feature check, so it answers while the feature is off.
- **One enforcement point.** Once authorized, each facade runs its membership,
  registry and policy-store work under system origin, so its own authorization is the
  single check. The policy store and the membership directory still apply their own
  confinement guards to that work.
- **Tenant-local names.** Groups, trees and rule ids are named tenant-locally. The
  facades compose `t/{tenant}/{name}` for a group, `t/{tenant}/{tree}` for a tree and
  `tenant:{tenant}:{id}` for a rule from the tenant the call names, so a caller can
  never name another tenant's group, tree or rule: it reads as not found.
  `TenantSubjectKind` (`User`, `TenantGroup`, `ClusterGroup`) says how to read a
  subject id. A `ClusterGroup` id that starts with `t/` is refused with
  `TenantAccessConfinementException` (`ForeignTenantGroup`), and so is a `User` id
  that starts with `t/` when it names a group member or a member-set entry. A `User`
  rule subject is taken as given.
- **Caps: verify and compensate.** A new group, a membership edge, a member-set entry
  and a new tenant-tier rule are each checked against the tenant's cap
  (`MaxGroups`, `MaxMembershipEdges`, `MaxMemberSubjects`, `MaxTenantRules` on
  `TenantQuotasDescriptor`) before the write, and counted again after it. A call that
  finds the tenant over its cap withdraws exactly what it added and is refused with
  `LatticeQuotaExceededException`. Concurrent callers may all be refused, which is the
  fail-closed direction; between a write and its withdrawal a reader can briefly see
  the tenant over its cap. The withdrawal is idempotent and is tried up to four times,
  uncancellably. If every attempt fails, the addition stays and the call is still
  refused with `LatticeQuotaExceededException`, whose message says the cap may stay
  exceeded until the addition is removed and whose `Current` is the over-cap count; a
  warning naming only the tenant and dimension is logged.
- **Removals win the merge.** A removal from the member set or the admin set is
  stamped later than the entry it removes as well as the local clock, so `Changed`
  `true` means the entry is gone even when another silo with a clock running ahead
  wrote it.
- **One id namespace.** Admin-set and member-set entries are plain ids, and user ids
  and cluster group ids share one namespace there, as they do in the membership
  directory: an entry matches a subject whose own id, or any of whose groups, equals
  it. Keeping user and group ids distinct is the identity provider's and the
  directory's job. Tenant groups cannot collide, because the facades never store a
  user or cluster group entry that starts with `t/`.
- **Idempotent mutations, ordinal listings.** Repeating an add or remove reports
  `Changed` (or `Removed`) `false`. Paged listings take a `TenantAccessPageRequest`
  (`PageSize` defaults to 100 and is clamped to 1000) and return `NextPageToken`.

| Failure | Raised when |
|---|---|
| `LatticeAuthorizationDeniedException` | The caller is neither a platform operator nor an admin of the tenant. |
| `TenantAccessAdministrationDisabledException` | The feature is off (every operation except `GetPostureAsync`). Derives from `Exception`; carries `TenantId`. |
| `ReservedTenantOperationException` | The tenant is the reserved `default` tenant, which has no tenant groups, members or tenant-tier rules. |
| `TenantAccessConfinementException` | The request breaks a confinement rule. An `ArgumentException` carrying `TenantId` and `Rule`: `GroupNesting`, `ForeignTenantGroup`, `RuleTree`, `RuleOperations` or `ReservedRuleId`. |
| `TenantLastAdminSubjectException` | Removing a group would remove the tenant's last admin-set entry. |
| `LatticeQuotaExceededException` | An addition would exceed one of the four access caps. Its `Dimension` is `tenant-groups`, `tenant-membership-edges`, `tenant-member-subjects` or `tenant-rules`. |
| `ArgumentException` | A malformed tenant id, local name or rule, or a named tenant group that does not exist. |

### `ILatticeTenantDirectoryAdmin`

| Method | Signature |
|---|---|
| `ListGroupsAsync` | `Task<TenantGroupPage> ListGroupsAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)` |
| `GetGroupAsync` | `Task<TenantGroupDescriptor?> GetGroupAsync(string tenantId, string groupName, CancellationToken cancellationToken = default)` |
| `UpsertGroupAsync` | `Task<TenantGroupDescriptor> UpsertGroupAsync(string tenantId, TenantGroupDescriptor group, CancellationToken cancellationToken = default)` |
| `RemoveGroupAsync` | `Task<TenantGroupRemovalResult> RemoveGroupAsync(string tenantId, string groupName, CancellationToken cancellationToken = default)` |
| `ListGroupMembersAsync` | `Task<IReadOnlyList<TenantGroupMember>> ListGroupMembersAsync(string tenantId, string groupName, CancellationToken cancellationToken = default)` |
| `AddGroupMemberAsync` | `Task<TenantMembershipChangeResult> AddGroupMemberAsync(string tenantId, string groupName, string memberId, TenantSubjectKind memberKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)` |
| `RemoveGroupMemberAsync` | `Task<TenantMembershipChangeResult> RemoveGroupMemberAsync(string tenantId, string groupName, string memberId, TenantSubjectKind memberKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)` |
| `ListMembersAsync` | `Task<TenantMemberPage> ListMembersAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)` |
| `AddMemberAsync` | `Task<TenantMembershipChangeResult> AddMemberAsync(string tenantId, string subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)` |
| `RemoveMemberAsync` | `Task<TenantMembershipChangeResult> RemoveMemberAsync(string tenantId, string subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)` |
| `ResolveSubjectAsync` | `Task<TenantSubjectResolution> ResolveSubjectAsync(string tenantId, string subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)` |

- **Groups.** A group name is 1 to 63 characters of lower-case ASCII letters, digits,
  `-`, `_` and `.`. Only creating a group counts against `MaxGroups`; an upsert of an
  existing group changes its display name. Listings are in ascending local-name
  order.
- **Members of a group.** A group may contain users, cluster groups and the tenant's
  own groups. A tenant group named as a member must exist. An edge that would nest a
  tenant group in a cluster group or in another tenant's group is refused for every
  caller (`GroupNesting`); see the
  [nesting invariant](../lattice.membership/README.md#tenant-groups). When an identity
  directory is registered and `LatticeIdentityDirectoryOptions.ValidationRequired` is
  set, a user or cluster group id must resolve to that kind, as on the cluster
  facade.
- **The member set.** Its entries are users, cluster groups and the tenant's own
  groups. Admins are implicitly members and are not repeated in the listing.
  Membership only lets a subject act as the tenant; what it may then do is decided by
  rules, default-deny.
- **Entry kinds on a listing.** The stored entries are plain ids, so a listing
  recovers each kind: a `t/` id is a tenant group (reported by its local name), an id
  with a group record in the membership directory, or that the identity directory
  resolves as a group, is a cluster group, and anything else is a user.
- **Removing a group** cascades, in this order: a guard that refuses with
  `TenantLastAdminSubjectException` when the group is the tenant's last admin-set
  entry, before anything is written; the group's member-set and admin-set entries,
  committed with the last-admin guard re-applied to the merged record, so a racing
  admin removal is refused with nothing written;
  the tenant-tier rules whose subject is the group; and finally its membership edges
  in both directions and its record. A removal interrupted part-way can be repeated.
  `TenantGroupRemovalResult` reports `Removed`, `EdgesRemoved`,
  `RemovedFromMemberSet`, `RemovedFromAdminSet` and the local `RemovedRuleIds`; a
  group that does not exist reports `Removed` `false` and cascades nothing.
- **`ResolveSubjectAsync`** expands the subject's transitive groups from the
  membership directory and reports `IsAdmin`, `IsMember` and the matching
  `AdminEntries` and `MemberEntries`. The subject may be any principal, so a tenant
  admin who added a cluster group to the tenant's member or admin set can learn
  whether a given user is a transitive member of that cluster group. That is by
  design: admitting the group lets its members act as the tenant, so who they are is
  the tenant admin's to know. Nothing is reported about a cluster group the tenant has
  not admitted, because only entries of the tenant's own sets are matched.

### `ILatticeTenantPolicyAdmin`

| Method | Signature |
|---|---|
| `PutRuleAsync` | `Task<TenantRuleView> PutRuleAsync(string tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default)` |
| `GetRuleAsync` | `Task<TenantRuleView?> GetRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default)` |
| `RemoveRuleAsync` | `Task<bool> RemoveRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default)` |
| `ListRulesAsync` | `Task<TenantRulePage> ListRulesAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)` |
| `ExplainAsync` | `Task<TenantExplanation> ExplainAsync(string tenantId, string subjectId, string treeName, string? key, LatticeOperation operation, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)` |
| `EffectivePermissionsAsync` | `Task<TenantEffectivePermissions> EffectivePermissionsAsync(string tenantId, string subjectId, string? treeName = null, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)` |
| `GetPostureAsync` | `Task<TenantAccessPosture> GetPostureAsync(string tenantId, CancellationToken cancellationToken = default)` |

- **Writing a rule.** A `TenantRuleDraft` carries a tenant-local `RuleId`, a subject
  (`SubjectId` and `SubjectKind`), a `ScopeKind` (`Tree`, `Prefix`, `Key` or
  `TenantWide`), a `TreeName` (omitted for `TenantWide`), a `KeyOrPrefix`,
  `Operations` and `Effect`. The facade refuses, with
  `TenantAccessConfinementException`: a rule id that already carries the reserved
  `tenant:` or `app:` prefix (`ReservedRuleId`); operations that are empty or outside
  `LatticeAuthOperations.All`, so never `Telemetry`, `Replication`, `TreeLifecycle`
  or `AppInstall` (`RuleOperations`); and a tree name that is one of the tenant's
  app-owned trees (`a/...`), a reserved or system tree, or the tenant-wide sentinel
  (`RuleTree`). A rule's subject may be a user, a cluster group or one of the tenant's
  own groups.
- **One copy per local id.** The policy store keys a rule by tree and id, so the
  facade keeps a local id unique across the tenant's trees: a put that moves a rule to
  another tree removes the copy it replaced. Only a put of a new local id counts
  against `MaxTenantRules`. The returned `TenantRuleView` is in the `Tenant` layer and
  editable. `RemoveRuleAsync` never removes an operator rule.
- **What a tenant administrator sees.** `ListRulesAsync` lists the tenant's
  tenant-tier rules (`Layer` `Tenant`, `Origin` `Tenant`, `Editable`) and the
  operator rules scoped to the tenant's own trees (`Layer` `Platform`, `Origin`
  `PlatformTree`, read-only). Cluster-wide `Tree:*` rules (`PlatformWide`) and app
  role rules (`AppRole`) are never listed; when one decides an explanation or applies
  to a subject, it is reported by `RuleId`, `Origin` and `Effect` only, and
  `SubjectWithheld` reads `true`.
- **Explain and effective permissions.** `ExplainAsync` evaluates the request with the
  subject's groups resolved from the membership directory (a group subject is
  evaluated as a member of that group) and reports `Allowed`, `Filtered`, `Reason`,
  the `DecidingLayer` and `DecidingRule` (both `null` when the default effect
  decided), `DefaultEffect`, and the `MatchedRules` the tenant may see in full,
  platform layer first. `EffectivePermissionsAsync` lists the rules of both layers
  that name the subject directly or through one of its groups, optionally on one
  tree. Both accept one of the tenant's app-owned trees as the tree, read-only.
- **Posture.** `TenantAccessPosture` reports `Enabled` (the cluster flag),
  `CallerIsTenantAdmin`, `CallerIsPlatformOperator`, and `Groups`,
  `MembershipEdges`, `MemberSubjects` and `TenantRules`, each a
  `TenantQuotaDimensionUsage` whose `Usage` is the tenant's count and whose `Limit` is
  the cap in force. It still refuses the `default` tenant and an unauthorized caller.

### Group-aware tenant-admin check

`AddLatticeTenantAdminApi` registers both facades, and replaces its own built-in
`TenantRegionResidencyAuthorizer` registration with one that reads the delegated-access
flag live on every check. While the flag is on, a caller is a tenant admin when its
subject id **or any of its resolved transitive groups** is a live admin-set entry;
while it is off, only its exact subject id counts, as before. Because every
tenant-tier verb authorizes through that authorizer, the group-aware rule applies to
residency, admin-subject, cross-tenant grant and quota-usage calls as well as to the
two new facades. An admin-set entry naming another tenant's group never counts. A
`TenantRegionResidencyAuthorizer` a host registers itself, and one built through its
public constructor, keeps the exact-id check.

### Transport bindings

The [gRPC binding](../lattice.api.tenantadmin.grpc/README.md#delegated-tenant-access-rpcs)
serves both facades as eighteen RPCs, and `LatticeTenantAdminApiGrpcClient` implements
both interfaces directly. The MCP server exposes them as the
[delegated tenant access tools](../lattice.api.mcp/tools.md#delegated-tenant-access-tools),
behind `EnableTenantAdminControlTools`.

## Authorization seams

The two fail-closed authorizers the facades consult are public types of this package
(namespace `Orleans.Lattice.Api.TenantAdmin`). Both honour a system-origin bypass for
trusted co-hosted infrastructure and are independent of the data-plane
`DefaultEffect`:

- `TenantAdminAccessAuthorizer` - the platform-operator gate for the lifecycle verbs.
  `AuthorizeTenantAdminAsync` throws `LatticeAuthorizationDeniedException` unless the
  caller holds whole-scope `Admin` on `PlatformOperatorScope` (the reserved
  authorization policy tree), refusing a key-filtered allow;
  `IsTenantAdminAuthorizedAsync` is its non-throwing probe.
- `TenantRegionResidencyAuthorizer` - the two-tier gate for the tenant-tier verbs.
  `AuthorizeOperatorAsync` is the operator tier. `AuthorizeTenantAdminAsync` admits the
  platform operator or a live admin subject on the tenant record (through one of the
  caller's groups too, in the instance `AddLatticeTenantAdminApi` registers, while
  delegated tenant access administration is on - see
  [Group-aware tenant-admin check](#group-aware-tenant-admin-check)) and returns that
  record, reporting an unknown tenant as `TenantNotFoundException` to the operator and
  as a denial to anyone else. `TryAuthorizeTenantAdminAsync` returns `null` instead of
  throwing - for a missing tenant as well as a denial - so a verb either of two
  tenants may perform can consider both sides.

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
| `TenantQuotasDescriptor` | model | A tenant's per-dimension resource ceilings (`null` = unbounded) and `BurstPercent`; `Unbounded` sentinel and `IsUnbounded` predicate; and the four delegated access caps (`null` = the default, never unbounded). |
| `TenantQuotasUpdateResult` | result | The tenant id and the quotas now in effect after authoring. |
| `TenantLifecycleStatus` | enum | `Active` / `Suspended`. |
| `TenantRegionAuthorizationResult` | result | The resulting allowed region set. |
| `TenantResidencyChangeResult` | result | The regions this call began adding (now `Provisioning`) and removing (now `Draining`), and the resulting per-region status rows. |
| `TenantRegionStatusReport` | result | Per-region rows (`TenantRegionStatusDescriptor`), ordered by region id. |
| `TenantRegionStatusDescriptor` | model | One region's allowed flag and lifecycle status. |
| `TenantRegionLifecycleStatus` | enum | `None` / `Provisioning` / `Backfilling` / `Online` / `Draining` / `Offline` / `Removed`. |
| `TenantNotFoundException` | exception | No tenant with that id is registered. |
| `TenantAlreadyExistsException` | exception | A tenant with the same id is already registered. |
| `ReservedTenantOperationException` | exception | Attempted suspend, delete, set-quotas, an admin-subject add / remove, a cross-tenant grant offer, or any delegated tenant access operation on the reserved `default` tenant. |
| `TenantRegionNotAllowedException` | exception | A residency region is not in the allowed set (or a revoked region is still resident). |
| `TenantLastRegionException` | exception | The change would remove the last resident region, as submitted or once merged with a concurrent removal. |
| `TenantAdminSubjectReport` | result | A tenant's live admin-subject set, in ordinal order. |
| `TenantAdminSubjectChangeResult` | result | An add / remove outcome: the subject, `Changed`, and the resulting admin-subject set. |
| `TenantLastAdminSubjectException` | exception | The removal would leave the tenant with no admin subjects, including removing a tenant group that is the last admin-set entry. |
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
| `TenantAccessPageRequest` | model | A delegated tenant access page request: `PageSize` (default 100, clamped to 1000) and `PageToken`. |
| `TenantGroupDescriptor` | model | A tenant group: its tenant-local `Name` and optional `DisplayName`. |
| `TenantGroupPage` | result | One page of a tenant's groups and the `NextPageToken`. |
| `TenantGroupMember` | model | One direct member of a tenant group: `MemberId` and `Kind`. |
| `TenantGroupRemovalResult` | result | What removing a tenant group cascaded to. |
| `TenantMemberEntry` | model | One member-set or admin-set entry: `SubjectId` and `Kind`. |
| `TenantMemberPage` | result | One page of a tenant's member set and the `NextPageToken`. |
| `TenantMembershipChangeResult` | result | A group-member or member-set add / remove outcome; `Changed` reports whether anything moved. |
| `TenantSubjectResolution` | result | Whether a subject is an admin or a member of a tenant, and through which entries. |
| `TenantSubjectKind` | enum | `User` / `TenantGroup` / `ClusterGroup` - how to read a subject id. |
| `TenantRuleDraft` | model | A tenant-tier rule to write, with a tenant-local id. |
| `TenantRuleView` | model | A rule as a tenant administrator sees it, with its `Layer`, `Origin` and `Editable`; `SubjectWithheld` for a platform-wide or app role rule. |
| `TenantRulePage` | result | One page of the rules governing a tenant and the `NextPageToken`. |
| `TenantRuleLayer` | enum | `Platform` / `Tenant`. |
| `TenantRuleOrigin` | enum | `PlatformTree` / `PlatformWide` / `AppRole` / `Tenant`. |
| `TenantRuleScopeKind` | enum | `Tree` / `Key` / `Prefix` / `TenantWide`. |
| `TenantExplanation` | result | A layer-aware explanation of one decision on a tenant's tree. |
| `TenantEffectivePermissions` | result | The rules of both layers that apply to a subject on a tenant's trees. |
| `TenantAccessPosture` | result | Whether the feature is enabled, the caller's standing, and the four access caps with usage. |
| `TenantAccessAdministrationDisabledException` | exception | Delegated tenant access administration is off. |
| `TenantAccessConfinementException` | exception | A delegated tenant access request broke a confinement rule; an `ArgumentException` carrying `Rule`. |
| `TenantAccessConfinementRule` | enum | `GroupNesting` / `ForeignTenantGroup` / `RuleTree` / `RuleOperations` / `ReservedRuleId`. |
| `ApiTenantAdminTypeAliases` | static class | The stable `oitn.`-prefixed Orleans serialization aliases of the tenant-admin contract types. |

## See also

- [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md) - the core multi-tenancy
  companion (isolation, quotas, metering, residency enforcement).
- [`Orleans.Lattice.Api.TenantAdmin.Grpc`](../lattice.api.tenantadmin.grpc/README.md) -
  the code-first gRPC binding and remote client for these facades.
- [`Orleans.Lattice.Api.TreeAdmin`](../lattice.api.treeadmin/README.md) - the sibling
  tree-administration facade this one composes and mirrors.
- [MultiTenancy sample](../../samples/MultiTenancy/README.md).
