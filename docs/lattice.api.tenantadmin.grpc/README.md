# Orleans.Lattice.Api.TenantAdmin.Grpc

The code-first gRPC **binding** and public **clients** for the
[`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) tenant
lifecycle and region-residency control facades and their read-only tenant
self-service companion. It exposes `ILatticeTenantAdmin`,
`ILatticeTenantRegionAdmin`, `ILatticeTenantAccessAdmin`,
`ILatticeTenantGrantAdmin`, `ILatticeTenantQuotaUsage`,
`ILatticeTenantSelfService`, and the delegated tenant access facades
`ILatticeTenantDirectoryAdmin` and `ILatticeTenantPolicyAdmin` over a network
transport as thin adapters - the control and scoping semantics live in the facades,
this package only marshals them.

## What is it?

A code-first gRPC binding - `Grpc.AspNetCore` method definitions whose messages are
marshalled with the Orleans binary serializer, with no hand-written `.proto` - that
hosts the tenant-administration facades as a gRPC service and ships strongly-typed clients for calling it remotely. It
mirrors the [TreeAdmin gRPC binding](../lattice.api.treeadmin.grpc/README.md)
packaging exactly: server-side registration + endpoint mapping extensions, a
`LatticeTenantAdminApiGrpcClient`, a read-only `LatticeTenantSelfServiceApiGrpcClient`,
a fail-closed auth interceptor, and an
auth-scheme advertisement RPC so a client can discover how to authenticate.

## Core properties

- **Thin adapter.** Each RPC forwards one-to-one to an `ILatticeTenantAdmin`,
  `ILatticeTenantRegionAdmin`, `ILatticeTenantAccessAdmin`,
  `ILatticeTenantGrantAdmin`, `ILatticeTenantQuotaUsage`,
  `ILatticeTenantSelfService`, `ILatticeTenantDirectoryAdmin`, or
  `ILatticeTenantPolicyAdmin` method; no control logic lives here.
- **Default-deny out of the box.** With `RequireAuthorization` left at its `true`
  default, the server interceptor consults the registered
  `ILatticeTenantAdminApiAuthorizer` on every admin RPC - and the registered default
  is `DenyTenantAdminApiAuthorizer`, which refuses **every** call. Presenting a
  credential is therefore *not* sufficient: a host must deliberately opt in by
  registering a permissive authorizer (`AllowAllTenantAdminApiAuthorizer` or its own
  implementation) before this surface answers at all, or set
  `RequireAuthorization = false` when an outer boundary already guards the endpoint.
  A refusal is returned as `PermissionDenied` before the call reaches the facade,
  which then applies its own two-tier gate on top.
- **Credential isolation per call.** The caller credential is read from a configurable
  header and bridged into the facade's authorization context for that call only.
- **Discoverable auth.** A `GetAuthScheme` RPC advertises the accepted credential
  schemes so a client can self-configure. It is exempt from the authorizer so a client
  can learn how to sign in before it holds any credential.
- **Self-service reads exempt from default-deny, still scoped.** The read-only
  self-service RPCs are exempt from the tenant-admin authorizer entirely, so they stay
  reachable by any read-capable caller - including an anonymous one. They are still
  credential-stamped and active-tenant-stamped, so the facade enforces fail-closed
  per-tenant scoping at the single narrowest seam: an anonymous caller lists nothing,
  and a caller only ever sees its own authorized tenants.

## Service and RPCs

The gRPC service name is `orleans.lattice.api.tenantadmin`, so each method's full
path is `/orleans.lattice.api.tenantadmin/<Rpc>`.

The service surfaces the `ILatticeTenantAdmin` lifecycle operations, the
`ILatticeTenantRegionAdmin` region-residency operations, the
`ILatticeTenantQuotaUsage` usage read, the `ILatticeTenantAccessAdmin`
tenant-admin subject operations, the `ILatticeTenantGrantAdmin` cross-tenant
grant operations, the read-only `ILatticeTenantSelfService` operations, the
[delegated tenant access RPCs](#delegated-tenant-access-rpcs), and the
auth-scheme advertisement. Each row below is one bound RPC:

| RPC | Facade method |
|---|---|
| `CreateTenant` | `CreateTenantAsync` (carries the optional admin-subject set) |
| `SuspendTenant` | `SuspendTenantAsync` |
| `ResumeTenant` | `ResumeTenantAsync` |
| `DeleteTenant` | `DeleteTenantAsync` |
| `SetTenantQuotas` | `SetTenantQuotasAsync` |
| `GetTenantQuotaUsage` | `ILatticeTenantQuotaUsage.GetQuotaUsageAsync` |
| `AuthorizeAllowedRegions` | `ILatticeTenantRegionAdmin.AuthorizeAllowedRegionsAsync` (operator-only) |
| `SetTenantResidency` | `ILatticeTenantRegionAdmin.SetResidencyAsync` (operator or tenant admin) |
| `GetTenantRegionStatus` | `ILatticeTenantRegionAdmin.GetTenantRegionStatusAsync` (operator or tenant admin) |
| `ListTenantAdminSubjects` | `ILatticeTenantAccessAdmin.ListAdminSubjectsAsync` |
| `AddTenantAdminSubject` | `ILatticeTenantAccessAdmin.AddAdminSubjectAsync` |
| `RemoveTenantAdminSubject` | `ILatticeTenantAccessAdmin.RemoveAdminSubjectAsync` |
| `ListCrossTenantGrants` | `ILatticeTenantGrantAdmin.ListGrantsAsync` |
| `OfferCrossTenantGrant` | `ILatticeTenantGrantAdmin.OfferGrantAsync` |
| `ApproveCrossTenantGrant` | `ILatticeTenantGrantAdmin.ApproveGrantAsync` |
| `RejectCrossTenantGrant` | `ILatticeTenantGrantAdmin.RejectGrantAsync` |
| `RevokeCrossTenantGrant` | `ILatticeTenantGrantAdmin.RevokeGrantAsync` |
| `GetCurrentTenant` | `ILatticeTenantSelfService.GetCurrentTenantAsync` (self-service; exempt from default-deny) |
| `ListAccessibleTenants` | `ILatticeTenantSelfService.ListAccessibleTenantsAsync` (self-service; exempt from default-deny) |
| `GetTenant` | `ILatticeTenantSelfService.GetTenantAsync` (self-service; exempt from default-deny) |
| `ListTenantGroups` | `ILatticeTenantDirectoryAdmin.ListGroupsAsync` |
| `GetTenantGroup` | `ILatticeTenantDirectoryAdmin.GetGroupAsync` |
| `UpsertTenantGroup` | `ILatticeTenantDirectoryAdmin.UpsertGroupAsync` |
| `RemoveTenantGroup` | `ILatticeTenantDirectoryAdmin.RemoveGroupAsync` |
| `ListTenantGroupMembers` | `ILatticeTenantDirectoryAdmin.ListGroupMembersAsync` |
| `AddTenantGroupMember` | `ILatticeTenantDirectoryAdmin.AddGroupMemberAsync` |
| `RemoveTenantGroupMember` | `ILatticeTenantDirectoryAdmin.RemoveGroupMemberAsync` |
| `ListTenantMembers` | `ILatticeTenantDirectoryAdmin.ListMembersAsync` |
| `AddTenantMember` | `ILatticeTenantDirectoryAdmin.AddMemberAsync` |
| `RemoveTenantMember` | `ILatticeTenantDirectoryAdmin.RemoveMemberAsync` |
| `ResolveTenantSubject` | `ILatticeTenantDirectoryAdmin.ResolveSubjectAsync` |
| `PutTenantRule` | `ILatticeTenantPolicyAdmin.PutRuleAsync` |
| `GetTenantRule` | `ILatticeTenantPolicyAdmin.GetRuleAsync` |
| `RemoveTenantRule` | `ILatticeTenantPolicyAdmin.RemoveRuleAsync` |
| `ListTenantRules` | `ILatticeTenantPolicyAdmin.ListRulesAsync` |
| `ExplainTenantAccess` | `ILatticeTenantPolicyAdmin.ExplainAsync` |
| `GetTenantEffectivePermissions` | `ILatticeTenantPolicyAdmin.EffectivePermissionsAsync` |
| `GetTenantAccessPosture` | `ILatticeTenantPolicyAdmin.GetPostureAsync` |
| `GetAuthScheme` | (binding-local) advertises accepted credential schemes |

### Region-residency RPCs

The region-residency RPCs bind the
[`ILatticeTenantRegionAdmin`](../lattice.api.tenantadmin/README.md#ilatticetenantregionadmin)
facade without widening it. Every one of them is **interceptor-enforced**: none is on
the self-service exemption list, so `RequireAuthorization` applies, and the facade then
re-runs its own two-tier gate. `AuthorizeAllowedRegions` stays **operator-only**, while
`SetTenantResidency` and `GetTenantRegionStatus` stay **operator-or-tenant-admin**,
exactly as in-process and independent of the data-plane `DefaultEffect`.

`AuthorizeAllowedRegions` and `SetTenantResidency` share the
`TenantAdminRegionSetRequest` DTO (`TenantId` plus the complete replacement region
set); `GetTenantRegionStatus` reuses the existing `TenantAdminTenantRequest`. The
interceptor decodes the authorization target from each, so an audit record names the
tenant the call acts on.

`ILatticeTenantRegionAdmin` is an **optional** dependency of the service, as are
`ILatticeTenantAccessAdmin`, `ILatticeTenantGrantAdmin`, and
`ILatticeTenantQuotaUsage`. `AddLatticeTenantAdminApi` registers all four, so an
ordinary silo serves every group; a host that composes the binding without one of them
still serves every lifecycle and self-service RPC and answers each RPC of the absent
facade with `Unimplemented`, rather than failing container construction at startup.

### Delegated tenant access RPCs

The eighteen delegated tenant access RPCs bind
[`ILatticeTenantDirectoryAdmin` and `ILatticeTenantPolicyAdmin`](../lattice.api.tenantadmin/README.md#delegated-tenant-access-administration)
without widening them. Every one is **interceptor-enforced**, `GetTenantAccessPosture`
included: none is on the self-service exemption list, so `RequireAuthorization`
applies, and the facade then runs its own check - a platform operator, or an admin of
the named tenant directly or through a group - followed by the feature flag (which
`GetTenantAccessPosture` skips, so it answers while the feature is off), the
reserved-tenant refusal, confinement and caps. The interceptor reads the target tenant
from each request, and `LatticeTenantAdminApiOperation` names each RPC (members 18 to
35).

Both facades are **optional** dependencies of the service. `AddLatticeTenantAdminApi`
registers both, so an ordinary silo serves every RPC; a host without one answers that
facade's RPCs with `Unimplemented`. Names, trees and rule ids travel tenant-local, as
the facades take them. The quota-setting RPC needs no new members: the four delegated
access caps (`MaxGroups`, `MaxMembershipEdges`, `MaxMemberSubjects`,
`MaxTenantRules`) are members of the `TenantQuotasDescriptor` that
`TenantAdminSetQuotasRequest` already carries, and a request from a client that
predates them reads the caps as `null`, which means their defaults.

`LatticeTenantAdminApiGrpcClient` implements both interfaces directly, so remote code
binds to `ILatticeTenantDirectoryAdmin` and `ILatticeTenantPolicyAdmin` with no
adapter. It checks only `null` and empty arguments locally; every other refusal comes
from the server's facade as an `RpcException` with the status below.

### Status mapping

Every domain failure maps to an explicit status rather than falling through to a
generic fault. The RPC groups (lifecycle, quota usage, region residency, tenant-admin subjects, cross-tenant grants, self-service) share the
same vocabulary; the last column notes where an arm applies to only some of them.

| Exception | gRPC status | Why |
|---|---|---|
| `TenantNotFoundException` | `NotFound` | The tenant is not registered - or, on the self-service surface, is not one the caller may see. |
| `TenantAlreadyExistsException` | `AlreadyExists` | `CreateTenant` was called for an id already registered. Lifecycle only. |
| `TenantRegionNotAllowedException` | `FailedPrecondition` | The requested residency is outside the operator-authored allowed set, or the revoked region is still resident. The caller must change state first, then retry. Region residency only. |
| `TenantLastRegionException` | `FailedPrecondition` | The change would remove the tenant's last resident region. Region residency only. |
| `TenantLastAdminSubjectException` | `FailedPrecondition` | The removal would leave the tenant with no admin subjects. Tenant-admin subjects and `RemoveTenantGroup` only. |
| `TenantAccessAdministrationDisabledException` | `FailedPrecondition` | Delegated tenant access administration is off on the cluster. Delegated tenant access RPCs only, except `GetTenantAccessPosture`, which answers while it is off. |
| `TenantAccessConfinementException` | `InvalidArgument` | The request would nest a tenant group outside its tenant, name another tenant's group, or write a rule over a tree, operation or id the tenant may not use. Mapped before the `ArgumentException` arm it derives from. Delegated tenant access RPCs only. |
| `LatticeQuotaExceededException` | `ResourceExhausted` | A delegated access cap is reached. The `lattice-quota-dimension` trailer carries the dimension when the exception names one, and the `lattice-quota-current` and `lattice-quota-limit` trailers carry the usage and cap when the limit is positive; the tree and tenant are not sent. Mapped before the `InvalidOperationException` arm it derives from. Delegated tenant access RPCs only. |
| `TenantGrantNotFoundException` | `NotFound` | No such cross-tenant grant has been offered - reported identically when the granting tenant is not registered. Cross-tenant grants only. |
| `TenantGrantTransitionException` | `FailedPrecondition` | The grant's lifecycle forbids the requested transition (for example approving a rejected or revoked grant), or the other party's concurrent transition won the merge. Cross-tenant grants only. |
| `ReservedTenantOperationException` | `FailedPrecondition` | The operation targets the reserved `default` tenant (suspend, delete, set-quotas, an admin-subject add / remove, a cross-tenant grant offer, or any delegated tenant access RPC). |
| `InvalidOperationException` | `FailedPrecondition` | A lifecycle or residency precondition the facade refuses on a well-formed request. Not mapped on the quota-usage and self-service RPCs, where it falls through to `Internal`. |
| `LatticeAuthorizationDeniedException` | `PermissionDenied` | The caller does not hold the required tier. |
| `LatticeTenantAccessDeniedException` | `PermissionDenied` | Fail-closed tenant resolution refused the caller's asserted active tenant. Deliberately not `Internal`, which a client would retry. |
| `ArgumentException` | `InvalidArgument` | A malformed tenant, region, or subject id, or a grant with a blank scope, an empty operation set, or the same tenant on both sides; also a negative `BurstPercent` or quota ceiling, and an added admin subject the identity directory cannot resolve (`LatticeDirectoryValidationException` derives from `ArgumentException`). |
| `OperationCanceledException` | `Cancelled` | The caller's deadline or cancellation token fired. |
| (optional facade not registered) | `Unimplemented` | The region-residency, tenant-admin subject, cross-tenant grant, quota-usage, tenant directory, or tenant policy facade is absent from the host, so its RPCs are not served. |
| anything else | `Internal` | The catch-all, logged server-side and returned without echoing the exception text. It includes the tenancy package's `TenantRegistryConcurrencyException` (sustained write contention on one tenant's registry record), which a client may retry. |

Each arm is explicit and separately tested. `TenantRegionNotAllowedException` and
`TenantLastRegionException` in particular must never reach the catch-all arm: that is
the failure mode fixed in #1697, where a domain exception surfaced to callers as an
opaque `Internal`.

### Client method signatures

`LatticeTenantAdminApiGrpcClient` (construct with
`LatticeTenantAdminApiGrpcClient.Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`):

| Method | Signature |
|---|---|
| `CreateTenantAsync` | `Task<TenantCreationResult> CreateTenantAsync(string tenantId, IReadOnlyCollection<string>? adminSubjects = null, CancellationToken cancellationToken = default)` |
| `SuspendTenantAsync` | `Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `ResumeTenantAsync` | `Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `DeleteTenantAsync` | `Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `SetTenantQuotasAsync` | `Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)` |
| `AuthorizeAllowedRegionsAsync` | `Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)` |
| `SetTenantResidencyAsync` | `Task<TenantResidencyChangeResult> SetTenantResidencyAsync(string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)` |
| `GetTenantRegionStatusAsync` | `Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `GetAuthSchemeAsync` | `Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)` |
| `GetTenantQuotaUsageAsync` | `Task<TenantQuotaUsageReport> GetTenantQuotaUsageAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `ListTenantAdminSubjectsAsync` | `Task<TenantAdminSubjectReport> ListTenantAdminSubjectsAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `AddTenantAdminSubjectAsync` | `Task<TenantAdminSubjectChangeResult> AddTenantAdminSubjectAsync(string tenantId, string subjectId, CancellationToken cancellationToken = default)` |
| `RemoveTenantAdminSubjectAsync` | `Task<TenantAdminSubjectChangeResult> RemoveTenantAdminSubjectAsync(string tenantId, string subjectId, CancellationToken cancellationToken = default)` |
| `ListCrossTenantGrantsAsync` | `Task<TenantGrantReport> ListCrossTenantGrantsAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `OfferCrossTenantGrantAsync` | `Task<TenantGrantChangeResult> OfferCrossTenantGrantAsync(string granterTenantId, string granteeTenantId, string scope, TenantGrantAccess operations, CancellationToken cancellationToken = default)` |
| `ApproveCrossTenantGrantAsync` | `Task<TenantGrantChangeResult> ApproveCrossTenantGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)` |
| `RejectCrossTenantGrantAsync` | `Task<TenantGrantChangeResult> RejectCrossTenantGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)` |
| `RevokeCrossTenantGrantAsync` | `Task<TenantGrantChangeResult> RevokeCrossTenantGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)` |

The client also implements `ILatticeTenantDirectoryAdmin` and
`ILatticeTenantPolicyAdmin`, with exactly the member signatures those interfaces
declare (see [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md#delegated-tenant-access-administration)).

`LatticeTenantSelfServiceApiGrpcClient` (read-only; construct with
`LatticeTenantSelfServiceApiGrpcClient.Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`):

| Method | Signature |
|---|---|
| `GetCurrentTenantAsync` | `Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)` |
| `ListAccessibleTenantsAsync` | `Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)` |
| `GetTenantAsync` | `Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |

### Wire message records

The request records this package defines are Orleans-serialized `[GenerateSerializer]`
records whose stable aliases carry the `oitng.` prefix (the constants live in the public
`GrpcTenantAdminTypeAliases` class). Responses are the facade result records from
`Orleans.Lattice.Api.Abstractions`, whose aliases carry the `oitn.` prefix, except
`ListAccessibleTenants`, which wraps its list in this package's `TenantSelfDescriptorList`,
`GetAuthScheme`, which answers with this package's `AuthSchemeAdvertisement`, and the four
delegated tenant access RPCs whose facade result is not a record: `GetTenantGroup`
(`TenantAdminGroupLookup`), `ListTenantGroupMembers` (`TenantAdminGroupMemberList`),
`GetTenantRule` (`TenantAdminRuleLookup`) and `RemoveTenantRule` (`TenantAdminRuleRemoval`).
Properties marked `required` must be set by the caller.

| Record | Members | Used by |
|---|---|---|
| `TenantAdminCreateRequest` | `required string TenantId`, `IReadOnlyList<string> AdminSubjects` | `CreateTenant` |
| `TenantAdminTenantRequest` | `required string TenantId` | `SuspendTenant`, `ResumeTenant`, `DeleteTenant`, `GetTenant`, `GetTenantRegionStatus`, `GetTenantQuotaUsage`, `ListTenantAdminSubjects`, `ListCrossTenantGrants`, `GetTenantAccessPosture` |
| `TenantAdminSetQuotasRequest` | `required string TenantId`, `TenantQuotasDescriptor Quotas` | `SetTenantQuotas` |
| `TenantAdminRegionSetRequest` | `required string TenantId`, `IReadOnlyList<string> Regions` | `AuthorizeAllowedRegions`, `SetTenantResidency` |
| `TenantAdminSubjectRequest` | `required string TenantId`, `required string SubjectId` | `AddTenantAdminSubject`, `RemoveTenantAdminSubject` |
| `TenantAdminGrantOfferRequest` | `required string GranterTenantId`, `required string GranteeTenantId`, `required string Scope`, `TenantGrantAccess Operations` | `OfferCrossTenantGrant` |
| `TenantAdminGrantRequest` | `required string GranterTenantId`, `required string GranteeTenantId`, `required string Scope` | `ApproveCrossTenantGrant`, `RejectCrossTenantGrant`, `RevokeCrossTenantGrant` |
| `TenantSelfCurrentRequest` | (empty) | `GetCurrentTenant` |
| `TenantSelfListRequest` | (empty) | `ListAccessibleTenants` |
| `TenantSelfDescriptorList` | `IReadOnlyList<TenantDescriptor> Tenants` | The `ListAccessibleTenants` response. |
| `AuthSchemeAdvertisementRequest` | (empty) | `GetAuthScheme` |
| `TenantAdminAccessListRequest` | `required string TenantId`, `required TenantAccessPageRequest Page` | `ListTenantGroups`, `ListTenantMembers`, `ListTenantRules` |
| `TenantAdminGroupRequest` | `required string TenantId`, `required string GroupName` | `GetTenantGroup`, `RemoveTenantGroup`, `ListTenantGroupMembers` |
| `TenantAdminGroupUpsertRequest` | `required string TenantId`, `required TenantGroupDescriptor Group` | `UpsertTenantGroup` |
| `TenantAdminGroupMemberRequest` | `required string TenantId`, `required string GroupName`, `required string MemberId`, `TenantSubjectKind MemberKind` | `AddTenantGroupMember`, `RemoveTenantGroupMember` |
| `TenantAdminMemberRequest` | `required string TenantId`, `required string SubjectId`, `TenantSubjectKind SubjectKind` | `AddTenantMember`, `RemoveTenantMember`, `ResolveTenantSubject` |
| `TenantAdminRulePutRequest` | `required string TenantId`, `required TenantRuleDraft Rule` | `PutTenantRule` |
| `TenantAdminRuleRequest` | `required string TenantId`, `required string RuleId` | `GetTenantRule`, `RemoveTenantRule` |
| `TenantAdminExplainRequest` | `required string TenantId`, `required string SubjectId`, `required string TreeName`, `string? Key`, `LatticeOperation Operation`, `TenantSubjectKind SubjectKind` | `ExplainTenantAccess` |
| `TenantAdminEffectivePermissionsRequest` | `required string TenantId`, `required string SubjectId`, `string? TreeName`, `TenantSubjectKind SubjectKind` | `GetTenantEffectivePermissions` |
| `TenantAdminGroupLookup` | `TenantGroupDescriptor? Group` | The `GetTenantGroup` response; `null` when the tenant has no such group. |
| `TenantAdminGroupMemberList` | `IReadOnlyList<TenantGroupMember> Members` | The `ListTenantGroupMembers` response. |
| `TenantAdminRuleLookup` | `TenantRuleView? Rule` | The `GetTenantRule` response; `null` when the tenant has no such rule. |
| `TenantAdminRuleRemoval` | `bool Removed` | The `RemoveTenantRule` response. |

## Registration

Server side (an ASP.NET Core host co-located with the silo):

- `AddLatticeTenantAdminApiGrpc(this IServiceCollection services, Action<LatticeTenantAdminApiGrpcOptions>? configure = null)` -
  registers the gRPC service and its method definitions, the auth interceptor, and a
  set of `TryAdd` seams a host may pre-empt with its own implementation: the
  **default-deny** `ILatticeTenantAdminApiAuthorizer`, the header-reading
  `ILatticeTenantAdminApiCredentialBridge`, and the options-backed
  `ILatticeTenantAdminApiAuthSchemeSource` (which advertises nothing by default).
  Because each is a `TryAdd`, an implementation you register **before** this call is
  kept, and one registered afterwards with `AddSingleton` also wins, because the last
  registration of a service is the one resolved; registering a permissive authorizer
  either way is what opts the surface in. The interceptor is the exception: each call
  appends it to the gRPC pipeline again, so a repeated call authorizes every call to
  this service once per registration - call it once.
- `MapLatticeTenantAdminApiGrpc(this IEndpointRouteBuilder endpoints)` - maps the gRPC
  endpoint. The host must have called `AddLatticeTenantAdminApiGrpc` and must expose
  `ILatticeTenantAdmin` (via `AddLatticeTenantAdminApi`) in the same service
  provider first.

## Configuration

`LatticeTenantAdminApiGrpcOptions`:

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the interceptor enforces the registered `ILatticeTenantAdminApiAuthorizer` on every admin call. Left at its default with the default-deny authorizer in place, the binding refuses everything. Set to `false` only when an outer authentication boundary already guards the endpoint. |
| `CredentialHeaderName` | `string` | `"authorization"` | The request header the caller credential is read from. The default bridge reads it on every call except the unauthenticated `GetAuthScheme`; without `Orleans.Lattice.Auth` registered the core no-op access gate ignores the bridged credential. |
| `CredentialScheme` | `string` | `"Bearer"` | The scheme stamped on the bridged credential. A case-insensitive scheme prefix on the header value is stripped before the remainder is used as the token. |
| `ActiveTenantHeaderName` | `string` | `"lattice-active-tenant"` (`LatticeActiveTenantAssertion.DefaultHeaderName`) | The request header carrying the tenant the caller is acting as. Set to an empty string to disable header-based tenant selection. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` (get-only, mutate in place) | empty | The credential schemes the unauthenticated `GetAuthScheme` RPC advertises, in preference order. Each descriptor must carry only public configuration - never a secret. |

### Per-tenant selection

On a cluster running the optional tenancy add-on, the caller's *active tenant* is
what the self-service surface reports and what scopes the tenant-local view of the
control plane. The binding lifts it from a single request header -
`lattice-active-tenant` by default, configurable through
`LatticeTenantAdminApiGrpcOptions.ActiveTenantHeaderName` - and stamps it onto the
call's ambient scope for the duration of the call.

The header carries only an *assertion*: the tenancy add-on re-validates it against
the caller's subject membership downstream, exactly as it validates the caller
credential. An absent, blank, or syntactically invalid header asserts no tenant, and
the caller resolves the reserved `default` tenant. `GetCurrentTenant` and
`ListAccessibleTenants` resolve the assertion first, so one the caller may not use is
refused as a `PermissionDenied` `RpcException` rather than reported as a tenant the
caller does not hold; `GetTenant` answers only for a tenant the caller administers or
its validated current tenant, and reports anything else as `NotFound`. The
lifecycle, region-residency, admin-subject, grant, and quota-usage RPCs authorize on
the caller's subject and the tenant the request names, not on the asserted active
tenant; every RPC group still maps a fail-closed tenant-resolution refusal to
`PermissionDenied`. Set the option to an empty string to disable header-based tenant
selection entirely. The facade itself requires the tenancy add-on, so a host serving this binding
always has the resolver that validates the assertion.

## Authorization surface

The public seams a host implements or substitutes to open this surface up:

| Type | Kind | Purpose |
|---|---|---|
| `ILatticeTenantAdminApiAuthorizer` | interface | The transport-level gate the interceptor consults on every admin RPC. Implement its single member, `Task<bool> IsAuthorizedAsync(LatticeTenantAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`, to apply a host policy. |
| `DenyTenantAdminApiAuthorizer` | class | The **registered default**: refuses every call, so the surface is closed until a host opts in. |
| `AllowAllTenantAdminApiAuthorizer` | class | Admits every call, deferring entirely to the facade's own gate. For a host whose endpoint is already guarded by an outer boundary. |
| `LatticeTenantAdminApiAuthorizationContext` | readonly struct | What the authorizer is handed: the `Operation`, the `TargetId` (the tenant id the request names - for every cross-tenant grant call, including the grantee-side approve and reject, the granting tenant - or `null` when not tenant-scoped), and the raw `ServerCallContext` for header / identity / peer inspection. |
| `LatticeTenantAdminApiOperation` | enum | The per-operation discriminator. Tenant lifecycle and quota: `CreateTenant`, `SuspendTenant`, `ResumeTenant`, `DeleteTenant`, `SetTenantQuotas`, `GetTenantQuotaUsage`. Region residency: `AuthorizeAllowedRegions`, `SetTenantResidency`, `GetTenantRegionStatus`. Tenant-admin subjects: `ListTenantAdminSubjects`, `AddTenantAdminSubject`, `RemoveTenantAdminSubject`. Cross-tenant grants: `ListCrossTenantGrants`, `OfferCrossTenantGrant`, `ApproveCrossTenantGrant`, `RejectCrossTenantGrant`, `RevokeCrossTenantGrant`. Delegated tenant access: `ListTenantGroups`, `GetTenantGroup`, `UpsertTenantGroup`, `RemoveTenantGroup`, `ListTenantGroupMembers`, `AddTenantGroupMember`, `RemoveTenantGroupMember`, `ListTenantMembers`, `AddTenantMember`, `RemoveTenantMember`, `ResolveTenantSubject`, `PutTenantRule`, `GetTenantRule`, `RemoveTenantRule`, `ListTenantRules`, `ExplainTenantAccess`, `GetTenantEffectivePermissions`, `GetTenantAccessPosture`. An unrecognised method maps to `Unknown`, never to a permissive default - so a deny-by-default policy refuses an RPC it has never heard of rather than falling through. |
| `ILatticeTenantAdminApiCredentialBridge` | interface | Lifts the inbound credential (`LatticeCredential? Resolve(ServerCallContext context)`) into the ambient Lattice credential for the duration of one call. The default reads the configured header; substitute it for a bespoke identity source such as a client certificate. |
| `ILatticeTenantAdminApiAuthSchemeSource` | interface | Supplies what the unauthenticated `GetAuthScheme` RPC advertises (`AuthSchemeAdvertisement GetAdvertisement()`). The default projects `LatticeTenantAdminApiGrpcOptions.AdvertisedAuthSchemes`. |
| `AuthSchemeDescriptor` | record | One advertised credential scheme: its required `SchemeId`, a friendly `DisplayName`, and the public `Parameters` a client needs to run the sign-in challenge. |
| `AuthSchemeAdvertisement` | record | The `GetAuthScheme` response envelope carrying the descriptor list. |

The interceptor itself is internal. It runs the authorizer and scopes enforcement to
this service by matching on the service-name prefix, so unrelated gRPC services hosted
in the same ASP.NET Core pipeline are unaffected. The credential bridging happens
after it, in the service: each RPC lifts the caller credential (through
`ILatticeTenantAdminApiCredentialBridge`) and the asserted active tenant onto the
ambient context for that call only, so the facade's fail-closed gate sees the caller's
identity and every credential is isolated to its own call.

## See also

- [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) - the
  transport-agnostic facade this package binds.
- [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md) - the core multi-tenancy
  companion.
- [`Orleans.Lattice.Api.TreeAdmin.Grpc`](../lattice.api.treeadmin.grpc/README.md) - the
  sibling gRPC binding this one mirrors.
- [MultiTenancy sample](../../samples/MultiTenancy/README.md).
