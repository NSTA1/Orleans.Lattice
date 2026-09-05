# Orleans.Lattice.Api.TenantAdmin.Grpc

The code-first gRPC **binding** and public **clients** for the
[`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) tenant
lifecycle and region-residency control facades and their read-only tenant
self-service companion. It exposes `ILatticeTenantAdmin`,
`ILatticeTenantRegionAdmin`, and `ILatticeTenantSelfService` over a network
transport as thin adapters - the control and scoping semantics live in the
facades, this package only marshals them.

## What is it?

A `protobuf-net.Grpc` code-first binding that hosts the tenant-administration facade
as a gRPC service and ships strongly-typed clients for calling it remotely. It
mirrors the [TreeAdmin gRPC binding](../lattice.api.treeadmin.grpc/README.md)
packaging exactly: server-side registration + endpoint mapping extensions, a
`LatticeTenantAdminApiGrpcClient`, a read-only `LatticeTenantSelfServiceApiGrpcClient`,
a fail-closed auth interceptor, and an
auth-scheme advertisement RPC so a client can discover how to authenticate.

## Core properties

- **Thin adapter.** Each RPC forwards one-to-one to an `ILatticeTenantAdmin`,
  `ILatticeTenantRegionAdmin`, or `ILatticeTenantSelfService` method; no control
  logic lives here.
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
grant operations, the read-only `ILatticeTenantSelfService` operations, and the
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

`ILatticeTenantRegionAdmin` is an **optional** dependency of the service.
`AddLatticeTenantAdminApi` registers it, so an ordinary silo serves the whole
region-residency group; a host that composes the binding without it still serves every
lifecycle and self-service RPC and answers each region RPC with `Unimplemented`,
rather than failing container construction at startup.

### Status mapping

Every domain failure maps to an explicit status rather than falling through to a
generic fault. The RPC groups (lifecycle, region residency, self-service) share the
same vocabulary; the last column notes where an arm applies to only some of them.

| Exception | gRPC status | Why |
|---|---|---|
| `TenantNotFoundException` | `NotFound` | The tenant is not registered - or, on the self-service surface, is not one the caller may see. |
| `TenantAlreadyExistsException` | `AlreadyExists` | `CreateTenant` was called for an id already registered. Lifecycle only. |
| `TenantRegionNotAllowedException` | `FailedPrecondition` | The requested residency is outside the operator-authored allowed set, or the revoked region is still resident. The caller must change state first, then retry. Region residency only. |
| `TenantLastRegionException` | `FailedPrecondition` | The change would remove the tenant's last resident region. Region residency only. |
| `ReservedTenantOperationException` | `FailedPrecondition` | The operation targets the reserved `default` tenant (suspend, delete, or set-quotas). |
| `InvalidOperationException` | `FailedPrecondition` | A lifecycle or residency precondition the facade refuses on a well-formed request. |
| `LatticeAuthorizationDeniedException` | `PermissionDenied` | The caller does not hold the required tier. |
| `LatticeTenantAccessDeniedException` | `PermissionDenied` | Fail-closed tenant resolution refused the caller's asserted active tenant. Deliberately not `Internal`, which a client would retry. |
| `ArgumentException` | `InvalidArgument` | A malformed tenant id or region id. |
| `OperationCanceledException` | `Cancelled` | The caller's deadline or cancellation token fired. |
| anything else | `Internal` | The catch-all, logged server-side and returned without echoing the exception text. |

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

`LatticeTenantSelfServiceApiGrpcClient` (read-only; construct with
`LatticeTenantSelfServiceApiGrpcClient.Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`):

| Method | Signature |
|---|---|
| `GetCurrentTenantAsync` | `Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)` |
| `ListAccessibleTenantsAsync` | `Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)` |
| `GetTenantAsync` | `Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |

## Registration

Server side (an ASP.NET Core host co-located with the silo):

- `AddLatticeTenantAdminApiGrpc(this IServiceCollection services, Action<LatticeTenantAdminApiGrpcOptions>? configure = null)` -
  registers the gRPC service and its method definitions, the auth interceptor, and a
  set of `TryAdd` seams a host may pre-empt with its own implementation: the
  **default-deny** `ILatticeTenantAdminApiAuthorizer`, the header-reading
  `ILatticeTenantAdminApiCredentialBridge`, and the options-backed
  `ILatticeTenantAdminApiAuthSchemeSource` (which advertises nothing by default).
  Because each is a `TryAdd`, registering your own **before** this call is what
  opts the surface in.
- `MapLatticeTenantAdminApiGrpc(this IEndpointRouteBuilder endpoints)` - maps the gRPC
  endpoint. The host must have called `AddLatticeTenantAdminApiGrpc` and must expose
  `ILatticeTenantAdmin` (via `AddLatticeTenantAdminApi`) in the same service
  provider first.

## Configuration

`LatticeTenantAdminApiGrpcOptions`:

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the interceptor enforces the registered `ILatticeTenantAdminApiAuthorizer` on every admin call. Left at its default with the default-deny authorizer in place, the binding refuses everything. Set to `false` only when an outer authentication boundary already guards the endpoint. |
| `CredentialHeaderName` | `string` | `"authorization"` | The request header the caller credential is read from. Only consulted when `Orleans.Lattice.Auth` is registered; without it no header is read. |
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
the caller resolves the reserved `default` tenant. An assertion the caller may not
use is refused, and every verb on this service - self-service, lifecycle, and region
residency alike - surfaces that as a `PermissionDenied` `RpcException` rather than
reporting a tenant the caller does not hold. Set the option to an empty string to disable header-based tenant selection
entirely; with no tenancy add-on registered the header is never consulted.

## Authorization surface

The public seams a host implements or substitutes to open this surface up:

| Type | Kind | Purpose |
|---|---|---|
| `ILatticeTenantAdminApiAuthorizer` | interface | The transport-level gate the interceptor consults on every admin RPC. Implement it to apply a host policy. |
| `DenyTenantAdminApiAuthorizer` | class | The **registered default**: refuses every call, so the surface is closed until a host opts in. |
| `AllowAllTenantAdminApiAuthorizer` | class | Admits every call, deferring entirely to the facade's own gate. For a host whose endpoint is already guarded by an outer boundary. |
| `LatticeTenantAdminApiAuthorizationContext` | readonly struct | What the authorizer is handed: the `Operation`, the `TargetId` (the tenant the call acts on, `null` when not tenant-scoped), and the raw `ServerCallContext` for header / identity / peer inspection. |
| `LatticeTenantAdminApiOperation` | enum | The per-operation discriminator. Tenant lifecycle and quota: `CreateTenant`, `SuspendTenant`, `ResumeTenant`, `DeleteTenant`, `SetTenantQuotas`, `GetTenantQuotaUsage`. Region residency: `AuthorizeAllowedRegions`, `SetTenantResidency`, `GetTenantRegionStatus`. Tenant-admin subjects: `ListTenantAdminSubjects`, `AddTenantAdminSubject`, `RemoveTenantAdminSubject`. Cross-tenant grants: `ListCrossTenantGrants`, `OfferCrossTenantGrant`, `ApproveCrossTenantGrant`, `RejectCrossTenantGrant`, `RevokeCrossTenantGrant`. An unrecognised method maps to `Unknown`, never to a permissive default - so a deny-by-default policy refuses an RPC it has never heard of rather than falling through. |
| `ILatticeTenantAdminApiCredentialBridge` | interface | Lifts the inbound credential into the ambient Lattice credential for the duration of one call. The default reads the configured header; substitute it for a bespoke identity source such as a client certificate. |
| `ILatticeTenantAdminApiAuthSchemeSource` | interface | Supplies what the unauthenticated `GetAuthScheme` RPC advertises. The default projects `LatticeTenantAdminApiGrpcOptions.AdvertisedAuthSchemes`. |
| `AuthSchemeDescriptor` | record | One advertised credential scheme (name and metadata). |
| `AuthSchemeAdvertisement` | record | The `GetAuthScheme` response envelope carrying the descriptor list. |

The interceptor itself is internal. It bridges the accepted credential into the
facade's authorization context per call, so the facade's fail-closed gate sees the
caller's identity and every credential is isolated to its own call, and it scopes
enforcement to this service by matching on the service-name prefix, so unrelated
gRPC services hosted in the same ASP.NET Core pipeline are unaffected.

## See also

- [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) - the
  transport-agnostic facade this package binds.
- [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md) - the core multi-tenancy
  companion.
- [`Orleans.Lattice.Api.TreeAdmin.Grpc`](../lattice.api.treeadmin.grpc/README.md) - the
  sibling gRPC binding this one mirrors.
- [MultiTenancy sample](../../samples/MultiTenancy/README.md).
