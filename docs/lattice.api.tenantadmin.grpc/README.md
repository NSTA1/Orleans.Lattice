# Orleans.Lattice.Api.TenantAdmin.Grpc

The code-first gRPC **binding** and public **clients** for the
[`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) tenant
lifecycle control facade and its read-only tenant self-service companion. It
exposes `ILatticeTenantAdmin` and `ILatticeTenantSelfService` over a network
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

- **Thin adapter.** Each RPC forwards one-to-one to an `ILatticeTenantAdmin` method;
  no control logic lives here.
- **Fail-closed authorization.** The server interceptor requires a caller credential
  by default (`RequireAuthorization = true`); a request without an accepted credential
  is rejected before it reaches the facade, which then applies its own gate.
- **Credential isolation per call.** The caller credential is read from a configurable
  header and bridged into the facade's authorization context for that call only.
- **Discoverable auth.** A `GetAuthScheme` RPC advertises the accepted credential
  schemes so a client can self-configure.
- **Self-service reads exempt from default-deny, still scoped.** The three read-only
  self-service RPCs are exempt from the tenant-admin authorizer's default-deny (any
  authenticated caller may call them) but are still credential-stamped, so the facade
  enforces fail-closed per-tenant scoping at the single narrowest seam - a caller only
  ever sees its own authorized tenants.

## Service and RPCs

The service surfaces the five `ILatticeTenantAdmin` lifecycle operations, three
read-only `ILatticeTenantSelfService` RPCs, and the auth-scheme advertisement:

| RPC | Facade method |
|---|---|
| `CreateTenant` | `CreateTenantAsync` (carries the optional admin-subject set) |
| `SuspendTenant` | `SuspendTenantAsync` |
| `ResumeTenant` | `ResumeTenantAsync` |
| `DeleteTenant` | `DeleteTenantAsync` |
| `SetTenantQuotas` | `SetTenantQuotasAsync` |
| `GetCurrentTenant` | `ILatticeTenantSelfService.GetCurrentTenantAsync` (self-service; exempt from default-deny) |
| `ListAccessibleTenants` | `ILatticeTenantSelfService.ListAccessibleTenantsAsync` (self-service; exempt from default-deny) |
| `GetTenant` | `ILatticeTenantSelfService.GetTenantAsync` (self-service; exempt from default-deny) |
| `GetAuthScheme` | (binding-local) advertises accepted credential schemes |

### Client method signatures

`LatticeTenantAdminApiGrpcClient`:

| Method | Signature |
|---|---|
| `CreateTenantAsync` | `Task<TenantCreationResult> CreateTenantAsync(string tenantId, IReadOnlyCollection<string>? adminSubjects = null, CancellationToken cancellationToken = default)` |
| `SuspendTenantAsync` | `Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `ResumeTenantAsync` | `Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `DeleteTenantAsync` | `Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `SetTenantQuotasAsync` | `Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)` |
| `GetAuthSchemeAsync` | `Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)` |

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
  registers the service and its interceptor.
- `MapLatticeTenantAdminApiGrpc(this IEndpointRouteBuilder endpoints)` - maps the gRPC
  endpoint.

## Configuration

`LatticeTenantAdminApiGrpcOptions`:

| Property | Default | Meaning |
|---|---|---|
| `RequireAuthorization` | `true` | Reject any call without an accepted caller credential before it reaches the facade. |
| `CredentialHeaderName` | `"authorization"` | The request header the caller credential is read from. |
| `CredentialScheme` | `"Bearer"` | The credential scheme prefix stripped from the header value. |
| `ActiveTenantHeaderName` | `"lattice-active-tenant"` | The request header carrying the tenant the caller is acting as. Set to an empty string to disable header-based tenant selection. |
| `AdvertisedAuthSchemes` | (empty) | The credential schemes advertised by the `GetAuthScheme` RPC. |

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
use is refused, and both the self-service and lifecycle verbs surface that as a
`PermissionDenied` `RpcException` rather than reporting a tenant the caller does not
hold. Set the option to an empty string to disable header-based tenant selection
entirely; with no tenancy add-on registered the header is never consulted.

## Authorization surface

- **`AuthSchemeDescriptor`** - one advertised credential scheme (name and metadata),
  returned by `GetAuthSchemeAsync` so a client can discover how to authenticate.
- The server interceptor bridges the accepted credential into the facade's
  authorization context per call, so the facade's fail-closed gate sees the caller's
  identity and every credential is isolated to its own call.

## See also

- [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) - the
  transport-agnostic facade this package binds.
- [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md) - the core multi-tenancy
  companion.
- [`Orleans.Lattice.Api.TreeAdmin.Grpc`](../lattice.api.treeadmin.grpc/README.md) - the
  sibling gRPC binding this one mirrors.
- [MultiTenancy sample](../../samples/MultiTenancy/README.md).
