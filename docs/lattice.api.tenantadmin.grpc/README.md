# Orleans.Lattice.Api.TenantAdmin.Grpc

The code-first gRPC **binding** and public **client** for the
[`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) tenant
lifecycle control facade. It exposes `ILatticeTenantAdmin` over a network transport
as a thin adapter - the control semantics live in the facade, this package only
marshals them.

## What is it?

A `protobuf-net.Grpc` code-first binding that hosts the tenant-administration facade
as a gRPC service and ships a strongly-typed client for calling it remotely. It
mirrors the [TreeAdmin gRPC binding](../lattice.api.treeadmin.grpc/README.md)
packaging exactly: server-side registration + endpoint mapping extensions, a
`LatticeTenantAdminApiGrpcClient`, a fail-closed auth interceptor, and an
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

## Service and RPCs

The service surfaces the four `ILatticeTenantAdmin` operations plus the auth-scheme
advertisement:

| RPC | Facade method |
|---|---|
| `CreateTenant` | `CreateTenantAsync` |
| `SuspendTenant` | `SuspendTenantAsync` |
| `ResumeTenant` | `ResumeTenantAsync` |
| `DeleteTenant` | `DeleteTenantAsync` |
| `GetAuthScheme` | (binding-local) advertises accepted credential schemes |

### Client method signatures

`LatticeTenantAdminApiGrpcClient`:

| Method | Signature |
|---|---|
| `CreateTenantAsync` | `Task<TenantCreationResult> CreateTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `SuspendTenantAsync` | `Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `ResumeTenantAsync` | `Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `DeleteTenantAsync` | `Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)` |
| `GetAuthSchemeAsync` | `Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)` |

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
| `AdvertisedAuthSchemes` | (empty) | The credential schemes advertised by the `GetAuthScheme` RPC. |

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
