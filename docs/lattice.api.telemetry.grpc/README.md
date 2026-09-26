# Orleans.Lattice.Api.Telemetry.Grpc

The **gRPC binding** for [`Orleans.Lattice.Api.Telemetry`](../lattice.api.telemetry/README.md).
It exposes the telemetry facade to a remote head - notably the Explorer's desktop
head, which cannot enforce tenant scoping locally and so must be served by a
routable, server-scoped endpoint.

## Reference closure

This package references **only** the shared contract package,
`Orleans.Lattice.Api.Abstractions`. It does not reference the facade
implementation, and it reaches no MCP package transitively. That closure is
asserted rather than assumed: the test suite walks the whole `ProjectReference`
graph, sweeps transitive `PackageReference` ids, and inspects the emitted
assembly references.

It also asserts that the client's reachable public surface contains no Orleans
grain interface, so a head that consumes this binding never takes a dependency on
the cluster's internal grain contracts.

## Service

Service name `orleans.lattice.api.telemetry`, three unary RPCs:

| RPC | Request -> Response | Notes |
|---|---|---|
| `GetCatalog` | `TelemetryCatalogRequest` -> `TelemetryQueryCatalog` | What this deployment offers. |
| `Query` | `TelemetryQueryRequest` -> `TelemetryQueryResponse` | Carries the contract's own messages unchanged. |
| `GetAuthScheme` | `AuthSchemeAdvertisementRequest` -> `AuthSchemeAdvertisement` | Unauthenticated; exempt from the interceptor so a client can discover how to authenticate. |

`Query` deliberately reuses the contract's own request and response types rather
than defining binding-specific ones. That is a deliberate constraint: with no
binding-owned query message, the wire can never grow a free-text query field or a
second tenant assertion, so the facade's "name a query id, never supply PromQL"
guarantee holds at the transport too.

## Hosting

A host composes two packages: the facade, and this binding. They are registered
separately and deliberately cannot be demonstrated in one compiled snippet -
this package does not reference the facade, which is the closure described
above, so a sample compiled against the binding alone cannot name
`AddLatticeTelemetryApi()`.

First register the facade, from
[`Orleans.Lattice.Api.Telemetry`](../lattice.api.telemetry/README.md):

```text
builder.Services.AddLatticeTelemetryApi();
```

Then the binding, which is what this package provides:

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Telemetry.Grpc;

var builder = WebApplication.CreateBuilder();

// RequireAuthorization defaults to true; the default authorizer denies.
builder.Services.AddLatticeTelemetryApiGrpc();

var app = builder.Build();
app.MapLatticeTelemetryApiGrpc();
```

`AddLatticeTelemetryApi()` is idempotent, so ordering between the two is not
load-bearing.

The binding resolves on a host with **only** `ILatticeTelemetry` registered - no
access gate, no membership context, no tenant-context resolver. A constructor
guard pins that, so adding a hidden dependency later fails loudly rather than
silently raising the bar for every host.

## Client

`LatticeTelemetryApiGrpcClient.Create(CallInvoker, IServiceProvider)` builds the
client over a caller-supplied `CallInvoker` and a service provider with Orleans
serialization registered (`AddSerializer()`), so its marshallers match the
server's. It exposes the three RPCs as `GetCatalogAsync`,
`QueryAsync(TelemetryQueryRequest)`, and `GetAuthSchemeAsync`, and carries no
transport policy of its own: address, TLS, retries, deadlines, and call
credentials live on the `CallInvoker` / `GrpcChannel`. It forwards the visibility
the caller requests and returns the facade's pinned `Scope` unchanged - render
that scope, never the one that was asked for.

## Authorization

| Seam | Default | Purpose |
|---|---|---|
| `ILatticeTelemetryApiAuthorizer` | `DenyTelemetryApiAuthorizer` | Fail-closed. Registered with `TryAdd`, so a host must deliberately replace it - with a custom authorizer, or with the opt-in `AllowAllTelemetryApiAuthorizer` behind a separate authentication boundary. Permitting a call never widens what the facade scopes the caller to. |
| `ILatticeTelemetryApiCredentialBridge` | header-based | Carries an opaque caller credential onto the ambient context for the duration of the call. |
| `ILatticeTelemetryApiAuthSchemeSource` | options-based | Backs the unauthenticated `GetAuthScheme` probe. |

The interceptor is scoped to this service's method prefix, exempts
`GetAuthScheme`, and maps an unrecognised method to `LatticeTelemetryApiOperation.Unknown`
so a deny-by-default policy refuses it rather than falling through.

### Server options (`LatticeTelemetryApiGrpcOptions`)

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the interceptor enforces `ILatticeTelemetryApiAuthorizer` on every inbound call. Set `false` only when an outer authentication boundary already guards the endpoint. |
| `CredentialHeaderName` | `string` | `authorization` | Request header carrying the caller's credential token, bridged into the ambient Lattice credential. The default header bridge reads it on every `GetCatalog` and `Query` call, whether or not the `Orleans.Lattice.Auth` add-on is registered; the facade's own gate resolves the caller's subject from it. |
| `CredentialScheme` | `string` | `Bearer` | Scheme stamped on the bridged credential; a matching case-insensitive prefix on the header value is stripped. |
| `ActiveTenantHeaderName` | `string` | `lattice-active-tenant` | Request header carrying the tenant the caller is acting as, lifted onto the ambient active-tenant context per call. Only carried: it is re-validated downstream and the facade derives the effective tenant server-side, so it can never widen a caller's scope. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | empty | Auth schemes the unauthenticated `GetAuthScheme` RPC advertises, in preference order; public configuration only. |

## The binding derives no tenant

It relays. `RequestedVisibility` and `RequestedTenantId` are forwarded verbatim,
and the facade's resulting `Scope` is returned untouched - including
`WasDowngraded` and `IsCrossTenant`, so a client can tell that it received less
than it asked for.

A reflection guard fails the build if any member named `*ResolveTenant*`,
`*DeriveTenant*`, `*EffectiveTenant*` or `*DefaultTenant*` ever appears in this
assembly. The authorizer's target id is the **query id**, never a wire-supplied
tenant.

## Status mapping

| Exception | Status | Why |
|---|---|---|
| `TelemetryQueryNotFoundException` | `NotFound` | Caller error. Unknown and unoffered stay indistinguishable. |
| `TelemetryQueryBoundsException` | `OutOfRange` | Well-formed request; the guardrails refuse the window. |
| `TelemetryBackendException` | `Unavailable` | Not the caller's fault - the retryable-with-backoff code. |
| `LatticeAuthorizationDeniedException`, `LatticeTenantAccessDeniedException` | `PermissionDenied` | A denial, not an internal fault. |
| `ArgumentException` | `InvalidArgument` | |
| `OperationCanceledException` | `Cancelled` | |
| anything else | `Internal` | The original message is suppressed. |

Two deliberate non-translations, both tested: an unconfigured backend arriving as
`NotFound` stays `NotFound` and is **not** upgraded to `Unavailable`, and a
capability denial never collapses into `NotFound`. A catalogue that offered
nothing cannot then refuse a query for a different-looking reason.

**The backend fault's detail is not forwarded.** Its message embeds the
underlying transport fault, which routinely carries the backend host and port;
this facade is routable and its callers are untrusted heads, so the real reason
is logged server-side and the caller receives a fixed detail naming only the
query id it already supplied.

## Wire aliases

Serializable types in this package use the reserved `oitlg.` alias prefix, which
is disjoint from the contract's `oitl.` set. Four aliases only - the binding adds
nothing else to the wire. Aliases are wire format: never rename or remove one.

## See also

- [`Orleans.Lattice.Api.Telemetry`](../lattice.api.telemetry/README.md) - the facade this binding exposes.
