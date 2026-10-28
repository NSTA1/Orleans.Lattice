---
agent_spec: "docs/agents/api/telemetry.json"
---

# Configuration

`AddLatticeTelemetryApiGrpc(Action<LatticeTelemetryApiGrpcOptions>? configure = null)` binds `LatticeTelemetryApiGrpcOptions`, the only options type in this package. It configures the server side of the binding: whether the transport authorizer runs, which request headers carry the caller's credential and asserted tenant, and what the unauthenticated `GetAuthScheme` RPC advertises. The client has no options; its transport policy lives on the `GrpcChannel` or `CallInvoker` passed to `LatticeTelemetryApiGrpcClient.Create`. No options validator is registered.

The facade's backend address, backend credential, range guardrails and metric allow-list are `LatticeTelemetryOptions`, documented in the [telemetry facade configuration](../lattice.api.telemetry/configuration.md).

## `LatticeTelemetryApiGrpcOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the interceptor consults `ILatticeTelemetryApiAuthorizer` before `GetCatalog` and `Query`. Default-deny: with the default `DenyTelemetryApiAuthorizer` both are refused with `PermissionDenied`. `GetAuthScheme` is never gated. Set `false` only when an outer authentication boundary already guards the endpoint; the facade's own `Telemetry` capability check still runs. Read per call through the options monitor. |
| `CredentialHeaderName` | `string` | `"authorization"` | Request-metadata header the default credential bridge reads on every `GetCatalog` and `Query` call, whether or not the `Orleans.Lattice.Auth` add-on is registered; the facade's gate resolves the caller's subject from it. |
| `CredentialScheme` | `string` | `"Bearer"` | Scheme stamped on the bridged credential; a matching case-insensitive prefix on the header value is stripped. |
| `ActiveTenantHeaderName` | `string` | `"lattice-active-tenant"` (`LatticeActiveTenantAssertion.DefaultHeaderName`) | Request-metadata header carrying the tenant the caller is acting as, lifted onto the ambient active-tenant scope per call. It is only carried: it is re-validated downstream and the facade derives the effective tenant itself, so it can never widen a caller's scope. An absent, blank or syntactically invalid value asserts no tenant. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | empty list | Auth schemes the unauthenticated `GetAuthScheme` RPC advertises, in preference order. Get-only: add descriptors to the existing list. Public configuration only - the advertisement is served without a credential. |

## Registration-time seams

`AddLatticeTelemetryApiGrpc` registers each seam with `TryAdd`, so an implementation the host registered earlier is kept.

| Seam | Registered default | Replace it to |
|---|---|---|
| `ILatticeTelemetryApiAuthorizer` | `DenyTelemetryApiAuthorizer` | Admit callers - with a custom authorizer, or with `AllowAllTelemetryApiAuthorizer` behind a separate authentication boundary. Admitting a call never widens the tenant scope the facade derives. |
| `ILatticeTelemetryApiCredentialBridge` | header bridge using `CredentialHeaderName` and `CredentialScheme` | Source the caller identity elsewhere. |
| `ILatticeTelemetryApiAuthSchemeSource` | source backed by `AdvertisedAuthSchemes` | Advertise schemes from another source; it must return public configuration only. |

Call `AddLatticeTelemetryApiGrpc` once. Every registration it makes is idempotent except the authorization interceptor, which is appended to the host's gRPC pipeline on each call. The interceptor acts only on methods under the `orleans.lattice.api.telemetry` service, so other gRPC services on the same host are unaffected.

## Source map

- [Options](../../src/lattice.api.telemetry.grpc/LatticeTelemetryApiGrpcOptions.cs)
- [Registration](../../src/lattice.api.telemetry.grpc/LatticeTelemetryApiGrpcServiceCollectionExtensions.cs)
- [Authorizer seam](../../src/lattice.api.telemetry.grpc/Security/ILatticeTelemetryApiAuthorizer.cs)
- [Credential bridge seam](../../src/lattice.api.telemetry.grpc/ILatticeTelemetryApiCredentialBridge.cs)
- [Auth-scheme source seam](../../src/lattice.api.telemetry.grpc/Security/ILatticeTelemetryApiAuthSchemeSource.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
- [Telemetry facade configuration](../lattice.api.telemetry/configuration.md)
