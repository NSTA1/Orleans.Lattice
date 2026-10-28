---
agent_spec: "docs/agents/api/telemetry.json"
---

# Public API

`Orleans.Lattice.Api.Telemetry.Grpc` publishes the remote half of the telemetry facade: `LatticeTelemetryApiGrpcClient`, the server options, the transport authorization seam and its two implementations, the identity and auth-scheme advertisement seams, the binding-owned wire records, and the registration extensions. The gRPC service, method definitions, marshallers and authorization interceptor are internal; they are described by behaviour in [Architecture](architecture.md).

`Query` carries the shared contract's own `TelemetryQueryRequest` and `TelemetryQueryResponse`, and `GetCatalog` answers with its `TelemetryQueryCatalog`; those types are listed in the [telemetry facade's public API](../lattice.api.telemetry/api.md#shared-contract-declaration-reference). This package references only `Orleans.Lattice.Api.Abstractions`, not the facade implementation; see [Reference closure](README.md#reference-closure).

## Registration

| Method | What it registers |
|---|---|
| `AddLatticeTelemetryApiGrpc(this IServiceCollection services, Action<LatticeTelemetryApiGrpcOptions>? configure = null)` | The options, the default-deny `ILatticeTelemetryApiAuthorizer` (`DenyTelemetryApiAuthorizer`), the header-based `ILatticeTelemetryApiCredentialBridge`, the options-backed `ILatticeTelemetryApiAuthSchemeSource`, the authorization interceptor, and the service. Each seam is registered with `TryAdd`; the interceptor is appended to the gRPC pipeline on every call, so call it once. |
| `MapLatticeTelemetryApiGrpc(this IEndpointRouteBuilder endpoints)` | Maps the `orleans.lattice.api.telemetry` service routes. |

The host must also register `ILatticeTelemetry`, normally with `AddLatticeTelemetryApi()` from the facade package; see [Hosting](README.md#hosting). Option values are documented in [Configuration](configuration.md).

## Client

`LatticeTelemetryApiGrpcClient.Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` builds a client over a caller-supplied call invoker, resolving the per-message Orleans serializers from `serializerProvider`. It carries no transport policy of its own.

| Client method | RPC | Request | Response |
|---|---|---|---|
| `GetCatalogAsync(CancellationToken)` | `GetCatalog` | `TelemetryCatalogRequest` (sent for you) | `TelemetryQueryCatalog` |
| `QueryAsync(TelemetryQueryRequest, CancellationToken)` | `Query` | `TelemetryQueryRequest` | `TelemetryQueryResponse` |
| `GetAuthSchemeAsync(CancellationToken)` | `GetAuthScheme` | `AuthSchemeAdvertisementRequest` (sent for you) | `AuthSchemeAdvertisement` |

All three RPCs are unary. A failed call surfaces as an `RpcException` whose status follows the binding's [status mapping](README.md#status-mapping).

## Seams

| Seam | Default | Purpose |
|---|---|---|
| `ILatticeTelemetryApiAuthorizer` | `DenyTelemetryApiAuthorizer` | Coarse admission for `GetCatalog` and `Query`, given a `LatticeTelemetryApiAuthorizationContext`. `AllowAllTelemetryApiAuthorizer` is the opt-in permissive implementation. |
| `ILatticeTelemetryApiCredentialBridge` | header-based (internal) | Lifts the caller credential from request metadata onto the ambient Lattice credential. |
| `ILatticeTelemetryApiAuthSchemeSource` | options-backed (internal) | Supplies the advertisement the unauthenticated `GetAuthScheme` RPC returns. |

## Wire records

The binding owns four serializable records - `TelemetryCatalogRequest`, `AuthSchemeAdvertisementRequest`, `AuthSchemeDescriptor` and `AuthSchemeAdvertisement` - whose `oitlg.` wire aliases are the constants in `GrpcTelemetryTypeAliases`. See [Wire aliases](README.md#wire-aliases).

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Telemetry facade public API](../lattice.api.telemetry/api.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Telemetry.Grpc.ILatticeTelemetryApiCredentialBridge`

[Source](../../src/lattice.api.telemetry.grpc/ILatticeTelemetryApiCredentialBridge.cs) (line 37).

`public interface ILatticeTelemetryApiCredentialBridge`

- `LatticeCredential? Resolve(ServerCallContext context)`

### `Orleans.Lattice.Api.Telemetry.Grpc.LatticeTelemetryApiGrpcClient`

[Source](../../src/lattice.api.telemetry.grpc/LatticeTelemetryApiGrpcClient.cs) (line 45).

`public sealed class LatticeTelemetryApiGrpcClient`

- `public static LatticeTelemetryApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)`
- `public Task<TelemetryQueryResponse> QueryAsync(TelemetryQueryRequest request, CancellationToken cancellationToken = default)`
- `public Task<AuthSchemeAdvertisement> GetAuthSchemeAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Telemetry.Grpc.LatticeTelemetryApiGrpcOptions`

[Source](../../src/lattice.api.telemetry.grpc/LatticeTelemetryApiGrpcOptions.cs) (line 7).

`public sealed class LatticeTelemetryApiGrpcOptions`

- `public bool RequireAuthorization`
- `public string CredentialHeaderName`
- `public string CredentialScheme`
- `public string ActiveTenantHeaderName`
- `public IList<AuthSchemeDescriptor> AdvertisedAuthSchemes`

### `Orleans.Lattice.Api.Telemetry.Grpc.LatticeTelemetryApiGrpcServiceCollectionExtensions`

[Source](../../src/lattice.api.telemetry.grpc/LatticeTelemetryApiGrpcServiceCollectionExtensions.cs) (line 41).

`public static class LatticeTelemetryApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeTelemetryApiGrpc(this IServiceCollection services, Action<LatticeTelemetryApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeTelemetryApiGrpc(this IEndpointRouteBuilder endpoints)`

### `Orleans.Lattice.Api.Telemetry.Grpc.AuthSchemeAdvertisement`

[Source](../../src/lattice.api.telemetry.grpc/Model/AuthSchemeAdvertisement.cs) (line 12).

`public sealed record AuthSchemeAdvertisement`

- `public IReadOnlyList<AuthSchemeDescriptor> Schemes`

### `Orleans.Lattice.Api.Telemetry.Grpc.AuthSchemeAdvertisementRequest`

[Source](../../src/lattice.api.telemetry.grpc/Model/AuthSchemeAdvertisementRequest.cs) (line 11).

`public sealed record AuthSchemeAdvertisementRequest`

### `Orleans.Lattice.Api.Telemetry.Grpc.AuthSchemeDescriptor`

[Source](../../src/lattice.api.telemetry.grpc/Model/AuthSchemeDescriptor.cs) (line 19).

`public sealed record AuthSchemeDescriptor`

- `public required string SchemeId`
- `public string DisplayName`
- `public IReadOnlyDictionary<string, string> Parameters`

### `Orleans.Lattice.Api.Telemetry.Grpc.GrpcTelemetryTypeAliases`

[Source](../../src/lattice.api.telemetry.grpc/Model/GrpcTelemetryTypeAliases.cs) (line 27).

`public static class GrpcTelemetryTypeAliases`

- `public const string AliasPrefix`
- `public const string TelemetryCatalogRequest`
- `public const string AuthSchemeAdvertisementRequest`
- `public const string AuthSchemeDescriptor`
- `public const string AuthSchemeAdvertisement`

### `Orleans.Lattice.Api.Telemetry.Grpc.TelemetryCatalogRequest`

[Source](../../src/lattice.api.telemetry.grpc/Model/TelemetryCatalogRequest.cs) (line 18).

`public sealed record TelemetryCatalogRequest`

### `Orleans.Lattice.Api.Telemetry.Grpc.ILatticeTelemetryApiAuthSchemeSource`

[Source](../../src/lattice.api.telemetry.grpc/Security/ILatticeTelemetryApiAuthSchemeSource.cs) (line 15).

`public interface ILatticeTelemetryApiAuthSchemeSource`

- `AuthSchemeAdvertisement GetAdvertisement()`

### `Orleans.Lattice.Api.Telemetry.Grpc.ILatticeTelemetryApiAuthorizer`

[Source](../../src/lattice.api.telemetry.grpc/Security/ILatticeTelemetryApiAuthorizer.cs) (line 23).

`public interface ILatticeTelemetryApiAuthorizer`

- `Task<bool> IsAuthorizedAsync(LatticeTelemetryApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Telemetry.Grpc.DenyTelemetryApiAuthorizer`

[Source](../../src/lattice.api.telemetry.grpc/Security/ILatticeTelemetryApiAuthorizer.cs) (line 47).

`public sealed class DenyTelemetryApiAuthorizer : ILatticeTelemetryApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeTelemetryApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Telemetry.Grpc.AllowAllTelemetryApiAuthorizer`

[Source](../../src/lattice.api.telemetry.grpc/Security/ILatticeTelemetryApiAuthorizer.cs) (line 62).

`public sealed class AllowAllTelemetryApiAuthorizer : ILatticeTelemetryApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeTelemetryApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Telemetry.Grpc.LatticeTelemetryApiOperation`

[Source](../../src/lattice.api.telemetry.grpc/Security/LatticeTelemetryApiAuthorizationContext.cs) (line 11).

`public enum LatticeTelemetryApiOperation`

- `GetCatalog = 0`
- `Query = 1`
- `Unknown = 2`

### `Orleans.Lattice.Api.Telemetry.Grpc.LatticeTelemetryApiAuthorizationContext`

[Source](../../src/lattice.api.telemetry.grpc/Security/LatticeTelemetryApiAuthorizationContext.cs) (line 35).

`public readonly struct LatticeTelemetryApiAuthorizationContext`

- `public LatticeTelemetryApiAuthorizationContext(ServerCallContext call, LatticeTelemetryApiOperation operation, string? targetId)`
- `public ServerCallContext Call`
- `public LatticeTelemetryApiOperation Operation`
- `public string? TargetId`
