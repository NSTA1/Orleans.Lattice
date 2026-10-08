# Public API

This page indexes the code-first gRPC surface for app control, catalogue, workspace, and bridge facades. Endpoint paths, authorization, credentials, and error mapping are described in [architecture](architecture.md); the facade contracts are described in the [in-process API page](../lattice.api.apps/api.md).

## Registration

`LatticeAppsApiGrpcServiceCollectionExtensions` exposes these public endpoint-registration methods. The three secondary service registrations reuse the shared options and authorization interceptor; each maps one code-first unary service.

| Public signature |
|---|
| `IServiceCollection AddLatticeAppsApiGrpc(this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)` |
| `IEndpointRouteBuilder MapLatticeAppsApiGrpc(this IEndpointRouteBuilder endpoints)` |
| `IServiceCollection AddLatticeAppCatalogApiGrpc(this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)` |
| `IEndpointRouteBuilder MapLatticeAppCatalogApiGrpc(this IEndpointRouteBuilder endpoints)` |
| `IServiceCollection AddLatticeAppWorkspaceApiGrpc(this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)` |
| `IEndpointRouteBuilder MapLatticeAppWorkspaceApiGrpc(this IEndpointRouteBuilder endpoints)` |
| `IServiceCollection AddLatticeAppBridgeApiGrpc(this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)` |
| `IEndpointRouteBuilder MapLatticeAppBridgeApiGrpc(this IEndpointRouteBuilder endpoints)` |

## Public type inventory

| Area | Public types |
|---|---|
| Registration and credential | `LatticeAppsApiGrpcServiceCollectionExtensions`, `LatticeAppsApiGrpcOptions`, `ILatticeAppsApiCredentialBridge` |
| Clients | `LatticeAppsApiGrpcClient`, `LatticeAppCatalogApiGrpcClient`, `LatticeAppWorkspaceApiGrpcClient`, `LatticeAppBridgeApiGrpcClient` |
| Authorization | `ILatticeAppsApiAuthorizer`, `DenyAppsApiAuthorizer`, `LatticeAppsApiAuthorizationContext`, `LatticeAppsApiOperation`, `ILatticeAppsApiAuthSchemeSource` |
| Control/auth models | `AppsEmptyRequest`, `AppsSlugRequest`, `AppsDescribeRequest`, `AppsDescribeResponse`, `AppsConsentResponse`, `AuthSchemeAdvertisementRequest`, `AuthSchemeAdvertisement`, `AuthSchemeDescriptor` |
| Catalogue/workspace models | `AppsSourceAppRequest`, `AppsSourcesResponse`, `AppsIconResponse`, `AppsWorkspaceListResponse`, `AppsWorkspaceDescribeResponse`, `AppsUiAssetRequest`, `AppsUiAssetResponse` |
| Bridge models | `AppsBridgeKeyRequest`, `AppsBridgeScanRequest`, `AppsBridgeSetRequest`, `AppsBridgeGetResponse`, `AppsBridgeDeleteResponse` |
| Public helper | `GrpcAppsTypeAliases` |

Shared inputs/results such as `AppInstallRequest`, `AppRoleBindingsUpdate`, `AvailableAppQuery`, and `AppBridgeTarget` are defined by `Orleans.Lattice.Api.Abstractions` and used by these clients.

## Unary RPC inventory

| Service | RPC methods |
|---|---|
| `orleans.lattice.api.apps` | `Install`, `Enable`, `Disable`, `Uninstall`, `List`, `Describe`, `GetConsent`, `UpdateConsent`, `GetCapabilities`, `UpdateRoleBindings`, `GetAuthScheme` |
| `orleans.lattice.api.apps.catalog` | `ListSources`, `ListAvailable`, `DescribeFromSource`, `GetIcon`, `GetCapabilities` |
| `orleans.lattice.api.apps.workspace` | `ListMyApps`, `DescribeMyApp`, `GetIcon`, `GetUiAsset` |
| `orleans.lattice.api.apps.bridge` | `Get`, `Scan`, `Set`, `Delete` |

## Authorization and credential extension points

| Public contract/type | Public members |
|---|---|
| `ILatticeAppsApiAuthorizer` | `Task<bool> IsAuthorizedAsync(LatticeAppsApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)` |
| `ILatticeAppsApiCredentialBridge` | `LatticeCredential? Resolve(ServerCallContext context)` |
| `ILatticeAppsApiAuthSchemeSource` | `AuthSchemeAdvertisement GetAdvertisement()` |
| `LatticeAppsApiAuthorizationContext` | Positional public properties `ServerCallContext Call`, `LatticeAppsApiOperation Operation`, and `string? AppSlug`. |

## Clients

`LatticeAppsApiGrpcClient` implements `ILatticeAppsControl` and `ILatticeAppRoleBindings`; its public helpers are `static LatticeAppsApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` and `Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)`. The other public factories are `static LatticeAppCatalogApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`, `static LatticeAppWorkspaceApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`, and `static LatticeAppBridgeApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`. Those clients implement their corresponding abstraction contracts directly, so a caller can use the same contract with an in-process or remote facade.

`LatticeAppsApiGrpcOptions` exposes `RequireAuthorization`, `CredentialHeaderName`, `CredentialScheme`, `ActiveTenantHeaderName`, and the get-only `IList<AuthSchemeDescriptor>` `AdvertisedAuthSchemes`; see the [configuration table](configuration.md).

## Source map

- [Registration](../../src/lattice.api.apps.grpc/LatticeAppsApiGrpcServiceCollectionExtensions.cs)
- [Authorizer contract](../../src/lattice.api.apps.grpc/Security/ILatticeAppsApiAuthorizer.cs)
- [Control client](../../src/lattice.api.apps.grpc/LatticeAppsApiGrpcClient.cs)
- [Options](../../src/lattice.api.apps.grpc/LatticeAppsApiGrpcOptions.cs)
- [Wire model directory](../../src/lattice.api.apps.grpc/Model)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Apps.Grpc.AppsBridgeDeleteResponse`

[Source](../../src/lattice.api.apps.grpc/Model/AppsBridgeDeleteResponse.cs) (line 4).

`public sealed record AppsBridgeDeleteResponse`

- `public bool Deleted { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsBridgeGetResponse`

[Source](../../src/lattice.api.apps.grpc/Model/AppsBridgeGetResponse.cs) (line 4).

`public sealed record AppsBridgeGetResponse`

- `public AppBridgeValue? Value { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsBridgeKeyRequest`

[Source](../../src/lattice.api.apps.grpc/Model/AppsBridgeKeyRequest.cs) (line 4).

`public sealed record AppsBridgeKeyRequest`

- `public required AppBridgeTarget Target { get; init; }`
- `public required string Key { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsBridgeScanRequest`

[Source](../../src/lattice.api.apps.grpc/Model/AppsBridgeScanRequest.cs) (line 4).

`public sealed record AppsBridgeScanRequest`

- `public required AppBridgeTarget Target { get; init; }`
- `public required string Prefix { get; init; }`
- `public int PageSize { get; init; }`
- `public string? Continuation { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsBridgeSetRequest`

[Source](../../src/lattice.api.apps.grpc/Model/AppsBridgeSetRequest.cs) (line 4).

`public sealed record AppsBridgeSetRequest`

- `public required AppBridgeTarget Target { get; init; }`
- `public required string Key { get; init; }`
- `public ReadOnlyMemory<byte> Value { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsConsentResponse`

[Source](../../src/lattice.api.apps.grpc/Model/AppsConsentResponse.cs) (line 4).

`public sealed record AppsConsentResponse`

- `public AppConsentReport? Consent { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsDescribeRequest`

[Source](../../src/lattice.api.apps.grpc/Model/AppsDescribeRequest.cs) (line 4).

`public sealed record AppsDescribeRequest`

- `public required string Slug { get; init; }`
- `public string? Version { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsDescribeResponse`

[Source](../../src/lattice.api.apps.grpc/Model/AppsDescribeResponse.cs) (line 4).

`public sealed record AppsDescribeResponse`

- `public AppDescriptor? Descriptor { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsEmptyRequest`

[Source](../../src/lattice.api.apps.grpc/Model/AppsEmptyRequest.cs) (line 4).

`public sealed record AppsEmptyRequest`


### `Orleans.Lattice.Api.Apps.Grpc.AppsIconResponse`

[Source](../../src/lattice.api.apps.grpc/Model/AppsIconResponse.cs) (line 4).

`public sealed record AppsIconResponse`

- `public AppIconAsset? Icon { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsSlugRequest`

[Source](../../src/lattice.api.apps.grpc/Model/AppsSlugRequest.cs) (line 4).

`public sealed record AppsSlugRequest`

- `public required string Slug { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsSourceAppRequest`

[Source](../../src/lattice.api.apps.grpc/Model/AppsSourceAppRequest.cs) (line 4).

`public sealed record AppsSourceAppRequest`

- `public required string SourceKey { get; init; }`
- `public required string Slug { get; init; }`
- `public string? Version { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsSourcesResponse`

[Source](../../src/lattice.api.apps.grpc/Model/AppsSourcesResponse.cs) (line 6).

`public sealed record AppsSourcesResponse`

- `public ImmutableArray<AppSourceSummary> Sources { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsUiAssetRequest`

[Source](../../src/lattice.api.apps.grpc/Model/AppsUiAssetRequest.cs) (line 4).

`public sealed record AppsUiAssetRequest`

- `public required string Slug { get; init; }`
- `public required string Path { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsUiAssetResponse`

[Source](../../src/lattice.api.apps.grpc/Model/AppsUiAssetResponse.cs) (line 4).

`public sealed record AppsUiAssetResponse`

- `public AppUiAsset? Asset { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsWorkspaceDescribeResponse`

[Source](../../src/lattice.api.apps.grpc/Model/AppsWorkspaceDescribeResponse.cs) (line 4).

`public sealed record AppsWorkspaceDescribeResponse`

- `public WorkspaceAppDescriptor? Descriptor { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AppsWorkspaceListResponse`

[Source](../../src/lattice.api.apps.grpc/Model/AppsWorkspaceListResponse.cs) (line 6).

`public sealed record AppsWorkspaceListResponse`

- `public ImmutableArray<WorkspaceAppSummary> Apps { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AuthSchemeAdvertisement`

[Source](../../src/lattice.api.apps.grpc/Model/AuthSchemeAdvertisement.cs) (line 6).

`public sealed record AuthSchemeAdvertisement`

- `public ImmutableArray<AuthSchemeDescriptor> Schemes { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.AuthSchemeAdvertisementRequest`

[Source](../../src/lattice.api.apps.grpc/Model/AuthSchemeAdvertisementRequest.cs) (line 4).

`public sealed record AuthSchemeAdvertisementRequest`


### `Orleans.Lattice.Api.Apps.Grpc.AuthSchemeDescriptor`

[Source](../../src/lattice.api.apps.grpc/Model/AuthSchemeDescriptor.cs) (line 6).

`public sealed record AuthSchemeDescriptor`

- `public required string SchemeId { get; init; }`
- `public string DisplayName { get; init; }`
- `public ImmutableDictionary<string, string> Parameters { get; init; }`

### `Orleans.Lattice.Api.Apps.Grpc.DenyAppsApiAuthorizer`

[Source](../../src/lattice.api.apps.grpc/Security/DenyAppsApiAuthorizer.cs) (line 4).

`public sealed class DenyAppsApiAuthorizer : ILatticeAppsApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeAppsApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Apps.Grpc.GrpcAppsTypeAliases`

[Source](../../src/lattice.api.apps.grpc/Model/GrpcAppsTypeAliases.cs) (line 4).

`public static class GrpcAppsTypeAliases`

- `public const string AliasPrefix`
- `public const string AppsEmptyRequest`
- `public const string AppsSlugRequest`
- `public const string AppsDescribeRequest`
- `public const string AppsDescribeResponse`
- `public const string AppsConsentResponse`
- `public const string AuthSchemeAdvertisementRequest`
- `public const string AuthSchemeDescriptor`
- `public const string AuthSchemeAdvertisement`
- `public const string AppsSourcesResponse`
- `public const string AppsSourceAppRequest`
- `public const string AppsIconResponse`
- `public const string AppsWorkspaceListResponse`
- `public const string AppsWorkspaceDescribeResponse`
- `public const string AppsUiAssetRequest`
- `public const string AppsUiAssetResponse`
- `public const string AppsBridgeKeyRequest`
- `public const string AppsBridgeGetResponse`
- `public const string AppsBridgeScanRequest`
- `public const string AppsBridgeSetRequest`
- `public const string AppsBridgeDeleteResponse`

### `Orleans.Lattice.Api.Apps.Grpc.ILatticeAppsApiAuthSchemeSource`

[Source](../../src/lattice.api.apps.grpc/Security/ILatticeAppsApiAuthSchemeSource.cs) (line 4).

`public interface ILatticeAppsApiAuthSchemeSource`

- `AuthSchemeAdvertisement GetAdvertisement()`

### `Orleans.Lattice.Api.Apps.Grpc.ILatticeAppsApiAuthorizer`

[Source](../../src/lattice.api.apps.grpc/Security/ILatticeAppsApiAuthorizer.cs) (line 4).

`public interface ILatticeAppsApiAuthorizer`

- `Task<bool> IsAuthorizedAsync(LatticeAppsApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Apps.Grpc.ILatticeAppsApiCredentialBridge`

[Source](../../src/lattice.api.apps.grpc/ILatticeAppsApiCredentialBridge.cs) (line 6).

`public interface ILatticeAppsApiCredentialBridge`

- `LatticeCredential? Resolve(ServerCallContext context)`

### `Orleans.Lattice.Api.Apps.Grpc.LatticeAppBridgeApiGrpcClient`

[Source](../../src/lattice.api.apps.grpc/LatticeAppBridgeApiGrpcClient.cs) (line 11).

`public sealed class LatticeAppBridgeApiGrpcClient : ILatticeAppBridge`

- `public static LatticeAppBridgeApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public async Task<AppBridgeValue?> GetAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)`
- `public Task<AppBridgePage> ScanAsync( AppBridgeTarget target, string prefix, int pageSize, string? continuation = null, CancellationToken cancellationToken = default)`
- `public async Task SetAsync( AppBridgeTarget target, string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)`
- `public async Task<bool> DeleteAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Apps.Grpc.LatticeAppCatalogApiGrpcClient`

[Source](../../src/lattice.api.apps.grpc/LatticeAppCatalogApiGrpcClient.cs) (line 10).

`public sealed class LatticeAppCatalogApiGrpcClient : ILatticeAppCatalog`

- `public static LatticeAppCatalogApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public async Task<ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default)`
- `public Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default)`
- `public async Task<AppDescriptor?> DescribeFromSourceAsync( string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default)`
- `public async Task<AppIconAsset?> GetIconAsync( string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default)`
- `public Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Apps.Grpc.LatticeAppWorkspaceApiGrpcClient`

[Source](../../src/lattice.api.apps.grpc/LatticeAppWorkspaceApiGrpcClient.cs) (line 10).

`public sealed class LatticeAppWorkspaceApiGrpcClient : ILatticeAppWorkspace`

- `public static LatticeAppWorkspaceApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public async Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default)`
- `public async Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default)`
- `public async Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default)`
- `public async Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Apps.Grpc.LatticeAppsApiAuthorizationContext`

[Source](../../src/lattice.api.apps.grpc/Security/LatticeAppsApiAuthorizationContext.cs) (line 9).

`public readonly record struct LatticeAppsApiAuthorizationContext( ServerCallContext Call, LatticeAppsApiOperation Operation, string? AppSlug)`

- `Primary constructor / positional members: ( ServerCallContext Call, LatticeAppsApiOperation Operation, string? AppSlug)`

### `Orleans.Lattice.Api.Apps.Grpc.LatticeAppsApiGrpcClient`

[Source](../../src/lattice.api.apps.grpc/LatticeAppsApiGrpcClient.cs) (line 9).

`public sealed class LatticeAppsApiGrpcClient : ILatticeAppsControl, ILatticeAppRoleBindings`

- `public static LatticeAppsApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default)`
- `public Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default)`
- `public Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default)`
- `public Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default)`
- `public Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default)`
- `public async Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default)`
- `public async Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default)`
- `public Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default)`
- `public Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)`
- `public Task<AppRoleBindingsReport> UpdateRoleBindingsAsync(AppRoleBindingsUpdate request, CancellationToken cancellationToken = default)`
- `public async Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Apps.Grpc.LatticeAppsApiGrpcOptions`

[Source](../../src/lattice.api.apps.grpc/LatticeAppsApiGrpcOptions.cs) (line 4).

`public sealed class LatticeAppsApiGrpcOptions`

- `public bool RequireAuthorization { get; set; }`
- `public string CredentialHeaderName { get; set; }`
- `public string CredentialScheme { get; set; }`
- `public string ActiveTenantHeaderName { get; set; }`
- `public IList<AuthSchemeDescriptor> AdvertisedAuthSchemes { get; }`

### `Orleans.Lattice.Api.Apps.Grpc.LatticeAppsApiGrpcServiceCollectionExtensions`

[Source](../../src/lattice.api.apps.grpc/LatticeAppsApiGrpcServiceCollectionExtensions.Bridge.cs) (line 9).

`public static partial class LatticeAppsApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeAppBridgeApiGrpc( this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeAppBridgeApiGrpc(this IEndpointRouteBuilder endpoints)`

[Source](../../src/lattice.api.apps.grpc/LatticeAppsApiGrpcServiceCollectionExtensions.Catalog.cs) (line 9).

`public static partial class LatticeAppsApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeAppCatalogApiGrpc( this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeAppCatalogApiGrpc(this IEndpointRouteBuilder endpoints)`

[Source](../../src/lattice.api.apps.grpc/LatticeAppsApiGrpcServiceCollectionExtensions.Workspace.cs) (line 9).

`public static partial class LatticeAppsApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeAppWorkspaceApiGrpc( this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeAppWorkspaceApiGrpc(this IEndpointRouteBuilder endpoints)`

[Source](../../src/lattice.api.apps.grpc/LatticeAppsApiGrpcServiceCollectionExtensions.cs) (line 10).

`public static partial class LatticeAppsApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeAppsApiGrpc( this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeAppsApiGrpc(this IEndpointRouteBuilder endpoints)`

### `Orleans.Lattice.Api.Apps.Grpc.LatticeAppsApiOperation`

[Source](../../src/lattice.api.apps.grpc/Security/LatticeAppsApiOperation.cs) (line 4).

`public enum LatticeAppsApiOperation`

- `Unknown = 0`
- `Install = 1`
- `Enable = 2`
- `Disable = 3`
- `Uninstall = 4`
- `List = 5`
- `Describe = 6`
- `GetConsent = 7`
- `UpdateConsent = 8`
- `GetCapabilities = 9`
- `ListSources = 10`
- `ListAvailable = 11`
- `DescribeFromSource = 12`
- `GetSourceIcon = 13`
- `GetCatalogCapabilities = 14`
- `ListMyApps = 15`
- `DescribeMyApp = 16`
- `GetMyAppIcon = 17`
- `GetUiAsset = 18`
- `BridgeGet = 19`
- `BridgeScan = 20`
- `BridgeSet = 21`
- `BridgeDelete = 22`
- `UpdateRoleBindings = 23`
