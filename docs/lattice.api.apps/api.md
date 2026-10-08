# Public API

`Orleans.Lattice.Api.Apps` implements transport-independent contracts declared by `Orleans.Lattice.Api.Abstractions`. The [facade guide](README.md) covers authorization, tenant handling, lifecycle behavior, and bridge limits.

## Registration and options

`LatticeAppsApiServiceCollectionExtensions` exposes these overloads; each requires `AddLatticeApps()` first, is idempotent, and registers the control, role-binding, catalogue, and workspace contracts:

- `ISiloBuilder AddLatticeAppsApi(this ISiloBuilder builder)`
- `IServiceCollection AddLatticeAppsApi(this IServiceCollection services)`

`LatticeAppBridgeApiServiceCollectionExtensions` exposes the bridge overloads below. They also register the control API; invalid rate-limit options fail when the bridge is first resolved:

- `ISiloBuilder AddLatticeAppBridgeApi(this ISiloBuilder builder, Action<LatticeAppBridgeOptions>? configure = null)`
- `IServiceCollection AddLatticeAppBridgeApi(this IServiceCollection services, Action<LatticeAppBridgeOptions>? configure = null)`

| Public options member | Type | Default |
|---|---|---|
| `DefaultRateLimitPermitLimit` | `const int` | 100 |
| `DefaultRateLimitWindow` | `static readonly TimeSpan` | 1 second |
| `RateLimitPermitLimit` | `int` | 100; must be at least 1 |
| `RateLimitWindow` | `TimeSpan` | 1 second; must be positive |

## Facade contracts

These abstraction contracts are implemented by this package. The signatures below include the public optional defaults; all return types are `Task`-based:

| Contract | Public operation signatures |
|---|---|
| `ILatticeAppsControl` | `Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default)`; `Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default)`; `Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default)`; `Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default)`; `Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default)`; `Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default)`; `Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default)`; `Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default)`; `Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)` |
| `ILatticeAppRoleBindings` | `Task<AppRoleBindingsReport> UpdateRoleBindingsAsync(AppRoleBindingsUpdate request, CancellationToken cancellationToken = default)` |
| `ILatticeAppCatalog` | `Task<ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default)`; `Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default)`; `Task<AppDescriptor?> DescribeFromSourceAsync(string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default)`; `Task<AppIconAsset?> GetIconAsync(string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default)`; `Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)` |
| `ILatticeAppWorkspace` | `Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default)`; `Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default)`; `Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default)`; `Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default)` |
| `ILatticeAppBridge` | `Task<AppBridgeValue?> GetAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)`; `Task<AppBridgePage> ScanAsync(AppBridgeTarget target, string prefix, int pageSize, string? continuation = null, CancellationToken cancellationToken = default)`; `Task SetAsync(AppBridgeTarget target, string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)`; `Task<bool> DeleteAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)` |

Administrative control and catalogue calls use `AppInstall`; workspace and bridge operations use role grants and the shared access gate, always in the caller's active tenant.

## Data contracts

The following 47 shared public contract and support types are declared in `Orleans.Lattice.Api.Abstractions` under `Apps` (the five facade interfaces are listed above):

- `ApiAppsTypeAliases`, `AppBridgeException`, `AppBridgeFailure`, `AppBridgePage`, `AppBridgeTarget`, `AppBridgeValue`, `AppCapabilityCeilingDescriptor`, `AppCatalog`, `AppConsentReport`, `AppConsentUpdate`, `AppDescriptor`, `AppExceptionScope`, `AppIconAsset`, `AppIconDescriptor`, `AppInstallRequest`, `AppLifecycleResult`, `AppLifecycleState`, `AppMcpToolDescriptor`, `AppPresentationDescriptor`, `AppProvenanceDescriptor`, `AppReplicationDescriptor`, `AppRoleBindingDescriptor`, `AppRoleBindingsReport`, `AppRoleBindingsUpdate`, `AppRoleDescriptor`, `AppRoleScope`, `AppSchemaDescriptor`, `AppSourceSummary`, `AppSourceSummaryCapabilities`, `AppSourceSummaryKind`, `AppSubscriptionDescriptor`, `AppSummary`, `AppTreeDescriptor`, `AppUiAsset`, `AppUiAssetDescriptor`, `AppUiBridgeGrantDescriptor`, `AppUiDescriptor`, `AppUiScriptDescriptor`, `AvailableAppFilter`, `AvailableAppPage`, `AvailableAppQuery`, `AvailableAppSummary`, `LatticeAppCatalogCapabilities`, `LatticeAppsCapabilities`, `WorkspaceAppDescriptor`, `WorkspaceAppSummary`, and `WorkspaceTreeDescriptor`.

The package itself declares three public types: `LatticeAppsApiServiceCollectionExtensions`, `LatticeAppBridgeApiServiceCollectionExtensions`, and `LatticeAppBridgeOptions`.

## Source map

- [Control registration](../../src/lattice.api.apps/LatticeAppsApiServiceCollectionExtensions.cs)
- [Bridge registration and options](../../src/lattice.api.apps/LatticeAppBridgeApiServiceCollectionExtensions.cs)
- [Control contract](../../src/lattice.api.abstractions/Apps/ILatticeAppsControl.cs)
- [Catalogue contract](../../src/lattice.api.abstractions/Apps/ILatticeAppCatalog.cs)
- [Workspace contract](../../src/lattice.api.abstractions/Apps/ILatticeAppWorkspace.cs)
- [Bridge contract](../../src/lattice.api.abstractions/Apps/ILatticeAppBridge.cs)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Apps.LatticeAppBridgeApiServiceCollectionExtensions`

[Source](../../src/lattice.api.apps/LatticeAppBridgeApiServiceCollectionExtensions.cs) (line 13).

`public static class LatticeAppBridgeApiServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeAppBridgeApi(this ISiloBuilder builder, Action<LatticeAppBridgeOptions>? configure = null)`
- `public static IServiceCollection AddLatticeAppBridgeApi(this IServiceCollection services, Action<LatticeAppBridgeOptions>? configure = null)`

### `Orleans.Lattice.Api.Apps.LatticeAppBridgeOptions`

[Source](../../src/lattice.api.apps/LatticeAppBridgeOptions.cs) (line 13).

`public sealed class LatticeAppBridgeOptions`

- `public const int DefaultRateLimitPermitLimit`
- `public static readonly TimeSpan DefaultRateLimitWindow`
- `public int RateLimitPermitLimit { get; set; }`
- `public TimeSpan RateLimitWindow { get; set; }`

### `Orleans.Lattice.Api.Apps.LatticeAppsApiServiceCollectionExtensions`

[Source](../../src/lattice.api.apps/LatticeAppsApiServiceCollectionExtensions.cs) (line 12).

`public static class LatticeAppsApiServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeAppsApi(this ISiloBuilder builder)`
- `public static IServiceCollection AddLatticeAppsApi(this IServiceCollection services)`

## Shared contract declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Apps.ApiAppsTypeAliases`

[Source](../../src/lattice.api.abstractions/Apps/Model/ApiAppsTypeAliases.cs) (line 4).

`public static class ApiAppsTypeAliases`

- `public const string AliasPrefix`
- `public const string AppInstallRequest`
- `public const string AppRoleBindingDescriptor`
- `public const string AppCapabilityCeilingDescriptor`
- `public const string AppExceptionScope`
- `public const string AppLifecycleResult`
- `public const string AppConsentUpdate`
- `public const string AppConsentReport`
- `public const string AppSummary`
- `public const string AppCatalog`
- `public const string AppDescriptor`
- `public const string AppProvenanceDescriptor`
- `public const string AppTreeDescriptor`
- `public const string AppRoleDescriptor`
- `public const string AppRoleScope`
- `public const string AppSubscriptionDescriptor`
- `public const string AppMcpToolDescriptor`
- `public const string AppReplicationDescriptor`
- `public const string AppSchemaDescriptor`
- `public const string LatticeAppsCapabilities`
- `public const string AppSourceSummary`
- `public const string AppPresentationDescriptor`
- `public const string AppIconDescriptor`
- `public const string AppUiDescriptor`
- `public const string AppUiScriptDescriptor`
- `public const string AppUiAssetDescriptor`
- `public const string AppUiBridgeGrantDescriptor`
- `public const string AppIconAsset`
- `public const string AppUiAsset`
- `public const string AvailableAppQuery`
- `public const string AvailableAppPage`
- `public const string AvailableAppSummary`
- `public const string WorkspaceAppSummary`
- `public const string WorkspaceAppDescriptor`
- `public const string WorkspaceTreeDescriptor`
- `public const string AppBridgeTarget`
- `public const string AppBridgeValue`
- `public const string AppBridgePage`
- `public const string AppBridgeException`
- `public const string LatticeAppCatalogCapabilities`
- `public const string AppRoleBindingsUpdate`
- `public const string AppRoleBindingsReport`

### `Orleans.Lattice.Api.Apps.AppBridgeException`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppBridgeException.cs) (line 15).

`public sealed class AppBridgeException : Exception`

- `public AppBridgeException()`
- `public AppBridgeException(AppBridgeFailure failure)`
- `public AppBridgeException(AppBridgeFailure failure, string message)`
- `public AppBridgeFailure Failure { get; }`
- `public static string DefaultMessage(AppBridgeFailure failure)`

### `Orleans.Lattice.Api.Apps.AppBridgeFailure`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppBridgeFailure.cs) (line 7).

`public enum AppBridgeFailure`

- `Denied = 0`
- `NotFound = 1`
- `Invalid = 2`
- `TooLarge = 3`
- `Conflict = 4`
- `Unavailable = 5`

### `Orleans.Lattice.Api.Apps.AppBridgePage`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppBridgePage.cs) (line 6).

`public sealed record AppBridgePage`

- `public ImmutableArray<AppBridgeValue> Entries { get; init; }`
- `public string? Continuation { get; init; }`

### `Orleans.Lattice.Api.Apps.AppBridgeTarget`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppBridgeTarget.cs) (line 7).

`public sealed record AppBridgeTarget`

- `public required string AppSlug { get; init; }`
- `public long InstallRevision { get; init; }`
- `public required string LogicalTree { get; init; }`

### `Orleans.Lattice.Api.Apps.AppBridgeValue`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppBridgeValue.cs) (line 4).

`public sealed record AppBridgeValue`

- `public required string Key { get; init; }`
- `public ReadOnlyMemory<byte> Value { get; init; }`

### `Orleans.Lattice.Api.Apps.AppCapabilityCeilingDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppCapabilityCeilingDescriptor.cs) (line 10).

`public sealed record AppCapabilityCeilingDescriptor`

- `public LatticeOperation AllowedOperations { get; init; }`
- `public ImmutableArray<AppExceptionScope> ApprovedExceptionScopes { get; init; }`

### `Orleans.Lattice.Api.Apps.AppCatalog`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppCatalog.cs) (line 6).

`public sealed record AppCatalog`

- `public ImmutableArray<AppSummary> Apps { get; init; }`

### `Orleans.Lattice.Api.Apps.AppConsentReport`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppConsentReport.cs) (line 6).

`public sealed record AppConsentReport`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public required AppCapabilityCeilingDescriptor Ceiling { get; init; }`
- `public ImmutableArray<AppUiBridgeGrantDescriptor>? BridgeGrants { get; init; }`

### `Orleans.Lattice.Api.Apps.AppConsentUpdate`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppConsentUpdate.cs) (line 6).

`public sealed record AppConsentUpdate`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public required AppCapabilityCeilingDescriptor Ceiling { get; init; }`
- `public ImmutableArray<AppUiBridgeGrantDescriptor>? BridgeGrants { get; init; }`

### `Orleans.Lattice.Api.Apps.AppDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppDescriptor.cs) (line 10).

`public sealed record AppDescriptor`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public required AppProvenanceDescriptor Provenance { get; init; }`
- `public AppLifecycleState State { get; init; }`
- `public AppCapabilityCeilingDescriptor? Ceiling { get; init; }`
- `public ImmutableArray<AppRoleBindingDescriptor> RoleBindings { get; init; }`
- `public ImmutableArray<AppTreeDescriptor> Trees { get; init; }`
- `public ImmutableArray<AppRoleDescriptor> Roles { get; init; }`
- `public ImmutableArray<AppSubscriptionDescriptor> Subscriptions { get; init; }`
- `public ImmutableArray<AppMcpToolDescriptor> McpTools { get; init; }`
- `public ImmutableArray<AppReplicationDescriptor> Replication { get; init; }`
- `public ImmutableArray<AppSchemaDescriptor> Schema { get; init; }`
- `public AppPresentationDescriptor? Presentation { get; init; }`
- `public AppUiDescriptor? Ui { get; init; }`
- `public string? SourceKey { get; init; }`
- `public string? ManifestDigest { get; init; }`

### `Orleans.Lattice.Api.Apps.AppExceptionScope`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppExceptionScope.cs) (line 15).

`public sealed record AppExceptionScope`

- `public LatticeScopeKind Kind { get; init; }`
- `public string? App { get; init; }`
- `public string? Tree { get; init; }`
- `public string? AdoptedTreeId { get; init; }`
- `public string? KeyOrPrefix { get; init; }`

### `Orleans.Lattice.Api.Apps.AppIconAsset`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppIconAsset.cs) (line 4).

`public sealed record AppIconAsset`

- `public ReadOnlyMemory<byte> Bytes { get; init; }`
- `public required string MediaType { get; init; }`
- `public required string Sha256 { get; init; }`

### `Orleans.Lattice.Api.Apps.AppIconDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppIconDescriptor.cs) (line 5).

`public sealed record AppIconDescriptor`

- `public required string Path { get; init; }`
- `public required string Sha256 { get; init; }`

### `Orleans.Lattice.Api.Apps.AppInstallRequest`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppInstallRequest.cs) (line 6).

`public sealed record AppInstallRequest`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public ImmutableArray<AppRoleBindingDescriptor> RoleBindings { get; init; }`
- `public required AppCapabilityCeilingDescriptor Ceiling { get; init; }`
- `public string? SourceKey { get; init; }`
- `public string? ExpectedManifestDigest { get; init; }`

### `Orleans.Lattice.Api.Apps.AppLifecycleResult`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppLifecycleResult.cs) (line 4).

`public sealed record AppLifecycleResult`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public AppLifecycleState State { get; init; }`
- `public bool Changed { get; init; }`

### `Orleans.Lattice.Api.Apps.AppLifecycleState`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppLifecycleState.cs) (line 4).

`public enum AppLifecycleState`

- `NotInstalled = 0`
- `Installed = 1`
- `Enabled = 2`
- `Disabled = 3`
- `Uninstalled = 4`
- `Failed = 5`

### `Orleans.Lattice.Api.Apps.AppMcpToolDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppMcpToolDescriptor.cs) (line 4).

`public sealed record AppMcpToolDescriptor`

- `public required string Name { get; init; }`
- `public required string Description { get; init; }`
- `public required string Role { get; init; }`

### `Orleans.Lattice.Api.Apps.AppPresentationDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppPresentationDescriptor.cs) (line 13).

`public sealed record AppPresentationDescriptor`

- `public required string DisplayName { get; init; }`
- `public string? Summary { get; init; }`
- `public string? Description { get; init; }`
- `public AppIconDescriptor? Icon { get; init; }`
- `public ImmutableArray<string> Categories { get; init; }`
- `public string? DocumentationUrl { get; init; }`
- `public string? PublisherDisplayName { get; init; }`

### `Orleans.Lattice.Api.Apps.AppProvenanceDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppProvenanceDescriptor.cs) (line 4).

`public sealed record AppProvenanceDescriptor`

- `public required string Source { get; init; }`
- `public required string Publisher { get; init; }`
- `public string? Reference { get; init; }`

### `Orleans.Lattice.Api.Apps.AppReplicationDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppReplicationDescriptor.cs) (line 4).

`public sealed record AppReplicationDescriptor`

- `public required string Tree { get; init; }`
- `public LatticeMergeMode MergeMode { get; init; }`

### `Orleans.Lattice.Api.Apps.AppRoleBindingDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppRoleBindingDescriptor.cs) (line 4).

`public sealed record AppRoleBindingDescriptor`

- `public required string RoleName { get; init; }`
- `public required string GroupId { get; init; }`

### `Orleans.Lattice.Api.Apps.AppRoleBindingsReport`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppRoleBindingsReport.cs) (line 6).

`public sealed record AppRoleBindingsReport`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public ImmutableArray<AppRoleBindingDescriptor> RoleBindings { get; init; }`
- `public AppLifecycleState State { get; init; }`

### `Orleans.Lattice.Api.Apps.AppRoleBindingsUpdate`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppRoleBindingsUpdate.cs) (line 9).

`public sealed record AppRoleBindingsUpdate`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public ImmutableArray<AppRoleBindingDescriptor> RoleBindings { get; init; }`

### `Orleans.Lattice.Api.Apps.AppRoleDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppRoleDescriptor.cs) (line 6).

`public sealed record AppRoleDescriptor`

- `public required string Name { get; init; }`
- `public LatticeOperation Operations { get; init; }`
- `public ImmutableArray<AppRoleScope> Scopes { get; init; }`

### `Orleans.Lattice.Api.Apps.AppRoleScope`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppRoleScope.cs) (line 6).

`public sealed record AppRoleScope`

- `public required string Tree { get; init; }`
- `public string? App { get; init; }`
- `public LatticeScopeKind Kind { get; init; }`
- `public string? KeyOrPrefix { get; init; }`

### `Orleans.Lattice.Api.Apps.AppSchemaDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppSchemaDescriptor.cs) (line 4).

`public sealed record AppSchemaDescriptor`

- `public required string Tree { get; init; }`
- `public required string Family { get; init; }`
- `public int Version { get; init; }`
- `public bool StrictIngest { get; init; }`

### `Orleans.Lattice.Api.Apps.AppSourceSummary`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppSourceSummary.cs) (line 4).

`public sealed record AppSourceSummary`

- `public required string Key { get; init; }`
- `public required string DisplayName { get; init; }`
- `public AppSourceSummaryKind Kind { get; init; }`
- `public AppSourceSummaryCapabilities Capabilities { get; init; }`

### `Orleans.Lattice.Api.Apps.AppSourceSummaryCapabilities`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppSourceSummaryCapabilities.cs) (line 4).

`public enum AppSourceSummaryCapabilities`

- `None = 0`
- `Enumerate = 1`
- `Search = 2`
- `MultipleVersions = 4`
- `RequiresAcquisition = 8`

### `Orleans.Lattice.Api.Apps.AppSourceSummaryKind`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppSourceSummaryKind.cs) (line 4).

`public enum AppSourceSummaryKind`

- `Static = 0`
- `Dynamic = 1`

### `Orleans.Lattice.Api.Apps.AppSubscriptionDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppSubscriptionDescriptor.cs) (line 4).

`public sealed record AppSubscriptionDescriptor`

- `public required string Name { get; init; }`
- `public required string Tree { get; init; }`
- `public string? App { get; init; }`
- `public string? KeyPrefix { get; init; }`

### `Orleans.Lattice.Api.Apps.AppSummary`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppSummary.cs) (line 4).

`public sealed record AppSummary`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public AppLifecycleState State { get; init; }`
- `public required AppProvenanceDescriptor Provenance { get; init; }`

### `Orleans.Lattice.Api.Apps.AppTreeDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppTreeDescriptor.cs) (line 7).

`public sealed record AppTreeDescriptor`

- `public required string Name { get; init; }`
- `public bool Rebuildable { get; init; }`
- `public string? AdoptedTreeId { get; init; }`
- `public int? ShardCount { get; init; }`
- `public int? VirtualShardCount { get; init; }`
- `public int? MaxLeafKeys { get; init; }`
- `public int? MaxInternalChildren { get; init; }`
- `public int? WalPartitions { get; init; }`
- `public TimeSpan? SoftDeleteDuration { get; init; }`
- `public string? OwnershipConflict { get; init; }`

### `Orleans.Lattice.Api.Apps.AppUiAsset`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppUiAsset.cs) (line 4).

`public sealed record AppUiAsset`

- `public required string Path { get; init; }`
- `public ReadOnlyMemory<byte> Bytes { get; init; }`
- `public required string MediaType { get; init; }`
- `public required string Sha256 { get; init; }`

### `Orleans.Lattice.Api.Apps.AppUiAssetDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppUiAssetDescriptor.cs) (line 4).

`public sealed record AppUiAssetDescriptor`

- `public required string Path { get; init; }`
- `public required string MediaType { get; init; }`
- `public required string Sha256 { get; init; }`

### `Orleans.Lattice.Api.Apps.AppUiBridgeGrantDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppUiBridgeGrantDescriptor.cs) (line 13).

`public sealed record AppUiBridgeGrantDescriptor`

- `public required string Operation { get; init; }`
- `public string? Tree { get; init; }`

### `Orleans.Lattice.Api.Apps.AppUiDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppUiDescriptor.cs) (line 9).

`public sealed record AppUiDescriptor`

- `public required string Entry { get; init; }`
- `public ImmutableArray<string> Styles { get; init; }`
- `public ImmutableArray<AppUiScriptDescriptor> Scripts { get; init; }`
- `public ImmutableArray<AppUiAssetDescriptor> Assets { get; init; }`
- `public required string BundleDigest { get; init; }`
- `public ImmutableArray<AppUiBridgeGrantDescriptor> Bridge { get; init; }`
- `public int MinProtocol { get; init; }`

### `Orleans.Lattice.Api.Apps.AppUiScriptDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/AppUiScriptDescriptor.cs) (line 4).

`public sealed record AppUiScriptDescriptor`

- `public required string Path { get; init; }`
- `public bool Module { get; init; }`

### `Orleans.Lattice.Api.Apps.AvailableAppFilter`

[Source](../../src/lattice.api.abstractions/Apps/Model/AvailableAppFilter.cs) (line 4).

`public enum AvailableAppFilter`

- `All = 0`
- `Installed = 1`
- `Available = 2`
- `Updates = 3`

### `Orleans.Lattice.Api.Apps.AvailableAppPage`

[Source](../../src/lattice.api.abstractions/Apps/Model/AvailableAppPage.cs) (line 6).

`public sealed record AvailableAppPage`

- `public ImmutableArray<AvailableAppSummary> Apps { get; init; }`
- `public string? Continuation { get; init; }`

### `Orleans.Lattice.Api.Apps.AvailableAppQuery`

[Source](../../src/lattice.api.abstractions/Apps/Model/AvailableAppQuery.cs) (line 4).

`public sealed record AvailableAppQuery`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public string? SourceKey { get; init; }`
- `public string? Text { get; init; }`
- `public AvailableAppFilter Filter { get; init; }`
- `public int PageSize { get; init; }`
- `public string? Continuation { get; init; }`

### `Orleans.Lattice.Api.Apps.AvailableAppSummary`

[Source](../../src/lattice.api.abstractions/Apps/Model/AvailableAppSummary.cs) (line 6).

`public sealed record AvailableAppSummary`

- `public required string SourceKey { get; init; }`
- `public required string Slug { get; init; }`
- `public required string NewestVersion { get; init; }`
- `public ImmutableArray<string> AvailableVersions { get; init; }`
- `public AppPresentationDescriptor? Presentation { get; init; }`
- `public bool HasUi { get; init; }`
- `public string? InstalledVersion { get; init; }`
- `public AppLifecycleState? InstalledState { get; init; }`

### `Orleans.Lattice.Api.Apps.ILatticeAppBridge`

[Source](../../src/lattice.api.abstractions/Apps/ILatticeAppBridge.cs) (line 25).

`public interface ILatticeAppBridge`

- `Task<AppBridgeValue?> GetAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)`
- `Task<AppBridgePage> ScanAsync( AppBridgeTarget target, string prefix, int pageSize, string? continuation = null, CancellationToken cancellationToken = default)`
- `Task SetAsync( AppBridgeTarget target, string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)`
- `Task<bool> DeleteAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Apps.ILatticeAppCatalog`

[Source](../../src/lattice.api.abstractions/Apps/ILatticeAppCatalog.cs) (line 18).

`public interface ILatticeAppCatalog`

- `Task<ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default)`
- `Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default)`
- `Task<AppDescriptor?> DescribeFromSourceAsync( string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default)`
- `Task<AppIconAsset?> GetIconAsync( string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default)`
- `Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Apps.ILatticeAppRoleBindings`

[Source](../../src/lattice.api.abstractions/Apps/ILatticeAppRoleBindings.cs) (line 15).

`public interface ILatticeAppRoleBindings`

- `Task<AppRoleBindingsReport> UpdateRoleBindingsAsync(AppRoleBindingsUpdate request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Apps.ILatticeAppWorkspace`

[Source](../../src/lattice.api.abstractions/Apps/ILatticeAppWorkspace.cs) (line 19).

`public interface ILatticeAppWorkspace`

- `Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default)`
- `Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default)`
- `Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default)`
- `Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Apps.ILatticeAppsControl`

[Source](../../src/lattice.api.abstractions/Apps/ILatticeAppsControl.cs) (line 18).

`public interface ILatticeAppsControl`

- `Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default)`
- `Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default)`
- `Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default)`
- `Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default)`
- `Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default)`
- `Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default)`
- `Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default)`
- `Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default)`
- `Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Apps.LatticeAppCatalogCapabilities`

[Source](../../src/lattice.api.abstractions/Apps/Model/LatticeAppCatalogCapabilities.cs) (line 4).

`public sealed record LatticeAppCatalogCapabilities`

- `public bool CanListSources { get; init; }`
- `public bool CanListAvailable { get; init; }`
- `public bool CanDescribeFromSource { get; init; }`
- `public bool CanGetIcon { get; init; }`

### `Orleans.Lattice.Api.Apps.LatticeAppsCapabilities`

[Source](../../src/lattice.api.abstractions/Apps/Model/LatticeAppsCapabilities.cs) (line 4).

`public sealed record LatticeAppsCapabilities`

- `public bool CanInstall { get; init; }`
- `public bool CanEnable { get; init; }`
- `public bool CanDisable { get; init; }`
- `public bool CanUninstall { get; init; }`
- `public bool CanList { get; init; }`
- `public bool CanDescribe { get; init; }`
- `public bool CanGetConsent { get; init; }`
- `public bool CanUpdateConsent { get; init; }`

### `Orleans.Lattice.Api.Apps.WorkspaceAppDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/WorkspaceAppDescriptor.cs) (line 14).

`public sealed record WorkspaceAppDescriptor`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public long InstallRevision { get; init; }`
- `public string? SourceKey { get; init; }`
- `public AppLifecycleState State { get; init; }`
- `public AppPresentationDescriptor? Presentation { get; init; }`
- `public ImmutableArray<WorkspaceTreeDescriptor> Trees { get; init; }`
- `public ImmutableArray<AppRoleDescriptor> Roles { get; init; }`
- `public ImmutableArray<AppMcpToolDescriptor> McpTools { get; init; }`
- `public ImmutableArray<AppSubscriptionDescriptor> Subscriptions { get; init; }`
- `public ImmutableArray<AppReplicationDescriptor> Replication { get; init; }`
- `public AppUiDescriptor? Ui { get; init; }`

### `Orleans.Lattice.Api.Apps.WorkspaceAppSummary`

[Source](../../src/lattice.api.abstractions/Apps/Model/WorkspaceAppSummary.cs) (line 6).

`public sealed record WorkspaceAppSummary`

- `public required string Slug { get; init; }`
- `public required string Version { get; init; }`
- `public long InstallRevision { get; init; }`
- `public AppPresentationDescriptor? Presentation { get; init; }`
- `public bool HasUi { get; init; }`
- `public ImmutableArray<string> Roles { get; init; }`

### `Orleans.Lattice.Api.Apps.WorkspaceTreeDescriptor`

[Source](../../src/lattice.api.abstractions/Apps/Model/WorkspaceTreeDescriptor.cs) (line 7).

`public sealed record WorkspaceTreeDescriptor`

- `public required string Name { get; init; }`
- `public bool Rebuildable { get; init; }`
- `public bool Adopted { get; init; }`
- `public int? ShardCount { get; init; }`
- `public int? VirtualShardCount { get; init; }`
- `public int? MaxLeafKeys { get; init; }`
- `public int? MaxInternalChildren { get; init; }`
- `public int? WalPartitions { get; init; }`
- `public TimeSpan? SoftDeleteDuration { get; init; }`
