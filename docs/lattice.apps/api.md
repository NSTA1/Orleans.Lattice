# Public API

This page indexes the public surface declared by `Orleans.Lattice.Apps`. The engine provides the registry, source, activation, ownership, role-compilation, and subscription seams. Transport contracts live in `Orleans.Lattice.Api.Abstractions`; see the [control facade API](../lattice.api.apps/api.md).

## Registration

`LatticeAppsServiceCollectionExtensions` declares these public extension overloads. `AddLatticeApps` follows `AddLattice`; `AddLatticeApp` makes the manifest installable but does not install or enable it.

| Public signature |
|---|
| `ISiloBuilder AddLatticeApps(this ISiloBuilder builder, Action<LatticeAppsOptions>? configure = null)` |
| `IServiceCollection AddLatticeApps(this IServiceCollection services, Action<LatticeAppsOptions>? configure = null)` |
| `ISiloBuilder AddLatticeApp(this ISiloBuilder builder, string slug, Assembly assembly, string manifestResourceName)` |
| `IServiceCollection AddLatticeApp(this IServiceCollection services, string slug, Assembly assembly, string manifestResourceName)` |
| `ISiloBuilder AddLatticeApp(this ISiloBuilder builder, string slug, Assembly assembly, string manifestResourceName, string assetResourcePrefix)` |
| `IServiceCollection AddLatticeApp(this IServiceCollection services, string slug, Assembly assembly, string manifestResourceName, string assetResourcePrefix)` |
| `ISiloBuilder AddLatticeAppSource<TSource>(this ISiloBuilder builder) where TSource : class, IAppCatalogSource` |
| `IServiceCollection AddLatticeAppSource<TSource>(this IServiceCollection services) where TSource : class, IAppCatalogSource` |
| `ISiloBuilder AddLatticeAppSource(this ISiloBuilder builder, IAppCatalogSource instance)` |
| `IServiceCollection AddLatticeAppSource(this IServiceCollection services, IAppCatalogSource instance)` |
| `IServiceCollection AddLatticeAppSubscriptionHandler<THandler>(this IServiceCollection services, AppSlug app, string subscriptionName) where THandler : class, IAppChangeFeedHandler` |
| `IServiceCollection AddLatticeAppSubscriptionHandler(this IServiceCollection services, AppSlug app, string subscriptionName, Func<IServiceProvider, IAppChangeFeedHandler> factory)` |

The public registration/configuration members on the in-image option and registration types are:

| Public type | Public members |
|---|---|
| `InImageAppSourceOptions` | `IList<InImageAppRegistration> Registrations { get; }`; `InImageAppSourceOptions Register(AppSlug slug, Assembly assembly, string manifestResourceName)` |
| `InImageAppRegistration` | `InImageAppRegistration(AppSlug slug, Assembly assembly, string manifestResourceName)`; `AppSlug Slug { get; }`; `Assembly Assembly { get; }`; `string ManifestResourceName { get; }`; `string Publisher { get; init; }`; `string AssetResourcePrefix { get; init; }` |

## Public type inventory

Each identifier below is a public top-level type declared by this package. The interfaces and operational methods are listed in the following sections; manifest/result records are grouped by function and described in the [engine guide](README.md).

| Area | Public types |
|---|---|
| Registration/options | `LatticeAppsServiceCollectionExtensions`, `LatticeAppsOptions` |
| Activation | `AppActivationFailure`, `AppActivationOperation`, `AppActivationOutcome`, `AppActivationStatus`, `IAppActivationPipeline` |
| Role compilation | `AppCeilingExcess`, `AppCeilingExcessKind`, `AppRoleCompiler`, `AppRuleCompilation`, `AppRuleSetDiff` |
| Consent/bindings | `AppCapabilityCeiling`, `AppRoleBinding` |
| Manifest identity/parsing | `AppIconReference`, `AppIdentity`, `AppManifest`, `AppManifestError`, `AppManifestParser`, `AppManifestResources`, `AppManifestResult`, `AppManifestValidator`, `AppSlug`, `AppVersion` |
| Manifest declarations | `AppMcpToolDeclaration`, `AppPresentation`, `AppProvenance`, `AppReplicationDeclaration`, `AppRoleDeclaration`, `AppSchemaDeclaration`, `AppScopeTemplate`, `AppSubscriptionDeclaration`, `AppTreeDeclaration`, `AppUiAsset`, `AppUiBridgeDeclaration`, `AppUiBridgeGrant`, `AppUiBridgeOperations`, `AppUiBridgeRequest`, `AppUiBundle`, `AppUiDeclaration`, `AppUiProtocol`, `AppUiScript` |
| Ownership | `AppTreeOwnerSnapshot`, `AppTreeOwnershipConflict`, `AppTreeOwnershipConflictReason` |
| Registry | `AppIsolationContext`, `AppRegistryInstallRequest`, `AppRegistryLifecycleState`, `AppRegistryRecord`, `AppRegistryTransitionError`, `AppRegistryTransitionResult`, `CompiledAppRegistrySnapshot`, `IAppRegistry`, `IAppRegistryProjection` |
| Sources | `AppActivationResult`, `AppAssetResult`, `AppAssetStatus`, `AppSourceCapabilities`, `AppSourceDescriptor`, `AppSourceEntry`, `AppSourceKind`, `AppSourcePage`, `AppSourceQuery`, `AppSourceResult`, `AppSourceSet`, `AppSourceStatus`, `IAppActivationHandle`, `IAppCatalogSource`, `IAppSource`, `InImageAppRegistration`, `InImageAppSource`, `InImageAppSourceOptions` |
| Subscriptions | `AppSubscriptionCompilation`, `AppSubscriptionCompiler`, `AppSubscriptionContext`, `AppSubscriptionDenial`, `IAppChangeFeedHandler` |

## Operational contracts

`IAppActivationPipeline` exposes `EnableAsync`, `DisableAsync`, `UninstallAsync`, `ReconcileAsync`, and `GetStatusAsync`; each is tenant- and slug-scoped and accepts a cancellation token. Mutations return `AppActivationOutcome`; the status read returns nullable `AppActivationStatus`.

`IAppRegistry` exposes install/upgrade transitions, enable/disable/uninstall transitions, tenant/global enumeration, and a read-only tree-ownership conflict query. `IAppRegistryProjection` exposes the current `CompiledAppRegistrySnapshot` and warm-up behavior for readers. Prefer `ILatticeAppsControl` for caller-facing administration because it resolves the tenant and authorizes before registry access.

`AppManifestParser.Parse(string?)` and `Parse(Stream)` return an `AppManifestResult`; `AppManifestValidator.Validate(AppManifest?, AppManifest? previous = null)` validates a manifest and optional previous version. `IAppSource` resolves a slug/version; `IAppCatalogSource` additionally exposes a descriptor, paged listing, and digest-checked asset opening. `IAppChangeFeedHandler.HandleAsync(AppSubscriptionContext, LatticeMutation, CancellationToken)` receives subscribed mutations.

## Source map

- [Registration](../../src/lattice.apps/LatticeAppsServiceCollectionExtensions.cs)
- [Manifest root](../../src/lattice.apps/Manifest/AppManifest.cs)
- [Registry contract](../../src/lattice.apps/Registry/IAppRegistry.cs)
- [Activation contract](../../src/lattice.apps/Activation/IAppActivationPipeline.cs)
- [Source contract](../../src/lattice.apps/Sources/IAppSource.cs)
- [Subscription handler contract](../../src/lattice.apps/Subscriptions/IAppChangeFeedHandler.cs)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Apps.AppActivationFailure`

[Source](../../src/lattice.apps/Activation/AppActivationFailure.cs) (line 7).

`public enum AppActivationFailure`

- `None = 0`
- `NotInstalled = 1`
- `InvalidTransition = 2`
- `CeilingNotPinned = 3`
- `MembershipNotRegistered = 4`
- `AuthorizationNotRegistered = 5`
- `SourceUnavailable = 6`
- `VersionMismatch = 7`
- `InvalidManifest = 8`
- `CeilingExceeded = 9`
- `UnknownRoleBinding = 10`
- `TreeProvisioningFailed = 11`
- `RulePersistenceFailed = 12`
- `RegistryConflict = 13`
- `Faulted = 14`
- `ReplicationModeChangeRejected = 15`
- `ReplicationPreconditionFailed = 16`
- `ReplicationEnrolmentFailed = 17`
- `TreeOwnershipConflict = 18`
- `BridgeConsentRequired = 19`
- `AppRoleBindingTenantMismatch = 20`

### `Orleans.Lattice.Apps.AppActivationOperation`

[Source](../../src/lattice.apps/Activation/AppActivationOperation.cs) (line 6).

`public enum AppActivationOperation`

- `Enable = 0`
- `Disable = 1`
- `Uninstall = 2`
- `Reconcile = 3`

### `Orleans.Lattice.Apps.AppActivationOutcome`

[Source](../../src/lattice.apps/Activation/AppActivationOutcome.cs) (line 9).

`public sealed record AppActivationOutcome`

- `public required TenantId Tenant { get; init; }`
- `public required AppSlug Slug { get; init; }`
- `public required AppActivationOperation Operation { get; init; }`
- `public AppActivationFailure Failure { get; init; }`
- `public AppVersion? Version { get; init; }`
- `public AppRegistryLifecycleState? State { get; init; }`
- `public bool Changed { get; init; }`
- `public IReadOnlyList<AppManifestError> Diagnostics { get; init; }`
- `public DateTimeOffset CompletedAtUtc { get; init; }`
- `public bool Succeeded`

### `Orleans.Lattice.Apps.AppActivationResult`

[Source](../../src/lattice.apps/Sources/AppActivationResult.cs) (line 9).

`public sealed class AppActivationResult`

- `public bool IsActivated`
- `public Assembly? Assembly { get; }`
- `public IReadOnlyList<AppManifestError> Errors { get; }`
- `public static AppActivationResult Activated(Assembly assembly)`
- `public static AppActivationResult Failed(IReadOnlyList<AppManifestError> errors)`

### `Orleans.Lattice.Apps.AppActivationStatus`

[Source](../../src/lattice.apps/Activation/AppActivationStatus.cs) (line 7).

`public sealed record AppActivationStatus`

- `public required TenantId Tenant { get; init; }`
- `public required AppSlug Slug { get; init; }`
- `public required AppActivationOutcome LastOutcome { get; init; }`
- `public AppManifest? AppliedManifest { get; init; }`
- `public IReadOnlyDictionary<string, bool> ReplicationTrees { get; init; }`

### `Orleans.Lattice.Apps.AppCapabilityCeiling`

[Source](../../src/lattice.apps/Install/AppCapabilityCeiling.cs) (line 11).

`public sealed record AppCapabilityCeiling`

- `public LatticeOperation AllowedOperations { get; init; }`
- `public IReadOnlyList<LatticeScope> ApprovedExceptionScopes { get; init; }`
- `public static AppCapabilityCeiling Structural(LatticeOperation allowed)`

### `Orleans.Lattice.Apps.AppCeilingExcess`

[Source](../../src/lattice.apps/Compiler/AppCeilingExcess.cs) (line 20).

`public sealed record AppCeilingExcess( string RoleName, AppCeilingExcessKind Kind, LatticeOperation Operations, LatticeScope? Scope)`

- `Primary constructor / positional members: ( string RoleName, AppCeilingExcessKind Kind, LatticeOperation Operations, LatticeScope? Scope)`

### `Orleans.Lattice.Apps.AppCeilingExcessKind`

[Source](../../src/lattice.apps/Compiler/AppCeilingExcessKind.cs) (line 4).

`public enum AppCeilingExcessKind`

- `Operations = 0`
- `Scope = 1`

### `Orleans.Lattice.Apps.AppIconReference`

[Source](../../src/lattice.apps/Manifest/AppIconReference.cs) (line 7).

`public sealed record AppIconReference`

- `public required string Path { get; init; }`
- `public required string Digest { get; init; }`

### `Orleans.Lattice.Apps.AppIdentity`

[Source](../../src/lattice.apps/Manifest/AppIdentity.cs) (line 4).

`public sealed record AppIdentity`

- `public required AppSlug Slug { get; init; }`
- `public required AppVersion Version { get; init; }`
- `public AppProvenance Provenance { get; init; }`

### `Orleans.Lattice.Apps.AppIsolationContext`

[Source](../../src/lattice.apps/Registry/AppIsolationContext.cs) (line 10).

`public sealed record AppIsolationContext`

- `public required TenantId Tenant { get; init; }`
- `public required string ClusterId { get; init; }`

### `Orleans.Lattice.Apps.AppManifest`

[Source](../../src/lattice.apps/Manifest/AppManifest.cs) (line 4).

`public sealed record AppManifest`

- `public required AppIdentity Identity { get; init; }`
- `public required AppTreeDeclaration[] Trees { get; init; }`
- `public required AppRoleDeclaration[] Roles { get; init; }`
- `public AppReplicationDeclaration[]? Replication { get; init; }`
- `public AppSchemaDeclaration[]? Schema { get; init; }`
- `public required AppSubscriptionDeclaration[] Subscriptions { get; init; }`
- `public required AppMcpToolDeclaration[] McpTools { get; init; }`
- `public AppPresentation? Presentation { get; init; }`
- `public AppUiDeclaration? Ui { get; init; }`

### `Orleans.Lattice.Apps.AppManifestError`

[Source](../../src/lattice.apps/Manifest/AppManifestError.cs) (line 7).

`public sealed record AppManifestError( string Code, string Path, string Message)`

- `Primary constructor / positional members: ( [property: Id(0)] string Code, [property: Id(1)] string Path, [property: Id(2)] string Message)`

### `Orleans.Lattice.Apps.AppManifestParser`

[Source](../../src/lattice.apps/Manifest/AppManifestParser.cs) (line 8).

`public static class AppManifestParser`

- `public static AppManifestResult Parse(string? json)`
- `public static AppManifestResult Parse(Stream stream)`

### `Orleans.Lattice.Apps.AppManifestResources`

[Source](../../src/lattice.apps/Manifest/AppManifestResources.cs) (line 6).

`public static class AppManifestResources`

- `public static AppManifestResult Load(Assembly assembly, string resourceName)`
- `public static string GetJsonSchema()`

### `Orleans.Lattice.Apps.AppManifestResult`

[Source](../../src/lattice.apps/Manifest/AppManifestResult.cs) (line 4).

`public sealed class AppManifestResult`

- `public bool IsValid`
- `public AppManifest? Manifest { get; }`
- `public IReadOnlyList<AppManifestError> Errors { get; }`

### `Orleans.Lattice.Apps.AppManifestValidator`

[Source](../../src/lattice.apps/Manifest/AppManifestValidator.Ui.cs) (line 6).

`public static partial class AppManifestValidator`

- `public static IReadOnlyList<AppManifestError> ValidateUiEntryFragment(ReadOnlySpan<byte> utf8Fragment)`

[Source](../../src/lattice.apps/Manifest/AppManifestValidator.cs) (line 6).

`public static partial class AppManifestValidator`

- `public static AppManifestResult Validate(AppManifest? manifest, AppManifest? previous = null)`

### `Orleans.Lattice.Apps.AppMcpToolDeclaration`

[Source](../../src/lattice.apps/Manifest/AppMcpToolDeclaration.cs) (line 4).

`public sealed record AppMcpToolDeclaration`

- `public required string Name { get; init; }`
- `public required string Description { get; init; }`
- `public required string Role { get; init; }`

### `Orleans.Lattice.Apps.AppPresentation`

[Source](../../src/lattice.apps/Manifest/AppPresentation.cs) (line 8).

`public sealed record AppPresentation`

- `public required string DisplayName { get; init; }`
- `public string? Summary { get; init; }`
- `public string? Description { get; init; }`
- `public AppIconReference? Icon { get; init; }`
- `public string[]? Categories { get; init; }`
- `public string? DocumentationUrl { get; init; }`
- `public string? PublisherDisplayName { get; init; }`

### `Orleans.Lattice.Apps.AppProvenance`

[Source](../../src/lattice.apps/Manifest/AppProvenance.cs) (line 4).

`public sealed record AppProvenance`

- `public string Source { get; init; }`
- `public string Publisher { get; init; }`
- `public string? Reference { get; init; }`

### `Orleans.Lattice.Apps.AppRegistryInstallRequest`

[Source](../../src/lattice.apps/Registry/AppRegistryInstallRequest.cs) (line 9).

`public sealed record AppRegistryInstallRequest`

- `public TenantId Tenant { get; init; }`
- `public required AppIdentity Identity { get; init; }`
- `public required AppCapabilityCeiling Ceiling { get; init; }`
- `public IReadOnlyList<AppRoleBinding> RoleBindings { get; init; }`
- `public AppVersion? ExpectedVersion { get; init; }`
- `public AppUiBridgeRequest? BridgeConsent { get; init; }`
- `public long? ExpectedRevision { get; init; }`

### `Orleans.Lattice.Apps.AppRegistryLifecycleState`

[Source](../../src/lattice.apps/Registry/AppRegistryLifecycleState.cs) (line 11).

`public enum AppRegistryLifecycleState`

- `Installed = 1`
- `Enabled = 2`
- `Disabled = 3`
- `Uninstalled = 4`

### `Orleans.Lattice.Apps.AppRegistryRecord`

[Source](../../src/lattice.apps/Registry/AppRegistryRecord.cs) (line 19).

`public sealed record AppRegistryRecord`

- `public required AppIsolationContext Isolation { get; init; }`
- `public required AppSlug Slug { get; init; }`
- `public required AppVersion Version { get; init; }`
- `public required AppProvenance Provenance { get; init; }`
- `public required AppCapabilityCeiling Ceiling { get; init; }`
- `public required AppVersion CeilingVersion { get; init; }`
- `public IReadOnlyList<AppRoleBinding> RoleBindings { get; init; }`
- `public required AppRegistryLifecycleState State { get; init; }`
- `public long Revision { get; init; }`
- `public DateTimeOffset InstalledAtUtc { get; init; }`
- `public DateTimeOffset StateChangedAtUtc { get; init; }`
- `public DateTimeOffset ConsentedAtUtc { get; init; }`
- `public string? ConsentedBy { get; init; }`
- `public AppUiBridgeRequest? ConsentedBridge { get; init; }`
- `public TenantId Tenant`
- `public bool IsCeilingPinnedToVersion`

### `Orleans.Lattice.Apps.AppRegistryTransitionError`

[Source](../../src/lattice.apps/Registry/AppRegistryTransitionError.cs) (line 4).

`public enum AppRegistryTransitionError`

- `None = 0`
- `NotInstalled = 1`
- `AlreadyInstalled = 2`
- `InvalidTransition = 3`
- `CeilingNotPinned = 4`
- `ConcurrencyConflict = 5`
- `TreeOwnershipConflict = 6`

### `Orleans.Lattice.Apps.AppRegistryTransitionResult`

[Source](../../src/lattice.apps/Registry/AppRegistryTransitionResult.cs) (line 8).

`public sealed record AppRegistryTransitionResult`

- `public AppRegistryRecord? Record { get; init; }`
- `public AppRegistryTransitionError Error { get; init; }`
- `public bool Changed { get; init; }`
- `public string? Message { get; init; }`
- `public bool Succeeded`

### `Orleans.Lattice.Apps.AppReplicationDeclaration`

[Source](../../src/lattice.apps/Manifest/AppReplicationDeclaration.cs) (line 4).

`public sealed record AppReplicationDeclaration`

- `public required string Tree { get; init; }`
- `public required LatticeMergeMode MergeMode { get; init; }`

### `Orleans.Lattice.Apps.AppRoleBinding`

[Source](../../src/lattice.apps/Install/AppRoleBinding.cs) (line 7).

`public sealed record AppRoleBinding`

- `public required string RoleName { get; init; }`
- `public required string GroupId { get; init; }`
- `public static AppRoleBinding Create(string roleName, string groupId)`

### `Orleans.Lattice.Apps.AppRoleCompiler`

[Source](../../src/lattice.apps/Compiler/AppRoleCompiler.cs) (line 77).

`public static class AppRoleCompiler`

- `public static string GetOwnedRuleIdPrefix(AppSlug slug)`
- `public static AppRuleCompilation Compile( AppManifest manifest, TenantId tenant, IReadOnlyList<AppRoleBinding> bindings, AppCapabilityCeiling ceiling, AppTreeOwnerSnapshot? owners = null)`
- `public static AppRuleSetDiff ComputeDiff( AppSlug slug, IReadOnlyList<LatticeAuthorizationRule> compiled, IEnumerable<LatticeAuthorizationRule> stored)`

### `Orleans.Lattice.Apps.AppRoleDeclaration`

[Source](../../src/lattice.apps/Manifest/AppRoleDeclaration.cs) (line 4).

`public sealed record AppRoleDeclaration`

- `public required string Name { get; init; }`
- `public required LatticeOperation Operations { get; init; }`
- `public required AppScopeTemplate[] Scopes { get; init; }`

### `Orleans.Lattice.Apps.AppRuleCompilation`

[Source](../../src/lattice.apps/Compiler/AppRuleCompilation.cs) (line 11).

`public sealed class AppRuleCompilation`

- `public bool Succeeded`
- `public IReadOnlyList<LatticeAuthorizationRule> Rules { get; }`
- `public IReadOnlyList<AppCeilingExcess> Excesses { get; }`
- `public IReadOnlyList<AppRoleBinding> UnknownRoleBindings { get; }`
- `public IReadOnlyList<AppRoleBinding> TenantMismatchBindings { get; }`
- `public IReadOnlyList<string> UnboundRoles { get; }`

### `Orleans.Lattice.Apps.AppRuleSetDiff`

[Source](../../src/lattice.apps/Compiler/AppRuleSetDiff.cs) (line 10).

`public sealed class AppRuleSetDiff`

- `public IReadOnlyList<LatticeAuthorizationRule> ToUpsert { get; }`
- `public IReadOnlyList<LatticeAuthorizationRule> ToDelete { get; }`
- `public bool IsEmpty`

### `Orleans.Lattice.Apps.AppSchemaDeclaration`

[Source](../../src/lattice.apps/Manifest/AppSchemaDeclaration.cs) (line 4).

`public sealed record AppSchemaDeclaration`

- `public required string Tree { get; init; }`
- `public required string Family { get; init; }`
- `public required int Version { get; init; }`
- `public bool StrictIngest { get; init; }`

### `Orleans.Lattice.Apps.AppScopeTemplate`

[Source](../../src/lattice.apps/Manifest/AppScopeTemplate.cs) (line 6).

`public sealed record AppScopeTemplate`

- `public required string Tree { get; init; }`
- `public AppSlug? App { get; init; }`
- `public LatticeScopeKind Kind { get; init; }`
- `public string? KeyOrPrefix { get; init; }`

### `Orleans.Lattice.Apps.AppSlug`

[Source](../../src/lattice.apps/Manifest/AppSlug.cs) (line 7).

`public readonly record struct AppSlug`

- `public string Value { get; private init; }`
- `public static AppSlug Parse(string value)`
- `public static bool TryParse(string? value, out AppSlug slug)`
- `public override string ToString()`

### `Orleans.Lattice.Apps.AppSourceResult`

[Source](../../src/lattice.apps/Sources/AppSourceResult.cs) (line 9).

`public sealed class AppSourceResult`

- `public AppSourceStatus Status { get; }`
- `public bool IsResolved`
- `public AppSlug Slug { get; }`
- `public AppManifest? Manifest { get; }`
- `public AppProvenance? Provenance { get; }`
- `public IAppActivationHandle? Activation { get; }`
- `public AppVersion? RequestedVersion { get; }`
- `public AppVersion? AvailableVersion { get; }`
- `public IReadOnlyList<string> SourceKeys { get; }`
- `public IReadOnlyList<AppManifestError> Errors { get; }`
- `public static AppSourceResult Resolved(AppManifest manifest, AppProvenance provenance, IAppActivationHandle activation)`
- `public static AppSourceResult NotFound(AppSlug slug)`
- `public static AppSourceResult VersionMismatch(AppSlug slug, AppVersion requested, AppVersion available)`
- `public static AppSourceResult InvalidManifest(AppSlug slug, IReadOnlyList<AppManifestError> errors)`
- `public static AppSourceResult IdentityMismatch(AppSlug slug, AppSlug declared)`
- `public static AppSourceResult DuplicateRegistration(AppSlug slug)`
- `public static AppSourceResult UnknownSource(AppSlug slug, string sourceKey)`
- `public static AppSourceResult Ambiguous(AppSlug slug, IReadOnlyList<string> sourceKeys)`
- `public static AppSourceResult SourceMisconfigured(AppSlug slug, IReadOnlyList<AppManifestError> errors)`

### `Orleans.Lattice.Apps.AppSourceStatus`

[Source](../../src/lattice.apps/Sources/AppSourceStatus.cs) (line 4).

`public enum AppSourceStatus`

- `Resolved = 0`
- `NotFound = 1`
- `VersionMismatch = 2`
- `InvalidManifest = 3`
- `IdentityMismatch = 4`
- `DuplicateRegistration = 5`
- `Ambiguous = 6`
- `SourceMisconfigured = 7`

### `Orleans.Lattice.Apps.AppSubscriptionCompilation`

[Source](../../src/lattice.apps/Subscriptions/AppSubscriptionCompilation.cs) (line 7).

`public sealed class AppSubscriptionCompilation`

- `public bool Succeeded`
- `public IReadOnlyList<AppSubscriptionContext> Subscriptions { get; }`
- `public IReadOnlyList<AppSubscriptionDenial> Denials { get; }`

### `Orleans.Lattice.Apps.AppSubscriptionCompiler`

[Source](../../src/lattice.apps/Subscriptions/AppSubscriptionCompiler.cs) (line 37).

`public static class AppSubscriptionCompiler`

- `public static AppSubscriptionCompilation Compile( AppManifest manifest, TenantId tenant, AppCapabilityCeiling ceiling, AppTreeOwnerSnapshot? owners = null)`

### `Orleans.Lattice.Apps.AppSubscriptionContext`

[Source](../../src/lattice.apps/Subscriptions/AppSubscriptionContext.cs) (line 9).

`public sealed class AppSubscriptionContext`

- `public TenantId Tenant { get; }`
- `public AppSlug App { get; }`
- `public string Name { get; }`
- `public AppSlug ObservedApp { get; }`
- `public string Tree { get; }`
- `public string LocalTreeId { get; }`
- `public string TreeId { get; }`
- `public string? KeyPrefix { get; }`
- `public bool IsCrossApp`

### `Orleans.Lattice.Apps.AppSubscriptionDeclaration`

[Source](../../src/lattice.apps/Manifest/AppSubscriptionDeclaration.cs) (line 4).

`public sealed record AppSubscriptionDeclaration`

- `public required string Name { get; init; }`
- `public required string Tree { get; init; }`
- `public AppSlug? App { get; init; }`
- `public string? KeyPrefix { get; init; }`

### `Orleans.Lattice.Apps.AppSubscriptionDenial`

[Source](../../src/lattice.apps/Subscriptions/AppSubscriptionDenial.cs) (line 16).

`public sealed record AppSubscriptionDenial(string SubscriptionName, AppSlug ObservedApp, LatticeScope Scope, string Message)`

- `Primary constructor / positional members: (string SubscriptionName, AppSlug ObservedApp, LatticeScope Scope, string Message)`

### `Orleans.Lattice.Apps.AppTreeDeclaration`

[Source](../../src/lattice.apps/Manifest/AppTreeDeclaration.cs) (line 4).

`public sealed record AppTreeDeclaration`

- `public required string Name { get; init; }`
- `public int? ShardCount { get; init; }`
- `public int? VirtualShardCount { get; init; }`
- `public int? MaxLeafKeys { get; init; }`
- `public int? MaxInternalChildren { get; init; }`
- `public int? WalPartitions { get; init; }`
- `public TimeSpan? SoftDeleteDuration { get; init; }`
- `public bool Rebuildable { get; init; }`
- `public string? AdoptedTreeId { get; init; }`

### `Orleans.Lattice.Apps.AppTreeOwnerSnapshot`

[Source](../../src/lattice.apps/Ownership/AppTreeOwnerSnapshot.cs) (line 13).

`public sealed class AppTreeOwnerSnapshot`

- `public static AppTreeOwnerSnapshot None { get; }`
- `public int Count`
- `public static AppTreeOwnerSnapshot Create(IEnumerable<KeyValuePair<string, AppSlug>> owners)`
- `public bool IsOwnedBy(string effectiveTreeId, AppSlug app)`

### `Orleans.Lattice.Apps.AppTreeOwnershipConflict`

[Source](../../src/lattice.apps/Ownership/AppTreeOwnershipConflict.cs) (line 13).

`public sealed record AppTreeOwnershipConflict( string TreeName, AppTreeOwnershipConflictReason Reason, AppSlug? OwningApp, string Message)`

- `Primary constructor / positional members: ( string TreeName, AppTreeOwnershipConflictReason Reason, AppSlug? OwningApp, string Message)`

### `Orleans.Lattice.Apps.AppTreeOwnershipConflictReason`

[Source](../../src/lattice.apps/Ownership/AppTreeOwnershipConflictReason.cs) (line 4).

`public enum AppTreeOwnershipConflictReason`

- `None = 0`
- `OwnedByAnotherApp = 1`
- `PreExistingUnownedTree = 2`
- `DerivedTree = 3`
- `AliasTarget = 4`

### `Orleans.Lattice.Apps.AppUiAsset`

[Source](../../src/lattice.apps/Manifest/AppUiAsset.cs) (line 4).

`public sealed record AppUiAsset`

- `public required string Path { get; init; }`
- `public required string MediaType { get; init; }`
- `public required string Digest { get; init; }`

### `Orleans.Lattice.Apps.AppUiBridgeDeclaration`

[Source](../../src/lattice.apps/Manifest/AppUiBridgeDeclaration.cs) (line 4).

`public sealed record AppUiBridgeDeclaration`

- `public required string Operation { get; init; }`
- `public string[]? Trees { get; init; }`

### `Orleans.Lattice.Apps.AppUiBridgeGrant`

[Source](../../src/lattice.apps/Manifest/AppUiBridgeGrant.cs) (line 8).

`public readonly record struct AppUiBridgeGrant`

- `public AppUiBridgeGrant(string operation, string? tree = null)`
- `public string Operation { get; init; }`
- `public string? Tree { get; init; }`

### `Orleans.Lattice.Apps.AppUiBridgeOperations`

[Source](../../src/lattice.apps/Manifest/AppUiBridgeOperations.cs) (line 12).

`public static class AppUiBridgeOperations`

- `public const string ContextRead`
- `public const string ContextUser`
- `public const string DataRead`
- `public const string DataWrite`
- `public const string DataDelete`
- `public const string NavSync`
- `public const string UiNotify`
- `public static IReadOnlySet<string> All { get; }`
- `public static bool IsKnown(string? operation)`
- `public static bool IsDataOperation(string? operation)`

### `Orleans.Lattice.Apps.AppUiBridgeRequest`

[Source](../../src/lattice.apps/Manifest/AppUiBridgeRequest.cs) (line 14).

`public sealed class AppUiBridgeRequest : IEquatable<AppUiBridgeRequest>`

- `public static AppUiBridgeRequest Empty { get; }`
- `public ImmutableArray<AppUiBridgeGrant> Grants { get; private init; }`
- `public bool IsEmpty`
- `public static AppUiBridgeRequest Create(IEnumerable<AppUiBridgeGrant> grants)`
- `public static AppUiBridgeRequest FromManifest(AppManifest manifest)`
- `public bool Covers(AppUiBridgeGrant grant)`
- `public AppUiBridgeRequest AddedRelativeTo(AppUiBridgeRequest consented)`
- `public bool Equals(AppUiBridgeRequest? other)`
- `public override bool Equals(object? obj)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Apps.AppUiBundle`

[Source](../../src/lattice.apps/Manifest/AppUiBundle.cs) (line 12).

`public static class AppUiBundle`

- `public const int MaxAssets`
- `public const int MaxAssetBytes`
- `public const int MaxBundleBytes`
- `public const int MaxPathLength`
- `public static IReadOnlySet<string> AllowedMediaTypes { get; }`
- `public static bool IsValidPath(string? path)`
- `public static bool IsValidDigest(string? digest)`
- `public static string ComputeBundleDigest(IReadOnlyCollection<AppUiAsset> assets)`

### `Orleans.Lattice.Apps.AppUiDeclaration`

[Source](../../src/lattice.apps/Manifest/AppUiDeclaration.cs) (line 8).

`public sealed record AppUiDeclaration`

- `public required string Entry { get; init; }`
- `public string[]? Styles { get; init; }`
- `public AppUiScript[]? Scripts { get; init; }`
- `public required AppUiAsset[] Assets { get; init; }`
- `public required string BundleDigest { get; init; }`
- `public AppUiBridgeDeclaration[]? Bridge { get; init; }`
- `public int MinProtocol { get; init; }`

### `Orleans.Lattice.Apps.AppUiProtocol`

[Source](../../src/lattice.apps/Manifest/AppUiProtocol.cs) (line 4).

`public static class AppUiProtocol`

- `public const int Current`

### `Orleans.Lattice.Apps.AppUiScript`

[Source](../../src/lattice.apps/Manifest/AppUiScript.cs) (line 9).

`public sealed record AppUiScript`

- `public required string Path { get; init; }`
- `public bool Module { get; init; }`

### `Orleans.Lattice.Apps.AppVersion`

[Source](../../src/lattice.apps/Manifest/AppVersion.cs) (line 6).

`public readonly record struct AppVersion`

- `public string Value { get; private init; }`
- `public static AppVersion Parse(string value)`
- `public static bool TryParse(string? value, out AppVersion version)`
- `public override string ToString()`

### `Orleans.Lattice.Apps.CompiledAppRegistrySnapshot`

[Source](../../src/lattice.apps/Registry/CompiledAppRegistrySnapshot.cs) (line 11).

`public sealed class CompiledAppRegistrySnapshot`

- `public static CompiledAppRegistrySnapshot Empty { get; }`
- `public long Epoch { get; }`
- `public IReadOnlyList<AppRegistryRecord> Records { get; }`
- `public int Count`
- `public bool TryGet(TenantId tenant, AppSlug slug, out AppRegistryRecord? record)`
- `public IReadOnlyList<AppRegistryRecord> GetTenantApps(TenantId tenant)`
- `public IReadOnlyList<AppRegistryRecord> GetEnabledTenantApps(TenantId tenant)`

### `Orleans.Lattice.Apps.IAppActivationHandle`

[Source](../../src/lattice.apps/Sources/IAppActivationHandle.cs) (line 7).

`public interface IAppActivationHandle`

- `AppIdentity Identity { get; }`
- `ValueTask<AppActivationResult> ActivateAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Apps.IAppActivationPipeline`

[Source](../../src/lattice.apps/Activation/IAppActivationPipeline.cs) (line 27).

`public interface IAppActivationPipeline`

- `Task<AppActivationOutcome> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)`
- `Task<AppActivationOutcome> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)`
- `Task<AppActivationOutcome> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)`
- `Task<AppActivationOutcome> ReconcileAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)`
- `Task<AppActivationStatus?> GetStatusAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Apps.IAppChangeFeedHandler`

[Source](../../src/lattice.apps/Subscriptions/IAppChangeFeedHandler.cs) (line 25).

`public interface IAppChangeFeedHandler`

- `Task HandleAsync(AppSubscriptionContext subscription, LatticeMutation mutation, CancellationToken cancellationToken)`

### `Orleans.Lattice.Apps.IAppRegistry`

[Source](../../src/lattice.apps/Registry/IAppRegistry.cs) (line 42).

`public interface IAppRegistry`

- `Task<AppRegistryRecord?> GetAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<AppRegistryRecord> ListAsync(CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<AppRegistryRecord> ListForTenantAsync(TenantId tenant, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<AppTreeOwnershipConflict>> GetTreeOwnershipConflictsAsync( TenantId tenant, AppManifest manifest, AppProvenance provenance, CancellationToken cancellationToken = default)`
- `Task<AppRegistryTransitionResult> InstallAsync(AppRegistryInstallRequest request, CancellationToken cancellationToken = default)`
- `Task<AppRegistryTransitionResult> UpgradeAsync(AppRegistryInstallRequest request, CancellationToken cancellationToken = default)`
- `Task<AppRegistryTransitionResult> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)`
- `Task<AppRegistryTransitionResult> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)`
- `Task<AppRegistryTransitionResult> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Apps.IAppRegistryProjection`

[Source](../../src/lattice.apps/Registry/IAppRegistryProjection.cs) (line 11).

`public interface IAppRegistryProjection`

- `long CurrentEpoch { get; }`
- `CompiledAppRegistrySnapshot Current { get; }`
- `Task EnsureWarmAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Apps.IAppSource`

[Source](../../src/lattice.apps/Sources/IAppSource.cs) (line 48).

`public interface IAppSource`

- `ValueTask<AppSourceResult> ResolveAsync( AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Apps.InImageAppRegistration`

[Source](../../src/lattice.apps/Sources/InImageAppRegistration.cs) (line 10).

`public sealed record InImageAppRegistration`

- `public InImageAppRegistration(AppSlug slug, Assembly assembly, string manifestResourceName)`
- `public AppSlug Slug { get; }`
- `public Assembly Assembly { get; }`
- `public string ManifestResourceName { get; }`
- `public string Publisher { get; init; }`
- `public string AssetResourcePrefix { get; init; }`

### `Orleans.Lattice.Apps.InImageAppSource`

[Source](../../src/lattice.apps/Sources/InImageAppSource.cs) (line 37).

`public sealed class InImageAppSource : IAppCatalogSource`

- `public const string SourceKey`
- `public const int MaxAssetBytes`
- `public InImageAppSource(IOptions<InImageAppSourceOptions> options)`
- `public AppSourceDescriptor Descriptor`
- `public ValueTask<AppSourceResult> ResolveAsync( AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)`
- `public ValueTask<AppSourcePage> ListAsync(AppSourceQuery query, CancellationToken cancellationToken = default)`
- `public ValueTask<AppAssetResult> OpenAssetAsync( AppSlug slug, AppVersion version, string path, string expectedSha256, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Apps.InImageAppSourceOptions`

[Source](../../src/lattice.apps/Sources/InImageAppSourceOptions.cs) (line 9).

`public sealed class InImageAppSourceOptions`

- `public IList<InImageAppRegistration> Registrations { get; }`
- `public InImageAppSourceOptions Register(AppSlug slug, Assembly assembly, string manifestResourceName)`

### `Orleans.Lattice.Apps.LatticeAppsOptions`

[Source](../../src/lattice.apps/LatticeAppsOptions.cs) (line 7).

`public sealed class LatticeAppsOptions`

- `public static readonly TimeSpan DefaultStartupRetryDelay`
- `public static readonly TimeSpan DefaultStartupRetryMaxDelay`
- `public bool ReconcileOnStartup { get; set; }`
- `public TimeSpan StartupRetryDelay { get; set; }`
- `public TimeSpan StartupRetryMaxDelay { get; set; }`

### `Orleans.Lattice.Apps.LatticeAppsServiceCollectionExtensions`

[Source](../../src/lattice.apps/LatticeAppsServiceCollectionExtensions.Sources.cs) (line 9).

`public static partial class LatticeAppsServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeAppSource<TSource>(this ISiloBuilder builder) where TSource : class, IAppCatalogSource`
- `public static IServiceCollection AddLatticeAppSource<TSource>(this IServiceCollection services) where TSource : class, IAppCatalogSource`
- `public static ISiloBuilder AddLatticeAppSource(this ISiloBuilder builder, IAppCatalogSource instance)`
- `public static IServiceCollection AddLatticeAppSource(this IServiceCollection services, IAppCatalogSource instance)`
- `public static ISiloBuilder AddLatticeApp( this ISiloBuilder builder, string slug, Assembly assembly, string manifestResourceName, string assetResourcePrefix)`
- `public static IServiceCollection AddLatticeApp( this IServiceCollection services, string slug, Assembly assembly, string manifestResourceName, string assetResourcePrefix)`

[Source](../../src/lattice.apps/LatticeAppsServiceCollectionExtensions.Subscriptions.cs) (line 5).

`public static partial class LatticeAppsServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeAppSubscriptionHandler<THandler>( this IServiceCollection services, AppSlug app, string subscriptionName) where THandler : class, IAppChangeFeedHandler`
- `public static IServiceCollection AddLatticeAppSubscriptionHandler( this IServiceCollection services, AppSlug app, string subscriptionName, Func<IServiceProvider, IAppChangeFeedHandler> factory)`

[Source](../../src/lattice.apps/LatticeAppsServiceCollectionExtensions.cs) (line 16).

`public static partial class LatticeAppsServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeApps(this ISiloBuilder builder, Action<LatticeAppsOptions>? configure = null)`
- `public static IServiceCollection AddLatticeApps(this IServiceCollection services, Action<LatticeAppsOptions>? configure = null)`
- `public static ISiloBuilder AddLatticeApp(this ISiloBuilder builder, string slug, Assembly assembly, string manifestResourceName)`
- `public static IServiceCollection AddLatticeApp(this IServiceCollection services, string slug, Assembly assembly, string manifestResourceName)`

### `Orleans.Lattice.Apps.Sources.AppAssetResult`

[Source](../../src/lattice.apps/Sources/AppAssetResult.cs) (line 11).

`public sealed class AppAssetResult`

- `public const int Sha256HexLength`
- `public AppAssetStatus Status { get; }`
- `public bool IsOpened`
- `public string Path { get; }`
- `public ReadOnlyMemory<byte> Content { get; }`
- `public string? MediaType { get; }`
- `public string? ActualSha256 { get; }`
- `public IReadOnlyList<AppManifestError> Errors { get; }`
- `public static AppAssetResult Verify(string path, ReadOnlyMemory<byte> content, string mediaType, string expectedSha256)`
- `public static AppAssetResult NotFound(string path)`
- `public static AppAssetResult NotAvailable(string path, string reason)`
- `public static bool IsSha256Hex(string? value)`

### `Orleans.Lattice.Apps.Sources.AppAssetStatus`

[Source](../../src/lattice.apps/Sources/AppAssetStatus.cs) (line 4).

`public enum AppAssetStatus`

- `Opened = 0`
- `NotFound = 1`
- `NotAvailable = 2`
- `DigestMismatch = 3`

### `Orleans.Lattice.Apps.Sources.AppSourceCapabilities`

[Source](../../src/lattice.apps/Sources/AppSourceCapabilities.cs) (line 4).

`public enum AppSourceCapabilities`

- `None = 0`
- `Enumerate = 1`
- `Search = 2`
- `MultipleVersions = 4`
- `RequiresAcquisition = 8`

### `Orleans.Lattice.Apps.Sources.AppSourceDescriptor`

[Source](../../src/lattice.apps/Sources/AppSourceDescriptor.cs) (line 8).

`public sealed record AppSourceDescriptor`

- `public AppSourceDescriptor(string key, string displayName, AppSourceKind kind, AppSourceCapabilities capabilities)`
- `public string Key { get; }`
- `public string DisplayName { get; }`
- `public AppSourceKind Kind { get; }`
- `public AppSourceCapabilities Capabilities { get; }`
- `public bool Supports(AppSourceCapabilities capability)`
- `public static bool IsValidKey(string? key)`

### `Orleans.Lattice.Apps.Sources.AppSourceEntry`

[Source](../../src/lattice.apps/Sources/AppSourceEntry.cs) (line 13).

`public sealed class AppSourceEntry`

- `public AppSlug Slug { get; }`
- `public bool IsAvailable`
- `public IReadOnlyList<AppVersion> Versions { get; }`
- `public AppManifest? Manifest { get; }`
- `public AppProvenance? Provenance { get; }`
- `public IReadOnlyList<AppManifestError> Errors { get; }`
- `public static AppSourceEntry Available(IReadOnlyList<AppVersion> versions, AppManifest manifest, AppProvenance provenance)`
- `public static AppSourceEntry Unavailable(AppSlug slug, IReadOnlyList<AppManifestError> errors)`

### `Orleans.Lattice.Apps.Sources.AppSourceKind`

[Source](../../src/lattice.apps/Sources/AppSourceKind.cs) (line 4).

`public enum AppSourceKind`

- `Static = 0`
- `Dynamic = 1`

### `Orleans.Lattice.Apps.Sources.AppSourcePage`

[Source](../../src/lattice.apps/Sources/AppSourcePage.cs) (line 4).

`public sealed class AppSourcePage`

- `public static AppSourcePage Empty { get; }`
- `public IReadOnlyList<AppSourceEntry> Entries { get; }`
- `public string? Continuation { get; }`
- `public bool HasMore`
- `public static AppSourcePage Create(IReadOnlyList<AppSourceEntry> entries, string? continuation)`

### `Orleans.Lattice.Apps.Sources.AppSourceQuery`

[Source](../../src/lattice.apps/Sources/AppSourceQuery.cs) (line 8).

`public sealed record AppSourceQuery`

- `public const int MinPageSize`
- `public const int MaxPageSize`
- `public const int DefaultPageSize`
- `public static AppSourceQuery Default { get; }`
- `public string? Text { get; init; }`
- `public int PageSize { get; init; }`
- `public string? Continuation { get; init; }`

### `Orleans.Lattice.Apps.Sources.AppSourceSet`

[Source](../../src/lattice.apps/Sources/AppSourceSet.cs) (line 28).

`public sealed class AppSourceSet : IAppSource`

- `public AppSourceSet(IEnumerable<IAppCatalogSource> sources)`
- `public IReadOnlyList<IAppCatalogSource> Sources { get; }`
- `public IReadOnlyList<AppManifestError> CompositionErrors { get; }`
- `public bool IsValid`
- `public bool TryGet(string key, out IAppCatalogSource? source)`
- `public ValueTask<AppSourceResult> ResolveAsync( AppSlug slug, AppVersion? version = null, string? sourceKey = null, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Apps.Sources.IAppCatalogSource`

[Source](../../src/lattice.apps/Sources/IAppCatalogSource.cs) (line 49).

`public interface IAppCatalogSource : IAppSource`

- `AppSourceDescriptor Descriptor { get; }`
- `ValueTask<AppSourcePage> ListAsync(AppSourceQuery query, CancellationToken cancellationToken = default)`
- `ValueTask<AppAssetResult> OpenAssetAsync( AppSlug slug, AppVersion version, string path, string expectedSha256, CancellationToken cancellationToken = default)`
