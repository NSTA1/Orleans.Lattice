# Orleans.Lattice.Explorer API reference

This reference covers the supported host entry points, the sign-in seam used by
hosts, and the public AppKit protocol shared with app-frame authors. The web
head composes the Core, UI and AppKit assemblies; it does not expose an area,
completion-source or palette-command registration API. The native UI is shipped
as a compiled, built-in console rather than as a component plug-in surface.

## Web-host entry points

### `LatticeExplorerWebServiceCollectionExtensions.AddLatticeExplorerWeb`

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Web;

var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeExplorerWeb(options => options.BasePath = "/explorer");
```

Signature:

```text
IServiceCollection AddLatticeExplorerWeb(
    IServiceCollection services,
    Action<LatticeExplorerWebOptions>? configure = null)
```

Registers the interactive-server UI, connection and configuration services,
per-circuit authentication, browser preference storage, tenant view, app-frame
host and all compiled-in areas. Configure the host options here, then map the
endpoints with `MapLatticeExplorer`. A null service collection throws
`ArgumentNullException`.

### `LatticeExplorerWebEndpointRouteBuilderExtensions.MapLatticeExplorer`

Signature:

```text
IEndpointRouteBuilder MapLatticeExplorer(IEndpointRouteBuilder endpoints)
```

Maps the Explorer's static assets, local sign-in and sign-out form endpoints,
app-frame bootstrap route and interactive Razor components. At the root, it
maps onto the application's endpoint pipeline. With a non-root
`LatticeExplorerWebOptions.BasePath`, it isolates the Explorer in a branch at
that prefix. The supplied route builder must also be the application middleware
pipeline so the host can install the required response-security headers; an
unsupported route group fails with `InvalidOperationException` instead of
serving the console without those headers. The method returns the same route
builder for chaining.

`LatticeExplorerWebOptions` is the host configuration type. Its complete
property/type/default reference is [Configuration](configuration.md#latticeexplorerweboptions).

## Core sign-in integration

The Core authentication surface is provider based. The web head registers the
built-in Basic method and a per-circuit auth session; optional providers add
another `IExplorerAuthMethod` registration.

| Public type | Public members | Behaviour |
|---|---|---|
| `IExplorerAuthMethod` | `SchemeId`, `CanHandle(string advertisedScheme)`, `ChallengeAsync(ExplorerAuthChallengeContext context, CancellationToken cancellationToken = default)` | Identifies a scheme, selects advertised schemes, and performs sign-in. |
| `ExplorerAuthChallengeContext` | `SchemeId`, `Parameters`, `Inputs`, `Endpoint`, `TimeProvider` | Carries the selected advertisement, operator inputs and endpoint into a challenge. |
| `ExplorerAuthSignIn` | `SchemeId`, `DisplayName`, `Authentication` | Supplies the credential applied to calls after sign-in. |
| `IExplorerAuthSession` | `IsAuthenticated`, `Username`, `CurrentScheme`, `CurrentAuthentication`, `AvailableSchemes`; `AuthenticationChanged`, `ReauthRequired`; `GetAuthenticationFor`, `InitializeAsync`, `LoginAsync`, `LoginWithMethodAsync`, `DiscoverAsync`, `LogoutAsync` | Owns per-circuit sign-in state, discovers endpoint schemes, applies credentials, and returns a credential only for the endpoint it was minted for. |
| `ExplorerAccessTokenSource` | constructor; `DefaultRefreshMargin`; `ReauthRequired`; `GetAuthorizationHeaderAsync`, `RefreshAsync`, `Dispose` | Refreshes bearer tokens before expiry, coalesces concurrent refreshes and latches when silent renewal ends. |
| `ExplorerAccessToken` | `Token`, `ExpiresOn`, `Scheme`; `ToAuthorizationHeader`, `ToString` | In-memory access-token value consumed by the refresh source; its string representation redacts the token. |
| `LatticeCallAuthentication` | `AuthorizationHeaderName`, `Headers`, `HasHeaders`, `CredentialProvider`, `HasCredentialProvider`; `Basic`, `Bearer` | Describes the call credential; static credential headers are restricted to secure transport unless local-development h2c is explicitly enabled. |

See [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
and [Adding a custom auth method](adding-a-custom-auth-method.md) for the
challenge lifecycle, credential binding, and extension examples. The web head
also exposes public configuration records for federated sign-out and
re-authentication; see [Configuration](configuration.md#explorerreauthoptions).

## AppKit protocol v1

`Orleans.Lattice.Explorer.AppKit` is a shared static-assets package. Its one
public .NET protocol type is `AppKitProtocol`; the package also serves the
versioned browser bootstrap, schema, stylesheet and fonts from
`_content/Orleans.Lattice.Explorer.AppKit/appkit/v1/`. `AppKitProtocol` is the
public owner of the frame protocol's literal names and bounds. `Messages`,
`Events`, `RevokedReasons`, `Operations`, `DataActions`, `ErrorCodes`,
`FailureCodes` and `Limits` are its public nested constant groups; each `All`
property is a read-only list in protocol order.

| Member group | Public constant members and values |
|---|---|
| `AppKitProtocol` | `Version = 1`; `AssetDirectory = "appkit/v1"`; `FrameDocument = "frame.html"`; `TreeNamePattern = "^[a-z][a-z0-9_-]*$"`. |
| `Messages` | `Ready = "lattice.ready"`; `Hello = "lattice.hello"`; `Bundle = "lattice.bundle"`; `Loaded = "lattice.loaded"`; `Failed = "lattice.failed"`; `All`. |
| `Events` | `ContextChanged = "context.changed"`; `NavChanged = "nav.changed"`; `Revoked = "lattice.revoked"`; `All`. |
| `RevokedReasons` | `Disabled = "disabled"`; `Uninstalled = "uninstalled"`; `Upgraded = "upgraded"`; `Revision = "revision"`; `Closed = "closed"`; `All`. |
| `Operations` | `ContextRead = "context.read"`; `ContextUser = "context.user"`; `DataRead = "data.read"`; `DataWrite = "data.write"`; `DataDelete = "data.delete"`; `NavSync = "nav.sync"`; `UiNotify = "ui.notify"`; `All`. |
| `DataActions` | `Get = "get"`; `Scan = "scan"`; `Set = "set"`; `Delete = "delete"`; `All`. |
| `ErrorCodes` | `Denied = "denied"`; `NotFound = "not_found"`; `Invalid = "invalid"`; `TooLarge = "too_large"`; `RateLimited = "rate_limited"`; `Unavailable = "unavailable"`; `Conflict = "conflict"`; `All`. |
| `FailureCodes` | `ProtocolUnsupported = "protocol_unsupported"`; `BundleMalformed = "bundle_malformed"`; `BundleTooLarge = "bundle_too_large"`; `AssetMissing = "asset_missing"`; `DigestMismatch = "digest_mismatch"`; `BundleDigestMismatch = "bundle_digest_mismatch"`; `CryptoUnavailable = "crypto_unavailable"`; `LoadFailed = "load_failed"`; `Internal = "internal"`; `All`. |
| `Limits` | `MaxValueBytes = 65,536`; `MaxValueBase64Length = 87,384`; `MaxRequestBytes = 131,072`; `MaxResponseBytes = 1,048,576`; `MaxPageSize = 200`; `MaxNotifyLength = 200`; `MaxKeyLength = 1,024`; `MaxTreeNameLength = 128`; `MaxPathLength = 1,024`; `MaxContinuationLength = 4,096`; `MaxRoles = 256`; `MaxRoleNameLength = 128`; `DefaultTimeoutMilliseconds = 30,000`; `MaxTimeoutMilliseconds = 300,000`. |

Requests are made through the in-frame `lattice` API; the host transfers only a
verified app bundle over a message port. The protocol provides no direct network,
cluster credential, app lifecycle or consent message. Data operations remain
scoped to the installed app's consented grants and the signed-in caller's rights.
See [Lattice Apps in the Explorer](lattice-apps.md#the-app-frame) and the
[Task board sample](../../samples/Explorer/Apps/TaskBoard/README.md) for the
runtime and trust boundary.

## Complete exported symbol index

This index lists every effectively public type and each member declared directly on that type in the current package builds. Overloaded members are listed once with their overload count. The preceding sections describe the supported integration contracts; feature-specific UI behavior is documented in the linked area guides.

### Orleans.Lattice.Explorer assemblies

#### `Orleans.Lattice.Explorer.Core` (167 exported types)
- `Orleans.Lattice.Explorer.Core.Authentication.BasicExplorerAuthMethod`
  - `constructors (1)`; `CanHandle`; `ChallengeAsync`; `SchemeId`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerAccessToken`
  - `constructors (1)`; `Equals (2 overloads)`; `ExpiresOn`; `GetHashCode`; `op_Equality`; `op_Inequality`; `Scheme`; `ToAuthorizationHeader`
    `Token`; `ToString`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerAccessTokenSource`
  - `constructors (1)`; `DefaultRefreshMargin`; `Dispose`; `GetAuthorizationHeaderAsync`; `ReauthRequired`; `RefreshAsync`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerAuthChallengeContext`
  - `constructors (1)`; `<Clone>$`; `Endpoint`; `Equals (2 overloads)`; `GetHashCode`; `Inputs`; `op_Equality`; `op_Inequality`; `Parameters`
    `SchemeId`; `TimeProvider`; `ToString`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerAuthSchemeAdvertisement`
  - `constructors (1)`; `<Clone>$`; `Empty`; `Equals (2 overloads)`; `GetHashCode`; `HasSchemes`; `op_Equality`; `op_Inequality`; `Schemes`
    `ToString`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerAuthSchemeDescriptor`
  - `constructors (1)`; `<Clone>$`; `DisplayName`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`; `Parameters`; `SchemeId`
    `ToString`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerAuthSchemes`
  - `AudienceParameter`; `AuthorityParameter`; `Basic`; `ClientIdParameter`; `Entra`; `Oidc`; `PasswordInput`; `TenantIdParameter`; `UsernameInput`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerAuthServiceCollectionExtensions`
  - `AddExplorerAuth`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerAuthSession`
  - `constructors (1)`; `AuthenticationChanged`; `AvailableSchemes`; `CurrentAuthentication`; `CurrentScheme`; `DiscoverAsync`; `Dispose`
    `GetAuthenticationFor`; `InitializeAsync`; `IsAuthenticated`; `LoginAsync`; `LoginWithMethodAsync`; `LogoutAsync`; `ReauthRequired`
    `SelectMethodForAdvertisement`; `Username`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerAuthSignIn`
  - `constructors (1)`; `<Clone>$`; `Authentication`; `DisplayName`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`
    `SchemeId`; `ToString`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerContentSecurityPolicyOptions`
  - `constructors (1)`; `AdditionalFormActionSources`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerReauthChallenge`
  - `BuildUrl`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerReauthOptions`
  - `constructors (1)`; `AppendReturnUrl`; `ChallengePath`; `DefaultReturnUrlParameter`; `ReturnUrlParameter`
- `Orleans.Lattice.Explorer.Core.Authentication.ExplorerSignOutOptions`
  - `constructors (1)`; `FederatedSignOutPath`
- `Orleans.Lattice.Explorer.Core.Authentication.GrpcExplorerAuthSchemeProbe`
  - `constructors (1)`; `Dispose`; `ProbeAsync (2 overloads)`
- `Orleans.Lattice.Explorer.Core.Authentication.ICredentialStore`
  - `ClearAsync`; `GetAsync`; `SetAsync`
- `Orleans.Lattice.Explorer.Core.Authentication.IExplorerAuthMethod`
  - `CanHandle`; `ChallengeAsync`; `SchemeId`
- `Orleans.Lattice.Explorer.Core.Authentication.IExplorerAuthSchemeProbe`
  - `ProbeAsync (2 overloads)`
- `Orleans.Lattice.Explorer.Core.Authentication.IExplorerAuthSession`
  - `AuthenticationChanged`; `AvailableSchemes`; `CurrentAuthentication`; `CurrentScheme`; `DiscoverAsync`; `GetAuthenticationFor`
    `InitializeAsync`; `IsAuthenticated`; `LoginAsync`; `LoginWithMethodAsync`; `LogoutAsync`; `ReauthRequired`; `Username`
- `Orleans.Lattice.Explorer.Core.Authentication.IExplorerCredentialSeed`
  - `TrySeed`
- `Orleans.Lattice.Explorer.Core.Authentication.InMemoryCredentialStore`
  - `constructors (1)`; `ClearAsync`; `GetAsync`; `SetAsync`
- `Orleans.Lattice.Explorer.Core.Authentication.StoredCredential`
  - `constructors (1)`; `<Clone>$`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`; `Password`; `ToString`
    `Username`
- `Orleans.Lattice.Explorer.Core.Catalog.CatalogItem`
  - `constructors (1)`; `<Clone>$`; `DisplayName`; `Equals (2 overloads)`; `GetHashCode`; `Id`; `IndexName`; `IsAggregation`; `IsHistory`
    `IsRestoreShadow`; `Kind`; `Label`; `Lifecycle`; `op_Equality`; `op_Inequality`; `ProjectionProviderKey`; `ProjectionVersion`
    `RestoreShadowOfTreeId`; `ShardCount`; `SourceTreeId`; `ToString`
- `Orleans.Lattice.Explorer.Core.Catalog.CatalogKind`
  - `TagIndexes`; `Trees`; `value__`; `Views`
- `Orleans.Lattice.Explorer.Core.Catalog.CatalogPage`
  - `constructors (1)`; `<Clone>$`; `Equals (2 overloads)`; `GetHashCode`; `HasMore`; `Items`; `NextPageToken`; `op_Equality`; `op_Inequality`
    `ScopedToTenantId`; `ScopeFilteredCount`; `ToString`
- `Orleans.Lattice.Explorer.Core.Catalog.CatalogReader`
  - `constructors (1)`; `LoadAsync`
- `Orleans.Lattice.Explorer.Core.Catalog.ExplorerCatalogServiceCollectionExtensions`
  - `AddExplorerCatalog`
- `Orleans.Lattice.Explorer.Core.Catalog.ExplorerSelection`
  - `constructors (1)`; `Select`; `Selected`; `SelectionChanged`
- `Orleans.Lattice.Explorer.Core.Catalog.ICatalogReader`
  - `LoadAsync`
- `Orleans.Lattice.Explorer.Core.Catalog.IExplorerSelection`
  - `Select`; `Selected`; `SelectionChanged`
- `Orleans.Lattice.Explorer.Core.Configuration.EndpointValidation`
  - `TryValidate`
- `Orleans.Lattice.Explorer.Core.Configuration.EnvironmentExplorerBootstrap`
  - `constructors (1)`; `ConfigPathVariable`; `EndpointVariable`; `InsecureDevVariable`; `PasswordVariable`; `TransportHeadersVariable`
    `UsernameVariable`
- `Orleans.Lattice.Explorer.Core.Configuration.ExplorerBootstrapServiceCollectionExtensions`
  - `AddExplorerEnvironmentBootstrap`
- `Orleans.Lattice.Explorer.Core.Configuration.ExplorerConfigStoreOptions`
  - `constructors (1)`; `DefaultFileName`; `DefaultFilePath`; `DefaultFolderName`; `FilePath`
- `Orleans.Lattice.Explorer.Core.Configuration.ExplorerConfiguration`
  - `constructors (1)`; `<Clone>$`; `AllowUnencryptedHttp2`; `CurrentSchemaVersion`; `Endpoint`; `Equals (2 overloads)`; `GetHashCode`; `Headers`
    `op_Equality`; `op_Inequality`; `SchemaVersion`; `ToConnectionSettings`; `ToString`; `TransportHeaders`; `TransportMode`
- `Orleans.Lattice.Explorer.Core.Configuration.ExplorerConfigurationServiceCollectionExtensions`
  - `AddExplorerConfiguration`
- `Orleans.Lattice.Explorer.Core.Configuration.ExplorerSession`
  - `constructors (1)`; `ApplyAsync`; `ConfigurationChanged`; `Connection`; `Current`; `InitializeAsync`; `IsConfigured`
- `Orleans.Lattice.Explorer.Core.Configuration.ExplorerTransportMode`
  - `InsecureLoopbackDev`; `Secure`; `value__`
- `Orleans.Lattice.Explorer.Core.Configuration.IExplorerConfigStore`
  - `Exists`; `FilePath`; `LoadAsync`; `SaveAsync`
- `Orleans.Lattice.Explorer.Core.Configuration.IExplorerConfigurationSeed`
  - `TrySeed`
- `Orleans.Lattice.Explorer.Core.Configuration.IExplorerEnvironment`
  - `GetVariable`
- `Orleans.Lattice.Explorer.Core.Configuration.IExplorerSession`
  - `ApplyAsync`; `ConfigurationChanged`; `Connection`; `Current`; `InitializeAsync`; `IsConfigured`
- `Orleans.Lattice.Explorer.Core.Configuration.JsonExplorerConfigStore`
  - `constructors (1)`; `Exists`; `FilePath`; `LoadAsync`; `SaveAsync`
- `Orleans.Lattice.Explorer.Core.Configuration.ProcessExplorerEnvironment`
  - `constructors (1)`; `GetVariable`
- `Orleans.Lattice.Explorer.Core.Configuration.TransportSecurityPolicy`
  - `TryValidateConnection`; `TryValidateEndpoint`
- `Orleans.Lattice.Explorer.Core.Connection.CallCredentialsInterceptor`
  - `constructors (1)`; `AsyncServerStreamingCall`; `AsyncUnaryCall`; `BlockingUnaryCall`
- `Orleans.Lattice.Explorer.Core.Connection.ILatticeActiveTenantProvider`
  - `AssertedTenant`
- `Orleans.Lattice.Explorer.Core.Connection.ILatticeCallCredentialProvider`
  - `GetAuthorizationHeaderAsync`; `RefreshAsync`
- `Orleans.Lattice.Explorer.Core.Connection.ILatticeStateClient`
  - `CancelScanAsync`; `GetClusterInfoAsync`; `GetDeadLetterCountAsync`; `GetEntryAsync`; `GetEntryHistoryAsync`; `GetMetricsSnapshotAsync`
    `GetTreeStructureAsync`; `ListCoveredTreesAsync`; `ListDeadLettersAsync`; `ListIndexTagsAsync`; `ListTagIndexesAsync`; `ListTagValuesAsync`
    `ListTreesAsync`; `ListViewsAsync`; `ObserveChangesAsync`; `ObserveMetricsAsync`; `ScanEntriesAsync`; `ScanTagMembersAsync`
- `Orleans.Lattice.Explorer.Core.Connection.ILatticeStateConnection`
  - `ConfigureAsync`; `ProbeAsync`; `ReconnectAsync`; `Status`; `StatusChanged`
- `Orleans.Lattice.Explorer.Core.Connection.IReauthRequiredSource`
  - `ReauthRequired`
- `Orleans.Lattice.Explorer.Core.Connection.LatticeCallAuthentication`
  - `constructors (1)`; `<Clone>$`; `AuthorizationHeaderName`; `Basic`; `Bearer`; `CredentialProvider`; `Equals (2 overloads)`; `GetHashCode`
    `HasCredentialProvider`; `HasHeaders`; `Headers`; `op_Equality`; `op_Inequality`; `ToString`
- `Orleans.Lattice.Explorer.Core.Connection.LatticeConnectionSettings`
  - `constructors (1)`; `<Clone>$`; `ActiveTenantProvider`; `Address`; `AllowUnencryptedHttp2`; `Authentication`; `DegradeAfter`
    `Equals (2 overloads)`; `GetHashCode`; `HealthCheckInterval`; `MaxTransientRetries`; `op_Equality`; `op_Inequality`; `ToString`
    `TransientRetryBackoff`; `TransportHeaders`
- `Orleans.Lattice.Explorer.Core.Connection.LatticeConnectionState`
  - `Connected`; `Connecting`; `Disconnected`; `Faulted`; `Reconnecting`; `value__`
- `Orleans.Lattice.Explorer.Core.Connection.LatticeConnectionStatus`
  - `constructors (1)`; `<Clone>$`; `Deconstruct`; `Disconnected`; `Endpoint`; `Equals (2 overloads)`; `GetHashCode`; `IsDisconnected`; `IsUsable`
    `Message`; `op_Equality`; `op_Inequality`; `RequiresAuthentication`; `State`; `ToString`
- `Orleans.Lattice.Explorer.Core.Connection.LatticeExplorerConnectionServiceCollectionExtensions`
  - `AddLatticeStateConnection`
- `Orleans.Lattice.Explorer.Core.Connection.LatticeGrpcChannelFactory`
  - `ApplyActiveTenant`; `ApplyTransportHeaders`; `BuildChannelOptions`; `CreateCallInvoker`; `CreateChannel`
- `Orleans.Lattice.Explorer.Core.Connection.LatticeStateApiException`
  - `constructors (2)`; `IsPermissionDenied`; `IsTransient`; `RequiresAuthentication`
- `Orleans.Lattice.Explorer.Core.Connection.LatticeStateConnection`
  - `constructors (2)`; `CancelScanAsync`; `ConfigureAsync`; `DisposeAsync`; `GetClusterInfoAsync`; `GetDeadLetterCountAsync`; `GetEntryAsync`
    `GetEntryHistoryAsync`; `GetMetricsSnapshotAsync`; `GetTreeStructureAsync`; `ListCoveredTreesAsync`; `ListDeadLettersAsync`
    `ListIndexTagsAsync`; `ListTagIndexesAsync`; `ListTagValuesAsync`; `ListTreesAsync`; `ListViewsAsync`; `ObserveChangesAsync`
    `ObserveMetricsAsync`; `ProbeAsync`; `ReconnectAsync`; `ScanEntriesAsync`; `ScanTagMembersAsync`; `Status`; `StatusChanged`
- `Orleans.Lattice.Explorer.Core.Data.DataCrdtMember`
  - `constructors (1)`; `<Clone>$`; `ElementFormat`; `ElementText`; `Equals (2 overloads)`; `From`; `GetHashCode`; `op_Equality`; `op_Inequality`
    `Ordinal`; `ReplicaId`; `ToString`
- `Orleans.Lattice.Explorer.Core.Data.DataEntry`
  - `constructors (1)`; `<Clone>$`; `CrdtShape`; `CurrentMembers`; `Equals (2 overloads)`; `ExpiresAtTicks`; `From`; `GetHashCode`; `Hlc`
    `IsTombstone`; `Key`; `op_Equality`; `op_Inequality`; `ToString`; `Truncated`; `Value`; `ValueLength`
- `Orleans.Lattice.Explorer.Core.Data.DataPage`
  - `constructors (1)`; `<Clone>$`; `ContinuationToken`; `Empty`; `Entries`; `Equals (2 overloads)`; `GetHashCode`; `HasMore`; `op_Equality`
    `op_Inequality`; `ToString`
- `Orleans.Lattice.Explorer.Core.Data.DataPager`
  - `constructors (1)`; `CanGoNext`; `CanGoPrevious`; `CloseAsync`; `Current`; `KeyPrefix`; `NextAsync`; `PageIndex`; `PageSize`; `Previous`
    `ResetAsync`; `ScanMode`; `TagFilter`; `TreeId`
- `Orleans.Lattice.Explorer.Core.Data.DataPaging`
  - `DefaultPageSize`; `Increment`; `MaxPageSize`; `Normalize`; `PageSizes`
- `Orleans.Lattice.Explorer.Core.Data.DataReader`
  - `constructors (1)`; `CancelScanAsync`; `GetEntryAsync`; `ListCoveredTreesForIndexAsync`; `ListTagIndexesForTreeAsync`; `ListTagsForIndexAsync`
    `ListTagValuesForIndexAsync`; `ScanAsync`; `ScanPreviewBudget`; `ScanTagMembersAsync`
- `Orleans.Lattice.Explorer.Core.Data.DataSelection`
  - `SelectedKey`
- `Orleans.Lattice.Explorer.Core.Data.EntryChangeSignal`
  - `constructors (1)`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `Hlc`; `Key`; `Kind`; `op_Equality`; `op_Inequality`; `ToString`
- `Orleans.Lattice.Explorer.Core.Data.EntryLiveFollower`
  - `constructors (1)`; `FollowAsync`
- `Orleans.Lattice.Explorer.Core.Data.ExplorerDataServiceCollectionExtensions`
  - `AddExplorerData`
- `Orleans.Lattice.Explorer.Core.Data.IDataReader`
  - `CancelScanAsync`; `GetEntryAsync`; `ListCoveredTreesForIndexAsync`; `ListTagIndexesForTreeAsync`; `ListTagsForIndexAsync`
    `ListTagValuesForIndexAsync`; `ScanAsync`; `ScanTagMembersAsync`
- `Orleans.Lattice.Explorer.Core.Data.IEntryLiveFollower`
  - `FollowAsync`
- `Orleans.Lattice.Explorer.Core.Data.RenderedValue`
  - `constructors (1)`; `<Clone>$`; `Content`; `Equals (2 overloads)`; `Format`; `GetHashCode`; `Note`; `op_Equality`; `op_Inequality`; `ToString`
- `Orleans.Lattice.Explorer.Core.Data.TagFilter`
  - `constructors (1)`; `<Clone>$`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `IndexName`; `op_Equality`; `op_Inequality`; `Tag`
    `ToString`
- `Orleans.Lattice.Explorer.Core.Data.TagIndexRef`
  - `constructors (1)`; `<Clone>$`; `Equals (2 overloads)`; `GetHashCode`; `IndexName`; `op_Equality`; `op_Inequality`; `ToString`; `TreeId`
- `Orleans.Lattice.Explorer.Core.Data.TagMemberPage`
  - `constructors (1)`; `<Clone>$`; `ContinuationToken`; `Empty`; `Equals (2 overloads)`; `GetHashCode`; `HasMore`; `Members`; `op_Equality`
    `op_Inequality`; `ToString`
- `Orleans.Lattice.Explorer.Core.Data.TagMemberRow`
  - `constructors (1)`; `<Clone>$`; `Equals (2 overloads)`; `GetHashCode`; `Key`; `op_Equality`; `op_Inequality`; `ToString`; `TreeId`
- `Orleans.Lattice.Explorer.Core.Data.ValueFormat`
  - `Empty`; `Hex`; `Json`; `Text`; `value__`
- `Orleans.Lattice.Explorer.Core.Data.ValueRenderer`
  - `HexDump`; `MaxJsonAutoFormatBytes`; `Render`
- `Orleans.Lattice.Explorer.Core.DeadLetter.DeadLetterEntry`
  - `constructors (1)`; `<Clone>$`; `Equals (2 overloads)`; `From`; `GetHashCode`; `Key`; `op_Equality`; `op_Inequality`; `Reason`; `Source`
    `TimestampUtc`; `ToString`; `Truncated`; `Value`; `ValueByteLength`
- `Orleans.Lattice.Explorer.Core.DeadLetter.DeadLetterPage`
  - `constructors (1)`; `<Clone>$`; `ContinuationToken`; `Empty`; `Entries`; `Equals (2 overloads)`; `GetHashCode`; `HasMore`; `op_Equality`
    `op_Inequality`; `ToString`
- `Orleans.Lattice.Explorer.Core.DeadLetter.DeadLetterReader`
  - `constructors (1)`; `CountAsync`; `ListAsync`
- `Orleans.Lattice.Explorer.Core.DeadLetter.DeadLetterSource`
  - `LocalRejected`; `Replication`; `Restore`; `Unknown`; `value__`
- `Orleans.Lattice.Explorer.Core.DeadLetter.ExplorerDeadLetterServiceCollectionExtensions`
  - `AddExplorerDeadLetter`
- `Orleans.Lattice.Explorer.Core.DeadLetter.IDeadLetterReader`
  - `CountAsync`; `ListAsync`
- `Orleans.Lattice.Explorer.Core.ExplorerInfo`
  - `ApplicationName`; `Description`; `DisplayName`
- `Orleans.Lattice.Explorer.Core.History.ExplorerHistoryServiceCollectionExtensions`
  - `AddExplorerHistory`
- `Orleans.Lattice.Explorer.Core.History.HistoryDiffLine`
  - `constructors (1)`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `Kind`; `op_Equality`; `op_Inequality`; `Text`; `ToString`
- `Orleans.Lattice.Explorer.Core.History.HistoryDiffLineKind`
  - `Added`; `Removed`; `Unchanged`; `value__`
- `Orleans.Lattice.Explorer.Core.History.HistoryLiveFollower`
  - `constructors (1)`; `FollowAsync`
- `Orleans.Lattice.Explorer.Core.History.HistoryLiveTail`
  - `constructors (1)`; `Covers`; `Key`; `MarkSeen`; `SeenCount`; `TryAccept`
- `Orleans.Lattice.Explorer.Core.History.HistoryMemberChange`
  - `constructors (1)`; `<Clone>$`; `ElementFormat`; `ElementText`; `Equals (2 overloads)`; `From`; `GetHashCode`; `Kind`; `op_Equality`
    `op_Inequality`; `Ordinal`; `ReplicaId`; `ToString`
- `Orleans.Lattice.Explorer.Core.History.HistoryPage`
  - `constructors (1)`; `<Clone>$`; `Bound`; `ContinuationToken`; `EarliestAvailable`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`
    `op_Inequality`; `Revisions`; `Status`; `ToString`
- `Orleans.Lattice.Explorer.Core.History.HistoryReader`
  - `constructors (1)`; `HistoryPreviewBudget`; `LoadAsync`
- `Orleans.Lattice.Explorer.Core.History.HistoryRevisionRow`
  - `constructors (1)`; `<Clone>$`; `Category`; `Diff`; `EndKey`; `Equals (2 overloads)`; `From`; `FromLive`; `GetHashCode`; `Hlc`; `IsLiveTail`
    `IsSnapshot`; `Kind`; `MemberChanges`; `Mode`; `op_Equality`; `op_Inequality`; `OriginClusterId`; `Position`; `RenderMode`; `RetentionChange`
    `RetentionMode`; `ToString`; `Truncated`; `Value`; `ValueHash`; `ValueLength`; `ValueRetained`
- `Orleans.Lattice.Explorer.Core.History.HistoryRowRenderMode`
  - `CrdtMembers`; `Delete`; `LiveTail`; `MetadataOnly`; `RangeTombstone`; `value__`; `ValueDiff`
- `Orleans.Lattice.Explorer.Core.History.HistoryTimeline`
  - `constructors (1)`; `<Clone>$`; `ActiveRetentionMode`; `ActiveValueRetained`; `Bound`; `Build`; `ContinuationToken`; `EarliestAvailable`
    `Equals (2 overloads)`; `GetHashCode`; `HasMore`; `HasRows`; `Key`; `op_Equality`; `op_Inequality`; `Rows`; `Status`; `ToString`; `TreeId`
- `Orleans.Lattice.Explorer.Core.History.HistoryValueDiff`
  - `Compute`
- `Orleans.Lattice.Explorer.Core.History.IHistoryLiveFollower`
  - `FollowAsync`
- `Orleans.Lattice.Explorer.Core.History.IHistoryReader`
  - `LoadAsync`
- `Orleans.Lattice.Explorer.Core.History.RetentionTransition`
  - `Describe`; `Equals (2 overloads)`; `From`; `FromValueRetained`; `GetHashCode`; `Label`; `op_Equality`; `op_Inequality`; `To`; `ToString`
    `ToValueRetained`
- `Orleans.Lattice.Explorer.Core.Metrics.ExplorerMetricsServiceCollectionExtensions`
  - `AddExplorerMetrics`
- `Orleans.Lattice.Explorer.Core.Metrics.IMetricsReader`
  - `GetAsync`
- `Orleans.Lattice.Explorer.Core.Metrics.MetricsReader`
  - `constructors (1)`; `GetAsync`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerNavigationRequest`
  - `constructors (1)`; `Address`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`; `Replace`; `ToString`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerNavigationServiceCollectionExtensions`
  - `AddExplorerNavigation`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerRoute`
  - `<Clone>$`; `AllTenants`; `Area`; `Equals (2 overloads)`; `GetHashCode`; `HasSelection`; `Home`; `Id`; `IsBare`; `Kind`; `op_Equality`
    `op_Inequality`; `Parameters`; `Root`; `Surface`; `Tenant`; `ToString`; `WithAllTenants`; `WithArea`; `WithKind`; `WithoutSelection`
    `WithParameter`; `WithParameters`; `WithSelection`; `WithSurface`; `WithTenant`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteParameter`
  - `constructors (1)`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `Key`; `op_Equality`; `op_Inequality`; `ToString`; `Value`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteParameters`
  - `Count`; `Create`; `Empty`; `Equals (2 overloads)`; `GetEnumerator`; `GetHashCode`; `GetValueOrEmpty`; `Item`; `TryGetValue`; `With`; `Without`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteParameters.Enumerator`
  - `Current`; `MoveNext`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteParseResult`
  - `constructors (1)`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `IsUnderstood`; `op_Equality`; `op_Inequality`; `Route`
    `ShouldCanonicalize`; `Status`; `ToString`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerRoutePath`
  - `Format`; `Parse`; `RootPath`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteSegments`
  - `AllTenantsQueryKey`; `AreaPathPrefix`; `Explore`; `TagIndexes`; `TenantQueryKey`; `Trees`; `TrueValue`; `Views`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteSlug`
  - `EnsureCanonical`; `FromIdentifier`; `IsCanonical`; `Normalize`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteStatus`
  - `Bare`; `Canonical`; `Malformed`; `Normalized`; `value__`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerShellEntry`
  - `constructors (1)`; `Action`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`; `Route`; `ToString`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerShellEntryAction`
  - `Canonicalize`; `RestoreRemembered`; `ShowAddress`; `value__`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerShellEntryPolicy`
  - `Decide`
- `Orleans.Lattice.Explorer.Core.Navigation.ExplorerShellRouter`
  - `constructors (1)`; `Canonicalize`; `Current`; `NavigateTo`; `NavigationRequested`; `RouteChanged`; `SetAddress`; `Status`
- `Orleans.Lattice.Explorer.Core.Navigation.IExplorerShellRouter`
  - `Canonicalize`; `Current`; `NavigateTo`; `NavigationRequested`; `RouteChanged`; `SetAddress`; `Status`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerPreferenceCatalog`
  - `constructors (2)`; `Contains`; `Keys`; `Register`; `TryGet`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerPreferenceFallbackReason`
  - `None`; `NotLoaded`; `NotResolvable`; `NotStored`; `value__`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerPreferenceKey`
  - `constructors (1)`; `Description`; `Name`; `Scope`; `ToString`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerPreferenceKeys`
  - `ActiveArea`; `ActiveTenant`; `All`; `AllTenantsVisible`; `CatalogKind`; `DetailSurface`; `Selection`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerPreferenceResolution`1`
  - `constructors (1)`; `Abandoned`; `Deconstruct`; `Equals (2 overloads)`; `Explanation`; `FellBack`; `GetHashCode`; `IsRestored`; `op_Equality`
    `op_Inequality`; `Reason`; `Restored`; `ToString`; `Value`; `WasAbandoned`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerPreferenceScope`
  - `User`; `UserAndCluster`; `value__`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerPreferenceScopeIdentity`
  - `constructors (1)`; `Anonymous`; `Cluster`; `Deconstruct`; `Empty`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`
    `ToScopeToken`; `ToString`; `Unconfigured`; `User`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerPreferenceScopeProvider`
  - `constructors (1)`; `Current`; `Dispose`; `ScopeChanged`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerSessionServiceCollectionExtensions`
  - `AddExplorerSession`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerShellEntryGate`
  - `constructors (1)`; `TryClaimEntry`
- `Orleans.Lattice.Explorer.Core.Session.ExplorerShellPreferences`
  - `constructors (1)`; `Changed`; `ClearAsync`; `Dispose`; `EnsureLoadedAsync`; `GetOrDefault`; `GetRememberedRoute`; `IsLoaded`; `Keys`
    `RememberRouteAsync`; `ResetAsync`; `Resolve (2 overloads)`; `RestoreAsync`; `SetAsync`
- `Orleans.Lattice.Explorer.Core.Session.IExplorerPreferenceCatalog`
  - `Contains`; `Keys`; `Register`; `TryGet`
- `Orleans.Lattice.Explorer.Core.Session.IExplorerPreferenceScopeProvider`
  - `Current`; `ScopeChanged`
- `Orleans.Lattice.Explorer.Core.Session.IExplorerShellEntryGate`
  - `TryClaimEntry`
- `Orleans.Lattice.Explorer.Core.Session.IExplorerShellPreferences`
  - `Changed`; `ClearAsync`; `EnsureLoadedAsync`; `GetOrDefault`; `GetRememberedRoute`; `IsLoaded`; `Keys`; `RememberRouteAsync`; `ResetAsync`
    `Resolve (2 overloads)`; `RestoreAsync`; `SetAsync`
- `Orleans.Lattice.Explorer.Core.Session.InMemoryUiPreferenceBackingStore`
  - `constructors (1)`; `GetAsync`; `RemoveAsync`; `SetAsync`
- `Orleans.Lattice.Explorer.Core.Session.IUiPreferenceBackingStore`
  - `GetAsync`; `RemoveAsync`; `SetAsync`
- `Orleans.Lattice.Explorer.Core.Session.IUiPreferenceStore`
  - `EnsureLoadedAsync`; `GarbageCollectAsync`; `GetOrDefault`; `IsLoaded`; `RemoveAsync`; `SetAsync`; `TryGet`
- `Orleans.Lattice.Explorer.Core.Session.IUiSessionStore`
  - `GetOrDefault`; `Remove`; `Set`; `TryGet`
- `Orleans.Lattice.Explorer.Core.Session.UiPreferenceStore`
  - `constructors (2)`; `BackingKey`; `DefaultRetention`; `Dispose`; `EnsureLoadedAsync`; `GarbageCollectAsync`; `GetOrDefault`; `IsLoaded`
    `RemoveAsync`; `SetAsync`; `TryGet`
- `Orleans.Lattice.Explorer.Core.Session.UiSessionStore`
  - `constructors (1)`; `GetOrDefault`; `Remove`; `Set`; `TryGet`
- `Orleans.Lattice.Explorer.Core.Tenancy.ExplorerTenantId`
  - `constructors (1)`; `Default`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`; `ToString`; `Value`
- `Orleans.Lattice.Explorer.Core.Tenancy.ExplorerTenantNoticeKind`
  - `Applied`; `Refused`; `RestoreAbandoned`; `Unknown`; `value__`
- `Orleans.Lattice.Explorer.Core.Tenancy.ExplorerTenantScopeNotice`
  - `constructors (1)`; `<Clone>$`; `Applied`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `IsDenial`; `Kind`; `Message`; `op_Equality`
    `op_Inequality`; `Refused`; `RestoreAbandoned`; `ToString`; `Unknown`; `VisibilityApplied`
- `Orleans.Lattice.Explorer.Core.Tenancy.ExplorerTenantServiceCollectionExtensions`
  - `AddExplorerTenantView`
- `Orleans.Lattice.Explorer.Core.Tenancy.ExplorerTenantTrees`
  - `DefaultTenantId`; `IsOwnedBy`; `SegmentPrefix`; `TryGetOwner`
- `Orleans.Lattice.Explorer.Core.Tenancy.ExplorerTenantVisibility`
  - `ActiveTenant`; `AllTenants`; `value__`
- `Orleans.Lattice.Explorer.Core.Tenancy.IExplorerAccessibleTenantSource`
  - `GetAccessibleTenantsAsync`
- `Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantContext`
  - `ActiveTenant`; `RequestedVisibility`
- `Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantIdentityResolver`
  - `ResolveAsync`
- `Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantOperatorGate`
  - `IsPlatformOperatorAsync`
- `Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantScopeNotices`
  - `Clear`; `Current`; `Publish`
- `Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantScopeRefresher`
  - `RefreshAsync`
- `Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantSwitcher`
  - `ActiveTenant`; `IsActive`; `IsOperatorAsync`; `RequestedVisibility`; `SetVisibilityAsync`; `SwitchTenantAsync`
- `Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantView`
  - `ActiveTenant`; `IsActive`; `IsVisible`; `ResolveEffectiveVisibilityAsync`; `ScopeAsync`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerAccessCopy`
  - `Denied`; `Describe`; `For`; `SignInRequired`; `Unavailable`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerBadge`
  - `Count`; `DocsLink`; `Equals (2 overloads)`; `Expansion`; `Explanation`; `GetHashCode`; `IsAbbreviated`; `IsEmpty`; `IsMuted`; `Label`
    `op_Equality`; `op_Inequality`; `ShortText`; `Term`; `TermId`; `Text`; `ToString`; `Value`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerBadges`
  - `Active`; `Aggregation`; `CachedCountLimit`; `DeadLetterCount`; `ForCatalogItem`; `ForLifecycle`; `History`; `MaxCatalogBadges`
    `ProjectionProvider`; `ProjectionVersion`; `Purging`; `ShardCount`; `SoftDeleted`; `SourceTree`; `TagIndex`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerDocsLinks`
  - `Api`; `Compaction`; `Crdt`; `DeadLetterQueue`; `Explorer`; `HistoryViews`; `ManagingAccess`; `ManagingBackups`; `ManagingSchema`
    `MaterialisedViews`; `Metrics`; `OnlineReshard`; `ProjectionRebuild`; `RunningTheExplorer`; `SchemaEnforcement`; `SigningIn`; `Telemetry`
    `Tenancy`; `TreeDeletion`; `TreeRegistry`; `TreeSizing`; `TreeStructure`; `Wal`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerGlossary`
  - `Contains`; `Count`; `DocsLinkFor`; `ExplanationFor`; `Find`; `ForLifecycle`; `Get`; `LabelFor`; `Terms`; `TryGet`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerStateCopy`
  - `Empty`; `Failed`; `For`; `Loading`; `NotPermitted`; `ScopedOut`; `SignInRequired`; `Unavailable`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerStateKind`
  - `Empty`; `Failed`; `Loading`; `NotPermitted`; `ScopedOut`; `SignInRequired`; `Unavailable`; `value__`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerStateMessage`
  - `constructors (1)`; `<Clone>$`; `ActionLabel`; `DocsLink`; `Equals (2 overloads)`; `Explanation`; `GetHashCode`; `Headline`; `IsBusy`
    `IsDenial`; `Kind`; `op_Equality`; `op_Inequality`; `Remedy`; `RemedyLabel`; `TermId`; `ToString`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerSubject`
  - `CollectionLabel`; `DocsLink`; `Equals (2 overloads)`; `GetHashCode`; `Id`; `IsEmpty`; `op_Equality`; `op_Inequality`; `Plural`; `Singular`
    `TermId`; `ToString`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerSubjects`
  - `Backups`; `Changes`; `DeadLetters`; `DetailSurfaces`; `Entries`; `ForCatalogKind`; `Grants`; `Metrics`; `SchemaVersions`; `Shards`
    `TagIndexes`; `TelemetrySignals`; `Tenants`; `Trees`; `Views`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerTerm`
  - `constructors (1)`; `<Clone>$`; `DocsLink`; `Equals (2 overloads)`; `Explanation`; `GetHashCode`; `HasDocsLink`; `Id`; `Label`; `op_Equality`
    `op_Inequality`; `ToString`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerTermIds`
  - `ActiveTenant`; `AdminSubject`; `AggregationView`; `AllTenants`; `Compaction`; `Crdt`; `DeadLetter`; `DeadLetterCount`; `DefaultTenant`; `Grant`
    `HistoryView`; `Leaf`; `LifecycleActive`; `LifecyclePurging`; `LifecycleSoftDeleted`; `MyTenantArea`; `NotAvailableHere`; `ProjectionProvider`
    `ProjectionVersion`; `Quota`; `Region`; `Reshard`; `Residency`; `Shard`; `ShardCount`; `SignInRequired`; `SourceTree`; `StrictSchema`
    `TagIndexes`; `Tenant`; `TenantAdministrationArea`; `Trees`; `Views`; `Wal`
- `Orleans.Lattice.Explorer.Core.Vocabulary.ExplorerVocabulary`
  - `AccessArea`; `ActiveTenantLabel`; `AllTenantsLabel`; `BackupsArea`; `CatalogLabel`; `ClearScopeAction`; `ExploreArea`; `FormatActiveTenant`
    `GrantAudience`; `MyTenantArea`; `NoSelectionExplanation`; `NoSelectionHeadline`; `RemedyLabel`; `RetryAction`; `SignInAction`
    `TagIndexesLabel`; `TelemetryArea`; `TenantAdministrationArea`; `TenantAdministrationAreaShort`; `TenantGrantAudience`; `TreesLabel`
    `ViewsLabel`; `ViewsLongLabel`

#### `Orleans.Lattice.Explorer.UI` (187 exported types)
- `Orleans.Lattice.Explorer.UI._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessExplainPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessGroupPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessGroupsPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessMembersPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessNav`
  - `constructors (1)`; `Current`; `Delegated`; `Tenant`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessPostureBanner`
  - `constructors (1)`; `Model`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessRuleEditor`
  - `constructors (1)`; `Model`; `OnCancel`; `OnSaved`; `Rule`; `Tenant`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessRulePage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessRulesPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessRuleTable`
  - `constructors (1)`; `Caption`; `CaptionHidden`; `DecidingRuleId`; `EmptyText`; `Rules`; `Tenant`; `TenantRules`
- `Orleans.Lattice.Explorer.UI.Areas.Access.AccessSubjectPicker`
  - `constructors (1)`; `AllowKindChange`; `ConfirmAsync`; `DirectoryAvailable`; `DirectoryExplanation`; `DirectorySearchDenied`; `Disabled`
    `Error`; `Id`; `IdChanged`; `Kind`; `KindChanged`; `Label`; `OnPrincipalSelected`; `SubjectKind`; `SubjectKindChanged`; `Tenant`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Explain.TenantExplainLayers`
  - `constructors (1)`; `Explanation`; `Tenant`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Explain.TenantExplainView`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups.TenantGroupDeleteDialog`
  - `constructors (1)`; `Name`; `OnDeleted`; `Open`; `OpenChanged`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups.TenantGroupDetailView`
  - `constructors (1)`; `Name`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups.TenantGroupHistory`
  - `constructors (1)`; `Name`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups.TenantGroupsView`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Members.TenantMembersView`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules.TenantRuleDetailView`
  - `constructors (1)`; `RuleId`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules.TenantRuleEditor`
  - `constructors (1)`; `DirectoryAvailable`; `DirectoryExplanation`; `DirectorySearchDenied`; `Dispose`; `OnCancel`; `OnSaved`; `Rule`; `Rules`
    `Tenant`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules.TenantRuleHistory`
  - `constructors (1)`; `Dispose`; `Rule`; `Tenant`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules.TenantRulesView`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules.TenantRuleTable`
  - `constructors (1)`; `Caption`; `CaptionHidden`; `Editable`; `EmptyText`; `Rules`; `Tenant`
- `Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.TenantAccessView`
  - `Dispose`; `Tenant`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.App.AppPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppCeilingStep`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppIcon`
  - `constructors (1)`; `DataUrl`; `Large`; `Slug`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppInstallSteps`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppLifecycleActions`
  - `constructors (1)`; `Dispose`; `OnChanged`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppReviewDetails`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppReviewPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppRoleBindingConfirmStep`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppRoleBindingStep`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppRoleHoldingNotice`
  - `constructors (1)`; `AppName`; `Compact`; `Enabled`; `OnCheckAgain`; `RebindHref`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppsCataloguePage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppsPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppsViewLinks`
  - `constructors (1)`; `CatalogueCurrent`; `CatalogueHref`; `ShowCatalogue`; `YourAppsHref`
- `Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppUpgradeDiffView`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Backups._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupCapturePage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupHealthPage`
  - `constructors (1)`; `Dispose`; `PageSize`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupHealthPill`
  - `constructors (1)`; `Pending`; `Report`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupMaintenancePage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupOperationPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupsCataloguePage`
  - `constructors (1)`; `Dispose`; `PageSize`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupSchedulesPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupsNav`
  - `constructors (1)`; `BackText`; `CataloguePage`; `Current`; `Dispose`; `HealthPage`; `MaintenancePage`; `SchedulesPage`
- `Orleans.Lattice.Explorer.UI.Areas.Backups.BackupTreeLabel`
  - `constructors (1)`; `AppLabel`; `Mono`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterOrphansPage`
  - `constructors (1)`; `Dispose`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterOverview`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterPage`
  - `constructors (1)`; `P7`; `P8`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterRegionDiagram`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterReshardPage`
  - `constructors (1)`; `Dispose`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterResizePage`
  - `constructors (1)`; `Dispose`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterSnapshotPage`
  - `constructors (1)`; `Dispose`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterToolsPage`
  - `constructors (1)`; `Dispose`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreeConfiguration`
  - `constructors (1)`; `Capabilities`; `Dispose`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreeHeader`
  - `constructors (1)`; `ChildContent`; `Heading`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreeLifecycle`
  - `constructors (1)`; `Capabilities`; `Dispose`; `OperationSettled`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreeLink`
  - `constructors (1)`; `Text`; `TreeId`; `View`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreeList`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreePage`
  - `constructors (1)`; `Dispose`; `Tab`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreeShards`
  - `constructors (1)`; `Capabilities`; `Dispose`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreeStorage`
  - `constructors (1)`; `Capabilities`; `Dispose`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterTreeSummary`
  - `constructors (1)`; `Capabilities`; `Dispose`; `OperationSettled`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterWalPage`
  - `constructors (1)`; `Dispose`; `Partition`; `Target`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterWalPartitions`
  - `constructors (1)`; `Partitions`
- `Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterWalReclamation`
  - `constructors (1)`; `Dispose`; `TreeId`
- `Orleans.Lattice.Explorer.UI.Areas.Data._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Data.DataDeadLettersPanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Data.DataDirectoryPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Data.DataEntryView`
  - `constructors (1)`; `Dispose`; `Key`; `Version`
- `Orleans.Lattice.Explorer.UI.Areas.Data.DataHistoryPanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Data.DataKeysPanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Data.DataMetricsPanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Data.DataTagIndexesPanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Data.DataTreePage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Data.DataViewsPanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Replication._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Replication.ReplicationEstatePage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Replication.ReplicationLinkTable`
  - `constructors (1)`; `Caption`; `CaptionHidden`; `EmptyText`; `Links`; `ShowTree`
- `Orleans.Lattice.Explorer.UI.Areas.Replication.ReplicationMap`
  - `constructors (1)`; `Filtered`; `Links`; `LocalRegionId`
- `Orleans.Lattice.Explorer.UI.Areas.Replication.ReplicationSections`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Replication.ReplicationToolbar`
  - `constructors (1)`; `Apps`; `Regions`
- `Orleans.Lattice.Explorer.UI.Areas.Replication.ReplicationTreePage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Replication.ReplicationTreesPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Schema._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Schema.SchemaCompliancePanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Schema.SchemaDeadLettersPanel`
  - `constructors (1)`; `Dispose`; `KeyCharacters`; `PageSize`
- `Orleans.Lattice.Explorer.UI.Areas.Schema.SchemaDirectoryPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Schema.SchemaOperationStatus`
  - `constructors (1)`; `Dispose`; `PollInterval`
- `Orleans.Lattice.Explorer.UI.Areas.Schema.SchemaPolicyPanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Schema.SchemaRemediationPanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Schema.SchemaRuleBuilder`
  - `constructors (1)`; `Busy`; `Dispose`; `OnCancel`; `OnSave`; `Policy`; `SaveError`
- `Orleans.Lattice.Explorer.UI.Areas.Schema.SchemaTreePage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Schema.SchemaVersionsPanel`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Telemetry._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Telemetry.TelemetryChart`
  - `constructors (1)`; `DataHref`; `Descriptor`; `Dispose`; `Generation`; `Note`; `OnDenied`; `OnScope`; `Problem`; `Request`; `ShowTable`
    `TableHref`; `TreeHref`
- `Orleans.Lattice.Explorer.UI.Areas.Telemetry.TelemetryPage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.MyTenantPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyAccessPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyDirectoryPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyGrants`
  - `constructors (1)`; `OpenOfferOnLoad`; `TenantId`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyGrantsPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyMembers`
  - `constructors (1)`; `Dispose`; `TenantId`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyNav`
  - `constructors (1)`; `Current`; `Own`; `Tenant`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyQuota`
  - `constructors (1)`; `CanEdit`; `TenantId`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyQuotaPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyRegions`
  - `constructors (1)`; `CanAuthorize`; `Dispose`; `TenantId`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyRegionsPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Areas.Tenancy.TenancyTenantPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Design.Components.ILtSuggestionSource`
  - `SuggestAsync`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtButton`
  - `constructors (1)`; `AdditionalAttributes`; `ChildContent`; `Disabled`; `OnClick`; `Pressed`; `Type`; `Variant`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtButtonType`
  - `Button`; `Submit`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtButtonVariant`
  - `Destructive`; `Outlined`; `Quiet`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtChain`
  - `constructors (1)`; `Label`; `Links`; `Mono`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtChainLink`
  - `constructors (1)`; `<Clone>$`; `Deconstruct`; `Equals (2 overloads)`; `GetHashCode`; `Href`; `op_Equality`; `op_Inequality`; `Text`; `ToString`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtCheckbox`
  - `constructors (1)`; `AdditionalAttributes`; `Checked`; `CheckedChanged`; `Disabled`; `Hint`; `Label`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtColumn`1`
  - `constructors (1)`; `Align`; `ChildContent`; `Dispose`; `FullText`; `Mono`; `RowHeader`; `SortBy`; `Title`; `Value`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtColumnAlign`
  - `End`; `Start`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtComboBox`
  - `constructors (1)`; `AdditionalAttributes`; `ConfirmAsync`; `DefaultLimit`; `Disabled`; `DisposeAsync`; `Error`; `ExistingMessage`; `FocusAsync`
    `Hint`; `InputId`; `Label`; `Leading`; `Limit`; `Mode`; `Mono`; `Noun`; `OnChoose`; `OnCommit`; `OnDismiss`; `OpenOnFocus`; `Placeholder`
    `ReadOnly`; `RejectExisting`; `Source`; `Value`; `ValueChanged`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtComboBoxMode`
  - `PickExisting`; `Suggest`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtCompactRow`
  - `constructors (1)`; `Mono`; `Primary`; `Secondary`; `State`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtConfirmDestructive`
  - `constructors (1)`; `ChildContent`; `ConfirmText`; `ObjectKind`; `ObjectName`; `OnConfirm`; `Open`; `OpenChanged`; `ReturnFocus`; `Title`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtDateTimeInput`
  - `constructors (1)`; `AllowFuture`; `ConfirmAsync`; `Disabled`; `DisposeAsync`; `EmptyText`; `Error`; `FocusAsync`; `Hint`; `InputId`; `Label`
    `Max`; `Min`; `QuickPicks`; `Value`; `ValueChanged`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtDefinition`
  - `constructors (1)`; `ChildContent`; `Mono`; `Term`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtDefinitionList`
  - `constructors (1)`; `ChildContent`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtDesignHead`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtDialog`
  - `constructors (1)`; `Actions`; `Alert`; `AutoFocus`; `ChildContent`; `CloseAsync`; `Description`; `DismissOnEscape`; `Open`; `OpenChanged`
    `Placement`; `ReturnFocus`; `ShowClose`; `Title`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtDialogPlacement`
  - `Center`; `End`; `Start`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtDurationInput`
  - `constructors (1)`; `ConfirmAsync`; `Disabled`; `Error`; `Hint`; `InputId`; `Label`; `Max`; `Min`; `Optional`; `Units`; `Value`; `ValueChanged`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtDurationUnits`
  - `Days`; `Hours`; `Minutes`; `None`; `Seconds`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtEmptyState`
  - `constructors (1)`; `Actions`; `ChildContent`; `HeadingLevel`; `Title`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtMark`
  - `constructors (1)`; `Label`; `Size`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtMonoCell`
  - `constructors (1)`; `CopyLabel`; `Truncate`; `Value`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtMultiComboBox`
  - `constructors (1)`; `ConfirmAsync`; `Disabled`; `Error`; `Hint`; `Label`; `Limit`; `Mode`; `Mono`; `Noun`; `Placeholder`; `Source`; `Values`
    `ValuesChanged`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtNameInput`
  - `constructors (1)`; `AdditionalAttributes`; `CheckFailedNote`; `ConfirmAsync`; `Disabled`; `Dispose`; `Error`; `Existing`; `ExistingMessage`
    `FocusAsync`; `Hint`; `InputId`; `Label`; `Mono`; `Noun`; `Placeholder`; `ReadOnly`; `RejectExisting`; `Validate`; `Value`; `ValueChanged`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtNode`
  - `constructors (1)`; `Kind`; `Label`; `Size`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtNodeKind`
  - `Concurrent`; `Filled`; `Hollow`; `Join`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtNodeSize`
  - `Large`; `Small`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtProgress`
  - `constructors (1)`; `AnnouncePhase`; `Detail`; `IsDeterminate`; `Label`; `Maximum`; `MaximumSegments`; `Percent`; `Phase`; `Stepped`; `StepText`
    `Value`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSearchInput`
  - `constructors (1)`; `AdditionalAttributes`; `Disabled`; `KeyShortcut`; `Label`; `Landmark`; `OnSubmit`; `Placeholder`; `Value`; `ValueChanged`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSelect`
  - `constructors (1)`; `AdditionalAttributes`; `Disabled`; `Hint`; `Label`; `Options`; `Value`; `ValueChanged`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSelectOption`
  - `constructors (1)`; `<Clone>$`; `Deconstruct`; `Disabled`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`; `Text`
    `ToString`; `Value`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSkeleton`
  - `constructors (1)`; `Label`; `Lines`; `MaximumLines`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSortDirection`
  - `Ascending`; `Descending`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSpine`
  - `constructors (1)`; `ChildContent`; `Label`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSpineStop`
  - `constructors (1)`; `Current`; `Href`; `Text`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtStatusPill`
  - `constructors (1)`; `State`; `Text`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSuggestion`
  - `constructors (1)`; `<Clone>$`; `Current`; `Deconstruct`; `Detail`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`
    `ToString`; `Value`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSuggestionSet`
  - `Empty`; `Find`; `IsAvailable`; `Items`; `Of`; `Truncated`; `Unavailable`; `UnavailableReason`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtSwitch`
  - `constructors (1)`; `AdditionalAttributes`; `Checked`; `CheckedChanged`; `Disabled`; `Hint`; `Label`; `OffText`; `OnText`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtTab`
  - `constructors (1)`; `ChildContent`; `Disabled`; `Dispose`; `Id`; `Title`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtTable`1`
  - `constructors (1)`; `Caption`; `CaptionHidden`; `ChildContent`; `Compact`; `CompactRow`; `CompactRowHeight`; `Detail`; `DetailActions`
    `DetailTitle`; `EmptyContent`; `IsCurrent`; `Items`; `RowHeight`; `RowKey`; `Virtualize`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtTableCompact`
  - `List`; `ScrollFrame`; `value__`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtTabs`
  - `constructors (1)`; `ActiveId`; `ActiveIdChanged`; `ChildContent`; `DisposeAsync`; `Label`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtTextInput`
  - `constructors (1)`; `AdditionalAttributes`; `Disabled`; `Error`; `FocusAsync`; `Hint`; `InputId`; `Label`; `Mono`; `Placeholder`; `ReadOnly`
    `Value`; `ValueChanged`
- `Orleans.Lattice.Explorer.UI.Design.Components.LtToastRegion`
  - `constructors (1)`; `Dispose`; `Label`
- `Orleans.Lattice.Explorer.UI.Design.Tokens.LtStateRole`
  - `Disabled`; `Drift`; `Enabled`; `Failed`; `Healthy`; `Installed`; `Lagging`; `Stalled`; `Uninstalled`; `Unknown`; `value__`
- `Orleans.Lattice.Explorer.UI.Framing.AppFrame`
  - `constructors (1)`; `AppSlug`; `DisposeAsync`; `Failure`; `LeaveHref`; `NotifyContextChangedAsync`; `OnEscape`; `OnFailure`; `OnLeave`
    `OnNavSync`; `Path`
- `Orleans.Lattice.Explorer.UI.Framing.AppFrameFailure`
  - `BundleDigestMismatch`; `BundleInvalid`; `DigestMismatch`; `FrameFailed`; `HandshakeTimeout`; `NoGrant`; `NoUi`; `ProtocolUnsupported`
    `Reloaded`; `Revoked`; `Unavailable`; `value__`
- `Orleans.Lattice.Explorer.UI.Layout._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Layout.AddressLine`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Layout.AppearanceControls`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Layout.AppearanceMenu`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Layout.DirectorySpine`
  - `constructors (1)`; `OnNavigate`; `Rail`
- `Orleans.Lattice.Explorer.UI.Layout.ExplorerPage`
  - `P1`; `P2`; `P3`; `P4`; `P5`; `P6`; `SetParametersAsync`; `Tenant`
- `Orleans.Lattice.Explorer.UI.Layout.ShellLayout`
  - `constructors (1)`; `DisposeAsync`
- `Orleans.Lattice.Explorer.UI.Layout.TenantSwitcher`
  - `constructors (1)`; `Dispose`; `Stacked`
- `Orleans.Lattice.Explorer.UI.Operations.LtOperationProgress`
  - `constructors (1)`; `Label`; `PhaseName`; `Status`
- `Orleans.Lattice.Explorer.UI.Pages._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Pages.HomePage`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Pages.NotFoundPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Session._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Session.ConnectionDialog`
  - `constructors (1)`; `AllowCancel`; `Initial`; `OnCancelled`; `OnSaved`
- `Orleans.Lattice.Explorer.UI.Session.ConnectionIndicator`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Session.IdentityMenu`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Session.ReauthInterstitial`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Session.ResetPage`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.UI.Session.SessionOverlay`
  - `constructors (1)`; `Dispose`
- `Orleans.Lattice.Explorer.UI.Session.SignInDialog`
  - `constructors (1)`; `OnClosed`

#### `Orleans.Lattice.Explorer.Web` (8 exported types)
- `Orleans.Lattice.Explorer.Web.AuthEndpoints`
  - `MapExplorerAuthEndpoints`
- `Orleans.Lattice.Explorer.Web.Components._Imports`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.Web.Components.App`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.Web.Components.Routes`
  - `constructors (1)`
- `Orleans.Lattice.Explorer.Web.CookieCredentialStore`
  - `constructors (2)`; `ClearAsync`; `GetAsync`; `SetAsync`
- `Orleans.Lattice.Explorer.Web.LatticeExplorerWebEndpointRouteBuilderExtensions`
  - `MapLatticeExplorer`
- `Orleans.Lattice.Explorer.Web.LatticeExplorerWebOptions`
  - `constructors (1)`; `AllowEnvironmentCredentialSeed`; `AllowInteractiveEndpointConfiguration`; `BasePath`; `ConfigFilePath`
    `ConfigureDataProtection`; `DataProtectionApplicationName`; `DataProtectionKeyRingBlobUri`; `DataProtectionKeyRingCredential`
    `UseEnvironmentBootstrap`
- `Orleans.Lattice.Explorer.Web.LatticeExplorerWebServiceCollectionExtensions`
  - `AddLatticeExplorerWeb`

#### `Orleans.Lattice.Explorer.AppKit` (9 exported types)
- `Orleans.Lattice.Explorer.AppKit.AppKitProtocol`
  - `AssetDirectory`; `FrameDocument`; `TreeNamePattern`; `Version`
- `Orleans.Lattice.Explorer.AppKit.AppKitProtocol.DataActions`
  - `All`; `Delete`; `Get`; `Scan`; `Set`
- `Orleans.Lattice.Explorer.AppKit.AppKitProtocol.ErrorCodes`
  - `All`; `Conflict`; `Denied`; `Invalid`; `NotFound`; `RateLimited`; `TooLarge`; `Unavailable`
- `Orleans.Lattice.Explorer.AppKit.AppKitProtocol.Events`
  - `All`; `ContextChanged`; `NavChanged`; `Revoked`
- `Orleans.Lattice.Explorer.AppKit.AppKitProtocol.FailureCodes`
  - `All`; `AssetMissing`; `BundleDigestMismatch`; `BundleMalformed`; `BundleTooLarge`; `CryptoUnavailable`; `DigestMismatch`; `Internal`
    `LoadFailed`; `ProtocolUnsupported`
- `Orleans.Lattice.Explorer.AppKit.AppKitProtocol.Limits`
  - `DefaultTimeoutMilliseconds`; `MaxContinuationLength`; `MaxKeyLength`; `MaxNotifyLength`; `MaxPageSize`; `MaxPathLength`; `MaxRequestBytes`
    `MaxResponseBytes`; `MaxRoleNameLength`; `MaxRoles`; `MaxTimeoutMilliseconds`; `MaxTreeNameLength`; `MaxValueBase64Length`; `MaxValueBytes`
- `Orleans.Lattice.Explorer.AppKit.AppKitProtocol.Messages`
  - `All`; `Bundle`; `Failed`; `Hello`; `Loaded`; `Ready`
- `Orleans.Lattice.Explorer.AppKit.AppKitProtocol.Operations`
  - `All`; `ContextRead`; `ContextUser`; `DataDelete`; `DataRead`; `DataWrite`; `NavSync`; `UiNotify`
- `Orleans.Lattice.Explorer.AppKit.AppKitProtocol.RevokedReasons`
  - `All`; `Closed`; `Disabled`; `Revision`; `Uninstalled`; `Upgraded`


## See also

- [Explorer architecture](architecture.md)
- [Configuration](configuration.md)
- [Explorer areas](areas.md)
- [Lattice Apps](lattice-apps.md)
