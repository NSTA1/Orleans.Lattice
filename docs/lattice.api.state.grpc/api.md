---
agent_spec: "docs/agents/api/state.json"
---

# Public API

`Orleans.Lattice.Api.State.Grpc` publishes the remote half of the read-only state API: the `LatticeStateApiGrpcClient`, the binding-owned wire envelopes, the server options, the transport authorization seam and its implementations (including the turnkey environment-variable credential authorizer and its public password-hash format), the identity and auth-scheme advertisement seams, and the registration extensions. The gRPC service, its method definitions, marshallers and authorization interceptor are internal; they are described by behaviour in [Architecture](architecture.md).

Most RPCs carry the facade's own request and response records unchanged; those are listed in the [state facade's public API](../lattice.api.state/api.md). The RPC-by-RPC contract is in the [gRPC contract](../lattice.api.state/grpc-contract.md), and building and driving the client is covered in [Client](../lattice.api.state/client.md).

## Registration

| Method | What it registers |
|---|---|
| `AddLatticeStateApiGrpc(this IServiceCollection services, Action<LatticeStateApiGrpcOptions>? configure = null)` | The options, the default-deny `ILatticeStateApiAuthorizer`, the header-based `ILatticeStateApiCredentialBridge`, the options-backed `ILatticeStateApiAuthSchemeSource`, the authorization interceptor, and the service. Each seam is registered with `TryAdd`. |
| `MapLatticeStateApiGrpc(this IEndpointRouteBuilder endpoints)` | Maps the `orleans.lattice.api.state` service routes. |
| `AddEnvVarCredentialAuthorizer(this IServiceCollection services, Action<EnvVarCredentialAuthorizerOptions>? configure = null)` | Binds `EnvVarCredentialAuthorizerOptions`, registers `ProcessEnvironmentVariableReader` as the `IEnvironmentVariableReader` (with `TryAdd`), and **replaces** every previously registered `ILatticeStateApiAuthorizer` with `EnvVarCredentialAuthorizer`. |

The silo must also register the facade with `AddLatticeStateApi()`; see [Setup](../lattice.api.state/setup.md). Option values are documented in [Configuration](configuration.md).

## Client

`LatticeStateApiGrpcClient.Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` builds a client over a caller-supplied call invoker, resolving the per-message Orleans serializers from `serializerProvider`. The client has one method per RPC:

| Kind | Client methods |
|---|---|
| Unary | `ListTreesAsync`, `ListViewsAsync`, `ListTagIndexesAsync`, `ListTagValuesAsync`, `ListCoveredTreesAsync`, `ListIndexTagsAsync`, `ScanTagMembersAsync`, `GetTreeStructureAsync`, `ScanEntriesAsync`, `GetEntryAsync`, `GetEntryHistoryAsync`, `CancelScanAsync`, `GetMetricsSnapshotAsync`, `GetClusterInfoAsync`, `GetAuthSchemeAsync`, `GetDeadLetterCountAsync`, `ListDeadLettersAsync` |
| Server-streaming (`IAsyncEnumerable<T>`) | `ObserveChangesAsync`, `ObserveMetricsAsync` |

## Wire envelopes

The binding-owned records wrap scalar facade arguments and results, or add a `StateQueryStatus`, so the wire contract can grow additively: `EntryGetRequest` / `EntryGetResponse`, `EntryScanResponse`, `EntryScanCancelResponse`, `EntryHistoryResponse`, `StructureResponse`, `DeadLetterCountRequest` / `DeadLetterCountResponse`, and the auth-scheme records `AuthSchemeAdvertisementRequest`, `AuthSchemeAdvertisement` and `AuthSchemeDescriptor`. Their stable wire aliases are the constants in `GrpcStateTypeAliases`.

## Seams

| Seam | Default | Purpose |
|---|---|---|
| `ILatticeStateApiAuthorizer` | `DenyAllStateApiAuthorizer` | Coarse admission for every protected RPC, given a `LatticeStateApiAuthorizationContext`. Implementations shipped: `DenyAllStateApiAuthorizer`, `AllowAllStateApiAuthorizer`, `EnvVarCredentialAuthorizer`. |
| `ILatticeStateApiCredentialBridge` | header-based (internal) | Lifts the caller credential from request metadata onto the ambient Lattice credential used by read visibility. |
| `ILatticeStateApiAuthSchemeSource` | options-backed (internal) | Supplies the advertisement the unauthenticated `GetAuthScheme` RPC returns. |
| `IEnvironmentVariableReader` | `ProcessEnvironmentVariableReader` | Where `EnvVarCredentialAuthorizer` reads stored password hashes. |

`LatticePasswordHash` encodes, parses and verifies the `pbkdf2-sha256$<iterations>$<base64-salt>$<base64-derived-key>` hash format the environment-variable authorizer reads; `TryParse` yields a `LatticePasswordHashComponents`. See [Security](../lattice.api.state/security.md#turnkey-credential-authorizer).

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [State facade public API](../lattice.api.state/api.md)
- [gRPC contract](../lattice.api.state/grpc-contract.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.State.Grpc.EnvVarCredentialAuthorizerServiceCollectionExtensions`

[Source](../../src/lattice.api.state.grpc/EnvVarCredentialAuthorizerServiceCollectionExtensions.cs) (line 24).

`public static class EnvVarCredentialAuthorizerServiceCollectionExtensions`

- `public static IServiceCollection AddEnvVarCredentialAuthorizer(this IServiceCollection services, Action<EnvVarCredentialAuthorizerOptions>? configure = null)`

### `Orleans.Lattice.Api.State.Grpc.ILatticeStateApiCredentialBridge`

[Source](../../src/lattice.api.state.grpc/ILatticeStateApiCredentialBridge.cs) (line 37).

`public interface ILatticeStateApiCredentialBridge`

- `LatticeCredential? Resolve(ServerCallContext context)`

### `Orleans.Lattice.Api.State.Grpc.LatticeStateApiGrpcClient`

[Source](../../src/lattice.api.state.grpc/LatticeStateApiGrpcClient.cs) (line 22).

`public sealed class LatticeStateApiGrpcClient`

- `public static LatticeStateApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)`
- `public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)`
- `public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)`
- `public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)`
- `public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)`
- `public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)`
- `public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)`
- `public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default)`
- `public Task<AuthSchemeAdvertisement> GetAuthSchemeAsync(AuthSchemeAdvertisementRequest request, CancellationToken cancellationToken = default)`
- `public Task<DeadLetterCountResponse> GetDeadLetterCountAsync(DeadLetterCountRequest request, CancellationToken cancellationToken = default)`
- `public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default)`
- `public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default)`
- `public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.State.Grpc.LatticeStateApiGrpcOptions`

[Source](../../src/lattice.api.state.grpc/LatticeStateApiGrpcOptions.cs) (line 7).

`public sealed class LatticeStateApiGrpcOptions`

- `public bool RequireAuthorization`
- `public string CredentialHeaderName`
- `public string ActiveTenantHeaderName`
- `public string CredentialScheme`
- `public IList<AuthSchemeDescriptor> AdvertisedAuthSchemes`

### `Orleans.Lattice.Api.State.Grpc.LatticeStateApiGrpcServiceCollectionExtensions`

[Source](../../src/lattice.api.state.grpc/LatticeStateApiGrpcServiceCollectionExtensions.cs) (line 32).

`public static class LatticeStateApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeStateApiGrpc(this IServiceCollection services, Action<LatticeStateApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeStateApiGrpc(this IEndpointRouteBuilder endpoints)`

### `Orleans.Lattice.Api.State.Grpc.AuthSchemeAdvertisement`

[Source](../../src/lattice.api.state.grpc/Model/AuthSchemeAdvertisement.cs) (line 12).

`public sealed record AuthSchemeAdvertisement`

- `public IReadOnlyList<AuthSchemeDescriptor> Schemes`

### `Orleans.Lattice.Api.State.Grpc.AuthSchemeAdvertisementRequest`

[Source](../../src/lattice.api.state.grpc/Model/AuthSchemeAdvertisementRequest.cs) (line 11).

`public sealed record AuthSchemeAdvertisementRequest`

### `Orleans.Lattice.Api.State.Grpc.AuthSchemeDescriptor`

[Source](../../src/lattice.api.state.grpc/Model/AuthSchemeDescriptor.cs) (line 19).

`public sealed record AuthSchemeDescriptor`

- `public required string SchemeId`
- `public string DisplayName`
- `public IReadOnlyDictionary<string, string> Parameters`

### `Orleans.Lattice.Api.State.Grpc.DeadLetterCountRequest`

[Source](../../src/lattice.api.state.grpc/Model/DeadLetterCountRequest.cs) (line 11).

`public sealed record DeadLetterCountRequest`

- `public required string TreeId`

### `Orleans.Lattice.Api.State.Grpc.DeadLetterCountResponse`

[Source](../../src/lattice.api.state.grpc/Model/DeadLetterCountResponse.cs) (line 12).

`public sealed record DeadLetterCountResponse`

- `public required string TreeId`
- `public int Count`

### `Orleans.Lattice.Api.State.Grpc.EntryGetRequest`

[Source](../../src/lattice.api.state.grpc/Model/EntryGetRequest.cs) (line 11).

`public sealed record EntryGetRequest`

- `public required string TreeId`
- `public required string Key`

### `Orleans.Lattice.Api.State.Grpc.EntryGetResponse`

[Source](../../src/lattice.api.state.grpc/Model/EntryGetResponse.cs) (line 11).

`public sealed record EntryGetResponse`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public required string Key`
- `public EntryRecord? Entry`

### `Orleans.Lattice.Api.State.Grpc.EntryHistoryResponse`

[Source](../../src/lattice.api.state.grpc/Model/EntryHistoryResponse.cs) (line 13).

`public sealed record EntryHistoryResponse`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public required string Key`
- `public IReadOnlyList<EntryRevisionRecord> Revisions`
- `public string? ContinuationToken`
- `public EntryHistoryBound Bound`
- `public HybridLogicalClock EarliestAvailable`

### `Orleans.Lattice.Api.State.Grpc.EntryScanCancelResponse`

[Source](../../src/lattice.api.state.grpc/Model/EntryScanCancelResponse.cs) (line 13).

`public sealed record EntryScanCancelResponse`

### `Orleans.Lattice.Api.State.Grpc.EntryScanResponse`

[Source](../../src/lattice.api.state.grpc/Model/EntryScanResponse.cs) (line 12).

`public sealed record EntryScanResponse`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public IReadOnlyList<EntryRecord> Entries`
- `public string? ContinuationToken`

### `Orleans.Lattice.Api.State.Grpc.GrpcStateTypeAliases`

[Source](../../src/lattice.api.state.grpc/Model/GrpcStateTypeAliases.cs) (line 15).

`public static class GrpcStateTypeAliases`

- `public const string EntryGetRequest`
- `public const string StructureResponse`
- `public const string EntryScanResponse`
- `public const string EntryScanCancelResponse`
- `public const string EntryGetResponse`
- `public const string EntryHistoryResponse`
- `public const string AuthSchemeAdvertisementRequest`
- `public const string AuthSchemeDescriptor`
- `public const string AuthSchemeAdvertisement`
- `public const string DeadLetterCountRequest`
- `public const string DeadLetterCountResponse`

### `Orleans.Lattice.Api.State.Grpc.StructureResponse`

[Source](../../src/lattice.api.state.grpc/Model/StructureResponse.cs) (line 12).

`public sealed record StructureResponse`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public IReadOnlyList<NodeStateSummary> Roots`
- `public bool Truncated`

### `Orleans.Lattice.Api.State.Grpc.EnvVarCredentialAuthorizer`

[Source](../../src/lattice.api.state.grpc/Security/EnvVarCredentialAuthorizer.cs) (line 36).

`public sealed class EnvVarCredentialAuthorizer : ILatticeStateApiAuthorizer`

- `public EnvVarCredentialAuthorizer(IEnvironmentVariableReader environment, IOptionsMonitor<EnvVarCredentialAuthorizerOptions> options, ILogger<EnvVarCredentialAuthorizer> logger, TimeProvider? timeProvider = null)`
- `public Task<bool> IsAuthorizedAsync(LatticeStateApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.State.Grpc.EnvVarCredentialAuthorizerOptions`

[Source](../../src/lattice.api.state.grpc/Security/EnvVarCredentialAuthorizerOptions.cs) (line 9).

`public sealed class EnvVarCredentialAuthorizerOptions`

- `public string EnvironmentVariablePrefix`
- `public int MaxFailedAttempts`
- `public TimeSpan LockoutDuration`

### `Orleans.Lattice.Api.State.Grpc.IEnvironmentVariableReader`

[Source](../../src/lattice.api.state.grpc/Security/IEnvironmentVariableReader.cs) (line 8).

`public interface IEnvironmentVariableReader`

- `string? GetVariable(string name)`

### `Orleans.Lattice.Api.State.Grpc.ProcessEnvironmentVariableReader`

[Source](../../src/lattice.api.state.grpc/Security/IEnvironmentVariableReader.cs) (line 24).

`public sealed class ProcessEnvironmentVariableReader : IEnvironmentVariableReader`

- `public string? GetVariable(string name)`

### `Orleans.Lattice.Api.State.Grpc.ILatticeStateApiAuthSchemeSource`

[Source](../../src/lattice.api.state.grpc/Security/ILatticeStateApiAuthSchemeSource.cs) (line 15).

`public interface ILatticeStateApiAuthSchemeSource`

- `AuthSchemeAdvertisement GetAdvertisement()`

### `Orleans.Lattice.Api.State.Grpc.ILatticeStateApiAuthorizer`

[Source](../../src/lattice.api.state.grpc/Security/ILatticeStateApiAuthorizer.cs) (line 15).

`public interface ILatticeStateApiAuthorizer`

- `Task<bool> IsAuthorizedAsync(LatticeStateApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.State.Grpc.DenyAllStateApiAuthorizer`

[Source](../../src/lattice.api.state.grpc/Security/ILatticeStateApiAuthorizer.cs) (line 41).

`public sealed class DenyAllStateApiAuthorizer : ILatticeStateApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeStateApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.State.Grpc.AllowAllStateApiAuthorizer`

[Source](../../src/lattice.api.state.grpc/Security/ILatticeStateApiAuthorizer.cs) (line 54).

`public sealed class AllowAllStateApiAuthorizer : ILatticeStateApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeStateApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.State.Grpc.LatticePasswordHash`

[Source](../../src/lattice.api.state.grpc/Security/LatticePasswordHash.cs) (line 28).

`public static class LatticePasswordHash`

- `public const string AlgorithmPrefix`
- `public const int DefaultIterations`
- `public const int SaltSizeBytes`
- `public const int DerivedKeySizeBytes`
- `public static string Hash(string password, int iterations = DefaultIterations)`
- `public static string Encode(string password, ReadOnlySpan<byte> salt, int iterations)`
- `public static bool Verify(string? password, string? encodedHash)`
- `public static bool TryParse(string? encodedHash, out LatticePasswordHashComponents result)`

### `Orleans.Lattice.Api.State.Grpc.LatticePasswordHashComponents`

[Source](../../src/lattice.api.state.grpc/Security/LatticePasswordHash.cs) (line 181).

`public readonly record struct LatticePasswordHashComponents(int Iterations, byte[] Salt, byte[] DerivedKey)`

### `Orleans.Lattice.Api.State.Grpc.LatticeStateApiOperation`

[Source](../../src/lattice.api.state.grpc/Security/LatticeStateApiAuthorizationContext.cs) (line 11).

`public enum LatticeStateApiOperation`

- `ListTrees`
- `ListViews`
- `GetTreeStructure`
- `ScanEntries`
- `GetEntry`
- `GetEntryHistory`
- `ListTagIndexes`
- `ListTagValues`
- `ListCoveredTrees`
- `ListIndexTags`
- `ScanTagMembers`
- `CancelScan`
- `ObserveChanges`
- `ObserveMetrics`
- `GetMetricsSnapshot`
- `GetClusterInfo`
- `Unknown`
- `GetDeadLetterCount`
- `ListDeadLetters`

### `Orleans.Lattice.Api.State.Grpc.LatticeStateApiAuthorizationContext`

[Source](../../src/lattice.api.state.grpc/Security/LatticeStateApiAuthorizationContext.cs) (line 107).

`public readonly struct LatticeStateApiAuthorizationContext`

- `public LatticeStateApiAuthorizationContext(ServerCallContext call, LatticeStateApiOperation operation, string? targetTreeId)`
- `public ServerCallContext Call`
- `public LatticeStateApiOperation Operation`
- `public string? TargetTreeId`
