# Orleans.Lattice.Api.Backup.Grpc API reference

The public surface is the typed client, the server-side options, the authorization and identity seams, the operation enum and authorization-context struct, the registration extensions, and the wire message records. The gRPC service, method definitions, marshallers, interceptor, and the default header credential bridge / options auth-scheme source are internal and are described by behaviour in [Architecture](architecture.md).

The wire message records are Orleans-serialized (`[GenerateSerializer]`) with stable aliases held in the public `GrpcBackupTypeAliases` constant class.

## Registration

### `LatticeBackupApiGrpcServiceCollectionExtensions`

Static extensions.

- `IServiceCollection AddLatticeBackupApiGrpc(this IServiceCollection services, Action<LatticeBackupApiGrpcOptions>? configure = null)`

  Registers the binding: the method-definition singleton, the server-side service, the default-deny authorizer, the header credential bridge, the options-backed auth-scheme source, and the authorization interceptor. The interceptor is registered globally but scopes enforcement to the backup control-API service by service-name prefix, so unrelated gRPC services on the same host are unaffected. Idempotent. Throws `ArgumentNullException` when `services` is null.

- `IEndpointRouteBuilder MapLatticeBackupApiGrpc(this IEndpointRouteBuilder endpoints)`

  Maps the backup control-API RPC routes. The host must have called `AddLatticeBackupApiGrpc` and must expose the control facade (via `AddLatticeBackupApi`) in the same service provider first. Throws `ArgumentNullException` when `endpoints` is null.

## Client

### `LatticeBackupApiGrpcClient`

The public typed client. Wraps a `CallInvoker` and the code-first method definitions; carries no transport policy of its own.

- `static LatticeBackupApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` - builds a client over the invoker, resolving the per-message Orleans serializers from `serializerProvider` (which must have `AddSerializer()` registered). Throws `ArgumentNullException` when either argument is null.

Methods (one per RPC):

| Method | Signature |
|---|---|
| `CreateBackupAsync` | `Task<LatticeBackupCaptureResult> CreateBackupAsync(LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)` |
| `CreateIncrementalBackupAsync` | `Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(LatticeBackupIncrementalCaptureRequest request, CancellationToken cancellationToken = default)` |
| `CreateBackupSetAsync` | `Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default)` |
| `ListBackupsAsync` | `Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken = default)` |
| `StreamBackupsAsync` | `IAsyncEnumerable<BackupManifest> StreamBackupsAsync(CancellationToken cancellationToken = default)` |
| `DescribeBackupAsync` | `Task<BackupChainDescription?> DescribeBackupAsync(string backupId, CancellationToken cancellationToken = default)` |
| `DeleteBackupAsync` | `Task<bool> DeleteBackupAsync(string backupId, CancellationToken cancellationToken = default)` |
| `RestoreBackupAsync` | `Task<LatticeRestoreResult> RestoreBackupAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` |
| `RevertRestoreAsync` | `Task RevertRestoreAsync(LatticeRestoreResult restore, CancellationToken cancellationToken = default)` |
| `ExportArtifactAsync` | `IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(string backupId, string artifactId, CancellationToken cancellationToken = default)` |
| `GetAuthSchemeAsync` | `Task<AuthSchemeAdvertisement> GetAuthSchemeAsync(AuthSchemeAdvertisementRequest request, CancellationToken cancellationToken = default)` |
| `ProbeCapabilitiesAsync` | `Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)` |
| `ScheduleBackupAsync` | `Task<TimeSpan> ScheduleBackupAsync(BackupScopeSelector scope, bool incremental, TimeSpan interval, CancellationToken cancellationToken = default)` |
| `CancelScheduleAsync` | `Task CancelScheduleAsync(BackupScopeSelector scope, bool incremental, CancellationToken cancellationToken = default)` |
| `GetScopeStatusAsync` | `Task<BackupScopeStatus?> GetScopeStatusAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)` |
| `IsHealthMonitoringAvailableAsync` | `Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default)` |
| `CheckBackupHealthAsync` | `Task<BackupHealthReport> CheckBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)` |
| `GetBackupHealthAsync` | `Task<BackupHealthReport?> GetBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)` |
| `ConfigureBackupHealthAsync` | `Task ConfigureBackupHealthAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)` |

`CreateBackupAsync`, `CreateIncrementalBackupAsync`, `CreateBackupSetAsync`, `RestoreBackupAsync`, and `RevertRestoreAsync` throw `ArgumentNullException` on a null request; `DescribeBackupAsync`, `DeleteBackupAsync`, `ExportArtifactAsync`, `CheckBackupHealthAsync`, `GetBackupHealthAsync`, and `ConfigureBackupHealthAsync` throw `ArgumentException` on a null or empty id. `DescribeBackupAsync` and `GetScopeStatusAsync` return `null` when the server reports the target absent; `GetBackupHealthAsync` returns `null` when no stored report exists. `ProbeCapabilitiesAsync`, `ScheduleBackupAsync`, `CancelScheduleAsync`, and `GetScopeStatusAsync` throw `ArgumentNullException` on a null scope. `ScheduleBackupAsync` throws `ArgumentOutOfRangeException` when `interval` is not strictly positive; it registers a recurring backup of the scope, authorized with the same grant as a capture, and returns the effective cadence actually registered (clamped up to the scheduler minimum when smaller). `CancelScheduleAsync` idempotently removes a runtime full or incremental schedule. `CheckBackupHealthAsync` verifies and persists a fresh report; `ConfigureBackupHealthAsync` also throws `ArgumentNullException` when `config` is null. `GetAuthSchemeAsync` is unauthenticated - callable before any credential is acquired.

## Server-side options

### `LatticeBackupApiGrpcOptions`

See [Configuration](configuration.md) for the full table. Properties: `bool RequireAuthorization` (default `true`), `string CredentialHeaderName` (default `authorization`), `string CredentialScheme` (default `Bearer`), and `IList<AuthSchemeDescriptor> AdvertisedAuthSchemes` (empty by default).

## Authorization and identity seams

### `ILatticeBackupApiAuthorizer`

The transport meta-authorization seam. A host supplies an implementation to decide whether an inbound call may drive the backup control API.

- `Task<bool> IsAuthorizedAsync(LatticeBackupApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)` - `true` to allow, `false` to reject with `PermissionDenied`.

### `DenyAllBackupApiAuthorizer` : `ILatticeBackupApiAuthorizer`

The default authorizer, registered automatically, that rejects every protected call so a host that maps the surface without configuring authorization fails closed. The unauthenticated `GetAuthScheme` discovery RPC bypasses this authorizer.

### `AllowAllBackupApiAuthorizer` : `ILatticeBackupApiAuthorizer`

An opt-in authorizer that permits every protected call, for trusted-network deployments behind a separate authentication boundary. Register explicitly to override the default-deny posture.

### `ILatticeBackupApiCredentialBridge`

The identity seam that lifts the caller identity on an inbound gRPC call into an ambient `LatticeCredential` so the backup access gate can resolve the caller's subject.

- `LatticeCredential? Resolve(ServerCallContext context)` - the resolved credential, or `null` when the call carries none (the caller is then anonymous, and denied when auth-backed backup control is active). The built-in default reads a single configurable bearer-style header; a host registers its own implementation for a bespoke identity source (client certificate, signed edge header, pre-resolved principal).

### `ILatticeBackupApiAuthSchemeSource`

Supplies the advertisement the unauthenticated `GetAuthScheme` RPC returns.

- `AuthSchemeAdvertisement GetAdvertisement()` - the current advertisement (an empty one when nothing is configured). An implementation must return only public configuration - never a secret.

### `LatticeBackupApiOperation`

Identifies which control-API operation an inbound call invokes, so an authorizer can make per-operation decisions. Values: `CreateBackup`, `CreateIncrementalBackup`, `CreateBackupSet`, `ListBackups`, `StreamBackups`, `DescribeBackup`, `DeleteBackup`, `RestoreBackup`, `RevertRestore`, `ExportArtifact`, `ScheduleBackup`, `CancelSchedule`, `GetScopeStatus`, `IsHealthMonitoringAvailable`, `CheckBackupHealth`, `GetBackupHealth`, `ConfigureBackupHealth`, and `Unknown` (an unrecognised method, presented so a deny-by-default policy refuses it rather than treating it as benign).

### `LatticeBackupApiAuthorizationContext`

A `readonly struct` describing an inbound call to the authorizer.

- Constructor: `LatticeBackupApiAuthorizationContext(ServerCallContext call, LatticeBackupApiOperation operation, string? targetId)`. Throws `ArgumentNullException` when `call` is null.
- `ServerCallContext Call` - the underlying gRPC call context (headers, deadline, peer).
- `LatticeBackupApiOperation Operation` - the operation being invoked.
- `string? TargetId` - the backup id for a backup-scoped call, or the target / scope tree id for a capture or restore not yet keyed by a backup id; `null` for whole-catalog and discovery operations.

## Wire message records

Each RPC wraps the facade DTOs in one of these Orleans-serialized request / response records. Properties marked `required` must be set by the caller.

| Record | Members |
|---|---|
| `BackupCaptureRequestMessage` | `required string Name`, `required BackupScopeSelector Scope`, `int PageSize` (default `LatticeBackupCaptureRequest.DefaultPageSize`). |
| `BackupIncrementalCaptureRequestMessage` | `required string Name`, `required BackupScopeSelector Scope`, `required string BaseBackupId`, `int PageSize` (default as above). |
| `BackupSetCaptureRequestMessage` | `required string Name`, `required IReadOnlyList<BackupScopeSelector> Scopes`, `bool CrossTreeConsistent`, `int PageSize` (default `LatticeBackupCaptureRequest.DefaultPageSize`). |
| `BackupCaptureResponse` | `required string BackupId`, `required BackupManifest Manifest`. |
| `BackupSetCaptureResponse` | `required BackupSetManifest SetManifest`, `required IReadOnlyList<BackupCaptureResponse> Members` (one per captured tree). |
| `BackupDescribeRequest` | `required string BackupId`. |
| `BackupChainResponse` | `bool Found`, `BackupManifest? Manifest`, `IReadOnlyList<string> ChainBackupIds`. |
| `BackupDeleteRequest` | `required string BackupId`. |
| `BackupDeleteResponse` | `bool Deleted`. |
| `BackupStreamRequest` | (empty - drains the whole readable catalog). |
| `RestoreRequestMessage` | `required string BackupId`, `string? TargetTreeId`, `BackupScopeSelector? Scope`, `LatticeRestoreMode Mode` (default `InPlace`), `string? OperationId`, `int ApplyBatchSize` (default `LatticeRestoreRequest.DefaultApplyBatchSize`). |
| `RestoreResponse` | `required string BackupId`, `required string TargetTreeId`, `LatticeRestoreMode Mode`, `required string OperationId`, `IReadOnlyList<string> ManifestChain`, `long EntriesApplied`, `string? ShadowPhysicalTreeId`, `string? PreviousPhysicalTreeId`. |
| `RevertRestoreResponse` | (empty). |
| `ArtifactExportRequest` | `required string BackupId`, `required string ArtifactId`. |
| `ArtifactChunk` | `required byte[] Data` - one chunk of a streamed artifact. |
| `AuthSchemeAdvertisementRequest` | (empty). |
| `AuthSchemeAdvertisement` | `IReadOnlyList<AuthSchemeDescriptor> Schemes`. |
| `AuthSchemeDescriptor` | `required string SchemeId`, `string DisplayName`, `IReadOnlyDictionary<string, string> Parameters`. |
| `BackupCapabilityProbeRequest` | `required BackupScopeSelector Scope` - the scope the read-only capability probe reports on. |
| `BackupScheduleRequestMessage` | `required BackupScopeSelector Scope`, `bool Incremental`, `long IntervalTicks` - the requested cadence between captures, sent as `TimeSpan.Ticks`. |
| `BackupScheduleResponse` | `bool Scheduled`, `long EffectiveIntervalTicks` - the cadence actually registered (clamped up to the scheduler minimum), as `TimeSpan.Ticks`. |
| `BackupCancelScheduleRequestMessage` | `required BackupScopeSelector Scope`, `bool Incremental` - the runtime schedule to remove. |
| `BackupCancelScheduleResponse` | (empty). |
| `BackupScopeStatusRequestMessage` | `required BackupScopeSelector Scope` - the scope whose status should be described. |
| `BackupScopeStatusResponse` | `bool Found`, `BackupScopeSelector? Scope`, `bool FullScheduleRegistered`, `bool IncrementalScheduleRegistered`, `DateTimeOffset? LastFullRunUtc`, `DateTimeOffset? LastFullSuccessUtc`, `DateTimeOffset? LastIncrementalRunUtc`, `DateTimeOffset? LastIncrementalSuccessUtc`, `BackupScopeRunOutcome LastRunOutcome`, `int ChainDepth`, `long? RuntimeFullBackupIntervalTicks`, `long? RuntimeIncrementalBackupIntervalTicks`. |
| `BackupHealthAvailabilityRequest` | (empty). |
| `BackupHealthAvailabilityResponse` | `bool Available`. |
| `BackupHealthCheckRequestMessage` | `required string BackupId`. |
| `BackupHealthGetRequestMessage` | `required string BackupId`. |
| `BackupHealthReportResponse` | `bool Found`, `BackupHealthReport? Report`. |
| `BackupHealthConfigureRequestMessage` | `required string BackupId`, `bool MonitoringEnabled`, `long IntervalTicks`. |
| `BackupHealthConfigureResponse` | (empty). |

The `RevertRestore` RPC reuses `RestoreResponse` as its request shape (the client sends back the restore result to revert) and returns `RevertRestoreResponse`. The `ProbeCapabilities` RPC takes a `BackupCapabilityProbeRequest` and returns a `BackupScopeCapabilities` (defined in [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/api.md)). The schedule RPCs use `BackupScheduleRequestMessage` / `BackupScheduleResponse` and `BackupCancelScheduleRequestMessage` / `BackupCancelScheduleResponse`. The health RPCs use the `BackupHealth*` records above and facade DTOs from `Orleans.Lattice.Backup`.

## Serialization aliases

### `GrpcBackupTypeAliases`

A public static class holding the stable Orleans serialization alias constants for the wire message records, referenced by their `[Alias(...)]` attributes so the wire contract stays stable across renames.
