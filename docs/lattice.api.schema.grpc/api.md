# Orleans.Lattice.Api.Schema.Grpc API reference

The public surface is the typed client, the server-side options, the registration extensions, and the wire message records. The gRPC service, method definitions, marshallers, interceptor, and default auth-scheme plumbing are internal and are described by behaviour in [Architecture](architecture.md).

The wire message records are Orleans-serialized (`[GenerateSerializer]`) with stable aliases prefixed `oisg.`. The facade and shared abstractions records use aliases prefixed `ois.`.

## Registration

### `LatticeSchemaApiGrpcServiceCollectionExtensions`

Static extensions.

- `IServiceCollection AddLatticeSchemaApiGrpc(this IServiceCollection services, Action<LatticeSchemaApiGrpcOptions>? configure = null)`

  Registers the binding: the method-definition singleton, the server-side service, the default-deny authorization path, the options-backed auth-scheme source, and the authorization interceptor. The interceptor is registered globally but scopes enforcement to the schema control-API service by service-name prefix, so unrelated gRPC services on the same host are unaffected. Idempotent. Throws `ArgumentNullException` when `services` is null.

- `IEndpointRouteBuilder MapLatticeSchemaApiGrpc(this IEndpointRouteBuilder endpoints)`

  Maps the schema control-API RPC routes. The host must have called `AddLatticeSchemaApiGrpc` and must expose the control facade (via `AddLatticeSchemaApi`) in the same service provider first. Throws `ArgumentNullException` when `endpoints` is null.

## Client

### `LatticeSchemaApiGrpcClient`

The public typed client. Wraps a `CallInvoker` and the code-first method definitions; carries no transport policy of its own.

- `static LatticeSchemaApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)` - builds a client over the invoker, resolving the per-message Orleans serializers from `serializerProvider` (which must have `AddSerializer()` registered). Throws `ArgumentNullException` when either argument is null.

Methods (one per RPC):

| Method | Signature |
|---|---|
| `SetPolicyAsync` | `Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)` |
| `ClearPolicyAsync` | `Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetPolicyAsync` | `Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)` |
| `ListDeadLettersAsync` | `IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)` |
| `CountDeadLettersAsync` | `Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)` |
| `SetVersionConfigAsync` | `Task SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)` |
| `GetVersionConfigAsync` | `Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)` |
| `AdvanceTargetVersionAsync` | `Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)` |
| `AdvanceAndMigrateAsync` | `Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)` |
| `MigrateToTargetVersionAsync` | `Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default)` |
| `ClearVersionConfigAsync` | `Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)` |
| `RemediateAsync` | `Task<LatticeSchemaRemediationReport> RemediateAsync(string treeId, LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy, CancellationToken cancellationToken = default)` |
| `GetRemediationStatusAsync` | `Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default)` |
| `ScanComplianceAsync` | `Task<LatticeSchemaComplianceReport> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default)` |
| `ProbeCapabilitiesAsync` | `Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetAuthSchemeAsync` | `Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)` |

Methods that take a tree id reject a null or empty id. Methods that take a policy, config, transform, or target policy reject null inputs. `ListDeadLettersAsync` is server-streaming and re-exposes the server stream as an `IAsyncEnumerable<LatticeSchemaDeadLetterEntry>`. `ProbeCapabilitiesAsync` reports the caller's allowed-operation set (`LatticeSchemaCapabilities`) with no side effects; it never replaces the fail-closed authorization each real RPC still performs. `GetAuthSchemeAsync` is unauthenticated - callable before any credential is acquired.

## Server-side options

### `LatticeSchemaApiGrpcOptions`

See [Configuration](configuration.md) for the full table. Properties: `bool RequireAuthorization` (default `true`), `string CredentialHeaderName` (default `authorization`), `string CredentialScheme` (default `Bearer`), and `IList<AuthSchemeDescriptor> AdvertisedAuthSchemes` (empty by default).

## Authorization behaviour

### `LatticeSchemaApiGrpcAuthInterceptor`

The fail-closed authorization interceptor. It denies by default, exempts only `GetAuthScheme`, and maps unknown or unmapped failures to safe gRPC status codes. A host must configure authorization deliberately or place the endpoint behind a trusted boundary and set `RequireAuthorization = false`.

`GetAuthScheme` advertises the configured public auth schemes to unauthenticated callers. It must never return secrets or user-specific data.

## Wire message records

Each RPC's request and response is one of these Orleans-serialized records, except the dead-letter stream and the capability probe, which carry schema-package record types directly (see the note below the table). Properties marked `required` must be set by the caller.

| Record | Members |
|---|---|
| `SchemaTreeRequest` | `required string TreeId`. |
| `SetPolicyRequest` | `required string TreeId`, `required LatticeSchemaPolicy Policy`. |
| `SetVersionConfigRequest` | `required string TreeId`, `required LatticeSchemaVersionConfig Config`. |
| `AdvanceVersionRequest` | `required string TreeId`, `required uint NewTargetVersion`. |
| `RemediateRequest` | `required string TreeId`, `required LatticeValueTransform Transform`, `required LatticeSchemaPolicy TargetPolicy`. |
| `AuthSchemeAdvertisementRequest` | (empty). |
| `SchemaAckResponse` | (empty). |
| `SchemaRemovedResponse` | `required bool Removed`. |
| `GetPolicyResponse` | `required bool Found`, `LatticeSchemaPolicy? Policy`. |
| `SchemaCountResponse` | `required int Count`. |
| `GetVersionConfigResponse` | `required bool Found`, `LatticeSchemaVersionConfig Config`. |
| `VersionConfigResponse` | `required LatticeSchemaVersionConfig Config`. |
| `SchemaRemediationReportResponse` | `required LatticeSchemaRemediationReport Report`. |
| `SchemaComplianceReportResponse` | `required LatticeSchemaComplianceReport Report`. |
| `AuthSchemeAdvertisement` | `IReadOnlyList<AuthSchemeDescriptor> Schemes`. |
| `AuthSchemeDescriptor` | `required string SchemeId`, `string DisplayName`, `IReadOnlyDictionary<string, string> Parameters`. |

`SetPolicy` and `SetVersionConfig` return the empty `SchemaAckResponse`; `ClearPolicy` and `ClearVersionConfig` return `SchemaRemovedResponse`. `StreamDeadLetters` takes a `SchemaTreeRequest` and streams `LatticeSchemaDeadLetterEntry` values directly (no wrapper record). `ProbeCapabilities` returns a `LatticeSchemaCapabilities` value directly. Both of those types are defined in the schema packages, not in this binding. `GetAuthScheme` takes `AuthSchemeAdvertisementRequest` and returns `AuthSchemeAdvertisement`; the typed client projects that response to `IReadOnlyList<AuthSchemeDescriptor>`.

## Serialization aliases

### `GrpcSchemaTypeAliases`

A public static class holding the stable Orleans serialization alias constants for the wire message records, referenced by their `[Alias(...)]` attributes so the wire contract stays stable across renames. Contract versioning is additive-only: new fields use new `[Id(n)]` values, and aliases or field numbers are never renumbered, so a newer response can decode under an older client.
