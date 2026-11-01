---
agent_spec: "docs/agents/api/telemetry.json"
---

# Public API

`Orleans.Lattice.Api.Telemetry` publishes the curated telemetry facade, the backend proxy beneath it, and the helpers a binding needs to apply the same rules. It implements the shared `ILatticeTelemetry` contract, whose interface, request/response records, catalogue types and exceptions live in `Orleans.Lattice.Api.Abstractions` under the `Orleans.Lattice.Api.Telemetry` namespace; both inventories are listed below. The [package guide](README.md) explains the catalogue, the allow-list rules, tenant scoping and the failure surface; this page is the declaration reference.

The remote transport's surface - `LatticeTelemetryApiGrpcClient`, its options and authorization seams - belongs to the gRPC binding and is listed in the [gRPC binding's public API](../lattice.api.telemetry.grpc/api.md).

## Registration

| Method | What it registers |
|---|---|
| `AddLatticeTelemetryBackend(this IServiceCollection services)` | The `TelemetryMetricAccessPolicy` compiled once from the bound options, and the default `PrometheusQueryClient` as `IPrometheusQueryClient` over a typed `HttpClient` - unless an `IPrometheusQueryClient` is already registered. Idempotent. Registers no capability check. |
| `AddLatticeTelemetryApi(this IServiceCollection services)` | Calls `AddLatticeTelemetryBackend`, then registers the built-in `LatticeTelemetryQueryCatalog`, `TelemetryAccessAuthorizer`, `TelemetryTenantScopeResolver` and `LatticeTelemetry` as `ILatticeTelemetry`, each with `TryAdd`. Idempotent. |

Neither method takes a configure delegate or registers options validation: bind `LatticeTelemetryOptions` with the options pattern and, to enforce its rules, register `LatticeTelemetryOptionsValidator`. See [Configuration](configuration.md).

## Type groups

| Group | Types |
|---|---|
| Facade | `LatticeTelemetry` (implements `ILatticeTelemetry`: `GetCatalogAsync`, `QueryAsync`) |
| Options | `LatticeTelemetryOptions`, `LatticeTelemetryBackendAuthMode`, `LatticeTelemetryBackendCredential`, `LatticeTelemetryMetricAccessMode`, `LatticeTelemetryOptionsValidator` |
| Catalogue | `LatticeTelemetryQueries` (the built-in definitions and their revision), `LatticeTelemetryQueryCatalog` (the deployment's offered subset) |
| Backend proxy | `IPrometheusQueryClient`, `PrometheusQueryClient`, `PrometheusQueryResponse`, `PrometheusMetadataResponse`, `ITelemetryBackendTokenProvider` |
| Enforcement helpers | `TelemetryAccessAuthorizer`, `TelemetryAccessGrant`, `TelemetryMetricAccessPolicy`, `TelemetryQueryAuthorizer`, `TelemetryRangeGuardrails`, `TelemetryTenantScopeResolver`, `PromQlMetricExtractor`, `PromQlMetricReferences` |

The enforcement helpers exist so a binding that evaluates caller-supplied PromQL - the MCP telemetry tool group is one - can apply the facade's capability check, allow-list and range guardrails without the curated catalogue; see [The backend proxy beneath the facade](README.md#the-backend-proxy-beneath-the-facade).

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [gRPC binding public API](../lattice.api.telemetry.grpc/api.md)
- [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Telemetry.LatticeTelemetryQueries`

[Source](../../src/lattice.api.telemetry/Catalog/LatticeTelemetryQueries.cs) (line 41).

`public static class LatticeTelemetryQueries`

- `public const int Version`
- `public static IReadOnlyList<TelemetryQueryDefinition> Definitions`

### `Orleans.Lattice.Api.Telemetry.LatticeTelemetryQueryCatalog`

[Source](../../src/lattice.api.telemetry/Catalog/LatticeTelemetryQueryCatalog.cs) (line 28).

`public sealed class LatticeTelemetryQueryCatalog`

- `public LatticeTelemetryQueryCatalog(TelemetryMetricAccessPolicy policy)`
- `public LatticeTelemetryQueryCatalog(IEnumerable<TelemetryQueryDefinition> definitions, int version, TelemetryMetricAccessPolicy policy)`
- `public int Version`
- `public TelemetryQueryCatalog Catalog`
- `public int Count`
- `public bool Offers(string queryId)`

### `Orleans.Lattice.Api.Telemetry.IPrometheusQueryClient`

[Source](../../src/lattice.api.telemetry/IPrometheusQueryClient.cs) (line 11).

`public interface IPrometheusQueryClient`

- `Task<PrometheusQueryResponse> InstantQueryAsync(string query, DateTimeOffset? time, CancellationToken cancellationToken)`
- `Task<PrometheusQueryResponse> RangeQueryAsync(string query, DateTimeOffset start, DateTimeOffset end, TimeSpan step, CancellationToken cancellationToken)`
- `Task<IReadOnlyList<string>> ListMetricNamesAsync(CancellationToken cancellationToken)`
- `Task<PrometheusMetadataResponse> MetricMetadataAsync(string? metric, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Telemetry.ITelemetryBackendTokenProvider`

[Source](../../src/lattice.api.telemetry/ITelemetryBackendTokenProvider.cs) (line 19).

`public interface ITelemetryBackendTokenProvider`

- `ValueTask<string> GetAccessTokenAsync(CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Telemetry.LatticeApiTelemetryServiceCollectionExtensions`

[Source](../../src/lattice.api.telemetry/LatticeApiTelemetryServiceCollectionExtensions.cs) (line 15).

`public static class LatticeApiTelemetryServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeTelemetryBackend(this IServiceCollection services)`
- `public static IServiceCollection AddLatticeTelemetryApi(this IServiceCollection services)`

### `Orleans.Lattice.Api.Telemetry.LatticeTelemetry`

[Source](../../src/lattice.api.telemetry/LatticeTelemetry.cs) (line 42).

`public sealed class LatticeTelemetry : ILatticeTelemetry`

- `public LatticeTelemetry(LatticeTelemetryQueryCatalog catalog, TelemetryTenantScopeResolver scopes, TelemetryAccessAuthorizer authorizer, IPrometheusQueryClient backend, IOptions<LatticeTelemetryOptions> options, TimeProvider? timeProvider = null)`
- `public Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)`
- `public Task<TelemetryQueryResponse> QueryAsync(TelemetryQueryRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Telemetry.LatticeTelemetryBackendAuthMode`

[Source](../../src/lattice.api.telemetry/LatticeTelemetryBackendAuthMode.cs) (line 10).

`public enum LatticeTelemetryBackendAuthMode`

- `None`
- `Bearer`
- `Basic`
- `MutualTls`
- `DynamicBearer`

### `Orleans.Lattice.Api.Telemetry.LatticeTelemetryBackendCredential`

[Source](../../src/lattice.api.telemetry/LatticeTelemetryBackendCredential.cs) (line 17).

`public sealed class LatticeTelemetryBackendCredential`

- `public string? BearerToken`
- `public string? BasicUsername`
- `public string? BasicPassword`
- `public X509Certificate2? ClientCertificate`

### `Orleans.Lattice.Api.Telemetry.LatticeTelemetryMetricAccessMode`

[Source](../../src/lattice.api.telemetry/LatticeTelemetryMetricAccessMode.cs) (line 6).

`public enum LatticeTelemetryMetricAccessMode`

- `ReadAll`
- `DenyAllExceptAllowed`

### `Orleans.Lattice.Api.Telemetry.LatticeTelemetryOptions`

[Source](../../src/lattice.api.telemetry/LatticeTelemetryOptions.cs) (line 20).

`public class LatticeTelemetryOptions`

- `public Uri? BackendAddress`
- `public LatticeTelemetryBackendAuthMode AuthMode`
- `public LatticeTelemetryBackendCredential? Credential`
- `public TimeSpan RequestTimeout`
- `public TimeSpan MaxRange`
- `public TimeSpan MaxStep`
- `public LatticeTelemetryMetricAccessMode MetricAccess`
- `public IList<string> AllowedMetrics`

### `Orleans.Lattice.Api.Telemetry.LatticeTelemetryOptionsValidator`

[Source](../../src/lattice.api.telemetry/LatticeTelemetryOptionsValidator.cs) (line 13).

`public sealed class LatticeTelemetryOptionsValidator : IValidateOptions<LatticeTelemetryOptions>`

- `public ValidateOptionsResult Validate(string? name, LatticeTelemetryOptions options)`

### `Orleans.Lattice.Api.Telemetry.PromQlMetricExtractor`

[Source](../../src/lattice.api.telemetry/PromQlMetricExtractor.cs) (line 53).

`public static class PromQlMetricExtractor`

- `public static PromQlMetricReferences ExtractReferences(string query)`

### `Orleans.Lattice.Api.Telemetry.PromQlMetricReferences`

[Source](../../src/lattice.api.telemetry/PromQlMetricReferences.cs) (line 16).

`public readonly record struct PromQlMetricReferences`

- `public required IReadOnlyList<string> Names`
- `public required bool HasUnresolvableNameMatcher`
- `public bool HasUnconstrainedSelector`

### `Orleans.Lattice.Api.Telemetry.PrometheusMetadataResponse`

[Source](../../src/lattice.api.telemetry/PrometheusMetadataResponse.cs) (line 17).

`public readonly record struct PrometheusMetadataResponse(string Status, JsonElement Data)`

### `Orleans.Lattice.Api.Telemetry.PrometheusQueryClient`

[Source](../../src/lattice.api.telemetry/PrometheusQueryClient.cs) (line 20).

`public sealed class PrometheusQueryClient : IPrometheusQueryClient`

- `public PrometheusQueryClient(HttpClient http, IOptions<LatticeTelemetryOptions> options, ITelemetryBackendTokenProvider? tokenProvider = null, ILogger<PrometheusQueryClient>? logger = null)`
- `public Task<PrometheusQueryResponse> InstantQueryAsync(string query, DateTimeOffset? time, CancellationToken cancellationToken)`
- `public Task<PrometheusQueryResponse> RangeQueryAsync(string query, DateTimeOffset start, DateTimeOffset end, TimeSpan step, CancellationToken cancellationToken)`
- `public Task<IReadOnlyList<string>> ListMetricNamesAsync(CancellationToken cancellationToken)`
- `public Task<PrometheusMetadataResponse> MetricMetadataAsync(string? metric, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Telemetry.PrometheusQueryResponse`

[Source](../../src/lattice.api.telemetry/PrometheusQueryResponse.cs) (line 17).

`public readonly record struct PrometheusQueryResponse(string Status, JsonElement Data)`

### `Orleans.Lattice.Api.Telemetry.TelemetryAccessAuthorizer`

[Source](../../src/lattice.api.telemetry/TelemetryAccessAuthorizer.cs) (line 42).

`public sealed class TelemetryAccessAuthorizer`

- `public TelemetryAccessAuthorizer(ILatticeAccessGate? gate = null, ILatticeMembershipContext? membership = null)`
- `public ValueTask AuthorizeClusterTelemetryAsync(CancellationToken cancellationToken = default)`
- `public ValueTask AuthorizeTenantTelemetryAsync(TenantId tenant, CancellationToken cancellationToken = default)`
- `public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Telemetry.TelemetryAccessGrant`

[Source](../../src/lattice.api.telemetry/TelemetryAccessGrant.cs) (line 18).

`public readonly record struct TelemetryAccessGrant`

- `public static TelemetryAccessGrant ClusterWide`
- `public bool IsClusterWide`
- `public string? TenantId`
- `public static TelemetryAccessGrant ForTenant(string tenantId)`

### `Orleans.Lattice.Api.Telemetry.TelemetryMetricAccessPolicy`

[Source](../../src/lattice.api.telemetry/TelemetryMetricAccessPolicy.cs) (line 39).

`public sealed class TelemetryMetricAccessPolicy`

- `public TelemetryMetricAccessPolicy(LatticeTelemetryOptions options)`
- `public bool IsReadAll`
- `public bool IsAdmitted(string metric)`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryAuthorizer`

[Source](../../src/lattice.api.telemetry/TelemetryQueryAuthorizer.cs) (line 23).

`public static class TelemetryQueryAuthorizer`

- `public static bool TryAuthorizeQuery(TelemetryMetricAccessPolicy policy, string query, out string? denialMessage)`
- `public static string DeniedMessage(string metric)`

### `Orleans.Lattice.Api.Telemetry.TelemetryRangeGuardrails`

[Source](../../src/lattice.api.telemetry/TelemetryRangeGuardrails.cs) (line 17).

`public static class TelemetryRangeGuardrails`

- `public static bool TryValidateRange(LatticeTelemetryOptions options, DateTimeOffset start, DateTimeOffset end, TimeSpan step, out string? violationMessage)`

### `Orleans.Lattice.Api.Telemetry.TelemetryTenantScopeResolver`

[Source](../../src/lattice.api.telemetry/TelemetryTenantScopeResolver.cs) (line 48).

`public sealed class TelemetryTenantScopeResolver`

- `public TelemetryTenantScopeResolver(ITenantContextResolver tenants, TelemetryAccessAuthorizer authorizer)`
- `public ValueTask<TelemetryAccessGrant> AuthorizeAsync(CancellationToken cancellationToken = default)`
- `public ValueTask<TelemetryTenantScope> ResolveAsync(TelemetryAccessGrant grant, TelemetryTenantVisibility requestedVisibility, string? requestedTenantId = null, CancellationToken cancellationToken = default)`
- `public ValueTask<TelemetryTenantScope> ResolveAsync(TelemetryTenantVisibility requestedVisibility, string? requestedTenantId = null, CancellationToken cancellationToken = default)`

## Shared contract declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Telemetry.ILatticeTelemetry`

[Source](../../src/lattice.api.abstractions/Telemetry/ILatticeTelemetry.cs) (line 43).

`public interface ILatticeTelemetry`

- `Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)`
- `Task<TelemetryQueryResponse> QueryAsync(TelemetryQueryRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Telemetry.ApiTelemetryTypeAliases`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/ApiTelemetryTypeAliases.cs) (line 24).

`public static class ApiTelemetryTypeAliases`

- `public const string AliasPrefix`
- `public const string TelemetryQueryCatalog`
- `public const string TelemetryQueryDescriptor`
- `public const string TelemetryQueryDefinition`
- `public const string TelemetryQueryBounds`
- `public const string TelemetryQueryParameters`
- `public const string TelemetryQueryKind`
- `public const string TelemetryInstrumentReference`
- `public const string TelemetryMeasurementSemantic`
- `public const string TelemetryQueryRequest`
- `public const string TelemetryQueryResponse`
- `public const string TelemetryTimeRange`
- `public const string TelemetryTimeSeries`
- `public const string TelemetryDataPoint`
- `public const string TelemetryLabel`
- `public const string TelemetryTenantScope`
- `public const string TelemetryTenantVisibility`
- `public const string TelemetryResultKind`
- `public const string TelemetryBoundsViolation`

### `Orleans.Lattice.Api.Telemetry.TelemetryBackendException`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryBackendException.cs) (line 34).

`public sealed class TelemetryBackendException : Exception`

- `public TelemetryBackendException(string queryId, string message)`
- `public TelemetryBackendException(string queryId, string message, Exception innerException)`
- `public string QueryId`

### `Orleans.Lattice.Api.Telemetry.TelemetryBoundsViolation`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryBoundsViolation.cs) (line 13).

`public enum TelemetryBoundsViolation`

- `None = 0`
- `RangeNotAscending = 1`
- `StepBelowMinimum = 2`
- `StepAboveMaximum = 3`
- `RangeTooLong = 4`
- `LookbackTooOld = 5`
- `TooManyPoints = 6`

### `Orleans.Lattice.Api.Telemetry.TelemetryDataPoint`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryDataPoint.cs) (line 20).

`public readonly record struct TelemetryDataPoint`

- `public TelemetryDataPoint(DateTimeOffset timestamp, double value)`
- `public DateTimeOffset Timestamp`
- `public double Value`
- `public bool IsFinite`

### `Orleans.Lattice.Api.Telemetry.TelemetryInstrumentReference`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryInstrumentReference.cs) (line 19).

`public readonly record struct TelemetryInstrumentReference`

- `public TelemetryInstrumentReference(string name, string meter, string unit, TelemetryMeasurementSemantic semantic)`
- `public string Name`
- `public string Meter`
- `public string Unit`
- `public TelemetryMeasurementSemantic Semantic`

### `Orleans.Lattice.Api.Telemetry.TelemetryLabel`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryLabel.cs) (line 13).

`public readonly record struct TelemetryLabel`

- `public TelemetryLabel(string name, string value)`
- `public string Name`
- `public string Value`

### `Orleans.Lattice.Api.Telemetry.TelemetryMeasurementSemantic`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryMeasurementSemantic.cs) (line 20).

`public enum TelemetryMeasurementSemantic`

- `Unspecified = 0`
- `PerOperation = 1`
- `PerRecord = 2`
- `PerBatch = 3`
- `Duration = 4`
- `Level = 5`
- `Ratio = 6`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryBounds`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryBounds.cs) (line 27).

`public readonly record struct TelemetryQueryBounds`

- `public TimeSpan MinStep`
- `public TimeSpan MaxStep`
- `public TimeSpan DefaultStep`
- `public TimeSpan MaxRange`
- `public TimeSpan MaxLookback`
- `public int MaxPoints`
- `public static TelemetryQueryBounds Unbounded`
- `public bool IsUnbounded`
- `public TimeSpan EffectiveStep(TimeSpan requested)`
- `public TelemetryBoundsViolation Validate(TelemetryTimeRange range, DateTimeOffset now)`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryBoundsException`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryBoundsException.cs) (line 14).

`public sealed class TelemetryQueryBoundsException : Exception`

- `public TelemetryQueryBoundsException(string queryId, TelemetryBoundsViolation violation)`
- `public TelemetryQueryBoundsException(string queryId, TelemetryBoundsViolation violation, string message)`
- `public string QueryId`
- `public TelemetryBoundsViolation Violation`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryCatalog`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryCatalog.cs) (line 32).

`public sealed record TelemetryQueryCatalog`

- `public required int Version`
- `public required IReadOnlyList<TelemetryQueryDescriptor> Queries`
- `public static TelemetryQueryCatalog Empty`
- `public int Count`
- `public bool TryGetQuery(string queryId, [NotNullWhen(true)] out TelemetryQueryDescriptor? descriptor)`
- `public bool Contains(string queryId)`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryDefinition`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryDefinition.cs) (line 30).

`public sealed record TelemetryQueryDefinition`

- `public required TelemetryQueryDescriptor Descriptor`
- `public required string QueryTemplate`
- `public string QueryId`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryDescriptor`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryDescriptor.cs) (line 36).

`public sealed record TelemetryQueryDescriptor`

- `public required string QueryId`
- `public required string Title`
- `public required string Description`
- `public required string Unit`
- `public TelemetryQueryKind Kind`
- `public TelemetryMeasurementSemantic Semantic`
- `public TelemetryQueryParameters Parameters`
- `public TelemetryQueryBounds Bounds`
- `public required IReadOnlyList<TelemetryInstrumentReference> Instruments`
- `public bool Accepts(TelemetryQueryParameters parameter)`
- `public bool ReadsInstrument(string instrumentName)`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryKind`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryKind.cs) (line 11).

`public enum TelemetryQueryKind`

- `Instant = 0`
- `Range = 1`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryNotFoundException`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryNotFoundException.cs) (line 16).

`public sealed class TelemetryQueryNotFoundException : Exception`

- `public TelemetryQueryNotFoundException(string queryId)`
- `public TelemetryQueryNotFoundException(string queryId, string message)`
- `public string QueryId`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryParameters`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryParameters.cs) (line 30).

`public enum TelemetryQueryParameters`

- `None = 0`
- `TimeRange = 1`
- `Step = 2`
- `TreeFilter = 4`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryRequest`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryRequest.cs) (line 38).

`public sealed record TelemetryQueryRequest`

- `public required string QueryId`
- `public TelemetryTimeRange Range`
- `public string? TreeId`
- `public TelemetryTenantVisibility RequestedVisibility`
- `public string? RequestedTenantId`

### `Orleans.Lattice.Api.Telemetry.TelemetryQueryResponse`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryResponse.cs) (line 32).

`public sealed record TelemetryQueryResponse`

- `public required string QueryId`
- `public required TelemetryTenantScope Scope`
- `public TelemetryResultKind ResultKind`
- `public required IReadOnlyList<TelemetryTimeSeries> Series`
- `public TelemetryTimeRange Range`
- `public int SeriesCount`
- `public bool IsEmpty`

### `Orleans.Lattice.Api.Telemetry.TelemetryResultKind`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryResultKind.cs) (line 10).

`public enum TelemetryResultKind`

- `Empty = 0`
- `Vector = 1`
- `Matrix = 2`
- `Scalar = 3`

### `Orleans.Lattice.Api.Telemetry.TelemetryTenantScope`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryTenantScope.cs) (line 38).

`public readonly record struct TelemetryTenantScope`

- `public TelemetryTenantVisibility RequestedVisibility`
- `public TelemetryTenantVisibility EffectiveVisibility`
- `public string? TenantId`
- `public bool WasDowngraded`
- `public bool IsCrossTenant`
- `public static TelemetryTenantScope PinnedTo(string tenantId, TelemetryTenantVisibility requestedVisibility)`
- `public static TelemetryTenantScope AtRequestedTenant(string tenantId)`
- `public static TelemetryTenantScope AcrossAllTenants()`

### `Orleans.Lattice.Api.Telemetry.TelemetryTenantVisibility`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryTenantVisibility.cs) (line 34).

`public enum TelemetryTenantVisibility`

- `ActiveTenant = 0`
- `AllTenants = 1`
- `SingleTenant = 2`

### `Orleans.Lattice.Api.Telemetry.TelemetryTimeRange`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryTimeRange.cs) (line 27).

`public readonly record struct TelemetryTimeRange`

- `public DateTimeOffset StartUtc`
- `public DateTimeOffset EndUtc`
- `public TimeSpan Step`
- `public static TelemetryTimeRange At(DateTimeOffset instant)`
- `public static TelemetryTimeRange Between(DateTimeOffset startUtc, DateTimeOffset endUtc, TimeSpan step)`
- `public TimeSpan Duration`
- `public bool IsInstant`
- `public bool IsAscending`
- `public long PointCount`
- `public TelemetryTimeRange WithStep(TimeSpan step)`

### `Orleans.Lattice.Api.Telemetry.TelemetryTimeSeries`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryTimeSeries.cs) (line 12).

`public sealed record TelemetryTimeSeries`

- `public required IReadOnlyList<TelemetryLabel> Labels`
- `public required IReadOnlyList<TelemetryDataPoint> Points`
- `public static TelemetryTimeSeries Empty`
- `public bool TryGetLabel(string name, out string? value)`
