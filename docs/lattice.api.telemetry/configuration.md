---
agent_spec: "docs/agents/api/telemetry.json"
---

# Configuration

`LatticeTelemetryOptions` is the package's only options type. It configures the metrics backend the facade proxies to, the credential the proxy presents to that backend, the deployment-wide range guardrails, and the metric allow-list. Neither `AddLatticeTelemetryApi()` nor `AddLatticeTelemetryBackend()` takes a configure delegate: bind the options with the options pattern (for example `services.Configure<LatticeTelemetryOptions>(...)`, as in the [registration example](README.md#registration)). The options are read once, when the facade, proxy and access policy are first constructed.

## `LatticeTelemetryOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `BackendAddress` | `Uri?` | `null` | The Prometheus-compatible endpoint. Must be absolute. Unset means no backend is configured: discovery returns the empty catalogue and every query reports as unoffered (`TelemetryQueryNotFoundException`). |
| `AuthMode` | `LatticeTelemetryBackendAuthMode` | `None` | How the proxy authenticates to the backend: `None`, `Bearer`, `Basic`, `MutualTls`, or `DynamicBearer` (a token resolved before each backend request through `ITelemetryBackendTokenProvider`). |
| `Credential` | `LatticeTelemetryBackendCredential?` | `null` | The static backend credential for `Bearer` (`BearerToken`), `Basic` (`BasicUsername` / `BasicPassword`), or `MutualTls` (`ClientCertificate`). Not consulted under `None` or `DynamicBearer`. |
| `RequestTimeout` | `TimeSpan` | 30 seconds | Per-request timeout against the backend, applied to the proxy's `HttpClient`. Must be strictly positive and no longer than `int.MaxValue` milliseconds (about 24.8 days), the longest finite timeout `HttpClient` accepts. |
| `MaxRange` | `TimeSpan` | 24 hours | The widest window a range query may evaluate. A wider requested window is refused with `TelemetryQueryBoundsException`, not narrowed. It also caps the default window used when a range request leaves its start unset. |
| `MaxStep` | `TimeSpan` | 1 hour | The coarsest step a range query may request. |
| `MetricAccess` | `LatticeTelemetryMetricAccessMode` | `ReadAll` | `ReadAll`, or `DenyAllExceptAllowed` to serve only metrics named by `AllowedMetrics`. |
| `AllowedMetrics` | `IList<string>` | empty | The allow-list consulted under `DenyAllExceptAllowed`. Get-only: add entries to the existing list. Each entry is an exact Prometheus exposition name or a `*` wildcard pattern (for example `orleans_lattice_wal_*`). Matching is whole-name and case-sensitive: a wildcard entry is anchored at both ends, its `*` never matches a newline, and it is matched without backtracking, in time linear in the name's length. Ignored under `ReadAll`. |

### `LatticeTelemetryBackendCredential`

| Property | Type | Used by |
|---|---|---|
| `BearerToken` | `string?` | `Bearer`: sent as `Authorization: Bearer <token>` on every backend request. |
| `BasicUsername` | `string?` | `Basic`: the user name of the `Authorization: Basic` header. |
| `BasicPassword` | `string?` | `Basic`: the password of the `Authorization: Basic` header. |
| `ClientCertificate` | `X509Certificate2?` | `MutualTls`: added to the proxy's primary HTTP handler as a client certificate. |

The backend credential flows only toward the backend: the proxy never forwards the caller's Lattice credential to it.

### Dynamic bearer tokens

Under `DynamicBearer` the proxy asks the registered `ITelemetryBackendTokenProvider` for a token before each backend request and stamps it as a bearer token. The provider is a DI service rather than an options value, so its presence is checked at first use: a backend request with no provider registered, or one that returns an empty token, fails in the proxy with `InvalidOperationException`, which the facade reports as `TelemetryBackendException`. The `Orleans.Lattice.Api.Mcp.Telemetry.Azure` package provides an Azure managed-identity implementation.

## Validation

Registration adds no options validation; the host owns binding and validating the options. Register `LatticeTelemetryOptionsValidator` as an `IValidateOptions<LatticeTelemetryOptions>` to enforce, when the options are first resolved:

- `BackendAddress` is supplied and absolute (under the validator an unset address fails resolution rather than degrading to the empty catalogue);
- `AuthMode` and `MetricAccess` are defined enum values;
- the credential member each static mode needs is present: `BearerToken` for `Bearer`, `BasicUsername` for `Basic`, `ClientCertificate` for `MutualTls`;
- `RequestTimeout` is strictly positive and at most `int.MaxValue` milliseconds;
- `MaxRange` and `MaxStep` are strictly positive;
- under `DenyAllExceptAllowed`, `AllowedMetrics` is non-empty and contains no null, empty or whitespace entry.

## Registration-time seams

| Seam | Registered by | Replacement rule |
|---|---|---|
| `IPrometheusQueryClient` | `AddLatticeTelemetryBackend` | The default `PrometheusQueryClient` is added only when no `IPrometheusQueryClient` is already registered; register your own first. |
| `LatticeTelemetryQueryCatalog` | `AddLatticeTelemetryApi` (`TryAdd`) | Register a catalogue built from your own `TelemetryQueryDefinition` set first to curate different queries. |
| `ITelemetryBackendTokenProvider` | not registered | Register one when `AuthMode` is `DynamicBearer`. |

Every other service `AddLatticeTelemetryApi` adds - the metric access policy, the capability authorizer, the tenant-scope resolver and the facade itself - is also registered with `TryAdd`.

## Source map

- [Options](../../src/lattice.api.telemetry/LatticeTelemetryOptions.cs)
- [Backend credential](../../src/lattice.api.telemetry/LatticeTelemetryBackendCredential.cs)
- [Validator](../../src/lattice.api.telemetry/LatticeTelemetryOptionsValidator.cs)
- [Registration](../../src/lattice.api.telemetry/LatticeApiTelemetryServiceCollectionExtensions.cs)
- [Token provider seam](../../src/lattice.api.telemetry/ITelemetryBackendTokenProvider.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
- [gRPC binding configuration](../lattice.api.telemetry.grpc/configuration.md)
