---
agent_spec: "docs/agents/api/mcp.json"
---

# Public API

The complete public surface of `Orleans.Lattice.Api.Mcp.Telemetry`, all in the `Orleans.Lattice.Api.Mcp.Telemetry` namespace: one registration method, one options type, and the structured records the four `lattice_telemetry_*` tools return. The backend proxy, the range guardrails, the metric-access policy and the capability check are public types of the transport-neutral `Orleans.Lattice.Api.Telemetry` package, which this package composes; see [Architecture](architecture.md). The tools' arguments and behaviour are in [Tools](tools.md), and every option is tabulated in [Configuration](configuration.md).

## Registration

`AddTelemetryTools(this IServiceCollection services, Action<LatticeApiMcpTelemetryOptions> configure)`, declared on `LatticeMcpTelemetryServiceCollectionExtensions`, binds and validates `LatticeApiMcpTelemetryOptions`, registers the backend proxy and its HTTP client, registers the call-time telemetry capability check, and contributes the telemetry tool group to an `AddLatticeMcp` server. Both arguments must be non-`null`. It is idempotent: repeated calls register one tool group and one backend client. The group is advertised only to a caller holding a cluster-wide `LatticeOperation.Telemetry` grant, and only when the registered `ILatticeApiMcpAuthorizer` admits the tools.

## Options

`LatticeApiMcpTelemetryOptions` derives from `LatticeTelemetryOptions` (in `Orleans.Lattice.Api.Telemetry`) and adds no members: every setting - the backend address, the backend authentication mode and credential, the request timeout, the range guardrails and the metric-access allow-list - is inherited. The type exists so the add-on is configured and resolved by its own name. See [Configuration](configuration.md).

## Tool results

Each result is an MCP payload serialized to the tool's JSON structured content by the SDK, with no Orleans serialization attributes. A capability refusal, guardrail rejection, metric-access denial or backend fault is returned as `Success = false` with a human-readable `Error`, never as an exception.

| Record | Returned by | Shape |
|---|---|---|
| `TelemetryQueryResult` | `lattice_telemetry_query`, `lattice_telemetry_query_range` | `Success`, `Error`, the backend `ResultType` (`vector`, `matrix`, `scalar` or `string`; empty on failure) and the projected `Series`. `Failure(error)` builds a failed result. |
| `TelemetrySeries` | an element of `TelemetryQueryResult.Series` | The series' `Labels` and its `Samples`. |
| `TelemetrySample` | an element of `TelemetrySeries.Samples` | `Timestamp` in unix seconds (possibly fractional) and `Value` in the backend's raw string form, so `NaN` and `+Inf` survive without loss of precision. |
| `TelemetryMetricListResult` | `lattice_telemetry_list_metrics` | `Success`, `Error` and the admitted metric names in `Metrics`. `Failure(error)` builds a failed result. |
| `TelemetryMetricMetadataResult` | `lattice_telemetry_metric_metadata` | `Success`, `Error`, the admitted `Metrics` entries, and a non-fatal `Notice` set when a requested name resolved to no metadata (typically an exposition name with a `_total`, `_bucket`, `_count` or `_sum` suffix instead of the instrument's base name). `Failure(error)` builds a failed result; `Empty()` is the successful empty result returned when the backend has no metadata surface. |
| `TelemetryMetricMetadata` | an element of `TelemetryMetricMetadataResult.Metrics` | `Metric`, its sample `Type` (for example `counter` or `gauge`), `Help` and `Unit`; each is an empty string when the backend omits it. A metric can have more than one entry when its metadata differs across targets. |

## Related

- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Setup](setup.md), [Tools](tools.md), [Security](security.md)
- [`Orleans.Lattice.Api.Mcp` public API](../lattice.api.mcp/api.md) - the server these tools register on.
- [`Orleans.Lattice.Api.Mcp.Telemetry.Azure` public API](../lattice.api.mcp.telemetry.azure/api.md) - the managed-identity backend token for Azure managed Prometheus.

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Mcp.Telemetry.LatticeApiMcpTelemetryOptions`

[Source](../../src/lattice.api.mcp.telemetry/LatticeApiMcpTelemetryOptions.cs) (line 33).

`public sealed class LatticeApiMcpTelemetryOptions : LatticeTelemetryOptions`

### `Orleans.Lattice.Api.Mcp.Telemetry.LatticeMcpTelemetryServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp.telemetry/LatticeMcpTelemetryServiceCollectionExtensions.cs) (line 13).

`public static class LatticeMcpTelemetryServiceCollectionExtensions`

- `public static IServiceCollection AddTelemetryTools(this IServiceCollection services, Action<LatticeApiMcpTelemetryOptions> configure)`

### `Orleans.Lattice.Api.Mcp.Telemetry.TelemetryMetricListResult`

[Source](../../src/lattice.api.mcp.telemetry/TelemetryMetricListResult.cs) (line 14).

`public sealed record TelemetryMetricListResult`

- `public bool Success { get; init; }`
- `public string? Error { get; init; }`
- `public IReadOnlyList<string> Metrics { get; init; }`
- `public static TelemetryMetricListResult Failure(string error)`

### `Orleans.Lattice.Api.Mcp.Telemetry.TelemetryMetricMetadata`

[Source](../../src/lattice.api.mcp.telemetry/TelemetryMetricMetadata.cs) (line 13).

`public sealed record TelemetryMetricMetadata`

- `public required string Metric { get; init; }`
- `public string Type { get; init; }`
- `public string Help { get; init; }`
- `public string Unit { get; init; }`

### `Orleans.Lattice.Api.Mcp.Telemetry.TelemetryMetricMetadataResult`

[Source](../../src/lattice.api.mcp.telemetry/TelemetryMetricMetadataResult.cs) (line 14).

`public sealed record TelemetryMetricMetadataResult`

- `public bool Success { get; init; }`
- `public string? Error { get; init; }`
- `public IReadOnlyList<TelemetryMetricMetadata> Metrics { get; init; }`
- `public string? Notice { get; init; }`
- `public static TelemetryMetricMetadataResult Failure(string error)`
- `public static TelemetryMetricMetadataResult Empty()`

### `Orleans.Lattice.Api.Mcp.Telemetry.TelemetryQueryResult`

[Source](../../src/lattice.api.mcp.telemetry/TelemetryQueryResult.cs) (line 16).

`public sealed record TelemetryQueryResult`

- `public bool Success { get; init; }`
- `public string? Error { get; init; }`
- `public string ResultType { get; init; }`
- `public IReadOnlyList<TelemetrySeries> Series { get; init; }`
- `public static TelemetryQueryResult Failure(string error)`

### `Orleans.Lattice.Api.Mcp.Telemetry.TelemetrySample`

[Source](../../src/lattice.api.mcp.telemetry/TelemetrySample.cs) (line 13).

`public sealed record TelemetrySample`

- `public double Timestamp { get; init; }`
- `public string Value { get; init; }`

### `Orleans.Lattice.Api.Mcp.Telemetry.TelemetrySeries`

[Source](../../src/lattice.api.mcp.telemetry/TelemetrySeries.cs) (line 13).

`public sealed record TelemetrySeries`

- `public IReadOnlyDictionary<string, string> Labels { get; init; }`
- `public IReadOnlyList<TelemetrySample> Samples { get; init; }`
