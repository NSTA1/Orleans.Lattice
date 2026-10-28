---
agent_spec: "docs/agents/api/mcp.json"
---

# Public API

`Orleans.Lattice.Api.Mcp.Telemetry.Azure` has a two-type public surface in the `Orleans.Lattice.Api.Mcp.Telemetry.Azure` namespace. It implements the `ITelemetryBackendTokenProvider` seam of the transport-neutral `Orleans.Lattice.Api.Telemetry` package for Azure managed Prometheus, and references only that package, not the MCP server. The [package guide](README.md) shows the registration; [Architecture](architecture.md) describes the token cache.

## `AddAzureTelemetryBackendToken`

Declared on the static class `LatticeAzureTelemetryTokenServiceCollectionExtensions`:

`public static IServiceCollection AddAzureTelemetryBackendToken(this IServiceCollection services, Action<AzureTelemetryBackendTokenOptions> configure)`

It binds `AzureTelemetryBackendTokenOptions` from `configure`, registers their validator, and registers the Azure token provider as the singleton `ITelemetryBackendTokenProvider` with `TryAdd`. Both arguments must be non-`null`. A second call rebinds the options but registers exactly one provider, and a host that registered its own `ITelemetryBackendTokenProvider` first keeps it.

The provider is consulted only when the telemetry options select `LatticeTelemetryBackendAuthMode.DynamicBearer`. Call it alongside whichever call registers the telemetry backend: [`AddTelemetryTools`](../lattice.api.mcp.telemetry/api.md#registration) for the MCP binding, or `AddLatticeTelemetryBackend` for a head hosting the neutral facade directly.

## `AzureTelemetryBackendTokenOptions`

| Member | Type | Meaning |
|---|---|---|
| `ManagedPrometheusScope` | `const string` | `https://prometheus.monitor.azure.com/.default`, the Azure Monitor managed-Prometheus query scope and the default `Scope`. |
| `Credential` | `TokenCredential?` (`Azure.Core`) | The credential the access token is acquired from. Required. |
| `Scope` | `string` | The scope the token is requested for. |
| `RefreshSkew` | `TimeSpan` | How long before expiry a cached token is replaced. |

Defaults and validation are in [Configuration](configuration.md).

## Related

- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Package guide](README.md)
- [`Orleans.Lattice.Api.Mcp.Telemetry` public API](../lattice.api.mcp.telemetry/api.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Mcp.Telemetry.Azure.AzureTelemetryBackendTokenOptions`

[Source](../../src/lattice.api.mcp.telemetry.azure/AzureTelemetryBackendTokenOptions.cs) (line 27).

`public sealed class AzureTelemetryBackendTokenOptions`

- `public const string ManagedPrometheusScope`
- `public TokenCredential? Credential { get; set; }`
- `public string Scope { get; set; }`
- `public TimeSpan RefreshSkew { get; set; }`

### `Orleans.Lattice.Api.Mcp.Telemetry.Azure.LatticeAzureTelemetryTokenServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp.telemetry.azure/LatticeAzureTelemetryTokenServiceCollectionExtensions.cs) (line 20).

`public static class LatticeAzureTelemetryTokenServiceCollectionExtensions`

- `public static IServiceCollection AddAzureTelemetryBackendToken(this IServiceCollection services, Action<AzureTelemetryBackendTokenOptions> configure)`
