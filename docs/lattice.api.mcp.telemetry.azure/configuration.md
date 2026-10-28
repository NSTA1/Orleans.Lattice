---
agent_spec: "docs/agents/api/mcp.json"
---

# Configuration

`AddAzureTelemetryBackendToken(Action<AzureTelemetryBackendTokenOptions> configure)` binds `AzureTelemetryBackendTokenOptions` with `services.Configure` and registers an options validator. No start-up validation is registered, so the options are validated when they are first resolved - on the first backend token request.

The provider is used only when the telemetry backend's `AuthMode` is `DynamicBearer`. That setting, the backend address and the other proxy options belong to the telemetry package: see [`Orleans.Lattice.Api.Mcp.Telemetry` configuration](../lattice.api.mcp.telemetry/configuration.md).

## `AzureTelemetryBackendTokenOptions`

| Option | Type | Default | Validation and meaning |
|---|---|---|---|
| `Credential` | `TokenCredential?` (`Azure.Core`) | `null` | Required. The workload or managed identity the token is acquired from, for example `DefaultAzureCredential` or `ManagedIdentityCredential`. Only the minted bearer token reaches the backend. |
| `Scope` | `string` | `https://prometheus.monitor.azure.com/.default` (`ManagedPrometheusScope`) | Must be non-blank. The audience the token is requested for. |
| `RefreshSkew` | `TimeSpan` | 5 minutes | Must not be negative. A cached token is served until it is within this margin of expiry, then re-acquired, so a query never presents an about-to-expire token. |

## Source map

- [Options](../../src/lattice.api.mcp.telemetry.azure/AzureTelemetryBackendTokenOptions.cs)
- [Validation rules](../../src/lattice.api.mcp.telemetry.azure/AzureTelemetryBackendTokenOptionsValidator.cs)
- [Registration](../../src/lattice.api.mcp.telemetry.azure/LatticeAzureTelemetryTokenServiceCollectionExtensions.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
- [Package guide](README.md)
