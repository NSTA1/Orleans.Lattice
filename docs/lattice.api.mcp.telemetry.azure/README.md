# Orleans.Lattice.Api.Mcp.Telemetry.Azure

Azure managed-identity backend-token provider for [`Orleans.Lattice.Api.Mcp.Telemetry`](../lattice.api.mcp.telemetry/README.md). It satisfies the telemetry proxy's `DynamicBearer` backend-auth mode with a rotating Entra (Azure AD) access token, so the MCP cluster-telemetry tools can query an **Azure Monitor workspace (managed Prometheus)** endpoint - which authenticates callers with a short-lived AAD bearer token rather than a static credential.

## When to use it

The core telemetry package's static auth modes (`None`, `Bearer`, `Basic`, `MutualTls`) cover a self-hosted Prometheus. Azure managed Prometheus instead requires an Entra token that rotates roughly hourly, so a static `Bearer` string cannot work. This companion plugs into the core's `ITelemetryBackendTokenProvider` seam and owns token acquisition, caching, and refresh - keeping the `Azure.Core` dependency out of the core package. KEDA and Grafana authenticate to the same workspace with native workload identity; this closes the equivalent gap for the MCP telemetry proxy.

## How it works

- The host selects `LatticeTelemetryBackendAuthMode.DynamicBearer` on `AddTelemetryTools(...)` and registers this provider with `AddAzureTelemetryBackendToken(...)`.
- Before each backend query the proxy asks the provider for a token. The provider serves a cached token and only calls the Azure credential when the cache is empty or within `RefreshSkew` of expiry.
- Concurrent queries during a rotation share a single in-flight acquisition, so a refresh never fans out into a burst of credential calls.
- The host supplies the credential (a workload or managed identity); only the minted bearer token reaches the backend, and the caller's Lattice credential is never forwarded.

## Register it

Point the proxy at the workspace's query endpoint, select `DynamicBearer`, and supply an Azure credential:

```csharp verify
using Azure.Identity;
using Orleans.Lattice.Api.Mcp.Telemetry;
using Orleans.Lattice.Api.Mcp.Telemetry.Azure;

var services = new ServiceCollection();
services.AddLatticeMcp();

services.AddTelemetryTools(o =>
{
    o.BackendAddress = new Uri("https://my-workspace.eastus.prometheus.monitor.azure.com/");
    o.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer;
});

services.AddAzureTelemetryBackendToken(o =>
{
    o.Credential = new DefaultAzureCredential();
    // o.Scope defaults to AzureTelemetryBackendTokenOptions.ManagedPrometheusScope
    // ("https://prometheus.monitor.azure.com/.default").
    o.RefreshSkew = TimeSpan.FromMinutes(5);
});
```

## Options

`AzureTelemetryBackendTokenOptions` (populated through the `AddAzureTelemetryBackendToken` delegate):

| Option | Type | Default | Purpose |
|---|---|---|---|
| `Credential` | `TokenCredential?` | none | The Azure credential the access token is acquired from (for example `new DefaultAzureCredential()` or a `ManagedIdentityCredential`). Required. |
| `Scope` | `string` | `ManagedPrometheusScope` | The scope the token is audienced for. Defaults to the Azure Monitor managed-Prometheus query scope. Must be non-empty. |
| `RefreshSkew` | `TimeSpan` | 5m | How long before expiry the provider proactively re-acquires, so an in-flight query never presents an about-to-expire token. Must be non-negative. |

The options are validated at startup: a credential must be supplied, the scope must be non-empty, and the refresh skew must not be negative.
