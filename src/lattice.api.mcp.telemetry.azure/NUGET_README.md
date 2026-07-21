# Orleans.Lattice.Api.Mcp.Telemetry.Azure

Azure managed-identity backend-token provider for [`Orleans.Lattice.Api.Mcp.Telemetry`](https://github.com/NSTA1/Orleans.Lattice). It satisfies the telemetry proxy's `DynamicBearer` auth mode with a rotating Entra (Azure AD) access token, so the MCP cluster-telemetry tools can query an **Azure Monitor workspace (managed Prometheus)** endpoint - which requires a short-lived AAD bearer token rather than a static credential.

## Why it exists

The core telemetry package stays free of any cloud-identity dependency: its static auth modes (`None`, `Bearer`, `Basic`, `MutualTls`) cover self-hosted Prometheus, but managed Prometheus needs a token that rotates roughly hourly. This companion plugs into the core's `ITelemetryBackendTokenProvider` seam and owns token acquisition, caching, and refresh, keeping the `Azure.Core` dependency out of the core package.

## What it gives you

- **A rotating backend token** - acquires an Entra access token for the managed-Prometheus scope from a caller-supplied `Azure.Core` `TokenCredential`, caches it, and refreshes it a configurable skew before expiry.
- **Single-flight refresh** - concurrent telemetry queries share one in-flight acquisition, so a token rotation never fans out into a burst of duplicate credential calls.
- **No secrets** - the host supplies a credential (workload/managed identity); only the minted bearer token reaches the backend, and the caller's Lattice credential is never forwarded.

Add it alongside the telemetry tools, selecting `DynamicBearer`:

```csharp
using Azure.Identity;

builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
builder.Services.AddTelemetryTools(o =>
{
    o.BackendAddress = new Uri("https://<workspace>.<region>.prometheus.monitor.azure.com/");
    o.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer;
});
builder.Services.AddAzureTelemetryBackendToken(o =>
{
    o.Credential = new DefaultAzureCredential();
    // o.Scope defaults to AzureTelemetryBackendTokenOptions.ManagedPrometheusScope.
});

var app = builder.Build();
app.MapLatticeMcp();
```

`AddAzureTelemetryBackendToken(...)` is idempotent and registers exactly one provider. A host that registers its own `ITelemetryBackendTokenProvider` first keeps it.

See the [MCP telemetry documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.mcp.telemetry/README.md) for the proxy's security and discovery model.
