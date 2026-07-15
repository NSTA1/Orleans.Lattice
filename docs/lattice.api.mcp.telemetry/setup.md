# Setup

Registering the `Orleans.Lattice.Api.Mcp.Telemetry` tool module on an MCP host, pointing it at a metrics backend, and configuring the credential, guardrails, and metric-access allow-list.

## Prerequisites

The telemetry tools plug into the `Orleans.Lattice.Api.Mcp` binding, so the host must:

- Have called `AddLatticeMcp(...)` (co-hosted) or `AddLatticeMcpRemote(...)` (remote) to register the MCP front door - see [MCP setup](../lattice.api.mcp/setup.md).
- Reach a read-only Prometheus / PromQL-compatible HTTP backend that scrapes the cluster's `orleans.lattice` metrics - see [Metrics](../lattice/metrics.md).

The telemetry group needs neither an `Orleans.Lattice.Api.*` facade nor an in-silo activation: it proxies the metrics backend directly, so it works identically co-hosted or remote.

## Register the tool module

`AddTelemetryTools(...)` binds and validates `LatticeApiMcpTelemetryOptions`, registers the default HTTP-backed backend client and the metric-access policy (both built once), and registers the telemetry tool group so its tools are advertised to a caller holding a `LatticeOperation.Telemetry` grant. It is idempotent: calling it twice registers exactly one tool group and one backend client.

```csharp verify
using Orleans.Lattice.Api.Mcp.Telemetry;

var services = new ServiceCollection();
services.AddLatticeMcp();

services.AddTelemetryTools(o =>
{
    o.BackendAddress = new Uri("https://prometheus.internal:9090/");
    o.RequestTimeout = TimeSpan.FromSeconds(30);
    o.MaxRange = TimeSpan.FromHours(24);
    o.MaxStep = TimeSpan.FromHours(1);
});
```

## Options

`LatticeApiMcpTelemetryOptions` (populated through the `AddTelemetryTools` delegate):

| Option | Type | Default | Purpose |
|---|---|---|---|
| `BackendAddress` | `Uri?` | none | The absolute base address of the read-only Prometheus / PromQL-compatible backend. A host that opts telemetry in must supply one. |
| `AuthMode` | `LatticeTelemetryBackendAuthMode` | `None` | How the proxy authenticates to the backend: `None`, `Bearer`, `Basic`, or `MutualTls`. Any non-`None` mode requires the matching `Credential` member. |
| `Credential` | `LatticeTelemetryBackendCredential?` | `null` | The backend credential secret, consulted per `AuthMode`. Carries the backend credential only - never the caller's Lattice credential. |
| `RequestTimeout` | `TimeSpan` | 30s | The per-request timeout for a backend call. Must be strictly positive. |
| `MaxRange` | `TimeSpan` | 24h | The largest window (`end - start`) a single range query may span. Must be strictly positive. |
| `MaxStep` | `TimeSpan` | 1h | The largest resolution step a single range query may request. Must be strictly positive. |
| `MetricAccess` | `LatticeTelemetryMetricAccessMode` | `ReadAll` | `ReadAll` exposes every backend metric; `DenyAllExceptAllowed` restricts the surface to `AllowedMetrics`. |
| `AllowedMetrics` | `IList<string>` | empty | Exact names and/or `*`-wildcard patterns permitted under `DenyAllExceptAllowed`. Ignored under `ReadAll`. |

The options are validated at startup: the backend address must be an absolute URI, the timeouts and range guardrails must be strictly positive, a non-`None` auth mode must carry its matching credential member, and `DenyAllExceptAllowed` must list at least one allowed metric.

## Backend authentication

Pick the mode that matches the backend and supply the matching `Credential` member:

```csharp verify
using Orleans.Lattice.Api.Mcp.Telemetry;

var services = new ServiceCollection();
services.AddLatticeMcp();

// HTTP basic authentication to the backend.
services.AddTelemetryTools(o =>
{
    o.BackendAddress = new Uri("https://prometheus.internal:9090/");
    o.AuthMode = LatticeTelemetryBackendAuthMode.Basic;
    o.Credential = new LatticeTelemetryBackendCredential
    {
        BasicUsername = "lattice-reader",
        BasicPassword = "backend-secret",
    };
});
```

- `Bearer` stamps `Authorization: Bearer <token>` from `Credential.BearerToken`.
- `Basic` stamps `Authorization: Basic <base64(user:password)>` from `Credential.BasicUsername` / `Credential.BasicPassword`.
- `MutualTls` presents `Credential.ClientCertificate` on the transport handler (no `Authorization` header).
- `None` sends no credential.

## Restrict the metric surface

By default the proxy reads any metric the backend exposes. To limit it to an explicit allow-list, switch to `DenyAllExceptAllowed` and list the exact names and/or `*` patterns:

```csharp verify
using Orleans.Lattice.Api.Mcp.Telemetry;

var services = new ServiceCollection();
services.AddLatticeMcp();

services.AddTelemetryTools(o =>
{
    o.BackendAddress = new Uri("https://prometheus.internal:9090/");
    o.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
    o.AllowedMetrics.Add("lattice_wal_append_total");   // exact name
    o.AllowedMetrics.Add("lattice_shard_*");            // wildcard pattern
});
```

Wildcard patterns are precompiled once when the policy is built, so a per-call admission check never recompiles a pattern. See [Security](security.md) for how the allow-list is enforced across the four tools.

## Next

- [Tools](tools.md) - the four telemetry tools and their arguments and results.
- [Security](security.md) - the dual-credential trust boundary and the metric-access allow-list.
