---
agent_spec: "docs/agents/api/mcp.json"
---

# Configuration

`AddTelemetryTools(Action<LatticeApiMcpTelemetryOptions> configure)` binds `LatticeApiMcpTelemetryOptions` with `services.Configure` and registers an options validator, which runs when the options are first resolved. The type inherits every setting from `LatticeTelemetryOptions` in the transport-neutral `Orleans.Lattice.Api.Telemetry` package and declares none of its own; the validator delegates to that package's `LatticeTelemetryOptionsValidator`. The neutral proxy, guardrails and allow-list are bound to the very same instance, so one object configures them all.

For the walkthrough, including backend authentication and restricting the metric surface, see [Setup](setup.md). The MCP server itself is configured separately; see [`Orleans.Lattice.Api.Mcp` configuration](../lattice.api.mcp/configuration.md).

## `LatticeApiMcpTelemetryOptions`

| Option | Type | Default | Validation and meaning |
|---|---|---|---|
| `BackendAddress` | `Uri?` | `null` | Required, and must be absolute. The base address of the read-only Prometheus / PromQL-compatible backend. |
| `AuthMode` | `LatticeTelemetryBackendAuthMode` | `None` | Must be a defined value: `None`, `Bearer`, `Basic`, `MutualTls` or `DynamicBearer`. Selects the backend credential the proxy presents; the caller's Lattice credential is never forwarded. |
| `Credential` | `LatticeTelemetryBackendCredential?` | `null` | The backend secret, consulted per `AuthMode`: `Bearer` requires a non-empty `BearerToken`, `Basic` a non-empty `BasicUsername` (`BasicPassword` is not checked), and `MutualTls` a `ClientCertificate`. `None` and `DynamicBearer` need none. |
| `RequestTimeout` | `TimeSpan` | 30 seconds | Must be strictly positive and at most `int.MaxValue` milliseconds. The per-request timeout of a backend call. |
| `MaxRange` | `TimeSpan` | 24 hours | Must be strictly positive. The widest window a single range query may span. |
| `MaxStep` | `TimeSpan` | 1 hour | Must be strictly positive. The largest resolution step a range query may request. |
| `MetricAccess` | `LatticeTelemetryMetricAccessMode` | `ReadAll` | Must be a defined value. `ReadAll` exposes every backend metric; `DenyAllExceptAllowed` restricts every query, listing and metadata call to `AllowedMetrics`. |
| `AllowedMetrics` | `IList<string>` | empty | Exact names and `*` wildcards admitted under `DenyAllExceptAllowed`, where it must hold at least one entry and no null or blank entry. Ignored under `ReadAll`. Get-only. |

`DynamicBearer` draws its token from a registered `ITelemetryBackendTokenProvider` on each backend request. The options validator cannot check for that service; a request made without one, or with a provider that returns an empty token, fails closed and is never sent unauthenticated. For Azure managed Prometheus, register the provider from [`Orleans.Lattice.Api.Mcp.Telemetry.Azure`](../lattice.api.mcp.telemetry.azure/configuration.md).

## Authorization is not configuration

No option grants access. The tools are offered and served only to a caller holding the cluster-wide `LatticeOperation.Telemetry` capability, and only when the registered MCP authorizer admits them; the allow-list then narrows what such a caller may read. See [Security](security.md).

## Source map

- [Options](../../src/lattice.api.mcp.telemetry/LatticeApiMcpTelemetryOptions.cs) and the inherited [neutral options](../../src/lattice.api.telemetry/LatticeTelemetryOptions.cs)
- [Validation rules](../../src/lattice.api.telemetry/LatticeTelemetryOptionsValidator.cs)
- [Registration](../../src/lattice.api.mcp.telemetry/LatticeMcpTelemetryServiceCollectionExtensions.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
- [Setup](setup.md), [Security](security.md)
