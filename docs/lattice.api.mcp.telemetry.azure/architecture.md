---
agent_spec: "docs/agents/api/mcp.json"
---

# Architecture

Azure managed Prometheus accepts only an Entra token that rotates roughly hourly, which no static `Bearer` setting can carry. `Orleans.Lattice.Api.Mcp.Telemetry.Azure` fills the `ITelemetryBackendTokenProvider` seam of the transport-neutral telemetry package with a provider that acquires, caches and refreshes that token, keeping the `Azure.Core` dependency out of the telemetry packages themselves. The provider type is internal and is described here by behaviour; the public surface is in [Public API](api.md).

## Where it sits

The telemetry backend proxy stamps a credential on every backend request according to `LatticeTelemetryOptions.AuthMode`. In `DynamicBearer` mode it asks the registered `ITelemetryBackendTokenProvider` for a token on each request and sends it as the bearer token. `AddAzureTelemetryBackendToken` registers this package's provider in that slot. The caller's Lattice credential plays no part: it is checked by the MCP server and the telemetry capability gate and never reaches the backend. See the [telemetry architecture](../lattice.api.mcp.telemetry/architecture.md).

## Token lifecycle

1. **Fast path.** A request first reads the single cached token without taking a lock. If the token is still more than `RefreshSkew` away from its expiry, it is returned. The cache is one immutable reference, so a reader never observes a torn token.
2. **Refresh.** Otherwise the request takes an asynchronous gate and checks the cache again, because a concurrent request may have refreshed it while this one waited. Concurrent requests during a rotation therefore share one acquisition; a refresh never fans out into a burst of credential calls.
3. **Acquisition.** The provider requests a token for `Scope` from the configured `Credential`, caches it with its expiry, and returns it. Expiry is compared against a `TimeProvider` clock - the system clock unless one is supplied.

A missing `Credential` is reported by the options validator; if the provider is reached without one, it throws rather than return an empty token. A credential failure propagates to the proxy. The proxy runs credential stamping inside the backend request's fault boundary, so either case fails that request closed - it is never sent unauthenticated - and the telemetry tool answers with its fixed backend-error result while the detail is logged server-side. An empty token returned by any provider is likewise refused by the proxy.

## Seams

| Seam | Owned by | Role here |
|---|---|---|
| `ITelemetryBackendTokenProvider` | `Orleans.Lattice.Api.Telemetry` | The slot this package fills, registered with `TryAdd` so a host's own provider wins. |
| `TokenCredential` | `Azure.Core` | The identity the host supplies; this package never constructs one. |

## Source map

- [Registration](../../src/lattice.api.mcp.telemetry.azure/LatticeAzureTelemetryTokenServiceCollectionExtensions.cs)
- [Token provider](../../src/lattice.api.mcp.telemetry.azure/AzureTelemetryBackendTokenProvider.cs)
- [Backend proxy credential stamping](../../src/lattice.api.telemetry/PrometheusQueryClient.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [Package guide](README.md)
