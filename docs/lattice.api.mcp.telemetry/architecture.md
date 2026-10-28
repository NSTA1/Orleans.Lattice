---
agent_spec: "docs/agents/api/mcp.json"
---

# Architecture

`Orleans.Lattice.Api.Mcp.Telemetry` is the MCP binding over the transport-neutral [`Orleans.Lattice.Api.Telemetry`](../lattice.api.telemetry/README.md) package. That package owns the backend proxy, the range guardrails, the metric-access allow-list and the capability check; this one adds only the four read-only `lattice_telemetry_*` tools and wires them so the same rules apply as for every other transport. Internal collaborators are described by behaviour; for the public types see [Public API](api.md).

## Composition

`AddTelemetryTools` does four things:

1. Binds and validates `LatticeApiMcpTelemetryOptions`, and registers `IOptions<LatticeTelemetryOptions>` as that same instance, so the neutral proxy, policy and guardrails observe exactly the values the host configured.
2. Calls `AddLatticeTelemetryBackend`, which registers the `TelemetryMetricAccessPolicy` (its wildcard patterns compiled once from the options) and, unless the host registered its own `IPrometheusQueryClient` first, the default `PrometheusQueryClient` with its `HttpClient`.
3. Registers the `TelemetryAccessAuthorizer` capability check. It is registered here, not inherited from the neutral facade's registration, because a host can expose the MCP tools without that facade and the tools must be gated either way.
4. Contributes the telemetry tool group (`LatticeApiMcpGroup.Telemetry`) to the MCP server.

## Request path

Discovery offers the group only to a caller holding `LatticeOperation.Telemetry` from an Allow rule at cluster-wide scope, and the MCP server's lock-step authorizer must admit each tool (see [`Orleans.Lattice.Api.Mcp` architecture](../lattice.api.mcp/architecture.md#request-path)). Because a tool name is guessable, discovery is not the gate; each call then runs, in order:

1. **Capability.** The handler checks the cluster-wide `Telemetry` capability through `TelemetryAccessAuthorizer`, the seam the neutral `ILatticeTelemetry` facade also consults. It runs first, so an unauthorized caller learns nothing - not even the configured range budget from a guardrail message.
2. **Guardrails.** A range query is checked against `MaxRange` and `MaxStep` by `TelemetryRangeGuardrails`.
3. **Allow-list.** The PromQL expression is checked against the metric-access policy by `TelemetryQueryAuthorizer`, which fails closed: an unparseable or ambiguous metric-name matcher is treated as not allowed. The allow-list only narrows; under the `ReadAll` default it admits every name, which is why the capability check precedes it.
4. **Backend call.** `IPrometheusQueryClient` sends the query, stamping the backend credential selected by `AuthMode`. In `DynamicBearer` mode the token comes from a registered `ITelemetryBackendTokenProvider` on every request; a missing provider or an empty token fails the request closed rather than sending it unauthenticated.
5. **Projection.** The backend envelope is mapped into the result records. Metric listing and metadata are filtered to admitted names under `DenyAllExceptAllowed`.

Every refusal and fault becomes a `Success = false` result. A backend transport or payload fault carries a fixed message - exception text from the HTTP pipeline could echo the outbound `Authorization` header - and its detail is logged server-side, with the query string dropped from the logged path. A `404` from the metadata endpoint is read as "no metadata surface" and answered with an empty successful result. A genuine caller cancellation propagates.

## The dual-credential boundary

The two halves of the trust boundary never meet. The caller's Lattice credential, stamped by the MCP server, is used only for the capability check. The backend sees only the configured backend credential: the proxy's collaborators are its `HttpClient`, the options, the optional token provider and a logger, none of which is a Lattice credential source. See [Security](security.md).

## Remote hosting

On a remote MCP head the telemetry tools always query the head's own configured backend; `AddLatticeMcpRemote` wires no gRPC adapter for telemetry. A `Telemetry` endpoint in the remote options only advertises where a region's telemetry facade is served, for discovery and routing. See [Remote hosting](../lattice.api.mcp/remote.md#options).

## Source map

- [Registration](../../src/lattice.api.mcp.telemetry/LatticeMcpTelemetryServiceCollectionExtensions.cs)
- [Tool handlers](../../src/lattice.api.mcp.telemetry/TelemetryToolHandlers.cs) and [tool group](../../src/lattice.api.mcp.telemetry/TelemetryToolGroup.cs)
- [Backend proxy](../../src/lattice.api.telemetry/PrometheusQueryClient.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [Security](security.md), [Tools](tools.md)
- [`Orleans.Lattice.Api.Mcp.Telemetry.Azure` architecture](../lattice.api.mcp.telemetry.azure/architecture.md) - the managed-identity token provider for `DynamicBearer`.
