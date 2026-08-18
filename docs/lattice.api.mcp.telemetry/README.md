# Orleans.Lattice.Api.Mcp.Telemetry

An optional, opt-in telemetry add-on for [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md). It exposes the cluster's OpenTelemetry metrics to an AI agent over MCP as a **read-only** proxy in front of a Prometheus / PromQL-compatible backend, so an agent can ask the cluster "how is it doing?" the same way it drives every other Lattice MCP tool.

## What is it?

The core [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md) server advertises the cluster's API facades as MCP tools. This companion package adds a further, opt-in tool group - **telemetry** - that turns a caller's PromQL question into a query against the metrics backend the cluster already publishes to (the `orleans.lattice` meter, scraped into Prometheus; see [Metrics](../lattice/metrics.md)).

It is deliberately a thin, read-only proxy:

- **Four read-only tools.** `lattice_telemetry_query`, `lattice_telemetry_query_range`, `lattice_telemetry_list_metrics`, and `lattice_telemetry_metric_metadata` cover instant queries, range queries, metric-name discovery, and metric metadata. There are no write, delete, or control verbs - the group is read-only by construction.
- **A dual-credential trust boundary.** The MCP-side authorization (a `LatticeOperation.Telemetry` grant) and the backend-side credential are two independent halves. The proxy stamps the configured *backend* credential on every backend call and **never** forwards the caller's Lattice credential to the backend.
- **A metric-access allow-list.** A `ReadAll` default exposes every backend metric; a `DenyAllExceptAllowed` posture restricts the surface to an explicit set of exact names and `*` wildcards, enforced on every query, listing, and metadata call.
- **Range guardrails.** A range query is bounded by a configured maximum window and step, so a single call cannot ask the backend for an unbounded scan.

## Core properties

- **Read-only.** Every telemetry tool carries `readOnlyHint = true` and `destructiveHint = false`. The package exposes no way to mutate the backend or the cluster.
- **Opt-in and permission-scoped.** The tools appear only when the host calls `AddTelemetryTools(...)` **and** the authenticated caller holds a `LatticeOperation.Telemetry` grant. An ungranted caller never sees the group, exactly like every other MCP tool module.
- **Cluster-wide capability.** `LatticeOperation.Telemetry` is a cluster-wide capability, deliberately distinct from the data-plane operations: it is granted explicitly over the all-trees sentinel scope (`LatticeScope.ClusterWide()`) and is conferred by no other operation, including `Admin`.
- **Credential isolation by construction.** The backend client's only collaborators are an `HttpClient` and the telemetry options; it has no reference to any Lattice credential source, so the caller's identity cannot leak to the backend.
- **Fail-clean.** A backend timeout, HTTP failure, non-success status, malformed payload, guardrail rejection, or metric-access denial is returned as a structured result with `Success = false` and a human-readable `Error`, never as an unhandled exception. A genuine caller cancellation still propagates.

## Quick start

Add the telemetry tool module alongside the MCP front door, pointing it at a read-only Prometheus / PromQL-compatible backend and stamping the backend credential the proxy should present:

```csharp verify
using Orleans.Lattice.Api.Mcp.Telemetry;

var services = new ServiceCollection();

// The MCP front door (from Orleans.Lattice.Api.Mcp).
services.AddLatticeMcp();

// Opt in to the telemetry tool module: point it at the metrics backend and
// stamp a backend bearer token. The caller's Lattice credential is never
// forwarded to the backend.
services.AddTelemetryTools(o =>
{
    o.BackendAddress = new Uri("https://prometheus.internal:9090/");
    o.AuthMode = LatticeTelemetryBackendAuthMode.Bearer;
    o.Credential = new LatticeTelemetryBackendCredential { BearerToken = "backend-token" };
});
```

A caller holding a cluster-wide `Telemetry` grant then discovers the four `lattice_telemetry_*` tools and asks PromQL questions such as `rate(lattice_wal_append_total[5m])` over MCP. Grant the capability with an ordinary Allow rule over the all-trees sentinel scope:

```csharp verify
using Orleans.Lattice.Auth;

// A cluster-wide Telemetry grant: an ordinary Allow rule over the all-trees
// sentinel scope, conferring the LatticeOperation.Telemetry capability.
var rule = new LatticeAuthorizationRule(
    "agent-telemetry",
    LatticeSubjectSelector.User("agent"),
    LatticeScope.ClusterWide(),
    LatticeOperation.Telemetry,
    LatticeEffect.Allow);
```

For a complete, runnable host that proxies a real Prometheus instance running in Docker and drives the telemetry tools over a real MCP client, see the [`McpTelemetry`](../../samples/McpTelemetry) sample under [`samples/`](../../samples).

## Reference

- [Setup](setup.md) - registering the tool module, the backend, the credential, and the guardrails.
- [Tools](tools.md) - the four telemetry tools and their arguments and results.
- [Security](security.md) - the dual-credential trust boundary, the `Telemetry` capability, and the metric-access allow-list.

## See also

- [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md) - the MCP server this package extends.
- [Metrics](../lattice/metrics.md) - the `orleans.lattice` meter whose metrics the backend scrapes and these tools read.
- [Security](../lattice/security.md) - the authorization model the `Telemetry` capability plugs into.
