# Orleans.Lattice.Api.Telemetry

Transport-neutral **telemetry proxy** for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). It holds the machinery that turns a cluster's OpenTelemetry metrics into answerable queries - the read-only Prometheus / PromQL-compatible backend client, the credential boundary in front of it, the range guardrails, and the fail-closed metric-access allow-list - without depending on any transport.

Every telemetry binding adapts over this one package, so the security-critical PromQL code exists exactly once and is never forked per transport.

## What it gives you

- **The curated telemetry facade** - `AddLatticeTelemetryApi()` registers `ILatticeTelemetry` (implemented by `LatticeTelemetry`) over a server-authored named-query catalogue: a caller selects a query by id and never supplies PromQL, the facade derives the tenant scope from the authenticated caller and reports the scope it applied on every response, and discovery degrades to an empty catalogue rather than failing. It calls `AddLatticeTelemetryBackend()` itself.
- **A read-only backend client** - `IPrometheusQueryClient` covers the four operations a telemetry surface needs: instant query, range query, metric-name listing, and metric metadata.
- **A dual-credential trust boundary** - the proxy stamps the configured **backend** credential (bearer, basic, dynamic bearer, or mutual-TLS) on every backend request. It takes no dependency on any Lattice credential source, so a caller's Lattice credential can never be forwarded to the backend. `ITelemetryBackendTokenProvider` is the seam a cloud-identity add-on plugs a rotating token into.
- **Guardrails** - a request timeout on every backend call, plus a maximum range and step for range queries that `TelemetryRangeGuardrails` applies so every binding rejects an over-budget request with the same message.
- **A capability gate** - `TelemetryAccessAuthorizer` checks the cluster-wide `Telemetry` capability. The facade runs it before every query (discovery degrades to the empty catalogue instead), and a binding that evaluates caller-supplied PromQL - the MCP tool group - registers it and runs it before any backend call. `AddLatticeTelemetryBackend()` does not register it, and the allow-list below only narrows what an authorized caller may read.
- **A fail-closed metric-access allow-list** - `TelemetryMetricAccessPolicy` is read-all by default or deny-all with an explicit list of exact names and `*` patterns, each matched against the whole name (anchored at both ends, never across a newline, and without backtracking). Under deny-all, `TelemetryQueryAuthorizer` gates a PromQL expression against it using `PromQlMetricExtractor`, a conservative scanner that **denies** a query carrying a `__name__` regex or negative matcher, a malformed or unterminated `__name__` matcher, an unconstrained (or unterminated) label-only selector, or no metric name it can extract at all, rather than admitting what it cannot prove safe. It skips `#` comments and quoted strings as Prometheus's own lexer does - including inside a grouping modifier's label list - so neither can hide a metric selector from the gate.

Register the backend once, after binding the options:

```csharp
services.Configure<LatticeTelemetryOptions>(o =>
{
    o.BackendAddress = new Uri("https://prometheus.internal:9090/");
    o.AuthMode = LatticeTelemetryBackendAuthMode.Bearer;
    o.Credential = new LatticeTelemetryBackendCredential { BearerToken = "..." };
    o.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
    o.AllowedMetrics.Add("orleans_lattice_*");
});

services.AddLatticeTelemetryBackend();
```

`AddLatticeTelemetryBackend()` is idempotent and defers to an `IPrometheusQueryClient` the host registered first, so a test or an alternative backend can be substituted without touching the policy wiring.

This package starts nothing and exposes no transport of its own. The gRPC binding for the curated facade ships in `Orleans.Lattice.Api.Telemetry.Grpc`, and the MCP tool group ships in `Orleans.Lattice.Api.Mcp.Telemetry`.
