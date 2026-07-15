# Orleans.Lattice.Api.Mcp.Telemetry

Optional, opt-in **telemetry** add-on for [`Orleans.Lattice.Api.Mcp`](https://github.com/NSTA1/Orleans.Lattice). It exposes a cluster's OpenTelemetry metrics as MCP tools by proxying a read-only Prometheus / PromQL-compatible backend, so an AI agent that already drives the cluster through the MCP endpoint can also ask operational questions ("what is the WAL append rate?") over the same authenticated surface.

## What it gives you

- **Metrics over MCP** - a telemetry tool group that answers instant queries, range queries, metric-name listings, and metric metadata by proxying the configured Prometheus / PromQL-compatible backend. Read-only.
- **A dual-credential trust boundary** - the MCP side authorizes the caller by a `Telemetry` grant (which no other operation, not even administrator, confers), while the proxy authenticates to the backend with a separately configured backend credential (bearer, basic, or mutual-TLS). The caller's Lattice credential is never forwarded to the backend.
- **Guardrails** - a request timeout, a maximum range and step for range queries, and a metric-access mode that is read-all by default or deny-all with an explicit allow-list of exact names and patterns.

Add it alongside the MCP binding and the tool modules you want:

```csharp
builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
builder.Services.AddTelemetryTools(o =>
{
    o.BackendAddress = new Uri("https://prometheus.internal:9090/");
    o.AuthMode = LatticeTelemetryBackendAuthMode.Bearer;
    o.Credential = new LatticeTelemetryBackendCredential { BearerToken = "..." };
});

var app = builder.Build();
app.MapLatticeMcp();
```

`AddTelemetryTools(...)` is idempotent and registers exactly one telemetry tool group. The group is advertised only to a caller whose effective permissions grant the `Telemetry` operation, so an ungranted caller is offered no telemetry tools at all.

See the [MCP API documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.mcp/README.md) for the binding's security and discovery model.
