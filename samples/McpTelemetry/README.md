# MCP Telemetry sample

A single-process demonstration of the optional
`Orleans.Lattice.Api.Mcp.Telemetry` add-on. It co-hosts a single-silo Orleans
cluster with the Model Context Protocol (MCP) server, exports the cluster's
`orleans.lattice` metrics over Prometheus, and drives the telemetry tools with a
real MCP client - proxying a **real Prometheus instance running in Docker**.

The round-trip is genuine: the Docker Prometheus scrapes this process's
`/metrics` endpoint, and this process's `lattice_telemetry_*` tools then query
that same Prometheus over its PromQL HTTP API.

```
  [ this process ]                         [ docker compose ]
  Orleans silo + Lattice                   prometheus:9090
    |  emits orleans.lattice metrics          ^   |
    |  /metrics  (OTel Prometheus exporter) --/   |  PromQL HTTP API
    |                                             v
  MCP server + AddTelemetryTools  -----------> queries Prometheus
    ^
    |  streamable HTTP + in-process MCP client
  AI agent journey
```

It proves the headline properties of the telemetry surface:

1. **Capability-gated discovery.** An agent granted the cluster-wide
   `LatticeOperation.Telemetry` capability discovers the four read-only
   `lattice_telemetry_*` tools and runs a live PromQL query end-to-end over MCP.
2. **Permission-scoping.** The same agent, granted *only* telemetry, does **not**
   see the state tools the server also registers - and an unauthenticated caller
   is offered nothing at all.
3. **The dual-credential boundary.** The tools authenticate to Prometheus with a
   backend credential the host configures (here `None`, because the sample's
   Prometheus is unauthenticated), never the caller's Lattice identity.

## Run it

The sample needs a Prometheus to talk to, so start it first with Docker, then run
the sample:

```
docker compose up -d
dotnet run --project samples/McpTelemetry/McpTelemetry.csproj
```

The sample seeds an `agent` subject with a cluster-wide telemetry grant, drives a
burst of writes and reads to populate the `orleans.lattice` metrics, waits for
Prometheus to scrape the silo, then:

- prints the four telemetry tools the agent discovered (and confirms it sees zero
  state tools),
- runs `lattice_telemetry_query` for the silo's scrape-health (`up`),
- lists the `orleans.lattice` metric names Prometheus discovered and queries one,
- shows the anonymous caller being offered zero tools,

and exits. Tear Prometheus down afterwards with:

```
docker compose down
```

Prometheus scrapes the host process at `host.docker.internal:5290`; on Docker
Desktop this resolves automatically, and the compose file adds a host-gateway
mapping so it also works on Linux. If the sample reports that Prometheus did not
report the silo as up, confirm `docker compose up -d` is running and that port
`5290` is reachable from the container.

Authorization on the MCP endpoint is disabled purely to keep the sample
one-command runnable with no identity provider: a demo credential bridge maps a
request carrying a marker header onto a fixed `agent` credential. A real
deployment leaves `RequireAuthorization` at its secure default and lifts an
authenticated ASP.NET Core principal onto the ambient credential instead, and
points the telemetry proxy at an authenticated Prometheus with a `Bearer`,
`Basic`, or `MutualTls` backend credential.

## What to look at

- `Program.cs` - the silo + MCP host wiring (`AddOpenTelemetry().WithMetrics(...)`,
  `AddLatticeMcp` / `AddStateTools` / `AddTelemetryTools` / `MapLatticeMcp` /
  `MapPrometheusScrapingEndpoint`), the cluster-wide telemetry grant seeding, and
  the MCP client journey.
- `docker-compose.yml` and `prometheus.yml` - the real Prometheus that scrapes
  the silo and answers the telemetry tools' PromQL queries.
- `DemoCredentialBridge.cs` / `DemoAuthenticator.cs` - the fail-closed demo
  identity plumbing (shared shape with the `McpServer` sample).
- The package docs under
  [`docs/lattice.api.mcp.telemetry/`](../../docs/lattice.api.mcp.telemetry/README.md)
  cover the tool catalogue, the dual-credential trust boundary, and the
  metric-access allow-list in depth.
