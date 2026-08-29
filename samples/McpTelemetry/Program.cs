using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using OpenTelemetry.Metrics;
using Orleans.Lattice;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Mcp;
using Orleans.Lattice.Api.Mcp.Telemetry;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Samples.McpTelemetry;

// ---------------------------------------------------------------------------
// Orleans.Lattice.Api.Mcp.Telemetry sample: a co-hosted single silo that
// exposes its OpenTelemetry metrics to an AI agent over MCP, proxied through a
// REAL Prometheus instance running in Docker.
//
// Topology:
//
//   [ this process ]                         [ docker compose ]
//   Orleans silo + Lattice                   prometheus:9090
//     |  emits orleans.lattice metrics          ^   |
//     |  /metrics  (OTel Prometheus exporter) --/   |  PromQL HTTP API
//     |                                             v
//   MCP server + AddTelemetryTools  -----------> queries Prometheus
//     ^
//     |  streamable HTTP + MCP client (in-process)
//   AI agent journey
//
// The Prometheus container scrapes this process's /metrics endpoint and this
// process's telemetry tools query that same Prometheus over its HTTP API. So a
// telemetry answer is a genuine round-trip: silo -> Prometheus scrape -> PromQL
// query -> MCP tool result.
//
// The sample proves the headline properties of the telemetry surface:
//
//   1. A caller granted the cluster-wide LatticeOperation.Telemetry capability
//      discovers the four read-only lattice_telemetry_* tools and runs a live
//      PromQL query end-to-end over MCP against real Prometheus.
//   2. Permission-scoping: the same caller does NOT see the state tools it was
//      not granted, and an unauthenticated caller is offered nothing at all.
//   3. The dual-credential boundary: the tools authenticate to Prometheus with a
//      backend credential the host configures, never the caller's identity.
//
// Prerequisite: start Prometheus first with `docker compose up -d` in this
// directory. See README.md.
// ---------------------------------------------------------------------------

const string DemoTree = "catalog";
const string Agent = DemoCredentialBridge.AgentSubject;
const string Scheme = DemoAuthenticator.Scheme;
const int Port = 5290;
const string PrometheusAddress = "http://localhost:9090/";

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();

// Bind on all interfaces so the Prometheus container can scrape /metrics via
// host.docker.internal, and the in-process MCP client can reach it via
// localhost.
builder.WebHost.UseUrls($"http://0.0.0.0:{Port}");

builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.AddMemoryGrainStorageAsDefault();
    silo.UseInMemoryReminderService();
    silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

    // Membership resolves the ambient caller credential into a subject.
    silo.AddLatticeMembership();

    // Auth installs the default-deny enforcement gate. "root-admin" is a
    // bootstrap administrator so the sample can seed the user + rule before any
    // rule exists.
    silo.AddLatticeAuth(options =>
    {
        options.DefaultEffect = LatticeEffect.Deny;
        options.BootstrapAdministrators.Add("root-admin");
    });

    silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DemoAuthenticator>();

    // The facades whose tools we register (state is added purely to demonstrate
    // that a Telemetry-only grant does not unlock them). The auth facade is
    // registered so the MCP discovery core can resolve the caller's effective
    // permissions - it is not exposed as a tool group here.
    silo.AddLatticeStateApi();
    silo.AddLatticeDataApi();
    silo.AddLatticeAuthApi();
});

// Export the orleans.lattice meter over Prometheus at /metrics so the Docker
// Prometheus service can scrape this silo.
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(LatticeMetrics.MeterName)
        .AddPrometheusExporter());

// The demo credential bridge is registered BEFORE AddLatticeMcp so its
// TryAdd-registered HttpContext bridge is skipped and ours wins.
builder.Services.AddSingleton<ILatticeApiMcpCredentialBridge, DemoCredentialBridge>();

// The MCP server front door, mounted at /mcp so it coexists with /metrics.
// RequireAuthorization is disabled purely to keep the sample one-command
// runnable; discovery is still fail-closed and permission-scoped underneath.
builder.Services.AddLatticeMcp(options =>
{
    options.RequireAuthorization = false;
    options.TransportPattern = "/mcp";
});

// Register the state tools (never granted to the agent below) and the telemetry
// tools, pointing the telemetry proxy at the real Prometheus. AuthMode is None
// because the sample's Prometheus is unauthenticated; a real backend uses
// Bearer / Basic / MutualTls with a configured backend credential.
builder.Services.AddStateTools();
builder.Services.AddTelemetryTools(o =>
{
    o.BackendAddress = new Uri(PrometheusAddress);
    o.AuthMode = LatticeTelemetryBackendAuthMode.None;
});

var app = builder.Build();
app.MapPrometheusScrapingEndpoint();
app.MapLatticeMcp();
await app.StartAsync();

Console.WriteLine($"Silo + MCP server started; /metrics on http://localhost:{Port}/metrics\n");

var store = app.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
var grainFactory = app.Services.GetRequiredService<IGrainFactory>();
var tree = grainFactory.GetGrain<ILattice>(DemoTree);

// -- Seed the agent, its Telemetry-only grant, and drive some load -----------
// Seeding writes the reserved policy tree, which requires Admin, so it runs as
// the bootstrap administrator (which bypasses the gate). The writes also drive
// the orleans.lattice meter so Prometheus has cluster metrics to serve.
Console.WriteLine("Seeding an 'agent' subject with a cluster-wide Telemetry grant, and driving load...");
using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    // A cluster-wide Telemetry grant: an Allow rule over the all-trees sentinel
    // scope, conferring only LatticeOperation.Telemetry.
    await store.PutRuleAsync(new LatticeAuthorizationRule(
        "agent-telemetry",
        LatticeSubjectSelector.User(Agent),
        LatticeScope.ClusterWide(),
        LatticeOperation.Telemetry,
        LatticeEffect.Allow));

    for (var i = 0; i < 50; i++)
    {
        await tree.SetAsync($"item/{i:D3}", Encoding.UTF8.GetBytes($"value-{i}"));
    }
    for (var i = 0; i < 50; i++)
    {
        _ = await tree.GetAsync($"item/{i:D3}");
    }
}

// -- Act 1: the authenticated agent discovers and uses the telemetry tools ---
Console.WriteLine("\n== Act 1: a Telemetry-granted agent queries live metrics over MCP ==");
await using var agentClient = await ConnectAsync(withAgentHeader: true);

// The compiled policy snapshot rebuilds off the policy-tree change feed, so poll
// the advertised tool list until the Telemetry grant surfaces the telemetry
// tools (the meta-tool alone is always present for an authenticated caller).
var agentTools = await WaitForTelemetryToolsAsync(agentClient, TimeSpan.FromSeconds(20));
var toolNames = agentTools.Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
var telemetryTools = toolNames.Where(n => n.StartsWith("lattice_telemetry_", StringComparison.Ordinal)).ToArray();
var stateTools = toolNames.Where(n => n.StartsWith("lattice_state_", StringComparison.Ordinal)).ToArray();

Console.WriteLine($"  agent sees {telemetryTools.Length} telemetry tool(s):");
foreach (var name in telemetryTools)
{
    Console.WriteLine($"    - {name}");
}
Console.WriteLine($"  agent sees {stateTools.Length} state tool(s) (it was NOT granted state access).");

// Wait for Prometheus to scrape this silo at least once (up == 1), which also
// confirms the Docker Prometheus is running and reachable.
Console.WriteLine($"\n  Waiting for Prometheus ({PrometheusAddress}) to scrape this silo...");
var scraped = await WaitForScrapeAsync(agentClient, TimeSpan.FromSeconds(45));
if (!scraped)
{
    Console.WriteLine("  [!] Prometheus did not report this silo as up.");
    Console.WriteLine("      Start it first with:  docker compose up -d");
    await app.StopAsync();
    return 2;
}

Console.WriteLine("  Prometheus is scraping this silo. Running live telemetry queries:\n");

// A guaranteed-present instant query: the scrape-health of this silo.
var up = await agentClient.CallToolAsync(
    "lattice_telemetry_query",
    new Dictionary<string, object?> { ["query"] = "up{job=\"lattice-silo\"}" });
Console.WriteLine($"  lattice_telemetry_query(up{{job=\"lattice-silo\"}}) -> {Structured(up)}");

// List the metric names Prometheus exposes and show the cluster's own metrics.
var metricsResult = await agentClient.CallToolAsync(
    "lattice_telemetry_list_metrics",
    new Dictionary<string, object?>());
var latticeMetrics = ExtractLatticeMetricNames(metricsResult).ToArray();
Console.WriteLine($"  lattice_telemetry_list_metrics -> {latticeMetrics.Length} orleans.lattice metric(s) discovered, e.g.:");
foreach (var name in latticeMetrics.Take(5))
{
    Console.WriteLine($"    - {name}");
}

// If we discovered a real lattice metric, query it too.
if (latticeMetrics.Length > 0)
{
    var metric = latticeMetrics[0];
    var series = await agentClient.CallToolAsync(
        "lattice_telemetry_query",
        new Dictionary<string, object?> { ["query"] = metric });
    Console.WriteLine($"  lattice_telemetry_query({metric}) -> {Structured(series)}");
}

// -- Act 2: the anonymous caller (fail-closed) -------------------------------
Console.WriteLine("\n== Act 2: an unauthenticated caller is offered nothing ==");
await using var anonClient = await ConnectAsync(withAgentHeader: false);
var anonTools = await anonClient.ListToolsAsync();
Console.WriteLine($"  anonymous caller sees {anonTools.Count} tools (fail-closed).");

var ok = telemetryTools.Length == 4 && stateTools.Length == 0 && anonTools.Count == 0 && scraped;
Console.WriteLine();
Console.WriteLine(ok
    ? "[OK] the Telemetry grant unlocked exactly the telemetry tools; the query hit real Prometheus; the anonymous caller got none."
    : "[FAIL] the sample did not reach the expected end state.");

await app.StopAsync();
return ok ? 0 : 1;

// --- helpers ---------------------------------------------------------------

// Connects a real MCP client over streamable HTTP to the /mcp endpoint. When
// withAgentHeader is set, the request carries the marker header the demo bridge
// maps to the agent.
async Task<McpClient> ConnectAsync(bool withAgentHeader)
{
    var options = new HttpClientTransportOptions
    {
        Endpoint = new Uri($"http://localhost:{Port}/mcp"),
        Name = withAgentHeader ? "agent" : "anonymous",
    };

    if (withAgentHeader)
    {
        options.AdditionalHeaders = new Dictionary<string, string>
        {
            [DemoCredentialBridge.AgentHeader] = "true",
        };
    }

    return await McpClient.CreateAsync(new HttpClientTransport(options));
}

// Polls the advertised tool list until a lattice_telemetry_* tool appears (the
// Telemetry grant has compiled into the policy snapshot) or the budget elapses.
async Task<IList<McpClientTool>> WaitForTelemetryToolsAsync(McpClient client, TimeSpan budget)
{
    var deadline = DateTime.UtcNow + budget;
    while (DateTime.UtcNow < deadline)
    {
        var tools = await client.ListToolsAsync();
        if (tools.Any(t => t.Name.StartsWith("lattice_telemetry_", StringComparison.Ordinal)))
        {
            return tools;
        }

        await Task.Delay(TimeSpan.FromMilliseconds(500));
    }

    return await client.ListToolsAsync();
}

// Polls lattice_telemetry_query for this silo's scrape-health until Prometheus
// reports it up (a non-empty vector), or the budget elapses.
async Task<bool> WaitForScrapeAsync(McpClient client, TimeSpan budget)
{
    var deadline = DateTime.UtcNow + budget;
    while (DateTime.UtcNow < deadline)
    {
        var result = await client.CallToolAsync(
            "lattice_telemetry_query",
            new Dictionary<string, object?> { ["query"] = "up{job=\"lattice-silo\"}" });

        if (TryReadStructured(result, out var root)
            && root.TryGetProperty("success", out var success)
            && success.GetBoolean()
            && root.TryGetProperty("series", out var series)
            && series.ValueKind == JsonValueKind.Array
            && series.GetArrayLength() > 0)
        {
            return true;
        }

        await Task.Delay(TimeSpan.FromSeconds(1));
    }

    return false;
}

// Renders a tool result's structured content compactly for the console.
string Structured(CallToolResult result) =>
    TryReadStructured(result, out var root) ? root.GetRawText() : "(no structured content)";

// Pulls the orleans.lattice metric names out of a list_metrics result.
IEnumerable<string> ExtractLatticeMetricNames(CallToolResult result)
{
    if (!TryReadStructured(result, out var root)
        || !root.TryGetProperty("metrics", out var metrics)
        || metrics.ValueKind != JsonValueKind.Array)
    {
        yield break;
    }

    foreach (var element in metrics.EnumerateArray())
    {
        if (element.ValueKind == JsonValueKind.String
            && element.GetString() is { } name
            && name.Contains("lattice", StringComparison.OrdinalIgnoreCase))
        {
            yield return name;
        }
    }
}

// Parses the tool result's structured content into a JsonElement.
static bool TryReadStructured(CallToolResult result, out JsonElement root)
{
    if (result.StructuredContent is { } element)
    {
        root = element.Clone();
        return true;
    }

    root = default;
    return false;
}
