using System.Text;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using Orleans.Lattice;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Mcp;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Samples.McpServer;

// ---------------------------------------------------------------------------
// Orleans.Lattice.Api.Mcp sample: a co-hosted single silo that exposes the
// Model Context Protocol (MCP) server over streamable HTTP, then drives it with
// a real MCP client end-to-end.
//
// One WebApplication process runs the whole stack: an Orleans silo with the core
// tree, Membership (identity), Auth (a default-deny enforcement gate), and the
// three transport-agnostic API facades (state, data, auth). On top of those the
// MCP server advertises the facades as MCP tools, scoped per caller by the
// caller's authorization grants.
//
// The sample proves the two headline properties of the MCP surface:
//
//   1. Permission-scoped discovery. An authenticated agent that has been granted
//      access sees the state / data / auth tool set and can call a tool
//      end-to-end over MCP.
//   2. Fail-closed by default. A caller the credential bridge cannot authenticate
//      is offered NOTHING - not even the lattice_capabilities meta-tool.
//
// Authorization on the endpoint is disabled purely to keep the sample
// one-command runnable with no identity provider; a demo credential bridge maps a
// request carrying a marker header onto a fixed "agent" credential, and a demo
// authenticator resolves that credential to the "agent" subject inside the
// cluster. A real deployment leaves RequireAuthorization at its secure default
// and lifts an authenticated ASP.NET Core principal onto the credential.
// ---------------------------------------------------------------------------

const string DemoTree = "catalog";
const string Agent = DemoCredentialBridge.AgentSubject;
const string Scheme = DemoAuthenticator.Scheme;
const int Port = 5290;

// Every data-plane + admin capability the four tool groups require, in one mask,
// so a single Allow rule unlocks the whole granted tool set for the agent.
const LatticeOperation AllOperations =
    LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete |
    LatticeOperation.RangeRead | LatticeOperation.RangeDelete | LatticeOperation.CrdtApply |
    LatticeOperation.AtomicWrite | LatticeOperation.BulkLoad | LatticeOperation.Admin |
    LatticeOperation.Backup | LatticeOperation.Restore;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.WebHost.UseUrls($"http://localhost:{Port}");

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

    // The trusted-token authenticator that maps a credential's token to the
    // caller subject id (a real deployment uses JWT / Entra).
    silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DemoAuthenticator>();

    // The three transport-agnostic facades the MCP tools adapt.
    silo.AddLatticeStateApi();
    silo.AddLatticeDataApi();
    silo.AddLatticeAuthApi();
});

// The demo credential bridge is registered BEFORE AddLatticeMcp so its
// TryAdd-registered HttpContext bridge is skipped and ours wins.
builder.Services.AddSingleton<ILatticeApiMcpCredentialBridge, DemoCredentialBridge>();

// The MCP server front door. RequireAuthorization is disabled purely to keep the
// sample one-command runnable; discovery is still fail-closed and
// permission-scoped underneath.
builder.Services.AddLatticeMcp(options => options.RequireAuthorization = false);

// Opt in to the three tool modules, with writes and auth administration enabled
// so the agent's full granted surface is exercised.
builder.Services.AddStateTools();
builder.Services.AddDataTools(enableWrites: true);
builder.Services.AddAuthTools(enableAdministration: true);

var app = builder.Build();
app.MapLatticeMcp();
await app.StartAsync();

Console.WriteLine($"Silo + MCP server started on http://localhost:{Port}\n");

var directory = app.Services.GetRequiredService<ILatticeMembershipDirectory>();
var store = app.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
var grainFactory = app.Services.GetRequiredService<IGrainFactory>();
var tree = grainFactory.GetGrain<ILattice>(DemoTree);

// -- Seed the agent, its grant, and some data ------------------------------
// Seeding writes the reserved membership / policy trees, which require Admin, so
// it runs as the bootstrap administrator (which bypasses the gate).
Console.WriteLine("Seeding an 'agent' subject with a full-access grant on the demo tree...");
using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    await directory.UpsertUserAsync(new MembershipUser(Agent, "Automation agent"));

    await store.PutRuleAsync(new LatticeAuthorizationRule(
        "agent-all",
        LatticeSubjectSelector.User(Agent),
        LatticeScope.Tree(DemoTree),
        AllOperations,
        LatticeEffect.Allow));

    for (var i = 0; i < 5; i++)
    {
        await tree.SetAsync($"item/{i:D3}", Encoding.UTF8.GetBytes($"value-{i}"));
    }
}

// -- Act 1: the authenticated, granted agent --------------------------------
Console.WriteLine("\n== Act 1: an authenticated, granted agent discovers and calls tools ==");
await using var agentClient = await ConnectAsync(withAgentHeader: true);

// The compiled policy snapshot rebuilds off the policy-tree change feed, so poll
// the advertised tool list until the grant is reflected.
var agentTools = await WaitForToolsAsync(agentClient, TimeSpan.FromSeconds(15));
var toolNames = agentTools.Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();

Console.WriteLine($"  agent sees {toolNames.Length} tools, including:");
foreach (var name in toolNames.Where(n => n is "lattice_capabilities"
             or "lattice_state_list_trees" or "lattice_data_get" or "lattice_auth_explain"))
{
    Console.WriteLine($"    - {name}");
}

var getEntry = await agentClient.CallToolAsync(
    "lattice_data_get",
    new Dictionary<string, object?> { ["treeId"] = DemoTree, ["key"] = "item/000" });
Console.WriteLine($"  called lattice_data_get(item/000) -> isError={getEntry.IsError == true}, "
    + $"structured={getEntry.StructuredContent?.ToString()}");

// -- Act 2: the anonymous caller (fail-closed) ------------------------------
Console.WriteLine("\n== Act 2: an unauthenticated caller is offered nothing ==");
await using var anonClient = await ConnectAsync(withAgentHeader: false);
var anonTools = await anonClient.ListToolsAsync();
Console.WriteLine($"  anonymous caller sees {anonTools.Count} tools (fail-closed).");

var ok = toolNames.Contains("lattice_state_list_trees") && anonTools.Count == 0;
Console.WriteLine();
Console.WriteLine(ok
    ? "[OK] permission-scoped discovery granted the agent its tools; the anonymous caller got none."
    : "[FAIL] the sample did not reach the expected end state.");

await app.StopAsync();
return ok ? 0 : 1;

// --- helpers ---------------------------------------------------------------

// Connects a real MCP client over streamable HTTP. When withAgentHeader is set,
// the request carries the marker header the demo bridge maps to the agent.
async Task<McpClient> ConnectAsync(bool withAgentHeader)
{
    var options = new HttpClientTransportOptions
    {
        Endpoint = new Uri($"http://localhost:{Port}"),
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

// Polls the advertised tool list until it is non-empty (the grant has been
// compiled into the policy snapshot) or the budget elapses.
async Task<IList<McpClientTool>> WaitForToolsAsync(McpClient client, TimeSpan budget)
{
    var deadline = DateTime.UtcNow + budget;
    while (DateTime.UtcNow < deadline)
    {
        var tools = await client.ListToolsAsync();
        if (tools.Count > 0)
        {
            return tools;
        }

        await Task.Delay(TimeSpan.FromMilliseconds(500));
    }

    return await client.ListToolsAsync();
}
