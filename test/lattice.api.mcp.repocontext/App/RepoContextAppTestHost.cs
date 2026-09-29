using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// A deterministic, in-memory composition of the MCP discovery core around a caller-supplied
/// repository-context registration: a fixed credential for <see cref="Principal"/>, a fixed
/// group access set, a settable app registry projection, the real in-image app source, and a
/// granting access gate, all behind the real <see cref="LatticeApiMcpSessionConfigurator"/>
/// fed exactly the tool groups and app tool sources the container resolves.
/// </summary>
internal sealed class RepoContextAppTestHost
{
    /// <summary>The principal every session resolves to.</summary>
    public const string Principal = "alice";

    private static readonly LatticeApiMcpAccessSet Access =
        LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.RepoContext);

    private readonly RepoContextMcpStubCredentialBridge _bridge =
        new(new LatticeCredential("token", principalId: Principal));

    public RepoContextAppTestHost(Action<IServiceCollection> register)
    {
        ArgumentNullException.ThrowIfNull(register);
        var services = new ServiceCollection()
            .AddLogging()
            .AddHttpContextAccessor()
            .AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer())
            .AddSingleton<ILatticeApiMcpCredentialBridge>(_bridge)
            .AddSingleton<IAppRegistryProjection>(Projection)
            .AddSingleton<IAppSource, InImageAppSource>()
            .AddSingleton<ILatticeAccessGate>(Gate)
            .AddSingleton<ILatticeMembershipContext>(Membership)
            .AddSingleton<ITenantContextResolver, RepoContextAppTenantResolver>();
        register(services);
        Services = services.BuildServiceProvider();
    }

    public RepoContextAppRegistryProjection Projection { get; } = new();

    public RepoContextAppGrantingGate Gate { get; } = new();

    /// <summary>The membership context every session resolves through.</summary>
    public RepoContextAppMembershipContext Membership { get; } = new();

    /// <summary>The group <see cref="Record"/> binds the manifest's <c>reader</c> role to.</summary>
    public const string ReadersGroup = "g-repo-readers";

    public ServiceProvider Services { get; }

    /// <summary>A registry record for the repository-context app at the manifest's version.</summary>
    public static AppRegistryRecord Record(AppRegistryLifecycleState state = AppRegistryLifecycleState.Enabled)
    {
        var version = RepoContextAppManifest.Load().Manifest!.Identity.Version;
        return new AppRegistryRecord
        {
            Isolation = new AppIsolationContext { Tenant = TenantId.Default, ClusterId = "test-cluster" },
            Slug = AppSlug.Parse(RepoContextAppManifest.Slug),
            Version = version,
            Provenance = new AppProvenance(),
            Ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read | LatticeOperation.RangeRead),
            CeilingVersion = version,
            RoleBindings = [AppRoleBinding.Create("reader", ReadersGroup)],
            State = state,
            Revision = 1,
        };
    }

    public async Task<LatticeApiMcpSessionPlan> PlanAsync()
    {
        var configurator = new LatticeApiMcpSessionConfigurator(
            _bridge,
            new RepoContextMcpStubPermissionResolver(Access),
            Services.GetServices<ILatticeApiMcpToolGroup>(),
            Services,
            NullLogger<LatticeApiMcpSessionConfigurator>.Instance,
            appToolSources: Services.GetServices<ILatticeApiMcpAppToolSource>());
        return await configurator.BuildSessionPlanAsync(Context(), CancellationToken.None);
    }

    public async Task<IReadOnlyList<string>> AdvertisedAsync()
    {
        var plan = await PlanAsync();
        return plan.Tools.Select(t => t.ProtocolTool.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
    }

    /// <summary>
    /// Snapshots the wire-visible surface: the <c>lattice_capabilities</c> payload (structured
    /// and text), every advertised tool's serialized protocol definition, and the instructions.
    /// </summary>
    public async Task<(string Capabilities, string Tools, string Instructions)> SnapshotAsync()
    {
        var plan = await PlanAsync();
        var capabilitiesTool = plan.Tools.Single(t => t.ProtocolTool.Name == "lattice_capabilities");
        var result = await InvokeAsync(capabilitiesTool);
        var capabilities = JsonSerializer.Serialize(result.StructuredContent) + "|"
            + string.Concat(result.Content.OfType<TextContentBlock>().Select(b => b.Text));
        var tools = string.Join(
            "\n",
            plan.Tools.Select(t => JsonSerializer.Serialize(t.ProtocolTool)).OrderBy(s => s, StringComparer.Ordinal));
        return (capabilities, tools, plan.Instructions);
    }

    /// <summary>Resolves <paramref name="toolName"/> from a fresh session's tool collection.</summary>
    public async Task<McpServerTool?> SessionToolAsync(string toolName)
    {
        var plan = await PlanAsync();
        return plan.Tools.TryGetPrimitive(toolName, out var tool) ? tool : null;
    }

    /// <summary>
    /// Invokes <paramref name="tool"/> with no transport: the SDK's request context needs a
    /// server instance, so one is created over an in-memory stream pair and never started.
    /// </summary>
    public async Task<CallToolResult> InvokeAsync(McpServerTool tool)
    {
        Services.GetRequiredService<IHttpContextAccessor>().HttpContext = Context();
        using var input = new MemoryStream();
        using var output = new MemoryStream();
        await using var transport = new StreamServerTransport(input, output);
        await using var server = McpServer.Create(transport, new McpServerOptions(), NullLoggerFactory.Instance, Services);
        var request = new RequestContext<CallToolRequestParams>(
            server,
            new JsonRpcRequest { Method = RequestMethods.ToolsCall },
            new CallToolRequestParams { Name = tool.ProtocolTool.Name })
        {
            Services = Services,
        };

        return await tool.InvokeAsync(request, CancellationToken.None);
    }

    private DefaultHttpContext Context() => new() { RequestServices = Services };
}
