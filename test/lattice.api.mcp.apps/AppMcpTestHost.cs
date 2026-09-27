using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// A deterministic, in-memory composition of the discovery core and the app tool surface:
/// a settable registry projection, an in-memory app source, a granting access gate, a
/// credential-echo membership context and a header-driven tenant, all behind the real
/// <see cref="LatticeApiMcpSessionConfigurator"/> and the real <c>AddAppMcpTools</c> wiring.
/// </summary>
internal sealed class AppMcpTestHost
{
    public AppMcpTestHost(bool registerApps = true, ILatticeApiMcpAuthorizer? authorizer = null)
    {
        Bridge = new FakeCredentialBridge(new LatticeCredential("token", principalId: "alice"));
        var services = new ServiceCollection()
            .AddLogging()
            .AddHttpContextAccessor()
            .AddSingleton(authorizer ?? new AllowAllMcpAuthorizer())
            .AddSingleton<ILatticeApiMcpCredentialBridge>(Bridge)
            .AddSingleton<ILatticeApiMcpActiveTenantBridge, HeaderTenantBridge>()
            .AddSingleton<IAppRegistryProjection>(Projection)
            .AddSingleton<IAppSource>(Source)
            .AddSingleton<ILatticeAccessGate>(Gate)
            .AddSingleton<ILatticeMembershipContext, CredentialEchoMembershipContext>()
            .AddSingleton<ITenantContextResolver, AmbientTenantResolver>();
        services.AddSingleton<IEnumerable<IAppMcpToolProvider>>(_ => Providers);
        if (registerApps)
            services.AddAppMcpTools();
        Services = services.BuildServiceProvider();
    }

    public FakeCredentialBridge Bridge { get; }

    public FakeAppRegistryProjection Projection { get; } = new(CompiledAppRegistrySnapshot.Empty);

    public FakeAppSource Source { get; } = new();

    public GrantingAccessGate Gate { get; } = new();

    public List<IAppMcpToolProvider> Providers { get; } = new();

    public ServiceProvider Services { get; }

    public AppMcpToolSource ToolSource => Services.GetRequiredService<AppMcpToolSource>();

    public AppMcpTestHost Provide(AppSlug slug, params McpServerTool[] tools)
    {
        Providers.Add(new AppMcpToolProvider(slug, tools));
        return this;
    }

    public AppMcpTestHost Publish(long epoch, params AppRegistryRecord[] records)
    {
        Projection.Current = AppMcpTestData.Snapshot(epoch, records);
        return this;
    }

    public LatticeApiMcpSessionConfigurator Configurator(
        LatticeApiMcpAccessSet access,
        params ILatticeApiMcpToolGroup[] groups)
        => new(
            Bridge,
            new FixedPermissionResolver(access),
            groups,
            Services,
            NullLogger<LatticeApiMcpSessionConfigurator>.Instance,
            appToolSources: Services.GetServices<ILatticeApiMcpAppToolSource>());

    public DefaultHttpContext Context(TenantId? tenant = null)
    {
        var context = new DefaultHttpContext { RequestServices = Services };
        if (tenant is { } t)
            context.Request.Headers[HeaderTenantBridge.HeaderName] = t.Value;
        return context;
    }

    public async Task<IReadOnlyList<string>> AdvertisedAsync(TenantId? tenant = null, params ILatticeApiMcpToolGroup[] groups)
    {
        var plan = await Configurator(LatticeApiMcpAccessSet.None, groups).BuildSessionPlanAsync(Context(tenant), CancellationToken.None);
        return plan.Tools.Select(t => t.ProtocolTool.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
    }

    /// <summary>Builds a session for <paramref name="tenant"/> and invokes <paramref name="toolName"/> from its collection.</summary>
    public async Task<McpServerTool?> SessionToolAsync(string toolName, TenantId? tenant = null)
    {
        var plan = await Configurator(LatticeApiMcpAccessSet.None).BuildSessionPlanAsync(Context(tenant), CancellationToken.None);
        return plan.Tools.TryGetPrimitive(toolName, out var tool) ? tool : null;
    }

    /// <summary>Invokes <paramref name="tool"/> as the ambient HTTP request of <paramref name="tenant"/>.</summary>
    public Task<ModelContextProtocol.Protocol.CallToolResult> InvokeAsync(McpServerTool tool, TenantId? tenant = null)
    {
        Services.GetRequiredService<IHttpContextAccessor>().HttpContext = Context(tenant);
        return McpToolInvocation.CallAsync(tool, Services);
    }
}
