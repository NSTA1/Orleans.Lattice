using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// A deterministic, in-memory composition of the discovery core and the app tool surface:
/// a settable registry projection, an in-memory app source, a granting access gate (the caller's
/// own rights, which must never confer an app role), a credential-echo membership context and a
/// header-driven tenant, all behind the real
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
            .AddSingleton<ILatticeMembershipContext>(Membership)
            .AddSingleton<ITenantContextResolver, AmbientTenantResolver>();
        services.AddSingleton<IEnumerable<IAppMcpToolProvider>>(_ => Providers);
        if (registerApps)
            services.AddAppMcpTools();
        Services = services.BuildServiceProvider();
    }

    public FakeCredentialBridge Bridge { get; }

    public FakeAppRegistryProjection Projection { get; } = new(CompiledAppRegistrySnapshot.Empty);

    public FakeAppSource Source { get; } = new();

    /// <summary>
    /// The access gate: the whole policy, including the install's compiled app rules. It allows by default
    /// (the app rules are live and nothing denies), so a fixture removes a role with an explicit deny.
    /// </summary>
    public GrantingAccessGate Gate { get; } = new() { AllowByDefault = true };

    /// <summary>The membership the host resolves callers through; join a caller to a bound group to give it a role.</summary>
    public CredentialEchoMembershipContext Membership { get; } = new();

    /// <summary>Joins <paramref name="subject"/> to the group the test records bind to <paramref name="role"/>.</summary>
    public AppMcpTestHost Bind(string subject, string role = "reader")
    {
        Membership.Join(subject, AppMcpTestData.GroupFor(role));
        return this;
    }

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
