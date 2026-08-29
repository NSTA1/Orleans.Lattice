using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Tenants;
using Orleans.Lattice.Explorer.Tenants.Views;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// Renders the Tenants plugin's panel to static HTML so a test can assert on the
/// markup an operator actually sees.
/// </summary>
/// <remarks>
/// <para>
/// Uses the framework's own <see cref="HtmlRenderer"/>, the same mechanism the
/// design system's and the Backups plugin's component tests use, so this plugin
/// needs no extra component-testing dependency. Rendering is driven entirely by
/// the fake domain and the access decision supplied here, so a test never waits
/// on a clock, a timer, a network call, or a background task.
/// </para>
/// <para>
/// The panel reaches the host through exactly two services - the per-plugin
/// host-context factory and the keyed access store - so the harness registers
/// exactly those two. That is the controlled domain-model seam under test as
/// much as it is plumbing.
/// </para>
/// </remarks>
internal static class TenantsRenderHarness
{
    /// <summary>
    /// Renders the Tenants panel beneath a cascaded shell context.
    /// </summary>
    /// <param name="domain">The controlled domain model the panel resolves.</param>
    /// <param name="access">The gate decision filed for the plugin.</param>
    /// <param name="breakpoint">The breakpoint to cascade, or <see langword="null"/> for none.</param>
    /// <returns>The rendered markup.</returns>
    public static async Task<string> RenderPanelAsync(
        ITenancyDomain domain,
        ExplorerPluginAccess? access = null,
        LatticeBreakpoint? breakpoint = LatticeBreakpoint.Expanded)
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(TenantsPluginKeys.PluginId, access ?? ExplorerPluginAccess.Allowed);

        var context = Substitute.For<IExplorerPluginHostContext>();
        context.PluginId.Returns(TenantsPluginKeys.PluginId);
        context.GetDomain<ITenancyDomain>().Returns(domain);

        var factory = Substitute.For<IExplorerPluginHostContextFactory>();
        factory.Create(TenantsPluginKeys.PluginId).Returns(context);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(factory);
        services.AddSingleton<IExplorerPluginAccessStore>(store);

        await using var provider = services.BuildServiceProvider();
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        return await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var parameters = breakpoint is null
                ? ParameterView.Empty
                : ParameterView.FromDictionary(new Dictionary<string, object?>
                {
                    ["Value"] = new LatticeAdaptiveContext(
                        breakpoint.Value,
                        LatticeDensity.Cosy,
                        IsMeasured: true),
                    ["ChildContent"] = (RenderFragment)(builder =>
                    {
                        builder.OpenComponent<TenantsPanel>(0);
                        builder.CloseComponent();
                    }),
                });

            var component = breakpoint is null
                ? await renderer.RenderComponentAsync<TenantsPanel>(parameters)
                : await renderer.RenderComponentAsync<CascadingValue<LatticeAdaptiveContext>>(parameters);

            return component.ToHtmlString();
        });
    }

    /// <summary>
    /// A fake domain already carrying one tenant, its detail, its usage reading,
    /// its regions, its admin subjects, and its grants.
    /// </summary>
    /// <returns>The seeded domain.</returns>
    public static FakeTenancyDomain SeededDomain()
    {
        var domain = new FakeTenancyDomain();
        var service = domain.Service;
        service.Tenants.Add(SampleTenants.Summary());
        service.Details[SampleTenants.Acme] = SampleTenants.Detail();
        service.Usage[SampleTenants.Acme] = SampleTenants.Usage();
        service.Regions[SampleTenants.Acme] =
            [SampleTenants.OnlineRegion(), SampleTenants.AllowedButEmptyRegion()];
        service.AdminSubjects[SampleTenants.Acme] = [SampleTenants.Subject];
        service.Grants[SampleTenants.Acme] = new ExplorerTenantGrants
        {
            TenantId = SampleTenants.Acme,
            Issued = [SampleTenants.Grant(ExplorerTenantGrantState.Pending)],
            Received =
            [
                SampleTenants.Grant(
                    ExplorerTenantGrantState.Active,
                    ExplorerTenantGrantAccess.ReadWrite,
                    SampleTenants.Globex,
                    SampleTenants.Acme,
                    "grant-2"),
            ],
        };

        return domain;
    }

    /// <summary>
    /// Counts non-overlapping occurrences of <paramref name="needle"/> in
    /// <paramref name="haystack"/> using an ordinal comparison.
    /// </summary>
    /// <param name="haystack">The text to search.</param>
    /// <param name="needle">The literal to count. Must not be empty.</param>
    /// <returns>The number of occurrences.</returns>
    public static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }
}
