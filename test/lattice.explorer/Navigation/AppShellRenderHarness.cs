using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// Renders <see cref="AppShell"/> itself - the shell frame, not a plugin inside
/// it - to static HTML beneath a cascaded shell context, so a test can assert on
/// the markup a breakpoint produces.
/// </summary>
/// <remarks>
/// <para>
/// This is the harness issue #1792 exists for. Every reflow assertion the epic
/// already had rendered a <em>plugin</em>, inside a pane whose width the shell
/// never changed, so the frame around them could ignore the breakpoint entirely
/// and every test stayed green. Rendering the shell is what observes the frame.
/// </para>
/// <para>
/// Uses the framework's own <see cref="HtmlRenderer"/>, the same mechanism the
/// design system's, the Backups plugin's, and the selection views' component
/// tests use, so no new component-testing dependency is taken. The shell's five
/// services are supplied here from controlled stubs and every gate answers
/// synchronously, so a render never waits on a clock, a timer, a network call,
/// or a background task.
/// </para>
/// </remarks>
internal static class AppShellRenderHarness
{
    /// <summary>The catalog fragment's marker, standing in for the tree browser.</summary>
    public const string CatalogMarker = "catalog-surface";

    /// <summary>The detail fragment's marker, standing in for the routed body.</summary>
    public const string DetailMarker = "detail-surface";

    /// <summary>
    /// The class the catalog carries in the product. The shell decides where an
    /// element with this class goes; it never renames it.
    /// </summary>
    public const string CatalogPaneClass = "lx-shell-nav";

    /// <summary>
    /// Renders the shell at <paramref name="breakpoint"/> over
    /// <paramref name="plugins"/>.
    /// </summary>
    /// <param name="breakpoint">The breakpoint to cascade.</param>
    /// <param name="isCatalogOpen">Whether the compact catalog drawer starts open.</param>
    /// <param name="isOverflowOpen">Whether the area strip's overflow menu starts open.</param>
    /// <param name="plugins">The area plugins the container yields.</param>
    /// <returns>The rendered markup.</returns>
    public static async Task<string> RenderAsync(
        LatticeBreakpoint breakpoint = LatticeBreakpoint.Expanded,
        bool isCatalogOpen = false,
        bool isOverflowOpen = false,
        params IExplorerPlugin[] plugins)
    {
        await using var provider = BuildServices(plugins);
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        return await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var parameters = ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                ["Value"] = new LatticeAdaptiveContext(
                    breakpoint,
                    LatticeDensity.Cosy,
                    IsMeasured: true),
                ["ChildContent"] = (RenderFragment)(builder =>
                {
                    builder.OpenComponent<AppShell>(0);
                    builder.AddComponentParameter(1, nameof(AppShell.Catalog), CatalogFragment());
                    builder.AddComponentParameter(2, nameof(AppShell.ChildContent), DetailFragment());
                    builder.AddComponentParameter(3, nameof(AppShell.IsCatalogOpen), isCatalogOpen);
                    builder.AddComponentParameter(4, nameof(AppShell.IsOverflowOpen), isOverflowOpen);
                    builder.CloseComponent();
                }),
            });

            var component = await renderer
                .RenderComponentAsync<CascadingValue<LatticeAdaptiveContext>>(parameters);

            return component.ToHtmlString();
        });
    }

    /// <summary>
    /// Renders the shell with no cascaded shell context at all, which is what a
    /// static render or a head without script produces.
    /// </summary>
    /// <param name="plugins">The area plugins the container yields.</param>
    /// <returns>The rendered markup.</returns>
    public static async Task<string> RenderWithoutContextAsync(params IExplorerPlugin[] plugins)
    {
        await using var provider = BuildServices(plugins);
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        return await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var parameters = ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                [nameof(AppShell.Catalog)] = CatalogFragment(),
                [nameof(AppShell.ChildContent)] = DetailFragment(),
            });

            var component = await renderer.RenderComponentAsync<AppShell>(parameters);
            return component.ToHtmlString();
        });
    }

    /// <summary>An always-allowed area plugin, so a strip of any length can be built.</summary>
    /// <param name="id">The plugin id.</param>
    /// <param name="label">The tab label.</param>
    /// <param name="order">The descriptor's ordering hint.</param>
    public static IExplorerPlugin Plugin(string id, string label, int order) =>
        new FakeExplorerPlugin(
            id,
            ExplorerPluginSurface.Area,
            order,
            label,
            ExplorerPluginAccessGates.Allowed,
            domainContract: null,
            typeof(StubAreaView));

    /// <summary>
    /// Counts non-overlapping occurrences of <paramref name="needle"/> in
    /// <paramref name="haystack"/> using an ordinal comparison.
    /// </summary>
    /// <param name="haystack">The text to search.</param>
    /// <param name="needle">The literal to count. Must not be empty.</param>
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

    private static ServiceProvider BuildServices(IExplorerPlugin[] plugins)
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);
        var auth = Substitute.For<IExplorerAuthSession>();
        var selection = Substitute.For<IExplorerSelection>();
        selection.Selected.Returns((CatalogItem?)null);

        var store = new ExplorerPluginAccessStore();
        var catalog = new ExplorerPluginCatalog(plugins);
        var hostState = new ExplorerPluginHostState(selection, connection);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IExplorerPluginCatalog>(catalog);
        services.AddSingleton<IExplorerPluginAccessStore>(store);
        services.AddSingleton(hostState);
        services.AddSingleton(auth);
        services.AddSingleton<IExplorerPluginPreferences, FakeExplorerPluginPreferences>();
        services.AddSingleton<IExplorerPluginDomainResolver>(
            provider => new ExplorerPluginDomainResolver(catalog, provider));
        services.AddSingleton<IExplorerPluginHostContextFactory>(
            provider => new ExplorerPluginHostContextFactory(
                hostState,
                provider.GetRequiredService<IExplorerPluginPreferences>(),
                provider.GetRequiredService<IExplorerPluginDomainResolver>()));
        services.AddSingleton<IExplorerPluginAccessRefresher>(
            provider => new ExplorerPluginAccessRefresher(
                catalog,
                store,
                provider.GetRequiredService<IExplorerPluginHostContextFactory>()));

        return services.BuildServiceProvider();
    }

    // The catalog the product supplies is a <nav class="lx-shell-nav">, which is
    // the element carrying the fixed sidebar width. Standing in for it with the
    // same class is what lets a test say where the shell put it.
    private static RenderFragment CatalogFragment() => builder =>
    {
        builder.OpenElement(0, "nav");
        builder.AddAttribute(1, "class", CatalogPaneClass);
        builder.AddContent(2, CatalogMarker);
        builder.CloseElement();
    };

    private static RenderFragment DetailFragment() => builder =>
    {
        builder.OpenElement(0, "section");
        builder.AddContent(1, DetailMarker);
        builder.CloseElement();
    };

    /// <summary>A stand-in area view, so an activated plugin has something to render.</summary>
    private sealed class StubAreaView : ComponentBase
    {
    }
}
