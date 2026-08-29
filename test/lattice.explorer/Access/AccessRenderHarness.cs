using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Access.Views;
using Orleans.Lattice.Explorer.Access.Workspace;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Renders the Access plugin's decomposed surface to static HTML so a test can
/// assert on the markup a breakpoint produces.
/// </summary>
/// <remarks>
/// <para>
/// Uses the framework's own <see cref="HtmlRenderer"/>, the same mechanism the
/// design system's and the Backups plugin's component tests use, so the plugin
/// needs no extra component-testing dependency. Every render is driven by a
/// stubbed domain supplied up front, so a test never waits on a clock, a timer,
/// a network call, or a background task.
/// </para>
/// <para>
/// The panel reaches the host through exactly two services, so the harness
/// registers exactly two: a host-context factory whose context hands back the
/// stub domain, and the keyed access store its gate publishes into. That is the
/// controlled domain-model seam under test as much as it is plumbing.
/// </para>
/// </remarks>
internal static class AccessRenderHarness
{
    /// <summary>
    /// Renders the whole Access panel, optionally beneath a cascaded shell
    /// context.
    /// </summary>
    /// <param name="domain">The controlled domain model the panel resolves.</param>
    /// <param name="breakpoint">
    /// The breakpoint to cascade, or <see langword="null"/> to render with no
    /// ambient shell context at all - which is what exercises the panel's own
    /// adaptive-root fallback.
    /// </param>
    /// <param name="access">The plugin-level decision the gate has published.</param>
    public static Task<string> RenderPanelAsync(
        IAccessDomain domain,
        LatticeBreakpoint? breakpoint = LatticeBreakpoint.Expanded,
        ExplorerPluginAccess? access = null)
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(AccessPluginKeys.PluginId, access ?? ExplorerPluginAccess.Allowed);

        return RenderAsync<AccessPanel>(
            breakpoint,
            parameters: null,
            configure: services =>
            {
                var context = Substitute.For<IExplorerPluginHostContext>();
                context.PluginId.Returns(AccessPluginKeys.PluginId);
                context.GetDomain<IAccessDomain>().Returns(domain);

                var factory = Substitute.For<IExplorerPluginHostContextFactory>();
                factory.Create(AccessPluginKeys.PluginId).Returns(context);

                services.AddSingleton(factory);
                services.AddSingleton<IExplorerPluginAccessStore>(store);
            });
    }

    /// <summary>
    /// Renders one of the plugin's sub-surface views directly over a real
    /// <see cref="AccessWorkspace"/>, beneath a cascaded shell context.
    /// </summary>
    /// <typeparam name="TView">The view component to render.</typeparam>
    /// <param name="state">The workspace the view reads and drives.</param>
    /// <param name="breakpoint">The breakpoint to cascade.</param>
    public static Task<string> RenderViewAsync<TView>(
        AccessWorkspace state,
        LatticeBreakpoint? breakpoint = LatticeBreakpoint.Expanded)
        where TView : IComponent =>
        RenderAsync<TView>(
            breakpoint,
            parameters: new Dictionary<string, object?> { ["State"] = state },
            configure: null);

    /// <summary>
    /// Builds a workspace over <paramref name="domain"/> whose gate admits the
    /// caller, loads it, and activates <paramref name="surfaceId"/>.
    /// </summary>
    /// <param name="domain">The controlled domain model.</param>
    /// <param name="surfaceId">The sub-surface to activate.</param>
    /// <param name="selectedTreeId">An optional tree to pin.</param>
    public static async Task<AccessWorkspace> CreateWorkspaceAsync(
        IAccessDomain domain,
        string? surfaceId = null,
        string? selectedTreeId = null)
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(AccessPluginKeys.PluginId, ExplorerPluginAccess.Allowed);

        var workspace = new AccessWorkspace(domain, store);
        await workspace.InitializeAsync();

        if (surfaceId is not null)
        {
            await workspace.SelectSurfaceAsync(surfaceId);
        }

        if (selectedTreeId is not null)
        {
            workspace.SelectTree(selectedTreeId);
        }

        return workspace;
    }

    private static async Task<string> RenderAsync<TComponent>(
        LatticeBreakpoint? breakpoint,
        IDictionary<string, object?>? parameters,
        Action<IServiceCollection>? configure)
        where TComponent : IComponent
    {
        var services = new ServiceCollection();
        services.AddLogging();
        configure?.Invoke(services);

        await using var provider = services.BuildServiceProvider();
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        return await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var componentParameters = parameters is null
                ? ParameterView.Empty
                : ParameterView.FromDictionary(parameters);

            if (breakpoint is null)
            {
                var bare = await renderer.RenderComponentAsync<TComponent>(componentParameters);
                return bare.ToHtmlString();
            }

            var cascaded = ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                ["Value"] = new LatticeAdaptiveContext(
                    breakpoint.Value,
                    LatticeDensity.Cosy,
                    IsMeasured: true),
                ["ChildContent"] = (RenderFragment)(builder =>
                {
                    builder.OpenComponent<TComponent>(0);
                    if (parameters is not null)
                    {
                        // A literal sequence number with a bulk attribute add,
                        // rather than an incrementing one: the renderer's diffing
                        // requires sequence numbers to be source-order constants.
                        builder.AddMultipleAttributes(
                            1,
                            parameters.Select(pair => new KeyValuePair<string, object>(pair.Key, pair.Value!)));
                    }

                    builder.CloseComponent();
                }),
            });

            var component = await renderer
                .RenderComponentAsync<CascadingValue<LatticeAdaptiveContext>>(cascaded);
            return component.ToHtmlString();
        });
    }
}
