using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// Renders a per-selection plugin view to static HTML so a test can assert on
/// the markup it produces.
/// </summary>
/// <remarks>
/// <para>
/// Uses the framework's own <see cref="HtmlRenderer"/>, the same mechanism the
/// design system's, the Backups plugin's, and the Access plugin's component
/// tests use, so no plugin needs an extra component-testing dependency.
/// </para>
/// <para>
/// A view derived from <c>SelectionPluginViewBase</c> reaches its cluster
/// through exactly one domain contract, so the harness registers exactly one
/// service: the controlled surface the caller supplies. Every render is driven
/// by that surface, so a test never waits on a clock, a timer, a network call,
/// or a background task.
/// </para>
/// </remarks>
internal static class SelectionViewRenderHarness
{
    /// <summary>
    /// Renders <typeparamref name="TView"/> over <paramref name="surface"/>
    /// beneath a cascaded shell context.
    /// </summary>
    /// <typeparam name="TView">The selection view to render.</typeparam>
    /// <typeparam name="TSurface">The single domain contract the view resolves.</typeparam>
    /// <param name="surface">The controlled domain model.</param>
    /// <param name="selection">The selected tree or view the tab renders.</param>
    /// <param name="breakpoint">The breakpoint to cascade.</param>
    /// <param name="configure">Any additional service the view injects.</param>
    /// <returns>The rendered markup.</returns>
    public static async Task<string> RenderAsync<TView, TSurface>(
        TSurface surface,
        CatalogItem selection,
        LatticeBreakpoint breakpoint = LatticeBreakpoint.Expanded,
        Action<IServiceCollection>? configure = null)
        where TView : IComponent
        where TSurface : class
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(surface);
        configure?.Invoke(services);

        await using var provider = services.BuildServiceProvider();
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
                    builder.OpenComponent<TView>(0);
                    builder.AddComponentParameter(1, "Selection", selection);
                    builder.CloseComponent();
                }),
            });

            var component = await renderer
                .RenderComponentAsync<CascadingValue<LatticeAdaptiveContext>>(parameters);

            return component.ToHtmlString();
        });
    }

    /// <summary>
    /// Renders <typeparamref name="TComponent"/> from its parameters alone,
    /// beneath a cascaded shell context, for a child component that resolves no
    /// domain contract of its own.
    /// </summary>
    /// <typeparam name="TComponent">The component to render.</typeparam>
    /// <param name="parameters">The component's parameters.</param>
    /// <param name="breakpoint">The breakpoint to cascade.</param>
    /// <returns>The rendered markup.</returns>
    public static async Task<string> RenderComponentAsync<TComponent>(
        IReadOnlyDictionary<string, object?> parameters,
        LatticeBreakpoint breakpoint = LatticeBreakpoint.Expanded)
        where TComponent : IComponent
    {
        var services = new ServiceCollection();
        services.AddLogging();

        await using var provider = services.BuildServiceProvider();
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        return await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var cascaded = ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                ["Value"] = new LatticeAdaptiveContext(
                    breakpoint,
                    LatticeDensity.Cosy,
                    IsMeasured: true),
                ["ChildContent"] = (RenderFragment)(builder =>
                {
                    builder.OpenComponent<TComponent>(0);

                    // A literal sequence number with a bulk attribute add: the
                    // renderer's diffing requires source-order constants.
                    builder.AddMultipleAttributes(
                        1,
                        parameters.Select(pair => new KeyValuePair<string, object>(pair.Key, pair.Value!)));
                    builder.CloseComponent();
                }),
            });

            var component = await renderer
                .RenderComponentAsync<CascadingValue<LatticeAdaptiveContext>>(cascaded);

            return component.ToHtmlString();
        });
    }

    /// <summary>A plain tree selection, which is what every tab here renders for.</summary>
    /// <param name="id">The tree id.</param>
    public static CatalogItem Tree(string id = "orders") =>
        new() { Id = id, Kind = CatalogKind.Trees };

    /// <summary>A tag-index selection.</summary>
    /// <param name="id">The index name.</param>
    public static CatalogItem TagIndex(string id = "by-region") =>
        new() { Id = id, Kind = CatalogKind.TagIndexes, IndexName = id };
}
