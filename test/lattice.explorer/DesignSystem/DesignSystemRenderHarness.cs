using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Renders a design-system component to static HTML so a component test can
/// assert on the markup a breakpoint produces.
/// </summary>
/// <remarks>
/// Uses the framework's own <see cref="HtmlRenderer"/>, the same mechanism the
/// Explorer's existing component tests use, so the design system needs no
/// extra component-testing dependency. Rendering is synchronous and driven
/// entirely by the parameters supplied, so a test never waits on a clock, a
/// timer, or a background task.
/// </remarks>
internal static class DesignSystemRenderHarness
{
    /// <summary>
    /// Renders <typeparamref name="TComponent"/> with the supplied parameters
    /// and returns its HTML.
    /// </summary>
    /// <typeparam name="TComponent">The component to render.</typeparam>
    /// <param name="parameters">
    /// The component parameters, keyed by parameter name. Pass an empty
    /// dictionary to render with defaults.
    /// </param>
    /// <param name="configureServices">
    /// An optional hook to register services the component resolves.
    /// </param>
    /// <returns>The rendered markup.</returns>
    public static async Task<string> RenderAsync<TComponent>(
        IDictionary<string, object?> parameters,
        Action<IServiceCollection>? configureServices = null)
        where TComponent : IComponent
    {
        var services = new ServiceCollection();
        services.AddLogging();
        configureServices?.Invoke(services);

        await using var provider = services.BuildServiceProvider();
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        return await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var component = await renderer.RenderComponentAsync<TComponent>(
                ParameterView.FromDictionary(parameters));
            return component.ToHtmlString();
        });
    }

    /// <summary>
    /// Renders <typeparamref name="TComponent"/> beneath a cascaded shell
    /// context, exactly as <c>LatticeAdaptiveRoot</c> supplies it, and returns
    /// its HTML.
    /// </summary>
    /// <typeparam name="TComponent">The component to render.</typeparam>
    /// <param name="context">The shell context to cascade.</param>
    /// <param name="parameters">The component's own parameters.</param>
    /// <returns>The rendered markup.</returns>
    public static Task<string> RenderCascadedAsync<TComponent>(
        LatticeAdaptiveContext context,
        IDictionary<string, object?> parameters)
        where TComponent : IComponent
    {
        var wrapper = new Dictionary<string, object?>
        {
            ["Value"] = context,
            ["ChildContent"] = (RenderFragment)(builder =>
            {
                builder.OpenComponent<TComponent>(0);

                // AddMultipleAttributes predates nullable annotations and asks
                // for a non-null value type; a null parameter value is legal and
                // means "not supplied", so the projection is deliberate.
                builder.AddMultipleAttributes(
                    1,
                    parameters.Select(p => new KeyValuePair<string, object>(p.Key, p.Value!)));
                builder.CloseComponent();
            }),
        };

        return RenderAsync<CascadingValue<LatticeAdaptiveContext>>(wrapper);
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
