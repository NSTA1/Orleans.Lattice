using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Schema.Domain;

namespace Orleans.Lattice.Explorer.Tests.Schema;

/// <summary>
/// Renders one Schema concern over a real <see cref="SchemaSession"/> and a
/// scripted domain, so a test can assert on the markup a breakpoint produces.
/// </summary>
/// <remarks>
/// <para>
/// Uses the framework's own <see cref="HtmlRenderer"/>, the same mechanism the
/// design system's, the Backups plugin's and the Access plugin's component
/// tests use, so the plugin needs no extra component-testing dependency.
/// </para>
/// <para>
/// A Schema concern takes the session as its single parameter and resolves no
/// service of its own, so the harness registers none: the whole substitution
/// boundary is the <see cref="ISchemaPluginDomain"/> the session was built
/// over. Every render is therefore driven by values supplied up front and
/// never waits on a clock, a timer, or a background task.
/// </para>
/// </remarks>
internal static class SchemaRenderHarness
{
    /// <summary>The tree every scripted session is scoped to.</summary>
    public const string TreeId = "orders";

    /// <summary>
    /// Builds a session over <paramref name="domain"/> with the plugin gate
    /// open, a tree selected, and every scoped capability granted.
    /// </summary>
    /// <param name="domain">The scripted domain the session drives.</param>
    /// <remarks>
    /// The grants come from a real <see cref="ExplorerPluginAccessStore"/>
    /// rather than a stub, because the fail-closed posture is a property of
    /// reading that store: an unprobed capability reads as denied, so a stubbed
    /// store could not observe it.
    /// </remarks>
    public static SchemaSession Session(StubSchemaDomain domain)
    {
        var access = new ExplorerPluginAccessStore();
        foreach (var capability in SchemaTreeGrants.Capabilities)
        {
            access.Set(SchemaTreeGrants.KeyFor(TreeId, capability), ExplorerPluginAccess.Allowed);
        }

        return new SchemaSession(domain)
        {
            IsAllowed = true,
            TreeId = TreeId,
            Grants = SchemaTreeGrants.For(access, TreeId),
        };
    }

    /// <summary>
    /// Renders <typeparamref name="TConcern"/> over <paramref name="session"/>
    /// beneath a cascaded shell context.
    /// </summary>
    /// <typeparam name="TConcern">The Schema concern to render.</typeparam>
    /// <param name="session">The area's shared state.</param>
    /// <param name="breakpoint">The breakpoint to cascade.</param>
    /// <param name="afterFirstRender">
    /// An optional action run against the rendered concern before the markup is
    /// read, so a test can reach a state that only an interaction produces.
    /// </param>
    /// <returns>The rendered markup.</returns>
    public static async Task<string> RenderAsync<TConcern>(
        SchemaSession session,
        LatticeBreakpoint breakpoint = LatticeBreakpoint.Expanded,
        Func<TConcern, Task>? afterFirstRender = null)
        where TConcern : IComponent
    {
        var services = new ServiceCollection();
        services.AddLogging();

        await using var provider = services.BuildServiceProvider();
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        return await renderer.Dispatcher.InvokeAsync(async () =>
        {
            TConcern? concern = default;

            var parameters = ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                ["Value"] = new LatticeAdaptiveContext(
                    breakpoint,
                    LatticeDensity.Cosy,
                    IsMeasured: true),
                ["ChildContent"] = (RenderFragment)(builder =>
                {
                    builder.OpenComponent<TConcern>(0);
                    builder.AddComponentParameter(1, "Session", session);

                    // Captured so a test can drive the concern into a state
                    // only an interaction reaches - the compliance audit, for
                    // instance - and then read the markup that produces.
                    builder.AddComponentReferenceCapture(
                        2, instance => concern = (TConcern)instance);
                    builder.CloseComponent();
                }),
            });

            var component = await renderer
                .RenderComponentAsync<CascadingValue<LatticeAdaptiveContext>>(parameters);

            if (afterFirstRender is not null)
            {
                Assert.That(concern, Is.Not.Null, "the concern must render before a test can drive it");
                await afterFirstRender(concern!);

                // A concern that mutates the session raises SchemaSession.Changed,
                // and in the app the panel answers that by re-rendering the area,
                // which re-parameterises every concern. Nothing hosts the panel
                // here, so the harness plays that part: without it the render
                // tree would still hold the pre-interaction markup.
                await concern!.SetParametersAsync(ParameterView.Empty);
            }

            return component.ToHtmlString();
        });
    }
}
