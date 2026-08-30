using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Backup.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// Renders the Backups plugin's decomposed surface to static HTML so a test can
/// assert on the markup a breakpoint produces.
/// </summary>
/// <remarks>
/// <para>
/// Uses the framework's own <see cref="HtmlRenderer"/>, the same mechanism the
/// design system's component tests use, so the plugin needs no extra
/// component-testing dependency. Rendering is driven entirely by the stubbed
/// domain supplied here, so a test never waits on a clock, a timer, a network
/// call, or a background task.
/// </para>
/// <para>
/// The panel reaches the host through exactly one service, so the harness
/// registers exactly one: a host-context factory whose context hands back the
/// stub domain and the stub preference store. That is the controlled
/// domain-model seam under test as much as it is plumbing.
/// </para>
/// </remarks>
internal static class BackupsRenderHarness
{
    /// <summary>
    /// Renders the Backups panel beneath a cascaded shell context.
    /// </summary>
    /// <param name="domain">The controlled domain model the panel resolves.</param>
    /// <param name="subTab">The sub-tab the stub preference store restores.</param>
    /// <param name="breakpoint">The breakpoint to cascade, or <see langword="null"/> for none.</param>
    /// <param name="afterFirstRender">
    /// An optional action run against the rendered panel before the markup is
    /// read, so a test can reach a state that only an interaction produces.
    /// </param>
    /// <returns>The rendered markup.</returns>
    public static async Task<string> RenderPanelAsync(
        IBackupsDomain domain,
        BackupsSubTab subTab = BackupsSubTab.Existing,
        LatticeBreakpoint? breakpoint = LatticeBreakpoint.Expanded,
        Func<BackupsPanel, Task>? afterFirstRender = null)
    {
        var preferences = Substitute.For<IExplorerPluginPreferences>();
        preferences.IsLoaded.Returns(true);
        preferences.GetOrDefault(Arg.Any<string>(), Arg.Any<BackupsSubTab>()).Returns(subTab);

        var context = Substitute.For<IExplorerPluginHostContext>();
        context.PluginId.Returns(BackupsPluginKeys.PluginId);
        context.Preferences.Returns(preferences);
        context.GetDomain<IBackupsDomain>().Returns(domain);

        var factory = Substitute.For<IExplorerPluginHostContextFactory>();
        factory.Create(BackupsPluginKeys.PluginId).Returns(context);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(factory);

        await using var provider = services.BuildServiceProvider();
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        return await renderer.Dispatcher.InvokeAsync(async () =>
        {
            BackupsPanel? panel = null;

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
                        builder.OpenComponent<BackupsPanel>(0);

                        // Captured so a test can drive the panel into a state
                        // only an interaction reaches - opening a dialog, for
                        // instance - and then read the markup that produces.
                        builder.AddComponentReferenceCapture(
                            1, instance => panel = (BackupsPanel)instance);
                        builder.CloseComponent();
                    }),
                });

            var component = breakpoint is null
                ? await renderer.RenderComponentAsync<BackupsPanel>(parameters)
                : await renderer.RenderComponentAsync<CascadingValue<LatticeAdaptiveContext>>(parameters);

            if (afterFirstRender is not null)
            {
                Assert.That(panel, Is.Not.Null, "the panel must render before a test can drive it");
                await afterFirstRender(panel!);
            }

            return component.ToHtmlString();
        });
    }

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
}
