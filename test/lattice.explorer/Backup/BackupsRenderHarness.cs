using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Backup.Components;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Session;

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
/// The panel reaches the <em>cluster</em> through exactly one service, so the
/// harness registers exactly one for that: a host-context factory whose context
/// hands back the stub domain. That is the controlled domain-model seam under
/// test as much as it is plumbing.
/// </para>
/// <para>
/// Alongside it the harness composes the shell's real session stack, which is
/// where the panel remembers and addresses its open surface. The real stack is
/// registered rather than substituted because the route model is a pure
/// in-memory type and the preference store defaults to an in-memory backing
/// store, so nothing here reaches a browser - and a substitute would let the
/// panel's use of the declared contract drift from the contract itself.
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
    /// <param name="preferencesLoaded">
    /// Whether the durable preference store is hydrated before the panel first
    /// reads it. Pass <see langword="false"/> to read the markup the panel
    /// renders <em>before</em> the retained surface has been restored, when
    /// neither surface is active.
    /// </param>
    /// <param name="address">
    /// An address to put the shell's router at before the panel mounts, so a
    /// test can observe a deep link. Omit to leave the router bare, which is
    /// what makes the remembered surface the one that opens.
    /// </param>
    /// <returns>The rendered markup.</returns>
    public static async Task<string> RenderPanelAsync(
        IBackupsDomain domain,
        BackupsSubTab subTab = BackupsSubTab.Existing,
        LatticeBreakpoint? breakpoint = LatticeBreakpoint.Expanded,
        Func<BackupsPanel, Task>? afterFirstRender = null,
        bool preferencesLoaded = true,
        string? address = null)
    {
        var (html, _) = await RenderPanelWithStateAsync(
            domain,
            subTab,
            breakpoint,
            afterFirstRender,
            preferencesLoaded,
            address);

        return html;
    }

    /// <summary>
    /// Renders the panel and hands back the shell state it left behind, so a
    /// test can assert what the area <em>remembered</em> and what it put in the
    /// address as well as what it drew.
    /// </summary>
    /// <param name="domain">The controlled domain model the panel resolves.</param>
    /// <param name="subTab">The surface the durable store is seeded with.</param>
    /// <param name="breakpoint">The breakpoint to cascade, or <see langword="null"/> for none.</param>
    /// <param name="afterFirstRender">An optional action run against the rendered panel.</param>
    /// <param name="preferencesLoaded">Whether the durable store is reachable.</param>
    /// <param name="address">An address to put the router at before the panel mounts.</param>
    /// <returns>The rendered markup, and the state the panel left.</returns>
    public static async Task<(string Html, BackupsShellState State)> RenderPanelWithStateAsync(
        IBackupsDomain domain,
        BackupsSubTab subTab = BackupsSubTab.Existing,
        LatticeBreakpoint? breakpoint = LatticeBreakpoint.Expanded,
        Func<BackupsPanel, Task>? afterFirstRender = null,
        bool preferencesLoaded = true,
        string? address = null)
    {
        var context = Substitute.For<IExplorerPluginHostContext>();
        context.PluginId.Returns(BackupsPluginKeys.PluginId);
        context.GetDomain<IBackupsDomain>().Returns(domain);

        var factory = Substitute.For<IExplorerPluginHostContextFactory>();
        factory.Create(BackupsPluginKeys.PluginId).Returns(context);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(factory);

        // The real shell-state stack: an in-memory route model and an in-memory
        // preference backing store, so nothing reaches a browser and the panel
        // is exercised against the contract it actually ships against.
        services.AddExplorerSession();

        // A store that never hydrates is how "the retained surface has not
        // resolved" is reproduced, which is a real state (prerender, or a head
        // whose browser storage is unreachable) and not a test artefact.
        if (!preferencesLoaded)
        {
            services.AddScoped<IUiPreferenceBackingStore, UnreachableBackingStore>();
        }

        await using var provider = services.BuildServiceProvider();

        // Seeded through the same contract the panel writes it with, so the
        // restore path under test is the product's own.
        if (preferencesLoaded)
        {
            provider.GetRequiredService<IExplorerPreferenceCatalog>()
                .Register(BackupsPluginKeys.SurfacePreference);

            var seed = provider.GetRequiredService<IExplorerShellPreferences>();
            await seed.EnsureLoadedAsync();
            await seed.SetAsync(BackupsPluginKeys.SurfacePreference, BackupsSurfaces.SlugFor(subTab));
        }

        var router = provider.GetRequiredService<IExplorerShellRouter>();
        if (address is not null)
        {
            router.SetAddress(address);
        }

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

            var state = new BackupsShellState(
                router.Current.Surface.Length != 0
                    ? router.Current.Surface
                    : router.Current.Parameters.GetValueOrEmpty(BackupsPluginKeys.SurfaceParameter),
                preferencesLoaded
                    ? provider.GetRequiredService<IExplorerShellPreferences>()
                        .GetOrDefault(BackupsPluginKeys.SurfacePreference, string.Empty)
                    : string.Empty);

            return (component.ToHtmlString(), state);
        });
    }

    /// <summary>
    /// What the area left behind in the shell's state: the surface the address
    /// names, and the surface the durable store remembers.
    /// </summary>
    /// <param name="AddressedSurface">The <c>surface</c> segment of the current address.</param>
    /// <param name="RememberedSurface">The surface slug the preference contract holds.</param>
    internal readonly record struct BackupsShellState(string AddressedSurface, string RememberedSurface);

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
