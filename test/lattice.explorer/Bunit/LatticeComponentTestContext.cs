using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Bunit;

/// <summary>
/// The shared bUnit setup for Explorer component tests: a <see cref="BunitContext"/>
/// that registers the five services <c>AppShell</c> and the area plugins resolve,
/// renders beneath the same cascaded <see cref="LatticeAdaptiveContext"/> the shell
/// reads in the product, and leaves JSInterop in loose mode so a component that
/// makes an incidental interop call does not fault the render.
/// </summary>
/// <remarks>
/// <para>
/// <b>The rule this base class exists to enforce: assert against the parsed DOM,
/// never against raw markup strings.</b> bUnit parses rendered output through
/// AngleSharp into a real DOM, so <c>element.GetAttribute("aria-selected")</c>
/// returns <c>""</c> for a bare boolean attribute - exactly what a browser reports.
/// </para>
/// <para>
/// That distinction is the whole point. The hand-rolled harnesses this pattern
/// replaces render to a markup <em>string</em> and assert with
/// <c>string.Contains</c>. That invites the vacuity trap issue #1793 was caught by
/// only through luck: a guard written as
/// <c>Assert.That(html, Does.Not.Contain("aria-selected=\"\""))</c> can never fire,
/// because the static renderer emits the <em>bare</em> attribute name, not the
/// empty-string form a browser DOM reports after parsing. The author held a DOM
/// mental model while asserting against raw markup, and the assertion silently
/// tested nothing. Parsing the markup into a DOM first closes that gap by
/// construction - a natural assertion against <c>GetAttribute(...)</c> sees the
/// browser value and catches the bug without the author having to know the
/// raw-markup quirk.
/// </para>
/// <para>
/// These are pure unit tests: every service is a controlled stub, every gate
/// answers synchronously, and no cluster, TestServer, host, or channel is stood
/// up - so a fixture built on this base must never carry a slow test category.
/// </para>
/// </remarks>
public abstract class LatticeComponentTestContext : BunitContext
{
    /// <summary>The keyed access store the shell reads plugin decisions from.</summary>
    protected ExplorerPluginAccessStore AccessStore { get; } = new();

    /// <summary>
    /// Registers the shell's services over <paramref name="plugins"/> and puts
    /// JSInterop in loose mode, so a subclass renders a component the same way the
    /// product wires it. Call once per test, before rendering.
    /// </summary>
    /// <param name="plugins">The plugins the catalog yields.</param>
    protected void ConfigureShellServices(params IExplorerPlugin[] plugins)
    {
        // Loose so an incidental JS call from a component does not fault the render;
        // a test that asserts on an interop call opts into strict mode itself.
        JSInterop.Mode = JSRuntimeMode.Loose;

        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);
        var auth = Substitute.For<IExplorerAuthSession>();
        var selection = Substitute.For<IExplorerSelection>();
        selection.Selected.Returns((CatalogItem?)null);

        var catalog = new ExplorerPluginCatalog(plugins);
        var hostState = new ExplorerPluginHostState(selection, connection);

        // The shell reads its active area from the router and its "hide what I
        // cannot use" preference from the declared contract, so both are
        // registered for real: the route model is a pure in-memory type and the
        // preference backing store defaults to an in-memory one, so neither
        // reaches a browser and neither introduces a wait.
        Services.AddExplorerSession();

        Services.AddSingleton<IExplorerPluginCatalog>(catalog);
        Services.AddSingleton<IExplorerPluginAccessStore>(AccessStore);
        Services.AddSingleton(hostState);
        Services.AddSingleton(auth);
        Services.AddSingleton<IExplorerPluginPreferences, FakeExplorerPluginPreferences>();
        Services.AddSingleton<IExplorerPluginDomainResolver>(
            provider => new ExplorerPluginDomainResolver(catalog, provider));
        Services.AddSingleton<IExplorerPluginHostContextFactory>(
            provider => new ExplorerPluginHostContextFactory(
                hostState,
                provider.GetRequiredService<IExplorerPluginPreferences>(),
                provider.GetRequiredService<IExplorerPluginDomainResolver>()));
        Services.AddSingleton<IExplorerPluginAccessRefresher>(
            provider => new ExplorerPluginAccessRefresher(
                catalog,
                AccessStore,
                provider.GetRequiredService<IExplorerPluginHostContextFactory>()));
    }

    /// <summary>
    /// The cascaded shell context for <paramref name="breakpoint"/>, ready to pass
    /// as a cascading value so a rendered component reads the breakpoint the way it
    /// does in the product rather than from a parameter set on the component itself.
    /// </summary>
    /// <param name="breakpoint">The breakpoint to cascade.</param>
    protected static LatticeAdaptiveContext AdaptiveContext(
        LatticeBreakpoint breakpoint = LatticeBreakpoint.Expanded) =>
        new(breakpoint, LatticeDensity.Cosy, IsMeasured: true);

    /// <summary>
    /// An always-allowed area plugin, so a strip of any length can be composed.
    /// </summary>
    /// <param name="id">The plugin id.</param>
    /// <param name="label">The tab label.</param>
    /// <param name="order">The descriptor's ordering hint.</param>
    protected static IExplorerPlugin AreaPlugin(string id, string label, int order) =>
        new FakeExplorerPlugin(
            id,
            ExplorerPluginSurface.Area,
            order,
            label,
            ExplorerPluginAccessGates.Allowed,
            domainContract: null,
            typeof(StubAreaView));

    /// <summary>A stand-in area view, so an activated plugin has something to render.</summary>
    private sealed class StubAreaView : ComponentBase
    {
    }
}
