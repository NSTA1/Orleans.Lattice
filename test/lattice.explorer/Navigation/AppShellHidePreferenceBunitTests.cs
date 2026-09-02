using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The "hide areas I cannot open" preference: its default, its persistence, and
/// the registration that makes the reset-view affordance able to disclose and
/// clear it.
/// </summary>
/// <remarks>
/// bUnit's context locks its service collection after the first render, and
/// NUnit reuses one fixture instance across a fixture's tests by default, so a
/// fixture with more than one case must ask for an instance per case.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppShellHidePreferenceBunitTests : LatticeComponentTestContext
{
    [Test]
    public void The_preference_defaults_to_showing_what_the_caller_cannot_use()
    {
        // The out-of-the-box answer, and the reason the whole demotion policy
        // works: a caller who cannot see that a Backups area exists cannot ask
        // an administrator for it.
        ConfigureShellServices(DeniedAreaPlugin("a", "Alpha"));

        var cut = RenderShell();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lx-shell-rail-demoted-label"), Has.Count.EqualTo(1));
            Assert.That(
                cut.Find("#" + ExplorerShellRegions.HideInaccessibleControl).HasAttribute("checked"),
                Is.False);
        });
    }

    [Test]
    public void A_remembered_preference_is_applied_when_the_rail_mounts()
    {
        ConfigureShellServices(DeniedAreaPlugin("a", "Alpha"));

        // Stand in for the previous session: the rail registers its key when it
        // mounts, and the value it wrote then outlives the session that wrote it.
        Services.GetRequiredService<IExplorerPreferenceCatalog>()
            .Register(ExplorerShellNavigationKeys.HideInaccessibleAreas);

        var preferences = Services.GetRequiredService<IExplorerShellPreferences>();
        preferences.EnsureLoadedAsync().GetAwaiter().GetResult();
        preferences
            .SetAsync(ExplorerShellNavigationKeys.HideInaccessibleAreas, true)
            .GetAwaiter()
            .GetResult();

        var cut = RenderShell();

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.FindAll(".lx-shell-rail-demoted-label"),
                Is.Empty,
                "a caller who has opted out is not shown the refusals again on every reload");
            Assert.That(
                cut.Find("#" + ExplorerShellRegions.HideInaccessibleControl).HasAttribute("checked"),
                Is.True);
        });
    }

    [Test]
    public void Changing_the_preference_hides_the_demoted_group_and_persists_the_choice()
    {
        ConfigureShellServices(DeniedAreaPlugin("a", "Alpha"));

        var cut = RenderShell();
        cut.Find("#" + ExplorerShellRegions.HideInaccessibleControl).Change(true);

        var preferences = Services.GetRequiredService<IExplorerShellPreferences>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lx-shell-rail-demoted-label"), Is.Empty);
            Assert.That(
                preferences.GetOrDefault(ExplorerShellNavigationKeys.HideInaccessibleAreas, false),
                Is.True,
                "the choice survives the session it was made in");
        });
    }

    [Test]
    public void An_invitation_is_never_hidden_by_the_preference()
    {
        // Hiding it would hide the remedy along with the refusal, and there is
        // nothing to be granted - the caller only has to sign in.
        ConfigureShellServices(
            GatedAreaPlugin("a", "Alpha", ExplorerPluginAccessGates.AuthenticationRequired));

        var cut = RenderShell();
        cut.Find("#" + ExplorerShellRegions.HideInaccessibleControl).Change(true);

        Assert.That(
            cut.FindAll("[role=tab]").Select(tab => tab.TextContent.Trim()),
            Is.EqualTo(new[] { "Explore", "Alpha" }));
    }

    [Test]
    public void The_rail_registers_its_key_so_the_reset_view_affordance_can_clear_it()
    {
        ConfigureShellServices();

        RenderShell();

        var preferences = Services.GetRequiredService<IExplorerShellPreferences>();

        Assert.That(
            preferences.Keys,
            Does.Contain(ExplorerShellNavigationKeys.HideInaccessibleAreas),
            "a key the contract does not know about is a key the reset escape cannot clear");
    }

    private IRenderedComponent<AppShell> RenderShell()
    {
        var catalog = (RenderFragment)(builder =>
        {
            builder.OpenElement(0, "nav");
            builder.AddAttribute(1, "aria-label", "catalog");
            builder.CloseElement();
        });

        var detail = (RenderFragment)(builder =>
        {
            builder.OpenElement(0, "section");
            builder.AddContent(1, "detail-surface");
            builder.CloseElement();
        });

        return Render<AppShell>(parameters => parameters
            .AddCascadingValue(AdaptiveContext(LatticeBreakpoint.Expanded))
            .Add(shell => shell.Catalog, catalog)
            .Add(shell => shell.ChildContent, detail));
    }

    private static IExplorerPlugin DeniedAreaPlugin(string id, string label) =>
        GatedAreaPlugin(id, label, ExplorerPluginAccessGates.Denied);

    private static IExplorerPlugin GatedAreaPlugin(string id, string label, IExplorerPluginAccessGate gate) =>
        new FakeExplorerPlugin(
            id,
            ExplorerPluginSurface.Area,
            100,
            label,
            gate,
            domainContract: null,
            typeof(StubGatedAreaView));

    /// <summary>A stand-in area view for a plugin behind a gate.</summary>
    private sealed class StubGatedAreaView : ComponentBase
    {
    }
}
