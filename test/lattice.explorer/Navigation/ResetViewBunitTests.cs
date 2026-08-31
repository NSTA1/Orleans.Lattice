using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.UI.Pages;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The reset-view escape: it discloses exactly what the contract remembers, and
/// clears all of it only when asked.
/// </summary>
/// <remarks>
/// A pure component test over stub services - no cluster, host or channel - so it
/// carries no slow category. Assertions read the parsed DOM rather than raw
/// markup, per <see cref="Bunit.LatticeComponentTestContext"/>'s rule.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ResetViewBunitTests : BunitContext
{
    private static readonly ExplorerPreferenceKey ExtraKey =
        new("feature.extra", "a preference some feature registered");

    [Test]
    public void Page_lists_every_declared_preference_before_resetting()
    {
        var preferences = Configure();

        var cut = Render<ResetView>();

        Assert.Multiple(() =>
        {
            var items = cut.FindAll("li").Select(static node => node.TextContent.Trim()).ToArray();
            foreach (var key in preferences.Keys)
            {
                Assert.That(items, Does.Contain(key.Description));
            }
        });
    }

    [Test]
    public void Page_discloses_a_key_a_feature_registered_without_being_edited()
    {
        // The practical payoff of an enumerated contract: a sibling issue's key
        // is disclosed and cleared here with no change to this page.
        var catalog = new ExplorerPreferenceCatalog();
        catalog.Register(ExtraKey);
        Configure(catalog);

        var cut = Render<ResetView>();

        Assert.That(
            cut.FindAll("li").Select(static node => node.TextContent.Trim()),
            Does.Contain(ExtraKey.Description));
    }

    [Test]
    public void Rendering_the_page_does_not_reset_anything()
    {
        // Following a link must not silently discard somebody's preferences.
        var preferences = Configure();
        preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants").GetAwaiter().GetResult();

        Render<ResetView>();

        Assert.That(
            preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, "none"),
            Is.EqualTo("tenants"));
    }

    [Test]
    public void Clicking_reset_forgets_the_remembered_view_and_confirms()
    {
        var preferences = Configure();
        preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants").GetAwaiter().GetResult();
        var cut = Render<ResetView>();

        cut.Find("button").Click();

        Assert.Multiple(() =>
        {
            Assert.That(
                preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, "none"),
                Is.EqualTo("none"));
            Assert.That(
                cut.FindAll("[role=status]"),
                Is.Not.Empty,
                "the outcome must be announced, not merely performed");
        });
    }

    [Test]
    public void Clicking_reset_twice_is_harmless()
    {
        var preferences = Configure();
        var cut = Render<ResetView>();

        cut.Find("button").Click();

        Assert.Multiple(() =>
        {
            // The button is gone once the confirmation shows, so a second reset is
            // not reachable - and the contract is still intact and readable.
            Assert.That(cut.FindAll("button"), Is.Empty);
            Assert.That(preferences.Keys, Is.Not.Empty);
        });
    }

    private ExplorerShellPreferences Configure(IExplorerPreferenceCatalog? catalog = null)
    {
        JSInterop.Mode = JSRuntimeMode.Loose;

        var preferences = new ExplorerShellPreferences(
            new FakeUiPreferenceStore(),
            catalog ?? new ExplorerPreferenceCatalog(),
            new Session.FakeExplorerPreferenceScopeProvider());

        Services.AddSingleton<IExplorerShellPreferences>(preferences);
        return preferences;
    }
}
