using System.Reflection;
using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Shell.Session;
using Orleans.Lattice.Explorer.Tests.Shell.Design;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// The reset-view page: it discloses exactly what the preference contract
/// remembers, and clears all of it only when asked.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ResetPageTests : ShellDesignTestContext
{
    private static readonly ExplorerPreferenceKey ExtraKey =
        new("feature.extra", "a preference some feature registered");

    [Test]
    public void The_page_is_routed_at_reset()
    {
        var route = typeof(ResetPage).GetCustomAttribute<RouteAttribute>();

        Assert.Multiple(() =>
        {
            Assert.That(route?.Template, Is.EqualTo("/reset"));
            Assert.That(ResetPage.Href, Is.EqualTo(route!.Template.TrimStart('/')), "links resolve against the document base");
        });
    }

    [Test]
    public void Page_lists_every_declared_preference_before_resetting()
    {
        var preferences = Configure();

        var cut = Render<ResetPage>();

        var items = cut.FindAll("li").Select(static node => node.TextContent.Trim()).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(preferences.Keys, Is.Not.Empty);
            foreach (var key in preferences.Keys)
            {
                Assert.That(items, Does.Contain(key.Description));
            }
        });
    }

    [Test]
    public void Page_discloses_a_key_a_feature_registered_without_being_edited()
    {
        var catalog = new ExplorerPreferenceCatalog();
        catalog.Register(ExtraKey);
        Configure(catalog);

        var cut = Render<ResetPage>();

        Assert.That(cut.FindAll("li").Select(static node => node.TextContent.Trim()), Does.Contain(ExtraKey.Description));
    }

    [Test]
    public void Rendering_the_page_does_not_reset_anything()
    {
        var preferences = Configure();
        preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants").GetAwaiter().GetResult();

        Render<ResetPage>();

        Assert.That(preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, "none"), Is.EqualTo("tenants"));
    }

    [Test]
    public void Clicking_reset_forgets_the_remembered_view_and_confirms()
    {
        var preferences = Configure();
        preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, "tenants").GetAwaiter().GetResult();
        var cut = Render<ResetPage>();

        cut.FindAll("button").Single(button => button.TextContent == "Reset view").Click();

        Assert.Multiple(() =>
        {
            Assert.That(preferences.GetOrDefault(ExplorerPreferenceKeys.ActiveArea, "none"), Is.EqualTo("none"));
            Assert.That(cut.FindAll("[role=status]"), Is.Not.Empty, "the outcome must be announced, not merely performed");
        });
    }

    [Test]
    public void Clicking_reset_twice_is_harmless()
    {
        var preferences = Configure();
        var cut = Render<ResetPage>();

        cut.FindAll("button").Single(button => button.TextContent == "Reset view").Click();

        Assert.Multiple(() =>
        {
            // The button is gone once the confirmation shows, so a second reset is
            // not reachable - and the contract is still intact and readable.
            Assert.That(cut.FindAll("button"), Is.Empty);
            Assert.That(preferences.Keys, Is.Not.Empty);
        });
    }

    [Test]
    public void Cancel_and_back_return_to_the_explorer_relative_to_the_document_base()
    {
        Configure();
        var cut = Render<ResetPage>();

        var cancel = cut.FindAll("a").Single(link => link.TextContent == "Cancel").GetAttribute("href");
        cut.FindAll("button").Single(button => button.TextContent == "Reset view").Click();
        var back = cut.FindAll("a").Single(link => link.TextContent == "Back to the Explorer").GetAttribute("href");

        Assert.Multiple(() =>
        {
            Assert.That(cancel, Is.EqualTo("./"));
            Assert.That(back, Is.EqualTo("./"));
        });
    }

    private ExplorerShellPreferences Configure(IExplorerPreferenceCatalog? catalog = null)
    {
        var preferences = new ExplorerShellPreferences(
            new InMemoryPreferenceStore(),
            catalog ?? new ExplorerPreferenceCatalog(),
            new FixedPreferenceScopeProvider());

        Services.AddSingleton<IExplorerShellPreferences>(preferences);
        return preferences;
    }
}
