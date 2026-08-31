using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// Tier two: the catalog-kind toggle, and the announcement a caller who cannot
/// see the list depends on.
/// </summary>
/// <remarks>
/// <para>
/// The toggle was a hand-rolled <c>role=tablist</c> whose tabs carried no
/// <c>tabindex</c> and answered no arrow key, so it promised the tabs pattern
/// without implementing it. It now runs on the shared primitive, in the
/// segmented presentation, so it reads as a toggle between peers rather than as
/// a second copy of the rail.
/// </para>
/// <para>
/// Switching it replaces the whole list beneath it, asynchronously and without
/// the caller having caused each row to change - which is exactly the case
/// WCAG SC 4.1.3 covers.
/// </para>
/// <para>
/// bUnit's context locks its service collection after the first render, and
/// NUnit reuses one fixture instance across a fixture's tests by default, so a
/// fixture with more than one case must ask for an instance per case.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class NavigationPanelCatalogKindBunitTests : LatticeComponentTestContext
{
    [Test]
    public void The_catalog_kind_toggle_runs_on_the_shared_primitive()
    {
        ConfigureCatalog();

        var cut = Render<NavigationPanel>();
        var strip = cut.Find("[role=tablist]");

        Assert.Multiple(() =>
        {
            Assert.That(strip.GetAttribute("aria-label"), Is.EqualTo("Catalog kind"));
            Assert.That(
                cut.Find(".lx-tabstrip-host").ClassList,
                Does.Contain("lx-tabstrip-segmented"),
                "the segmented variant is what makes the tier legible without reading its labels");
            Assert.That(
                cut.FindAll("[role=tab]").Count(tab => tab.GetAttribute("tabindex") == "0"),
                Is.EqualTo(1),
                "a roving tabindex, which the hand-rolled copy never had");
        });
    }

    [Test]
    public void Every_catalog_kind_tab_names_the_list_it_swaps()
    {
        ConfigureCatalog();

        var cut = Render<NavigationPanel>();
        var list = cut.Find("#" + ExplorerShellRegions.CatalogList);

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.FindAll("[role=tab]")
                    .All(tab => tab.GetAttribute("aria-controls") == ExplorerShellRegions.CatalogList),
                Is.True);
            Assert.That(list.GetAttribute("role"), Is.EqualTo("tabpanel"));
            Assert.That(
                list.GetAttribute("aria-labelledby"),
                Is.EqualTo(ExplorerShellRegions.TabElementId(
                    ExplorerShellRegions.CatalogKindStrip,
                    ExplorerRouteSegments.Trees)));
        });
    }

    [Test]
    public void A_polite_live_region_is_in_the_document_before_it_has_anything_to_say()
    {
        // A live region rendered at the same moment as its message is silent, so
        // it has to be there from the first pass and merely change its text.
        ConfigureCatalog();

        var cut = Render<NavigationPanel>();
        var region = cut.Find("[role=status]");

        Assert.That(region.GetAttribute("aria-live"), Is.EqualTo("polite"));
    }

    [Test]
    public void Switching_the_catalog_kind_is_announced_politely()
    {
        ConfigureCatalog();

        var cut = Render<NavigationPanel>();
        cut.FindAll("[role=tab]").Single(tab => tab.TextContent.Trim() == "Views").Click();

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.Find("[role=tab][aria-selected=true]").TextContent.Trim(),
                Is.EqualTo("Views"),
                "prove the change happened before asserting how it was announced");
            Assert.That(
                cut.Find("[role=status]").TextContent,
                Does.Contain("Views"),
                "a caller who cannot see the list is otherwise told nothing at all");
        });
    }

    [Test]
    public void The_catalog_pane_carries_the_level_two_heading_beneath_the_surface_title()
    {
        ConfigureCatalog();

        var cut = Render<NavigationPanel>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("h1"), Is.Empty, "the surface title belongs to the shell, not to a pane");
            Assert.That(cut.Find("h2").TextContent.Trim(), Is.EqualTo("Catalog"));
        });
    }

    private void ConfigureCatalog()
    {
        JSInterop.Mode = JSRuntimeMode.Loose;

        var catalog = Substitute.For<ICatalogReader>();
        catalog
            .LoadAsync(Arg.Any<CatalogKind>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new CatalogPage
            {
                Items =
                [
                    new CatalogItem
                    {
                        Id = "orders",
                        Kind = callInfo.ArgAt<CatalogKind>(0),
                    },
                ],
            }));

        var preferences = Substitute.For<IUiPreferenceStore>();
        preferences.IsLoaded.Returns(true);
        preferences.GetOrDefault("nav-kind", CatalogKind.Trees).Returns(CatalogKind.Trees);
        preferences.GetOrDefault<CatalogItem?>("nav-selected", null).Returns((CatalogItem?)null);

        Services.AddSingleton(catalog);
        Services.AddSingleton(Substitute.For<IDeadLetterReader>());
        Services.AddSingleton(Substitute.For<IExplorerSelection>());
        Services.AddSingleton(Substitute.For<IExplorerSession>());
        Services.AddSingleton(preferences);
    }
}
