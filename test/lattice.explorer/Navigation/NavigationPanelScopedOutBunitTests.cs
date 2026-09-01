using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// Tier two: whether an empty catalog can say why it is empty.
/// </summary>
/// <remarks>
/// <para>
/// The reader applied the tenant scope and then discarded it, so a page emptied
/// by the scope was byte-identical to a page from a cluster that holds nothing.
/// The panel could only choose between "the read failed" and "there is nothing
/// here", and so told an operator whose scope was hiding every tree that the
/// cluster was empty - the one answer with no remedy, and the wrong one.
/// </para>
/// <para>
/// The distinguishing fact is the count the scope removed, not the presence of a
/// scope: a tenant-scoped cluster that genuinely holds nothing must still say it
/// is empty. Asserting a filter that removed nothing is the same class of
/// untruth as concealing one that did, so both directions are covered here.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class NavigationPanelScopedOutBunitTests : LatticeComponentTestContext
{
    [Test]
    public void A_catalog_emptied_by_the_tenant_scope_says_the_scope_did_it()
    {
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 3);

        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.Find(".lx-shell-nav-state-headline").TextContent,
            Is.EqualTo(ExplorerStateCopy.ScopedOut(ExplorerSubjects.ForCatalogKind(CatalogKind.Trees), "acme").Headline));
    }

    [Test]
    public void A_catalog_emptied_by_the_tenant_scope_names_the_tenant_responsible()
    {
        // Naming it is the whole remedy: without the tenant, "something is
        // filtered" is not actionable by the person reading it.
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 3);

        var cut = Render<NavigationPanel>();

        Assert.That(cut.Markup, Does.Contain("acme"));
    }

    [Test]
    public void A_catalog_emptied_by_the_tenant_scope_offers_the_way_out_of_it()
    {
        // The panel's state block renders the remedy prose, not the copy's
        // ActionLabel (which no surface in the repo consumes), so this asserts
        // what a reader is actually shown.
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 3);

        var cut = Render<NavigationPanel>();

        var scopedOut = ExplorerStateCopy.ScopedOut(
            ExplorerSubjects.ForCatalogKind(CatalogKind.Trees), "acme");

        Assert.Multiple(() =>
        {
            Assert.That(scopedOut.Remedy, Is.Not.Null.And.Not.Empty);
            Assert.That(cut.Markup, Does.Contain(scopedOut.Remedy!));
        });
    }

    [Test]
    public void A_scope_that_filtered_nothing_leaves_an_empty_catalog_saying_it_is_empty()
    {
        // The scope is active but removed nothing, so there is genuinely nothing
        // to show and nothing for the caller to undo.
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 0);

        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.Find(".lx-shell-nav-state-headline").TextContent,
            Is.EqualTo(ExplorerStateCopy.Empty(ExplorerSubjects.ForCatalogKind(CatalogKind.Trees)).Headline));
    }

    [Test]
    public void An_untenanted_empty_catalog_still_says_it_is_empty()
    {
        ConfigureCatalog(scopedToTenantId: null, scopeFilteredCount: 0);

        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.Find(".lx-shell-nav-state-headline").TextContent,
            Is.EqualTo(ExplorerStateCopy.Empty(ExplorerSubjects.ForCatalogKind(CatalogKind.Trees)).Headline));
    }

    [Test]
    public void The_live_region_announces_the_scope_rather_than_an_empty_cluster()
    {
        // A caller who cannot see the list learns the state only through the
        // live region, so the correction has to reach that text too.
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 3);

        var cut = Render<NavigationPanel>();
        var announcement = cut.Find("[aria-live=polite]").TextContent;

        var scopedOut = ExplorerStateCopy.ScopedOut(
            ExplorerSubjects.ForCatalogKind(CatalogKind.Trees), "acme");

        Assert.That(announcement, Does.Contain(scopedOut.Headline));
    }

    [Test]
    public void A_scope_that_filtered_only_some_rows_leaves_the_rows_speaking_for_themselves()
    {
        // Rows are present, so the state block never renders: the filtered count
        // must not turn a populated list into a message about emptiness.
        ConfigureCatalog(
            scopedToTenantId: "acme",
            scopeFilteredCount: 2,
            items:
            [
                new CatalogItem { Id = "t/acme/orders", Kind = CatalogKind.Trees, ShardCount = 4 },
            ]);

        var cut = Render<NavigationPanel>();

        Assert.That(cut.FindAll(".lx-shell-nav-state-headline"), Is.Empty);
    }

    private void ConfigureCatalog(
        string? scopedToTenantId,
        int scopeFilteredCount,
        CatalogItem[]? items = null)
    {
        JSInterop.Mode = JSRuntimeMode.Loose;

        var catalog = Substitute.For<ICatalogReader>();
        catalog
            .LoadAsync(Arg.Any<CatalogKind>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new CatalogPage
            {
                Items = items ?? [],
                ScopedToTenantId = scopedToTenantId,
                ScopeFilteredCount = scopeFilteredCount,
            }));

        Services.AddSingleton(catalog);
        Services.AddSingleton(Substitute.For<IDeadLetterReader>());
        Services.AddSingleton(Substitute.For<IExplorerSelection>());
        Services.AddSingleton(Substitute.For<IExplorerSession>());
        Services.AddExplorerSession();
    }
}
