using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.Tests.Tenancy;
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
            cut.Find(".lx-selection-message-headline").TextContent,
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
        // The remedy prose still has to be there: it names the way out for a
        // reader who cannot take the shortcut, and for one who is not permitted
        // to.
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
    public void A_scoped_out_catalog_offers_its_remedy_as_a_control_and_not_only_as_prose()
    {
        // The divergence this fixture's issue was raised for. ExplorerStateMessage
        // carries an ActionLabel so a state has a way out; the shared
        // SelectionStateView renders it, and the catalog - which hand-rolled its
        // own state block - did not. A reader was told what would fix it and
        // given nothing to click.
        var switcher = new StubTenantSwitcher(isActive: true, isOperator: true);
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 3, switcher: switcher);

        var cut = Render<NavigationPanel>();

        var action = cut.Find(".lx-selection-message-action");

        Assert.That(action.TextContent.Trim(), Is.EqualTo(ExplorerVocabulary.ClearScopeAction));
    }

    [Test]
    public void Taking_the_scoped_out_remedy_actually_widens_the_scope()
    {
        // A control that renders but resolves nothing would be the same defect
        // wearing a button, so this asserts the seam was actually asked.
        var switcher = new StubTenantSwitcher(isActive: true, isOperator: true);
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 3, switcher: switcher);

        var cut = Render<NavigationPanel>();
        cut.Find(".lx-selection-message-action").Click();

        Assert.That(switcher.RequestedScope, Is.EqualTo(ExplorerTenantVisibility.AllTenants));
    }

    [Test]
    public void A_caller_who_may_not_widen_the_scope_is_not_offered_the_control()
    {
        // The switcher is fail-closed and honours only an operator, so offering
        // the button to anyone else would promise a way out that silently
        // refuses. The prose remedy still stands, because it names something
        // this reader can ask someone else for.
        var switcher = new StubTenantSwitcher(isActive: true, isOperator: false);
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 3, switcher: switcher);

        var cut = Render<NavigationPanel>();

        var scopedOut = ExplorerStateCopy.ScopedOut(
            ExplorerSubjects.ForCatalogKind(CatalogKind.Trees), "acme");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lx-selection-message-action"), Is.Empty);
            Assert.That(cut.Markup, Does.Contain(scopedOut.Remedy!));
        });
    }

    [Test]
    public void The_catalog_and_the_selection_surfaces_share_one_renderer()
    {
        // The drift guard. Two renderers for one message type is how ActionLabel
        // came to reach one surface and silently not the other, so the catalog
        // must resolve through the shared component rather than through markup of
        // its own that happens to agree today.
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 3);

        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.FindAll(".lx-selection-message"),
            Is.Not.Empty,
            "the catalog state block must be rendered by SelectionStateView");
    }

    [Test]
    public void A_scope_that_filtered_nothing_leaves_an_empty_catalog_saying_it_is_empty()
    {
        // The scope is active but removed nothing, so there is genuinely nothing
        // to show and nothing for the caller to undo.
        ConfigureCatalog(scopedToTenantId: "acme", scopeFilteredCount: 0);

        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.Find(".lx-selection-message-headline").TextContent,
            Is.EqualTo(ExplorerStateCopy.Empty(ExplorerSubjects.ForCatalogKind(CatalogKind.Trees)).Headline));
    }

    [Test]
    public void An_untenanted_empty_catalog_still_says_it_is_empty()
    {
        ConfigureCatalog(scopedToTenantId: null, scopeFilteredCount: 0);

        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.Find(".lx-selection-message-headline").TextContent,
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

        Assert.That(cut.FindAll(".lx-selection-message-headline"), Is.Empty);
    }

    [Test]
    public void A_catalog_refused_for_want_of_a_grant_says_so_rather_than_reporting_a_failure()
    {
        // The server answered; it did not break. Reporting a refusal as a fault
        // sends the reader to check the cluster instead of their permissions.
        ConfigureRefusedCatalog(new LatticeStateApiException("Access to the state API was denied.")
        {
            RequiresAuthentication = true,
            IsPermissionDenied = true,
        });

        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.Find(".lx-selection-message-headline").TextContent,
            Is.EqualTo(ExplorerStateCopy.NotPermitted(ExplorerSubjects.ForCatalogKind(CatalogKind.Trees)).Headline));
    }

    [Test]
    public void A_catalog_refused_because_the_caller_is_anonymous_asks_them_to_sign_in()
    {
        ConfigureRefusedCatalog(new LatticeStateApiException("Authentication is required to access the state API.")
        {
            RequiresAuthentication = true,
            IsPermissionDenied = false,
        });

        var cut = Render<NavigationPanel>();

        Assert.That(
            cut.Find(".lx-selection-message-headline").TextContent,
            Is.EqualTo(ExplorerStateCopy.SignInRequired(ExplorerSubjects.ForCatalogKind(CatalogKind.Trees)).Headline));
    }

    [Test]
    public void A_signed_in_caller_missing_a_grant_is_never_told_to_sign_in()
    {
        // The loop this prevents: offering "sign in" to someone already signed in,
        // whose problem signing in again cannot possibly solve.
        ConfigureRefusedCatalog(new LatticeStateApiException("Access to the state API was denied.")
        {
            RequiresAuthentication = true,
            IsPermissionDenied = true,
        });

        var cut = Render<NavigationPanel>();

        var signIn = ExplorerStateCopy.SignInRequired(ExplorerSubjects.ForCatalogKind(CatalogKind.Trees));

        Assert.That(cut.Find(".lx-selection-message-headline").TextContent, Is.Not.EqualTo(signIn.Headline));
    }

    [Test]
    public void A_transport_failure_is_still_reported_as_a_failure_and_not_as_a_refusal()
    {
        // The opposite lie: telling someone they lack permission when the cluster
        // is simply unreachable would send them to an administrator who can do
        // nothing for them. A transient fault keeps the failure copy, whose
        // "Try again" affordance is the one that actually helps here.
        ConfigureRefusedCatalog(new LatticeStateApiException("The state API is unavailable.")
        {
            IsTransient = true,
            RequiresAuthentication = false,
            IsPermissionDenied = false,
        });

        var cut = Render<NavigationPanel>();

        var failed = ExplorerStateCopy.Failed(
            ExplorerSubjects.ForCatalogKind(CatalogKind.Trees),
            "The state API is unavailable.");

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.Find(".lx-selection-message-headline").TextContent,
                Is.EqualTo(failed.Headline));
            Assert.That(
                cut.FindAll(".lx-selection-message-action"),
                Is.Not.Empty,
                "a failed read is the one state the panel can re-issue, so it must offer the retry");
        });
    }

    [Test]
    public void A_failure_shows_the_cluster_s_own_detail_rather_than_only_a_generic_headline()
    {
        // The consolidation's other half. The hand-rolled error block rendered the
        // raw exception text while the live region announced the composed copy, so
        // the two audiences were told different things about the same failure. The
        // copy carries the detail, so rendering it serves both.
        ConfigureRefusedCatalog(new LatticeStateApiException("The state API is unavailable.")
        {
            IsTransient = true,
        });

        var cut = Render<NavigationPanel>();

        Assert.That(cut.Markup, Does.Contain("The state API is unavailable."));
    }

    [Test]
    public void A_refusal_is_not_offered_a_try_again_button()
    {
        // Retrying an unauthorized read changes nothing, so offering the retry
        // affordance invites the reader to hammer a door that will not open. The
        // copy for a refusal carries no action label of its own, and the panel
        // supplies no handler for one, so the shared renderer offers no control.
        ConfigureRefusedCatalog(new LatticeStateApiException("Access to the state API was denied.")
        {
            RequiresAuthentication = true,
            IsPermissionDenied = true,
        });

        var cut = Render<NavigationPanel>();

        Assert.That(cut.FindAll(".lx-selection-message-action"), Is.Empty);
    }

    [Test]
    public void A_refusal_outranks_the_tenant_scope_because_the_read_never_reached_it()
    {
        ConfigureRefusedCatalog(new LatticeStateApiException("Access to the state API was denied.")
        {
            RequiresAuthentication = true,
            IsPermissionDenied = true,
        });

        var cut = Render<NavigationPanel>();

        var scopedOut = ExplorerStateCopy.ScopedOut(ExplorerSubjects.ForCatalogKind(CatalogKind.Trees), "acme");

        Assert.That(cut.Find(".lx-selection-message-headline").TextContent, Is.Not.EqualTo(scopedOut.Headline));
    }

    [Test]
    public void The_three_empty_states_do_not_collapse_into_one_message()
    {
        // The regression this fixture exists to prevent: absent, scoped-out and
        // not-permitted all reading identically, which is what shipped.
        var subject = ExplorerSubjects.ForCatalogKind(CatalogKind.Trees);

        var absent = ExplorerStateCopy.Empty(subject).Headline;
        var scopedOut = ExplorerStateCopy.ScopedOut(subject, "acme").Headline;
        var notPermitted = ExplorerStateCopy.NotPermitted(subject).Headline;

        Assert.That(
            new[] { absent, scopedOut, notPermitted },
            Is.Unique);
    }

    private void ConfigureRefusedCatalog(Exception refusal)
    {
        JSInterop.Mode = JSRuntimeMode.Loose;

        var catalog = Substitute.For<ICatalogReader>();
        catalog
            .LoadAsync(Arg.Any<CatalogKind>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<CatalogPage>(refusal));

        Services.AddSingleton(catalog);
        Services.AddSingleton(Substitute.For<IDeadLetterReader>());
        Services.AddSingleton(Substitute.For<IExplorerSelection>());
        Services.AddSingleton(Substitute.For<IExplorerSession>());
        Services.AddExplorerSession();
    }

    private void ConfigureCatalog(
        string? scopedToTenantId,
        int scopeFilteredCount,
        CatalogItem[]? items = null,
        StubTenantSwitcher? switcher = null)
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

        // Registered only when the test asks for it, so the default path still
        // exercises a head with no tenancy seam at all.
        if (switcher is not null)
        {
            Services.AddSingleton<IExplorerTenantSwitcher>(switcher);
        }

        Services.AddExplorerSession();
    }
}
