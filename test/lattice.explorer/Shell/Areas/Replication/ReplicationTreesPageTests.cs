using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.Shell.Areas.Replication;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using static Orleans.Lattice.Explorer.Tests.Shell.Areas.Replication.ReplicationTestData;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Replication;

/// <summary>
/// <c>/replication/trees</c>: enrolled trees with merge mode, source and app ownership;
/// app-owned and static trees are read-only; enable and disable are confirmed and go
/// through the enrolment facade; filters, fault states and the compact presentation.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class ReplicationTreesPageTests : ReplicationTestContext
{
    private void UseTrees()
    {
        UseEstate();
        Control.Trees.AddRange(
        [
            Tree("orders"),
            Tree("stock", enabled: false, mode: LatticeMergeMode.OrSet),
            Tree("a/crm/contacts"),
            Tree("legacy", source: ReplicationEnrollmentSource.Static),
            Tree("both", source: ReplicationEnrollmentSource.RuntimeAndStatic),
            Tree("dual", ambiguous: true, mode: null),
        ]);
    }

    private static AngleSharp.Dom.IElement Row(IRenderedComponent<ReplicationTreesPage> cut, string tree) =>
        cut.FindAll("tbody tr").Single(row => row.Children[0].TextContent.Trim() == tree);

    private static string[] Cells(AngleSharp.Dom.IElement row) => [.. row.Children.Select(cell => cell.TextContent.Trim())];

    [Test]
    public void It_lists_each_tree_with_its_state_merge_mode_source_owner_and_link_health()
    {
        UseTrees();

        var cut = RenderAt<ReplicationTreesPage>("replication/trees");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Enrolled trees"));
            Assert.That(cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent.Trim()),
                Is.EqualTo(new[] { "a/crm/contacts", "both", "dual", "legacy", "orders", "stock" }));
            Assert.That(Cells(Row(cut, "orders"))[..6], Is.EqualTo(new[] { "orders", "Enabled", "LWW register", "Runtime", "Cluster", "Healthy" }));
            Assert.That(Cells(Row(cut, "stock"))[..6], Is.EqualTo(new[] { "stock", "Disabled", "OR-set", "Runtime", "Cluster", "No links" }));
            Assert.That(Cells(Row(cut, "both"))[3], Is.EqualTo("Runtime and static"));
            Assert.That(Cells(Row(cut, "dual"))[1..3], Is.EqualTo(new[] { "Ambiguous", "None" }));
            Assert.That(Row(cut, "orders").QuerySelector("th a")!.GetAttribute("href"), Is.EqualTo("replication/trees/orders"));
            Assert.That(cut.FindAll(".lt-replication-sections__link")[1].GetAttribute("aria-current"), Is.EqualTo("page"));
        });
    }

    [Test]
    public void An_app_owned_tree_is_read_only_and_links_to_its_app()
    {
        UseTrees();

        var cut = RenderAt<ReplicationTreesPage>("replication/trees");

        cut.WaitUntil(() =>
        {
            var row = Row(cut, "a/crm/contacts");
            Assert.That(row.QuerySelectorAll("button"), Is.Empty, "no toggle for an app-owned tree");
            Assert.That(row.QuerySelectorAll("a").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "replication/trees/a/crm/contacts", "apps/crm/replication", "apps/crm/replication" }));
            Assert.That(row.QuerySelector("[data-lt-replication-app]")!.TextContent, Is.EqualTo("Managed by crm"));
            Assert.That(Cells(row)[4], Is.EqualTo("App crm"));
            Assert.That(Cells(row)[5], Is.EqualTo("Stalled"));
        });
    }

    [Test]
    public void A_tree_declared_only_at_deployment_offers_no_toggle()
    {
        UseTrees();

        var cut = RenderAt<ReplicationTreesPage>("replication/trees");

        cut.WaitUntil(() =>
        {
            var row = Row(cut, "legacy");
            Assert.That(row.QuerySelectorAll("button"), Is.Empty);
            Assert.That(Cells(row)[6], Is.EqualTo("Declared at deployment"));
            Assert.That(Row(cut, "orders").QuerySelector("button")!.TextContent.Trim(), Is.EqualTo("Disable"));
            Assert.That(Row(cut, "stock").QuerySelector("button")!.TextContent.Trim(), Is.EqualTo("Enable"));
            Assert.That(Row(cut, "orders").QuerySelector("button")!.GetAttribute("aria-label"), Is.EqualTo("Disable replication for orders"));
        });
    }

    [Test]
    public void Disable_is_confirmed_by_typing_the_tree_name_and_goes_through_the_facade()
    {
        UseTrees();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        Row(cut, "orders").QuerySelector("button")!.Click();
        var confirm = cut.Find("form.lt-confirm");
        var disabledBeforeTyping = confirm.QuerySelector("button[type=\"submit\"]")!.HasAttribute("disabled");
        Assert.That(cut.Find(".lt-dialog").TextContent, Does.Contain("stop replicating").And.Contain("keeps its merge mode"));

        cut.Find("form.lt-confirm input").Input("orders");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(disabledBeforeTyping, Is.True);
            Assert.That(Control.Disables, Is.EqualTo(new[] { "orders" }));
            Assert.That(Cells(Row(cut, "orders"))[1], Is.EqualTo("Disabled"));
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("Replication disabled for orders."));
            Assert.That(ToastService.Toasts.Single().Tone, Is.EqualTo(LtToastTone.Success));
            Assert.That(cut.FindAll("form.lt-confirm"), Is.Empty);
        });
    }

    [Test]
    public void Disabling_a_tree_also_in_the_static_map_warns_that_it_stays_replicated()
    {
        UseTrees();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        Row(cut, "both").QuerySelector("button")!.Click();

        Assert.That(cut.Find(".lt-dialog").TextContent, Does.Contain("also declared in the deployment's static map"));
    }

    [Test]
    public void Cancelling_a_disable_calls_nothing()
    {
        UseTrees();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        Row(cut, "orders").QuerySelector("button")!.Click();
        cut.FindAll("form.lt-confirm button").Single(button => button.TextContent.Trim() == "Cancel").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("form.lt-confirm"), Is.Empty);
            Assert.That(Control.Disables, Is.Empty);
        });
    }

    [Test]
    public void A_denied_change_is_reported_without_the_server_detail()
    {
        UseTrees();
        Control.ChangeFailure = new LatticeAuthorizationDeniedException("secret detail");
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        Row(cut, "orders").QuerySelector("button")!.Click();
        cut.Find("form.lt-confirm input").Input("orders");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("You are not allowed to disable replication for orders."));
            Assert.That(ToastService.Toasts.Single().Tone, Is.EqualTo(LtToastTone.Danger));
            Assert.That(Cells(Row(cut, "orders"))[1], Is.EqualTo("Enabled"));
        });
    }

    [TestCase(typeof(InvalidOperationException), "The cluster refused to disable replication for orders. A tree's merge mode is fixed when it is first enabled.")]
    [TestCase(typeof(NotSupportedException), "This cluster does not serve replication enrolment.")]
    [TestCase(typeof(TimeoutException), "The cluster could not disable replication for orders. Try again in a moment.")]
    public void Other_change_faults_have_their_own_sentence(Type fault, string message)
    {
        UseTrees();
        Control.ChangeFailure = (Exception)Activator.CreateInstance(fault)!;
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        Row(cut, "orders").QuerySelector("button")!.Click();
        cut.Find("form.lt-confirm input").Input("orders");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() => Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo(message)));
    }

    [Test]
    public void A_restricted_identity_that_manages_no_tree_sees_an_empty_list_and_no_enable_control()
    {
        Status.Failure = new LatticeAuthorizationDeniedException();

        var cut = RenderAt<ReplicationTreesPage>("replication/trees");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("No tree you may manage is enrolled for replication."));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Does.Contain("Enable replication for a tree"),
                "the enrolment report was readable, so enabling a tree the caller holds a grant for is offered");
        });
    }

    [Test]
    public void A_denied_enrolment_report_is_told_so_and_offers_no_control()
    {
        Control.ReadFailure = new LatticeAuthorizationDeniedException();

        var cut = RenderAt<ReplicationTreesPage>("replication/trees");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Replication enrolment is not open to you"));
            Assert.That(cut.FindAll("button"), Is.Empty);
        });
    }

    [Test]
    public void With_no_enrolment_facade_the_page_says_it_is_not_served()
    {
        Services.RemoveAll<ILatticeReplicationControl>();

        var cut = RenderAt<ReplicationTreesPage>("replication/trees");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Replication enrolment is not served here")));
    }

    [Test]
    public void A_failed_enrolment_read_offers_try_again()
    {
        Control.ReadFailure = new TimeoutException();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Replication enrolment could not be read")));

        Control.ReadFailure = null;
        UseTrees();
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));
    }

    [TestCase("replication/trees?app=crm", new[] { "a/crm/contacts" })]
    [TestCase("replication/trees?health=stalled", new[] { "a/crm/contacts" })]
    [TestCase("replication/trees?health=healthy", new[] { "orders" })]
    [TestCase("replication/trees?region=us-east", new[] { "orders" })]
    public void Filters_narrow_the_trees(string address, string[] expected)
    {
        UseTrees();

        var cut = RenderAt<ReplicationTreesPage>(address);

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent.Trim()), Is.EqualTo(expected)));
    }

    [Test]
    public void A_health_filter_without_readable_status_explains_why_nothing_matches()
    {
        UseTrees();
        Status.Failure = new LatticeAuthorizationDeniedException();

        var cut = RenderAt<ReplicationTreesPage>("replication/trees?health=stalled");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-replication-note").TextContent, Does.Contain("Link health is unknown"));
            Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("No enrolled tree matches these filters."));
        });
    }

    [Test]
    public void The_trees_command_has_a_visible_control_on_its_page()
    {
        UseTrees();

        var cut = RenderAt<ReplicationTreesPage>("replication/trees");

        cut.WaitUntil(() => ExplorerCommandControls.AssertVisibleControl(cut, Area.Commands.Single(command => command.Id == ReplicationArea.TreesCommandId)));
    }

    [Test]
    public void Below_the_compact_width_rows_open_a_sheet_with_the_row_action()
    {
        UseTrees();

        var cut = RenderAt<ReplicationTreesPage>("replication/trees", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__open"), Has.Count.EqualTo(6)));

        var crm = cut.FindAll(".lt-table-list__open")[0];
        Assert.Multiple(() =>
        {
            Assert.That(crm.QuerySelector(".lt-compact-row")!.TextContent, Does.Contain("a/crm/contacts").And.Contain("Enabled").And.Contain("LWW register - Runtime - app crm"));
            Assert.That(crm.QuerySelectorAll("a, button"), Is.Empty, "a compact row holds no nested control");
        });

        crm.Click();
        cut.WaitUntil(() =>
        {
            var sheet = cut.Find(".lt-dialog");
            Assert.That(sheet.QuerySelectorAll("a").Select(link => link.GetAttribute("href")), Does.Contain("apps/crm/replication"));
            Assert.That(sheet.QuerySelectorAll("button").Select(button => button.TextContent.Trim()), Does.Not.Contain("Disable"));
        });

        cut.Find(".lt-dialog .lt-dialog__header button").Click();
        cut.FindAll(".lt-table-list__open").Single(button => button.TextContent.Contains("orders", StringComparison.Ordinal)).Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog").QuerySelectorAll("button").Select(button => button.TextContent.Trim()), Does.Contain("Disable")));
    }

    [Test]
    public void With_tenancy_on_tree_and_app_links_are_rooted_at_the_active_tenant()
    {
        UseTenancy("acme");
        UseTrees();

        var cut = RenderAt<ReplicationTreesPage>("t/acme/replication/trees", tenancy: true);

        cut.WaitUntil(() =>
        {
            Assert.That(Row(cut, "orders").QuerySelector("th a")!.GetAttribute("href"), Is.EqualTo("t/acme/replication/trees/orders"));
            Assert.That(Row(cut, "a/crm/contacts").QuerySelector("[data-lt-replication-app]")!.GetAttribute("href"), Is.EqualTo("t/acme/apps/crm/replication"));
        });
    }
}
