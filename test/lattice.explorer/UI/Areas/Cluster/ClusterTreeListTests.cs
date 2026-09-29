using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// <c>/cluster/trees</c>: logical names only (physical shadows never listed),
/// app trees naming their app, filter, empty and error states, the compact list
/// form, and the "Reshard tree..." palette command's visible control.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterTreeListTests : ClusterTestContext
{
    [Test]
    public void It_lists_logical_trees_with_their_owners_and_never_a_physical_id()
    {
        UseTrees(
            Tree("a/crm/orders", 64) with { IsAlias = true, PhysicalTreeId = "a/crm/orders/resized/7f3a" },
            Tree("a/crm/orders/resized/7f3a", 64),
            Tree("t/acme/invoices") with { Lifecycle = TreeLifecycleState.SoftDeleted });

        var cut = RenderAt("/cluster/trees");

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].QuerySelector("a")!.TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(rows[0].QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("cluster/trees/a/crm/orders"));
            Assert.That(rows[0].TextContent, Does.Contain("app crm").And.Contain("Live, aliased"));
            Assert.That(rows[1].TextContent, Does.Contain("tenant acme").And.Contain("Deleted"));
            Assert.That(cut.Markup, Does.Not.Contain("resized/7f3a"));
        });
    }

    [Test]
    public void The_filter_narrows_by_name_owner_or_tenant_and_says_when_nothing_matches()
    {
        UseTrees(Tree("a/crm/orders"), Tree("invoices"));
        var cut = RenderAt("/cluster/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2)));

        cut.Find("input[type=search]").Input("CRM");
        Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1));

        cut.Find("input[type=search]").Input("nothing");
        Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No tree matches"));
    }

    [Test]
    public void An_empty_cluster_and_a_failed_read_each_say_so()
    {
        var empty = RenderAt("/cluster/trees");
        empty.WaitUntil(() => Assert.That(empty.Find(".lt-empty h2").TextContent, Is.EqualTo("No trees")));

        Explorer.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException("no"));
        Services.GetRequiredService<ClusterTreeCatalog>().Invalidate();
        var failed = RenderAt("/cluster/trees");
        failed.WaitUntil(() => Assert.That(failed.Find(".lt-cluster-error").TextContent, Is.EqualTo("You do not have permission to do this.")));
    }

    [Test]
    public void Refresh_reads_the_catalogue_again()
    {
        UseTrees(Tree("orders"));
        var cut = RenderAt("/cluster/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        UseTrees(Tree("orders"), Tree("invoices"));
        Button(cut, "Refresh").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2)));
    }

    [Test]
    public void Compact_rows_carry_the_name_its_owner_and_state_and_the_sheet_links_the_tree()
    {
        UseTrees(Tree("a/crm/orders", 64));
        var cut = RenderAt("/cluster/trees", LtBreakpoint.Compact);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(1)));
        var row = cut.Find(".lt-table-list__row");
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(row.QuerySelector(".lt-compact-row__primary")!.TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(row.TextContent, Does.Contain("app crm, 64 shards").And.Contain("Live"));
        });

        row.QuerySelector("button")!.Click();
        Assert.That(cut.FindAll(".lt-dialog a.lt-cluster-link").Select(link => link.TextContent), Does.Contain("Administer this tree"));
    }

    [Test]
    public void The_reshard_command_has_a_visible_control_that_opens_a_tree_picker()
    {
        UseTrees(Tree("a/crm/orders"));
        var cut = RenderAt("/cluster/trees");
        var area = Services.GetServices<IExplorerArea>().OfType<ClusterArea>().Single();
        var command = area.Commands.Single(candidate => candidate.Id == ClusterArea.ReshardCommandId);

        ExplorerCommandControls.AssertVisibleControl(cut, command);

        cut.Find($"[data-lt-command=\"{command.Id}\"]").Click();
        Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Reshard a tree"));
    }

    [Test]
    public void The_palette_invoking_the_command_opens_the_picker_and_a_known_tree_continues_to_its_reshard_page()
    {
        UseTrees(Tree("a/crm/orders"));
        var cut = RenderAt("/cluster/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        var command = Services.GetServices<IExplorerArea>().OfType<ClusterArea>().Single().Commands[0];

        cut.InvokeAsync(() => command.InvokeAsync!(CancellationToken.None).AsTask());
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog"), Has.Count.EqualTo(1)));

        cut.Find(".lt-dialog form").Submit();
        Assert.That(cut.Find(".lt-dialog .lt-field__error").TextContent, Does.Contain("Name the tree to reshard."));

        cut.Find(".lt-dialog input").Input("a/crm/missing");
        cut.Find(".lt-dialog form").Submit();
        Assert.That(cut.Find(".lt-dialog .lt-field__error").TextContent, Does.Contain("No tree is named a/crm/missing."));

        cut.Find(".lt-dialog input").Input("a/crm/orders");
        cut.Find(".lt-dialog form").Submit();
        Assert.That(Navigation.Uri, Does.EndWith("/cluster/trees/a/crm/orders/reshard"));
    }

    [Test]
    public void A_command_requested_before_the_page_renders_is_taken_on_arrival()
    {
        var signals = Services.GetRequiredService<ClusterCommandSignals>();
        signals.RequestAsync(ClusterArea.ReshardCommandId);

        var cut = RenderAt("/cluster/trees");

        Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Reshard a tree"));
    }

    [Test]
    public void At_compact_the_picker_is_a_sheet()
    {
        var cut = RenderAt("/cluster/trees", LtBreakpoint.Compact);

        Button(cut, "Reshard tree...").Click();

        Assert.That(cut.Find(".lt-dialog").ClassList, Does.Contain("lt-dialog--end"));
    }

    [Test]
    public void A_tree_link_without_an_address_is_plain_text()
    {
        var cut = Render<ClusterTreeLink>(parameters => parameters.Add(link => link.TreeId, "a/b/c/d/e/f/g/h/i"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("a"), Is.Empty);
            Assert.That(cut.Find("span").TextContent, Is.EqualTo("a/b/c/d/e/f/g/h/i"));
        });
    }
}
