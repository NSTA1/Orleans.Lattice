using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// G4 (issue #5368): <b>New tree...</b> on <c>/cluster/trees</c> - the command's
/// visible control, hidden and refused for a caller who may not create trees;
/// the form starting at the library defaults; validation, the review's
/// pre-flight, the create, and landing on the new tree's Summary with what to do
/// next; and the dialog's reuse by a host that takes the created tree itself.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterTreeCreateDialogTests : ClusterTestContext
{
    private const string NewTree = "factory-orders";

    [SetUp]
    public void Cluster()
    {
        UseTrees(Tree("lines"));
        Admin.CheckTreeExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeExistenceResult { TreeId = call.Arg<string>(), Exists = call.Arg<string>() == "lines" });
        Admin.CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeCreationResult
            {
                TreeId = call.ArgAt<string>(0),
                Created = true,
                ShardCount = call.ArgAt<int?>(1) ?? 64,
                MaxLeafKeys = call.ArgAt<int?>(2) ?? 128,
                MaxInternalChildren = call.ArgAt<int?>(3) ?? 128,
            });
    }

    [Test]
    public void The_create_command_has_a_visible_control_for_a_caller_who_may_create_trees()
    {
        var cut = RenderAt("/cluster/trees");
        var command = Services.GetServices<IExplorerArea>().OfType<ClusterArea>().Single().Commands.Single(candidate => candidate.Id == ClusterArea.CreateTreeCommandId);

        cut.WaitUntil(() => ExplorerCommandControls.AssertVisibleControl(cut, command));
        Assert.Multiple(() =>
        {
            Assert.That(command.Title, Is.EqualTo("New tree..."));
            Assert.That(command.Target, Is.EqualTo(ClusterAddresses.Trees));
        });
    }

    [Test]
    public void For_a_caller_who_may_not_create_trees_the_control_is_hidden_and_the_command_refused()
    {
        Granted = Grants.Read;
        var cut = RenderAt("/cluster/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        var command = Services.GetServices<IExplorerArea>().OfType<ClusterArea>().Single().Commands.Single(candidate => candidate.Id == ClusterArea.CreateTreeCommandId);

        cut.InvokeAsync(() => command.InvokeAsync!(CancellationToken.None).AsTask());

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-create-tree-hidden]").TextContent, Is.EqualTo(ClusterTreeList.CreateRefusedText)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll($"[data-lt-command=\"{ClusterArea.CreateTreeCommandId}\"]"), Is.Empty);
            Assert.That(cut.FindAll(".lt-dialog"), Is.Empty);
        });
        Admin.DidNotReceive().CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void The_form_starts_at_the_library_defaults_and_explains_each_field()
    {
        var cut = OpenDialog();

        var inputs = cut.FindAll(".lt-dialog input");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("New tree"));
            Assert.That(inputs.Select(input => input.GetAttribute("value") ?? string.Empty), Is.EqualTo(new[] { string.Empty, "64", "128", "128" }));
            Assert.That(cut.Find(".lt-dialog").TextContent, Does.Contain("library default of 64").And.Contain("from 1 to 4,096")
                .And.Contain("at least 2").And.Contain("at least 3").And.Contain("reserved"));
        });
    }

    [Test]
    public void A_reserved_name_and_out_of_range_sizes_are_refused_beside_their_fields_before_the_cluster_is_asked()
    {
        var cut = OpenDialog();
        var inputs = cut.FindAll(".lt-dialog input");
        inputs[0].Input("_lattice_trees");
        inputs[1].Input("0");
        inputs[2].Input("1");
        inputs[3].Input("two");

        cut.Find(".lt-dialog form").Submit();

        cut.WaitUntil(() =>
        {
            var errors = cut.FindAll(".lt-dialog .lt-field__error").Select(error => error.TextContent).ToArray();
            Assert.That(errors, Has.Length.EqualTo(4));
            Assert.That(errors[0], Does.Contain("the '_lattice_' namespace is managed by the library"));
            Assert.That(errors[1], Does.Contain("from 1 to 4,096"));
            Assert.That(errors[2], Does.Contain("at least 2"));
            Assert.That(errors[3], Does.Contain("at least 3"));
        });
        Admin.DidNotReceive().CheckTreeExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_name_already_taken_is_refused_at_review_with_the_reason()
    {
        var cut = OpenDialog();
        cut.FindAll(".lt-dialog input")[0].Input("lines");

        cut.Find(".lt-dialog form").Submit();

        cut.WaitUntil(() => Assert.That(
            cut.Find("[data-lt-create-tree-refusal]").TextContent,
            Is.EqualTo("A tree named lines already exists. Creating it again would change nothing; choose another name.")));
        Assert.That(cut.FindAll("[data-lt-create-tree=review]"), Is.Empty);
    }

    [Test]
    public void Review_create_and_land_on_the_new_trees_summary_with_what_to_do_next()
    {
        var cut = OpenDialog();
        var inputs = cut.FindAll(".lt-dialog input");
        inputs[0].Input(" " + NewTree + " ");
        inputs[1].Input("4");

        cut.Find(".lt-dialog form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-create-tree=review]").TextContent, Does.Contain(
            "Create the empty tree factory-orders with 4 physical shards, 128 keys per leaf and 128 children per internal node.")));
        Admin.DidNotReceive().CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());

        Button(cut, "Create tree").Click();

        cut.WaitUntil(() => Assert.That(Navigation.Uri, Does.EndWith("/cluster/trees/factory-orders")));
        Admin.Received(1).CreateTreeAsync(NewTree, 4, 128, 128, Arg.Any<CancellationToken>());
        Assert.That(Toasts, Does.Contain("Tree factory-orders created."));

        var page = RenderAt("/cluster/trees/" + NewTree);
        page.WaitUntil(() =>
        {
            var created = page.Find("[data-lt-tree-created]");
            Assert.That(created.TextContent, Does.Contain("factory-orders is new and empty, with 4 physical shards, 128 keys per leaf and 128 children per internal node."));
            Assert.That(created.QuerySelector("a[href=\"cluster/trees/factory-orders/tools\"]")!.TextContent, Is.EqualTo("Seed it with a bulk load"));
            Assert.That(created.QuerySelector("[data-lt-tree-created-next=configuration]")!.GetAttribute("href"), Is.EqualTo("cluster/trees/factory-orders?tab=configuration"));
            Assert.That(created.QuerySelector("[data-lt-tree-created-next=data]")!.GetAttribute("href"), Is.EqualTo("data/factory-orders"));
        });

        var again = RenderAt("/cluster/trees/" + NewTree);
        again.WaitUntil(() => Assert.That(again.FindAll(".lt-tabs, [role=tablist]"), Is.Not.Empty));
        Assert.That(again.FindAll("[data-lt-tree-created]"), Is.Empty, "what to do next is offered once, on arrival");
    }

    [Test]
    public void Back_returns_to_the_form_keeping_what_was_typed()
    {
        var cut = OpenDialog();
        cut.FindAll(".lt-dialog input")[0].Input(NewTree);
        cut.Find(".lt-dialog form").Submit();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-create-tree=review]"), Has.Count.EqualTo(1)));

        Button(cut, "Back").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog input")[0].GetAttribute("value"), Is.EqualTo(NewTree)));
    }

    [Test]
    public void A_refusal_at_the_create_stays_on_the_review_and_nothing_is_navigated()
    {
        Admin.CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(new TreeCreationResult { TreeId = NewTree, Created = false });
        var cut = OpenDialog();
        cut.FindAll(".lt-dialog input")[0].Input(NewTree);
        cut.Find(".lt-dialog form").Submit();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-create-tree=review]"), Has.Count.EqualTo(1)));

        Button(cut, "Create tree").Click();

        cut.WaitUntil(() => Assert.That(
            cut.Find("[data-lt-create-tree-refusal]").TextContent,
            Is.EqualTo("The cluster already has a tree named factory-orders, so nothing was created and its configuration is unchanged.")));
        Assert.That(Navigation.Uri, Does.EndWith("/cluster/trees"));
    }

    [Test]
    public void Under_a_tenant_the_tree_is_created_there_and_a_tenant_at_its_quota_is_told_so()
    {
        var quota = Substitute.For<ILatticeTenantQuotaUsage>();
        quota.GetQuotaUsageAsync("acme", Arg.Any<CancellationToken>()).Returns(new TenantQuotaUsageReport
        {
            TenantId = "acme",
            TreeCount = new TenantQuotaDimensionUsage { Usage = 1, Limit = 2, BurstLimit = 2 },
        });
        Services.AddKeyedSingleton(ShellFacades.Key, quota);
        UseTrees(Tree("t/acme/lines"));

        var cut = OpenDialog("/t/acme/cluster/trees");
        Assert.That(cut.Find(".lt-dialog").TextContent, Does.Contain("It is created in tenant acme."));
        cut.FindAll(".lt-dialog input")[0].Input(NewTree);
        cut.Find(".lt-dialog form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-create-tree=review]").TextContent, Does.Contain("Create the empty tree factory-orders in tenant acme with")));

        quota.GetQuotaUsageAsync("acme", Arg.Any<CancellationToken>()).Returns(new TenantQuotaUsageReport
        {
            TenantId = "acme",
            TreeCount = new TenantQuotaDimensionUsage { Usage = 2, Limit = 2, BurstLimit = 2 },
        });
        Button(cut, "Back").Click();
        cut.Find(".lt-dialog form").Submit();

        cut.WaitUntil(() => Assert.That(
            cut.Find("[data-lt-create-tree-refusal]").TextContent,
            Is.EqualTo("Tenant acme is at its tree quota: 2 of 2 trees. Delete a tree, or ask a platform operator to raise the quota.")));
    }

    [Test]
    public void A_host_that_takes_the_created_tree_gets_it_and_nothing_is_navigated()
    {
        TreeCreationResult? taken = null;
        var closed = false;
        var cut = Render<ClusterTreeCreateDialog>(parameters => parameters
            .Add(dialog => dialog.Open, true)
            .Add(dialog => dialog.Title, "New tree from file")
            .Add(dialog => dialog.InitialTreeId, NewTree)
            .Add(dialog => dialog.OpenChanged, open => closed = !open)
            .Add(dialog => dialog.Created, result => taken = result));

        Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("New tree from file"));
        cut.Find(".lt-dialog form").Submit();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-create-tree=review]"), Has.Count.EqualTo(1)));
        var before = Navigation.Uri;

        Button(cut, "Create tree").Click();

        cut.WaitUntil(() => Assert.That(taken?.TreeId, Is.EqualTo(NewTree)));
        Assert.Multiple(() =>
        {
            Assert.That(closed, Is.True);
            Assert.That(Navigation.Uri, Is.EqualTo(before));
        });
    }

    [Test]
    public void Cancel_closes_and_a_reopened_dialog_starts_afresh()
    {
        var cut = OpenDialog();
        cut.FindAll(".lt-dialog input")[0].Input("lines");
        cut.Find(".lt-dialog form").Submit();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-create-tree-refusal]"), Has.Count.EqualTo(1)));

        Button(cut, "Cancel").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog"), Is.Empty));

        cut.Find($"[data-lt-command=\"{ClusterArea.CreateTreeCommandId}\"]").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog input")[0].GetAttribute("value") ?? string.Empty, Is.Empty));
        Assert.That(cut.FindAll("[data-lt-create-tree-refusal]"), Is.Empty);
    }

    [Test]
    public void At_compact_the_dialog_is_a_sheet()
    {
        var cut = OpenDialog(breakpoint: LtBreakpoint.Compact);

        Assert.That(cut.Find(".lt-dialog").ClassList, Does.Contain("lt-dialog--end"));
    }

    [Test]
    public void A_create_requested_before_the_page_renders_is_taken_on_arrival()
    {
        Services.GetRequiredService<ClusterCommandSignals>().RequestAsync(ClusterArea.CreateTreeCommandId);

        var cut = RenderAt("/cluster/trees");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("New tree")));
    }

    private IRenderedComponent<ClusterPage> OpenDialog(string address = "/cluster/trees", LtBreakpoint? breakpoint = null)
    {
        var cut = RenderAt(address, breakpoint);
        cut.WaitUntil(() => Assert.That(cut.FindAll($"[data-lt-command=\"{ClusterArea.CreateTreeCommandId}\"]"), Has.Count.EqualTo(1)));
        cut.Find($"[data-lt-command=\"{ClusterArea.CreateTreeCommandId}\"]").Click();
        // #4254: the dialog renders after an async continuation.
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-create-tree=compose]"), Has.Count.EqualTo(1)));
        return cut;
    }
}
