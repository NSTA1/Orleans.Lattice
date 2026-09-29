using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// <c>/cluster/wal</c>: the placement audit, the "Plan WAL move..." command's
/// visible control and its dialog, a plan at its own resumable address, and the
/// execute and reclaim verbs behind typed confirmations and the TreeLifecycle
/// grant.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterWalPageTests : ClusterTestContext
{
    private const string TreeId = "orders";

    [SetUp]
    public void Placement()
    {
        Admin.AuditWalPlacementAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeWalPlacementAudit
        {
            TreeId = TreeId, Version = 5, PartitionCount = 2, AllResolvableOnThisSilo = false,
            Partitions = [new TreeWalPartitionPlacement { Partition = 0, ProviderKey = "blob-a", ResolvableOnThisSilo = true }, new TreeWalPartitionPlacement { Partition = 1, ProviderKey = "blob-x" }],
            KnownProviderKeys = ["blob-a", "blob-b"],
        });
        Admin.PlanWalMoveAsync(TreeId, 1, "blob-b", Arg.Any<CancellationToken>()).Returns(new TreeWalMovePlan
        {
            TreeId = TreeId, Partition = 1, FromProviderKey = "blob-x", ToProviderKey = "blob-b", EntriesToCopy = 1200,
            SourceLowestOffset = 10, SourceHighestOffset = 1210, TargetResolvableOnThisSilo = true, PlacementVersion = 5,
        });
    }

    [Test]
    public void Without_a_tree_it_asks_for_one_and_the_audit_form_navigates()
    {
        UseTrees(Tree("orders"));
        var cut = RenderAt("/cluster/wal");

        Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Choose a tree"));
        cut.Find("form[aria-label='Choose a tree']").Submit();
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Name the tree to audit."));

        cut.Find("form[aria-label='Choose a tree'] input").Input("orders");
        cut.Find("form[aria-label='Choose a tree']").Submit();
        cut.WaitUntil(() => Assert.That(Navigation.Uri, Does.EndWith("/cluster/wal?tree=orders")));
    }

    [Test]
    public void The_audit_names_drift_in_words_and_lists_each_partition()
    {
        var cut = RenderAt("/cluster/wal?tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("Drift").And.Contain("2 partitions, placement version 5").And.Contain("blob-a, blob-b"));
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void The_plan_command_has_a_visible_control_whose_dialog_plans_at_a_resumable_address()
    {
        UseTrees(Tree("orders"));
        var cut = RenderAt("/cluster/wal?tree=orders");
        var command = Services.GetServices<IExplorerArea>().OfType<ClusterArea>().Single().Commands.Single(candidate => candidate.Id == ClusterArea.PlanWalMoveCommandId);

        ExplorerCommandControls.AssertVisibleControl(cut, command);
        cut.Find($"[data-lt-command=\"{command.Id}\"]").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog input").GetAttribute("value"), Is.EqualTo("orders")));
        Assert.That(cut.Markup, Does.Contain("Known keys: blob-a, blob-b."));

        cut.Find(".lt-dialog form").Submit();
        Assert.That(cut.FindAll(".lt-dialog .lt-field__error"), Has.Count.EqualTo(2));

        cut.FindAll(".lt-dialog input")[1].Input("1");
        cut.FindAll(".lt-dialog input")[2].Input("blob-b");
        cut.Find(".lt-dialog form").Submit();

        cut.WaitUntil(() => Assert.That(Navigation.Uri, Does.EndWith("/cluster/wal?tree=orders&partition=1&target=blob-b")));
    }

    [Test]
    public void The_palette_opens_the_plan_dialog_on_the_page()
    {
        var cut = RenderAt("/cluster/wal");
        var signals = Services.GetRequiredService<ClusterCommandSignals>();

        cut.InvokeAsync(() => signals.RequestAsync(ClusterArea.PlanWalMoveCommandId).AsTask());

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Plan a WAL move")));
    }

    [Test]
    public void A_plan_is_moved_behind_a_confirmation_and_its_retained_source_reclaimed_behind_another()
    {
        Admin.ExecuteWalMoveAsync(TreeId, 1, "blob-b", null, Arg.Any<CancellationToken>()).Returns(new TreeWalMoveReceipt
        {
            TreeId = TreeId, Partition = 1, FromProviderKey = "blob-x", ToProviderKey = "blob-b", CopiedFromOffset = 10, CopiedThroughOffset = 1210,
            NewPlacementVersion = 6, SourceRetained = true, Outcome = TreeWalMoveOutcome.Moved,
        });
        Admin.ReclaimMovedWalSourceAsync(TreeId, 1, "blob-x", Arg.Any<CancellationToken>()).Returns(new TreeWalMoveReceipt
        {
            TreeId = TreeId, Partition = 1, FromProviderKey = "blob-x", ToProviderKey = "blob-b", Outcome = TreeWalMoveOutcome.SourceReclaimed,
        });
        var cut = RenderAt("/cluster/wal?tree=orders&partition=1&target=blob-b");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("1,200").And.Contain("10 to 1,210")));
        Assert.That(HasButton(cut, "Reclaim the source..."), Is.False, "nothing to reclaim before a move");

        Button(cut, "Move partition...").Click();
        Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("Quiesces partition 1 briefly"));
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-result").TextContent, Does.Contain("The source is retained until you reclaim it.")));

        Button(cut, "Reclaim the source...").Click();
        Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("blob-x").And.Contain("can no longer be reverted"));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-result").TextContent, Does.Contain("was reclaimed"));
            Assert.That(Toasts, Does.Contain("WAL source reclaimed."));
        });
    }

    [Test]
    public void A_partition_already_at_its_target_offers_to_reclaim_a_chosen_orphaned_source()
    {
        Admin.PlanWalMoveAsync(TreeId, 0, "blob-a", Arg.Any<CancellationToken>()).Returns(new TreeWalMovePlan
        {
            TreeId = TreeId, Partition = 0, FromProviderKey = "blob-a", ToProviderKey = "blob-a", AlreadyAtTarget = true,
        });
        Admin.ReclaimMovedWalSourceAsync(TreeId, 0, "blob-b", Arg.Any<CancellationToken>())
            .Returns(new TreeWalMoveReceipt { TreeId = TreeId, FromProviderKey = "blob-b", Outcome = TreeWalMoveOutcome.SourceReclaimed });
        var cut = RenderAt("/cluster/wal?tree=orders&partition=0&target=blob-a");
        cut.WaitUntil(() => Assert.That(cut.FindAll("select"), Has.Count.EqualTo(1)));

        Assert.That(HasButton(cut, "Move partition..."), Is.False);
        cut.Find("select").Change("blob-b");
        Button(cut, "Reclaim the source...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("WAL source reclaimed.")));
    }

    [Test]
    public void Moving_needs_the_lifecycle_grant()
    {
        Granted = Grants.Read;

        var cut = RenderAt("/cluster/wal?tree=orders&partition=1&target=blob-b");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("needs the TreeLifecycle grant"));
            Assert.That(HasButton(cut, "Move partition..."), Is.False);
        });
    }

    [Test]
    public void At_compact_the_plan_dialog_is_a_sheet_and_the_partitions_are_rows()
    {
        var cut = RenderAt("/cluster/wal?tree=orders", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(2)));

        Button(cut, "Plan a move...").Click();

        Assert.That(cut.Find(".lt-dialog").ClassList, Does.Contain("lt-dialog--end"));
    }
}
