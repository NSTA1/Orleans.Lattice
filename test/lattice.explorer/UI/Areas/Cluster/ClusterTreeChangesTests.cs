using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// Regressions for stale displays after a tree-changing action: a reshard, a
/// resize and its undo, a snapshot, and every lifecycle verb change the set of
/// trees or their topology, so each must forget the circuit's remembered tree
/// lists (the Cluster catalogue and the Data directory) at once rather than leave
/// them stale until a reload, and the tree page's heading must re-read the tree
/// once a lifecycle verb settles.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterTreeChangesTests : ClusterTestContext
{
    private const string TreeId = "a/crm/orders";

    [SetUp]
    public void Trees() => UseTrees(Tree(TreeId));

    [Test]
    public async Task A_started_reshard_and_its_settling_forget_the_tree_lists()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Reshard(false, 4), Reshard(true, 4, 8), Reshard(false, 8));
        Admin.ReshardTreeAsync(TreeId, 8, Arg.Any<CancellationToken>()).Returns(Reshard(true, 4, 8));
        var cut = RenderAt("/cluster/trees/a/crm/orders/reshard");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No reshard is running.")));
        var probe = await TreeListProbe.PrimeAsync(Services);

        cut.Find("form input").Input("8");
        cut.Find("form").Submit();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Reshard..."), Is.True));
        await probe.AssertNeitherForgottenAsync();
        Button(cut, "Reshard...").Click();
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Resharding to 8 physical shards.")));

        await probe.AssertBothForgottenAsync();

        var settled = await TreeListProbe.PrimeAsync(Services);
        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True);
        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("Reshard complete: 8 physical shards.")));

        await settled.AssertBothForgottenAsync();
    }

    [Test]
    public async Task A_started_resize_and_its_undo_each_forget_the_tree_lists()
    {
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(false));
        Admin.ResizeTreeAsync(TreeId, 256, 32, Arg.Any<CancellationToken>()).Returns(Resize(true, 256, 32));
        Admin.UndoTreeResizeAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(false));
        var cut = RenderAt("/cluster/trees/a/crm/orders/resize");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No resize is running.")));
        var started = await TreeListProbe.PrimeAsync(Services);

        cut.FindAll("form input")[0].Input("256");
        cut.FindAll("form input")[1].Input("32");
        cut.Find("form").Submit();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Resize..."), Is.True));
        Button(cut, "Resize...").Click();
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Resizing to 256 keys per leaf and 32 children per node.")));
        await started.AssertBothForgottenAsync();

        var undone = await TreeListProbe.PrimeAsync(Services);
        Button(cut, "Undo the last resize...").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("deletes the resized copy")));
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("Resize undone.")));

        await undone.AssertBothForgottenAsync();
    }

    [Test]
    public async Task A_started_snapshot_forgets_the_tree_lists_because_it_creates_a_tree()
    {
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeSnapshotStatus { TreeId = TreeId });
        Admin.SnapshotTreeAsync(TreeId, "a/crm/orders-copy", Arg.Any<TreeSnapshotMode>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(new TreeSnapshotStatus { TreeId = TreeId, InProgress = true, RequestedDestinationTreeId = "a/crm/orders-copy", RequestedMode = TreeSnapshotMode.Online });
        var cut = RenderAt("/cluster/trees/a/crm/orders/snapshot");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No snapshot is running.")));
        var probe = await TreeListProbe.PrimeAsync(Services);

        cut.FindAll("form input")[0].Input("a/crm/orders-copy");
        cut.Find("form").Submit();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Snapshot..."), Is.True));
        Button(cut, "Snapshot...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-confirm"), Is.Not.Empty));
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Copying into a/crm/orders-copy")));

        await probe.AssertBothForgottenAsync();
    }

    [Test]
    public async Task Deleting_a_tree_forgets_the_tree_lists()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = TreeId });
        Admin.DeleteTreeAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeDeletionStatus { TreeId = TreeId, IsDeleted = true, CanRecover = true });
        var cut = Lifecycle("Delete tree...");
        var probe = await TreeListProbe.PrimeAsync(Services);

        Button(cut, "Delete tree...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-confirm"), Is.Not.Empty));
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("Tree deleted. It can be recovered until its window closes.")));

        await probe.AssertBothForgottenAsync();
    }

    [Test]
    public async Task Recovering_and_purging_a_tree_each_forget_the_tree_lists()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeDeletionStatus { TreeId = TreeId, IsDeleted = true, CanRecover = true });
        Admin.RecoverTreeAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = TreeId });
        Admin.PurgeTreeAsync(TreeId, true, Arg.Any<CancellationToken>())
            .Returns(new TreeDeletionStatus { TreeId = TreeId, IsDeleted = true, PurgeComplete = true });

        var recover = Lifecycle("Recover tree...");
        var recovered = await TreeListProbe.PrimeAsync(Services);
        Button(recover, "Recover tree...").Click();
        ConfirmTyping(recover, TreeId);
        recover.WaitUntil(() => Assert.That(Toasts, Does.Contain("Tree recovered.")));
        await recovered.AssertBothForgottenAsync();

        var purge = Lifecycle("Purge now...");
        var purged = await TreeListProbe.PrimeAsync(Services);
        Button(purge, "Purge now...").Click();
        ConfirmTyping(purge, TreeId);
        purge.WaitUntil(() => Assert.That(purge.Markup, Does.Contain("Purged")));
        await purged.AssertBothForgottenAsync();
    }

    [Test]
    public async Task Setting_an_alias_forgets_the_tree_lists_and_redraws_the_trees_heading()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = TreeId });
        Admin.GetTreeConfigAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(
                new TreeConfigurationReport { TreeId = TreeId, Exists = true, ShardCount = 4 },
                new TreeConfigurationReport { TreeId = TreeId, Exists = true, ShardCount = 16 });
        Admin.SetTreeAliasAsync(TreeId, "a/crm/orders-v2", Arg.Any<CancellationToken>())
            .Returns(new TreeAliasResolution { TreeId = TreeId, PhysicalTreeId = "a/crm/orders-v2", IsAliased = true });
        UseTrees(Tree(TreeId), Tree("a/crm/orders-v2"));
        var cut = RenderAt("/cluster/trees/" + TreeId + "?tab=lifecycle");
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("4 shards"));
            Assert.That(cut.FindAll("form[aria-label='Set alias']"), Has.Count.EqualTo(1));
        });
        var probe = await TreeListProbe.PrimeAsync(Services);

        cut.Find("form[aria-label='Set alias'] input").Input("a/crm/orders-v2");
        cut.Find("form[aria-label='Set alias']").Submit();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-confirm"), Is.Not.Empty));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Alias set."));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("16 shards").And.Not.Contain("4 shards"));
        });
        await probe.AssertBothForgottenAsync();
    }

    private IRenderedComponent<Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterPage> Lifecycle(string verb)
    {
        var cut = RenderAt("/cluster/trees/" + TreeId + "?tab=lifecycle");
        cut.WaitUntil(() => Assert.That(HasButton(cut, verb), Is.True));
        return cut;
    }

    private static TreeReshardStatus Reshard(bool running, int current, int? requested = null) =>
        new() { TreeId = TreeId, InProgress = running, CurrentPhysicalShardCount = current, RequestedShardCount = requested, VirtualShardCount = 4096 };

    private static TreeResizeStatus Resize(bool running, int? leaf = null, int? children = null) =>
        new() { TreeId = TreeId, InProgress = running, CurrentMaxLeafKeys = 128, CurrentMaxInternalChildren = 64, RequestedMaxLeafKeys = leaf, RequestedMaxInternalChildren = children };
}
