using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// An online reshard can shrink a tree (issue 4076, after core's #4069): the
/// form accepts any count from 2 to the tree's virtual slot count, a shrink
/// explains its trade-off before it is submitted, and its progress runs toward
/// the smaller count and stays running at the target until the cluster reports
/// the reshard complete, while the retired shards' storage is released.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterReshardShrinkTests : ClusterTestContext
{
    private const string TreeId = "a/crm/orders";
    private const string Address = "/cluster/trees/a/crm/orders/reshard";

    [Test]
    public void A_shrink_is_reviewed_with_its_trade_off_confirmed_and_submitted()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Idle(8));
        Admin.ReshardTreeAsync(TreeId, 4, Arg.Any<CancellationToken>()).Returns(Running(start: 8, current: 8, target: 4));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No reshard is running.")));

        Stage(cut, "4");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty, "a count below the current one is a shrink, not an error");
            Assert.That(cut.Find(".lt-cluster-review").TextContent, Does.Contain("Shrink a/crm/orders from 8 physical shards to 4 physical shards."));
            Assert.That(cut.Find(".lt-cluster-review").TextContent, Does.Contain("lower the tree's write and point-read throughput ceiling"));
            Assert.That(cut.Find(".lt-cluster-review").TextContent, Does.Contain("scans, counts, snapshots and resizes fan out to fewer shards"));
            Assert.That(cut.Markup, Does.Not.Contain("only grows"));
        });

        Button(cut, "Reshard...").Click();
        Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("Shrinks the tree to 4 physical shards"));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("Resharding to 4 physical shards."));
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Folding shards together"));
            Assert.That(Time.ArmedTimers, Is.EqualTo(1));
        });
        Admin.Received(1).ReshardTreeAsync(TreeId, 4, Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_grow_is_reviewed_without_the_shrink_trade_off()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Idle(4));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(1)));

        Stage(cut, "8");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-cluster-review").TextContent, Does.Contain("Grow a/crm/orders from 4 physical shards to 8 physical shards."));
            Assert.That(cut.Find(".lt-cluster-review").TextContent, Does.Not.Contain("throughput ceiling"));
        });
        Button(cut, "Reshard...").Click();
        Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("Grows the tree to 8 physical shards"));
    }

    [Test]
    public void The_target_runs_from_two_to_the_trees_virtual_slot_count_and_must_change_the_count()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Idle(8, virtualSlots: 16));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(1)));

        Assert.That(cut.Find(".lt-field__hint").TextContent, Does.Contain("From 2 to 16."));

        Stage(cut, "1");
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("at least 2"));
        Stage(cut, "8");
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("already has 8 physical shards"));
        Stage(cut, "17");
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("at most 16"));
        Stage(cut, "2");
        Assert.That(cut.Find(".lt-cluster-review").TextContent, Does.Contain("from 8 physical shards to 2 physical shards"));
    }

    [Test]
    public async Task A_shrink_at_its_target_keeps_running_while_retired_shards_are_released()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Running(start: 8, current: 6, target: 4), Running(start: 8, current: 4, target: 4), Idle(4));
        var cut = RenderAt(Address);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Folding shards together"));
            Assert.That(ProgressValue(cut), Is.EqualTo("40"), "two of four folds done, then the release: 2 of 5");
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("6 physical shards now, down to 4."));
            Assert.That(Time.ArmedTimers, Is.EqualTo(1));
        });

        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Releasing retired shards"));
            Assert.That(ProgressValue(cut), Is.EqualTo("80"), "reaching the target count is not done");
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Does.Contain("once the retired shards' storage is released"));
            Assert.That(cut.Find(".lt-cluster-stage .lt-pill").TextContent, Is.EqualTo("Running"));
            Assert.That(Toasts, Is.Empty, "the count at the target does not complete the reshard");
            Assert.That(cut.FindAll("form"), Is.Empty, "no reshard is staged over one still releasing storage");
        });
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "a reshard at its target but in flight is asked again");

        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Reshard complete: 4 physical shards."));
            Assert.That(cut.Markup, Does.Contain("No reshard is running."));
            Assert.That(cut.FindAll(".lt-progress"), Is.Empty);
            Assert.That(Time.ArmedTimers, Is.Zero);
        });
    }

    private static void Stage(IRenderedComponent<ClusterPage> cut, string target)
    {
        cut.Find("form input").Input(target);
        cut.Find("form").Submit();
    }

    private static string? ProgressValue(IRenderedComponent<ClusterPage> cut) =>
        cut.Find("[role=progressbar]").GetAttribute("aria-valuenow");

    private static TreeReshardStatus Idle(int current, int virtualSlots = 4096) =>
        new() { TreeId = TreeId, CurrentPhysicalShardCount = current, VirtualShardCount = virtualSlots };

    private static TreeReshardStatus Running(int start, int current, int target) =>
        new() { TreeId = TreeId, InProgress = true, StartPhysicalShardCount = start, CurrentPhysicalShardCount = current, TargetShardCount = target, VirtualShardCount = 4096 };
}
