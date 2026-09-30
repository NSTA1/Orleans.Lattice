using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// The Cluster pages against main's accept-then-poll tree operations (issue
/// 3958): an accepted resize undo or purge is followed until it has actually
/// finished rather than reported done when the call returns, every running
/// operation shows its progress on its own page and on the tree summary, and a
/// standalone status read that does not echo the trigger's target no longer
/// blanks the page's sentence.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterOperationProgressPagesTests : ClusterTestContext
{
    private const string TreeId = "a/crm/orders";

    // ----- Resize and its undo -----

    [Test]
    public void A_running_resize_shows_its_progress_and_keeps_its_target_across_a_standalone_read()
    {
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Resize(false), Resize(true, phase: TreeResizePhase.Swap, completed: 8, total: 11));
        Admin.ResizeTreeAsync(TreeId, 256, 32, Arg.Any<CancellationToken>())
            .Returns(Resize(true, 256, 32, TreeResizePhase.Copy, 2, 11));
        var cut = RenderAt("/cluster/trees/a/crm/orders/resize");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No resize is running.")));

        cut.FindAll("form input")[0].Input("256");
        cut.FindAll("form input")[1].Input("32");
        cut.Find("form").Submit();
        Button(cut, "Resize...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(ProgressValue(cut), Is.EqualTo("18"));
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Copying the tree at the new size"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("2 of 8 shards copied, then 3 steps to finish."));
        });

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Pointing the tree's name at the copy"));
            Assert.That(ProgressValue(cut), Is.EqualTo("72"));
            Assert.That(cut.Markup, Does.Contain("Resizing to 256 keys per leaf and 32 children per node."), "a standalone read does not echo the target, so the page keeps it");
        });
    }

    [Test]
    public void An_accepted_undo_is_followed_until_it_has_unwound_rather_than_reported_done_at_once()
    {
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Resize(true, 256, 32, TreeResizePhase.Copy, 2, 11), Resize(false, undo: true), Resize(false));
        Admin.UndoTreeResizeAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(true, undo: true));
        var cut = RenderAt("/cluster/trees/a/crm/orders/resize");
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Undo the last resize..."), Is.True));

        Button(cut, "Undo the last resize...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Undo accepted. It is unwinding the resize; this page follows it."));
            Assert.That(Toasts, Does.Not.Contain("Resize undone."), "the undo was only accepted");
            Assert.That(cut.Find(".lt-cluster-stage .lt-pill").TextContent, Is.EqualTo("Undoing"));
            Assert.That(cut.Markup, Does.Contain("An undo was accepted and is unwinding the resize."));
            Assert.That(cut.Find(".lt-progress").GetAttribute("data-lt-progress"), Is.EqualTo("indeterminate"));
            Assert.That(HasButton(cut, "Undo the last resize..."), Is.False, "an accepted undo is not offered again");
            Assert.That(Time.ArmedTimers, Is.EqualTo(1));
        });

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        cut.WaitUntil(() => Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.GetResizeStatusAsync)), Is.EqualTo(2)));
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "an undo still unwinding is asked again");
        Assert.That(Toasts, Does.Not.Contain("Resize undone."));
        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Resize undone."));
            Assert.That(cut.Markup, Does.Contain("No resize is running."));
            Assert.That(cut.FindAll(".lt-progress"), Is.Empty);
            Assert.That(Time.ArmedTimers, Is.Zero);
        });
    }

    [Test]
    public void Arriving_while_an_undo_of_a_finished_resize_unwinds_follows_it_and_stages_nothing()
    {
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(false, undo: true));

        var cut = RenderAt("/cluster/trees/a/crm/orders/resize");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-stage .lt-pill").TextContent, Is.EqualTo("Undoing"), "UndoRequested is read before InProgress");
            Assert.That(cut.FindAll("form"), Is.Empty, "no resize is staged over an unwinding undo");
            Assert.That(Time.ArmedTimers, Is.EqualTo(1));
        });
    }

    [Test]
    public void An_undo_that_could_not_be_applied_says_the_resize_carries_on()
    {
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Resize(true, undo: true), Resize(true, phase: TreeResizePhase.Copy, completed: 4, total: 11));

        var cut = RenderAt("/cluster/trees/a/crm/orders/resize");
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1)));
        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Has.Some.StartsWith("The undo could not be applied, so the resize carries on."));
            Assert.That(Toasts, Does.Not.Contain("Resize undone."));
            Assert.That(cut.Find(".lt-cluster-stage .lt-pill").TextContent, Is.EqualTo("Running"));
        });
    }

    [Test]
    public void An_undo_that_finished_within_the_call_is_reported_done()
    {
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(false));
        Admin.UndoTreeResizeAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(false));
        var cut = RenderAt("/cluster/trees/a/crm/orders/resize");
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Undo the last resize..."), Is.True));

        Button(cut, "Undo the last resize...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Resize undone."));
            Assert.That(Time.ArmedTimers, Is.Zero);
        });
    }

    // ----- Snapshot -----

    [Test]
    public void A_running_snapshot_shows_its_shards_and_keeps_its_destination_across_a_standalone_read()
    {
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeSnapshotStatus { TreeId = TreeId }, Snapshot(TreeSnapshotPhase.Copy, 3, 8));
        Admin.SnapshotTreeAsync(TreeId, "a/crm/orders-copy", TreeSnapshotMode.Online, null, null, Arg.Any<CancellationToken>())
            .Returns(Snapshot(TreeSnapshotPhase.BeginForwarding, 0, 8) with { RequestedDestinationTreeId = "a/crm/orders-copy", RequestedMode = TreeSnapshotMode.Online });
        var cut = RenderAt("/cluster/trees/a/crm/orders/snapshot");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No snapshot is running.")));

        cut.FindAll("form input")[0].Input("a/crm/orders-copy");
        cut.Find("form").Submit();
        Button(cut, "Snapshot...").Click();
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Starting to forward live writes")));

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("Copying into a/crm/orders-copy, online."));
            Assert.That(ProgressValue(cut), Is.EqualTo("37"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("3 of 8 shards copied."));
        });
    }

    [Test]
    public void Arriving_at_a_running_snapshot_never_writes_an_empty_destination()
    {
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Snapshot(TreeSnapshotPhase.Copy, 1, 2));

        var cut = RenderAt("/cluster/trees/a/crm/orders/snapshot");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("A snapshot of this tree is running."));
            Assert.That(cut.Markup, Does.Not.Contain("Copying into ,"));
            Assert.That(ProgressValue(cut), Is.EqualTo("50"));
        });
    }

    // ----- Reshard -----

    [Test]
    public void A_running_reshard_is_measured_from_where_it_started_on_a_standalone_read()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeReshardStatus { TreeId = TreeId, InProgress = true, CurrentPhysicalShardCount = 5, TargetShardCount = 8, StartPhysicalShardCount = 2, VirtualShardCount = 4096 });

        var cut = RenderAt("/cluster/trees/a/crm/orders/reshard");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("Resharding to 8 physical shards."), "the target comes from the coordinator, not only from a trigger's echo");
            Assert.That(ProgressValue(cut), Is.EqualTo("42"), "three of six splits, then the step that completes it: 3 of 7");
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("5 of 8 physical shards."));
        });
    }

    [Test]
    public void A_status_read_that_fails_mid_follow_keeps_following_with_backoff()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromResult(new TreeReshardStatus { TreeId = TreeId, InProgress = true, CurrentPhysicalShardCount = 4, TargetShardCount = 8, StartPhysicalShardCount = 4 }),
                _ => Task.FromException<TreeReshardStatus>(new InvalidOperationException("The connection dropped.")),
                _ => Task.FromResult(new TreeReshardStatus { TreeId = TreeId, CurrentPhysicalShardCount = 8 }));
        var cut = RenderAt("/cluster/trees/a/crm/orders/reshard");
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1)));

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        cut.WaitUntil(() => Assert.That(ReshardReads(), Is.EqualTo(2)));
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "a failed read does not end the follow");
        Assert.That(cut.Markup, Does.Contain("Resharding to 8 physical shards."), "the last answer stays on screen");

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        Assert.That(ReshardReads(), Is.EqualTo(2), "after a failure the poller waits longer");
        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Reshard complete: 8 physical shards."));
            Assert.That(Time.ArmedTimers, Is.Zero);
        });
    }

    // ----- Purge -----

    [Test]
    public void An_accepted_purge_is_followed_to_completion_rather_than_reported_purged_at_once()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Deleted(), Purging(3, 4), Purged(4));
        Admin.PurgeTreeAsync(TreeId, true, Arg.Any<CancellationToken>()).Returns(Purging(1, 4));
        var cut = RenderTab<ClusterTreeLifecycle>();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Purge now..."), Is.True));

        Button(cut, "Purge now...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Purge accepted. Its shards are being removed; this page follows it."));
            Assert.That(Toasts, Does.Not.Contain("Tree purged."), "the purge was only accepted");
            Assert.That(cut.Markup, Does.Contain("Purging"));
            Assert.That(ProgressValue(cut), Is.EqualTo("25"));
            Assert.That(HasButton(cut, "Purge now..."), Is.False);
            Assert.That(HasButton(cut, "Recover tree..."), Is.False);
            Assert.That(Time.ArmedTimers, Is.EqualTo(1));
        });

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        cut.WaitUntil(() => Assert.That(ProgressValue(cut), Is.EqualTo("75")));
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True);
        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Tree purged."));
            Assert.That(ProgressValue(cut), Is.EqualTo("100"));
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Purged"));
            Assert.That(cut.Markup, Does.Not.Contain("Recoverable until"), "a purged tree cannot be recovered");
            Assert.That(Time.ArmedTimers, Is.Zero);
        });
    }

    [Test]
    public void Arriving_at_a_running_purge_resumes_following_it()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Purging(2, 8));

        var cut = RenderTab<ClusterTreeLifecycle>();

        cut.WaitUntil(() =>
        {
            Assert.That(ProgressValue(cut), Is.EqualTo("25"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("2 of 8 shards purged."));
            Assert.That(Time.ArmedTimers, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_tree_created_again_under_a_purged_id_can_be_deleted()
    {
        // #3945: a purged id registered again reads as the live tree it now is.
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeDeletionStatus { TreeId = TreeId });

        var cut = RenderTab<ClusterTreeLifecycle>();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("Live"));
            Assert.That(HasButton(cut, "Delete tree..."), Is.True);
            Assert.That(cut.FindAll(".lt-progress"), Is.Empty);
        });
    }

    // ----- Summary -----

    [Test]
    public void The_summary_shows_each_running_operations_progress_and_follows_it_until_it_settles()
    {
        StubSummaryReads();
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeReshardStatus { TreeId = TreeId, InProgress = true, CurrentPhysicalShardCount = 3, TargetShardCount = 8, StartPhysicalShardCount = 2 },
                     new TreeReshardStatus { TreeId = TreeId, CurrentPhysicalShardCount = 8 });
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(true, phase: TreeResizePhase.RetireOldCopy, completed: 10, total: 11));
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeSnapshotStatus { TreeId = TreeId });

        var cut = RenderTab<ClusterTreeSummary>();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("In progress, to 8 physical shards."));
            Assert.That(cut.Markup, Does.Not.Contain("to  shards"));
            Assert.That(cut.FindAll("[role=progressbar]").Select(bar => bar.GetAttribute("aria-label")), Is.EqualTo(new[] { "Reshard progress", "Resize progress" }));
            Assert.That(cut.FindAll("[role=progressbar]").Select(bar => bar.GetAttribute("aria-valuenow")), Is.EqualTo(new[] { "14", "90" }));
            Assert.That(Time.ArmedTimers, Is.EqualTo(1));
        });

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=progressbar]").Select(bar => bar.GetAttribute("aria-label")), Is.EqualTo(new[] { "Resize progress" })));
    }

    [Test]
    public void The_summary_keeps_following_a_shrink_that_reached_its_target_until_it_completes()
    {
        StubSummaryReads();
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeReshardStatus { TreeId = TreeId, InProgress = true, CurrentPhysicalShardCount = 4, TargetShardCount = 4, StartPhysicalShardCount = 8 },
                     new TreeReshardStatus { TreeId = TreeId, CurrentPhysicalShardCount = 4 });
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(false));
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeSnapshotStatus { TreeId = TreeId });

        var cut = RenderTab<ClusterTreeSummary>();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("In progress, to 4 physical shards."));
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Releasing retired shards"));
            Assert.That(ProgressValue(cut), Is.EqualTo("80"));
            Assert.That(Time.ArmedTimers, Is.EqualTo(1), "at the target count the reshard is still followed");
        });

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("None in progress."));
            Assert.That(cut.FindAll("[role=progressbar]"), Is.Empty);
        });
    }

    [Test]
    public void The_summary_follows_nothing_when_nothing_runs()
    {
        StubSummaryReads();
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeReshardStatus { TreeId = TreeId });
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(false));
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeSnapshotStatus { TreeId = TreeId });

        var cut = RenderTab<ClusterTreeSummary>();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("128 keys per leaf, 64 children per node."));
            Assert.That(cut.FindAll("[role=progressbar]"), Is.Empty);
            Assert.That(Time.ArmedTimers, Is.Zero);
        });
    }

    private void StubSummaryReads()
    {
        Admin.GetTreeStatsAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeStatsReport { TreeId = TreeId });
        Admin.ResolveTreeAliasAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeAliasResolution { TreeId = TreeId, PhysicalTreeId = TreeId });
    }

    private int ReshardReads() =>
        Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.GetReshardStatusAsync));

    private static string? ProgressValue<TComponent>(IRenderedComponent<TComponent> cut)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        cut.Find("[role=progressbar]").GetAttribute("aria-valuenow");

    private IRenderedComponent<TTab> RenderTab<TTab>()
        where TTab : Microsoft.AspNetCore.Components.IComponent
    {
        var capabilities = new LatticeTreeAdminCapabilities
        {
            TreeId = TreeId,
            Schema = new LatticeSchemaCapabilities { TreeId = TreeId },
            CanViewDiagnostics = true,
            CanAdministerTree = true,
            CanManageTreeLifecycle = true,
            CanBulkLoad = true,
        };

        return Render(builder =>
        {
            builder.OpenComponent<TTab>(0);
            builder.AddComponentParameter(1, nameof(ClusterTreeLifecycle.TreeId), TreeId);
            builder.AddComponentParameter(2, nameof(ClusterTreeLifecycle.Capabilities), capabilities);
            builder.CloseComponent();
        }).FindComponent<TTab>();
    }

    private static TreeResizeStatus Resize(
        bool running, int? leaf = null, int? children = null, TreeResizePhase? phase = null, int completed = 0, int? total = null, bool undo = false) =>
        new()
        {
            TreeId = TreeId,
            InProgress = running,
            CurrentMaxLeafKeys = 128,
            CurrentMaxInternalChildren = 64,
            RequestedMaxLeafKeys = leaf,
            RequestedMaxInternalChildren = children,
            UndoRequested = undo,
            Phase = undo ? TreeResizePhase.Undo : phase,
            CompletedUnits = completed,
            TotalUnits = total,
        };

    private static TreeSnapshotStatus Snapshot(TreeSnapshotPhase phase, int copied, int total) =>
        new() { TreeId = TreeId, InProgress = true, Phase = phase, CopiedShardCount = copied, ShardCount = total };

    private static TreeDeletionStatus Deleted() =>
        new() { TreeId = TreeId, IsDeleted = true, CanRecover = true };

    private static TreeDeletionStatus Purging(int purged, int total) =>
        new() { TreeId = TreeId, IsDeleted = true, PurgeInProgress = true, PurgedShardCount = purged, PurgeShardCount = total };

    private static TreeDeletionStatus Purged(int total) =>
        new() { TreeId = TreeId, IsDeleted = true, PurgeComplete = true, PurgedShardCount = total, PurgeShardCount = total, RecoveryDeadlineUtc = new DateTimeOffset(2026, 10, 5, 0, 0, 0, TimeSpan.Zero) };
}
