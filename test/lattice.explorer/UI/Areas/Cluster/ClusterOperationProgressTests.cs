using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// How each tree operation's status becomes the progress the Cluster pages draw
/// (issue 3958): a known total becomes a determinate bar, an unknown one an
/// indeterminate bar with its phase, never an invented figure; a settled
/// operation draws nothing; an accepted undo is read before the resize flag.
/// </summary>
[TestFixture]
public sealed class ClusterOperationProgressTests
{
    private const string Tree = "a/crm/orders";

    [Test]
    public void A_copying_resize_counts_shards_then_the_steps_after_the_copy()
    {
        var progress = ClusterOperationProgress.Of(new TreeResizeStatus { TreeId = Tree, InProgress = true, Phase = TreeResizePhase.Copy, CompletedUnits = 3, TotalUnits = 11 });

        Assert.That(progress, Is.EqualTo(new ClusterOperationProgress("Resize progress", "Copying the tree at the new size", 3, 11, "3 of 8 shards copied, then 3 steps to finish.")));
    }

    [Test]
    [TestCase("Swap", 8, "Pointing the tree's name at the copy", "Copy complete. Step 1 of 3 to finish.")]
    [TestCase("RejectOldShards", 9, "Turning requests away from the old copy", "Copy complete. Step 2 of 3 to finish.")]
    [TestCase("RetireOldCopy", 10, "Retiring the old copy", "Copy complete. Step 3 of 3 to finish.")]
    public void A_resize_past_its_copy_names_its_step(string phase, int completed, string text, string detail)
    {
        var progress = ClusterOperationProgress.Of(new TreeResizeStatus { TreeId = Tree, InProgress = true, Phase = Enum.Parse<TreeResizePhase>(phase), CompletedUnits = completed, TotalUnits = 11 });

        Assert.That(progress, Is.EqualTo(new ClusterOperationProgress("Resize progress", text, completed, 11, detail)));
    }

    [Test]
    public void A_resize_from_a_build_that_reports_no_progress_is_indeterminate()
    {
        var progress = ClusterOperationProgress.Of(new TreeResizeStatus { TreeId = Tree, InProgress = true });

        Assert.That(progress, Is.EqualTo(new ClusterOperationProgress("Resize progress", "Resizing", 0, null, null)));
    }

    [Test]
    public void An_accepted_undo_is_read_before_the_resize_flag_even_when_the_resize_had_finished()
    {
        var progress = ClusterOperationProgress.Of(new TreeResizeStatus { TreeId = Tree, InProgress = false, UndoRequested = true });

        Assert.Multiple(() =>
        {
            Assert.That(progress?.Label, Is.EqualTo("Undo progress"));
            Assert.That(progress?.Phase, Is.EqualTo("Undoing the resize"));
            Assert.That(progress?.Maximum, Is.Null);
        });
    }

    [Test]
    public void Settled_operations_draw_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterOperationProgress.Of(new TreeResizeStatus { TreeId = Tree }), Is.Null);
            Assert.That(ClusterOperationProgress.Of(new TreeSnapshotStatus { TreeId = Tree }), Is.Null);
            Assert.That(ClusterOperationProgress.Of(new TreeReshardStatus { TreeId = Tree }), Is.Null);
            Assert.That(ClusterOperationProgress.Of(new TreeDeletionStatus { TreeId = Tree, IsDeleted = true }), Is.Null);
        });
    }

    [Test]
    [TestCase("LockSource", "Taking the source out of service")]
    [TestCase("BeginForwarding", "Starting to forward live writes")]
    [TestCase("Copy", "Copying shards")]
    [TestCase("UnlockSource", "Returning a copied shard to service")]
    public void A_snapshot_counts_the_shards_it_has_copied(string phase, string text)
    {
        var progress = ClusterOperationProgress.Of(new TreeSnapshotStatus { TreeId = Tree, InProgress = true, Phase = Enum.Parse<TreeSnapshotPhase>(phase), CopiedShardCount = 2, ShardCount = 5 });

        Assert.That(progress, Is.EqualTo(new ClusterOperationProgress("Snapshot progress", text, 2, 5, "2 of 5 shards copied.")));
    }

    [Test]
    public void A_snapshot_without_a_total_is_indeterminate()
    {
        var progress = ClusterOperationProgress.Of(new TreeSnapshotStatus { TreeId = Tree, InProgress = true });

        Assert.That(progress, Is.EqualTo(new ClusterOperationProgress("Snapshot progress", "Copying", 0, null, null)));
    }

    [Test]
    public void A_reshard_is_measured_from_where_it_started()
    {
        var progress = ClusterOperationProgress.Of(new TreeReshardStatus { TreeId = Tree, InProgress = true, CurrentPhysicalShardCount = 5, TargetShardCount = 8, StartPhysicalShardCount = 2 });

        Assert.That(progress, Is.EqualTo(new ClusterOperationProgress("Reshard progress", "Splitting shards", 3, 6, "5 of 8 physical shards.")));
    }

    [Test]
    public void A_reshard_without_a_recorded_start_is_indeterminate_but_names_its_target()
    {
        var progress = ClusterOperationProgress.Of(new TreeReshardStatus { TreeId = Tree, InProgress = true, CurrentPhysicalShardCount = 5, RequestedShardCount = 8 });

        Assert.That(progress, Is.EqualTo(new ClusterOperationProgress("Reshard progress", "Splitting shards", 0, null, "5 of 8 physical shards.")));
    }

    [Test]
    public void A_reshard_without_any_target_says_how_far_it_has_got()
    {
        var progress = ClusterOperationProgress.Of(new TreeReshardStatus { TreeId = Tree, InProgress = true, CurrentPhysicalShardCount = 5 });

        Assert.That(progress, Is.EqualTo(new ClusterOperationProgress("Reshard progress", "Splitting shards", 0, null, "5 physical shards so far.")));
    }

    [Test]
    public void A_reshard_that_overshot_its_target_is_clamped()
    {
        var progress = ClusterOperationProgress.Of(new TreeReshardStatus { TreeId = Tree, InProgress = true, CurrentPhysicalShardCount = 9, TargetShardCount = 8, StartPhysicalShardCount = 2 });

        Assert.That((progress?.Value, progress?.Maximum), Is.EqualTo(((long?)6, (long?)6)));
    }

    [Test]
    public void A_running_purge_counts_its_shards_and_a_finished_one_is_whole()
    {
        var running = ClusterOperationProgress.Of(new TreeDeletionStatus { TreeId = Tree, IsDeleted = true, PurgeInProgress = true, PurgedShardCount = 1, PurgeShardCount = 4 });
        var done = ClusterOperationProgress.Of(new TreeDeletionStatus { TreeId = Tree, IsDeleted = true, PurgeComplete = true, PurgedShardCount = 4, PurgeShardCount = 4 });
        var legacy = ClusterOperationProgress.Of(new TreeDeletionStatus { TreeId = Tree, IsDeleted = true, PurgeInProgress = true });

        Assert.Multiple(() =>
        {
            Assert.That(running, Is.EqualTo(new ClusterOperationProgress("Purge progress", "Purging shards", 1, 4, "1 of 4 shards purged.")));
            Assert.That(done, Is.EqualTo(new ClusterOperationProgress("Purge progress", "Purged", 4, 4, "4 of 4 shards purged.")));
            Assert.That(legacy?.Maximum, Is.Null, "a purge recorded by an earlier build has no shard count");
        });
    }
}
