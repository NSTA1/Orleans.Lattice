using Azure;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Planner and helper coverage for reconciliation running beside a
/// concurrent manifest writer (#3348): committed batches discovered
/// above TAIL, the lost-race classifier, and the per-shard write
/// tracker reconciliation drains before it scans.
/// </summary>
public partial class AzureTableWalStorageProviderTests
{
    private static AzureTableWalStorageProvider.OrphanBatch Committed(long startOffset, long endOffsetInclusive) =>
        new(startOffset, endOffsetInclusive, $"_b_|t|0|S{startOffset:D19}", HasCandidateRow: false, AlreadyCommitted: true);

    [Test]
    public void PlanReconciliation_committed_batch_above_tail_rolls_forward_and_advances_tail()
    {
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: 9L,
            orphansAscending: new[] { Committed(10L, 14L) });

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(14L));
            Assert.That(plan.RollForward.Select(o => o.StartOffset), Is.EqualTo(new[] { 10L }));
            Assert.That(plan.RollBack, Is.Empty);
        });
    }

    [Test]
    public void PlanReconciliation_committed_batch_above_a_gap_is_never_rolled_back()
    {
        // A manifest row is authoritative even across a hole below it:
        // rolling it back would strand the M-row over deleted entries.
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: 9L,
            orphansAscending: new[] { Committed(20L, 24L) });

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(24L));
            Assert.That(plan.RollForward.Select(o => o.StartOffset), Is.EqualTo(new[] { 20L }));
            Assert.That(plan.RollBack, Is.Empty);
        });
    }

    [Test]
    public void PlanReconciliation_committed_batch_reanchors_contiguity_for_later_orphans()
    {
        // [15,19] is an uncommitted orphan above a gap, so it rolls
        // back; the committed [20,24] re-anchors contiguity, so the
        // uncommitted [25,29] after it rolls forward.
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: 9L,
            orphansAscending: new[] { Orphan(15L, 19L), Committed(20L, 24L), Orphan(25L, 29L) });

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(29L));
            Assert.That(plan.RollForward.Select(o => o.StartOffset), Is.EqualTo(new[] { 20L, 25L }));
            Assert.That(plan.RollBack.Select(o => o.StartOffset), Is.EqualTo(new[] { 15L }));
        });
    }

    [Test]
    public void PlanReconciliation_committed_batch_below_tail_never_lowers_tail()
    {
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: 30L,
            orphansAscending: new[] { Committed(10L, 14L) });

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(30L));
            Assert.That(plan.RollBack, Is.Empty);
        });
    }

    [TestCase(409, true)]
    [TestCase(412, true)]
    [TestCase(404, false)]
    [TestCase(500, false)]
    public void IsConcurrentManifestConflict_classifies_lost_races_only(int status, bool expected)
    {
        var ex = new RequestFailedException(status, "conflict");

        Assert.That(AzureTableWalStorageProvider.IsConcurrentManifestConflict(ex), Is.EqualTo(expected));
    }

    [Test]
    public void WalShardWriteTracker_is_idle_when_nothing_entered()
    {
        var tracker = new WalShardWriteTracker();

        Assert.Multiple(() =>
        {
            Assert.That(tracker.Active, Is.Zero);
            Assert.That(tracker.WhenIdleAsync().IsCompleted, Is.True);
        });
    }

    [Test]
    public async Task WalShardWriteTracker_releases_waiter_only_when_last_write_exits()
    {
        var tracker = new WalShardWriteTracker();
        tracker.Enter();
        tracker.Enter();

        var idle = tracker.WhenIdleAsync();
        tracker.Exit();
        Assert.That(idle.IsCompleted, Is.False, "one write is still in motion");

        tracker.Exit();
        await idle.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(tracker.Active, Is.Zero);
    }

    [Test]
    public void WalShardWriteTracker_unmatched_exit_does_not_go_negative()
    {
        var tracker = new WalShardWriteTracker();
        tracker.Exit();
        tracker.Enter();

        Assert.That(tracker.Active, Is.EqualTo(1));
        tracker.Exit();
        Assert.That(tracker.WhenIdleAsync().IsCompleted, Is.True);
    }

    [Test]
    public void WalShardActivity_exposes_idle_tracker_and_free_reconcile_gate()
    {
        var activity = new WalShardActivity();

        Assert.Multiple(() =>
        {
            Assert.That(activity.Writes.Active, Is.Zero);
            Assert.That(activity.ReconcileGate.CurrentCount, Is.EqualTo(1));
        });
    }
}