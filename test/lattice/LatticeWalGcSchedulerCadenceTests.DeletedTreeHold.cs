using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Issue #3930: the WAL GC must not heal a retention floor by touching the
/// leaves of a deleted physical tree. An undone resize's destination was only
/// soft-deleted, its leaves' never-checkpointed pins blocked its floor, and the
/// blocked-leaf sweep kept reactivating those leaves - replaying a log for a tree
/// nobody could read - for the whole soft-delete window. These tests arm the
/// exact shape that drove the sweep (a floor blocked by a named consumer, aged
/// past the minimum block age) and vary only what the deletion grain reports.
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private static ITreeDeletionGrain DeletionReporting(
        IGrainFactory factory, PhysicalTreeRetention retention, string treeId = StrandedTree)
    {
        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.GetPhysicalRetentionAsync().Returns(Task.FromResult(retention));
        factory.GetGrain<ITreeDeletionGrain>(treeId).Returns(deletion);
        return deletion;
    }

    private static ILatticeWalGc BlockedGc()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        return gc;
    }

    [Test]
    public async Task ExecuteAsync_does_not_reactivate_a_blocked_leaf_of_a_deleted_tree()
    {
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        var deletion = DeletionReporting(factory, PhysicalTreeRetention.Deleted);
        var reporter = Substitute.For<ILeafCursorReporter>();

        var scheduler = CreateScheduler(factory, BlockedGc(), Adaptive(), time, cursorReporter: reporter);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(10));

        await deletion.Received().GetPhysicalRetentionAsync();
        await deletion.Received().DiscardIfAbandonedDerivedCopyAsync();
        await leaf.DidNotReceive().DriveStarvedCheckpointAsync();
        factory.DidNotReceive().GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());

        // A deleted copy is still recoverable, so its pins keep protecting it.
        await reporter.DidNotReceive().UnregisterTreeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await reporter.DidNotReceive().UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_retires_the_pins_of_a_discarded_tree_without_touching_its_leaves()
    {
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        DeletionReporting(factory, PhysicalTreeRetention.Discarded);
        var reporter = Substitute.For<ILeafCursorReporter>();

        var scheduler = CreateScheduler(factory, BlockedGc(), Adaptive(), time, cursorReporter: reporter);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(10));

        await reporter.Received().UnregisterTreeAsync(StrandedTree, Arg.Any<CancellationToken>());
        await leaf.DidNotReceive().DriveStarvedCheckpointAsync();
        factory.DidNotReceive().GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_reports_a_deleted_trees_blocked_floor_once_per_episode_at_error_level()
    {
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);
        DeletionReporting(factory, PhysicalTreeRetention.Deleted);
        var logs = new RecordingLoggerFactory();

        var scheduler = CreateScheduler(
            factory, BlockedGc(), Adaptive(), time,
            logger: new Microsoft.Extensions.Logging.Logger<LatticeWalGcScheduler>(logs));
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(10));

        var holds = logs.Entries.Count(e =>
            e.Level == Microsoft.Extensions.Logging.LogLevel.Error
            && e.Message.Contains("physical tree that has been deleted", StringComparison.Ordinal));
        Assert.That(holds, Is.EqualTo(1), "a terminal hold must be reported, once, distinctly from a floor that is merely behind");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_discards_a_deleted_tree_that_is_an_abandoned_resize_copy()
    {
        // The self-heal for an estate a build predating the discard left
        // holding an undone resize's destination: the copy reports Deleted,
        // and its deletion grain confirms no resize can recover it.
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        var deletion = DeletionReporting(factory, PhysicalTreeRetention.Deleted);
        deletion.DiscardIfAbandonedDerivedCopyAsync().Returns(Task.FromResult(true));
        var logs = new RecordingLoggerFactory();

        var scheduler = CreateScheduler(
            factory, BlockedGc(), Adaptive(), time,
            logger: new Microsoft.Extensions.Logging.Logger<LatticeWalGcScheduler>(logs));
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));

        await deletion.Received().DiscardIfAbandonedDerivedCopyAsync();
        await leaf.DidNotReceive().DriveStarvedCheckpointAsync();
        Assert.That(
            logs.Entries.Any(e => e.Level == Microsoft.Extensions.Logging.LogLevel.Error),
            Is.False,
            "a copy the GC has just discarded is healing, not terminally held");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_still_reactivates_a_blocked_leaf_of_a_live_tree_when_the_probe_says_live()
    {
        // The control arm: the probe runs on exactly the passes that would reach
        // a remedy, and a live answer leaves the remedy exactly as it was.
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        var deletion = DeletionReporting(factory, PhysicalTreeRetention.Live);

        var scheduler = CreateScheduler(factory, BlockedGc(), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));

        await deletion.Received().GetPhysicalRetentionAsync();
        await leaf.Received().DriveStarvedCheckpointAsync();

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_treats_a_tree_as_live_when_the_retention_probe_faults()
    {
        // Fail open: a probe that cannot answer must not withhold a remedy the
        // tree would have received before the probe existed.
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.GetPhysicalRetentionAsync().ThrowsAsync(new TimeoutException("deletion grain unavailable"));
        factory.GetGrain<ITreeDeletionGrain>(StrandedTree).Returns(deletion);

        var scheduler = CreateScheduler(factory, BlockedGc(), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));

        await leaf.Received().DriveStarvedCheckpointAsync();

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_does_not_probe_the_retention_of_a_tree_that_reclaims()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 5)));
        var time = new VirtualTimeProvider();
        var factory = FactoryWithTrees(StrandedTree);
        var deletion = DeletionReporting(factory, PhysicalTreeRetention.Deleted);

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);

        await deletion.DidNotReceive().GetPhysicalRetentionAsync();

        await scheduler.StopAsync(CancellationToken.None);
    }
}
