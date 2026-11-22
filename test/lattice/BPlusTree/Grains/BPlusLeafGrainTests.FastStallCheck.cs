using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6145. The fast stall check runs between full coverage-lag passes on
/// <see cref="LatticeOptions.LeafCheckpointStallCheckIntervalSeconds"/>, so a
/// materialiser consumer frozen over a quiet source is driven within seconds
/// rather than fifteen minutes - and it must do so without a single grain call,
/// because it runs on every leaf in the estate.
/// <para>
/// The dispatched drive is marked pending in these fixtures so the drive itself
/// (which does make grain calls, and has its own fixtures) stays out of the
/// count: what is asserted is the decision, and the decision's cost.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private async Task<(BPlusLeafGrain Grain, GrowingWal Wal, string TreeId)> FrozenEmptyLeafForFastCheckAsync(string name)
    {
        var treeId = UniqueCoverageRepairTreeId(name);
        var wal = new GrowingWal();
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L, treeId: treeId, replayCoordinator: wal.Coordinator);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        grain.SetInterleavedTimerDrivePendingForTest(true);
        wal.Coordinator.ClearReceivedCalls();
        return (grain, wal, treeId);
    }

    private static async Task<int> FastTicksAsync(BPlusLeafGrain grain, string treeId, int ticks)
    {
        var reasons = new List<string>();
        using (ListenForDriverDeclinesOnTree(treeId, reasons))
        {
            for (var tick = 0; tick < ticks; tick++)
                await grain.OnCheckpointStallFastTickAsync(CancellationToken.None);
        }

        var stalled = (string)LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value!;
        return reasons.Count(r => string.Equals(r, stalled, StringComparison.Ordinal));
    }

    [Test]
    [NonParallelizable]
    public async Task Fast_stall_check_drives_a_frozen_empty_leaf_on_local_evidence_without_a_grain_call()
    {
        var (grain, wal, treeId) = await FrozenEmptyLeafForFastCheckAsync("fast-stall-drive");
        try
        {
            WalCommitLogWriter.RecordAppend(treeId, 0, 5, DateTime.UtcNow.Ticks);

            var stalled = await FastTicksAsync(grain, treeId, TicksToReachStalledDrive);

            Assert.Multiple(() =>
            {
                Assert.That(stalled, Is.EqualTo(1),
                    "a sibling append beyond the frozen claim must drive the leaf on the fast cadence");
                Assert.That(wal.Coordinator.ReceivedCalls(), Is.Empty,
                    "the fast decision must cost no grain call: it runs on every leaf every few seconds");
            });

            Assert.That(await FastTicksAsync(grain, treeId, TicksToReachStalledDrive * 3), Is.Zero,
                "evidence already acted on must not repeat a drive until the checkpoint moves; "
                + "the full pass's coordinator probe decides from there");

            // A replay advance (not a live advance) re-arms the fast check.
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(5, CancellationToken.None);
            WalCommitLogWriter.RecordAppend(treeId, 0, 6, DateTime.UtcNow.Ticks);
            Assert.That(await FastTicksAsync(grain, treeId, TicksToReachStalledDrive), Is.EqualTo(1),
                "progress clears the latch, so a fresh freeze is driven again");
        }
        finally
        {
            WalCommitLogWriter._walAppendTimes.TryRemove(treeId, out _);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Fast_stall_check_never_drives_a_leaf_the_local_timeline_shows_caught_up()
    {
        var (grain, wal, treeId) = await FrozenEmptyLeafForFastCheckAsync("fast-stall-caught-up");
        try
        {
            WalCommitLogWriter.RecordAppend(treeId, 0, 4, DateTime.UtcNow.Ticks);

            Assert.That(await FastTicksAsync(grain, treeId, TicksToReachStalledDrive * 3), Is.Zero,
                "the newest acknowledged append is the checkpoint itself: nothing to drain");

            // The discriminating control: one later append, and the same leaf is driven. The
            // declining ticks above re-armed with up to two ticks of jitter, hence the margin.
            WalCommitLogWriter.RecordAppend(treeId, 0, 5, DateTime.UtcNow.Ticks);
            Assert.That(await FastTicksAsync(grain, treeId, TicksToReachStalledDrive + 2), Is.EqualTo(1),
                "the negative above must be the predicate declining, not the fast check never running");
            Assert.That(wal.Coordinator.ReceivedCalls(), Is.Empty);
        }
        finally
        {
            WalCommitLogWriter._walAppendTimes.TryRemove(treeId, out _);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Fast_stall_check_never_drives_a_live_advanced_partition()
    {
        var (grain, _, treeId) = await LiveAdvancedResidentLeafAsync("fast-stall-live-advanced");
        try
        {
            grain.SetInterleavedTimerDrivePendingForTest(true);
            for (var offset = 3; offset <= 5; offset++)
                WalCommitLogWriter.RecordAppend(treeId, 0, offset, DateTime.UtcNow.Ticks);

            Assert.That(await FastTicksAsync(grain, treeId, TicksToReachStalledDrive * 3), Is.Zero,
                "a live-advanced partition withdrew its drain claim (#5981); the full pass owns it");
        }
        finally
        {
            WalCommitLogWriter._walAppendTimes.TryRemove(treeId, out _);
        }
    }

    [Test]
    public async Task Coverage_lag_timer_runs_the_full_pass_on_its_first_fire_and_every_nth_after()
    {
        var treeId = UniqueCoverageRepairTreeId("fast-stall-cadence");
        var (grain, _, _, _) = CreateLeafForCoverageRepair(persistedCheckpoint: -1L, treeId: treeId);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        grain.SetCoverageLagFullPassEveryForTest(3);

        var fullPasses = new List<int>();
        for (var fire = 0; fire < 7; fire++)
        {
            var reasons = new List<string>();
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
                await grain.OnCoverageLagTimerFireAsync(CancellationToken.None);
            // The full pass always records a recheck arm; a declining fast check records nothing.
            if (reasons.Count > 0)
                fullPasses.Add(fire);
        }

        Assert.That(fullPasses, Is.EqualTo(new[] { 0, 3, 6 }));
    }

    [TestCase(300, 10, 10, 30)]
    [TestCase(300, 7, 7, 43)]
    [TestCase(300, 0, 300, 1)]
    [TestCase(300, 300, 300, 1)]
    [TestCase(300, 600, 300, 1)]
    [TestCase(5, 10, 5, 1)]
    public void Coverage_lag_timer_period_and_full_pass_ratio_keep_the_coverage_bound(
        int lagSeconds, int stallSeconds, int expectedPeriodSeconds, int expectedEvery)
    {
        var period = BPlusLeafGrain.ComputeCoverageLagTimerPeriod(lagSeconds, stallSeconds);
        Assert.Multiple(() =>
        {
            Assert.That(period, Is.EqualTo(TimeSpan.FromSeconds(expectedPeriodSeconds)));
            Assert.That(BPlusLeafGrain.ComputeCoverageLagFullPassEvery(lagSeconds, period), Is.EqualTo(expectedEvery));
        });
    }
}
