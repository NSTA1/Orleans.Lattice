using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    [NonParallelizable]
    public async Task Coverage_stall_timer_only_drives_unscanned_partitions_that_hold_live_rows(bool laggingPartitionHasRows)
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var wal = new GrowingWal();
        var treeId = UniqueCoverageRepairTreeId("stall-live-partition");
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1, treeId: treeId, walPartitions: 2,
            maxConcurrentReplays: TimerAdmissionCeiling, replayCoordinator: wal.Coordinator);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        var keys = Enumerable.Range(0, 100).Select(i => $"partition-key-{i}").ToArray();
        SeedRow(grain, keys.First(key => WalPartitionHash.Compute(key, 2) == 1));
        if (laggingPartitionHasRows)
            SeedRow(grain, keys.First(key => WalPartitionHash.Compute(key, 2) == 0));
        using (LatticeApplyOffsetContext.BeginScope(0, 4))
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        using (LatticeApplyOffsetContext.BeginScope(1, 5))
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(5, CancellationToken.None);
        wal.GrowTo(5);
        Assert.That(wal.Head, Is.EqualTo(6));

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var held = HoldTimerShare(gate);
        try
        {
            var reasons = new List<string>();
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
            {
                for (var tick = 0; tick < 16; tick++)
                    await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            }
            Assert.That(reasons, Is.Not.Empty, "the real timer must run");
            Assert.That(reasons.Contains(TimerDriveRefusedReason), Is.EqualTo(laggingPartitionHasRows),
                "partition zero lags but must not justify a permit until it holds live rows");
            var laggingKey = keys.First(key => WalPartitionHash.Compute(key, 2) == 0);
            if (laggingPartitionHasRows)
                grain.EntriesForTest.Remove(laggingKey);
            else
                SeedRow(grain, laggingKey);
            reasons.Clear();
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
            {
                for (var tick = 0; tick < 32; tick++)
                    await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            }
            Assert.That(reasons.Contains(TimerDriveRefusedReason), Is.EqualTo(!laggingPartitionHasRows),
                "the cached partition buffer must reflect rows added or removed between ticks");
        }
        finally
        {
            ReleaseTimerShare(gate, held);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [TestCase(0L)]
    [TestCase(4L)]
    [NonParallelizable]
    public async Task Coverage_stall_timer_caught_up_leaf_never_requests_a_replay_permit(long checkpoint)
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var wal = new GrowingWal();
        var treeId = UniqueCoverageRepairTreeId("caught-up-stall");
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1, treeId: treeId,
            maxConcurrentReplays: TimerAdmissionCeiling, replayCoordinator: wal.Coordinator);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        wal.GrowTo(checkpoint);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(checkpoint, CancellationToken.None);
        Assert.That(wal.Head, Is.EqualTo(checkpoint + 1), "the head is exclusive, not the newest offset");
        wal.Coordinator.ClearReceivedCalls();

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var held = HoldTimerShare(gate);
        try
        {
            var reasons = new List<string>();
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
            {
                for (var tick = 0; tick < 32; tick++)
                    await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            }

            Assert.Multiple(() =>
            {
                Assert.That(reasons, Is.Not.Empty, "the real timer call site must execute");
                Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value));
                Assert.That(reasons, Does.Not.Contain(TimerDriveRefusedReason),
                    "the timer share is full: any permit request would be refused and counted");
            });
            await wal.Coordinator.DidNotReceive().ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());

            // A negative head decision must not latch out work appended after the probe.
            wal.GrowTo(checkpoint + 1);
            ReleaseTimerShare(gate, held);
            held = 0;
            for (var tick = 0; tick < 8; tick++)
                await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(checkpoint + 1),
                "new WAL work must still reach the timer's replay drive");
        }
        finally
        {
            ReleaseTimerShare(gate, held);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Coverage_stall_timer_refused_cohort_disperses_on_its_first_retry()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        const int leafCount = 24;
        var wal = new GrowingWal();
        var leaves = new BPlusLeafGrain[leafCount];
        var treeId = UniqueCoverageRepairTreeId("stall-cohort");
        for (var i = 0; i < leafCount; i++)
        {
            var (grain, _, _, _) = CreateLeafForCoverageRepair(
                persistedCheckpoint: -1, treeId: treeId,
                maxConcurrentReplays: TimerAdmissionCeiling, replayCoordinator: wal.Coordinator,
                replicaId: $"stall-cohort-{i}");
            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
            SeedRow(grain);
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
            leaves[i] = grain;
        }
        wal.GrowTo(5);
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var held = HoldTimerShare(gate);
        try
        {
            var retries = new int[leafCount];
            var refusals = new int[leafCount];
            var reasons = new List<string>();
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
            {
                for (var tick = 1; tick <= 16; tick++)
                {
                    for (var i = 0; i < leafCount; i++)
                    {
                        reasons.Clear();
                        await leaves[i].OnCoverageLagTimerTickAsync(CancellationToken.None);
                        if (!reasons.Contains(TimerDriveRefusedReason))
                            continue;
                        if (++refusals[i] == 1)
                            Assert.That(tick, Is.EqualTo(TicksToReachStalledDrive),
                                "precondition: the initial asks really form a refused cohort");
                        else if (refusals[i] == 2)
                            retries[i] = tick;
                    }
                }
            }
            Assert.Multiple(() =>
            {
                Assert.That(retries, Has.All.InRange(10, 14), "every leaf retries within the bounded re-arm");
                Assert.That(retries.Distinct().Count(), Is.GreaterThan(1),
                    "the first refusal backoff is one opportunity for every leaf; re-arm must break that lock-step");
            });
        }
        finally
        {
            ReleaseTimerShare(gate, held);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
