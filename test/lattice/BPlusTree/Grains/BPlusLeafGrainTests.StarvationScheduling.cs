using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task Coverage_stall_timer_refreshes_its_phase_on_every_refused_or_deferred_opportunity()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var wal = new GrowingWal();
        var treeId = UniqueCoverageRepairTreeId("repeated-stall-rearm");
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1, treeId: treeId,
            maxConcurrentReplays: TimerAdmissionCeiling, replayCoordinator: wal.Coordinator);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        wal.GrowTo(5);
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var held = HoldTimerShare(gate);
        try
        {
            var opportunities = new List<int>();
            var reasons = new List<string>();
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
            {
                for (var tick = 0; tick < 64; tick++)
                {
                    reasons.Clear();
                    await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
                    if (reasons.Contains(LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value!))
                        opportunities.Add(tick);
                }
            }

            var intervals = opportunities.Zip(opportunities.Skip(1), (a, b) => b - a).ToArray();
            Assert.Multiple(() =>
            {
                Assert.That(intervals.Length, Is.GreaterThan(10), "exercise repeated opportunities, not first-arm jitter");
                Assert.That(intervals, Has.All.InRange(3, 5), "each rearm adds zero through two ticks");
                Assert.That(intervals.Distinct().Count(), Is.GreaterThan(1),
                    "a fixed phase per leaf is not a refreshed phase per opportunity");
                Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DriverDeclineRecheckNoDurableCheckpoint.Value),
                    "this detector must run the frozen-checkpoint route, not the never-checkpointed route");
            });
        }
        finally
        {
            ReleaseTimerShare(gate, held);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
