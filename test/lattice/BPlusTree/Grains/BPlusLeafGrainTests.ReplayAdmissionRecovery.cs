using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase("empty")]
    [TestCase("stale")]
    [TestCase("depth_disabled")]
    [TestCase("time_disabled")]
    [TestCase("no_progress_below_bound")]
    [TestCase("healthy_below_bound")]
    [NonParallelizable]
    public async Task Replay_admission_preserves_recovery_and_disabled_paths(string scenario)
    {
        var options = new LatticeOptions();
        var queued = scenario == "empty" ? 0 : 1;
        await SeedAdmittedWaitersAsync(_ => queued);
        if (scenario is "stale" or "no_progress_below_bound" or "healthy_below_bound")
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                scenario == "stale" ? options.WalReplayPermitMaxQueueWait : TimeSpan.FromMilliseconds(2),
                scenario == "no_progress_below_bound"
                    ? options.WalReplayPermitMaxQueueWait + TimeSpan.FromSeconds(1) : TimeSpan.Zero,
                sinceLastSample: scenario == "stale"
                    ? options.WalReplayPermitMaxQueueWait + TimeSpan.FromSeconds(1) : TimeSpan.Zero);
        }

        var treeId = UniqueReplayPermitTree();
        var refusals = new ConcurrentBag<KeyValuePair<string, object?>[]>();
        try
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                null, 0, 0, configureOptions: o =>
                {
                    if (scenario == "depth_disabled")
                        o.WalReplayPermitQueueDepthPerPermit = 0;
                    if (scenario == "time_disabled")
                        o.WalReplayPermitMaxQueueWait = TimeSpan.Zero;
                });
            state.State.TreeId = treeId;
            using (ListenForSaturationRefusals(treeId, refusals))
                await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
            Assert.That(grain.ReplayAdmissionPhaseForTest,
                Is.EqualTo(BPlusLeafGrain.ReplayAdmissionPhase.HoldsPermit),
                "admission must reach the actual replay, not a no-op activation");
            Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(queued));
            Assert.That(refusals, Is.Empty);
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }
    }
}
