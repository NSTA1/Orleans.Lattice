using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A stall drive publishes the durable pin at most once per coverage-lag period (#6166).
/// During an ingest every resident leaf is driven every few stall ticks, and each drive's
/// persist mirrored the pin to the tree's few shared pin grains, where a bulk seed's birth
/// pins - written through - queued behind them.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task CutAdvance_a_second_stall_drive_inside_the_coverage_lag_period_does_not_mirror_the_pin()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: TimerAdmissionCeiling, queued: 0);
        try
        {
            var rig = new LiveAdvanceRig();
            rig.FenceGate.IsPreArmOpen = true;
            rig.Shard.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new WalSplitCut(Head: 40, ForeignHigh: -1, UnstampedWatermark: -1)));
            var reporter = Substitute.For<ILeafCursorReporter>();
            rig.CursorReporter = reporter;
            var state = new FakePersistentState<LeafNodeState>();
            state.State.ProjectionCheckpointOffset = CutAdvanceCheckpoint;
            state.State.ProjectionCheckpointOffsetAssigned = true;

            // Every advance persists, so each drive's cut persists and reaches the pin mirror.
            var grain = CreateGrain(
                state,
                options: new LatticeOptions { WalPartitions = 1, MaterialiserCheckpointInterval = TimeSpan.Zero },
                liveAdvance: rig,
                configureGrainFactory: f => f.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(rig.Shard));
            await InitialiseOwnMarkerAsync(rig);
            await SetStampedAsync(grain, "a", "1");

            await grain.DispatchTimerDriveAsync(1, "arm=test", cutFirst: true);
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(39L), "precondition: the first drive took the cut head");
            Assert.That(reporter.ReceivedCalls().Count(c => c.GetMethodInfo().Name is nameof(ILeafCursorReporter.FlushDurableMaterialiserFrontierAsync) or nameof(ILeafCursorReporter.NoteDurableMaterialiserFrontier)), Is.GreaterThan(0),
                "precondition: the first drive published the pin");

            reporter.ClearReceivedCalls();
            rig.Shard.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new WalSplitCut(Head: 80, ForeignHigh: -1, UnstampedWatermark: -1)));
            await grain.DispatchTimerDriveAsync(1, "arm=test", cutFirst: true);

            Assert.Multiple(() =>
            {
                Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(79L), "precondition: the second drive persisted the cut head");
                reporter.DidNotReceive().NoteDurableMaterialiserFrontier(
                    Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<long>());
                reporter.DidNotReceive().FlushDurableMaterialiserFrontierAsync(
                    Arg.Any<string>(), Arg.Any<IReadOnlyList<MaterialiserPinReport>>(), Arg.Any<CancellationToken>());
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
