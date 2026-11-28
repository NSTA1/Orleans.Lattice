using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A bulk append's catch-up of its rightmost leaf tries the leaf's consistent cut before it
/// replays (#6166); a WAL GC sweep's drive is unchanged.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TestCase(true)]
    [TestCase(false)]
    [NonParallelizable]
    public async Task CutAdvance_a_bulk_catch_up_takes_the_cut_head_and_a_gc_sweep_drive_does_not(bool bulkCatchUp)
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: TimerAdmissionCeiling, queued: 0);
        try
        {
            var rig = new LiveAdvanceRig();
            rig.FenceGate.IsPreArmOpen = true;
            rig.Shard.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new WalSplitCut(Head: 40, ForeignHigh: -1, UnstampedWatermark: -1)));
            rig.CursorReporter = Substitute.For<ILeafCursorReporter>();
            var state = new FakePersistentState<LeafNodeState>();
            state.State.ProjectionCheckpointOffset = CutAdvanceCheckpoint;
            state.State.ProjectionCheckpointOffsetAssigned = true;
            var grain = CreateGrain(
                state,
                options: new LatticeOptions { WalPartitions = 1 },
                liveAdvance: rig,
                configureGrainFactory: f => f.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(rig.Shard));
            await InitialiseOwnMarkerAsync(rig);
            await SetStampedAsync(grain, "a", "1");

            LeafStarvationDriveOutcome outcome;
            if (bulkCatchUp)
            {
                using (LatticeBulkCatchUpContext.BeginScope())
                {
                    outcome = await grain.DriveStarvedCheckpointAsync();
                }
            }
            else
            {
                outcome = await grain.DriveStarvedCheckpointAsync();
            }

            if (bulkCatchUp)
            {
                Assert.Multiple(() =>
                {
                    Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(39L), "the catch-up stands the leaf at the cut head");
                    Assert.That(outcome, Is.AnyOf(LeafStarvationDriveOutcome.Lifted, LeafStarvationDriveOutcome.NoAdvance));
                });
            }
            else
            {
                await rig.Shard.DidNotReceive().GetSplitCutAsync(
                    Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>());
            }

            Assert.That(LatticeBulkCatchUpContext.IsActive, Is.False, "the scope restores the context");
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
