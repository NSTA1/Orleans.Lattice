using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The consistent cut's stamps persist once (#6166). After a bulk seed every partition's
/// gap is past <c>MaterialiserCheckpointEntries</c>, and each stamp persisted the leaf,
/// published its digest and reported its cursors on its own: one storage write per
/// partition on each activation and each stall drive.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task CutAdvance_stamping_every_partition_past_a_seed_gap_persists_the_leaf_once()
    {
        const int partitions = 4;
        var rig = new LiveAdvanceRig(partitions);
        rig.FenceGate.IsPreArmOpen = true;
        var head = CutAdvanceCheckpoint + 1 + (2 * LatticeOptions.DefaultMaterialiserCheckpointEntries);
        rig.Shard.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WalSplitCut(Head: head, ForeignHigh: -1, UnstampedWatermark: -1)));
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(
            state,
            liveAdvance: rig,
            configureGrainFactory: f => f.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(rig.Shard));
        await InitialiseOwnMarkerAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        for (var p = 0; p < partitions; p++)
        {
            using (LatticeApplyOffsetContext.BeginScope(p, CutAdvanceCheckpoint))
            {
                await ((ILeafProjection)grain).SetCheckpointOffsetAsync(CutAdvanceCheckpoint, CancellationToken.None);
            }
        }

        var writesBefore = state.WriteCount;

        var outcome = await grain.TryAdvanceCheckpointFromCutAsync(partitions);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(BPlusLeafGrain.CutAdvanceOutcome.Reached), "precondition: the cut stamped every partition");
            for (var p = 0; p < partitions; p++)
            {
                Assert.That(state.State.ProjectionCheckpointOffsetsByPartition![p], Is.EqualTo(head - 1), $"partition {p}'s stamp was not persisted");
            }

            Assert.That(state.WriteCount - writesBefore, Is.EqualTo(1), "the cut persisted the leaf once per stamped partition");
        });
    }
}
