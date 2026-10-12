using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The legacy chain walk compacts a leaf and reads its chain link in one grain
/// call (<see cref="IBPlusLeafGrain.CompactTombstonesAndNextSiblingAsync"/>), so
/// each visited leaf costs one call rather than a compaction call plus a
/// separate <see cref="IBPlusLeafGrain.GetKeyRangeAndNextSiblingAsync"/>.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    [Test]
    public async Task The_chain_walk_compacts_and_advances_each_leaf_in_one_grain_call()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        var leafIds = new[]
        {
            GrainId.Create("leaf", "combined-0"),
            GrainId.Create("leaf", "combined-1"),
            GrainId.Create("leaf", "combined-2"),
        };
        SetupShardWithLeaves(grainFactory, 0, leafIds);

        // Replace the rig's composed leaves with bare substitutes that answer
        // only the combined call, so any separate link or compaction call is
        // visible as a received call.
        var leaves = new IBPlusLeafGrain[leafIds.Length];
        for (var i = 0; i < leafIds.Length; i++)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leaf);
            leaf.GetTreeIdAsync().Returns(Task.FromResult<string?>(TreeId));
            leaf.CompactTombstonesAndNextSiblingAsync(Arg.Any<TimeSpan>(), Arg.Any<HybridLogicalClock?>())
                .Returns(Task.FromResult(new LeafCompactionPage
                {
                    Result = LeafCompactionResult.Complete(0),
                    NextSibling = i + 1 < leafIds.Length ? leafIds[i + 1] : null,
                }));
            leaves[i] = leaf;
        }

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        foreach (var leaf in leaves)
        {
            await leaf.Received(1).CompactTombstonesAndNextSiblingAsync(Arg.Any<TimeSpan>(), Arg.Any<HybridLogicalClock?>());
            await leaf.DidNotReceive().GetKeyRangeAndNextSiblingAsync();
            await leaf.DidNotReceive().GetNextSiblingAsync();
            await leaf.DidNotReceive().CompactTombstonesAsync(Arg.Any<TimeSpan>());
            await leaf.DidNotReceive().CompactTombstonesBelowAsync(Arg.Any<TimeSpan>(), Arg.Any<HybridLogicalClock>());
        }
    }
}
