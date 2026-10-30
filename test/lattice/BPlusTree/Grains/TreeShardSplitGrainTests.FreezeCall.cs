using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeShardSplitGrainTests
{
    [Test]
    public async Task Swap_uses_one_freeze_call_before_drain_and_map_publication()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Swap;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [2, 4, 6];
        state.State.OriginalShardMap = ShardMap.CreateDefault(8, 2);
        source.SealMovedSlotsAndEnterRejectAsync(Arg.Any<int[]>(), Arg.Any<int>()).Returns(Task.CompletedTask);
        source.ClearReceivedCalls();

        await grain.SwapAsync();

        await source.Received(1).SealMovedSlotsAndEnterRejectAsync(
            Arg.Is<int[]>(slots => slots.SequenceEqual(new[] { 2, 4, 6 })), 8);
        await source.DidNotReceive().MarkLeavesMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>());
        await source.DidNotReceive().EnterRejectPhaseAsync();
        Received.InOrder(() =>
        {
            source.SealMovedSlotsAndEnterRejectAsync(Arg.Any<int[]>(), 8);
            source.GetLeftmostLeafIdAsync();
            registry.ReassignSlotsAsync(
                Arg.Any<string>(), Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<ShardMap>(), Arg.Any<string>());
        });
    }
}
