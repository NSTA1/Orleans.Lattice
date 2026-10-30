using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeShardConsolidationGrainTests
{
    [Test]
    public async Task Swap_uses_one_freeze_call_before_drain_and_map_publication()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Swap));
        h.Donor.SealMovedSlotsAndEnterRejectAsync(Arg.Any<int[]>(), Arg.Any<int>()).Returns(Task.CompletedTask);
        h.Donor.ClearReceivedCalls();

        await h.Grain.SwapAsync();

        await h.Donor.Received(1).SealMovedSlotsAndEnterRejectAsync(
            Arg.Is<int[]>(slots => slots.SequenceEqual(h.State.State.DonorSlots)), VirtualShardCount);
        await h.Donor.DidNotReceive().MarkLeavesMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>());
        await h.Donor.DidNotReceive().EnterRejectPhaseAsync();
        Received.InOrder(() =>
        {
            h.Donor.SealMovedSlotsAndEnterRejectAsync(Arg.Any<int[]>(), VirtualShardCount);
            h.Donor.GetLeftmostLeafIdAsync();
            h.Registry.ReassignSlotsAsync(
                Arg.Any<string>(), Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<ShardMap>(), Arg.Any<string>());
        });
    }
}
