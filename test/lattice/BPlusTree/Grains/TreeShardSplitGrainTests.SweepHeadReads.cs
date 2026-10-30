using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeShardSplitGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task RetroactiveSweep_first_attempt_uses_only_one_head_probe(bool empty)
    {
        var (grain, _, source, _, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [], preCheckStatus: TxStatus.InFlight);
        if (empty) source.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));

        await grain.InitiateSplitStateAsync(0);

        await source.Received(1).GetLeftmostLeafIdAsync();
    }

    [Test]
    public async Task RetroactiveSweep_row_loss_retry_uses_the_new_source_head()
    {
        var factory = Substitute.For<IGrainFactory>();
        var snap = BuildSetSnapshot(Guid.NewGuid(), out var key);
        var (grain, _, source, target, retired, _) = CreateGrainWithSweepWiring(
            [snap], TxStatus.InFlight, factory: factory);
        var newHead = GrainId.Create("leaf", "replacement");
        var replacement = LeafSubstituteCombinedCalls.CreateComposed();
        replacement.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(new List<PendingMutationSnapshot> { snap });
        factory.GetGrain<IBPlusLeafGrain>(newHead).Returns(replacement);
        retired.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>())
            .Returns<Task<List<PendingMutationSnapshot>>>(_ =>
            {
                source.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(newHead));
                throw new LeafStateRowLostException("retired", TreeId, "concurrent fold", null);
            });

        await grain.InitiateSplitStateAsync(0);

        await source.Received(2).GetLeftmostLeafIdAsync();
        await retired.Received(1).GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>());
        await replacement.Received(1).GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>());
        await target.Received(1).SetAsync(key, Arg.Is<byte[]>(b => b.SequenceEqual(snap.Value!)));
    }

    [Test]
    public async Task RetroactiveSweep_persistent_row_loss_still_fails_after_five_fresh_attempts()
    {
        var (grain, _, source, _, leaf, _) = CreateGrainWithSweepWiring([], TxStatus.InFlight);
        leaf.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>())
            .Returns<Task<List<PendingMutationSnapshot>>>(_ =>
                throw new LeafStateRowLostException("lost", TreeId, "missing row", null));

        Assert.That(async () => await grain.InitiateSplitStateAsync(0),
            Throws.InstanceOf<LeafStateRowLostException>());
        await source.Received(5).GetLeftmostLeafIdAsync();
    }
}
