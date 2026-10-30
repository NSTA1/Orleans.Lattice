using System.Collections.Immutable;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public sealed class BarrierAbandonCallReductionTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task AbandonOriginBarriersAsync_uses_one_index_read_per_tree_and_one_conditional_call_per_unique_barrier(bool includeDecided)
    {
        var factory = Substitute.For<IGrainFactory>();
        var index = Substitute.For<ICrossTreeBarrierIndexGrain>();
        var receiver = Substitute.For<ILatticeCrossTreeReceiverGrain>();
        factory.GetGrain<ICrossTreeBarrierIndexGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(index);
        factory.GetGrain<ILatticeCrossTreeReceiverGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(receiver);
        index.GetAsync().Returns(ImmutableArray.Create("shared", "live"));
        index.GetAllAsync().Returns(ImmutableArray.Create("shared", "live", "dead"));
        receiver.AbandonForOriginAsync("peer", includeDecided).Returns(true);

        var count = await LatticeReplicationPeerDecommissioner.AbandonOriginBarriersAsync(
            factory, ["a", "b"], "peer", includeDecided, CancellationToken.None);

        Assert.That(count, Is.EqualTo(includeDecided ? 3 : 2));
        await index.Received(includeDecided ? 2 : 0).GetAllAsync();
        await index.Received(includeDecided ? 0 : 2).GetAsync();
        await index.DidNotReceive().GetTombstonesAsync();
        await receiver.Received(includeDecided ? 3 : 2).AbandonForOriginAsync("peer", includeDecided);
        await receiver.DidNotReceive().GetStatusAsync();
        await receiver.DidNotReceive().AbandonAsync();
    }

    [Test]
    public void AbandonOriginBarriersAsync_propagates_failed_index_reads_before_abandoning()
    {
        var factory = Substitute.For<IGrainFactory>();
        var index = Substitute.For<ICrossTreeBarrierIndexGrain>();
        factory.GetGrain<ICrossTreeBarrierIndexGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(index);
        index.GetAllAsync().Returns(Task.FromException<ImmutableArray<string>>(new InvalidOperationException("unavailable")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await LatticeReplicationPeerDecommissioner.AbandonOriginBarriersAsync(
                factory, ["a"], "peer", true, CancellationToken.None));
        factory.DidNotReceive().GetGrain<ILatticeCrossTreeReceiverGrain>(Arg.Any<string>(), Arg.Any<string?>());
    }
}
