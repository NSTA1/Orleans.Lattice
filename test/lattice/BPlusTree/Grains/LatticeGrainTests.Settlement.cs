using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Replicated_aborted_prepare_uses_one_settlement_dispatch_without_staging(bool delete)
    {
        var (grain, factory) = CreateGrain(shardCount: 1);
        var registry = Substitute.For<ITxRegistryGrain>();
        var txid = Guid.NewGuid();
        registry.GetStatusForSettlementAsync(txid).Returns(TxStatus.Aborted);
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry);
        var fence = Substitute.For<ICopyReceiveFenceGrain>();
        fence.GetStatusAsync().Returns(new CopyReceiveFenceStatus());
        factory.GetGrain<ICopyReceiveFenceGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(fence);
        var shard = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(shard);

        if (delete)
            await grain.ApplyPreparedDeleteAsync("k", HybridLogicalClock.Zero, "origin", null, txid, 1, 0);
        else
            await grain.ApplyPreparedSetAsync("k", [1], HybridLogicalClock.Zero, "origin", null, 0, txid, 1, 0);

        await registry.Received(1).GetStatusForSettlementAsync(txid);
        await registry.DidNotReceive().GetStatusForTerminalAsync(txid);
        await registry.DidNotReceive().GetRecordedStatusAsync(txid);
        Assert.That(shard.ReceivedCalls(), Is.Empty, "An aborted prepare must not write or stage a value.");
    }
}
