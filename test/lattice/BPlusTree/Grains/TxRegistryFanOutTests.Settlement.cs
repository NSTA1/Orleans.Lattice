using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TxRegistryFanOutTests
{
    [Test]
    public async Task Settlement_batch_preserves_owning_registry_groups_without_scalar_dispatches()
    {
        var a = IdOnShard(0);
        var b = IdOnShard(0);
        var c = IdOnShard(2);
        var legacy = Guid.NewGuid();
        Shard(0).GetStatusManyForSettlementAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(new Dictionary<Guid, TxStatus> { [a] = TxStatus.Committed, [b] = TxStatus.Aborted });
        Shard(2).GetStatusManyForSettlementAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(new Dictionary<Guid, TxStatus> { [c] = TxStatus.InFlight });
        Legacy.GetStatusManyForSettlementAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(new Dictionary<Guid, TxStatus> { [legacy] = TxStatus.Committed });

        var answer = await TxRegistryFanOut.GetStatusManyForSettlementAsync(_factory, TreeId, [a, c, legacy, b]);

        Assert.That(answer, Is.EquivalentTo(new Dictionary<Guid, TxStatus>
        {
            [a] = TxStatus.Committed, [b] = TxStatus.Aborted, [c] = TxStatus.InFlight, [legacy] = TxStatus.Committed,
        }));
        await Shard(0).Received(1).GetStatusManyForSettlementAsync(
            Arg.Is<IReadOnlyList<Guid>>(ids => ids.Count == 2 && ids.Contains(a) && ids.Contains(b)));
        await Shard(2).Received(1).GetStatusManyForSettlementAsync(
            Arg.Is<IReadOnlyList<Guid>>(ids => ids.Count == 1 && ids[0] == c));
        await Legacy.Received(1).GetStatusManyForSettlementAsync(
            Arg.Is<IReadOnlyList<Guid>>(ids => ids.Count == 1 && ids[0] == legacy));
        await Shard(1).DidNotReceive().GetStatusManyForSettlementAsync(Arg.Any<IReadOnlyList<Guid>>());
        foreach (var registry in _registries.Values)
        {
            await registry.DidNotReceive().GetStatusManyForTerminalAsync(Arg.Any<IReadOnlyList<Guid>>());
            await registry.DidNotReceive().GetRecordedStatusAsync(Arg.Any<Guid>());
        }
    }

    [Test]
    public void Settlement_batch_propagates_one_owning_registry_failure_without_returning_partial_answers()
    {
        var a = IdOnShard(0);
        var b = IdOnShard(2);
        Shard(0).GetStatusManyForSettlementAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(new Dictionary<Guid, TxStatus> { [a] = TxStatus.Committed });
        Shard(2).GetStatusManyForSettlementAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(Task.FromException<Dictionary<Guid, TxStatus>>(new InvalidOperationException("registry unavailable")));

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            TxRegistryFanOut.GetStatusManyForSettlementAsync(_factory, TreeId, [a, b]));
    }

    [Test]
    public async Task Settlement_batch_with_no_ids_dispatches_nothing()
    {
        Assert.That(await TxRegistryFanOut.GetStatusManyForSettlementAsync(_factory, TreeId, []), Is.Empty);
        foreach (var registry in _registries.Values)
            await registry.DidNotReceive().GetStatusManyForSettlementAsync(Arg.Any<IReadOnlyList<Guid>>());
    }
}
