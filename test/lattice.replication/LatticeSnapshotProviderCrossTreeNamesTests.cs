using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Snapshot-export naming of cross-tree sub-sagas
/// (<see cref="LatticeSnapshotProvider.CrossTreeNames"/>) when the authoring
/// tree's transaction registry stores no cross-tree membership: the case of a
/// sub-saga parked before replication was enabled (#5073), or authored before
/// memberships existed (#4683).
/// </summary>
[TestFixture]
public class LatticeSnapshotProviderCrossTreeNamesTests
{
    private const string Tree = "names-tree";

    [Test]
    public async Task NameAsync_leaves_a_row_unchanged_when_its_saga_has_no_membership()
    {
        var txid = Guid.NewGuid();
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ITxRegistryGrain, IGrainBase>();
        ((IGrainBase)registry).GrainContext.GrainId.Returns(GrainId.Create("tx-registry", TxRegistryRouting.ShardKey(Tree, txid)));
        factory.GetGrain<ITxRegistryGrain>(TxRegistryRouting.ShardKey(Tree, txid)).Returns(registry);
        registry.GetCrossTreeMembershipsAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(new Dictionary<Guid, CrossTreeMembership>());
        var row = new SnapshotEntry { Key = "k", Value = [1], IsPrepared = true, TransactionId = txid };
        var names = new LatticeSnapshotProvider.CrossTreeNames(factory, Tree);

        var named = await names.NameAsync(row);
        var again = await names.NameAsync(row);

        Assert.Multiple(() =>
        {
            Assert.That(named, Is.EqualTo(row));
            Assert.That(named.CrossTreeOperationId, Is.Null);
            Assert.That(again, Is.EqualTo(row));
        });
        await registry.Received(1).GetCrossTreeMembershipsAsync(Arg.Any<IReadOnlyList<Guid>>());
    }
}
