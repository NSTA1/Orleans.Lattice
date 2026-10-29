using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class LatticeBootstrapCoordinatorGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Drain_combines_capture_and_foreign_reads_and_discards_once_per_leaf(bool failDiscard)
    {
        var (grain, state, factory, provider, _, _, hwm, _) = Create();
        Seed(state);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry
        {
            PhysicalTreeId = Tree,
            ShardMap = ShardMap.GetOrCreateDefaultShared(4096, 1),
        });
        var shard = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{Tree}/0").Returns(shard);
        var ids = new[] { GrainId.Create("leaf", "boot-first"), GrainId.Create("leaf", "boot-tail") };
        shard.GetLeftmostLeafIdAsync().Returns(ids[0]);
        var leaves = new IBPlusLeafGrain[ids.Length];
        var stale = new Guid[ids.Length][];
        for (var i = 0; i < ids.Length; i++)
        {
            var leaf = leaves[i] = Substitute.For<IBPlusLeafGrain>();
            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leaf);
            GrainId? next = i + 1 < ids.Length ? ids[i + 1] : null;
            leaf.GetBootstrapCaptureAsync(Arg.Any<VersionVector>()).Returns((
                new StateDelta
                {
                    Entries = new()
                    {
                        ["source-tombstone"] = LwwValue<byte[]>.Tombstone(Hlc(5)) with { OriginClusterId = SourceCluster },
                    },
                    Version = new VersionVector(),
                },
                new LeafLiveRawEntriesPage { Entries = [], NextSibling = next }));
            leaf.GetLiveRawEntriesAndNextSiblingAsync()
                .Returns(new LeafLiveRawEntriesPage { Entries = [], NextSibling = next });
            leaf.GetPendingMutationsForSlotsAndNextSiblingAsync(Arg.Any<int[]>(), Arg.Any<int>())
                .Returns(new LeafPendingMutationsPage { Mutations = [], NextSibling = next });
            stale[i] = [Guid.NewGuid(), Guid.NewGuid()];
            var pending = stale[i].Select(tx => new PendingMutationSnapshot
            {
                TransactionId = tx,
                Key = tx.ToString("N"),
                Value = [1],
                Timestamp = Hlc(10),
                OriginClusterId = "site-q",
            }).ToList();
            // A second key of the first saga must not cause another discard.
            pending.Add(pending[0] with { Key = "duplicate-saga-key" });
            leaf.GetLiveRawEntriesAndPendingAsync(Arg.Any<int[]>(), Arg.Any<int>())
                .Returns((new List<LwwEntry>(), new LeafPendingMutationsPage { Mutations = pending, NextSibling = next }));
        }

        var snapshot = new SnapshotStream(Tree, HybridLogicalClock.Zero, new VersionVector(), Stream())
        {
            OpenGeneration = StableGeneration,
            CloseGeneration = StableGeneration,
            OpenFrontier = ExportedFrontier(StableGeneration.Lineage),
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(snapshot);
        if (failDiscard)
            leaves[0].DiscardPendingTransactionsAndGetNextSiblingAsync(Arg.Any<Guid[]>())
                .Returns(Task.FromException<GrainId?>(new InvalidOperationException("discard failed")));

        if (failDiscard)
            Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextPhaseAsync());
        else
            await grain.ProcessNextPhaseAsync();

        Assert.That(state.State.HeldNoSourceRowsAtImportStart, Is.False,
            "a source tombstone in the delta still counts even though the live page is empty");
        for (var i = 0; i < leaves.Length; i++)
        {
            var leaf = leaves[i];
            await leaf.Received(1).GetBootstrapCaptureAsync(Arg.Any<VersionVector>());
            await leaf.DidNotReceive().GetDeltaSinceAsync(Arg.Any<VersionVector>());
            await leaf.Received(1).GetLiveRawEntriesAndPendingAsync(
                Arg.Is<int[]>(slots => slots.Length == 4096 && slots[0] == 0 && slots[4095] == 4095), 4096);
            await leaf.DidNotReceive().GetLiveRawEntriesAsync();
            await leaf.DidNotReceive().DiscardPendingTransactionAsync(Arg.Any<Guid>());
            if (!failDiscard || i == 0)
                await leaf.Received(1).DiscardPendingTransactionsAndGetNextSiblingAsync(
                    Arg.Is<Guid[]>(txs => txs.Length == 2 && txs.Contains(stale[i][0]) && txs.Contains(stale[i][1])));
            else
                await leaf.DidNotReceive().DiscardPendingTransactionsAndGetNextSiblingAsync(Arg.Any<Guid[]>());
        }
        if (failDiscard)
        {
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
            await hwm.DidNotReceive().FinalizeBootstrapFloorAsync(Arg.Any<CancellationToken>());
        }
        else
        {
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
            await hwm.Received(1).FinalizeBootstrapFloorAsync(Arg.Any<CancellationToken>());
        }
    }
}
