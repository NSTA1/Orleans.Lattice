using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5125: a fold carries every saga prepare already on the donor's
/// folding slots onto the survivor before its drain may start, as a split does,
/// so the drain never imports a pre-saga value the survivor cannot gate.
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    [Test]
    public async Task Initiate_replays_a_prepare_already_on_the_donor_onto_the_survivor_before_the_drain()
    {
        var h = CreateGrain();
        var txid = Guid.NewGuid();
        var snapshot = WireDonorPrepare(h, txid);

        await h.Grain.InitiateConsolidationStateAsync(1, 0);

        await h.Survivor.Received(1).SetAsync(snapshot.Key, Arg.Is<byte[]>(v => v.SequenceEqual(snapshot.Value!)));
        await h.Survivor.Received(1).MarkSagaShadowAsync(
            txid, Arg.Is<IReadOnlyList<string>>(keys => keys.Count == 1 && keys[0] == snapshot.Key));
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Drain));
    }

    [Test]
    public async Task ReopenShadowWriteAsync_re_runs_the_prepared_sweep_before_the_drain()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.BeginShadowWrite));
        var txid = Guid.NewGuid();
        var snapshot = WireDonorPrepare(h, txid);

        await h.Grain.ReopenShadowWriteAsync();

        await h.Survivor.Received(1).SetAsync(snapshot.Key, Arg.Is<byte[]>(v => v.SequenceEqual(snapshot.Value!)));
        await h.Survivor.Received(1).MarkSagaShadowAsync(
            txid, Arg.Is<IReadOnlyList<string>>(keys => keys.Count == 1 && keys[0] == snapshot.Key));
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Drain));
    }

    [Test]
    public async Task Initiate_stays_in_BeginShadowWrite_when_the_prepared_sweep_fails()
    {
        var h = CreateGrain();
        var leaf = WireDonorLeaf(h);
        leaf.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(Task.FromException<List<PendingMutationSnapshot>>(new InvalidOperationException("sweep fault")));

        await h.Grain.InitiateConsolidationStateAsync(1, 0);

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.InProgress, Is.True, "the fold stays in flight for recovery to resume");
            Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.BeginShadowWrite),
                "the drain must not start before the sweep has run");
            Assert.That(h.Log.Entries, Does.Contain("donor.BeginSplit"));
            Assert.That(h.Log.Entries, Does.Not.Contain("survivor.MergeMany"));
        });
    }

    /// <summary>
    /// Gives the donor a one-leaf chain whose bucket holds an in-flight prepare
    /// of <paramref name="txid"/>, and a registry that reports the saga in flight.
    /// </summary>
    private static PendingMutationSnapshot WireDonorPrepare(Harness h, Guid txid)
    {
        var snapshot = new PendingMutationSnapshot
        {
            TransactionId = txid,
            Key = "prepared-key",
            Value = [1, 2, 3],
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            IsTombstone = false,
            ExpiresAtTicks = 0,
            OriginClusterId = null,
            VectorClock = null,
        };

        var leaf = WireDonorLeaf(h);
        leaf.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(Task.FromResult(new List<PendingMutationSnapshot> { snapshot }));

        var txRegistry = Substitute.For<ITxRegistryGrain>();
        txRegistry.GetStatusForTerminalAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.InFlight));
        txRegistry.GetStatusForSettlementAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.InFlight));
        txRegistry.GetStatusManyForTerminalAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(ci => Task.FromResult(((IReadOnlyList<Guid>)ci[0]).ToDictionary(id => id, _ => TxStatus.InFlight)));
        h.Factory.GetGrain<ITxRegistryGrain>(TreeId).Returns(txRegistry);
        return snapshot;
    }

    /// <summary>Points the donor's leftmost leaf at a single leaf with no sibling.</summary>
    private static IBPlusLeafGrain WireDonorLeaf(Harness h)
    {
        var leafId = GrainId.Create("leaf", "donor-prepared-leaf");
        var leaf = LeafSubstituteCombinedCalls.CreateComposed();
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        h.Donor.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));
        h.Factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);
        return leaf;
    }
}
