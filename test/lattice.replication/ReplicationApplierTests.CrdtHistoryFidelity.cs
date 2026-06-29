using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression coverage for the cross-cluster CRDT history-fidelity fix: a
/// steady-state delta-carrying replicated CRDT entry that falls onto the
/// per-entry applier path must be recorded as a <c>CrdtDelta</c> (member
/// diff + origin) through <see cref="ILattice.ApplyCrdtDeltaAsync"/>,
/// not flattened to a full-value <see cref="MutationKind.Set"/> via a
/// read-merge-write fold. Bootstrap committed-projection rows (no Delta)
/// keep their full-state merge.
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyAsync_per_entry_crdt_delta_records_crdt_delta_not_full_state_set()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier();
        var ts = Hlc(33, 2);
        var entry = SetEntry("k", ts) with
        {
            Mode = LatticeMergeMode.PnCounter,
            Value = null,
            Delta = EncodePnCounterDelta(d => d["site-b"] = 7),
            OriginClusterId = RemoteCluster,
        };

        await applier.ApplyAsync(entry);

        await lattice.Received(1).ApplyCrdtDeltaAsync(
            "k", LatticeMergeMode.PnCounter, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
        await apply.DidNotReceiveWithAnyArgs().ApplyCrdtDeltaManyAsync(default!);
        await lattice.DidNotReceiveWithAnyArgs().SetIfVersionAsync(default!, default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_bootstrap_full_state_set_stays_state_merge_not_crdt_delta()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier();
        // Bootstrap row: full state in Value, no per-delta shape.
        var entry = SetEntry("k", Hlc(5)) with
        {
            Mode = LatticeMergeMode.OrSet,
            Value = EncodeOrSet(s => s.Add(OrSetMember, "site-b", 1)),
            Delta = null,
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await lattice.DidNotReceiveWithAnyArgs().ApplyCrdtDeltaAsync(default!, default, default!, default);
        await lattice.Received(1).SetIfVersionAsync(
            "k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }
}
