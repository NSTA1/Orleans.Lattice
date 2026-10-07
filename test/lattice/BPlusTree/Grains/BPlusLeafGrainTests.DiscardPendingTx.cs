using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for durable receiver-side discard of poisoned saga
/// pending buckets.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task DiscardPendingTransactionAsync_reactivation_does_not_rebuild_discarded_prepare_without_registry_decision()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var projection = AsProjection(grain);
        var txid = Guid.NewGuid();
        var prepared = BuildPreparedSet(txid, "poisoned", Encoding.UTF8.GetBytes("pv"), treeId: "discard-tree");

        using (LatticeApplyOffsetContext.BeginScope(partition: 0, offset: 5))
        {
            projection.Apply(prepared);
        }

        Assert.That(await grain.GetPendingKeysAsync(), Is.EqualTo(new[] { "poisoned" }),
            "precondition: the replayed prepare staged a pending bucket");

        await grain.DiscardPendingTransactionAsync(txid);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetPendingKeysAsync(), Is.Empty,
                "discard must clear the live activation's bucket");
            Assert.That(state.State.DiscardedSagaPrepares?.Select(e => e.TransactionId),
                Does.Contain(txid),
                "discard must persist a replay backstop because leaf snapshots do not carry pending buckets");
        });

        var reactivated = CreateGrain(state, replicaId: "test-leaf-reactivated");
        var reactivatedProjection = AsProjection(reactivated);
        using (LatticeApplyOffsetContext.BeginScope(partition: 0, offset: 5))
        {
            reactivatedProjection.Apply(prepared);
            await reactivatedProjection.SetCheckpointOffsetAsync(5, CancellationToken.None);
        }
        await reactivatedProjection.FlushCheckpointAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await reactivated.GetPendingKeysAsync(), Is.Empty,
                "reactivation replay must skip the discarded prepare even without a receiver registry decision");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5L),
                "the freed prepare clamp must allow the partition checkpoint to pass the discarded prepare");
            Assert.That(state.State.DiscardedSagaPrepares, Is.Null,
                "once the durable checkpoint covers the discarded prepare, the per-leaf marker is pruned");
        });
    }

    [Test]
    public async Task DiscardPendingTransactionAsync_refuses_a_routed_prepare_of_the_discarded_saga_that_arrives_later()
    {
        // Issue #4689: a saga discards its prepares on a copy it re-bound away
        // from before it decides. A routed prepare of that saga still on the wire
        // must not recreate the bucket after the discard, or the copy would hold
        // part of a committed batch for a revert to serve.
        var grain = CreateGrain(new FakePersistentState<LeafNodeState>());
        await grain.SetTreeIdAsync("discard-tree");
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "k1", Encoding.UTF8.GetBytes("v1"));

        await grain.DiscardPendingTransactionAsync(txid);
        await PreparePendingSetAsync(grain, txid, "k2", Encoding.UTF8.GetBytes("v2"));

        Assert.That(await grain.GetPendingKeysAsync(), Is.Empty,
            "a prepare of a saga this leaf discarded is refused, as a prepare behind a terminal is");
    }
}
