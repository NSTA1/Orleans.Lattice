using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4692, the de-enrolment residual: a participating tree that stops being
/// replicated here before its own terminal arrives has that terminal dropped at
/// the enrollment gate, so it never arrives. The barrier then stops waiting for
/// it and decides on the trees that remain, and the dropped tree's pending
/// bucket of the sub-saga is discarded. A terminal dropped or deferred for any
/// other reason must not decide the barrier.
/// </summary>
public partial class ReceiverSagaDeferralIntegrationTests
{
    private async Task<int> CountPendingAsync(string tree, Guid txid)
    {
        var registry = _cluster.Client.GetLatticeRegistry();
        var physical = await registry.ResolveAsync(tree);
        var map = await registry.GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var slots = Enumerable.Range(0, map.VirtualShardCount).ToArray();
        var count = 0;
        foreach (var shardIndex in map.GetPhysicalShardIndices())
        {
            var leafId = await _cluster.Client.GetGrain<IShardRootGrain>($"{physical}/{shardIndex}").GetLeftmostLeafIdAsync();
            while (leafId is not null)
            {
                var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.Value);
                count += (await leaf.GetPendingMutationsForSlotsAsync(slots, map.VirtualShardCount)).Count(m => m.TransactionId == txid);
                leafId = await leaf.GetNextSiblingAsync();
            }
        }

        return count;
    }

    [Test]
    public async Task A_participant_that_stops_being_replicated_before_its_terminal_lets_the_barrier_decide_on_the_rest()
    {
        const string treeA = "rsd-xc-deenrol-a";
        const string treeB = "rsd-xc-deenrol-b";
        const string operation = "rsd-xc-deenrol-op";
        var (txA, txB) = (Guid.NewGuid(), Guid.NewGuid());

        Assert.That(await DeliverAsync(CrossTreePrepare(treeA, "k", 1, txA, 25_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreePrepare(treeB, "k", 2, txB, 25_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreeCommit(treeA, "k", txA, 25_100, operation, treeA, treeB)), Is.True);
        Assert.That(await ReadAsync(treeA, "k"), Is.Null, "precondition: the barrier waits for tree B");
        Assert.That(await CountPendingAsync(treeB, txB), Is.GreaterThan(0), "precondition: tree B holds the staged sub-saga");

        // Tree B stops being replicated here; its terminal is dropped at the enrollment gate.
        NotReplicatedHere[treeB] = true;
        var acked = await DeliverAsync(CrossTreeCommit(treeB, "k", txB, 25_101, operation, treeA, treeB));

        Assert.Multiple(async () =>
        {
            Assert.That(acked, Is.True, "a terminal for a tree not replicated here is dropped and acknowledged, as before");
            Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo(new byte[] { 1 }),
                "the barrier stops waiting for a tree that will never arrive and decides on the rest");
            Assert.That(await CountPendingAsync(treeB, txB), Is.Zero,
                "tree B is no longer a replica of the origin, so its pending sub-saga is discarded");
        });
    }

    [Test]
    public async Task A_terminal_rejected_for_a_merge_mode_mismatch_does_not_decide_the_barrier()
    {
        const string treeA = "rsd-xc-mismatch-a";
        const string treeB = "rsd-xc-mismatch-b";
        const string operation = "rsd-xc-mismatch-op";
        var (txA, txB) = (Guid.NewGuid(), Guid.NewGuid());

        Assert.That(await DeliverAsync(CrossTreePrepare(treeA, "k", 1, txA, 26_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreePrepare(treeB, "k", 2, txB, 26_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreeCommit(treeA, "k", txA, 26_100, operation, treeA, treeB)), Is.True);

        // Tree B is still replicated here; its terminal is rejected for another reason.
        await DeliverAsync(CrossTreeCommit(treeB, "k", txB, 26_101, operation, treeA, treeB) with { Mode = LatticeMergeMode.GCounter });

        Assert.That(await ReadAsync(treeA, "k"), Is.Null,
            "only a tree that has really stopped being replicated here may be dropped from the barrier");
    }

    [Test]
    public async Task A_terminal_that_keeps_failing_does_not_decide_the_barrier()
    {
        const string treeA = "rsd-xc-failing-a";
        const string treeB = "rsd-xc-failing-b";
        const string operation = "rsd-xc-failing-op";
        var (txA, txB) = (Guid.NewGuid(), Guid.NewGuid());

        Assert.That(await DeliverAsync(CrossTreePrepare(treeA, "k", 1, txA, 27_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreePrepare(treeB, "k", 2, txB, 27_000)), Is.True);
        Assert.That(await DeliverAsync(CrossTreeCommit(treeA, "k", txA, 27_100, operation, treeA, treeB)), Is.True);

        _failing.Fail = r => r.TransactionId == txB && r.Op == MutationKind.TxCommit;
        Assert.That(await DeliverAsync(CrossTreeCommit(treeB, "k", txB, 27_101, operation, treeA, treeB)), Is.False);

        Assert.That(await ReadAsync(treeA, "k"), Is.Null, "a deferred terminal is not an absent tree");
    }
}
