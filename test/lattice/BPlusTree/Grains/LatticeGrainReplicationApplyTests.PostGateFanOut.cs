using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the receiver's post-gate fan-out rows of
/// <c>spec/atomic-commit/RefinementCrossCluster.md</c> (issue #4436):
/// <c>ReceiverFanOut(k)</c>, <c>RCommittedEventuallyVisible</c> and
/// <c>RNoStrandedPrepare</c>. A replicated terminal that completes the receiver's
/// tally marks the registry AND fans the terminal out to the leaves, and only the
/// second of those consumes the leaf's prepared bucket. Reads cannot tell the two
/// apart while the registry answers Committed - the gate surfaces a pending bucket
/// under a committed registry - so these tests look at the leaves' pending buckets
/// directly: after the terminal none may remain for the saga, on a commit (where
/// the value must also be in the projection) and on an abort.
/// </summary>
public partial class LatticeGrainReplicationApplyTests
{
    [Test]
    public async Task A_replicated_commit_terminal_drains_the_bucket_into_the_projection()
    {
        var tree = "rapply-fanout-commit-" + Guid.NewGuid().ToString("N")[..8];
        var txid = Guid.NewGuid();
        var hlc = Hlc(80_000, 0);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await apply.ApplyPreparedSetAsync(
            "k", new byte[] { 7 }, hlc, "site-x", sourceVectorClock: null,
            expiresAtTicks: 0, transactionId: txid, atomicBatchSize: 1, atomicBatchIndex: 0);
        var staged = await PendingMutationsForAsync(tree, txid);

        await apply.ApplyTxTerminalAsync(
            txid, committed: true, shardIndex: await SourceShardOfAsync(tree, "k"),
            terminalHlc: hlc with { WallClockTicks = hlc.WallClockTicks + 1 },
            originClusterId: "site-x", atomicShardCount: 1);

        var remaining = await PendingMutationsForAsync(tree, txid);
        var value = await lattice.GetAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(staged, Is.EqualTo(1),
                "Positive control: the prepared write must have been staged in a pending bucket, or the "
                + "drain assertion below observes nothing.");
            Assert.That(remaining, Is.Zero,
                "The post-gate fan-out must consume the saga's bucket; a bucket left behind is a stranded "
                + "prepare that only the registry's answer keeps visible.");
            Assert.That(value, Is.EqualTo(new byte[] { 7 }));
        });
    }

    [Test]
    public async Task A_replicated_abort_terminal_discards_the_bucket()
    {
        var tree = "rapply-fanout-abort-" + Guid.NewGuid().ToString("N")[..8];
        var txid = Guid.NewGuid();
        var hlc = Hlc(90_000, 0);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await apply.ApplyPreparedSetAsync(
            "k", new byte[] { 8 }, hlc, "site-x", sourceVectorClock: null,
            expiresAtTicks: 0, transactionId: txid, atomicBatchSize: 1, atomicBatchIndex: 0);
        var staged = await PendingMutationsForAsync(tree, txid);

        await apply.ApplyTxTerminalAsync(
            txid, committed: false, shardIndex: await SourceShardOfAsync(tree, "k"),
            terminalHlc: hlc with { WallClockTicks = hlc.WallClockTicks + 1 },
            originClusterId: "site-x", atomicShardCount: 1);

        var remaining = await PendingMutationsForAsync(tree, txid);
        var value = await lattice.GetAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(staged, Is.EqualTo(1), "Positive control: the prepared write must have been staged.");
            Assert.That(remaining, Is.Zero, "The post-gate fan-out must discard an aborted saga's bucket.");
            Assert.That(value, Is.Null);
        });
    }

    /// <summary>
    /// The physical shard <paramref name="key"/> routes to. The terminal is a
    /// source-shard terminal, and the fan-out reaches exactly that shard's
    /// split-forward closure, so the test must address the shard the key lives on
    /// (origin and receiver share the default layout here).
    /// </summary>
    private async Task<int> SourceShardOfAsync(string tree, string key)
    {
        var shardMap = await _fixture.Cluster.Client.GetLatticeRegistry().GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        return shardMap.Resolve(key);
    }

    /// <summary>
    /// Counts the pending prepared mutations every leaf of every physical shard
    /// of <paramref name="tree"/> holds for <paramref name="txid"/>, walking the
    /// leaf chains the way the snapshot export's prepared-row pass does.
    /// </summary>
    private async Task<int> PendingMutationsForAsync(string tree, Guid txid)
    {
        var registry = _fixture.Cluster.Client.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(tree);
        var shardMap = await registry.GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var allSlots = Enumerable.Range(0, shardMap.VirtualShardCount).ToArray();

        var count = 0;
        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            var shard = _fixture.Cluster.Client.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync();
            while (leafId is not null)
            {
                var leaf = _fixture.Cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.Value);
                var pending = await leaf.GetPendingMutationsForSlotsAsync(allSlots, shardMap.VirtualShardCount);
                count += pending.Count(m => m.TransactionId == txid);
                leafId = await leaf.GetNextSiblingAsync();
            }
        }

        return count;
    }
}
