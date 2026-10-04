using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// A receiver bootstrapped while an origin saga's decision has aged out over a
/// stranded prepare (issue #4481). The saga committed and its terminal drained
/// keyA, but keyB's bucket was never drained; the decision's tombstone then
/// outlived <c>TxDecisionRetention</c>, so the frozen registry snapshot reports
/// the saga as <see cref="TxStatus.Indeterminate"/>. The export must ship the
/// saga whole: the receiver's registry has no row for it, so a keyB shipped as a
/// prepared row would read as in flight there and serve its pre-saga value
/// beside keyA's post-saga value, with nothing on either side ever repairing it.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    private static (string KeyA, string KeyB) AgedKeysOnDistinctShards()
    {
        const string keyA = "stranded-alpha";
        var shardA = LatticeSharding.GetShardIndex(keyA, LatticeConstants.DefaultShardCount);
        for (var i = 0; i < 1000; i++)
        {
            var candidate = $"stranded-beta-{i}";
            if (LatticeSharding.GetShardIndex(candidate, LatticeConstants.DefaultShardCount) != shardA)
            {
                return (keyA, candidate);
            }
        }

        throw new InvalidOperationException("could not find two keys on distinct shards");
    }

    /// <summary>
    /// Replays an export onto a fresh receiver tree through the same apply seam
    /// the bootstrap coordinator drives: committed rows as replicated writes,
    /// prepared rows into the receiver's pending buckets.
    /// </summary>
    private async Task ReplayOntoReceiverAsync(string receiverTree, IEnumerable<SnapshotEntry> entries)
    {
        var apply = _cluster.Client.GetGrain<IReplicationApplyGrain>(receiverTree);
        foreach (var entry in entries)
        {
            if (entry.IsPrepared)
            {
                await apply.ApplyPreparedSetAsync(
                    entry.Key, entry.Value, entry.Timestamp, ClusterId, sourceVectorClock: null,
                    expiresAtTicks: entry.ExpiresAtTicks, entry.TransactionId,
                    atomicBatchSize: entry.AtomicBatchSize, atomicBatchIndex: entry.AtomicBatchIndex);
            }
            else
            {
                await apply.ApplySetAsync(
                    entry.Key, entry.Value, entry.Timestamp, ClusterId, sourceVectorClock: null,
                    expiresAtTicks: entry.ExpiresAtTicks);
            }
        }
    }

    [Test]
    public async Task Receiver_bootstrapped_over_an_aged_out_commit_with_a_stranded_prepare_serves_the_saga_whole()
    {
        const string receiverTree = "snap-stranded-receiver";
        var (keyA, keyB) = AgedKeysOnDistinctShards();
        var sourceHlc = Hlc(3_000);
        var txid = Guid.NewGuid();

        var source = _cluster.Client.GetGrain<IReplicationApplyGrain>(AgedTree);
        await source.ApplyPreparedSetAsync(
            keyA, new byte[] { 1 }, sourceHlc, ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 0);
        await source.ApplyPreparedSetAsync(
            keyB, new byte[] { 2 }, sourceHlc, ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 1);

        // The saga commits and its terminal reaches keyA's shard only, so keyB's
        // bucket is stranded; then the decision ages out of the readable window.
        var shardA = LatticeSharding.GetShardIndex(keyA, LatticeConstants.DefaultShardCount);
        await source.ApplyTxTerminalAsync(txid, committed: true, shardIndex: shardA, Hlc(3_100), ClusterId);
        var registry = _cluster.Client.GetGrain<ITxRegistryGrain>(AgedTree);
        await registry.ForgetAsync(txid);
        await Task.Delay(TimeSpan.FromMilliseconds(700));

        var snapshot = await registry.SnapshotAsync();
        var sourceLattice = _cluster.Client.GetGrain<ILattice>(AgedTree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(snapshot.TryGetValue(txid, out var aged) ? aged : TxStatus.InFlight,
                Is.EqualTo(TxStatus.Indeterminate), "precondition: the decision has aged out while its row is stored");
            Assert.That(await sourceLattice.GetAsync(keyA), Is.EqualTo(new byte[] { 1 }),
                "precondition: the terminal drained keyA on the source");
        });

        var stream = await _provider.ExportAsync(AgedTree, HybridLogicalClock.Zero);
        var entries = (await DrainAsync(stream)).Where(e => e.Key == keyA || e.Key == keyB).ToList();
        await ReplayOntoReceiverAsync(receiverTree, entries);

        var receiver = _cluster.Client.GetGrain<ILattice>(receiverTree);
        var receivedA = await receiver.GetAsync(keyA);
        var receivedB = await receiver.GetAsync(keyB);
        Assert.Multiple(() =>
        {
            Assert.That(receivedA, Is.EqualTo(new byte[] { 1 }), "keyA arrives post-saga");
            Assert.That(receivedB, Is.EqualTo(new byte[] { 2 }),
                "keyB must arrive post-saga beside keyA: the recorded verdict behind the aged-out row is a commit");
        });
    }
}
