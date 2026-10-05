using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4598: a saga the export's snap0 does not know - it starts after snap0
/// and stages its prepares after the prepared-row pass has read its leaves -
/// decides and drains some of its keys before the committed-projection pass
/// reads them. Those keys ship as plain committed values; the rest ship as
/// nothing, and no decision row names the saga, so the bootstrapped receiver
/// serves the saga split until the incremental stream delivers the saga's
/// last source-shard terminal. Runs the real export, registry, shard roots and
/// receiver apply path.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    [Test]
    public async Task Export_never_ships_part_of_a_saga_that_decides_after_the_prepared_pass_so_the_receiver_never_serves_it_split()
    {
        const string sourceTree = "snap-post-snapshot-source";
        const string receiverTree = "snap-post-snapshot-receiver";
        var (keyA, keyB) = AgedKeysOnDistinctShards();
        var txid = Guid.NewGuid();
        var source = _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree);

        // Materialise the source tree before the export opens.
        await source.ApplySetAsync("seed", new byte[] { 0 }, Hlc(8_000), ClusterId, sourceVectorClock: null, expiresAtTicks: 0);

        // After the prepared pass has read every leaf, the saga stages both
        // prepares, commits, and its terminal drains keyA's shard only before the
        // committed pass reads it: keyB's terminal is still on its way.
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physical = await registry.ResolveAsync(sourceTree);
        _provider.AfterPreparedPassForTesting = async () =>
        {
            await source.ApplyPreparedSetAsync(
                keyA, new byte[] { 1 }, Hlc(9_000), ClusterId, sourceVectorClock: null,
                expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 0);
            await source.ApplyPreparedSetAsync(
                keyB, new byte[] { 2 }, Hlc(9_000), ClusterId, sourceVectorClock: null,
                expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 1);
            await TxRegistryRouting.GetRegistry(_cluster.Client, sourceTree, txid).MarkCommittedAsync(txid);
            await _cluster.Client
                .GetGrain<IShardRootGrain>($"{physical}/{LatticeSharding.GetShardIndex(keyA, LatticeConstants.DefaultShardCount)}")
                .AppendTxTerminalAsync(txid, true);
        };

        List<SnapshotEntry> entries;
        try
        {
            entries = await DrainAsync(await _provider.ExportAsync(sourceTree, HybridLogicalClock.Zero));
        }
        finally
        {
            _provider.AfterPreparedPassForTesting = null;
        }

        Assert.That(entries.Any(e => e.IsPrepared && e.TransactionId == txid), Is.False,
            "precondition: the prepared pass ran before the saga staged its prepares");

        var applier = ReceiverApplier;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            foreach (var entry in entries.Where(e => e.IsDecision ? e.TransactionId == txid : e.Key == keyA || e.Key == keyB))
            {
                if (entry.IsDecision)
                {
                    await LatticeBootstrapCoordinatorGrain.ApplySettledDecisionAsync(_cluster.Client, receiverTree, entry);
                }
                else if (LatticeBootstrapCoordinatorGrain.ToSnapshotWalRecord(
                    entry, receiverTree, PreCutOrigin, LatticeMergeMode.LwwRegister) is { } record)
                {
                    await applier.ApplyAsync(record);
                }
            }
        }

        var read = await _cluster.Client.GetGrain<ILattice>(receiverTree).GetManyAsync([keyA, keyB]);
        Assert.That(read.Count == 0 || read.Count == 2, Is.True,
            $"the bootstrapped receiver serves the saga split to one atomic read: keyA {(read.ContainsKey(keyA) ? "visible" : "absent")}, keyB {(read.ContainsKey(keyB) ? "visible" : "absent")}");
    }
}
