using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4627: a saga the export's snap0 had in flight decides and drains
/// some of its keys while the export runs. Those keys reach the committed pass
/// as plain values and the rest ship as prepared rows; with the saga's terminal
/// trimmed (a forced gap), nothing settles the prepared rows on the receiver,
/// which serves the saga split. The export must ship the late decision. Runs
/// the real export, registry, shard roots and receiver apply path.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    [Test]
    public async Task Export_ships_the_decision_of_a_saga_that_decides_mid_export_so_the_receiver_never_serves_it_split()
    {
        const string sourceTree = "snap-late-decision-source";
        const string receiverTree = "snap-late-decision-receiver";
        var (keyA, keyB) = AgedKeysOnDistinctShards();
        var txid = Guid.NewGuid();
        var source = _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree);
        await source.ApplyPreparedSetAsync(
            keyA, new byte[] { 1 }, Hlc(9_000), ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 0);
        await source.ApplyPreparedSetAsync(
            keyB, new byte[] { 2 }, Hlc(9_000), ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 1);

        // After snap0 has the saga in flight, it commits and drains keyA's shard
        // only: keyB's terminal is the record a forced gap trims.
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physical = await registry.ResolveAsync(sourceTree);
        _provider.AfterRegistrySnapshotForTesting = async () =>
        {
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
            _provider.AfterRegistrySnapshotForTesting = null;
        }

        Assert.That(entries.Any(e => e.IsPrepared && e.Key == keyB), Is.True,
            "precondition: keyB was still pending when the prepared pass read it");

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

        var lattice = _cluster.Client.GetGrain<ILattice>(receiverTree);
        var a = await lattice.GetAsync(keyA);
        var b = await lattice.GetAsync(keyB);
        Assert.That((a is null) == (b is null), Is.True,
            $"the receiver serves the saga split: keyA {(a is null ? "absent" : "visible")}, keyB {(b is null ? "absent" : "visible")}");
        Assert.That(b, Is.EqualTo(new byte[] { 2 }), "the late commit reaches the receiver whole");
    }
}
