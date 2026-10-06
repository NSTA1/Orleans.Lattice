using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The re-seed drain's stale-bucket clear, end to end through the real
/// bootstrap coordinator (issue #4533, epic #4430 review finding S1). A
/// re-seed drain settles every leftover pending bucket from the source, except
/// those of a saga its export carried as prepared rows: that saga was still in
/// flight at the cut, and the terminal that follows the re-seed commits it.
/// The carried set is derived by the coordinator from the export it drains, so
/// only a test that drives a real drain pins that wiring; the clearer's own
/// unit tests hand it a carried set directly.
/// </summary>
public partial class ReceiverSagaPoisonReseedIntegrationTests
{
    [Test]
    public async Task Reseed_drain_keeps_a_saga_its_export_carried_in_flight_and_its_terminal_commits_it_whole()
    {
        const string tree = "rspr-reseed-carried";
        var (keyA, keyB) = KeysOnDistinctShards("rsca");
        var txid = Guid.NewGuid();

        // The source holds the saga prepared and undecided, so the re-seed's
        // export carries it as prepared rows.
        var sourceApply = _siteA.Client.GetGrain<IReplicationApplyGrain>(tree);
        await sourceApply.ApplyPreparedSetAsync(
            keyA, [1], Hlc(30_000), SiteAClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 0);
        await sourceApply.ApplyPreparedSetAsync(
            keyB, [2], Hlc(30_001), SiteAClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 1);

        // The receiver staged one of its prepares before the sender took the
        // peer off the log, so the drain finds a leftover bucket of the saga.
        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 30_000)), Is.True);
        Assert.That(await PendingKeysAsync(_siteB, tree), Is.EquivalentTo(new[] { keyA }),
            "precondition: the receiver holds a pre-re-seed bucket of the saga");

        // The sender asks for a re-seed after the current export epoch, so the
        // export the receiver drains postdates the request and the clear runs.
        var reseedAfter = await _siteA.Client.GetGrain<IReplicationExportEpochGrain>(tree).GetAsync();
        var coordinator = _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
        await coordinator.BootstrapForReseedAsync(SiteAClusterId, reseedAfter, start: true, CancellationToken.None);
        await WaitForLiveIncrementalAsync(_siteB, tree);

        var receiver = _siteB.Client.GetGrain<ILattice>(tree);
        Assert.Multiple(async () =>
        {
            Assert.That(await coordinator.IsReseedPendingAsync(SiteAClusterId), Is.False,
                "precondition: the drain consumed the re-seed request, so its stale clear ran");
            Assert.That(await PendingKeysAsync(_siteB, tree), Is.EquivalentTo(new[] { keyA, keyB }),
                "the clear must leave a saga the export carried in flight: its buckets wait for the terminal");
            Assert.That(await receiver.GetAsync(keyA), Is.Null, "the in-flight saga stays invisible until its terminal");
            Assert.That(await receiver.GetAsync(keyB), Is.Null, "the in-flight saga stays invisible until its terminal");
        });

        foreach (var key in new[] { keyA, keyB })
        {
            var shard = LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);
            Assert.That(await DeliverAsync(new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.TxCommit,
                Key = shard.ToString(System.Globalization.CultureInfo.InvariantCulture),
                Timestamp = Hlc(30_100 + shard),
                OriginClusterId = SiteAClusterId,
                TransactionId = txid,
                ShardIndex = shard,
                AtomicShardCount = 2,
            }), Is.True);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await receiver.GetAsync(keyA), Is.EqualTo(new byte[] { 1 }), "the carried saga commits whole");
            Assert.That(await receiver.GetAsync(keyB), Is.EqualTo(new byte[] { 2 }), "the carried saga commits whole");
            Assert.That(await PendingKeysAsync(_siteB, tree), Is.Empty, "no bucket strands after the terminal");
        });
    }
}
