using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4692: a tree removed from replication and later re-added must come
/// back as a fresh replica, holding no leftover pending bucket from the origin.
/// Its re-add is a plain full bootstrap - not a re-seed the sender held saga
/// records back for - so the drain settles exactly the sagas that were pending
/// before the export opened and that the export does not carry. A saga staged
/// after the export opened is not stale, and keeps its bucket.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    private static WalRecord StrandedPrepare(string tree, string key, Guid txid) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 5 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = SiteAClusterId,
        TransactionId = txid,
        IsPrepared = true,
        AtomicBatchSize = 1,
        AtomicBatchIndex = 0,
    };

    private async Task<int> CountSiteBPendingAsync(string tree, Guid txid)
    {
        var registry = _siteB.Client.GetLatticeRegistry();
        var physical = await registry.ResolveAsync(tree);
        var map = await registry.GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var slots = Enumerable.Range(0, map.VirtualShardCount).ToArray();
        var count = 0;
        foreach (var shardIndex in map.GetPhysicalShardIndices())
        {
            var leafId = await _siteB.Client.GetGrain<IShardRootGrain>($"{physical}/{shardIndex}").GetLeftmostLeafIdAsync();
            while (leafId is not null)
            {
                var leaf = _siteB.Client.GetGrain<IBPlusLeafGrain>(leafId.Value);
                count += (await leaf.GetPendingMutationsForSlotsAsync(slots, map.VirtualShardCount)).Count(m => m.TransactionId == txid);
                leafId = await leaf.GetNextSiblingAsync();
            }
        }

        return count;
    }

    [Test]
    public async Task A_re_added_tree_bootstraps_as_a_fresh_replica_with_no_leftover_bucket_from_the_origin()
    {
        const string tree = "rsdr-4692-readd";
        const string key = "stranded";
        var txid = Guid.NewGuid();

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // A saga from the origin is staged on the receiver; the tree then stops
        // being replicated here, so its terminal never arrives. The source has
        // long since settled and purged the saga, so no export mentions it.
        Assert.That((await Applier(_siteB).ApplyAsync(StrandedPrepare(tree, key, txid))).Applied, Is.True, "precondition");
        Assert.That(await CountSiteBPendingAsync(tree, txid), Is.GreaterThan(0), "precondition: the bucket is stranded");

        // Re-added: a plain full bootstrap, with no re-seed request behind it.
        await BootstrapSiteBAsync(tree);

        Assert.Multiple(async () =>
        {
            Assert.That(await CountSiteBPendingAsync(tree, txid), Is.Zero,
                "the re-added tree must hold no leftover pending bucket from the origin");
            Assert.That(await siteB.GetAsync(key), Is.Null, "a discarded bucket installs nothing");
            Assert.That(await siteB.GetAsync("anchor"), Is.EqualTo(new byte[] { 1 }));
        });
    }

    [Test]
    public async Task A_plain_bootstrap_keeps_the_bucket_of_a_saga_staged_after_the_export_opened()
    {
        const string tree = "rsdr-4692-readd-live";
        const string key = "staged-mid-drain";
        var txid = Guid.NewGuid();

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // The sender holds nothing back during a plain bootstrap, so a saga can
        // stage on the receiver after the export opened. The export does not
        // mention it, but its terminal is still to come: it is not stale.
        ApplyResult? staged = null;
        _onDrainStarted = async () =>
        {
            Task<ApplyResult> delivery;
            using (ExecutionContext.SuppressFlow())
            {
                delivery = Task.Run(() => Applier(_siteB).ApplyAsync(StrandedPrepare(tree, key, txid)));
            }

            staged = await delivery;
        };
        try
        {
            await BootstrapSiteBAsync(tree);
        }
        finally
        {
            _onDrainStarted = null;
        }

        Assert.That(staged?.Applied, Is.True, "precondition: the prepare staged inside the drain");
        Assert.That(await CountSiteBPendingAsync(tree, txid), Is.GreaterThan(0),
            "a saga staged after the export opened must keep its bucket for its terminal");
    }
}
