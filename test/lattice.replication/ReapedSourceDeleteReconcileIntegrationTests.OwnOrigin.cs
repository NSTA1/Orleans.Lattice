using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4549: the bootstrap drop floor never covers the source's own origin.
/// The source's own writes are re-shipped in log order with their deletes, and
/// no HLC is downward-closed over them - per-leaf clocks are unordered, and a
/// source restore can lose its own writes without deleting them - so a floor
/// for them would drop deliveries the export never reflected.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    private static readonly byte[] SourceValue = [7];

    private static async Task<ApplyResult> DeliverFromSiteAOutsideTheDrainAsync(
        TestCluster cluster, string tree, string key, HybridLogicalClock timestamp)
    {
        Task<ApplyResult> delivery;
        using (ExecutionContext.SuppressFlow())
        {
            delivery = Task.Run(() => Applier(cluster).ApplyAsync(new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.Set,
                Key = key,
                Value = SourceValue,
                Timestamp = timestamp,
                OriginClusterId = SiteAClusterId,
            }));
        }

        return await delivery;
    }

    [Test]
    public async Task A_source_origin_write_below_the_floor_is_kept_because_the_source_origin_is_never_floored()
    {
        const string tree = "rsdr-4549-own-origin";
        const string sourceKey = "source-late";
        const string thirdKey = "third-late";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // The frontier also names the source's own origin, with the same
        // watermark as C's. Production's export never carries one; the receiver
        // must not floor the source's origin even when a frontier does.
        var lateWrite = PastHlc(5);
        var watermark = PastHlc(1);
        await RebootstrapSiteBAsync(tree, metadata => new SnapshotSourceFrontier
        {
            Lineage = metadata.OpenGeneration?.Lineage,
            LowWatermarks = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                [SiteCClusterId] = watermark,
                [SiteAClusterId] = watermark,
            },
            Held = new Dictionary<string, HybridLogicalClock[]>(StringComparer.Ordinal),
        });

        var third = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, thirdKey, lateWrite);
        var source = await DeliverFromSiteAOutsideTheDrainAsync(_siteB, tree, sourceKey, lateWrite);
        Assert.Multiple(async () =>
        {
            Assert.That(third.Applied, Is.False, "precondition: the floor is installed and final for C");
            Assert.That(third.Deferred, Is.False, "precondition: the import closed stable, so C's write is dropped");
            Assert.That(source.Applied, Is.True, "a source-origin write below the watermark is never floored");
            Assert.That(await siteB.GetAsync(sourceKey), Is.EqualTo(SourceValue),
                "the export does not reflect a source write it never carried, so the receiver must keep it");
        });
    }

    [Test]
    public async Task The_export_carries_no_watermark_for_the_source_origin_even_when_its_frontier_holds_one()
    {
        const string tree = "rsdr-4549-own-origin-export";
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("anchor", new byte[] { 1 });

        // The source's tree frontier holds a watermark for C and, here, for its
        // own origin too.
        var frontier = _siteA.Client.GetGrain<IReplicationTreeFrontierGrain>(tree);
        var watermark = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks });
        foreach (var origin in new[] { SiteCClusterId, SiteAClusterId })
        {
            var epoch = await frontier.ObserveAsync(origin, shipped: null);
            Assert.That(epoch, Is.Not.EqualTo(Guid.Empty), "precondition: the tree tracks a lineage, so its frontier is exact");
            await frontier.ObserveAsync(origin, new ReplicationSourceFrontier
            {
                ReceiverLineage = epoch,
                TreeLowWatermark = watermark,
                OriginLowWatermark = watermark,
                OriginGeneration = 1,
            });
        }

        var held = (await frontier.GetAsync()).LowWatermarks;
        var stream = await _siteAProvider.ExportAsync(tree, HybridLogicalClock.Zero);
        Assert.Multiple(() =>
        {
            Assert.That(held, Does.ContainKey(SiteAClusterId), "precondition: the frontier holds the source's own origin");
            Assert.That(stream.OpenFrontier?.LowWatermarks, Does.ContainKey(SiteCClusterId),
                "precondition: the export carries the foreign origin's watermark");
            Assert.That(stream.OpenFrontier!.LowWatermarks, Does.Not.ContainKey(SiteAClusterId),
                "the export never carries a watermark for the source's own origin, so no receiver can floor it");
        });
    }
}
