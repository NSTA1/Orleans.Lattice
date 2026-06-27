using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the replication-side overlay of the
/// <see cref="ILatticeWalGc"/> public contract:
/// <see cref="ILatticeWalGc.RunOnceAsync"/> returns a
/// <see cref="LatticeWalGcReport"/> populated with the requested tree
/// name, non-negative shard / entry counters, and a non-<see langword="null"/>
/// <see cref="LatticeWalGcReport.MinCursor"/> after the shipper has
/// reported.
/// <para>
/// The default DI registration assertion lives in the core
/// <c>PublicApiContractTests.WalGc</c> partial because the type is
/// declared in <c>Orleans.Lattice</c>; this partial covers the
/// replication-driven traffic on top of it.
/// </para>
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public async Task ILatticeWalGc_run_once_returns_report_with_tree_name_and_non_negative_counters()
    {
        var treeId = NextTreeId("gc-run");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var gc = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ILatticeWalGc>();

        var report = await gc.RunOnceAsync(treeId);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeName, Is.EqualTo(treeId));
            Assert.That(report.ShardsScanned, Is.GreaterThanOrEqualTo(1));
            Assert.That(report.EntriesTrimmed, Is.GreaterThanOrEqualTo(0));
        });
    }

    [Test]
    public async Task ILatticeWalGc_run_once_min_cursor_is_non_null_after_consumer_reports()
    {
        var treeId = NextTreeId("gc-min-cursor");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var siteAServices = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId);
        var gc = siteAServices.GetRequiredService<ILatticeWalGc>();
        var grainFactory = siteAServices.GetRequiredService<Orleans.IGrainFactory>();
        var pinGrain = grainFactory
            .GetGrain<Orleans.Lattice.BPlusTree.Grains.IWalMaterialiserPinGrain>(treeId);

        // A data-capable leaf seeds a HybridLogicalClock.Zero materialiser
        // "block" pin at birth (issue #947), which deliberately holds the
        // WAL GC's cursor-trim floor at null until that leaf produces its
        // first durable checkpoint and advances the pin past Zero. A leaf
        // advances its checkpoint by replaying its WAL on activation, so
        // force the site-A data leaf to deactivate and then reactivate it
        // with a read: the reactivation replay flushes the leaf's first
        // checkpoint, lifts the block pin, and lets the shipper's reported
        // cursor surface as MinCursor. The leaf grain ids are recovered
        // from the durable pin store's per-leaf consumer ids.
        LatticeWalGcReport report = default;
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                foreach (var leaf in await DataLeavesFromPinsAsync(pinGrain, grainFactory))
                {
                    await leaf.ForceDeactivateAsync();
                }

                await treeOnA.GetAsync("k");
                report = await gc.RunOnceAsync(treeId);
                return report.MinCursor is not null;
            },
            "GC run to observe a non-null MinCursor after the leaf checkpoints");

        Assert.That(report.MinCursor, Is.Not.Null);
        Assert.That(report.MinCursor!.Value.CompareTo(HybridLogicalClock.Zero), Is.GreaterThanOrEqualTo(0));
    }

    /// <summary>
    /// Recovers the distinct data-leaf grain references for the tree from the
    /// durable materialiser pin store. Each pin's consumer id has the shape
    /// <c>_lattice_materialiser_{treeId}_{leafGrainId}_{partition}</c>; the
    /// embedded <c>bplusleaf/{guid}</c> grain id is extracted and de-duplicated
    /// across partitions so each leaf is deactivated once.
    /// </summary>
    private static async Task<IReadOnlyList<Orleans.Lattice.BPlusTree.IBPlusLeafGrain>> DataLeavesFromPinsAsync(
        Orleans.Lattice.BPlusTree.Grains.IWalMaterialiserPinGrain pinGrain,
        Orleans.IGrainFactory grainFactory)
    {
        const string marker = "bplusleaf/";
        var pins = await pinGrain.GetPinsAsync();
        var leaves = new List<Orleans.Lattice.BPlusTree.IBPlusLeafGrain>();
        var seen = new HashSet<Guid>();
        foreach (var consumerId in pins.Keys)
        {
            var start = consumerId.IndexOf(marker, StringComparison.Ordinal);
            if (start < 0)
            {
                continue;
            }

            start += marker.Length;
            var end = consumerId.LastIndexOf('_');
            if (end <= start)
            {
                continue;
            }

            if (Guid.TryParseExact(consumerId[start..end], "N", out var leafId) && seen.Add(leafId))
            {
                leaves.Add(grainFactory.GetGrain<Orleans.Lattice.BPlusTree.IBPlusLeafGrain>(leafId));
            }
        }

        return leaves;
    }
}
