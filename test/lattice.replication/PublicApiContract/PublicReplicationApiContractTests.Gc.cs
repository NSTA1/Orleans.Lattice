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
        // "block" pin at birth (issue #947), which deliberately holds the WAL
        // GC's cursor-trim floor at null until that leaf produces its first
        // durable checkpoint and advances the pin past Zero. A leaf advances
        // its checkpoint by replaying its WAL on a *cold* activation: only a
        // read that lands on a freshly-reactivated leaf replays the Set and
        // flushes the first checkpoint. A read against an already-warm leaf
        // does nothing, so the data leaves must be deactivated and then read
        // through the tree on each attempt. The leaf grain ids are recovered
        // from the durable pin store's per-leaf consumer ids.
        //
        // ForceDeactivateAsync is fire-and-forget: it calls DeactivateOnIdle,
        // which only *schedules* deactivation after the current turn. The prior
        // shape issued the deactivation and then immediately read with no gap,
        // so the read frequently raced the not-yet-applied deactivation and hit
        // the still-warm leaf (no replay, pin stays Zero). That is a lockstep
        // race, not slow I/O, which is why widening the timeout (40 s, then
        // 90 s) never helped: the read simply never landed on a cold leaf.
        // The fix deactivates, then waits for the scheduled deactivation to
        // apply before reading, so the read deterministically drives a cold
        // reactivation, replay, and checkpoint flush. The shared convergence
        // timeout is sufficient; no per-call override is needed.
        var dataLeaves = await DataLeavesFromPinsAsync(pinGrain, grainFactory);

        LatticeWalGcReport report = default;
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                foreach (var leaf in dataLeaves)
                {
                    await leaf.ForceDeactivateAsync();
                }

                // Let the scheduled DeactivateOnIdle apply before the read so
                // the read lands on a cold leaf and drives the replay.
                await Task.Delay(250);
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
