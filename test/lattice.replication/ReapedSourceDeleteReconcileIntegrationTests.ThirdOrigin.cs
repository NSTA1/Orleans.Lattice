using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4549: a re-bootstrap must also converge on rows written by a third
/// origin. The export carries, at open, the source's applied frontier per
/// origin. The receiver installs it as a drop floor before the drain, so a
/// third-origin write still in flight cannot resurrect a key the source deleted
/// and reaped, and after the drain it deletes its live third-origin rows the
/// export lacks whose writes the source had applied. Site C is a third cluster
/// whose writes reach both sites through their real appliers.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    private static readonly byte[] ThirdValue = [3];

    /// <summary>A third-origin write stamped well before any write either site makes during the test.</summary>
    private static HybridLogicalClock PastHlc(int minutesAgo) =>
        new() { WallClockTicks = DateTime.UtcNow.AddMinutes(-minutesAgo).Ticks };

    private static Task<ApplyResult> ApplyFromSiteCAsync(TestCluster cluster, string tree, string key, HybridLogicalClock timestamp) =>
        Applier(cluster).ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = ThirdValue,
            Timestamp = timestamp,
            OriginClusterId = SiteCClusterId,
        });

    /// <summary>
    /// Applies from outside the bootstrap drain's ambient scope: the drain hook
    /// runs inside the coordinator's flow, where the drain's own bypass is on.
    /// </summary>
    private static async Task<ApplyResult> DeliverFromSiteCOutsideTheDrainAsync(
        TestCluster cluster, string tree, string key, HybridLogicalClock timestamp)
    {
        Task<ApplyResult> delivery;
        using (ExecutionContext.SuppressFlow())
        {
            delivery = Task.Run(() => ApplyFromSiteCAsync(cluster, tree, key, timestamp));
        }

        return await delivery;
    }

    private static Func<RemoteSnapshotMetadata, SnapshotSourceFrontier?> SiteCFrontier(
        HybridLogicalClock lowWatermark,
        HybridLogicalClock[]? held = null,
        Guid? lineage = null) =>
        metadata => new SnapshotSourceFrontier
        {
            Lineage = lineage ?? metadata.OpenGeneration?.Lineage,
            LowWatermarks = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [SiteCClusterId] = lowWatermark },
            Held = new Dictionary<string, HybridLogicalClock[]>(StringComparer.Ordinal) { [SiteCClusterId] = held ?? [] },
        };

    private async Task RebootstrapSiteBAsync(
        string tree,
        Func<RemoteSnapshotMetadata, SnapshotSourceFrontier?> frontier,
        Func<Task>? inDrain = null)
    {
        _openFrontier = frontier;
        _onDrainStarted = inDrain;
        try
        {
            await BootstrapSiteBAsync(tree);
        }
        finally
        {
            _openFrontier = null;
            _onDrainStarted = null;
        }
    }

    [Test]
    public async Task Re_bootstrap_deletes_a_third_origin_key_the_source_applied_then_deleted_and_reaped()
    {
        const string tree = "rsdr-4549-third-delete";
        const string key = "third-deleted";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition: the source applied C's write");
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, key, written)).Applied, Is.True, "precondition: the receiver applied C's write");

        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);
        Assert.That((await ExportSiteAAsync(tree)).Select(e => e.Key), Has.No.Member(key),
            "precondition: the reaped delete leaves no row in the export");

        await RebootstrapSiteBAsync(tree, SiteCFrontier(HybridLogicalClock.Tick(written)));

        Assert.Multiple(async () =>
        {
            Assert.That(await siteB.GetAsync(key), Is.Null,
                "the source applied C's write and then deleted the key, so the receiver must not keep C's row");
            Assert.That(await siteB.GetAsync("anchor"), Is.EqualTo(new byte[] { 1 }));
        });
    }

    [Test]
    public async Task Bootstrap_drop_floor_stops_an_in_flight_third_origin_write_resurrecting_a_reaped_delete()
    {
        const string tree = "rsdr-4549-in-flight";
        const string key = "third-in-flight";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // C's write reaches the source, which deletes the key and reaps the
        // tombstone; the copy bound for the receiver is still in flight.
        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition: the source applied C's write");
        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);

        ApplyResult? inDrain = null;
        await RebootstrapSiteBAsync(
            tree,
            SiteCFrontier(HybridLogicalClock.Tick(written)),
            async () => inDrain = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, key, written));

        Assert.That(inDrain, Is.Not.Null, "precondition: the delivery landed inside the drain");
        var late = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, key, written);
        Assert.Multiple(async () =>
        {
            Assert.That(inDrain!.Value.Applied, Is.False, "a write below the floor arriving mid-drain is dropped");
            Assert.That(late.Applied, Is.False, "the floor stays in force after the drain");
            Assert.That(await siteB.GetAsync(key), Is.Null,
                "the in-flight write must not resurrect a key the source deleted and reaped");
        });
    }

    [Test]
    public async Task A_third_origin_write_the_source_holds_is_neither_dropped_nor_reconciled()
    {
        const string tree = "rsdr-4549-held";
        const string heldKey = "held-at-source";
        const string inDrainKey = "held-in-drain";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // The source holds both writes without applying them, below its low watermark.
        var heldWrite = PastHlc(6);
        var inDrainWrite = PastHlc(5);
        var lowWatermark = PastHlc(4);
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, heldKey, heldWrite)).Applied, Is.True, "precondition");

        ApplyResult? inDrain = null;
        await RebootstrapSiteBAsync(
            tree,
            SiteCFrontier(lowWatermark, [heldWrite, inDrainWrite]),
            async () => inDrain = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, inDrainKey, inDrainWrite));

        Assert.Multiple(async () =>
        {
            Assert.That(inDrain?.Applied, Is.True, "a held write is not covered by the export, so it must apply");
            Assert.That(await siteB.GetAsync(inDrainKey), Is.EqualTo(ThirdValue));
            Assert.That(await siteB.GetAsync(heldKey), Is.EqualTo(ThirdValue),
                "the export lacks a held write because the source never applied it, so it must not be deleted");
        });
    }

    [Test]
    public async Task An_unstable_export_clears_the_drop_floor_and_infers_no_third_origin_delete()
    {
        const string tree = "rsdr-4549-unstable";
        const string key = "third-kept";
        const string late = "third-late";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition");
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, key, written)).Applied, Is.True, "precondition");
        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);

        await RebootstrapSiteBAsync(
            tree,
            SiteCFrontier(PastHlc(3)),
            async () =>
            {
                await siteA.DeleteTreeAsync();
                await siteA.RecoverTreeAsync();
            });

        var afterwards = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, late, PastHlc(4));
        Assert.Multiple(async () =>
        {
            Assert.That(await siteB.GetAsync(key), Is.EqualTo(ThirdValue), "an unstable export must not infer deletes");
            Assert.That(afterwards.Applied, Is.True, "the floor of an unstable export is cleared, so it drops nothing");
            Assert.That(await siteB.GetAsync(late), Is.EqualTo(ThirdValue));
        });
    }

    [Test]
    public async Task A_frontier_read_under_another_lineage_installs_no_floor_and_reconciles_nothing()
    {
        const string tree = "rsdr-4549-lineage";
        const string key = "third-other-lineage";
        const string inDrainKey = "third-in-drain";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, key, written)).Applied, Is.True, "precondition");

        ApplyResult? inDrain = null;
        await RebootstrapSiteBAsync(
            tree,
            SiteCFrontier(PastHlc(3), lineage: Guid.NewGuid()),
            async () => inDrain = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, inDrainKey, PastHlc(4)));

        Assert.Multiple(async () =>
        {
            Assert.That(inDrain?.Applied, Is.True, "no floor is installed from a frontier of another lineage");
            Assert.That(await siteB.GetAsync(key), Is.EqualTo(ThirdValue), "nothing is reconciled against it either");
        });
    }
}
