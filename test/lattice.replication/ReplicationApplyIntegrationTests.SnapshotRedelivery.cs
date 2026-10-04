using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4463 removed the snapshot-pinned drop floor, so entries a bootstrap
/// snapshot ALREADY contains are no longer filtered when the incremental stream
/// re-delivers them: they reach the leaf merge. These tests prove that merge is
/// idempotent on the real site-B tree rather than citing it. Each one
/// bootstrap-applies entries under the drain scope (as the coordinator does),
/// pins the frontier, then re-delivers snapshot-held entries through a FRESH
/// applier - an empty shadow-forward identity cache, which is the worst case
/// (cache eviction or a receiver restart) - and asserts the visible state is
/// unchanged.
/// </summary>
public partial class ReplicationApplyIntegrationTests
{
    private async Task BootstrapApplyAndPinAsync(string tree, params WalRecord[] snapshotEntries)
    {
        var drain = CreateSiteBApplier();
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            foreach (var entry in snapshotEntries)
            {
                await drain.ApplyAsync(entry);
            }
        }

        var frontier = new VersionVector();
        foreach (var entry in snapshotEntries)
        {
            if (entry.Timestamp > frontier.GetClock(entry.OriginClusterId!))
            {
                frontier.Entries[entry.OriginClusterId!] = entry.Timestamp;
            }
        }

        await _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(tree)
            .PinSnapshotAsync(HybridLogicalClock.Zero, frontier, CancellationToken.None);
    }

    private static WalRecord LwwSet(string tree, string key, byte[] value, HybridLogicalClock ts) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = ts,
        Mode = LatticeMergeMode.LwwRegister,
        OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
    };

    [Test]
    public async Task Redelivered_snapshot_lww_entries_leave_the_value_unchanged_and_an_older_duplicate_does_not_regress_a_newer_row()
    {
        const string tree = "ri-redeliver-lww";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var kAt100 = LwwSet(tree, "k", new byte[] { 1 }, Hlc(100));
        var jAt150 = LwwSet(tree, "j", new byte[] { 2 }, Hlc(150));
        var jAt200 = LwwSet(tree, "j", new byte[] { 3 }, Hlc(200));

        // The snapshot holds k@100 and the newer j@200 (j@150 was superseded
        // at the source before the export).
        await BootstrapApplyAndPinAsync(tree, kAt100, jAt200);

        // The incremental stream re-delivers what the snapshot already holds,
        // including the older j@150 that the snapshot superseded.
        var redelivery = CreateSiteBApplier();
        await redelivery.ApplyAsync(kAt100);
        await redelivery.ApplyAsync(jAt150);
        await redelivery.ApplyAsync(jAt200);

        var k = await lattice.GetWithVersionAsync("k");
        var j = await lattice.GetWithVersionAsync("j");
        Assert.Multiple(() =>
        {
            Assert.That(k.Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(k.Version, Is.EqualTo(Hlc(100)));
            Assert.That(j.Value, Is.EqualTo(new byte[] { 3 }), "An older-HLC duplicate must not regress a newer row.");
            Assert.That(j.Version, Is.EqualTo(Hlc(200)));
        });
    }

    [Test]
    public async Task Redelivered_snapshot_pn_counter_delta_does_not_double_count()
    {
        const string tree = "ri-redeliver-pn";
        const string key = "k";
        TwoSiteClusterFixture.TreeModeOverrides[tree] = LatticeMergeMode.PnCounter;
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);

        var counter = new PnCounter();
        counter.Increment("site-a", 5);
        counter.Decrement("site-a", 1);
        var delta = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["site-a"] = 5 },
            Decrements = new Dictionary<string, long> { ["site-a"] = 1 },
        };
        var entry = new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = JsonLatticeSerializer<PnCounter>.Default.Serialize(counter),
            Delta = JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(delta),
            Timestamp = Hlc(1_000),
            Mode = LatticeMergeMode.PnCounter,
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        };

        await BootstrapApplyAndPinAsync(tree, entry);
        Assert.That((await lattice.PnCounter(key).GetAsync()).Value, Is.EqualTo(4), "Snapshot baseline.");

        // Re-delivered twice, through fresh appliers, so no identity cache
        // short-circuits the fold.
        await CreateSiteBApplier().ApplyAsync(entry);
        await CreateSiteBApplier().ApplyAsync(entry);

        Assert.That((await lattice.PnCounter(key).GetAsync()).Value, Is.EqualTo(4),
            "Re-folding a delta the snapshot already holds must not double the count.");
    }

    [Test]
    public async Task Redelivered_delete_stays_deleted_and_a_redelivered_older_set_does_not_resurrect_the_key()
    {
        const string tree = "ri-redeliver-del";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var setAt100 = LwwSet(tree, "k", new byte[] { 7 }, Hlc(100));
        var deleteAt200 = new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Delete,
            Key = "k",
            Timestamp = Hlc(200),
            IsTombstone = true,
            Mode = LatticeMergeMode.LwwRegister,
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        };

        // The snapshot holds a live row the source later deletes; the delete
        // reaches the receiver on the incremental stream.
        await BootstrapApplyAndPinAsync(tree, setAt100);
        await CreateSiteBApplier().ApplyAsync(deleteAt200);
        Assert.That(await lattice.GetAsync("k"), Is.Null, "Delete applied.");

        // Both the delete and the snapshot-held older set are re-delivered.
        var redelivery = CreateSiteBApplier();
        await redelivery.ApplyAsync(deleteAt200);
        await redelivery.ApplyAsync(setAt100);

        Assert.That(await lattice.GetAsync("k"), Is.Null,
            "A re-delivered older set must not resurrect a deleted key.");
    }
}
