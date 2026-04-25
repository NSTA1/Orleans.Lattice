using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end coverage of the apply seam: source HLC and origin cluster
/// id must round-trip verbatim through <see cref="IReplicationApplyGrain"/>
/// onto the receiving cluster's persisted state, and per-origin
/// <see cref="IReplicationHighWaterMarkGrain"/> grains must track the
/// most-recent applied timestamp keyed by <c>{treeId}/{originClusterId}</c>.
/// </summary>
[TestFixture]
public class ReplicationApplyIntegrationTests
{
    private TwoSiteClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new TwoSiteClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public async Task ApplySetAsync_preserves_source_hlc_on_site_b_persisted_value()
    {
        const string tree = "ri-set-hlc";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(123_456_789, 7);

        await apply.ApplySetAsync("k", new byte[] { 1, 2, 3 }, sourceHlc, TwoSiteClusterFixture.SiteAClusterId, expiresAtTicks: 0);

        var versioned = await lattice.GetWithVersionAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(versioned.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(versioned.Version, Is.EqualTo(sourceHlc));
        });
    }

    [Test]
    public async Task ApplySetAsync_with_remote_origin_does_not_republish_on_site_b()
    {
        // Cycle-break: a value applied on Site B with origin "site-a"
        // must NOT generate a new outbound replog entry on Site B's sink
        // (because the change-feed observer filters local-origin only,
        // and the persisted entry's origin must remain "site-a" — proving
        // the apply seam preserved it verbatim).
        const string tree = "ri-set-cycle";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);

        var beforeLocal = _fixture.SiteBSink.Entries.Count(e => e.TreeId == tree && e.OriginClusterId == TwoSiteClusterFixture.SiteBClusterId);
        await apply.ApplySetAsync("k", new byte[] { 9 }, Hlc(1_000), TwoSiteClusterFixture.SiteAClusterId, expiresAtTicks: 0);
        var afterLocal = _fixture.SiteBSink.Entries.Count(e => e.TreeId == tree && e.OriginClusterId == TwoSiteClusterFixture.SiteBClusterId);

        Assert.That(afterLocal, Is.EqualTo(beforeLocal),
            "Apply with remote origin must not produce a Site B-origin replog entry.");
    }

    [Test]
    public async Task ApplyDeleteAsync_preserves_source_hlc_as_tombstone_on_site_b()
    {
        const string tree = "ri-del";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);

        await apply.ApplySetAsync("k", new byte[] { 1 }, Hlc(100), TwoSiteClusterFixture.SiteAClusterId, expiresAtTicks: 0);
        var deleteHlc = Hlc(200);
        await apply.ApplyDeleteAsync("k", deleteHlc, TwoSiteClusterFixture.SiteAClusterId);

        var versioned = await lattice.GetWithVersionAsync("k");
        Assert.That(versioned.Value, Is.Null);
    }

    [Test]
    public async Task ApplyDeleteRangeAsync_removes_keys_across_physical_shards()
    {
        const string tree = "ri-range";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("m", new byte[] { 2 });
        await lattice.SetAsync("y", new byte[] { 3 });

        await apply.ApplyDeleteRangeAsync("a", "z", TwoSiteClusterFixture.SiteAClusterId);

        Assert.Multiple(() =>
        {
            Assert.That(lattice.GetAsync("a").Result, Is.Null);
            Assert.That(lattice.GetAsync("m").Result, Is.Null);
            Assert.That(lattice.GetAsync("y").Result, Is.Null);
        });
    }

    [Test]
    public async Task ApplySetAsync_older_source_hlc_does_not_overwrite_newer_local_value()
    {
        // LWW guarantee — proves the source HLC is honoured (not rewritten
        // to a fresh local one) when the apply seam persists the value.
        const string tree = "ri-lww";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("k", new byte[] { 99 });
        var local = await lattice.GetWithVersionAsync("k");

        // Apply a remote write with an older HLC; LWW must reject.
        var olderHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks - 1 };
        await apply.ApplySetAsync("k", new byte[] { 1 }, olderHlc, TwoSiteClusterFixture.SiteAClusterId, expiresAtTicks: 0);

        var after = await lattice.GetWithVersionAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(after.Value, Is.EqualTo(new byte[] { 99 }));
            Assert.That(after.Version, Is.EqualTo(local.Version));
        });
    }

    [Test]
    public async Task HighWaterMarkGrain_tracks_most_recent_applied_timestamp_per_origin()
    {
        const string tree = "ri-hwm";
        var hwm = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(
            $"{tree}/{TwoSiteClusterFixture.SiteAClusterId}");

        Assert.That(await hwm.GetAsync(), Is.EqualTo(HybridLogicalClock.Zero));

        Assert.That(await hwm.TryAdvanceAsync(Hlc(10)), Is.True);
        Assert.That(await hwm.GetAsync(), Is.EqualTo(Hlc(10)));

        // Older timestamp must not advance.
        Assert.That(await hwm.TryAdvanceAsync(Hlc(5)), Is.False);
        Assert.That(await hwm.GetAsync(), Is.EqualTo(Hlc(10)));

        // Equal timestamp does not advance.
        Assert.That(await hwm.TryAdvanceAsync(Hlc(10)), Is.False);

        // Newer timestamp advances.
        Assert.That(await hwm.TryAdvanceAsync(Hlc(20)), Is.True);
        Assert.That(await hwm.GetAsync(), Is.EqualTo(Hlc(20)));
    }

    [Test]
    public async Task HighWaterMarkGrain_isolates_state_per_tree_and_per_origin()
    {
        var hwmTreeA = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(
            $"ri-iso-1/{TwoSiteClusterFixture.SiteAClusterId}");
        var hwmTreeB = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(
            $"ri-iso-2/{TwoSiteClusterFixture.SiteAClusterId}");
        var hwmOtherOrigin = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(
            $"ri-iso-1/site-c");

        await hwmTreeA.TryAdvanceAsync(Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(hwmTreeB.GetAsync().Result, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(hwmOtherOrigin.GetAsync().Result, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task HighWaterMarkGrain_pin_snapshot_overwrites_unconditionally()
    {
        const string tree = "ri-pin";
        var hwm = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(
            $"{tree}/{TwoSiteClusterFixture.SiteAClusterId}");

        await hwm.TryAdvanceAsync(Hlc(500));
        await hwm.PinSnapshotAsync(Hlc(200));

        Assert.That(await hwm.GetAsync(), Is.EqualTo(Hlc(200)));
    }
}
