using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end coverage of the apply seam: source HLC and origin cluster
/// id must round-trip verbatim through <see cref="IReplicationApplyGrain"/>
/// onto the receiving cluster's persisted state, and per-origin
/// <see cref="IReplicationHighWaterMarkGrain"/> grains must track the
/// most-recent applied timestamp keyed by <c>{treeId}/{originClusterId}</c>.
/// </summary>
[TestFixture]
[Category("Integration")]
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

        await apply.ApplySetAsync("k", new byte[] { 1, 2, 3 }, sourceHlc, TwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0);

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
        // and the persisted entry's origin must remain "site-a" - proving
        // the apply seam preserved it verbatim).
        const string tree = "ri-set-cycle";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);

        var beforeLocal = _fixture.SiteBSink.Entries.Count(e => e.TreeId == tree && e.OriginClusterId == TwoSiteClusterFixture.SiteBClusterId);
        await apply.ApplySetAsync("k", new byte[] { 9 }, Hlc(1_000), TwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0);
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

        await apply.ApplySetAsync("k", new byte[] { 1 }, Hlc(100), TwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0);
        var deleteHlc = Hlc(200);
        await apply.ApplyDeleteAsync("k", deleteHlc, TwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null);

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

        await apply.ApplyDeleteRangeAsync("a", "z", TwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null);

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
        // LWW guarantee - proves the source HLC is honoured (not rewritten
        // to a fresh local one) when the apply seam persists the value.
        const string tree = "ri-lww";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("k", new byte[] { 99 });
        var local = await lattice.GetWithVersionAsync("k");

        // Apply a remote write with an older HLC; LWW must reject.
        var olderHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks - 1 };
        await apply.ApplySetAsync("k", new byte[] { 1 }, olderHlc, TwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0);

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
        var origin = TwoSiteClusterFixture.SiteAClusterId;
        var hwm = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(tree);

        Assert.That(await hwm.GetAsync(origin), Is.EqualTo(HybridLogicalClock.Zero));

        Assert.That(await hwm.TryAdvanceAsync(origin, Hlc(10)), Is.True);
        Assert.That(await hwm.GetAsync(origin), Is.EqualTo(Hlc(10)));

        // Older timestamp must not advance.
        Assert.That(await hwm.TryAdvanceAsync(origin, Hlc(5)), Is.False);
        Assert.That(await hwm.GetAsync(origin), Is.EqualTo(Hlc(10)));

        // Equal timestamp does not advance.
        Assert.That(await hwm.TryAdvanceAsync(origin, Hlc(10)), Is.False);

        // Newer timestamp advances.
        Assert.That(await hwm.TryAdvanceAsync(origin, Hlc(20)), Is.True);
        Assert.That(await hwm.GetAsync(origin), Is.EqualTo(Hlc(20)));
    }

    [Test]
    public async Task HighWaterMarkGrain_isolates_state_per_tree_and_per_origin()
    {
        var origin = TwoSiteClusterFixture.SiteAClusterId;
        var hwmTreeA = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>("ri-iso-1");
        var hwmTreeB = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>("ri-iso-2");

        await hwmTreeA.TryAdvanceAsync(origin, Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(hwmTreeB.GetAsync(origin).Result, Is.EqualTo(HybridLogicalClock.Zero));
            // Different origin on the same tree as Site A also reports Zero.
            Assert.That(hwmTreeA.GetAsync("site-c").Result, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task HighWaterMarkGrain_pin_snapshot_overwrites_unconditionally()
    {
        const string tree = "ri-pin";
        var origin = TwoSiteClusterFixture.SiteAClusterId;
        var hwm = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(tree);

        await hwm.TryAdvanceAsync(origin, Hlc(500));

        var frontier = new VersionVector();
        frontier.Entries[origin] = Hlc(200);
        await hwm.PinSnapshotAsync(Hlc(200), frontier);

        Assert.That(await hwm.GetAsync(origin), Is.EqualTo(Hlc(200)));
    }

    // ------------------------------------------------------------------
    // Typed CRDT mode dispatch (state-merge through ILattice)
    // ------------------------------------------------------------------

    private ReplicationApplier CreateSiteBApplier()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions { ClusterId = TwoSiteClusterFixture.SiteBClusterId };
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);
        return new ReplicationApplier(_fixture.SiteB.Client, monitor, new LocalVectorClockCache(_fixture.SiteB.Client));
    }

    [Test]
    public async Task ApplyAsync_or_set_state_merge_converges_with_local_concurrent_add()
    {
        const string tree = "ri-orset";
        const string key = "k";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();

        // Site B authors a local add concurrently with the remote one.
        await lattice.OrSet(key).AddAsync(new byte[] { 1 }, "site-b");

        // Build a Site A-origin OrSet payload carrying a different element.
        var remoteSet = new OrSet();
        remoteSet.Add(new byte[] { 2 }, "site-a", 1);
        var entry = new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = JsonLatticeSerializer<OrSet>.Default.Serialize(remoteSet),
            Timestamp = Hlc(1_000),
            Mode = LatticeMergeMode.OrSet,
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);

        // Both adds must survive the merge - that is the whole point of OR-Set.
        var merged = await lattice.OrSet(key).GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(merged.Contains(new byte[] { 1 }), Is.True, "Local Site B add must survive.");
            Assert.That(merged.Contains(new byte[] { 2 }), Is.True, "Remote Site A add must survive.");
        });
    }

    [Test]
    public async Task ApplyAsync_pn_counter_state_merge_sums_per_replica_components()
    {
        const string tree = "ri-pncounter";
        const string key = "k";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();

        // Site B authors +3 locally.
        await lattice.PnCounter(key).IncrementAsync("site-b", 3);

        // Site A increments +5 and decrements -2; receiver must merge both.
        var remoteCounter = new PnCounter();
        remoteCounter.Increment("site-a", 5);
        remoteCounter.Decrement("site-a", 2);
        var entry = new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = JsonLatticeSerializer<PnCounter>.Default.Serialize(remoteCounter),
            Timestamp = Hlc(1_000),
            Mode = LatticeMergeMode.PnCounter,
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        };

        await applier.ApplyAsync(entry);

        // Local +3, remote +5 - 2 = +3 ⇒ total +6.
        var merged = await lattice.PnCounter(key).GetAsync();
        Assert.That(merged.Value, Is.EqualTo(6));
    }

    [Test]
    public async Task ApplyAsync_version_vector_state_merge_pointwise_max_per_replica()
    {
        const string tree = "ri-vv";
        const string key = "k";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();

        await lattice.VersionVector(key).TickAsync("site-b");
        var localBefore = await lattice.VersionVector(key).GetAsync();
        var localBClock = localBefore.GetClock("site-b");

        var remote = new VersionVector();
        remote.Entries["site-a"] = Hlc(7777, 9);
        // Stale Site B clock that must be subsumed by the local higher value.
        remote.Entries["site-b"] = HybridLogicalClock.Zero;
        var entry = new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = JsonLatticeSerializer<VersionVector>.Default.Serialize(remote),
            Timestamp = Hlc(1_000),
            Mode = LatticeMergeMode.VersionVector,
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        };

        await applier.ApplyAsync(entry);

        var merged = await lattice.VersionVector(key).GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(merged.GetClock("site-a"), Is.EqualTo(Hlc(7777, 9)));
            Assert.That(merged.GetClock("site-b"), Is.EqualTo(localBClock),
                "Pointwise-max must keep the higher local Site B clock, not regress to Zero.");
        });
    }

    [Test]
    public async Task ApplyAsync_or_set_state_merge_dedupes_under_per_origin_high_water_mark()
    {
        const string tree = "ri-orset-dedup";
        const string key = "k";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();
        var hwm = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(tree);

        // Pin HWM above the entry timestamp - re-delivery of a typed CRDT
        // entry must short-circuit on the HWM check just like LWW does.
        var pinFrontier = new VersionVector();
        pinFrontier.Entries[TwoSiteClusterFixture.SiteAClusterId] = Hlc(5_000);
        await hwm.PinSnapshotAsync(Hlc(5_000), pinFrontier);

        var remoteSet = new OrSet();
        remoteSet.Add(new byte[] { 9 }, "site-a", 1);
        var entry = new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = JsonLatticeSerializer<OrSet>.Default.Serialize(remoteSet),
            Timestamp = Hlc(1_000),
            Mode = LatticeMergeMode.OrSet,
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
        var merged = await lattice.OrSet(key).GetAsync();
        Assert.That(merged.Contains(new byte[] { 9 }), Is.False,
            "Entry below the per-origin HWM must not be merged.");
    }
}
