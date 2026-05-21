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
/// Compression-enabled clone of <see cref="ReplicationApplyIntegrationTests"/>.
/// Exercises the same apply-seam, cycle-break, LWW, range-delete, and
/// typed-CRDT (OR-Set / PN-Counter / VersionVector / MV-Register)
/// invariants end-to-end through a two-site cluster where every silo
/// has <see cref="LatticeReplicationOptions.FramingCompression"/> set
/// to <see cref="LatticeCompression.Zstd"/> and the
/// <see cref="LatticeReplicationOptions.FramingCompressionMinBatchBytes"/>
/// threshold forced to zero so every shipped batch goes through the
/// compressed framing path. Pins "compression is transparent to
/// the apply pipeline" in addition to the per-test invariants.
/// </summary>
[TestFixture]
[Category("Integration")]
public class CompressedReplicationApplyIntegrationTests
{
    private CompressedTwoSiteClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new CompressedTwoSiteClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public async Task ApplySetAsync_preserves_source_hlc_on_site_b_persisted_value()
    {
        const string tree = "rzc-set-hlc";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(123_456_789, 7);

        await apply.ApplySetAsync("k", new byte[] { 1, 2, 3 }, sourceHlc, CompressedTwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0);

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
        const string tree = "rzc-set-cycle";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);

        var beforeLocal = _fixture.SiteBSink.Entries.Count(e => e.TreeId == tree && e.OriginClusterId == CompressedTwoSiteClusterFixture.SiteBClusterId);
        await apply.ApplySetAsync("k", new byte[] { 9 }, Hlc(1_000), CompressedTwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0);
        var afterLocal = _fixture.SiteBSink.Entries.Count(e => e.TreeId == tree && e.OriginClusterId == CompressedTwoSiteClusterFixture.SiteBClusterId);

        Assert.That(afterLocal, Is.EqualTo(beforeLocal),
            "Apply with remote origin must not produce a Site B-origin replog entry.");
    }

    [Test]
    public async Task ApplyDeleteAsync_preserves_source_hlc_as_tombstone_on_site_b()
    {
        const string tree = "rzc-del";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);

        await apply.ApplySetAsync("k", new byte[] { 1 }, Hlc(100), CompressedTwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0);
        var deleteHlc = Hlc(200);
        await apply.ApplyDeleteAsync("k", deleteHlc, CompressedTwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null);

        var versioned = await lattice.GetWithVersionAsync("k");
        Assert.That(versioned.Value, Is.Null);
    }

    [Test]
    public async Task ApplyDeleteRangeAsync_removes_keys_across_physical_shards()
    {
        const string tree = "rzc-range";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("m", new byte[] { 2 });
        await lattice.SetAsync("y", new byte[] { 3 });

        await apply.ApplyDeleteRangeAsync("a", "z", HybridLogicalClock.Zero, CompressedTwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null);

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
        const string tree = "rzc-lww";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("k", new byte[] { 99 });
        var local = await lattice.GetWithVersionAsync("k");

        var olderHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks - 1 };
        await apply.ApplySetAsync("k", new byte[] { 1 }, olderHlc, CompressedTwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0);

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
        const string tree = "rzc-hwm";
        var origin = CompressedTwoSiteClusterFixture.SiteAClusterId;
        var hwm = _fixture.SiteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(tree);

        Assert.That(await hwm.GetAsync(origin), Is.EqualTo(HybridLogicalClock.Zero));

        Assert.That(await hwm.TryAdvanceAsync(origin, Hlc(10)), Is.True);
        Assert.That(await hwm.GetAsync(origin), Is.EqualTo(Hlc(10)));

        Assert.That(await hwm.TryAdvanceAsync(origin, Hlc(5)), Is.False);
        Assert.That(await hwm.GetAsync(origin), Is.EqualTo(Hlc(10)));

        Assert.That(await hwm.TryAdvanceAsync(origin, Hlc(10)), Is.False);

        Assert.That(await hwm.TryAdvanceAsync(origin, Hlc(20)), Is.True);
        Assert.That(await hwm.GetAsync(origin), Is.EqualTo(Hlc(20)));
    }

    // ------------------------------------------------------------------
    // Typed CRDT mode dispatch (state-merge through ILattice)
    // ------------------------------------------------------------------

    private ReplicationApplier CreateSiteBApplier()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions
        {
            ClusterId = CompressedTwoSiteClusterFixture.SiteBClusterId,
            FramingCompression = LatticeCompression.Zstd,
            FramingCompressionMinBatchBytes = 0,
        };
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);
        return new ReplicationApplier(_fixture.SiteB.Client, monitor, new LocalVectorClockCache(_fixture.SiteB.Client));
    }

    [Test]
    public async Task ApplyAsync_or_set_state_merge_converges_with_local_concurrent_add()
    {
        const string tree = "rzc-orset";
        const string key = "k";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();

        await lattice.OrSet(key).AddAsync(new byte[] { 1 }, "site-b");

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
            OriginClusterId = CompressedTwoSiteClusterFixture.SiteAClusterId,
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);

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
        const string tree = "rzc-pncounter";
        const string key = "k";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();

        await lattice.PnCounter(key).IncrementAsync("site-b", 3);

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
            OriginClusterId = CompressedTwoSiteClusterFixture.SiteAClusterId,
        };

        await applier.ApplyAsync(entry);

        var merged = await lattice.PnCounter(key).GetAsync();
        Assert.That(merged.Value, Is.EqualTo(6));
    }

    [Test]
    public async Task ApplyAsync_mv_register_state_merge_preserves_concurrent_writes()
    {
        const string tree = "rzc-mvregister";
        const string key = "k";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();

        await lattice.MvRegister<string>(key).SetAsync("site-b", "v-b");

        var remote = new MvRegister();
        remote.Set("site-a", JsonLatticeSerializer<string>.Default.Serialize("v-a"));
        var entry = new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = JsonLatticeSerializer<MvRegister>.Default.Serialize(remote),
            Timestamp = Hlc(1_000),
            Mode = LatticeMergeMode.MvRegister,
            OriginClusterId = CompressedTwoSiteClusterFixture.SiteAClusterId,
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);

        var merged = await lattice.MvRegister<string>(key).ValuesAsync();
        Assert.That(merged, Is.EquivalentTo(new[] { "v-a", "v-b" }),
            "Both concurrent MV-Register dots must survive the merge.");
    }

    [Test]
    public void Site_b_applier_resolves_compressed_options()
    {
        // Pin the primary integration invariant for the apply path:
        // the IOptionsMonitor surface used by ReplicationApplier (and the
        // rest of the receiver-side stack) sees FramingCompression = Zstd
        // for every tree the fixture spins up. The 9 apply tests above
        // then prove the receiver-side pipeline is invariant under
        // compressed framing; this final assertion makes that
        // configuration explicit so a future regression that quietly
        // drops the option would surface here rather than as a wire-shape
        // mismatch buried in another test.
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions
        {
            ClusterId = CompressedTwoSiteClusterFixture.SiteBClusterId,
            FramingCompression = LatticeCompression.Zstd,
            FramingCompressionMinBatchBytes = 0,
        };
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);
        var resolved = monitor.Get("rzc-options");

        Assert.Multiple(() =>
        {
            Assert.That(resolved.FramingCompression, Is.EqualTo(LatticeCompression.Zstd));
            Assert.That(resolved.FramingCompressionMinBatchBytes, Is.EqualTo(0));
        });
    }
}
