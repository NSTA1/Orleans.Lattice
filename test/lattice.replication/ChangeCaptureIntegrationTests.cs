using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Integration tests for the change-feed observer registered by
/// <c>AddLatticeReplication</c>: every committed mutation must surface a
/// fully-formed <see cref="ReplogEntry"/> on the registered
/// <see cref="IReplogSink"/> before the originating grain call returns.
/// </summary>
[TestFixture]
public class ChangeCaptureIntegrationTests
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

    [Test]
    public async Task SetAsync_emits_replog_entry_with_value_and_local_origin()
    {
        const string tree = "ccap-set";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var before = _fixture.SiteASink.Entries.Count;
        await lattice.SetAsync("k", new byte[] { 1, 2, 3 });

        var entries = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Key == "k").ToArray();
        Assert.That(entries, Is.Not.Empty);
        var entry = entries[^1];
        Assert.Multiple(() =>
        {
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.Set));
            Assert.That(entry.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(entry.IsTombstone, Is.False);
            Assert.That(entry.OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteAClusterId));
            Assert.That(entry.Timestamp.WallClockTicks, Is.GreaterThan(0L));
        });
    }

    [Test]
    public async Task DeleteAsync_emits_tombstone_entry()
    {
        const string tree = "ccap-del";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("gone", new byte[] { 9 });
        var beforeDelete = _fixture.SiteASink.Entries.Count;
        await lattice.DeleteAsync("gone");

        var deletes = _fixture.SiteASink.Entries.Skip(beforeDelete)
            .Where(e => e.TreeId == tree && e.Key == "gone" && e.Op == ReplogOp.Delete)
            .ToArray();
        Assert.That(deletes, Is.Not.Empty);
        Assert.Multiple(() =>
        {
            Assert.That(deletes[^1].IsTombstone, Is.True);
            Assert.That(deletes[^1].Value, Is.Null);
            Assert.That(deletes[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteAClusterId));
        });
    }

    [Test]
    public async Task DeleteRangeAsync_emits_range_entry_per_shard()
    {
        const string tree = "ccap-range";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });

        var before = _fixture.SiteASink.Entries.Count;
        await lattice.DeleteRangeAsync("a", "z");

        var ranges = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Op == ReplogOp.DeleteRange)
            .ToArray();
        Assert.That(ranges, Is.Not.Empty);
        Assert.Multiple(() =>
        {
            Assert.That(ranges[^1].Key, Is.EqualTo("a"));
            Assert.That(ranges[^1].EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(ranges[^1].IsTombstone, Is.True);
            Assert.That(ranges[^1].OriginClusterId, Is.EqualTo(TwoSiteClusterFixture.SiteAClusterId));
        });
    }

    [Test]
    public async Task SetAsync_with_ttl_preserves_expires_at_ticks_on_replog_entry()
    {
        const string tree = "ccap-ttl";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var before = _fixture.SiteASink.Entries.Count;
        await lattice.SetAsync("kttl", new byte[] { 1 }, TimeSpan.FromMinutes(5));

        var entries = _fixture.SiteASink.Entries.Skip(before)
            .Where(e => e.TreeId == tree && e.Key == "kttl").ToArray();
        Assert.That(entries, Is.Not.Empty);
        Assert.That(entries[^1].ExpiresAtTicks, Is.GreaterThan(0L));
    }

    [Test]
    public async Task Site_b_does_not_observe_site_a_mutations()
    {
        const string tree = "ccap-iso";
        var lattice = _fixture.SiteA.Client.GetGrain<ILattice>(tree);

        var beforeB = _fixture.SiteBSink.Entries.Count;
        await lattice.SetAsync("local-only", new byte[] { 7 });

        // Site B is not wired to Site A by any transport in the fixture, so its
        // sink must not see Site A's writes.
        Assert.That(
            _fixture.SiteBSink.Entries.Skip(beforeB).Any(e => e.TreeId == tree),
            Is.False);
    }
}
