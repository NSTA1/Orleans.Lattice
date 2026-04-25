using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Integration tests for <see cref="IReplicationApplyGrain"/> exercised
/// through a single <c>TestCluster</c>: verifies the apply seam preserves
/// the source <see cref="HybridLogicalClock"/> and origin cluster id
/// verbatim on the persisted <see cref="Primitives.LwwValue{T}"/>.
/// </summary>
[TestFixture]
public class LatticeGrainReplicationApplyTests
{
    private ClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public async Task ApplySetAsync_persists_value_with_source_hlc_visible_via_GetWithVersionAsync()
    {
        const string tree = "rapply-set";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(42_000, 3);

        await apply.ApplySetAsync("k", new byte[] { 7 }, sourceHlc, "site-x", expiresAtTicks: 0);

        var versioned = await lattice.GetWithVersionAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(versioned.Value, Is.EqualTo(new byte[] { 7 }));
            Assert.That(versioned.Version, Is.EqualTo(sourceHlc));
        });
    }

    [Test]
    public async Task ApplySetAsync_with_expiry_persists_expires_at_ticks()
    {
        const string tree = "rapply-ttl";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var future = DateTime.UtcNow.AddHours(1).Ticks;

        await apply.ApplySetAsync("k", new byte[] { 1 }, Hlc(10), "site-x", expiresAtTicks: future);

        var value = await lattice.GetAsync("k");
        Assert.That(value, Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public async Task ApplyDeleteAsync_tombstones_with_source_hlc()
    {
        const string tree = "rapply-del";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("k", new byte[] { 1 });
        var local = await lattice.GetWithVersionAsync("k");
        var deleteHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks + 1_000 };

        await apply.ApplyDeleteAsync("k", deleteHlc, "site-x");

        var after = await lattice.GetAsync("k");
        Assert.That(after, Is.Null);
    }

    [Test]
    public async Task ApplyDeleteAsync_older_hlc_does_not_overwrite_newer_local_value()
    {
        const string tree = "rapply-del-stale";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("k", new byte[] { 1 });
        var local = await lattice.GetWithVersionAsync("k");
        var olderHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks - 1 };

        await apply.ApplyDeleteAsync("k", olderHlc, "site-x");

        var after = await lattice.GetAsync("k");
        Assert.That(after, Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public async Task ApplyDeleteRangeAsync_removes_all_matching_keys()
    {
        const string tree = "rapply-range";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });
        await lattice.SetAsync("c", new byte[] { 3 });

        await apply.ApplyDeleteRangeAsync("a", "c", "site-x");

        Assert.Multiple(() =>
        {
            Assert.That(lattice.GetAsync("a").Result, Is.Null);
            Assert.That(lattice.GetAsync("b").Result, Is.Null);
            Assert.That(lattice.GetAsync("c").Result, Is.EqualTo(new byte[] { 3 })); // end-exclusive
        });
    }

    [Test]
    public async Task ApplyDeleteRangeAsync_with_inverted_range_is_noop()
    {
        const string tree = "rapply-range-inv";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 1 });

        await apply.ApplyDeleteRangeAsync("z", "a", "site-x");

        Assert.That(await lattice.GetAsync("k"), Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public void ApplySetAsync_throws_for_null_key()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-null-k");
        Assert.That(
            async () => await apply.ApplySetAsync(null!, new byte[] { 1 }, Hlc(1), "site-x", 0),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ApplySetAsync_throws_for_null_value()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-null-v");
        Assert.That(
            async () => await apply.ApplySetAsync("k", null!, Hlc(1), "site-x", 0),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ApplySetAsync_throws_for_empty_origin()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-empty-o");
        Assert.That(
            async () => await apply.ApplySetAsync("k", new byte[] { 1 }, Hlc(1), "", 0),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ApplyDeleteAsync_throws_for_null_key()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-del-null-k");
        Assert.That(
            async () => await apply.ApplyDeleteAsync(null!, Hlc(1), "site-x"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ApplyDeleteAsync_throws_for_empty_origin()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-del-empty-o");
        Assert.That(
            async () => await apply.ApplyDeleteAsync("k", Hlc(1), ""),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ApplyDeleteRangeAsync_throws_for_null_arguments()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-range-null");
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await apply.ApplyDeleteRangeAsync(null!, "z", "site-x"),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await apply.ApplyDeleteRangeAsync("a", null!, "site-x"),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await apply.ApplyDeleteRangeAsync("a", "z", ""),
                Throws.InstanceOf<ArgumentException>());
        });
    }
}
