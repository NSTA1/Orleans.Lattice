using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class CasIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task GetWithVersion_returns_zero_for_missing_key()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("cas-missing");
        var result = await tree.GetWithVersionAsync("nonexistent");
        Assert.That(result.Value, Is.Null);
        Assert.That(result.Version, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task GetWithVersion_returns_value_and_version()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("cas-get-ver");
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var result = await tree.GetWithVersionAsync("k1");
        Assert.That(result.Value, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result.Value!), Is.EqualTo("v1"));
        Assert.That(result.Version, Is.Not.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task SetIfVersion_succeeds_with_correct_version()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("cas-success");
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var versioned = await tree.GetWithVersionAsync("k1");
        var success = await tree.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v2"), versioned.Version);

        Assert.That(success, Is.True);
        var readBack = await tree.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("v2"));
    }

    [Test]
    public async Task SetIfVersion_fails_with_stale_version()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("cas-fail");
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var versioned = await tree.GetWithVersionAsync("k1");
        // Another writer updates the key.
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1-updated"));

        // CAS with stale version should fail.
        var success = await tree.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v2"), versioned.Version);

        Assert.That(success, Is.False);
        var readBack = await tree.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("v1-updated"));
    }

    [Test]
    public async Task SetIfVersion_creates_new_key_with_zero_version()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("cas-new-key");
        var success = await tree.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v1"), HybridLogicalClock.Zero);

        Assert.That(success, Is.True);
        var readBack = await tree.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task SetIfVersion_fails_creating_existing_key_with_zero_version()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("cas-existing-zero");
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var success = await tree.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v2"), HybridLogicalClock.Zero);

        Assert.That(success, Is.False);
        var readBack = await tree.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task SetIfVersion_typed_roundtrip()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("cas-typed");
        await tree.SetAsync("k1", "hello");

        var versioned = await tree.GetWithVersionAsync<string>("k1");
        Assert.That(versioned.Value, Is.EqualTo("hello"));

        var success = await tree.SetIfVersionAsync("k1", "world", versioned.Version);
        Assert.That(success, Is.True);

        var readBack = await tree.GetAsync<string>("k1");
        Assert.That(readBack, Is.EqualTo("world"));
    }
}