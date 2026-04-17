using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class LeafCacheIntegrationTests
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
    public async Task Cached_read_returns_value_written_through_router()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-1");
        await router.SetAsync("ck1", Encoding.UTF8.GetBytes("cached-value"));

        var result = await router.GetAsync("ck1");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("cached-value"));
    }

    [Test]
    public async Task Cached_read_returns_null_for_missing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-2");
        var result = await router.GetAsync("nonexistent-cached");
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Cached_read_reflects_overwrites()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-3");
        await router.SetAsync("ow", Encoding.UTF8.GetBytes("v1"));

        // First read populates cache.
        var r1 = await router.GetAsync("ow");
        Assert.That(Encoding.UTF8.GetString(r1!), Is.EqualTo("v1"));

        // Overwrite goes directly to the primary leaf.
        await router.SetAsync("ow", Encoding.UTF8.GetBytes("v2"));

        // Second read should get the updated value via delta refresh.
        var r2 = await router.GetAsync("ow");
        Assert.That(Encoding.UTF8.GetString(r2!), Is.EqualTo("v2"));
    }

    [Test]
    public async Task Cached_read_returns_null_after_delete()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-4");
        await router.SetAsync("del-cached", Encoding.UTF8.GetBytes("exists"));

        // Populate cache.
        var before = await router.GetAsync("del-cached");
        Assert.That(before, Is.Not.Null);

        await router.DeleteAsync("del-cached");

        // Cache should pick up the tombstone via delta refresh.
        var after = await router.GetAsync("del-cached");
        Assert.That(after, Is.Null);
    }

    [Test]
    public async Task Multiple_keys_through_cache_are_independent()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-6");
        await router.SetAsync("ca", Encoding.UTF8.GetBytes("a"));
        await router.SetAsync("cb", Encoding.UTF8.GetBytes("b"));
        await router.SetAsync("cc", Encoding.UTF8.GetBytes("c"));

        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("ca"))!), Is.EqualTo("a"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("cb"))!), Is.EqualTo("b"));
        Assert.That(Encoding.UTF8.GetString((await router.GetAsync("cc"))!), Is.EqualTo("c"));
    }
}
