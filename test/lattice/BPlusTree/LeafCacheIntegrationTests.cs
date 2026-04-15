using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[Collection(ClusterCollection.Name)]
public class LeafCacheIntegrationTests(ClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task Cached_read_returns_value_written_through_router()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-1");
        await router.SetAsync("ck1", Encoding.UTF8.GetBytes("cached-value"));

        var result = await router.GetAsync("ck1");
        Assert.NotNull(result);
        Assert.Equal("cached-value", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Cached_read_returns_null_for_missing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-2");
        var result = await router.GetAsync("nonexistent-cached");
        Assert.Null(result);
    }

    [Fact]
    public async Task Cached_read_reflects_overwrites()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-3");
        await router.SetAsync("ow", Encoding.UTF8.GetBytes("v1"));

        // First read populates cache.
        var r1 = await router.GetAsync("ow");
        Assert.Equal("v1", Encoding.UTF8.GetString(r1!));

        // Overwrite goes directly to the primary leaf.
        await router.SetAsync("ow", Encoding.UTF8.GetBytes("v2"));

        // Second read should get the updated value via delta refresh.
        var r2 = await router.GetAsync("ow");
        Assert.Equal("v2", Encoding.UTF8.GetString(r2!));
    }

    [Fact]
    public async Task Cached_read_returns_null_after_delete()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-4");
        await router.SetAsync("del-cached", Encoding.UTF8.GetBytes("exists"));

        // Populate cache.
        var before = await router.GetAsync("del-cached");
        Assert.NotNull(before);

        await router.DeleteAsync("del-cached");

        // Cache should pick up the tombstone via delta refresh.
        var after = await router.GetAsync("del-cached");
        Assert.Null(after);
    }

    [Fact]
    public async Task Multiple_keys_through_cache_are_independent()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("cache-test-6");
        await router.SetAsync("ca", Encoding.UTF8.GetBytes("a"));
        await router.SetAsync("cb", Encoding.UTF8.GetBytes("b"));
        await router.SetAsync("cc", Encoding.UTF8.GetBytes("c"));

        Assert.Equal("a", Encoding.UTF8.GetString((await router.GetAsync("ca"))!));
        Assert.Equal("b", Encoding.UTF8.GetString((await router.GetAsync("cb"))!));
        Assert.Equal("c", Encoding.UTF8.GetString((await router.GetAsync("cc"))!));
    }
}
