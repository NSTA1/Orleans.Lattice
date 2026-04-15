using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

public sealed class ClusterFixture : IAsyncLifetime
{
    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddMemoryGrainStorage("bplustree");
        }
    }
}

[CollectionDefinition(Name)]
public class ClusterCollection : ICollectionFixture<ClusterFixture>
{
    public const string Name = "ClusterCollection";
}

[Collection(ClusterCollection.Name)]
public class BPlusTreeIntegrationTests(ClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task Set_and_Get_roundtrips_a_value()
    {
        var router = _cluster.GrainFactory.GetGrain<IShardRouterGrain>("test-tree");
        var value = Encoding.UTF8.GetBytes("hello-world");

        await router.SetAsync("key1", value);
        var result = await router.GetAsync("key1");

        Assert.NotNull(result);
        Assert.Equal("hello-world", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Get_returns_null_for_missing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<IShardRouterGrain>("test-tree-miss");
        var result = await router.GetAsync("nonexistent");
        Assert.Null(result);
    }

    [Fact]
    public async Task Delete_returns_false_for_missing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<IShardRouterGrain>("test-tree-del-miss");
        var result = await router.DeleteAsync("nonexistent");
        Assert.False(result);
    }

    [Fact]
    public async Task Delete_removes_a_previously_set_key()
    {
        var router = _cluster.GrainFactory.GetGrain<IShardRouterGrain>("test-tree-del");
        await router.SetAsync("to-delete", Encoding.UTF8.GetBytes("value"));

        var deleted = await router.DeleteAsync("to-delete");
        Assert.True(deleted);

        var result = await router.GetAsync("to-delete");
        Assert.Null(result);
    }

    [Fact]
    public async Task Set_overwrites_existing_value()
    {
        var router = _cluster.GrainFactory.GetGrain<IShardRouterGrain>("test-tree-overwrite");
        await router.SetAsync("k", Encoding.UTF8.GetBytes("v1"));
        await router.SetAsync("k", Encoding.UTF8.GetBytes("v2"));

        var result = await router.GetAsync("k");
        Assert.NotNull(result);
        Assert.Equal("v2", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Multiple_keys_in_same_shard_are_independent()
    {
        var router = _cluster.GrainFactory.GetGrain<IShardRouterGrain>("test-tree-multi");
        await router.SetAsync("alpha", Encoding.UTF8.GetBytes("a"));
        await router.SetAsync("bravo", Encoding.UTF8.GetBytes("b"));
        await router.SetAsync("charlie", Encoding.UTF8.GetBytes("c"));

        Assert.Equal("a", Encoding.UTF8.GetString((await router.GetAsync("alpha"))!));
        Assert.Equal("b", Encoding.UTF8.GetString((await router.GetAsync("bravo"))!));
        Assert.Equal("c", Encoding.UTF8.GetString((await router.GetAsync("charlie"))!));
    }
}
