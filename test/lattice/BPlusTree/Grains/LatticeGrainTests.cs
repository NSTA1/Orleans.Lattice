using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class LatticeGrainTests
{
    private static (LatticeGrain grain, IGrainFactory factory) CreateGrain(string treeId = "my-tree")
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var grain = new LatticeGrain(context, grainFactory);
        return (grain, grainFactory);
    }

    private static IShardRootGrain SetupShardRoot(IGrainFactory factory)
    {
        var shardRoot = Substitute.For<IShardRootGrain>();
        // GetGrain<T>(string) extension calls GetGrain<T>(string, null) on the interface.
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>())
            .Returns(shardRoot);
        return shardRoot;
    }

    [Fact]
    public async Task GetAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetAsync("k1").Returns(Encoding.UTF8.GetBytes("v1"));

        var result = await grain.GetAsync("k1");

        Assert.Equal("v1", Encoding.UTF8.GetString(result!));
        await shardRoot.Received(1).GetAsync("k1");
    }

    [Fact]
    public async Task SetAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await shardRoot.Received(1).SetAsync("k1", Arg.Any<byte[]>());
    }

    [Fact]
    public async Task DeleteAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.DeleteAsync("k1").Returns(true);

        var result = await grain.DeleteAsync("k1");

        Assert.True(result);
        await shardRoot.Received(1).DeleteAsync("k1");
    }

    [Fact]
    public void GetShardIndex_is_deterministic()
    {
        var a = LatticeGrain.GetShardIndex("hello", 64);
        var b = LatticeGrain.GetShardIndex("hello", 64);
        Assert.Equal(a, b);
    }

    [Fact]
    public void GetShardIndex_stays_in_range()
    {
        var keys = new[] { "a", "b", "foo", "bar", "customer-12345", "", "z" };
        foreach (var key in keys)
        {
            var index = LatticeGrain.GetShardIndex(key, 64);
            Assert.InRange(index, 0, 63);
        }
    }

    [Fact]
    public void GetShardIndex_distributes_across_shards()
    {
        var shards = new HashSet<int>();
        for (int i = 0; i < 1000; i++)
        {
            shards.Add(LatticeGrain.GetShardIndex($"key-{i}", 64));
        }

        Assert.True(shards.Count > 30, $"Expected >30 distinct shards, got {shards.Count}");
    }

    [Fact]
    public async Task Same_key_always_routes_to_same_shard()
    {
        var (grain1, factory1) = CreateGrain("tree-a");
        var shardRoot1 = SetupShardRoot(factory1);
        shardRoot1.GetAsync("stable-key").Returns(Encoding.UTF8.GetBytes("v1"));

        var (grain2, factory2) = CreateGrain("tree-a");
        var shardRoot2 = SetupShardRoot(factory2);
        shardRoot2.GetAsync("stable-key").Returns(Encoding.UTF8.GetBytes("v1"));

        await grain1.GetAsync("stable-key");
        await grain2.GetAsync("stable-key");

        // Both grains should have resolved the same shard key.
        var expectedShardIndex = LatticeGrain.GetShardIndex("stable-key", LatticeOptions.DefaultShardCount);
        var expectedKey = $"tree-a/{expectedShardIndex}";
        factory1.Received(1).GetGrain<IShardRootGrain>(
            Arg.Is<string>(s => s == expectedKey), Arg.Any<string>());
        factory2.Received(1).GetGrain<IShardRootGrain>(
            Arg.Is<string>(s => s == expectedKey), Arg.Any<string>());
    }
}
