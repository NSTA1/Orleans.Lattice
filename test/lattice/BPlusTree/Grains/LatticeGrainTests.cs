using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    private static (LatticeGrain grain, IGrainFactory factory) CreateGrain(
        string treeId = "my-tree",
        LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        // Setup registry mock for alias resolution (returns treeId itself — no alias).
        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(callInfo => Task.FromResult(callInfo.Arg<string>()));

        var grain = new LatticeGrain(context, grainFactory, optionsMonitor);
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

    [Test]
    public async Task GetAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetAsync("k1").Returns(Encoding.UTF8.GetBytes("v1"));

        var result = await grain.GetAsync("k1");

        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("v1"));
        await shardRoot.Received(1).GetAsync("k1");
    }

    [Test]
    public async Task SetAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await shardRoot.Received(1).SetAsync("k1", Arg.Any<byte[]>());
    }

    [Test]
    public async Task DeleteAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.DeleteAsync("k1").Returns(true);

        var result = await grain.DeleteAsync("k1");

        Assert.That(result, Is.True);
        await shardRoot.Received(1).DeleteAsync("k1");
    }

    [Test]
    public void GetShardIndex_is_deterministic()
    {
        var a = LatticeGrain.GetShardIndex("hello", 64);
        var b = LatticeGrain.GetShardIndex("hello", 64);
        Assert.That(b, Is.EqualTo(a));
    }

    [Test]
    public void GetShardIndex_stays_in_range()
    {
        var keys = new[] { "a", "b", "foo", "bar", "customer-12345", "", "z" };
        foreach (var key in keys)
        {
            var index = LatticeGrain.GetShardIndex(key, 64);
            Assert.That(index, Is.InRange(0, 63));
        }
    }

    [Test]
    public void GetShardIndex_distributes_across_shards()
    {
        var shards = new HashSet<int>();
        for (int i = 0; i < 1000; i++)
        {
            shards.Add(LatticeGrain.GetShardIndex($"key-{i}", 64));
        }

        Assert.That(shards.Count > 30, Is.True, $"Expected >30 distinct shards, got {shards.Count}");
    }

    [Test]
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
