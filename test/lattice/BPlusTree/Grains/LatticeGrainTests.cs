using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class LatticeGrainTests
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

    // --- Compaction reminder registration tests ---

    private static ITombstoneCompactionGrain SetupCompactionGrain(IGrainFactory factory, string treeId)
    {
        var compaction = Substitute.For<ITombstoneCompactionGrain>();
        factory.GetGrain<ITombstoneCompactionGrain>(treeId, Arg.Any<string>())
            .Returns(compaction);
        return compaction;
    }

    [Fact]
    public async Task SetAsync_CallsEnsureReminderAsync()
    {
        const string treeId = "compaction-set";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.SetAsync("k1", [1, 2, 3]);

        await compaction.Received(1).EnsureReminderAsync();
    }

    [Fact]
    public async Task DeleteAsync_CallsEnsureReminderAsync()
    {
        const string treeId = "compaction-delete";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.DeleteAsync("k1");

        await compaction.Received(1).EnsureReminderAsync();
    }

    [Fact]
    public async Task GetAsync_DoesNotCallEnsureReminderAsync()
    {
        const string treeId = "compaction-get";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.GetAsync("k1");

        await compaction.DidNotReceive().EnsureReminderAsync();
    }

    [Fact]
    public async Task SetAsync_WhenCompactionDisabled_DoesNotCallEnsureReminderAsync()
    {
        const string treeId = "compaction-disabled";
        var (grain, factory) = CreateGrain(treeId, new LatticeOptions
        {
            TombstoneGracePeriod = Timeout.InfiniteTimeSpan
        });
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.SetAsync("k1", [1, 2, 3]);

        await compaction.DidNotReceive().EnsureReminderAsync();
    }

    [Fact]
    public async Task SetAsync_SecondCall_DoesNotCallEnsureReminderAgain()
    {
        const string treeId = "compaction-dedup";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.SetAsync("k1", [1]);
        await grain.SetAsync("k2", [2]);

        await compaction.Received(1).EnsureReminderAsync();
    }

    // --- ExistsAsync tests ---

    [Fact]
    public async Task ExistsAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.ExistsAsync("k1").Returns(true);

        var result = await grain.ExistsAsync("k1");

        Assert.True(result);
        await shardRoot.Received(1).ExistsAsync("k1");
    }

    [Fact]
    public async Task ExistsAsync_returns_false_for_missing_key()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.ExistsAsync("missing").Returns(false);

        var result = await grain.ExistsAsync("missing");

        Assert.False(result);
    }

    // --- GetManyAsync tests ---

    [Fact]
    public async Task GetManyAsync_returns_existing_keys()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetAsync("k1").Returns(Encoding.UTF8.GetBytes("v1"));
        shardRoot.GetAsync("k2").Returns(Encoding.UTF8.GetBytes("v2"));

        var result = await grain.GetManyAsync(["k1", "k2"]);

        Assert.Equal(2, result.Count);
        Assert.Equal("v1", Encoding.UTF8.GetString(result["k1"]));
        Assert.Equal("v2", Encoding.UTF8.GetString(result["k2"]));
    }

    [Fact]
    public async Task GetManyAsync_omits_missing_keys()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetAsync("k1").Returns(Encoding.UTF8.GetBytes("v1"));
        shardRoot.GetAsync("k2").Returns((byte[]?)null);

        var result = await grain.GetManyAsync(["k1", "k2"]);

        Assert.Single(result);
        Assert.True(result.ContainsKey("k1"));
        Assert.False(result.ContainsKey("k2"));
    }

    [Fact]
    public async Task GetManyAsync_returns_empty_for_no_keys()
    {
        var (grain, factory) = CreateGrain();
        SetupShardRoot(factory);

        var result = await grain.GetManyAsync([]);

        Assert.Empty(result);
    }

    // --- SetManyAsync tests ---

    [Fact]
    public async Task SetManyAsync_delegates_all_entries_to_shard()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, "my-tree");

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", Encoding.UTF8.GetBytes("v1")),
            new("k2", Encoding.UTF8.GetBytes("v2")),
        };

        await grain.SetManyAsync(entries);

        await shardRoot.Received(1).SetAsync("k1", Arg.Any<byte[]>());
        await shardRoot.Received(1).SetAsync("k2", Arg.Any<byte[]>());
    }

    [Fact]
    public async Task SetManyAsync_CallsEnsureReminderAsync()
    {
        const string treeId = "compaction-setmany";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.SetManyAsync([new("k1", [1])]);

        await compaction.Received(1).EnsureReminderAsync();
    }
}
