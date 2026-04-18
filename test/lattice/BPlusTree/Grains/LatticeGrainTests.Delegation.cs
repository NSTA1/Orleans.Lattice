using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- Null validation tests ---

    [Test]
    public void GetAsync_throws_on_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.GetAsync(null!));
    }

    [Test]
    public void SetAsync_throws_on_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.SetAsync(null!, [1]));
    }

    [Test]
    public void SetAsync_throws_on_null_value()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.SetAsync("k1", null!));
    }

    [Test]
    public void DeleteAsync_throws_on_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.DeleteAsync(null!));
    }

    [Test]
    public void BulkLoadAsync_throws_on_null_entries()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.BulkLoadAsync(null!));
    }

    // --- GetOrSetAsync tests ---

    [Test]
    public void GetOrSetAsync_throws_on_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.GetOrSetAsync(null!, [1]));
    }

    [Test]
    public void GetOrSetAsync_throws_on_null_value()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.GetOrSetAsync("k1", null!));
    }

    [Test]
    public async Task GetOrSetAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetOrSetAsync("k1", Arg.Any<byte[]>())
            .Returns((byte[]?)null);

        var result = await grain.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(result, Is.Null);
        await shardRoot.Received(1).GetOrSetAsync("k1", Arg.Any<byte[]>());
    }

    [Test]
    public async Task GetOrSetAsync_returns_existing_value_from_shard()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetOrSetAsync("k1", Arg.Any<byte[]>())
            .Returns(Encoding.UTF8.GetBytes("existing"));

        var result = await grain.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("ignored"));

        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("existing"));
    }

    // --- ExistsAsync tests ---

    [Test]
    public async Task ExistsAsync_throws_on_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.ExistsAsync(null!));
    }

    [Test]
    public async Task ExistsAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.ExistsAsync("k1").Returns(true);

        var result = await grain.ExistsAsync("k1");

        Assert.That(result, Is.True);
        await shardRoot.Received(1).ExistsAsync("k1");
    }

    [Test]
    public async Task ExistsAsync_returns_false_for_missing_key()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.ExistsAsync("missing").Returns(false);

        var result = await grain.ExistsAsync("missing");

        Assert.That(result, Is.False);
    }

    // --- GetManyAsync tests ---

    [Test]
    public async Task GetManyAsync_throws_on_null_keys()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.GetManyAsync(null!));
    }

    [Test]
    public async Task GetManyAsync_returns_existing_keys()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetManyAsync(Arg.Any<List<string>>()).Returns(callInfo =>
        {
            var keys = callInfo.Arg<List<string>>();
            var result = new Dictionary<string, byte[]>();
            foreach (var key in keys)
            {
                if (key == "k1") result[key] = Encoding.UTF8.GetBytes("v1");
                if (key == "k2") result[key] = Encoding.UTF8.GetBytes("v2");
            }
            return result;
        });

        var result = await grain.GetManyAsync(["k1", "k2"]);

        Assert.That(result.Count, Is.EqualTo(2));
        Assert.That(Encoding.UTF8.GetString(result["k1"]), Is.EqualTo("v1"));
        Assert.That(Encoding.UTF8.GetString(result["k2"]), Is.EqualTo("v2"));
    }

    [Test]
    public async Task GetManyAsync_omits_missing_keys()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetManyAsync(Arg.Any<List<string>>()).Returns(callInfo =>
        {
            var keys = callInfo.Arg<List<string>>();
            var result = new Dictionary<string, byte[]>();
            foreach (var key in keys)
            {
                if (key == "k1") result[key] = Encoding.UTF8.GetBytes("v1");
                // k2 is "missing" — not added to result
            }
            return result;
        });

        var result = await grain.GetManyAsync(["k1", "k2"]);

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result.ContainsKey("k1"), Is.True);
        Assert.That(result.ContainsKey("k2"), Is.False);
    }

    [Test]
    public async Task GetManyAsync_returns_empty_for_no_keys()
    {
        var (grain, factory) = CreateGrain();
        SetupShardRoot(factory);

        var result = await grain.GetManyAsync([]);

        Assert.That(result, Is.Empty);
    }

    // --- SetManyAsync tests ---

    [Test]
    public async Task SetManyAsync_throws_on_null_entries()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.SetManyAsync(null!));
    }

    [Test]
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

        // All shards resolve to the same mock; verify SetManyAsync was called (not individual SetAsync).
        await shardRoot.ReceivedWithAnyArgs().SetManyAsync(default!);
        await shardRoot.DidNotReceiveWithAnyArgs().SetAsync(default!, default!);
    }

    [Test]
    public async Task SetManyAsync_CallsEnsureReminderAsync()
    {
        const string treeId = "compaction-setmany";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var compaction = SetupCompactionGrain(factory, treeId);

        await grain.SetManyAsync([new("k1", [1])]);

        await compaction.Received(1).EnsureReminderAsync();
    }

    // --- GetManyAsync batch delegation test ---

    [Test]
    public async Task GetManyAsync_calls_shard_GetManyAsync_not_individual_GetAsync()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetManyAsync(Arg.Any<List<string>>()).Returns(new Dictionary<string, byte[]>
        {
            ["k1"] = Encoding.UTF8.GetBytes("v1"),
        });

        await grain.GetManyAsync(["k1"]);

        // Should call the batch method, not individual GetAsync.
        await shardRoot.Received(1).GetManyAsync(Arg.Any<List<string>>());
        await shardRoot.DidNotReceive().GetAsync(Arg.Any<string>());
    }

    // --- DeleteRangeAsync ---

    [Test]
    public async Task DeleteRangeAsync_delegates_to_all_shards()
    {
        var (grain, factory) = CreateGrain(options: new LatticeOptions { ShardCount = 2 });

        var shard0 = Substitute.For<IShardRootGrain>();
        var shard1 = Substitute.For<IShardRootGrain>();

        factory.GetGrain<IShardRootGrain>("my-tree/0", null).Returns(shard0);
        factory.GetGrain<IShardRootGrain>("my-tree/1", null).Returns(shard1);
        shard0.DeleteRangeAsync("b", "d").Returns(3);
        shard1.DeleteRangeAsync("b", "d").Returns(2);

        var result = await grain.DeleteRangeAsync("b", "d");

        Assert.That(result, Is.EqualTo(5));
        await shard0.Received(1).DeleteRangeAsync("b", "d");
        await shard1.Received(1).DeleteRangeAsync("b", "d");
    }

    [Test]
    public void DeleteRangeAsync_throws_for_null_startInclusive()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.DeleteRangeAsync(null!, "z"));
    }

    [Test]
    public void DeleteRangeAsync_throws_for_null_endExclusive()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.DeleteRangeAsync("a", null!));
    }

    // --- GetWithVersionAsync tests ---

    [Test]
    public void GetWithVersionAsync_throws_on_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.GetWithVersionAsync(null!));
    }

    [Test]
    public async Task GetWithVersionAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        var expected = new VersionedValue
        {
            Value = Encoding.UTF8.GetBytes("v1"),
            Version = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 }
        };
        shardRoot.GetWithVersionAsync("k1").Returns(expected);

        var result = await grain.GetWithVersionAsync("k1");

        Assert.That(Encoding.UTF8.GetString(result.Value!), Is.EqualTo("v1"));
        Assert.That(result.Version, Is.EqualTo(expected.Version));
        await shardRoot.Received(1).GetWithVersionAsync("k1");
    }

    // --- SetIfVersionAsync tests ---

    [Test]
    public void SetIfVersionAsync_throws_on_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.SetIfVersionAsync(null!, [1], HybridLogicalClock.Zero));
    }

    [Test]
    public void SetIfVersionAsync_throws_on_null_value()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.SetIfVersionAsync("k1", null!, HybridLogicalClock.Zero));
    }

    [Test]
    public async Task SetIfVersionAsync_delegates_to_shard_root()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, "my-tree");
        var version = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        shardRoot.SetIfVersionAsync("k1", Arg.Any<byte[]>(), version).Returns(true);

        var result = await grain.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v1"), version);

        Assert.That(result, Is.True);
        await shardRoot.Received(1).SetIfVersionAsync("k1", Arg.Any<byte[]>(), version);
    }
}
