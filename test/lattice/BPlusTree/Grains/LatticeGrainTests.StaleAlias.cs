using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- Stale alias retry tests ---

    [Test]
    public async Task GetAsync_retries_on_stale_alias()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        var callCount = 0;
        shardRoot.GetAsync("k1").Returns(_ =>
        {
            if (callCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.FromResult<byte[]?>([1, 2, 3]);
        });

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public async Task SetAsync_retries_on_stale_alias()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, "my-tree");
        var callCount = 0;
        shardRoot.SetAsync("k1", Arg.Any<byte[]>()).Returns(_ =>
        {
            if (callCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.CompletedTask;
        });

        await grain.SetAsync("k1", [1]);

        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public async Task DeleteAsync_retries_on_stale_alias()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, "my-tree");
        var callCount = 0;
        shardRoot.DeleteAsync("k1").Returns(_ =>
        {
            if (callCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.FromResult(true);
        });

        var result = await grain.DeleteAsync("k1");

        Assert.That(result, Is.True);
        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public async Task ExistsAsync_retries_on_stale_alias()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        var callCount = 0;
        shardRoot.ExistsAsync("k1").Returns(_ =>
        {
            if (callCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.FromResult(true);
        });

        var result = await grain.ExistsAsync("k1");

        Assert.That(result, Is.True);
        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public async Task GetManyAsync_retries_on_stale_alias()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        var callCount = 0;
        shardRoot.GetManyAsync(Arg.Any<List<string>>()).Returns(_ =>
        {
            if (callCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.FromResult(new Dictionary<string, byte[]> { ["k1"] = [1] });
        });

        var result = await grain.GetManyAsync(["k1"]);

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public async Task SetManyAsync_retries_on_stale_alias()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, "my-tree");
        var callCount = 0;
        shardRoot.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(_ =>
        {
            if (callCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.CompletedTask;
        });

        await grain.SetManyAsync([new KeyValuePair<string, byte[]>("k1", [1])]);

        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public async Task DeleteRangeAsync_retries_on_stale_alias()
    {
        var (grain, factory) = CreateGrain(shardCount: 1);
        SetupCompactionGrain(factory, "my-tree");

        var shard = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>("my-tree/0", null).Returns(shard);
        var callCount = 0;
        shard.DeleteRangeAsync("a", "z").Returns(_ =>
        {
            if (callCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.FromResult(5);
        });

        var result = await grain.DeleteRangeAsync("a", "z");

        Assert.That(result, Is.EqualTo(5));
        Assert.That(callCount, Is.EqualTo(2));
    }
}
