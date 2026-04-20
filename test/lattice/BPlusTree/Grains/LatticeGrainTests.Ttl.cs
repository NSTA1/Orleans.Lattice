using NSubstitute;
using Orleans.Lattice.BPlusTree;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- TTL / SetAsync(ttl) ---

    [Test]
    public void SetAsync_ttl_throws_on_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() =>
            grain.SetAsync(null!, [1], TimeSpan.FromMinutes(1)));
    }

    [Test]
    public void SetAsync_ttl_throws_on_null_value()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() =>
            grain.SetAsync("k", null!, TimeSpan.FromMinutes(1)));
    }

    [Test]
    public void SetAsync_ttl_throws_on_zero_ttl()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            grain.SetAsync("k", [1], TimeSpan.Zero));
    }

    [Test]
    public void SetAsync_ttl_throws_on_negative_ttl()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            grain.SetAsync("k", [1], TimeSpan.FromSeconds(-1)));
    }

    [Test]
    public void SetAsync_ttl_throws_on_overflow_ttl()
    {
        var (grain, _) = CreateGrain();
        // MaxValue as a TTL always exceeds DateTimeOffset.MaxValue - UtcNow.
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            grain.SetAsync("k", [1], TimeSpan.MaxValue));
    }

    [Test]
    public async Task SetAsync_ttl_delegates_to_shard_root_with_expiry()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);

        var ttl = TimeSpan.FromHours(1);
        var before = DateTimeOffset.UtcNow.Add(ttl).UtcTicks;
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"), ttl);
        var after = DateTimeOffset.UtcNow.Add(ttl).UtcTicks;

        await shardRoot.Received(1).SetAsync(
            "k1",
            Arg.Any<byte[]>(),
            Arg.Is<long>(t => t >= before && t <= after));
    }
}

