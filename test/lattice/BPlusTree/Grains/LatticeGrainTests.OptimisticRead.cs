using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// LatticeGrain point reads try the interleavable optimistic shard-root read first and
/// fall back to the serial <see cref="IShardRootGrain.GetAsync"/> (issue #3474).
/// </summary>
public partial class LatticeGrainTests
{
    [Test]
    public async Task GetAsync_validated_optimistic_read_skips_the_serial_read()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.TryGetOptimisticAsync("k1")
            .Returns(OptimisticReadResult.FromValue(Encoding.UTF8.GetBytes("fast")));

        var result = await grain.GetAsync("k1");

        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("fast"));
        await shardRoot.DidNotReceive().GetAsync(Arg.Any<string>());
    }

    [Test]
    public async Task GetAsync_validated_absent_optimistic_read_returns_null_without_serial_read()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.TryGetOptimisticAsync("k1").Returns(OptimisticReadResult.FromValue(null));

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.Null);
        await shardRoot.DidNotReceive().GetAsync(Arg.Any<string>());
    }

    [Test]
    public async Task GetAsync_unvalidated_optimistic_read_falls_back_to_serial_read()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.TryGetOptimisticAsync("k1").Returns(OptimisticReadResult.SerialRetry);
        shardRoot.GetAsync("k1").Returns(Encoding.UTF8.GetBytes("serial"));

        var result = await grain.GetAsync("k1");

        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("serial"));
        await shardRoot.Received(1).TryGetOptimisticAsync("k1");
        await shardRoot.Received(1).GetAsync("k1");
    }

    [Test]
    public async Task GetAsync_with_optimistic_reads_disabled_never_calls_the_optimistic_read()
    {
        var (grain, factory) = CreateGrain(options: new LatticeOptions { OptimisticShardRootPointReads = false });
        var shardRoot = SetupShardRoot(factory);
        shardRoot.GetAsync("k1").Returns(Encoding.UTF8.GetBytes("serial"));

        var result = await grain.GetAsync("k1");

        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("serial"));
        await shardRoot.DidNotReceive().TryGetOptimisticAsync(Arg.Any<string>());
    }

    [Test]
    public async Task GetAsync_stale_routing_after_optimistic_retry_reaches_the_serial_read_on_the_next_attempt()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = SetupShardRoot(factory);
        shardRoot.TryGetOptimisticAsync("k1").Returns(OptimisticReadResult.SerialRetry);
        shardRoot.GetAsync("k1").Returns(
            _ => throw new StaleShardRoutingException(0, 1, 5),
            _ => Task.FromResult<byte[]?>(Encoding.UTF8.GetBytes("settled")));

        var result = await grain.GetAsync("k1");

        // Every attempt of the stale-routing retry loop reaches the serial read, so
        // a condition only the serial path settles cannot pin the loop on the
        // optimistic read (issue #3474 no-livelock property).
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("settled"));
        await shardRoot.Received(2).TryGetOptimisticAsync("k1");
        await shardRoot.Received(2).GetAsync("k1");
    }

    [Test]
    public void OptimisticReadResult_default_is_a_serial_retry()
    {
        OptimisticReadResult result = default;

        Assert.That(result.IsValidated, Is.False);
        Assert.That(OptimisticReadResult.SerialRetry.IsValidated, Is.False);
    }

    [Test]
    public void OptimisticReadResult_FromValue_is_validated_even_for_an_absent_key()
    {
        var absent = OptimisticReadResult.FromValue(null);
        var present = OptimisticReadResult.FromValue([1, 2]);

        Assert.That(absent.IsValidated, Is.True);
        Assert.That(absent.Value, Is.Null);
        Assert.That(present.IsValidated, Is.True);
        Assert.That(present.Value, Is.EqualTo(new byte[] { 1, 2 }));
    }

    [Test]
    public void LatticeOptions_OptimisticShardRootPointReads_defaults_on()
    {
        Assert.That(new LatticeOptions().OptimisticShardRootPointReads, Is.True);
        Assert.That(LatticeOptions.DefaultOptimisticShardRootPointReads, Is.True);
    }
}
