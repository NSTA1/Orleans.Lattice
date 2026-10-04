using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A read whose stale-routing signal never clears must give up with the typed
/// <see cref="StaleShardRoutingException"/> before its caller's response timeout,
/// and must pace its retries rather than spin (issue #4545). Before the fix the
/// read paths retried in a tight loop for 60 seconds, twice the default response
/// timeout, so the caller saw an anonymous <see cref="TimeoutException"/> while
/// the routing activation enqueued tens of thousands of registry and shard calls.
/// </summary>
public partial class LatticeGrainTests
{
    private static readonly TimeSpan ShortResponseTimeout = TimeSpan.FromMilliseconds(1200);

    /// <summary>
    /// A service provider answering a silo messaging configuration whose response
    /// timeout is <paramref name="responseTimeout"/>.
    /// </summary>
    private static IServiceProvider ServicesWithResponseTimeout(TimeSpan responseTimeout)
    {
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(IOptions<SiloMessagingOptions>))
            .Returns(Options.Create(new SiloMessagingOptions { ResponseTimeout = responseTimeout }));
        return services;
    }

    /// <summary>
    /// A grain whose every routing resolves the key to shard 0, which refuses
    /// every read with a stale-routing signal that never clears.
    /// </summary>
    private static (LatticeGrain Grain, IShardRootGrain Shard) CreatePermanentlyStaleGrain(string treeId)
    {
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2, services: ServicesWithResponseTimeout(ShortResponseTimeout));
        var map = new ShardMap { Slots = [0, 0], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));
        var shard = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>()).Returns(shard);
        return (grain, shard);
    }

    private static StaleShardRoutingException Stale() => new(-1, -1, -1);

    private static int CallsTo(IShardRootGrain shard, string method) =>
        shard.ReceivedCalls().Count(c => c.GetMethodInfo().Name == method);

    /// <summary>
    /// Asserts the read gave up inside the bounded budget, after more than the
    /// immediate retries, and paced its retries: a 1.2 s response timeout gives a
    /// 1 s budget, and with a 50 ms cap on the backoff that is a few dozen
    /// attempts. An unpaced loop makes thousands in the same second.
    /// </summary>
    private static void AssertBoundedAndPaced(TimeSpan elapsed, int attempts)
    {
        var budget = StaleRoutingReadRetry.Budget(ShortResponseTimeout, TimeSpan.FromSeconds(60));
        Assert.Multiple(() =>
        {
            Assert.That(elapsed, Is.GreaterThanOrEqualTo(budget - TimeSpan.FromMilliseconds(50)),
                "the read keeps retrying for its budget");
            Assert.That(elapsed, Is.LessThan(ShortResponseTimeout),
                "the typed fault must reach the caller before its response timeout");
            Assert.That(attempts, Is.GreaterThan(StaleRoutingReadRetry.ImmediateRetries + 1));
            Assert.That(attempts, Is.LessThan(200),
                "retries must be paced; an unpaced loop spins thousands of times in the budget");
        });
    }

    [Test]
    public async Task GetAsync_gives_up_with_the_typed_fault_inside_the_response_timeout_and_paces_its_retries()
    {
        var (grain, shard) = CreatePermanentlyStaleGrain("stale-forever-get");
        shard.TryGetOptimisticAsync("k1").Returns<Task<OptimisticReadResult>>(_ => throw Stale());
        shard.GetAsync("k1").Returns<Task<byte[]?>>(_ => throw Stale());

        var started = System.Diagnostics.Stopwatch.StartNew();
        Assert.That(async () => await grain.GetAsync("k1"), Throws.InstanceOf<StaleShardRoutingException>());
        started.Stop();

        AssertBoundedAndPaced(
            started.Elapsed,
            CallsTo(shard, nameof(IShardRootGrain.TryGetOptimisticAsync)) + CallsTo(shard, nameof(IShardRootGrain.GetAsync)));
    }

    [Test]
    public async Task ExistsAsync_gives_up_with_the_typed_fault_inside_the_response_timeout_and_paces_its_retries()
    {
        var (grain, shard) = CreatePermanentlyStaleGrain("stale-forever-exists");
        shard.ExistsAsync("k1").Returns<Task<bool>>(_ => throw Stale());

        var started = System.Diagnostics.Stopwatch.StartNew();
        Assert.That(async () => await grain.ExistsAsync("k1"), Throws.InstanceOf<StaleShardRoutingException>());
        started.Stop();

        AssertBoundedAndPaced(started.Elapsed, CallsTo(shard, nameof(IShardRootGrain.ExistsAsync)));
    }

    [Test]
    public async Task GetWithVersionAsync_gives_up_with_the_typed_fault_inside_the_response_timeout_and_paces_its_retries()
    {
        var (grain, shard) = CreatePermanentlyStaleGrain("stale-forever-version");
        shard.GetWithVersionAsync("k1").Returns<Task<VersionedValue>>(_ => throw Stale());

        var started = System.Diagnostics.Stopwatch.StartNew();
        Assert.That(async () => await grain.GetWithVersionAsync("k1"), Throws.InstanceOf<StaleShardRoutingException>());
        started.Stop();

        AssertBoundedAndPaced(started.Elapsed, CallsTo(shard, nameof(IShardRootGrain.GetWithVersionAsync)));
    }

    [Test]
    public async Task GetManyAsync_gives_up_with_the_typed_fault_inside_the_response_timeout_and_paces_its_retries()
    {
        var (grain, shard) = CreatePermanentlyStaleGrain("stale-forever-many");
        shard.GetManyAsync(Arg.Any<List<string>>()).Returns<Task<Dictionary<string, byte[]>>>(_ => throw Stale());

        var started = System.Diagnostics.Stopwatch.StartNew();
        Assert.That(async () => await grain.GetManyAsync(["k1", "k2"]), Throws.InstanceOf<StaleShardRoutingException>());
        started.Stop();

        AssertBoundedAndPaced(started.Elapsed, CallsTo(shard, nameof(IShardRootGrain.GetManyAsync)));
    }
}
