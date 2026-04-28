using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="LatticeWalIntrospection"/>. Walks the
/// per-tree WAL shards and returns the minimum head HLC across them,
/// or <see langword="null"/> when every shard is empty.
/// </summary>
[TestFixture]
public class LatticeWalIntrospectionTests
{
    private const string Tree = "intro-tree";

    private static HybridLogicalClock Hlc(long ticks) =>
        new() { WallClockTicks = ticks, Counter = 0 };

    private static ReplogShardPage PageWithHead(long ticks) => new()
    {
        Entries = new ReplogShardEntry[]
        {
            new() { Sequence = 0, Entry = new ReplogEntry { Timestamp = Hlc(ticks) } },
        },
        NextSequence = 1,
    };

    private static (
        LatticeWalIntrospection Introspection,
        IGrainFactory Factory) Create(int partitions)
    {
        var factory = Substitute.For<IGrainFactory>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "self",
            ReplogPartitions = partitions,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        return (new LatticeWalIntrospection(factory, monitor), factory);
    }

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        Assert.That(
            () => new LatticeWalIntrospection(
                null!,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_options_monitor_is_null()
    {
        Assert.That(
            () => new LatticeWalIntrospection(Substitute.For<IGrainFactory>(), null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetOldestAvailableHlcAsync_throws_when_tree_name_is_null()
    {
        var (intro, _) = Create(1);
        Assert.That(
            async () => await intro.GetOldestAvailableHlcAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetOldestAvailableHlcAsync_throws_when_tree_name_is_empty()
    {
        var (intro, _) = Create(1);
        Assert.That(
            async () => await intro.GetOldestAvailableHlcAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetOldestAvailableHlcAsync_observes_cancellation_before_dispatch()
    {
        var (intro, _) = Create(1);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await intro.GetOldestAvailableHlcAsync(Tree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_returns_null_when_every_shard_is_empty()
    {
        var (intro, factory) = Create(3);
        var grain = Substitute.For<IReplogShardGrain>();
        grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ReplogShardPage.Empty(0)));
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(grain);

        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);
        Assert.That(oldest, Is.Null);
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_returns_head_for_single_partition()
    {
        var (intro, factory) = Create(1);
        var grain = Substitute.For<IReplogShardGrain>();
        grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(PageWithHead(42)));
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(grain);

        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);
        Assert.That(oldest, Is.EqualTo(Hlc(42)));
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_returns_minimum_across_multiple_partitions()
    {
        var (intro, factory) = Create(3);
        var p0 = Substitute.For<IReplogShardGrain>();
        p0.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(PageWithHead(100)));
        var p1 = Substitute.For<IReplogShardGrain>();
        p1.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(PageWithHead(25)));  // minimum
        var p2 = Substitute.For<IReplogShardGrain>();
        p2.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(PageWithHead(75)));

        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(p0);
        factory.GetGrain<IReplogShardGrain>($"{Tree}/1").Returns(p1);
        factory.GetGrain<IReplogShardGrain>($"{Tree}/2").Returns(p2);

        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);
        Assert.That(oldest, Is.EqualTo(Hlc(25)));
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_skips_empty_shards_and_returns_min_of_populated_ones()
    {
        var (intro, factory) = Create(3);
        var p0 = Substitute.For<IReplogShardGrain>();
        p0.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ReplogShardPage.Empty(0)));
        var p1 = Substitute.For<IReplogShardGrain>();
        p1.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(PageWithHead(99)));
        var p2 = Substitute.For<IReplogShardGrain>();
        p2.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(PageWithHead(33)));

        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(p0);
        factory.GetGrain<IReplogShardGrain>($"{Tree}/1").Returns(p1);
        factory.GetGrain<IReplogShardGrain>($"{Tree}/2").Returns(p2);

        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);
        Assert.That(oldest, Is.EqualTo(Hlc(33)));
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_resolves_each_shard_by_canonical_grain_key()
    {
        var (intro, factory) = Create(2);
        var grain = Substitute.For<IReplogShardGrain>();
        grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(PageWithHead(10)));
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(grain);

        await intro.GetOldestAvailableHlcAsync(Tree);

        factory.Received(1).GetGrain<IReplogShardGrain>($"{Tree}/0");
        factory.Received(1).GetGrain<IReplogShardGrain>($"{Tree}/1");
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_observes_cancellation_after_partition_dispatch()
    {
        // Cancellation that fires while the shard reads are in flight must
        // surface as OperationCanceledException, not a partial result.
        var (intro, factory) = Create(2);
        using var cts = new CancellationTokenSource();

        var grain = Substitute.For<IReplogShardGrain>();
        grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                cts.Cancel();
                return Task.FromCanceled<ReplogShardPage>(cts.Token);
            });
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(grain);

        Assert.That(
            async () => await intro.GetOldestAvailableHlcAsync(Tree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_dispatches_every_partition_read_concurrently()
    {
        // Pin the parallel-fan-out contract: every shard's ReadAsync must
        // be dispatched before any shard's task completes. Each shard's
        // task only resolves once *every* shard has been observed in
        // flight, so a serial implementation would deadlock.
        const int partitions = 4;
        var (intro, factory) = Create(partitions);

        var dispatched = 0;
        var allDispatched = new TaskCompletionSource();

        ReplogShardPage HeadPage(int ticks) => new()
        {
            Entries = new ReplogShardEntry[]
            {
                new() { Sequence = 0, Entry = new ReplogEntry { Timestamp = Hlc(ticks) } },
            },
            NextSequence = 1,
        };

        for (var i = 0; i < partitions; i++)
        {
            var ticks = (i + 1) * 10;
            var grain = Substitute.For<IReplogShardGrain>();
            grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
                .Returns(async _ =>
                {
                    if (Interlocked.Increment(ref dispatched) == partitions)
                    {
                        allDispatched.TrySetResult();
                    }
                    await allDispatched.Task.ConfigureAwait(false);
                    return HeadPage(ticks);
                });
            factory.GetGrain<IReplogShardGrain>($"{Tree}/{i}").Returns(grain);
        }

        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);

        Assert.That(dispatched, Is.EqualTo(partitions));
        Assert.That(oldest, Is.EqualTo(Hlc(10)));  // partition 0 has min ticks
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_returns_null_when_partition_count_is_zero()
    {
        // Defensive: even though the options validator rejects
        // ReplogPartitions <= 0, a host that resolves a custom options
        // instance bypassing validation must not crash the introspector.
        var (intro, _) = Create(0);
        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);
        Assert.That(oldest, Is.Null);
    }
}