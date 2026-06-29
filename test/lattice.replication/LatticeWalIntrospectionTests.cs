using Orleans.Lattice.BPlusTree.Grains;
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

    private static WalShardPage PageWithHead(long ticks) => new()
    {
        Entries = new WalShardSequencedEntry[]
        {
            new() { Sequence = 0, Entry = new WalRecord { Timestamp = Hlc(ticks) } },
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
        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(WalShardPage.Empty(0)));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(grain);

        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);
        Assert.That(oldest, Is.Null);
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_returns_head_for_single_partition()
    {
        var (intro, factory) = Create(1);
        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithHead(42)));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);
        Assert.That(oldest, Is.EqualTo(Hlc(42)));
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_returns_minimum_across_multiple_partitions()
    {
        var (intro, factory) = Create(3);
        var p0 = Substitute.For<IWalShardGrain>();
        p0.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithHead(100)));
        var p1 = Substitute.For<IWalShardGrain>();
        p1.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithHead(25)));  // minimum
        var p2 = Substitute.For<IWalShardGrain>();
        p2.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithHead(75)));

        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(p0);
        factory.GetGrain<IWalShardGrain>($"{Tree}/1").Returns(p1);
        factory.GetGrain<IWalShardGrain>($"{Tree}/2").Returns(p2);

        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);
        Assert.That(oldest, Is.EqualTo(Hlc(25)));
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_skips_empty_shards_and_returns_min_of_populated_ones()
    {
        var (intro, factory) = Create(3);
        var p0 = Substitute.For<IWalShardGrain>();
        p0.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(WalShardPage.Empty(0)));
        var p1 = Substitute.For<IWalShardGrain>();
        p1.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithHead(99)));
        var p2 = Substitute.For<IWalShardGrain>();
        p2.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithHead(33)));

        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(p0);
        factory.GetGrain<IWalShardGrain>($"{Tree}/1").Returns(p1);
        factory.GetGrain<IWalShardGrain>($"{Tree}/2").Returns(p2);

        var oldest = await intro.GetOldestAvailableHlcAsync(Tree);
        Assert.That(oldest, Is.EqualTo(Hlc(33)));
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_resolves_each_shard_by_canonical_grain_key()
    {
        var (intro, factory) = Create(2);
        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithHead(10)));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(grain);

        await intro.GetOldestAvailableHlcAsync(Tree);

        factory.Received(1).GetGrain<IWalShardGrain>($"{Tree}/0");
        factory.Received(1).GetGrain<IWalShardGrain>($"{Tree}/1");
    }

    [Test]
    public async Task GetOldestAvailableHlcAsync_observes_cancellation_after_partition_dispatch()
    {
        // Cancellation that fires while the shard reads are in flight must
        // surface as OperationCanceledException, not a partial result.
        var (intro, factory) = Create(2);
        using var cts = new CancellationTokenSource();

        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                cts.Cancel();
                return new ValueTask<WalShardPage>(Task.FromCanceled<WalShardPage>(cts.Token));
            });
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(grain);

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

        WalShardPage HeadPage(int ticks) => new()
        {
            Entries = new WalShardSequencedEntry[]
            {
                new() { Sequence = 0, Entry = new WalRecord { Timestamp = Hlc(ticks) } },
            },
            NextSequence = 1,
        };

        for (var i = 0; i < partitions; i++)
        {
            var ticks = (i + 1) * 10;
            var grain = Substitute.For<IWalShardGrain>();
            grain.ReadAsync(0, 1, Arg.Any<CancellationToken>())
                .Returns((Func<NSubstitute.Core.CallInfo, ValueTask<WalShardPage>>)(async _ =>
                {
                    if (Interlocked.Increment(ref dispatched) == partitions)
                    {
                        allDispatched.TrySetResult();
                    }
                    await allDispatched.Task.ConfigureAwait(false);
                    return HeadPage(ticks);
                }));
            factory.GetGrain<IWalShardGrain>($"{Tree}/{i}").Returns(grain);
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

    // --- GetOldestAvailableHlcByOriginAsync (per-origin grouping) ---

    private const int OriginScanBudget = 64;

    private static WalShardPage PageWithOrigins(params (long Ticks, string? Origin)[] entries)
    {
        var arr = new WalShardSequencedEntry[entries.Length];
        for (var i = 0; i < entries.Length; i++)
        {
            arr[i] = new WalShardSequencedEntry
            {
                Sequence = i,
                Entry = new WalRecord { Timestamp = Hlc(entries[i].Ticks), OriginClusterId = entries[i].Origin },
            };
        }
        return new WalShardPage { Entries = arr, NextSequence = entries.Length };
    }

    [Test]
    public void GetOldestAvailableHlcByOriginAsync_throws_when_tree_name_is_null()
    {
        var (intro, _) = Create(1);
        Assert.That(
            async () => await intro.GetOldestAvailableHlcByOriginAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetOldestAvailableHlcByOriginAsync_throws_when_tree_name_is_empty()
    {
        var (intro, _) = Create(1);
        Assert.That(
            async () => await intro.GetOldestAvailableHlcByOriginAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetOldestAvailableHlcByOriginAsync_observes_cancellation_before_dispatch()
    {
        var (intro, _) = Create(1);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await intro.GetOldestAvailableHlcByOriginAsync(Tree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetOldestAvailableHlcByOriginAsync_returns_empty_when_partition_count_is_zero()
    {
        var (intro, _) = Create(0);
        var byOrigin = await intro.GetOldestAvailableHlcByOriginAsync(Tree);
        Assert.That(byOrigin, Is.Empty);
    }

    [Test]
    public async Task GetOldestAvailableHlcByOriginAsync_returns_empty_when_every_shard_is_empty()
    {
        var (intro, factory) = Create(2);
        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(0, OriginScanBudget, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(WalShardPage.Empty(0)));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(grain);

        var byOrigin = await intro.GetOldestAvailableHlcByOriginAsync(Tree);
        Assert.That(byOrigin, Is.Empty);
    }

    [Test]
    public async Task GetOldestAvailableHlcByOriginAsync_groups_oldest_per_origin_within_a_partition()
    {
        var (intro, factory) = Create(1);
        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(0, OriginScanBudget, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithOrigins(
                (10, "us"), (20, "eu"), (5, "us"), (30, "eu"))));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var byOrigin = await intro.GetOldestAvailableHlcByOriginAsync(Tree);

        Assert.That(byOrigin["us"], Is.EqualTo(Hlc(5)));
        Assert.That(byOrigin["eu"], Is.EqualTo(Hlc(20)));
    }

    [Test]
    public async Task GetOldestAvailableHlcByOriginAsync_takes_min_per_origin_across_partitions()
    {
        var (intro, factory) = Create(2);
        var p0 = Substitute.For<IWalShardGrain>();
        p0.ReadAsync(0, OriginScanBudget, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithOrigins((100, "us"), (40, "eu"))));
        var p1 = Substitute.For<IWalShardGrain>();
        p1.ReadAsync(0, OriginScanBudget, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithOrigins((50, "us"), (80, "eu"))));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(p0);
        factory.GetGrain<IWalShardGrain>($"{Tree}/1").Returns(p1);

        var byOrigin = await intro.GetOldestAvailableHlcByOriginAsync(Tree);

        Assert.That(byOrigin["us"], Is.EqualTo(Hlc(50)));
        Assert.That(byOrigin["eu"], Is.EqualTo(Hlc(40)));
    }

    [Test]
    public async Task GetOldestAvailableHlcByOriginAsync_attributes_unstamped_entries_to_local_cluster()
    {
        // ClusterId is "self" in the test options. A WAL entry with no
        // OriginClusterId (pre-origin-stamping) must group under the
        // local cluster id so the receiver-side probe treats it as
        // self-origin data and never bootstraps from it.
        var (intro, factory) = Create(1);
        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(0, OriginScanBudget, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithOrigins((7, null), (9, ""))));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);

        var byOrigin = await intro.GetOldestAvailableHlcByOriginAsync(Tree);

        Assert.That(byOrigin.Keys, Is.EquivalentTo(new[] { "self" }));
        Assert.That(byOrigin["self"], Is.EqualTo(Hlc(7)));
    }

    [Test]
    public async Task GetOldestAvailableHlcByOriginAsync_reads_a_bounded_window_per_canonical_shard_key()
    {
        var (intro, factory) = Create(2);
        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(0, OriginScanBudget, Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(PageWithOrigins((1, "us"))));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(grain);

        await intro.GetOldestAvailableHlcByOriginAsync(Tree);

        factory.Received(1).GetGrain<IWalShardGrain>($"{Tree}/0");
        factory.Received(1).GetGrain<IWalShardGrain>($"{Tree}/1");
        await grain.Received(2).ReadAsync(0, OriginScanBudget, Arg.Any<CancellationToken>());
    }
}