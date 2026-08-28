using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LatticeStorageUsageGrain"/>, the per-tree
/// byte-accurate storage-usage aggregator. Pins the reliability contract from
/// issue #1728: a storage surface that fails to answer must flag the report
/// <see cref="TreeStorageUsageReport.Partial"/> rather than contribute a
/// silent zero that understates the tree while still presenting the total as
/// authoritative. Also pins the bounded fan-out that keeps a wide tree from
/// dispatching every shard-root read in one burst.
/// </summary>
[TestFixture]
public sealed class LatticeStorageUsageGrainTests
{
    private const string TreeId = "usage-test-tree";

    private readonly List<IDisposable> _disposables = [];

    [TearDown]
    public void DisposeMeters()
    {
        foreach (var d in _disposables)
        {
            d.Dispose();
        }
        _disposables.Clear();
    }

    /// <summary>
    /// Builds the grain over substituted shard roots and WAL shards. The
    /// per-shard and per-partition behaviours are supplied as delegates so a
    /// test can fail one surface, block one surface, or count concurrency.
    /// </summary>
    private LatticeStorageUsageGrain CreateGrain(
        int shardCount,
        int walPartitions,
        Func<int, Task<ShardStorageUsage>> shardBehaviour,
        Func<int, Task<long>> walBehaviour,
        LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("ol.lsu", TreeId));

        var factory = Substitute.For<IGrainFactory>();
        options ??= new LatticeOptions();
        options.WalPartitions = walPartitions;

        var lattice = Substitute.For<ILattice>();
        var map = ShardMap.CreateDefault(Math.Max(16, shardCount), shardCount);
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(new RoutingInfo(TreeId, map));
        factory.GetGrain<ILattice>(TreeId).Returns(lattice);

        for (var i = 0; i < shardCount; i++)
        {
            var idx = i;
            var shard = Substitute.For<IShardRootGrain>();
            shard.GetStorageUsageAsync(Arg.Any<CancellationToken>()).Returns(_ => shardBehaviour(idx));
            shard.RefreshLeafByteFootprintsAsync(Arg.Any<CancellationToken>()).Returns(_ => shardBehaviour(idx));
            factory.GetGrain<IShardRootGrain>($"{TreeId}/{idx}").Returns(shard);
        }

        for (var p = 0; p < walPartitions; p++)
        {
            var partition = p;
            var wal = Substitute.For<IWalShardGrain>();
            wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>())
                .Returns(_ => walBehaviour(partition));
            factory.GetGrain<IWalShardGrain>($"{TreeId}/{partition}").Returns(wal);
        }

        var usageMetrics = new LatticeStorageUsageMetrics();
        var admissionMetrics = new LatticeAdmissionMetrics();
        _disposables.Add(usageMetrics);
        _disposables.Add(admissionMetrics);

        return new LatticeStorageUsageGrain(
            context,
            factory,
            TestOptionsResolver.ForFactory(factory, options),
            usageMetrics,
            admissionMetrics,
            NullLogger<LatticeStorageUsageGrain>.Instance);
    }

    private static Task<ShardStorageUsage> Usage(long leaf, long snapshot, long liveKeys) =>
        Task.FromResult(new ShardStorageUsage
        {
            LeafStateBytes = leaf,
            SnapshotBytes = snapshot,
            LiveKeys = liveKeys,
        });

    // --- Problem 1: a failed shard must not silently understate the tree ---

    [Test]
    public async Task GetReportAsync_shard_fanout_failure_marks_the_report_partial()
    {
        var grain = CreateGrain(
            shardCount: 2,
            walPartitions: 1,
            shardBehaviour: i => i == 0
                ? Task.FromException<ShardStorageUsage>(new InvalidOperationException("shard 0 down"))
                : Usage(leaf: 100, snapshot: 10, liveKeys: 5),
            walBehaviour: _ => Task.FromResult(7L));

        var report = await grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // The headline guard. Before the fix the failed shard contributed a
            // zeroed ShardStorageUsage and Partial stayed false, so a caller saw
            // an understated total presented as complete.
            Assert.That(report.Partial, Is.True, "A shard that did not answer must flag the report Partial.");
            // The surviving shard still contributes: one bad shard does not
            // abort the whole tree.
            Assert.That(report.LeafStateBytes, Is.EqualTo(100));
            Assert.That(report.SnapshotBytes, Is.EqualTo(10));
            Assert.That(report.LiveKeys, Is.EqualTo(5));
            Assert.That(report.WalRetainedBytes, Is.EqualTo(7));
        });
    }

    [Test]
    public async Task GetReportAsync_every_shard_failing_reports_partial_rather_than_a_confident_zero()
    {
        var grain = CreateGrain(
            shardCount: 3,
            walPartitions: 1,
            shardBehaviour: _ => Task.FromException<ShardStorageUsage>(new TimeoutException("deadline")),
            walBehaviour: _ => Task.FromResult(0L));

        var report = await grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True);
            Assert.That(report.LeafStateBytes, Is.Zero);
            Assert.That(report.TotalBytes, Is.Zero);
        });
    }

    [Test]
    public async Task GetReportAsync_all_shards_healthy_is_not_partial()
    {
        var grain = CreateGrain(
            shardCount: 3,
            walPartitions: 2,
            shardBehaviour: _ => Usage(leaf: 100, snapshot: 10, liveKeys: 5),
            walBehaviour: _ => Task.FromResult(7L));

        var report = await grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.False);
            Assert.That(report.LeafStateBytes, Is.EqualTo(300));
            Assert.That(report.SnapshotBytes, Is.EqualTo(30));
            Assert.That(report.LiveKeys, Is.EqualTo(15));
            Assert.That(report.WalRetainedBytes, Is.EqualTo(14));
            Assert.That(report.TotalBytes, Is.EqualTo(344));
        });
    }

    [Test]
    public async Task GetReportAsync_a_shard_reporting_genuine_zeroes_is_not_partial()
    {
        // An empty shard answers with zeroes, which must stay distinguishable
        // from a shard that did not answer at all.
        var grain = CreateGrain(
            shardCount: 2,
            walPartitions: 1,
            shardBehaviour: i => i == 0 ? Usage(0, 0, 0) : Usage(leaf: 50, snapshot: 5, liveKeys: 2),
            walBehaviour: _ => Task.FromResult(1L));

        var report = await grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.False);
            Assert.That(report.LeafStateBytes, Is.EqualTo(50));
        });
    }

    // --- The WAL -1 sentinel keeps its existing meaning ---

    [Test]
    public async Task GetReportAsync_wal_negative_sentinel_marks_partial_and_contributes_nothing()
    {
        var grain = CreateGrain(
            shardCount: 1,
            walPartitions: 2,
            shardBehaviour: _ => Usage(leaf: 100, snapshot: 0, liveKeys: 1),
            // Partition 0 reports "byte accounting unsupported"; partition 1 answers.
            walBehaviour: p => Task.FromResult(p == 0 ? -1L : 9L));

        var report = await grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True);
            Assert.That(report.WalRetainedBytes, Is.EqualTo(9), "The -1 sentinel contributes nothing, not -1.");
            Assert.That(report.TotalBytes, Is.EqualTo(109));
        });
    }

    [Test]
    public async Task GetReportAsync_wal_fanout_failure_marks_partial()
    {
        var grain = CreateGrain(
            shardCount: 1,
            walPartitions: 1,
            shardBehaviour: _ => Usage(leaf: 100, snapshot: 0, liveKeys: 1),
            walBehaviour: _ => Task.FromException<long>(new InvalidOperationException("wal down")));

        var report = await grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True);
            Assert.That(report.WalRetainedBytes, Is.Zero);
            Assert.That(report.LeafStateBytes, Is.EqualTo(100));
        });
    }

    // --- Problem 2: the per-tree surface fan-out is bounded ---

    [Test]
    public async Task GetReportAsync_surface_fanout_never_exceeds_the_configured_bound()
    {
        const int Bound = 3;
        const int Shards = 8;
        const int Partitions = 4;

        var inFlight = 0;
        var peak = 0;
        // Released only once the gate is provably saturated, so the test proves
        // overlap without depending on any timing.
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task TrackAsync()
        {
            var current = Interlocked.Increment(ref inFlight);
            RecordPeak(ref peak, current);
            if (current >= Bound) release.TrySetResult();
            await release.Task;
            Interlocked.Decrement(ref inFlight);
        }

        var grain = CreateGrain(
            shardCount: Shards,
            walPartitions: Partitions,
            shardBehaviour: async _ =>
            {
                await TrackAsync();
                return new ShardStorageUsage { LeafStateBytes = 1 };
            },
            walBehaviour: async _ =>
            {
                await TrackAsync();
                return 1L;
            },
            options: new LatticeOptions { MaxConcurrentStorageUsageSurfaces = Bound });

        var report = await grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // The bound is a joint ceiling over shard roots and WAL partitions.
            Assert.That(peak, Is.EqualTo(Bound),
                $"Expected at most {Bound} concurrent surface reads across {Shards} shards and {Partitions} WAL partitions.");
            // ...and the bound changed only the schedule, not the answer.
            Assert.That(report.LeafStateBytes, Is.EqualTo(Shards));
            Assert.That(report.WalRetainedBytes, Is.EqualTo(Partitions));
            Assert.That(report.Partial, Is.False);
        });
    }

    [Test]
    public async Task GetReportAsync_bound_wider_than_the_tree_still_produces_the_same_report()
    {
        var grain = CreateGrain(
            shardCount: 2,
            walPartitions: 1,
            shardBehaviour: _ => Usage(leaf: 10, snapshot: 1, liveKeys: 1),
            walBehaviour: _ => Task.FromResult(3L),
            options: new LatticeOptions { MaxConcurrentStorageUsageSurfaces = 1024 });

        var report = await grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.That(report.TotalBytes, Is.EqualTo(25));
    }

    [Test]
    public async Task GetReportAsync_bound_below_one_is_clamped_and_does_not_deadlock()
    {
        var grain = CreateGrain(
            shardCount: 3,
            walPartitions: 2,
            shardBehaviour: _ => Usage(leaf: 10, snapshot: 0, liveKeys: 1),
            walBehaviour: _ => Task.FromResult(2L),
            options: new LatticeOptions { MaxConcurrentStorageUsageSurfaces = 0 });

        var report = await grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.LeafStateBytes, Is.EqualTo(30));
            Assert.That(report.WalRetainedBytes, Is.EqualTo(4));
        });
    }

    // --- Problem 3: cancellation is prompt and leaves nothing unobserved ---

    [Test]
    public void GetReportAsync_cancelled_mid_fanout_throws_rather_than_returning_a_misleading_total()
    {
        using var cts = new CancellationTokenSource();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var grain = CreateGrain(
            shardCount: 8,
            walPartitions: 2,
            shardBehaviour: async _ =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, cts.Token);
                return new ShardStorageUsage();
            },
            walBehaviour: async _ =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, cts.Token);
                return 0L;
            },
            options: new LatticeOptions { MaxConcurrentStorageUsageSurfaces = 2 });

        var pending = grain.GetReportAsync(forceRefresh: false, cts.Token);

        Assert.That(async () =>
        {
            await started.Task;
            await cts.CancelAsync();
            await pending;
        }, Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>
    /// Sentinel fault thrown only by this test's fake surfaces. The
    /// <see cref="TaskScheduler.UnobservedTaskException"/> hook is process-global,
    /// so in a full-suite run it also catches faults abandoned by fixtures running
    /// in parallel; matching on this type scopes the assertion to the claim the
    /// test can actually own - that <b>this</b> fan-out observed <b>its own</b>
    /// children.
    /// </summary>
    private sealed class SurfaceProbeException(string message) : Exception(message);

    [Test]
    public async Task GetReportAsync_cancelled_fanout_leaves_no_unobserved_task_exceptions()
    {
        var unobserved = new List<Exception>();
        void Handler(object? sender, UnobservedTaskExceptionEventArgs e)
        {
            if (e.Exception.Flatten().InnerExceptions.Any(x => x is SurfaceProbeException))
            {
                lock (unobserved) unobserved.Add(e.Exception);
            }

            e.SetObserved();
        }

        TaskScheduler.UnobservedTaskException += Handler;
        try
        {
            using var cts = new CancellationTokenSource();
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var grain = CreateGrain(
                shardCount: 16,
                walPartitions: 4,
                // Half the shards fail outright while the fan-out is being
                // cancelled: the old shape awaited the shard batch and the WAL
                // batch in sequence, so a throw from the first left the second
                // batch's tasks abandoned.
                shardBehaviour: async i =>
                {
                    started.TrySetResult();
                    await Task.Delay(20, CancellationToken.None);
                    if (i % 2 == 0) throw new SurfaceProbeException($"shard {i} down");
                    return new ShardStorageUsage { LeafStateBytes = 1 };
                },
                walBehaviour: async _ =>
                {
                    await Task.Delay(20, CancellationToken.None);
                    throw new SurfaceProbeException("wal down");
                },
                options: new LatticeOptions { MaxConcurrentStorageUsageSurfaces = 4 });

            var pending = grain.GetReportAsync(forceRefresh: false, cts.Token);
            await started.Task;
            await cts.CancelAsync();
            try
            {
                await pending;
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation wins the race.
            }

            // Force finalisation of any task dropped without its fault observed.
            for (var i = 0; i < 3; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            lock (unobserved)
            {
                Assert.That(unobserved, Is.Empty, "The bounded fan-out must observe every child fault.");
            }
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= Handler;
        }
    }

    private static void RecordPeak(ref int peak, int candidate)
    {
        var observed = Volatile.Read(ref peak);
        while (candidate > observed)
        {
            var prior = Interlocked.CompareExchange(ref peak, candidate, observed);
            if (prior == observed) return;
            observed = prior;
        }
    }
}
