using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LatticeAdminGrain"/>. Pins the split
/// between the cheap WAL-only polling path
/// (<see cref="ILatticeAdmin.PollWalUsageAsync"/>) and the deep
/// operator-driven refresh
/// (<see cref="ILatticeAdmin.RefreshStorageUsageAsync"/>).
/// </summary>
[TestFixture]
public sealed class LatticeAdminGrainTests
{
    private static LatticeAdminGrain CreateGrain(
        IGrainFactory factory,
        ILatticeRegistry? registry = null,
        LatticeOptions? options = null)
    {
        if (registry is null)
        {
            registry = Substitute.For<ILatticeRegistry>();
            registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>()));
        }
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("ol.gad", LatticeConstants.AdminGrainKey));

        IOptionsMonitor<LatticeOptions>? monitor = null;
        if (options is not null)
        {
            monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            monitor.Get(Arg.Any<string>()).Returns(options);
        }

        return new LatticeAdminGrain(
            context,
            factory,
            Substitute.For<ILogger<LatticeAdminGrain>>(),
            optionsMonitor: monitor);
    }

    [Test]
    public async Task PollWalUsageAsync_calls_only_the_wal_usage_aggregator_for_each_tree()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(new[] { "alpha", "beta" }));

        var factory = Substitute.For<IGrainFactory>();
        var walAlpha = Substitute.For<ILatticeWalUsage>();
        walAlpha.GetWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new TreeWalUsageReport { TreeId = "alpha", WalRetainedBytes = 1 });
        var walBeta = Substitute.For<ILatticeWalUsage>();
        walBeta.GetWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new TreeWalUsageReport { TreeId = "beta", WalRetainedBytes = 2 });
        factory.GetGrain<ILatticeWalUsage>("alpha").Returns(walAlpha);
        factory.GetGrain<ILatticeWalUsage>("beta").Returns(walBeta);

        var grain = CreateGrain(factory, registry);

        await grain.PollWalUsageAsync(CancellationToken.None);

        await walAlpha.Received(1).GetWalUsageAsync(Arg.Any<CancellationToken>());
        await walBeta.Received(1).GetWalUsageAsync(Arg.Any<CancellationToken>());
        // The deep path must never be invoked by the polling fan-out.
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeStorageUsage>(default!);
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILattice>(default!);
    }

    [Test]
    public async Task PollWalUsageAsync_swallows_per_tree_failures_and_continues()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(new[] { "alpha", "beta" }));

        var factory = Substitute.For<IGrainFactory>();
        var walAlpha = Substitute.For<ILatticeWalUsage>();
        walAlpha.GetWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns<TreeWalUsageReport>(_ => throw new InvalidOperationException("alpha down"));
        var walBeta = Substitute.For<ILatticeWalUsage>();
        walBeta.GetWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new TreeWalUsageReport { TreeId = "beta", WalRetainedBytes = 2 });
        factory.GetGrain<ILatticeWalUsage>("alpha").Returns(walAlpha);
        factory.GetGrain<ILatticeWalUsage>("beta").Returns(walBeta);

        var grain = CreateGrain(factory, registry);

        Assert.That(async () => await grain.PollWalUsageAsync(CancellationToken.None), Throws.Nothing);
        await walBeta.Received(1).GetWalUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PollWalUsageAsync_with_no_trees_is_a_noop()
    {
        var factory = Substitute.For<IGrainFactory>();
        var grain = CreateGrain(factory);

        await grain.PollWalUsageAsync(CancellationToken.None);
        // No throws; no grain calls beyond the registry lookup.
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeWalUsage>(default!);
    }

    [Test]
    public async Task GetTotalStorageUsageAsync_routes_through_the_cached_deep_aggregator()
    {
        var (factory, _, storage) = SetUpDeepFactoryWithOneTree("alpha", wal: 10, snap: 20, leaf: 30);
        var grain = CreateGrain(factory, BuildRegistry("alpha"));

        var report = await grain.GetTotalStorageUsageAsync(CancellationToken.None);

        await storage.Received(1).GetReportAsync(forceRefresh: false, Arg.Any<CancellationToken>());
        Assert.That(report.TotalBytes, Is.EqualTo(60));
    }

    [Test]
    public async Task RefreshStorageUsageAsync_forces_a_cache_bypass_on_each_tree()
    {
        var (factory, _, storage) = SetUpDeepFactoryWithOneTree("alpha", wal: 10, snap: 20, leaf: 30);
        var grain = CreateGrain(factory, BuildRegistry("alpha"));

        var report = await grain.RefreshStorageUsageAsync(CancellationToken.None);

        await storage.Received(1).GetReportAsync(forceRefresh: true, Arg.Any<CancellationToken>());
        Assert.That(report.TreeCount, Is.EqualTo(1));
        Assert.That(report.TotalBytes, Is.EqualTo(60));
    }

    [Test]
    public async Task RefreshStorageUsageAsync_per_tree_failure_yields_partial_report()
    {
        var registry = BuildRegistry("alpha");
        var factory = Substitute.For<IGrainFactory>();
        var storage = Substitute.For<ILatticeStorageUsage>();
        storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns<TreeStorageUsageReport>(_ => throw new InvalidOperationException("down"));
        factory.GetGrain<ILatticeStorageUsage>("alpha").Returns(storage);

        var grain = CreateGrain(factory, registry);

        var report = await grain.RefreshStorageUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeCount, Is.EqualTo(1));
            Assert.That(report.Partial, Is.True);
            Assert.That(report.Trees[0].TreeId, Is.EqualTo("alpha"));
        });
    }

    // --- Bounded per-tree fan-out (issue #1728) ---

    [Test]
    public async Task GetTotalStorageUsageAsync_tree_fanout_never_exceeds_the_configured_bound()
    {
        const int Bound = 3;
        const int Trees = 12;

        var treeIds = Enumerable.Range(0, Trees).Select(i => $"tree-{i:D2}").ToArray();
        var inFlight = 0;
        var peak = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var factory = Substitute.For<IGrainFactory>();
        foreach (var treeId in treeIds)
        {
            var id = treeId;
            var storage = Substitute.For<ILatticeStorageUsage>();
            storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(async _ =>
            {
                var current = Interlocked.Increment(ref inFlight);
                RecordPeak(ref peak, current);
                if (current >= Bound) release.TrySetResult();
                await release.Task;
                Interlocked.Decrement(ref inFlight);
                return new TreeStorageUsageReport { TreeId = id, TotalBytes = 1, SampledAt = DateTimeOffset.UtcNow };
            });
            factory.GetGrain<ILatticeStorageUsage>(id).Returns(storage);
        }

        var grain = CreateGrain(
            factory,
            BuildRegistry(treeIds),
            new LatticeOptions { MaxConcurrentStorageUsageTrees = Bound });

        var report = await grain.GetTotalStorageUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(peak, Is.EqualTo(Bound),
                $"Expected at most {Bound} trees sampled concurrently across {Trees} registered trees.");
            // Bounding changes the schedule, not the answer.
            Assert.That(report.TreeCount, Is.EqualTo(Trees));
            Assert.That(report.TotalBytes, Is.EqualTo(Trees));
        });
    }

    [Test]
    public async Task PollWalUsageAsync_tree_fanout_never_exceeds_the_configured_bound()
    {
        const int Bound = 2;
        const int Trees = 10;

        var treeIds = Enumerable.Range(0, Trees).Select(i => $"tree-{i:D2}").ToArray();
        var inFlight = 0;
        var peak = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var factory = Substitute.For<IGrainFactory>();
        foreach (var treeId in treeIds)
        {
            var id = treeId;
            var wal = Substitute.For<ILatticeWalUsage>();
            wal.GetWalUsageAsync(Arg.Any<CancellationToken>()).Returns(async _ =>
            {
                var current = Interlocked.Increment(ref inFlight);
                RecordPeak(ref peak, current);
                if (current >= Bound) release.TrySetResult();
                await release.Task;
                Interlocked.Decrement(ref inFlight);
                return new TreeWalUsageReport { TreeId = id, WalRetainedBytes = 1 };
            });
            factory.GetGrain<ILatticeWalUsage>(id).Returns(wal);
        }

        var grain = CreateGrain(
            factory,
            BuildRegistry(treeIds),
            new LatticeOptions { MaxConcurrentStorageUsageTrees = Bound });

        await grain.PollWalUsageAsync(CancellationToken.None);

        Assert.That(peak, Is.EqualTo(Bound),
            $"Expected at most {Bound} trees polled concurrently across {Trees} registered trees.");
    }

    [Test]
    public async Task GetTotalStorageUsageAsync_bounded_rollup_preserves_registry_tree_order()
    {
        // The registry returns sorted ids and the roll-up's ordering guarantee
        // is load-bearing; a bound that completed trees out of order (for
        // example by appending results as they finish) would break it.
        var treeIds = Enumerable.Range(0, 9).Select(i => $"tree-{i:D2}").ToArray();

        var factory = Substitute.For<IGrainFactory>();
        for (var i = 0; i < treeIds.Length; i++)
        {
            var id = treeIds[i];
            // Later trees answer first, so completion order is the reverse of
            // registry order.
            var delayMs = (treeIds.Length - i) * 4;
            var storage = Substitute.For<ILatticeStorageUsage>();
            storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(async _ =>
            {
                await Task.Delay(delayMs, CancellationToken.None);
                return new TreeStorageUsageReport { TreeId = id, SampledAt = DateTimeOffset.UtcNow };
            });
            factory.GetGrain<ILatticeStorageUsage>(id).Returns(storage);
        }

        var grain = CreateGrain(
            factory,
            BuildRegistry(treeIds),
            new LatticeOptions { MaxConcurrentStorageUsageTrees = 2 });

        var report = await grain.GetTotalStorageUsageAsync(CancellationToken.None);

        Assert.That(report.Trees.Select(t => t.TreeId), Is.EqualTo(treeIds).AsCollection);
    }

    [Test]
    public async Task GetTotalStorageUsageAsync_one_failing_tree_still_yields_a_partial_cluster_report()
    {
        var treeIds = new[] { "alpha", "beta", "gamma" };
        var factory = Substitute.For<IGrainFactory>();
        foreach (var treeId in treeIds)
        {
            var id = treeId;
            var storage = Substitute.For<ILatticeStorageUsage>();
            if (id == "beta")
            {
                storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
                    .Returns<TreeStorageUsageReport>(_ => throw new TimeoutException("beta deadline"));
            }
            else
            {
                storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
                    .Returns(new TreeStorageUsageReport
                    {
                        TreeId = id,
                        LeafStateBytes = 100,
                        TotalBytes = 100,
                        SampledAt = DateTimeOffset.UtcNow,
                    });
            }
            factory.GetGrain<ILatticeStorageUsage>(id).Returns(storage);
        }

        var grain = CreateGrain(
            factory,
            BuildRegistry(treeIds),
            new LatticeOptions { MaxConcurrentStorageUsageTrees = 2 });

        var report = await grain.GetTotalStorageUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            // The roll-up completes rather than aborting...
            Assert.That(report.TreeCount, Is.EqualTo(3));
            Assert.That(report.TotalBytes, Is.EqualTo(200));
            // ...and is honest that it is missing a tree.
            Assert.That(report.Partial, Is.True);
            Assert.That(report.Trees.Select(t => t.TreeId), Is.EqualTo(treeIds).AsCollection);
        });
    }

    [Test]
    public void GetTotalStorageUsageAsync_cancelled_mid_rollup_throws_rather_than_reporting_partial_zeroes()
    {
        using var cts = new CancellationTokenSource();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var treeIds = Enumerable.Range(0, 8).Select(i => $"tree-{i:D2}").ToArray();

        var factory = Substitute.For<IGrainFactory>();
        foreach (var treeId in treeIds)
        {
            var storage = Substitute.For<ILatticeStorageUsage>();
            storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(async _ =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, cts.Token);
                return new TreeStorageUsageReport();
            });
            factory.GetGrain<ILatticeStorageUsage>(treeId).Returns(storage);
        }

        var grain = CreateGrain(
            factory,
            BuildRegistry(treeIds),
            new LatticeOptions { MaxConcurrentStorageUsageTrees = 2 });

        var pending = grain.GetTotalStorageUsageAsync(cts.Token);

        Assert.That(async () =>
        {
            await started.Task;
            await cts.CancelAsync();
            await pending;
        }, Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task PollWalUsageAsync_bounded_poll_still_visits_every_tree()
    {
        var treeIds = Enumerable.Range(0, 7).Select(i => $"tree-{i:D2}").ToArray();
        var visited = new List<string>();

        var factory = Substitute.For<IGrainFactory>();
        foreach (var treeId in treeIds)
        {
            var id = treeId;
            var wal = Substitute.For<ILatticeWalUsage>();
            wal.GetWalUsageAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            {
                lock (visited) visited.Add(id);
                return new TreeWalUsageReport { TreeId = id };
            });
            factory.GetGrain<ILatticeWalUsage>(id).Returns(wal);
        }

        var grain = CreateGrain(
            factory,
            BuildRegistry(treeIds),
            new LatticeOptions { MaxConcurrentStorageUsageTrees = 2 });

        await grain.PollWalUsageAsync(CancellationToken.None);

        lock (visited)
        {
            Assert.That(visited, Is.EquivalentTo(treeIds));
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

    private static ILatticeRegistry BuildRegistry(params string[] treeIds)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(treeIds));
        return registry;
    }

    private static (IGrainFactory Factory, ILattice Lattice, ILatticeStorageUsage Storage) SetUpDeepFactoryWithOneTree(
        string treeId, long wal, long snap, long leaf)
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        var storage = Substitute.For<ILatticeStorageUsage>();
        storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new TreeStorageUsageReport
            {
                TreeId = treeId,
                WalRetainedBytes = wal,
                SnapshotBytes = snap,
                LeafStateBytes = leaf,
                TotalBytes = wal + snap + leaf,
                SampledAt = DateTimeOffset.UtcNow,
            });
        factory.GetGrain<ILatticeStorageUsage>(treeId).Returns(storage);
        factory.GetGrain<ILattice>(treeId).Returns(lattice);
        return (factory, lattice, storage);
    }

    /// <summary>
    /// Builds a factory whose per-tree usage grains never answer: each blocks on
    /// a delay far longer than any budget under test, so the only way the call
    /// completes is the roll-up budget lapsing.
    /// </summary>
    private static IGrainFactory StallingTreeFactory(IEnumerable<string> treeIds)
    {
        var factory = Substitute.For<IGrainFactory>();
        foreach (var treeId in treeIds)
        {
            var storage = Substitute.For<ILatticeStorageUsage>();
            storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
                .Returns(call => StallAsync(call.Arg<CancellationToken>()));
            factory.GetGrain<ILatticeStorageUsage>(treeId).Returns(storage);
        }

        return factory;
    }

    private static async Task<TreeStorageUsageReport> StallAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(TimeSpan.FromMinutes(5), cancellationToken);
        throw new InvalidOperationException("The stall should always be cancelled first.");
    }

    [Test]
    public async Task GetTotalStorageUsageAsync_budget_expiry_returns_a_flagged_partial_rather_than_failing()
    {
        // Bounding the fan-out caps the burst but not the total work: a deep
        // refresh over a large catalogue cannot finish inside one response
        // deadline however gently it is dispatched. Before the budget the whole
        // call failed on the deadline and the caller learned nothing at all.
        var treeIds = new[] { "alpha", "beta", "gamma" };
        var factory = StallingTreeFactory(treeIds);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(treeIds));
        var grain = CreateGrain(
            factory,
            registry,
            new LatticeOptions { StorageUsageRollupBudget = TimeSpan.FromMilliseconds(150) });

        var report = await grain.RefreshStorageUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True, "A budget-truncated roll-up must flag itself Partial.");
            Assert.That(report.TreeCount, Is.EqualTo(treeIds.Length));
            Assert.That(report.Trees.Select(t => t.TreeId), Is.EqualTo(treeIds));
            Assert.That(report.Trees.All(t => t.Partial), Is.True);
        });
    }

    [Test]
    public async Task GetTotalStorageUsageAsync_trees_sampled_before_the_budget_keep_their_real_figures()
    {
        // The budget truncates rather than discards: whatever was sampled in
        // time is real, and only the remainder reads as not-answered.
        var treeIds = new[] { "alpha", "beta" };
        var factory = Substitute.For<IGrainFactory>();

        var fast = Substitute.For<ILatticeStorageUsage>();
        fast.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new TreeStorageUsageReport
            {
                TreeId = "alpha",
                LeafStateBytes = 400,
                TotalBytes = 400,
                SampledAt = DateTimeOffset.UtcNow,
            });
        factory.GetGrain<ILatticeStorageUsage>("alpha").Returns(fast);

        var stalled = Substitute.For<ILatticeStorageUsage>();
        stalled.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => StallAsync(call.Arg<CancellationToken>()));
        factory.GetGrain<ILatticeStorageUsage>("beta").Returns(stalled);

        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(treeIds));
        var grain = CreateGrain(
            factory,
            registry,
            new LatticeOptions { StorageUsageRollupBudget = TimeSpan.FromMilliseconds(150) });

        var report = await grain.RefreshStorageUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True);
            Assert.That(report.LeafStateBytes, Is.EqualTo(400), "The tree that answered in time must still contribute.");
            Assert.That(report.TotalBytes, Is.EqualTo(400));
            Assert.That(report.Trees.Single(t => t.TreeId == "alpha").Partial, Is.False);
            Assert.That(report.Trees.Single(t => t.TreeId == "beta").Partial, Is.True);
        });
    }

    [Test]
    public void GetTotalStorageUsageAsync_caller_cancellation_still_aborts_rather_than_reporting_partial()
    {
        // A budget expiry is a deliberate truncation; a caller-driven cancel is
        // not, and must never be laundered into a confident-looking partial.
        var treeIds = new[] { "alpha", "beta" };
        var factory = StallingTreeFactory(treeIds);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(treeIds));
        var grain = CreateGrain(
            factory,
            registry,
            new LatticeOptions { StorageUsageRollupBudget = TimeSpan.FromMinutes(5) });

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(150));

        Assert.That(
            async () => await grain.RefreshStorageUsageAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetTotalStorageUsageAsync_non_positive_budget_runs_to_completion()
    {
        // A non-positive budget disables truncation, restoring the previous
        // run-to-completion behaviour for an operator who wants it.
        var (factory, _, _) = SetUpDeepFactoryWithOneTree("alpha", wal: 1, snap: 2, leaf: 3);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync()
            .Returns(Task.FromResult<IReadOnlyList<string>>(new[] { "alpha" }));
        var grain = CreateGrain(
            factory,
            registry,
            new LatticeOptions { StorageUsageRollupBudget = TimeSpan.Zero });

        var report = await grain.GetTotalStorageUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.False);
            Assert.That(report.TotalBytes, Is.EqualTo(6));
        });
    }
}
