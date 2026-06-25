using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeWalGcScheduler"/>: the per-silo
/// background service that drives the WAL garbage collector for every
/// registered tree on the <see cref="LatticeOptions.WalGcInterval"/>
/// cadence, closing the retention gap for durable-WAL hosts that run
/// without the replication package and for non-replicated trees in a
/// replicated host (issue #920).
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeWalGcSchedulerTests
{
    private static IOptionsMonitor<LatticeOptions> Monitor(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IGrainFactory FactoryWithTrees(out ILatticeRegistry registry, params string[] treeIds)
    {
        registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(treeIds));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return factory;
    }

    private static LatticeWalGcScheduler CreateScheduler(
        IGrainFactory factory,
        ILatticeWalGc gc,
        LatticeOptions options)
        => new(
            factory,
            gc,
            Monitor(options),
            Substitute.For<Microsoft.Extensions.Logging.ILogger<LatticeWalGcScheduler>>());

    [Test]
    public async Task ExecuteAsync_disabled_interval_never_runs_a_gc_pass()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var factory = FactoryWithTrees(out var registry, "alpha", "beta");

        var scheduler = CreateScheduler(factory, gc, new LatticeOptions { WalGcInterval = TimeSpan.Zero });

        await scheduler.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await scheduler.StopAsync(CancellationToken.None);

        await registry.DidNotReceive().GetAllTreeIdsAsync();
        await gc.DidNotReceiveWithAnyArgs().RunOnceAsync(default!, default);
    }

    [Test]
    public async Task ExecuteAsync_negative_interval_never_runs_a_gc_pass()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var factory = FactoryWithTrees(out _, "alpha");

        var scheduler = CreateScheduler(
            factory, gc, new LatticeOptions { WalGcInterval = TimeSpan.FromSeconds(-1) });

        await scheduler.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await scheduler.StopAsync(CancellationToken.None);

        await gc.DidNotReceiveWithAnyArgs().RunOnceAsync(default!, default);
    }

    [Test]
    public async Task ExecuteAsync_enabled_runs_gc_for_every_registered_tree()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new LatticeWalGcReport());
        var factory = FactoryWithTrees(out _, "alpha", "beta");

        var scheduler = CreateScheduler(
            factory, gc, new LatticeOptions { WalGcInterval = TimeSpan.FromMilliseconds(25) });

        await scheduler.StartAsync(CancellationToken.None);
        // The first pass runs immediately; the timer drives subsequent ones.
        await Task.Delay(200);
        await scheduler.StopAsync(CancellationToken.None);

        // Both the replicated-or-not trees the registry reports must be
        // collected - this is the headline issue-#920 gate: a tree no
        // other driver collects is now GC'd by the core scheduler.
        await gc.Received().RunOnceAsync("alpha", Arg.Any<CancellationToken>());
        await gc.Received().RunOnceAsync("beta", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_empty_registry_runs_no_gc_pass()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var factory = FactoryWithTrees(out var registry);

        var scheduler = CreateScheduler(
            factory, gc, new LatticeOptions { WalGcInterval = TimeSpan.FromMilliseconds(25) });

        await scheduler.StartAsync(CancellationToken.None);
        await Task.Delay(150);
        await scheduler.StopAsync(CancellationToken.None);

        await registry.ReceivedWithAnyArgs().GetAllTreeIdsAsync();
        await gc.DidNotReceiveWithAnyArgs().RunOnceAsync(default!, default);
    }

    [Test]
    public async Task ExecuteAsync_skips_null_or_empty_tree_ids()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new LatticeWalGcReport());
        var factory = FactoryWithTrees(out _, "alpha", "", "beta");

        var scheduler = CreateScheduler(
            factory, gc, new LatticeOptions { WalGcInterval = TimeSpan.FromMilliseconds(25) });

        await scheduler.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await scheduler.StopAsync(CancellationToken.None);

        await gc.Received().RunOnceAsync("alpha", Arg.Any<CancellationToken>());
        await gc.Received().RunOnceAsync("beta", Arg.Any<CancellationToken>());
        await gc.DidNotReceive().RunOnceAsync("", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_per_tree_failure_does_not_stop_the_remaining_trees()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync("alpha", Arg.Any<CancellationToken>())
            .Returns<Task<LatticeWalGcReport>>(_ => throw new InvalidOperationException("wal wedged"));
        gc.RunOnceAsync("beta", Arg.Any<CancellationToken>())
            .Returns(new LatticeWalGcReport());
        var factory = FactoryWithTrees(out _, "alpha", "beta");

        var scheduler = CreateScheduler(
            factory, gc, new LatticeOptions { WalGcInterval = TimeSpan.FromMilliseconds(25) });

        await scheduler.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        // A wedged tree must not crash the scheduler; stopping is clean.
        Assert.That(async () => await scheduler.StopAsync(CancellationToken.None), Throws.Nothing);

        // The sibling tree is still collected despite alpha throwing.
        await gc.Received().RunOnceAsync("beta", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_registry_failure_does_not_crash_the_scheduler()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync()
            .Returns<Task<IReadOnlyList<string>>>(_ => throw new InvalidOperationException("registry not ready"));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var scheduler = CreateScheduler(
            factory, gc, new LatticeOptions { WalGcInterval = TimeSpan.FromMilliseconds(25) });

        await scheduler.StartAsync(CancellationToken.None);
        await Task.Delay(150);
        Assert.That(async () => await scheduler.StopAsync(CancellationToken.None), Throws.Nothing);

        await registry.ReceivedWithAnyArgs().GetAllTreeIdsAsync();
        await gc.DidNotReceiveWithAnyArgs().RunOnceAsync(default!, default);
    }

    [Test]
    public void WalGcInterval_defaults_to_one_hour_enabling_the_scheduler()
    {
        var options = new LatticeOptions();
        Assert.That(options.WalGcInterval, Is.EqualTo(TimeSpan.FromHours(1)),
            "the core WAL GC scheduler must default on (hourly) so durable-WAL hosts get bounded WAL retention out of the box.");
        Assert.That(LatticeOptions.DefaultWalGcInterval, Is.EqualTo(TimeSpan.FromHours(1)));
    }
}
