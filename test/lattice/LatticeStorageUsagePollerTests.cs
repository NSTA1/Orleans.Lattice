using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeStorageUsagePoller"/>: the per-silo
/// background service that drives the storage-usage gauges so they populate
/// without any caller invoking <see cref="ILattice.GetStorageUsageAsync"/>.
/// </summary>
[TestFixture]
public sealed class LatticeStorageUsagePollerTests
{
    private readonly List<LatticeStorageUsageMetrics> _createdMetrics = new();

    [TearDown]
    public void DisposeCreatedMetrics()
    {
        foreach (var metrics in _createdMetrics)
        {
            metrics.Dispose();
        }
        _createdMetrics.Clear();
    }

    private static IOptionsMonitor<LatticeOptions> Monitor(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private LatticeStorageUsagePoller CreatePoller(
        IGrainFactory factory,
        LatticeOptions options,
        out LatticeStorageUsageMetrics metrics)
    {
        metrics = new LatticeStorageUsageMetrics();
        _createdMetrics.Add(metrics);
        return new LatticeStorageUsagePoller(
            factory,
            metrics,
            Monitor(options),
            Substitute.For<Microsoft.Extensions.Logging.ILogger<LatticeStorageUsagePoller>>());
    }

    [Test]
    public async Task ExecuteAsync_disabled_interval_never_calls_admin()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.Zero }, out _);

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await poller.StopAsync(CancellationToken.None);

        await admin.DidNotReceive().PollWalUsageAsync(Arg.Any<CancellationToken>());
        await admin.DidNotReceive().GetTotalStorageUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_enabled_polls_the_admin_grain()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromMilliseconds(25) }, out _);

        await poller.StartAsync(CancellationToken.None);
        // Allow a few ticks; the first poll runs immediately then the timer
        // drives subsequent ones.
        await Task.Delay(200);
        await poller.StopAsync(CancellationToken.None);

        await admin.ReceivedWithAnyArgs().PollWalUsageAsync(default);
        // The poller must never invoke the deep fan-out path that
        // activates every leaf and snapshot grain. Asserting on
        // GetTotalStorageUsageAsync directly is the headline regression
        // gate; if the poller is ever re-pointed at the deep path this
        // test fails before any cold-tree leaf-activation regression can
        // escape into CI.
        await admin.DidNotReceive().GetTotalStorageUsageAsync(Arg.Any<CancellationToken>());
        await admin.DidNotReceive().RefreshStorageUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_enabled_sets_sink_staleness_horizon()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        // A poll interval whose 4x multiple exceeds the 60s default so we can
        // observe the poller widening the horizon off the cadence.
        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromSeconds(30) }, out var metrics);

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await poller.StopAsync(CancellationToken.None);

        Assert.That(metrics.StalenessHorizon, Is.EqualTo(TimeSpan.FromSeconds(120)),
            "the poller sizes the sink staleness horizon to 4x the poll interval");
    }

    [Test]
    public async Task ExecuteAsync_short_interval_floors_horizon_at_the_default()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        // 5s * 4 = 20s < the 60s default, so the poller must clamp the
        // horizon up to the floor rather than letting a fast poll cadence
        // shrink the window below what tolerates a few missed polls.
        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromSeconds(5) }, out var metrics);

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await poller.StopAsync(CancellationToken.None);

        Assert.That(metrics.StalenessHorizon, Is.EqualTo(LatticeStorageUsageMetrics.DefaultStalenessHorizon),
            "a poll interval whose 4x multiple is below the default horizon must floor at the default");
    }

    [Test]
    public async Task ExecuteAsync_admin_failure_does_not_crash_the_poller()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ => throw new InvalidOperationException("registry not ready"));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromMilliseconds(25) }, out _);

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(150);
        // The poller swallows the fault and keeps ticking; stopping is clean.
        Assert.That(async () => await poller.StopAsync(CancellationToken.None), Throws.Nothing);

        await admin.ReceivedWithAnyArgs().PollWalUsageAsync(default);
    }

    [Test]
    public async Task ExecuteAsync_deep_interval_enabled_drives_the_non_force_deep_aggregator()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageReport());
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(
            factory,
            new LatticeOptions
            {
                StorageUsagePollInterval = TimeSpan.FromMilliseconds(25),
                StorageUsageDeepPollInterval = TimeSpan.FromMilliseconds(25),
            },
            out _);

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await poller.StopAsync(CancellationToken.None);

        await admin.ReceivedWithAnyArgs().PollWalUsageAsync(default);
        await admin.ReceivedWithAnyArgs().GetTotalStorageUsageAsync(default);
        // The deep poll uses the non-force aggregator; it must never invoke the
        // operator-driven force-refresh that re-walks every leaf.
        await admin.DidNotReceive().RefreshStorageUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_deep_only_polls_the_deep_path_without_the_wal_poll()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageReport());
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(
            factory,
            new LatticeOptions
            {
                StorageUsagePollInterval = TimeSpan.Zero,
                StorageUsageDeepPollInterval = TimeSpan.FromMilliseconds(25),
            },
            out _);

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await poller.StopAsync(CancellationToken.None);

        await admin.ReceivedWithAnyArgs().GetTotalStorageUsageAsync(default);
        await admin.DidNotReceive().PollWalUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_sizes_horizon_off_the_slower_deep_cadence()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageReport());
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        // WAL 5s would floor the horizon at 60s, but the slower 30s deep
        // cadence widens it to 4x30s = 120s so a deep series survives a few
        // missed deep polls.
        var poller = CreatePoller(
            factory,
            new LatticeOptions
            {
                StorageUsagePollInterval = TimeSpan.FromSeconds(5),
                StorageUsageDeepPollInterval = TimeSpan.FromSeconds(30),
            },
            out var metrics);

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await poller.StopAsync(CancellationToken.None);

        Assert.That(metrics.StalenessHorizon, Is.EqualTo(TimeSpan.FromSeconds(120)),
            "the poller sizes the sink staleness horizon off the slower of the WAL and deep cadences");
    }

    // --- Crash-safety: the poller must never stop the host (issue #1728) ---

    [Test]
    public async Task ExecuteAsync_out_of_range_poll_interval_does_not_fault_the_background_service()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        // PeriodicTimer rejects any period beyond ~49.7 days. Constructing it
        // outside the loop's try turned an out-of-range knob into a faulted
        // ExecuteAsync, which BackgroundService's default StopHost behaviour
        // escalates into a host-wide outage.
        var poller = CreatePoller(
            factory,
            new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromDays(365) },
            out _);

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await poller.StopAsync(CancellationToken.None);

        Assert.That(poller.ExecuteTask, Is.Not.Null);
        Assert.That(poller.ExecuteTask!.IsFaulted, Is.False,
            "an out-of-range poll interval must degrade the poller, not fault the background service");
        // The immediate first poll still runs; only the wait cadence is clamped.
        await admin.ReceivedWithAnyArgs().PollWalUsageAsync(default);
    }

    [Test]
    public async Task ExecuteAsync_options_read_failure_does_not_fault_the_background_service()
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(Substitute.For<ILatticeAdmin>());

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(_ => throw new OptionsValidationException(
            Options.DefaultName, typeof(LatticeOptions), ["bad options"]));

        var metrics = new LatticeStorageUsageMetrics();
        _createdMetrics.Add(metrics);
        var poller = new LatticeStorageUsagePoller(
            factory,
            metrics,
            monitor,
            Substitute.For<Microsoft.Extensions.Logging.ILogger<LatticeStorageUsagePoller>>());

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await poller.StopAsync(CancellationToken.None);

        Assert.That(poller.ExecuteTask, Is.Not.Null);
        Assert.That(poller.ExecuteTask!.IsFaulted, Is.False,
            "an unusable options value must not take the host down with the poller");
    }
}