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
    private static IOptionsMonitor<LatticeOptions> Monitor(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static LatticeStorageUsagePoller CreatePoller(
        IGrainFactory factory,
        LatticeOptions options,
        out LatticeStorageUsageMetrics metrics)
    {
        metrics = new LatticeStorageUsageMetrics();
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

        await admin.DidNotReceive().GetTotalStorageUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_enabled_polls_the_admin_grain()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(default(ClusterStorageUsageReport));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromMilliseconds(25) }, out _);

        await poller.StartAsync(CancellationToken.None);
        // Allow a few ticks; the first poll runs immediately then the timer
        // drives subsequent ones.
        await Task.Delay(200);
        await poller.StopAsync(CancellationToken.None);

        await admin.ReceivedWithAnyArgs().GetTotalStorageUsageAsync(default);
    }

    [Test]
    public async Task ExecuteAsync_enabled_sets_sink_staleness_horizon()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(default(ClusterStorageUsageReport));
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
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(default(ClusterStorageUsageReport));
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
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns<ClusterStorageUsageReport>(_ => throw new InvalidOperationException("registry not ready"));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromMilliseconds(25) }, out _);

        await poller.StartAsync(CancellationToken.None);
        await Task.Delay(150);
        // The poller swallows the fault and keeps ticking; stopping is clean.
        Assert.That(async () => await poller.StopAsync(CancellationToken.None), Throws.Nothing);

        await admin.ReceivedWithAnyArgs().GetTotalStorageUsageAsync(default);
    }
}