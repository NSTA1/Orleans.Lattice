using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;

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

    /// <summary>
    /// Thread-safe tally of admin calls made from the poller's background
    /// loops. The loops run on the thread pool, so the assertions need a
    /// happens-after signal they can poll rather than a fixed sleep that
    /// merely hopes the loop got there first.
    /// </summary>
    private sealed class CallCounter
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Increment() => Interlocked.Increment(ref _count);
    }

    /// <summary>
    /// Awaits <paramref name="task"/> under a bounded deadline, failing the
    /// test with <paramref name="because"/> rather than hanging the run if
    /// the task never settles.
    /// </summary>
    private static async Task AwaitCompletionAsync(Task task, string because, int timeoutMs = 10000)
    {
        var settled = await Task.WhenAny(task, Task.Delay(timeoutMs));
        Assert.That(settled, Is.SameAs(task), because);
        await task;
    }

    [Test]
    public async Task ExecuteAsync_disabled_interval_never_calls_admin()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.Zero }, out _);

        await poller.StartAsync(CancellationToken.None);

        // With both cadences disabled the service body returns immediately,
        // so its completion is a real happens-after barrier: once
        // ExecuteTask has settled the poller provably had its one and only
        // chance to reach the admin grain. That is strictly stronger than
        // sleeping for a fixed window and hoping it would have polled.
        await AwaitCompletionAsync(
            poller.ExecuteTask!,
            "a fully disabled poller must return from its service body instead of spinning a loop");
        await poller.StopAsync(CancellationToken.None);

        await admin.DidNotReceive().PollWalUsageAsync(Arg.Any<CancellationToken>());
        await admin.DidNotReceive().GetTotalStorageUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_enabled_polls_the_admin_grain()
    {
        var walPolls = new CallCounter();
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            walPolls.Increment();
            return Task.CompletedTask;
        });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromMilliseconds(25) }, out _);

        await poller.StartAsync(CancellationToken.None);
        // Wait for the poll to actually be observed; the first poll runs
        // immediately then the timer drives subsequent ones. Polling for
        // the evidence keeps the test fast when the loop is prompt and
        // patient when a loaded CI agent delays the thread pool.
        await TestPoll.UntilAsync(() => walPolls.Count > 0,
            "an enabled poller must reach the admin grain's WAL poll");
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
        var walPolls = new CallCounter();
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            walPolls.Increment();
            return Task.CompletedTask;
        });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        // A poll interval whose 4x multiple exceeds the 60s default so we can
        // observe the poller widening the horizon off the cadence.
        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromSeconds(30) }, out var metrics);

        await poller.StartAsync(CancellationToken.None);
        // The horizon is sized before the poll loops are started, so the
        // first observed poll is a sound happens-after signal for it -
        // unlike a fixed sleep, which asserts on whatever the loop
        // happened to have reached.
        await TestPoll.UntilAsync(() => walPolls.Count > 0,
            "the poller must start its WAL loop, which happens only after the horizon is sized");
        await poller.StopAsync(CancellationToken.None);

        Assert.That(metrics.StalenessHorizon, Is.EqualTo(TimeSpan.FromSeconds(120)),
            "the poller sizes the sink staleness horizon to 4x the poll interval");
    }

    [Test]
    public async Task ExecuteAsync_short_interval_floors_horizon_at_the_default()
    {
        var walPolls = new CallCounter();
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            walPolls.Increment();
            return Task.CompletedTask;
        });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        // 5s * 4 = 20s < the 60s default, so the poller must clamp the
        // horizon up to the floor rather than letting a fast poll cadence
        // shrink the window below what tolerates a few missed polls.
        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromSeconds(5) }, out var metrics);

        await poller.StartAsync(CancellationToken.None);
        await TestPoll.UntilAsync(() => walPolls.Count > 0,
            "the poller must start its WAL loop, which happens only after the horizon is sized");
        await poller.StopAsync(CancellationToken.None);

        Assert.That(metrics.StalenessHorizon, Is.EqualTo(LatticeStorageUsageMetrics.DefaultStalenessHorizon),
            "a poll interval whose 4x multiple is below the default horizon must floor at the default");
    }

    [Test]
    public async Task ExecuteAsync_admin_failure_does_not_crash_the_poller()
    {
        var walPolls = new CallCounter();
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ =>
            {
                walPolls.Increment();
                throw new InvalidOperationException("registry not ready");
            });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);

        var poller = CreatePoller(factory, new LatticeOptions { StorageUsagePollInterval = TimeSpan.FromMilliseconds(25) }, out _);

        await poller.StartAsync(CancellationToken.None);
        // "Swallows the fault and keeps ticking" is only demonstrated by a
        // second tick arriving after the first one threw, so wait for that
        // rather than for an arbitrary number of milliseconds.
        await TestPoll.UntilAsync(() => walPolls.Count >= 2,
            "the poller must swallow a failing poll and tick again");
        // The poller swallows the fault and keeps ticking; stopping is clean.
        Assert.That(async () => await poller.StopAsync(CancellationToken.None), Throws.Nothing);

        await admin.ReceivedWithAnyArgs().PollWalUsageAsync(default);
        Assert.That(poller.ExecuteTask!.IsFaulted, Is.False,
            "a failing admin call must not fault the background service");
    }

    [Test]
    public async Task ExecuteAsync_deep_interval_enabled_drives_the_non_force_deep_aggregator()
    {
        var walPolls = new CallCounter();
        var deepPolls = new CallCounter();
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            walPolls.Increment();
            return Task.CompletedTask;
        });
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                deepPolls.Increment();
                return new ClusterStorageUsageReport();
            });
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
        await TestPoll.UntilAsync(() => walPolls.Count > 0 && deepPolls.Count > 0,
            "both the WAL loop and the deep loop must reach the admin grain");
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
        var deepPolls = new CallCounter();
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                deepPolls.Increment();
                return new ClusterStorageUsageReport();
            });
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
        // Waiting on several deep ticks gives the (incorrectly-started) WAL
        // loop far more opportunity to fire than a fixed sleep did, so the
        // negative assertion below is stronger, not weaker.
        await TestPoll.UntilAsync(() => deepPolls.Count >= 3,
            "the deep loop must reach the admin grain repeatedly on its own cadence");
        await poller.StopAsync(CancellationToken.None);

        await admin.ReceivedWithAnyArgs().GetTotalStorageUsageAsync(default);
        await admin.DidNotReceive().PollWalUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_sizes_horizon_off_the_slower_deep_cadence()
    {
        var walPolls = new CallCounter();
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            walPolls.Increment();
            return Task.CompletedTask;
        });
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
        await TestPoll.UntilAsync(() => walPolls.Count > 0,
            "the poller must start its poll loops, which happens only after the horizon is sized");
        await poller.StopAsync(CancellationToken.None);

        Assert.That(metrics.StalenessHorizon, Is.EqualTo(TimeSpan.FromSeconds(120)),
            "the poller sizes the sink staleness horizon off the slower of the WAL and deep cadences");
    }

    // --- Crash-safety: the poller must never stop the host (issue #1728) ---

    [Test]
    public async Task ExecuteAsync_out_of_range_poll_interval_does_not_fault_the_background_service()
    {
        var walPolls = new CallCounter();
        var admin = Substitute.For<ILatticeAdmin>();
        admin.PollWalUsageAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            walPolls.Increment();
            return Task.CompletedTask;
        });
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
        // The immediate first poll still runs; only the wait cadence is clamped.
        await TestPoll.UntilAsync(() => walPolls.Count > 0,
            "the clamped interval must still let the immediate first poll run");
        await poller.StopAsync(CancellationToken.None);

        Assert.That(poller.ExecuteTask, Is.Not.Null);
        Assert.That(poller.ExecuteTask!.IsFaulted, Is.False,
            "an out-of-range poll interval must degrade the poller, not fault the background service");
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

        // The options read is the first thing the service body does, so the
        // body settles as soon as the throw is swallowed. Awaiting that
        // settle proves the failure was actually reached and handled -
        // a fixed sleep proved neither.
        await AwaitCompletionAsync(
            poller.ExecuteTask!,
            "an unusable options value must be swallowed and end the service body, not hang it");
        await poller.StopAsync(CancellationToken.None);

        Assert.That(poller.ExecuteTask, Is.Not.Null);
        Assert.That(poller.ExecuteTask!.IsFaulted, Is.False,
            "an unusable options value must not take the host down with the poller");
    }
}