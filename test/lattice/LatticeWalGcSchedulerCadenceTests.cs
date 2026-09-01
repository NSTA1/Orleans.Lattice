using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Cadence and metering tests for <see cref="LatticeWalGcScheduler"/> (issue
/// #1832): the bounded startup stagger, the per-tree interval that shortens
/// under backlog and relaxes when it clears, per-tree failure isolation, and
/// the instruments the WAL-reclamation measurement rigs read.
/// <para>
/// Everything is driven off an injected <see cref="VirtualTimeProvider"/>, so
/// no assertion depends on wall-clock time, thread-pool latency, or test
/// ordering. The scheduler observes backlog through
/// <see cref="LatticeWalGcReport.EntriesTrimmed"/> - what the collector's own
/// predicate found above the trim floor - so these tests change <i>when</i> a
/// pass runs and never what one is allowed to reclaim.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeWalGcSchedulerCadenceTests
{
    private static readonly TimeSpan Ceiling = TimeSpan.FromHours(1);
    private static readonly TimeSpan Floor = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A small but positive startup stagger for the cadence fixtures. It has to
    /// be positive: <c>Task.Delay(TimeSpan.Zero, ...)</c> completes without ever
    /// arming a timer, so a zero stagger would run the first pass during the
    /// service start rather than on a step the test drives.
    /// </summary>
    private static readonly TimeSpan Stagger = TimeSpan.FromSeconds(1);

    private static IOptionsMonitor<LatticeOptions> Monitor(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>Options with the adaptive band pinned and a tiny stagger, for deterministic cadence assertions.</summary>
    private static LatticeOptions Adaptive(TimeSpan? floor = null, TimeSpan? ceiling = null) => new()
    {
        WalGcInterval = ceiling ?? Ceiling,
        WalGcMinInterval = floor ?? Floor,
        WalGcStartupDelay = Stagger,
    };

    private static IGrainFactory FactoryFor(Func<IReadOnlyList<string>> trees)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(_ => Task.FromResult(trees()));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return factory;
    }

    private static IGrainFactory FactoryWithTrees(params string[] treeIds) =>
        FactoryFor(() => treeIds);

    private static LatticeWalGcScheduler CreateScheduler(
        IGrainFactory factory,
        ILatticeWalGc gc,
        LatticeOptions options,
        TimeProvider time)
        => new(
            factory,
            gc,
            Monitor(options),
            Substitute.For<Microsoft.Extensions.Logging.ILogger<LatticeWalGcScheduler>>(),
            time);

    private static LatticeWalGcReport Report(long entriesTrimmed, long? retainedBytesAfter = null) =>
        new("tree", null, null, null, null, 1, entriesTrimmed, null, null, retainedBytesAfter);

    private static Task Parked(Task parked) => parked.WaitAsync(TimeSpan.FromSeconds(30));

    /// <summary>
    /// Starts the scheduler and waits until it has armed its startup delay.
    /// <see cref="Microsoft.Extensions.Hosting.BackgroundService.StartAsync"/>
    /// does not guarantee that <c>ExecuteAsync</c> has reached its first delay
    /// by the time it returns, so every test synchronises on the arm event
    /// rather than assuming a synchronous start.
    /// </summary>
    private static async Task StartArmedAsync(LatticeWalGcScheduler scheduler, VirtualTimeProvider time)
    {
        var armed = time.NextTimerAsync();
        await scheduler.StartAsync(CancellationToken.None);
        await Parked(armed);
    }

    /// <summary>
    /// Starts the scheduler and drives it through its first pass, whatever
    /// startup stagger it drew, leaving it parked on its first cadence delay.
    /// The advance is by the exact offset the scheduler asked for, so the step
    /// is deterministic even when that offset is randomized.
    /// </summary>
    private static async Task StartAndRunFirstPassAsync(
        LatticeWalGcScheduler scheduler,
        VirtualTimeProvider time)
    {
        await StartArmedAsync(scheduler, time);
        await TickAsync(time);
    }

    /// <summary>Fires the currently parked delay and waits for the scheduler to park on the next one.</summary>
    private static async Task TickAsync(VirtualTimeProvider time)
    {
        var next = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(next);
    }

    // ---------------------------------------------------------------- startup

    [Test]
    public async Task ExecuteAsync_runs_the_first_pass_within_the_bounded_startup_window()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(0));
        var time = new VirtualTimeProvider();

        var options = new LatticeOptions
        {
            WalGcInterval = Ceiling,
            WalGcStartupDelay = TimeSpan.FromSeconds(30),
        };
        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, options, time);

        await StartArmedAsync(scheduler, time);

        var startup = time.ScheduledDelays[0];
        Assert.Multiple(() =>
        {
            // The whole point of #1832: the first pass is in the low tens of
            // seconds, not the 30-to-60 minutes the old [interval/2, interval)
            // stagger produced at the default hourly cadence.
            Assert.That(startup, Is.GreaterThanOrEqualTo(TimeSpan.FromSeconds(15)),
                "the first pass must still clear the silo's activation window.");
            Assert.That(startup, Is.LessThan(TimeSpan.FromSeconds(30)),
                "the first pass must land inside the configured startup window.");
        });

        var parked = time.NextTimerAsync();
        time.Advance(TimeSpan.FromSeconds(30));
        await Parked(parked);

        await gc.Received().RunOnceAsync("alpha", Arg.Any<CancellationToken>());
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_caps_the_startup_window_at_the_configured_interval()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(0));
        var time = new VirtualTimeProvider();

        // A host that asks for a 10-second cadence must not be made to wait a
        // 30-second startup window for its first pass.
        var options = new LatticeOptions
        {
            WalGcInterval = TimeSpan.FromSeconds(10),
            WalGcStartupDelay = TimeSpan.FromSeconds(30),
        };
        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, options, time);

        await StartArmedAsync(scheduler, time);

        Assert.That(time.ScheduledDelays[0], Is.GreaterThanOrEqualTo(TimeSpan.FromSeconds(5))
            .And.LessThan(TimeSpan.FromSeconds(10)));

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_zero_startup_delay_asks_for_no_stagger_at_all()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 4));
        var time = new VirtualTimeProvider();

        var options = new LatticeOptions
        {
            WalGcInterval = Ceiling,
            WalGcMinInterval = Floor,
            WalGcStartupDelay = TimeSpan.Zero,
        };
        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, options, time);

        await StartArmedAsync(scheduler, time);

        // The documented opt-out. No stagger is awaited at all, so the very
        // first delay this scheduler ever arms is already its post-pass cadence
        // and the pass ran without the virtual clock moving by a single tick.
        Assert.That(time.ScheduledDelays[0], Is.EqualTo(Floor));
        await gc.Received(1).RunOnceAsync("alpha", Arg.Any<CancellationToken>());

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_de_correlates_the_first_pass_across_schedulers()
    {
        const int Silos = 8;
        var schedulers = new List<LatticeWalGcScheduler>(Silos);
        var startupDelays = new List<TimeSpan>(Silos);
        var options = new LatticeOptions
        {
            WalGcInterval = Ceiling,
            WalGcStartupDelay = TimeSpan.FromSeconds(30),
        };

        try
        {
            for (var i = 0; i < Silos; i++)
            {
                var gc = Substitute.For<ILatticeWalGc>();
                gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(0));
                var time = new VirtualTimeProvider();
                var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, options, time);
                schedulers.Add(scheduler);

                await StartArmedAsync(scheduler, time);
                startupDelays.Add(time.ScheduledDelays[0]);
            }

            Assert.Multiple(() =>
            {
                // Every silo stays inside the window ...
                Assert.That(startupDelays, Is.All.GreaterThanOrEqualTo(TimeSpan.FromSeconds(15)));
                Assert.That(startupDelays, Is.All.LessThan(TimeSpan.FromSeconds(30)));

                // ... but identically configured silos must not align their first
                // fan-out, or a rolling restart reproduces the correlated I/O
                // storm the stagger exists to prevent. The offsets are drawn from
                // a continuous distribution, so identical draws across eight
                // silos are not a realistic outcome.
                Assert.That(
                    startupDelays.Distinct().Count(),
                    Is.GreaterThan(1),
                    "identically configured schedulers must draw independent startup offsets.");
            });
        }
        finally
        {
            foreach (var scheduler in schedulers)
            {
                await scheduler.StopAsync(CancellationToken.None);
            }
        }
    }

    // ---------------------------------------------------------------- cadence

    [Test]
    public async Task ExecuteAsync_holds_the_floor_while_a_backlog_is_being_reclaimed()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 5_000));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // A tree that keeps reclaiming keeps being collected at the responsive
        // floor rather than waiting out the hourly ceiling.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
        for (var pass = 0; pass < 4; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
        }

        await gc.Received(5).RunOnceAsync("alpha", Arg.Any<CancellationToken>());
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_relaxes_toward_the_interval_once_the_backlog_clears()
    {
        var trimmed = 5_000L;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(trimmed)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));

        // The log goes quiet: nothing is left above the trim floor.
        trimmed = 0;

        TimeSpan[] expected =
        [
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(4),
            TimeSpan.FromMinutes(8),
            TimeSpan.FromMinutes(16),
            TimeSpan.FromMinutes(32),
        ];
        foreach (var interval in expected)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(interval));
        }

        // ... and settles at the configured ceiling, never above it.
        await TickAsync(time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Ceiling));
        await TickAsync(time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Ceiling),
            "the configured interval is the upper bound on the quiet-path tick.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_snaps_back_to_the_floor_when_a_backlog_returns()
    {
        var trimmed = 0L;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(trimmed)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Let the tree relax well away from the floor first.
        await TickAsync(time);
        await TickAsync(time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(4)));

        // Writes resume and the next pass finds backlog above the trim floor.
        trimmed = 1;
        await TickAsync(time);

        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
            "a growing log must be collected promptly rather than waiting out the relaxed interval.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_throwing_tree_does_not_stall_the_pass_or_the_cadence()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync("wedged", Arg.Any<CancellationToken>())
            .Returns<Task<LatticeWalGcReport>>(_ => throw new InvalidOperationException("wal wedged"));
        gc.RunOnceAsync("healthy", Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 42));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("wedged", "healthy"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // The wedged tree is visited first and throws; the healthy sibling is
        // still collected in the same pass, and the wedged tree's own backoff
        // does not push the silo's cadence off the healthy tree's floor.
        await gc.Received(1).RunOnceAsync("healthy", Arg.Any<CancellationToken>());
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));

        for (var pass = 1; pass <= 3; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
                "one wedged tree must never stall the cadence for the rest.");
        }

        await gc.Received(4).RunOnceAsync("healthy", Arg.Any<CancellationToken>());
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_busy_tree_keeps_its_floor_while_an_idle_sibling_relaxes()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync("idle", Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 0));
        gc.RunOnceAsync("busy", Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 9));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("idle", "busy"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Pass 1 collects both. The idle tree relaxes to 60s, the busy one stays
        // at the 30s floor, so the silo wakes on the busy tree's schedule.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));

        await TickAsync(time);

        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
        await gc.Received(2).RunOnceAsync("busy", Arg.Any<CancellationToken>());
        await gc.Received(1).RunOnceAsync("idle", Arg.Any<CancellationToken>());

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_zero_min_interval_restores_a_fixed_interval_tick()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 5_000));
        var time = new VirtualTimeProvider();

        // The documented kill switch: no adaptive band, just the configured tick.
        var options = new LatticeOptions
        {
            WalGcInterval = Ceiling,
            WalGcMinInterval = TimeSpan.Zero,
            WalGcStartupDelay = Stagger,
        };
        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, options, time);

        await StartAndRunFirstPassAsync(scheduler, time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Ceiling));

        await TickAsync(time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Ceiling),
            "with the adaptive cadence disabled a reclaiming tree must still tick at the fixed interval.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_min_interval_above_the_interval_is_clamped_to_the_interval()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 1));
        var time = new VirtualTimeProvider();

        var options = new LatticeOptions
        {
            WalGcInterval = TimeSpan.FromMinutes(5),
            WalGcMinInterval = TimeSpan.FromHours(2),
            WalGcStartupDelay = Stagger,
        };
        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, options, time);

        await StartAndRunFirstPassAsync(scheduler, time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(5)),
            "a floor above the ceiling is meaningless and collapses the band to the interval.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_collects_a_newly_registered_tree_on_the_pass_it_first_appears()
    {
        var trees = new List<string> { "alpha" };
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 3));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryFor(() => trees.ToArray()), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        trees.Add("gamma");
        await TickAsync(time);

        // A tree that was not registered when the previous pass ran must not
        // have to wait out an interval it was never scheduled in.
        await gc.Received(1).RunOnceAsync("gamma", Arg.Any<CancellationToken>());
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_stops_collecting_a_tree_the_registry_no_longer_reports()
    {
        var trees = new List<string> { "alpha", "retired" };
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 3));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryFor(() => trees.ToArray()), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await gc.Received(1).RunOnceAsync("retired", Arg.Any<CancellationToken>());

        trees.Remove("retired");
        await TickAsync(time);
        await TickAsync(time);

        await gc.Received(1).RunOnceAsync("retired", Arg.Any<CancellationToken>());
        await gc.Received(3).RunOnceAsync("alpha", Arg.Any<CancellationToken>());
        await scheduler.StopAsync(CancellationToken.None);
    }

    // --------------------------------------------------------------- metering

    [Test]
    public async Task ExecuteAsync_publishes_a_reclaimed_pass_outcome_and_the_selected_interval()
    {
        const string Tree = "walgc-metering-reclaimed";
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 12));
        var time = new VirtualTimeProvider();

        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Tree);
        using var intervals = new InstrumentRecorder(LatticeMetrics.WalGcInterval, Tree);

        var scheduler = CreateScheduler(FactoryWithTrees(Tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(passes.Measurements, Has.Count.EqualTo(1));
            Assert.That(passes.Measurements[0].Value, Is.EqualTo(1));
            Assert.That(passes.Measurements[0].Tag(LatticeMetrics.TagOutcome), Is.EqualTo("reclaimed"));
            Assert.That(intervals.Measurements, Has.Count.EqualTo(1));
            Assert.That(intervals.Measurements[0].Value, Is.EqualTo(Floor.TotalSeconds));
        });
    }

    [Test]
    public async Task ExecuteAsync_publishes_an_idle_pass_outcome_and_the_relaxed_interval()
    {
        const string Tree = "walgc-metering-idle";
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(entriesTrimmed: 0));
        var time = new VirtualTimeProvider();

        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Tree);
        using var intervals = new InstrumentRecorder(LatticeMetrics.WalGcInterval, Tree);

        var scheduler = CreateScheduler(FactoryWithTrees(Tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(passes.Measurements, Has.Count.EqualTo(1));
            Assert.That(passes.Measurements[0].Tag(LatticeMetrics.TagOutcome), Is.EqualTo("idle"));
            Assert.That(intervals.Measurements[0].Value, Is.EqualTo(TimeSpan.FromMinutes(1).TotalSeconds));
        });
    }

    [Test]
    public async Task ExecuteAsync_publishes_a_failed_pass_outcome_when_a_tree_throws()
    {
        const string Tree = "walgc-metering-failed";
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Tree, Arg.Any<CancellationToken>())
            .Returns<Task<LatticeWalGcReport>>(_ => throw new InvalidOperationException("wal wedged"));
        var time = new VirtualTimeProvider();

        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Tree);
        using var backlog = new InstrumentRecorder(LatticeMetrics.WalGcBacklogBytes, Tree);

        var scheduler = CreateScheduler(FactoryWithTrees(Tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(passes.Measurements, Has.Count.EqualTo(1));
            Assert.That(passes.Measurements[0].Tag(LatticeMetrics.TagOutcome), Is.EqualTo("failed"),
                "a wedged tree must be visible as a failed pass rather than an idle one.");
            Assert.That(backlog.Measurements, Is.Empty);
        });
    }

    [Test]
    public async Task ExecuteAsync_publishes_backlog_bytes_when_the_provider_accounts_bytes()
    {
        const string Tree = "walgc-metering-backlog";
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Report(entriesTrimmed: 7, retainedBytesAfter: 4_096));
        var time = new VirtualTimeProvider();

        using var backlog = new InstrumentRecorder(LatticeMetrics.WalGcBacklogBytes, Tree);

        var scheduler = CreateScheduler(FactoryWithTrees(Tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(backlog.Measurements, Has.Count.EqualTo(1));
        Assert.That(backlog.Measurements[0].Value, Is.EqualTo(4_096));
    }

    [Test]
    public async Task ExecuteAsync_omits_backlog_bytes_when_the_provider_does_not_account_bytes()
    {
        const string Tree = "walgc-metering-no-byte-accounting";
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Report(entriesTrimmed: 7, retainedBytesAfter: null));
        var time = new VirtualTimeProvider();

        using var backlog = new InstrumentRecorder(LatticeMetrics.WalGcBacklogBytes, Tree);
        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Tree);

        var scheduler = CreateScheduler(FactoryWithTrees(Tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Byte accounting is a provider capability, and its absence is a
            // defined branch: nothing is recorded on the backlog histogram ...
            Assert.That(backlog.Measurements, Is.Empty);

            // ... and the pass counter is still emitted, so a consumer can tell
            // "not measured" apart from "no backlog" instead of reading silence.
            Assert.That(passes.Measurements, Has.Count.EqualTo(1));
            Assert.That(passes.Measurements[0].Tag(LatticeMetrics.TagOutcome), Is.EqualTo("reclaimed"));
        });
    }

    [Test]
    public async Task ExecuteAsync_metering_carries_the_tree_and_derived_tenant_dimensions()
    {
        const string Tree = "walgc-metering-tenant";
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Report(entriesTrimmed: 1, retainedBytesAfter: 64));
        var time = new VirtualTimeProvider();

        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Tree);
        using var intervals = new InstrumentRecorder(LatticeMetrics.WalGcInterval, Tree);
        using var backlog = new InstrumentRecorder(LatticeMetrics.WalGcBacklogBytes, Tree);

        var scheduler = CreateScheduler(FactoryWithTrees(Tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        foreach (var recorder in new[] { passes, intervals, backlog })
        {
            Assert.That(recorder.Measurements, Is.Not.Empty);
            var measurement = recorder.Measurements[0];
            Assert.Multiple(() =>
            {
                Assert.That(measurement.Tag(LatticeMetrics.TagTree), Is.EqualTo(Tree));
                Assert.That(
                    measurement.Tag(LatticeTenantLabel.TagTenant),
                    Is.EqualTo(LatticeTenantLabel.DefaultTenant),
                    "every emission site must carry the derived tenant dimension.");
            });
        }
    }

    /// <summary>One captured measurement: its value coerced to <see cref="double"/> and the tags it carried.</summary>
    private sealed record Captured(double Value, KeyValuePair<string, object?>[] Tags)
    {
        public object? Tag(string key)
        {
            foreach (var tag in Tags)
            {
                if (string.Equals(tag.Key, key, StringComparison.Ordinal))
                {
                    return tag.Value;
                }
            }

            return null;
        }
    }

    /// <summary>
    /// Captures <see cref="long"/> and <see cref="double"/> measurements recorded
    /// on one instrument, filtered to a single tree.
    /// <para>
    /// The Lattice meter is a process-wide static, so a listener also observes
    /// measurements from any fixture running concurrently. Matching the
    /// instrument by reference and keeping only the measurements carrying a
    /// test-unique tree id makes these assertions immune to that.
    /// </para>
    /// </summary>
    private sealed class InstrumentRecorder : IDisposable
    {
        private readonly List<Captured> _measurements = [];
        private readonly object _gate = new();
        private readonly MeterListener _listener;
        private readonly string _tree;

        public InstrumentRecorder(Instrument instrument, string tree)
        {
            ArgumentNullException.ThrowIfNull(instrument);

            _tree = tree;
            _listener = new MeterListener
            {
                InstrumentPublished = (published, listener) =>
                {
                    if (ReferenceEquals(published, instrument))
                    {
                        listener.EnableMeasurementEvents(published);
                    }
                },
            };

            _listener.SetMeasurementEventCallback<long>((_, value, tags, _) => Capture(value, tags));
            _listener.SetMeasurementEventCallback<double>((_, value, tags, _) => Capture(value, tags));
            _listener.Start();
        }

        public IReadOnlyList<Captured> Measurements
        {
            get { lock (_gate) { return _measurements.ToArray(); } }
        }

        public void Dispose() => _listener.Dispose();

        private void Capture(double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var captured = tags.ToArray();
            foreach (var tag in captured)
            {
                if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal)
                    && string.Equals(tag.Value as string, _tree, StringComparison.Ordinal))
                {
                    lock (_gate) { _measurements.Add(new Captured(value, captured)); }
                    return;
                }
            }
        }
    }
}
