using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeWalGcScheduler"/>: the per-silo background
/// service that drives the WAL garbage collector for every registered tree,
/// closing the retention gap for durable-WAL hosts that run without the
/// replication package and for non-replicated trees in a replicated host
/// (issue #920), on a startup delay and cadence that respond to backlog rather
/// than to a fixed clock (issue #1832).
/// <para>
/// Every test here is driven off an injected <see cref="VirtualTimeProvider"/>:
/// virtual time only moves when the test moves it, and each step waits on the
/// logical event "the scheduler parked on its next delay" rather than on an
/// elapsed wall-clock duration, so nothing depends on timing, thread-pool
/// latency, or test ordering.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeWalGcSchedulerTests
{
    /// <summary>
    /// A small but positive startup stagger. It has to be positive:
    /// <c>Task.Delay(TimeSpan.Zero, ...)</c> completes without ever arming a
    /// timer, so a zero stagger would run the first pass during the service
    /// start rather than on a step the test drives.
    /// </summary>
    private static readonly TimeSpan Stagger = TimeSpan.FromSeconds(1);

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
        LatticeOptions options,
        TimeProvider time)
        => new(
            factory,
            gc,
            Monitor(options),
            Substitute.For<Microsoft.Extensions.Logging.ILogger<LatticeWalGcScheduler>>(),
            time);

    /// <summary>
    /// A report shaped for cadence tests: only the entry count (the scheduler's
    /// backlog signal) and the optional post-pass retained-byte sample matter.
    /// </summary>
    private static LatticeWalGcReport Report(long entriesTrimmed, long? retainedBytesAfter = null) =>
        new("tree", null, null, null, null, 1, entriesTrimmed, null, null, retainedBytesAfter);

    /// <summary>
    /// Waits, with a generous bound, for a task that completes on a logical
    /// scheduler event. A timeout is a genuine failure (the scheduler never came
    /// back round), never a slow machine tripping an assertion.
    /// </summary>
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
    /// is deterministic even though the offset itself is randomized.
    /// </summary>
    private static async Task StartAndRunFirstPassAsync(
        LatticeWalGcScheduler scheduler,
        VirtualTimeProvider time)
    {
        await StartArmedAsync(scheduler, time);
        var parked = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(parked);
    }

    [Test]
    public async Task ExecuteAsync_disabled_interval_never_runs_a_gc_pass()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var factory = FactoryWithTrees(out var registry, "alpha", "beta");
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(factory, gc, new LatticeOptions { WalGcInterval = TimeSpan.Zero }, time);

        await scheduler.StartAsync(CancellationToken.None);
        // StopAsync awaits the execute task, so once it returns the disabled
        // scheduler has provably finished without arming anything.
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(time.TimersCreated, Is.Zero, "a disabled scheduler must not schedule any delay at all.");
        await registry.DidNotReceive().GetAllTreeIdsAsync();
        await gc.DidNotReceiveWithAnyArgs().RunOnceAsync(default!, default);
    }

    [Test]
    public async Task ExecuteAsync_negative_interval_never_runs_a_gc_pass()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var factory = FactoryWithTrees(out _, "alpha");
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(
            factory, gc, new LatticeOptions { WalGcInterval = TimeSpan.FromSeconds(-1) }, time);

        await scheduler.StartAsync(CancellationToken.None);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(time.TimersCreated, Is.Zero);
        await gc.DidNotReceiveWithAnyArgs().RunOnceAsync(default!, default);
    }

    [Test]
    public async Task ExecuteAsync_enabled_runs_gc_for_every_registered_tree()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(0));
        var factory = FactoryWithTrees(out _, "alpha", "beta");
        var time = new VirtualTimeProvider();

        // A zero startup delay opts out of the stagger, so the first pass runs
        // synchronously at start and the assertion needs no clock movement.
        var scheduler = CreateScheduler(
            factory,
            gc,
            new LatticeOptions { WalGcInterval = TimeSpan.FromHours(1), WalGcStartupDelay = Stagger },
            time);

        await StartAndRunFirstPassAsync(scheduler, time);
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
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(
            factory,
            gc,
            new LatticeOptions { WalGcInterval = TimeSpan.FromHours(1), WalGcStartupDelay = Stagger },
            time);

        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        await registry.ReceivedWithAnyArgs().GetAllTreeIdsAsync();
        await gc.DidNotReceiveWithAnyArgs().RunOnceAsync(default!, default);
    }

    [Test]
    public async Task ExecuteAsync_empty_registry_relaxes_its_retry_toward_the_configured_interval()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var factory = FactoryWithTrees(out _);
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(
            factory,
            gc,
            new LatticeOptions
            {
                WalGcInterval = TimeSpan.FromHours(1),
                WalGcMinInterval = TimeSpan.FromSeconds(30),
                WalGcStartupDelay = Stagger,
            },
            time);

        await StartAndRunFirstPassAsync(scheduler, time);

        // A silo with nothing registered must not poll the registry at the
        // responsive floor forever; it backs off geometrically like a quiet tree.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromSeconds(30)));

        var parked = time.NextTimerAsync();
        time.Advance(TimeSpan.FromSeconds(30));
        await Parked(parked);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(1)));

        parked = time.NextTimerAsync();
        time.Advance(TimeSpan.FromMinutes(1));
        await Parked(parked);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(2)));

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_skips_null_or_empty_tree_ids()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(0));
        var factory = FactoryWithTrees(out _, "alpha", "", "beta");
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(
            factory,
            gc,
            new LatticeOptions { WalGcInterval = TimeSpan.FromHours(1), WalGcStartupDelay = Stagger },
            time);

        await StartAndRunFirstPassAsync(scheduler, time);
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
        gc.RunOnceAsync("beta", Arg.Any<CancellationToken>()).Returns(Report(0));
        var factory = FactoryWithTrees(out _, "alpha", "beta");
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(
            factory,
            gc,
            new LatticeOptions { WalGcInterval = TimeSpan.FromHours(1), WalGcStartupDelay = Stagger },
            time);

        await StartAndRunFirstPassAsync(scheduler, time);
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
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(
            factory,
            gc,
            new LatticeOptions
            {
                WalGcInterval = TimeSpan.FromHours(1),
                WalGcMinInterval = TimeSpan.FromSeconds(30),
                WalGcStartupDelay = Stagger,
            },
            time);

        await StartAndRunFirstPassAsync(scheduler, time);

        // The pass was attempted and the scheduler is still parked on a retry
        // rather than dead.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromSeconds(30)));

        var parked = time.NextTimerAsync();
        time.Advance(TimeSpan.FromSeconds(30));
        await Parked(parked);

        Assert.That(async () => await scheduler.StopAsync(CancellationToken.None), Throws.Nothing);
        await registry.ReceivedWithAnyArgs().GetAllTreeIdsAsync();
        await gc.DidNotReceiveWithAnyArgs().RunOnceAsync(default!, default);
    }

    [Test]
    public async Task ExecuteAsync_staggers_the_first_pass_instead_of_running_at_startup()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var factory = FactoryWithTrees(out var registry, "alpha");
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(
            factory,
            gc,
            new LatticeOptions
            {
                WalGcInterval = TimeSpan.FromHours(1),
                WalGcStartupDelay = TimeSpan.FromSeconds(30),
            },
            time);

        await StartArmedAsync(scheduler, time);

        // The stagger is drawn from [window/2, window), so it can never put the
        // first pass inside the silo's activation window ...
        Assert.That(time.ScheduledDelays[0], Is.GreaterThanOrEqualTo(TimeSpan.FromSeconds(15))
            .And.LessThan(TimeSpan.FromSeconds(30)));

        // ... and just short of that floor nothing has been collected yet.
        time.Advance(TimeSpan.FromSeconds(14));
        await registry.DidNotReceive().GetAllTreeIdsAsync();
        await gc.DidNotReceiveWithAnyArgs().RunOnceAsync(default!, default);

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public void WalGcInterval_defaults_to_one_hour_enabling_the_scheduler()
    {
        var options = new LatticeOptions();
        Assert.That(options.WalGcInterval, Is.EqualTo(TimeSpan.FromHours(1)),
            "the core WAL GC scheduler must default on (hourly) so durable-WAL hosts get bounded WAL retention out of the box.");
        Assert.That(LatticeOptions.DefaultWalGcInterval, Is.EqualTo(TimeSpan.FromHours(1)));
    }

    [Test]
    public void WalGcStartupDelay_defaults_to_thirty_seconds()
    {
        var options = new LatticeOptions();
        Assert.That(options.WalGcStartupDelay, Is.EqualTo(TimeSpan.FromSeconds(30)),
            "the first pass must land in the low tens of seconds so a short-lived box still reclaims its WAL.");
        Assert.That(LatticeOptions.DefaultWalGcStartupDelay, Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void WalGcMinInterval_defaults_to_thirty_seconds()
    {
        var options = new LatticeOptions();
        Assert.That(options.WalGcMinInterval, Is.EqualTo(TimeSpan.FromSeconds(30)),
            "a tree with backlog must be re-collected on a responsive floor rather than the hourly ceiling.");
        Assert.That(LatticeOptions.DefaultWalGcMinInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
    }
}
