using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="LatticeFallOffLogDetector"/>. The
/// detector compares the receiver's per-origin high-water-mark
/// against a sender-supplied oldest-available HLC and, on
/// fall-off-the-log detection, records the
/// <see cref="LatticeReplicationMetrics.PeerFellOffLog"/> counter
/// and (when configured) kicks off
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/>.
/// </summary>
[TestFixture]
public class LatticeFallOffLogDetectorTests
{
    private const string Tree = "lwm-tree";
    private const string Source = "site-a";

    private static (
        LatticeFallOffLogDetector Detector,
        ILatticeBootstrapCoordinator Coordinator,
        IReplicationHighWaterMarkGrain Grain,
        LatticeReplicationOptions Options) Create(
        HybridLogicalClock localHwm,
        bool autoBootstrap = true)
    {
        var grain = Substitute.For<IReplicationHighWaterMarkGrain>();
        grain.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(localHwm));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(grain);

        var coordinator = Substitute.For<ILatticeBootstrapCoordinator>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "self",
            AutoBootstrapOnFallOffLog = autoBootstrap,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var detector = new LatticeFallOffLogDetector(
            factory, coordinator, monitor,
            NullLogger<LatticeFallOffLogDetector>.Instance);
        return (detector, coordinator, grain, options);
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        Assert.That(
            () => new LatticeFallOffLogDetector(
                null!,
                Substitute.For<ILatticeBootstrapCoordinator>(),
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                NullLogger<LatticeFallOffLogDetector>.Instance),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_coordinator_is_null()
    {
        Assert.That(
            () => new LatticeFallOffLogDetector(
                Substitute.For<IGrainFactory>(),
                null!,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                NullLogger<LatticeFallOffLogDetector>.Instance),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_options_monitor_is_null()
    {
        Assert.That(
            () => new LatticeFallOffLogDetector(
                Substitute.For<IGrainFactory>(),
                Substitute.For<ILatticeBootstrapCoordinator>(),
                null!,
                NullLogger<LatticeFallOffLogDetector>.Instance),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_logger_is_null()
    {
        Assert.That(
            () => new LatticeFallOffLogDetector(
                Substitute.For<IGrainFactory>(),
                Substitute.For<ILatticeBootstrapCoordinator>(),
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void CheckAndTriggerAsync_throws_when_tree_name_is_null()
    {
        var (detector, _, _, _) = Create(Hlc(0));
        Assert.That(
            async () => await detector.CheckAndTriggerAsync(null!, Source, Hlc(10)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void CheckAndTriggerAsync_throws_when_tree_name_is_empty()
    {
        var (detector, _, _, _) = Create(Hlc(0));
        Assert.That(
            async () => await detector.CheckAndTriggerAsync(string.Empty, Source, Hlc(10)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void CheckAndTriggerAsync_throws_when_source_cluster_id_is_null()
    {
        var (detector, _, _, _) = Create(Hlc(0));
        Assert.That(
            async () => await detector.CheckAndTriggerAsync(Tree, null!, Hlc(10)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void CheckAndTriggerAsync_throws_when_source_cluster_id_is_empty()
    {
        var (detector, _, _, _) = Create(Hlc(0));
        Assert.That(
            async () => await detector.CheckAndTriggerAsync(Tree, string.Empty, Hlc(10)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void CheckAndTriggerAsync_observes_cancellation_before_dispatch()
    {
        var (detector, _, _, _) = Create(Hlc(0));
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await detector.CheckAndTriggerAsync(Tree, Source, Hlc(10), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task CheckAndTriggerAsync_returns_no_fall_off_when_local_hwm_exceeds_sender_oldest()
    {
        var (detector, coordinator, _, _) = Create(Hlc(100));
        var decision = await detector.CheckAndTriggerAsync(Tree, Source, Hlc(50));

        Assert.Multiple(() =>
        {
            Assert.That(decision.FellOffLog, Is.False);
            Assert.That(decision.LocalHighWaterMark, Is.EqualTo(Hlc(100)));
            Assert.That(decision.BootstrapTriggered, Is.False);
        });
        await coordinator.DidNotReceive().BootstrapAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CheckAndTriggerAsync_returns_no_fall_off_when_local_hwm_equals_sender_oldest()
    {
        var (detector, coordinator, _, _) = Create(Hlc(50));
        var decision = await detector.CheckAndTriggerAsync(Tree, Source, Hlc(50));

        Assert.Multiple(() =>
        {
            Assert.That(decision.FellOffLog, Is.False);
            Assert.That(decision.BootstrapTriggered, Is.False);
        });
        await coordinator.DidNotReceive().BootstrapAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CheckAndTriggerAsync_detects_fall_off_and_triggers_bootstrap_when_auto_bootstrap_enabled()
    {
        var (detector, coordinator, _, _) = Create(Hlc(10));

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.PeerFellOffLogName);

        var decision = await detector.CheckAndTriggerAsync(Tree, Source, Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(decision.FellOffLog, Is.True);
            Assert.That(decision.LocalHighWaterMark, Is.EqualTo(Hlc(10)));
            Assert.That(decision.BootstrapTriggered, Is.True);
        });

        await coordinator.Received(1).BootstrapAsync(Tree, Source, Arg.Any<CancellationToken>());

        var measurements = collector.Measurements;
        Assert.That(measurements, Has.Count.EqualTo(1));
        Assert.That(measurements.Single().Value, Is.EqualTo(1));
        Assert.That(measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagTree && (string?)t.Value == Tree));
        Assert.That(measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagOrigin && (string?)t.Value == Source));
    }

    [Test]
    public async Task CheckAndTriggerAsync_detects_fall_off_but_skips_bootstrap_when_auto_disabled()
    {
        var (detector, coordinator, _, _) = Create(Hlc(10), autoBootstrap: false);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.PeerFellOffLogName);

        var decision = await detector.CheckAndTriggerAsync(Tree, Source, Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(decision.FellOffLog, Is.True);
            Assert.That(decision.BootstrapTriggered, Is.False);
        });

        await coordinator.DidNotReceive().BootstrapAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());

        // Metric still emitted regardless of auto-bootstrap option.
        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task CheckAndTriggerAsync_passes_origin_argument_to_high_water_mark_grain()
    {
        var (detector, _, grain, _) = Create(Hlc(0));
        await detector.CheckAndTriggerAsync(Tree, Source, Hlc(0));
        await grain.Received(1).GetAsync(Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public void CheckAndTriggerAsync_propagates_coordinator_exception_verbatim()
    {
        var (detector, coordinator, _, _) = Create(Hlc(0));
        coordinator.BootstrapAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException("boom")));

        Assert.That(
            async () => await detector.CheckAndTriggerAsync(Tree, Source, Hlc(100)),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("boom"));
    }

    [Test]
    public async Task CheckAndTriggerAsync_detects_fall_off_for_cold_start_receiver_with_zero_local_hwm()
    {
        // A fresh receiver has never applied anything from this origin -
        // its HWM grain returns HybridLogicalClock.Zero. If the sender's
        // oldest available WAL entry is anywhere past zero, the receiver
        // has implicitly fallen off the log and needs a bootstrap.
        var (detector, coordinator, _, _) = Create(HybridLogicalClock.Zero);

        var decision = await detector.CheckAndTriggerAsync(Tree, Source, Hlc(1));

        Assert.Multiple(() =>
        {
            Assert.That(decision.FellOffLog, Is.True);
            Assert.That(decision.LocalHighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(decision.BootstrapTriggered, Is.True);
        });
        await coordinator.Received(1).BootstrapAsync(Tree, Source, Arg.Any<CancellationToken>());
    }

    [Test]
    public void CheckAndTriggerAsync_propagates_high_water_mark_grain_exception_verbatim()
    {
        // Constructing a detector whose HWM grain throws on GetAsync.
        var grain = Substitute.For<IReplicationHighWaterMarkGrain>();
        grain.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<HybridLogicalClock>>(_ => throw new TimeoutException("hwm down"));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(grain);

        var coordinator = Substitute.For<ILatticeBootstrapCoordinator>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "self" });

        var detector = new LatticeFallOffLogDetector(
            factory, coordinator, monitor,
            NullLogger<LatticeFallOffLogDetector>.Instance);

        Assert.That(
            async () => await detector.CheckAndTriggerAsync(Tree, Source, Hlc(100)),
            Throws.InstanceOf<TimeoutException>().With.Message.EqualTo("hwm down"));
    }
}