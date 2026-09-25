using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// Position-age discrimination of the drain-lag input, end to end through the real
// registry (issue #3131). A leaf re-reports its persisted clock with a fresh report
// time on every activation, and a leaf whose key range has seen no write keeps an
// old clock while fully caught up. Against a tree-wide WAL head that read as tens
// of hours of lag on a freshly started silo, and held the tree Throttled.
public partial class WalSaturationSamplerDrainLagTests
{
    private string LeafConsumerId(string leaf)
        => ILeafCursorReporter.MaterialiserConsumerIdPrefix + _treeId + "_" + leaf;

    private sealed class CapturingLogger : ILogger<WalSaturationSampler>
    {
        public List<(LogLevel Level, string Message)> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (Lines)
            {
                Lines.Add((logLevel, formatter(state, exception)));
            }
        }
    }

    // Builds a sampler over the real registry, observed at real "now" so the
    // registry's DateTime.UtcNow report stamps fall inside the freshness window.
    private static WalSaturationSampler CreateRegistrySampler(
        InMemoryWalCursorRegistry registry,
        DateTimeOffset observedAt,
        WalSaturationSignal signal,
        ILogger<WalSaturationSampler> logger,
        int sampleWindows = 1)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            WalSaturationRecoveryWindow = TimeSpan.Zero,
            WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(30),
            WalSaturationMaterialiserLagSampleWindows = sampleWindows,
            WalDrainLagConsumerFreshness = TimeSpan.FromMinutes(5),
        });

        return new WalSaturationSampler(
            signal,
            new WalSaturationObserverDispatcher(
                Array.Empty<IWalSaturationObserver>(),
                NullLogger<WalSaturationObserverDispatcher>.Instance),
            monitor,
            logger,
            new VirtualTimeProvider(observedAt),
            registry);
    }

    private static WalSaturationSignal NewSignal()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        return signal;
    }

    private static HybridLogicalClock At(DateTimeOffset instant)
        => new() { WallClockTicks = instant.UtcTicks, Counter = 0 };

    [Test]
    public async Task Leaf_re_reporting_an_unmoved_ancient_position_records_zero_lag_and_does_not_trip()
    {
        var registry = new InMemoryWalCursorRegistry();
        var persistedClock = At(DateTimeOffset.UtcNow - TimeSpan.FromHours(45));

        // Activation re-asserts the persisted clock, then the checkpoint flush tail
        // re-asserts it again: two fresh reports, one unmoved position.
        await registry.ReportCursorAsync(_treeId, LeafConsumerId("idle"), persistedClock, CancellationToken.None);
        await registry.ReportCursorAsync(_treeId, LeafConsumerId("idle"), persistedClock, CancellationToken.None);

        var observedAt = DateTimeOffset.UtcNow;
        WalCommitLogWriter._walHeadWallClockTicks[_treeId] = observedAt.UtcTicks;
        var signal = NewSignal();
        var sampler = CreateRegistrySampler(registry, observedAt, signal, NullLogger<WalSaturationSampler>.Instance);
        var sink = new List<(double Value, string? Tree)>();
        using var listener = ListenForDrainLag(sink);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
                "a caught-up leaf whose range has seen no write must not hold the tree Throttled on phantom lag");
            Assert.That(sink.Where(m => m.Tree == _treeId).Select(m => m.Value), Is.EqualTo(new[] { 0d }),
                "the drain-lag histogram must read zero, not the 45 h distance from an unmoved leaf clock to the tree-wide head");
        });
    }

    [Test]
    public async Task Leaf_draining_behind_the_head_still_trips_Throttled_and_is_named_in_the_log()
    {
        var registry = new InMemoryWalCursorRegistry();
        var start = DateTimeOffset.UtcNow;
        var draining = LeafConsumerId("draining");

        await registry.ReportCursorAsync(_treeId, draining, At(start - TimeSpan.FromHours(2)), CancellationToken.None);
        await registry.ReportCursorAsync(_treeId, draining, At(start - TimeSpan.FromHours(1)), CancellationToken.None);

        var observedAt = DateTimeOffset.UtcNow;
        WalCommitLogWriter._walHeadWallClockTicks[_treeId] = observedAt.UtcTicks;
        var signal = NewSignal();
        var logger = new CapturingLogger();
        var sampler = CreateRegistrySampler(registry, observedAt, signal, logger);
        var lagging = new List<(int Value, string? Tree, string? Tenant)>();
        using var listener = ListenForLaggingConsumers(lagging);

        await sampler.SampleOnceAsync(CancellationToken.None);

        var warnings = logger.Lines.Where(l => l.Level == LogLevel.Warning).Select(l => l.Message).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
                "a leaf whose position is advancing but still an hour behind is genuine backlog and must still trip");
            Assert.That(lagging.Where(m => m.Tree == _treeId).Select(m => m.Value), Is.EqualTo(new[] { 1 }),
                "the lagging-consumer count must apply the same eligibility as the aggregate and count the draining leaf");
            Assert.That(warnings, Has.Count.EqualTo(1));
            Assert.That(warnings.Single(), Does.Contain(draining).And.Contain(_treeId),
                "the warning must name the consumer holding the minimum and the tree it holds");
        });
    }

    [Test]
    public async Task Tree_wide_consumer_re_reporting_an_unmoved_position_still_trips_Throttled()
    {
        var registry = new InMemoryWalCursorRegistry();
        var stalled = At(DateTimeOffset.UtcNow - TimeSpan.FromHours(1));

        await registry.ReportCursorAsync(_treeId, "view-maintainer", stalled, CancellationToken.None);
        await registry.ReportCursorAsync(_treeId, "view-maintainer", stalled, CancellationToken.None);

        var observedAt = DateTimeOffset.UtcNow;
        WalCommitLogWriter._walHeadWallClockTicks[_treeId] = observedAt.UtcTicks;
        var signal = NewSignal();
        var sampler = CreateRegistrySampler(registry, observedAt, signal, NullLogger<WalSaturationSampler>.Instance);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "a stalled tree-wide tailer shares the head's scope, so its gap is real backlog the position-age exclusion must not hide");
    }

    [Test]
    public async Task Min_holder_warning_is_logged_once_per_threshold_crossing()
    {
        var registry = new InMemoryWalCursorRegistry();
        var start = DateTimeOffset.UtcNow;
        var draining = LeafConsumerId("draining");

        await registry.ReportCursorAsync(_treeId, draining, At(start - TimeSpan.FromHours(2)), CancellationToken.None);
        await registry.ReportCursorAsync(_treeId, draining, At(start - TimeSpan.FromHours(1)), CancellationToken.None);

        var observedAt = DateTimeOffset.UtcNow;
        WalCommitLogWriter._walHeadWallClockTicks[_treeId] = observedAt.UtcTicks;
        var signal = NewSignal();
        var logger = new CapturingLogger();
        var sampler = CreateRegistrySampler(registry, observedAt, signal, logger, sampleWindows: 3);

        for (var i = 0; i < 4; i++)
        {
            await sampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
                "precondition: the tree stayed over threshold for every tick");
            Assert.That(logger.Lines.Count(l => l.Level == LogLevel.Warning && l.Message.Contains(draining)), Is.EqualTo(1),
                "a standing over-threshold tree must log its min-holder on the crossing tick only, not on every tick");
        });
    }
}
