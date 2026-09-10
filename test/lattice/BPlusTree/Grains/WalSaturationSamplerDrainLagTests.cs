using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the leaf-materialiser drain-lag classifier input
/// (<see cref="LatticeOptions.WalSaturationMaterialiserLagThreshold"/> +
/// <see cref="LatticeOptions.WalSaturationMaterialiserLagSampleWindows"/>)
/// that <see cref="WalSaturationSampler"/> computes live every tick from the
/// in-memory WAL head wall clock
/// (<see cref="WalCommitLogWriter._walHeadWallClockTicks"/>) minus the slowest
/// in-memory materialiser cursor (the <see cref="IWalCursorRegistry"/> min) -
/// the direct leaf-materialiser drain-lag back-pressure surface (issue #1030).
/// The lag is recomputed fresh each tick (no GC dependency, no staleness
/// window), and a sustained run drives
/// <see cref="WalSaturationState.Throttled"/> - a pure back-off - rather than
/// Saturated, so it never engages the writer admission gate's fast-fail.
/// </summary>
[TestFixture]
public class WalSaturationSamplerDrainLagTests
{
    private static int _treeIdSeed;
    private string _treeId = null!;
    private IWalCursorRegistry _cursors = null!;
    private long _headTicks;

    [SetUp]
    public void SetUp()
    {
        WalCommitLogWriter._trackers.Clear();
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();
        WalCommitLogWriter._providerFailureCounts.Clear();
        WalCommitLogWriter._flushLatencyTripCounts.Clear();
        WalCommitLogWriter._walHeadWallClockTicks.Clear();
        _cursors = Substitute.For<IWalCursorRegistry>();
        _cursors.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(Array.Empty<WalCursorSnapshot>()));
        _headTicks = DateTimeOffset.UtcNow.UtcTicks;
        _treeId = $"tree-drain-lag-{Interlocked.Increment(ref _treeIdSeed)}";
    }

    private WalSaturationSampler CreateSampler(
        LatticeOptions options,
        out WalSaturationSignal signal)
    {
        signal = new WalSaturationSignal();
        signal.ResetForTesting();
        var dispatcher = new WalSaturationObserverDispatcher(
            Array.Empty<IWalSaturationObserver>(),
            NullLogger<WalSaturationObserverDispatcher>.Instance);

        options.WalSaturationRecoveryWindow = TimeSpan.Zero;

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        return new WalSaturationSampler(
            signal,
            dispatcher,
            monitor,
            NullLogger<WalSaturationSampler>.Instance,
            _cursors);
    }

    // Drives a fresh per-tick drain-lag of exactly <paramref name="lag"/> for the
    // tree by recording the WAL head wall clock and a materialiser frontier
    // cursor that trails it by that amount. A zero lag places the frontier at the
    // head (the materialiser has caught up).
    private void SetLevel(TimeSpan lag, string? tree = null)
    {
        var t = tree ?? _treeId;
        WalCommitLogWriter._walHeadWallClockTicks[t] = _headTicks;
        var frontier = new HybridLogicalClock { WallClockTicks = _headTicks - lag.Ticks, Counter = 0 };
        _cursors.GetMinCursorAsync(t, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<HybridLogicalClock?>(frontier));
    }

    [Test]
    public async Task Disabled_threshold_never_escalates_regardless_of_level()
    {
        var sampler = CreateSampler(
            new LatticeOptions { WalSaturationMaterialiserLagThreshold = null },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        SetLevel(TimeSpan.FromMinutes(5));

        for (var i = 0; i < 10; i++)
        {
            await sampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "with the drain-lag input disabled (threshold null), a standing lag level must never escalate the regime");
    }

    [Test]
    public async Task Escalates_to_Throttled_after_consecutive_windows()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 3,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // A single standing over-threshold level persists across ticks (this is
        // the whole point of the level model): each tick re-reads it and
        // increments the consecutive-window counter.
        SetLevel(TimeSpan.FromSeconds(30));

        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "after 3 consecutive over-threshold windows the drain-lag input holds the tree at Throttled");
    }

    [Test]
    public async Task Drain_lag_never_escalates_to_Saturated()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 1,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        SetLevel(TimeSpan.FromMinutes(10));
        for (var i = 0; i < 20; i++)
        {
            await sampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "drain lag is a pure back-off: even an extreme, sustained lag holds at Throttled and never escalates to Saturated");
    }

    [Test]
    public async Task Resets_consecutive_counter_when_a_window_is_under_threshold()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 3,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        SetLevel(TimeSpan.FromSeconds(30));
        await sampler.SampleOnceAsync(CancellationToken.None);
        await sampler.SampleOnceAsync(CancellationToken.None);

        // A fresh at/under-threshold observation (the materialiser caught up):
        // counter must reset to 0.
        SetLevel(TimeSpan.Zero);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        SetLevel(TimeSpan.FromSeconds(30));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "after the counter reset, a single over-threshold window must not immediately re-escalate");
    }

    [Test]
    public async Task Single_window_threshold_escalates_on_first_over_threshold_window()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 1,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline
        SetLevel(TimeSpan.FromSeconds(30));
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled));
    }

    [Test]
    public async Task Trees_are_independent_for_the_consecutive_window_counter()
    {
        var otherTree = _treeId + "-other";
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 2,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        SetLevel(TimeSpan.FromSeconds(30));
        SetLevel(TimeSpan.FromSeconds(30), otherTree);
        await sampler.SampleOnceAsync(CancellationToken.None);

        // tree A stays over threshold; tree B catches up (under threshold) -> resets.
        SetLevel(TimeSpan.FromSeconds(30));
        SetLevel(TimeSpan.Zero, otherTree);
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "tree A had 2 consecutive over-threshold windows");
        Assert.That(signal.GetCurrentState(otherTree), Is.EqualTo(WalSaturationState.Healthy),
            "tree B's counter was reset by the under-threshold window");
    }

    [Test]
    public async Task Null_frontier_is_treated_as_zero_lag_and_never_escalates()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 1,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // A tree with an advancing WAL head but no materialiser cursor reported
        // (e.g. a never-checkpointed leaf, or a block pin disabling the cursor
        // branch): the registry returns null. We cannot measure a head-relative
        // lag, so the block-pin contract requires zero lag - never a trip.
        WalCommitLogWriter._walHeadWallClockTicks[_treeId] = _headTicks;
        _cursors.GetMinCursorAsync(_treeId, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<HybridLogicalClock?>(null));

        for (var i = 0; i < 5; i++)
        {
            await sampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "a never-checkpointed leaf (null frontier) must never pin the regime, even with an advancing head");
    }

    // Marks a consumer that has never reported a cursor. The registry stores
    // HybridLogicalClock.Zero for these; the min() meet excludes them, and the
    // lagging-consumer count must exclude them with it.
    private static readonly TimeSpan NeverReported = TimeSpan.MinValue;

    // Stubs the per-consumer cursor snapshot the sampler reads for a tree it has
    // already found over threshold. Each consumer's cursor is placed the given
    // amount behind the same WAL head the aggregate is measured against, so a
    // consumer's individual lag and the tree's aggregate lag are on one clock.
    private void SetConsumers(params (string ConsumerId, TimeSpan Lag)[] consumers)
    {
        var snapshot = new List<WalCursorSnapshot>(consumers.Length);
        foreach (var (consumerId, lag) in consumers)
        {
            var cursor = lag == NeverReported
                ? HybridLogicalClock.Zero
                : new HybridLogicalClock { WallClockTicks = _headTicks - lag.Ticks, Counter = 0 };
            snapshot.Add(new WalCursorSnapshot(consumerId, cursor, _headTicks));
        }

        _cursors.SnapshotAsync(_treeId, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(snapshot));
    }

    // Listens for the lagging-consumer count. The instrument is passed as an
    // argument so its owning type initialiser has necessarily completed before
    // the listener starts (see Orleans.Lattice.Testing.MeterListening).
    private static MeterListener ListenForLaggingConsumers(
        List<(int Value, string? Tree, string? Tenant)> sink)
        => MeterListening.StartForInstrument(
            LatticeMetrics.MaterialiserLaggingConsumers,
            listener => listener.SetMeasurementEventCallback<int>((_, value, tags, _) =>
            {
                string? tree = null;
                string? tenant = null;
                foreach (var tag in tags)
                {
                    if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
                    {
                        tree = tag.Value as string;
                    }
                    else if (string.Equals(tag.Key, LatticeTenantLabel.TagTenant, StringComparison.Ordinal))
                    {
                        tenant = tag.Value as string;
                    }
                }

                lock (sink)
                {
                    sink.Add((value, tree, tenant));
                }
            }));

    private WalSaturationSampler CreateLaggingConsumerSampler()
        => CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 1,
            },
            out _);

    [Test]
    public async Task Lagging_consumer_count_is_not_recorded_for_a_tree_under_threshold()
    {
        var sampler = CreateLaggingConsumerSampler();
        var sink = new List<(int Value, string? Tree, string? Tenant)>();
        using var listener = ListenForLaggingConsumers(sink);

        // The aggregate is under threshold, so the tree is not in the regime the
        // count exists to decompose - even though a consumer behind the frontier
        // would count if it were. This is what keeps a healthy estate free of the
        // snapshot read that backs the instrument.
        SetLevel(TimeSpan.FromSeconds(1));
        SetConsumers(("leaf-a", TimeSpan.FromMinutes(9)));

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(sink.Where(m => m.Tree == _treeId), Is.Empty,
            "a tree whose aggregate drain lag is under threshold must not emit a lagging-consumer count");
        await _cursors.DidNotReceive().SnapshotAsync(_treeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Lagging_consumer_count_separates_one_dormant_consumer_from_many_behind()
    {
        // The defect this instrument closes (issue #2444): the aggregate is a
        // min() across consumers, so these two populations produce an identical
        // drain-lag reading while calling for opposite responses. Both are driven
        // through the same over-threshold aggregate here, so the only thing that
        // differs between the assertions is the count.
        var oneSampler = CreateLaggingConsumerSampler();
        var oneSink = new List<(int Value, string? Tree, string? Tenant)>();
        using (var listener = ListenForLaggingConsumers(oneSink))
        {
            SetLevel(TimeSpan.FromMinutes(9));
            SetConsumers(
                ("leaf-dormant", TimeSpan.FromMinutes(9)),
                ("leaf-healthy-a", TimeSpan.FromSeconds(1)),
                ("leaf-healthy-b", TimeSpan.FromSeconds(1)));

            await oneSampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(oneSink.Where(m => m.Tree == _treeId).Select(m => m.Value), Is.EqualTo(new[] { 1 }),
            "one consumer past the threshold behind two caught-up ones must count exactly 1");

        _treeId = $"tree-drain-lag-{Interlocked.Increment(ref _treeIdSeed)}";
        var manySampler = CreateLaggingConsumerSampler();
        var manySink = new List<(int Value, string? Tree, string? Tenant)>();
        using (var listener = ListenForLaggingConsumers(manySink))
        {
            SetLevel(TimeSpan.FromMinutes(9));
            SetConsumers(
                ("leaf-behind-a", TimeSpan.FromMinutes(9)),
                ("leaf-behind-b", TimeSpan.FromMinutes(7)),
                ("leaf-behind-c", TimeSpan.FromMinutes(6)));

            await manySampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(manySink.Where(m => m.Tree == _treeId).Select(m => m.Value), Is.EqualTo(new[] { 3 }),
            "three consumers past the threshold must count 3, distinguishing a broad stall from a single dormant consumer");
    }

    [Test]
    public async Task Lagging_consumer_count_excludes_consumers_that_never_reported_a_cursor()
    {
        var sampler = CreateLaggingConsumerSampler();
        var sink = new List<(int Value, string? Tree, string? Tenant)>();
        using var listener = ListenForLaggingConsumers(sink);

        // A never-reported consumer sits at HLC zero, which is arbitrarily far
        // behind any head. It is excluded from the min() meet that produces the
        // aggregate, so counting it here would report consumers the aggregate
        // does not answer for - and would read as a broad stall on a tree with a
        // single genuine laggard.
        SetLevel(TimeSpan.FromMinutes(9));
        SetConsumers(
            ("leaf-behind", TimeSpan.FromMinutes(9)),
            ("leaf-never-reported-a", NeverReported),
            ("leaf-never-reported-b", NeverReported));

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(sink.Where(m => m.Tree == _treeId).Select(m => m.Value), Is.EqualTo(new[] { 1 }),
            "consumers that have never reported a cursor must be excluded, exactly as they are from the min() meet");
    }

    [Test]
    public async Task Lagging_consumer_count_is_tagged_by_tree_and_tenant_and_never_by_consumer()
    {
        var sampler = CreateLaggingConsumerSampler();
        var sink = new List<(int Value, string? Tree, string? Tenant)>();
        var tagKeys = new List<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.MaterialiserLaggingConsumers,
            l => l.SetMeasurementEventCallback<int>((_, value, tags, _) =>
            {
                string? tree = null;
                string? tenant = null;
                foreach (var tag in tags)
                {
                    lock (tagKeys)
                    {
                        tagKeys.Add(tag.Key);
                    }

                    if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
                    {
                        tree = tag.Value as string;
                    }
                    else if (string.Equals(tag.Key, LatticeTenantLabel.TagTenant, StringComparison.Ordinal))
                    {
                        tenant = tag.Value as string;
                    }
                }

                lock (sink)
                {
                    sink.Add((value, tree, tenant));
                }
            }));

        SetLevel(TimeSpan.FromMinutes(9));
        SetConsumers(("leaf-behind", TimeSpan.FromMinutes(9)));

        await sampler.SampleOnceAsync(CancellationToken.None);

        var measurement = sink.Single(m => m.Tree == _treeId);
        Assert.Multiple(() =>
        {
            Assert.That(measurement.Tenant, Is.EqualTo(LatticeTenantLabel.ForTree(_treeId).Value),
                "the count must carry the derived tenant label, as the drain-lag aggregate it decomposes does");
            Assert.That(tagKeys, Is.EquivalentTo(new[] { LatticeMetrics.TagTree, LatticeTenantLabel.TagTenant }),
                "consumer identity is unbounded cardinality and must never become a tag: the count is triageable, not diagnosable");
        });
    }
}
