using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the durable materialiser pin-latency classifier input
/// (<see cref="LatticeOptions.WalSaturationMaterialiserPinLatencyThreshold"/> +
/// <see cref="LatticeOptions.WalSaturationMaterialiserPinLatencySampleWindows"/>)
/// added for issue #2015.
/// <para>
/// Every other materialiser input to the saturation signal is derived from
/// <i>in-memory</i> state, so a wedged <b>durable</b> pin store - the exact
/// condition of issue #2012, where the WAL trim floor stalled and the log grew
/// without bound - read perfectly healthy. This input measures the durable write
/// itself, via the caller-side <see cref="WalMaterialiserPinPressure"/> counter.
/// </para>
/// <para>
/// The bound to <see cref="WalSaturationState.Throttled"/> is load-bearing, not
/// conservatism: <see cref="WalSaturationState.Saturated"/> engages the writer
/// admission gate's <c>LatticeSaturatedException</c> fast-fail, which would
/// convert a retention-floor maintenance problem into user-visible write
/// failures and make the very incident this input detects strictly worse.
/// </para>
/// </summary>
[TestFixture]
public sealed class WalSaturationSamplerPinLatencyTests
{
    private static int _treeIdSeed;
    private string _treeId = null!;
    private string _shardKey = null!;
    private IWalCursorRegistry _cursors = null!;

    [SetUp]
    public void SetUp()
    {
        WalCommitLogWriter._trackers.Clear();
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();
        WalCommitLogWriter._providerFailureCounts.Clear();
        WalCommitLogWriter._flushLatencyTripCounts.Clear();
        WalCommitLogWriter._walHeadWallClockTicks.Clear();
        WalMaterialiserPinPressure.ResetForTests();
        _cursors = Substitute.For<IWalCursorRegistry>();
        _treeId = $"tree-pin-latency-{Interlocked.Increment(ref _treeIdSeed)}";
        _shardKey = _treeId + WalMaterialiserPinRouting.ShardSeparator + "0";
    }

    [TearDown]
    public void TearDown() => WalMaterialiserPinPressure.ResetForTests();

    private WalSaturationSampler CreateSampler(LatticeOptions options, out WalSaturationSignal signal)
    {
        signal = new WalSaturationSignal();
        signal.ResetForTesting();
        var dispatcher = new WalSaturationObserverDispatcher(
            Array.Empty<IWalSaturationObserver>(),
            NullLogger<WalSaturationObserverDispatcher>.Instance);

        // Isolate the input under test from the burst-smoothing upgrade.
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

    private static LatticeOptions Enabled(int windows = 3) => new()
    {
        WalSaturationMaterialiserPinLatencyThreshold = TimeSpan.FromSeconds(5),
        WalSaturationMaterialiserPinLatencySampleWindows = windows,
    };

    // Records one over-threshold durable pin write against the tree's shard,
    // which is what a reporting leaf does at its own call site.
    private void Trip() =>
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 30_000, faulted: false, latencyThresholdMs: 5_000);

    [Test]
    public async Task Disabled_threshold_never_escalates_regardless_of_trips()
    {
        var sampler = CreateSampler(
            new LatticeOptions { WalSaturationMaterialiserPinLatencyThreshold = null },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        for (var i = 0; i < 10; i++)
        {
            // Threshold null at the call site records nothing, and the classifier
            // ignores the input regardless.
            WalMaterialiserPinPressure.RecordWrite(_shardKey, 30_000, false, null);
            await sampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "left at its default null the input must be a complete no-op, so an existing deployment sees no behaviour change");
    }

    [Test]
    public async Task Escalates_to_Throttled_after_consecutive_windows()
    {
        var sampler = CreateSampler(Enabled(windows: 3), out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        Trip();
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        Trip();
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        Trip();
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "after 3 consecutive windows each carrying a durable pin-write trip the tree is held at Throttled");
    }

    [Test]
    public async Task Single_window_threshold_escalates_on_the_first_window()
    {
        var sampler = CreateSampler(Enabled(windows: 1), out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline
        Trip();
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled));
    }

    [Test]
    public async Task Pin_latency_never_escalates_to_Saturated()
    {
        var sampler = CreateSampler(Enabled(windows: 1), out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        for (var i = 0; i < 20; i++)
        {
            Trip();
            Trip();
            Trip();
            await sampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "escalating a stalled retention floor to Saturated would engage the admission gate's fast-fail and turn slow WAL trimming into user-visible write failures");
    }

    [Test]
    public async Task Resets_consecutive_counter_when_a_window_has_no_trip()
    {
        var sampler = CreateSampler(Enabled(windows: 3), out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        Trip();
        await sampler.SampleOnceAsync(CancellationToken.None);
        Trip();
        await sampler.SampleOnceAsync(CancellationToken.None);

        // A quiet window (the durable store recovered) must reset the counter.
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        Trip();
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "after the reset a single trip must not immediately re-escalate");
    }

    [Test]
    public async Task Recovers_to_Healthy_once_trips_stop()
    {
        var sampler = CreateSampler(Enabled(windows: 1), out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline
        Trip();
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled));

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "the back-off must lift on its own once the durable store catches up");
    }

    [Test]
    public async Task Trips_on_one_tree_do_not_escalate_another()
    {
        var sampler = CreateSampler(Enabled(windows: 1), out var signal);
        var otherTree = $"{_treeId}-sibling";

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline
        Trip();
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled));
            Assert.That(signal.GetCurrentState(otherTree), Is.EqualTo(WalSaturationState.Healthy),
                "the signal is per-tree: one tree's stalled pin store must not throttle another");
        });
    }

    [Test]
    public async Task Baseline_tick_does_not_escalate_on_pre_existing_trips()
    {
        // Trips recorded before the sampler ever ran belong to no window. The
        // sampler seeds its prior-count map on the first tick and only counts
        // deltas after that, so a process that starts with a non-zero counter
        // must not immediately report pressure.
        Trip();
        Trip();

        var sampler = CreateSampler(Enabled(windows: 1), out var signal);
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));
    }
}
