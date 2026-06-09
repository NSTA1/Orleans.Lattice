using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the fourth Saturated classifier input
/// (<see cref="LatticeOptions.WalSaturationFlushLatencyThreshold"/> +
/// <see cref="LatticeOptions.WalSaturationFlushLatencySampleWindows"/>)
/// that <see cref="WalSaturationSampler"/> reads off
/// <see cref="WalCommitLogWriter._flushLatencyTripCounts"/>. Mirrors
/// the existing per-input fixtures (depth-ratio, dispatch-timeout,
/// provider-failure) by driving the sampler one tick at a time and
/// asserting the tree's resolved state on the
/// <see cref="WalSaturationSignal"/> after each tick.
/// </summary>
[TestFixture]
public class WalSaturationSamplerFlushLatencyTests
{
    private static int _treeIdSeed;
    private string _treeId = null!;

    [SetUp]
    public void SetUp()
    {
        WalCommitLogWriter._trackers.Clear();
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();
        WalCommitLogWriter._providerFailureCounts.Clear();
        WalCommitLogWriter._flushLatencyTripCounts.Clear();
        _treeId = $"tree-flush-latency-{Interlocked.Increment(ref _treeIdSeed)}";
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

        // Pin the recovery window at zero so the deterministic per-
        // tick classifier behaviour drives the regime directly - the
        // flush-latency tests are about the consecutive-window
        // counter, not the recovery-window upgrade.
        options.WalSaturationRecoveryWindow = TimeSpan.Zero;

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        return new WalSaturationSampler(
            signal,
            dispatcher,
            monitor,
            NullLogger<WalSaturationSampler>.Instance);
    }

    [Test]
    public async Task Disabled_threshold_never_escalates_regardless_of_trip_count()
    {
        // Threshold null = input disabled. Even a very high trip count
        // must not affect the regime; the historical three-input
        // classifier behaviour is observed exactly.
        var sampler = CreateSampler(
            new LatticeOptions { WalSaturationFlushLatencyThreshold = null },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // Inject a large trip count on a shard for our tree.
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 100;

        for (var i = 0; i < 10; i++)
        {
            await sampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "with the flush-latency input disabled (threshold null), trip counts must never escalate the regime");
    }

    [Test]
    public async Task Escalates_to_Saturated_after_consecutive_windows()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationFlushLatencyThreshold = TimeSpan.FromMilliseconds(500),
                WalSaturationFlushLatencySampleWindows = 3,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline tick

        // Tick 1: first trip observed. Counter = 1.
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "after 1 consecutive window the counter is below the 3-window threshold");

        // Tick 2: another trip. Counter = 2.
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 2;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "after 2 consecutive windows the counter is still below the 3-window threshold");

        // Tick 3: another trip. Counter = 3 -> Saturated.
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 3;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "after 3 consecutive non-zero windows the flush-latency input escalates the regime");
    }

    [Test]
    public async Task Resets_consecutive_counter_when_a_window_has_no_trips()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationFlushLatencyThreshold = TimeSpan.FromMilliseconds(500),
                WalSaturationFlushLatencySampleWindows = 3,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // Two consecutive trip windows.
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 2;
        await sampler.SampleOnceAsync(CancellationToken.None);

        // Tick with no new trip: counter must reset to 0.
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        // One more trip after the reset: counter = 1, still below 3.
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 3;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "after the counter reset, a single trip window must not immediately re-escalate; the consecutive requirement still applies");
    }

    [Test]
    public async Task Single_window_threshold_escalates_on_first_trip_after_baseline()
    {
        // With SampleWindows = 1, the very first non-zero window
        // escalates - useful for tests that want to exercise the
        // branch without arming a 3-window cycle.
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationFlushLatencyThreshold = TimeSpan.FromMilliseconds(500),
                WalSaturationFlushLatencySampleWindows = 1,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));
    }

    [Test]
    public async Task RecordFlushLatencyTrip_increments_per_tree_shard_counter()
    {
        WalCommitLogWriter.RecordFlushLatencyTrip(_treeId, 7);
        WalCommitLogWriter.RecordFlushLatencyTrip(_treeId, 7);
        WalCommitLogWriter.RecordFlushLatencyTrip(_treeId, 9);
        WalCommitLogWriter.RecordFlushLatencyTrip("other-tree", 0);

        Assert.That(WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 7)], Is.EqualTo(2L));
        Assert.That(WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 9)], Is.EqualTo(1L));
        Assert.That(WalCommitLogWriter._flushLatencyTripCounts[("other-tree", 0)], Is.EqualTo(1L));
        await Task.CompletedTask;
    }

    [Test]
    public async Task Trees_are_independent_for_the_consecutive_window_counter()
    {
        var otherTree = _treeId + "-other";
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationFlushLatencyThreshold = TimeSpan.FromMilliseconds(500),
                WalSaturationFlushLatencySampleWindows = 2,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // Tree A: 2 consecutive trips -> Saturated. Tree B: 1 trip
        // then 0 then 1 -> counter resets after the gap, so it never
        // reaches the 2-window threshold.
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 1;
        WalCommitLogWriter._flushLatencyTripCounts[(otherTree, 0)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);

        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 2;
        // otherTree: no new trip (delta = 0) -> counter resets.
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "tree A had 2 consecutive non-zero windows");
        Assert.That(signal.GetCurrentState(otherTree), Is.EqualTo(WalSaturationState.Healthy),
            "tree B's counter was reset by the zero-delta window between trips");
    }
}
