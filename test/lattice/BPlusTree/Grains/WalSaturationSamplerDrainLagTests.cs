using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the fifth Saturated classifier input
/// (<see cref="LatticeOptions.WalSaturationMaterialiserLagThreshold"/> +
/// <see cref="LatticeOptions.WalSaturationMaterialiserLagSampleWindows"/>)
/// that <see cref="WalSaturationSampler"/> reads off
/// <see cref="WalCommitLogWriter._materialiserDrainLagTripCounts"/> - the
/// direct leaf-materialiser drain-lag back-pressure surface (issue #1030).
/// Mirrors <see cref="WalSaturationSamplerFlushLatencyTests"/> by driving the
/// sampler one tick at a time and asserting the tree's resolved state.
/// </summary>
[TestFixture]
public class WalSaturationSamplerDrainLagTests
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
        WalCommitLogWriter._materialiserDrainLagTripCounts.Clear();
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
            NullLogger<WalSaturationSampler>.Instance);
    }

    [Test]
    public async Task Disabled_threshold_never_escalates_regardless_of_trip_count()
    {
        var sampler = CreateSampler(
            new LatticeOptions { WalSaturationMaterialiserLagThreshold = null },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 100;

        for (var i = 0; i < 10; i++)
        {
            await sampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "with the drain-lag input disabled (threshold null), trip counts must never escalate the regime");
    }

    [Test]
    public async Task Escalates_to_Saturated_after_consecutive_windows()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 3,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 2;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 3;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "after 3 consecutive non-zero windows the drain-lag input escalates the regime");
    }

    [Test]
    public async Task Resets_consecutive_counter_when_a_window_has_no_trips()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 3,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);
        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 2;
        await sampler.SampleOnceAsync(CancellationToken.None);

        // Tick with no new trip: counter must reset to 0.
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 3;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "after the counter reset, a single trip window must not immediately re-escalate");
    }

    [Test]
    public async Task Single_window_threshold_escalates_on_first_trip_after_baseline()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 1,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline
        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));
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

        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 1;
        WalCommitLogWriter._materialiserDrainLagTripCounts[(otherTree, 0)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);

        WalCommitLogWriter._materialiserDrainLagTripCounts[(_treeId, 0)] = 2;
        // otherTree: no new trip (delta = 0) -> counter resets.
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "tree A had 2 consecutive non-zero windows");
        Assert.That(signal.GetCurrentState(otherTree), Is.EqualTo(WalSaturationState.Healthy),
            "tree B's counter was reset by the zero-delta window between trips");
    }
}
