using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the leaf-materialiser drain-lag classifier input
/// (<see cref="LatticeOptions.WalSaturationMaterialiserLagThreshold"/> +
/// <see cref="LatticeOptions.WalSaturationMaterialiserLagSampleWindows"/>)
/// that <see cref="WalSaturationSampler"/> reads off
/// <see cref="WalCommitLogWriter._materialiserDrainLagLevels"/> - the direct
/// leaf-materialiser drain-lag back-pressure surface (issue #1030). The input
/// is a standing LEVEL (the WAL GC refreshes a per-tree lag observation at its
/// own cadence; the sampler re-reads it every tick), and a sustained run drives
/// <see cref="WalSaturationState.Throttled"/> - a pure back-off - rather than
/// Saturated, so it never engages the writer admission gate's fast-fail.
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
        WalCommitLogWriter._materialiserDrainLagLevels.Clear();
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

    // Writes a fresh standing drain-lag level observation (observed now).
    private void SetLevel(TimeSpan lag, string? tree = null) =>
        WalCommitLogWriter._materialiserDrainLagLevels[tree ?? _treeId] =
            new MaterialiserDrainLagLevel(lag.Ticks, DateTimeOffset.UtcNow.UtcTicks);

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
    public async Task Stale_observation_does_not_escalate()
    {
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(5),
                WalSaturationMaterialiserLagSampleWindows = 1,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // An over-threshold lag observed far in the past (older than the
        // staleness window): the GC has stopped refreshing it, so it must be
        // treated as absent and never escalate.
        WalCommitLogWriter._materialiserDrainLagLevels[_treeId] =
            new MaterialiserDrainLagLevel(
                TimeSpan.FromMinutes(10).Ticks,
                DateTimeOffset.UtcNow.AddHours(-1).UtcTicks);

        for (var i = 0; i < 5; i++)
        {
            await sampler.SampleOnceAsync(CancellationToken.None);
        }

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "a stale level observation must not pin the regime once the GC stops refreshing it");
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
}
