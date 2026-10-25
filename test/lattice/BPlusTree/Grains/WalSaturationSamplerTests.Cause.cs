using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalSaturationSamplerTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task SampleOnceAsync_sums_acute_trip_deltas_before_publishing_every_partition(bool provider)
    {
        var recorder = new RecordingWalSaturationObserver();
        var sampler = CreateSampler(new LatticeOptions
        {
            WalSaturationDispatchTimeoutThreshold = 2,
            WalSaturationProviderFailureRateThreshold = 2,
        }, [recorder], out var signal, out _);
        var counters = provider
            ? WalCommitLogWriter._providerFailureCounts
            : WalCommitLogWriter._dispatchTimeoutCounts;
        try
        {
            for (var partition = 0; partition < 2; partition++)
            {
                SeedPartition(_treeId, partition, depth: 0, cap: 16);
                counters[(_treeId, partition)] = 0;
            }
            await sampler.SampleOnceAsync(CancellationToken.None);
            Assert.That(recorder.Changes, Is.Empty);
            for (var partition = 0; partition < 2; partition++)
                counters[(_treeId, partition)] = 1;

            await sampler.SampleOnceAsync(CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));
                for (var partition = 0; partition < 2; partition++)
                    Assert.That(((IWalPartitionSaturationSignal)signal).GetCurrentState(_treeId, partition),
                        Is.EqualTo(WalSaturationState.Saturated),
                        "each individually sub-threshold partition inherits the summed acute tree input");
                Assert.That(recorder.Changes.Select(change => change.Cause), Is.EqualTo(new[]
                {
                    provider ? WalSaturationCause.ProviderFailures : WalSaturationCause.DispatchTimeouts,
                }));
            });
        }
        finally
        {
            signal.ResetForTesting();
        }
    }

    [TestCase(WalSaturationCause.DispatchTimeouts)]
    [TestCase(WalSaturationCause.ProviderFailures)]
    [TestCase(WalSaturationCause.FlushLatency)]
    [TestCase(WalSaturationCause.AdmissionDepth)]
    [TestCase(WalSaturationCause.MaterialiserDrainLag)]
    [TestCase(WalSaturationCause.MaterialiserPinLatency)]
    [TestCase(WalSaturationCause.None)]
    public async Task SampleOnceAsync_attributes_each_cause_in_priority_order_and_floor_only_None(
        WalSaturationCause expected)
    {
        var recorder = new RecordingWalSaturationObserver();
        var options = new LatticeOptions
        {
            WalSaturationRecoveryWindow = expected == WalSaturationCause.None
                ? TimeSpan.FromMinutes(5) : TimeSpan.Zero,
            WalSaturationAcuteOnly = true,
            WalSaturationDispatchTimeoutThreshold = 1,
            WalSaturationProviderFailureRateThreshold = 1,
            WalSaturationFlushLatencyThreshold = TimeSpan.FromMilliseconds(1),
            WalSaturationFlushLatencySampleWindows = 1,
            WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(1),
            WalSaturationMaterialiserLagSampleWindows = 1,
            WalSaturationMaterialiserPinLatencyThreshold = TimeSpan.FromMilliseconds(1),
            WalSaturationMaterialiserPinLatencySampleWindows = 1,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var cursors = Substitute.For<IWalCursorRegistry>();
        cursors.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>([]));
        var signal = new WalSaturationSignal();
        var dispatcher = new WalSaturationObserverDispatcher([recorder],
            NullLogger<WalSaturationObserverDispatcher>.Instance);
        var sampler = new WalSaturationSampler(signal, dispatcher, monitor,
            NullLogger<WalSaturationSampler>.Instance, cursors);
        SeedPartition(_treeId, 0, depth: 0, cap: 16);
        WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 0)] = 0;
        WalCommitLogWriter._providerFailureCounts[(_treeId, 0)] = 0;
        WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 0;
        WalMaterialiserPinPressure.ResetForTests();
        try
        {
            await sampler.SampleOnceAsync(CancellationToken.None);
            Assert.That(recorder.Changes, Is.Empty, "establish counter baselines while Healthy");
            if (expected == WalSaturationCause.None)
            {
                WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 0)] = 1;
                await sampler.SampleOnceAsync(CancellationToken.None);
                Assert.That(recorder.Changes.Single().Cause, Is.EqualTo(WalSaturationCause.DispatchTimeouts));
                await sampler.SampleOnceAsync(CancellationToken.None);
            }
            else
            {
                var acute = expected is WalSaturationCause.DispatchTimeouts
                    or WalSaturationCause.ProviderFailures or WalSaturationCause.FlushLatency;
                if (expected == WalSaturationCause.DispatchTimeouts)
                    WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 0)] = 1;
                if (expected is WalSaturationCause.DispatchTimeouts or WalSaturationCause.ProviderFailures)
                    WalCommitLogWriter._providerFailureCounts[(_treeId, 0)] = 1;
                if (acute)
                    WalCommitLogWriter._flushLatencyTripCounts[(_treeId, 0)] = 1;
                if (acute || expected == WalSaturationCause.AdmissionDepth)
                    SeedPartition(_treeId, 0, depth: 12, cap: 16);
                if (expected != WalSaturationCause.MaterialiserPinLatency)
                {
                    var now = DateTimeOffset.UtcNow.UtcTicks;
                    var timeline = new WalAppendTimeline();
                    timeline.Record(1, now - TimeSpan.FromMinutes(1).Ticks);
                    WalCommitLogWriter._walAppendTimes.GetOrAdd(_treeId, static _ => new())[0] = timeline;
                    IReadOnlyList<WalCursorSnapshot> snapshots =
                    [
                        new("cause-reader", new HybridLogicalClock { WallClockTicks = 1 }, now)
                        {
                            CheckpointOffsets = new Dictionary<int, long> { [0] = 0 },
                        },
                    ];
                    cursors.SnapshotAsync(_treeId, Arg.Any<CancellationToken>()).Returns(Task.FromResult(snapshots));
                }
                WalMaterialiserPinPressure.RecordWrite(
                    _treeId + WalMaterialiserPinRouting.ShardSeparator + "0",
                    elapsedMs: 30_000, faulted: false, latencyThresholdMs: 1);
                await sampler.SampleOnceAsync(CancellationToken.None);
            }

            var change = recorder.Changes.Last();
            Assert.Multiple(() =>
            {
                Assert.That(change.Cause, Is.EqualTo(expected),
                    "the highest-priority fresh input wins; recovery-only Throttled has no fresh cause");
                Assert.That(change.NewState, Is.EqualTo(expected is WalSaturationCause.DispatchTimeouts
                    or WalSaturationCause.ProviderFailures or WalSaturationCause.FlushLatency
                        ? WalSaturationState.Saturated : WalSaturationState.Throttled));
            });
        }
        finally
        {
            WalCommitLogWriter._flushLatencyTripCounts.TryRemove((_treeId, 0), out _);
            WalCommitLogWriter._walAppendTimes.TryRemove(_treeId, out _);
            WalMaterialiserPinPressure.ResetForTests();
            signal.ResetForTesting();
        }
    }
}
