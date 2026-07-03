using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the opt-in cluster-wide split admission gate
/// (<see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>) wired into the
/// autonomic monitor, plus the split-admission metrics that emit regardless of
/// whether the gate is enabled.
/// </summary>
public partial class HotShardMonitorGrainTests
{
    [Test]
    public async Task RunSamplingPass_does_not_consult_cluster_gate_when_option_is_null()
    {
        // Default options leave MaxClusterConcurrentAutoSplits null (disabled),
        // so the monitor must never resolve or call the cluster gate grain.
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
        };
        var (grain, grainFactory, _, splitGrain, _, shardOf, _) = CreateGrain(options: opts);
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 10_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        // The split still triggers via the per-tree path...
        await splitGrain.Received(1).SplitAsync(1);
        // ...but the cluster gate was never touched (zero-cost disabled path).
        grainFactory.DidNotReceive().GetGrain<IClusterSplitConcurrencyGrain>(Arg.Any<long>());
    }

    [Test]
    public async Task RunSamplingPass_cluster_cap_limits_triggers_and_emits_admission_deferred()
    {
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 3,
            MaxClusterConcurrentAutoSplits = 1,
        };
        var (grain, grainFactory, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: 4, options: opts);
        shardOf(0).GetHotnessAsync().Returns(new ShardHotness { Reads = 5_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 8_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(2).GetHotnessAsync().Returns(new ShardHotness { Reads = 3_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(3).GetHotnessAsync().Returns(new ShardHotness { Reads = 0, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        // The gate grants exactly one slot against the cluster ceiling.
        var gate = Substitute.For<IClusterSplitConcurrencyGrain>();
        gate.AcquireSlotsAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<TimeSpan>())
            .Returns(1);
        grainFactory.GetGrain<IClusterSplitConcurrencyGrain>(Arg.Any<long>()).Returns(gate);

        using var recorder = new LongMetricRecorder();
        await grain.RunSamplingPassAsync();

        // Only one split triggered despite three eligible hot shards and three
        // per-tree slots, because the cluster ceiling admitted a single split.
        await splitGrain.Received(1).SplitAsync(Arg.Any<int>());

        // Two otherwise-eligible splits were held back by the cluster gate.
        var deferred = recorder.Sum(LatticeMetrics.SplitAdmissionDeferred.Name);
        Assert.That(deferred, Is.EqualTo(2), "two cluster-cap denials must increment admission.deferred");
        Assert.That(recorder.HasReasonTag(LatticeMetrics.SplitAdmissionDeferred.Name, "cluster_cap"), Is.True);

        // The monitor reports its footprint to the gate once per pass; there is
        // no separate release step in the heartbeat model.
        await gate.Received(1).AcquireSlotsAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RunSamplingPass_emits_in_flight_and_candidates_suppressed_when_gate_disabled()
    {
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1, // one slot, two hot shards -> one suppressed
        };
        var (grain, grainFactory, _, _, _, shardOf, _) = CreateGrain(options: opts);
        shardOf(0).GetHotnessAsync().Returns(new ShardHotness { Reads = 5_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 8_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        using var recorder = new LongMetricRecorder();
        await grain.RunSamplingPassAsync();

        // split.in_flight is sampled every pass even with the gate disabled.
        Assert.That(recorder.Count(LatticeMetrics.SplitInFlight.Name), Is.GreaterThanOrEqualTo(1),
            "split.in_flight must be sampled every pass regardless of the gate");

        // One eligible hot shard was suppressed by the per-tree cap.
        Assert.That(recorder.Sum(LatticeMetrics.SplitCandidatesSuppressed.Name), Is.EqualTo(1),
            "the second hot shard is suppressed by the per-tree cap and must be counted");

        // The gate itself was never consulted (option is null).
        grainFactory.DidNotReceive().GetGrain<IClusterSplitConcurrencyGrain>(Arg.Any<long>());
    }

    /// <summary>
    /// Captures every <c>long</c> measurement on <see cref="LatticeMetrics.Meter"/>
    /// so split-admission instruments can be asserted at read time.
    /// </summary>
    private sealed class LongMetricRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> _records = new();
        private readonly object _lock = new();

        public LongMetricRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                        l.EnableMeasurementEvents(inst);
                },
            };
            _listener.SetMeasurementEventCallback<long>(OnLong);
            _listener.Start();
        }

        private void OnLong(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            lock (_lock)
            {
                _records.Add((instrument.Name, value, tags.ToArray()));
            }
        }

        public long Sum(string name)
        {
            lock (_lock)
            {
                long total = 0;
                foreach (var r in _records)
                    if (r.Name == name) total += r.Value;
                return total;
            }
        }

        public int Count(string name)
        {
            lock (_lock)
            {
                var n = 0;
                foreach (var r in _records)
                    if (r.Name == name) n++;
                return n;
            }
        }

        public bool HasReasonTag(string name, string reason)
        {
            lock (_lock)
            {
                foreach (var r in _records)
                {
                    if (r.Name != name) continue;
                    foreach (var t in r.Tags)
                        if (t.Key == LatticeMetrics.TagReason && (string?)t.Value == reason) return true;
                }
                return false;
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
