using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the opt-in cluster-wide split admission gate
/// (<see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>) wired into the
/// autonomic monitor, the edge-triggered footprint heartbeat that keeps the gate
/// a readable cluster-wide split-activity source even with no ceiling configured
/// (#1224), plus the split-admission metrics that emit regardless of whether the
/// gate is enabled.
/// </summary>
public partial class HotShardMonitorGrainTests
{
    /// <summary>
    /// Substitutes the cluster gate onto the factory and returns it, so a test
    /// can assert exactly which of the admission and heartbeat paths was taken.
    /// </summary>
    private static IClusterSplitConcurrencyGrain SubstituteGate(IGrainFactory grainFactory, int grant = 0)
    {
        var gate = Substitute.For<IClusterSplitConcurrencyGrain>();
        gate.AcquireSlotsAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<TimeSpan>())
            .Returns(grant);
        grainFactory.GetGrain<IClusterSplitConcurrencyGrain>(Arg.Any<long>()).Returns(gate);
        return gate;
    }

    [Test]
    public async Task RunSamplingPass_never_requests_admission_when_the_ceiling_is_null()
    {
        // Default options leave MaxClusterConcurrentAutoSplits null (disabled),
        // so the monitor must make no admission request at all - the per-tree cap
        // alone decides how many splits start.
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
        };
        var (grain, grainFactory, _, splitGrain, _, shardOf, _) = CreateGrain(options: opts);
        var gate = SubstituteGate(grainFactory);
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 10_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        // The split still triggers via the per-tree path...
        await splitGrain.Received(1).SplitAsync(1);
        // ...and no slot was ever requested from the cluster gate.
        await gate.DidNotReceive().AcquireSlotsAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RunSamplingPass_publishes_a_footprint_for_splits_it_triggers_with_no_ceiling()
    {
        // Without a ceiling the gate makes no admission decision, but it is still
        // the cluster's readable split-activity source, so a triggered split must
        // be published immediately rather than a sampling interval later.
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
        };
        var (grain, grainFactory, _, _, _, shardOf, _) = CreateGrain(options: opts);
        var gate = SubstituteGate(grainFactory);
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 10_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        await gate.Received(1).ReportInFlightAsync(TreeId, 1, Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RunSamplingPass_publishes_the_in_flight_count_of_draining_splits()
    {
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        var (grain, grainFactory, _, _, _, shardOf, _) = CreateGrain(physicalShardCount: 3, options: opts);
        var gate = SubstituteGate(grainFactory);
        // Two shards already mid-split, none hot enough to start another.
        shardOf(0).IsSplittingAsync().Returns(true);
        shardOf(1).IsSplittingAsync().Returns(true);

        await grain.RunSamplingPassAsync();

        await gate.Received(1).ReportInFlightAsync(TreeId, 2, Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RunSamplingPass_leaves_the_gate_untouched_on_a_fully_idle_pass()
    {
        // Nothing splitting and nothing hot: the heartbeat is edge-triggered, so
        // an idle tree must cost no extra RPC at all.
        var (grain, grainFactory, _, _, _, _, _) = CreateGrain();
        var gate = SubstituteGate(grainFactory);

        await grain.RunSamplingPassAsync();

        await gate.DidNotReceive().ReportInFlightAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<TimeSpan>());
        grainFactory.DidNotReceive().GetGrain<IClusterSplitConcurrencyGrain>(Arg.Any<long>());
    }

    [Test]
    public async Task RunSamplingPass_clears_its_footprint_once_and_then_goes_quiet()
    {
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        var (grain, grainFactory, _, _, _, shardOf, _) = CreateGrain(physicalShardCount: 2, options: opts);
        var gate = SubstituteGate(grainFactory);

        // Pass one: a split is draining, so a footprint of 1 is published.
        shardOf(0).IsSplittingAsync().Returns(true);
        await grain.RunSamplingPassAsync();
        await gate.Received(1).ReportInFlightAsync(TreeId, 1, Arg.Any<TimeSpan>());

        // Pass two: the split has finished. Exactly one clearing call releases
        // the tree's share without waiting for the footprint to expire.
        shardOf(0).IsSplittingAsync().Returns(false);
        await grain.RunSamplingPassAsync();
        await gate.Received(1).ReportInFlightAsync(TreeId, 0, Arg.Any<TimeSpan>());

        // Pass three: nothing changed, so the monitor stays quiet.
        await grain.RunSamplingPassAsync();
        await gate.Received(2).ReportInFlightAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RunSamplingPass_uses_admission_not_the_heartbeat_when_a_ceiling_is_set()    {
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
            MaxClusterConcurrentAutoSplits = 2,
        };
        var (grain, grainFactory, _, _, _, shardOf, _) = CreateGrain(options: opts);
        var gate = SubstituteGate(grainFactory, grant: 1);
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 10_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        // AcquireSlotsAsync already records the footprint, so the heartbeat must
        // not double-report on the admission path.
        await gate.Received(1).AcquireSlotsAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<TimeSpan>());
        await gate.DidNotReceive().ReportInFlightAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<TimeSpan>());
    }

    // --- Resilience: the heartbeat must never cost the tree its elasticity ---

    [Test]
    public async Task RunSamplingPass_still_triggers_splits_when_the_gate_heartbeat_fails()
    {
        // The heartbeat is pure observability but sits upstream of the split
        // triggers, so a transient gate failure must not abort the pass and
        // leave the tree with zero splits started.
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
        };
        var (grain, grainFactory, _, splitGrain, _, shardOf, _) = CreateGrain(options: opts);
        var gate = SubstituteGate(grainFactory);
        gate.ReportInFlightAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<TimeSpan>())
            .Returns(Task.FromException(new TimeoutException("gate unavailable")));
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 10_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        Assert.That(async () => await grain.RunSamplingPassAsync(), Throws.Nothing);

        await splitGrain.Received(1).SplitAsync(1);
    }

    // --- Suppressed passes must not strand an outstanding footprint ----------

    [Test]
    public async Task RunSamplingPass_keeps_its_footprint_alive_when_a_bulk_graft_suppresses_the_pass()
    {
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        var (grain, grainFactory, _, _, _, shardOf, _) = CreateGrain(physicalShardCount: 2, options: opts);
        var gate = SubstituteGate(grainFactory);
        shardOf(0).IsSplittingAsync().Returns(true);

        // A bulk graft appears while a split is still draining. The graft
        // suppresses new splits but does not stop the one already running, so
        // the footprint must keep being published or the activity source would
        // report an idle cluster mid-split.
        shardOf(1).HasPendingBulkOperationAsync().Returns(true);
        await grain.RunSamplingPassAsync();

        await gate.Received(1).ReportInFlightAsync(TreeId, 1, Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RunSamplingPass_keeps_its_footprint_alive_when_maintenance_suppresses_the_pass()
    {
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        var (grain, grainFactory, lattice, _, _, shardOf, _) = CreateGrain(physicalShardCount: 2, options: opts);
        var gate = SubstituteGate(grainFactory);
        shardOf(0).IsSplittingAsync().Returns(true);

        // Pass one publishes a footprint of 1.
        await grain.RunSamplingPassAsync();
        await gate.Received(1).ReportInFlightAsync(TreeId, 1, Arg.Any<TimeSpan>());

        // A resize starts, which aborts the pass before it can recompute the
        // count. The outstanding footprint must be refreshed at its last known
        // value rather than left to lapse.
        lattice.IsResizeCompleteAsync().Returns(false);
        await grain.RunSamplingPassAsync();

        await gate.Received(2).ReportInFlightAsync(TreeId, 1, Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RunSamplingPass_keeps_its_footprint_alive_when_auto_split_is_disabled_mid_drain()
    {
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        var (grain, grainFactory, _, _, _, shardOf, options) = CreateGrain(physicalShardCount: 2, options: opts);
        var gate = SubstituteGate(grainFactory);
        shardOf(0).IsSplittingAsync().Returns(true);

        await grain.RunSamplingPassAsync();
        await gate.Received(1).ReportInFlightAsync(TreeId, 1, Arg.Any<TimeSpan>());

        // An operator disables auto-split while the split is still draining.
        // The timer keeps ticking and the split keeps running on its own
        // coordinator, so the footprint must keep being refreshed.
        options.AutoSplitEnabled = false;
        await grain.RunSamplingPassAsync();

        await gate.Received(2).ReportInFlightAsync(TreeId, 1, Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RunSamplingPass_does_not_refresh_a_footprint_it_never_published()
    {
        var opts = new LatticeOptions { AutoSplitEnabled = false };
        var (grain, grainFactory, _, _, _, _, _) = CreateGrain(options: opts);
        var gate = SubstituteGate(grainFactory);

        await grain.RunSamplingPassAsync();

        // The refresh is edge-triggered on an outstanding footprint, so a tree
        // that has published nothing stays free of extra calls.
        await gate.DidNotReceive().ReportInFlightAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<TimeSpan>());
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
        var gate = SubstituteGate(grainFactory);
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

        // No admission was requested (the ceiling option is null); the gate is
        // only used as the readable split-activity source on this path.
        await gate.DidNotReceive().AcquireSlotsAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<TimeSpan>());
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
