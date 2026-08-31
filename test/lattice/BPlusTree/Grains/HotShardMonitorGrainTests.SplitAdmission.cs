using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for shape-aware split admission: the monitor must refuse to split a
/// tree whose load is high but uniform (the bulk-ingest shape that shattered the
/// vector trees into roughly 1,100 leaves against a baseline of 64), while still
/// relieving a genuinely skewed hot shard exactly as before.
/// </summary>
public partial class HotShardMonitorGrainTests
{
    /// <summary>
    /// Manually advanced <see cref="TimeProvider"/> so the per-shard split
    /// cooldown can be driven without wall-clock waits.
    /// </summary>
    private sealed class ManualClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

    /// <summary>
    /// Drives every shard at the same high rate, which is what a bulk ingest
    /// streaming writes through the whole key space looks like.
    /// </summary>
    private static void ApplyUniformLoad(Func<int, IShardRootGrain> shardOf, int shardCount, long opsPerWindow)
    {
        for (var i = 0; i < shardCount; i++)
        {
            shardOf(i).GetHotnessAsync().Returns(new ShardHotness
            {
                Reads = 0,
                Writes = opsPerWindow,
                Window = TimeSpan.FromSeconds(10),
            });
        }
    }

    /// <summary>
    /// Drives one shard far above a uniform background, which is what a
    /// read-skewed production workload looks like.
    /// </summary>
    private static void ApplySkewedLoad(
        Func<int, IShardRootGrain> shardOf, int shardCount, int hotShard, long backgroundOps, long hotOps)
    {
        for (var i = 0; i < shardCount; i++)
        {
            shardOf(i).GetHotnessAsync().Returns(new ShardHotness
            {
                Reads = i == hotShard ? hotOps : backgroundOps,
                Writes = 0,
                Window = TimeSpan.FromSeconds(10),
            });
        }
    }

    [Test]
    public async Task RunSamplingPass_admits_no_splits_under_sustained_uniform_bulk_write()
    {
        // Every shard streaming writes at 5,000 ops/s - twenty-five times the
        // shipped threshold - but none disproportionately loaded. Splitting
        // cannot relieve this workload: each half would be equally hot, and the
        // only durable effect is a permanent multiplication of activations.
        const int Shards = 8;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        ApplyUniformLoad(shardOf, Shards, opsPerWindow: 50_000);

        using var recorder = new LongMetricRecorder();
        await grain.RunSamplingPassAsync();

        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
        Assert.Multiple(() =>
        {
            Assert.That(recorder.Sum(LatticeMetrics.SplitAdmissionDeferred.Name), Is.EqualTo(Shards),
                "every hot-but-uniform shard must be counted as a deferred split candidate");
            Assert.That(recorder.HasReasonTag(LatticeMetrics.SplitAdmissionDeferred.Name, "uniform_load"), Is.True,
                "the deferral must be attributed to uniform load, not to a concurrency cap");
        });
    }

    [Test]
    public async Task RunSamplingPass_does_not_probe_occupancy_when_the_tree_is_uniformly_loaded()
    {
        // The occupancy probe costs a leaf-chain walk per candidate. A tree
        // under bulk ingest produces no candidates at all, so it must pay
        // nothing for the new gate.
        const int Shards = 8;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
        };
        var (grain, _, _, _, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        ApplyUniformLoad(shardOf, Shards, opsPerWindow: 50_000);

        await grain.RunSamplingPassAsync();

        for (var i = 0; i < Shards; i++)
            await shardOf(i).DidNotReceive().CountAsync();
    }

    [Test]
    public async Task RunSamplingPass_still_splits_a_genuinely_skewed_hot_shard()
    {
        // Regression guard for the capability this change must preserve: one
        // shard at ten times the background rate is a real hot spot and must
        // still be relieved.
        const int Shards = 8;
        const int HotShard = 3;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        ApplySkewedLoad(shardOf, Shards, HotShard, backgroundOps: 3_000, hotOps: 30_000);

        await grain.RunSamplingPassAsync();

        await splitGrain.Received(1).SplitAsync(HotShard);
    }

    [Test]
    public async Task RunSamplingPass_does_not_split_a_shard_holding_too_few_entries()
    {
        // The measured pathology: shards carrying about 33 records each.
        // Splitting one cannot redistribute anything.
        const int Shards = 8;
        const int HotShard = 3;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        ApplySkewedLoad(shardOf, Shards, HotShard, backgroundOps: 0, hotOps: 30_000);
        shardOf(HotShard).CountAsync().Returns(33);

        using var recorder = new LongMetricRecorder();
        await grain.RunSamplingPassAsync();

        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
        Assert.Multiple(() =>
        {
            Assert.That(recorder.HasReasonTag(LatticeMetrics.SplitAdmissionDeferred.Name, "low_occupancy"), Is.True,
                "an under-occupied hot shard must be deferred on occupancy grounds");
            Assert.That(recorder.Sum(LatticeMetrics.SplitAdmissionDeferred.Name), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RunSamplingPass_probes_occupancy_only_for_shards_that_cleared_every_cheaper_clause()
    {
        const int Shards = 8;
        const int HotShard = 3;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        var (grain, _, _, _, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        ApplySkewedLoad(shardOf, Shards, HotShard, backgroundOps: 0, hotOps: 30_000);

        await grain.RunSamplingPassAsync();

        await shardOf(HotShard).Received(1).CountAsync();
        for (var i = 0; i < Shards; i++)
        {
            if (i == HotShard) continue;
            await shardOf(i).DidNotReceive().CountAsync();
        }
    }

    [Test]
    public async Task RunSamplingPass_skips_the_occupancy_probe_when_the_floor_is_disabled()
    {
        const int Shards = 8;
        const int HotShard = 3;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
            HotShardMinShardEntries = 0,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        ApplySkewedLoad(shardOf, Shards, HotShard, backgroundOps: 0, hotOps: 30_000);

        await grain.RunSamplingPassAsync();

        await splitGrain.Received(1).SplitAsync(HotShard);
        await shardOf(HotShard).DidNotReceive().CountAsync();
    }

    [Test]
    public async Task RunSamplingPass_refuses_splits_once_the_tree_reaches_its_shard_ceiling()
    {
        // A pathological signal must not be able to run a tree away: past the
        // ceiling no shard is admitted, however skewed the load.
        const int Shards = 8;
        const int HotShard = 3;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
            MaxPhysicalShardsPerTree = Shards,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        ApplySkewedLoad(shardOf, Shards, HotShard, backgroundOps: 0, hotOps: 30_000);

        using var recorder = new LongMetricRecorder();
        await grain.RunSamplingPassAsync();

        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
        Assert.Multiple(() =>
        {
            Assert.That(recorder.HasReasonTag(LatticeMetrics.SplitAdmissionDeferred.Name, "shard_ceiling"), Is.True);
            Assert.That(recorder.Sum(LatticeMetrics.SplitAdmissionDeferred.Name), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RunSamplingPass_admits_a_split_one_shard_below_the_ceiling()
    {
        const int Shards = 8;
        const int HotShard = 3;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
            MaxPhysicalShardsPerTree = Shards + 1,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        ApplySkewedLoad(shardOf, Shards, HotShard, backgroundOps: 0, hotOps: 30_000);

        await grain.RunSamplingPassAsync();

        await splitGrain.Received(1).SplitAsync(HotShard);
    }

    [Test]
    public async Task RunSamplingPass_honours_the_split_cooldown_on_the_injected_clock()
    {
        const int Shards = 8;
        const int HotShard = 3;
        var cooldown = TimeSpan.FromMinutes(2);
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
            HotShardSplitCooldown = cooldown,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        var clock = new ManualClock(DateTimeOffset.UnixEpoch);
        grain.TimeProvider = clock;
        ApplySkewedLoad(shardOf, Shards, HotShard, backgroundOps: 0, hotOps: 30_000);

        await grain.RunSamplingPassAsync();
        await splitGrain.Received(1).SplitAsync(HotShard);

        // Still inside the cooldown window: the shard stays hot but is skipped.
        clock.Advance(cooldown - TimeSpan.FromSeconds(1));
        await grain.RunSamplingPassAsync();
        await splitGrain.Received(1).SplitAsync(HotShard);

        // Past the window: the shard becomes a candidate again.
        clock.Advance(TimeSpan.FromSeconds(2));
        await grain.RunSamplingPassAsync();
        await splitGrain.Received(2).SplitAsync(HotShard);
    }

    [Test]
    public async Task RunSamplingPass_admits_uniform_load_when_the_skew_gate_is_disabled()
    {
        // The skew clause is a knob, not a hard-coded rule: setting the ratio to
        // 1.0 or below restores pure rate-based admission for an operator who
        // deliberately wants it.
        const int Shards = 8;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
            HotShardMinSkewRatio = 1d,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        ApplyUniformLoad(shardOf, Shards, opsPerWindow: 50_000);

        await grain.RunSamplingPassAsync();

        await splitGrain.Received(1).SplitAsync(Arg.Any<int>());
    }

    [Test]
    public void RunSamplingPass_awaits_every_occupancy_probe_even_when_one_faults()
    {
        // The probes are dispatched together, so a failing one must not leave a
        // sibling's task unobserved. The pass surfaces the first failure (the
        // timer tick logs it) after every probe has been awaited.
        const int Shards = 4;
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: Shards, options: opts);
        for (var i = 0; i < Shards; i++)
        {
            shardOf(i).GetHotnessAsync().Returns(new ShardHotness
            {
                Reads = i < 2 ? 30_000 : 0,
                Writes = 0,
                Window = TimeSpan.FromSeconds(10),
            });
        }
        shardOf(0).CountAsync().Returns(Task.FromException<int>(new InvalidOperationException("probe failed")));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RunSamplingPassAsync());

        Assert.That(
            shardOf(1).ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.CountAsync)),
            Is.EqualTo(1),
            "the sibling probe must still be awaited so its task is observed");
        splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
    }
}
