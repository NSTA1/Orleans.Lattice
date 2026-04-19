using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public partial class HotShardMonitorGrainTests
{
    private const string TreeId = "monitor-test-tree";

    private static (HotShardMonitorGrain grain,
                    IGrainFactory grainFactory,
                    ILattice lattice,
                    ITreeShardSplitGrain splitGrain,
                    ILatticeRegistry registry,
                    Func<int, IShardRootGrain> shardOf,
                    LatticeOptions options) CreateGrain(
        int physicalShardCount = 2,
        int virtualShardCount = 16,
        LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("monitor", TreeId));
        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options ??= new LatticeOptions
        {
            ShardCount = physicalShardCount,
            VirtualShardCount = virtualShardCount,
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var lattice = Substitute.For<ILattice>();
        lattice.IsResizeCompleteAsync().Returns(true);
        lattice.IsMergeCompleteAsync().Returns(true);
        lattice.IsSnapshotCompleteAsync().Returns(true);
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var splitGrain = Substitute.For<ITreeShardSplitGrain>();
        splitGrain.IsCompleteAsync().Returns(true);
        grainFactory.GetGrain<ITreeShardSplitGrain>(Arg.Any<string>()).Returns(splitGrain);

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(ShardMap.CreateDefault(virtualShardCount, physicalShardCount));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        // One shard substitute per physical shard index. Defaults to cold.
        var shardSubs = new Dictionary<int, IShardRootGrain>();
        IShardRootGrain Shard(int i)
        {
            if (shardSubs.TryGetValue(i, out var s)) return s;
            var sub = Substitute.For<IShardRootGrain>();
            sub.GetHotnessAsync().Returns(new ShardHotness { Reads = 0, Writes = 0, Window = TimeSpan.FromSeconds(30) });
            sub.HasPendingBulkOperationAsync().Returns(false);
            sub.IsSplittingAsync().Returns(false);
            shardSubs[i] = sub;
            return sub;
        }
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            var idx = int.Parse(key[(key.LastIndexOf('/') + 1)..]);
            return Shard(idx);
        });

        var grain = new HotShardMonitorGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            new LoggerFactory().CreateLogger<HotShardMonitorGrain>(),
            new FakePersistentState<HotShardMonitorState>());
        return (grain, grainFactory, lattice, splitGrain, registry, Shard, options);
    }

    [Test]
    public async Task RunSamplingPass_does_nothing_when_AutoSplit_disabled()
    {
        var opts = new LatticeOptions
        {
            ShardCount = 2,
            VirtualShardCount = 16,
            AutoSplitEnabled = false,
        };
        var (grain, _, _, splitGrain, _, _, _) = CreateGrain(options: opts);
        await grain.RunSamplingPassAsync();
        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunSamplingPass_does_not_split_cold_shards()
    {
        var (grain, _, _, splitGrain, _, _, _) = CreateGrain();
        await grain.RunSamplingPassAsync();
        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunSamplingPass_triggers_split_when_shard_exceeds_threshold()
    {
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain();
        // Shard 1 is hot.
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness
        {
            Reads = 5_000, Writes = 5_000, Window = TimeSpan.FromSeconds(10),
        });

        await grain.RunSamplingPassAsync();

        await splitGrain.Received(1).SplitAsync(1);
    }

    [Test]
    public async Task RunSamplingPass_suppressed_when_resize_in_progress()
    {
        var (grain, _, lattice, splitGrain, _, shardOf, _) = CreateGrain();
        lattice.IsResizeCompleteAsync().Returns(false);
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 10_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunSamplingPass_suppressed_when_a_shard_has_pending_bulk_operation()
    {
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain();
        shardOf(0).HasPendingBulkOperationAsync().Returns(true);
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 10_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunSamplingPass_picks_hottest_shard_when_multiple_exceed_threshold()
    {
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain();
        shardOf(0).GetHotnessAsync().Returns(new ShardHotness { Reads = 1_500, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 5_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        await splitGrain.Received(1).SplitAsync(1);
        await splitGrain.DidNotReceive().SplitAsync(0);
    }

    [Test]
    public async Task RunSamplingPass_does_not_split_shard_with_only_one_virtual_slot()
    {
        var slots = new int[] { 0, 0, 0, 1 }; // shard 1 owns single slot
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(new ShardMap { Slots = slots });

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("monitor", TreeId));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var lattice = Substitute.For<ILattice>();
        lattice.IsResizeCompleteAsync().Returns(true);
        lattice.IsMergeCompleteAsync().Returns(true);
        lattice.IsSnapshotCompleteAsync().Returns(true);
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);
        var splitGrain = Substitute.For<ITreeShardSplitGrain>();
        splitGrain.IsCompleteAsync().Returns(true);
        grainFactory.GetGrain<ITreeShardSplitGrain>(Arg.Any<string>()).Returns(splitGrain);

        var hotShard1 = Substitute.For<IShardRootGrain>();
        hotShard1.GetHotnessAsync().Returns(new ShardHotness { Reads = 10_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        hotShard1.HasPendingBulkOperationAsync().Returns(false);
        hotShard1.IsSplittingAsync().Returns(false);
        var coldShard0 = Substitute.For<IShardRootGrain>();
        coldShard0.GetHotnessAsync().Returns(new ShardHotness { Reads = 0, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        coldShard0.HasPendingBulkOperationAsync().Returns(false);
        coldShard0.IsSplittingAsync().Returns(false);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(coldShard0);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/1").Returns(hotShard1);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            ShardCount = 2, VirtualShardCount = 4,
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
        });

        var grain = new HotShardMonitorGrain(
            context, grainFactory, Substitute.For<IReminderRegistry>(), optionsMonitor,
            new LoggerFactory().CreateLogger<HotShardMonitorGrain>(),
            new FakePersistentState<HotShardMonitorState>());

        await grain.RunSamplingPassAsync();

        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunSamplingPass_suppressed_when_max_concurrent_in_flight_already_reached()
    {
        var opts = new LatticeOptions
        {
            ShardCount = 2, VirtualShardCount = 16,
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(options: opts);
        // Shard 0 is already splitting (counts as 1 in-flight). Shard 1 is hot
        // but the cap is reached so no new split is triggered this tick.
        shardOf(0).IsSplittingAsync().Returns(true);
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 10_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunSamplingPass_triggers_two_splits_when_two_shards_hot_and_MaxConcurrent_is_2()
    {
        var opts = new LatticeOptions
        {
            ShardCount = 4, VirtualShardCount = 16,
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: 4, options: opts);
        // Two hot shards, two cold shards, no in-flight splits, cap = 2 → both hot ones split.
        shardOf(0).GetHotnessAsync().Returns(new ShardHotness { Reads = 5_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 8_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(2).GetHotnessAsync().Returns(new ShardHotness { Reads = 0, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(3).GetHotnessAsync().Returns(new ShardHotness { Reads = 0, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        await splitGrain.Received(1).SplitAsync(0);
        await splitGrain.Received(1).SplitAsync(1);
    }

    [Test]
    public async Task RunSamplingPass_caps_concurrent_splits_at_MaxConcurrentAutoSplits()
    {
        var opts = new LatticeOptions
        {
            ShardCount = 4, VirtualShardCount = 16,
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
        };
        var (grain, _, _, splitGrain, _, shardOf, _) = CreateGrain(physicalShardCount: 4, options: opts);
        // Three hot shards but cap = 2 → only the two hottest split.
        shardOf(0).GetHotnessAsync().Returns(new ShardHotness { Reads = 5_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(1).GetHotnessAsync().Returns(new ShardHotness { Reads = 8_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(2).GetHotnessAsync().Returns(new ShardHotness { Reads = 3_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        shardOf(3).GetHotnessAsync().Returns(new ShardHotness { Reads = 0, Writes = 0, Window = TimeSpan.FromSeconds(10) });

        await grain.RunSamplingPassAsync();

        await splitGrain.Received(1).SplitAsync(1); // hottest
        await splitGrain.Received(1).SplitAsync(0); // second hottest
        await splitGrain.DidNotReceive().SplitAsync(2); // third — over cap
    }

    [Test]
    public async Task ActivationUtc_persists_and_survives_reactivation()
    {
        // FX-004 regression: the monitor's first-ever activation time must
        // be persisted so the AutoSplitMinTreeAge grace period is
        // anchored to the tree's first activation, not the current silo
        // restart. Without this, a cluster that restarts silos more
        // often than the grace window would never trigger an autonomic
        // split.
        var sharedState = new FakePersistentState<HotShardMonitorState>();
        LatticeOptions opts = new()
        {
            ShardCount = 2, VirtualShardCount = 16,
            AutoSplitMinTreeAge = TimeSpan.FromMinutes(5),
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };

        // --- First activation: writes the activation time to state.
        var (_, _, _, splitGrain1, _, shardOf1, _) = CreateGrain(options: opts);
        // Patch the grain instance to use the shared state so a second
        // activation can read it back. We rebuild the grain manually for
        // this scenario because CreateGrain allocates an isolated state.
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("monitor", TreeId));
        var gf = Substitute.For<IGrainFactory>();
        var om = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        om.Get(Arg.Any<string>()).Returns(opts);
        var lattice = Substitute.For<ILattice>();
        lattice.IsResizeCompleteAsync().Returns(true);
        lattice.IsMergeCompleteAsync().Returns(true);
        lattice.IsSnapshotCompleteAsync().Returns(true);
        gf.GetGrain<ILattice>(TreeId).Returns(lattice);
        var reg = Substitute.For<ILatticeRegistry>();
        reg.ResolveAsync(TreeId).Returns(TreeId);
        reg.GetShardMapAsync(TreeId).Returns(ShardMap.CreateDefault(16, 2));
        gf.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(reg);
        var split = Substitute.For<ITreeShardSplitGrain>();
        split.IsCompleteAsync().Returns(true);
        gf.GetGrain<ITreeShardSplitGrain>(Arg.Any<string>()).Returns(split);
        var hotShard = Substitute.For<IShardRootGrain>();
        hotShard.GetHotnessAsync().Returns(new ShardHotness { Reads = 50_000, Writes = 0, Window = TimeSpan.FromSeconds(10) });
        hotShard.HasPendingBulkOperationAsync().Returns(false);
        hotShard.IsSplittingAsync().Returns(false);
        gf.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(hotShard);

        var first = new HotShardMonitorGrain(
            ctx, gf, Substitute.For<IReminderRegistry>(), om,
            new LoggerFactory().CreateLogger<HotShardMonitorGrain>(), sharedState);
        await first.RunSamplingPassAsync();

        Assert.That(sharedState.State.ActivationUtc, Is.Not.Null, "first activation should persist ActivationUtc");
        var persistedTime = sharedState.State.ActivationUtc!.Value;

        // Grace window is 5min and ActivationUtc was just written → no split.
        await split.DidNotReceive().SplitAsync(Arg.Any<int>());

        // --- Second activation (simulated restart): reuses persisted time.
        var second = new HotShardMonitorGrain(
            ctx, gf, Substitute.For<IReminderRegistry>(), om,
            new LoggerFactory().CreateLogger<HotShardMonitorGrain>(), sharedState);
        await second.RunSamplingPassAsync();

        Assert.That(sharedState.State.ActivationUtc, Is.EqualTo(persistedTime),
            "second activation must not overwrite the persisted ActivationUtc");
    }
}
