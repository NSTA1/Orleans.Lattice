using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class HotShardMonitorGrainTests
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
            new LoggerFactory().CreateLogger<HotShardMonitorGrain>());
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
            new LoggerFactory().CreateLogger<HotShardMonitorGrain>());

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
}
