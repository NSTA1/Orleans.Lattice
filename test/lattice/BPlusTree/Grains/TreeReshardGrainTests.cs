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
public class TreeReshardGrainTests
{
    private const string TreeId = "reshard-test-tree";

    private static (TreeReshardGrain grain,
                    FakePersistentState<TreeReshardState> state,
                    IGrainFactory grainFactory,
                    ILatticeRegistry registry) CreateGrain(
        int virtualShardCount = 16,
        int physicalShardCount = 2,
        ShardMap? existingMap = null,
        FakePersistentState<TreeReshardState>? existingState = null,
        int maxConcurrentMigrations = 4)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("reshard", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            ShardCount = physicalShardCount,
            VirtualShardCount = virtualShardCount,
            MaxConcurrentMigrations = maxConcurrentMigrations,
        });

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(existingMap
            ?? ShardMap.CreateDefault(virtualShardCount, physicalShardCount));

        var state = existingState ?? new FakePersistentState<TreeReshardState>();
        var grain = new TreeReshardGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            new LoggerFactory().CreateLogger<TreeReshardGrain>(), state);
        return (grain, state, grainFactory, registry);
    }

    // --- ReshardAsync validation ---

    [Test]
    public void ReshardAsync_throws_when_target_below_two()
    {
        var (grain, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ReshardAsync(1));
    }

    [Test]
    public void ReshardAsync_throws_when_target_exceeds_virtual_shard_count()
    {
        var (grain, _, _, _) = CreateGrain(virtualShardCount: 16);
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ReshardAsync(17));
    }

    [Test]
    public void ReshardAsync_throws_when_target_equals_current_shard_count()
    {
        var (grain, _, _, _) = CreateGrain(physicalShardCount: 2);
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ReshardAsync(2));
    }

    [Test]
    public void ReshardAsync_throws_when_target_below_current_shard_count()
    {
        var (grain, _, _, _) = CreateGrain(physicalShardCount: 4);
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ReshardAsync(2));
    }

    [Test]
    public async Task ReshardAsync_idempotent_when_in_progress_for_same_target()
    {
        var (grain, state, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.TargetShardCount = 4;

        await grain.ReshardAsync(4);

        Assert.That(state.State.TargetShardCount, Is.EqualTo(4));
    }

    [Test]
    public void ReshardAsync_throws_when_in_progress_for_different_target()
    {
        var (grain, state, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.TargetShardCount = 4;

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ReshardAsync(8));
    }

    // NOTE: The success path of ReshardAsync (which starts a grain timer via
    // RegisterGrainTimer) requires a real Orleans runtime service provider,
    // so it is covered by ReshardIntegrationTests rather than unit-tested here.

    // --- MigrateAsync ---

    [Test]
    public async Task MigrateAsync_completes_when_target_shard_count_already_reached()
    {
        var map = ShardMap.CreateDefault(16, 4); // 4 distinct shards.
        var (grain, state, _, _) = CreateGrain(virtualShardCount: 16, physicalShardCount: 4, existingMap: map);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 4;

        await grain.MigrateAsync();

        Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.Complete));
    }

    [Test]
    public async Task MigrateAsync_dispatches_splits_on_largest_slot_holders()
    {
        // 2 physical shards, each owning 8 of 16 slots. Target 4 shards.
        var map = ShardMap.CreateDefault(16, 2);
        var (grain, state, grainFactory, _) = CreateGrain(
            virtualShardCount: 16, physicalShardCount: 2,
            existingMap: map, maxConcurrentMigrations: 4);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 4;

        // Stub the shard roots used for IsSplittingAsync polling.
        var shard0 = Substitute.For<IShardRootGrain>();
        var shard1 = Substitute.For<IShardRootGrain>();
        shard0.IsSplittingAsync().Returns(false);
        shard1.IsSplittingAsync().Returns(false);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(shard0);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/1").Returns(shard1);

        // Stub the per-shard split coordinators.
        var split0 = Substitute.For<ITreeShardSplitGrain>();
        var split1 = Substitute.For<ITreeShardSplitGrain>();
        grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/0").Returns(split0);
        grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/1").Returns(split1);

        await grain.MigrateAsync();

        // Both shards needed (target=4, current=2) → both splits dispatched.
        await split0.Received(1).SplitAsync(0);
        await split1.Received(1).SplitAsync(1);
    }

    [Test]
    public async Task MigrateAsync_respects_max_concurrent_migrations()
    {
        // 4 physical shards, each owning 4 of 16 slots. Target 8 shards (need 4 more).
        var map = ShardMap.CreateDefault(16, 4);
        var (grain, state, grainFactory, _) = CreateGrain(
            virtualShardCount: 16, physicalShardCount: 4,
            existingMap: map, maxConcurrentMigrations: 2);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 8;

        for (int i = 0; i < 4; i++)
        {
            var s = Substitute.For<IShardRootGrain>();
            s.IsSplittingAsync().Returns(false);
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(s);
        }
        var splits = new ITreeShardSplitGrain[4];
        for (int i = 0; i < 4; i++)
        {
            splits[i] = Substitute.For<ITreeShardSplitGrain>();
            grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/{i}").Returns(splits[i]);
        }

        await grain.MigrateAsync();

        var totalDispatches = 0;
        for (int i = 0; i < 4; i++)
            totalDispatches += splits[i].ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITreeShardSplitGrain.SplitAsync));

        Assert.That(totalDispatches, Is.EqualTo(2),
            "Only MaxConcurrentMigrations splits should be dispatched per tick.");
    }

    [Test]
    public async Task MigrateAsync_returns_early_when_in_flight_at_cap()
    {
        var map = ShardMap.CreateDefault(16, 4);
        var (grain, state, grainFactory, _) = CreateGrain(
            virtualShardCount: 16, physicalShardCount: 4,
            existingMap: map, maxConcurrentMigrations: 2);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 8;

        // Two shards already splitting (at cap).
        var shards = new IShardRootGrain[4];
        for (int i = 0; i < 4; i++)
        {
            shards[i] = Substitute.For<IShardRootGrain>();
            shards[i].IsSplittingAsync().Returns(i < 2);
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(shards[i]);
        }
        var splits = new ITreeShardSplitGrain[4];
        for (int i = 0; i < 4; i++)
        {
            splits[i] = Substitute.For<ITreeShardSplitGrain>();
            grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/{i}").Returns(splits[i]);
        }

        await grain.MigrateAsync();

        for (int i = 0; i < 4; i++)
            await splits[i].DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task MigrateAsync_skips_shards_with_fewer_than_two_slots()
    {
        // Shard 0 owns 15 slots, shard 1 owns 1 slot — only shard 0 is eligible.
        var slots = new int[16];
        for (int i = 0; i < 15; i++) slots[i] = 0;
        slots[15] = 1;
        var map = new ShardMap { Slots = slots };

        var (grain, state, grainFactory, _) = CreateGrain(
            virtualShardCount: 16, physicalShardCount: 2,
            existingMap: map, maxConcurrentMigrations: 4);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 4;

        var shard0 = Substitute.For<IShardRootGrain>();
        shard0.IsSplittingAsync().Returns(false);
        var shard1 = Substitute.For<IShardRootGrain>();
        shard1.IsSplittingAsync().Returns(false);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(shard0);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/1").Returns(shard1);

        var split0 = Substitute.For<ITreeShardSplitGrain>();
        var split1 = Substitute.For<ITreeShardSplitGrain>();
        grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/0").Returns(split0);
        grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/1").Returns(split1);

        await grain.MigrateAsync();

        await split0.Received(1).SplitAsync(0);
        await split1.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task MigrateAsync_swallows_invalid_operation_from_dispatch()
    {
        var map = ShardMap.CreateDefault(16, 2);
        var (grain, state, grainFactory, _) = CreateGrain(
            virtualShardCount: 16, physicalShardCount: 2, existingMap: map);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 4;

        var shard0 = Substitute.For<IShardRootGrain>();
        shard0.IsSplittingAsync().Returns(false);
        var shard1 = Substitute.For<IShardRootGrain>();
        shard1.IsSplittingAsync().Returns(false);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(shard0);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/1").Returns(shard1);

        var split0 = Substitute.For<ITreeShardSplitGrain>();
        split0.SplitAsync(Arg.Any<int>()).Returns(Task.FromException(new InvalidOperationException("busy")));
        var split1 = Substitute.For<ITreeShardSplitGrain>();
        grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/0").Returns(split0);
        grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/1").Returns(split1);

        // Must not throw — dispatch errors are logged and retried next tick.
        Assert.DoesNotThrowAsync(() => grain.MigrateAsync());
    }

    // --- FinaliseAsync / IsCompleteAsync ---

    [Test]
    public async Task FinaliseAsync_clears_in_progress_and_marks_complete()
    {
        var (grain, state, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Complete;

        await grain.FinaliseAsync();

        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
        Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.None));
    }

    [Test]
    public async Task IsCompleteAsync_returns_true_when_no_reshard_in_progress()
    {
        var (grain, _, _, _) = CreateGrain();
        Assert.That(await grain.IsCompleteAsync(), Is.True);
    }

    [Test]
    public async Task IsCompleteAsync_returns_false_during_reshard()
    {
        var (grain, state, _, _) = CreateGrain();
        state.State.InProgress = true;

        Assert.That(await grain.IsCompleteAsync(), Is.False);
    }

    // --- ProcessNextPhaseAsync ---

    [Test]
    public async Task ProcessNextPhaseAsync_no_op_when_not_in_progress()
    {
        var (grain, state, _, _) = CreateGrain();
        var before = state.WriteCount;

        await grain.ProcessNextPhaseAsync();

        Assert.That(state.WriteCount, Is.EqualTo(before));
    }

    [Test]
    public async Task ProcessNextPhaseAsync_advances_planning_to_migrating()
    {
        var (grain, state, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Planning;

        await grain.ProcessNextPhaseAsync();

        Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.Migrating));
    }

    [Test]
    public async Task MigrateAsync_clamps_dispatch_to_shards_still_needed()
    {
        // 4 shards, target 5 → only 1 more shard needed even though all 4 are
        // eligible and the concurrency cap is 4.
        var map = ShardMap.CreateDefault(16, 4);
        var (grain, state, grainFactory, _) = CreateGrain(
            virtualShardCount: 16, physicalShardCount: 4,
            existingMap: map, maxConcurrentMigrations: 4);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 5;

        for (int i = 0; i < 4; i++)
        {
            var s = Substitute.For<IShardRootGrain>();
            s.IsSplittingAsync().Returns(false);
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(s);
        }
        var splits = new ITreeShardSplitGrain[4];
        for (int i = 0; i < 4; i++)
        {
            splits[i] = Substitute.For<ITreeShardSplitGrain>();
            grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/{i}").Returns(splits[i]);
        }

        await grain.MigrateAsync();

        var totalDispatches = 0;
        for (int i = 0; i < 4; i++)
            totalDispatches += splits[i].ReceivedCalls()
                .Count(c => c.GetMethodInfo().Name == nameof(ITreeShardSplitGrain.SplitAsync));

        Assert.That(totalDispatches, Is.EqualTo(1),
            "Dispatch budget must be clamped to (target - current - inFlight).");
    }

    [Test]
    public void ReshardAsync_refused_while_resize_in_flight()
    {
        // Bidirectional interlock: a grow reshard must refuse to start while
        // a resize is still migrating data across physical trees, since the
        // ShardMap mutations a reshard performs on the source would
        // invalidate the resize snapshot's per-slot routing.
        var (grain, _, grainFactory, _) = CreateGrain(virtualShardCount: 16, physicalShardCount: 2);
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.IsCompleteAsync().Returns(Task.FromResult(false));
        grainFactory.GetGrain<ITreeResizeGrain>(TreeId).Returns(resize);

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ReshardAsync(4));
    }

    [Test]
    public void ReshardAsync_validates_arguments_before_consulting_resize_interlock()
    {
        // Bad argument must throw ArgumentOutOfRangeException even when a
        // resize is in flight — caller feedback on bad input must not be
        // masked by the interlock check.
        var (grain, _, grainFactory, _) = CreateGrain(virtualShardCount: 16, physicalShardCount: 2);
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.IsCompleteAsync().Returns(Task.FromResult(false));
        grainFactory.GetGrain<ITreeResizeGrain>(TreeId).Returns(resize);

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ReshardAsync(1));
    }
}
