using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class TombstoneCompactionGrainTests
{
    private const string TreeId = "test-tree";
    private const int ShardCount = 2;

    private static (TombstoneCompactionGrain grain,
                     FakePersistentState<TombstoneCompactionState> state,
                     IReminderRegistry reminderRegistry,
                     IGrainFactory grainFactory,
                     IOptionsMonitor<LatticeOptions> optionsMonitor) CreateGrain(
        LatticeOptions? options = null,
        FakePersistentState<TombstoneCompactionState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("compaction", TreeId));
        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options ??= new LatticeOptions
        {
            ShardCount = ShardCount,
            TombstoneGracePeriod = TimeSpan.FromHours(24)
        };
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);
        var state = existingState ?? new FakePersistentState<TombstoneCompactionState>();

        var grain = new TombstoneCompactionGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, new LoggerFactory().CreateLogger<TombstoneCompactionGrain>(), state);
        return (grain, state, reminderRegistry, grainFactory, optionsMonitor);
    }

    private static void SetupShardWithLeaves(
        IGrainFactory grainFactory,
        int shardIndex,
        params GrainId[] leafIds)
    {
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{shardIndex}")
            .Returns(shardRoot);

        if (leafIds.Length == 0)
        {
            shardRoot.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
            return;
        }

        shardRoot.GetLeftmostLeafIdAsync()
            .Returns(Task.FromResult<GrainId?>(leafIds[0]));

        for (int i = 0; i < leafIds.Length; i++)
        {
            var leafMock = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leafMock);
            leafMock.CompactTombstonesAsync(Arg.Any<TimeSpan>()).Returns(Task.FromResult(0));

            var nextId = i + 1 < leafIds.Length ? (GrainId?)leafIds[i + 1] : null;
            leafMock.GetNextSiblingAsync().Returns(Task.FromResult(nextId));
        }
    }

    // --- EnsureReminderAsync ---

    [Fact]
    public async Task EnsureReminder_registers_reminder_with_clamped_period()
    {
        var (grain, _, reminderRegistry, _, _) = CreateGrain();

        await grain.EnsureReminderAsync();

        await reminderRegistry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "tombstone-compaction",
            Arg.Any<TimeSpan>(),
            Arg.Any<TimeSpan>());
    }

    [Fact]
    public async Task EnsureReminder_clamps_period_to_one_minute_minimum()
    {
        var options = new LatticeOptions
        {
            ShardCount = ShardCount,
            TombstoneGracePeriod = TimeSpan.FromSeconds(10)
        };
        var (grain, _, reminderRegistry, _, _) = CreateGrain(options);

        await grain.EnsureReminderAsync();

        await reminderRegistry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "tombstone-compaction",
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(1));
    }

    [Fact]
    public async Task EnsureReminder_skips_when_compaction_disabled()
    {
        var options = new LatticeOptions
        {
            ShardCount = ShardCount,
            TombstoneGracePeriod = Timeout.InfiniteTimeSpan
        };
        var (grain, _, reminderRegistry, _, _) = CreateGrain(options);

        await grain.EnsureReminderAsync();

        await reminderRegistry.DidNotReceiveWithAnyArgs()
            .RegisterOrUpdateReminder(default, default!, default, default);
    }

    // --- BeginCompactionStateAsync ---

    [Fact]
    public async Task BeginCompaction_persists_InProgress_and_shard_index()
    {
        var (grain, state, _, _, _) = CreateGrain();

        await grain.BeginCompactionStateAsync(startFromShard: 3);

        Assert.True(state.State.InProgress);
        Assert.Equal(3, state.State.NextShardIndex);
        Assert.Equal(0, state.State.ShardRetries);
    }

    [Fact]
    public async Task BeginCompaction_registers_keepalive_reminder()
    {
        var (grain, _, reminderRegistry, _, _) = CreateGrain();

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        await reminderRegistry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "compaction-keepalive",
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(1));
    }

    // --- ProcessNextShardAsync ---

    [Fact]
    public async Task ProcessNextShard_compacts_leaves_and_advances_index()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leafId);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        Assert.Equal(1, state.State.NextShardIndex);
        Assert.Equal(0, state.State.ShardRetries);

        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        await leaf.Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }

    [Fact]
    public async Task ProcessNextShard_walks_entire_leaf_chain()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf2 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leaf0, leaf1, leaf2);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf1).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf2).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }

    [Fact]
    public async Task ProcessNextShard_skips_empty_shard()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupShardWithLeaves(grainFactory, 0); // no leaves

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        Assert.Equal(1, state.State.NextShardIndex);
    }

    [Fact]
    public async Task ProcessNextShard_retries_on_first_failure()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(shardRoot);
        shardRoot.GetLeftmostLeafIdAsync().Throws(new InvalidOperationException("transient"));

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        // Should have incremented retries, not shard index.
        Assert.Equal(0, state.State.NextShardIndex);
        Assert.Equal(1, state.State.ShardRetries);
    }

    [Fact]
    public async Task ProcessNextShard_skips_shard_after_exhausting_retries()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(shardRoot);
        shardRoot.GetLeftmostLeafIdAsync().Throws(new InvalidOperationException("persistent"));

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // First call: retry.
        await grain.ProcessNextShardAsync();
        Assert.Equal(0, state.State.NextShardIndex);
        Assert.Equal(1, state.State.ShardRetries);

        // Second call: skip.
        await grain.ProcessNextShardAsync();
        Assert.Equal(1, state.State.NextShardIndex);
        Assert.Equal(0, state.State.ShardRetries);
    }

    [Fact]
    public async Task ProcessNextShard_processes_shards_sequentially()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leaf0);
        SetupShardWithLeaves(grainFactory, 1, leaf1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        await grain.ProcessNextShardAsync(); // shard 0
        Assert.Equal(1, state.State.NextShardIndex);

        await grain.ProcessNextShardAsync(); // shard 1
        Assert.Equal(2, state.State.NextShardIndex);
    }

    // --- CompleteCompactionAsync ---

    [Fact]
    public async Task CompleteCompaction_resets_state()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        await grain.BeginCompactionStateAsync(startFromShard: 5);
        await grain.CompleteCompactionAsync();

        Assert.False(state.State.InProgress);
        Assert.Equal(0, state.State.NextShardIndex);
        Assert.Equal(0, state.State.ShardRetries);
    }

    [Fact]
    public async Task CompleteCompaction_unregisters_keepalive_reminder()
    {
        var (grain, _, reminderRegistry, _, _) = CreateGrain();
        var mockReminder = Substitute.For<IGrainReminder>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Returns(Task.FromResult(mockReminder));

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.CompleteCompactionAsync();

        await reminderRegistry.Received(1)
            .UnregisterReminder(Arg.Any<GrainId>(), mockReminder);
    }

    [Fact]
    public async Task CompleteCompaction_tolerates_missing_keepalive_reminder()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Returns(Task.FromResult<IGrainReminder>(null!));

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.CompleteCompactionAsync();

        // Should not throw; state should still be cleaned up.
        Assert.False(state.State.InProgress);
    }

    [Fact]
    public async Task CompleteCompaction_tolerates_reminder_unregister_failure()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Throws(new InvalidOperationException("reminder service unavailable"));

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.CompleteCompactionAsync();

        // Should not throw; state should still be cleaned up.
        Assert.False(state.State.InProgress);
    }

    // --- Full pass (begin → process all shards → complete) ---

    [Fact]
    public async Task Full_pass_compacts_all_shards_and_completes()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leaf0);
        SetupShardWithLeaves(grainFactory, 1, leaf1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // Simulate timer ticks.
        await grain.ProcessNextShardAsync(); // shard 0
        await grain.ProcessNextShardAsync(); // shard 1
        await grain.ProcessNextShardAsync(); // triggers completion

        Assert.False(state.State.InProgress);
        Assert.Equal(0, state.State.NextShardIndex);
    }

    // --- Recovery (keepalive resume) ---

    [Fact]
    public async Task Recovery_resumes_from_persisted_shard_index()
    {
        // Simulate a grain that was mid-compaction when silo restarted.
        var existingState = new FakePersistentState<TombstoneCompactionState>();
        existingState.State.InProgress = true;
        existingState.State.NextShardIndex = 1; // shard 0 already done
        existingState.State.ShardRetries = 0;

        var (grain, state, reminderRegistry, grainFactory, _) =
            CreateGrain(existingState: existingState);
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 1, leaf1);
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        // Simulate: the keepalive fires, grain detects InProgress, resumes.
        await grain.BeginCompactionStateAsync(startFromShard: state.State.NextShardIndex);

        Assert.Equal(1, state.State.NextShardIndex); // still at shard 1

        await grain.ProcessNextShardAsync(); // process shard 1
        Assert.Equal(2, state.State.NextShardIndex);

        await grain.ProcessNextShardAsync(); // all done → complete
        Assert.False(state.State.InProgress);
    }

    // --- Passes grace period to leaves ---

    [Fact]
    public async Task ProcessNextShard_passes_configured_grace_period_to_leaves()
    {
        var gracePeriod = TimeSpan.FromHours(12);
        var options = new LatticeOptions
        {
            ShardCount = ShardCount,
            TombstoneGracePeriod = gracePeriod
        };
        var (grain, _, _, grainFactory, _) = CreateGrain(options);
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leafId);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        await grainFactory.GetGrain<IBPlusLeafGrain>(leafId).Received(1)
            .CompactTombstonesAsync(gracePeriod);
    }
}
