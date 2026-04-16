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

public class TreeDeletionGrainTests
{
    private const string TreeId = "test-tree";
    private const int ShardCount = 2;

    private static (TreeDeletionGrain grain,
                     FakePersistentState<TreeDeletionState> state,
                     IReminderRegistry reminderRegistry,
                     IGrainFactory grainFactory,
                     IOptionsMonitor<LatticeOptions> optionsMonitor) CreateGrain(
        LatticeOptions? options = null,
        FakePersistentState<TreeDeletionState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("deletion", TreeId));
        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options ??= new LatticeOptions
        {
            ShardCount = ShardCount,
            SoftDeleteDuration = TimeSpan.FromHours(72),
        };
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);
        var state = existingState ?? new FakePersistentState<TreeDeletionState>();

        // Set up shard root mocks.
        for (int i = 0; i < options.ShardCount; i++)
        {
            var shardRoot = Substitute.For<IShardRootGrain>();
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}")
                .Returns(shardRoot);
            shardRoot.MarkDeletedAsync().Returns(Task.CompletedTask);
            shardRoot.IsDeletedAsync().Returns(Task.FromResult(false));
            shardRoot.PurgeAsync().Returns(Task.CompletedTask);
        }

        // Set up compaction grain mock.
        var compaction = Substitute.For<ITombstoneCompactionGrain>();
        grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId).Returns(compaction);

        var grain = new TreeDeletionGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, state);
        return (grain, state, reminderRegistry, grainFactory, optionsMonitor);
    }

    // --- DeleteTreeAsync ---

    [Fact]
    public async Task DeleteTree_marks_all_shards_as_deleted()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        await grain.DeleteTreeAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).MarkDeletedAsync();
        }
    }

    [Fact]
    public async Task DeleteTree_persists_deleted_state()
    {
        var (grain, state, _, _, _) = CreateGrain();

        await grain.DeleteTreeAsync();

        Assert.True(state.State.IsDeleted);
        Assert.NotNull(state.State.DeletedAtUtc);
    }

    [Fact]
    public async Task DeleteTree_registers_reminder()
    {
        var (grain, _, reminderRegistry, _, _) = CreateGrain();

        await grain.DeleteTreeAsync();

        await reminderRegistry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "tree-deletion",
            Arg.Any<TimeSpan>(),
            Arg.Any<TimeSpan>());
    }

    [Fact]
    public async Task DeleteTree_is_idempotent()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        await grain.DeleteTreeAsync();
        await grain.DeleteTreeAsync();

        // Shards only marked once.
        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).MarkDeletedAsync();
        }
    }

    // --- IsDeletedAsync ---

    [Fact]
    public async Task IsDeleted_returns_false_initially()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.False(await grain.IsDeletedAsync());
    }

    [Fact]
    public async Task IsDeleted_returns_true_after_delete()
    {
        var (grain, _, _, _, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        Assert.True(await grain.IsDeletedAsync());
    }

    // --- Purge ---

    [Fact]
    public async Task ProcessNextShard_purges_shards_sequentially()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        // Set up as if purge just started.
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
        await grain.BeginPurgeStateAsync(0);

        // Process each shard.
        await grain.ProcessNextShardAsync();
        Assert.Equal(1, state.State.NextShardIndex);

        await grain.ProcessNextShardAsync();
        Assert.Equal(2, state.State.NextShardIndex);

        // Verify all shards were purged.
        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).PurgeAsync();
        }
    }

    [Fact]
    public async Task ProcessNextShard_completes_purge_after_all_shards()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
        await grain.BeginPurgeStateAsync(0);

        // Process all shards.
        for (int i = 0; i < ShardCount; i++)
            await grain.ProcessNextShardAsync();

        // The next call should complete the purge.
        await grain.ProcessNextShardAsync();
        Assert.True(state.State.PurgeComplete);
        Assert.False(state.State.PurgeInProgress);
    }

    [Fact]
    public async Task ProcessNextShard_retries_failed_shard_once_then_skips()
    {
        var options = new LatticeOptions { ShardCount = ShardCount, SoftDeleteDuration = TimeSpan.FromHours(1) };
        var (grain, state, _, grainFactory, _) = CreateGrain(options);
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);

        // Make shard 0 fail.
        var failingShard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0");
        failingShard.PurgeAsync().ThrowsAsync(new Exception("Storage error"));

        await grain.BeginPurgeStateAsync(0);

        // First attempt — retry counter incremented.
        await grain.ProcessNextShardAsync();
        Assert.Equal(0, state.State.NextShardIndex);
        Assert.Equal(1, state.State.ShardRetries);

        // Second attempt — retries exhausted, skip to next shard.
        await grain.ProcessNextShardAsync();
        Assert.Equal(1, state.State.NextShardIndex);
        Assert.Equal(0, state.State.ShardRetries);
    }

    [Fact]
    public async Task Purge_resumes_from_persisted_shard_index()
    {
        var existingState = new FakePersistentState<TreeDeletionState>();
        existingState.State.IsDeleted = true;
        existingState.State.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
        existingState.State.PurgeInProgress = true;
        existingState.State.NextShardIndex = 1;

        var (grain, state, _, grainFactory, _) = CreateGrain(existingState: existingState);

        // Start purge from persisted index (shard 1).
        await grain.BeginPurgeStateAsync(state.State.NextShardIndex);
        await grain.ProcessNextShardAsync();

        // Only shard 1 should have been purged.
        var shard0 = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0");
        var shard1 = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/1");
        await shard0.DidNotReceive().PurgeAsync();
        await shard1.Received(1).PurgeAsync();
    }

    // --- RecoverAsync ---

    [Fact]
    public async Task Recover_throws_if_tree_not_deleted()
    {
        var (grain, _, _, _, _) = CreateGrain();
        await Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
    }

    [Fact]
    public async Task Recover_throws_if_purge_complete()
    {
        var existingState = new FakePersistentState<TreeDeletionState>();
        existingState.State.IsDeleted = true;
        existingState.State.PurgeComplete = true;

        var (grain, _, _, _, _) = CreateGrain(existingState: existingState);
        await Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
    }

    [Fact]
    public async Task Recover_throws_if_purge_in_progress()
    {
        var existingState = new FakePersistentState<TreeDeletionState>();
        existingState.State.IsDeleted = true;
        existingState.State.PurgeInProgress = true;

        var (grain, _, _, _, _) = CreateGrain(existingState: existingState);
        await Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
    }

    [Fact]
    public async Task Recover_unmarks_all_shards_and_clears_state()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.RecoverAsync();

        Assert.False(state.State.IsDeleted);
        Assert.Null(state.State.DeletedAtUtc);

        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).UnmarkDeletedAsync();
        }
    }

    [Fact]
    public async Task Recover_unregisters_reminders()
    {
        var (grain, _, reminderRegistry, _, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        // Set up reminder mocks for unregistration.
        var deletionReminder = Substitute.For<IGrainReminder>();
        var keepaliveReminder = Substitute.For<IGrainReminder>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "tree-deletion")
            .Returns(Task.FromResult<IGrainReminder?>(deletionReminder));
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "deletion-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(keepaliveReminder));

        await grain.RecoverAsync();

        await reminderRegistry.Received().UnregisterReminder(Arg.Any<GrainId>(), deletionReminder);
    }

    [Fact]
    public async Task DeleteTree_unregisters_compaction_reminder()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();

        await grain.DeleteTreeAsync();

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.Received(1).UnregisterReminderAsync();
    }

    [Fact]
    public async Task Recover_reinstates_compaction_reminder()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.RecoverAsync();

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.Received(1).EnsureReminderAsync();
    }

    [Fact]
    public async Task DeleteTree_compaction_unregister_failure_propagates()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        compaction.UnregisterReminderAsync().Returns(Task.FromException(new Exception("Compaction grain unavailable")));

        await Assert.ThrowsAsync<Exception>(() => grain.DeleteTreeAsync());
    }

    [Fact]
    public async Task Recover_compaction_reinstate_failure_propagates()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        compaction.EnsureReminderAsync().Returns(Task.FromException(new Exception("Compaction grain unavailable")));

        await Assert.ThrowsAsync<Exception>(() => grain.RecoverAsync());
    }

    // --- PurgeNowAsync ---

    [Fact]
    public async Task PurgeNow_throws_if_tree_not_deleted()
    {
        var (grain, _, _, _, _) = CreateGrain();
        await Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());
    }

    [Fact]
    public async Task PurgeNow_throws_if_already_purged()
    {
        var existingState = new FakePersistentState<TreeDeletionState>();
        existingState.State.IsDeleted = true;
        existingState.State.PurgeComplete = true;

        var (grain, _, _, _, _) = CreateGrain(existingState: existingState);
        await Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());
    }

    [Fact]
    public async Task PurgeNow_purges_all_shards_and_marks_complete()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.PurgeNowAsync();

        Assert.True(state.State.PurgeComplete);
        Assert.False(state.State.PurgeInProgress);

        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).PurgeAsync();
        }
    }
}
