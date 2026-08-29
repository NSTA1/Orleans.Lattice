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

public partial class TreeDeletionGrainTests
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
            SoftDeleteDuration = TimeSpan.FromHours(72),
        };
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);
        var state = existingState ?? new FakePersistentState<TreeDeletionState>();

        // Set up shard root mocks.
        for (int i = 0; i < ShardCount; i++)
        {
            var shardRoot = Substitute.For<IShardRootGrain>();
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}")
                .Returns(shardRoot);
            shardRoot.MarkDeletedAsync().Returns(Task.CompletedTask);
            shardRoot.IsDeletedAsync().Returns(Task.FromResult(false));
            shardRoot.PurgeAsync().Returns(Task.CompletedTask);
            shardRoot.ReseedNodeBindingsAsync().Returns(Task.CompletedTask);
        }

        // Set up compaction grain mock.
        var compaction = Substitute.For<ITombstoneCompactionGrain>();
        grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId).Returns(compaction);

        // Set up registry grain mock.
        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
            }));
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);

        var grain = new TreeDeletionGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<TreeDeletionGrain>(), state);
        return (grain, state, reminderRegistry, grainFactory, optionsMonitor);
    }

    // --- DeleteTreeAsync ---

    [Test]
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

    [Test]
    public async Task DeleteTree_persists_deleted_state()
    {
        var (grain, state, _, _, _) = CreateGrain();

        await grain.DeleteTreeAsync();

        Assert.That(state.State.IsDeleted, Is.True);
        Assert.That(state.State.DeletedAtUtc, Is.Not.Null);
    }

    [Test]
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

    [Test]
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

    [Test]
    public async Task IsDeleted_returns_false_initially()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.That(await grain.IsDeletedAsync(), Is.False);
    }

    [Test]
    public async Task IsDeleted_returns_true_after_delete()
    {
        var (grain, _, _, _, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        Assert.That(await grain.IsDeletedAsync(), Is.True);
    }

    // --- GetDeletionStatusAsync ---

    [Test]
    public async Task GetDeletionStatus_reports_a_live_tree_with_no_deadline()
    {
        var (grain, _, _, _, _) = CreateGrain();

        var status = await grain.GetDeletionStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.IsDeleted, Is.False);
            Assert.That(status.DeletedAtUtc, Is.Null);
            Assert.That(status.RecoveryDeadlineUtc, Is.Null);
            Assert.That(status.PurgeInProgress, Is.False);
            Assert.That(status.PurgeComplete, Is.False);
            Assert.That(status.CanRecover, Is.False);
        });
    }

    [Test]
    public async Task GetDeletionStatus_derives_the_recovery_deadline_from_the_soft_delete_duration()
    {
        var options = new LatticeOptions { SoftDeleteDuration = TimeSpan.FromHours(72) };
        var (grain, _, _, _, _) = CreateGrain(options);

        await grain.DeleteTreeAsync();
        var status = await grain.GetDeletionStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.IsDeleted, Is.True);
            Assert.That(status.DeletedAtUtc, Is.Not.Null);
            Assert.That(status.RecoveryDeadlineUtc, Is.EqualTo(status.DeletedAtUtc!.Value + TimeSpan.FromHours(72)));
            Assert.That(status.CanRecover, Is.True);
        });
    }

    [Test]
    public async Task GetDeletionStatus_reports_purge_complete_and_denies_recovery()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
        state.State.PurgeComplete = true;

        var status = await grain.GetDeletionStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.IsDeleted, Is.True);
            Assert.That(status.PurgeComplete, Is.True);
            Assert.That(status.CanRecover, Is.False);
        });
    }

    [Test]
    public async Task GetDeletionStatus_reports_purge_in_progress_and_denies_recovery()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
        state.State.PurgeInProgress = true;

        var status = await grain.GetDeletionStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.PurgeInProgress, Is.True);
            Assert.That(status.CanRecover, Is.False);
        });
    }

    [Test]
    public async Task GetDeletionStatus_does_not_require_an_internal_origin_marker()
    {
        // A pure read must not assert internal origin (mirrors IsDeletedAsync), so it
        // never throws for a direct call, unlike the mutating verbs.
        var (grain, _, _, _, _) = CreateGrain();
        Assert.That(async () => await grain.GetDeletionStatusAsync(), Throws.Nothing);
    }

    // --- Purge ---

    [Test]
    public async Task ProcessNextShard_purges_shards_sequentially()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        // Set up as if purge just started.
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
        await grain.BeginPurgeStateAsync(0);

        // Process each shard.
        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));

        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(2));

        // Verify all shards were purged.
        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).PurgeAsync();
        }
    }

    [Test]
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
        Assert.That(state.State.PurgeComplete, Is.True);
        Assert.That(state.State.PurgeInProgress, Is.False);
    }

    [Test]
    public async Task ProcessNextShard_retries_failed_shard_once_then_skips()
    {
        var options = new LatticeOptions { SoftDeleteDuration = TimeSpan.FromHours(1) };
        var (grain, state, _, grainFactory, _) = CreateGrain(options);
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);

        // Make shard 0 fail.
        var failingShard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0");
        failingShard.PurgeAsync().ThrowsAsync(new Exception("Storage error"));

        await grain.BeginPurgeStateAsync(0);

        // First attempt - retry counter incremented.
        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
        Assert.That(state.State.ShardRetries, Is.EqualTo(1));

        // Second attempt - retries exhausted, skip to next shard.
        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
        Assert.That(state.State.ShardRetries, Is.EqualTo(0));
    }

    [Test]
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

    [Test]
    public async Task Recover_throws_if_tree_not_deleted()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
    }

    [Test]
    public async Task Recover_throws_if_purge_complete()
    {
        var existingState = new FakePersistentState<TreeDeletionState>();
        existingState.State.IsDeleted = true;
        existingState.State.PurgeComplete = true;

        var (grain, _, _, _, _) = CreateGrain(existingState: existingState);
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
    }

    [Test]
    public async Task Recover_throws_if_purge_in_progress()
    {
        var existingState = new FakePersistentState<TreeDeletionState>();
        existingState.State.IsDeleted = true;
        existingState.State.PurgeInProgress = true;

        var (grain, _, _, _, _) = CreateGrain(existingState: existingState);
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
    }

    [Test]
    public async Task Recover_unmarks_all_shards_and_clears_state()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.RecoverAsync();

        Assert.That(state.State.IsDeleted, Is.False);
        Assert.That(state.State.DeletedAtUtc, Is.Null);

        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).UnmarkDeletedAsync();
        }
    }

    [Test]
    public async Task Recover_reseeds_node_bindings_on_all_shards()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.RecoverAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).ReseedNodeBindingsAsync();
        }
    }

    [Test]
    public async Task Recover_reseed_failure_propagates()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0");
        shard.ReseedNodeBindingsAsync().Returns(Task.FromException(new Exception("Shard root unavailable")));

        var ex = Assert.ThrowsAsync<Exception>(() => grain.RecoverAsync());
        Assert.That(ex!.Message, Is.EqualTo("Shard root unavailable"));
    }

    [Test]
    public async Task Recover_reseed_failure_leaves_the_tree_deleted_so_a_retry_is_clean()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0");
        shard.ReseedNodeBindingsAsync().Returns(Task.FromException(new Exception("Shard root unavailable")));

        Assert.ThrowsAsync<Exception>(() => grain.RecoverAsync());

        // The re-seed runs before the IsDeleted write, so a failed repair does
        // not strand the tree: RecoverAsync's own precondition still passes on
        // the operator's retry rather than throwing "not been deleted".
        Assert.That(state.State.IsDeleted, Is.True);
        Assert.That(state.State.DeletedAtUtc, Is.Not.Null);

        shard.ReseedNodeBindingsAsync().Returns(Task.CompletedTask);
        await grain.RecoverAsync();
        Assert.That(state.State.IsDeleted, Is.False);
    }

    [Test]
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

    [Test]
    public async Task DeleteTree_unregisters_compaction_reminder()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();

        await grain.DeleteTreeAsync();

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.Received(1).UnregisterReminderAsync();
    }

    [Test]
    public async Task Recover_reinstates_compaction_reminder()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.RecoverAsync();

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.Received(1).EnsureReminderAsync();
    }

    [Test]
    public async Task DeleteTree_compaction_unregister_failure_propagates()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        compaction.UnregisterReminderAsync().Returns(Task.FromException(new Exception("Compaction grain unavailable")));

        Assert.ThrowsAsync<Exception>(() => grain.DeleteTreeAsync());
    }

    [Test]
    public async Task Recover_compaction_reinstate_failure_propagates()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        compaction.EnsureReminderAsync().Returns(Task.FromException(new Exception("Compaction grain unavailable")));

        Assert.ThrowsAsync<Exception>(() => grain.RecoverAsync());
    }

    // --- PurgeNowAsync ---

    [Test]
    public async Task PurgeNow_throws_if_tree_not_deleted()
    {
        var (grain, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());
    }

    [Test]
    public async Task PurgeNow_throws_if_already_purged()
    {
        var existingState = new FakePersistentState<TreeDeletionState>();
        existingState.State.IsDeleted = true;
        existingState.State.PurgeComplete = true;

        var (grain, _, _, _, _) = CreateGrain(existingState: existingState);
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());
    }

    [Test]
    public async Task PurgeNow_purges_all_shards_and_marks_complete()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.PurgeNowAsync();

        Assert.That(state.State.PurgeComplete, Is.True);
        Assert.That(state.State.PurgeInProgress, Is.False);

        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).PurgeAsync();
        }
    }
}
