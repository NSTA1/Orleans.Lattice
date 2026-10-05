using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #3940. A completed purge leaves the tree's
/// deletion record behind, and a later write, explicit create, or alias
/// assignment under the same id registers a new, live tree (a read never does -
/// issue #4219). The record used to go on describing that tree:
/// it read as deleted and purged, so a delete was a silent no-op, recovery and
/// purge were refused, and every alias change - a resize among them - was
/// refused, wedging the id for good. A purge is terminal, so once the id is
/// registered again the record must stop reporting it deleted, and must never
/// do so for a tree that is soft-deleted or whose purge is still running.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private static async Task<(TreeDeletionGrain grain,
                               FakePersistentState<TreeDeletionState> state,
                               IReminderRegistry reminderRegistry,
                               IGrainFactory grainFactory)> CreatePurgedAndReregisteredAsync()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        await grain.PurgeNowAsync();
        RegistryOf(grainFactory).ExistsAsync(TreeId).Returns(true);
        return (grain, state, reminderRegistry, grainFactory);
    }

    [Test]
    public async Task IsDeleted_reports_a_purged_tree_whose_id_was_registered_again_as_live()
    {
        var (grain, state, _, _) = await CreatePurgedAndReregisteredAsync();

        Assert.That(await grain.IsDeletedAsync(), Is.False);
        Assert.That(state.State.PurgeComplete, Is.True, "a read must not rewrite the record");
    }

    [Test]
    public async Task IsDeleted_keeps_reporting_a_purged_tree_deleted_while_its_id_is_unregistered()
    {
        var (grain, _, _, _, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        await grain.PurgeNowAsync();

        Assert.That(await grain.IsDeletedAsync(), Is.True);
        Assert.That((await grain.GetDeletionStatusAsync()).PurgeComplete, Is.True);
    }

    [Test]
    public async Task GetDeletionStatus_reports_a_purged_tree_whose_id_was_registered_again_as_live()
    {
        var (grain, _, _, _) = await CreatePurgedAndReregisteredAsync();

        var status = await grain.GetDeletionStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.IsDeleted, Is.False);
            Assert.That(status.PurgeComplete, Is.False);
            Assert.That(status.PurgeInProgress, Is.False);
            Assert.That(status.DeletedAtUtc, Is.Null);
            Assert.That(status.RecoveryDeadlineUtc, Is.Null);
        });
    }

    [Test]
    public async Task EnsureAliasWritable_admits_a_purged_tree_whose_id_was_registered_again()
    {
        var (grain, _, _, _) = await CreatePurgedAndReregisteredAsync();

        Assert.DoesNotThrowAsync(() => grain.EnsureAliasWritableAsync());
    }

    [Test]
    public async Task BeginAliasChange_clears_the_record_of_a_purged_tree_whose_id_was_registered_again()
    {
        var (grain, state, _, _) = await CreatePurgedAndReregisteredAsync();

        await grain.BeginAliasChangeAsync("resize:op");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.IsDeleted, Is.False);
            Assert.That(state.State.PurgeComplete, Is.False);
            Assert.That(state.State.LocalDeleteTargetPinned, Is.False);
            Assert.That(state.State.AliasOperationId, Is.EqualTo("resize:op"));
        });
    }

    [Test]
    public async Task DeleteTree_deletes_a_tree_created_again_under_a_purged_id()
    {
        var (grain, state, _, grainFactory) = await CreatePurgedAndReregisteredAsync();
        var firstDeletion = state.State.DeletedAtUtc;
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").ClearReceivedCalls();

        await grain.DeleteTreeAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(state.State.IsDeleted, Is.True);
            Assert.That(state.State.PurgeComplete, Is.False, "the new tree is soft-deleted, not purged");
            Assert.That(state.State.DeletedAtUtc, Is.Not.EqualTo(firstDeletion));
            Assert.That((await grain.GetDeletionStatusAsync()).CanRecover, Is.True);
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Received(1).MarkDeletedAsync();
        });
    }

    [Test]
    public async Task Recover_on_a_purged_tree_whose_id_was_registered_again_clears_the_record()
    {
        var (grain, state, reminderRegistry, _) = await CreatePurgedAndReregisteredAsync();
        reminderRegistry.ClearReceivedCalls();

        await grain.RecoverAsync();

        Assert.That(state.State.IsDeleted, Is.False);
        Assert.That(state.State.PurgeComplete, Is.False);
        await reminderRegistry.Received().GetReminder(Arg.Any<GrainId>(), "tree-deletion");
        // The record is gone, so the tree reads as the live tree it is and a
        // second recover is refused as for any tree that is not deleted.
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
    }

    [Test]
    public async Task PurgeNow_on_a_purged_tree_whose_id_was_registered_again_refuses_as_not_deleted()
    {
        var (grain, state, _, _) = await CreatePurgedAndReregisteredAsync();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());

        Assert.That(ex!.Message, Does.Contain("not been deleted"));
        Assert.That(state.State.PurgeComplete, Is.False);
    }

    [Test]
    public async Task DeleteRetiredPhysicalTree_retires_a_tree_created_again_under_a_purged_id()
    {
        var (grain, state, _, grainFactory) = await CreatePurgedAndReregisteredAsync();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").ClearReceivedCalls();

        await grain.DeleteRetiredPhysicalTreeAsync();

        Assert.That(state.State.IsDeleted, Is.True);
        Assert.That(state.State.RetainsRegistryEntry, Is.True);
        Assert.That(state.State.PurgeComplete, Is.False);
        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Received(1).MarkDeletedAsync();
    }

    [Test]
    public async Task A_logical_purge_record_is_cleared_once_its_id_is_registered_again()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.LogicalPhysicalTreeId = $"{TreeId}/resized/op";
        state.State.LogicalDeletedAtUtc = DateTimeOffset.UtcNow;
        state.State.LogicalDeleteComplete = true;
        state.State.LogicalPurgeComplete = true;
        state.State.IsDeleted = true;
        state.State.PurgeComplete = true;
        state.State.RetainsRegistryEntry = true;
        RegistryOf(grainFactory).ExistsAsync(TreeId).Returns(true);

        Assert.That(await grain.IsDeletedAsync(), Is.False);
        Assert.That((await grain.GetDeletionStatusAsync()).IsDeleted, Is.False);

        await grain.BeginAliasChangeAsync("resize:op2");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.LogicalPhysicalTreeId, Is.Null);
            Assert.That(state.State.LogicalPurgeComplete, Is.False);
            Assert.That(state.State.IsDeleted, Is.False);
            Assert.That(state.State.RetainsRegistryEntry, Is.False);
        });
    }

    [Test]
    public async Task A_soft_deleted_tree_is_never_reported_live_although_its_id_is_registered()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        RegistryOf(grainFactory).ExistsAsync(TreeId).Returns(true);

        Assert.That(await grain.IsDeletedAsync(), Is.True);
        Assert.That((await grain.GetDeletionStatusAsync()).CanRecover, Is.True);
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.EnsureAliasWritableAsync());
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.BeginAliasChangeAsync("resize:op"));
        Assert.That(state.State.IsDeleted, Is.True);
    }

    [Test]
    public async Task A_purge_in_progress_is_never_reported_live_although_its_id_is_registered()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        await grain.BeginPurgeStateAsync(startFromShard: 0);
        RegistryOf(grainFactory).ExistsAsync(TreeId).Returns(true);

        Assert.That(await grain.IsDeletedAsync(), Is.True);
        Assert.That((await grain.GetDeletionStatusAsync()).PurgeInProgress, Is.True);
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.BeginAliasChangeAsync("resize:op"));
        Assert.That(state.State.PurgeInProgress, Is.True);
    }

    [Test]
    public async Task A_purge_finalising_its_unregister_is_not_reported_live()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        var registry = RegistryOf(grainFactory);
        // The registry entry is still present while the purge removes it.
        registry.ExistsAsync(TreeId).Returns(true);
        bool? deletedDuringUnregister = null;
        registry.UnregisterAsync(TreeId).Returns(async _ =>
        {
            deletedDuringUnregister = await grain.IsDeletedAsync();
            registry.ExistsAsync(TreeId).Returns(false);
        });

        await grain.PurgeNowAsync();

        Assert.That(deletedDuringUnregister, Is.True);
    }

    [Test]
    public async Task A_completed_reminder_purge_finalising_its_unregister_is_not_reported_live()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        await grain.BeginPurgeStateAsync(startFromShard: 0);
        var registry = RegistryOf(grainFactory);
        registry.ExistsAsync(TreeId).Returns(true);
        bool? deletedDuringUnregister = null;
        registry.UnregisterAsync(TreeId).Returns(async _ =>
        {
            deletedDuringUnregister = await grain.IsDeletedAsync();
            registry.ExistsAsync(TreeId).Returns(false);
        });

        await grain.CompletePurgeAsync();

        Assert.That(deletedDuringUnregister, Is.True);
    }

    [Test]
    public async Task Clearing_a_reused_record_restores_it_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _) = await CreatePurgedAndReregisteredAsync();
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.BeginAliasChangeAsync("resize:op"));

        Assert.That(state.State.IsDeleted, Is.True);
        Assert.That(state.State.PurgeComplete, Is.True);
        Assert.That(state.State.AliasOperationId, Is.Null);
    }
}
