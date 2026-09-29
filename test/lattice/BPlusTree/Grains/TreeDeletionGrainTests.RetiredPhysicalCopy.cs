using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for retiring a physical tree whose id is also a live
/// logical tree - the original copy a tree's first resize replaces. The resize
/// used to soft-delete it with an ordinary delete, so the purge that follows
/// <see cref="LatticeOptions.SoftDeleteDuration"/> unregistered the logical
/// tree's registry entry - its alias to the resized copy and its sizing - and
/// the live tree became unreachable, and the delete also switched off the
/// tombstone compaction that serves the resized copy.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private static ILatticeRegistry RegistryOf(IGrainFactory grainFactory) =>
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [Test]
    public async Task DeleteRetiredPhysicalTree_marks_the_retired_shards_deleted_and_persists_the_retention()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();

        await grain.DeleteRetiredPhysicalTreeAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Received(1).MarkDeletedAsync();
        }
        Assert.That(state.State.IsDeleted, Is.True);
        Assert.That(state.State.RetainsRegistryEntry, Is.True);
        await reminderRegistry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "tree-deletion", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task DeleteRetiredPhysicalTree_keeps_the_tombstone_compaction_reminder()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();

        await grain.DeleteRetiredPhysicalTreeAsync();

        // The compaction grain under this id resolves the logical tree's alias
        // and compacts the live resized copy.
        await grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId)
            .DidNotReceive().UnregisterReminderAsync();
    }

    [Test]
    public async Task DeleteTree_does_not_retain_the_registry_entry()
    {
        var (grain, state, _, _, _) = CreateGrain();

        await grain.DeleteTreeAsync();

        Assert.That(state.State.RetainsRegistryEntry, Is.False);
    }

    [Test]
    public async Task PurgeNow_after_a_retirement_keeps_the_logical_registry_entry()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteRetiredPhysicalTreeAsync();

        await grain.PurgeNowAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Received(1).PurgeAsync();
        }
        Assert.That(state.State.PurgeComplete, Is.True);
        await RegistryOf(grainFactory).DidNotReceive().UnregisterAsync(Arg.Any<string>());
    }

    [Test]
    public async Task CompletePurge_after_a_retirement_keeps_the_logical_registry_entry()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteRetiredPhysicalTreeAsync();
        await grain.BeginPurgeStateAsync(startFromShard: 0);

        await grain.CompletePurgeAsync();

        Assert.That(state.State.PurgeComplete, Is.True);
        await RegistryOf(grainFactory).DidNotReceive().UnregisterAsync(Arg.Any<string>());
    }

    [Test]
    public async Task PurgeNow_after_an_ordinary_delete_unregisters_the_tree()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.PurgeNowAsync();

        await RegistryOf(grainFactory).Received(1).UnregisterAsync(TreeId);
    }

    [Test]
    public async Task CompletePurge_after_an_ordinary_delete_unregisters_the_tree()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        await grain.BeginPurgeStateAsync(startFromShard: 0);

        await grain.CompletePurgeAsync();

        await RegistryOf(grainFactory).Received(1).UnregisterAsync(TreeId);
    }

    [Test]
    public async Task Recover_after_a_retirement_clears_the_retention()
    {
        var (grain, state, _, _, _) = CreateGrain();
        await grain.DeleteRetiredPhysicalTreeAsync();

        await grain.RecoverAsync();

        Assert.That(state.State.IsDeleted, Is.False);
        Assert.That(state.State.RetainsRegistryEntry, Is.False);
    }

    [Test]
    public void DeleteRetiredPhysicalTree_reverts_the_retention_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteRetiredPhysicalTreeAsync());

        Assert.That(state.State.IsDeleted, Is.False);
        Assert.That(state.State.RetainsRegistryEntry, Is.False);
    }

    [Test]
    public async Task Recover_after_a_retirement_restores_the_retention_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _) = CreateGrain();
        await grain.DeleteRetiredPhysicalTreeAsync();
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());

        // A retry must still see a retirement, not an ordinary deletion whose
        // purge would unregister the live logical tree.
        Assert.That(state.State.IsDeleted, Is.True);
        Assert.That(state.State.RetainsRegistryEntry, Is.True);
    }
}
