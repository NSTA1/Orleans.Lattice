using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4265. A purge persists its completion before
/// it removes the tree's registry entry. When that removal threw, nothing ever
/// retried it: a retry of the purge was refused as "already fully purged", the
/// reminders saw a completed purge and tore themselves down, and once the
/// activation deactivated the surviving registry entry made the id read as a
/// live, empty tree that kept the purged tree's settings. The unregister must
/// be recorded as owed and re-driven until it lands.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private static readonly InvalidOperationException UnregisterFault = new("simulated registry failure");

    /// <summary>
    /// Deletes and purges the tree with the registry's unregister failing, and
    /// returns the persisted state the failed purge left behind.
    /// </summary>
    private static async Task<FakePersistentState<TreeDeletionState>> PurgeWithFailingUnregisterAsync(
        bool reminderDriven)
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        var registry = RegistryOf(grainFactory);
        registry.ExistsAsync(TreeId).Returns(true);
        registry.UnregisterAsync(TreeId).ThrowsAsync(UnregisterFault);

        if (reminderDriven)
        {
            await grain.BeginPurgeStateAsync(startFromShard: 0);
            Assert.ThrowsAsync<InvalidOperationException>(() => grain.CompletePurgeAsync());
        }
        else
        {
            Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());
        }

        Assert.That(state.State.PurgeComplete, Is.True, "the walk's completion is durable");
        return state;
    }

    /// <summary>
    /// A fresh activation over the persisted state, whose registry still holds
    /// the purged tree's entry - the unregister never landed.
    /// </summary>
    private static (TreeDeletionGrain grain, IReminderRegistry reminderRegistry, ILatticeRegistry registry)
        Reactivate(FakePersistentState<TreeDeletionState> state)
    {
        var (grain, _, reminderRegistry, grainFactory, _) = CreateGrain(existingState: state);
        var registry = RegistryOf(grainFactory);
        registry.ExistsAsync(TreeId).Returns(true);
        return (grain, reminderRegistry, registry);
    }

    [Test]
    public async Task A_purge_whose_unregister_threw_is_not_reported_live_after_a_reactivation()
    {
        var state = await PurgeWithFailingUnregisterAsync(reminderDriven: false);
        var (grain, _, _) = Reactivate(state);

        var status = await grain.GetDeletionStatusAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.IsDeletedAsync(), Is.True, "the registry entry is owed a removal, not a reused id");
            Assert.That(status.IsDeleted, Is.True);
            Assert.That(status.PurgeInProgress, Is.True, "the purge has not finished until the entry is removed");
            Assert.That(status.PurgeComplete, Is.False);
            Assert.That(state.State.PurgeComplete, Is.True, "a read must not clear the record");
        });
    }

    [Test]
    public async Task PurgeNow_retried_on_the_same_activation_redrives_a_failed_unregister()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        var registry = RegistryOf(grainFactory);
        registry.ExistsAsync(TreeId).Returns(true);
        registry.UnregisterAsync(TreeId).ThrowsAsync(UnregisterFault);
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());
        registry.UnregisterAsync(TreeId).Returns(Task.CompletedTask);
        registry.ClearReceivedCalls();

        await grain.PurgeNowAsync();

        await registry.Received(1).UnregisterAsync(TreeId);
        Assert.That(state.State.PurgeComplete, Is.True);
        Assert.That(state.State.RegistryUnregisterPending, Is.False);
    }

    [Test]
    public async Task PurgeNow_after_a_reactivation_redrives_a_failed_unregister()
    {
        var state = await PurgeWithFailingUnregisterAsync(reminderDriven: false);
        var (grain, _, registry) = Reactivate(state);

        await grain.PurgeNowAsync();

        await registry.Received(1).UnregisterAsync(TreeId);
        Assert.Multiple(async () =>
        {
            Assert.That(state.State.IsDeleted, Is.True, "the record is the purged tree's, not cleared as a reused id");
            Assert.That(state.State.PurgeComplete, Is.True);
            Assert.That(state.State.RegistryUnregisterPending, Is.False);
            registry.ExistsAsync(TreeId).Returns(false);
            Assert.That((await grain.GetDeletionStatusAsync()).PurgeComplete, Is.True);
        });
    }

    [Test]
    public async Task BeginPurge_after_a_reactivation_redrives_a_failed_unregister()
    {
        var state = await PurgeWithFailingUnregisterAsync(reminderDriven: true);
        var (grain, reminderRegistry, registry) = Reactivate(state);

        await grain.BeginPurgeAsync();

        await registry.Received(1).UnregisterAsync(TreeId);
        Assert.That(state.State.PurgeComplete, Is.True);
        Assert.That(state.State.RegistryUnregisterPending, Is.False);
        await reminderRegistry.Received().GetReminder(Arg.Any<GrainId>(), "deletion-keepalive");
    }

    [Test]
    public async Task The_keepalive_reminder_redrives_a_failed_unregister()
    {
        var state = await PurgeWithFailingUnregisterAsync(reminderDriven: true);
        var (grain, reminderRegistry, registry) = Reactivate(state);

        await grain.ReceiveReminder("deletion-keepalive", default);

        await registry.Received(1).UnregisterAsync(TreeId);
        Assert.That(state.State.RegistryUnregisterPending, Is.False);
        await reminderRegistry.Received().GetReminder(Arg.Any<GrainId>(), "deletion-keepalive");
    }

    [Test]
    public async Task The_keepalive_reminder_keeps_retrying_while_the_unregister_keeps_failing()
    {
        var state = await PurgeWithFailingUnregisterAsync(reminderDriven: true);
        var (grain, reminderRegistry, registry) = Reactivate(state);
        registry.UnregisterAsync(TreeId).ThrowsAsync(UnregisterFault);

        Assert.DoesNotThrowAsync(() => grain.ReceiveReminder("deletion-keepalive", default));

        Assert.That(state.State.RegistryUnregisterPending, Is.True);
        await reminderRegistry.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task A_synchronous_purge_whose_unregister_threw_arms_the_keepalive_reminder()
    {
        var (grain, _, reminderRegistry, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        RegistryOf(grainFactory).UnregisterAsync(TreeId).ThrowsAsync(UnregisterFault);
        reminderRegistry.ClearReceivedCalls();

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());

        await reminderRegistry.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "deletion-keepalive", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task A_failed_write_clearing_the_owed_unregister_leaves_it_owed()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        var registry = RegistryOf(grainFactory);
        var failNextWrite = true;
        registry.UnregisterAsync(TreeId).Returns(_ =>
        {
            // Fails the write that clears the owed removal, once.
            if (failNextWrite) state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");
            failNextWrite = false;
            return Task.CompletedTask;
        });

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());

        Assert.That(state.State.RegistryUnregisterPending, Is.True);
        await grain.PurgeNowAsync();
        Assert.That(state.State.RegistryUnregisterPending, Is.False);
        await registry.Received(2).UnregisterAsync(TreeId);
    }

    [Test]
    public async Task A_purge_never_owes_an_unregister_once_it_has_landed()
    {
        var (grain, state, _, _, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.PurgeNowAsync();

        Assert.That(state.State.RegistryUnregisterPending, Is.False);
        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgePhysicalAsync());
        Assert.That(ex!.Message, Does.Contain("already been fully purged"));
    }
}
