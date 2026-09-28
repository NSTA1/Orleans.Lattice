using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeDeletionGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Self_resolution_after_retirement_refuses_pending_copy_or_starts_a_new_delete(bool purged)
    {
        var (grain, state, _, factory, _) = CreateGrain();
        state.State.IsDeleted = true;
        state.State.RetainsRegistryEntry = true;
        state.State.PurgeComplete = purged;
        if (!purged)
        {
            Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteTreeAsync());
            Assert.That(state.State.RetainsRegistryEntry, Is.True);
            await factory.GetGrain<IShardRootGrain>($"{TreeId}/0").DidNotReceive().MarkDeletedAsync();
            return;
        }
        await grain.DeleteTreeAsync();
        Assert.That(state.State.RetainsRegistryEntry, Is.False);
        Assert.That(state.State.PurgeComplete, Is.False);
        Assert.That(await grain.IsDeletedAsync(), Is.True);
        await factory.GetGrain<IShardRootGrain>($"{TreeId}/0").Received(1).MarkDeletedAsync();
    }

    [Test]
    public async Task Logical_delete_is_idempotent_and_recover_and_second_purge_refuse_after_completion()
    {
        var (grain, _, _, factory, _) = CreateGrain();
        var target = ConfigureAlias(factory);
        await grain.DeleteTreeAsync();
        await grain.DeleteTreeAsync();
        await target.Received(1).DeleteDelegatedAsync();
        await grain.PurgeNowAsync();
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
        await target.Received(1).PurgePhysicalAsync();
    }

    [Test]
    public async Task Logical_reminder_waits_for_the_window_then_retries_a_target_failure_on_the_next_tick()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var target = ConfigureAlias(factory);
        await grain.DeleteTreeAsync();
        await grain.ReceiveReminder("logical-tree-deletion", new TickStatus());
        await target.DidNotReceive().PurgePhysicalAsync();
        state.State.LogicalDeletedAtUtc = DateTimeOffset.UtcNow.AddDays(-10);
        target.PurgePhysicalAsync().ThrowsAsync(new IOException("target unavailable"));
        await grain.ReceiveReminder("logical-tree-deletion", new TickStatus());
        Assert.That(state.State.LogicalPurgeComplete, Is.False);
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
        target.PurgePhysicalAsync().Returns(Task.CompletedTask);
        await grain.ReceiveReminder("logical-tree-deletion", new TickStatus());
        Assert.That(state.State.LogicalPurgeComplete, Is.True);
        await target.Received(2).PurgePhysicalAsync();
    }

    [Test]
    public async Task Logical_reminder_unregisters_when_no_deletion_is_pending()
    {
        var (grain, _, reminders, _, _) = CreateGrain();
        var reminder = Substitute.For<IGrainReminder>();
        reminders.GetReminder(Arg.Any<GrainId>(), "logical-tree-deletion").Returns(reminder);
        await grain.ReceiveReminder("logical-tree-deletion", new TickStatus());
        await reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    [Test]
    public async Task Logical_purge_reclaims_a_pending_retirement_before_removing_its_registry_configuration()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        await grain.DeleteRetiredPhysicalTreeAsync();
        ConfigureAlias(factory);
        await grain.DeleteTreeAsync();
        RegistryOf(factory).UnregisterAsync(TreeId).Returns(_ =>
        {
            Assert.That(state.State.PurgeComplete, Is.True);
            return Task.CompletedTask;
        });
        await grain.PurgeNowAsync();
        for (var i = 0; i < ShardCount; i++)
            await factory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Received(1).PurgeAsync();
        Assert.That(state.State.LogicalPurgeComplete, Is.True);
    }

    [Test]
    public async Task Failed_logical_target_persistence_has_no_physical_side_effect()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var target = ConfigureAlias(factory);
        RegistryOf(factory).GetAliasesTargetingAsync(PhysicalTarget).Returns(_ =>
        {
            state.ThrowOnWrite = new IOException("target pin failed");
            return Task.FromResult<IReadOnlyList<string>>(new[] { TreeId });
        });
        Assert.ThrowsAsync<IOException>(() => grain.DeleteTreeAsync());
        Assert.That(state.State.LogicalPhysicalTreeId, Is.Null);
        Assert.That(await grain.IsDeletedAsync(), Is.False);
        await target.DidNotReceive().DeleteDelegatedAsync();
    }

    [Test]
    public async Task Recovery_of_partially_applied_delegated_delete_unmarks_every_shard()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var failedShard = factory.GetGrain<IShardRootGrain>($"{TreeId}/1");
        failedShard.MarkDeletedAsync().ThrowsAsync(new IOException("mark response lost"));
        Assert.ThrowsAsync<IOException>(() => grain.DeleteDelegatedAsync());
        Assert.That(state.State.IsDeleted, Is.False);
        Assert.That(await grain.IsPhysicalDeletedAsync(), Is.True);

        await grain.RecoverPhysicalAsync();

        for (var i = 0; i < ShardCount; i++)
        {
            var shard = factory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).UnmarkDeletedAsync();
            await shard.Received(1).ReseedNodeBindingsAsync();
        }
        Assert.That(await grain.IsPhysicalDeletedAsync(), Is.False);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Ambiguous_delegated_delete_can_be_recovered_or_purged(bool purge)
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var target = ConfigureAlias(factory);
        target.DeleteDelegatedAsync().ThrowsAsync(new IOException("response lost"));
        Assert.ThrowsAsync<IOException>(() => grain.DeleteTreeAsync());
        Assert.That(state.State.LogicalPhysicalTreeId, Is.EqualTo(PhysicalTarget));
        target.DeleteDelegatedAsync().Returns(Task.CompletedTask);
        target.IsPhysicalDeletedAsync().Returns(true);
        if (purge)
        {
            await grain.PurgeNowAsync();
            await target.Received(2).DeleteDelegatedAsync();
            await target.Received(1).PurgePhysicalAsync();
            Assert.That(state.State.LogicalPurgeComplete, Is.True);
        }
        else
        {
            await grain.RecoverAsync();
            await target.Received(1).RecoverPhysicalAsync();
            Assert.That(await grain.IsDeletedAsync(), Is.False);
        }
    }

    [Test]
    public async Task Failed_target_recovery_preserves_the_logical_deletion_for_retry()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var target = ConfigureAlias(factory);
        target.IsPhysicalDeletedAsync().Returns(true);
        await grain.DeleteTreeAsync();
        target.RecoverPhysicalAsync().ThrowsAsync(new IOException("recovery failed"));
        Assert.ThrowsAsync<IOException>(() => grain.RecoverAsync());
        Assert.That(state.State.LogicalPhysicalTreeId, Is.EqualTo(PhysicalTarget));
        Assert.That(await grain.IsDeletedAsync(), Is.True);
        target.RecoverPhysicalAsync().Returns(Task.CompletedTask);
        await grain.RecoverAsync();
        Assert.That(await grain.IsDeletedAsync(), Is.False);
    }
}
