using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeDeletionGrainTests
{
    [Test]
    public async Task External_caller_cannot_abandon_a_reservation_and_internal_release_is_idempotent()
    {
        var context = Substitute.For<IGrainContext>();
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(LatticeInternalOriginEnforcementMarker))
            .Returns(new LatticeInternalOriginEnforcementMarker());
        context.ActivationServices.Returns(services);
        var (grain, state, _, _, _) = CreateGrain(grainContext: context);
        state.State.AliasOperationId = "control-plane-operation";

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() =>
            grain.EndAliasChangeAsync("control-plane-operation"));
        Assert.That(state.State.AliasOperationId, Is.EqualTo("control-plane-operation"));
        Assert.That(state.WriteCount, Is.Zero);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await grain.EndAliasChangeAsync("control-plane-operation");
            await grain.EndAliasChangeAsync("control-plane-operation");
        }
        Assert.That(state.State.AliasOperationId, Is.Null);
        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    private const string PhysicalTarget = "test-tree/resized/copy";

    private static ITreeDeletionGrain ConfigureAlias(IGrainFactory factory)
    {
        var registry = RegistryOf(factory);
        registry.ResolveAsync(TreeId).Returns(PhysicalTarget);
        registry.GetEntryAsync(PhysicalTarget).Returns(new TreeRegistryEntry { DerivedFrom = TreeId });
        registry.GetAliasesTargetingAsync(PhysicalTarget).Returns(new[] { TreeId });
        var target = Substitute.For<ITreeDeletionGrain>();
        target.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot { IsDeleted = true });
        factory.GetGrain<ITreeDeletionGrain>(PhysicalTarget).Returns(target);
        return target;
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task DeleteTree_legacy_retirement_does_not_hide_logical_deletion(bool purged)
    {
        var (grain, state, _, factory, _) = CreateGrain();
        state.State.IsDeleted = true;
        state.State.RetainsRegistryEntry = true;
        state.State.PurgeComplete = purged;
        var target = ConfigureAlias(factory);

        Assert.That(await grain.IsDeletedAsync(), Is.False);
        Assert.That((await grain.GetDeletionStatusAsync()).PurgeComplete, Is.False);
        await grain.DeleteTreeAsync();

        Assert.That(await grain.IsDeletedAsync(), Is.True);
        Assert.That(state.State.LogicalPhysicalTreeId, Is.EqualTo(PhysicalTarget));
        Assert.That(state.State.PurgeComplete, Is.EqualTo(purged));
        await target.Received(1).DeleteDelegatedAsync();
        await factory.GetGrain<IShardRootGrain>($"{TreeId}/0").DidNotReceive().MarkDeletedAsync();
    }

    [Test]
    public async Task Recover_logically_live_retirement_refuses_without_touching_shards_or_reminders()
    {
        var (grain, _, reminders, factory, _) = CreateGrain();
        await grain.DeleteRetiredPhysicalTreeAsync();
        reminders.ClearReceivedCalls();

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());

        await factory.GetGrain<IShardRootGrain>($"{TreeId}/0").DidNotReceive().UnmarkDeletedAsync();
        await reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        Assert.That(await grain.IsPhysicalDeletedAsync(), Is.True);
    }

    [Test]
    public async Task Recover_alias_uses_persisted_target_and_leaves_retirement_intact()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        await grain.DeleteRetiredPhysicalTreeAsync();
        var target = ConfigureAlias(factory);
        target.IsPhysicalDeletedAsync().Returns(true);
        await grain.DeleteTreeAsync();
        RegistryOf(factory).ResolveAsync(TreeId).Returns("another-target");

        await grain.RecoverAsync();

        await target.Received(1).RecoverPhysicalAsync();
        Assert.That(await grain.IsDeletedAsync(), Is.False);
        Assert.That(state.State.RetainsRegistryEntry, Is.True);
        Assert.That(state.State.IsDeleted, Is.True);
    }

    [Test]
    public async Task Purge_alias_drives_the_target_then_unregisters_the_logical_tree()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var target = ConfigureAlias(factory);
        await grain.DeleteTreeAsync();

        await grain.PurgeNowAsync();

        await target.Received(1).PurgePhysicalAsync();
        await RegistryOf(factory).Received(1).UnregisterAsync(TreeId);
        Assert.That(state.State.LogicalPurgeComplete, Is.True);
    }

    [Test]
    public async Task Delete_alias_persists_the_target_before_any_physical_side_effect()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var target = ConfigureAlias(factory);
        target.DeleteDelegatedAsync().Returns(_ =>
        {
            Assert.That(state.State.LogicalPhysicalTreeId, Is.EqualTo(PhysicalTarget));
            Assert.That(state.WriteCount, Is.GreaterThan(0));
            throw new InvalidOperationException("physical mark failed");
        });

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteTreeAsync());
        target.DeleteDelegatedAsync().Returns(Task.CompletedTask);
        await grain.DeleteTreeAsync();

        Assert.That(state.State.LogicalDeleteComplete, Is.True);
        await target.Received(2).DeleteDelegatedAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Delete_alias_refuses_independent_or_shared_target(bool shared)
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var target = ConfigureAlias(factory);
        if (shared)
            RegistryOf(factory).GetAliasesTargetingAsync(PhysicalTarget).Returns(new[] { TreeId, "other" });
        else
            RegistryOf(factory).GetEntryAsync(PhysicalTarget).Returns(new TreeRegistryEntry());

        var error = Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteTreeAsync());

        Assert.That(error!.Message, Does.Contain(PhysicalTarget));
        Assert.That(state.State.LogicalPhysicalTreeId, Is.Null);
        Assert.That(state.State.DeletePending, Is.False);
        await target.DidNotReceive().DeleteDelegatedAsync();
    }

    [Test]
    public async Task Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable()
    {
        var (grain, _, _, _, _) = CreateGrain();
        await grain.BeginAliasChangeAsync("restore-1");
        await grain.BeginAliasChangeAsync("restore-1");
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.BeginAliasChangeAsync("resize-2"));
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteTreeAsync());
        await grain.EndAliasChangeAsync("stale");
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteTreeAsync());
        await grain.EndAliasChangeAsync("restore-1");
        await grain.EndAliasChangeAsync("restore-1");
        await grain.BeginAliasChangeAsync("resize-2");
        await grain.EndAliasChangeAsync("restore-1");
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteTreeAsync());
        await grain.EndAliasChangeAsync("resize-2");
        await grain.DeleteTreeAsync();
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.BeginAliasChangeAsync("resize-2"));
    }

    [Test]
    public async Task Delete_pending_fences_alias_writes_during_controlled_registry_interleaving()
    {
        var (grain, _, _, factory, _) = CreateGrain();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<IReadOnlyList<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        RegistryOf(factory).GetAliasesTargetingAsync(TreeId).Returns(_ =>
        {
            entered.SetResult();
            return release.Task;
        });
        var deleting = grain.DeleteTreeAsync();
        await entered.Task;

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.EnsureAliasWritableAsync());
        release.SetResult(Array.Empty<string>());
        await deleting;
    }

    [Test]
    public async Task Delegated_delete_has_no_independent_purge_reminder()
    {
        var (grain, _, reminders, _, _) = CreateGrain();
        await grain.DeleteDelegatedAsync();
        await reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task Ordinary_delete_pins_self_before_mark_and_retry_does_not_resolve_a_new_alias()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var shard = factory.GetGrain<IShardRootGrain>($"{TreeId}/0");
        shard.MarkDeletedAsync().Returns(_ =>
        {
            Assert.That(state.State.LocalDeleteTargetPinned, Is.True);
            throw new InvalidOperationException("mark failed");
        });
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DeleteTreeAsync());
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.EnsureAliasWritableAsync());
        RegistryOf(factory).ResolveAsync(TreeId).Returns("unexpected-target");
        shard.MarkDeletedAsync().Returns(Task.CompletedTask);
        await grain.DeleteTreeAsync();
        await RegistryOf(factory).Received(1).ResolveAsync(TreeId);
        Assert.That(await grain.IsDeletedAsync(), Is.True);
    }

    [Test]
    public async Task Recovering_physical_work_clears_delegation_and_suppression_for_future_operations()
    {
        var (grain, state, _, _, _) = CreateGrain();
        await grain.DeleteDelegatedAsync();
        await grain.RecoverPhysicalAsync();
        Assert.That(state.State.Delegated, Is.False);
        Assert.That(state.State.SuppressLifecycleEvents, Is.False);
    }

    [Test]
    public async Task Purge_alias_retry_finishes_after_target_unregisters_itself()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var target = ConfigureAlias(factory);
        await grain.DeleteTreeAsync();
        var attempts = 0;
        RegistryOf(factory).UnregisterAsync(TreeId).Returns(_ =>
            ++attempts == 1 ? Task.FromException(new IOException("registry unavailable")) : Task.CompletedTask);
        Assert.ThrowsAsync<IOException>(() => grain.PurgeNowAsync());
        Assert.That(state.State.LogicalPurgeInProgress, Is.True);
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RecoverAsync());
        target.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot { IsDeleted = true, PurgeComplete = true });
        RegistryOf(factory).GetEntryAsync(PhysicalTarget).Returns((TreeRegistryEntry?)null);
        await grain.PurgeNowAsync();
        Assert.That(state.State.LogicalPurgeComplete, Is.True);
    }
}
