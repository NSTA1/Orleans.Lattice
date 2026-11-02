using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the second remediation round of the WAL pin
/// retirement review (#5351): the deletion record a purged shard consults
/// before re-seeding (issue #5458), the logical purge's ownership check on a
/// re-driven completion (issue #5459), and the paths of #5417's fix that the
/// first detectors did not drive - the accepted purge's own completion
/// (<see cref="TreeDeletionGrain.CompletePurgeAsync"/>) and a delegated copy's
/// re-driven cleanup.
/// </summary>
public partial class TreeDeletionGrainTests
{
    // --- HoldsDeletionAsync (#5458) ---

    [Test]
    public async Task HoldsDeletion_is_false_for_a_live_tree()
    {
        var (grain, _, _, _, _) = CreateGrain();

        Assert.That(await grain.HoldsDeletionAsync(), Is.False);
    }

    [Test]
    public async Task HoldsDeletion_holds_a_soft_deleted_tree_and_a_purge_still_walking()
    {
        var (grain, state, _, _, _) = CreateGrain();
        await grain.DeleteTreeAsync();
        Assert.That(await grain.HoldsDeletionAsync(), Is.True, "soft deleted");

        await grain.BeginPurgeStateAsync(startFromShard: 0);

        Assert.That(state.State.PurgeInProgress, Is.True);
        Assert.That(await grain.HoldsDeletionAsync(), Is.True, "a purge still walking");
    }

    [Test]
    public async Task HoldsDeletion_holds_a_completed_purge_whose_registry_removal_is_owed()
    {
        var h = await PurgeWhoseRemovalLandedButStayedOwedAsync();

        Assert.That(await h.Grain.HoldsDeletionAsync(), Is.True,
            "the registry row still naming the id is the purged tree's own, so a write may not re-seed under it");
    }

    [Test]
    public async Task HoldsDeletion_releases_a_completed_purge_once_its_registry_row_is_removed()
    {
        var (grain, state, _, _, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        await grain.PurgeNowAsync();

        Assert.That(state.State.RegistryUnregisterPending, Is.False);
        Assert.That(await grain.HoldsDeletionAsync(), Is.False,
            "the row is gone: a write registering the id again is the deliberate reuse of #3940");
    }

    [Test]
    public async Task HoldsDeletion_holds_a_logical_deletion_until_its_purge_completes()
    {
        var h = CreateTimedGrain();
        var target = ConfigureAlias(h.Factory);
        await h.Grain.DeleteTreeAsync();
        await h.Grain.BeginPurgeAsync();
        Assert.That(await h.Grain.HoldsDeletionAsync(), Is.True, "a logical purge in flight");

        target.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot { IsDeleted = true, PurgeComplete = true });
        Assert.That(await h.Grain.TryCompleteLogicalPurgeAsync(), Is.True);

        Assert.That(await h.Grain.HoldsDeletionAsync(), Is.False);
    }

    // --- The logical purge's re-driven completion (#5459) ---

    private static readonly Guid LogicalPurgedLineage = new("54590000-0000-0000-0000-000000000001");
    private static readonly Guid LogicalReusedLineage = new("54590000-0000-0000-0000-000000000002");

    /// <summary>
    /// An accepted logical purge whose completion's registry removal lands but
    /// whose completion write fails, as the removal's reply or the write is lost.
    /// </summary>
    private static async Task<TimedHarness> LogicalPurgeWhoseRemovalLandedButNotItsCompletionAsync()
    {
        var h = CreateTimedGrain();
        var target = ConfigureAlias(h.Factory);
        var registry = RegistryOf(h.Factory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(LogicalPurgedLineage)));
        await h.Grain.DeleteTreeAsync();
        await h.Grain.BeginPurgeAsync();
        target.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot { IsDeleted = true, PurgeComplete = true });

        registry.UnregisterAsync(TreeId).Returns(_ =>
        {
            registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(null));
            h.State.ThrowOnWrite = new InvalidOperationException("simulated storage failure");
            return Task.CompletedTask;
        });
        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.TryCompleteLogicalPurgeAsync());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LogicalPurgeComplete, Is.False, "the completion did not reach storage");
            Assert.That(h.State.State.LogicalPurgeInProgress, Is.True);
            Assert.That(h.State.State.LogicalPurgedRegistryLineageRecorded, Is.True);
            Assert.That(h.State.State.LogicalPurgedRegistryLineage, Is.EqualTo(LogicalPurgedLineage));
        });
        registry.ClearReceivedCalls();
        return h;
    }

    [Test]
    public async Task A_redriven_logical_purge_completion_leaves_a_reused_logical_id_registered()
    {
        var h = await LogicalPurgeWhoseRemovalLandedButNotItsCompletionAsync();
        var registry = RegistryOf(h.Factory);

        // A write registers the logical id again: a new tree, a new lineage.
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(LogicalReusedLineage)));

        Assert.That(await h.Grain.TryCompleteLogicalPurgeAsync(), Is.True);

        await registry.DidNotReceive().UnregisterAsync(TreeId);
        Assert.That(h.State.State.LogicalPurgeComplete, Is.True);
    }

    [Test]
    public async Task A_redriven_logical_purge_completion_whose_removal_landed_removes_nothing_again()
    {
        var h = await LogicalPurgeWhoseRemovalLandedButNotItsCompletionAsync();
        var registry = RegistryOf(h.Factory);

        Assert.That(await h.Grain.TryCompleteLogicalPurgeAsync(), Is.True);

        await registry.DidNotReceive().UnregisterAsync(TreeId);
        Assert.That(h.State.State.LogicalPurgeComplete, Is.True);
    }

    [Test]
    public async Task A_redriven_logical_purge_completion_still_removes_its_own_entry()
    {
        // The control: the removal never landed, so the entry still carries the
        // recorded lineage and the re-drive removes it.
        var h = CreateTimedGrain();
        var target = ConfigureAlias(h.Factory);
        var registry = RegistryOf(h.Factory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(LogicalPurgedLineage)));
        await h.Grain.DeleteTreeAsync();
        await h.Grain.BeginPurgeAsync();
        target.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot { IsDeleted = true, PurgeComplete = true });
        registry.UnregisterAsync(TreeId).Returns(Task.FromException(new InvalidOperationException("registry down")));
        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.TryCompleteLogicalPurgeAsync());
        registry.UnregisterAsync(TreeId).Returns(Task.CompletedTask);
        registry.ClearReceivedCalls();

        Assert.That(await h.Grain.TryCompleteLogicalPurgeAsync(), Is.True);

        await registry.Received(1).UnregisterAsync(TreeId);
    }

    [Test]
    public async Task A_redriven_synchronous_logical_purge_leaves_a_reused_logical_id_registered()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        ConfigureAlias(factory);
        var registry = RegistryOf(factory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(LogicalPurgedLineage)));
        await grain.DeleteTreeAsync();
        registry.UnregisterAsync(TreeId).Returns(_ =>
        {
            state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");
            return Task.CompletedTask;
        });
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());
        Assert.That(state.State.LogicalPurgeComplete, Is.False, "precondition: the completion did not reach storage");
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(LogicalReusedLineage)));
        registry.ClearReceivedCalls();

        await grain.PurgeNowAsync();

        await registry.DidNotReceive().UnregisterAsync(TreeId);
        Assert.That(state.State.LogicalPurgeComplete, Is.True);
    }

    // --- #5417 on the accepted purge's own completion ---

    [Test]
    public async Task An_accepted_purge_whose_removal_landed_leaves_a_reused_tree_id_untouched()
    {
        // CompletePurgeAsync is the completion PurgeTreeAsync's walk and the
        // soft-delete reminder take, so it must record the lineage too.
        var h = CreateDiscardHarness(4, 7, 9);
        var registry = RegistryOf(h.GrainFactory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(PurgedLineage)));
        await h.Grain.DeleteTreeAsync();
        await h.Grain.BeginPurgeStateAsync(startFromShard: 0);
        registry.UnregisterAsync(TreeId).Returns(_ =>
        {
            registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(null));
            h.State.ThrowOnWrite = new InvalidOperationException("simulated storage failure");
            return Task.CompletedTask;
        });
        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.CompletePurgeAsync());
        Assert.That(h.State.State.RegistryUnregisterPending, Is.True, "precondition: the removal landed but its record did not");
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(ReusedLineage)));
        registry.ClearReceivedCalls();
        h.Wal.ClearReceivedCalls();
        h.Reporter.ClearReceivedCalls();

        await h.Grain.ReceiveReminder("deletion-keepalive", default);

        await h.Wal.DidNotReceive().TrimAsync(TreeId, Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
        await h.Reporter.DidNotReceive().UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
        await registry.DidNotReceive().UnregisterAsync(TreeId);
    }

    // --- A delegated copy's re-driven cleanup ---

    private static FakePersistentState<TreeDeletionState> DelegatedPurgeOwingItsRemoval(Guid recordedLineage) =>
        new()
        {
            State = new TreeDeletionState
            {
                IsDeleted = true,
                DeletedAtUtc = DateTimeOffset.UtcNow,
                Delegated = true,
                PurgeComplete = true,
                RegistryUnregisterPending = true,
                PurgedRegistryLineageRecorded = true,
                PurgedRegistryLineage = recordedLineage,
            },
        };

    [Test]
    public async Task A_delegated_copy_s_redriven_cleanup_leaves_a_reused_id_untouched()
    {
        var (grain, state, reporter, factory) = CreateGrainWithReporter(
            existingState: DelegatedPurgeOwingItsRemoval(PurgedLineage));
        var registry = RegistryOf(factory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(ReusedLineage)));

        await grain.BeginPurgeAsync();

        await reporter.DidNotReceive().UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
        await registry.DidNotReceive().UnregisterAsync(TreeId);
        Assert.That(state.State.RegistryUnregisterPending, Is.False, "the landed removal is recorded");
    }

    [Test]
    public async Task A_delegated_copy_s_redriven_cleanup_clears_the_pin_store_before_the_registry_frees_the_id()
    {
        var (grain, _, reporter, factory) = CreateGrainWithReporter(
            existingState: DelegatedPurgeOwingItsRemoval(PurgedLineage));
        RegistryOf(factory).GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(PurgedLineage)));
        var order = RecordPurgeTailOrder(reporter, factory);

        await grain.BeginPurgeAsync();

        Assert.That(order, Is.EqualTo(new[] { "pins", "registry" }));
    }
}
