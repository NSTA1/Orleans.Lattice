using Microsoft.Extensions.DependencyInjection;
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
/// Regression coverage for issue 3923: the undo is accept-then-poll. The
/// interleaved <see cref="TreeResizeGrain.RequestUndoAsync"/> persists the intent
/// in its own slot and returns without touching the coordinator's phase state,
/// and the phase loop runs the existing phase-aware unwind at its next boundary -
/// including the boundary at the end of a snapshot slice during which the undo
/// was requested.
/// </summary>
public partial class TreeResizeGrainTests
{
    private sealed record UndoHarness(
        TreeResizeGrain Grain,
        FakePersistentState<TreeResizeState> State,
        FakePersistentState<TreeResizeUndoState> Intent,
        IReminderRegistry Reminders,
        IGrainFactory GrainFactory,
        ITimerRegistry Timers);

    /// <summary>
    /// A grain whose activation services carry a timer registry, so the request
    /// path can arm the phase loop exactly as it does on a live silo.
    /// </summary>
    private static UndoHarness CreateUndoHarness(ResizePhase? phase = ResizePhase.Snapshot)
    {
        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());
        var services = new ServiceCollection();
        services.AddSingleton(timers);

        var intent = new FakePersistentState<TreeResizeUndoState>();
        var (grain, state, reminders, grainFactory, _) = CreateGrain(
            activationServices: services.BuildServiceProvider(), undoState: intent);
        SetupKeepalive(reminders);
        if (phase is { } p) SeedInFlightResize(state, p);
        return new UndoHarness(grain, state, intent, reminders, grainFactory, timers);
    }

    private static int TimerRegistrations(ITimerRegistry timers) =>
        timers.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));

    // --- Admission: the request records intent and nothing else ---

    [Test]
    public async Task RequestUndo_persists_the_intent_in_its_own_slot_and_never_writes_phase_state()
    {
        var h = CreateUndoHarness();

        var operationId = await h.Grain.RequestUndoAsync();

        Assert.Multiple(() =>
        {
            Assert.That(operationId, Is.EqualTo(UndoSnapshotSuffix));
            Assert.That(h.Intent.State.RequestedOperationId, Is.EqualTo(UndoSnapshotSuffix));
            Assert.That(h.Intent.State.RequestedAtUtc, Is.Not.Null);
            Assert.That(h.Intent.WriteCount, Is.EqualTo(1));
            Assert.That(h.State.WriteCount, Is.Zero,
                "the request runs interleaved with a phase turn that may be mid-way through mutating "
                + "TreeResizeState; writing that row from here could persist a half-applied transition");
            Assert.That(h.State.State.InProgress, Is.True, "the unwind is the phase loop's job, not the request's");
            Assert.That(h.State.State.Phase, Is.EqualTo(ResizePhase.Snapshot));
        });
        await h.GrainFactory.GetGrain<ITreeSnapshotGrain>(TreeId).DidNotReceive().AbortAsync(Arg.Any<string>());
    }

    [Test]
    public async Task RequestUndo_arms_the_phase_loop_and_its_keepalive()
    {
        var h = CreateUndoHarness();

        await h.Grain.RequestUndoAsync();

        Assert.That(TimerRegistrations(h.Timers), Is.EqualTo(1));
        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "resize-keepalive", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task RequestUndo_is_acknowledged_again_while_the_undo_is_pending()
    {
        var h = CreateUndoHarness();

        var first = await h.Grain.RequestUndoAsync();
        var second = await h.Grain.RequestUndoAsync();

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.EqualTo(first));
            Assert.That(h.Intent.WriteCount, Is.EqualTo(1), "a retry must not rewrite an intent already recorded");
        });
        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public void RequestUndo_throws_when_no_resize_exists()
    {
        var h = CreateUndoHarness(phase: null);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.RequestUndoAsync());

        Assert.That(ex!.Message, Is.EqualTo($"No resize exists for tree '{TreeId}' that can be undone."));
        Assert.That(h.Intent.WriteCount, Is.Zero);
    }

    [Test]
    public void RequestUndo_throws_when_the_resize_state_is_incomplete()
    {
        var h = CreateUndoHarness();
        h.State.State.SnapshotTreeId = null;

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.RequestUndoAsync());

        Assert.That(ex!.Message, Does.Contain("is incomplete"));
        Assert.That(h.Intent.WriteCount, Is.Zero);
    }

    [Test]
    public void A_failed_intent_write_is_rolled_back_so_the_undo_is_not_reported_as_accepted()
    {
        var h = CreateUndoHarness();
        h.Intent.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.RequestUndoAsync());

        Assert.That(h.Intent.State.RequestedOperationId, Is.Null);
    }

    [Test]
    public async Task A_retry_after_a_completed_undo_names_the_undo_that_already_succeeded()
    {
        var h = CreateUndoHarness(ResizePhase.Swap);
        SetupOldTreeDeletion(h.GrainFactory, isDeleted: false);
        await h.Grain.RequestUndoAsync();
        await h.Grain.ProcessNextPhaseAsync();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.RequestUndoAsync());

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.StartWith($"No resize exists for tree '{TreeId}' that can be undone."));
            Assert.That(ex.Message, Does.Contain($"(operation '{UndoSnapshotSuffix}') was already undone at"),
                "a retry after a successful undo must say so rather than read as a fresh failure");
            Assert.That(h.Intent.State.UndoneOperationId, Is.EqualTo(UndoSnapshotSuffix));
            Assert.That(h.Intent.State.UndoneAtUtc, Is.Not.Null);
        });
    }

    // --- Status: running, undo requested, none ---

    [Test]
    public async Task Undo_progress_and_idleness_distinguish_running_unwinding_and_none()
    {
        var h = CreateUndoHarness(ResizePhase.Swap);
        SetupOldTreeDeletion(h.GrainFactory, isDeleted: false);

        var running = await h.Grain.GetUndoProgressAsync();
        var runningIdle = await h.Grain.IsIdleAsync();

        await h.Grain.RequestUndoAsync();
        var unwinding = await h.Grain.GetUndoProgressAsync();
        var unwindingIdle = await h.Grain.IsIdleAsync();

        await h.Grain.ProcessNextPhaseAsync();
        var none = await h.Grain.GetUndoProgressAsync();
        var noneIdle = await h.Grain.IsIdleAsync();

        Assert.Multiple(() =>
        {
            Assert.That((running.Pending, runningIdle), Is.EqualTo((false, false)), "resize running");
            Assert.That((unwinding.Pending, unwindingIdle), Is.EqualTo((true, false)), "undo requested, unwinding");
            Assert.That((none.Pending, noneIdle), Is.EqualTo((false, true)), "no resize");
            Assert.That(none.FailedOperationId, Is.Null);
        });
    }

    [Test]
    public async Task An_undo_of_a_completed_resize_is_pending_without_reopening_its_completion()
    {
        var h = CreateUndoHarness(ResizePhase.Cleanup);
        h.State.State.InProgress = false;
        h.State.State.Complete = true;
        SetupOldTreeDeletion(h.GrainFactory, isDeleted: true);

        await h.Grain.RequestUndoAsync();

        // Completion is monotonic for a given resize; the unwind is reported by
        // the undo progress instead.
        Assert.That(await h.Grain.IsIdleAsync(), Is.True);
        Assert.That((await h.Grain.GetUndoProgressAsync()).Pending, Is.True);
        await h.Grain.ProcessNextPhaseAsync();
        Assert.That((await h.Grain.GetUndoProgressAsync()).Pending, Is.False);
        await AssertAfterSwapCompensationCompleteAsync(h.State, h.GrainFactory);
    }

    [Test]
    public async Task The_keepalive_keeps_a_completed_resize_alive_while_its_accepted_undo_is_pending()
    {
        var h = CreateUndoHarness(ResizePhase.Cleanup);
        h.State.State.InProgress = false;
        h.State.State.Complete = true;
        h.Intent.State.RequestedOperationId = UndoSnapshotSuffix;

        await h.Grain.ReceiveReminder("resize-keepalive", new TickStatus());

        Assert.That(TimerRegistrations(h.Timers), Is.EqualTo(1),
            "a reactivated coordinator must re-arm the loop that will run the accepted undo");
        await h.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    // --- The phase loop runs the unwind, from every phase ---

    [TestCase(1)] // ResizePhase.Swap
    [TestCase(3)] // ResizePhase.Reject
    [TestCase(2)] // ResizePhase.Cleanup
    public async Task The_phase_loop_unwinds_an_accepted_undo_after_the_swap_instead_of_advancing(int phase)
    {
        var h = CreateUndoHarness((ResizePhase)phase);
        SetupOldTreeDeletion(h.GrainFactory, isDeleted: false);
        await h.Grain.RequestUndoAsync();

        await h.Grain.ProcessNextPhaseAsync();

        var registry = h.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
        await h.GrainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0")
            .DidNotReceive().EnterRejectingAsync(Arg.Any<string>());
        await h.GrainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .DidNotReceive().DeleteRetiredPhysicalTreeAsync();
        await AssertAfterSwapCompensationCompleteAsync(h.State, h.GrainFactory);
        Assert.That((await h.Grain.GetUndoProgressAsync()).Pending, Is.False);
    }

    [Test]
    public async Task The_phase_loop_unwinds_an_accepted_undo_at_the_snapshot_phase_without_running_a_slice()
    {
        var h = CreateUndoHarness(ResizePhase.Snapshot);
        SetupOldTreeDeletion(h.GrainFactory, isDeleted: false);
        await h.Grain.RequestUndoAsync();

        await h.Grain.ProcessNextPhaseAsync();

        var snapshot = h.GrainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        await snapshot.DidNotReceive().RunSnapshotSliceAsync();
        await snapshot.Received(1).AbortAsync(UndoSnapshotSuffix);
        await h.GrainFactory.GetGrain<ITreeDeletionGrain>($"{TreeId}/resized/{UndoSnapshotSuffix}")
            .Received(1).DeleteDerivedPhysicalTreeAsync();
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task An_undo_requested_while_a_snapshot_slice_runs_is_unwound_at_the_slice_boundary_without_a_swap()
    {
        // The headline case: the slice holds the coordinator's turn, the operator's
        // undo is admitted interleaved during it, and the slice then reports the
        // copy finished. The resize must not swap the alias onto a copy the
        // operator asked to discard; it unwinds from the Snapshot phase instead.
        var h = CreateUndoHarness(ResizePhase.Snapshot);
        SetupOldTreeDeletion(h.GrainFactory, isDeleted: false);
        var snapshot = h.GrainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        snapshot.RunSnapshotSliceAsync().Returns(async _ =>
        {
            await h.Grain.RequestUndoAsync();
            return true;
        });

        await h.Grain.ProcessNextPhaseAsync();

        var registry = h.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
        await snapshot.Received(1).AbortAsync(UndoSnapshotSuffix);
        await h.GrainFactory.GetGrain<ITreeDeletionGrain>($"{TreeId}/resized/{UndoSnapshotSuffix}")
            .Received(1).DeleteDerivedPhysicalTreeAsync();
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.Phase, Is.EqualTo(ResizePhase.Snapshot), "the alias must never have swapped");
            Assert.That(h.State.State.InProgress, Is.False);
            Assert.That(h.Intent.State.UndoneOperationId, Is.EqualTo(UndoSnapshotSuffix));
        });
    }

    [Test]
    public async Task The_manual_pass_unwinds_an_accepted_undo_instead_of_running_the_resize()
    {
        var h = CreateUndoHarness(ResizePhase.Snapshot);
        SetupOldTreeDeletion(h.GrainFactory, isDeleted: false);
        await h.Grain.RequestUndoAsync();

        await h.Grain.RunResizePassAsync();

        await h.GrainFactory.GetGrain<ITreeSnapshotGrain>(TreeId).DidNotReceive().RunSnapshotPassAsync();
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task The_manual_pass_unwinds_an_undo_accepted_during_its_snapshot_pass()
    {
        var h = CreateUndoHarness(ResizePhase.Snapshot);
        SetupOldTreeDeletion(h.GrainFactory, isDeleted: false);
        var snapshot = h.GrainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        snapshot.RunSnapshotPassAsync().Returns(async _ => { await h.Grain.RequestUndoAsync(); });

        await h.Grain.RunResizePassAsync();

        var registry = h.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task A_new_resize_is_refused_while_an_accepted_undo_is_unwinding()
    {
        var h = CreateUndoHarness(ResizePhase.Snapshot);
        await h.Grain.RequestUndoAsync();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => h.Grain.ResizeAsync(h.State.State.NewMaxLeafKeys, h.State.State.NewMaxInternalChildren));

        Assert.That(ex!.Message, Does.Contain("is still unwinding"),
            "even the idempotent same-parameters re-trigger must not report a resize the loop is about to undo");
    }

    // --- Failure handling ---

    [Test]
    public async Task An_unwind_that_cannot_be_applied_is_withdrawn_with_its_reason()
    {
        var h = CreateUndoHarness(ResizePhase.Cleanup);
        var deletion = h.GrainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        deletion.IsPhysicalDeletedAsync().Returns(Task.FromResult(true));
        deletion.RecoverPhysicalAsync().ThrowsAsync(
            new InvalidOperationException("Cannot recover a tree whose data has already been purged."));
        await h.Grain.RequestUndoAsync();

        // The loop swallows the fault into its log; the outcome is recorded.
        await h.Grain.ProcessNextPhaseAsync();

        var progress = await h.Grain.GetUndoProgressAsync();
        Assert.Multiple(() =>
        {
            Assert.That(progress.Pending, Is.False, "an impossible unwind must not be retried forever");
            Assert.That(progress.FailedOperationId, Is.EqualTo(UndoSnapshotSuffix));
            Assert.That(progress.FailureMessage, Does.Contain("already been purged"));
            Assert.That(h.Intent.State.RequestedOperationId, Is.Null);
        });
    }

    [Test]
    public async Task RunPendingUndo_rethrows_the_reason_an_unwind_was_withdrawn()
    {
        var h = CreateUndoHarness(ResizePhase.Cleanup);
        var deletion = h.GrainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        deletion.IsPhysicalDeletedAsync().Returns(Task.FromResult(true));
        deletion.RecoverPhysicalAsync().ThrowsAsync(
            new InvalidOperationException("Cannot recover a tree whose data has already been purged."));
        await h.Grain.RequestUndoAsync();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.RunPendingUndoAsync());

        Assert.That(ex!.Message, Does.Contain("already been purged"));
    }

    [Test]
    public async Task A_new_request_clears_a_previously_withdrawn_failure()
    {
        var h = CreateUndoHarness(ResizePhase.Swap);
        h.Intent.State.FailedOperationId = UndoSnapshotSuffix;
        h.Intent.State.FailureMessage = "earlier failure";

        await h.Grain.RequestUndoAsync();

        var progress = await h.Grain.GetUndoProgressAsync();
        Assert.Multiple(() =>
        {
            Assert.That(progress.Pending, Is.True);
            Assert.That(progress.FailedOperationId, Is.Null);
            Assert.That(progress.FailureMessage, Is.Null);
        });
    }

    [Test]
    public async Task A_transient_unwind_failure_leaves_the_undo_pending_for_the_next_tick()
    {
        var h = CreateUndoHarness(ResizePhase.Swap);
        SetupOldTreeDeletion(h.GrainFactory, isDeleted: false);
        h.GrainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").ClearShadowForwardAsync(Arg.Any<string>())
            .Returns(Task.FromException(new TimeoutException("shard busy")), Task.CompletedTask);
        await h.Grain.RequestUndoAsync();

        await h.Grain.ProcessNextPhaseAsync();
        Assert.That((await h.Grain.GetUndoProgressAsync()).Pending, Is.True);

        await h.Grain.ProcessNextPhaseAsync();
        Assert.That((await h.Grain.GetUndoProgressAsync()).Pending, Is.False);
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task A_new_resize_makes_an_old_intent_inert()
    {
        var h = CreateUndoHarness(ResizePhase.Snapshot);
        h.Intent.State.RequestedOperationId = "an-earlier-resize";

        var progress = await h.Grain.GetUndoProgressAsync();
        var idle = await h.Grain.IsIdleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.Pending, Is.False);
            Assert.That(idle, Is.False, "the current resize is still running");
        });
    }
}
