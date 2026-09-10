using Microsoft.Extensions.DependencyInjection;
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

/// <summary>
/// Coordinator-seam and per-phase revert coverage for
/// <see cref="TreeResizeGrain"/>, split from the main fixture by concern.
/// <para>
/// Two families live here. The first is the phase-machine's own
/// <c>WriteStateAsync</c> revert arms: every phase advance snapshots the phase,
/// mutates it, persists, and restores the previous phase when the persist
/// fails. Without the restore, the in-memory phase would run ahead of disk and
/// the resize would skip the phase it never durably recorded - a silent data
/// -movement gap rather than a retryable error. The second is the empty-tree
/// fast path, which repins structural sizing in the registry without running
/// the online pipeline at all and carries its own revert arm.
/// </para>
/// </summary>
public partial class TreeResizeGrainTests
{
    private const string KeepaliveReminder = "resize-keepalive";

    /// <summary>
    /// Points every shard grain at a substitute reporting no live keys, so the
    /// <see cref="TreeEmptinessProbe"/> positively observes the tree as empty and
    /// unlocks the fast path. Without this the factory hands back a grain whose
    /// probe faults, the probe's containment arm reports "not empty", and the
    /// coordinator path runs instead.
    /// </summary>
    private static void SetupEmptyShards(IGrainFactory grainFactory)
    {
        var shard = Substitute.For<IShardRootGrain>();
        shard.AnyBoundedAsync(Arg.Any<string?>())
            .Returns(Task.FromResult(new ShardAnyPage { Found = false }));
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
    }

    /// <summary>
    /// Clears the reshard interlock. An auto-substituted
    /// <see cref="ITreeReshardGrain"/> reports <c>IsIdleAsync() == false</c>,
    /// which <see cref="TreeResizeGrain.ResizeAsync"/> reads as "a reshard is in
    /// flight" and refuses.
    /// </summary>
    private static void SetupIdleReshard(IGrainFactory grainFactory)
    {
        var reshard = Substitute.For<ITreeReshardGrain>();
        reshard.IsIdleAsync().Returns(Task.FromResult(true));
        grainFactory.GetGrain<ITreeReshardGrain>(TreeId).Returns(reshard);
    }

    // --- Empty-tree fast path ---

    [Test]
    public async Task An_empty_tree_is_repinned_in_place_without_running_the_resize_pipeline()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupIdleReshard(grainFactory);
        SetupEmptyShards(grainFactory);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        await grain.ResizeAsync(256, 64);

        await registry.Received().UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(
            e => e.MaxLeafKeys == 256 && e.MaxInternalChildren == 64));
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Complete, Is.True);
            Assert.That(state.State.InProgress, Is.False,
                "The fast path must not start the coordinator machinery.");
            Assert.That(state.State.SnapshotTreeId, Is.Null,
                "No destination tree is created when nothing has to be migrated.");
        });
    }

    [Test]
    public void The_empty_tree_fast_path_reverts_its_flags_when_WriteStateAsync_throws()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SetupIdleReshard(grainFactory);
        SetupEmptyShards(grainFactory);

        var prevComplete = state.State.Complete;
        var prevNewMaxLeafKeys = state.State.NewMaxLeafKeys;
        var prevNewMaxInternalChildren = state.State.NewMaxInternalChildren;

        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.ResizeAsync(256, 64));
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Complete, Is.EqualTo(prevComplete),
                "A Complete left ahead of disk would make the next ResizeAsync clear it and diverge.");
            Assert.That(state.State.NewMaxLeafKeys, Is.EqualTo(prevNewMaxLeafKeys));
            Assert.That(state.State.NewMaxInternalChildren, Is.EqualTo(prevNewMaxInternalChildren));
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    // --- Per-phase revert arms ---

    [Test]
    public void WaitForSnapshot_reverts_the_phase_when_WriteStateAsync_throws()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        grainFactory.GetGrain<ITreeSnapshotGrain>(Arg.Any<string>())
            .Returns(Substitute.For<ITreeSnapshotGrain>());

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OldPhysicalTreeId = TreeId;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.WaitForSnapshotAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Snapshot),
                "An unrecorded advance to Swap would skip the snapshot pass on the retry.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public void SwapAlias_reverts_the_phase_when_WriteStateAsync_throws()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Swap;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/op1";
        state.State.OperationId = "op1";
        state.State.NewMaxLeafKeys = 256;
        state.State.NewMaxInternalChildren = 64;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.SwapAliasAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Swap),
                "The alias is already swapped; the phase must stay put so Reject is not skipped.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public void RejectOldShards_reverts_the_phase_when_WriteStateAsync_throws()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IShardRootGrain>());

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Reject;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.OperationId = "op1";
        state.State.ShardCount = ShardCount;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RejectOldShardsAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Reject),
                "Cleanup must not run against a Reject that was never durably recorded.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public void UndoResize_during_the_drain_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        grainFactory.GetGrain<ITreeSnapshotGrain>(Arg.Any<string>())
            .Returns(Substitute.For<ITreeSnapshotGrain>());
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IShardRootGrain>());
        grainFactory.GetGrain<ITreeDeletionGrain>(Arg.Any<string>())
            .Returns(Substitute.For<ITreeDeletionGrain>());

        // Before the alias swap: the destination is torn down and the source is
        // untouched, so the undo takes the drain branch rather than the
        // post-swap recovery branch.
        var oldEntry = new TreeRegistryEntry { MaxLeafKeys = 64, MaxInternalChildren = 32 };
        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/op1";
        state.State.OperationId = "op1";
        state.State.ShardCount = ShardCount;
        state.State.OldRegistryEntry = oldEntry;

        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True,
                "A cleared InProgress left ahead of disk would trip the UndoResizeAsync top guard and refuse every retry.");
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.SnapshotTreeId, Is.EqualTo($"{TreeId}/resized/op1"));
            Assert.That(state.State.OldPhysicalTreeId, Is.EqualTo(TreeId));
            Assert.That(state.State.OldRegistryEntry, Is.EqualTo(oldEntry));
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    // --- Phase-tick fault containment ---

    [Test]
    public async Task A_failing_phase_is_logged_and_swallowed_so_the_coordinator_keeps_ticking()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshot = Substitute.For<ITreeSnapshotGrain>();
        snapshot.RunSnapshotPassAsync().ThrowsAsync(new InvalidOperationException("snapshot pass failed"));
        grainFactory.GetGrain<ITreeSnapshotGrain>(Arg.Any<string>()).Returns(snapshot);

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OldPhysicalTreeId = TreeId;

        // The phase pump must absorb a phase fault: rethrowing would kill the
        // grain timer and wedge the resize with no further ticks to recover it.
        Assert.DoesNotThrowAsync(() => grain.ProcessNextPhaseAsync());
        await Task.CompletedTask;

        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Snapshot),
            "A failed phase must not advance.");
    }

    [Test]
    public async Task A_phase_tick_on_an_idle_resize_is_a_no_op()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.State.InProgress = false;

        await grain.ProcessNextPhaseAsync();

        Assert.That(state.WriteCount, Is.Zero);
    }

    // --- Coordinator keepalive seam ---

    [Test]
    public async Task The_keepalive_reminder_keeps_an_in_progress_resize_alive()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("resize", TreeId));

        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());
        var services = new ServiceCollection();
        services.AddSingleton(timers);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        SetupKeepalive(reminderRegistry);
        var options = new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);
        var state = new FakePersistentState<TreeResizeState> { State = { InProgress = true } };

        var grain = new TreeResizeGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            TestOptionsResolver.ForFactory(grainFactory, options),
            new LoggerFactory().CreateLogger<TreeResizeGrain>(),
            Substitute.For<ITagIndexReconcileTrigger>(), state);

        await grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        // Still working: the coordinator re-arms its phase timer and must NOT
        // retire its own crash-recovery reminder, or a silo restart would strand
        // the resize mid-flight.
        Assert.That(
            timers.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer)),
            Is.EqualTo(1),
            "An in-progress resize must re-arm its phase timer on reactivation.");
        await reminderRegistry.DidNotReceive()
            .UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task The_keepalive_reminder_retires_itself_once_the_resize_is_finished()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        state.State.InProgress = false;

        await grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        await reminderRegistry.Received()
            .UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public void A_failing_keepalive_unregister_is_logged_against_the_tree_and_swallowed()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder)
            .ThrowsAsync(new InvalidOperationException("reminder table unavailable"));
        state.State.InProgress = false;

        // The warning path renders LogContext ("tree {TreeId}"). A reminder-table
        // fault here is non-fatal: the resize is already finished, so the worst
        // case is a stale reminder that finds nothing to do on its next tick.
        Assert.DoesNotThrowAsync(() => grain.ReceiveReminder(KeepaliveReminder, new TickStatus()));
    }
}
