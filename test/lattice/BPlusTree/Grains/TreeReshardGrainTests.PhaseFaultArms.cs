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
/// Per-phase revert coverage and coordinator-seam coverage for
/// <see cref="TreeReshardGrain"/>, split from the main fixture by concern.
/// <para>
/// <c>TreeReshardGrainTests.WriteFailure</c> already pins the two revert arms
/// that bracket the reshard - <c>ReshardAsync</c> (intent) and
/// <c>FinaliseAsync</c> (terminal). This file covers the arms in between: the
/// phase machine's own <c>Planning -&gt; Migrating</c> and
/// <c>Migrating -&gt; Complete</c> flips, which exist on two separate drive
/// paths (the pull-based <see cref="TreeReshardGrain.RunReshardPassAsync"/> and
/// the timer-driven <c>ProcessNextPhaseAsync</c>), plus the empty-tree fast
/// path's three-field revert.
/// </para>
/// <para>
/// The invariant every revert protects is the same one: an in-memory
/// <c>Phase</c> left ahead of disk makes the coordinator skip the phase it
/// never durably recorded. That is a silent data-movement gap rather than a
/// retryable error, because the next tick reads the dirty in-memory value and
/// moves on, while a reactivation reloads the older durable value and repeats
/// work the activation believed done.
/// </para>
/// </summary>
public partial class TreeReshardGrainTests
{
    private const string ReshardKeepaliveReminder = "reshard-keepalive";

    /// <summary>
    /// Builds a reshard grain whose logger is captured and whose activation
    /// really resolves an <see cref="ITimerRegistry"/>, so the coordinator
    /// seam (<c>ReceiveReminder</c> -&gt; <c>StartPhaseTimer</c>) can be driven
    /// and its warning output asserted. The main <c>CreateGrain</c> helper
    /// returns neither the reminder registry nor the log sink, both of which
    /// this file's coordinator tests assert against.
    /// </summary>
    private static (TreeReshardGrain grain,
                    FakePersistentState<TreeReshardState> state,
                    IReminderRegistry reminderRegistry,
                    ITimerRegistry timerRegistry,
                    RecordingLoggerFactory logs) CreateCoordinatorGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("reshard", TreeId));

        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());

        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var options = new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var logs = new RecordingLoggerFactory();
        var state = new FakePersistentState<TreeReshardState>();
        var grain = new TreeReshardGrain(
            context,
            grainFactory,
            reminderRegistry,
            optionsMonitor,
            TestOptionsResolver.ForFactory(grainFactory, options),
            logs.CreateLogger<TreeReshardGrain>(),
            Substitute.For<ITagIndexReconcileTrigger>(),
            state);

        return (grain, state, reminderRegistry, timerRegistry, logs);
    }

    /// <summary>
    /// Points every shard grain at a substitute reporting no live keys, so
    /// <see cref="TreeEmptinessProbe"/> positively observes the tree as empty
    /// and unlocks the empty-tree fast path. The default harness deliberately
    /// reports a non-empty tree so the validation path runs instead.
    /// </summary>
    private static void SetupEmptyShards(IGrainFactory grainFactory)
    {
        var shard = Substitute.For<IShardRootGrain>();
        shard.AnyBoundedAsync(Arg.Any<string?>())
            .Returns(Task.FromResult(new ShardAnyPage { Found = false }));
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
    }

    // --- Planning -> Migrating revert, on both drive paths ---

    [Test]
    public void RunReshardPass_reverts_the_phase_when_the_Planning_flip_fails_to_persist()
    {
        var (grain, state, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Planning;
        state.State.TargetShardCount = 4;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RunReshardPassAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.Planning),
                "An unrecorded advance to Migrating would make the next pass skip planning "
                + "and start migrating slots the coordinator never durably decided to move.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public void ProcessNextPhase_reverts_the_phase_when_the_Planning_flip_fails_to_persist()
    {
        // The same flip exists on the timer-driven path. It is a separate arm
        // in the source, so a revert fixed on only one of the two would leave
        // the timer path - the one that actually drives a live reshard -
        // diverging on exactly the fault the pull path is protected from.
        var (grain, state, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Planning;
        state.State.TargetShardCount = 4;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextPhaseAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.Planning));
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task ProcessNextPhase_advances_Planning_to_Migrating_when_the_persist_succeeds()
    {
        // The positive half of the pair above: without it, both revert tests
        // would still pass if the flip never ran at all.
        var (grain, state, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Planning;
        state.State.TargetShardCount = 4;

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.Migrating));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RunReshardPass_drives_a_Planning_reshard_through_to_completion()
    {
        // The pull path's positive control. It also covers the case the phase
        // machine is built around: one pass may traverse several phases, because
        // each phase's guard re-reads the phase the previous one just set. With
        // the tree already at its target count the whole Planning -> Migrating
        // -> Complete chain runs in a single call.
        var (grain, state, _, _) = CreateGrain(physicalShardCount: 2);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Planning;
        state.State.TargetShardCount = 2;
        state.State.OperationId = "op-run-pass";

        await grain.RunReshardPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.Not.EqualTo(ReshardPhase.Planning),
                "The Planning flip must have been durably recorded and left behind.");
            Assert.That(state.State.Complete, Is.True);
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.WriteCount, Is.GreaterThanOrEqualTo(2),
                "Each phase transition persists before the next one is evaluated.");
        });
    }

    [Test]
    public async Task RunReshardPass_on_an_idle_reshard_is_a_no_op()
    {
        var (grain, state, _, _) = CreateGrain();
        state.State.InProgress = false;

        await grain.RunReshardPassAsync();

        Assert.That(state.WriteCount, Is.Zero);
    }

    // --- Migrating -> Complete revert ---

    [Test]
    public void Migrate_reverts_the_phase_when_the_Complete_flip_fails_to_persist()
    {
        // A tree already at its target physical count: MigrateAsync's
        // "target reached" branch flips Migrating -> Complete and persists.
        var (grain, state, _, _) = CreateGrain(physicalShardCount: 2);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 2;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.MigrateAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.Migrating),
                "A dirty in-memory Complete would trip RunReshardPassAsync's "
                + "`if (Phase == Complete) FinaliseAsync()` on the next tick, finalising a "
                + "reshard whose migration disk still records as unfinished.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task Migrate_advances_to_Complete_once_the_target_shard_count_is_reached()
    {
        var (grain, state, _, _) = CreateGrain(physicalShardCount: 2);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 2;

        await grain.MigrateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.Complete));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    // --- Empty-tree fast path revert ---

    [Test]
    public void The_empty_tree_fast_path_reverts_its_three_fields_when_WriteStateAsync_throws()
    {
        var (grain, state, grainFactory, _) = CreateGrain(physicalShardCount: 2);
        SetupEmptyShards(grainFactory);

        var prevComplete = state.State.Complete;
        var prevPhase = state.State.Phase;
        var prevTargetShardCount = state.State.TargetShardCount;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ReshardAsync(8));

        // No coordinator is active on this path, so a dirty Complete=true would
        // make IsCompleteAsync lie to callers about a reshard that never
        // persisted, and a retry from the same activation would observe the new
        // TargetShardCount on the fast path's re-evaluation.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Complete, Is.EqualTo(prevComplete));
            Assert.That(state.State.Phase, Is.EqualTo(prevPhase));
            Assert.That(state.State.TargetShardCount, Is.EqualTo(prevTargetShardCount));
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task The_empty_tree_fast_path_completes_without_starting_the_coordinator()
    {
        // Positive control for the revert above: it proves the fast path is
        // actually reached by this arrangement, so the revert assertions are
        // not passing merely because the mutation never ran.
        var (grain, state, grainFactory, registry) = CreateGrain(physicalShardCount: 2);
        SetupEmptyShards(grainFactory);

        await grain.ReshardAsync(8);

        await registry.Received().SetShardMapAsync(TreeId, Arg.Any<ShardMap>());
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Complete, Is.True);
            Assert.That(state.State.TargetShardCount, Is.EqualTo(8));
            Assert.That(state.State.Phase, Is.EqualTo(ReshardPhase.None));
            Assert.That(state.State.InProgress, Is.False,
                "The fast path must not start the coordinator machinery.");
        });
    }

    // --- Sparse slot histogram fallback ---

    [Test]
    public void CountSlotsPerPhysicalShard_uses_the_dense_counter_for_ordinary_shard_indices()
    {
        // Positive control for the sparse pair below, and the shape every real
        // tree takes: ShardMap.CreateDefault only ever emits small, dense
        // physical indices.
        int[] physicalShards = [0, 1, 2];
        int[] slots = [0, 0, 1, 2, 2, 2];

        var counts = TreeReshardGrain.CountSlotsPerPhysicalShard(physicalShards, slots);

        Assert.That(counts, Is.EqualTo(new[] { 2, 1, 3 }));
    }

    [Test]
    public void CountSlotsPerPhysicalShard_falls_back_to_binary_search_for_pathological_indices()
    {
        // A physical index at or above the 1<<20 dense-counter limit switches
        // the histogram to a binary search rather than allocating a counter
        // array of that size. This is the documented contract of the fallback -
        // bounded allocation - and the reason the method is exposed as internal.
        const int Sparse = (1 << 20) + 7;
        int[] physicalShards = [3, 11, Sparse];
        int[] slots = [11, Sparse, 11, 3];

        var counts = TreeReshardGrain.CountSlotsPerPhysicalShard(physicalShards, slots);

        Assert.That(counts, Is.EqualTo(new[] { 1, 2, 1 }),
            "The binary-search fallback must produce the same histogram as the dense path.");
    }

    [Test]
    public void CountSlotsPerPhysicalShard_ignores_slots_owned_by_no_listed_shard()
    {
        // Drives the binary search's not-found return: a slot value absent from
        // the ascending list must be skipped rather than counted against an
        // arbitrary ordinal. Values below, between and above the listed indices
        // all take the search's descending arm at least once.
        const int Sparse = (1 << 20) + 7;
        int[] physicalShards = [3, 11, Sparse];
        int[] slots = [0, 4, 10, 12, Sparse - 1, Sparse + 1, 11];

        var counts = TreeReshardGrain.CountSlotsPerPhysicalShard(physicalShards, slots);

        Assert.That(counts, Is.EqualTo(new[] { 0, 1, 0 }),
            "Only the one slot that matches a listed physical shard may be counted.");
    }

    [Test]
    public void CountSlotsPerPhysicalShard_returns_an_empty_histogram_for_no_physical_shards()
    {
        Assert.That(TreeReshardGrain.CountSlotsPerPhysicalShard([], [1, 2, 3]), Is.Empty);
    }

    // --- Coordinator seam: InProgress and LogContext overrides ---

    [Test]
    public async Task The_keepalive_reminder_re_arms_the_phase_timer_while_a_reshard_is_in_progress()
    {
        // Exercises the InProgress override: the base coordinator reads it to
        // decide between re-arming and retiring. A reshard that lost its timer
        // to a silo restart is resumed only by this path.
        var (grain, state, reminderRegistry, timerRegistry, _) = CreateCoordinatorGrain();
        state.State.InProgress = true;

        await grain.ReceiveReminder(ReshardKeepaliveReminder, new TickStatus());

        Assert.That(
            timerRegistry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer)),
            Is.EqualTo(1),
            "An in-progress reshard must re-arm its phase timer on reactivation.");
        await reminderRegistry.DidNotReceive()
            .UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task The_keepalive_reminder_retires_itself_once_the_reshard_is_finished()
    {
        var (grain, state, reminderRegistry, timerRegistry, _) = CreateCoordinatorGrain();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), ReshardKeepaliveReminder)
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
        state.State.InProgress = false;

        await grain.ReceiveReminder(ReshardKeepaliveReminder, new TickStatus());

        await reminderRegistry.Received()
            .UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        Assert.That(
            timerRegistry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer)),
            Is.Zero,
            "A finished reshard must not re-arm the phase timer it is about to abandon.");
    }

    [Test]
    public async Task An_unrelated_reminder_is_ignored_by_the_reshard_coordinator()
    {
        var (grain, state, reminderRegistry, timerRegistry, _) = CreateCoordinatorGrain();
        state.State.InProgress = true;

        await grain.ReceiveReminder("some-other-reminder", new TickStatus());

        Assert.That(timerRegistry.ReceivedCalls(), Is.Empty);
        Assert.That(reminderRegistry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void A_failing_keepalive_unregister_is_logged_against_the_tree_and_swallowed()
    {
        // Exercises the LogContext override, which renders "tree {TreeId}" into
        // the coordinator's warning. A reminder-table fault here is non-fatal -
        // the reshard is already finished - so the worst case is a stale
        // reminder that finds nothing to do on its next tick.
        var (grain, state, reminderRegistry, _, logs) = CreateCoordinatorGrain();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), ReshardKeepaliveReminder)
            .ThrowsAsync(new InvalidOperationException("reminder table unavailable"));
        state.State.InProgress = false;

        Assert.DoesNotThrowAsync(() => grain.ReceiveReminder(ReshardKeepaliveReminder, new TickStatus()));

        var warning = logs.Warnings.SingleOrDefault(w => w.Value("ReminderName") is ReshardKeepaliveReminder);
        Assert.That(warning, Is.Not.Null, "The swallowed fault must still be reported.");
        Assert.That(warning!.Value("Context"), Is.EqualTo($"tree {TreeId}"),
            "LogContext must identify the tree, not the bare grain key, or the warning "
            + "cannot be attributed to a tree in a multi-tree silo's logs.");
    }
}
