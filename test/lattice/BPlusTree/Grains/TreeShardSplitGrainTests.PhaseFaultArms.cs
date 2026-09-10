using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Fault-arm and coordinator-seam coverage for <see cref="TreeShardSplitGrain"/>,
/// split from the main fixture by concern.
/// <para>
/// <c>TreeShardSplitGrainTests.WriteFailure</c> already pins the revert that
/// brackets each phase's <b>first</b> persist. This file covers the arms either
/// side of those: the source shard refusing the shadow-write window <i>after</i>
/// the intent has already been committed, the second persist inside
/// <c>InitiateSplitStateAsync</c> (the <c>BeginShadowWrite -&gt; Drain</c>
/// advance), the bounded drain's resume-cursor revert on the <i>incomplete</i>
/// sweep path, the timer-driven phase pump's swallow-and-log arm, the
/// fire-and-forget diagnostics notification, and the TTL-preserving replay in
/// the retroactive prepared-mutation sweep.
/// </para>
/// <para>
/// The invariant every revert protects is the same one the sibling file states:
/// in-memory coordinator state must never reflect a mutation disk has not
/// accepted. A dirty <c>InProgress</c> makes <see cref="TreeShardSplitGrain.SplitAsync"/>
/// short-circuit every retry from the same activation, and a dirty <c>Phase</c>
/// makes the next tick skip a phase that was never durably recorded - both
/// silent data-movement gaps rather than retryable errors.
/// </para>
/// </summary>
public partial class TreeShardSplitGrainTests
{
    private const string SplitKeepaliveReminder = "shard-split-keepalive";

    /// <summary>
    /// Builds a split coordinator whose logger is captured and whose activation
    /// really resolves an <see cref="ITimerRegistry"/>, so the coordinator seam
    /// (<c>ReceiveReminder</c> -&gt; <c>StartPhaseTimer</c>) can be driven and
    /// its warning output asserted. The main <c>CreateGrain</c> helper returns
    /// neither the reminder registry nor the log sink, both of which this file's
    /// coordinator tests assert against.
    /// </summary>
    private static (TreeShardSplitGrain grain,
                    FakePersistentState<TreeShardSplitState> state,
                    IReminderRegistry reminderRegistry,
                    ITimerRegistry timerRegistry,
                    RecordingLoggerFactory logs) CreateCoordinatorGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("split", $"{TreeId}/0"));

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
        var state = new FakePersistentState<TreeShardSplitState>();
        var grain = new TreeShardSplitGrain(
            context,
            grainFactory,
            reminderRegistry,
            optionsMonitor,
            TestOptionsResolver.ForFactory(grainFactory, options),
            logs.CreateLogger<TreeShardSplitGrain>(),
            state);

        return (grain, state, reminderRegistry, timerRegistry, logs);
    }

    // --- Coordinator seam: the InProgress and LogContext overrides ---

    [Test]
    public async Task The_keepalive_reminder_re_arms_the_phase_timer_while_the_split_is_in_progress()
    {
        // Exercises the InProgress override on its true arm. After a silo
        // restart the keepalive is the only thing that reactivates the
        // coordinator, so a split that reported "not in progress" here would
        // strand a half-migrated shard with no pump to finish it.
        var (grain, state, reminderRegistry, timerRegistry, _) = CreateCoordinatorGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Drain;

        await grain.ReceiveReminder(SplitKeepaliveReminder, new TickStatus());

        Assert.That(
            timerRegistry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer)),
            Is.EqualTo(1),
            "An in-progress split must re-arm its phase timer on reactivation.");
        await reminderRegistry.DidNotReceive()
            .UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task The_keepalive_reminder_retires_itself_once_the_split_is_finished()
    {
        // The false arm of the same override, and the positive control for the
        // test above: without it, a test asserting "the timer was not re-armed"
        // would also pass against a coordinator that never arms a timer at all.
        var (grain, state, reminderRegistry, timerRegistry, _) = CreateCoordinatorGrain();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), SplitKeepaliveReminder)
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
        state.State.InProgress = false;

        await grain.ReceiveReminder(SplitKeepaliveReminder, new TickStatus());

        await reminderRegistry.Received()
            .UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        Assert.That(
            timerRegistry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer)),
            Is.Zero,
            "A finished split must not re-arm the phase timer it is about to abandon.");
    }

    [Test]
    public void A_failing_keepalive_unregister_is_logged_against_the_tree_and_swallowed()
    {
        // Exercises the LogContext override, which renders "tree {TreeId}" into
        // the coordinator's warning. A reminder-table fault here is non-fatal -
        // the split has already committed - so the worst case is a stale
        // reminder that finds nothing to do on its next tick.
        var (grain, state, reminderRegistry, _, logs) = CreateCoordinatorGrain();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), SplitKeepaliveReminder)
            .ThrowsAsync(new InvalidOperationException("reminder table unavailable"));
        state.State.InProgress = false;

        Assert.DoesNotThrowAsync(() => grain.ReceiveReminder(SplitKeepaliveReminder, new TickStatus()));

        var warning = logs.Warnings.SingleOrDefault(w => w.Value("ReminderName") is SplitKeepaliveReminder);
        Assert.That(warning, Is.Not.Null, "The swallowed fault must still be reported.");
        Assert.That(warning!.Value("Context"), Is.EqualTo($"tree {TreeId}"),
            "LogContext must identify the tree, not the bare grain key, or the warning "
            + "cannot be attributed to a tree in a multi-tree silo's logs.");
    }

    // --- SplitAsync: the coordinator key is the authority on the source shard ---

    [Test]
    public void SplitAsync_rejects_a_source_shard_that_disagrees_with_the_coordinator_key()
    {
        // The coordinator key is {treeId}/{sourceShardIndex} and the registry's
        // per-shard allocation is keyed on it, so honouring a mismatched
        // argument would run the whole split against a shard whose coordinator
        // is a different grain - two coordinators, one shard, no interlock
        // between them.
        var (grain, state, _, _, _, _) = CreateGrain(sourceShardIndex: 0);

        var ex = Assert.ThrowsAsync<ArgumentException>(() => grain.SplitAsync(1));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("sourceShardIndex"));
            Assert.That(ex.Message, Does.Contain("does not match coordinator key shard 0"),
                "The message must name both shards, because the caller's argument and the "
                + "grain key are the two things an operator has to reconcile.");
            Assert.That(state.WriteCount, Is.Zero,
                "A rejected split must not persist an intent.");
        });
    }

    [Test]
    public async Task SplitAsync_accepts_a_source_shard_that_matches_the_coordinator_key()
    {
        // Positive control for the guard above: a guard tested only by the
        // cases it rejects is indistinguishable from one that rejects
        // everything. This drives the whole of SplitAsync, so it needs the
        // fully-wired harness - StartCoordinatorAsync resolves an
        // ITimerRegistry off the activation to arm the phase timer.
        var (grain, state, _, source, _) = CreateFullyWiredGrain(sourceShardIndex: 0);

        await grain.SplitAsync(0);

        Assert.That(state.State.InProgress, Is.True);
        await source.Received(1).BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>());
    }

    // --- InitiateSplitStateAsync: the two arms after the intent is committed ---

    [Test]
    public void InitiateSplit_unwinds_the_committed_intent_when_the_source_refuses_the_shadow_window()
    {
        // The source shard acquired a migration record between SplitAsync's
        // cheap pre-check and this call - an online consolidation taking it as
        // a donor, in practice. The intent is already on disk at this point, so
        // unlike the first-write revert this arm must ALSO re-persist: leaving
        // InProgress=true durably with no reminder anchored on it makes the
        // shard permanently unsplittable, because SplitAsync short-circuits on
        // InProgress and nothing is pumping the phase machine.
        var (grain, state, _, _, source, _) = CreateGrain(sourceShardIndex: 0);
        source.BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>())
            .ThrowsAsync(new InvalidOperationException("a migration is already in progress"));

        Assert.That(async () => await grain.InitiateSplitStateAsync(0),
            Throws.TypeOf<InvalidOperationException>()
                .With.Message.EqualTo("a migration is already in progress"),
            "The refusal must reach the caller - it is what stops the coordinator starting.");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.OperationId, Is.Null);
            Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.None));
            Assert.That(state.State.SourceShardIndex, Is.EqualTo(0));
            Assert.That(state.State.TargetShardIndex, Is.EqualTo(0));
            Assert.That(state.State.MovedSlots, Is.Empty);
            Assert.That(state.State.OriginalShardMap, Is.Null);
            Assert.That(state.WriteCount, Is.EqualTo(2),
                "The unwind must be DURABLE, not just in-memory: one write committed the "
                + "intent and a second must retract it, or disk keeps an InProgress split "
                + "that no coordinator is driving.");
        });
    }

    [Test]
    public void InitiateSplit_reverts_the_drain_advance_when_its_persist_fails()
    {
        // The second persist inside InitiateSplitStateAsync, which advances
        // BeginShadowWrite -> Drain once the shadow window is open and the
        // retroactive sweep has run. An unrecorded advance leaves the
        // activation believing the shadow-write phase is done while disk still
        // says otherwise, so a reactivation re-opens a window the source has
        // already got.
        var (grain, state, _, _, source, _) = CreateGrain(sourceShardIndex: 0);

        // ThrowOnWrite is one-shot and self-clearing, so arm it from the first
        // successful write rather than up front - otherwise the intent write
        // takes the fault and this arm is never reached.
        state.OnWriteState = _ =>
        {
            if (state.WriteCount == 1)
                state.ThrowOnWrite = new InvalidOperationException("drain-advance persist boom");
        };

        Assert.That(async () => await grain.InitiateSplitStateAsync(0),
            Throws.TypeOf<InvalidOperationException>()
                .With.Message.EqualTo("drain-advance persist boom"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.BeginShadowWrite),
                "in-memory Phase must not advance to Drain when the persist failed");
            Assert.That(state.State.InProgress, Is.True,
                "The intent write DID commit, so this revert must roll back only the phase - "
                + "clearing InProgress here would discard a shadow window the source has open.");
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
        source.Received(1).BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>());
    }

    // --- ProcessNextPhaseAsync: the timer-driven pump ---

    [Test]
    public async Task ProcessNextPhase_drives_the_shadow_write_phase_through_the_full_split_pass()
    {
        // The BeginShadowWrite arm of the phase switch is the only one that
        // delegates to RunSplitPassAsync rather than to a single phase method,
        // because a crash between persisting intent and calling the source must
        // re-issue the (idempotent) begin before anything else can proceed.
        var (grain, state, _, _, source, _) = CreateGrain(sourceShardIndex: 0);
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.BeginShadowWrite;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [1, 3];
        state.State.OriginalShardMap = ShardMap.CreateDefault(16, 2);

        await grain.ProcessNextPhaseAsync();

        await source.Received().BeginSplitAsync(2, Arg.Any<int[]>(), 16);
        Assert.That(state.State.Phase, Is.Not.EqualTo(ShardSplitPhase.BeginShadowWrite),
            "The pump must leave the shadow-write phase behind, or the split never advances.");
    }

    [Test]
    public async Task ProcessNextPhase_is_a_no_op_once_the_split_is_no_longer_in_progress()
    {
        // Positive control for the pump: the tests above only prove it does
        // something, not that it stops.
        var (grain, state, _, _, source, _) = CreateGrain(sourceShardIndex: 0);
        state.State.InProgress = false;
        state.State.Phase = ShardSplitPhase.BeginShadowWrite;

        await grain.ProcessNextPhaseAsync();

        await source.DidNotReceive().BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>());
        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public void ProcessNextPhase_swallows_a_failing_phase_and_names_it_in_the_warning()
    {
        // The pump is driven by a grain timer whose exceptions the base
        // coordinator logs and discards, so a phase that throws must not
        // escape: the next tick retries it. The phase has to appear in the
        // warning, because a shard-split stuck in a retry loop is only
        // diagnosable from which phase is failing.
        var (grain, state, _, _, _, logs) = CreateGrainWithRecordedLogs(sourceShardIndex: 0);
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Reject;
        state.State.SourceShardIndex = 0;
        state.ThrowOnWrite = new InvalidOperationException("reject persist boom");

        Assert.DoesNotThrowAsync(() => grain.ProcessNextPhaseAsync(),
            "A failing phase must not fault the timer callback.");

        var warning = logs.Warnings.SingleOrDefault(w => w.Value("Phase") is ShardSplitPhase.Reject);
        Assert.That(warning, Is.Not.Null, "The swallowed phase fault must still be reported.");
        Assert.That(warning!.Value("TreeId"), Is.EqualTo(TreeId));
        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Reject),
            "Swallowing the fault must not also lose the revert - the phase stays put so "
            + "the next tick retries the same step.");
    }

    // --- DrainAsync: the resume-cursor revert on the incomplete-sweep path ---

    [Test]
    public void Drain_reverts_the_resume_cursor_when_persisting_it_fails()
    {
        // The sibling WriteFailure fixture covers the revert on the path where
        // the sweep COMPLETES (Phase advances to Swap). This is the other one:
        // a bounded pass that ran out of budget persists only a resume key and
        // deliberately stays in Drain. If that persist fails and the cursor is
        // left dirty, the next pass resumes from a key disk never recorded, so
        // every entry between the durable cursor and the dirty one is skipped -
        // silently, and only on the target shard.
        var (grain, state) = CreateDrainingSplitWithBudget(leafCount: 3, leavesPerPass: 1);
        state.ThrowOnWrite = new InvalidOperationException("cursor persist boom");

        Assert.That(async () => await grain.DrainAsync(),
            Throws.TypeOf<InvalidOperationException>()
                .With.Message.EqualTo("cursor persist boom"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.DrainCursorKey, Is.Null,
                "The resume cursor must roll back to the durable value, or the next pass "
                + "skips every entry the failed write was supposed to account for.");
            Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Drain),
                "An incomplete sweep must never advance the phase; Swap's ordering "
                + "invariants assume the historical sweep is finished.");
        });
    }

    [Test]
    public async Task Drain_persists_a_resume_cursor_and_stays_in_drain_when_the_pass_runs_out_of_budget()
    {
        // Positive control for the revert above, over the same fixture: it
        // proves this harness really does produce an INCOMPLETE sweep, so the
        // revert test is exercising the arm it claims to and not a sweep that
        // finished in one pass.
        var (grain, state) = CreateDrainingSplitWithBudget(leafCount: 3, leavesPerPass: 1);

        var complete = await grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.False);
            Assert.That(state.State.DrainCursorKey, Is.Not.Null);
            Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Drain));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    // --- FinaliseAsync: the fire-and-forget diagnostics notification ---

    [Test]
    public async Task Finalise_commits_the_split_even_when_the_diagnostics_notification_faults()
    {
        // The notification is fire-and-forget precisely so the commit path
        // never waits on the diagnostics ring buffer. A faulted task is
        // observed by an OnlyOnFaulted continuation that debug-logs it; the
        // point of the continuation is that the exception is retrieved rather
        // than surfacing later as an unobserved task exception.
        var (grain, state, grainFactory, _, source, _) = CreateGrain(sourceShardIndex: 0);
        var stats = Substitute.For<ILatticeStats>();
        stats.RecordSplitAsync(Arg.Any<int>(), Arg.Any<DateTime>())
            .Returns(Task.FromException(new InvalidOperationException("diagnostics ring unavailable")));
        grainFactory.GetGrain<ILatticeStats>(TreeId).Returns(stats);

        SetUpCompletableSplit(state);

        await grain.FinaliseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.True,
                "A diagnostics fault must never hold back the split's terminal state.");
        });
        await source.Received(1).CompleteSplitAsync();
    }

    [Test]
    public async Task Finalise_commits_the_split_even_when_resolving_the_diagnostics_grain_throws()
    {
        // The outer catch, which is a different arm from the faulted-task
        // continuation above: here the failure happens before there is any task
        // to continue from, so only a synchronous guard can contain it.
        var (grain, state, grainFactory, _, source, _) = CreateGrain(sourceShardIndex: 0);
        grainFactory.GetGrain<ILatticeStats>(TreeId)
            .Returns(_ => throw new InvalidOperationException("stats grain unresolvable"));

        SetUpCompletableSplit(state);

        await grain.FinaliseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.True,
                "Diagnostics plumbing must never affect split completion.");
        });
        await source.Received(1).CompleteSplitAsync();
    }

    // --- Retroactive sweep: per-entry expiry survives the replay ---

    [Test]
    public async Task RetroactiveSweep_replays_an_expiring_prepared_entry_through_the_TTL_aware_Set_overload()
    {
        // A prepared mutation that carried a TTL must reach the destination
        // through the ExpiresAtTicks overload. Routing it through the plain
        // Set would resurrect the entry as durable on the target, so a key the
        // operator expected to lapse would outlive the split - a data-retention
        // change caused purely by a rebalance.
        var txid = Guid.NewGuid();
        var expiresAt = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        var snap = BuildSetSnapshot(txid, out var key) with { ExpiresAtTicks = expiresAt };
        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.InFlight);

        await grain.InitiateSplitStateAsync(0);

        await target.Received(1).SetAsync(
            key, Arg.Is<byte[]>(b => b.SequenceEqual(snap.Value!)), expiresAt);
        await target.DidNotReceive().SetAsync(key, Arg.Any<byte[]>());
    }

    [Test]
    public async Task RetroactiveSweep_replays_a_durable_prepared_entry_through_the_plain_Set_overload()
    {
        // Positive control for the overload selection: without it, a regression
        // that routed EVERY replay through the TTL overload would still pass
        // the test above.
        var txid = Guid.NewGuid();
        var snap = BuildSetSnapshot(txid, out var key);
        var (grain, _, _, target, _, _) = CreateGrainWithSweepWiring(
            leafSnapshots: [snap],
            preCheckStatus: TxStatus.InFlight);

        await grain.InitiateSplitStateAsync(0);

        await target.Received(1).SetAsync(key, Arg.Is<byte[]>(b => b.SequenceEqual(snap.Value!)));
        await target.DidNotReceive().SetAsync(key, Arg.Any<byte[]>(), Arg.Any<long>());
    }

    // --- helpers ---

    /// <summary>
    /// Populates <paramref name="state"/> so <see cref="TreeShardSplitGrain.FinaliseAsync"/>
    /// runs all the way through its terminal write to the diagnostics
    /// notification, with an empty source leaf chain so the final drain is a
    /// no-op.
    /// </summary>
    private static void SetUpCompletableSplit(FakePersistentState<TreeShardSplitState> state)
    {
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Complete;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [];
        state.State.OriginalShardMap = ShardMap.CreateDefault(16, 2);
    }

    /// <summary>
    /// The main <c>CreateGrain</c> helper with a <see cref="RecordingLoggerFactory"/>
    /// substituted for its throwaway logger, so a test can assert on what the
    /// phase pump logged.
    /// </summary>
    private static (TreeShardSplitGrain grain,
                    FakePersistentState<TreeShardSplitState> state,
                    IGrainFactory grainFactory,
                    ILatticeRegistry registry,
                    IShardRootGrain sourceShard,
                    RecordingLoggerFactory logs) CreateGrainWithRecordedLogs(int sourceShardIndex = 0)
    {
        var (grain, state, grainFactory, registry, sourceShard, logs) = BuildGrain(sourceShardIndex);
        return (grain, state, grainFactory, registry, sourceShard, logs);
    }

    /// <summary>
    /// The same harness, exposed for tests that drive the whole of
    /// <see cref="TreeShardSplitGrain.SplitAsync"/> rather than one phase
    /// method. <c>SplitAsync</c> ends in <c>StartCoordinatorAsync</c>, which
    /// resolves an <see cref="ITimerRegistry"/> off the activation, so the
    /// main fixture's <c>CreateGrain</c> (which leaves
    /// <c>ActivationServices</c> unwired) cannot reach past it.
    /// </summary>
    private static (TreeShardSplitGrain grain,
                    FakePersistentState<TreeShardSplitState> state,
                    IGrainFactory grainFactory,
                    IShardRootGrain sourceShard,
                    RecordingLoggerFactory logs) CreateFullyWiredGrain(int sourceShardIndex = 0)
    {
        var (grain, state, grainFactory, _, sourceShard, logs) = BuildGrain(sourceShardIndex);
        return (grain, state, grainFactory, sourceShard, logs);
    }

    private static (TreeShardSplitGrain grain,
                    FakePersistentState<TreeShardSplitState> state,
                    IGrainFactory grainFactory,
                    ILatticeRegistry registry,
                    IShardRootGrain sourceShard,
                    RecordingLoggerFactory logs) BuildGrain(int sourceShardIndex)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("split", $"{TreeId}/{sourceShardIndex}"));

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

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(ShardMap.CreateDefault(16, 2));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 2 }));
        registry.AllocateNextShardIndexAsync(TreeId, Arg.Any<int>())
            .Returns(ci => Task.FromResult(((int)ci[1]) + 1));

        var sourceShard = Substitute.For<IShardRootGrain>();
        var targetShard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            var idx = int.Parse(key[(key.LastIndexOf('/') + 1)..]);
            return idx == sourceShardIndex ? sourceShard : targetShard;
        });
        sourceShard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));

        var logs = new RecordingLoggerFactory();
        var state = new FakePersistentState<TreeShardSplitState>();
        var grain = new TreeShardSplitGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            TestOptionsResolver.ForFactory(grainFactory, options),
            logs.CreateLogger<TreeShardSplitGrain>(), state);

        return (grain, state, grainFactory, registry, sourceShard, logs);
    }

    /// <summary>
    /// Builds a coordinator already in <see cref="ShardSplitPhase.Drain"/> over
    /// a source shard with <paramref name="leafCount"/> leaves and a per-pass
    /// budget of <paramref name="leavesPerPass"/>, so a single
    /// <c>DrainAsync</c> deliberately runs out of budget and takes the
    /// resume-cursor branch rather than the phase-advancing one.
    /// </summary>
    private static (TreeShardSplitGrain grain, FakePersistentState<TreeShardSplitState> state)
        CreateDrainingSplitWithBudget(int leafCount, int leavesPerPass)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("split", $"{TreeId}/0"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var options = new LatticeOptions { BackgroundDrainLeavesPerPass = leavesPerPass };
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(TreeId);
        var map = ShardMap.CreateDefault(16, 2);
        registry.GetShardMapAsync(TreeId).Returns(map);

        var sourceShard = Substitute.For<IShardRootGrain>();
        var targetShard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            var idx = int.Parse(key[(key.LastIndexOf('/') + 1)..]);
            return idx == 0 ? sourceShard : targetShard;
        });

        var leafIds = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            leafIds[i] = GrainId.Create("leaf", $"drain-leaf-{i}");

        sourceShard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafIds[0]));
        sourceShard.GetLeafIdForKeyAsync(null).Returns(Task.FromResult<GrainId?>(leafIds[0]));

        var wall = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leaf);
            leaf.GetDeltaSinceForSlotsAsync(Arg.Any<VersionVector>(), Arg.Any<int[]>(), Arg.Any<int>())
                .Returns(_ => Task.FromResult(new StateDelta
                {
                    Entries = new Dictionary<string, LwwValue<byte[]>>
                    {
                        [$"entry-{index}"] = LwwValue<byte[]>.Create(
                            [(byte)index],
                            new HybridLogicalClock { WallClockTicks = wall, Counter = index }),
                    },
                    Version = new VersionVector(),
                }));
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(
                index + 1 < leafCount ? (GrainId?)leafIds[index + 1] : null));
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = $"k{index:D4}",
                HighKeyExclusive = index + 1 < leafCount ? $"k{index + 1:D4}" : null,
            }));
            sourceShard.GetLeafIdForKeyAsync($"k{index:D4}").Returns(Task.FromResult<GrainId?>(leafIds[index]));
        }

        var state = new FakePersistentState<TreeShardSplitState>
        {
            State = new TreeShardSplitState
            {
                InProgress = true,
                Phase = ShardSplitPhase.Drain,
                OperationId = "op-drain",
                SourceShardIndex = 0,
                TargetShardIndex = 1,
                MovedSlots = [1, 3, 5, 7],
                OriginalShardMap = map,
            },
        };

        var grain = new TreeShardSplitGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            TestOptionsResolver.ForFactory(grainFactory, options),
            new LoggerFactory().CreateLogger<TreeShardSplitGrain>(), state);

        return (grain, state);
    }
}
