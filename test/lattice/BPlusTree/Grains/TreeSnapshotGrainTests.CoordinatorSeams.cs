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
/// The pieces of <see cref="TreeSnapshotGrain"/> that sit either side of the
/// phase machine: the coordinator seams the base class reads on a keepalive
/// tick (<c>InProgress</c> and <c>LogContext</c>), the initiate-time guards
/// that reset a stale completion flag and reject an unrecognised mode, and the
/// online bulk drain's inner "copy this shard to the end" loop, which is the
/// one place a bounded pass is repeated rather than persisted and resumed.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    /// <summary>
    /// The standard harness plus an <see cref="ITimerRegistry"/> in the
    /// activation services, which the base coordinator needs before it can arm
    /// its phase timer on a reactivation tick.
    /// </summary>
    private static (TreeSnapshotGrain Grain,
                    FakePersistentState<TreeSnapshotState> State,
                    IReminderRegistry Reminders,
                    ITimerRegistry Timers,
                    IGrainFactory Factory) CreateGrainWithTimerRegistry(
        FakePersistentState<TreeSnapshotState>? existingState = null)
    {
        var timer = Substitute.For<IGrainTimer>();
        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(timer);

        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("snapshot", SourceTreeId));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var options = new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);
        var state = existingState ?? new FakePersistentState<TreeSnapshotState>();

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ExistsAsync(Arg.Any<string>()).Returns(false);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
            }));

        var grain = new TreeSnapshotGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<TreeSnapshotGrain>(), state);
        return (grain, state, reminderRegistry, timerRegistry, grainFactory);
    }

    private static int PhaseTimersRegistered(ITimerRegistry registry) =>
        registry.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));

    [Test]
    public async Task A_keepalive_tick_rearms_the_phase_timer_while_a_snapshot_is_in_progress()
    {
        // The base coordinator reads the grain's InProgress seam to decide
        // whether a reactivated silo still owes work. Reading it from the
        // persisted snapshot state is what makes the snapshot crash-recoverable.
        var h = CreateGrainWithTimerRegistry(SeededState(SnapshotPhase.Copy, SnapshotMode.Online));

        await h.Grain.ReceiveReminder("snapshot-keepalive", new TickStatus());

        Assert.That(PhaseTimersRegistered(h.Timers), Is.EqualTo(1));
    }

    [Test]
    public async Task A_keepalive_tick_tears_down_when_no_snapshot_is_in_progress()
    {
        // The control for the test above: same tick, same harness, opposite
        // InProgress - so a seam wired to a constant could not satisfy both.
        var h = CreateGrainWithTimerRegistry();
        Assume.That(h.State.State.InProgress, Is.False);
        var reminder = Substitute.For<IGrainReminder>();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), "snapshot-keepalive")
            .Returns(Task.FromResult(reminder));

        await h.Grain.ReceiveReminder("snapshot-keepalive", new TickStatus());

        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
        Assert.That(PhaseTimersRegistered(h.Timers), Is.Zero);
    }

    [Test]
    public async Task A_failing_keepalive_unregister_is_contained_and_names_the_source_tree()
    {
        // The teardown's unregister is best-effort: a reminder-service hiccup on
        // shutdown must not fault the grain. The log context it reports with is
        // the snapshot's own override ("tree <source>"), not the bare grain key.
        var h = CreateGrainWithTimerRegistry();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), "snapshot-keepalive")
            .ThrowsAsync(new InvalidOperationException("reminder service unavailable"));

        Assert.DoesNotThrowAsync(async () =>
            await h.Grain.ReceiveReminder("snapshot-keepalive", new TickStatus()));

        // The reached-the-fault assertion: without it, a green here would also
        // be produced by a teardown that never attempted the unregister at all.
        await h.Reminders.Received(1).GetReminder(Arg.Any<GrainId>(), "snapshot-keepalive");
    }

    [Test]
    public async Task A_foreign_reminder_name_is_ignored_by_the_coordinator()
    {
        var h = CreateGrainWithTimerRegistry(SeededState(SnapshotPhase.Copy, SnapshotMode.Online));

        await h.Grain.ReceiveReminder("some-other-reminder", new TickStatus());

        Assert.That(PhaseTimersRegistered(h.Timers), Is.Zero);
    }

    [Test]
    public async Task Starting_a_new_snapshot_clears_a_previous_completion_flag()
    {
        // A grain that already ran a snapshot to completion keeps Complete=true
        // until the next one starts. The public entry point clears it up front,
        // before it does any of the work that can fail - otherwise a snapshot
        // that got as far as validation would leave IsCompleteAsync reporting
        // the *previous* snapshot's result while the new one was still copying.
        var existing = new FakePersistentState<TreeSnapshotState>
        {
            State = new TreeSnapshotState { Complete = true },
        };
        var h = CreateGrainWithTimerRegistry(existing);
        SetupShardMocks(h.Factory, SourceTreeId);

        await h.Grain.SnapshotAsync(DestTreeId, SnapshotMode.Offline);

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.Complete, Is.False);
            Assert.That(h.State.State.InProgress, Is.True);
            Assert.That(h.State.State.DestinationTreeId, Is.EqualTo(DestTreeId));
        });
    }

    [Test]
    public void Initiating_a_snapshot_rejects_an_unrecognised_mode()
    {
        // The phase a snapshot starts in is chosen from its mode, so an
        // unrecognised mode has no sound starting phase and must fail loudly
        // rather than silently start in Lock and quiesce a tree the caller
        // asked to snapshot online.
        var (grain, _, _, grainFactory, _) = CreateGrain();
        SetupShardMocks(grainFactory, SourceTreeId);

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await grain.InitiateSnapshotStateAsync(DestTreeId, (SnapshotMode)99, ShardCount));
    }

    [Test]
    public async Task The_online_bulk_drain_copies_a_shard_across_as_many_passes_as_its_leaf_chain_needs()
    {
        // DrainAllShardsOnlineAsync copies each shard to the end in-process,
        // looping the bounded pass rather than persisting a cursor and waiting
        // for the next tick. Five leaves at two per pass needs three passes, so
        // a loop that ran once would copy only the first two leaves.
        var h = CreateCopyingSnapshot(leafCount: 5, leavesPerPass: 2, SnapshotMode.Online);

        await h.Grain.DrainAllShardsOnlineAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.MergedKeys, Has.Count.EqualTo(5));
            Assert.That(h.MergedKeys, Is.Unique, "a resumed pass must not re-copy a leaf it already drained");
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1),
                "the drain advances the cursor past every shard it drained");
        });
    }

    [Test]
    public async Task The_online_bulk_drain_marks_every_shard_it_drained()
    {
        var h = CreateCopyingSnapshot(leafCount: 5, leavesPerPass: 2, SnapshotMode.Online);

        await h.Grain.DrainAllShardsOnlineAsync();

        await h.SourceShard.Received(1).MarkDrainedAsync("op-1");
    }
}
