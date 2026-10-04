using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4271: a split, consolidation or reshard saga
/// whose tree is purged mid-flight faulted on every phase tick and kept its
/// keepalive reminder for as long as the cluster ran. Each saga must now
/// abandon itself - clear its state, stop its timer, unregister its reminder,
/// and count the abandonment - once a faulted tick finds the tree purged, while
/// a tree that is only soft-deleted keeps today's retrying behaviour.
/// </summary>
/// <remarks>
/// Every test drives the real captured phase-timer callback after the keepalive
/// reminder re-arms it, which is exactly the path a reactivated saga takes, so
/// the base coordinator's tick handling is exercised rather than simulated.
/// </remarks>
[TestFixture]
public sealed partial class PurgedTreeSagaAbandonmentTests
{
    private const string TreeId = "purged-saga-tree";

    private sealed class Harness
    {
        public required IRemindable Grain { get; init; }
        public required string ReminderName { get; init; }
        public required GrainId GrainId { get; init; }
        public required IReminderRegistry Reminders { get; init; }
        public required ITimerRegistry Timers { get; init; }
        public required IGrainTimer Timer { get; init; }
        public required ITreeDeletionGrain Deletion { get; init; }
        public required IGrainFactory Factory { get; init; }
        public required Func<bool> InProgress { get; init; }
        public required Func<bool> RecordExists { get; init; }
    }

    private sealed record Wiring(
        IGrainContext Context,
        GrainId GrainId,
        IGrainFactory Factory,
        IReminderRegistry Reminders,
        ITimerRegistry Timers,
        IGrainTimer Timer,
        IOptionsMonitor<LatticeOptions> Options,
        LatticeOptionsResolver Resolver,
        ITreeDeletionGrain Deletion);

    /// <summary>
    /// Builds the substitutes shared by every coordinator: a registry that
    /// refuses every call for the purged tree exactly as the real one does after
    /// issue #4230, shard roots that refuse every call, and a deletion grain
    /// whose purge verdict each test sets.
    /// </summary>
    private static Wiring Wire(string grainType, string key)
    {
        var timer = Substitute.For<IGrainTimer>();
        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(timer);

        var services = new ServiceCollection();
        services.AddSingleton(timers);

        var grainId = GrainId.Create(grainType, key);
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(grainId);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var notRegistered = new LatticeTreeNotRegisteredException(TreeId, "the saga step");
        registry.ResolveAsync(Arg.Any<string>()).ThrowsAsync(notRegistered);
        registry.GetShardMapAsync(Arg.Any<string>()).ThrowsAsync(notRegistered);
        registry.GetEntryAsync(Arg.Any<string>()).ThrowsAsync(notRegistered);

        var shard = Substitute.For<IShardRootGrain>();
        shard.GetLeftmostLeafIdAsync().ThrowsAsync(new InvalidOperationException("purged"));
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);

        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.HoldsCompletedPurgeAsync().Returns(true);
        factory.GetGrain<ITreeDeletionGrain>(TreeId).Returns(deletion);

        var options = new LatticeOptions();
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        return new Wiring(
            context, grainId, factory, Substitute.For<IReminderRegistry>(), timers, timer,
            monitor, TestOptionsResolver.ForFactory(factory, options), deletion);
    }

    private static Harness SplitSaga(string? boundPhysicalTreeId = null)
    {
        var w = Wire("split", $"{TreeId}/0");
        var state = new FakePersistentState<TreeShardSplitState>();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Drain;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [1, 3];
        state.State.OperationId = "split-op";
        state.State.PhysicalTreeId = boundPhysicalTreeId;

        w.Factory.StubResizeIdle();
        var grain = new TreeShardSplitGrain(
            w.Context, w.Factory, w.Reminders, w.Options, w.Resolver,
            new LoggerFactory().CreateLogger<TreeShardSplitGrain>(), state);

        return Build(w, grain, "shard-split-keepalive", () => state.State.InProgress, () => state.RecordExists);
    }

    private static Harness ConsolidationSaga(string? boundPhysicalTreeId = null)
    {
        var w = Wire("consolidation", $"{TreeId}/1");
        var state = new FakePersistentState<TreeShardConsolidationState>();
        state.State.InProgress = true;
        state.State.Phase = ShardConsolidationPhase.BeginShadowWrite;
        state.State.DonorShardIndex = 1;
        state.State.SurvivorShardIndex = 0;
        state.State.DonorSlots = [1, 3];
        state.State.OperationId = "consolidation-op";
        state.State.PhysicalTreeId = boundPhysicalTreeId;

        w.Factory.StubResizeIdle();
        var grain = new TreeShardConsolidationGrain(
            w.Context, w.Factory, w.Reminders, w.Options, w.Resolver,
            new LoggerFactory().CreateLogger<TreeShardConsolidationGrain>(), state);

        return Build(w, grain, "shard-consolidation-keepalive", () => state.State.InProgress, () => state.RecordExists);
    }

    private static Harness ReshardSaga()
    {
        var w = Wire("reshard", TreeId);
        var state = new FakePersistentState<TreeReshardState>();
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 4;
        state.State.StartShardCount = 2;

        var grain = new TreeReshardGrain(
            w.Context, w.Factory, w.Reminders, w.Options, w.Resolver,
            new LoggerFactory().CreateLogger<TreeReshardGrain>(),
            Substitute.For<ITagIndexReconcileTrigger>(), state);

        return Build(w, grain, "reshard-keepalive", () => state.State.InProgress, () => state.RecordExists);
    }

    private static Harness Build(
        Wiring w, IRemindable grain, string reminderName, Func<bool> inProgress, Func<bool> recordExists) =>
        new()
        {
            Grain = grain,
            ReminderName = reminderName,
            GrainId = w.GrainId,
            Reminders = w.Reminders,
            Timers = w.Timers,
            Timer = w.Timer,
            Deletion = w.Deletion,
            Factory = w.Factory,
            InProgress = inProgress,
            RecordExists = recordExists,
        };

    private static readonly string[] Sagas = ["split", "consolidation", "reshard"];

    private static Harness Create(string saga) => saga switch
    {
        "split" => SplitSaga(),
        "consolidation" => ConsolidationSaga(),
        "reshard" => ReshardSaga(),
        _ => throw new ArgumentOutOfRangeException(nameof(saga), saga, null),
    };

    /// <summary>
    /// Re-arms the phase timer through the keepalive reminder, as a reactivated
    /// saga does, and returns the tick callback handed to the timer registry.
    /// </summary>
    private static async Task<Func<CancellationToken, Task>> ArmAsync(Harness h)
    {
        await h.Grain.ReceiveReminder(h.ReminderName, default);
        var call = h.Timers.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (Func<CancellationToken, Task>)call.GetArguments()[2]!;
    }

    [TestCaseSource(nameof(Sagas))]
    public async Task Faulted_tick_on_a_purged_tree_abandons_the_saga(string saga)
    {
        var h = Create(saga);
        var tick = await ArmAsync(h);

        await tick(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(h.InProgress(), Is.False, "The saga's state must be cleared.");
            Assert.That(h.RecordExists(), Is.False, "The saga's durable state row must be deleted.");
        });
        h.Timer.Received(1).Dispose();
        await h.Reminders.Received(1).UnregisterReminder(h.GrainId, Arg.Any<IGrainReminder>());
    }

    [TestCaseSource(nameof(Sagas))]
    public async Task Faulted_tick_on_a_deleted_but_unpurged_tree_keeps_retrying(string saga)
    {
        var h = Create(saga);
        h.Deletion.HoldsCompletedPurgeAsync().Returns(false);
        var tick = await ArmAsync(h);

        await tick(CancellationToken.None);
        await tick(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(h.InProgress(), Is.True, "A tree not yet purged keeps its saga.");
            Assert.That(h.RecordExists(), Is.True);
        });
        h.Timer.DidNotReceive().Dispose();
        await h.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [TestCaseSource(nameof(Sagas))]
    public async Task Faulted_tick_keeps_retrying_when_the_purge_verdict_cannot_be_read(string saga)
    {
        var h = Create(saga);
        h.Deletion.HoldsCompletedPurgeAsync().ThrowsAsync(new TimeoutException("deletion grain unreachable"));
        var tick = await ArmAsync(h);

        await tick(CancellationToken.None);

        Assert.That(h.InProgress(), Is.True, "An unknown purge verdict must not abandon the saga.");
        h.Timer.DidNotReceive().Dispose();
        await h.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [TestCaseSource(nameof(Sagas))]
    public async Task Abandonment_is_counted_once_tagged_by_coordinator_and_tree(string saga)
    {
        var h = Create(saga);
        var tick = await ArmAsync(h);
        var measurements = new List<(long Value, string? Kind, string? Tree)>();

        using (MeterListening.StartForInstrument(
            LatticeMetrics.CoordinatorPurgedTreeAbandonments,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? kind = null, tree = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagKind) kind = tag.Value?.ToString();
                    else if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value?.ToString();
                }

                lock (measurements) measurements.Add((value, kind, tree));
            })))
        {
            await tick(CancellationToken.None);
        }

        var ours = measurements.Where(m => m.Tree == TreeId && m.Kind == h.ReminderName).ToList();
        Assert.That(ours.Sum(m => m.Value), Is.EqualTo(1));
    }

    [TestCaseSource(nameof(Sagas))]
    public async Task Abandonment_counter_is_zero_primed_when_the_phase_timer_arms(string saga)
    {
        var h = Create(saga);
        var measurements = new List<(long Value, string? Kind, string? Tree)>();

        using (MeterListening.StartForInstrument(
            LatticeMetrics.CoordinatorPurgedTreeAbandonments,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? kind = null, tree = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagKind) kind = tag.Value?.ToString();
                    else if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value?.ToString();
                }

                lock (measurements) measurements.Add((value, kind, tree));
            })))
        {
            await ArmAsync(h);
        }

        Assert.That(
            measurements.Where(m => m.Tree == TreeId && m.Kind == h.ReminderName).Select(m => m.Value),
            Is.EqualTo(new[] { 0L }));
    }
}
