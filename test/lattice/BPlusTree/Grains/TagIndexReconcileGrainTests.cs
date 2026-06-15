using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="TagIndexReconcileGrain"/> schedule-reminder
/// lifecycle and reminder dispatch, exercised against substitutes without a
/// silo. The end-to-end digest-gated sweep is covered by the integration suite.
/// </summary>
[TestFixture]
public class TagIndexReconcileGrainTests
{
    private const string IndexName = "test-index";

    private static (TagIndexReconcileGrain grain,
                     FakePersistentState<TagIndexReconcileState> state,
                     IReminderRegistry reminderRegistry,
                     IGrainFactory grainFactory) CreateGrain(
        LatticeTagIndexReconciliationOptions? options = null,
        FakePersistentState<TagIndexReconcileState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tag-index-reconcile", IndexName));
        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeTagIndexReconciliationOptions>>();
        options ??= new LatticeTagIndexReconciliationOptions();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);
        var state = existingState ?? new FakePersistentState<TagIndexReconcileState>();

        var grain = new TagIndexReconcileGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            new LoggerFactory().CreateLogger<TagIndexReconcileGrain>(), state);
        return (grain, state, reminderRegistry, grainFactory);
    }

    [Test]
    public async Task EnsureScheduleAsync_registers_schedule_when_enabled()
    {
        var (grain, _, reminderRegistry, _) = CreateGrain();

        await grain.EnsureScheduleAsync();

        await reminderRegistry.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "tag-index-reconcile-schedule",
            TimeSpan.FromHours(1),
            TimeSpan.FromHours(1));
    }

    [Test]
    public async Task EnsureScheduleAsync_clamps_interval_below_minimum()
    {
        var options = new LatticeTagIndexReconciliationOptions { Interval = TimeSpan.FromSeconds(10) };
        var (grain, _, reminderRegistry, _) = CreateGrain(options);

        await grain.EnsureScheduleAsync();

        await reminderRegistry.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "tag-index-reconcile-schedule",
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(1));
    }

    [Test]
    public async Task EnsureScheduleAsync_unregisters_when_disabled()
    {
        var options = new LatticeTagIndexReconciliationOptions { Enabled = false };
        var (grain, _, reminderRegistry, _) = CreateGrain(options);
        var reminder = Substitute.For<IGrainReminder>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "tag-index-reconcile-schedule")
            .Returns(Task.FromResult<IGrainReminder?>(reminder));

        await grain.EnsureScheduleAsync();

        await reminderRegistry.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        await reminderRegistry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    [Test]
    public async Task ReceiveReminder_schedule_drift_corrects_period_while_in_progress()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true; // a sweep is already running; only drift-correction should fire
        var (grain, _, reminderRegistry, _) = CreateGrain(existingState: state);

        // Default TickStatus reports a zero period, which differs from the
        // desired 1-hour cadence, so the handler re-registers the schedule.
        await grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        await reminderRegistry.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "tag-index-reconcile-schedule",
            TimeSpan.FromHours(1),
            TimeSpan.FromHours(1));
    }

    [Test]
    public async Task ReceiveReminder_schedule_unregisters_when_disabled()
    {
        var options = new LatticeTagIndexReconciliationOptions { Enabled = false };
        var (grain, _, reminderRegistry, _) = CreateGrain(options);
        var reminder = Substitute.For<IGrainReminder>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "tag-index-reconcile-schedule")
            .Returns(Task.FromResult<IGrainReminder?>(reminder));

        await grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        await reminderRegistry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    [Test]
    public async Task IsIdleAsync_reflects_in_progress_flag()
    {
        var (grain, state, _, _) = CreateGrain();

        Assert.That(await grain.IsIdleAsync(), Is.True);

        state.State.InProgress = true;
        Assert.That(await grain.IsIdleAsync(), Is.False);
    }
}
