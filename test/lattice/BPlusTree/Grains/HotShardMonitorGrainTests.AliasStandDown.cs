using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6097: a monitor that stood down because its tree is a copy its owner
/// aliases stays stood down, instead of having its keepalive re-registered by
/// every new router activation and removed again by the next pass.
/// </summary>
public partial class HotShardMonitorGrainTests
{
    private const string AliasOwnerTreeId = "monitor-test-owner";

    private static LifecycleHarness CreateAliasTargetMonitor(
        Func<bool>? aliased = null,
        FakePersistentState<HotShardMonitorState>? state = null) =>
        CreateLifecycleGrain(
            state: state,
            shapeEntry: (id, row) => aliased?.Invoke() == false
                ? row
                : id switch
                {
                    TreeId => row with { DerivedFrom = AliasOwnerTreeId },
                    AliasOwnerTreeId => row with { PhysicalTreeId = TreeId },
                    _ => row,
                });

    private static int MonitorKeepaliveRegistrations(IReminderRegistry reminders) =>
        reminders.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(IReminderRegistry.RegisterOrUpdateReminder));

    [Test]
    public async Task A_pass_that_stands_down_for_an_alias_owner_records_it_and_keeps_no_keepalive()
    {
        var state = new FakePersistentState<HotShardMonitorState>();
        var h = CreateAliasTargetMonitor(state: state);
        await h.Grain.EnsureRunningAsync();

        await CapturedSamplingTick(h.Timers)(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.AliasStandDownOwner, Is.EqualTo(AliasOwnerTreeId));
            Assert.That(MonitorKeepaliveRegistrations(h.Reminders), Is.EqualTo(1),
                "the arming registration only; the tick that stood down must not re-register it");
        });
        await h.Reminders.Received().GetReminder(Arg.Any<GrainId>(), "hot-shard-monitor");
    }

    [Test]
    public async Task EnsureRunning_after_an_alias_stand_down_does_nothing_until_the_recheck_interval()
    {
        var clock = new ManualTimeProvider();
        var h = CreateAliasTargetMonitor();
        h.Grain.TimeProvider = clock;
        await h.Grain.EnsureRunningAsync();
        await CapturedSamplingTick(h.Timers)(CancellationToken.None);
        h.Timers.ClearReceivedCalls();
        h.Reminders.ClearReceivedCalls();

        await h.Grain.EnsureRunningAsync();

        Assert.Multiple(() =>
        {
            Assert.That(TimersRegistered(h.Timers), Is.Zero);
            Assert.That(h.Reminders.ReceivedCalls(), Is.Empty);
        });

        clock.Advance(HotShardMonitorGrain.AliasStandDownRecheckInterval);
        await h.Grain.EnsureRunningAsync();

        Assert.Multiple(() =>
        {
            Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1), "a lapsed latch re-checks with one pass");
            Assert.That(MonitorKeepaliveRegistrations(h.Reminders), Is.Zero, "a re-check registers no keepalive");
        });
    }

    [Test]
    public async Task A_fresh_monitor_for_a_stood_down_alias_target_touches_no_reminder()
    {
        var state = new FakePersistentState<HotShardMonitorState>
        {
            State = new HotShardMonitorState { ActivationUtc = DateTime.UtcNow, AliasStandDownOwner = AliasOwnerTreeId },
        };
        var h = CreateAliasTargetMonitor(state: state);

        await h.Grain.EnsureRunningAsync();
        await CapturedSamplingTick(h.Timers)(CancellationToken.None);

        Assert.That(h.Reminders.ReceivedCalls(), Is.Empty,
            "a recorded stand-down re-checks without registering or removing a reminder");
    }

    [Test]
    public async Task A_pass_resumes_the_keepalive_once_the_tree_is_no_longer_an_alias_target()
    {
        var state = new FakePersistentState<HotShardMonitorState>
        {
            State = new HotShardMonitorState { ActivationUtc = DateTime.UtcNow, AliasStandDownOwner = AliasOwnerTreeId },
        };
        var h = CreateAliasTargetMonitor(aliased: () => false, state: state);

        await h.Grain.EnsureRunningAsync();
        await CapturedSamplingTick(h.Timers)(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.AliasStandDownOwner, Is.Null);
            Assert.That(MonitorKeepaliveRegistrations(h.Reminders), Is.EqualTo(1));
        });
    }
}
