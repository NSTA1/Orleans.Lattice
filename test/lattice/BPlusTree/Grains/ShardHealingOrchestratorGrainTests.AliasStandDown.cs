using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6097: an orchestrator that stood down because its tree is a copy its
/// owner aliases stays stood down. The copy's routers are stateless workers, and
/// every new one arms the orchestrator again; before the stand-down was durable
/// each re-arm registered the keepalive and the next sweep removed it, rewriting
/// the reminder row about once a minute per copy.
/// </summary>
public partial class ShardHealingOrchestratorGrainTests
{
    private static Harness CreateAliasTarget(
        Func<bool>? aliased = null,
        ShardHealingOrchestratorState? existingState = null) =>
        CreateGrain(
            existingState: existingState,
            shapeEntry: (id, row) => aliased?.Invoke() == false
                ? row
                : id switch
                {
                    TreeId => row with { DerivedFrom = AliasOwner },
                    AliasOwner => row with { PhysicalTreeId = TreeId },
                    _ => row,
                });

    private static int KeepaliveRegistrations(Harness h) =>
        h.Reminders.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(IReminderRegistry.RegisterOrUpdateReminder));

    [Test]
    public async Task RunHealingPass_records_the_alias_stand_down_durably()
    {
        var h = CreateAliasTarget();
        var reminder = Substitute.For<IGrainReminder>();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), "shard-healing").Returns(reminder);

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.AliasStandDownOwner, Is.EqualTo(AliasOwner));
            Assert.That(h.State.WriteCount, Is.GreaterThanOrEqualTo(1));
        });
        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    [Test]
    public async Task A_sweep_that_stands_down_does_not_register_the_keepalive_again()
    {
        var h = CreateAliasTarget();
        await h.Grain.EnsureRunningAsync();

        await CapturedSweepTick(h.Timers)(CancellationToken.None);

        Assert.That(KeepaliveRegistrations(h), Is.EqualTo(1),
            "the arming registration only; the tick that stood down must not re-register it");
    }

    [Test]
    public async Task EnsureRunning_after_an_alias_stand_down_does_nothing_until_the_recheck_interval()
    {
        var h = CreateAliasTarget();
        await h.Grain.EnsureRunningAsync();
        await CapturedSweepTick(h.Timers)(CancellationToken.None);
        h.Timers.ClearReceivedCalls();
        h.Reminders.ClearReceivedCalls();

        await h.Grain.EnsureRunningAsync();

        Assert.Multiple(() =>
        {
            Assert.That(SweepTimersArmed(h.Timers), Is.Zero);
            Assert.That(h.Reminders.ReceivedCalls(), Is.Empty);
        });

        h.Clock.Advance(ShardHealingOrchestratorGrain.AliasStandDownRecheckInterval);
        await h.Grain.EnsureRunningAsync();

        Assert.Multiple(() =>
        {
            Assert.That(SweepTimersArmed(h.Timers), Is.EqualTo(1), "a lapsed latch re-checks with one sweep");
            Assert.That(KeepaliveRegistrations(h), Is.Zero, "a re-check registers no keepalive");
        });
    }

    [Test]
    public async Task A_fresh_activation_of_a_stood_down_alias_target_touches_no_reminder()
    {
        var h = CreateAliasTarget(existingState: new ShardHealingOrchestratorState { AliasStandDownOwner = AliasOwner });

        await h.Grain.EnsureRunningAsync();
        await CapturedSweepTick(h.Timers)(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SweepTimersArmed(h.Timers), Is.EqualTo(1));
            Assert.That(h.Reminders.ReceivedCalls(), Is.Empty,
                "a recorded stand-down re-checks without registering or removing a reminder");
            Assert.That(h.State.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task A_sweep_resumes_the_keepalive_once_the_tree_is_no_longer_an_alias_target()
    {
        var h = CreateAliasTarget(
            aliased: () => false,
            existingState: new ShardHealingOrchestratorState { AliasStandDownOwner = AliasOwner });

        await h.Grain.EnsureRunningAsync();
        await CapturedSweepTick(h.Timers)(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.AliasStandDownOwner, Is.Null);
            Assert.That(KeepaliveRegistrations(h), Is.EqualTo(1));
        });
    }
}
