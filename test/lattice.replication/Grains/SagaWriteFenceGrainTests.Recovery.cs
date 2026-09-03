using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The recovery, retention, and non-fatal-fault half of
/// <see cref="SagaWriteFenceGrain"/>: the reminder beat that re-evaluates the two
/// release gates after a crash, the retention sweep that reclaims the grain's
/// state once the fence reaches its terminal phase, the superseding engage that
/// starts a fresh cycle over a lifted fence, and the reminder-registry faults the
/// grain deliberately treats as non-fatal because the on-demand poll still
/// applies.
/// </summary>
public partial class SagaWriteFenceGrainTests
{
    private const string PollReminder = "saga-write-fence-poll";
    private const string TtlReminder = "saga-write-fence-ttl";

    /// <summary>
    /// A reminder registry whose registration or lookup faults, so the grain's
    /// deliberately non-fatal catch arms are reached. Registration is faulted per
    /// reminder name so a test can break the poll reminder without also breaking
    /// the base TTL slide.
    /// </summary>
    private static IReminderRegistry FaultingReminders(
        string? faultRegisterFor = null, bool faultGet = false)
    {
        var reminders = Substitute.For<IReminderRegistry>();

        reminders.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(call => faultRegisterFor is null || call.ArgAt<string>(1) == faultRegisterFor
                ? Task.FromException<IGrainReminder>(new InvalidOperationException("reminder service down"))
                : Task.FromResult(Substitute.For<IGrainReminder>()));

        if (faultGet)
        {
            reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
                .Returns(Task.FromException<IGrainReminder?>(
                    new InvalidOperationException("reminder service down")));
        }

        return reminders;
    }

    [Test]
    public async Task Engage_over_a_lifted_fence_starts_a_fresh_cycle()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));
        await h.Grain.LiftAsync();

        var lifted = await h.Grain.GetSnapshotAsync();
        Assert.That(lifted.ShippingResumed, Is.True, "precondition: the first cycle completed");

        // A second cutover reuses the same saga id. The release flags carried
        // over from the lifted cycle would otherwise read as "already released",
        // so an engage over a terminal fence must clear them.
        await h.Grain.EngageAsync(Request("orders"));

        var reEngaged = await h.Grain.GetSnapshotAsync();
        Assert.Multiple(() =>
        {
            Assert.That(reEngaged.Phase, Is.EqualTo(SagaWriteFencePhase.Engaged));
            Assert.That(reEngaged.WritesUnblocked, Is.False,
                "a superseding engage must re-fence writes, not inherit the prior release");
            Assert.That(reEngaged.ShippingResumed, Is.False,
                "and must re-pause shipping, or the second cutover ships across its own fence");
        });
    }

    [Test]
    public async Task UnblockWrites_before_any_engage_is_a_no_op()
    {
        var h = CreateGrain(["peer-a"]);

        await h.Grain.UnblockWritesAsync();

        await h.Shard.DidNotReceive().LiftWriteFenceAsync(Arg.Any<string>());
        Assert.That((await h.Grain.GetSnapshotAsync()).Phase, Is.EqualTo(SagaWriteFencePhase.None));
    }

    [Test]
    public async Task UnblockWrites_is_idempotent()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));

        await h.Grain.UnblockWritesAsync();
        await h.Grain.UnblockWritesAsync();

        // The second call must short-circuit rather than re-fan-out a lift over
        // every shard of every tree.
        await h.Shard.Received(ShardCount).LiftWriteFenceAsync(SagaId);
    }

    [Test]
    public async Task The_poll_reminder_re_evaluates_the_release_gates()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));
        h.Completion.Complete = true;

        // The reminder beat is the crash-recovery path: nobody is calling
        // PollResumeAsync on a coordinator that never came back.
        await h.Grain.ReceiveReminder(PollReminder, default);

        await h.Shipper.Received(1).ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
        Assert.That((await h.Grain.GetSnapshotAsync()).Phase, Is.EqualTo(SagaWriteFencePhase.Lifted));
    }

    [Test]
    public async Task An_unrelated_reminder_name_does_not_evaluate_the_gates()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));
        h.Completion.Complete = true;

        await h.Grain.ReceiveReminder("some-other-reminder", default);

        await h.Shipper.DidNotReceive().ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
        Assert.That(h.Completion.ProbeCount, Is.Zero, "an unrelated tick must not probe completion");
    }

    [Test]
    public async Task Polling_a_lifted_fence_does_no_further_work()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));
        await h.Grain.LiftAsync();
        var probesAtLift = h.Completion.ProbeCount;

        var snap = await h.Grain.PollResumeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snap.Phase, Is.EqualTo(SagaWriteFencePhase.Lifted));
            Assert.That(h.Completion.ProbeCount, Is.EqualTo(probesAtLift),
                "a terminal fence must not keep probing global completion forever");
        });
        await h.Shipper.Received(1).ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Retention_expiry_clears_the_persisted_fence()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders", "orders-index"));
        await h.Grain.LiftAsync();

        await h.Grain.ReceiveReminder(TtlReminder, default);

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.SagaId, Is.Null);
            Assert.That(h.State.State.Trees, Is.Empty);
            Assert.That(h.State.State.Phase, Is.EqualTo(SagaWriteFencePhase.None));
            Assert.That(h.State.State.FenceDeadlineTicks, Is.Zero);
            Assert.That(h.State.State.CoordinatorClusterId, Is.Null);
            Assert.That(h.State.State.WritesUnblocked, Is.False);
            Assert.That(h.State.State.ShippingResumed, Is.False);
            Assert.That(h.State.State.EngagedAtTicks, Is.Zero);
        });
    }

    [Test]
    public async Task A_fence_engaged_in_the_future_records_a_clamped_duration()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));

        // Clock skew (or a state written by a silo whose clock ran ahead) can put
        // the engage stamp in the future. The duration histogram must never take
        // a negative sample.
        h.State.State.EngagedAtTicks = DateTime.UtcNow.AddMinutes(5).Ticks;

        Assert.That(() => h.Grain.UnblockWritesAsync(), Throws.Nothing);
        await h.Grain.LiftAsync();

        Assert.That((await h.Grain.GetSnapshotAsync()).Phase, Is.EqualTo(SagaWriteFencePhase.Lifted));
    }

    [Test]
    public async Task Arming_the_poll_reminder_survives_a_reminder_service_fault()
    {
        // The reminder only accelerates crash recovery; an on-demand poll still
        // releases the fence, so a reminder-service outage must not fail the
        // engage and strand every write behind an unfenced cutover.
        var h = CreateGrain(["peer-a"], FaultingReminders(faultRegisterFor: PollReminder));

        await h.Grain.EngageAsync(Request("orders"));

        await h.Shard.Received(ShardCount).EngageWriteFenceAsync(SagaId, Arg.Any<long>());
        Assert.That((await h.Grain.GetSnapshotAsync()).Phase, Is.EqualTo(SagaWriteFencePhase.Engaged));
    }

    [Test]
    public async Task Unregistering_the_poll_reminder_survives_a_reminder_service_fault()
    {
        var h = CreateGrain(["peer-a"], FaultingReminders(faultGet: true));
        await h.Grain.EngageAsync(Request("orders"));

        // The terminal lift tears the poll reminder down. A failure there must
        // not abort the lift itself, or the fence would stay engaged.
        await h.Grain.LiftAsync();

        await h.Shipper.Received(1).ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
        Assert.That((await h.Grain.GetSnapshotAsync()).Phase, Is.EqualTo(SagaWriteFencePhase.Lifted));
    }
}
