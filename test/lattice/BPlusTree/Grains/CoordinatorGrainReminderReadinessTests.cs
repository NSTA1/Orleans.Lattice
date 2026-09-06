using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue #2086: a coordinator started inside Orleans'
/// asynchronous reminder-service startup window must not fail its caller's
/// operation just because the keepalive-reminder registration raced the reminder
/// service into life. <see cref="CoordinatorGrain{TSelf}.StartCoordinatorAsync"/>
/// now waits the transient "Reminder Service is still initializing" condition out
/// with the same bounded retry the atomic-write saga's essential keepalive uses,
/// rather than propagating it. Any other fault, and a transient that never clears
/// within the retry budget, still surface with their original shape.
/// </summary>
[TestFixture]
public sealed class CoordinatorGrainReminderReadinessTests
{
    /// <summary>
    /// Test-only coordinator that exposes <c>StartCoordinatorAsync</c> and lets the
    /// retry backoff be injected so the retry budget is driven without real delays,
    /// exactly as <see cref="ReminderServiceReadiness"/> exposes its backoff-injectable
    /// core for the same reason.
    /// </summary>
    private sealed class ReadinessCoordinator(
        IGrainContext context,
        IReminderRegistry reminderRegistry,
        IReadOnlyList<TimeSpan> backoff)
        : CoordinatorGrain<ReadinessCoordinator>(context, reminderRegistry, NullLogger<ReadinessCoordinator>.Instance)
    {
        protected override string KeepaliveReminderName => "readiness-coord";
        protected override bool InProgress => false;
        protected internal override Task ProcessNextPhaseAsync() => Task.CompletedTask;
        protected override IReadOnlyList<TimeSpan> KeepaliveRegistrationBackoff => backoff;

        public Task StartAsync() => StartCoordinatorAsync();
    }

    private static (ReadinessCoordinator Grain, IReminderRegistry Registry) Create(IReadOnlyList<TimeSpan> backoff)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("readiness-coord", "k"));

        // StartCoordinatorAsync arms the phase timer after registering the reminder;
        // the timer registration resolves ITimerRegistry from ActivationServices, so a
        // bare substituted context would NRE. A substitute registry is enough - the
        // timer handle it returns is never used by these tests.
        context.ActivationServices.GetService(typeof(ITimerRegistry))
            .Returns(Substitute.For<ITimerRegistry>());

        var registry = Substitute.For<IReminderRegistry>();
        return (new ReadinessCoordinator(context, registry, backoff), registry);
    }

    private static Exception StillInitializing()
        => new InvalidOperationException(
            ReminderServiceReadiness.StillInitializingMarker + " and it is taking a long time. Please retry again later.",
            new TimeoutException());

    [Test]
    public void The_reminder_service_still_initialising_transient_is_classified_as_retriable()
    {
        // Premise: the classifier the fix relies on recognises the transient by its
        // message anywhere in the inner chain, and does not over-match an unrelated
        // fault.
        Assert.Multiple(() =>
        {
            Assert.That(ReminderServiceReadiness.IsStillInitializing(StillInitializing()), Is.True);
            Assert.That(
                ReminderServiceReadiness.IsStillInitializing(new InvalidOperationException("unrelated")),
                Is.False);
        });
    }

    [Test]
    public async Task StartCoordinatorAsync_waits_out_a_transient_reminder_service_initialisation()
    {
        // The regression: before the fix a single "still initializing" transient
        // propagated straight out of StartCoordinatorAsync and failed the caller's
        // operation. It must now be retried and the registration must land.
        var (grain, registry) = Create([TimeSpan.Zero]);
        var attempts = 0;
        registry.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                attempts++;
                if (attempts == 1)
                {
                    throw StillInitializing();
                }

                return Task.FromResult(Substitute.For<IGrainReminder>());
            });

        await grain.StartAsync();

        Assert.That(attempts, Is.EqualTo(2),
            "The keepalive registration must be retried past the startup-window transient and then land.");
    }

    [Test]
    public void StartCoordinatorAsync_propagates_an_unrelated_registration_fault_immediately()
    {
        // Only the "still initializing" transient is waited out. Any other fault must
        // surface unchanged, on the first attempt, without consuming a retry slot.
        var (grain, registry) = Create([TimeSpan.FromSeconds(30)]);
        var attempts = 0;
        registry.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns<Task<IGrainReminder>>(_ =>
            {
                attempts++;
                throw new InvalidOperationException("unrelated");
            });

        Assert.That(
            async () => await grain.StartAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("unrelated"));
        Assert.That(attempts, Is.EqualTo(1),
            "An unrelated fault must not be retried, so the 30 s backoff is never entered.");
    }

    [Test]
    public void StartCoordinatorAsync_rethrows_when_the_reminder_service_never_finishes_initialising()
    {
        // The durability guarantee is not silently swallowed: a transient that never
        // clears within the retry budget still surfaces with its original shape so a
        // genuinely stuck reminder service is not mistaken for a healthy start.
        var (grain, registry) = Create([]);
        registry.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns<Task<IGrainReminder>>(_ => throw StillInitializing());

        Assert.That(
            async () => await grain.StartAsync(),
            Throws.TypeOf<InvalidOperationException>()
                .With.Message.Contains(ReminderServiceReadiness.StillInitializingMarker));
    }
}
