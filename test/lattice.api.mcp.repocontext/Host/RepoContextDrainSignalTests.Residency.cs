using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the residency sampling and drain recording added for issue #2598.
/// </summary>
/// <remarks>
/// <para>
/// This signal has always ended its abandonment message with the observation that
/// "drain duration tracks the resident activation set, which nothing here bounds".
/// That sentence is the defect stated by the code that has it, and until now it was
/// emitted purely as commentary: nothing sampled the set, and nothing carried the
/// measured drain anywhere a later process could compare it against its own budget.
/// These tests pin both halves, and pin the constraint on both - that a reading which
/// cannot be taken, or a record which cannot be written, must cost a diagnostic and
/// never the stop sequence that was being diagnosed.
/// </para>
/// <para>
/// They also carry the regression guard for issue #2401. The exit-70 signal is the
/// only part of an abandoned drain an orchestrator can see, and it is now emitted
/// from a method that does considerably more than it used to.
/// </para>
/// </remarks>
public sealed partial class RepoContextDrainSignalTests
{
    /// <summary>An alarm that fires at once, standing in for an expired budget.</summary>
    private static Task FiresImmediately(TimeSpan budget, CancellationToken cancellationToken)
        => Task.CompletedTask;

    private static RepoContextDrainSignal SignalWithRecorder(
        List<RepoContextDrainObservation> recorded,
        Func<int?>? resident = null,
        Func<long>? timestamp = null,
        Func<TimeSpan, CancellationToken, Task>? alarm = null,
        Action<int>? reportExitCode = null)
        => new(
            NullLogger<RepoContextDrainSignal>.Instance,
            Budget,
            timestamp,
            alarm ?? NeverFires,
            reportExitCode,
            resident,
            recorded.Add);

    [Test]
    public void The_start_line_reports_the_resident_set_the_drain_has_to_get_through()
    {
        var logger = new RecordingLogger();
        var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            residentActivations: () => 12_345);

        signal.BeginDrain();

        Assert.That(logger.Lines[0], Does.Contain("12345"));
    }

    [Test]
    public void An_unreadable_resident_count_reads_as_unreadable_rather_than_as_an_empty_silo()
    {
        // Substituting a zero would turn a lost measurement into a confident claim
        // that the drain has nothing to do, which is the more expensive of the two
        // mistakes by a wide margin.
        var logger = new RecordingLogger();
        var signal = new RepoContextDrainSignal(logger, Budget, residentActivations: () => null);

        signal.BeginDrain();

        Assert.Multiple(() =>
        {
            Assert.That(logger.Lines[0], Does.Contain("unreadable"));
            Assert.That(logger.Lines[0], Does.Not.Contain("0 resident"));
        });
    }

    [Test]
    public void A_throwing_residency_probe_does_not_disturb_the_drain_it_was_measuring()
    {
        var logger = new RecordingLogger();
        var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            residentActivations: () => throw new InvalidOperationException("probe fault"));

        Assert.That(signal.BeginDrain, Throws.Nothing);
        Assert.That(signal.IsDraining, Is.True, "the drain must proceed whether or not it can be measured");
    }

    [Test]
    public void A_drain_records_a_start_marker_before_it_does_any_work()
    {
        // Written first on purpose. A process killed part-way then leaves a record
        // saying a drain began and never saying how it ended, and that record is the
        // only evidence available from inside a container that its real grace period
        // was smaller than the drain needed.
        var recorded = new List<RepoContextDrainObservation>();
        var signal = SignalWithRecorder(recorded, resident: () => 500);

        signal.BeginDrain();

        Assert.That(recorded, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(recorded[0].Outcome, Is.EqualTo(RepoContextDrainOutcome.Started));
            Assert.That(recorded[0].Duration, Is.Null, "nothing has been measured yet and nothing may be invented");
            Assert.That(recorded[0].ResidentActivations, Is.EqualTo(500));
            Assert.That(recorded[0].Budget, Is.EqualTo(Budget));
        });
    }

    [Test]
    public void A_completed_drain_replaces_the_start_marker_with_its_measured_duration()
    {
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(41.5)));
        var recorded = new List<RepoContextDrainObservation>();
        var signal = SignalWithRecorder(recorded, resident: () => 900, timestamp: clock.Next);

        signal.BeginDrain();
        signal.CompleteDrain();

        Assert.That(recorded, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(recorded[1].Outcome, Is.EqualTo(RepoContextDrainOutcome.Completed));
            Assert.That(recorded[1].Duration!.Value.TotalSeconds, Is.EqualTo(41.5).Within(0.05));
            Assert.That(
                recorded[1].ResidentActivations,
                Is.EqualTo(900),
                "the residency recorded is the set the drain got through, sampled when it began");
        });
    }

    [Test]
    public void An_abandoned_drain_records_the_abandonment_so_the_next_start_knows_the_budget_was_short()
    {
        var recorded = new List<RepoContextDrainObservation>();
        var signal = SignalWithRecorder(
            recorded,
            resident: () => 10_000,
            timestamp: new FakeClock(0, TicksFor(TimeSpan.FromSeconds(90))).Next,
            alarm: FiresImmediately);

        signal.BeginDrain();
        SpinWait.SpinUntil(() => recorded.Count > 1, TimeSpan.FromSeconds(5));

        Assert.That(recorded, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(recorded[1].Outcome, Is.EqualTo(RepoContextDrainOutcome.Abandoned));
            Assert.That(recorded[1].Duration, Is.Not.Null);
        });
    }

    [Test]
    public void A_completion_after_an_abandonment_supersedes_it_with_the_more_accurate_measurement()
    {
        // The alarm can only ever report the budget it fired at; the completion path
        // reports what the drain actually took. The second is the number a grace
        // period should be derived from, so it must win.
        var clock = new FakeClock(0, TicksFor(TimeSpan.FromSeconds(90)), TicksFor(TimeSpan.FromSeconds(118.4)));
        var recorded = new List<RepoContextDrainObservation>();
        var signal = SignalWithRecorder(
            recorded,
            resident: () => 10_000,
            timestamp: clock.Next,
            alarm: FiresImmediately);

        signal.BeginDrain();
        SpinWait.SpinUntil(() => recorded.Count > 1, TimeSpan.FromSeconds(5));
        signal.CompleteDrain();

        Assert.That(recorded, Has.Count.EqualTo(3));
        Assert.Multiple(() =>
        {
            Assert.That(recorded[2].Outcome, Is.EqualTo(RepoContextDrainOutcome.Abandoned));
            Assert.That(
                recorded[2].Duration!.Value.TotalSeconds,
                Is.EqualTo(118.4).Within(0.05),
                "the drain did not stop at the budget just because the host stopped waiting for it");
        });
    }

    [Test]
    public void A_repeated_completion_records_nothing_new_so_a_completed_drain_stays_completed()
    {
        // The completion record latches. Without that, a duplicate lifetime callback
        // could leave a second, contradictory record and the next start would report
        // against whichever arrived last.
        var recorded = new List<RepoContextDrainObservation>();
        var signal = SignalWithRecorder(
            recorded,
            resident: () => 10,
            timestamp: new FakeClock(0, TicksFor(TimeSpan.FromSeconds(5))).Next);

        signal.BeginDrain();
        signal.CompleteDrain();
        var afterCompletion = recorded.Count;

        signal.CompleteDrain();

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Count.EqualTo(afterCompletion), "a repeated transition records nothing new");
            Assert.That(recorded[^1].Outcome, Is.EqualTo(RepoContextDrainOutcome.Completed));
        });
    }

    [Test]
    public void A_throwing_recorder_does_not_disturb_the_shutdown_it_was_recording()
    {
        var signal = new RepoContextDrainSignal(
            NullLogger<RepoContextDrainSignal>.Instance,
            Budget,
            recordObservation: _ => throw new IOException("disk full"));

        Assert.Multiple(() =>
        {
            Assert.That(signal.BeginDrain, Throws.Nothing);
            Assert.That(signal.CompleteDrain, Throws.Nothing);
            Assert.That(signal.HasCompleted, Is.True);
        });
    }

    [Test]
    public void A_signal_with_no_recorder_behaves_exactly_as_it_did_before()
    {
        // Both new dependencies default to absent, which is what keeps this change
        // inert for every fixture and every caller that does not opt in.
        var signal = new RepoContextDrainSignal(NullLogger<RepoContextDrainSignal>.Instance, Budget);

        signal.BeginDrain();
        signal.CompleteDrain();

        Assert.That(signal.HasCompleted, Is.True);
    }

    [Test]
    public void The_abandonment_message_names_the_activations_stranded_at_the_instant_the_host_stopped_waiting()
    {
        // The count sampled at the overrun is the set STILL resident, which is the
        // loss the message could previously only assert the existence of.
        var logger = new LevelRecordingLogger();
        var counts = new Queue<int?>([10_000, 3_400]);
        var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            new FakeClock(0, TicksFor(TimeSpan.FromSeconds(90))).Next,
            FiresImmediately,
            residentActivations: () => counts.Count > 0 ? counts.Dequeue() : 0);

        signal.BeginDrain();
        SpinWait.SpinUntil(() => logger.Lines.Any(l => l.Message.Contains("ABANDONED")), TimeSpan.FromSeconds(5));

        var abandoned = logger.Lines.Single(l => l.Message.Contains("ABANDONED")).Message;
        Assert.Multiple(() =>
        {
            Assert.That(abandoned, Does.Contain("3400 activations were still resident"));
            Assert.That(abandoned, Does.Contain("out of 10000 resident when the drain began"));
        });
    }

    [Test]
    public void The_abandonment_message_says_the_count_is_unreadable_rather_than_claiming_nothing_was_lost()
    {
        var logger = new LevelRecordingLogger();
        var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            new FakeClock(0, TicksFor(TimeSpan.FromSeconds(90))).Next,
            FiresImmediately,
            residentActivations: () => null);

        signal.BeginDrain();
        SpinWait.SpinUntil(() => logger.Lines.Any(l => l.Message.Contains("ABANDONED")), TimeSpan.FromSeconds(5));

        Assert.That(
            logger.Lines.Single(l => l.Message.Contains("ABANDONED")).Message,
            Does.Contain("the resident count could not be read"));
    }

    [Test]
    public void The_abandonment_message_names_the_grant_derived_from_this_drain_rather_than_advising_a_raise()
    {
        var logger = new LevelRecordingLogger();
        var signal = new RepoContextDrainSignal(
            logger,
            Budget,
            new FakeClock(0, TicksFor(TimeSpan.FromSeconds(102.1))).Next,
            FiresImmediately);

        signal.BeginDrain();
        SpinWait.SpinUntil(() => logger.Lines.Any(l => l.Message.Contains("ABANDONED")), TimeSpan.FromSeconds(5));

        var abandoned = logger.Lines.Single(l => l.Message.Contains("ABANDONED")).Message;
        Assert.Multiple(() =>
        {
            Assert.That(abandoned, Does.Contain("137s"), "the grant this drain actually needed");
            Assert.That(
                abandoned,
                Does.Contain("Raising only the declaration buys no drain time"),
                "issue #2598's central trap must stay stated where the failure is read");
        });
    }

    [Test]
    public void An_abandoned_drain_still_reports_exit_code_seventy_once()
    {
        // Regression guard for issue #2401. The exit code is the only part of an
        // abandoned drain an orchestrator sees, and it is now reported from a method
        // that also samples residency and writes a record - either of which could
        // have thrown past it had they not been guarded.
        var reported = new List<int>();
        var signal = new RepoContextDrainSignal(
            NullLogger<RepoContextDrainSignal>.Instance,
            Budget,
            new FakeClock(0, TicksFor(TimeSpan.FromSeconds(90))).Next,
            FiresImmediately,
            reported.Add,
            residentActivations: () => throw new InvalidOperationException("probe fault"),
            recordObservation: _ => throw new IOException("disk full"));

        signal.BeginDrain();
        SpinWait.SpinUntil(() => reported.Count > 0, TimeSpan.FromSeconds(5));

        Assert.That(reported, Is.EqualTo(new[] { RepoContextExitCode.DrainAbandoned }));
    }
}
