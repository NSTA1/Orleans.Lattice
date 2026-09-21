using Microsoft.Extensions.Diagnostics.HealthChecks;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextHealthSignal"/>, the holder that carries the
/// container's own health verdict from the health-check service to the scrape.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2868. For 43 minutes this container reported <c>Health=unhealthy</c> while
/// <c>/metrics</c> answered 200 with a full scrape and every MCP call needing
/// authorization returned 500. The health check was right the whole time. Its verdict
/// simply reached no consumer, because the only externally reachable surface carried
/// no health series at all.
/// </para>
/// <para>
/// The properties defended here are therefore about <i>readability</i>, not about
/// detection: that a never-evaluated component is distinguishable from a healthy one,
/// that a failure which classifies nothing is still counted, and that the per-cause
/// arms agree with an independently maintained total - so an absence on this signal
/// is interpretable rather than merely reassuring.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextHealthSignalTests
{
    private const string Backup = "backup";

    private static RepoContextHealthSignal NewSignal()
        => new([RepoContextHealthSignal.SiloComponent, Backup, "ready"]);

    /// <summary>
    /// Every registered component is reported from construction, and reports
    /// never-evaluated rather than healthy. Defaulting to healthy would reproduce the
    /// exact false green this type exists to remove, one level up.
    /// </summary>
    [Test]
    public void Every_component_reports_never_evaluated_before_any_report_is_published()
    {
        var signal = NewSignal();

        Assert.Multiple(() =>
        {
            // Known-positive control. The assertions below are all absences, so a
            // signal holding no components at all would satisfy every one of them
            // vacuously. This proves the detector has something to report on.
            Assert.That(
                signal.Components,
                Is.EquivalentTo(new[] { RepoContextHealthSignal.SiloComponent, Backup, "ready" }),
                "control: the holder knows all three components, so the never-evaluated assertions "
                + "below are about components that exist rather than about an empty set.");

            foreach (var component in signal.Components)
            {
                var reading = signal.Read(component);
                Assert.That(
                    reading.Status,
                    Is.Null,
                    $"{component} has published no verdict, and null is what the meter renders as zero "
                    + "on all three status arms. Defaulting an unevaluated component to Healthy would "
                    + "make a container that has never run a check indistinguishable from a well one.");
                Assert.That(reading.Evaluations, Is.Zero);
            }
        });
    }

    /// <summary>
    /// The evaluation counter is what makes an all-zero status block interpretable,
    /// so it must rise on a healthy verdict too, not only on a failing one.
    /// </summary>
    [Test]
    public void A_healthy_verdict_still_counts_an_evaluation()
    {
        var signal = NewSignal();

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Healthy,
                RepoContextSiloProbeFaultCause.None))));

        var reading = signal.Read(RepoContextHealthSignal.SiloComponent);
        Assert.Multiple(() =>
        {
            Assert.That(reading.Status, Is.EqualTo(HealthStatus.Healthy));
            Assert.That(
                reading.Evaluations,
                Is.EqualTo(1),
                "a healthy verdict is still a verdict. If only failures counted, a permanently healthy "
                + "container and one whose publisher never ran would both read zero, which is the "
                + "ambiguity this counter exists to resolve.");
            Assert.That(
                signal.ReadSiloFaults().Total,
                Is.Zero,
                "adversarial arm: a successful probe must attribute no fault at all. Without this, a "
                + "classifier that reported the same cause unconditionally would pass every arm above.");
        });
    }

    /// <summary>
    /// The wedge shape of issue #2868 lands on its own arm, and on no other.
    /// </summary>
    [Test]
    public void A_wedged_probe_is_attributed_to_the_probe_deadline_arm_alone()
    {
        var signal = NewSignal();

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Unhealthy,
                RepoContextSiloProbeFaultCause.ProbeDeadline))));

        var tally = signal.ReadSiloFaults();
        Assert.Multiple(() =>
        {
            Assert.That(
                tally.ProbeDeadline,
                Is.EqualTo(1),
                "control: the arm under test fired, so the zeros below are measured rather than the "
                + "silence of a signal that recorded nothing.");
            Assert.That(tally.GrainTimeout, Is.Zero);
            Assert.That(
                tally.AccessDenied,
                Is.Zero,
                "the distinction that matters operationally: a hung tree is cleared by a restart and a "
                + "refused grant is not. Collapsing them is how a healthy box gets restarted.");
            Assert.That(tally.DrainHung, Is.Zero);
            Assert.That(tally.Unexpected, Is.Zero);
            Assert.That(tally.Total, Is.EqualTo(tally.ArmSum));
        });
    }

    private static IEnumerable<RepoContextSiloProbeFaultCause> FaultCauses()
    {
        yield return RepoContextSiloProbeFaultCause.ProbeDeadline;
        yield return RepoContextSiloProbeFaultCause.GrainTimeout;
        yield return RepoContextSiloProbeFaultCause.AccessDenied;
        yield return RepoContextSiloProbeFaultCause.DrainHung;
        yield return RepoContextSiloProbeFaultCause.Unexpected;
    }

    /// <summary>
    /// Each cause lands on the arm it names. A table, so a mapping that routed two
    /// causes to one arm fails rather than passing on whichever arm was asserted.
    /// </summary>
    /// <param name="cause">The cause the entry carries.</param>
    [TestCaseSource(nameof(FaultCauses))]
    public void Each_cause_increments_its_own_arm(RepoContextSiloProbeFaultCause cause)
    {
        var signal = NewSignal();

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(HealthStatus.Unhealthy, cause))));

        var tally = signal.ReadSiloFaults();
        var arm = cause switch
        {
            RepoContextSiloProbeFaultCause.ProbeDeadline => tally.ProbeDeadline,
            RepoContextSiloProbeFaultCause.GrainTimeout => tally.GrainTimeout,
            RepoContextSiloProbeFaultCause.AccessDenied => tally.AccessDenied,
            RepoContextSiloProbeFaultCause.DrainHung => tally.DrainHung,
            _ => tally.Unexpected,
        };

        Assert.Multiple(() =>
        {
            Assert.That(arm, Is.EqualTo(1));
            Assert.That(
                tally.ArmSum,
                Is.EqualTo(1),
                "exactly one arm moved. A cause that incremented two would still satisfy the assertion "
                + "above while double-counting on the scrape.");
            Assert.That(tally.Total, Is.EqualTo(1));
        });
    }

    /// <summary>
    /// The independently maintained total must equal the sum of the arms across a
    /// mixed sequence. This is the tally assertion that makes the <c>cause</c>
    /// dimension interpretable: an arm recorded nowhere, or recorded twice, becomes a
    /// build failure rather than a quiet discrepancy nobody reads at 3am.
    /// </summary>
    [Test]
    public void The_independent_total_equals_the_sum_of_the_arms_across_a_mixed_sequence()
    {
        var signal = NewSignal();
        var sequence = new[]
        {
            RepoContextSiloProbeFaultCause.ProbeDeadline,
            RepoContextSiloProbeFaultCause.ProbeDeadline,
            RepoContextSiloProbeFaultCause.AccessDenied,
            RepoContextSiloProbeFaultCause.None,
            RepoContextSiloProbeFaultCause.Unexpected,
            RepoContextSiloProbeFaultCause.None,
            RepoContextSiloProbeFaultCause.DrainHung,
            RepoContextSiloProbeFaultCause.GrainTimeout,
        };

        foreach (var cause in sequence)
        {
            var status = cause == RepoContextSiloProbeFaultCause.None
                ? HealthStatus.Healthy
                : HealthStatus.Unhealthy;
            signal.Publish(HealthReports.Report(
                (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(status, cause))));
        }

        var tally = signal.ReadSiloFaults();
        var reading = signal.Read(RepoContextHealthSignal.SiloComponent);
        Assert.Multiple(() =>
        {
            Assert.That(
                tally.Total,
                Is.EqualTo(6),
                "control: six of the eight publications carried a fault, so the equality below is "
                + "between two non-zero quantities rather than between two zeros.");
            Assert.That(
                tally.ArmSum,
                Is.EqualTo(tally.Total),
                "the tally assertion. The cause dimension is bounded but the invariant it rests on is "
                + "that every fault lands on exactly one arm, so an unrecorded or double-recorded arm "
                + "must fail here at build time rather than be discovered from a scrape.");
            Assert.That(
                reading.Evaluations,
                Is.EqualTo(8),
                "every publication counts, fault or not.");
            Assert.That(
                tally.Total,
                Is.LessThanOrEqualTo(reading.Evaluations),
                "faults and evaluations share the publisher's cadence, so a fault count exceeding the "
                + "evaluation count could only mean one of the two is being written from somewhere "
                + "else, and the ratio an operator reads would be meaningless.");
        });
    }

    /// <summary>
    /// A failure that classifies nothing is still counted. This is the failure mode
    /// the whole signal exists to end, so it must not be reintroduced by the signal.
    /// </summary>
    [Test]
    public void An_unclassified_failure_is_counted_as_unexpected_rather_than_dropped()
    {
        var signal = NewSignal();

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Unclassified(HealthStatus.Unhealthy))));

        var tally = signal.ReadSiloFaults();
        Assert.Multiple(() =>
        {
            Assert.That(
                tally.Unexpected,
                Is.EqualTo(1),
                "failing open. A probe that broke in a way this taxonomy does not name must still move "
                + "a series, or the change has merely relocated the silence it was meant to remove.");
            Assert.That(tally.Total, Is.EqualTo(tally.ArmSum));
            Assert.That(
                signal.Read(RepoContextHealthSignal.SiloComponent).Status,
                Is.EqualTo(HealthStatus.Unhealthy),
                "control: the verdict was recorded, so the arm above is attributing a failure that "
                + "actually reached the signal.");
        });
    }

    /// <summary>
    /// An unclassified <i>healthy</i> entry is not a fault. The fail-open above must
    /// not turn every unannotated success into an Unexpected.
    /// </summary>
    [Test]
    public void An_unclassified_healthy_entry_records_no_fault()
    {
        var signal = NewSignal();

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Unclassified(HealthStatus.Healthy))));

        Assert.Multiple(() =>
        {
            Assert.That(
                signal.ReadSiloFaults().Total,
                Is.Zero,
                "adversarial arm for the fail-open: reading a missing classification as a fault "
                + "regardless of verdict would make the fault counter track the evaluation counter "
                + "exactly, and attribute a cause to a container that is working.");
            Assert.That(
                signal.Read(RepoContextHealthSignal.SiloComponent).Evaluations,
                Is.EqualTo(1),
                "control: the entry was processed, so the zero above is a measured absence rather than "
                + "a report that was ignored.");
        });
    }

    /// <summary>
    /// Only the grain-liveness component contributes faults. Another component
    /// carrying the same data key must not be attributed to the silo probe.
    /// </summary>
    [Test]
    public void A_fault_cause_on_another_component_is_not_attributed_to_the_silo_probe()
    {
        var signal = NewSignal();

        signal.Publish(HealthReports.Report(
            (Backup, HealthReports.Entry(HealthStatus.Unhealthy, RepoContextSiloProbeFaultCause.DrainHung))));

        Assert.Multiple(() =>
        {
            Assert.That(
                signal.Read(Backup).Status,
                Is.EqualTo(HealthStatus.Unhealthy),
                "control: the backup component's verdict was recorded, so the signal did see this "
                + "report and the zero below is scoping rather than a dropped publication.");
            Assert.That(signal.ReadSiloFaults().Total, Is.Zero);
        });
    }

    /// <summary>
    /// A report naming a component this holder was not built with is ignored. Minting
    /// a series mid-flight is what the collector can refuse at a ceiling while the
    /// exposition still looks complete.
    /// </summary>
    [Test]
    public void A_report_entry_for_an_unregistered_component_is_ignored()
    {
        var signal = NewSignal();

        signal.Publish(HealthReports.Report(
            ("not-registered", HealthReports.Entry(HealthStatus.Unhealthy, RepoContextSiloProbeFaultCause.Unexpected)),
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Healthy,
                RepoContextSiloProbeFaultCause.None))));

        Assert.Multiple(() =>
        {
            Assert.That(
                signal.Components,
                Does.Not.Contain("not-registered"),
                "the component set is fixed at construction.");
            Assert.That(
                signal.Read("not-registered").Evaluations,
                Is.Zero);
            Assert.That(
                signal.Read(RepoContextHealthSignal.SiloComponent).Evaluations,
                Is.EqualTo(1),
                "control: the rest of the report was still processed, so the unknown entry was skipped "
                + "rather than the whole publication being abandoned.");
        });
    }

    /// <summary>
    /// The last verdict wins, including a recovery. A signal that latched red would
    /// report a wedge that has since cleared.
    /// </summary>
    [Test]
    public void The_latest_verdict_replaces_the_previous_one_in_both_directions()
    {
        var signal = NewSignal();

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Unhealthy,
                RepoContextSiloProbeFaultCause.ProbeDeadline))));
        var wedged = signal.Read(RepoContextHealthSignal.SiloComponent).Status;

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Healthy,
                RepoContextSiloProbeFaultCause.None))));
        var recovered = signal.Read(RepoContextHealthSignal.SiloComponent);

        Assert.Multiple(() =>
        {
            Assert.That(wedged, Is.EqualTo(HealthStatus.Unhealthy), "control: the signal did go red.");
            Assert.That(recovered.Status, Is.EqualTo(HealthStatus.Healthy));
            Assert.That(
                signal.ReadSiloFaults().ProbeDeadline,
                Is.EqualTo(1),
                "the gauge is a verdict and the counter is a history. Recovery clears the verdict and "
                + "must not rewrite the fault that happened, or a wedge that self-cleared leaves no "
                + "trace at all.");
            Assert.That(recovered.Evaluations, Is.EqualTo(2));
        });
    }

    /// <summary>Blank and duplicate component names are discarded at construction.</summary>
    [Test]
    public void Blank_and_duplicate_component_names_are_discarded()
    {
        var signal = new RepoContextHealthSignal(["silo", "silo", "  ", string.Empty, "ready"]);

        Assert.That(signal.Components, Is.EquivalentTo(new[] { "silo", "ready" }));
    }

    /// <summary>Reading a component that was never registered is not an error.</summary>
    [Test]
    public void Reading_an_unknown_component_reports_never_evaluated()
    {
        var reading = NewSignal().Read("absent");

        Assert.Multiple(() =>
        {
            Assert.That(reading.Status, Is.Null);
            Assert.That(reading.Evaluations, Is.Zero);
        });
    }

    /// <summary>Parameter validation on the public surface.</summary>
    [Test]
    public void Null_arguments_are_rejected()
        => Assert.Multiple(() =>
        {
            Assert.That(() => new RepoContextHealthSignal(null!), Throws.ArgumentNullException);
            Assert.That(() => NewSignal().Publish(null!), Throws.ArgumentNullException);
            Assert.That(() => NewSignal().Read(null!), Throws.ArgumentNullException);
            Assert.That(() => NewSignal().Read(string.Empty), Throws.ArgumentException);
        });

    /// <summary>
    /// The tally record's <see cref="RepoContextSiloProbeFaultTally.ArmSum"/> must
    /// actually sum the arms, or every tally assertion in this fixture is vacuous.
    /// </summary>
    [Test]
    public void ArmSum_sums_the_five_arms()
    {
        var tally = new RepoContextSiloProbeFaultTally(1, 2, 4, 8, 16, Total: 99);

        Assert.Multiple(() =>
        {
            Assert.That(
                tally.ArmSum,
                Is.EqualTo(31),
                "distinct powers of two, so an arm omitted or counted twice cannot land on the right "
                + "sum by coincidence.");
            Assert.That(
                tally.Total,
                Is.EqualTo(99),
                "the total is held independently and is NOT computed from the arms - which is the only "
                + "reason comparing the two proves anything.");
        });
    }
}
