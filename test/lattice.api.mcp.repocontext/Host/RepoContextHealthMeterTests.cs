using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextHealthMeter"/>, which puts the container's own health
/// verdict onto the <c>/metrics</c> scrape that every dashboard, alert rule and probe
/// is already wired to.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2868 recorded 43 minutes during which <c>docker inspect</c> reported
/// <c>Health=unhealthy</c> - correctly - while <c>/metrics</c> answered 200 with a
/// complete scrape and MCP returned 500 to every call needing authorization. The
/// health signal and the service signal disagreed and the reassuring one was the only
/// one anything could read.
/// </para>
/// <para>
/// The property under test is not that three instruments exist. It is that a zero on
/// any of them is <i>interpretable</i>: every component-by-status arm and every fault
/// cause is published from construction, so a zero is a measurement; and the
/// evaluation counter denominates the status gauge, so an all-zero status block can be
/// told apart from a healthy one.
/// </para>
/// </remarks>
// NonParallelizable because a MeterListener is process-wide: a sibling fixture
// publishing on the same host meter name would land foreign measurements here.
[TestFixture]
[NonParallelizable]
public sealed class RepoContextHealthMeterTests
{
    private const string Backup = "backup";

    private static RepoContextHealthSignal NewSignal()
        => new([RepoContextHealthSignal.SiloComponent, Backup]);

    /// <summary>
    /// Every component-by-status arm and every fault cause is published from
    /// construction, before any verdict exists. An unprimed series and a measured zero
    /// are byte-identical on a scrape, so an alert resting on an unproven zero is
    /// worth nothing.
    /// </summary>
    [Test]
    public void Every_status_arm_and_every_fault_cause_is_published_before_any_verdict()
    {
        var signal = NewSignal();
        using var observed = new HealthMeterObserver();
        using var meter = new RepoContextHealthMeter(signal);

        observed.Sample();

        Assert.Multiple(() =>
        {
            // Known-positive control. Every assertion below is that a series exists
            // carrying zero, which a listener that saw nothing at all would fail, but
            // an assertion written the other way round would not.
            Assert.That(
                observed.SeriesCount,
                Is.EqualTo((2 * 3) + 2 + 5),
                "control: two components times three status arms, plus one evaluation counter per "
                + "component, plus all five fault causes. A count short of this means an arm was "
                + "created lazily and would be absent from the scrape during the very window an alert "
                + "is meant to cover.");

            foreach (var component in new[] { RepoContextHealthSignal.SiloComponent, Backup })
            {
                foreach (var status in new[]
                {
                    RepoContextHealthMeter.StatusHealthyTag,
                    RepoContextHealthMeter.StatusDegradedTag,
                    RepoContextHealthMeter.StatusUnhealthyTag,
                })
                {
                    Assert.That(
                        observed.Status(component, status),
                        Is.Zero,
                        $"{component}/{status} exists and carries zero.");
                }

                Assert.That(observed.Evaluations(component), Is.Zero);
            }

            foreach (var cause in AllCauseTags())
            {
                Assert.That(
                    observed.Faults(cause),
                    Is.Zero,
                    $"the {cause} arm is primed. A cause series that first appeared on a first "
                    + "occurrence would be missing for exactly as long as nothing had gone wrong.");
            }
        });
    }

    /// <summary>
    /// The reading that makes the whole signal usable: never-evaluated is all-zero on
    /// the status gauge, and only the evaluation counter tells it apart from healthy.
    /// </summary>
    [Test]
    public void Never_evaluated_is_told_apart_from_healthy_only_by_the_evaluation_counter()
    {
        var signal = NewSignal();
        using var observed = new HealthMeterObserver();
        using var meter = new RepoContextHealthMeter(signal);

        observed.Sample();
        var beforeHealthy = observed.Status(RepoContextHealthSignal.SiloComponent, RepoContextHealthMeter.StatusHealthyTag);
        var beforeDegraded = observed.Status(RepoContextHealthSignal.SiloComponent, RepoContextHealthMeter.StatusDegradedTag);
        var beforeUnhealthy = observed.Status(RepoContextHealthSignal.SiloComponent, RepoContextHealthMeter.StatusUnhealthyTag);
        var beforeEvaluations = observed.Evaluations(RepoContextHealthSignal.SiloComponent);

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Healthy,
                RepoContextSiloProbeFaultCause.None))));
        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(beforeHealthy, Is.Zero);
            Assert.That(beforeDegraded, Is.Zero);
            Assert.That(
                beforeUnhealthy,
                Is.Zero,
                "a container that has evaluated nothing is not unhealthy either. The status gauge "
                + "cannot express 'unknown', which is precisely why the counter beside it must.");
            Assert.That(beforeEvaluations, Is.Zero);

            // The known-positive control for the four zeros above: the same detector,
            // shown firing. Without this, a meter that reported nothing at all would
            // satisfy every absence assertion in this test.
            Assert.That(
                observed.Evaluations(RepoContextHealthSignal.SiloComponent),
                Is.EqualTo(1),
                "control: after one publication the counter moves, so the zeros above were measured "
                + "rather than the silence of a meter nobody observed.");
            Assert.That(
                observed.Status(RepoContextHealthSignal.SiloComponent, RepoContextHealthMeter.StatusHealthyTag),
                Is.EqualTo(1),
                "and the verdict is now legible on the scrape, which is the entire change: the only "
                + "surface anything reads now carries the answer the health log had all along.");
        });
    }

    /// <summary>
    /// The verdict is one-hot: exactly the current status carries 1 and the other two
    /// carry 0. A gauge that latched every status it had ever seen would read as a
    /// container that is simultaneously healthy and wedged.
    /// </summary>
    [Test]
    public void The_current_verdict_is_the_only_status_arm_carrying_one()
    {
        var signal = NewSignal();
        using var observed = new HealthMeterObserver();
        using var meter = new RepoContextHealthMeter(signal);

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Healthy,
                RepoContextSiloProbeFaultCause.None))));
        observed.Sample();

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Unhealthy,
                RepoContextSiloProbeFaultCause.ProbeDeadline))));
        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Status(RepoContextHealthSignal.SiloComponent, RepoContextHealthMeter.StatusUnhealthyTag),
                Is.EqualTo(1),
                "control: the arm an operator alerts on fired.");
            Assert.That(
                observed.Status(RepoContextHealthSignal.SiloComponent, RepoContextHealthMeter.StatusHealthyTag),
                Is.Zero,
                "the earlier healthy verdict must be cleared, not accumulated. A stuck 1 here is the "
                + "reassuring signal winning again, which is the whole defect.");
            Assert.That(
                observed.Status(RepoContextHealthSignal.SiloComponent, RepoContextHealthMeter.StatusDegradedTag),
                Is.Zero);
        });
    }

    /// <summary>
    /// A component that publishes no verdict is unaffected by another component's.
    /// The gauge is per-component, so a wedged silo must not colour the rest red or,
    /// worse, a healthy backup colour the silo green.
    /// </summary>
    [Test]
    public void One_components_verdict_does_not_move_another_components_arms()
    {
        var signal = NewSignal();
        using var observed = new HealthMeterObserver();
        using var meter = new RepoContextHealthMeter(signal);

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Unhealthy,
                RepoContextSiloProbeFaultCause.ProbeDeadline))));
        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Status(RepoContextHealthSignal.SiloComponent, RepoContextHealthMeter.StatusUnhealthyTag),
                Is.EqualTo(1),
                "control: the silo went red.");
            Assert.That(
                observed.Status(Backup, RepoContextHealthMeter.StatusUnhealthyTag),
                Is.Zero);
            Assert.That(
                observed.Evaluations(Backup),
                Is.Zero,
                "and the untouched component still reads never-evaluated rather than inheriting the "
                + "silo's publication count.");
        });
    }

    /// <summary>
    /// The wedge lands on its own cause arm and leaves the rest at a measured zero.
    /// </summary>
    [Test]
    public void A_wedged_probe_raises_only_the_probe_deadline_cause_arm()
    {
        var signal = NewSignal();
        using var observed = new HealthMeterObserver();
        using var meter = new RepoContextHealthMeter(signal);

        signal.Publish(HealthReports.Report(
            (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                HealthStatus.Unhealthy,
                RepoContextSiloProbeFaultCause.ProbeDeadline))));
        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Faults(RepoContextHealthMeter.CauseProbeDeadlineTag),
                Is.EqualTo(1),
                "control: the arm fired.");
            Assert.That(observed.Faults(RepoContextHealthMeter.CauseGrainTimeoutTag), Is.Zero);
            Assert.That(observed.Faults(RepoContextHealthMeter.CauseAccessDeniedTag), Is.Zero);
            Assert.That(observed.Faults(RepoContextHealthMeter.CauseDrainHungTag), Is.Zero);
            Assert.That(observed.Faults(RepoContextHealthMeter.CauseUnexpectedTag), Is.Zero);
        });
    }

    /// <summary>
    /// Adversarial arm: a healthy container must raise no cause at all. Without this,
    /// a meter that reported a constant cause would satisfy every positive arm above.
    /// </summary>
    [Test]
    public void A_healthy_container_raises_no_cause_arm_at_all()
    {
        var signal = NewSignal();
        using var observed = new HealthMeterObserver();
        using var meter = new RepoContextHealthMeter(signal);

        for (var i = 0; i < 3; i++)
        {
            signal.Publish(HealthReports.Report(
                (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(
                    HealthStatus.Healthy,
                    RepoContextSiloProbeFaultCause.None))));
        }

        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Evaluations(RepoContextHealthSignal.SiloComponent),
                Is.EqualTo(3),
                "control: three evaluations happened, so the cause zeros below are measured absences "
                + "and not an unobserved meter.");
            foreach (var cause in AllCauseTags())
            {
                Assert.That(observed.Faults(cause), Is.Zero, $"{cause} must stay at zero.");
            }
        });
    }

    /// <summary>
    /// The tally assertion, at the scrape. The five cause arms on the metric must sum
    /// to the independently maintained total the signal holds, so a bounded dimension
    /// that is deliberately read as attribution is anchored by a quantity that is not
    /// derived from it.
    /// </summary>
    [Test]
    public void The_cause_arms_on_the_scrape_sum_to_the_independently_held_total()
    {
        var signal = NewSignal();
        using var observed = new HealthMeterObserver();
        using var meter = new RepoContextHealthMeter(signal);

        foreach (var cause in new[]
        {
            RepoContextSiloProbeFaultCause.ProbeDeadline,
            RepoContextSiloProbeFaultCause.AccessDenied,
            RepoContextSiloProbeFaultCause.ProbeDeadline,
            RepoContextSiloProbeFaultCause.DrainHung,
            RepoContextSiloProbeFaultCause.GrainTimeout,
            RepoContextSiloProbeFaultCause.Unexpected,
        })
        {
            signal.Publish(HealthReports.Report(
                (RepoContextHealthSignal.SiloComponent, HealthReports.Entry(HealthStatus.Unhealthy, cause))));
        }

        observed.Sample();
        var scraped = AllCauseTags().Sum(observed.Faults);

        Assert.Multiple(() =>
        {
            Assert.That(scraped, Is.EqualTo(6), "control: six faults reached the scrape.");
            Assert.That(
                scraped,
                Is.EqualTo(signal.ReadSiloFaults().Total),
                "the exported arms and the independently held total agree, so the per-cause breakdown "
                + "on the scrape can be trusted to account for every fault rather than for the subset "
                + "somebody remembered to export.");
            Assert.That(
                scraped,
                Is.LessThanOrEqualTo(observed.Evaluations(RepoContextHealthSignal.SiloComponent)),
                "faults share the publisher's cadence with evaluations, so this ratio is meaningful.");
        });
    }

    /// <summary>
    /// Every cause renders to a distinct, stable tag. A collision would silently merge
    /// two arms whose remedies differ, which is the distinction the taxonomy exists for.
    /// </summary>
    [Test]
    public void Every_named_cause_renders_to_a_distinct_tag()
    {
        var named = new[]
        {
            RepoContextSiloProbeFaultCause.ProbeDeadline,
            RepoContextSiloProbeFaultCause.GrainTimeout,
            RepoContextSiloProbeFaultCause.AccessDenied,
            RepoContextSiloProbeFaultCause.DrainHung,
            RepoContextSiloProbeFaultCause.Unexpected,
        };

        var tags = named.Select(RepoContextHealthMeter.DescribeCause).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(tags, Is.Unique);
            Assert.That(tags, Has.Length.EqualTo(5), "control: five causes were rendered.");
            Assert.That(
                RepoContextHealthMeter.DescribeCause((RepoContextSiloProbeFaultCause)999),
                Is.EqualTo(RepoContextHealthMeter.CauseUnexpectedTag),
                "failing open. An enum member added without a tag must still be exported, because the "
                + "observation callback throwing would silence the whole instrument.");
        });
    }

    /// <summary>Each verdict renders to its own distinct tag.</summary>
    [Test]
    public void Every_status_renders_to_a_distinct_tag()
        => Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextHealthMeter.DescribeStatus(HealthStatus.Healthy),
                Is.EqualTo(RepoContextHealthMeter.StatusHealthyTag));
            Assert.That(
                RepoContextHealthMeter.DescribeStatus(HealthStatus.Degraded),
                Is.EqualTo(RepoContextHealthMeter.StatusDegradedTag));
            Assert.That(
                RepoContextHealthMeter.DescribeStatus(HealthStatus.Unhealthy),
                Is.EqualTo(RepoContextHealthMeter.StatusUnhealthyTag));
            Assert.That(
                new[]
                {
                    RepoContextHealthMeter.StatusHealthyTag,
                    RepoContextHealthMeter.StatusDegradedTag,
                    RepoContextHealthMeter.StatusUnhealthyTag,
                },
                Is.Unique);
        });

    /// <summary>
    /// The instruments carry the names the documentation and dashboards reference.
    /// </summary>
    [Test]
    public void The_instrument_names_are_the_ones_the_runbook_names()
        => Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextHealthMeter.StatusGaugeName,
                Is.EqualTo("lattice_repocontext_health_status"));
            Assert.That(
                RepoContextHealthMeter.EvaluationsCounterName,
                Is.EqualTo("lattice_repocontext_health_evaluations_total"));
            Assert.That(
                RepoContextHealthMeter.SiloProbeFaultsCounterName,
                Is.EqualTo("lattice_repocontext_silo_probe_faults_total"));
        });

    /// <summary>Parameter validation.</summary>
    [Test]
    public void A_null_signal_is_rejected()
        => Assert.That(() => new RepoContextHealthMeter(null!), Throws.ArgumentNullException);

    private static string[] AllCauseTags() =>
    [
        RepoContextHealthMeter.CauseProbeDeadlineTag,
        RepoContextHealthMeter.CauseGrainTimeoutTag,
        RepoContextHealthMeter.CauseAccessDeniedTag,
        RepoContextHealthMeter.CauseDrainHungTag,
        RepoContextHealthMeter.CauseUnexpectedTag,
    ];

    /// <summary>
    /// A tag-aware listener over the three health instruments on the host meter.
    /// </summary>
    private sealed class HealthMeterObserver : IDisposable
    {
        private static readonly string[] Wanted =
        [
            RepoContextHealthMeter.StatusGaugeName,
            RepoContextHealthMeter.EvaluationsCounterName,
            RepoContextHealthMeter.SiloProbeFaultsCounterName,
        ];

        private readonly Dictionary<string, long> _values = new(StringComparer.Ordinal);
        private readonly MeterListener _listener = new();

        public HealthMeterObserver()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextHostMeter.Name
                    && Wanted.Contains(instrument.Name, StringComparer.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<int>((instrument, measurement, tags, _) =>
                Record(instrument.Name, tags, measurement));
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
                Record(instrument.Name, tags, measurement));
            _listener.Start();
        }

        public int SeriesCount
        {
            get
            {
                lock (_values)
                {
                    return _values.Count;
                }
            }
        }

        public void Sample() => _listener.RecordObservableInstruments();

        public long Status(string component, string status) => Read(
            RepoContextHealthMeter.StatusGaugeName,
            $"{RepoContextHealthMeter.ComponentTagKey}={component}",
            $"{RepoContextHealthMeter.StatusTagKey}={status}");

        public long Evaluations(string component) => Read(
            RepoContextHealthMeter.EvaluationsCounterName,
            $"{RepoContextHealthMeter.ComponentTagKey}={component}");

        public long Faults(string cause) => Read(
            RepoContextHealthMeter.SiloProbeFaultsCounterName,
            $"{RepoContextHealthMeter.CauseTagKey}={cause}");

        public void Dispose() => _listener.Dispose();

        private void Record(string instrument, ReadOnlySpan<KeyValuePair<string, object?>> tags, long value)
        {
            var parts = new string[tags.Length];
            for (var i = 0; i < tags.Length; i++)
            {
                parts[i] = $"{tags[i].Key}={tags[i].Value}";
            }

            Array.Sort(parts, StringComparer.Ordinal);
            var key = instrument + "|" + string.Join(",", parts);
            lock (_values)
            {
                _values[key] = value;
            }
        }

        private long Read(string instrument, params string[] tags)
        {
            var parts = tags.ToArray();
            Array.Sort(parts, StringComparer.Ordinal);
            var key = instrument + "|" + string.Join(",", parts);
            lock (_values)
            {
                return _values.TryGetValue(key, out var value)
                    ? value
                    : throw new AssertionException(
                        $"no series '{key}' was observed. An absent series is NOT a zero: it is the "
                        + "failure mode this meter exists to remove, so the fixture refuses to read it "
                        + "as one.");
            }
        }
    }
}
