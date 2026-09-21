using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the bootstrap coverage-probe instrument (issue #2964), which makes
/// the store's read-path access gate visible when it stands an ingestion arm down.
/// <para>
/// The instrument exists because a gate-pruned stand-down is silent BY DESIGN: the
/// probe answered, the gate did exactly its job, the pass continued, and nothing in
/// the code path looks wrong at the point the signal is lost. There is no error, no
/// fault, and no warning an operator has any reason to expect - so "did less work
/// because there was less to do" and "did less work because it was not permitted to
/// see the work" render identically on the scrape.
/// </para>
/// <para>
/// The pre-minting assertions below are the load-bearing ones. They are what make an
/// all-zero reading mean "the seam was reached and nothing was pruned" rather than
/// "this build does not have the instrument", and they are asserted through a
/// <see cref="MeterListener"/> rather than through the reporter's own snapshot,
/// because a snapshot is an array that always has nine slots - it cannot tell an
/// absent series from a zero one, which is the exact distinction under test.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextCoverageProbeReporterTests
{
    private sealed record ProbeMeasurement(long Value, string? Arm, string? Outcome);

    /// <summary>
    /// Starts a listener over the coverage-probe instrument, matched by meter and
    /// instrument name.
    /// <para>
    /// Matching by name rather than by instrument reference is required here, not
    /// merely convenient: the meter is a per-instance field created inside the
    /// reporter's own constructor, which is also where the zero-prime happens, so
    /// there is no reference to pass and no way to have a listener running before the
    /// instrument exists other than by name. A listener started afterwards would miss
    /// every priming sample and the test would assert nothing.
    /// </para>
    /// </summary>
    private static MeterListener ListenForProbes(List<ProbeMeasurement> sink)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (string.Equals(instrument.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal)
                    && string.Equals(instrument.Name, RepoContextCoverageProbeReporter.ProbeInstrumentName, StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? arm = null;
            string? outcome = null;
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, RepoContextCoverageProbeReporter.ArmTagKey, StringComparison.Ordinal))
                {
                    arm = tag.Value?.ToString();
                }
                else if (string.Equals(tag.Key, RepoContextCoverageProbeReporter.OutcomeTagKey, StringComparison.Ordinal))
                {
                    outcome = tag.Value?.ToString();
                }
            }

            lock (sink)
            {
                sink.Add(new ProbeMeasurement(value, arm, outcome));
            }
        });

        listener.Start();
        return listener;
    }

    private static List<ProbeMeasurement> Snapshot(List<ProbeMeasurement> sink)
    {
        lock (sink)
        {
            return [.. sink];
        }
    }

    private static readonly string[] AllArms =
    [
        RepoContextCoverageProbeReporter.ArmFileTag,
        RepoContextCoverageProbeReporter.ArmSymbolTag,
        RepoContextCoverageProbeReporter.ArmSweepTag,
    ];

    private static readonly string[] AllOutcomes =
    [
        RepoContextCoverageProbeReporter.OutcomeConclusiveTag,
        RepoContextCoverageProbeReporter.OutcomeGatePrunedTag,
        RepoContextCoverageProbeReporter.OutcomeProbeFailedTag,
    ];

    /// <summary>
    /// Constructing the reporter mints all nine series at zero, before any ingestion
    /// has run and therefore before any early return on any arm can be taken.
    /// <para>
    /// This is the regression guard the issue asks for. If the priming were ever moved
    /// down onto the paths that charge the arms - which is the natural-looking
    /// refactor, since that is where the tags already are - then an arm whose path was
    /// never taken would have no series at all, and its absence would be
    /// indistinguishable from a build that shipped without the instrument. A test that
    /// merely observed the counters after a successful pass would pass either way and
    /// would not catch it.
    /// </para>
    /// </summary>
    [Test]
    public void Constructing_the_reporter_pre_mints_every_arm_and_outcome_at_zero()
    {
        var sink = new List<ProbeMeasurement>();
        using var listener = ListenForProbes(sink);

        using var reporter = new RepoContextCoverageProbeReporter();

        var measurements = Snapshot(sink);

        Assert.Multiple(() =>
        {
            foreach (var arm in AllArms)
            {
                foreach (var outcome in AllOutcomes)
                {
                    Assert.That(
                        measurements.Any(m =>
                            m.Value == 0
                            && string.Equals(m.Arm, arm, StringComparison.Ordinal)
                            && string.Equals(m.Outcome, outcome, StringComparison.Ordinal)),
                        Is.True,
                        $"arm={arm} outcome={outcome} must be minted at zero by the constructor, so that its "
                        + "absence from a scrape means the build lacks the instrument rather than that this "
                        + "outcome did not occur.");
                }
            }

            Assert.That(
                measurements,
                Has.Count.EqualTo(AllArms.Length * AllOutcomes.Length),
                "and construction emits exactly the nine priming samples and nothing else, so a later "
                + "assertion that an arm was charged cannot be satisfied by the priming.");
        });
    }

    /// <summary>
    /// Recording an outcome charges exactly that one series and leaves the other eight
    /// alone. Without this, an arm that quietly incremented its siblings would satisfy
    /// every "the arm advanced" assertion elsewhere while making the partition
    /// meaningless.
    /// </summary>
    [Test]
    public void Recording_charges_exactly_the_named_arm_and_outcome()
    {
        Assert.Multiple(() =>
        {
            foreach (var arm in Enum.GetValues<RepoContextCoverageProbeArm>())
            {
                foreach (var outcome in Enum.GetValues<RepoContextCoverageProbeOutcome>())
                {
                    AssertChargesExactlyOne(arm, outcome);
                }
            }
        });
    }

    private static void AssertChargesExactlyOne(
        RepoContextCoverageProbeArm arm,
        RepoContextCoverageProbeOutcome outcome)
    {
        using var reporter = new RepoContextCoverageProbeReporter();
        var before = reporter.Snapshot();

        reporter.Record(arm, outcome);

        var after = reporter.Snapshot();

        Assert.That(
            after.Count(arm, outcome) - before.Count(arm, outcome),
            Is.EqualTo(1),
            $"arm={arm} outcome={outcome} advances by one,");

        foreach (var otherArm in Enum.GetValues<RepoContextCoverageProbeArm>())
        {
            foreach (var otherOutcome in Enum.GetValues<RepoContextCoverageProbeOutcome>())
            {
                if (otherArm == arm && otherOutcome == outcome)
                {
                    continue;
                }

                Assert.That(
                    after.Count(otherArm, otherOutcome) - before.Count(otherArm, otherOutcome),
                    Is.Zero,
                    $"and recording arm={arm} outcome={outcome} leaves arm={otherArm} "
                    + $"outcome={otherOutcome} alone.");
            }
        }
    }

    /// <summary>
    /// A default snapshot reads zero rather than throwing. Callers hold the reporter
    /// as a nullable optional dependency, so a default struct is reachable from any
    /// host that registered none, and a throw there would turn an absent instrument
    /// into a crashed ingest pass.
    /// </summary>
    [Test]
    public void A_default_snapshot_reads_zero_for_every_series()
    {
        var snapshot = default(RepoContextCoverageProbeSnapshot);

        Assert.Multiple(() =>
        {
            foreach (var arm in Enum.GetValues<RepoContextCoverageProbeArm>())
            {
                foreach (var outcome in Enum.GetValues<RepoContextCoverageProbeOutcome>())
                {
                    Assert.That(snapshot.Count(arm, outcome), Is.Zero);
                }
            }
        });
    }
}
