using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Unit tests for <see cref="RepoContextCoverageVerdictReporter"/> - the classifier
/// that decides why a pass did or did not observe converged embedding coverage, and
/// the counter that publishes that reason.
/// <para>
/// The classifier is the load-bearing half. Issue #3340 was caused by collapsing
/// "the probe was refused" into "a gap was measured", so the precedence between the
/// four verdicts is the behaviour under test, not an implementation detail.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextCoverageVerdictReporterTests
{
    private static readonly RepoContextCoverageVerdict[] AllVerdicts =
    [
        RepoContextCoverageVerdict.Converged,
        RepoContextCoverageVerdict.ArmFailure,
        RepoContextCoverageVerdict.GapFound,
        RepoContextCoverageVerdict.ProbeUnmeasurable,
    ];

    [Test]
    public void Classify_a_faulted_arm_reports_arm_failure_whatever_the_outcome_said()
    {
        // An arm that threw did not complete, so every coverage fact this pass
        // produced is inadmissible - including a clean-looking one. Arm failure
        // therefore outranks the outcome rather than being read alongside it.
        var verdict = RepoContextCoverageVerdictReporter.Classify(
            armFaulted: true, new RepoFileVectorIngestOutcome(0, 0, CoverageEstablished: true));

        Assert.That(verdict, Is.EqualTo(RepoContextCoverageVerdict.ArmFailure));
    }

    [Test]
    public void Classify_a_clean_probe_with_no_gaps_reports_converged()
    {
        var verdict = RepoContextCoverageVerdictReporter.Classify(
            armFaulted: false, new RepoFileVectorIngestOutcome(3, 0, CoverageEstablished: true));

        Assert.That(verdict, Is.EqualTo(RepoContextCoverageVerdict.Converged));
    }

    [Test]
    public void Classify_a_measured_hole_reports_gap_found()
    {
        var verdict = RepoContextCoverageVerdictReporter.Classify(
            armFaulted: false, new RepoFileVectorIngestOutcome(0, 7, CoverageEstablished: true));

        Assert.That(verdict, Is.EqualTo(RepoContextCoverageVerdict.GapFound));
    }

    [Test]
    public void Classify_an_unestablished_probe_reports_probe_unmeasurable()
    {
        // The live failure mode: the store refused the membership probe under
        // saturation. Zero selected gaps here means "never asked", not "none".
        var verdict = RepoContextCoverageVerdictReporter.Classify(
            armFaulted: false, new RepoFileVectorIngestOutcome(0, 0, CoverageEstablished: false));

        Assert.That(verdict, Is.EqualTo(RepoContextCoverageVerdict.ProbeUnmeasurable));
    }

    [Test]
    public void Classify_a_deferred_pass_reports_probe_unmeasurable()
    {
        var verdict = RepoContextCoverageVerdictReporter.Classify(
            armFaulted: false,
            new RepoFileVectorIngestOutcome(0, 0, CoverageEstablished: true, Deferred: true));

        Assert.That(verdict, Is.EqualTo(RepoContextCoverageVerdict.ProbeUnmeasurable));
    }

    [Test]
    public void Classify_a_skipped_gap_scan_reports_probe_unmeasurable()
    {
        var verdict = RepoContextCoverageVerdictReporter.Classify(
            armFaulted: false,
            new RepoFileVectorIngestOutcome(0, 0, CoverageEstablished: true, GapScanSkipped: true));

        Assert.That(verdict, Is.EqualTo(RepoContextCoverageVerdict.ProbeUnmeasurable));
    }

    [Test]
    public void Classify_an_unmeasurable_pass_outranks_a_selected_gap()
    {
        // A pass that deferred can still have selected some gaps before it gave up.
        // That count is a floor on an unknown, not a measurement, so it must not be
        // promoted to a finding - the same category error as issue #3340 itself.
        var verdict = RepoContextCoverageVerdictReporter.Classify(
            armFaulted: false,
            new RepoFileVectorIngestOutcome(0, 4, CoverageEstablished: true, Deferred: true));

        Assert.That(verdict, Is.EqualTo(RepoContextCoverageVerdict.ProbeUnmeasurable));
    }

    [Test]
    public void Snapshot_starts_at_zero_for_every_verdict()
    {
        using var reporter = new RepoContextCoverageVerdictReporter();

        var snapshot = reporter.Snapshot();

        Assert.Multiple(() =>
        {
            foreach (var verdict in AllVerdicts)
            {
                Assert.That(snapshot.Count(verdict), Is.Zero, $"{verdict} starts at zero");
            }

            Assert.That(snapshot.Total, Is.Zero);
        });
    }

    [Test]
    public void Record_tallies_each_verdict_independently()
    {
        using var reporter = new RepoContextCoverageVerdictReporter();

        foreach (var verdict in AllVerdicts)
        {
            reporter.Record(verdict);
        }

        reporter.Record(RepoContextCoverageVerdict.ProbeUnmeasurable);
        var snapshot = reporter.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count(RepoContextCoverageVerdict.Converged), Is.EqualTo(1));
            Assert.That(snapshot.Count(RepoContextCoverageVerdict.ArmFailure), Is.EqualTo(1));
            Assert.That(snapshot.Count(RepoContextCoverageVerdict.GapFound), Is.EqualTo(1));
            Assert.That(snapshot.Count(RepoContextCoverageVerdict.ProbeUnmeasurable), Is.EqualTo(2));
            Assert.That(snapshot.Total, Is.EqualTo(5));
        });
    }

    [Test]
    public void Snapshot_is_a_copy_that_does_not_move_under_later_records()
    {
        using var reporter = new RepoContextCoverageVerdictReporter();
        reporter.Record(RepoContextCoverageVerdict.GapFound);

        var taken = reporter.Snapshot();
        reporter.Record(RepoContextCoverageVerdict.GapFound);

        Assert.That(taken.Count(RepoContextCoverageVerdict.GapFound), Is.EqualTo(1));
    }

    [Test]
    public void A_default_snapshot_reads_as_empty_rather_than_throwing()
    {
        var snapshot = default(RepoContextCoverageVerdictSnapshot);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count(RepoContextCoverageVerdict.Converged), Is.Zero);
            Assert.That(snapshot.Total, Is.Zero);
        });
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var reporter = new RepoContextCoverageVerdictReporter();

        reporter.Dispose();

        Assert.That(() => reporter.Dispose(), Throws.Nothing);
    }

    [Test]
    public void Every_verdict_series_is_minted_at_zero_before_any_pass_records_one()
    {
        // The whole point of this instrument is to make a quiet outcome legible. A
        // series that only appears once it is non-zero cannot do that: an absent
        // probe_unmeasurable series and a zero one are the same scrape, so an alert
        // written against it would never fire on the transition it exists to catch.
        var observed = new List<KeyValuePair<string, object?>>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == RepoContextCoverageVerdictReporter.VerdictInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            if (measurement != 0)
            {
                return;
            }

            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextCoverageVerdictReporter.ReasonTagKey)
                {
                    observed.Add(tag);
                }
            }
        });
        listener.Start();

        using var reporter = new RepoContextCoverageVerdictReporter();

        Assert.That(
            observed.Select(tag => (string?)tag.Value).Order(StringComparer.Ordinal),
            Is.EqualTo(new[]
            {
                RepoContextCoverageVerdictReporter.ReasonArmFailureTag,
                RepoContextCoverageVerdictReporter.ReasonConvergedTag,
                RepoContextCoverageVerdictReporter.ReasonGapFoundTag,
                RepoContextCoverageVerdictReporter.ReasonProbeUnmeasurableTag,
            }.Order(StringComparer.Ordinal)));
    }
}
