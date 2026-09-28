using System.Diagnostics.Metrics;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The build-stage reporter adapts the vector package's observer seam onto the
/// repocontext meter, so build-slice time becomes attributable by stage.
/// </summary>
/// <remarks>
/// The attribution this publishes previously did not exist: a slice yielding 2%
/// of its batch cap looked identical whether it was bound by a slow source, by
/// key assignment, or by the index itself, and the answer had to be read out of
/// the source instead of the telemetry.
/// </remarks>
[TestFixture]
public sealed class RepoContextAnnBuildStageReporterTests
{
    private static VectorIndexBuildSliceTimings Timings(
        double sourceMs = 5, double keyMs = 3, double upsertMs = 1, double flushMs = 7, int consumed = 42)
        => new(
            TimeSpan.FromMilliseconds(sourceMs),
            TimeSpan.FromMilliseconds(keyMs),
            TimeSpan.FromMilliseconds(upsertMs),
            TimeSpan.FromMilliseconds(flushMs),
            consumed);

    private static List<KeyValuePair<string, double>> CaptureStages(
        RepoContextAnnBuildStageReporter reporter, Action act)
    {
        var captured = new List<KeyValuePair<string, double>>();
        using var listener = MeterListening.StartForMeter(reporter.Meter, l =>
            l.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            {
                if (instrument.Name != RepoContextAnnBuildStageReporter.StageDurationInstrumentName)
                {
                    return;
                }

                foreach (var tag in tags)
                {
                    if (tag.Key == RepoContextAnnBuildStageReporter.StageTagKey)
                    {
                        captured.Add(new KeyValuePair<string, double>((string)tag.Value!, value));
                    }
                }
            }));

        act();
        listener.RecordObservableInstruments();
        return captured;
    }

    [Test]
    public void OnSliceCompleted_records_one_measurement_per_stage()
    {
        using var reporter = new RepoContextAnnBuildStageReporter();

        var captured = CaptureStages(reporter, () => reporter.OnSliceCompleted(Timings()));

        Assert.That(
            captured.Select(c => c.Key),
            Is.EquivalentTo(new[]
            {
                RepoContextAnnBuildStageReporter.StageSourceWait,
                RepoContextAnnBuildStageReporter.StageKeyAssign,
                RepoContextAnnBuildStageReporter.StageIndexUpsert,
                RepoContextAnnBuildStageReporter.StageKeyFlush,
            }));
    }

    [Test]
    public void OnSliceCompleted_records_each_stage_in_seconds()
    {
        using var reporter = new RepoContextAnnBuildStageReporter();

        var captured = CaptureStages(reporter, () => reporter.OnSliceCompleted(Timings(sourceMs: 250)));

        var sourceWait = captured.Single(c => c.Key == RepoContextAnnBuildStageReporter.StageSourceWait);
        Assert.That(sourceWait.Value, Is.EqualTo(0.25).Within(0.001));
    }

    [Test]
    public void OnSliceCompleted_clamps_a_negative_duration_to_zero()
    {
        using var reporter = new RepoContextAnnBuildStageReporter();

        // A negative reading can only come from a clock irregularity, and once
        // summed it is indistinguishable from a genuine measurement.
        var captured = CaptureStages(
            reporter,
            () => reporter.OnSliceCompleted(new VectorIndexBuildSliceTimings(
                TimeSpan.FromMilliseconds(-5), TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, 1)));

        var sourceWait = captured.Single(c => c.Key == RepoContextAnnBuildStageReporter.StageSourceWait);
        Assert.That(sourceWait.Value, Is.Zero);
    }

    [Test]
    public void OnSliceCompleted_counts_the_items_a_slice_consumed()
    {
        using var reporter = new RepoContextAnnBuildStageReporter();
        long total = 0;
        using var listener = MeterListening.StartForMeter(reporter.Meter, l =>
            l.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
            {
                if (instrument.Name == RepoContextAnnBuildStageReporter.SliceItemsInstrumentName)
                {
                    total += value;
                }
            }));

        reporter.OnSliceCompleted(Timings(consumed: 81));
        reporter.OnSliceCompleted(Timings(consumed: 19));

        // The denominator that makes vectors-per-slice obtainable from metrics
        // alone, which dividing the cumulative vectorsIndexed by the
        // process-scoped slice counter is not: the former is inherited across a
        // restart, the latter resets at the deploy boundary.
        Assert.That(total, Is.EqualTo(100));
    }

    [Test]
    public void OnSliceCompleted_does_not_count_a_slice_that_consumed_nothing()
    {
        using var reporter = new RepoContextAnnBuildStageReporter();
        var measurements = 0;
        using var listener = MeterListening.StartForMeter(reporter.Meter, l =>
            l.SetMeasurementEventCallback<long>((instrument, _, _, _) =>
            {
                if (instrument.Name == RepoContextAnnBuildStageReporter.SliceItemsInstrumentName)
                {
                    measurements++;
                }
            }));

        reporter.OnSliceCompleted(Timings(consumed: 0));

        Assert.That(measurements, Is.Zero);
    }

    [Test]
    public void The_reporter_publishes_on_the_shared_repocontext_meter()
    {
        using var reporter = new RepoContextAnnBuildStageReporter();

        // One scraper subscription has to cover the whole repocontext surface.
        Assert.That(reporter.Meter.Name, Is.EqualTo(RepoContextUsageRecorder.MeterName));
    }
}
