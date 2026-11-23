using NUnit.Framework;
using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

[TestFixture]
public sealed class BenchLatencyRecorderTests
{
    [Test]
    public void Quantiles_are_within_bucket_resolution_and_never_under_reported()
    {
        var recorder = new BenchLatencyRecorder();
        // 1..1000 ms uniformly: p50 ~ 500 ms, p95 ~ 950 ms, p99 ~ 990 ms.
        for (var ms = 1; ms <= 1000; ms++) recorder.RecordMicros(ms * 1000.0);

        var summary = recorder.Total();

        Assert.Multiple(() =>
        {
            Assert.That(summary.Count, Is.EqualTo(1000));
            Assert.That(summary.P50Ms, Is.InRange(500, 500 * 1.05));
            Assert.That(summary.P95Ms, Is.InRange(950, 950 * 1.05));
            Assert.That(summary.P99Ms, Is.InRange(990, 990 * 1.05));
            Assert.That(summary.MaxMs, Is.InRange(1000, 1000 * 1.05));
        });
    }

    [Test]
    public void DrainWindow_resets_the_window_but_not_the_total()
    {
        var recorder = new BenchLatencyRecorder();
        recorder.RecordMicros(2000);
        recorder.RecordMicros(4000);

        Assert.That(recorder.DrainWindow().Count, Is.EqualTo(2));
        Assert.That(recorder.DrainWindow().Count, Is.Zero);
        Assert.That(recorder.Total().Count, Is.EqualTo(2));
    }

    [Test]
    public void Steady_counts_only_samples_after_MarkSteady()
    {
        var recorder = new BenchLatencyRecorder();
        recorder.RecordMicros(50_000);
        recorder.MarkSteady();
        recorder.RecordMicros(1000);

        var steady = recorder.Steady();

        Assert.That(steady.Count, Is.EqualTo(1));
        Assert.That(steady.MaxMs, Is.LessThan(2));
    }

    [Test]
    public void Empty_and_non_positive_samples_summarise_safely()
    {
        var recorder = new BenchLatencyRecorder();
        Assert.That(recorder.Total(), Is.EqualTo(new LatencySummary(0, 0, 0, 0, 0)));

        recorder.RecordMicros(0);
        recorder.RecordMicros(double.NaN);
        Assert.That(recorder.Total().Count, Is.EqualTo(2));
    }

    [Test]
    public void Format_emits_the_tokens_the_report_parses()
    {
        var text = new LatencySummary(3, 1.5, 2.25, 3, 4).Format();
        Assert.That(text, Is.EqualTo("count=3 p50Ms=1.500 p95Ms=2.250 p99Ms=3.000 maxMs=4.000"));
    }
}
