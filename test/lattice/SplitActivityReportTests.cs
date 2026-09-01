namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="SplitActivityReport"/>: the value semantics of the
/// cluster-wide split-activity snapshot and the <see cref="SplitActivityReport.AnyInFlight"/>
/// projection the scale-in safety gate reads.
/// </summary>
[TestFixture]
public sealed class SplitActivityReportTests
{
    [Test]
    public void Default_report_describes_an_idle_cluster()
    {
        var report = default(SplitActivityReport);

        Assert.Multiple(() =>
        {
            Assert.That(report.InFlight, Is.Zero);
            Assert.That(report.ReportingTrees, Is.Zero);
            Assert.That(report.AnyInFlight, Is.False);
            Assert.That(report.ObservedAt, Is.EqualTo(default(DateTimeOffset)));
        });
    }

    [TestCase(0, false)]
    [TestCase(1, true)]
    [TestCase(7, true)]
    public void AnyInFlight_projects_the_in_flight_count(int inFlight, bool expected)
    {
        var report = new SplitActivityReport { InFlight = inFlight };

        Assert.That(report.AnyInFlight, Is.EqualTo(expected));
    }

    [Test]
    public void Properties_round_trip_through_the_initialiser()
    {
        var observed = new DateTimeOffset(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

        var report = new SplitActivityReport
        {
            InFlight = 4,
            ReportingTrees = 3,
            ObservedAt = observed,
        };

        Assert.Multiple(() =>
        {
            Assert.That(report.InFlight, Is.EqualTo(4));
            Assert.That(report.ReportingTrees, Is.EqualTo(3));
            Assert.That(report.ObservedAt, Is.EqualTo(observed));
        });
    }

    [Test]
    public void Reports_with_the_same_content_are_equal()
    {
        var observed = DateTimeOffset.UnixEpoch;
        var a = new SplitActivityReport { InFlight = 2, ReportingTrees = 1, ObservedAt = observed };
        var b = new SplitActivityReport { InFlight = 2, ReportingTrees = 1, ObservedAt = observed };
        var c = a with { InFlight = 3 };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
            Assert.That(a, Is.Not.EqualTo(c));
        });
    }
}
