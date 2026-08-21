using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit test for the <c>subjects</c> observable gauge callback
/// (<c>AuthSnapshotGaugeRegistry.ObserveSubjects</c>): scraping the meter with a
/// live, warmed maintainer in the process yields at least one measurement equal to
/// that maintainer's distinct-subject count.
/// </summary>
[TestFixture]
public sealed class AuthSnapshotGaugeRegistrySubjectsTests
{
    [Test]
    public async Task ObserveSubjects_reports_the_distinct_subject_count_of_a_live_maintainer()
    {
        var rules = new[]
        {
            new LatticeAuthorizationRule("r1", LatticeSubjectSelector.User("alice"), LatticeScope.Tree("app"), LatticeOperation.Read, LatticeEffect.Allow),
            new LatticeAuthorizationRule("r2", LatticeSubjectSelector.Group("admins"), LatticeScope.Tree("app"), LatticeOperation.Write, LatticeEffect.Allow),
        };
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions(), rules);
        var expected = harness.Maintainer.CurrentSubjectCount;

        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.SnapshotSubjectsName);
        collector.RecordObservableInstruments();

        Assert.That(expected, Is.GreaterThan(0), "the seeded policy has at least one distinct subject");
        Assert.That(
            collector.Measurements.Select(m => m.Value),
            Has.Some.EqualTo(expected),
            "scraping the subjects gauge observes the live maintainer");
        GC.KeepAlive(harness.Maintainer);
    }
}
