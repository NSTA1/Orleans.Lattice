using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for the serializable compliance-audit model types
/// <see cref="LatticeSchemaComplianceReport"/> and
/// <see cref="LatticeSchemaComplianceRuleCount"/>: the ungoverned factory, field
/// retention, and Orleans serialization round-tripping (so the report survives the
/// gRPC wire).
/// </summary>
[TestFixture]
public sealed class LatticeSchemaComplianceReportTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void Ungoverned_reports_no_policy_and_all_zero_counts()
    {
        var report = LatticeSchemaComplianceReport.Ungoverned("orders");

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo("orders"));
            Assert.That(report.HasPolicy, Is.False);
            Assert.That(report.CompliantCount, Is.Zero);
            Assert.That(report.NonCompliantCount, Is.Zero);
            Assert.That(report.ScannedCount, Is.Zero);
            Assert.That(report.RuleBreakdown, Is.Empty);
        });
    }

    [Test]
    public void Report_round_trips_with_its_breakdown()
    {
        var original = new LatticeSchemaComplianceReport
        {
            TreeId = "orders",
            HasPolicy = true,
            CompliantCount = 7,
            NonCompliantCount = 3,
            ScannedCount = 10,
            RuleBreakdown = new[]
            {
                new LatticeSchemaComplianceRuleCount { Reason = "must be json", Count = 2 },
                new LatticeSchemaComplianceRuleCount { Reason = "too long", Count = 1 },
            },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.HasPolicy, Is.True);
            Assert.That(copy.CompliantCount, Is.EqualTo(7));
            Assert.That(copy.NonCompliantCount, Is.EqualTo(3));
            Assert.That(copy.ScannedCount, Is.EqualTo(10));
            Assert.That(copy.RuleBreakdown, Has.Count.EqualTo(2));
            Assert.That(copy.RuleBreakdown[0].Reason, Is.EqualTo("must be json"));
            Assert.That(copy.RuleBreakdown[0].Count, Is.EqualTo(2));
        });
    }

    [Test]
    public void RuleCount_round_trips()
    {
        var copy = RoundTrip(new LatticeSchemaComplianceRuleCount { Reason = "bad", Count = 42 });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Reason, Is.EqualTo("bad"));
            Assert.That(copy.Count, Is.EqualTo(42));
        });
    }
}
