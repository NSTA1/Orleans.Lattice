namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Round-trips a compliance report through the result map a tracked compliance
/// scan records (#4126), and rejects a map that does not describe one.
/// </summary>
[TestFixture]
public sealed class SchemaComplianceScanResultsTests
{
    private static LatticeSchemaComplianceReport Report() => new()
    {
        TreeId = "orders",
        HasPolicy = true,
        RuleCount = 3,
        CompliantCount = 7,
        NonCompliantCount = 3,
        ScannedCount = 10,
        RuleBreakdown =
        [
            new LatticeSchemaComplianceRuleCount { Reason = "must be json", Count = 2 },
            new LatticeSchemaComplianceRuleCount { Reason = "too long, really", Count = 1 },
        ],
    };

    [Test]
    public void A_report_round_trips_through_the_result_map()
    {
        var map = SchemaComplianceScanResults.ToResultMap(Report());

        Assert.That(SchemaComplianceScanResults.TryReadReport(map, out var read), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(read.TreeId, Is.EqualTo("orders"));
            Assert.That(read.HasPolicy, Is.True);
            Assert.That(read.RuleCount, Is.EqualTo(3));
            Assert.That(map[SchemaComplianceScanResults.FailureReasonCountKey], Is.EqualTo("2"));
            Assert.That(read.CompliantCount, Is.EqualTo(7));
            Assert.That(read.NonCompliantCount, Is.EqualTo(3));
            Assert.That(read.ScannedCount, Is.EqualTo(10));
            Assert.That(read.RuleBreakdown, Is.EqualTo(Report().RuleBreakdown));
            Assert.That(map[SchemaComplianceScanResults.ScannedCountKey], Is.EqualTo("10"));
        });
    }

    [Test]
    public void An_ungoverned_report_round_trips_with_an_empty_breakdown()
    {
        var map = SchemaComplianceScanResults.ToResultMap(LatticeSchemaComplianceReport.Ungoverned("orders"));

        Assert.That(SchemaComplianceScanResults.TryReadReport(map, out var read), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(read.HasPolicy, Is.False);
            Assert.That(read.RuleBreakdown, Is.Empty);
        });
    }

    [Test]
    public void A_map_that_does_not_describe_a_compliance_scan_is_not_read()
    {
        var map = new Dictionary<string, string>(SchemaComplianceScanResults.ToResultMap(Report()));
        map.Remove(SchemaComplianceScanResults.CompliantCountKey);
        var broken = new Dictionary<string, string>(SchemaComplianceScanResults.ToResultMap(Report()))
        {
            [SchemaComplianceScanResults.FailureReasonCountKey] = "5",
        };

        Assert.Multiple(() =>
        {
            Assert.That(SchemaComplianceScanResults.TryReadReport(new Dictionary<string, string>(), out _), Is.False);
            Assert.That(SchemaComplianceScanResults.TryReadReport(map, out _), Is.False);
            Assert.That(SchemaComplianceScanResults.TryReadReport(broken, out _), Is.False, "A breakdown row the count promises but the map lacks.");
        });
    }

    [Test]
    public void TryReadReport_legacy_maps_keep_breakdown_readable_without_inventing_policy_count()
    {
        var legacy = new Dictionary<string, string>(SchemaComplianceScanResults.ToResultMap(Report()));
        legacy.Remove(SchemaComplianceScanResults.FailureReasonCountKey);
        legacy[SchemaComplianceScanResults.RuleCountKey] = "2";

        Assert.That(SchemaComplianceScanResults.TryReadReport(legacy, out var report), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(report.RuleCount, Is.Zero);
            Assert.That(report.RuleBreakdown, Is.EqualTo(Report().RuleBreakdown));
        });
    }

    [TestCase("-1")]
    [TestCase("2147483647")]
    [TestCase("invalid")]
    public void TryReadReport_invalid_breakdown_count_is_rejected_without_allocating(string count)
    {
        var map = new Dictionary<string, string>(SchemaComplianceScanResults.ToResultMap(Report()))
        {
            [SchemaComplianceScanResults.FailureReasonCountKey] = count,
        };
        Assert.That(SchemaComplianceScanResults.TryReadReport(map, out _), Is.False);
    }

    [Test]
    public void Null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => SchemaComplianceScanResults.TryReadReport(null!, out _), Throws.ArgumentNullException);
        });
    }
}
