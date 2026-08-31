using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// Covers <see cref="GrainIndexDriftReport"/>: the small value the detector
/// hands the reconciler, and the breaking / safe split the reconciler branches
/// on.
/// </summary>
[TestFixture]
public sealed class GrainIndexDriftReportTests
{
    [Test]
    public void The_no_drift_report_has_no_changed_fields()
    {
        var report = GrainIndexDriftReport.None;

        Assert.Multiple(() =>
        {
            Assert.That(report.ChangedFields, Is.Empty);
            Assert.That(report.HasDrift, Is.False);
            Assert.That(report.HasBreakingChange(), Is.False);
            Assert.That(report.BreakingFields(), Is.Empty);
        });
    }

    [Test]
    public void A_report_over_only_safe_fields_reports_drift_but_nothing_breaking()
    {
        var report = new GrainIndexDriftReport([GrainIndexDefinitionField.AllowReplication]);

        Assert.Multiple(() =>
        {
            Assert.That(report.HasDrift, Is.True);
            Assert.That(report.HasBreakingChange(), Is.False);
            Assert.That(report.BreakingFields(), Is.Empty);
        });
    }

    [Test]
    public void A_report_over_a_breaking_field_reports_it()
    {
        var report = new GrainIndexDriftReport([GrainIndexDefinitionField.Properties]);

        Assert.Multiple(() =>
        {
            Assert.That(report.HasDrift, Is.True);
            Assert.That(report.HasBreakingChange(), Is.True);
            Assert.That(report.BreakingFields(), Is.EqualTo(new[] { GrainIndexDefinitionField.Properties }));
        });
    }

    [Test]
    public void A_mixed_report_returns_only_the_breaking_subset_in_declaration_order()
    {
        var report = new GrainIndexDriftReport(
        [
            GrainIndexDefinitionField.TreeName,
            GrainIndexDefinitionField.Properties,
            GrainIndexDefinitionField.AllowReplication,
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(report.HasBreakingChange(), Is.True);
            Assert.That(report.BreakingFields(), Is.EqualTo(new[]
            {
                GrainIndexDefinitionField.TreeName,
                GrainIndexDefinitionField.Properties,
            }),
                "The subset must keep the order it was reported in so the exception message reads "
                + "the same way the detector walked the declaration.");
        });
    }

    [Test]
    public void A_default_report_has_no_changed_fields_rather_than_faulting()
    {
        var report = default(GrainIndexDriftReport);

        Assert.Multiple(() =>
        {
            Assert.That(report.ChangedFields, Is.Null,
                "A default struct carries no list; the reconciler never produces one, and this "
                + "pins the fact so a future caller does not assume otherwise.");
            Assert.That(() => report.HasDrift, Throws.TypeOf<NullReferenceException>());
        });
    }

    [Test]
    public void A_null_changed_field_list_is_rejected()
    {
        Assert.That(() => new GrainIndexDriftReport(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_no_drift_report_is_a_shared_instance_rather_than_a_per_call_allocation()
    {
        Assert.That(
            GrainIndexDriftReport.None.ChangedFields,
            Is.SameAs(GrainIndexDriftReport.None.ChangedFields));
    }
}
