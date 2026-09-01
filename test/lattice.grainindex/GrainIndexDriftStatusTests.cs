namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexDriftStatus"/>: the operator-visible form of the
/// registry's drift verdict, including the distinction between any drift at all
/// and drift that invalidates the entries already written.
/// </summary>
[TestFixture]
public sealed class GrainIndexDriftStatusTests
{
    [Test]
    public void A_status_rejects_a_null_changed_field_list() =>
        Assert.That(
            () => new GrainIndexDriftStatus(null!),
            Throws.ArgumentNullException);

    [Test]
    public void A_status_keeps_the_changed_fields_it_was_given()
    {
        GrainIndexDefinitionField[] changed =
        [
            GrainIndexDefinitionField.Properties,
            GrainIndexDefinitionField.AllowReplication,
        ];

        var status = new GrainIndexDriftStatus(changed);

        Assert.That(status.ChangedFields, Is.EqualTo(changed));
    }

    [Test]
    public void No_changed_fields_means_no_drift_and_nothing_breaking()
    {
        var status = new GrainIndexDriftStatus([]);

        Assert.Multiple(() =>
        {
            Assert.That(status.HasDrift, Is.False);
            Assert.That(status.HasBreakingChange, Is.False);
        });
    }

    [Test]
    public void A_drift_safe_change_is_drift_without_being_breaking()
    {
        var status = new GrainIndexDriftStatus([GrainIndexDefinitionField.AllowReplication]);

        Assert.Multiple(() =>
        {
            Assert.That(status.HasDrift, Is.True);
            Assert.That(status.HasBreakingChange, Is.False);
        });
    }

    [TestCase(GrainIndexDefinitionField.Name)]
    [TestCase(GrainIndexDefinitionField.TreeName)]
    [TestCase(GrainIndexDefinitionField.GrainInterfaceType)]
    [TestCase(GrainIndexDefinitionField.StateType)]
    [TestCase(GrainIndexDefinitionField.KeyCodec)]
    [TestCase(GrainIndexDefinitionField.Properties)]
    public void Every_breaking_field_reports_a_breaking_change(GrainIndexDefinitionField field)
    {
        var status = new GrainIndexDriftStatus([field]);

        Assert.Multiple(() =>
        {
            Assert.That(status.HasDrift, Is.True);
            Assert.That(status.HasBreakingChange, Is.True);
        });
    }

    [Test]
    public void One_breaking_field_among_safe_ones_still_reports_breaking()
    {
        var status = new GrainIndexDriftStatus(
            [GrainIndexDefinitionField.AllowReplication, GrainIndexDefinitionField.Properties]);

        Assert.That(status.HasBreakingChange, Is.True);
    }

    [Test]
    public void The_none_status_describes_a_declaration_that_matches_its_record()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainIndexDriftStatus.None.ChangedFields, Is.Empty);
            Assert.That(GrainIndexDriftStatus.None.HasDrift, Is.False);
            Assert.That(GrainIndexDriftStatus.None.HasBreakingChange, Is.False);
            Assert.That(GrainIndexDriftStatus.None, Is.SameAs(GrainIndexDriftStatus.None));
        });
    }
}
