namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexDriftClassification"/> and the
/// <see cref="GrainIndexDefinitionField"/> enum it classifies: the rule the
/// startup reconciler branches on when a declaration has changed.
/// </summary>
[TestFixture]
public sealed class GrainIndexDriftClassificationTests
{
    private static IReadOnlyList<GrainIndexDefinitionField> AllFields() =>
        Enum.GetValues<GrainIndexDefinitionField>();

    [Test]
    public void Every_declared_field_is_classified()
    {
        var unclassified = AllFields()
            .Where(field =>
            {
                try
                {
                    GrainIndexDriftClassification.IsBreaking(field);
                    return false;
                }
                catch (ArgumentOutOfRangeException)
                {
                    return true;
                }
            })
            .ToArray();

        Assert.That(unclassified, Is.Empty,
            "An unclassified field would be silently treated as safe by whatever handled it, "
            + "which is exactly the silent-corruption outcome the gate exists to prevent. "
            + "Unclassified: " + string.Join(", ", unclassified));
    }

    [Test]
    public void The_two_published_lists_partition_the_field_set_exactly()
    {
        var breaking = GrainIndexDriftClassification.BreakingFields;
        var safe = GrainIndexDriftClassification.SafeFields;

        Assert.Multiple(() =>
        {
            Assert.That(breaking.Concat(safe).OrderBy(f => f), Is.EqualTo(AllFields().OrderBy(f => f)),
                "Together the lists must name every field exactly once, so a field added later "
                + "cannot slip past both.");
            Assert.That(breaking.Intersect(safe), Is.Empty,
                "A field cannot be both breaking and safe.");
        });
    }

    [Test]
    public void The_published_lists_agree_with_the_predicate()
    {
        Assert.Multiple(() =>
        {
            foreach (var field in GrainIndexDriftClassification.BreakingFields)
            {
                Assert.That(GrainIndexDriftClassification.IsBreaking(field), Is.True,
                    $"{field} is published as breaking, so the predicate must agree.");
            }

            foreach (var field in GrainIndexDriftClassification.SafeFields)
            {
                Assert.That(GrainIndexDriftClassification.IsBreaking(field), Is.False,
                    $"{field} is published as safe, so the predicate must agree.");
            }
        });
    }

    [TestCase(GrainIndexDefinitionField.Name)]
    [TestCase(GrainIndexDefinitionField.TreeName)]
    [TestCase(GrainIndexDefinitionField.GrainInterfaceType)]
    [TestCase(GrainIndexDefinitionField.StateType)]
    [TestCase(GrainIndexDefinitionField.KeyCodec)]
    [TestCase(GrainIndexDefinitionField.Properties)]
    public void Fields_that_stored_entries_depend_on_are_breaking(GrainIndexDefinitionField field)
    {
        Assert.That(GrainIndexDriftClassification.IsBreaking(field), Is.True,
            $"Changing {field} invalidates index entries already written, so honouring it "
            + "without a rebuild would return incorrect query results.");
    }

    [Test]
    public void The_replication_opt_in_is_drift_safe()
    {
        Assert.That(
            GrainIndexDriftClassification.IsBreaking(GrainIndexDefinitionField.AllowReplication),
            Is.False,
            "No part of an index entry's encoding depends on the replication opt-in; the startup "
            + "replication guard audits it separately.");
    }

    [Test]
    public void An_undeclared_field_value_is_rejected_rather_than_defaulted_to_safe()
    {
        Assert.That(
            () => GrainIndexDriftClassification.IsBreaking((GrainIndexDefinitionField)9999),
            Throws.TypeOf<ArgumentOutOfRangeException>(),
            "Defaulting an unknown field to safe is what would let a breaking change through.");
    }

    [Test]
    public void The_published_lists_are_stable_across_reads()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                GrainIndexDriftClassification.BreakingFields,
                Is.SameAs(GrainIndexDriftClassification.BreakingFields),
                "The lists are constants, not per-call allocations.");
            Assert.That(
                GrainIndexDriftClassification.SafeFields,
                Is.SameAs(GrainIndexDriftClassification.SafeFields));
        });
    }
}
