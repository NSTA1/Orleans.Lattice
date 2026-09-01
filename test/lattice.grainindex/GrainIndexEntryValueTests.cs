namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The payload field-name contract. These names are wire format for every query
/// built on a grain index, so they are pinned by an explicit test rather than
/// left to a rename.
/// </summary>
[TestFixture]
public class GrainIndexEntryValueTests
{
    [Test]
    public void The_metadata_field_names_are_the_documented_ones()
    {
        Assert.That(GrainIndexEntryValue.GrainKeyField, Is.EqualTo("$grain"));
        Assert.That(GrainIndexEntryValue.PropertyField, Is.EqualTo("$property"));
    }

    [Test]
    public void The_metadata_field_names_cannot_collide_with_a_csharp_property_name()
    {
        // A leading '$' is not legal in a C# identifier, so no lambda over a
        // state type can ever name one of these fields.
        foreach (var field in new[] { GrainIndexEntryValue.GrainKeyField, GrainIndexEntryValue.PropertyField })
        {
            Assert.That(field, Does.StartWith("$"));
            Assert.That(field.Skip(1).All(char.IsLetter), Is.True, field);
        }
    }
}
