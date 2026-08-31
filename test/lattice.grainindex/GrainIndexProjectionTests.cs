namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>The projection record's guards and its empty form.</summary>
[TestFixture]
public class GrainIndexProjectionTests
{
    [Test]
    public void A_projection_keeps_the_grain_key_and_entries_it_was_given()
    {
        var entries = new[] { new GrainIndexEntry("k", [1]) };
        var projection = new GrainIndexProjection("alice", entries);

        Assert.That(projection.GrainKey, Is.EqualTo("alice"));
        Assert.That(projection.Entries, Is.SameAs(entries));
    }

    [Test]
    public void An_empty_projection_names_its_grain_and_carries_no_entries()
    {
        var projection = GrainIndexProjection.Empty("alice");

        Assert.That(projection.GrainKey, Is.EqualTo("alice"));
        Assert.That(projection.Entries, Is.Empty);
    }

    [Test]
    public void An_empty_projection_accepts_an_empty_grain_key()
    {
        Assert.That(GrainIndexProjection.Empty(string.Empty).GrainKey, Is.Empty);
    }

    [Test]
    public void A_projection_rejects_null_arguments()
    {
        Assert.That(() => new GrainIndexProjection(null!, []), Throws.ArgumentNullException);
        Assert.That(() => new GrainIndexProjection("alice", null!), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexProjection.Empty(null!), Throws.ArgumentNullException);
    }
}
