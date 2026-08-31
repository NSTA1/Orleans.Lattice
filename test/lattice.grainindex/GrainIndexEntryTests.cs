namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The entry record's guards and the content-based payload equality the
/// projection diff depends on: the default record equality would have compared
/// the payload array by reference, which is never the question being asked.
/// </summary>
[TestFixture]
public class GrainIndexEntryTests
{
    [Test]
    public void An_entry_keeps_the_key_and_payload_it_was_given()
    {
        byte[] payload = [1, 2, 3];
        var entry = new GrainIndexEntry("k", payload);

        Assert.That(entry.Key, Is.EqualTo("k"));
        Assert.That(entry.Value, Is.SameAs(payload));
    }

    [Test]
    public void Entries_are_equal_when_their_payloads_match_by_content_not_by_reference()
    {
        var left = new GrainIndexEntry("k", [1, 2, 3]);
        var right = new GrainIndexEntry("k", [1, 2, 3]);

        Assert.That(left, Is.EqualTo(right));
        Assert.That(left.Value, Is.Not.SameAs(right.Value));
        Assert.That(left.GetHashCode(), Is.EqualTo(right.GetHashCode()));
    }

    [Test]
    public void Entries_differ_when_the_payload_content_differs()
    {
        Assert.That(new GrainIndexEntry("k", [1, 2, 3]), Is.Not.EqualTo(new GrainIndexEntry("k", [1, 2, 4])));
        Assert.That(new GrainIndexEntry("k", [1, 2, 3]), Is.Not.EqualTo(new GrainIndexEntry("k", [1, 2])));
    }

    [Test]
    public void Entries_differ_when_the_key_differs_ordinally()
    {
        Assert.That(new GrainIndexEntry("k", [1]), Is.Not.EqualTo(new GrainIndexEntry("K", [1])));
    }

    [Test]
    public void Two_empty_payloads_are_equal()
    {
        Assert.That(new GrainIndexEntry("k", []), Is.EqualTo(new GrainIndexEntry("k", [])));
    }

    [Test]
    public void An_entry_rejects_null_arguments()
    {
        Assert.That(() => new GrainIndexEntry(null!, []), Throws.ArgumentNullException);
        Assert.That(() => new GrainIndexEntry("k", null!), Throws.ArgumentNullException);
    }
}
