using System.Text;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The match record's construction, guards, and content-based equality.
/// </summary>
[TestFixture]
public sealed class GrainIndexMatchTests
{
    [Test]
    public void A_match_carries_its_grain_key_property_and_payload()
    {
        var payload = Encoding.UTF8.GetBytes("{\"Age\":18}");

        var match = new GrainIndexMatch("alice", "Age", payload);

        Assert.Multiple(() =>
        {
            Assert.That(match.GrainKey, Is.EqualTo("alice"));
            Assert.That(match.PropertyName, Is.EqualTo("Age"));
            Assert.That(match.Value, Is.SameAs(payload));
        });
    }

    [Test]
    public void A_match_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new GrainIndexMatch(null!, "Age", []));
            Assert.Throws<ArgumentNullException>(() => new GrainIndexMatch("alice", null!, []));
            Assert.Throws<ArgumentNullException>(() => new GrainIndexMatch("alice", "Age", null!));
        });
    }

    [Test]
    public void Equality_compares_the_payload_by_content_not_by_reference()
    {
        var left = new GrainIndexMatch("alice", "Age", Encoding.UTF8.GetBytes("{\"Age\":18}"));
        var right = new GrainIndexMatch("alice", "Age", Encoding.UTF8.GetBytes("{\"Age\":18}"));

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.EqualTo(right));
            Assert.That(left.GetHashCode(), Is.EqualTo(right.GetHashCode()));
        });
    }

    [Test]
    public void Equality_separates_a_different_payload()
    {
        var left = new GrainIndexMatch("alice", "Age", Encoding.UTF8.GetBytes("{\"Age\":18}"));
        var right = new GrainIndexMatch("alice", "Age", Encoding.UTF8.GetBytes("{\"Age\":19}"));

        Assert.That(left, Is.Not.EqualTo(right));
    }

    [Test]
    public void Equality_separates_a_different_grain_or_property()
    {
        var match = new GrainIndexMatch("alice", "Age", []);

        Assert.Multiple(() =>
        {
            Assert.That(match, Is.Not.EqualTo(new GrainIndexMatch("bob", "Age", [])));
            Assert.That(match, Is.Not.EqualTo(new GrainIndexMatch("alice", "Country", [])));
        });
    }

    [Test]
    public void A_default_match_hashes_without_throwing()
    {
        var match = default(GrainIndexMatch);

        Assert.That(match.GetHashCode(), Is.EqualTo(default(GrainIndexMatch).GetHashCode()));
    }
}
