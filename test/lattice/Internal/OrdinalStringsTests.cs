namespace Orleans.Lattice.Tests.Internal;

/// <summary>
/// Covers <see cref="OrdinalStrings"/>: the null-as-unbounded range-bound folds
/// and the ordinal membership probe.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class OrdinalStringsTests
{
    [Test]
    public void MaxBound_treats_null_as_unbounded_and_otherwise_takes_the_ordinal_maximum()
    {
        Assert.Multiple(() =>
        {
            Assert.That(OrdinalStrings.MaxBound(null, null), Is.Null);
            Assert.That(OrdinalStrings.MaxBound(null, "b"), Is.EqualTo("b"));
            Assert.That(OrdinalStrings.MaxBound("a", null), Is.EqualTo("a"));
            Assert.That(OrdinalStrings.MaxBound("a", "b"), Is.EqualTo("b"));
            Assert.That(OrdinalStrings.MaxBound("B", "a"), Is.EqualTo("a"), "ordinal, not culture, ordering");
        });
    }

    [Test]
    public void MinBound_treats_null_as_unbounded_and_otherwise_takes_the_ordinal_minimum()
    {
        Assert.Multiple(() =>
        {
            Assert.That(OrdinalStrings.MinBound(null, null), Is.Null);
            Assert.That(OrdinalStrings.MinBound(null, "b"), Is.EqualTo("b"));
            Assert.That(OrdinalStrings.MinBound("a", null), Is.EqualTo("a"));
            Assert.That(OrdinalStrings.MinBound("a", "b"), Is.EqualTo("a"));
            Assert.That(OrdinalStrings.MinBound("B", "a"), Is.EqualTo("B"), "ordinal, not culture, ordering");
        });
    }

    [Test]
    public void Contains_matches_ordinally()
    {
        IReadOnlyList<string> values = ["alpha", "beta"];

        Assert.Multiple(() =>
        {
            Assert.That(OrdinalStrings.Contains(values, "beta"), Is.True);
            Assert.That(OrdinalStrings.Contains(values, "BETA"), Is.False);
            Assert.That(OrdinalStrings.Contains(Array.Empty<string>(), "beta"), Is.False);
        });
    }
}
