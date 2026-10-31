namespace Orleans.Lattice.Tests.Internal;

/// <summary>
/// Covers <see cref="OrdinalStrings"/>: the null-as-unbounded range-bound folds,
/// half-open intersections and the ordinal membership probe.
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
    public void TryIntersectBounds_matches_independent_membership_for_all_bound_combinations()
    {
        string?[] bounds = [null, "", "B", "a", "a\0", "b"];
        string[] keys = ["", "\0", "A", "B", "C", "a", "a\0", "a\0\0", "ab", "b", "z"];

        foreach (var windowStart in bounds)
        foreach (var windowEnd in bounds)
        foreach (var rangeStart in bounds)
        foreach (var rangeEnd in bounds)
        {
            var intersects = OrdinalStrings.TryIntersectBounds(
                windowStart, windowEnd, rangeStart, rangeEnd, out var start, out var end);
            var expectedStart = new[] { windowStart, rangeStart }
                .Where(bound => bound is not null).Order(StringComparer.Ordinal).LastOrDefault();
            var expectedEnd = new[] { windowEnd, rangeEnd }
                .Where(bound => bound is not null).Order(StringComparer.Ordinal).FirstOrDefault();
            var expected = keys.Where(key =>
                (windowStart is null || StringComparer.Ordinal.Compare(key, windowStart) >= 0)
                && (windowEnd is null || StringComparer.Ordinal.Compare(key, windowEnd) < 0)
                && (rangeStart is null || StringComparer.Ordinal.Compare(key, rangeStart) >= 0)
                && (rangeEnd is null || StringComparer.Ordinal.Compare(key, rangeEnd) < 0));
            var actual = keys.Where(key => intersects
                && (start is null || StringComparer.Ordinal.Compare(key, start) >= 0)
                && (end is null || StringComparer.Ordinal.Compare(key, end) < 0));

            Assert.Multiple(() =>
            {
                Assert.That(start, Is.EqualTo(expectedStart));
                Assert.That(end, Is.EqualTo(expectedEnd));
                Assert.That(intersects, Is.EqualTo(expectedStart is null || expectedEnd is null
                    || StringComparer.Ordinal.Compare(expectedStart, expectedEnd) < 0));
                Assert.That(actual, Is.EqualTo(expected));
            });
        }
    }

    [Test]
    public void TryIntersectBounds_does_not_allocate()
    {
        for (var i = 0; i < 1_000; i++)
            OrdinalStrings.TryIntersectBounds("a", "z", "b", "y", out _, out _);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var intersections = 0;
        for (var i = 0; i < 10_000; i++)
        {
            if (OrdinalStrings.TryIntersectBounds("a", "z", "b", "y", out _, out _))
                intersections++;
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(intersections, Is.EqualTo(10_000));
        Assert.That(allocated, Is.Zero);
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
