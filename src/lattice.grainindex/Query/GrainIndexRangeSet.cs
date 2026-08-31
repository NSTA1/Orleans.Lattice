namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// Interval algebra over the ordinal key ranges a clause resolves to. A clause
/// carries a <i>set</i> of ranges rather than one range because negation turns a
/// point lookup into the two ranges either side of it: <c>Age != 18</c> is
/// everything below the <c>18</c> slot plus everything above it.
/// <para>
/// Every operation preserves the representation invariant that a range set is
/// ordinal-ascending, pairwise disjoint, and free of empty ranges, so a set can
/// be scanned range by range without producing an entry twice.
/// </para>
/// </summary>
internal static class GrainIndexRangeSet
{
    /// <summary>The unsatisfiable set, which the planner short-circuits on.</summary>
    internal static GrainIndexKeyRange[] Empty { get; } = [];

    /// <summary>
    /// Intersects two range sets. Both inputs must satisfy the representation
    /// invariant; the result does too.
    /// </summary>
    internal static GrainIndexKeyRange[] Intersect(GrainIndexKeyRange[] left, GrainIndexKeyRange[] right)
    {
        if (left.Length == 0 || right.Length == 0)
            return Empty;

        // Both sides are ordinal-ascending and disjoint, so a single merge walk
        // finds every overlap. The sets are tiny (one or two ranges in practice),
        // so the result list is only allocated once an overlap is actually found.
        List<GrainIndexKeyRange>? overlaps = null;
        var i = 0;
        var j = 0;
        while (i < left.Length && j < right.Length)
        {
            var a = left[i];
            var b = right[j];

            string start = string.CompareOrdinal(a.StartInclusive, b.StartInclusive) >= 0
                ? a.StartInclusive
                : b.StartInclusive;
            string end = string.CompareOrdinal(a.EndExclusive, b.EndExclusive) <= 0
                ? a.EndExclusive
                : b.EndExclusive;

            if (string.CompareOrdinal(start, end) < 0)
            {
                overlaps ??= new List<GrainIndexKeyRange>(2);
                overlaps.Add(new GrainIndexKeyRange(start, end));
            }

            // Advance whichever range ends first: the other may still overlap the
            // next one on that side.
            if (string.CompareOrdinal(a.EndExclusive, b.EndExclusive) <= 0)
            {
                i++;
            }
            else
            {
                j++;
            }
        }

        return overlaps is null ? Empty : overlaps.ToArray();
    }

    /// <summary>
    /// Complements <paramref name="ranges"/> within
    /// <c>[<paramref name="universeStart"/>, <paramref name="universeEnd"/>)</c>,
    /// which is how a negated clause is planned: the negation of an
    /// <i>exact</i> range set is exactly the gaps it leaves inside the property's
    /// own range.
    /// </summary>
    internal static GrainIndexKeyRange[] Complement(
        GrainIndexKeyRange[] ranges,
        string universeStart,
        string universeEnd)
    {
        if (ranges.Length == 0)
            return [new GrainIndexKeyRange(universeStart, universeEnd)];

        List<GrainIndexKeyRange>? gaps = null;
        string cursor = universeStart;
        for (var i = 0; i < ranges.Length; i++)
        {
            var range = ranges[i];
            if (string.CompareOrdinal(cursor, range.StartInclusive) < 0)
            {
                gaps ??= new List<GrainIndexKeyRange>(ranges.Length + 1);
                gaps.Add(new GrainIndexKeyRange(cursor, range.StartInclusive));
            }

            if (string.CompareOrdinal(range.EndExclusive, cursor) > 0)
            {
                cursor = range.EndExclusive;
            }
        }

        if (string.CompareOrdinal(cursor, universeEnd) < 0)
        {
            gaps ??= new List<GrainIndexKeyRange>(1);
            gaps.Add(new GrainIndexKeyRange(cursor, universeEnd));
        }

        return gaps is null ? Empty : gaps.ToArray();
    }

    /// <summary>
    /// Reports whether the set covers the whole of
    /// <c>[<paramref name="universeStart"/>, <paramref name="universeEnd"/>)</c>
    /// as a single range, which is the planner's "no key-range pruning happened"
    /// signal and the least selective clause shape.
    /// </summary>
    internal static bool IsUniverse(
        GrainIndexKeyRange[] ranges,
        string universeStart,
        string universeEnd) =>
        ranges.Length == 1
        && string.Equals(ranges[0].StartInclusive, universeStart, StringComparison.Ordinal)
        && string.Equals(ranges[0].EndExclusive, universeEnd, StringComparison.Ordinal);
}
