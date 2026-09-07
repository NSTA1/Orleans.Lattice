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
        // finds every overlap. The result is accumulated inline rather than into
        // a list, so an intersection that narrows to one interval - which is what
        // every conjunction over a property does - builds its range set with one
        // allocation instead of a list, its backing array, and a copy.
        var overlaps = default(RangeAccumulator);
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
                overlaps.Add(start, end);
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

        return overlaps.ToArray();
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

        // A complement leaves at most one gap per range plus a trailing gap, and
        // in practice negating a point lookup leaves exactly the two gaps either
        // side of it. Accumulating inline builds that result with the single
        // exact-width array it returns.
        var gaps = default(RangeAccumulator);
        string cursor = universeStart;
        for (var i = 0; i < ranges.Length; i++)
        {
            var range = ranges[i];
            if (string.CompareOrdinal(cursor, range.StartInclusive) < 0)
            {
                gaps.Add(cursor, range.StartInclusive);
            }

            if (string.CompareOrdinal(range.EndExclusive, cursor) > 0)
            {
                cursor = range.EndExclusive;
            }
        }

        if (string.CompareOrdinal(cursor, universeEnd) < 0)
        {
            gaps.Add(cursor, universeEnd);
        }

        return gaps.ToArray();
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

    /// <summary>
    /// Accumulates a result range set, holding the first two ranges inline.
    /// <para>
    /// Every operation here yields one or two ranges in practice - intersecting a
    /// property's clauses narrows to a single interval, and complementing a point
    /// lookup leaves the gaps either side of it - so the common result is built
    /// with exactly one allocation, the exact-width array that is returned. A
    /// third range spills to a list, which only a set produced by negating an
    /// already-disjoint set can reach.
    /// </para>
    /// </summary>
    private struct RangeAccumulator
    {
        private GrainIndexKeyRange _first;
        private GrainIndexKeyRange _second;
        private List<GrainIndexKeyRange>? _rest;
        private int _count;

        /// <summary>Appends a range. Callers add in ordinal-ascending order.</summary>
        internal void Add(string startInclusive, string endExclusive)
        {
            switch (_count)
            {
                case 0:
                    _first = new GrainIndexKeyRange(startInclusive, endExclusive);
                    break;
                case 1:
                    _second = new GrainIndexKeyRange(startInclusive, endExclusive);
                    break;
                default:
                    _rest ??= new List<GrainIndexKeyRange>(2);
                    _rest.Add(new GrainIndexKeyRange(startInclusive, endExclusive));
                    break;
            }

            _count++;
        }

        /// <summary>Materialises the accumulated ranges as an exact-width array.</summary>
        internal GrainIndexKeyRange[] ToArray()
        {
            switch (_count)
            {
                case 0:
                    return Empty;
                case 1:
                    return [_first];
                case 2:
                    return [_first, _second];
                default:
                    var result = new GrainIndexKeyRange[_count];
                    result[0] = _first;
                    result[1] = _second;
                    _rest!.CopyTo(result, 2);
                    return result;
            }
        }
    }
}
