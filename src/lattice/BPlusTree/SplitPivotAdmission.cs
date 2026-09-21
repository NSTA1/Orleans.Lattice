namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, dependency-free rule deciding whether a key may be used as the pivot
/// that divides a leaf, and which key to use instead when the selected one may
/// not. Extracted so the decision the production leaf grain executes on its split
/// path (<c>BPlusLeafGrain.SplitAsync</c>, via
/// <c>BPlusLeafGrain.IsAdmissibleSplitPivot</c> and
/// <c>TryFindAdmissibleSplitPivot</c>) is the same artifact the Coyote model
/// drives under systematic schedule exploration, with no possibility of drift.
/// <para>
/// <b>Why a pivot needs its own rule rather than reusing
/// <see cref="SplitBoundary.Owns"/>.</b> A division hands
/// <c>[Low, pivot)</c> to the donor and <c>[pivot, High)</c> to the sibling, so a
/// pivot has to leave <em>both</em> halves non-empty. Ownership only requires the
/// key to fall in <c>[Low, High)</c>, and its low bound is inclusive, so it admits
/// <c>pivot == Low</c> - which produces a donor declaring <c>[Low, Low)</c>.
/// Admissibility is therefore strictly stronger than ownership on the low side:
/// strictly greater than <c>Low</c>, strictly less than <c>High</c>.
/// </para>
/// <para>
/// <b>Why the selected pivot can be out of span at all.</b> The pivot is drawn
/// from the leaf's <em>row set</em>, and a leaf's rows are not a subset of its
/// declared span. Span admission's forward is deliberately fail-open - when no
/// forward target resolves, an out-of-span row commits locally (see
/// <c>BPlusLeafGrain.SpanAdmission.cs</c>) - and a cross-shard migration grafts
/// rows onto a destination before the range fixup lands. Bisecting such a leaf can
/// return a key outside <c>[Low, High)</c>.
/// </para>
/// <para>
/// <b>What goes wrong when it does.</b> One half is born declaring an empty range.
/// That leaf is not inert: it still holds rows and still answers reads, because the
/// read path is custody-agnostic by design. But no routing descent can ever select
/// it, since every descent tests the key against exactly that range, so it can
/// never be written, never be chosen as a forward target, and never be drained. A
/// reader routed to it observes a value no writer can correct - a key frozen at an
/// old value while its siblings advance. Issue 3117.
/// </para>
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no wall-clock, and no Orleans types.
/// <see cref="IsAdmissible"/> is a total function of a key and two nullable bounds
/// and allocates nothing; <see cref="SelectMedianAdmissible"/> allocates nothing
/// beyond the enumerators the caller's own sequence hands out. A
/// <see langword="null"/> bound means "no constraint on that side", used for the
/// chain's leftmost and rightmost leaves and for legacy state shapes that pre-date
/// the persisted range. Comparison is ordinal, matching the B+ tree's key ordering.
/// </para>
/// </summary>
internal static class SplitPivotAdmission
{
    /// <summary>
    /// Reports whether <paramref name="pivot"/> may divide a leaf declaring
    /// <c>[<paramref name="lowInclusive"/>, <paramref name="highExclusive"/>)</c>
    /// without leaving either half declaring an empty range.
    /// <para>
    /// Both comparisons are strict. The high side matches
    /// <see cref="SplitBoundary.Owns"/>, because a pivot equal to the high bound
    /// would seed the sibling with <c>[High, High)</c>. The low side deliberately
    /// does <b>not</b>: ownership admits <c>pivot == Low</c>, which would seal the
    /// donor at <c>[Low, Low)</c>. Both are fatal and neither is recoverable, so
    /// the rule has to be two-sided.
    /// </para>
    /// </summary>
    /// <param name="pivot">
    /// The candidate pivot, or <see langword="null"/> when no key was selected.
    /// A null pivot is never admissible.
    /// </param>
    /// <param name="lowInclusive">
    /// The leaf's inclusive low bound, or <see langword="null"/> for no lower
    /// constraint. An unbounded low side cannot be emptied, so it admits anything.
    /// </param>
    /// <param name="highExclusive">
    /// The leaf's exclusive high bound, or <see langword="null"/> for no upper
    /// constraint. An unbounded high side cannot be emptied, so it admits anything.
    /// </param>
    public static bool IsAdmissible(string? pivot, string? lowInclusive, string? highExclusive)
    {
        if (pivot is null)
        {
            return false;
        }

        return (lowInclusive is null || string.CompareOrdinal(pivot, lowInclusive) > 0)
            && (highExclusive is null || string.CompareOrdinal(pivot, highExclusive) < 0);
    }

    /// <summary>
    /// Selects the median admissible key from <paramref name="orderedKeys"/>, or
    /// <see langword="null"/> when no key falls strictly inside the declared range.
    /// <para>
    /// A null result means the leaf owns nothing it is entitled to divide, and the
    /// caller must decline the split rather than force one. Declining is
    /// self-correcting rather than a wedge: the leaf's rows are all out of span, so
    /// they drain to their real custodians, after which the leaf is either back
    /// under its capacity threshold or divisible normally.
    /// </para>
    /// <para>
    /// Two passes rather than one pass into a list: the admissible count is not
    /// known ahead of time, and this runs only on the cold repair path, so a second
    /// walk is cheaper than the intermediate buffer a single pass would need.
    /// Nothing is allocated here beyond the enumerators
    /// <paramref name="orderedKeys"/> itself hands out.
    /// </para>
    /// </summary>
    /// <param name="orderedKeys">
    /// The leaf's keys in ascending ordinal order. Only the relative order matters;
    /// the median is taken over the admissible subsequence, not over the whole.
    /// </param>
    /// <param name="lowInclusive">The leaf's inclusive low bound, or <see langword="null"/>.</param>
    /// <param name="highExclusive">The leaf's exclusive high bound, or <see langword="null"/>.</param>
    public static string? SelectMedianAdmissible(
        IEnumerable<string> orderedKeys, string? lowInclusive, string? highExclusive)
    {
        ArgumentNullException.ThrowIfNull(orderedKeys);

        var admissible = 0;
        foreach (var key in orderedKeys)
        {
            if (IsAdmissible(key, lowInclusive, highExclusive))
            {
                admissible++;
            }
        }

        if (admissible == 0)
        {
            return null;
        }

        var target = admissible / 2;
        var seen = 0;
        foreach (var key in orderedKeys)
        {
            if (!IsAdmissible(key, lowInclusive, highExclusive))
            {
                continue;
            }

            if (seen++ == target)
            {
                return key;
            }
        }

        return null;
    }
}
