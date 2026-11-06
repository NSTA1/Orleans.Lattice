namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Projects a leaf's filtered slice out of cached, unfiltered
/// <see cref="ReplayPage"/>s (issue #5896, condition A-a), applying exactly the
/// rule <see cref="WalFilteredRead"/> applies to a storage read, so a slice
/// served from the ring cannot differ from one read directly.
/// </summary>
internal static class ReplaySliceProjection
{
    /// <summary>
    /// Projects the slice for <c>(<paramref name="fromExclusive"/>, <paramref name="toInclusive"/>]</c>
    /// from <paramref name="run"/>, a contiguous run of pages whose first page
    /// covers <paramref name="fromExclusive"/>.
    /// </summary>
    /// <param name="run">Pages in any order; they are walked by ascending start offset, and only while each starts at or below what is already covered, so a page beyond an uncovered gap is never stitched in.</param>
    /// <param name="fromExclusive">The exclusive lower bound of the window.</param>
    /// <param name="toInclusive">The inclusive upper bound of the window.</param>
    /// <param name="budget">Maximum entries to examine; at least 1.</param>
    /// <param name="filter">The reader's ownership.</param>
    /// <returns>
    /// The projected slice, and whether it is <b>complete</b>: equal to what a
    /// direct filtered read of the window would return. It is complete when the
    /// budget was exhausted, or when the run covers the window through
    /// <paramref name="toInclusive"/>. An incomplete projection is still a correct
    /// prefix, but the caller must extend the run from
    /// <c>CoveredThrough</c> by a fill before serving it as the whole window
    /// (condition A-b): nothing above the run's coverage is ever treated as empty.
    /// </returns>
    public static (CommitLogSliceEntry[] Slice, bool Complete, long CoveredThroughInclusive) Project(
        IReadOnlyList<ReplayPage> run,
        long fromExclusive,
        long toInclusive,
        int budget,
        WalKeyFilter filter)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentOutOfRangeException.ThrowIfLessThan(budget, 1);

        var result = new List<CommitLogSliceEntry>();
        var examined = 0;
        var covered = fromExclusive;
        var trailingExcluded = false;
        var trailing = default(CommitLogSliceEntry);
        var budgetSpent = false;

        var ordered = run.Count <= 1 ? run : [.. run.OrderBy(p => p.FromExclusive)];
        foreach (var page in ordered)
        {
            if (page.CoveredThroughInclusive <= covered)
                continue;

            // Contiguity: a page that starts above what is already covered
            // would stitch across an uncovered gap.
            if (page.FromExclusive > covered)
                break;

            foreach (var entry in page.Entries)
            {
                if (entry.Offset <= covered)
                    continue;
                if (entry.Offset > toInclusive)
                    break;

                examined++;
                covered = entry.Offset;
                if (filter.Excludes(entry.Mutation.Kind, entry.Mutation.Key))
                {
                    trailingExcluded = true;
                    trailing = entry;
                }
                else
                {
                    trailingExcluded = false;
                    result.Add(Detached(entry));
                }

                if (examined >= budget)
                {
                    budgetSpent = true;
                    break;
                }
            }

            if (budgetSpent || covered >= toInclusive)
                break;

            // A page covers through its last returned entry (condition A-b).
            // When that entry lies above the window, the page has shown that
            // nothing lies between the last entry examined and the window's end.
            if (page.CoveredThroughInclusive > covered)
                covered = Math.Min(page.CoveredThroughInclusive, toInclusive);
        }

        if (trailingExcluded)
            result.Add(trailing with { Mutation = WalFilteredRead.RoutingOnly(trailing.Mutation) });

        var complete = budgetSpent || run.Count > 0;
        return (result.ToArray(), complete, covered);
    }

    /// <summary>
    /// A copy of <paramref name="entry"/> that shares no mutable state with the
    /// cached page (condition A-d): its value bytes and version vector are
    /// copied, so whatever one leaf does with what it was handed, the next leaf
    /// served from the same page receives the bytes storage returned. A
    /// key-scoped entry reaches only its owning leaf, so this costs about one
    /// copy per entry, as each leaf decoding its own read did.
    /// </summary>
    private static CommitLogSliceEntry Detached(CommitLogSliceEntry entry)
    {
        var mutation = entry.Mutation;
        if (mutation.Value is null && mutation.VectorClock is null)
            return entry;

        return entry with
        {
            Mutation = mutation with
            {
                Value = mutation.Value?.ToArray(),
                VectorClock = mutation.VectorClock?.Clone(),
            },
        };
    }
}
