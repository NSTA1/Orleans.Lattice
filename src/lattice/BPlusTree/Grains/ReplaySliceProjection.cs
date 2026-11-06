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
    /// <param name="run">Pages in ascending order, each starting at or below the previous page's <see cref="ReplayPage.CoveredThroughInclusive"/>.</param>
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

        foreach (var page in run)
        {
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
                    result.Add(entry);
                }

                if (examined >= budget)
                {
                    budgetSpent = true;
                    break;
                }
            }

            if (budgetSpent || covered >= toInclusive)
                break;

            // A page's coverage ends at its last returned entry, even when that
            // entry lies below the window.
            if (page.CoveredThroughInclusive > covered)
                covered = Math.Min(page.CoveredThroughInclusive, toInclusive);
        }

        if (trailingExcluded)
            result.Add(trailing with { Mutation = WalFilteredRead.RoutingOnly(trailing.Mutation) });

        var complete = budgetSpent || covered >= toInclusive;
        return (result.ToArray(), complete, covered);
    }
}
