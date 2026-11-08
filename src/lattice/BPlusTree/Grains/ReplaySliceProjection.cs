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

        var projection = ProjectIndexed(run, fromExclusive, toInclusive, budget, int.MaxValue, null, filter);
        return (projection.Slice, projection.Complete, projection.CoveredThroughInclusive);
    }

    /// <summary>
    /// How many times <see cref="LatticeOptions.WalReplaySliceBudget"/> a
    /// ring-served slice may examine (issue #5997). Examining is cheap once the
    /// owner index skips what a filter excludes, so a slice served from the ring
    /// spans a wider window and a leaf makes this many times fewer calls; what it
    /// returns stays bounded by the budget.
    /// </summary>
    internal const int RingExamineFactor = 64;

    /// <summary>
    /// Projects a wide slice from <paramref name="run"/> for a ring serve (issue
    /// #5997): examines up to <see cref="RingExamineFactor"/> x
    /// <paramref name="budget"/> entries, returns at most
    /// <paramref name="budget"/> kept entries plus one trailing routing-only
    /// marker, and widens only over pages read at
    /// <paramref name="incarnation"/>.
    /// </summary>
    /// <returns>
    /// The slice equals the concatenation of the consecutive budget-sized
    /// filtered reads over the window it covers, with their intermediate
    /// routing-only markers elided: a marker only advances a reader past entries
    /// it does not own, and the next returned entry advances it further. It is
    /// complete when a bound was reached - the examine cap, the return cap or
    /// <paramref name="toInclusive"/> - and otherwise ends where the run's
    /// coverage at this incarnation ends, at
    /// <see cref="IndexedProjection.CoveredThroughInclusive"/>.
    /// </returns>
    internal static IndexedProjection ProjectWide(
        IReadOnlyList<ReplayPage> run,
        long fromExclusive,
        long toInclusive,
        int budget,
        WalKeyFilter filter,
        long incarnation)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentOutOfRangeException.ThrowIfLessThan(budget, 1);

        var examine = (int)Math.Min(int.MaxValue, (long)budget * RingExamineFactor);
        return ProjectIndexed(run, fromExclusive, toInclusive, examine, budget, incarnation, filter);
    }

    /// <summary>
    /// The outcome of an indexed projection: the slice, whether it is complete,
    /// how far it covers, and how many entries it examined.
    /// </summary>
    internal readonly record struct IndexedProjection(
        CommitLogSliceEntry[] Slice, bool Complete, long CoveredThroughInclusive, long Examined);

    /// <summary>
    /// <see cref="ProjectLinear"/> computed through each page's
    /// <see cref="ReplayPage.Index"/>: the same entries, order, coverage,
    /// completeness and trailing marker, without visiting what the filter
    /// excludes (issue #5997). With an unbounded <paramref name="returnBudget"/>
    /// and no <paramref name="incarnation"/> it is exactly the narrow projection.
    /// </summary>
    private static IndexedProjection ProjectIndexed(
        IReadOnlyList<ReplayPage> run,
        long fromExclusive,
        long toInclusive,
        int examineBudget,
        int returnBudget,
        long? incarnation,
        WalKeyFilter filter)
    {
        var ordered = InOffsetOrder(run);
        long examined = 0;
        var kept = 0;
        var covered = fromExclusive;
        var trailingExcluded = false;
        var trailing = default(CommitLogSliceEntry);
        var budgetSpent = false;
        CommitLogSliceEntry[]? buffer = null;
        int[]? positions = null;
        try
        {
            for (var r = 0; r < ordered.Count; r++)
            {
                var page = ordered[r];
                if (page.CoveredThroughInclusive <= covered)
                    continue;

                // Contiguity: a page that starts above what is already covered
                // would stitch across an uncovered gap.
                if (page.FromExclusive > covered)
                    break;

                // A wide serve never widens across an incarnation change.
                if (incarnation is { } current && page.Incarnation != current)
                    break;

                var entries = page.Entries;
                var start = FirstAbove(entries, covered);
                var end = Math.Min(FirstAbove(entries, toInclusive), start + (int)Math.Min(int.MaxValue, examineBudget - examined));
                if (start < end)
                {
                    if (positions is null || positions.Length < end - start)
                    {
                        if (positions is not null)
                            System.Buffers.ArrayPool<int>.Shared.Return(positions);
                        positions = System.Buffers.ArrayPool<int>.Shared.Rent(end - start);
                    }

                    var found = page.Index.Collect(in filter, start, end, positions);

                    // The return cap ends the slice at its last returned entry.
                    if (kept + found >= returnBudget)
                    {
                        found = returnBudget - kept;
                        end = positions[found - 1] + 1;
                        budgetSpent = true;
                    }

                    if (buffer is null || buffer.Length < kept + found)
                    {
                        var grown = System.Buffers.ArrayPool<CommitLogSliceEntry>.Shared.Rent(Math.Max(kept + found, 16));
                        if (buffer is not null)
                        {
                            Array.Copy(buffer, grown, kept);
                            System.Buffers.ArrayPool<CommitLogSliceEntry>.Shared.Return(buffer, clearArray: true);
                        }

                        buffer = grown;
                    }

                    for (var i = 0; i < found; i++)
                        buffer[kept++] = Detached(entries[positions[i]]);

                    // DO NOT MERGE: red for #5997 - a wide serve keeps an intermediate marker per page.
                    if (incarnation is not null && found > 0 && positions[found - 1] != end - 1 && end < entries.Length && kept < buffer.Length)
                        buffer[kept++] = entries[end - 1] with { Mutation = WalFilteredRead.RoutingOnly(entries[end - 1].Mutation) };

                    examined += end - start;
                    covered = entries[end - 1].Offset;
                    trailingExcluded = found == 0 || positions[found - 1] != end - 1;
                    if (trailingExcluded)
                        trailing = entries[end - 1];

                    if (examined >= examineBudget)
                        budgetSpent = true;
                }

                if (budgetSpent || covered >= toInclusive)
                    break;

                // A page covers through its last returned entry (condition A-b).
                if (page.CoveredThroughInclusive > covered)
                    covered = Math.Min(page.CoveredThroughInclusive, toInclusive);
            }

            var length = kept + (trailingExcluded ? 1 : 0);
            var slice = length == 0 ? [] : new CommitLogSliceEntry[length];
            if (kept > 0)
                Array.Copy(buffer!, slice, kept);
            if (trailingExcluded)
                slice[kept] = trailing with { Mutation = WalFilteredRead.RoutingOnly(trailing.Mutation) };

            var complete = budgetSpent || covered >= toInclusive;
            return new IndexedProjection(slice, complete, covered, examined);
        }
        finally
        {
            if (positions is not null)
                System.Buffers.ArrayPool<int>.Shared.Return(positions);
            if (buffer is not null)
                System.Buffers.ArrayPool<CommitLogSliceEntry>.Shared.Return(buffer, clearArray: true);
        }
    }

    /// <summary>The run in ascending start order, sorting only when it is not already.</summary>
    private static IReadOnlyList<ReplayPage> InOffsetOrder(IReadOnlyList<ReplayPage> run)
    {
        if (run is ReplayPageRing.RunView { Sorted: true })
            return run;

        for (var i = 1; i < run.Count; i++)
        {
            if (run[i - 1].FromExclusive > run[i].FromExclusive)
                return [.. run.OrderBy(p => p.FromExclusive)];
        }

        return run;
    }

    /// <summary>The first position whose offset is above <paramref name="offset"/>; entries ascend by offset.</summary>
    private static int FirstAbove(CommitLogSliceEntry[] entries, long offset)
    {
        int lo = 0, hi = entries.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (entries[mid].Offset <= offset)
                lo = mid + 1;
            else
                hi = mid;
        }

        return lo;
    }

    /// <summary>
    /// The reference projection (issue #5896): visits every entry of the window
    /// and applies <see cref="WalKeyFilter.Excludes"/> to each. Retained as the
    /// specification <see cref="Project"/> is proven equal to (issue #5997).
    /// </summary>
    internal static (CommitLogSliceEntry[] Slice, bool Complete, long CoveredThroughInclusive) ProjectLinear(
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

        var complete = budgetSpent || covered >= toInclusive;
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
