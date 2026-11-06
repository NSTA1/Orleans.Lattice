namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// One immutable page of a WAL partition held by a <see cref="ReplayPageRing"/>
/// (issue #5896): the <b>unfiltered</b>, ascending entries a single fill read
/// from storage, from <see cref="FromExclusive"/> up to the last entry the fill
/// actually returned.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="CoveredThroughInclusive"/> is the offset of the last entry the
/// fill returned, never the upper bound it asked for (condition A-b). An offset
/// that was not readable when the page was filled can therefore only lie above
/// it, which is exactly what a direct reader that advances by the last offset it
/// receives would see. A fill that returns nothing creates no page.
/// </para>
/// <para>
/// <see cref="Incarnation"/> is the WAL partition's durable incarnation, read in
/// the same shard turn as the entries (condition I2). A page is served only while
/// the partition is still at that incarnation (condition I3).
/// </para>
/// <para>
/// Pages are never mutated after construction (condition A-d): a slice handed
/// to a leaf is always a fresh array, and entries are shared values.
/// </para>
/// </remarks>
internal sealed class ReplayPage
{
    /// <summary>Creates a page from a fill's returned entries.</summary>
    /// <param name="fromExclusive">The exclusive lower bound the fill read from.</param>
    /// <param name="entries">The unfiltered entries the fill returned, ascending, all above <paramref name="fromExclusive"/>; at least one.</param>
    /// <param name="incarnation">The partition incarnation read with the entries.</param>
    public ReplayPage(long fromExclusive, CommitLogSliceEntry[] entries, long incarnation)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Length == 0)
            throw new ArgumentException("A fill that returned nothing creates no page.", nameof(entries));

        var previous = fromExclusive;
        foreach (var entry in entries)
        {
            if (entry.Offset <= previous)
                throw new ArgumentException("Page entries must be strictly ascending and above the page's lower bound.", nameof(entries));
            previous = entry.Offset;
        }

        FromExclusive = fromExclusive;
        Entries = entries;
        Incarnation = incarnation;
    }

    /// <summary>The exclusive lower bound the fill read from.</summary>
    public long FromExclusive { get; }

    /// <summary>The offset of the last entry the fill returned (condition A-b).</summary>
    public long CoveredThroughInclusive => Entries[^1].Offset;

    /// <summary>The unfiltered entries, ascending. Never mutated.</summary>
    public CommitLogSliceEntry[] Entries { get; }

    /// <summary>The partition incarnation the entries were read at.</summary>
    public long Incarnation { get; }

    /// <summary>Whether the page covers <paramref name="offset"/>'s successor, i.e. a read from <paramref name="offset"/> can start inside it.</summary>
    public bool CoversFrom(long offset) => offset >= FromExclusive && offset < CoveredThroughInclusive;
}
