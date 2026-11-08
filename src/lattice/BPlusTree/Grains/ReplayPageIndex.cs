using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The owner index of one <see cref="ReplayPage"/> (issue #5997): which of the
/// page's positions a <see cref="WalKeyFilter"/> keeps, found without visiting
/// the entries it excludes.
/// </summary>
/// <remarks>
/// <para>
/// A cold restart projects each leaf's slice from pages that carry every key of
/// the partition, and a leaf owns a small fraction of them. Visiting every entry
/// to apply <see cref="WalKeyFilter.Excludes"/> made projection cost
/// proportional to leaves x log. The index answers the same question from three
/// structures built once per page:
/// </para>
/// <list type="bullet">
/// <item>the positions of the key-scoped, keyed entries, ordered by key under
/// ordinal comparison - the order <see cref="SplitBoundary.Owns"/> compares in -
/// so the filter's key range is two binary searches;</item>
/// <item>each such entry's virtual slot, from <see cref="ShardMap.GetVirtualSlot"/>
/// over the filter's <see cref="WalKeyFilter.VirtualShardCount"/>, computed once
/// per slot count and cached, so the shard axis is a bit test;</item>
/// <item>the positions of the entries no filter excludes - every kind that is not
/// key-scoped, and a key-scoped entry with no key - which every reader receives.</item>
/// </list>
/// <para>
/// Together these are exactly <see cref="WalKeyFilter.Excludes"/>: an entry is
/// kept when it is never excludable, or its key lies in the range and, under a
/// shard constraint, its slot is owned. The index is immutable after
/// construction apart from the slot cache, which is published whole and only
/// ever replaced by an equal-valued table for another slot count.
/// </para>
/// </remarks>
internal sealed class ReplayPageIndex
{
    private readonly CommitLogSliceEntry[] _entries;
    private readonly int[] _byKey;
    private readonly int[] _always;
    private SlotTable? _slots;

    /// <summary>Builds the index of <paramref name="entries"/>.</summary>
    /// <param name="entries">The page's entries, ascending by offset. Not copied; never mutated.</param>
    public ReplayPageIndex(CommitLogSliceEntry[] entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        _entries = entries;

        var keyed = 0;
        for (var i = 0; i < entries.Length; i++)
        {
            if (IsExcludable(in entries[i]))
                keyed++;
        }

        var byKey = new int[keyed];
        var always = new int[entries.Length - keyed];
        int k = 0, a = 0;
        for (var i = 0; i < entries.Length; i++)
        {
            if (IsExcludable(in entries[i]))
                byKey[k++] = i;
            else
                always[a++] = i;
        }

        // Ties keep position order, so equal keys stay ascending by offset.
        Array.Sort(byKey, (x, y) =>
        {
            var c = string.CompareOrdinal(entries[x].Mutation.Key, entries[y].Mutation.Key);
            return c != 0 ? c : x.CompareTo(y);
        });

        _byKey = byKey;
        _always = always;
    }

    /// <summary>
    /// The bytes the index holds once its slot table is built: one position per
    /// entry across the two position arrays, plus a slot table of one slot per
    /// entry. Charged to the page's estimate so the ring's ledger accounts for it.
    /// </summary>
    public long EstimatedBytes => 96 + (8L * _entries.Length);

    /// <summary>
    /// Writes into <paramref name="kept"/>, ascending, the positions in
    /// <c>[<paramref name="start"/>, <paramref name="end"/>)</c> that
    /// <paramref name="filter"/> does not exclude, and returns how many were
    /// written. <paramref name="kept"/> must hold at least
    /// <c><paramref name="end"/> - <paramref name="start"/></c> positions.
    /// </summary>
    public int Collect(in WalKeyFilter filter, int start, int end, Span<int> kept)
    {
        if (start >= end)
            return 0;

        var count = 0;
        if (filter.IsUnbounded)
        {
            for (var p = start; p < end; p++)
                kept[count++] = p;
            return count;
        }

        var low = filter.LowKeyInclusive is { } lowKey ? LowerBound(lowKey) : 0;
        var high = filter.HighKeyExclusive is { } highKey ? LowerBound(highKey) : _byKey.Length;
        var slots = filter.HasShardConstraint ? SlotsFor(filter.VirtualShardCount) : null;
        for (var i = low; i < high; i++)
        {
            var p = _byKey[i];
            if (p < start || p >= end)
                continue;
            if (slots is not null && !filter.OwnsSlot(slots[p]))
                continue;
            kept[count++] = p;
        }

        var first = Array.BinarySearch(_always, start);
        var alwaysFrom = first < 0 ? ~first : first;
        var i2 = alwaysFrom;
        for (; i2 < _always.Length && _always[i2] < end; i2++)
            _ = _always[i2]; // DO NOT MERGE: red for #5997 - drop the never-excludable entries

        VisitedForTest += Math.Max(0, high - low) + (i2 - alwaysFrom);

        if (count > 1)
            kept[..count].Sort();
        return count;
    }

    /// <summary>
    /// The entries this thread's <see cref="Collect"/> calls have visited, for
    /// the detector that proves the index never visits an entry outside a
    /// filter's key range or the never-excludable set.
    /// </summary>
    [ThreadStatic]
    internal static long VisitedForTest;

    /// <summary>Whether a filter can exclude this entry: key-scoped, with a key.</summary>
    private static bool IsExcludable(in CommitLogSliceEntry entry) =>
        WalKeyFilter.IsKeyScoped(entry.Mutation.Kind) && entry.Mutation.Key is not null;

    /// <summary>The first rank in key order whose key is at or above <paramref name="key"/>.</summary>
    private int LowerBound(string key)
    {
        int lo = 0, hi = _byKey.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (string.CompareOrdinal(_entries[_byKey[mid]].Mutation.Key, key) < 0)
                lo = mid + 1;
            else
                hi = mid;
        }

        return lo;
    }

    /// <summary>The virtual slot of every keyed position for <paramref name="virtualShardCount"/>.</summary>
    private int[] SlotsFor(int virtualShardCount)
    {
        var table = Volatile.Read(ref _slots);
        if (table is not null && table.VirtualShardCount == virtualShardCount)
            return table.Slots;

        var slots = new int[_entries.Length];
        foreach (var p in _byKey)
            slots[p] = ShardMap.GetVirtualSlot(_entries[p].Mutation.Key!, virtualShardCount);
        Volatile.Write(ref _slots, new SlotTable(virtualShardCount, slots));
        return slots;
    }

    private sealed record SlotTable(int VirtualShardCount, int[] Slots);
}
