namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Chooses which shard's row an entries scan takes when several of its cursors
/// hold a row for the same key (issue #4361).
/// <para>
/// A point read routes a key through the shard map and asks only the shard that
/// owns its slot. A full scan opens a cursor on every physical shard and merges
/// their rows, and a shard filters only the slots it records as moved away. That
/// leaves rows a shard holds for slots it never owned: an atomic write's
/// cross-migration backstop hands every transitively-discovered split shard the
/// batch's whole value set, and the shard writes each key into its own leaves. The
/// merge used to keep whichever copy it dequeued first, so a scan could return a
/// key at a value from rounds earlier - and a schema remediation, which copies a
/// tree by scanning it, wrote that stale value into its destination.
/// </para>
/// <para>
/// Every cursor holding a row for a key is tied at the top of the merge when the key
/// is reached, so the scan takes the owner's row from among them. It never drops a
/// key that only a non-owner holds: a consolidation's survivor holds a retired
/// donor's keys before a scan opened under the older map has learned the donor is
/// gone, and dropping them would yield those keys later and out of order.
/// </para>
/// </summary>
internal static class ScanOwnership
{
    /// <summary>
    /// Returns the index of the cursor whose row for <paramref name="key"/> the scan
    /// takes, from <paramref name="firstIndex"/> and the cursors in
    /// <paramref name="tiedIndices"/> that hold a row for the same key. A
    /// reconciliation drain cursor (any index beyond the live shard cursors) wins: it
    /// was read from the key's owner under a newer map. Otherwise the live cursor on
    /// the shard <paramref name="routedMap"/> routes the key to wins. Failing both,
    /// the first cursor's row is taken, as before.
    /// </summary>
    /// <param name="routedMap">The shard map the live shard cursors were opened under.</param>
    /// <param name="liveShards">The physical shard index each live cursor reads, by cursor index.</param>
    /// <param name="firstIndex">The cursor dequeued first for the key.</param>
    /// <param name="tiedIndices">The other cursors holding a row for the key.</param>
    /// <param name="key">The key.</param>
    public static int PickOwnerRow(
        ShardMap routedMap,
        IReadOnlyList<int> liveShards,
        int firstIndex,
        IReadOnlyList<int> tiedIndices,
        string key)
    {
        ArgumentNullException.ThrowIfNull(routedMap);
        ArgumentNullException.ThrowIfNull(liveShards);
        ArgumentNullException.ThrowIfNull(tiedIndices);
        ArgumentNullException.ThrowIfNull(key);

        var owner = routedMap.Resolve(key);
        int? routed = null;
        for (var i = -1; i < tiedIndices.Count; i++)
        {
            var index = i < 0 ? firstIndex : tiedIndices[i];
            if (index >= liveShards.Count)
            {
                return index;
            }

            if (routed is null && liveShards[index] == owner)
            {
                routed = index;
            }
        }

        return routed ?? firstIndex;
    }
}
