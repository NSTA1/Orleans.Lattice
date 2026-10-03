namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Decides whether a row a full-tree scan's per-shard cursor produced may be taken
/// as the key's value (issue #4361).
/// <para>
/// A point read routes a key through the shard map and asks only the shard that
/// owns its slot, and a count asks each shard only for the slots the map routes to
/// it. A full scan opens a cursor on every physical shard and merges their rows,
/// and a shard filters only the slots it records as moved away. That leaves rows a
/// shard holds for slots it never owned: an atomic write's cross-migration backstop
/// hands every transitively-discovered split shard the batch's whole value set, and
/// the shard writes each key into its own leaves. Those rows are invisible to a
/// point read and to a count, but a scan merged them with the owner's row and kept
/// whichever it dequeued first, so it could return a key at a value from rounds
/// earlier - and a schema remediation, which copies a tree by scanning it, wrote
/// that stale value into its destination.
/// </para>
/// <para>
/// A scan therefore takes a row from a live shard cursor only when the map the
/// cursors were opened under routes the key to that cursor's shard. A slot that
/// moves after the scan opened is reconciled exactly as before: the previous owner
/// reports it as moved away and the scan drains it from the new owner, and those
/// drained rows - read only for the slots their shard owns - are not filtered.
/// </para>
/// </summary>
internal static class ScanOwnership
{
    /// <summary>
    /// Returns <see langword="true"/> when the row with <paramref name="key"/> that
    /// cursor <paramref name="cursorIndex"/> produced may be taken: the cursor is not
    /// one of the live shard cursors (it is a reconciliation drain, already scoped to
    /// its owner's slots), or <paramref name="routedMap"/> routes the key to the shard
    /// the cursor reads.
    /// </summary>
    /// <param name="routedMap">The shard map the live shard cursors were opened under.</param>
    /// <param name="liveShards">The physical shard index each live cursor reads, by cursor index.</param>
    /// <param name="cursorIndex">The index of the cursor that produced the row.</param>
    /// <param name="key">The row's key.</param>
    public static bool IsRoutedRow(ShardMap routedMap, IReadOnlyList<int> liveShards, int cursorIndex, string key)
    {
        ArgumentNullException.ThrowIfNull(routedMap);
        ArgumentNullException.ThrowIfNull(liveShards);
        ArgumentNullException.ThrowIfNull(key);
        return cursorIndex >= liveShards.Count || routedMap.Resolve(key) == liveShards[cursorIndex];
    }
}
