namespace Orleans.Lattice;

/// <summary>
/// Collapses a batch of <see cref="ViewWrite"/>s to at most one survivor per
/// view key, keeping the write with the highest source
/// <see cref="ViewWrite.Timestamp"/> (last-writer-wins). The maintainer runs
/// this over each batch it reads off the source write-ahead log before applying
/// to the view tree, so repeated or reordered writes to the same key within a
/// batch converge to the same survivor independent of arrival order.
/// </summary>
public static class ViewWriteCoalescer
{
    /// <summary>
    /// Returns one survivor per <see cref="ViewWrite.Key"/> from
    /// <paramref name="writes"/>, each being the highest-<see cref="ViewWrite.Timestamp"/>
    /// write for its key. A write whose timestamp ties an existing survivor does
    /// not displace it (first-seen wins on an exact tie), so the result is
    /// deterministic for a fixed input order. The survivors are returned in the
    /// order their keys were first seen.
    /// </summary>
    /// <param name="writes">The batch of writes to coalesce. Must not be <see langword="null"/>.</param>
    public static IReadOnlyList<ViewWrite> Coalesce(IEnumerable<ViewWrite> writes)
    {
        ArgumentNullException.ThrowIfNull(writes);

        // Presize both buffers to the batch size when it is cheaply known (the
        // maintainer always passes a materialised List), so the survivor list and
        // the key index do not regrow their backing arrays as the batch is folded.
        // The capacity is bounded by the actual input count - at most one survivor
        // and one index entry per input write - so a small batch presizes small;
        // this is not the fixed-batchSize presize that over-allocates on sparse
        // drains.
        var capacity = writes.TryGetNonEnumeratedCount(out var count) ? count : 0;
        var index = new Dictionary<string, int>(capacity, StringComparer.Ordinal);
        var survivors = new List<ViewWrite>(capacity);

        foreach (var write in writes)
        {
            if (index.TryGetValue(write.Key, out var existingPos))
            {
                if (write.Timestamp.CompareTo(survivors[existingPos].Timestamp) > 0)
                {
                    survivors[existingPos] = write;
                }
            }
            else
            {
                index[write.Key] = survivors.Count;
                survivors.Add(write);
            }
        }

        return survivors;
    }
}
