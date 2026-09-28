namespace Orleans.Lattice.Replication;

/// <summary>
/// Folds the per-silo answers of one peer-status read into a single page.
/// <see cref="ReplicationPeerStats"/> is a per-silo singleton: an outbound row
/// lives on whichever silo hosts that <c>(tree, peer)</c> shipper (and a stale
/// copy stays behind on a silo it was rebalanced away from), and an inbound row
/// lives on every silo that applied a batch from that origin. The same key can
/// therefore arrive from several silos, and exactly one copy is kept.
/// </summary>
/// <remarks>
/// <para>
/// <b>Which copy wins.</b> The copy with the most recent successful contact is the
/// live one, so it is kept whole (its backlog, error streak and in-flight depth
/// are reported together, never mixed across silos). When no copy has ever made
/// contact the one with the longest error streak - then the larger backlog - is
/// kept, so a failing link is never masked by a quieter copy.
/// </para>
/// <para>
/// <b>Why the first <c>limit</c> rows of each silo suffice.</b> Every silo returns
/// its first <c>limit</c> keys after the cursor in the shared order. Any key among
/// the first <c>limit</c> distinct keys cluster-wide is preceded, on each silo that
/// holds it, only by keys that also precede it cluster-wide - fewer than
/// <c>limit</c> - so every copy of it is inside every holding silo's answer, and
/// the merge sees all of them.
/// </para>
/// </remarks>
internal static class ReplicationPeerStatusMerge
{
    /// <summary>
    /// Merges the per-silo answers, de-duplicating by <c>(tree, peer, direction)</c>,
    /// and returns at most <paramref name="limit"/> rows in
    /// <see cref="ReplicationPeerStatusOrder"/>.
    /// </summary>
    /// <param name="perSilo">Each silo's sorted, limit-bounded answer. Must not be <see langword="null"/>.</param>
    /// <param name="limit">The maximum number of rows to return; clamped to at least one.</param>
    /// <param name="stripPrefix">The caller's tenant qualification, or <see langword="null"/> for none.</param>
    /// <returns>The merged, ordered rows.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="perSilo"/> is <see langword="null"/>.</exception>
    public static IReadOnlyList<ReplicationPeerStatusRow> Merge(
        IReadOnlyList<IReadOnlyList<ReplicationPeerStatusRow>> perSilo,
        int limit,
        string? stripPrefix)
    {
        ArgumentNullException.ThrowIfNull(perSilo);
        limit = Math.Max(1, limit);

        var byKey = new Dictionary<(string Tree, string Peer, ReplicationContactDirection Direction), ReplicationPeerStatusRow>();
        foreach (var rows in perSilo)
        {
            if (rows is null)
            {
                continue;
            }

            foreach (var row in rows)
            {
                var key = (row.Tree, row.Peer, row.Direction);
                byKey[key] = byKey.TryGetValue(key, out var existing) ? Prefer(existing, row) : row;
            }
        }

        var merged = new List<ReplicationPeerStatusRow>(byKey.Values);
        merged.Sort((a, b) => ReplicationPeerStatusOrder.Compare(a, b, stripPrefix));
        if (merged.Count > limit)
        {
            merged.RemoveRange(limit, merged.Count - limit);
        }

        return merged;
    }

    /// <summary>
    /// Chooses between two copies of the same key reported by different silos.
    /// </summary>
    /// <param name="a">The first copy.</param>
    /// <param name="b">The second copy.</param>
    /// <returns>The copy to report.</returns>
    public static ReplicationPeerStatusRow Prefer(in ReplicationPeerStatusRow a, in ReplicationPeerStatusRow b)
    {
        var aContacted = !double.IsNaN(a.LastContactSeconds);
        var bContacted = !double.IsNaN(b.LastContactSeconds);
        if (aContacted && bContacted)
        {
            return a.LastContactSeconds <= b.LastContactSeconds ? a : b;
        }

        if (aContacted != bContacted)
        {
            return aContacted ? a : b;
        }

        if (a.ConsecutiveErrors != b.ConsecutiveErrors)
        {
            return a.ConsecutiveErrors > b.ConsecutiveErrors ? a : b;
        }

        return a.EntriesBehind >= b.EntriesBehind ? a : b;
    }
}
