namespace Orleans.Lattice.Replication;

/// <summary>
/// The single total order the peer-status read path pages in, shared by every
/// silo's local read and the cross-silo merge so a cursor means the same thing
/// everywhere. Rows are ordered by display tree id, then by whether that id was
/// rendered from the caller's own tenant qualification (rendered first), then by
/// peer cluster id, then by direction - all ordinal.
/// </summary>
/// <remarks>
/// The display tree id is the effective tree id with the caller's own tenant
/// qualification (<c>t/{tenant}/</c>) removed, so a tenant-confined caller sees
/// the logical, tenant-local name (<c>a/{app}/{tree}</c> for an app tree) and is
/// never handed its composed id. Ordering on the display id rather than the
/// effective id is what lets a cursor carry only what the caller was shown.
/// </remarks>
internal static class ReplicationPeerStatusOrder
{
    /// <summary>
    /// Renders <paramref name="tree"/> for a caller whose tenant qualification is
    /// <paramref name="stripPrefix"/>: the qualification is removed when the id
    /// carries it and something follows it; otherwise the id is returned whole.
    /// Allocation-free.
    /// </summary>
    /// <param name="tree">The effective tree id.</param>
    /// <param name="stripPrefix">The caller's tenant qualification, or <see langword="null"/> for none.</param>
    /// <param name="stripped">Whether the qualification was removed.</param>
    /// <returns>The display slice of <paramref name="tree"/>.</returns>
    public static ReadOnlySpan<char> DisplayTree(string tree, string? stripPrefix, out bool stripped)
    {
        if (stripPrefix is { Length: > 0 }
            && tree.Length > stripPrefix.Length
            && tree.StartsWith(stripPrefix, StringComparison.Ordinal))
        {
            stripped = true;
            return tree.AsSpan(stripPrefix.Length);
        }

        stripped = false;
        return tree.AsSpan();
    }

    /// <summary>
    /// Renders <paramref name="tree"/> as a string for a caller whose tenant
    /// qualification is <paramref name="stripPrefix"/>. Returns the input
    /// reference unchanged when nothing is removed.
    /// </summary>
    /// <param name="tree">The effective tree id.</param>
    /// <param name="stripPrefix">The caller's tenant qualification, or <see langword="null"/> for none.</param>
    /// <param name="stripped">Whether the qualification was removed.</param>
    /// <returns>The display tree id.</returns>
    public static string DisplayTreeString(string tree, string? stripPrefix, out bool stripped)
    {
        var display = DisplayTree(tree, stripPrefix, out stripped);
        return stripped ? new string(display) : tree;
    }

    /// <summary>Compares two keys in the read order.</summary>
    /// <returns>Negative, zero or positive as the first key orders before, with, or after the second.</returns>
    public static int Compare(
        ReadOnlySpan<char> treeA,
        bool strippedA,
        string peerA,
        ReplicationContactDirection directionA,
        ReadOnlySpan<char> treeB,
        bool strippedB,
        string peerB,
        ReplicationContactDirection directionB)
    {
        var order = treeA.SequenceCompareTo(treeB);
        if (order != 0)
        {
            return order;
        }

        if (strippedA != strippedB)
        {
            return strippedA ? -1 : 1;
        }

        order = string.CompareOrdinal(peerA, peerB);
        if (order != 0)
        {
            return order;
        }

        return ((int)directionA).CompareTo((int)directionB);
    }

    /// <summary>Compares a row key against a cursor in the read order.</summary>
    /// <returns>Negative, zero or positive as the row orders before, at, or after the cursor.</returns>
    public static int Compare(
        ReadOnlySpan<char> tree,
        bool stripped,
        string peer,
        ReplicationContactDirection direction,
        in ReplicationPeerStatusCursor cursor) =>
        Compare(tree, stripped, peer, direction, cursor.Tree.AsSpan(), cursor.Stripped, cursor.Peer, cursor.Direction);

    /// <summary>Compares two rows in the read order for a caller rendering away <paramref name="stripPrefix"/>.</summary>
    /// <returns>Negative, zero or positive as <paramref name="a"/> orders before, with, or after <paramref name="b"/>.</returns>
    public static int Compare(in ReplicationPeerStatusRow a, in ReplicationPeerStatusRow b, string? stripPrefix)
    {
        var treeA = DisplayTree(a.Tree, stripPrefix, out var strippedA);
        var treeB = DisplayTree(b.Tree, stripPrefix, out var strippedB);
        return Compare(treeA, strippedA, a.Peer, a.Direction, treeB, strippedB, b.Peer, b.Direction);
    }

    /// <summary>Builds the cursor that resumes strictly after <paramref name="row"/>.</summary>
    /// <param name="row">The last row handed to the caller.</param>
    /// <param name="stripPrefix">The caller's tenant qualification, or <see langword="null"/> for none.</param>
    /// <returns>The cursor keyed on the row's display tree id.</returns>
    public static ReplicationPeerStatusCursor CursorAfter(in ReplicationPeerStatusRow row, string? stripPrefix)
    {
        var tree = DisplayTreeString(row.Tree, stripPrefix, out var stripped);
        return new ReplicationPeerStatusCursor(tree, stripped, row.Peer, row.Direction);
    }
}
