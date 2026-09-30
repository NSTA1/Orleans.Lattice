namespace Orleans.Lattice.Replication;

/// <summary>
/// The single total order the peer-status read path pages in, shared by every
/// silo's local read and the cross-silo merge so a cursor means the same thing
/// everywhere. Rows are ordered by effective tree id, then by peer cluster id,
/// then by direction - all ordinal.
/// </summary>
/// <remarks>
/// The tree id is the effective id exactly as the telemetry state records it, so
/// a tenant's own tree orders (and is reported) under its tenant-qualified
/// <c>t/{tenant}/{name}</c> id - the id the replication enrolment report names it
/// by - and no rendering is applied on the read path (issue #4000). The cursor
/// therefore carries the id the caller was shown, and nothing more.
/// </remarks>
internal static class ReplicationPeerStatusOrder
{
    /// <summary>Compares two keys in the read order.</summary>
    /// <returns>Negative, zero or positive as the first key orders before, with, or after the second.</returns>
    public static int Compare(
        string treeA,
        string peerA,
        ReplicationContactDirection directionA,
        string treeB,
        string peerB,
        ReplicationContactDirection directionB)
    {
        var order = string.CompareOrdinal(treeA, treeB);
        if (order != 0)
        {
            return order;
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
        string tree,
        string peer,
        ReplicationContactDirection direction,
        in ReplicationPeerStatusCursor cursor) =>
        Compare(tree, peer, direction, cursor.Tree, cursor.Peer, cursor.Direction);

    /// <summary>Compares two rows in the read order.</summary>
    /// <returns>Negative, zero or positive as <paramref name="a"/> orders before, with, or after <paramref name="b"/>.</returns>
    public static int Compare(in ReplicationPeerStatusRow a, in ReplicationPeerStatusRow b) =>
        Compare(a.Tree, a.Peer, a.Direction, b.Tree, b.Peer, b.Direction);

    /// <summary>Builds the cursor that resumes strictly after <paramref name="row"/>.</summary>
    /// <param name="row">The last row handed to the caller.</param>
    /// <returns>The cursor keyed on the row's effective tree id, peer and direction.</returns>
    public static ReplicationPeerStatusCursor CursorAfter(in ReplicationPeerStatusRow row) =>
        new(row.Tree, row.Peer, row.Direction);
}
