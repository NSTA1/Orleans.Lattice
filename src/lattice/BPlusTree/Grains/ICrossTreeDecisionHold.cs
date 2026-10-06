namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Keeps a cross-tree sub-saga's decision until no replication peer can still
/// need it (issue #4684). A receiver that imports one participant tree of a
/// replicated cross-tree atomic write settles the tree's arrival at its
/// cross-tree barrier from the decision row the export carries; once the
/// origin has purged the row, the import carries only committed rows and a
/// sibling tree that already delegated to the barrier waits for ever. The
/// replication layer registers an implementation; a host without one purges
/// cross-tree decisions on the ordinary rules.
/// </summary>
internal interface ICrossTreeDecisionHold
{
    /// <summary>
    /// Whether the expired tombstone of sub-saga <paramref name="txid"/> of
    /// <paramref name="treeId"/>, which belongs to the cross-tree write
    /// <paramref name="membership"/>, may be physically purged. A failure must
    /// propagate, so the registry keeps the tombstone.
    /// </summary>
    Task<bool> MayPurgeAsync(string treeId, Guid txid, CrossTreeMembership membership, CancellationToken cancellationToken = default);
}
