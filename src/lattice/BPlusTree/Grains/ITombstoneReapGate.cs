namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bounds which tombstones a compaction pass may reap (issue #4615). The grace
/// period alone is a wall-clock bound: on a replicated tree, a write older than
/// the delete but delivered after the reap would find no tombstone and resurrect
/// the key on that replica only. A gate returns the HLC below which no write a
/// tombstone beats can still be delivered here, and the pass reaps only below
/// it.
/// </summary>
internal interface ITombstoneReapGate
{
    /// <summary>
    /// The reap ceiling for <paramref name="treeId"/>, or <see langword="null"/>
    /// when the tree is ungated and reaps on the grace period alone.
    /// <see cref="HybridLogicalClock.Zero"/> reaps nothing. A failure must
    /// propagate, so the pass reaps nothing rather than reaping ungated.
    /// </summary>
    Task<HybridLogicalClock?> GetReapCeilingAsync(string treeId, CancellationToken cancellationToken = default);
}
