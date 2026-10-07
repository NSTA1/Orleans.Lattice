namespace Orleans.Lattice.Replication;

/// <summary>
/// Reads a tree's registry lineage on this cluster (issue #4586 part 2b): the
/// identity the tree registry re-stamps whenever the tree's contents are
/// replaced. The receiver's tree frontier compares it on activation with the
/// lineage it last observed, so a replacement it was not told about still
/// forces a gap. <see langword="null"/> means the registry tracks no lineage for
/// the tree, and the receiver stays in degraded mode for it.
/// </summary>
internal interface ITreeLineageSource
{
    /// <summary>The registry lineage of <paramref name="treeId"/>, or <see langword="null"/> when none is tracked.</summary>
    Task<Guid?> GetLineageAsync(string treeId, CancellationToken cancellationToken = default);
}