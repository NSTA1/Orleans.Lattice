namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A shard root's answer to whether descent for a leaf's low bound reaches that leaf
/// (#5876, a5e2e2ef's D1). A descent that overlapped a mutation of the shard's routing
/// structure - a link batch, a root promotion, a reclaim's unlink, a bulk graft or a
/// shard split - is <see cref="Undecided"/>, never a verdict: it may have seen the
/// structure half-applied.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.DescentVerdict)]
internal enum DescentVerdict : byte
{
    /// <summary>Descent, over a settled structure, did not reach the leaf.</summary>
    Unreachable = 0,

    /// <summary>Descent, over a settled structure, reached the leaf.</summary>
    Reachable = 1,

    /// <summary>The structure was in motion before, during or after the descent; no verdict.</summary>
    Undecided = 2,
}
