namespace Orleans.Lattice.Replication;

/// <summary>
/// A read-only request asking a peer cluster for its subtree content digest
/// over a cluster-stable separator-key range <c>[RangeStartKey, RangeEndKey)</c>
/// at a given logical depth, used by the anti-entropy Merkle-walk drift
/// localisation pass to compare divergent subtrees apples-to-apples across
/// clusters that have independent physical B+ tree layouts. Strictly read-only:
/// answering this request must never mutate data or any replication cursor.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.MerkleWalkProbeRequest)]
[Immutable]
public readonly record struct MerkleWalkProbeRequest
{
    /// <summary>The logical replicated-tree name the probe targets.</summary>
    [Id(0)]
    public string TreeName { get; init; }

    /// <summary>The shard index within the tree whose subtree is being probed.</summary>
    [Id(1)]
    public int ShardIndex { get; init; }

    /// <summary>
    /// Inclusive lower separator-key bound of the subtree range, or
    /// <see langword="null"/> for the leftmost (unbounded-below) range.
    /// </summary>
    [Id(2)]
    public string? RangeStartKey { get; init; }

    /// <summary>
    /// Exclusive upper separator-key bound of the subtree range, or
    /// <see langword="null"/> for the rightmost (unbounded-above) range.
    /// </summary>
    [Id(3)]
    public string? RangeEndKey { get; init; }

    /// <summary>
    /// The logical depth in the internal-node tree at which the local side is
    /// probing - <c>0</c> at the shard root, incrementing per level descended.
    /// </summary>
    [Id(4)]
    public int Depth { get; init; }
}
