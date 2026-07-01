namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A read-only, point-in-time structural snapshot of one node in a shard's
/// B+ tree, plus its expanded descendants up to a caller-supplied depth
/// limit. Reconstructed by the shard root and internal nodes from the
/// per-child <see cref="ChildDigestSnapshot"/> data that already propagates
/// up the tree on every digest publish, so a topology query is answered
/// without fanning out to the leaves: an internal node summarises each leaf
/// child from its own stored snapshot table and only recurses into internal
/// children. Carried by <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain"/> /
/// <see cref="IBPlusInternalGrain"/> topology reads.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardTopologyNode)]
[Immutable]
internal sealed record ShardTopologyNode
{
    /// <summary>The grain identity of this node, rendered as a stable string.</summary>
    [Id(0)] public required string NodeId { get; init; }

    /// <summary>Whether this node is a leaf (<c>true</c>) or an internal node (<c>false</c>).</summary>
    [Id(1)] public bool IsLeaf { get; init; }

    /// <summary>The shard index this node belongs to, when known.</summary>
    [Id(2)] public int? ShardIndex { get; init; }

    /// <summary>Height of the subtree rooted at this node: <c>1</c> for a leaf, <c>1 + max(child depth)</c> for an internal node.</summary>
    [Id(3)] public int SubtreeDepth { get; init; }

    /// <summary>Lowest key inclusively covered by this subtree, or <see langword="null"/> when empty.</summary>
    [Id(4)] public string? LowKeyInclusive { get; init; }

    /// <summary>Exclusive upper bound of the key range this subtree covers, or <see langword="null"/> when unbounded/empty.</summary>
    [Id(5)] public string? HighKeyExclusive { get; init; }

    /// <summary>Total entries (live plus tombstoned) folded into this subtree.</summary>
    [Id(6)] public long EntryCount { get; init; }

    /// <summary>Count of live (non-tombstoned, unexpired) entries in this subtree.</summary>
    [Id(7)] public long LiveCount { get; init; }

    /// <summary>Count of tombstoned entries retained in this subtree.</summary>
    [Id(8)] public long TombstoneCount { get; init; }

    /// <summary>Number of immediate children: <c>0</c> for a leaf, otherwise the internal node's child count.</summary>
    [Id(9)] public int ChildFanout { get; init; }

    /// <summary>
    /// <see langword="true"/> when this node has children that were not
    /// expanded into <see cref="Children"/> because the depth limit was
    /// reached. The summary counts and bounds remain accurate; only the
    /// nested structure is omitted.
    /// </summary>
    [Id(10)] public bool ChildrenTruncated { get; init; }

    /// <summary>
    /// Expanded child nodes in separator-key order. Empty for a leaf, and
    /// empty for an internal node whose children were truncated at the
    /// depth limit (see <see cref="ChildrenTruncated"/>).
    /// </summary>
    [Id(11)] public IReadOnlyList<ShardTopologyNode> Children { get; init; } = [];
}
