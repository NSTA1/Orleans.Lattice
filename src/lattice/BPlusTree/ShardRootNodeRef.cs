using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A cluster-local reference to a shard's current B+ tree root node - the
/// grain identity of the root and whether that root is a leaf (a flat,
/// single-node shard) or an internal node. Returned by
/// <see cref="IShardRootGrain.GetRootNodeRefAsync"/> as the entry point for a
/// read-only top-down tree walk. A <see langword="null"/> reference denotes an
/// empty shard with no root yet.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardRootNodeRef)]
[Immutable]
internal readonly record struct ShardRootNodeRef
{
    /// <summary>The grain identity of the shard's current root node.</summary>
    [Id(0)]
    public GrainId NodeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the root is a leaf grain (a flat shard);
    /// <see langword="false"/> when the root is an internal node.
    /// </summary>
    [Id(1)]
    public bool IsLeaf { get; init; }
}
