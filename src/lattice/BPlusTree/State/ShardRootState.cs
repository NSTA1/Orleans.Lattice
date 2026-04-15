namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for the shard root grain. Tracks whether the root of this
/// shard is currently a leaf or has been promoted to an internal node.
/// </summary>
[GenerateSerializer]
public sealed class ShardRootState
{
    /// <summary>The grain identity of the current root node (leaf or internal).</summary>
    [Id(0)] public GrainId? RootNodeId { get; set; }

    /// <summary>Whether the current root is a leaf (<c>true</c>) or internal node (<c>false</c>).</summary>
    [Id(1)] public bool RootIsLeaf { get; set; } = true;
}
