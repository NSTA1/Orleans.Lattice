namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for the shard root grain. Tracks whether the root of this
/// shard is currently a leaf or has been promoted to an internal node.
/// </summary>
[GenerateSerializer]
internal sealed class ShardRootState
{
    /// <summary>The grain identity of the current root node (leaf or internal).</summary>
    [Id(0)] public GrainId? RootNodeId { get; set; }

    /// <summary>Whether the current root is a leaf (<c>true</c>) or internal node (<c>false</c>).</summary>
    [Id(1)] public bool RootIsLeaf { get; set; } = true;

    /// <summary>
    /// If a root promotion is in progress, the split result that triggered it.
    /// Persisted before creating the new root so that a crash-retry can resume.
    /// </summary>
    [Id(2)] public SplitResult? PendingPromotion { get; set; }

    /// <summary>
    /// Whether <see cref="RootIsLeaf"/> was <c>true</c> when the pending promotion
    /// started. Used to pass the correct <c>childrenAreLeaves</c> value when
    /// creating the new internal root.
    /// </summary>
    [Id(3)] public bool PendingPromotionRootWasLeaf { get; set; }
}
