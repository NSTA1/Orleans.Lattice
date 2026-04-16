using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for an internal (non-leaf) node grain.
/// Children are stored in separator-key order; the first child has a <c>null</c>
/// separator and acts as the leftmost catch-all.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.InternalNodeState)]
internal sealed class InternalNodeState
{
    /// <summary>
    /// Ordered list of children. The first entry has <see cref="ChildEntry.SeparatorKey"/> == <c>null</c>
    /// (leftmost). Subsequent entries represent "keys ≥ separator go to this child."
    /// </summary>
    [Id(0)] public List<ChildEntry> Children { get; set; } = [];

    /// <summary>Monotonic split lifecycle for this internal node.</summary>
    [Id(1)] public SplitState SplitState { get; set; }

    /// <summary>The logical clock for this grain.</summary>
    [Id(2)] public HybridLogicalClock Clock { get; set; }

    /// <summary>Whether this node's children are leaves (<c>true</c>) or internal nodes (<c>false</c>).</summary>
    [Id(3)] public bool ChildrenAreLeaves { get; set; } = true;

    /// <summary>The tree this node belongs to. Used to resolve named <see cref="BPlusTree.LatticeOptions"/>.</summary>
    [Id(4)] public string? TreeId { get; set; }

    /// <summary>If a split is in progress, the separator key being promoted.</summary>
    [Id(5)] public string? SplitKey { get; set; }

    /// <summary>If a split is in progress, the grain identity of the new right sibling.</summary>
    [Id(6)] public GrainId? SplitSiblingId { get; set; }

    /// <summary>If a split is in progress, the children that belong to the right sibling.</summary>
    [Id(7)] public List<ChildEntry>? SplitRightChildren { get; set; }

    /// <summary>
    /// Routes a key to the correct child grain by finding the rightmost separator ≤ key.
    /// </summary>
    public GrainId Route(string key)
    {
        // Walk backwards through children to find the first separator that is ≤ key.
        for (int i = Children.Count - 1; i >= 0; i--)
        {
            var sep = Children[i].SeparatorKey;
            if (sep is null || string.Compare(key, sep, StringComparison.Ordinal) >= 0)
            {
                return Children[i].ChildId;
            }
        }

        // Should not happen if the node is well-formed (first child has null separator).
        return Children[0].ChildId;
    }
}
