using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A single child entry in an internal node: the separator key and the grain id of the child.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record ChildEntry
{
    /// <summary>
    /// The separator key. All keys strictly less than this value are routed to the
    /// <em>previous</em> child. This is <c>null</c> for the leftmost (catch-all) child.
    /// </summary>
    [Id(0)] public string? SeparatorKey { get; init; }

    /// <summary>The grain identity of the child node.</summary>
    [Id(1)] public required GrainId ChildId { get; init; }
}

/// <summary>
/// Persistent state for an internal (non-leaf) node grain.
/// Children are stored in separator-key order; the first child has a <c>null</c>
/// separator and acts as the leftmost catch-all.
/// </summary>
[GenerateSerializer]
public sealed class InternalNodeState
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
