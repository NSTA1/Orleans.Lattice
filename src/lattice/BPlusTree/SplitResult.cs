using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Result returned when a leaf or internal node splits.
/// The parent node uses this to insert the promoted separator key and
/// the reference to the newly created sibling.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SplitResult)]
[Immutable]
internal sealed record SplitResult
{
    /// <summary>The separator key promoted to the parent.</summary>
    [Id(0)] public required string PromotedKey { get; init; }

    /// <summary>The grain identity of the newly created right sibling.</summary>
    [Id(1)] public required GrainId NewSiblingId { get; init; }

    /// <summary>
    /// Whether <see cref="NewSiblingId"/> identifies a leaf grain (<c>true</c>)
    /// or an internal-node grain (<c>false</c>). Self-describes the sibling's
    /// node type so a deferred <see cref="ShardRootState.PendingPromotion"/>
    /// resume path can construct the new internal root with the correct
    /// <c>childrenAreLeaves</c> value without re-reading <c>RootIsLeaf</c>
    /// from shard-root state - a value an interleaved peer
    /// <c>SetManyAsync</c> turn may have already flipped after this
    /// <see cref="SplitResult"/> was produced. Captured at every
    /// construction site (leaf split, internal split, bulk-graft) and
    /// preserved across the <c>AcceptSplitAsync</c> bubble loop.
    /// </summary>
    [Id(2)] public bool ChildIsLeaf { get; init; }
}
