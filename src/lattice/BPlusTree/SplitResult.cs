namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Result returned when a leaf or internal node splits.
/// The parent node uses this to insert the promoted separator key and
/// the reference to the newly created sibling.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record SplitResult
{
    /// <summary>The separator key promoted to the parent.</summary>
    [Id(0)] public required string PromotedKey { get; init; }

    /// <summary>The grain identity of the newly created right sibling.</summary>
    [Id(1)] public required GrainId NewSiblingId { get; init; }
}
