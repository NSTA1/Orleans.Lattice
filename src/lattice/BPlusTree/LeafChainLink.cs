namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One leaf's persisted key range together with its right-sibling pointer, read
/// in a single grain turn by
/// <see cref="IBPlusLeafGrain.GetKeyRangeAndNextSiblingAsync"/> so a bounded
/// leaf-chain walk that needs both to advance pays one grain call per leaf
/// instead of two. Not part of the end-user API surface.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafChainLink)]
[Immutable]
internal readonly record struct LeafChainLink
{
    /// <summary>The leaf's persisted owned-key-range bounds.</summary>
    [Id(0)] public LeafKeyRange KeyRange { get; init; }

    /// <summary>The leaf's right sibling, as <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> returns it.</summary>
    [Id(1)] public GrainId? NextSibling { get; init; }
}
