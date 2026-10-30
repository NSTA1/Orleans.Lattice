namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One leaf's persisted key range together with its left-sibling pointer, read
/// in a single grain turn by
/// <see cref="IBPlusLeafGrain.GetKeyRangeAndPrevSiblingAsync"/> so a reverse
/// leaf-chain walk that needs both to advance pays one grain call per leaf
/// instead of two. The mirror of <see cref="LeafChainLink"/>. Not part of the
/// end-user API surface.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafPrevChainLink)]
[Immutable]
internal readonly record struct LeafPrevChainLink
{
    /// <summary>The leaf's persisted owned-key-range bounds.</summary>
    [Id(0)] public LeafKeyRange KeyRange { get; init; }

    /// <summary>The leaf's left sibling, as <see cref="IBPlusLeafGrain.GetPrevSiblingAsync"/> returns it.</summary>
    [Id(1)] public GrainId? PrevSibling { get; init; }
}
