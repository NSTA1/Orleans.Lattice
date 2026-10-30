namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One leaf's tombstone-compaction result together with its right-sibling
/// pointer and bounds, returned in a single grain turn by
/// <see cref="IBPlusLeafGrain.CompactTombstonesAndNextSiblingAsync"/> so the
/// compaction coordinator's chain walk pays one grain call per leaf instead of
/// two. Not part of the end-user API surface.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafCompactionPage)]
[Immutable]
internal readonly record struct LeafCompactionPage
{
    /// <summary>The leaf's compaction result, as <see cref="IBPlusLeafGrain.CompactTombstonesAsync"/> returns it.</summary>
    [Id(0)] public LeafCompactionResult Result { get; init; }

    /// <summary>The leaf's right sibling, as <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> returns it.</summary>
    [Id(1)] public GrainId? NextSibling { get; init; }

    /// <summary>
    /// The leaf's bounds, as <see cref="IBPlusLeafGrain.GetKeyRangeAndNextSiblingAsync"/> returns
    /// them, read in the same turn so a bounded walk can advance past this leaf
    /// without a further call. <see langword="default"/> from a leaf that predates
    /// the field, which a bounded walk treats as a leaf with no resume bound.
    /// </summary>
    [Id(2)] public LeafKeyRange KeyRange { get; init; }

    /// <summary>This page's sibling pointer and bounds as a <see cref="LeafChainLink"/>.</summary>
    public LeafChainLink Link => new() { KeyRange = KeyRange, NextSibling = NextSibling };
}
