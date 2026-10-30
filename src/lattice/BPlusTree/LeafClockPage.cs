namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One leaf's hybrid logical clock together with its right-sibling pointer and
/// bounds, returned in a single grain turn by
/// <see cref="IBPlusLeafGrain.GetClockAndNextSiblingAsync"/> so the shard root's
/// bounded range-clock probe pays one grain call per leaf instead of two. Not
/// part of the end-user API surface.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafClockPage)]
[Immutable]
internal readonly record struct LeafClockPage
{
    /// <summary>The leaf's clock, as <see cref="IBPlusLeafGrain.GetClockAsync"/> returns it.</summary>
    [Id(0)] public HybridLogicalClock Clock { get; init; }

    /// <summary>The leaf's right sibling, as <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> returns it.</summary>
    [Id(1)] public GrainId? NextSibling { get; init; }

    /// <summary>The leaf's bounds, as <see cref="IBPlusLeafGrain.GetKeyRangeAndNextSiblingAsync"/> returns them.</summary>
    [Id(2)] public LeafKeyRange KeyRange { get; init; }
}
