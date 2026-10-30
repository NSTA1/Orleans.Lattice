namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One leaf's range-delete result together with its right-sibling pointer and
/// bounds, returned in a single grain turn by
/// <see cref="IBPlusLeafGrain.DeleteRangeAndNextSiblingAsync"/> so the shard
/// root's bounded range-delete walk pays one grain call per leaf instead of two
/// (three on a hop where the work budget asks it to yield). Not part of the
/// end-user API surface.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafRangeDeletePage)]
[Immutable]
internal readonly record struct LeafRangeDeletePage
{
    /// <summary>The leaf's range-delete result, as <see cref="IBPlusLeafGrain.DeleteRangeAsync"/> returns it.</summary>
    [Id(0)] public RangeDeleteResult Result { get; init; }

    /// <summary>The leaf's right sibling, as <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> returns it.</summary>
    [Id(1)] public GrainId? NextSibling { get; init; }

    /// <summary>
    /// The leaf's bounds, as <see cref="IBPlusLeafGrain.GetKeyRangeAndNextSiblingAsync"/> returns
    /// them, read in the same turn after the delete (and any split it caused)
    /// completed, so the walk can pick a resume key without a further call.
    /// </summary>
    [Id(2)] public LeafKeyRange KeyRange { get; init; }
}
