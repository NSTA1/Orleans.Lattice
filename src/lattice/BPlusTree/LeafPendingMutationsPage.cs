namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One leaf's pending-mutation snapshots together with its right-sibling
/// pointer, read in a single grain turn by
/// <see cref="IBPlusLeafGrain.GetPendingMutationsForSlotsAndNextSiblingAsync"/>
/// so a leaf-chain walk that needs both pays one grain call per leaf instead of
/// two. Not part of the end-user API surface.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafPendingMutationsPage)]
[Immutable]
internal readonly record struct LeafPendingMutationsPage
{
    /// <summary>The snapshots, as <see cref="IBPlusLeafGrain.GetPendingMutationsForSlotsAsync"/> returns them.</summary>
    [Id(0)] public List<PendingMutationSnapshot> Mutations { get; init; }

    /// <summary>The leaf's right sibling, as <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> returns it.</summary>
    [Id(1)] public GrainId? NextSibling { get; init; }
}
