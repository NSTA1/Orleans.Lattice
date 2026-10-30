using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One leaf's frozen projection together with its right-sibling pointer,
/// returned in a single grain turn by
/// <see cref="IBPlusLeafGrain.FreezeProjectionAndNextSiblingAsync"/> so the
/// snapshot-baseline freeze pass pays one grain call per leaf instead of two.
/// Not part of the end-user API surface. Deliberately not <c>[Immutable]</c>:
/// <see cref="LeafBaselineFreeze"/> is a mutable class, so a same-silo hop must
/// deep-copy it on every hop.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafBaselineFreezePage)]
internal readonly record struct LeafBaselineFreezePage
{
    /// <summary>The leaf's frozen projection: committed cache, per-partition frontier and in-flight prepared sagas.</summary>
    [Id(0)] public LeafBaselineFreeze Freeze { get; init; }

    /// <summary>The leaf's right sibling, as <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> returns it.</summary>
    [Id(1)] public GrainId? NextSibling { get; init; }
}
