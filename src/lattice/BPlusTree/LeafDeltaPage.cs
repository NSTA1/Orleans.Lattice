using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One leaf's state delta together with its right-sibling pointer, read in a
/// single grain turn by <see cref="IBPlusLeafGrain.GetDeltaSinceAndNextSiblingAsync"/>
/// so a leaf-chain walk that needs both pays one grain call per leaf instead of
/// two. Not part of the end-user API surface.
/// <para>
/// Deliberately not <c>[Immutable]</c>: <see cref="StateDelta"/> carries a
/// mutable <see cref="VersionVector"/> and may be the leaf's shared empty-delta
/// singleton, so a same-silo call must keep the deep copy it gets today.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafDeltaPage)]
internal readonly record struct LeafDeltaPage
{
    /// <summary>The delta, as <see cref="IBPlusLeafGrain.GetDeltaSinceAsync"/> returns it.</summary>
    [Id(0)] public StateDelta Delta { get; init; }

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
