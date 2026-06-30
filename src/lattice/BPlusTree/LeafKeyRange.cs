namespace Orleans.Lattice;

/// <summary>
/// The persisted owned-key-range bounds of a single B+ tree leaf, returned by
/// <see cref="BPlusTree.IBPlusLeafGrain.GetKeyRangeAsync"/>.
/// <para>
/// A leaf owns exactly the half-open key span
/// <c>[<see cref="LowKeyInclusive"/>, <see cref="HighKeyExclusive"/>)</c>; the
/// WAL materialiser drops any record whose key falls outside this span, so the
/// bounds are an authoritative structural separator between adjacent leaves -
/// a leaf's <see cref="HighKeyExclusive"/> equals the next sibling's
/// <see cref="LowKeyInclusive"/>.
/// </para>
/// <para>
/// The shard-root coordinator consults these bounds to terminate a paged
/// range-scan sibling walk as soon as the walk provably leaves the requested
/// <c>[startInclusive, endExclusive)</c> range, rather than reading every
/// remaining leaf to the end of the tree. A <see langword="null"/> bound (the
/// chain's outermost leaf, or any leaf whose state pre-dates this slot) is
/// treated as "no constraint" so the walk falls back to its prior behaviour.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafKeyRange)]
[Immutable]
[System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
public readonly record struct LeafKeyRange
{
    /// <summary>
    /// Inclusive lower bound of the leaf's owned key range, or
    /// <see langword="null"/> when the leaf has no persisted lower bound.
    /// </summary>
    [Id(0)] public string? LowKeyInclusive { get; init; }

    /// <summary>
    /// Exclusive upper bound of the leaf's owned key range, or
    /// <see langword="null"/> when the leaf has no persisted upper bound.
    /// </summary>
    [Id(1)] public string? HighKeyExclusive { get; init; }
}
