using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// The immutable specification of a stateful-cursor scan (F-033): the range
/// to scan, the direction, and the kind of work the cursor performs. Captured
/// on <see cref="ILattice.OpenKeyCursorAsync"/> /
/// <see cref="ILattice.OpenEntryCursorAsync"/> /
/// <see cref="ILattice.OpenDeleteRangeCursorAsync"/> and persisted so the
/// cursor grain can resume across silo failovers.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCursorSpec)]
[Immutable]
public readonly record struct LatticeCursorSpec
{
    /// <summary>The kind of scan this cursor performs.</summary>
    [Id(0)] public LatticeCursorKind Kind { get; init; }

    /// <summary>
    /// Inclusive lower bound of the scan range, or <c>null</c> to start from
    /// the first key. For reverse scans this is still the lexicographic lower
    /// bound (the scan walks from high to low).
    /// </summary>
    [Id(1)] public string? StartInclusive { get; init; }

    /// <summary>
    /// Exclusive upper bound of the scan range, or <c>null</c> to scan to the
    /// end of the tree. For reverse scans this is still the lexicographic
    /// upper bound.
    /// </summary>
    [Id(2)] public string? EndExclusive { get; init; }

    /// <summary>
    /// When <c>true</c>, the cursor walks keys in descending lexicographic
    /// order. Not applicable to <see cref="LatticeCursorKind.DeleteRange"/> —
    /// range deletes are always forward.
    /// </summary>
    [Id(3)] public bool Reverse { get; init; }
}
