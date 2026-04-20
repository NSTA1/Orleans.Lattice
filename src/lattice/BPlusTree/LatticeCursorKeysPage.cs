using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// A page of keys returned by <see cref="ILattice.NextKeysAsync"/> from a
/// stateful cursor.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCursorKeysPage)]
[Immutable]
public sealed record LatticeCursorKeysPage
{
    /// <summary>
    /// The keys in this page, in the cursor's scan order (ascending or
    /// descending per <see cref="LatticeCursorSpec.Reverse"/>).
    /// </summary>
    [Id(0)] public required IReadOnlyList<string> Keys { get; init; }

    /// <summary>
    /// <c>true</c> when the cursor may have more keys to yield. <c>false</c>
    /// only after the cursor has been fully drained.
    /// </summary>
    [Id(1)] public required bool HasMore { get; init; }
}
