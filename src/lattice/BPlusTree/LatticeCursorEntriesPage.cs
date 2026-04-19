using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// A page of key-value entries returned by <see cref="ILattice.NextEntriesAsync"/>
/// from a stateful cursor (F-033).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCursorEntriesPage)]
[Immutable]
public sealed record LatticeCursorEntriesPage
{
    /// <summary>
    /// The entries in this page, in the cursor's scan order (ascending or
    /// descending per <see cref="LatticeCursorSpec.Reverse"/>).
    /// </summary>
    [Id(0)] public required IReadOnlyList<KeyValuePair<string, byte[]>> Entries { get; init; }

    /// <summary>
    /// <c>true</c> when the cursor may have more entries to yield. <c>false</c>
    /// only after the cursor has been fully drained.
    /// </summary>
    [Id(1)] public required bool HasMore { get; init; }
}
