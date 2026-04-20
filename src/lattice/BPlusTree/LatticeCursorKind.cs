using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// The kind of scan a <see cref="ILattice"/> stateful cursor
/// performs when opened.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCursorKind)]
public enum LatticeCursorKind : byte
{
    /// <summary>Enumerates keys only (matches <see cref="ILattice.KeysAsync"/>).</summary>
    Keys = 0,

    /// <summary>Enumerates key-value entries (matches <see cref="ILattice.EntriesAsync"/>).</summary>
    Entries = 1,

    /// <summary>
    /// Resumable range-delete (cursor-backed variant of
    /// <see cref="ILattice.DeleteRangeAsync"/> bounded per <see cref="ILattice.DeleteRangeStepAsync"/> call).
    /// </summary>
    DeleteRange = 2,
}
