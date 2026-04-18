using System.ComponentModel;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// A value paired with its <see cref="HybridLogicalClock"/> version, enabling
/// optimistic concurrency via compare-and-swap. Obtain this from
/// <see cref="ILattice.GetWithVersionAsync"/> and pass the
/// <see cref="Version"/> to <see cref="ILattice.SetIfVersionAsync"/>
/// for a conditional write.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.Versioned)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public readonly record struct Versioned<T>
{
    /// <summary>The stored value, or <c>default</c> if the key is absent or tombstoned.</summary>
    [Id(0)] public T? Value { get; init; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> timestamp of the current entry.
    /// Pass this to <see cref="ILattice.SetIfVersionAsync"/> to perform a
    /// conditional write. <see cref="HybridLogicalClock.Zero"/> when the key
    /// is absent or tombstoned.
    /// </summary>
    [Id(1)] public HybridLogicalClock Version { get; init; }
}