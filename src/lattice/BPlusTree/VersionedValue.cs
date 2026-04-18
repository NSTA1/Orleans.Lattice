using System.ComponentModel;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// A byte[] value paired with its <see cref="HybridLogicalClock"/> version.
/// Used as the return type of <see cref="ILattice.GetWithVersionAsync"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.VersionedValue)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed record VersionedValue
{
    /// <summary>The stored value, or <c>null</c> if the key is absent or tombstoned.</summary>
    [Id(0)] public byte[]? Value { get; init; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> timestamp of the current entry.
    /// <see cref="HybridLogicalClock.Zero"/> when the key is absent or tombstoned.
    /// </summary>
    [Id(1)] public HybridLogicalClock Version { get; init; }
}