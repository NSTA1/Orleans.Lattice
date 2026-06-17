using System.ComponentModel;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// A byte[] value paired with its <see cref="HybridLogicalClock"/> version.
/// Used as the return type of <see cref="ILattice.GetWithVersionAsync"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.VersionedValue)]
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

    /// <summary>
    /// Absolute UTC tick at which the entry expires, or <c>0</c> when it does not
    /// expire (the common case). Surfaced so a consumer that re-projects a live
    /// entry - notably the materialised-view rebuild scan - can carry the entry's
    /// time-to-live onto the projected write rather than dropping it. Defaults to
    /// <c>0</c> for persisted state and callers that pre-date the field.
    /// </summary>
    [Id(2)] public long ExpiresAtTicks { get; init; }
}
