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

    /// <summary>
    /// The per-key convergence discriminator recorded by the leaf for this key,
    /// or <see langword="null"/> when the key is a plain last-writer-wins row (or
    /// no mode has been recorded). Surfaced so read/projection callers can decode
    /// a typed CRDT's logical value - and flag genuinely opaque payloads as raw -
    /// even on a local, non-replicated, or mixed-mode tree, where the per-tree
    /// merge-mode resolver reports nothing. Additive; defaults to <see langword="null"/>
    /// for persisted state and callers that pre-date the field.
    /// </summary>
    [Id(3)] public LatticeMergeMode? MergeMode { get; init; }

    /// <summary>
    /// Compares two results by value: the <see cref="Value"/> bytes compared by
    /// content plus every scalar field. The compiler-generated record equality
    /// compares the <see cref="byte"/> array with
    /// <see cref="EqualityComparer{T}.Default"/>, which is reference equality, so
    /// two structurally identical results - and a result that round-trips through
    /// serialization versus its pre-serialization self - would otherwise never
    /// compare equal.
    /// </summary>
    /// <param name="other">The result to compare against.</param>
    public bool Equals(VersionedValue? other) =>
        other is not null
        && Version.Equals(other.Version)
        && ExpiresAtTicks == other.ExpiresAtTicks
        && MergeMode == other.MergeMode
        && BytesEqual(Value, other.Value);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        hash.Add(Version);
        hash.Add(ExpiresAtTicks);
        hash.Add(MergeMode);
        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
