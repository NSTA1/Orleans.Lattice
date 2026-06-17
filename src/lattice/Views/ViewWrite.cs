using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// A single effect an <see cref="ILatticeViewProjection"/> wants applied to the
/// view (<c>view-{name}</c>) tree, derived from one source
/// <see cref="LatticeMutation"/>. The maintainer coalesces repeated writes to
/// the same <see cref="Key"/> within a batch (keeping the highest
/// <see cref="Timestamp"/>) and applies the survivor with last-writer-wins
/// semantics, so reordered or duplicated source entries converge.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ViewWrite)]
[Immutable]
public readonly record struct ViewWrite
{
    /// <summary>The effect to apply (upsert, delete, or the reserved CRDT delta).</summary>
    [Id(0)] public ViewWriteKind Kind { get; init; }

    /// <summary>The view-tree key this write targets. For a key-preserving projection this equals the source key.</summary>
    [Id(1)] public string Key { get; init; }

    /// <summary>
    /// The value to store for an <see cref="ViewWriteKind.Upsert"/>; <c>null</c>
    /// for a <see cref="ViewWriteKind.Delete"/>.
    /// </summary>
    [Id(2)] public byte[]? Value { get; init; }

    /// <summary>
    /// Absolute UTC tick at which the view entry expires, or <c>0</c> when it
    /// does not expire. Projected verbatim from
    /// <see cref="LatticeMutation.ExpiresAtTicks"/> so a view entry expires in
    /// lockstep with its source entry.
    /// </summary>
    [Id(3)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// The source entry's <see cref="HybridLogicalClock"/>, used as the
    /// last-writer-wins ordering key when coalescing repeated writes to the same
    /// <see cref="Key"/>.
    /// </summary>
    [Id(4)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary>
    /// Creates an <see cref="ViewWriteKind.Upsert"/> write.
    /// </summary>
    /// <param name="key">The view-tree key. Must not be <see langword="null"/>.</param>
    /// <param name="value">The value to store. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source HLC used for last-writer-wins ordering.</param>
    /// <param name="expiresAtTicks">Absolute UTC expiry tick, or <c>0</c> for no expiry.</param>
    public static ViewWrite Upsert(string key, byte[] value, HybridLogicalClock timestamp, long expiresAtTicks = 0)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        return new ViewWrite
        {
            Kind = ViewWriteKind.Upsert,
            Key = key,
            Value = value,
            ExpiresAtTicks = expiresAtTicks,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Creates a <see cref="ViewWriteKind.Delete"/> write.
    /// </summary>
    /// <param name="key">The view-tree key to remove. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source HLC used for last-writer-wins ordering.</param>
    public static ViewWrite Delete(string key, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(key);
        return new ViewWrite
        {
            Kind = ViewWriteKind.Delete,
            Key = key,
            Value = null,
            ExpiresAtTicks = 0,
            Timestamp = timestamp,
        };
    }
}
