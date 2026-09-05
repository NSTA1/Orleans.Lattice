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
    /// Exclusive upper bound of the affected key range for a
    /// <see cref="ViewWriteKind.RangeDelete"/> or
    /// <see cref="ViewWriteKind.RangeReconcile"/> write (the range is
    /// <c>[<see cref="Key"/>, <see cref="EndKey"/>)</c>); <see langword="null"/>
    /// for a point <see cref="ViewWriteKind.Upsert"/> or
    /// <see cref="ViewWriteKind.Delete"/>.
    /// </summary>
    [Id(5)] public string? EndKey { get; init; }

    /// <summary>
    /// The source key that produced this write, when known. Used by the
    /// maintainer to detect re-key collisions (two distinct source keys mapping
    /// to one view key under an injective re-map, a configuration error). The
    /// built-in <see cref="PredicateLatticeViewProjection"/> stamps it on every
    /// point write; <see langword="null"/> when a projection does not attribute a
    /// write to a single source key, in which case collision detection skips it.
    /// </summary>
    [Id(6)] public string? SourceKey { get; init; }

    /// <summary>
    /// Compares two writes by value: every scalar field plus the
    /// <see cref="Value"/> bytes compared by content. The compiler-generated
    /// record-struct equality compares that array with
    /// <see cref="EqualityComparer{T}.Default"/>, which for a <see cref="byte"/>
    /// array is reference equality, so two structurally identical writes - and a
    /// write that round-trips through serialization versus its pre-serialization
    /// self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The write to compare against.</param>
    public bool Equals(ViewWrite other) =>
        Kind == other.Kind
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && ExpiresAtTicks == other.ExpiresAtTicks
        && Timestamp.Equals(other.Timestamp)
        && string.Equals(EndKey, other.EndKey, StringComparison.Ordinal)
        && string.Equals(SourceKey, other.SourceKey, StringComparison.Ordinal)
        && BytesEqual(Value, other.Value);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Kind);
        hash.Add(Key, StringComparer.Ordinal);
        hash.Add(ExpiresAtTicks);
        hash.Add(Timestamp);
        hash.Add(EndKey, StringComparer.Ordinal);
        hash.Add(SourceKey, StringComparer.Ordinal);
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));

    /// <summary>
    /// Creates an <see cref="ViewWriteKind.Upsert"/> write.
    /// </summary>
    /// <param name="key">The view-tree key. Must not be <see langword="null"/>.</param>
    /// <param name="value">The value to store. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source HLC used for last-writer-wins ordering.</param>
    /// <param name="expiresAtTicks">Absolute UTC expiry tick, or <c>0</c> for no expiry.</param>
    /// <param name="sourceKey">The originating source key, for re-key collision detection. Optional.</param>
    public static ViewWrite Upsert(string key, byte[] value, HybridLogicalClock timestamp, long expiresAtTicks = 0, string? sourceKey = null)
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
            SourceKey = sourceKey,
        };
    }

    /// <summary>
    /// Creates a <see cref="ViewWriteKind.Delete"/> write.
    /// </summary>
    /// <param name="key">The view-tree key to remove. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source HLC used for last-writer-wins ordering.</param>
    /// <param name="sourceKey">The originating source key, for re-key collision detection. Optional.</param>
    public static ViewWrite Delete(string key, HybridLogicalClock timestamp, string? sourceKey = null)
    {
        ArgumentNullException.ThrowIfNull(key);
        return new ViewWrite
        {
            Kind = ViewWriteKind.Delete,
            Key = key,
            Value = null,
            ExpiresAtTicks = 0,
            Timestamp = timestamp,
            SourceKey = sourceKey,
        };
    }

    /// <summary>
    /// Creates a <see cref="ViewWriteKind.RangeDelete"/> write covering the
    /// half-open view-key range <c>[<paramref name="startInclusive"/>,
    /// <paramref name="endExclusive"/>)</c>. Valid only for a key-preserving
    /// projection, where the view key equals the source key.
    /// </summary>
    /// <param name="startInclusive">Inclusive lower bound of the range. Must not be <see langword="null"/>.</param>
    /// <param name="endExclusive">Exclusive upper bound of the range. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source HLC used for last-writer-wins ordering.</param>
    public static ViewWrite RangeDelete(string startInclusive, string endExclusive, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        return new ViewWrite
        {
            Kind = ViewWriteKind.RangeDelete,
            Key = startInclusive,
            EndKey = endExclusive,
            Value = null,
            ExpiresAtTicks = 0,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Creates a <see cref="ViewWriteKind.RangeReconcile"/> write asking the
    /// maintainer to re-derive the view over the affected source range
    /// <c>[<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)</c>
    /// from current source state. Emitted by a re-keyed projection for an
    /// unconstrained range delete it cannot lower to exact per-key writes.
    /// </summary>
    /// <param name="startInclusive">Inclusive lower bound of the affected source range. Must not be <see langword="null"/>.</param>
    /// <param name="endExclusive">Exclusive upper bound of the affected source range. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source HLC of the range delete that triggered the reconcile.</param>
    public static ViewWrite RangeReconcile(string startInclusive, string endExclusive, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        return new ViewWrite
        {
            Kind = ViewWriteKind.RangeReconcile,
            Key = startInclusive,
            EndKey = endExclusive,
            Value = null,
            ExpiresAtTicks = 0,
            Timestamp = timestamp,
        };
    }
}
