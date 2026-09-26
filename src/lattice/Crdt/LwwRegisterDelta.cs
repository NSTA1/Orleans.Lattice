using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for a last-writer-wins register mutation. Carries
/// the raw bytes the producer committed, the <see cref="HybridLogicalClock"/>
/// assigned at commit time, and the originating cluster id for cycle-break
/// and dedupe.
/// <para>
/// Whether the producer treats those bytes as an LWW register, a serialised
/// CRDT primitive, or opaque payload is invisible to this record - the wire
/// is always bytes. The current replication path ships <see cref="WalRecord"/>
/// payloads directly; this DTO remains the serializable LWW-delta shape for tests
/// and compatibility.
/// </para>
/// <para>
/// Receiver-side conflict resolution is the normal <see cref="Primitives.LwwValue{T}"/>
/// merge order: compare HLC first and use the origin id only as the equal-HLC
/// tiebreaker. Apply <em>never</em> goes through a fresh <c>SetAsync</c> - that
/// would stamp a new local HLC and lose the source causality.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LwwRegisterDelta)]
[Immutable]
public readonly record struct LwwRegisterDelta
{
    /// <summary>The committed value bytes; <c>null</c> when <see cref="IsTombstone"/> is <c>true</c>.</summary>
    [Id(0)] public byte[]? Value { get; init; }

    /// <summary>The <see cref="HybridLogicalClock"/> stamped at commit time on the originating cluster.</summary>
    [Id(1)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary><c>true</c> when this delta represents a delete (tombstone) rather than a write.</summary>
    [Id(2)] public bool IsTombstone { get; init; }

    /// <summary>
    /// Absolute UTC tick at which the entry expires, or <c>0</c> when it
    /// does not expire. Preserved end-to-end so TTL semantics survive
    /// cross-cluster replication.
    /// </summary>
    [Id(3)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// Identifier of the cluster that authored this mutation, or <c>null</c>
    /// for hand-constructed deltas used in tests. Receivers use this to
    /// break replication cycles, to populate per-origin high-water-mark
    /// dedupe state, and as the lexicographic tiebreaker on equal HLCs.
    /// </summary>
    [Id(4)] public string? OriginClusterId { get; init; }

    /// <summary>
    /// Creates a tombstone delta carrying the supplied <paramref name="timestamp"/>
    /// and <paramref name="originClusterId"/>. Mirrors
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Tombstone"/> so receivers and tests have a
    /// single canonical way to author a tombstone delta without leaving
    /// <see cref="Value"/> populated.
    /// </summary>
    public static LwwRegisterDelta Tombstone(HybridLogicalClock timestamp, string? originClusterId = null) => new()
    {
        Value = null,
        Timestamp = timestamp,
        IsTombstone = true,
        OriginClusterId = originClusterId,
    };

    /// <summary>
    /// Compares two deltas by value, with <see cref="Value"/> compared by
    /// content. The compiler-generated record-struct equality compares the
    /// <see cref="Value"/> byte array with <see cref="EqualityComparer{T}.Default"/> -
    /// reference equality for a <see cref="byte"/> array - so two deltas built
    /// from independently allocated but byte-identical payloads (including a
    /// delta and its post-serialization self) would otherwise never compare
    /// equal.
    /// </summary>
    /// <param name="other">The delta to compare against.</param>
    public bool Equals(LwwRegisterDelta other) =>
        BytesEqual(Value, other.Value)
        && Timestamp.Equals(other.Timestamp)
        && IsTombstone == other.IsTombstone
        && ExpiresAtTicks == other.ExpiresAtTicks
        && string.Equals(OriginClusterId, other.OriginClusterId, StringComparison.Ordinal);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        hash.Add(Timestamp);
        hash.Add(IsTombstone);
        hash.Add(ExpiresAtTicks);
        hash.Add(OriginClusterId, StringComparer.Ordinal);
        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
