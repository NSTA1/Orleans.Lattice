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
/// is always bytes. This same shape is therefore the opaque-bytes fallback
/// used by the commit-time emission path for value types that are not a
/// recognised CRDT primitive.
/// </para>
/// <para>
/// Apply semantics on the receiver: install when
/// <c>(this.Timestamp, this.OriginClusterId)</c> compares strictly greater
/// than <c>(existing.Timestamp, existing.OriginClusterId)</c> under the
/// lexicographic ordering (HLC first, origin id second). The origin
/// tiebreaker is needed because two clusters may concurrently produce
/// equal HLCs; without it, convergence on a single value across all
/// receivers is not guaranteed. Apply <em>never</em> goes through a fresh
/// <c>SetAsync</c> - that would stamp a new local HLC and lose the source
/// causality.
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
    /// <see cref="LwwValue{T}.Tombstone"/> so receivers and tests have a
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
}
