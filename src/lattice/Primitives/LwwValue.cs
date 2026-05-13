using Orleans.Lattice;

namespace Orleans.Lattice.Primitives;

/// <summary>
/// A last-writer-wins register. The value with the highest timestamp wins on merge.
/// This is the simplest monotonic conflict-resolution strategy for individual keys.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LwwValue)]
internal readonly record struct LwwValue<T>
{
    [Id(0)] public T? Value { get; init; }
    [Id(1)] public HybridLogicalClock Timestamp { get; init; }
    [Id(2)] public bool IsTombstone { get; init; }

    /// <summary>
    /// Absolute wall-clock expiry in UTC <see cref="DateTime.Ticks"/>.
    /// <c>0</c> means the entry does not expire. Set by
    /// <see cref="CreateWithExpiry"/> when a caller provides a TTL on
    /// <c>SetAsync</c>; entries are treated as tombstoned during reads
    /// once the current UTC wall clock passes this value and are reaped by
    /// background tombstone compaction after the configured grace period.
    /// </summary>
    [Id(3)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// Identifier of the cluster that authored this mutation, or <c>null</c>
    /// when the write originated locally. Stamped at commit time from the
    /// ambient <see cref="LatticeOriginContext"/> inside the grain write
    /// path and preserved verbatim across shadow-forward, saga
    /// prepare/compensate, tree snapshot / restore, bulk-load, and merge.
    /// Exposed on the public <see cref="LatticeMutation"/> surface so
    /// downstream <see cref="IMutationObserver"/> consumers (notably the
    /// replication package) can skip re-forwarding mutations that
    /// originated elsewhere. Wire-compatible: legacy persisted state
    /// without this field decodes to <c>null</c>.
    /// </summary>
    [Id(4)] public string? OriginClusterId { get; init; }

    /// <summary>
    /// Sparse <c>{originClusterId → HybridLogicalClock}</c> frontier
    /// captured at commit time, or <c>null</c> when the writer did not
    /// supply one (the equivalent of an empty frontier). Stamped from
    /// the ambient <see cref="LatticeVectorClockContext"/> inside the
    /// grain write path and preserved verbatim across every persistence,
    /// merge, snapshot / restore, bulk-load, compaction, saga
    /// prepare / compensate, and shard-split shadow-forward path so the
    /// frontier travels with the value. The library itself does not
    /// merge or interpret the frontier - replication-aware consumers
    /// pin or compare it as needed. Wire-compatible: legacy persisted
    /// state without this field decodes to <c>null</c>.
    /// </summary>
    [Id(5)] public VersionVector? VectorClock { get; init; }

    public static LwwValue<T> Create(T value, HybridLogicalClock timestamp) =>
        new() { Value = value, Timestamp = timestamp };

    /// <summary>
    /// Creates a live entry that expires at the given absolute UTC tick.
    /// An <paramref name="expiresAtTicks"/> of <c>0</c> produces a non-expiring
    /// entry (equivalent to <see cref="Create"/>).
    /// </summary>
    public static LwwValue<T> CreateWithExpiry(T value, HybridLogicalClock timestamp, long expiresAtTicks) =>
        new() { Value = value, Timestamp = timestamp, ExpiresAtTicks = expiresAtTicks };

    public static LwwValue<T> Tombstone(HybridLogicalClock timestamp) =>
        new() { IsTombstone = true, Timestamp = timestamp };

    /// <summary>
    /// Returns <c>true</c> when this is a live (non-tombstone) entry carrying
    /// an expiry and <paramref name="nowUtcTicks"/> has reached or passed it.
    /// Tombstones and entries with <see cref="ExpiresAtTicks"/> <c>== 0</c>
    /// are never considered expired.
    /// </summary>
    public bool IsExpired(long nowUtcTicks) =>
        !IsTombstone && ExpiresAtTicks != 0 && ExpiresAtTicks <= nowUtcTicks;

    /// <summary>
    /// Lattice merge: keep the value with the higher timestamp. On an HLC tie
    /// (two replicas authored at the same <see cref="HybridLogicalClock"/>),
    /// the result is resolved deterministically by a stable total order on
    /// (<see cref="Timestamp"/>, <see cref="OriginClusterId"/>, 
    /// <see cref="IsTombstone"/>) so replicas converge regardless of the
    /// order in which they observe the writes. Commutative, associative,
    /// idempotent.
    /// </summary>
    public static LwwValue<T> Merge(LwwValue<T> left, LwwValue<T> right)
    {
        // Primary: higher HLC wins.
        var clockCmp = left.Timestamp.CompareTo(right.Timestamp);
        if (clockCmp != 0) return clockCmp > 0 ? left : right;

        // Secondary: break HLC ties on writer identity. Ordinal string
        // compare is a total order; null is deterministically ordered
        // before any non-null id (legacy state without OriginClusterId).
        var originCmp = string.CompareOrdinal(left.OriginClusterId, right.OriginClusterId);
        if (originCmp != 0) return originCmp > 0 ? left : right;

        // Tertiary: same writer, same HLC, but the live/tombstone bit
        // differs. Tombstone wins on tie to preserve delete intent and
        // keep the result stable across replicas that observed only one
        // of the two writes.
        if (left.IsTombstone != right.IsTombstone)
            return left.IsTombstone ? left : right;

        // Indistinguishable on every stable identity field - the values are
        // for all CRDT purposes equivalent, so picking left is deterministic.
        return left;
    }

    public int CompareTo(LwwValue<T> other) => Timestamp.CompareTo(other.Timestamp);

    public override string ToString() =>
        IsTombstone ? $"LWW(⊥ @{Timestamp})" : $"LWW({Value} @{Timestamp})";
}
