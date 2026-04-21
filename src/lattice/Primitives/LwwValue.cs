using Orleans.Lattice;

namespace Orleans.Lattice.Primitives;

/// <summary>
/// A last-writer-wins register. The value with the highest timestamp wins on merge.
/// This is the simplest monotonic conflict-resolution strategy for individual keys.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LwwValue)]
[Immutable]
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
    /// Lattice merge: keep the value with the higher timestamp.
    /// Commutative, associative, idempotent.
    /// </summary>
    public static LwwValue<T> Merge(LwwValue<T> left, LwwValue<T> right) =>
        left.Timestamp >= right.Timestamp ? left : right;

    public int CompareTo(LwwValue<T> other) => Timestamp.CompareTo(other.Timestamp);

    public override string ToString() =>
        IsTombstone ? $"LWW(⊥ @{Timestamp})" : $"LWW({Value} @{Timestamp})";
}
