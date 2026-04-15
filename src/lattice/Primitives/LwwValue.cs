namespace Orleans.Lattice.Primitives;

/// <summary>
/// A last-writer-wins register. The value with the highest timestamp wins on merge.
/// This is the simplest monotonic conflict-resolution strategy for individual keys.
/// </summary>
[GenerateSerializer]
[Immutable]
public readonly record struct LwwValue<T>
{
    [Id(0)] public T? Value { get; init; }
    [Id(1)] public HybridLogicalClock Timestamp { get; init; }
    [Id(2)] public bool IsTombstone { get; init; }

    public static LwwValue<T> Create(T value, HybridLogicalClock timestamp) =>
        new() { Value = value, Timestamp = timestamp };

    public static LwwValue<T> Tombstone(HybridLogicalClock timestamp) =>
        new() { IsTombstone = true, Timestamp = timestamp };

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
