namespace Orleans.Lattice.Primitives;

/// <summary>
/// A positive-negative (PN) counter CRDT. Each replica tracks its own
/// monotonic positive and negative components; the counter's value is the
/// sum of all positive components minus the sum of all negative components.
/// State-level <see cref="Merge(PnCounter, PnCounter)"/> is pointwise-max
/// per replica per side, making the CRDT commutative, associative, and
/// idempotent under arbitrary delivery order.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.PnCounter)]
public sealed class PnCounter
{
    /// <summary>Per-replica cumulative positive component.</summary>
    [Id(0)]
    public Dictionary<string, long> Increments { get; set; } = [];

    /// <summary>Per-replica cumulative negative component.</summary>
    [Id(1)]
    public Dictionary<string, long> Decrements { get; set; } = [];

    /// <summary>The counter's current value: sum of <see cref="Increments"/> minus sum of <see cref="Decrements"/>.</summary>
    public long Value
    {
        get
        {
            long total = 0;
            foreach (var v in Increments.Values) total += v;
            foreach (var v in Decrements.Values) total -= v;
            return total;
        }
    }

    /// <summary>
    /// Advances the positive component for <paramref name="replicaId"/> by
    /// <paramref name="amount"/>. <paramref name="amount"/> must be non-negative.
    /// </summary>
    public void Increment(string replicaId, long amount = 1)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        if (amount == 0) return;
        Increments.TryGetValue(replicaId, out var current);
        Increments[replicaId] = current + amount;
    }

    /// <summary>
    /// Advances the negative component for <paramref name="replicaId"/> by
    /// <paramref name="amount"/>. <paramref name="amount"/> must be non-negative.
    /// </summary>
    public void Decrement(string replicaId, long amount = 1)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        if (amount == 0) return;
        Decrements.TryGetValue(replicaId, out var current);
        Decrements[replicaId] = current + amount;
    }

    /// <summary>
    /// Lattice merge: pointwise-max per replica on both <see cref="Increments"/>
    /// and <see cref="Decrements"/>. Commutative, associative, idempotent.
    /// </summary>
    public static PnCounter Merge(PnCounter left, PnCounter right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <summary>
    /// In-place lattice merge: applies the pointwise-max of
    /// <paramref name="other"/>'s components into this counter without
    /// allocating a new instance. Equivalent to
    /// <see cref="Merge(PnCounter, PnCounter)"/> followed by replacing the
    /// receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(PnCounter other)
    {
        ArgumentNullException.ThrowIfNull(other);
        MergeSide(Increments, other.Increments);
        MergeSide(Decrements, other.Decrements);
    }

    /// <summary>Creates a deep copy of this counter.</summary>
    public PnCounter Clone()
    {
        var copy = new PnCounter();
        foreach (var (k, v) in Increments) copy.Increments[k] = v;
        foreach (var (k, v) in Decrements) copy.Decrements[k] = v;
        return copy;
    }

    private static void MergeSide(Dictionary<string, long> target, Dictionary<string, long> source)
    {
        foreach (var (k, v) in source)
        {
            target[k] = target.TryGetValue(k, out var existing) && existing > v ? existing : v;
        }
    }
}
