namespace Orleans.Lattice;

using System.Runtime.InteropServices;

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
public sealed class PnCounter : ICrdt<PnCounter>
{
    /// <summary>Per-replica cumulative positive component.</summary>
    [Id(0)]
    public Dictionary<string, long> Increments { get; set; } = [];

    /// <summary>Per-replica cumulative negative component.</summary>
    [Id(1)]
    public Dictionary<string, long> Decrements { get; set; } = [];

    /// <inheritdoc />
    /// <remarks>
    /// A <see cref="PnCounter"/> is bottom when no replica has
    /// recorded any increment or decrement. A counter whose recorded
    /// per-replica components happen to sum to zero is <em>not</em>
    /// bottom - the components carry replica history that is not the
    /// lattice bottom element.
    /// </remarks>
    public bool IsBottom => Increments.Count == 0 && Decrements.Count == 0;

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
        ref var inc = ref CollectionsMarshal.GetValueRefOrAddDefault(Increments, replicaId, out _);
        inc += amount;
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
        ref var dec = ref CollectionsMarshal.GetValueRefOrAddDefault(Decrements, replicaId, out _);
        dec += amount;
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
    public PnCounter Clone() =>
        // The dictionary copy constructor presizes each backing store to the
        // source Count exactly and bulk-copies the entries, eliminating the
        // incremental Resize() grows the previous entry-by-entry fill paid as
        // replica components accumulated. string keys and long values are
        // immutable, so the shallow per-entry copy is a deep copy.
        new()
        {
            Increments = new Dictionary<string, long>(Increments),
            Decrements = new Dictionary<string, long>(Decrements),
        };

    /// <summary>
    /// Folds a <see cref="PnCounterDelta"/> into this counter: every
    /// per-replica entry in <see cref="PnCounterDelta.Increments"/> /
    /// <see cref="PnCounterDelta.Decrements"/> is pointwise-max'd into
    /// the matching local component. Commutative, associative, and
    /// idempotent against arrival order and duplicate delivery.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Null
    /// inner dictionaries are treated as empty.
    /// </param>
    public void MergeDelta(PnCounterDelta delta)
    {
        var inc = delta.Increments;
        if (inc is { Count: > 0 }) MergeSide(Increments, inc);
        var dec = delta.Decrements;
        if (dec is { Count: > 0 }) MergeSide(Decrements, dec);
    }

    private static void MergeSide(Dictionary<string, long> target, Dictionary<string, long> source)
    {
        foreach (var (k, v) in source)
        {
            // Single-probe fold: GetValueRefOrAddDefault hashes k once and
            // returns a ref to the slot (added zero-initialised when absent),
            // replacing the previous TryGetValue-then-indexer pattern that
            // hashed and bucket-walked twice for every replica component. The
            // pointwise-max result is identical: a missing slot is 0 (the
            // add-default value), and an existing slot is bumped only when the
            // incoming value is strictly greater, matching the old
            // existing > v ? existing : v choice. Mirrors the single-probe
            // fold VersionVector.Merge already uses.
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(target, k, out var existed);
            if (!existed || v > slot) slot = v;
        }
    }
}
